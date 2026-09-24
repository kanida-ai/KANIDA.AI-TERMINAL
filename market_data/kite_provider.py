"""Zerodha Kite historical-data provider (contract §1).

What this module is
-------------------
A thin, *read-only* adapter over ``kiteconnect.KiteConnect.historical_data``
that returns normalized ``RawCandle`` rows.  It fetches market data.  It never
places an order, never writes to the engine project, and never touches
``db/kanida.db``.

Credentials
-----------
There is exactly one source of truth and this module does not add a second one:

1. newest row of ``kite_tokens`` in the SQLite DB at ``KANIDA_DB_PATH``
   (read from the engine project's ``config/.env``), then
2. the ``KITE_ACCESS_TOKEN`` value in that same ``.env``.

This mirrors ``backend/services/kite_auth.get_access_token`` and
``universe_engine/engine/data_fetch.get_latest_access_token`` in the
"Kanida.ai Terminal Quant Intelligence Engine" project.

**Secrets never leave this process.**  Values read from ``.env`` are kept in a
module-private dict, not pushed into ``os.environ``; no token, api key or
secret is ever printed, logged, put in an exception message or written to the
instrument cache.  Log lines carry the *length* or a yes/no at most.

Auto-authentication (owner-approved, contract decision 4)
---------------------------------------------------------
On a Kite ``TokenException`` we do **not** implement a login flow.  We invoke
the engine project's existing, tested worker exactly the way its Scheduled Task
does — a fresh short-lived process::

    <engine python> scripts/auth_worker.py          (cwd=<engine>/backend,
                                                     PLAYWRIGHT_BROWSERS_PATH=
                                                     C:\\ProgramData\\ms-playwright
                                                     on Windows, Playwright's
                                                     per-user cache on macOS)

...**once** per provider instance, then retry the failed call once.  The
worker self-gates to weekdays 06:00-16:30 IST; outside that window it exits 0
without minting, so we detect "token unchanged" and raise a clear
``TokenError`` instead of looping.  Child stdout/stderr is never echoed.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from .provider import BaseProvider, register_provider
from .ratelimit import get_limiter
from .types import (
    IST,
    Instrument,
    ProviderError,
    QualityFlag,
    RawCandle,
    RateLimitError,
    TokenError,
    bar_end_for,
    basic_quality_flags,
    in_session,
    is_on_grid,
    session_close,
    session_open,
    to_ist,
    utc_now,
)

log = logging.getLogger("market_data.kite")

# ── engine-project locations (overridable by env, never hard-coded twice) ────

if sys.platform == "win32":
    DEFAULT_ENGINE_ROOT = Path(
        r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
    )
    DEFAULT_AUTH_PYTHON = Path(r"C:\Users\SPS\anaconda3\python.exe")
    # Task Scheduler cannot see the user-profile default; ProgramData it can.
    DEFAULT_PLAYWRIGHT_PATH = r"C:\ProgramData\ms-playwright"
else:
    # macOS layout from migration/MAC_MIGRATION.md. launchd agents run in the
    # user's session, so Playwright's own per-user default is visible there.
    DEFAULT_ENGINE_ROOT = Path.home() / "Kanida" / "engine"
    DEFAULT_AUTH_PYTHON = DEFAULT_ENGINE_ROOT / ".venv" / "bin" / "python"
    DEFAULT_PLAYWRIGHT_PATH = str(
        Path.home() / ("Library/Caches" if sys.platform == "darwin" else ".cache") / "ms-playwright"
    )
ENGINE_ROOT = Path(os.environ.get("KANIDA_ENGINE_ROOT", str(DEFAULT_ENGINE_ROOT)))
ENV_FILE = ENGINE_ROOT / "config" / ".env"
AUTH_WORKER = ENGINE_ROOT / "scripts" / "auth_worker.py"
PLAYWRIGHT_BROWSERS_PATH = (
    DEFAULT_PLAYWRIGHT_PATH if sys.platform == "win32"
    else os.environ.get("PLAYWRIGHT_BROWSERS_PATH", DEFAULT_PLAYWRIGHT_PATH)
)

CACHE_DIR = Path(__file__).resolve().parent / "cache"

#: Kite per-request calendar-day caps.  Measured live (see
#: ``tests/test_kite_provider.py::test_measured_caps``) — not copied from docs.
#: ``15minute`` was confirmed to accept a 200-day span and to reject 201.
KITE_MAX_DAYS: Dict[str, int] = {
    "minute": 60,
    "3minute": 100,
    "5minute": 100,
    "10minute": 100,
    "15minute": 200,
    "30minute": 200,
    "60minute": 400,
    "day": 2000,
}

#: Kite's published historical-API limit is 3 requests/second for the key.
KITE_RATE_PER_SECOND = float(os.environ.get("KITE_RATE_PER_SECOND", "3"))

_ENV_LOCK = threading.Lock()
_ENV_CACHE: Optional[Dict[str, str]] = None


# ── .env (private; secrets never enter os.environ) ───────────────────────────

def _env() -> Dict[str, str]:
    """Parse the engine project's ``config/.env`` once, into a private dict."""
    global _ENV_CACHE
    with _ENV_LOCK:
        if _ENV_CACHE is not None:
            return _ENV_CACHE
        data: Dict[str, str] = {}
        if ENV_FILE.exists():
            for line in ENV_FILE.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and v:
                    data[k] = v
        _ENV_CACHE = data
        return data


def _cfg(name: str, default: str = "") -> str:
    """Process env wins (so a caller can override), then the engine ``.env``."""
    return os.environ.get(name) or _env().get(name, default)


def reset_env_cache() -> None:
    """Test hook: force the next ``_cfg`` call to re-read ``.env``."""
    global _ENV_CACHE
    with _ENV_LOCK:
        _ENV_CACHE = None


def kanida_db_path() -> Path:
    return Path(_cfg("KANIDA_DB_PATH", str(ENGINE_ROOT / "data" / "db" / "kanida_quant.db")))


def _token_from_db() -> Optional[str]:
    """Newest ``kite_tokens`` row, or None.  Never raises, never logs the value."""
    db = kanida_db_path()
    if not db.exists():
        return None
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        try:
            row = con.execute(
                "SELECT access_token FROM kite_tokens ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            con.close()
        if row and row[0]:
            return str(row[0])
    except Exception as e:  # missing table, locked db, ...
        log.debug("kite: token DB read failed (%s)", type(e).__name__)
    return None


def _token_fingerprint(token: Optional[str]) -> str:
    """A safe way to say "the token changed" without revealing any of it."""
    if not token:
        return "none"
    import hashlib

    return hashlib.sha256(token.encode()).hexdigest()[:8]


def get_access_token() -> str:
    """DB first, ``.env`` second.  Raises ``TokenError`` if neither exists."""
    tok = _token_from_db() or _cfg("KITE_ACCESS_TOKEN")
    if not tok:
        raise TokenError(
            f"no Kite access token: kite_tokens in {kanida_db_path()} is empty/absent "
            f"and KITE_ACCESS_TOKEN is unset in {ENV_FILE}",
            provider_id="kite",
        )
    return tok


# ── auto-auth (call the engine project's worker; do NOT reimplement login) ───

def run_auto_auth(timeout: float = 300.0) -> tuple[bool, str]:
    """Run the engine project's auth worker once in a fresh process.

    Returns ``(token_changed, detail)``.  ``detail`` is safe to log: it contains
    only the return code and whether the DB token fingerprint moved.
    """
    if not AUTH_WORKER.exists():
        return False, f"auth worker not found at {AUTH_WORKER}"

    py = Path(os.environ.get("KANIDA_AUTH_PYTHON", str(DEFAULT_AUTH_PYTHON)))
    if not py.exists():
        py = Path(sys.executable)

    before = _token_fingerprint(_token_from_db())
    child_env = dict(os.environ)
    # The Scheduled Task sets this; the user-profile default is invisible to a
    # non-interactive session and is the documented root cause of past failures.
    child_env["PLAYWRIGHT_BROWSERS_PATH"] = PLAYWRIGHT_BROWSERS_PATH
    argv = [str(py), str(AUTH_WORKER)]
    if os.environ.get("MARKET_DATA_AUTOAUTH_FORCE") == "1":
        argv.append("--force")
    try:
        proc = subprocess.run(
            argv,
            cwd=str(ENGINE_ROOT / "backend"),
            env=child_env,
            capture_output=True,   # captured ONLY so it is never echoed
            text=True,
            timeout=timeout,
        )
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        return False, "auth worker timed out"
    except Exception as e:
        return False, f"auth worker could not start ({type(e).__name__})"

    after = _token_fingerprint(_token_from_db())
    changed = after != before and after != "none"
    return changed, f"rc={rc} token_changed={'yes' if changed else 'no'}"


def _is_token_error(exc: BaseException) -> bool:
    name = type(exc).__name__
    if name in ("TokenException", "PermissionException"):
        return True
    msg = str(exc).lower()
    return (
        "tokenexception" in msg
        or "access_token" in msg
        or "incorrect `api_key`" in msg
        or "invalid api_key" in msg
        or "invalid session" in msg
    )


def _is_rate_error(exc: BaseException) -> bool:
    name = type(exc).__name__
    return name == "NetworkException" and "too many requests" in str(exc).lower()


# ── provider ─────────────────────────────────────────────────────────────────

class KiteProvider(BaseProvider):
    """Historical OHLCV from Kite Connect.  Thread-safe; share one instance."""

    provider_id = "kite"
    delay_seconds = 0               # Kite historical is not delayed
    rate_limit_per_second = KITE_RATE_PER_SECOND
    max_days_per_request = dict(KITE_MAX_DAYS)
    supported_timeframes = ("15minute", "30minute", "60minute", "day")
    #: Kite equity candles come back on the vendor's own adjustment basis.
    adjustment_basis_id = "kite-eod-adjusted"

    def __init__(
        self,
        *,
        exchange: str = "NSE",
        max_retries: int = 5,
        auto_auth: bool = True,
        instruments_ttl_hours: float = 12.0,
    ):
        self.exchange = exchange
        self.max_retries = max_retries
        self.auto_auth = auto_auth
        self.instruments_ttl = timedelta(hours=instruments_ttl_hours)
        self._kite = None
        self._kite_lock = threading.Lock()
        self._auto_auth_done = False
        self._instruments: Optional[List[Instrument]] = None
        self._by_symbol: Dict[str, Instrument] = {}
        self._inst_lock = threading.Lock()
        # ONE bucket for the whole process, whatever how many threads fetch.
        self.limiter = get_limiter("kite", self.rate_limit_per_second)
        self.requests_made = 0

    # -- client ----------------------------------------------------------

    def _build_client(self):
        try:
            from kiteconnect import KiteConnect
        except ImportError as e:
            raise ProviderError(
                "kiteconnect is not installed in this interpreter", provider_id="kite"
            ) from e
        api_key = _cfg("KITE_API_KEY")
        if not api_key:
            raise TokenError(f"KITE_API_KEY missing from {ENV_FILE}", provider_id="kite")
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(get_access_token())
        kite.timeout = float(os.environ.get("KITE_TIMEOUT_SEC", "20"))
        return kite

    @property
    def kite(self):
        with self._kite_lock:
            if self._kite is None:
                self._kite = self._build_client()
            return self._kite

    def _refresh_client(self) -> None:
        with self._kite_lock:
            self._kite = None

    def token_health(self) -> tuple[bool, str]:
        """One cheap ``profile()`` call.  Returns ``(ok, reason)``; never leaks."""
        try:
            self._call(lambda k: k.profile(), retries=1, allow_auto_auth=False)
            return True, "profile() ok"
        except Exception as e:
            return False, type(e).__name__

    # -- the single guarded call path ------------------------------------

    def _call(self, fn, *, retries: Optional[int] = None, allow_auto_auth: bool = True):
        """Rate-limited call with exponential backoff and one auto-auth retry."""
        retries = self.max_retries if retries is None else retries
        delay = 1.0
        last: Optional[BaseException] = None
        for attempt in range(retries):
            self.limiter.acquire()
            try:
                self.requests_made += 1
                return fn(self.kite)
            except Exception as e:  # noqa: BLE001 - classified immediately below
                last = e
                if _is_token_error(e):
                    if allow_auto_auth and self.auto_auth and not self._auto_auth_done:
                        self._auto_auth_done = True
                        log.warning("kite: token rejected — running engine auto-auth once")
                        changed, detail = run_auto_auth()
                        log.warning("kite: auto-auth finished (%s)", detail)
                        self._refresh_client()
                        if changed:
                            continue
                    raise TokenError(
                        f"kite rejected the access token ({type(e).__name__}); "
                        f"auto-auth {'ran but did not mint a new token' if self._auto_auth_done else 'not attempted'}",
                        provider_id="kite",
                    ) from None
                if _is_rate_error(e):
                    time.sleep(delay)
                    delay = min(delay * 2, 16.0)
                    continue
                time.sleep(delay)
                delay = min(delay * 2, 16.0)
        raise ProviderError(
            f"kite call failed after {retries} attempts: {type(last).__name__}: {last}",
            provider_id="kite",
            retryable=True,
        )

    # -- instruments -----------------------------------------------------

    def _cache_file(self) -> Path:
        return CACHE_DIR / f"instruments_{self.exchange}.json"

    def _load_cached_instruments(self) -> Optional[List[Instrument]]:
        p = self._cache_file()
        if not p.exists():
            return None
        try:
            blob = json.loads(p.read_text(encoding="utf-8"))
            fetched = datetime.fromisoformat(blob["fetched_at"])
            if utc_now() - fetched > self.instruments_ttl:
                return None
            return [
                Instrument(
                    instrument_id=str(r["instrument_id"]),
                    symbol=r["symbol"],
                    exchange=r.get("exchange", self.exchange),
                    name=r.get("name", ""),
                    segment=r.get("segment", ""),
                    instrument_type=r.get("instrument_type", "EQ"),
                    lot_size=int(r.get("lot_size", 1)),
                    tick_size=float(r.get("tick_size", 0.05)),
                    vendor_id="kite",
                )
                for r in blob["instruments"]
            ]
        except Exception as e:
            log.debug("kite: instrument cache unusable (%s)", type(e).__name__)
            return None

    def _save_cached_instruments(self, items: List[Instrument]) -> None:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "exchange": self.exchange,
            "fetched_at": utc_now().isoformat(),
            "instruments": [
                {
                    "instrument_id": i.instrument_id,
                    "symbol": i.symbol,
                    "exchange": i.exchange,
                    "name": i.name,
                    "segment": i.segment,
                    "instrument_type": i.instrument_type,
                    "lot_size": i.lot_size,
                    "tick_size": i.tick_size,
                }
                for i in items
            ],
        }
        tmp = self._cache_file().with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        tmp.replace(self._cache_file())

    def instruments(self, refresh: bool = False) -> List[Instrument]:
        with self._inst_lock:
            if self._instruments is not None and not refresh:
                return self._instruments
            items = None if refresh else self._load_cached_instruments()
            if items is None:
                raw = self._call(lambda k: k.instruments(self.exchange))
                items = [
                    Instrument(
                        instrument_id=str(r["instrument_token"]),
                        symbol=r["tradingsymbol"],
                        exchange=r.get("exchange", self.exchange),
                        name=r.get("name", "") or "",
                        segment=r.get("segment", "") or "",
                        instrument_type=r.get("instrument_type", "EQ") or "EQ",
                        lot_size=int(r.get("lot_size") or 1),
                        tick_size=float(r.get("tick_size") or 0.05),
                        vendor_id="kite",
                    )
                    for r in raw
                ]
                self._save_cached_instruments(items)
            self._instruments = items
            # EQ segment wins on a tradingsymbol collision with derivatives.
            self._by_symbol = {}
            for i in items:
                if i.symbol not in self._by_symbol or i.segment == self.exchange:
                    self._by_symbol[i.symbol] = i
            return items

    def resolve(self, symbol: str) -> Instrument:
        if not self._by_symbol:
            self.instruments()
        inst = self._by_symbol.get(symbol)
        if inst is None:
            self.instruments(refresh=True)
            inst = self._by_symbol.get(symbol)
        if inst is None:
            raise ProviderError(
                f"symbol {symbol!r} is not in the Kite {self.exchange} instrument list "
                f"(invisible to this account, delisted, or renamed)",
                provider_id="kite",
            )
        return inst

    # -- candles ---------------------------------------------------------

    def _fetch_window(self, symbol: str, timeframe: str, start_day: date, end_day: date):
        inst = self.resolve(symbol)
        frm = datetime.combine(start_day, datetime.min.time())
        to = datetime.combine(end_day, datetime.max.time().replace(microsecond=0))
        raw = self._call(
            lambda k: k.historical_data(int(inst.instrument_id), frm, to, timeframe)
        )
        return self._normalize(raw, inst, timeframe)

    def _normalize(self, raw, inst: Instrument, timeframe: str) -> List[RawCandle]:
        """Vendor rows -> ``RawCandle``.  No row is dropped and none is invented."""
        fetched_at = utc_now()
        cutoff = fetched_at.timestamp() - self.delay_seconds
        out: List[RawCandle] = []
        for r in raw or []:
            ts = to_ist(r["date"])
            if timeframe == "day":
                # Kite stamps daily bars at 00:00 IST; normalize to the session.
                bar_start = session_open(ts.date())
                bar_end = session_close(ts.date())
            else:
                bar_start = ts
                bar_end = bar_end_for(ts, timeframe)
            o, h, l, c = (
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
            )
            vol = int(r.get("volume") or 0)
            flags = basic_quality_flags(o, h, l, c, vol)
            if not is_on_grid(bar_start, timeframe):
                flags |= QualityFlag.OFF_GRID
            if not in_session(bar_start, timeframe):
                flags |= QualityFlag.OUTSIDE_SESSION
            complete = bar_end.timestamp() <= cutoff
            if not complete:
                flags |= QualityFlag.PARTIAL_BAR
            out.append(
                RawCandle(
                    instrument_id=inst.instrument_id,
                    symbol=inst.symbol,
                    exchange=inst.exchange,
                    timeframe=timeframe,
                    bar_start=bar_start,
                    bar_end=bar_end,
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    volume=vol,
                    candle_complete=complete,
                    quality_flags=flags,
                    adjustment_basis_id=self.adjustment_basis_id,
                    vendor_id="kite",
                    vendor_revision="",
                    fetched_at=fetched_at,
                )
            )
        return out


register_provider("kite", lambda **kw: KiteProvider(**kw))


__all__ = [
    "KiteProvider",
    "get_access_token",
    "run_auto_auth",
    "kanida_db_path",
    "reset_env_cache",
    "KITE_MAX_DAYS",
    "ENGINE_ROOT",
    "ENV_FILE",
]
