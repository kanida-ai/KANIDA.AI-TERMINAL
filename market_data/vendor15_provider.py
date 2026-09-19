r"""REST adapter skeleton for the incoming 15-minute delayed vendor (contract §1/§6).

Owner decision 6: *"Vendor later: REST, 15-minute OHLCV, ~15-minute delay. The
swap must be a config change."*  This module is that swap.  Turning the
pipeline over to the vendor is::

    MARKET_DATA_PROVIDER=vendor15
    VENDOR15_BASE_URL=https://api.thevendor.example/v1
    VENDOR15_API_KEY=...            (never logged, never echoed)

and, if their JSON does not happen to use our names, editing exactly one dict:
``FIELD_MAP`` below.  No call site changes.  Everything else that could differ
between vendors is also a value, not code: ``VENDOR15_*`` env vars cover the
endpoint path, auth style, interval token, timestamp convention, timezone,
date format, per-request span, rate limit and the JSON envelope key.

=============================================================================
CHECKLIST — must be confirmed IN WRITING by the vendor before this goes live
=============================================================================
Every item below is a silent-corruption risk if we guess.  The defaults here
are *assumptions*, marked as such, and each one is a single env var to change.

 1. **Symbol identifiers** — do they key on NSE tradingsymbol, ISIN, or a
    private id?  What happens on a symbol rename or an NSE series change?  Is
    there a stable instrument id we can persist as ``instrument_id``?
 2. **Adjusted vs raw prices** — are OHLC adjusted for splits/bonuses?  If so,
    adjusted *as of when*, and does a new corporate action silently restate
    history?  We must be able to record an ``adjustment_basis_id``
    (``VENDOR15_ADJUSTMENT_BASIS_ID``) that changes when the basis changes.
 3. **Timestamp timezone and convention** — IST or UTC?  Does the timestamp
    mark the bar's START or its END?  (``VENDOR15_TIMEZONE``,
    ``VENDOR15_TIMESTAMP_CONVENTION``.)  Getting this wrong shifts every
    signal by one bar and is invisible in a spot check.
 4. **Inclusive/exclusive range semantics** — is ``to`` inclusive?  Are bars
    returned for the boundary instants?  (``VENDOR15_RANGE_END_INCLUSIVE``.)
 5. **Max range per request** and pagination — how many days per call, is
    there a cursor, and is there a max row count that truncates silently?
 6. **Rate limit** — requests/second and any daily quota, per key or per IP.
 7. **Corporate-action handling** — are historical bars restated in place?  Is
    there a revision/version field per bar (``FIELD_MAP['vendor_revision']``)
    so we can store a new revision instead of overwriting?  Is there a
    corporate-actions endpoint?
 8. **Historical depth** — how far back does 15-minute data go, and is depth
    the same for every symbol?  What about suspended/delisted names (the
    FORCEMOT 2023-10-26..2024-02-13 case in contract §3.5)?
 9. Also confirm: exact publication delay (we assume 900s), behaviour on a
    half-day/special session, whether volume is cumulative or per-bar, and
    what an empty window returns (empty array vs 404 vs zero-filled bars —
    **zero-filled bars would be fabricated data and must be rejected**).

Until the vendor answers, this adapter is exercised only against the recorded
fixture in ``tests/fixtures/vendor15_*.json`` via the same
``assert_provider_conformance`` suite Kite and Fake pass.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .provider import BaseProvider, register_provider
from .ratelimit import get_limiter
from .types import (
    IST,
    Instrument,
    ProviderError,
    QualityFlag,
    RateLimitError,
    RawCandle,
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

log = logging.getLogger("market_data.vendor15")


# =============================================================================
# THE ONE DICT TO EDIT WHEN THE VENDOR'S JSON DIFFERS
# =============================================================================
#: our normalized name -> the vendor's JSON key for a candle row.
#: A dotted key ("ohlc.o") walks nested objects.  A value of ``None`` means the
#: vendor does not supply it and the adapter derives/defaults it.
FIELD_MAP: Dict[str, Optional[str]] = {
    "timestamp": "timestamp",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "vendor_revision": None,     # e.g. "rev" / "version" once §7 is answered
    "symbol": None,              # row-level symbol, if the vendor echoes it
}

#: our normalized name -> the vendor's JSON key for an instrument row.
INSTRUMENT_FIELD_MAP: Dict[str, Optional[str]] = {
    "instrument_id": "id",
    "symbol": "symbol",
    "exchange": "exchange",
    "name": "name",
    "isin": "isin",
}

#: our timeframe id -> the vendor's ``interval`` query value.
INTERVAL_MAP: Dict[str, str] = {
    "15minute": "15m",
    "day": "1d",
}


def _dig(row: Any, key: Optional[str], default=None):
    """Read a (possibly dotted) key out of a vendor row."""
    if key is None:
        return default
    cur = row
    for part in key.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


class Vendor15Provider(BaseProvider):
    """REST, 15-minute OHLCV, delayed by design.

    ``delay_seconds=900`` is not a bug and not staleness: contract §5 says a
    15-minute delayed vendor reads as "delayed 15 min by design".  Everything
    downstream gets that number from the provider, so freshness alarms stay
    correct after the swap.
    """

    provider_id = "vendor15"
    supported_timeframes = ("15minute", "day")
    adjustment_basis_id_default = "vendor15-unconfirmed"

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        session: Any = None,
        max_retries: int = 4,
    ):
        self.base_url = (base_url or _env("VENDOR15_BASE_URL")).rstrip("/")
        self._api_key = api_key if api_key is not None else _env("VENDOR15_API_KEY")
        self.candles_path = _env("VENDOR15_CANDLES_PATH", "/candles")
        self.instruments_path = _env("VENDOR15_INSTRUMENTS_PATH", "/instruments")
        self.auth_style = _env("VENDOR15_AUTH_STYLE", "header").lower()  # header|query|bearer
        self.auth_header = _env("VENDOR15_AUTH_HEADER", "X-API-Key")
        self.auth_query_param = _env("VENDOR15_AUTH_QUERY_PARAM", "apikey")
        self.rows_key = _env("VENDOR15_ROWS_KEY", "data")       # "" = top-level array
        # the instruments endpoint often uses a different envelope key
        self.instruments_rows_key = _env("VENDOR15_INSTRUMENTS_ROWS_KEY", "") or self.rows_key
        self.date_format = _env("VENDOR15_DATE_FORMAT", "%Y-%m-%d")
        self.tz_name = _env("VENDOR15_TIMEZONE", "IST").upper()  # IST|UTC  (checklist §3)
        #: "start" or "end" — which instant the vendor's timestamp marks (§3)
        self.timestamp_convention = _env("VENDOR15_TIMESTAMP_CONVENTION", "start").lower()
        self.range_end_inclusive = _env("VENDOR15_RANGE_END_INCLUSIVE", "1") != "0"  # §4
        self.adjustment_basis_id = _env(
            "VENDOR15_ADJUSTMENT_BASIS_ID", self.adjustment_basis_id_default
        )  # §2
        self.delay_seconds = int(_env("VENDOR15_DELAY_SECONDS", "900"))  # §9
        self.rate_limit_per_second = float(_env("VENDOR15_RATE_PER_SECOND", "2"))  # §6
        self.max_days_per_request = {  # §5
            "15minute": int(_env("VENDOR15_MAX_DAYS_15MINUTE", "30")),
            "day": int(_env("VENDOR15_MAX_DAYS_DAY", "365")),
        }
        self.timeout = float(_env("VENDOR15_TIMEOUT_SEC", "20"))
        self.max_retries = max_retries
        self._session = session
        self.limiter = get_limiter("vendor15", self.rate_limit_per_second)
        self.requests_made = 0
        self._instruments: Optional[List[Instrument]] = None

    # -- http ------------------------------------------------------------

    @property
    def session(self):
        if self._session is None:
            try:
                import requests
            except ImportError as e:
                raise ProviderError(
                    "requests is not installed in this interpreter", provider_id="vendor15"
                ) from e
            self._session = requests.Session()
        return self._session

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self._api_key and self.auth_style == "header":
            h[self.auth_header] = self._api_key
        elif self._api_key and self.auth_style == "bearer":
            h["Authorization"] = f"Bearer {self._api_key}"
        return h

    def _get(self, path: str, params: Dict[str, Any]) -> Any:
        """One rate-limited GET with exponential backoff.

        The api key is never placed in a log line and never in an exception
        message — only the path and the status code are reported.
        """
        if not self.base_url:
            raise ProviderError(
                "VENDOR15_BASE_URL is not configured", provider_id="vendor15"
            )
        if self._api_key and self.auth_style == "query":
            params = {**params, self.auth_query_param: self._api_key}
        url = f"{self.base_url}{path}"
        delay = 1.0
        last = ""
        for _ in range(self.max_retries):
            self.limiter.acquire()
            self.requests_made += 1
            try:
                resp = self.session.get(
                    url, params=params, headers=self._headers(), timeout=self.timeout
                )
            except Exception as e:  # transport failure
                last = type(e).__name__
                time.sleep(delay)
                delay = min(delay * 2, 16.0)
                continue
            if resp.status_code in (401, 403):
                raise TokenError(
                    f"vendor15 rejected the credentials (HTTP {resp.status_code} on {path})",
                    provider_id="vendor15",
                )
            if resp.status_code == 404:
                # "no data for this window" must NOT become a fabricated bar.
                return []
            if resp.status_code == 429 or resp.status_code >= 500:
                last = f"HTTP {resp.status_code}"
                retry_after = float(resp.headers.get("Retry-After") or delay)
                time.sleep(min(retry_after, 30.0))
                delay = min(delay * 2, 16.0)
                continue
            if resp.status_code != 200:
                raise ProviderError(
                    f"vendor15 HTTP {resp.status_code} on {path}", provider_id="vendor15"
                )
            try:
                return resp.json()
            except Exception as e:
                raise ProviderError(
                    f"vendor15 returned non-JSON on {path} ({type(e).__name__})",
                    provider_id="vendor15",
                ) from e
        raise RateLimitError(
            f"vendor15 unavailable after {self.max_retries} attempts on {path} ({last})",
            provider_id="vendor15",
        )

    def _rows(self, payload: Any, rows_key: Optional[str] = None) -> List[dict]:
        if payload is None:
            return []
        if isinstance(payload, list):
            return payload
        key = self.rows_key if rows_key is None else rows_key
        if not key:
            return []
        rows = _dig(payload, key, [])
        return rows if isinstance(rows, list) else []

    # -- interface -------------------------------------------------------

    def instruments(self, refresh: bool = False) -> List[Instrument]:
        if self._instruments is not None and not refresh:
            return self._instruments
        rows = self._rows(
            self._get(self.instruments_path, {"exchange": "NSE"}), self.instruments_rows_key
        )
        m = INSTRUMENT_FIELD_MAP
        self._instruments = [
            Instrument(
                instrument_id=str(_dig(r, m["instrument_id"], "") or _dig(r, m["symbol"], "")),
                symbol=str(_dig(r, m["symbol"], "")),
                exchange=str(_dig(r, m["exchange"], "NSE") or "NSE"),
                name=str(_dig(r, m["name"], "") or ""),
                isin=str(_dig(r, m["isin"], "") or ""),
                vendor_id="vendor15",
            )
            for r in rows
        ]
        return self._instruments

    def _parse_ts(self, value: Any) -> datetime:
        """Vendor timestamp -> tz-aware IST instant (checklist §3)."""
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
        else:
            s = str(value).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST if self.tz_name == "IST" else timezone.utc)
        return dt.astimezone(IST)

    def _fetch_window(self, symbol: str, timeframe: str, start_day: date, end_day: date):
        interval = INTERVAL_MAP.get(timeframe)
        if interval is None:
            raise ProviderError(
                f"vendor15: no interval mapping for {timeframe!r}", provider_id="vendor15"
            )
        req_end = end_day if self.range_end_inclusive else end_day + timedelta(days=1)
        payload = self._get(
            self.candles_path,
            {
                "symbol": symbol,
                "interval": interval,
                "from": start_day.strftime(self.date_format),
                "to": req_end.strftime(self.date_format),
            },
        )
        return self._normalize(self._rows(payload), symbol, timeframe)

    def _normalize(self, rows: List[dict], symbol: str, timeframe: str) -> List[RawCandle]:
        m = FIELD_MAP
        fetched_at = utc_now()
        cutoff = fetched_at.timestamp() - self.delay_seconds
        step = timedelta(minutes=15) if timeframe == "15minute" else None
        out: List[RawCandle] = []
        for r in rows:
            ts = self._parse_ts(_dig(r, m["timestamp"]))
            if timeframe == "day":
                bar_start = session_open(ts.date())
                bar_end = session_close(ts.date())
            else:
                # §3: if the vendor stamps the bar END, shift back one step.
                bar_start = ts - step if self.timestamp_convention == "end" else ts
                bar_end = bar_end_for(bar_start, timeframe)
            o = float(_dig(r, m["open"]))
            h = float(_dig(r, m["high"]))
            l = float(_dig(r, m["low"]))
            c = float(_dig(r, m["close"]))
            vol = int(_dig(r, m["volume"], 0) or 0)
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
                    instrument_id=str(_dig(r, m["symbol"], symbol) or symbol),
                    symbol=str(_dig(r, m["symbol"], symbol) or symbol),
                    exchange="NSE",
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
                    vendor_id="vendor15",
                    vendor_revision=str(_dig(r, m["vendor_revision"], "") or ""),
                    fetched_at=fetched_at,
                )
            )
        return out


register_provider("vendor15", lambda **kw: Vendor15Provider(**kw))

__all__ = ["Vendor15Provider", "FIELD_MAP", "INSTRUMENT_FIELD_MAP", "INTERVAL_MAP"]
