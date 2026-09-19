"""The NFO contract universe and the front-two-expiry scope (spec §1).

Verified against Kite on 2026-09-18: 35,922 NFO instruments, 216 underlyings
that have options, 647 futures.  Restricting to the **front two expiries per
underlying per kind** leaves 27,260 contracts (13,458 CE · 13,370 PE · 432 FUT).

Nothing here builds a Kite client: it takes the shared, authenticated
``KiteProvider`` from ``market_data.get_provider('kite')`` and goes through that
object's one guarded call path, which owns the 3 req/s limiter, the retries and
the single auto-auth.  A second client would be a second rate budget.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

from . import config

LOG = logging.getLogger("market_data.derivatives.instruments")

OPTION_TYPES = ("CE", "PE")
FUTURE_TYPE = "FUT"

#: The six F&O underlyings whose spot is an index rather than a stock.  The
#: mapping is recorded on every `underlying_snapshots` row as `spot_symbol`, so
#: a wrong mapping is visible in the data instead of hiding inside the code.
INDEX_SPOT = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
    "FINNIFTY": "NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NIFTY MID SELECT",
    "NIFTYNXT50": "NIFTY NEXT 50",
    "NIFTYFPI": "NIFTY FPI 150",
}


def utcnow() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


@dataclass(frozen=True)
class Contract:
    """One NFO instrument, normalised out of the Kite instrument dump."""

    instrument_token: int
    tradingsymbol: str
    underlying: str
    instrument_type: str      # CE | PE | FUT
    strike: float
    expiry: date
    lot_size: int
    tick_size: float = 0.05
    exchange: str = "NFO"
    segment: str = ""

    @property
    def is_option(self) -> bool:
        return self.instrument_type in OPTION_TYPES

    @property
    def kind(self) -> str:
        """The expiry ladder this contract belongs to: options or futures."""
        return FUTURE_TYPE if self.instrument_type == FUTURE_TYPE else "OPT"

    def days_to_expiry(self, on: date) -> int:
        return (self.expiry - on).days

    def as_row(self, *, in_scope: bool, snapshot_id: str | None,
               first_seen: str, fetched_at: str) -> tuple:
        return (
            self.instrument_token, self.tradingsymbol, self.underlying,
            self.instrument_type, float(self.strike), self.expiry.isoformat(),
            int(self.lot_size), float(self.tick_size), self.exchange, self.segment,
            first_seen, fetched_at, 1 if in_scope else 0, config.VENDOR_ID,
            fetched_at, snapshot_id,
        )


# ── the one guarded call path ────────────────────────────────────────────────

def kite_call(provider, fn: Callable, **kw):
    """Run ``fn(kite_client)`` through the provider's rate-limited call path.

    ``KiteProvider._call`` is the only place in this repo that holds the shared
    ``get_limiter('kite')`` bucket, retries, and runs auto-auth exactly once.
    Calling ``provider.kite`` directly would bypass all three, so this helper
    prefers a public ``call`` if the provider grows one and falls back to the
    private method otherwise.  A fake provider only has to offer one of them.
    """
    caller = getattr(provider, "call", None) or getattr(provider, "_call", None)
    if caller is None:
        raise AttributeError(
            f"{type(provider).__name__} exposes no guarded call path; "
            "derivatives capture refuses to build its own Kite client")
    return caller(fn, **kw)


# ── the instrument dump ──────────────────────────────────────────────────────

def _cache_path(cache_dir=None) -> Path:
    return Path(cache_dir or config.CACHE_DIR) / "instruments_NFO.json"


def _parse_expiry(value) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _to_contract(row: Mapping) -> Contract | None:
    expiry = _parse_expiry(row.get("expiry"))
    itype = (row.get("instrument_type") or "").upper()
    if expiry is None or itype not in (*OPTION_TYPES, FUTURE_TYPE):
        return None
    try:
        token = int(row["instrument_token"])
    except (KeyError, TypeError, ValueError):
        return None
    return Contract(
        instrument_token=token,
        tradingsymbol=str(row.get("tradingsymbol") or ""),
        underlying=str(row.get("name") or ""),
        instrument_type=itype,
        strike=float(row.get("strike") or 0.0),
        expiry=expiry,
        lot_size=int(row.get("lot_size") or 1),
        tick_size=float(row.get("tick_size") or 0.05),
        exchange=str(row.get("exchange") or config.EXCHANGE),
        segment=str(row.get("segment") or ""),
    )


def fetch_nfo_instruments(provider, *, refresh: bool = False,
                          ttl_hours: float = 12.0, cache_dir=None,
                          use_cache: bool = True) -> list[Contract]:
    """The whole NFO dump as ``Contract`` objects, cached on disk.

    One request.  The cache is ours (``db/derivatives_cache``) so it can never
    collide with the equity provider's own ``instruments_NSE.json``.
    """
    path = _cache_path(cache_dir)
    if use_cache and not refresh and path.exists():
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
            age = datetime.now(timezone.utc).replace(tzinfo=None) - datetime.fromisoformat(blob["fetched_at"])
            if age <= timedelta(hours=ttl_hours):
                out = [_to_contract(r) for r in blob["instruments"]]
                out = [c for c in out if c is not None]
                if out:
                    LOG.debug("NFO instruments from cache: %d", len(out))
                    return out
        except Exception as e:  # a bad cache is never fatal, just cold
            LOG.debug("NFO instrument cache unusable (%s)", type(e).__name__)

    raw = kite_call(provider, lambda k: k.instruments(config.EXCHANGE))
    contracts = [c for c in (_to_contract(r) for r in raw) if c is not None]
    if not contracts:
        raise RuntimeError("Kite returned no usable NFO instruments — refusing to "
                           "continue with an empty universe")
    if not use_cache:
        LOG.info("NFO instruments: %d (not cached)", len(contracts))
        return contracts
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "exchange": config.EXCHANGE,
            "fetched_at": utcnow(),
            "instruments": [
                {
                    "instrument_token": c.instrument_token,
                    "tradingsymbol": c.tradingsymbol,
                    "name": c.underlying,
                    "instrument_type": c.instrument_type,
                    "strike": c.strike,
                    "expiry": c.expiry.isoformat(),
                    "lot_size": c.lot_size,
                    "tick_size": c.tick_size,
                    "exchange": c.exchange,
                    "segment": c.segment,
                }
                for c in contracts
            ],
        }
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, path)
    except OSError as e:
        LOG.warning("could not cache the NFO dump (%s) — continuing", e)
    LOG.info("NFO instruments: %d", len(contracts))
    return contracts


# ── scope selection (spec §1) ────────────────────────────────────────────────

@dataclass
class Scope:
    """The contracts we capture, with the numbers that justify the list."""

    on: date
    contracts: list[Contract]
    expiries: dict[tuple[str, str], list[date]] = field(default_factory=dict)
    dropped_expired: int = 0
    total_listed: int = 0

    @property
    def tokens(self) -> list[int]:
        return [c.instrument_token for c in self.contracts]

    @property
    def underlyings(self) -> list[str]:
        return sorted({c.underlying for c in self.contracts})

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {"CE": 0, "PE": 0, "FUT": 0}
        for c in self.contracts:
            out[c.instrument_type] = out.get(c.instrument_type, 0) + 1
        out["total"] = len(self.contracts)
        out["underlyings"] = len(self.underlyings)
        return out

    def by_token(self) -> dict[int, Contract]:
        return {c.instrument_token: c for c in self.contracts}

    def front_future(self, underlying: str) -> Contract | None:
        futs = [c for c in self.contracts
                if c.underlying == underlying and c.instrument_type == FUTURE_TYPE]
        return min(futs, key=lambda c: c.expiry) if futs else None


def select_scope(contracts: Iterable[Contract], on: date | None = None, *,
                 expiries_per_underlying: int = config.EXPIRIES_PER_UNDERLYING,
                 underlyings: Sequence[str] | None = None) -> Scope:
    """Front ``n`` un-expired expiries per underlying, options and futures apart.

    An expiry on ``on`` itself is still live (it expires at the close), so the
    cut is ``expiry >= on``.  Everything else is ignored, which is the spec's
    rule and also the only way 35,922 instruments fit inside a 15-minute cycle.
    """
    on = on or date.today()
    items = [c for c in contracts if c.underlying]
    total_listed = len(items)
    if underlyings is not None:
        wanted = {u.upper() for u in underlyings}
        items = [c for c in items if c.underlying.upper() in wanted]
    live = [c for c in items if c.expiry >= on]
    dropped = len(items) - len(live)

    ladders: dict[tuple[str, str], set[date]] = {}
    for c in live:
        ladders.setdefault((c.underlying, c.kind), set()).add(c.expiry)
    front = {key: sorted(exps)[:expiries_per_underlying] for key, exps in ladders.items()}

    keep = [c for c in live if c.expiry in front.get((c.underlying, c.kind), ())]
    keep.sort(key=lambda c: (c.underlying, c.instrument_type, c.expiry, c.strike))
    return Scope(on=on, contracts=keep, expiries={k: list(v) for k, v in front.items()},
                 dropped_expired=dropped, total_listed=total_listed)


# ── spot instruments for the underlyings ─────────────────────────────────────

def spot_map(provider, underlyings: Iterable[str]) -> dict[str, tuple[str, int]]:
    """``underlying -> (nse_symbol, instrument_token)`` for the spot leg.

    Stock underlyings carry the same tradingsymbol on NSE.  The six index
    underlyings are mapped explicitly by ``INDEX_SPOT``.  An underlying this
    account cannot see is simply absent from the result — the capture then
    stores a NULL spot rather than a made-up one.
    """
    try:
        nse = provider.instruments()
    except Exception as e:
        LOG.warning("spot instrument list unavailable (%s); spot will be NULL",
                    type(e).__name__)
        return {}
    by_symbol: dict[str, object] = {}
    for inst in nse:
        seg = getattr(inst, "segment", "")
        sym = getattr(inst, "symbol", "")
        if seg in ("NSE", "INDICES") and sym not in by_symbol:
            by_symbol[sym] = inst
    out: dict[str, tuple[str, int]] = {}
    for u in underlyings:
        sym = INDEX_SPOT.get(u, u)
        inst = by_symbol.get(sym)
        if inst is None:
            continue
        try:
            out[u] = (sym, int(getattr(inst, "instrument_id")))
        except (TypeError, ValueError):
            continue
    missing = [u for u in underlyings if u not in out]
    if missing:
        LOG.info("no spot instrument for %d underlyings (spot stays NULL): %s",
                 len(missing), ", ".join(sorted(missing)[:10]))
    return out


__all__ = [
    "Contract", "Scope", "INDEX_SPOT", "OPTION_TYPES", "FUTURE_TYPE",
    "fetch_nfo_instruments", "select_scope", "spot_map", "kite_call", "utcnow",
]
