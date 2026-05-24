"""MTF eligibility cache.

Pulls kite.instruments("NSE") and marks all NSE EQ stocks as MTF-eligible
(optimistic). Zerodha's actual MTF list is broker-specific and not exposed via
the standard SDK; for Phase 1 we let Kite reject non-MTF orders at placement
time (handled as F7 in spec — skip + continue).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

log = logging.getLogger("kanida.falcon.trade.mtf")

_cache: Dict[str, dict] = {}
_fetched_at: Optional[datetime] = None
TTL = timedelta(hours=24)


def _refresh(kite) -> int:
    global _cache, _fetched_at
    instruments = kite.instruments("NSE")
    new_cache: Dict[str, dict] = {}
    for inst in instruments:
        sym = inst.get("tradingsymbol")
        if not sym:
            continue
        is_eq = (
            inst.get("instrument_type") == "EQ"
            and inst.get("segment") in ("NSE", "BSE")  # cash equity
        )
        # tick_size is per-scrip on NSE (0.05 / 0.10 / 1.00 etc.). Kite rejects orders
        # not on the right tick. Default to 0.05 if unknown.
        tick_size = float(inst.get("tick_size") or 0.05)
        if tick_size <= 0:
            tick_size = 0.05
        new_cache[sym] = {
            "mtf":              bool(is_eq),
            "tick_size":        tick_size,
            "instrument_token": int(inst.get("instrument_token") or 0),
        }
    _cache = new_cache
    _fetched_at = datetime.now()
    log.info("MTF cache refreshed: %d symbols", len(_cache))
    return len(_cache)


def get_tick_size(kite, symbol: str) -> float:
    """Return per-symbol tick size in rupees. Falls back to 0.05 if unknown."""
    if _fetched_at is None or datetime.now() - _fetched_at > TTL:
        try:
            _refresh(kite)
        except Exception as e:
            log.warning("tick_size: cache refresh failed for %s: %s", symbol, e)
            return 0.05
    entry = _cache.get(symbol)
    if not entry:
        return 0.05
    return entry.get("tick_size", 0.05)


def get_instrument_token(kite, symbol: str) -> Optional[int]:
    """Return NSE instrument_token for kite.historical_data(). None if unknown."""
    if _fetched_at is None or datetime.now() - _fetched_at > TTL:
        try:
            _refresh(kite)
        except Exception as e:
            log.warning("instrument_token: cache refresh failed for %s: %s", symbol, e)
            return None
    entry = _cache.get(symbol)
    if not entry:
        return None
    tok = entry.get("instrument_token") or 0
    return int(tok) if tok > 0 else None


def is_mtf_eligible(kite, symbol: str) -> bool:
    if _fetched_at is None or datetime.now() - _fetched_at > TTL:
        try:
            _refresh(kite)
        except Exception as e:
            log.warning("MTF cache refresh failed: %s — fail-open", e)
            return True   # fail-open: broker will reject at placement (F7)
    entry = _cache.get(symbol)
    if entry is None:
        # Unknown symbol — conservative
        return False
    return entry.get("mtf", False)


def refresh_cache(kite) -> int:
    return _refresh(kite)


def is_cached() -> bool:
    return _fetched_at is not None


def reset_cache_for_test() -> None:
    """Test-only: forget cache so next call re-fetches."""
    global _cache, _fetched_at
    _cache = {}
    _fetched_at = None
