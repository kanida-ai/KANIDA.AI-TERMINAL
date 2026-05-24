"""Rolling-low fetcher for the 10d_low (Donchian) trail method.

Returns the lowest LOW across the last `lookback` *completed trading sessions*
(today excluded — decision #2: completed sessions only).

Caching: per (symbol, lookback) key, 30-minute TTL. Daily lows only change at
EOD, so a half-hour cache is plenty conservative.

Failure mode (decision: skip-on-None): on any Kite API error we return None.
The caller (trail_manager) MUST then skip the trail update for that poll —
it never substitutes a stale percentage. The previously-set Kite SL stays
in place at the broker; next successful poll re-evaluates.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

from .mtf_eligibility import get_instrument_token

log = logging.getLogger("kanida.falcon.trade.trail_data")
IST = timezone(timedelta(hours=5, minutes=30))
TTL = timedelta(minutes=30)

# Cache: (symbol, lookback) -> (low_value, fetched_at)
_cache: Dict[Tuple[str, int], Tuple[float, datetime]] = {}


def _calendar_window_for(lookback: int) -> int:
    """Calendar days to query so we get at least `lookback` trading sessions.

    Trading days ≈ 0.7 of calendar days (5 of 7), with holidays adding more
    slack. Multiplier 1.6 + 5-day cushion handles long weekends and festival
    breaks (e.g. Diwali) without ever returning fewer than `lookback` rows
    in normal markets.
    """
    return int(lookback * 1.6) + 5


def get_rolling_low(kite, symbol: str, lookback: int = 10,
                    force_refresh: bool = False) -> Optional[float]:
    """Lowest low over last `lookback` *completed* trading sessions.

    Returns None on any error (caller must skip trail update for this poll).
    """
    if lookback < 1:
        return None
    key = (symbol, lookback)
    now = datetime.now(IST)

    if not force_refresh:
        cached = _cache.get(key)
        if cached and now - cached[1] < TTL:
            return cached[0]

    token = get_instrument_token(kite, symbol)
    if token is None:
        log.warning("rolling_low: no instrument_token for %s — skipping", symbol)
        return None

    # Window: from = today − N calendar days, to = yesterday (excludes today).
    today = now.date()
    yesterday = today - timedelta(days=1)
    from_date = today - timedelta(days=_calendar_window_for(lookback))

    try:
        bars = kite.historical_data(token, from_date, yesterday, "day")
    except Exception as e:
        log.warning("rolling_low: kite.historical_data(%s) failed: %s", symbol, e)
        return None

    if not bars:
        log.warning("rolling_low: empty bars for %s (token=%s, %s..%s)",
                    symbol, token, from_date, yesterday)
        return None

    # Take the most recent `lookback` completed sessions.
    recent = bars[-lookback:]
    lows = [float(b.get("low") or 0) for b in recent if (b.get("low") or 0) > 0]
    if not lows:
        log.warning("rolling_low: no valid lows in window for %s", symbol)
        return None

    low = min(lows)
    _cache[key] = (low, now)
    log.debug("rolling_low: %s lookback=%d sessions=%d low=%.2f",
              symbol, lookback, len(lows), low)
    return low


def invalidate(symbol: Optional[str] = None) -> None:
    """Clear cache. If symbol given, only that symbol's entries; else all."""
    global _cache
    if symbol is None:
        _cache = {}
        return
    _cache = {k: v for k, v in _cache.items() if k[0] != symbol}


def cache_size() -> int:
    return len(_cache)
