"""SL-M vs SL-L decision based on VIX, pre-open gap, and intraday range.

Pure function. Phase 2 will wire real VIX / gap / range; Phase 1 calls with VIX
only (gap=None, range=None) at 9:15 entry time.
"""
from __future__ import annotations

from typing import Optional

VIX_THRESHOLD = 18.0
GAP_THRESHOLD_PCT = 2.0
RANGE_THRESHOLD_PCT = 4.0


def sl_order_type(vix: float, gap_pct: Optional[float] = None,
                  intraday_range_pct: Optional[float] = None) -> str:
    """Returns 'SL-M' for volatile conditions, 'SL-L' otherwise."""
    if vix > VIX_THRESHOLD:
        return "SL-M"
    if gap_pct is not None and abs(gap_pct) > GAP_THRESHOLD_PCT:
        return "SL-M"
    if intraday_range_pct is not None and intraday_range_pct > RANGE_THRESHOLD_PCT:
        return "SL-M"
    return "SL-L"


def round_to_tick(price: float, tick: float = 0.05) -> float:
    """Round price to nearest tick. NSE EQ tick is ₹0.05 for stocks > ₹1.
    Kite rejects orders not on tick. Used for ALL SL/target/limit prices going to Kite."""
    if price <= 0 or tick <= 0:
        return round(price, 2)
    return round(round(price / tick) * tick, 2)


def sl_limit_price(trigger_price: float, tick: float = 0.05) -> float:
    """For SL-L orders: place limit 0.2% below trigger, rounded to tick."""
    return round_to_tick(trigger_price * 0.998, tick)
