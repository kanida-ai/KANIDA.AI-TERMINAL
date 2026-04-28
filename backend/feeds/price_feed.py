"""
NSE price feed — Zerodha Kite Connect only.
"""
from __future__ import annotations

from typing import Optional
from .zerodha_feed import get_ltp as _zerodha_ltp


def get_current_prices(tickers: list[str]) -> dict[str, Optional[float]]:
    """Return last traded prices for the given tickers via Zerodha."""
    if not tickers:
        return {}
    return _zerodha_ltp(tickers)
