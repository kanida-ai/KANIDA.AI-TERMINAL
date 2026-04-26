"""
Unified NSE price feed.
Priority: Zerodha Kite Connect → yfinance (fallback).
Accepts a dynamic ticker list — no hardcoded symbols.
"""
from __future__ import annotations

from typing import Optional
from .zerodha_feed import get_ltp as _zerodha_ltp
from .yfinance_feed import get_ltp as _yfinance_ltp


def get_current_prices(tickers: list[str]) -> dict[str, Optional[float]]:
    """
    Return last traded prices for the given ticker list.
    Tries Zerodha first; falls back to yfinance if all prices are None.
    """
    if not tickers:
        return {}

    prices = _zerodha_ltp(tickers)
    if any(v is not None for v in prices.values()):
        return prices
    return _yfinance_ltp(tickers)
