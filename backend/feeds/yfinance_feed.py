"""
yfinance price feed — fallback when Zerodha credentials are absent/expired.
Accepts a dynamic ticker list. Uses NSE .NS suffix.
NOTE: yfinance is banned for historical data. This fallback is LTP-only.
"""
from __future__ import annotations

from typing import Optional


def get_ltp(tickers: list[str]) -> dict[str, Optional[float]]:
    prices: dict[str, Optional[float]] = {t: None for t in tickers}
    try:
        import yfinance as yf
        for ticker in tickers:
            try:
                fi = yf.Ticker(f"{ticker}.NS").fast_info
                val = fi.last_price
                if val and float(val) > 0:
                    prices[ticker] = round(float(val), 2)
            except Exception:
                pass
    except ImportError:
        pass
    return prices
