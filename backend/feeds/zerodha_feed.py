"""
Zerodha Kite Connect price feed.
Uses services.kite_auth as single credential source — no local .env parsing.
"""
from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger("kanida.zerodha_feed")


def get_ltp(tickers: list[str]) -> dict[str, Optional[float]]:
    """
    Return last traded prices for the given NSE ticker list.
    Returns {ticker: price_or_None}.
    Logs a warning (not silent) if token is missing/expired.
    """
    prices: dict[str, Optional[float]] = {t: None for t in tickers}
    if not tickers:
        return prices

    try:
        from services.kite_auth import get_kite_client, KiteAuthError
        kite = get_kite_client()
    except Exception as e:
        log.warning("Kite client unavailable for LTP fetch: %s", e)
        return prices

    sym_map = {f"NSE:{t}": t for t in tickers}
    try:
        raw = kite.ltp(list(sym_map.keys()))
        for kite_sym, ticker in sym_map.items():
            entry = raw.get(kite_sym, {})
            ltp   = entry.get("last_price")
            if ltp is not None and float(ltp) > 0:
                prices[ticker] = round(float(ltp), 2)
    except Exception as e:
        log.warning("Kite LTP call failed: %s", e)

    return prices


def get_quote(ticker: str) -> Optional[dict]:
    """Return full OHLCV quote dict for one ticker."""
    try:
        from services.kite_auth import get_kite_client
        kite = get_kite_client()
    except Exception as e:
        log.warning("Kite client unavailable for quote fetch: %s", e)
        return None

    sym = f"NSE:{ticker.upper()}"
    try:
        raw  = kite.quote([sym])
        data = raw.get(sym, {})
        if not data:
            return None
        ohlc = data.get("ohlc", {})
        return {
            "ltp":        round(float(data.get("last_price", 0)), 2),
            "open":       round(float(ohlc.get("open",  0)), 2),
            "high":       round(float(ohlc.get("high",  0)), 2),
            "low":        round(float(ohlc.get("low",   0)), 2),
            "close":      round(float(ohlc.get("close", 0)), 2),
            "volume":     int(data.get("volume", 0)),
            "change":     round(float(data.get("net_change", 0)), 2),
            "change_pct": round(float(data.get("change",     0)), 2),
        }
    except Exception as e:
        log.warning("Kite quote failed for %s: %s", ticker, e)
        return None
