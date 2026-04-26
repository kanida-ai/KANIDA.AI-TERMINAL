"""
Zerodha Kite Connect price feed for NSE stocks.

Reads credentials from config/.env (KITE_API_KEY / KITE_ACCESS_TOKEN).
Accepts a dynamic list of tickers — no hardcoded symbol table.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).parent.parent.parent
_ENV  = _ROOT / "config" / ".env"


def _load_env() -> None:
    if not _ENV.exists():
        return
    for line in _ENV.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _creds() -> tuple[str, str]:
    _load_env()
    return (
        os.getenv("KITE_API_KEY", ""),
        os.getenv("KITE_ACCESS_TOKEN", ""),
    )


def get_ltp(tickers: list[str]) -> dict[str, Optional[float]]:
    """
    Return last traded prices for the given NSE ticker list.
    tickers: plain NSE symbols, e.g. ['SBIN', 'RELIANCE']
    Returns {ticker: price_or_None}.
    """
    prices: dict[str, Optional[float]] = {t: None for t in tickers}
    if not tickers:
        return prices

    api_key, access_tok = _creds()
    if not api_key or not access_tok:
        return prices

    # Build exchange:symbol list for Kite LTP endpoint
    sym_map = {f"NSE:{t}": t for t in tickers}

    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_tok)

        # Kite allows up to 500 symbols per LTP call
        sym_list = list(sym_map.keys())
        raw = kite.ltp(sym_list)

        for kite_sym, ticker in sym_map.items():
            entry = raw.get(kite_sym, {})
            ltp = entry.get("last_price")
            if ltp is not None and float(ltp) > 0:
                prices[ticker] = round(float(ltp), 2)
    except Exception:
        pass

    return prices


def get_quote(ticker: str) -> Optional[dict]:
    """Return full OHLCV quote dict for one ticker."""
    api_key, access_tok = _creds()
    if not api_key or not access_tok:
        return None

    sym = f"NSE:{ticker.upper()}"
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_tok)
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
    except Exception:
        return None
