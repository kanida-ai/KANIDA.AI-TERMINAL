"""
Stock context service — enriched OHLCV statistics for a ticker.
Used by explainer and ticker profile endpoint.
"""
from __future__ import annotations

import os
import sqlite3
import statistics
from pathlib import Path
from typing import Optional

_HERE = Path(__file__).parent
DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"),
)


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def get_stock_context(ticker: str, market: str = "NSE") -> Optional[dict]:
    """
    Return enriched price context for a ticker:
    - Latest OHLC + volume
    - ATR14
    - SMA20, SMA50 position
    - Recent returns (1d, 5d, 20d)
    - Average volume vs recent volume
    """
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT trade_date, open, high, low, close, volume
            FROM ohlc_daily
            WHERE ticker = ? AND market = ? AND quality_flag = 'ok'
            ORDER BY trade_date DESC
            LIMIT 60
            """,
            (ticker, market),
        ).fetchall()

    if not rows:
        return None

    rows = [dict(r) for r in rows]
    closes = [r["close"] for r in rows]
    highs  = [r["high"]  for r in rows]
    lows   = [r["low"]   for r in rows]
    vols   = [r["volume"] for r in rows if r["volume"]]

    latest = rows[0]

    # ATR14
    atr14: Optional[float] = None
    if len(rows) >= 15:
        true_ranges = [
            max(highs[i] - lows[i], abs(highs[i] - closes[i + 1]), abs(lows[i] - closes[i + 1]))
            for i in range(14)
        ]
        atr14 = round(statistics.mean(true_ranges), 2)

    # SMAs
    sma20 = round(statistics.mean(closes[:20]), 2) if len(closes) >= 20 else None
    sma50 = round(statistics.mean(closes[:50]), 2) if len(closes) >= 50 else None

    def _ret(n: int) -> Optional[float]:
        if len(closes) > n:
            return round((closes[0] / closes[n] - 1) * 100, 2)
        return None

    avg_vol_20 = round(statistics.mean(vols[1:21]), 0) if len(vols) > 20 else None
    vol_ratio  = round(vols[0] / avg_vol_20, 2) if (avg_vol_20 and vols) else None

    return {
        "ticker":        ticker,
        "market":        market,
        "latest_date":   latest["trade_date"],
        "close":         latest["close"],
        "open":          latest["open"],
        "high":          latest["high"],
        "low":           latest["low"],
        "volume":        latest["volume"],
        "atr14":         atr14,
        "atr14_pct":     round(atr14 / latest["close"] * 100, 2) if atr14 else None,
        "sma20":         sma20,
        "sma50":         sma50,
        "above_sma20":   latest["close"] > sma20 if sma20 else None,
        "above_sma50":   latest["close"] > sma50 if sma50 else None,
        "return_1d":     _ret(1),
        "return_5d":     _ret(5),
        "return_20d":    _ret(20),
        "avg_volume_20": avg_vol_20,
        "volume_ratio":  vol_ratio,
    }
