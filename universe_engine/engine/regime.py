"""
NIFTY 60-day realised volatility regime.
Buckets each trading day into low_vol / mid_vol / high_vol based on annualised vol.
"""
from __future__ import annotations
import math
import sqlite3
from typing import Dict


def compute_realised_vol_map(con: sqlite3.Connection, lookback_days: int = 60,
                              symbol: str = "NIFTY50") -> Dict[str, float]:
    """Return {trade_date: annualised_vol_%} from rolling lookback-day realised vol."""
    rows = con.execute(
        "SELECT trade_date, close FROM ohlc_daily WHERE symbol = ? ORDER BY trade_date",
        (symbol,)
    ).fetchall()
    if not rows:
        return {}
    closes = {r[0]: r[1] for r in rows}
    dates  = sorted(closes.keys())
    out: Dict[str, float] = {}
    for i in range(lookback_days, len(dates)):
        rets = []
        for j in range(i - lookback_days + 1, i + 1):
            p1 = closes[dates[j-1]]; p2 = closes[dates[j]]
            if p1 and p2 and p1 > 0:
                rets.append(math.log(p2 / p1))
        if rets:
            mu = sum(rets) / len(rets)
            sd = math.sqrt(sum((x - mu) ** 2 for x in rets) / len(rets))
            out[dates[i]] = sd * math.sqrt(252) * 100
    return out


def classify(vol_pct: float, low_max: float = 10.7, high_min: float = 13.4) -> str:
    if vol_pct is None: return "unknown"
    if vol_pct < low_max:  return "low_vol"
    if vol_pct > high_min: return "high_vol"
    return "mid_vol"


def write_regime_calendar(con: sqlite3.Connection, vol_map: Dict[str, float],
                          low_max: float = 10.7, high_min: float = 13.4) -> int:
    """Persist regime calendar to DB."""
    rows = []
    for d, v in sorted(vol_map.items()):
        rows.append((d, v, classify(v, low_max, high_min)))
    con.execute("DELETE FROM regime_calendar")
    con.executemany(
        "INSERT INTO regime_calendar (trade_date, realised_vol_pct, regime) VALUES (?, ?, ?)",
        rows
    )
    con.commit()
    return len(rows)


def regime_for(vol_map: Dict[str, float], date_str: str,
               low_max: float = 10.7, high_min: float = 13.4) -> str:
    """Return regime for a given date; falls back to nearest preceding day if exact missing."""
    v = vol_map.get(date_str[:10])
    if v is None:
        for d in sorted(vol_map.keys(), reverse=True):
            if d <= date_str[:10]:
                v = vol_map[d]; break
    return classify(v, low_max, high_min) if v is not None else "unknown"
