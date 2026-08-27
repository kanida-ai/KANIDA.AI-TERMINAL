"""
Chart Agent · OHLCV loader.

Point-in-time data access for the detectors + evidence. Today this reads daily bars from the R&D
SQLite DB so the ported logic is testable in the product immediately.

  [BUILT] SQLite fallback — env ``AGENT_CHART_DB`` (default the R&D kanida.db), table ``ohlc_daily``.
  [SPEC]  Cloud feeds / S3 daily Parquet / the platform market-data service (docs §2 topology).
          When wired, this module swaps its source but keeps the same (symbol -> DataFrame) contract;
          detectors and evidence are unchanged.

Adjustment policy (v3 §2): kanida.db stores corporate-action-adjusted, dividend-unadjusted prices
(matches TradingView). Detectors depend on this — raw vs adjusted prices produce different levels.
"""
from __future__ import annotations
import os
import sqlite3
from functools import lru_cache
import pandas as pd

DEFAULT_DB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
NIFTY = "NIFTY 50"

# A small, liquid default universe for scan() — full nifty500 / survivorship-clean set is [SPEC].
DEFAULT_UNIVERSE = [
    "RELIANCE", "INFY", "SBIN", "TCS", "HDFCBANK", "ICICIBANK", "AXISBANK", "KOTAKBANK",
    "LT", "ITC", "TITAN", "MARUTI", "SUNPHARMA", "TATAMOTORS", "TATASTEEL", "HINDALCO",
]


def db_path() -> str:
    return os.environ.get("AGENT_CHART_DB", DEFAULT_DB)


def db_available() -> bool:
    return os.path.exists(db_path())


@lru_cache(maxsize=256)
def load_daily(symbol: str) -> pd.DataFrame:
    """Daily OHLCV for one symbol, date-indexed, with an aligned NIFTY close column (for regime
    context). Read-only. Raises if the DB or symbol is absent — callers guard/skip."""
    path = db_path()
    con = sqlite3.connect(f"file:{os.path.abspath(path)}?mode=ro", uri=True)
    try:
        q = ("SELECT substr(bar_time,1,10) date, open, high, low, close, volume "
             "FROM ohlc_daily WHERE symbol=? ORDER BY bar_time")
        df = pd.read_sql_query(q, con, params=(symbol,))
        nf = pd.read_sql_query(
            "SELECT substr(bar_time,1,10) date, close FROM ohlc_daily WHERE symbol=? ORDER BY bar_time",
            con, params=(NIFTY,))
    finally:
        con.close()
    if df.empty:
        raise ValueError(f"no daily bars for {symbol!r} in {path}")
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    if not nf.empty:
        nf["date"] = pd.to_datetime(nf["date"])
        nf = nf.set_index("date")["close"]
        df["nifty"] = nf.reindex(df.index).ffill()
    else:
        df["nifty"] = float("nan")
    return df.dropna(subset=["open", "high", "low", "close"])
