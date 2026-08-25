"""
Data layer for the Agent Builder backend. Reads OHLCV as wide (date x symbol) matrices from Parquet —
LOCAL directory OR S3 — with a SQLite fallback so it runs before the S3 move. Cached in-process.

Canonical Parquet schema (one row per stock-day, partitioned by symbol):
    symbol TEXT, date DATE, open, high, low, close, volume
Point the service at data via env:
    AGENT_DATA_URI = s3://your-bucket/kanida/daily/         (or a local dir of parquet)
    AGENT_DATA_1MIN_URI = s3://your-bucket/kanida/1min/     (optional, for the 1-min tier)
    AGENT_SQLITE_FALLBACK = C:\\...\\db\\kanida.db          (used only if no Parquet URI is set)
"""
from __future__ import annotations
import os, functools
import numpy as np, pandas as pd

DATA_URI = os.environ.get("AGENT_DATA_URI", "")
SQLITE_FALLBACK = os.environ.get("AGENT_SQLITE_FALLBACK", "")
NIFTY_SYMBOL = os.environ.get("AGENT_NIFTY_SYMBOL", "NIFTY 50")


def _read_parquet_wide(uri: str) -> pd.DataFrame:
    """Return long frame [symbol,date,open,high,low,close,volume] from a parquet dir/glob (local or s3://)."""
    import duckdb
    con = duckdb.connect()
    if uri.startswith("s3://"):
        region = os.environ.get("AWS_REGION", "ap-south-1")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        try:
            # Pick up the ECS task-role / instance credentials automatically (duckdb aws ext).
            con.execute("INSTALL aws; LOAD aws;")
            con.execute(f"CREATE SECRET s3sec (TYPE S3, PROVIDER credential_chain, REGION '{region}')")
        except Exception:
            con.execute(f"SET s3_region='{region}'")   # fallback: relies on env AWS_* keys if present
    glob = uri.rstrip("/") + "/**/*.parquet"
    return con.execute(
        f"SELECT symbol, CAST(date AS DATE) date, open, high, low, close, volume "
        f"FROM read_parquet('{glob}', hive_partitioning=1) ORDER BY symbol, date"
    ).df()


def _read_sqlite_long(path: str) -> pd.DataFrame:
    import sqlite3
    con = sqlite3.connect("file:" + path.replace("\\", "/") + "?mode=ro", uri=True)
    df = pd.read_sql_query(
        "SELECT symbol, substr(bar_time,1,10) date, open, high, low, close, volume "
        "FROM ohlc_daily ORDER BY symbol, bar_time", con)
    con.close()
    df["date"] = pd.to_datetime(df["date"])
    return df


@functools.lru_cache(maxsize=1)
def _long() -> pd.DataFrame:
    if DATA_URI:
        df = _read_parquet_wide(DATA_URI)
    elif SQLITE_FALLBACK and os.path.exists(SQLITE_FALLBACK):
        df = _read_sqlite_long(SQLITE_FALLBACK)
    else:
        raise RuntimeError("No data source: set AGENT_DATA_URI (parquet/S3) or AGENT_SQLITE_FALLBACK (kanida.db)")
    df["date"] = pd.to_datetime(df["date"])
    return df


@functools.lru_cache(maxsize=1)
def wide():
    """Return dict of wide (date x symbol) DataFrames o/h/l/c/v, plus the NIFTY close series aligned to dates."""
    df = _long()
    stk = df[df["symbol"] != NIFTY_SYMBOL]
    piv = lambda col: stk.pivot_table(index="date", columns="symbol", values=col, aggfunc="last").sort_index()
    o, h, l, c, v = piv("open"), piv("high"), piv("low"), piv("close"), piv("volume")
    nf = df[df["symbol"] == NIFTY_SYMBOL].set_index("date")["close"].reindex(c.index).ffill()
    return {"o": o, "h": h, "l": l, "c": c, "v": v, "nifty": nf}


def n_symbols() -> int:
    return wide()["c"].shape[1]


def n_bars() -> int:
    return wide()["c"].shape[0]
