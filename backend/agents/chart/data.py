"""
Chart Agent · OHLCV loader.

Point-in-time daily bars for the detectors + evidence, from either source (same symbol->DataFrame
contract; detectors + evidence unchanged):

  [BUILT] S3 / local Parquet via duckdb — env ``AGENT_DATA_URI`` (e.g. s3://.../kanida/daily/).
          This is the CLOUD path and is the SAME daily source the Agent Builder reads in prod
          (the container entrypoint already sets AGENT_DATA_URI), so no new cloud config is needed.
  [BUILT] SQLite fallback — env ``AGENT_CHART_DB`` (or ``AGENT_SQLITE_FALLBACK``), table
          ``ohlc_daily`` — for local dev / tests.

Precedence: if AGENT_DATA_URI is set -> Parquet; else the SQLite path if it exists. Per-symbol reads
(partition-pruned on Parquet), cached in-process, so an interactive scan stays responsive.

Adjustment policy (v3 §2): corporate-action-adjusted, dividend-unadjusted prices (matches
TradingView). Detectors depend on this — raw vs adjusted prices produce different levels.
"""
from __future__ import annotations
import os
import sqlite3
from functools import lru_cache
import pandas as pd

DEFAULT_DB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
NIFTY = os.environ.get("AGENT_NIFTY_SYMBOL", "NIFTY 50")

# A small, liquid default universe for scan() — full nifty500 / survivorship-clean set is [SPEC].
DEFAULT_UNIVERSE = [
    "RELIANCE", "INFY", "SBIN", "TCS", "HDFCBANK", "ICICIBANK", "AXISBANK", "KOTAKBANK",
    "LT", "ITC", "TITAN", "MARUTI", "SUNPHARMA", "TATAMOTORS", "TATASTEEL", "HINDALCO",
]


# --------------------------------------------------------------------------- source resolution
def _data_uri() -> str:
    return os.environ.get("AGENT_DATA_URI", "")


def _sqlite_path() -> str:
    return os.environ.get("AGENT_CHART_DB") or os.environ.get("AGENT_SQLITE_FALLBACK") or DEFAULT_DB


def db_path() -> str:
    """The active source (Parquet URI wins over the SQLite path)."""
    return _data_uri() or _sqlite_path()


def db_available() -> bool:
    return bool(_data_uri()) or os.path.exists(_sqlite_path())


# --------------------------------------------------------------------------- readers (per symbol)
def _parquet_symbol(symbol: str, cols: str = "open, high, low, close, volume") -> pd.DataFrame:
    import duckdb
    uri = _data_uri()
    con = duckdb.connect()
    try:
        if uri.startswith("s3://"):
            region = os.environ.get("AWS_REGION", "ap-south-1")
            con.execute("INSTALL httpfs; LOAD httpfs;")
            try:  # ECS task-role credentials via the duckdb aws extension
                con.execute("INSTALL aws; LOAD aws;")
                con.execute(f"CREATE SECRET s3sec (TYPE S3, PROVIDER credential_chain, REGION '{region}')")
            except Exception:  # noqa: BLE001 — fall back to env AWS_* keys if the ext/secret is unavailable
                con.execute(f"SET s3_region='{region}'")
        glob = uri.rstrip("/") + "/**/*.parquet"
        return con.execute(
            f"SELECT CAST(date AS DATE) date, {cols} FROM read_parquet('{glob}', hive_partitioning=1) "
            f"WHERE symbol=? ORDER BY date", [symbol]).df()
    finally:
        con.close()


def _sqlite_symbol(symbol: str, cols: str = "open, high, low, close, volume") -> pd.DataFrame:
    path = _sqlite_path()
    con = sqlite3.connect(f"file:{os.path.abspath(path)}?mode=ro", uri=True)
    try:
        return pd.read_sql_query(
            f"SELECT substr(bar_time,1,10) date, {cols} FROM ohlc_daily WHERE symbol=? ORDER BY bar_time",
            con, params=(symbol,))
    finally:
        con.close()


def _read_symbol(symbol: str, cols: str = "open, high, low, close, volume") -> pd.DataFrame:
    return _parquet_symbol(symbol, cols) if _data_uri() else _sqlite_symbol(symbol, cols)


# --------------------------------------------------------------------------- public API
@lru_cache(maxsize=1)
def _nifty_close() -> "pd.Series | None":
    """NIFTY daily close for the regime column, from whichever source is active."""
    try:
        nf = _read_symbol(NIFTY, "close")
    except Exception:  # noqa: BLE001
        return None
    if nf.empty:
        return None
    # Normalize to ns so both sources yield an identical index (Parquet's DATE cast lands as us).
    nf["date"] = pd.to_datetime(nf["date"]).astype("datetime64[ns]")
    return nf.set_index("date")["close"].sort_index()


@lru_cache(maxsize=512)
def load_daily(symbol: str) -> pd.DataFrame:
    """Daily OHLCV for one symbol, date-indexed, with an aligned NIFTY close column (regime context).
    Read-only. Raises if the symbol is absent — callers guard/skip."""
    df = _read_symbol(symbol)
    if df.empty:
        raise ValueError(f"no daily bars for {symbol!r} in {db_path()}")
    # Normalize to ns so Parquet (DATE->us) and SQLite (->ns) produce an identical date index —
    # downstream point-in-time resolution (_as_of_idx searchsort, recency Timedelta math) depends
    # on the two sources being indistinguishable.
    df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")
    df = df.set_index("date").sort_index()
    nf = _nifty_close()
    df["nifty"] = nf.reindex(df.index).ffill() if nf is not None else float("nan")
    return df.dropna(subset=["open", "high", "low", "close"])
