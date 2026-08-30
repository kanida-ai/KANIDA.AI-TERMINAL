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
def _duckdb_con():
    """A duckdb connection with S3 credentials wired the same way for every Parquet read (per-symbol
    reads AND the full-universe panel), so the cloud path is configured in exactly one place."""
    import duckdb
    uri = _data_uri()
    con = duckdb.connect()
    if uri.startswith("s3://"):
        region = os.environ.get("AWS_REGION", "ap-south-1")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        try:  # ECS task-role credentials via the duckdb aws extension
            con.execute("INSTALL aws; LOAD aws;")
            con.execute(f"CREATE SECRET s3sec (TYPE S3, PROVIDER credential_chain, REGION '{region}')")
        except Exception:  # noqa: BLE001 — fall back to env AWS_* keys if the ext/secret is unavailable
            con.execute(f"SET s3_region='{region}'")
    return con


def _parquet_glob() -> str:
    return _data_uri().rstrip("/") + "/**/*.parquet"


def _parquet_symbol(symbol: str, cols: str = "open, high, low, close, volume") -> pd.DataFrame:
    con = _duckdb_con()
    try:
        return con.execute(
            f"SELECT CAST(date AS DATE) date, {cols} FROM read_parquet('{_parquet_glob()}', "
            f"hive_partitioning=1) WHERE symbol=? ORDER BY date", [symbol]).df()
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
# All caches are keyed on the ACTIVE SOURCE (db_path()) as well as the symbol, so swapping
# AGENT_DATA_URI / AGENT_CHART_DB in-process can NEVER serve bars/universe from the previous source
# (a real footgun in tooling/tests). The public names keep .cache_clear()/.cache_info() for callers.
@lru_cache(maxsize=8)
def _nifty_close_cached(source: str) -> "pd.Series | None":
    try:
        nf = _read_symbol(NIFTY, "close")
    except Exception:  # noqa: BLE001
        return None
    if nf.empty:
        return None
    # Normalize to ns so both sources yield an identical index (Parquet's DATE cast lands as us).
    nf["date"] = pd.to_datetime(nf["date"]).astype("datetime64[ns]")
    return nf.set_index("date")["close"].sort_index()


def _nifty_close() -> "pd.Series | None":
    """NIFTY daily close for the regime column, from whichever source is active (source-keyed cache)."""
    return _nifty_close_cached(db_path())


_nifty_close.cache_clear = _nifty_close_cached.cache_clear      # type: ignore[attr-defined]
_nifty_close.cache_info = _nifty_close_cached.cache_info        # type: ignore[attr-defined]


@lru_cache(maxsize=2048)
def _load_daily_cached(source: str, symbol: str) -> pd.DataFrame:
    df = _read_symbol(symbol)
    if df.empty:
        raise ValueError(f"no daily bars for {symbol!r} in {source}")
    # Normalize to ns so Parquet (DATE->us) and SQLite (->ns) produce an identical date index —
    # downstream point-in-time resolution (_as_of_idx searchsort, recency Timedelta math) depends
    # on the two sources being indistinguishable.
    df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")
    df = df.set_index("date").sort_index()
    nf = _nifty_close()
    df["nifty"] = nf.reindex(df.index).ffill() if nf is not None else float("nan")
    return df.dropna(subset=["open", "high", "low", "close"])


def load_daily(symbol: str) -> pd.DataFrame:
    """Daily OHLCV for one symbol, date-indexed, with an aligned NIFTY close column (regime context).
    Read-only, cached per (active source, symbol). Raises if the symbol is absent — callers guard/skip."""
    return _load_daily_cached(db_path(), symbol)


load_daily.cache_clear = _load_daily_cached.cache_clear        # type: ignore[attr-defined]
load_daily.cache_info = _load_daily_cached.cache_info          # type: ignore[attr-defined]


# ------------------------------------------------------------------- FULL-UNIVERSE panel (screener)
# The Chart Agent screener scans the whole daily source point-in-time. To do that in SECONDS it does
# NOT call load_daily 1500× (each a full-history read); it pulls ONE windowed panel (bars <= as_of,
# within a lookback window big enough that the windowed frame is INDISTINGUISHABLE from full history
# for the detector's 120-bar level window — see screener.LOOKBACK_DAYS) and splits it per symbol.
# Same symbol->DataFrame contract the detectors already consume; strictly <= as_of, so point-in-time
# is STRUCTURAL (a bar dated after as_of is never even read from the source).

@lru_cache(maxsize=8)
def _all_symbols_cached(source: str) -> tuple:
    if _data_uri():
        con = _duckdb_con()
        try:
            df = con.execute(
                f"SELECT DISTINCT symbol FROM read_parquet('{_parquet_glob()}', hive_partitioning=1) "
                f"WHERE symbol<>? ORDER BY symbol", [NIFTY]).df()
        finally:
            con.close()
    else:
        path = _sqlite_path()
        con = sqlite3.connect(f"file:{os.path.abspath(path)}?mode=ro", uri=True)
        try:
            df = pd.read_sql_query(
                "SELECT DISTINCT symbol FROM ohlc_daily WHERE symbol<>? ORDER BY symbol",
                con, params=(NIFTY,))
        finally:
            con.close()
    return tuple(str(s) for s in df["symbol"].tolist())


def all_symbols() -> tuple:
    """Every distinct symbol in the active daily source, minus NIFTY, sorted. Cached per active
    source. Returned as a tuple so it is hashable/cacheable; callers treat it as a sequence."""
    return _all_symbols_cached(db_path())


all_symbols.cache_clear = _all_symbols_cached.cache_clear       # type: ignore[attr-defined]
all_symbols.cache_info = _all_symbols_cached.cache_info         # type: ignore[attr-defined]


def _sqlite_panel(lo: str, hi: str) -> pd.DataFrame:
    path = _sqlite_path()
    con = sqlite3.connect(f"file:{os.path.abspath(path)}?mode=ro", uri=True)
    try:
        return pd.read_sql_query(
            "SELECT symbol, substr(bar_time,1,10) date, open,high,low,close,volume FROM ohlc_daily "
            "WHERE substr(bar_time,1,10)<=? AND substr(bar_time,1,10)>=? AND symbol<>? "
            "ORDER BY symbol, bar_time", con, params=(hi, lo, NIFTY))
    finally:
        con.close()


def _parquet_panel(lo: str, hi: str) -> pd.DataFrame:
    con = _duckdb_con()
    try:
        # CAST the date COLUMN in the WHERE too: under Parquet hive_partitioning `date` is VARCHAR,
        # and DuckDB refuses VARCHAR<=DATE ("Binder Error: Cannot compare VARCHAR and DATE"). SQLite
        # is lenient so the suite missed it — the DuckDB-over-Parquet panel test below now covers it.
        return con.execute(
            f"SELECT symbol, CAST(date AS DATE) date, open,high,low,close,volume "
            f"FROM read_parquet('{_parquet_glob()}', hive_partitioning=1) "
            f"WHERE CAST(date AS DATE)<=CAST(? AS DATE) AND CAST(date AS DATE)>=CAST(? AS DATE) AND symbol<>? "
            f"ORDER BY symbol, date", [hi, lo, NIFTY]).df()
    finally:
        con.close()


@lru_cache(maxsize=8)
def _sector_map_cached(source: str) -> tuple:
    """(symbol, sector) pairs from the SQLite ``instrument_labels`` table (a REAL sector source that
    ships in kanida.db — 22 sectors, ~500 F&O/nifty500 names). Returned as a tuple of pairs so it is
    hashable/cacheable. Guarded: if the table/column is absent (e.g. a Parquet-only source) returns ()
    so callers report sector concentration as UNAVAILABLE rather than fabricating it."""
    if _data_uri():
        return ()   # Parquet source has no instrument_labels table — honest miss
    path = _sqlite_path()
    if not os.path.exists(path):
        return ()
    try:
        con = sqlite3.connect(f"file:{os.path.abspath(path)}?mode=ro", uri=True)
        try:
            rows = con.execute(
                "SELECT symbol, sector FROM instrument_labels "
                "WHERE sector IS NOT NULL AND sector<>''").fetchall()
        finally:
            con.close()
    except Exception:  # noqa: BLE001 — table missing / db locked -> honest empty map
        return ()
    return tuple((str(s), str(sec)) for s, sec in rows)


def sector_map() -> dict:
    """{symbol -> sector} from the active source, or {} when no sector source exists (honest). Cached."""
    return dict(_sector_map_cached(db_path()))


sector_map.cache_clear = _sector_map_cached.cache_clear         # type: ignore[attr-defined]


def load_panel(as_of_date, lookback_days: int) -> dict:
    """Point-in-time OHLCV panel for the FULL universe: {symbol -> date-indexed OHLCV frame}, every
    bar dated <= as_of_date and >= (as_of - lookback_days). One windowed query, split per symbol.
    NIFTY column is intentionally omitted — the live-stage classifier (detect) never reads it, and
    leaving it off keeps the panel light. Source-agnostic (Parquet via duckdb, else SQLite)."""
    as_of = pd.Timestamp(as_of_date)
    lo = (as_of - pd.Timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")
    hi = as_of.strftime("%Y-%m-%d")
    long = _parquet_panel(lo, hi) if _data_uri() else _sqlite_panel(lo, hi)
    if long.empty:
        return {}
    long["date"] = pd.to_datetime(long["date"]).astype("datetime64[ns]")
    out: dict = {}
    for sym, g in long.groupby("symbol", sort=False):
        frame = (g.drop(columns=["symbol"]).set_index("date").sort_index()
                 .dropna(subset=["open", "high", "low", "close"]))
        out[str(sym)] = frame
    return out
