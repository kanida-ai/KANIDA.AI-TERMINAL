"""SQLite access layer.

Two rules for a 100-million-row intraday table:

  1. Never SELECT * from it. Stream it one symbol at a time.
  2. Never let the research loop touch it. Collapse intraday into one row per
     symbol-day ONCE (see intraday.py), cache that, and join on (date, symbol).

Your column names are almost certainly not the ones this engine expects, so
everything goes through an explicit ColumnMap. Run

    python run_engine.py inspect-db --db market.db

to print the schema and a suggested mapping.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field, asdict
from typing import Iterator
import json

import pandas as pd

CANONICAL = ["date", "symbol", "open", "high", "low", "close", "volume"]

# common aliases seen in the wild, used only to SUGGEST a mapping
_ALIASES = {
    "date": ["date", "dt", "timestamp", "datetime", "time", "ts", "trade_date", "bar_time"],
    "symbol": ["symbol", "ticker", "tradingsymbol", "instrument", "scrip", "name", "sym", "token"],
    "open": ["open", "o", "open_price", "op"],
    "high": ["high", "h", "high_price"],
    "low": ["low", "l", "low_price"],
    "close": ["close", "c", "close_price", "last_price", "ltp"],
    "volume": ["volume", "v", "vol", "qty", "quantity", "traded_qty"],
    "sector": ["sector", "industry", "sector_name", "gics_sector"],
}


@dataclass
class ColumnMap:
    """Maps YOUR column names onto the engine's canonical names."""
    date: str = "date"
    symbol: str = "symbol"
    open: str = "open"
    high: str = "high"
    low: str = "low"
    close: str = "close"
    volume: str = "volume"
    sector: str | None = None

    def select_clause(self) -> str:
        parts = [f'"{getattr(self, c)}" AS {c}' for c in CANONICAL]
        if self.sector:
            parts.append(f'"{self.sector}" AS sector')
        return ", ".join(parts)

    def rename_map(self) -> dict:
        m = {getattr(self, c): c for c in CANONICAL}
        if self.sector:
            m[self.sector] = "sector"
        return m


@dataclass
class DBConfig:
    db_path: str
    daily_table: str = "daily"
    intraday_table: str = "intraday"
    daily_cols: ColumnMap = field(default_factory=ColumnMap)
    intraday_cols: ColumnMap = field(default_factory=ColumnMap)
    sector_table: str | None = None      # optional: symbol -> sector lookup
    session_start: str = "09:15"
    session_end: str = "15:30"

    def to_json(self, path: str) -> None:
        d = asdict(self)
        with open(path, "w") as f:
            json.dump(d, f, indent=2)

    @staticmethod
    def from_json(path: str) -> "DBConfig":
        with open(path) as f:
            d = json.load(f)
        d["daily_cols"] = ColumnMap(**d["daily_cols"])
        d["intraday_cols"] = ColumnMap(**d["intraday_cols"])
        return DBConfig(**d)


# --------------------------------------------------------------------------- #
# inspection                                                                    #
# --------------------------------------------------------------------------- #
def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA cache_size = -200000")   # ~200MB page cache
    con.execute("PRAGMA temp_store = MEMORY")
    con.execute("PRAGMA mmap_size = 268435456")
    return con


def inspect_db(db_path: str, verbose: bool = True) -> dict:
    """Print tables, columns, row counts, indexes, and a suggested ColumnMap."""
    con = connect(db_path)
    out = {}
    tables = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    if verbose:
        print(f"\nDATABASE: {db_path}")
        print("=" * 68)
    for t in tables:
        cols = [r[1] for r in con.execute(f'PRAGMA table_info("{t}")')]
        try:
            n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        except sqlite3.Error:
            n = -1
        idx = [r[1] for r in con.execute(f'PRAGMA index_list("{t}")')]
        idx_cols = {}
        for i in idx:
            idx_cols[i] = [r[2] for r in con.execute(f'PRAGMA index_info("{i}")')]
        suggested = {}
        low = {c.lower(): c for c in cols}
        for canon, aliases in _ALIASES.items():
            for a in aliases:
                if a in low:
                    suggested[canon] = low[a]
                    break
        out[t] = {"columns": cols, "rows": n, "indexes": idx_cols,
                  "suggested_map": suggested}
        if verbose:
            print(f"\n  TABLE {t}   ({n:,} rows)")
            print(f"    columns : {', '.join(cols)}")
            print(f"    indexes : {idx_cols if idx_cols else 'NONE  <-- see warning below'}")
            miss = [c for c in CANONICAL if c not in suggested]
            print(f"    mapping : {suggested}")
            if miss:
                print(f"    UNMAPPED: {miss}  <-- set these by hand in db_config.json")

    if verbose:
        print("\n" + "=" * 68)
        print("PERFORMANCE NOTE")
        print("=" * 68)
        print("  An intraday table without an index on (symbol, <time column>) will")
        print("  force a full table scan for every symbol. At 500 symbols that is")
        print("  500 full scans. Create the index once:")
        print("\n    CREATE INDEX IF NOT EXISTS ix_intraday_sym_ts")
        print("      ON intraday (symbol, timestamp);\n")
        print("  It costs disk and a few minutes. It saves hours.")
    con.close()
    return out


def suggest_config(db_path: str) -> DBConfig:
    """Best-effort DBConfig from schema sniffing. Always eyeball the result."""
    info = inspect_db(db_path, verbose=False)
    daily_t, intra_t = None, None
    best_small, best_big = -1, -1
    for t, meta in info.items():
        s = meta["suggested_map"]
        if not all(c in s for c in ["date", "symbol", "close"]):
            continue
        n = meta["rows"]
        if n > best_big:
            best_big, intra_t = n, t
    for t, meta in info.items():
        s = meta["suggested_map"]
        if not all(c in s for c in ["date", "symbol", "close"]) or t == intra_t:
            continue
        if meta["rows"] > best_small:
            best_small, daily_t = meta["rows"], t
    cfg = DBConfig(db_path=db_path,
                   daily_table=daily_t or "daily",
                   intraday_table=intra_t or "intraday")
    if daily_t:
        cfg.daily_cols = ColumnMap(**{k: v for k, v in info[daily_t]["suggested_map"].items()
                                      if k in ColumnMap.__dataclass_fields__})
    if intra_t:
        cfg.intraday_cols = ColumnMap(**{k: v for k, v in info[intra_t]["suggested_map"].items()
                                         if k in ColumnMap.__dataclass_fields__})
    return cfg


# --------------------------------------------------------------------------- #
# reads                                                                         #
# --------------------------------------------------------------------------- #
def load_daily(db: DBConfig, symbols: list[str] | None = None,
               start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """Load the daily panel. 500 symbols x 1,100 days is ~550k rows: trivial."""
    con = connect(db.db_path)
    cm = db.daily_cols
    q = f'SELECT {cm.select_clause()} FROM "{db.daily_table}"'
    where, params = [], []
    if symbols:
        where.append(f'"{cm.symbol}" IN ({",".join("?" * len(symbols))})')
        params += list(symbols)
    if start:
        where.append(f'"{cm.date}" >= ?'); params.append(start)
    if end:
        where.append(f'"{cm.date}" <= ?'); params.append(end)
    if where:
        q += " WHERE " + " AND ".join(where)
    df = pd.read_sql_query(q, con, params=params)
    con.close()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    if "sector" not in df.columns:
        df["sector"] = "UNKNOWN"
    return df.sort_values(["symbol", "date"]).reset_index(drop=True)


def list_symbols(db: DBConfig, table: str | None = None) -> list[str]:
    con = connect(db.db_path)
    t = table or db.daily_table
    cm = db.daily_cols if t == db.daily_table else db.intraday_cols
    syms = [r[0] for r in con.execute(f'SELECT DISTINCT "{cm.symbol}" FROM "{t}"')]
    con.close()
    return sorted(syms)


def iter_intraday(db: DBConfig, symbols: list[str] | None = None,
                  start: str | None = None, end: str | None = None,
                  resample: str | None = None) -> Iterator[tuple[str, pd.DataFrame]]:
    """Yield (symbol, intraday DataFrame) one symbol at a time.

    One symbol of 1-minute data over two years is ~200k rows (~10MB). The whole
    table at 500 symbols is ~100M rows and will not fit in memory. Stream it.

    `resample` e.g. '5min' aggregates on the fly, so you can keep 1-minute data
    on disk and research at 5-minute resolution.
    """
    cm = db.intraday_cols
    syms = symbols or list_symbols(db, db.intraday_table)
    con = connect(db.db_path)
    base = f'SELECT {cm.select_clause()} FROM "{db.intraday_table}" WHERE "{cm.symbol}" = ?'
    if start:
        base += f' AND "{cm.date}" >= \'{start}\''
    if end:
        base += f' AND "{cm.date}" <= \'{end}\''
    base += f' ORDER BY "{cm.date}"'
    try:
        for s in syms:
            df = pd.read_sql_query(base, con, params=[s])
            if df.empty:
                continue
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
            if resample:
                df = (df.resample(resample, label="left", closed="left")
                        .agg({"symbol": "first", "open": "first", "high": "max",
                              "low": "min", "close": "last", "volume": "sum"})
                        .dropna(subset=["close"]))
            yield s, df.reset_index()
    finally:
        con.close()


def ensure_indexes(db: DBConfig) -> None:
    """Create the indexes that make per-symbol streaming fast. Run once."""
    con = connect(db.db_path)
    for tbl, cm in [(db.daily_table, db.daily_cols),
                    (db.intraday_table, db.intraday_cols)]:
        name = f"ix_{tbl}_sym_dt"
        sql = f'CREATE INDEX IF NOT EXISTS "{name}" ON "{tbl}" ("{cm.symbol}", "{cm.date}")'
        print(f"  {sql}")
        con.execute(sql)
    con.commit()
    con.close()
    print("  done")
