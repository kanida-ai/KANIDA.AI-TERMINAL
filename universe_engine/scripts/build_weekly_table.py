"""
Build ohlc_weekly aggregate from ohlc_daily.

One SQL pass — pure local SQLite aggregation, no external calls.
Week starts on Monday (NSE convention).
"""
from __future__ import annotations
import sqlite3, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "db" / "kanida_universe.db"


SCHEMA = """
DROP TABLE IF EXISTS ohlc_weekly;
CREATE TABLE ohlc_weekly (
    symbol      TEXT NOT NULL,
    week_start  TEXT NOT NULL,                    -- Monday of the week
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      INTEGER,
    n_days      INTEGER,                          -- trading days in the week
    PRIMARY KEY (symbol, week_start)
);
CREATE INDEX idx_ohlc_weekly_date ON ohlc_weekly(week_start);
"""


AGGREGATE_SQL = """
INSERT INTO ohlc_weekly
    (symbol, week_start, open, high, low, close, volume, n_days)
WITH ranked AS (
    SELECT
        symbol,
        trade_date,
        open, high, low, close, volume,
        date(trade_date, 'weekday 0', '-6 days') AS week_start,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, date(trade_date, 'weekday 0', '-6 days')
            ORDER BY trade_date ASC
        ) AS rn_asc,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, date(trade_date, 'weekday 0', '-6 days')
            ORDER BY trade_date DESC
        ) AS rn_desc
    FROM ohlc_daily
    WHERE trade_date >= '2022-01-01'
)
SELECT
    symbol,
    week_start,
    MAX(CASE WHEN rn_asc = 1 THEN open END) AS open,
    MAX(high) AS high,
    MIN(low)  AS low,
    MAX(CASE WHEN rn_desc = 1 THEN close END) AS close,
    SUM(volume) AS volume,
    COUNT(*) AS n_days
FROM ranked
GROUP BY symbol, week_start
"""


def main():
    if not DB.exists():
        sys.exit(f"DB not found: {DB}")
    print(f"DB: {DB}")
    con = sqlite3.connect(DB, timeout=60.0)

    t0 = time.time()
    print("[weekly] (re)creating ohlc_weekly schema ...")
    con.executescript(SCHEMA)
    con.commit()

    print("[weekly] aggregating daily -> weekly ...")
    con.execute(AGGREGATE_SQL)
    con.commit()
    print(f"[weekly] done in {time.time()-t0:.1f}s")

    # Coverage
    n_rows, n_syms, mn, mx = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(week_start), MAX(week_start)
        FROM ohlc_weekly
    """).fetchone()
    print(f"\n[weekly] ohlc_weekly: {n_rows:,} rows, {n_syms} symbols, "
          f"{mn} -> {mx}")

    # Sanity: average weeks per symbol
    avg_weeks = n_rows / n_syms if n_syms else 0
    print(f"  Average weeks per symbol: {avg_weeks:.0f}")

    # Spot check: a known stock
    print("\n[weekly] Sample (RELIANCE last 5 weeks):")
    rows = con.execute("""
        SELECT week_start, open, high, low, close, volume, n_days
        FROM ohlc_weekly WHERE symbol='RELIANCE'
        ORDER BY week_start DESC LIMIT 5
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}  O={r[1]:.1f} H={r[2]:.1f} L={r[3]:.1f} C={r[4]:.1f} "
              f"V={r[5]:,} ({r[6]}d)")

    con.close()


if __name__ == "__main__":
    main()
