"""
Migration: add ohlc_weekly table to kanida_quant.db.
Run once: python data/db/migrate_add_weekly.py
"""
from __future__ import annotations
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "kanida_quant.db"

SQL = """
CREATE TABLE IF NOT EXISTS ohlc_weekly (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    market      TEXT    NOT NULL,
    ticker      TEXT    NOT NULL,
    week_start  TEXT    NOT NULL,   -- Monday of the week  'YYYY-MM-DD'
    open        REAL    NOT NULL,
    high        REAL    NOT NULL,
    low         REAL    NOT NULL,
    close       REAL    NOT NULL,
    volume      INTEGER NOT NULL,
    source      TEXT    NOT NULL,
    quality_flag TEXT   DEFAULT 'ok',
    ingested_at TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(market, ticker, week_start)
);
CREATE INDEX IF NOT EXISTS idx_ohlcw_stock_date ON ohlc_weekly(market, ticker, week_start DESC);
"""

con = sqlite3.connect(DB_PATH)
con.executescript(SQL)
con.commit()
con.close()
print("ohlc_weekly table ready.")
