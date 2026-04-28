"""
KANIDA.AI — SQLite → PostgreSQL migration script.

PRE-REQUISITES:
  1. Set DATABASE_URL env var:
       export DATABASE_URL=postgresql://user:pass@host:5432/kanida
  2. Install psycopg2:
       pip install psycopg2-binary
  3. Run from project root:
       python scripts/migrate_to_postgres.py

This is a ONE-TIME migration. It:
  - Creates all tables in PostgreSQL (with proper types)
  - Copies all data from SQLite in batches
  - Prints row counts for verification

After migration, update routers to use DATABASE_URL instead of KANIDA_DB_PATH.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT    = Path(__file__).parent.parent
SQLITE  = Path(os.environ.get("KANIDA_DB_PATH", ROOT / "data" / "db" / "kanida_quant.db"))
PG_URL  = os.environ.get("DATABASE_URL", "")

BATCH_SIZE = 1000

# PostgreSQL schema (TEXT → VARCHAR/TEXT, REAL → DOUBLE PRECISION, INTEGER → BIGINT)
SCHEMA = """
CREATE TABLE IF NOT EXISTS ohlc_daily (
    id          BIGSERIAL PRIMARY KEY,
    market      VARCHAR(10),
    ticker      VARCHAR(50),
    trade_date  DATE,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,
    source      VARCHAR(20),
    quality_flag VARCHAR(20),
    ingested_at TIMESTAMPTZ,
    UNIQUE (market, ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS snapshot_runs (
    id                  BIGSERIAL PRIMARY KEY,
    run_type            VARCHAR(50),
    status              VARCHAR(20),
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    learned_patterns    INTEGER,
    live_opportunities  INTEGER,
    tickers_processed   INTEGER,
    message             TEXT,
    run_date            DATE
);

CREATE TABLE IF NOT EXISTS live_opportunities (
    id                  BIGSERIAL PRIMARY KEY,
    snapshot_run_id     BIGINT REFERENCES snapshot_runs(id),
    market              VARCHAR(10),
    ticker              VARCHAR(50),
    direction           VARCHAR(10),
    target_move         DOUBLE PRECISION,
    forward_window      INTEGER,
    behavior_pattern    TEXT,
    occurrences         INTEGER,
    hits                INTEGER,
    display_probability DOUBLE PRECISION,
    credibility         VARCHAR(30),
    lift                DOUBLE PRECISION,
    opportunity_score   DOUBLE PRECISION,
    decision_score      DOUBLE PRECISION,
    tier                VARCHAR(30),
    latest_date         DATE,
    current_close       DOUBLE PRECISION,
    current_behavior    TEXT,
    current_atoms       TEXT,
    similarity          DOUBLE PRECISION,
    setup_summary       TEXT,
    decay_flag          INTEGER,
    created_at          TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS trade_log (
    id                BIGSERIAL PRIMARY KEY,
    market            VARCHAR(10),
    ticker            VARCHAR(50),
    trade_type        VARCHAR(20),
    direction         VARCHAR(10),
    signal_event_id   BIGINT,
    signal_date       DATE,
    entry_date        DATE,
    entry_price       DOUBLE PRECISION,
    target_price      DOUBLE PRECISION,
    stop_price        DOUBLE PRECISION,
    stop_type         VARCHAR(20),
    atr14_at_entry    DOUBLE PRECISION,
    atr_multiplier    DOUBLE PRECISION,
    exit_date         DATE,
    exit_price        DOUBLE PRECISION,
    exit_reason       VARCHAR(20),
    days_held         INTEGER,
    pnl_pct           DOUBLE PRECISION,
    pnl_abs           DOUBLE PRECISION,
    position_size_pct DOUBLE PRECISION,
    risk_reward_ratio DOUBLE PRECISION,
    notes             TEXT,
    created_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS pattern_library (
    id                    BIGSERIAL PRIMARY KEY,
    market                VARCHAR(10),
    ticker                VARCHAR(50),
    direction             VARCHAR(10),
    target_move           DOUBLE PRECISION,
    forward_window        INTEGER,
    pattern_size          INTEGER,
    behavior_pattern      TEXT,
    occurrences           INTEGER,
    hits                  INTEGER,
    baseline_probability  DOUBLE PRECISION,
    raw_probability       DOUBLE PRECISION,
    trusted_probability   DOUBLE PRECISION,
    display_probability   DOUBLE PRECISION,
    probability_ci_low    DOUBLE PRECISION,
    probability_ci_high   DOUBLE PRECISION,
    credibility           VARCHAR(30),
    lift                  DOUBLE PRECISION,
    avg_forward_return    DOUBLE PRECISION,
    recent_probability    DOUBLE PRECISION,
    stability             DOUBLE PRECISION,
    opportunity_score     DOUBLE PRECISION,
    decay_flag            INTEGER,
    tier                  VARCHAR(30),
    lifecycle_status      VARCHAR(20),
    first_seen_date       DATE,
    last_seen_date        DATE,
    created_at            TIMESTAMPTZ,
    updated_at            TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS execution_log (
    id                BIGSERIAL PRIMARY KEY,
    trade_log_id      BIGINT,
    ticker            VARCHAR(50),
    direction         VARCHAR(10),
    signal_date       DATE,
    entry_date        DATE,
    exec_code         VARCHAR(20),
    trade_taken       INTEGER,
    entry_window      VARCHAR(20),
    exec_notes        TEXT,
    prev_close        DOUBLE PRECISION,
    entry_open        DOUBLE PRECISION,
    entry_high        DOUBLE PRECISION,
    entry_low         DOUBLE PRECISION,
    entry_close       DOUBLE PRECISION,
    gap_pct           DOUBLE PRECISION,
    gap_category      VARCHAR(20),
    day_move_pct      DOUBLE PRECISION,
    day_range_pct     DOUBLE PRECISION,
    nifty_open        DOUBLE PRECISION,
    nifty_close       DOUBLE PRECISION,
    nifty_day_move    DOUBLE PRECISION,
    nifty_is_weak     INTEGER,
    rs_vs_nifty       DOUBLE PRECISION,
    blind_entry_price DOUBLE PRECISION,
    smart_entry_price DOUBLE PRECISION,
    exit_price        DOUBLE PRECISION,
    blind_pnl_pct     DOUBLE PRECISION,
    smart_pnl_pct     DOUBLE PRECISION,
    pnl_improvement   DOUBLE PRECISION,
    created_at        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS orders (
    id              BIGSERIAL PRIMARY KEY,
    ticker          VARCHAR(50) NOT NULL,
    market          VARCHAR(10) NOT NULL DEFAULT 'NSE',
    direction       VARCHAR(10) NOT NULL,
    order_type      VARCHAR(20) NOT NULL,
    quantity        INTEGER NOT NULL,
    price           DOUBLE PRECISION,
    trigger_price   DOUBLE PRECISION,
    product         VARCHAR(10) DEFAULT 'CNC',
    kite_order_id   VARCHAR(50),
    status          VARCHAR(20) DEFAULT 'PENDING',
    reject_reason   TEXT,
    opportunity_id  BIGINT,
    signal_date     DATE,
    placed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
"""

TABLES_TO_MIGRATE = [
    "ohlc_daily", "snapshot_runs", "live_opportunities",
    "trade_log", "pattern_library", "execution_log",
]


def main():
    if not PG_URL:
        print("ERROR: DATABASE_URL not set.")
        sys.exit(1)

    try:
        import psycopg2
    except ImportError:
        print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
        sys.exit(1)

    if not SQLITE.exists():
        print(f"ERROR: SQLite DB not found at {SQLITE}")
        sys.exit(1)

    print(f"Migrating {SQLITE} → {PG_URL[:40]}...")
    pg  = psycopg2.connect(PG_URL)
    sq  = sqlite3.connect(str(SQLITE))
    sq.row_factory = sqlite3.Row

    with pg.cursor() as cur:
        for stmt in SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        pg.commit()
    print("Schema created.")

    for table in TABLES_TO_MIGRATE:
        rows = sq.execute(f"SELECT * FROM [{table}]").fetchall()
        if not rows:
            print(f"  {table}: 0 rows (skipped)")
            continue

        cols = list(rows[0].keys())
        placeholders = ",".join(["%s"] * len(cols))
        col_list = ",".join(cols)
        insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        total = 0
        with pg.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = [tuple(r) for r in rows[i:i + BATCH_SIZE]]
                cur.executemany(insert_sql, batch)
                total += len(batch)
            pg.commit()
        print(f"  {table}: {total} rows migrated")

    sq.close()
    pg.close()
    print(f"\nMigration complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Next: set DATABASE_URL in Railway and update routers to use psycopg2.")


if __name__ == "__main__":
    main()
