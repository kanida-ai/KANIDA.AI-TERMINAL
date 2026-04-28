"""
KANIDA Signals — Backfill from paper_ledger
============================================
Reads kanida_fingerprints.db (read-only) and populates:
  - signal_events      (one row per distinct strategy fire)
  - signal_outcomes    (partial: ret_15d + win_15d from forward_15d_ret)

trend_state_at_signal is left NULL here and filled later by
trend_state.py after OHLC data is synced.

Full multi-horizon outcomes (ret_1d..ret_30d, mfe, mae) are also
filled later by outcomes.py once OHLC is available.
"""

import sqlite3
import os
import sys
import time
from datetime import datetime, timezone

from .db import get_conn, SIGNALS_DB_PATH, LEGACY_DB_PATH


# ──────────────────────────────────────────────────────────────────
# SIGNAL EVENTS
# ──────────────────────────────────────────────────────────────────

_INSERT_SIGNAL_EVENTS = """
INSERT OR IGNORE INTO signal_events
    (ticker, market, timeframe, signal_date, strategy_name, bias,
     entry_price, trend_state, trend_strength, source, detected_at)
SELECT DISTINCT
    pl.ticker,
    pl.market,
    pl.timeframe,
    pl.signal_date,
    pl.strategy_name,
    pl.bias,
    pl.entry_price,
    NULL   AS trend_state,
    NULL   AS trend_strength,
    'backfill_paper_ledger' AS source,
    COALESCE(pl.logged_at, pl.signal_date || ' 00:00:00') AS detected_at
FROM legacy.paper_ledger pl
WHERE pl.market = :market
  AND pl.entry_price IS NOT NULL
  AND pl.strategy_name IS NOT NULL
  AND pl.signal_date IS NOT NULL;
"""

# ──────────────────────────────────────────────────────────────────
# SIGNAL OUTCOMES  (partial — ret_15d + win_15d only)
# ──────────────────────────────────────────────────────────────────

_INSERT_SIGNAL_OUTCOMES = """
INSERT OR IGNORE INTO signal_outcomes
    (signal_event_id, ticker, market, timeframe, strategy_name, bias,
     signal_date, entry_price, trend_state_at_signal,
     ret_15d, win_15d,
     is_complete, source, measured_at)
SELECT
    se.id,
    pl.ticker,
    pl.market,
    pl.timeframe,
    pl.strategy_name,
    pl.bias,
    pl.signal_date,
    pl.entry_price,
    NULL  AS trend_state_at_signal,
    pl.forward_15d_ret  AS ret_15d,
    pl.win              AS win_15d,
    CASE WHEN pl.status NOT IN ('OPEN', 'open') THEN 1 ELSE 0 END AS is_complete,
    'backfill'          AS source,
    COALESCE(pl.logged_at, pl.signal_date || ' 00:00:00') AS measured_at
FROM legacy.paper_ledger pl
JOIN signal_events se
  ON  se.ticker       = pl.ticker
  AND se.market       = pl.market
  AND se.timeframe    = pl.timeframe
  AND se.signal_date  = pl.signal_date
  AND se.strategy_name = pl.strategy_name
  AND se.bias         = pl.bias
WHERE pl.market = :market
  AND pl.forward_15d_ret IS NOT NULL;
"""


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────

def run_backfill(
    market: str = "NSE",
    signals_db: str = SIGNALS_DB_PATH,
    legacy_db: str = LEGACY_DB_PATH,
) -> None:
    if not os.path.exists(legacy_db):
        print(f"ERROR: Legacy DB not found: {legacy_db}")
        sys.exit(1)
    if not os.path.exists(signals_db):
        print(f"ERROR: Signals DB not found: {signals_db}")
        print("Run init_signals_db.py first.")
        sys.exit(1)

    print(f"\nKANIDA — Signal Events Backfill")
    print(f"  Source : {legacy_db}")
    print(f"  Target : {signals_db}")
    print(f"  Market : {market}\n")

    conn = get_conn(signals_db)
    conn.execute(f"ATTACH DATABASE '{legacy_db}' AS legacy")

    params = {"market": market}

    # ── Step 1: signal_events ──────────────────────────────────────
    print("Step 1: Inserting signal_events from paper_ledger...")
    t0 = time.time()
    with conn:
        conn.execute(_INSERT_SIGNAL_EVENTS, params)
    count_events = conn.execute(
        "SELECT COUNT(*) FROM signal_events WHERE market = ?", (market,)
    ).fetchone()[0]
    print(f"  Done in {time.time()-t0:.1f}s — {count_events:,} signal_events rows")

    # ── Step 2: signal_outcomes (partial) ─────────────────────────
    print("\nStep 2: Inserting signal_outcomes (ret_15d + win_15d)...")
    t0 = time.time()
    with conn:
        conn.execute(_INSERT_SIGNAL_OUTCOMES, params)
    count_outcomes = conn.execute(
        "SELECT COUNT(*) FROM signal_outcomes WHERE market = ?", (market,)
    ).fetchone()[0]
    complete = conn.execute(
        "SELECT COUNT(*) FROM signal_outcomes WHERE market = ? AND is_complete = 1", (market,)
    ).fetchone()[0]
    print(f"  Done in {time.time()-t0:.1f}s — {count_outcomes:,} outcome rows ({complete:,} complete)")

    # ── Validation ─────────────────────────────────────────────────
    print("\n-- Validation ------------------------------------------")
    rows = conn.execute("""
        SELECT timeframe, bias, COUNT(*) AS cnt
        FROM signal_events WHERE market = ?
        GROUP BY timeframe, bias ORDER BY timeframe, cnt DESC
    """, (market,)).fetchall()
    print(f"\n{'Timeframe':<12} {'Bias':<12} {'Events':>10}")
    print("-" * 36)
    for r in rows:
        print(f"  {r[0]:<10} {r[1]:<12} {r[2]:>10,}")

    date_range = conn.execute(
        "SELECT MIN(signal_date), MAX(signal_date) FROM signal_events WHERE market = ?",
        (market,)
    ).fetchone()
    print(f"\n  Date range : {date_range[0]}  →  {date_range[1]}")

    distinct_strats = conn.execute(
        "SELECT COUNT(DISTINCT strategy_name) FROM signal_events WHERE market = ?", (market,)
    ).fetchone()[0]
    distinct_tickers = conn.execute(
        "SELECT COUNT(DISTINCT ticker) FROM signal_events WHERE market = ?", (market,)
    ).fetchone()[0]
    print(f"  Tickers    : {distinct_tickers}")
    print(f"  Strategies : {distinct_strats}")

    conn.close()
    print("\nBackfill complete.")
    print("Next: run run_ohlc_sync.py to fetch price history, then run_calibration.py")
