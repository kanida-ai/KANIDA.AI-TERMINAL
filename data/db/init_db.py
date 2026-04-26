"""
KANIDA.AI Terminal — Quant Intelligence Engine
Fresh database initializer.

Run once to create the clean database:
    python data/db/init_db.py

Creates kanida_quant.db with all required tables.
No legacy data is imported — this is a clean slate.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


DB_PATH = Path(__file__).parent / "kanida_quant.db"


SCHEMA = """
-- ============================================================
-- MARKET DATA  (verified sources: Kite Connect / Polygon.io)
-- ============================================================

CREATE TABLE IF NOT EXISTS ohlc_daily (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market          TEXT    NOT NULL,           -- 'NSE' | 'US'
    ticker          TEXT    NOT NULL,
    trade_date      TEXT    NOT NULL,           -- 'YYYY-MM-DD'
    open            REAL    NOT NULL,
    high            REAL    NOT NULL,
    low             REAL    NOT NULL,
    close           REAL    NOT NULL,
    volume          INTEGER NOT NULL,
    source          TEXT    NOT NULL,           -- 'kite' | 'polygon'
    quality_flag    TEXT    DEFAULT 'ok',       -- 'ok' | 'suspect' | 'rejected'
    ingested_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(market, ticker, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_ohlc_stock_date ON ohlc_daily(market, ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_ohlc_date ON ohlc_daily(trade_date);

-- ============================================================
-- INSTRUMENT UNIVERSE
-- ============================================================

CREATE TABLE IF NOT EXISTS instruments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market          TEXT    NOT NULL,           -- 'NSE' | 'US'
    ticker          TEXT    NOT NULL,
    name            TEXT,
    sector          TEXT,
    industry        TEXT,
    market_cap_tier TEXT,                       -- 'large' | 'mid' | 'small'
    is_active       INTEGER DEFAULT 1,
    kite_token      TEXT,                       -- Zerodha instrument token
    polygon_ticker  TEXT,                       -- Polygon.io ticker (may differ)
    added_at        TEXT    DEFAULT (datetime('now')),
    UNIQUE(market, ticker)
);

-- ============================================================
-- BEHAVIORAL FEATURES  (derived from OHLCV, rebuilt nightly)
-- ============================================================

CREATE TABLE IF NOT EXISTS behavior_features (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    trade_date      TEXT    NOT NULL,
    close           REAL,
    -- raw returns
    ret1            REAL,
    ret3            REAL,
    ret5            REAL,
    ret10           REAL,
    ret20           REAL,
    ret60           REAL,
    -- behavior atoms (bucketed labels)
    trend_20        TEXT,
    trend_60        TEXT,
    current_move    TEXT,
    flow            TEXT,
    candle          TEXT,
    volume_state    TEXT,
    volatility      TEXT,
    range_state     TEXT,
    ma_position     TEXT,
    ma_slope        TEXT,
    sr_state        TEXT,
    gap_state       TEXT,
    breakout_state  TEXT,
    -- composite strings
    behavior_atoms  TEXT,   -- JSON array of 'key:value' strings
    coarse_behavior TEXT,   -- pipe-separated top-5 atoms
    computed_at     TEXT    DEFAULT (datetime('now')),
    UNIQUE(market, ticker, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_feat_stock_date ON behavior_features(market, ticker, trade_date DESC);

-- ============================================================
-- PATTERN LIBRARY  (outcome-first learned patterns)
-- ============================================================

CREATE TABLE IF NOT EXISTS pattern_library (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    market              TEXT    NOT NULL,
    ticker              TEXT    NOT NULL,
    direction           TEXT    NOT NULL,       -- 'rally' | 'fall'
    target_move         REAL    NOT NULL,       -- e.g. 0.05
    forward_window      INTEGER NOT NULL,       -- days
    pattern_size        INTEGER NOT NULL,
    behavior_pattern    TEXT    NOT NULL,       -- 'atom_a + atom_b + ...'
    -- evidence stats
    occurrences         INTEGER NOT NULL,
    hits                INTEGER NOT NULL,
    baseline_probability    REAL,
    raw_probability         REAL,
    trusted_probability     REAL,
    display_probability     REAL,
    probability_ci_low      REAL,
    probability_ci_high     REAL,
    credibility         TEXT,                   -- 'strong' | 'solid' | 'thin_but_interesting' | 'exploratory'
    lift                REAL,
    avg_forward_return  REAL,
    recent_probability  REAL,
    stability           REAL,
    opportunity_score   REAL,
    decay_flag          INTEGER DEFAULT 0,
    tier                TEXT,                   -- 'high_conviction' | 'medium' | 'exploratory'
    -- lifecycle
    lifecycle_status    TEXT    DEFAULT 'active',  -- 'active' | 'watch' | 'retiring' | 'retired'
    first_seen_date     TEXT,
    last_seen_date      TEXT,
    created_at          TEXT    DEFAULT (datetime('now')),
    updated_at          TEXT    DEFAULT (datetime('now')),
    UNIQUE(market, ticker, direction, target_move, forward_window, behavior_pattern)
);
CREATE INDEX IF NOT EXISTS idx_pattern_stock ON pattern_library(market, ticker, direction, opportunity_score DESC);
CREATE INDEX IF NOT EXISTS idx_pattern_tier ON pattern_library(tier, credibility, opportunity_score DESC);

-- ============================================================
-- ATOM REGISTRY  (individual behavior atoms and their stats)
-- ============================================================

CREATE TABLE IF NOT EXISTS atom_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    atom_key        TEXT    NOT NULL,           -- e.g. 'trend_20:up'
    direction       TEXT    NOT NULL,
    target_move     REAL    NOT NULL,
    forward_window  INTEGER NOT NULL,
    occurrences     INTEGER NOT NULL,
    hits            INTEGER NOT NULL,
    raw_probability REAL,
    baseline        REAL,
    lift            REAL,
    updated_at      TEXT    DEFAULT (datetime('now')),
    UNIQUE(market, ticker, atom_key, direction, target_move, forward_window)
);

-- ============================================================
-- SIGNAL FIRING LOG  (every time a live pattern match fires)
-- ============================================================

CREATE TABLE IF NOT EXISTS signal_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    signal_date     TEXT    NOT NULL,           -- date the signal was detected
    direction       TEXT    NOT NULL,           -- 'rally' | 'fall'
    target_move     REAL    NOT NULL,
    forward_window  INTEGER NOT NULL,
    pattern_id      INTEGER REFERENCES pattern_library(id),
    behavior_pattern    TEXT,
    decision_score  REAL,
    opportunity_score   REAL,
    display_probability REAL,
    credibility     TEXT,
    tier            TEXT,
    current_close   REAL,
    current_behavior    TEXT,
    similarity      REAL,
    setup_summary   TEXT,
    source_run_id   INTEGER,
    fired_at        TEXT    DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_signal_stock_date ON signal_events(market, ticker, signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_signal_date_score ON signal_events(signal_date DESC, decision_score DESC);

-- ============================================================
-- FORWARD OUTCOMES  (what actually happened after a signal)
-- ============================================================

CREATE TABLE IF NOT EXISTS signal_outcomes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_event_id     INTEGER REFERENCES signal_events(id),
    market              TEXT    NOT NULL,
    ticker              TEXT    NOT NULL,
    signal_date         TEXT    NOT NULL,
    direction           TEXT    NOT NULL,
    target_move         REAL    NOT NULL,
    forward_window      INTEGER NOT NULL,
    entry_close         REAL,
    -- forward price action
    max_forward_return  REAL,
    min_forward_return  REAL,
    final_forward_return    REAL,
    -- outcome classification
    hit_target          INTEGER,                -- 1 | 0
    days_to_target      INTEGER,
    exit_reason         TEXT,                   -- 'tp_hit' | 'sl_hit' | 'expired' | 'manual'
    exit_price          REAL,
    exit_date           TEXT,
    pnl_pct             REAL,
    -- behavioral context at exit
    mae_pct             REAL,                   -- max adverse excursion
    mfe_pct             REAL,                   -- max favorable excursion
    post_exit_5d_pct    REAL,                   -- continued after exit
    continued_after_tp  INTEGER,               -- MPI flag
    resolved_at         TEXT    DEFAULT (datetime('now')),
    UNIQUE(signal_event_id)
);
CREATE INDEX IF NOT EXISTS idx_outcome_stock ON signal_outcomes(market, ticker, signal_date DESC);

-- ============================================================
-- TRADE LOG  (paper + live trades, manually or system-entered)
-- ============================================================

CREATE TABLE IF NOT EXISTS trade_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    trade_type      TEXT    DEFAULT 'paper',    -- 'paper' | 'live'
    direction       TEXT    NOT NULL,           -- 'long' | 'short'
    signal_event_id INTEGER REFERENCES signal_events(id),
    signal_date     TEXT,
    entry_date      TEXT,
    entry_price     REAL,
    target_price    REAL,
    stop_price      REAL,
    -- stop methodology
    stop_type       TEXT    DEFAULT 'atr',      -- 'atr' | 'fixed_pct' | 'swing'
    atr14_at_entry  REAL,
    atr_multiplier  REAL    DEFAULT 1.5,
    -- exit
    exit_date       TEXT,
    exit_price      REAL,
    exit_reason     TEXT,                       -- 'tp' | 'sl' | 'manual' | 'expired'
    days_held       INTEGER,
    pnl_pct         REAL,
    pnl_abs         REAL,
    -- risk
    position_size_pct   REAL,                  -- % of portfolio
    risk_reward_ratio   REAL,
    -- notes
    notes           TEXT,
    created_at      TEXT    DEFAULT (datetime('now')),
    updated_at      TEXT    DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_trade_stock ON trade_log(market, ticker, entry_date DESC);
CREATE INDEX IF NOT EXISTS idx_trade_date ON trade_log(entry_date DESC);

-- ============================================================
-- STOP LOSS BEHAVIOR  (ATR analysis, false stop detection)
-- ============================================================

CREATE TABLE IF NOT EXISTS stoploss_behavior (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_log_id    INTEGER REFERENCES trade_log(id),
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    signal_date     TEXT,
    sl_hit_date     TEXT,
    sl_price        REAL,
    sl_type         TEXT,                       -- 'atr' | 'fixed_pct'
    atr14_value     REAL,
    recovery_5d_pct REAL,                       -- price change 5 days after SL hit
    is_false_stop   INTEGER,                    -- 1 if recovered >2% within 5 days
    false_stop_threshold    REAL    DEFAULT 0.02,
    post_sl_direction   TEXT,                   -- 'recovered' | 'continued_down' | 'neutral'
    notes           TEXT,
    analyzed_at     TEXT    DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_sl_stock ON stoploss_behavior(market, ticker);

-- ============================================================
-- TARGET BEHAVIOR  (MPI — Missed Profit Index)
-- ============================================================

CREATE TABLE IF NOT EXISTS target_behavior (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_log_id    INTEGER REFERENCES trade_log(id),
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    signal_date     TEXT,
    tp_hit_date     TEXT,
    tp_price        REAL,
    target_move_pct REAL,
    post_tp_5d_high REAL,                       -- highest price in 5 days after TP
    post_tp_5d_return   REAL,                   -- return from TP price in 5 days
    extension_pct   REAL,                       -- how much further it went (MPI)
    continued_beyond_tp INTEGER,               -- 1 if extended > 1.5x target
    mpi_value       REAL,                       -- actual missed profit %
    pattern_id      INTEGER REFERENCES pattern_library(id),
    trailing_stop_would_have_captured   REAL,   -- sim: ATR trailing stop capture
    analyzed_at     TEXT    DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_tp_stock ON target_behavior(market, ticker);

-- ============================================================
-- TIMEFRAME ANALYSIS  (multi-timeframe regime context)
-- ============================================================

CREATE TABLE IF NOT EXISTS timeframe_analysis (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    analysis_date   TEXT    NOT NULL,
    -- daily (1D)
    d1_trend        TEXT,
    d1_flow         TEXT,
    d1_volatility   TEXT,
    d1_ma_position  TEXT,
    d1_breakout     TEXT,
    -- weekly proxy (derived from 5-day bars)
    w1_trend        TEXT,
    w1_flow         TEXT,
    w1_ma_position  TEXT,
    -- monthly proxy (derived from 20-day bars)
    m1_trend        TEXT,
    m1_flow         TEXT,
    -- regime classification
    regime          TEXT,                       -- 'trending_up' | 'trending_down' | 'choppy' | 'accumulation' | 'distribution'
    regime_confidence   REAL,
    -- market context
    market_context  TEXT,                       -- 'risk_on' | 'risk_off' | 'neutral'
    computed_at     TEXT    DEFAULT (datetime('now')),
    UNIQUE(market, ticker, analysis_date)
);
CREATE INDEX IF NOT EXISTS idx_tf_stock_date ON timeframe_analysis(market, ticker, analysis_date DESC);

-- ============================================================
-- PATTERN ROSTER  (fitness tracking — lifecycle management)
-- ============================================================

CREATE TABLE IF NOT EXISTS pattern_roster (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_id      INTEGER REFERENCES pattern_library(id),
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    direction       TEXT    NOT NULL,
    target_move     REAL,
    forward_window  INTEGER,
    behavior_pattern    TEXT,
    -- fitness metrics
    wilson_lower    REAL,
    win_rate_30d    REAL,
    win_rate_90d    REAL,
    win_rate_all    REAL,
    avg_return_30d  REAL,
    avg_return_all  REAL,
    occurrences_30d INTEGER,
    occurrences_all INTEGER,
    -- lifecycle
    lifecycle_status    TEXT    DEFAULT 'test', -- 'keep' | 'watch' | 'test' | 'retire'
    promotion_date  TEXT,
    demotion_date   TEXT,
    demotion_reason TEXT,
    -- scores
    evidence_score  REAL,
    opportunity_score   REAL,
    decay_flag      INTEGER DEFAULT 0,
    last_evaluated  TEXT    DEFAULT (datetime('now')),
    UNIQUE(market, ticker, direction, target_move, forward_window, behavior_pattern)
);

-- ============================================================
-- DATA INGESTION LOG  (track every fetch, catch quality issues)
-- ============================================================

CREATE TABLE IF NOT EXISTS ingestion_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT    NOT NULL,           -- 'kite' | 'polygon'
    market          TEXT    NOT NULL,
    ticker          TEXT,                       -- NULL = full market fetch
    fetch_type      TEXT    NOT NULL,           -- 'daily_ohlcv' | 'options_chain' | 'instruments'
    from_date       TEXT,
    to_date         TEXT,
    records_fetched INTEGER DEFAULT 0,
    records_written INTEGER DEFAULT 0,
    records_rejected    INTEGER DEFAULT 0,
    status          TEXT    NOT NULL,           -- 'success' | 'partial' | 'failed'
    error_message   TEXT,
    duration_ms     INTEGER,
    started_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    finished_at     TEXT
);

-- ============================================================
-- SNAPSHOT RUNS  (outcome-first learning run tracking)
-- ============================================================

CREATE TABLE IF NOT EXISTS snapshot_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type        TEXT    NOT NULL,
    status          TEXT    NOT NULL,           -- 'running' | 'success' | 'failed'
    started_at      TEXT    NOT NULL,
    finished_at     TEXT,
    learned_patterns    INTEGER DEFAULT 0,
    live_opportunities  INTEGER DEFAULT 0,
    tickers_processed   INTEGER DEFAULT 0,
    message         TEXT
);

-- ============================================================
-- LIVE OPPORTUNITIES  (current active pattern matches)
-- ============================================================

CREATE TABLE IF NOT EXISTS live_opportunities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_run_id INTEGER REFERENCES snapshot_runs(id),
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    direction       TEXT    NOT NULL,
    target_move     REAL,
    forward_window  INTEGER,
    behavior_pattern    TEXT,
    occurrences     INTEGER,
    hits            INTEGER,
    display_probability REAL,
    credibility     TEXT,
    lift            REAL,
    opportunity_score   REAL,
    decision_score  REAL,
    tier            TEXT,
    latest_date     TEXT,
    current_close   REAL,
    current_behavior    TEXT,
    current_atoms   TEXT,
    similarity      REAL,
    setup_summary   TEXT,
    decay_flag      INTEGER DEFAULT 0,
    created_at      TEXT    DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_live_opp ON live_opportunities(snapshot_run_id, market, direction, decision_score DESC);
"""


def init_db(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        con.executescript(SCHEMA)
        con.commit()
        print(f"Database initialized: {db_path}")
        # Print table summary
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        print(f"Created {len(tables)} tables:")
        for (t,) in tables:
            print(f"  - {t}")
    finally:
        con.close()


if __name__ == "__main__":
    init_db()
