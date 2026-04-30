-- ============================================================
-- KANIDA.AI — Canonical Postgres Schema (Supabase)
-- Migration: 0001_initial
--
-- Run this in the Supabase SQL editor ONCE to create all tables.
-- Then run scripts/migrate_to_supabase.py to copy SQLite data.
--
-- Extensions required (already enabled on Supabase):
--   pgcrypto (for gen_random_uuid)
-- ============================================================

-- ── Market Data ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ohlc_daily (
    id           BIGSERIAL PRIMARY KEY,
    market       TEXT    NOT NULL,
    ticker       TEXT    NOT NULL,
    trade_date   TEXT    NOT NULL,
    open         DOUBLE PRECISION NOT NULL,
    high         DOUBLE PRECISION NOT NULL,
    low          DOUBLE PRECISION NOT NULL,
    close        DOUBLE PRECISION NOT NULL,
    volume       BIGINT  NOT NULL,
    source       TEXT    NOT NULL CHECK (source IN ('kite','polygon')),
    quality_flag TEXT    DEFAULT 'ok' CHECK (quality_flag IN ('ok','suspect','rejected')),
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (market, ticker, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_ohlc_stock_date ON ohlc_daily (market, ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_ohlc_date       ON ohlc_daily (trade_date);
CREATE INDEX IF NOT EXISTS idx_ohlc_source     ON ohlc_daily (source);

-- ── Instrument Registry ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS instruments (
    id              BIGSERIAL PRIMARY KEY,
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    name            TEXT,
    sector          TEXT,
    industry        TEXT,
    market_cap_tier TEXT,
    is_active       INTEGER DEFAULT 1,
    kite_token      TEXT,
    polygon_ticker  TEXT,
    added_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (market, ticker)
);

-- ── Universe (managed via admin portal) ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS universe (
    symbol          TEXT NOT NULL,
    exchange        TEXT NOT NULL DEFAULT 'NSE',
    asset_class     TEXT NOT NULL DEFAULT 'EQUITY',
    company_name    TEXT,
    sector          TEXT,
    industry        TEXT,
    universe_sets   TEXT NOT NULL DEFAULT '["FNO"]',
    is_active       INTEGER NOT NULL DEFAULT 1,
    added_date      TEXT NOT NULL,
    added_by        TEXT NOT NULL DEFAULT 'system',
    notes           TEXT,
    PRIMARY KEY (symbol, exchange)
);
CREATE INDEX IF NOT EXISTS idx_universe_active   ON universe (exchange, is_active);
CREATE INDEX IF NOT EXISTS idx_universe_sector   ON universe (sector);

-- ── Strategies (Strategy Lab) ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS strategies (
    id               TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name             TEXT NOT NULL,
    description      TEXT,
    version          INTEGER NOT NULL DEFAULT 1,
    status           TEXT NOT NULL DEFAULT 'draft'
                         CHECK (status IN ('draft','sandbox','staging','prod','archived')),
    universe_filter  TEXT NOT NULL DEFAULT '{"sets":["FNO"],"exchange":"NSE"}',
    params           TEXT NOT NULL DEFAULT
        '{"rr_ratio":2.0,"min_overlap":0.65,"max_hold_days":21,'
        '"entry_type":"smart","directions":["rally"],"backtest_years":["2024","2025","2026"]}',
    backtest_result  TEXT,
    last_backtest_at TEXT,
    promoted_by      TEXT,
    promoted_at      TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    notes            TEXT
);
CREATE INDEX IF NOT EXISTS idx_strategies_status ON strategies (status);

-- ── Behavior Features ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS behavior_features (
    id             BIGSERIAL PRIMARY KEY,
    market         TEXT NOT NULL,
    ticker         TEXT NOT NULL,
    trade_date     TEXT NOT NULL,
    close          DOUBLE PRECISION,
    ret1           DOUBLE PRECISION,
    ret3           DOUBLE PRECISION,
    ret5           DOUBLE PRECISION,
    ret10          DOUBLE PRECISION,
    ret20          DOUBLE PRECISION,
    ret60          DOUBLE PRECISION,
    trend_20       TEXT,
    trend_60       TEXT,
    current_move   TEXT,
    flow           TEXT,
    candle         TEXT,
    volume_state   TEXT,
    volatility     TEXT,
    range_state    TEXT,
    ma_position    TEXT,
    ma_slope       TEXT,
    sr_state       TEXT,
    gap_state      TEXT,
    breakout_state TEXT,
    behavior_atoms TEXT,
    coarse_behavior TEXT,
    computed_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (market, ticker, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_feat_stock_date ON behavior_features (market, ticker, trade_date DESC);

-- ── Pattern Library ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pattern_library (
    id                     BIGSERIAL PRIMARY KEY,
    market                 TEXT    NOT NULL,
    ticker                 TEXT    NOT NULL,
    direction              TEXT    NOT NULL,
    target_move            DOUBLE PRECISION NOT NULL,
    forward_window         INTEGER NOT NULL,
    pattern_size           INTEGER NOT NULL,
    behavior_pattern       TEXT    NOT NULL,
    occurrences            INTEGER NOT NULL,
    hits                   INTEGER NOT NULL,
    baseline_probability   DOUBLE PRECISION,
    raw_probability        DOUBLE PRECISION,
    trusted_probability    DOUBLE PRECISION,
    display_probability    DOUBLE PRECISION,
    probability_ci_low     DOUBLE PRECISION,
    probability_ci_high    DOUBLE PRECISION,
    credibility            TEXT,
    lift                   DOUBLE PRECISION,
    avg_forward_return     DOUBLE PRECISION,
    recent_probability     DOUBLE PRECISION,
    stability              DOUBLE PRECISION,
    opportunity_score      DOUBLE PRECISION,
    decay_flag             INTEGER DEFAULT 0,
    tier                   TEXT,
    lifecycle_status       TEXT    DEFAULT 'active',
    first_seen_date        TEXT,
    last_seen_date         TEXT,
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    updated_at             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (market, ticker, direction, target_move, forward_window, behavior_pattern)
);
CREATE INDEX IF NOT EXISTS idx_pattern_stock ON pattern_library (market, ticker, direction, opportunity_score DESC);
CREATE INDEX IF NOT EXISTS idx_pattern_tier  ON pattern_library (tier, credibility, opportunity_score DESC);

-- ── Atom Stats ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS atom_stats (
    id              BIGSERIAL PRIMARY KEY,
    market          TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    atom_key        TEXT    NOT NULL,
    direction       TEXT    NOT NULL,
    target_move     DOUBLE PRECISION NOT NULL,
    forward_window  INTEGER NOT NULL,
    occurrences     INTEGER NOT NULL,
    hits            INTEGER NOT NULL,
    raw_probability DOUBLE PRECISION,
    baseline        DOUBLE PRECISION,
    lift            DOUBLE PRECISION,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (market, ticker, atom_key, direction, target_move, forward_window)
);

-- ── Signal Events ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS signal_events (
    id                  BIGSERIAL PRIMARY KEY,
    market              TEXT    NOT NULL,
    ticker              TEXT    NOT NULL,
    signal_date         TEXT    NOT NULL,
    direction           TEXT    NOT NULL,
    target_move         DOUBLE PRECISION NOT NULL,
    forward_window      INTEGER NOT NULL,
    pattern_id          BIGINT  REFERENCES pattern_library (id),
    behavior_pattern    TEXT,
    decision_score      DOUBLE PRECISION,
    opportunity_score   DOUBLE PRECISION,
    display_probability DOUBLE PRECISION,
    credibility         TEXT,
    tier                TEXT,
    current_close       DOUBLE PRECISION,
    current_behavior    TEXT,
    similarity          DOUBLE PRECISION,
    setup_summary       TEXT,
    source_run_id       BIGINT,
    fired_at            TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_signal_stock_date  ON signal_events (market, ticker, signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_signal_date_score  ON signal_events (signal_date DESC, decision_score DESC);

-- ── Signal Outcomes ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS signal_outcomes (
    id                   BIGSERIAL PRIMARY KEY,
    signal_event_id      BIGINT  REFERENCES signal_events (id),
    market               TEXT    NOT NULL,
    ticker               TEXT    NOT NULL,
    signal_date          TEXT    NOT NULL,
    direction            TEXT    NOT NULL,
    target_move          DOUBLE PRECISION NOT NULL,
    forward_window       INTEGER NOT NULL,
    entry_close          DOUBLE PRECISION,
    max_forward_return   DOUBLE PRECISION,
    min_forward_return   DOUBLE PRECISION,
    final_forward_return DOUBLE PRECISION,
    hit_target           INTEGER,
    days_to_target       INTEGER,
    exit_reason          TEXT,
    exit_price           DOUBLE PRECISION,
    exit_date            TEXT,
    pnl_pct              DOUBLE PRECISION,
    mae_pct              DOUBLE PRECISION,
    mfe_pct              DOUBLE PRECISION,
    post_exit_5d_pct     DOUBLE PRECISION,
    continued_after_tp   INTEGER,
    resolved_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (signal_event_id)
);
CREATE INDEX IF NOT EXISTS idx_outcome_stock ON signal_outcomes (market, ticker, signal_date DESC);

-- ── Trade Log ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS trade_log (
    id                  BIGSERIAL PRIMARY KEY,
    market              TEXT    NOT NULL,
    ticker              TEXT    NOT NULL,
    trade_type          TEXT    DEFAULT 'paper',
    direction           TEXT    NOT NULL,
    signal_event_id     BIGINT  REFERENCES signal_events (id),
    strategy_id         TEXT    REFERENCES strategies (id),
    signal_date         TEXT,
    entry_date          TEXT,
    entry_price         DOUBLE PRECISION,
    target_price        DOUBLE PRECISION,
    stop_price          DOUBLE PRECISION,
    stop_type           TEXT    DEFAULT 'atr',
    atr14_at_entry      DOUBLE PRECISION,
    atr_multiplier      DOUBLE PRECISION DEFAULT 1.5,
    exit_date           TEXT,
    exit_price          DOUBLE PRECISION,
    exit_reason         TEXT,
    days_held           INTEGER,
    pnl_pct             DOUBLE PRECISION,
    pnl_abs             DOUBLE PRECISION,
    position_size_pct   DOUBLE PRECISION,
    risk_reward_ratio   DOUBLE PRECISION,
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_trade_stock    ON trade_log (market, ticker, entry_date DESC);
CREATE INDEX IF NOT EXISTS idx_trade_date     ON trade_log (entry_date DESC);
CREATE INDEX IF NOT EXISTS idx_trade_strategy ON trade_log (strategy_id);

-- ── Execution Log ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS execution_log (
    id               BIGSERIAL PRIMARY KEY,
    trade_log_id     BIGINT  NOT NULL,
    ticker           TEXT    NOT NULL,
    direction        TEXT    NOT NULL,
    signal_date      TEXT    NOT NULL,
    entry_date       TEXT    NOT NULL,
    exec_code        TEXT    NOT NULL,
    trade_taken      INTEGER NOT NULL,
    entry_window     TEXT,
    exec_notes       TEXT,
    prev_close       DOUBLE PRECISION,
    entry_open       DOUBLE PRECISION,
    entry_high       DOUBLE PRECISION,
    entry_low        DOUBLE PRECISION,
    entry_close      DOUBLE PRECISION,
    gap_pct          DOUBLE PRECISION,
    gap_category     TEXT,
    day_move_pct     DOUBLE PRECISION,
    day_range_pct    DOUBLE PRECISION,
    nifty_open       DOUBLE PRECISION,
    nifty_close      DOUBLE PRECISION,
    nifty_day_move   DOUBLE PRECISION,
    nifty_is_weak    INTEGER,
    rs_vs_nifty      DOUBLE PRECISION,
    blind_entry_price  DOUBLE PRECISION,
    smart_entry_price  DOUBLE PRECISION,
    exit_price         DOUBLE PRECISION,
    blind_pnl_pct      DOUBLE PRECISION,
    smart_pnl_pct      DOUBLE PRECISION,
    pnl_improvement    DOUBLE PRECISION,
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (trade_log_id)
);
CREATE INDEX IF NOT EXISTS idx_execlog_ticker     ON execution_log (ticker);
CREATE INDEX IF NOT EXISTS idx_execlog_exec_code  ON execution_log (exec_code);

-- ── Snapshot Runs ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS snapshot_runs (
    id                  BIGSERIAL PRIMARY KEY,
    run_type            TEXT    NOT NULL,
    status              TEXT    NOT NULL,
    started_at          TEXT    NOT NULL,
    finished_at         TEXT,
    learned_patterns    INTEGER DEFAULT 0,
    live_opportunities  INTEGER DEFAULT 0,
    tickers_processed   INTEGER DEFAULT 0,
    message             TEXT
);

-- ── Live Opportunities ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS live_opportunities (
    id                  BIGSERIAL PRIMARY KEY,
    snapshot_run_id     BIGINT  REFERENCES snapshot_runs (id),
    market              TEXT    NOT NULL,
    ticker              TEXT    NOT NULL,
    direction           TEXT    NOT NULL,
    target_move         DOUBLE PRECISION,
    forward_window      INTEGER,
    behavior_pattern    TEXT,
    occurrences         INTEGER,
    hits                INTEGER,
    display_probability DOUBLE PRECISION,
    credibility         TEXT,
    lift                DOUBLE PRECISION,
    opportunity_score   DOUBLE PRECISION,
    decision_score      DOUBLE PRECISION,
    tier                TEXT,
    latest_date         TEXT,
    current_close       DOUBLE PRECISION,
    current_behavior    TEXT,
    current_atoms       TEXT,
    similarity          DOUBLE PRECISION,
    setup_summary       TEXT,
    decay_flag          INTEGER DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_live_opp ON live_opportunities (snapshot_run_id, market, direction, decision_score DESC);

-- ── Stoploss & Target Behavior ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS stoploss_behavior (
    id                      BIGSERIAL PRIMARY KEY,
    trade_log_id            BIGINT  REFERENCES trade_log (id),
    market                  TEXT    NOT NULL,
    ticker                  TEXT    NOT NULL,
    signal_date             TEXT,
    sl_hit_date             TEXT,
    sl_price                DOUBLE PRECISION,
    sl_type                 TEXT,
    atr14_value             DOUBLE PRECISION,
    recovery_5d_pct         DOUBLE PRECISION,
    is_false_stop           INTEGER,
    false_stop_threshold    DOUBLE PRECISION DEFAULT 0.02,
    post_sl_direction       TEXT,
    notes                   TEXT,
    analyzed_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS target_behavior (
    id                              BIGSERIAL PRIMARY KEY,
    trade_log_id                    BIGINT  REFERENCES trade_log (id),
    market                          TEXT    NOT NULL,
    ticker                          TEXT    NOT NULL,
    signal_date                     TEXT,
    tp_hit_date                     TEXT,
    tp_price                        DOUBLE PRECISION,
    target_move_pct                 DOUBLE PRECISION,
    post_tp_5d_high                 DOUBLE PRECISION,
    post_tp_5d_return               DOUBLE PRECISION,
    extension_pct                   DOUBLE PRECISION,
    continued_beyond_tp             INTEGER,
    mpi_value                       DOUBLE PRECISION,
    pattern_id                      BIGINT  REFERENCES pattern_library (id),
    trailing_stop_would_have_captured DOUBLE PRECISION,
    analyzed_at                     TIMESTAMPTZ DEFAULT NOW()
);

-- ── Ingestion Log ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ingestion_log (
    id               BIGSERIAL PRIMARY KEY,
    source           TEXT    NOT NULL,
    market           TEXT    NOT NULL,
    ticker           TEXT,
    fetch_type       TEXT    NOT NULL,
    from_date        TEXT,
    to_date          TEXT,
    records_fetched  INTEGER DEFAULT 0,
    records_written  INTEGER DEFAULT 0,
    records_rejected INTEGER DEFAULT 0,
    status           TEXT    NOT NULL,
    error_message    TEXT,
    duration_ms      INTEGER,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ
);

-- ── Pattern Roster ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pattern_roster (
    id                  BIGSERIAL PRIMARY KEY,
    pattern_id          BIGINT  REFERENCES pattern_library (id),
    market              TEXT    NOT NULL,
    ticker              TEXT    NOT NULL,
    direction           TEXT    NOT NULL,
    target_move         DOUBLE PRECISION,
    forward_window      INTEGER,
    behavior_pattern    TEXT,
    wilson_lower        DOUBLE PRECISION,
    win_rate_30d        DOUBLE PRECISION,
    win_rate_90d        DOUBLE PRECISION,
    win_rate_all        DOUBLE PRECISION,
    avg_return_30d      DOUBLE PRECISION,
    avg_return_all      DOUBLE PRECISION,
    occurrences_30d     INTEGER,
    occurrences_all     INTEGER,
    lifecycle_status    TEXT    DEFAULT 'test',
    promotion_date      TEXT,
    demotion_date       TEXT,
    demotion_reason     TEXT,
    evidence_score      DOUBLE PRECISION,
    opportunity_score   DOUBLE PRECISION,
    decay_flag          INTEGER DEFAULT 0,
    last_evaluated      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (market, ticker, direction, target_move, forward_window, behavior_pattern)
);

-- ── schema_migrations (track applied migrations) ──────────────────────────────

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO schema_migrations (version) VALUES ('0001_initial')
    ON CONFLICT (version) DO NOTHING;
