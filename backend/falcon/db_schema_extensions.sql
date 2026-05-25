-- Falcon schema extensions for production runtime.
-- Adds 3 new tables on top of existing falcon_* tables (features, outcomes,
-- patterns, validations, promoted_patterns). Idempotent.

-- Live signals emitted each day (production picks).
CREATE TABLE IF NOT EXISTS falcon_signals_live (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_date     TEXT NOT NULL,            -- date the signal was generated
    entry_date      TEXT,                     -- target trading day (T+1)
    rank            INTEGER NOT NULL,         -- 1, 2, 3 ... (lower = stronger)
    symbol          TEXT NOT NULL,
    sector          TEXT,
    n_fires         INTEGER NOT NULL,         -- # patterns that fired
    score           REAL NOT NULL,            -- aggregate OOS lift
    close_at_signal REAL,
    avg_value_60d   REAL,                     -- liquidity proxy
    fired_pattern_ids TEXT,                   -- JSON array of pattern_ids
    sample_rules    TEXT,                     -- JSON of top-5 rule strings
    engine_version  TEXT NOT NULL DEFAULT '7.1.0',
    emitted_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (signal_date, symbol, engine_version)
);
CREATE INDEX IF NOT EXISTS idx_signals_live_date  ON falcon_signals_live(signal_date);
CREATE INDEX IF NOT EXISTS idx_signals_live_rank  ON falcon_signals_live(signal_date, rank);

-- Audit log of cron runs.
CREATE TABLE IF NOT EXISTS falcon_signal_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name        TEXT NOT NULL,            -- 'daily_data_refresh', etc.
    started_at      TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at     TEXT,
    status          TEXT NOT NULL DEFAULT 'running',  -- running | success | failed
    rows_affected   INTEGER,
    notes           TEXT,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_signal_runs_job ON falcon_signal_runs(job_name, started_at DESC);

-- Notification queue (in-app + email).
CREATE TABLE IF NOT EXISTS falcon_notifications_out (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    channel         TEXT NOT NULL,            -- 'inapp' | 'email'
    subject         TEXT NOT NULL,
    body_md         TEXT,
    body_html       TEXT,
    payload_json    TEXT,                     -- arbitrary structured data for in-app
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | sent | failed
    sent_at         TEXT,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_notifs_status ON falcon_notifications_out(status, created_at DESC);

-- F4 (2026-05-24): Falcon Top 10 live-audit trail.
-- Internal only — populated by V7 pipeline step 4 (top10_audit.run).
-- Records every Top 10 pick emitted live, plus its realised exit when the
-- close-job walks the position forward through the same exit logic the
-- persona simulator uses (init_stop / trail / time_stop). Lets ops monitor
-- live-vs-backtest drift on the locked rules without touching the user-
-- facing card.
CREATE TABLE IF NOT EXISTS falcon_top10_audit (
    signal_date          TEXT NOT NULL,
    entry_date           TEXT NOT NULL,
    rank                 INTEGER NOT NULL,         -- user-facing rank (avg_lift order)
    symbol               TEXT NOT NULL,
    sector               TEXT,
    avg_lift             REAL,                     -- score / n_fires at signal time
    n_fires              INTEGER,
    close_at_signal      REAL,
    -- Filled by close-job once entry_date + hold_days has passed:
    exit_date            TEXT,
    exit_price           REAL,
    net_return_pct       REAL,                     -- (exit/entry - 1) * 100, after fee + slip
    exit_reason          TEXT,                     -- INIT_STOP | TRAIL_GIVEBACK | TIME_STOP
    inserted_at          TEXT NOT NULL DEFAULT (datetime('now')),
    closed_at            TEXT,                     -- when close-job filled exit_* fields
    PRIMARY KEY (signal_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_top10_audit_open
    ON falcon_top10_audit(entry_date) WHERE exit_date IS NULL;
CREATE INDEX IF NOT EXISTS idx_top10_audit_signal_date
    ON falcon_top10_audit(signal_date DESC);

-- Trade module: batch run + per-order audit (Phase 1 auto-trade panel).
CREATE TABLE IF NOT EXISTS falcon_trade_runs (
    batch_id            TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    status              TEXT NOT NULL,            -- PENDING | PLACING | COMPLETED | ABORTED
    signal_date         TEXT NOT NULL,
    entry_date          TEXT NOT NULL,
    selected_symbols    TEXT NOT NULL,            -- JSON array
    total_capital       INTEGER NOT NULL,
    per_trade           INTEGER NOT NULL,
    use_leverage        INTEGER NOT NULL,         -- 0 | 1
    hold_days           INTEGER NOT NULL,
    sl_pct              REAL NOT NULL,
    trail_trigger_pct   REAL NOT NULL,
    trail_lookback_days INTEGER NOT NULL,
    n_attempted         INTEGER DEFAULT 0,
    n_filled            INTEGER DEFAULT 0,
    n_failed            INTEGER DEFAULT 0,
    n_overlap           INTEGER DEFAULT 0,
    n_skip_action       INTEGER DEFAULT 0,
    n_average_action    INTEGER DEFAULT 0,
    total_deployed      INTEGER DEFAULT 0,
    notes               TEXT
);
CREATE INDEX IF NOT EXISTS idx_trade_runs_entry ON falcon_trade_runs(entry_date, status);

CREATE TABLE IF NOT EXISTS falcon_trade_orders (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id          TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    side              TEXT NOT NULL,              -- BUY | SELL
    role              TEXT NOT NULL,              -- ENTRY | STOP | SMOKE | EXIT
    is_averaging      INTEGER DEFAULT 0,          -- 0 | 1
    kite_order_id     TEXT,
    qty               INTEGER NOT NULL,           -- ordered qty (spec)
    filled_qty        INTEGER DEFAULT 0,          -- actual qty filled at Kite (Phase 2.2 partial-fill tracking)
    last_reconciled_at TEXT,                       -- when filled_qty was last refreshed from kite.orders()
    price             REAL,
    trigger_price     REAL,
    product           TEXT NOT NULL,              -- MTF | CNC
    order_type        TEXT NOT NULL,              -- MARKET | LIMIT | SL | SL-M
    status            TEXT NOT NULL,              -- PENDING | PLACING | PLACED | REJECTED | CANCELLED | FAILED | PARTIAL
    placed_at         TEXT,
    filled_at         TEXT,
    error             TEXT,
    idempotency_key   TEXT NOT NULL UNIQUE,
    FOREIGN KEY (batch_id) REFERENCES falcon_trade_runs(batch_id)
);
CREATE INDEX IF NOT EXISTS idx_trade_orders_batch  ON falcon_trade_orders(batch_id);
CREATE INDEX IF NOT EXISTS idx_trade_orders_symbol ON falcon_trade_orders(symbol);
CREATE INDEX IF NOT EXISTS idx_trade_orders_role   ON falcon_trade_orders(role);

-- Phase 2: live-monitoring state per held symbol (one row per symbol).
CREATE TABLE IF NOT EXISTS falcon_position_state (
    symbol            TEXT PRIMARY KEY,
    managed_by        TEXT NOT NULL,         -- falcon | external
    product           TEXT,                  -- MTF | CNC | MIXED — for trail-config lookup
    qty               INTEGER NOT NULL,
    avg_entry         REAL NOT NULL,
    initial_sl_price  REAL NOT NULL,         -- entry × (1 + sl_pct/100), set at adoption
    current_sl_price  REAL NOT NULL,         -- moves up when HW reached + trail re-place
    target_price      REAL NOT NULL,         -- entry × (1 + trail_trigger_pct/100)
    high_water_price  REAL NOT NULL,         -- max current_price seen since entry
    hw_reached        INTEGER DEFAULT 0,     -- 0|1, set true once high_water >= target
    trail_active      INTEGER DEFAULT 0,     -- 0|1, set true after first SL re-place
    sl_kite_order_id  TEXT,                  -- Kite order ID of the live SL (for replace/cancel)
    last_action_at    TEXT,                  -- timestamp of last auto-action (rate limiting)
    entry_date        TEXT,
    hold_days_max     INTEGER DEFAULT 7,
    last_seen_price   REAL,
    last_polled_at    TEXT,
    last_event_kind   TEXT,                  -- e.g. 'NEAR_SL', 'HW_REACHED', 'BREACHED'
    last_event_at     TEXT
);

-- Phase 2: per-position events (trigger feed). One row per detected event.
CREATE TABLE IF NOT EXISTS falcon_trade_events (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    detected_at       TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    kind              TEXT NOT NULL,         -- NEAR_SL | NEAR_TARGET | HW_REACHED | BREACHED_SL | TARGET_HIT | TIME_STOP_DUE | SL_REPLACED | EXIT_PLACED
    severity          TEXT NOT NULL,         -- info | warn | critical
    detail            TEXT,
    auto_action_taken INTEGER DEFAULT 0,     -- 0|1
    related_kite_id   TEXT
);
CREATE INDEX IF NOT EXISTS idx_trade_events_symbol ON falcon_trade_events(symbol, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_trade_events_kind   ON falcon_trade_events(kind, detected_at DESC);

-- Phase 2: per-symbol entry tracking (when Falcon first detected this position).
-- Used as a fallback for entry_date when neither Falcon's own records nor Kite's
-- t1_quantity flag (today-buy detector) give us the date.
CREATE TABLE IF NOT EXISTS falcon_position_first_seen (
    symbol            TEXT PRIMARY KEY,
    first_seen_date   TEXT NOT NULL,        -- ISO YYYY-MM-DD
    first_seen_at     TEXT NOT NULL,        -- ISO timestamp
    detected_via      TEXT NOT NULL,        -- 'falcon_order' | 'kite_t1' | 'first_poll'
    qty_at_first_seen INTEGER
);

-- Phase 2: trail-stop config per product (MTF, CNC).
-- Original 4 percentage params: activate / lock / trail_sl / trail_profit.
-- Phase 2.1: trail_method + trail_lookback added to support 10d_low Donchian exit.
--   trail_method='percentage' → uses trail_sl_pct (legacy)
--   trail_method='10d_low'    → uses min(low) over last `trail_lookback` completed
--                                trading sessions; trail_sl_pct/trail_profit_pct ignored
CREATE TABLE IF NOT EXISTS falcon_trail_config (
    product            TEXT PRIMARY KEY,    -- 'MTF' | 'CNC'
    activate_pct       REAL NOT NULL,       -- e.g. 10.0 → trigger at +10%
    lock_pct           REAL NOT NULL,       -- e.g. 3.0  → SL floor at +3% once activated
    trail_sl_pct       REAL NOT NULL,       -- e.g. 5.0  → SL = current × (1 - 5%) [percentage method only]
    trail_profit_pct   REAL NOT NULL,       -- e.g. 2.0  → target trails current × (1 + 2%) [percentage method only]
    auto_exit_enabled  INTEGER DEFAULT 0,   -- 0|1 — gate auto-actions per product
    initial_sl_pct     REAL NOT NULL,       -- e.g. -7.0 (BEFORE activation)
    hold_days_max      INTEGER NOT NULL,    -- e.g. 7
    trail_method       TEXT NOT NULL DEFAULT 'percentage',  -- 'percentage' | '10d_low'
    trail_lookback     INTEGER NOT NULL DEFAULT 10,         -- N trading sessions (10d_low method only)
    updated_at         TEXT NOT NULL
);

-- Seed defaults (idempotent — INSERT OR IGNORE)
INSERT OR IGNORE INTO falcon_trail_config
  (product, activate_pct, lock_pct, trail_sl_pct, trail_profit_pct,
   auto_exit_enabled, initial_sl_pct, hold_days_max,
   trail_method, trail_lookback, updated_at)
VALUES
  ('MTF', 12.0, 3.0, 7.0, 2.0, 0, -7.0, 7, 'percentage', 10, datetime('now')),
  ('CNC', 12.0, 3.0, 7.0, 2.0, 0, -7.0, 7, 'percentage', 10, datetime('now'));

-- Phase 2.4: Engine Playbook — singleton operator-level config that drives
-- new-trade behavior in the /falcon/trade panel. Separate from per-product
-- trail config (which manages already-placed positions) — this is the rules
-- for sizing & selecting NEW trades. Editable from /falcon/config UI.
CREATE TABLE IF NOT EXISTS falcon_engine_config (
    id                   INTEGER PRIMARY KEY CHECK (id = 1),  -- enforced singleton
    per_trade_pct        REAL    NOT NULL DEFAULT 6.0,        -- per-trade cash = total × this %
    daily_picks_max      INTEGER NOT NULL DEFAULT 14,         -- hard cap on top-N selected per day
    skip_already_held    INTEGER NOT NULL DEFAULT 1,          -- 1=true, 0=false
    mining_window_years  INTEGER NOT NULL DEFAULT 4,          -- B4 publish cutoff: mined_year >= (current_year - this)
    updated_at           TEXT    NOT NULL
);
INSERT OR IGNORE INTO falcon_engine_config (id, per_trade_pct, daily_picks_max, skip_already_held, mining_window_years, updated_at)
VALUES (1, 6.0, 14, 1, 4, datetime('now'));

-- Phase 2.3: pre-market staging table.
-- EOD orchestrator stages signals + bulk-adopt items here at 16:05 IST.
-- User reviews + confirms in /falcon/premarket → status flips STAGED → QUEUED.
-- Pre-market deployer thread fires QUEUED items at 9:15 IST next trading day.
CREATE TABLE IF NOT EXISTS falcon_premarket_staging (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    staged_at       TEXT NOT NULL,                  -- when EOD orchestrator created the row
    target_date     TEXT NOT NULL,                  -- ISO date the deploy targets (next trading day)
    kind            TEXT NOT NULL,                  -- 'NEW_ENTRY' | 'BULK_ADOPT'
    symbol          TEXT NOT NULL,
    payload_json    TEXT NOT NULL,                  -- frozen request shape — what deployer will send
    status          TEXT NOT NULL DEFAULT 'STAGED', -- STAGED | QUEUED | DEPLOYED | FAILED | CANCELLED
    confirmed_at    TEXT,                            -- when user clicked Confirm & Schedule
    deployed_at     TEXT,                            -- when deployer ran it
    deploy_result   TEXT,                            -- JSON result on success
    deploy_error    TEXT,                            -- error string on failure
    UNIQUE (target_date, kind, symbol)               -- one row per (date, kind, symbol)
);
CREATE INDEX IF NOT EXISTS idx_premarket_target_status
  ON falcon_premarket_staging(target_date, status);
CREATE INDEX IF NOT EXISTS idx_premarket_status
  ON falcon_premarket_staging(status);
