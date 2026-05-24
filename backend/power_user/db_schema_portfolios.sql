-- KANIDA.AI Power User Portal — Co-Trader portfolio schema (Sprint 5d)
-- Version: 1 (locked 2026-05-16)
-- All statements idempotent (CREATE IF NOT EXISTS).
--
-- 4 tables for the 5 frozen virtual portfolios:
--   portfolio_definitions     — locked parameters per portfolio
--   portfolio_positions       — every position, open or closed
--   portfolio_equity_history  — daily mark-to-market of total equity
--   portfolio_event_log       — public audit trail (ENTER / EXIT / SL / TARGET / TIME)
--
-- Naming convention: `portfolio_*` (NOT `power_user_*`) — these are computed
-- by the scheduler from engine output, not written by the public API. The
-- public API is read-only on these tables.

-- ─── PORTFOLIO DEFINITIONS ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS portfolio_definitions (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    slug                  TEXT NOT NULL UNIQUE,
    name                  TEXT NOT NULL,
    tagline               TEXT NOT NULL,
    entry_rule            TEXT NOT NULL,                 -- audit / debug ID
    exit_rule             TEXT NOT NULL,                 -- audit / debug ID
    virtual_capital_start REAL NOT NULL,                 -- ₹ starting capital, locked
    start_date            TEXT NOT NULL,                 -- 'YYYY-MM-DD' inception
    is_active             INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
    display_order         INTEGER NOT NULL DEFAULT 0,
    -- Validated backtest metrics (operator-locked, sourced from V3 simulator)
    backtest_metrics_json TEXT,
    -- Parameter blob (fixed_per_trade_rs, hold days, SL/target/trail). JSON so
    -- we can add new fields without migrating.
    parameters_json       TEXT NOT NULL,
    -- Persona narrative blocks (what_it_does, trading_cycle, best_fit, etc.).
    -- Added 2026-05-16 with the V3 lock-down so the frontend reads the locked
    -- copy from one place. Nullable to keep historical rows compatible.
    narrative_json        TEXT,
    created_at            TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at            TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Idempotent column-add for environments that already have the old schema.
-- SQLite doesn't support ADD COLUMN IF NOT EXISTS pre-3.35, so we guard via
-- the application layer (db_init.py will catch the duplicate-column error).
CREATE INDEX IF NOT EXISTS ix_portfolio_def_active ON portfolio_definitions(is_active, display_order);


-- ─── POSITIONS (open + closed) ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS portfolio_positions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id        INTEGER NOT NULL REFERENCES portfolio_definitions(id),
    signal_date         TEXT NOT NULL,          -- date the engine emitted the rank
    entry_date          TEXT NOT NULL,          -- next trading day
    symbol              TEXT NOT NULL,
    sector              TEXT,
    rank_on_entry       INTEGER,                -- 1..14 etc, audit field
    score_on_entry      REAL,
    story_on_entry      TEXT,                   -- "Why I bought" — locked at entry
    entry_price         REAL NOT NULL,
    qty                 INTEGER NOT NULL,
    capital_committed   REAL NOT NULL,          -- entry_price * qty
    sl_level            REAL NOT NULL,          -- absolute price ₹
    target_level        REAL,                   -- absolute price ₹ (NULL if trail-only)
    trail_high_water    REAL,                   -- highest close since trail trigger; NULL until armed
    trail_active        INTEGER NOT NULL DEFAULT 0 CHECK (trail_active IN (0,1)),
    -- Exit fields, populated on close
    exit_date           TEXT,
    exit_price          REAL,
    exit_reason         TEXT CHECK (exit_reason IN ('SL','TARGET','TIME','TRAIL','MANUAL')),
    pnl_rs              REAL,
    pnl_pct             REAL,
    holding_days        INTEGER,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    -- Defensive: never open the same (portfolio, day, symbol) twice
    UNIQUE(portfolio_id, signal_date, symbol)
);
CREATE INDEX IF NOT EXISTS ix_port_pos_open      ON portfolio_positions(portfolio_id, exit_date) WHERE exit_date IS NULL;
CREATE INDEX IF NOT EXISTS ix_port_pos_entry     ON portfolio_positions(portfolio_id, entry_date DESC);
CREATE INDEX IF NOT EXISTS ix_port_pos_exit      ON portfolio_positions(portfolio_id, exit_date DESC) WHERE exit_date IS NOT NULL;


-- ─── DAILY EQUITY HISTORY ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS portfolio_equity_history (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id           INTEGER NOT NULL REFERENCES portfolio_definitions(id),
    trade_date             TEXT NOT NULL,
    cash_balance           REAL NOT NULL,
    deployed_capital       REAL NOT NULL,        -- sum of capital_committed for open positions
    mtm_unrealized         REAL NOT NULL,        -- sum of (qty * close - capital_committed) for open positions
    total_equity           REAL NOT NULL,        -- cash + deployed + mtm
    daily_pnl_rs           REAL NOT NULL DEFAULT 0,
    daily_pnl_pct          REAL NOT NULL DEFAULT 0,
    cumulative_return_pct  REAL NOT NULL DEFAULT 0,   -- vs. virtual_capital_start
    max_drawdown_pct       REAL NOT NULL DEFAULT 0,   -- worst dd-to-date
    peak_equity            REAL NOT NULL DEFAULT 0,   -- running peak (for dd calc)
    n_open_positions       INTEGER NOT NULL DEFAULT 0,
    n_closed_today         INTEGER NOT NULL DEFAULT 0,
    n_opened_today         INTEGER NOT NULL DEFAULT 0,
    created_at             TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(portfolio_id, trade_date)
);
CREATE INDEX IF NOT EXISTS ix_port_eq_recent ON portfolio_equity_history(portfolio_id, trade_date DESC);


-- ─── PUBLIC EVENT LOG ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS portfolio_event_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id INTEGER NOT NULL REFERENCES portfolio_definitions(id),
    event_date   TEXT NOT NULL,
    event_type   TEXT NOT NULL CHECK (event_type IN ('ENTER','EXIT_SL','EXIT_TARGET','EXIT_TIME','EXIT_TRAIL')),
    symbol       TEXT NOT NULL,
    price        REAL NOT NULL,
    reason_text  TEXT NOT NULL,        -- trader-voice, public
    position_id  INTEGER REFERENCES portfolio_positions(id),
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_port_event_recent ON portfolio_event_log(portfolio_id, event_date DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_port_event_type   ON portfolio_event_log(portfolio_id, event_type, event_date DESC);


-- ─── YEAR-BY-YEAR + MONTH-BY-MONTH BACKTEST PERFORMANCE ────────────────
-- Populated from operator's V3 audit Excel files (persona*_V3_*.xlsx) by
-- scripts/import_persona_excels.py. Read-only at runtime. Wipe + reload
-- whenever the auditor re-publishes the Excels.

CREATE TABLE IF NOT EXISTS portfolio_yearly_performance (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id    INTEGER NOT NULL REFERENCES portfolio_definitions(id),
    year            INTEGER NOT NULL,
    start_cap_rs    REAL NOT NULL,            -- always ₹5 L per V3 yearly-reset model
    end_equity_rs   REAL NOT NULL,
    pnl_rs          REAL NOT NULL,
    return_pct      REAL NOT NULL,
    max_drawdown_pct REAL NOT NULL,
    win_rate_pct    REAL NOT NULL,
    n_closed_trades INTEGER NOT NULL,
    winning_months  INTEGER,                  -- 0..12 (NULL if month data unavailable)
    is_partial      INTEGER NOT NULL DEFAULT 0 CHECK (is_partial IN (0,1)),    -- 2026 = partial
    mining_window   TEXT,                     -- e.g. "2020-2020" (debug field)
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(portfolio_id, year)
);
CREATE INDEX IF NOT EXISTS ix_port_yearly ON portfolio_yearly_performance(portfolio_id, year);


CREATE TABLE IF NOT EXISTS portfolio_monthly_performance (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id    INTEGER NOT NULL REFERENCES portfolio_definitions(id),
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    end_equity_rs   REAL NOT NULL,
    return_pct      REAL NOT NULL,            -- vs prior-month equity (or vs ₹5 L for Jan)
    is_partial      INTEGER NOT NULL DEFAULT 0 CHECK (is_partial IN (0,1)),
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(portfolio_id, year, month)
);
CREATE INDEX IF NOT EXISTS ix_port_monthly ON portfolio_monthly_performance(portfolio_id, year, month);
