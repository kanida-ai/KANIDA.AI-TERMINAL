-- KANIDA.AI — OLTP schema for PostgreSQL (Stage 2, generated).
-- Source: data/db/kanida_universe.db (SQLite) -> translated to PG.
-- Domain: per-user TRANSACTIONAL state only. Market/engine tables
-- (ohlc_*, falcon_features, patterns, signals) intentionally stay on SQLite.
-- Re-runnable: every statement uses IF NOT EXISTS.
-- NOTE: no BEGIN/COMMIT here on purpose — backend/pg_migrate.py runs this
-- inside pgdb.pg_conn(), which owns the transaction (so a failure rolls
-- the WHOLE migration back). Nesting BEGIN would commit early.

-- ── power_user_users ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS power_user_users (
    id              BIGSERIAL PRIMARY KEY,
    email           TEXT NOT NULL UNIQUE,
    google_sub      TEXT NOT NULL UNIQUE,           -- Google account ID (stable across email changes)
    display_name    TEXT,
    picture_url     TEXT,
    invite_code     TEXT,                           -- code they used to sign up
    role            TEXT NOT NULL DEFAULT 'user'
                    CHECK (role IN ('user','partner','admin')),
    is_active       BIGINT NOT NULL DEFAULT 1
                    CHECK (is_active IN (0,1)),
    created_at      TEXT NOT NULL,                  -- IST ISO 8601
    last_seen_at    TEXT
, billing_plan TEXT DEFAULT 'founding', razorpay_customer_id TEXT, subscription_status TEXT DEFAULT 'active');
CREATE INDEX IF NOT EXISTS ix_pu_users_email ON power_user_users(email);
CREATE INDEX IF NOT EXISTS ix_pu_users_google_sub ON power_user_users(google_sub);

-- ── power_user_invite_codes ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS power_user_invite_codes (
    code            TEXT PRIMARY KEY,
    issued_by       TEXT NOT NULL,                  -- admin email or 'system'
    issued_at       TEXT NOT NULL,                  -- IST ISO
    expires_at      TEXT,                           -- NULL = never expires
    used_by_user_id BIGINT,                        -- FK → power_user_users.id
    used_at         TEXT,
    note            TEXT,                           -- e.g. "Influencer: @username", "Beta cohort A"
    FOREIGN KEY (used_by_user_id) REFERENCES power_user_users(id)
);
CREATE INDEX IF NOT EXISTS ix_pu_invite_used ON power_user_invite_codes(used_by_user_id);

-- ── power_user_waitlist ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS power_user_waitlist (
    id              BIGSERIAL PRIMARY KEY,
    email           TEXT NOT NULL UNIQUE,
    joined_at       TEXT NOT NULL,
    source          TEXT,                           -- 'login_no_invite' | 'landing_cta' | 'demo_replay'
    invite_issued   BIGINT NOT NULL DEFAULT 0
);

-- ── power_user_subscriptions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS power_user_subscriptions (
    id                        BIGSERIAL PRIMARY KEY,
    user_id                   BIGINT NOT NULL,
    razorpay_subscription_id  TEXT UNIQUE,
    plan_code                 TEXT NOT NULL,             -- 'monthly_999' at launch
    status                    TEXT NOT NULL,             -- Razorpay sub states
    current_start             TEXT,                      -- IST ISO 8601 (TIMESTAMPTZ in prod)
    current_end               TEXT,                      -- access valid until this
    created_at                TEXT NOT NULL,             -- IST ISO; app sets (no now() default)
    updated_at                TEXT NOT NULL,             -- IST ISO; app sets
    FOREIGN KEY (user_id) REFERENCES power_user_users(id)
);
CREATE INDEX IF NOT EXISTS ix_pu_subs_user    ON power_user_subscriptions(user_id);
CREATE INDEX IF NOT EXISTS ix_pu_subs_rzp_sub ON power_user_subscriptions(razorpay_subscription_id);

-- ── power_user_watchlists ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS power_user_watchlists (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL,
    symbol          TEXT NOT NULL,
    added_at        TEXT NOT NULL,
    note            TEXT,
    UNIQUE(user_id, symbol),
    FOREIGN KEY (user_id) REFERENCES power_user_users(id)
);
CREATE INDEX IF NOT EXISTS ix_pu_watchlist_user ON power_user_watchlists(user_id);

-- ── power_user_push_subscriptions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS power_user_push_subscriptions (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT,                       -- NULL = admin (no user row needed)
    endpoint        TEXT NOT NULL UNIQUE,          -- the push service URL (Chrome/Firefox/Safari)
    p256dh          TEXT NOT NULL,                 -- key for encrypting payload
    auth            TEXT NOT NULL,                 -- key for encrypting payload
    user_agent      TEXT,                          -- for debugging which device this is
    created_at      TEXT NOT NULL,
    last_sent_at    TEXT,
    last_send_error TEXT,
    is_active       BIGINT NOT NULL DEFAULT 1
                    CHECK (is_active IN (0,1))
);
CREATE INDEX IF NOT EXISTS ix_push_subs_active ON power_user_push_subscriptions(is_active, user_id);

-- ── power_user_magic_links ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS power_user_magic_links (
    token           TEXT PRIMARY KEY,              -- random 32-hex
    kind            TEXT NOT NULL                  -- 'admin_auth_refresh'  (future: 'user_email_verify' etc.)
                    CHECK (kind IN ('admin_auth_refresh')),
    created_at      TEXT NOT NULL,
    expires_at      TEXT NOT NULL,
    used_at         TEXT,                          -- NULL = unused; set on first redemption
    used_ip_hash    TEXT,                          -- audit who consumed it
    issued_for      TEXT                           -- e.g. admin email or 'system'
);
CREATE INDEX IF NOT EXISTS ix_magic_links_expires ON power_user_magic_links(expires_at);

-- ── power_user_billing_events ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS power_user_billing_events (
    id                  BIGSERIAL PRIMARY KEY,
    user_id             BIGINT,                          -- nullable: event may arrive pre-link
    razorpay_event_id   TEXT UNIQUE NOT NULL,             -- idempotency key
    event_type          TEXT NOT NULL,                    -- e.g. subscription.activated
    payload             TEXT NOT NULL,                    -- raw event JSON (JSONB in prod)
    received_at         TEXT NOT NULL,                    -- IST ISO; app sets (no now() default)
    FOREIGN KEY (user_id) REFERENCES power_user_users(id)
);
CREATE INDEX IF NOT EXISTS ix_pu_bill_events_user ON power_user_billing_events(user_id);
CREATE INDEX IF NOT EXISTS ix_pu_bill_events_type ON power_user_billing_events(event_type, received_at);

-- ── power_user_request_log ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS power_user_request_log (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT,                        -- NULL for anonymous; FK loose (no cascade)
    route           TEXT NOT NULL,                  -- e.g. '/api/power/picks/today'
    status_code     BIGINT NOT NULL,
    latency_ms      BIGINT,
    created_at      TEXT NOT NULL,                  -- IST ISO
    ip_hash         TEXT                            -- SHA256(ip + UA) — never raw IP
);
CREATE INDEX IF NOT EXISTS ix_pulog_created   ON power_user_request_log(created_at);
CREATE INDEX IF NOT EXISTS ix_pulog_iphash_t  ON power_user_request_log(ip_hash, created_at);
CREATE INDEX IF NOT EXISTS ix_pulog_user      ON power_user_request_log(user_id, created_at);

-- ── broker_accounts ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS broker_accounts (
    broker_account_id TEXT PRIMARY KEY,           -- uuid4 hex (stable handle)
    user_id           TEXT NOT NULL,              -- portal user
    broker            TEXT NOT NULL,              -- zerodha|upstox|angel|dhan|fyers
    account_label     TEXT NOT NULL,              -- user-chosen, e.g. "Main Kite"
    api_key           TEXT NOT NULL,              -- broker app api_key
    api_secret_enc    BYTEA,                       -- Fernet-ENCRYPTED api_secret
    access_token_enc  BYTEA,                       -- Fernet-ENCRYPTED access_token
    token_date        TEXT,                       -- IST date the token was minted
    token_expiry      TEXT,                       -- ISO IST expiry (broker-specific)
    status            TEXT NOT NULL DEFAULT 'PENDING',
        -- PENDING | ACTIVE | EXPIRED | REVOKED | ERROR
    created_at        TEXT NOT NULL,              -- ISO IST
    updated_at        TEXT,
    last_login_at     TEXT, refresh_token_enc BYTEA, token_expires_at TEXT, last_health_at TEXT, last_health_status TEXT, last_error TEXT, redirect_state TEXT, egress_proxy_url_enc BYTEA,
    UNIQUE (user_id, broker, account_label)
);
CREATE INDEX IF NOT EXISTS idx_broker_accounts_user
    ON broker_accounts(user_id, status);

-- ── autotrade_sessions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_sessions (
    session_id      TEXT PRIMARY KEY,             -- uuid4 hex
    created_at      TEXT NOT NULL,                -- ISO IST
    started_at      TEXT,
    closed_at       TEXT,
    status          TEXT NOT NULL DEFAULT 'CREATED',
        -- CREATED | RUNNING | KILLING | CLOSED | FAILED
    mode            TEXT NOT NULL DEFAULT 'paper',   -- 'paper' | 'live'
    total_allocated_capital DOUBLE PRECISION NOT NULL,
    config_json     TEXT NOT NULL,
    last_gross_return DOUBLE PRECISION,
    kill_reason     TEXT,
    notes           TEXT
, invested_basis DOUBLE PRECISION, trail_armed BIGINT DEFAULT 0, trail_peak DOUBLE PRECISION, entry_latency_ms BIGINT, exit_latency_ms BIGINT, user_id TEXT, broker_account_id TEXT, ladder_id TEXT, config_version BIGINT NOT NULL DEFAULT 0, entry_basket_notional DOUBLE PRECISION, mag_entry_complete BIGINT NOT NULL DEFAULT 0, mag_leg2_plan_json TEXT);
CREATE INDEX IF NOT EXISTS idx_autotrade_sessions_ladder ON autotrade_sessions(ladder_id);

-- ── autotrade_positions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_positions (
    id              BIGSERIAL PRIMARY KEY,
    session_id      TEXT NOT NULL,
    broker_profile  TEXT,
    symbol          TEXT NOT NULL,
    instrument_type TEXT NOT NULL DEFAULT 'EQ',
    exchange        TEXT,
    qty             BIGINT NOT NULL DEFAULT 0,
    avg_price       DOUBLE PRECISION NOT NULL DEFAULT 0,
    sl_level        DOUBLE PRECISION,
    target_price    DOUBLE PRECISION,
    ltp             DOUBLE PRECISION,
    unrealised_pnl  DOUBLE PRECISION,
    status          TEXT NOT NULL DEFAULT 'OPEN',  -- OPEN | CLOSED | EXIT_FAILED
    exit_lock       BIGINT NOT NULL DEFAULT 0,
    exit_initiated_by TEXT,
    opened_at       TEXT,
    closed_at       TEXT,
    exit_price      DOUBLE PRECISION,
    realised_pnl    DOUBLE PRECISION,
    close_reason    TEXT
, gtt_id TEXT, gtt_stop DOUBLE PRECISION, gtt_target DOUBLE PRECISION, broker_account_id TEXT, direction TEXT NOT NULL DEFAULT 'long', entry_order_id TEXT, exit_order_id TEXT, pos_trail_armed BIGINT DEFAULT 0, pos_trail_peak DOUBLE PRECISION, ltp_as_of TEXT, client_order_id TEXT, product TEXT, exit_client_order_id TEXT, slm_order_id TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS uq_autotrade_positions_sess_sym_prof
    ON autotrade_positions(session_id, symbol, broker_profile);
CREATE INDEX IF NOT EXISTS idx_autotrade_positions_session
    ON autotrade_positions(session_id, status);

-- ── autotrade_order_events ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_order_events (
    id              BIGSERIAL PRIMARY KEY,
    ts              TEXT NOT NULL,                 -- ISO IST
    session_id      TEXT,
    position_ref    TEXT,                          -- position id or "session:symbol"
    symbol          TEXT,
    product         TEXT,
    broker_profile  TEXT,
    broker_order_id TEXT,                          -- NULL for paper / pre-submission
    client_order_id TEXT,                          -- our durable ownership key
    event_type      TEXT NOT NULL,
    qty             BIGINT,
    price           DOUBLE PRECISION,
    source          TEXT,                          -- entry|exit|kill|reconcile|...
    detail          TEXT
);
CREATE INDEX IF NOT EXISTS idx_autotrade_order_events_session
    ON autotrade_order_events(session_id, symbol);
CREATE INDEX IF NOT EXISTS idx_autotrade_order_events_coid
    ON autotrade_order_events(client_order_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_autotrade_order_events_bid_type_prof
    ON autotrade_order_events(broker_order_id, event_type,
                             COALESCE(broker_profile,''));

-- ── autotrade_alerts ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_alerts (
    id           BIGSERIAL PRIMARY KEY,
    ts           TEXT NOT NULL,                 -- ISO IST when raised
    incident_id  TEXT,                          -- dedup key (kind|session|symbol)
    severity     TEXT NOT NULL DEFAULT 'info',  -- info | warn | critical | urgent
    kind         TEXT NOT NULL,                 -- EXIT_FAILED | NAKED_POSITION | ...
    session_id   TEXT,                          -- owning session (may be NULL)
    symbol       TEXT,                          -- affected symbol (may be NULL)
    detail       TEXT,                          -- human-readable detail
    acknowledged BIGINT NOT NULL DEFAULT 0,    -- 0/1 operator ack
    escalated    BIGINT NOT NULL DEFAULT 0,    -- 0/1 re-pushed after threshold
    pushed       BIGINT NOT NULL DEFAULT 0,    -- 0/1 web_push dispatch succeeded
    push_result  TEXT                           -- transport result / error string
, ack_reason TEXT, auto_resolved BIGINT NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS idx_autotrade_alerts_ts
    ON autotrade_alerts(ts DESC);
CREATE INDEX IF NOT EXISTS idx_autotrade_alerts_incident
    ON autotrade_alerts(incident_id, ts);
CREATE INDEX IF NOT EXISTS idx_autotrade_alerts_session
    ON autotrade_alerts(session_id, ts DESC);

-- ── autotrade_recon_alerts ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_recon_alerts (
    id          BIGSERIAL PRIMARY KEY,
    ts          TEXT NOT NULL,                 -- ISO IST when detected
    session_id  TEXT,                          -- reconciling session (may be NULL)
    symbol      TEXT NOT NULL,
    product     TEXT,                          -- CNC | MIS | NRML | MTF
    kind        TEXT NOT NULL,                 -- UNATTRIBUTED_CLOSE | ORPHAN_AT_BROKER
    detail      TEXT                           -- human-readable divergence detail
);
CREATE INDEX IF NOT EXISTS idx_autotrade_recon_alerts_ts
    ON autotrade_recon_alerts(ts DESC);

-- ── autotrade_slippage ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_slippage (
    id              BIGSERIAL PRIMARY KEY,
    session_id      TEXT,
    broker_profile  TEXT,
    symbol          TEXT NOT NULL,
    expected_price  DOUBLE PRECISION NOT NULL,
    actual_price    DOUBLE PRECISION NOT NULL,
    slippage_pct    DOUBLE PRECISION NOT NULL,
    qty             BIGINT NOT NULL,
    recorded_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_autotrade_slippage_session
    ON autotrade_slippage(session_id, recorded_at DESC);

-- ── autotrade_claims ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_claims (
    claim_key    TEXT PRIMARY KEY,   -- unique claim identity
    holder       TEXT,               -- optional holder tag (pid/host/diag)
    leased_until TEXT NOT NULL,      -- ISO IST; claim is live while now < this
    created_at   TEXT NOT NULL       -- ISO IST when (re)claimed
);

-- ── autotrade_kill_switch_log ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_kill_switch_log (
    id              BIGSERIAL PRIMARY KEY,
    session_id      TEXT NOT NULL,
    fired_at        TEXT NOT NULL,
    trigger_reason  TEXT NOT NULL,
    gross_return    DOUBLE PRECISION,
    n_positions     BIGINT,
    n_exited_ok     BIGINT,
    n_exit_failed   BIGINT,
    mode            TEXT NOT NULL DEFAULT 'paper',
    detail_json     TEXT
);

-- ── autotrade_ladders ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_ladders (
    ladder_id           TEXT PRIMARY KEY,             -- uuid4 hex
    user_id             TEXT,                         -- portal user (NULL = operator)
    broker_account_id   TEXT,                         -- vaulted account (NULL = global)
    mode                TEXT NOT NULL DEFAULT 'paper', -- 'paper' | 'live' (children inherit)
    total_capital       DOUBLE PRECISION NOT NULL,                -- the deployed-capital ceiling
    order_product       TEXT NOT NULL DEFAULT 'CNC',  -- CNC | MTF only (MIS rejected)
    per_basket_capital  DOUBLE PRECISION NOT NULL,                -- FROZEN at create = total/3
    status              TEXT NOT NULL DEFAULT 'RUNNING',
        -- RUNNING | PAUSED | ENDED | COMPLETED
    start_date          TEXT,                         -- ISO date the campaign began
    end_date            TEXT,                         -- ISO date (last trading day of
                                                      -- start month) or NULL = manual-only
    mode_kill           TEXT,                         -- flatten_now | stop_new_let_finish (on KILL)
    -- 5-DAY DOWNTURN ALERT state. rolling window of per-trading-day realized
    -- returns (JSON list of {date, ret}); alert_active latches on a down-crossing
    -- so we fire alerts.send() at most once per crossing (surfaced in status()).
    daily_returns_json  TEXT,                         -- JSON [{"date","ret"}, ...]
    alert_active        BIGINT NOT NULL DEFAULT 0,   -- 0/1 down-crossing latch
    last_tick_date      TEXT,                         -- ISO date of the last daily tick (idempotency)
    created_at          TEXT NOT NULL,                -- ISO IST
    updated_at          TEXT
, child_config_json TEXT, config_version BIGINT NOT NULL DEFAULT 0, campaign_type TEXT NOT NULL DEFAULT 'positional');
CREATE INDEX IF NOT EXISTS idx_autotrade_ladders_user
    ON autotrade_ladders(user_id, status);

-- ── autotrade_portfolio_snapshots ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_portfolio_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    session_id      TEXT NOT NULL,
    snapped_at      TEXT NOT NULL,
    gross_return    DOUBLE PRECISION NOT NULL,
    total_unrealised DOUBLE PRECISION NOT NULL,
    total_allocated_capital DOUBLE PRECISION NOT NULL,
    n_open_positions BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_autotrade_snap_session
    ON autotrade_portfolio_snapshots(session_id, snapped_at DESC);

-- ── autotrade_session_account_allocations ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_session_account_allocations (
    session_id        TEXT NOT NULL,
    broker_account_id TEXT,                       -- NULL = operator/global account
    allocated_capital DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at        TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_atsaa_session_account
    ON autotrade_session_account_allocations(
        session_id, COALESCE(broker_account_id,''));

-- ── autotrade_config_presets ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_config_presets (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    created_at  TEXT NOT NULL,
    config_json TEXT NOT NULL
);

-- ── autotrade_config_edits ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_config_edits (
    id           BIGSERIAL PRIMARY KEY,
    ts           TEXT NOT NULL,                 -- ISO IST when applied
    target_type  TEXT NOT NULL,                 -- 'session' | 'ladder'
    target_id    TEXT NOT NULL,                 -- session_id | ladder_id
    user_id      TEXT,                          -- the caller (who)
    from_version BIGINT,                       -- config_version before the edit
    to_version   BIGINT,                       -- config_version after the edit
    field_changes TEXT,                         -- JSON {field: applied_value}
    detail       TEXT
);
CREATE INDEX IF NOT EXISTS idx_autotrade_config_edits_target
    ON autotrade_config_edits(target_type, target_id, ts DESC);

-- ── autotrade_broker_profiles ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autotrade_broker_profiles (
    profile_id      TEXT PRIMARY KEY,
    broker_name     TEXT NOT NULL,                -- zerodha | fyers | upstox | angel | dhan
    allocated_capital DOUBLE PRECISION NOT NULL DEFAULT 0,
    symbols_json    TEXT,                         -- NULL = use Falcon top_n
    rank_low        BIGINT,
    rank_high       BIGINT,
    order_product   TEXT NOT NULL DEFAULT 'CNC',
    instrument_type TEXT NOT NULL DEFAULT 'EQ',
    enabled         BIGINT NOT NULL DEFAULT 1,
    creds_configured BIGINT NOT NULL DEFAULT 0,  -- 1 if env/token present
    created_at      TEXT NOT NULL,
    updated_at      TEXT
);

-- ── portfolio_positions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolio_positions (
    id                  BIGSERIAL PRIMARY KEY,
    portfolio_id        BIGINT NOT NULL REFERENCES portfolio_definitions(id),
    signal_date         TEXT NOT NULL,          -- date the engine emitted the rank
    entry_date          TEXT NOT NULL,          -- next trading day
    symbol              TEXT NOT NULL,
    sector              TEXT,
    rank_on_entry       BIGINT,                -- 1..14 etc, audit field
    score_on_entry      DOUBLE PRECISION,
    story_on_entry      TEXT,                   -- "Why I bought" — locked at entry
    entry_price         DOUBLE PRECISION NOT NULL,
    qty                 BIGINT NOT NULL,
    capital_committed   DOUBLE PRECISION NOT NULL,          -- entry_price * qty
    sl_level            DOUBLE PRECISION NOT NULL,          -- absolute price ₹
    target_level        DOUBLE PRECISION,                   -- absolute price ₹ (NULL if trail-only)
    trail_high_water    DOUBLE PRECISION,                   -- highest close since trail trigger; NULL until armed
    trail_active        BIGINT NOT NULL DEFAULT 0 CHECK (trail_active IN (0,1)),
    -- Exit fields, populated on close
    exit_date           TEXT,
    exit_price          DOUBLE PRECISION,
    exit_reason         TEXT CHECK (exit_reason IN ('SL','TARGET','TIME','TRAIL','MANUAL')),
    pnl_rs              DOUBLE PRECISION,
    pnl_pct             DOUBLE PRECISION,
    holding_days        BIGINT,
    created_at          TEXT NOT NULL DEFAULT (TIMESTAMPTZ('now')),
    -- Defensive: never open the same (portfolio, day, symbol) twice
    UNIQUE(portfolio_id, signal_date, symbol)
);
CREATE INDEX IF NOT EXISTS ix_port_pos_open      ON portfolio_positions(portfolio_id, exit_date) WHERE exit_date IS NULL;
CREATE INDEX IF NOT EXISTS ix_port_pos_entry     ON portfolio_positions(portfolio_id, entry_date DESC);
CREATE INDEX IF NOT EXISTS ix_port_pos_exit      ON portfolio_positions(portfolio_id, exit_date DESC) WHERE exit_date IS NOT NULL;

-- ── portfolio_event_log ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolio_event_log (
    id           BIGSERIAL PRIMARY KEY,
    portfolio_id BIGINT NOT NULL REFERENCES portfolio_definitions(id),
    event_date   TEXT NOT NULL,
    event_type   TEXT NOT NULL CHECK (event_type IN ('ENTER','EXIT_SL','EXIT_TARGET','EXIT_TIME','EXIT_TRAIL')),
    symbol       TEXT NOT NULL,
    price        DOUBLE PRECISION NOT NULL,
    reason_text  TEXT NOT NULL,        -- trader-voice, public
    position_id  BIGINT REFERENCES portfolio_positions(id),
    created_at   TEXT NOT NULL DEFAULT (TIMESTAMPTZ('now'))
);
CREATE INDEX IF NOT EXISTS ix_port_event_recent ON portfolio_event_log(portfolio_id, event_date DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_port_event_type   ON portfolio_event_log(portfolio_id, event_type, event_date DESC);

-- ── portfolio_equity_history ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolio_equity_history (
    id                     BIGSERIAL PRIMARY KEY,
    portfolio_id           BIGINT NOT NULL REFERENCES portfolio_definitions(id),
    trade_date             TEXT NOT NULL,
    cash_balance           DOUBLE PRECISION NOT NULL,
    deployed_capital       DOUBLE PRECISION NOT NULL,        -- sum of capital_committed for open positions
    mtm_unrealized         DOUBLE PRECISION NOT NULL,        -- sum of (qty * close - capital_committed) for open positions
    total_equity           DOUBLE PRECISION NOT NULL,        -- cash + deployed + mtm
    daily_pnl_rs           DOUBLE PRECISION NOT NULL DEFAULT 0,
    daily_pnl_pct          DOUBLE PRECISION NOT NULL DEFAULT 0,
    cumulative_return_pct  DOUBLE PRECISION NOT NULL DEFAULT 0,   -- vs. virtual_capital_start
    max_drawdown_pct       DOUBLE PRECISION NOT NULL DEFAULT 0,   -- worst dd-to-date
    peak_equity            DOUBLE PRECISION NOT NULL DEFAULT 0,   -- running peak (for dd calc)
    n_open_positions       BIGINT NOT NULL DEFAULT 0,
    n_closed_today         BIGINT NOT NULL DEFAULT 0,
    n_opened_today         BIGINT NOT NULL DEFAULT 0,
    created_at             TEXT NOT NULL DEFAULT (TIMESTAMPTZ('now')),
    UNIQUE(portfolio_id, trade_date)
);
CREATE INDEX IF NOT EXISTS ix_port_eq_recent ON portfolio_equity_history(portfolio_id, trade_date DESC);

-- ── portfolio_monthly_performance ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolio_monthly_performance (
    id              BIGSERIAL PRIMARY KEY,
    portfolio_id    BIGINT NOT NULL REFERENCES portfolio_definitions(id),
    year            BIGINT NOT NULL,
    month           BIGINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    end_equity_rs   DOUBLE PRECISION NOT NULL,
    return_pct      DOUBLE PRECISION NOT NULL,            -- vs prior-month equity (or vs ₹5 L for Jan)
    is_partial      BIGINT NOT NULL DEFAULT 0 CHECK (is_partial IN (0,1)),
    created_at      TEXT NOT NULL DEFAULT (TIMESTAMPTZ('now')),
    UNIQUE(portfolio_id, year, month)
);
CREATE INDEX IF NOT EXISTS ix_port_monthly ON portfolio_monthly_performance(portfolio_id, year, month);

-- ── portfolio_yearly_performance ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolio_yearly_performance (
    id              BIGSERIAL PRIMARY KEY,
    portfolio_id    BIGINT NOT NULL REFERENCES portfolio_definitions(id),
    year            BIGINT NOT NULL,
    start_cap_rs    DOUBLE PRECISION NOT NULL,            -- always ₹5 L per V3 yearly-reset model
    end_equity_rs   DOUBLE PRECISION NOT NULL,
    pnl_rs          DOUBLE PRECISION NOT NULL,
    return_pct      DOUBLE PRECISION NOT NULL,
    max_drawdown_pct DOUBLE PRECISION NOT NULL,
    win_rate_pct    DOUBLE PRECISION NOT NULL,
    n_closed_trades BIGINT NOT NULL,
    winning_months  BIGINT,                  -- 0..12 (NULL if month data unavailable)
    is_partial      BIGINT NOT NULL DEFAULT 0 CHECK (is_partial IN (0,1)),    -- 2026 = partial
    mining_window   TEXT,                     -- e.g. "2020-2020" (debug field)
    created_at      TEXT NOT NULL DEFAULT (TIMESTAMPTZ('now')),
    UNIQUE(portfolio_id, year)
);
CREATE INDEX IF NOT EXISTS ix_port_yearly ON portfolio_yearly_performance(portfolio_id, year);

-- ── portfolio_definitions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolio_definitions (
    id                    BIGSERIAL PRIMARY KEY,
    slug                  TEXT NOT NULL UNIQUE,
    name                  TEXT NOT NULL,
    tagline               TEXT NOT NULL,
    entry_rule            TEXT NOT NULL,                 -- machine-readable rule key
    exit_rule             TEXT NOT NULL,                 -- machine-readable rule key
    virtual_capital_start DOUBLE PRECISION NOT NULL,                 -- ₹ starting capital, locked
    start_date            TEXT NOT NULL,                 -- 'YYYY-MM-DD' inception
    is_active             BIGINT NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
    display_order         BIGINT NOT NULL DEFAULT 0,
    -- Validated backtest metrics (operator-locked, sourced from offline backtests)
    backtest_metrics_json TEXT,
    -- Parameter BYTEA (per-trade %, hold days, SL/target/trail). JSON so we can
    -- add new fields without migrating.
    parameters_json       TEXT NOT NULL,
    created_at            TEXT NOT NULL DEFAULT (TIMESTAMPTZ('now')),
    updated_at            TEXT NOT NULL DEFAULT (TIMESTAMPTZ('now'))
, narrative_json TEXT);
CREATE INDEX IF NOT EXISTS ix_portfolio_def_active ON portfolio_definitions(is_active, display_order);

-- ── falcon_trade_events ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS falcon_trade_events (
    id                BIGSERIAL PRIMARY KEY,
    detected_at       TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    kind              TEXT NOT NULL,         -- NEAR_SL | NEAR_TARGET | HW_REACHED | BREACHED_SL | TARGET_HIT | TIME_STOP_DUE | SL_REPLACED | EXIT_PLACED
    severity          TEXT NOT NULL,         -- info | warn | critical
    detail            TEXT,
    auto_action_taken BIGINT DEFAULT 0,     -- 0|1
    related_kite_id   TEXT
);
CREATE INDEX IF NOT EXISTS idx_trade_events_symbol ON falcon_trade_events(symbol, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_trade_events_kind   ON falcon_trade_events(kind, detected_at DESC);

-- ── falcon_trade_orders ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS falcon_trade_orders (
    id                BIGSERIAL PRIMARY KEY,
    batch_id          TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    side              TEXT NOT NULL,              -- BUY | SELL
    role              TEXT NOT NULL,              -- ENTRY | STOP | SMOKE | EXIT
    is_averaging      BIGINT DEFAULT 0,          -- 0 | 1
    kite_order_id     TEXT,
    qty               BIGINT NOT NULL,
    price             DOUBLE PRECISION,
    trigger_price     DOUBLE PRECISION,
    product           TEXT NOT NULL,              -- MTF | CNC
    order_type        TEXT NOT NULL,              -- MARKET | LIMIT | SL | SL-M
    status            TEXT NOT NULL,              -- PENDING | PLACING | PLACED | REJECTED | CANCELLED | FAILED
    placed_at         TEXT,
    filled_at         TEXT,
    error             TEXT,
    idempotency_key   TEXT NOT NULL UNIQUE, filled_qty BIGINT DEFAULT 0, last_reconciled_at TEXT,
    FOREIGN KEY (batch_id) REFERENCES falcon_trade_runs(batch_id)
);
CREATE INDEX IF NOT EXISTS idx_trade_orders_batch  ON falcon_trade_orders(batch_id);
CREATE INDEX IF NOT EXISTS idx_trade_orders_symbol ON falcon_trade_orders(symbol);
CREATE INDEX IF NOT EXISTS idx_trade_orders_role   ON falcon_trade_orders(role);

-- ── falcon_trade_runs ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS falcon_trade_runs (
    batch_id            TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    status              TEXT NOT NULL,            -- PENDING | PLACING | COMPLETED | ABORTED
    signal_date         TEXT NOT NULL,
    entry_date          TEXT NOT NULL,
    selected_symbols    TEXT NOT NULL,            -- JSON array
    total_capital       BIGINT NOT NULL,
    per_trade           BIGINT NOT NULL,
    use_leverage        BIGINT NOT NULL,         -- 0 | 1
    hold_days           BIGINT NOT NULL,
    sl_pct              DOUBLE PRECISION NOT NULL,
    trail_trigger_pct   DOUBLE PRECISION NOT NULL,
    trail_lookback_days BIGINT NOT NULL,
    n_attempted         BIGINT DEFAULT 0,
    n_filled            BIGINT DEFAULT 0,
    n_failed            BIGINT DEFAULT 0,
    n_overlap           BIGINT DEFAULT 0,
    n_skip_action       BIGINT DEFAULT 0,
    n_average_action    BIGINT DEFAULT 0,
    total_deployed      BIGINT DEFAULT 0,
    notes               TEXT
);
CREATE INDEX IF NOT EXISTS idx_trade_runs_entry ON falcon_trade_runs(entry_date, status);

-- ── falcon_position_state ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS falcon_position_state (
    symbol            TEXT PRIMARY KEY,
    managed_by        TEXT NOT NULL,         -- falcon | external
    qty               BIGINT NOT NULL,
    avg_entry         DOUBLE PRECISION NOT NULL,
    initial_sl_price  DOUBLE PRECISION NOT NULL,         -- entry × (1 + sl_pct/100), set at adoption
    current_sl_price  DOUBLE PRECISION NOT NULL,         -- moves up when HW reached + trail re-place
    target_price      DOUBLE PRECISION NOT NULL,         -- entry × (1 + trail_trigger_pct/100)
    high_water_price  DOUBLE PRECISION NOT NULL,         -- max current_price seen since entry
    hw_reached        BIGINT DEFAULT 0,     -- 0|1, set true once high_water >= target
    trail_active      BIGINT DEFAULT 0,     -- 0|1, set true after first SL re-place
    entry_date        TEXT,
    hold_days_max     BIGINT DEFAULT 7,
    last_seen_price   DOUBLE PRECISION,
    last_polled_at    TEXT,
    last_event_kind   TEXT,                  -- e.g. 'NEAR_SL', 'HW_REACHED', 'BREACHED'
    last_event_at     TEXT
, product TEXT, sl_kite_order_id TEXT, last_action_at TEXT, exit_lock BIGINT DEFAULT 0, exit_initiated_by TEXT, session_id TEXT, broker_profile TEXT, total_allocated_capital DOUBLE PRECISION, unrealised_pnl DOUBLE PRECISION);

-- ── falcon_position_first_seen ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS falcon_position_first_seen (
    symbol            TEXT PRIMARY KEY,
    first_seen_date   TEXT NOT NULL,        -- ISO YYYY-MM-DD
    first_seen_at     TEXT NOT NULL,        -- ISO timestamp
    detected_via      TEXT NOT NULL,        -- 'falcon_order' | 'kite_t1' | 'first_poll'
    qty_at_first_seen BIGINT
);

-- ── falcon_premarket_staging ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS falcon_premarket_staging (
    id              BIGSERIAL PRIMARY KEY,
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

-- ── falcon_live_decisions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS falcon_live_decisions (
    id              BIGSERIAL PRIMARY KEY,
    signal_date     TEXT NOT NULL,                  -- previous trading day (when signals were emitted)
    entry_date      TEXT NOT NULL,                  -- today (the day we're deciding for)
    cycle           TEXT NOT NULL,                  -- '0930' | '0945' | '1000'
    rank            BIGINT NOT NULL,
    symbol          TEXT NOT NULL,
    sector          TEXT,
    score           DOUBLE PRECISION,
    tier            TEXT
                    CHECK (tier IN ('ELITE','HIGH','MID','LOWER','TAIL','DEEP-TAIL')),
    action          TEXT
                    CHECK (action IN ('ENTER','WAIT','SKIP')),
    reason          TEXT,                           -- plain-English rule outcome
    -- Decision provenance: which cycle FINALIZED this row's action.
    -- Lock semantics (operator policy 2026-05-14):
    --   9:30 ENTER/SKIP decisions are LOCKED. 9:45 and 10:00 cycles only re-
    --   evaluate WAIT rows from earlier cycles. So a row in cycle='1000' may
    --   carry decided_at_cycle='0930' if that's when ENTER/SKIP was decided.
    --   UI renders "decided at 9:30 IST" badge so traders know the action
    --   isn't going to flip on them mid-morning.
    decided_at_cycle TEXT
                     CHECK (decided_at_cycle IN ('0930','0945','1000')),
    ret_15          DOUBLE PRECISION,                           -- first 15-min return %
    vol_pct         DOUBLE PRECISION,                           -- 15-min vol / yesterday's total %
    close_loc       DOUBLE PRECISION,                           -- 15-min close location in 15-min range
    computed_at     TEXT NOT NULL,                  -- IST ISO
    UNIQUE(entry_date, cycle, rank),                -- replace-in-place on re-run
    CHECK (cycle IN ('0930','0945','1000'))
);
CREATE INDEX IF NOT EXISTS ix_live_dec_date_cycle ON falcon_live_decisions(entry_date, cycle);
CREATE INDEX IF NOT EXISTS ix_live_dec_action ON falcon_live_decisions(entry_date, cycle, action);

-- ── falcon_auth_log ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS falcon_auth_log (
    id              BIGSERIAL PRIMARY KEY,
    attempt_at      TEXT NOT NULL,                 -- IST ISO
    attempt_of_day  BIGINT NOT NULL,              -- 1..4 (which morning cycle)
    trigger_kind    TEXT NOT NULL                  -- 'scheduled' | 'manual' | 'magic_link'
                    CHECK (trigger_kind IN ('scheduled','manual','magic_link')),
    status          TEXT NOT NULL                  -- success | failed | skipped
                    CHECK (status IN ('success','failed','skipped','partial')),
    elapsed_ms      BIGINT,
    -- What stage we got to (so failure logs are actionable):
    --   'login_page'   — opened login form
    --   'credentials'  — submitted username/password
    --   'totp'         — submitted TOTP
    --   'request_token'— captured request_token from redirect
    --   'access_token' — exchanged for access_token, wrote to kite_tokens
    stage           TEXT,
    error_code      TEXT,                          -- machine-readable: BAD_CREDS / TOTP_FAILED / TIMEOUT / KITE_API_ERROR / BROWSER_CRASHED / ...
    error_detail    TEXT,                          -- redacted human-readable (NEVER include passwords/totp seed)
    token_preview   TEXT                           -- first 8 chars of new access_token, never the full token
);
CREATE INDEX IF NOT EXISTS ix_auth_log_attempt ON falcon_auth_log(attempt_at DESC);
CREATE INDEX IF NOT EXISTS ix_auth_log_status  ON falcon_auth_log(status, attempt_at DESC);

-- ── falcon_notifications_out ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS falcon_notifications_out (
    id              BIGSERIAL PRIMARY KEY,
    created_at      TEXT NOT NULL DEFAULT (TIMESTAMPTZ('now')),
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

-- ── workflow_health ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workflow_health (
                    id       BIGSERIAL PRIMARY KEY,
                    ts       TEXT NOT NULL,      -- IST ISO when the run completed
                    "check"  TEXT NOT NULL,      -- check name (snake_case; reserved word, quoted)
                    status   TEXT NOT NULL,      -- OK | WARN | FAIL | ALERT_SENT
                    detail   TEXT,               -- 1-line observation
                    healed   BIGINT DEFAULT 0   -- 1 if this check self-healed this run
                );
CREATE INDEX IF NOT EXISTS ix_workflow_health_check_ts ON workflow_health("check", ts);

-- ── sysagent_incidents ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sysagent_incidents (
    incident_id   TEXT PRIMARY KEY,          -- stable signature hash
    ts            TEXT NOT NULL,             -- ISO IST first-seen
    severity      TEXT NOT NULL,             -- OK|WARN|ALERT|CRITICAL
    root_cause    TEXT,                      -- one-line root cause
    summary       TEXT,                      -- human-readable page body
    impacted      TEXT,                      -- JSON list of subsystem names
    source        TEXT NOT NULL DEFAULT 'deterministic',  -- 'llm' | 'deterministic'
    paged         BIGINT NOT NULL DEFAULT 0,
    alert_id      BIGINT,                   -- autotrade_alerts.id if it paged
    detail_json   TEXT
);
CREATE INDEX IF NOT EXISTS idx_sysagent_incidents_ts
    ON sysagent_incidents(ts DESC);

-- ── sysagent_health_snapshots ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sysagent_health_snapshots (
    id             BIGSERIAL PRIMARY KEY,
    ts             TEXT NOT NULL,            -- ISO IST of the run
    overall_status TEXT NOT NULL,           -- OK|WARN|ALERT|CRITICAL|UNKNOWN
    n_signals      BIGINT NOT NULL DEFAULT 0,
    n_unknown      BIGINT NOT NULL DEFAULT 0,
    incident_id    TEXT,                     -- FK-ish into sysagent_incidents (may be NULL)
    signals_json   TEXT NOT NULL             -- JSON list of sanitized HealthSignal dicts
);
CREATE INDEX IF NOT EXISTS idx_sysagent_snapshots_ts
    ON sysagent_health_snapshots(ts DESC);

-- ── strategy_visibility ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS strategy_visibility (
    strategy_id             TEXT PRIMARY KEY,       -- registry strategy_id
    visible_to_power_users  BIGINT NOT NULL DEFAULT 0,  -- 0 hidden | 1 visible
    updated_by              TEXT,                   -- admin user_id / 'operator'
    updated_at              TEXT
);

-- ── falcon_trail_config ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS falcon_trail_config (
    product            TEXT PRIMARY KEY,    -- 'MTF' | 'CNC'
    activate_pct       DOUBLE PRECISION NOT NULL,       -- e.g. 10.0 → trigger at +10%
    lock_pct           DOUBLE PRECISION NOT NULL,       -- e.g. 3.0  → SL floor at +3% once activated
    trail_sl_pct       DOUBLE PRECISION NOT NULL,       -- e.g. 5.0  → SL = current × (1 - 5%)
    trail_profit_pct   DOUBLE PRECISION NOT NULL,       -- e.g. 2.0  → target trails current × (1 + 2%)
    auto_exit_enabled  BIGINT DEFAULT 0,   -- 0|1 — gate auto-actions per product
    initial_sl_pct     DOUBLE PRECISION NOT NULL,       -- e.g. -7.0 (BEFORE activation)
    hold_days_max      BIGINT NOT NULL,    -- e.g. 7
    updated_at         TEXT NOT NULL
, trail_method TEXT NOT NULL DEFAULT 'percentage', trail_lookback BIGINT NOT NULL DEFAULT 10);

-- (transaction owned by pg_migrate.apply_schema)