-- KANIDA.AI Power User Portal — DB schema additions
-- Version: 1 (locked 2026-05-14)
-- All statements idempotent (CREATE IF NOT EXISTS). Safe to run at every boot.
--
-- Convention:
--   power_user_*       — auth/invite/watchlist tables (writes from public surface)
--   falcon_live_*      — scheduler-computed decisions table (writes from scheduler only)
--   falcon_replay_*    — replay cache (writes from public surface or admin re-cache)
--
-- Engine output tables (falcon_signals_live, falcon_promoted_patterns, etc.)
-- are NEVER written by Power User code. Read-only contract.

-- ─── AUTH + INVITE ────────────────────────────────────────────────────

-- power_user_users: anyone who's redeemed an invite + signed in via Google.
--
-- Role semantics:
--   'user'    — standard beta user, redeemed a regular invite code
--   'partner' — trading influencer / content partner (2-3 in the Phase 1 cohort
--               per investor brief); same UI access as 'user' in Phase 1 but
--               flagged for future affiliate / co-marketing features (Phase 1B+).
--   'admin'   — operator. Currently identified by env ADMIN_SECRET header on
--               admin routes; the 'admin' role here is reserved for in-DB
--               admin promotion (Phase 2 — once we have multi-admin needs).
CREATE TABLE IF NOT EXISTS power_user_users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT NOT NULL UNIQUE,
    google_sub      TEXT NOT NULL UNIQUE,           -- Google account ID (stable across email changes)
    display_name    TEXT,
    picture_url     TEXT,
    invite_code     TEXT,                           -- code they used to sign up
    role            TEXT NOT NULL DEFAULT 'user'
                    CHECK (role IN ('user','partner','admin')),
    is_active       INTEGER NOT NULL DEFAULT 1
                    CHECK (is_active IN (0,1)),
    created_at      TEXT NOT NULL,                  -- IST ISO 8601
    last_seen_at    TEXT
);
CREATE INDEX IF NOT EXISTS ix_pu_users_email ON power_user_users(email);
CREATE INDEX IF NOT EXISTS ix_pu_users_google_sub ON power_user_users(google_sub);

-- power_user_invite_codes: admin-issued, one-time-use codes.
-- Format suggestion: "kn-2026-{6-hex}" — easy to dictate over phone.
CREATE TABLE IF NOT EXISTS power_user_invite_codes (
    code            TEXT PRIMARY KEY,
    issued_by       TEXT NOT NULL,                  -- admin email or 'system'
    issued_at       TEXT NOT NULL,                  -- IST ISO
    expires_at      TEXT,                           -- NULL = never expires
    used_by_user_id INTEGER,                        -- FK → power_user_users.id
    used_at         TEXT,
    note            TEXT,                           -- e.g. "Influencer: @username", "Beta cohort A"
    FOREIGN KEY (used_by_user_id) REFERENCES power_user_users(id)
);
CREATE INDEX IF NOT EXISTS ix_pu_invite_used ON power_user_invite_codes(used_by_user_id);

-- power_user_waitlist: emails captured when no invite available.
CREATE TABLE IF NOT EXISTS power_user_waitlist (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT NOT NULL UNIQUE,
    joined_at       TEXT NOT NULL,
    source          TEXT,                           -- 'login_no_invite' | 'landing_cta' | 'demo_replay'
    invite_issued   INTEGER NOT NULL DEFAULT 0
);

-- power_user_watchlists: Phase 1B feature; schema created now for forward-compat.
CREATE TABLE IF NOT EXISTS power_user_watchlists (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    symbol          TEXT NOT NULL,
    added_at        TEXT NOT NULL,
    note            TEXT,
    UNIQUE(user_id, symbol),
    FOREIGN KEY (user_id) REFERENCES power_user_users(id)
);
CREATE INDEX IF NOT EXISTS ix_pu_watchlist_user ON power_user_watchlists(user_id);

-- ─── LIVE TIER DECISIONS ──────────────────────────────────────────────

-- falcon_live_decisions: scheduler writes at 09:30:30 / 09:45 / 10:00 IST.
-- Users read from this table — sub-millisecond, no Kite call per request.
-- All users see the same decisions; signals are universal, only execution differs.
CREATE TABLE IF NOT EXISTS falcon_live_decisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_date     TEXT NOT NULL,                  -- previous trading day (when signals were emitted)
    entry_date      TEXT NOT NULL,                  -- today (the day we're deciding for)
    cycle           TEXT NOT NULL,                  -- '0930' | '0945' | '1000'
    rank            INTEGER NOT NULL,
    symbol          TEXT NOT NULL,
    sector          TEXT,
    score           REAL,
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
    ret_15          REAL,                           -- first 15-min return %
    vol_pct         REAL,                           -- 15-min vol / yesterday's total %
    close_loc       REAL,                           -- 15-min close location in 15-min range
    computed_at     TEXT NOT NULL,                  -- IST ISO
    UNIQUE(entry_date, cycle, rank),                -- replace-in-place on re-run
    CHECK (cycle IN ('0930','0945','1000'))
);
CREATE INDEX IF NOT EXISTS ix_live_dec_date_cycle ON falcon_live_decisions(entry_date, cycle);
CREATE INDEX IF NOT EXISTS ix_live_dec_action ON falcon_live_decisions(entry_date, cycle, action);

-- ─── REPLAY CACHE ─────────────────────────────────────────────────────

-- falcon_replay_cache: featured-date payloads (immutable) + arbitrary-date 24h cache.
CREATE TABLE IF NOT EXISTS falcon_replay_cache (
    replay_date     TEXT PRIMARY KEY,               -- the signal_date being replayed
    payload_json    TEXT NOT NULL,                  -- full Pick[] array + aggregate stats
    is_featured     INTEGER NOT NULL DEFAULT 0,     -- 1 = never expires, hardcoded list of 3
    title           TEXT,                           -- "The Blowout" for featured; NULL for random
    computed_at     TEXT NOT NULL,
    expires_at      TEXT                            -- NULL when is_featured=1
);
CREATE INDEX IF NOT EXISTS ix_replay_featured ON falcon_replay_cache(is_featured, replay_date);

-- ─── ZERODHA AUTO-AUTH (production hardening, Sprint 5c-1) ────────────

-- falcon_auth_log: one row per Zerodha auth attempt (auto OR manual).
-- The bot fires every 30 min from 06:30 to 16:30 IST on weekdays (21 cycles
-- after the 2026-05-27 expansion; was 4 morning cycles originally). Each
-- attempt writes a row whether it succeeded, failed at login, failed at
-- exchange, or threw. Read by the admin widget and used to gate later
-- attempts (skip if today's log success + the live token still works).
CREATE TABLE IF NOT EXISTS falcon_auth_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_at      TEXT NOT NULL,                 -- IST ISO
    attempt_of_day  INTEGER NOT NULL,              -- 1..21 (cycle index, post 2026-05-27)
    trigger_kind    TEXT NOT NULL                  -- 'scheduled' | 'manual' | 'magic_link'
                    CHECK (trigger_kind IN ('scheduled','manual','magic_link')),
    status          TEXT NOT NULL                  -- success | failed | skipped
                    CHECK (status IN ('success','failed','skipped','partial')),
    elapsed_ms      INTEGER,
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


-- power_user_push_subscriptions: admin (and later, opt-in users) get a Web
-- Push subscription tied to their browser/device. We send notifications
-- via VAPID when Layer 1 auth attempts fail.
CREATE TABLE IF NOT EXISTS power_user_push_subscriptions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER,                       -- NULL = admin (no user row needed)
    endpoint        TEXT NOT NULL UNIQUE,          -- the push service URL (Chrome/Firefox/Safari)
    p256dh          TEXT NOT NULL,                 -- key for encrypting payload
    auth            TEXT NOT NULL,                 -- key for encrypting payload
    user_agent      TEXT,                          -- for debugging which device this is
    created_at      TEXT NOT NULL,
    last_sent_at    TEXT,
    last_send_error TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1
                    CHECK (is_active IN (0,1))
);
CREATE INDEX IF NOT EXISTS ix_push_subs_active ON power_user_push_subscriptions(is_active, user_id);


-- power_user_magic_links: one-time short-lived tokens for "tap notification
-- to refresh Zerodha auth" flow. 15-minute TTL, single-use.
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

-- ─── OBSERVABILITY ────────────────────────────────────────────────────

-- power_user_request_log: every API request gets a row.
-- Used for: DAU/retention metrics, anonymous rate-limit window, debugging.
-- 30-day retention is enforced by a nightly prune job (added in Phase 1B).
CREATE TABLE IF NOT EXISTS power_user_request_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER,                        -- NULL for anonymous; FK loose (no cascade)
    route           TEXT NOT NULL,                  -- e.g. '/api/power/picks/today'
    status_code     INTEGER NOT NULL,
    latency_ms      INTEGER,
    created_at      TEXT NOT NULL,                  -- IST ISO
    ip_hash         TEXT                            -- SHA256(ip + UA) — never raw IP
);
CREATE INDEX IF NOT EXISTS ix_pulog_created   ON power_user_request_log(created_at);
CREATE INDEX IF NOT EXISTS ix_pulog_iphash_t  ON power_user_request_log(ip_hash, created_at);
CREATE INDEX IF NOT EXISTS ix_pulog_user      ON power_user_request_log(user_id, created_at);

-- ─── BILLING (Razorpay) — Migration 0001, DEV (SQLite) mirror ─────────────
--
-- DIALECT NOTE: This file boots LOCAL DEV (SQLite). The canonical PRODUCTION
-- migration is backend/power_user/migrations/0001_billing.sql, written for
-- PostgreSQL (Supabase, the App DB). The two MUST stay logically equivalent;
-- only the dialect differs:
--     Postgres            -> SQLite (dev)
--     BIGSERIAL PRIMARY KEY  -> INTEGER PRIMARY KEY AUTOINCREMENT
--     BIGINT                 -> INTEGER
--     TIMESTAMPTZ            -> TEXT (IST ISO 8601, same convention as rest of file)
--     JSONB                  -> TEXT (JSON stored as text in dev)
--     now()                  -> handled in app code (no DEFAULT now() in SQLite)
--
-- google_sub NOT NULL RELAXATION — DEV STRATEGY:
--   The Postgres migration does `ALTER COLUMN google_sub DROP NOT NULL` so open
--   email signup (M5) can create users with no Google account. SQLite CANNOT
--   drop a NOT NULL constraint without a full table rebuild (create-new ->
--   copy -> drop -> rename), which is risky against a live dev DB and is
--   explicitly avoided here. Instead, in DEV the open-signup path writes a
--   SYNTHETIC non-null value:  google_sub = 'email:' || sha256(lower(email))
--   so the existing NOT NULL is never violated. The REAL relaxation (true NULL
--   google_sub) happens only in the Postgres migration in production. This
--   keeps dev and prod behaviourally aligned at the application layer while
--   respecting SQLite's ALTER limitations.

-- power_user_users billing columns (additive; INV7).
--
-- IMPORTANT — these ADD COLUMN statements are DELIBERATELY NOT in this file.
-- This file is run via sqlite3.executescript() at EVERY boot (see db_init.py).
-- SQLite has no "ADD COLUMN IF NOT EXISTS", so a bare ALTER here would raise
-- "duplicate column name" on the second boot and, inside executescript(), would
-- abort the ENTIRE schema init and crash dev startup.
--
-- They are therefore applied by db_init.py as guarded, per-statement ALTERs
-- wrapped in try/except OperationalError("duplicate column") — exactly the same
-- pattern already used for portfolio_definitions.narrative_json. The intended
-- DDL (for reference / parity with the Postgres migration) is:
--   ALTER TABLE power_user_users ADD COLUMN billing_plan TEXT DEFAULT 'founding';
--   ALTER TABLE power_user_users ADD COLUMN razorpay_customer_id TEXT;
--   ALTER TABLE power_user_users ADD COLUMN subscription_status TEXT DEFAULT 'active';
-- followed by the founding/active backfill (UPDATE ... WHERE billing_plan IS NULL).
--
-- SQLite ADD COLUMN also cannot carry a CHECK against the new column's domain
-- the way the Postgres migration does; the billing_plan domain
-- (founding/comp/paid/blocked) is enforced at the application layer in dev.
-- Pre-existing rows come out 'founding'/'active' (INV1) via the db_init backfill.

-- power_user_subscriptions (NEW) — SQLite mirror of CONTRACT §1.2.
CREATE TABLE IF NOT EXISTS power_user_subscriptions (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id                   INTEGER NOT NULL,
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

-- power_user_billing_events (NEW) — SQLite mirror of CONTRACT §1.3.
-- razorpay_event_id UNIQUE NOT NULL = idempotency key (dedupe duplicate webhooks).
-- payload is JSON stored as TEXT in dev (JSONB in prod).
CREATE TABLE IF NOT EXISTS power_user_billing_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id             INTEGER,                          -- nullable: event may arrive pre-link
    razorpay_event_id   TEXT UNIQUE NOT NULL,             -- idempotency key
    event_type          TEXT NOT NULL,                    -- e.g. subscription.activated
    payload             TEXT NOT NULL,                    -- raw event JSON (JSONB in prod)
    received_at         TEXT NOT NULL,                    -- IST ISO; app sets (no now() default)
    FOREIGN KEY (user_id) REFERENCES power_user_users(id)
);
CREATE INDEX IF NOT EXISTS ix_pu_bill_events_user ON power_user_billing_events(user_id);
CREATE INDEX IF NOT EXISTS ix_pu_bill_events_type ON power_user_billing_events(event_type, received_at);
