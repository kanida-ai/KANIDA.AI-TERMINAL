"""AutoTrade DB migrations — idempotent, additive only.

Runs against the Falcon universe DB via falcon.db.falcon_conn (same DB the
existing trade layer uses). Safe to call repeatedly (IF NOT EXISTS + ALTER
guards). Called once at app boot from autotrade.api.autotrade_routes import,
and also exposed for tests / manual invocation.

DATA-ISOLATION (CRITICAL): autotrade session positions live in their OWN table,
`autotrade_positions`, keyed by (session_id, symbol[, broker_profile]). The
autotrade system must NEVER read/write/upsert/lock falcon_position_state for
session positions — that table belongs to the existing Falcon swing system and
its PRIMARY KEY is `symbol`, so a paper session writing into it would overwrite
a real held position. See the regression test in tests/autotrade.

This module ADDS the following without touching any existing table definition:
  * autotrade_positions         — ISOLATED session position store (the fix)
  * falcon_position_state.exit_lock          (INTEGER 0/1)  — legacy, now unused
  * falcon_position_state.exit_initiated_by  (TEXT)         — legacy, now unused
  * falcon_position_state.session_id         (TEXT)         — legacy, now unused
  * falcon_position_state.broker_profile     (TEXT)         — legacy, now unused
  * falcon_position_state.total_allocated_capital (REAL)    — legacy, now unused
  * falcon_position_state.unrealised_pnl     (REAL)         — legacy, now unused
    (These columns are kept as-is — harmless — and are NOT dropped, but the
     autotrade code no longer touches falcon_position_state at all.)
  * autotrade_sessions          — TradingSessionConfig persistence + status
                                  (+ invested_basis: frozen Σ qty*avg_price)
  * autotrade_config_presets    — named saved config presets
  * autotrade_broker_profiles   — BrokerProfile rows (no plaintext secrets)
  * autotrade_slippage          — per-fill slippage record
  * autotrade_portfolio_snapshots — periodic gross_return snapshots
  * autotrade_kill_switch_log   — kill-switch fire audit
  * broker_accounts             — PHASE-2 multi-tenant credential vault
                                  (per user×broker×account; Fernet-encrypted)
  * autotrade_sessions.user_id / .broker_account_id  (TEXT, NULLABLE) — bind a
                                  session to a portal user + vaulted account;
                                  NULL = operator/global creds path (today)
  * autotrade_positions.broker_account_id  (TEXT, NULLABLE) — per-account audit
  * broker_accounts.refresh_token_enc / token_expires_at / last_health_at /
    last_health_status / last_error / redirect_state  — Stage-0 broker-agnostic
    auth foundation (all NULLABLE; Kite path unchanged)
"""
from __future__ import annotations

import logging
from typing import List

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.migrations")


# Columns we add to the EXISTING falcon_position_state table. (name, ddl_type, default)
_POSITION_STATE_ADD_COLUMNS = [
    ("exit_lock",                "INTEGER", "0"),
    ("exit_initiated_by",        "TEXT",    "NULL"),
    ("session_id",               "TEXT",    "NULL"),
    ("broker_profile",           "TEXT",    "NULL"),
    ("total_allocated_capital",  "REAL",    "NULL"),
    ("unrealised_pnl",           "REAL",    "NULL"),
]


def _existing_columns(con, table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def _table_exists(con, table: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def run_migrations() -> dict:
    """Apply all idempotent migrations. Returns a manifest of what was added."""
    added_cols: List[str] = []
    created_tables: List[str] = []

    with falcon_conn() as con:
        # ── 1. ALTER-guard the two (+helper) columns on falcon_position_state ──
        # Only ALTER if the base table already exists (it is created by the
        # existing Falcon schema extensions). If it does not exist yet we skip
        # silently — the existing schema bootstrap will create it, and a later
        # migration run picks the columns up. We never CREATE it here.
        if _table_exists(con, "falcon_position_state"):
            have = set(_existing_columns(con, "falcon_position_state"))
            for name, ddl_type, default in _POSITION_STATE_ADD_COLUMNS:
                if name not in have:
                    default_clause = "" if default == "NULL" else f" DEFAULT {default}"
                    con.execute(
                        f"ALTER TABLE falcon_position_state "
                        f"ADD COLUMN {name} {ddl_type}{default_clause}"
                    )
                    added_cols.append(name)

        # ── 2. New tables (all IF NOT EXISTS) ────────────────────────────────
        con.executescript(_SCHEMA_SQL)

        # ── 2b. ALTER-guard the GTT-OCO columns on autotrade_positions ───────
        # FEATURE 1: broker-held per-position OCO backup. gtt_id is the Kite GTT
        # trigger id returned by kite.place_gtt; gtt_stop/gtt_target the levels.
        # Additive + idempotent — only added when missing.
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            for name, ddl_type in (("gtt_id", "TEXT"),
                                   ("gtt_stop", "REAL"),
                                   ("gtt_target", "REAL")):
                if name not in have:
                    con.execute(
                        f"ALTER TABLE autotrade_positions ADD COLUMN {name} {ddl_type}")
                    added_cols.append(name)

        # ── 2b-oid. ALTER-guard the ORDER-ID tracking columns on
        # autotrade_positions (RECONCILIATION FRAMEWORK Phase 1). entry_order_id
        # is the broker order-id of the ENTRY fill; exit_order_id the broker
        # order-id of the EXIT fill (or the GTT-fired order). Both NULLABLE — every
        # existing row stays NULL (the reconcilers handle absent ids), so this is
        # byte-for-byte unchanged for legacy rows. These enable PER-SESSION
        # attribution of a broker order (never the account aggregate). Additive +
        # idempotent.
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            for name in ("entry_order_id", "exit_order_id"):
                if name not in have:
                    con.execute(
                        f"ALTER TABLE autotrade_positions ADD COLUMN {name} TEXT")
                    added_cols.append(name)

        # ── 2b-dir. ALTER-guard `direction` on autotrade_positions ────────────
        # FUTURES long/short. Per-position direction so the P&L sign + exit side
        # invert ONLY for shorts. DEFAULT 'long' → every existing row + every
        # equity/long position is byte-for-byte unchanged (sign +1). Additive +
        # idempotent. "long" | "short".
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            if "direction" not in have:
                con.execute(
                    "ALTER TABLE autotrade_positions "
                    "ADD COLUMN direction TEXT NOT NULL DEFAULT 'long'")
                added_cols.append("direction")

        # ── 2c. ALTER-guard invested_basis on autotrade_sessions ─────────────
        # INVESTED-CAPITAL-BASIS feature: the FROZEN sum of qty*avg_price across
        # the session's positions AT ENTRY. This is the product-aware capital
        # actually put to work (MTF = leveraged invested value; CNC = deployed
        # cash) and is the denominator the kill switch / gross return measure
        # against. Captured ONCE in session._fire_entries after orders are
        # placed; never updated as positions close. Additive + idempotent.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            if "invested_basis" not in have:
                con.execute(
                    "ALTER TABLE autotrade_sessions ADD COLUMN invested_basis REAL")
                added_cols.append("invested_basis")

        # ── 2d. ALTER-guard the INTRADAY-BASKET trail state on autotrade_sessions
        # strategy=="intraday_basket" persists its trailing-profit state here so a
        # resumed RUNNING session restores armed+peak and the trail continues
        # mid-day. trail_armed: 0/1; trail_peak: the high-water gross return G.
        # Additive + idempotent; inert for portfolio_kill_switch sessions.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            for name, ddl_type, default in (
                    ("trail_armed", "INTEGER", "0"),
                    ("trail_peak", "REAL", "NULL")):
                if name not in have:
                    default_clause = "" if default == "NULL" else f" DEFAULT {default}"
                    con.execute(
                        f"ALTER TABLE autotrade_sessions "
                        f"ADD COLUMN {name} {ddl_type}{default_clause}")
                    added_cols.append(name)

        # ── 2e-mt. PHASE-2 MULTI-TENANT scoping columns (additive, NULLABLE) ──
        # user_id + broker_account_id on autotrade_sessions bind a session to a
        # portal user and a specific vaulted broker account. BOTH default NULL →
        # a NULL on either behaves EXACTLY as today (operator/global creds path).
        # Existing sessions (no user_id/broker_account_id) are byte-for-byte
        # unaffected. broker_account_id on autotrade_positions carries the bound
        # account onto each position for per-account audit (also NULLABLE).
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            for name in ("user_id", "broker_account_id"):
                if name not in have:
                    con.execute(
                        f"ALTER TABLE autotrade_sessions ADD COLUMN {name} TEXT")
                    added_cols.append(name)
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            if "broker_account_id" not in have:
                con.execute(
                    "ALTER TABLE autotrade_positions ADD COLUMN broker_account_id TEXT")
                added_cols.append("broker_account_id")

        # ── 2e-vault. BROKER-AGNOSTIC AUTH FOUNDATION (Stage 0) ───────────────
        # Additive, NULLABLE columns on broker_accounts for refresh-token brokers
        # + connection health + the OAuth CSRF `state` round-trip. All nullable
        # and default-absent → the existing Kite (request-token, no-refresh) path
        # is byte-for-byte unchanged; these are populated only by brokers that use
        # them. refresh_token_enc is FERNET-ENCRYPTED at rest (same as the other
        # *_enc columns); token_expires_at is an absolute ISO-IST stamp for
        # non-daily brokers; last_health_* record the most recent validate() ping.
        if _table_exists(con, "broker_accounts"):
            have = set(_existing_columns(con, "broker_accounts"))
            for name, ddl_type in (
                    ("refresh_token_enc", "BLOB"),
                    ("token_expires_at", "TEXT"),
                    ("last_health_at", "TEXT"),
                    ("last_health_status", "TEXT"),
                    ("last_error", "TEXT"),
                    ("redirect_state", "TEXT")):
                if name not in have:
                    con.execute(
                        f"ALTER TABLE broker_accounts ADD COLUMN {name} {ddl_type}")
                    added_cols.append(name)

        # ── 2e. ALTER-guard the SPEED-PASS latency observability columns ──────
        # entry_latency_ms: fire start → all legs settled (asyncio.gather done).
        # exit_latency_ms: flatten trigger → all positions flat. Persisted so the
        # operator can SEE the end-to-end speed in status(). Additive + idempotent
        # + inert for any existing session. last_tick_age_ms is computed live in
        # status() (now − newest tick used), not persisted.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            for name in ("entry_latency_ms", "exit_latency_ms"):
                if name not in have:
                    con.execute(
                        f"ALTER TABLE autotrade_sessions ADD COLUMN {name} INTEGER")
                    added_cols.append(name)

        # ── 2f. LADDER ORCHESTRATOR — tag each spawned child session with its
        # parent campaign. NULLABLE ladder_id on autotrade_sessions: NULL (the
        # default for every existing/standalone session) means "not part of a
        # ladder" → byte-for-byte unchanged. Set only on children the ladder
        # daily-tick spawns. Additive + idempotent.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            if "ladder_id" not in have:
                con.execute(
                    "ALTER TABLE autotrade_sessions ADD COLUMN ladder_id TEXT")
                added_cols.append("ladder_id")
            # Index the ladder tag AFTER the column exists (executescript above
            # runs before this ALTER, so the index can't live in _SCHEMA_SQL).
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_autotrade_sessions_ladder "
                "ON autotrade_sessions(ladder_id)")

        for t in (
            "autotrade_positions",
            "autotrade_sessions", "autotrade_config_presets",
            "autotrade_broker_profiles", "autotrade_slippage",
            "autotrade_portfolio_snapshots", "autotrade_kill_switch_log",
            "broker_accounts", "autotrade_ladders",
        ):
            if _table_exists(con, t):
                created_tables.append(t)
        con.commit()

    manifest = {"added_columns": added_cols, "tables_present": created_tables}
    log.info("AutoTrade migrations OK: +cols=%s tables=%s",
             added_cols, created_tables)
    return manifest


_SCHEMA_SQL = """
-- ISOLATED autotrade session positions. The autotrade system reads/writes ONLY
-- this table for session positions — never falcon_position_state. Composite
-- uniqueness on (session_id, symbol, broker_profile) so a paper session can
-- never collide with (or overwrite) a real Falcon swing position keyed by
-- `symbol` in falcon_position_state.
CREATE TABLE IF NOT EXISTS autotrade_positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    broker_profile  TEXT,
    symbol          TEXT NOT NULL,
    instrument_type TEXT NOT NULL DEFAULT 'EQ',
    exchange        TEXT,
    qty             INTEGER NOT NULL DEFAULT 0,
    avg_price       REAL NOT NULL DEFAULT 0,
    sl_level        REAL,
    target_price    REAL,
    ltp             REAL,
    unrealised_pnl  REAL,
    status          TEXT NOT NULL DEFAULT 'OPEN',  -- OPEN | CLOSED | EXIT_FAILED
    exit_lock       INTEGER NOT NULL DEFAULT 0,
    exit_initiated_by TEXT,
    opened_at       TEXT,
    closed_at       TEXT,
    exit_price      REAL,
    realised_pnl    REAL,
    close_reason    TEXT,
    -- FEATURE 1: broker-held per-position GTT-OCO backup (LIVE only). gtt_id is
    -- the Kite GTT trigger id; gtt_stop/gtt_target the placed/intended levels.
    gtt_id          TEXT,
    gtt_stop        REAL,
    gtt_target      REAL,
    -- RECONCILIATION FRAMEWORK (Phase 1): the broker order-ids for this position.
    -- entry_order_id = the ENTRY fill's order id; exit_order_id = the EXIT fill's
    -- order id (or the GTT-fired order). Both NULLABLE — enable PER-SESSION
    -- attribution of a broker order by order-id (never the account aggregate).
    entry_order_id  TEXT,
    exit_order_id   TEXT,
    -- FUTURES long/short. 'long' (default) = entry BUY, exit SELL, P&L
    -- (ltp-avg)*qty. 'short' = entry SELL, exit BUY-to-cover, P&L (avg-ltp)*qty.
    -- Every equity/long row is 'long' → byte-for-byte unchanged.
    direction       TEXT NOT NULL DEFAULT 'long'
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_autotrade_positions_sess_sym_prof
    ON autotrade_positions(session_id, symbol, broker_profile);
CREATE INDEX IF NOT EXISTS idx_autotrade_positions_session
    ON autotrade_positions(session_id, status);

-- Sessions: one row per TradingSession. config_json holds the full
-- TradingSessionConfig (sans secrets). status drives the state machine.
CREATE TABLE IF NOT EXISTS autotrade_sessions (
    session_id      TEXT PRIMARY KEY,             -- uuid4 hex
    created_at      TEXT NOT NULL,                -- ISO IST
    started_at      TEXT,
    closed_at       TEXT,
    status          TEXT NOT NULL DEFAULT 'CREATED',
        -- CREATED | RUNNING | KILLING | CLOSED | FAILED
    mode            TEXT NOT NULL DEFAULT 'paper',   -- 'paper' | 'live'
    total_allocated_capital REAL NOT NULL,
    -- FROZEN invested (notional) capital basis = Σ(qty*avg_price) at entry.
    -- Product-aware (MTF leveraged value / CNC cash). The kill-switch + gross
    -- return denominator. NULL until _fire_entries captures it. (Also added via
    -- idempotent ALTER for DBs created before this column.)
    invested_basis  REAL,
    -- INTRADAY-BASKET trailing-profit state (strategy=="intraday_basket").
    -- trail_armed: 0/1 — has the trail armed (G crossed +arm_pct). trail_peak:
    -- the high-water gross return G since arming. Persisted each tick + restored
    -- on boot-resume so the trail continues correctly mid-day. (Also added via
    -- idempotent ALTER for DBs created before these columns.)
    trail_armed     INTEGER DEFAULT 0,
    trail_peak      REAL,
    -- SPEED-PASS latency observability. entry_latency_ms: fire start → all legs
    -- settled. exit_latency_ms: flatten trigger → all positions flat. (Also
    -- added via idempotent ALTER for DBs created before these columns.)
    entry_latency_ms INTEGER,
    exit_latency_ms  INTEGER,
    config_json     TEXT NOT NULL,
    last_gross_return REAL,
    kill_reason     TEXT,
    notes           TEXT
);

-- Named, reusable config presets (UI "save preset").
CREATE TABLE IF NOT EXISTS autotrade_config_presets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    created_at  TEXT NOT NULL,
    config_json TEXT NOT NULL
);

-- Broker profiles. SECRETS ARE NOT STORED HERE — api_key/secret/token live in
-- env / the existing kite_tokens table. We persist only routing + non-secret
-- profile fields, plus a boolean for whether creds are configured.
CREATE TABLE IF NOT EXISTS autotrade_broker_profiles (
    profile_id      TEXT PRIMARY KEY,
    broker_name     TEXT NOT NULL,                -- zerodha | fyers | upstox | angel | dhan
    allocated_capital REAL NOT NULL DEFAULT 0,
    symbols_json    TEXT,                         -- NULL = use Falcon top_n
    rank_low        INTEGER,
    rank_high       INTEGER,
    order_product   TEXT NOT NULL DEFAULT 'CNC',
    instrument_type TEXT NOT NULL DEFAULT 'EQ',
    enabled         INTEGER NOT NULL DEFAULT 1,
    creds_configured INTEGER NOT NULL DEFAULT 0,  -- 1 if env/token present
    created_at      TEXT NOT NULL,
    updated_at      TEXT
);

-- Per-fill slippage record.
CREATE TABLE IF NOT EXISTS autotrade_slippage (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT,
    broker_profile  TEXT,
    symbol          TEXT NOT NULL,
    expected_price  REAL NOT NULL,
    actual_price    REAL NOT NULL,
    slippage_pct    REAL NOT NULL,
    qty             INTEGER NOT NULL,
    recorded_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_autotrade_slippage_session
    ON autotrade_slippage(session_id, recorded_at DESC);

-- Periodic portfolio snapshots (gross return over time, for charts/audit).
CREATE TABLE IF NOT EXISTS autotrade_portfolio_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    snapped_at      TEXT NOT NULL,
    gross_return    REAL NOT NULL,
    total_unrealised REAL NOT NULL,
    total_allocated_capital REAL NOT NULL,
    n_open_positions INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_autotrade_snap_session
    ON autotrade_portfolio_snapshots(session_id, snapped_at DESC);

-- PHASE-2 MULTI-TENANT credential vault. One row per (user, broker, account).
-- api_secret_enc / access_token_enc are FERNET-ENCRYPTED at rest (the key lives
-- outside the DB, via vault.KeyProvider). Secrets are NEVER returned over any
-- API (only status + masked previews). A user holds MANY accounts via distinct
-- account_label values; UNIQUE(user_id,broker,account_label) blocks dup labels
-- but allows unlimited accounts. Per-user isolation: every read is WHERE
-- user_id=?. This table is INDEPENDENT of the legacy kite_tokens table (which
-- the legacy /falcon path keeps using untouched).
CREATE TABLE IF NOT EXISTS broker_accounts (
    broker_account_id TEXT PRIMARY KEY,           -- uuid4 hex (stable handle)
    user_id           TEXT NOT NULL,              -- portal user
    broker            TEXT NOT NULL,              -- zerodha|upstox|angel|dhan|fyers
    account_label     TEXT NOT NULL,              -- user-chosen, e.g. "Main Kite"
    api_key           TEXT NOT NULL,              -- broker app api_key
    api_secret_enc    BLOB,                       -- Fernet-ENCRYPTED api_secret
    access_token_enc  BLOB,                       -- Fernet-ENCRYPTED access_token
    token_date        TEXT,                       -- IST date the token was minted
    token_expiry      TEXT,                       -- ISO IST expiry (broker-specific)
    status            TEXT NOT NULL DEFAULT 'PENDING',
        -- PENDING | ACTIVE | EXPIRED | REVOKED | ERROR
    created_at        TEXT NOT NULL,              -- ISO IST
    updated_at        TEXT,
    last_login_at     TEXT,
    -- BROKER-AGNOSTIC AUTH FOUNDATION (Stage 0), all NULLABLE / default-absent:
    refresh_token_enc  BLOB,   -- Fernet-ENCRYPTED refresh token (brokers that have one)
    token_expires_at   TEXT,   -- absolute ISO-IST expiry (non-daily brokers)
    last_health_at     TEXT,   -- ISO-IST of the last validate() ping
    last_health_status TEXT,   -- ACTIVE | EXPIRED | REVOKED | ERROR
    last_error         TEXT,   -- last health/refresh error detail
    redirect_state     TEXT,   -- CSRF `state` for the OAuth round-trip
    UNIQUE (user_id, broker, account_label)
);
CREATE INDEX IF NOT EXISTS idx_broker_accounts_user
    ON broker_accounts(user_id, status);

-- Kill-switch fire audit.
CREATE TABLE IF NOT EXISTS autotrade_kill_switch_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    fired_at        TEXT NOT NULL,
    trigger_reason  TEXT NOT NULL,
    gross_return    REAL,
    n_positions     INTEGER,
    n_exited_ok     INTEGER,
    n_exit_failed   INTEGER,
    mode            TEXT NOT NULL DEFAULT 'paper',
    detail_json     TEXT
);

-- ── LADDER ORCHESTRATOR (campaign layer) ────────────────────────────────────
-- A "set once, run for a month" campaign that opens/manages/rolls positional
-- baskets every trading day with no further operator input. Each opened basket
-- is a normal positional AutoTrade session (autotrade_sessions) TAGGED with this
-- ladder's ladder_id. This table holds ONLY the campaign-level state; the child
-- sessions + their positions live in the existing isolated tables (never
-- falcon_position_state). per_basket_capital is FROZEN at create (= total/3).
-- Capital accounting is on the TRADER-MONEY (margin) basis: free capital =
-- total_capital − Σ(child.total_allocated_capital of OPEN children); MTF leverage
-- lives INSIDE each child and never inflates this ceiling.
CREATE TABLE IF NOT EXISTS autotrade_ladders (
    ladder_id           TEXT PRIMARY KEY,             -- uuid4 hex
    user_id             TEXT,                         -- portal user (NULL = operator)
    broker_account_id   TEXT,                         -- vaulted account (NULL = global)
    mode                TEXT NOT NULL DEFAULT 'paper', -- 'paper' | 'live' (children inherit)
    total_capital       REAL NOT NULL,                -- the deployed-capital ceiling
    order_product       TEXT NOT NULL DEFAULT 'CNC',  -- CNC | MTF only (MIS rejected)
    per_basket_capital  REAL NOT NULL,                -- FROZEN at create = total/3
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
    alert_active        INTEGER NOT NULL DEFAULT 0,   -- 0/1 down-crossing latch
    last_tick_date      TEXT,                         -- ISO date of the last daily tick (idempotency)
    created_at          TEXT NOT NULL,                -- ISO IST
    updated_at          TEXT
);
CREATE INDEX IF NOT EXISTS idx_autotrade_ladders_user
    ON autotrade_ladders(user_id, status);
"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run_migrations())
