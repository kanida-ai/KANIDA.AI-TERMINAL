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

        for t in (
            "autotrade_positions",
            "autotrade_sessions", "autotrade_config_presets",
            "autotrade_broker_profiles", "autotrade_slippage",
            "autotrade_portfolio_snapshots", "autotrade_kill_switch_log",
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
    gtt_target      REAL
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
"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run_migrations())
