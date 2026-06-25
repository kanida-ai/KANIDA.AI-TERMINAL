"""AutoTrade DB migrations — idempotent, additive only.

Runs against the Falcon universe DB via falcon.db.falcon_conn (same DB the
existing trade layer uses). Safe to call repeatedly (IF NOT EXISTS + ALTER
guards). Called once at app boot from autotrade.api.autotrade_routes import,
and also exposed for tests / manual invocation.

This module ADDS the following without touching any existing table definition:
  * falcon_position_state.exit_lock          (INTEGER 0/1)  — exit gate lock
  * falcon_position_state.exit_initiated_by  (TEXT)         — who claimed exit
  * falcon_position_state.session_id         (TEXT)         — links to session
  * falcon_position_state.broker_profile     (TEXT)         — owning broker
  * falcon_position_state.total_allocated_capital (REAL)    — session denom snapshot
  * falcon_position_state.unrealised_pnl     (REAL)         — last computed uPnL
  * autotrade_sessions          — TradingSessionConfig persistence + status
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

        for t in (
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
