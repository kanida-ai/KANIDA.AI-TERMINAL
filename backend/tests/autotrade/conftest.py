"""Test fixtures for the AutoTrade suite.

Spins up an ISOLATED temp SQLite DB (FALCON_DB_PATH set BEFORE any falcon import
so falcon.config.FALCON_DB resolves to it), seeds the minimal falcon_position_state
+ falcon_signals_live tables the existing schema would create, then runs the
additive AutoTrade migrations on top. No real broker, no real Kite, no real
orders are ever used.
"""
import os
import sqlite3
import tempfile
import uuid

import pytest

# CRITICAL: set the DB path before importing anything under falcon/autotrade,
# because falcon.config.FALCON_DB is captured at import time.
_TMP_DB = os.path.join(tempfile.gettempdir(),
                       f"kanida_autotrade_test_{uuid.uuid4().hex}.db")
os.environ["FALCON_DB_PATH"] = _TMP_DB
os.environ.setdefault("FALCON_OPERATOR_TOKEN", "test-operator-token")
# Master live-trade switch stays OFF for all tests — defence in depth.
os.environ.pop("FALCON_AUTOTRADE_ENABLED", None)


def _seed_base_schema(path: str) -> None:
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS falcon_position_state (
            symbol TEXT PRIMARY KEY,
            managed_by TEXT NOT NULL,
            product TEXT,
            qty INTEGER NOT NULL,
            avg_entry REAL NOT NULL,
            initial_sl_price REAL NOT NULL,
            current_sl_price REAL NOT NULL,
            target_price REAL NOT NULL,
            high_water_price REAL NOT NULL,
            hw_reached INTEGER DEFAULT 0,
            trail_active INTEGER DEFAULT 0,
            sl_kite_order_id TEXT,
            last_action_at TEXT,
            entry_date TEXT,
            hold_days_max INTEGER DEFAULT 7,
            last_seen_price REAL,
            last_polled_at TEXT,
            last_event_kind TEXT,
            last_event_at TEXT
        );
        CREATE TABLE IF NOT EXISTS falcon_signals_live (
            signal_date TEXT, entry_date TEXT, rank INTEGER, symbol TEXT,
            sector TEXT, n_fires INTEGER, score REAL, close_at_signal REAL,
            avg_value_60d REAL, engine_version TEXT, emitted_at TEXT
        );
        CREATE TABLE IF NOT EXISTS falcon_trade_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, detected_at TEXT, symbol TEXT,
            kind TEXT, severity TEXT, detail TEXT, auto_action_taken INTEGER,
            related_kite_id TEXT
        );
    """)
    con.commit()
    con.close()


@pytest.fixture(scope="session", autouse=True)
def _db():
    _seed_base_schema(_TMP_DB)
    from autotrade.db_migrations import run_migrations
    run_migrations()
    yield
    try:
        os.remove(_TMP_DB)
    except OSError:
        pass


@pytest.fixture
def clean_positions():
    """Wipe positions between tests."""
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute("DELETE FROM falcon_position_state")
        con.execute("DELETE FROM autotrade_portfolio_snapshots")
        con.execute("DELETE FROM autotrade_kill_switch_log")
        con.commit()
    yield


def seed_signals(symbols_ranks):
    """Insert Falcon picks. symbols_ranks = [(symbol, rank, score, close), ...]."""
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute("DELETE FROM falcon_signals_live")
        for sym, rank, score, close in symbols_ranks:
            con.execute(
                """INSERT INTO falcon_signals_live
                   (signal_date, entry_date, rank, symbol, sector, n_fires,
                    score, close_at_signal, avg_value_60d, engine_version, emitted_at)
                   VALUES ('2026-06-24','2026-06-25',?,?,?,?,?,?,?,?,?)""",
                (rank, sym, "TEST", 10, score, close, 1e7, "7.1.0",
                 "2026-06-24T09:00:00"),
            )
        con.commit()
