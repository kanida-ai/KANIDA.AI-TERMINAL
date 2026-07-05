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
# DETERMINISM for the trading-day fire gate: freeze "now" to a known NSE trading
# day DURING market hours (Thu 2026-06-25 10:00 IST) so the firing tests in the
# existing suite fire deterministically regardless of the wall clock. The
# trading-day rule's OWN tests override / clear this per-test via set_fake_now().
os.environ["FALCON_AUTOTRADE_FAKE_NOW"] = "2026-06-25T10:00:00"


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
    # Default: tests drive session.tick() manually, so the background tick driver
    # must NOT auto-start (it would race assertions on a shared temp DB). The
    # dedicated auto-fire test re-enables it for its own scope.
    from autotrade.monitoring import tick_driver
    tick_driver.set_autostart(False)
    # Same for the sub-second WS driver — off by default in tests so its daemon
    # thread can't race assertions on the shared temp DB.
    from autotrade.monitoring import ws_driver
    ws_driver.set_autostart(False)
    # Same for the intraday-basket square-off scheduler.
    from autotrade.monitoring import square_off_scheduler
    square_off_scheduler.set_autostart(False)
    yield
    try:
        os.remove(_TMP_DB)
    except OSError:
        pass


@pytest.fixture
def clean_positions():
    """Wipe positions between tests + stop any background tick drivers so their
    daemon threads can't mutate the shared temp DB across tests."""
    from falcon.db import falcon_conn
    from autotrade.monitoring import tick_driver
    from autotrade.monitoring import entry_scheduler
    from autotrade.monitoring import ws_driver
    from autotrade.monitoring import square_off_scheduler
    from autotrade.monitoring import fire_guard

    def _stop_all_drivers():
        with tick_driver._LOCK:
            drivers = list(tick_driver._DRIVERS.values())
        for drv in drivers:
            drv.stop()
        for drv in drivers:
            drv._thread.join(timeout=2.0)
        # Also stop any sub-second WS drivers (FEATURE 2 daemon threads).
        with ws_driver._LOCK:
            wsdrv = list(ws_driver._DRIVERS.values())
        for drv in wsdrv:
            drv.stop()
        for drv in wsdrv:
            drv._thread.join(timeout=2.0)
        # Also stop any armed entry schedulers so their daemon threads can't
        # fire across tests on the shared temp DB.
        with entry_scheduler._LOCK:
            scheds = list(entry_scheduler._SCHEDULERS.values())
        for sch in scheds:
            sch.stop()
        for sch in scheds:
            sch._thread.join(timeout=2.0)
        # Also stop any armed square-off schedulers (intraday_basket).
        with square_off_scheduler._LOCK:
            sqscheds = list(square_off_scheduler._SCHEDULERS.values())
        for sch in sqscheds:
            sch.stop()
        for sch in sqscheds:
            sch._thread.join(timeout=2.0)
        # Reset the per-session fire guard so a session_id reused across tests
        # isn't stuck "already fired".
        with fire_guard._LOCK:
            fire_guard._FIRED.clear()

    _stop_all_drivers()
    with falcon_conn() as con:
        # autotrade_positions is the ONLY session-position store now. We also
        # clear falcon_position_state to PROVE (in the regression test) that the
        # autotrade path never writes into it.
        con.execute("DELETE FROM autotrade_positions")
        con.execute("DELETE FROM falcon_position_state")
        con.execute("DELETE FROM autotrade_sessions")
        con.execute("DELETE FROM autotrade_portfolio_snapshots")
        con.execute("DELETE FROM autotrade_kill_switch_log")
        con.execute("DELETE FROM autotrade_ladders")
        con.commit()
    yield
    _stop_all_drivers()


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
