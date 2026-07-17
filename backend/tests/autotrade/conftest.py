"""Test fixtures for the AutoTrade suite.

Spins up an ISOLATED temp SQLite DB (FALCON_DB_PATH set BEFORE any falcon import
so falcon.config.FALCON_DB resolves to it), seeds the minimal falcon_position_state
+ falcon_signals_live tables the existing schema would create, then runs the
additive AutoTrade migrations on top. No real broker, no real Kite, no real
orders are ever used.
"""
import datetime as _dt
import os
import sqlite3
import tempfile
import uuid

import pytest

_IST = _dt.timezone(_dt.timedelta(hours=5, minutes=30))

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


# ── WALL-CLOCK HERMETICITY ───────────────────────────────────────────────────
# The conftest declares a deterministic "now" via FALCON_AUTOTRADE_FAKE_NOW
# (2026-06-25T10:00 IST, an NSE trading day during market hours), honoured by
# autotrade.session.now_ist() — the SEAM.
#
# HISTORY: the TIME-BASED money paths used to BYPASS that seam and read the raw
# wall clock, so this fixture had to freeze `datetime` on the modules from the
# OUTSIDE. As of the clock-seam change those sites are ROUTED through now_ist():
#
#   * autotrade/session.py       — the MIS defensive square-off TICK BACKSTOP
#                                  (default 15:12 IST) in tick() + _tick_tesla,
#                                  and the 09:15–15:29 market-hours guards.
#   * monitoring/trail_engine.py — decide()'s `now` now defaults to the seam
#                                  (via _clock_now()); the SQUARE_OFF branch
#                                  (15:29 IST) still takes precedence over
#                                  ARM / STOP / HOLD.
#
# so the env var above ALONE is now sufficient to make those paths deterministic.
# (Boundary coverage of each routed site lives in test_clock_seam.py, driven
# purely through the seam.)
#
# This fixture is RETAINED because it also pins any *unrouted* datetime.now()
# read on those two modules, and existing tests depend on it. It freezes the
# clock to the SAME instant the seam already advertises, so the two agree and the
# assertion under test is the only variable. It stays TEST-ONLY: with the seam
# unset, production reads the real clock exactly as before.
_FROZEN_NOW_IST = _dt.datetime(2026, 6, 25, 10, 0, 0, tzinfo=_IST)


class _FrozenDatetime(_dt.datetime):
    """datetime with a pinned now(); everything else (fromisoformat, strptime,
    arithmetic, isinstance) inherits real behaviour."""

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return _FROZEN_NOW_IST.replace(tzinfo=None)
        return _FROZEN_NOW_IST.astimezone(tz)

    @classmethod
    def utcnow(cls):
        return _FROZEN_NOW_IST.astimezone(_dt.timezone.utc).replace(tzinfo=None)


@pytest.fixture
def market_hours_clock(monkeypatch):
    """Pin the wall clock read by the time-based exit paths to 2026-06-25 10:00
    IST (a trading day, inside market hours, before the 15:12 MIS square-off and
    the 15:29 basket square-off).

    Use on any test that ticks a MIS session or a square_off_enabled
    intraday_basket and asserts on a NON-time-based outcome (kill sign, trail
    arm/stop, reconciliation). Without it the test is a function of the wall clock.
    """
    import autotrade.session as _sess
    from autotrade.monitoring import trail_engine as _te

    monkeypatch.setattr(_sess, "datetime", _FrozenDatetime)
    monkeypatch.setattr(_te, "datetime", _FrozenDatetime)
    # Keep the explicit seam in lockstep with the frozen wall clock.
    _sess.set_fake_now(_FROZEN_NOW_IST)
    yield _FROZEN_NOW_IST
    _sess.set_fake_now(None)


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
        # isn't stuck "already fired". Also clear the entry-claim (C5) so a reused
        # session_id isn't stuck "entry already claimed".
        with fire_guard._LOCK:
            fire_guard._FIRED.clear()
            fire_guard._ENTRY_CLAIMED.clear()
        # Clear the R4 basket reconcile-validation stamps so a reused session_id
        # doesn't carry a stale "validated" set across tests.
        from autotrade.monitoring import basket_gen
        with basket_gen._LOCK:
            basket_gen._LAST_RECONCILED.clear()

    _stop_all_drivers()
    with falcon_conn() as con:
        # ISOLATION GUARD (2026-07-09 incident): NEVER let this wipe run against a
        # real DB. If import-order ever lets falcon.config.FALCON_DB resolve to the
        # PROD path, the DELETEs below would destroy the live AutoTrade book (it
        # happened once). Check the connection's ACTUAL open file — not an env var —
        # and hard-fail before deleting anything if it is not our temp test DB.
        _dbs = con.execute("PRAGMA database_list").fetchall()
        _main = next((d[2] for d in _dbs if d[1] == "main"), "")
        if "kanida_autotrade_test_" not in str(_main):
            raise RuntimeError(
                "TEST ISOLATION FAILURE: clean_positions refuses to DELETE against a "
                "non-test DB (%s). Expected the temp DB %s. Aborting to protect the "
                "live AutoTrade book." % (_main, _TMP_DB))
        # autotrade_positions is the ONLY session-position store now. We also
        # clear falcon_position_state to PROVE (in the regression test) that the
        # autotrade path never writes into it.
        con.execute("DELETE FROM autotrade_positions")
        con.execute("DELETE FROM falcon_position_state")
        con.execute("DELETE FROM autotrade_sessions")
        con.execute("DELETE FROM autotrade_portfolio_snapshots")
        con.execute("DELETE FROM autotrade_kill_switch_log")
        con.execute("DELETE FROM autotrade_ladders")
        con.execute("DELETE FROM autotrade_recon_alerts")
        con.execute("DELETE FROM autotrade_alerts")
        # CLUSTER 9d FIX F2 — clear the per-account committed-capital ledger so a
        # reused session_id doesn't carry a stale allocation split across tests.
        con.execute("DELETE FROM autotrade_session_account_allocations")
        # ITEM 2 — clear the cross-process durable claims so a reused session_id
        # isn't stuck fired / entry-claimed / exit-in-flight across tests.
        con.execute("DELETE FROM autotrade_claims")
        con.commit()
    yield
    _stop_all_drivers()


def _assert_test_db(con):
    """Hard-fail unless the connection's ACTUAL open file is our temp test DB.

    ISOLATION GUARD (2026-07-09 incident #2): seed_signals wiped the LIVE
    falcon_signals_live (served A/B/C test picks to real users on the portal)
    when FALCON_DB import-order let falcon_conn() resolve to the prod DB. Every
    test helper that DELETE/INSERTs via falcon_conn MUST call this first — an env
    var check is not enough; assert on the real file (PRAGMA database_list)."""
    _dbs = con.execute("PRAGMA database_list").fetchall()
    _main = next((d[2] for d in _dbs if d[1] == "main"), "")
    if "kanida_autotrade_test_" not in str(_main):
        raise RuntimeError(
            "TEST ISOLATION FAILURE: a test helper would write against a non-test DB "
            "(%s). Expected the temp DB %s. Aborting to protect live data." % (_main, _TMP_DB))


def seed_signals(symbols_ranks):
    """Insert Falcon picks. symbols_ranks = [(symbol, rank, score, close), ...]."""
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        _assert_test_db(con)  # never wipe the live falcon_signals_live
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
