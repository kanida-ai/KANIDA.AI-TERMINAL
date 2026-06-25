"""Tests for boot-time recovery (autotrade/recovery.py).

resume_active_sessions() must:
  * re-arm the tick driver for a RUNNING session,
  * re-arm the entry scheduler for a SCHEDULED session whose entry_time is still
    in the future,
  * FIRE a SCHEDULED session whose entry_time already passed while down,
  * be idempotent + safe when there are no active sessions,
  * leave falcon_position_state untouched.

All paper / dry-run — patches broker.router.build_client to return MockBrokers
so no real Kite is touched.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession
from autotrade.monitoring import entry_scheduler, tick_driver
from autotrade import recovery
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))


@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 50.0, "D": 150.0, "E": 300.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _ist_hhmmss(dt: datetime) -> str:
    return dt.strftime("%H:%M:%S")


def _session_status_db(session_id):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status FROM autotrade_sessions WHERE session_id=?",
            (session_id,)).fetchone()
    return row["status"] if row else None


# ── 1. no active sessions → safe no-op ────────────────────────────────────────

def test_resume_no_sessions_is_safe(clean_positions):
    summary = recovery.resume_active_sessions()
    assert summary["running"] == 0
    assert summary["scheduled"] == 0
    assert summary["fired"] == 0
    assert summary["errors"] == 0
    assert summary["sessions"] == []


# ── 2. RUNNING session → tick driver re-armed ─────────────────────────────────

def test_resume_running_rearms_tick_driver(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())  # status RUNNING; autostart disabled in tests
    assert _session_status_db(sess.session_id) == "RUNNING"
    # Simulate restart: no tick driver is running for this session.
    assert not tick_driver.is_running(sess.session_id)

    # Enable autostart (as production has it) so resume can actually arm.
    tick_driver.set_autostart(True)
    import os
    os.environ["FALCON_AUTOTRADE_TICK_INTERVAL"] = "0.5"
    try:
        summary = recovery.resume_active_sessions()
        assert summary["running"] == 1
        assert summary["rearmed"] >= 1
        # Driver is now live again → LTP/gross_return keep refreshing.
        assert tick_driver.is_running(sess.session_id)
    finally:
        tick_driver.set_autostart(False)
        tick_driver.stop_for_session(sess.session_id)
        os.environ.pop("FALCON_AUTOTRADE_TICK_INTERVAL", None)


# ── 3. SCHEDULED future → entry scheduler re-armed ────────────────────────────

def test_resume_scheduled_future_rearms_scheduler(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    target = datetime.now(IST) + timedelta(seconds=30)  # far enough out
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_time=_ist_hhmmss(target))
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start(when="scheduled"))
    assert _session_status_db(sess.session_id) == "SCHEDULED"
    # Simulate restart: stop the in-memory scheduler thread.
    entry_scheduler.stop_for_session(sess.session_id)
    deadline = time.time() + 3.0
    while time.time() < deadline and entry_scheduler.is_running(sess.session_id):
        time.sleep(0.05)
    assert not entry_scheduler.is_running(sess.session_id)

    summary = recovery.resume_active_sessions()
    assert summary["scheduled"] == 1
    assert summary["rearmed"] >= 1
    assert summary["fired"] == 0
    # Scheduler re-armed; nothing placed yet.
    assert entry_scheduler.is_running(sess.session_id)
    assert sess.status()["n_open_positions"] == 0

    # Cleanup: cancel so the 30s thread doesn't outlive the test.
    entry_scheduler.stop_for_session(sess.session_id)


# ── 4. SCHEDULED past-due → fired now ─────────────────────────────────────────

def test_resume_scheduled_pastdue_fires_now(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    # Build a SCHEDULED session with a FUTURE time so start() arms it cleanly,
    # then rewrite config_json to a PAST time to simulate "entry_time passed
    # while the backend was down".
    target = datetime.now(IST) + timedelta(seconds=30)
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_time=_ist_hhmmss(target))
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start(when="scheduled"))
    assert _session_status_db(sess.session_id) == "SCHEDULED"

    # Simulate restart: stop scheduler thread, then set entry_time to the past.
    entry_scheduler.stop_for_session(sess.session_id)
    past = datetime.now(IST) - timedelta(minutes=5)
    cfg.entry_time = _ist_hhmmss(past)
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET config_json=? WHERE session_id=?",
                    (cfg.to_json(), sess.session_id))
        con.commit()

    summary = recovery.resume_active_sessions()
    assert summary["scheduled"] == 1
    assert summary["fired"] == 1
    # Fired now → RUNNING with both positions placed.
    assert _session_status_db(sess.session_id) == "RUNNING"
    assert sess.status()["n_open_positions"] == 2


# ── 5. regression: recovery leaves falcon_position_state untouched ────────────

def test_resume_does_not_touch_falcon_position_state(clean_positions,
                                                     patched_brokers):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO falcon_position_state
               (symbol, managed_by, product, qty, avg_entry, initial_sl_price,
                current_sl_price, target_price, high_water_price, entry_date)
               VALUES ('A','falcon','CNC', 999, 55.5, 50, 50, 70, 60, '2026-06-01')""")
        con.commit()

    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    # A past-due SCHEDULED session → recovery FIRES it (exercises the write path).
    target = datetime.now(IST) + timedelta(seconds=30)
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_time=_ist_hhmmss(target))
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start(when="scheduled"))
    entry_scheduler.stop_for_session(sess.session_id)
    past = datetime.now(IST) - timedelta(minutes=5)
    cfg.entry_time = _ist_hhmmss(past)
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET config_json=? WHERE session_id=?",
                    (cfg.to_json(), sess.session_id))
        con.commit()

    recovery.resume_active_sessions()

    with falcon_conn() as con:
        at = con.execute(
            "SELECT symbol FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchall()
        fp = con.execute(
            "SELECT qty, avg_entry, managed_by FROM falcon_position_state "
            "WHERE symbol='A'").fetchone()
        total_fp = con.execute(
            "SELECT COUNT(*) FROM falcon_position_state").fetchone()[0]
    assert len(at) == 2
    assert fp["qty"] == 999
    assert abs(fp["avg_entry"] - 55.5) < 1e-9
    assert fp["managed_by"] == "falcon"
    assert total_fp == 1
