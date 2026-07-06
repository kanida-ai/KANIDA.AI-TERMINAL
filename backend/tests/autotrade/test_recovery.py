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
from autotrade.session import TradingSession, set_fake_now
from autotrade.monitoring import entry_scheduler, tick_driver
from autotrade import recovery
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))

TRADING_DAY = "2026-06-25"          # Thursday NSE trading day
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    """Default recovery tests to mid-session on a trading day; per-test override
    via set_fake_now()."""
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


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
    # Future trading-day target (14:00 vs frozen 10:00) so resume re-arms, not fires.
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_date=TRADING_DAY, entry_time="14:00:00")
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


# ── 4. SCHEDULED past-due WITHIN GRACE + market open → fired now ──────────────

def test_resume_scheduled_pastdue_fires_now(clean_positions, patched_brokers):
    """A session whose target is just past (within grace) on the SAME trading
    day with the market open → recovery fires it (it didn't miss the window)."""
    set_fake_now(datetime(2026, 6, 25, 10, 0, 30, tzinfo=IST))  # 30s past target
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_date=TRADING_DAY, entry_time="10:00:00",
                               entry_grace_seconds=120)
    sess = TradingSession.create(cfg, mode="paper")
    # Persist as SCHEDULED to simulate a session that was scheduled before the
    # restart (recovery only scans RUNNING/SCHEDULED).
    sess._set_status("SCHEDULED")

    summary = recovery.resume_active_sessions()
    assert summary["scheduled"] == 1
    assert summary["fired"] == 1
    assert _session_status_db(sess.session_id) == "RUNNING"
    assert sess.status()["n_open_positions"] == 2


# ── 4b. SCHEDULED comes back up on a NON-TRADING DAY → NO fire ────────────────

def test_resume_on_non_trading_day_does_not_fire(clean_positions, patched_brokers):
    """Restart on a Sunday: a SCHEDULED session must NOT fire — recovery defers
    it to the next trading day's resolved target (entry_date unset → resolve)."""
    set_fake_now(datetime(2026, 6, 28, 11, 0, 0, tzinfo=IST))  # Sunday
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_time="09:15:00")  # entry_date unset
    sess = TradingSession.create(cfg, mode="paper")
    sess._set_status("SCHEDULED")

    summary = recovery.resume_active_sessions()
    assert summary["fired"] == 0
    assert _session_status_db(sess.session_id) == "SCHEDULED"
    assert sess.status()["n_open_positions"] == 0
    # Cleanup the re-armed scheduler thread.
    entry_scheduler.stop_for_session(sess.session_id)


# ── 5. regression: recovery leaves falcon_position_state untouched ────────────

def test_resume_does_not_touch_falcon_position_state(clean_positions,
                                                     patched_brokers):
    set_fake_now(datetime(2026, 6, 25, 10, 0, 30, tzinfo=IST))
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO falcon_position_state
               (symbol, managed_by, product, qty, avg_entry, initial_sl_price,
                current_sl_price, target_price, high_water_price, entry_date)
               VALUES ('A','falcon','CNC', 999, 55.5, 50, 50, 70, 60, '2026-06-01')""")
        con.commit()

    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    # A past-due (within grace) SCHEDULED session → recovery FIRES it.
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_date=TRADING_DAY, entry_time="10:00:00",
                               entry_grace_seconds=120)
    sess = TradingSession.create(cfg, mode="paper")
    sess._set_status("SCHEDULED")

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


# ── square-off re-arm delegates to the MIS-aware fire path (2026-07-06 regression) ─

def test_rearm_square_off_delegates_to_arm_square_off(clean_positions):
    """The resume path must re-arm the square-off using the SAME MIS-aware target
    selection as the fire path. Previously _rearm_square_off used only
    square_off_time, so a restart re-armed a MIS session at 15:29 instead of the
    15:12 defensive mis_square_off_time — Zerodha then force-squared at ~15:20 and
    our 15:29 order was rejected. It now delegates to _arm_square_off (single
    source of truth: min(square_off_time, mis_square_off_time for a MIS product))."""
    import pytest as _pytest
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=3,
                               strategy="intraday_basket", order_product="MIS",
                               instrument_type="EQ", square_off_time="15:29:00",
                               mis_square_off_time="15:12:00",
                               kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    calls = []
    from autotrade.session import TradingSession as TS
    mp = _pytest.MonkeyPatch()
    mp.setattr(TS, "_arm_square_off",
               lambda self: (calls.append(self.session_id), True)[1])
    try:
        recovery._rearm_square_off(sess.session_id)
    finally:
        mp.undo()
    assert calls == [sess.session_id]   # resume routed through the MIS-aware path
