"""Tests for the dual entry-firing modes: start(when="now") and
start(when="scheduled").

All paper / dry-run — patches broker.router.build_client to return MockBrokers
so no real Kite is ever touched. The scheduled tests use a near-future
entry_time (1-2s ahead) so they run fast, and assert the SCHEDULED → RUNNING
arming + fire, the PAST-time immediate fallback, cancel/kill of a SCHEDULED
session, and the falcon_position_state isolation regression for both paths.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession
from autotrade.monitoring import entry_scheduler, tick_driver
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


# ── 1. when="now" fires immediately ──────────────────────────────────────────

def test_start_now_fires_immediately(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 3
    st = sess.status()
    assert st["status"] == "RUNNING"
    assert st["n_open_positions"] == 3


def test_start_default_when_is_now(clean_positions, patched_brokers):
    """Backward-compat: start() with no arg == start(when='now')."""
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start())
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 1


# ── 2. when="scheduled" with a near-future entry_time arms then fires ─────────

def test_start_scheduled_future_arms_then_fires(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    target = datetime.now(IST) + timedelta(seconds=2)
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_time=_ist_hhmmss(target))
    sess = TradingSession.create(cfg, mode="paper")

    res = asyncio.run(sess.start(when="scheduled"))
    assert res["status"] == "SCHEDULED"
    assert res["n_placed"] == 0
    assert res["scheduler_armed"] is True
    assert res["seconds_remaining"] >= 0
    assert "fires_at" in res

    # status() reflects SCHEDULED with the entry time + countdown.
    st = sess.status()
    assert st["status"] == "SCHEDULED"
    assert st["entry_time"] == _ist_hhmmss(target)
    assert "fires_at" in st
    assert st["seconds_remaining"] >= 0
    assert st["n_open_positions"] == 0
    assert entry_scheduler.is_running(sess.session_id)

    # Wait for the scheduler thread to wake, flip RUNNING, AND finish placing
    # both positions. _fire_entries() sets RUNNING before the placement loop, so
    # we poll on the position count (the real completion signal), not status.
    deadline = time.time() + 8.0
    while time.time() < deadline:
        st2 = sess.status()
        if st2["status"] == "RUNNING" and st2["n_open_positions"] == 2:
            break
        time.sleep(0.1)
    st2 = sess.status()
    assert st2["status"] == "RUNNING", "scheduled session did not fire by entry_time"
    assert st2["n_open_positions"] == 2

    # Scheduler self-deregisters once the fire completes.
    deadline = time.time() + 3.0
    while time.time() < deadline and entry_scheduler.is_running(sess.session_id):
        time.sleep(0.1)
    assert not entry_scheduler.is_running(sess.session_id)


# ── 3. when="scheduled" with a PAST entry_time fires immediately + note ───────

def test_start_scheduled_past_fires_immediately(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    past = datetime.now(IST) - timedelta(minutes=5)
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_time=_ist_hhmmss(past))
    sess = TradingSession.create(cfg, mode="paper")

    res = asyncio.run(sess.start(when="scheduled"))
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 2
    assert "note" in res
    assert "already passed" in res["note"].lower()
    assert not entry_scheduler.is_running(sess.session_id)
    assert sess.status()["n_open_positions"] == 2


# ── 4. cancel/kill a SCHEDULED session stops it + places nothing ──────────────

def test_kill_scheduled_session_places_nothing(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    target = datetime.now(IST) + timedelta(seconds=30)  # far enough out
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_time=_ist_hhmmss(target))
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start(when="scheduled"))
    assert sess.status()["status"] == "SCHEDULED"
    assert entry_scheduler.is_running(sess.session_id)

    res = asyncio.run(sess.kill(reason="OPERATOR"))
    # Nothing was open, so nothing was exited.
    assert res["n_positions"] == 0

    st = sess.status()
    assert st["status"] == "CLOSED"
    assert st["n_open_positions"] == 0

    # Scheduler stopped; give the thread a moment to wind down.
    deadline = time.time() + 3.0
    while time.time() < deadline and entry_scheduler.is_running(sess.session_id):
        time.sleep(0.1)
    assert not entry_scheduler.is_running(sess.session_id)

    # Wait past where the original target would have been short-circuited and
    # confirm NO positions ever landed in autotrade_positions.
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        n = con.execute(
            "SELECT COUNT(*) FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchone()[0]
    assert n == 0


# ── 5. regression: scheduled fire leaves falcon_position_state untouched ──────

def test_scheduled_fire_does_not_touch_falcon_position_state(clean_positions,
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
    target = datetime.now(IST) + timedelta(seconds=2)
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_time=_ist_hhmmss(target))
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start(when="scheduled"))

    deadline = time.time() + 8.0
    while time.time() < deadline:
        if (_session_status_db(sess.session_id) == "RUNNING"
                and sess.status()["n_open_positions"] == 2):
            break
        time.sleep(0.1)
    assert _session_status_db(sess.session_id) == "RUNNING"

    # Session positions in autotrade_positions only.
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
    # Pre-existing Falcon swing row untouched.
    assert fp["qty"] == 999
    assert abs(fp["avg_entry"] - 55.5) < 1e-9
    assert fp["managed_by"] == "falcon"
    assert total_fp == 1
