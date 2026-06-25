"""Tests for the bulk-delete + single-delete session endpoints.

Bulk delete must:
  * remove the targeted sessions + their autotrade_positions rows,
  * stop the targeted sessions' tick driver + entry scheduler,
  * leave OTHER sessions (and their positions) untouched,
  * leave falcon_position_state untouched,
  * return {deleted: n, ids: [...]}.

All paper / dry-run — patches broker.router.build_client to MockBrokers.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession
from autotrade.monitoring import entry_scheduler, tick_driver
from autotrade.api import autotrade_routes as routes
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


def _count(table, session_id):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        return con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE session_id=?",
            (session_id,)).fetchone()[0]


def _session_exists(session_id):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        return con.execute(
            "SELECT COUNT(*) FROM autotrade_sessions WHERE session_id=?",
            (session_id,)).fetchone()[0] == 1


# ── 1. bulk delete removes sessions + positions, leaves others untouched ──────

def test_bulk_delete_removes_targets_leaves_others(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False)
    s1 = TradingSession.create(cfg, mode="paper")
    asyncio.run(s1.start())
    s2 = TradingSession.create(cfg, mode="paper")
    asyncio.run(s2.start())
    keep = TradingSession.create(cfg, mode="paper")
    asyncio.run(keep.start())

    assert _count("autotrade_positions", s1.session_id) == 2
    assert _count("autotrade_positions", s2.session_id) == 2
    assert _count("autotrade_positions", keep.session_id) == 2

    res = routes.sessions_delete(
        routes.DeleteSessionsRequest(session_ids=[s1.session_id, s2.session_id]))
    assert res["deleted"] == 2
    assert set(res["ids"]) == {s1.session_id, s2.session_id}

    # Targets gone (session row + positions).
    assert not _session_exists(s1.session_id)
    assert not _session_exists(s2.session_id)
    assert _count("autotrade_positions", s1.session_id) == 0
    assert _count("autotrade_positions", s2.session_id) == 0

    # Untouched session intact.
    assert _session_exists(keep.session_id)
    assert _count("autotrade_positions", keep.session_id) == 2


# ── 2. delete stops the session's drivers ─────────────────────────────────────

def test_delete_stops_drivers(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    # SCHEDULED far-future session so an entry scheduler is armed.
    target = datetime.now(IST) + timedelta(seconds=30)
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_time=_ist_hhmmss(target))
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start(when="scheduled"))
    assert entry_scheduler.is_running(sess.session_id)

    res = routes.sessions_delete(
        routes.DeleteSessionsRequest(session_ids=[sess.session_id]))
    assert res["deleted"] == 1

    # Scheduler stopped; give the thread a moment to wind down.
    deadline = time.time() + 3.0
    while time.time() < deadline and entry_scheduler.is_running(sess.session_id):
        time.sleep(0.05)
    assert not entry_scheduler.is_running(sess.session_id)
    assert not _session_exists(sess.session_id)
    assert not tick_driver.is_running(sess.session_id)


# ── 3. single DELETE endpoint ─────────────────────────────────────────────────

def test_single_delete(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    assert _session_exists(sess.session_id)

    res = routes.session_delete(sess.session_id)
    assert res == {"deleted": 1, "ids": [sess.session_id]}
    assert not _session_exists(sess.session_id)

    # Deleting a missing id → 404.
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as ei:
        routes.session_delete("does-not-exist")
    assert ei.value.status_code == 404


# ── 4. bulk delete skips missing ids gracefully ───────────────────────────────

def test_bulk_delete_skips_missing(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())

    res = routes.sessions_delete(
        routes.DeleteSessionsRequest(session_ids=[sess.session_id, "nope-123"]))
    assert res["deleted"] == 1
    assert res["ids"] == [sess.session_id]


# ── 5. regression: delete leaves falcon_position_state untouched ──────────────

def test_delete_does_not_touch_falcon_position_state(clean_positions, patched_brokers):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO falcon_position_state
               (symbol, managed_by, product, qty, avg_entry, initial_sl_price,
                current_sl_price, target_price, high_water_price, entry_date)
               VALUES ('A','falcon','CNC', 999, 55.5, 50, 50, 70, 60, '2026-06-01')""")
        con.commit()

    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())

    routes.sessions_delete(
        routes.DeleteSessionsRequest(session_ids=[sess.session_id]))

    with falcon_conn() as con:
        fp = con.execute(
            "SELECT qty, avg_entry, managed_by FROM falcon_position_state "
            "WHERE symbol='A'").fetchone()
        total_fp = con.execute(
            "SELECT COUNT(*) FROM falcon_position_state").fetchone()[0]
    assert fp["qty"] == 999
    assert abs(fp["avg_entry"] - 55.5) < 1e-9
    assert fp["managed_by"] == "falcon"
    assert total_fp == 1
