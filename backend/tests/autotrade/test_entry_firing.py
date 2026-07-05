"""Tests for the entry-firing modes under the EXECUTION-DATE / TRADING-DAY rule:
start(when="now") and start(when="scheduled").

All paper / dry-run — patches broker.router.build_client to return MockBrokers so
no real Kite is ever touched. "now" is FROZEN per-test via set_fake_now() so the
trading-day / market-open gate is deterministic (no wall-clock dependence). The
scheduled-future tests use a near-future entry_time so the scheduler thread fires
fast; the gate-evaluation 'now' is the frozen clock, the SLEEP target is the real
wall-clock entry_time (the scheduler thread sleeps in real time).

Trading-day baseline: 2026-06-25 is a Thursday NSE trading day; 09:15–15:30 IST
is the open window.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from autotrade.monitoring import entry_scheduler, tick_driver
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))

# A known NSE trading day, mid-session.
TRADING_DAY = "2026-06-25"          # Thursday
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    """Default every test in this module to mid-session on a trading day. Tests
    that need a different 'now' call set_fake_now() themselves."""
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


# ── 1. when="now" fires immediately ON A TRADING DAY DURING MARKET HOURS ───────

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


# ── 2. when="now" with the MARKET CLOSED is REFUSED — places NOTHING ───────────

def test_start_now_market_closed_refused(clean_positions, patched_brokers):
    """Instant start after the bell (still a trading day) must NOT fire."""
    set_fake_now(datetime(2026, 6, 25, 16, 0, 0, tzinfo=IST))  # after 15:30
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "EXPIRED_MISSED_WINDOW"
    assert res["n_placed"] == 0
    assert sess.status()["n_open_positions"] == 0


def test_start_now_sunday_refused(clean_positions, patched_brokers):
    """Instant start on a Sunday must NOT fire (no order)."""
    set_fake_now(datetime(2026, 6, 28, 11, 0, 0, tzinfo=IST))  # Sunday
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "REJECTED_NON_TRADING_DAY"
    assert res["n_placed"] == 0
    assert sess.status()["n_open_positions"] == 0


# ── 3. when="scheduled" with a near-future entry_time arms then fires ──────────

def test_start_scheduled_future_arms_then_fires(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    # Freeze 'now' to 10:00 on the trading day; arm the scheduler for 10:00:02 so
    # it sleeps ~2 REAL seconds (target − frozen now), wakes, and fires through
    # the gate (still frozen at 10:00 — market open). Deterministic.
    frozen = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)
    set_fake_now(frozen)
    target_clock = frozen + timedelta(seconds=2)
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_date=TRADING_DAY,
                               entry_time=_ist_hhmmss(target_clock))
    sess = TradingSession.create(cfg, mode="paper")

    res = asyncio.run(sess.start(when="scheduled"))
    assert res["status"] == "SCHEDULED"
    assert res["n_placed"] == 0
    assert res["scheduler_armed"] is True
    assert res["seconds_remaining"] >= 0
    assert "fires_at" in res

    st = sess.status()
    assert st["status"] == "SCHEDULED"
    assert st["entry_date"] == TRADING_DAY
    assert st["is_trading_day"] is True
    assert "fires_at" in st
    assert entry_scheduler.is_running(sess.session_id)

    # The scheduler thread wakes at the wall-clock entry_time and fires through
    # the gate. Its gate 'now' is the frozen fake-now (still 10:00, market open).
    deadline = time.time() + 8.0
    while time.time() < deadline:
        st2 = sess.status()
        if st2["status"] == "RUNNING" and st2["n_open_positions"] == 2:
            break
        time.sleep(0.1)
    st2 = sess.status()
    assert st2["status"] == "RUNNING", "scheduled session did not fire by entry_time"
    assert st2["n_open_positions"] == 2

    deadline = time.time() + 3.0
    while time.time() < deadline and entry_scheduler.is_running(sess.session_id):
        time.sleep(0.1)
    assert not entry_scheduler.is_running(sess.session_id)


# ── 4. when="scheduled" with a PAST target BEYOND GRACE → EXPIRED, no order ────

def test_start_scheduled_past_beyond_grace_expires(clean_positions, patched_brokers):
    """The dangerous 'fire immediately if past' is REPLACED: a target long past
    (beyond entry_grace_seconds) on a trading day must EXPIRE, not fire."""
    set_fake_now(OPEN_NOW)  # 10:00
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_date=TRADING_DAY,
                               entry_time="09:15:00",  # 45min before now → missed
                               on_missed_window="expire",
                               entry_grace_seconds=120)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="scheduled"))
    assert res["status"] == "EXPIRED_MISSED_WINDOW"
    assert res["n_placed"] == 0
    assert not entry_scheduler.is_running(sess.session_id)
    assert sess.status()["n_open_positions"] == 0


def test_start_scheduled_past_within_grace_and_open_fires(clean_positions,
                                                          patched_brokers):
    """A target just past (within grace), still the same trading day + market
    open → FIRES."""
    set_fake_now(datetime(2026, 6, 25, 10, 0, 30, tzinfo=IST))  # 30s past 10:00
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_date=TRADING_DAY,
                               entry_time="10:00:00",  # 30s ago, within 120s grace
                               on_missed_window="expire",
                               entry_grace_seconds=120)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="scheduled"))
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 2
    assert sess.status()["n_open_positions"] == 2


# ── 5. carry_next_trading_day rolls a missed window to the next trading day ────

def test_start_scheduled_missed_carries_to_next_day(clean_positions,
                                                    patched_brokers):
    set_fake_now(OPEN_NOW)  # 10:00, target 09:15 already missed
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_date=TRADING_DAY,
                               entry_time="09:15:00",
                               on_missed_window="carry_next_trading_day")
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="scheduled"))
    assert res["status"] == "SCHEDULED"
    assert res["n_placed"] == 0
    # 2026-06-25 Thu → next trading day is Mon 2026-06-29
    # (Fri 06-26 is the Muharram holiday, 06-27/28 weekend).
    assert res["entry_date"] == "2026-06-29"


# ── 6. cancel/kill a SCHEDULED session stops it + places nothing ──────────────

def test_kill_scheduled_session_places_nothing(clean_positions, patched_brokers):
    set_fake_now(OPEN_NOW)
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_date=TRADING_DAY,
                               entry_time="14:00:00")  # future vs 10:00
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start(when="scheduled"))
    assert sess.status()["status"] == "SCHEDULED"
    assert entry_scheduler.is_running(sess.session_id)

    res = asyncio.run(sess.kill(reason="OPERATOR"))
    assert res["n_positions"] == 0

    st = sess.status()
    assert st["status"] == "CLOSED"
    assert st["n_open_positions"] == 0

    deadline = time.time() + 3.0
    while time.time() < deadline and entry_scheduler.is_running(sess.session_id):
        time.sleep(0.1)
    assert not entry_scheduler.is_running(sess.session_id)

    from falcon.db import falcon_conn
    with falcon_conn() as con:
        n = con.execute(
            "SELECT COUNT(*) FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchone()[0]
    assert n == 0


# ── 7. regression: scheduled fire leaves falcon_position_state untouched ──────

def test_scheduled_fire_does_not_touch_falcon_position_state(clean_positions,
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
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               entry_date=TRADING_DAY,
                               entry_time="10:00:00",  # within grace of 10:00:30
                               entry_grace_seconds=120)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="scheduled"))
    assert res["status"] == "RUNNING"

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
