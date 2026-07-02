"""FEATURE A — MIS product-aware defensive square-off (SAFETY).

A session whose EFFECTIVE product is MIS must be squared off BY US before the
broker's ~15:20 compulsory auto-square, on BOTH strategies. Covers:
  * config: is_intraday_product(), mis_square_off_time validation.
  * arming: MIS + kill_switch arms a square-off; MIS + intraday_basket arms at
    the EARLIER of (mis_square_off_time, square_off_time); a NON-MIS kill_switch
    session arms NOTHING.
  * tick backstop: fires MIS_SQUARE_OFF when the in-memory timer is missing.

All paper-safe (MockBroker, no real orders). To make the arming assertions
independent of the wall clock, the arming tests use LATE clock times (23:5x) so
the daemon thread just sleeps at its future target and never fires mid-test.
"""
import asyncio

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession
from autotrade.monitoring import square_off_scheduler
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker


# ── config: is_intraday_product() + validation ───────────────────────────────

def test_is_intraday_product_session_level():
    cfg = TradingSessionConfig(total_allocated_capital=1.0, order_product="MIS")
    assert cfg.is_intraday_product() is True
    for p in ("CNC", "MTF", "NRML"):
        cfg2 = TradingSessionConfig(total_allocated_capital=1.0, order_product=p)
        assert cfg2.is_intraday_product() is False


def test_is_intraday_product_broker_profile_level():
    # Session-level CNC but a broker profile trades MIS → still intraday.
    cfg = TradingSessionConfig(
        total_allocated_capital=1.0, order_product="CNC",
        broker_profiles=[BrokerProfile("z", "zerodha", order_product="MIS")])
    assert cfg.is_intraday_product() is True


def test_mis_square_off_time_must_be_parseable():
    cfg = TradingSessionConfig(total_allocated_capital=1.0,
                               mis_square_off_time="not-a-time")
    with pytest.raises(ValueError, match="mis_square_off_time"):
        cfg.validate()


def test_mis_square_off_must_be_before_basket_square_off():
    # intraday_basket: mis time must be strictly BEFORE square_off_time.
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5, mis_square_off_time="15:29:00",
        square_off_time="15:29:00")
    with pytest.raises(ValueError, match="strictly before"):
        cfg.validate()


# ── arming ────────────────────────────────────────────────────────────────────

@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 50.0, "D": 150.0, "E": 300.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


@pytest.fixture
def squareoff_autostart_on():
    square_off_scheduler.set_autostart(True)
    yield
    square_off_scheduler.set_autostart(False)


def test_mis_kill_switch_session_arms_square_off(clean_positions, patched_brokers,
                                                 squareoff_autostart_on):
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, top_n_stocks=1, sizing_mode="equal",
        strategy="portfolio_kill_switch", order_product="MIS",
        kill_switch_enabled=False, per_position_gtt_enabled=False,
        mis_square_off_time="23:58:00")   # future → daemon sleeps, no fire mid-test
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # A MIS kill-switch session MUST have a square-off armed at the MIS time.
    assert square_off_scheduler.is_running(sess.session_id) is True
    target = square_off_scheduler.target_for_session(sess.session_id)
    assert (target.hour, target.minute) == (23, 58)


def test_mis_intraday_basket_arms_at_earlier_time(clean_positions, patched_brokers,
                                                  squareoff_autostart_on):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    # intraday_basket square_off 23:59, MIS defensive 23:58 → earlier (23:58) wins.
    cfg = TradingSessionConfig(
        total_allocated_capital=300000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="MIS",
        per_position_gtt_enabled=False,
        square_off_time="23:59:00", mis_square_off_time="23:58:00",
        arm_pct=0.01, floor_pct=0.01)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    assert square_off_scheduler.is_running(sess.session_id) is True
    target = square_off_scheduler.target_for_session(sess.session_id)
    assert (target.hour, target.minute) == (23, 58)   # EARLIER MIS time wins


def test_non_mis_kill_switch_arms_no_square_off(clean_positions, patched_brokers,
                                                squareoff_autostart_on):
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, top_n_stocks=1, sizing_mode="equal",
        strategy="portfolio_kill_switch", order_product="CNC",
        kill_switch_enabled=False, per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # A CNC kill-switch session must arm NO square-off (unchanged behaviour).
    assert square_off_scheduler.is_running(sess.session_id) is False


# ── tick backstop ───────────────────────────────────────────────────────────

def test_tick_backstop_fires_mis_square_off_when_timer_missing(
        clean_positions, patched_brokers):
    # square-off scheduler autostart is OFF (default in tests) → no timer armed.
    # A MIS kill-switch session past its mis_square_off_time must be flattened by
    # the in-tick backstop. mis time 00:00:01 is always in the past today.
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, top_n_stocks=1, sizing_mode="equal",
        strategy="portfolio_kill_switch", order_product="MIS",
        kill_switch_enabled=False, per_position_gtt_enabled=False,
        mis_square_off_time="00:00:01")
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    assert sess.status()["n_open_positions"] == 1
    assert square_off_scheduler.is_running(sess.session_id) is False  # no timer
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert "MIS_SQUARE_OFF" in (out["kill_reason"] or "")
    assert sess.status()["n_open_positions"] == 0
    # close_reason must be tagged MIS_SQUARE_OFF on the flattened positions.
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT close_reason FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchall()
    assert all(r["close_reason"] == "MIS_SQUARE_OFF" for r in rows)


def test_tick_backstop_does_not_fire_for_cnc(clean_positions, patched_brokers):
    # A CNC session must NEVER be squared off by the MIS backstop (no-fire case).
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, top_n_stocks=1, sizing_mode="equal",
        strategy="portfolio_kill_switch", order_product="CNC",
        kill_switch_enabled=False, per_position_gtt_enabled=False,
        mis_square_off_time="00:00:01")   # past — but must be inert for CNC
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is False
    assert sess.status()["n_open_positions"] == 1
