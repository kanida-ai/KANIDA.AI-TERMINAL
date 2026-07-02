"""POSITIONAL vs INTRADAY trailing protection (strategy=="intraday_basket").

The intraday_basket trail engine (arm / peak-ratchet / floor / giveback /
hard-stop on the invested-notional return) becomes operator-selectable between:

  * INTRADAY  (square_off_enabled=True, DEFAULT)  → force a time-based square-off
    at square_off_time. Today's behaviour, byte-for-byte.
  * POSITIONAL (square_off_enabled=False)         → NO forced square-off; the
    trailing floor + peak ratchet + downside hard stop + giveback exit persist
    across days. Never an unprotected overnight position.

Covers:
  * trail_engine.decide(): positional does NOT emit SQUARE_OFF even PAST the time,
    but STILL emits STOP / TRAIL_EXIT / FLOOR_EXIT; intraday still squares off.
  * config: positional rejected for MIS; positional allowed for CNC / MTF.
  * params_from_config threads the flag.
  * session: positional intraday_basket arms NO square-off scheduler (CNC),
    intraday DOES; peak/floor persist + resume across a simulated restart; a
    NON-intraday_basket (kill switch) session ignores the flag.

All paper-safe (MockBroker, no real orders).
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession
from autotrade.monitoring import square_off_scheduler
from autotrade.monitoring import trail_engine
from autotrade.monitoring.trail_engine import TrailParams, TrailState, decide
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))

# A moment that is unambiguously PAST any intraday square_off_time TODAY: use
# 23:59 as the square_off_time and evaluate at exactly that second so the
# intraday branch fires but a positional one must not.
_PAST_SQ = "00:00:01"           # 00:00:01 IST — always in the past by 10:00 test now
_NOW_PAST = datetime.now(IST).replace(hour=12, minute=0, second=0, microsecond=0)


# ── trail_engine.decide(): SQUARE_OFF skip when positional ────────────────────

def test_positional_does_not_square_off_past_time():
    # Flat return, past square-off time, positional → HOLD (no SQUARE_OFF).
    params = TrailParams(square_off_time=_PAST_SQ, square_off_enabled=False)
    d = decide(0.0, TrailState(), params, now=_NOW_PAST)
    assert d.action == "HOLD"
    assert d.reason is None


def test_intraday_squares_off_past_time_unchanged():
    # Same inputs but intraday (default) → EXIT SQUARE_OFF (today's behaviour).
    params = TrailParams(square_off_time=_PAST_SQ, square_off_enabled=True)
    d = decide(0.0, TrailState(), params, now=_NOW_PAST)
    assert d.action == "EXIT"
    assert d.reason == "SQUARE_OFF"


def test_positional_still_hard_stops():
    # Positional must STILL fire the downside hard stop even past square-off time.
    params = TrailParams(stop_pct=0.015, square_off_time=_PAST_SQ,
                         square_off_enabled=False)
    d = decide(-0.02, TrailState(), params, now=_NOW_PAST)
    assert d.action == "EXIT"
    assert d.reason == "STOP"


def test_positional_still_trail_exits_on_giveback():
    # Armed at peak +2%, giveback 0.75% → trigger 1.25%. G drops to 1.0% → exit.
    params = TrailParams(arm_pct=0.01, floor_pct=0.01, trail_giveback_pct=0.0075,
                         square_off_time=_PAST_SQ, square_off_enabled=False)
    armed = TrailState(armed=True, peak=0.02)
    d = decide(0.010, armed, params, now=_NOW_PAST)
    assert d.action == "EXIT"
    assert d.reason == "TRAIL_EXIT"


def test_positional_still_floor_exits():
    # Peak just above floor so the floor clamps the trigger → FLOOR_EXIT.
    params = TrailParams(arm_pct=0.01, floor_pct=0.01, trail_giveback_pct=0.02,
                         square_off_time=_PAST_SQ, square_off_enabled=False)
    armed = TrailState(armed=True, peak=0.012)
    d = decide(0.009, armed, params, now=_NOW_PAST)
    assert d.action == "EXIT"
    assert d.reason == "FLOOR_EXIT"


def test_positional_still_arms():
    # Positional still ARMS the ratchet when G first reaches +arm_pct.
    params = TrailParams(arm_pct=0.01, square_off_time=_PAST_SQ,
                         square_off_enabled=False)
    d = decide(0.015, TrailState(), params, now=_NOW_PAST)
    assert d.action == "ARM"
    assert d.state.armed is True
    assert d.state.peak == pytest.approx(0.015)


def test_positional_peak_ratchets_and_holds():
    # Below the trigger — positional ratchets the peak up and HOLDS (no exit).
    params = TrailParams(arm_pct=0.01, floor_pct=0.01, trail_giveback_pct=0.0075,
                         square_off_time=_PAST_SQ, square_off_enabled=False)
    armed = TrailState(armed=True, peak=0.02)
    d = decide(0.030, armed, params, now=_NOW_PAST)
    assert d.action == "HOLD"
    assert d.state.peak == pytest.approx(0.030)
    assert d.state_changed is True


# ── config validation ────────────────────────────────────────────────────────

def test_config_rejects_positional_mis_session_level():
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5, order_product="MIS", square_off_enabled=False)
    with pytest.raises(ValueError, match="positional .*not allowed for MIS"):
        cfg.validate()


def test_config_rejects_positional_mis_broker_profile():
    # Session-level CNC but a broker leg trades MIS → still rejected positional.
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5, order_product="CNC", square_off_enabled=False,
        broker_profiles=[BrokerProfile("z", "zerodha", order_product="MIS")])
    with pytest.raises(ValueError, match="positional .*not allowed for MIS"):
        cfg.validate()


def test_config_allows_positional_cnc():
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5, order_product="CNC", square_off_enabled=False)
    cfg.validate()   # must not raise


def test_config_allows_positional_mtf():
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5, order_product="MTF", instrument_type="MTF",
        square_off_enabled=False)
    cfg.validate()   # must not raise


def test_config_default_is_intraday_and_roundtrips():
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5)
    assert cfg.square_off_enabled is True   # default = today's behaviour
    # A dict WITHOUT the key loads as intraday (every existing session unchanged).
    d = cfg.to_dict()
    del d["square_off_enabled"]
    assert TradingSessionConfig.from_dict(d).square_off_enabled is True
    # Explicit False round-trips.
    d2 = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5, order_product="CNC",
        square_off_enabled=False).to_dict()
    assert TradingSessionConfig.from_dict(d2).square_off_enabled is False


def test_params_from_config_threads_flag():
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5, order_product="CNC", square_off_enabled=False)
    p = trail_engine.params_from_config(cfg)
    assert p.square_off_enabled is False


# ── session-level arming + recovery ───────────────────────────────────────────

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


def _basket_signals():
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])


def test_positional_cnc_arms_no_square_off(clean_positions, patched_brokers,
                                           squareoff_autostart_on):
    _basket_signals()
    cfg = TradingSessionConfig(
        total_allocated_capital=300000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False, square_off_enabled=False,
        arm_pct=0.01, floor_pct=0.01,
        square_off_time="23:59:00")   # would be armed if intraday
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # POSITIONAL CNC: NO square-off scheduler armed (the trail carries across days).
    assert square_off_scheduler.is_running(sess.session_id) is False
    st = sess.status()
    assert st["trail"]["square_off_enabled"] is False
    assert st["trail"]["trail_mode"] == "positional"


def test_intraday_cnc_arms_square_off(clean_positions, patched_brokers,
                                      squareoff_autostart_on):
    _basket_signals()
    cfg = TradingSessionConfig(
        total_allocated_capital=300000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False, square_off_enabled=True,
        arm_pct=0.01, floor_pct=0.01,
        square_off_time="23:59:00")   # future → daemon sleeps, no fire mid-test
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # INTRADAY (default): the basket square-off scheduler IS armed (unchanged).
    assert square_off_scheduler.is_running(sess.session_id) is True
    target = square_off_scheduler.target_for_session(sess.session_id)
    assert (target.hour, target.minute) == (23, 59)
    st = sess.status()
    assert st["trail"]["square_off_enabled"] is True
    assert st["trail"]["trail_mode"] == "intraday"


def test_positional_peak_persists_and_resumes(clean_positions, patched_brokers):
    """Peak/armed persist on the session row and a simulated restart (reload +
    recovery re-arm) keeps trailing WITHOUT arming any square-off."""
    _basket_signals()
    cfg = TradingSessionConfig(
        total_allocated_capital=300000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False, square_off_enabled=False,
        arm_pct=0.01, floor_pct=0.01, square_off_time="23:59:00")
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    session_id = sess.session_id

    # Simulate a mid-day armed trail with a ratcheted peak.
    sess.monitor.save_trail_state(TrailState(armed=True, peak=0.0234))

    # "Restart": load a fresh session object + run the recovery resume path.
    from autotrade import recovery
    reloaded = TradingSession.load(session_id)
    assert reloaded is not None
    state = reloaded.monitor.load_trail_state()
    assert state.armed is True
    assert state.peak == pytest.approx(0.0234)   # peak persisted

    # Recovery resume re-arms the tick/ws drivers (keeps trailing) but NOT a
    # square-off (positional → no overnight flatten).
    recovery._resume_running(session_id)
    assert square_off_scheduler.is_running(session_id) is False


def test_kill_switch_session_ignores_flag(clean_positions, patched_brokers,
                                          squareoff_autostart_on):
    # A NON-intraday_basket (portfolio_kill_switch) session ignores the flag:
    # square_off_enabled=False on a CNC kill-switch session arms nothing and the
    # flag has no effect (validate() accepts it; is_intraday_product False).
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, top_n_stocks=1, sizing_mode="equal",
        strategy="portfolio_kill_switch", order_product="CNC",
        kill_switch_enabled=False, per_position_gtt_enabled=False,
        square_off_enabled=False)
    cfg.validate()   # accepted — flag inert for kill switch
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # Kill-switch CNC arms no square-off regardless of the flag (unchanged).
    assert square_off_scheduler.is_running(sess.session_id) is False
