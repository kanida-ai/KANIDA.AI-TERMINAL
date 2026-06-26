"""Intraday-basket TRAILING-PROFIT engine + square-off — pure-decision tests
plus session.tick() integration (paper, MockBroker).

Covers the spec's required cases:
  * arm transition (G crosses arm_pct → armed, peak set, no exit)
  * TRAIL_EXIT (peak high, G gives back > giveback → exit)
  * FLOOR_EXIT (armed, peak just above arm, G drops to floor → exit AT floor)
  * STOP (pre-arm G <= -stop_pct → exit)
  * SQUARE_OFF (now >= square_off → exit regardless of P&L)
  * RESTART (armed+peak persisted, fresh load resumes them and trails correctly)
  * no-premature-exit sanity
  * config validation of the new knobs
  * regression: portfolio_kill_switch path untouched

All paper / dry-run — no real orders.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession
from autotrade.monitoring import trail_engine
from autotrade.monitoring.trail_engine import (
    TrailState, TrailParams, decide, compute_trigger, EXIT_REASONS)
from autotrade.monitoring.monitor import PortfolioMonitor
from autotrade.monitoring.registry import PositionRegistry
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))


# A far-future square-off so the time gate never interferes with P&L-logic tests.
_FAR = "23:59:59"


def _params(**kw):
    base = dict(arm_pct=0.01, floor_pct=0.01, trail_giveback_pct=0.0075,
                stop_pct=0.015, square_off_time=_FAR)
    base.update(kw)
    return TrailParams(**base)


# ════════════════════════════════════════════════════════════════════════════
# PURE ENGINE TESTS (no DB, no broker)
# ════════════════════════════════════════════════════════════════════════════

def test_pre_arm_hold_no_exit():
    """Below arm_pct, above -stop_pct → HOLD, not armed."""
    d = decide(0.005, TrailState(), _params())
    assert d.action == "HOLD"
    assert d.reason is None
    assert d.state.armed is False
    assert d.trigger is None


def test_arm_transition():
    """G crosses arm_pct → ARM, peak=G, no exit."""
    d = decide(0.012, TrailState(), _params(arm_pct=0.01))
    assert d.action == "ARM"
    assert d.reason is None
    assert d.state.armed is True
    assert abs(d.state.peak - 0.012) < 1e-12
    assert d.state_changed is True
    # trigger = max(peak-giveback, floor) = max(0.012-0.0075, 0.01) = 0.01 (floor)
    assert abs(d.trigger - 0.01) < 1e-12


def test_arm_exact_at_threshold():
    """G == arm_pct exactly → arms (>=)."""
    d = decide(0.01, TrailState(), _params(arm_pct=0.01))
    assert d.action == "ARM"
    assert d.state.armed is True


def test_armed_peak_ratchets_up_no_exit():
    """Armed, G makes a new high above the trigger → HOLD, peak ratchets."""
    st = TrailState(armed=True, peak=0.02)
    d = decide(0.03, st, _params())   # new high, well above trigger
    assert d.action == "HOLD"
    assert abs(d.state.peak - 0.03) < 1e-12
    assert d.state_changed is True
    # trigger = max(0.03-0.0075, 0.01) = 0.0225
    assert abs(d.trigger - 0.0225) < 1e-12


def test_trail_exit_giveback_from_peak():
    """Peak high; G gives back more than giveback → TRAIL_EXIT (giveback binds)."""
    # peak 0.05; giveback level = 0.0425; floor 0.01 → trigger = 0.0425.
    st = TrailState(armed=True, peak=0.05)
    d = decide(0.04, st, _params(floor_pct=0.01, trail_giveback_pct=0.0075))
    assert d.action == "EXIT"
    assert d.reason == "TRAIL_EXIT"
    assert abs(d.trigger - 0.0425) < 1e-12


def test_floor_exit_at_floor_not_below():
    """Peak just above arm so giveback level < floor → floor clamps → FLOOR_EXIT,
    and the trigger is the floor (exit AT floor, not below)."""
    # peak 0.012; giveback level = 0.0045; floor 0.01 → trigger = max = 0.01.
    st = TrailState(armed=True, peak=0.012)
    p = _params(arm_pct=0.01, floor_pct=0.01, trail_giveback_pct=0.0075)
    # G drops to the floor exactly → exit at floor.
    d = decide(0.01, st, p)
    assert d.action == "EXIT"
    assert d.reason == "FLOOR_EXIT"
    assert abs(d.trigger - 0.01) < 1e-12   # exits AT floor, never below


def test_floor_exit_below_floor():
    """Same shape, G dips below floor → still FLOOR_EXIT at the floor trigger."""
    st = TrailState(armed=True, peak=0.012)
    d = decide(0.008, st, _params(arm_pct=0.01, floor_pct=0.01))
    assert d.action == "EXIT"
    assert d.reason == "FLOOR_EXIT"


def test_stop_pre_arm():
    """Pre-arm G <= -stop_pct → STOP."""
    d = decide(-0.02, TrailState(), _params(stop_pct=0.015))
    assert d.action == "EXIT"
    assert d.reason == "STOP"


def test_stop_exact_threshold():
    d = decide(-0.015, TrailState(), _params(stop_pct=0.015))
    assert d.action == "EXIT"
    assert d.reason == "STOP"


def test_no_premature_exit_just_below_arm_and_above_stop():
    """A basket bouncing in (-stop, arm) must never exit."""
    for g in (-0.014, -0.005, 0.0, 0.005, 0.0099):
        d = decide(g, TrailState(), _params(arm_pct=0.01, stop_pct=0.015))
        assert d.action == "HOLD", f"G={g} should HOLD, got {d.action}"


def test_square_off_overrides_pnl():
    """now >= square_off_time → SQUARE_OFF regardless of P&L (even a winner)."""
    now = datetime.now(IST)
    sq = (now - timedelta(minutes=1)).strftime("%H:%M:%S")
    d = decide(0.005, TrailState(), _params(square_off_time=sq), now=now)
    assert d.action == "EXIT"
    assert d.reason == "SQUARE_OFF"


def test_square_off_before_time_does_not_fire():
    now = datetime.now(IST)
    sq = (now + timedelta(minutes=5)).strftime("%H:%M:%S")
    d = decide(0.005, TrailState(), _params(square_off_time=sq), now=now)
    assert d.action == "HOLD"


def test_compute_trigger_unarmed_is_none():
    assert compute_trigger(TrailState(armed=False, peak=0.0), _params()) is None


def test_exit_reason_set():
    assert EXIT_REASONS == {"SQUARE_OFF", "STOP", "TRAIL_EXIT", "FLOOR_EXIT"}


# ════════════════════════════════════════════════════════════════════════════
# CONFIG VALIDATION (the new knobs)
# ════════════════════════════════════════════════════════════════════════════

def test_config_intraday_defaults_valid():
    cfg = TradingSessionConfig(total_allocated_capital=1e6,
                               strategy="intraday_basket", top_n_stocks=5)
    cfg.validate()  # must not raise


def test_config_floor_gt_arm_rejected():
    cfg = TradingSessionConfig(total_allocated_capital=1e6,
                               strategy="intraday_basket", top_n_stocks=5,
                               arm_pct=0.01, floor_pct=0.02)
    with pytest.raises(ValueError):
        cfg.validate()


def test_config_squareoff_before_entry_rejected():
    cfg = TradingSessionConfig(total_allocated_capital=1e6,
                               strategy="intraday_basket", top_n_stocks=5,
                               entry_time="09:15:00", square_off_time="09:00:00")
    with pytest.raises(ValueError):
        cfg.validate()


def test_config_fraction_out_of_range_rejected():
    cfg = TradingSessionConfig(total_allocated_capital=1e6,
                               strategy="intraday_basket", top_n_stocks=5,
                               stop_pct=1.5)  # 150% — mis-scaled percent
    with pytest.raises(ValueError):
        cfg.validate()


def test_config_basket_size_bounds():
    for n in (2, 11):
        cfg = TradingSessionConfig(total_allocated_capital=1e6,
                                   strategy="intraday_basket", top_n_stocks=n)
        with pytest.raises(ValueError):
            cfg.validate()
    for n in (3, 10):
        TradingSessionConfig(total_allocated_capital=1e6,
                             strategy="intraday_basket",
                             top_n_stocks=n).validate()


def test_config_knobs_inert_for_kill_switch():
    """Kill-switch sessions must NOT be validated against the trail knobs (so an
    absurd default can't block them) — only the kill-switch rules apply."""
    cfg = TradingSessionConfig(total_allocated_capital=1e6,
                               strategy="portfolio_kill_switch",
                               floor_pct=0.49, arm_pct=0.01,  # floor>arm: inert
                               square_off_time="00:00:00")    # before entry: inert
    cfg.validate()  # must not raise


def test_config_roundtrip_preserves_strategy_and_knobs():
    cfg = TradingSessionConfig(total_allocated_capital=1e6,
                               strategy="intraday_basket", top_n_stocks=4,
                               arm_pct=0.012, floor_pct=0.011,
                               trail_giveback_pct=0.005, stop_pct=0.02,
                               square_off_time="15:20:00")
    c2 = TradingSessionConfig.from_json(cfg.to_json())
    assert c2.strategy == "intraday_basket"
    assert abs(c2.arm_pct - 0.012) < 1e-12
    assert abs(c2.floor_pct - 0.011) < 1e-12
    assert abs(c2.trail_giveback_pct - 0.005) < 1e-12
    assert abs(c2.stop_pct - 0.02) < 1e-12
    assert c2.square_off_time == "15:20:00"


def test_legacy_config_json_defaults_to_kill_switch():
    """A config_json from before this feature (no 'strategy' key) loads as the
    kill-switch strategy → existing sessions behave unchanged."""
    c = TradingSessionConfig.from_json('{"total_allocated_capital": 1000000}')
    assert c.strategy == "portfolio_kill_switch"


# ════════════════════════════════════════════════════════════════════════════
# STATE PERSISTENCE + RESTART
# ════════════════════════════════════════════════════════════════════════════

def _make_session_row(session_id, cap, config_json="{}"):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json)
               VALUES (?,?,?,?,?,?)""",
            (session_id, "2026-06-25T09:15:00", "RUNNING", "paper", cap,
             config_json))
        con.commit()


def test_trail_state_persist_and_reload(clean_positions):
    import uuid
    sid = uuid.uuid4().hex
    cap = 1e6
    _make_session_row(sid, cap)
    mon = PortfolioMonitor(sid, cap)
    # default before any save
    st0 = mon.load_trail_state()
    assert st0.armed is False and st0.peak == 0.0
    # save + reload
    mon.save_trail_state(TrailState(armed=True, peak=0.034))
    st1 = mon.load_trail_state()
    assert st1.armed is True
    assert abs(st1.peak - 0.034) < 1e-12


def test_restart_resumes_armed_peak_and_trails(clean_positions):
    """A fresh PortfolioMonitor (simulating a restart) restores armed+peak and
    the engine then trails from the restored peak — a giveback exits, a new high
    ratchets."""
    import uuid
    sid = uuid.uuid4().hex
    cap = 1e6
    _make_session_row(sid, cap)
    mon = PortfolioMonitor(sid, cap)
    mon.save_trail_state(TrailState(armed=True, peak=0.05))

    # Simulate restart: brand-new monitor instance loads from DB.
    mon2 = PortfolioMonitor(sid, cap)
    st = mon2.load_trail_state()
    assert st.armed is True
    assert abs(st.peak - 0.05) < 1e-12

    p = _params(floor_pct=0.01, trail_giveback_pct=0.0075)
    # G gives back below trigger (0.0425) → TRAIL_EXIT, peak preserved.
    d = decide(0.04, st, p)
    assert d.action == "EXIT" and d.reason == "TRAIL_EXIT"
    # A new high instead → ratchet, HOLD.
    d2 = decide(0.06, st, p)
    assert d2.action == "HOLD"
    assert abs(d2.state.peak - 0.06) < 1e-12


# ════════════════════════════════════════════════════════════════════════════
# SESSION.TICK INTEGRATION (paper, MockBroker) — the four exit reasons flatten
# ════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}
    shared = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        # dry_run=False so paper-mode flatten still records exits on the mock,
        # letting us assert positions actually got flattened (no real Kite).
        mb = MockBroker(profile=profile, dry_run=False, ltps=dict(shared))
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _start_intraday(square_off="23:59:59", **knobs):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    square_off = knobs.pop("square_off_time", square_off)
    cfg = TradingSessionConfig(
        total_allocated_capital=300000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", kill_switch_enabled=False,
        square_off_time=square_off, **knobs)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())   # fires entries; autostart disabled in tests
    return sess


def _set_basket_gross(sess, g):
    """Drive the basket's invested-basis gross return to ~g by setting LTPs.
    uPnL = g * invested_basis; spread it onto symbol A's LTP. Sets the LTP on the
    BROKER mock (not just the DB row) so tick()'s refresh_ltps re-marks to it."""
    ib = sess.monitor.invested_basis()
    target_upnl = g * ib
    pos = sess.registry.get_open_positions()
    p0 = pos[0]
    qty = p0["qty"]
    avg = p0["avg_price"]
    new_ltp = avg + target_upnl / qty
    broker = sess.brokers[p0["broker_profile"]]
    broker.set_ltp(p0["symbol"], float(new_ltp))
    # Also write it to the DB row so a status() read before the next tick agrees.
    sess.registry.update_ltp(p0["symbol"], float(new_ltp),
                             broker_profile=p0.get("broker_profile"))


def test_session_arm_then_hold(clean_positions, patched_brokers):
    sess = _start_intraday(arm_pct=0.01, floor_pct=0.01)
    _set_basket_gross(sess, 0.012)
    out = asyncio.run(sess.tick())
    assert out["strategy"] == "intraday_basket"
    assert out["trail_action"] == "ARM"
    assert out["trail_armed"] is True
    assert not out["kill_switch_fired"]
    # state persisted
    st = sess.monitor.load_trail_state()
    assert st.armed is True and abs(st.peak - 0.012) < 5e-4


def test_session_trail_exit_flattens(clean_positions, patched_brokers):
    sess = _start_intraday(arm_pct=0.01, floor_pct=0.01,
                           trail_giveback_pct=0.0075)
    # Arm + ratchet a high peak.
    _set_basket_gross(sess, 0.05)
    asyncio.run(sess.tick())
    assert sess.monitor.load_trail_state().peak >= 0.049
    # Give back below trigger (0.0425) → TRAIL_EXIT.
    _set_basket_gross(sess, 0.03)
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert out["kill_reason"] == "TRAIL_EXIT"
    assert sess.status()["status"] == "CLOSED"
    assert sess.status()["exit_reason"] == "TRAIL_EXIT"


def test_session_floor_exit_flattens(clean_positions, patched_brokers):
    sess = _start_intraday(arm_pct=0.01, floor_pct=0.01,
                           trail_giveback_pct=0.0075)
    # Arm with a peak just above arm so giveback level < floor.
    _set_basket_gross(sess, 0.012)
    asyncio.run(sess.tick())
    # Drop to the floor → FLOOR_EXIT (at floor, not below).
    _set_basket_gross(sess, 0.009)
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert out["kill_reason"] == "FLOOR_EXIT"
    assert sess.status()["exit_reason"] == "FLOOR_EXIT"


def test_session_stop_flattens(clean_positions, patched_brokers):
    sess = _start_intraday(stop_pct=0.015)
    _set_basket_gross(sess, -0.02)   # pre-arm, below -stop
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert out["kill_reason"] == "STOP"
    assert sess.status()["exit_reason"] == "STOP"


def test_session_square_off_flattens_regardless(clean_positions, patched_brokers):
    now = datetime.now(IST)
    sq = (now - timedelta(minutes=1)).strftime("%H:%M:%S")
    # Use an entry_time earlier than square-off so the config validates.
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(
        total_allocated_capital=300000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", kill_switch_enabled=False,
        entry_time="00:00:01", square_off_time=sq)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # A WINNING basket must still square off.
    _set_basket_gross(sess, 0.005)
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert out["kill_reason"] == "SQUARE_OFF"
    assert sess.status()["status"] == "CLOSED"
    assert sess.status()["exit_reason"] == "SQUARE_OFF"


def test_session_no_premature_exit(clean_positions, patched_brokers):
    sess = _start_intraday(arm_pct=0.01, stop_pct=0.015)
    _set_basket_gross(sess, 0.005)   # below arm, above -stop
    out = asyncio.run(sess.tick())
    assert not out["kill_switch_fired"]
    assert out["trail_action"] == "HOLD"
    assert sess.status()["status"] == "RUNNING"


def test_status_exposes_trail_fields(clean_positions, patched_brokers):
    sess = _start_intraday(arm_pct=0.01, floor_pct=0.01, square_off_time="15:29:00")
    st = sess.status()
    assert st["strategy"] == "intraday_basket"
    assert "trail" in st
    t = st["trail"]
    for k in ("armed", "peak", "current_gross_return", "trigger", "arm_pct",
              "floor_pct", "trail_giveback_pct", "stop_pct", "square_off_time",
              "seconds_to_square_off"):
        assert k in t, f"missing trail field {k}"
    assert t["arm_pct"] == 0.01
    assert t["square_off_time"] == "15:29:00"
    assert st["trail_armed"] is False


# ════════════════════════════════════════════════════════════════════════════
# REGRESSION: portfolio_kill_switch path untouched
# ════════════════════════════════════════════════════════════════════════════

def test_kill_switch_strategy_tick_unchanged(clean_positions, patched_brokers):
    """A default (portfolio_kill_switch) session ticks via the kill-switch path:
    no trail fields, kill switch governs the exit."""
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(
        total_allocated_capital=200000.0, top_n_stocks=2, sizing_mode="equal",
        kill_switch_enabled=True, kill_switch_pct=0.01,
        kill_switch_direction="both")
    assert cfg.strategy == "portfolio_kill_switch"
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    out = asyncio.run(sess.tick())
    # No trail keys on the kill-switch tick payload.
    assert "trail_action" not in out
    assert "strategy" not in out
    assert sess.status()["status"] == "RUNNING"
