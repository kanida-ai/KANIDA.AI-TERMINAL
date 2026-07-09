"""R1 — the trail peak persist is a MONOTONE ratchet.

The 5s tick_driver and the sub-second ws_driver both load→decide→save the trail
state. A stale writer (loaded an older peak) must never regress the persisted
peak, which would lower the exit trigger and over-give-back profit. The save is
MAX(existing, new), so a late/low write can't step the peak down.
"""
import pytest

from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession
from autotrade.monitoring.trail_engine import TrailState
from falcon.db import falcon_conn


def _mk():
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               strategy="intraday_basket")
    return TradingSession.create(cfg, mode="paper")


def _peak(session_id):
    with falcon_conn() as con:
        r = con.execute("SELECT trail_armed, trail_peak FROM autotrade_sessions "
                        "WHERE session_id=?", (session_id,)).fetchone()
    return dict(r)


def test_session_peak_never_regresses(clean_positions):
    sess = _mk()
    sess.monitor.save_trail_state(TrailState(armed=True, peak=0.05))
    assert _peak(sess.session_id)["trail_peak"] == pytest.approx(0.05)

    # A stale writer with a LOWER peak must NOT lower it.
    sess.monitor.save_trail_state(TrailState(armed=True, peak=0.03))
    assert _peak(sess.session_id)["trail_peak"] == pytest.approx(0.05)

    # A higher peak DOES ratchet up.
    sess.monitor.save_trail_state(TrailState(armed=True, peak=0.07))
    assert _peak(sess.session_id)["trail_peak"] == pytest.approx(0.07)


def test_armed_never_unarms(clean_positions):
    sess = _mk()
    sess.monitor.save_trail_state(TrailState(armed=True, peak=0.04))
    assert _peak(sess.session_id)["trail_armed"] == 1
    # A stale write with armed=False cannot un-arm.
    sess.monitor.save_trail_state(TrailState(armed=False, peak=0.04))
    assert _peak(sess.session_id)["trail_armed"] == 1


def test_per_stock_peak_never_regresses(clean_positions):
    sess = _mk()
    prof = sess.config.broker_profiles[0].profile_id if \
        sess.config.broker_profiles else "zerodha_default"
    sess.registry.register(symbol="A", broker_profile=prof, qty=100,
                           avg_price=100.0, product="CNC", instrument_type="EQ")

    sess.monitor.save_per_stock_trail_state("A", TrailState(armed=True, peak=0.06))
    sess.monitor.save_per_stock_trail_state("A", TrailState(armed=True, peak=0.02))
    with falcon_conn() as con:
        r = con.execute("SELECT pos_trail_armed, pos_trail_peak "
                        "FROM autotrade_positions WHERE session_id=? AND symbol=?",
                        (sess.session_id, "A")).fetchone()
    assert r["pos_trail_peak"] == pytest.approx(0.06)   # not regressed to 0.02
    assert r["pos_trail_armed"] == 1
