"""CAPITAL-BASIS intraday_basket trail (2026-07-07).

The intraday_basket trail (arm / floor / giveback / basket-stop) now decides on
the ALLOCATED-CAPITAL gross return (PortfolioMonitor.compute_gross_return =
(uPnL+realised)/total_allocated_capital), NOT the notional/invested basis used by
the portfolio kill switch. On a leveraged product invested_basis >> deployed
capital, so the old notional-basis arm fired at leverage× the intended % of the
trader's money (on MIS 5x a notional 2.5% arm = 12.5% of capital). The knobs now
mean "% of my deployed money".

These tests PROVE the trail keys on capital, not notional:
  * a LEVERAGED session (invested_basis = 5× capital): the SAME uPnL that would
    NOT have armed under the old notional basis DOES arm on capital, and a basket
    down 3% of CAPITAL stops (old: 3% of notional = 15% of capital).
  * a 1x session (invested_basis == capital): behaviour identical (regression).
  * a POSITIONAL basket (square_off_enabled=False): arms at ~3% of capital, carry
    unaffected.
  * the sub-second ws_driver path uses the same capital basis.

All paper-safe (MockBroker, no real orders).
"""
import asyncio

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession
from autotrade.monitoring import ws_driver as ws_driver_mod
from falcon.db import falcon_conn
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker


# ── shared broker with a MUTABLE ltp map so we can control uPnL post-entry ─────
@pytest.fixture
def lev_brokers(monkeypatch):
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 50.0}
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return shared_ltps


def _signals():
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])


def _cfg(*, capital, arm_pct, stop_pct=0.03, square_off_enabled=True,
         max_hold_sessions=0):
    # trail_step_lock_enabled=False: this file is the FIXED-FLOOR / capital-basis
    # regression suite (it proves the trail keys on CAPITAL not notional, on the
    # legacy arm-at-arm_pct path). The PROFIT STEP-LOCK ratchet has its own suite
    # (test_step_lock_trail.py); opting out here keeps the legacy arm semantics.
    return TradingSessionConfig(
        total_allocated_capital=capital, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False, per_stock_stop_enabled=False,
        square_off_enabled=square_off_enabled, max_hold_sessions=max_hold_sessions,
        trail_step_lock_enabled=False,
        arm_pct=arm_pct, floor_pct=0.01, trail_giveback_pct=0.015,
        stop_pct=stop_pct, square_off_time="15:29:00")


def _normalise_positions(sid, qty=100, avg=100.0):
    """Set every open position to a known qty/avg so total uPnL is exactly
    n_positions * qty * (ltp - avg). Keeps the arithmetic in the tests exact."""
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_positions SET qty=?, avg_price=? "
            "WHERE session_id=? AND status='OPEN'", (qty, avg, sid))
        con.commit()


def _set_invested_basis(sid, value):
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET invested_basis=? "
                    "WHERE session_id=?", (value, sid))
        con.commit()


def _open_count(sid):
    with falcon_conn() as con:
        r = con.execute("SELECT COUNT(*) c FROM autotrade_positions "
                        "WHERE session_id=? AND status='OPEN'", (sid,)).fetchone()
    return r["c"]


def _start_leveraged(cfg, shared_ltps, *, invested_basis):
    """Create+start an intraday CNC basket, normalise to qty=100/avg=100 across 3
    legs, then OVERRIDE invested_basis to simulate leverage (CNC entry would
    otherwise freeze invested≈capital). Returns the session_id."""
    _signals()
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    sid = sess.session_id
    assert _open_count(sid) == 3
    _normalise_positions(sid)
    _set_invested_basis(sid, invested_basis)
    return sid


# ── 1. LEVERAGED: arms on CAPITAL, not NOTIONAL ───────────────────────────────

def test_leveraged_no_arm_below_capital_arm_threshold(clean_positions, lev_brokers,
                                                      market_hours_clock):
    """invested_basis = 5× capital. uPnL → capital return +3.25% / notional +0.65%.
    With arm_pct=0.05 the trail is fed the +3.25% CAPITAL number (NOT +0.65%) and
    does NOT arm (3.25% < 5%). Proves the fed basis is capital and the returned
    gross_return is the capital-basis number."""
    cap = 100_000.0
    sid = _start_leveraged(_cfg(capital=cap, arm_pct=0.05), lev_brokers,
                           invested_basis=5 * cap)
    # uPnL = 3 legs * 100 qty * (L-100) = +3250  ->  L = 100 + 3250/300
    for s in ("A", "B", "C"):
        lev_brokers[s] = 100.0 + 3250.0 / 300.0
    res = asyncio.run(TradingSession.load(sid).tick())

    # gross_return returned/logged is the CAPITAL basis (+3.25%), not notional.
    assert res["gross_return"] == pytest.approx(0.0325, abs=1e-6)
    assert res["gross_return"] != pytest.approx(0.0065, abs=1e-6)  # not notional
    # 3.25% < 5% arm → NOT armed, HELD.
    assert res["trail_armed"] is False
    assert res["trail_action"] == "HOLD"
    assert _open_count(sid) == 3


def test_leveraged_arms_on_capital_where_notional_would_not(clean_positions,
                                                            lev_brokers,
                                                            market_hours_clock):
    """invested_basis = 5× capital. Feed a higher uPnL: capital return +5.5% while
    notional is only +1.1%. arm_pct=0.05 → the CAPITAL-basis trail ARMS (5.5% ≥ 5%)
    whereas the OLD notional basis (1.1% < 5%) would NOT have armed. This is the
    core proof the trail now keys on capital, not notional."""
    cap = 100_000.0
    sid = _start_leveraged(_cfg(capital=cap, arm_pct=0.05), lev_brokers,
                           invested_basis=5 * cap)
    # uPnL = +5500 -> capital +5.5%, notional +1.1%.
    for s in ("A", "B", "C"):
        lev_brokers[s] = 100.0 + 5500.0 / 300.0
    sess = TradingSession.load(sid)
    sess._build_brokers()
    sess.monitor.refresh_ltps(sess.brokers)   # mark to the new LTPs before reading
    # Sanity: the two bases really are 5× apart (the leverage under test).
    assert sess.monitor.compute_gross_return() == pytest.approx(0.055, abs=1e-6)
    assert sess.monitor.compute_gross_return_invested() == pytest.approx(0.011,
                                                                         abs=1e-6)
    res = asyncio.run(sess.tick())

    assert res["gross_return"] == pytest.approx(0.055, abs=1e-6)   # capital basis
    assert res["trail_armed"] is True                              # ARMED on capital
    assert res["trail_action"] == "ARM"
    # The OLD notional value (1.1%) is below the 5% arm → old code would NOT arm.
    assert 0.011 < 0.05
    # Persisted so a restart resumes the armed trail.
    with falcon_conn() as con:
        r = con.execute("SELECT trail_armed, trail_peak FROM autotrade_sessions "
                        "WHERE session_id=?", (sid,)).fetchone()
    assert r["trail_armed"] == 1
    assert r["trail_peak"] == pytest.approx(0.055, abs=1e-6)


def test_leveraged_basket_stop_on_capital(clean_positions, lev_brokers,
                                          market_hours_clock):
    """invested_basis = 5× capital. Basket down 3% of CAPITAL (uPnL -3000) hits the
    -stop_pct hard stop and flattens. On the OLD notional basis this is only -0.6%
    (3% of capital = 15% of notional would be needed) → it would NOT have stopped."""
    cap = 100_000.0
    sid = _start_leveraged(_cfg(capital=cap, arm_pct=0.05, stop_pct=0.03),
                           lev_brokers, invested_basis=5 * cap)
    # uPnL = -3000 -> capital -3.0% (== -stop_pct), notional -0.6%.
    for s in ("A", "B", "C"):
        lev_brokers[s] = 100.0 - 3000.0 / 300.0
    sess = TradingSession.load(sid)
    sess._build_brokers()
    sess.monitor.refresh_ltps(sess.brokers)   # mark to the new LTPs before reading
    assert sess.monitor.compute_gross_return() == pytest.approx(-0.03, abs=1e-6)
    assert sess.monitor.compute_gross_return_invested() == pytest.approx(-0.006,
                                                                         abs=1e-6)
    res = asyncio.run(sess.tick())

    assert res["kill_reason"] == "STOP"
    assert res["kill_switch_fired"] is True
    assert res["gross_return"] == pytest.approx(-0.03, abs=1e-6)   # capital basis
    assert _open_count(sid) == 0                                   # flattened
    # -0.6% notional is ABOVE -3% stop → old notional code would NOT have stopped.
    assert -0.006 > -0.03


# ── 2. 1x REGRESSION: capital == invested → behaviour identical ───────────────

def test_1x_session_behaviour_unchanged(clean_positions, lev_brokers,
                                        market_hours_clock):
    """A 1x CNC basket: invested_basis == capital, so capital and notional returns
    are IDENTICAL and the trail arms at exactly the same point as before. arm_pct
    0.03; uPnL +3.5% arms. gross_return == both bases."""
    cap = 300_000.0   # 3 legs * 100 * 100 = 30_000 invested per the normaliser
    sid = _start_leveraged(_cfg(capital=cap, arm_pct=0.03), lev_brokers,
                           invested_basis=cap)  # 1x: invested == capital
    # uPnL = +10500 on 300k capital = +3.5%. (300 shares * (L-100) = 10500)
    for s in ("A", "B", "C"):
        lev_brokers[s] = 100.0 + 10500.0 / 300.0
    sess = TradingSession.load(sid)
    # 1x: the two bases coincide (regression invariant).
    assert sess.monitor.compute_gross_return() == pytest.approx(
        sess.monitor.compute_gross_return_invested(), abs=1e-9)
    res = asyncio.run(sess.tick())
    assert res["gross_return"] == pytest.approx(0.035, abs=1e-6)
    assert res["trail_armed"] is True
    assert res["trail_action"] == "ARM"


# ── 3. POSITIONAL (square_off_enabled=False): arm ~3% of capital, carry intact ─

def test_positional_arms_at_3pct_capital_and_carries(clean_positions, lev_brokers,
                                                     market_hours_clock):
    """Positional basket (square_off_enabled=False, arm_pct=0.03, 1x CNC): arms at
    +3% of capital and is NOT force-squared-off (carry). max_hold_sessions=0 → no
    time cap fires. Positional behaviour unchanged by the basis switch."""
    cap = 300_000.0
    sid = _start_leveraged(
        _cfg(capital=cap, arm_pct=0.03, square_off_enabled=False,
             max_hold_sessions=0),
        lev_brokers, invested_basis=cap)
    for s in ("A", "B", "C"):
        lev_brokers[s] = 100.0 + 9000.0 / 300.0   # +9000 = +3.0% of capital
    res = asyncio.run(TradingSession.load(sid).tick())
    assert res["gross_return"] == pytest.approx(0.03, abs=1e-6)
    assert res["trail_armed"] is True
    # NOT squared off (positional carries) — still holding, no MAX_HOLD/SQUARE_OFF.
    assert res["kill_reason"] not in ("SQUARE_OFF", "MAX_HOLD_EXIT")
    assert _open_count(sid) == 3


# ── 4. ws_driver sub-second path uses the SAME capital basis ──────────────────

def test_ws_driver_arms_on_capital_basis(clean_positions, lev_brokers,
                                         market_hours_clock):
    """The sub-second ws_driver._tick_once feeds the trail the ALLOCATED-CAPITAL
    return too: a leveraged session at +5.5% capital / +1.1% notional ARMS
    (arm_pct=0.05) via the WS path, matching session.tick()."""
    from autotrade.monitoring import fire_guard
    cap = 100_000.0
    sid = _start_leveraged(_cfg(capital=cap, arm_pct=0.05), lev_brokers,
                           invested_basis=5 * cap)
    # Injected sub-second LTP source: L for capital +5.5% (notional +1.1%).
    lprice = 100.0 + 5500.0 / 300.0
    drv = ws_driver_mod._WSDriver(sid, 0.1, ltp_source=lambda s: lprice)
    sess = TradingSession.load(sid)
    sess._build_brokers()
    drv._tick_once(sess, fire_guard)

    with falcon_conn() as con:
        r = con.execute("SELECT trail_armed, trail_peak FROM autotrade_sessions "
                        "WHERE session_id=?", (sid,)).fetchone()
    assert r["trail_armed"] == 1                                   # armed on capital
    assert r["trail_peak"] == pytest.approx(0.055, abs=1e-6)       # capital-basis peak
    # Basket not flattened (ARM is not an EXIT).
    assert _open_count(sid) == 3
