"""Behavioural lock for the Falcon Intraday Magnifier: split entry (50% @09:15 +
50% @09:16), DEFERRED trail arming (no stop until BOTH legs fill), blended cost,
and 5× MIS sizing. Paper / dry-run throughout (patched MockBrokers).

The deferred-arm test is MUTATION-VERIFIED: before the second leg completes, a
profit far above the arm threshold must NOT arm/exit (trail dormant). If the
deferred-arm gate were removed, that assertion fails.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


@pytest.fixture
def patched_brokers(monkeypatch):
    """MockBroker with 5× MIS margins so target qty is even + splittable.
    ltp/margin: A 100/20, B 200/40, C 50/10 → qty = capital_slice / margin."""
    created = {}
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}
    margins = {"A": 20.0, "B": 40.0, "C": 10.0}   # 5× (margin = price / 5)

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=dict(ltps),
                        margins=dict(margins), margins_available=True)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


@pytest.fixture
def all_high_tier(monkeypatch):
    """Treat every seeded pick as high-tier so the fire tests exercise the split /
    deferred-arm mechanics, not the tier classifier (that has its own test)."""
    def _all(picks, high_tier):
        return list(picks), {p.symbol: "GOLD" for p in picks}
    monkeypatch.setattr(sess_mod, "_magnifier_high_tier_filter", _all)


def _mag_cfg(capital=300000.0):
    return TradingSessionConfig(
        total_allocated_capital=capital, strategy="intraday_magnifier",
        order_product="MIS", instrument_type="EQ", direction="long",
        top_n_stocks=15, sizing_mode="equal",
        arm_pct=0.06, floor_pct=0.02, trail_giveback_pct=0.05, stop_pct=0.03,
        magnifier_second_leg_offset_sec=600)   # long offset → we drive leg 2 by hand


def _positions(session_id):
    with falcon_conn() as con:
        return {r["symbol"]: dict(r) for r in con.execute(
            "SELECT * FROM autotrade_positions WHERE session_id=?",
            (session_id,)).fetchall()}


def _mag_flags(session_id):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT mag_entry_complete, invested_basis FROM autotrade_sessions "
            "WHERE session_id=?", (session_id,)).fetchone()
    return dict(r)


# ── 1. Split entry places 50% at the first leg; NO basis frozen, NO stop yet ────

def test_split_entry_places_half_leg1_and_defers_arm(clean_positions,
                                                     patched_brokers, all_high_tier):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    assert res["magnifier_entry_complete"] is False
    assert res["magnifier_stage"] == 1

    pos = _positions(sess.session_id)
    # 100000 slice / margin: A 5000, B 2500, C 10000 target → leg1 = half.
    assert pos["A"]["qty"] == 2500
    assert pos["B"]["qty"] == 1250
    assert pos["C"]["qty"] == 5000
    # NO stop on the 09:15 leg (no broker SL-M placed by phase 1).
    for mb in patched_brokers.values():
        assert mb.slm_orders == []
    # Entry NOT yet complete + invested_basis NOT frozen on the half-leg.
    flags = _mag_flags(sess.session_id)
    assert flags["mag_entry_complete"] == 0
    assert not flags["invested_basis"]        # None/0 until both legs are in


# ── 2. Second leg completes the basket at the BLENDED cost + arms the trail ─────

def test_second_leg_blends_cost_and_arms(clean_positions, patched_brokers,
                                         all_high_tier):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    # Move the market UP before the 09:16 leg so the two fills differ → the blend
    # is provable (A: leg1@100, leg2@120 → blended 110 over 5000 shares).
    for mb in patched_brokers.values():
        mb.set_ltp("A", 120.0)
        mb.set_ltp("B", 240.0)
        mb.set_ltp("C", 60.0)
    res2 = asyncio.run(sess.complete_magnifier_entry())
    assert res2["magnifier_entry_complete"] is True
    assert res2["magnifier_stage"] == 2

    pos = _positions(sess.session_id)
    # Full target qty now (both legs averaged into ONE row).
    assert pos["A"]["qty"] == 5000
    assert pos["B"]["qty"] == 2500
    assert pos["C"]["qty"] == 10000
    # Blended avg = (2500*100 + 2500*120)/5000 = 110.
    assert abs(pos["A"]["avg_price"] - 110.0) < 1e-6
    assert abs(pos["B"]["avg_price"] - 220.0) < 1e-6
    assert abs(pos["C"]["avg_price"] - 55.0) < 1e-6
    # Invested basis frozen NOW on the blended basket.
    flags = _mag_flags(sess.session_id)
    assert flags["mag_entry_complete"] == 1
    assert flags["invested_basis"] and flags["invested_basis"] > 0

    # idempotent — a second call is a no-op.
    again = asyncio.run(sess.complete_magnifier_entry())
    assert again.get("already_complete") is True


# ── 3. MUTATION: the trail is DORMANT until BOTH legs fill ──────────────────────

def test_deferred_arm_no_exit_before_completion(clean_positions, patched_brokers,
                                                all_high_tier):
    """A profit far above the arm threshold must NOT arm/exit while only the 09:15
    half-leg is in. After the second leg completes, the same book is managed by the
    trail. If the deferred-arm gate were removed, the pre-completion tick would
    ARM/EXIT and this test would fail."""
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    # Huge profit move (well above arm 6% of capital at 5×) BEFORE completion.
    for mb in patched_brokers.values():
        mb.set_ltp("A", 140.0)
        mb.set_ltp("B", 280.0)
        mb.set_ltp("C", 70.0)
    tick1 = asyncio.run(sess.tick())
    # DORMANT: no arm, no exit, entry still incomplete.
    assert tick1["magnifier_entry_complete"] is False
    assert tick1["trail_action"] == "PENDING_SPLIT_ENTRY"
    assert tick1["kill_switch_fired"] is False
    assert sess.status()["status"] == "RUNNING"
    # Position still the half-leg only (nothing exited).
    assert _positions(sess.session_id)["A"]["qty"] == 2500

    # Complete the split entry → the trail engages from here on the blended cost.
    asyncio.run(sess.complete_magnifier_entry())
    tick2 = asyncio.run(sess.tick())
    assert tick2.get("strategy") == "intraday_magnifier"
    assert "trail_action" in tick2
    # No longer the pending sentinel → the trail is now live.
    assert tick2["trail_action"] != "PENDING_SPLIT_ENTRY"


# ── 4. 5× MIS sizing: leg1 + leg2 == the full leveraged target ─────────────────

def test_mis_5x_sizing_full_target(clean_positions, patched_brokers, all_high_tier):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_mag_cfg(capital=300000.0), mode="paper")
    asyncio.run(sess.start(when="now"))
    asyncio.run(sess.complete_magnifier_entry())
    pos = _positions(sess.session_id)
    # 100000 slice at 5× (margin = price/5): A 100000/20=5000, notional 5000*100
    # = 500000 = 5× the 100000 cash slice. Same 5× for B and C.
    assert pos["A"]["qty"] == 5000    # 5× leverage on the 100k slice
    assert pos["B"]["qty"] == 2500
    assert pos["C"]["qty"] == 10000


# ── 5. Isolation regression: the magnifier never touches falcon_position_state ──

def test_magnifier_does_not_touch_falcon_position_state(clean_positions,
                                                        patched_brokers,
                                                        all_high_tier):
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO falcon_position_state
               (symbol, managed_by, product, qty, avg_entry, initial_sl_price,
                current_sl_price, target_price, high_water_price, entry_date)
               VALUES ('A','falcon','CNC', 999, 55.5, 50, 50, 70, 60, '2026-06-01')""")
        con.commit()
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    asyncio.run(sess.complete_magnifier_entry())
    with falcon_conn() as con:
        fp = con.execute(
            "SELECT qty, avg_entry, managed_by FROM falcon_position_state "
            "WHERE symbol='A'").fetchone()
        total_fp = con.execute(
            "SELECT COUNT(*) FROM falcon_position_state").fetchone()[0]
    assert fp["qty"] == 999 and abs(fp["avg_entry"] - 55.5) < 1e-9
    assert fp["managed_by"] == "falcon" and total_fp == 1
