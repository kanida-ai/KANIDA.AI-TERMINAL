"""Behavioural lock for the REDEFINED Falcon BTST Oscillator.

The BTST Oscillator now REUSES the Magnifier's two-leg split entry (50% @09:15 +
50% @09:16, blended cost) + the SAME Top-15 high-tier selection, but on CNC at 1×
(no leverage), POSITIONAL (carry overnight), 2-session hold, with NO profit trail
armed and ONLY the −6% disaster stop on the blended cost.

Covered here:
  * DELTA 1 — selection returns the high-tier-filtered Top-15 basket (rank order
    preserved); zero high-tier → no entry (FAILED, same fail-safe as the Magnifier).
  * DELTA 2 — split entry: 50% at leg-1 (CNC, NO stop/trail), 50% at leg-2, blended
    cost, invested basis frozen on the blend; the −6% disaster stop fires on the
    BLENDED cost and NO trail is ever armed (a profit far above any arm does NOT arm
    or exit); a loss just above −6% does NOT fire.
  * The BTST ladder campaign (campaign_type='btst') forces CNC, sizes the sleeve to
    capital/2, and builds a btst_oscillator child.
  * Config validation for the new btst_oscillator strategy.
  * REGRESSION: the Magnifier + default positional ladder are UNCHANGED.

Paper / dry-run throughout (patched MockBrokers).
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, load_falcon_picks, set_fake_now
from autotrade.ladder import (LadderCampaign, CAMPAIGN_BTST, BTST_TOP_N,
                              BTST_MAX_HOLD, BTST_STOP_PCT, BTST_ARM_PCT)
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
    """MockBroker sized for 1× CNC cash (margin == price → qty = slice / price
    whether the sizer takes the cash or the margin path)."""
    created = {}
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}
    margins = {"A": 100.0, "B": 200.0, "C": 50.0}   # 1× (margin == price)

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
    """Treat every seeded pick as high-tier so the split / stop tests exercise the
    entry + exit mechanics, not the tier classifier (that has its own test)."""
    def _all(picks, high_tier):
        return list(picks), {p.symbol: "GOLD" for p in picks}
    monkeypatch.setattr(sess_mod, "_magnifier_high_tier_filter", _all)


def _btst_cfg(capital=300000.0, second_leg_offset=600):
    return TradingSessionConfig(
        total_allocated_capital=capital, strategy="btst_oscillator",
        order_product="CNC", instrument_type="EQ", direction="long",
        top_n_stocks=BTST_TOP_N, sizing_mode="equal",
        arm_pct=BTST_ARM_PCT, floor_pct=0.01, trail_giveback_pct=0.04,
        stop_pct=BTST_STOP_PCT, square_off_enabled=False,
        max_hold_sessions=BTST_MAX_HOLD,
        # Mirror the real BTST child (ladder._make_child_config): step-lock OFF so
        # the profit trail never arms below arm_pct (0.5, unreachable).
        trail_step_lock_enabled=False,
        magnifier_second_leg_offset_sec=second_leg_offset)   # drive leg 2 by hand


def _positions(session_id):
    with falcon_conn() as con:
        return {r["symbol"]: dict(r) for r in con.execute(
            "SELECT * FROM autotrade_positions WHERE session_id=?",
            (session_id,)).fetchall()}


def _flags(session_id):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT mag_entry_complete, invested_basis FROM autotrade_sessions "
            "WHERE session_id=?", (session_id,)).fetchone()
    return dict(r)


# ── DELTA 1: selection = high-tier-filtered Top-15, rank order preserved ────────

def test_btst_selection_high_tier_only_rank_preserved(clean_positions, monkeypatch):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0), ("D", 4, 6.0, 75.0),
                  ("E", 5, 5.0, 40.0)])
    # A (rank1) and C (rank3) high-tier; B/D/E not. Reuses the SHARED filter via
    # the real classifier path, stubbed by symbol.
    tier_by_sym = {"A": "GOLD", "B": "STANDARD", "C": "PREMIUM-Pullback",
                   "D": "AVOID", "E": "STANDARD-weak"}

    def fake_enrich(con, picks, signal_date):
        for p in picks:
            p["signal_tier"] = tier_by_sym.get(p["symbol"])
    monkeypatch.setattr("power_user.services.signal_tier.enrich_picks",
                        fake_enrich)

    cfg = _btst_cfg()
    sess = TradingSession.create(cfg, mode="paper")
    picks = sess._select_magnifier_picks()   # BTST reuses the shared selection
    # only the high-tier names survive, in rank order (A before C).
    assert [p.symbol for p in picks] == ["A", "C"]


def test_btst_zero_high_tier_is_no_entry(clean_positions, patched_brokers,
                                         monkeypatch):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    # No pick classifies high-tier → empty basket → FAILED (same fail-safe as the
    # Magnifier; NEVER falls back to un-filtered picks).
    def _none(picks, high_tier):
        return [], {p.symbol: "STANDARD" for p in picks}
    monkeypatch.setattr(sess_mod, "_magnifier_high_tier_filter", _none)

    sess = TradingSession.create(_btst_cfg(), mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "FAILED"
    assert res["strategy"] == "btst_oscillator"
    assert _positions(sess.session_id) == {}


# ── DELTA 2: split entry — 50% leg1 (CNC, NO stop), NO basis frozen yet ─────────

def test_btst_split_entry_half_leg1_cnc_no_stop(clean_positions, patched_brokers,
                                                all_high_tier):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_btst_cfg(), mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    assert res["strategy"] == "btst_oscillator"
    assert res["magnifier_entry_complete"] is False
    assert res["magnifier_stage"] == 1

    pos = _positions(sess.session_id)
    # 100000 slice / price (1× cash): A 1000, B 500, C 2000 target → leg1 = half.
    assert pos["A"]["qty"] == 500
    assert pos["B"]["qty"] == 250
    assert pos["C"]["qty"] == 1000
    # CNC 1× (not MIS): the order product on the placed leg is CNC.
    assert pos["A"]["product"] == "CNC"
    # NO stop on the 09:15 leg (no broker SL-M placed by phase 1).
    for mb in patched_brokers.values():
        assert mb.slm_orders == []
    # Entry NOT yet complete + invested_basis NOT frozen on the half-leg.
    flags = _flags(sess.session_id)
    assert flags["mag_entry_complete"] == 0
    assert not flags["invested_basis"]


# ── DELTA 2: second leg blends the cost, freezes basis, arms NO trail ───────────

def test_btst_second_leg_blends_cost_no_trail(clean_positions, patched_brokers,
                                              all_high_tier):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_btst_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    # Move the market UP before the 09:16 leg → the two fills differ, so the blend
    # is provable (A: leg1@100, leg2@120 → blended 110 over 1000 shares).
    for mb in patched_brokers.values():
        mb.set_ltp("A", 120.0)
        mb.set_ltp("B", 240.0)
        mb.set_ltp("C", 60.0)
    res2 = asyncio.run(sess.complete_btst_entry())
    assert res2["strategy"] == "btst_oscillator"
    assert res2["magnifier_entry_complete"] is True
    assert res2["magnifier_stage"] == 2

    pos = _positions(sess.session_id)
    # Full target qty now (both legs averaged into ONE row).
    assert pos["A"]["qty"] == 1000
    assert pos["B"]["qty"] == 500
    assert pos["C"]["qty"] == 2000
    # Blended avg = (500*100 + 500*120)/1000 = 110.
    assert abs(pos["A"]["avg_price"] - 110.0) < 1e-6
    assert abs(pos["B"]["avg_price"] - 220.0) < 1e-6
    assert abs(pos["C"]["avg_price"] - 55.0) < 1e-6
    # Invested basis frozen NOW on the blended basket.
    flags = _flags(sess.session_id)
    assert flags["mag_entry_complete"] == 1
    assert flags["invested_basis"] and flags["invested_basis"] > 0

    # idempotent — a second call is a no-op.
    again = asyncio.run(sess.complete_btst_entry())
    assert again.get("already_complete") is True


# ── DELTA 2: BEFORE completion the exit logic is DORMANT (no stop on bare leg1) ─

def test_btst_deferred_no_exit_before_completion(clean_positions, patched_brokers,
                                                 all_high_tier):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_btst_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    # A big DROP (well past −6%) BEFORE completion must NOT trip a stop — the bare
    # 09:15 half-leg carries no stop until both legs are in.
    for mb in patched_brokers.values():
        mb.set_ltp("A", 80.0)
        mb.set_ltp("B", 160.0)
        mb.set_ltp("C", 40.0)
    tick1 = asyncio.run(sess.tick())
    assert tick1["magnifier_entry_complete"] is False
    assert tick1["trail_action"] == "PENDING_SPLIT_ENTRY"
    assert tick1["kill_switch_fired"] is False
    assert sess.status()["status"] == "RUNNING"
    assert _positions(sess.session_id)["A"]["qty"] == 500   # still the half-leg


# ── DELTA 2: after completion — the −6% disaster stop fires on the BLENDED cost ─

def test_btst_minus6_disaster_stop_on_blended_cost(clean_positions, patched_brokers,
                                                   all_high_tier):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_btst_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    # Complete leg2 with prices FLAT (blended == entry → invested_basis == capital
    # 300000, so a clean −6% price move == gr_capital −0.06 exactly).
    asyncio.run(sess.complete_btst_entry())
    pos = _positions(sess.session_id)
    assert abs(pos["A"]["avg_price"] - 100.0) < 1e-6   # blended == entry (flat)

    # −6% on every name → basket gr == −0.06 → the disaster STOP fires.
    for mb in patched_brokers.values():
        mb.set_ltp("A", 94.0)
        mb.set_ltp("B", 188.0)
        mb.set_ltp("C", 47.0)
    tick = asyncio.run(sess.tick())
    assert tick["strategy"] == "btst_oscillator"
    assert tick["kill_switch_fired"] is True
    assert abs(tick["gross_return"] + 0.06) < 1e-6      # measured on blended cost


def test_btst_no_fire_above_stop_and_no_arm_on_profit(clean_positions,
                                                      patched_brokers, all_high_tier):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_btst_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    asyncio.run(sess.complete_btst_entry())

    # (a) NO-FIRE: a −5% basket loss is above the −6% disaster stop → HOLD.
    for mb in patched_brokers.values():
        mb.set_ltp("A", 95.0)
        mb.set_ltp("B", 190.0)
        mb.set_ltp("C", 47.5)
    t_loss = asyncio.run(sess.tick())
    assert t_loss["kill_switch_fired"] is False
    assert abs(t_loss["gross_return"] + 0.05) < 1e-6

    # (b) NO-ARM: a big PROFIT does NOT arm the trail (arm_pct 0.5 unreachable) and
    # does NOT exit — BTST has no profit trail.
    for mb in patched_brokers.values():
        mb.set_ltp("A", 120.0)
        mb.set_ltp("B", 240.0)
        mb.set_ltp("C", 60.0)
    t_gain = asyncio.run(sess.tick())
    assert t_gain["kill_switch_fired"] is False
    assert t_gain.get("trail_armed") in (False, None)
    assert t_gain.get("trail_action") != "ARM"
    assert sess.status()["status"] == "RUNNING"


# ── BTST ladder campaign (campaign_type='btst') ─────────────────────────────────

def test_btst_campaign_forces_cnc_sizes_sleeve_by_hold(clean_positions):
    lad = LadderCampaign.create(total_capital=1000000.0, campaign_type="btst",
                                mode="paper", order_product="MTF")  # MTF → forced CNC
    assert lad.campaign_type == CAMPAIGN_BTST
    assert lad.order_product == "CNC"
    assert lad.per_basket_capital == 500000.0        # total / BTST_MAX_HOLD (2)
    child = lad._build_child_config()
    assert child.strategy == "btst_oscillator"
    assert child.order_product == "CNC"
    assert child.instrument_type == "EQ"
    assert child.direction == "long"
    assert child.square_off_enabled is False         # POSITIONAL carry
    assert child.max_hold_sessions == BTST_MAX_HOLD  # 2-session hold
    assert child.top_n_stocks == BTST_TOP_N          # Top-15 pool
    assert child.arm_pct == BTST_ARM_PCT             # 0.5 → no trail
    assert child.stop_pct == BTST_STOP_PCT           # −6% disaster stop
    child.validate()   # a valid btst_oscillator config


def test_btst_campaign_spawns_btst_child(clean_positions, monkeypatch):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}
    margins = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=dict(ltps),
                          margins=dict(margins), margins_available=True)
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)

    def _all(picks, high_tier):
        return list(picks), {p.symbol: "GOLD" for p in picks}
    monkeypatch.setattr(sess_mod, "_magnifier_high_tier_filter", _all)

    lad = LadderCampaign.create(total_capital=600000.0, campaign_type="btst",
                                mode="paper")
    lad.start()
    res = lad.daily_tick(ref_now=OPEN_NOW)
    assert res["opened"] is True
    sid = res["session_id"]
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status, config_json, mag_entry_complete FROM "
            "autotrade_sessions WHERE session_id=?", (sid,)).fetchone()
    assert row["status"] == "RUNNING"
    assert '"btst_oscillator"' in row["config_json"]
    assert row["mag_entry_complete"] == 0    # split: second leg not yet in


# ── Config validation for the new btst_oscillator strategy ──────────────────────

def test_btst_config_valid():
    _btst_cfg().validate()   # must not raise


def test_btst_config_rejects_non_cnc():
    cfg = _btst_cfg()
    cfg.order_product = "MIS"
    with pytest.raises(ValueError, match="CNC"):
        cfg.validate()


def test_btst_config_rejects_square_off_true():
    cfg = _btst_cfg()
    cfg.square_off_enabled = True
    with pytest.raises(ValueError, match="square_off_enabled must be False"):
        cfg.validate()


def test_btst_config_rejects_zero_hold():
    cfg = _btst_cfg()
    cfg.max_hold_sessions = 0
    with pytest.raises(ValueError, match="max_hold_sessions"):
        cfg.validate()


def test_btst_config_rejects_short_direction():
    cfg = _btst_cfg()
    cfg.direction = "short"
    with pytest.raises(ValueError, match="long-only"):
        cfg.validate()


# ── REGRESSION: the Magnifier + default positional ladder are UNCHANGED ─────────

def test_magnifier_campaign_still_forces_mis_and_full_cash(clean_positions):
    from autotrade.ladder import MAGNIFIER_TOP_N
    lad = LadderCampaign.create(total_capital=250000.0, campaign_type="magnifier",
                                mode="paper", order_product="CNC")
    assert lad.order_product == "MIS"
    assert lad.per_basket_capital == 250000.0        # hold_length 1 → full cash/day
    child = lad._build_child_config()
    assert child.strategy == "intraday_magnifier"
    assert child.order_product == "MIS"
    assert child.square_off_enabled is True          # intraday, unchanged
    assert child.top_n_stocks == MAGNIFIER_TOP_N
    assert (child.arm_pct, child.floor_pct, child.trail_giveback_pct,
            child.stop_pct) == (0.06, 0.02, 0.05, 0.03)
    child.validate()


def test_positional_ladder_still_top5_three_session(clean_positions):
    lad = LadderCampaign.create(total_capital=300000.0, mode="paper",
                                order_product="CNC")
    assert lad.campaign_type == "positional"
    assert lad.order_product == "CNC"
    assert lad.per_basket_capital == 100000.0        # total / 3, unchanged
    child = lad._build_child_config()
    assert child.strategy == "intraday_basket"
    assert child.top_n_stocks == 5                   # Top-5 pure-rank, unchanged
    assert child.square_off_enabled is False
    assert child.max_hold_sessions == 3
    child.validate()
