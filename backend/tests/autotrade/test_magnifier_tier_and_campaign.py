"""High-tier universe selection + the Magnifier monthly campaign (ladder)."""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.session import TradingSession, load_falcon_picks, set_fake_now
from autotrade.ladder import LadderCampaign, CAMPAIGN_MAGNIFIER, MAGNIFIER_TOP_N
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


# ── 1. high-tier selection keeps only the real high-tier names ─────────────────

def test_high_tier_filter_selects_only_high_tier(clean_positions, monkeypatch):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0), ("D", 4, 6.0, 75.0),
                  ("E", 5, 5.0, 40.0)])

    # Stamp deterministic tiers by symbol (bypasses the ohlc-based classifier,
    # which needs bars the test DB doesn't seed). A/C high-tier; B/D/E not.
    tier_by_sym = {"A": "GOLD", "B": "STANDARD", "C": "PREMIUM-Pullback",
                   "D": "AVOID", "E": "STANDARD-weak"}

    def fake_enrich(con, picks, signal_date):
        for p in picks:
            p["signal_tier"] = tier_by_sym.get(p["symbol"])

    monkeypatch.setattr("power_user.services.signal_tier.enrich_picks",
                        fake_enrich)

    picks = load_falcon_picks(top_n=15)
    from autotrade.session import _magnifier_high_tier_filter
    high = ["ENTERPRISE-Dryup", "GOLD", "GOLD-baseline",
            "PREMIUM-Compression", "PREMIUM-Pullback"]
    kept, tiers = _magnifier_high_tier_filter(picks, high)
    kept_syms = {p.symbol for p in kept}
    assert kept_syms == {"A", "C"}      # only the high-tier names survive
    assert tiers["B"] == "STANDARD"     # classified but excluded


def test_select_magnifier_picks_uses_top_n_pool(clean_positions, monkeypatch):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    # keep only B high-tier
    def _only_b(picks, high_tier):
        return [p for p in picks if p.symbol == "B"], {}
    monkeypatch.setattr(sess_mod, "_magnifier_high_tier_filter", _only_b)
    from autotrade.config import TradingSessionConfig
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_magnifier",
        order_product="MIS", instrument_type="EQ", direction="long",
        top_n_stocks=15)
    sess = TradingSession.create(cfg, mode="paper")
    picks = sess._select_magnifier_picks()
    assert [p.symbol for p in picks] == ["B"]


# ── 2. Magnifier campaign create invariants ────────────────────────────────────

def test_magnifier_campaign_create_forces_mis_and_full_daily_cash(clean_positions):
    # Operator picks the cash/day (NOT hard-coded). MIS is forced; hold_length=1
    # → per_basket = total (one day's full cash/day).
    lad = LadderCampaign.create(total_capital=250000.0,
                                campaign_type="magnifier", mode="paper",
                                order_product="CNC")  # CNC ignored → forced MIS
    assert lad.campaign_type == CAMPAIGN_MAGNIFIER
    assert lad.order_product == "MIS"
    assert lad.per_basket_capital == 250000.0      # full cash/day, not total/3
    child = lad._build_child_config()
    assert child.strategy == "intraday_magnifier"
    assert child.order_product == "MIS"
    assert child.instrument_type == "EQ"
    assert child.square_off_enabled is True        # INTRADAY (no overnight carry)
    assert child.top_n_stocks == MAGNIFIER_TOP_N   # Top-15 pool
    assert (child.arm_pct, child.floor_pct, child.trail_giveback_pct,
            child.stop_pct) == (0.06, 0.02, 0.05, 0.03)
    child.validate()   # a valid magnifier config


def test_positional_campaign_unchanged(clean_positions):
    # Additive: the default positional ladder is byte-for-byte unchanged.
    lad = LadderCampaign.create(total_capital=300000.0, mode="paper",
                                order_product="CNC")
    assert lad.campaign_type == "positional"
    assert lad.order_product == "CNC"
    assert lad.per_basket_capital == 100000.0      # total / 3
    child = lad._build_child_config()
    assert child.strategy == "intraday_basket"
    assert child.square_off_enabled is False       # positional carry


# ── 3. Magnifier campaign spawns an intraday child that fires split entry ───────

def test_magnifier_campaign_spawns_intraday_child(clean_positions, monkeypatch):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}
    margins = {"A": 20.0, "B": 40.0, "C": 10.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=dict(ltps),
                          margins=dict(margins), margins_available=True)
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)

    def _all(picks, high_tier):
        return list(picks), {p.symbol: "GOLD" for p in picks}
    monkeypatch.setattr(sess_mod, "_magnifier_high_tier_filter", _all)

    lad = LadderCampaign.create(total_capital=300000.0,
                                campaign_type="magnifier", mode="paper")
    lad.start()
    res = lad.daily_tick(ref_now=OPEN_NOW)
    assert res["opened"] is True
    sid = res["session_id"]
    # The child is an intraday_magnifier session with the 09:15 half-legs placed.
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status, config_json, mag_entry_complete FROM "
            "autotrade_sessions WHERE session_id=?", (sid,)).fetchone()
    assert row["status"] == "RUNNING"
    assert '"intraday_magnifier"' in row["config_json"]
    # Split-entry: second leg not yet in (fires on its own timer / next resume).
    assert row["mag_entry_complete"] == 0
    with falcon_conn() as con:
        n = con.execute(
            "SELECT COUNT(*) FROM autotrade_positions WHERE session_id=? "
            "AND status='OPEN'", (sid,)).fetchone()[0]
    assert n == 3    # the ~9-name basket (here 3 seeded), half-legs open
