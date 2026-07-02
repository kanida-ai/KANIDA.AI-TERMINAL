"""CapitalAllocator — 3 sizing modes + quantity calc + InsufficientCapital."""
import math

import pytest

from autotrade.capital import CapitalAllocator, InsufficientCapitalError
from autotrade.config import TradingSessionConfig
from tests.autotrade.mock_broker import MockBroker


def _cfg(**kw):
    base = dict(total_allocated_capital=500000.0, top_n_stocks=5,
                instrument_type="EQ")
    base.update(kw)
    return TradingSessionConfig(**base)


def test_equal_distribution():
    cfg = _cfg(sizing_mode="equal", top_n_stocks=5)
    alloc = CapitalAllocator(cfg)
    out = alloc.allocate(["A", "B", "C", "D", "E"])
    assert all(abs(v - 100000.0) < 1e-6 for v in out.values())


def test_pct_cap_caps_concentration():
    cfg = _cfg(sizing_mode="pct_cap", max_pct_per_position=0.05, top_n_stocks=5)
    alloc = CapitalAllocator(cfg)
    out = alloc.allocate(["A", "B", "C", "D", "E"])
    # ₹5L * 5% = ₹25k cap, which is below ₹5L/5 = ₹1L → cap wins
    assert all(abs(v - 25000.0) < 1e-6 for v in out.values())


def test_manual_amounts():
    cfg = _cfg(sizing_mode="manual",
               manual_amounts={"INFY": 25000, "TCS": 50000, "WIPRO": 15000})
    alloc = CapitalAllocator(cfg)
    out = alloc.allocate(["INFY", "TCS", "WIPRO"])
    assert out == {"INFY": 25000, "TCS": 50000, "WIPRO": 15000}


def test_manual_total_exceeds_capital_rejected():
    cfg = _cfg(sizing_mode="manual",
               total_allocated_capital=50000,
               manual_amounts={"A": 40000, "B": 40000})
    with pytest.raises(ValueError):
        cfg.validate()


def test_quantity_eq_floor():
    cfg = _cfg(sizing_mode="equal", instrument_type="EQ")
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps={"A": 333.0})
    qty = alloc.calculate_quantity("A", 100000.0, b)
    assert qty == math.floor(100000.0 / 333.0)


def test_quantity_never_zero_raises():
    cfg = _cfg(sizing_mode="equal", instrument_type="EQ")
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps={"PRICEY": 5000.0})
    with pytest.raises(InsufficientCapitalError):
        alloc.calculate_quantity("PRICEY", 1000.0, b)


def test_quantity_fut_lot_rounding():
    # FUTURES sizing is now MARGIN-based (retail), not notional. Supply a per-lot
    # margin so the mock reports it; qty must still round to whole lots.
    cfg = _cfg(sizing_mode="equal", instrument_type="FUT", expiry_preference="near")
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps={"NIFTYW": 200.0, "NIFTYWFUT": 200.0},
                   lot_size=50, fut_margin_per_lot=40_000.0)
    qty = alloc.calculate_quantity("NIFTYW", 1_000_000.0, b)
    assert qty % 50 == 0 and qty > 0


# ── FEATURE C: MIS intraday leverage sizing ──────────────────────────────────

def test_mis_sizes_on_margin_leveraged():
    # MIS now leverages (sizes on intraday MIS margin), like MTF. LTP=100 but the
    # MIS margin is 20/share (5x) → floor(20000/20)=1000 vs cash floor(20000/100)=200.
    cfg = _cfg(sizing_mode="equal", instrument_type="EQ", order_product="MIS")
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps={"A": 100.0}, margins={"A": 20.0})
    qty = alloc.calculate_quantity("A", 20000.0, b)
    assert qty == 1000  # leveraged on the MIS margin, not cash LTP


def test_mis_refuses_to_overdeploy_when_margin_api_down():
    # Margin API DOWN → MIS must cash-fall-back (never over-deploy on leverage).
    cfg = _cfg(sizing_mode="equal", instrument_type="EQ", order_product="MIS")
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps={"A": 100.0}, margins_available=False)
    qty = alloc.calculate_quantity("A", 20000.0, b)
    assert qty == 200  # cash-sized on LTP (safe fallback), NOT leveraged


def test_mis_cached_path_leverages_too():
    cfg = _cfg(sizing_mode="equal", instrument_type="EQ", order_product="MIS")
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps={"A": 100.0}, margins={"A": 20.0})
    cache = alloc.prefetch(["A"], b)
    qty = alloc.calculate_quantity_cached("A", 20000.0, b, cache=cache)
    assert qty == 1000


def test_cnc_unchanged_by_margin_product_helper():
    # CNC (default equity) still cash-sizes on LTP — margin path never engaged.
    cfg = _cfg(sizing_mode="equal", instrument_type="EQ", order_product="CNC")
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps={"A": 100.0}, margins={"A": 20.0})
    qty = alloc.calculate_quantity("A", 20000.0, b)
    assert qty == 200  # LTP-sized (margin ignored for CNC)


# ── FEATURE C: stranded-cash redistribution + unaffordable-pick skip ──────────

_UTIL_LTPS = {"CANFINHOME": 700.0, "IDEA": 8.0, "PAGEIND": 43610.0,
              "ADANIENT": 2400.0, "MCX": 6500.0}


def test_util_redistribute_skips_unaffordable_and_deploys_more():
    # The exact ₹1L / 5-pick case. Slice = 20,000 each.
    # PAGEIND @ 43,610 > 20,000 → SKIPPED. Its slice freed into redistribution.
    cfg = _cfg(total_allocated_capital=100_000.0, top_n_stocks=5,
               sizing_mode="equal", instrument_type="EQ", order_product="CNC",
               redistribute_unused_capital=True)
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps=dict(_UTIL_LTPS))
    plan = alloc.plan_quantities(list(_UTIL_LTPS.keys()), b)
    skipped_syms = {s["symbol"] for s in plan["skipped"]}
    assert skipped_syms == {"PAGEIND"}          # too high-priced for its slice
    assert "PAGEIND" not in plan["quantities"]
    # Deployed must be MATERIALLY higher than the naive floor-each-slice (~76k)
    # AND never exceed the ₹1L budget.
    assert plan["deployed"] > 90_000.0
    assert plan["deployed"] <= 100_000.0 + 1e-6
    # Recompute deployed from quantities*LTP to prove the number is honest.
    recomputed = sum(q * _UTIL_LTPS[s] for s, q in plan["quantities"].items())
    assert abs(recomputed - plan["deployed"]) < 1e-6


def test_util_redistribute_off_preserves_naive_floor():
    cfg = _cfg(total_allocated_capital=100_000.0, top_n_stocks=5,
               sizing_mode="equal", instrument_type="EQ", order_product="CNC",
               redistribute_unused_capital=False)
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps=dict(_UTIL_LTPS))
    plan = alloc.plan_quantities(list(_UTIL_LTPS.keys()), b)
    # PAGEIND still skipped (unaffordable slice was a hard skip before too).
    assert {s["symbol"] for s in plan["skipped"]} == {"PAGEIND"}
    # Each affordable pick = floor(20000 / ltp), NO top-up.
    for s in ("CANFINHOME", "IDEA", "ADANIENT", "MCX"):
        assert plan["quantities"][s] == math.floor(20000.0 / _UTIL_LTPS[s])


def test_util_never_overdeploys_even_with_cheap_stock():
    # A very cheap stock (IDEA @ 8) could absorb the entire remainder — assert the
    # cap holds (deployed never exceeds budget).
    cfg = _cfg(total_allocated_capital=100_000.0, top_n_stocks=5,
               sizing_mode="equal", instrument_type="EQ", order_product="CNC",
               redistribute_unused_capital=True)
    alloc = CapitalAllocator(cfg)
    b = MockBroker(profile=None, ltps=dict(_UTIL_LTPS))
    plan = alloc.plan_quantities(list(_UTIL_LTPS.keys()), b)
    assert plan["deployed"] <= 100_000.0 + 1e-6
    assert plan["remainder"] >= -1e-6
    # remainder must be smaller than the cheapest affordable unit (IDEA @ 8).
    assert plan["remainder"] < 8.0 + 1e-6
