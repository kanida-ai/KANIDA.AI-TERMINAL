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
