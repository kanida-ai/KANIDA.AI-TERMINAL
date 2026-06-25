"""Order construction, LIMIT pricing, async retry, partial-fill registry."""
import asyncio

import pytest

from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.execution.orders import (build_order, place_order_with_retry,
                                        OrderTimeoutError, Order)
from autotrade.monitoring.registry import PositionRegistry
from tests.autotrade.mock_broker import MockBroker


def _cfg(**kw):
    base = dict(total_allocated_capital=500000.0, instrument_type="EQ")
    base.update(kw)
    return TradingSessionConfig(**base)


def test_build_eq_order():
    b = MockBroker(profile=None, ltps={"A": 100})
    o = build_order("A", 10, _cfg(order_product="CNC", order_type="MARKET"), b)
    assert o.symbol == "A" and o.qty == 10 and o.product == "CNC"


def test_build_mtf_order():
    b = MockBroker(profile=None, ltps={"A": 100})
    o = build_order("A", 10, _cfg(instrument_type="MTF"), b)
    assert o.product == "MTF"


def test_build_fut_order_lot_rounded():
    b = MockBroker(profile=None, ltps={"A": 100}, lot_size=50)
    o = build_order("A", 137, _cfg(instrument_type="FUT"), b)
    assert o.qty == 100  # floor(137/50)=2 lots * 50
    assert o.exchange == "NFO" and o.product == "NRML"


def test_limit_price_offset():
    o = Order(symbol="A", qty=10, order_type="LIMIT", limit_offset_pct=0.001)
    assert o.compute_limit_price(1000.0) == 1001.0


def test_async_place_dry_run_safe():
    # MockBroker dry_run=True → place_order still returns PLACED in mock; the
    # safety is on the REAL ZerodhaBroker. Here we just exercise the retry path.
    b = MockBroker(profile=None, ltps={"A": 100}, dry_run=True)
    o = Order(symbol="A", qty=10)
    res = asyncio.run(place_order_with_retry(o, b))
    assert res.status in ("PLACED", "DRY_RUN")


def test_async_place_timeout_raises():
    class SlowBroker(MockBroker):
        async def place_order(self, order):
            await asyncio.sleep(0.3)
            return await super().place_order(order)
    b = SlowBroker(profile=None, ltps={"A": 100})
    o = Order(symbol="A", qty=10)
    with pytest.raises(OrderTimeoutError):
        asyncio.run(place_order_with_retry(o, b, max_retries=2, timeout_ms=50))


# ── Parity check #3: partial fill 80/100 registers as 80 ─────────────────────

def test_partial_fill_registers_80_not_0_or_100(clean_positions):
    sid = "sess-partial"
    reg = PositionRegistry(sid, 500000.0)
    reg.register_partial("PART", "zer", 80, 100.0)
    rows = reg.get_open_positions()
    assert len(rows) == 1
    assert rows[0]["qty"] == 80
