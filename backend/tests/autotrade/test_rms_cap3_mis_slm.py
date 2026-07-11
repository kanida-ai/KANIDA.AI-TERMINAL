"""RMS CAP 3 — MIS broker-side protective SL-M (stop-market).

Proves:
  * An MIS leg (no GTT) gets a Falcon-tagged broker-side DAY SL-M at the
    capital-basis stop trigger — a SHORT places a BUY-to-cover SL-M ABOVE entry.
  * The SL-M is cancelled on the flatten (kill switch) so no orphan stop remains.
  * A CARRIED (CNC) leg does NOT get an SL-M (it carries a GTT instead).
  * The flag mis_protective_slm_enabled=False disables it (additive default-off).

MUTATION-VERIFIED (test_mis_short_places_protective_slm):
  Revert = in gtt_manager.place_for_position's MIS-suppressed branch, delete the
      slm_id = self._place_protective_slm(pos, stop_trig)
  line (record levels only, as before). Then no SL-M is placed → this test's
  assert len(broker.slm_orders) == 1 fails.
"""
import asyncio

import pytest

from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.gtt_manager import GTTManager
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from tests.autotrade.mock_broker import MockBroker


def _sid():
    import uuid
    return uuid.uuid4().hex


def _mis_short_cfg(slm_enabled=True):
    return TradingSessionConfig(
        total_allocated_capital=10000.0, top_n_stocks=3, sizing_mode="equal",
        order_product="MIS", instrument_type="EQ", direction="short",
        per_position_stop_pct=0.08, per_position_target_pct=0.20,
        mis_protective_slm_enabled=slm_enabled)


def _register(sid, symbol, qty, avg, ltp, direction="short",
              instrument_type="EQ", product="MIS"):
    reg = PositionRegistry(sid, 0)
    reg.register(symbol=symbol, broker_profile="zer", qty=qty, avg_price=avg,
                 product=product, instrument_type=instrument_type,
                 direction=direction, client_order_id="FAL-" + symbol)
    reg.update_ltp(symbol, ltp)
    return reg


def test_mis_short_places_protective_slm(clean_positions):
    sid = _sid()
    cfg = _mis_short_cfg(slm_enabled=True)
    reg = _register(sid, "SHORTY", 100, 100.0, 100.0)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False)
    gm = GTTManager(sid, cfg, {"zer": broker}, reg)
    pos = reg.get_open_positions()[0]
    res = gm.place_for_position(pos)
    assert res["status"] == "SUPPRESSED_INTRADAY"    # MIS → no GTT
    # A single broker-side SL-M was placed as a BUY-to-cover ABOVE entry.
    assert len(broker.slm_orders) == 1
    o = broker.slm_orders[0]
    assert o["symbol"] == "SHORTY"
    assert o["direction"] == "short"                 # adapter → BUY-to-cover
    assert o["trigger_price"] == pytest.approx(108.0)  # entry 100 * (1 + 0.08)
    assert o["client_tag"] is not None               # Falcon-owned tag
    # Persisted on the row.
    pos2 = reg.get_open_positions()[0]
    assert pos2["slm_order_id"] == "slm-SHORTY"


def test_slm_flag_off_disables_placement(clean_positions):
    sid = _sid()
    cfg = _mis_short_cfg(slm_enabled=False)
    reg = _register(sid, "SHORTY", 100, 100.0, 100.0)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False)
    gm = GTTManager(sid, cfg, {"zer": broker}, reg)
    gm.place_for_position(reg.get_open_positions()[0])
    assert broker.slm_orders == []                   # flag off → no SL-M


def test_slm_idempotent_not_replaced(clean_positions):
    sid = _sid()
    cfg = _mis_short_cfg(slm_enabled=True)
    reg = _register(sid, "SHORTY", 100, 100.0, 100.0)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False)
    gm = GTTManager(sid, cfg, {"zer": broker}, reg)
    gm.place_for_position(reg.get_open_positions()[0])
    gm.place_for_position(reg.get_open_positions()[0])   # already protected
    assert len(broker.slm_orders) == 1                  # not re-placed


def test_slm_cancelled_on_flatten(clean_positions):
    sid = _sid()
    cfg = _mis_short_cfg(slm_enabled=True)
    reg = _register(sid, "SHORTY", 100, 100.0, 100.0)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False)
    gm = GTTManager(sid, cfg, {"zer": broker}, reg)
    gm.place_for_position(reg.get_open_positions()[0])
    assert broker.slm_orders                              # placed
    # Kill switch flatten must cancel the SL-M (no orphan stop).
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg, gtt_manager=gm)
    asyncio.run(ks.fire("TEST"))
    assert "slm-SHORTY" in broker.slm_cancelled


def test_cnc_leg_gets_no_slm(clean_positions):
    """A carried CNC leg carries a GTT, not an SL-M (SL-M is MIS-only)."""
    sid = _sid()
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, top_n_stocks=3, sizing_mode="equal",
        order_product="CNC", instrument_type="EQ",
        per_position_stop_pct=0.08, per_position_target_pct=0.20)
    reg = _register(sid, "LONGY", 100, 100.0, 100.0, direction="long",
                    product="CNC")
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False)
    gm = GTTManager(sid, cfg, {"zer": broker}, reg)
    res = gm.place_for_position(reg.get_open_positions()[0])
    assert res["status"] != "SUPPRESSED_INTRADAY"    # CNC → GTT path
    assert broker.slm_orders == []
