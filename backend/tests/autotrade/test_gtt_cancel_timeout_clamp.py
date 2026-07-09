"""R3 — a GTT cancel that TIMES OUT (OCO possibly live) must not let the market
exit oversell. The exit qty is clamped to the SESSION-SCOPED live-held qty so a
GTT firing concurrently can never make us sell more than we still hold.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now, _exit_single_position
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


class _SlowGTTBroker(MockBroker):
    def cancel_gtt(self, gtt_id):
        time.sleep(0.4)          # forces the (shrunk) cancel timeout to trip
        return {"status": "ok"}


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _mk(monkeypatch, net_positions):
    def fake_build_client(profile, dry_run=True):
        return _SlowGTTBroker(profile=profile, dry_run=False,
                              ltps={"A": 96.0}, net_positions=net_positions)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    # Shrink the cancel timeout so the test doesn't wait 5s.
    monkeypatch.setattr(sess_mod, "_GTT_CANCEL_TIMEOUT_SEC", 0.1)
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _register(sess, qty):
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="A", broker_profile=prof, qty=qty,
                           avg_price=100.0, product="CNC", instrument_type="EQ",
                           exchange="NSE")
    sess.registry.update_ltp("A", 96.0, broker_profile=prof)
    sess.registry.set_gtt("A", "G-A", broker_profile=prof)
    return prof


def test_clamps_exit_qty_when_gtt_partly_filled(clean_positions, monkeypatch):
    """GTT fired mid-exit and sold 60 of our 100 (broker net now 40). The cancel
    times out → clamp the market exit to 40, never 100 (no oversell / naked)."""
    sess = _mk(monkeypatch, {"A": 40})
    _register(sess, 100)
    pos = sess.registry.get_open_positions()[0]

    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    broker = next(iter(sess.brokers.values()))
    # Exactly ONE exit, for the CLAMPED 40 — never the tracked 100.
    assert broker.exits == [("A", 40)]
    assert res["status"] in ("COMPLETE", "EXITED", "RECONCILED_FLAT")


def test_no_clamp_when_broker_fully_covers(clean_positions, monkeypatch):
    """Cancel times out but the broker still holds our full 100 (GTT didn't fill)
    → no clamp, the exit is the full 100."""
    sess = _mk(monkeypatch, {"A": 100})
    _register(sess, 100)
    pos = sess.registry.get_open_positions()[0]

    asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    broker = next(iter(sess.brokers.values()))
    assert broker.exits == [("A", 100)]
