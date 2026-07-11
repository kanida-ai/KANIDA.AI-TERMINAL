"""CLUSTER 3 ITEM 3(b) — durable, idempotent QUERY-BEFORE-PLACE on retry / resume.

Before placing on a retry / restart-resume, the exit path queries the broker
orderbook for OUR client tag (compact_tag of the position's persisted
exit_client_order_id). If a tagged order already exists (OPEN or COMPLETE) it is
OUR own earlier placement surviving the crash → ADOPT it and place ZERO new
orders. This closes the cross-process exactly-once window the in-process
single-flight lock cannot.

MUTATION (verified): remove the ITEM-3(b) adopt block in
session._exit_single_position_inner (the `if exit_coid: ... adopt_tagged_...`
branch) → the retry/resume PLACES a fresh market exit → `broker.exits == []`
fails (a duplicate order was placed).
"""
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade import order_ledger
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now, _exit_single_position
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _mk(monkeypatch):
    holder = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps={"A": 99.0})
        holder["broker"] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.02, order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess, holder["broker"]


def _pos_row(session_id, sym):
    with falcon_conn() as con:
        r = con.execute("SELECT * FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (session_id, sym)).fetchone()
    return dict(r) if r else None


import asyncio


def test_retry_adopts_existing_tagged_exit_places_zero(clean_positions, monkeypatch):
    sess, broker = _mk(monkeypatch)
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="A", broker_profile=prof, qty=100,
                           avg_price=100.0, product="CNC",
                           instrument_type="EQ", exchange="NSE")
    sess.registry.update_ltp("A", 99.0, broker_profile=prof)
    sess.monitor.freeze_invested_basis()

    # A prior exit attempt minted + persisted this exit client_order_id, and its
    # order (tag = compact_tag) is ALREADY at the broker, COMPLETE — but our DB
    # never recorded the close (crash). The retry/resume must ADOPT it.
    coid = order_ledger.make_client_order_id(sess.session_id, "A", attempt=1)
    sess.registry.set_exit_client_order_id("A", coid, broker_profile=prof)
    tag = order_ledger.compact_tag(coid)
    broker._orders = [{
        "order_id": "ADOPT-A", "status": "COMPLETE", "filled_quantity": 100,
        "average_price": 98.5, "transaction_type": "SELL",
        "tradingsymbol": "A", "tag": tag,
    }]
    broker._order_status_map = {
        "ADOPT-A": {"status": "COMPLETE", "filled_quantity": 100,
                    "average_price": 98.5}}

    pos = _pos_row(sess.session_id, "A")
    assert pos["exit_client_order_id"] == coid

    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="EXIT_RETRY",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    # ADOPTED — placed ZERO new orders.
    assert res.get("adopted") is True, res
    assert broker.exits == [], broker.exits
    assert _pos_row(sess.session_id, "A")["status"] == "CLOSED"


def test_first_exit_mints_and_persists_stable_tag(clean_positions, monkeypatch):
    """No tagged order at the broker yet (first exit): mint + PERSIST the exit
    client_order_id, place ONE order carrying its tag (so a future retry/resume can
    recognise + adopt it). get_orders() empty → no adoption."""
    sess, broker = _mk(monkeypatch)
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="A", broker_profile=prof, qty=100,
                           avg_price=100.0, product="CNC",
                           instrument_type="EQ", exchange="NSE")
    sess.registry.update_ltp("A", 99.0, broker_profile=prof)
    sess.monitor.freeze_invested_basis()
    broker._orders = []      # nothing of ours resting yet

    pos = _pos_row(sess.session_id, "A")
    assert pos["exit_client_order_id"] is None

    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    # Placed exactly one order carrying our stable tag; id persisted for a retry.
    assert len(broker.exits) == 1
    persisted = _pos_row(sess.session_id, "A")["exit_client_order_id"]
    assert persisted is not None
    # The placed exit carried our client_order_id (stable tag source).
    assert broker.exit_calls[0]["client_order_id"] == persisted
