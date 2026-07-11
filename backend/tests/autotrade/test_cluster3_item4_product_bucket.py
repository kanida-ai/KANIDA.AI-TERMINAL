"""CLUSTER 3 ITEM 4 — reconciliation product SOURCE OF TRUTH = the PERSISTED
autotrade_positions.product, not the single session-level order_product.

A position whose product differs from the session default (a session whose
broker_profiles mix products) must reconcile under ITS OWN product bucket.

MUTATION (verified): revert position_reconciler to bucket by _session_product
(the session config order_product) instead of _position_product → the MTF leg
buckets under the session's CNC → the MTF net-book row no longer matches → no
sell evidence in the CNC bucket → the leg is NOT closed (UNATTRIBUTED_CLOSE,
stays OPEN) → this test's `status == CLOSED` assertion fails.
"""
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from autotrade.monitoring.position_reconciler import reconcile_broker_positions
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _mk(monkeypatch, net_book, *, order_product="CNC"):
    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps={"X": 100.0},
                          net_book=net_book)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               kill_switch_pct=0.02, order_product=order_product)
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _status(sess, sym):
    with falcon_conn() as con:
        r = con.execute("SELECT status FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (sess.session_id, sym)).fetchone()
    return r["status"] if r else None


def test_position_reconciles_under_its_own_product(clean_positions, monkeypatch):
    # Session default is CNC, but THIS leg was opened MTF. The broker's day book
    # shows the MTF leg round-tripped to flat (buy 100 + sell 100, net 0) with a
    # real sell price → provable external close FOR THE MTF BUCKET.
    net_book = {"X": {"tradingsymbol": "X", "product": "MTF", "quantity": 0,
                      "buy_quantity": 100, "sell_quantity": 100,
                      "sell_price": 105.0, "average_price": 100.0,
                      "exchange": "NSE"}}
    sess = _mk(monkeypatch, net_book, order_product="CNC")
    prof = sess.config.broker_profiles[0].profile_id
    # Persist the leg under MTF (≠ the session's CNC default).
    sess.registry.register(symbol="X", broker_profile=prof, qty=100,
                           avg_price=100.0, product="MTF",
                           instrument_type="EQ", exchange="NSE")
    sess.registry.update_ltp("X", 100.0, broker_profile=prof)
    sess.monitor.freeze_invested_basis()

    actions = reconcile_broker_positions(sess)

    # Bucketed under MTF (its own product) → the MTF row matches → flat close.
    assert any(a.get("action") == "CLOSED_EXTERNAL_FLAT" for a in actions), actions
    assert _status(sess, "X") == "CLOSED"


def test_matching_product_still_reconciles(clean_positions, monkeypatch):
    """Regression: when the leg's product EQUALS the session default the bucket is
    unchanged (a CNC leg under a CNC session still reconciles)."""
    net_book = {"X": {"tradingsymbol": "X", "product": "MTF", "quantity": 0,
                      "buy_quantity": 100, "sell_quantity": 100,
                      "sell_price": 105.0, "average_price": 100.0,
                      "exchange": "NSE"}}
    sess = _mk(monkeypatch, net_book, order_product="MTF")
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="X", broker_profile=prof, qty=100,
                           avg_price=100.0, product="MTF",
                           instrument_type="EQ", exchange="NSE")
    sess.registry.update_ltp("X", 100.0, broker_profile=prof)
    sess.monitor.freeze_invested_basis()

    actions = reconcile_broker_positions(sess)
    assert any(a.get("action") == "CLOSED_EXTERNAL_FLAT" for a in actions), actions
    assert _status(sess, "X") == "CLOSED"
