"""CLUSTER 3 ITEM 2 — reconcile-health gating of the sub-second fast path.

The ws stale-basket guard must NOT treat a basket as reconcile-validated when the
broker→DB reconcile NO-OP'd because the broker book was None/unreachable (an
outage). A stale/failed reconcile BLOCKS the fast-path fire (ws defers); only a
HEALTHY reconcile (a successful net-book read for every relevant profile) stamps
the basket as validated.

MUTATION (verified): in basket_gen.basket_reconcile_validated remove the
`if healthy is False: return False` gate (stamp/act regardless of health) → after
an unreachable-book reconcile the ws FIRES on the unvalidated basket →
`broker.exits == []` fails.
"""
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from autotrade.monitoring import basket_gen, fire_guard
from autotrade.monitoring.position_reconciler import reconcile_broker_positions
from autotrade.monitoring.ws_driver import _WSDriver
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _mk(monkeypatch, net_book):
    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False,
                          ltps={"A": 90.0, "B": 90.0, "C": 90.0},
                          net_book=net_book)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.02, kill_switch_direction="both",
                               order_product="MIS")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    for sym in ("A", "B", "C"):
        sess.registry.register(symbol=sym, broker_profile=prof, qty=100,
                               avg_price=100.0, product="MIS",
                               instrument_type="EQ", exchange="NSE")
        sess.registry.update_ltp(sym, 90.0, broker_profile=prof)   # -10% loss
    sess.monitor.freeze_invested_basis()
    basket_gen.reset(sess.session_id)
    return sess


def _status(sess):
    with falcon_conn() as con:
        r = con.execute("SELECT status FROM autotrade_sessions WHERE session_id=?",
                        (sess.session_id,)).fetchone()
    return r["status"] if r else None


def test_unhealthy_reconcile_blocks_ws_fire(clean_positions, monkeypatch):
    # net_book None → broker book unreachable → reconcile is a NO-OP and UNHEALTHY.
    sess = _mk(monkeypatch, net_book=None)
    actions = reconcile_broker_positions(sess)
    assert actions == []                                    # no-op reconcile
    assert basket_gen.reconcile_healthy(sess.session_id) is False
    # The basket is NOT reconcile-validated (unhealthy) → the fast path must defer.
    assert basket_gen.basket_reconcile_validated(
        sess.session_id, sess.registry.get_open_positions()) is False

    drv = _WSDriver(sess.session_id, 0.1, ltp_source=lambda s: 90.0)
    drv._tick_once(sess, fire_guard)

    assert _status(sess) != "CLOSED"
    broker = next(iter(sess.brokers.values()))
    assert broker.exits == []


def test_healthy_reconcile_validates_and_ws_fires(clean_positions, monkeypatch):
    # A readable, in-sync net book (MIS held == db) → reconcile HEALTHY, no mutation.
    net_book = {s: {"tradingsymbol": s, "product": "MIS", "quantity": 100,
                    "average_price": 100.0, "exchange": "NSE"}
                for s in ("A", "B", "C")}
    sess = _mk(monkeypatch, net_book=net_book)
    actions = reconcile_broker_positions(sess)
    assert actions == []                                    # in-sync, no mutation
    assert basket_gen.reconcile_healthy(sess.session_id) is True
    assert basket_gen.last_successful_reconcile_ts(sess.session_id) is not None
    # Stamp the validated set (as session.tick does on a healthy reconcile).
    basket_gen.stamp_reconciled(
        sess.session_id, sess.registry.get_open_positions())
    assert basket_gen.basket_reconcile_validated(
        sess.session_id, sess.registry.get_open_positions()) is True

    drv = _WSDriver(sess.session_id, 0.1, ltp_source=lambda s: 90.0)
    drv._tick_once(sess, fire_guard)

    assert _status(sess) == "CLOSED"
