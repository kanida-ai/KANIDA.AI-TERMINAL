"""R4 — the ws_driver defers a FIRE when its basket diverged from the last tick
reconcile-validated set (a leg closed at the broker but the DB isn't reconciled
yet). It still updates marks; the next 5s tick reconciles then fires.
"""
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from autotrade.monitoring import basket_gen, fire_guard
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


def _mk(monkeypatch):
    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False,
                          ltps={"A": 90.0, "B": 90.0, "C": 90.0})

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.02, kill_switch_direction="both",
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    for sym in ("A", "B", "C"):
        sess.registry.register(symbol=sym, broker_profile=prof, qty=100,
                               avg_price=100.0, product="CNC",
                               instrument_type="EQ", exchange="NSE")
        sess.registry.update_ltp(sym, 90.0, broker_profile=prof)   # -10% loss
    sess.monitor.freeze_invested_basis()
    return sess


def _status(sess):
    with falcon_conn() as con:
        r = con.execute("SELECT status FROM autotrade_sessions WHERE session_id=?",
                        (sess.session_id,)).fetchone()
    return r["status"] if r else None


def test_ws_defers_fire_when_basket_not_validated(clean_positions, monkeypatch):
    sess = _mk(monkeypatch)
    # The last reconcile validated only {A,B} — the DB shows {A,B,C}, so C is an
    # unvalidated (possibly phantom) leg. gr is deeply negative (crosses -2%).
    basket_gen.stamp_reconciled(sess.session_id, [
        {"symbol": "A", "qty": 100}, {"symbol": "B", "qty": 100}])

    drv = _WSDriver(sess.session_id, 0.1, ltp_source=lambda s: 90.0)
    drv._tick_once(sess, fire_guard)

    # No kill fired — the ws deferred to the next tick.
    assert _status(sess) != "CLOSED"
    broker = next(iter(sess.brokers.values()))
    assert broker.exits == []


def test_ws_fires_when_basket_validated(clean_positions, monkeypatch):
    sess = _mk(monkeypatch)
    # Stamp matches the current DB open set → the basket is validated → fire.
    basket_gen.stamp_reconciled(sess.session_id, sess.registry.get_open_positions())

    drv = _WSDriver(sess.session_id, 0.1, ltp_source=lambda s: 90.0)
    drv._tick_once(sess, fire_guard)

    assert _status(sess) == "CLOSED"
    broker = next(iter(sess.brokers.values()))
    assert sorted(s for s, _q in broker.exits) == ["A", "B", "C"]


def test_ws_fresh_session_no_stamp_still_fires(clean_positions, monkeypatch):
    """No reconcile stamp yet (fresh) → validated=True → the fast kill is NOT
    delayed (no regression of the sub-second path)."""
    sess = _mk(monkeypatch)
    basket_gen.reset(sess.session_id)   # ensure no stamp

    drv = _WSDriver(sess.session_id, 0.1, ltp_source=lambda s: 90.0)
    drv._tick_once(sess, fire_guard)

    assert _status(sess) == "CLOSED"
