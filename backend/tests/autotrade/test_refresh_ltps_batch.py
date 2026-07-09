"""P1(a) — refresh_ltps uses ONE batched LTP call per broker profile (not a
per-symbol broker round-trip). Byte-identical marks vs the per-symbol path.
"""
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


class _CountingBroker(MockBroker):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.batch_calls = []
        self.get_ltp_calls = 0

    def get_ltps_batch(self, symbols):
        self.batch_calls.append(list(symbols))
        return super().get_ltps_batch(symbols)

    def get_ltp(self, symbol):
        self.get_ltp_calls += 1
        return super().get_ltp(symbol)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _mk(monkeypatch):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = _CountingBroker(profile=profile, dry_run=False,
                             ltps={"A": 101.0, "B": 202.0, "C": 51.0})
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess, created


def test_one_batch_call_per_profile(clean_positions, monkeypatch):
    sess, created = _mk(monkeypatch)
    prof = sess.config.broker_profiles[0].profile_id
    for sym, avg in (("A", 100.0), ("B", 200.0), ("C", 50.0)):
        sess.registry.register(symbol=sym, broker_profile=prof, qty=10,
                               avg_price=avg, product="CNC",
                               instrument_type="EQ", exchange="NSE")

    n = sess.monitor.refresh_ltps(sess.brokers)
    assert n == 3

    broker = created[prof]
    # Exactly ONE batched call covering all three symbols.
    assert len(broker.batch_calls) == 1
    assert sorted(broker.batch_calls[0]) == ["A", "B", "C"]

    # Marks persisted correctly (byte-identical to the per-symbol path).
    with falcon_conn() as con:
        marks = {r["symbol"]: r["ltp"] for r in con.execute(
            "SELECT symbol, ltp FROM autotrade_positions WHERE session_id=?",
            (sess.session_id,)).fetchall()}
    assert marks == {"A": 101.0, "B": 202.0, "C": 51.0}
