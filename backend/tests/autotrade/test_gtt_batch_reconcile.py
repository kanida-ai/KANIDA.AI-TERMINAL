"""P1(b) — reconcile_gtt_fills resolves N positions from ONE get_gtts_map() call
instead of N per-position get_gtt round-trips. A gtt_id present in the map skips
the single get_gtt; the close semantics are unchanged.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


class _MapGTTBroker(MockBroker):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.map_calls = 0
        self.single_get_gtt_calls = 0

    def get_gtts_map(self):
        self.map_calls += 1
        if self._gtts is None:
            return None
        return {str(k): v for k, v in self._gtts.items()}

    def get_gtt(self, gtt_id):
        self.single_get_gtt_calls += 1
        return super().get_gtt(gtt_id)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def test_reconcile_uses_batched_gtt_map(clean_positions, monkeypatch):
    gtts = {"G-A": {"status": "triggered",
                    "orders": [{"result": {"order_id": "O-A"}}]},
            "G-B": {"status": "triggered",
                    "orders": [{"result": {"order_id": "O-B"}}]}}
    order_status = {"O-A": {"status": "COMPLETE", "filled_quantity": 10,
                            "average_price": 96.0},
                    "O-B": {"status": "COMPLETE", "filled_quantity": 10,
                            "average_price": 48.0}}
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = _MapGTTBroker(profile=profile, dry_run=False,
                           ltps={"A": 96.0, "B": 48.0},
                           gtts=gtts, order_status=order_status)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)

    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="MIS")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    for sym, gid in (("A", "G-A"), ("B", "G-B")):
        sess.registry.register(symbol=sym, broker_profile=prof, qty=10,
                               avg_price=100.0, product="MIS",
                               instrument_type="EQ", exchange="NSE")
        sess.registry.set_gtt(sym, gid, broker_profile=prof)
    sess.monitor.freeze_invested_basis()

    out = asyncio.run(sess.gtt_manager.reconcile_gtt_fills())

    closed = {o["symbol"] for o in out if o.get("status") == "CLOSED_GTT"}
    assert closed == {"A", "B"}
    broker = created[prof]
    # The batch map was used; the single get_gtt was NOT called for present ids.
    assert broker.map_calls == 1
    assert broker.single_get_gtt_calls == 0
    with falcon_conn() as con:
        for sym in ("A", "B"):
            r = con.execute("SELECT status FROM autotrade_positions "
                            "WHERE session_id=? AND symbol=?",
                            (sess.session_id, sym)).fetchone()
            assert r["status"] == "CLOSED"
