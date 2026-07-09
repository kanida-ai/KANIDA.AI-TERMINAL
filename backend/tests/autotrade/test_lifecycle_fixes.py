"""Lifecycle panel/P&L fixes: #7 snapshot incl. realised, #8 mark as-of,
#9 partial-entry by fill_qty, #10 reconcile-flat excluded from win/loss.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from autotrade.api.pnl_summary import _bucket_stats
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from autotrade.broker.base import OrderResult
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _patch(monkeypatch, factory):
    def fake_build_client(profile, dry_run=True):
        return factory(profile, dry_run)
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)


# ── Lifecycle#7 — snapshot gross includes realised ────────────────────────────
def test_snapshot_gross_includes_realised(clean_positions, monkeypatch):
    _patch(monkeypatch, lambda p, d: MockBroker(profile=p, dry_run=False,
                                                ltps={"A": 100.0, "B": 90.0}))
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    for sym in ("A", "B"):
        sess.registry.register(symbol=sym, broker_profile=prof, qty=100,
                               avg_price=100.0, product="CNC",
                               instrument_type="EQ", exchange="NSE")
        sess.registry.update_ltp(sym, 100.0 if sym == "A" else 90.0,
                                 broker_profile=prof)
    sess.monitor.freeze_invested_basis()
    # Close B at 90 → realised -1000.
    sess.registry.mark_closed("B", "STOP_STOCK", exit_price=90.0,
                              broker_profile=prof)

    snap = sess.monitor.snapshot()
    # Snapshot gr must equal the live panel's on-fund gross (which includes
    # realised), not the uPnL-only figure.
    assert snap["gross_return"] == pytest.approx(sess.monitor.compute_gross_return())
    assert snap["gross_return"] == pytest.approx(-1000.0 / 100000.0)
    with falcon_conn() as con:
        lgr = con.execute("SELECT last_gross_return FROM autotrade_sessions "
                          "WHERE session_id=?", (sess.session_id,)).fetchone()[0]
    assert lgr == pytest.approx(-1000.0 / 100000.0)


# ── Lifecycle#8 — mark as-of stamped + surfaced ───────────────────────────────
def test_mark_as_of_stamped_and_surfaced(clean_positions, monkeypatch):
    _patch(monkeypatch, lambda p, d: MockBroker(profile=p, dry_run=False,
                                                ltps={"A": 101.0}))
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="A", broker_profile=prof, qty=10,
                           avg_price=100.0, product="CNC", instrument_type="EQ")
    sess.monitor.refresh_ltps(sess.brokers)

    with falcon_conn() as con:
        r = con.execute("SELECT ltp_as_of FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (sess.session_id, "A")).fetchone()
    assert r["ltp_as_of"] is not None
    st = sess.status()
    assert "oldest_mark_age_ms" in st
    assert isinstance(st["oldest_mark_age_ms"], int)


# ── Lifecycle#9 — partial ENTRY classified by fill_qty < ordered ──────────────
class _UnderFillBroker(MockBroker):
    def __init__(self, *a, underfill=None, **k):
        super().__init__(*a, **k)
        self._underfill = underfill or {}

    async def place_order(self, order):
        self.placed.append(order)
        if order.symbol in self._underfill:
            return OrderResult(status="PLACED",
                               broker_order_id="ord-" + order.symbol,
                               symbol=order.symbol, qty=order.qty,
                               filled_qty=self._underfill[order.symbol],
                               avg_price=self.ltps.get(order.symbol))
        return await super().place_order(order)


def test_entry_under_fill_flagged_partial(clean_positions, monkeypatch):
    seed_signals([("A", 1, 9.0, 100.0)])
    _patch(monkeypatch, lambda p, d: _UnderFillBroker(
        profile=p, dry_run=False, ltps={"A": 100.0}, underfill={"A": 3}))
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    a = next(o for o in res["orders"] if o["symbol"] == "A")
    # Broker said PLACED (not PARTIAL) but filled fewer than ordered → flagged.
    assert a["status"] == "PARTIAL"
    assert a["qty"] == 3
    assert a["ordered_qty"] > 3


# ── Lifecycle#10 — reconcile-flat excluded from charge-bearing win/loss ────────
def test_reconcile_flat_excluded_from_win_loss():
    trades = [
        {"gross": 100.0, "charges": 5.0, "net": 95.0, "reconcile_flat": False},
        {"gross": -50.0, "charges": 5.0, "net": -55.0, "reconcile_flat": False},
        {"gross": 0.0, "charges": 0.0, "net": 0.0, "reconcile_flat": True},
    ]
    s = _bucket_stats(trades)
    assert s["trades"] == 3          # total closed rows still reported
    assert s["wins"] == 1
    assert s["losses"] == 1          # the reconcile-flat row is NOT a phantom loss
    assert s["win_rate"] == pytest.approx(50.0)
