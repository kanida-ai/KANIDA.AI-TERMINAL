"""SPRINT CLUSTER 6 ITEM 3 — the NAKED-POSITION detector.

A REAL broker position (|qty|>0) that NO live session manages, but which we have
POSITIVE ownership evidence WE placed (a CLOSED row we own, or a broker order
carrying OUR tag / recorded order-id) → ONE urgent NAKED_POSITION page. A broker
position with NO ownership evidence is a manual/foreign holding → INVISIBLE.

MUTATION (verified): remove the alerts.send_urgent_deduped(...) call in
alert_monitor.detect_naked_positions → test_naked_closed_but_broker_holds finds no
NAKED_POSITION alert → fails.
"""
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade import alerts
from autotrade.config import TradingSessionConfig
from autotrade.monitoring import alert_monitor
from autotrade.order_ledger import compact_tag
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


class _FakeTransport:
    def __init__(self):
        self.calls = []

    def __call__(self, *, title, body, kind, severity):
        self.calls.append(kind)
        return {"sent": 1, "failed": 0}


@pytest.fixture(autouse=True)
def _transport_and_clock():
    alerts.set_transport(_FakeTransport())
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)
    alerts.set_transport(None)


def _count_naked():
    with falcon_conn() as con:
        return con.execute(
            "SELECT COUNT(*) AS n FROM autotrade_alerts WHERE kind='NAKED_POSITION'"
        ).fetchone()["n"]


def _mk(monkeypatch, net_book, orders=None):
    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False,
                          ltps={"NAKED": 100.0, "MANUAL": 100.0,
                                "MANAGED": 100.0, "ORPH": 100.0},
                          net_book=net_book, orders=orders)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="MIS")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _prof(sess):
    return sess.config.broker_profiles[0].profile_id


def test_naked_closed_but_broker_holds(clean_positions, monkeypatch):
    # DB shows CLOSED (we owned + closed it) but the broker STILL holds it → naked.
    net_book = {"NAKED": {"tradingsymbol": "NAKED", "product": "MIS",
                          "quantity": 100, "average_price": 100.0,
                          "exchange": "NSE"}}
    sess = _mk(monkeypatch, net_book)
    prof = _prof(sess)
    sess.registry.register(symbol="NAKED", broker_profile=prof, qty=100,
                           avg_price=100.0, product="MIS", instrument_type="EQ",
                           exchange="NSE")
    # Close it in our DB (we own it) — but the broker book above still holds 100.
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_positions SET status='CLOSED', qty=0 "
                    "WHERE session_id=? AND symbol='NAKED'", (sess.session_id,))
        con.commit()

    naked = alert_monitor.detect_naked_positions(sess)

    assert any(a["symbol"] == "NAKED" for a in naked)
    assert _count_naked() == 1
    # Idempotent within the dedup window.
    alert_monitor.detect_naked_positions(sess)
    assert _count_naked() == 1


def test_manual_holding_is_invisible(clean_positions, monkeypatch):
    # A broker position we have NO ownership evidence for = a manual/foreign
    # holding → INVISIBLE (no page), exactly as the P7 reconciler.
    net_book = {"MANUAL": {"tradingsymbol": "MANUAL", "product": "MIS",
                           "quantity": 100, "average_price": 100.0,
                           "exchange": "NSE"}}
    sess = _mk(monkeypatch, net_book)
    naked = alert_monitor.detect_naked_positions(sess)
    assert naked == []
    assert _count_naked() == 0


def test_managed_open_position_not_naked(clean_positions, monkeypatch):
    # An OPEN session position covering the broker holding is MANAGED → not naked.
    net_book = {"MANAGED": {"tradingsymbol": "MANAGED", "product": "MIS",
                            "quantity": 100, "average_price": 100.0,
                            "exchange": "NSE"}}
    sess = _mk(monkeypatch, net_book)
    prof = _prof(sess)
    sess.registry.register(symbol="MANAGED", broker_profile=prof, qty=100,
                           avg_price=100.0, product="MIS", instrument_type="EQ",
                           exchange="NSE")
    naked = alert_monitor.detect_naked_positions(sess)
    assert naked == []
    assert _count_naked() == 0


def test_naked_by_broker_order_tag(clean_positions, monkeypatch):
    # We placed ORPH (our client_order_id → our compact tag rides the broker order)
    # but lost the owning session row → the tag proves ownership → naked.
    coid = "FAL-orphan-ORPH-123"
    net_book = {"ORPH": {"tradingsymbol": "ORPH", "product": "MIS",
                         "quantity": 100, "average_price": 100.0,
                         "exchange": "NSE"}}
    orders = [{"order_id": "ord-orph", "tradingsymbol": "ORPH",
               "tag": compact_tag(coid), "status": "COMPLETE",
               "filled_quantity": 100, "transaction_type": "BUY"}]
    sess = _mk(monkeypatch, net_book, orders=orders)
    prof = _prof(sess)
    # A separate CLOSED row carrying that client_order_id makes the tag "ours"
    # (a different symbol, so it's the TAG — not the closed pair — that owns ORPH).
    sess.registry.register(symbol="OTHER", broker_profile=prof, qty=100,
                           avg_price=50.0, product="MIS", instrument_type="EQ",
                           exchange="NSE", client_order_id=coid)
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_positions SET status='CLOSED', qty=0 "
                    "WHERE session_id=? AND symbol='OTHER'", (sess.session_id,))
        con.commit()

    naked = alert_monitor.detect_naked_positions(sess)
    assert any(a["symbol"] == "ORPH" for a in naked)
    assert _count_naked() == 1


def test_paper_session_never_scans(clean_positions, monkeypatch):
    # A paper (dry_run) session must not page — byte-identical paper.
    net_book = {"NAKED": {"tradingsymbol": "NAKED", "product": "MIS",
                          "quantity": 100, "average_price": 100.0,
                          "exchange": "NSE"}}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=True, net_book=net_book)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", order_product="MIS")
    sess = TradingSession.create(cfg, mode="paper")
    sess._build_brokers()
    assert alert_monitor.detect_naked_positions(sess) == []
    assert _count_naked() == 0
