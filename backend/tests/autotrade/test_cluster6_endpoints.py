"""SPRINT CLUSTER 6 ITEM 4 — observability endpoints (alerts feed + ack + health).

Contract + tenant isolation + ack persistence. Endpoints are called directly with
a resolved Caller (the same pattern as test_tenant_isolation).
"""
import pytest
from fastapi import HTTPException

from autotrade import alerts
from autotrade.api.autotrade_routes import (Caller, alerts_list, alert_ack,
                                            health)
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession
from falcon.db import falcon_conn


class _FakeTransport:
    def __call__(self, *, title, body, kind, severity):
        return {"sent": 0, "failed": 0, "skipped": True}


@pytest.fixture(autouse=True)
def _transport():
    alerts.set_transport(_FakeTransport())
    yield
    alerts.set_transport(None)


ADMIN = Caller(user_id=None, is_admin=True, authenticated=False)


def _session(user_id=None, status=None):
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=3)
    sess = TradingSession.create(cfg, mode="paper", user_id=user_id)
    if status:
        with falcon_conn() as con:
            con.execute("UPDATE autotrade_sessions SET status=? WHERE session_id=?",
                        (status, sess.session_id))
            con.commit()
    return sess


def test_alerts_list_returns_persisted_and_ack_flips_flag(clean_positions):
    sess = _session(user_id=None)
    aid = alerts.send_urgent("boom", kind="EXIT_FAILED",
                             session_id=sess.session_id, symbol="Z")
    out = alerts_list(caller=ADMIN)
    row = next((a for a in out["alerts"] if a["id"] == aid), None)
    assert row is not None
    assert row["kind"] == "EXIT_FAILED"
    assert row["acknowledged"] == 0
    assert "incident_id" in row          # contract exposes the dedup key

    res = alert_ack(aid, caller=ADMIN)
    assert res["acked"] is True

    out2 = alerts_list(caller=ADMIN)
    row2 = next(a for a in out2["alerts"] if a["id"] == aid)
    assert row2["acknowledged"] == 1


def test_alerts_list_unacked_only_filter(clean_positions):
    sess = _session(user_id=None)
    a1 = alerts.send_urgent("a", kind="EXIT_FAILED",
                            session_id=sess.session_id, symbol="P")
    a2 = alerts.send_urgent("b", kind="EXIT_FAILED",
                            session_id=sess.session_id, symbol="Q")
    alerts.acknowledge(a1)
    unacked = alerts_list(unacked_only=True, caller=ADMIN)["alerts"]
    ids = [a["id"] for a in unacked]
    assert a2 in ids
    assert a1 not in ids


def test_alerts_list_tenant_scoped(clean_positions):
    s2 = _session(user_id="2")
    aid = alerts.send_urgent("x", kind="EXIT_FAILED",
                             session_id=s2.session_id, symbol="Z")
    u2 = Caller(user_id="2", is_admin=False, authenticated=True)
    u3 = Caller(user_id="3", is_admin=False, authenticated=True)
    assert any(a["id"] == aid for a in alerts_list(caller=u2)["alerts"])
    # user 3 does NOT own s2 → cannot see its alerts.
    assert all(a["id"] != aid for a in alerts_list(caller=u3)["alerts"])


def test_ack_nonowner_404(clean_positions):
    s2 = _session(user_id="2")
    aid = alerts.send_urgent("x", kind="EXIT_FAILED",
                             session_id=s2.session_id, symbol="Z")
    with pytest.raises(HTTPException) as e:
        alert_ack(aid, caller=Caller(user_id="3", is_admin=False,
                                     authenticated=True))
    assert e.value.status_code == 404


def test_ack_unknown_id_404(clean_positions):
    with pytest.raises(HTTPException) as e:
        alert_ack(9_999_999, caller=ADMIN)
    assert e.value.status_code == 404


def test_health_surface_gauges(clean_positions):
    sess = _session(user_id=None, status="RUNNING")
    prof = "p1"
    sess.registry.register(symbol="OPN", broker_profile=prof, qty=10,
                           avg_price=100.0, product="CNC", instrument_type="EQ",
                           exchange="NSE")
    sess.registry.register(symbol="FAIL", broker_profile=prof, qty=10,
                           avg_price=100.0, product="CNC", instrument_type="EQ",
                           exchange="NSE")
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_positions SET status='EXIT_FAILED' "
                    "WHERE session_id=? AND symbol='FAIL'", (sess.session_id,))
        con.commit()

    out = health(caller=ADMIN)
    assert "sessions" in out and "breaker" in out and "as_of" in out
    row = next((s for s in out["sessions"]
                if s["session_id"] == sess.session_id), None)
    assert row is not None
    assert row["status"] == "RUNNING"
    assert row["n_open"] == 2                  # OPEN + EXIT_FAILED (both held)
    assert row["has_exit_failed"] is True
    assert "oldest_mark_age_ms" in row
    assert "last_reconcile_age_seconds" in row
    assert "reconcile_healthy" in row


def test_health_tenant_scoped(clean_positions):
    s2 = _session(user_id="2", status="RUNNING")
    _session(user_id="3", status="RUNNING")
    u2 = Caller(user_id="2", is_admin=False, authenticated=True)
    out = health(caller=u2)
    sids = [s["session_id"] for s in out["sessions"]]
    assert s2.session_id in sids
    assert all(s["user_id"] == "2" for s in out["sessions"])
