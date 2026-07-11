"""SPRINT CLUSTER 6 ITEM 1 — alerts.py is a REAL notifier (push + persist + ack +
escalate), not the log-only stub.

MUTATION (verified): revert send_urgent() to the log-only stub —
    def send_urgent(message): log.critical("[ALERT:URGENT] %s", message)
→ test_send_urgent_pushes_and_persists FAILS (the injected transport is never
called AND no autotrade_alerts row is written).
"""
from datetime import datetime, timedelta, timezone

import pytest

from autotrade import alerts
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))


class FakeTransport:
    """Records every dispatch; optionally raises to prove failure isolation."""

    def __init__(self, raise_exc=None):
        self.calls = []
        self.raise_exc = raise_exc

    def __call__(self, *, title, body, kind, severity):
        self.calls.append({"title": title, "body": body,
                           "kind": kind, "severity": severity})
        if self.raise_exc:
            raise self.raise_exc
        return {"sent": 1, "failed": 0}


@pytest.fixture(autouse=True)
def _restore_transport():
    yield
    alerts.set_transport(None)


def _row(alert_id):
    with falcon_conn() as con:
        return con.execute("SELECT * FROM autotrade_alerts WHERE id=?",
                           (alert_id,)).fetchone()


def test_send_urgent_pushes_and_persists(clean_positions):
    ft = FakeTransport()
    alerts.set_transport(ft)
    aid = alerts.send_urgent("MANUAL EXIT REQUIRED: FOO", kind="EXIT_FAILED",
                             session_id="sess1", symbol="FOO")
    # (a) a push WAS dispatched via the injected transport.
    assert len(ft.calls) == 1
    assert ft.calls[0]["kind"] == "EXIT_FAILED"
    # (b) an UNACKED record was persisted.
    assert aid is not None
    r = _row(aid)
    assert r is not None
    assert r["acknowledged"] == 0
    assert r["escalated"] == 0
    assert r["severity"] == "urgent"
    assert r["kind"] == "EXIT_FAILED"
    assert r["session_id"] == "sess1"
    assert r["symbol"] == "FOO"
    assert r["pushed"] == 1


def test_push_failure_never_raises_into_caller(clean_positions):
    # A transport that raises must NOT propagate into the trading path — and the
    # record must STILL persist (best-effort push).
    alerts.set_transport(FakeTransport(raise_exc=RuntimeError("push down")))
    aid = alerts.send_urgent("x", kind="EXIT_FAILED", session_id="s2", symbol="B")
    assert aid is not None            # did NOT raise
    r = _row(aid)
    assert r is not None
    assert r["pushed"] == 0           # push failed but the record persisted


def test_send_urgent_deduped_pages_once_per_window(clean_positions):
    ft = FakeTransport()
    alerts.set_transport(ft)
    a1 = alerts.send_urgent_deduped(kind="EXIT_FAILED", session_id="s3",
                                    symbol="C", detail="d1")
    a2 = alerts.send_urgent_deduped(kind="EXIT_FAILED", session_id="s3",
                                    symbol="C", detail="d2")
    assert a1 is not None
    assert a2 is None                 # deduped (same kind|session|symbol in window)
    assert len(ft.calls) == 1
    # A DIFFERENT symbol is a distinct incident → pages again.
    a3 = alerts.send_urgent_deduped(kind="EXIT_FAILED", session_id="s3",
                                    symbol="D", detail="d3")
    assert a3 is not None
    assert len(ft.calls) == 2


def test_acknowledge_flips_the_flag(clean_positions):
    alerts.set_transport(FakeTransport())
    aid = alerts.send_urgent("x", kind="EXIT_FAILED", session_id="s4", symbol="E")
    assert alerts.acknowledge(aid) is True
    assert _row(aid)["acknowledged"] == 1
    assert alerts.acknowledge(9_999_999) is False   # unknown id


def _insert_old_urgent(session_id, symbol, age_sec, acked=0):
    ts = (datetime.now(IST) - timedelta(seconds=age_sec)).isoformat()
    with falcon_conn() as con:
        cur = con.execute(
            "INSERT INTO autotrade_alerts (ts, incident_id, severity, kind, "
            "session_id, symbol, detail, acknowledged, escalated, pushed) "
            "VALUES (?,?,?,?,?,?,?,?,0,1)",
            (ts, f"EXIT_FAILED|{session_id}|{symbol}", "urgent", "EXIT_FAILED",
             session_id, symbol, "old", acked))
        con.commit()
        return cur.lastrowid


def test_escalate_stale_unacked_urgent(clean_positions):
    ft = FakeTransport()
    alerts.set_transport(ft)
    stale_id = _insert_old_urgent("s5", "F", age_sec=1000, acked=0)
    acked_id = _insert_old_urgent("s5", "G", age_sec=1000, acked=1)
    fresh_id = _insert_old_urgent("s5", "H", age_sec=10, acked=0)

    escalated = alerts.escalate_stale_alerts(threshold_sec=300)

    assert stale_id in escalated            # old + unacked → escalated
    assert acked_id not in escalated        # acked → never escalated
    assert fresh_id not in escalated        # within threshold → not yet
    assert len(ft.calls) == 1               # exactly the one stale alert re-pushed
    with falcon_conn() as con:
        assert con.execute("SELECT escalated FROM autotrade_alerts WHERE id=?",
                           (stale_id,)).fetchone()["escalated"] == 1
    # Idempotent: a second pass does not re-escalate the same row.
    assert alerts.escalate_stale_alerts(threshold_sec=300) == []
