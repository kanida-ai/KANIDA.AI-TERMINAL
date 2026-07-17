"""ALERT AUTO-TRIAGE — Tier 1 (evidence-gated auto-resolve), Tier 2 (suppress at
the detector), Tier 3 (always page, never auto-ack).

THE DESIGN LESSON UNDER TEST (from triaging all 31 unacked alerts, 2026-07-16):
the "URGENT" alerts were NOISE and the QUIET ones were the real ₹8.3L event.
  * ECLERX CORP_ACTION_SUSPECTED was flagged URGENT and was pure coincidence:
    1465/498 = 2.9417 vs a 3.0 tolerance of 0.06 → it matched a "1:3 reverse
    split" BY 0.0017.
  * The quiet UNATTRIBUTED_CLOSE deficits were the paper-contaminates-live bug
    that left 706 real MAPMYINDIA shares (~₹8.33L) unsold.
Therefore every test here asserts that auto-ack keys on PROVEN-TRANSIENT
EVIDENCE, never on an alert's kind or severity — each Tier-1 rule is tested BOTH
ways (acks WITH its evidence, does NOT ack WITHOUT it).

MUTATION CHECKS (each documented at its test):
  * Drop the is_certified gate → test_uncertified_not_acked_while_still_uncertified FAILS.
  * Drop the exit-in-blind-window gate → test_reconcile_stale_not_acked_when_exit_occurred FAILS.
  * Restore _CORP_ACTION_TOL 0.02 on the triage path → test_eclerx_ratio_coincidence_rejected FAILS.
  * Remove the in-flight/selling-session attribution → test_deficit_attributed_to_selling_session FAILS.
  * Remove the Tier-3 detector → test_reconciled_flat_no_exit_order_fires_on_83L_shape FAILS.
"""
from datetime import datetime, timedelta, timezone

import pytest

from autotrade import alerts
from autotrade.monitoring import alert_monitor, basket_gen
from autotrade.monitoring import position_reconciler as pr
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
NOW = datetime(2026, 7, 16, 11, 0, 0, tzinfo=IST)


# ── helpers ──────────────────────────────────────────────────────────────────
class _T:
    """Push transport stub — records dispatches, never touches the network."""

    def __init__(self):
        self.calls = []

    def __call__(self, *, title, body, kind, severity):
        self.calls.append({"kind": kind, "severity": severity, "body": body})
        return {"sent": 1, "failed": 0}


@pytest.fixture(autouse=True)
def _triage_env(monkeypatch):
    """Every test starts with the flag OFF and clean triage state; a test that
    wants the layer on calls _on(monkeypatch) explicitly."""
    monkeypatch.delenv("FALCON_AUTOTRADE_AUTO_TRIAGE", raising=False)
    monkeypatch.delenv("FALCON_AUTOTRADE_HIGH_SLIPPAGE_PCT", raising=False)
    # Min-age gate off by default so a freshly-inserted test alert is eligible;
    # the age gate has its own dedicated test.
    monkeypatch.setenv("FALCON_AUTOTRADE_TRIAGE_MIN_AGE_SEC", "0")
    pr.reset_divergence_streaks()
    alerts.set_transport(_T())
    yield
    alerts.set_transport(None)
    pr.reset_divergence_streaks()
    with basket_gen._LOCK:
        basket_gen._RECONCILE_SUCCESS_TS.clear()


def _on(monkeypatch):
    monkeypatch.setenv("FALCON_AUTOTRADE_AUTO_TRIAGE", "true")


def _wipe():
    with falcon_conn() as con:
        con.execute("DELETE FROM autotrade_alerts")
        con.execute("DELETE FROM autotrade_order_events")
        con.execute("DELETE FROM autotrade_positions")
        con.execute("DELETE FROM autotrade_sessions")
        con.execute("DELETE FROM autotrade_slippage")
        con.commit()


@pytest.fixture(autouse=True)
def _clean(clean_positions):
    _wipe()
    yield
    _wipe()


def _mk_alert(kind, *, session_id=None, symbol=None, detail="", ts=None,
              severity="urgent"):
    with falcon_conn() as con:
        cur = con.execute(
            """INSERT INTO autotrade_alerts
               (ts, incident_id, severity, kind, session_id, symbol, detail,
                acknowledged, escalated, pushed, push_result)
               VALUES (?,?,?,?,?,?,?,0,0,0,'')""",
            ((ts or (NOW - timedelta(hours=1))).isoformat(),
             f"{kind}|{session_id or ''}|{symbol or ''}", severity, kind,
             session_id, symbol, detail))
        con.commit()
        return cur.lastrowid


def _alert_row(aid):
    with falcon_conn() as con:
        r = con.execute("SELECT * FROM autotrade_alerts WHERE id=?",
                        (aid,)).fetchone()
    return dict(r) if r else None


def _mk_session(sid, mode="live"):
    with falcon_conn() as con:
        con.execute(
            "INSERT INTO autotrade_sessions (session_id, mode, status, "
            "config_json, total_allocated_capital, created_at) "
            "VALUES (?,?,?,?,?,?)",
            (sid, mode, "RUNNING", '{"order_product":"MIS"}', 1000000.0,
             (NOW - timedelta(hours=3)).isoformat()))
        con.commit()


def _mk_event(session_id, symbol, event_type, *, broker_order_id=None, qty=None,
              ts=None, product="MIS", profile="default"):
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_order_events
               (ts, session_id, symbol, product, broker_profile,
                broker_order_id, client_order_id, event_type, qty, source)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            ((ts or NOW).isoformat(), session_id, symbol, product, profile,
             broker_order_id, f"COID-{symbol}", event_type, qty, "exit"))
        con.commit()


def _mk_position(sid, symbol, *, qty=100, status="CLOSED",
                 close_reason="STOP_RECONCILED_FLAT", exit_order_id=None,
                 avg_price=1180.0, closed_at=None):
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_positions
               (session_id, broker_profile, symbol, instrument_type, qty,
                avg_price, status, close_reason, exit_order_id, product,
                opened_at, closed_at, direction)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sid, "default", symbol, "EQ", qty, avg_price, status, close_reason,
             exit_order_id, "MIS", (NOW - timedelta(hours=2)).isoformat(),
             (closed_at or NOW).isoformat(), "long"))
        con.commit()


# ═════════════════════════════════════════════════════════════════════════════
# TIER 3 — THE ₹8.3L DETECTOR (built first: highest value)
# ═════════════════════════════════════════════════════════════════════════════
def test_reconciled_flat_no_exit_order_fires_on_83L_shape(monkeypatch):
    """THE 2026-07-15 MAPMYINDIA SHAPE, EXACTLY: a LIVE session's position booked
    CLOSED with close_reason='STOP_RECONCILED_FLAT' and exit_order_id NULL. 706
    real shares (~₹8.33L) were never sold and NOTHING paged.

    MUTATION: delete detect_reconciled_flat_without_exit_order's page → FAILS."""
    _on(monkeypatch)
    _mk_session("b447b0d7f6dc", mode="live")
    _mk_position("b447b0d7f6dc", "MAPMYINDIA", qty=706, avg_price=1180.0,
                 status="CLOSED", close_reason="STOP_RECONCILED_FLAT",
                 exit_order_id=None)

    out = alert_monitor.detect_reconciled_flat_without_exit_order(now=NOW)

    assert len(out) == 1, "the ₹8.3L shape MUST page"
    assert out[0]["symbol"] == "MAPMYINDIA"
    assert out[0]["qty"] == 706
    assert out[0]["session_id"] == "b447b0d7f6dc"
    # ~₹8.33L notional surfaced in the page so the operator sees the stake.
    assert 830000 < out[0]["notional"] < 840000
    assert out[0]["alert_id"] is not None
    row = _alert_row(out[0]["alert_id"])
    assert row["kind"] == "RECONCILED_FLAT_NO_EXIT_ORDER"
    assert row["severity"] == "urgent"
    assert "UNSOLD" in row["detail"]


def test_reconciled_flat_with_exit_order_id_does_not_fire(monkeypatch):
    """NO-FIRE: the same close reason WITH a real exit_order_id is a genuine,
    order-id-proven exit. It must never page."""
    _on(monkeypatch)
    _mk_session("s-live", mode="live")
    _mk_position("s-live", "MAPMYINDIA", qty=706,
                 close_reason="STOP_RECONCILED_FLAT", exit_order_id="250716123")
    assert alert_monitor.detect_reconciled_flat_without_exit_order(now=NOW) == []


def test_reconciled_flat_paper_session_does_not_fire(monkeypatch):
    """NO-FIRE: a PAPER close has no broker order-id by construction, so paper
    would otherwise fire on every single close. Paper must be invisible."""
    _on(monkeypatch)
    _mk_session("s-paper", mode="paper")
    _mk_position("s-paper", "MAPMYINDIA", qty=706,
                 close_reason="STOP_RECONCILED_FLAT", exit_order_id=None)
    assert alert_monitor.detect_reconciled_flat_without_exit_order(now=NOW) == []


def test_reconciled_flat_detector_is_off_when_flag_off():
    """FLAG-OFF: byte-identical — the detector does not even read a row."""
    _mk_session("b447b0d7f6dc", mode="live")
    _mk_position("b447b0d7f6dc", "MAPMYINDIA", qty=706, exit_order_id=None)
    assert alert_monitor.detect_reconciled_flat_without_exit_order(now=NOW) == []
    with falcon_conn() as con:
        n = con.execute("SELECT COUNT(*) c FROM autotrade_alerts").fetchone()["c"]
    assert n == 0


def test_reconciled_flat_kind_is_never_auto_ackable(monkeypatch):
    """Tier 3 contract: the detector's own kind can NEVER be auto-acked, and
    neither can any other real-money kind."""
    _on(monkeypatch)
    for kind in ("EXIT_FAILED", "KILLING_INCOMPLETE", "NAKED_POSITION",
                 "MANUAL_CONFLICT", "UNATTRIBUTED_CLOSE", "ORPHAN_AT_BROKER",
                 "DOUBLE_FILL", "RECONCILED_FLAT_NO_EXIT_ORDER"):
        assert kind in alert_monitor._NEVER_AUTO_ACK
        assert kind not in alert_monitor._AUTO_ACKABLE_KINDS
        assert kind not in alert_monitor._TIER1_RULES


def test_tier3_kinds_are_never_acked_by_auto_resolve(monkeypatch):
    """End-to-end: seed one alert of EVERY Tier-3 kind and prove auto_resolve
    leaves every one of them UNACKED."""
    _on(monkeypatch)
    _mk_session("s1", mode="live")
    ids = [_mk_alert(k, session_id="s1", symbol="FOO", detail=f"{k}: bad")
           for k in sorted(alert_monitor._NEVER_AUTO_ACK)]
    assert alert_monitor.auto_resolve(now=NOW) == []
    for aid in ids:
        assert _alert_row(aid)["acknowledged"] == 0


# ═════════════════════════════════════════════════════════════════════════════
# TIER 1(a) — UNCERTIFIED_BROKER_BLOCKED superseded by certification
# ═════════════════════════════════════════════════════════════════════════════
_BLOCK_DETAIL = ("UNCERTIFIED_BROKER_BLOCKED: rupeezy is not certified for LIVE "
                 "order placement — refused (paper only).")


def test_uncertified_acked_when_certified_now_and_zero_live_orders(monkeypatch):
    """FIRE (ack): the real 2026-07-16 case — rupeezy is NOW certified, so the 5
    historical alerts are superseded. Evidence: is_certified(broker)==True."""
    _on(monkeypatch)
    monkeypatch.setattr("autotrade.broker.registry.is_certified",
                        lambda name: name == "rupeezy")
    aid = _mk_alert("UNCERTIFIED_BROKER_BLOCKED", symbol="TATASTEEL",
                    detail=_BLOCK_DETAIL)

    out = alert_monitor.auto_resolve(now=NOW)

    assert len(out) == 1 and out[0]["alert_id"] == aid
    row = _alert_row(aid)
    assert row["acknowledged"] == 1
    assert row["auto_resolved"] == 1
    # AUDIT: the reason names the EVIDENCE, and the row was NOT deleted.
    assert "superseded by certification" in row["ack_reason"]
    assert "rupeezy" in row["ack_reason"]
    assert row["detail"] == _BLOCK_DETAIL


def test_uncertified_not_acked_while_still_uncertified(monkeypatch):
    """NO-FIRE: the broker is STILL uncertified → the alert is a LIVE condition,
    not history. MUTATION: drop the is_certified gate → this FAILS."""
    _on(monkeypatch)
    monkeypatch.setattr("autotrade.broker.registry.is_certified",
                        lambda name: False)
    aid = _mk_alert("UNCERTIFIED_BROKER_BLOCKED", symbol="TATASTEEL",
                    detail=_BLOCK_DETAIL)
    assert alert_monitor.auto_resolve(now=NOW) == []
    assert _alert_row(aid)["acknowledged"] == 0
    assert _alert_row(aid)["ack_reason"] is None


def test_uncertified_not_acked_when_session_placed_live_orders(monkeypatch):
    """NO-FIRE: a session that placed REAL broker orders is entangled with live
    execution → a human reads it, whatever the certification says."""
    _on(monkeypatch)
    monkeypatch.setattr("autotrade.broker.registry.is_certified",
                        lambda name: True)
    _mk_session("s-live", mode="live")
    _mk_event("s-live", "TATASTEEL", "ORDER_SUBMITTED",
              broker_order_id="2507160001", qty=10)
    aid = _mk_alert("UNCERTIFIED_BROKER_BLOCKED", session_id="s-live",
                    symbol="TATASTEEL", detail=_BLOCK_DETAIL)
    assert alert_monitor.auto_resolve(now=NOW) == []
    assert _alert_row(aid)["acknowledged"] == 0


def test_uncertified_acked_when_session_has_only_paper_orders(monkeypatch):
    """FIRE (ack): a session whose ledger has ONLY paper events (broker_order_id
    NULL) placed 0 LIVE orders → the gate passes."""
    _on(monkeypatch)
    monkeypatch.setattr("autotrade.broker.registry.is_certified",
                        lambda name: True)
    _mk_session("s-paper", mode="paper")
    _mk_event("s-paper", "TATASTEEL", "ORDER_CREATED", broker_order_id=None,
              qty=10)
    aid = _mk_alert("UNCERTIFIED_BROKER_BLOCKED", session_id="s-paper",
                    symbol="TATASTEEL", detail=_BLOCK_DETAIL)
    assert len(alert_monitor.auto_resolve(now=NOW)) == 1
    assert _alert_row(aid)["acknowledged"] == 1


def test_uncertified_broker_name_is_read_from_the_alert_not_hardcoded(
        monkeypatch):
    """BROKER-AGNOSTIC (operator hard requirement): the rule reads whatever
    broker the alert names and asks the registry. No `if broker == "..."`."""
    _on(monkeypatch)
    asked = []

    def _cert(name):
        asked.append(name)
        return True

    monkeypatch.setattr("autotrade.broker.registry.is_certified", _cert)
    _mk_alert("UNCERTIFIED_BROKER_BLOCKED", detail=(
        "UNCERTIFIED_BROKER_BLOCKED: somebroker9 is not certified for LIVE "
        "order placement"))
    alert_monitor.auto_resolve(now=NOW)
    assert asked == ["somebroker9"]


def test_uncertified_not_acked_when_broker_unparseable(monkeypatch):
    """NO-FIRE: cannot name the broker → cannot prove anything → no ack."""
    _on(monkeypatch)
    monkeypatch.setattr("autotrade.broker.registry.is_certified",
                        lambda name: True)
    aid = _mk_alert("UNCERTIFIED_BROKER_BLOCKED", detail="something went wrong")
    assert alert_monitor.auto_resolve(now=NOW) == []
    assert _alert_row(aid)["acknowledged"] == 0


# ═════════════════════════════════════════════════════════════════════════════
# TIER 1(b) — RECONCILE_STALE self-healed
# ═════════════════════════════════════════════════════════════════════════════
def test_reconcile_stale_acked_when_later_healthy_reconcile_and_no_exit(
        monkeypatch):
    """FIRE (ack): a HEALTHY reconcile landed AFTER the page and NO exit happened
    during the blind window → provably transient."""
    _on(monkeypatch)
    _mk_session("s-stale", mode="live")
    a_ts = NOW - timedelta(minutes=10)
    aid = _mk_alert("RECONCILE_STALE", session_id="s-stale", ts=a_ts,
                    detail="RECONCILE_STALE: last healthy reconcile was 130s ago")
    # EVIDENCE: a healthy reconcile 60s AFTER the page.
    basket_gen.record_reconcile_health(
        "s-stale", True, ts=(a_ts + timedelta(seconds=60)).isoformat())

    out = alert_monitor.auto_resolve(now=NOW)

    assert len(out) == 1 and out[0]["alert_id"] == aid
    row = _alert_row(aid)
    assert row["acknowledged"] == 1 and row["auto_resolved"] == 1
    assert "self-healed after 60s" in row["ack_reason"]
    assert "0 exit events" in row["ack_reason"]


def test_reconcile_stale_not_acked_without_a_later_healthy_reconcile(
        monkeypatch):
    """NO-FIRE: no healthy reconcile recorded at all → we are still blind, or the
    process restarted and cannot prove recovery. Fail CLOSED."""
    _on(monkeypatch)
    _mk_session("s-stale", mode="live")
    aid = _mk_alert("RECONCILE_STALE", session_id="s-stale",
                    ts=NOW - timedelta(minutes=10))
    assert alert_monitor.auto_resolve(now=NOW) == []
    assert _alert_row(aid)["acknowledged"] == 0


def test_reconcile_stale_not_acked_when_healthy_reconcile_predates_the_alert(
        monkeypatch):
    """NO-FIRE: the only healthy reconcile is OLDER than the page → it is the one
    that WENT stale, not a recovery."""
    _on(monkeypatch)
    _mk_session("s-stale", mode="live")
    a_ts = NOW - timedelta(minutes=10)
    aid = _mk_alert("RECONCILE_STALE", session_id="s-stale", ts=a_ts)
    basket_gen.record_reconcile_health(
        "s-stale", True, ts=(a_ts - timedelta(seconds=200)).isoformat())
    assert alert_monitor.auto_resolve(now=NOW) == []
    assert _alert_row(aid)["acknowledged"] == 0


def test_reconcile_stale_not_acked_when_exit_occurred(monkeypatch):
    """NO-FIRE — THE MONEY CASE: an exit fired while we were reconcile-blind. We
    exited real money against an unvalidated basket; that is a REAL finding.

    MUTATION: drop the exit-in-blind-window gate → this FAILS."""
    _on(monkeypatch)
    _mk_session("s-stale", mode="live")
    a_ts = NOW - timedelta(minutes=10)
    aid = _mk_alert("RECONCILE_STALE", session_id="s-stale", ts=a_ts)
    basket_gen.record_reconcile_health(
        "s-stale", True, ts=(a_ts + timedelta(seconds=60)).isoformat())
    # An exit landed INSIDE the blind window.
    _mk_event("s-stale", "BIOCON", "EXIT_PLACED", broker_order_id="X1", qty=50,
              ts=a_ts + timedelta(seconds=20))

    assert alert_monitor.auto_resolve(now=NOW) == []
    assert _alert_row(aid)["acknowledged"] == 0


def test_reconcile_stale_acked_when_exit_was_outside_the_blind_window(
        monkeypatch):
    """FIRE (ack): an exit well AFTER the window closed does not taint the blind
    window — the window is bounded, not "any exit ever"."""
    _on(monkeypatch)
    _mk_session("s-stale", mode="live")
    a_ts = NOW - timedelta(minutes=10)
    aid = _mk_alert("RECONCILE_STALE", session_id="s-stale", ts=a_ts)
    basket_gen.record_reconcile_health(
        "s-stale", True, ts=(a_ts + timedelta(seconds=60)).isoformat())
    _mk_event("s-stale", "BIOCON", "EXIT_PLACED", broker_order_id="X1", qty=50,
              ts=a_ts + timedelta(seconds=300))   # AFTER the healed ts
    assert len(alert_monitor.auto_resolve(now=NOW)) == 1
    assert _alert_row(aid)["acknowledged"] == 1


# ═════════════════════════════════════════════════════════════════════════════
# TIER 1 — cross-cutting safety guards
# ═════════════════════════════════════════════════════════════════════════════
def test_open_tier3_incident_quarantines_the_session(monkeypatch):
    """SAFETY GUARD: a session with ANY unacked Tier-3 alert has NONE of its
    alerts acked — a "transient" stale next to a live EXIT_FAILED is context a
    human needs, not noise to sweep away."""
    _on(monkeypatch)
    _mk_session("s-bad", mode="live")
    a_ts = NOW - timedelta(minutes=10)
    aid = _mk_alert("RECONCILE_STALE", session_id="s-bad", ts=a_ts)
    basket_gen.record_reconcile_health(
        "s-bad", True, ts=(a_ts + timedelta(seconds=60)).isoformat())
    # Without this the alert WOULD be acked (proven by the fire test above).
    _mk_alert("EXIT_FAILED", session_id="s-bad", symbol="BIOCON",
              detail="EXIT_FAILED: still held")

    assert alert_monitor.auto_resolve(now=NOW) == []
    assert _alert_row(aid)["acknowledged"] == 0


def test_fresh_alert_is_not_acked_before_min_age(monkeypatch):
    """SAFETY GUARD: a condition that fired seconds ago has not proven itself
    transient. The min-age gate holds it."""
    _on(monkeypatch)
    monkeypatch.setenv("FALCON_AUTOTRADE_TRIAGE_MIN_AGE_SEC", "300")
    _mk_session("s-stale", mode="live")
    a_ts = NOW - timedelta(seconds=30)          # far younger than 300s
    aid = _mk_alert("RECONCILE_STALE", session_id="s-stale", ts=a_ts)
    basket_gen.record_reconcile_health(
        "s-stale", True, ts=(a_ts + timedelta(seconds=10)).isoformat())
    assert alert_monitor.auto_resolve(now=NOW) == []
    assert _alert_row(aid)["acknowledged"] == 0


def test_auto_ack_never_deletes_a_row(monkeypatch):
    """CONTRACT: an auto-ack flips flags + writes an audit reason. The row and
    its original detail survive forever."""
    _on(monkeypatch)
    monkeypatch.setattr("autotrade.broker.registry.is_certified",
                        lambda name: True)
    aid = _mk_alert("UNCERTIFIED_BROKER_BLOCKED", detail=_BLOCK_DETAIL)
    alert_monitor.auto_resolve(now=NOW)
    with falcon_conn() as con:
        n = con.execute("SELECT COUNT(*) c FROM autotrade_alerts").fetchone()["c"]
    assert n == 1
    row = _alert_row(aid)
    assert row["detail"] == _BLOCK_DETAIL
    assert row["ack_reason"].startswith("AUTO_TRIAGE: ")


def test_auto_resolve_is_a_noop_when_flag_off(monkeypatch):
    """FLAG-OFF: byte-identical — a perfectly ackable alert stays UNACKED."""
    monkeypatch.setattr("autotrade.broker.registry.is_certified",
                        lambda name: True)
    aid = _mk_alert("UNCERTIFIED_BROKER_BLOCKED", detail=_BLOCK_DETAIL)
    assert alert_monitor.auto_resolve(now=NOW) == []
    assert _alert_row(aid)["acknowledged"] == 0


def test_operator_ack_path_is_unchanged():
    """BACKWARD COMPAT: acknowledge(id) with no reason behaves exactly as before
    — acked, ack_reason NULL, auto_resolved 0 (a human ack, not a machine one)."""
    aid = _mk_alert("EXIT_FAILED", session_id="s1", symbol="FOO")
    assert alerts.acknowledge(aid) is True
    row = _alert_row(aid)
    assert row["acknowledged"] == 1
    assert row["ack_reason"] is None
    assert row["auto_resolved"] == 0


def test_auto_triage_entry_point_disabled_by_default():
    """The master flag defaults OFF."""
    assert alerts.auto_triage_enabled() is False
    assert alert_monitor.auto_triage(now=NOW, force=True)["enabled"] is False


# ═════════════════════════════════════════════════════════════════════════════
# TIER 2 — SUPPRESS AT THE DETECTOR
# ═════════════════════════════════════════════════════════════════════════════
def test_eclerx_ratio_coincidence_rejected(monkeypatch):
    """THE ECLERX LESSON: 1465/498 = 2.9417 matched "1:3 reverse split" by 0.0017
    under the ±2% tolerance (0.06 at R=3.0). Under the STRICT ±0.2% tolerance it
    is correctly rejected as the coincidence it is.

    MUTATION: pass tol=_CORP_ACTION_TOL (0.02) here → this FAILS."""
    # The arithmetic of the coincidence, asserted exactly. (Note 1465/498 =
    # 2.941767… — the incident write-up's "2.9417" was truncated, not rounded.)
    raw = 1465 / 498
    assert raw == pytest.approx(2.94177, abs=1e-5)
    assert abs(raw - 3.0) == pytest.approx(0.0582, abs=1e-4)   # the miss
    assert pr._CORP_ACTION_TOL * 3.0 == pytest.approx(0.06)    # the old window
    assert abs(raw - 3.0) < pr._CORP_ACTION_TOL * 3.0          # → it QUALIFIED
    # The legacy tolerance DID classify it as a "1:3 reverse split" (broker <
    # db → the reciprocal 1/3). This is the bug, reproduced exactly.
    assert pr._corp_action_ratio(498, 1465) == round(1 / 3, 4)
    # The STRICT tolerance is far tighter than the 0.0583 miss → rejected.
    assert pr._CORP_ACTION_TOL_STRICT * 3.0 == pytest.approx(0.006)
    assert pr._corp_action_ratio(498, 1465,
                                 tol=pr._CORP_ACTION_TOL_STRICT) is None


def test_a_real_split_still_classifies_under_the_strict_tolerance():
    """NO-FALSE-NEGATIVE: a GENUINE corporate action is EXACT, so tightening the
    tolerance does not lose it. 1465 → 4395 is exactly ×3."""
    assert pr._corp_action_ratio(4395, 1465,
                                 tol=pr._CORP_ACTION_TOL_STRICT) == 3.0
    # A true 1:3 reverse split of a round lot: 1500 → 500, exactly ×(1/3).
    assert pr._corp_action_ratio(500, 1500,
                                 tol=pr._CORP_ACTION_TOL_STRICT) == round(1 / 3, 4)
    # Integer rounding on an odd lot is still forgiven (1465/488 = 3.0020).
    assert pr._corp_action_ratio(488, 1465,
                                 tol=pr._CORP_ACTION_TOL_STRICT) is not None


def test_legacy_corp_action_tolerance_unchanged_by_default():
    """FLAG-OFF / default-arg: the legacy ±2% behaviour is byte-identical."""
    assert pr._CORP_ACTION_TOL == 0.02
    assert pr._corp_action_ratio(498, 1465) is not None   # as it always did


def test_corp_action_calendar_is_not_implemented_and_never_confirms():
    """A ratio is NOT evidence. The calendar cross-check is a declared stub that
    returns UNKNOWN (None) — never a silent True."""
    assert pr._corp_action_calendar_confirms("ECLERX", 3.0) is None


def test_quiet_period_suppresses_a_transient_divergence(monkeypatch):
    """FIRE/NO-FIRE: cycle 1 of a divergence is SUPPRESSED (the entry-fill and
    iceberg races all self-heal in <4 min); cycle 2 alerts."""
    _on(monkeypatch)
    key = ("BIOCON", "MIS", None, "DEFICIT")
    assert pr._bump_divergence_streak(key) == 1     # cycle 1 → below the bound
    assert pr._min_divergence_cycles() == 2
    assert pr._bump_divergence_streak(key) == 2     # cycle 2 → persistent


def test_quiet_period_streak_restarts_after_a_gap(monkeypatch):
    """CONSECUTIVE means consecutive: a divergence that vanishes and returns
    later is NEW, not persistent."""
    _on(monkeypatch)
    key = ("BIOCON", "MIS", None, "DEFICIT")
    assert pr._bump_divergence_streak(key) == 1
    # Simulate the gap by ageing the last-seen stamp beyond the streak gap.
    with pr._STREAK_LOCK:
        c, last = pr._DIVERGENCE_STREAK[key]
        pr._DIVERGENCE_STREAK[key] = (c, last - (pr._STREAK_GAP_SEC + 1))
    assert pr._bump_divergence_streak(key) == 1     # restarted, NOT 2


def test_min_cycles_is_tunable(monkeypatch):
    _on(monkeypatch)
    monkeypatch.setenv("FALCON_AUTOTRADE_TRIAGE_MIN_CYCLES", "3")
    assert pr._min_divergence_cycles() == 3
    # Never below 1 (a 0 would disable alerting entirely).
    monkeypatch.setenv("FALCON_AUTOTRADE_TRIAGE_MIN_CYCLES", "0")
    assert pr._min_divergence_cycles() == 1


def test_inflight_exit_detected_when_no_terminal_event(monkeypatch):
    """IN-FLIGHT AWARENESS: an EXIT_PLACED with a real broker order-id and NO
    terminal event is our own exit working at the broker."""
    _on(monkeypatch)
    _mk_session("1aeb11b8", mode="live")
    _mk_event("1aeb11b8", "MAPMYINDIA", "EXIT_PLACED", broker_order_id="OID1",
              qty=40, ts=NOW - timedelta(seconds=5))
    inflight = pr._inflight_exits_for("MAPMYINDIA", "MIS", ["default"], now=NOW)
    assert len(inflight) == 1
    assert pr._inflight_exit_qty(inflight) == 40


def test_inflight_exit_not_counted_once_terminal(monkeypatch):
    """NO-FIRE: an exit that FILLED is resolved — it is no longer in flight, so
    it can no longer explain (suppress) a deficit."""
    _on(monkeypatch)
    _mk_session("1aeb11b8", mode="live")
    _mk_event("1aeb11b8", "MAPMYINDIA", "EXIT_PLACED", broker_order_id="OID1",
              qty=40, ts=NOW - timedelta(seconds=30))
    _mk_event("1aeb11b8", "MAPMYINDIA", "EXIT_FILLED", broker_order_id="OID1",
              qty=40, ts=NOW - timedelta(seconds=20))
    assert pr._inflight_exits_for("MAPMYINDIA", "MIS", ["default"], now=NOW) == []


def test_inflight_exit_expires_and_stops_suppressing(monkeypatch):
    """A STUCK exit is a real finding, not a race: past the in-flight max age it
    no longer counts as in flight, so the deficit alerts again."""
    _on(monkeypatch)
    _mk_session("1aeb11b8", mode="live")
    _mk_event("1aeb11b8", "MAPMYINDIA", "EXIT_PLACED", broker_order_id="OID1",
              qty=40, ts=NOW - timedelta(seconds=3600))   # an hour old
    assert pr._inflight_exits_for("MAPMYINDIA", "MIS", ["default"], now=NOW) == []


def test_deficit_attributed_to_selling_session(monkeypatch):
    """THE ATTRIBUTION FIX, on the proven numbers: alerts 21-24 each equalled
    session 1aeb11b8's FIRST ICEBERG EXIT CHILD exactly — MAPMYINDIA 40, BIOCON
    2091, CHALET 31, ECLERX 52 — yet all four were blamed on d9b218cf, which
    never sold. The deficit belongs to the SELLING session.

    MUTATION: make _selling_sessions return the reconciling session → FAILS."""
    _on(monkeypatch)
    _mk_session("1aeb11b8", mode="live")     # the SELLER
    _mk_session("d9b218cf", mode="live")     # blamed by the old code; never sold
    for sym, qty in (("MAPMYINDIA", 40), ("BIOCON", 2091), ("CHALET", 31),
                     ("ECLERX", 52)):
        _mk_event("1aeb11b8", sym, "EXIT_PLACED", broker_order_id=f"OID-{sym}",
                  qty=qty, ts=NOW - timedelta(seconds=5))

    for sym, qty in (("MAPMYINDIA", 40), ("BIOCON", 2091), ("CHALET", 31),
                     ("ECLERX", 52)):
        inflight = pr._inflight_exits_for(sym, "MIS", ["default"], now=NOW)
        # The in-flight qty equals the observed deficit EXACTLY (the proof).
        assert pr._inflight_exit_qty(inflight) == qty, sym
        # And it is attributed to the SELLER — never to d9b218cf.
        assert pr._selling_sessions(inflight) == ["1aeb11b8"], sym
        assert "d9b218cf" not in pr._selling_sessions(inflight), sym


def test_inflight_scan_scoped_to_broker_profile(monkeypatch):
    """Another broker account's exit can never explain THIS account's deficit."""
    _on(monkeypatch)
    _mk_session("other", mode="live")
    _mk_event("other", "MAPMYINDIA", "EXIT_PLACED", broker_order_id="OID9",
              qty=40, ts=NOW - timedelta(seconds=5), profile="acctB")
    assert pr._inflight_exits_for("MAPMYINDIA", "MIS", ["default"],
                                  now=NOW) == []
    assert len(pr._inflight_exits_for("MAPMYINDIA", "MIS", ["acctB"],
                                      now=NOW)) == 1


def test_paper_exit_never_counts_as_inflight(monkeypatch):
    """A paper exit has no broker order-id → it can never suppress a LIVE
    deficit (the paper-contaminates-live bug class, kept shut)."""
    _on(monkeypatch)
    _mk_session("s-paper", mode="paper")
    _mk_event("s-paper", "MAPMYINDIA", "EXIT_PLACED", broker_order_id=None,
              qty=40, ts=NOW - timedelta(seconds=5))
    assert pr._inflight_exits_for("MAPMYINDIA", "MIS", ["default"],
                                  now=NOW) == []


def test_tier2_gates_are_inert_when_flag_off(monkeypatch):
    """FLAG-OFF: byte-identical — _triage_on() is False so no Tier-2 gate can
    read a row or suppress anything."""
    assert pr._triage_on() is False
    _mk_session("1aeb11b8", mode="live")
    _mk_event("1aeb11b8", "MAPMYINDIA", "EXIT_PLACED", broker_order_id="OID1",
              qty=40, ts=NOW - timedelta(seconds=5))
    # The helper still works when called directly (it is pure), but the
    # reconciler never calls it while the flag is off.
    assert pr._triage_on() is False


# ═════════════════════════════════════════════════════════════════════════════
# TIER 1(c) — slippage retune
# ═════════════════════════════════════════════════════════════════════════════
def _slip_alerts():
    with falcon_conn() as con:
        return [dict(r) for r in con.execute(
            "SELECT * FROM autotrade_alerts ORDER BY id ASC").fetchall()]


def test_slippage_flag_off_is_byte_identical():
    """FLAG-OFF: threshold 0.5, kind GENERIC, severity warn, NO session_id —
    exactly the pre-triage row."""
    from autotrade.execution import slippage
    assert slippage._high_slippage_threshold() == 0.5
    slippage.record_slippage("BIOCON", 100.0, 100.6, 10, session_id="s1")
    rows = _slip_alerts()
    assert len(rows) == 1
    assert rows[0]["kind"] == "GENERIC"
    assert rows[0]["severity"] == "warn"
    assert rows[0]["session_id"] is None


def test_slippage_retuned_threshold_under_flag(monkeypatch):
    """FLAG-ON: the 0.5% threshold sat exactly at p90 of fills (median 0.111,
    p90 0.487) → it fired on 10.4% of fills. Retuned to 1.5% it does NOT fire on
    a 0.6% fill (which is normal execution, not an anomaly)."""
    _on(monkeypatch)
    from autotrade.execution import slippage
    assert slippage._high_slippage_threshold() == 1.5
    slippage.record_slippage("BIOCON", 100.0, 100.6, 10, session_id="s1")
    assert _slip_alerts() == []


def test_slippage_fires_with_real_kind_and_session_under_flag(monkeypatch):
    """FLAG-ON: a genuine 2% anomaly still fires — now with a REAL kind, a
    session_id, and info severity (telemetry, not a page)."""
    _on(monkeypatch)
    from autotrade.execution import slippage
    slippage.record_slippage("BIOCON", 100.0, 102.0, 10, session_id="s1")
    rows = _slip_alerts()
    assert len(rows) == 1
    assert rows[0]["kind"] == "HIGH_SLIPPAGE"
    assert rows[0]["severity"] == "info"
    assert rows[0]["session_id"] == "s1"
    assert rows[0]["symbol"] == "BIOCON"
    assert rows[0]["pushed"] == 0          # info never pages


def test_slippage_threshold_is_env_tunable(monkeypatch):
    _on(monkeypatch)
    from autotrade.execution import slippage
    monkeypatch.setenv("FALCON_AUTOTRADE_HIGH_SLIPPAGE_PCT", "3.0")
    assert slippage._high_slippage_threshold() == 3.0
    monkeypatch.setenv("FALCON_AUTOTRADE_HIGH_SLIPPAGE_PCT", "garbage")
    assert slippage._high_slippage_threshold() == 1.5      # safe fallback


def test_slippage_row_always_recorded_regardless_of_flag(monkeypatch):
    """The autotrade_slippage ROW is telemetry and is unconditional — only the
    ALERT is thresholded. Retuning must never lose data."""
    _on(monkeypatch)
    from autotrade.execution import slippage
    slippage.record_slippage("BIOCON", 100.0, 100.6, 10, session_id="s1")
    with falcon_conn() as con:
        n = con.execute("SELECT COUNT(*) c FROM autotrade_slippage "
                        "WHERE session_id='s1'").fetchone()["c"]
    assert n == 1


# ═════════════════════════════════════════════════════════════════════════════
# WIRING
# ═════════════════════════════════════════════════════════════════════════════
def test_maybe_escalate_runs_triage_before_escalation(monkeypatch):
    """ORDERING: triage runs BEFORE the escalation scan, so an alert it can prove
    transient is resolved rather than re-paged."""
    _on(monkeypatch)
    _mk_session("s-stale", mode="live")
    a_ts = NOW - timedelta(minutes=10)
    aid = _mk_alert("RECONCILE_STALE", session_id="s-stale", ts=a_ts)
    basket_gen.record_reconcile_health(
        "s-stale", True, ts=(a_ts + timedelta(seconds=60)).isoformat())
    # Reset the escalate throttle so this call actually runs.
    alerts._LAST_ESCALATE_MONO = 0.0
    alert_monitor._TRIAGE_LAST_MONO = 0.0

    escalated = alerts.maybe_escalate(threshold_sec=1, now=NOW)

    # It was ACKED by triage, therefore NOT escalated.
    assert _alert_row(aid)["acknowledged"] == 1
    assert aid not in escalated


def test_maybe_escalate_unchanged_when_flag_off(monkeypatch):
    """FLAG-OFF: an ackable alert is NOT acked and IS escalated — exactly today's
    behaviour."""
    _mk_session("s-stale", mode="live")
    a_ts = NOW - timedelta(minutes=10)
    aid = _mk_alert("RECONCILE_STALE", session_id="s-stale", ts=a_ts)
    basket_gen.record_reconcile_health(
        "s-stale", True, ts=(a_ts + timedelta(seconds=60)).isoformat())
    alerts._LAST_ESCALATE_MONO = 0.0

    escalated = alerts.maybe_escalate(threshold_sec=1, now=NOW)

    assert _alert_row(aid)["acknowledged"] == 0
    assert aid in escalated


def test_auto_triage_never_raises_on_a_broken_db(monkeypatch):
    """A triage failure must NEVER crash the tick path."""
    _on(monkeypatch)

    def _boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(alerts, "unacked_alerts", _boom)
    monkeypatch.setattr(alert_monitor, "_live_session_ids", _boom)
    out = alert_monitor.auto_triage(now=NOW, force=True)
    assert out["enabled"] is True
    assert out["paged"] == [] and out["auto_resolved"] == []
