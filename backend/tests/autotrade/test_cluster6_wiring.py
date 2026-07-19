"""SPRINT CLUSTER 6 ITEM 2 — the five money-losing events are wired to
send_urgent (deduped, LIVE-only).

Each page_* helper in alert_monitor IS the wire (it routes a detected condition
into alerts.send_urgent_deduped). The direct tests prove each fires exactly ONE
deduped page of the right kind and is INERT in paper (is_live=False).

MUTATION (verified) per condition: delete the alerts.send_urgent_deduped(...) call
inside the corresponding alert_monitor.page_X → that condition's test finds 0 rows
→ fails.

MUTATION (end-to-end): remove the `_am6.page_recon_divergences(...)` call in
session.tick → test_tick_pages_recon_divergence finds no UNATTRIBUTED_CLOSE alert
row → fails.
"""
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade import alerts
from autotrade.config import TradingSessionConfig
from autotrade.monitoring import alert_monitor
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


def _count(kind):
    with falcon_conn() as con:
        return con.execute("SELECT COUNT(*) AS n FROM autotrade_alerts WHERE kind=?",
                           (kind,)).fetchone()["n"]


# ── (a) EXIT_FAILED / KILLING_INCOMPLETE ─────────────────────────────────────
def test_page_exit_failed(clean_positions):
    ef = [{"symbol": "FOO"}]
    alert_monitor.page_exit_failed("s1", ef, killing_incomplete=False, is_live=True)
    alert_monitor.page_exit_failed("s1", ef, killing_incomplete=False, is_live=True)
    assert _count("EXIT_FAILED") == 1                      # deduped → exactly one
    # paper (is_live False) never pages.
    alert_monitor.page_exit_failed("s2", [{"symbol": "BAR"}],
                                   killing_incomplete=False, is_live=False)
    assert _count("EXIT_FAILED") == 1
    # KILLING_INCOMPLETE is its own page.
    alert_monitor.page_exit_failed("s1", [], killing_incomplete=True, is_live=True)
    assert _count("KILLING_INCOMPLETE") == 1


# ── (b) reconcile divergence (UNATTRIBUTED_CLOSE / ORPHAN / CORP_ACTION) ──────
def test_page_recon_divergences(clean_positions):
    actions = [{"action": "UNATTRIBUTED_CLOSE", "symbol": "X", "product": "CNC"},
               {"action": "CLOSED_RECONCILED", "symbol": "Y"}]  # NOT a divergence
    alert_monitor.page_recon_divergences("s1", actions, is_live=True)
    alert_monitor.page_recon_divergences("s1", actions, is_live=True)  # dedup
    assert _count("UNATTRIBUTED_CLOSE") == 1
    assert _count("CLOSED_RECONCILED") == 0        # a benign close is never paged
    alert_monitor.page_recon_divergences("s2", actions, is_live=False)  # paper
    assert _count("UNATTRIBUTED_CLOSE") == 1


def _unacked(kind):
    with falcon_conn() as con:
        return con.execute(
            "SELECT id FROM autotrade_alerts WHERE kind=? AND acknowledged=0 "
            "ORDER BY id", (kind,)).fetchall()


def test_corp_action_storm_pages_once_per_symbol_ratio(clean_positions):
    """REGRESSION (2026-07-18 storm): the reconciler emitted CORP_ACTION_SUSPECTED
    every ~5s reconcile for FOUR CNC symbols all reading broker_held ≈ 2× db_held
    (HFCL/MAPMYINDIA/KALYANKJIL/BIOCON) → 306 unacked urgent pages. The wire must
    now page ONCE per (symbol, ratio), regardless of how many cycles re-detect it."""
    storm = [
        {"action": "CORP_ACTION_SUSPECTED", "symbol": "HFCL", "product": "CNC",
         "ratio": 2.0, "broker_held": 922, "db_held": 461},
        {"action": "CORP_ACTION_SUSPECTED", "symbol": "MAPMYINDIA", "product": "CNC",
         "ratio": 2.0, "broker_held": 168, "db_held": 84},
        {"action": "CORP_ACTION_SUSPECTED", "symbol": "KALYANKJIL", "product": "CNC",
         "ratio": 2.0, "broker_held": 366, "db_held": 183},
        {"action": "CORP_ACTION_SUSPECTED", "symbol": "BIOCON", "product": "CNC",
         "ratio": 2.0, "broker_held": 456, "db_held": 228},
    ]
    # 50 reconcile cycles (the storm ran for days) → still exactly 4 pages, one per
    # symbol. Pre-fix this asserted 200.
    for _ in range(50):
        alert_monitor.page_recon_divergences("sStorm", storm, is_live=True)
    assert _count("CORP_ACTION_SUSPECTED") == 4
    assert len(_unacked("CORP_ACTION_SUSPECTED")) == 4

    # A GENUINE new corporate action — a DIFFERENT symbol at a real ratio — is a
    # distinct incident and still surfaces exactly once (detection NOT weakened).
    real = [{"action": "CORP_ACTION_SUSPECTED", "symbol": "GENUINE", "product": "CNC",
             "ratio": 3.0, "broker_held": 300, "db_held": 100}]
    alert_monitor.page_recon_divergences("sStorm", real, is_live=True)
    alert_monitor.page_recon_divergences("sStorm", real, is_live=True)  # re-detect
    assert _count("CORP_ACTION_SUSPECTED") == 5   # +1, and only +1

    # A DIFFERENT ratio on an already-suspected symbol is a NEW suspicion (a fresh
    # action), so it is NOT swallowed by the prior HFCL 2.0 incident.
    hfcl3 = [{"action": "CORP_ACTION_SUSPECTED", "symbol": "HFCL", "product": "CNC",
              "ratio": 3.0, "broker_held": 1383, "db_held": 461}]
    alert_monitor.page_recon_divergences("sStorm", hfcl3, is_live=True)
    assert _count("CORP_ACTION_SUSPECTED") == 6

    # Paper never pages.
    alert_monitor.page_recon_divergences("sPaper", storm, is_live=False)
    assert _count("CORP_ACTION_SUSPECTED") == 6


def test_corp_action_rearms_after_ack(clean_positions):
    """Page-once-until-cleared: while an unacked suspicion is open OR still inside
    the dedup window the storm stays silenced; after the operator ACKS it (clears)
    AND the window elapses, an unchanged re-detection re-arms EXACTLY one reminder
    page (the divergence is real and still unresolved). alerts.py evaluates the
    window against the real wall-clock, so we drive the elapse by back-dating the
    persisted row rather than a fake clock."""
    import os
    os.environ["FALCON_AUTOTRADE_CORP_ACTION_DEDUP_SEC"] = "1"
    try:
        act = [{"action": "CORP_ACTION_SUSPECTED", "symbol": "HFCL",
                "product": "CNC", "ratio": 2.0, "broker_held": 922, "db_held": 461}]
        # First detection → one page.
        alert_monitor.page_recon_divergences("sAck", act, is_live=True)
        assert _count("CORP_ACTION_SUSPECTED") == 1
        # Re-detected immediately: unacked + in-window → suppressed.
        alert_monitor.page_recon_divergences("sAck", act, is_live=True)
        assert _count("CORP_ACTION_SUSPECTED") == 1
        # Operator acks the open incident (the 'clear').
        for r in _unacked("CORP_ACTION_SUSPECTED"):
            alerts.acknowledge(r["id"])
        assert len(_unacked("CORP_ACTION_SUSPECTED")) == 0
        # Back-date the acked row well past the 1s window (simulate next day).
        old_ts = (datetime.now(IST) - timedelta(hours=25)).isoformat()
        with falcon_conn() as con:
            con.execute("UPDATE autotrade_alerts SET ts=? WHERE kind=?",
                        (old_ts, "CORP_ACTION_SUSPECTED"))
            con.commit()
        # Now acked AND out-of-window → re-arms exactly one reminder page.
        alert_monitor.page_recon_divergences("sAck", act, is_live=True)
        assert _count("CORP_ACTION_SUSPECTED") == 2
    finally:
        os.environ.pop("FALCON_AUTOTRADE_CORP_ACTION_DEDUP_SEC", None)


# ── (c) reconcile-staleness ──────────────────────────────────────────────────
def test_page_reconcile_stale(clean_positions):
    alert_monitor.page_reconcile_stale("s1", 500.0, is_running=True,
                                       in_market_hours=True, is_live=True,
                                       bound_sec=120)
    assert _count("RECONCILE_STALE") == 1
    # not running → no page.
    alert_monitor.page_reconcile_stale("s2", 500.0, is_running=False,
                                       in_market_hours=True, is_live=True,
                                       bound_sec=120)
    # within bound → no page.
    alert_monitor.page_reconcile_stale("s3", 10.0, is_running=True,
                                       in_market_hours=True, is_live=True,
                                       bound_sec=120)
    # outside market hours → no page.
    alert_monitor.page_reconcile_stale("s4", 500.0, is_running=True,
                                       in_market_hours=False, is_live=True,
                                       bound_sec=120)
    assert _count("RECONCILE_STALE") == 1


# ── (d) mark-staleness ───────────────────────────────────────────────────────
def test_page_mark_stale(clean_positions):
    alert_monitor.page_mark_stale("s1", 90000, mark_stale_flag=False,
                                  is_live=True, bound_ms=60000)
    assert _count("MARK_STALE") == 1
    # the C5 abstain flag pages even with a small age.
    alert_monitor.page_mark_stale("s2", 100, mark_stale_flag=True,
                                  is_live=True, bound_ms=60000)
    assert _count("MARK_STALE") == 2
    # fresh + no flag → no page.
    alert_monitor.page_mark_stale("s3", 100, mark_stale_flag=False,
                                  is_live=True, bound_ms=60000)
    assert _count("MARK_STALE") == 2


# ── (e) RMS daily-loss breaker ───────────────────────────────────────────────
def test_page_breaker(clean_positions):
    alert_monitor.page_breaker("s1", {"reason": "aggregate loss"}, is_live=True)
    alert_monitor.page_breaker("s1", {"reason": "aggregate loss"}, is_live=True)
    assert _count("DAILY_LOSS_BREAKER") == 1
    alert_monitor.page_breaker("s2", None, is_live=True)          # no breaker
    alert_monitor.page_breaker("s3", {"reason": "x"}, is_live=False)  # paper
    assert _count("DAILY_LOSS_BREAKER") == 1


# ── END-TO-END through session.tick(): a reconcile UNATTRIBUTED_CLOSE pages ───
def _mk_live_session(monkeypatch, net_book):
    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False,
                          ltps={"A": 100.0, "B": 100.0, "C": 100.0},
                          net_book=net_book)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="MIS")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    for sym in ("A", "B", "C"):
        sess.registry.register(symbol=sym, broker_profile=prof, qty=100,
                               avg_price=100.0, product="MIS",
                               instrument_type="EQ", exchange="NSE")
        sess.registry.update_ltp(sym, 100.0, broker_profile=prof)
    sess.monitor.freeze_invested_basis()
    # Mark RUNNING so tick services it as a live running session.
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET status='RUNNING' WHERE session_id=?",
                    (sess.session_id,))
        con.commit()
    return sess


def test_tick_pages_recon_divergence(clean_positions, monkeypatch):
    import asyncio
    # A,B in sync (100==100); C shows 90 at the broker (a 10-share deficit that is
    # NOT a clean corp-action ratio) with NO close-side evidence and no order-id →
    # the reconciler emits UNATTRIBUTED_CLOSE (no mutation).
    net_book = {
        "A": {"tradingsymbol": "A", "product": "MIS", "quantity": 100,
              "average_price": 100.0, "exchange": "NSE"},
        "B": {"tradingsymbol": "B", "product": "MIS", "quantity": 100,
              "average_price": 100.0, "exchange": "NSE"},
        "C": {"tradingsymbol": "C", "product": "MIS", "quantity": 90,
              "average_price": 100.0, "exchange": "NSE"},
    }
    sess = _mk_live_session(monkeypatch, net_book)
    res = asyncio.run(sess.tick())
    divergences = [a for a in (res.get("broker_reconciled") or [])
                   if a.get("action") == "UNATTRIBUTED_CLOSE"]
    assert divergences, "expected an UNATTRIBUTED_CLOSE from the reconcile"
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT COUNT(*) AS n FROM autotrade_alerts "
            "WHERE kind='UNATTRIBUTED_CLOSE' AND session_id=?",
            (sess.session_id,)).fetchone()
    assert rows["n"] == 1        # the tick wired the divergence to a page
