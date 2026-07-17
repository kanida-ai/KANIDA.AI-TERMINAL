"""ALERT AUTO-TRIAGE — TIER 2 gates driven THROUGH reconcile_broker_positions().

The helper-level tests live in test_alert_auto_triage.py. These drive the REAL
reconciler end-to-end, because a gate that works in isolation but is wired into
the wrong branch is worth nothing.

Harness mirrors test_recon_matrix.py: MockBroker, a frozen IST clock on a known
trading day, a LIVE (dry_run=False) session, seed rows, ONE reconcile, assert.
Paper is byte-for-byte unchanged; falcon_position_state is NEVER touched.
"""
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from autotrade.monitoring import position_reconciler as pr
from autotrade.monitoring.position_reconciler import reconcile_broker_positions
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock_and_flag(monkeypatch, clean_positions):
    set_fake_now(OPEN_NOW)
    monkeypatch.delenv("FALCON_AUTOTRADE_AUTO_TRIAGE", raising=False)
    pr.reset_divergence_streaks()
    with falcon_conn() as con:
        con.execute("DELETE FROM autotrade_order_events")
        con.execute("DELETE FROM autotrade_recon_alerts")
        con.commit()
    yield
    set_fake_now(None)
    pr.reset_divergence_streaks()
    with falcon_conn() as con:
        con.execute("DELETE FROM autotrade_order_events")
        con.execute("DELETE FROM autotrade_recon_alerts")
        con.commit()


def _on(monkeypatch):
    monkeypatch.setenv("FALCON_AUTOTRADE_AUTO_TRIAGE", "true")


def _patch_brokers(monkeypatch, factory):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = factory(profile)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _live_session(monkeypatch, *, net_book, ltps, product="MIS"):
    """A LIVE session whose broker reports `net_book`."""
    _patch_brokers(monkeypatch, lambda p: MockBroker(
        profile=p, dry_run=False, ltps=ltps, net_book=net_book, holdings=[],
        orders=[]))
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               kill_switch_pct=0.02, order_product=product)
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _net_row(symbol, qty, *, product="MIS", buy_q=0, sell_q=0, exch="NSE"):
    return {"tradingsymbol": symbol, "exchange": exch, "product": product,
            "quantity": qty, "buy_quantity": buy_q, "sell_quantity": sell_q,
            "last_price": 100.0}


def _mk_exit_placed(session_id, symbol, qty, *, order_id, product="MIS",
                    profile, age_sec=5):
    """Seed an in-flight EXIT_PLACED ledger event `age_sec` seconds old.

    NOTE the timestamp is REAL-now-relative, not fake-clock-relative, and that is
    deliberate: order_ledger.append_event() stamps ts with the REAL wall clock
    (_now_iso()), and _inflight_exits_for() measures the in-flight window against
    the REAL clock too. The fake clock governs the trading-day FIRE gate, not the
    ledger. Stamping these rows with OPEN_NOW would make them a month old and
    silently fall outside the in-flight window — the test would pass/fail for a
    reason that never occurs in production."""
    ts = datetime.now(IST) - timedelta(seconds=age_sec)
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_order_events
               (ts, session_id, symbol, product, broker_profile,
                broker_order_id, client_order_id, event_type, qty, source)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (ts.isoformat(), session_id, symbol, product, profile,
             order_id, f"COID-{symbol}", "EXIT_PLACED", qty, "exit"))
        con.commit()


def _recon_alerts():
    with falcon_conn() as con:
        return [dict(r) for r in con.execute(
            "SELECT * FROM autotrade_recon_alerts ORDER BY id ASC").fetchall()]


def _acts(actions, kind):
    return [a for a in actions if a.get("action") == kind]


# ═════════════════════════════════════════════════════════════════════════════
# QUIET PERIOD — through the real reconciler
# ═════════════════════════════════════════════════════════════════════════════
def test_quiet_period_suppresses_a_transient_deficit_then_fires_when_persistent(
        monkeypatch):
    """THE CORE TIER-2 ASSERTION. An unexplained deficit (db 100, broker 60) is
    NOT alerted on cycle 1 — the entry-fill and iceberg races all self-heal well
    inside the quiet period. It IS alerted on cycle 2, because a REAL divergence
    is still there.

    MUTATION: remove the quiet-period gate → cycle 1 alerts → this FAILS."""
    _on(monkeypatch)
    # broker holds 60, we track 100, no sell evidence → an unexplained deficit
    # that cannot be auto-booked (no closing-side volume).
    sess = _live_session(monkeypatch, ltps={"BIOCON": 100.0},
                         net_book=[_net_row("BIOCON", 60, buy_q=60)])
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="BIOCON", broker_profile=prof, qty=100,
                           avg_price=100.0, product="MIS", instrument_type="EQ")

    # ── CYCLE 1: quiet period — no alert.
    a1 = reconcile_broker_positions(sess)
    assert _acts(a1, "DEFICIT_QUIET_PERIOD"), "cycle 1 must be held quiet"
    assert _acts(a1, "UNATTRIBUTED_CLOSE") == []
    assert _recon_alerts() == [], "no alert row on cycle 1"

    # ── CYCLE 2: still diverging → it is REAL → alert.
    a2 = reconcile_broker_positions(sess)
    assert _acts(a2, "UNATTRIBUTED_CLOSE"), "a PERSISTENT deficit MUST alert"
    assert _acts(a2, "DEFICIT_QUIET_PERIOD") == []
    rows = _recon_alerts()
    assert len(rows) == 1 and rows[0]["kind"] == "UNATTRIBUTED_CLOSE"
    assert rows[0]["symbol"] == "BIOCON"
    # The position was NEVER mutated by the quiet period (flag/alert only).
    row = [p for p in sess.registry.get_all_positions()
           if p["symbol"] == "BIOCON"][0]
    assert row["status"] == "OPEN" and row["qty"] == 100


def test_flag_off_alerts_on_cycle_1_exactly_as_today(monkeypatch):
    """FLAG-OFF BYTE-IDENTICAL: the SAME divergence alerts immediately on cycle
    1, with no quiet period and no triage actions at all."""
    sess = _live_session(monkeypatch, ltps={"BIOCON": 100.0},
                         net_book=[_net_row("BIOCON", 60, buy_q=60)])
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="BIOCON", broker_profile=prof, qty=100,
                           avg_price=100.0, product="MIS", instrument_type="EQ")

    a1 = reconcile_broker_positions(sess)

    assert _acts(a1, "UNATTRIBUTED_CLOSE"), "flag OFF must alert on cycle 1"
    assert _acts(a1, "DEFICIT_QUIET_PERIOD") == []
    assert _acts(a1, "DEFICIT_INFLIGHT_EXIT_SUPPRESSED") == []
    assert len(_recon_alerts()) == 1


# ═════════════════════════════════════════════════════════════════════════════
# IN-FLIGHT EXIT AWARENESS + SELLING-SESSION ATTRIBUTION
# ═════════════════════════════════════════════════════════════════════════════
def test_deficit_from_another_sessions_inflight_exit_is_suppressed_and_attributed(
        monkeypatch):
    """THE ALERTS 21-24 SHAPE, through the real reconciler.

    Session d9b218cf reconciles and sees a 40-share deficit on MAPMYINDIA. It
    never sold anything — session 1aeb11b8's FIRST ICEBERG EXIT CHILD (exactly
    40) is working at the broker right now. The old code blamed d9b218cf (first
    in iteration order). The gate must SUPPRESS the alert and name 1aeb11b8.

    MUTATION: remove the in-flight gate → an UNATTRIBUTED_CLOSE is raised against
    d9b218cf → this FAILS."""
    _on(monkeypatch)
    # Broker: account net 60 (100 tracked − 40 sold by the in-flight child).
    sess = _live_session(monkeypatch, ltps={"MAPMYINDIA": 1180.0},
                         net_book=[_net_row("MAPMYINDIA", 60, buy_q=100,
                                            sell_q=40)])
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="MAPMYINDIA", broker_profile=prof, qty=100,
                           avg_price=1180.0, product="MIS",
                           instrument_type="EQ")
    # The OTHER session's iceberg exit child, in flight (no terminal event).
    _mk_exit_placed("1aeb11b8", "MAPMYINDIA", 40, order_id="OID-ICE-1",
                    profile=prof)

    actions = reconcile_broker_positions(sess)

    sup = _acts(actions, "DEFICIT_INFLIGHT_EXIT_SUPPRESSED")
    assert sup, "an in-flight exit MUST suppress the deficit alert"
    assert sup[0]["deficit"] == 40
    assert sup[0]["inflight_qty"] == 40
    # ATTRIBUTION: named to the SELLER, never to the reconciling session.
    assert sup[0]["selling_sessions"] == ["1aeb11b8"]
    assert sess.session_id not in sup[0]["selling_sessions"]
    # No alert, and — critically — no partial-external-close mutation booked
    # against THIS session's lot for another session's sell.
    assert _acts(actions, "UNATTRIBUTED_CLOSE") == []
    assert _acts(actions, "PARTIAL_EXTERNAL_CLOSE") == []
    assert _recon_alerts() == []
    row = [p for p in sess.registry.get_all_positions()
           if p["symbol"] == "MAPMYINDIA"][0]
    assert row["status"] == "OPEN" and row["qty"] == 100


def test_deficit_larger_than_inflight_still_reaches_the_normal_path(monkeypatch):
    """NO-OVER-SUPPRESSION: an in-flight exit of 40 does NOT excuse a deficit of
    70. The unexplained remainder must still be processed normally (and, being
    persistent, alert)."""
    _on(monkeypatch)
    sess = _live_session(monkeypatch, ltps={"MAPMYINDIA": 1180.0},
                         net_book=[_net_row("MAPMYINDIA", 30, buy_q=100,
                                            sell_q=70)])
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="MAPMYINDIA", broker_profile=prof, qty=100,
                           avg_price=1180.0, product="MIS",
                           instrument_type="EQ")
    _mk_exit_placed("1aeb11b8", "MAPMYINDIA", 40, order_id="OID-ICE-2",
                    profile=prof)

    a1 = reconcile_broker_positions(sess)
    assert _acts(a1, "DEFICIT_INFLIGHT_EXIT_SUPPRESSED") == [], \
        "40 in flight must NOT suppress a 70 deficit"


def test_terminal_inflight_exit_no_longer_suppresses(monkeypatch):
    """NO-FIRE→FIRE: once the exit FILLED it is resolved, so it can no longer
    suppress. The deficit goes back to the normal (quiet-period) path."""
    _on(monkeypatch)
    sess = _live_session(monkeypatch, ltps={"MAPMYINDIA": 1180.0},
                         net_book=[_net_row("MAPMYINDIA", 60, buy_q=100,
                                            sell_q=40)])
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="MAPMYINDIA", broker_profile=prof, qty=100,
                           avg_price=1180.0, product="MIS",
                           instrument_type="EQ")
    _mk_exit_placed("1aeb11b8", "MAPMYINDIA", 40, order_id="OID-ICE-3",
                    profile=prof, age_sec=30)
    with falcon_conn() as con:      # the exit reached a TERMINAL state
        con.execute(
            """INSERT INTO autotrade_order_events
               (ts, session_id, symbol, product, broker_profile,
                broker_order_id, event_type, qty, source)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (OPEN_NOW.isoformat(), "1aeb11b8", "MAPMYINDIA", "MIS", prof,
             "OID-ICE-3", "EXIT_FILLED", 40, "exit"))
        con.commit()

    a1 = reconcile_broker_positions(sess)
    assert _acts(a1, "DEFICIT_INFLIGHT_EXIT_SUPPRESSED") == []


# ═════════════════════════════════════════════════════════════════════════════
# CORP ACTION — a ratio is not evidence
# ═════════════════════════════════════════════════════════════════════════════
def test_eclerx_no_longer_raises_corp_action_through_the_reconciler(monkeypatch):
    """THE ECLERX ALERT, reproduced end-to-end and then killed.

    db 1465 vs broker 498 → 1465/498 = 2.9417, which the ±2% window (0.06 at
    R=3.0) accepted as a "1:3 reverse split" BY 0.0017. Under the flag the strict
    ±0.2% window rejects it, and the divergence is classified as what it actually
    is — an unattributed deficit — after the quiet period.

    MUTATION: revert to _CORP_ACTION_TOL on the triage path → a
    CORP_ACTION_SUSPECTED is raised → this FAILS."""
    _on(monkeypatch)
    sess = _live_session(monkeypatch, ltps={"ECLERX": 3000.0},
                         net_book=[_net_row("ECLERX", 498, buy_q=498)])
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="ECLERX", broker_profile=prof, qty=1465,
                           avg_price=3000.0, product="MIS",
                           instrument_type="EQ")

    reconcile_broker_positions(sess)            # cycle 1 — quiet period
    a2 = reconcile_broker_positions(sess)       # cycle 2 — persistent

    assert _acts(a2, "CORP_ACTION_SUSPECTED") == [], \
        "a 0.0017 coincidence must NOT be classified as a corporate action"
    assert _acts(a2, "UNATTRIBUTED_CLOSE"), \
        "it is an unattributed deficit and must be reported as one"
    kinds = {r["kind"] for r in _recon_alerts()}
    assert "CORP_ACTION_SUSPECTED" not in kinds
    assert "UNATTRIBUTED_CLOSE" in kinds


def test_eclerx_DOES_raise_corp_action_with_the_flag_off(monkeypatch):
    """FLAG-OFF BYTE-IDENTICAL: the noisy ECLERX classification is EXACTLY
    preserved when the flag is off. This test documents the bug that Tier 2
    fixes — and proves flag-off changes nothing."""
    sess = _live_session(monkeypatch, ltps={"ECLERX": 3000.0},
                         net_book=[_net_row("ECLERX", 498, buy_q=498)])
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="ECLERX", broker_profile=prof, qty=1465,
                           avg_price=3000.0, product="MIS",
                           instrument_type="EQ")

    a1 = reconcile_broker_positions(sess)

    ca = _acts(a1, "CORP_ACTION_SUSPECTED")
    assert ca, "flag OFF must reproduce the original (noisy) behaviour"
    assert ca[0]["ratio"] == round(1 / 3, 4)
    assert ca[0]["broker_held"] == 498 and ca[0]["db_held"] == 1465


def test_a_real_split_still_classifies_through_the_reconciler(monkeypatch):
    """NO-FALSE-NEGATIVE: a GENUINE ×3 bonus (1465 → 4395, exact) still
    classifies as CORP_ACTION_SUSPECTED under the strict tolerance — after
    proving itself stable across the quiet period."""
    _on(monkeypatch)
    sess = _live_session(monkeypatch, ltps={"BONUSCO": 100.0},
                         net_book=[_net_row("BONUSCO", 4395, buy_q=4395)])
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="BONUSCO", broker_profile=prof, qty=1465,
                           avg_price=100.0, product="MIS",
                           instrument_type="EQ")

    reconcile_broker_positions(sess)            # cycle 1 — quiet period
    a2 = reconcile_broker_positions(sess)       # cycle 2 — stable

    ca = _acts(a2, "CORP_ACTION_SUSPECTED")
    assert ca, "a REAL, exact, stable corporate action must still surface"
    assert ca[0]["ratio"] == 3.0
    # Still NON-mutating (the invariant the reconciler has always held).
    row = [p for p in sess.registry.get_all_positions()
           if p["symbol"] == "BONUSCO"][0]
    assert row["status"] == "OPEN" and row["qty"] == 1465


def test_corp_action_rejected_while_an_exit_is_in_flight(monkeypatch):
    """A qty that moves while OUR OWN exit is working moved for a reason that is
    not a corporate action. Never classify a corp action over an in-flight exit."""
    _on(monkeypatch)
    sess = _live_session(monkeypatch, ltps={"BONUSCO": 100.0},
                         net_book=[_net_row("BONUSCO", 4395, buy_q=4395)])
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="BONUSCO", broker_profile=prof, qty=1465,
                           avg_price=100.0, product="MIS",
                           instrument_type="EQ")
    _mk_exit_placed("other-sess", "BONUSCO", 10, order_id="OID-IF",
                    profile=prof)

    reconcile_broker_positions(sess)
    a2 = reconcile_broker_positions(sess)

    assert _acts(a2, "CORP_ACTION_SUSPECTED") == []


# ═════════════════════════════════════════════════════════════════════════════
# THE MUTATING PATHS MUST BE UNTOUCHED
# ═════════════════════════════════════════════════════════════════════════════
def test_order_id_confirmed_close_still_closes_under_the_flag(monkeypatch):
    """REGRESSION GUARD: Tier 2 gates only the ALERT. A close backed by POSITIVE
    order-id evidence must still close on cycle 1, flag on — the quiet period
    must never delay a real, evidenced close."""
    _on(monkeypatch)
    _patch_brokers(monkeypatch, lambda p: MockBroker(
        profile=p, dry_run=False, ltps={"GONE": 96.0},
        net_book=[_net_row("GONE", 0, product="CNC", buy_q=10, sell_q=10)],
        holdings=[], orders=[],
        order_status={"O-GONE": {"status": "COMPLETE", "filled_quantity": 10,
                                 "average_price": 96.0}}))
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               kill_switch_pct=0.02, order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="GONE", broker_profile=prof, qty=10,
                           avg_price=100.0, product="CNC", instrument_type="EQ")
    sess.registry.mark_exit_failed("GONE", "boom", broker_profile=prof)
    with falcon_conn() as con:      # our OWN exit order-id, confirmed COMPLETE
        con.execute("UPDATE autotrade_positions SET exit_order_id='O-GONE', "
                    "status='EXIT_FAILED' WHERE session_id=? AND symbol='GONE'",
                    (sess.session_id,))
        con.commit()

    actions = reconcile_broker_positions(sess)

    assert _acts(actions, "CLOSED_RECONCILED"), \
        "an order-id-evidenced close must NOT be delayed by the quiet period"
    row = [p for p in sess.registry.get_all_positions()
           if p["symbol"] == "GONE"][0]
    assert row["status"] == "CLOSED"


def test_in_sync_book_produces_no_triage_actions(monkeypatch):
    """A healthy, in-sync book produces nothing at all — the gates never fire on
    a non-divergence (no streak state, no suppression, no alert)."""
    _on(monkeypatch)
    sess = _live_session(monkeypatch, ltps={"BIOCON": 100.0},
                         net_book=[_net_row("BIOCON", 100, buy_q=100)])
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="BIOCON", broker_profile=prof, qty=100,
                           avg_price=100.0, product="MIS", instrument_type="EQ")

    assert reconcile_broker_positions(sess) == []
    assert reconcile_broker_positions(sess) == []
    assert _recon_alerts() == []


def test_paper_session_never_reconciles_under_the_flag(monkeypatch):
    """PAPER BYTE-IDENTICAL: the flag does not open any new path for a dry_run
    session — the reconciler still returns [] immediately."""
    _on(monkeypatch)
    _patch_brokers(monkeypatch, lambda p: MockBroker(
        profile=p, dry_run=True, ltps={"BIOCON": 100.0},
        net_book=[_net_row("BIOCON", 60, buy_q=60)], holdings=[], orders=[]))
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               kill_switch_pct=0.02, order_product="MIS")
    sess = TradingSession.create(cfg, mode="paper")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="BIOCON", broker_profile=prof, qty=100,
                           avg_price=100.0, product="MIS", instrument_type="EQ")

    assert reconcile_broker_positions(sess) == []
    assert _recon_alerts() == []
