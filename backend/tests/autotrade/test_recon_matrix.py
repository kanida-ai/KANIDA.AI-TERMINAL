"""RECONCILIATION FRAMEWORK — Phase 6 COMPLETE TEST MATRIX (fill the gaps).

This module adds a focused, MODE-NAMED test for each 30-mode-catalog failure mode
that the existing suite safeguards but did NOT yet exercise with a dedicated
assertion. Modes already covered by a focused test elsewhere are NOT duplicated
here (see the P6 coverage table in the task report for the full mode → safeguard →
test map). Every test below FAILS if its safeguard is removed — it asserts the
SAFEGUARDED behaviour, not a tautology.

Harness mirrors test_broker_reconciliation.py / test_phase5_guards.py: MockBroker,
a frozen IST clock on a known trading day, a LIVE (dry_run=False) session where
the mode is live-only, seed rows, ONE action, assert. Paper is byte-for-byte
unchanged; falcon_position_state is NEVER touched.

Modes filled here:
  A Entry     : A1 pending-not-filled, A2 partial-at-entry, A5 poll-timeout
  B Hold      : B3 manual/external exit (pre-exit reconcile flat)
  D Exit      : D1 exit rejected, D2 confirm missed (TIMEOUT), D4 exit price,
                D5 partial exit
  E Infra     : E1 API error, E6 duplicate order
  F State/P&L : F1 stale basis (refreeze)
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession, set_fake_now, _exit_single_position
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.exit_poller import confirm_exit
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from autotrade.broker.base import OrderResult
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)  # Thu, mid-session


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _prof(pid="zer", product="MIS", instrument_type="EQ"):
    return BrokerProfile(profile_id=pid, broker_name="zerodha",
                         allocated_capital=300000.0, order_product=product,
                         instrument_type=instrument_type)


def _patch_brokers(monkeypatch, factory):
    """Patch build_client (both router + session) to `factory(profile)`."""
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = factory(profile)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _row(session_id, symbol):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT status, qty, avg_price, ltp, exit_price, close_reason, "
            "realised_pnl, exit_order_id, exit_lock, exit_initiated_by "
            "FROM autotrade_positions WHERE session_id=? AND symbol=?",
            (session_id, symbol)).fetchone()
    return dict(r) if r else None


def _n_rows(session_id, symbol):
    with falcon_conn() as con:
        return con.execute(
            "SELECT COUNT(*) FROM autotrade_positions "
            "WHERE session_id=? AND symbol=?", (session_id, symbol)).fetchone()[0]


# ══════════════════════════════════════════════════════════════════════════════
# A ENTRY
# ══════════════════════════════════════════════════════════════════════════════

def test_a1_pending_not_filled_falls_back_to_mark(clean_positions, monkeypatch):
    """A1 pending-not-filled: a LIVE entry that returns an order_id but no fill,
    and whose status never confirms within the poll window, falls back to the
    reference MARK (never drops a real order silently, never invents a fill). The
    position registers at the mark qty/price and the session runs. Safeguard:
    session._place_one fill-reconcile fallback (session.py ~1690)."""
    class _PendingBroker(MockBroker):
        async def place_order(self, order):
            self.placed.append(order)
            # order_id issued, NO fill numbers (a live PLACED market order).
            return OrderResult(status="PLACED", broker_order_id="ord-" + order.symbol,
                               symbol=order.symbol, qty=order.qty,
                               filled_qty=0, avg_price=None)

        def get_order_status(self, order_id):
            # Never COMPLETE, never REJECTED → the poll times out → None → mark.
            return {"status": "OPEN", "filled_quantity": 0, "average_price": 0.0}

    _patch_brokers(monkeypatch, lambda p: _PendingBroker(
        profile=p, dry_run=False, ltps={"A": 100.0}))
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="MIS",
                               per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="live")
    # Speed up the poll so the test doesn't wait 8s.
    monkeypatch.setattr(sess, "_reconcile_entry_fill",
                        lambda *a, **k: _async_none())
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 1
    r = _row(sess.session_id, "A")
    assert r is not None and r["status"] == "OPEN"
    # Fell back to the mark: qty = sized qty, avg_price = the 100.0 reference.
    assert r["qty"] > 0
    assert r["avg_price"] == pytest.approx(100.0)


async def _async_none():
    return None


def test_a2_partial_fill_at_entry_registers_filled_qty(clean_positions,
                                                        monkeypatch):
    """A2 partial: a PARTIAL entry fill registers the FILLED qty (not the ordered
    qty, not 0), attributable by entry_order_id. Safeguard: _place_one PARTIAL
    branch → registry.register_partial (session.py ~1714)."""
    _patch_brokers(monkeypatch, lambda p: MockBroker(
        profile=p, dry_run=False, ltps={"A": 100.0},
        partial_fills={"A": 3}))          # ordered N, only 3 fill
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="MIS",
                               per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="live")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    r = _row(sess.session_id, "A")
    assert r is not None and r["status"] == "OPEN"
    assert r["qty"] == 3               # the REAL fill, not the ordered qty / 0
    with falcon_conn() as con:
        eoid = con.execute(
            "SELECT entry_order_id FROM autotrade_positions "
            "WHERE session_id=? AND symbol=?",
            (sess.session_id, "A")).fetchone()[0]
    assert eoid == "ord-A"             # attributable by order-id


def test_a5_poll_timeout_returns_none_caller_uses_mark(clean_positions,
                                                       monkeypatch):
    """A5 poll-timeout: _reconcile_entry_fill polls, never sees COMPLETE/REJECTED
    within max_wait_sec, and returns None (so the caller falls back to the mark —
    NEVER invents a fill price). Safeguard: session._reconcile_entry_fill deadline
    branch (session.py ~1788)."""
    _patch_brokers(monkeypatch, lambda p: MockBroker(profile=p, dry_run=False))
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1)
    sess = TradingSession.create(cfg, mode="live")

    class _StuckBroker:
        def __init__(self):
            self.calls = 0
        def get_order_status(self, order_id):
            self.calls += 1
            return {"status": "OPEN", "filled_quantity": 0, "average_price": 0.0}

    b = _StuckBroker()
    rec = asyncio.run(sess._reconcile_entry_fill(b, "ORD-STUCK", 10,
                                                 max_wait_sec=0.3,
                                                 poll_interval=0.05))
    assert rec is None                 # timed out → mark fallback, no invented fill
    assert b.calls >= 1                # the poll DID run (not a no-op)


# ══════════════════════════════════════════════════════════════════════════════
# B HOLD-INTRADAY
# ══════════════════════════════════════════════════════════════════════════════

def test_b3_manual_external_exit_reconciles_flat_places_no_order(clean_positions,
                                                                 monkeypatch):
    """B3 manual exit: the operator (or a fired broker SL) already flattened this
    position at the broker. When our per-stock stop runs, the PRE-EXIT reconcile
    probe (get_net_position_qty == 0) marks the row CLOSED and places NO order —
    never a naked/duplicate sell. Safeguard: _exit_single_position pre-exit
    reconcile guard (session.py ~846)."""
    # Broker reports the symbol FLAT (already closed externally).
    broker = MockBroker(profile=_prof(), dry_run=False, ltps={"X": 105.0},
                        net_positions={"X": 0})
    reg = PositionRegistry("sess-b3", 300000.0)
    reg.register(symbol="X", broker_profile="zer", qty=10, avg_price=100.0,
                 product="MIS", instrument_type="EQ")
    reg.update_ltp("X", 105.0, broker_profile="zer")

    out = asyncio.run(_exit_single_position(
        "sess-b3", reg.get_open_positions()[0], "STOP_STOCK",
        {"zer": broker}, reg, gtt_manager=None))
    assert out["status"] == "RECONCILED_FLAT"
    assert broker.exits == []          # NO order placed
    r = _row("sess-b3", "X")
    assert r["status"] == "CLOSED"
    assert r["close_reason"] == "STOP_STOCK_RECONCILED_FLAT"


# ══════════════════════════════════════════════════════════════════════════════
# D EXIT
# ══════════════════════════════════════════════════════════════════════════════

def test_d1_exit_rejected_marks_exit_failed_and_releases_gate(clean_positions):
    """D1 exit rejected: an exit order the broker REJECTS → mark_exit_failed (the
    position is NOT falsely CLOSED) and the exit_gate is RELEASED so a later tick
    can retry. Safeguard: exit_poller.confirm_exit REJECTED branch
    (exit_poller.py ~124) + registry.mark_exit_failed releasing the lock."""
    reg = PositionRegistry("sess-d1", 300000.0)
    reg.register(symbol="X", broker_profile="zer", qty=10, avg_price=100.0,
                 product="MIS")
    # Claim the gate first (as a real exit path would) so we can assert release.
    from autotrade import exit_gate
    assert exit_gate.claim_exit_session("sess-d1", "X", "STOP_STOCK") is True

    class _RejectBroker:
        def get_order_status(self, order_id):
            return {"status": "REJECTED", "filled_quantity": 0,
                    "average_price": 0.0}

    res = asyncio.run(confirm_exit(
        session_id="sess-d1", symbol="X", order_id="OID-REJ", qty=10,
        broker=_RejectBroker(), registry=reg, close_reason="STOP_STOCK",
        max_wait_sec=5, poll_interval_sec=0.05))
    assert res["status"] == "REJECTED"
    r = _row("sess-d1", "X")
    assert r["status"] == "EXIT_FAILED"      # NOT falsely CLOSED
    assert r["exit_order_id"] == "OID-REJ"
    # Gate released → a retry can re-claim it.
    assert exit_gate.is_locked_session("sess-d1", "X") is False


def test_d2_confirm_missed_timeout_leaves_position_open(clean_positions):
    """D2 confirm missed: the exit order never reaches a terminal state within the
    poll deadline → confirm_exit returns TIMEOUT and makes NO registry mutation
    (the position stays OPEN, no phantom close, no fabricated price). Safeguard:
    exit_poller.confirm_exit deadline branch (exit_poller.py ~143)."""
    reg = PositionRegistry("sess-d2", 300000.0)
    reg.register(symbol="X", broker_profile="zer", qty=10, avg_price=100.0,
                 product="MIS")

    class _PendingBroker:
        def get_order_status(self, order_id):
            return {"status": "OPEN", "filled_quantity": 0, "average_price": 0.0}

    res = asyncio.run(confirm_exit(
        session_id="sess-d2", symbol="X", order_id="OID-PEND", qty=10,
        broker=_PendingBroker(), registry=reg, close_reason="STOP_STOCK",
        max_wait_sec=0.3, poll_interval_sec=0.05))
    assert res["status"] == "TIMEOUT"
    r = _row("sess-d2", "X")
    assert r["status"] == "OPEN"             # untouched — no phantom close
    assert r["exit_price"] is None
    assert r["realised_pnl"] in (None, 0, 0.0)


def test_d4_exit_price_is_real_fill_not_the_mark(clean_positions):
    """D4 exit price: the closed row records the REAL broker fill (avg_price from
    get_order_status), NOT the pre-exit mark/ltp. A wrong exit price mis-states
    realised P&L. Safeguard: confirm_exit COMPLETE branch passes the confirmed
    avg_price to mark_closed (exit_poller.py ~105)."""
    reg = PositionRegistry("sess-d4", 300000.0)
    reg.register(symbol="X", broker_profile="zer", qty=10, avg_price=100.0,
                 product="MIS")
    reg.update_ltp("X", 111.0, broker_profile="zer")   # stale mark = 111

    class _FillBroker:
        def get_order_status(self, order_id):
            # The REAL fill came in at 104.25, NOT the 111 mark.
            return {"status": "COMPLETE", "filled_quantity": 10,
                    "average_price": 104.25}

    res = asyncio.run(confirm_exit(
        session_id="sess-d4", symbol="X", order_id="OID-FILL", qty=10,
        broker=_FillBroker(), registry=reg, close_reason="STOP_STOCK",
        max_wait_sec=5, poll_interval_sec=0.05))
    assert res["status"] == "COMPLETE"
    assert res["exit_price"] == pytest.approx(104.25)
    r = _row("sess-d4", "X")
    assert r["status"] == "CLOSED"
    assert r["exit_price"] == pytest.approx(104.25)     # real fill, not 111
    assert r["realised_pnl"] == pytest.approx((104.25 - 100.0) * 10)
    assert r["exit_order_id"] == "OID-FILL"


def test_d5_partial_exit_reduces_qty_keeps_position_open(clean_positions):
    """D5 partial exit: an exit that fills only part of the qty → update_partial_exit
    reduces the open qty by the filled amount, books the partial realised P&L, and
    leaves the position OPEN with the remaining qty for a follow-up. Safeguard:
    confirm_exit PARTIAL branch → registry.update_partial_exit (exit_poller.py
    ~113 / registry.py ~338)."""
    reg = PositionRegistry("sess-d5", 300000.0)
    reg.register(symbol="X", broker_profile="zer", qty=10, avg_price=100.0,
                 product="MIS")

    class _PartialBroker:
        def get_order_status(self, order_id):
            return {"status": "COMPLETE", "filled_quantity": 4,   # < 10 ordered
                    "average_price": 105.0}

    res = asyncio.run(confirm_exit(
        session_id="sess-d5", symbol="X", order_id="OID-PART", qty=10,
        broker=_PartialBroker(), registry=reg, close_reason="STOP_STOCK",
        max_wait_sec=5, poll_interval_sec=0.05))
    assert res["status"] == "PARTIAL"
    assert res["remaining_qty"] == 6
    r = _row("sess-d5", "X")
    assert r["status"] == "OPEN"                 # still open for the remaining 6
    assert r["qty"] == 6
    assert r["realised_pnl"] == pytest.approx((105.0 - 100.0) * 4)   # filled part


# ══════════════════════════════════════════════════════════════════════════════
# E INFRA
# ══════════════════════════════════════════════════════════════════════════════

def test_e1_api_error_during_confirm_retries_then_times_out_no_mutation(
        clean_positions):
    """E1 API error: get_order_status raising an exception mid-confirm must be
    caught and retried (never crash the exit path); if it keeps erroring the
    confirm returns TIMEOUT and mutates NOTHING. Safeguard: confirm_exit try/except
    around get_order_status (exit_poller.py ~88)."""
    reg = PositionRegistry("sess-e1", 300000.0)
    reg.register(symbol="X", broker_profile="zer", qty=10, avg_price=100.0,
                 product="MIS")

    class _ErrBroker:
        def __init__(self):
            self.calls = 0
        def get_order_status(self, order_id):
            self.calls += 1
            raise RuntimeError("kite API 500")

    b = _ErrBroker()
    res = asyncio.run(confirm_exit(
        session_id="sess-e1", symbol="X", order_id="OID-ERR", qty=10,
        broker=b, registry=reg, close_reason="STOP_STOCK",
        max_wait_sec=0.3, poll_interval_sec=0.05))
    assert res["status"] == "TIMEOUT"       # survived the errors, no crash
    assert b.calls >= 1                     # it DID retry through the error
    r = _row("sess-e1", "X")
    assert r["status"] == "OPEN"            # nothing mutated on API error


def test_e6_duplicate_entry_registration_upserts_single_row(clean_positions):
    """E6 duplicate order: registering the SAME (session, symbol, profile) twice
    (e.g. a retried/duplicated entry event) UPSERTs a SINGLE row keyed by
    (session_id, symbol, broker_profile) — never two phantom rows for one holding.
    Safeguard: registry.register existing-row UPDATE branch (registry.py ~64)."""
    reg = PositionRegistry("sess-e6", 300000.0)
    reg.register(symbol="X", broker_profile="zer", qty=10, avg_price=100.0,
                 product="MIS", entry_order_id="ord-1")
    # A duplicate event for the same holding (idempotent re-register).
    reg.register(symbol="X", broker_profile="zer", qty=10, avg_price=100.0,
                 product="MIS", entry_order_id="ord-1")
    assert _n_rows("sess-e6", "X") == 1     # ONE row, not two phantoms
    # A DIFFERENT broker_profile IS a distinct leg (correctly a separate row).
    reg.register(symbol="X", broker_profile="zer2", qty=5, avg_price=100.0,
                 product="MIS")
    assert _n_rows("sess-e6", "X") == 2


# ══════════════════════════════════════════════════════════════════════════════
# F STATE / P&L
# ══════════════════════════════════════════════════════════════════════════════

def test_f1_stale_basis_refrozen_after_reconciled_close(clean_positions,
                                                        monkeypatch):
    """F1 stale basis: after the reconciler CLOSES one leg on order-id evidence,
    the session invested_basis (the kill/trail denominator) is RE-FROZEN over the
    REMAINING open cash-equity rows — a stale basis would over/under-state the kill
    threshold. Safeguard: position_reconciler.refreeze after a close
    (position_reconciler.py ~566)."""
    from autotrade.monitoring.position_reconciler import reconcile_broker_positions

    # Two CNC legs (KEEP 10*100 + GONE 10*100 = basis 2000). GONE's GTT fired
    # COMPLETE and the broker net drops to 10 (only KEEP remains).
    net_book = {"GONE": {"tradingsymbol": "GONE", "quantity": 0,
                         "buy_quantity": 10, "sell_quantity": 10,
                         "sell_price": 96.0, "average_price": 100.0,
                         "exchange": "NSE", "product": "CNC"},
                "KEEP": {"tradingsymbol": "KEEP", "quantity": 10,
                         "buy_quantity": 10, "sell_quantity": 0,
                         "average_price": 100.0, "exchange": "NSE",
                         "product": "CNC"}}
    gtts = {"G-GONE": {"status": "triggered",
                       "orders": [{"result": {"order_id": "O-GONE"}}]}}
    order_status = {"O-GONE": {"status": "COMPLETE", "filled_quantity": 10,
                               "average_price": 96.0}}

    def factory(p):
        return MockBroker(profile=p, dry_run=False,
                          ltps={"GONE": 96.0, "KEEP": 100.0},
                          net_book=net_book, gtts=gtts,
                          order_status=order_status, holdings={})

    _patch_brokers(monkeypatch, factory)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               kill_switch_pct=0.02, order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="KEEP", broker_profile=prof, qty=10,
                           avg_price=100.0, product="CNC", instrument_type="EQ")
    sess.registry.register(symbol="GONE", broker_profile=prof, qty=10,
                           avg_price=100.0, product="CNC", instrument_type="EQ")
    sess.registry.update_ltp("KEEP", 100.0, broker_profile=prof)
    sess.registry.update_ltp("GONE", 96.0, broker_profile=prof)
    sess.registry.set_gtt("GONE", "G-GONE", broker_profile=prof)
    sess.monitor.freeze_invested_basis()

    with falcon_conn() as con:
        before = con.execute("SELECT invested_basis FROM autotrade_sessions "
                             "WHERE session_id=?", (sess.session_id,)).fetchone()[0]
    assert before == pytest.approx(2000.0)     # both legs

    actions = reconcile_broker_positions(sess)
    assert any(a["action"] == "CLOSED_RECONCILED" and a["symbol"] == "GONE"
               for a in actions), actions
    assert _row(sess.session_id, "GONE")["status"] == "CLOSED"

    with falcon_conn() as con:
        after = con.execute("SELECT invested_basis FROM autotrade_sessions "
                            "WHERE session_id=?", (sess.session_id,)).fetchone()[0]
    assert after == pytest.approx(1000.0)      # re-frozen over KEEP only
