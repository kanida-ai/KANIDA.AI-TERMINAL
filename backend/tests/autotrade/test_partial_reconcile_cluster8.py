"""SPRINT CLUSTER 8 ITEM 4 — position_reconciler close is PARTIAL-FILL-AWARE.

When a CONFIRMED exit order filled FEWER shares than the position qty (a deliberate
partial that fully filled), the reconciler must book ONLY the filled portion (via
update_partial_exit) and leave the REMAINDER OPEN — never over-close the whole row
(which would realise P&L on shares still held).

The test PASSES with the fix and FAILS on the stated revert. Paper-safe.
"""
from tests.autotrade.test_broker_reconciliation import (
    _make_live_session, _register)
from autotrade.monitoring.position_reconciler import reconcile_broker_positions
from falcon.db import falcon_conn


def _row(sess, symbol):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT status, qty, realised_pnl FROM autotrade_positions "
            "WHERE session_id=? AND symbol=?",
            (sess.session_id, symbol)).fetchone()
    return dict(r) if r else None


def test_confirmed_close_partial_fill_keeps_remainder_open(clean_positions,
                                                           monkeypatch):
    """Exit order EX-A COMPLETE with filled_quantity 20 on a 35-share row (broker net
    now shows 15) → the row stays OPEN at 15 with realised P&L booked for the 20, not
    a full 35-share close.

    MUTATION REVERT: delete the ITEM 4 partial branch (`if 0 < _ev_filled < _pos_qty:
    update_partial_exit(...); continue`) in position_reconciler.reconcile_broker_
    positions → mark_closed books a FULL 35-share close → `status == 'OPEN'` and
    `qty == 15` FAIL (the over-close this fix prevents)."""
    # broker holds only 15 (20 of our 35 sold via EX-A); order-status confirms the
    # partial fill of exactly 20 @ ₹101.
    net_book = {"A": {"quantity": 15, "buy_quantity": 35, "sell_quantity": 20,
                      "average_price": 100.0, "exchange": "NSE", "product": "CNC"}}
    order_status = {"EX-A": {"status": "COMPLETE", "filled_quantity": 20,
                             "average_price": 101.0}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_status=order_status)
    _register(sess, "A", 35, 100.0, ltp=100.0, exit_order_id="EX-A")

    actions = reconcile_broker_positions(sess)

    row = _row(sess, "A")
    assert row["status"] == "OPEN"                 # NOT over-closed
    assert row["qty"] == 15                         # 35 - 20 remainder
    assert row["realised_pnl"] == 20.0             # (101 - 100) × 20 filled
    assert any(a.get("action") == "PARTIAL_CLOSED_RECONCILED" for a in actions)


def test_confirmed_close_full_fill_still_closes(clean_positions, monkeypatch):
    """No-fire complement: a CONFIRMED exit that filled the FULL 35 (broker flat)
    still books a normal full CLOSE — the partial branch only triggers on a genuine
    under-fill (byte-for-byte unchanged for the full-fill case)."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 35, "sell_quantity": 35,
                      "average_price": 100.0, "exchange": "NSE", "product": "CNC"}}
    order_status = {"EX-A": {"status": "COMPLETE", "filled_quantity": 35,
                             "average_price": 101.0}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_status=order_status)
    _register(sess, "A", 35, 100.0, ltp=100.0, exit_order_id="EX-A")

    reconcile_broker_positions(sess)

    row = _row(sess, "A")
    assert row["status"] == "CLOSED"
    assert row["realised_pnl"] == 35.0             # (101 - 100) × 35 full
