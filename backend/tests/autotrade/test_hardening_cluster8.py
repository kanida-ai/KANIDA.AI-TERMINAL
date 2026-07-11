"""SPRINT CLUSTER 8 ITEM 6 — small hardening cluster.

(a) opposite-direction cross-session netting: a long session + a short session on the
    SAME (symbol, product) net to 0 at the broker; the reconciler must NOT phantom-
    close them. Fixed via a SIGNED db_held sum.
(b) WAL durability: the AutoTrade write path runs PRAGMA synchronous = FULL.
(c) doc drift (trail_engine module docstring + _tick_intraday defaults) — DOC-ONLY,
    no runtime test (pure comments).

Each runtime test PASSES with the fix and FAILS on the stated revert. Paper-safe.
"""
from tests.autotrade.test_broker_reconciliation import (
    _make_live_session, _register)
from autotrade.monitoring.position_reconciler import reconcile_broker_positions
from falcon.db import falcon_conn


def _row(sess, symbol):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT status, qty, direction FROM autotrade_positions "
            "WHERE session_id=? AND symbol=?",
            (sess.session_id, symbol)).fetchone()
    return dict(r) if r else None


# ══════════════════════════════════════════════════════════════════════════════
# (a) opposite-direction cross-session netting → no phantom close
# ══════════════════════════════════════════════════════════════════════════════
def test_opposite_direction_sessions_no_phantom_close(clean_positions, monkeypatch):
    """Session A long 100 + Session B short 100 of the SAME MIS symbol net to 0 at
    the broker (buy_q 100 / sell_q 100 / net 0). The SIGNED db sum is +100−100 = 0 =
    the broker's net → in sync → NEITHER position is closed.

    MUTATION REVERT: restore `db_held_all = sum(int(p['qty']) ...)` (UNSIGNED) in
    position_reconciler.reconcile_broker_positions → db_held_all becomes 200 while
    broker_held is 0 → both are phantom-closed CLOSED_EXTERNAL_FLAT → the
    `status == 'OPEN'` asserts FAIL."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": 0, "buy_quantity": 100,
                      "sell_quantity": 100, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    a = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                           order_product="MIS")
    _register(a, "A", 100, 100.0, ltp=100.0, instrument_type="EQ",
              direction="long")
    b = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                           order_product="MIS")
    _register(b, "A", 100, 100.0, ltp=100.0, instrument_type="EQ",
              direction="short")

    actions_a = reconcile_broker_positions(a)
    actions_b = reconcile_broker_positions(b)

    assert actions_a == [] and actions_b == []
    assert _row(a, "A")["status"] == "OPEN" and _row(a, "A")["qty"] == 100
    assert _row(b, "A")["status"] == "OPEN" and _row(b, "A")["qty"] == 100


def test_single_direction_group_unchanged(clean_positions, monkeypatch):
    """No-fire complement: a single-direction group is byte-for-byte unchanged — a
    genuine external flat (broker net 0 with sell evidence) of an all-long book
    still closes (|signed| == the old unsigned sum for one direction)."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": 0, "buy_quantity": 100,
                      "sell_quantity": 100, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    a = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                           order_product="MIS")
    _register(a, "A", 100, 100.0, ltp=100.0, instrument_type="EQ",
              direction="long")
    reconcile_broker_positions(a)
    assert _row(a, "A")["status"] == "CLOSED"     # genuinely flat → closed


# ══════════════════════════════════════════════════════════════════════════════
# (b) WAL durability — synchronous = FULL
# ══════════════════════════════════════════════════════════════════════════════
def test_autotrade_write_path_synchronous_full():
    """The AutoTrade DB connection runs PRAGMA synchronous = FULL (2), so a live
    order-book commit is fsynced against OS-crash / power loss.

    MUTATION REVERT: change falcon.db.connect_falcon back to `PRAGMA synchronous =
    NORMAL` → the pragma reads 1 → the `== 2` assert FAILS."""
    with falcon_conn() as con:
        val = con.execute("PRAGMA synchronous").fetchone()[0]
    assert val == 2                                # 2 == FULL (1 == NORMAL)
