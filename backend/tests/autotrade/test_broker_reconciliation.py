"""ORDER-ID-DRIVEN, INVARIANT-BASED broker→DB reconciler tests (v2).

The reconciler is MULTI-SESSION-SAFE: it NEVER writes a session's qty from the
account aggregate and only CLOSES a position on POSITIVE order-id evidence
(a triggered-GTT whose order filled COMPLETE, or an exit_order that filled
COMPLETE). Any divergence it can't attribute to one of OUR order-ids is an ALERT
(never a mutation).

Each scenario builds a LIVE-mode session (dry_run=False), seeds OPEN/EXIT_FAILED
rows in autotrade_positions, injects a mock broker net book / holdings, runs ONE
reconcile, and asserts the invariant-driven outcome.

The invariant, per (symbol, product):
    Σ open-position qty (ALL sessions on the account) == broker net + holdings.
  * ==  → in sync, NO action.
  * <   → order-id resolution (close per position on positive evidence), else
          UNATTRIBUTED_CLOSE alert (nothing closed).
  * >   → ORPHAN_AT_BROKER alert (nothing mutated).

Harness mirrors the prior file: patch broker.router.build_client to a MockBroker,
freeze "now" to a mid-session trading day so tick()'s market guard passes.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from autotrade.monitoring.position_reconciler import reconcile_broker_positions
from tests.autotrade.conftest import seed_signals  # noqa: F401 (import parity)
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _patch_brokers(monkeypatch, net_book, ltps=None, gtts=None,
                   order_status=None, holdings=None, orders=None):
    """Patch build_client so every profile gets a MockBroker carrying the given
    net_book / ltps / gtts / order_status / holdings / orders. Returns brokers by
    profile."""
    created = {}
    ltps = ltps or {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=ltps,
                        net_book=net_book, gtts=gtts, order_status=order_status,
                        holdings=holdings, orders=orders)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _make_live_session(monkeypatch, net_book, ltps=None, *, gtts=None,
                       order_status=None, holdings=None, orders=None,
                       kill_switch_enabled=False, capital=300000.0, top_n=3,
                       order_product="CNC"):
    _patch_brokers(monkeypatch, net_book, ltps=ltps, gtts=gtts,
                   order_status=order_status, holdings=holdings, orders=orders)
    cfg = TradingSessionConfig(total_allocated_capital=capital, top_n_stocks=top_n,
                               sizing_mode="equal",
                               kill_switch_enabled=kill_switch_enabled,
                               kill_switch_pct=0.02, order_product=order_product)
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _register(sess, symbol, qty, avg_price, *, ltp=None, status="OPEN",
              instrument_type="EQ", direction="long", exchange="NSE",
              gtt_id=None, exit_order_id=None, product=None):
    prof = sess.config.broker_profiles[0].profile_id
    # CLUSTER 3 ITEM 4: the reconciler now buckets by the position's PERSISTED
    # product, so a leg must carry the SAME product its session/net-book uses.
    # Default to the session's order_product (the intended bucket for these tests);
    # a test can override to model a genuinely mixed-product leg.
    reg_product = product if product is not None else sess.config.order_product
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=avg_price, product=reg_product,
                           instrument_type=instrument_type, exchange=exchange,
                           direction=direction)
    if ltp is not None:
        sess.registry.update_ltp(symbol, ltp, broker_profile=prof)
    if gtt_id is not None:
        sess.registry.set_gtt(symbol, gtt_id, broker_profile=prof)
    if exit_order_id is not None:
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_positions SET exit_order_id=? "
                "WHERE session_id=? AND symbol=?",
                (exit_order_id, sess.session_id, symbol))
            con.commit()
    if status != "OPEN":
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_positions SET status=? WHERE session_id=? "
                "AND symbol=?", (status, sess.session_id, symbol))
            con.commit()


def _row(sess, symbol):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT status, qty, avg_price, exit_price, close_reason, "
            "exit_order_id, realised_pnl FROM autotrade_positions "
            "WHERE session_id=? AND symbol=?",
            (sess.session_id, symbol)).fetchone()
    return dict(r) if r else None


def _freeze(sess):
    sess.monitor.freeze_invested_basis()


def _invested_basis(sess):
    with falcon_conn() as con:
        r = con.execute("SELECT invested_basis FROM autotrade_sessions "
                        "WHERE session_id=?", (sess.session_id,)).fetchone()
    return r["invested_basis"] if r else None


def _alerts(kind=None):
    with falcon_conn() as con:
        if kind:
            rows = con.execute(
                "SELECT * FROM autotrade_recon_alerts WHERE kind=?",
                (kind,)).fetchall()
        else:
            rows = con.execute("SELECT * FROM autotrade_recon_alerts").fetchall()
    return [dict(r) for r in rows]


# ── IN SYNC: broker net == db qty → NO action (the base case) ─────────────────

def test_in_sync_no_action(clean_positions, monkeypatch):
    net_book = {"A": {"quantity": 10, "buy_quantity": 10, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE", "product": "CNC"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0})
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 10


# ── THE 2026-07-07 CORRUPTION REGRESSION ──────────────────────────────────────
# Three same-pick sessions on ONE account hold WELCORP: two CNC (30 + 21) and one
# MIS (326). Broker net = CNC 51 / MIS 326; holdings 0. The invariant holds per
# (symbol, product): 30+21==51 and 326==326. NO qty may be changed on ANY session.
# The OLD code overwrote every session's qty to the account aggregate (51/51/51).

def test_corruption_regression_same_symbol_multi_session_no_qty_change(
        clean_positions, monkeypatch):
    net_book = {
        "WELCORP_CNC": {"tradingsymbol": "WELCORP", "quantity": 51,
                        "buy_quantity": 51, "sell_quantity": 0,
                        "average_price": 100.0, "exchange": "NSE",
                        "product": "CNC"},
        "WELCORP_MIS": {"tradingsymbol": "WELCORP", "quantity": 326,
                        "buy_quantity": 326, "sell_quantity": 0,
                        "average_price": 100.0, "exchange": "NSE",
                        "product": "MIS"},
    }
    # Session 1 + 2: CNC, hold 30 and 21. Session 3: MIS, holds 326.
    s1 = _make_live_session(monkeypatch, net_book, ltps={"WELCORP": 100.0},
                            order_product="CNC")
    _register(s1, "WELCORP", 30, 100.0, ltp=100.0)
    _freeze(s1)
    s2 = _make_live_session(monkeypatch, net_book, ltps={"WELCORP": 100.0},
                            order_product="CNC")
    _register(s2, "WELCORP", 21, 100.0, ltp=100.0)
    _freeze(s2)
    s3 = _make_live_session(monkeypatch, net_book, ltps={"WELCORP": 100.0},
                            order_product="MIS")
    _register(s3, "WELCORP", 326, 100.0, ltp=100.0)
    _freeze(s3)

    # Reconcile each session. NONE may mutate a qty; all in sync per (sym,product).
    for s in (s1, s2, s3):
        actions = reconcile_broker_positions(s)
        assert actions == [], f"unexpected actions for {s.session_id}: {actions}"

    assert _row(s1, "WELCORP")["qty"] == 30       # NOT corrupted to 51
    assert _row(s2, "WELCORP")["qty"] == 21       # NOT corrupted to 51
    assert _row(s3, "WELCORP")["qty"] == 326
    for s in (s1, s2, s3):
        assert _row(s, "WELCORP")["status"] == "OPEN"
    assert _alerts() == []                         # no divergence → no alerts


# ── CLOSE via GTT order-id: only the position with the fired GTT closes ────────

def test_close_via_gtt_order_id_only_that_position(clean_positions, monkeypatch):
    """Two CNC sessions hold WELCORP (30 + 21). Session 1's position has a fired
    GTT (COMPLETE @ 96.5) for its 30. Broker net drops to 21 (only session 2's
    lot remains). ONLY session 1's position closes at the real fill; session 2 is
    untouched."""
    net_book = {"WELCORP": {"tradingsymbol": "WELCORP", "quantity": 21,
                            "buy_quantity": 51, "sell_quantity": 30,
                            "sell_price": 96.5, "average_price": 100.0,
                            "exchange": "NSE", "product": "CNC"}}
    gtts = {"G-WEL": {"status": "triggered",
                      "orders": [{"result": {"order_id": "O-WEL-FIRED"}}]}}
    order_status = {"O-WEL-FIRED": {"status": "COMPLETE", "filled_quantity": 30,
                                    "average_price": 96.5}}
    s1 = _make_live_session(monkeypatch, net_book, ltps={"WELCORP": 96.5},
                            gtts=gtts, order_status=order_status,
                            order_product="CNC")
    _register(s1, "WELCORP", 30, 100.0, ltp=96.5, gtt_id="G-WEL")
    _freeze(s1)
    s2 = _make_live_session(monkeypatch, net_book, ltps={"WELCORP": 96.5},
                            gtts=gtts, order_status=order_status,
                            order_product="CNC")
    _register(s2, "WELCORP", 21, 100.0, ltp=96.5)
    _freeze(s2)

    # Session 1 reconciles: db_all=30+21=51, broker=21 → deficit 30; its GTT
    # fill (30 @ 96.5) attributes exactly → close session 1's position only.
    actions = reconcile_broker_positions(s1)
    assert len(actions) == 1
    a = actions[0]
    assert a["action"] == "CLOSED_RECONCILED"
    assert a["close_reason"] == "GTT"
    assert a["exit_price"] == pytest.approx(96.5)
    assert a["exit_order_id"] == "O-WEL-FIRED"

    r1 = _row(s1, "WELCORP")
    assert r1["status"] == "CLOSED"
    assert r1["exit_price"] == pytest.approx(96.5)
    assert r1["exit_order_id"] == "O-WEL-FIRED"
    assert r1["realised_pnl"] == pytest.approx((96.5 - 100.0) * 30)
    # Sibling session UNTOUCHED.
    assert _row(s2, "WELCORP")["status"] == "OPEN"
    assert _row(s2, "WELCORP")["qty"] == 21
    assert _alerts("UNATTRIBUTED_CLOSE") == []

    # Session 2 now reconciles: db_all=21 (s1 closed), broker=21 → in sync, no-op.
    actions2 = reconcile_broker_positions(s2)
    assert actions2 == []
    assert _row(s2, "WELCORP")["status"] == "OPEN"


# ── CLOSE via exit_order_id (our own exit confirmed COMPLETE) ─────────────────

def test_close_via_exit_order_id(clean_positions, monkeypatch):
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "sell_price": 104.25, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    order_status = {"O-EXIT-A": {"status": "COMPLETE", "filled_quantity": 10,
                                 "average_price": 104.25}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 104.25},
                              order_status=order_status, order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=104.25, exit_order_id="O-EXIT-A")
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert len(actions) == 1
    assert actions[0]["action"] == "CLOSED_RECONCILED"
    assert actions[0]["close_reason"] == "RECONCILED_EXIT"
    r = _row(sess, "A")
    assert r["status"] == "CLOSED"
    assert r["exit_price"] == pytest.approx(104.25)
    assert r["exit_order_id"] == "O-EXIT-A"
    assert r["realised_pnl"] == pytest.approx((104.25 - 100.0) * 10)


# ── FULLY FLAT at the broker + no order-id → CLOSED_EXTERNAL_FLAT (2026-07-09) ──

def test_fully_flat_external_close_no_order_id(clean_positions, monkeypatch):
    """The manual-exit-of-our-own-position gap (2026-07-09 ATHERENERG): the broker
    is FULLY FLAT (net 0 WITH real sell evidence) but the position has NO
    gtt_id / exit_order_id we own — the trader squared it off manually. broker_held
    ==0 makes the close UNAMBIGUOUS → CLOSE it as CLOSED_EXTERNAL_FLAT at the broker
    sell avg, instead of looping UNATTRIBUTED_CLOSE forever and leaving a phantom
    OPEN row the trail keeps tracking (a latent false-sell)."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "sell_price": 96.5, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0)     # no order-ids (manual exit)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    kinds = {a["action"] for a in actions}
    assert "CLOSED_EXTERNAL_FLAT" in kinds
    assert "UNATTRIBUTED_CLOSE" not in kinds        # no more phantom-alert loop
    a = next(a for a in actions if a["action"] == "CLOSED_EXTERNAL_FLAT")
    assert a["exit_price"] == pytest.approx(96.5)   # broker sell avg
    r = _row(sess, "A")
    assert r["status"] == "CLOSED"                  # phantom cleared
    assert r["exit_price"] == pytest.approx(96.5)
    assert r["realised_pnl"] == pytest.approx((96.5 - 100.0) * 10)


def test_fully_flat_but_no_sell_evidence_still_alerts(clean_positions, monkeypatch):
    """SAFETY: broker_held==0 ALONE is not enough. A symbol ABSENT from the net book
    (empty/glitchy — no sell/buy volume) must NOT auto-close (a transient empty book
    can never flatten us) → ALERT, position stays OPEN."""
    net_book = {}    # symbol absent entirely → no rows, no sell evidence
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    kinds = {a["action"] for a in actions}
    assert "CLOSED_EXTERNAL_FLAT" not in kinds       # NOT closed on an absent book
    assert "UNATTRIBUTED_CLOSE" in kinds
    assert _row(sess, "A")["status"] == "OPEN"       # stays OPEN (safe)


def test_partial_deficit_no_evidence_still_alerts(clean_positions, monkeypatch):
    """A PARTIAL deficit (broker still holds SOME) with NO market evidence
    (no sell/buy volume — a stale/glitchy book) is STILL an alert, never a
    close: without positive sell/buy evidence C4a's partial external-close is
    NOT triggered (a transient empty/odd book can never mutate our qty)."""
    net_book = {"A": {"quantity": 7, "buy_quantity": 0, "sell_quantity": 0,
                      "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0)       # broker holds 7 < db 10
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    kinds = {a["action"] for a in actions}
    assert "UNATTRIBUTED_CLOSE" in kinds             # partial, no evidence → alert
    assert "PARTIAL_EXTERNAL_CLOSE" not in kinds     # NOT booked (no evidence)
    assert "CLOSED_EXTERNAL_FLAT" not in kinds       # NOT closed (broker holds 7)
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 10              # qty untouched


def test_gtt_triggered_open_order_still_flat_closes_external(clean_positions,
                                                            monkeypatch):
    """A triggered GTT whose FIRED order is still OPEN (not COMPLETE) is NOT valid
    order-id evidence (the order-id path correctly rejects it). But the broker is
    FULLY FLAT (net 0 + sell evidence), so the position is closed via the full-flat
    path as CLOSED_EXTERNAL_FLAT at the broker sell avg (exit_order_id None) — never
    left as a phantom, and never falsely attributed to the un-filled GTT order."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "sell_price": 96.5, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    gtts = {"G-A": {"status": "triggered",
                    "orders": [{"result": {"order_id": "O-A"}}]}}
    order_status = {"O-A": {"status": "OPEN", "filled_quantity": 0,
                            "average_price": 0.0}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              gtts=gtts, order_status=order_status,
                              order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0, gtt_id="G-A")
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    a = next(a for a in actions if a["action"] == "CLOSED_EXTERNAL_FLAT")
    assert a["exit_order_id"] is None             # NOT attributed to the OPEN GTT
    assert a["exit_price"] == pytest.approx(96.5)
    assert _row(sess, "A")["status"] == "CLOSED"  # phantom cleared


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 7 — ORDER-ID-SCOPED cross-check: a MANUAL trade in an overlapping symbol
# produces ZERO reconciliation signal. A SURPLUS (broker > our tracked qty) is
# INVISIBLE; only a DEFICIT with no order-id evidence alerts.
# ══════════════════════════════════════════════════════════════════════════════

def test_p7_manual_buy_arbitrary_overlap_invisible(clean_positions, monkeypatch):
    """HEADLINE P7: our session holds TRENT 100; the trader ALSO manually buys 37
    → broker 137 (our 100 + manual 37, an ARBITRARY ratio 1.37). NO action, NO
    alert; our qty stays 100. The manual trade is invisible."""
    net_book = {"TRENT": {"tradingsymbol": "TRENT", "quantity": 137,
                          "buy_quantity": 137, "sell_quantity": 0,
                          "average_price": 100.0, "exchange": "NSE",
                          "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"TRENT": 100.0},
                              order_product="MIS")
    _register(sess, "TRENT", 100, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _alerts() == []                      # NO orphan, NO corp-action
    assert _row(sess, "TRENT")["status"] == "OPEN"
    assert _row(sess, "TRENT")["qty"] == 100


def test_p7_manual_buy_clean_multiple_not_orphan(clean_positions, monkeypatch):
    """MANUAL BUY overlap that happens to land on a CLEAN multiple: our 100 +
    manual 50 = 150 (ratio ×1.5). This is NOT an ORPHAN. Because a split/bonus
    would look identical (no order-id to confirm either), it surfaces as at most a
    NON-mutating CORP_ACTION_SUSPECTED — the documented rare-coincidence trade-off.
    Never mutated."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": 150, "buy_quantity": 150,
                      "sell_quantity": 0, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 100, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert not any(a["action"] == "ORPHAN_AT_BROKER" for a in actions)
    assert _alerts("ORPHAN_AT_BROKER") == []
    # Corp-action-suspected at most, and NON-mutating.
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 100


def test_p7_manual_sell_others_shares_in_sync(clean_positions, monkeypatch):
    """MANUAL SELL of the trader's OTHER shares: the account was our 100 + the
    trader's own 50 = 150; the trader sells THEIR 50 → broker 100 == our 100 → IN
    SYNC. No alert, nothing mutated (the broker still fully covers our tracked qty)."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": 100, "buy_quantity": 150,
                      "sell_quantity": 50, "sell_price": 101.0,
                      "average_price": 100.0, "exchange": "NSE",
                      "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 101.0},
                              order_product="MIS")
    _register(sess, "A", 100, 100.0, ltp=101.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _alerts() == []
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 100


def test_p7_manual_sell_below_tracked_no_evidence_unattributed(clean_positions,
                                                               monkeypatch):
    """C4a — a manual sell dips the FUNGIBLE account BELOW our tracked qty
    (our 100, broker 60) with sell EVIDENCE but no order-id we own. The account
    can no longer cover our 100, so `deficit` (40) shares are provably gone: we
    BOOK a partial external close of 40 against our oldest lot (qty 100→60,
    remainder OPEN) — which prevents over-tracking / a naked oversell — and still
    raise ONE (deduped) UNATTRIBUTED_CLOSE for ops. A manual SELL order under an
    id we DON'T own is NEVER attributed as our close (order-id-scoped)."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": 60, "buy_quantity": 100,
                      "sell_quantity": 40, "sell_price": 99.0,
                      "average_price": 100.0, "exchange": "NSE",
                      "product": "MIS"}}
    orders = {"MANUAL-SELL-1": {"status": "COMPLETE", "filled_quantity": 40,
                                "average_price": 99.0, "transaction_type": "SELL",
                                "tradingsymbol": "A", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 99.0},
                              orders=orders, order_product="MIS")
    _register(sess, "A", 100, 100.0, ltp=99.0)      # NO exit_order_id / gtt

    actions = reconcile_broker_positions(sess)
    ua = [a for a in actions if a["action"] == "UNATTRIBUTED_CLOSE"]
    assert len(ua) == 1
    assert ua[0]["deficit"] == 40
    assert ua[0]["partial_close_booked"] == 40
    pec = [a for a in actions if a["action"] == "PARTIAL_EXTERNAL_CLOSE"]
    assert len(pec) == 1 and pec[0]["closed_qty"] == 40
    r = _row(sess, "A")
    assert r["status"] == "OPEN"                      # remainder stays open
    assert r["qty"] == 60                             # over-tracked deficit booked
    assert len(_alerts("UNATTRIBUTED_CLOSE")) == 1


def test_p7_our_gtt_fires_manual_remainder_not_orphan(clean_positions,
                                                      monkeypatch):
    """OUR GTT fires (our 100 sold) while a manual 37 remains → broker net 37. Our
    position CLOSES on the GTT order-id evidence; the manual remainder (37) is NOT
    treated as an orphan (never surfaces)."""
    net_book = {"TRENT": {"tradingsymbol": "TRENT", "quantity": 37,
                          "buy_quantity": 137, "sell_quantity": 100,
                          "sell_price": 96.0, "average_price": 100.0,
                          "exchange": "NSE", "product": "MIS"}}
    gtts = {"G-T": {"status": "triggered",
                    "orders": [{"result": {"order_id": "O-T"}}]}}
    order_status = {"O-T": {"status": "COMPLETE", "filled_quantity": 100,
                            "average_price": 96.0}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"TRENT": 96.0},
                              gtts=gtts, order_status=order_status,
                              order_product="MIS")
    _register(sess, "TRENT", 100, 100.0, ltp=96.0, gtt_id="G-T")
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert any(a["action"] == "CLOSED_RECONCILED" for a in actions), actions
    assert not any(a["action"] == "ORPHAN_AT_BROKER" for a in actions)
    assert not any(a["action"] == "UNATTRIBUTED_CLOSE" for a in actions)
    r = _row(sess, "TRENT")
    assert r["status"] == "CLOSED"
    assert r["close_reason"] == "GTT"
    assert r["exit_price"] == pytest.approx(96.0)
    assert _alerts("ORPHAN_AT_BROKER") == []


def test_p7_orderbook_strengthens_our_exit_attribution(clean_positions,
                                                       monkeypatch):
    """PHASE 7 get_orders() strengthening: our exit_order_id filled COMPLETE, but
    the per-position get_order_status probe reports 0-fill (transient miss). The
    BATCHED orderbook shows OUR exit order COMPLETE → the deficit is attributed to
    OUR order-id and the position CLOSES at the real fill (never a manual order)."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": 0, "buy_quantity": 100,
                      "sell_quantity": 100, "sell_price": 98.5,
                      "average_price": 100.0, "exchange": "NSE",
                      "product": "MIS"}}
    # No order_status map entry for O-BOOK → per-position get_order_status returns a
    # synthetic 0-fill → _confirmed_close returns None. The orderbook resolves it.
    orders = {"O-BOOK": {"status": "COMPLETE", "filled_quantity": 100,
                         "average_price": 98.5, "transaction_type": "SELL",
                         "tradingsymbol": "A", "product": "MIS"},
              "MANUAL-9": {"status": "COMPLETE", "filled_quantity": 5,
                           "average_price": 98.5, "transaction_type": "SELL",
                           "tradingsymbol": "A", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 98.5},
                              orders=orders, order_product="MIS")
    _register(sess, "A", 100, 100.0, ltp=98.5, exit_order_id="O-BOOK")
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    closed = [a for a in actions if a["action"] == "CLOSED_RECONCILED"]
    assert len(closed) == 1
    assert closed[0]["close_reason"] == "RECONCILED_EXIT"
    r = _row(sess, "A")
    assert r["status"] == "CLOSED"
    assert r["exit_price"] == pytest.approx(98.5)
    assert r["realised_pnl"] == pytest.approx((98.5 - 100.0) * 100)
    assert _alerts("UNATTRIBUTED_CLOSE") == []


# ── PARTIAL divergence with no order evidence → UNATTRIBUTED (never qty-correct) ─

def test_partial_broker_less_no_evidence_unattributed(clean_positions,
                                                      monkeypatch):
    """Broker holds 6, DB thinks 10, no filled order explains the 4 gap. The OLD
    code silently QTY_CORRECTED 10→6. The v2 reconciler REFUSES to correct qty
    from the aggregate → UNATTRIBUTED_CLOSE alert, qty unchanged."""
    net_book = {"A": {"quantity": 6, "buy_quantity": 6, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)
    assert _invested_basis(sess) == pytest.approx(10 * 100.0)

    actions = reconcile_broker_positions(sess)
    assert any(a["action"] == "UNATTRIBUTED_CLOSE" for a in actions)
    assert _row(sess, "A")["qty"] == 10            # NOT corrected to 6
    assert _invested_basis(sess) == pytest.approx(10 * 100.0)   # unchanged


# ── IDEMPOTENT: an already-CLOSED row is never re-touched ─────────────────────

def test_already_closed_idempotent_noop(clean_positions, monkeypatch):
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "sell_price": 96.5, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="A", broker_profile=prof, qty=10,
                           avg_price=100.0, product="MIS", instrument_type="EQ")
    sess.registry.update_ltp("A", 98.0, broker_profile=prof)
    sess.registry.mark_closed("A", "OUR_EXIT", exit_price=98.0,
                              broker_profile=prof)
    before = _row(sess, "A")

    actions = reconcile_broker_positions(sess)
    assert actions == []
    after = _row(sess, "A")
    assert after == before
    assert after["close_reason"] == "OUR_EXIT"


# ── FAIL-SAFE: None / empty book / paper → ZERO changes ───────────────────────

def test_none_book_never_mutates(clean_positions, monkeypatch):
    sess = _make_live_session(monkeypatch, net_book=None, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 10


def test_empty_book_never_mutates(clean_positions, monkeypatch):
    """An EMPTY net book while we hold OPEN → deficit is unattributable, but the
    fail-safe contract is that we make NO position mutation (a genuine close shows
    a day-flat row + an order we can attribute). It surfaces as an alert at most."""
    sess = _make_live_session(monkeypatch, net_book=[], ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    # No position may be closed (no positive evidence).
    assert all(a["action"] != "CLOSED_RECONCILED" for a in actions)
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 10


def test_paper_session_noop(clean_positions, monkeypatch):
    _patch_brokers(monkeypatch,
                   net_book={"A": {"quantity": 0, "buy_quantity": 10,
                                   "sell_quantity": 10, "sell_price": 96.5,
                                   "exchange": "NSE", "product": "MIS"}},
                   ltps={"A": 100.0})
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    sess._build_brokers()
    _register(sess, "A", 10, 100.0, ltp=100.0)

    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _row(sess, "A")["status"] == "OPEN"     # paper byte-for-byte unchanged
    assert _alerts() == []


# ── OVERNIGHT CNC / holdings: a delivered CNC in holdings is IN SYNC, not closed ─

def test_overnight_cnc_in_holdings_in_sync_no_action(clean_positions,
                                                     monkeypatch):
    """A delivered CNC shows net 0 (no sell) but is HELD in holdings. broker_held
    (holdings 10) == db_held_all (10) → in sync, NO action / alert."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 0, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE",
                      "product": "CNC"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 105.0},
                              order_product="CNC",
                              holdings={"A": {"quantity": 10, "t1_quantity": 0,
                                              "average_price": 100.0}})
    _register(sess, "A", 10, 100.0, ltp=105.0)
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 10


def test_overnight_cnc_t1_quantity_counts_as_held(clean_positions, monkeypatch):
    net_book = {"A": {"quantity": 0, "buy_quantity": 0, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE",
                      "product": "CNC"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 104.0},
                              order_product="CNC",
                              holdings={"A": {"quantity": 0, "t1_quantity": 10}})
    _register(sess, "A", 10, 100.0, ltp=104.0)
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _row(sess, "A")["status"] == "OPEN"


# ── CNC broker_held = signed day-net + settled holdings (LIVE 2026-07-06 shapes) ─

def test_cnc_today_net_plus_overnight_holdings_summed_in_sync(clean_positions,
                                                              monkeypatch):
    """Live AEGISLOG shape: today's CNC buys sit in net.quantity (57) while an
    OVERNIGHT lot from another session has settled into holdings.t1 (35). The two
    are DISJOINT — true held = 57 + 35 = 92. Two sessions (today 57, overnight 35)
    sum to 92 → in sync. The OLD max(net_abs, held)=max(57,35)=57 would fire a
    FALSE UNATTRIBUTED_CLOSE of 35 every tick; the summed formula is in sync."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": 57, "buy_quantity": 57,
                      "sell_quantity": 0, "average_price": 100.0,
                      "exchange": "NSE", "product": "CNC"}}
    holdings = {"A": {"quantity": 0, "t1_quantity": 35, "average_price": 98.0}}
    s_today = _make_live_session(monkeypatch, net_book, ltps={"A": 101.0},
                                 holdings=holdings, order_product="CNC")
    _register(s_today, "A", 57, 100.0, ltp=101.0)
    _freeze(s_today)
    s_overnight = _make_live_session(monkeypatch, net_book, ltps={"A": 101.0},
                                     holdings=holdings, order_product="CNC")
    _register(s_overnight, "A", 35, 98.0, ltp=101.0)
    _freeze(s_overnight)

    for s in (s_today, s_overnight):
        actions = reconcile_broker_positions(s)
        assert actions == [], f"unexpected {actions} for {s.session_id}"
    assert _row(s_today, "A")["qty"] == 57
    assert _row(s_overnight, "A")["qty"] == 35
    assert _alerts() == []


def test_cnc_sell_negative_net_offsets_not_phantom_hold(clean_positions,
                                                        monkeypatch):
    """Live ACUTAAS shape: a fully-exited CNC shows a NEGATIVE day-net (sold 12)
    with 0 holdings → truly held 0. The OLD abs(-12)=12 would read as still-held
    and MISS the close (position wrongly OPEN, broker_held==db → 'in sync'); the
    SIGNED formula floors at 0 and the fired-GTT evidence closes it at the fill."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": -12, "buy_quantity": 0,
                      "sell_quantity": 12, "sell_price": 3481.875,
                      "average_price": 3500.0, "exchange": "NSE",
                      "product": "CNC"}}
    gtts = {"G-A": {"status": "triggered",
                    "orders": [{"result": {"order_id": "O-A"}}]}}
    order_status = {"O-A": {"status": "COMPLETE", "filled_quantity": 12,
                            "average_price": 3481.875}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 3481.875},
                              gtts=gtts, order_status=order_status,
                              order_product="CNC")
    _register(sess, "A", 12, 3500.0, ltp=3481.875, gtt_id="G-A")
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert any(a["action"] == "CLOSED_RECONCILED" for a in actions), actions
    r = _row(sess, "A")
    assert r["status"] == "CLOSED"
    assert r["exit_price"] == pytest.approx(3481.875)
    assert r["close_reason"] == "GTT"


def test_cnc_held_lot_survives_other_sessions_same_day_sells(clean_positions,
                                                             monkeypatch):
    """LIVE 2026-07-08 AEGISLOG: one session still HOLDS 35 CNC (in holdings t1=35),
    while OTHER sessions' ladder exits SOLD their lots today → broker day-net = -57.
    Those 57 are ALREADY removed from holdings, so the true held is 35. The OLD
    formula max(0, net+holdings)=max(0,-57+35)=0 fired a FALSE UNATTRIBUTED_CLOSE;
    the fixed formula holdings+max(0,net)=35+0=35 is IN SYNC. Our held position must
    NOT be flagged or closed."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": -57, "buy_quantity": 0,
                      "sell_quantity": 57, "sell_price": 1320.0,
                      "average_price": 1344.0, "exchange": "NSE",
                      "product": "CNC"}}
    holdings = {"A": {"quantity": 0, "t1_quantity": 35, "average_price": 1344.4}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 1330.0},
                              holdings=holdings, order_product="CNC")
    _register(sess, "A", 35, 1344.4, ltp=1330.0)
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    assert actions == [], f"held lot falsely flagged: {actions}"
    assert _alerts() == []
    assert _row(sess, "A")["status"] == "OPEN" and _row(sess, "A")["qty"] == 35


# ── EXIT_FAILED row is examined too; closes only on positive evidence ─────────

def test_exit_failed_with_confirmed_exit_order_closed(clean_positions,
                                                      monkeypatch):
    """An EXIT_FAILED row whose exit_order actually filled COMPLETE later → CLOSED
    at the real fill (positive evidence)."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 15, "sell_quantity": 15,
                      "sell_price": 97.8, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    order_status = {"O-XF": {"status": "COMPLETE", "filled_quantity": 15,
                             "average_price": 97.8}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_status=order_status, order_product="MIS")
    _register(sess, "A", 15, 100.0, ltp=100.0, status="EXIT_FAILED",
              exit_order_id="O-XF")
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    assert any(a["action"] == "CLOSED_RECONCILED" for a in actions)
    r = _row(sess, "A")
    assert r["status"] == "CLOSED"
    assert r["exit_price"] == pytest.approx(97.8)
    assert r["realised_pnl"] == pytest.approx((97.8 - 100.0) * 15)


def test_exit_failed_fully_flat_closes_external(clean_positions, monkeypatch):
    """An EXIT_FAILED row with NO attributable filled order but the broker FULLY
    FLAT (net 0 + sell evidence): the position is objectively gone (a retry/manual/
    RMS exit landed) → close it CLOSED_EXTERNAL_FLAT instead of leaving it stuck
    EXIT_FAILED forever retrying an exit on a position that no longer exists."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 15, "sell_quantity": 15,
                      "sell_price": 97.8, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 15, 100.0, ltp=100.0, status="EXIT_FAILED")
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    a = next(a for a in actions if a["action"] == "CLOSED_EXTERNAL_FLAT")
    assert a["exit_price"] == pytest.approx(97.8)
    assert _row(sess, "A")["status"] == "CLOSED"         # no longer stuck


# ── END-TO-END through session.tick(): close surfaces under broker_reconciled ─

def test_close_via_order_id_at_tick(clean_positions, monkeypatch,
                                    market_hours_clock):
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "sell_price": 95.0, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"},
                "B": {"quantity": 5, "buy_quantity": 5, "sell_quantity": 0,
                      "average_price": 200.0, "exchange": "NSE",
                      "product": "MIS"}}
    order_status = {"O-A": {"status": "COMPLETE", "filled_quantity": 10,
                            "average_price": 95.0}}
    sess = _make_live_session(monkeypatch, net_book,
                              ltps={"A": 100.0, "B": 200.0},
                              order_status=order_status, order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0, exit_order_id="O-A")
    _register(sess, "B", 5, 200.0, ltp=200.0)
    _freeze(sess)

    tick = asyncio.run(sess.tick())
    recon = {a["symbol"]: a for a in tick.get("broker_reconciled", [])}
    assert recon["A"]["action"] == "CLOSED_RECONCILED"
    assert _row(sess, "A")["status"] == "CLOSED"
    assert _row(sess, "B")["status"] == "OPEN"     # B in sync


# ── C4a — PARTIAL external close (broker < db, sell evidence, no order-id) ─────

def test_c4a_partial_external_close_books_deficit(clean_positions, monkeypatch):
    """Broker holds 60 of our 100 (sell evidence, no order-id). The account can't
    cover our tracked qty → book a partial external close of exactly the deficit
    (40) against our oldest lot; remainder (60) stays OPEN; ONE alert."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": 60, "buy_quantity": 100,
                      "sell_quantity": 40, "sell_price": 97.0,
                      "average_price": 100.0, "exchange": "NSE",
                      "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 99.0},
                              order_product="MIS")
    _register(sess, "A", 100, 100.0, ltp=99.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    pec = [a for a in actions if a["action"] == "PARTIAL_EXTERNAL_CLOSE"]
    assert len(pec) == 1
    assert pec[0]["closed_qty"] == 40
    assert pec[0]["exit_price"] == pytest.approx(97.0)   # broker sell avg
    r = _row(sess, "A")
    assert r["status"] == "OPEN"
    assert r["qty"] == 60
    # Booked realised P&L on the closed 40 at the sell avg.
    assert r["realised_pnl"] == pytest.approx((97.0 - 100.0) * 40)
    assert len(_alerts("UNATTRIBUTED_CLOSE")) == 1


def test_c4a_next_tick_in_sync_no_reclose(clean_positions, monkeypatch):
    """After booking the deficit, db == broker → the next reconcile is in-sync and
    does NOTHING (no double-book, no new alert)."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": 60, "buy_quantity": 100,
                      "sell_quantity": 40, "sell_price": 97.0,
                      "average_price": 100.0, "exchange": "NSE",
                      "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 99.0},
                              order_product="MIS")
    _register(sess, "A", 100, 100.0, ltp=99.0)
    _freeze(sess)

    reconcile_broker_positions(sess)
    assert _row(sess, "A")["qty"] == 60
    actions2 = reconcile_broker_positions(sess)
    assert actions2 == []
    assert _row(sess, "A")["qty"] == 60
    assert len(_alerts("UNATTRIBUTED_CLOSE")) == 1        # still exactly one


def test_c4b_alert_deduped_across_ticks(clean_positions, monkeypatch):
    """A persistent partial deficit with NO market evidence alerts EVERY tick in
    the old code (unbounded rows). Dedup caps it at one row per (session,symbol,
    product,kind) per day even across many reconcile passes."""
    net_book = {"A": {"tradingsymbol": "A", "quantity": 7, "buy_quantity": 0,
                      "sell_quantity": 0, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)

    for _ in range(4):
        reconcile_broker_positions(sess)
    # No evidence → never booked (position untouched), but only ONE alert row.
    assert _row(sess, "A")["qty"] == 10
    assert len(_alerts("UNATTRIBUTED_CLOSE")) == 1


# ── M#11 — cash exchange match (a BSE row must not shadow an NSE hold) ─────────

def test_m11_bse_row_does_not_shadow_nse_hold(clean_positions, monkeypatch):
    """The broker net book carries a BSE row of the SAME base name as our NSE
    CNC hold. It must NOT count toward the NSE bucket (different security). With
    ONLY the BSE row present, our NSE hold reads broker_held 0 → resolved by ITS
    OWN order-id evidence, NOT by the BSE quantity."""
    net_book = {"BSEROW": {"tradingsymbol": "A", "quantity": 25,
                           "buy_quantity": 25, "sell_quantity": 0,
                           "average_price": 100.0, "exchange": "BSE",
                           "product": "CNC"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="CNC")
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="A", broker_profile=prof, qty=10,
                           avg_price=100.0, product="CNC", instrument_type="EQ",
                           exchange="NSE")
    sess.registry.update_ltp("A", 100.0, broker_profile=prof)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    # The BSE 25 is NOT counted as our NSE hold; broker_held for NSE is 0 (no
    # holdings, no matching net row) → a deficit alert, never an in-sync / a
    # BSE-driven qty. Crucially the BSE qty never becomes our broker_held.
    assert not any(a.get("action") == "CORP_ACTION_SUSPECTED" for a in actions)
    # Our NSE row is untouched (never mutated from the BSE row).
    assert _row(sess, "A")["qty"] == 10


# ── HARDENING: a HELD delivery leg (buy-only, no closing-side sell) must NEVER be
#    fully-flat false-closed (2026-07-15 Rupeezy CNC BTST blocker) ──────────────

def test_cnc_held_long_buy_only_not_false_closed(clean_positions, monkeypatch):
    """DEFENSE-IN-DEPTH for the BTST blocker. A genuinely-HELD long delivery leg
    that reaches the reconciler with net quantity=0, buy_quantity>0, sell_quantity=0
    (a held ENTRY — no close) and NO holdings must NOT be closed. This is EXACTLY
    the raw Vortex held-CNC shape if the adapter's buy−sell normalisation ever
    regressed (broker_held would compute to 0). The OLD group-level gate
    (_has_exit_evidence) returned True on the BUY alone → CLOSED_EXTERNAL_FLAT of a
    HELD position. Requiring CLOSING-side evidence (a SELL for a long) leaves the
    held leg OPEN and un-alerted. Mutation check: reverting to _has_exit_evidence
    closes the row → this fails."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 4, "sell_quantity": 0,
                      "buy_price": 100.0, "average_price": 100.0,
                      "exchange": "NSE", "product": "CNC"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 101.0},
                              order_product="CNC")
    _register(sess, "A", 4, 100.0, ltp=101.0)      # held, no order-ids
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    kinds = {a["action"] for a in actions}
    assert "CLOSED_EXTERNAL_FLAT" not in kinds     # NEVER false-close a held leg
    assert "CLOSED_SETTLED_AWAY" not in kinds
    assert "UNATTRIBUTED_CLOSE" not in kinds       # a held buy is not a divergence
    assert _row(sess, "A")["status"] == "OPEN"     # held BTST leg survives
    assert _row(sess, "A")["qty"] == 4


def test_cnc_round_trip_to_flat_still_closes_external(clean_positions, monkeypatch):
    """CONTROL: the hardening must NOT block a REAL external close. A delivery leg
    the trader/RMS actually squared off shows the CLOSING side (sell_quantity>0);
    broker_held==0 WITH sell evidence still closes CLOSED_EXTERNAL_FLAT at the sell
    avg — the legit path is preserved."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 4, "sell_quantity": 4,
                      "sell_price": 99.0, "average_price": 100.0,
                      "exchange": "NSE", "product": "CNC"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="CNC")
    _register(sess, "A", 4, 100.0, ltp=100.0)      # no order-ids (manual/RMS exit)
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    kinds = {a["action"] for a in actions}
    assert "CLOSED_EXTERNAL_FLAT" in kinds         # real sell evidence → closes
    r = _row(sess, "A")
    assert r["status"] == "CLOSED"
    assert r["exit_price"] == pytest.approx(99.0)
