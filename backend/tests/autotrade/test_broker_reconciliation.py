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
                   order_status=None, holdings=None):
    """Patch build_client so every profile gets a MockBroker carrying the given
    net_book / ltps / gtts / order_status / holdings. Returns brokers by profile."""
    created = {}
    ltps = ltps or {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=ltps,
                        net_book=net_book, gtts=gtts, order_status=order_status,
                        holdings=holdings)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _make_live_session(monkeypatch, net_book, ltps=None, *, gtts=None,
                       order_status=None, holdings=None,
                       kill_switch_enabled=False, capital=300000.0, top_n=3,
                       order_product="CNC"):
    _patch_brokers(monkeypatch, net_book, ltps=ltps, gtts=gtts,
                   order_status=order_status, holdings=holdings)
    cfg = TradingSessionConfig(total_allocated_capital=capital, top_n_stocks=top_n,
                               sizing_mode="equal",
                               kill_switch_enabled=kill_switch_enabled,
                               kill_switch_pct=0.02, order_product=order_product)
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _register(sess, symbol, qty, avg_price, *, ltp=None, status="OPEN",
              instrument_type="EQ", direction="long", exchange="NSE",
              gtt_id=None, exit_order_id=None):
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=avg_price, product="MIS",
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


# ── UNATTRIBUTED_CLOSE: deficit with NO filled order → ALERT, nothing closed ──

def test_unattributed_close_alert_no_evidence(clean_positions, monkeypatch):
    """Broker shows a deficit (net 0) but the position has NO gtt_id / exit_order_id
    that filled → we CANNOT attribute the close → ALERT, position stays OPEN."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "sell_price": 96.5, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0)     # no order-ids
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    kinds = {a["action"] for a in actions}
    assert "UNATTRIBUTED_CLOSE" in kinds
    ua = next(a for a in actions if a["action"] == "UNATTRIBUTED_CLOSE")
    assert ua["deficit"] == 10
    # NOTHING closed — a stale-but-flagged OPEN beats a false close.
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 10
    persisted = _alerts("UNATTRIBUTED_CLOSE")
    assert len(persisted) == 1
    assert persisted[0]["symbol"] == "A"
    assert persisted[0]["product"] == "MIS"


def test_unattributed_close_gtt_triggered_but_order_open(clean_positions,
                                                         monkeypatch):
    """A triggered GTT whose FIRED order is still OPEN (not COMPLETE) is NOT
    positive evidence → the deficit is UNATTRIBUTED, position stays OPEN."""
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
    assert any(a["action"] == "UNATTRIBUTED_CLOSE" for a in actions)
    assert _row(sess, "A")["status"] == "OPEN"    # never close on non-COMPLETE


# ── ORPHAN_AT_BROKER: broker holds MORE than we track → ALERT, nothing mutated ─

def test_orphan_at_broker_alert(clean_positions, monkeypatch):
    """Broker net (17) > Σ tracked (10) → an untracked lot at the broker. ALERT;
    NEVER adopt or mutate our position.

    NOTE: 17 vs 10 is a NON-clean ratio deliberately — GUARD G3 reclassifies a
    CLEAN corp-action surplus (e.g. 30 = 10×3) as CORP_ACTION_SUSPECTED, so this
    generic-orphan case must use a diff that is NOT a split/bonus multiple."""
    net_book = {"A": {"quantity": 17, "buy_quantity": 17, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    orphan = [a for a in actions if a["action"] == "ORPHAN_AT_BROKER"]
    assert len(orphan) == 1
    assert orphan[0]["extra"] == 7
    # Our position is untouched (never adopt the broker's extra).
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 10
    assert len(_alerts("ORPHAN_AT_BROKER")) == 1


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


def test_exit_failed_no_evidence_unattributed(clean_positions, monkeypatch):
    """An EXIT_FAILED row with NO attributable filled order + a broker deficit →
    UNATTRIBUTED_CLOSE alert, NOT auto-closed (we never guess a fill price)."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 15, "sell_quantity": 15,
                      "sell_price": 97.8, "average_price": 100.0,
                      "exchange": "NSE", "product": "MIS"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="MIS")
    _register(sess, "A", 15, 100.0, ltp=100.0, status="EXIT_FAILED")
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    assert any(a["action"] == "UNATTRIBUTED_CLOSE" for a in actions)
    assert _row(sess, "A")["status"] == "EXIT_FAILED"    # unchanged, flagged


# ── END-TO-END through session.tick(): close surfaces under broker_reconciled ─

def test_close_via_order_id_at_tick(clean_positions, monkeypatch):
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
