"""AUTHORITATIVE broker→DB position reconciler tests.

The broker is the SINGLE SOURCE OF TRUTH for position existence / qty / exit
price. Each scenario builds a LIVE-mode session (dry_run=False so the reconcile
path runs), seeds OPEN/EXIT_FAILED rows directly in autotrade_positions, injects
a mock broker net book, runs ONE reconcile tick, and asserts panel == broker.

Harness mirrors test_entry_firing.py: patch broker.router.build_client to return
a MockBroker, and freeze "now" to a mid-session trading day (Thu 2026-06-25 10:00
IST) so tick()'s market-hours guard is satisfied and nothing whipsaws.

SAFETY invariants under test (the whole point):
  * None book (broker unreachable / paper) → ZERO changes.
  * EMPTY book ([]) while holding OPEN → ZERO changes (transient blip).
  * ABSENT from a non-empty book → flagged, NOT closed.
  * day-flat (net 0) → CLOSED at the REAL broker sell/buy avg.
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


def _patch_brokers(monkeypatch, net_book, ltps=None):
    """Patch build_client so every broker profile gets a MockBroker carrying the
    given net_book + ltps. Returns the dict of created brokers by profile_id."""
    created = {}
    ltps = ltps or {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=ltps,
                        net_book=net_book)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _make_live_session(monkeypatch, net_book, ltps=None, *,
                       kill_switch_enabled=False, capital=300000.0, top_n=3,
                       order_product="CNC"):
    _patch_brokers(monkeypatch, net_book, ltps=ltps)
    cfg = TradingSessionConfig(total_allocated_capital=capital, top_n_stocks=top_n,
                               sizing_mode="equal",
                               kill_switch_enabled=kill_switch_enabled,
                               kill_switch_pct=0.02, order_product=order_product)
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _register(sess, symbol, qty, avg_price, *, ltp=None, status="OPEN",
              instrument_type="EQ", direction="long", exchange="NSE"):
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=avg_price, product="MIS",
                           instrument_type=instrument_type, exchange=exchange,
                           direction=direction)
    if ltp is not None:
        sess.registry.update_ltp(symbol, ltp, broker_profile=prof)
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
            "realised_pnl FROM autotrade_positions WHERE session_id=? AND symbol=?",
            (sess.session_id, symbol)).fetchone()
    return dict(r) if r else None


def _freeze(sess):
    sess.monitor.freeze_invested_basis()


def _invested_basis(sess):
    with falcon_conn() as con:
        r = con.execute("SELECT invested_basis FROM autotrade_sessions "
                        "WHERE session_id=?", (sess.session_id,)).fetchone()
    return r["invested_basis"] if r else None


# ── (a) New order placed, not yet filled → buy in progress, NO change ─────────

def test_a_buy_in_progress_no_change(clean_positions, monkeypatch):
    """Broker shows a buy started (quantity>0, sell_quantity=0). The position
    stays OPEN — the reconciler makes NO change (net qty > 0 and == DB qty)."""
    net_book = {"A": {"quantity": 10, "buy_quantity": 10, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0})
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions == []
    r = _row(sess, "A")
    assert r["status"] == "OPEN"
    assert r["qty"] == 10


# ── (b) Partial fill → broker qty < DB qty → corrected + basis re-frozen ──────

def test_b_partial_fill_qty_corrected(clean_positions, monkeypatch):
    net_book = {"A": {"quantity": 6, "buy_quantity": 6, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0})
    _register(sess, "A", 10, 100.0, ltp=100.0)   # DB thinks 10; broker holds 6
    _freeze(sess)
    assert _invested_basis(sess) == pytest.approx(10 * 100.0)

    actions = reconcile_broker_positions(sess)
    assert len(actions) == 1
    assert actions[0]["action"] == "QTY_CORRECTED"
    assert actions[0]["from"] == 10 and actions[0]["to"] == 6
    r = _row(sess, "A")
    assert r["status"] == "OPEN"
    assert r["qty"] == 6
    # invested_basis re-frozen to the corrected qty.
    assert _invested_basis(sess) == pytest.approx(6 * 100.0)


# ── (c) Stray OPEN row absent from a non-empty book → ABSENT_REVIEW, not closed

def test_c_absent_from_nonempty_book_review_not_closed(clean_positions,
                                                       monkeypatch):
    net_book = {"A": {"quantity": 10, "buy_quantity": 10, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0, "Z": 50.0})
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _register(sess, "Z", 5, 50.0, ltp=50.0)      # Z is NOT in the broker book
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    acts = {a["symbol"]: a for a in actions}
    assert acts["Z"]["action"] == "ABSENT_REVIEW"
    # Z must remain OPEN — a false close is worse than a flagged stale-open.
    assert _row(sess, "Z")["status"] == "OPEN"
    assert _row(sess, "A")["status"] == "OPEN"   # A in sync, untouched


# ── (d) Stop-loss / trailing trigger → day-flat → CLOSED at sell avg ──────────

def test_d_stop_triggered_day_flat_closed_at_sell_avg(clean_positions,
                                                      monkeypatch):
    """Defense-in-depth beyond the GTT path: a broker SL fired (bought 10, sold
    10 at 96.5) → day-flat → CLOSED at the SELL average, not the mark."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "buy_price": 100.0, "sell_price": 96.5,
                      "average_price": 100.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0})
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert len(actions) == 1
    a = actions[0]
    assert a["action"] == "CLOSED_EXTERNAL"
    assert a["exit_price"] == pytest.approx(96.5)
    assert "exit_price_approx" not in a
    r = _row(sess, "A")
    assert r["status"] == "CLOSED"
    assert r["close_reason"] == "BROKER_RECONCILED_FLAT"
    assert r["exit_price"] == pytest.approx(96.5)
    # realised P&L computed from the REAL sell avg, not the mark.
    assert r["realised_pnl"] == pytest.approx((96.5 - 100.0) * 10)


# ── (e) Manual broker-side exit → CLOSED at real sell avg + correct P&L ───────

def test_e_manual_broker_exit_closed_correct_pnl(clean_positions, monkeypatch):
    """THE KEY new capability: a manual broker exit (no GTT, not our path). The
    position is day-flat at a REAL sell avg → CLOSED with the correct realised P&L."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 20, "sell_quantity": 20,
                      "buy_price": 100.0, "sell_price": 104.25,
                      "average_price": 100.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0})
    _register(sess, "A", 20, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions[0]["action"] == "CLOSED_EXTERNAL"
    assert actions[0]["source"] == "BROKER_RECONCILED_FLAT"
    r = _row(sess, "A")
    assert r["status"] == "CLOSED"
    assert r["exit_price"] == pytest.approx(104.25)
    assert r["realised_pnl"] == pytest.approx((104.25 - 100.0) * 20)


# ── (f) Our own auto-exit already CLOSED → idempotent no-op ───────────────────

def test_f_already_closed_idempotent_noop(clean_positions, monkeypatch):
    """A position we already CLOSED (our exit path) must not be re-touched — the
    reconciler only examines OPEN/EXIT_FAILED rows."""
    # Broker still shows a day-flat row, but our DB row is already CLOSED.
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "sell_price": 96.5, "average_price": 100.0,
                      "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0})
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
    assert after["close_reason"] == "OUR_EXIT"        # NOT overwritten
    assert after["exit_price"] == pytest.approx(98.0)  # our price, not 96.5
    assert after == before


# ── (g) Carried overnight → resume: still-held no-op; flat-overnight closed ───

def test_g_overnight_still_held_no_change(clean_positions, monkeypatch):
    net_book = {"A": {"quantity": 10, "buy_quantity": 10, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0})
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _row(sess, "A")["status"] == "OPEN"


def test_g_overnight_flat_at_broker_closed(clean_positions, monkeypatch):
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "sell_price": 99.0, "average_price": 100.0,
                      "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0})
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions[0]["action"] == "CLOSED_EXTERNAL"
    assert _row(sess, "A")["status"] == "CLOSED"
    assert _row(sess, "A")["exit_price"] == pytest.approx(99.0)


# ── (h) API delay: None book AND empty book → ZERO changes (most important) ───

def test_h_none_book_never_flattens(clean_positions, monkeypatch):
    """Broker session reconnect / API delay → get_positions_net() returns None →
    the reconciler must make ZERO changes (never flatten the DB)."""
    sess = _make_live_session(monkeypatch, net_book=None, ltps={"A": 100.0})
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 10


def test_h_empty_book_never_flattens(clean_positions, monkeypatch):
    """An EMPTY net book ([]) while we hold OPEN positions is a transient API
    blip → ZERO changes (a genuine close shows a day-flat row, not an empty book)."""
    sess = _make_live_session(monkeypatch, net_book=[], ltps={"A": 100.0})
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 10


def test_h_paper_session_noop(clean_positions, monkeypatch):
    """Paper (dry_run) → the reconciler returns [] immediately and mutates
    nothing, even with a broker net book that would otherwise flatten."""
    _patch_brokers(monkeypatch,
                   net_book={"A": {"quantity": 0, "buy_quantity": 10,
                                   "sell_quantity": 10, "sell_price": 96.5,
                                   "exchange": "NSE"}},
                   ltps={"A": 100.0})
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    sess._build_brokers()
    _register(sess, "A", 10, 100.0, ltp=100.0)

    actions = reconcile_broker_positions(sess)
    assert actions == []
    assert _row(sess, "A")["status"] == "OPEN"   # paper is byte-for-byte unchanged


# ── (i) Divergence in qty + avg + status → corrected to broker + P&L recomputed

def test_i_qty_avg_status_all_corrected_to_broker(clean_positions, monkeypatch):
    """Broker/panel diverge in qty AND avg. Broker holds 8 @ 102 (>0.5% off the
    DB's 90); DB thinks 10 @ 100. The reconciler corrects qty→8, avg→102, and
    unrealised_pnl is recomputed from the broker truth at the persisted ltp."""
    net_book = {"A": {"quantity": 8, "buy_quantity": 8, "sell_quantity": 0,
                      "average_price": 102.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 105.0})
    _register(sess, "A", 10, 90.0, ltp=105.0)    # divergent qty AND avg
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions[0]["action"] == "QTY_CORRECTED"
    assert actions[0]["from"] == 10 and actions[0]["to"] == 8
    assert actions[0]["avg_to"] == pytest.approx(102.0)
    with falcon_conn() as con:
        r = con.execute(
            "SELECT qty, avg_price, unrealised_pnl FROM autotrade_positions "
            "WHERE session_id=? AND symbol=?",
            (sess.session_id, "A")).fetchone()
    assert r["qty"] == 8
    assert r["avg_price"] == pytest.approx(102.0)
    # uPnL recomputed from broker avg + persisted ltp: (105-102)*8 = 24.
    assert r["unrealised_pnl"] == pytest.approx((105.0 - 102.0) * 8)


# ── today's incident regression: EXIT_FAILED + broker day-flat → CLOSED ───────

def test_exit_failed_day_flat_reconciled_closed(clean_positions, monkeypatch):
    """The 2026-07-06 MIS case: a MIS basket was RMS-squared at 15:25 but our DB
    stayed EXIT_FAILED with wrong P&L. The reconciler sees the day-flat broker row
    and marks it CLOSED at the real sell avg."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 15, "sell_quantity": 15,
                      "buy_price": 100.0, "sell_price": 97.8,
                      "average_price": 100.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0})
    _register(sess, "A", 15, 100.0, ltp=100.0, status="EXIT_FAILED")
    _freeze(sess)

    actions = reconcile_broker_positions(sess)
    assert actions[0]["action"] == "CLOSED_EXTERNAL"
    r = _row(sess, "A")
    assert r["status"] == "CLOSED"
    assert r["exit_price"] == pytest.approx(97.8)
    assert r["realised_pnl"] == pytest.approx((97.8 - 100.0) * 15)


# ── panel==broker via a full reconcile TICK (not just the raw reconcile fn) ───

def test_reconcile_runs_at_tick_start(clean_positions, monkeypatch):
    """End-to-end through session.tick(): the day-flat position is CLOSED and the
    tick dict surfaces the action under 'broker_reconciled'."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "sell_price": 95.0, "average_price": 100.0,
                      "exchange": "NSE"},
                "B": {"quantity": 5, "buy_quantity": 5, "sell_quantity": 0,
                      "average_price": 200.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book,
                              ltps={"A": 100.0, "B": 200.0})
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _register(sess, "B", 5, 200.0, ltp=200.0)
    _freeze(sess)

    tick = asyncio.run(sess.tick())
    recon = {a["symbol"]: a for a in tick.get("broker_reconciled", [])}
    assert recon["A"]["action"] == "CLOSED_EXTERNAL"
    assert _row(sess, "A")["status"] == "CLOSED"
    assert _row(sess, "B")["status"] == "OPEN"   # B in sync


# ── OVERNIGHT CNC / holdings safety (the T+1 delivery bug) ─────────────────────
# CRITICAL: an equity CNC position settles OUT of positions()['net'] into holdings
# on T+1 — the next day it shows net 0 (sell_quantity=0) or vanishes from the net
# book WITHOUT a sell. The reconciler must NEVER close a position that has no exit
# trade; holdings is the source of truth for a delivered CNC hold.

def _set_holdings(sess, holdings):
    for b in sess.brokers.values():
        b._holdings = holdings


def test_overnight_cnc_net_zero_no_sell_but_in_holdings_not_closed(
        clean_positions, monkeypatch):
    """THE BUG: a delivered CNC shows net 0 with sell_quantity=0 in the net book
    but is STILL HELD in holdings. It must NOT be closed."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 0, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE", "product": "CNC"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 105.0},
                              order_product="CNC")
    _register(sess, "A", 10, 100.0, ltp=105.0)
    _set_holdings(sess, {"A": {"quantity": 10, "t1_quantity": 0,
                               "average_price": 100.0}})
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    assert all(a["action"] != "CLOSED_EXTERNAL" for a in actions)
    assert _row(sess, "A")["status"] == "OPEN"
    assert _row(sess, "A")["qty"] == 10


def test_overnight_cnc_absent_from_net_but_in_holdings_not_closed(
        clean_positions, monkeypatch):
    """Day 2+: the CNC has fully left the net book (it's in holdings). Absent from
    a NON-empty net book but held in holdings → in-sync, NOT closed/flagged."""
    net_book = {"OTHER": {"quantity": 5, "buy_quantity": 5, "sell_quantity": 0,
                          "average_price": 50.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 105.0},
                              order_product="CNC")
    _register(sess, "A", 10, 100.0, ltp=105.0)
    _set_holdings(sess, {"A": {"quantity": 10, "t1_quantity": 0}})
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    a_acts = [a for a in actions if a["symbol"] == "A"]
    assert all(a["action"] not in ("CLOSED_EXTERNAL", "ABSENT_REVIEW")
               for a in a_acts)
    assert _row(sess, "A")["status"] == "OPEN"


def test_overnight_cnc_t1_quantity_counts_as_held(clean_positions, monkeypatch):
    """A CNC bought yesterday sits in holdings as t1_quantity (not yet delivered)
    — that still counts as HELD → not closed."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 0, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 104.0},
                              order_product="CNC")
    _register(sess, "A", 10, 100.0, ltp=104.0)
    _set_holdings(sess, {"A": {"quantity": 0, "t1_quantity": 10}})
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    assert all(a["action"] != "CLOSED_EXTERNAL" for a in actions)
    assert _row(sess, "A")["status"] == "OPEN"


def test_cnc_net_zero_WITH_sell_is_closed_at_sell_avg(clean_positions,
                                                      monkeypatch):
    """A CNC GENUINELY sold (sell_quantity>0) → net 0 WITH exit evidence AND not in
    holdings → CLOSED at the real sell average. Confirms we still catch a real exit."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 10, "sell_quantity": 10,
                      "sell_price": 108.0, "average_price": 100.0,
                      "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 108.0},
                              order_product="CNC")
    _register(sess, "A", 10, 100.0, ltp=108.0)
    _set_holdings(sess, None)   # sold → not held
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    assert any(a["action"] == "CLOSED_EXTERNAL" for a in actions)
    r = _row(sess, "A")
    assert r["status"] == "CLOSED"
    assert r["exit_price"] == pytest.approx(108.0)
    assert r["realised_pnl"] == pytest.approx((108.0 - 100.0) * 10)


def test_cnc_net_zero_no_sell_no_holdings_info_not_closed(clean_positions,
                                                          monkeypatch):
    """Defensive: net 0, NO sell trade, and holdings unavailable (None) → NEVER
    closed (a delivery position is only closed on a real exit trade)."""
    net_book = {"A": {"quantity": 0, "buy_quantity": 0, "sell_quantity": 0,
                      "average_price": 100.0, "exchange": "NSE"}}
    sess = _make_live_session(monkeypatch, net_book, ltps={"A": 100.0},
                              order_product="CNC")
    _register(sess, "A", 10, 100.0, ltp=100.0)
    _set_holdings(sess, None)
    _freeze(sess)
    actions = reconcile_broker_positions(sess)
    assert all(a["action"] != "CLOSED_EXTERNAL" for a in actions)
    assert _row(sess, "A")["status"] == "OPEN"
