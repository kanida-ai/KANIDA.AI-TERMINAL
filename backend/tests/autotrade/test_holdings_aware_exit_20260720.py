"""HOLDINGS-AWARE PRE-EXIT flat guard — the 2026-07-20 leaked-BTST fix.

GROUND TRUTH (verified live on the Rupeezy broker 2026-07-20): a BTST/CNC
positional session (strategy btst_oscillator, max_hold_sessions=2) entered Fri
07-17 should have sold Mon 07-20 at 15:29. MAX_HOLD_EXIT fired but ZERO sells
were placed — 5 names stayed HELD at the broker. Root cause: the pre-exit
reconcile guard read ONLY the broker's DAY net-positions book. After T+1
settlement an overnight CNC lot LEAVES the day book and lives in HOLDINGS, so the
day-net read 0 → our_held==0 → the guard booked the position CLOSED
(…_RECONCILED_FLAT) and placed NO order.

THE FIX (BROKER-AGNOSTIC): BrokerClient.held_qty_for_exit() combines
|day net| + Σ holdings(quantity + t1_quantity) for CNC/delivery legs, day-net
only for MIS/NRML/MTF/F&O, via the adapter's own broker_held_qty semantics. It
lives in base.py and consumes only the two STANDARD reads (get_net_position_qty /
get_holdings) every adapter already implements — so Zerodha, Rupeezy, and every
FUTURE broker are covered by the ONE shared method (no `if broker == "…"`).

These tests drive the fix through a broker-agnostic MockBroker (which inherits
the base held_qty_for_exit UNCHANGED) so they prove the SHARED seam, not a
per-broker patch. MUTATION-PROOF: reverting the base method turns them RED.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import (TradingSession, set_fake_now,
                               _exit_single_position)
from tests.autotrade.mock_broker import MockBroker
from autotrade.broker.base import BrokerClient
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 7, 20, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


# ── harness (mirrors test_session_scoped_flat_guard, + a holdings book) ────────
def _patch(monkeypatch, net_positions, holdings=None, ltps=None):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=ltps or {},
                        net_positions=net_positions, holdings=holdings)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _mk(monkeypatch, net_positions, holdings=None, ltps=None, build=True,
        order_product="CNC"):
    created = _patch(monkeypatch, net_positions, holdings, ltps)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               kill_switch_pct=0.02, order_product=order_product)
    sess = TradingSession.create(cfg, mode="live")
    if build:
        sess._build_brokers()
    return sess, created


def _reg(sess, symbol, qty, avg, ltp, product="CNC", instrument_type="EQ"):
    prof = sess.config.broker_profiles[0].profile_id \
        if sess.config.broker_profiles else "default"
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=avg, product=product,
                           instrument_type=instrument_type, exchange="NSE",
                           direction="long")
    sess.registry.update_ltp(symbol, ltp, broker_profile=prof)


def _row(sess, symbol):
    with falcon_conn() as con:
        r = con.execute("SELECT status, qty FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (sess.session_id, symbol)).fetchone()
    return dict(r) if r else None


# Kite-shaped holdings row (tradingsymbol / quantity / t1_quantity).
def _kite_hold(sym, quantity, t1=0):
    return {"tradingsymbol": sym, "quantity": quantity, "t1_quantity": t1,
            "average_price": 100.0, "product": "CNC"}


# Rupeezy-NORMALISED holdings row: RupeezyBroker.get_holdings emits the SAME
# tradingsymbol/quantity/t1_quantity contract (quantity=total_free) but keeps the
# original nse/bse/total_free/isin fields on the row. Proves the base method reads
# the normalised contract, not any Kite-only field.
def _rupeezy_hold(sym, quantity, t1=0):
    return {"tradingsymbol": sym, "quantity": quantity, "t1_quantity": t1,
            "average_price": 100.0, "product": "CNC",
            "nse": {"symbol": sym, "token": 999}, "bse": {},
            "total_free": quantity, "dp_free": quantity, "isin": "IN" + sym}


# ══════════════════════════════════════════════════════════════════════════════
# 1. PURE base-method unit — the holdings-aware read itself.
# ══════════════════════════════════════════════════════════════════════════════
def test_held_qty_for_exit_settled_cnc_kite_shape():
    """Settled CNC: day-net 0 (left the day book) + holdings 76 → 76, NOT 0."""
    b = MockBroker(profile=None, dry_run=False,
                   net_positions={"AEGISLOG": 0},
                   holdings=[_kite_hold("AEGISLOG", 76, t1=0)])
    assert b.get_net_position_qty("AEGISLOG", "EQ") == 0          # the OLD read
    assert b.held_qty_for_exit("AEGISLOG", "EQ", "CNC") == 76     # the FIX


def test_held_qty_for_exit_settled_cnc_rupeezy_shape():
    """Same fix on a Rupeezy-normalised holdings row (settled + T+1 split)."""
    b = MockBroker(profile=None, dry_run=False,
                   net_positions={"BIOCON": 0},
                   holdings=[_rupeezy_hold("BIOCON", 200, t1=44)])
    # quantity(200 settled) + t1_quantity(44 not-yet-delivered) = 244 held.
    assert b.held_qty_for_exit("BIOCON", "EQ", "CNC") == 244


def test_held_qty_for_exit_same_day_cnc_buy_no_double_count():
    """A same-day CNC buy sits in the DAY net (not yet holdings) → net 76 +
    holdings 0 = 76 (broker_held_qty's hq + max(0,net); no double count)."""
    b = MockBroker(profile=None, dry_run=False,
                   net_positions={"KALYANKJIL": 76}, holdings=[])
    assert b.held_qty_for_exit("KALYANKJIL", "EQ", "CNC") == 76


def test_held_qty_for_exit_mis_ignores_holdings():
    """MIS never settles to holdings → day-net ONLY. Even with a (spurious)
    holdings row, a day-flat MIS reads 0 (so it correctly reconciles flat)."""
    b = MockBroker(profile=None, dry_run=False,
                   net_positions={"HFCL": 0},
                   holdings=[_kite_hold("HFCL", 461)])
    assert b.held_qty_for_exit("HFCL", "EQ", "MIS") == 0
    assert b.is_delivery_product("MIS", "EQ") is False
    assert b.is_delivery_product("CNC", "EQ") is True
    assert b.is_delivery_product("EQ", "EQ") is True       # EQ→CNC delivery
    assert b.is_delivery_product("CNC", "FUT") is False    # F&O never delivery


def test_held_qty_for_exit_paper_none():
    """Paper / not-live: get_net_position_qty None → None (caller proceeds)."""
    b = MockBroker(profile=None, dry_run=False, net_positions=None,
                   holdings=[_kite_hold("X", 50)])
    assert b.held_qty_for_exit("X", "EQ", "CNC") is None


def test_held_qty_for_exit_holdings_unavailable_treated_as_empty():
    """CONTRACT-PRESERVING: CNC day-net 0 with holdings UNAVAILABLE (None) is
    treated as an EMPTY holdings book → held 0 → a genuinely-flat CNC still
    reconciles flat EXACTLY as before the fix. (A full broker outage RAISES on the
    net read first — B1 — so this narrow partial-failure never places a blind
    order.) A settled lot's real fix relies on holdings being AVAILABLE, which it
    was in the live 2026-07-20 incident."""
    b = MockBroker(profile=None, dry_run=False,
                   net_positions={"MAPMYINDIA": 0}, holdings=None)
    assert b.held_qty_for_exit("MAPMYINDIA", "EQ", "CNC") == 0


def test_held_qty_for_exit_daynet_error_raises_b1():
    """A live day-net error must RAISE (B1) so the caller aborts — never a blind
    exit. held_qty_for_exit inherits get_net_position_qty's fail-loud contract."""
    b = MockBroker(profile=None, dry_run=False,
                   net_positions={"AEGISLOG": 0},
                   holdings=[_kite_hold("AEGISLOG", 76)],
                   net_probe_raise_symbols={"AEGISLOG"})
    with pytest.raises(ConnectionResetError):
        b.held_qty_for_exit("AEGISLOG", "EQ", "CNC")


def test_base_default_is_none():
    """The abstract base default (no overrides) returns None (paper-safe) — a stub
    broker never false-flats."""
    class _Bare(BrokerClient):
        broker_name = "bare"
        def get_ltp(self, s): return None
        def get_lot_size(self, c): return 1
        def get_active_futures(self, s, e): return ""
        def get_option_chain(self, s): return []
        def get_option_contract(self, s, k, e): return ""
        async def place_order(self, o): ...
        async def get_pending_orders(self): return []
        async def cancel_order(self, oid): ...
        async def place_market_exit(self, *a, **k): ...
    assert _Bare(profile=None).held_qty_for_exit("X", "EQ", "CNC") is None


# ══════════════════════════════════════════════════════════════════════════════
# 2. THE REPRODUCTION — a settled CNC exit must PLACE a sell, not RECONCILE_FLAT.
# ══════════════════════════════════════════════════════════════════════════════
def test_reproduction_kill_path_places_sell_for_settled_cnc(
        clean_positions, monkeypatch):
    """MAX_HOLD_EXIT flattens through kill_switch.fire. A settled CNC name
    (day-net 0, holdings 76) must PLACE a market sell of 76 — NOT reconcile flat.
    This is the exact 2026-07-20 AEGISLOG failure."""
    sess, created = _mk(monkeypatch, net_positions={"AEGISLOG": 0},
                        holdings=[_kite_hold("AEGISLOG", 76)],
                        ltps={"AEGISLOG": 250.0})
    _reg(sess, "AEGISLOG", 76, 240.0, 250.0, product="CNC")

    asyncio.run(sess.kill_switch.fire("MAX_HOLD_EXIT test", gross_return=0.0,
                                      close_reason="MAX_HOLD_EXIT"))

    prof = sess.config.broker_profiles[0].profile_id
    broker = created[prof]
    placed = [(s, q) for s, q in broker.exits if s == "AEGISLOG"]
    assert placed == [("AEGISLOG", 76)], "settled CNC must SELL 76, not flat"
    # NOT booked as a phantom RECONCILED_FLAT.
    assert "RECONCILED_FLAT" not in (_current_close_reason(sess, "AEGISLOG") or "")


def test_reproduction_exit_single_position_places_sell(
        clean_positions, monkeypatch):
    """The per-position exit path (_exit_single_position, used by the tick /
    per-stock stop / retry sweep) is ALSO holdings-aware for a settled CNC."""
    sess, created = _mk(monkeypatch, net_positions={"MAPMYINDIA": 0},
                        holdings=[_rupeezy_hold("MAPMYINDIA", 106)],
                        ltps={"MAPMYINDIA": 1800.0})
    _reg(sess, "MAPMYINDIA", 106, 1750.0, 1800.0, product="CNC")

    pos = sess.registry.get_open_positions()[0]
    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="MAX_HOLD_EXIT",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    prof = sess.config.broker_profiles[0].profile_id
    broker = created[prof]
    assert any(s == "MAPMYINDIA" and q == 106 for s, q in broker.exits)
    assert res["status"] not in ("RECONCILED_FLAT",)


# ══════════════════════════════════════════════════════════════════════════════
# 3. SHARED-LOGIN — two BTST sessions on ONE login, each sells ITS slice.
# ══════════════════════════════════════════════════════════════════════════════
def test_shared_login_two_sessions_each_sell_attributed_qty(
        clean_positions, monkeypatch):
    """Two sessions hold the SAME settled CNC name (holdings 344 = 228 + 116, day-
    net 0). Each session's exit sells ITS attributed qty (228 vs 116) — no
    oversell, no double-sell. This is the BIOCON 228/116 shared-login case."""
    hold = [_kite_hold("BIOCON", 344)]        # broker holdings = both lots
    sess_a, created_a = _mk(monkeypatch, net_positions={"BIOCON": 0},
                            holdings=hold, ltps={"BIOCON": 340.0})
    _reg(sess_a, "BIOCON", 228, 330.0, 340.0, product="CNC")
    sess_b, created_b = _mk(monkeypatch, net_positions={"BIOCON": 0},
                            holdings=hold, ltps={"BIOCON": 340.0})
    _reg(sess_b, "BIOCON", 116, 330.0, 340.0, product="CNC")

    asyncio.run(sess_a.kill_switch.fire("MAX_HOLD_EXIT", gross_return=0.0,
                                        close_reason="MAX_HOLD_EXIT"))
    asyncio.run(sess_b.kill_switch.fire("MAX_HOLD_EXIT", gross_return=0.0,
                                        close_reason="MAX_HOLD_EXIT"))

    prof = sess_a.config.broker_profiles[0].profile_id
    a_sold = sum(q for s, q in created_a[prof].exits if s == "BIOCON")
    b_sold = sum(q for s, q in created_b[prof].exits if s == "BIOCON")
    assert a_sold == 228, f"session A must sell its 228 (got {a_sold})"
    assert b_sold == 116, f"session B must sell its 116 (got {b_sold})"
    # No oversell: the two sessions together never sell more than the 344 held.
    assert a_sold + b_sold == 344


# ══════════════════════════════════════════════════════════════════════════════
# 4. MIS REGRESSION — intraday must NOT consult holdings (still reconciles flat).
# ══════════════════════════════════════════════════════════════════════════════
def test_mis_daynet_zero_still_reconciles_flat(clean_positions, monkeypatch):
    """An MIS position, day-net 0 → the exit still reconciles FLAT and places NO
    order (MIS never settles to holdings, so holdings must NOT be consulted). This
    proves the fix did NOT change intraday behaviour — even with a spurious
    holdings row present."""
    sess, created = _mk(monkeypatch, net_positions={"HFCL": 0},
                        holdings=[_kite_hold("HFCL", 461)],  # spurious for MIS
                        ltps={"HFCL": 90.0}, order_product="MIS")
    _reg(sess, "HFCL", 461, 88.0, 90.0, product="MIS")

    asyncio.run(sess.kill_switch.fire("KILL test", gross_return=-0.05,
                                      close_reason="KILL_SWITCH"))

    prof = sess.config.broker_profiles[0].profile_id
    broker = created[prof]
    assert not any(s == "HFCL" for s, _q in broker.exits), \
        "MIS day-flat must reconcile flat, NOT consult holdings + sell"
    assert _row(sess, "HFCL")["status"] in ("CLOSED", "EXIT_FAILED")


def _current_close_reason(sess, symbol):
    with falcon_conn() as con:
        r = con.execute("SELECT close_reason FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (sess.session_id, symbol)).fetchone()
    return (r["close_reason"] if r else None)
