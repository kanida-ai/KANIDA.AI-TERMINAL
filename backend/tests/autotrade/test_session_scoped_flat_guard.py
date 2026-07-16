"""C1 — SESSION-SCOPED pre-exit flat guard.

The broker net book is ACCOUNT-wide. When two sessions hold the same symbol,
session A exiting must decide "already flat" on ITS OWN shares — never the whole
account net — so it never (a) reads session B's still-open shares as its own, nor
(b) places a market exit that OVERSELLS into B's lot.

Each scenario builds LIVE-mode sessions (dry_run=False mock), seeds OPEN rows,
injects a broker net-position book, fires a kill, and asserts the exit did NOT
place a naked/oversell order and reconciled only the firing session.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import (TradingSession, set_fake_now,
                               _exit_single_position)
from autotrade.monitoring.registry import (sibling_open_qty,
                                           our_held_at_broker)
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _patch(monkeypatch, net_positions, ltps=None):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False,
                        ltps=ltps or {}, net_positions=net_positions)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _mk(monkeypatch, net_positions, ltps=None, build=True):
    created = _patch(monkeypatch, net_positions, ltps)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               kill_switch_pct=0.02, order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    if build:
        sess._build_brokers()
    return sess, created


def _reg(sess, symbol, qty, avg, ltp):
    prof = sess.config.broker_profiles[0].profile_id \
        if sess.config.broker_profiles else "default"
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=avg, product="CNC", instrument_type="EQ",
                           exchange="NSE", direction="long")
    sess.registry.update_ltp(symbol, ltp, broker_profile=prof)


def _row(sess, symbol):
    with falcon_conn() as con:
        r = con.execute("SELECT status, qty FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (sess.session_id, symbol)).fetchone()
    return dict(r) if r else None


# ── pure helper units ─────────────────────────────────────────────────────────
def test_our_held_none_in_paper(clean_positions, monkeypatch):
    sess, _ = _mk(monkeypatch, {"X": 0})
    _reg(sess, "X", 100, 100.0, 100.0)
    # broker_net None (paper / unknown) → None → caller proceeds normally.
    assert our_held_at_broker(sess.session_id, "X", "EQ", 100, None) is None


def test_our_held_single_session_byte_identical(clean_positions, monkeypatch):
    sess, _ = _mk(monkeypatch, {"X": 0})
    _reg(sess, "X", 100, 100.0, 100.0)
    # No siblings: fully flat broker → 0; fully held → our_qty; matches the old
    # `broker_net == 0` decision for the single-session case.
    assert our_held_at_broker(sess.session_id, "X", "EQ", 100, 0) == 0
    assert our_held_at_broker(sess.session_id, "X", "EQ", 100, 100) == 100
    assert our_held_at_broker(sess.session_id, "X", "EQ", 100, 250) == 100  # clamp


def test_sibling_qty_subtracted(clean_positions, monkeypatch):
    sess_a, _ = _mk(monkeypatch, {"X": 100})
    _reg(sess_a, "X", 100, 100.0, 100.0)
    sess_b, _ = _mk(monkeypatch, {"X": 100}, build=False)
    _reg(sess_b, "X", 100, 100.0, 100.0)
    # A's siblings hold 100; broker only shows 100 net → A's shares are GONE.
    assert sibling_open_qty(sess_a.session_id, "X", "EQ") == 100
    assert our_held_at_broker(sess_a.session_id, "X", "EQ", 100, 100) == 0


# ── the real-money scenario ───────────────────────────────────────────────────
def test_kill_does_not_oversell_into_sibling_lot(clean_positions, monkeypatch):
    """Two sessions hold X. A was stale-closed (broker net = B's 100 only). A's
    kill must NOT place an exit (that would sell into B's lot) and reconciles A."""
    sess_a, created = _mk(monkeypatch, {"X": 100}, ltps={"X": 99.0})
    _reg(sess_a, "X", 100, 100.0, 99.0)
    sess_b, _ = _mk(monkeypatch, {"X": 100}, ltps={"X": 99.0}, build=False)
    _reg(sess_b, "X", 100, 100.0, 99.0)

    asyncio.run(sess_a.kill_switch.fire("TEST", gross_return=-0.05))

    prof = sess_a.config.broker_profiles[0].profile_id
    broker = created[prof]
    # NO oversell: A never placed a market exit for X.
    assert not any(s == "X" for s, _q in broker.exits)
    # A is reconciled (closed, not left OPEN); B is untouched.
    assert _row(sess_a, "X")["status"] in ("CLOSED", "EXIT_FAILED")
    assert _row(sess_b, "X") == {"status": "OPEN", "qty": 100}


def test_single_session_full_held_still_exits(clean_positions, monkeypatch):
    """No siblings, broker holds our full qty → the exit STILL fires (byte-
    identical to before: our_held == our_qty > 0)."""
    sess, created = _mk(monkeypatch, {"X": 100}, ltps={"X": 99.0})
    _reg(sess, "X", 100, 100.0, 99.0)

    asyncio.run(sess.kill_switch.fire("TEST", gross_return=-0.05))

    prof = sess.config.broker_profiles[0].profile_id
    broker = created[prof]
    assert any(s == "X" for s, _q in broker.exits)


def test_single_session_flat_reconciles(clean_positions, monkeypatch):
    """No siblings, broker flat (0) → reconcile, place nothing (unchanged)."""
    sess, created = _mk(monkeypatch, {"X": 0}, ltps={"X": 99.0})
    _reg(sess, "X", 100, 100.0, 99.0)

    asyncio.run(sess.kill_switch.fire("TEST", gross_return=-0.05))

    prof = sess.config.broker_profiles[0].profile_id
    broker = created[prof]
    assert not any(s == "X" for s, _q in broker.exits)
    assert _row(sess, "X")["status"] in ("CLOSED", "EXIT_FAILED")


def test_exit_single_position_scoped_flat(clean_positions, monkeypatch):
    """The per-stock-stop / retry path (_exit_single_position) uses the same
    session-scoped guard: a sibling-held symbol is not oversold."""
    sess_a, created = _mk(monkeypatch, {"X": 100}, ltps={"X": 99.0})
    _reg(sess_a, "X", 100, 100.0, 99.0)
    sess_b, _ = _mk(monkeypatch, {"X": 100}, ltps={"X": 99.0}, build=False)
    _reg(sess_b, "X", 100, 100.0, 99.0)

    pos = sess_a.registry.get_open_positions()[0]
    res = asyncio.run(_exit_single_position(
        session_id=sess_a.session_id, position=pos, reason="PER_STOCK_STOP",
        brokers=sess_a.brokers, registry=sess_a.registry,
        gtt_manager=sess_a.gtt_manager, kite_product="CNC"))

    prof = sess_a.config.broker_profiles[0].profile_id
    broker = created[prof]
    assert not any(s == "X" for s, _q in broker.exits)
    assert res["status"] in ("RECONCILED_FLAT", "EXIT_FAILED")
    assert _row(sess_b, "X") == {"status": "OPEN", "qty": 100}
