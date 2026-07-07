"""RECONCILIATION FRAMEWORK — Phase 1 (order-id tracking) + Phase 4 (GTT-fill
confirmation).

Principle: the broker is the source of truth, attributed PER SESSION by
ORDER-ID; mutate only on POSITIVE evidence; fail safe.

Phase 1 — entry/exit order-ids are persisted on the position row so a broker
order can be attributed to THIS session (never the account aggregate).

Phase 4 — a TRIGGERED GTT is confirmed via the FIRED ORDER'S status
(get_gtt_fill), not the GTT object alone (the ACUTAAS-class gap). Only a
CONFIRMED COMPLETE fill closes the position; anything else stays OPEN.
"""
import asyncio

from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.gtt_manager import GTTManager
from tests.autotrade.mock_broker import MockBroker


def _cfg(**kw):
    base = dict(total_allocated_capital=500000.0, instrument_type="EQ")
    base.update(kw)
    return TradingSessionConfig(**base)


def _row(reg, symbol):
    for r in reg.get_all_positions():
        if r["symbol"] == symbol:
            return r
    return None


# ── PHASE 1: order-id persistence ────────────────────────────────────────────

def test_entry_order_id_persisted_on_register(clean_positions):
    reg = PositionRegistry("sess-oid-1", 500000.0)
    reg.register(symbol="AAA", broker_profile="zer", qty=10, avg_price=100.0,
                 entry_order_id="ORD-ENTRY-1")
    row = _row(reg, "AAA")
    assert row is not None
    assert row["entry_order_id"] == "ORD-ENTRY-1"
    assert row["exit_order_id"] is None


def test_entry_order_id_persisted_on_register_partial(clean_positions):
    reg = PositionRegistry("sess-oid-partial", 500000.0)
    reg.register_partial("BBB", "zer", 80, 100.0, entry_order_id="ORD-PART-1")
    row = _row(reg, "BBB")
    assert row["qty"] == 80
    assert row["entry_order_id"] == "ORD-PART-1"


def test_entry_order_id_none_default_is_null(clean_positions):
    """Legacy / paper call (no entry_order_id) leaves the column NULL."""
    reg = PositionRegistry("sess-oid-null", 500000.0)
    reg.register(symbol="CCC", broker_profile="zer", qty=5, avg_price=50.0)
    row = _row(reg, "CCC")
    assert row["entry_order_id"] is None


def test_entry_order_id_preserved_on_update(clean_positions):
    """A re-register with entry_order_id=None must NOT wipe an existing id."""
    reg = PositionRegistry("sess-oid-keep", 500000.0)
    reg.register(symbol="DDD", broker_profile="zer", qty=10, avg_price=100.0,
                 entry_order_id="ORD-KEEP")
    # Update (e.g. a re-register) without an id → COALESCE keeps the original.
    reg.register(symbol="DDD", broker_profile="zer", qty=12, avg_price=101.0)
    row = _row(reg, "DDD")
    assert row["qty"] == 12
    assert row["entry_order_id"] == "ORD-KEEP"


def test_exit_order_id_persisted_on_mark_closed(clean_positions):
    reg = PositionRegistry("sess-oid-exit", 500000.0)
    reg.register(symbol="EEE", broker_profile="zer", qty=10, avg_price=100.0,
                 entry_order_id="ORD-E")
    reg.mark_closed("EEE", "KILL_SWITCH", exit_price=110.0,
                    broker_profile="zer", exit_order_id="ORD-X-1")
    row = _row(reg, "EEE")
    assert row["status"] == "CLOSED"
    assert row["entry_order_id"] == "ORD-E"      # entry id untouched
    assert row["exit_order_id"] == "ORD-X-1"
    assert abs(row["realised_pnl"] - 100.0) < 1e-6   # (110-100)*10


def test_exit_order_id_persisted_on_mark_exit_failed(clean_positions):
    reg = PositionRegistry("sess-oid-fail", 500000.0)
    reg.register(symbol="FFF", broker_profile="zer", qty=10, avg_price=100.0)
    reg.mark_exit_failed("FFF", "order REJECTED", broker_profile="zer",
                         exit_order_id="ORD-X-FAIL")
    row = _row(reg, "FFF")
    assert row["status"] == "EXIT_FAILED"
    assert row["exit_order_id"] == "ORD-X-FAIL"


# ── PHASE 4: get_gtt_fill positive-evidence flow (mocked kite) ────────────────

def _prof():
    return BrokerProfile(profile_id="zer", broker_name="zerodha",
                         allocated_capital=500000.0)


def test_get_gtt_fill_triggered_complete_returns_fill():
    """A triggered GTT whose fired order is COMPLETE returns the fill evidence."""
    gtts = {"G1": {"status": "triggered",
                   "orders": [{"result": {"order_id": "O-FIRED"}}]}}
    order_status = {"O-FIRED": {"status": "COMPLETE", "filled_quantity": 10,
                               "average_price": 97.25}}
    b = MockBroker(profile=_prof(), gtts=gtts, order_status=order_status)
    fill = b.get_gtt_fill("G1")
    assert fill is not None
    assert fill["status"] == "COMPLETE"
    assert fill["filled_quantity"] == 10
    assert abs(fill["average_price"] - 97.25) < 1e-6
    assert fill["order_id"] == "O-FIRED"


def test_get_gtt_fill_triggered_order_open_returns_pending_evidence():
    """Triggered GTT whose fired order is still OPEN → status OPEN (not COMPLETE);
    the reconciler must treat this as pending and never close."""
    gtts = {"G2": {"status": "triggered",
                   "orders": [{"result": {"order_id": "O-OPEN"}}]}}
    order_status = {"O-OPEN": {"status": "OPEN", "filled_quantity": 0,
                              "average_price": 0.0}}
    b = MockBroker(profile=_prof(), gtts=gtts, order_status=order_status)
    fill = b.get_gtt_fill("G2")
    assert fill is not None
    assert fill["status"] == "OPEN"
    assert fill["filled_quantity"] == 0


def test_get_gtt_fill_active_returns_none():
    """An active (armed, not fired) GTT yields NO fill evidence."""
    gtts = {"G3": {"status": "active", "orders": []}}
    b = MockBroker(profile=_prof(), gtts=gtts, order_status={})
    assert b.get_gtt_fill("G3") is None


def test_get_gtt_fill_absent_returns_none():
    b = MockBroker(profile=_prof(), gtts={}, order_status={})
    assert b.get_gtt_fill("NOPE") is None


def test_get_gtt_fill_no_map_returns_none():
    """Paper / stub broker (no gtts map) → base no-op None."""
    b = MockBroker(profile=_prof())
    assert b.get_gtt_fill("G1") is None


# ── PHASE 4: reconcile_gtt_fills (the ACUTAAS regression) ─────────────────────

def _gtt_mgr(session_id, reg, broker):
    cfg = _cfg(per_position_gtt_enabled=True)
    return GTTManager(session_id, cfg, {"zer": broker}, reg)


def test_reconcile_triggered_filled_closes_at_real_fill(clean_positions):
    """ACUTAAS regression: a triggered+FILLED GTT → position CLOSED at the REAL
    fill price with exit_order_id set."""
    sid = "sess-recon-fill"
    reg = PositionRegistry(sid, 500000.0)
    reg.register(symbol="ACUTAAS", broker_profile="zer", qty=10, avg_price=100.0,
                 entry_order_id="ORD-ENTRY")
    reg.set_gtt("ACUTAAS", "G-FILL", gtt_stop=97.0, gtt_target=106.0,
                broker_profile="zer")
    gtts = {"G-FILL": {"status": "triggered",
                       "orders": [{"result": {"order_id": "O-GTT-FIRED"}}]}}
    order_status = {"O-GTT-FIRED": {"status": "COMPLETE", "filled_quantity": 10,
                                   "average_price": 96.80}}
    b = MockBroker(profile=_prof(), gtts=gtts, order_status=order_status)
    mgr = _gtt_mgr(sid, reg, b)

    out = asyncio.run(mgr.reconcile_gtt_fills())
    closed = [o for o in out if o["status"] == "CLOSED_GTT"]
    assert len(closed) == 1
    assert closed[0]["exit_order_id"] == "O-GTT-FIRED"

    row = _row(reg, "ACUTAAS")
    assert row["status"] == "CLOSED"
    assert row["close_reason"] == "GTT"
    assert abs(row["exit_price"] - 96.80) < 1e-6      # REAL fill, not trigger
    assert row["exit_order_id"] == "O-GTT-FIRED"
    assert row["entry_order_id"] == "ORD-ENTRY"        # entry id untouched


def test_reconcile_triggered_order_pending_stays_open(clean_positions):
    """A triggered GTT whose fired order is still OPEN → position STAYS OPEN
    (never close on anything but a confirmed COMPLETE fill)."""
    sid = "sess-recon-pending"
    reg = PositionRegistry(sid, 500000.0)
    reg.register(symbol="PENDY", broker_profile="zer", qty=10, avg_price=100.0)
    reg.set_gtt("PENDY", "G-PEND", gtt_stop=97.0, gtt_target=106.0,
                broker_profile="zer")
    gtts = {"G-PEND": {"status": "triggered",
                       "orders": [{"result": {"order_id": "O-PEND"}}]}}
    order_status = {"O-PEND": {"status": "OPEN", "filled_quantity": 0,
                              "average_price": 0.0}}
    b = MockBroker(profile=_prof(), gtts=gtts, order_status=order_status)
    mgr = _gtt_mgr(sid, reg, b)

    out = asyncio.run(mgr.reconcile_gtt_fills())
    assert any(o["status"] == "GTT_PENDING" for o in out)
    row = _row(reg, "PENDY")
    assert row["status"] == "OPEN"
    assert row["exit_order_id"] is None


def test_reconcile_cancelled_without_fill_stays_open_gtt_cleared(clean_positions):
    """A cancelled-without-fill GTT → position STAYS OPEN + gtt_id cleared."""
    sid = "sess-recon-cancel"
    reg = PositionRegistry(sid, 500000.0)
    reg.register(symbol="CANC", broker_profile="zer", qty=10, avg_price=100.0)
    reg.set_gtt("CANC", "G-CANC", gtt_stop=97.0, gtt_target=106.0,
                broker_profile="zer")
    gtts = {"G-CANC": {"status": "cancelled", "orders": []}}
    b = MockBroker(profile=_prof(), gtts=gtts, order_status={})
    mgr = _gtt_mgr(sid, reg, b)

    out = asyncio.run(mgr.reconcile_gtt_fills())
    assert any(o["status"] == "GTT_CANCELLED_UNPROTECTED" for o in out)
    row = _row(reg, "CANC")
    assert row["status"] == "OPEN"                # unprotected, NOT closed
    assert not row["gtt_id"]                      # gtt_id cleared (None/'')


def test_reconcile_no_gtt_map_noop(clean_positions):
    """Paper / stub broker (get_gtt & get_gtt_fill None) → no-op, position OPEN."""
    sid = "sess-recon-paper"
    reg = PositionRegistry(sid, 500000.0)
    reg.register(symbol="PAP", broker_profile="zer", qty=10, avg_price=100.0)
    reg.set_gtt("PAP", "G-PAP", gtt_stop=97.0, gtt_target=106.0,
                broker_profile="zer")
    b = MockBroker(profile=_prof())   # no gtts / order_status maps
    mgr = _gtt_mgr(sid, reg, b)

    out = asyncio.run(mgr.reconcile_gtt_fills())
    assert out == []
    assert _row(reg, "PAP")["status"] == "OPEN"


def test_reconcile_gtt_close_refreezes_invested_basis(clean_positions):
    """After a GTT close, the session invested_basis is re-frozen over the
    REMAINING open cash-equity rows."""
    from falcon.db import falcon_conn
    sid = "sess-recon-refreeze"
    reg = PositionRegistry(sid, 500000.0)
    # Seed a session row with a stale invested_basis covering BOTH positions.
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                invested_basis, config_json)
               VALUES (?,?,?,?,?,?,?)""",
            (sid, "2026-07-06T09:15:00", "RUNNING", "paper", 500000.0,
             2000.0, "{}"))
        con.commit()
    # Two open cash positions: KEEPME (10*100=1000) + GONE (10*100=1000) → 2000.
    reg.register(symbol="KEEPME", broker_profile="zer", qty=10, avg_price=100.0)
    reg.register(symbol="GONE", broker_profile="zer", qty=10, avg_price=100.0)
    reg.set_gtt("GONE", "G-GONE", gtt_stop=97.0, gtt_target=106.0,
                broker_profile="zer")
    gtts = {"G-GONE": {"status": "triggered",
                       "orders": [{"result": {"order_id": "O-GONE"}}]}}
    order_status = {"O-GONE": {"status": "COMPLETE", "filled_quantity": 10,
                              "average_price": 96.0}}
    b = MockBroker(profile=_prof(), gtts=gtts, order_status=order_status)
    mgr = _gtt_mgr(sid, reg, b)

    asyncio.run(mgr.reconcile_gtt_fills())
    with falcon_conn() as con:
        ib = con.execute(
            "SELECT invested_basis FROM autotrade_sessions WHERE session_id=?",
            (sid,)).fetchone()[0]
    # Only KEEPME remains open: 10*100 = 1000 (was 2000 before the close).
    assert abs(ib - 1000.0) < 1e-6


def test_extract_fired_order_id_real_kite_shape():
    """REGRESSION (verified live 2026-07-07 on the fired ACUTAAS GTT 326335110):
    Kite nests the fired order-id at orders[].result.order_result.order_id — NOT
    result.order_id. The extractor must read that real nested path; the flat
    shapes stay as fallbacks. An offline mock with the flat shape would pass while
    the live path silently returned None (the bug this locks out)."""
    from autotrade.broker.zerodha import ZerodhaBroker
    real = {"status": "triggered", "orders": [
        {"result": {"status": "success",
                    "order_result": {"status": "success",
                                     "order_id": "260707220073670"}}},
        {"result": None},   # the un-fired OCO leg
    ]}
    assert ZerodhaBroker._extract_fired_order_id(real) == "260707220073670"
    # flat fallbacks still resolve
    assert ZerodhaBroker._extract_fired_order_id(
        {"status": "triggered", "orders": [{"result": {"order_id": "FLAT"}}]}) == "FLAT"
    assert ZerodhaBroker._extract_fired_order_id(
        {"status": "triggered", "orders": [{"order_id": "LEG"}]}) == "LEG"
    # not fired / empty → None
    assert ZerodhaBroker._extract_fired_order_id(
        {"status": "active", "orders": []}) is None
