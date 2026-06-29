"""Tests for GTTManager._gtt_execution_result and updated reconcile_gtt_fills.

Fix 2: reconcile_gtt_fills() must confirm an actual COMPLETE fill before
marking a position CLOSED. 'triggered' only means Kite placed the sell order;
the order might still be PENDING (e.g. price gapped past the limit).

Covers:
  test_gtt_active_no_action
      — status=active → None (no action)

  test_gtt_triggered_order_complete
      — status=triggered, orders=[{status:COMPLETE, average_price:318.80}]
        → {"status":"complete", "exit_price":318.80}

  test_gtt_triggered_order_pending
      — status=triggered, orders=[{status:OPEN}] → {"status":"pending"} (no close)

  test_gtt_triggered_no_orders
      — status=triggered, orders=[] → {"status":"pending"} (conservative)

  test_gtt_triggered_missing_orders_field
      — status=triggered, no 'orders' key → {"status":"pending"} (conservative)

  test_gtt_cancelled_returns_cancelled
      — status=cancelled → {"status":"cancelled"}

  test_gtt_deleted_returns_cancelled
      — status=deleted → {"status":"cancelled"}

  test_gtt_expired_returns_cancelled
      — status=expired → {"status":"cancelled"}

  test_gtt_unknown_status_is_pending
      — status='frobulated' → {"status":"pending"} (conservative)

  test_gtt_none_state_returns_none
      — state=None → None

  test_gtt_exit_price_uses_fill_not_trigger
      — fill @ 318.80 (above trigger 315.00) → exit_price=318.80

  test_reconcile_active_no_close (integration)
      — active GTT → position stays OPEN

  test_reconcile_triggered_complete_closes (integration)
      — complete fill → position marked CLOSED with fill price

  test_reconcile_triggered_pending_no_close (integration)
      — triggered but OPEN order → position stays OPEN

  test_reconcile_cancelled_logs_warning_no_close (integration)
      — cancelled GTT → WARNING logged, position stays OPEN

  test_reconcile_close_uses_fill_price_not_trigger (integration)
      — fill price stored on the position row (not the trigger price)
"""
from __future__ import annotations

import uuid

import pytest

from autotrade.monitoring.gtt_manager import GTTManager
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.registry import PositionRegistry
from tests.autotrade.mock_broker import MockBroker


# ── helpers ────────────────────────────────────────────────────────────────────

def _sid():
    return uuid.uuid4().hex


def _make_session(sid, cap=500_000.0, mode="live"):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            "INSERT INTO autotrade_sessions "
            "(session_id, created_at, status, mode, total_allocated_capital, config_json) "
            "VALUES (?,?,?,?,?,?)",
            (sid, "2026-06-28T09:00:00", "RUNNING", mode, cap, "{}"))
        con.commit()


def _make_gtt_state(status, orders=None):
    """Build a Kite-like GTT state dict."""
    d = {"status": status}
    if orders is not None:
        d["orders"] = orders
    return d


def _make_complete_order(avg_price=318.80, qty=189, tx="SELL"):
    return {
        "transaction_type": tx,
        "status": "COMPLETE",
        "quantity": qty,
        "average_price": avg_price,
    }


def _make_pending_order(tx="SELL"):
    return {"transaction_type": tx, "status": "OPEN", "quantity": 100}


def _make_gtt_manager(sid, cap, broker, mode="live"):
    reg = PositionRegistry(sid, cap)
    cfg = TradingSessionConfig(total_allocated_capital=cap,
                               per_position_stop_pct=0.03,
                               per_position_target_pct=0.06)
    return GTTManager(sid, cfg, {"zer": broker}, reg), reg


# ── Unit tests for _gtt_execution_result ─────────────────────────────────────

class TestGttExecutionResult:

    def test_gtt_none_state_returns_none(self):
        assert GTTManager._gtt_execution_result(None) is None

    def test_gtt_active_no_action(self):
        state = _make_gtt_state("active")
        assert GTTManager._gtt_execution_result(state) is None

    def test_gtt_triggered_order_complete(self):
        orders = [_make_complete_order(avg_price=318.80, qty=189)]
        state = _make_gtt_state("triggered", orders=orders)
        result = GTTManager._gtt_execution_result(state)
        assert result is not None
        assert result["status"] == "complete"
        assert abs(result["exit_price"] - 318.80) < 1e-6
        assert result["filled_qty"] == 189

    def test_gtt_triggered_order_pending(self):
        orders = [_make_pending_order()]
        state = _make_gtt_state("triggered", orders=orders)
        result = GTTManager._gtt_execution_result(state)
        assert result is not None
        assert result["status"] == "pending"

    def test_gtt_triggered_no_orders(self):
        """orders=[] is conservative — treat as pending."""
        state = _make_gtt_state("triggered", orders=[])
        result = GTTManager._gtt_execution_result(state)
        assert result is not None
        assert result["status"] == "pending"

    def test_gtt_triggered_missing_orders_field(self):
        """No 'orders' key → conservative → pending."""
        state = {"status": "triggered"}
        result = GTTManager._gtt_execution_result(state)
        assert result is not None
        assert result["status"] == "pending"

    def test_gtt_cancelled_returns_cancelled(self):
        state = _make_gtt_state("cancelled")
        result = GTTManager._gtt_execution_result(state)
        assert result is not None
        assert result["status"] == "cancelled"

    def test_gtt_deleted_returns_cancelled(self):
        state = _make_gtt_state("deleted")
        result = GTTManager._gtt_execution_result(state)
        assert result["status"] == "cancelled"

    def test_gtt_expired_returns_cancelled(self):
        state = _make_gtt_state("expired")
        result = GTTManager._gtt_execution_result(state)
        assert result["status"] == "cancelled"

    def test_gtt_unknown_status_is_pending(self):
        """Unknown Kite status → conservative → pending (never close on ambiguity)."""
        state = _make_gtt_state("frobulated")
        result = GTTManager._gtt_execution_result(state)
        assert result is not None
        assert result["status"] == "pending"

    def test_gtt_exit_price_uses_fill_not_trigger(self):
        """Fill price (average_price) may differ from trigger; exit_price=fill."""
        # trigger_price might be 318.69, but the actual fill is at 318.80.
        orders = [_make_complete_order(avg_price=318.80, qty=50)]
        state = _make_gtt_state("triggered", orders=orders)
        result = GTTManager._gtt_execution_result(state)
        assert result["status"] == "complete"
        assert abs(result["exit_price"] - 318.80) < 1e-6, (
            "exit_price must be the actual fill price, not the trigger price")

    def test_gtt_triggered_only_buy_leg_no_close(self):
        """Only a BUY leg present (unexpected) → conservative → pending."""
        orders = [{"transaction_type": "BUY", "status": "COMPLETE",
                   "quantity": 10, "average_price": 100.0}]
        state = _make_gtt_state("triggered", orders=orders)
        result = GTTManager._gtt_execution_result(state)
        assert result["status"] == "pending"

    def test_gtt_triggered_sell_not_complete(self):
        """SELL order exists but status is REJECTED → still pending."""
        orders = [{"transaction_type": "SELL", "status": "REJECTED",
                   "quantity": 10, "average_price": 0.0}]
        state = _make_gtt_state("triggered", orders=orders)
        result = GTTManager._gtt_execution_result(state)
        assert result["status"] == "pending"

    def test_gtt_state_with_no_status_field(self):
        """State dict with no 'status' key → None (can't determine)."""
        state = {"id": 12345, "orders": []}
        result = GTTManager._gtt_execution_result(state)
        assert result is None

    def test_gtt_triggered_uppercase_status_in_orders(self):
        """Order status field is case-insensitive — COMPLETE in any case."""
        orders = [{"transaction_type": "SELL", "status": "complete",
                   "quantity": 5, "average_price": 200.0}]
        state = _make_gtt_state("triggered", orders=orders)
        result = GTTManager._gtt_execution_result(state)
        assert result["status"] == "complete"
        assert abs(result["exit_price"] - 200.0) < 1e-6


# ── Integration tests for reconcile_gtt_fills ────────────────────────────────

class TestReconcileGttFills:

    def test_reconcile_active_no_close(self, clean_positions):
        """Active GTT → position stays OPEN (no action taken)."""
        sid = _sid()
        _make_session(sid)
        broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                            ltps={"ACTIVE_STOCK": 100.0})
        mgr, reg = _make_gtt_manager(sid, 500_000.0, broker)
        reg.register(symbol="ACTIVE_STOCK", broker_profile="zer", qty=10,
                     avg_price=100.0)
        # Manually set a GTT id and seed as 'active'.
        reg.set_gtt("ACTIVE_STOCK", "gtt-1", gtt_stop=97.0, gtt_target=106.0)
        broker.gtt_states["gtt-1"] = {"status": "active"}

        out = mgr.reconcile_gtt_fills()
        # Nothing should be in the output for an active GTT.
        assert all(r["status"] != "CLOSED_GTT" for r in out)
        positions = reg.get_open_positions()
        assert len(positions) == 1, "Position should still be OPEN"

    def test_reconcile_triggered_complete_closes(self, clean_positions):
        """triggered + COMPLETE sell → position marked CLOSED."""
        sid = _sid()
        _make_session(sid)
        broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                            ltps={"COMP_STOCK": 318.80})
        mgr, reg = _make_gtt_manager(sid, 500_000.0, broker)
        reg.register(symbol="COMP_STOCK", broker_profile="zer", qty=189,
                     avg_price=330.0)
        reg.set_gtt("COMP_STOCK", "gtt-2", gtt_stop=320.0, gtt_target=350.0)
        # Simulate a complete fill at 318.80.
        broker.gtt_states["gtt-2"] = {
            "status": "triggered",
            "orders": [_make_complete_order(avg_price=318.80, qty=189)],
        }

        out = mgr.reconcile_gtt_fills()
        closed = [r for r in out if r["status"] == "CLOSED_GTT"]
        assert len(closed) == 1, "One position should be CLOSED"
        assert abs(closed[0]["exit_price"] - 318.80) < 1e-6
        # DB row must be CLOSED.
        all_pos = reg.get_all_positions()
        row = next(p for p in all_pos if p["symbol"] == "COMP_STOCK")
        assert row["status"] == "CLOSED"
        assert abs((row["exit_price"] or 0.0) - 318.80) < 1e-6

    def test_reconcile_triggered_pending_no_close(self, clean_positions):
        """triggered + OPEN order → position stays OPEN (fill not confirmed)."""
        sid = _sid()
        _make_session(sid)
        broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                            ltps={"PEND_STOCK": 315.0})
        mgr, reg = _make_gtt_manager(sid, 500_000.0, broker)
        reg.register(symbol="PEND_STOCK", broker_profile="zer", qty=100,
                     avg_price=330.0)
        reg.set_gtt("PEND_STOCK", "gtt-3", gtt_stop=320.0, gtt_target=350.0)
        broker.gtt_states["gtt-3"] = {
            "status": "triggered",
            "orders": [_make_pending_order()],
        }

        out = mgr.reconcile_gtt_fills()
        pending = [r for r in out if r["status"] == "GTT_PENDING"]
        assert len(pending) == 1, "Should report GTT_PENDING, not CLOSED"
        # Position must still be OPEN.
        open_pos = reg.get_open_positions()
        assert len(open_pos) == 1

    def test_reconcile_triggered_no_orders_no_close(self, clean_positions):
        """triggered + empty orders list → conservative, position stays OPEN."""
        sid = _sid()
        _make_session(sid)
        broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                            ltps={"EMPTY_STOCK": 315.0})
        mgr, reg = _make_gtt_manager(sid, 500_000.0, broker)
        reg.register(symbol="EMPTY_STOCK", broker_profile="zer", qty=50,
                     avg_price=320.0)
        reg.set_gtt("EMPTY_STOCK", "gtt-4", gtt_stop=310.0, gtt_target=340.0)
        broker.gtt_states["gtt-4"] = {"status": "triggered", "orders": []}

        out = mgr.reconcile_gtt_fills()
        assert all(r["status"] != "CLOSED_GTT" for r in out)
        assert len(reg.get_open_positions()) == 1

    def test_reconcile_cancelled_logs_warning_no_close(self, clean_positions,
                                                        caplog):
        """Cancelled GTT → WARNING logged, position stays OPEN (unprotected)."""
        import logging
        sid = _sid()
        _make_session(sid)
        broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                            ltps={"CANCEL_STOCK": 300.0})
        mgr, reg = _make_gtt_manager(sid, 500_000.0, broker)
        reg.register(symbol="CANCEL_STOCK", broker_profile="zer", qty=30,
                     avg_price=310.0)
        reg.set_gtt("CANCEL_STOCK", "gtt-5", gtt_stop=300.0, gtt_target=330.0)
        broker.gtt_states["gtt-5"] = {"status": "cancelled"}

        with caplog.at_level(logging.WARNING, logger="kanida.autotrade.gtt_manager"):
            out = mgr.reconcile_gtt_fills()

        cancelled = [r for r in out if r["status"] == "GTT_CANCELLED_UNPROTECTED"]
        assert len(cancelled) == 1, "Should report GTT_CANCELLED_UNPROTECTED"
        # Position must still be OPEN.
        assert len(reg.get_open_positions()) == 1
        # Must have logged a WARNING.
        assert any("CANCELLED" in r.message and "unprotected" in r.message.lower()
                   for r in caplog.records), (
            "Should log a WARNING that the GTT was cancelled and position is unprotected")

    def test_reconcile_close_uses_fill_price_not_trigger(self, clean_positions):
        """exit_price stored on position row is the FILL price, not the trigger."""
        sid = _sid()
        _make_session(sid)
        broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                            ltps={"FILL_STOCK": 318.80})
        mgr, reg = _make_gtt_manager(sid, 500_000.0, broker)
        reg.register(symbol="FILL_STOCK", broker_profile="zer", qty=100,
                     avg_price=330.0)
        # trigger_price = 318.69 but fill happened at 318.80.
        reg.set_gtt("FILL_STOCK", "gtt-6", gtt_stop=318.69, gtt_target=350.0)
        broker.gtt_states["gtt-6"] = {
            "status": "triggered",
            "orders": [_make_complete_order(avg_price=318.80, qty=100)],
        }

        mgr.reconcile_gtt_fills()

        all_pos = reg.get_all_positions()
        row = next(p for p in all_pos if p["symbol"] == "FILL_STOCK")
        assert row["status"] == "CLOSED"
        assert abs((row["exit_price"] or 0.0) - 318.80) < 1e-6, (
            f"exit_price must be the fill price 318.80, got {row['exit_price']}")
        assert abs((row["exit_price"] or 0.0) - 318.69) > 1e-6, (
            "exit_price must NOT be the trigger price 318.69")
