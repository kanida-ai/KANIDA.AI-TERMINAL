"""Tests for monitoring/exit_poller.confirm_exit.

Covers:
  test_confirm_exit_complete_immediately
      — order immediately COMPLETE → mark_closed called with fill price

  test_confirm_exit_partial_fill
      — order COMPLETE with partial qty → update_partial_exit called, returns PARTIAL

  test_confirm_exit_timeout_then_retry
      — order OPEN for max_wait then TIMEOUT returned (no mark_closed)

  test_confirm_exit_rejected
      — order REJECTED → mark_exit_failed called, gate released

  test_confirm_exit_dry_run
      — order_id None / "DRY_RUN" → mark_closed immediately (no polling)

  test_confirm_exit_polls_until_complete
      — order OPEN for 2 polls then COMPLETE → polls 3 times total
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any, Optional
from unittest.mock import MagicMock, call

import pytest

from autotrade.monitoring.exit_poller import confirm_exit, cancel_and_retry_exit
from autotrade.broker.base import BrokerClient, OrderResult


# ── Minimal stub broker (get_order_status only) ──────────────────────────────

class _SequenceBroker:
    """Test-only broker stub: returns pre-loaded status dicts in sequence."""

    def __init__(self, statuses: list, avg_price: float = 123.45,
                 place_result_oid: str = "ord-1"):
        self._statuses = list(statuses)
        self._call_count = 0
        self.avg_price = avg_price
        self._place_result_oid = place_result_oid
        self.cancel_sync_calls: list = []
        self.exit_calls: list = []

    def get_order_status(self, order_id: str) -> dict:
        idx = min(self._call_count, len(self._statuses) - 1)
        self._call_count += 1
        return self._statuses[idx]

    def cancel_order_sync(self, order_id: str) -> bool:
        self.cancel_sync_calls.append(order_id)
        return True

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str) -> OrderResult:
        self.exit_calls.append((symbol, qty))
        return OrderResult(status="PLACED",
                           broker_order_id=self._place_result_oid,
                           symbol=symbol, qty=qty)


# ── Minimal registry mock ─────────────────────────────────────────────────────

def _mock_registry(session_id: str, symbol: str,
                   orig_qty: int = 10, avg_price: float = 100.0):
    """Return a MagicMock that also carries position metadata for partial checks."""
    reg = MagicMock()
    reg.session_id = session_id
    return reg


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_confirm_exit_complete_immediately():
    """Order immediately COMPLETE → mark_closed called with fill price."""
    sid = uuid.uuid4().hex
    symbol = "INFY"
    order_id = "ord-123"
    qty = 10
    fill_price = 1500.50

    broker = _SequenceBroker([
        {"status": "COMPLETE", "filled_quantity": 10, "average_price": fill_price}
    ])
    registry = _mock_registry(sid, symbol)

    result = asyncio.run(confirm_exit(
        session_id=sid, symbol=symbol, order_id=order_id,
        qty=qty, broker=broker, registry=registry,
        close_reason="KILL_SWITCH",
        max_wait_sec=10, poll_interval_sec=0.05,
    ))

    assert result["status"] == "COMPLETE"
    assert result["filled_qty"] == qty
    assert abs(result["exit_price"] - fill_price) < 1e-6
    registry.mark_closed.assert_called_once_with(
        symbol, "KILL_SWITCH", exit_price=fill_price)
    registry.mark_exit_failed.assert_not_called()
    assert broker._call_count == 1  # only one poll needed


def test_confirm_exit_partial_fill():
    """Order COMPLETE but filled_qty < qty → update_partial_exit called, PARTIAL returned."""
    sid = uuid.uuid4().hex
    symbol = "WIPRO"
    order_id = "ord-456"
    qty = 20
    filled = 12
    fill_price = 250.0

    broker = _SequenceBroker([
        {"status": "COMPLETE", "filled_quantity": filled, "average_price": fill_price}
    ])
    registry = _mock_registry(sid, symbol)

    result = asyncio.run(confirm_exit(
        session_id=sid, symbol=symbol, order_id=order_id,
        qty=qty, broker=broker, registry=registry,
        max_wait_sec=10, poll_interval_sec=0.05,
    ))

    assert result["status"] == "PARTIAL"
    assert result["filled_qty"] == filled
    assert result["remaining_qty"] == qty - filled
    assert abs(result["exit_price"] - fill_price) < 1e-6
    registry.update_partial_exit.assert_called_once_with(
        symbol, filled, fill_price)
    registry.mark_closed.assert_not_called()


def test_confirm_exit_timeout_then_retry():
    """Order stays OPEN past deadline → returns TIMEOUT (no registry change)."""
    sid = uuid.uuid4().hex
    symbol = "HDFC"
    order_id = "ord-timeout"
    qty = 5

    # Only returns OPEN — will always be pending.
    broker = _SequenceBroker([
        {"status": "OPEN", "filled_quantity": 0, "average_price": 0.0}
    ])
    registry = _mock_registry(sid, symbol)

    result = asyncio.run(confirm_exit(
        session_id=sid, symbol=symbol, order_id=order_id,
        qty=qty, broker=broker, registry=registry,
        max_wait_sec=0,        # expires immediately
        poll_interval_sec=0.01,
    ))

    assert result["status"] == "TIMEOUT"
    registry.mark_closed.assert_not_called()
    registry.mark_exit_failed.assert_not_called()


def test_confirm_exit_rejected():
    """Order REJECTED → mark_exit_failed called, gate released via registry."""
    sid = uuid.uuid4().hex
    symbol = "TCS"
    order_id = "ord-rej"
    qty = 8

    broker = _SequenceBroker([
        {"status": "REJECTED", "filled_quantity": 0, "average_price": 0.0}
    ])
    registry = _mock_registry(sid, symbol)

    result = asyncio.run(confirm_exit(
        session_id=sid, symbol=symbol, order_id=order_id,
        qty=qty, broker=broker, registry=registry,
        max_wait_sec=10, poll_interval_sec=0.05,
    ))

    assert result["status"] == "REJECTED"
    registry.mark_exit_failed.assert_called_once_with(
        symbol, "order REJECTED", broker_profile=None)
    registry.mark_closed.assert_not_called()


def test_confirm_exit_cancelled():
    """Order CANCELLED → mark_exit_failed called (gate released)."""
    sid = uuid.uuid4().hex
    symbol = "SBIN"
    order_id = "ord-canc"
    qty = 15

    broker = _SequenceBroker([
        {"status": "CANCELLED", "filled_quantity": 0, "average_price": 0.0}
    ])
    registry = _mock_registry(sid, symbol)

    result = asyncio.run(confirm_exit(
        session_id=sid, symbol=symbol, order_id=order_id,
        qty=qty, broker=broker, registry=registry,
        max_wait_sec=10, poll_interval_sec=0.05,
    ))

    assert result["status"] == "CANCELLED"
    registry.mark_exit_failed.assert_called_once()
    registry.mark_closed.assert_not_called()


def test_confirm_exit_dry_run_none():
    """order_id=None → DRY_RUN path, mark_closed immediately, no polling."""
    sid = uuid.uuid4().hex
    symbol = "RELIANCE"
    qty = 3

    broker = _SequenceBroker([])  # should never be called
    registry = _mock_registry(sid, symbol)

    result = asyncio.run(confirm_exit(
        session_id=sid, symbol=symbol, order_id=None,
        qty=qty, broker=broker, registry=registry,
        max_wait_sec=10, poll_interval_sec=0.05,
    ))

    assert result["status"] == "DRY_RUN"
    assert result["filled_qty"] == qty
    registry.mark_closed.assert_called_once_with(symbol, "EXIT_DRY_RUN")
    assert broker._call_count == 0  # no polling


def test_confirm_exit_dry_run_string():
    """order_id='DRY_RUN' → paper path, mark_closed immediately."""
    sid = uuid.uuid4().hex
    symbol = "MARUTI"
    qty = 7

    broker = _SequenceBroker([])
    registry = _mock_registry(sid, symbol)

    result = asyncio.run(confirm_exit(
        session_id=sid, symbol=symbol, order_id="DRY_RUN",
        qty=qty, broker=broker, registry=registry,
        max_wait_sec=10, poll_interval_sec=0.05,
    ))

    assert result["status"] == "DRY_RUN"
    registry.mark_closed.assert_called_once()
    assert broker._call_count == 0


def test_confirm_exit_polls_until_complete():
    """Order OPEN for 2 polls then COMPLETE → polls 3 times total."""
    sid = uuid.uuid4().hex
    symbol = "HCLTECH"
    order_id = "ord-poll"
    qty = 4
    fill_price = 1800.0

    broker = _SequenceBroker([
        {"status": "OPEN", "filled_quantity": 0, "average_price": 0.0},
        {"status": "OPEN", "filled_quantity": 0, "average_price": 0.0},
        {"status": "COMPLETE", "filled_quantity": qty, "average_price": fill_price},
    ])
    registry = _mock_registry(sid, symbol)

    result = asyncio.run(confirm_exit(
        session_id=sid, symbol=symbol, order_id=order_id,
        qty=qty, broker=broker, registry=registry,
        max_wait_sec=30, poll_interval_sec=0.01,
    ))

    assert result["status"] == "COMPLETE"
    assert result["filled_qty"] == qty
    assert broker._call_count == 3  # OPEN, OPEN, COMPLETE
    registry.mark_closed.assert_called_once_with(
        symbol, "EXIT_CONFIRMED", exit_price=fill_price)


def test_confirm_exit_zero_avg_price_uses_none():
    """avg_price=0 in COMPLETE response → exit_price=None (not 0.0) in mark_closed."""
    sid = uuid.uuid4().hex
    symbol = "AXISBANK"
    order_id = "ord-zero"
    qty = 6

    broker = _SequenceBroker([
        {"status": "COMPLETE", "filled_quantity": qty, "average_price": 0.0}
    ])
    registry = _mock_registry(sid, symbol)

    result = asyncio.run(confirm_exit(
        session_id=sid, symbol=symbol, order_id=order_id,
        qty=qty, broker=broker, registry=registry,
        max_wait_sec=10, poll_interval_sec=0.05,
    ))

    assert result["status"] == "COMPLETE"
    # avg_price=0 → exit_price should be None (falsy → None in our code).
    assert result["exit_price"] is None
    registry.mark_closed.assert_called_once_with(
        symbol, "EXIT_CONFIRMED", exit_price=None)


def test_cancel_and_retry_exit_succeeds_on_first_retry():
    """cancel_and_retry: cancel stale → place fresh → COMPLETE on first try."""
    sid = uuid.uuid4().hex
    symbol = "ULTRACEMCO"
    stale_order_id = "ord-stale"
    new_order_id = "ord-new"
    qty = 3

    # The broker sequence for the NEW order (ord-new) → COMPLETE immediately.
    new_order_statuses = [
        {"status": "COMPLETE", "filled_quantity": qty, "average_price": 2500.0}
    ]
    broker = _SequenceBroker(
        statuses=new_order_statuses,
        place_result_oid=new_order_id,
    )
    # Make get_order_status key off the new order id.
    # The _SequenceBroker always returns from the same sequence regardless of order_id.
    registry = _mock_registry(sid, symbol)

    result = asyncio.run(cancel_and_retry_exit(
        session_id=sid, symbol=symbol, order_id=stale_order_id,
        qty=qty, broker=broker, registry=registry,
        close_reason="KILL_SWITCH", max_retries=3,
        max_wait_sec=10, poll_interval_sec=0.01,
    ))

    assert result["status"] == "COMPLETE"
    assert broker.cancel_sync_calls == [stale_order_id]
    assert len(broker.exit_calls) == 1
    registry.mark_closed.assert_called_once()
