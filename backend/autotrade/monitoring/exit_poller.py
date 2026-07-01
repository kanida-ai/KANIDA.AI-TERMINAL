"""exit_poller — fill-confirmed exit for software market sells.

confirm_exit() polls broker.get_order_status() until the order is COMPLETE,
REJECTED, CANCELLED, or the deadline passes (TIMEOUT).

On COMPLETE  → registry.mark_closed(fill_price)
On PARTIAL   → registry.update_partial_exit(filled_qty, fill_price)
               returns PARTIAL so the caller can place a follow-up exit
On REJECTED/CANCELLED → registry.mark_exit_failed (releases exit_gate)
On TIMEOUT   → returns TIMEOUT; caller decides (cancel + retry)
DRY_RUN path → (order_id is None/"DRY_RUN") → mark_closed immediately

The function is async so the kill switch can await it per-position after
the parallel placement gather.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

log = logging.getLogger("kanida.autotrade.exit_poller")
IST = timezone(timedelta(hours=5, minutes=30))

# Kite status values that mean "order is still alive" (not terminal).
_PENDING_STATUSES = frozenset({
    "OPEN", "TRIGGER PENDING", "PENDING", "TRANSIT", "VALIDATION PENDING",
    "PUT ORDER REQ RECEIVED", "MODIFY VALIDATION PENDING",
    "MODIFY COMPLETE",  # transitional, becomes OPEN next
})


async def confirm_exit(
    session_id: str,
    symbol: str,
    order_id: Optional[str],
    qty: int,
    broker: Any,
    registry: Any,
    close_reason: str = "EXIT_CONFIRMED",
    max_wait_sec: int = 60,
    poll_interval_sec: float = 5.0,
) -> Dict[str, Any]:
    """Poll the broker until the exit order is fill-confirmed.

    Parameters
    ----------
    session_id      : the autotrade session (for registry + gate scoping).
    symbol          : the position symbol.
    order_id        : Kite order_id from place_market_exit. None / "DRY_RUN"
                      → paper mode; mark_closed immediately, no polling.
    qty             : expected filled quantity (from the position row).
    broker          : BrokerClient with get_order_status(order_id) -> dict.
    registry        : PositionRegistry with mark_closed / update_partial_exit /
                      mark_exit_failed.
    close_reason    : written to close_reason column on success.
    max_wait_sec    : poll timeout in seconds (default 60).
    poll_interval_sec: seconds between polls (default 5).

    Returns
    -------
    Dict with at least {"status": str} where status is one of:
      COMPLETE  — fully filled; registry.mark_closed() called.
      PARTIAL   — partial fill; registry.update_partial_exit() called;
                  remaining_qty indicates how much is still open.
      REJECTED  — order rejected; registry.mark_exit_failed() called.
      CANCELLED — order cancelled; registry.mark_exit_failed() called.
      TIMEOUT   — polling timed out; order still pending; no registry change.
      DRY_RUN   — paper mode; registry.mark_closed() called immediately.
    """
    # DRY_RUN: paper path — mark closed immediately, no polling needed.
    if order_id is None or str(order_id).upper() in ("DRY_RUN", "NONE", ""):
        log.info("confirm_exit %s/%s: DRY_RUN — marking closed immediately",
                 session_id, symbol)
        registry.mark_closed(symbol, "EXIT_DRY_RUN")
        return {"status": "DRY_RUN", "filled_qty": qty, "exit_price": None}

    deadline = datetime.now(IST) + timedelta(seconds=max_wait_sec)
    filled_qty = 0
    avg_price = 0.0
    last_kite_status = "UNKNOWN"

    log.info("confirm_exit %s/%s: polling order %s (max %ds)",
             session_id, symbol, order_id, max_wait_sec)

    while datetime.now(IST) < deadline:
        try:
            status_dict = await asyncio.to_thread(broker.get_order_status, order_id)
        except Exception as e:
            log.warning("confirm_exit %s/%s: get_order_status error: %s — retrying",
                        session_id, symbol, e)
            await asyncio.sleep(poll_interval_sec)
            continue

        kite_status = str(status_dict.get("status", "")).upper()
        filled_qty = int(status_dict.get("filled_quantity") or 0)
        avg_price = float(status_dict.get("average_price") or 0.0)
        last_kite_status = kite_status

        if kite_status == "COMPLETE":
            if filled_qty >= qty:
                # Full fill confirmed.
                fill_price = avg_price if avg_price > 0 else None
                registry.mark_closed(symbol, close_reason, exit_price=fill_price)
                log.info(
                    "confirm_exit %s/%s: COMPLETE fill_qty=%d exit_price=%s",
                    session_id, symbol, filled_qty, fill_price)
                return {"status": "COMPLETE", "filled_qty": filled_qty,
                        "exit_price": fill_price}
            else:
                # Partial fill: update registry but leave position OPEN for retry.
                fill_price = avg_price if avg_price > 0 else None
                registry.update_partial_exit(symbol, filled_qty, fill_price)
                remaining = qty - filled_qty
                log.warning(
                    "confirm_exit %s/%s: PARTIAL fill_qty=%d remaining=%d",
                    session_id, symbol, filled_qty, remaining)
                return {"status": "PARTIAL", "filled_qty": filled_qty,
                        "remaining_qty": remaining, "exit_price": fill_price}

        elif kite_status in ("REJECTED", "CANCELLED"):
            registry.mark_exit_failed(
                symbol, f"order {kite_status}",
                broker_profile=None)
            log.error("confirm_exit %s/%s: order %s — gate released for retry",
                      session_id, symbol, kite_status)
            return {"status": kite_status, "filled_qty": filled_qty,
                    "exit_price": None}

        elif kite_status == "DRY_RUN":
            # Synthetic status from mock/paper broker.
            registry.mark_closed(symbol, "EXIT_DRY_RUN")
            return {"status": "COMPLETE", "filled_qty": qty, "exit_price": None}

        # Still pending (OPEN / TRIGGER PENDING / etc.) — wait and retry.
        log.debug("confirm_exit %s/%s: order status=%s filled=%d — waiting %.0fs",
                  session_id, symbol, kite_status, filled_qty, poll_interval_sec)
        await asyncio.sleep(poll_interval_sec)

    # Deadline passed without terminal status.
    log.error(
        "confirm_exit %s/%s: TIMEOUT after %ds (last_status=%s filled=%d)",
        session_id, symbol, max_wait_sec, last_kite_status, filled_qty)
    return {"status": "TIMEOUT", "filled_qty": filled_qty, "exit_price": None}


async def cancel_and_retry_exit(
    session_id: str,
    symbol: str,
    order_id: str,
    qty: int,
    broker: Any,
    registry: Any,
    close_reason: str = "EXIT_RETRY",
    max_retries: int = 3,
    max_wait_sec: int = 60,
    poll_interval_sec: float = 5.0,
    kite_product: str | None = None,
) -> Dict[str, Any]:
    """Cancel a pending exit order and place a fresh market sell, with retries.

    Sequence per attempt:
      1. Cancel the stale order via broker.cancel_order_sync(order_id).
      2. Place a fresh market sell via broker.place_market_exit().
      3. confirm_exit() on the new order.

    Returns the confirm_exit result of the last attempt, or {"status": "FAILED"}
    after max_retries without a COMPLETE fill.
    """
    last_result: Dict[str, Any] = {"status": "FAILED"}
    current_order_id = order_id

    for attempt in range(1, max_retries + 1):
        log.warning(
            "cancel_and_retry_exit %s/%s: attempt %d/%d (cancelling order %s)",
            session_id, symbol, attempt, max_retries, current_order_id)

        # Step 1: cancel the stale/pending order (best-effort).
        try:
            broker.cancel_order_sync(current_order_id)
        except Exception as e:
            log.warning(
                "cancel_and_retry_exit %s/%s: cancel_order_sync(%s) failed: %s — continuing",
                session_id, symbol, current_order_id, e)

        # Brief pause to allow the cancel to propagate on the exchange.
        await asyncio.sleep(1.0)

        # Step 2: fresh market sell.
        try:
            res = await broker.place_market_exit(symbol, qty, "EQ",
                                                  kite_product=kite_product)
        except Exception as e:
            log.error(
                "cancel_and_retry_exit %s/%s attempt %d: place_market_exit raised: %s",
                session_id, symbol, attempt, e)
            last_result = {"status": "FAILED", "error": str(e)}
            continue

        new_order_id = getattr(res, "broker_order_id", None)
        if getattr(res, "status", None) == "FAILED" or not new_order_id:
            log.error(
                "cancel_and_retry_exit %s/%s attempt %d: place_market_exit FAILED: %s",
                session_id, symbol, attempt, getattr(res, "error", ""))
            last_result = {"status": "FAILED",
                           "error": getattr(res, "error", "place_market_exit failed")}
            continue

        # DRY_RUN path from paper broker — mark closed immediately.
        if str(new_order_id).upper() in ("DRY_RUN", "NONE", ""):
            registry.mark_closed(symbol, close_reason)
            return {"status": "COMPLETE", "filled_qty": qty, "exit_price": None}

        # Step 3: confirm the new order.
        current_order_id = new_order_id
        result = await confirm_exit(
            session_id=session_id,
            symbol=symbol,
            order_id=new_order_id,
            qty=qty,
            broker=broker,
            registry=registry,
            close_reason=close_reason,
            max_wait_sec=max_wait_sec,
            poll_interval_sec=poll_interval_sec,
        )
        last_result = result
        if result.get("status") == "COMPLETE":
            return result

    # All retries exhausted.
    log.error(
        "cancel_and_retry_exit %s/%s: all %d retries failed — marking EXIT_FAILED",
        session_id, symbol, max_retries)
    registry.mark_exit_failed(symbol, "cancel_and_retry exhausted")
    return last_result
