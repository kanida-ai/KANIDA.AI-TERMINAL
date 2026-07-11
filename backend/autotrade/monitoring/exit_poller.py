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

from .. import order_ledger as _order_ledger

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
    broker_profile: Optional[str] = None,
) -> Dict[str, Any]:
    """Poll the broker until the exit order is fill-confirmed.

    broker_profile (Fix 4, 2026-07-11): when supplied, the registry mutations
    (mark_closed / update_partial_exit / mark_exit_failed) are scoped to this
    (session, symbol, broker_profile) leg so a same-symbol-two-profile session
    never mutates BOTH rows. None (default) keeps the old symbol-wide behaviour.

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

    # CAP 3/5 — a durable EXIT_PLACED event, keyed by the broker exit order-id, is
    # written the moment we start confirming a LIVE exit. This is the cross-day
    # attribution key: if the exit fills but the close never gets recorded (crash /
    # cross-day settlement), the reconciler finds THIS persisted exit event and
    # closes the position cleanly instead of looping UNATTRIBUTED_CLOSE. This is
    # the ONE universal exit choke point (kill switch, per-stock stop, retry all
    # route here). Best-effort — never blocks the confirm poll.
    _order_ledger.append_event(
        session_id=session_id, symbol=symbol,
        event_type=_order_ledger.EV_EXIT_PLACED,
        broker_profile=broker_profile, broker_order_id=order_id, qty=qty,
        source="exit", detail=close_reason)

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
                # Full fill confirmed. RECONCILIATION Phase 1: record the EXIT
                # order-id on the closed row (attributable by order-id).
                fill_price = avg_price if avg_price > 0 else None
                # CAP 2 — durable EXIT_FILLED event carrying the fill price + broker
                # exit order-id (mark_closed below also records POSITION_CLOSED).
                _order_ledger.append_event(
                    session_id=session_id, symbol=symbol,
                    event_type=_order_ledger.EV_EXIT_FILLED,
                    broker_profile=broker_profile, broker_order_id=order_id,
                    qty=filled_qty, price=fill_price, source="exit",
                    detail=close_reason)
                registry.mark_closed(symbol, close_reason, exit_price=fill_price,
                                     exit_order_id=order_id,
                                     broker_profile=broker_profile)
                log.info(
                    "confirm_exit %s/%s: COMPLETE fill_qty=%d exit_price=%s",
                    session_id, symbol, filled_qty, fill_price)
                return {"status": "COMPLETE", "filled_qty": filled_qty,
                        "exit_price": fill_price}
            else:
                # Partial fill: update registry but leave position OPEN for retry.
                fill_price = avg_price if avg_price > 0 else None
                registry.update_partial_exit(symbol, filled_qty, fill_price,
                                             broker_profile=broker_profile)
                remaining = qty - filled_qty
                log.warning(
                    "confirm_exit %s/%s: PARTIAL fill_qty=%d remaining=%d",
                    session_id, symbol, filled_qty, remaining)
                return {"status": "PARTIAL", "filled_qty": filled_qty,
                        "remaining_qty": remaining, "exit_price": fill_price}

        elif kite_status in ("REJECTED", "CANCELLED"):
            registry.mark_exit_failed(
                symbol, f"order {kite_status}",
                broker_profile=broker_profile, exit_order_id=order_id)
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
    direction: str = "long",
    instrument_type: str = "EQ",
    broker_profile: str | None = None,
) -> Dict[str, Any]:
    """Cancel a pending exit order and place a fresh market sell, with retries.

    Sequence per attempt:
      1. Cancel the stale order via broker.cancel_order_sync(order_id).
      2. PRE-PLACE RECONCILIATION GUARD (Fix 1) — re-probe the broker's live net
         BEFORE every placement.
      3. Place a fresh market sell via broker.place_market_exit().
      4. confirm_exit() on the new order.

    Returns the confirm_exit result of the last attempt, or {"status": "FAILED"}
    after max_retries without a COMPLETE fill.

    broker_profile (Fix 4) scopes the registry mutations to the firing leg.
    """
    from .registry import our_held_at_broker as _our_held  # avoid module cycle

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

        # Step 2: PRE-PLACE RECONCILIATION GUARD (Fix 1, 2026-07-11 audit — BRIGADE
        # via timeout+late-fill). The FIRST exit may have actually FILLED but its
        # status read TIMED OUT (confirm_exit → TIMEOUT), which is what routes us
        # here. Placing a blind fresh exit now would DOUBLE-cover → naked reverse.
        # Run the SAME guard as _exit_single_position_inner BEFORE every placement:
        #   * probe RAISES (broker unreachable) → ABORT the retry (no order), flag
        #     the leg EXIT_FAILED for the guarded retry sweep once reachable.
        #   * our_held == 0 (broker already flat FOR THIS SESSION) → reconcile-flat,
        #     place NOTHING, return a flat COMPLETE result.
        #   * 0 < our_held < qty → CLAMP the retry qty to our_held (never oversell).
        # our_held is None in paper / when the broker can't answer → NO guard, the
        # retry proceeds exactly as before (byte-for-byte unchanged).
        try:
            net_qty = broker.get_net_position_qty(symbol, instrument_type)
        except Exception as _net_e:
            log.error(
                "cancel_and_retry_exit %s/%s: pre-place net probe RAISED: %s — "
                "ABORTING retry (no blind order); leg left for guarded retry",
                session_id, symbol, _net_e)
            registry.mark_exit_failed(
                symbol, "retry aborted: net probe raised",
                broker_profile=broker_profile)
            return {"status": "EXIT_ABORTED_PROBE_FAILED", "symbol": symbol}
        our_held = _our_held(session_id, symbol, instrument_type, qty, net_qty)
        if net_qty is not None and our_held == 0:
            log.warning(
                "cancel_and_retry_exit %s/%s: broker already flat FOR THIS SESSION "
                "(net=%s) — reconciling, placing NO order", session_id, symbol,
                net_qty)
            registry.mark_closed(symbol, f"{close_reason}_RECONCILED_FLAT",
                                 broker_profile=broker_profile)
            return {"status": "COMPLETE", "reconciled_flat": True,
                    "filled_qty": 0, "exit_price": None, "symbol": symbol}
        if our_held is not None and 0 < our_held < qty:
            log.warning(
                "cancel_and_retry_exit %s/%s: CLAMP retry qty %d→%d "
                "(session-scoped live-held)", session_id, symbol, qty,
                int(our_held))
            qty = int(our_held)

        # Step 3: fresh market exit in the CLOSING side (long→SELL, short→BUY).
        try:
            res = await broker.place_market_exit(symbol, qty, instrument_type,
                                                  kite_product=kite_product,
                                                  direction=direction)
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
            registry.mark_closed(symbol, close_reason,
                                 broker_profile=broker_profile)
            return {"status": "COMPLETE", "filled_qty": qty, "exit_price": None}

        # Step 4: confirm the new order.
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
            broker_profile=broker_profile,
        )
        last_result = result
        if result.get("status") == "COMPLETE":
            return result

    # All retries exhausted.
    log.error(
        "cancel_and_retry_exit %s/%s: all %d retries failed — marking EXIT_FAILED",
        session_id, symbol, max_retries)
    registry.mark_exit_failed(symbol, "cancel_and_retry exhausted",
                              broker_profile=broker_profile)
    return last_result
