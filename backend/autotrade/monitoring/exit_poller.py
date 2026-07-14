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
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

from .. import order_ledger as _order_ledger

log = logging.getLogger("kanida.autotrade.exit_poller")
IST = timezone(timedelta(hours=5, minutes=30))


def _persisted_exit_coid(session_id: str, symbol: str,
                         broker_profile: Optional[str] = None) -> Optional[str]:
    """The exit client_order_id already persisted on the position row (if any),
    scoped to (session_id, symbol[, broker_profile]). Used by the kill/retry path
    so the exit tag is STABLE across restarts (query-before-place). Best-effort:
    any DB error → None (mint a fresh one)."""
    try:
        from falcon.db import falcon_conn
        with falcon_conn() as con:
            if broker_profile is not None:
                r = con.execute(
                    "SELECT exit_client_order_id FROM autotrade_positions "
                    "WHERE session_id=? AND symbol=? "
                    "AND COALESCE(broker_profile,'')=COALESCE(?,'')",
                    (session_id, symbol, broker_profile)).fetchone()
            else:
                r = con.execute(
                    "SELECT exit_client_order_id FROM autotrade_positions "
                    "WHERE session_id=? AND symbol=?",
                    (session_id, symbol)).fetchone()
        v = r["exit_client_order_id"] if r else None
        return str(v) if v not in (None, "") else None
    except Exception as e:  # pragma: no cover - defensive
        log.debug("_persisted_exit_coid %s/%s failed: %s", session_id, symbol, e)
        return None


class OrderbookSnapshot:
    """SPRINT CLUSTER 5 ITEM 6 — ONE broker orderbook fetch per poll CYCLE shared
    across N in-flight exit legs.

    During a kill flatten of N legs, each confirm_exit used to poll its own leg via
    broker.get_order_status(order_id), and each of those does a FULL kite.orders()
    scan → N full-orderbook fetches per 5s cycle. This snapshot fetches the whole
    orderbook ONCE (broker.get_orders()) and serves every leg's status from the
    cached map, refreshing at most once per `ttl_sec`. Thread-safe: confirm_exit
    calls .status() from a worker thread (asyncio.to_thread), and the lock makes
    concurrent legs share a single fetch. .fetches counts the real broker fetches
    (for the mutation test). When the broker exposes NO batched orderbook
    (get_orders() returns None — a paper broker or an adapter without a batched
    call) the snapshot FALLS BACK to the per-leg broker.get_order_status(order_id),
    so behaviour is byte-for-byte the pre-Cluster-5 path for those brokers."""

    def __init__(self, broker: Any, ttl_sec: float = 5.0):
        self._broker = broker
        self._ttl = float(ttl_sec)
        self._rows: Optional[Dict[str, dict]] = None
        self._ts = 0.0
        self._lock = threading.Lock()
        self.fetches = 0

    def status(self, order_id: str) -> dict:
        now = time.monotonic()
        use_fallback = False
        with self._lock:
            if self._rows is None or (now - self._ts) >= self._ttl:
                try:
                    ob = self._broker.get_orders()
                except Exception as e:  # pragma: no cover - defensive
                    log.debug("OrderbookSnapshot: get_orders failed: %s", e)
                    ob = None
                if ob is not None:
                    self._rows = {str(r.get("order_id")): r for r in ob
                                  if isinstance(r, dict)}
                    self._ts = now
                    self.fetches += 1
                elif self._rows is None:
                    # No batched orderbook available at all → per-leg fallback
                    # (byte-for-byte the pre-Cluster-5 path for this broker).
                    use_fallback = True
            rows = None if use_fallback else (self._rows or {})
        if use_fallback:
            try:
                return dict(self._broker.get_order_status(order_id) or {})
            except Exception:  # pragma: no cover - defensive
                return {}
        return dict(rows.get(str(order_id), {}))

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
    status_provider: Optional[Callable[[str], dict]] = None,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Poll the broker until the exit order is fill-confirmed.

    client_order_id (SPRINT CLUSTER 10 ITEM 1, additive, DEFAULT None): the durable
    EXIT client_order_id whose compact_tag rode on this exit order. When supplied it
    is persisted onto the EXIT_PLACED / EXIT_FILLED ledger events so the ledger — not
    just the position row — carries OUR ownership key for the exit, letting recovery
    + the reconciler attribute the fill by our tag even if the broker order-id was
    never recorded on the row. None (all pre-CLUSTER-10 callers) keeps the ledger
    rows byte-for-byte (a NULL client_order_id, exactly as before).

    status_provider (CLUSTER 5 ITEM 6): an optional callable order_id -> status
    dict used INSTEAD of broker.get_order_status. The kill path passes a shared
    OrderbookSnapshot.status so N concurrent legs resolve from ONE orderbook fetch
    per poll cycle rather than N full get_order_status scans. None (DEFAULT) keeps
    the per-leg broker.get_order_status path byte-for-byte (all other callers).

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
        broker_profile=broker_profile, broker_order_id=order_id,
        client_order_id=client_order_id, qty=qty,
        source="exit", detail=close_reason)

    deadline = datetime.now(IST) + timedelta(seconds=max_wait_sec)
    filled_qty = 0
    avg_price = 0.0
    last_kite_status = "UNKNOWN"

    log.info("confirm_exit %s/%s: polling order %s (max %ds)",
             session_id, symbol, order_id, max_wait_sec)

    _status_fn = status_provider if status_provider is not None \
        else broker.get_order_status
    while datetime.now(IST) < deadline:
        try:
            status_dict = await asyncio.to_thread(_status_fn, order_id)
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
                    client_order_id=client_order_id,
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
            # CLUSTER 9 ITEM 7 (2026-07-11) — a CANCELLED/REJECTED order can still
            # carry a PARTIAL fill (some lots executed before the cancel/reject).
            # Booking NOTHING here left the DB qty OVERSTATED → the guarded retry
            # would try to exit the WHOLE original qty again → oversell into a
            # reverse/naked position. Reduce the DB qty by the filled portion FIRST
            # (profile-scoped), so the retry exits only the REMAINDER. filled_qty==0
            # (the common full-reject) is byte-for-byte unchanged (no partial call).
            if filled_qty > 0:
                fill_price = avg_price if avg_price > 0 else None
                registry.update_partial_exit(symbol, filled_qty, fill_price,
                                             broker_profile=broker_profile)
                log.warning("confirm_exit %s/%s: order %s with PARTIAL fill=%d — "
                            "booked filled portion before EXIT_FAILED (remainder "
                            "%d)", session_id, symbol, kite_status, filled_qty,
                            max(0, qty - filled_qty))
            registry.mark_exit_failed(
                symbol, f"order {kite_status}",
                broker_profile=broker_profile, exit_order_id=order_id)
            log.error("confirm_exit %s/%s: order %s — gate released for retry",
                      session_id, symbol, kite_status)
            return {"status": kite_status, "filled_qty": filled_qty,
                    "remaining_qty": max(0, qty - filled_qty), "exit_price": None}

        elif kite_status == "DRY_RUN":
            # Synthetic status from mock/paper broker.
            registry.mark_closed(symbol, "EXIT_DRY_RUN")
            return {"status": "COMPLETE", "filled_qty": qty, "exit_price": None}

        # Still pending (OPEN / TRIGGER PENDING / etc.) — wait and retry.
        log.debug("confirm_exit %s/%s: order status=%s filled=%d — waiting %.0fs",
                  session_id, symbol, kite_status, filled_qty, poll_interval_sec)
        await asyncio.sleep(poll_interval_sec)

    # Deadline passed without terminal status.
    # CLUSTER 9 ITEM 7 — a TIMEOUT can still have a PARTIAL fill (some lots executed
    # while the rest stayed pending). Book the filled portion (profile-scoped) so
    # the DB qty is the REMAINDER before the caller cancels + retries, otherwise the
    # retry would re-exit the whole original qty → oversell. The caller's
    # cancel_and_retry_exit cancels the still-pending order FIRST and re-probes the
    # net (our_held clamp) so the remainder is never double-covered; total realised
    # P&L = partial + remainder = correct. filled_qty==0 is byte-for-byte unchanged.
    if filled_qty > 0:
        fill_price = avg_price if avg_price > 0 else None
        registry.update_partial_exit(symbol, filled_qty, fill_price,
                                     broker_profile=broker_profile)
        log.warning("confirm_exit %s/%s: TIMEOUT with PARTIAL fill=%d — booked "
                    "filled portion (remainder %d) before retry", session_id,
                    symbol, filled_qty, max(0, qty - filled_qty))
    log.error(
        "confirm_exit %s/%s: TIMEOUT after %ds (last_status=%s filled=%d)",
        session_id, symbol, max_wait_sec, last_kite_status, filled_qty)
    return {"status": "TIMEOUT", "filled_qty": filled_qty,
            "remaining_qty": max(0, qty - filled_qty), "exit_price": None}


async def poll_order_fill(
    order_id: Optional[str],
    qty: int,
    broker: Any,
    *,
    max_wait_sec: int = 60,
    poll_interval_sec: float = 5.0,
    status_provider: Optional[Callable[[str], dict]] = None,
) -> Dict[str, Any]:
    """SPRINT CLUSTER 8 — poll ONE exit order to a terminal state, returning
    {status, filled_qty, avg_price} WITHOUT any registry mutation.

    This is the CHILD-level confirmation primitive for the exit iceberg
    (slice_and_confirm_exit): a large exit is placed as N child orders and each
    child is confirmed here, but the registry close/partial is performed ONCE at
    the end by the aggregator. confirm_exit() (which mutates the registry per
    order) is deliberately NOT reused per child, because a single child COMPLETE
    must NEVER mark the WHOLE multi-child position CLOSED.

    status is one of COMPLETE / PARTIAL / REJECTED / CANCELLED / TIMEOUT / DRY_RUN.
    A None / DRY_RUN order-id returns DRY_RUN immediately (paper — filled=qty).
    Never mutates the registry; never places or cancels an order."""
    if order_id is None or str(order_id).upper() in ("DRY_RUN", "NONE", ""):
        return {"status": "DRY_RUN", "filled_qty": int(qty), "avg_price": None}
    deadline = datetime.now(IST) + timedelta(seconds=max_wait_sec)
    filled_qty = 0
    avg_price = 0.0
    _status_fn = status_provider if status_provider is not None \
        else broker.get_order_status
    while datetime.now(IST) < deadline:
        try:
            status_dict = await asyncio.to_thread(_status_fn, order_id)
        except Exception as e:
            log.warning("poll_order_fill %s: get_order_status error: %s — retrying",
                        order_id, e)
            await asyncio.sleep(poll_interval_sec)
            continue
        kite_status = str(status_dict.get("status", "")).upper()
        filled_qty = int(status_dict.get("filled_quantity") or 0)
        avg_price = float(status_dict.get("average_price") or 0.0)
        if kite_status == "COMPLETE":
            px = avg_price if avg_price > 0 else None
            if filled_qty >= qty:
                return {"status": "COMPLETE", "filled_qty": filled_qty,
                        "avg_price": px}
            return {"status": "PARTIAL", "filled_qty": filled_qty, "avg_price": px}
        if kite_status == "DRY_RUN":
            return {"status": "DRY_RUN", "filled_qty": int(qty), "avg_price": None}
        if kite_status in ("REJECTED", "CANCELLED"):
            return {"status": kite_status, "filled_qty": filled_qty,
                    "avg_price": None}
        await asyncio.sleep(poll_interval_sec)
    return {"status": "TIMEOUT", "filled_qty": filled_qty, "avg_price": None}


async def slice_and_confirm_exit(
    *,
    session_id: str,
    symbol: str,
    total_qty: int,
    legs: list,
    broker: Any,
    registry: Any,
    close_reason: str,
    broker_profile: Optional[str] = None,
    direction: str = "long",
    instrument_type: str = "EQ",
    kite_product: Optional[str] = None,
    exec_cfg: Any = None,
    parent_exit_coid: Optional[str] = None,
    status_provider: Optional[Callable[[str], dict]] = None,
    max_wait_sec: int = 60,
    poll_interval_sec: float = 5.0,
) -> Dict[str, Any]:
    """SPRINT CLUSTER 8 — EXIT ICEBERG. Place `legs` child exits SEQUENTIALLY
    (each <= the effective slice), CONFIRM each fill, AGGREGATE, and CLOSE the
    position ONLY when the FULL `total_qty` is covered.

    `total_qty` MUST already be the C1-clamped qty (never more than our_held): the
    caller clamps FIRST, then slices the clamped qty — this function never clamps,
    it only slices what it was given, so it can never place more than total_qty.

    * All children fill (Σ == total_qty) → registry.mark_closed at the
      quantity-weighted-average exit fill. COMPLETE.
    * A child reject / partial / timeout STOPS further children: the filled portion
      is booked via update_partial_exit (row stays OPEN at the remainder) and the
      REMAINDER is flagged EXIT_FAILED (releases the gate → the guarded EXIT_FAILED
      retry sweep re-attempts it, re-slicing the remainder). Never a mis-stated
      full close.
    * No child filled → EXIT_FAILED (nothing booked).

    Single-flight is the CALLER's responsibility (the whole sliced exit runs under
    ONE exit_gate claim / begin_exit_flight): this function claims/releases
    nothing except the release implied by mark_exit_failed on the failure path,
    matching the existing single-order exit paths."""
    total = int(total_qty)
    parent = parent_exit_coid or _order_ledger.make_client_order_id(
        session_id, symbol)
    filled_total = 0
    weighted_px = 0.0
    n_children = 0
    child_oids: list = []
    stopped_reason: Optional[str] = None

    for i, leg_qty in enumerate(legs):
        leg_qty = int(leg_qty)
        if leg_qty <= 0:
            continue
        child_coid = f"{parent}-X{i}"
        try:
            res = await broker.place_market_exit(
                symbol, leg_qty, instrument_type, kite_product=kite_product,
                direction=direction, exec_cfg=exec_cfg,
                client_order_id=child_coid)
        except Exception as e:
            stopped_reason = f"child {i} place raised: {e}"
            log.error("iceberg exit %s/%s: %s — stopping at %d/%d filled",
                      session_id, symbol, stopped_reason, filled_total, total)
            break
        if res is None or getattr(res, "status", None) == "FAILED":
            stopped_reason = (f"child {i} rejected "
                              f"({getattr(res, 'error', None)})")
            log.error("iceberg exit %s/%s: %s — STOPPING; %d/%d covered",
                      session_id, symbol, stopped_reason, filled_total, total)
            break
        oid = getattr(res, "broker_order_id", None)
        is_dry = (oid is None or str(oid).upper() in ("DRY_RUN", "NONE", ""))
        # Durable EXIT_PLACED per child (the ONE universal exit choke-point
        # attribution key). Best-effort — never blocks the flatten.
        _order_ledger.append_event(
            session_id=session_id, symbol=symbol,
            event_type=_order_ledger.EV_EXIT_PLACED,
            broker_profile=broker_profile, broker_order_id=oid,
            client_order_id=child_coid, qty=leg_qty,
            source="exit", detail=f"{close_reason}:iceberg-child-{i}")
        if is_dry:
            filled_total += leg_qty
            n_children += 1
            continue
        fill = await poll_order_fill(
            oid, leg_qty, broker, max_wait_sec=max_wait_sec,
            poll_interval_sec=poll_interval_sec, status_provider=status_provider)
        st = fill.get("status")
        fq = int(fill.get("filled_qty") or 0)
        px = fill.get("avg_price")
        if st in ("COMPLETE", "DRY_RUN"):
            n_children += 1
            filled_total += leg_qty
            if px:
                weighted_px += float(px) * leg_qty
            if oid:
                child_oids.append(str(oid))
            continue
        if st == "PARTIAL":
            if fq > 0:
                filled_total += fq
                if px:
                    weighted_px += float(px) * fq
                if oid:
                    child_oids.append(str(oid))
            # Cancel the resting remainder of THIS child so it can't fill alongside
            # the retry (a resting order does not reduce the broker net).
            try:
                await asyncio.to_thread(broker.cancel_order_sync, oid)
            except Exception as _ce:  # pragma: no cover - best-effort
                log.warning("iceberg exit %s/%s: cancel of partial child %s "
                            "failed: %s", session_id, symbol, oid, _ce)
            stopped_reason = f"child {i} PARTIAL {fq}/{leg_qty}"
            break
        if st == "TIMEOUT":
            try:
                await asyncio.to_thread(broker.cancel_order_sync, oid)
            except Exception as _ce:  # pragma: no cover - best-effort
                log.warning("iceberg exit %s/%s: cancel of timed-out child %s "
                            "failed: %s", session_id, symbol, oid, _ce)
            stopped_reason = f"child {i} TIMEOUT"
            break
        # REJECTED / CANCELLED
        stopped_reason = f"child {i} {st}"
        break

    avg_px = (weighted_px / filled_total) if (filled_total > 0
                                              and weighted_px > 0) else None
    primary_oid = child_oids[0] if child_oids else None

    if filled_total >= total:
        # FULL cover → CLOSE the aggregate position ONCE, at the weighted-avg fill.
        registry.mark_closed(symbol, close_reason, exit_price=avg_px,
                             broker_profile=broker_profile,
                             exit_order_id=primary_oid)
        log.warning("iceberg exit %s/%s: FULL cover %d across %d child(ren) "
                    "@ %s — CLOSED", session_id, symbol, filled_total,
                    n_children, avg_px)
        return {"status": "COMPLETE", "filled_qty": filled_total,
                "exit_price": avg_px, "n_children": n_children,
                "symbol": symbol, "iceberg": True}
    if filled_total > 0:
        # Partial cover → book the filled portion, leave the REMAINDER OPEN and
        # flag it EXIT_FAILED (guarded retry re-slices it). NEVER a full close.
        registry.update_partial_exit(symbol, filled_total, avg_px,
                                     broker_profile=broker_profile)
        remaining = total - filled_total
        registry.mark_exit_failed(
            symbol,
            f"iceberg exit partial: covered {filled_total}/{total} "
            f"({stopped_reason})",
            broker_profile=broker_profile, exit_order_id=primary_oid)
        log.error("iceberg exit %s/%s: PARTIAL cover %d/%d — remainder %d "
                  "EXIT_FAILED for guarded retry (%s)", session_id, symbol,
                  filled_total, total, remaining, stopped_reason)
        return {"status": "EXIT_FAILED", "filled_qty": filled_total,
                "remaining_qty": remaining, "exit_price": avg_px,
                "n_children": n_children, "symbol": symbol, "iceberg": True,
                "partial": True, "stopped_reason": stopped_reason}
    # No child filled → nothing booked; flag EXIT_FAILED for retry.
    registry.mark_exit_failed(
        symbol, f"iceberg exit: no child filled ({stopped_reason})",
        broker_profile=broker_profile)
    log.error("iceberg exit %s/%s: NO child filled — EXIT_FAILED (%s)",
              session_id, symbol, stopped_reason)
    return {"status": "EXIT_FAILED", "filled_qty": 0, "n_children": 0,
            "symbol": symbol, "iceberg": True, "stopped_reason": stopped_reason}


async def work_and_confirm_exit(
    *,
    session_id: str,
    symbol: str,
    total_qty: int,
    broker: Any,
    registry: Any,
    close_reason: str,
    exec_cfg: Any,
    broker_profile: Optional[str] = None,
    direction: str = "long",
    instrument_type: str = "EQ",
    kite_product: Optional[str] = None,
    parent_exit_coid: Optional[str] = None,
    status_provider: Optional[Callable[[str], dict]] = None,
    deadline_ts: Optional[float] = None,
    max_wait_sec: int = 60,
    poll_interval_sec: float = 5.0,
    now_fn: Optional[Callable[[], float]] = None,
    sleep_fn: Optional[Callable[[float], Any]] = None,
    volume_fn: Optional[Callable[[str], Optional[float]]] = None,
    child_sizer: Optional[Callable] = None,
) -> Dict[str, Any]:
    """WORKED (participation / TWAP) EXIT — the PACED sibling of
    slice_and_confirm_exit. Instead of firing all freeze-cap child legs
    back-to-back, it sizes each child by POV (worked_participation_pct × recent
    volume) with a TWAP floor + freeze cap and SPACES them by worked_interval_sec
    until the (tighter) deadline — so a ₹125cr flatten does not slam the book in one
    burst. Reuses the SAME child primitives (place_market_exit → EXIT_PLACED ledger
    → poll_order_fill) and books ONCE at the end (mark_closed on full cover; else
    update_partial_exit + mark_exit_failed on the remainder), IDENTICAL disposition
    to slice_and_confirm_exit.

    `total_qty` MUST already be the C1-clamped our_held qty (the caller clamps
    first). Single-flight / the exit_gate claim is the CALLER's responsibility (the
    whole worked exit runs under ONE claim), matching slice_and_confirm_exit.
    Injection (tests): now_fn / sleep_fn / volume_fn / deadline_ts.

    DEFAULT-OFF: only reached when exec_cfg.execution_mode == "worked". A paper
    (DRY_RUN) broker fills each child immediately (no poll), byte-for-byte with the
    iceberg-exit paper path."""
    from ..execution import worked_order as _wo
    from .. import iceberg as _iceberg

    total = int(total_qty)
    parent = parent_exit_coid or _order_ledger.make_client_order_id(
        session_id, symbol)
    agg = {"weighted": 0.0, "oids": [], "n": 0}

    async def _place_child(*, idx: int, qty: int, recent_volume=None):
        leg_qty = int(qty)
        child_coid = f"{parent}-XW{idx}"
        try:
            res = await broker.place_market_exit(
                symbol, leg_qty, instrument_type, kite_product=kite_product,
                direction=direction, exec_cfg=exec_cfg, client_order_id=child_coid)
        except Exception as e:
            return {"filled_qty": 0, "avg_price": 0.0, "rejected": True,
                    "error": f"place raised: {e}"}
        if res is None or getattr(res, "status", None) == "FAILED":
            return {"filled_qty": 0, "avg_price": 0.0, "rejected": True,
                    "error": getattr(res, "error", None) or "exit child rejected"}
        oid = getattr(res, "broker_order_id", None)
        is_dry = (oid is None or str(oid).upper() in ("DRY_RUN", "NONE", ""))
        _order_ledger.append_event(
            session_id=session_id, symbol=symbol,
            event_type=_order_ledger.EV_EXIT_PLACED,
            broker_profile=broker_profile, broker_order_id=oid,
            client_order_id=child_coid, qty=leg_qty,
            source="exit", detail=f"{close_reason}:worked-child-{idx}")
        if is_dry:
            agg["n"] += 1
            return {"filled_qty": leg_qty, "avg_price": 0.0, "status": "DRY_RUN",
                    "broker_order_id": oid, "client_order_id": child_coid}
        fill = await poll_order_fill(
            oid, leg_qty, broker, max_wait_sec=max_wait_sec,
            poll_interval_sec=poll_interval_sec, status_provider=status_provider)
        st = fill.get("status")
        fq = int(fill.get("filled_qty") or 0)
        px = fill.get("avg_price")
        if st in ("COMPLETE", "DRY_RUN"):
            # A COMPLETE child filled its FULL leg_qty by definition (mirror
            # slice_and_confirm_exit — never trust a possibly-cumulative poll
            # filled_qty for the accounting).
            agg["n"] += 1
            if px:
                agg["weighted"] += float(px) * leg_qty
            if oid:
                agg["oids"].append(str(oid))
            return {"filled_qty": leg_qty, "avg_price": float(px or 0.0),
                    "status": st, "broker_order_id": oid,
                    "client_order_id": child_coid}
        if st == "PARTIAL":
            if px and fq > 0:
                agg["weighted"] += float(px) * fq
            if oid:
                agg["oids"].append(str(oid))
            # Cancel the resting remainder so it can't fill alongside a retry, then
            # STOP the worked exit (remainder → EXIT_FAILED for the guarded retry).
            try:
                await asyncio.to_thread(broker.cancel_order_sync, oid)
            except Exception as _ce:  # pragma: no cover - best-effort
                log.warning("worked exit %s/%s: cancel of partial child %s failed: %s",
                            session_id, symbol, oid, _ce)
            return {"filled_qty": fq, "avg_price": float(px or 0.0),
                    "stop_after": True, "status": "PARTIAL",
                    "reason": f"child {idx} PARTIAL {fq}/{leg_qty}"}
        # TIMEOUT / REJECTED / CANCELLED — cancel any resting order, stop.
        try:
            await asyncio.to_thread(broker.cancel_order_sync, oid)
        except Exception:  # pragma: no cover - best-effort
            pass
        return {"filled_qty": 0, "avg_price": 0.0, "stop_after": True,
                "status": st, "reason": f"child {idx} {st}"}

    freeze_cap = None
    try:
        freeze_cap = _iceberg.freeze_cap_for(exec_cfg, symbol)
    except Exception:  # pragma: no cover - defensive
        freeze_cap = None
    if deadline_ts is None:
        deadline_ts = _wo.resolve_deadline_ts(exec_cfg, tighten_exit=True)
    _side = "BUY" if str(direction).lower() == "short" else "SELL"
    wp = _wo.WorkedParent(
        symbol=symbol, side=_side, target_qty=total, kind="exit",
        product=(kite_product or instrument_type), instrument_type=instrument_type,
        session_id=session_id, broker_profile=broker_profile,
        deadline_ts=deadline_ts,
        interval_sec=float(getattr(exec_cfg, "worked_interval_sec", 20)),
        participation_pct=float(getattr(exec_cfg, "worked_participation_pct", 0.10)),
        freeze_cap=freeze_cap,
        min_child_qty=int(getattr(exec_cfg, "worked_min_child_qty", 1)),
        max_children=int(getattr(exec_cfg, "worked_max_children", 500)))
    # v2 (VWAP-curve pacing) for the paced UNWIND: build the pluggable child sizer
    # when worked_vwap_enabled (else None → v1 flat POV, byte-identical). No profile
    # → make_vwap_sizer returns None → v1 fallback. Same safety on every child.
    _sizer = child_sizer
    if _sizer is None:
        _sizer = _wo.make_vwap_sizer(exec_cfg, symbol)
    result = await _wo.work_order(
        wp, place_child=_place_child, volume_fn=volume_fn, now_fn=now_fn,
        sleep_fn=sleep_fn, child_sizer=_sizer)

    filled_total = int(result.filled_qty)
    avg_px = (agg["weighted"] / filled_total) if (filled_total > 0
                                                  and agg["weighted"] > 0) else None
    primary_oid = agg["oids"][0] if agg["oids"] else None
    n_children = agg["n"]

    if filled_total >= total:
        registry.mark_closed(symbol, close_reason, exit_price=avg_px,
                             broker_profile=broker_profile,
                             exit_order_id=primary_oid)
        log.warning("worked exit %s/%s: FULL cover %d across %d child(ren) @ %s "
                    "— CLOSED", session_id, symbol, filled_total, n_children, avg_px)
        return {"status": "COMPLETE", "filled_qty": filled_total,
                "exit_price": avg_px, "n_children": n_children, "symbol": symbol,
                "worked": True}
    if filled_total > 0:
        registry.update_partial_exit(symbol, filled_total, avg_px,
                                     broker_profile=broker_profile)
        remaining = total - filled_total
        registry.mark_exit_failed(
            symbol, f"worked exit partial: covered {filled_total}/{total} "
            f"({result.stopped_reason})", broker_profile=broker_profile,
            exit_order_id=primary_oid)
        log.error("worked exit %s/%s: PARTIAL cover %d/%d — remainder %d "
                  "EXIT_FAILED for guarded retry (%s)", session_id, symbol,
                  filled_total, total, remaining, result.stopped_reason)
        return {"status": "EXIT_FAILED", "filled_qty": filled_total,
                "remaining_qty": remaining, "exit_price": avg_px,
                "n_children": n_children, "symbol": symbol, "worked": True,
                "partial": True, "stopped_reason": result.stopped_reason}
    registry.mark_exit_failed(
        symbol, f"worked exit: no child filled ({result.stopped_reason})",
        broker_profile=broker_profile)
    log.error("worked exit %s/%s: NO child filled — EXIT_FAILED (%s)",
              session_id, symbol, result.stopped_reason)
    return {"status": "EXIT_FAILED", "filled_qty": 0, "n_children": 0,
            "symbol": symbol, "worked": True,
            "stopped_reason": result.stopped_reason}


async def adopt_tagged_exit_if_present(
    session_id: str,
    symbol: str,
    exit_client_order_id: Optional[str],
    qty: int,
    broker: Any,
    registry: Any,
    close_reason: str = "EXIT_ADOPTED",
    broker_profile: Optional[str] = None,
    direction: str = "long",
) -> Optional[Dict[str, Any]]:
    """CLUSTER 3 ITEM 3(b) — QUERY-BEFORE-PLACE. Before placing a RETRY / RESUME
    exit, ask the broker orderbook whether OUR exit tag (compact_tag of the
    position's persisted exit_client_order_id) is ALREADY there. If a tagged order
    exists (COMPLETE or still working) it is OUR own earlier placement surviving a
    retry / restart → ADOPT it: confirm/await it and place ZERO new orders. This
    closes the cross-process exactly-once window the in-process locks cannot.

    Returns a confirm_exit-shaped result dict when an order was adopted, else None
    (the caller then proceeds to place). Best-effort — any error / no tag / no
    orderbook → None (place exactly as before; paper is byte-for-byte unchanged
    because a paper broker's get_orders() returns None)."""
    if not exit_client_order_id:
        return None
    try:
        orders = await asyncio.to_thread(broker.get_orders)
    except Exception as e:  # pragma: no cover - defensive
        log.debug("adopt_tagged_exit %s/%s: get_orders raised: %s",
                  session_id, symbol, e)
        return None
    if not orders:
        return None
    tag = _order_ledger.compact_tag(exit_client_order_id)
    closing_side = "BUY" if str(direction).lower() == "short" else "SELL"
    match = _order_ledger.find_broker_order_by_tag(orders, tag,
                                                   closing_side=closing_side)
    if match is None:
        return None
    oid = match.get("order_id")
    if oid in (None, ""):
        return None
    log.warning(
        "adopt_tagged_exit %s/%s: OUR tagged exit order %s already at the broker "
        "(status=%s) — ADOPTING, placing NO new order (query-before-place)",
        session_id, symbol, oid, match.get("status"))
    # Confirm / await the existing order: COMPLETE → mark closed at its fill; still
    # working → poll to terminal. Uses the ONE universal confirm choke point.
    return await confirm_exit(
        session_id=session_id, symbol=symbol, order_id=str(oid), qty=qty,
        broker=broker, registry=registry, close_reason=close_reason,
        broker_profile=broker_profile, client_order_id=exit_client_order_id)


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
    broker_account_id: str | None = None,
    product: str | None = None,
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

    # ITEM 2 (C3 I3a) — QUERY-BEFORE-PLACE on the KILL/RETRY exit path. Until now
    # this path placed an UNTAGGED retry exit, so the adopt_tagged_exit_if_present
    # primitive was dormant here (no persisted exit client_order_id to match). Mint
    # + persist a STABLE exit client_order_id (reuse an existing one so the tag is
    # stable across restarts), then ADOPT any already-placed tagged order at the
    # broker instead of placing a duplicate — cross-restart exactly-once on the kill
    # path. Best-effort; paper's get_orders() is None → adopt returns None → place
    # exactly as before (byte-for-byte unchanged).
    exit_coid = _persisted_exit_coid(session_id, symbol, broker_profile)
    if not exit_coid:
        exit_coid = _order_ledger.make_client_order_id(session_id, symbol,
                                                       attempt=1)
        try:
            registry.set_exit_client_order_id(symbol, exit_coid,
                                              broker_profile=broker_profile)
        except Exception as e:  # pragma: no cover - never block the retry
            log.warning("cancel_and_retry_exit %s/%s: persist exit coid failed: %s",
                        session_id, symbol, e)
    try:
        _adopted = await adopt_tagged_exit_if_present(
            session_id=session_id, symbol=symbol,
            exit_client_order_id=exit_coid, qty=qty, broker=broker,
            registry=registry, close_reason=close_reason,
            broker_profile=broker_profile, direction=direction)
    except Exception as e:  # pragma: no cover - never block the retry
        log.warning("cancel_and_retry_exit %s/%s: adopt raised: %s",
                    session_id, symbol, e)
        _adopted = None
    if _adopted is not None:
        log.warning("cancel_and_retry_exit %s/%s: ADOPTED an existing tagged exit "
                    "(status=%s) — placed NO new order", session_id, symbol,
                    _adopted.get("status"))
        return _adopted

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
        # CLUSTER 9 ITEM 3: scope the sibling subtraction to the SAME broker account
        # so a different account's same-symbol lot is never subtracted.
        our_held = _our_held(session_id, symbol, instrument_type, qty, net_qty,
                             broker_profile=broker_profile,
                             broker_account_id=broker_account_id,
                             product=product)
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
        # ITEM 2: thread the stable exit client_order_id so this placement carries
        # OUR tag → a later retry / restart can ADOPT it (query-before-place).
        try:
            res = await broker.place_market_exit(symbol, qty, instrument_type,
                                                  kite_product=kite_product,
                                                  direction=direction,
                                                  client_order_id=exit_coid)
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
            client_order_id=exit_coid,
        )
        last_result = result
        if result.get("status") == "COMPLETE":
            return result

    # All retries exhausted. CLUSTER 5 ITEM 4 — distinguish a STRANDED-AT-CIRCUIT
    # exit (the stock is locked at the adverse circuit so no counterparty exists)
    # from a generic EXIT_FAILED, so the operator sees the real cause instead of a
    # spinning retry loop. Best-effort: any error → the generic reason (unchanged).
    fail_reason = "cancel_and_retry exhausted"
    try:
        from ..execution.quote_pricer import exit_circuit_locked_reason
        qmap = broker.get_quotes([symbol]) or {}
        locked = exit_circuit_locked_reason(qmap.get(symbol), direction)
        if locked == "circuit_locked":
            fail_reason = ("circuit_locked: exit stranded — stock locked at the "
                           "adverse circuit (no counterparty)")
            last_result = {**last_result, "status": "EXIT_STRANDED_CIRCUIT_LOCKED",
                           "reason": "circuit_locked", "symbol": symbol}
    except Exception as e:  # pragma: no cover - defensive; never block the mark
        log.debug("cancel_and_retry_exit %s/%s: circuit-lock probe failed: %s",
                  session_id, symbol, e)
    log.error(
        "cancel_and_retry_exit %s/%s: all %d retries failed — marking EXIT_FAILED "
        "(%s)", session_id, symbol, max_retries, fail_reason)
    registry.mark_exit_failed(symbol, fail_reason, broker_profile=broker_profile)
    return last_result
