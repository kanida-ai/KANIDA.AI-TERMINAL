"""Batch executor — places BUY + SL on Kite with idempotency tags + audit log.

Safety gate: FALCON_AUTOTRADE_ENABLED env var must equal 'true' or every
place_order call short-circuits to REJECTED. Operator confirmation in the UI
is the primary control; this is defense-in-depth for dev/testing.

Retry policy (Requirements §10 F8): linear backoff on transient Kite errors
(network blip, rate limit). Idempotency tag prevents duplicate orders if a
retry succeeds after the first attempt actually went through but its response
got dropped — _orders_with_tag pre-check catches that case.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional

from . import trade_db
from .order_planner import OrderSpec

log = logging.getLogger("kanida.falcon.trade.executor")

# Retry budget for transient Kite errors (network / 429 rate-limit).
# Linear backoff: attempt N waits N seconds (1, 2, 3 seconds before retries
# 1, 2, 3 — total worst-case ~6s additional latency).
RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_SLEEP_SEC = 1.0


def _autotrade_enabled() -> bool:
    return os.environ.get("FALCON_AUTOTRADE_ENABLED", "").lower() == "true"


def _is_transient_kite_error(exc: BaseException) -> bool:
    """True if exc is the kind we should retry (network / 429 / 502 / etc.).
    False for validation / auth / order-rejected — those are deterministic and
    should not be retried."""
    name = type(exc).__name__
    msg = str(exc).lower()
    # Permanent failures — never retry
    permanent = (
        "TokenException", "InputException", "OrderException",
        "PermissionException", "TwoFAException",
    )
    if name in permanent:
        return False
    # Validation/business errors visible only via message
    deterministic_phrases = (
        "insufficient", "margin", "circuit limit", "tick size",
        "freeze quantity", "not allowed", "rejected", "unauthorized",
        "invalid", "blocked",
    )
    if any(p in msg for p in deterministic_phrases):
        return False
    # Transient signatures
    transient_phrases = (
        "timeout", "timed out", "connection", "network", "temporarily",
        "rate limit", "too many requests", "429", "502", "503", "504",
        "upstream", "unavailable", "gateway",
    )
    if name in ("NetworkException", "GeneralException", "DataException"):
        return True
    if any(p in msg for p in transient_phrases):
        return True
    # Default: don't retry unknown errors — safer to fail loudly than to spam
    return False


def _retry_kite_call(fn: Callable[[], Any], op: str, symbol: str = "") -> Any:
    """Call fn() with linear backoff on transient errors.
    Re-raises the last exception if all attempts fail. Permanent errors raise
    immediately on first attempt (no retry)."""
    last_exc: Optional[BaseException] = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if not _is_transient_kite_error(e):
                raise  # permanent — fail fast, don't waste retries
            if attempt >= RETRY_MAX_ATTEMPTS:
                log.error("%s %s exhausted %d retries: %s",
                          op, symbol, RETRY_MAX_ATTEMPTS, e)
                raise
            sleep_sec = RETRY_BASE_SLEEP_SEC * attempt
            log.warning("%s %s transient error (attempt %d/%d): %s — retrying in %.1fs",
                        op, symbol, attempt, RETRY_MAX_ATTEMPTS, e, sleep_sec)
            time.sleep(sleep_sec)
    # Should never reach here, but be defensive
    if last_exc:
        raise last_exc
    raise RuntimeError(f"{op} {symbol} failed without exception")


_ROLE_CHAR = {"ENTRY": "e", "STOP": "s", "SMOKE": "k", "EXIT": "x"}


def idempotency_key(batch_id: str, symbol: str, role: str) -> str:
    """Generate Kite-tag-compatible idempotency key (max 20 chars).
    Role chars are explicit (no first-letter aliasing — SMOKE/STOP would collide).

    Picks the LAST 6-hex segment in batch_id, regardless of prefix. Handles:
      btc_YYYY_MM_DD_<6hex>            → hex
      btc_YYYY_MM_DD_<6hex>_smk        → hex (skips suffix)
      adopt_bulk_YYYY_MM_DD_<6hex>     → hex
      anything_with_<6hex>_at_end      → hex
    Falls back to last segment if no hex found.
    """
    import string
    hex_chars = set(string.hexdigits.lower())
    parts = batch_id.split("_")
    batch_short = parts[-1][:6].lower()  # default: last segment
    for p in reversed(parts):
        candidate = p[:6].lower()
        if len(candidate) == 6 and all(c in hex_chars for c in candidate):
            batch_short = candidate
            break
    sym_short = symbol[:8].lower().replace("-", "").replace("&", "")
    role_char = _ROLE_CHAR.get(role.upper(), "?")
    tag = f"f-{batch_short}-{sym_short}-{role_char}"
    return tag[:20]


def _orders_with_tag(kite, tag: str) -> List[dict]:
    try:
        all_orders = kite.orders()
    except Exception as e:
        log.warning("kite.orders() failed during idempotency check: %s", e)
        return []
    return [o for o in all_orders if (o.get("tag") or "") == tag]


def _kite_order_type(kite, order_type: str):
    if order_type == "MARKET":
        return kite.ORDER_TYPE_MARKET
    if order_type == "LIMIT":
        return kite.ORDER_TYPE_LIMIT
    if order_type == "SL-M":
        return kite.ORDER_TYPE_SLM
    if order_type in ("SL", "SL-L"):
        return kite.ORDER_TYPE_SL
    raise ValueError(f"Unknown order_type: {order_type}")


def _kite_product(kite, product: str):
    # Kite SDK exposes PRODUCT_CNC / PRODUCT_MIS / PRODUCT_NRML as class constants
    # but NOT PRODUCT_MTF — MTF is passed as the literal string "MTF".
    if product == "MTF":
        return "MTF"
    if product == "CNC":
        return kite.PRODUCT_CNC
    return product


def _place_with_idempotency(kite, batch_id: str, spec: OrderSpec,
                             role: str) -> Dict[str, Any]:
    """Place a single Kite order with idempotency. Returns dict with status."""
    tag = idempotency_key(batch_id, spec.symbol, role)

    # Pre-place idempotency check
    existing = _orders_with_tag(kite, tag)
    for e in existing:
        st = (e.get("status") or "").upper()
        if st in ("OPEN", "COMPLETE", "TRIGGER PENDING"):
            log.info("Idempotent skip: tag=%s already placed (kite_id=%s status=%s)",
                     tag, e.get("order_id"), st)
            return {
                "kite_order_id": str(e.get("order_id")),
                "status":        "PLACED",
                "error":         None,
                "skipped_dup":   True,
                "tag":           tag,
            }

    if not _autotrade_enabled():
        return {
            "kite_order_id": None,
            "status":        "REJECTED",
            "error":         "FALCON_AUTOTRADE_ENABLED not set to 'true'",
            "skipped_dup":   False,
            "tag":           tag,
        }

    if role == "ENTRY":
        side, order_type, price, trigger = "BUY", "MARKET", None, None
    elif role == "STOP":
        side, order_type = "SELL", spec.sl_order_type
        price   = spec.sl_limit_price          # only for SL-L
        trigger = spec.sl_price
    elif role == "SMOKE":
        side, order_type, price, trigger = "BUY", "MARKET", None, None
    elif role == "EXIT":
        side, order_type, price, trigger = "SELL", "MARKET", None, None
    else:
        return {"kite_order_id": None, "status": "REJECTED",
                "error": f"unknown role: {role}", "skipped_dup": False, "tag": tag}

    # Zerodha rejects naked MARKET (and SL-M) orders via API without market_protection
    # (a slippage cap %). 1.0 = ±1% from current quote. Liquid NSE EQ rarely moves >1%
    # in seconds; if it does, the order is rejected (which is the safety we want).
    extra: Dict[str, Any] = {}
    if order_type in ("MARKET", "SL-M"):
        extra["market_protection"] = 1.0

    def _do_place():
        return kite.place_order(
            variety          = kite.VARIETY_REGULAR,
            exchange         = kite.EXCHANGE_NSE,
            tradingsymbol    = spec.symbol,
            transaction_type = (kite.TRANSACTION_TYPE_BUY if side == "BUY"
                                else kite.TRANSACTION_TYPE_SELL),
            quantity         = spec.qty,
            product          = _kite_product(kite, "MTF"),
            order_type       = _kite_order_type(kite, order_type),
            price            = price,
            trigger_price    = trigger,
            tag              = tag,
            **extra,
        )

    try:
        kite_order_id = _retry_kite_call(_do_place, "place_order", spec.symbol)
        return {
            "kite_order_id": str(kite_order_id),
            "status":        "PLACED",
            "error":         None,
            "skipped_dup":   False,
            "tag":           tag,
        }
    except Exception as e:
        log.error("Kite place_order failed (%s/%s): %s", spec.symbol, role, e)
        return {
            "kite_order_id": None,
            "status":        "FAILED",
            "error":         str(e),
            "skipped_dup":   False,
            "tag":           tag,
        }


def execute_batch(kite, batch_id: str, orders: List[OrderSpec]) -> Dict[str, Any]:
    """Execute a batch of orders. ENTRY then STOP per stock; abort on hard failures.
    F7: per-stock margin error → skip + continue (NOT abort).
    F5/S6: BUY ok + SL fails → abort.
    """
    n_filled = 0
    n_failed = 0
    aborted  = False
    abort_reason: Optional[str] = None
    total_deployed = 0

    for spec in orders:
        # ENTRY
        entry_tag = idempotency_key(batch_id, spec.symbol, "ENTRY")
        entry_id = trade_db.insert_order(
            batch_id        = batch_id,
            symbol          = spec.symbol,
            side            = "BUY",
            role            = "ENTRY",
            qty             = spec.qty,
            idempotency_key = entry_tag,
            price           = None,
            trigger_price   = None,
            product         = "MTF",
            order_type      = "MARKET",
            status          = "PLACING",
            is_averaging    = spec.is_averaging,
        )
        result = _place_with_idempotency(kite, batch_id, spec, "ENTRY")
        trade_db.update_order_status(entry_id, result["status"],
                                      result["kite_order_id"], result["error"])

        if result["status"] != "PLACED":
            err_lower = (result["error"] or "").lower()
            # F7: per-stock margin → skip + continue
            if "margin" in err_lower or "insufficient" in err_lower:
                log.warning("Per-stock margin error on %s; skipping (F7)", spec.symbol)
                n_failed += 1
                continue
            # Otherwise abort the whole batch
            n_failed += 1
            aborted = True
            abort_reason = f"ENTRY_FAILED on {spec.symbol}: {result['error']}"
            break

        # STOP — atomically follows ENTRY
        stop_tag = idempotency_key(batch_id, spec.symbol, "STOP")
        stop_id = trade_db.insert_order(
            batch_id        = batch_id,
            symbol          = spec.symbol,
            side            = "SELL",
            role            = "STOP",
            qty             = spec.qty,
            idempotency_key = stop_tag,
            price           = spec.sl_limit_price,
            trigger_price   = spec.sl_price,
            product         = "MTF",
            order_type      = spec.sl_order_type,
            status          = "PLACING",
            is_averaging    = spec.is_averaging,
        )
        sl_result = _place_with_idempotency(kite, batch_id, spec, "STOP")
        trade_db.update_order_status(stop_id, sl_result["status"],
                                      sl_result["kite_order_id"], sl_result["error"])

        if sl_result["status"] != "PLACED":
            # S6 / F5: BUY ok + SL fails → abort, naked long alert
            n_failed += 1
            aborted = True
            abort_reason = (f"STOP_FAILED_AFTER_BUY on {spec.symbol}: "
                            f"naked long pending; {sl_result['error']}")
            break

        n_filled += 1
        total_deployed += int(spec.notional)

    return {
        "n_attempted":    len(orders),
        "n_filled":       n_filled,
        "n_failed":       n_failed,
        "total_deployed": total_deployed,
        "aborted":        aborted,
        "abort_reason":   abort_reason,
    }


def smoke_test_one(kite, batch_id: str, cheapest: OrderSpec) -> Dict[str, Any]:
    """Place a 1-share BUY of the cheapest pick (no SL). Verifies Kite plumbing."""
    smoke_spec = OrderSpec(
        rank          = 0,
        symbol        = cheapest.symbol,
        sector        = cheapest.sector,
        close         = cheapest.close,
        qty           = 1,
        notional      = cheapest.close,
        sl_price      = 0.0,
        target_price  = 0.0,
        sl_order_type = "SL-L",
        sl_limit_price= None,
        is_averaging  = False,
        existing_qty  = 0,
    )
    smoke_tag = idempotency_key(batch_id, cheapest.symbol, "SMOKE")
    order_id = trade_db.insert_order(
        batch_id        = batch_id,
        symbol          = cheapest.symbol,
        side            = "BUY",
        role            = "SMOKE",
        qty             = 1,
        idempotency_key = smoke_tag,
        product         = "MTF",
        order_type      = "MARKET",
        status          = "PLACING",
    )
    result = _place_with_idempotency(kite, batch_id, smoke_spec, "SMOKE")
    trade_db.update_order_status(order_id, result["status"],
                                  result["kite_order_id"], result["error"])
    return result


def cancel_all(kite, batch_id: str) -> Dict[str, Any]:
    """Cancel all PENDING/PLACED orders in a batch. Kill switch."""
    orders = trade_db.get_orders_for_batch(batch_id)
    cancelled = 0
    skipped   = 0
    errors: List[Dict[str, Any]] = []
    for o in orders:
        if o["status"] not in ("PLACED", "PENDING"):
            skipped += 1
            continue
        if not o["kite_order_id"]:
            skipped += 1
            continue
        try:
            _retry_kite_call(
                lambda: kite.cancel_order(variety=kite.VARIETY_REGULAR,
                                            order_id=o["kite_order_id"]),
                "cancel_order", o.get("symbol") or "",
            )
            trade_db.update_order_status(o["id"], "CANCELLED")
            cancelled += 1
        except Exception as e:
            errors.append({"order_id": o["id"], "error": str(e)})
    return {"cancelled": cancelled, "skipped": skipped, "errors": errors}
