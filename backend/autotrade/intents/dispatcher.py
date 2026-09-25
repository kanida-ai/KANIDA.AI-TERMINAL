"""Intake + group-by-group dispatch of a strategy-intent basket.

Dispatch rules (real-money safety):
  * Groups run in order; a group is sent only after EVERY leg of the previous group is COMPLETELY filled.
    BUY (hedge) groups always precede SELL groups (policy.check_hedge_first), so a short is never sent naked.
  * Live gates are re-checked before every group; a revoked arm or a closed switch stops the basket.
  * Any failure, rejection, timeout or cancel stops the basket: resting orders of the current group are
    cancelled, later groups are never sent, and a basket with any fill ends 'attention_required' for the
    operator. Nothing is ever auto-flattened or blindly resubmitted.
  * Orders go through the existing broker adapter's place_order, which itself returns DRY_RUN unless the adapter
    is live - a dry run never makes a broker order call.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import gates as G
from . import policy
from . import store

log = logging.getLogger("kanida.autotrade.intents")

GROUP_TIMEOUT_S = 60.0
POLL_S = 1.0
RECHECK = ("master", "options", "intents_live", "broker_certified", "market_open")


class IntakeError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


# ── intake ─────────────────────────────────────────────────────────────────────────────────────────────────────────

def submit(body: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """Validate, dedupe and record a basket. Returns (intent, replayed). Never places an order."""
    try:
        basket = policy.validate(body, store.now_ist().date())
    except policy.PolicyError as e:
        raise IntakeError(400, e.code, e.message)
    prior = store.find_by_key(basket["source"], basket["idempotency_key"])
    if prior:
        if prior["payload_hash"] != basket["payload_hash"]:
            raise IntakeError(409, "IDEMPOTENCY_CONFLICT", "this idempotency_key was already used for a different basket")
        return store.get(prior["id"]), True
    ev = G.evaluate(broker=basket["broker"], user_id=basket.get("user_id"),
                    broker_account_id=basket.get("broker_account_id"), max_loss=basket["max_loss"])
    state, reason, arm_id = "accepted", None, None
    if basket["mode"] == "live" and not (basket.get("user_id") and basket.get("broker_account_id")):
        state, reason = "blocked", "LIVE_NEEDS_ACCOUNT: a live basket must name the user and the broker account"
    elif basket["mode"] == "live":
        bad = G.failing(ev["gates"])
        if bad:
            state, reason = "blocked", "LIVE_GATES_FAILED: " + ", ".join(bad)
        elif not store.consume_arm(ev["arm"]["id"]):
            state, reason = "blocked", "LIVE_GATES_FAILED: armed"
        else:
            arm_id = ev["arm"]["id"]
    try:
        iid = store.create(basket, state, reason, ev["gates"], arm_id)
    except Exception:
        if arm_id:
            store.refund_arm(arm_id)                         # this basket was never recorded
        prior = store.find_by_key(basket["source"], basket["idempotency_key"])     # lost a race on the unique key
        if prior and prior["payload_hash"] == basket["payload_hash"]:
            return store.get(prior["id"]), True
        if prior:
            raise IntakeError(409, "IDEMPOTENCY_CONFLICT", "this idempotency_key was already used for a different basket")
        raise
    return store.get(iid), False


def cancel(iid: str) -> Dict[str, Any]:
    """Stop a basket: before dispatch it is cancelled outright; during dispatch the worker stops before the next
    group and cancels resting orders of the current one."""
    store.request_cancel(iid)
    if store.set_state(iid, "cancelled", "Cancelled before dispatch", only_from={"accepted"}):
        _mark_remaining(store.get(iid), "not_sent")
    return store.get(iid)


# ── dispatch ───────────────────────────────────────────────────────────────────────────────────────────────────────

def default_broker_factory(rec: Dict[str, Any], live: bool):
    """Build the broker adapter for this basket. Per-account creds come from the vault, fail-closed: an account that
    cannot be resolved leaves the adapter unable to build a live client (it refuses, never falls back to global)."""
    from ..broker.router import build_client
    from ..config import BrokerProfile
    legs = rec["legs"]
    prof = BrokerProfile(profile_id="strategy-intents", broker_name=rec["broker"], allocated_capital=0.0,
                         order_product=legs[0]["product"], instrument_type=legs[0]["option_type"],
                         broker_account_id=rec.get("broker_account_id") or None)
    acct = prof.broker_account_id
    if acct is not None:
        from .. import vault
        setattr(prof, "_bound_account_id_original", acct)
        creds = vault.get_decrypted_creds(acct, user_id=rec.get("user_id") or None) if vault.vault_enabled() else None
        if creds is None:
            prof.broker_account_id = None
            setattr(prof, "_account_specified_unresolvable", True)
        else:
            setattr(prof, "_account_specified_unresolvable", False)
            prof.api_key, prof.api_secret = creds.api_key or "", creds.api_secret or ""
            prof.access_token = creds.access_token or ""
            if getattr(creds, "broker", None) and str(creds.broker).lower() != rec["broker"]:
                # the gates certified rec["broker"]; never trade through a different adapter than was checked
                raise RuntimeError(f"BROKER_MISMATCH: account is {creds.broker}, basket says {rec['broker']}")
    prof.owner_user_id = rec.get("user_id") or None
    try:
        from ..session import _owner_is_admin
        prof.owner_is_admin = _owner_is_admin(prof.owner_user_id)
    except Exception:
        prof.owner_is_admin = False
    return build_client(prof, dry_run=not live)


def _kite_orders(legs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{"exchange": "NFO", "tradingsymbol": l["tradingsymbol"], "transaction_type": l["side"],
             "variety": "regular", "product": l["product"], "order_type": "LIMIT", "quantity": l["quantity"],
             "price": l["limit_price"]} for l in legs]


async def margin_check(broker, legs: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """Required basket margin (broker-computed, hedge benefit included) vs free margin. Unknown -> refuse."""
    orders = _kite_orders(legs)
    try:
        if hasattr(broker, "basket_margin"):
            required = await asyncio.to_thread(broker.basket_margin, orders)
        else:
            kite = broker.kite
            res = await asyncio.to_thread(kite.basket_order_margins, orders, True, "compact")
            required = float(((res or {}).get("final") or {}).get("total"))
        available = await asyncio.to_thread(broker.available_margin)
    except Exception as e:
        return False, f"MARGIN_UNKNOWN: {type(e).__name__}"
    if required is None or available is None:
        return False, "MARGIN_UNKNOWN: the broker did not return required or available margin"
    if float(required) > float(available):
        return False, f"INSUFFICIENT_MARGIN: needs {float(required):,.2f}, free {float(available):,.2f}"
    return True, f"needs {float(required):,.2f}, free {float(available):,.2f}"


def _recheck(rec: Dict[str, Any]) -> Optional[str]:
    ev = G.evaluate(broker=rec["broker"], user_id=rec.get("user_id") or None,
                    broker_account_id=rec.get("broker_account_id") or None)
    bad = [g["gate"] for g in ev["gates"] if g["gate"] in RECHECK and not g["pass"]]
    arm = ev["arm"]
    if not arm or arm["id"] != rec.get("arm_id"):
        bad.append("armed")                                  # disarmed, expired or replaced mid-basket
    return ", ".join(bad) or None


def _mark_remaining(rec: Dict[str, Any], state: str) -> None:
    for l in rec["legs"]:
        if l["state"] == "pending":
            store.update_leg(l["id"], state=state)


def _any_fill(iid: str) -> bool:
    """True when any order reached the broker without a confirmed zero-fill end (a cancel may race a fill)."""
    return any(int(l["filled_qty"] or 0) > 0
               or l["state"] in ("placed", "partially_filled", "cancel_requested", "filled", "unknown")
               or (l["state"] == "pending" and l.get("client_order_id"))       # crashed between mint and record
               for l in store.get(iid)["legs"])


def _finish_stopped(iid: str, reason: str, *, cancelled: bool = False) -> None:
    possible = _any_fill(iid)                                # before pending legs are relabelled
    rec = store.get(iid)
    for l in rec["legs"]:
        if l["state"] == "pending" and l.get("client_order_id"):
            store.update_leg(l["id"], state="unknown", error="dispatch stopped after the order id was minted")
    _mark_remaining(store.get(iid), "not_sent")
    if possible:
        store.set_state(iid, "attention_required", reason + " - filled legs stay open; review with the operator")
    else:
        store.set_state(iid, "cancelled" if cancelled else "failed", reason)


async def _submit_live(broker, order):
    """ONE live placement - never the adapter's retrying place_order (a retried timeout can double an order).

    On any exception the broker orderbook is queried for OUR tag: found -> adopt it; definitively absent -> FAILED
    (safe, nothing reached the broker); the lookup itself fails -> UNKNOWN (treated as a possible fill)."""
    from ..broker.base import OrderResult
    once = getattr(broker, "place_order_once", None)
    if once is not None:                                    # an adapter that provides its own single-shot path
        return await once(order)
    if not broker._live_allowed():
        return OrderResult(status="DRY_RUN", broker_order_id=None, symbol=order.symbol, qty=order.qty)
    block = broker._token_abort_reason() or broker._preflight_block_reason()
    if block:
        return OrderResult(status="FAILED", broker_order_id=None, symbol=order.symbol, qty=order.qty, error=block)
    try:
        kite = broker.kite
        params = order.to_kite_params(kite)
        oid = await asyncio.to_thread(lambda: kite.place_order(**params))
        return OrderResult(status="PLACED", broker_order_id=str(oid), symbol=order.symbol, qty=order.qty)
    except Exception as e:
        err = f"{type(e).__name__}: {e}"[:200]
    try:
        found = await asyncio.to_thread(broker.find_recent_order, order)
    except Exception as e2:
        return OrderResult(status="UNKNOWN", broker_order_id=None, symbol=order.symbol, qty=order.qty,
                           error=f"{err}; orderbook lookup failed ({type(e2).__name__})")
    if found and found.get("order_id"):
        return OrderResult(status="PLACED", broker_order_id=str(found["order_id"]), symbol=order.symbol,
                           qty=order.qty, raw={"adopted": True})
    return OrderResult(status="FAILED", broker_order_id=None, symbol=order.symbol, qty=order.qty,
                       error=f"{err}; confirmed absent from the orderbook")


async def _place(broker, iid: str, leg: Dict[str, Any], live: bool) -> None:
    from ..execution.orders import Order
    from ..order_ledger import (EV_ORDER_SUBMITTED, append_event, compact_tag, make_client_order_id, record_intent)
    sess = f"intent:{iid}"
    coid = make_client_order_id(f"si{iid[:8]}", leg["tradingsymbol"])
    record_intent(session_id=sess, symbol=leg["tradingsymbol"], client_order_id=coid, qty=leg["quantity"],
                  side=leg["side"], product=leg["product"], broker_profile="strategy-intents",
                  instrument_type=leg["option_type"], source="strategy_intent")
    store.update_leg(leg["id"], client_order_id=coid)
    order = Order(symbol=leg["tradingsymbol"], qty=int(leg["quantity"]), exchange="NFO", product=leg["product"],
                  order_type="LIMIT", instrument_type=leg["option_type"], transaction_type=leg["side"],
                  price=float(leg["limit_price"]), lot_size=int(leg["lot_size"]), strike=float(leg["strike"]),
                  client_order_id=coid, tag=compact_tag(coid))
    try:
        res = await (_submit_live(broker, order) if live else broker.place_order(order))
    except Exception as e:                                   # the outcome is not known: never assume "not sent"
        store.update_leg(leg["id"], state="unknown", error=f"{type(e).__name__}: {e}"[:300])
        return
    if res.status == "DRY_RUN":
        store.update_leg(leg["id"], state="dry_run")
    elif res.status == "PLACED" and res.broker_order_id:
        store.update_leg(leg["id"], state="placed", broker_order_id=str(res.broker_order_id))
        append_event(session_id=sess, symbol=leg["tradingsymbol"], event_type=EV_ORDER_SUBMITTED,
                     position_ref=f"{sess}:{leg['tradingsymbol']}", product=leg["product"],
                     broker_profile="strategy-intents", broker_order_id=res.broker_order_id,
                     client_order_id=coid, qty=leg["quantity"], price=leg["limit_price"],
                     source="strategy_intent", detail=f"side={leg['side']}")
    elif res.status == "UNKNOWN":
        store.update_leg(leg["id"], state="unknown", error=(res.error or "outcome unknown")[:300])
    else:
        store.update_leg(leg["id"], state="failed", error=(res.error or res.status or "not placed")[:300])


async def _poll_group(broker, iid: str, group: int, timeout_s: float, poll_s: float) -> bool:
    """Wait until every placed leg of the group is COMPLETE with its full quantity. False on any rejection,
    cancel request or timeout - resting orders are then cancelled."""
    from ..order_ledger import EV_FILLED, EV_REJECTED, append_event
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        rec = store.get(iid)
        legs = [l for l in rec["legs"] if l["group"] == group]
        for l in legs:
            if l["state"] not in ("placed", "partially_filled"):
                continue
            try:
                st = await asyncio.to_thread(broker.get_order_status, l["broker_order_id"]) or {}
            except Exception:
                continue                                                   # transient; re-poll until the deadline
            status = str(st.get("status") or "").upper()
            filled = int(st.get("filled_quantity") or 0)
            avg = st.get("average_price")
            if status == "COMPLETE" and filled >= int(l["quantity"]):
                store.update_leg(l["id"], state="filled", filled_qty=filled, avg_price=avg)
                append_event(session_id=f"intent:{iid}", symbol=l["tradingsymbol"], event_type=EV_FILLED,
                             broker_order_id=l["broker_order_id"], client_order_id=l["client_order_id"],
                             qty=filled, price=avg, source="strategy_intent")
            elif status in ("REJECTED", "CANCELLED") or (status == "COMPLETE" and filled < int(l["quantity"])):
                store.update_leg(l["id"], state="rejected" if status == "REJECTED" else "cancelled",
                                 filled_qty=filled, avg_price=avg, error=(st.get("status_message") or status)[:300])
                append_event(session_id=f"intent:{iid}", symbol=l["tradingsymbol"], event_type=EV_REJECTED,
                             broker_order_id=l["broker_order_id"], client_order_id=l["client_order_id"],
                             qty=filled, source="strategy_intent", detail=status)
            elif filled > 0:
                store.update_leg(l["id"], state="partially_filled", filled_qty=filled, avg_price=avg)
        legs = [l for l in store.get(iid)["legs"] if l["group"] == group]
        if all(l["state"] == "filled" for l in legs):
            return True
        stop = (any(l["state"] in ("rejected", "cancelled", "failed") for l in legs)
                or store.get(iid)["cancel_requested"] or loop.time() >= deadline)
        if stop:
            for l in legs:
                if l["state"] in ("placed", "partially_filled"):
                    try:
                        await asyncio.to_thread(broker.cancel_order_sync, l["broker_order_id"])
                    except Exception:
                        pass
                    store.update_leg(l["id"], state="cancel_requested")
            return False
        await asyncio.sleep(poll_s)


async def run(iid: str, *, broker_factory: Optional[Callable] = None, group_timeout_s: float = GROUP_TIMEOUT_S,
              poll_s: float = POLL_S) -> Dict[str, Any]:
    """Dispatch one accepted basket. Safe to call twice: only the caller that moves it to 'dispatching' runs."""
    if not store.set_state(iid, "dispatching", None, only_from={"accepted"}):
        return store.get(iid)
    rec = store.get(iid)
    live = rec["mode"] == "live"
    try:
        broker = (broker_factory or default_broker_factory)(rec, live)
        if live:
            bad = _recheck(rec)
            if bad:
                _finish_stopped(iid, "LIVE_GATES_FAILED: " + bad)
                return store.get(iid)
            ok, detail = await margin_check(broker, rec["legs"])
            if not ok:
                _finish_stopped(iid, detail)
                return store.get(iid)
        for gi, group in enumerate(sorted({l["group"] for l in rec["legs"]})):
            rec = store.get(iid)
            if rec["cancel_requested"]:
                _finish_stopped(iid, "Cancelled by request", cancelled=True)
                return store.get(iid)
            if live and gi:
                bad = _recheck(rec)
                if bad:
                    _finish_stopped(iid, f"LIVE_GATES_FAILED before group {group}: {bad}")
                    return store.get(iid)
            for leg in [l for l in rec["legs"] if l["group"] == group]:
                await _place(broker, iid, leg, live)
            legs = [l for l in store.get(iid)["legs"] if l["group"] == group]
            if any(l["state"] in ("failed", "unknown") for l in legs):
                for l in legs:
                    if l["state"] == "placed":
                        try:
                            await asyncio.to_thread(broker.cancel_order_sync, l["broker_order_id"])
                        except Exception:
                            pass
                        store.update_leg(l["id"], state="cancel_requested")
                _finish_stopped(iid, f"Group {group}: a leg was not placed")
                return store.get(iid)
            if all(l["state"] == "dry_run" for l in legs):
                continue
            if any(l["state"] == "dry_run" for l in legs):                 # mixed = adapter mode changed under us
                _finish_stopped(iid, f"Group {group}: adapter returned a mix of dry-run and live results")
                return store.get(iid)
            if not await _poll_group(broker, iid, group, group_timeout_s, poll_s):
                _finish_stopped(iid, f"Group {group} did not fully fill; later groups were not sent",
                                cancelled=bool(store.get(iid)["cancel_requested"]))
                return store.get(iid)
        final = store.get(iid)
        dry = all(l["state"] == "dry_run" for l in final["legs"])
        store.set_state(iid, "dry_run_complete" if dry else "completed",
                        "Dry run - no broker order was placed" if dry else "All groups filled")
    except Exception as e:  # never leave a basket in 'dispatching'
        log.exception("strategy intent %s dispatch crashed", iid)
        _finish_stopped(iid, f"DISPATCH_ERROR: {type(e).__name__}: {e}"[:300])
    return store.get(iid)


def sweep_stale(max_idle_minutes: int = 10) -> int:
    """Startup safety: a basket left 'dispatching' by a crash/restart (no leg update for max_idle_minutes) goes to
    'attention_required' - its legs may be resting at the broker and only the operator can reconcile them."""
    from datetime import timedelta
    n = 0
    cutoff = store._iso(store.now_ist() - timedelta(minutes=max_idle_minutes))
    for rec in store.list_for(None, None, 200):
        if rec["state"] != "dispatching":
            continue
        last = max([rec["updated_at"]] + [l["updated_at"] for l in rec["legs"]])
        if last < cutoff:
            _finish_stopped(rec["id"], "DISPATCH_INTERRUPTED: the process stopped mid-basket")
            n += 1
    return n
