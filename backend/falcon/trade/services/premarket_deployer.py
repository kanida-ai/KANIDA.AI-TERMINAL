"""Pre-market deployer — fires QUEUED staging items at 9:15 IST next trading day.

Lifecycle:
    Boot → daemon thread starts
    9:14 IST weekday → thread wakes from long sleep, polls every 30s
    9:15 IST → market opens; for each QUEUED item:
        NEW_ENTRY  → re-plan with morning prices + place_orders
        BULK_ADOPT → call bulk-adopt path (existing-Kite-SL or fresh-place)
    Mark DEPLOYED / FAILED per item, emit events.
    9:30 IST → window closes; thread sleeps until 9:14 next weekday.

Safety guards (defense-in-depth on top of order_executor's own checks):
  * Kite token preflight per cycle — abort if invalid (operator must refresh).
  * FALCON_AUTOTRADE_ENABLED env var must be 'true'.
  * Market-hours strict check (mirrors trail_manager).
  * Per-item try/except — one bad item never aborts the whole batch.

Design note: this thread is the BACKEND counterpart to /falcon/premarket UI.
The user clicks Confirm & Schedule (UI) → items flip STAGED → QUEUED.
This thread sees QUEUED + market-open → fires them. No other path.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from . import (
    holdings_fetch,
    margin_calc,
    mtf_eligibility,
    order_planner,
    premarket_staging,
    trade_db,
    trail_config,
)
from .order_executor import _autotrade_enabled, execute_batch
from .order_planner import plan_orders
from .services_round import round_to_tick_size
from .volatility_gate import sl_order_type

log = logging.getLogger("kanida.falcon.trade.deployer")
IST = timezone(timedelta(hours=5, minutes=30))

POLL_INTERVAL_SEC = 30                  # how often to check during the deploy window
DEPLOY_WINDOW_START = (9, 14)           # start polling from 9:14 IST
DEPLOY_WINDOW_END   = (9, 30)           # stop polling after 9:30 IST

_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()
_state: Dict[str, Any] = {
    "started":          False,
    "started_at":       None,
    "last_cycle_at":    None,
    "last_deploy_at":   None,
    "n_deployed":       0,
    "n_failed":         0,
    "last_error":       None,
}


# ── Public API ───────────────────────────────────────────────────────────────

def start() -> bool:
    """Launch the background thread once. Idempotent."""
    global _thread
    if _thread is not None and _thread.is_alive():
        return True
    _stop_event.clear()
    _thread = threading.Thread(target=_run, name="premarket-deployer", daemon=True)
    _thread.start()
    _state["started"] = True
    _state["started_at"] = datetime.now(IST).isoformat()
    log.info("Pre-market deployer thread launched")
    return True


def stop() -> None:
    _stop_event.set()


def status() -> Dict[str, Any]:
    return dict(_state)


def deploy_once_now() -> Dict[str, Any]:
    """Force a single deploy cycle right now (bypasses time window).
    Useful for manual operator override or testing. Returns the cycle result."""
    return _run_cycle(force=True)


# ── Thread loop ──────────────────────────────────────────────────────────────

def _run() -> None:
    while not _stop_event.is_set():
        try:
            now = datetime.now(IST)
            if _in_deploy_window(now) and now.weekday() < 5:
                _run_cycle()
                _stop_event.wait(POLL_INTERVAL_SEC)
            else:
                # Sleep until next weekday's 9:14 IST.
                next_window = _next_window_start(now)
                wait_sec = max(60.0, (next_window - now).total_seconds())
                log.info("Pre-market deployer: next window at %s IST (%.0f min)",
                          next_window.strftime("%Y-%m-%d %H:%M"), wait_sec / 60)
                _stop_event.wait(wait_sec)
        except Exception as e:
            _state["last_error"] = str(e)
            log.exception("Pre-market deployer loop crashed (will retry in 60s): %s", e)
            _stop_event.wait(60)


def _in_deploy_window(now: datetime) -> bool:
    sh, sm = DEPLOY_WINDOW_START
    eh, em = DEPLOY_WINDOW_END
    after_start = (now.hour, now.minute) >= (sh, sm)
    before_end  = (now.hour, now.minute) <  (eh, em)
    return after_start and before_end


def _next_window_start(now: datetime) -> datetime:
    sh, sm = DEPLOY_WINDOW_START
    nxt = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    if nxt <= now:
        nxt += timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt


def _market_open(now: Optional[datetime] = None) -> bool:
    n = now or datetime.now(IST)
    if n.weekday() >= 5:
        return False
    if (n.hour, n.minute) < (9, 15):
        return False
    if (n.hour, n.minute) >= (15, 30):
        return False
    return True


# ── Single cycle ─────────────────────────────────────────────────────────────

def _run_cycle(force: bool = False) -> Dict[str, Any]:
    """One pass: pull QUEUED items, fire what's eligible, mark each.

    Date scoping:
      * Normal cron path (force=False): only items with target_date == today.
        The auto-deployer is the 9:15 IST window for "items planned for today".
      * Manual override (force=True): ALL pending items regardless of target_date.
        Operator's "Deploy now (force)" really means "fire everything QUEUED right
        now" — including items staged post-9:15 IST for tomorrow's window (the
        common Positions-page bulk-adopt flow during market hours).
    """
    _state["last_cycle_at"] = datetime.now(IST).isoformat()
    today_iso = datetime.now(IST).date().isoformat()

    # Force mode: ignore target_date filter. Normal mode: today-only.
    items = premarket_staging.list_pending(target_date=None if force else today_iso)
    queued = [i for i in items if i.get("status") == "QUEUED"]
    if not queued:
        if force:
            log.info("Pre-market force-deploy: no QUEUED items across any target_date "
                     "(nothing to fire). today=%s", today_iso)
        return {"target_date": today_iso, "n_queued": 0, "n_deployed": 0, "n_failed": 0,
                "force": force}

    # Pre-flight gates
    if not _autotrade_enabled():
        msg = "FALCON_AUTOTRADE_ENABLED env var not set to 'true' — refusing to deploy"
        log.warning("Pre-market deployer: %s (have %d queued items)", msg, len(queued))
        return {"target_date": today_iso, "n_queued": len(queued), "n_deployed": 0,
                "n_failed": 0, "skipped": msg}

    # Structural preflight: refuses to deploy if any RED invariant is broken
    # (bad token, IP allowlist, stale OHLC, missing entry_date, etc.). See
    # backend/falcon/preflight.py for the 12 invariants checked.
    try:
        from .. import preflight as _pf  # noqa: WPS433
        pf = _pf.run(force=True)         # force=True: never trust cache on hot path
        if not pf.ok:
            red_names = [c.name for c in pf.checks if c.status == _pf.RED]
            red_details = [f"{c.name}: {c.detail}" for c in pf.checks if c.status == _pf.RED]
            msg = "PREFLIGHT_BLOCKED: " + "; ".join(red_details)
            log.error("Pre-market deployer: preflight BLOCKED — refusing %d items. red=%s",
                      len(queued), red_names)
            _state["last_error"] = msg[:500]
            # Don't flip items to FAILED — they'll retry next cycle once operator fixes preflight
            return {"target_date": today_iso, "n_queued": len(queued), "n_deployed": 0,
                    "n_failed": 0, "skipped": "PREFLIGHT_BLOCKED",
                    "preflight_red": red_names}
    except Exception as e:
        log.exception("preflight raised — proceeding without gate (degraded mode): %s", e)

    if not force and not _market_open():
        # Still inside 9:14 wake-up window but before 9:15 open; no-op.
        return {"target_date": today_iso, "n_queued": len(queued), "n_deployed": 0,
                "n_failed": 0, "skipped": "MARKET_NOT_OPEN_YET"}

    kite = _get_kite()
    if kite is None:
        # Token invalid. Mark nothing failed (operator must refresh) and emit critical event.
        log.error("Pre-market deployer: Kite token invalid; %d items will retry next cycle", len(queued))
        _state["last_error"] = "KITE_TOKEN_INVALID"
        return {"target_date": today_iso, "n_queued": len(queued), "n_deployed": 0,
                "n_failed": 0, "skipped": "KITE_TOKEN_INVALID"}

    n_deployed = 0
    n_failed   = 0
    results: List[Dict[str, Any]] = []

    # Run NEW_ENTRY items as batches, grouped by target_date. _deploy_new_entries_batch
    # loads falcon_signals_live for items[0]["target_date"], so a mixed-date batch would
    # silently drop the off-date items. In force-deploy mode the queue can contain items
    # spanning multiple dates (e.g. today's leftovers + tomorrow's staged), so split first.
    new_entries = [i for i in queued if i["kind"] == "NEW_ENTRY"]
    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for it in new_entries:
        by_date.setdefault(it["target_date"], []).append(it)
    for td, group in sorted(by_date.items()):
        try:
            r = _deploy_new_entries_batch(kite, group)
            n_deployed += r.get("n_deployed", 0)
            n_failed   += r.get("n_failed", 0)
            results.append({"kind": "NEW_ENTRY", "target_date": td, **r})
        except Exception as e:
            log.exception("Pre-market: NEW_ENTRY batch crashed (target_date=%s)", td)
            for it in group:
                premarket_staging.mark_failed(it["id"], f"BATCH_CRASH: {e}")
                n_failed += 1

    # Run BULK_ADOPT items individually (each maps to an existing position).
    adopt_items = [i for i in queued if i["kind"] == "BULK_ADOPT"]
    for it in adopt_items:
        try:
            r = _deploy_one_adopt(kite, it)
            if r.get("ok"):
                premarket_staging.mark_deployed(it["id"], r)
                n_deployed += 1
            else:
                premarket_staging.mark_failed(it["id"], r.get("error", "unknown"))
                n_failed += 1
        except Exception as e:
            log.exception("Pre-market: BULK_ADOPT %s crashed", it.get("symbol"))
            premarket_staging.mark_failed(it["id"], f"CRASH: {e}")
            n_failed += 1

    _state["n_deployed"] += n_deployed
    _state["n_failed"]   += n_failed
    if n_deployed > 0:
        _state["last_deploy_at"] = datetime.now(IST).isoformat()

    target_dates_seen = sorted({i["target_date"] for i in queued})
    log.info("Pre-market cycle%s: %d queued → %d deployed, %d failed (target_dates=%s)",
              " [FORCE]" if force else "", len(queued), n_deployed, n_failed,
              ",".join(target_dates_seen) or "—")
    return {
        "target_date":   today_iso,
        "target_dates":  target_dates_seen,
        "force":         force,
        "n_queued":      len(queued),
        "n_deployed":    n_deployed,
        "n_failed":      n_failed,
        "results":       results,
    }


# ── Per-kind deploy helpers ──────────────────────────────────────────────────

def _deploy_new_entries_batch(kite, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Re-plan with morning prices, place all in one Kite batch.

    The signals' close_at_signal is yesterday's EOD; we re-fetch from
    falcon_signals_live (same data, but plan_orders also calls margin/MTF
    checks that need a live Kite client). All items in a cycle share the
    same per_trade/sl_pct/etc. (we use the first item's config).
    """
    # All items share config (set in EOD orchestrator)
    cfg = items[0]["payload"].get("config", {})
    per_trade           = int(cfg.get("per_trade") or 50_000)
    sl_pct              = float(cfg.get("sl_pct") or -7.0)
    trail_trigger_pct   = float(cfg.get("trail_trigger_pct") or 12.0)

    # Re-load signals from DB for accuracy (same data the EOD wrote)
    target_date = items[0]["target_date"]
    from ...db import falcon_conn
    with falcon_conn() as con:
        sig_rows = con.execute("""
            SELECT * FROM falcon_signals_live WHERE entry_date = ? ORDER BY rank
        """, (target_date,)).fetchall()
    signals = [dict(r) for r in sig_rows]
    if not signals:
        for it in items:
            premarket_staging.mark_failed(it["id"], "NO_SIGNALS_FOR_DATE")
        return {"n_deployed": 0, "n_failed": len(items), "error": "NO_SIGNALS"}

    selected = [it["symbol"] for it in items]
    held = holdings_fetch.get_held_positions(kite, force_refresh=True)
    margin_fn = lambda batch: margin_calc.fetch_margins_batch(kite, batch)
    sl_fn     = lambda sym: sl_order_type(14.0, gap_pct=None)
    mtf_fn    = lambda sym: mtf_eligibility.is_mtf_eligible(kite, sym)
    tick_fn   = lambda sym: mtf_eligibility.get_tick_size(kite, sym)

    orders, skipped, action_counts = plan_orders(
        signals          = signals,
        selected_symbols = selected,
        per_trade        = per_trade,
        sl_pct           = sl_pct,
        trail_trigger_pct= trail_trigger_pct,
        held_by_symbol   = held,
        hold_actions     = {},      # at deploy time, default = no override
        mtf_eligible_fn  = mtf_fn,
        sl_order_type_fn = sl_fn,
        margin_fn        = margin_fn,
        tick_size_fn     = tick_fn,
        default_product  = "MTF",
    )

    if not orders:
        for it in items:
            sym = it["symbol"]
            reason = next((s["reason"] for s in skipped if s.get("symbol") == sym), "NO_ORDER_PLANNED")
            premarket_staging.mark_failed(it["id"], reason)
        return {"n_deployed": 0, "n_failed": len(items), "skipped": skipped}

    # Place orders (creates batch_id, inserts audit rows)
    batch_id = _new_batch_id()
    trade_db.create_run(
        batch_id            = batch_id,
        signal_date         = signals[0].get("signal_date") or target_date,
        entry_date          = target_date,
        selected_symbols    = selected,
        total_capital       = int(cfg.get("total_capital") or 30_00_000),
        per_trade           = per_trade,
        use_leverage        = True,
        hold_days           = int(cfg.get("hold_days") or 7),
        sl_pct              = sl_pct,
        trail_trigger_pct   = trail_trigger_pct,
        trail_lookback_days = int(cfg.get("trail_lookback_days") or 10),
        started_at          = datetime.now(IST).isoformat(),
    )
    placed_orders = execute_batch(kite, batch_id, orders)

    # Map back to staging items: any symbol whose ENTRY succeeded → DEPLOYED
    placed_syms = {o.symbol for o in orders if any(
        po.get("symbol") == o.symbol and po.get("role") == "ENTRY"
            and po.get("status") in ("PLACED", "OPEN", "COMPLETE")
        for po in placed_orders.get("orders", [])
    )}
    n_deployed = 0
    n_failed   = 0
    for it in items:
        sym = it["symbol"]
        if sym in placed_syms:
            premarket_staging.mark_deployed(it["id"], {"batch_id": batch_id, "symbol": sym, "status": "PLACED"})
            n_deployed += 1
        else:
            # Could be skipped (NOT_IN_SIGNALS, MTF_INELIGIBLE) or failed at Kite
            err_row = next((po for po in placed_orders.get("orders", [])
                            if po.get("symbol") == sym and po.get("status") not in ("PLACED", "OPEN")), None)
            err = (err_row or {}).get("error") or next(
                (s["reason"] for s in skipped if s.get("symbol") == sym),
                "NOT_PLACED"
            )
            premarket_staging.mark_failed(it["id"], str(err))
            n_failed += 1

    return {"n_deployed": n_deployed, "n_failed": n_failed, "batch_id": batch_id,
            "orders": [o.to_dict() for o in orders], "execute_result": placed_orders}


def _deploy_one_adopt(kite, item: Dict[str, Any]) -> Dict[str, Any]:
    """Adopt one external position. Mirrors /positions/bulk-adopt logic for a single item.

    Path A: live Kite SL already exists → record DB row, no new Kite order.
    Path B: no live SL → place SL with the staged price.
    """
    payload = item["payload"]
    sym     = payload["symbol"]
    qty     = int(payload["qty"])
    product = payload.get("product", "CNC")
    sl_price_staged = float(payload["sl_price"])

    # Check Kite-side first — adopt existing if found
    try:
        all_orders = kite.orders()
    except Exception as e:
        return {"ok": False, "error": f"kite.orders failed: {e}"}

    existing = None
    for o in all_orders:
        if o.get("tradingsymbol") != sym: continue
        if o.get("transaction_type") != "SELL": continue
        if o.get("order_type") not in ("SL", "SL-M"): continue
        if o.get("status") not in ("TRIGGER PENDING", "OPEN"): continue
        existing = o
        break

    from .order_executor import idempotency_key
    batch_id = _new_batch_id(suffix="_pma")
    tag = idempotency_key(batch_id, sym, "STOP")

    if existing:
        try:
            trade_db.insert_order(
                batch_id        = batch_id,
                symbol          = sym,
                side            = "SELL",
                role            = "STOP",
                qty             = qty,
                idempotency_key = tag,
                kite_order_id   = str(existing.get("order_id")),
                price           = float(existing.get("price") or sl_price_staged),
                trigger_price   = float(existing.get("trigger_price") or sl_price_staged),
                product         = product,
                order_type      = existing.get("order_type") or "SL",
                status          = "PLACED",
            )
            return {"ok": True, "path": "ADOPTED_EXISTING_KITE_SL",
                    "kite_order_id": str(existing.get("order_id"))}
        except Exception as e:
            return {"ok": False, "error": f"DB insert failed: {e}"}

    # No live SL — place fresh
    try:
        from .holdings_fetch import get_circuit_limits
        tick = mtf_eligibility.get_tick_size(kite, sym)
        circuit = get_circuit_limits(kite, [sym]).get(sym, {})
        lower_circuit = circuit.get("lower", 0)
        raw_sl = sl_price_staged
        if lower_circuit > 0 and raw_sl <= lower_circuit:
            raw_sl = lower_circuit + tick
        sl_price = round_to_tick_size(raw_sl, tick)
        sl_limit = round_to_tick_size(max(sl_price * 0.998, (lower_circuit + tick) if lower_circuit > 0 else 0), tick)
        if sl_limit > sl_price:
            sl_limit = sl_price

        kite_id = kite.place_order(
            variety           = kite.VARIETY_REGULAR,
            exchange          = kite.EXCHANGE_NSE,
            tradingsymbol     = sym,
            transaction_type  = kite.TRANSACTION_TYPE_SELL,
            quantity          = qty,
            product           = "MTF" if product == "MTF" else kite.PRODUCT_CNC,
            order_type        = kite.ORDER_TYPE_SL,
            price             = sl_limit,
            trigger_price     = sl_price,
            tag               = tag,
        )
        trade_db.insert_order(
            batch_id        = batch_id,
            symbol          = sym,
            side            = "SELL",
            role            = "STOP",
            qty             = qty,
            idempotency_key = tag,
            kite_order_id   = str(kite_id),
            price           = sl_limit,
            trigger_price   = sl_price,
            product         = product,
            order_type      = "SL-L",
            status          = "PLACED",
        )
        return {"ok": True, "path": "PLACED_FRESH", "kite_order_id": str(kite_id),
                "sl_price": sl_price, "sl_limit": sl_limit}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _new_batch_id(suffix: str = "") -> str:
    import secrets
    today = date.today().isoformat().replace("-", "_")
    return f"btc_{today}_{secrets.token_hex(3)}{suffix}"


def _get_kite():
    try:
        from services.kite_auth import get_kite_client  # noqa: WPS433
        return get_kite_client(check=True)
    except Exception as e:
        log.warning("Pre-market deployer: cannot get Kite client: %s", e)
        return None
