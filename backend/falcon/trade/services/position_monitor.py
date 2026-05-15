"""Phase 2 — live position monitor.

Background polling loop (60s default) that watches every held position for:
  - NEAR_SL          (current within 3% above SL)
  - NEAR_TARGET      (current within 3% below trail-trigger)
  - HW_REACHED       (current >= entry × 1 + trail_trigger_pct/100, first time)
  - BREACHED_SL      (current <= sl_price)
  - TARGET_HIT       (current >= target_price)
  - TIME_STOP_DUE    (days_held >= hold_days_max)

Phase 2.A: events are LOGGED but NO auto-actions. Operator sees feed + manual exit.
Phase 2.B: auto-actions enabled when FALCON_AUTO_EXIT_ENABLED='true' AND position is
           managed_by='falcon' (external positions are NEVER auto-acted on).
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ...db import falcon_conn
from . import holdings_fetch, trade_db, trail_config

log = logging.getLogger("kanida.falcon.trade.monitor")
IST = timezone(timedelta(hours=5, minutes=30))

POLL_INTERVAL_SEC      = int(os.environ.get("FALCON_MONITOR_INTERVAL_SEC", "60"))
NEAR_THRESHOLD_PCT     = 3.0
SL_PCT                 = -7.0      # initial SL = entry × 0.93
TRAIL_TRIGGER_PCT      = 10.0      # HW = entry × 1.10
HOLD_DAYS_MAX          = 7

_started = False
_lock    = threading.Lock()


def _now_iso() -> str:
    return datetime.now(IST).isoformat()


def _auto_exit_enabled() -> bool:
    return os.environ.get("FALCON_AUTO_EXIT_ENABLED", "").lower() == "true"


# ─── State CRUD ──────────────────────────────────────────────────────────────

def _record_first_seen(symbol: str, qty: int, detected_via: str) -> str:
    """Record the first-seen date for a symbol (idempotent). Returns ISO date.
    IST-aware — first-seen is recorded against trading-calendar date, not local."""
    today_iso = datetime.now(IST).date().isoformat()
    with falcon_conn() as con:
        existing = con.execute("SELECT first_seen_date FROM falcon_position_first_seen WHERE symbol=?",
                                (symbol,)).fetchone()
        if existing:
            return existing["first_seen_date"]
        con.execute("""
            INSERT INTO falcon_position_first_seen
              (symbol, first_seen_date, first_seen_at, detected_via, qty_at_first_seen)
            VALUES (?,?,?,?,?)
        """, (symbol, today_iso, _now_iso(), detected_via, qty))
        con.commit()
    return today_iso


def _resolve_entry_date(symbol: str, qty: int, falcon_entry_date: Optional[str],
                        is_t1_only: bool) -> str:
    """Determine the best entry_date for a symbol.
    Priority: Falcon's own records > Kite t1_quantity (bought today) > first-seen anchor.
    """
    if falcon_entry_date:
        # Most accurate — Falcon placed this entry, knows the exact date
        return falcon_entry_date
    if is_t1_only:
        # All current quantity is in T+1 settlement → bought today
        return _record_first_seen(symbol, qty, "kite_t1")
    # Older position; use first-seen anchor
    return _record_first_seen(symbol, qty, "first_poll")


def upsert_state(symbol: str, *, managed_by: str, product: Optional[str], qty: int,
                 avg_entry: float, entry_date: Optional[str], current_price: float,
                 sl_price_from_falcon: Optional[float] = None,
                 sl_kite_order_id: Optional[str] = None,
                 is_t1_only: bool = False) -> Dict[str, Any]:
    """Insert or refresh state row. Returns the loaded state.
    Auto-resolves entry_date if not provided (Kite t1 hint or first-seen anchor)."""
    initial_sl = sl_price_from_falcon if sl_price_from_falcon else round(avg_entry * (1 + SL_PCT / 100), 2)
    target     = round(avg_entry * (1 + TRAIL_TRIGGER_PCT / 100), 2)
    resolved_entry = _resolve_entry_date(symbol, qty, entry_date, is_t1_only)
    with falcon_conn() as con:
        existing = con.execute("SELECT * FROM falcon_position_state WHERE symbol=?", (symbol,)).fetchone()
        if existing:
            new_hw = max(existing["high_water_price"] or 0, current_price)
            con.execute("""
                UPDATE falcon_position_state
                SET qty=?, last_seen_price=?, high_water_price=?, last_polled_at=?,
                    managed_by=?, product=?, avg_entry=?, target_price=?,
                    entry_date=COALESCE(entry_date, ?),
                    sl_kite_order_id=COALESCE(?, sl_kite_order_id)
                WHERE symbol=?
            """, (qty, current_price, new_hw, _now_iso(),
                  managed_by, product, avg_entry, target, resolved_entry,
                  sl_kite_order_id, symbol))
            con.commit()
        else:
            con.execute("""
                INSERT INTO falcon_position_state
                  (symbol, managed_by, product, qty, avg_entry, initial_sl_price, current_sl_price,
                   target_price, high_water_price, hw_reached, trail_active,
                   entry_date, hold_days_max, last_seen_price, last_polled_at, sl_kite_order_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (symbol, managed_by, product, qty, avg_entry, initial_sl, initial_sl,
                  target, current_price, 0, 0,
                  resolved_entry, HOLD_DAYS_MAX, current_price, _now_iso(), sl_kite_order_id))
            con.commit()
        row = con.execute("SELECT * FROM falcon_position_state WHERE symbol=?", (symbol,)).fetchone()
    return dict(row)


def log_event(symbol: str, kind: str, severity: str, detail: str = "",
              auto_action_taken: bool = False, related_kite_id: Optional[str] = None) -> None:
    with falcon_conn() as con:
        con.execute("""
            INSERT INTO falcon_trade_events
              (detected_at, symbol, kind, severity, detail, auto_action_taken, related_kite_id)
            VALUES (?,?,?,?,?,?,?)
        """, (_now_iso(), symbol, kind, severity, detail,
              1 if auto_action_taken else 0, related_kite_id))
        con.commit()
        # Also stamp on state row
        con.execute("UPDATE falcon_position_state SET last_event_kind=?, last_event_at=? WHERE symbol=?",
                    (kind, _now_iso(), symbol))
        con.commit()


def list_events(limit: int = 50, since: Optional[str] = None) -> List[Dict[str, Any]]:
    with falcon_conn() as con:
        if since:
            rows = con.execute("""
                SELECT * FROM falcon_trade_events
                WHERE detected_at > ?
                ORDER BY id DESC LIMIT ?
            """, (since, limit)).fetchall()
        else:
            rows = con.execute("""
                SELECT * FROM falcon_trade_events
                ORDER BY id DESC LIMIT ?
            """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def list_states() -> List[Dict[str, Any]]:
    with falcon_conn() as con:
        rows = con.execute("SELECT * FROM falcon_position_state").fetchall()
    return [dict(r) for r in rows]


def get_state(symbol: str) -> Optional[Dict[str, Any]]:
    with falcon_conn() as con:
        row = con.execute("SELECT * FROM falcon_position_state WHERE symbol=?", (symbol,)).fetchone()
    return dict(row) if row else None


# ─── Trigger detection ───────────────────────────────────────────────────────

def detect_triggers(state: Dict[str, Any]) -> List[Dict[str, str]]:
    """Pure function: given a state row, returns list of triggers fired in this poll.
    No DB writes here; the caller logs them."""
    out: List[Dict[str, str]] = []
    cur = state.get("last_seen_price") or 0
    if cur <= 0:
        return out

    sl = state.get("current_sl_price") or 0
    tgt = state.get("target_price") or 0
    avg = state.get("avg_entry") or 0
    hw = state.get("high_water_price") or 0
    hw_already_reached = bool(state.get("hw_reached"))
    entry_date = state.get("entry_date")
    days_held = 0
    if entry_date:
        try:
            ed = datetime.fromisoformat(entry_date).date()
            days_held = (datetime.now(IST).date() - ed).days  # IST-aware
        except Exception:
            pass
    hold_max = state.get("hold_days_max") or HOLD_DAYS_MAX

    # 1. Hard breach
    if sl > 0 and cur <= sl:
        out.append({"kind": "BREACHED_SL", "severity": "critical",
                    "detail": f"current ₹{cur:.2f} ≤ SL ₹{sl:.2f}"})
    # 2. Hard target
    if tgt > 0 and cur >= tgt and not hw_already_reached:
        out.append({"kind": "TARGET_HIT", "severity": "info",
                    "detail": f"current ₹{cur:.2f} ≥ target ₹{tgt:.2f}"})
    # 3. HW reached (first time)
    if avg > 0 and hw >= avg * (1 + TRAIL_TRIGGER_PCT/100) and not hw_already_reached:
        out.append({"kind": "HW_REACHED", "severity": "info",
                    "detail": f"high-water ₹{hw:.2f} ≥ entry × {1 + TRAIL_TRIGGER_PCT/100} (₹{avg*(1+TRAIL_TRIGGER_PCT/100):.2f})"})
    # 4. NEAR_SL (within 3% above)
    if sl > 0 and cur > sl:
        dist_pct = ((cur - sl) / cur) * 100
        if dist_pct < NEAR_THRESHOLD_PCT:
            out.append({"kind": "NEAR_SL", "severity": "warn",
                        "detail": f"distance to SL: {dist_pct:.2f}%"})
    # 5. NEAR_TARGET
    if tgt > 0 and cur < tgt:
        dist_pct = ((tgt - cur) / cur) * 100
        if dist_pct < NEAR_THRESHOLD_PCT:
            out.append({"kind": "NEAR_TARGET", "severity": "info",
                        "detail": f"distance to target: {dist_pct:.2f}%"})
    # 6. Time-stop due
    if days_held >= hold_max:
        out.append({"kind": "TIME_STOP_DUE", "severity": "warn",
                    "detail": f"held {days_held}/{hold_max} days"})

    return out


# ─── Suppression: don't log the same event twice in quick succession ─────────

def _was_logged_recently(symbol: str, kind: str, within_sec: int = 300) -> bool:
    cutoff = (datetime.now(IST) - timedelta(seconds=within_sec)).isoformat()
    with falcon_conn() as con:
        row = con.execute("""
            SELECT 1 FROM falcon_trade_events
            WHERE symbol=? AND kind=? AND detected_at >= ?
            LIMIT 1
        """, (symbol, kind, cutoff)).fetchone()
    return row is not None


# ─── Main poll cycle ─────────────────────────────────────────────────────────

def poll_once(kite) -> Dict[str, Any]:
    """One sweep: refresh holdings → upsert state → detect & log triggers.
    Returns counts for log/observability."""
    # Reconcile fills FIRST — if a STOP partially filled since last poll, the
    # detect_triggers() pass below should see the updated row. Reconciler is
    # soft-fail: kite.orders() error returns an error-stamped summary and we
    # carry on; the next poll retries.
    fill_summary: Dict[str, Any] = {}
    try:
        from . import fill_reconciler  # noqa: WPS433
        fill_summary = fill_reconciler.reconcile(kite)
    except Exception as e:
        log.exception("fill_reconciler.reconcile raised: %s", e)

    # Refresh KiteTicker subscriptions to match current Falcon state. Soft-fail
    # — when ticker is disconnected we just lose the latency edge, not data.
    ticker_summary: Dict[str, Any] = {"status": "skipped"}
    try:
        from . import kite_ticker  # noqa: WPS433
        if kite_ticker.is_connected():
            ticker_summary = kite_ticker.refresh_subscriptions(kite)
    except Exception as e:
        log.exception("kite_ticker.refresh_subscriptions raised: %s", e)

    held = holdings_fetch.get_held_positions(kite, force_refresh=True)
    sl_orders = trade_db.get_open_sl_orders()
    entry_dates = trade_db.get_entry_dates()

    # Trail config per product (MTF + CNC). Read once per poll for consistency.
    cfg_by_product = {c["product"]: c for c in trail_config.get_all_configs()}

    # Pull the ticker module reference once — used per-symbol to override LTP
    # when a fresher tick is cached. Module-level is safe (lazy connect).
    try:
        from . import kite_ticker  # noqa: WPS433
        _ticker_get_ltp = kite_ticker.get_ltp
    except Exception:
        _ticker_get_ltp = None

    n_states = 0
    n_events = 0
    n_actions = 0
    n_ticker_overrides = 0
    for symbol, h in held.items():
        managed_by = "falcon" if symbol in sl_orders else "external"
        sl_info = sl_orders.get(symbol, {})
        sl_falcon = sl_info.get("sl_price")
        sl_kite_id = sl_info.get("kite_order_id")
        falcon_entry = entry_dates.get(symbol)
        product = h.get("product")
        # Prefer the ticker's fresh LTP over the holdings snapshot's last_price
        # (which can be 60s+ stale because of the holdings cache TTL).
        cur_price = h.get("current_price") or 0
        if _ticker_get_ltp is not None:
            tick_ltp = _ticker_get_ltp(symbol)
            if tick_ltp and tick_ltp > 0:
                cur_price = tick_ltp
                n_ticker_overrides += 1
        state = upsert_state(
            symbol               = symbol,
            managed_by           = managed_by,
            product              = product,
            qty                  = h["qty"],
            avg_entry            = h.get("avg_entry") or 0,
            entry_date           = falcon_entry,
            current_price        = cur_price,
            sl_price_from_falcon = sl_falcon,
            sl_kite_order_id     = sl_kite_id,
            is_t1_only           = bool(h.get("is_t1_only")),
        )
        n_states += 1
        triggers = detect_triggers(state)
        for t in triggers:
            if _was_logged_recently(symbol, t["kind"], within_sec=300):
                continue
            log_event(symbol, t["kind"], t["severity"], t["detail"],
                      auto_action_taken=False)
            n_events += 1

        # Phase 2 Chunk B — auto-actions (gated by per-product config + market hours)
        if managed_by == "falcon" and product in cfg_by_product:
            try:
                # Lazy-import to avoid circular dependency at module load
                from . import trail_manager  # noqa: WPS433
                acted = trail_manager.process_position(kite, state, cfg_by_product[product])
                n_actions += acted
            except Exception as e:
                log.exception("trail_manager.process_position raised for %s: %s", symbol, e)

    return {"poll_at": _now_iso(), "n_states": n_states,
            "n_events": n_events, "n_actions": n_actions,
            "fills": fill_summary, "ticker": ticker_summary,
            "n_ticker_overrides": n_ticker_overrides}


# ─── Background thread ────────────────────────────────────────────────────────

def _loop() -> None:
    log.info("Falcon position monitor started — interval=%ds", POLL_INTERVAL_SEC)
    # Lazy import to avoid circular issues at module load
    from services.kite_auth import get_kite_client, KiteAuthError  # noqa: WPS433
    from . import eod_flush  # noqa: WPS433
    while True:
        try:
            kite = get_kite_client(check=False)
            summary = poll_once(kite)
            log.info("Monitor poll: %s", summary)
            # EOD flush — idempotent, no-ops outside the post-15:35 window
            # and after first run of the day.
            try:
                eod_summary = eod_flush.maybe_run(kite)
                if eod_summary is not None:
                    log.info("EOD flush done: %s", eod_summary)
            except Exception as ee:
                log.exception("EOD flush raised: %s", ee)
        except KiteAuthError as e:
            log.warning("Monitor: token invalid (%s) — skip", e)
        except Exception as e:
            log.exception("Monitor poll crashed: %s", e)
        time.sleep(POLL_INTERVAL_SEC)


def start_background_monitor() -> None:
    """Start the monitor thread once. Idempotent."""
    global _started
    with _lock:
        if _started:
            return
        t = threading.Thread(target=_loop, daemon=True, name="falcon-position-monitor")
        t.start()
        _started = True
        log.info("Falcon position monitor thread launched.")
