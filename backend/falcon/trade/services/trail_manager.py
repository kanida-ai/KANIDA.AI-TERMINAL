"""Phase 2 Chunk B — trail manager + auto-exit executor.

Pure decision logic + Kite action execution. Called from position_monitor.poll_once()
after state is upserted and triggers are detected.

Safety guards (all enforced):
  1. Only acts on managed_by == 'falcon' positions. External positions are NEVER auto-acted on.
  2. Per-product auto_exit_enabled toggle in falcon_trail_config gates everything.
  3. FALCON_AUTOTRADE_ENABLED env var is the master kill switch (defense-in-depth).
  4. Market-hours check: no actions outside 09:15–15:30 IST on weekdays.
  5. Per-symbol rate limit: max one action per ACTION_COOLDOWN_SEC (default 120s).
  6. Action audit: every action emits an event row in falcon_trade_events.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ...db import falcon_conn
from . import trade_db
from .order_executor import _autotrade_enabled

log = logging.getLogger("kanida.falcon.trade.trail")
IST = timezone(timedelta(hours=5, minutes=30))

ACTION_COOLDOWN_SEC = int(os.environ.get("FALCON_ACTION_COOLDOWN_SEC", "120"))


# ─── Action types ────────────────────────────────────────────────────────────

@dataclass
class ReplaceSL:
    new_sl_price: float
    reason:       str    # 'TRAIL_ACTIVATED' | 'TRAIL_RAISED'


@dataclass
class ExitAtMarket:
    reason: str          # 'BREACHED_SL' | 'TIME_STOP' | 'TARGET_HIT'


Action = Any   # ReplaceSL | ExitAtMarket


# ─── Pure evaluation (deterministic given inputs) ────────────────────────────

def evaluate_position(state: Dict[str, Any], cfg: Dict[str, Any],
                       today_iso: str,
                       rolling_low: Optional[float] = None
                       ) -> Tuple[List[Action], Dict[str, Any]]:
    """Decide actions based on state + product config. Returns (actions, state_updates).

    `rolling_low` is required when cfg["trail_method"] == "10d_low":
    it's the lowest LOW across the last `cfg["trail_lookback"]` completed
    trading sessions. Caller (process_position) fetches it via trail_data;
    if the fetch failed it passes None and we skip trail mutations for this
    poll (decision: skip-on-failure, never substitute percentage).
    Hard SL breach + time-stop still fire because they don't depend on
    rolling_low — only the trail-raise / activation paths do.
    """
    cur = state.get("last_seen_price") or 0
    avg = state.get("avg_entry") or 0
    cur_sl = state.get("current_sl_price") or 0

    if state.get("managed_by") != "falcon":
        return [], {}
    if not cfg.get("auto_exit_enabled"):
        return [], {}
    if cur <= 0 or avg <= 0:
        return [], {}

    # 1. Hard SL breach → exit (independent of trail method or rolling_low fetch)
    if cur <= cur_sl:
        return [ExitAtMarket(reason="BREACHED_SL")], {}

    # 2. Time-stop (independent of trail method or rolling_low fetch)
    # Window-gated to 15:25-15:30 IST per blueprint Workflow C4 (2026-05-10):
    # we want time-stop sells to fire NEAR market close, not mid-session.
    # Mid-session market sells take avoidable slippage and can't be rebought
    # cheaply. Holding through the day until 15:25 lets a winner finish strong.
    days_held = _days_between(state.get("entry_date"), today_iso)
    if days_held is not None and days_held >= (cfg.get("hold_days_max") or 7):
        if _in_time_stop_window():
            return [ExitAtMarket(reason="TIME_STOP")], {}
        # else: due but outside window — silently skip, will fire at 15:25 IST

    method = (cfg.get("trail_method") or "percentage").lower()

    # If 10d_low method but rolling_low fetch failed → skip trail mutations.
    # Existing Kite SL stays in place; next successful poll re-evaluates.
    if method == "10d_low" and rolling_low is None:
        return [], {}

    from .volatility_gate import round_to_tick
    activate_threshold = avg * (1 + (cfg["activate_pct"] or 0) / 100)
    lock_floor         = avg * (1 + (cfg["lock_pct"] or 0) / 100)
    trail_active       = bool(state.get("trail_active"))

    # Compute candidate trail SL based on the chosen method.
    if method == "10d_low":
        # Donchian floor: lowest low over last N completed sessions, clamped
        # to lock_floor so we never accept worse-than-+lock_pct profit.
        candidate_sl = max(lock_floor, float(rolling_low))
    else:
        # Percentage method (legacy): SL = current × (1 - trail_sl_pct/100)
        candidate_sl = max(lock_floor, cur * (1 - (cfg["trail_sl_pct"] or 0) / 100))

    # 3. Trail activation: jump SL from initial floor to candidate
    if not trail_active and cur >= activate_threshold:
        new_sl = round_to_tick(candidate_sl)
        # Target trailing on activation. Under 10d_low, we still set a reference
        # target = current × (1 + trail_profit_pct/100) — purely informational
        # for the UI, no Kite order. Under percentage, same formula.
        new_target = round_to_tick(cur * (1 + (cfg.get("trail_profit_pct") or 0) / 100))
        cur_target = state.get("target_price") or 0
        target_update = {"target_price": new_target} if new_target > cur_target else {}
        if new_sl > cur_sl:
            return ([ReplaceSL(new_sl_price=new_sl, reason="TRAIL_ACTIVATED")],
                    {"trail_active": 1, "current_sl_price": new_sl,
                     "hw_reached": 1, **target_update})

    # 4. Already trailing — ratchet SL and target up only (never lower)
    if trail_active:
        new_sl     = round_to_tick(candidate_sl)
        new_target = round_to_tick(cur * (1 + (cfg.get("trail_profit_pct") or 0) / 100))
        cur_target = state.get("target_price") or 0
        actions = []
        updates: Dict[str, Any] = {}
        # Raise SL? (epsilon-guarded to avoid micro-adjustments per poll)
        if new_sl > cur_sl + max(0.05, cur_sl * 0.001):
            actions.append(ReplaceSL(new_sl_price=new_sl, reason="TRAIL_RAISED"))
            updates["current_sl_price"] = new_sl
        # Raise target? (state-only update — no Kite order; UI reflects trailing target)
        if new_target > cur_target + max(0.05, cur_target * 0.001):
            updates["target_price"] = new_target
        if actions or updates:
            return actions, updates

    return [], {}


def _days_between(entry_date: Optional[str], today_iso: str) -> Optional[int]:
    if not entry_date:
        return None
    try:
        ed = datetime.fromisoformat(entry_date).date()
        td = datetime.fromisoformat(today_iso).date()
        return (td - ed).days
    except Exception:
        return None


# ─── Safety guards ───────────────────────────────────────────────────────────

def _market_open_now() -> bool:
    n = datetime.now(IST)
    if n.weekday() >= 5:
        return False
    if n.hour < 9 or (n.hour == 9 and n.minute < 15):
        return False
    if n.hour > 15 or (n.hour == 15 and n.minute >= 30):
        return False
    return True


def _in_time_stop_window(now: Optional[datetime] = None) -> bool:
    """Time-stop fires only in the last 5 minutes of the session (15:25-15:30 IST).
    Mid-session market sells eat avoidable slippage; this lets a winner run all
    day before flipping out at the close on hold_days_max."""
    n = now if now is not None else datetime.now(IST)
    if n.weekday() >= 5:
        return False
    h, m = n.hour, n.minute
    return (h == 15 and 25 <= m < 30)


def _action_throttled(state: Dict[str, Any]) -> bool:
    last = state.get("last_action_at")
    if not last:
        return False
    try:
        last_dt = datetime.fromisoformat(last)
        return (datetime.now(IST) - last_dt).total_seconds() < ACTION_COOLDOWN_SEC
    except Exception:
        return False


# ─── Action execution ────────────────────────────────────────────────────────

def execute_replace_sl(kite, state: Dict[str, Any], action: ReplaceSL) -> Dict[str, Any]:
    """Modify the existing SL order in place (atomic, no qty re-lock race).
    Falls back to cancel+place only when there's no existing order ID."""
    symbol = state["symbol"]
    qty    = state["qty"]
    old_id = state.get("sl_kite_order_id")

    # Compute new prices with per-stock tick + lower-circuit guard
    from .mtf_eligibility import get_tick_size
    from .holdings_fetch import get_circuit_limits
    from .services_round import round_to_tick_size
    tick = get_tick_size(kite, symbol)
    circuit = get_circuit_limits(kite, [symbol]).get(symbol, {})
    lower_circuit = circuit.get("lower", 0)
    raw_trigger = action.new_sl_price
    if lower_circuit > 0 and raw_trigger <= lower_circuit:
        raw_trigger = lower_circuit + tick
        log.warning("Trail SL for %s raised above lower_circuit ₹%.2f → ₹%.2f",
                    symbol, lower_circuit, raw_trigger)
    trigger = round_to_tick_size(raw_trigger, tick)
    raw_limit = max(trigger * 0.998, (lower_circuit + tick) if lower_circuit > 0 else 0)
    limit = round_to_tick_size(raw_limit, tick)
    if limit > trigger:
        limit = trigger

    from .order_executor import _retry_kite_call

    # Path A — modify existing order in place. Atomic, no cancel-relock race.
    if old_id:
        try:
            _retry_kite_call(
                lambda: kite.modify_order(
                    variety       = kite.VARIETY_REGULAR,
                    order_id      = old_id,
                    price         = limit,
                    trigger_price = trigger,
                ),
                "modify_order", symbol,
            )
            log.info("Modified SL for %s in place: trigger ₹%.2f limit ₹%.2f (id=%s)",
                     symbol, trigger, limit, old_id)
            return {"status": "PLACED", "kite_order_id": str(old_id),
                    "trigger": trigger, "limit": limit, "error": None,
                    "modified_in_place": True}
        except Exception as e:
            log.warning("modify_order failed for %s (id=%s): %s — falling back to cancel+place",
                        symbol, old_id, e)
            try:
                _retry_kite_call(
                    lambda: kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=old_id),
                    "cancel_order", symbol,
                )
            except Exception as ce:
                log.warning("Old SL cancel also failed for %s: %s", symbol, ce)

    # Path B — fresh place (no prior order, or modify failed)
    new_tag = f"f-trail-{symbol[:7].lower()}-{int(datetime.now(IST).timestamp()) % 100000}"[:20]
    try:
        new_id = _retry_kite_call(
            lambda: kite.place_order(
                variety           = kite.VARIETY_REGULAR,
                exchange          = kite.EXCHANGE_NSE,
                tradingsymbol     = symbol,
                transaction_type  = kite.TRANSACTION_TYPE_SELL,
                quantity          = qty,
                product           = "MTF" if state.get("product") == "MTF" else kite.PRODUCT_CNC,
                order_type        = kite.ORDER_TYPE_SL,
                price             = limit,
                trigger_price     = trigger,
                tag               = new_tag,
            ),
            "place_order(trail)", symbol,
        )
        return {"status": "PLACED", "kite_order_id": str(new_id),
                "trigger": trigger, "limit": limit, "error": None,
                "modified_in_place": False}
    except Exception as e:
        log.error("Trail SL place failed for %s: %s", symbol, e)
        return {"status": "FAILED", "kite_order_id": None,
                "trigger": trigger, "limit": limit, "error": str(e),
                "modified_in_place": False}


def execute_exit_at_market(kite, state: Dict[str, Any], reason: str) -> Dict[str, Any]:
    """Cancel SL, place MARKET sell with market_protection=1.0."""
    symbol = state["symbol"]
    qty    = state["qty"]
    old_id = state.get("sl_kite_order_id")

    if old_id:
        try:
            kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=old_id)
        except Exception as e:
            log.warning("SL cancel during exit failed for %s: %s", symbol, e)

    exit_tag = f"f-exit-{reason[:3].lower()}-{symbol[:7].lower()}"[:20]
    try:
        new_id = kite.place_order(
            variety           = kite.VARIETY_REGULAR,
            exchange          = kite.EXCHANGE_NSE,
            tradingsymbol     = symbol,
            transaction_type  = kite.TRANSACTION_TYPE_SELL,
            quantity          = qty,
            product           = "MTF" if state.get("product") == "MTF" else kite.PRODUCT_CNC,
            order_type        = kite.ORDER_TYPE_MARKET,
            market_protection = 1.0,
            tag               = exit_tag,
        )
        return {"status": "PLACED", "kite_order_id": str(new_id), "error": None}
    except Exception as e:
        log.error("Auto-exit place failed for %s (%s): %s", symbol, reason, e)
        return {"status": "FAILED", "kite_order_id": None, "error": str(e)}


# ─── State update helper ─────────────────────────────────────────────────────

def apply_state_updates(symbol: str, updates: Dict[str, Any],
                         sl_kite_order_id: Optional[str] = None,
                         stamp_cooldown: bool = True) -> None:
    """Update state row.
    By default stamps last_action_at (used by cooldown). Pass stamp_cooldown=False
    for state-only ratchets (e.g. target_price) that shouldn't block the next
    real action.
    Pass updates={} on action failure to enforce cooldown without changing other state."""
    sets, vals = [], []
    for k, v in updates.items():
        sets.append(f"{k} = ?")
        vals.append(v)
    if sl_kite_order_id is not None:
        sets.append("sl_kite_order_id = ?")
        vals.append(sl_kite_order_id)
    if stamp_cooldown:
        sets.append("last_action_at = ?")
        vals.append(datetime.now(IST).isoformat())
    if not sets:
        return
    vals.append(symbol)
    with falcon_conn() as con:
        con.execute(f"UPDATE falcon_position_state SET {', '.join(sets)} WHERE symbol=?", vals)
        con.commit()


def log_action_event(symbol: str, kind: str, severity: str, detail: str,
                      kite_id: Optional[str]) -> None:
    """Wrapper around position_monitor.log_event tailored to action audit."""
    from . import position_monitor
    position_monitor.log_event(symbol, kind, severity, detail,
                                auto_action_taken=True, related_kite_id=kite_id)


def _record_trail_sl_in_audit(state: Dict[str, Any], result: Dict[str, Any],
                               modified_in_place: bool) -> None:
    """Sync falcon_trade_orders so trade_db.get_open_sl_orders() returns the
    LIVE trail SL (not the original adopted one). Called only after a successful
    SL replacement.

    Two cases:
      A. modify_order succeeded — same kite_order_id, just update price/trigger
      B. cancel+place — mark old row CANCELLED, insert new row PLACED
    """
    symbol = state["symbol"]
    new_id = result.get("kite_order_id")
    new_trigger = result.get("trigger")
    new_limit = result.get("limit")
    if not new_id:
        return

    if modified_in_place:
        # Same Kite order ID — just update prices on the existing row
        with falcon_conn() as con:
            con.execute("""
                UPDATE falcon_trade_orders
                SET trigger_price = ?, price = ?
                WHERE kite_order_id = ? AND role = 'STOP' AND status IN ('PLACED', 'PENDING')
            """, (new_trigger, new_limit, new_id))
            con.commit()
        return

    # Cancel+place fallback — mark the old SL row CANCELLED, insert a new PLACED row
    from . import trade_db as _tdb
    old = _tdb.get_open_sl_order(symbol)
    if old:
        with falcon_conn() as con:
            con.execute("""
                UPDATE falcon_trade_orders SET status = 'CANCELLED'
                WHERE id = ? AND role = 'STOP'
            """, (old["id"],))
            con.commit()

    # Use the previous batch_id if available (preserves audit grouping)
    batch_id = old["batch_id"] if old else f"trail_{symbol[:6].lower()}"
    tag = f"f-trailrec-{symbol[:7].lower()}-{int(datetime.now(IST).timestamp()) % 100000}"[:20]
    _tdb.insert_order(
        batch_id        = batch_id,
        symbol          = symbol,
        side            = "SELL",
        role            = "STOP",
        qty             = state["qty"],
        idempotency_key = tag,
        kite_order_id   = new_id,
        price           = new_limit,
        trigger_price   = new_trigger,
        product         = state.get("product", "MTF"),
        order_type      = "SL-L",
        status          = "PLACED",
    )


# ─── Top-level orchestrator (called from monitor) ────────────────────────────

def process_position(kite, state: Dict[str, Any], cfg: Dict[str, Any]) -> int:
    """Evaluate one position, take actions if any. Returns count of actions taken.

    Caller (monitor.poll_once) handles the per-position try/except and emits the
    standard event logs; this is the action-taking layer."""
    if not _autotrade_enabled():
        return 0
    if not _market_open_now():
        return 0
    if _action_throttled(state):
        return 0

    # Fetch rolling low ONLY if this product is configured for the 10d_low method.
    # On API failure get_rolling_low() returns None → evaluate_position will skip
    # trail mutations (existing Kite SL stays); next poll retries.
    rolling_low: Optional[float] = None
    if (cfg.get("trail_method") or "percentage").lower() == "10d_low":
        from .trail_data import get_rolling_low
        rolling_low = get_rolling_low(
            kite, state["symbol"], int(cfg.get("trail_lookback") or 10)
        )

    today_iso = datetime.now(IST).date().isoformat()
    actions, updates = evaluate_position(state, cfg, today_iso, rolling_low=rolling_low)
    if not actions:
        # State-only updates (e.g. target_price ratchet) still need to be persisted
        # even when no Kite action fires. stamp_cooldown=False so a ratchet doesn't
        # block the next real SL action.
        if updates:
            apply_state_updates(state["symbol"], updates, stamp_cooldown=False)
        return 0

    method_tag = (cfg.get("trail_method") or "percentage").lower()
    n_actions = 0
    for a in actions:
        if isinstance(a, ReplaceSL):
            result = execute_replace_sl(kite, state, a)
            if result["status"] == "PLACED":
                apply_state_updates(state["symbol"], updates,
                                     sl_kite_order_id=result["kite_order_id"])
                # Sync falcon_trade_orders to reflect the new live SL.
                # Without this, monitor.upsert_state will keep overwriting
                # sl_kite_order_id back to the (cancelled) original via COALESCE.
                _record_trail_sl_in_audit(
                    state, result, modified_in_place=result.get("modified_in_place", False),
                )
                # Tag the event with the method used so the audit log distinguishes
                # donchian-driven vs percentage-driven SL bumps.
                rl_str = f" rl=₹{rolling_low:.2f}" if (method_tag == "10d_low" and rolling_low) else ""
                log_action_event(state["symbol"], "SL_REPLACED", "info",
                    f"{a.reason} [{method_tag}]: trigger ₹{result['trigger']:.2f} limit ₹{result['limit']:.2f}{rl_str}",
                    result["kite_order_id"])
                n_actions += 1
            else:
                # Cooldown on failure (prevents the 60s retry loop that
                # stacks rejected orders against Kite qty checks).
                apply_state_updates(state["symbol"], {})
                log_action_event(state["symbol"], "SL_REPLACE_FAILED", "warn",
                    f"{a.reason} [{method_tag}]: {result['error']}", None)
        elif isinstance(a, ExitAtMarket):
            result = execute_exit_at_market(kite, state, a.reason)
            if result["status"] == "PLACED":
                apply_state_updates(state["symbol"], {"managed_by": "falcon_exited"},
                                     sl_kite_order_id=None)
                log_action_event(state["symbol"], "EXIT_PLACED", "critical",
                    f"{a.reason} — market sell qty={state['qty']}",
                    result["kite_order_id"])
                n_actions += 1
            else:
                apply_state_updates(state["symbol"], {})
                log_action_event(state["symbol"], "EXIT_FAILED", "critical",
                    f"{a.reason}: {result['error']}", None)
    return n_actions
