"""End-of-day orchestrator — runs at ~16:10 IST after the daily pipeline.

Two responsibilities:
  1. **Stage tomorrow's NEW_ENTRY items** — for each signal in falcon_signals_live
     for the next trading day, build a frozen request payload (selected_symbols,
     per_trade, sl_pct, etc.) and insert into falcon_premarket_staging.

  2. **Stage BULK_ADOPT items for external positions** — scan kite.holdings(),
     find positions that aren't yet falcon-managed, compute SL price using the
     product's trail config (initial_sl_pct), insert into staging.

User reviews these in /falcon/premarket and confirms (STAGED → QUEUED).
The premarket_deployer thread fires QUEUED items at 9:15 IST next day.

Idempotency: re-running this in the same EOD window is safe — items in
STAGED status get their payload refreshed; terminal items (DEPLOYED/CANCELLED)
are never overwritten.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ...db import falcon_conn
from . import holdings_fetch, mtf_eligibility, premarket_staging, trade_db, trail_config
from .order_planner import plan_orders
from .services_round import round_to_tick_size
from .volatility_gate import sl_order_type

log = logging.getLogger("kanida.falcon.trade.eod")
IST = timezone(timedelta(hours=5, minutes=30))

# Default user preferences when staging — override by passing kwargs.
DEFAULT_TOTAL_CAPITAL  = 30_00_000   # ₹30L (matches V7 backtest sizing)
DEFAULT_PER_TRADE      = 50_000      # ₹50K margin per trade
DEFAULT_HOLD_DAYS      = 7
DEFAULT_SL_PCT         = -7.0
DEFAULT_TRAIL_TRIGGER  = 12.0
DEFAULT_TRAIL_LOOKBACK = 10


def _next_trading_day(d: date) -> date:
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt


# Pre-market deploy window opens at 9:15 IST. Used to decide whether to target
# TODAY (still pre-market) or tomorrow (post-9:15, today's window passed).
DEPLOY_HOUR_IST = 9
DEPLOY_MIN_IST  = 15


def _next_deploy_target_ist(now: Optional[datetime] = None) -> date:
    """Next trading day whose 9:15 IST deploy window hasn't yet passed.

    Examples (now in IST):
      Friday 22:55       → Monday   (Friday's 09:15 already passed; weekend skip)
      Saturday 11:00     → Monday
      Monday 06:00       → Monday   (today's 09:15 hasn't happened yet)
      Monday 12:00       → Tuesday  (today's 09:15 already passed)

    ALWAYS uses IST. Never reads local server clock or date.today() — those
    drift on non-IST hosts. See feedback_always_use_ist.md.
    """
    n = now if now is not None else datetime.now(IST)
    if n.tzinfo is None:
        n = n.replace(tzinfo=IST)
    today = n.date()
    today_deploy = n.replace(hour=DEPLOY_HOUR_IST, minute=DEPLOY_MIN_IST,
                              second=0, microsecond=0)
    # If today is a weekday AND we're still before 9:15 IST → today is our target
    if today.weekday() < 5 and n < today_deploy:
        return today
    # Else: next weekday
    nxt = today + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt


def _latest_signal_date() -> Optional[str]:
    with falcon_conn() as con:
        row = con.execute("SELECT MAX(signal_date) FROM falcon_signals_live").fetchone()
    return row[0] if row and row[0] else None


def stage_new_entries_for(target_date: str,
                           total_capital: int = DEFAULT_TOTAL_CAPITAL,
                           per_trade: int = DEFAULT_PER_TRADE,
                           hold_days: int = DEFAULT_HOLD_DAYS,
                           sl_pct: float = DEFAULT_SL_PCT,
                           trail_trigger_pct: float = DEFAULT_TRAIL_TRIGGER,
                           trail_lookback_days: int = DEFAULT_TRAIL_LOOKBACK
                           ) -> Dict[str, Any]:
    """Stage NEW_ENTRY items for `target_date`. Reads signals from falcon_signals_live
    (signal_date = the day they were emitted; entry_date = the day to trade).

    Returns {n_signals, n_staged, n_skipped} summary."""
    # Read the operator's daily_picks_max cap from engine_config. The signal
    # emitter produces 100 picks (top_N for visibility), but the operator
    # configured daily_picks_max=14 (Engine Playbook) — staging ALL 84-100 makes
    # /falcon/premarket unmanageable. Cap at playbook value here.
    # Bug fix 2026-05-14: orchestrator was staging every signal regardless of cap.
    try:
        from . import engine_config
        cfg = engine_config.get_config() or {}
        daily_picks_max = int(cfg.get("daily_picks_max") or 14)
    except Exception as e:
        log.warning("EOD: engine_config read failed (%s) — defaulting daily_picks_max=14", e)
        daily_picks_max = 14

    with falcon_conn() as con:
        rows = con.execute("""
            SELECT symbol, sector, score, n_fires, close_at_signal, fired_pattern_ids
              FROM falcon_signals_live
             WHERE entry_date = ?
             ORDER BY rank ASC
             LIMIT ?
        """, (target_date, daily_picks_max)).fetchall()

    if not rows:
        log.info("EOD: no signals for entry_date=%s — nothing to stage", target_date)
        return {"target_date": target_date, "n_signals": 0, "n_staged": 0, "n_skipped": 0,
                "daily_picks_max": daily_picks_max}

    log.info("EOD: staging top %d of available signals for entry_date=%s (daily_picks_max cap)",
             len(rows), target_date)

    n_staged = 0
    n_skipped = 0
    staged_symbols: List[str] = []
    skipped_symbols: List[Dict[str, str]] = []
    for r in rows:
        sym = r["symbol"]
        # stage_item itself preserves terminal rows (DEPLOYED/FAILED/CANCELLED).
        # Per-symbol payload: a tiny preview-style request the deployer will use.
        # NOTE: the deployer re-runs the full preview at 9:15 IST so margin/qty
        # reflect that morning's prices — we just freeze the operator's CONFIG choices.
        payload = {
            "symbol":              sym,
            "sector":              r["sector"],
            "score":               r["score"],
            "n_fires":             r["n_fires"],
            "close_at_signal":     r["close_at_signal"],
            "config": {
                "total_capital":       total_capital,
                "per_trade":           per_trade,
                "hold_days":           hold_days,
                "sl_pct":              sl_pct,
                "trail_trigger_pct":   trail_trigger_pct,
                "trail_lookback_days": trail_lookback_days,
            },
        }
        try:
            premarket_staging.stage_item(
                target_date=target_date, kind="NEW_ENTRY",
                symbol=sym, payload=payload,
            )
            n_staged += 1
            staged_symbols.append(sym)
        except Exception as e:
            log.warning("EOD: stage NEW_ENTRY %s failed: %s", sym, e)
            skipped_symbols.append({"symbol": sym, "reason": str(e)})
            n_skipped += 1

    log.info("EOD: staged %d NEW_ENTRY items for %s (skipped %d, cap=%d)",
              n_staged, target_date, n_skipped, daily_picks_max)
    return {
        "target_date":      target_date,
        "n_signals":        len(rows),
        "n_staged":         n_staged,
        "n_skipped":        n_skipped,
        "daily_picks_max":  daily_picks_max,
        "staged_symbols":   staged_symbols,
        "skipped_symbols":  skipped_symbols,
    }


def stage_bulk_adopts_for(target_date: str, kite=None) -> Dict[str, Any]:
    """Scan holdings, stage BULK_ADOPT items for non-falcon-managed positions.

    For each external position:
      * Pull product's trail config (initial_sl_pct, etc.)
      * Compute SL price = avg_entry × (1 + initial_sl_pct/100), tick-rounded
      * Stage a payload the deployer can feed straight to /positions/bulk-adopt
    """
    if kite is None:
        try:
            from services.kite_auth import get_kite_client  # noqa: WPS433
            kite = get_kite_client(check=True)
        except Exception as e:
            log.warning("EOD: stage_bulk_adopts: no Kite client (%s) — skipping", e)
            return {"target_date": target_date, "n_external": 0, "n_staged": 0, "skipped": []}

    try:
        held = holdings_fetch.get_held_positions(kite, force_refresh=True)
    except Exception as e:
        log.warning("EOD: holdings_fetch failed: %s — skipping bulk-adopt staging", e)
        return {"target_date": target_date, "n_external": 0, "n_staged": 0, "skipped": []}

    if not held:
        return {"target_date": target_date, "n_external": 0, "n_staged": 0, "skipped": []}

    # Identify symbols already managed by Falcon (have a live SL row).
    managed_syms = {sym for sym in trade_db.get_open_sl_orders().keys()}
    external = {sym: h for sym, h in held.items() if sym not in managed_syms}

    cfgs = {c["product"]: c for c in trail_config.get_all_configs()}
    n_staged = 0
    skipped: List[Dict[str, str]] = []
    staged_symbols: List[Dict[str, Any]] = []

    for sym, h in external.items():
        product = h.get("product", "CNC")
        if product == "MIXED":
            skipped.append({"symbol": sym, "reason": "PRODUCT_MIXED"})
            continue
        cfg = cfgs.get(product)
        initial_sl_pct = cfg["initial_sl_pct"] if cfg else DEFAULT_SL_PCT
        avg_entry = h.get("avg_entry") or 0
        if avg_entry <= 0:
            skipped.append({"symbol": sym, "reason": "NO_AVG_ENTRY"})
            continue
        try:
            tick = mtf_eligibility.get_tick_size(kite, sym)
        except Exception:
            tick = 0.05
        raw_sl = avg_entry * (1 + initial_sl_pct / 100)
        sl_price = round_to_tick_size(raw_sl, tick)

        payload = {
            "symbol":           sym,
            "qty":              h["qty"],
            "avg_entry":        avg_entry,
            "current_price":    h.get("current_price"),
            "product":          product,
            "sl_price":         sl_price,
            "computed_via":     "trail_config.initial_sl_pct",
            "initial_sl_pct":   initial_sl_pct,
        }
        try:
            premarket_staging.stage_item(
                target_date=target_date, kind="BULK_ADOPT",
                symbol=sym, payload=payload,
            )
            n_staged += 1
            staged_symbols.append({"symbol": sym, "qty": h["qty"], "sl_price": sl_price, "product": product})
        except Exception as e:
            log.warning("EOD: stage BULK_ADOPT %s failed: %s", sym, e)
            skipped.append({"symbol": sym, "reason": str(e)})

    log.info("EOD: staged %d BULK_ADOPT items for %s (held=%d, already-managed=%d, skipped=%d)",
              n_staged, target_date, len(held), len(managed_syms), len(skipped))
    return {
        "target_date":    target_date,
        "n_held":         len(held),
        "n_external":     len(external),
        "n_managed":      len(managed_syms),
        "n_staged":       n_staged,
        "skipped":        skipped,
        "staged_symbols": staged_symbols,
    }


def run_eod(target_date: Optional[str] = None, kite=None,
             auto_stage_bulk_adopts: bool = False) -> Dict[str, Any]:
    """One-shot EOD orchestrator. Stages tomorrow's NEW_ENTRY signals always.

    BULK_ADOPT items are NOT auto-staged anymore (operator preference, 2026-05-09):
    the pre-market queue stays empty until the operator explicitly stages adopts
    from /positions → Stage Bulk Adopt. This gives operators full control over
    what fires at 9:15 IST.

    Pass `auto_stage_bulk_adopts=True` to opt into the old behavior (e.g. for
    ops/recovery scripts that want every external position queued automatically).

    Args:
        target_date: ISO date to stage for. If None, derives from CURRENT IST
                     CLOCK — next 9:15 IST deploy window that hasn't passed.
        kite: optional KiteConnect client (else fetched from kite_auth).
        auto_stage_bulk_adopts: when True, also scan held positions and stage
                                BULK_ADOPT items. Default False (explicit only).
    """
    if target_date is None:
        target_date = _next_deploy_target_ist().isoformat()

    log.info("EOD orchestrator starting for target_date=%s (IST now=%s, auto_bulk=%s)",
              target_date, datetime.now(IST).isoformat(), auto_stage_bulk_adopts)

    # Step 0: reconcile Kite-side state. Zerodha auto-cancels Day-validity SL
    # orders at 15:30 IST, so any falcon_position_state row with
    # managed_by='falcon' is stale by EOD. Flip them to 'external' so the
    # bulk-adopt scan below picks them up for tomorrow.
    reconcile_summary: Dict[str, Any] = {"skipped": "no_kite_client"}
    if kite is not None:
        try:
            from . import kite_reconcile  # noqa: WPS433
            reconcile_summary = kite_reconcile.reconcile_managed_positions(kite)
            # If Kite returned zero alive orders (typical post-15:30 state),
            # fall back to force-empty mode — we KNOW the EOD wipe happened.
            if reconcile_summary.get("skipped") == "ABORTED_NO_ALIVE_KITE_ORDERS":
                log.info("EOD: Kite had zero alive orders (post-EOD wipe) — running force-empty reconcile")
                reconcile_summary = kite_reconcile.reconcile_force_all_stale(kite)
        except Exception as e:
            log.exception("EOD: reconcile crashed (continuing with stale state): %s", e)
            reconcile_summary = {"error": str(e)}

    new_entries = stage_new_entries_for(target_date)
    if auto_stage_bulk_adopts:
        bulk_adopts = stage_bulk_adopts_for(target_date, kite=kite)
    else:
        # Default: don't auto-stage. Operator stages adopts explicitly from
        # /positions → Stage Bulk Adopt. Reconcile already flipped stale
        # falcon-managed rows to external above; they're now visible there.
        bulk_adopts = {
            "target_date":   target_date,
            "n_external":    0,
            "n_staged":      0,
            "skipped":       "auto_stage_disabled_use_positions_page",
            "staged_symbols": [],
        }

    summary = {
        "target_date":  target_date,
        "ran_at":       datetime.now(IST).isoformat(),
        "reconcile":    reconcile_summary,
        "new_entries":  new_entries,
        "bulk_adopts":  bulk_adopts,
        "total_staged": new_entries["n_staged"] + bulk_adopts["n_staged"],
    }
    log.info("EOD orchestrator complete: %d total items staged for %s",
              summary["total_staged"], target_date)
    return summary
