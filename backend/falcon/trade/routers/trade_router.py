"""/api/falcon/trade/* — pre-market entry + held-positions visibility.

Endpoints (per Design.md §3):
  POST /api/falcon/trade/preview
  POST /api/falcon/trade/smoke-test
  POST /api/falcon/trade/place
  POST /api/falcon/trade/cancel-all
  GET  /api/falcon/trade/status?batch_id=...
  GET  /api/falcon/trade/positions
  POST /api/falcon/trade/positions/exit
"""
from __future__ import annotations

import logging
import os
import secrets
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field

from ...db import falcon_conn
from ..services import holdings_fetch, margin_calc, mtf_eligibility, position_monitor, trade_db, trail_config
from ..services.order_executor import (
    _autotrade_enabled,
    cancel_all,
    execute_batch,
    idempotency_key,
    smoke_test_one,
)
from ..services.order_planner import OrderSpec, plan_orders, cheapest as cheapest_order
from ..services.volatility_gate import round_to_tick, sl_order_type

log = logging.getLogger("kanida.falcon.trade.router")
router = APIRouter()
IST = timezone(timedelta(hours=5, minutes=30))

# In-memory preview store: preview_id → {created_at, request, orders, ...}
_preview_store: Dict[str, Dict[str, Any]] = {}
PREVIEW_TTL = timedelta(minutes=30)


def _purge_old_previews() -> None:
    now = datetime.now()
    for pid in list(_preview_store.keys()):
        if now - _preview_store[pid]["created_at"] > PREVIEW_TTL:
            del _preview_store[pid]


# ── Request / response models ────────────────────────────────────────────────

class PreviewRequest(BaseModel):
    signal_date:          Optional[str] = None
    selected_symbols:     List[str]
    total_capital:        int
    per_trade:            int
    use_leverage:         bool  = True
    hold_days:            int   = 7
    sl_pct:               float = -7.0
    trail_trigger_pct:    float = 10.0
    trail_lookback_days:  int   = 10
    hold_actions:         Dict[str, str] = Field(default_factory=dict)


class SmokeRequest(BaseModel):
    preview_id: str


class PlaceRequest(BaseModel):
    preview_id:   str
    confirm_text: str


class CancelRequest(BaseModel):
    batch_id: str


class PositionExitRequest(BaseModel):
    symbol:  str
    confirm: bool = True


class MtfCheckRequest(BaseModel):
    symbols: List[str]


class PositionAdoptRequest(BaseModel):
    symbol:    str
    sl_price:  Optional[float] = None   # if omitted, computed from product config (avg × (1 + initial_sl_pct/100))
    sl_basis:  str = "entry"            # "entry" | "current" — default uses avg_entry as anchor
    confirm:   bool = False             # must be true to actually place


class BulkAdoptItem(BaseModel):
    symbol:    str
    sl_price:  Optional[float] = None


class BulkAdoptRequest(BaseModel):
    items:        List[BulkAdoptItem]
    product:      Optional[str] = None     # filter: "MTF" | "CNC" | None for both — informational only
    confirm:      bool = False              # must be true to actually place


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_kite():
    """Returns an authenticated KiteConnect client or raises 401."""
    from services.kite_auth import KiteAuthError, get_kite_client  # noqa: WPS433
    try:
        return get_kite_client(check=True)
    except KiteAuthError as e:
        raise HTTPException(401, f"KITE_TOKEN_INVALID: {e.detail}")


def _load_signals(signal_date: Optional[str]) -> List[dict]:
    with falcon_conn() as con:
        if signal_date is None:
            row = con.execute("SELECT MAX(signal_date) FROM falcon_signals_live").fetchone()
            signal_date = row[0] if row else None
        if not signal_date:
            raise HTTPException(404, "NO_SIGNALS_FOR_DATE")
        rows = con.execute("""
            SELECT * FROM falcon_signals_live
            WHERE signal_date = ?
            ORDER BY rank ASC
        """, (signal_date,)).fetchall()
    if not rows:
        raise HTTPException(404, f"NO_SIGNALS_FOR_DATE: {signal_date}")
    return [dict(r) for r in rows]


def _next_trading_day(d: str) -> str:
    dt = datetime.fromisoformat(d).date()
    nxt = dt + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt = nxt + timedelta(days=1)
    return nxt.isoformat()


def _new_batch_id(suffix: str = "") -> str:
    today = date.today().isoformat().replace("-", "_")
    rand  = secrets.token_hex(3)
    base  = f"btc_{today}_{rand}"
    return f"{base}{suffix}" if suffix else base


def _market_is_open_now() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    if now.hour < 9 or (now.hour == 9 and now.minute < 15):
        return False
    if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
        return False
    return True


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/falcon/trade/preview")
def preview(req: PreviewRequest):
    if req.total_capital <= 0:
        raise HTTPException(400, "INVALID_INPUT: total_capital must be > 0")
    if req.per_trade <= 0:
        raise HTTPException(400, "INVALID_INPUT: per_trade must be > 0")
    if not req.selected_symbols:
        raise HTTPException(400, "INVALID_INPUT: selected_symbols is empty")

    n_selected = len(req.selected_symbols)
    total_deployed = n_selected * req.per_trade
    if total_deployed > req.total_capital:
        raise HTTPException(400, (f"INSUFFICIENT_CAPITAL: "
                                   f"{n_selected} × ₹{req.per_trade} > ₹{req.total_capital}"))

    if trade_db.has_active_batch_for_today():
        raise HTTPException(409, "BATCH_IN_PROGRESS")

    kite = _get_kite()
    signals = _load_signals(req.signal_date)
    signal_date = signals[0]["signal_date"]
    entry_date  = _next_trading_day(signal_date)

    held = holdings_fetch.get_held_positions(kite)

    # Phase 1: VIX is a constant placeholder. Phase 2 wires real value.
    vix_today = 14.0
    sl_fn = lambda sym: sl_order_type(vix_today, gap_pct=None)
    mtf_fn = lambda sym: mtf_eligibility.is_mtf_eligible(kite, sym)

    # Phase 2.2: ask Kite per-symbol MTF margin upfront so qty matches what
    # Zerodha actually requires (PER_TRADE = MARGIN under MTF, not cash).
    # Single batched call across all selected symbols; cached 24h per symbol.
    margin_fn = lambda items: margin_calc.fetch_margins_batch(kite, items)

    # Per-symbol tick lookup. Many NSE scrips have tick=0.10 (not the 0.05
    # default) — Zerodha rejects SL orders rounded to the wrong tick. Cached
    # via mtf_eligibility (it caches the full instrument row per symbol).
    tick_fn = lambda sym: mtf_eligibility.get_tick_size(kite, sym)

    orders, skipped, action_counts = plan_orders(
        signals          = signals,
        selected_symbols = req.selected_symbols,
        per_trade        = req.per_trade,
        sl_pct           = req.sl_pct,
        trail_trigger_pct= req.trail_trigger_pct,
        held_by_symbol   = held,
        hold_actions     = req.hold_actions,
        mtf_eligible_fn  = mtf_fn,
        sl_order_type_fn = sl_fn,
        margin_fn        = margin_fn,
        tick_size_fn     = tick_fn,
        default_product  = "MTF",
    )

    overlap_syms = [s for s in req.selected_symbols if s in held]
    n_skipped_mtf = sum(1 for s in skipped if s.get("reason") == "MTF_INELIGIBLE")
    util_pct = round((total_deployed / req.total_capital) * 100, 2) if req.total_capital else 0
    plan_total_notional = sum(o.notional for o in orders)
    plan_total_margin   = sum(o.margin_required for o in orders)
    n_margin_failed     = sum(1 for o in orders if o.margin_lookup_failed)
    plan_eff_leverage   = round(plan_total_notional / plan_total_margin, 2) if plan_total_margin > 0 else None

    preview_id = "prv_" + secrets.token_hex(4)
    _purge_old_previews()
    _preview_store[preview_id] = {
        "created_at":    datetime.now(),
        "request":       req.model_dump(),
        "orders":        [o.to_dict() for o in orders],
        "skipped":       skipped,
        "action_counts": action_counts,
        "signal_date":   signal_date,
        "entry_date":    entry_date,
        "vix_today":     vix_today,
        "n_overlap":     len(overlap_syms),
    }

    warnings: List[str] = []
    if util_pct > 90:
        warnings.append("UTILIZATION_HIGH")
    if n_margin_failed:
        warnings.append(f"MARGIN_LOOKUP_FAILED_FOR_{n_margin_failed}_SYMBOLS_CASH_SIZED")

    return {
        "preview_id":         preview_id,
        "signal_date":        signal_date,
        "entry_date":         entry_date,
        "n_signals":          len(signals),
        "n_selected":         n_selected,
        "n_eligible":         len(orders) + action_counts["n_skip_action"],
        "n_skipped_mtf":      n_skipped_mtf,
        "n_overlap":          len(overlap_syms),
        "n_skip_action":      action_counts["n_skip_action"],
        "n_average_action":   action_counts["n_average_action"],
        "skipped":            skipped,
        "existing_positions": [{"symbol": sym, **held[sym]} for sym in held],
        "per_trade":          req.per_trade,
        "total_deployed":     total_deployed,
        "headroom":           req.total_capital - total_deployed,
        "utilization_pct":    util_pct,
        "plan_total_notional": plan_total_notional,
        # Phase 2.2 — margin-aware totals. PER_TRADE budget is interpreted as
        # margin under MTF, so plan_total_margin should ≈ n_orders × per_trade.
        "plan_total_margin":   round(plan_total_margin, 2),
        "plan_eff_leverage":   plan_eff_leverage,
        "n_margin_failed":     n_margin_failed,
        "orders":             [o.to_dict() for o in orders],
        "warnings":           warnings,
        "holdings_fetch_error": holdings_fetch.last_error(),
    }


@router.post("/falcon/trade/smoke-test")
def smoke_test(req: SmokeRequest):
    p = _preview_store.get(req.preview_id)
    if not p:
        raise HTTPException(400, "PREVIEW_EXPIRED_OR_INVALID")
    if datetime.now() - p["created_at"] > PREVIEW_TTL:
        raise HTTPException(400, "PREVIEW_EXPIRED")
    if not p["orders"]:
        raise HTTPException(400, "EMPTY_PLAN")

    kite = _get_kite()
    cheapest_dict = min(p["orders"], key=lambda o: o["close"])
    cheapest = OrderSpec(**cheapest_dict)

    smoke_batch_id = _new_batch_id(suffix="_smk")
    trade_db.create_run(
        batch_id            = smoke_batch_id,
        signal_date         = p["signal_date"],
        entry_date          = p["entry_date"],
        selected_symbols    = [cheapest.symbol],
        total_capital       = p["request"]["total_capital"],
        per_trade           = p["request"]["per_trade"],
        use_leverage        = p["request"]["use_leverage"],
        hold_days           = p["request"]["hold_days"],
        sl_pct              = p["request"]["sl_pct"],
        trail_trigger_pct   = p["request"]["trail_trigger_pct"],
        trail_lookback_days = p["request"]["trail_lookback_days"],
        started_at          = datetime.now(IST).isoformat(),
    )
    placed = False
    crash_error: Optional[str] = None
    result: Dict[str, Any] = {"status": "FAILED", "kite_order_id": None, "error": "did not run"}
    try:
        result = smoke_test_one(kite, smoke_batch_id, cheapest)
        placed = result["status"] == "PLACED"
    except Exception as e:
        crash_error = str(e)
        log.exception("smoke_test_one raised")
    finally:
        trade_db.finalize_run(
            batch_id         = smoke_batch_id,
            status           = "COMPLETED" if placed else "ABORTED",
            n_attempted      = 1,
            n_filled         = 1 if placed else 0,
            n_failed         = 0 if placed else 1,
            n_overlap        = 0,
            n_skip_action    = 0,
            n_average_action = 0,
            total_deployed   = int(cheapest.close) if placed else 0,
            finished_at      = datetime.now(IST).isoformat(),
            notes            = crash_error,
        )
    if crash_error:
        raise HTTPException(500, f"SMOKE_CRASHED: {crash_error}")

    return {
        "status":         result["status"],
        "smoke_symbol":   cheapest.symbol,
        "smoke_qty":      1,
        "smoke_price":    cheapest.close,
        "kite_order_id":  result["kite_order_id"],
        "placed_at":      datetime.now(IST).isoformat() if placed else None,
        "error":          result["error"],
        "smoke_batch_id": smoke_batch_id,
    }


@router.post("/falcon/trade/place")
def place(req: PlaceRequest):
    if req.confirm_text != "CONFIRM":
        raise HTTPException(400, "CONFIRM_TEXT_MISMATCH")

    p = _preview_store.get(req.preview_id)
    if not p:
        raise HTTPException(400, "PREVIEW_EXPIRED_OR_INVALID")
    if datetime.now() - p["created_at"] > PREVIEW_TTL:
        raise HTTPException(400, "PREVIEW_EXPIRED")
    if not p["orders"]:
        raise HTTPException(400, "EMPTY_PLAN")

    if not _market_is_open_now():
        raise HTTPException(403, "MARKET_CLOSED: wait until 9:15:00 IST on a weekday")

    if trade_db.has_active_batch_for_today():
        raise HTTPException(409, "BATCH_IN_PROGRESS")

    # Structural preflight gate: every place_order path must pass the 12
    # invariants. Reports the specific check name(s) that blocked so the
    # operator can fix exactly what's broken.
    try:
        from ... import preflight as _pf  # noqa: WPS433
        pf = _pf.run(force=True)
        if not pf.ok:
            red = [{"name": c.name, "detail": c.detail, "remediation": c.remediation}
                   for c in pf.checks if c.status == _pf.RED]
            raise HTTPException(412, {
                "code":      "PREFLIGHT_BLOCKED",
                "message":   f"{len(red)} preflight check(s) failed — refusing to place orders",
                "red":       red,
                "preflight_url": "/api/falcon/preflight",
            })
    except HTTPException:
        raise
    except Exception as e:
        log.exception("preflight raised in /place — proceeding (degraded): %s", e)

    kite = _get_kite()
    batch_id = _new_batch_id()
    started_at = datetime.now(IST).isoformat()

    trade_db.create_run(
        batch_id            = batch_id,
        signal_date         = p["signal_date"],
        entry_date          = p["entry_date"],
        selected_symbols    = p["request"]["selected_symbols"],
        total_capital       = p["request"]["total_capital"],
        per_trade           = p["request"]["per_trade"],
        use_leverage        = p["request"]["use_leverage"],
        hold_days           = p["request"]["hold_days"],
        sl_pct              = p["request"]["sl_pct"],
        trail_trigger_pct   = p["request"]["trail_trigger_pct"],
        trail_lookback_days = p["request"]["trail_lookback_days"],
        started_at          = started_at,
    )

    orders = [OrderSpec(**o) for o in p["orders"]]

    summary: Dict[str, Any] = {
        "n_attempted": 0, "n_filled": 0, "n_failed": 0,
        "total_deployed": 0, "aborted": True, "abort_reason": "did not run",
    }
    crash_error: Optional[str] = None
    try:
        summary = execute_batch(kite, batch_id, orders)
    except Exception as e:
        crash_error = str(e)
        log.exception("execute_batch raised")
    finally:
        finished_at = datetime.now(IST).isoformat()
        trade_db.finalize_run(
            batch_id         = batch_id,
            status           = "ABORTED" if (summary["aborted"] or crash_error) else "COMPLETED",
            n_attempted      = summary["n_attempted"],
            n_filled         = summary["n_filled"],
            n_failed         = summary["n_failed"],
            n_overlap        = p.get("n_overlap", 0),
            n_skip_action    = p["action_counts"]["n_skip_action"],
            n_average_action = p["action_counts"]["n_average_action"],
            total_deployed   = summary["total_deployed"],
            finished_at      = finished_at,
            notes            = crash_error or summary.get("abort_reason"),
        )
    if crash_error:
        raise HTTPException(500, f"BATCH_CRASHED: {crash_error}")

    rows = trade_db.get_orders_for_batch(batch_id)
    return {
        "batch_id":     batch_id,
        "status":       "ABORTED" if summary["aborted"] else "COMPLETED",
        "n_attempted":  summary["n_attempted"],
        "n_filled":     summary["n_filled"],
        "n_failed":     summary["n_failed"],
        "started_at":   started_at,
        "finished_at":  finished_at,
        "abort_reason": summary["abort_reason"],
        "orders":       rows,
    }


@router.post("/falcon/trade/mtf-check")
def mtf_check(req: MtfCheckRequest):
    """Bulk check MTF eligibility for a list of symbols. Used by the trade UI on
    initial load to populate the MTF column in the signals table."""
    kite = _get_kite()
    return {sym: mtf_eligibility.is_mtf_eligible(kite, sym) for sym in req.symbols}


@router.post("/falcon/trade/cancel-all")
def cancel_all_endpoint(req: CancelRequest):
    kite = _get_kite()
    return cancel_all(kite, req.batch_id)


@router.get("/falcon/trade/status")
def status(batch_id: str = Query(...)):
    run = trade_db.get_run(batch_id)
    if not run:
        raise HTTPException(404, "BATCH_NOT_FOUND")
    return {**run, "orders": trade_db.get_orders_for_batch(batch_id)}


@router.get("/falcon/trade/positions")
def positions():
    kite = _get_kite()
    held = holdings_fetch.get_held_positions(kite)
    sl_orders = trade_db.get_open_sl_orders()
    entry_dates = trade_db.get_entry_dates()

    today = date.today()
    positions_list: List[Dict[str, Any]] = []
    for symbol, h in held.items():
        sl = sl_orders.get(symbol)
        managed_by = "falcon" if sl else "external"

        entry_date = entry_dates.get(symbol) or h.get("entry_date")
        days_held: Optional[int] = h.get("days_held")
        if days_held is None and entry_date:
            try:
                days_held = (today - datetime.fromisoformat(entry_date).date()).days
            except Exception:
                days_held = None
        # IMPORTANT: keep None when truly unknown (external positions, no trade log).
        # Frontend renders "—" so the operator isn't misled by a fake "0".

        avg_entry     = h.get("avg_entry") or 0
        current_price = h.get("current_price") or 0
        target_price  = round(avg_entry * 1.10, 2) if avg_entry else 0
        tgt_dist_pct  = round(((target_price - current_price) / current_price) * 100, 2) if current_price else 0
        pnl_pct       = round(((current_price / avg_entry) - 1) * 100, 2) if avg_entry else 0

        if sl:
            # Falcon-managed: real SL order live on Kite
            sl_price          = sl["sl_price"] or 0
            sl_kite_order_id  = sl["kite_order_id"]
            sl_type           = sl["sl_type"]
        else:
            # Externally-placed position: no Falcon SL on Kite. Compute advisory SL = avg × 0.93 (-7%)
            sl_price          = round(avg_entry * 0.93, 2) if avg_entry else 0
            sl_kite_order_id  = None
            sl_type           = "ADVISORY"
        sl_dist_pct = round(((current_price - sl_price) / current_price) * 100, 2) if current_price else 0

        positions_list.append({
            "symbol":              symbol,
            "managed_by":          managed_by,           # "falcon" | "external"
            "qty":                 h["qty"],
            "avg_entry":           avg_entry,
            "current_price":       current_price,
            "product":             h["product"],
            "entry_date":          entry_date,           # ISO YYYY-MM-DD or None
            "days_held":           days_held,            # int or None (unknown for externals)
            "hold_days_max":       7,
            "sl_price":            sl_price,
            "sl_distance_pct":     sl_dist_pct,
            "sl_kite_order_id":    sl_kite_order_id,
            "sl_type":             sl_type,
            "target_price":        target_price,
            "target_distance_pct": tgt_dist_pct,
            "trail_active":        False,                 # Phase 2
            "trail_low_10d":       None,
            "pnl_pct":             pnl_pct,
        })

    total_notional = sum(p["qty"] * p["current_price"] for p in positions_list)
    total_entry    = sum(p["qty"] * p["avg_entry"] for p in positions_list)
    unrealized     = total_notional - total_entry
    pnl_pct_total  = round((unrealized / total_entry) * 100, 2) if total_entry else 0

    near_sl        = sorted(positions_list, key=lambda p: p["sl_distance_pct"])[:3]
    near_target    = sorted(positions_list, key=lambda p: p["target_distance_pct"])[:3]
    near_time_stop = [p for p in positions_list
                      if p["days_held"] is not None
                      and p["hold_days_max"] - p["days_held"] <= 2]

    return {
        "as_of":             datetime.now(IST).isoformat(),
        "n_positions":       len(positions_list),
        "total_notional":    total_notional,
        "total_entry_value": total_entry,
        "unrealized_pnl":    unrealized,
        "pnl_pct":           pnl_pct_total,
        "positions":         positions_list,
        "trigger_watch": {
            "near_sl":        [{"symbol": p["symbol"], "distance_pct": p["sl_distance_pct"]}     for p in near_sl],
            "near_target":    [{"symbol": p["symbol"], "distance_pct": p["target_distance_pct"]} for p in near_target],
            "near_time_stop": [{"symbol": p["symbol"], "days_held":    p["days_held"]}            for p in near_time_stop],
        },
    }


class TrailConfigUpdate(BaseModel):
    product:           str
    activate_pct:      float
    lock_pct:          float
    trail_sl_pct:      float
    trail_profit_pct:  float
    initial_sl_pct:    float
    hold_days_max:     int
    auto_exit_enabled: bool
    trail_method:      str = "percentage"   # 'percentage' | '10d_low'
    trail_lookback:    int = 10              # N completed trading sessions (10d_low only)


# ─── Engine Playbook (operator rules for staging new trades) ────────────────

class EngineConfigUpdate(BaseModel):
    per_trade_pct:       float
    daily_picks_max:     int
    skip_already_held:   bool
    mining_window_years: int = 4   # B4 publish cutoff: mined_year >= (current_year - this)


@router.get("/falcon/trade/engine-config")
def get_engine_config():
    """Return the playbook: per_trade_pct, daily_picks_max, skip_already_held,
    mining_window_years."""
    from ..services import engine_config  # noqa: WPS433
    return {"config": engine_config.get_config()}


@router.post("/falcon/trade/engine-config")
def post_engine_config(req: EngineConfigUpdate):
    """Save the playbook. Validates ranges."""
    from ..services import engine_config  # noqa: WPS433
    try:
        cfg = engine_config.upsert_config(
            per_trade_pct       = req.per_trade_pct,
            daily_picks_max     = req.daily_picks_max,
            skip_already_held   = req.skip_already_held,
            mining_window_years = req.mining_window_years,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "saved", "config": cfg}


@router.get("/falcon/trade/config")
def get_trail_config():
    """Return per-product trail-stop config (activate / lock / trail_sl / trail_profit
    plus initial SL%, hold days max, auto-exit toggle, and trail_method/lookback)."""
    return {"configs": trail_config.get_all_configs()}


@router.post("/falcon/trade/config")
def post_trail_config(req: TrailConfigUpdate):
    try:
        updated = trail_config.upsert_config(
            product           = req.product,
            activate_pct      = req.activate_pct,
            lock_pct           = req.lock_pct,
            trail_sl_pct      = req.trail_sl_pct,
            trail_profit_pct  = req.trail_profit_pct,
            initial_sl_pct    = req.initial_sl_pct,
            hold_days_max     = req.hold_days_max,
            auto_exit_enabled = req.auto_exit_enabled,
            trail_method      = req.trail_method,
            trail_lookback    = req.trail_lookback,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "saved", "config": updated}


@router.get("/falcon/trade/events")
def trade_events(limit: int = Query(50, ge=1, le=500),
                 since: Optional[str] = Query(None)):
    """Phase 2 — list trigger events emitted by the position monitor."""
    return {"events": position_monitor.list_events(limit=limit, since=since)}


@router.get("/falcon/trade/eod")
def eod_last():
    """Phase 2.2 — last end-of-day flush summary (counts of bumps/exits/partials/stale)."""
    from ..services import eod_flush  # noqa: WPS433
    return {"last": eod_flush.get_last_summary()}


@router.post("/falcon/trade/positions/tradebook")
async def positions_tradebook_upload(file: UploadFile = File(...)):
    """Upload a Zerodha tradebook .xlsx. Parses + FIFO-derives entry dates +
    upserts into falcon_position_first_seen. Returns parse summary."""
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(400, "Expected .xlsx file (Zerodha Console export)")
    data = await file.read()
    if not data:
        raise HTTPException(400, "Empty file")
    if len(data) > 10 * 1024 * 1024:  # 10 MB cap
        raise HTTPException(400, "File too large (max 10 MB)")
    from ..services import tradebook_import  # noqa: WPS433
    try:
        summary = tradebook_import.import_xlsx(data)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        log.exception("tradebook_import failed")
        raise HTTPException(500, f"Import failed: {type(e).__name__}: {e}")
    return summary


@router.get("/falcon/trade/ticker")
def ticker_status():
    """Phase 3 — KiteTicker WebSocket diagnostic. Reports connect status,
    subscribed symbol count, last tick timestamp, total tick count, etc."""
    from ..services import kite_ticker  # noqa: WPS433
    return kite_ticker.status()


@router.post("/falcon/trade/ticker/start")
def ticker_start(force: bool = Query(False)):
    """Manually (re)start the KiteTicker connection. Pass force=true after a
    token refresh to tear down the stale connection and reconnect with the
    new access_token (KiteTicker can't swap creds mid-session)."""
    from ..services import kite_ticker  # noqa: WPS433
    ok = kite_ticker.start(force=force)
    return {"started": bool(ok), "forced": force, "status": kite_ticker.status()}


@router.post("/falcon/trade/eod/run")
def eod_run_now():
    """Manually trigger the EOD flush (admin/test). Idempotent: returns existing
    summary if already run today; otherwise runs it now."""
    from ..services import eod_flush  # noqa: WPS433
    kite = _get_kite()
    if eod_flush._already_ran_today():
        return {"status": "already_ran", "last": eod_flush.get_last_summary()}
    summary = eod_flush.run_flush(kite)
    return {"status": "ran", "summary": summary}


# ─── Pre-market staging + deployer (Phase 2.3) ───────────────────────────────

class ItemIdsRequest(BaseModel):
    item_ids: List[int] = Field(default_factory=list)


class PremarketStageRequest(BaseModel):
    target_date: Optional[str] = None   # ISO date; None → next trading day


class PremarketItemPatch(BaseModel):
    sl_price: Optional[float] = None
    qty:      Optional[int]   = None


@router.get("/falcon/trade/premarket")
def premarket_list(target_date: Optional[str] = Query(None)):
    """List staging items for a target_date (default: next IST deploy window).
    Returns items grouped by kind + summary counts + token status."""
    from ..services import premarket_staging, eod_orchestrator  # noqa: WPS433
    if target_date is None:
        # Use IST clock — never local server time. Matches what eod_orchestrator
        # uses when staging, so list and stage are always aligned.
        target_date = eod_orchestrator._next_deploy_target_ist().isoformat()

    items = premarket_staging.list_for_date(target_date)
    summary = premarket_staging.summary_for_date(target_date)

    # Token status for the banner
    token_status = _kite_token_status_safe()

    return {
        "target_date":      target_date,
        "summary":          summary,
        "items":            items,
        "token_status":     token_status,
    }


@router.get("/falcon/trade/premarket/token-status")
def premarket_token_status():
    """Kite token freshness for the pre-market banner."""
    return _kite_token_status_safe()


@router.post("/falcon/trade/premarket/stage")
def premarket_stage_now(req: PremarketStageRequest):
    """Manually trigger the EOD orchestrator for a given target_date.
    Idempotent: re-running refreshes STAGED rows, leaves terminal rows alone."""
    from ..services import eod_orchestrator  # noqa: WPS433
    kite: Any = None
    try:
        kite = _get_kite()
    except HTTPException:
        kite = None  # Allow staging signals even without Kite (no held adopts then)
    summary = eod_orchestrator.run_eod(target_date=req.target_date, kite=kite)
    return summary


@router.post("/falcon/trade/premarket/confirm")
def premarket_confirm(req: ItemIdsRequest):
    """STAGED → QUEUED. Empty item_ids list = confirm-all-for-date."""
    from ..services import premarket_staging, eod_orchestrator  # noqa: WPS433
    if req.item_ids:
        n = premarket_staging.confirm_items(req.item_ids)
        return {"status": "confirmed", "n_confirmed": n}
    # Confirm-all path — IST-aware target date
    target_date = eod_orchestrator._next_deploy_target_ist().isoformat()
    n = premarket_staging.confirm_all_for_date(target_date)
    return {"status": "confirmed", "target_date": target_date, "n_confirmed": n}


@router.post("/falcon/trade/premarket/cancel")
def premarket_cancel(req: ItemIdsRequest):
    """Cancel = hard DELETE for STAGED|QUEUED rows. Terminal (DEPLOYED/FAILED)
    rows are preserved as the deploy history."""
    from ..services import premarket_staging  # noqa: WPS433
    if not req.item_ids:
        raise HTTPException(400, "item_ids required for cancel")
    n = premarket_staging.cancel_items(req.item_ids)
    return {"status": "cancelled", "n_removed": n}


@router.post("/falcon/trade/premarket/purge-cancelled")
def premarket_purge_cancelled():
    """One-shot cleanup of legacy soft-CANCELLED rows from before the cancel
    behavior was changed to hard-delete. Idempotent."""
    from ..services import premarket_staging  # noqa: WPS433
    n = premarket_staging.purge_cancelled_legacy()
    return {"status": "purged", "n_removed": n}


@router.post("/falcon/trade/reconcile")
def reconcile_kite_state(force_empty: bool = Query(False), dry_run: bool = Query(False)):
    """Reconcile Falcon DB ↔ Kite alive-orders. Zerodha auto-cancels Day-
    validity SL orders at 15:30 IST; this flips stale managed_by='falcon'
    rows back to 'external' so they can be re-staged via /premarket.

    Query params:
      * dry_run=true → report what would change without mutating
      * force_empty=true → operator-confirmed flip-all when Kite returns
        zero alive orders (typical post-EOD state). Use only when you
        KNOW Zerodha has wiped your SLs.
    """
    kite = _get_kite()
    from ..services import kite_reconcile  # noqa: WPS433
    if force_empty:
        return kite_reconcile.reconcile_force_all_stale(kite, dry_run=dry_run)
    return kite_reconcile.reconcile_managed_positions(kite, dry_run=dry_run)


@router.patch("/falcon/trade/premarket/items/{item_id}")
def premarket_patch_item(item_id: int, patch: PremarketItemPatch):
    """Edit an item's payload (e.g. override SL price). Only allowed on STAGED items."""
    from ..services import premarket_staging  # noqa: WPS433
    payload_patch: Dict[str, Any] = {}
    if patch.sl_price is not None:
        payload_patch["sl_price"] = patch.sl_price
    if patch.qty is not None:
        payload_patch["qty"] = patch.qty
    if not payload_patch:
        raise HTTPException(400, "no editable fields supplied")
    try:
        updated = premarket_staging.update_payload(item_id, payload_patch)
    except ValueError as e:
        raise HTTPException(409, str(e))
    if updated is None:
        raise HTTPException(404, f"item {item_id} not found")
    return {"status": "patched", "item": updated}


@router.post("/falcon/trade/premarket/deploy-now")
def premarket_deploy_now():
    """Force one deploy cycle right now (bypasses 9:14-9:30 IST window check).
    Operator-only safety override — useful for testing or recovery."""
    from ..services import premarket_deployer  # noqa: WPS433
    if not _autotrade_enabled():
        raise HTTPException(403, "FALCON_AUTOTRADE_ENABLED env var not set to 'true'")
    result = premarket_deployer.deploy_once_now()
    return {"status": "ran", "result": result}


@router.get("/falcon/trade/premarket/deployer-status")
def premarket_deployer_status():
    from ..services import premarket_deployer  # noqa: WPS433
    return premarket_deployer.status()


# ─── Routes that STAGE instead of firing directly ────────────────────────────

class StageFromPreviewRequest(BaseModel):
    preview_id: str


@router.post("/falcon/trade/premarket/stage-entries")
def premarket_stage_entries(req: StageFromPreviewRequest):
    """Take a preview's orders and stage each as a NEW_ENTRY item in pre-market.

    Replaces the direct-/place flow for Trade page: instead of firing on Kite
    immediately, orders go to falcon_premarket_staging where they wait for
    operator Confirm + 9:15 IST deployer.

    Idempotent: re-staging the same symbol on the same target_date refreshes
    the STAGED row (terminal rows are preserved)."""
    from ..services import premarket_staging  # noqa: WPS433
    p = _preview_store.get(req.preview_id)
    if not p:
        raise HTTPException(400, "PREVIEW_EXPIRED_OR_INVALID")

    # IST-aware target_date. Prefer the preview's frozen entry_date if present
    # (which was computed at /preview time from signal_date) — else derive from
    # current IST clock (next 9:15 deploy window).
    from ..services import eod_orchestrator  # noqa: WPS433
    target_date = p.get("entry_date") or eod_orchestrator._next_deploy_target_ist().isoformat()
    request_cfg = p.get("request") or {}

    staged: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for o in p.get("orders") or []:
        sym = o.get("symbol")
        if not sym:
            continue
        # Freeze EVERYTHING the deployer needs — qty, SL, target, margin info
        # so /premarket can show the full picture without re-running the preview.
        payload = {
            "symbol":             sym,
            "sector":             o.get("sector"),
            "rank":               o.get("rank"),
            "close_at_signal":    o.get("close"),
            "qty":                o.get("qty"),
            "notional":           o.get("notional"),
            "sl_price":           o.get("sl_price"),
            "target_price":       o.get("target_price"),
            "sl_order_type":      o.get("sl_order_type"),
            "sl_limit_price":     o.get("sl_limit_price"),
            "product":            o.get("product"),
            "margin_per_share":   o.get("margin_per_share"),
            "margin_required":    o.get("margin_required"),
            "margin_pct":         o.get("margin_pct"),
            "effective_leverage": o.get("effective_leverage"),
            "is_averaging":       o.get("is_averaging"),
            "existing_qty":       o.get("existing_qty"),
            "origin":             "TRADE_PAGE",
            # Config the deployer needs at 9:15 to re-plan if necessary.
            "config": {
                "total_capital":       request_cfg.get("total_capital"),
                "per_trade":           request_cfg.get("per_trade"),
                "hold_days":           request_cfg.get("hold_days") or 7,
                "sl_pct":              request_cfg.get("sl_pct") or -7.0,
                "trail_trigger_pct":   request_cfg.get("trail_trigger_pct") or 12.0,
                "trail_lookback_days": request_cfg.get("trail_lookback_days") or 10,
            },
        }
        try:
            row = premarket_staging.stage_item(
                target_date=target_date, kind="NEW_ENTRY",
                symbol=sym, payload=payload,
            )
            staged.append({"symbol": sym, "id": row.get("id"), "status": row.get("status")})
        except Exception as e:
            log.warning("stage-entries: %s failed: %s", sym, e)
            skipped.append({"symbol": sym, "reason": str(e)})

    return {
        "target_date":   target_date,
        "n_requested":   len(p.get("orders") or []),
        "n_staged":      len(staged),
        "n_skipped":     len(skipped),
        "staged":        staged,
        "skipped":       skipped,
        "preview_id":    req.preview_id,
    }


class StageAdoptItem(BaseModel):
    symbol:    str
    sl_price:  Optional[float] = None   # if None, deployer/orchestrator will compute from trail config


class StageAdoptsRequest(BaseModel):
    items: List[StageAdoptItem]


@router.post("/falcon/trade/premarket/stage-adopts")
def premarket_stage_adopts(req: StageAdoptsRequest):
    """Take a list of (symbol, sl_price) and stage each as a BULK_ADOPT item.

    Replaces the direct-/positions/bulk-adopt flow for the Positions page:
    instead of firing Kite SLs immediately, items go to falcon_premarket_staging.
    SL price can be omitted — deployer computes from trail config at 9:15."""
    from ..services import premarket_staging  # noqa: WPS433

    if not req.items:
        raise HTTPException(400, "items list required")

    kite = _get_kite()
    held = holdings_fetch.get_held_positions(kite, force_refresh=True)
    cfgs = {c["product"]: c for c in trail_config.get_all_configs()}
    # IST-aware target date — next 9:15 deploy window that hasn't passed.
    from ..services import eod_orchestrator  # noqa: WPS433
    target_date = eod_orchestrator._next_deploy_target_ist().isoformat()

    staged: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for it in req.items:
        sym = it.symbol
        h = held.get(sym)
        if not h or h["qty"] <= 0:
            skipped.append({"symbol": sym, "reason": "NOT_HELD"})
            continue
        product = h.get("product", "CNC")
        if product == "MIXED":
            skipped.append({"symbol": sym, "reason": "PRODUCT_MIXED"})
            continue

        cfg = cfgs.get(product)
        initial_sl_pct = cfg["initial_sl_pct"] if cfg else -7.0
        avg_entry = h.get("avg_entry") or 0
        if it.sl_price is not None:
            sl_price = float(it.sl_price)
            sl_basis = "operator_override"
        else:
            sl_price = avg_entry * (1 + initial_sl_pct / 100)
            sl_basis = "trail_config.initial_sl_pct"

        # Round to tick size
        try:
            tick = mtf_eligibility.get_tick_size(kite, sym)
        except Exception:
            tick = 0.05
        from ..services.services_round import round_to_tick_size
        sl_price = round_to_tick_size(sl_price, tick)

        payload = {
            "symbol":         sym,
            "qty":            h["qty"],
            "avg_entry":      avg_entry,
            "current_price":  h.get("current_price"),
            "product":        product,
            "sl_price":       sl_price,
            "sl_basis":       sl_basis,
            "initial_sl_pct": initial_sl_pct,
            "origin":         "POSITIONS_PAGE",
        }
        try:
            row = premarket_staging.stage_item(
                target_date=target_date, kind="BULK_ADOPT",
                symbol=sym, payload=payload,
            )
            staged.append({"symbol": sym, "id": row.get("id"), "status": row.get("status"),
                           "sl_price": sl_price, "qty": h["qty"], "product": product})
        except Exception as e:
            log.warning("stage-adopts: %s failed: %s", sym, e)
            skipped.append({"symbol": sym, "reason": str(e)})

    return {
        "target_date": target_date,
        "n_requested": len(req.items),
        "n_staged":    len(staged),
        "n_skipped":   len(skipped),
        "staged":      staged,
        "skipped":     skipped,
    }


def _kite_token_status_safe() -> Dict[str, Any]:
    try:
        from services.kite_auth import get_token_status  # noqa: WPS433
        return get_token_status()
    except Exception as e:
        return {"valid": False, "reason": f"check_failed: {e}"}


@router.get("/falcon/trade/monitor")
def monitor_status():
    """Phase 2 — monitor + per-position state (for the live trigger feed).
    `auto_exit_enabled` reflects the master kill switch (FALCON_AUTOTRADE_ENABLED)
    that gates trail_manager.process_position. Per-product gates live in
    falcon_trail_config.auto_exit_enabled — both must be ON for actions to fire."""
    states = position_monitor.list_states()
    # Use the same env var the trail_manager actually checks. Historical legacy
    # also accepts FALCON_AUTO_EXIT_ENABLED for badge-only display.
    auto_exit = (
        os.environ.get("FALCON_AUTOTRADE_ENABLED", "").lower() == "true"
        or os.environ.get("FALCON_AUTO_EXIT_ENABLED", "").lower() == "true"
    )
    return {
        "interval_sec":         position_monitor.POLL_INTERVAL_SEC,
        "auto_exit_enabled":    auto_exit,
        "n_tracked":            len(states),
        "states":               states,
    }


@router.post("/falcon/trade/positions/adopt")
def positions_adopt(req: PositionAdoptRequest):
    """Adopt an externally-placed position into Falcon management by placing a
    Falcon SL order on Kite. Once placed, the position becomes managed_by=falcon
    and the auto-exit logic engages on the next monitor poll.

    Safety:
      - Market must be open (Kite rejects SL placement outside hours).
      - Operator must pass confirm=true.
      - Computes SL preview if confirm=false and returns it without placing.
    """
    if not _market_is_open_now():
        raise HTTPException(403, "MARKET_CLOSED: SL orders can only be placed 9:15–15:30 IST on weekdays")

    kite = _get_kite()
    try:
        held = holdings_fetch.get_held_positions(kite, force_refresh=True)
    except Exception as e:
        log.exception("adopt: holdings_fetch failed")
        raise HTTPException(502, f"HOLDINGS_FETCH_FAILED: {type(e).__name__}: {e}")
    h = held.get(req.symbol)
    if not h or h["qty"] <= 0:
        raise HTTPException(404, f"NOT_HELD: {req.symbol} not in your holdings")

    # Already adopted?
    try:
        if trade_db.get_open_sl_order(req.symbol):
            raise HTTPException(409, f"ALREADY_ADOPTED: {req.symbol} already has a live Falcon SL order")
    except HTTPException:
        raise
    except Exception as e:
        log.exception("adopt: get_open_sl_order failed")
        raise HTTPException(500, f"ADOPT_DB_CHECK_FAILED: {type(e).__name__}: {e}")

    avg_entry = h.get("avg_entry") or 0
    current_price = h.get("current_price") or 0
    product = h.get("product", "CNC")
    if product == "MIXED":
        raise HTTPException(400, f"PRODUCT_MIXED: {req.symbol} has both CNC + MTF holdings; resolve manually first")

    # Pull product config for default SL%
    try:
        cfg = trail_config.get_config(product)
    except Exception as e:
        log.exception("adopt: trail_config.get_config failed")
        raise HTTPException(500, f"TRAIL_CONFIG_LOAD_FAILED: {type(e).__name__}: {e}")
    initial_sl_pct = cfg["initial_sl_pct"] if cfg else -7.0

    # Per-stock tick + lower-circuit guard (BOTH trigger and limit must clear circuit)
    tick = mtf_eligibility.get_tick_size(kite, req.symbol)
    circuits = holdings_fetch.get_circuit_limits(kite, [req.symbol])
    lower_circuit = circuits.get(req.symbol, {}).get("lower", 0)

    from ..services.services_round import round_to_tick_size
    if req.sl_price is not None:
        raw_sl = req.sl_price
    else:
        anchor = avg_entry if req.sl_basis == "entry" else current_price
        raw_sl = anchor * (1 + initial_sl_pct / 100)
    if lower_circuit > 0 and raw_sl <= lower_circuit:
        raw_sl = lower_circuit + tick
    sl_price = round_to_tick_size(raw_sl, tick)
    raw_limit = max(sl_price * 0.998, (lower_circuit + tick) if lower_circuit > 0 else 0)
    sl_limit = round_to_tick_size(raw_limit, tick)
    if sl_limit > sl_price:
        sl_limit = sl_price

    # Compute preview info
    distance_pct = ((current_price - sl_price) / current_price * 100) if current_price else 0
    will_trigger_immediately = current_price <= sl_price

    # If just previewing, don't place
    if not req.confirm:
        return {
            "preview":           True,
            "symbol":             req.symbol,
            "qty":                h["qty"],
            "avg_entry":          avg_entry,
            "current_price":      current_price,
            "product":            product,
            "computed_sl_price":  sl_price,
            "computed_sl_limit":  sl_limit,
            "distance_to_sl_pct": round(distance_pct, 2),
            "will_trigger_immediately": will_trigger_immediately,
            "warning":  ("Position is BELOW the computed SL — adopting will fire BREACHED_SL on next poll → auto-exit at market"
                          if will_trigger_immediately else None),
        }

    # Confirmed: place real Kite SL order
    if not _autotrade_enabled():
        raise HTTPException(403, "FALCON_AUTOTRADE_ENABLED env var not set to 'true'")

    today_iso = date.today().isoformat().replace("-", "_")
    batch_id = f"adopt_{today_iso}_{secrets.token_hex(3)}"
    started_at = datetime.now(IST).isoformat()

    # Create a synthetic run row so audit + has_active_batch checks remain coherent.
    try:
        trade_db.create_run(
            batch_id            = batch_id,
            signal_date         = date.today().isoformat(),
            entry_date          = date.today().isoformat(),
            selected_symbols    = [req.symbol],
            total_capital       = 0,
            per_trade           = int(avg_entry * h["qty"]),
            use_leverage        = (product == "MTF"),
            hold_days           = (cfg["hold_days_max"] if cfg else 7),
            sl_pct              = initial_sl_pct,
            trail_trigger_pct   = (cfg["activate_pct"] if cfg else 10.0),
            trail_lookback_days = 10,
            started_at          = started_at,
        )
    except Exception as e:
        log.exception("adopt: create_run failed")
        raise HTTPException(500, f"ADOPT_DB_CREATE_RUN_FAILED: {type(e).__name__}: {e}")

    # Idempotency tag — adopt batches use 'a' role char
    sym_short = req.symbol[:8].lower().replace("-", "").replace("&", "")
    tag = f"f-adopt-{sym_short}"[:20]

    try:
        kite_order_id = kite.place_order(
            variety           = kite.VARIETY_REGULAR,
            exchange          = kite.EXCHANGE_NSE,
            tradingsymbol     = req.symbol,
            transaction_type  = kite.TRANSACTION_TYPE_SELL,
            quantity          = h["qty"],
            product           = "MTF" if product == "MTF" else kite.PRODUCT_CNC,
            order_type        = kite.ORDER_TYPE_SL,
            price             = sl_limit,
            trigger_price     = sl_price,
            tag               = tag,
        )
    except Exception as e:
        log.exception("adopt place_order failed for %s", req.symbol)
        try:
            trade_db.finalize_run(
                batch_id=batch_id, status="ABORTED",
                n_attempted=1, n_filled=0, n_failed=1,
                n_overlap=0, n_skip_action=0, n_average_action=0,
                total_deployed=0, finished_at=datetime.now(IST).isoformat(),
                notes=f"adopt place_order failed: {e}",
            )
        except Exception:
            log.exception("finalize_run cleanup failed")
        raise HTTPException(500, f"ADOPT_PLACE_FAILED: {type(e).__name__}: {e}")

    # Kite SL is LIVE from this point forward. If anything below fails we MUST NOT
    # raise 500 — that would orphan a live SL on Kite while telling the operator
    # nothing happened, leading to a duplicate adopt → duplicate SL on retry.
    db_warning: Optional[str] = None
    try:
        trade_db.insert_order(
            batch_id        = batch_id,
            symbol          = req.symbol,
            side            = "SELL",
            role            = "STOP",
            qty             = h["qty"],
            idempotency_key = tag,
            kite_order_id   = str(kite_order_id),
            price           = sl_limit,
            trigger_price   = sl_price,
            product         = product,
            order_type      = "SL-L",
            status          = "PLACED",
        )
    except Exception as e:
        log.exception("adopt insert_order failed AFTER successful Kite SL placement (kite_id=%s)", kite_order_id)
        db_warning = f"DB record failed but Kite SL is LIVE (kite_id={kite_order_id}): {type(e).__name__}: {e}"

    try:
        trade_db.finalize_run(
            batch_id=batch_id, status="COMPLETED",
            n_attempted=1, n_filled=1, n_failed=0,
            n_overlap=0, n_skip_action=0, n_average_action=0,
            total_deployed=int(avg_entry * h["qty"]),
            finished_at=datetime.now(IST).isoformat(),
            notes=("position adopted into Falcon management"
                   if not db_warning else f"adopted but: {db_warning}"),
        )
    except Exception as e:
        log.exception("adopt finalize_run failed AFTER successful Kite SL placement")
        db_warning = (db_warning or "") + f" | finalize_run failed: {type(e).__name__}: {e}"

    return {
        "preview":          False,
        "status":           "ADOPTED" if not db_warning else "ADOPTED_WITH_WARNING",
        "symbol":           req.symbol,
        "qty":              h["qty"],
        "sl_price":         sl_price,
        "sl_limit":         sl_limit,
        "kite_order_id":    str(kite_order_id),
        "batch_id":         batch_id,
        "managed_by":       "falcon",
        "message":          (f"{req.symbol} now Falcon-managed. Auto-exit engages on next monitor poll (≤60s)."
                             if not db_warning else
                             f"Kite SL placed (kite_id {kite_order_id}) but DB warning: {db_warning}"),
        "warning":          db_warning,
    }


@router.post("/falcon/trade/positions/bulk-adopt")
def positions_bulk_adopt(req: BulkAdoptRequest):
    """Adopt multiple external positions in one operation. Each gets its own
    Falcon SL on Kite, all share a single batch_id for audit grouping.

    Two-pass: confirm=false returns preview (what would be placed); confirm=true
    actually places. Per-item errors are captured without aborting the whole bulk.

    Handles per-stock tick size + lower-circuit constraints automatically — the
    computed SL is rounded to the symbol's actual tick and raised above the lower
    circuit limit (plus 1 tick buffer) when needed."""

    if req.confirm and not _market_is_open_now():
        raise HTTPException(403, "MARKET_CLOSED: SL orders can only be placed 9:15–15:30 IST on weekdays")

    kite = _get_kite()
    held = holdings_fetch.get_held_positions(kite, force_refresh=True)
    cfgs = {c["product"]: c for c in trail_config.get_all_configs()}

    # Pre-fetch circuit limits for all requested symbols in ONE quote() call
    requested_syms = [it.symbol for it in req.items]
    circuits = holdings_fetch.get_circuit_limits(kite, requested_syms)

    previews: List[Dict[str, Any]] = []
    placed:   List[Dict[str, Any]] = []
    failed:   List[Dict[str, Any]] = []

    # Build the batch_id once if confirm=true; share across all positions
    bulk_batch_id = None
    if req.confirm:
        today_iso = date.today().isoformat().replace("-", "_")
        bulk_batch_id = f"adopt_bulk_{today_iso}_{secrets.token_hex(3)}"
        symbols_only = [it.symbol for it in req.items]
        trade_db.create_run(
            batch_id            = bulk_batch_id,
            signal_date         = date.today().isoformat(),
            entry_date          = date.today().isoformat(),
            selected_symbols    = symbols_only,
            total_capital       = 0,
            per_trade           = 0,
            use_leverage        = False,
            hold_days           = 7,
            sl_pct              = -7.0,
            trail_trigger_pct   = 10.0,
            trail_lookback_days = 10,
            started_at          = datetime.now(IST).isoformat(),
        )

    if req.confirm and not _autotrade_enabled():
        raise HTTPException(403, "FALCON_AUTOTRADE_ENABLED env var not set to 'true'")

    # Pre-fetch live Kite-side SELL SL orders ONCE for all requested symbols.
    # Used to skip duplicate placements when an SL already exists at Kite —
    # we record/refresh the DB row to point at the existing kite_order_id
    # rather than placing a 2nd order (which would 2x-lock qty).
    live_kite_sls: Dict[str, Dict[str, Any]] = {}
    try:
        all_kite_orders = kite.orders() if req.confirm else []
    except Exception as e:
        log.warning("bulk_adopt: kite.orders() failed during pre-scan: %s — proceeding without dedupe", e)
        all_kite_orders = []
    requested_set = {it.symbol for it in req.items}
    for o in all_kite_orders:
        if o.get("tradingsymbol") not in requested_set:
            continue
        if o.get("transaction_type") != "SELL":
            continue
        if o.get("order_type") not in ("SL", "SL-M"):
            continue
        # Live = TRIGGER PENDING or OPEN. Filled / Cancelled / Rejected are not live.
        if o.get("status") not in ("TRIGGER PENDING", "OPEN"):
            continue
        sym = o["tradingsymbol"]
        # Keep first match per symbol (most recent comes first in kite.orders())
        if sym not in live_kite_sls:
            live_kite_sls[sym] = {
                "order_id":      str(o.get("order_id")),
                "trigger_price": float(o.get("trigger_price") or 0),
                "limit_price":   float(o.get("price") or 0),
                "order_type":    o.get("order_type"),
                "qty":           int(o.get("quantity") or 0),
                "product":       o.get("product"),
            }
    if live_kite_sls:
        log.info("bulk_adopt: pre-scan found %d live Kite SLs for %d requested symbols",
                 len(live_kite_sls), len(requested_set))

    for it in req.items:
        sym = it.symbol
        h = held.get(sym)
        if not h or h["qty"] <= 0:
            failed.append({"symbol": sym, "error": "NOT_HELD"})
            continue
        # Falcon DB already tracks this symbol's live SL? → skip cleanly.
        # Distinct from the Kite-side dedupe below (DB sync re-does that path).
        existing_db = trade_db.get_open_sl_order(sym)
        if existing_db:
            placed.append({
                "symbol":         sym,
                "kite_order_id":  existing_db.get("kite_order_id"),
                "sl_price":       existing_db.get("sl_price"),
                "qty":            h["qty"],
                "status":         "ALREADY_ADOPTED",
            })
            continue
        product = h.get("product", "CNC")
        if product == "MIXED":
            failed.append({"symbol": sym, "error": "PRODUCT_MIXED"})
            continue

        cfg = cfgs.get(product)
        initial_sl_pct = cfg["initial_sl_pct"] if cfg else -7.0
        avg_entry = h.get("avg_entry") or 0
        current_price = h.get("current_price") or 0

        # Per-stock tick (NSE varies: 0.05 / 0.10 / 1.00)
        tick = mtf_eligibility.get_tick_size(kite, sym)

        # Compute SL — caller may override
        if it.sl_price is not None:
            raw_sl = it.sl_price
        else:
            raw_sl = avg_entry * (1 + initial_sl_pct / 100)

        # Lower circuit guard: Kite rejects SL where EITHER trigger price or limit
        # price is ≤ lower_circuit_limit. We must clamp BOTH above the circuit.
        circuit = circuits.get(sym, {})
        lower_circuit = circuit.get("lower", 0)
        if lower_circuit > 0 and raw_sl <= lower_circuit:
            raw_sl = lower_circuit + tick
            sl_adjusted_for_circuit = True
        else:
            sl_adjusted_for_circuit = False

        from ..services.services_round import round_to_tick_size
        sl_price = round_to_tick_size(raw_sl, tick)
        # Limit = trigger × 0.998 by default, but must also stay above circuit.
        raw_limit = max(sl_price * 0.998, (lower_circuit + tick) if lower_circuit > 0 else 0)
        sl_limit = round_to_tick_size(raw_limit, tick)
        # Edge case: ensure limit ≤ trigger (Kite requires this for SL SELL)
        if sl_limit > sl_price:
            sl_limit = sl_price

        will_trigger = current_price <= sl_price
        activate_threshold = avg_entry * (1 + (cfg["activate_pct"] if cfg else 10.0) / 100)
        will_trail_activate = current_price >= activate_threshold

        item_preview = {
            "symbol":              sym,
            "product":             product,
            "qty":                 h["qty"],
            "avg_entry":           avg_entry,
            "current_price":       current_price,
            "sl_price":            sl_price,
            "sl_limit":            sl_limit,
            "tick_size":           tick,
            "lower_circuit":       lower_circuit,
            "sl_adjusted_for_circuit": sl_adjusted_for_circuit,
            "distance_to_sl_pct":  round(((current_price - sl_price) / current_price * 100) if current_price else 0, 2),
            "will_trigger_immediately": will_trigger,
            "will_trail_activate":      will_trail_activate,
        }

        if not req.confirm:
            previews.append(item_preview)
            continue

        # confirm=true: place new SL — OR adopt the one already live at Kite.
        # Idempotency tag: include batch_id hex so retries with a new batch_id
        # never collide with stale rows in falcon_trade_orders. Same tag for
        # both paths (live-adopt and fresh-place) — distinguished by status.
        from ..services.order_executor import idempotency_key
        tag = idempotency_key(bulk_batch_id, sym, "STOP")

        existing_sl = live_kite_sls.get(sym)
        if existing_sl:
            # Path A: live Kite SL already exists for this symbol — adopt it
            # in DB (no duplicate place_order, which would 2x-lock qty).
            try:
                trade_db.insert_order(
                    batch_id        = bulk_batch_id,
                    symbol          = sym,
                    side            = "SELL",
                    role            = "STOP",
                    qty             = h["qty"],
                    idempotency_key = tag,
                    kite_order_id   = existing_sl["order_id"],
                    price           = existing_sl["limit_price"] or sl_limit,
                    trigger_price   = existing_sl["trigger_price"] or sl_price,
                    product         = product,
                    order_type      = existing_sl["order_type"] or "SL",
                    status          = "PLACED",
                )
                placed.append({
                    **item_preview,
                    "kite_order_id": existing_sl["order_id"],
                    "sl_price":      existing_sl["trigger_price"] or sl_price,
                    "sl_limit":      existing_sl["limit_price"] or sl_limit,
                    "status":        "ADOPTED_EXISTING_KITE_SL",
                })
            except Exception as e:
                # Most likely a UNIQUE collision because a row for this tag
                # already exists. Treat as success — the DB knows about this
                # SL, monitor will pick it up on next poll.
                log.info("bulk_adopt: %s already in DB (tag=%s): %s — treating as ALREADY_ADOPTED", sym, tag, e)
                placed.append({
                    **item_preview,
                    "kite_order_id": existing_sl["order_id"],
                    "status":        "ALREADY_ADOPTED",
                })
            continue

        # Path B: no live Kite SL — place a fresh one.
        try:
            kite_order_id = kite.place_order(
                variety           = kite.VARIETY_REGULAR,
                exchange          = kite.EXCHANGE_NSE,
                tradingsymbol     = sym,
                transaction_type  = kite.TRANSACTION_TYPE_SELL,
                quantity          = h["qty"],
                product           = "MTF" if product == "MTF" else kite.PRODUCT_CNC,
                order_type        = kite.ORDER_TYPE_SL,
                price             = sl_limit,
                trigger_price     = sl_price,
                tag               = tag,
            )
            trade_db.insert_order(
                batch_id        = bulk_batch_id,
                symbol          = sym,
                side            = "SELL",
                role            = "STOP",
                qty             = h["qty"],
                idempotency_key = tag,
                kite_order_id   = str(kite_order_id),
                price           = sl_limit,
                trigger_price   = sl_price,
                product         = product,
                order_type      = "SL-L",
                status          = "PLACED",
            )
            placed.append({**item_preview, "kite_order_id": str(kite_order_id), "status": "ADOPTED"})
        except Exception as e:
            failed.append({**item_preview, "error": str(e)})

    if req.confirm and bulk_batch_id:
        trade_db.finalize_run(
            batch_id         = bulk_batch_id,
            status           = "COMPLETED" if placed and not failed
                                 else ("ABORTED" if not placed else "COMPLETED"),
            n_attempted      = len(req.items),
            n_filled         = len(placed),
            n_failed         = len(failed),
            n_overlap        = 0,
            n_skip_action    = 0,
            n_average_action = 0,
            total_deployed   = sum(int(p["qty"] * p["avg_entry"]) for p in placed),
            finished_at      = datetime.now(IST).isoformat(),
            notes            = f"bulk adopt: {len(placed)} placed, {len(failed)} failed",
        )

    return {
        "preview":        not req.confirm,
        "batch_id":       bulk_batch_id,
        "n_requested":    len(req.items),
        "previews":       previews,
        "placed":         placed,
        "failed":         failed,
    }


@router.post("/falcon/trade/positions/exit")
def positions_exit(req: PositionExitRequest):
    if not req.confirm:
        raise HTTPException(400, "CONFIRM_REQUIRED")

    kite = _get_kite()
    sl = trade_db.get_open_sl_order(req.symbol)
    if not sl:
        raise HTTPException(404, f"NO_LIVE_SL_FOR_{req.symbol}")

    # Cancel SL first
    try:
        kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=sl["kite_order_id"])
        trade_db.update_order_status(sl["id"], "CANCELLED")
    except Exception as e:
        raise HTTPException(500, f"SL_CANCEL_FAILED: {e}")

    held = holdings_fetch.get_held_positions(kite, force_refresh=True)
    h = held.get(req.symbol)
    if not h or h["qty"] <= 0:
        raise HTTPException(404, f"NOT_HELD_{req.symbol}")

    if not _autotrade_enabled():
        raise HTTPException(403, "FALCON_AUTOTRADE_ENABLED not set to 'true'")

    exit_tag = f"f-exit-{req.symbol[:8].lower().replace('-','').replace('&','')}"[:20]
    try:
        exit_kite_order_id = kite.place_order(
            variety            = kite.VARIETY_REGULAR,
            exchange           = kite.EXCHANGE_NSE,
            tradingsymbol      = req.symbol,
            transaction_type   = kite.TRANSACTION_TYPE_SELL,
            quantity           = h["qty"],
            product            = ("MTF" if sl["product"] == "MTF" else kite.PRODUCT_CNC),
            order_type         = kite.ORDER_TYPE_MARKET,
            market_protection  = 1.0,   # Zerodha API rejects naked MARKET orders
            tag                = exit_tag,
        )
    except Exception as e:
        raise HTTPException(500, f"EXIT_PLACE_FAILED: {e}")

    trade_db.insert_order(
        batch_id        = sl["batch_id"],
        symbol          = req.symbol,
        side            = "SELL",
        role            = "EXIT",
        qty             = h["qty"],
        idempotency_key = exit_tag,
        product         = sl["product"],
        order_type      = "MARKET",
        status          = "PLACED",
        kite_order_id   = str(exit_kite_order_id),
    )

    return {
        "status":             "EXITED",
        "symbol":             req.symbol,
        "cancelled_sl":       sl["kite_order_id"],
        "exit_kite_order_id": str(exit_kite_order_id),
        "exit_qty":           h["qty"],
        "exit_price":         h["current_price"],
    }
