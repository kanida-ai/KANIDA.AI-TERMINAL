"""Daily Trade Journal endpoint.

GET /autotrade/session/{session_id}/journal

Returns a structured per-session trade journal built from autotrade_sessions +
autotrade_positions.  The journal is available for ANY session status (CREATED,
RUNNING, CLOSED) — open positions contribute unrealised_pnl; closed positions
contribute realised_pnl.  Returns 404 if the session is not found.

Auth: operator-token gated at the router level (inherited from autotrade_routes).
DB:   reads ONLY autotrade_sessions + autotrade_positions (never falcon_position_state).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.journal")
IST = timezone(timedelta(hours=5, minutes=30))


# ── Review flag constants ─────────────────────────────────────────────────────

_FLAG_STOP_RECOVERED       = "STOP_RECOVERED"
_FLAG_CONTINUED_DECLINE    = "CONTINUED_DECLINE_OPEN"
_FLAG_DEEP_LOSS            = "DEEP_LOSS"
_FLAG_WINNER_OPEN          = "WINNER_OPEN"
_FLAG_LARGE_WIN            = "LARGE_WIN"


# ── Internal helpers ──────────────────────────────────────────────────────────

def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 datetime string (with or without tz) to a naive UTC
    datetime, or None if s is falsy / unparseable."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _hold_minutes(opened_at: Optional[str],
                  closed_at: Optional[str]) -> Optional[float]:
    """Minutes between open and close (float), or None if either is missing."""
    t0 = _parse_dt(opened_at)
    t1 = _parse_dt(closed_at)
    if t0 is None or t1 is None:
        return None
    delta = t1 - t0
    return round(delta.total_seconds() / 60.0, 1)


def _review_flag(pos: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """Compute (flag, note) for one position row.

    Priority order (first match wins):
      DEEP_LOSS           pnl_pct < -4%
      LARGE_WIN           pnl_pct > 3%
      STOP_RECOVERED      closed via GTT-stop (exit_price < avg_price)
      CONTINUED_DECLINE   open + ltp < avg * 0.985
      WINNER_OPEN         open + ltp > avg * 1.01
    """
    status      = (pos.get("status") or "").upper()
    close_reason = pos.get("close_reason") or ""
    avg_price   = pos.get("avg_price") or 0.0
    exit_price  = pos.get("exit_price")
    ltp         = pos.get("ltp")
    pnl_pct     = pos.get("pnl_pct") or 0.0

    if avg_price <= 0:
        return None, None

    if pnl_pct < -4.0:
        return _FLAG_DEEP_LOSS, "Loss exceeded -4% — review stop placement"

    if pnl_pct > 3.0:
        return _FLAG_LARGE_WIN, "Strong exit"

    if status == "CLOSED" and close_reason == "GTT":
        # GTT heuristic: exit_price < avg_price → stop leg (price fell to trigger)
        if exit_price is not None and float(exit_price) < float(avg_price):
            return (_FLAG_STOP_RECOVERED,
                    "Stopped out — verify if price recovered after exit")

    if status == "OPEN" and ltp is not None:
        ltp_f = float(ltp)
        avg_f = float(avg_price)
        if ltp_f < avg_f * 0.985:
            return (_FLAG_CONTINUED_DECLINE,
                    "Position open and declining, approaching stop")
        if ltp_f > avg_f * 1.01:
            return _FLAG_WINNER_OPEN, "Open winner above +1% — trail watch"

    return None, None


def _build_position_row(p: Dict[str, Any],
                        session_product: str = "CNC") -> Dict[str, Any]:
    """Build the per-position journal dict from a raw DB row.

    session_product: the session's order_product (CNC/MIS/NRML/MTF), used ONLY by
    the GUARD G4 net-P&L estimate (autotrade_positions has no per-row product
    column — the reconciler likewise resolves product from the session config)."""
    qty       = int(p.get("qty") or 0)
    avg_price = float(p.get("avg_price") or 0.0)
    invested  = round(qty * avg_price, 2)

    sl        = p.get("sl_level")
    target    = p.get("target_price")
    sl_pct    = (
        round((float(sl) - avg_price) / avg_price * 100, 4)
        if sl is not None and avg_price > 0 else None
    )
    target_pct = (
        round((float(target) - avg_price) / avg_price * 100, 4)
        if target is not None and avg_price > 0 else None
    )

    status    = (p.get("status") or "").upper()
    exit_p    = p.get("exit_price")
    r_pnl     = p.get("realised_pnl")
    u_pnl     = p.get("unrealised_pnl")

    # pnl_rs: realised if closed, else unrealised (or 0 if neither available)
    pnl_rs = (float(r_pnl) if r_pnl is not None else
               float(u_pnl) if u_pnl is not None else 0.0)
    pnl_pct = round(pnl_rs / invested * 100, 4) if invested > 0 else 0.0

    # ── GUARD G4 (mode F4): GROSS vs NET P&L clarity ──────────────────────────
    # The existing realised_pnl is GROSS (no charges). ADD an ESTIMATED net for
    # CLOSED positions: gross − estimated charges (brokerage/STT/txn/GST/stamp/DP).
    # We NEVER rename or alter the gross field; net is additive + clearly labelled
    # an estimate. product comes from the position row's stored `product` (falls
    # back to the session product resolved by the caller via `_product`).
    charges_est: Optional[Dict[str, Any]] = None
    realised_pnl_net: Optional[float] = None
    if status == "CLOSED" and r_pnl is not None and exit_p is not None and \
            avg_price > 0 and qty > 0:
        from ..charges import estimate_charges
        buy_value  = qty * avg_price
        sell_value = qty * float(exit_p)
        charges_est = estimate_charges(
            product=session_product,
            buy_value=buy_value, sell_value=sell_value, legs=2)
        realised_pnl_net = round(float(r_pnl) - charges_est["total"], 2)

    row: Dict[str, Any] = {
        "symbol":        p.get("symbol"),
        "exchange":      p.get("exchange"),
        "qty":           qty,
        "avg_price":     avg_price,
        "invested_rs":   invested,
        "sl_level":      float(sl) if sl is not None else None,
        "target_price":  float(target) if target is not None else None,
        "sl_pct":        sl_pct,
        "target_pct":    target_pct,
        "gtt_stop":      p.get("gtt_stop"),
        "gtt_target":    p.get("gtt_target"),
        "ltp":           p.get("ltp"),
        "status":        status,
        "exit_price":    float(exit_p) if exit_p is not None else None,
        # realised_pnl is GROSS (no charges) — UNCHANGED field. GUARD G4 adds the
        # net estimate + a mirror gross label WITHOUT touching this key.
        "realised_pnl":       float(r_pnl) if r_pnl is not None else None,
        "realised_pnl_gross": float(r_pnl) if r_pnl is not None else None,
        "realised_pnl_net":   realised_pnl_net,   # ESTIMATE (gross − est charges)
        "charges_estimate":   charges_est,        # breakdown dict or None
        "unrealised_pnl": float(u_pnl) if u_pnl is not None else None,
        "pnl_rs":        round(pnl_rs, 2),
        "pnl_pct":       pnl_pct,
        "close_reason":  p.get("close_reason"),
        "opened_at":     p.get("opened_at"),
        "closed_at":     p.get("closed_at"),
        "hold_minutes":  _hold_minutes(p.get("opened_at"), p.get("closed_at")),
        "review_flag":   None,
        "review_note":   None,
    }

    # Attach review flag now that pnl_pct is available on the row.
    row["pnl_pct"] = pnl_pct   # already set, kept for clarity
    row_with_pnl = dict(row)
    flag, note = _review_flag(row_with_pnl)
    row["review_flag"] = flag
    row["review_note"] = note

    return row


def build_journal(session_id: str) -> Dict[str, Any]:
    """Build the full journal dict for session_id.  Raises HTTPException 404 if
    the session row is not found.  All reads are from autotrade_* tables only."""
    with falcon_conn() as con:
        sess_row = con.execute(
            "SELECT * FROM autotrade_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
        if sess_row is None:
            raise HTTPException(404, f"session {session_id!r} not found")

        pos_rows = con.execute(
            "SELECT * FROM autotrade_positions WHERE session_id=? "
            "ORDER BY opened_at ASC",
            (session_id,),
        ).fetchall()

    sess = dict(sess_row)
    positions_raw = [dict(r) for r in pos_rows]

    # ── Parse config_json for strategy metadata ──────────────────────────────
    cfg: Dict[str, Any] = {}
    try:
        raw_cfg = sess.get("config_json") or "{}"
        cfg = json.loads(raw_cfg) if isinstance(raw_cfg, str) else (raw_cfg or {})
    except (json.JSONDecodeError, TypeError):
        cfg = {}

    strategy    = cfg.get("strategy", cfg.get("name", "autotrade"))
    fund        = float(sess.get("total_allocated_capital") or 0.0)
    inv_basis   = float(sess.get("invested_basis") or 0.0) or fund
    leverage    = round(inv_basis / fund, 4) if fund > 0 else 1.0
    # GUARD G4: the session product drives the net-P&L charge estimate (the
    # positions table has no per-row product; EQ→CNC handled in charges.py).
    session_product = str(cfg.get("order_product") or "CNC")

    # ── Build per-position rows ───────────────────────────────────────────────
    pos_journal: List[Dict[str, Any]] = [
        _build_position_row(p, session_product) for p in positions_raw]

    # ── Aggregate summary ─────────────────────────────────────────────────────
    n_total   = len(pos_journal)
    closed    = [p for p in pos_journal if p["status"] == "CLOSED"]
    open_pos  = [p for p in pos_journal if p["status"] == "OPEN"]

    total_realised   = sum(p["realised_pnl"] or 0.0 for p in closed)
    total_unrealised = sum(p["unrealised_pnl"] or 0.0 for p in open_pos)
    total_pnl        = round(total_realised + total_unrealised, 2)

    # GUARD G4: session-level ESTIMATED charges + net realised. Additive; the
    # gross totals above are UNCHANGED. total_charges_est sums the per-position
    # estimates; realised falls back to gross when a row has no net estimate.
    total_charges_est = round(sum(
        (p.get("charges_estimate") or {}).get("total", 0.0) for p in closed), 2)
    total_realised_net = round(sum(
        (p["realised_pnl_net"] if p.get("realised_pnl_net") is not None
         else (p["realised_pnl"] or 0.0)) for p in closed), 2)

    n_winners = sum(1 for p in closed if (p["realised_pnl"] or 0.0) > 0)
    n_losers  = sum(1 for p in closed if (p["realised_pnl"] or 0.0) <= 0)

    # GTT heuristic: exit_price < avg_price → stop leg; ≥ avg_price → target leg.
    n_stop_hits   = 0
    n_target_hits = 0
    for p in closed:
        if p.get("close_reason") == "GTT":
            ep  = p.get("exit_price")
            avg = p.get("avg_price") or 0.0
            if ep is not None and avg > 0:
                if float(ep) < float(avg):
                    n_stop_hits += 1
                else:
                    n_target_hits += 1
            else:
                n_stop_hits += 1  # conservative: can't tell → count as stop

    n_trail_exits  = sum(1 for p in closed
                         if p.get("close_reason") in ("TRAIL_EXIT", "FLOOR_EXIT"))
    n_square_off   = sum(1 for p in closed
                         if p.get("close_reason") == "SQUARE_OFF")
    n_kill_switch  = sum(1 for p in closed
                         if p.get("close_reason") == "KILL_SWITCH")

    # Best / worst by realised_pnl (closed only).
    best_trade  = None
    worst_trade = None
    if closed:
        best_p  = max(closed, key=lambda p: p["realised_pnl"] or 0.0)
        worst_p = min(closed, key=lambda p: p["realised_pnl"] or 0.0)
        inv_b   = best_p["invested_rs"]
        inv_w   = worst_p["invested_rs"]
        best_trade = {
            "symbol":  best_p["symbol"],
            "pnl_rs":  round(best_p["realised_pnl"] or 0.0, 2),
            "pnl_pct": round((best_p["realised_pnl"] or 0.0) / inv_b * 100, 4)
                       if inv_b > 0 else 0.0,
        }
        worst_trade = {
            "symbol":  worst_p["symbol"],
            "pnl_rs":  round(worst_p["realised_pnl"] or 0.0, 2),
            "pnl_pct": round((worst_p["realised_pnl"] or 0.0) / inv_w * 100, 4)
                       if inv_w > 0 else 0.0,
        }

    hold_times = [p["hold_minutes"] for p in closed if p["hold_minutes"] is not None]
    avg_hold   = round(sum(hold_times) / len(hold_times), 1) if hold_times else None

    total_pnl_pct_inv  = round(total_pnl / inv_basis * 100, 4) if inv_basis > 0 else 0.0
    total_pnl_pct_fund = round(total_pnl / fund * 100, 4) if fund > 0 else 0.0

    # ── trading_date: date of started_at, or created_at, or today ────────────
    started_at_str = sess.get("started_at") or sess.get("created_at") or ""
    try:
        trading_date = started_at_str[:10]
    except Exception:
        trading_date = datetime.now(IST).date().isoformat()

    # ── Review items list ─────────────────────────────────────────────────────
    review_items = [
        {"symbol": p["symbol"], "flag": p["review_flag"], "note": p["review_note"]}
        for p in pos_journal
        if p["review_flag"] is not None
    ]

    return {
        "session_id":   session_id,
        "trading_date": trading_date,
        "strategy":     strategy,
        "mode":         sess.get("mode", "paper"),
        "session_summary": {
            "total_allocated_capital": fund,
            "invested_basis":          round(inv_basis, 2),
            "leverage":                leverage,
            "total_realised_pnl":       round(total_realised, 2),  # GROSS
            "total_realised_pnl_gross": round(total_realised, 2),
            "total_realised_pnl_net":   total_realised_net,        # ESTIMATE
            "total_charges_estimate":   total_charges_est,         # ESTIMATE
            "total_unrealised_pnl":    round(total_unrealised, 2),
            "total_pnl":               total_pnl,
            "total_pnl_pct_invested":  total_pnl_pct_inv,
            "total_pnl_pct_fund":      total_pnl_pct_fund,
            "n_positions":             n_total,
            "n_closed":                len(closed),
            "n_open":                  len(open_pos),
            "n_winners":               n_winners,
            "n_losers":                n_losers,
            "n_stop_hits":             n_stop_hits,
            "n_target_hits":           n_target_hits,
            "n_trail_exits":           n_trail_exits,
            "n_square_off":            n_square_off,
            "n_kill_switch":           n_kill_switch,
            "best_trade":              best_trade,
            "worst_trade":             worst_trade,
            "avg_hold_minutes":        avg_hold,
            "session_status":          sess.get("status", "UNKNOWN"),
            "entry_latency_ms":        sess.get("entry_latency_ms"),
            "exit_latency_ms":         sess.get("exit_latency_ms"),
        },
        "positions":     pos_journal,
        "review_items":  review_items,
    }
