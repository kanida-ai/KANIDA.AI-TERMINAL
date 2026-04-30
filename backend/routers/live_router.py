"""
KANIDA.AI Live Positions Router

A "live position" is defined as:
  1. The engine generated a signal and entry was executed (entry_date <= today).
  2. The trade is within its maximum holding window (MAX_HOLD_DAYS calendar days).
  3. Status is determined by comparing the CURRENT market price to TP / SL levels —
     NOT by the backtest simulation's exit date or exit reason.

This means a trade the backtest classified as "timeout" is re-evaluated here in real
time. If current_price >= target_price (long), it is TARGET HIT regardless of what
the simulation recorded.
"""
from __future__ import annotations

import json
import os
import sys
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, HTTPException

router  = APIRouter()
_HERE   = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent))
from db import get_conn

DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"),
)

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_HOLD_DAYS  = 28   # calendar days ≈ 20 NSE trading days
NEAR_TARGET_PCT = 3.0  # % from target → NEAR TARGET
NEAR_STOP_PCT   = 2.0  # % from stop   → NEAR STOP


# ── Helpers ───────────────────────────────────────────────────────────────────

def _conn():
    return get_conn()


def _n(notes_str) -> dict:
    try:
        return json.loads(notes_str) if notes_str else {}
    except Exception:
        return {}


def _market_info() -> dict:
    """NSE market status in IST (UTC+05:30)."""
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    wd = now_ist.weekday()

    open_t  = now_ist.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_t = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    pre_t   = now_ist.replace(hour=9,  minute=0,  second=0, microsecond=0)

    if wd >= 5:
        status, label = "closed", "Weekend — Market Closed"
    elif now_ist < pre_t:
        status, label = "pre",    "Pre-Market"
    elif now_ist < open_t:
        status, label = "pre",    "Pre-Open (09:00–09:15 IST)"
    elif now_ist <= close_t:
        status, label = "open",   "Market Open"
    else:
        status, label = "closed", "After Hours"

    mins_to_open = mins_to_close = None
    if status == "pre":
        mins_to_open  = max(0, int((open_t  - now_ist).total_seconds() / 60))
    elif status == "open":
        mins_to_close = max(0, int((close_t - now_ist).total_seconds() / 60))

    return {
        "status":             status,
        "label":              label,
        "ist_time":           now_ist.strftime("%H:%M:%S"),
        "ist_date":           now_ist.strftime("%Y-%m-%d"),
        "ist_datetime":       now_ist.strftime("%Y-%m-%d %H:%M IST"),
        "is_open":            status == "open",
        "mins_to_open":       mins_to_open,
        "mins_to_close":      mins_to_close,
        "refresh_interval_s": 30 if status == "open" else 180,
    }


def _get_prices(tickers: list[str]) -> dict[str, float | None]:
    if not tickers:
        return {}
    try:
        import sys
        sys.path.insert(0, str(_HERE.parent))
        from feeds.price_feed import get_current_prices
        return get_current_prices(tickers)
    except Exception:
        return {t: None for t in tickers}


def _live_status(
    entry_date: str,
    entry_price: float,
    stop_price: float,
    target_price: float,
    current_price: float | None,
    direction: str,
    days_open: int,
) -> tuple[str, str]:
    """
    Determine the REAL live status of a position based on current market price.
    The backtest simulation's exit_date / exit_reason are intentionally ignored.
    """
    today = date.today().isoformat()

    # ── Not yet entered ──────────────────────────────────────────────────────
    if not entry_date or entry_date > today:
        return "pending_entry", "PENDING ENTRY"

    # ── Max holding period exceeded ──────────────────────────────────────────
    if days_open >= MAX_HOLD_DAYS:
        return "expired", "EXPIRED"

    # ── No live price — treat as open ────────────────────────────────────────
    if current_price is None or current_price <= 0 or entry_price <= 0:
        return "open", "OPEN"

    is_long = direction != "short"

    # ── Target or stop hit ───────────────────────────────────────────────────
    if is_long:
        if current_price >= target_price:
            return "target_hit", "TARGET HIT"
        if current_price <= stop_price:
            return "stop_hit",   "STOP HIT"
    else:
        if current_price <= target_price:
            return "target_hit", "TARGET HIT"
        if current_price >= stop_price:
            return "stop_hit",   "STOP HIT"

    # ── Near levels ──────────────────────────────────────────────────────────
    if is_long:
        pct_to_tp = (target_price - current_price) / entry_price * 100
        pct_to_sl = (current_price - stop_price)   / entry_price * 100
    else:
        pct_to_tp = (current_price - target_price) / entry_price * 100
        pct_to_sl = (stop_price    - current_price) / entry_price * 100

    if pct_to_tp <= NEAR_TARGET_PCT:
        return "near_target", "NEAR TARGET"
    if pct_to_sl <= NEAR_STOP_PCT:
        return "near_stop",   "NEAR STOP"

    return "open", "OPEN"


def _build_live_position(t: dict, n: dict, cur_p: float | None) -> dict:
    today      = date.today().isoformat()
    entry_date = t["entry_date"] or ""
    entry_p    = float(t["entry_price"]  or 0)
    stop_p     = float(t["stop_price"]   or 0)
    target_p   = float(t["target_price"] or 0)
    direction  = t["direction"] or "long"
    is_long    = direction != "short"

    # Days since entry
    try:
        days_open = max(0, (date.today() - date.fromisoformat(entry_date)).days) if entry_date else 0
    except Exception:
        days_open = 0

    days_left = max(0, MAX_HOLD_DAYS - days_open)

    # Live status (price-based, not simulation-based)
    status_code, status_label = _live_status(
        entry_date, entry_p, stop_p, target_p, cur_p, direction, days_open
    )

    # Live P&L
    if cur_p and entry_p > 0:
        raw_pnl = (cur_p - entry_p) / entry_p * 100
        live_pnl = raw_pnl if is_long else -raw_pnl
    else:
        live_pnl = 0.0

    # Live P&L in rupees (approximate — per share)
    live_pnl_rs = round((cur_p - entry_p) if (cur_p and is_long) else
                        (entry_p - cur_p) if (cur_p and not is_long) else 0.0, 2)

    # Distance to levels from current (or entry if no price)
    ref = cur_p or entry_p
    if entry_p > 0 and ref:
        if is_long:
            pct_to_tp = round((target_p - ref) / entry_p * 100, 2)
            pct_to_sl = round((ref - stop_p)   / entry_p * 100, 2)
        else:
            pct_to_tp = round((ref - target_p) / entry_p * 100, 2)
            pct_to_sl = round((stop_p - ref)   / entry_p * 100, 2)
    else:
        pct_to_tp = pct_to_sl = 0.0

    # R:R
    sl_dist = abs(entry_p - stop_p)
    rr = round(abs(target_p - entry_p) / sl_dist, 2) if sl_dist > 0 else 2.0

    # Time-based expiry info
    signal_dt = n.get("signal_time", f"{t['signal_date']} 15:30:00 IST")
    entry_dt  = n.get("entry_time",  f"{entry_date} 09:15:00 IST")

    return {
        "trade_id":            t["id"],
        "signal_id":           f"SIG-{t['id']:06d}",
        "ticker":              t["ticker"],
        "signal_type":         n.get("signal_type", "AI Pattern"),
        "pattern":             n.get("pattern", t.get("pattern") or ""),
        "direction":           direction,
        "bucket":              n.get("bucket", "standard").capitalize(),
        "timeframe":           n.get("timeframe", "1D"),
        # ── Dates & times ──────────────────────────────────────────────────
        "signal_date":         t["signal_date"] or "",
        "signal_datetime":     signal_dt,
        "entry_date":          entry_date,
        "entry_datetime":      entry_dt,
        "delay_label":         n.get("delay_label", "overnight"),
        # ── Prices ─────────────────────────────────────────────────────────
        "entry_price":         entry_p,
        "stop_price":          stop_p,
        "target_price":        target_p,
        "rr":                  rr,
        "current_price":       cur_p,
        # ── Live P&L ───────────────────────────────────────────────────────
        "live_pnl_pct":        round(live_pnl, 2),
        "live_pnl_rs":         live_pnl_rs,
        # ── Distance to levels ─────────────────────────────────────────────
        "pct_to_tp":           pct_to_tp,
        "pct_to_sl":           pct_to_sl,
        # ── Time ───────────────────────────────────────────────────────────
        "days_open":           days_open,
        "days_left":           days_left,
        "max_hold_days":       MAX_HOLD_DAYS,
        # ── Status ─────────────────────────────────────────────────────────
        "status_code":         status_code,
        "status_label":        status_label,
        # ── Signal quality ─────────────────────────────────────────────────
        "opportunity_score":   n.get("opportunity_score"),
        "reason_code":         n.get("reason_code", "FRESH_SIGNAL"),
        "multi_pattern_count": n.get("multi_pattern_count", 1),
        "tier":                n.get("tier", ""),
        "credibility":         n.get("credibility", ""),
    }


# Status priority — most urgent first
_STATUS_RANK = {
    "near_target":   0,
    "near_stop":     1,
    "pending_entry": 2,
    "open":          3,
    "target_hit":    4,
    "stop_hit":      5,
    "expired":       6,
}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/live/positions")
def live_positions(ticker: str = None, bucket: str = None):
    """
    Returns all positions entered within the last MAX_HOLD_DAYS days,
    with status determined by current market price vs TP/SL levels.

    'positions'  — actionable: pending_entry, open, near_target, near_stop
    'closed'     — resolved this window: target_hit, stop_hit, expired
    """
    today      = date.today().isoformat()
    since      = (date.today() - timedelta(days=MAX_HOLD_DAYS + 2)).isoformat()
    # +2 days buffer to catch pending entries (signal fired today/yesterday)
    since_sig  = (date.today() - timedelta(days=MAX_HOLD_DAYS + 2)).isoformat()

    try:
        con   = _conn()
        where = [
            "trade_type='backtest'",
            "(entry_date >= ? OR signal_date >= ?)",
            "direction IN ('rally','long')",                         # long-only cash equity
            "COALESCE(json_extract(notes,'$.bucket'),'standard') != 'trap'",  # exclude trap
        ]
        prm   = [since, since_sig]
        if ticker:
            where.append("ticker=?")
            prm.append(ticker.upper())

        rows = con.execute(
            f"SELECT * FROM trade_log WHERE {' AND '.join(where)} ORDER BY entry_date DESC LIMIT 500",
            prm,
        ).fetchall()
        con.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    market = _market_info()

    # Collect unique tickers from rows first, then fetch prices in one call
    all_rows = [dict(r) for r in rows]
    unique_tickers = list({r["ticker"] for r in all_rows if r.get("ticker")})
    prices = _get_prices(unique_tickers)

    all_positions: list[dict] = []
    for t in all_rows:
        n = _n(t["notes"])

        b = n.get("bucket", "standard").lower()
        if bucket and b != bucket.lower():
            continue

        pos = _build_live_position(t, n, prices.get(t["ticker"]))
        all_positions.append(pos)

    # Deduplicate by signal_id (prefer most recent)
    seen: set[int] = set()
    unique: list[dict] = []
    for p in all_positions:
        if p["trade_id"] not in seen:
            seen.add(p["trade_id"])
            unique.append(p)
    all_positions = unique

    # Sort: most urgent first, then highest conviction
    all_positions.sort(
        key=lambda p: (
            _STATUS_RANK.get(p["status_code"], 9),
            -(p["opportunity_score"] or 0),
        )
    )

    # Split into actionable vs resolved
    active_codes  = {"pending_entry", "open", "near_target", "near_stop"}
    positions     = [p for p in all_positions if p["status_code"] in active_codes]
    closed        = [p for p in all_positions if p["status_code"] not in active_codes]

    # Summary
    summary: dict[str, int] = defaultdict(int)
    for p in all_positions:
        summary[p["status_code"]] += 1

    # Alerts: near-level positions + new pending entries from last signal cycle
    ist_date = market["ist_date"]
    alerts = [
        p for p in positions
        if p["status_code"] in ("near_target", "near_stop")
        or (p["status_code"] == "pending_entry" and p["signal_date"] >= ist_date)
    ]

    return {
        "market":          market,
        "current_prices":  prices,
        "summary":         dict(summary),
        "positions":       positions,       # actionable only
        "closed":          closed,          # target_hit, stop_hit, expired
        "alerts":          alerts,
        "total_active":    len(positions),
        "total_closed":    len(closed),
        "as_of":           datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


@router.get("/live/history")
def live_history(ticker: str = None, days: int = 90):
    """Closed positions over the last N days with cumulative P&L."""
    today  = date.today().isoformat()
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    try:
        con   = _conn()
        where = [
            "trade_type='backtest'",
            "exit_date >= ?",
            "exit_date <= ?",
            "direction IN ('rally','long')",
            "COALESCE(json_extract(notes,'$.bucket'),'standard') != 'trap'",
        ]
        prm   = [cutoff, today]
        if ticker:
            where.append("ticker=?")
            prm.append(ticker.upper())

        rows = con.execute(
            f"SELECT * FROM trade_log WHERE {' AND '.join(where)} ORDER BY exit_date DESC LIMIT 500",
            prm,
        ).fetchall()
        con.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    hist_rows = [dict(r) for r in rows]
    hist_tickers = list({r["ticker"] for r in hist_rows if r.get("ticker")})
    prices = _get_prices(hist_tickers)
    history = []
    for t in hist_rows:
        n    = _n(t["notes"])
        pnl  = round(float(t["pnl_pct"] or 0), 2)
        ep   = float(t["entry_price"] or 0)
        xp   = float(t["exit_price"]  or 0)
        cur  = prices.get(t["ticker"])

        # Re-evaluate status using current price (may differ from sim outcome)
        try:
            days_open = (date.today() - date.fromisoformat(t["entry_date"])).days
        except Exception:
            days_open = int(t["days_held"] or 0)

        sim_exit = t["exit_reason"] or "timeout"

        history.append({
            "trade_id":    t["id"],
            "signal_id":   f"SIG-{t['id']:06d}",
            "ticker":      t["ticker"],
            "signal_type": n.get("signal_type", "AI Pattern"),
            "pattern":     n.get("pattern", t.get("pattern") or ""),
            "direction":   t["direction"],
            "bucket":      n.get("bucket", "standard").capitalize(),
            "entry_date":  t["entry_date"] or "",
            "exit_date":   t["exit_date"]  or "",
            "entry_price": ep,
            "exit_price":  xp,
            "pnl_pct":     pnl,
            "exit_reason": sim_exit,
            "days_held":   int(t["days_held"] or 0),
            "current_price": cur,
        })

    # Running cumulative P&L (oldest first)
    cum = 0.0
    for item in reversed(history):
        cum += item["pnl_pct"]
        item["cumulative_pnl"] = round(cum, 2)

    wins  = sum(1 for h in history if h["exit_reason"] == "tp")
    total = len(history)
    return {
        "total":    total,
        "wins":     wins,
        "losses":   total - wins,
        "win_rate": round(wins / total * 100, 1) if total else 0.0,
        "history":  history,
    }
