"""
KANIDA.AI Backtest API Router
Serves 2024-2026 trade performance analysis from trade_log.
"""
from __future__ import annotations

import json
import os
import sys
import sqlite3
from collections import defaultdict
from pathlib import Path
from fastapi import APIRouter, HTTPException

router  = APIRouter()
_HERE   = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent))
from db import get_conn

DB_PATH = os.environ.get("KANIDA_DB_PATH",
          str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"))


def _conn():
    return get_conn()


def _n(notes_str) -> dict:
    try:
        return json.loads(notes_str) if notes_str else {}
    except Exception:
        return {}


# ── Overview ──────────────────────────────────────────────────────────────────

@router.get("/backtest/overview")
def overview():
    try:
        con = _conn()
        rows = con.execute("""
            SELECT id, ticker, market, direction, exit_reason, pnl_pct,
                   days_held, notes, entry_date
            FROM trade_log WHERE trade_type='backtest'
            ORDER BY ticker, entry_date
        """).fetchall()
        con.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    stocks: dict = defaultdict(lambda: {
        "wins": 0, "total": 0,
        "turbo": 0, "super": 0, "standard": 0, "trap": 0,
        "pnls": [], "win_pnls": [], "loss_pnls": [],
        "years": defaultdict(lambda: {"wins": 0, "total": 0}),
        "multi_counts": [],
    })

    for row in rows:
        t = dict(row)
        n = _n(t["notes"])
        key = t["ticker"]
        s   = stocks[key]
        pnl = float(t["pnl_pct"] or 0)
        s["total"] += 1
        s["pnls"].append(pnl)
        b = n.get("bucket", "standard")
        s[b] = s.get(b, 0) + 1
        yr = (t["entry_date"] or "")[:4]
        s["years"][yr]["total"] += 1
        s["multi_counts"].append(n.get("multi_pattern_count", 1))
        if t["exit_reason"] == "tp":
            s["wins"] += 1;  s["win_pnls"].append(pnl);  s["years"][yr]["wins"] += 1
        else:
            s["loss_pnls"].append(pnl)

    result = []
    for ticker, s in sorted(stocks.items()):
        total = s["total"]
        wins  = s["wins"]
        pnls  = s["pnls"]
        avg_mc = round(sum(s["multi_counts"]) / len(s["multi_counts"]), 1) if s["multi_counts"] else 1

        year_data = []
        for yr in ["2024", "2025", "2026"]:
            yd = s["years"].get(yr, {"wins": 0, "total": 0})
            year_data.append({
                "year": yr, "total": yd["total"], "wins": yd["wins"],
                "win_rate": round(yd["wins"] / yd["total"] * 100, 1) if yd["total"] > 0 else 0,
            })

        result.append({
            "ticker":                ticker,
            "total_trades":          total,
            "wins":                  wins,
            "losses":                total - wins,
            "win_rate":              round(wins / total * 100, 1) if total else 0,
            "avg_pnl":               round(sum(pnls) / len(pnls), 2) if pnls else 0,
            "avg_win":               round(sum(s["win_pnls"]) / len(s["win_pnls"]), 2) if s["win_pnls"] else 0,
            "avg_loss":              round(sum(s["loss_pnls"]) / len(s["loss_pnls"]), 2) if s["loss_pnls"] else 0,
            "turbo":                 s.get("turbo", 0),
            "super":                 s.get("super", 0),
            "standard":              s.get("standard", 0),
            "trap":                  s.get("trap", 0),
            "avg_competing_patterns": avg_mc,
            "by_year":               year_data,
            "best_year":             max(year_data, key=lambda y: y["win_rate"])["year"] if year_data else "",
        })

    total_trades = sum(s["total"] for s in stocks.values())
    all_pnls     = [p for s in stocks.values() for p in s["pnls"]]
    total_wins   = sum(s["wins"] for s in stocks.values())

    return {
        "stocks":           result,
        "total_trades":     total_trades,
        "overall_win_rate": round(total_wins / total_trades * 100, 1) if total_trades else 0,
        "overall_avg_pnl":  round(sum(all_pnls) / len(all_pnls), 2) if all_pnls else 0,
        "data_note":        "1 trade per stock/date/direction (best opportunity_score pattern selected when multiple patterns fire on same day)",
        "timeframe_note":   "All signals use 1D daily data. Signal fires at NSE close (15:30 IST). Entry at next open (09:15 IST).",
    }


# ── Trade Log ─────────────────────────────────────────────────────────────────

@router.get("/backtest/trades")
def trades(ticker: str = None, year: str = None, bucket: str = None, limit: int = 500):
    try:
        con = _conn()
        where  = ["trade_type='backtest'"]
        params: list = []
        if ticker:
            where.append("ticker=?");  params.append(ticker.upper())
        rows = con.execute(
            f"SELECT * FROM trade_log WHERE {' AND '.join(where)} ORDER BY entry_date DESC LIMIT ?",
            params + [limit]
        ).fetchall()
        con.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    result = []
    for row in rows:
        t = dict(row)
        n = _n(t["notes"])
        entry_yr = (t["entry_date"] or "")[:4]
        if year   and entry_yr != year:               continue
        if bucket and n.get("bucket", "").lower() != bucket.lower(): continue

        entry_p  = float(t["entry_price"]  or 0)
        target_p = float(t["target_price"] or 0)
        stop_p   = float(t["stop_price"]   or 0)
        rr       = round((target_p - entry_p) / (entry_p - stop_p), 2) if entry_p > stop_p > 0 else 2.0

        result.append({
            # IDs
            "trade_id":              t["id"],
            "signal_id":             f"SIG-{t['id']:06d}",
            # What / When
            "ticker":                t["ticker"],
            "market":                t["market"],
            "year":                  entry_yr,
            "timeframe":             n.get("timeframe", "1D"),
            "signal_type":           n.get("signal_type", "AI Pattern"),
            "pattern":               n.get("pattern", ""),
            "direction":             t["direction"],
            # DateTime — signal fires at close, entry at next open
            "signal_date":           t["signal_date"],
            "signal_datetime":       n.get("signal_time", f"{t['signal_date']} 15:30:00 IST"),
            "entry_date":            t["entry_date"],
            "entry_datetime":        n.get("entry_time", f"{t['entry_date']} 09:15:00 IST"),
            "signal_to_entry_mins":  n.get("signal_to_entry_mins"),
            "delay_label":           n.get("delay_label", "overnight"),
            # Trade levels
            "entry_price":           entry_p,
            "stop_price":            stop_p,
            "target_price":          target_p,
            "rr":                    rr,
            # Exit
            "exit_date":             t["exit_date"],
            "exit_price":            float(t["exit_price"] or 0),
            "exit_reason":           t["exit_reason"],
            "days_held":             int(t["days_held"] or 0),
            "pnl_pct":               round(float(t["pnl_pct"] or 0), 2),
            # Quality metrics
            "bucket":                n.get("bucket", "standard").capitalize(),
            "mfe_pct":               n.get("mfe_pct"),
            "mae_pct":               n.get("mae_pct"),
            "mpi_pct":               n.get("mpi_pct"),
            "post_5d_pct":           n.get("post_5d_pct"),
            # Signal metadata
            "reason_code":           n.get("reason_code", "FRESH_SIGNAL"),
            "multi_pattern_count":   n.get("multi_pattern_count", 1),
            "opportunity_score":     n.get("opportunity_score"),
            "tier":                  n.get("tier", ""),
            "credibility":           n.get("credibility", ""),
        })

    return {"count": len(result), "trades": result}


# ── Signal Combinations ───────────────────────────────────────────────────────

@router.get("/backtest/combinations")
def combinations(ticker: str = None):
    try:
        con = _conn()
        where = ["trade_type='backtest'"]
        params: list = []
        if ticker:
            where.append("ticker=?");  params.append(ticker.upper())
        rows = con.execute(
            f"SELECT ticker, exit_reason, pnl_pct, notes FROM trade_log WHERE {' AND '.join(where)}",
            params
        ).fetchall()
        con.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    combo: dict = defaultdict(lambda: {
        "wins": 0, "total": 0, "pnls": [], "tickers": set(), "buckets": defaultdict(int)
    })

    for row in rows:
        t   = dict(row)
        n   = _n(t["notes"])
        sig = n.get("signal_type", "AI Pattern")
        pat = n.get("pattern", "unknown")
        atoms = pat.split(" + ")
        key   = sig + "|" + " + ".join(atoms[:3])
        combo[key]["total"] += 1
        combo[key]["pnls"].append(float(t["pnl_pct"] or 0))
        combo[key]["tickers"].add(t["ticker"])
        combo[key]["buckets"][n.get("bucket", "standard")] += 1
        if t["exit_reason"] == "tp":
            combo[key]["wins"] += 1
        combo[key]["full_pattern"] = pat
        combo[key]["signal_type"]  = sig

    result = []
    for key, d in combo.items():
        sig, short_pat = key.split("|", 1)
        total    = d["total"]
        wins     = d["wins"]
        pnls     = d["pnls"]
        avg_ret  = round(sum(pnls) / len(pnls), 2) if pnls else 0
        win_rate = round(wins / total * 100, 1) if total else 0
        best_b   = max(d["buckets"], key=lambda b: d["buckets"][b]) if d["buckets"] else "standard"

        result.append({
            "signal_type":  sig,
            "pattern":      short_pat,
            "full_pattern": d.get("full_pattern", short_pat),
            "win_rate":     win_rate,
            "avg_return":   avg_ret,
            "total":        total,
            "wins":         wins,
            "category":     best_b,
            "tickers":      sorted(d["tickers"]),
        })

    result.sort(key=lambda r: (-r["win_rate"], -r["total"]))
    return {"count": len(result), "combinations": result[:60]}


# ── Missed Profit Index ───────────────────────────────────────────────────────

@router.get("/backtest/missed-profit")
def missed_profit(ticker: str = None):
    try:
        con = _conn()
        where = ["trade_type='backtest'", "exit_reason='tp'"]
        params: list = []
        if ticker:
            where.append("ticker=?");  params.append(ticker.upper())
        rows = con.execute(
            f"SELECT * FROM trade_log WHERE {' AND '.join(where)} ORDER BY entry_date DESC",
            params
        ).fetchall()
        con.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    result = []
    for row in rows:
        t   = dict(row)
        n   = _n(t["notes"])
        mpi = n.get("mpi_pct")
        if mpi is None or mpi < 2.0:
            continue

        entry_p  = float(t["entry_price"] or 1)
        booked   = round(float(t["pnl_pct"] or 0), 2)

        result.append({
            "trade_id":        t["id"],
            "signal_id":       f"SIG-{t['id']:06d}",
            "ticker":          t["ticker"],
            "signal_datetime": n.get("signal_time", t["signal_date"]),
            "entry_datetime":  n.get("entry_time",  t["entry_date"]),
            "exit_date":       t["exit_date"],
            "direction":       t["direction"],
            "entry_price":     round(entry_p, 2),
            "exit_price":      round(float(t["exit_price"] or 0), 2),
            "booked_pct":      booked,
            "continued_pct":   round(mpi, 2),
            "total_available": round(booked + mpi, 2),
            "missed_pct":      round(mpi, 2),
            "pattern":         n.get("pattern", ""),
            "signal_type":     n.get("signal_type", ""),
            "bucket":          n.get("bucket", ""),
            "reason_code":     n.get("reason_code", ""),
            "timeframe":       n.get("timeframe", "1D"),
        })

    result.sort(key=lambda r: -r["missed_pct"])
    total_missed = sum(r["missed_pct"] for r in result)
    avg_missed   = round(total_missed / len(result), 2) if result else 0

    return {
        "count":          len(result),
        "avg_missed_pct": avg_missed,
        "trades":         result[:150],
    }


# ── MPI Next Steps (Recommendations) ─────────────────────────────────────────

@router.get("/backtest/mpi-recommendations")
def mpi_recommendations(ticker: str = None):
    """
    Analyse which signal types consistently leave profit on the table.
    Returns ranked recommendations: extend target / use trailing stop / keep as-is.
    """
    try:
        con = _conn()
        where = ["trade_type='backtest'", "exit_reason='tp'"]
        params: list = []
        if ticker:
            where.append("ticker=?");  params.append(ticker.upper())
        rows = con.execute(
            f"SELECT * FROM trade_log WHERE {' AND '.join(where)}",
            params
        ).fetchall()
        con.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    # Group by signal_type + ticker
    groups: dict = defaultdict(lambda: {
        "mpis": [], "pnls": [], "total": 0, "tickers": set()
    })

    for row in rows:
        t = dict(row)
        n = _n(t["notes"])
        mpi = n.get("mpi_pct", 0) or 0
        key = f"{t['ticker']}|{n.get('signal_type', 'AI Pattern')}"
        groups[key]["mpis"].append(mpi)
        groups[key]["pnls"].append(float(t["pnl_pct"] or 0))
        groups[key]["total"] += 1
        groups[key]["tickers"].add(t["ticker"])
        groups[key]["signal_type"] = n.get("signal_type", "AI Pattern")
        groups[key]["ticker"] = t["ticker"]

    recommendations = []
    for key, d in groups.items():
        mpis = d["mpis"]
        pnls = d["pnls"]
        total = d["total"]
        if total < 3:
            continue

        avg_mpi       = round(sum(mpis) / total, 2)
        avg_pnl       = round(sum(pnls) / total, 2)
        high_mpi_rate = round(sum(1 for m in mpis if m >= 5) / total * 100, 1)
        max_mpi       = round(max(mpis), 2)

        # Determine recommendation
        if avg_mpi >= 7 and high_mpi_rate >= 40:
            action      = "USE TRAILING STOP"
            action_code = "trailing_stop"
            rationale   = (f"This signal consistently continues {avg_mpi:.1f}% beyond target "
                           f"({high_mpi_rate:.0f}% of trades have >5% continuation). "
                           f"Replace fixed TP with ATR-based trailing stop to capture the full move.")
            simulated_gain = round(avg_pnl + avg_mpi * 0.6, 2)  # capture ~60% of continuation
        elif avg_mpi >= 3 and high_mpi_rate >= 25:
            action      = "EXTEND TARGET"
            action_code = "extend_target"
            rationale   = (f"Signal continues {avg_mpi:.1f}% post-TP in {high_mpi_rate:.0f}% of cases. "
                           f"Consider increasing target by 50% (1.5x current) to capture more of the move.")
            simulated_gain = round(avg_pnl + avg_mpi * 0.4, 2)
        else:
            action      = "KEEP AS-IS"
            action_code = "keep"
            rationale   = (f"Continuation is modest ({avg_mpi:.1f}% avg). "
                           f"Fixed TP is appropriate for this signal type.")
            simulated_gain = avg_pnl

        recommendations.append({
            "ticker":           d["ticker"],
            "signal_type":      d["signal_type"],
            "total_tp_trades":  total,
            "avg_booked_pct":   avg_pnl,
            "avg_mpi_pct":      avg_mpi,
            "max_mpi_pct":      max_mpi,
            "high_mpi_rate":    high_mpi_rate,
            "action":           action,
            "action_code":      action_code,
            "rationale":        rationale,
            "simulated_avg_pnl": simulated_gain,
            "extra_gain_pct":   round(simulated_gain - avg_pnl, 2),
        })

    recommendations.sort(key=lambda r: (-r["avg_mpi_pct"], -r["total_tp_trades"]))
    return {"count": len(recommendations), "recommendations": recommendations}


# ── Bucket Summary ────────────────────────────────────────────────────────────

@router.get("/backtest/buckets")
def buckets(ticker: str = None):
    try:
        con = _conn()
        where = ["trade_type='backtest'"]
        params: list = []
        if ticker:
            where.append("ticker=?");  params.append(ticker.upper())
        rows = con.execute(
            f"SELECT ticker, exit_reason, pnl_pct, days_held, notes FROM trade_log WHERE {' AND '.join(where)}",
            params
        ).fetchall()
        con.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    DEFS = {
        "turbo":    ("🚀 TURBO",    "Hit target fast (<30% of window), strong continuation, low drawdown"),
        "super":    ("🔥 SUPER",    "Clean target hit within 55% of window with continuation"),
        "standard": ("⚡ STANDARD", "Hit target (slower) or clean stop loss — tradeable expectancy"),
        "trap":     ("❌ TRAP",     "Stop hit then price reversed — false breakout / weak signal"),
    }
    data: dict = {k: {"label": v[0], "desc": v[1], "trades": []} for k, v in DEFS.items()}

    for row in rows:
        t = dict(row)
        n = _n(t["notes"])
        b = n.get("bucket", "standard")
        if b not in data:
            b = "standard"
        data[b]["trades"].append({
            "pnl":    float(t["pnl_pct"] or 0),
            "days":   int(t["days_held"] or 0),
            "won":    t["exit_reason"] == "tp",
            "ticker": t["ticker"],
            "mfe":    n.get("mfe_pct", 0),
            "mpi":    n.get("mpi_pct"),
        })

    result = {}
    for key, bd in data.items():
        trs   = bd["trades"]
        total = len(trs)
        wins  = sum(1 for t in trs if t["won"])
        pnls  = [t["pnl"] for t in trs]
        mpi_vals = [t["mpi"] for t in trs if t.get("mpi") is not None]

        result[key] = {
            "label":            bd["label"],
            "description":      bd["desc"],
            "total":            total,
            "win_rate":         round(wins / total * 100, 1) if total else 0,
            "avg_return":       round(sum(pnls) / total, 2) if total else 0,
            "avg_days_to_exit": round(sum(t["days"] for t in trs) / total, 1) if total else 0,
            "avg_mfe":          round(sum(t["mfe"] or 0 for t in trs) / total, 2) if total else 0,
            "avg_continuation": round(sum(mpi_vals) / len(mpi_vals), 2) if mpi_vals else None,
            "by_ticker":        {tkr: sum(1 for t in trs if t["ticker"] == tkr)
                                 for tkr in set(t["ticker"] for t in trs)},
        }

    return {"buckets": result}
