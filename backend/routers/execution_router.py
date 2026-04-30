"""
KANIDA.AI Execution Intelligence API Router

Serves execution quality analysis: blind vs smart entry comparison,
waiting-for-confirmation signals, and per-trade execution metadata.

All data sourced from execution_log table (populated by run_execution_analysis.py).
"""
from __future__ import annotations

import os
import sys
import sqlite3
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

router  = APIRouter()
_HERE   = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent))
from db import get_conn

DB_PATH = os.environ.get("KANIDA_DB_PATH",
          str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"))


def _conn():
    return get_conn()


def _row(r) -> dict:
    return dict(r)


# ── /api/execution/summary ────────────────────────────────────────────────────

@router.get("/execution/summary")
def execution_summary(ticker: Optional[str] = Query(None)):
    """
    High-level blind vs smart comparison stats.
    Optional ticker filter.
    """
    try:
        con = _conn()
        where = "WHERE ticker = ?" if ticker else ""
        params = (ticker,) if ticker else ()

        rows = con.execute(f"""
            SELECT
                COUNT(*)                        AS total,
                SUM(trade_taken)                AS taken,
                COUNT(*) - SUM(trade_taken)     AS skipped,
                AVG(blind_pnl_pct)              AS blind_avg_pnl,
                SUM(CASE WHEN blind_pnl_pct > 0 THEN 1 ELSE 0 END) AS blind_wins,
                AVG(CASE WHEN trade_taken=1 THEN smart_pnl_pct END) AS smart_avg_pnl,
                SUM(CASE WHEN trade_taken=1 AND smart_pnl_pct > 0 THEN 1 ELSE 0 END) AS smart_wins,
                AVG(pnl_improvement)            AS avg_pnl_improvement,
                AVG(gap_pct)                    AS avg_gap_pct,
                SUM(CASE WHEN nifty_is_weak=1 THEN 1 ELSE 0 END) AS nifty_weak_days
            FROM execution_log {where}
        """, params).fetchone()

        if not rows or rows["total"] == 0:
            raise HTTPException(status_code=404, detail="No execution data — run run_execution_analysis.py first")

        total  = rows["total"]
        taken  = rows["taken"] or 0

        # Exec code distribution
        dist_rows = con.execute(f"""
            SELECT exec_code, COUNT(*) AS cnt
            FROM execution_log {where}
            GROUP BY exec_code ORDER BY cnt DESC
        """, params).fetchall()

        distribution = [{"exec_code": r["exec_code"], "count": r["cnt"],
                         "pct": round(r["cnt"] / total * 100, 1)}
                        for r in dist_rows]

        # Per-ticker breakdown
        ticker_rows = con.execute(f"""
            SELECT
                ticker,
                COUNT(*)                         AS total,
                SUM(trade_taken)                 AS taken,
                ROUND(AVG(blind_pnl_pct),2)      AS blind_avg_pnl,
                ROUND(AVG(CASE WHEN trade_taken=1 THEN smart_pnl_pct END), 2) AS smart_avg_pnl,
                SUM(CASE WHEN blind_pnl_pct > 0 THEN 1 ELSE 0 END) AS blind_wins,
                SUM(CASE WHEN trade_taken=1 AND smart_pnl_pct > 0 THEN 1 ELSE 0 END) AS smart_wins
            FROM execution_log {where}
            GROUP BY ticker ORDER BY total DESC
        """, params).fetchall()

        per_ticker = []
        for r in ticker_rows:
            t = r["total"] or 1
            tk = r["taken"] or 0
            per_ticker.append({
                "ticker":            r["ticker"],
                "total":             r["total"],
                "taken":             tk,
                "taken_pct":         round(tk / t * 100, 1),
                "blind_avg_pnl":     r["blind_avg_pnl"],
                "smart_avg_pnl":     r["smart_avg_pnl"],
                "blind_win_rate":    round((r["blind_wins"] or 0) / t * 100, 1),
                "smart_win_rate":    round((r["smart_wins"] or 0) / max(1, tk) * 100, 1),
            })

        # Gap distribution
        gap_dist = con.execute(f"""
            SELECT gap_category, COUNT(*) AS cnt
            FROM execution_log {where}
            GROUP BY gap_category ORDER BY cnt DESC
        """, params).fetchall()

        con.close()
        return {
            "total":              total,
            "taken":              taken,
            "skipped":            rows["skipped"] or 0,
            "taken_pct":          round(taken / total * 100, 1),
            "blind_avg_pnl":      round(rows["blind_avg_pnl"] or 0, 2),
            "blind_win_rate":     round((rows["blind_wins"] or 0) / total * 100, 1),
            "smart_avg_pnl":      round(rows["smart_avg_pnl"] or 0, 2),
            "smart_win_rate":     round((rows["smart_wins"] or 0) / max(1, taken) * 100, 1),
            "avg_pnl_improvement":round(rows["avg_pnl_improvement"] or 0, 2),
            "avg_gap_pct":        round(rows["avg_gap_pct"] or 0, 2),
            "nifty_weak_days":    rows["nifty_weak_days"] or 0,
            "exec_distribution":  distribution,
            "per_ticker":         per_ticker,
            "gap_distribution":   [{"category": r["gap_category"], "count": r["cnt"]}
                                   for r in gap_dist],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── /api/execution/trades ─────────────────────────────────────────────────────

@router.get("/execution/trades")
def execution_trades(
    ticker:      Optional[str] = Query(None),
    exec_code:   Optional[str] = Query(None),
    trade_taken: Optional[int] = Query(None, ge=0, le=1),
    limit:       int           = Query(200, ge=1, le=1000),
    offset:      int           = Query(0, ge=0),
):
    """
    Individual execution records with blind vs smart P&L comparison.
    """
    try:
        con  = _conn()
        cond = []
        params: list = []

        if ticker:
            cond.append("e.ticker = ?");      params.append(ticker)
        if exec_code:
            cond.append("e.exec_code = ?");   params.append(exec_code)
        if trade_taken is not None:
            cond.append("e.trade_taken = ?"); params.append(trade_taken)

        where = ("WHERE " + " AND ".join(cond)) if cond else ""

        count = con.execute(
            f"SELECT COUNT(*) FROM execution_log e {where}", params
        ).fetchone()[0]

        rows = con.execute(f"""
            SELECT
                e.id, e.trade_log_id, e.ticker, e.direction,
                e.signal_date, e.entry_date,
                e.exec_code, e.trade_taken, e.entry_window, e.exec_notes,
                e.gap_pct, e.gap_category, e.day_move_pct, e.day_range_pct,
                e.nifty_day_move, e.nifty_is_weak,
                e.prev_close, e.entry_open, e.entry_high, e.entry_low, e.entry_close,
                e.blind_entry_price, e.smart_entry_price, e.exit_price,
                e.blind_pnl_pct, e.smart_pnl_pct, e.pnl_improvement,
                t.notes AS trade_notes
            FROM execution_log e
            LEFT JOIN trade_log t ON t.id = e.trade_log_id
            {where}
            ORDER BY e.entry_date DESC
            LIMIT ? OFFSET ?
        """, params + [limit, offset]).fetchall()

        import json
        trades = []
        for r in rows:
            d = _row(r)
            try:
                tn = json.loads(d.pop("trade_notes") or "{}")
            except Exception:
                tn = {}
            d["pattern"]      = tn.get("pattern", "")
            d["signal_type"]  = tn.get("signal_type", "")
            d["bucket"]       = tn.get("bucket", "")
            d["tier"]         = tn.get("tier", "")
            d["nifty_is_weak"] = bool(d["nifty_is_weak"])
            d["trade_taken"]   = bool(d["trade_taken"])
            trades.append(d)

        con.close()
        return {"count": count, "trades": trades}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── /api/execution/waiting ────────────────────────────────────────────────────

@router.get("/execution/waiting")
def execution_waiting(ticker: Optional[str] = Query(None)):
    """
    Signals currently in 'Waiting for Confirmation' state.
    These are live_opportunities rows not yet acted on, enriched with
    what the execution engine would decide given today's opening data.

    NOTE: this endpoint uses ohlc_daily for today's open if available;
    otherwise falls back to the price feed for current price.
    """
    try:
        con = _conn()
        where = "AND ticker = ?" if ticker else ""
        params = (ticker,) if ticker else ()

        # Fetch pending signals from live_opportunities
        rows = con.execute(f"""
            SELECT id, ticker, direction, signal_date, entry_price,
                   target_price, stop_price, notes
            FROM live_opportunities
            WHERE status IN ('pending', 'waiting')
              {where}
            ORDER BY signal_date DESC
            LIMIT 50
        """, params).fetchall()

        import json
        signals = []
        for r in rows:
            d = _row(r)
            try:
                notes = json.loads(d.pop("notes") or "{}")
            except Exception:
                notes = {}
            d["pattern"]     = notes.get("pattern", "")
            d["signal_type"] = notes.get("signal_type", "")
            d["bucket"]      = notes.get("bucket", "")
            signals.append(d)

        con.close()
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── /api/execution/comparison ────────────────────────────────────────────────

@router.get("/execution/comparison")
def execution_comparison(ticker: Optional[str] = Query(None)):
    """
    Granular blind-vs-smart P&L comparison sliced by exec_code, gap_category,
    and direction. Powers the comparison cards in the Execution IQ tab.
    """
    try:
        con = _conn()
        where = "WHERE ticker = ?" if ticker else ""
        params = (ticker,) if ticker else ()

        # By exec_code
        by_code = con.execute(f"""
            SELECT
                exec_code,
                COUNT(*)                                      AS total,
                SUM(trade_taken)                              AS taken,
                ROUND(AVG(blind_pnl_pct), 2)                 AS blind_avg,
                ROUND(AVG(CASE WHEN trade_taken=1 THEN smart_pnl_pct END), 2) AS smart_avg,
                ROUND(100.0*SUM(CASE WHEN blind_pnl_pct>0 THEN 1 ELSE 0 END)/COUNT(*), 1) AS blind_wr,
                ROUND(100.0*SUM(CASE WHEN trade_taken=1 AND smart_pnl_pct>0 THEN 1 ELSE 0 END)
                      /MAX(1,SUM(trade_taken)), 1)            AS smart_wr
            FROM execution_log {where}
            GROUP BY exec_code ORDER BY total DESC
        """, params).fetchall()

        # By gap_category
        by_gap = con.execute(f"""
            SELECT
                gap_category,
                COUNT(*)                                      AS total,
                ROUND(AVG(blind_pnl_pct), 2)                 AS blind_avg,
                ROUND(AVG(CASE WHEN trade_taken=1 THEN smart_pnl_pct END), 2) AS smart_avg,
                ROUND(100.0*SUM(CASE WHEN blind_pnl_pct>0 THEN 1 ELSE 0 END)/COUNT(*), 1) AS blind_wr,
                ROUND(100.0*SUM(CASE WHEN trade_taken=1 AND smart_pnl_pct>0 THEN 1 ELSE 0 END)
                      /MAX(1,SUM(trade_taken)), 1)            AS smart_wr
            FROM execution_log {where}
            GROUP BY gap_category ORDER BY total DESC
        """, params).fetchall()

        # By direction
        by_dir = con.execute(f"""
            SELECT
                direction,
                COUNT(*)                                      AS total,
                SUM(trade_taken)                              AS taken,
                ROUND(AVG(blind_pnl_pct), 2)                 AS blind_avg,
                ROUND(AVG(CASE WHEN trade_taken=1 THEN smart_pnl_pct END), 2) AS smart_avg,
                ROUND(100.0*SUM(CASE WHEN blind_pnl_pct>0 THEN 1 ELSE 0 END)/COUNT(*), 1) AS blind_wr,
                ROUND(100.0*SUM(CASE WHEN trade_taken=1 AND smart_pnl_pct>0 THEN 1 ELSE 0 END)
                      /MAX(1,SUM(trade_taken)), 1)            AS smart_wr
            FROM execution_log {where}
            GROUP BY direction
        """, params).fetchall()

        # Monthly trend (blind vs smart avg pnl)
        monthly = con.execute(f"""
            SELECT
                SUBSTR(entry_date,1,7)                        AS month,
                COUNT(*)                                      AS total,
                ROUND(AVG(blind_pnl_pct), 2)                 AS blind_avg,
                ROUND(AVG(CASE WHEN trade_taken=1 THEN smart_pnl_pct END), 2) AS smart_avg
            FROM execution_log {where}
            GROUP BY month ORDER BY month
        """, params).fetchall()

        con.close()
        return {
            "by_exec_code":    [_row(r) for r in by_code],
            "by_gap_category": [_row(r) for r in by_gap],
            "by_direction":    [_row(r) for r in by_dir],
            "monthly_trend":   [_row(r) for r in monthly],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
