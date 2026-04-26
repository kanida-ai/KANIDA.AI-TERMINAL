"""
KANIDA.AI Swing Trading Terminal — Backend API

Serves long-only NSE swing trade analytics.
Cash equity only: buy → hold → exit (no short selling).
Short-side signals are tracked separately for future F&O expansion.

All P&L uses smart_entry_price from execution_log where trade_taken=1,
falls back to blind entry otherwise.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Query

router  = APIRouter()
_HERE   = Path(__file__).parent
DB_PATH = os.environ.get("KANIDA_DB_PATH",
          str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"))

LONG_DIRS   = ("rally", "long")
BUCKETS     = ["turbo", "super", "standard"]   # trap excluded from terminal display

BUCKET_META = {
    "turbo":    {"label": "Turbo Engine",    "icon": "🚀",
                 "description": "Fast momentum exits — highest conviction, fastest resolution"},
    "super":    {"label": "Super Engine",    "icon": "🔥",
                 "description": "Strong trend continuation — highest avg return per trade"},
    "standard": {"label": "Standard Engine", "icon": "📊",
                 "description": "High-volume signals — selective entry required"},
    "trap":     {"label": "Trap Engine",     "icon": "⚠️",
                 "description": "Counter-move signals — use as exit alert, not entry"},
}


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _smart_pnl_expr(alias: str = "") -> str:
    """SQL expression: smart_pnl when trade_taken=1, else blind_pnl, else t.pnl_pct fallback."""
    a = f"{alias}." if alias else ""
    return (
        f"CASE WHEN {a}trade_taken=1 AND {a}smart_pnl_pct IS NOT NULL "
        f"THEN {a}smart_pnl_pct "
        f"ELSE COALESCE({a}blind_pnl_pct, t.pnl_pct) END"
    )


def _date_offset(days: int) -> str:
    return str(date.today() - timedelta(days=days))


# ── /api/swing/overview ───────────────────────────────────────────────────────

@router.get("/swing/overview")
def swing_overview(year: Optional[str] = Query(None)):
    """
    Full overview for the Swing Trading Terminal.
    Returns top-level summary, engine bucket cards, and top-ranked stocks per engine.
    Long-only: direction IN ('rally', 'long').
    Uses smart entry P&L where execution_log has trade_taken=1.
    Optional year filter applies to all historical stats (not rolling 90d/180d).
    """
    con = _conn()

    yr_cond   = "AND strftime('%Y', t.entry_date) = ?" if year else ""
    yr_params = [year] if year else []

    # ── Active signals from live_opportunities (long/rally only) ──────────────
    active_rows = con.execute("""
        SELECT ticker, direction, tier, opportunity_score, credibility,
               latest_date, setup_summary
        FROM live_opportunities
        WHERE direction = 'rally'
        ORDER BY opportunity_score DESC
    """).fetchall()
    active_tickers = {r["ticker"] for r in active_rows}
    active_count   = len(active_rows)

    NO_TRAP = "COALESCE(json_extract(t.notes,'$.bucket'),'standard') != 'trap'"

    # ── Overall summary (trap excluded, optional year filter) ─────────────────
    summary_row = con.execute(f"""
        SELECT
            COUNT(*)                                                AS total,
            ROUND(AVG({_smart_pnl_expr('e')}), 2)                  AS smart_avg,
            SUM(CASE WHEN {_smart_pnl_expr('e')} > 0 THEN 1 ELSE 0 END) AS smart_wins,
            ROUND(AVG(t.days_held), 1)                             AS avg_days,
            MIN(t.entry_date)                                      AS first_trade,
            MAX(t.entry_date)                                      AS last_trade
        FROM trade_log t
        LEFT JOIN execution_log e ON e.trade_log_id = t.id
        WHERE t.direction IN ('rally','long') AND {NO_TRAP} {yr_cond}
    """, yr_params).fetchone()

    # ── High-conviction summary (turbo + super only, optional year filter) ────
    hc_row = con.execute(f"""
        SELECT
            COUNT(*)                                                AS total,
            SUM(CASE WHEN {_smart_pnl_expr('e')} > 0 THEN 1 ELSE 0 END) AS wins,
            ROUND(AVG({_smart_pnl_expr('e')}), 2)                  AS avg_pnl,
            ROUND(SUM({_smart_pnl_expr('e')}), 1)                  AS total_pnl
        FROM trade_log t
        LEFT JOIN execution_log e ON e.trade_log_id = t.id
        WHERE t.direction IN ('rally','long')
          AND json_extract(t.notes,'$.bucket') IN ('turbo','super')
          {yr_cond}
    """, yr_params).fetchone()

    total    = summary_row["total"] or 1
    hc_total = hc_row["total"] or 1
    summary = {
        "total_long_trades":  total,
        "smart_win_rate":     round((summary_row["smart_wins"] or 0) / total * 100, 1),
        "smart_avg_pnl":      summary_row["smart_avg"] or 0,
        "avg_days_held":      summary_row["avg_days"] or 0,
        "active_signals":     active_count,
        "first_trade":        summary_row["first_trade"],
        "last_trade":         summary_row["last_trade"],
        # High-conviction (Turbo + Super) hero metrics
        "hc_trades":          hc_row["total"] or 0,
        "hc_win_rate":        round((hc_row["wins"] or 0) / hc_total * 100, 1),
        "hc_avg_pnl":         hc_row["avg_pnl"] or 0,
        "hc_total_pnl":       hc_row["total_pnl"] or 0,
    }

    # ── Per-engine cards ──────────────────────────────────────────────────────
    engines = []
    for bucket in BUCKETS:
        meta = BUCKET_META[bucket]

        # Overall stats for this bucket (year-filtered when year param is set)
        stats = con.execute(f"""
            SELECT
                COUNT(*)                                                    AS total,
                SUM(CASE WHEN t.pnl_pct>0 THEN 1 ELSE 0 END)              AS blind_wins,
                ROUND(AVG(t.pnl_pct),2)                                    AS blind_avg,
                ROUND(AVG({_smart_pnl_expr('e')}),2)                       AS smart_avg,
                SUM(CASE WHEN {_smart_pnl_expr('e')}>0 THEN 1 ELSE 0 END) AS smart_wins,
                ROUND(AVG(t.days_held),1)                                  AS avg_days,
                ROUND(SUM({_smart_pnl_expr('e')}),1)                       AS total_pnl
            FROM trade_log t
            LEFT JOIN execution_log e ON e.trade_log_id = t.id
            WHERE t.direction IN ('rally','long')
              AND json_extract(t.notes,'$.bucket') = ?
              {yr_cond}
        """, [bucket] + yr_params).fetchone()

        # 90-day rolling P&L (always trailing 90d from today, unaffected by year filter)
        pnl_90 = con.execute(f"""
            SELECT ROUND(AVG({_smart_pnl_expr('e')}),2) AS avg_pnl,
                   COUNT(*) AS n
            FROM trade_log t
            LEFT JOIN execution_log e ON e.trade_log_id = t.id
            WHERE t.direction IN ('rally','long')
              AND json_extract(t.notes,'$.bucket') = ?
              AND t.entry_date >= ?
        """, (bucket, _date_offset(90))).fetchone()

        # 180-day rolling P&L (always trailing 180d from today, unaffected by year filter)
        pnl_180 = con.execute(f"""
            SELECT ROUND(AVG({_smart_pnl_expr('e')}),2) AS avg_pnl,
                   COUNT(*) AS n
            FROM trade_log t
            LEFT JOIN execution_log e ON e.trade_log_id = t.id
            WHERE t.direction IN ('rally','long')
              AND json_extract(t.notes,'$.bucket') = ?
              AND t.entry_date >= ?
        """, (bucket, _date_offset(180))).fetchone()

        n = stats["total"] or 1

        # Active signals that would fit this bucket (use tier as proxy)
        tier_map = {"turbo": ["high_conviction"], "super": ["high_conviction", "medium"],
                    "standard": ["medium", "low"], "trap": []}
        bucket_active = [r for r in active_rows
                         if r["tier"] in tier_map.get(bucket, [])]
        if bucket == "turbo":
            bucket_active = [r for r in active_rows if r["opportunity_score"] >= 0.90]
        elif bucket == "super":
            bucket_active = [r for r in active_rows
                             if 0.80 <= r["opportunity_score"] < 0.90]
        elif bucket == "standard":
            bucket_active = [r for r in active_rows
                             if 0.65 <= r["opportunity_score"] < 0.80]
        else:
            bucket_active = []

        # Top stocks ranked by avg smart P&L for this bucket (year-filtered)
        top_rows = con.execute(f"""
            SELECT
                t.ticker,
                COUNT(*)                                                        AS total,
                SUM(CASE WHEN t.pnl_pct>0 THEN 1 ELSE 0 END)                  AS wins,
                ROUND(AVG({_smart_pnl_expr('e')}),2)                           AS avg_pnl,
                ROUND(SUM({_smart_pnl_expr('e')}),1)                           AS total_pnl,
                ROUND(AVG(t.days_held),1)                                      AS avg_days,
                MAX(t.entry_date)                                               AS last_trade,
                -- 90d sub-stats (always trailing window)
                SUM(CASE WHEN t.entry_date >= ? THEN 1 ELSE 0 END)             AS trades_90d,
                ROUND(AVG(CASE WHEN t.entry_date >= ?
                    THEN {_smart_pnl_expr('e')} END),2)                        AS avg_pnl_90d
            FROM trade_log t
            LEFT JOIN execution_log e ON e.trade_log_id = t.id
            WHERE t.direction IN ('rally','long')
              AND json_extract(t.notes,'$.bucket') = ?
              {yr_cond}
            GROUP BY t.ticker
            ORDER BY avg_pnl DESC
            LIMIT 10
        """, [_date_offset(90), _date_offset(90), bucket] + yr_params).fetchall()

        top_stocks = []
        for rank, row in enumerate(top_rows, 1):
            n_stock = row["total"] or 1
            top_stocks.append({
                "rank":        rank,
                "ticker":      row["ticker"],
                "total":       row["total"],
                "win_rate":    round((row["wins"] or 0) / n_stock * 100, 1),
                "avg_pnl":     row["avg_pnl"] or 0,
                "total_pnl":   row["total_pnl"] or 0,
                "avg_days":    row["avg_days"] or 0,
                "last_trade":  row["last_trade"],
                "trades_90d":  row["trades_90d"] or 0,
                "avg_pnl_90d": row["avg_pnl_90d"],
                "active":      row["ticker"] in active_tickers,
            })

        engines.append({
            "bucket":        bucket,
            "label":         meta["label"],
            "icon":          meta["icon"],
            "description":   meta["description"],
            "total_trades":  stats["total"] or 0,
            "win_rate":      round((stats["blind_wins"] or 0) / n * 100, 1),
            "smart_win_rate":round((stats["smart_wins"] or 0) / n * 100, 1),
            "avg_pnl":       stats["blind_avg"] or 0,
            "smart_avg_pnl": stats["smart_avg"] or 0,
            "avg_days":      stats["avg_days"] or 0,
            "total_pnl_all": stats["total_pnl"] or 0,
            "pnl_90d_avg":   pnl_90["avg_pnl"],
            "pnl_90d_trades":pnl_90["n"] or 0,
            "pnl_180d_avg":  pnl_180["avg_pnl"],
            "pnl_180d_trades":pnl_180["n"] or 0,
            "active_signals":len(bucket_active),
            "active_tickers":[r["ticker"] for r in bucket_active],
            "top_stocks":    top_stocks,
        })

    # ── Active signals detail ─────────────────────────────────────────────────
    def _score_to_bucket(score: float) -> str:
        if score >= 0.90: return "turbo"
        if score >= 0.80: return "super"
        return "standard"

    active_signals = [
        {
            "ticker":            r["ticker"],
            "bucket":            _score_to_bucket(r["opportunity_score"]),
            "tier":              r["tier"],
            "opportunity_score": round(r["opportunity_score"], 4),
            "credibility":       r["credibility"],
            "latest_date":       r["latest_date"],
            "setup_summary":     (r["setup_summary"] or "")[:200],
        }
        for r in active_rows
    ]

    con.close()
    return {
        "as_of":          str(date.today()),
        "summary":        summary,
        "engines":        engines,
        "active_signals": active_signals,
    }


# ── /api/swing/trades ─────────────────────────────────────────────────────────

@router.get("/swing/trades")
def swing_trades(
    ticker:  Optional[str] = Query(None),
    bucket:  Optional[str] = Query(None),
    year:    Optional[str] = Query(None),
    limit:   int           = Query(200, ge=1, le=2000),
    offset:  int           = Query(0, ge=0),
):
    """
    Long-only swing trades with smart entry P&L.
    Replaces backtest /trades for production cash-equity use.
    """
    con = _conn()
    cond = ["t.direction IN ('rally','long')",
            "COALESCE(json_extract(t.notes,'$.bucket'),'standard') != 'trap'"]
    params: list = []

    if ticker:
        cond.append("t.ticker = ?");                       params.append(ticker)
    if bucket:
        cond.append("json_extract(t.notes,'$.bucket')=?"); params.append(bucket.lower())
    if year:
        cond.append("json_extract(t.notes,'$.year')=?");   params.append(year)

    where = "WHERE " + " AND ".join(cond)

    count = con.execute(
        f"SELECT COUNT(*) FROM trade_log t {where}", params
    ).fetchone()[0]

    rows = con.execute(f"""
        SELECT
            t.id, t.ticker, t.direction,
            t.signal_date, t.entry_date, t.exit_date,
            t.entry_price, t.stop_price, t.target_price,
            t.exit_price, t.exit_reason, t.days_held, t.pnl_pct,
            t.notes,
            e.exec_code, e.trade_taken, e.entry_window,
            e.smart_entry_price, e.smart_pnl_pct, e.blind_pnl_pct,
            e.gap_category, e.gap_pct, e.day_move_pct, e.rs_vs_nifty,
            {_smart_pnl_expr('e')} AS effective_pnl
        FROM trade_log t
        LEFT JOIN execution_log e ON e.trade_log_id = t.id
        {where}
        ORDER BY t.entry_date DESC
        LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()

    trades = []
    for r in rows:
        n           = json.loads(r["notes"]) if r["notes"] else {}
        entry_p     = float(r["entry_price"]  or 0)
        stop_p      = float(r["stop_price"]   or 0)
        target_p    = float(r["target_price"] or 0)
        rr          = round((target_p - entry_p) / (entry_p - stop_p), 2) if entry_p > stop_p > 0 else 2.0
        taken       = bool(r["trade_taken"]) if r["trade_taken"] is not None else None
        smart_ep    = r["smart_entry_price"]
        eff_entry   = round(float(smart_ep), 2) if (taken and smart_ep) else entry_p
        trades.append({
            "id":                  r["id"],
            "trade_id":            r["id"],
            "signal_id":           f"SIG-{r['id']:06d}",
            "ticker":              r["ticker"],
            "direction":           r["direction"],
            "timeframe":           n.get("timeframe", "1D"),
            "signal_type":         n.get("signal_type", "AI Pattern"),
            "pattern":             n.get("pattern", ""),
            "signal_date":         r["signal_date"],
            "signal_datetime":     n.get("signal_time", f"{r['signal_date']} 15:30:00 IST"),
            "entry_date":          r["entry_date"],
            "entry_datetime":      n.get("entry_time", f"{r['entry_date']} 09:15:00 IST"),
            "delay_label":         n.get("delay_label", "overnight"),
            "entry_price":         entry_p,
            "effective_entry_price": eff_entry,
            "stop_price":          stop_p,
            "target_price":        target_p,
            "rr":                  rr,
            "exit_date":           r["exit_date"],
            "exit_price":          float(r["exit_price"] or 0),
            "exit_reason":         r["exit_reason"],
            "days_held":           int(r["days_held"] or 0),
            "pnl_pct":             round(float(r["pnl_pct"] or 0), 2),
            "effective_pnl":       round(float(r["effective_pnl"] or 0), 2),
            "bucket":              n.get("bucket", ""),
            "year":                (r["entry_date"] or "")[:4],
            "tier":                n.get("tier", ""),
            "credibility":         n.get("credibility", ""),
            "opportunity_score":   n.get("opportunity_score"),
            "reason_code":         n.get("reason_code", "FRESH_SIGNAL"),
            "multi_pattern_count": n.get("multi_pattern_count", 1),
            "mfe_pct":             n.get("mfe_pct"),
            "mae_pct":             n.get("mae_pct"),
            "mpi_pct":             n.get("mpi_pct"),
            "exec_code":           r["exec_code"],
            "trade_taken":         taken,
            "entry_window":        r["entry_window"],
            "smart_entry_price":   round(float(smart_ep), 2) if smart_ep else None,
            "smart_pnl_pct":       r["smart_pnl_pct"],
            "gap_category":        r["gap_category"],
            "gap_pct":             r["gap_pct"],
            "day_move_pct":        r["day_move_pct"],
            "rs_vs_nifty":         r["rs_vs_nifty"],
        })

    con.close()
    return {"count": count, "trades": trades}


# ── /api/swing/stock-profile ──────────────────────────────────────────────────

@router.get("/swing/stock-profile")
def swing_stock_profile(ticker: str = Query(...)):
    """
    Full profile for a single stock:
    - All-time / 90d / 180d stats per engine bucket (long only)
    - Best/worst patterns
    - Active signal if present
    """
    con = _conn()

    # Per-bucket breakdown
    buckets = con.execute(f"""
        SELECT
            json_extract(t.notes,'$.bucket') AS bucket,
            COUNT(*)                                                        AS total,
            SUM(CASE WHEN t.pnl_pct>0 THEN 1 ELSE 0 END)                  AS wins,
            ROUND(AVG({_smart_pnl_expr('e')}),2)                           AS avg_pnl,
            ROUND(SUM({_smart_pnl_expr('e')}),1)                           AS total_pnl,
            ROUND(AVG(t.days_held),1)                                      AS avg_days,
            MAX(t.entry_date)                                               AS last_trade,
            ROUND(AVG(CASE WHEN t.entry_date >= ?
                THEN {_smart_pnl_expr('e')} END),2)                        AS avg_pnl_90d
        FROM trade_log t
        LEFT JOIN execution_log e ON e.trade_log_id = t.id
        WHERE t.direction IN ('rally','long') AND t.ticker = ?
        GROUP BY bucket ORDER BY avg_pnl DESC
    """, (_date_offset(90), ticker)).fetchall()

    # Active signal
    active = con.execute("""
        SELECT tier, opportunity_score, credibility, latest_date, setup_summary
        FROM live_opportunities
        WHERE ticker = ? AND direction = 'rally'
        ORDER BY opportunity_score DESC LIMIT 1
    """, (ticker,)).fetchone()

    con.close()
    return {
        "ticker":  ticker,
        "active_signal": dict(active) if active else None,
        "buckets": [
            {
                "bucket":     r["bucket"],
                "total":      r["total"],
                "win_rate":   round((r["wins"] or 0) / (r["total"] or 1) * 100, 1),
                "avg_pnl":    r["avg_pnl"] or 0,
                "total_pnl":  r["total_pnl"] or 0,
                "avg_days":   r["avg_days"] or 0,
                "last_trade": r["last_trade"],
                "avg_pnl_90d":r["avg_pnl_90d"],
            }
            for r in buckets
        ],
    }


# ── /api/swing/tickers ───────────────────────────────────────────────────────

@router.get("/swing/tickers")
def swing_tickers():
    """
    Returns all distinct tickers that have trade_log data (long-only, no trap).
    Used by the frontend to build the dynamic stock filter dropdown.
    """
    con = _conn()
    rows = con.execute("""
        SELECT DISTINCT t.ticker
        FROM trade_log t
        WHERE t.direction IN ('rally','long')
          AND COALESCE(json_extract(t.notes,'$.bucket'),'standard') != 'trap'
        ORDER BY t.ticker
    """).fetchall()
    con.close()
    return {"tickers": [r["ticker"] for r in rows]}


# ── /api/swing/leaderboard ────────────────────────────────────────────────────

@router.get("/swing/leaderboard")
def swing_leaderboard(
    bucket:  Optional[str] = Query(None),
    period:  str           = Query("all", regex="^(90d|180d|all)$"),
    sort_by: str           = Query("avg_pnl", regex="^(avg_pnl|win_rate|total_trades|total_pnl)$"),
    limit:   int           = Query(20),
):
    """
    Ranked stock leaderboard for all engines (or one bucket).
    Designed to scale to 200-300 stocks.
    """
    con = _conn()

    bucket_filter = "AND json_extract(t.notes,'$.bucket') = ?" if bucket else ""
    bucket_params = [bucket.lower()] if bucket else []

    date_filter = ""
    date_params: list = []
    if period == "90d":
        date_filter = f"AND t.entry_date >= ?"
        date_params = [_date_offset(90)]
    elif period == "180d":
        date_filter = f"AND t.entry_date >= ?"
        date_params = [_date_offset(180)]

    rows = con.execute(f"""
        SELECT
            t.ticker,
            json_extract(t.notes,'$.bucket')                                AS bucket,
            COUNT(*)                                                        AS total_trades,
            SUM(CASE WHEN t.pnl_pct>0 THEN 1 ELSE 0 END)                  AS wins,
            ROUND(AVG({_smart_pnl_expr('e')}),2)                           AS avg_pnl,
            ROUND(SUM({_smart_pnl_expr('e')}),1)                           AS total_pnl,
            ROUND(AVG(t.days_held),1)                                      AS avg_days,
            MAX(t.entry_date)                                               AS last_trade
        FROM trade_log t
        LEFT JOIN execution_log e ON e.trade_log_id = t.id
        WHERE t.direction IN ('rally','long')
          {bucket_filter}
          {date_filter}
        GROUP BY t.ticker, json_extract(t.notes,'$.bucket')
        HAVING total_trades >= 2
        ORDER BY {sort_by} DESC
        LIMIT ?
    """, bucket_params + date_params + [limit]).fetchall()

    # Get active tickers
    active_tickers_rows = con.execute(
        "SELECT ticker FROM live_opportunities WHERE direction='rally'"
    ).fetchall()
    active_set = {r[0] for r in active_tickers_rows}

    leaderboard = []
    for rank, row in enumerate(rows, 1):
        n = row["total_trades"] or 1
        leaderboard.append({
            "rank":         rank,
            "ticker":       row["ticker"],
            "bucket":       row["bucket"],
            "total_trades": row["total_trades"],
            "win_rate":     round((row["wins"] or 0) / n * 100, 1),
            "avg_pnl":      row["avg_pnl"] or 0,
            "total_pnl":    row["total_pnl"] or 0,
            "avg_days":     row["avg_days"] or 0,
            "last_trade":   row["last_trade"],
            "active":       row["ticker"] in active_set,
        })

    con.close()
    return {"period": period, "count": len(leaderboard), "leaderboard": leaderboard}
