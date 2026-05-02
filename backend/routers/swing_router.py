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
import sys
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Query

router  = APIRouter()
_HERE   = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent))
from db import get_conn

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


def _conn():
    return get_conn()


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


def _ticker_index_filter(ticker: Optional[str], index: Optional[str]) -> tuple[str, list]:
    """
    Build a SQL fragment + params to filter by ticker and/or index membership.
    Stock wins over category: if ticker is set, the index filter is ignored.
    Returns ('', []) when neither filter is set.
    """
    if ticker:
        return (" AND t.ticker = ? ", [ticker.upper()])
    if index:
        return (
            " AND t.ticker IN (SELECT ticker FROM stock_index_membership WHERE index_name = ?) ",
            [index],
        )
    return ("", [])


def _ensure_membership_table_exists(con) -> None:
    """Create membership table if missing — keeps swing endpoints safe before first refresh."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS stock_index_membership (
            ticker      TEXT NOT NULL,
            index_name  TEXT NOT NULL,
            added_on    TEXT NOT NULL DEFAULT (CURRENT_DATE),
            PRIMARY KEY (ticker, index_name)
        )
    """)


def _ensure_universe_stub(con) -> None:
    """Make sector LEFT JOIN safe even if universe table is absent (older DB snapshots)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS universe (
            symbol          TEXT NOT NULL,
            exchange        TEXT NOT NULL DEFAULT 'NSE',
            sector          TEXT,
            is_active       INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (symbol, exchange)
        )
    """)


# ── /api/swing/overview ───────────────────────────────────────────────────────

@router.get("/swing/overview")
def swing_overview(
    year:   Optional[str] = Query(None),
    ticker: Optional[str] = Query(None),
    index:  Optional[str] = Query(None, description="NSE index name, e.g. 'NIFTY 50'"),
):
    """
    Full overview for the Swing Trading Terminal.
    Returns top-level summary, engine bucket cards, and top-ranked stocks per engine.
    Long-only: direction IN ('rally', 'long').
    Uses smart entry P&L where execution_log has trade_taken=1.

    Filters:
      - year:   filters historical stats by entry_date year (rolling 90/180d unaffected)
      - ticker: restricts every aggregate to one ticker
      - index:  restricts to members of an NSE index (e.g. 'NIFTY 50')
                Stock wins: if ticker is set, index is ignored.

    Hero ('hc_*') scope is unified: all five hero numbers share the same
    Turbo+Super filter — including avg_days_held, which used to come from
    the broader 'all non-trap' scope.
    """
    con = _conn()
    _ensure_membership_table_exists(con)

    yr_cond   = "AND strftime('%Y', t.entry_date) = ?" if year else ""
    yr_params = [year] if year else []

    tk_cond, tk_params = _ticker_index_filter(ticker, index)

    # ── Active signals from live_opportunities (long/rally only) ──────────────
    # Active-signal count is left global for legacy callers; the new
    # /swing/active-signals endpoint owns this surface going forward.
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

    # ── Overall summary (trap excluded, optional year/ticker/index filter) ────
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
        WHERE t.direction IN ('rally','long') AND {NO_TRAP} {yr_cond} {tk_cond}
    """, yr_params + tk_params).fetchone()

    # ── High-conviction summary (turbo + super only) ─────────────────────────
    # avg_days NOW lives here so the hero box has one consistent scope.
    hc_row = con.execute(f"""
        SELECT
            COUNT(*)                                                AS total,
            SUM(CASE WHEN {_smart_pnl_expr('e')} > 0 THEN 1 ELSE 0 END) AS wins,
            ROUND(AVG({_smart_pnl_expr('e')}), 2)                  AS avg_pnl,
            ROUND(SUM({_smart_pnl_expr('e')}), 1)                  AS total_pnl,
            ROUND(AVG(t.days_held), 1)                             AS avg_days
        FROM trade_log t
        LEFT JOIN execution_log e ON e.trade_log_id = t.id
        WHERE t.direction IN ('rally','long')
          AND json_extract(t.notes,'$.bucket') IN ('turbo','super')
          {yr_cond} {tk_cond}
    """, yr_params + tk_params).fetchone()

    total    = summary_row["total"] or 1
    hc_total = hc_row["total"] or 1
    summary = {
        "total_long_trades":  summary_row["total"] or 0,
        "smart_win_rate":     round((summary_row["smart_wins"] or 0) / total * 100, 1),
        "smart_avg_pnl":      summary_row["smart_avg"] or 0,
        # avg_days_held is now hero-scoped (Turbo+Super, with same filters).
        "avg_days_held":      hc_row["avg_days"] or 0,
        "active_signals":     active_count,
        "first_trade":        summary_row["first_trade"],
        "last_trade":         summary_row["last_trade"],
        # High-conviction (Turbo + Super) hero metrics — all share same scope
        "hc_trades":          hc_row["total"] or 0,
        "hc_win_rate":        round((hc_row["wins"] or 0) / hc_total * 100, 1),
        "hc_avg_pnl":         hc_row["avg_pnl"] or 0,
        "hc_total_pnl":       hc_row["total_pnl"] or 0,
    }

    # ── Per-engine cards ──────────────────────────────────────────────────────
    engines = []
    for bucket in BUCKETS:
        meta = BUCKET_META[bucket]

        # Overall stats for this bucket (year + ticker/index filters when set)
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
              {yr_cond} {tk_cond}
        """, [bucket] + yr_params + tk_params).fetchone()

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
              {yr_cond} {tk_cond}
            GROUP BY t.ticker
            ORDER BY avg_pnl DESC
            LIMIT 10
        """, [_date_offset(90), _date_offset(90), bucket] + yr_params + tk_params).fetchall()

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


# ── /api/swing/active-signals ─────────────────────────────────────────────────

@router.get("/swing/active-signals")
def swing_active_signals(
    engine:      Optional[str] = Query(None, description="bucket label inferred from score: turbo|super|standard"),
    index:       Optional[str] = Query(None, description="NSE index, e.g. 'NIFTY 50'"),
    sector:      Optional[str] = Query(None),
    credibility: Optional[str] = Query(None),
    search:      Optional[str] = Query(None, description="ticker substring (case-insensitive)"),
    ticker:      Optional[str] = Query(None),
):
    """
    Long-only (rally) live opportunities with independent filters.
    Decoupled from /swing/overview so its own filter row in the UI doesn't
    affect the engine performance hero.
    Engine bucket here is derived from opportunity_score thresholds:
      turbo >= 0.90, super >= 0.80, standard < 0.80.
    """
    con = _conn()
    _ensure_membership_table_exists(con)
    _ensure_universe_stub(con)

    clauses = ["lo.direction = 'rally'"]
    params: list = []

    if ticker:
        clauses.append("lo.ticker = ?")
        params.append(ticker.upper())
    elif index:
        clauses.append("lo.ticker IN (SELECT ticker FROM stock_index_membership WHERE index_name = ?)")
        params.append(index)
    if sector:
        clauses.append("lo.ticker IN (SELECT symbol FROM universe WHERE sector = ?)")
        params.append(sector)
    if credibility:
        clauses.append("lo.credibility = ?")
        params.append(credibility)
    if search:
        clauses.append("UPPER(lo.ticker) LIKE ?")
        params.append(f"%{search.upper()}%")

    rows = con.execute(f"""
        SELECT lo.ticker, lo.direction, lo.tier, lo.opportunity_score,
               lo.credibility, lo.latest_date, lo.setup_summary,
               lo.current_close, u.sector
        FROM live_opportunities lo
        LEFT JOIN universe u ON u.symbol = lo.ticker
        WHERE {' AND '.join(clauses)}
        ORDER BY lo.opportunity_score DESC
    """, params).fetchall()

    def _score_to_bucket(score: float) -> str:
        if score >= 0.90: return "turbo"
        if score >= 0.80: return "super"
        return "standard"

    out = []
    for r in rows:
        b = _score_to_bucket(r["opportunity_score"] or 0)
        if engine and engine.lower() != b:
            continue
        out.append({
            "ticker":            r["ticker"],
            "engine":            b,
            "tier":              r["tier"],
            "opportunity_score": round(r["opportunity_score"] or 0, 4),
            "credibility":       r["credibility"],
            "latest_date":       r["latest_date"],
            "current_close":     r["current_close"],
            "sector":            r["sector"],
            "setup_summary":     (r["setup_summary"] or "")[:240],
        })

    con.close()
    return {"count": len(out), "signals": out}


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
        HAVING COUNT(*) >= 2
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


# ── /api/swing/breadth ────────────────────────────────────────────────────────

@router.get("/swing/breadth")
def swing_breadth():
    """Universe-wide breadth & momentum snapshot for the macro strip.
    All numbers come from our own ohlc_daily — no third-party feed."""
    con = _conn()
    _ensure_universe_stub(con)

    latest_row = con.execute(
        "SELECT MAX(trade_date) AS dt FROM ohlc_daily WHERE market='NSE'"
    ).fetchone()
    latest = latest_row["dt"]
    if not latest:
        con.close()
        return {"as_of": None, "total_stocks": 0, "advancers": 0, "decliners": 0,
                "unchanged": 0, "avg_pct": 0, "best_stock": None, "worst_stock": None,
                "best_sector": None, "worst_sector": None,
                "signals_total": 0, "signals_hc": 0, "last_pipeline_run": None}

    prior = con.execute(
        "SELECT MAX(trade_date) AS dt FROM ohlc_daily WHERE market='NSE' AND trade_date < ?",
        (latest,),
    ).fetchone()["dt"]

    rows = con.execute("""
        SELECT t.ticker, t.close AS today_close, p.close AS prior_close, u.sector
        FROM ohlc_daily t
        LEFT JOIN ohlc_daily p ON p.ticker = t.ticker AND p.trade_date = ? AND p.market = 'NSE'
        LEFT JOIN universe u ON u.symbol = t.ticker AND u.exchange = 'NSE'
        WHERE t.trade_date = ? AND t.market = 'NSE'
    """, (prior, latest)).fetchall()

    movers: list = []
    sectors: dict = {}
    advancers = decliners = unchanged = 0
    pct_sum = 0.0
    for r in rows:
        tc, pc = r["today_close"], r["prior_close"]
        if tc is None or pc is None or pc == 0:
            continue
        pct = (tc - pc) / pc * 100.0
        movers.append((r["ticker"], pct, r["sector"]))
        pct_sum += pct
        if   pct > 0.05: advancers += 1
        elif pct < -0.05: decliners += 1
        else: unchanged += 1
        if r["sector"]:
            sectors.setdefault(r["sector"], []).append(pct)

    n = len(movers)
    sorted_movers = sorted(movers, key=lambda x: x[1], reverse=True)
    best_s  = sorted_movers[0]  if sorted_movers else None
    worst_s = sorted_movers[-1] if sorted_movers else None

    sector_avg = [(sec, sum(vs) / len(vs), len(vs)) for sec, vs in sectors.items() if vs]
    sector_sorted = sorted(sector_avg, key=lambda x: x[1], reverse=True)
    best_sec  = sector_sorted[0]  if sector_sorted else None
    worst_sec = sector_sorted[-1] if sector_sorted else None

    sig_total = con.execute(
        "SELECT COUNT(*) FROM live_opportunities WHERE direction = 'rally'"
    ).fetchone()[0]
    sig_hc = con.execute(
        "SELECT COUNT(*) FROM live_opportunities WHERE direction = 'rally' AND opportunity_score >= 0.90"
    ).fetchone()[0]

    last_pipeline_run = None
    try:
        import main as m
        last_pipeline_run = m._pipeline_status.get("last_run")
    except Exception:
        pass

    con.close()
    return {
        "as_of":        latest,
        "total_stocks": n,
        "advancers":    advancers,
        "decliners":    decliners,
        "unchanged":    unchanged,
        "avg_pct":      round(pct_sum / n, 2) if n else 0,
        "best_stock":   {"ticker": best_s[0],  "pct": round(best_s[1],  2), "sector": best_s[2]}  if best_s  else None,
        "worst_stock":  {"ticker": worst_s[0], "pct": round(worst_s[1], 2), "sector": worst_s[2]} if worst_s else None,
        "best_sector":  {"sector": best_sec[0],  "avg_pct": round(best_sec[1],  2), "members": best_sec[2]}  if best_sec  else None,
        "worst_sector": {"sector": worst_sec[0], "avg_pct": round(worst_sec[1], 2), "members": worst_sec[2]} if worst_sec else None,
        "signals_total":     sig_total,
        "signals_hc":        sig_hc,
        "last_pipeline_run": last_pipeline_run,
    }


# ── /api/swing/top-movers ─────────────────────────────────────────────────────

@router.get("/swing/top-movers")
def swing_top_movers(limit: int = Query(10, ge=1, le=50)):
    """Top gainers and losers from the latest trading day."""
    con = _conn()
    _ensure_universe_stub(con)

    latest = con.execute(
        "SELECT MAX(trade_date) AS dt FROM ohlc_daily WHERE market='NSE'"
    ).fetchone()["dt"]
    if not latest:
        con.close()
        return {"as_of": None, "gainers": [], "losers": []}
    prior = con.execute(
        "SELECT MAX(trade_date) AS dt FROM ohlc_daily WHERE market='NSE' AND trade_date < ?",
        (latest,),
    ).fetchone()["dt"]

    active_tickers = {
        r[0] for r in con.execute(
            "SELECT ticker FROM live_opportunities WHERE direction='rally'"
        ).fetchall()
    }

    rows = con.execute("""
        SELECT t.ticker, t.close, t.volume, p.close AS prior_close, u.sector
        FROM ohlc_daily t
        LEFT JOIN ohlc_daily p ON p.ticker = t.ticker AND p.trade_date = ? AND p.market = 'NSE'
        LEFT JOIN universe u ON u.symbol = t.ticker AND u.exchange = 'NSE'
        WHERE t.trade_date = ? AND t.market = 'NSE'
    """, (prior, latest)).fetchall()

    movers = []
    for r in rows:
        if r["close"] is None or r["prior_close"] is None or r["prior_close"] == 0:
            continue
        pct = (r["close"] - r["prior_close"]) / r["prior_close"] * 100.0
        movers.append({
            "ticker":  r["ticker"],
            "sector":  r["sector"] or "—",
            "close":   round(r["close"], 2),
            "pct":     round(pct, 2),
            "volume":  r["volume"],
            "active_signal": r["ticker"] in active_tickers,
        })
    movers.sort(key=lambda x: x["pct"], reverse=True)

    con.close()
    return {
        "as_of":   latest,
        "gainers": movers[:limit],
        "losers":  list(reversed(movers[-limit:])) if movers else [],
    }


# ── /api/swing/sector-stats ───────────────────────────────────────────────────

@router.get("/swing/sector-stats")
def swing_sector_stats():
    """Per-sector aggregated move for latest day. Drives the sector heatmap."""
    con = _conn()
    _ensure_universe_stub(con)

    latest = con.execute(
        "SELECT MAX(trade_date) AS dt FROM ohlc_daily WHERE market='NSE'"
    ).fetchone()["dt"]
    if not latest:
        con.close()
        return {"as_of": None, "sectors": []}
    prior = con.execute(
        "SELECT MAX(trade_date) AS dt FROM ohlc_daily WHERE market='NSE' AND trade_date < ?",
        (latest,),
    ).fetchone()["dt"]

    rows = con.execute("""
        SELECT u.sector AS sector, t.ticker, t.close, p.close AS prior_close
        FROM ohlc_daily t
        LEFT JOIN ohlc_daily p ON p.ticker = t.ticker AND p.trade_date = ? AND p.market = 'NSE'
        LEFT JOIN universe u ON u.symbol = t.ticker AND u.exchange = 'NSE'
        WHERE t.trade_date = ? AND t.market = 'NSE'
    """, (prior, latest)).fetchall()

    by_sector: dict = {}
    for r in rows:
        sec = r["sector"] or "Unknown"
        if r["close"] is None or r["prior_close"] is None or r["prior_close"] == 0:
            continue
        pct = (r["close"] - r["prior_close"]) / r["prior_close"] * 100.0
        s = by_sector.setdefault(sec, {"sector": sec, "members": 0, "advancers": 0,
                                       "decliners": 0, "pct_sum": 0.0,
                                       "best_ticker": None, "best_pct": -1e9})
        s["members"]   += 1
        s["pct_sum"]   += pct
        s["advancers"] += (1 if pct > 0.05  else 0)
        s["decliners"] += (1 if pct < -0.05 else 0)
        if pct > s["best_pct"]:
            s["best_pct"] = pct
            s["best_ticker"] = r["ticker"]

    sectors = []
    for s in by_sector.values():
        n = s["members"] or 1
        sectors.append({
            "sector":      s["sector"],
            "members":     s["members"],
            "advancers":   s["advancers"],
            "decliners":   s["decliners"],
            "avg_pct":     round(s["pct_sum"] / n, 2),
            "best_ticker": s["best_ticker"],
            "best_pct":    round(s["best_pct"], 2),
        })
    sectors.sort(key=lambda x: x["avg_pct"], reverse=True)
    con.close()
    return {"as_of": latest, "sectors": sectors}


# ── /api/swing/activity-feed ──────────────────────────────────────────────────

@router.get("/swing/activity-feed")
def swing_activity_feed(limit: int = Query(20, ge=1, le=100)):
    """Recent engine-driven events for the right-rail feed."""
    con = _conn()
    items: list = []

    # Latest exited trades
    exits = con.execute("""
        SELECT t.ticker, t.exit_date, t.exit_reason, t.pnl_pct,
               json_extract(t.notes,'$.bucket') AS bucket
        FROM trade_log t
        WHERE t.exit_date IS NOT NULL AND t.direction IN ('rally','long')
        ORDER BY t.exit_date DESC, t.id DESC
        LIMIT ?
    """, (limit,)).fetchall()
    for r in exits:
        items.append({
            "kind":    "trade_exit",
            "when":    r["exit_date"],
            "ticker":  r["ticker"],
            "engine":  (r["bucket"] or "standard").lower(),
            "outcome": r["exit_reason"],
            "pnl":     r["pnl_pct"],
            "title":   r["ticker"] + " closed " + (r["exit_reason"] or ""),
            "detail":  ("{:+.2f}%".format(r["pnl_pct"])) if r["pnl_pct"] is not None else None,
        })

    # Fresh signals
    sigs = con.execute("""
        SELECT ticker, opportunity_score, tier, latest_date, created_at, setup_summary
        FROM live_opportunities
        WHERE direction = 'rally'
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    for r in sigs:
        sc = r["opportunity_score"] or 0
        engine = "turbo" if sc >= 0.90 else ("super" if sc >= 0.80 else "standard")
        items.append({
            "kind":   "signal_fired",
            "when":   r["created_at"] or r["latest_date"],
            "ticker": r["ticker"],
            "engine": engine,
            "score":  round(sc, 3),
            "tier":   r["tier"],
            "title":  r["ticker"] + " fired " + engine.upper(),
            "detail": (r["setup_summary"] or "")[:140],
        })

    # Pipeline status
    try:
        import main as m
        st = m._pipeline_status
        if st.get("last_run"):
            items.append({
                "kind":   "pipeline",
                "when":   st["last_run"],
                "ticker": None,
                "engine": None,
                "title":  "Pipeline " + (st.get("last_result") or "ran"),
                "detail": st.get("last_result") or "",
            })
    except Exception:
        pass

    items.sort(key=lambda x: (x["when"] or ""), reverse=True)
    con.close()
    return {"count": len(items[:limit]), "items": items[:limit]}
