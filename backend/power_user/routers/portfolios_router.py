"""/api/power/portfolios/* — Co-Trader public read API (Sprint 5d).

All endpoints public + read-only. Writes happen via the scheduler
(portfolio_engine.run_eod_for_date). Source-of-truth for the 5 portfolio
parameters lives in portfolio_defs.py, NOT in the DB — the DB row is a
materialised mirror updated on every boot via seed_portfolio_definitions.

Endpoints:
  GET  /api/power/portfolios                         list summary cards
  GET  /api/power/portfolios/sizing?capital=N        sizing assistant for a capital level
  GET  /api/power/portfolios/{slug}                  portfolio detail + current equity
  GET  /api/power/portfolios/{slug}/positions        open positions
  GET  /api/power/portfolios/{slug}/trades?limit&offset   closed trades, paginated
  GET  /api/power/portfolios/{slug}/equity?from&to   equity curve points
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import Field

from fastapi import Depends as _Depends

from ..services.portfolio_sizing import compute_sizing
from .dependencies import current_paid_user_required, get_db, hash_ip_ua, log_request

log = logging.getLogger("kanida.power_user.portfolios_router")

# Paywall (M4 / CONTRACT §4): EVERY route in this router is product data
# (portfolio summaries, positions, closed trades, equity curves, sizing).
# There are no health/meta routes here, so a router-level dependency gates
# them all uniformly — login required (401) then billing checked (402).
router = APIRouter(
    prefix="/api/power/portfolios",
    tags=["power_user_portfolios"],
    dependencies=[_Depends(current_paid_user_required)],
)


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def _portfolio_row(con: sqlite3.Connection, slug: str) -> sqlite3.Row:
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT * FROM portfolio_definitions WHERE slug = ? AND is_active = 1", (slug,)
    ).fetchone()
    if not row:
        raise HTTPException(404, {"code": "PORTFOLIO_NOT_FOUND",
                                    "message": f"No portfolio with slug '{slug}'"})
    return row


def _latest_equity(
    con: sqlite3.Connection, portfolio_id: int,
) -> Optional[sqlite3.Row]:
    con.row_factory = sqlite3.Row
    return con.execute("""
        SELECT * FROM portfolio_equity_history
         WHERE portfolio_id = ?
         ORDER BY trade_date DESC
         LIMIT 1
    """, (portfolio_id,)).fetchone()


def _equity_at_or_before(
    con: sqlite3.Connection, portfolio_id: int, cutoff_date: str,
) -> Optional[sqlite3.Row]:
    """Last equity row <= cutoff_date. Used for YTD / MTD baselines."""
    con.row_factory = sqlite3.Row
    return con.execute("""
        SELECT * FROM portfolio_equity_history
         WHERE portfolio_id = ? AND trade_date <= ?
         ORDER BY trade_date DESC
         LIMIT 1
    """, (portfolio_id, cutoff_date)).fetchone()


def _cash_out_metrics(
    con: sqlite3.Connection,
    portfolio_id: int,
    capital_start: float,
    today_pnl_rs: float,
    today_pnl_pct: float,
) -> Dict[str, Any]:
    """Cash Out mode — operator architecture (2026-05-16):

      ONE position book. The trades that exist are the trades the compounded
      engine actually ran. We don't simulate a counterfactual book.

      Cash Out cumulative return = sum(realized pnl_rs from closed trades)
                                   / capital_start × 100.

      Today's P&L is IDENTICAL in both modes (it's the actual MTM change of
      the actual open positions — already computed and passed in here).

      Cash Out's mental model: profits "go to your wallet" the moment a
      trade closes. The book itself still uses the engine's compounded
      sizing, but the DISPLAY tells you what your cumulative withdrawals
      would have been.
    """
    row = con.execute("""
        SELECT
          COALESCE(SUM(pnl_rs), 0)                                AS realized_total,
          COALESCE(SUM(CASE WHEN substr(exit_date,1,4) = strftime('%Y','now') THEN pnl_rs END), 0) AS realized_ytd,
          COALESCE(SUM(CASE WHEN substr(exit_date,1,7) = strftime('%Y-%m','now') THEN pnl_rs END), 0) AS realized_mtd
        FROM portfolio_positions
        WHERE portfolio_id = ? AND exit_date IS NOT NULL AND pnl_rs IS NOT NULL
    """, (portfolio_id,)).fetchone()

    realized_total = float(row[0] or 0)
    realized_ytd   = float(row[1] or 0)
    realized_mtd   = float(row[2] or 0)

    pct = lambda n: (n / capital_start) * 100.0 if capital_start else 0.0
    return {
        "total_pnl_rs":      realized_total,
        "total_return_pct":  pct(realized_total),
        # Today's P&L mirrors Reinvest — operator requirement: the actual MTM
        # day's change of the actual positions, identical in both views.
        "today_pnl_rs":      today_pnl_rs,
        "today_pnl_pct":     today_pnl_pct,
        "ytd_return_pct":    pct(realized_ytd),
        "mtd_return_pct":    pct(realized_mtd),
    }


def _ytd_mtd_for(
    con: sqlite3.Connection, portfolio_id: int, latest: sqlite3.Row,
) -> Dict[str, float]:
    """Return YTD and MTD return % in Compound mode for the latest equity row."""
    today  = latest["trade_date"]            # 'YYYY-MM-DD'
    year   = today[:4]
    month  = today[:7]
    # Baseline = last equity row BEFORE Jan 1 of current year / 1st of month.
    # If no prior row exists (portfolio inception this year/month), baseline =
    # capital_start so YTD == cumulative_return.
    capital_start = float(latest["total_equity"]) - 0  # we have it from the row's history
    # cheaper: re-read portfolio_definitions
    cap_row = con.execute(
        "SELECT virtual_capital_start FROM portfolio_definitions WHERE id = ?",
        (portfolio_id,)
    ).fetchone()
    capital_start = float(cap_row[0]) if cap_row else 5_000_000.0

    cur_equity = float(latest["total_equity"])

    jan1     = f"{year}-01-01"
    month1st = f"{month}-01"
    ytd_base = _equity_at_or_before(con, portfolio_id, f"{int(year)-1}-12-31")
    mtd_base = _equity_at_or_before(con, portfolio_id,
                                      (datetime.fromisoformat(month1st) - timedelta(days=1)).date().isoformat())

    ytd_base_eq = float(ytd_base["total_equity"]) if ytd_base else capital_start
    mtd_base_eq = float(mtd_base["total_equity"]) if mtd_base else capital_start

    ytd_pct = (cur_equity / ytd_base_eq - 1.0) * 100.0 if ytd_base_eq else 0.0
    mtd_pct = (cur_equity / mtd_base_eq - 1.0) * 100.0 if mtd_base_eq else 0.0
    return {"ytd_pct": ytd_pct, "mtd_pct": mtd_pct,
            "ytd_baseline_equity": ytd_base_eq, "mtd_baseline_equity": mtd_base_eq}


def _best_worst_from_history(
    con: sqlite3.Connection, portfolio_id: int,
) -> Dict[str, Any]:
    """Live best-win + worst-loss + worst-stretch derived from this portfolio's
    actual trade & equity history (not the validated backtest)."""
    con.row_factory = sqlite3.Row

    best = con.execute("""
        SELECT symbol, pnl_pct, exit_date FROM portfolio_positions
         WHERE portfolio_id = ? AND pnl_pct IS NOT NULL
         ORDER BY pnl_pct DESC LIMIT 1
    """, (portfolio_id,)).fetchone()
    worst = con.execute("""
        SELECT symbol, pnl_pct, exit_date FROM portfolio_positions
         WHERE portfolio_id = ? AND pnl_pct IS NOT NULL
         ORDER BY pnl_pct ASC LIMIT 1
    """, (portfolio_id,)).fetchone()

    # Live worst stretch: deepest dd point and the peak it fell from.
    eq_rows = con.execute("""
        SELECT trade_date, total_equity FROM portfolio_equity_history
         WHERE portfolio_id = ?
         ORDER BY trade_date ASC
    """, (portfolio_id,)).fetchall()

    worst_stretch = None
    if eq_rows:
        peak_eq, peak_date = float(eq_rows[0]["total_equity"]), eq_rows[0]["trade_date"]
        trough_eq, trough_date = peak_eq, peak_date
        cur_peak_eq, cur_peak_date = peak_eq, peak_date
        worst_dd_pct = 0.0
        for r in eq_rows:
            eq, d = float(r["total_equity"]), r["trade_date"]
            if eq > cur_peak_eq:
                cur_peak_eq, cur_peak_date = eq, d
                continue
            dd = (eq / cur_peak_eq - 1.0) * 100.0
            if dd < worst_dd_pct:
                worst_dd_pct = dd
                peak_eq, peak_date = cur_peak_eq, cur_peak_date
                trough_eq, trough_date = eq, d
        if worst_dd_pct < 0:
            # weeks between peak_date and trough_date
            try:
                days = (datetime.fromisoformat(trough_date).date()
                        - datetime.fromisoformat(peak_date).date()).days
            except ValueError:
                days = 0
            worst_stretch = {
                "dd_pct":      round(worst_dd_pct, 2),
                "from_date":   peak_date,
                "to_date":     trough_date,
                "weeks":       max(1, days // 7),
                "rs_lost":     trough_eq - peak_eq,
            }
    return {
        "best_win":      ({"symbol": best["symbol"],   "pnl_pct": float(best["pnl_pct"]),
                            "date":   best["exit_date"]} if best else None),
        "worst_loss":    ({"symbol": worst["symbol"],  "pnl_pct": float(worst["pnl_pct"]),
                            "date":   worst["exit_date"]} if worst else None),
        "worst_stretch": worst_stretch,
    }


def _avg_winner_loser_rs(
    con: sqlite3.Connection, portfolio_id: int,
) -> Dict[str, Optional[float]]:
    """Mean winning pnl_rs and mean losing pnl_rs from this portfolio's
    closed trades. Returns None when the bucket is empty."""
    rows = con.execute("""
        SELECT AVG(CASE WHEN pnl_rs > 0 THEN pnl_rs END) AS avg_win_rs,
               AVG(CASE WHEN pnl_rs < 0 THEN pnl_rs END) AS avg_loss_rs,
               AVG(CASE WHEN pnl_pct > 0 THEN pnl_pct END) AS avg_win_pct,
               AVG(CASE WHEN pnl_pct < 0 THEN pnl_pct END) AS avg_loss_pct
          FROM portfolio_positions
         WHERE portfolio_id = ? AND pnl_rs IS NOT NULL
    """, (portfolio_id,)).fetchone()
    if not rows:
        return {"avg_win_rs": None, "avg_loss_rs": None, "avg_win_pct": None, "avg_loss_pct": None}
    return {
        "avg_win_rs":   float(rows[0]) if rows[0]   is not None else None,
        "avg_loss_rs":  float(rows[1]) if rows[1]   is not None else None,
        "avg_win_pct":  float(rows[2]) if rows[2]   is not None else None,
        "avg_loss_pct": float(rows[3]) if rows[3]   is not None else None,
    }


def _portfolio_summary(con: sqlite3.Connection, p: sqlite3.Row) -> Dict[str, Any]:
    """Build the full summary used by /portfolios and /portfolios/{slug}.

    Shape mirrors Zerodha's holdings page + V3 lock-down (2026-05-16): persona
    narrative blocks (what_it_does, trading_cycle, best_fit etc.), risk_tier,
    disclosure, and both Reinvest / Cash Out return buckets so the UI toggle
    is a single field read.
    """
    latest          = _latest_equity(con, p["id"])
    metrics         = json.loads(p["backtest_metrics_json"]) if p["backtest_metrics_json"] else {}
    params          = json.loads(p["parameters_json"]) if p["parameters_json"] else {}
    # V3-locked schema added narrative_json. Older rows (pre-2026-05-16) have it null.
    try:
        narrative   = json.loads(p["narrative_json"]) if p["narrative_json"] else {}
    except (KeyError, IndexError, TypeError):
        narrative   = {}
    capital_start   = float(p["virtual_capital_start"])
    # Fixed-₹ sizing under V3. We still expose per_trade_pct-equivalent for the
    # frontend's "X% per trade" copy, derived from the audited fixed size.
    fixed_per_trade = float(params.get("fixed_per_trade_rs",
                              params.get("per_trade_pct", 6.0) * capital_start / 100.0))

    live: Optional[Dict[str, Any]] = None
    if latest:
        cur_equity         = float(latest["total_equity"])
        total_pnl_rs_re    = cur_equity - capital_start
        total_return_re    = (total_pnl_rs_re / capital_start) * 100.0 if capital_start else 0.0
        # Today's P&L = actual MTM change today. Identical in both views.
        today_pnl_rs       = float(latest["daily_pnl_rs"])
        today_pnl_pct      = float(latest["daily_pnl_pct"])

        ytd_mtd            = _ytd_mtd_for(con, p["id"], latest)
        cash_out           = _cash_out_metrics(con, p["id"], capital_start,
                                                 today_pnl_rs=today_pnl_rs,
                                                 today_pnl_pct=today_pnl_pct)
        bw                 = _best_worst_from_history(con, p["id"])
        win_loss_rs        = _avg_winner_loser_rs(con, p["id"])

        live = {
            "as_of_date":           latest["trade_date"],
            "total_investment_rs":  capital_start,
            "current_value_rs":     cur_equity,
            "invested_right_now_rs": float(latest["deployed_capital"]),
            "n_open_positions":     int(latest["n_open_positions"]),
            # Operator-renamed buckets (2026-05-16):
            #   reinvest  = compounding equity, profits stay in the book
            #   cash_out  = realised-only, profits go "to your wallet" each
            #              time a trade closes; today's P&L is the SAME MTM
            #              change in both modes.
            "reinvest": {
                "total_pnl_rs":      total_pnl_rs_re,
                "total_return_pct":  total_return_re,
                "today_pnl_rs":      today_pnl_rs,
                "today_pnl_pct":     today_pnl_pct,
                "ytd_return_pct":    ytd_mtd["ytd_pct"],
                "mtd_return_pct":    ytd_mtd["mtd_pct"],
                "worst_stretch_pct": float(latest["max_drawdown_pct"] or 0),
            },
            "cash_out": {
                "total_pnl_rs":      cash_out["total_pnl_rs"],
                "total_return_pct":  cash_out["total_return_pct"],
                "today_pnl_rs":      cash_out["today_pnl_rs"],
                "today_pnl_pct":     cash_out["today_pnl_pct"],
                "ytd_return_pct":    cash_out["ytd_return_pct"],
                "mtd_return_pct":    cash_out["mtd_return_pct"],
                "worst_stretch_pct": float(latest["max_drawdown_pct"] or 0),
            },
            # Live-derived best/worst, used by "ENGINE PERFORMANCE" panel
            "best_win":      bw["best_win"],
            "worst_loss":    bw["worst_loss"],
            "worst_stretch": bw["worst_stretch"],
            "avg_winner_rs":  win_loss_rs["avg_win_rs"],
            "avg_loser_rs":   win_loss_rs["avg_loss_rs"],
            "avg_winner_pct": win_loss_rs["avg_win_pct"],
            "avg_loser_pct":  win_loss_rs["avg_loss_pct"],
        }

    return {
        "slug":                  p["slug"],
        "name":                  p["name"],
        "tagline":               p["tagline"],
        "display_order":         p["display_order"],
        "virtual_capital_start": capital_start,
        "start_date":            p["start_date"],
        "parameters":            params,
        "validated":             metrics,
        "narrative":             narrative,
        "risk_tier":             metrics.get("risk_tier"),
        "risk_tier_color":       metrics.get("risk_tier_color"),
        "persona_number":        metrics.get("persona_number"),
        "backtest_window":       metrics.get("backtest_window"),
        "live":                  live,
    }


# ──────────────────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────────────────

@router.get("")
def list_portfolios(
    request: Request,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """List all 5 portfolios with their current headline metrics."""
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT * FROM portfolio_definitions
         WHERE is_active = 1
         ORDER BY display_order ASC, id ASC
    """).fetchall()
    summaries = [_portfolio_summary(con, r) for r in rows]
    log_request(con, user_id=None, route="/api/power/portfolios",
                status_code=200, latency_ms=0, ip_hash=hash_ip_ua(request))
    return {"portfolios": summaries, "n": len(summaries)}


@router.get("/sizing")
def sizing_assistant(
    request: Request,
    capital: float = Query(..., ge=0, description="User capital in ₹"),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Position sizing assistant — per-portfolio projection at user's capital.

    Public, no auth. Rate-limited at the global per-IP middleware level.
    """
    result = compute_sizing(capital)
    log_request(con, user_id=None, route="/api/power/portfolios/sizing",
                status_code=200, latency_ms=0, ip_hash=hash_ip_ua(request))
    return result


@router.get("/{slug}")
def portfolio_detail(
    slug: str,
    request: Request,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    p = _portfolio_row(con, slug)
    summary = _portfolio_summary(con, p)
    # Add last 10 events for the detail page
    con.row_factory = sqlite3.Row
    events = con.execute("""
        SELECT event_date, event_type, symbol, price, reason_text
          FROM portfolio_event_log
         WHERE portfolio_id = ?
         ORDER BY event_date DESC, id DESC
         LIMIT 10
    """, (p["id"],)).fetchall()
    summary["recent_events"] = [dict(r) for r in events]
    log_request(con, user_id=None, route="/api/power/portfolios/{slug}",
                status_code=200, latency_ms=0, ip_hash=hash_ip_ua(request))
    return summary


@router.get("/{slug}/positions")
def portfolio_positions(
    slug: str,
    request: Request,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """All currently OPEN positions for this portfolio."""
    p = _portfolio_row(con, slug)
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT id, signal_date, entry_date, symbol, sector, rank_on_entry,
               story_on_entry, entry_price, qty, capital_committed,
               sl_level, target_level, trail_active, trail_high_water,
               created_at
          FROM portfolio_positions
         WHERE portfolio_id = ? AND exit_date IS NULL
         ORDER BY entry_date ASC, id ASC
    """, (p["id"],)).fetchall()
    # Decorate with current MTM (today's close)
    out = []
    for r in rows:
        d = dict(r)
        cur_price = _last_close(con, r["symbol"])
        d["current_price"] = cur_price
        if cur_price is not None:
            d["unrealized_pnl_rs"]  = (cur_price - r["entry_price"]) * r["qty"]
            d["unrealized_pnl_pct"] = (cur_price - r["entry_price"]) / r["entry_price"] * 100.0
        else:
            d["unrealized_pnl_rs"]  = None
            d["unrealized_pnl_pct"] = None
        out.append(d)
    log_request(con, user_id=None, route="/api/power/portfolios/{slug}/positions",
                status_code=200, latency_ms=0, ip_hash=hash_ip_ua(request))
    return {"slug": slug, "n": len(out), "positions": out}


@router.get("/{slug}/trades")
def portfolio_trades(
    slug: str,
    request: Request,
    limit:  int = Query(50,  ge=1, le=500),
    offset: int = Query(0,   ge=0),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """All CLOSED trades for this portfolio (paginated, newest first)."""
    p = _portfolio_row(con, slug)
    con.row_factory = sqlite3.Row
    total = con.execute(
        "SELECT COUNT(*) FROM portfolio_positions WHERE portfolio_id = ? AND exit_date IS NOT NULL",
        (p["id"],),
    ).fetchone()[0]
    rows = con.execute("""
        SELECT signal_date, entry_date, exit_date, symbol, sector,
               entry_price, exit_price, qty, capital_committed,
               pnl_rs, pnl_pct, exit_reason, holding_days, story_on_entry
          FROM portfolio_positions
         WHERE portfolio_id = ? AND exit_date IS NOT NULL
         ORDER BY exit_date DESC, id DESC
         LIMIT ? OFFSET ?
    """, (p["id"], limit, offset)).fetchall()
    log_request(con, user_id=None, route="/api/power/portfolios/{slug}/trades",
                status_code=200, latency_ms=0, ip_hash=hash_ip_ua(request))
    return {
        "slug":   slug,
        "n":      len(rows),
        "total":  total,
        "trades": [dict(r) for r in rows],
        "limit":  limit, "offset": offset,
    }


@router.get("/{slug}/performance")
def portfolio_performance(
    slug: str,
    request: Request,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Year-by-year + month-by-month backtest performance breakdown.

    Source: operator's V3 audit Excel files imported via
    scripts/import_persona_excels.py into portfolio_yearly_performance +
    portfolio_monthly_performance.

    Returns:
        {
          "slug":    str,
          "yearly":  [{ year, return_pct, max_drawdown_pct, winning_months,
                          n_closed_trades, win_rate_pct, is_partial }, …],
          "monthly": { "YYYY": [{ month, return_pct, end_equity_rs }, …] }
        }
    """
    p = _portfolio_row(con, slug)
    con.row_factory = sqlite3.Row
    yearly_rows = con.execute("""
        SELECT year, return_pct, max_drawdown_pct, win_rate_pct,
               n_closed_trades, winning_months, is_partial, end_equity_rs, start_cap_rs
          FROM portfolio_yearly_performance
         WHERE portfolio_id = ?
         ORDER BY year ASC
    """, (p["id"],)).fetchall()
    monthly_rows = con.execute("""
        SELECT year, month, return_pct, end_equity_rs, is_partial
          FROM portfolio_monthly_performance
         WHERE portfolio_id = ?
         ORDER BY year ASC, month ASC
    """, (p["id"],)).fetchall()

    monthly_by_year: Dict[str, List[Dict[str, Any]]] = {}
    for r in monthly_rows:
        monthly_by_year.setdefault(str(r["year"]), []).append(dict(r))

    log_request(con, user_id=None, route="/api/power/portfolios/{slug}/performance",
                status_code=200, latency_ms=0, ip_hash=hash_ip_ua(request))
    return {
        "slug":    slug,
        "yearly":  [dict(r) for r in yearly_rows],
        "monthly": monthly_by_year,
    }


@router.get("/{slug}/equity")
def portfolio_equity(
    slug: str,
    request: Request,
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date:   Optional[str] = Query(None, description="YYYY-MM-DD"),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Equity curve points for charting. Default: all history.

    Each point carries BOTH reinvest (`total_equity`) and cash_out
    (`cash_out_total_equity`) views.

    Reinvest curve = the actual mark-to-market equity of the (compounded)
    book — exactly what's stored in portfolio_equity_history.

    Cash Out curve = capital_start + sum(realised pnl_rs of trades closed
    BY date d). Step function — rises only when a trade exits. This is the
    operator's "what your account would look like if you withdrew profits
    as they appeared" semantic.
    """
    p = _portfolio_row(con, slug)
    capital_start = float(p["virtual_capital_start"])

    con.row_factory = sqlite3.Row
    where  = ["portfolio_id = ?"]
    args:  List[Any] = [p["id"]]
    if from_date:
        where.append("trade_date >= ?"); args.append(from_date)
    if to_date:
        where.append("trade_date <= ?"); args.append(to_date)
    sql = f"""
        SELECT trade_date, total_equity, cumulative_return_pct,
               daily_pnl_pct, max_drawdown_pct, n_open_positions
          FROM portfolio_equity_history
         WHERE {' AND '.join(where)}
         ORDER BY trade_date ASC
    """
    rows = con.execute(sql, args).fetchall()

    cash_out_by_date = _cash_out_equity_curve(con, p["id"], capital_start)

    points: List[Dict[str, Any]] = []
    running_cash_out = capital_start
    for r in rows:
        d = dict(r)
        # Cash Out step-function: jumps on exit dates, flat otherwise.
        if d["trade_date"] in cash_out_by_date:
            running_cash_out = cash_out_by_date[d["trade_date"]]
        d["cash_out_total_equity"]          = running_cash_out
        d["cash_out_cumulative_return_pct"] = (running_cash_out / capital_start - 1.0) * 100.0
        points.append(d)

    log_request(con, user_id=None, route="/api/power/portfolios/{slug}/equity",
                status_code=200, latency_ms=0, ip_hash=hash_ip_ua(request))
    return {
        "slug":          slug,
        "from_date":     from_date,
        "to_date":       to_date,
        "n":             len(points),
        "capital_start": capital_start,
        "points":        points,
    }


def _cash_out_equity_curve(
    con: sqlite3.Connection,
    portfolio_id: int,
    capital_start: float,
) -> Dict[str, float]:
    """date → cash_out_total_equity map.

    Walks closed trades chronologically. On each exit_date, add the
    REALISED pnl_rs of every trade closed that day to the running total.
    Open positions don't contribute (they haven't been "cashed out" yet).
    Result is the operator's "withdraw-on-exit" mental model.
    """
    rows = con.execute("""
        SELECT exit_date, SUM(pnl_rs) FROM portfolio_positions
         WHERE portfolio_id = ? AND exit_date IS NOT NULL AND pnl_rs IS NOT NULL
         GROUP BY exit_date
         ORDER BY exit_date ASC
    """, (portfolio_id,)).fetchall()
    cum_realized_rs = 0.0
    out: Dict[str, float] = {}
    for date, sum_rs in rows:
        cum_realized_rs += float(sum_rs or 0)
        out[date] = capital_start + cum_realized_rs
    return out


def _last_close(con: sqlite3.Connection, symbol: str) -> Optional[float]:
    row = con.execute("""
        SELECT close FROM ohlc_daily
         WHERE symbol = ?
         ORDER BY trade_date DESC
         LIMIT 1
    """, (symbol,)).fetchone()
    return float(row[0]) if row else None
