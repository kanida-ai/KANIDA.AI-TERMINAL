"""
KANIDA.AI Signals Dashboard — API Router
========================================
Reads the new signals DB (kanida_signals.db) built by the tier-redesign pipeline.

Endpoints (mounted at /signals/*):
  GET /signals/health                         market snapshot + firing counts
  GET /signals/top                            ranked Top-N bullish/bearish signals
  GET /signals/stock/{ticker}                 stock intelligence detail
  GET /signals/stock/{ticker}/history         last-N paper-trade outcomes

The ranking formula is:

    live_score =   tier_weight
                 + 0.30 * fitness_score
                 + 20.0 * win_rate_15d
                 +  1.5 * min(avg_ret_15d, 10)
                 + 10.0 * wilson_lower_15d
                 + (10 if firing_today else 0)

Tier dominates (60-point swing) so a Core Signal always ranks above lower
tiers within its cohort; fitness / win / ret / wilson break ties inside a
tier; firing_bonus lifts actively-firing strategies past borderline peers.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

# ── DB path: always the signals DB (not the legacy fingerprints DB) ──────────
_HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.normpath(
    os.path.join(_HERE, "..", "..", "data", "db", "kanida_signals.db")
)


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


# ── Tier taxonomy ────────────────────────────────────────────────────────────
TIER_WEIGHT = {
    "core_active":     60,
    "high_conviction": 55,
    "steady":          40,
    "emerging":        25,
    "experimental":    10,
    "retired":         -9999,
}

TIER_LABEL = {
    "core_active":     "Core Signal",
    "high_conviction": "Rare Edge",
    "steady":          "Proven",
    "emerging":        "Building Track Record",
    "experimental":    "Observing",
    "retired":         "Stopped Working",
}

TIER_RANK = {k: i for i, k in enumerate(
    ["core_active", "high_conviction", "steady", "emerging", "experimental", "retired"]
)}


def _live_score(row: dict) -> float:
    tier = row.get("tier") or ""
    fitness = row.get("fitness_score") or 0.0
    win15 = row.get("win_rate_15d") or 0.0
    ret15 = row.get("avg_ret_15d") or 0.0
    wilson = row.get("wilson_lower_15d") or 0.0
    firing = 1 if row.get("firing_today") else 0
    return (
        TIER_WEIGHT.get(tier, 0)
        + 0.30 * float(fitness)
        + 20.0 * float(win15)
        + 1.5 * min(float(ret15), 10.0)
        + 10.0 * float(wilson)
        + (10.0 if firing else 0.0)
    )


def _enrich(row: sqlite3.Row) -> dict:
    d = dict(row)
    tier = d.get("tier") or ""
    d["tier_label"] = TIER_LABEL.get(tier, tier)
    d["size_multiplier"] = d.get("size_multiplier") or 0.0
    d["live_score"] = round(_live_score(d), 2)
    return d


# ── Common SQL: roster × fitness × firing (per-bias latest signal_date) ─────
# Bullish and bearish scans do not always land on the same calendar day —
# bearish setups often fire later or on different days than bullish breakouts.
# Anchoring `firing_today` to the global MAX(signal_date) silently hides
# bearish firings whenever bullish was the most recent scan. We now anchor to
# the latest signal_date for THIS (market, bias) and include a 3-day window so
# a bearish core signal from two days ago still surfaces as "actively firing".
FIRING_WINDOW_DAYS = 3
_ROW_SQL = """
WITH latest AS (
  SELECT MAX(signal_date) AS d
  FROM signal_events
  WHERE market = :mkt AND bias = :bias
),
firing AS (
  SELECT DISTINCT ticker, market, timeframe, strategy_name, bias
  FROM signal_events
  WHERE market = :mkt
    AND bias = :bias
    AND signal_date >= date((SELECT d FROM latest), :window_days)
    AND signal_date <= (SELECT d FROM latest)
)
SELECT
  r.ticker, r.market, r.timeframe, r.strategy_name, r.bias,
  r.tier, r.quality_grade, r.frequency_grade,
  r.fitness_score, r.size_multiplier, r.trend_gate,
  f.total_appearances, f.recent_appearances,
  f.win_rate_15d, f.avg_ret_15d, f.wilson_lower_15d,
  f.avg_mfe_pct, f.avg_mae_pct, f.last_signal_date,
  CASE WHEN fr.ticker IS NOT NULL THEN 1 ELSE 0 END AS firing_today,
  sm.company_name, sm.sector,
  (SELECT d FROM latest) AS latest_signal_date
FROM signal_roster r
JOIN stock_signal_fitness f
  ON f.ticker = r.ticker AND f.market = r.market
 AND f.timeframe = r.timeframe
 AND f.strategy_name = r.strategy_name AND f.bias = r.bias
LEFT JOIN firing fr
  ON fr.ticker = r.ticker AND fr.market = r.market
 AND fr.timeframe = r.timeframe
 AND fr.strategy_name = r.strategy_name AND fr.bias = r.bias
LEFT JOIN sector_mapping sm
  ON sm.ticker = r.ticker AND sm.market = r.market
WHERE r.market = :mkt
  AND r.bias = :bias
  AND r.tier IS NOT NULL
  AND r.tier != 'retired'
"""


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/health")
def health():
    """Market snapshot for dashboard header."""
    try:
        conn = _conn()
        out = {"markets": {}, "db_path": DB_PATH}
        for mkt in ("NSE", "US"):
            # Per-bias latest scan date (bearish scans may lag bullish scans)
            latest_bull = conn.execute(
                "SELECT MAX(signal_date) d FROM signal_events WHERE market=? AND bias='bullish'",
                (mkt,),
            ).fetchone()["d"]
            latest_bear = conn.execute(
                "SELECT MAX(signal_date) d FROM signal_events WHERE market=? AND bias='bearish'",
                (mkt,),
            ).fetchone()["d"]
            latest = max(d for d in (latest_bull, latest_bear) if d) if (latest_bull or latest_bear) else None
            bull = bear = 0
            window = f"-{FIRING_WINDOW_DAYS} days"
            if latest_bull:
                row = conn.execute("""
                    SELECT COUNT(DISTINCT ticker || '|' || timeframe || '|' || strategy_name) n
                    FROM signal_events
                    WHERE market=? AND bias='bullish'
                      AND signal_date >= date(?, ?)
                      AND signal_date <= ?
                """, (mkt, latest_bull, window, latest_bull)).fetchone()
                bull = int(row["n"] or 0)
            if latest_bear:
                row = conn.execute("""
                    SELECT COUNT(DISTINCT ticker || '|' || timeframe || '|' || strategy_name) n
                    FROM signal_events
                    WHERE market=? AND bias='bearish'
                      AND signal_date >= date(?, ?)
                      AND signal_date <= ?
                """, (mkt, latest_bear, window, latest_bear)).fetchone()
                bear = int(row["n"] or 0)
            roster_row = conn.execute("""
                SELECT COUNT(*) n,
                       SUM(CASE WHEN tier='core_active' THEN 1 ELSE 0 END) core,
                       SUM(CASE WHEN tier='high_conviction' THEN 1 ELSE 0 END) hc
                FROM signal_roster WHERE market=? AND tier IS NOT NULL AND tier!='retired'
            """, (mkt,)).fetchone()
            out["markets"][mkt] = {
                "latest_signal_date":         latest,
                "latest_bullish_signal_date": latest_bull,
                "latest_bearish_signal_date": latest_bear,
                "firing_bullish":             bull,
                "firing_bearish":             bear,
                "active_roster":      int(roster_row["n"] or 0),
                "core_signals":       int(roster_row["core"] or 0),
                "rare_edges":         int(roster_row["hc"] or 0),
            }
        conn.close()
        from datetime import datetime
        out["server_time"] = datetime.now().isoformat(timespec="seconds")
        return out
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/top")
def top(
    market:       str  = Query("NSE", description="NSE | US | BOTH"),
    bias:         str  = Query("bullish", description="bullish | bearish"),
    limit:        int  = 20,
    timeframe:    Optional[str]  = Query(None, description="1D | 1W"),
    tier:         Optional[str]  = Query(None, description="core_active,high_conviction,..."),
    firing_only:  bool = False,
    q:            Optional[str]  = Query(None, description="ticker search substring"),
):
    if bias not in ("bullish", "bearish"):
        raise HTTPException(400, "bias must be bullish or bearish")
    if market not in ("NSE", "US", "BOTH"):
        raise HTTPException(400, "market must be NSE, US, or BOTH")

    markets = ["NSE", "US"] if market == "BOTH" else [market]
    try:
        conn = _conn()
        all_rows: list[dict] = []
        window_days = f"-{FIRING_WINDOW_DAYS} days"
        for mkt in markets:
            sql = _ROW_SQL
            params = {"mkt": mkt, "bias": bias, "window_days": window_days}
            for raw in conn.execute(sql, params).fetchall():
                d = _enrich(raw)
                if timeframe and d["timeframe"] != timeframe:
                    continue
                if tier:
                    wanted = {t.strip() for t in tier.split(",") if t.strip()}
                    if d["tier"] not in wanted:
                        continue
                if firing_only and not d["firing_today"]:
                    continue
                if q:
                    ql = q.lower()
                    hay = (d["ticker"] or "").lower() + " " + (d.get("company_name") or "").lower()
                    if ql not in hay:
                        continue
                all_rows.append(d)
        conn.close()

        all_rows.sort(key=lambda r: (-r["live_score"], -(r["fitness_score"] or 0)))
        ranked = all_rows[:limit]
        for i, r in enumerate(ranked, 1):
            r["rank"] = i
        return {
            "market": market, "bias": bias,
            "count": len(ranked),
            "total_candidates": len(all_rows),
            "rows": ranked,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/stock/{ticker}")
def stock_detail(
    ticker: str,
    market: str = Query("NSE"),
    ladder_limit: int = 12,
):
    if market not in ("NSE", "US"):
        raise HTTPException(400, "market must be NSE or US")
    ticker = ticker.upper()

    try:
        conn = _conn()

        meta = conn.execute("""
            SELECT ticker, market, company_name, sector
            FROM sector_mapping WHERE ticker=? AND market=?
        """, (ticker, market)).fetchone()
        if not meta:
            raise HTTPException(404, f"{ticker} not found in {market}")

        # Predictability: mean fitness of non-retired roster rows, weighted toward core/HC.
        pred = conn.execute("""
            SELECT
              AVG(fitness_score) avg_fit,
              COUNT(*) n_active,
              SUM(CASE WHEN tier IN ('core_active','high_conviction') THEN 1 ELSE 0 END) n_top
            FROM signal_roster
            WHERE ticker=? AND market=? AND tier IS NOT NULL AND tier!='retired'
        """, (ticker, market)).fetchone()
        avg_fit = float(pred["avg_fit"] or 0)
        n_active = int(pred["n_active"] or 0)
        n_top = int(pred["n_top"] or 0)
        # 0-100: fitness at 60→60 pts, top-tier share adds up to 40
        top_share = (n_top / n_active) if n_active else 0
        predictability = round(min(100.0, avg_fit + 40.0 * top_share), 1)

        # Firing today (across all biases/tfs)
        latest_row = conn.execute(
            "SELECT MAX(signal_date) d FROM signal_events WHERE market=? AND ticker=?",
            (market, ticker)).fetchone()
        latest_date = latest_row["d"]
        firing_today: list[dict] = []
        if latest_date:
            for r in conn.execute("""
                SELECT timeframe, strategy_name, bias, entry_price, trend_state
                FROM signal_events
                WHERE ticker=? AND market=? AND signal_date=?
                ORDER BY bias, timeframe, strategy_name
            """, (ticker, market, latest_date)):
                firing_today.append(dict(r))

        # Signal ladder: top rows by live_score for this stock, both biases
        ladder: list[dict] = []
        window_days = f"-{FIRING_WINDOW_DAYS} days"
        for bias in ("bullish", "bearish"):
            params = {"mkt": market, "bias": bias, "window_days": window_days}
            for raw in conn.execute(_ROW_SQL, params).fetchall():
                if raw["ticker"] != ticker:
                    continue
                ladder.append(_enrich(raw))
        ladder.sort(key=lambda r: -r["live_score"])
        ladder = ladder[:ladder_limit]

        # Paper trade idea: most recent OPEN paper_trade for this ticker,
        # or (if none open) the best currently-firing strategy's entry price.
        open_row = conn.execute("""
            SELECT p.ticker, p.market, p.timeframe, p.strategy_name, p.bias,
                   p.entry_date, p.entry_price, p.stop_price, p.target_1, p.target_2,
                   p.roster_tier, p.size_multiplier, p.status
            FROM paper_trades p
            WHERE p.ticker=? AND p.market=? AND p.status='open'
            ORDER BY p.entry_date DESC
            LIMIT 1
        """, (ticker, market)).fetchone()
        paper_idea = dict(open_row) if open_row else None
        if paper_idea:
            paper_idea["tier_label"] = TIER_LABEL.get(paper_idea.get("roster_tier") or "", "")

        # Last-10 paper-trade outcomes
        history: list[dict] = []
        for r in conn.execute("""
            SELECT timeframe, strategy_name, bias, entry_date, entry_price,
                   stop_price, target_1, exit_date, exit_price, outcome_pct, win,
                   status, days_held, roster_tier, size_multiplier
            FROM paper_trades
            WHERE ticker=? AND market=? AND status!='open'
            ORDER BY COALESCE(exit_date, entry_date) DESC
            LIMIT 10
        """, (ticker, market)):
            d = dict(r)
            d["tier_label"] = TIER_LABEL.get(d.get("roster_tier") or "", "")
            history.append(d)

        conn.close()

        return {
            "ticker":         meta["ticker"],
            "market":         meta["market"],
            "company_name":   meta["company_name"],
            "sector":         meta["sector"],
            "predictability": predictability,
            "roster_active":  n_active,
            "roster_top":     n_top,
            "latest_signal_date": latest_date,
            "firing_today":   firing_today,
            "ladder":         ladder,
            "paper_idea":     paper_idea,
            "history":        history,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/stock/{ticker}/history")
def stock_history(ticker: str, market: str = "NSE", limit: int = 10):
    try:
        conn = _conn()
        rows = conn.execute("""
            SELECT timeframe, strategy_name, bias, entry_date, entry_price,
                   stop_price, target_1, exit_date, exit_price, outcome_pct, win,
                   status, days_held, roster_tier
            FROM paper_trades
            WHERE ticker=? AND market=? AND status!='open'
            ORDER BY COALESCE(exit_date, entry_date) DESC
            LIMIT ?
        """, (ticker.upper(), market.upper(), limit)).fetchall()
        conn.close()
        return {"ticker": ticker.upper(), "market": market.upper(),
                "rows": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(500, str(e))
