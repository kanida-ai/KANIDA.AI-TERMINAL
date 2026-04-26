"""
KANIDA.AI — Insights Router
===========================
Autonomous headline generation for the dashboard homepage.

Endpoint:
  GET /signals/insights?market=NSE|US|BOTH

Returns a list of "headline buckets" that Kanida.AI thinks matter right now:

  • rare_edges_firing  — high_conviction setups firing in the last 3 days
  • core_firing        — core_active setups firing in the last 3 days
  • bearish_firing     — proven bearish setups firing in the last 3 days
  • fallen_from_peak   — stocks ≥ 15% below their 90-day high, with signs of stabilising
  • breakout_ready     — price coiled within 3% of 252-day high, volume dry
  • volume_dry_up      — recent volume ≤ 70% of 20-day average
  • base_forming       — low-volatility consolidation in upper half of 20-day range

Each bucket is pre-computed from the existing DB — no LLM in the hot path.
The UI renders these as clickable headline cards; each ticker inside carries
a `drill_prompt` that seeds the stock-detail smart-prompt layer.

All tier internals (core_active, high_conviction, fitness_score, wilson_lower,
etc.) are translated to trader-friendly language before leaving the server.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

_HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.normpath(
    os.path.join(_HERE, "..", "..", "data", "db", "kanida_signals.db")
)

FIRING_WINDOW_DAYS = 3
MAX_TICKERS_PER_BUCKET = 8


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


# ─────────────────────────────────────────────────────────────────────
# Trader-friendly label helpers
# ─────────────────────────────────────────────────────────────────────
TIER_LABEL = {
    "core_active":     "Core Signal",
    "high_conviction": "Rare Edge",
    "steady":          "Proven",
    "emerging":        "Building",
    "experimental":    "Observing",
    "retired":         "Stopped Working",
}


def _fmt_win(wr: Optional[float]) -> str:
    if wr is None:
        return ""
    return f"{int(round(wr * 100))}% wins"


def _fmt_pct(v: Optional[float], signed: bool = False, digits: int = 1) -> str:
    if v is None:
        return ""
    if signed:
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:.{digits}f}%"
    return f"{v:.{digits}f}%"


# ─────────────────────────────────────────────────────────────────────
# Per-bucket queries
# ─────────────────────────────────────────────────────────────────────

def _firing_bucket(
    conn: sqlite3.Connection,
    market: str,
    bias: str,
    tiers: list[str],
    limit: int,
) -> list[dict]:
    """
    Top ranked (tier, win, fitness) setups that fired within FIRING_WINDOW_DAYS.
    Returns one row per ticker (best setup for that ticker).
    """
    tier_placeholders = ",".join("?" * len(tiers))
    sql = f"""
    WITH latest AS (
      SELECT MAX(signal_date) d FROM signal_events WHERE market=? AND bias=?
    ),
    firing AS (
      SELECT DISTINCT ticker, timeframe, strategy_name, bias
      FROM signal_events
      WHERE market=? AND bias=?
        AND signal_date >= date((SELECT d FROM latest), ?)
        AND signal_date <= (SELECT d FROM latest)
    )
    SELECT
      r.ticker,
      r.timeframe,
      r.strategy_name,
      r.tier,
      r.fitness_score,
      f.win_rate_15d,
      f.avg_ret_15d,
      f.total_appearances,
      sm.company_name
    FROM signal_roster r
    JOIN firing fr ON fr.ticker=r.ticker AND fr.timeframe=r.timeframe
                  AND fr.strategy_name=r.strategy_name AND fr.bias=r.bias
    JOIN stock_signal_fitness f
      ON f.ticker=r.ticker AND f.market=r.market
     AND f.timeframe=r.timeframe AND f.strategy_name=r.strategy_name AND f.bias=r.bias
    LEFT JOIN sector_mapping sm ON sm.ticker=r.ticker AND sm.market=r.market
    WHERE r.market=? AND r.bias=?
      AND r.tier IN ({tier_placeholders})
    ORDER BY r.fitness_score DESC, f.win_rate_15d DESC
    """
    window = f"-{FIRING_WINDOW_DAYS} days"
    params = [market, bias, market, bias, window, market, bias, *tiers]
    rows = conn.execute(sql, params).fetchall()

    # Collapse to one best-setup per ticker
    seen: dict[str, dict] = {}
    for r in rows:
        t = r["ticker"]
        if t in seen:
            continue
        seen[t] = {
            "ticker":       t,
            "company_name": r["company_name"],
            "timeframe":    r["timeframe"],
            "tier":         r["tier"],
            "tier_label":   TIER_LABEL.get(r["tier"], r["tier"]),
            "one_line_reason": (
                f"{TIER_LABEL.get(r['tier'], r['tier'])} · "
                f"{_fmt_win(r['win_rate_15d'])} over {r['total_appearances']} hits · "
                f"avg {_fmt_pct(r['avg_ret_15d'], signed=True)}"
            ),
            "drill_prompt_id": "why_in_highlights",
        }
        if len(seen) >= limit:
            break
    return list(seen.values())


def _ohlc_bucket_candidates(conn: sqlite3.Connection, market: str) -> list[sqlite3.Row]:
    """
    For each ticker, compute latest close + 90d high + 252d high + 20d avg volume
    + recent volume + 20d stdev/mean of close. One pass, grouped, cheap on 300 tickers.
    Restricted to tickers with a non-retired roster (i.e. Kanida cares about them).
    """
    sql = """
    WITH latest AS (
      SELECT MAX(trade_date) d FROM ohlc_daily WHERE market=?
    ),
    tracked AS (
      SELECT DISTINCT ticker FROM signal_roster
      WHERE market=? AND tier IS NOT NULL AND tier!='retired'
    ),
    windowed AS (
      SELECT o.ticker, o.trade_date, o.close, o.high, o.volume,
             (SELECT d FROM latest) AS latest_d
      FROM ohlc_daily o
      JOIN tracked t ON t.ticker = o.ticker
      WHERE o.market=?
        AND o.trade_date >= date((SELECT d FROM latest), '-260 days')
    ),
    agg AS (
      SELECT
        ticker,
        MAX(CASE WHEN trade_date = latest_d THEN close    END)    AS close_latest,
        MAX(CASE WHEN trade_date = latest_d THEN volume   END)    AS vol_latest,
        MAX(CASE WHEN trade_date >= date(latest_d, '-90 days')  THEN high     END) AS high_90d,
        MAX(CASE WHEN trade_date >= date(latest_d, '-252 days') THEN high     END) AS high_252d,
        AVG(CASE WHEN trade_date >= date(latest_d, '-20 days')  THEN volume   END) AS vol_20d_avg,
        AVG(CASE WHEN trade_date >= date(latest_d, '-20 days')  THEN close    END) AS close_20d_avg,
        MIN(CASE WHEN trade_date >= date(latest_d, '-20 days')  THEN close    END) AS low_20d,
        MAX(CASE WHEN trade_date >= date(latest_d, '-20 days')  THEN close    END) AS high_20d
      FROM windowed
      GROUP BY ticker
    )
    SELECT a.*, sm.company_name, sm.sector
    FROM agg a
    LEFT JOIN sector_mapping sm ON sm.ticker=a.ticker AND sm.market=?
    WHERE a.close_latest IS NOT NULL
    """
    return conn.execute(sql, (market, market, market, market)).fetchall()


def _fallen_from_peak(cand: list[sqlite3.Row], limit: int) -> list[dict]:
    """Stocks 15%+ below their 90-day high, holding above 20d mean (stabilising)."""
    out = []
    for r in cand:
        if not (r["close_latest"] and r["high_90d"]):
            continue
        off = (r["high_90d"] - r["close_latest"]) / r["high_90d"]
        if off >= 0.15 and r["close_20d_avg"] and r["close_latest"] >= r["close_20d_avg"]:
            out.append({
                "ticker":       r["ticker"],
                "company_name": r["company_name"],
                "one_line_reason":
                    f"{int(round(off*100))}% off 90-day peak · holding above 20-day mean",
                "drill_prompt_id": "is_base_forming",
                "_rank_key":    off,
            })
    out.sort(key=lambda x: -x["_rank_key"])
    for x in out:
        x.pop("_rank_key")
    return out[:limit]


def _breakout_ready(cand: list[sqlite3.Row], limit: int) -> list[dict]:
    """Price within 3% of 252-day high AND volume dry (≤ 80% of 20-day avg)."""
    out = []
    for r in cand:
        if not (r["close_latest"] and r["high_252d"] and r["vol_latest"] and r["vol_20d_avg"]):
            continue
        near_hi = r["close_latest"] >= 0.97 * r["high_252d"]
        vol_dry = r["vol_latest"] <= 0.80 * r["vol_20d_avg"]
        if near_hi and vol_dry:
            gap = (r["high_252d"] - r["close_latest"]) / r["high_252d"]
            out.append({
                "ticker":       r["ticker"],
                "company_name": r["company_name"],
                "one_line_reason":
                    f"Within {gap*100:.1f}% of 52-week high · volume quiet",
                "drill_prompt_id": "near_breakout",
                "_rank_key":    -gap,  # closer to hi is better
            })
    out.sort(key=lambda x: -x["_rank_key"])
    for x in out:
        x.pop("_rank_key")
    return out[:limit]


def _volume_dry_up(cand: list[sqlite3.Row], limit: int) -> list[dict]:
    """Latest volume ≤ 70% of 20-day avg. Consolidation candidates."""
    out = []
    for r in cand:
        if not (r["vol_latest"] and r["vol_20d_avg"] and r["vol_20d_avg"] > 0):
            continue
        ratio = r["vol_latest"] / r["vol_20d_avg"]
        if ratio <= 0.70:
            out.append({
                "ticker":       r["ticker"],
                "company_name": r["company_name"],
                "one_line_reason":
                    f"Volume at {int(round(ratio*100))}% of 20-day average",
                "drill_prompt_id": "volume_profile",
                "_rank_key":    -ratio,
            })
    out.sort(key=lambda x: -x["_rank_key"])
    for x in out:
        x.pop("_rank_key")
    return out[:limit]


def _base_forming(cand: list[sqlite3.Row], limit: int) -> list[dict]:
    """
    Low-range consolidation in upper half of 20-day range:
      (high_20d - low_20d) / close_20d_avg ≤ 6%  AND  position in range >= 0.5
    """
    out = []
    for r in cand:
        if not (r["close_20d_avg"] and r["high_20d"] and r["low_20d"]):
            continue
        rng = r["high_20d"] - r["low_20d"]
        if rng <= 0:
            continue
        rng_pct = rng / r["close_20d_avg"]
        pos = (r["close_latest"] - r["low_20d"]) / rng
        if rng_pct <= 0.06 and pos >= 0.5:
            out.append({
                "ticker":       r["ticker"],
                "company_name": r["company_name"],
                "one_line_reason":
                    f"Tight 20-day range ({rng_pct*100:.1f}% of price) · upper half",
                "drill_prompt_id": "is_base_forming",
                "_rank_key":    -rng_pct,
            })
    out.sort(key=lambda x: -x["_rank_key"])
    for x in out:
        x.pop("_rank_key")
    return out[:limit]


# ─────────────────────────────────────────────────────────────────────
# Endpoint
# ─────────────────────────────────────────────────────────────────────

def _compute_market_insights(conn: sqlite3.Connection, market: str) -> dict:
    # Per-bias latest signal dates (informational headline)
    latest_bull = conn.execute(
        "SELECT MAX(signal_date) d FROM signal_events WHERE market=? AND bias='bullish'",
        (market,)).fetchone()["d"]
    latest_bear = conn.execute(
        "SELECT MAX(signal_date) d FROM signal_events WHERE market=? AND bias='bearish'",
        (market,)).fetchone()["d"]

    # Firing buckets
    rare_bull = _firing_bucket(conn, market, "bullish", ["high_conviction"], MAX_TICKERS_PER_BUCKET)
    core_bull = _firing_bucket(conn, market, "bullish", ["core_active"], MAX_TICKERS_PER_BUCKET)
    firing_bear = _firing_bucket(conn, market, "bearish",
                                 ["core_active", "high_conviction", "steady"],
                                 MAX_TICKERS_PER_BUCKET)

    # Price/volume regime buckets
    ohlc_cand = _ohlc_bucket_candidates(conn, market)
    fallen   = _fallen_from_peak(ohlc_cand, MAX_TICKERS_PER_BUCKET)
    breakout = _breakout_ready(ohlc_cand,  MAX_TICKERS_PER_BUCKET)
    vol_dry  = _volume_dry_up(ohlc_cand,   MAX_TICKERS_PER_BUCKET)
    base     = _base_forming(ohlc_cand,    MAX_TICKERS_PER_BUCKET)

    insights = []
    if rare_bull:
        insights.append({
            "id":        "rare_edges_firing",
            "title":     "Rare Edges Firing",
            "one_liner": f"{len(rare_bull)} high-confidence bullish setups just fired — "
                         f"historically the sharpest edges in this market.",
            "accent":    "violet",
            "count":     len(rare_bull),
            "tickers":   rare_bull,
        })
    if core_bull:
        insights.append({
            "id":        "core_firing",
            "title":     "Core Signals Firing",
            "one_liner": f"{len(core_bull)} proven bullish setups fired in the last 3 days.",
            "accent":    "emerald",
            "count":     len(core_bull),
            "tickers":   core_bull,
        })
    if firing_bear:
        insights.append({
            "id":        "bearish_firing",
            "title":     "Bearish Setups Firing",
            "one_liner": f"{len(firing_bear)} short-side setups fired — "
                         f"known to work on these stocks historically.",
            "accent":    "rose",
            "count":     len(firing_bear),
            "tickers":   firing_bear,
        })
    if breakout:
        insights.append({
            "id":        "breakout_ready",
            "title":     "Breakout Ready",
            "one_liner": f"{len(breakout)} stocks coiled near 52-week highs on quiet volume.",
            "accent":    "amber",
            "count":     len(breakout),
            "tickers":   breakout,
        })
    if base:
        insights.append({
            "id":        "base_forming",
            "title":     "Base Forming",
            "one_liner": f"{len(base)} stocks consolidating tightly in the upper half of their range.",
            "accent":    "sky",
            "count":     len(base),
            "tickers":   base,
        })
    if fallen:
        insights.append({
            "id":        "fallen_from_peak",
            "title":     "Fallen From Peak",
            "one_liner": f"{len(fallen)} stocks are well off 90-day peaks but have stopped bleeding.",
            "accent":    "zinc",
            "count":     len(fallen),
            "tickers":   fallen,
        })
    if vol_dry:
        insights.append({
            "id":        "volume_dry_up",
            "title":     "Volume Dry-Up",
            "one_liner": f"{len(vol_dry)} stocks trading on unusually quiet volume — pre-move signature.",
            "accent":    "sky",
            "count":     len(vol_dry),
            "tickers":   vol_dry,
        })

    total_firing = len(rare_bull) + len(core_bull) + len(firing_bear)
    if total_firing >= 1:
        headline = (f"{total_firing} setups fired. "
                    f"{len(rare_bull)} rare edge{'s' if len(rare_bull) != 1 else ''} in the mix.")
    else:
        headline = "Scanning — no tier-1 setups firing right now."

    return {
        "market":                     market,
        "headline":                   headline,
        "latest_bullish_signal_date": latest_bull,
        "latest_bearish_signal_date": latest_bear,
        "insights":                   insights,
    }


@router.get("/insights")
def insights(market: str = Query("NSE", description="NSE | US | BOTH")):
    if market not in ("NSE", "US", "BOTH"):
        raise HTTPException(400, "market must be NSE, US, or BOTH")
    try:
        conn = _conn()
        if market == "BOTH":
            out = {"markets": {m: _compute_market_insights(conn, m) for m in ("NSE", "US")}}
        else:
            out = _compute_market_insights(conn, market)
        conn.close()
        return out
    except Exception as e:
        raise HTTPException(500, str(e))
