"""
KANIDA.AI — Smart Prompts Router
================================
Conversational exploration layer for a single stock.

Endpoint:
  GET /signals/prompts/{ticker}?market=NSE
  GET /signals/prompts/{ticker}/answer?market=NSE&prompt_id=...

The first call returns the full menu of ~18 pre-computed prompts for a stock,
each with a trader-friendly question. The second call returns the data-backed
answer for one specific prompt.

All answers are pre-computed SQL + natural-language templates. No LLM in the
hot path — fast, deterministic, and the proprietary scoring internals stay
server-side. The user sees plain English with cited numbers.

Every answer also returns `next_prompts` — the conversational follow-up hints
the frontend can surface beneath the answer, so the user can drill in without
knowing what to type.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Callable, Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

_HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.normpath(
    os.path.join(_HERE, "..", "..", "data", "db", "kanida_signals.db")
)

FIRING_WINDOW_DAYS = 3


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


TIER_LABEL = {
    "core_active":     "Core Signal",
    "high_conviction": "Rare Edge",
    "steady":          "Proven",
    "emerging":        "Building",
    "experimental":    "Observing",
    "retired":         "Stopped Working",
}


# ─────────────────────────────────────────────────────────────────────
# Prompt registry — the menu the UI shows
# ─────────────────────────────────────────────────────────────────────
PROMPT_MENU: list[dict] = [
    {"id": "why_in_highlights",   "group": "context",   "question": "Why is this stock in today's highlights?"},
    {"id": "firing_today",        "group": "context",   "question": "What is firing on this stock right now?"},
    {"id": "best_signals",        "group": "edge",      "question": "Which signals have worked best on this stock?"},
    {"id": "strongest_bull",      "group": "edge",      "question": "What's the strongest bullish setup here?"},
    {"id": "strongest_bear",      "group": "edge",      "question": "What bearish risk should I know?"},
    {"id": "is_base_forming",     "group": "structure", "question": "Is this stock forming a base?"},
    {"id": "near_breakout",       "group": "structure", "question": "Is this stock near a breakout?"},
    {"id": "fallen_from_peak",    "group": "structure", "question": "How far has it fallen from its peak?"},
    {"id": "volume_profile",      "group": "structure", "question": "Has volume been building or drying up?"},
    {"id": "trend_behaviour",     "group": "structure", "question": "How does it behave in uptrends vs downtrends?"},
    {"id": "last_wins",           "group": "evidence",  "question": "Show me the last times a setup worked here."},
    {"id": "last_losses",         "group": "evidence",  "question": "When has a setup failed on this stock?"},
    {"id": "typical_hold",        "group": "evidence",  "question": "What's the typical hold time for winners?"},
    {"id": "recent_drawdown",     "group": "evidence",  "question": "Is the strategy in drawdown recently?"},
    {"id": "sector_peers",        "group": "evidence",  "question": "How does this compare to sector peers?"},
    {"id": "stop_target_ladder",  "group": "trade",     "question": "What's the current stop / target ladder?"},
    {"id": "open_paper_trade",    "group": "trade",     "question": "Is there an open paper trade on this stock?"},
    {"id": "predictability",      "group": "context",   "question": "How predictable is this stock overall?"},
]


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _fmt_win(wr: Optional[float]) -> str:
    if wr is None:
        return "—"
    return f"{int(round(wr * 100))}%"


def _fmt_signed(v: Optional[float], digits: int = 1) -> str:
    if v is None:
        return "—"
    s = "+" if v >= 0 else ""
    return f"{s}{v:.{digits}f}%"


def _get_meta(conn, ticker, market) -> sqlite3.Row:
    r = conn.execute(
        "SELECT ticker, market, company_name, sector FROM sector_mapping WHERE ticker=? AND market=?",
        (ticker, market)).fetchone()
    if not r:
        raise HTTPException(404, f"{ticker} not found in {market}")
    return r


def _ohlc_summary(conn, ticker, market) -> dict:
    """Single-pass ohlc stats for prompt answers."""
    row = conn.execute("""
    WITH latest AS (SELECT MAX(trade_date) d FROM ohlc_daily WHERE market=? AND ticker=?),
    w AS (
      SELECT trade_date, close, high, low, volume
      FROM ohlc_daily
      WHERE market=? AND ticker=?
        AND trade_date >= date((SELECT d FROM latest), '-260 days')
    )
    SELECT
      (SELECT d FROM latest)                                           AS latest_d,
      (SELECT close FROM w WHERE trade_date=(SELECT d FROM latest))    AS close_latest,
      (SELECT volume FROM w WHERE trade_date=(SELECT d FROM latest))   AS vol_latest,
      (SELECT MAX(high) FROM w WHERE trade_date >= date((SELECT d FROM latest),'-90 days'))  AS high_90d,
      (SELECT MAX(high) FROM w WHERE trade_date >= date((SELECT d FROM latest),'-252 days')) AS high_252d,
      (SELECT MIN(low)  FROM w WHERE trade_date >= date((SELECT d FROM latest),'-252 days')) AS low_252d,
      (SELECT AVG(volume) FROM w WHERE trade_date >= date((SELECT d FROM latest),'-20 days')) AS vol_20d_avg,
      (SELECT AVG(close)  FROM w WHERE trade_date >= date((SELECT d FROM latest),'-20 days')) AS close_20d_avg,
      (SELECT MIN(close)  FROM w WHERE trade_date >= date((SELECT d FROM latest),'-20 days')) AS low_20d,
      (SELECT MAX(close)  FROM w WHERE trade_date >= date((SELECT d FROM latest),'-20 days')) AS high_20d
    """, (market, ticker, market, ticker)).fetchone()
    return dict(row) if row else {}


# ─────────────────────────────────────────────────────────────────────
# Prompt handlers — each returns (answer_text, evidence, next_prompts)
# ─────────────────────────────────────────────────────────────────────

def _h_why_in_highlights(conn, ticker, market) -> dict:
    window = f"-{FIRING_WINDOW_DAYS} days"
    rows = conn.execute("""
    WITH latest AS (SELECT MAX(signal_date) d FROM signal_events WHERE market=? AND ticker=?)
    SELECT se.timeframe, se.strategy_name, se.bias, se.signal_date,
           r.tier, f.win_rate_15d, f.total_appearances, f.avg_ret_15d
    FROM signal_events se
    JOIN signal_roster r
      ON r.ticker=se.ticker AND r.market=se.market
     AND r.timeframe=se.timeframe AND r.strategy_name=se.strategy_name AND r.bias=se.bias
    JOIN stock_signal_fitness f
      ON f.ticker=se.ticker AND f.market=se.market
     AND f.timeframe=se.timeframe AND f.strategy_name=se.strategy_name AND f.bias=se.bias
    WHERE se.market=? AND se.ticker=?
      AND se.signal_date >= date((SELECT d FROM latest), ?)
      AND r.tier IN ('core_active','high_conviction','steady')
    ORDER BY
      CASE r.tier WHEN 'high_conviction' THEN 0 WHEN 'core_active' THEN 1 ELSE 2 END,
      f.win_rate_15d DESC
    LIMIT 6
    """, (market, ticker, market, ticker, window)).fetchall()

    if not rows:
        return {
            "answer": f"{ticker} is in view because of its overall track record — "
                      f"no tier-1 setup fired in the last {FIRING_WINDOW_DAYS} trading days.",
            "evidence": [],
            "next_prompts": ["best_signals", "predictability", "near_breakout"],
        }

    top = rows[0]
    bias_word = {"bullish": "bullish", "bearish": "bearish", "neutral": "neutral"}.get(top["bias"], top["bias"])
    answer = (
        f"{TIER_LABEL.get(top['tier'], top['tier'])} {bias_word} setup fired on "
        f"{top['signal_date']} ({'daily' if top['timeframe']=='1D' else 'weekly'}). "
        f"This pattern has fired {top['total_appearances']} times historically on {ticker} "
        f"with a {_fmt_win(top['win_rate_15d'])} 15-day win rate "
        f"and average move of {_fmt_signed(top['avg_ret_15d'])}."
    )
    if len(rows) > 1:
        answer += f" {len(rows)-1} more supporting setup{'s' if len(rows)>2 else ''} also fired."

    return {
        "answer": answer,
        "evidence": [
            {
                "label": f"{TIER_LABEL.get(r['tier'], r['tier'])} · {r['bias']} · "
                         f"{'daily' if r['timeframe']=='1D' else 'weekly'}",
                "detail": f"{r['strategy_name']} · {_fmt_win(r['win_rate_15d'])} wins · "
                          f"{r['total_appearances']} hits · avg {_fmt_signed(r['avg_ret_15d'])}",
                "when": r["signal_date"],
            }
            for r in rows
        ],
        "next_prompts": ["last_wins", "stop_target_ladder", "strongest_bull", "strongest_bear"],
    }


def _h_firing_today(conn, ticker, market) -> dict:
    latest = conn.execute(
        "SELECT MAX(signal_date) d FROM signal_events WHERE market=? AND ticker=?",
        (market, ticker)).fetchone()["d"]
    if not latest:
        return {"answer": "No signals have ever fired on this stock in the tracked universe.",
                "evidence": [], "next_prompts": ["best_signals", "predictability"]}
    rows = conn.execute("""
      SELECT se.timeframe, se.strategy_name, se.bias, se.entry_price, se.signal_date,
             r.tier
      FROM signal_events se
      LEFT JOIN signal_roster r
        ON r.ticker=se.ticker AND r.market=se.market
       AND r.timeframe=se.timeframe AND r.strategy_name=se.strategy_name AND r.bias=se.bias
      WHERE se.market=? AND se.ticker=? AND se.signal_date=?
      ORDER BY r.tier, se.bias, se.timeframe
    """, (market, ticker, latest)).fetchall()
    if not rows:
        return {"answer": f"No setups are firing today on {ticker}.",
                "evidence": [], "next_prompts": ["best_signals", "near_breakout"]}
    bulls = [r for r in rows if r["bias"] == "bullish"]
    bears = [r for r in rows if r["bias"] == "bearish"]
    parts = []
    if bulls: parts.append(f"{len(bulls)} bullish")
    if bears: parts.append(f"{len(bears)} bearish")
    answer = f"{' and '.join(parts)} setup{'s' if len(rows)!=1 else ''} firing on {latest}."
    return {
        "answer": answer,
        "evidence": [
            {"label": f"{r['bias']} · {'daily' if r['timeframe']=='1D' else 'weekly'} · "
                      f"{TIER_LABEL.get(r['tier'] or '', r['tier'] or 'Observing')}",
             "detail": f"{r['strategy_name']} · entry ≈ {r['entry_price']:.2f}",
             "when":   r["signal_date"]}
            for r in rows
        ],
        "next_prompts": ["why_in_highlights", "stop_target_ladder", "last_wins"],
    }


def _h_best_signals(conn, ticker, market) -> dict:
    rows = conn.execute("""
      SELECT r.tier, r.timeframe, r.strategy_name, r.bias,
             f.win_rate_15d, f.total_appearances, f.avg_ret_15d, f.fitness_score
      FROM signal_roster r
      JOIN stock_signal_fitness f
        ON f.ticker=r.ticker AND f.market=r.market
       AND f.timeframe=r.timeframe AND f.strategy_name=r.strategy_name AND f.bias=r.bias
      WHERE r.market=? AND r.ticker=? AND r.tier IN ('core_active','high_conviction','steady')
      ORDER BY r.fitness_score DESC, f.win_rate_15d DESC
      LIMIT 8
    """, (market, ticker)).fetchall()
    if not rows:
        return {"answer": f"No tier-1 setups have been calibrated on {ticker} yet.",
                "evidence": [], "next_prompts": ["predictability", "near_breakout"]}
    top = rows[0]
    answer = (
        f"The strongest edge on {ticker} is a {top['bias']} "
        f"{TIER_LABEL.get(top['tier'], top['tier']).lower()} setup — "
        f"{_fmt_win(top['win_rate_15d'])} wins over {top['total_appearances']} hits, "
        f"avg move {_fmt_signed(top['avg_ret_15d'])}. "
        f"{len(rows)} tier-1 setup{'s' if len(rows)!=1 else ''} are active in total."
    )
    return {
        "answer": answer,
        "evidence": [
            {"label": f"{TIER_LABEL.get(r['tier'], r['tier'])} · {r['bias']} · "
                      f"{'daily' if r['timeframe']=='1D' else 'weekly'}",
             "detail": f"{r['strategy_name']} · {_fmt_win(r['win_rate_15d'])} "
                       f"over {r['total_appearances']} hits · avg {_fmt_signed(r['avg_ret_15d'])}"}
            for r in rows
        ],
        "next_prompts": ["strongest_bull", "strongest_bear", "last_wins", "typical_hold"],
    }


def _h_strongest(conn, ticker, market, bias) -> dict:
    row = conn.execute("""
      SELECT r.tier, r.timeframe, r.strategy_name,
             f.win_rate_15d, f.total_appearances, f.avg_ret_15d, f.wilson_lower_15d,
             f.avg_mfe_pct, f.avg_mae_pct, f.last_signal_date
      FROM signal_roster r
      JOIN stock_signal_fitness f
        ON f.ticker=r.ticker AND f.market=r.market
       AND f.timeframe=r.timeframe AND f.strategy_name=r.strategy_name AND f.bias=r.bias
      WHERE r.market=? AND r.ticker=? AND r.bias=?
        AND r.tier IN ('core_active','high_conviction','steady')
      ORDER BY r.fitness_score DESC, f.win_rate_15d DESC
      LIMIT 1
    """, (market, ticker, bias)).fetchone()
    if not row:
        return {"answer": f"No tier-1 {bias} setup has been calibrated on {ticker}.",
                "evidence": [], "next_prompts": ["best_signals", "predictability"]}
    answer = (
        f"Strongest {bias} edge: {TIER_LABEL.get(row['tier'], row['tier'])} · "
        f"{'daily' if row['timeframe']=='1D' else 'weekly'}. "
        f"Historical {_fmt_win(row['win_rate_15d'])} win rate over "
        f"{row['total_appearances']} hits, avg move {_fmt_signed(row['avg_ret_15d'])}. "
        f"Typical upside excursion {_fmt_signed(row['avg_mfe_pct'])}, "
        f"typical drawdown {_fmt_signed(row['avg_mae_pct'])}."
    )
    return {
        "answer": answer,
        "evidence": [{
            "label":  "Setup detail",
            "detail": f"{row['strategy_name']} · last fired {row['last_signal_date']}",
        }],
        "next_prompts": ["last_wins", "last_losses", "stop_target_ladder", "typical_hold"],
    }


def _h_strongest_bull(conn, t, m): return _h_strongest(conn, t, m, "bullish")
def _h_strongest_bear(conn, t, m): return _h_strongest(conn, t, m, "bearish")


def _h_is_base_forming(conn, ticker, market) -> dict:
    o = _ohlc_summary(conn, ticker, market)
    if not o.get("close_latest"):
        return {"answer": "No price data available for this stock.",
                "evidence": [], "next_prompts": ["best_signals"]}
    rng = (o["high_20d"] or 0) - (o["low_20d"] or 0)
    if rng <= 0:
        return {"answer": "Not enough 20-day range to evaluate a base.",
                "evidence": [], "next_prompts": ["volume_profile"]}
    pos = (o["close_latest"] - o["low_20d"]) / rng
    rng_pct = rng / o["close_20d_avg"] * 100 if o["close_20d_avg"] else 0
    verdict = (
        "Yes — tight 20-day range in the upper half, classic base signature." if (rng_pct < 6 and pos >= 0.5) else
        "Forming but not tight yet — range still wide." if pos >= 0.5 else
        "Not a base — price is in the lower half of the 20-day range."
    )
    answer = (
        f"{verdict} 20-day range is {rng_pct:.1f}% of price; "
        f"current close sits at {int(round(pos*100))}% of that range."
    )
    return {
        "answer": answer,
        "evidence": [
            {"label": "20-day range", "detail": f"{o['low_20d']:.2f} – {o['high_20d']:.2f}"},
            {"label": "Current close", "detail": f"{o['close_latest']:.2f}"},
        ],
        "next_prompts": ["near_breakout", "volume_profile", "strongest_bull"],
    }


def _h_near_breakout(conn, ticker, market) -> dict:
    o = _ohlc_summary(conn, ticker, market)
    if not o.get("close_latest") or not o.get("high_252d"):
        return {"answer": "Not enough price history to evaluate breakout proximity.",
                "evidence": [], "next_prompts": ["volume_profile"]}
    gap = (o["high_252d"] - o["close_latest"]) / o["high_252d"]
    vol_ratio = (o["vol_latest"] / o["vol_20d_avg"]) if o.get("vol_20d_avg") else None
    if gap <= 0:
        verdict = "Breaking out — trading at a new 52-week high."
    elif gap <= 0.03:
        verdict = f"Very close — within {gap*100:.1f}% of its 52-week high."
    elif gap <= 0.10:
        verdict = f"Within range — {gap*100:.1f}% below its 52-week high."
    else:
        verdict = f"Far from breakout — {gap*100:.1f}% below its 52-week high."
    vol_note = ""
    if vol_ratio is not None:
        if vol_ratio <= 0.80:
            vol_note = f" Volume is quiet ({int(round(vol_ratio*100))}% of 20-day average) — "\
                       f"a constructive setup."
        elif vol_ratio >= 1.20:
            vol_note = f" Volume is elevated ({int(round(vol_ratio*100))}% of 20-day average)."
    return {
        "answer": verdict + vol_note,
        "evidence": [
            {"label": "52-week high", "detail": f"{o['high_252d']:.2f}"},
            {"label": "Current close", "detail": f"{o['close_latest']:.2f}"},
        ],
        "next_prompts": ["is_base_forming", "volume_profile", "strongest_bull"],
    }


def _h_fallen_from_peak(conn, ticker, market) -> dict:
    o = _ohlc_summary(conn, ticker, market)
    if not o.get("close_latest") or not o.get("high_90d"):
        return {"answer": "Not enough history.", "evidence": [], "next_prompts": []}
    off90  = (o["high_90d"]  - o["close_latest"]) / o["high_90d"]  if o["high_90d"]  else 0
    off252 = (o["high_252d"] - o["close_latest"]) / o["high_252d"] if o["high_252d"] else 0
    above20 = (o["close_latest"] >= (o["close_20d_avg"] or 0)) if o.get("close_20d_avg") else False
    status = "holding above its 20-day mean" if above20 else "still trading below its 20-day mean"
    return {
        "answer": f"{off90*100:.1f}% below its 90-day peak and {off252*100:.1f}% below its "
                  f"52-week peak. Currently {status}.",
        "evidence": [
            {"label": "90-day high",  "detail": f"{o['high_90d']:.2f}"},
            {"label": "52-week high", "detail": f"{o['high_252d']:.2f}"},
            {"label": "Current close","detail": f"{o['close_latest']:.2f}"},
        ],
        "next_prompts": ["is_base_forming", "volume_profile", "strongest_bull"],
    }


def _h_volume_profile(conn, ticker, market) -> dict:
    o = _ohlc_summary(conn, ticker, market)
    if not o.get("vol_latest") or not o.get("vol_20d_avg"):
        return {"answer": "Not enough volume history.", "evidence": [], "next_prompts": []}
    r = o["vol_latest"] / o["vol_20d_avg"]
    if r <= 0.70:
        verdict = f"Drying up — latest volume is only {int(round(r*100))}% of the 20-day average."
    elif r <= 0.90:
        verdict = f"Slightly quiet — {int(round(r*100))}% of the 20-day average."
    elif r <= 1.10:
        verdict = f"Normal — {int(round(r*100))}% of the 20-day average."
    elif r <= 1.50:
        verdict = f"Elevated — {int(round(r*100))}% of the 20-day average."
    else:
        verdict = f"Heavy — {int(round(r*100))}% of the 20-day average."
    return {
        "answer": verdict,
        "evidence": [
            {"label": "20-day avg volume", "detail": f"{o['vol_20d_avg']:,.0f}"},
            {"label": "Latest volume",     "detail": f"{o['vol_latest']:,.0f}"},
        ],
        "next_prompts": ["is_base_forming", "near_breakout"],
    }


def _h_trend_behaviour(conn, ticker, market) -> dict:
    row = conn.execute("""
      SELECT AVG(win_rate_in_uptrend)   wr_up,   SUM(appearances_in_uptrend)   n_up,
             AVG(win_rate_in_downtrend) wr_dn,   SUM(appearances_in_downtrend) n_dn,
             AVG(win_rate_in_range)     wr_rg,   SUM(appearances_in_range)     n_rg
      FROM stock_signal_fitness
      WHERE ticker=? AND market=? AND bias='bullish'
    """, (ticker, market)).fetchone()
    if not row or (row["n_up"] or 0) + (row["n_dn"] or 0) + (row["n_rg"] or 0) == 0:
        return {"answer": "Not enough regime-segmented history yet.",
                "evidence": [], "next_prompts": ["best_signals"]}
    parts = []
    if row["n_up"]:  parts.append(f"uptrends: {_fmt_win(row['wr_up'])} over {int(row['n_up'])} hits")
    if row["n_rg"]:  parts.append(f"range-bound: {_fmt_win(row['wr_rg'])} over {int(row['n_rg'])} hits")
    if row["n_dn"]:  parts.append(f"downtrends: {_fmt_win(row['wr_dn'])} over {int(row['n_dn'])} hits")
    return {
        "answer": "Bullish setups have historically performed — " + "; ".join(parts) + ".",
        "evidence": [], "next_prompts": ["best_signals", "strongest_bull"],
    }


def _h_last_wins(conn, ticker, market) -> dict:
    rows = conn.execute("""
      SELECT timeframe, strategy_name, bias, entry_date, entry_price,
             exit_date, exit_price, outcome_pct, days_held, roster_tier
      FROM paper_trades
      WHERE ticker=? AND market=? AND win=1
      ORDER BY COALESCE(exit_date, entry_date) DESC
      LIMIT 10
    """, (ticker, market)).fetchall()
    if not rows:
        return {"answer": "No winning paper trades on record yet.",
                "evidence": [], "next_prompts": ["best_signals", "predictability"]}
    answer = f"Last {len(rows)} winning paper trade{'s' if len(rows)!=1 else ''} on {ticker}."
    def _price_arrow(r):
        exit_txt = f"{r['exit_price']:.2f}" if r['exit_price'] is not None else "—"
        return f"{r['entry_price']:.2f} → {exit_txt}"
    return {
        "answer": answer,
        "evidence": [
            {"label": f"{r['bias']} · {'daily' if r['timeframe']=='1D' else 'weekly'} · "
                      f"{TIER_LABEL.get(r['roster_tier'] or '', 'Observing')}",
             "detail": f"{r['strategy_name']} · "
                       f"{_fmt_signed(r['outcome_pct'])} in {r['days_held'] or '—'}d "
                       f"({_price_arrow(r)})",
             "when": r["exit_date"] or r["entry_date"]}
            for r in rows
        ],
        "next_prompts": ["last_losses", "typical_hold", "stop_target_ladder"],
    }


def _h_last_losses(conn, ticker, market) -> dict:
    rows = conn.execute("""
      SELECT timeframe, strategy_name, bias, entry_date, entry_price,
             exit_date, exit_price, outcome_pct, days_held, roster_tier
      FROM paper_trades
      WHERE ticker=? AND market=? AND win=0
      ORDER BY COALESCE(exit_date, entry_date) DESC
      LIMIT 10
    """, (ticker, market)).fetchall()
    if not rows:
        return {"answer": "No losing paper trades on record — uncommon but noted.",
                "evidence": [], "next_prompts": ["last_wins", "best_signals"]}
    return {
        "answer": f"Last {len(rows)} losing paper trade{'s' if len(rows)!=1 else ''} on {ticker}.",
        "evidence": [
            {"label": f"{r['bias']} · {'daily' if r['timeframe']=='1D' else 'weekly'} · "
                      f"{TIER_LABEL.get(r['roster_tier'] or '', 'Observing')}",
             "detail": f"{r['strategy_name']} · "
                       f"{_fmt_signed(r['outcome_pct'])} in {r['days_held'] or '—'}d",
             "when": r["exit_date"] or r["entry_date"]}
            for r in rows
        ],
        "next_prompts": ["last_wins", "recent_drawdown", "best_signals"],
    }


def _h_typical_hold(conn, ticker, market) -> dict:
    row = conn.execute("""
      SELECT AVG(CASE WHEN win=1 THEN days_held END) avg_w,
             AVG(CASE WHEN win=0 THEN days_held END) avg_l,
             COUNT(*) n
      FROM paper_trades
      WHERE ticker=? AND market=? AND status!='open' AND days_held IS NOT NULL
    """, (ticker, market)).fetchone()
    if not row or (row["n"] or 0) == 0:
        return {"answer": "Not enough closed trades to estimate typical hold time.",
                "evidence": [], "next_prompts": ["last_wins"]}
    parts = []
    if row["avg_w"] is not None: parts.append(f"winners ≈ {row['avg_w']:.1f} days")
    if row["avg_l"] is not None: parts.append(f"losers ≈ {row['avg_l']:.1f} days")
    return {
        "answer": "Typical hold: " + ", ".join(parts) + f" (across {row['n']} trades).",
        "evidence": [], "next_prompts": ["last_wins", "stop_target_ladder"],
    }


def _h_recent_drawdown(conn, ticker, market) -> dict:
    rows = conn.execute("""
      SELECT outcome_pct, win
      FROM paper_trades
      WHERE ticker=? AND market=? AND status!='open' AND outcome_pct IS NOT NULL
      ORDER BY COALESCE(exit_date, entry_date) DESC
      LIMIT 5
    """, (ticker, market)).fetchall()
    if not rows:
        return {"answer": "No recent closed trades.",
                "evidence": [], "next_prompts": ["best_signals"]}
    wins = sum(1 for r in rows if r["win"] == 1)
    ret_sum = sum((r["outcome_pct"] or 0) for r in rows)
    verdict = (
        "Hot streak recently." if wins >= 4 else
        "In drawdown recently." if wins <= 1 else
        "Mixed recent results."
    )
    return {
        "answer": f"{verdict} Last 5 closed trades: {wins} wins, "
                  f"cumulative {_fmt_signed(ret_sum)}.",
        "evidence": [], "next_prompts": ["last_losses", "best_signals"],
    }


def _h_sector_peers(conn, ticker, market) -> dict:
    me = conn.execute("SELECT sector FROM sector_mapping WHERE ticker=? AND market=?",
                      (ticker, market)).fetchone()
    if not me or not me["sector"]:
        return {"answer": "Sector not set for this stock.",
                "evidence": [], "next_prompts": ["best_signals"]}
    rows = conn.execute("""
      SELECT sm.ticker,
             AVG(r.fitness_score) avg_fit,
             COUNT(CASE WHEN r.tier IN ('core_active','high_conviction') THEN 1 END) n_top
      FROM sector_mapping sm
      JOIN signal_roster r ON r.ticker=sm.ticker AND r.market=sm.market
      WHERE sm.market=? AND sm.sector=? AND r.tier IS NOT NULL AND r.tier!='retired'
      GROUP BY sm.ticker
      ORDER BY n_top DESC, avg_fit DESC
      LIMIT 8
    """, (market, me["sector"])).fetchall()
    if not rows:
        return {"answer": f"No peer data available for sector {me['sector']}.",
                "evidence": [], "next_prompts": ["best_signals"]}
    my_rank = next((i+1 for i, r in enumerate(rows) if r["ticker"] == ticker), None)
    rank_txt = f"ranks #{my_rank} of {len(rows)}" if my_rank else f"is outside the top {len(rows)}"
    return {
        "answer": f"In {me['sector']}, {ticker} {rank_txt} by setup depth.",
        "evidence": [
            {"label": r["ticker"],
             "detail": f"{int(r['n_top'])} top-tier setups · avg fitness {r['avg_fit']:.0f}"}
            for r in rows
        ],
        "next_prompts": ["best_signals", "predictability"],
    }


def _h_stop_target_ladder(conn, ticker, market) -> dict:
    rows = conn.execute("""
      SELECT timeframe, strategy_name, bias, entry_date, entry_price,
             stop_price, target_1, target_2, roster_tier, status
      FROM paper_trades
      WHERE ticker=? AND market=? AND status='open'
      ORDER BY entry_date DESC
      LIMIT 5
    """, (ticker, market)).fetchall()
    if not rows:
        return {"answer": "No open paper trade on this stock — no active stop/target ladder.",
                "evidence": [], "next_prompts": ["firing_today", "best_signals"]}
    top = rows[0]
    answer = (
        f"Open {top['bias']} position from {top['entry_date']}: "
        f"entry {top['entry_price']:.2f}"
        + (f", stop {top['stop_price']:.2f}" if top['stop_price'] else "")
        + (f", target {top['target_1']:.2f}" if top['target_1'] else "")
        + (f" / {top['target_2']:.2f}" if top['target_2'] else "")
        + "."
    )
    return {
        "answer": answer,
        "evidence": [
            {"label": f"{r['bias']} · {'daily' if r['timeframe']=='1D' else 'weekly'} · "
                      f"{TIER_LABEL.get(r['roster_tier'] or '', 'Observing')}",
             "detail": f"{r['strategy_name']} · "
                       f"entry {r['entry_price']:.2f}"
                       + (f" · stop {r['stop_price']:.2f}" if r['stop_price'] else "")
                       + (f" · t1 {r['target_1']:.2f}" if r['target_1'] else ""),
             "when": r["entry_date"]}
            for r in rows
        ],
        "next_prompts": ["open_paper_trade", "typical_hold", "last_wins"],
    }


def _h_open_paper_trade(conn, ticker, market) -> dict:
    return _h_stop_target_ladder(conn, ticker, market)


def _h_predictability(conn, ticker, market) -> dict:
    row = conn.execute("""
      SELECT AVG(fitness_score) avg_fit,
             COUNT(*) n_active,
             SUM(CASE WHEN tier IN ('core_active','high_conviction') THEN 1 ELSE 0 END) n_top
      FROM signal_roster
      WHERE ticker=? AND market=? AND tier IS NOT NULL AND tier!='retired'
    """, (ticker, market)).fetchone()
    if not row or not row["n_active"]:
        return {"answer": "No active setups calibrated — predictability unknown.",
                "evidence": [], "next_prompts": ["best_signals"]}
    avg_fit = float(row["avg_fit"] or 0)
    n_top = int(row["n_top"] or 0)
    n_active = int(row["n_active"])
    top_share = n_top / n_active if n_active else 0
    score = min(100.0, avg_fit + 40.0 * top_share)
    verdict = (
        "Highly predictable — strong edges on this stock." if score >= 75 else
        "Predictable — several working setups." if score >= 60 else
        "Moderately predictable." if score >= 45 else
        "Low predictability — few edges right now."
    )
    return {
        "answer": f"{verdict} Predictability score {score:.0f} / 100 based on "
                  f"{n_active} active setup{'s' if n_active!=1 else ''} "
                  f"({n_top} top-tier).",
        "evidence": [], "next_prompts": ["best_signals", "sector_peers"],
    }


HANDLERS: dict[str, Callable] = {
    "why_in_highlights":  _h_why_in_highlights,
    "firing_today":       _h_firing_today,
    "best_signals":       _h_best_signals,
    "strongest_bull":     _h_strongest_bull,
    "strongest_bear":     _h_strongest_bear,
    "is_base_forming":    _h_is_base_forming,
    "near_breakout":      _h_near_breakout,
    "fallen_from_peak":   _h_fallen_from_peak,
    "volume_profile":     _h_volume_profile,
    "trend_behaviour":    _h_trend_behaviour,
    "last_wins":          _h_last_wins,
    "last_losses":        _h_last_losses,
    "typical_hold":       _h_typical_hold,
    "recent_drawdown":    _h_recent_drawdown,
    "sector_peers":       _h_sector_peers,
    "stop_target_ladder": _h_stop_target_ladder,
    "open_paper_trade":   _h_open_paper_trade,
    "predictability":     _h_predictability,
}


# ─────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────

@router.get("/prompts/{ticker}")
def list_prompts(ticker: str, market: str = Query("NSE")):
    if market not in ("NSE", "US"):
        raise HTTPException(400, "market must be NSE or US")
    ticker = ticker.upper()
    try:
        conn = _conn()
        meta = _get_meta(conn, ticker, market)
        latest = conn.execute(
            "SELECT MAX(signal_date) d FROM signal_events WHERE market=? AND ticker=?",
            (market, ticker)).fetchone()["d"]
        conn.close()
        return {
            "ticker":       meta["ticker"],
            "market":       meta["market"],
            "company_name": meta["company_name"],
            "sector":       meta["sector"],
            "latest_signal_date": latest,
            "prompts":      PROMPT_MENU,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/prompts/{ticker}/answer")
def answer_prompt(
    ticker: str,
    prompt_id: str = Query(...),
    market: str = Query("NSE"),
):
    if market not in ("NSE", "US"):
        raise HTTPException(400, "market must be NSE or US")
    handler = HANDLERS.get(prompt_id)
    if not handler:
        raise HTTPException(404, f"unknown prompt_id: {prompt_id}")
    ticker = ticker.upper()
    try:
        conn = _conn()
        _ = _get_meta(conn, ticker, market)
        result = handler(conn, ticker, market)
        conn.close()
        # Attach the question text for UI convenience
        q = next((p["question"] for p in PROMPT_MENU if p["id"] == prompt_id), prompt_id)
        # Expand next_prompts to full question strings
        np = []
        for pid in result.get("next_prompts", []):
            q2 = next((p["question"] for p in PROMPT_MENU if p["id"] == pid), None)
            if q2:
                np.append({"id": pid, "question": q2})
        return {
            "ticker":       ticker,
            "market":       market,
            "prompt_id":    prompt_id,
            "question":     q,
            "answer":       result.get("answer", ""),
            "evidence":     result.get("evidence", []),
            "next_prompts": np,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
