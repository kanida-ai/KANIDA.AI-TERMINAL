"""
KANIDA.AI Quant Intelligence Engine — API Router
Serves pattern_library + live_opportunities from kanida_quant.db
to the existing frontend (terminal_router shape-compatible).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger(__name__)
router = APIRouter()

# ── DB path ────────────────────────────────────────────────────────────────────
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent))
from db import get_conn

DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"),
)


def _conn():
    return get_conn()


# ── Mapping helpers ────────────────────────────────────────────────────────────

def _direction_to_bias(direction: str) -> str:
    return "bullish" if direction == "rally" else "bearish"

def _tier_to_conviction(tier: str) -> str:
    if tier == "high_conviction": return "HIGH"
    if tier == "medium":          return "MEDIUM"
    return "LOW"

def _classify_signal(row: dict) -> str:
    pattern = str(row.get("behavior_pattern", ""))
    tier    = str(row.get("tier", ""))
    opp     = float(row.get("opportunity_score") or 0)
    if "breakout" in pattern:   return "BREAKOUT_READY"
    if "volume_dryup" in pattern or "compressed" in pattern: return "VOLUME_DRY"
    if "range_contrac" in pattern or "pullback" in pattern:  return "BASE_FORMING"
    if row.get("direction") == "fall":  return "PEAK_FALL"
    if tier == "high_conviction": return "BREAKOUT_READY"
    if opp > 0.65:                return "HIGH_EDGE"
    return "BASE_FORMING"

def _make_headline(row: dict) -> str:
    setup = row.get("setup_summary") or ""
    if setup and len(setup) > 20:
        # First sentence only
        return setup.split(".")[0].strip() + "."
    direction = "rally" if row.get("direction") == "rally" else "fall"
    target    = float(row.get("target_move") or 0)
    prob      = float(row.get("display_probability") or 0)
    return (
        f"{row['ticker']} is showing a {direction} setup "
        f"targeting {target*100:.0f}% with {prob*100:.0f}% historical probability."
    )

def _make_subline(row: dict) -> str:
    occ  = int(row.get("occurrences") or 0)
    prob = float(row.get("display_probability") or 0)
    lift = float(row.get("lift") or 0)
    cred = row.get("credibility") or "exploratory"
    return (
        f"{prob*100:.0f}% win rate | {occ} occurrences | "
        f"{lift:.2f}x baseline lift | {cred}"
    )

def _make_trigger(row: dict) -> str:
    pattern = str(row.get("behavior_pattern") or "")
    prob    = float(row.get("display_probability") or 0)
    occ     = int(row.get("occurrences") or 0)
    return (
        f"Pattern: {pattern} - "
        f"{prob*100:.0f}% win rate across {occ} occurrences"
    )

def _suggested_prompts(ticker: str, signal_type: str) -> list[str]:
    p = {
        "BREAKOUT_READY": [
            f"What is the win rate on breakout setups for {ticker}?",
            f"Show me performance when {ticker} broke out historically",
            f"What is the risk:reward on {ticker} at current levels?",
        ],
        "BASE_FORMING": [
            f"Which patterns worked best during {ticker} base formations?",
            f"How long do {ticker} bases typically last?",
            f"What happens after {ticker} breaks out of a base?",
        ],
        "VOLUME_DRY": [
            f"What happens to {ticker} after volume compression?",
            f"What are {ticker} best setups during consolidation?",
            f"Show me {ticker} historical volatility patterns.",
        ],
        "PEAK_FALL": [
            f"How deep has {ticker} fallen from its peak historically?",
            f"When does {ticker} typically recover after a fall?",
            f"What is the current bearish edge strength for {ticker}?",
        ],
        "HIGH_EDGE": [
            f"What does history say about {ticker}?",
            f"Which strategies have the highest win rate for {ticker}?",
            f"Tell me about {ticker} current setup.",
        ],
    }
    return p.get(signal_type, p["HIGH_EDGE"])

def _live_row_to_card(row: dict) -> dict:
    bias       = _direction_to_bias(row.get("direction", "rally"))
    conviction = _tier_to_conviction(row.get("tier", "exploratory"))
    sig_type   = _classify_signal(row)
    win_rate   = float(row.get("display_probability") or 0)
    occ        = int(row.get("occurrences") or 0)
    opp_score  = float(row.get("opportunity_score") or 0)
    dec_score  = float(row.get("decision_score") or 0)
    avg_ret    = float(row.get("avg_forward_return") or 0)
    if bias == "bearish":
        avg_ret = -abs(avg_ret)

    return {
        "ticker":           row["ticker"],
        "market":           row.get("market", "NSE"),
        "signalType":       sig_type,
        "headline":         _make_headline(row),
        "subline":          _make_subline(row),
        "triggerReason":    _make_trigger(row),
        "suggestedPrompts": _suggested_prompts(row["ticker"], sig_type),
        "conviction":       conviction,
        "edgeScore":        int(min(100, max(0, (opp_score * 0.6 + dec_score * 0.4) * 100))),
        "price":            row.get("current_close"),
        "regime":           _infer_regime(row),
        "regimeScore":      int(opp_score * 100),
        "avgWinRate":       round(win_rate, 4),
        "avgOutcome":       round(avg_ret * 100, 2),
        "primaryBias":      bias,
        "totalSignals":     occ,
        "snapshotDate":     (row.get("latest_date") or row.get("created_at") or "")[:10],
        "isCounterTrend":   False,
        "dominantBias":     bias,
        "dominantWinRate":  round(win_rate, 4),
        "dominantSignals":  occ,
        # extra fields for ticker detail
        "behavior_pattern": row.get("behavior_pattern"),
        "target_move":      row.get("target_move"),
        "forward_window":   row.get("forward_window"),
        "lift":             row.get("lift"),
        "credibility":      row.get("credibility"),
        "decay_flag":       row.get("decay_flag"),
    }

def _infer_regime(row: dict) -> str:
    behavior = str(row.get("current_behavior") or "")
    if "strong_up" in behavior or "up_continuation" in behavior:
        return "BULL"
    if "strong_down" in behavior or "down_continuation" in behavior:
        return "BEAR"
    return "MIXED"


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/health")
def health():
    try:
        con = _conn()
        patterns  = con.execute("SELECT COUNT(*) FROM pattern_library").fetchone()[0]
        live      = con.execute("SELECT COUNT(*) FROM live_opportunities").fetchone()[0]
        tickers   = con.execute("SELECT COUNT(DISTINCT ticker) FROM pattern_library").fetchone()[0]
        daily_bars = con.execute("SELECT COUNT(*) FROM ohlc_daily").fetchone()[0]
        run = con.execute(
            "SELECT started_at, finished_at, status FROM snapshot_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        con.close()
        last_run  = run["finished_at"] if run else None
        last_date = last_run[:10] if last_run else None
        today     = datetime.today().strftime("%Y-%m-%d")
        return {
            "status":               "ok",
            "db_path":              DB_PATH,
            "patterns":             patterns,
            "live_opportunities":   live,
            "tickers":              tickers,
            "daily_bars":           daily_bars,
            "last_learning_run":    last_run,
            # Legacy-compatible fields (for frontend Health type)
            "fingerprints":         patterns,
            "paper_trades":         0,
            "snapshots":            live,
            "snapshot_date":        last_date,
            "snapshot_age_minutes": None,
            "snapshot_stale":       last_date != today if last_date else True,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/feed")
def get_feed(market: str = "NSE", limit: int = 20):
    try:
        con = _conn()
        # Get the most recent successful run
        run = con.execute(
            "SELECT id FROM snapshot_runs WHERE status='success' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not run:
            return {"market": market, "count": 0, "cards": []}

        rows = con.execute("""
            SELECT lo.*, pl.avg_forward_return
            FROM live_opportunities lo
            LEFT JOIN pattern_library pl
              ON pl.market=lo.market AND pl.ticker=lo.ticker
              AND pl.direction=lo.direction AND pl.target_move=lo.target_move
              AND pl.forward_window=lo.forward_window AND pl.behavior_pattern=lo.behavior_pattern
            WHERE lo.snapshot_run_id = ? AND lo.market = ?
            ORDER BY lo.decision_score DESC
            LIMIT ?
        """, [run["id"], market.upper(), limit]).fetchall()
        con.close()

        cards = [_live_row_to_card(dict(r)) for r in rows]
        # Sort HIGH first
        cards.sort(key=lambda c: (0 if c["conviction"] == "HIGH" else 1, -c["avgWinRate"]))
        return {"market": market, "count": len(cards), "cards": cards}
    except Exception as e:
        log.error(f"Feed error: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@router.get("/feed/grouped")
def get_feed_grouped(market: str = "NSE"):
    result = get_feed(market=market, limit=40)
    all_cards = result.get("cards", [])
    GROUP_META = {
        "BREAKOUT_READY": {"label": "Breakout Ready",   "sub": "Volume + momentum confluence",              "bull": True},
        "BASE_FORMING":   {"label": "Base Formation",   "sub": "Tight consolidation - watching resolution", "bull": True},
        "VOLUME_DRY":     {"label": "Volume Dry-Up",    "sub": "Compression — potential energy building",   "bull": True},
        "PEAK_FALL":      {"label": "Fallen from Peak", "sub": "Off highs with sustained pressure",         "bull": False},
        "HIGH_EDGE":      {"label": "High Edge",        "sub": "Strong historical edge confirmed",          "bull": True},
    }
    buckets: dict = {k: [] for k in GROUP_META}
    for card in all_cards:
        st = card.get("signalType", "HIGH_EDGE")
        if st in buckets:
            buckets[st].append(card)
    groups = []
    for st, meta in GROUP_META.items():
        grp = buckets[st]
        if grp:
            groups.append({
                "signalType": st, "label": meta["label"], "sub": meta["sub"],
                "bull": meta["bull"], "count": len(grp),
                "tickers": [c["ticker"] for c in grp[:4]], "cards": grp[:4],
            })
    return {"market": market, "groups": groups, "total": len(all_cards)}


@router.get("/screener")
def screener(market: str = "NSE", bias: str = "bullish"):
    direction = "rally" if bias == "bullish" else "fall"
    try:
        con = _conn()
        rows = con.execute("""
            SELECT ticker, market,
                   AVG(display_probability) as avg_win_rate,
                   SUM(occurrences) as total_signals,
                   AVG(avg_forward_return) as avg_return,
                   MAX(opportunity_score) as best_score,
                   MAX(tier) as best_tier,
                   COUNT(*) as pattern_count
            FROM pattern_library
            WHERE market = ? AND direction = ?
            GROUP BY market, ticker
            ORDER BY avg_win_rate DESC
            LIMIT 50
        """, [market.upper(), direction]).fetchall()
        con.close()

        result = []
        for i, r in enumerate(rows):
            wr    = float(r["avg_win_rate"] or 0)
            score = float(r["best_score"] or 0)
            tier  = r["best_tier"] or "exploratory"
            conv  = "HIGH" if tier == "high_conviction" else ("MEDIUM" if wr > 0.40 else "LOW")
            result.append({
                "rank":       i + 1,
                "ticker":     r["ticker"],
                "conviction": conv,
                "win_rate":   round(wr * 100, 1),
                "signals":    int(r["total_signals"] or 0),
                "avg_gain":   round(float(r["avg_return"] or 0) * 100, 2),
                "regime":     "NSE",
                "price":      None,
            })
        return {"bias": bias, "market": market.upper(), "count": len(result), "rows": result}
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/strategies")
def strategies(market: str = "NSE", bias: str = "bullish"):
    direction = "rally" if bias == "bullish" else "fall"
    try:
        con = _conn()
        rows = con.execute("""
            SELECT ticker, market, direction, behavior_pattern,
                   forward_window, target_move,
                   display_probability as win_rate,
                   avg_forward_return, occurrences, tier, credibility
            FROM pattern_library
            WHERE market = ? AND direction = ? AND tier != 'exploratory'
            ORDER BY display_probability DESC, occurrences DESC
            LIMIT 30
        """, [market.upper(), direction]).fetchall()
        con.close()
        return {
            "market": market, "bias": bias, "count": len(rows),
            "rows": [
                {
                    "ticker":      r["ticker"],
                    "pattern":     r["behavior_pattern"],
                    "timeframe":   f"{r['forward_window']}D",
                    "bias":        bias,
                    "win_rate":    round(float(r["win_rate"] or 0) * 100, 1),
                    "avg_gain":    round(float(r["avg_forward_return"] or 0) * 100, 2),
                    "conviction":  _tier_to_conviction(r["tier"] or "exploratory"),
                    "occurrences": r["occurrences"],
                }
                for r in rows
            ],
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/ticker/{ticker}")
def ticker_data(ticker: str, market: str = "NSE"):
    ticker = ticker.upper()
    try:
        con = _conn()

        # Check if ticker exists
        exists = con.execute(
            "SELECT COUNT(*) FROM pattern_library WHERE ticker=? AND market=?",
            [ticker, market.upper()]
        ).fetchone()[0]
        if not exists:
            raise HTTPException(404, f"{ticker} not found in the engine database")

        # Latest OHLCV for price
        latest_bar = con.execute("""
            SELECT close, trade_date FROM ohlc_daily
            WHERE ticker=? AND market=? AND quality_flag!='rejected'
            ORDER BY trade_date DESC LIMIT 1
        """, [ticker, market.upper()]).fetchone()
        current_price = float(latest_bar["close"]) if latest_bar else None

        # Bullish stats
        bull = con.execute("""
            SELECT AVG(display_probability) as avg_wr,
                   MAX(display_probability) as best_wr,
                   AVG(avg_forward_return)  as avg_gain,
                   MAX(avg_forward_return)  as best_gain,
                   MIN(avg_forward_return)  as worst,
                   SUM(occurrences)         as total,
                   MAX(tier)               as best_tier,
                   COUNT(*)                as cnt
            FROM pattern_library
            WHERE ticker=? AND market=? AND direction='rally'
        """, [ticker, market.upper()]).fetchone()

        # Bearish stats
        bear = con.execute("""
            SELECT AVG(display_probability) as avg_wr,
                   AVG(avg_forward_return)  as avg_gain,
                   SUM(occurrences)         as total,
                   MAX(tier)               as best_tier
            FROM pattern_library
            WHERE ticker=? AND market=? AND direction='fall'
        """, [ticker, market.upper()]).fetchone()

        # Top patterns (bullish)
        top_patterns = con.execute("""
            SELECT behavior_pattern, display_probability, avg_forward_return, occurrences, tier
            FROM pattern_library
            WHERE ticker=? AND market=? AND direction='rally'
            ORDER BY opportunity_score DESC LIMIT 5
        """, [ticker, market.upper()]).fetchall()

        # Live opportunity
        live_opp = con.execute("""
            SELECT lo.*, pl.avg_forward_return
            FROM live_opportunities lo
            LEFT JOIN pattern_library pl ON
                pl.ticker=lo.ticker AND pl.market=lo.market
                AND pl.direction=lo.direction AND pl.behavior_pattern=lo.behavior_pattern
            WHERE lo.ticker=? AND lo.market=?
            ORDER BY lo.decision_score DESC LIMIT 1
        """, [ticker, market.upper()]).fetchone()

        con.close()

        bull_wr   = float(bull["avg_wr"] or 0)
        bear_wr   = float(bear["avg_wr"] or 0)
        bull_tier = bull["best_tier"] or "exploratory"
        dom_bias  = "bullish" if bull_wr >= bear_wr else "bearish"

        # Price levels (ATR-based rough estimate)
        tp1 = round(current_price * 1.05, 2) if current_price else None
        tp2 = round(current_price * 1.10, 2) if current_price else None
        sl  = round(current_price * 0.97, 2) if current_price else None

        active_signal = None
        if live_opp:
            lo = dict(live_opp)
            active_signal = {
                "bias":            _direction_to_bias(lo.get("direction", "rally")),
                "score_pct":       int(float(lo.get("decision_score") or 0) * 100),
                "score_label":     _tier_to_conviction(lo.get("tier") or "exploratory"),
                "firing_count":    lo.get("occurrences") or 0,
                "qualified_total": lo.get("occurrences") or 0,
                "top_strategy":    lo.get("behavior_pattern") or "",
                "top_win_rate":    float(lo.get("display_probability") or 0),
                "snapshot_time":   lo.get("created_at") or "",
                "snapshot_date":   (lo.get("latest_date") or lo.get("created_at") or "")[:10],
                "timeframe":       f"{lo.get('forward_window', 10)}D",
                "regime":          _infer_regime(lo),
                "regime_score":    int(float(lo.get("opportunity_score") or 0) * 100),
                "avg_win":         round(float(lo.get("avg_forward_return") or 0) * 100, 2),
                "avg_loss":        None,
                "signal_age_days": 1,
            }

        return {
            "ticker":       ticker,
            "market":       market.upper(),
            "primary_bias": dom_bias,
            "conviction":   _tier_to_conviction(bull_tier),
            "regime": {
                "regime":       _infer_regime(dict(live_opp) if live_opp else {}),
                "regime_score": int(bull_wr * 100),
                "regime_label": "Pattern-derived",
                "snapshot_date": datetime.today().strftime("%Y-%m-%d"),
            },
            "bullish": {
                "conviction":    _tier_to_conviction(bull_tier),
                "avg_win_rate":  round(bull_wr * 100, 1),
                "best_win_rate": round(float(bull["best_wr"] or 0) * 100, 1),
                "avg_gain":      round(float(bull["avg_gain"] or 0) * 100, 2),
                "best_gain":     round(float(bull["best_gain"] or 0) * 100, 2),
                "worst_loss":    round(float(bull["worst"] or 0) * 100, 2),
                "total_signals": int(bull["total"] or 0),
                "data_depth":    "3 years",
                "patterns": [
                    {
                        "category":    p["behavior_pattern"],
                        "win_rate":    round(float(p["display_probability"] or 0) * 100, 1),
                        "avg_gain":    round(float(p["avg_forward_return"] or 0) * 100, 2),
                        "occurrences": p["occurrences"],
                    }
                    for p in top_patterns
                ],
            } if bull["cnt"] else None,
            "bearish": {
                "conviction":    _tier_to_conviction(bear["best_tier"] or "exploratory"),
                "avg_win_rate":  round(bear_wr * 100, 1),
                "avg_gain":      round(float(bear["avg_gain"] or 0) * 100, 2),
                "total_signals": int(bear["total"] or 0),
            } if bear["total"] else None,
            "levels": {
                "price":       current_price,
                "target_1":    tp1,
                "target_2":    tp2,
                "stop_loss":   sl,
                "t1_pct":      5.0,
                "t2_pct":      10.0,
                "sl_pct":      -3.0,
                "levels_bias": dom_bias,
                "rr":          round(5.0 / 3.0, 2),
            } if current_price else None,
            "active_signal":      active_signal,
            "historical_context": {
                "dominant_bias":          dom_bias,
                "dominant_win_rate":      round(max(bull_wr, bear_wr) * 100, 1),
                "dominant_total_signals": int((bull["total"] or 0) + (bear["total"] or 0)),
                "is_counter_trend":       False,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Chat endpoint (Claude) ─────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message:  str
    history:  list = []
    ticker:   Optional[str] = None
    market:   Optional[str] = None
    intent:   Optional[str] = None


@router.post("/chat")
def chat(req: ChatRequest):
    try:
        import anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key or "YOUR_KEY" in api_key:
            return {
                "type": "general",
                "response": (
                    "KANIDA.AI chat is not yet configured. "
                    "Add your ANTHROPIC_API_KEY to config/.env to enable conversational analysis. "
                    "Pattern data is live — use the feed and screener endpoints."
                ),
            }

        # Build context from DB if ticker provided
        context = ""
        ticker = (req.ticker or "").upper()
        market = (req.market or "NSE").upper()
        if ticker:
            try:
                d = ticker_data(ticker, market)
                bull = d.get("bullish") or {}
                context = (
                    f"{ticker} ({market}): "
                    f"bullish win rate {bull.get('avg_win_rate', 0):.1f}%, "
                    f"{bull.get('total_signals', 0)} occurrences, "
                    f"conviction {d.get('conviction', 'unknown')}. "
                    f"Current price: {(d.get('levels') or {}).get('price', 'N/A')}."
                )
            except Exception:
                pass

        system = (
            "You are KANIDA.AI, a quant intelligence engine for NSE and US stocks. "
            "You have access to pattern-learning data from verified Kite Connect and Polygon.io sources. "
            "Give direct, evidence-based analysis. Avoid financial advice disclaimers — "
            "frame everything as statistical observations. Be concise."
        )
        if context:
            system += f"\n\nLive data: {context}"

        client = anthropic.Anthropic(api_key=api_key)
        messages = [{"role": m["role"], "content": m["content"]} for m in req.history if m.get("content")]
        messages.append({"role": "user", "content": req.message})

        resp = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=1100,
            system=system,
            messages=messages,
        )
        return {
            "type":     "ticker" if ticker else "general",
            "ticker":   ticker or None,
            "market":   market,
            "response": resp.content[0].text,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/snapshot/status")
def snapshot_status():
    try:
        con = _conn()
        runs = con.execute("""
            SELECT run_type, status, started_at, finished_at,
                   learned_patterns, live_opportunities, tickers_processed
            FROM snapshot_runs ORDER BY id DESC LIMIT 5
        """).fetchall()
        con.close()
        return {
            "today":         datetime.today().strftime("%Y-%m-%d"),
            "snapshots":     [dict(r) for r in runs],
            "build_running": False,
            "last_build_started": runs[0]["started_at"] if runs else None,
            "last_build_result":  dict(runs[0]) if runs else None,
        }
    except Exception as e:
        raise HTTPException(500, str(e))
