"""
KANIDA.AI TERMINAL — API Router
================================
Wraps the existing kanida_quant.py and kanida_chat.py engines into
FastAPI endpoints. These two files are the intelligence core — this
router is just the HTTP interface on top of them.

Endpoints:
  POST /api/chat          — main chat endpoint (ticker analysis, screener, strategies)
  GET  /api/feed          — autonomous insight feed (top signals)
  GET  /api/screener      — screener by market + bias
  GET  /api/ticker/{ticker} — full ticker analysis (no chat, raw quant data)
  GET  /api/health        — health + DB stats

Place kanida_quant.py and kanida_chat.py in the same folder as this file
(backend/) or in backend/agents/.

Add to main.py:
  from routers.terminal_router import router as terminal_router
  app.include_router(terminal_router, prefix="/api", tags=["Terminal"])
"""

import os, sys, re, sqlite3, logging, threading
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger(__name__)
router = APIRouter()

# ── Import quant + chat engines ───────────────────────────────────────────────
# They live in backend/agents/ — add to path
_HERE = os.path.dirname(os.path.abspath(__file__))
_AGENTS = os.path.join(_HERE, "..", "agents")
_BACKEND = os.path.join(_HERE, "..")
for p in [_AGENTS, _BACKEND]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    import kanida_quant as Q
    import kanida_chat as CH
    ENGINES_OK = True
    log.info("kanida_quant + kanida_chat loaded OK")
except ImportError as e:
    ENGINES_OK = False
    log.error(f"Engine import failed: {e}")

# ── DB path (same DB the quant engine uses) ───────────────────────────────────
DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    r"C:\Users\SPS\Desktop\KANIDA.AI_TERMINAL\data\db\kanida_fingerprints.db"
)

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


# ══════════════════════════════════════════════════════════════════════════════
# REQUEST / RESPONSE MODELS
# ══════════════════════════════════════════════════════════════════════════════

class ChatRequest(BaseModel):
    message:  str
    history:  list = []   # list of {role, content} dicts
    ticker:   Optional[str] = None   # pre-resolved ticker (optional)
    market:   Optional[str] = None
    intent:   Optional[str] = None   # pre-resolved intent (optional)


# ══════════════════════════════════════════════════════════════════════════════
# INTENT DETECTION (ported from kanida_bot.py)
# ══════════════════════════════════════════════════════════════════════════════

def detect_user_bias(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["sell","short","bearish","bear","downside",
                              "going down","fall","crash","negative"]):
        return "bearish"
    if any(k in t for k in ["buy","long","bullish","bull","upside",
                              "going up","rally"]):
        return "bullish"
    return "general"

def detect_market(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["us ","us market","us stock","american",
                              "nasdaq","nyse","dollar","usd"]):
        return "US"
    return "NSE"

SCREENER_SELL_KW = [
    "stocks to sell","sell now","short candidates","bearish stocks",
    "stocks falling","stocks going down","what to sell","give me bearish",
    "bearish stocks in","falling stocks",
]
SCREENER_BUY_KW = [
    "stocks to buy","what to buy","top stocks","best stocks",
    "give me stocks","which stocks","screener","stocks to invest",
    "highest win rate","best win rate","highest breakout","best breakout",
    "highest accuracy","most accurate","strongest edge","best edge",
    "which stock has","which nse stock","top nse","best nse",
    "show me bullish","give me bullish","bullish stocks","winning stocks",
    "which has the","what has the","find me stocks","recommend stocks",
]
STRATEGY_KW = [
    "strategies","strategy","which strategy","what strategy",
    "top strategy","best strategy","what worked","patterns",
    "top strategies","best strategies","highest annualized","most reliable",
]

def detect_intent(text: str) -> dict:
    t    = text.lower().strip()
    mkt  = detect_market(text)
    bias = detect_user_bias(text)

    if any(k in t for k in ["help","what can you","commands"]):
        return dict(intent="help")

    # Strategy query
    if any(k in t for k in STRATEGY_KW) and not Q.resolve_ticker(text)[0]:
        return dict(intent="top_strategies", tickers=[], market=mkt, bias=bias)

    # Multi-ticker
    tickers = Q.resolve_all_tickers(text) if ENGINES_OK else []
    if len(tickers) > 1:
        ticker, raw_market = Q.resolve_ticker(text)
        return dict(intent="ticker_analysis", tickers=tickers,
                    market=raw_market or mkt, bias=bias)

    raw_ticker, raw_market = Q.resolve_ticker(text) if ENGINES_OK else (None, mkt)

    # Screener — only if no specific ticker
    if not raw_ticker:
        if any(k in t for k in SCREENER_SELL_KW) or \
           (bias == "bearish" and any(k in t for k in ["stocks","give me","list","which","what"])):
            return dict(intent="screener", tickers=[], market=mkt, bias="bearish")
        if any(k in t for k in SCREENER_BUY_KW) or \
           (bias == "bullish" and any(k in t for k in ["stocks","give me","list","which","what"])):
            return dict(intent="screener", tickers=[], market=mkt, bias="bullish")
        if any(k in t for k in ["stocks","screener","scan"]):
            return dict(intent="screener", tickers=[], market=mkt,
                        bias=bias if bias != "general" else "bullish")

    # Single ticker
    if raw_ticker:
        return dict(intent="ticker_analysis", tickers=[raw_ticker],
                    market=raw_market or mkt, bias=bias)

    return dict(intent="unknown", tickers=[], market=mkt, bias=bias)


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/health")
def health():
    """Health check + DB stats + snapshot freshness."""
    if not ENGINES_OK:
        return {"status": "degraded", "error": "kanida_quant/kanida_chat not loaded"}
    try:
        conn = _conn()
        fingerprints = conn.execute("SELECT COUNT(*) FROM fingerprints").fetchone()[0]
        paper_trades = conn.execute("SELECT COUNT(*) FROM paper_ledger").fetchone()[0]
        tickers = conn.execute("SELECT COUNT(DISTINCT ticker) FROM fingerprints").fetchone()[0]
        snapshots = conn.execute("SELECT COUNT(*) FROM agent_signal_snapshots").fetchone()[0]
        snap_row = conn.execute("""
            SELECT MAX(snapshot_date) as latest_date, MAX(snapshot_time) as latest_time
            FROM agent_signal_snapshots
        """).fetchone()
        conn.close()

        latest_date = snap_row["latest_date"] if snap_row else None
        latest_time = snap_row["latest_time"] if snap_row else None
        today = datetime.today().strftime("%Y-%m-%d")
        snap_age_min = None
        if latest_time:
            try:
                age_s = (datetime.now() - datetime.strptime(latest_time, "%Y-%m-%d %H:%M:%S")).total_seconds()
                snap_age_min = round(age_s / 60, 1)
            except Exception:
                pass

        return {
            "status":               "ok",
            "db_path":              DB_PATH,
            "fingerprints":         fingerprints,
            "paper_trades":         paper_trades,
            "tickers":              tickers,
            "snapshots":            snapshots,
            "snapshot_date":        latest_date,
            "snapshot_age_minutes": snap_age_min,
            "snapshot_stale":       latest_date != today if latest_date else True,
            "snapshot_build_running": _snapshot_build_status["running"],
            "auto_refresh_enabled": _refresh_state["enabled"],
            "auto_refresh_interval_minutes": _refresh_state["interval_minutes"],
            "auto_refresh_next_at": _refresh_state["next_cycle_at"],
            "workers":              _refresh_state["workers"],
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.post("/chat")
def chat(req: ChatRequest):
    """
    Main chat endpoint. Routes message to correct quant function,
    builds prompt, calls Claude, returns response.

    Accepts pre-resolved ticker/intent from frontend (faster),
    or auto-detects from message text.
    """
    if not ENGINES_OK:
        raise HTTPException(503, "Quant engine not available")

    msg = req.message.strip()
    if not msg:
        raise HTTPException(400, "Empty message")

    # Use pre-resolved intent if provided by frontend
    if req.ticker and req.intent == "ticker_analysis":
        routed = dict(
            intent="ticker_analysis",
            tickers=[req.ticker],
            market=req.market or "NSE",
            bias=detect_user_bias(msg),
        )
    else:
        routed = detect_intent(msg)

    intent  = routed.get("intent", "unknown")
    tickers = routed.get("tickers", [])
    market  = routed.get("market", "NSE")
    bias    = routed.get("bias", "general")

    # Convert history format: [{role, content}] → [{role, content}]
    # kanida_chat.ask() expects list of {role, content} — already correct
    history = [
        {"role": m.get("role", "user"), "content": m.get("content", "")}
        for m in (req.history or [])
        if m.get("content")
    ]

    try:
        # ── Ticker analysis ──────────────────────────────────────────────────
        if intent == "ticker_analysis" and tickers:
            if len(tickers) == 1:
                analysis = Q.analyse_ticker(tickers[0], market, bias)
                prompt   = CH.ticker_prompt(analysis, msg)
                response = CH.ask(prompt, history)
                return {
                    "type":     "ticker",
                    "ticker":   tickers[0],
                    "market":   market,
                    "response": response,
                    "data": {
                        "exists":      analysis.exists_in_db,
                        "primary_bias": analysis.primary_bias,
                        "conviction":  analysis.overall_conviction,
                        "regime":      analysis.regime.regime if analysis.regime else None,
                        "regime_score": analysis.regime.regime_score if analysis.regime else None,
                        "win_rate":    analysis.bullish.avg_win_rate if analysis.bullish else None,
                        "total_signals": analysis.bullish.total_signals if analysis.bullish else None,
                    }
                }
            else:
                # Multi-ticker compare
                analyses = [Q.analyse_ticker(t, market, bias) for t in tickers]
                prompt   = CH.compare_prompt(analyses, msg)
                response = CH.ask(prompt, history)
                return {
                    "type":     "compare",
                    "tickers":  tickers,
                    "response": response,
                }

        # ── Screener ─────────────────────────────────────────────────────────
        elif intent == "screener":
            screen_bias = bias if bias in ("bullish","bearish") else "bullish"
            output   = Q.screen_stocks(market, screen_bias)
            prompt   = CH.screener_prompt(output, msg)
            response = CH.ask(prompt, history)
            return {
                "type":     "screener",
                "bias":     screen_bias,
                "market":   market,
                "count":    output.count,
                "response": response,
                "rows": [
                    {
                        "rank":       r.rank,
                        "ticker":     r.ticker,
                        "conviction": r.conviction,
                        "win_rate":   round(r.avg_win_rate * 100, 1),
                        "signals":    r.total_signals,
                        "avg_gain":   r.avg_gain_pct,
                        "regime":     r.regime,
                    }
                    for r in output.rows
                ],
            }

        # ── Top strategies ───────────────────────────────────────────────────
        elif intent == "top_strategies":
            strat_bias = bias if bias in ("bullish","bearish") else None
            items    = Q.top_strategies(market, strat_bias)
            prompt   = CH.top_strats_prompt(items, msg, market, strat_bias)
            response = CH.ask(prompt, history)
            return {
                "type":     "strategies",
                "market":   market,
                "response": response,
                "rows": [
                    {
                        "ticker":      s.ticker,
                        "pattern":     s.pattern_type,
                        "timeframe":   s.timeframe,
                        "bias":        s.bias,
                        "win_rate":    round(s.win_rate * 100, 1),
                        "avg_gain":    s.avg_gain,
                        "conviction":  s.conviction,
                        "occurrences": s.occurrences,
                    }
                    for s in items
                ],
            }

        # ── Help ─────────────────────────────────────────────────────────────
        elif intent == "help":
            return {
                "type": "help",
                "response": (
                    "Ask me about any NSE F&O or US options stock. Examples:\n\n"
                    "▶ RECLTD — what does history say?\n"
                    "▶ Show me bullish stocks in NSE\n"
                    "▶ What are the top breakout strategies?\n"
                    "▶ Compare SBIN and HDFCBANK\n\n"
                    "I run 129,000+ historical fingerprints across 215 stocks. "
                    "Historical data only — not financial advice."
                ),
            }

        # ── Unknown — fallback with screener context ────────────────────────
        else:
            try:
                screener_out = Q.screen_stocks(market, "bullish")
                top5 = screener_out.rows[:5] if screener_out.rows else []
                lines = [
                    f"{r.ticker}: {r.avg_win_rate*100:.0f}% win rate, +{r.avg_gain:.1f}% avg gain, {r.total_signals} signals"
                    for r in top5
                ]
                ctx = chr(10).join(lines)
                prompt = (
                    "User question: " + msg + chr(10) + chr(10) +
                    "Top " + market + " stocks by historical win rate:" + chr(10) + ctx + chr(10) + chr(10) +
                    "Answer the user directly using this data. Name specific tickers and numbers. "
                    "If asked about highest win rate, state the answer. Historical data only, not financial advice."
                )
                response = CH.ask(prompt, history)
            except Exception:
                response = CH.ask(
                    "User asked: " + msg + chr(10) +
                    "Answer helpfully using your knowledge of NSE F&O patterns.",
                    history
                )
            return {"type": "general", "response": response}

    except Exception as e:
        log.error(f"Chat error: {e}", exc_info=True)
        raise HTTPException(500, f"Engine error: {str(e)}")


# ── Narrative headline builder ────────────────────────────────────────────────
# Pattern category → readable phrase for headline
PATTERN_PHRASES = {
    "Breakout":            "breakout and momentum",
    "Moving Average":      "trend and moving average",
    "Volume Pattern":      "volume and accumulation",
    "Chart Pattern":       "base formation and chart pattern",
    "Momentum Indicator":  "momentum and oscillator",
    "Support/Resistance":  "support and resistance",
    "Reversal Candle":     "reversal candle and price action",
    "Trend":               "trend continuation",
    "Price Action":        "price action and structure",
    "Pullback":            "pullback and retracement",
    "VWAP":               "VWAP and intraday volume",
    "Candle Structure":    "candle structure and range",
    "Technical Setup":     "technical confluence",
}

def _pattern_phrase(patterns: list) -> str:
    """Return a readable phrase from the top 1-2 pattern categories."""
    if not patterns:
        return "multiple technical setups"
    top = patterns[:2]
    phrases = [PATTERN_PHRASES.get(p.get("category", ""), p.get("category", "").lower()) for p in top]
    if len(phrases) == 2:
        return f"{phrases[0]} and {phrases[1]}"
    return phrases[0]

def _signal_word(total_signals: int) -> str:
    """Signal depth adjective — drives confidence wording."""
    if total_signals >= 8000: return "repeated"
    if total_signals >= 3000: return "consistent"
    if total_signals >= 800:  return "emerging"
    return "early"

def _outcome_phrase(avg_outcome: float, bias: str) -> str:
    """Outcome strength phrase — only used when meaningful."""
    val = abs(avg_outcome)
    if val >= 12: return "with very strong historical follow-through"
    if val >= 7:  return "with strong historical follow-through"
    if val >= 4:  return "with solid historical follow-through"
    return "with moderate historical follow-through"

def _narrative(ticker: str, bias: str, regime: str, conviction: str,
               signal_type: str, patterns: list, wr: float,
               avg_outcome: float, total_signals: int) -> str:
    """
    Headline engine — final form.
    Rules: max ~15 words · behavior + context · no taxonomy
    Variables: signal depth · outcome strength · regime contrast · conviction
    """
    regime_upper  = regime.upper()
    contradiction = (
        (bias == "bullish" and regime_upper == "BEAR") or
        (bias == "bearish" and regime_upper in ("BULL", "OK"))
    )
    sig  = _signal_word(total_signals)
    out  = _outcome_phrase(avg_outcome, bias)
    mkt_weak   = "despite broader market weakness"
    mkt_strong = "despite broader market strength"

    # ── HIGH_EDGE ────────────────────────────────────────────────────────────
    if signal_type == "HIGH_EDGE":
        if bias == "bullish":
            if contradiction:
                if conviction == "HIGH":
                    return f"{ticker} is showing {sig} bullish setups {out} {mkt_weak}"
                return f"{ticker} is holding bullish structure {mkt_weak}"
            else:
                if conviction == "HIGH":
                    return f"{ticker} is generating {sig} bullish signals {out}"
                return f"{ticker} is building bullish momentum with moderate historical support"
        else:
            if contradiction:
                if conviction == "HIGH":
                    return f"{ticker} is showing {sig} bearish pressure {out} {mkt_strong}"
                return f"{ticker} continues to show bearish signals {mkt_strong}"
            else:
                if conviction == "HIGH":
                    return f"{ticker} is generating {sig} bearish signals {out}"
                return f"{ticker} is showing bearish pressure with moderate historical support"

    # ── BASE_FORMING ─────────────────────────────────────────────────────────
    elif signal_type == "BASE_FORMING":
        if bias == "bullish":
            if contradiction:
                return f"{ticker} is forming a base structure {mkt_weak}"
            if conviction == "HIGH":
                return f"{ticker} is forming a high-conviction base ahead of a potential move"
            return f"{ticker} is showing early base formation with moderate historical support"
        else:
            if conviction == "HIGH":
                return f"{ticker} is forming a distribution base with bearish resolution historically"
            return f"{ticker} is showing early distribution with bearish bias in a weak regime"

    # ── BREAKOUT_READY ───────────────────────────────────────────────────────
    elif signal_type == "BREAKOUT_READY":
        if bias == "bullish":
            if contradiction:
                return f"{ticker} is approaching breakout territory {mkt_weak}"
            if conviction == "HIGH":
                return f"{ticker} is building breakout pressure {out}"
            return f"{ticker} is approaching a high-probability breakout setup"
        else:
            return f"{ticker} is breaking down from resistance {out}"

    # ── VOLUME_DRY ───────────────────────────────────────────────────────────
    elif signal_type == "VOLUME_DRY":
        if bias == "bullish":
            return f"{ticker} is showing volume compression ahead of a potential bullish move"
        return f"{ticker} is showing volume dry-up with bearish resolution more likely"

    # ── PEAK_FALL ────────────────────────────────────────────────────────────
    elif signal_type == "PEAK_FALL":
        if bias == "bearish":
            if contradiction:
                return f"{ticker} continues to show weakness {mkt_strong}"
            if conviction == "HIGH":
                return f"{ticker} remains under sustained pressure after falling from peak levels"
            return f"{ticker} is still weak after a peak-fall with limited recovery evidence"
        else:
            return f"{ticker} is attempting recovery after a significant peak-fall setup"

    return f"{ticker} is showing a notable setup worth deeper analysis"


# ── Signal classification (pattern-category-driven) ───────────────────────────

def _classify_signal(bias: str, score: float, label: str, patterns: list,
                      win_rate: float = 0.0, total_signals: int = 0) -> str:
    """
    Classify signal type.
    Priority: bias → pattern category → win-rate tier → score/label fallback.
    Does NOT rely on score_label (all are "NO SIGNAL" in current DB scan run).
    """
    if bias == "bearish":
        return "PEAK_FALL"

    top_cats = [p.get("category", "") for p in patterns[:3]] if patterns else []
    top = top_cats[0] if top_cats else ""

    # Pattern-category-driven (most reliable when pattern data is present)
    if top == "Breakout":
        return "BREAKOUT_READY"
    if top == "Volume Pattern":
        return "VOLUME_DRY"
    if top in ("Chart Pattern", "Pullback"):
        return "BASE_FORMING"

    # Win-rate tier (works even when snapshots are missing)
    # HIGH conviction stocks (>82% WR) → breakout-quality edge
    if win_rate >= 0.82:
        return "BREAKOUT_READY"
    # Moderate WR with deep signal base → base forming
    if win_rate >= 0.72 and total_signals >= 1000:
        return "BASE_FORMING"
    # Low-moderate WR or thin data → volume dry / watchlist
    if win_rate >= 0.62:
        return "VOLUME_DRY"

    # Score/label fallback (when scanner has run and produced signals)
    label_up = label.upper()
    if "STRONG BUY" in label_up or score >= 75:
        return "BREAKOUT_READY"
    if "BUY" in label_up or score >= 60:
        return "BASE_FORMING"
    if "WATCHLIST" in label_up:
        return "VOLUME_DRY"

    # Secondary patterns in top_cats
    if "Momentum Indicator" in top_cats or "Support/Resistance" in top_cats:
        return "HIGH_EDGE"
    if "Moving Average" in top_cats or "Trend" in top_cats:
        return "BASE_FORMING"

    return "HIGH_EDGE"


def _trigger_reason(signal_type: str, patterns: list, wr: float, total_signals: int) -> str:
    """Human-readable trigger — why this specific signal was surfaced."""
    top_cat = patterns[0].get("category", "Technical Setup") if patterns else "Technical Setup"
    wr_pct  = round(wr * 100)
    sigs    = f"{total_signals:,}" if total_signals >= 1000 else str(total_signals)
    reasons = {
        "BREAKOUT_READY": f"{top_cat} patterns firing — {wr_pct}% win rate across {sigs} signals",
        "BASE_FORMING":   f"Base structure forming — {wr_pct}% historical resolution rate ({sigs} signals)",
        "VOLUME_DRY":     f"Volume compression active — {wr_pct}% win rate on volume expansion ({sigs} signals)",
        "PEAK_FALL":      f"Sustained weakness confirmed — {wr_pct}% of setups resolved lower ({sigs} signals)",
        "HIGH_EDGE":      f"{top_cat} setup — {wr_pct}% historical win rate ({sigs} signals)",
    }
    return reasons.get(signal_type, f"{wr_pct}% win rate · {sigs} signals")


def _suggested_prompts(ticker: str, signal_type: str) -> list:
    """3 context-aware prompts for the click-to-explore UX."""
    p = {
        "BREAKOUT_READY": [
            f"What's the win rate on breakout setups for {ticker}?",
            f"Show me performance when {ticker} broke out historically",
            f"What's the risk:reward on {ticker} at current levels?",
        ],
        "BASE_FORMING": [
            f"Which strategies worked most in {ticker} base formations?",
            f"How long do {ticker}'s bases typically last before resolving?",
            f"What's the risk:reward if {ticker} breaks out from here?",
        ],
        "VOLUME_DRY": [
            f"What happens to {ticker} after volume dry-up historically?",
            f"Which strategies fire when {ticker} volume compresses?",
            f"Show me {ticker}'s best setups during consolidation phases",
        ],
        "PEAK_FALL": [
            f"How deep has {ticker} fallen from its peak historically?",
            f"When does {ticker} typically recover after a peak-fall?",
            f"What's the current bearish edge strength for {ticker}?",
        ],
        "HIGH_EDGE": [
            f"What does history say about {ticker}?",
            f"Which strategies have the highest win rate for {ticker}?",
            f"Do you want to run a paper trade on {ticker}?",
        ],
    }
    return p.get(signal_type, p["HIGH_EDGE"])


@router.get("/feed")
def get_feed(market: str = "NSE", limit: int = 20):
    """
    Autonomous insight feed — narrative headlines, evidence in subline.
    Pulls from screener data (higher quality) then agent_signal_snapshots.
    Only surfaces MEDIUM and HIGH conviction signals.
    """
    if not ENGINES_OK:
        raise HTTPException(503, "Quant engine not available")
    try:
        # Pull from screener — higher quality win rates than raw snapshots
        screener_out = Q.screen_stocks(market, "bullish")
        bear_out     = Q.screen_stocks(market, "bearish")

        # Merge and tag
        all_rows = []
        for r in screener_out.rows:
            all_rows.append((r, "bullish"))
        for r in bear_out.rows:
            all_rows.append((r, "bearish"))

        # Also pull regime context from snapshots for each ticker
        conn = _conn()
        snap_map = {}
        snaps = conn.execute("""
            SELECT ticker, bias, score_pct, score_label, regime, regime_score,
                   hist_avg_outcome, live_price, firing_count, snapshot_date
            FROM agent_signal_snapshots
            WHERE market = ?
            ORDER BY snapshot_date DESC
        """, (market.upper(),)).fetchall()
        conn.close()
        for s in snaps:
            # Key by ticker only — price and regime are ticker-level data regardless of bias
            key = s["ticker"]
            if key not in snap_map:
                snap_map[key] = dict(s)

        # ── Pre-pass: build win-rate lookup for counter-trend detection ─────────
        # ticker -> {bias: (avg_win_rate_decimal, total_signals)}
        # Using screener results (already in memory, no extra DB query needed).
        _wr_lookup: dict = {}
        for (_r, _b) in all_rows:
            if _r.ticker not in _wr_lookup:
                _wr_lookup[_r.ticker] = {}
            _wr_lookup[_r.ticker][_b] = (float(_r.avg_win_rate), int(_r.total_signals))

        # Cap each bias independently so bearish can't be crowded out by bullish.
        # seen is keyed by (ticker, bias) — same stock may appear as both bull and bear.
        per_bias_limit = max(1, limit // 2)
        bull_count = 0
        bear_count = 0

        cards = []
        seen = set()
        for (r, bias) in all_rows:
            if (r.ticker, bias) in seen:
                continue
            # Respect per-bias cap so bullish rows don't consume all limit slots
            if bias == "bullish" and bull_count >= per_bias_limit:
                continue
            if bias == "bearish" and bear_count >= per_bias_limit:
                continue
            seen.add((r.ticker, bias))

            wr  = r.avg_win_rate        # already decimal from screener
            # Conviction filter — only surface MEDIUM and HIGH
            if r.conviction == "LOW":
                continue

            # Get snapshot context — keyed by ticker (price/regime are ticker-level)
            snap = snap_map.get(r.ticker, {})
            score   = float(snap.get("score_pct") or 50)
            # r.regime may be the string "UNKNOWN" (truthy) — ignore it and prefer snapshot value
            _r_regime = r.regime if (r.regime and r.regime.upper() not in ("UNKNOWN", "N/A", "")) else None
            regime  = (_r_regime or snap.get("regime") or "UNKNOWN").upper()
            reg_score = int(snap.get("regime_score") or 0)
            firing  = int(snap.get("firing_count") or 0)
            snap_date = snap.get("snapshot_date", "")

            # Pull top patterns first — needed for both signal classification and headline
            try:
                _fp = Q._bias_profile(r.ticker, bias)
                _patterns = [
                    {"category": p.category}
                    for p in (_fp.top_patterns[:2] if _fp else [])
                ]
            except Exception:
                _patterns = []

            # Signal type — pattern-category + win-rate driven (no score_label dependency)
            label = (snap.get("score_label") or "")
            signal_type = _classify_signal(
                bias, score, label, _patterns,
                win_rate=wr, total_signals=r.total_signals
            )

            # Directional avg outcome — flip sign for bearish so positive = bearish follow-through
            raw_outcome = float(snap.get("hist_avg_outcome") or r.avg_gain_pct or 0)
            if bias == "bearish":
                # For bearish: negative raw_outcome means price fell = good bearish outcome
                # Show as positive directional follow-through for user clarity
                directional_outcome = -raw_outcome if raw_outcome > 0 else abs(raw_outcome)
            else:
                directional_outcome = raw_outcome
            headline = _narrative(
                ticker=r.ticker, bias=bias, regime=regime,
                conviction=r.conviction, signal_type=signal_type,
                patterns=_patterns, wr=wr,
                avg_outcome=directional_outcome,
                total_signals=r.total_signals,
            )

            # Subline: evidence only
            subline = (
                f"{wr*100:.0f}% historical win rate · "
                f"{r.total_signals} signals · "
                f"{firing or r.qualified_setups} strategies · "
                f"{regime} regime"
            )

            # ── Counter-trend detection ───────────────────────────────────────
            # If the other side's historical win rate is higher than this side's,
            # today's signal is going against the dominant historical edge.
            _ct_map   = _wr_lookup.get(r.ticker, {})
            _alt_bias = "bearish" if bias == "bullish" else "bullish"
            _alt_wr, _alt_sig = _ct_map.get(_alt_bias, (0.0, 0))
            _is_ct     = bool(_alt_wr > 0 and _alt_wr > wr)
            _dom_bias  = _alt_bias if _is_ct else bias
            _dom_wr    = _alt_wr   if _is_ct else wr
            _dom_sig   = _alt_sig  if _is_ct else r.total_signals

            cards.append({
                "ticker":           r.ticker,
                "market":           market.upper(),
                "signalType":       signal_type,
                "headline":         headline,
                "subline":          subline,
                "triggerReason":    _trigger_reason(signal_type, _patterns, wr, r.total_signals),
                "suggestedPrompts": _suggested_prompts(r.ticker, signal_type),
                "conviction":       r.conviction,
                "edgeScore":        int(score),
                "price":            snap.get("live_price") or r.price,
                "regime":           regime,
                "regimeScore":      reg_score,
                "avgWinRate":       round(wr, 4),
                "avgOutcome":       round(directional_outcome, 2),
                "primaryBias":      bias,
                "totalSignals":     r.total_signals,
                "snapshotDate":     snap_date,
                "isCounterTrend":   _is_ct,
                "dominantBias":     _dom_bias,
                "dominantWinRate":  round(_dom_wr, 4),
                "dominantSignals":  int(_dom_sig),
            })

            if bias == "bullish":
                bull_count += 1
            else:
                bear_count += 1

            if len(cards) >= limit:
                break

        # Sort: HIGH conviction first, then by win rate
        cards.sort(key=lambda c: (0 if c["conviction"] == "HIGH" else 1, -c["avgWinRate"]))
        return {"market": market, "count": len(cards), "cards": cards}

    except Exception as e:
        log.error(f"Feed error: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@router.get("/feed/grouped")
def get_feed_grouped(market: str = "NSE"):
    """
    Feed grouped by signal type — powers the 4-section dashboard UI.
    Returns each active group with label, description, count, and top 4 cards.
    """
    result = get_feed(market=market, limit=40)
    all_cards = result.get("cards", [])

    GROUP_META = {
        "BREAKOUT_READY": {"label": "Breakout Ready",     "sub": "Volume expansion + multi-pattern confirmation",   "bull": True},
        "BASE_FORMING":   {"label": "Base Formation",     "sub": "Tight consolidation — watching for resolution",   "bull": True},
        "VOLUME_DRY":     {"label": "Volume Dry-Up",      "sub": "Compression active — potential energy building",  "bull": True},
        "PEAK_FALL":      {"label": "Fallen from Peak",   "sub": "Off highs with sustained bearish pressure",       "bull": False},
        "HIGH_EDGE":      {"label": "High Edge Setups",   "sub": "Strong historical edge across multiple patterns", "bull": True},
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
                "signalType": st,
                "label":      meta["label"],
                "sub":        meta["sub"],
                "bull":       meta["bull"],
                "count":      len(grp),
                "tickers":    [c["ticker"] for c in grp[:4]],
                "cards":      grp[:4],
            })

    return {"market": market, "groups": groups, "total": len(all_cards)}


@router.get("/screener")
def screener(market: str = "NSE", bias: str = "bullish"):
    """Screener endpoint — top stocks by market and bias."""
    if not ENGINES_OK:
        raise HTTPException(503, "Quant engine not available")
    if bias not in ("bullish", "bearish"):
        raise HTTPException(400, "bias must be 'bullish' or 'bearish'")
    try:
        output = Q.screen_stocks(market, bias)
        # Fetch snapshot prices and regimes (keyed by ticker)
        conn = _conn()
        _snaps = conn.execute("""
            SELECT ticker, live_price, regime
            FROM agent_signal_snapshots
            WHERE market = ? ORDER BY snapshot_date DESC
        """, (market.upper(),)).fetchall()
        conn.close()
        _price_map: dict = {}
        for s in _snaps:
            if s["ticker"] not in _price_map:
                _price_map[s["ticker"]] = {"price": s["live_price"], "regime": s["regime"]}
        return {
            "bias":   output.bias,
            "market": output.market,
            "count":  output.count,
            "rows": [
                {
                    "rank":       r.rank,
                    "ticker":     r.ticker,
                    "conviction": r.conviction,
                    "win_rate":   round(r.avg_win_rate * 100, 1),
                    "signals":    r.total_signals,
                    "avg_gain":   r.avg_gain_pct,
                    "regime":     _price_map.get(r.ticker, {}).get("regime") or r.regime or "UNKNOWN",
                    "price":      _price_map.get(r.ticker, {}).get("price") or r.price,
                }
                for r in output.rows
            ],
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/strategies")
def strategies(market: str = "NSE", bias: str = "bullish"):
    """
    Top strategy patterns by market + bias — no chat required.
    Powers the Strategy Intelligence tab directly from DB.
    """
    if not ENGINES_OK:
        raise HTTPException(503, "Quant engine not available")
    bias_param = bias if bias in ("bullish", "bearish") else None
    try:
        items = Q.top_strategies(market, bias_param)
        return {
            "market": market,
            "bias":   bias,
            "count":  len(items),
            "rows": [
                {
                    "ticker":      s.ticker,
                    "pattern":     s.pattern_type,
                    "timeframe":   s.timeframe,
                    "bias":        s.bias,
                    "win_rate":    round(s.win_rate * 100, 1),
                    "avg_gain":    s.avg_gain,
                    "conviction":  s.conviction,
                    "occurrences": s.occurrences,
                }
                for s in items
            ],
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/ticker/{ticker}")
def ticker_data(ticker: str, market: str = "NSE", intent: str = "general"):
    """
    Raw quant data for a ticker — no Claude, no chat.
    Used by the stock card page to show stats before the user types anything.
    """
    if not ENGINES_OK:
        raise HTTPException(503, "Quant engine not available")
    try:
        analysis = Q.analyse_ticker(ticker.upper(), market.upper(), intent)
        if not analysis.exists_in_db:
            raise HTTPException(404, f"{ticker} not found in KANIDA database")

        bull = analysis.bullish
        bear = analysis.bearish
        reg  = analysis.regime
        lvl  = analysis.levels

        # ── Active signal: what Bot 2 detected in the most recent snapshot ───────
        # Ordered by snapshot_date DESC then score_pct DESC so we always get the
        # freshest, strongest signal — the one that should drive the hero section.
        # timeframe, regime, regime_score are now included so the UI can show
        # exactly what the signal is based on without extra round-trips.
        try:
            _sc = _conn()
            _snap = _sc.execute("""
                SELECT bias, score_pct, score_label, firing_count, qualified_total,
                       top_strategy, top_win_rate, snapshot_time, snapshot_date,
                       timeframe, regime, regime_score
                FROM agent_signal_snapshots
                WHERE ticker = ? AND market = ?
                ORDER BY snapshot_date DESC, score_pct DESC
                LIMIT 1
            """, (ticker.upper(), market.upper())).fetchone()
            _sc.close()
            active_signal = dict(_snap) if _snap else None
        except Exception:
            active_signal = None

        # ── Paper ledger: avg gain on winning trades / avg loss on losing trades ─
        # Scoped to the active signal's bias + timeframe so the split reflects
        # exactly what a trader would have experienced on this specific setup.
        # paper_ledger has ~10k+ historical rows per ticker so this is meaningful.
        if active_signal:
            try:
                _lc    = _conn()
                _ab    = active_signal.get("bias")
                _at    = active_signal.get("timeframe")
                _lpms  = [ticker.upper(), market.upper()]
                _lcls  = ["ticker=?", "market=?", "source='historical'", "win IS NOT NULL"]
                if _ab: _lcls.append("bias=?");      _lpms.append(_ab)
                if _at: _lcls.append("timeframe=?"); _lpms.append(_at)
                _lrow = _lc.execute(
                    "SELECT AVG(CASE WHEN win=1 THEN outcome_pct END) as avg_win,"
                    "       AVG(CASE WHEN win=0 THEN outcome_pct END) as avg_loss,"
                    "       COUNT(*) as total"
                    f" FROM paper_ledger WHERE {' AND '.join(_lcls)}",
                    _lpms,
                ).fetchone()
                _lc.close()
                if _lrow and (_lrow["total"] or 0) > 0:
                    active_signal["avg_win"]  = round(_lrow["avg_win"],  2) if _lrow["avg_win"]  is not None else None
                    active_signal["avg_loss"] = round(_lrow["avg_loss"], 2) if _lrow["avg_loss"] is not None else None
                else:
                    active_signal["avg_win"]  = None
                    active_signal["avg_loss"] = None
            except Exception:
                active_signal["avg_win"]  = None
                active_signal["avg_loss"] = None

        # ── Signal freshness: consecutive trading days the signal has been active ─
        # Counts distinct snapshot dates for this ticker/bias/timeframe going back
        # from the latest, stopping when a gap > 7 calendar days is found.
        # Gaps on weekends and market holidays are absorbed (≤ 7 day gap tolerance).
        if active_signal:
            try:
                _fc   = _conn()
                _ab2  = active_signal.get("bias")
                _at2  = active_signal.get("timeframe")
                _rows = _fc.execute("""
                    SELECT DISTINCT snapshot_date
                    FROM   agent_signal_snapshots
                    WHERE  ticker=? AND market=? AND bias=? AND timeframe=?
                    AND    snapshot_date >= date('now', '-60 days')
                    ORDER  BY snapshot_date DESC
                """, (ticker.upper(), market.upper(), _ab2, _at2)).fetchall()
                _fc.close()
                _dates  = [r["snapshot_date"] for r in _rows]
                _streak = 0
                if _dates:
                    _streak = 1
                    for _i in range(1, len(_dates)):
                        _d1 = datetime.strptime(_dates[_i - 1], "%Y-%m-%d")
                        _d2 = datetime.strptime(_dates[_i],     "%Y-%m-%d")
                        if (_d1 - _d2).days <= 7:
                            _streak += 1
                        else:
                            break   # gap too large — streak ends here
                active_signal["signal_age_days"] = _streak
            except Exception:
                active_signal["signal_age_days"] = None

        # ── Historical context: dominant long-term side + counter-trend flag ─────
        # Dominant = whichever side has the higher historical avg win rate.
        # Counter-trend = today's active signal goes against that dominant side.
        _bull_wr  = round(bull.avg_win_rate * 100, 1) if bull else 0.0
        _bear_wr  = round(bear.avg_win_rate * 100, 1) if bear else 0.0
        _bull_sig = bull.total_signals if bull else 0
        _bear_sig = bear.total_signals if bear else 0

        if bull and bear:
            _dom_bias = "bullish" if _bull_wr >= _bear_wr else "bearish"
        elif bull:
            _dom_bias = "bullish"
        elif bear:
            _dom_bias = "bearish"
        else:
            _dom_bias = None

        _dom_wr  = _bull_wr  if _dom_bias == "bullish" else _bear_wr
        _dom_sig = _bull_sig if _dom_bias == "bullish" else _bear_sig
        _active_bias = active_signal.get("bias") if active_signal else None
        _is_ct = bool(_active_bias and _dom_bias and _active_bias != _dom_bias)

        historical_context = {
            "dominant_bias":          _dom_bias,
            "dominant_win_rate":      _dom_wr,
            "dominant_total_signals": _dom_sig,
            "is_counter_trend":       _is_ct,
        } if _dom_bias else None

        return {
            "ticker":      analysis.ticker,
            "market":      analysis.market,
            "primary_bias": analysis.primary_bias,
            "conviction":  analysis.overall_conviction,
            "regime": {
                "regime":       reg.regime       if reg else None,
                "regime_score": reg.regime_score if reg else None,
                "regime_label": reg.regime_label if reg else None,
                "snapshot_date": reg.snapshot_date if reg else None,
            } if reg else None,
            "bullish": {
                "conviction":   bull.conviction,
                "avg_win_rate": round(bull.avg_win_rate * 100, 1),
                "best_win_rate": round(bull.best_win_rate * 100, 1),
                "avg_gain":     bull.avg_gain_pct,
                "best_gain":    bull.best_gain_pct,
                "worst_loss":   bull.worst_loss_pct,
                "total_signals": bull.total_signals,
                "data_depth":   bull.data_depth,
                "patterns": [
                    {
                        "category":    p.category,
                        "win_rate":    round(p.win_rate * 100, 1),
                        "avg_gain":    p.avg_gain,
                        "occurrences": p.occurrences,
                    }
                    for p in bull.top_patterns
                ],
            } if bull else None,
            "bearish": {
                "conviction":   bear.conviction,
                "avg_win_rate": round(bear.avg_win_rate * 100, 1),
                "avg_gain":     bear.avg_gain_pct,
                "total_signals": bear.total_signals,
            } if bear else None,
            "levels": {
                "price":      lvl.current_price,
                "target_1":   lvl.target_1,
                "target_2":   lvl.target_2,
                "stop_loss":  lvl.stop_loss,
                "t1_pct":     lvl.target_1_pct,
                "t2_pct":     lvl.target_2_pct,
                "sl_pct":     lvl.stop_loss_pct,
                "levels_bias": lvl.levels_bias,
                "rr":         lvl.risk_reward_t1,
            } if lvl else None,
            "active_signal":      active_signal,
            "historical_context": historical_context,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/ticker/{ticker}/history")
def ticker_history(ticker: str, market: str = "NSE", limit: int = 5):
    """Recent paper_ledger entries for this ticker — powers the trade history panel."""
    try:
        conn = _conn()
        rows = conn.execute("""
            SELECT ticker, bias, strategy_name, signal_date, entry_price,
                   stop_loss, target_1, exit_date, exit_price, outcome_pct,
                   forward_15d_ret, status, win, logged_at
            FROM paper_ledger
            WHERE UPPER(ticker) = UPPER(?) AND UPPER(market) = UPPER(?)
            ORDER BY logged_at DESC
            LIMIT ?
        """, (ticker, market, limit)).fetchall()
        conn.close()
        return {"ticker": ticker.upper(), "rows": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(500, str(e))


class PaperTradeRequest(BaseModel):
    market: str = "NSE"


@router.post("/ticker/{ticker}/paper-trade")
def start_paper_trade(ticker: str, body: PaperTradeRequest):
    """Log a new paper trade to paper_ledger from current price levels."""
    if not ENGINES_OK:
        raise HTTPException(503, "Quant engine not available")
    try:
        analysis = Q.analyse_ticker(ticker.upper(), body.market.upper(), "general")
        if not analysis.exists_in_db:
            raise HTTPException(404, f"{ticker} not found")
        lvl = analysis.levels
        if not lvl or not lvl.current_price:
            raise HTTPException(422, "No price levels available for this ticker")
        conn = _conn()
        conn.execute("""
            INSERT INTO paper_ledger
              (ticker, market, bias, strategy_name, source, signal_date,
               entry_price, stop_loss, target_1, target_2, status, logged_at)
            VALUES (?,?,?,?,?,date('now'),?,?,?,?,'open',datetime('now'))
        """, (
            ticker.upper(), body.market.upper(),
            analysis.primary_bias, "Terminal Manual", "user",
            lvl.current_price, lvl.stop_loss, lvl.target_1, lvl.target_2,
        ))
        conn.commit()
        trade_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return {
            "status":       "ok",
            "trade_id":     trade_id,
            "ticker":       ticker.upper(),
            "entry_price":  lvl.current_price,
            "stop":         lvl.stop_loss,
            "target":       lvl.target_1,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Snapshot management ───────────────────────────────────────────────────────

_snapshot_build_lock  = threading.Lock()
_snapshot_build_status: dict = {"running": False, "last_result": None, "last_started": None}

# ── Auto-refresh config (all overridable via env vars) ────────────────────────
# KANIDA_AUTO_REFRESH              1|0          Enable/disable recurring refresh (default on)
# KANIDA_REFRESH_INTERVAL_MINUTES  int          How often to refresh (default 5)
# KANIDA_REFRESH_MARKETS           NSE|US|ALL   Comma-separated markets to refresh (default NSE)
# KANIDA_REFRESH_BIASES            ALL|bullish…  Biases to refresh (default ALL)
# KANIDA_SB_WORKERS                int          Parallel workers passed to snapshot builder
_AUTO_REFRESH_ENABLED = os.environ.get("KANIDA_AUTO_REFRESH", "1") == "1"
_REFRESH_INTERVAL_MIN = int(os.environ.get("KANIDA_REFRESH_INTERVAL_MINUTES", "5"))
_REFRESH_MARKETS      = [m.strip().upper() for m in os.environ.get("KANIDA_REFRESH_MARKETS", "NSE").split(",") if m.strip()]
_REFRESH_BIASES       = os.environ.get("KANIDA_REFRESH_BIASES", "ALL").strip()  # ALL | bullish | bearish | neutral
_SB_WORKERS           = int(os.environ.get("KANIDA_SB_WORKERS", "5"))

# Mutable refresh state — written by auto-refresh loop, read by status endpoints
_refresh_state: dict = {
    "enabled":         _AUTO_REFRESH_ENABLED,
    "interval_minutes": _REFRESH_INTERVAL_MIN,
    "markets":         _REFRESH_MARKETS,
    "biases":          _REFRESH_BIASES,
    "workers":         _SB_WORKERS,
    "cycle_count":     0,
    "last_cycle_at":   None,   # ISO timestamp
    "next_cycle_at":   None,   # ISO timestamp
    "last_cycle_result": None, # {built, errors, elapsed_s}
}


@router.get("/snapshot/status")
def snapshot_status():
    """Show snapshot freshness for all market/bias combinations."""
    try:
        conn = _conn()
        rows = conn.execute("""
            SELECT market, bias, MAX(snapshot_date) as latest_date,
                   MAX(snapshot_time) as latest_time,
                   COUNT(DISTINCT ticker) as stocks
            FROM agent_signal_snapshots
            GROUP BY market, bias
            ORDER BY market, bias
        """).fetchall()
        today = datetime.today().strftime("%Y-%m-%d")
        result = []
        for r in rows:
            lt = r["latest_time"]
            age_min = None
            if lt:
                try:
                    age_min = round((datetime.now() - datetime.strptime(lt, "%Y-%m-%d %H:%M:%S")).total_seconds() / 60, 1)
                except Exception:
                    pass
            result.append({
                "market":      r["market"],
                "bias":        r["bias"],
                "latest_date": r["latest_date"],
                "latest_time": lt,
                "stocks":      r["stocks"],
                "age_minutes": age_min,
                "stale":       r["latest_date"] != today if r["latest_date"] else True,
            })
        conn.close()
        return {
            "today":              today,
            "snapshots":          result,
            "build_running":      _snapshot_build_status["running"],
            "last_build_started": _snapshot_build_status["last_started"],
            "last_build_result":  _snapshot_build_status["last_result"],
            "auto_refresh": {
                "enabled":           _refresh_state["enabled"],
                "interval_minutes":  _refresh_state["interval_minutes"],
                "markets":           _refresh_state["markets"],
                "biases":            _refresh_state["biases"],
                "workers":           _refresh_state["workers"],
                "cycle_count":       _refresh_state["cycle_count"],
                "last_cycle_at":     _refresh_state["last_cycle_at"],
                "next_cycle_at":     _refresh_state["next_cycle_at"],
                "last_cycle_result": _refresh_state["last_cycle_result"],
            },
        }
    except Exception as e:
        raise HTTPException(500, str(e))


def _run_snapshot_build(market: str, bias: str, workers: Optional[int] = None):
    """
    Blocking snapshot build — call from a background thread only.
    Loads snapshot_builder fresh via importlib so env var changes take effect
    without restarting the API server.
    """
    import time as _t
    global _snapshot_build_status
    _w = workers if workers is not None else _SB_WORKERS
    _snapshot_build_status["running"]      = True
    _snapshot_build_status["last_started"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_start = _t.time()
    try:
        import importlib.util, pathlib
        spec = importlib.util.spec_from_file_location(
            "snapshot_builder",
            pathlib.Path(_AGENTS) / "snapshot_builder.py"
        )
        sb = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(sb)

        if market == "ALL":
            results = sb.build_all(workers=_w, db_path=DB_PATH)
        else:
            biases  = ["bullish", "bearish", "neutral"] if bias == "ALL" else [bias]
            results = [
                sb.build_snapshots(market=market, bias=b, workers=_w, db_path=DB_PATH)
                for b in biases
            ]

        if not isinstance(results, list):
            results = [results] if results else []

        total_built  = sum(r.get("built",  0) for r in results)
        total_errors = sum(r.get("errors", 0) for r in results)
        elapsed_s    = round(_t.time() - run_start, 1)
        _snapshot_build_status["last_result"] = {
            "status":      "success",
            "built":       total_built,
            "errors":      total_errors,
            "elapsed_s":   elapsed_s,
            "workers":     _w,
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        _snapshot_build_status["last_result"] = {
            "status":      "error",
            "error":       str(e),
            "elapsed_s":   round(_t.time() - run_start, 1),
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        log.error(f"[SnapshotBuild] Failed: {e}")
    finally:
        _snapshot_build_status["running"] = False


# ── Auto-refresh loop ────────────────────────────────────────────────────────

def _auto_refresh_loop():
    """
    Daemon thread — refreshes snapshots on a fixed cadence.

    Design:
      - Fires every KANIDA_REFRESH_INTERVAL_MINUTES (default 5).
      - Each cycle records its start time, runs _run_snapshot_build() (blocking),
        then sleeps the REMAINING time until the next scheduled tick.
        This means builds that finish faster than the interval sleep longer;
        builds that overrun fire the next cycle immediately (no backlog pileup).
      - If a manual build is already running when a tick fires, the cycle
        is logged and skipped — no overlap possible.
      - One market at a time; if KANIDA_REFRESH_MARKETS=NSE,US both are
        refreshed sequentially within the same cycle before sleeping.
    """
    import time as _t
    log.info(
        f"[AutoRefresh] Thread started — interval={_REFRESH_INTERVAL_MIN}m  "
        f"markets={_REFRESH_MARKETS}  biases={_REFRESH_BIASES}  workers={_SB_WORKERS}"
    )

    while True:
        # ── Schedule the NEXT tick before doing any work ──────────────────
        cycle_start   = _t.time()
        next_tick     = cycle_start + _REFRESH_INTERVAL_MIN * 60
        _refresh_state["next_cycle_at"] = (
            datetime.fromtimestamp(next_tick).strftime("%Y-%m-%d %H:%M:%S")
        )

        # ── Do the build synchronously in this thread ─────────────────────
        if _snapshot_build_status["running"]:
            log.info("[AutoRefresh] Tick skipped — a build is already running")
        else:
            _refresh_state["cycle_count"]   += 1
            _refresh_state["last_cycle_at"]  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log.info(
                f"[AutoRefresh] Cycle #{_refresh_state['cycle_count']} — "
                f"{_REFRESH_MARKETS} × {_REFRESH_BIASES}"
            )
            cycle_results = []
            for mkt in _REFRESH_MARKETS:
                _run_snapshot_build(mkt, _REFRESH_BIASES, workers=_SB_WORKERS)
                last = _snapshot_build_status.get("last_result") or {}
                cycle_results.append({**last, "market": mkt})

            _refresh_state["last_cycle_result"] = {
                "markets":   _REFRESH_MARKETS,
                "biases":    _REFRESH_BIASES,
                "results":   cycle_results,
                "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        # ── Sleep the remaining time until the next tick ──────────────────
        remaining = next_tick - _t.time()
        if remaining > 0:
            _t.sleep(remaining)


def start_auto_refresh():
    """
    Start the background snapshot refresh daemon.
    Call once from app startup. Safe to call multiple times — only starts once.
    """
    if not _AUTO_REFRESH_ENABLED:
        log.info("[AutoRefresh] Disabled — set KANIDA_AUTO_REFRESH=1 to enable")
        return
    t = threading.Thread(
        target=_auto_refresh_loop,
        daemon=True,
        name="kanida-auto-refresh",
    )
    t.start()
    log.info(
        f"[AutoRefresh] Started — interval={_REFRESH_INTERVAL_MIN}m  "
        f"workers={_SB_WORKERS}  markets={_REFRESH_MARKETS}"
    )


class SnapshotBuildRequest(BaseModel):
    market: str = "NSE"   # NSE | US | ALL
    bias:   str = "ALL"   # bullish | bearish | neutral | ALL


@router.post("/snapshot/build")
def trigger_snapshot_build(req: SnapshotBuildRequest):
    """
    Trigger a background snapshot refresh.
    Builds fresh agent_signal_snapshots for the given market/bias.
    Returns immediately — check /snapshot/status for progress.
    """
    if _snapshot_build_status["running"]:
        return {"status": "already_running", "started": _snapshot_build_status["last_started"]}

    if req.market not in ("NSE", "US", "ALL"):
        raise HTTPException(400, "market must be NSE, US, or ALL")
    if req.bias not in ("bullish", "bearish", "neutral", "ALL"):
        raise HTTPException(400, "bias must be bullish, bearish, neutral, or ALL")

    t = threading.Thread(
        target=_run_snapshot_build,
        args=(req.market, req.bias),
        daemon=True,
    )
    t.start()
    return {
        "status":  "started",
        "market":  req.market,
        "bias":    req.bias,
        "message": "Snapshot build running in background. Check /api/snapshot/status for progress.",
    }
