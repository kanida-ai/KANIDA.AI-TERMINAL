"""
KANIDA.AI — Quant Engine  (kanida_quant.py)
============================================
Pure data layer. No Claude, no display, no IP leakage.
Strategy names abstracted to pattern categories before leaving this module.
"""

import os, re, math, sqlite3
from dataclasses import dataclass, field
from typing import Optional, List

# DB path: env var set by main.py → fallback to the canonical path with full data
DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    r"C:\Users\SPS\Desktop\BG for retail Traders\kanida_fingerprints.db"
)

# ══════════════════════════════════════════════════════════════════════════════
# DATACLASSES — clean output objects. Chat engine only sees these.
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class PatternEntry:
    """One abstracted strategy category with aggregated stats."""
    category:    str    # e.g. "Breakout", "Moving Average", "Volume Pattern"
    win_rate:    float  # avg WR across strategies in this category (0-1)
    avg_gain:    float  # weighted avg forward return %
    best_gain:   float  # best single outcome %
    worst_loss:  float  # worst single outcome %
    occurrences: int    # total historical signals
    count:       int    # number of strategies in category

@dataclass
class BiasProfile:
    bias:             str
    conviction:       str       # HIGH / MEDIUM / LOW / NONE
    qualified_setups: int       # total qualified strategies
    avg_win_rate:     float     # 0-1
    best_win_rate:    float
    avg_gain_pct:     float     # weighted avg forward return
    best_gain_pct:    float
    worst_loss_pct:   float
    total_signals:    int
    backtest_years:   int
    supporting_count: int       # strategies with WR >= 75%
    data_depth:       str       # DEEP / MODERATE / THIN
    top_patterns:     List[PatternEntry] = field(default_factory=list)  # all categories sorted by WR

@dataclass
class RegimeContext:
    regime:           str   # BULL / BEAR / UNKNOWN
    regime_score:     int   # 0-100
    regime_label:     str
    snapshot_date:    str
    snapshot_bias:    str   # which bias the scanner ran
    signal_label:     str
    signal_score:     float
    strategies_firing:int
    strategies_total: int
    hist_win_rate:    float
    hist_avg_outcome: float

@dataclass
class PriceLevels:
    current_price:   Optional[float]
    price_source:    str
    levels_bias:     str   # "bullish" or "bearish" — clarifies direction of T1/T2/SL
    target_1:        Optional[float]
    target_2:        Optional[float]
    stop_loss:       Optional[float]
    target_1_pct:    Optional[float]
    target_2_pct:    Optional[float]
    stop_loss_pct:   Optional[float]
    risk_reward_t1:  Optional[float]

@dataclass
class TickerAnalysis:
    ticker:           str
    market:           str
    user_intent:      str   # bullish / bearish / general
    exists_in_db:     bool
    bullish:          Optional[BiasProfile] = None
    bearish:          Optional[BiasProfile] = None
    regime:           Optional[RegimeContext] = None
    levels:           Optional[PriceLevels] = None
    primary_bias:     str = ""
    overall_conviction: str = ""
    quant_note:       str = ""

@dataclass
class ScreenerRow:
    rank:             int
    ticker:           str
    price:            Optional[float]
    regime:           str
    conviction:       str
    avg_win_rate:     float
    best_win_rate:    float
    avg_gain_pct:     float
    qualified_setups: int
    total_signals:    int
    data_depth:       str

@dataclass
class ScreenerOutput:
    bias:   str
    market: str
    count:  int
    rows:   List[ScreenerRow] = field(default_factory=list)

@dataclass
class TopStrategyRow:
    ticker:       str
    pattern_type: str
    timeframe:    str
    bias:         str
    occurrences:  int
    win_rate:     float
    avg_gain:     float
    conviction:   str

# ══════════════════════════════════════════════════════════════════════════════
# DB
# ══════════════════════════════════════════════════════════════════════════════

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def _f(v) -> float:
    if v is None: return 0.0
    try:
        f = float(v)
        return 0.0 if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return 0.0

# ══════════════════════════════════════════════════════════════════════════════
# PATTERN ABSTRACTION — raw strategy names never leave this function
# ══════════════════════════════════════════════════════════════════════════════

_CATS = [
    (r"sma|ema|ma |moving.average|golden.cross|death.cross|slope|stack|ribbon|crossover|200|50 |20 |sma\d|ema\d", "Moving Average"),
    (r"volume|vol |vol_|selling.pressure|buying.pressure|dry|accumulation|distribution|vol.price|vol.confirm", "Volume Pattern"),
    (r"breakout|break |triangle|range.break|flag|ascending|52w|52.week|new.high|momentum.break|range.expansion", "Breakout"),
    (r"reversal|doji|harami|engulf|soldier|crow|morning.star|evening.star|pinbar|outside.day|inside.day", "Reversal Candle"),
    (r"support|resistance|demand.zone|supply.zone|retest|bounce|rejection|cluster|pivot", "Support/Resistance"),
    (r"rsi|macd|stoch|momentum|overbought|oversold|divergence|histogram|oscillator|mfi", "Momentum Indicator"),
    (r"vwap|volume.weight", "VWAP"),
    (r"cup|handle|double.bottom|double.top|head.shoulder|base|vcp|coil|compress|squeeze|narrow|atr|bollinger|spring|volatility", "Chart Pattern"),
    (r"consecutive|multiple|series|expansion|thrust|power|candle.expansion|price.action|bar", "Price Action"),
    (r"high.open|low.open|close.near|body.to|range|spread|wick|tail|gap|open.equal|close.vs|midpoint", "Candle Structure"),
    (r"pullback|retracement|dip|retrace|ema.*pull|sma.*pull|pullback", "Pullback"),
    (r"trend|higher.high|higher.low|lower.high|lower.low|rising|falling|continuation|uptrend|downtrend", "Trend"),
    (r"sector|relative.strength|rs |ratio", "Relative Strength"),
]

def _abstract(name: str) -> str:
    s = name.lower()
    for pattern, cat in _CATS:
        if re.search(pattern, s):
            return cat
    return "Technical Setup"

# ══════════════════════════════════════════════════════════════════════════════
# SCORING
# ══════════════════════════════════════════════════════════════════════════════

def _conviction(avg_wr: float, total: int) -> str:
    if avg_wr >= 0.82 and total >= 60: return "HIGH"
    if avg_wr >= 0.75 and total >= 25: return "HIGH"
    if avg_wr >= 0.68 and total >= 10: return "MEDIUM"
    if avg_wr >= 0.58:                  return "MEDIUM"
    return "LOW"

def _depth(total: int) -> str:
    if total >= 100: return "DEEP"
    if total >= 30:  return "MODERATE"
    return "THIN"

def _regime_label(regime: str, score: int) -> str:
    r = regime.upper()
    if r == "BEAR":
        return "strongly bearish — confirmed downtrend" if score <= 25 else "bearish — market under pressure"
    if r in ("OK","BULL"):
        return "strongly bullish — uptrend confirmed" if score >= 70 else "supportive — conditions favourable"
    return "transitional — direction unclear, wait for confirmation"

# ══════════════════════════════════════════════════════════════════════════════
# CORE COMPUTATIONS
# ══════════════════════════════════════════════════════════════════════════════

def _bias_profile(ticker: str, bias: str) -> Optional[BiasProfile]:
    conn = _conn()
    rows = conn.execute("""
        SELECT strategy_name, timeframe, appearances, wins,
               win_rate, avg_forward, best_case, worst_case, backtest_years
        FROM fingerprints
        WHERE ticker=? AND bias=? AND qualifies=1 AND appearances >= 4
        ORDER BY win_rate DESC, appearances DESC
        LIMIT 25
    """, (ticker.upper(), bias)).fetchall()
    conn.close()
    if not rows: return None
    rows = [dict(r) for r in rows]

    total   = sum(r["appearances"] for r in rows)
    avg_wr  = sum(_f(r["win_rate"]) for r in rows) / len(rows)
    best_wr = max(_f(r["win_rate"]) for r in rows)
    wavg    = sum(_f(r["avg_forward"]) * r["appearances"] for r in rows) / max(total, 1)
    best_g  = max(_f(r["best_case"]) for r in rows)
    worst_g = min(_f(r["worst_case"]) for r in rows)
    years   = max(r["backtest_years"] or 0 for r in rows)

    # Aggregate by abstracted category — gives Claude real strategy depth
    cat_map: dict = {}
    for r in rows:
        cat = _abstract(r["strategy_name"])
        wr  = _f(r["win_rate"])
        avg = _f(r["avg_forward"])
        bc  = _f(r["best_case"])
        wc  = _f(r["worst_case"])
        app = r["appearances"]
        if cat not in cat_map:
            cat_map[cat] = {"wrs": [], "wavg_num": 0.0, "apps": 0,
                            "bests": [], "worsts": [], "count": 0}
        cat_map[cat]["wrs"].append(wr)
        cat_map[cat]["wavg_num"] += avg * app
        cat_map[cat]["apps"]     += app
        cat_map[cat]["bests"].append(bc)
        cat_map[cat]["worsts"].append(wc)
        cat_map[cat]["count"]    += 1

    top_patterns = []
    for cat, v in cat_map.items():
        top_patterns.append(PatternEntry(
            category    = cat,
            win_rate    = round(sum(v["wrs"]) / len(v["wrs"]), 4),
            avg_gain    = round(v["wavg_num"] / max(v["apps"], 1), 2),
            best_gain   = round(max(v["bests"]), 2),
            worst_loss  = round(min(v["worsts"]), 2),
            occurrences = v["apps"],
            count       = v["count"],
        ))
    top_patterns.sort(key=lambda x: (x.win_rate, x.occurrences), reverse=True)

    return BiasProfile(
        bias             = bias,
        conviction       = _conviction(avg_wr, total),
        qualified_setups = len(rows),
        avg_win_rate     = round(avg_wr, 4),
        best_win_rate    = round(best_wr, 4),
        avg_gain_pct     = round(wavg, 2),
        best_gain_pct    = round(best_g, 2),
        worst_loss_pct   = round(worst_g, 2),
        total_signals    = total,
        backtest_years   = years,
        supporting_count = sum(1 for r in rows if _f(r["win_rate"]) >= 0.75),
        data_depth       = _depth(total),
        top_patterns     = top_patterns,
    )

def _regime(ticker: str, prefer_bias: str) -> Optional[RegimeContext]:
    """Read regime from bias-matched snapshot. Falls back to any snapshot."""
    conn = _conn()
    row = conn.execute("""
        SELECT bias, snapshot_date, score_pct, score_label,
               firing_count, qualified_total, regime, regime_score,
               hist_win_pct, hist_avg_outcome
        FROM agent_signal_snapshots
        WHERE ticker=? AND bias=?
        ORDER BY snapshot_date DESC LIMIT 1
    """, (ticker.upper(), prefer_bias)).fetchone()

    if not row:
        row = conn.execute("""
            SELECT bias, snapshot_date, score_pct, score_label,
                   firing_count, qualified_total, regime, regime_score,
                   hist_win_pct, hist_avg_outcome
            FROM agent_signal_snapshots
            WHERE ticker=?
            ORDER BY snapshot_date DESC LIMIT 1
        """, (ticker.upper(),)).fetchone()
    conn.close()
    if not row: return None

    reg   = (row["regime"] or "UNKNOWN").upper()
    score = int(_f(row["regime_score"]))
    fire  = int(row["firing_count"] or 0)
    total = int(row["qualified_total"] or 1)

    return RegimeContext(
        regime            = reg,
        regime_score      = score,
        regime_label      = _regime_label(reg, score),
        snapshot_date     = row["snapshot_date"] or "",
        snapshot_bias     = row["bias"] or "",
        signal_label      = row["score_label"] or "NO SIGNAL",
        signal_score      = _f(row["score_pct"]),
        strategies_firing = fire,
        strategies_total  = total,
        hist_win_rate     = _f(row["hist_win_pct"]),
        hist_avg_outcome  = _f(row["hist_avg_outcome"]),
    )

def _levels(ticker: str, intent: str) -> PriceLevels:
    """Pick snapshot matching intent so targets are never directionally inverted."""
    snap_bias = "bearish" if intent == "bearish" else "bullish"
    conn = _conn()
    row = conn.execute("""
        SELECT live_price, target_1, target_2, stop_loss, snapshot_date, bias
        FROM agent_signal_snapshots
        WHERE ticker=? AND bias=?
        ORDER BY snapshot_date DESC LIMIT 1
    """, (ticker.upper(), snap_bias)).fetchone()

    if not row:
        row = conn.execute("""
            SELECT live_price, target_1, target_2, stop_loss, snapshot_date, bias
            FROM agent_signal_snapshots WHERE ticker=?
            ORDER BY snapshot_date DESC LIMIT 1
        """, (ticker.upper(),)).fetchone()
    conn.close()

    price = _f(row["live_price"]) if row else 0
    t1    = _f(row["target_1"])   if row else 0
    t2    = _f(row["target_2"])   if row else 0
    sl    = _f(row["stop_loss"])  if row else 0
    src   = f"{snap_bias} snapshot {row['snapshot_date']}" if row else "unavailable"
    lbias = row["bias"] if row else snap_bias

    if not price:
        try:
            import yfinance as yf
            hist = yf.Ticker(ticker + ".NS").history(period="1d")
            if not hist.empty:
                price = float(hist["Close"].iloc[-1])
                src   = "live"
        except Exception:
            pass

    def pct(v):
        return round((v - price) / price * 100, 1) if price and v and price > 0 else None

    rr = None
    if price and t1 and sl and abs(price - sl) > 0:
        rr = round(abs(t1 - price) / abs(price - sl), 2)

    return PriceLevels(
        current_price  = round(price, 2) if price else None,
        price_source   = src,
        levels_bias    = lbias,
        target_1       = round(t1, 2) if t1 else None,
        target_2       = round(t2, 2) if t2 else None,
        stop_loss      = round(sl, 2) if sl else None,
        target_1_pct   = pct(t1),
        target_2_pct   = pct(t2),
        stop_loss_pct  = pct(sl),
        risk_reward_t1 = rr,
    )

def _primary(bull, bear, regime, intent) -> tuple:
    if intent == "bearish":
        c    = bear.conviction if bear else "NONE"
        note = f"Bearish intent | {bear.avg_win_rate*100:.0f}% avg WR, {bear.total_signals} signals" if bear else "No bearish data in DB"
        return "bearish", c, note
    if intent == "bullish":
        c    = bull.conviction if bull else "NONE"
        note = f"Bullish intent | {bull.avg_win_rate*100:.0f}% avg WR, {bull.total_signals} signals" if bull else "No bullish data in DB"
        return "bullish", c, note

    bs = (bull.avg_win_rate * bull.total_signals) if bull else 0
    rs = (bear.avg_win_rate * bear.total_signals) if bear else 0
    reg = (regime.regime if regime else "UNKNOWN").upper()
    if reg == "BEAR":   rs *= 1.3
    elif reg in ("OK","BULL"): bs *= 1.3

    primary = "bullish" if bs >= rs else "bearish"
    p       = bull if primary == "bullish" else bear
    rl      = regime.regime_label if regime else "unknown regime"
    note    = (f"{primary.capitalize()} edge | {p.avg_win_rate*100:.0f}% avg WR, "
               f"{p.total_signals} signals | Regime: {rl}" if p else "Insufficient data")
    return primary, (p.conviction if p else "NONE"), note

# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def analyse_ticker(ticker: str, market: str, user_intent: str) -> TickerAnalysis:
    conn = _conn()
    exists = conn.execute(
        "SELECT COUNT(*) FROM fingerprints WHERE ticker=?", (ticker.upper(),)
    ).fetchone()[0] > 0
    conn.close()

    if not exists:
        return TickerAnalysis(
            ticker=ticker, market=market,
            user_intent=user_intent, exists_in_db=False,
        )

    bull   = _bias_profile(ticker, "bullish")
    bear   = _bias_profile(ticker, "bearish")
    reg_b  = "bearish" if user_intent == "bearish" else "bullish"
    regime = _regime(ticker, reg_b)
    levels = _levels(ticker, user_intent)
    pri, conv, note = _primary(bull, bear, regime, user_intent)

    return TickerAnalysis(
        ticker=ticker, market=market, user_intent=user_intent,
        exists_in_db=True, bullish=bull, bearish=bear,
        regime=regime, levels=levels,
        primary_bias=pri, overall_conviction=conv, quant_note=note,
    )

def screen_stocks(market: str, bias: str) -> ScreenerOutput:
    conn = _conn()
    rows = conn.execute("""
        SELECT f.ticker,
               AVG(f.win_rate)                                         AS avg_wr,
               MAX(f.win_rate)                                         AS best_wr,
               SUM(f.avg_forward*f.appearances)/SUM(f.appearances)     AS wavg,
               COUNT(*)                                                 AS n_s,
               SUM(f.appearances)                                       AS total_app,
               s.live_price, s.regime
        FROM fingerprints f
        LEFT JOIN (
            SELECT ticker, live_price, regime,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY snapshot_date DESC) rn
            FROM agent_signal_snapshots WHERE bias=?
        ) s ON f.ticker=s.ticker AND s.rn=1
        WHERE f.market=? AND f.bias=? AND f.qualifies=1
              AND f.appearances >= 6 AND f.win_rate >= 0.60
        GROUP BY f.ticker HAVING n_s >= 3
        ORDER BY avg_wr DESC, total_app DESC LIMIT 20
    """, (bias, market.upper(), bias)).fetchall()
    conn.close()

    result = []
    for i, r in enumerate(rows, 1):
        avg_wr = _f(r["avg_wr"])
        total  = int(r["total_app"] or 0)
        price  = _f(r["live_price"]) or None
        result.append(ScreenerRow(
            rank=i, ticker=r["ticker"],
            price=round(price, 0) if price else None,
            regime=(r["regime"] or "UNKNOWN").upper(),
            conviction=_conviction(avg_wr, total),
            avg_win_rate=round(avg_wr, 4),
            best_win_rate=round(_f(r["best_wr"]), 4),
            avg_gain_pct=round(_f(r["wavg"]), 2),
            qualified_setups=int(r["n_s"] or 0),
            total_signals=total,
            data_depth=_depth(total),
        ))
    return ScreenerOutput(bias=bias, market=market, count=len(result), rows=result)

def top_strategies(market: str, bias: Optional[str] = None) -> List[TopStrategyRow]:
    conn = _conn()
    params = [market.upper(), 10]
    sql = """
        SELECT ticker, strategy_name, bias, timeframe, appearances, win_rate, avg_forward
        FROM fingerprints
        WHERE market=? AND qualifies=1 AND appearances >= ?
    """
    if bias:
        sql += " AND bias=?"; params.append(bias)
    sql += " ORDER BY win_rate DESC, appearances DESC LIMIT 15"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [
        TopStrategyRow(
            ticker=r["ticker"], pattern_type=_abstract(r["strategy_name"]),
            timeframe=r["timeframe"] or "", bias=r["bias"] or "",
            occurrences=r["appearances"], win_rate=round(_f(r["win_rate"]), 4),
            avg_gain=round(_f(r["avg_forward"]), 2),
            conviction=_conviction(_f(r["win_rate"]), r["appearances"]),
        )
        for r in rows
    ]

# ══════════════════════════════════════════════════════════════════════════════
# TICKER RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════

_ALL_TICKERS = None

def load_all_tickers() -> dict:
    global _ALL_TICKERS
    if _ALL_TICKERS is None:
        conn = _conn()
        rows = conn.execute("SELECT DISTINCT ticker, market FROM fingerprints").fetchall()
        conn.close()
        _ALL_TICKERS = {r["ticker"]: r["market"] for r in rows}
    return _ALL_TICKERS

_STOP = {
    "A","AN","THE","AND","OR","FOR","IN","OF","ON","AT","TO","DO","IS","IT","MY",
    "ME","CAN","GET","NOW","ALL","TOP","NEW","LOW","HIGH","THIS","THAT","WITH",
    "FROM","WAS","HAS","NOT","NO","ANY","JUST","SOME","LIKE","MOST","EACH","WHEN",
    "WHERE","HAVE","WANT","NEED","TRADE","TRADING","STOCK","STOCKS","MARKET","PRICE",
    "DATA","SIGNAL","PATTERN","STRATEGY","SETUP","TODAY","STRATEGIES","ANALYSIS",
    "HISTORY","GIVE","SHOW","TELL","WHAT","WHICH","HOW","TIME","WILL","GOING","UP",
    "DOWN","FALL","RISE","GOOD","BAD","NEXT","LAST","YEAR","WEEK","BEST","LIST",
    "HELP","CMP","LTP","RATE","VIEW","THINK","ABOUT","FIND","AM","I","SHOULD",
    "PLEASE","COULD","NSE","BSE","BUY","SELL","SHORT","LONG","BULL","BEAR",
    "BULLISH","BEARISH","ALSO","THEN","THAN","BEEN","WELL","BOTH","ONLY","SAME",
    "AFTER","ABOVE","WOULD","GENERAL","COMPARE","VERSUS","VS","US","CONSISTENTLY",
    "WORKED","WORKING","WHICH","ALL","EVERY","GIVE","ME","SHOW","TELL","USED",
}

_COMPOUND = {
    "HDFCBANK":  ["HDFC","BANK"],
    "ICICIBANK": ["ICICI","BANK"],
    "KOTAKBANK": ["KOTAK","BANK"],
    "AXISBANK":  ["AXIS","BANK"],
    "BAJAJFINSV":["BAJAJ","FINSV"],
    "BAJFINANCE":["BAJ","FINANCE"],
    "SBICARD":   ["SBI","CARD"],
    "SBILIFE":   ["SBI","LIFE"],
    "BAJAJ-AUTO":["BAJAJ","AUTO"],
    "M&M":       ["M&M"],
}

def resolve_ticker(text: str) -> tuple:
    """Returns (ticker_or_None, market)."""
    tickers = load_all_tickers()
    words   = text.upper().split()
    for w in words:
        c = re.sub(r"[^A-Z0-9\-&]", "", w)
        if c in tickers and c not in _STOP:
            return c, tickers[c]
    for comp, parts in _COMPOUND.items():
        if all(p in text.upper() for p in parts) and comp in tickers:
            return comp, tickers[comp]
    for w in words:
        c = re.sub(r"[^A-Z0-9\-&]", "", w)
        if re.match(r"^[A-Z]{2,8}$", c) and c not in _STOP and c in tickers:
            return c, tickers[c]
    return None, "NSE"

def resolve_all_tickers(text: str) -> list:
    """Extract multiple tickers from text like 'SBIN, RELIANCE'."""
    tickers = load_all_tickers()
    found   = []
    for part in re.split(r"[,\s]+", text.upper()):
        c = re.sub(r"[^A-Z0-9\-&]", "", part)
        if c in tickers and c not in _STOP and c not in found:
            found.append(c)
    for comp, parts in _COMPOUND.items():
        if all(p in text.upper() for p in parts) and comp in tickers and comp not in found:
            found.append(comp)
    return found
