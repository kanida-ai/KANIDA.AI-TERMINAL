"""
KANIDA.AI — Chat Engine  (kanida_chat.py)
==========================================
Receives abstracted quant output. Formats prompt. Calls Claude.
Never touches DB. Never exposes raw strategy names.
"""

import os
from typing import Optional, List
from kanida_quant import (
    TickerAnalysis, ScreenerOutput, ScreenerRow,
    BiasProfile, RegimeContext, PriceLevels,
    TopStrategyRow, PatternEntry,
)

# Never cache client at import time — key may not be in env yet.
try:
    import anthropic as _anth
    HAS_CLAUDE = True
except ImportError:
    _anth      = None
    HAS_CLAUDE = False

MODEL      = "claude-opus-4-6"
MAX_TOKENS = 1100

def _client():
    """Fresh client every call — reads key from environment at call time."""
    if not HAS_CLAUDE: return None
    return _anth.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

# ══════════════════════════════════════════════════════════════════════════════
# SYSTEM PROMPT
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM = """You are Kanida — market intelligence engine for KANIDA.AI.

You receive structured quant output pre-computed from 129,000+ historical pattern
fingerprints across 215 NSE F&O and US options stocks.

━━━ MANDATORY FORMAT — every response, no exceptions ━━━
1. FIRST SENTENCE = the single most important insight from the data.
   No greeting. No "great question". No preamble. Just the insight.
2. Structure: Key finding → Supporting numbers → Current context → Risk → Watch for
3. Under 350 words.
4. Every number you write MUST exist in the data given to you. Never invent.
5. End with exactly 2 follow-up prompts the user can actually ask about THIS stock.
   Format: ▶ "question text"
   Only suggest questions this data can answer — strategy performance, levels, regime.
6. Final line always: "Historical data only — not financial advice."

━━━ PATTERN BREAKDOWN DATA ━━━
The data includes "Pattern breakdown" listing abstracted strategy categories:
  e.g. Breakout, Moving Average, Volume Pattern, Chart Pattern, Candle Structure...
Each entry shows: WR%, avg gain, best/worst outcome, signal count.

When user asks "which strategies worked", "what worked consistently",
"give me all strategies", or "show me strategies" — LIST EVERY CATEGORY from the
pattern breakdown with its WR% and avg gain. Do not summarise. Do not say
"I don't have strategy names." The categories ARE the strategies, abstracted
for IP protection. Give the full list.

━━━ ABSOLUTE PROHIBITIONS ━━━
✗ Never open with: "Great question" / "Sure" / "Absolutely" / "Of course" /
  "I see" / "Interesting" / "Happy to" / "Let me" / any variant of these
✗ Never say "I don't have" or "I only have data for [ticker]"
✗ Never say "What I don't have" or "not in this snapshot" unless the specific
  field is truly absent from the data given
✗ Never repeat regime context more than once
✗ Never suggest follow-up prompts that require data not in this prompt
  (e.g. don't suggest "which other stocks have X" unless screener data is provided)
✗ Never use a table unless comparing 2+ stocks side by side

━━━ CONVICTION INTERPRETATION ━━━
HIGH   = speak confidently, the statistics are robust
MEDIUM = measured confidence, decent data
LOW    = flag uncertainty clearly
NONE   = no data, say so in one sentence and move on

━━━ REGIME INTERPRETATION ━━━
BEAR (0–40): bearish bias has statistical edge, bulls face headwinds
UNKNOWN (50): market at inflection, wait for directional confirmation  
BULL (60–100): bullish bias has edge, bears face headwinds

━━━ PRICE LEVELS ━━━
The levels_bias field tells you which direction the levels apply:
- levels_bias=bullish: Target 1 & 2 are ABOVE current price (upside targets),
  Stop Loss is BELOW current price (downside protection)
- levels_bias=bearish: Target 1 & 2 are BELOW current price (downside targets),
  Stop Loss is ABOVE current price (invalidation level)
Always state this direction explicitly so the user is never confused.
"""

# ══════════════════════════════════════════════════════════════════════════════
# FORMATTERS — safe, abstracted, no raw DB internals
# ══════════════════════════════════════════════════════════════════════════════

def _fmt_bias(b: Optional[BiasProfile], label: str) -> str:
    if not b:
        return f"{label}: No qualified data in database.\n"

    lines = [
        f"{label} ({b.bias.upper()}): conviction={b.conviction} | "
        f"avg_WR={b.avg_win_rate*100:.0f}% | best_WR={b.best_win_rate*100:.0f}% | "
        f"avg_gain={b.avg_gain_pct:+.1f}% | best={b.best_gain_pct:+.1f}% | "
        f"worst={b.worst_loss_pct:+.1f}% | total_signals={b.total_signals} ({b.data_depth}) | "
        f"setups_above_75pct_WR={b.supporting_count} | years={b.backtest_years}\n"
    ]

    if b.top_patterns:
        lines.append(
            f"  Strategy categories sorted by win rate "
            f"(abstracted for IP — use these to answer 'which strategies worked'):\n"
        )
        for p in b.top_patterns:
            lines.append(
                f"    • {p.category}: WR={p.win_rate*100:.0f}% | "
                f"avg={p.avg_gain:+.1f}% | best={p.best_gain:+.1f}% | "
                f"worst={p.worst_loss:+.1f}% | {p.occurrences} signals | "
                f"{p.count} strategies\n"
            )
    return "".join(lines)

def _fmt_regime(r: Optional[RegimeContext]) -> str:
    if not r:
        return "Regime: No snapshot available.\n"
    return (
        f"Regime: {r.regime} ({r.regime_score}/100) — {r.regime_label} | "
        f"snapshot={r.snapshot_date} | scanner_ran={r.snapshot_bias}_bias | "
        f"signal={r.signal_label} ({r.signal_score:.0f}%) | "
        f"firing={r.strategies_firing}/{r.strategies_total} | "
        f"hist_WR={r.hist_win_rate:.0f}% | hist_avg_outcome={r.hist_avg_outcome:+.2f}%\n"
    )

def _fmt_levels(l: Optional[PriceLevels]) -> str:
    if not l or not l.current_price:
        return "Price levels: unavailable\n"
    direction = ("Targets ABOVE price = upside | Stop BELOW = downside protection"
                 if l.levels_bias == "bullish"
                 else "Targets BELOW price = downside | Stop ABOVE = invalidation level")
    lines = [
        f"Price: Rs.{l.current_price:,.2f} ({l.price_source}) | "
        f"levels_bias={l.levels_bias} | {direction}\n"
    ]
    if l.target_1:
        lines.append(
            f"  T1={l.target_1:,.2f} ({l.target_1_pct:+.1f}%) | "
            f"T2={l.target_2:,.2f} ({l.target_2_pct:+.1f}%)\n" if l.target_2 else
            f"  T1={l.target_1:,.2f} ({l.target_1_pct:+.1f}%)\n"
        )
    if l.stop_loss:
        lines.append(f"  SL={l.stop_loss:,.2f} ({l.stop_loss_pct:+.1f}%) | "
                     f"R:R=1:{l.risk_reward_t1}\n" if l.risk_reward_t1 else
                     f"  SL={l.stop_loss:,.2f} ({l.stop_loss_pct:+.1f}%)\n")
    return "".join(lines)

# ══════════════════════════════════════════════════════════════════════════════
# PROMPT BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def ticker_prompt(analysis: TickerAnalysis, user_msg: str) -> str:
    if not analysis.exists_in_db:
        return (
            f"User asked: {user_msg}\n"
            f"Ticker {analysis.ticker} is NOT in the KANIDA database.\n"
            f"Tell the user this clearly. Suggest they check spelling or ask about "
            f"an NSE F&O or US options stock."
        )
    return (
        f"User question: {user_msg}\n"
        f"Ticker: {analysis.ticker} | Market: {analysis.market} | "
        f"User intent: {analysis.user_intent}\n"
        f"Quant engine summary: {analysis.quant_note}\n"
        f"Primary edge identified: {analysis.primary_bias.upper()} "
        f"({analysis.overall_conviction} conviction)\n\n"
        f"REGIME\n{_fmt_regime(analysis.regime)}\n"
        f"BULLISH DATA\n{_fmt_bias(analysis.bullish, 'Bull')}\n"
        f"BEARISH DATA\n{_fmt_bias(analysis.bearish, 'Bear')}\n"
        f"PRICE LEVELS\n{_fmt_levels(analysis.levels)}\n"
        f"Focus on {analysis.primary_bias.upper()} as primary. "
        f"If user asked about strategies, list EVERY pattern category above with its stats. "
        f"Under 350 words."
    )

def screener_prompt(output: ScreenerOutput, user_msg: str) -> str:
    lines = [
        f"User question: {user_msg}\n",
        f"Screener: top {output.bias.upper()} stocks in {output.market} "
        f"| {output.count} results ranked by avg win rate\n\n",
    ]
    for r in output.rows:
        ps = f"Rs.{r.price:,.0f}" if r.price else "N/A"
        lines.append(
            f"{r.rank}. {r.ticker} | {ps} | regime={r.regime} | "
            f"conviction={r.conviction} | avg_WR={r.avg_win_rate*100:.0f}% | "
            f"best_WR={r.best_win_rate*100:.0f}% | avg_gain={r.avg_gain_pct:+.1f}% | "
            f"signals={r.total_signals} ({r.data_depth})\n"
        )
    lines.append(
        "\nGroup by conviction level. Pick 3–4 standouts and explain why. "
        "Mention the overall market regime context once only. Under 400 words."
    )
    return "".join(lines)

def top_strats_prompt(items: List[TopStrategyRow], user_msg: str,
                       market: str, bias: Optional[str]) -> str:
    label = bias.upper() if bias else "ALL BIASES"
    lines = [f"User question: {user_msg}\nTop strategy patterns in {market} ({label}):\n\n"]
    for i, s in enumerate(items, 1):
        lines.append(
            f"{i}. {s.ticker} | {s.pattern_type} [{s.timeframe}, {s.bias}] | "
            f"{s.occurrences} signals | WR={s.win_rate*100:.0f}% | avg={s.avg_gain:+.1f}%\n"
        )
    lines.append(
        "\nGroup by pattern type. Which categories dominate? "
        "Which stocks show the strongest patterns? Under 300 words."
    )
    return "".join(lines)

def compare_prompt(analyses: List[TickerAnalysis], user_msg: str) -> str:
    parts = [f"User question: {user_msg}\nSide-by-side comparison:\n\n"]
    for a in analyses:
        parts.append(f"=== {a.ticker} ===\n")
        parts.append(f"Primary edge: {a.primary_bias.upper()} ({a.overall_conviction})\n")
        parts.append(f"Regime: {_fmt_regime(a.regime)}")
        parts.append(f"Bullish: {_fmt_bias(a.bullish, 'Bull')}")
        parts.append(f"Bearish: {_fmt_bias(a.bearish, 'Bear')}")
        parts.append(f"Levels: {_fmt_levels(a.levels)}\n")
    parts.append(
        "Compare directly using a structured format. "
        "State which has stronger data for the user's intent. Under 400 words."
    )
    return "".join(parts)

# ══════════════════════════════════════════════════════════════════════════════
# CLAUDE CALL
# ══════════════════════════════════════════════════════════════════════════════

def ask(prompt: str, history: list, system: str = SYSTEM) -> str:
    if not HAS_CLAUDE:
        return "Install anthropic: pip install anthropic"
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return "ANTHROPIC_API_KEY not set. Run: setx ANTHROPIC_API_KEY sk-ant-..."
    try:
        cli  = _client()
        msgs = list(history) + [{"role": "user", "content": prompt}]
        resp = cli.messages.create(
            model=MODEL, max_tokens=MAX_TOKENS, system=system, messages=msgs
        )
        return resp.content[0].text
    except _anth.AuthenticationError:
        return "API key invalid or expired. Check console.anthropic.com."
    except _anth.APIConnectionError:
        return "Network error — check your internet connection and try again."
    except _anth.RateLimitError:
        return "Rate limit hit — wait a few seconds and try again."
    except Exception as e:
        return f"Error: {str(e)}"
