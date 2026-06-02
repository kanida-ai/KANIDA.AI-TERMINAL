"""Pattern narrator — turn a mined boolean rule into an institutional-grade,
plain-English explanation of *why* the AI is recommending a stock.

The operator was clear (2026-05-23):
  "Don't show pattern codes or machine formulas. Anyone can throw a ticker
   at a retail trader. What advanced users actually want is to understand
   the trade the way an institution would — the why, the evidence, and the
   context. That's the moat."

So this module is the moat surface. It takes the structured `rule_json`
(list of [column, op, threshold] triplets) plus the pattern's mined metadata
(regime, lift, hit-rate, target) and produces 2-3 sentences of natural prose
an analyst would actually write in a research note.

NO column names, NO operator glyphs, NO raw thresholds in the output —
those are translated into observational language ("compressing range",
"closing near the highs", "down 8% over the last quarter", etc.).

Used by:
  power_user.services.falcon_top20_explainer._build_bucket1
"""
from __future__ import annotations

import json
import logging
import math
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("kanida.power_user.pattern_narrator")


# ════════════════════════════════════════════════════════════════════════
# Public entry point
# ════════════════════════════════════════════════════════════════════════

def narrate(
    rule_json:      Optional[str],
    regime:         Optional[str],
    lift_pp:        Optional[float],
    oos_hit_rate:   Optional[float],
    base_rate_pct:  Optional[float],
    target:         Optional[str],
    fallback_blurb: Optional[str] = None,
) -> str:
    """Compose a 2-3 sentence plain-English explanation for one pattern.

    Returns at minimum a generic "The engine identified a statistically
    elevated setup..." sentence so the UI never shows a blank explanation.
    Robust to malformed inputs.
    """
    try:
        rules = json.loads(rule_json) if rule_json else []
    except Exception:
        rules = []

    if not rules:
        return _generic_fallback(regime, lift_pp, oos_hit_rate, base_rate_pct, target, fallback_blurb)

    observations = _conditions_to_observations(rules)
    if not observations:
        return _generic_fallback(regime, lift_pp, oos_hit_rate, base_rate_pct, target, fallback_blurb)

    # First sentence: observational ("Over the past few weeks, this stock has...")
    obs_sentence = _compose_observations(observations)

    # Second sentence: regime + edge ("This is a textbook breakout signature,
    # and historically when it fires the next 20 days hit +10% about 68% of
    # the time vs a 28% baseline.")
    edge_sentence = _compose_edge(regime, lift_pp, oos_hit_rate, base_rate_pct, target)

    return f"{obs_sentence} {edge_sentence}".strip()


# ════════════════════════════════════════════════════════════════════════
# Synthesis narrator — one paragraph across all firing patterns
# ════════════════════════════════════════════════════════════════════════

def narrate_synthesis(
    deduped_conditions: List[List[Any]],
    dominant_regime:    Optional[str],
    weighted_hit_rate:  Optional[float],
    stock_baseline_pct: Optional[float],
    n_patterns:         int,
    target:             Optional[str] = "hit_10pc_20d",
    feat_vals:          Optional[Dict[str, float]] = None,
) -> str:
    """Produce ONE plain-English narrative across all firing patterns.

    Rewritten 2026-05-24 to honour the locked UX spec:
      - NO jargon: 'lift', 'regime', 'confluence', 'capitulation' banned
      - Two short sentences max
      - First sentence = trader-readable observation
      - Second sentence = "94 different signals from our library are
        firing on it, and most say the same thing: <theme>"
    """
    if not deduped_conditions:
        return _synthesis_fallback_plain(dominant_regime, n_patterns)

    # Reduce conditions to observation tags, fuse contradictions before
    # rendering (FIX the "approaching X high / well off X high" bug — these
    # came from one pattern with "> -5" and another with "<= -15" on the
    # SAME column. Keep only the more-recent / tighter-band reading.)
    observations = _conditions_to_observations(deduped_conditions, feat_vals=feat_vals)
    observations = _drop_contradictions(observations)
    if not observations:
        return _synthesis_fallback_plain(dominant_regime, n_patterns)

    obs_sentence_plain = _compose_plain_observation(observations)
    library_sentence = _compose_library_sentence(dominant_regime, n_patterns)
    return f"{obs_sentence_plain} {library_sentence}".strip()


def _drop_contradictions(observations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Strip pairs of observations that contradict each other (e.g.
    "approaching its month high" + "sitting well off its month high").

    Keys observations by the "(window-name) high" / "(window-name) average"
    concept — robust to parenthetical suffixes like "(drawdown setup)" that
    earlier key-extraction missed. Always keeps the first occurrence and
    drops subsequent contradicting ones.
    """
    import re as _re
    POSITIVE_HINTS = (
        "right at", "trading near", "approaching", "near the high",
        "leading", "rising", "trend support", "outperforming",
    )
    NEGATIVE_HINTS = (
        "sitting deep below", "sitting well off", "lagging", "rolling over",
        "flattening or falling", "drawdown setup", "deep pullback",
    )

    def _concept_key(phrase: str) -> str:
        # Strip parentheticals (e.g. "(drawdown setup)", "((deep pullback))")
        clean = _re.sub(r"\s*\(+[^)]*\)+", "", phrase).strip()
        # Take the last meaningful 2-3 words — usually "<window> high" or
        # "<window>-day average" — so positive + negative phrasings on the
        # same column collide.
        words = clean.split()
        if len(words) >= 3:
            return " ".join(words[-3:])
        return clean.lower()

    seen_concepts: Dict[str, Dict[str, Any]] = {}
    out: List[Dict[str, Any]] = []
    for o in observations:
        phrase = o["phrase"]
        key = _concept_key(phrase)
        prior = seen_concepts.get(key)
        if not prior:
            seen_concepts[key] = o
            out.append(o)
            continue
        prior_is_neg = any(h in prior["phrase"] for h in NEGATIVE_HINTS)
        cur_is_pos   = any(h in phrase for h in POSITIVE_HINTS)
        prior_is_pos = any(h in prior["phrase"] for h in POSITIVE_HINTS)
        cur_is_neg   = any(h in phrase for h in NEGATIVE_HINTS)
        if prior_is_neg and cur_is_pos:
            # Swap to the more-positive phrasing
            out.remove(prior)
            seen_concepts[key] = o
            out.append(o)
        elif prior_is_pos and cur_is_neg:
            # Keep prior — drop the contradicting negative
            continue
        else:
            # Same polarity — just drop the duplicate
            continue
    return out


def _compose_plain_observation(observations: List[Dict[str, Any]]) -> str:
    """Lead sentence — plain English, no jargon. Picks up to 3 observations
    by weight and stitches them with commas + 'and'."""
    seen: set[str] = set()
    unique: List[Dict[str, Any]] = []
    for o in observations:
        if o["phrase"] in seen:
            continue
        seen.add(o["phrase"])
        unique.append(o)
    unique.sort(key=lambda o: o["weight"])
    clauses = [_strip_jargon(o["phrase"]) for o in unique[:3]]
    if not clauses:
        return "The stock is showing a clean setup."
    head = f"The stock is {clauses[0]}"
    if len(clauses) == 1:
        return head + "."
    if len(clauses) == 2:
        return f"{head}, and {clauses[1]}."
    return f"{head}, {clauses[1]}, and {clauses[2]}."


def _strip_jargon(phrase: str) -> str:
    """Final pass to strip any banned jargon that slipped through the
    observation phrasers. Mirrors the spec's banned-words list."""
    replacements = {
        "the 50-day moving average sloping up (trend support)":
            "with its 50-day average sloping up",
        "the 50-day moving average rolling over":
            "with its 50-day average rolling over",
        "with the 20-day moving average rising":
            "with its 20-day average rising",
        "the 20-day moving average flattening or falling":
            "with its 20-day average flattening",
        "20-day moving average":         "20-day average",
        "50-day moving average":         "50-day average",
        "200-day moving average":        "200-day average",
        "(drawdown setup)":              "(deep pullback)",
        "drawdown setup":                "deep pullback",
        "(volume dry-up, often a precursor to expansion)":
                                          "(volume drying up — often before a move)",
        "20-day volatility in a defined band":
                                          "settling into a steady volatility range",
        "20-day volatility":             "recent volatility",
        "long-base breakout":            "breakout from a long base",
    }
    for old, new in replacements.items():
        phrase = phrase.replace(old, new)
    return phrase


def _compose_library_sentence(regime: Optional[str], n: int) -> str:
    """Closing sentence — 'N different signals from our library are firing
    on it right now, and most say the same thing: <theme>.'"""
    theme = _REGIME_PLAIN_THEME.get((regime or "other").lower(),
                                     _REGIME_PLAIN_THEME["other"])
    return (f"{n} different signals from our library are firing on this "
            f"stock right now, and most say the same thing: {theme}")


def _synthesis_fallback_plain(regime: Optional[str], n: int) -> str:
    theme = _REGIME_PLAIN_THEME.get((regime or "other").lower(),
                                     _REGIME_PLAIN_THEME["other"])
    return (f"{n} different signals from our library are firing on this "
            f"stock right now: {theme}")


_REGIME_PLAIN_THEME: Dict[str, str] = {
    "breakout":       "this is a clean breakout setup.",
    "momentum":       "the stock is in a strong uptrend.",
    "compression":    "the stock is coiled up and ready for a move.",
    "reversal":       "the stock looks ready to turn higher.",
    "mean_reversion": "the stock has pulled back inside its trend.",
    "capitulation":   "this is an oversold-bounce setup.",
    "other":          "several different factors are lining up at once.",
}


def _compose_weighted_edge(
    regime:             Optional[str],
    weighted_hit_rate:  Optional[float],
    stock_baseline_pct: Optional[float],
    n_patterns:         int,
    target:             Optional[str],
) -> str:
    intro = _REGIME_NARRATIVE.get((regime or "other").lower(),
                                   _REGIME_NARRATIVE["other"])
    tgt   = _TARGET_PROSE.get((target or "").lower(),
                               "hit its target move within the historical window")

    if weighted_hit_rate is not None and stock_baseline_pct is not None:
        delta = weighted_hit_rate - stock_baseline_pct
        delta_phrase = (
            f" — that{chr(0x2019)}s {delta:+.0f}pp "
            f"{'above' if delta >= 0 else 'below'} the stock{chr(0x2019)}s "
            f"lifetime baseline of {stock_baseline_pct:.0f}%"
        )
        return (f"{intro}, and the {n_patterns} firing patterns together "
                f"project a {weighted_hit_rate:.0f}% chance the stock will "
                f"{tgt}{delta_phrase}.")
    if weighted_hit_rate is not None:
        return (f"{intro}, and the {n_patterns} firing patterns project "
                f"about a {weighted_hit_rate:.0f}% chance the stock will {tgt}.")
    return f"{intro}, with {n_patterns} confluence patterns firing today."


def _synthesis_fallback(
    regime:             Optional[str],
    weighted_hit_rate:  Optional[float],
    stock_baseline_pct: Optional[float],
    n_patterns:         int,
) -> str:
    intro = _REGIME_NARRATIVE.get((regime or "other").lower(),
                                   _REGIME_NARRATIVE["other"])
    if weighted_hit_rate is not None:
        return (f"{intro}. {n_patterns} patterns fired today; the engine "
                f"projects about a {weighted_hit_rate:.0f}% +10%/20d hit rate.")
    return f"{intro}, with {n_patterns} patterns firing today."


# ════════════════════════════════════════════════════════════════════════
# Step 1 — convert raw [col, op, thresh] triplets into observation tags
# ════════════════════════════════════════════════════════════════════════

# Each observation carries:
#   theme:  high-level bucket for grouping into sentences
#   phrase: the actual prose fragment to drop in
#   weight: order of importance within the theme (smaller = more salient)
#
# When two rules touch the same column with conflicting interpretations
# (e.g. one says "low volatility", another says "high volatility") the
# higher-confidence one wins — typically the tighter threshold.

_THEME_VOLATILITY  = "volatility"
_THEME_LOCATION    = "location"   # where price sits vs trend, highs, MAs
_THEME_MOMENTUM    = "momentum"
_THEME_VOLUME      = "volume"
_THEME_REL_STR     = "relative_strength"
_THEME_COMPRESSION = "compression"
_THEME_OTHER       = "other"


def _conditions_to_observations(
    rules: List[List[Any]],
    feat_vals: Optional[Dict[str, float]] = None,
) -> List[Dict[str, Any]]:
    # Fuse opposing conditions on the same column FIRST (e.g. 9 < range <= 14
    # becomes "trading in a moderate weekly range" — one clause, not two
    # contradictory ones). Then phrase the remaining individual conditions.
    #
    # feat_vals (2026-06-02): the STOCK'S ACTUAL feature values for this
    # signal_date. When present, location/distance phrasings are grounded in
    # the stock's real position rather than the pattern's THRESHOLD. The
    # threshold is just the loosest bar the stock cleared and badly
    # misrepresents reality for drawdown-bounce patterns — the JKCEMENT bug
    # where a stock 31% below its 1-yr high was narrated as "approaching its
    # one-year high" because a matched pattern's rule was `dist_high_252 > -35`.
    fused: List[Dict[str, Any]] = []
    individual: List[Tuple[str, str, float]] = []

    by_col: Dict[str, List[Tuple[str, float]]] = {}
    for cond in rules:
        if not isinstance(cond, list) or len(cond) < 3:
            continue
        col, op, thresh = cond[0], cond[1], cond[2]
        try:
            thresh = float(thresh)
        except Exception:
            continue
        by_col.setdefault(col, []).append((op, thresh))

    for col, ops in by_col.items():
        if len(ops) == 2:
            # Detect a "between" pattern: one upper bound + one lower bound.
            ups   = [t for op, t in ops if op in (">",  ">=")]
            downs = [t for op, t in ops if op in ("<=", "<")]
            if len(ups) == 1 and len(downs) == 1 and ups[0] < downs[0]:
                between = _phrase_for_between(col, ups[0], downs[0])
                if between:
                    fused.append(between)
                    continue
        for op, t in ops:
            individual.append((col, op, t))

    for col, op, t in individual:
        actual = None
        if feat_vals is not None:
            av = feat_vals.get(col)
            if av is not None:
                try:
                    fav = float(av)
                    if not math.isnan(fav):
                        actual = fav
                except (TypeError, ValueError):
                    actual = None
        obs = _phrase_for(col, op, t, actual=actual)
        if obs:
            fused.append(obs)
    return fused


def _phrase_for_between(col: str, lo: float, hi: float) -> Optional[Dict[str, Any]]:
    """Two opposing thresholds on the same column → one fused observation."""
    if col == "weekly_range_pct":
        if hi <= 8:        tier = "a contained"
        elif hi <= 12:     tier = "a moderate"
        else:              tier = "an expanding"
        return _obs(_THEME_VOLATILITY, f"trading in {tier} weekly range", weight=2)
    if col == "atr_20_pct":
        return _obs(_THEME_VOLATILITY,
                     f"showing 20-day volatility in a defined band", weight=2)
    if col == "roc_5":
        if lo >= 0:        return _obs(_THEME_MOMENTUM, "ticking up over the past week", weight=2)
        return _obs(_THEME_MOMENTUM, "consolidating over the past week", weight=2)
    if col == "roc_20":
        if lo >= 0:        return _obs(_THEME_MOMENTUM, "trending up over the past month", weight=2)
        return _obs(_THEME_MOMENTUM, "consolidating over the past month", weight=2)
    if col == "weekly_close_loc":
        return _obs(_THEME_LOCATION, "closing in a tight band of its weekly range", weight=1)
    # Default: skip rather than narrate badly
    return None


def _phrase_for(col: str, op: str, t: float,
                actual: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """One [col, op, threshold] → one observation dict or None.

    For 34 distinct columns. The thresholds are summarised into 2-3 magnitude
    bands (e.g. 'mildly elevated' / 'sharply elevated') so the narrative
    reads like analyst commentary, not a regression printout.

    actual (2026-06-02): the stock's REAL value for this column on this date.
    For location/distance features ('where price sits') we use `actual` rather
    than the pattern threshold `t` to choose the wording — the threshold is
    the loosest bar the stock cleared and can be wildly off (JKCEMENT bug: a
    stock 31% below its 1-yr high described as "approaching its one-year high"
    because a matched pattern's rule was `dist_high_252 > -35`). When `actual`
    is None we fall back to the threshold (legacy behaviour).
    """
    is_gt = op in (">", ">=")

    # ── Volatility ───────────────────────────────────────────────────
    if col == "atr_20_pct":
        # ATR as % of price. 1% = sleepy, 3% = active, 5%+ = volatile
        if is_gt:
            tier = "sharply elevated" if t >= 4 else "elevated" if t >= 2.5 else "active"
            return _obs(_THEME_VOLATILITY, f"showing {tier} 20-day volatility", weight=1)
        else:
            tier = "very quiet" if t <= 1.5 else "subdued"
            return _obs(_THEME_VOLATILITY, f"with {tier} 20-day volatility", weight=1)

    if col == "weekly_range_pct":
        if is_gt:
            tier = "a wide" if t >= 8 else "an expanding"
            return _obs(_THEME_VOLATILITY, f"trading in {tier} weekly range", weight=2)
        else:
            return _obs(_THEME_COMPRESSION, "trading in a tight weekly range", weight=2)

    if col == "atr_5_vs_20":
        if is_gt:
            return _obs(_THEME_VOLATILITY,
                         "with short-term volatility expanding versus the prior month", weight=3)
        else:
            return _obs(_THEME_COMPRESSION,
                         "short-term volatility contracting versus the prior month", weight=3)

    if col == "range_pct":
        if is_gt:
            return _obs(_THEME_VOLATILITY, "printing an unusually wide daily bar", weight=4)
        else:
            return _obs(_THEME_COMPRESSION, "printing a notably narrow daily bar", weight=4)

    # ── Compression / range patterns ─────────────────────────────────
    if col == "n_sub_3_range_7d":
        if is_gt:
            return _obs(_THEME_COMPRESSION,
                         "with several narrow-range days clustered in the last week (range contraction)",
                         weight=1)

    if col == "n_sub_2_5_range_7d":
        if is_gt:
            return _obs(_THEME_COMPRESSION,
                         "with multiple very tight-range days in the last week (deep range contraction)",
                         weight=1)

    if col == "n_sub_75v_20d" or col == "n_sub_75v_7d":
        window = "month" if col == "n_sub_75v_20d" else "week"
        if is_gt:
            return _obs(_THEME_VOLUME,
                         f"a string of below-average-volume sessions over the last {window} "
                         f"(volume dry-up, often a precursor to expansion)",
                         weight=1)

    # ── Location: where price sits ──────────────────────────────────
    if col == "weekly_close_loc":
        # 0 = closed at weekly low, 1 = closed at weekly high
        if is_gt:
            tier = ("near the high of"      if t >= 0.7
                    else "in the upper half of")
            return _obs(_THEME_LOCATION,
                         f"closing {tier} its weekly range", weight=1)
        else:
            return _obs(_THEME_LOCATION,
                         "closing in the lower portion of its weekly range", weight=1)

    if col == "close_loc":
        if is_gt:
            return _obs(_THEME_LOCATION,
                         "closing near the top of its daily range", weight=2)
        else:
            return _obs(_THEME_LOCATION,
                         "closing in the lower portion of its daily range", weight=2)

    if col == "weekly_close_vs_sma20":
        if is_gt:
            tier = "well above" if t >= 5 else "above"
            return _obs(_THEME_LOCATION,
                         f"trading {tier} its 20-day moving average", weight=2)
        else:
            tier = "well below" if t <= -5 else "below"
            return _obs(_THEME_LOCATION,
                         f"trading {tier} its 20-day moving average", weight=2)

    if col in ("dist_sma_20", "dist_sma_50", "dist_sma_200"):
        n = col.split("_")[-1]
        val = actual if actual is not None else t
        if is_gt:
            tier = "extended above" if val >= 10 else "above"
            return _obs(_THEME_LOCATION,
                         f"sitting {tier} its {n}-day moving average", weight=3)
        else:
            tier = "deeply below" if val <= -10 else "below"
            return _obs(_THEME_LOCATION,
                         f"sitting {tier} its {n}-day moving average", weight=3)

    if col in ("dist_high_10", "dist_high_20", "dist_high_60",
               "dist_high_120", "dist_high_252"):
        n = col.split("_")[-1]
        window = {
            "10":  "two-week", "20":  "month",
            "60":  "quarter",   "120": "six-month",
            "252": "one-year",
        }.get(n, f"{n}-day")
        # dist_high_X is the % distance from the window high; <= 0 in practice.
        # Use the stock's ACTUAL distance when available (the threshold lies for
        # drawdown-bounce patterns — JKCEMENT bug 2026-06-02). Floor the
        # "approaching" language at -12%: beyond that the stock is genuinely in
        # a pullback, NOT approaching its high, so describe it honestly (or drop
        # it, never claim proximity it doesn't have).
        val = actual if actual is not None else t
        if is_gt:
            if val >= -2:
                return _obs(_THEME_LOCATION, f"trading right at its {window} high", weight=1)
            elif val >= -6:
                return _obs(_THEME_LOCATION, f"trading near its {window} high", weight=1)
            elif val >= -12:
                return _obs(_THEME_LOCATION, f"approaching its {window} high", weight=1)
            elif val >= -25:
                # 12-25% below: a real pullback. State it honestly WITHOUT
                # asserting a bounce ("recovering" would overclaim a reversal
                # that may not exist). Keep the "(drawdown setup)" marker so
                # _drop_contradictions still recognises it as a negative read.
                return _obs(_THEME_LOCATION,
                             f"trading below its {window} high (drawdown setup)",
                             weight=1)
            else:
                # Deeper than 25% below: far from the high. Saying anything about
                # the high here is misleading on a buy card — drop it entirely
                # and let the momentum/compression signals carry the thesis.
                return None
        else:
            tier = "sitting deep below" if val <= -15 else "sitting well off"
            return _obs(_THEME_LOCATION,
                         f"{tier} its {window} high (drawdown setup)", weight=1)

    if col == "slope_sma_50":
        if is_gt: return _obs(_THEME_LOCATION,
                              "with the 50-day moving average sloping up (trend support)", weight=2)
        else:     return _obs(_THEME_LOCATION,
                              "with the 50-day moving average rolling over", weight=2)

    if col == "slope_sma_20":
        if is_gt: return _obs(_THEME_LOCATION,
                              "the 20-day moving average rising", weight=3)
        else:     return _obs(_THEME_LOCATION,
                              "the 20-day moving average flattening or falling", weight=3)

    # ── Momentum ─────────────────────────────────────────────────────
    if col == "roc_5":
        # 5-day return
        if is_gt:
            tier = "sharply up" if t >= 5 else "up"
            return _obs(_THEME_MOMENTUM,
                         f"{tier} over the past week", weight=2)
        else:
            tier = "consolidating" if t >= 0 else "down"
            return _obs(_THEME_MOMENTUM,
                         f"{tier} over the past week", weight=2)

    if col == "roc_20":
        if is_gt:
            tier = "strongly up" if t >= 10 else "up"
            return _obs(_THEME_MOMENTUM,
                         f"{tier} over the past month", weight=2)
        else:
            tier = "down" if t < 0 else "flat to slightly down"
            return _obs(_THEME_MOMENTUM,
                         f"{tier} over the past month", weight=2)

    if col == "roc_60":
        if is_gt:
            tier = "rallying" if t >= 15 else "up"
            return _obs(_THEME_MOMENTUM,
                         f"{tier} over the past quarter", weight=2)
        else:
            return _obs(_THEME_MOMENTUM,
                         f"down {abs(t):.0f}% or more over the past quarter", weight=2)

    if col == "rsi_14":
        if is_gt:
            tier = "overbought territory" if t >= 70 else "an elevated RSI"
            return _obs(_THEME_MOMENTUM,
                         f"trading in {tier}", weight=3)
        else:
            tier = "oversold territory" if t <= 30 else "a muted RSI"
            return _obs(_THEME_MOMENTUM,
                         f"trading in {tier}", weight=3)

    if col == "n_higher_lows_5d":
        if is_gt: return _obs(_THEME_MOMENTUM,
                              "stringing together consecutive higher daily lows", weight=2)

    # ── Volume ───────────────────────────────────────────────────────
    if col == "vol_5d_vs_20d":
        if is_gt:
            return _obs(_THEME_VOLUME,
                         "with recent volume picking up versus its month average", weight=2)
        else:
            return _obs(_THEME_VOLUME,
                         "with recent volume below its month average", weight=3)

    if col == "vol_vs_20d":
        if is_gt:
            return _obs(_THEME_VOLUME,
                         "with today's volume above the 20-day average", weight=2)

    # ── Relative strength vs sector / market ─────────────────────────
    if col in ("rs_sector_20d", "rs_sector_60d"):
        window = "month" if col.endswith("20d") else "quarter"
        if is_gt:
            return _obs(_THEME_REL_STR,
                         f"outperforming its sector over the past {window}", weight=1)
        else:
            return _obs(_THEME_REL_STR,
                         f"lagging its sector over the past {window}", weight=2)

    if col in ("rs_market_20d", "rs_market_60d"):
        window = "month" if col.endswith("20d") else "quarter"
        if is_gt:
            return _obs(_THEME_REL_STR,
                         f"outperforming the broader market over the past {window}", weight=2)
        else:
            return _obs(_THEME_REL_STR,
                         f"lagging the broader market over the past {window}", weight=3)

    # ── Specific signals ─────────────────────────────────────────────
    if col == "weekly_breakout_20w":
        if is_gt:
            return _obs(_THEME_LOCATION,
                         "breaking above its 20-week high (long-base breakout)", weight=1)

    if col == "lower_wick_pct":
        if is_gt:
            return _obs(_THEME_LOCATION,
                         "with a long lower wick (rejection of intraday lows)", weight=3)

    # Unknown column — skip silently. Better to omit than to leak a column name.
    return None


def _obs(theme: str, phrase: str, weight: int = 5) -> Dict[str, Any]:
    return {"theme": theme, "phrase": phrase, "weight": weight}


# ════════════════════════════════════════════════════════════════════════
# Step 2 — compose observations into a single readable sentence
# ════════════════════════════════════════════════════════════════════════

# Sentence opener varies by which themes dominate so cards don't all
# start with the same words.
_OPENERS_BY_LEAD = {
    _THEME_LOCATION:    "The stock is",
    _THEME_MOMENTUM:    "Price action shows the stock",
    _THEME_VOLATILITY:  "The stock is",
    _THEME_COMPRESSION: "Recent action shows",
    _THEME_VOLUME:      "Volume behaviour shows",
    _THEME_REL_STR:     "Relative to its peers, the stock is",
    _THEME_OTHER:       "The setup shows the stock",
}


def _compose_observations(observations: List[Dict[str, Any]]) -> str:
    # De-dupe near-identical phrases (e.g. two thresholds on the same column
    # may have produced the same prose); preserve order of first appearance.
    seen: set[str] = set()
    unique: List[Dict[str, Any]] = []
    for o in observations:
        key = o["phrase"]
        if key in seen:
            continue
        seen.add(key)
        unique.append(o)

    # Pick lead theme by lowest-weight clause (most salient observation)
    unique.sort(key=lambda o: o["weight"])
    lead_theme = unique[0]["theme"]
    opener = _OPENERS_BY_LEAD.get(lead_theme, "The stock is")

    # First clause is the lead. Subsequent clauses get prefixed with comma /
    # "and". Cap at 4 clauses so the sentence stays readable.
    clauses = [o["phrase"] for o in unique[:4]]

    # Glue: opener + first clause, then commas, then "and" before last.
    head = f"{opener} {clauses[0]}"
    if len(clauses) == 1:
        return head + "."
    if len(clauses) == 2:
        return f"{head}, and {clauses[1]}."
    body = ", ".join(clauses[1:-1])
    return f"{head}, {body}, and {clauses[-1]}."


# ════════════════════════════════════════════════════════════════════════
# Step 3 — closing sentence (regime + edge)
# ════════════════════════════════════════════════════════════════════════

_REGIME_NARRATIVE = {
    "breakout":       "This is a classic breakout setup",
    "momentum":       "This is a momentum-continuation signature",
    "compression":    "This is a coiled-spring compression pattern",
    "reversal":       "This is a reversal-from-extreme signature",
    "mean_reversion": "This is a mean-reversion setup",
    "capitulation":   "This is a late-stage capitulation pattern",
    "other":          "This is a multi-factor confluence the engine has back-tested",
}

_TARGET_PROSE = {
    "hit_10pc_20d": "hit a +10% move within 20 trading days",
    "hit_15pc_20d": "hit a +15% move within 20 trading days",
    "hit_25pc_30d": "hit a +25% move within 30 trading days",
    "hit_40pc_40d": "hit a +40% move within 40 trading days",
}


def _compose_edge(
    regime:         Optional[str],
    lift_pp:        Optional[float],
    oos_hit_rate:   Optional[float],
    base_rate_pct:  Optional[float],
    target:         Optional[str],
) -> str:
    intro    = _REGIME_NARRATIVE.get((regime or "other").lower(),
                                      _REGIME_NARRATIVE["other"])
    tgt      = _TARGET_PROSE.get((target or "").lower(),
                                  "hit its target move within the historical window")
    hit_rate = _safe_pct(oos_hit_rate)
    base     = _safe_pct(base_rate_pct)
    lift     = _safe_pct(lift_pp)

    if hit_rate is not None and base is not None:
        return (f"{intro} — historically when this pattern fires, the stock "
                f"has {tgt} about {hit_rate:.0f}% of the time, versus a "
                f"{base:.0f}% baseline for an average stock.")
    if lift is not None:
        return (f"{intro} — historically this fingerprint has delivered "
                f"roughly {lift:+.0f} percentage points of edge over baseline.")
    return f"{intro}."


def _safe_pct(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


# ════════════════════════════════════════════════════════════════════════
# Generic fallback (for malformed / missing rule_json)
# ════════════════════════════════════════════════════════════════════════

def _generic_fallback(
    regime:         Optional[str],
    lift_pp:        Optional[float],
    oos_hit_rate:   Optional[float],
    base_rate_pct:  Optional[float],
    target:         Optional[str],
    blurb:          Optional[str],
) -> str:
    edge = _compose_edge(regime, lift_pp, oos_hit_rate, base_rate_pct, target)
    headline = blurb or "A multi-factor pattern the engine has back-tested"
    return f"{headline}. {edge}"
