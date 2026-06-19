"""Trader-voice pick explainer — the production module.

Ported from `universe_engine/.../explain_picks_v3.py` (worktree reference impl).
Three polish fixes applied during the port:

  1. SECTOR NATURALIZATION  — "A telecommunication in..." → "A telecom stock in..."
     We map every sector enum value to a natural form via `SECTOR_NATURAL`.
  2. SENTENCE CAPITALIZATION — "just confirmed" after a period → "Just confirmed".
     Helper `_capitalize_sentences` normalises every assembled story.
  3. STORY JOINING — no double periods (`bits[-1]` may already end in `.`),
     every sentence starts capitalised.

Public surface (what routers/replay_cache import):

    build_pick_payload(rank, pick, outcomes=None) -> dict
        Canonical Pick payload per Design.md §5. Single source of truth shape.

    compute_top_n(con, date_str, top_n) -> list[pick]
        Recompute top-N for any date from features + patterns.

    get_outcomes(con, symbol, entry_date) -> dict[str, float]
        D+1/3/5/10/15 returns from ohlc_daily (historical replay mode).

    aggregate_outcomes(picks_with_outcomes) -> dict
        Per-horizon WR / hit_5pct / avg_ret across a list of picks.
"""
from __future__ import annotations

import json
import re
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from .feature_cols import FEATURE_COLS, in_drawdown_bounce


# ──────────────────────────────────────────────────────────────────────────
#  POLISH FIX #1 — Sector naturalization
# ──────────────────────────────────────────────────────────────────────────
# The story generator says `"A {sector.lower()}"` which produces awkward
# phrases like "A telecommunication" or "A capital goods". This dict maps
# every sector enum we have to a natural noun phrase. Default fallback:
# "A {sector.lower()} stock" — same as original behaviour for unknown sectors.

SECTOR_NATURAL: Dict[str, str] = {
    "Automobile and Auto Components":  "An auto-sector stock",
    "Capital Goods":                   "A capital-goods stock",
    "Chemicals":                       "A chemicals stock",
    "Construction":                    "A construction stock",
    "Construction Materials":          "A construction-materials stock",
    "Consumer Durables":               "A consumer-durables stock",
    "Consumer Services":               "A consumer-services stock",
    "Diversified":                     "A diversified play",
    "Fast Moving Consumer Goods":      "An FMCG stock",
    "Financial Services":              "A financial-services stock",
    "Healthcare":                      "A healthcare stock",
    "Information Technology":          "An IT stock",
    "Media Entertainment & Publication": "A media stock",
    "Metals & Mining":                 "A metals stock",
    "Oil Gas & Consumable Fuels":      "An energy stock",
    "Power":                           "A power-sector stock",
    "Realty":                          "A real-estate stock",
    "Services":                        "A services stock",
    "Telecommunication":               "A telecom stock",
    "Textiles":                        "A textiles stock",
}


def _sector_natural(sector: Optional[str]) -> str:
    """Return natural noun phrase for the sector, with fallback."""
    if not sector:
        return "A stock"
    s = SECTOR_NATURAL.get(sector)
    if s:
        return s
    return f"A {sector.lower()} stock"   # graceful fallback


# ──────────────────────────────────────────────────────────────────────────
#  POLISH FIX #2 + #3 — Sentence capitalization + story joining
# ──────────────────────────────────────────────────────────────────────────

_DOUBLE_PERIOD_RE = re.compile(r"\.{2,}")
_SENT_SPLIT_RE    = re.compile(r"(?<=[.!?])\s+")


def _capitalize_sentences(s: str) -> str:
    """Ensure every sentence starts with a capital letter, no double periods.

    Idempotent. Safe to call multiple times. Preserves trailing punctuation
    but normalises whitespace between sentences.
    """
    if not s:
        return s
    # Collapse runs of periods → single period (handles ".." artifacts)
    s = _DOUBLE_PERIOD_RE.sub(".", s)
    # Split on sentence-ending punctuation + whitespace
    parts = _SENT_SPLIT_RE.split(s.strip())
    out = []
    for p in parts:
        if not p:
            continue
        # Capitalize first alpha char; leave rest untouched
        i = 0
        while i < len(p) and not p[i].isalpha():
            i += 1
        if i < len(p):
            p = p[:i] + p[i].upper() + p[i + 1:]
        out.append(p)
    return " ".join(out).strip()


def _join_story_bits(bits: List[str]) -> str:
    """Join sentence fragments into a clean 1-3 sentence story.

    Each `bit` is a fragment that does NOT end with a period. We add one
    period between bits, then capitalize the first letter of each sentence.
    """
    if not bits:
        return "A high-conviction setup identified by multiple patterns."

    # Strip trailing periods from individual bits (defensive)
    bits = [b.rstrip(".").strip() for b in bits if b and b.strip()]

    if len(bits) == 1:
        return _capitalize_sentences(bits[0] + ".")
    if len(bits) == 2:
        # Combine into one or two sentences depending on length
        return _capitalize_sentences(bits[0] + ", " + bits[1] + ".")
    # 3+ bits → first sentence = bits[0] + bits[1] (richer opener),
    # second sentence = bits[2:] joined by period
    sent1 = bits[0] + ", " + bits[1] + "."
    sent2 = ". ".join(bits[2:]) + "."
    return _capitalize_sentences(sent1 + " " + sent2)


# ──────────────────────────────────────────────────────────────────────────
#  TRANSLATION LIBRARY — (feature, op, threshold) → trader-voice phrase
# ──────────────────────────────────────────────────────────────────────────
# Verbatim from worktree reference impl. Each phrase_* function handles
# one feature family. Returning None means "no clean phrase, skip this
# condition in the trader summary."

def phrase_close_loc(op: str, th: float) -> Optional[str]:
    if op == ">"  and th >= 0.85: return "closed at the very top of the day (powerful close)"
    if op == ">"  and th >= 0.66: return "closed strong in the upper third of the day"
    if op == ">"  and th >= 0.5:  return "closed in the upper half"
    if op == "<=" and th <= 0.33: return "closed weak in the lower third"
    return None

def phrase_weekly_close_loc(op: str, th: float) -> Optional[str]:
    if op == ">"  and th >= 0.66: return "closed the WEEK powerfully (upper third of weekly range)"
    if op == ">"  and th >= 0.5:  return "closed the week in the upper half"
    if op == "<=" and th <= 0.33: return "weak weekly close"
    return None

def phrase_weekly_range(op: str, th: float) -> Optional[str]:
    if op == ">"  and th >= 14: return "very wide weekly trading range (volatile, moving)"
    if op == ">"  and th >= 10: return "wide weekly range"
    if op == ">"  and th >= 7:  return "decent weekly range"
    if op == "<=" and th <= 5:  return "tight weekly range (compression)"
    return None

def phrase_atr(op: str, th: float) -> Optional[str]:
    if op == ">"  and th >= 5:   return "high daily volatility"
    if op == ">"  and th >= 3:   return "above-average volatility"
    if op == ">"  and th >= 2:   return "moderate volatility"
    if op == "<=" and th <= 1.5: return "very low volatility (quiet, coiling)"
    if op == "<=" and th <= 2.5: return "low daily volatility (quiet)"
    return None

def phrase_rsi(op: str, th: float) -> Optional[str]:
    if op == ">"  and th >= 70: return "very strong momentum (RSI 70+)"
    if op == ">"  and th >= 60: return "strong momentum"
    if op == ">"  and th >= 50: return "positive momentum"
    if op == "<=" and th <= 30: return "oversold (potential bounce zone)"
    if op == "<=" and th <= 45: return "weak momentum"
    return None

def phrase_vol(op: str, th: float) -> Optional[str]:
    if op == ">"  and th >= 2.5: return "very heavy volume (strong buying)"
    if op == ">"  and th >= 1.5: return "heavy volume"
    if op == ">"  and th >= 1.0: return "above-average volume"
    if op == "<=" and th <= 0.5: return "very dry volume (compression before move)"
    if op == "<=" and th <= 0.75: return "low volume (quiet)"
    return None

def phrase_dist_sma(feature: str, op: str, th: float) -> Optional[str]:
    days = feature.split("_")[-1]
    if op == ">"  and th >= 20:   return f"strong uptrend ({days}-day MA)"
    if op == ">"  and th >= 10:   return f"comfortably above {days}-day MA"
    if op == ">"  and th >= 0:    return f"above {days}-day MA"
    if op == "<=" and th <= -10:  return f"deep below {days}-day MA"
    return None

def phrase_slope_sma(feature: str, op: str, th: float) -> Optional[str]:
    days = feature.split("_")[-1]
    if op == ">"  and th >  0: return f"{days}-day MA rising"
    if op == "<=" and th <= 0: return f"{days}-day MA falling"
    return None

def phrase_roc(feature: str, op: str, th: float) -> Optional[str]:
    days = feature.split("_")[-1]
    if op == ">"  and th >= 30: return f"big rally already underway ({days}-day +{th:.0f}%+)"
    if op == ">"  and th >= 10: return f"strong recent gains ({days}-day +{th:.0f}%+)"
    if op == ">"  and th >= 0:  return f"positive recent returns ({days}-day)"
    if op == "<=" and th <= 5 and th > 0: return f"recent return controlled ({days}-day ≤ +{th:.0f}%)"
    if op == "<=" and th <= 0: return f"in pullback ({days}-day down)"
    return None

def phrase_dist_high(feature: str, op: str, th: float) -> Optional[str]:
    days = feature.split("_")[-1]
    label = "52-week" if days == "252" else f"{days}-day"
    if op == ">"  and th >= -2:   return f"at {label} high"
    if op == ">"  and th >= -10:  return f"near {label} high"
    if op == "<=" and th <= -20:  return f"in drawdown (well below {label} high)"
    return None

def phrase_breakout(op: str, th: float) -> Optional[str]:
    if op == ">" and th == 0: return "broke above 20-week high (multi-month breakout)"
    return None

def phrase_higher_lows(op: str, th: float) -> Optional[str]:
    if op == ">": return f"stair-step uptrend forming ({int(th)}+ higher lows in 5 days)"
    return None

def phrase_higher_highs(op: str, th: float) -> Optional[str]:
    if op == ">": return f"making new highs day-after-day ({int(th)}+ higher highs in 5 days)"
    return None

def phrase_compression(feature: str, op: str, th: float) -> Optional[str]:
    if op == ">" and th >= 3: return "volume dry-up (compression — coiling for breakout)"
    return None

def phrase_rs_sector(op: str, th: float) -> Optional[str]:
    if op == ">"  and th >= 10:  return "strongly outperforming its sector"
    if op == ">"  and th >= 0:   return "outperforming its sector"
    if op == "<=" and th <= -10: return "underperforming sector"
    return None

def phrase_rs_market(op: str, th: float) -> Optional[str]:
    if op == ">" and th >= 10: return "strongly outperforming the market"
    if op == ">" and th >= 0:  return "outperforming the market"
    return None

def phrase_weekly_vs_sma(op: str, th: float) -> Optional[str]:
    if op == ">"  and th >= 30: return "weekly close way above 20-week trend"
    if op == ">"  and th >= 10: return "weekly close above 20-week trend"
    if op == "<=" and th <= 0:  return "weekly close below 20-week trend"
    return None


def feature_to_phrase(feature: str, op: str, threshold: float) -> Optional[str]:
    """Convert one (feature, op, threshold) tuple → trader-voice phrase (or None)."""
    if feature == "close_loc":              return phrase_close_loc(op, threshold)
    if feature == "weekly_close_loc":       return phrase_weekly_close_loc(op, threshold)
    if feature == "weekly_range_pct":       return phrase_weekly_range(op, threshold)
    if feature == "atr_20_pct":             return phrase_atr(op, threshold)
    if feature == "rsi_14":                 return phrase_rsi(op, threshold)
    if feature == "vol_vs_20d":             return phrase_vol(op, threshold)
    if feature.startswith("dist_sma_"):     return phrase_dist_sma(feature, op, threshold)
    if feature.startswith("slope_sma_"):    return phrase_slope_sma(feature, op, threshold)
    if feature.startswith("roc_"):          return phrase_roc(feature, op, threshold)
    if feature.startswith("dist_high_"):    return phrase_dist_high(feature, op, threshold)
    if feature == "weekly_breakout_20w":    return phrase_breakout(op, threshold)
    if feature == "n_higher_lows_5d":       return phrase_higher_lows(op, threshold)
    if feature == "n_higher_highs_5d":      return phrase_higher_highs(op, threshold)
    if feature.startswith("n_sub_75v") or feature.startswith("n_sub_2_5_range") or feature.startswith("n_sub_3_range"):
        return phrase_compression(feature, op, threshold)
    if feature.startswith("rs_sector"):     return phrase_rs_sector(op, threshold)
    if feature.startswith("rs_market"):     return phrase_rs_market(op, threshold)
    if feature == "weekly_close_vs_sma20":  return phrase_weekly_vs_sma(op, threshold)
    return None


def rule_to_trader_phrase(rule: List[Tuple[str, str, float]]) -> str:
    """Convert a full pattern rule (list of conditions) to a trader-voice summary."""
    phrases: List[str] = []
    seen = set()
    for f, op, th in rule:
        p = feature_to_phrase(f, op, th)
        if p and p not in seen:
            phrases.append(p)
            seen.add(p)
    if not phrases:
        return "complex multi-factor setup"
    if len(phrases) == 1:
        return phrases[0]
    return " + ".join(phrases)


# ──────────────────────────────────────────────────────────────────────────
#  HIT RATE COMPUTER
# ──────────────────────────────────────────────────────────────────────────
# baseline (% of trading days) that each outcome target is hit in general.
# Source: 2-year walk-forward calibration.

BASELINE_RATES: Dict[str, int] = {
    "hit_5pc_5d":   35, "hit_5pc_10d":  45, "hit_7pc_10d":  35,
    "hit_10pc_15d": 30, "hit_10pc_20d": 35, "hit_15pc_20d": 22,
    "hit_20pc_20d": 12,
}

OUTCOME_LABEL: Dict[str, str] = {
    "hit_5pc_5d":   "+5% within 5 days",   "hit_5pc_10d":  "+5% within 10 days",
    "hit_7pc_10d":  "+7% within 10 days",  "hit_10pc_15d": "+10% within 15 days",
    "hit_10pc_20d": "+10% within 20 days", "hit_15pc_20d": "+15% within 20 days",
    "hit_20pc_20d": "+20% within 20 days",
}


def hit_rate_numbers(target: str, lift: float) -> Dict[str, float]:
    """Return raw numbers for frontend charts/sorting (used alongside hit_rate_phrase)."""
    base = BASELINE_RATES.get(target, 30)
    pattern_wr = max(0, min(100, base + lift))
    edge = pattern_wr / base if base > 0 else 0
    return {
        "hit_rate_pct":    round(pattern_wr, 1),
        "baseline_pct":    round(base, 1),
        "edge_multiplier": round(edge, 2),
    }


def hit_rate_phrase(target: str, lift: float) -> str:
    """Return '6 of 10 hit +15% within 20 days (baseline 2 of 10 → 2.9× edge)'."""
    base = BASELINE_RATES.get(target, 30)
    pattern_wr = max(0, min(100, base + lift))
    p10 = round(pattern_wr / 10)
    b10 = round(base / 10)
    edge = pattern_wr / base if base > 0 else 0
    out_label = OUTCOME_LABEL.get(target, target)
    return f"{p10} of 10 hit {out_label} (baseline {b10} of 10 → {edge:.1f}× edge)"


# ──────────────────────────────────────────────────────────────────────────
#  STORY GENERATOR (with the 3 polish fixes applied)
# ──────────────────────────────────────────────────────────────────────────

def generate_story(symbol: str, sector: Optional[str], feat_vals: Dict[str, Any]) -> str:
    """Auto-generate a 2-3 sentence narrative from feature values.

    Polish fixes:
      - Sector word naturalized via SECTOR_NATURAL dict
      - Sentence capitalization via _capitalize_sentences
      - No double periods (story joining via _join_story_bits)
    """
    sma200 = feat_vals.get("dist_sma_200")
    rsi    = feat_vals.get("rsi_14")
    r20    = feat_vals.get("roc_20")
    vol    = feat_vals.get("vol_vs_20d")
    wb     = feat_vals.get("weekly_breakout_20w")
    wcl    = feat_vals.get("weekly_close_loc")
    cl     = feat_vals.get("close_loc")
    d252   = feat_vals.get("dist_high_252")
    d20    = feat_vals.get("dist_high_20")
    rng    = feat_vals.get("weekly_range_pct")

    sector_phrase = _sector_natural(sector)
    bits: List[str] = []

    # Opening: sector + trend state
    if sma200 is not None:
        if   sma200 > 30: bits.append(f"{sector_phrase} in a strong, sustained uptrend (+{sma200:.0f}% above its 200-day average)")
        elif sma200 > 10: bits.append(f"{sector_phrase} in an established uptrend")
        elif sma200 > 0:  bits.append(f"{sector_phrase} above its long-term trend line")
        elif sma200 > -10: bits.append(f"{sector_phrase} near its long-term moving average")
        else:             bits.append(f"{sector_phrase} in a downtrend showing early recovery signs")
    else:
        bits.append(sector_phrase)

    # Recent action
    if r20 is not None and r20 > 20:
        bits.append(f"up {r20:.0f}% in the last 20 days")
    elif r20 is not None and r20 > 0:
        bits.append(f"with {r20:.0f}% gain over 20 days")
    elif r20 is not None and r20 < -5:
        bits.append(f"down {-r20:.0f}% over 20 days but stabilizing")

    # Key signal: prefer breakout > weekly close > volume close
    signal_str: Optional[str] = None
    if wb is not None and wb > 0:
        signal_str = "just confirmed a multi-month breakout above its 20-week high"
    elif wcl is not None and wcl > 0.66 and rng is not None and rng > 10:
        signal_str = f"closed its week powerfully after a wide {rng:.0f}% range"
    elif wcl is not None and wcl > 0.66:
        signal_str = "closed the week in the upper third of its range"
    elif cl is not None and cl > 0.66 and vol is not None and vol > 1.5:
        signal_str = f"closed strong on {vol:.1f}x normal volume (institutional buying)"

    if signal_str:
        bits.append(signal_str)

    # 52-week-high tag
    if d252 is not None and d252 > -3:
        bits.append("sitting at 52-week highs")
    elif d20 is not None and d20 > -2:
        bits.append("at 20-day highs")

    return _join_story_bits(bits)


# ──────────────────────────────────────────────────────────────────────────
#  TIER + EXPECTED OUTCOMES
# ──────────────────────────────────────────────────────────────────────────

def tier_info(rank: int) -> Tuple[str, str, str]:
    """Return (tier_name, icon, description) for a rank."""
    if rank <= 3:   return ("ELITE", "⭐",  "Top-tier conviction. Only ~5% of trading days produce this.")
    if rank <= 7:   return ("HIGH",  "🟢",  "High conviction. Strong historical edge if intraday confirms.")
    if rank <= 14:  return ("MID",   "🟡",  "Decent conviction. Look for intraday volume and close strength.")
    if rank <= 25:  return ("LOWER", "🟠",  "Marginal edge. Only enter with very strong intraday confirmation.")
    return            ("TAIL",  "⚪",  "Tail-end pick. Engine sees the setup but edge is small.")


EXPECTED_BY_TIER: Dict[str, Dict[str, Any]] = {
    "ELITE": {"d5_range": [60, 70], "d10_range": [60, 70], "d15_avg_range": [ 8, 12]},
    "HIGH":  {"d5_range": [50, 65], "d10_range": [55, 65], "d15_avg_range": [ 5,  9]},
    "MID":   {"d5_range": [45, 60], "d10_range": [50, 60], "d15_avg_range": [ 4,  7]},
    "LOWER": {"d5_range": [40, 55], "d10_range": [45, 55], "d15_avg_range": [ 3,  5]},
    "TAIL":  {"d5_range": [35, 50], "d10_range": [40, 50], "d15_avg_range": [ 2,  4]},
}

# Decouple tier emoji from CSS choice — frontend reads tier_color directly.
TIER_COLOR: Dict[str, str] = {
    "ELITE": "amber",
    "HIGH":  "green",
    "MID":   "yellow",
    "LOWER": "orange",
    "TAIL":  "gray",
}

# Plain-English summary of each tier's live-decision rule. Pre-computed
# here so the live decisions endpoint doesn't reconstruct from raw thresholds.
# Mirrors backend/power_user/services/live_tier.py apply_tier_rule().
TIER_ACTION_HINT: Dict[str, str] = {
    "ELITE": "Enter if first 15-min ret > +0.2%",
    "HIGH":  "Enter if first 15-min ret > +0.5% AND vol > 10% of yesterday's total",
    "MID":   "Enter if 15-min close in upper third of range AND vol > 6% of yesterday's",
    "LOWER": "Enter only with strong confirmation (ret > +0.5% AND vol > 10%)",
    "TAIL":  "Skip unless very strong intraday (ret > +0.5%)",
}


# ──────────────────────────────────────────────────────────────────────────
#  ENGINE: COMPUTE TOP-N FOR A DATE
# ──────────────────────────────────────────────────────────────────────────

_pattern_cache: Dict[str, Any] = {"patterns": None, "loaded_at": 0.0}
_pattern_cache_lock = threading.Lock()
_PATTERN_TTL = 300.0   # 5 min — patterns refresh on weekly_remine, hourly bound is fine


def load_patterns(con: sqlite3.Connection, force: bool = False) -> List[Dict[str, Any]]:
    """Load all promoted patterns (excluding drawdown-bounce). Cached 5 min."""
    import time
    with _pattern_cache_lock:
        if not force and _pattern_cache["patterns"] is not None:
            if time.time() - _pattern_cache["loaded_at"] < _PATTERN_TTL:
                return _pattern_cache["patterns"]

        rows = con.execute("""
            SELECT c.pattern_id, c.mined_year, c.rule_json, c.outcome_target,
                   p.avg_oos_year_lift_pp
              FROM falcon_promoted_patterns p
              JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
             WHERE p.classification IN ('universal','regime_dependent')
        """).fetchall()
        out: List[Dict[str, Any]] = []
        for pid, my, rj, tgt, lift in rows:
            rule = [(f, op, float(t)) for f, op, t in json.loads(rj)]
            if in_drawdown_bounce(rule):
                continue
            out.append({"pid": pid, "my": int(my), "rule": rule, "tgt": tgt, "lift": float(lift or 0)})
        _pattern_cache["patterns"]  = out
        _pattern_cache["loaded_at"] = time.time()
        return out


def rule_fires(rule: List[Tuple[str, str, float]], feat_vals: Dict[str, Any]) -> bool:
    """True iff every condition in the rule holds for these feature values."""
    for f, op, t in rule:
        v = feat_vals.get(f)
        if v is None:
            return False
        if op == "<=":
            if not (v <= t):
                return False
        else:
            if not (v > t):
                return False
    return True


def compute_top_n(con: sqlite3.Connection, date_str: str, top_n: int = 100,
                   patterns: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Recompute top-N picks for a given date from features + patterns.

    Used for arbitrary historical replay AND for the live tier tool (where we
    need 100 ranks but signals_live only stores 100 anyway — we recompute for
    consistency between the two paths).
    """
    if patterns is None:
        patterns = load_patterns(con)
    feat_select = ", ".join(FEATURE_COLS)
    rows = con.execute(
        f"SELECT symbol, {feat_select} FROM falcon_features WHERE trade_date = ? ORDER BY symbol",
        (date_str,)
    ).fetchall()
    if not rows:
        return []
    sectors = dict(con.execute("SELECT symbol, sector FROM falcon_sectors").fetchall())
    year = int(date_str[:4])
    results: List[Dict[str, Any]] = []
    for row in rows:
        sym = row[0]
        feat_vals = dict(zip(FEATURE_COLS, row[1:]))
        firing = [p for p in patterns
                  if year > p["my"] and rule_fires(p["rule"], feat_vals)]
        if firing:
            score = sum(p["lift"] for p in firing)
            results.append({
                "symbol":    sym,
                "sector":    sectors.get(sym, ""),
                "score":     score,
                "n_fires":   len(firing),
                "feat_vals": feat_vals,
                "firing":    sorted(firing, key=lambda p: -p["lift"]),
            })
    results.sort(key=lambda r: -r["score"])
    return results[:top_n]


def get_outcomes(con: sqlite3.Connection, sym: str, entry_date: str) -> Dict[str, float]:
    """Return {'d1': pct, 'd3': pct, ...} from ohlc_daily starting at entry_date.

    Keys are lowercase snake-style (`d1`, `d3`, `d5`, `d10`, `d15`) — JSON-safe,
    template-safe, TS-safe. Frontend renders as `D+1` etc.
    """
    rows = con.execute("""
        SELECT trade_date, open, close FROM ohlc_daily
         WHERE symbol = ? AND trade_date >= ? ORDER BY trade_date LIMIT 20
    """, (sym, entry_date)).fetchall()
    if not rows:
        return {}
    entry_open = rows[0][1]
    if not entry_open or entry_open <= 0:
        return {}
    out: Dict[str, float] = {}
    for d in (1, 3, 5, 10, 15):
        if d - 1 < len(rows):
            out[f"d{d}"] = (rows[d - 1][2] / entry_open - 1) * 100
    return out


def aggregate_outcomes(picks_with_outcomes: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute per-horizon WR / hit_5pct / avg_ret across picks.

    Keys are lowercase (`d1`, `d3`, ...) to match get_outcomes shape.
    """
    horizons = ("d1", "d3", "d5", "d10", "d15")
    n_total = 0
    wr     = {h: 0 for h in horizons}
    hit5   = {h: 0 for h in horizons}
    rets: Dict[str, List[float]] = {h: [] for h in horizons}
    for p in picks_with_outcomes:
        o = p.get("actual") or {}
        if not o:
            continue
        n_total += 1
        for h in horizons:
            v = o.get(h)
            if v is None:
                continue
            if v > 0:  wr[h]   += 1
            if v >= 5: hit5[h] += 1
            rets[h].append(v)
    out: Dict[str, Any] = {"n_picks": n_total, "horizons": {}}
    if n_total == 0:
        return out
    for h in horizons:
        if rets[h]:
            out["horizons"][h] = {
                "wr":       round(wr[h]   / n_total * 100, 1),
                "hit_5pct": round(hit5[h] / n_total * 100, 1),
                "avg_ret":  round(float(np.mean(rets[h])),  2),
            }
    return out


# ──────────────────────────────────────────────────────────────────────────
#  CANONICAL PICK PAYLOAD — single source of truth shape (Design.md §5)
# ──────────────────────────────────────────────────────────────────────────

PICK_SCHEMA_VERSION = 2
"""Bumped whenever the payload shape changes. Old clients can detect mismatch.

Change log:
  v1 (2026-05-14): initial — locked after operator review with 10 fixes.
  v2 (2026-06-18): additive — signal-time tiering. Adds optional keys
    signal_tier, signal_tier_reason, signal_tier_color, signal_day_ret_pct,
    two_day_ret_pct (populated by signal_tier.enrich_picks). All optional, so
    callers that don't enrich still produce valid payloads.
"""

# The exact key set every Pick payload MUST have. Validated by validate_pick_payload
# at the boundary of every router that returns picks (single chokepoint = zero drift).
PICK_REQUIRED_KEYS = frozenset({
    "_version", "rank", "symbol", "sector",
    "signal_date", "entry_date",
    "score", "n_fires",
    "tier", "tier_icon", "tier_color", "tier_desc", "tier_action_hint",
    "story", "top_patterns", "expected", "risk",
})
PICK_OPTIONAL_KEYS = frozenset({
    "actual",
    # v2 signal-time tiering (additive; populated by signal_tier.enrich_picks)
    "signal_tier", "signal_tier_reason", "signal_tier_color",
    "signal_day_ret_pct", "two_day_ret_pct",
})

# Enum for the optional signal-time tier (distinct axis from the rank `tier`).
PICK_SIGNAL_TIER_ENUM = frozenset({
    "PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup",
    "GOLD", "GOLD-baseline", "STANDARD", "STANDARD-weak", "AVOID",
})

PICK_PATTERN_REQUIRED_KEYS = frozenset({
    "position", "pattern_id", "trader_phrase", "hit_phrase",
    "hit_rate_pct", "baseline_pct", "edge_multiplier",
    "target", "oos_lift_pp", "mined_year",
})

PICK_EXPECTED_REQUIRED_KEYS = frozenset({"d5_range", "d10_range", "d15_avg_range"})

PICK_RISK_REQUIRED_KEYS = frozenset({"stop_loss_pct", "trail_trigger_pct", "time_exit_days"})

PICK_ACTUAL_KEYS = frozenset({"d1", "d3", "d5", "d10", "d15"})


def validate_pick_payload(p: Dict[str, Any]) -> None:
    """Assert that `p` matches the Pick v1 contract. Raises ValueError on any drift.

    Call this at the boundary of every router that returns picks. Single
    chokepoint prevents the entire system from drifting away from the locked
    payload shape — if someone hand-constructs a pick dict in a router instead
    of calling build_pick_payload, this catches it at CI time.

    Args:
        p — a single Pick dict (the one returned by build_pick_payload)

    Raises:
        ValueError with the specific mismatch — never silently passes a broken
        payload through to the frontend.
    """
    if not isinstance(p, dict):
        raise ValueError(f"pick payload must be dict, got {type(p).__name__}")

    # 1. Version match
    v = p.get("_version")
    if v != PICK_SCHEMA_VERSION:
        raise ValueError(f"_version mismatch: payload has {v}, expected {PICK_SCHEMA_VERSION}")

    # 2. Required top-level keys
    keys = set(p.keys())
    missing = PICK_REQUIRED_KEYS - keys
    if missing:
        raise ValueError(f"pick missing required keys: {sorted(missing)}")
    extra = keys - PICK_REQUIRED_KEYS - PICK_OPTIONAL_KEYS
    if extra:
        raise ValueError(f"pick has unexpected keys: {sorted(extra)} "
                         f"(bump PICK_SCHEMA_VERSION if intended)")

    # 3. Nested shape: top_patterns
    if not isinstance(p["top_patterns"], list):
        raise ValueError(f"top_patterns must be list, got {type(p['top_patterns']).__name__}")
    if len(p["top_patterns"]) > 3:
        raise ValueError(f"top_patterns must be at most 3 entries, got {len(p['top_patterns'])}")
    for i, tp in enumerate(p["top_patterns"]):
        tp_missing = PICK_PATTERN_REQUIRED_KEYS - tp.keys()
        if tp_missing:
            raise ValueError(f"top_patterns[{i}] missing keys: {sorted(tp_missing)}")

    # 4. Nested shape: expected
    exp = p["expected"]
    exp_missing = PICK_EXPECTED_REQUIRED_KEYS - exp.keys()
    if exp_missing:
        raise ValueError(f"expected missing keys: {sorted(exp_missing)}")
    for k in ("d5_range", "d10_range", "d15_avg_range"):
        rng = exp[k]
        if not (isinstance(rng, list) and len(rng) == 2):
            raise ValueError(f"expected.{k} must be [low, high] list, got {rng!r}")

    # 5. Nested shape: risk
    risk_missing = PICK_RISK_REQUIRED_KEYS - p["risk"].keys()
    if risk_missing:
        raise ValueError(f"risk missing keys: {sorted(risk_missing)}")

    # 6. Tier enum (matches the 5 display tiers — DEEP-TAIL collapses to TAIL)
    if p["tier"] not in {"ELITE", "HIGH", "MID", "LOWER", "TAIL"}:
        raise ValueError(f"tier {p['tier']!r} not in display set "
                         f"(internal DEEP-TAIL must be collapsed to TAIL)")

    # 6b. Signal-time tier enum (optional, v2). Distinct from the rank `tier`.
    if p.get("signal_tier") is not None and p["signal_tier"] not in PICK_SIGNAL_TIER_ENUM:
        raise ValueError(f"signal_tier {p['signal_tier']!r} not in "
                         f"{sorted(PICK_SIGNAL_TIER_ENUM)}")

    # 7. actual outcomes (optional) — keys must be lowercase d-prefixed
    if "actual" in p:
        actual = p["actual"]
        bad = set(actual.keys()) - PICK_ACTUAL_KEYS
        if bad:
            raise ValueError(f"actual has unexpected keys: {sorted(bad)} "
                             f"(must be lowercase: d1, d3, d5, d10, d15)")


def build_pick_payload(rank: int,
                       pick: Dict[str, Any],
                       outcomes: Optional[Dict[str, float]] = None,
                       signal_date: Optional[str] = None,
                       entry_date: Optional[str] = None) -> Dict[str, Any]:
    """Build the canonical Pick payload — see SPEC/Design.md §5.

    Single source of truth: routers, frontend, replay cache, live tier — all
    consume this exact dict. Schema is versioned via PICK_SCHEMA_VERSION.

    Args:
        rank        — 1-based rank in the day's pick list
        pick        — dict from compute_top_n with keys: symbol, sector, score,
                      n_fires, feat_vals, firing (list of patterns)
        outcomes    — optional {d1, d3, d5, d10, d15} from get_outcomes()
        signal_date — date the signal was emitted (e.g. "2026-05-14")
        entry_date  — next trading day (e.g. "2026-05-15")
    """
    tier, icon, tier_desc = tier_info(rank)
    fv  = pick["feat_vals"]
    exp = EXPECTED_BY_TIER[tier]

    top_patterns: List[Dict[str, Any]] = []
    for i, p in enumerate(pick["firing"][:3], 1):
        nums = hit_rate_numbers(p["tgt"], p["lift"])
        top_patterns.append({
            "position":         i,                                    # 1-3 within top_patterns
            "pattern_id":       f"p_{p['pid']}",                       # prefixed string ID
            "trader_phrase":    rule_to_trader_phrase(p["rule"]),
            "hit_phrase":       hit_rate_phrase(p["tgt"], p["lift"]),
            "hit_rate_pct":     nums["hit_rate_pct"],                  # raw % for charts/sort
            "baseline_pct":     nums["baseline_pct"],
            "edge_multiplier":  nums["edge_multiplier"],
            "target":           p["tgt"],
            "oos_lift_pp":      round(p["lift"], 2),                   # renamed from oos_lift
            "mined_year":       p["my"],
        })

    payload: Dict[str, Any] = {
        "_version":         PICK_SCHEMA_VERSION,
        "rank":             rank,
        "symbol":           pick["symbol"],
        "sector":           pick["sector"] or None,                    # null when missing
        "signal_date":      signal_date,
        "entry_date":       entry_date,
        "score":            round(pick["score"], 2),
        "n_fires":          pick["n_fires"],
        "tier":             tier,
        "tier_icon":        icon,
        "tier_color":       TIER_COLOR[tier],
        "tier_desc":        tier_desc,
        "tier_action_hint": TIER_ACTION_HINT[tier],
        # v2 signal-time tiering (None unless signal_tier.enrich_picks ran)
        "signal_tier":        pick.get("signal_tier"),
        "signal_tier_reason": pick.get("signal_tier_reason"),
        "signal_tier_color":  pick.get("signal_tier_color"),
        "signal_day_ret_pct": pick.get("signal_day_ret_pct"),
        "two_day_ret_pct":    pick.get("two_day_ret_pct"),
        "story":            generate_story(pick["symbol"], pick["sector"], fv),
        "top_patterns":     top_patterns,
        "expected": {
            "d5_range":      exp["d5_range"],
            "d10_range":     exp["d10_range"],
            "d15_avg_range": exp["d15_avg_range"],                     # list [low, high], not string
        },
        "risk": {
            "stop_loss_pct":     -7,
            "trail_trigger_pct": 12,                                   # renamed for consistency
            "time_exit_days":     7,
        },
    }
    if outcomes:
        # Outcome keys are already lowercased (d1, d3, ...) by get_outcomes;
        # accept legacy uppercase form too for one release of safety net.
        normalized: Dict[str, float] = {}
        for k, v in outcomes.items():
            key = k.lower().replace("+", "")    # "D+1" → "d1", "d1" → "d1"
            normalized[key] = round(v, 2)
        payload["actual"] = normalized

    # Self-validate at construction time so a broken build_pick_payload never
    # leaves this function. Cheap (~10us) — keeps routers honest without
    # them needing to call validate themselves.
    validate_pick_payload(payload)
    return payload
