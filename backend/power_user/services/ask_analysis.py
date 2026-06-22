"""Ask-Falcon per-stock analysis — read-only, honest, derived from real data.

Powers GET /api/power/ask/analyze-stock. Builds a trader-friendly analysis for
ANY covered stock (present in falcon_features) from:

  - the latest falcon_features row     → trend / momentum / volume / range
  - signal-tier (signal_tier service)  → GOLD / ENTERPRISE / PREMIUM / STANDARD / AVOID
  - firing promoted patterns           → avg_lift + pattern observations (explainer)
  - falcon_sectors                      → sector
  - explainer.generate_story            → human-readable narrative

NEVER fabricates. Every field is either derived from a real row or returned as
None with a clear flag. If the symbol isn't in falcon_features the caller
returns 404 NOT_COVERED.

This module touches ONLY read paths (falcon_features, ohlc_daily, falcon_sectors,
the promoted-pattern catalog). It never goes near the auto-trade execution path.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from .feature_cols import FEATURE_COLS
from .explainer import generate_story, load_patterns, rule_fires, rule_to_trader_phrase
from .signal_tier import (
    TIER_COLOR,
    TIER_REASON,
    _signal_day_features,
    classify_from_rulebook,
    classify_signal_tier,
    load_active_rulebook,
)

log = logging.getLogger("kanida.power_user.ask_analysis")


def _f(v: Any) -> Optional[float]:
    """Coerce to float; return None on None/NaN/non-numeric (honest nulls)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:   # NaN
        return None
    return f


# ── Plain-English derivations from the real feature row ─────────────────────

def _current_trend(fv: Dict[str, Any]) -> str:
    """Trend read from 200-day distance + 20-day slope. No invention."""
    d200 = _f(fv.get("dist_sma_200"))
    slope20 = _f(fv.get("slope_sma_20"))
    if d200 is None:
        return "Trend unavailable (insufficient history for the 200-day average)."
    if d200 > 20:
        base = f"Strong, sustained uptrend — trading {d200:.0f}% above its 200-day average."
    elif d200 > 5:
        base = f"In an established uptrend — {d200:.0f}% above its 200-day average."
    elif d200 > 0:
        base = "Just above its long-term (200-day) trend line."
    elif d200 > -10:
        base = f"Sitting near its long-term average ({d200:.0f}% vs 200-day)."
    else:
        base = f"In a downtrend — {abs(d200):.0f}% below its 200-day average."
    if slope20 is not None:
        if slope20 > 0:
            base += " Short-term (20-day) trend is rising."
        elif slope20 < 0:
            base += " Short-term (20-day) trend is still falling."
    return base


def _recent_price_movement(fv: Dict[str, Any]) -> str:
    """20-day + 5-day return read from roc features."""
    r20 = _f(fv.get("roc_20"))
    r5 = _f(fv.get("roc_5"))
    rsi = _f(fv.get("rsi_14"))
    bits: List[str] = []
    if r20 is not None:
        if r20 > 10:
            bits.append(f"Up {r20:.0f}% over the last 20 days")
        elif r20 > 0:
            bits.append(f"Up {r20:.0f}% over the last 20 days (controlled)")
        elif r20 < -5:
            bits.append(f"Down {abs(r20):.0f}% over the last 20 days")
        else:
            bits.append(f"Roughly flat over 20 days ({r20:+.0f}%)")
    if r5 is not None:
        bits.append(f"{r5:+.0f}% in the last 5 days")
    if rsi is not None:
        if rsi >= 70:
            bits.append(f"RSI {rsi:.0f} (overbought)")
        elif rsi >= 60:
            bits.append(f"RSI {rsi:.0f} (strong momentum)")
        elif rsi <= 30:
            bits.append(f"RSI {rsi:.0f} (oversold)")
        else:
            bits.append(f"RSI {rsi:.0f}")
    return ". ".join(bits) + "." if bits else "Recent price movement unavailable."


def _recent_volume_behavior(fv: Dict[str, Any]) -> str:
    """Volume vs 20-day average + dry-up count. The dry-up lever is real edge."""
    vv = _f(fv.get("vol_vs_20d"))
    dry7 = _f(fv.get("n_sub_75v_7d"))
    bits: List[str] = []
    if vv is not None:
        if vv >= 2.5:
            bits.append(f"Very heavy volume — {vv:.1f}x the 20-day average (strong participation)")
        elif vv >= 1.5:
            bits.append(f"Heavy volume — {vv:.1f}x the 20-day average")
        elif vv >= 1.0:
            bits.append(f"Above-average volume ({vv:.1f}x the 20-day average)")
        elif vv <= 0.5:
            bits.append(f"Very dry volume — only {vv:.1f}x the 20-day average (compression)")
        else:
            bits.append(f"Below-average volume ({vv:.1f}x the 20-day average)")
    if dry7 is not None and dry7 >= 3:
        bits.append(f"{int(dry7)} of the last 7 days came in below 75% of normal volume "
                    "(volume dry-up — historically a positive signal)")
    return ". ".join(bits) + "." if bits else "Recent volume behavior unavailable."


def _signal_day_movement(sret: Optional[float], twoday: Optional[float],
                         rng: Optional[float]) -> str:
    """Signal-day price action — the same numbers the tier is derived from."""
    bits: List[str] = []
    if sret is not None:
        if sret > 10:
            bits.append(f"Extended on its signal day ({sret:+.1f}%)")
        elif sret > 5:
            bits.append(f"Up strongly on its signal day ({sret:+.1f}%)")
        elif sret >= -2:
            bits.append(f"Flat-to-quiet on its signal day ({sret:+.1f}%)")
        else:
            bits.append(f"Fell into the signal ({sret:+.1f}% on the signal day)")
    if twoday is not None:
        bits.append(f"{twoday:+.1f}% over the prior two days")
    if rng is not None:
        if rng < 2:
            bits.append("tight intraday range (coiling)")
        elif rng > 6:
            bits.append("wide intraday range")
    return ". ".join(bits) + "." if bits else "Signal-day movement unavailable."


def _sector_performance(fv: Dict[str, Any], sector: Optional[str]) -> str:
    """Relative strength vs sector + market — honest about missing RS data."""
    rss = _f(fv.get("rs_sector_20d"))
    rsm = _f(fv.get("rs_market_20d"))
    sec = sector or "its sector"
    if rss is None and rsm is None:
        return (f"Relative-strength data vs {sec} is not available for this symbol "
                "in the latest feature row.")
    bits: List[str] = []
    if rss is not None:
        if rss > 5:
            bits.append(f"Outperforming {sec} by {rss:.0f}% over 20 days")
        elif rss < -5:
            bits.append(f"Underperforming {sec} by {abs(rss):.0f}% over 20 days")
        else:
            bits.append(f"Roughly in line with {sec} ({rss:+.0f}% RS, 20d)")
    if rsm is not None:
        if rsm > 5:
            bits.append(f"outperforming the broad market ({rsm:+.0f}%, 20d)")
        elif rsm < -5:
            bits.append(f"lagging the broad market ({rsm:+.0f}%, 20d)")
    return ". ".join(bits) + "." if bits else f"Relative strength vs {sec} unavailable."


def _falcon_pattern_observations(firing: List[Dict[str, Any]]) -> str:
    """Plain-English summary of the promoted patterns currently firing on this
    stock. Empty list → say so honestly (no patterns is a real, valid answer)."""
    if not firing:
        return ("No promoted Falcon patterns are firing on this stock right now. "
                "It is covered by the engine but not flagged today.")
    phrases: List[str] = []
    seen = set()
    for p in firing[:3]:
        ph = rule_to_trader_phrase(p["rule"])
        if ph and ph not in seen:
            phrases.append(ph)
            seen.add(ph)
    n = len(firing)
    lead = (f"{n} promoted Falcon pattern{'s' if n != 1 else ''} firing"
            if n else "No patterns firing")
    if phrases:
        return f"{lead}. Key reads: " + "; ".join(phrases) + "."
    return f"{lead}."


def _entry_context(tier: str, fv: Dict[str, Any], n_fires: int) -> str:
    """Trader-friendly framing of what the tier + confluence means for entry."""
    if tier.startswith("PREMIUM"):
        return ("Highest-quality signal-time setup (bounce-from-drawdown / tight coil on a "
                "strong pattern). These have shown the best historical win-rates, but the "
                "2026 walk-forward sample is thin — size accordingly.")
    if tier.startswith("ENTERPRISE"):
        return ("Flat/down on drying-up volume — the volume dry-up is the real edge here. "
                "Wait for intraday confirmation before committing full size.")
    if tier.startswith("GOLD"):
        return ("Flat/down signal day on light turnover — a constructive, above-average setup. "
                "Confirm with an intraday close in the upper part of the range.")
    if tier.startswith("STANDARD"):
        return ("Average-edge setup. Already up on the signal day — demand strong intraday "
                "confirmation and keep stops tight.")
    if tier == "AVOID":
        return ("Extended / heavy-turnover (froth). Historically the weakest win-rate band — "
                "the engine sees it but the edge is poor. Caution.")
    return "Standard entry rules apply: confirm intraday strength before entry."


def _risk_warnings(fv: Dict[str, Any], sret: Optional[float], tier: str) -> List[str]:
    """Derive risk warnings from the REAL features. Each entry is data-backed."""
    warns: List[str] = []
    d252 = _f(fv.get("dist_high_252"))
    roc20 = _f(fv.get("roc_20"))
    vv = _f(fv.get("vol_vs_20d"))
    rsi = _f(fv.get("rsi_14"))

    # Overextension: at/near 52-week high AND a big recent run
    if d252 is not None and d252 > -3 and roc20 is not None and roc20 > 20:
        warns.append(f"Overextended: at 52-week highs after a {roc20:.0f}% 20-day run — "
                     "pullback risk is elevated.")
    elif sret is not None and sret > 10:
        warns.append(f"Extended on the signal day ({sret:+.1f}%) — chasing strength carries "
                     "higher stop-out risk.")

    # Froth: extended + heavy turnover
    if sret is not None and sret > 7 and vv is not None and vv >= 1.5:
        warns.append("Up sharply on heavy volume — possible froth / blow-off; historical "
                     "win-rate is weaker in this band.")

    # Weak volume on a move
    if vv is not None and vv < 0.5 and roc20 is not None and roc20 > 5:
        warns.append("Recent gains came on thin volume — moves without participation can fade.")

    # Overbought
    if rsi is not None and rsi >= 75:
        warns.append(f"RSI {rsi:.0f} is overbought — short-term mean-reversion risk.")

    # Downtrend caution
    if _f(fv.get("dist_sma_200")) is not None and _f(fv.get("dist_sma_200")) < -10:
        warns.append("Trading well below the 200-day average — this is a counter-trend / "
                     "recovery play, not a momentum continuation.")

    if tier == "AVOID" and not warns:
        warns.append("Classified AVOID at signal time (extended / heavy turnover) — low "
                     "historical win-rate.")
    return warns


def build_analysis(prod_con: sqlite3.Connection, symbol: str) -> Optional[Dict[str, Any]]:
    """Build the per-stock analysis dict, or None if the symbol is NOT covered
    (absent from falcon_features). Read-only; never raises on missing sub-data —
    fields degrade to None / honest-flag strings instead.
    """
    symbol = symbol.strip().upper()

    # 1. Latest feature row — also our coverage gate.
    feat_select = ", ".join(FEATURE_COLS)
    row = prod_con.execute(
        f"SELECT trade_date, {feat_select} FROM falcon_features "
        "WHERE symbol = ? ORDER BY trade_date DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if row is None:
        return None   # caller → 404 NOT_COVERED

    as_of = row[0]
    fv: Dict[str, Any] = dict(zip(FEATURE_COLS, row[1:]))

    # 2. Sector (no company-name table → name == symbol, honest).
    srow = prod_con.execute(
        "SELECT sector FROM falcon_sectors WHERE symbol = ?", (symbol,)
    ).fetchone()
    sector = srow[0] if srow and srow[0] else None

    # 3. Signal-day price/volume features from ohlc_daily (look-ahead safe;
    #    last bar == as_of, same windows enrich_picks uses).
    bars = prod_con.execute(
        "SELECT trade_date, close, high, low, volume FROM ohlc_daily "
        "WHERE symbol = ? AND trade_date <= ? ORDER BY trade_date",
        (symbol, as_of),
    ).fetchall()
    bar_dicts = [{"close": b[1], "high": b[2], "low": b[3], "volume": b[4]}
                 for b in bars][-260:]
    sd_feats = _signal_day_features(bar_dicts) if len(bar_dicts) >= 2 else {}
    sret = _f(sd_feats.get("signal_day_ret_pct"))
    twoday = _f(sd_feats.get("two_day_ret_pct"))
    rng = _f(sd_feats.get("range_pct"))
    trend3_20 = _f(sd_feats.get("trend3_20"))
    turn_pct = _f(sd_feats.get("turn_pct"))

    # 4. Firing promoted patterns → avg_lift (same engine as compute_top_n).
    patterns = load_patterns(prod_con)
    year = int(as_of[:4])
    firing = [p for p in patterns
              if year > p["my"] and rule_fires(p["rule"], fv)]
    firing.sort(key=lambda p: -p["lift"])
    n_fires = len(firing)
    avg_lift = (sum(p["lift"] for p in firing) / n_fires) if n_fires else None

    # 5. Signal tier — prefer the learned rulebook, fall back to code (parity
    #    with picks panel / live tier).
    rules = load_active_rulebook(prod_con)
    if rules:
        tier = classify_from_rulebook(
            {"sret": sret, "twoday": twoday, "rng": rng, "avg_lift": avg_lift,
             "trend3_20": trend3_20, "turn_pct": turn_pct},
            rules)
    else:
        tier = classify_signal_tier(sret, twoday, rng, avg_lift, trend3_20, turn_pct)
    tier_reason = TIER_REASON.get(tier)
    tier_color = TIER_COLOR.get(tier, "gray")

    # 6. Narrative — reuse the production story generator.
    explanation = generate_story(symbol, sector, fv)

    return {
        "symbol":                 symbol,
        "name":                   symbol,          # honest: no company-name table
        "name_is_symbol":         True,            # explicit flag for the frontend
        "sector":                 sector,
        "as_of":                  as_of,
        "current_trend":          _current_trend(fv),
        "recent_price_movement":  _recent_price_movement(fv),
        "recent_volume_behavior": _recent_volume_behavior(fv),
        "signal_day_movement":    _signal_day_movement(sret, twoday, rng),
        "sector_performance":     _sector_performance(fv, sector),
        "falcon_pattern_observations": _falcon_pattern_observations(firing),
        "tier":                   tier,
        "tier_reason":            tier_reason,
        "tier_color":             tier_color,
        "entry_context":          _entry_context(tier, fv, n_fires),
        "risk_warnings":          _risk_warnings(fv, sret, tier),
        "explanation":            explanation,
        # Honest raw numbers (null where not derivable) — lets the frontend
        # render chips without re-deriving or guessing.
        "n_patterns_firing":      n_fires,
        "avg_lift":               round(avg_lift, 2) if avg_lift is not None else None,
        "signal_day_ret_pct":     round(sret, 2) if sret is not None else None,
        "two_day_ret_pct":        round(twoday, 2) if twoday is not None else None,
    }
