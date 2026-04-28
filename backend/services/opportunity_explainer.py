"""
Opportunity explainer service.
Converts a raw live_opportunity row into a structured, human-readable explanation.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Optional

_HERE = Path(__file__).parent
DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"),
)

# Human-readable labels for common atom keys
_ATOM_LABELS: dict[str, dict[str, str]] = {
    "trend_20": {
        "strong_up":    "strong uptrend (20d)",
        "up":           "uptrend (20d)",
        "sideways":     "sideways (20d)",
        "down":         "downtrend (20d)",
        "strong_down":  "strong downtrend (20d)",
    },
    "trend_50": {
        "strong_up":    "strong uptrend (50d)",
        "up":           "uptrend (50d)",
        "sideways":     "sideways (50d)",
        "down":         "downtrend (50d)",
        "strong_down":  "strong downtrend (50d)",
    },
    "volatility": {
        "low_vol":      "low volatility",
        "normal_vol":   "normal volatility",
        "high_vol":     "elevated volatility",
        "very_high_vol":"very high volatility",
    },
    "volume": {
        "volume_rising":  "rising volume",
        "volume_falling": "falling volume",
        "volume_flat":    "flat volume",
        "volume_spike":   "volume spike",
    },
    "breakout_state": {
        "inside_range":   "inside consolidation range",
        "near_breakout":  "near breakout level",
        "broke_out":      "recent breakout",
        "failed_break":   "failed breakout",
    },
}

_CREDIBILITY_DESC = {
    "strong":            "Strong — large sample size, high statistical confidence",
    "solid":             "Solid — adequate sample with consistent results",
    "thin_but_interesting": "Thin but interesting — limited occurrences, treat as a hypothesis",
    "exploratory":       "Exploratory — very few occurrences, informational only",
}

_TIER_DESC = {
    "high_conviction": "High-conviction setup",
    "medium":          "Medium-conviction setup",
    "exploratory":     "Exploratory / watchlist",
}


def _parse_pattern(behavior_pattern: str) -> list[str]:
    """Break 'key:value + key:value' pattern string into human phrases."""
    phrases = []
    for part in behavior_pattern.split("+"):
        part = part.strip()
        if ":" not in part:
            phrases.append(part)
            continue
        key, val = part.split(":", 1)
        key, val = key.strip(), val.strip()
        label = _ATOM_LABELS.get(key, {}).get(val)
        phrases.append(label if label else f"{key}: {val}")
    return phrases


def explain_opportunity(opp: dict) -> dict:
    """
    Given a dict from live_opportunities, return a structured explanation:
    {
        pattern_phrases: [...],
        win_rate_sentence: str,
        direction_sentence: str,
        credibility_label: str,
        tier_label: str,
        risk_context: str,
        summary: str,       ← setup_summary if present, else generated
    }
    """
    direction   = opp.get("direction", "rally")
    ticker      = opp.get("ticker", "")
    win_rate    = opp.get("display_probability", 0)
    occurrences = opp.get("occurrences", 0)
    target_move = opp.get("target_move", 0)
    fwd_window  = opp.get("forward_window", 0)
    lift        = opp.get("lift", 1.0)
    credibility = opp.get("credibility", "")
    tier        = opp.get("tier", "")
    behavior    = opp.get("behavior_pattern", "")
    setup_sum   = opp.get("setup_summary", "")

    direction_word = "bullish" if direction in ("rally", "long") else "bearish"
    move_pct       = round((target_move or 0) * 100, 1)

    pattern_phrases = _parse_pattern(behavior) if behavior else []

    win_rate_pct = round((win_rate or 0) * 100, 0)
    win_rate_sentence = (
        f"{int(win_rate_pct)}% historical win rate across {occurrences} occurrences "
        f"({lift:.1f}x above baseline)."
    )

    direction_sentence = (
        f"{ticker} is showing a {direction_word} setup "
        f"targeting {move_pct}% over {fwd_window} trading days."
    )

    risk_context = (
        f"Pattern fires in a {move_pct}% window — "
        f"position sizing should account for {'low' if move_pct < 3 else 'moderate' if move_pct < 7 else 'high'} "
        f"expected move magnitude."
    )

    summary = setup_sum if setup_sum else f"{direction_sentence} {win_rate_sentence}"

    return {
        "pattern_phrases":    pattern_phrases,
        "win_rate_sentence":  win_rate_sentence,
        "direction_sentence": direction_sentence,
        "credibility_label":  _CREDIBILITY_DESC.get(credibility, credibility),
        "tier_label":         _TIER_DESC.get(tier, tier),
        "risk_context":       risk_context,
        "summary":            summary,
    }


def explain_ticker(ticker: str, market: str = "NSE") -> Optional[dict]:
    """Fetch the latest live_opportunity for a ticker and return its explanation."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT lo.*
            FROM live_opportunities lo
            JOIN snapshot_runs sr ON sr.id = lo.snapshot_run_id
            WHERE lo.ticker = ? AND lo.market = ?
            ORDER BY sr.run_date DESC, lo.opportunity_score DESC
            LIMIT 1
            """,
            (ticker, market),
        ).fetchone()

    if not row:
        return None

    return explain_opportunity(dict(row))
