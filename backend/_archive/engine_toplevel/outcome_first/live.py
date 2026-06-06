from __future__ import annotations

from collections import defaultdict
from typing import Any

from engine.outcome_first.features import build_behavior_rows
from engine.outcome_first.trust import apply_trust


def rank_live_outcome_opportunities(
    learned_patterns: list[dict[str, Any]],
    ohlcv_by_stock: dict[tuple[str, str], list[dict[str, Any]]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    live_by_stock: dict[tuple[str, str], dict[str, Any]] = {}
    for key, rows in ohlcv_by_stock.items():
        behavior = build_behavior_rows(rows)
        if behavior:
            live_by_stock[key] = behavior[-1]

    scored = []
    min_overlap = float(config["outcome_first"]["similarity_min_overlap"])
    for pattern in learned_patterns:
        live = live_by_stock.get((pattern["market"], pattern["ticker"]))
        if not live:
            continue
        pattern_atoms = set(str(pattern["behavior_pattern"]).split(" + "))
        live_atoms = set(live["behavior_atoms"])
        overlap = len(pattern_atoms & live_atoms) / len(pattern_atoms) if pattern_atoms else 0.0
        if overlap < min_overlap:
            continue
        score = (
            0.45 * float(pattern["opportunity_score"])
            + 0.25 * overlap
            + 0.15 * min(3.0, float(pattern["lift"])) / 3.0
            + 0.15 * float(pattern["recent_probability"])
        )
        row = apply_trust(pattern)
        row.update(
            {
                "latest_date": live["trade_date"],
                "current_close": live["close"],
                "current_behavior": live["coarse_behavior"],
                "current_atoms": ", ".join(live["behavior_atoms"]),
                "similarity": overlap,
                "decision_score": max(0.0, min(1.0, score)),
                "setup_summary": _summary(pattern, live, overlap),
            }
        )
        scored.append(row)

    scored.sort(
        key=lambda r: (
            -float(r["decision_score"]),
            -float(r["target_probability"]),
            -float(r["lift"]),
            -int(r["occurrences"]),
        )
    )
    return _limit_per_market(scored, int(config["outcome_first"]["top_opportunities_per_market"]))


def _limit_per_market(rows: list[dict[str, Any]], per_market: int) -> list[dict[str, Any]]:
    counts: dict[str, int] = defaultdict(int)
    stock_seen: set[tuple[str, str, str, float, int]] = set()
    out = []
    for row in rows:
        if counts[row["market"]] >= per_market:
            continue
        key = (row["market"], row["ticker"], row["direction"], float(row["target_move"]), int(row["forward_window"]))
        if key in stock_seen:
            continue
        stock_seen.add(key)
        counts[row["market"]] += 1
        out.append(row)
    return out


def _summary(pattern: dict[str, Any], live: dict[str, Any], similarity: float) -> str:
    trusted_pattern = apply_trust(pattern)
    move = f"+{float(pattern['target_move']):.0%}" if pattern["direction"] == "rally" else f"-{float(pattern['target_move']):.0%}"
    verb = "rally" if pattern["direction"] == "rally" else "fall"
    side = "upside" if pattern["direction"] == "rally" else "downside"
    display_prob = float(trusted_pattern.get("display_probability", trusted_pattern["target_probability"]))
    raw_prob = float(trusted_pattern["target_probability"])
    baseline = float(pattern["baseline_probability"])
    ci_low = float(trusted_pattern.get("probability_ci_low", 0.0))
    ci_high = float(trusted_pattern.get("probability_ci_high", 0.0))
    credibility = trusted_pattern.get("credibility", "exploratory")
    state = plain_state(str(live["coarse_behavior"]))
    caveat = credibility_phrase(str(credibility), int(float(pattern["occurrences"])))
    return (
        f"{pattern['ticker']} is showing a familiar {side} setup. "
        f"The stock is currently {state}, and today's behavior matches {similarity:.0%} of a pattern "
        f"that has appeared before its own {move} moves. In the past, similar conditions reached that "
        f"target within {int(pattern['forward_window'])} trading days {raw_prob:.1%} of the time, versus "
        f"a normal baseline of {baseline:.1%}. After sample-size adjustment, Kanida's conservative read is "
        f"{display_prob:.1%}, with a 95% confidence band of {ci_low:.1%}-{ci_high:.1%}. "
        f"Average directional follow-through was {float(pattern['avg_forward_return']):.2%}. "
        f"{caveat}"
    )


def plain_state(state: str) -> str:
    parts = state.split("|")
    labels = {
        "strong_up": "in a strong short-term uptrend",
        "up": "in an uptrend",
        "sideways": "moving sideways",
        "strong_down": "coming out of a sharp decline",
        "down": "in a short-term downtrend",
        "up_continuation": "continuing higher",
        "up_pullback": "pulling back inside an uptrend",
        "down_continuation": "still under pressure",
        "down_reversal_attempt": "attempting a reversal from weakness",
        "range_move": "moving inside its range",
        "compressed": "with compressed volatility",
        "expanded": "with expanded volatility",
        "normal_vol": "with normal volatility",
        "above_stacked_ma": "above a healthy moving-average stack",
        "above_key_ma": "above key moving averages",
        "below_stacked_ma": "below key moving averages",
        "below_key_ma": "below key moving averages",
        "ma_mixed": "mixed around moving averages",
        "breakout": "near a breakout",
        "breakdown": "near a breakdown",
        "inside_range": "still inside its recent range",
        "range_expansion_up": "expanding upward",
        "range_expansion_down": "expanding downward",
    }
    readable = [labels.get(p, p.replace("_", " ")) for p in parts if p]
    if not readable:
        return "in an unclear technical state"
    return ", ".join(readable[:3])


def credibility_phrase(credibility: str, occurrences: int) -> str:
    if credibility == "strong":
        return f"The evidence base is strong with {occurrences} historical matches, so this deserves priority monitoring."
    if credibility == "solid":
        return f"The evidence base is solid with {occurrences} historical matches, but confirmation from price action still matters."
    if credibility == "thin_but_interesting":
        return f"The edge is interesting but still sample-sensitive with {occurrences} historical matches, so treat it as a watchlist candidate rather than a guarantee."
    return f"This is exploratory evidence from {occurrences} historical matches and should be used for discovery, not blind action."
