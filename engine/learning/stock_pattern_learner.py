from __future__ import annotations

from typing import Any

from engine.learning.roster_classifier import classify_pattern


FIELD_NAMES = [
    "market",
    "ticker",
    "timeframe",
    "bias",
    "pattern_type",
    "pattern_size",
    "pattern",
    "trend_state",
    "weekly_context",
    "market_context",
    "sector_context",
    "volatility_regime",
    "sector",
    "occurrences",
    "win_rate",
    "raw_avg_directional_return",
    "avg_directional_return",
    "weighted_directional_return",
    "recent_win_rate",
    "recent_avg_directional_return",
    "return_stddev",
    "stability",
    "decay_winrate_delta",
    "decay_return_delta",
    "decay_flag",
    "min_support",
    "evidence_score",
    "roster_state",
]


def summarize_stats(stats: dict[tuple[Any, ...], Any], config: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, stat in stats.items():
        (
            market,
            ticker,
            timeframe,
            bias,
            pattern_type,
            pattern_size,
            pattern,
            trend_state,
            weekly_context,
            market_context,
            sector_context,
            volatility_regime,
            sector,
        ) = key
        metrics = stat.summarize(config)
        row = {
            "market": market,
            "ticker": ticker,
            "timeframe": timeframe,
            "bias": bias,
            "pattern_type": pattern_type,
            "pattern_size": pattern_size,
            "pattern": pattern,
            "trend_state": trend_state,
            "weekly_context": weekly_context,
            "market_context": market_context,
            "sector_context": sector_context,
            "volatility_regime": volatility_regime,
            "sector": sector,
            **metrics,
        }
        row["roster_state"] = classify_pattern(row, config)
        rows.append(row)
    rows.sort(
        key=lambda r: (
            r["roster_state"] != "keep",
            -float(r["evidence_score"]),
            -int(r["occurrences"]),
            -float(r["avg_directional_return"]),
        )
    )
    return rows
