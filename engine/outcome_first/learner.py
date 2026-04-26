from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from math import log10
from statistics import pstdev
from typing import Any

from engine.outcome_first.features import build_behavior_rows, forward_outcomes
from engine.outcome_first.trust import trusted_probability


def learn_outcome_patterns(
    ohlcv_by_stock: dict[tuple[str, str], list[dict[str, Any]]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    opts = config["outcome_first"]
    windows = [int(x) for x in opts["forward_windows"]]
    targets = [float(x) for x in opts["move_targets"]]
    primary_windows = set(int(x) for x in opts["primary_windows"])
    primary_targets = set(float(x) for x in opts["primary_targets"])
    min_bars = int(opts["min_history_bars"])
    max_pattern_size = int(opts["max_pattern_size"])
    min_occurrences = int(opts["min_pattern_occurrences"])
    max_per_stock_target = int(opts["max_patterns_per_stock_target"])

    learned: list[dict[str, Any]] = []
    for (market, ticker), rows in ohlcv_by_stock.items():
        if len(rows) < min_bars:
            continue
        behavior_rows = build_behavior_rows(rows)
        if not behavior_rows:
            continue
        behavior_by_date = {r["trade_date"]: r for r in behavior_rows}
        outcomes = forward_outcomes(rows, windows)

        stock_rows: list[dict[str, Any]] = []
        for window in windows:
            for direction, target in _direction_targets(targets):
                if window not in primary_windows or target not in primary_targets:
                    continue
                event_rows = []
                all_rows = []
                for date, feat in behavior_by_date.items():
                    outcome = outcomes.get((date, window))
                    if not outcome:
                        continue
                    hit = _hit(outcome, direction, target)
                    directional_return = _directional_return(outcome["forward_return"], direction)
                    rec = {
                        "date": date,
                        "hit": hit,
                        "directional_return": directional_return,
                        "atoms": feat["behavior_atoms"],
                        "coarse_behavior": feat["coarse_behavior"],
                    }
                    all_rows.append(rec)
                    if hit:
                        event_rows.append(rec)
                if len(all_rows) < min_bars // 2:
                    continue
                baseline = sum(r["hit"] for r in all_rows) / len(all_rows)
                if baseline <= 0:
                    continue
                stats = _mine_patterns(
                    all_rows,
                    event_rows,
                    market,
                    ticker,
                    direction,
                    target,
                    window,
                    baseline,
                    max_pattern_size,
                    min_occurrences,
                    config,
                )
                stock_rows.extend(stats)

        grouped: dict[tuple[str, float, int], list[dict[str, Any]]] = defaultdict(list)
        for row in stock_rows:
            grouped[(row["direction"], row["target_move"], row["forward_window"])].append(row)
        for group_rows in grouped.values():
            group_rows.sort(
                key=lambda r: (
                    -float(r["opportunity_score"]),
                    -float(r["lift"]),
                    -int(r["occurrences"]),
                    int(r["pattern_size"]),
                )
            )
            learned.extend(group_rows[:max_per_stock_target])
    learned.sort(
        key=lambda r: (
            r["market"],
            r["ticker"],
            r["direction"],
            -float(r["opportunity_score"]),
        )
    )
    return learned


def _mine_patterns(
    all_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
    market: str,
    ticker: str,
    direction: str,
    target: float,
    window: int,
    baseline: float,
    max_pattern_size: int,
    min_occurrences: int,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    pattern_counts: dict[tuple[str, ...], int] = defaultdict(int)
    pattern_hits: dict[tuple[str, ...], int] = defaultdict(int)
    pattern_returns: dict[tuple[str, ...], list[float]] = defaultdict(list)
    pattern_dates: dict[tuple[str, ...], list[str]] = defaultdict(list)

    for rec in all_rows:
        atoms = sorted(set(rec["atoms"]))
        patterns = _candidate_patterns(atoms, max_pattern_size)
        for pattern in patterns:
            pattern_counts[pattern] += 1
            pattern_hits[pattern] += int(rec["hit"])
            pattern_returns[pattern].append(float(rec["directional_return"]))
            pattern_dates[pattern].append(rec["date"])

    out = []
    for pattern, occurrences in pattern_counts.items():
        if occurrences < min_occurrences:
            continue
        hits = pattern_hits[pattern]
        probability = hits / occurrences if occurrences else 0.0
        trust = trusted_probability(hits, occurrences, baseline)
        if hits < max(3, min_occurrences // 3):
            continue
        lift = probability / baseline if baseline else 0.0
        if lift < float(config["outcome_first"]["baseline_lift_floor"]):
            continue
        returns = pattern_returns[pattern]
        recent_n = int(config["outcome_first"]["recent_occurrences"])
        recent_returns = returns[-recent_n:]
        recent_hits = [1 if r >= target else 0 for r in recent_returns]
        avg_ret = _avg_clip(returns, config)
        recent_probability = sum(recent_hits) / len(recent_hits) if recent_hits else probability
        stability = 1.0 / (1.0 + min((pstdev(returns) if len(returns) > 1 else 0.0) / 0.10, 3.0))
        complexity_penalty = max(0, len(pattern) - 2) * 0.04
        support_factor = min(1.0, log10(occurrences + 1) / log10(max(min_occurrences, 2) + 1))
        score = (
            0.24 * probability
            + 0.24 * min(3.0, lift) / 3.0
            + 0.18 * min(1.0, max(avg_ret, 0) / max(target, 0.01))
            + 0.14 * support_factor
            + 0.10 * recent_probability
            + 0.10 * stability
            - complexity_penalty
        )
        out.append(
            {
                "market": market,
                "ticker": ticker,
                "direction": direction,
                "target_move": target,
                "forward_window": window,
                "pattern_size": len(pattern),
                "behavior_pattern": " + ".join(pattern),
                "occurrences": occurrences,
                "hits": hits,
                "baseline_probability": baseline,
                "target_probability": probability,
                "raw_probability": trust["raw_probability"],
                "probability_ci_low": trust["probability_ci_low"],
                "probability_ci_high": trust["probability_ci_high"],
                "trusted_probability": trust["trusted_probability"],
                "display_probability": trust["trusted_probability"],
                "credibility": trust["credibility"],
                "lift": lift,
                "avg_forward_return": avg_ret,
                "recent_probability": recent_probability,
                "stability": stability,
                "opportunity_score": max(0.0, min(1.0, score)),
                "decay_flag": int(recent_probability + 0.05 < probability),
                "tier": _tier(score, probability, lift, occurrences),
            }
        )
    return out


def _candidate_patterns(atoms: list[str], max_size: int) -> list[tuple[str, ...]]:
    priority_prefixes = (
        "trend_20:",
        "flow:",
        "volatility:",
        "ma_position:",
        "breakout_state:",
        "volume:",
        "candle:",
        "sr_state:",
        "range_state:",
        "ma_slope:",
        "gap_state:",
    )
    atoms = [a for a in atoms if a.startswith(priority_prefixes)]
    patterns = [(a,) for a in atoms]
    for size in range(2, min(max_size, len(atoms)) + 1):
        for combo in combinations(atoms, size):
            families = {c.split(":", 1)[0] for c in combo}
            if len(families) != len(combo):
                continue
            if "trend_20" not in families and "flow" not in families:
                continue
            patterns.append(combo)
    return patterns


def _direction_targets(targets: list[float]) -> list[tuple[str, float]]:
    return [("rally", t) for t in targets] + [("fall", t) for t in targets]


def _hit(outcome: dict[str, float], direction: str, target: float) -> int:
    if direction == "rally":
        return int(outcome["max_forward_return"] >= target)
    return int(outcome["min_forward_return"] <= -target)


def _directional_return(forward_return: float, direction: str) -> float:
    return forward_return if direction == "rally" else -forward_return


def _avg_clip(values: list[float], config: dict[str, Any]) -> float:
    max_abs = float(config["outcome_first"]["max_abs_return_for_scoring"])
    clipped = [max(-max_abs, min(max_abs, v)) for v in values]
    return sum(clipped) / len(clipped) if clipped else 0.0


def _tier(score: float, probability: float, lift: float, occurrences: int) -> str:
    if score >= 0.72 and probability >= 0.45 and lift >= 1.8 and occurrences >= 15:
        return "high_conviction"
    if score >= 0.56 and probability >= 0.30 and lift >= 1.25:
        return "medium"
    return "exploratory"
