from __future__ import annotations

import math
from dataclasses import dataclass, field
from statistics import pstdev
from typing import Any


@dataclass
class PatternStats:
    key: tuple[Any, ...]
    pattern_type: str
    pattern_size: int
    returns: list[float] = field(default_factory=list)
    wins: int = 0
    dates: list[str] = field(default_factory=list)
    recency_weights: list[float] = field(default_factory=list)

    def add(self, ret: float, win: int, date: str, recency_weight: float = 1.0) -> None:
        self.returns.append(ret)
        self.wins += int(win)
        self.dates.append(date)
        self.recency_weights.append(recency_weight)

    @property
    def occurrences(self) -> int:
        return len(self.returns)

    def summarize(self, config: dict[str, Any]) -> dict[str, Any]:
        n = self.occurrences
        max_abs = float(config["evidence"].get("max_abs_return_for_scoring", 0.5))
        clipped_returns = [_clip(x, -max_abs, max_abs) for x in self.returns]
        wr = self.wins / n if n else 0.0
        raw_avg_ret = sum(self.returns) / n if n else 0.0
        avg_ret = sum(clipped_returns) / n if n else 0.0
        weighted_ret = _weighted_avg(clipped_returns, self.recency_weights)
        recent_n = int(config["decay"]["recent_occurrences"])
        recent_returns = clipped_returns[-recent_n:]
        recent_wr = sum(1 for x in recent_returns if x > 0) / len(recent_returns) if recent_returns else 0.0
        recent_avg = sum(recent_returns) / len(recent_returns) if recent_returns else 0.0
        std = pstdev(self.returns) if n > 1 else 0.0
        stability = 1.0 / (1.0 + min(std / 0.10, 3.0))
        decay_wr = recent_wr - wr
        decay_ret = recent_avg - avg_ret
        min_support = _min_support(self.pattern_type, self.pattern_size, config)
        evidence = evidence_score(
            occurrences=n,
            win_rate=wr,
            avg_return=avg_ret,
            weighted_return=weighted_ret,
            stability=stability,
            recent_win_rate=recent_wr,
            recent_avg_return=recent_avg,
            pattern_size=self.pattern_size,
            min_support=min_support,
            config=config,
        )
        decay_flag = (
            len(recent_returns) >= int(config["decay"]["min_recent_samples"])
            and (
                decay_wr <= -float(config["decay"]["warn_winrate_drop"])
                or decay_ret <= -float(config["decay"]["warn_return_drop"])
            )
        )
        return {
            "occurrences": n,
            "win_rate": wr,
            "raw_avg_directional_return": raw_avg_ret,
            "avg_directional_return": avg_ret,
            "weighted_directional_return": weighted_ret,
            "recent_win_rate": recent_wr,
            "recent_avg_directional_return": recent_avg,
            "return_stddev": std,
            "stability": stability,
            "decay_winrate_delta": decay_wr,
            "decay_return_delta": decay_ret,
            "decay_flag": int(decay_flag),
            "min_support": min_support,
            "evidence_score": evidence,
        }


def evidence_score(
    *,
    occurrences: int,
    win_rate: float,
    avg_return: float,
    weighted_return: float,
    stability: float,
    recent_win_rate: float,
    recent_avg_return: float,
    pattern_size: int,
    min_support: int,
    config: dict[str, Any],
) -> float:
    support_factor = min(1.0, math.log10(occurrences + 1) / math.log10(max(min_support, 2) + 1))
    win_component = max(0.0, min(1.0, (win_rate - 0.45) / 0.25))
    recent_component = max(0.0, min(1.0, (recent_win_rate - 0.45) / 0.25))
    target_return = float(config["evidence"]["target_return"])
    ret_component = max(0.0, min(1.0, avg_return / target_return))
    weighted_component = max(0.0, min(1.0, weighted_return / target_return))
    recent_ret_component = max(0.0, min(1.0, recent_avg_return / target_return))
    complexity_penalty = max(0, pattern_size - 1) * float(
        config["anti_overfit"]["complexity_penalty_per_extra_signal"]
    )
    low_support_penalty = (
        float(config["anti_overfit"]["low_support_penalty"])
        if occurrences < min_support
        else 0.0
    )
    raw = (
        0.22 * support_factor
        + 0.20 * win_component
        + 0.18 * ret_component
        + 0.12 * weighted_component
        + 0.12 * recent_component
        + 0.08 * recent_ret_component
        + 0.08 * stability
    )
    return max(0.0, min(1.0, raw - complexity_penalty - low_support_penalty))


def _weighted_avg(values: list[float], weights: list[float]) -> float:
    total_w = sum(weights)
    if not values or total_w == 0:
        return 0.0
    return sum(v * w for v, w in zip(values, weights)) / total_w


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _min_support(pattern_type: str, pattern_size: int, config: dict[str, Any]) -> int:
    support = config["anti_overfit"]["min_occurrences"]
    if pattern_type == "sequence":
        return int(support["sequence"])
    if pattern_type == "cross_timeframe":
        return int(support["cross_timeframe"])
    if pattern_size <= 1:
        return int(support["single"])
    if pattern_size == 2:
        return int(support["pair"])
    return int(support["triple"])
