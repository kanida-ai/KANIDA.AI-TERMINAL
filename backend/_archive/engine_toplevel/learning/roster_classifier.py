from __future__ import annotations

from typing import Any


def classify_pattern(row: dict[str, Any], config: dict[str, Any]) -> str:
    n = int(row["occurrences"])
    min_support = int(row["min_support"])
    score = float(row["evidence_score"])
    wr = float(row["win_rate"])
    avg_ret = float(row["avg_directional_return"])
    decay = bool(int(row.get("decay_flag", 0)))

    if n < min_support:
        return "test" if wr >= 0.58 and avg_ret > 0 else "retire"
    if n >= min_support and (wr < 0.48 or avg_ret <= 0):
        return "retire"
    if n >= min_support and score >= float(config["evidence"]["min_keep_score"]) and not decay:
        return "keep"
    if score >= float(config["evidence"]["min_watch_score"]):
        return "watch" if not decay else "test"
    return "retire"
