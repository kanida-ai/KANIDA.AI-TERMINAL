from __future__ import annotations

import math
from typing import Any


def wilson_interval(hits: int, occurrences: int, z: float = 1.96) -> tuple[float, float]:
    if occurrences <= 0:
        return 0.0, 0.0
    p = hits / occurrences
    denom = 1 + z * z / occurrences
    center = p + z * z / (2 * occurrences)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * occurrences)) / occurrences)
    return max(0.0, (center - margin) / denom), min(1.0, (center + margin) / denom)


def trusted_probability(hits: int, occurrences: int, baseline: float) -> dict[str, Any]:
    raw = hits / occurrences if occurrences else 0.0
    lower, upper = wilson_interval(hits, occurrences)
    # Empirical-Bayes shrinkage toward the stock baseline. This is deliberately
    # conservative for small samples and prevents thin 100% records from looking
    # like certainties.
    prior_n = 20
    shrunk = ((raw * occurrences) + (baseline * prior_n)) / (occurrences + prior_n) if occurrences else baseline
    display = min(shrunk, lower + 0.18)
    credibility = credibility_label(occurrences, lower, raw)
    return {
        "raw_probability": raw,
        "probability_ci_low": lower,
        "probability_ci_high": upper,
        "trusted_probability": max(0.0, min(1.0, display)),
        "credibility": credibility,
    }


def credibility_label(occurrences: int, lower: float, raw: float) -> str:
    if occurrences >= 50 and lower >= 0.55:
        return "strong"
    if occurrences >= 25 and lower >= 0.45:
        return "solid"
    if occurrences >= 12 and raw >= 0.65:
        return "thin_but_interesting"
    return "exploratory"


def apply_trust(row: dict[str, Any]) -> dict[str, Any]:
    hits = int(float(row.get("hits", 0) or 0))
    occurrences = int(float(row.get("occurrences", 0) or 0))
    baseline = float(row.get("baseline_probability", 0.0) or 0.0)
    trusted = trusted_probability(hits, occurrences, baseline)
    out = dict(row)
    out.update(trusted)
    out["display_probability"] = trusted["trusted_probability"]
    return out
