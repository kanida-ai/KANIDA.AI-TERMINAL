"""Executable research grammar. Model output is data, never executable code."""
from dataclasses import asdict, dataclass
import hashlib
import json
import math

FEATURES = {
    "return_1": (-1, 3), "return_3": (-1, 5),
    "volume_ratio": (0, 100), "down_streak": (0, 30),
    "up_streak": (0, 30), "gainer_rank": (0, 1),
    "breakout_20": (-1, 3), "gap": (-1, 3),
    "volume_declines": (0, 30), "breadth": (0, 1),
    "market_return": (-1, 3),
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


@dataclass(frozen=True)
class Policy:
    mission: str = "Discover understandable daily equity patterns; learn from independent paper outcomes."
    initial_capital: float = 500_000
    position_fraction: float = 0.02
    max_exposure: float = 0.20
    max_positions: int = 10
    max_drawdown: float = 0.05
    cost_bps_per_side: float = 10
    max_volume_fraction: float = 0.001
    max_experiments_per_cycle: int = 3
    max_proposals_per_cycle: int = 8
    max_registry: int = 5000
    max_bars: int = 250_000
    min_screen_days: int = 20
    min_paper_days: int = 20
    min_history_sessions: int = 90
    max_data_age_days: int = 4
    max_model_calls_per_day: int = 4
    max_model_output_tokens: int = 4000

    def __post_init__(self):
        for name, value in asdict(self).items():
            if name == "mission":
                if not isinstance(value, str) or not value.strip():
                    raise ValueError("Mission must be nonempty")
                continue
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"Invalid policy value: {name}")
        for name in ("position_fraction", "max_exposure", "max_drawdown", "max_volume_fraction"):
            if getattr(self, name) > 1:
                raise ValueError(f"Invalid fraction: {name}")
        for name in ("max_positions", "max_experiments_per_cycle", "max_proposals_per_cycle", "max_registry", "max_bars", "min_screen_days", "min_paper_days", "min_history_sessions", "max_data_age_days", "max_model_calls_per_day", "max_model_output_tokens"):
            if type(getattr(self, name)) is not int:
                raise ValueError(f"Expected integer: {name}")
        if self.position_fraction > self.max_exposure or self.cost_bps_per_side >= 1000:
            raise ValueError("Invalid exposure or cost limits")


def validate_proposal(raw):
    fields = {"question", "rationale", "goal", "conditions", "hold_sessions", "top_k", "parent_id", "resources"}
    if not isinstance(raw, dict) or set(raw) != fields:
        raise ValueError("Proposal does not match the research contract")
    for name in ("question", "rationale", "goal"):
        if not isinstance(raw[name], str) or not 1 <= len(raw[name]) <= 1200:
            raise ValueError(f"Invalid {name}")
    if raw["parent_id"] is not None and (not isinstance(raw["parent_id"], str) or len(raw["parent_id"]) > 64):
        raise ValueError("Invalid parent experiment")
    if type(raw["hold_sessions"]) is not int or not 1 <= raw["hold_sessions"] <= 5:
        raise ValueError("Holding period must be 1 to 5 sessions")
    if type(raw["top_k"]) is not int or not 1 <= raw["top_k"] <= 5:
        raise ValueError("top_k must be 1 to 5")
    if not isinstance(raw["resources"], list) or not 1 <= len(raw["resources"]) <= 5 or any(not isinstance(x, str) or len(x) > 80 for x in raw["resources"]):
        raise ValueError("Invalid resources")
    if "daily_ohlcv" not in raw["resources"]:
        raise ValueError("Daily executor requires daily_ohlcv")
    conditions = raw["conditions"]
    if not isinstance(conditions, list) or not 1 <= len(conditions) <= 4:
        raise ValueError("Require 1 to 4 conditions")
    for c in conditions:
        if not isinstance(c, dict) or set(c) != {"feature", "op", "value"}:
            raise ValueError("Invalid condition")
        if c["feature"] not in FEATURES or c["op"] not in (">=", "<="):
            raise ValueError("Unsupported feature/operator")
        lo, hi = FEATURES[c["feature"]]
        if type(c["value"]) not in (float, int) or not math.isfinite(c["value"]) or not lo <= c["value"] <= hi:
            raise ValueError("Invalid threshold")
    result = dict(raw)
    result["conditions"] = sorted(conditions, key=canonical)
    result["resources"] = sorted(set(raw["resources"]))
    return result


def experiment_id(proposal):
    # Descriptions, goals and lineage cannot disguise the same executable test.
    return digest({k: proposal[k] for k in ("conditions", "hold_sessions", "top_k", "resources")})[:24]


def matches(proposal, row):
    return all(c["feature"] in row and
               (row[c["feature"]] >= c["value"] if c["op"] == ">=" else row[c["feature"]] <= c["value"])
               for c in proposal["conditions"])
