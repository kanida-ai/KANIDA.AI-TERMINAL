"""
The registered structured-output schemas, verbatim from
docs/STRATEGY_METHODOLOGY.md §3.3.

Schemas are versioned by id. **Changing one is a new id, never an edit** — an old
`llm_calls` row must always be reproducible against the schema it actually used.
"""
from __future__ import annotations

from typing import Any

REASON_V1: dict[str, Any] = {
    "type": "object", "additionalProperties": False,
    "required": ["claim", "body", "fact_refs", "abstained"],
    "properties": {
        "claim": {"type": "string", "maxLength": 160},
        "body": {"type": "string", "maxLength": 1200},
        "fact_refs": {"type": "array", "items": {"type": "string", "pattern": "^fct_[a-z0-9_]+$"}},
        "proposed_action": {
            "type": ["string", "null"],
            "enum": ["open_experiment", "tune_parameter", "cut_version", "promote",
                     "kill", "queue_next", "no_action", None],
        },
        # L4 is not expressible. A Constitution change cannot be proposed by a model.
        "learning_level": {"type": ["string", "null"], "enum": ["L1", "L2", "L3", None]},
        "confidence": {"type": ["string", "null"],
                       "enum": ["provisional", "supported", "strong", None]},
        "abstained": {"type": "boolean"},
        "abstain_reason": {"type": ["string", "null"], "maxLength": 300},
    },
}

NARRATE_V1: dict[str, Any] = {
    "type": "object", "additionalProperties": False,
    "required": ["beat", "headline", "body", "fact_refs"],
    "properties": {
        "beat": {"type": "string",
                 "enum": ["noticed", "hypothesis", "experiment", "outcome", "learning", "next"]},
        # The pattern makes a bare numeral in a headline structurally impossible.
        "headline": {"type": "string", "maxLength": 120, "pattern": "^[^0-9]*$"},
        "body": {"type": "string", "maxLength": 900},
        "fact_refs": {"type": "array", "items": {"type": "string", "pattern": "^fct_[a-z0-9_]+$"}},
    },
}

#: `reason` plus a COMPOSED hypothesis. The model may only assemble one out of the
#: closed primitive library in engine/hypothesis.py; `HypothesisSpec.parse` then
#: checks every parameter against the Constitution's approved range and REJECTS an
#: out-of-range value rather than clamping it. A model cannot invent a feature here,
#: and it cannot invent a threshold.
HYPOTHESIS_V1: dict[str, Any] = {
    "type": "object", "additionalProperties": False,
    "required": ["claim", "body", "fact_refs", "abstained", "hypothesis"],
    "properties": {
        **REASON_V1["properties"],
        "hypothesis": {
            "type": "object", "additionalProperties": False,
            "required": ["trigger", "params", "context", "direction",
                         "horizon_sessions", "stop_pct"],
            "properties": {
                "trigger": {"type": "string", "enum": [
                    "gap_up", "gap_down", "breakout", "breakdown", "oversold", "overbought",
                    "streak_up", "streak_down", "volume_dry_up", "pullback_in_uptrend",
                    "inside_day", "range_expansion"]},
                "params": {"type": "object", "additionalProperties": {"type": "number"}},
                "context": {"type": "string", "enum": [
                    "any", "index_above_200dma", "index_below_200dma", "high_volatility", "calm"]},
                "direction": {"type": "string", "enum": ["long", "short"]},
                "horizon_sessions": {"type": "integer", "minimum": 1, "maximum": 10},
                "stop_pct": {"type": "number", "minimum": 1.0, "maximum": 12.0},
                "target_pct": {"type": ["number", "null"], "minimum": 1.0, "maximum": 20.0},
            },
        },
    },
}

CLASSIFY_V1: dict[str, Any] = {
    "type": "object", "additionalProperties": False,
    "required": ["label", "rationale"],
    "properties": {
        "label": {"type": "string"},
        "labels": {"type": "array", "items": {"type": "string"}},
        "rationale": {"type": "string", "maxLength": 400},
        "score": {"type": ["number", "null"], "minimum": 0, "maximum": 1},
    },
}

SCHEMAS: dict[str, dict[str, Any]] = {
    "pathfinder.reason.v1": REASON_V1,
    "pathfinder.narrate.v1": NARRATE_V1,
    "pathfinder.hypothesis.v1": HYPOTHESIS_V1,
    "pathfinder.classify.v1": CLASSIFY_V1,
}
