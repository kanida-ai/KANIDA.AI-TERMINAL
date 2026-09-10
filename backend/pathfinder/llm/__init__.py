"""
`pathfinder_llm` — the provider-independent GenAI gateway.

P0 ships the CONTRACT ONLY (see gateway.py). There is no provider implementation
in this package and no SDK import anywhere in it. Session P1 lands the Claude
provider behind the same Protocol.

The authority for this contract is docs/STRATEGY_METHODOLOGY.md § "The
pathfinder_llm gateway". If the two ever disagree, the doc wins and this file is
the bug.
"""
from .gateway import (  # noqa: F401
    Budget,
    BudgetExceeded,
    ClassifyResult,
    GatewayError,
    Job,
    JOB_MODEL_ROUTING,
    LlmResult,
    NarrateResult,
    PathfinderLLM,
    ReasonResult,
    Usage,
)
