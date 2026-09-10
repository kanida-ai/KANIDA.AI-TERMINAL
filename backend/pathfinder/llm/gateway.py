"""
`pathfinder_llm` — gateway CONTRACT. No provider, no SDK import, no network.

Product code NEVER calls a model SDK directly. It calls one of three verbs:

    pathfinder_llm.reason(...)      propose / critique / decide-what-next   (Sonnet 5)
    pathfinder_llm.narrate(...)     deterministic results -> storyline      (Haiku 4.5)
    pathfinder_llm.classify(...)    tag / label / summarise                 (Haiku 4.5)

Three invariants this contract exists to enforce:

  1. THE LLM NEVER CALCULATES. Every call takes `facts` — already-computed
     deterministic numbers — and every call returns text that may reference them
     only as `{{fact:<id>}}`. A provider implementation MUST reject a completion
     whose prose contains a literal numeral (`_assert_no_bare_numerals`).
  2. PROVIDER-INDEPENDENT. Claude -> GPT -> Gemini is a config swap. Nothing in
     this module names an SDK type.
  3. EVERY CALL IS METERED. `LlmResult.usage` carries the real token counts from
     the provider response, and the daily budget is enforced BEFORE the call.

Model routing, structured-output schemas, caching layout, batch policy and the
budget rules are specified in docs/STRATEGY_METHODOLOGY.md. This file is the
typed shape of that specification.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Protocol, Sequence


# ── Jobs → models (the locked routing from docs/sessions/PATHFINDER.md) ──────

class Job(str, Enum):
    """Every LLM call declares its job. The job — not the caller — picks the model."""
    hypothesis = "hypothesis"            # generate a genuinely new hypothesis
    critique = "critique"                # reflection after an experiment
    decide_next = "decide_next"          # decide the next research direction
    narrate = "narrate"                  # deterministic results -> storyline
    classify = "classify"                # classify / tag / summarize
    hard_research = "hard_research"      # exceptional hard problem (rare)


#: The locked routing. Changing a row is a Constitution-level (L4) decision.
JOB_MODEL_ROUTING: dict[Job, str] = {
    Job.hypothesis: "claude-sonnet-5",
    Job.critique: "claude-sonnet-5",
    Job.decide_next: "claude-sonnet-5",
    Job.narrate: "claude-haiku-4-5",
    Job.classify: "claude-haiku-4-5",
    Job.hard_research: "claude-opus-5",
}


# ── Errors ───────────────────────────────────────────────────────────────────

class GatewayError(RuntimeError):
    """Any gateway failure. Callers must degrade gracefully — never fabricate text."""


class BudgetExceeded(GatewayError):
    """The hard daily token/cost cap was hit. The loop pauses; it does not improvise."""


class OutputContractViolation(GatewayError):
    """The completion broke a product law (a bare numeral, an undeclared fact ref)."""


# ── Metering ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Usage:
    """Real token accounting, straight from the provider response."""
    model: str
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0


@dataclass(frozen=True)
class Budget:
    """The hard daily cap. Checked BEFORE a call, never after."""
    daily_cost_usd: float
    spent_usd_today: float = 0.0

    @property
    def remaining_usd(self) -> float:
        return max(0.0, self.daily_cost_usd - self.spent_usd_today)


# ── Results ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class LlmResult:
    """Base result. `usage` is never optional — an unmetered call is a bug."""
    job: Job
    model: str
    usage: Usage
    prompt_version: str
    constitution_version: str
    raw_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReasonResult(LlmResult):
    """
    Output of `reason()`. Shape mirrors the `pathfinder.reason.v1` structured schema.

    `fact_refs` are the deterministic facts the reasoning leans on; the gateway
    verifies every one was actually supplied in the call's `facts`.
    """
    claim: str = ""
    body: str = ""
    fact_refs: Sequence[str] = ()
    proposed_action: Optional[str] = None
    learning_level: Optional[str] = None       # "L1" | "L2" | "L3" — never "L4"
    confidence: Optional[str] = None           # provisional | supported | strong
    abstained: bool = False
    abstain_reason: Optional[str] = None


@dataclass(frozen=True)
class NarrateResult(LlmResult):
    """Output of `narrate()`. One story beat, no numbers of its own."""
    beat: str = ""
    headline: str = ""
    body: str = ""
    fact_refs: Sequence[str] = ()


@dataclass(frozen=True)
class ClassifyResult(LlmResult):
    """Output of `classify()`. A label from a closed set, plus its rationale."""
    label: str = ""
    labels: Sequence[str] = ()
    rationale: str = ""
    score: Optional[float] = None


# ── The gateway ──────────────────────────────────────────────────────────────

class PathfinderLLM(Protocol):
    """
    The ONLY interface product code may use to reach a model.

    Every method takes `facts` (already computed, deterministic) and a `schema_id`
    naming a registered structured-output schema. Implementations must:
      * refuse the call when `budget` has no remaining headroom (BudgetExceeded);
      * request strict structured output for `schema_id`;
      * place the cache breakpoint after the stable prefix (Constitution + rulebook
        + fact table) and before the volatile question;
      * validate the completion against the product laws and raise
        `OutputContractViolation` rather than return prose containing a numeral;
      * populate `usage` from the provider response, never from an estimate.
    """

    def reason(
        self,
        *,
        job: Job,
        question: str,
        facts: Sequence[dict[str, Any]],
        context: Sequence[dict[str, Any]] = (),
        schema_id: str = "pathfinder.reason.v1",
        constitution_version: str,
        prompt_version: str,
        budget: Budget,
        batch: bool = False,
    ) -> ReasonResult:
        """Propose a hypothesis, critique an outcome, or decide what to test next."""
        ...

    def narrate(
        self,
        *,
        beat: str,
        facts: Sequence[dict[str, Any]],
        context: Sequence[dict[str, Any]] = (),
        schema_id: str = "pathfinder.narrate.v1",
        constitution_version: str,
        prompt_version: str,
        budget: Budget,
        batch: bool = True,
    ) -> NarrateResult:
        """Turn already-computed results into one beat of the loop story."""
        ...

    def classify(
        self,
        *,
        text: str,
        labels: Sequence[str],
        facts: Sequence[dict[str, Any]] = (),
        schema_id: str = "pathfinder.classify.v1",
        constitution_version: str,
        prompt_version: str,
        budget: Budget,
        batch: bool = True,
    ) -> ClassifyResult:
        """Assign a label from a CLOSED set. The model may not invent a label."""
        ...


__all__ = [
    "Budget",
    "BudgetExceeded",
    "ClassifyResult",
    "GatewayError",
    "JOB_MODEL_ROUTING",
    "Job",
    "LlmResult",
    "NarrateResult",
    "OutputContractViolation",
    "PathfinderLLM",
    "ReasonResult",
    "Usage",
]
