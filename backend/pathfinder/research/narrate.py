"""
Narration and selection through the `pathfinder_llm` gateway — and what happens without it.

Two jobs the model is allowed:
  * SELECT   `classify` on the closed set {publish, hold} for a card that ALREADY cleared the
             usefulness threshold. A "hold" is recorded. The model cannot publish a card the
             engine rejected and cannot change the order — ranking is deterministic.
  * NARRATE  `narrate` from the card's fact table. The provider enforces the output contract
             (no bare numeral, no unsupplied fact ref); the `Narrative` schema enforces it
             again at the API boundary.

Without a provider (no ANTHROPIC_API_KEY, or a cassette miss) the card is narrated by the
engine's own digit-free template, and says so: `produced_by = "engine"`. Nothing is ever
silently re-badged.

S1 audit P1 — what the model may NOT do, enforced here on top of the gateway contract:
  * write a quantity as a word ("nine in ten", "half", "doubled", "usually") without a fact
    reference in the same sentence (`enforce_narrate(strict=True)`);
  * narrate without citing a single fact;
  * replace the HEADLINE. The headline is the decision, and the decision is the engine's:
    the model's headline is discarded and the engine's kept, so a body cannot be published
    under a headline the engine did not compute.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from ..llm.contracts import enforce_narrate
from ..llm.gateway import Budget, GatewayError, PathfinderLLM
from ..schemas import FACT_REF_RE, Author, Narrative
from .library import CardDraft

PROMPT_VERSION = "pathfinder.feed@s1.0.0"
CONSTITUTION_VERSION = "pathfinder_constitution@draft"


def _refs(text: str) -> list[str]:
    return sorted({m.group("id") for m in FACT_REF_RE.finditer(text)})


def engine_narrative(draft: CardDraft, at: datetime, reason: Optional[str] = None) -> Narrative:
    return Narrative(
        headline=draft.headline, body=draft.body, produced_by=Author.engine, at=at,
        fact_refs=_refs(draft.body),
    )


@dataclass
class Selection:
    publish: bool
    decided_by: str          # "engine" | "llm"
    reason: str


class Narrator:
    """Tries the model; records honestly when it could not."""

    def __init__(self, llm: Optional[PathfinderLLM], *, provider_name: str,
                 budget: Optional[Budget] = None) -> None:
        self._llm = llm
        self.provider_name = provider_name
        self._budget = budget or Budget(daily_cost_usd=0.0)
        self.failures: list[str] = []
        self.llm_calls: int = 0

    def _context(self, draft: CardDraft) -> list[dict[str, Any]]:
        return [{
            "template_id": draft.template_id, "question": draft.question,
            "subject": draft.subject, "decision": draft.decision.value,
            "decision_reason": draft.decision_reason, "level": draft.level.value,
            "comparison_group": draft.comparison_group,
            "engine_draft": draft.body,
        }]

    def select(self, draft: CardDraft, finding_id: str) -> Selection:
        if self._llm is None:
            return Selection(True, "engine", "usefulness threshold (no model configured)")
        try:
            r = self._llm.classify(
                text=f"{draft.question} Subject: {draft.subject}. Decision: {draft.decision.value}.",
                labels=["publish", "hold"], facts=draft.facts.as_dicts(),
                constitution_version=CONSTITUTION_VERSION, prompt_version=PROMPT_VERSION,
                budget=self._budget, key=f"{finding_id}:select",
            )
            self.llm_calls += 1
            return Selection(r.label == "publish", "llm", r.rationale)
        except GatewayError as e:
            self.failures.append(f"select[{finding_id}]: {e}")
            return Selection(True, "engine", "usefulness threshold (model unavailable)")

    def narrate(self, draft: CardDraft, finding_id: str, at: datetime) -> Narrative:
        if self._llm is None:
            return engine_narrative(draft, at)
        try:
            r = self._llm.narrate(
                beat="noticed", facts=draft.facts.as_dicts(), context=self._context(draft),
                constitution_version=CONSTITUTION_VERSION, prompt_version=PROMPT_VERSION,
                budget=self._budget, key=f"{finding_id}:narrate",
            )
            self.llm_calls += 1
            # The feed's stricter contract (P1), re-applied here so no provider can skip it.
            enforce_narrate({"beat": r.beat, "headline": r.headline, "body": r.body,
                             "fact_refs": list(r.fact_refs)}, draft.facts.as_dicts(), strict=True)
            return Narrative(
                headline=draft.headline,           # the ENGINE's headline: the decision is not the model's to restate
                body=r.body, produced_by=Author.llm, model=r.model,
                prompt_version=PROMPT_VERSION, at=at, fact_refs=list(r.fact_refs),
            )
        except GatewayError as e:
            self.failures.append(f"narrate[{finding_id}]: {e}")
            return engine_narrative(draft, at, reason=str(e))
