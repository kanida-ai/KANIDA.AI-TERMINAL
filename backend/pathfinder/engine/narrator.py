"""
Story lines: how a computed result becomes a sentence, and who is on the hook for it.

Two authors, and the difference is recorded on every line:

  * `produced_by="llm"` — the model wrote it. It may contain **no literal numeral**;
    numbers appear as `{{fact:…}}` tokens the client resolves against `facts[]`.
  * `produced_by="engine"` — a deterministic template wrote it, because no model was
    available or a call failed. §3.8 forbids *silently* falling back to a template, so
    this is the opposite of silent: the author is a field on every line, the reason is
    recorded on the cycle, and the payload the customer sees says which.

Engine-authored lines are also written with fact tokens rather than inline numbers —
not because they must be, but so that swapping a beat from engine to LLM later changes
the author and nothing else.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Sequence

from ..llm.gateway import GatewayError, Job, PathfinderLLM
from .config import now_ist


@dataclass(frozen=True)
class Line:
    beat: str
    headline: str
    body: str
    produced_by: str                 # engine | llm | human
    fact_refs: tuple[str, ...]
    at: datetime
    model: Optional[str] = None
    prompt_version: Optional[str] = None
    llm_call_id: Optional[str] = None
    fallback_reason: Optional[str] = None


class Narrator:
    """Tries the model; records honestly when it could not."""

    prompt_version = "pathfinder.narrate@p1.0.0"

    def __init__(self, llm: PathfinderLLM, *, constitution_version: str, budget,
                 on_call=None) -> None:
        self._llm = llm
        self._cv = constitution_version
        self._budget = budget
        #: Called with the completed `NarrateResult` so the caller can write the
        #: `llm_calls` metering row. Every call is metered — an unmetered one is a bug
        #: (docs/STRATEGY_METHODOLOGY.md §3.6), and narration is the majority of them.
        self._on_call = on_call
        self.failures: list[str] = []

    def beat(
        self,
        *,
        beat: str,
        facts: Sequence[dict[str, Any]],
        context: Sequence[dict[str, Any]],
        key: str,
        fallback_headline: str,
        fallback_body: str,
        fallback_refs: Sequence[str],
    ) -> Line:
        try:
            r = self._llm.narrate(
                beat=beat, facts=list(facts), context=list(context),
                constitution_version=self._cv, prompt_version=self.prompt_version,
                budget=self._budget, key=key,
            )
            call_id = self._on_call(r) if self._on_call else None
            return Line(
                beat=beat, headline=r.headline, body=r.body, produced_by="llm",
                fact_refs=tuple(r.fact_refs), at=now_ist(), model=r.model,
                prompt_version=self.prompt_version, llm_call_id=call_id,
            )
        except GatewayError as e:
            reason = f"{beat}[{key}]: {e}"
            self.failures.append(reason)
            return Line(
                beat=beat, headline=fallback_headline, body=fallback_body,
                produced_by="engine", fact_refs=tuple(fallback_refs), at=now_ist(),
                fallback_reason=str(e),
            )
