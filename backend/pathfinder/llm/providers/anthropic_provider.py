"""
The live Claude provider. THE ONLY FILE IN PATHFINDER THAT IMPORTS A MODEL SDK.

Provider independence (docs/STRATEGY_METHODOLOGY.md §3.7) means: nothing outside this
module names a vendor type or reads a vendor-shaped response. Swapping to another
provider is writing a sibling of this file and changing one env var.

Claude-specific details this implementation is responsible for getting right:
  * `output_config={"format": ...}` with `strict: true` — NOT the deprecated
    `output_format` parameter;
  * `thinking={"type": "adaptive"}` on `claude-sonnet-5` / `claude-opus-5`
    (`budget_tokens` is rejected on those); `{"type": "enabled", "budget_tokens": N}`
    on `claude-haiku-4-5`, which in turn rejects `output_config.effort`;
  * cache breakpoints AFTER the Constitution + methodology system blocks and AFTER
    the rulebook + fact table, never around the volatile question;
  * no assistant prefill on any of the three models;
  * `stop_reason` checked before the content is read;
  * structured output parsed with a JSON parser, never string-matched.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Optional, Sequence

from ...engine.config import now_ist
from ..budget import BudgetLedger, cost_usd
from ..contracts import enforce_classify, enforce_narrate, enforce_reason, render_facts
from ..gateway import (
    Budget, ClassifyResult, GatewayError, JOB_MODEL_ROUTING, Job, NarrateResult,
    OutputContractViolation, ReasonResult, Usage,
)
from ..schemas import SCHEMAS
from .base import ProviderResult

THINKING_ADAPTIVE = {"claude-sonnet-5", "claude-opus-5"}


class AnthropicProvider:
    """`PathfinderLLM` over the Anthropic Messages API."""

    provider_name = "live:anthropic"

    def __init__(
        self,
        *,
        ledger: BudgetLedger,
        constitution_text: str,
        methodology_text: str,
        api_key: Optional[str] = None,
        max_tokens: int = 2048,
    ) -> None:
        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise GatewayError(
                "ANTHROPIC_API_KEY is not set. Pathfinder will not write narrative "
                "without a model behind it."
            )
        try:
            import anthropic  # noqa: PLC0415 — deliberately local: the ONLY SDK import
        except ImportError as e:  # pragma: no cover
            raise GatewayError("the `anthropic` package is not installed") from e
        self._client = anthropic.Anthropic(api_key=key)
        self._ledger = ledger
        self._constitution = constitution_text
        self._methodology = methodology_text
        self._max_tokens = max_tokens

    # ── prefix layout (§3.4) ────────────────────────────────────────────────
    # No timestamps, no UUIDs, no now(): the `as_of` belongs in the volatile tail.

    def _system(self) -> list[dict[str, Any]]:
        return [
            {"type": "text", "text": self._constitution},
            {"type": "text", "text": self._methodology,
             "cache_control": {"type": "ephemeral"}},
        ]

    def _messages(self, *, stable: str, volatile: str) -> list[dict[str, Any]]:
        return [{
            "role": "user",
            "content": [
                {"type": "text", "text": stable, "cache_control": {"type": "ephemeral"}},
                {"type": "text", "text": volatile},
            ],
        }]

    def _thinking(self, model: str) -> dict[str, Any]:
        if model in THINKING_ADAPTIVE:
            return {"type": "adaptive"}
        return {"type": "enabled", "budget_tokens": max(1024, self._max_tokens // 2)}

    # ── the one call ────────────────────────────────────────────────────────

    def _call(self, *, job: Job, schema_id: str, stable: str, volatile: str) -> ProviderResult:
        model = JOB_MODEL_ROUTING[job]
        self._ledger.check(job=job.value)
        schema = SCHEMAS[schema_id]

        kwargs: dict[str, Any] = {
            "model": model,
            "max_tokens": self._max_tokens,
            "system": self._system(),
            "messages": self._messages(stable=stable, volatile=volatile),
            "thinking": self._thinking(model),
            "output_config": {"format": {"type": "json_schema", "schema": schema, "strict": True}},
        }
        t0 = time.perf_counter()
        try:
            resp = self._client.messages.create(**kwargs)
        except Exception as e:  # noqa: BLE001 — any SDK failure is a gateway failure
            raise GatewayError(f"{model} call failed for job {job.value}: {e}") from e
        latency_ms = int((time.perf_counter() - t0) * 1000)

        stop = getattr(resp, "stop_reason", None)
        if stop not in (None, "end_turn", "stop_sequence"):
            raise GatewayError(f"{model} stopped with {stop!r}; refusing to read a truncated completion")

        text = "".join(
            b.text for b in resp.content
            if getattr(b, "type", None) == "text" and hasattr(b, "text")
        )
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as e:
            raise OutputContractViolation(f"{model} returned non-JSON for {schema_id}") from e

        u = resp.usage
        usage = Usage(
            model=model,
            input_tokens=int(getattr(u, "input_tokens", 0) or 0),
            output_tokens=int(getattr(u, "output_tokens", 0) or 0),
            cache_read_input_tokens=int(getattr(u, "cache_read_input_tokens", 0) or 0),
            cache_creation_input_tokens=int(getattr(u, "cache_creation_input_tokens", 0) or 0),
            latency_ms=latency_ms,
        )
        usage = Usage(
            **{**usage.__dict__,
               "cost_usd": cost_usd(model, input_tokens=usage.input_tokens,
                                    output_tokens=usage.output_tokens,
                                    cache_read=usage.cache_read_input_tokens,
                                    cache_write=usage.cache_creation_input_tokens)}
        )
        self._ledger.record(usage)
        return ProviderResult(payload=payload, usage=usage, model=model, provider=self.provider_name)

    # ── the three verbs ─────────────────────────────────────────────────────

    def reason(self, *, job: Job, question: str, facts: Sequence[dict[str, Any]],
               context: Sequence[dict[str, Any]] = (), schema_id: str = "pathfinder.reason.v1",
               constitution_version: str, prompt_version: str, budget: Budget,
               batch: bool = False, key: str = "") -> ReasonResult:
        stable = f"FACTS (computed by the deterministic engine):\n{render_facts(facts)}"
        volatile = (
            f"CONTEXT:\n{json.dumps(list(context), sort_keys=True, indent=2)}\n\n"
            f"QUESTION:\n{question}\n\n"
            "Reference every number as {{fact:<id>}}. Do not write a numeral. "
            "Abstaining is a correct answer when the evidence does not support one."
        )
        # `key` is the cassette address used by the recorded provider; a live call
        # ignores it. Keeping it in the signature keeps the two interchangeable.
        r = self._call(job=job, schema_id=schema_id, stable=stable, volatile=volatile)
        p = enforce_reason(r.payload, facts, job=job)
        return ReasonResult(
            job=job, model=r.model, usage=r.usage, prompt_version=prompt_version,
            constitution_version=constitution_version, raw_json=p,
            claim=str(p["claim"]), body=str(p["body"]),
            fact_refs=tuple(p.get("fact_refs") or ()),
            proposed_action=p.get("proposed_action"), learning_level=p.get("learning_level"),
            confidence=p.get("confidence"), abstained=bool(p["abstained"]),
            abstain_reason=p.get("abstain_reason"),
        )

    def narrate(self, *, beat: str, facts: Sequence[dict[str, Any]],
                context: Sequence[dict[str, Any]] = (), schema_id: str = "pathfinder.narrate.v1",
                constitution_version: str, prompt_version: str, budget: Budget,
                batch: bool = True, key: str = "") -> NarrateResult:
        stable = f"FACTS (computed by the deterministic engine):\n{render_facts(facts)}"
        volatile = (
            f"BEAT: {beat}\nCONTEXT:\n{json.dumps(list(context), sort_keys=True, indent=2)}\n\n"
            "Write one beat of the research story. The headline must contain no digit "
            "at all. In the body, every number is a {{fact:<id>}} reference."
        )
        r = self._call(job=Job.narrate, schema_id=schema_id, stable=stable, volatile=volatile)
        p = enforce_narrate(r.payload, facts)
        return NarrateResult(
            job=Job.narrate, model=r.model, usage=r.usage, prompt_version=prompt_version,
            constitution_version=constitution_version, raw_json=p,
            beat=str(p["beat"]), headline=str(p["headline"]), body=str(p["body"]),
            fact_refs=tuple(p.get("fact_refs") or ()),
        )

    def classify(self, *, text: str, labels: Sequence[str], facts: Sequence[dict[str, Any]] = (),
                 schema_id: str = "pathfinder.classify.v1", constitution_version: str,
                 prompt_version: str, budget: Budget, batch: bool = True,
                 key: str = "") -> ClassifyResult:
        stable = f"LABELS (closed set):\n{json.dumps(sorted(labels))}"
        volatile = f"TEXT:\n{text}\n\nChoose exactly one label from the closed set."
        r = self._call(job=Job.classify, schema_id=schema_id, stable=stable, volatile=volatile)
        p = enforce_classify(r.payload, labels)
        return ClassifyResult(
            job=Job.classify, model=r.model, usage=r.usage, prompt_version=prompt_version,
            constitution_version=constitution_version, raw_json=p,
            label=str(p["label"]), labels=tuple(p.get("labels") or ()),
            rationale=str(p["rationale"]), score=p.get("score"),
        )
