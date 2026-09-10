"""
The RECORDED provider — cassette replay.

Why it exists: the loop must be reproducible in CI and on a machine with no API key,
and a test suite that needs a live model is a test suite nobody runs.

Why it is honest: a cassette entry records **which model actually authored the
prose**, and that is what lands in `llm_calls.model`. It is never re-badged as the
model the routing table would have used. Token counts are zero and cost is zero,
because replaying a recording spends nothing — the numbers are not estimates, they
are the truth about this call.

Every replayed completion is put through the **same** `enforce_*` contracts as a live
one. A cassette that broke the no-bare-numerals law would fail here exactly as a live
completion would; the contract does not know or care where the text came from.

Reading a cassette entry: it is untrusted text as far as the engine is concerned.
Nothing in it can set a number — the only numeric content it may carry is a
`{{fact:…}}` id, which is resolved against facts the engine computed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional, Sequence

from ..budget import BudgetLedger
from ..contracts import enforce_classify, enforce_narrate, enforce_reason
from ..gateway import (
    Budget, ClassifyResult, GatewayError, Job, NarrateResult, ReasonResult, Usage,
)

CASSETTE_DIR = Path(__file__).resolve().parents[1] / "cassettes"


class CassetteMiss(GatewayError):
    """No recording for this call. Loud, because a silent template would be a lie."""


class RecordedProvider:
    """`PathfinderLLM` served from `llm/cassettes/*.json`."""

    provider_name = "recorded"

    def __init__(self, *, ledger: BudgetLedger, cassette_dir: Optional[Path] = None) -> None:
        self._ledger = ledger
        self._dir = cassette_dir or CASSETTE_DIR
        self._entries: dict[str, dict[str, Any]] = {}
        for f in sorted(self._dir.glob("*.json")):
            data = json.loads(f.read_text(encoding="utf-8"))
            for entry in data["entries"]:
                key = f"{entry['job']}::{entry['key']}"
                if key in self._entries:
                    raise GatewayError(f"duplicate cassette entry {key!r} in {f.name}")
                entry.setdefault("authored_by_model", data["authored_by_model"])
                entry.setdefault("recorded_at", data["recorded_at"])
                entry.setdefault("note", data["note"])
                self._entries[key] = entry

    # ── plumbing ────────────────────────────────────────────────────────────

    def _fetch(self, job: Job, key: str) -> dict[str, Any]:
        self._ledger.check(job=job.value)
        entry = self._entries.get(f"{job.value}::{key}")
        if entry is None:
            raise CassetteMiss(
                f"no recorded completion for job={job.value} key={key!r}. "
                "The loop will record the beat as unavailable rather than invent it."
            )
        return entry

    @staticmethod
    def _usage(entry: dict[str, Any]) -> Usage:
        # Replay spends nothing. Zero is the measurement, not a placeholder.
        return Usage(model=str(entry["authored_by_model"]), input_tokens=0, output_tokens=0,
                     cost_usd=0.0, latency_ms=0)

    # ── the three verbs ─────────────────────────────────────────────────────

    def reason(self, *, job: Job, question: str, facts: Sequence[dict[str, Any]],
               context: Sequence[dict[str, Any]] = (), schema_id: str = "pathfinder.reason.v1",
               constitution_version: str, prompt_version: str, budget: Budget,
               batch: bool = False, key: str = "") -> ReasonResult:
        entry = self._fetch(job, key or question)
        p = enforce_reason(dict(entry["payload"]), facts, job=job)
        usage = self._usage(entry)
        self._ledger.record(usage)
        return ReasonResult(
            job=job, model=usage.model, usage=usage, prompt_version=prompt_version,
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
        entry = self._fetch(Job.narrate, key or beat)
        p = enforce_narrate(dict(entry["payload"]), facts)
        usage = self._usage(entry)
        self._ledger.record(usage)
        return NarrateResult(
            job=Job.narrate, model=usage.model, usage=usage, prompt_version=prompt_version,
            constitution_version=constitution_version, raw_json=p,
            beat=str(p["beat"]), headline=str(p["headline"]), body=str(p["body"]),
            fact_refs=tuple(p.get("fact_refs") or ()),
        )

    def classify(self, *, text: str, labels: Sequence[str], facts: Sequence[dict[str, Any]] = (),
                 schema_id: str = "pathfinder.classify.v1", constitution_version: str,
                 prompt_version: str, budget: Budget, batch: bool = True,
                 key: str = "") -> ClassifyResult:
        entry = self._fetch(Job.classify, key or text)
        p = enforce_classify(dict(entry["payload"]), labels)
        usage = self._usage(entry)
        self._ledger.record(usage)
        return ClassifyResult(
            job=Job.classify, model=usage.model, usage=usage, prompt_version=prompt_version,
            constitution_version=constitution_version, raw_json=p,
            label=str(p["label"]), labels=tuple(p.get("labels") or ()),
            rationale=str(p["rationale"]), score=p.get("score"),
        )


class NullProvider:
    """
    No model is configured. Every verb raises.

    This is not a degraded mode that quietly writes the sentence itself — §3.8 forbids
    a silent fallback to a template. The caller catches `GatewayError` and records the
    beat as engine-authored, visibly, in the payload the customer sees.
    """

    provider_name = "none"

    def __init__(self, reason: str = "no LLM provider configured") -> None:
        self._reason = reason

    def _fail(self, verb: str):
        raise GatewayError(f"{verb}: {self._reason}")

    def reason(self, **kw): self._fail("reason")           # noqa: D102, E704
    def narrate(self, **kw): self._fail("narrate")         # noqa: D102, E704
    def classify(self, **kw): self._fail("classify")       # noqa: D102, E704
