"""
The output contract, enforced on EVERY completion regardless of provider.

This is the third of the three independent places the core principle is enforced
(database CHECK · API schema validator · here). A provider that skipped it would be
the one hole in the wall, so no provider is allowed to construct a result without
passing through `enforce()`.
"""
from __future__ import annotations

import json
import re
from typing import Any, Iterable, Sequence

from .gateway import Job, OutputContractViolation

FACT_REF_RE = re.compile(r"\{\{fact:(?P<id>fct_[a-z0-9_]+)\}\}")
REF_TOKEN_RE = re.compile(r"\{\{(?:fact|exp|evd|ver):[A-Za-z0-9_.\-]+\}\}")

#: `reason` may not express an L4 change — the enum makes it unrepresentable.
REASON_LEVELS = {"L1", "L2", "L3", None}
BEATS = {"noticed", "hypothesis", "experiment", "outcome", "learning", "next"}


def strip_refs(text: str) -> str:
    return REF_TOKEN_RE.sub("", text)


def assert_no_bare_numerals(text: str, *, where: str) -> None:
    """
    The machine-checkable form of "the LLM never calculates".

    A model may say *"the edge is smaller out of sample"*. It may not say *"the edge
    fell to 0.7%"* — that number must be `{{fact:fct_…}}`, resolved from a row the
    engine computed.
    """
    residue = strip_refs(text)
    if any(ch.isdigit() for ch in residue):
        offenders = sorted({ch for ch in residue if ch.isdigit()})
        raise OutputContractViolation(
            f"{where}: LLM-authored prose contains literal numerals {offenders}. "
            "Numbers must be {{fact:<id>}} references to deterministic facts."
        )


def assert_refs_supplied(refs: Iterable[str], facts: Sequence[dict[str, Any]], *, where: str) -> None:
    """A model may not return a fact id it was never given. That would be invention."""
    known = {str(f["id"]) for f in facts}
    unknown = sorted(set(refs) - known)
    if unknown:
        raise OutputContractViolation(
            f"{where}: referenced facts that were not supplied to the call: {unknown}"
        )


def assert_refs_used_are_declared(text_fields: Sequence[str], declared: Sequence[str], *, where: str) -> None:
    used: set[str] = set()
    for t in text_fields:
        used |= {m.group("id") for m in FACT_REF_RE.finditer(t)}
    missing = sorted(used - set(declared))
    if missing:
        raise OutputContractViolation(f"{where}: fact refs used but not declared: {missing}")


def enforce_reason(payload: dict[str, Any], facts: Sequence[dict[str, Any]], *, job: Job) -> dict[str, Any]:
    for key in ("claim", "body", "fact_refs", "abstained"):
        if key not in payload:
            raise OutputContractViolation(f"reason: missing required field {key!r}")
    if payload.get("learning_level") not in REASON_LEVELS:
        raise OutputContractViolation(
            f"reason: learning_level {payload.get('learning_level')!r} is not expressible — "
            "L4 is human-only and the schema does not permit it"
        )
    refs = list(payload.get("fact_refs") or [])
    assert_refs_supplied(refs, facts, where=f"reason({job.value})")
    for field in ("claim", "body"):
        assert_no_bare_numerals(str(payload[field]), where=f"reason({job.value}).{field}")
    assert_refs_used_are_declared(
        [str(payload["claim"]), str(payload["body"])], refs, where=f"reason({job.value})"
    )
    return payload


def enforce_narrate(payload: dict[str, Any], facts: Sequence[dict[str, Any]]) -> dict[str, Any]:
    for key in ("beat", "headline", "body", "fact_refs"):
        if key not in payload:
            raise OutputContractViolation(f"narrate: missing required field {key!r}")
    if payload["beat"] not in BEATS:
        raise OutputContractViolation(f"narrate: unknown beat {payload['beat']!r}")
    if any(ch.isdigit() for ch in str(payload["headline"])):
        raise OutputContractViolation(
            "narrate: `headline` may not contain a digit at all — not even a fact token"
        )
    refs = list(payload.get("fact_refs") or [])
    assert_refs_supplied(refs, facts, where="narrate")
    assert_no_bare_numerals(str(payload["body"]), where="narrate.body")
    assert_refs_used_are_declared([str(payload["body"])], refs, where="narrate")
    return payload


def enforce_classify(payload: dict[str, Any], labels: Sequence[str]) -> dict[str, Any]:
    for key in ("label", "rationale"):
        if key not in payload:
            raise OutputContractViolation(f"classify: missing required field {key!r}")
    if payload["label"] not in labels:
        raise OutputContractViolation(
            f"classify: label {payload['label']!r} is not in the closed set {list(labels)} — "
            "the model may not invent a category"
        )
    assert_no_bare_numerals(str(payload["rationale"]), where="classify.rationale")
    return payload


def render_facts(facts: Sequence[dict[str, Any]]) -> str:
    """
    Deterministic fact-table rendering for the cached prefix.

    Sorted keys, stable formatting. An unsorted `json.dumps` here is a silent cache
    invalidator (§3.4) and the sort is the whole fix.
    """
    return json.dumps(sorted(facts, key=lambda f: str(f["id"])), sort_keys=True, indent=2)
