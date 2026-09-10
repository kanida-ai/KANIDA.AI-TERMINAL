"""
The after-close scan.

    seal(as_of) -> regime -> for each LIBRARY template: compute -> score -> threshold
    -> (model may veto) -> narrate -> publish edition -> grade every prior finding whose
    horizon completed -> snapshot the scoreboard

Editions are append-only: a date that already has one is refused, not recomputed.
Nothing here computes a statistic — the templates do (library.py), the grader does
(grading.py). This file wires them and keeps the record honest.

S1 second audit:
  * A1 — an edition generated after its own session date (IST) is a SIMULATED BACKFILL and is
    labelled so on the edition, on every card and on the scoreboard. Only a scan run on the
    session's own date is a forward record.
  * A2 — a draft carrying the same claim (evidence signature) as a finding whose horizon is
    still running is a CONTINUATION: published as `continue`, pointing at the root finding,
    not counted as a publication and never graded on its own. Once the root's horizon
    completes, the same claim is a new publication and is graded independently — one grade
    per claim per non-overlapping horizon.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from ..schemas import (
    DateRange, Decision, Finding, FindingProvenance, GradingState, GradingStatus, Tier,
)
from .config import IST, ResearchConfig, now_ist
from .data import MarketData
from .grading import evaluate
from . import library as LIB
from .library import CardDraft, ScanContext, parameters_for
from .narrate import Narrator, engine_narrative
from .ranking import novelty_key, score
from .regime import build_regime, regime_on
from .store import ResearchStore


@dataclass
class ScanReport:
    edition_date: str
    data_as_of: str
    regime: str
    universe_scanned: int
    candidates: int
    published: list[str] = field(default_factory=list)
    continued: list[str] = field(default_factory=list)
    held: list[str] = field(default_factory=list)
    below_threshold: list[str] = field(default_factory=list)
    graded: list[tuple[str, str]] = field(default_factory=list)
    narration_failures: list[str] = field(default_factory=list)
    llm_provider: str = "none"
    skipped: bool = False
    backfilled: bool = True


def _finding_id(draft: CardDraft) -> str:
    return f"fnd_{draft.slug}"


def is_backfilled(edition_date: str, computed_at: datetime) -> bool:
    """
    A1: forward means generated on the edition's own session date (IST). Anything later —
    even one session later, even with a warehouse that holds no later bar — is a backfill:
    the outcome was knowable in the world when the finding was written.
    """
    at = computed_at if computed_at.tzinfo is not None else computed_at.replace(tzinfo=IST)
    return at.astimezone(IST).date().isoformat() > edition_date


def grade_due(store: ResearchStore, md: MarketData, cfg: ResearchConfig, *,
              graded_at: Optional[datetime] = None) -> list[tuple[str, str]]:
    """Grade every published ROOT finding whose horizon completed on or before `md.as_of`."""
    from ..schemas import GradingRule
    graded_at = graded_at or now_ist()
    out: list[tuple[str, str]] = []
    for row in store.pending():
        rule = GradingRule.model_validate_json(row["grading_rule_json"])   # the FROZEN rule
        if row["edition_date"] > md.as_of:
            continue
        res = evaluate(rule, finding_slug=row["finding_id"][len("fnd_"):],
                       edition_date=row["edition_date"], md=md, cfg=cfg, graded_at=graded_at)
        if res is None:
            continue
        store.put_grade(finding_id=row["finding_id"], graded_at=graded_at, data_as_of=md.as_of,
                        due_session=res.due_session, verdict=res.verdict,
                        rule_version=rule.rule_version, realized=res.facts.facts)
        out.append((row["finding_id"], res.verdict.value))
    if out:
        store.put_scoreboard_snapshot(store.scoreboard(md.as_of))
    store.commit()
    return out


def run_scan(cfg: ResearchConfig, store: ResearchStore, *, md: Optional[MarketData] = None,
             as_of: Optional[str] = None, narrator: Optional[Narrator] = None,
             computed_at: Optional[datetime] = None) -> ScanReport:
    md = md or MarketData.load(cfg, as_of=as_of)
    if as_of and md.as_of != str(as_of)[:10]:
        md = md.sealed(as_of)
    computed_at = computed_at or now_ist()
    narrator = narrator or Narrator(None, provider_name="none")
    edition = md.as_of
    backfilled = is_backfilled(edition, computed_at)

    if store.has_edition(edition):
        rep = ScanReport(edition, md.as_of, "", md.n_symbols_today, 0, skipped=True, backfilled=backfilled)
        rep.graded = grade_due(store, md, cfg, graded_at=computed_at)
        return rep

    reg = build_regime(md)
    snap = regime_on(reg, edition)
    ctx = ScanContext(md=md, cfg=cfg, regime=snap, computed_at=computed_at)

    # 1. Ask every approved question. The library is the ONLY source of computations.
    drafts: list[tuple[CardDraft, object]] = []
    for t in LIB.LIBRARY:
        for d in t.computation(ctx, parameters_for(t, cfg)):
            drafts.append((d, t))

    # 2. Score deterministically; the threshold gates publication. No quota, no padding.
    def _open(prior_edition: str, horizon: int) -> bool:
        due = md.session_after(prior_edition, horizon)
        return due is None or due > edition

    scored = []
    for d, t in drafts:
        key = novelty_key(d, snap.state)
        nov = store.novelty(key, before=edition, lookback_editions=cfg.novelty_lookback_editions)
        s = score(d, t, novelty=nov, cfg=cfg)
        root = store.open_root(key, before=edition, lookback_editions=cfg.novelty_lookback_editions, is_open=_open)
        scored.append((d, t, key, s, root))
    scored.sort(key=lambda x: (-x[3].total, x[0].template_id, x[0].subject))

    rep = ScanReport(edition, md.as_of, snap.label, md.n_symbols_today, len(scored),
                     llm_provider=narrator.provider_name, backfilled=backfilled)
    # `engine_version` carries the content hash of research/*.py (S1 audit C1): an edition is
    # attributable to the exact code that computed it, and two runs of the same code on the
    # same seal must produce identical cards (test_pathfinder_s1_audit: determinism).
    params = {t.id: {k: v for k, v in parameters_for(t, cfg).items() if k != "pairs"} for t in LIB.LIBRARY}
    params |= {"pairs": list(cfg.pairs), "cost_hurdle_pct": cfg.cost_hurdle_pct, "slippage_pct": cfg.slippage_pct,
               "hurdle_pct": cfg.hurdle_pct,
               "data_exclusions": md.exclusions.as_dict() if md.exclusions else None}
    store.put_edition(
        edition_date=edition, data_as_of=md.as_of, generated_at=computed_at.isoformat(),
        engine_version=cfg.engine_version, llm_provider=narrator.provider_name, regime=snap.label,
        universe_scanned=md.n_symbols_today, candidates=len(scored), threshold=cfg.usefulness_threshold,
        params_json=json.dumps(params, sort_keys=True, default=str),
        backfilled=int(backfilled), data_through=md.data_through or md.as_of)

    def _provenance(d: CardDraft) -> FindingProvenance:
        return FindingProvenance(
            level=d.level, n=d.n,
            period=DateRange(start=d.facts.period[0], end=d.facts.period[1]),
            regime=snap.label, comparison_group=d.comparison_group,
            cost_hurdle_pct=cfg.hurdle_pct, cost_convention=cfg.cost_convention,
            data_source=cfg.data_source, universe=cfg.universe_id, as_of=date.fromisoformat(md.as_of),
            computed_by=d.facts.component, computed_at=computed_at, disclosures=cfg.data_disclosures)

    rank = 0
    continuations: list[tuple[CardDraft, str, object, object]] = []
    for d, t, key, s, root in scored:
        fid = _finding_id(d)
        if root is not None:
            continuations.append((d, key, s, root))
            continue
        if s.total < s.threshold:
            rep.below_threshold.append(f"{fid} ({s.total:.2f})")
            store.put_candidate(edition_date=edition, template_id=d.template_id, subject=d.subject,
                                decision=d.decision.value, novelty_key=key, usefulness=s.total,
                                score=s.model_dump(), published=False, reason="below usefulness threshold",
                                finding_id=None)
            continue
        sel = narrator.select(d, fid)
        if not sel.publish:
            rep.held.append(f"{fid}: {sel.reason}")
            store.put_candidate(edition_date=edition, template_id=d.template_id, subject=d.subject,
                                decision=d.decision.value, novelty_key=key, usefulness=s.total,
                                score=s.model_dump(), published=False, reason=f"held by {sel.decided_by}: {sel.reason}",
                                finding_id=None)
            continue
        rank += 1
        tier = Tier.what_matters_now if rank <= cfg.what_matters_now else Tier.discovery
        narrative = narrator.narrate(d, fid, computed_at)
        due = md.session_after(edition, d.grading_rule.horizon_sessions)
        finding = Finding(
            id=fid, edition_date=date.fromisoformat(edition), rank=rank, tier=tier,
            template_id=d.template_id, question=d.question, subject=d.subject, subject_kind=d.subject_kind,
            decision=d.decision, decision_reason=d.decision_reason, narrative=narrative,
            facts=d.facts.facts, key_fact_refs=d.key_facts,
            provenance=_provenance(d), grading_rule=d.grading_rule,
            grading=GradingState(status=GradingStatus.pending,
                                 due_session=(date.fromisoformat(due) if due else None), backfilled=backfilled),
            usefulness=s, related_symbols=d.related_symbols, follow_up_questions=d.follow_ups,
            backfilled=backfilled,
        )
        store.put_finding(finding, novelty_key=key, selected_by=sel.decided_by)
        store.put_candidate(edition_date=edition, template_id=d.template_id, subject=d.subject,
                            decision=d.decision.value, novelty_key=key, usefulness=s.total,
                            score=s.model_dump(), published=True, reason=f"published (selected by {sel.decided_by})",
                            finding_id=fid)
        rep.published.append(f"{fid} [{d.decision.value}] {s.total:.2f}")

    # 2b. Continuations (A2): the same claim as an OPEN finding. Served after the new findings,
    # never in "what matters now", never a publication, never graded on their own.
    for d, key, s, root in continuations:
        fid = _finding_id(d)
        rank += 1
        root_due = md.session_after(root["edition_date"], int(root["horizon_sessions"]))
        base = engine_narrative(d, computed_at)
        narrative = base.model_copy(update={"headline": "Continuing: " + base.headline})
        finding = Finding(
            id=fid, edition_date=date.fromisoformat(edition), rank=rank, tier=Tier.discovery,
            template_id=d.template_id, question=d.question, subject=d.subject, subject_kind=d.subject_kind,
            decision=Decision.continue_,
            decision_reason=("The same claim on the same evidence as a finding whose horizon is still running; "
                             "a continuation, not a new publication, graded once through the finding it continues."),
            narrative=narrative, facts=d.facts.facts, key_fact_refs=d.key_facts,
            provenance=_provenance(d), grading_rule=d.grading_rule,
            grading=GradingState(status=GradingStatus.continued, continues=root["finding_id"],
                                 due_session=(date.fromisoformat(root_due) if root_due else None),
                                 backfilled=backfilled),
            usefulness=s, related_symbols=d.related_symbols, follow_up_questions=d.follow_ups,
            backfilled=backfilled, continues=root["finding_id"],
        )
        store.put_finding(finding, novelty_key=key, selected_by="engine")
        store.put_candidate(edition_date=edition, template_id=d.template_id, subject=d.subject,
                            decision=Decision.continue_.value, novelty_key=key, usefulness=s.total,
                            score=s.model_dump(), published=False,
                            reason=f"continuation of {root['finding_id']} (open horizon) — not a new publication",
                            finding_id=fid)
        rep.continued.append(f"{fid} continues {root['finding_id']}")
    # The published count is DERIVED from pf_findings at read time — the edition row is
    # append-only and is never updated.
    rep.narration_failures = list(narrator.failures)
    store.commit()

    # 3. Grade whatever completed its horizon on this close.
    rep.graded = grade_due(store, md, cfg, graded_at=computed_at)
    return rep
