"""
The after-close scan.

    seal(as_of) -> regime -> for each LIBRARY template: compute -> score -> threshold
    -> (model may veto) -> narrate -> publish edition -> grade every prior finding whose
    horizon completed -> snapshot the scoreboard

Editions are append-only: a date that already has one is refused, not recomputed.
Nothing here computes a statistic — the templates do (library.py), the grader does
(grading.py). This file wires them and keeps the record honest.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from ..schemas import (
    DateRange, Finding, FindingProvenance, GradingState, GradingStatus, Tier,
)
from .config import ResearchConfig, now_ist
from .data import MarketData
from .grading import evaluate
from . import library as LIB
from .library import CardDraft, ScanContext, parameters_for
from .narrate import Narrator
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
    held: list[str] = field(default_factory=list)
    below_threshold: list[str] = field(default_factory=list)
    graded: list[tuple[str, str]] = field(default_factory=list)
    narration_failures: list[str] = field(default_factory=list)
    llm_provider: str = "none"
    skipped: bool = False


def _finding_id(draft: CardDraft) -> str:
    return f"fnd_{draft.slug}"


def grade_due(store: ResearchStore, md: MarketData, cfg: ResearchConfig, *,
              graded_at: Optional[datetime] = None) -> list[tuple[str, str]]:
    """Grade every published finding whose horizon completed on or before `md.as_of`."""
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

    if store.has_edition(edition):
        rep = ScanReport(edition, md.as_of, "", md.n_symbols_today, 0, skipped=True)
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
    scored = []
    for d, t in drafts:
        key = novelty_key(d, snap.state)
        nov = store.novelty(key, before=edition, lookback_editions=cfg.novelty_lookback_editions)
        s = score(d, t, novelty=nov, cfg=cfg)
        scored.append((d, t, key, s))
    scored.sort(key=lambda x: (-x[3].total, x[0].template_id, x[0].subject))

    rep = ScanReport(edition, md.as_of, snap.label, md.n_symbols_today, len(scored),
                     llm_provider=narrator.provider_name)
    # `engine_version` carries the content hash of research/*.py (S1 audit C1): an edition is
    # attributable to the exact code that computed it, and two runs of the same code on the
    # same seal must produce identical cards (test_pathfinder_s1_audit: determinism).
    store.put_edition(
        edition_date=edition, data_as_of=md.as_of, generated_at=computed_at.isoformat(),
        engine_version=cfg.engine_version, llm_provider=narrator.provider_name, regime=snap.label,
        universe_scanned=md.n_symbols_today, candidates=len(scored), threshold=cfg.usefulness_threshold,
        params_json=json.dumps({t.id: {k: v for k, v in parameters_for(t, cfg).items() if k != "pairs"}
                                for t in LIB.LIBRARY} | {"pairs": list(cfg.pairs), "cost_hurdle_pct": cfg.cost_hurdle_pct},
                               sort_keys=True, default=str))

    rank = 0
    for d, t, key, s in scored:
        fid = _finding_id(d)
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
            provenance=FindingProvenance(
                level=d.level, n=d.n,
                period=DateRange(start=d.facts.period[0], end=d.facts.period[1]),
                regime=snap.label, comparison_group=d.comparison_group,
                cost_hurdle_pct=cfg.cost_hurdle_pct, cost_convention=cfg.cost_convention,
                data_source=cfg.data_source, universe=cfg.universe_id, as_of=date.fromisoformat(md.as_of),
                computed_by=d.facts.component, computed_at=computed_at),
            grading_rule=d.grading_rule,
            grading=GradingState(status=GradingStatus.pending,
                                 due_session=(date.fromisoformat(due) if due else None)),
            usefulness=s, related_symbols=d.related_symbols, follow_up_questions=d.follow_ups,
        )
        store.put_finding(finding, novelty_key=key, selected_by=sel.decided_by)
        store.put_candidate(edition_date=edition, template_id=d.template_id, subject=d.subject,
                            decision=d.decision.value, novelty_key=key, usefulness=s.total,
                            score=s.model_dump(), published=True, reason=f"published (selected by {sel.decided_by})",
                            finding_id=fid)
        rep.published.append(f"{fid} [{d.decision.value}] {s.total:.2f}")
    # The published count is DERIVED from pf_findings at read time — the edition row is
    # append-only and is never updated.
    rep.narration_failures = list(narrator.failures)
    store.commit()

    # 3. Grade whatever completed its horizon on this close.
    rep.graded = grade_due(store, md, cfg, graded_at=computed_at)
    return rep
