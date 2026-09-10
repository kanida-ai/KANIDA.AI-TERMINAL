"""
Rows -> contract models. Reads only; the Pydantic contracts refuse anything dishonest.

Two shapes, two audiences (spec addendum 6):
  * `card()`   — the PUBLIC card: theme + evidence + the seven-line story. No constituent, no
                 entry, no target, no stop; the schema has no field for them and lints the text.
  * `record()` — the in-app record: versions, trials, periods, expected vs actual, learning,
                 the proposal, the post-mortem. Constituent names are WITHHELD unless the
                 serving process is told RA review has happened (`KANIDA_PF_RA_REVIEWED=1`).
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from typing import Optional

import numpy as np

from ..schemas import (
    BACKFILL_LABEL, FORWARD_LABEL, Author, BasketView, ChangeLogEntry, ComparisonCategory, DeathCause, Direction,
    ExpectedVsActual, Expectation, ExperimentCard, ExperimentGradingRule, ExperimentRecord, ExperimentScoreCounts,
    ExperimentState, ExperimentStoryLine, ExperimentsResponse, Fact, ForwardResult, GateView, LearningLevel,
    LearningView, PeriodView, PostMortem, ProposalView, RejectedCandidate, StoryBeat, StoryLine, TrialView, Verdict,
    VersionView,
)
from .hypotheses import CONDITION_BY_ID, Variant
from .store import ExperimentStore


def _facts(raw: str) -> list[Fact]:
    return [Fact.model_validate(x) for x in json.loads(raw)]


def _gates(raw: str) -> list[GateView]:
    return [GateView(**g) for g in json.loads(raw)]


def ra_reviewed() -> bool:
    return os.environ.get("KANIDA_PF_RA_REVIEWED", "").strip().lower() in ("1", "true", "yes")


def _outcomes_as_of(store: ExperimentStore, eid: str, as_of: Optional[str]) -> list:
    """The outcomes a reader on `as_of` could have seen (point-in-time: graded on a seal <= as_of)."""
    return [o for o in store.outcomes(eid) if as_of is None or o["data_as_of"] <= as_of]


def _score(outs) -> ExperimentScoreCounts:
    c = {"right": 0, "wrong": 0, "inconclusive": 0, "void": 0}
    for o in outs:
        c[o["verdict"]] += 1
    return ExperimentScoreCounts(right=c["right"], wrong=c["wrong"], inconclusive=c["inconclusive"],
                                 n=c["right"] + c["wrong"] + c["inconclusive"], void=c["void"])


def _latest_comparison(outs) -> ComparisonCategory:
    if not outs:
        return ComparisonCategory.pending
    return ComparisonCategory(json.loads(outs[-1]["comparison_json"])["category"])


def state_as_of(store: ExperimentStore, eid: str, as_of: Optional[str]) -> ExperimentState:
    pm = store.post_mortem(eid)
    if pm is not None and (as_of is None or pm["buried_edition"] <= as_of):
        return ExperimentState.buried
    pr = store.proposal(eid)
    if pr is not None and (as_of is None or pr["edition_date"] <= as_of):
        return ExperimentState.proposed
    return ExperimentState.testing


def card(store: ExperimentStore, eid: str, *, edition: Optional[str] = None) -> Optional[ExperimentCard]:
    """
    The public card. With `edition`, everything on it is AS OF that edition — the story written
    on it, the state, the score, the trial count — so a card served in the feed for a past
    edition never shows what was learned later (point-in-time is law on the served surface too).
    """
    e = store.experiment(eid)
    if e is None:
        return None
    n = store.narrative(eid, edition)
    if n is None:
        return None
    from ..schemas import FindingProvenance
    as_of = edition
    news = n["edition_date"]
    lines = [ExperimentStoryLine.model_validate(x) for x in json.loads(n["story_json"])]
    trials = [t for t in store.trials_for_experiment(eid, e["source_finding_id"]) if as_of is None or t["edition_date"] <= as_of]
    versions = [v for v in store.versions(eid) if as_of is None or v["created_edition"] <= as_of]
    outs = _outcomes_as_of(store, eid, as_of)
    # the card's backfill flag is the NEWS edition's (audit finding 1): a card written on a forward
    # edition is forward even when the experiment was opened in a backfill — and says both
    ed_flag = store.edition_backfilled(news)
    backfilled = bool(e["backfilled"]) if ed_flag is None else ed_flag
    return ExperimentCard(
        id=eid, opened_edition=date.fromisoformat(e["opened_edition"]), news_edition=date.fromisoformat(news),
        state=state_as_of(store, eid, as_of), family=e["family_id"], theme=e["theme"], source_finding_id=e["source_finding_id"],
        evidence=FindingProvenance.model_validate_json(e["evidence_json"]), story=lines,
        facts=_withhold_constituents(_facts(n["facts_json"]), {t["symbol"] for t in store.trades(eid)}),
        versions_count=len(versions), trials_total=len(trials),
        periods_graded=sum(1 for o in outs if o["verdict"] != "void"), score=_score(outs),
        latest_comparison=_latest_comparison(outs), backfilled=backfilled, opened_backfilled=bool(e["backfilled"]),
        record_label=BACKFILL_LABEL if backfilled else FORWARD_LABEL, llm_provider=n["llm_provider"])


def _withhold_constituents(facts: list[Fact], constituents: set[str]) -> list[Fact]:
    """
    Addendum 6 on fact VALUES (audit finding 9): a text fact whose value names a stock the experiment's
    book actually held is withheld on the public card — same id (the story's token still resolves), the
    value replaced — so a constituent can never leak through the S1 card's subject.
    """
    out = []
    for f in facts:
        if isinstance(f.value, str) and f.value in constituents:
            out.append(f.model_copy(update={"value": "a constituent — withheld pending RA review",
                                            "note": "the name is on the in-app record after RA review"}))
        else:
            out.append(f)
    return out


def _period_view(store: ExperimentStore, eid: str, v: int, p, rule: ExperimentGradingRule, capital: float) -> PeriodView:
    pno = int(p["period_no"])
    o = store.outcome(eid, v, pno)
    marks = store.marks(eid, v, pno)
    trades = store.trades(eid, v, pno)
    backfilled = bool(p["backfilled"])
    if o is not None:
        fwd = ForwardResult.model_validate_json(o["forward_json"])
        cmp_ = ExpectedVsActual.model_validate_json(o["comparison_json"])
        lj = json.loads(o["learning_json"])
        learning = LearningView(statement=lj["statement"], fact_refs=lj["fact_refs"], level=LearningLevel(lj["level"]),
                                trials_evaluated=int(lj["trials_evaluated"]), trials_passing=int(lj["trials_passing"]),
                                adopted_rule=lj.get("adopted_rule"), buried=bool(lj["buried"]), next_action=lj["next_action"])
        verdict = Verdict(o["verdict"])
        return PeriodView(period_no=pno, start=date.fromisoformat(o["period_start"]), sessions=int(p["sessions"]),
                          end=date.fromisoformat(o["period_end"]), due=date.fromisoformat(o["due_session"]),
                          status=("void" if verdict == Verdict.void else "graded"), forward=fwd, grading_rule=rule, verdict=verdict,
                          grader_version=o["grader_version"],
                          graded_at=datetime.fromisoformat(o["graded_at"]), data_as_of=date.fromisoformat(o["data_as_of"]),
                          realized_facts=_facts(o["realized_facts_json"]), expected_vs_actual=cmp_, learning=learning,
                          backfilled=bool(o["backfilled"]))
    if marks:
        eq = np.array([float(m["equity_inr"]) for m in marks]) / capital
        peak = np.maximum.accumulate(np.r_[1.0, eq])[1:]
        resolved = [t for t in trades if t["resolved"]]
        net = np.array([float(t["pnl_pct_net"]) for t in resolved], dtype=float)
        fwd = ForwardResult(as_of=date.fromisoformat(marks[-1]["session"]), capital_inr=capital, n_closed=len(resolved),
                            n_open=int(marks[-1]["open_positions"]), signal_days=len({t["signal_date"] for t in resolved}),
                            signals_seen=0, signals_taken=len(trades) + int(marks[-1]["open_positions"]),
                            mean_net_pct=(float(net.mean()) if net.size else None),
                            hit_rate_pct=(float((net > 0).mean() * 100) if net.size else None),
                            book_return_pct=float((eq[-1] - 1) * 100), max_drawdown_pct=float(((peak - eq) / peak * 100).max()),
                            current_drawdown_pct=float((peak[-1] - eq[-1]) / peak[-1] * 100), n_unresolved=len(trades) - len(resolved))
        return PeriodView(period_no=pno, start=date.fromisoformat(marks[0]["session"]), sessions=int(p["sessions"]), status="open",
                          forward=fwd, grading_rule=rule, backfilled=backfilled)
    fwd = ForwardResult(as_of=date.fromisoformat(p["opened_edition"]), capital_inr=capital, n_closed=0, n_open=0, signal_days=0,
                        signals_seen=0, signals_taken=0, max_drawdown_pct=0.0, current_drawdown_pct=0.0)
    return PeriodView(period_no=pno, sessions=int(p["sessions"]), status="pending", forward=fwd, grading_rule=rule, backfilled=backfilled)


def _version_view(store: ExperimentStore, eid: str, vrow, capital: float) -> VersionView:
    v = int(vrow["version"])
    variant = Variant.from_dict(json.loads(vrow["variant_json"]))
    rule = ExperimentGradingRule.model_validate_json(vrow["grading_rule_json"])
    return VersionView(
        version=v, created_edition=date.fromisoformat(vrow["created_edition"]), rule_text=variant.rule_text(),
        conditions=[CONDITION_BY_ID[c].label for c in variant.conditions], horizon_sessions=variant.horizon,
        change=vrow["change"], why=vrow["why"], level=LearningLevel(vrow["level"]), validation=vrow["validation"],
        trials_for_this_version=int(vrow["trials_for_version"]), expectation=Expectation.model_validate_json(vrow["expectation_json"]),
        periods=[_period_view(store, eid, v, p, rule, capital) for p in store.periods(eid, v)],
        status=store.version_status(eid, v), backfilled=bool(vrow["backfilled"]))


def _trial_view(t) -> TrialView:
    st = json.loads(t["stats_json"])
    variant = Variant.from_dict(json.loads(t["variant_json"]))
    return TrialView(trial_no=int(t["trial_no"]), context=t["context"], signature=t["signature"], rule_text=variant.rule_text(),
                     passed=bool(t["passed"]), adopted=bool(t["adopted"]), reason=t["reason"],
                     expectancy_net_pct=st["whole"]["expectancy_net"], trailing_expectancy_net_pct=st["trailing"]["expectancy_net"],
                     n=int(st["whole"]["n"]), gates=[GateView(**g) for g in json.loads(t["gates_json"])])


def record(store: ExperimentStore, eid: str) -> Optional[ExperimentRecord]:
    c = card(store, eid)
    if c is None:
        return None
    e = store.experiment(eid)
    capital = float(e["capital_inr"])
    versions = [_version_view(store, eid, vr, capital) for vr in store.versions(eid)]
    latest = versions[-1]
    trades = store.trades(eid)
    names = sorted({t["symbol"] for t in trades})
    reviewed = ra_reviewed()
    basket = BasketView(description=e["theme"] + " — the names are whichever stocks the rule selected, session by session",
                        constituents_visibility=("in_app_ra_reviewed" if reviewed else "withheld_pending_ra_review"),
                        ra_review_state=("reviewed (served because KANIDA_PF_RA_REVIEWED is set)" if reviewed
                                         else "pending — constituent names are withheld until a Research Analyst reviews them"),
                        constituents=(names if reviewed else []))
    change_log: list[ChangeLogEntry] = []

    def _forward_mean(version: int) -> Optional[float]:
        r = [float(t["pnl_pct_net"]) for t in store.trades(eid, version, resolved_only=True)]
        return float(np.mean(r)) if r else None

    for vw, vr in zip(versions, store.versions(eid)):
        refs = [f["id"] for f in json.loads(vr["expectation_facts_json"])["expectation"]][:6]
        # `improved` (audit finding 11): computed, never guessed — a version that replaced another is
        # judged on its own forward record against its predecessor's, once it has one; null until then
        improved: Optional[bool] = None
        if vw.version > 1:
            mine, prev = _forward_mean(vw.version), _forward_mean(vw.version - 1)
            graded = any(o["verdict"] != "void" for o in store.outcomes(eid) if int(o["version"]) == vw.version)
            if graded and mine is not None and prev is not None:
                improved = mine > prev
        change_log.append(ChangeLogEntry(
            seq=vw.version, level=vw.level, at=datetime.fromisoformat(vr["created_at"]), what_changed=vw.change, why=vw.why,
            evidence_refs=refs or [f"ver:{eid}:v{vw.version}"], previous_version=(None if vw.version == 1 else f"v{vw.version - 1}"),
            new_version=f"v{vw.version}", improved=improved, decided_by=Author.engine, constitution_version=e["constitution_version"],
            validation=vw.validation))
    pm_row = store.post_mortem(eid)
    pm = None
    if pm_row is not None:
        pm = PostMortem(died_at=datetime.fromisoformat(pm_row["created_at"]), cause=DeathCause(pm_row["cause"]),
                        summary=StoryLine(beat=StoryBeat.learning, headline="Why the idea is buried", body=pm_row["summary"],
                                          produced_by=Author.engine, at=datetime.fromisoformat(pm_row["created_at"]),
                                          fact_refs=json.loads(pm_row["fact_refs_json"])),
                        what_we_kept=pm_row["what_we_kept"], evidence_refs=json.loads(pm_row["fact_refs_json"]) or [f"exp:{eid}"],
                        retired_version=f"v{pm_row['retired_version']}", decided_by=Author.engine)
    pr_row = store.proposal(eid)
    pr = None
    if pr_row is not None:
        pr = ProposalView(proposed_edition=date.fromisoformat(pr_row["edition_date"]), version=int(pr_row["version"]),
                          target_agent=pr_row["target_agent"], status=pr_row["status"], gates=_gates(pr_row["gates_json"]),
                          incumbent=pr_row["incumbent"])
    trials = [_trial_view(t) for t in store.trials_for_experiment(eid, e["source_finding_id"])]
    return ExperimentRecord(
        **c.model_dump(), question=e["question"], current_rule_text=latest.rule_text, direction=Direction(e["direction"]),
        versions=versions, trials=trials, basket=basket, worth_testing_gates=_gates(e["gates_json"]), change_log=change_log,
        post_mortem=pm, proposal=pr, constitution_version=e["constitution_version"])


def rejected(store: ExperimentStore, as_of: Optional[str] = None) -> list[RejectedCandidate]:
    out = []
    for r in store.candidates():
        if r["opened_experiment_id"] is not None or (as_of and r["edition_date"] > as_of):
            continue
        out.append(RejectedCandidate(finding_id=r["finding_id"], edition_date=date.fromisoformat(r["edition_date"]),
                                     template_id=r["template_id"], family=r["family_id"], trials_evaluated=int(r["trials_evaluated"]),
                                     reason=r["reason"], best_rule_text=r["best_rule_text"], best_expectancy_net_pct=r["best_expectancy_net"],
                                     best_failed_gates=json.loads(r["best_failed_gates_json"])))
    return out


def cards_on(store: ExperimentStore, edition: str) -> list[ExperimentCard]:
    return [c for n in store.narratives_on(edition) if (c := card(store, n["experiment_id"], edition=edition)) is not None]


def experiments_response(store: ExperimentStore, *, engine_version: str) -> Optional[ExperimentsResponse]:
    as_of = store.latest_edition()
    if as_of is None:
        return None
    items = [c for e in store.experiments() if (c := card(store, e["experiment_id"])) is not None]
    order = {ExperimentState.buried: 0, ExperimentState.testing: 1, ExperimentState.proposed: 2}
    items.sort(key=lambda c: (order[c.state], c.opened_edition, c.id))
    provider = next((c.llm_provider for c in items if c.llm_provider != "none"), "none")
    return ExperimentsResponse(as_of=date.fromisoformat(as_of), count=len(items), items=items, not_opened=rejected(store, as_of),
                               scoreboard=store.scoreboard(as_of), llm_provider=provider, engine_version=engine_version)
