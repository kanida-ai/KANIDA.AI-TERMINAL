"""
The daily step of the experiment loop, sealed at one edition date D.

    1. TRACK   every open period of every open version: re-walk its virtual book on the frame
               sealed at D (a pure function of rule, period, seal), append the new marks and the
               newly closed trades; if the period completed on or before D — GRADE it under the
               rule frozen when its version opened, COMPARE expected vs actual, LEARN (with every
               candidate revision counted), then CONTINUE / REVISE (a new version) / BURY; then
               run the graduation gate on the version's whole forward record and file a
               PROPOSAL if — and only if — every gate but the human signature passes.
    2. OPEN    for every new ROOT finding S1 published on D whose card names a family: research
               the closed variant set on the frame sealed at D (every variant a trial), apply
               the worth-testing gate, open v1 with a frozen expectation and a frozen grading
               rule — or record why not.
    3. NARRATE the seven-line story for every experiment with news on D, from facts only.

Backfilled (S1 A1): a step run after D's own session date in IST is a simulated backfill and
every row it writes says so. Editions are append-only: a date that already has one is skipped.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Optional

import numpy as np

from ..engine.governance import Constitution, load_constitution
from ..research.config import ResearchConfig, now_ist
from ..research.data import MarketData
from ..research.facts import FactSet
from ..research.scan import is_backfilled
from ..research.store import ResearchStore
from ..schemas import (
    ComparisonCategory, EvidenceLevel, Expectation, ExpectedVsActual, Finding, ForwardResult, DateRange, Verdict,
)
from .book import BookRun, passive_incumbent, run_book
from .config import ExperimentConfig
from .gate import (
    ForwardRecord, gate_rows, graduation_gates, proposal_status, summarize, worth_testing_gates,
)
from .grading import GRADING_RULES_VERSION, build_experiment_rule, compare, cumulative_verdict, judge
from .hypotheses import (
    CONDITION_BY_ID, Conditioning, Variant, VariantEvidence, Windows, block_placebo_means, cluster_robust_t,
    family_for, measure, not_derivable_reason, threshold_of, variants_for, windows_for,
)
from .learning import Learning, learn
from .narrate import Beat, ExperimentNarrator, T, engine_beats
from .store import ExperimentStore
from ..schemas import ExperimentGradingRule

S1_FACT_NAMES = ("subject", "move_today", "threshold", "cases", "hit_1", "expectancy_1", "typical_1", "hurdle", "regime")


@dataclass
class StepReport:
    edition_date: str
    data_as_of: str
    backfilled: bool
    skipped: bool = False
    opened: list[str] = field(default_factory=list)
    declined: list[str] = field(default_factory=list)
    tracked: list[str] = field(default_factory=list)
    graded: list[tuple[str, int, int, str]] = field(default_factory=list)     # (exp, version, period, verdict)
    revised: list[str] = field(default_factory=list)
    buried: list[str] = field(default_factory=list)
    proposed: list[str] = field(default_factory=list)
    narrated: list[str] = field(default_factory=list)
    llm_provider: str = "none"


# ── helpers ───────────────────────────────────────────────────────────────────

def _j(x: Any) -> str:
    return json.dumps(x, sort_keys=True, default=str)


def _facts_json(fs: FactSet) -> str:
    return _j([f.model_dump(mode="json") for f in fs.facts])


def _load_facts(raw: str) -> list:
    from ..schemas import Fact
    return [Fact.model_validate(x) for x in json.loads(raw)]


def constitution_for(xcfg: ExperimentConfig) -> Constitution:
    return load_constitution(xcfg.constitution_path)


def _risk(c: Constitution) -> dict[str, float]:
    r = c.document["risk"]
    return {"capital": float(r["virtual_capital_inr"]), "fraction": float(r["max_fraction_per_position"]),
            "max_concurrent": int(r["max_concurrent_positions"]), "max_new": int(r["max_new_positions_per_session"])}


def _book(variant: Variant, md: MarketData, cx: Conditioning, *, start: str, xcfg: ExperimentConfig,
          rcfg: ResearchConfig, c: Constitution) -> BookRun:
    r = _risk(c)
    return run_book(variant, md, cx, period_start=start, period_sessions=xcfg.period_sessions, rcfg=rcfg,
                    capital_inr=r["capital"], fraction_per_position=r["fraction"], max_concurrent=r["max_concurrent"],
                    max_new_per_session=r["max_new"])


def forward_result(run: BookRun, as_of: str) -> ForwardResult:
    graded = run.graded
    net = np.array([t.pnl_pct_net for t in graded], dtype=float)
    mdd, cdd = run.drawdowns()
    return ForwardResult(
        as_of=date.fromisoformat(as_of), capital_inr=run.capital_inr, n_closed=len(graded), n_open=len(run.open_at_end),
        signal_days=int(len({t.signal_date for t in graded})), signals_seen=run.signals_seen, signals_taken=run.signals_taken,
        mean_net_pct=(float(net.mean()) if net.size else None), hit_rate_pct=(float((net > 0).mean() * 100) if net.size else None),
        book_return_pct=run.total_return_pct, max_drawdown_pct=mdd, current_drawdown_pct=cdd, n_unresolved=len(run.unresolved))


def expectation_facts(fs: FactSet, ev: VariantEvidence, *, xcfg: ExperimentConfig, rcfg: ResearchConfig) -> None:
    w, t, d = ev.whole, ev.trailing, ev.discovery
    v = ev.variant
    fs.add("rule", "the rule under test", v.rule_text(), "text")
    fs.add("condition", "the condition the rule adds", " and ".join(CONDITION_BY_ID[c].label for c in v.conditions), "text")
    fs.add("horizon", "sessions held, next open to horizon close", v.horizon, "sessions", sample="parameter")
    fs.add("cases", "cases on the whole sealed history", w.n, "count")
    fs.add("signal_days", "distinct signal sessions those cases came from", w.signal_days, "count")
    fs.add("hit_rate", "share of cases positive net of costs (supporting, never the headline)", w.hit_pct, "pct", n=w.n)
    fs.add("median_net", "median net result per case", w.median_net, "pct", n=w.n)
    fs.add("expectancy_net", "expectancy per trade net of costs and slippage on the whole sealed history (winsorised mean)",
           w.expectancy_net, "pct", n=w.n)
    fs.add("expectancy_2x", "the same expectancy at twice the slippage", w.expectancy_2x_net, "pct", n=w.n)
    fs.add("baseline_net", "every stock-session in the same window under the same exits, net (the baseline)",
           w.baseline_net, "pct", n=w.baseline_n)
    fs.add("edge", "the rule's mean minus the baseline's, gross of both", w.edge_pct, "pct", n=w.n)
    if w.placebo_p is not None:
        fs.add("placebo_p", "day-blocked placebo: share of same-shape random draws at least this good", w.placebo_p, "ratio", n=w.placebo_draws)
    if w.cluster_t is not None:
        fs.add("cluster_t", "t-statistic with trades clustered on their signal day", w.cluster_t, "ratio", n=w.signal_days)
    fs.add("discovery_end", "last session of the discovery window", xcfg.discovery_end, "text")
    if t.n:
        fs.add("trailing_cases", "cases on the trailing validation window alone", t.n, "count")
        fs.add("trailing_expectancy", "expectancy net of costs on the trailing validation window alone", t.expectancy_net, "pct", n=t.n)
        fs.add("trailing_expectancy_2x", "the same at twice the slippage", t.expectancy_2x_net, "pct", n=t.n)
    if d.n:
        fs.add("discovery_cases", "cases on the discovery window alone", d.n, "count")
        fs.add("discovery_expectancy", "expectancy net of costs on the discovery window alone", d.expectancy_net, "pct", n=d.n)
        fs.add("discovery_expectancy_2x", "the same at twice the slippage", d.expectancy_2x_net, "pct", n=d.n)
    fs.add("hurdle", "round-trip hurdle: costs plus slippage both ways", rcfg.hurdle_pct * 100, "bps")


def trailing_looks_fact(fs: FactSet, n_trials: int) -> None:
    """Audit finding 5: the trailing window was seen by every variant — it is a persistence check, not a holdout."""
    fs.add("trailing_looks", "variants whose trailing-window result was ALSO seen before this one was chosen — the trailing "
                             "number is a persistence check inside the selection history, not an independent holdout",
           n_trials, "count")


def expectation_model(ev: VariantEvidence, *, frozen_at: datetime, seal: str, rcfg: ResearchConfig, computed_by: str) -> Expectation:
    w, t, d = ev.whole, ev.trailing, ev.discovery
    return Expectation(
        frozen_at=frozen_at, seal=date.fromisoformat(seal),
        metric=("mean net P&L per trade, next open to horizon close, net of costs and slippage both ways, "
                "winsorised at one percent either tail"),
        expectancy_net_pct=float(w.expectancy_net), expectancy_2x_slippage_net_pct=float(w.expectancy_2x_net),
        hit_rate_pct=float(w.hit_pct), median_net_pct=float(w.median_net), n=w.n, signal_days=w.signal_days,
        period=DateRange(start=date.fromisoformat(w.window_start), end=date.fromisoformat(w.window_end)),
        trailing_expectancy_net_pct=t.expectancy_net, trailing_n=t.n,
        trailing_period=(DateRange(start=date.fromisoformat(t.window_start), end=date.fromisoformat(t.window_end)) if t.n else None),
        discovery_expectancy_2x_slippage_net_pct=d.expectancy_2x_net, discovery_n=d.n,
        edge_vs_baseline_pct=w.edge_pct, baseline_n=w.baseline_n, placebo_p=w.placebo_p, placebo_draws=w.placebo_draws,
        cluster_t=w.cluster_t, hurdle_pct=rcfg.hurdle_pct, computed_by=computed_by)


def _version_row(store: ExperimentStore, eid: str, version: int, *, edition: str, ev: VariantEvidence, change: str, why: str,
                 level: str, validation: Optional[str], trials: int, computed_at: datetime, fact_cfg: ResearchConfig,
                 rcfg: ResearchConfig, xcfg: ExperimentConfig, source_facts: list, backfilled: bool) -> None:
    fs = FactSet(finding_slug=f"{eid}_v{version}_expectation", cfg=fact_cfg, as_of=edition,
                 period_start=ev.whole.window_start, period_end=edition, component="experiment_research", computed_at=computed_at)
    expectation_facts(fs, ev, xcfg=xcfg, rcfg=rcfg)
    trailing_looks_fact(fs, trials)
    exp = expectation_model(ev, frozen_at=computed_at, seal=edition, rcfg=rcfg, computed_by=fs.component)
    rule = build_experiment_rule(ev.variant, expected_net_pct=exp.expectancy_net_pct, hurdle_pct=rcfg.hurdle_pct,
                                 min_trades=xcfg.min_trades_to_grade, frozen_at=computed_at)
    store.put_version(
        experiment_id=eid, version=version, created_edition=edition, variant_json=_j(ev.variant.as_dict()),
        change=change, why=why, level=level, validation=validation, expectation_json=exp.model_dump_json(),
        expectation_facts_json=_j({"expectation": [f.model_dump(mode="json") for f in fs.facts],
                                   "source": [f.model_dump(mode="json") for f in source_facts]}),
        grading_rule_json=rule.model_dump_json(), trials_for_version=int(trials), backfilled=int(backfilled),
        created_at=computed_at.isoformat())


def _put_trials(store: ExperimentStore, *, owner_kind: str, owner_id: str, edition: str, context: str, trials, computed_at: datetime) -> None:
    for t in trials:
        store.put_trial(owner_kind=owner_kind, owner_id=owner_id, edition_date=edition, trial_no=int(t.trial_no), context=context,
                        signature=t.variant.signature, variant_json=_j(t.variant.as_dict()), stats_json=_j(t.stats),
                        gates_json=_j(t.gates), passed=int(t.passed), adopted=int(t.adopted), reason=t.reason,
                        created_at=computed_at.isoformat())


@dataclass
class _OpenTrial:
    trial_no: int
    variant: Variant
    passed: bool
    reason: str
    stats: dict
    gates: list
    adopted: bool = False
    evidence: Optional[VariantEvidence] = None


# ── 2. OPEN ───────────────────────────────────────────────────────────────────

def _source_facts(f: Finding) -> list:
    keep = tuple(f"_{n}" for n in S1_FACT_NAMES)
    return [x for x in f.facts if x.id.endswith(keep)]


def consider_finding(f: Finding, *, md: MarketData, cx: Conditioning, w: Windows, store: ExperimentStore, rcfg: ResearchConfig,
                     xcfg: ExperimentConfig, c: Constitution, computed_at: datetime, backfilled: bool, rep: StepReport) -> Optional[str]:
    D = md.as_of
    fam = family_for(f)
    row: dict[str, Any] = dict(edition_date=D, finding_id=f.id, template_id=f.template_id, family_id=(fam.id if fam else None),
                               trials_evaluated=0, opened_experiment_id=None, best_signature=None, best_rule_text=None,
                               best_expectancy_net=None, best_failed_gates_json="[]", created_at=computed_at.isoformat())
    if fam is None:
        store.put_candidate(**row, reason=not_derivable_reason(f))
        rep.declined.append(f"{f.id}: not derivable")
        return None
    existing = store.experiment_for_family(fam.id)
    if existing is not None:
        st = store.state_of(existing["experiment_id"]).value
        store.put_candidate(**row, reason=f"family {fam.id} already has experiment {existing['experiment_id']} ({st}); one idea, one record")
        rep.declined.append(f"{f.id}: family has {existing['experiment_id']}")
        return None
    last = store.one("SELECT edition_date FROM pfx_candidates WHERE family_id = ? AND opened_experiment_id IS NULL "
                     "AND trials_evaluated > 0 ORDER BY edition_date DESC LIMIT 1", [fam.id])
    if last is not None:
        s = md.sessions
        if last[0] in s and s.index(D) - s.index(last[0]) < xcfg.retry_after_sessions:
            store.put_candidate(**row, reason=(f"family {fam.id} was researched on {last[0]} and declined; re-testing one hypothesis "
                                               f"daily is p-hacking by repetition — next evaluation after {xcfg.retry_after_sessions} sessions"))
            rep.declined.append(f"{f.id}: declined on {last[0]}, retry later")
            return None
    thr = threshold_of(f, fam)
    variants = variants_for(fam, thr)
    strength = float(f.usefulness.evidence_strength)
    trials: list[_OpenTrial] = []
    for i, v in enumerate(variants, start=1):
        ev = measure(v, md, cx, w, rcfg=rcfg, draws=xcfg.placebo_draws, seed=xcfg.rng_seed + i)
        g = worth_testing_gates(ev, c=c, xcfg=xcfg, n_trials=len(variants), evidence_strength=strength, novel=True,
                                novelty_note=f"no experiment exists for family {fam.id}")
        reason = "clears the gate" if g.passed else "fails: " + "; ".join(x.name for x in g.failures)
        trials.append(_OpenTrial(i, v, g.passed, reason,
                                 {"whole": summarize(ev.whole), "discovery": summarize(ev.discovery), "trailing": summarize(ev.trailing)},
                                 gate_rows(g), evidence=ev))
    passing = [t for t in trials if t.passed]
    row["trials_evaluated"] = len(trials)
    # the variant that came CLOSEST to clearing (fewest failed fatal gates, then expectancy) — the
    # one a founder reading the decline needs to see, not the one with the prettiest expectancy
    def _closeness(t: _OpenTrial) -> tuple:
        failed = sum(1 for g in t.gates if not g["passed"] and g["fatal"])
        etv = t.stats["whole"]["expectancy_net"]
        return (failed, -(etv if etv is not None else -1e9), t.variant.signature)
    best = sorted(trials, key=_closeness)[0]
    row["best_signature"], row["best_rule_text"] = best.variant.signature, best.variant.rule_text()
    row["best_expectancy_net"] = best.stats["whole"]["expectancy_net"]
    if not passing:
        failed = [g for g in best.gates if not g["passed"] and g["fatal"]]
        row["best_failed_gates_json"] = _j([f"{g['name']}={g['value']} vs {g['bar']}" if g["value"] is not None else g["name"]
                                            for g in failed])
        store.put_candidate(**row, reason=f"not worth testing: none of {len(trials)} variants cleared the gate")
        _put_trials(store, owner_kind="finding", owner_id=f.id, edition=D, context="opening", trials=trials, computed_at=computed_at)
        rep.declined.append(f"{f.id}: {len(trials)} trials, none cleared")
        return None
    passing.sort(key=lambda t: (-t.stats["whole"]["expectancy_net"], t.variant.signature))
    chosen = passing[0]
    chosen.adopted = True
    eid = f"exp_{fam.id}_{D.replace('-', '')}"
    assert chosen.evidence is not None
    r = _risk(c)
    store.put_experiment(
        experiment_id=eid, family_id=fam.id, source_finding_id=f.id, opened_edition=D, theme=fam.label, question=fam.question,
        direction=fam.direction, threshold_pct=thr, capital_inr=r["capital"], engine_version=xcfg.engine_version,
        constitution_version=c.version, params_json=_j(xcfg.as_params()), evidence_json=f.provenance.model_dump_json(),
        gates_json=_j(chosen.gates), backfilled=int(backfilled), created_at=computed_at.isoformat())
    _version_row(store, eid, 1, edition=D, ev=chosen.evidence, change="initial version",
                 # digit-free (audit finding 13): the counts are facts on the card and rows on the record
                 why=("the variant with the highest whole-history expectancy among those that cleared the worth-testing gate "
                      "(more than one did; every one evaluated is a counted trial on the record)" if len(passing) > 1 else
                      "the one variant that cleared the worth-testing gate (every one evaluated is a counted trial on the record)"),
                 level="L1", validation=None, trials=len(trials), computed_at=computed_at, fact_cfg=xcfg.fact_cfg(rcfg), rcfg=rcfg,
                 xcfg=xcfg, source_facts=_source_facts(f), backfilled=backfilled)
    store.put_period(experiment_id=eid, version=1, period_no=1, start_after=D, sessions=xcfg.period_sessions, opened_edition=D,
                     backfilled=int(backfilled), created_at=computed_at.isoformat())
    row["opened_experiment_id"] = eid
    store.put_candidate(**row, reason=f"opened {eid}: {len(passing)} of {len(trials)} variants cleared the gate")
    _put_trials(store, owner_kind="finding", owner_id=f.id, edition=D, context="opening", trials=trials, computed_at=computed_at)
    rep.opened.append(f"{eid} <- {f.id} ({chosen.variant.signature}; {len(passing)}/{len(trials)} cleared)")
    return eid


# ── 1. TRACK + GRADE ──────────────────────────────────────────────────────────

def _period_calendar(md: MarketData, start_after: str, sessions: int, h: int) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """(start, end, due) — each None while it lies past the seal."""
    if start_after not in md.sessions:
        return None, None, None
    start = md.session_after(start_after, 1)
    if start is None:
        return None, None, None
    end = md.session_after(start, sessions - 1)
    due = md.session_after(end, h) if end is not None else None
    return start, end, due


def _comparison(grade, expected: float, fs: FactSet, category: ComparisonCategory) -> ExpectedVsActual:
    t = T(fs.facts)
    e, a = fs.id("expected_net"), fs.id("actual_net")
    if category == ComparisonCategory.void:
        stmt = f"Nothing to compare: {t(fs.id('void_reason'))}."
    elif category == ComparisonCategory.failed:
        stmt = (f"Historical expectation {t(e)} per trade; forward virtual {t(a)} — the experiment failed: the period finished "
                f"more than one standard error below zero.")
    elif category == ComparisonCategory.inconclusive:
        stmt = (f"Historical expectation {t(e)} per trade; forward virtual {t(a)} — inconclusive: inside one standard error of "
                f"zero, or too few closed trades to judge.")
    elif category == ComparisonCategory.weaker:
        stmt = f"Historical expectation {t(e)} per trade; forward virtual {t(a)} — the edge is present, but weaker than expected."
    else:
        stmt = f"Historical expectation {t(e)} per trade; forward virtual {t(a)} — the edge is present, at least as strong as expected."
    return ExpectedVsActual(expected_net_pct=expected, actual_net_pct=grade.mean_net,
                            gap_pct=(None if grade.mean_net is None else grade.mean_net - expected), category=category,
                            statement=stmt, fact_refs=sorted({e, a} & fs._ids) + ([fs.id("void_reason")] if category == ComparisonCategory.void else []))


def _learning_statement(lr: Learning, fs: FactSet, version: int, xcfg: ExperimentConfig) -> tuple[str, list[str]]:
    t = T(fs.facts)
    parts: list[str] = []
    if lr.worst_condition is not None and fs.id(f"losers_{lr.worst_condition}") in fs._ids:
        parts.append(f"Most of the period's losing trades came {CONDITION_BY_ID[lr.worst_condition].label} "
                     f"({t(fs.id(f'losers_{lr.worst_condition}'))} of {t(fs.id('losers'))} losers).")
    if lr.next_action == "revise" and lr.adopted is not None:
        parts.append(f"I added the condition {t(fs.id('added_condition'))}: it would have excluded {t(fs.id('losers_excluded'))} of "
                     f"this period's losers and it clears the gate on the sealed history; that is version {t(fs.id('next_version'))}, "
                     f"backtested now and forward-validated in its own periods. {t(fs.id('revision_trials'))} candidate revisions were "
                     f"evaluated and {t(fs.id('revision_passing'))} cleared.")
    elif lr.next_action == "bury":
        if lr.bury_cause == "revisions_exhausted":
            parts.append(f"The idea has used every version the retirement rule allows ({t(fs.id('versions_allowed'))}); it is buried.")
        else:
            parts.append(f"{t(fs.id('revision_trials'))} candidate revisions were evaluated and none cleared the gate on the sealed "
                         f"history; the idea is buried rather than tuned until something fits.")
    else:
        parts.append(f"No change to the rule; the next period continues under version {t(fs.id('this_version'))} to build the forward record.")
    text = " ".join(parts)
    refs = sorted({m for m in [x.id for x in fs.facts] if "{{fact:" + m + "}}" in text})
    return text, refs


def _version_forward(store: ExperimentStore, eid: str, version: int, *, md: MarketData, xcfg: ExperimentConfig,
                     rcfg: ResearchConfig, first_start: str) -> ForwardRecord:
    """The version's forward record across all its periods (RESOLVED trades only), on the frame sealed at D."""
    rows = store.trades(eid, version, resolved_only=True)
    net = [float(r["pnl_pct_net"]) for r in rows]
    days = [str(r["signal_date"]) for r in rows]
    # chained equity across graded periods' marks
    marks = store.q("SELECT period_no, session, equity_inr FROM pfx_marks WHERE experiment_id = ? AND version = ? ORDER BY period_no, session", [eid, version])
    cap = float(store.experiment(eid)["capital_inr"])
    curve: list[float] = []
    scale = 1.0
    last_p = None
    last_eq = cap
    for r in marks:
        if last_p is not None and r["period_no"] != last_p:
            scale *= last_eq / cap
        curve.append(scale * float(r["equity_inr"]) / cap)
        last_eq = float(r["equity_inr"])
        last_p = r["period_no"]
    total = (curve[-1] - 1.0) * 100.0 if curve else None
    mdd = 0.0
    if curve:
        arr = np.array(curve)
        peak = np.maximum.accumulate(np.r_[1.0, arr])[1:]
        mdd = float(((peak - arr) / peak * 100.0).max())
    p: Optional[float] = None
    draws = 0
    ct = None
    if net:
        vrow = store.one("SELECT variant_json FROM pfx_versions WHERE experiment_id = ? AND version = ?", [eid, version])
        variant = Variant.from_dict(json.loads(vrow["variant_json"]))
        df = md.df
        col = f"f{variant.horizon}"
        # The null over the FORWARD window only: every resolved stock-session between the version's
        # first signal session and the seal, day-blocked to the version's own signal-day shape.
        inw = (df["d"] >= first_start) & (df["d"] <= md.as_of) & df[col].notna()
        pool = df.loc[inw, ["d", col]].sort_values("d", kind="mergesort")
        pv = variant.sign * pool[col].to_numpy(dtype=float) * 100.0
        counts = np.array([days.count(d) for d in sorted(set(days))])
        means = block_placebo_means(pool["d"].to_numpy(), pv, counts, draws=xcfg.placebo_draws, seed=xcfg.rng_seed + 7)
        if means.size:
            draws = int(means.size)
            gross_mean = float(np.mean(net)) + rcfg.hurdle_pct      # the pool is gross; the trades are net of the hurdle
            p = max(float((means >= gross_mean).mean()), 1.0 / draws)
        ct = cluster_robust_t(np.array(net), np.array(days))
    return ForwardRecord(tuple(net), tuple(days), total, mdd, p, draws, ct)


def track_period(prow, *, md: MarketData, cx: Conditioning, w: Windows, store: ExperimentStore, rcfg: ResearchConfig,
                 xcfg: ExperimentConfig, c: Constitution, computed_at: datetime, backfilled: bool, rep: StepReport) -> None:
    D = md.as_of
    eid, v, pno = prow["experiment_id"], int(prow["version"]), int(prow["period_no"])
    vrow = store.one("SELECT * FROM pfx_versions WHERE experiment_id = ? AND version = ?", [eid, v])
    variant = Variant.from_dict(json.loads(vrow["variant_json"]))
    start, end, due = _period_calendar(md, prow["start_after"], int(prow["sessions"]), variant.horizon)
    if start is None:
        return
    run = _book(variant, md, cx, start=start, xcfg=xcfg, rcfg=rcfg, c=c)
    have_marks = {r["session"] for r in store.marks(eid, v, pno)}
    closed_cum = 0
    for s, eq in run.equity.items():
        closed_cum = sum(1 for t in run.closed if t.exit_date <= s)
        if s in have_marks:
            continue
        open_n = sum(1 for t in run.closed if t.entry_date <= s < t.exit_date) + sum(1 for t in run.open_at_end if t.entry_date <= s)
        store.put_mark(experiment_id=eid, version=v, period_no=pno, session=s, equity_inr=float(eq), open_positions=int(open_n),
                       closed_cum=int(closed_cum), recorded_edition=D)
    have_trades = {(r["symbol"], r["signal_date"]) for r in store.trades(eid, v, pno)}
    for t in run.closed:
        if (t.symbol, t.signal_date) in have_trades:
            continue
        store.put_trade(experiment_id=eid, version=v, period_no=pno, symbol=t.symbol, signal_date=t.signal_date,
                        entry_date=t.entry_date, entry_price=t.entry_price, exit_date=t.exit_date, exit_price=t.exit_price,
                        holding_sessions=t.holding_sessions, pnl_pct_gross=t.pnl_pct_gross, pnl_pct_net=t.pnl_pct_net,
                        costs_pct=t.costs_pct, notional_inr=t.notional_inr, resolved=int(t.resolved),
                        unresolved_reason=t.unresolved_reason, recorded_edition=D)
    rep.tracked.append(f"{eid} v{v} p{pno}: {len(run.closed)} closed ({len(run.unresolved)} unresolved), "
                       f"{len(run.open_at_end)} open, marked to {run.walked_to}")
    if due is None:
        return
    # ── the period completed on or before D: GRADE under the frozen rule ──
    rule = ExperimentGradingRule.model_validate_json(vrow["grading_rule_json"])
    # Audit finding 6: the evaluator that runs must be the evaluator the rule was frozen under. A
    # changed `grading.py` cannot quietly judge an older version — the registry is archived and rebuilt.
    if rule.rule_version != GRADING_RULES_VERSION:
        raise RuntimeError(f"{eid} v{v}: the frozen rule is {rule.rule_version} but this grader is {GRADING_RULES_VERSION}; "
                           "a grader may not change under an open version — archive the registry and rebuild")
    fact_cfg = xcfg.fact_cfg(rcfg)
    fs = FactSet(finding_slug=f"{eid}_v{v}_p{pno}", cfg=fact_cfg, as_of=D, period_start=start, period_end=due,
                 component="experiment_grader", computed_at=computed_at)
    grade = judge(rule, run, fs=fs)
    expected = float(rule.spec["expected_net_pct"])
    category = compare(grade, expected)
    fs.add("this_version", "the version this period ran under", v, "count")
    erow = store.experiment(eid)
    n_prev = len(store.trials_for_experiment(eid, erow["source_finding_id"]))
    # the S1 card's evidence strength as the opening gate measured it — a revision is judged on
    # the same source evidence, never on a number invented here
    strength = float(next(g["value"] for g in json.loads(erow["gates_json"]) if g["name"] == "source_evidence_strength"))
    lr = learn(verdict=grade.verdict, variant=variant, version=v, run=run, md=md, cx=cx, w=w, rcfg=rcfg, xcfg=xcfg, c=c,
               evidence_strength=strength, fs=fs, trial_no_start=n_prev + 1)
    if lr.next_action == "revise":
        fs.add("next_version", "the version the learning creates", v + 1, "count")
    stmt, refs = _learning_statement(lr, fs, v, xcfg)
    cmp_ = _comparison(grade, expected, fs, category)
    fwd = forward_result(run, D)
    learning_json = {"statement": stmt, "fact_refs": refs, "level": ("L3" if lr.next_action == "revise" else "L1"),
                     "trials_evaluated": len(lr.trials), "trials_passing": sum(1 for t in lr.trials if t.passed),
                     "adopted_signature": (lr.adopted.signature if lr.adopted else None),
                     "adopted_rule": (lr.adopted.rule_text() if lr.adopted else None), "buried": lr.buried,
                     "bury_cause": lr.bury_cause, "next_action": lr.next_action, "worst_condition": lr.worst_condition,
                     "worst_share": lr.worst_share}
    # Audit finding 4: the FROZEN cumulative rule on the version's whole forward record — resolved trades
    # across every period so far, this one included — so a losing version cannot hide behind a wide
    # per-period band, and a rule that stopped firing is not tracked forever.
    prior = store.trades(eid, v, resolved_only=True)
    cum_net = np.array([float(r["pnl_pct_net"]) for r in prior], dtype=float)
    cum_days = np.array([str(r["signal_date"]) for r in prior])
    consecutive_void = (store.consecutive_void(eid, v) + 1) if grade.verdict == Verdict.void else 0
    cum_cause = cumulative_verdict(rule, cum_net, cum_days, consecutive_void)
    if cum_cause is not None and lr.next_action != "bury":
        lr.buried, lr.bury_cause, lr.next_action = True, cum_cause, "bury"
        fs.add("cumulative_trades", "resolved forward trades the cumulative rule was judged on", int(cum_net.size), "count")
        if cum_cause == "rule_stopped_firing":
            fs.add("consecutive_void", "periods in a row that closed no trade", consecutive_void, "count")
        stmt = (stmt + " " + ("The rule has stopped firing: its condition has not occurred for "
                              f"{T(fs.facts)(fs.id('consecutive_void'))} periods in a row; the idea is buried." if cum_cause == "rule_stopped_firing"
                              else f"Across {T(fs.facts)(fs.id('cumulative_trades'))} resolved forward trades the whole record is significantly "
                                   "below zero on independent signal days; the frozen cumulative rule buries the idea.")).strip()
        refs = sorted({x.id for x in fs.facts if "{{fact:" + x.id + "}}" in stmt})
        learning_json.update({"statement": stmt, "fact_refs": refs, "buried": True, "bury_cause": cum_cause, "next_action": "bury"})
    store.put_outcome(experiment_id=eid, version=v, period_no=pno, graded_edition=D, data_as_of=D, period_start=start, period_end=end,
                      due_session=due, verdict=grade.verdict.value, rule_version=rule.rule_version, grader_version=GRADING_RULES_VERSION,
                      forward_json=fwd.model_dump_json(), comparison_json=cmp_.model_dump_json(), realized_facts_json=_facts_json(fs),
                      learning_json=_j(learning_json), next_action=lr.next_action, backfilled=int(backfilled),
                      graded_at=computed_at.isoformat())
    if lr.trials:
        _put_trials(store, owner_kind="experiment", owner_id=eid, edition=D, context=f"after v{v} period {pno}", trials=lr.trials,
                    computed_at=computed_at)
    rep.graded.append((eid, v, pno, grade.verdict.value))
    if lr.next_action == "bury":
        cause = lr.bury_cause or "edge_did_not_persist_oos"
        kept = ("The unconditioned group rule carries no edge and the conditioned one did not survive forward; what survives "
                "is the record: every counted trial and every period's marks and trades, on the registry for anyone to read.")
        t = T(fs.facts)
        if fs.id("actual_net") in fs._ids:
            summary = (f"Version {t(fs.id('this_version'))} failed ({t(fs.id('actual_net'))} per trade against "
                       f"{t(fs.id('expected_net'))} expected); {stmt}")
        else:
            summary = f"Version {t(fs.id('this_version'))} is buried; {stmt}"
        store.put_post_mortem(experiment_id=eid, buried_edition=D, cause=cause, retired_version=v, what_we_kept=kept, summary=summary,
                              fact_refs_json=_j(sorted({x.id for x in fs.facts if "{{fact:" + x.id + "}}" in summary})),
                              backfilled=int(backfilled), created_at=computed_at.isoformat())
        rep.buried.append(f"{eid} v{v} ({cause})")
        return
    if lr.next_action == "revise" and lr.adopted is not None:
        adopted = next(t for t in lr.trials if t.adopted)
        assert adopted.evidence is not None
        src = json.loads(vrow["expectation_facts_json"])["source"]
        _version_row(store, eid, v + 1, edition=D, ev=adopted.evidence,
                     change=f"added the condition: {CONDITION_BY_ID[lr.adopted.conditions[-1]].label}",
                     why=stmt, level="L3",
                     validation=(f"backtested on the history sealed at {D} (whole window, the window ending {xcfg.discovery_end}, "
                                 f"and the trailing window alone) under the same worth-testing gate as v1; forward validation is "
                                 f"the version's own periods — `improved` stays null until they grade"),
                     trials=len(lr.trials), computed_at=computed_at, fact_cfg=fact_cfg, rcfg=rcfg, xcfg=xcfg,
                     source_facts=_load_facts(_j(src)), backfilled=backfilled)
        store.put_period(experiment_id=eid, version=v + 1, period_no=1, start_after=D, sessions=xcfg.period_sessions, opened_edition=D,
                         backfilled=int(backfilled), created_at=computed_at.isoformat())
        rep.revised.append(f"{eid} v{v} -> v{v + 1} ({lr.adopted.signature})")
        return
    # continue: the next period follows the last signal session of this one
    store.put_period(experiment_id=eid, version=v, period_no=pno + 1, start_after=end, sessions=xcfg.period_sessions, opened_edition=D,
                     backfilled=int(backfilled), created_at=computed_at.isoformat())
    # ── graduation gate on the version's whole forward record ──
    first = store.periods(eid, v)[0]
    first_start = md.session_after(first["start_after"], 1) or start
    fr = _version_forward(store, eid, v, md=md, xcfg=xcfg, rcfg=rcfg, first_start=first_start)
    inc = passive_incumbent(md, start=first_start, end=D, rcfg=rcfg, capital_inr=run.capital_inr)
    g = graduation_gates(fr, incumbent=inc, direction=variant.direction, horizon=variant.horizon, c=c, slippage_pct=rcfg.slippage_pct)
    status = proposal_status(g)
    store.put_gate_check(experiment_id=eid, version=v, edition_date=D, gates_json=_j(gate_rows(g)), proposable=int(status is not None))
    if status is not None and store.one("SELECT 1 FROM pfx_proposals WHERE experiment_id = ? AND version = ?", [eid, v]) is None:
        target = "trader" if variant.horizon <= 10 else "investor"
        store.put_proposal(experiment_id=eid, version=v, edition_date=D, target_agent=target, status=status, gates_json=_j(gate_rows(g)),
                           incumbent=(inc.selection_rule if inc else "none"), backfilled=int(backfilled), created_at=computed_at.isoformat())
        rep.proposed.append(f"{eid} v{v} ({status})")


# ── 3. NARRATE ────────────────────────────────────────────────────────────────

def narrate_experiment(eid: str, *, md: MarketData, store: ExperimentStore, rcfg: ResearchConfig, xcfg: ExperimentConfig,
                       c: Constitution, narrator: ExperimentNarrator, computed_at: datetime, rep: StepReport) -> None:
    D = md.as_of
    if store.narrative(eid, D) is not None:
        return
    erow = store.experiment(eid)
    vrow = store.latest_version(eid)
    v = int(vrow["version"])
    xf = json.loads(vrow["expectation_facts_json"])
    exp_facts, src_facts = _load_facts(_j(xf["expectation"])), _load_facts(_j(xf["source"]))
    s1 = {n: next(x.id for x in src_facts if x.id.endswith(f"_{n}")) for n in S1_FACT_NAMES
          if any(x.id.endswith(f"_{n}") for x in src_facts)}
    ex = {n: next(x.id for x in exp_facts if x.id.endswith(f"_{n}")) for n in
          ("condition", "horizon", "expectancy_net", "cases", "signal_days", "hit_rate", "edge", "trailing_expectancy", "trailing_cases",
           "placebo_p", "discovery_end", "trailing_looks")}
    fs = FactSet(finding_slug=f"{eid}_{D.replace('-', '')}_story", cfg=xcfg.fact_cfg(rcfg), as_of=D, period_start=md.first_session,
                 period_end=D, component="experiment_story", computed_at=computed_at)
    opening = store.trials("finding", erow["source_finding_id"])
    fs.add("trials_opening", "variants evaluated to open the experiment (every one counted)", len(opening), "count")
    fs.add("passing_opening", "of those, variants that cleared the worth-testing gate", sum(1 for t in opening if t["passed"]), "count")
    r = _risk(c)
    fs.add("capital", "virtual research capital deployed to the experiment", r["capital"], "inr", sample="parameter")
    fs.add("fraction", "largest share of that capital in any one position", r["fraction"] * 100, "pct", sample="parameter")
    fs.add("period_sessions", "signal sessions per tracking period", xcfg.period_sessions, "sessions", sample="parameter")
    gates = json.loads(erow["gates_json"])
    adv_failed = any((not g["passed"]) and (not g["fatal"]) for g in gates)
    story: dict[str, Any] = {
        "trials_opening": fs.id("trials_opening"), "passing_opening": fs.id("passing_opening"), "capital": fs.id("capital"),
        "fraction": fs.id("fraction"), "period_sessions": fs.id("period_sessions"), "advisories_failed": adv_failed,
        "condition_label": ex["condition"], "horizon": ex["horizon"], "expectancy_net": ex["expectancy_net"], "n": ex["cases"],
        "signal_days": ex["signal_days"], "hit_rate": ex["hit_rate"], "edge": ex["edge"], "trailing_expectancy": ex["trailing_expectancy"],
        "trailing_n": ex["trailing_cases"], "placebo_p": ex["placebo_p"], "discovery_end": ex["discovery_end"],
        "trailing_looks": ex["trailing_looks"],
    }
    facts = list(src_facts) + list(exp_facts)
    outs = store.outcomes(eid)
    latest_out = outs[-1] if outs else None
    periods = store.periods(eid, v)
    open_p = next((p for p in periods if store.outcome(eid, v, int(p["period_no"])) is None), None)
    pm = store.post_mortem(eid)
    prop = store.proposal(eid)
    realized: list = []
    if latest_out is not None:
        realized = _load_facts(latest_out["realized_facts_json"])
        facts += realized
        rid = {n: next((x.id for x in realized if x.id.endswith(f"_{n}")), None) for n in
               ("closed_trades", "signal_days", "actual_net", "expected_net", "book_return", "max_drawdown", "worst_trade", "best_trade", "void_reason")}
        fs.add("period_no", "the period just graded", int(latest_out["period_no"]), "count")
        story["period_no"] = fs.id("period_no")
        lj = json.loads(latest_out["learning_json"])
        if latest_out["verdict"] == "void":
            story["state_of_period"] = "void"
            story["void_reason"] = rid["void_reason"]
            story["happened_headline"] = "The period closed no trade, so nothing was graded"
        else:
            story["state_of_period"] = "graded"
            story.update({"closed_trades": rid["closed_trades"], "period_signal_days": rid["signal_days"], "actual_net": rid["actual_net"],
                          "expected_net": rid["expected_net"], "book_return": rid["book_return"], "max_drawdown": rid["max_drawdown"],
                          "worst_trade": rid["worst_trade"], "best_trade": rid["best_trade"]})
            story["happened_headline"] = {"right": "The forward test agreed with history", "wrong": "The forward test failed",
                                          "inconclusive": "The forward test was inconclusive"}[latest_out["verdict"]]
        cj = json.loads(latest_out["comparison_json"])
        story["learned_body"] = (cj["statement"] + " " + lj["statement"]).strip()
        story["learned_headline"] = {"stronger": "The edge showed up, at least as strong as expected", "weaker": "The edge showed up, weaker than expected",
                                     "failed": "It failed, and here is what the failures had in common", "inconclusive": "Too early to say",
                                     "void": "Nothing happened to learn from"}[cj["category"]]
    elif open_p is not None:
        m = store.marks(eid, v, int(open_p["period_no"]))
        if m:
            cap = float(erow["capital_inr"])
            eq = np.array([float(x["equity_inr"]) for x in m]) / cap
            peak = np.maximum.accumulate(np.r_[1.0, eq])[1:]
            fs.add("open_positions", "positions open at the latest close", int(m[-1]["open_positions"]), "count")
            fs.add("closed_so_far", "trades closed so far this period", int(m[-1]["closed_cum"]), "count")
            fs.add("book_return_now", "virtual book return so far, marked to the latest close", float((eq[-1] - 1) * 100), "pct", sample="observation")
            fs.add("max_dd_now", "worst drawdown so far", float(((peak - eq) / peak * 100).max()), "pct", sample="observation")
            story.update({"state_of_period": "open", "open_positions": fs.id("open_positions"), "closed_so_far": fs.id("closed_so_far"),
                          "book_return_now": fs.id("book_return_now"), "max_dd_now": fs.id("max_dd_now")})
            story["happened_headline"] = "The virtual test is running"
        else:
            story["state_of_period"] = "pending"
            story["happened_headline"] = "Virtual money is committed; the first period opens next session"
        story["learned_headline"] = "Nothing to learn yet"
    else:
        story["state_of_period"] = "pending"
        story["happened_headline"] = "Virtual money is committed"
        story["learned_headline"] = "Nothing to learn yet"
    # 7. next
    t = T(facts + fs.facts)
    all_trades = store.trades(eid, resolved_only=True)
    fs.add("forward_closed", "closed, resolved forward virtual trades on the record, all versions", len(all_trades), "count")
    fs.add("periods_graded", "periods graded so far", len([o for o in outs if o["verdict"] != "void"]), "count")
    fs.add("min_n_graduation", "closed forward trades the graduation gate requires", int(c.document["gauntlet"]["min_n_for_promotion"]),
           "count", sample="parameter")
    fs.add("version_now", "the version in play", v, "count")
    t = T(facts + fs.facts)
    if pm is not None:
        story["next_headline"] = "The idea is buried"
        story["next_body"] = (f"The idea is buried after version {t(fs.id('version_now'))}; its family stays closed and the record "
                              f"stays public: {t(fs.id('forward_closed'))} forward trades across {t(fs.id('periods_graded'))} graded periods.")
    elif prop is not None:
        story["next_headline"] = "A graduation proposal awaits a human"
        story["next_body"] = (f"Version {t(fs.id('version_now'))} cleared every graduation gate the engine can measure on "
                              f"{t(fs.id('forward_closed'))} forward trades; a proposal is on the record for a human to decide. The "
                              f"Constitution is an unsigned draft, so nothing promotes until it is signed. Tracking continues.")
    else:
        story["next_headline"] = "Next: the current version keeps testing, period by period"
        story["next_body"] = (f"Next I keep testing version {t(fs.id('version_now'))} period by period. Graduation needs at least "
                              f"{t(fs.id('min_n_graduation'))} closed forward trades that beat the placebo, the cluster test and the "
                              f"incumbent on net return with no worse drawdown, and a signed Constitution; the record holds "
                              f"{t(fs.id('forward_closed'))} so far across {t(fs.id('periods_graded'))} graded periods.")
    facts_all = facts + fs.facts
    beats = engine_beats(T(facts_all), family_id=erow["family_id"], s1=s1, story=story)
    lines = narrator.lines(beats, facts_all, key=f"{eid}:{D}", at=computed_at,
                           context={"experiment_id": eid, "family": erow["family_id"], "theme": erow["theme"]})
    store.put_narrative(experiment_id=eid, edition_date=D, story_json=_j([ln.model_dump(mode="json") for ln in lines]),
                        facts_json=_j([f.model_dump(mode="json") for f in facts_all]),
                        produced_by=("llm" if any(ln.produced_by.value == "llm" for ln in lines) else "engine"),
                        llm_provider=narrator.provider_name, created_at=computed_at.isoformat())
    rep.narrated.append(eid)


# ── the step ──────────────────────────────────────────────────────────────────

def run_experiment_step(rcfg: ResearchConfig, xcfg: ExperimentConfig, rstore: ResearchStore, xstore: ExperimentStore, *,
                        md: MarketData, constitution: Optional[Constitution] = None,
                        narrator: Optional[ExperimentNarrator] = None, computed_at: Optional[datetime] = None) -> StepReport:
    D = md.as_of
    computed_at = computed_at or now_ist()
    narrator = narrator or ExperimentNarrator(None, provider_name="none")
    c = constitution or constitution_for(xcfg)
    backfilled = is_backfilled(D, computed_at)
    rep = StepReport(D, md.as_of, backfilled, llm_provider=narrator.provider_name)
    if xstore.has_edition(D):
        rep.skipped = True
        return rep
    cx = Conditioning(md)
    w = windows_for(md, xcfg.discovery_end)
    touched: list[str] = []
    for prow in xstore.open_periods():
        before = (len(rep.graded), len(rep.buried), len(rep.revised), len(rep.proposed))
        track_period(prow, md=md, cx=cx, w=w, store=xstore, rcfg=rcfg, xcfg=xcfg, c=c, computed_at=computed_at, backfilled=backfilled, rep=rep)
        if (len(rep.graded), len(rep.buried), len(rep.revised), len(rep.proposed)) != before:
            touched.append(prow["experiment_id"])
    if rstore.has_edition(D):
        for f in rstore.findings_for(D):
            if f.continues is not None:
                continue
            if xstore.candidate_for_finding(f.id) is not None:
                continue
            eid = consider_finding(f, md=md, cx=cx, w=w, store=xstore, rcfg=rcfg, xcfg=xcfg, c=c, computed_at=computed_at,
                                   backfilled=backfilled, rep=rep)
            if eid:
                touched.append(eid)
    for eid in sorted(set(touched)):
        narrate_experiment(eid, md=md, store=xstore, rcfg=rcfg, xcfg=xcfg, c=c, narrator=narrator, computed_at=computed_at, rep=rep)
    xstore.put_edition(edition_date=D, data_as_of=md.as_of, engine_version=xcfg.engine_version, constitution_version=c.version,
                       params_json=_j({**xcfg.as_params(), "hurdle_pct": rcfg.hurdle_pct, "slippage_pct": rcfg.slippage_pct,
                                       "risk": _risk(c), "gauntlet": c.document["gauntlet"]}),
                       backfilled=int(backfilled), generated_at=computed_at.isoformat())
    xstore.commit()
    return rep
