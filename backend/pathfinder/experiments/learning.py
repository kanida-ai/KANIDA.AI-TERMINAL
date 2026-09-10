"""
Learning — what a completed period changes, computed, with every trial counted.

A learning is not a sentence; it is (a) a FAILURE-CONDITIONING analysis of the period's closed
trades over the closed condition set ("most failures came on sessions when …"), and (b) the
same gate the version had to clear, re-run for every candidate revision — the current rule
with one more condition from the closed set — on the sealed history at the grading seal.
Every candidate is a trial on the record whether or not it is adopted; the count is published
next to the result (addendum 3: no silent p-hacking).

What a period's grade does to the experiment:

    Right / Inconclusive / void   no change; the version continues into its next period —
                                  more forward evidence is what an unproven edge needs
    Wrong                         the failure analysis runs; the candidate that would have
                                  excluded the largest share of the period's losers AND clears
                                  the gate becomes the next version (an L3 strategy change:
                                  backtested on the seal, forward-validated by its own periods);
                                  if none clears, the idea is BURIED with a post-mortem. A
                                  condition the rule already implies, or one exclusive with it,
                                  is never a candidate (`hypotheses.candidate_conditions`): a
                                  revision that changes nothing would be a free trial
    any                           a version beyond `max_versions` cannot be created: BURIED

A horizon change would be an L2 parameter move inside the Constitution's `_exits` range; this
build revises only by adding a condition, so every revision is L3.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from ..engine.governance import Constitution
from ..research.config import ResearchConfig
from ..research.data import MarketData
from ..research.facts import FactSet
from ..schemas import Verdict
from .book import BookRun
from .config import ExperimentConfig
from .gate import gate_rows, summarize, worth_testing_gates
from .hypotheses import CONDITION_BY_ID, Conditioning, Variant, VariantEvidence, Windows, candidate_conditions, measure


@dataclass
class Trial:
    trial_no: int
    variant: Variant
    passed: bool
    reason: str
    stats: dict
    gates: list[dict]
    adopted: bool = False
    loser_share_excluded: Optional[float] = None
    evidence: Optional[VariantEvidence] = field(default=None, repr=False)


@dataclass
class Learning:
    verdict: Verdict
    trials: list[Trial] = field(default_factory=list)
    adopted: Optional[Variant] = None
    buried: bool = False
    bury_cause: Optional[str] = None
    next_action: str = "continue"          # continue | revise | bury
    facts: Optional[FactSet] = None
    worst_condition: Optional[str] = None  # condition id under which the largest share of losers fell
    worst_share: Optional[float] = None


def failure_conditioning(run: BookRun, cx: Conditioning, variant: Variant, fs: FactSet) -> tuple[Optional[str], Optional[float]]:
    """Share of the period's LOSING trades that fell under each condition (on the signal day). Facts, not a verdict."""
    losers = [t for t in run.graded if t.pnl_pct_net is not None and t.pnl_pct_net <= 0]
    fs.add("losers", "closed trades in the period that lost after costs", len(losers), "count")
    if not losers:
        return None, None
    best: tuple[Optional[str], float] = (None, -1.0)
    for cid in candidate_conditions(variant.conditions):     # never a condition the rule already implies
        c = CONDITION_BY_ID[cid]
        by = cx.by_date(c.id)
        share = float(np.mean([bool(by.get(t.signal_date, False)) for t in losers]) * 100.0)
        fs.add(f"losers_{c.id}", f"share of the period's losing trades that fired {c.label}", share, "pct", n=len(losers))
        if share > best[1] + 1e-9:
            best = (c.id, share)
    return best[0], (best[1] if best[0] else None)


def learn(*, verdict: Verdict, variant: Variant, version: int, run: BookRun, md: MarketData, cx: Conditioning,
          w: Windows, rcfg: ResearchConfig, xcfg: ExperimentConfig, c: Constitution, evidence_strength: float,
          fs: FactSet, trial_no_start: int) -> Learning:
    out = Learning(verdict=verdict, facts=fs)
    worst, share = failure_conditioning(run, cx, variant, fs)
    out.worst_condition, out.worst_share = worst, share
    if verdict != Verdict.wrong:
        out.next_action = "continue"
        return out
    if version + 1 > xcfg.max_versions:
        out.buried, out.bury_cause, out.next_action = True, "revisions_exhausted", "bury"
        fs.add("versions_allowed", "the most versions an idea may go through before it is buried", xcfg.max_versions, "count",
               sample="parameter")
        return out
    losers = [t for t in run.graded if t.pnl_pct_net is not None and t.pnl_pct_net <= 0]
    trial_no = trial_no_start
    cands = candidate_conditions(variant.conditions)
    # the family-wise bar counts EVERY trial the idea will have had after this round, whatever the
    # candidate's position in the loop (audit finding 12)
    n_trials = trial_no_start - 1 + len(cands)
    for cid in cands:
        cond = CONDITION_BY_ID[cid]
        cand = variant.with_condition(cond.id)
        ev = measure(cand, md, cx, w, rcfg=rcfg, draws=xcfg.placebo_draws, seed=xcfg.rng_seed + trial_no)
        g = worth_testing_gates(ev, c=c, xcfg=xcfg, n_trials=n_trials, evidence_strength=evidence_strength,
                                novel=True, novelty_note="a revision of an open experiment, not a new claim")
        by = cx.by_date(cond.id)
        excl = (float(np.mean([not bool(by.get(t.signal_date, False)) for t in losers]) * 100.0) if losers else None)
        reason = "clears the gate" if g.passed else "fails: " + "; ".join(x.name for x in g.failures)
        out.trials.append(Trial(trial_no, cand, g.passed, reason,
                                {"whole": summarize(ev.whole), "discovery": summarize(ev.discovery), "trailing": summarize(ev.trailing)},
                                gate_rows(g), loser_share_excluded=excl, evidence=ev))
        trial_no += 1
    passing = [t for t in out.trials if t.passed]
    fs.add("revision_trials", "candidate revisions evaluated after this period (every one counted)", len(out.trials), "count")
    fs.add("revision_passing", "of those, revisions that cleared the gate on the sealed history", len(passing), "count")
    if not passing:
        out.buried, out.bury_cause, out.next_action = True, "edge_did_not_persist_oos", "bury"
        return out
    passing.sort(key=lambda t: (-(t.loser_share_excluded or 0.0), -(t.stats["whole"]["expectancy_net"] or 0.0), t.variant.signature))
    chosen = passing[0]
    chosen.adopted = True
    out.adopted, out.next_action = chosen.variant, "revise"
    added = chosen.variant.conditions[-1]
    fs.add("added_condition", "the condition the next version adds", CONDITION_BY_ID[added].label, "text")
    if chosen.loser_share_excluded is not None:
        fs.add("losers_excluded", "share of this period's losing trades the added condition would have excluded",
               chosen.loser_share_excluded, "pct", n=len(losers))
    return out
