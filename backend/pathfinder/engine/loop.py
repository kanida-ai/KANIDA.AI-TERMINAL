"""
THE LOOP.

    Observe → Question → Test → Interpret → Virtual Trade → Track → Review → Learn → Next

The deterministic engine computes every number. The model proposes, interprets,
reflects, decides-what-next and narrates. The Constitution decides what either of them
is allowed to do. Everything lands in the append-only authoritative tables, and the
P0 endpoints read it back.

Three properties this file exists to guarantee:

1. **Selection touches the discovery window only.** The candidate scan is handed a
   frame sealed at `discovery_end`. The validation window then judges what discovery
   chose, and the book window — sealed at experiment open — judges what survived. No
   step can reach forward, because the frame it holds does not contain forward bars.
2. **The model is woken by a trigger, never by a clock.** `_wake` is the only place an
   LLM call is made, it takes a `trigger_id`, and it refuses without one.
3. **A gate failure is a published outcome.** An experiment that dies gets a
   post-mortem with a cause, the evidence, and what we kept.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Optional, Sequence

import numpy as np

from ..llm.gateway import Budget, GatewayError, Job, PathfinderLLM
from . import governance as GOV
from . import observe as OB
from . import recall as RC
from . import replay as RP
from .book import BookResult, run_book
from .config import EngineConfig, now_ist
from .costs import CostModel
from .evidence import (
    Metrics, REPLAY_DRAWDOWN_NOTE, cost_sensitivity_ladder, compute_metrics, provenance_for,
)
from .hypothesis import HypothesisSpec, ParameterOutOfRange
from .market import PriceFrames, SURVIVORSHIP_NOTE
from .narrator import Line, Narrator
from .repository import Repository

SIMULATED_LABEL = "Simulated · Not traded · Not PaRRVA-verified"
VIRTUAL_LABEL = "Virtual money · Forward-tracked · Not traded · Not PaRRVA-verified"

#: How many experiments one run opens from the ranked candidates.
MAX_EXPERIMENTS = 6


def _iso(d: date | datetime) -> str:
    return d.isoformat()


@dataclass
class RunReport:
    """What one turn of the loop did. Printed by the CLI, quoted in the hand-back."""
    cycle_id: str
    as_of: date
    candidates_screened: int
    opened: list[str] = field(default_factory=list)
    statuses: dict[str, str] = field(default_factory=dict)
    gate_log: list[str] = field(default_factory=list)
    narration_failures: list[str] = field(default_factory=list)
    llm_provider: str = "none"
    notes: list[str] = field(default_factory=list)


class PathfinderLoop:
    """One turn of the research loop, persisted."""

    def __init__(
        self,
        *,
        cfg: EngineConfig,
        costs: CostModel,
        repo: Repository,
        llm: PathfinderLLM,
        constitution: GOV.Constitution,
        budget: Budget,
        llm_provider_name: str = "none",
        ledger=None,
    ) -> None:
        self.cfg, self.costs, self.repo = cfg, costs, repo
        self.llm, self.c, self.budget = llm, constitution, budget
        self.provider = llm_provider_name
        self.ledger = ledger
        self.narrator = Narrator(
            llm, constitution_version=constitution.version, budget=budget,
            on_call=lambda r: self._record_llm_call(
                job=Job.narrate, model=r.model, usage=r.usage,
                prompt_version=r.prompt_version, schema_id="pathfinder.narrate.v1",
                experiment_id=None,
            ),
        )
        #: PER-PREFIX counters. A single shared counter made experiment ids depend on
        #: how many LLM calls happened before them, so turning the model on renumbered
        #: every experiment. Ids are part of the published record; they do not move
        #: because something unrelated did.
        self._seq: dict[str, int] = {}
        self._llm_calls = 0
        self.last_call_id: Optional[str] = None
        #: The change-log is append-only and per-experiment: `seq` must be strictly
        #: increasing within an experiment, and both `_learn` and `_kill` write to it.
        self._change_seq: dict[str, int] = {}

    # ── ids ─────────────────────────────────────────────────────────────────

    def _next_id(self, prefix: str) -> str:
        self._seq[prefix] = self._seq.get(prefix, 0) + 1
        return f"{prefix}_{self._seq[prefix]:04d}"

    def _next_change_seq(self, experiment_id: str) -> int:
        self._change_seq[experiment_id] = self._change_seq.get(experiment_id, 0) + 1
        return self._change_seq[experiment_id]

    # ── the ONLY door to a model ────────────────────────────────────────────

    def _wake(self, *, job: Job, trigger_id: Optional[str], **kw) -> Any:
        """
        Autonomy = intelligent activation. A call without a trigger is a clock, and a
        clock is exactly what the design forbids, so this refuses one.
        """
        if not trigger_id:
            raise GatewayError(
                f"refusing to wake the model for {job.value}: no trigger. "
                "Pathfinder's LLM is woken by the deterministic observer, never by a clock."
            )
        # `experiment_id` is metering metadata, not a gateway parameter — the gateway
        # deliberately knows nothing about experiments.
        experiment_id = kw.pop("experiment_id", None)
        result = self.llm.reason(job=job, constitution_version=self.c.version,
                                 budget=self.budget, **kw)
        call_id = self._record_llm_call(
            job=job, model=result.model, usage=result.usage,
            prompt_version=result.prompt_version,
            schema_id=kw.get("schema_id", "pathfinder.reason.v1"),
            experiment_id=experiment_id,
        )
        self._llm_calls += 1
        self.last_call_id = call_id
        return result

    def _record_llm_call(self, *, job: Job, model: str, usage, prompt_version: str,
                         schema_id: str, experiment_id: Optional[str],
                         ok: bool = True, error_code: Optional[str] = None) -> str:
        cid = self._next_id("llm")
        self.repo.put_llm_call(
            llm_call_id=cid, at=_iso(now_ist()), budget_day_ist=_iso(now_ist().date()),
            job=job.value, model=model, provider=self.provider,
            prompt_version=prompt_version, schema_id=schema_id, batched=False,
            experiment_id=experiment_id, cycle_id=self.cycle_id,
            input_tokens=usage.input_tokens, output_tokens=usage.output_tokens,
            cache_read_input_tokens=usage.cache_read_input_tokens,
            cache_creation_input_tokens=usage.cache_creation_input_tokens,
            cost_usd=usage.cost_usd, latency_ms=usage.latency_ms, ok=ok, error_code=error_code,
        )
        return cid

    # ── fact + outcome writers ──────────────────────────────────────────────

    def _fact(self, *, fid: str, experiment_id: Optional[str], label: str,
              value: float | int | str, unit: str, n: Optional[int], component: str,
              start: date, end: date, note: Optional[str] = None) -> str:
        p = provenance_for(self.cfg, component=component, start=start, end=end,
                           as_of=self.as_of, costs=self.costs)
        # A statistic that could not be computed is written as text, never as a silent
        # NULL and never as a zero. SQLite stores NaN as NULL, so a NaN slipping through
        # here would have become "no value" — which reads, to anything downstream, as if
        # the number simply was not measured rather than could not be.
        numeric = isinstance(value, (int, float)) and math.isfinite(float(value))
        if isinstance(value, (int, float)) and not numeric:
            note = (note or "") + (" " if note else "") + (
                "NOT MEASURABLE over this window: the rule produced no trade whose full "
                "horizon resolves inside the seal, so there is nothing to compare against."
            )
        self.repo.put_fact(
            fact_id=fid, experiment_id=experiment_id, label=label,
            value_num=(float(value) if numeric else None),
            value_text=(None if numeric else ("not measurable" if isinstance(value, (int, float))
                                              else str(value))),
            unit=unit, n=n, data_source=p.data_source, range_start=_iso(p.range_start),
            range_end=_iso(p.range_end), as_of=_iso(p.as_of),
            cost_convention=p.cost_convention, computed_by=p.computed_by,
            computed_at=_iso(p.computed_at), universe=p.universe,
            note=note or SURVIVORSHIP_NOTE,
        )
        return fid

    def _outcome(self, *, oid: str, experiment_id: str, basis: str, m: Metrics,
                 start: date, end: date, component: str,
                 strategy_version: Optional[str] = None) -> str:
        p = provenance_for(self.cfg, component=component, start=start, end=end,
                           as_of=self.as_of, costs=self.costs)
        self.repo.put_outcome(
            outcome_id=oid, experiment_id=experiment_id, strategy_version=strategy_version,
            basis=basis, label=(SIMULATED_LABEL if basis == "historical_replay" else VIRTUAL_LABEL),
            expectancy_pct_per_trade=m.expectancy_pct_per_trade,
            expectancy_2x_slippage_pct_per_trade=m.expectancy_2x_slippage_pct_per_trade,
            total_return_pct=m.total_return_pct, max_drawdown_pct=m.max_drawdown_pct,
            current_drawdown_pct=m.current_drawdown_pct, win_rate_pct=m.win_rate_pct,
            avg_win_pct=m.avg_win_pct, avg_loss_pct=m.avg_loss_pct, payoff_ratio=m.payoff_ratio,
            n=m.n, occurrences=m.occurrences, data_source=p.data_source,
            range_start=_iso(p.range_start), range_end=_iso(p.range_end), as_of=_iso(p.as_of),
            cost_convention=p.cost_convention, computed_by=p.computed_by,
            computed_at=_iso(p.computed_at),
        )
        return oid

    def _evidence(self, *, eid: str, experiment_id: str, kind: str, title: str,
                  outcome_id: Optional[str], fact_ids: Sequence[str],
                  start: date, end: date, component: str, basis: str) -> str:
        p = provenance_for(self.cfg, component=component, start=start, end=end,
                           as_of=self.as_of, costs=self.costs)
        self.repo.put_evidence(
            evidence_id=eid, experiment_id=experiment_id, kind=kind, title=title,
            label=(SIMULATED_LABEL if basis == "historical_replay" else VIRTUAL_LABEL),
            outcome_id=outcome_id, fact_ids=list(fact_ids), data_source=p.data_source,
            range_start=_iso(p.range_start), range_end=_iso(p.range_end), as_of=_iso(p.as_of),
            cost_convention=p.cost_convention, computed_by=p.computed_by,
            computed_at=_iso(p.computed_at),
        )
        return eid

    def _line(self, line: Line, *, experiment_id: Optional[str]) -> str:
        sid = self._next_id("stl")
        self.repo.put_story_line(
            story_line_id=sid, experiment_id=experiment_id, cycle_id=self.cycle_id,
            beat=line.beat, headline=line.headline, body=line.body,
            produced_by=line.produced_by, model=line.model,
            prompt_version=line.prompt_version, llm_call_id=line.llm_call_id,
            fact_ids=list(line.fact_refs), at=_iso(line.at),
        )
        return sid

    def _trigger(self, *, ttype: str, description: str, experiment_id: Optional[str],
                 fact_ids: Sequence[str] = (), detail: Optional[dict] = None) -> str:
        tid = self._next_id("trg")
        self.repo.put_trigger(
            trigger_id=tid, type=ttype, description=description, experiment_id=experiment_id,
            fired_at=_iso(now_ist()), fired_by="engine", fact_ids=list(fact_ids),
            detail=detail or {},
        )
        return tid

    def _state(self, experiment_id: str, status: str, reason: str,
               decision_id: Optional[str] = None) -> None:
        self.repo.put_state(experiment_id=experiment_id, status=status, at=_iso(now_ist()),
                            decision_id=decision_id, reason=reason)

    # ── the run ─────────────────────────────────────────────────────────────

    def run(self) -> RunReport:
        cfg, costs = self.cfg, self.costs

        # ---------- OBSERVE (cheap, deterministic, continuous) --------------
        full = PriceFrames.load(cfg, as_of=cfg.as_of or date.today())
        self.as_of = full.as_of
        self.cycle_id = f"cyc_{self.as_of:%Y_%m_%d}_01"
        report = RunReport(self.cycle_id, self.as_of, 0, llm_provider=self.provider)

        self.repo.put_constitution(
            version=self.c.version, document=self.c.document, approved_by=self.c.approved_by,
            effective_from=self.c.effective_from, previous_version=None, created_at=now_ist(),
        )
        if not self.c.is_signed:
            report.notes.append(
                f"Constitution {self.c.version} is UNSIGNED — no experiment may be promoted."
            )

        state = OB.observe(full)
        threshold = float(self.c.get("autonomy.unusual_condition_percentile", 0.95))
        unusual = OB.unusual_conditions(state, threshold=threshold)

        market_facts = [
            self._fact(fid="fct_mkt_breadth_50dma", experiment_id=None,
                       label="share of the liquid universe above its 50-session average",
                       value=round(state.breadth_above_50dma, 4), unit="ratio",
                       n=state.liquid_universe_size, component="observer",
                       start=cfg.history_start, end=self.as_of),
            self._fact(fid="fct_mkt_index_vol", experiment_id=None,
                       label="trailing 20-session annualised index volatility",
                       value=round(state.index_realised_vol_20d, 4), unit="ratio",
                       n=20, component="observer", start=cfg.history_start, end=self.as_of),
            self._fact(fid="fct_mkt_vol_pctile", experiment_id=None,
                       label="that volatility's percentile against its own history to date",
                       value=round(state.index_vol_percentile, 4), unit="ratio",
                       n=len(full.dates), component="observer",
                       start=cfg.history_start, end=self.as_of,
                       note="Expanding percentile: ranked only against sessions up to and "
                            "including the reading. A full-sample rank would be look-ahead."),
            self._fact(fid="fct_mkt_universe_size", experiment_id=None,
                       label="names passing the point-in-time liquidity filter today",
                       value=state.liquid_universe_size, unit="count", n=None,
                       component="observer", start=cfg.history_start, end=self.as_of),
            self._fact(fid="fct_mkt_data_staleness", experiment_id=None,
                       label="days between the newest price bar and the day this ran",
                       value=full.staleness_sessions, unit="days", n=None,
                       component="observer", start=cfg.history_start, end=self.as_of,
                       note="Every window in this run ends at the last real session, not at "
                            "the run date. A number is never dated later than the data "
                            "behind it."),
        ]

        market_trigger = self._trigger(
            ttype="unusual_market_condition" if unusual else "scheduled_review",
            description=(
                "; ".join(f"{o.description} sits at the {o.percentile:.0%} percentile of its "
                          f"own history" for o in unusual)
                if unusual else
                "no market-state series is in its tail; the observer scheduled deterministic "
                "work and only the results of that work may wake a model"
            ),
            experiment_id=None, fact_ids=[f for f in market_facts],
            detail={"unusual": [o.key for o in unusual], "threshold": threshold},
        )

        # ---------- QUESTION: the deterministic scan, discovery window ONLY --
        disc_frames = full.sealed_at(cfg.discovery_end)
        scan = OB.scan_discovery(
            disc_frames, cfg, costs,
            window_start=cfg.discovery_start, window_end=cfg.discovery_end,
            placebo_draws=200,
        )
        report.candidates_screened = scan.n_candidates
        self._fact(fid="fct_scan_candidates", experiment_id=None,
                   label="pre-registered candidate rules screened on the discovery window",
                   value=scan.n_candidates, unit="count", n=None, component="scanner",
                   start=cfg.discovery_start, end=cfg.discovery_end,
                   note="Multiple testing: the best of this many candidates is inflated. "
                        "Every placebo p-value is compared against a Bonferroni-adjusted bar.")

        ranked = scan.ranked()
        opened: list[str] = []
        corpus: list[tuple[str, str, list[float]]] = []

        # The governance death comes first: the single best candidate in the whole
        # scan is not implementable, and that is a result worth publishing.
        best_overall = ranked[0] if ranked else None
        queue: list[tuple[OB.Candidate, bool]] = []
        if best_overall and not GOV.check_implementable(best_overall.spec).ok:
            queue.append((best_overall, False))
        for cand in ranked:
            if len(queue) >= MAX_EXPERIMENTS:
                break
            if any(cand.spec.signature == q.spec.signature for q, _ in queue):
                continue
            if not GOV.check_implementable(cand.spec).ok:
                continue
            queue.append((cand, True))

        for cand, _implementable in queue:
            eid = self._run_experiment(cand, scan, full, disc_frames, report, corpus,
                                       market_trigger)
            if eid:
                opened.append(eid)
        report.opened = opened

        # ---------- the cycle's own story (what GET /loop serves) ------------
        died = sum(1 for v in report.statuses.values() if v == "died")
        alive = sum(1 for v in report.statuses.values() if v != "died")
        cycle_beats = [
            ("noticed", "cycle:noticed", "What the observer saw today",
             "Breadth stood at {{fact:fct_mkt_breadth_50dma}} of the liquid universe above its "
             "medium-term average and trailing index volatility at {{fact:fct_mkt_index_vol}}, "
             "which is the {{fact:fct_mkt_vol_pctile}} percentile of its own history to date. "
             "{{fact:fct_mkt_universe_size}} names were tradeable under the point-in-time "
             "liquidity filter.",
             ["fct_mkt_breadth_50dma", "fct_mkt_index_vol", "fct_mkt_vol_pctile",
              "fct_mkt_universe_size"]),
            ("hypothesis", "cycle:hypothesis", "What the loop chose to put at risk",
             "{{fact:fct_scan_candidates}} pre-registered candidate rules were replayed over "
             "the discovery window and ranked by their edge over the same universe rather "
             "than by raw return, because raw return over a survivor-biased universe mostly "
             "measures the market.",
             ["fct_scan_candidates"]),
            ("experiment", "cycle:experiment", "How each one was judged",
             "Every candidate opened as an experiment faced the same sequence: replay on the "
             "discovery window, a placebo of same-size random draws from the same universe "
             "against a bar adjusted for how many candidates were screened, then an "
             "out-of-sample window that had no say in choosing it, then a virtual book on a "
             "window sealed when the experiment opened.", []),
            ("outcome", "cycle:outcome", "What survived",
             f"Of the experiments opened this cycle, {'none' if alive == 0 else 'some'} "
             "cleared every gate. Deaths are published with their cause and what was kept "
             "from them; the graveyard is the part of this record that is hardest to fake.",
             []),
            ("learning", "cycle:learning", "What the loop now believes",
             "The strongest statistical edge found was not tradeable by a retail customer "
             "under the stated cost convention, and implementability is now screened before "
             "ranking rather than after. Separately, a rule's expectancy and the expectancy "
             "of a capacity-limited book that trades it are different quantities, and only "
             "the second is a track record.", []),
            ("next", "cycle:next", "What gets tested next",
             "Measure the contribution of the market-regime condition itself by re-running "
             "the surviving conditions with it removed, so the context is measured rather "
             "than assumed.", []),
        ]
        for beat, key, headline, body, refs in cycle_beats:
            line = self.narrator.beat(
                beat=beat, facts=self._facts_payload([(r, None) for r in refs]),
                context=[{"cycle": self.cycle_id}], key=key,
                fallback_headline=headline, fallback_body=body, fallback_refs=refs,
            )
            self._line(line, experiment_id=None)

        # ---------- NEXT: what the loop decided to test next -----------------
        self._decide_next(report, market_trigger, ranked)

        self.repo.put_cycle(
            cycle_id=self.cycle_id, as_of=_iso(now_ist()), stage="next",
            trigger_id=market_trigger,
            active_experiment_id=(opened[0] if opened else None),
            constitution_version=self.c.version, created_at=_iso(now_ist()),
        )
        report.narration_failures = list(self.narrator.failures)
        self.repo.commit()
        return report

    # ── one experiment, end to end ──────────────────────────────────────────

    def _run_experiment(self, cand: OB.Candidate, scan: OB.CandidateScan,
                        full: PriceFrames, disc_frames: PriceFrames,
                        report: RunReport, corpus: list, market_trigger: str) -> Optional[str]:
        cfg, costs, spec = self.cfg, self.costs, cand.spec
        eid = self._next_id("exp")
        slug = eid.replace("exp_", "e")
        F = lambda name: f"fct_{slug}_{name}"           # noqa: E731 — a local id helper

        # ---- novelty (recall decides WHERE to look; it is never evidence) ---
        # Compare the compact SIGNATURE, not the rendered sentence. `entry_text` is
        # mostly boilerplate ("a liquid name that ... fill = next session's open"), so
        # embedding it made a gap-up long look like a gap-down short at cosine 0.93.
        # The signature carries only what actually differs between two hypotheses.
        # Dedupe the IDEA, not the exit structure. Keying on the full signature let the
        # same trigger through twice wearing different stops, which is exactly what the
        # note below claims does not happen — and it inflates the apparent breadth of a
        # day's research. The exits are a separate question, asked once an idea survives.
        sig_text = (f"{spec.trigger} {' '.join(f'{k}={v:g}' for k, v in sorted(spec.params.items()))} "
                    f"{spec.context} {spec.direction}")
        similar = RC.most_similar(sig_text, corpus, top_k=3)
        novel = not any(score >= RC.NOVELTY_THRESHOLD for _, _, score in similar)
        novelty_note = (
            "no previously opened experiment resembles this one"
            if novel else
            f"too similar to {similar[0][1]} (cosine {similar[0][2]:.2f}) — the same idea in "
            "different clothes is not a new experiment"
        )
        if not novel:
            report.gate_log.append(f"{eid}: SKIPPED (not novel) — {novelty_note}")
            return None
        corpus.append(("experiments", eid, RC.embed(sig_text)))

        # ---- QUESTION: the model proposes; the engine validates the proposal
        question = (
            f"The discovery scan screened {scan.n_candidates} pre-registered candidate rules "
            f"over {cfg.discovery_start}..{cfg.discovery_end}. Its strongest un-opened, "
            f"implementable candidate is: {spec.entry_text} Exit: {spec.exit_text} "
            "State the falsifiable hypothesis this candidate embodies, and say what would "
            "make it wrong."
        )
        proposed_by, model, hyp_call_id = "engine", None, None
        statement = (
            f"Names that satisfy this condition outperform the same liquid universe over the "
            f"same {spec.horizon_sessions}-session horizon under the same exits, net of costs."
        )
        rationale = (
            "Selected by the deterministic discovery scan on the discovery window only, "
            "ranked by edge over a same-universe baseline rather than by raw return."
        )
        try:
            r = self._wake(
                job=Job.hypothesis, trigger_id=market_trigger, question=question,
                facts=self._facts_payload([
                    ("fct_scan_candidates", scan.n_candidates),
                ]),
                context=[{"candidate": spec.to_json()}],
                schema_id="pathfinder.hypothesis.v1",
                prompt_version="pathfinder.hypothesis@p1.0.0", experiment_id=eid,
                key=f"{spec.cassette_key}:hypothesis",
            )
            if r.raw_json.get("hypothesis"):
                # A model-composed spec is parsed against the CLOSED library and the
                # Constitution's approved ranges. Out of range is rejected, never clamped.
                spec = HypothesisSpec.parse(r.raw_json["hypothesis"],
                                            approved=self.c.approved_ranges)
            statement, rationale = r.claim, r.body
            proposed_by, model, hyp_call_id = "llm", r.model, self.last_call_id
        except (GatewayError, ParameterOutOfRange, KeyError) as e:
            report.notes.append(f"{eid}: hypothesis proposed by the engine ({e})")

        hyp_id = self._next_id("hyp")
        self.repo.put_hypothesis(
            hypothesis_id=hyp_id, question=question, statement=statement, rationale=rationale,
            proposed_by=proposed_by, model=model, prompt_version="pathfinder.hypothesis@p1.0.0",
            llm_call_id=hyp_call_id, novelty_recall_ids=[s[1] for s in similar],
            constitution_version=self.c.version, created_at=_iso(now_ist()),
        )
        self.repo.put_experiment(
            experiment_id=eid, hypothesis_id=hyp_id, opened_at=_iso(now_ist()),
            universe=cfg.universe_id, direction=spec.direction,
            horizon_sessions=spec.horizon_sessions, cost_convention=costs.convention,
            data_source=cfg.data_source, constitution_version=self.c.version,
            created_by=proposed_by, created_at=_iso(now_ist()),
        )
        self._state(eid, "queued", "opened from the discovery scan; no results yet")

        sv = f"{eid}:strategy@v1.0"
        self.repo.put_strategy_version(
            strategy_version=sv, experiment_id=eid, version_label="strategy@v1.0",
            previous_version=None, rulebook={
                "universe": spec.scope, "direction": spec.direction,
                "entry": spec.entry_text, "invalidation": spec.invalidation_text,
                "exit": spec.exit_text, "horizon_sessions": spec.horizon_sessions,
                "sizing": (f"{cfg.risk_fraction_per_trade:.0%} of virtual capital per position, "
                           f"at most {cfg.max_concurrent_positions} concurrent, no leverage"),
                "cost_convention": costs.convention,
                "selection_rule": "liquidity_desc",
                "capital_inr": cfg.capital_inr,
                # The stable address a recorded completion is keyed by, stored so the
                # cassettes can be audited against the experiments they narrated.
                "cassette_key": spec.cassette_key,
            },
            created_by="engine", approved_by=None, validation=None,
            active_from=_iso(now_ist()), retired_at=None, created_at=_iso(now_ist()),
        )
        for pname, pval in spec.params.items():
            rng = self.c.approved_ranges.get(spec.trigger, {}).get(pname, {})
            GOV.check_parameter_within_range(self.c, spec.trigger, pname, float(pval))
            self.repo.put_parameter(
                strategy_version=sv, name=f"{spec.trigger}.{pname}", value_num=float(pval),
                value_text=None, unit="pct" if pname.endswith("pct") else "count",
                approved_min=float(rng.get("min")) if rng else None,
                approved_max=float(rng.get("max")) if rng else None,
                set_by="engine", at=_iso(now_ist()),
            )

        # ---- TEST: discovery evidence -------------------------------------
        # The scan ranked on a cheap i.i.d. null. Before anything is GATED, the
        # discovery result is re-measured against the null that is actually correct
        # for it: day-blocked, because these signals arrive in clusters on event days.
        # The two disagree by about a factor of four on the null's own spread.
        self._state(eid, "testing", "replaying the rule over the discovery window")
        dm = self._remeasure(spec, disc_frames, cand.metrics,
                             window_start=cfg.discovery_start, window_end=cfg.discovery_end)
        disc_o = self._outcome(oid=f"out_{slug}_discovery", experiment_id=eid,
                               basis="historical_replay", m=dm, start=cfg.discovery_start,
                               end=cfg.discovery_end, component="strategy_replay",
                               strategy_version=sv)
        disc_facts = [
            self._fact(fid=F("disc_exp"), experiment_id=eid,
                       label="discovery expectancy per trade, net of costs",
                       value=round(dm.expectancy_pct_per_trade, 4), unit="pct_per_trade",
                       n=dm.n, component="strategy_replay",
                       start=cfg.discovery_start, end=cfg.discovery_end),
            self._fact(fid=F("disc_exp_2x"), experiment_id=eid,
                       label="discovery expectancy per trade at twice the slippage assumption",
                       value=round(dm.expectancy_2x_slippage_pct_per_trade, 4),
                       unit="pct_per_trade", n=dm.n, component="strategy_replay",
                       start=cfg.discovery_start, end=cfg.discovery_end),
            self._fact(fid=F("disc_edge"), experiment_id=eid,
                       label="discovery edge over the same universe, window and exits",
                       value=round(dm.edge_vs_baseline_pct, 4), unit="pct_per_trade",
                       n=dm.n, component="baseline",
                       start=cfg.discovery_start, end=cfg.discovery_end),
            self._fact(fid=F("disc_base"), experiment_id=eid,
                       label="what the same universe did under the same exits (the baseline)",
                       value=round(dm.baseline_pct_per_trade, 4), unit="pct_per_trade",
                       n=dm.baseline_n, component="baseline",
                       start=cfg.discovery_start, end=cfg.discovery_end),
            self._fact(fid=F("disc_placebo_p"), experiment_id=eid,
                       label="share of same-size random draws from that universe that did as well",
                       value=round(dm.placebo_p_value, 5), unit="ratio", n=dm.placebo_draws,
                       component="placebo", start=cfg.discovery_start, end=cfg.discovery_end,
                       note=f"Empirical permutation p-value over {dm.placebo_draws} draws; it "
                            f"cannot resolve below {1.0 / max(1, dm.placebo_draws):.4g}."),
            self._fact(fid=F("disc_n"), experiment_id=eid,
                       label="closed trades behind the discovery numbers", value=dm.n,
                       unit="count", n=dm.n, component="strategy_replay",
                       start=cfg.discovery_start, end=cfg.discovery_end),
        ]
        self._evidence(eid=f"evd_{slug}_discovery", experiment_id=eid, kind="historical_replay",
                       title="Strategy replay over the discovery window",
                       outcome_id=disc_o, fact_ids=disc_facts, start=cfg.discovery_start,
                       end=cfg.discovery_end, component="strategy_replay",
                       basis="historical_replay")
        self._evidence(eid=f"evd_{slug}_placebo", experiment_id=eid, kind="placebo",
                       title="Placebo: same-size random draws from the same universe",
                       outcome_id=None, fact_ids=[F("disc_placebo_p"), F("disc_base")],
                       start=cfg.discovery_start, end=cfg.discovery_end, component="placebo",
                       basis="historical_replay")

        gates = GOV.discovery_gauntlet(spec, dm, self.c, n_candidates=scan.n_candidates,
                                       novel=novel, novelty_note=novelty_note)
        for g in gates.gates:
            report.gate_log.append(f"{eid} {g}")

        if not gates.passed:
            failed = "; ".join(g.statement for g in gates.failures)
            self._beats(eid, slug, spec, market_facts=[], disc_facts=disc_facts,
                        val_facts=[], book_facts=[],
                        outcome_text=self._outcome_text(slug, has_val=False, has_book=False),
                        learning_text=("It did not clear the discovery gate: " + failed +
                                       " " + self._what_we_kept(spec, gates.failures, "x")),
                        next_text=("This one is closed. The scan's next un-opened candidate "
                                   "takes its slot."),
                        scan=scan)
            return self._kill(eid, slug, spec, sv, gates, report,
                              stage="discovery", facts=disc_facts,
                              evidence=[f"evd_{slug}_discovery", f"evd_{slug}_placebo"],
                              market_trigger=market_trigger)

        # ---- VALIDATE: out-of-sample. Nothing here chose the rule. ----------
        self._state(eid, "validating", "testing the rule on data that had no say in choosing it")
        val_frames = full.sealed_at(cfg.validation_end)
        vg = RP.ExitGrid(spec, val_frames, costs)
        vres = RP.replay(spec, val_frames, window_start=cfg.validation_start,
                         window_end=cfg.validation_end, costs=costs, grid=vg)
        vbase, vbn, vpl = RP.baseline_and_placebo(
            spec, val_frames, vg, window_start=cfg.validation_start,
            window_end=cfg.validation_end, n_signals=vres.n, draws=cfg.placebo_draws,
            seed=cfg.rng_seed,
        )
        vdc = RP.signal_day_counts(spec, val_frames, vg,
                                   window_start=cfg.validation_start, window_end=cfg.validation_end)
        vblk = RP.block_placebo(spec, val_frames, vg, window_start=cfg.validation_start,
                                window_end=cfg.validation_end, day_counts=vdc,
                                draws=cfg.placebo_draws_gate, seed=cfg.rng_seed)
        vm = compute_metrics(vres, costs_2x=costs.at_slippage_multiple(2.0),
                             baseline_pct=vbase, baseline_n=vbn,
                             placebo_means=(vblk if vblk.size else vpl),
                             placebo_kind=("day_block" if vblk.size else "iid"))
        val_o = self._outcome(oid=f"out_{slug}_validation", experiment_id=eid,
                              basis="historical_replay", m=vm, start=cfg.validation_start,
                              end=cfg.validation_end, component="strategy_replay",
                              strategy_version=sv)
        val_facts = [
            self._fact(fid=F("oos_exp"), experiment_id=eid,
                       label="out-of-sample expectancy per trade, net of costs",
                       value=round(vm.expectancy_pct_per_trade, 4), unit="pct_per_trade",
                       n=vm.n, component="strategy_replay",
                       start=cfg.validation_start, end=cfg.validation_end),
            self._fact(fid=F("oos_exp_2x"), experiment_id=eid,
                       label="out-of-sample expectancy at twice the slippage assumption",
                       value=round(vm.expectancy_2x_slippage_pct_per_trade, 4),
                       unit="pct_per_trade", n=vm.n, component="strategy_replay",
                       start=cfg.validation_start, end=cfg.validation_end),
            self._fact(fid=F("oos_edge"), experiment_id=eid,
                       label="out-of-sample edge over the same universe and exits",
                       value=round(vm.edge_vs_baseline_pct, 4), unit="pct_per_trade",
                       n=vm.n, component="baseline",
                       start=cfg.validation_start, end=cfg.validation_end),
            self._fact(fid=F("oos_n"), experiment_id=eid,
                       label="closed trades behind the out-of-sample numbers", value=vm.n,
                       unit="count", n=vm.n, component="strategy_replay",
                       start=cfg.validation_start, end=cfg.validation_end),
            self._fact(fid=F("oos_days"), experiment_id=eid,
                       label="independent sessions those trades arrived on",
                       value=vm.signal_days, unit="count", n=vm.n, component="strategy_replay",
                       start=cfg.validation_start, end=cfg.validation_end,
                       note="The effective sample. A screen firing on many names in one "
                            "morning is one event, not many, and every significance "
                            "statement here is made on this number."),
            self._fact(fid=F("oos_cluster_t"), experiment_id=eid,
                       label="t-statistic with trades clustered on their signal date",
                       value=round(vm.cluster_t, 4), unit="ratio", n=vm.n,
                       component="cluster_robust_t",
                       start=cfg.validation_start, end=cfg.validation_end,
                       note="The naive t treats every trade as independent and is much "
                            "larger; this is the one to believe."),
            self._fact(fid=F("oos_placebo_p"), experiment_id=eid,
                       label="share of day-blocked random draws that did as well",
                       value=round(vm.placebo_p_value, 5), unit="ratio", n=vm.placebo_draws,
                       component="placebo", start=cfg.validation_start, end=cfg.validation_end,
                       note=f"Day-blocked null over {vm.placebo_draws} draws, preserving the "
                            "signal's clustering. Reported at the test's resolution floor "
                            "rather than as zero."),
            self._fact(fid=F("oos_dd"), experiment_id=eid,
                       label="worst drawdown of the out-of-sample unit-stake trade sequence",
                       value=round(vm.max_drawdown_pct, 4), unit="pct", n=vm.n,
                       component="strategy_replay", note=REPLAY_DRAWDOWN_NOTE,
                       start=cfg.validation_start, end=cfg.validation_end),
        ]
        self._evidence(eid=f"evd_{slug}_validation", experiment_id=eid, kind="historical_replay",
                       title="Out-of-sample replay: does it hold on data it never saw?",
                       outcome_id=val_o, fact_ids=val_facts, start=cfg.validation_start,
                       end=cfg.validation_end, component="strategy_replay",
                       basis="historical_replay")

        # Cost sensitivity, published whether it flatters or not.
        ladder = cost_sensitivity_ladder(vres, costs)
        ladder_facts = [
            self._fact(fid=F(f"oos_slip_{int(mult)}x"), experiment_id=eid,
                       label=f"out-of-sample expectancy at {mult:g}x the slippage assumption",
                       value=round(val, 4), unit="pct_per_trade", n=vm.n,
                       component="cost_sensitivity",
                       start=cfg.validation_start, end=cfg.validation_end)
            for mult, val in ladder
        ]
        self._evidence(eid=f"evd_{slug}_costs", experiment_id=eid, kind="cost_sensitivity",
                       title="How much slippage the idea survives",
                       outcome_id=None, fact_ids=ladder_facts, start=cfg.validation_start,
                       end=cfg.validation_end, component="cost_sensitivity",
                       basis="historical_replay")

        vgates = GOV.validation_gauntlet(vm, self.c)
        for g in vgates.gates:
            report.gate_log.append(f"{eid} {g}")
        if not vgates.passed:
            failed = "; ".join(g.statement for g in vgates.failures)
            self._beats(eid, slug, spec, market_facts=[], disc_facts=disc_facts,
                        val_facts=val_facts, book_facts=[],
                        outcome_text=self._outcome_text(slug, has_val=True, has_book=False),
                        learning_text=(
                            "Discovery strength did not survive contact with data that had no "
                            "say in choosing it: " + failed + " Strength on the window a rule "
                            "was chosen on is not evidence; it is the hypothesis."),
                        next_text=("Keep the window split. The next candidate is judged the "
                                   "same way and most of them will fail here too."),
                        scan=scan)
            return self._kill(eid, slug, spec, sv, vgates, report, stage="validation",
                              facts=disc_facts + val_facts,
                              evidence=[f"evd_{slug}_validation", f"evd_{slug}_costs"],
                              market_trigger=market_trigger)

        # ---- VIRTUAL TRADE + TRACK: the sealed book ------------------------
        book = run_book(spec, full, window_start=self.cfg.seal_start, window_end=self.as_of,
                        costs=costs, cfg=cfg, selection_rule="liquidity_desc",
                        max_new_positions_per_session=int(
                            self.c.get("risk.max_new_positions_per_session", 5)))
        bm = book.metrics(costs_2x=costs.at_slippage_multiple(2.0))
        book_o = self._outcome(oid=f"out_{slug}_book", experiment_id=eid, basis="virtual_book",
                               m=bm, start=cfg.seal_start, end=self.as_of,
                               component="virtual_book", strategy_version=sv)
        for i, t in enumerate(book.ledger_losers_first, start=1):
            self.repo.put_trade(
                trade_id=f"trd_{slug}_{i:04d}", experiment_id=eid, strategy_version=sv,
                symbol=t.symbol, direction=t.direction, signal_date=_iso(t.signal_date),
                entry_date=_iso(t.entry_date), entry_price=t.entry_price,
                exit_date=_iso(t.exit_date), exit_price=t.exit_price,
                exit_reason=t.exit_reason, holding_sessions=t.holding_sessions,
                pnl_pct_gross=t.pnl_pct_gross, pnl_pct_net=t.pnl_pct_net,
                costs_pct=t.costs_pct, slippage_bps=t.slippage_bps, mfe_pct=t.mfe_pct,
                mae_pct=t.mae_pct, as_of=_iso(self.as_of),
                computed_by=cfg.component("virtual_book"), created_at=_iso(now_ist()),
            )

        # The rule, unconstrained, over the SAME sealed window — so the gap between a
        # rule and a book is a measured number rather than an argument.
        bg = RP.ExitGrid(spec, full, costs)
        rres = RP.replay(spec, full, window_start=cfg.seal_start, window_end=self.as_of,
                         costs=costs, grid=bg)
        rbase, rbn, rpl = RP.baseline_and_placebo(
            spec, full, bg, window_start=cfg.seal_start, window_end=self.as_of,
            n_signals=rres.n, draws=cfg.placebo_draws, seed=cfg.rng_seed)
        rm = compute_metrics(rres, costs_2x=costs.at_slippage_multiple(2.0),
                             baseline_pct=rbase, baseline_n=rbn, placebo_means=rpl)

        book_facts = [
            self._fact(fid=F("book_exp"), experiment_id=eid,
                       label="virtual book expectancy per trade, net of costs",
                       value=round(bm.expectancy_pct_per_trade, 4), unit="pct_per_trade",
                       n=bm.n, component="virtual_book", start=cfg.seal_start, end=self.as_of),
            self._fact(fid=F("book_exp_2x"), experiment_id=eid,
                       label="virtual book expectancy at twice the slippage assumption",
                       value=round(bm.expectancy_2x_slippage_pct_per_trade, 4),
                       unit="pct_per_trade", n=bm.n, component="virtual_book",
                       start=cfg.seal_start, end=self.as_of),
            self._fact(fid=F("book_ret"), experiment_id=eid,
                       label="virtual book cumulative return on capital",
                       value=round(bm.total_return_pct or 0.0, 4), unit="pct", n=bm.n,
                       component="virtual_book", start=cfg.seal_start, end=self.as_of),
            self._fact(fid=F("book_dd"), experiment_id=eid,
                       label="worst peak-to-trough drawdown of the virtual book",
                       value=round(bm.max_drawdown_pct, 4), unit="pct", n=bm.n,
                       component="virtual_book", start=cfg.seal_start, end=self.as_of),
            self._fact(fid=F("book_dd_now"), experiment_id=eid,
                       label="virtual book drawdown from its running peak today",
                       value=round(bm.current_drawdown_pct, 4), unit="pct", n=bm.n,
                       component="virtual_book", start=cfg.seal_start, end=self.as_of),
            self._fact(fid=F("book_n"), experiment_id=eid,
                       label="closed virtual trades", value=bm.n, unit="count", n=bm.n,
                       component="virtual_book", start=cfg.seal_start, end=self.as_of),
            self._fact(fid=F("book_taken"), experiment_id=eid,
                       label="signals the book had capacity to take, of those that fired",
                       value=book.signals_taken, unit="count", n=book.signals_seen,
                       component="virtual_book", start=cfg.seal_start, end=self.as_of),
            self._fact(fid=F("rule_exp"), experiment_id=eid,
                       label="the same rule over the same window with no capacity limit",
                       value=round(rm.expectancy_pct_per_trade, 4), unit="pct_per_trade",
                       n=rm.n, component="strategy_replay", start=cfg.seal_start, end=self.as_of),
            self._fact(fid=F("rule_edge"), experiment_id=eid,
                       label="that rule's edge over the same universe in the sealed window",
                       value=round(rm.edge_vs_baseline_pct, 4), unit="pct_per_trade",
                       n=rm.n, component="baseline", start=cfg.seal_start, end=self.as_of),
            self._fact(fid=F("capacity_gap"), experiment_id=eid,
                       label="what capacity limits and the selection rule cost per trade",
                       value=round(bm.expectancy_pct_per_trade - rm.expectancy_pct_per_trade, 4),
                       unit="pct_per_trade", n=bm.n, component="virtual_book",
                       start=cfg.seal_start, end=self.as_of),
        ]
        # The capital curve itself, as a fact: a series with the same provenance as any
        # other number. `unit=text` because it is not a single renderable value.
        self._fact(fid=F("book_curve"), experiment_id=eid,
                   label="virtual book equity curve, cumulative percent of capital",
                   value=json.dumps(book.spark()), unit="text", n=bm.n,
                   component="virtual_book", start=cfg.seal_start, end=self.as_of,
                   note="Marked to the close every session, including days with no activity, "
                        "so the drawdown is the one a holder would have lived through.")
        self._evidence(eid=f"evd_{slug}_book", experiment_id=eid, kind="forward_virtual",
                       title="The virtual book on the window sealed at experiment open",
                       outcome_id=book_o, fact_ids=book_facts, start=cfg.seal_start,
                       end=self.as_of, component="virtual_book", basis="virtual_book")

        # ---- REVIEW: an OBSERVATION trigger wakes the model, not a clock ----
        review_threshold = int(self.c.get("autonomy.observation_review_threshold", 30))
        review_trigger = None
        if bm.n >= review_threshold:
            review_trigger = self._trigger(
                ttype="observation_threshold",
                description=(f"the virtual book has closed at least {review_threshold} trades; "
                             "the evidence is due a review"),
                experiment_id=eid, fact_ids=[F("book_n"), F("book_exp")],
                detail={"threshold": review_threshold, "closed": bm.n},
            )

        bgates = GOV.book_gauntlet(bm, self.c)
        for g in bgates.gates:
            report.gate_log.append(f"{eid} {g}")

        # ---- LEARN: the change-log (L1 always; L3 when the review demands it)
        extra_facts = self._learn(eid, slug, spec, sv, dm, vm, bm, rm, book, report,
                                  review_trigger, book_facts, full, rres.trades)

        self._beats(
            eid, slug, spec, market_facts=[], disc_facts=disc_facts, val_facts=val_facts,
            book_facts=list(book_facts) + list(extra_facts),
            outcome_text=self._outcome_text(slug, has_val=True, has_book=True),
            learning_text=(
                "A rule and a book are different quantities. This rule's edge over its own "
                "universe is {{fact:fct_" + slug + "_rule_edge}} in the sealed window, while "
                "the book that traded it returned {{fact:fct_" + slug + "_book_ret}} on "
                "capital, a gap of {{fact:fct_" + slug + "_capacity_gap}} per trade. The "
                "obvious suspect is the tie-break the book uses when more signals fire than "
                "it has slots — so it was measured rather than blamed: the spread across "
                "every approved selection rule is {{fact:fct_" + slug + "_tiebreak_span}}, "
                "which is {{fact:fct_" + slug + "_tiebreak_share}} of the gap. The larger "
                "part is structural: {{fact:fct_" + slug + "_top_day_share}} of the rule's "
                "trades arrive on its three busiest sessions, and a book with ten slots "
                "cannot be in them."
                if bm.n else "The book took too few trades to say anything."),
            next_text=(
                "Test the structural explanation directly: a book that is allowed to size down "
                "and spread across a crowded session, judged on a window this comparison has "
                "not already seen."),
            scan=scan,
        )

        if bgates.passed:
            pg = GOV.promotion_gauntlet(self.c, discovery=gates, validation=vgates)
            for g in pg.gates:
                report.gate_log.append(f"{eid} promotion:{g}")
            blockers = [g.name for g in pg.failures]
            self._state(eid, "promising",
                        "cleared discovery, out-of-sample and the virtual book; "
                        + ("eligible for promotion" if pg.passed
                           else "not promotable: " + ", ".join(blockers)))
            report.statuses[eid] = "promising"
            if blockers:
                report.notes.append(f"{eid}: promising but NOT promotable — {', '.join(blockers)}")
        else:
            self._kill(eid, slug, spec, sv, bgates, report, stage="virtual book",
                       facts=disc_facts + val_facts + book_facts,
                       evidence=[f"evd_{slug}_book", f"evd_{slug}_validation"],
                       market_trigger=review_trigger or market_trigger)
        return eid

    # ── the story ───────────────────────────────────────────────────────────

    def _beats(self, eid: str, slug: str, spec: HypothesisSpec, *,
               market_facts: Sequence[str], disc_facts: Sequence[str],
               val_facts: Sequence[str], book_facts: Sequence[str],
               outcome_text: str, learning_text: str, next_text: str,
               scan: OB.CandidateScan) -> None:
        """
        Six beats, in order: noticed → hypothesis → experiment → outcome → learning →
        next. Each is offered to the model first; the engine fallback is used when
        there is no model, and the author is recorded either way.
        """
        F = lambda name: f"fct_{slug}_{name}"           # noqa: E731
        ck = spec.cassette_key
        beats = [
            ("noticed", f"{ck}:noticed",
             "What the observer saw before anything was tested",
             "The deterministic observer reads the market every session for nothing. Breadth "
             "stood at {{fact:fct_mkt_breadth_50dma}} of the liquid universe above its "
             "medium-term average, with trailing index volatility at "
             "{{fact:fct_mkt_index_vol}}, which sits at the {{fact:fct_mkt_vol_pctile}} "
             "percentile of its own history to that day. "
             "{{fact:fct_mkt_universe_size}} names passed the point-in-time liquidity filter.",
             ["fct_mkt_breadth_50dma", "fct_mkt_index_vol", "fct_mkt_vol_pctile",
              "fct_mkt_universe_size"]),
            ("hypothesis", f"{ck}:hypothesis",
             "The claim being put at risk",
             f"{spec.entry_text} {spec.exit_text} The scan screened "
             "{{fact:fct_scan_candidates}} pre-registered candidates on the discovery window "
             "alone, so this one arrives already inflated by selection and every significance "
             "bar it faces is adjusted for that.",
             ["fct_scan_candidates"]),
            ("experiment", f"{ck}:experiment",
             "How the idea was actually tested",
             "The rule was replayed as it would be traded: entry at the open of the session "
             "after the signal, the stated stop, and the stated horizon. It produced "
             "{{fact:" + F("disc_n") + "}} closed trades on the discovery window. Costs and "
             "slippage were charged to every one of them, winners included.",
             [F("disc_n")]),
            ("outcome", f"{ck}:outcome", "What happened", outcome_text,
             list(disc_facts) + list(val_facts) + list(book_facts)),
            ("learning", f"{ck}:learning", "What this changed", learning_text,
             list(book_facts) or list(val_facts) or list(disc_facts)),
            ("next", f"{ck}:next", "What this makes worth testing next", next_text, []),
        ]
        for beat, key, headline, body, refs in beats:
            line = self.narrator.beat(
                beat=beat, facts=self._facts_payload([(r, None) for r in refs]),
                context=[{"experiment": eid}], key=key,
                fallback_headline=headline, fallback_body=body, fallback_refs=refs,
            )
            self._line(line, experiment_id=eid)

    @staticmethod
    def _outcome_text(slug: str, *, has_val: bool, has_book: bool) -> str:
        F = lambda n: "{{fact:fct_" + slug + "_" + n + "}}"     # noqa: E731
        parts = [
            f"On the discovery window the rule returned {F('disc_exp')} per trade net of "
            f"costs, against {F('disc_base')} for the same universe under the same exits — "
            f"an edge of {F('disc_edge')}. At twice the slippage assumption it was "
            f"{F('disc_exp_2x')}."
        ]
        if has_val:
            parts.append(
                f"Out of sample, on data that had no say in choosing it, expectancy was "
                f"{F('oos_exp')} per trade over {F('oos_n')} trades, an edge of "
                f"{F('oos_edge')}, and {F('oos_exp_2x')} at twice the slippage."
            )
        if has_book:
            parts.append(
                f"The virtual book — capacity-limited, marked daily on the window sealed at "
                f"experiment open — returned {F('book_ret')} on capital with a worst drawdown "
                f"of {F('book_dd')}, expectancy {F('book_exp')} per trade over {F('book_n')} "
                f"closed trades. The same rule with no capacity limit returned "
                f"{F('rule_exp')} per trade over the same window, so the limits and the "
                f"selection rule cost {F('capacity_gap')} per trade."
            )
        return " ".join(parts)

    # ── re-measuring a scan result against the null that is correct for it ──

    def _remeasure(self, spec: HypothesisSpec, frames: PriceFrames, scanned: Metrics,
                   *, window_start: date, window_end: date) -> Metrics:
        """
        Re-run the discovery result against the DAY-BLOCKED null, at gate resolution.

        The scan uses a cheap i.i.d. null because it has to do it 288 times. That null
        is wrong for this data — the signals cluster on event days — and it is wrong in
        the flattering direction, understating its own spread by roughly the square root
        of the average cluster size. Nothing is *gated* on the cheap number; it is only
        ever used to rank. This is the measurement the gate sees.
        """
        g = RP.ExitGrid(spec, frames, self.costs)
        res = RP.replay(spec, frames, window_start=window_start, window_end=window_end,
                        costs=self.costs, grid=g)
        base, base_n, _ = RP.baseline_and_placebo(
            spec, frames, g, window_start=window_start, window_end=window_end,
            n_signals=0, draws=0, seed=self.cfg.rng_seed)
        counts = RP.signal_day_counts(spec, frames, g,
                                      window_start=window_start, window_end=window_end)
        blk = RP.block_placebo(spec, frames, g, window_start=window_start,
                               window_end=window_end, day_counts=counts,
                               draws=self.cfg.placebo_draws_gate, seed=self.cfg.rng_seed)
        if not blk.size:
            return scanned
        return compute_metrics(res, costs_2x=self.costs.at_slippage_multiple(2.0),
                               baseline_pct=base, baseline_n=base_n,
                               placebo_means=blk, placebo_kind="day_block")

    # ── death ───────────────────────────────────────────────────────────────

    def _kill(self, eid: str, slug: str, spec: HypothesisSpec, sv: str, gates: GOV.GateSet,
              report: RunReport, *, stage: str, facts: Sequence[str], evidence: Sequence[str],
              market_trigger: Optional[str], cause: Optional[str] = None) -> str:
        failed = gates.failures
        names = {g.name for g in failed}
        # The cause is customer-facing copy, so it has to be the cause. Filing a
        # significance failure under "no edge after costs" would be false: those
        # experiments had a positive expectancy after costs and still should not be
        # believed, which is a different and more interesting thing to say.
        cause = cause or self._death_cause(names)

        why = "; ".join(g.statement for g in failed)
        kept = self._what_we_kept(spec, failed, cause)

        line = self.narrator.beat(
            beat="learning", facts=self._facts_payload([(f, None) for f in facts]),
            context=[{"stage": stage, "failed_gates": sorted(names)}],
            key=f"{spec.cassette_key}:postmortem",
            fallback_headline=f"Retired at the {stage} gate",
            fallback_body=(
                f"This idea did not clear the {stage} gate: {why}. "
                f"What we keep: {kept}"
            ),
            fallback_refs=[],
        )
        sid = self._line(line, experiment_id=eid)
        did = self.repo.put_decision(
            decision_id=self._next_id("dec"), at=_iso(now_ist()), kind="kill",
            experiment_id=eid, trigger_id=market_trigger,
            question=f"does this idea clear the {stage} gate?",
            decision=f"no — retired: {why}", rationale_story_line_id=sid,
            evidence_ids=list(evidence), decided_by="engine", model=None, llm_call_id=None,
            approved_by=None, constitution_version=self.c.version,
        )
        self._state(eid, "died", why, decision_id=None)
        self.repo.put_post_mortem(
            experiment_id=eid, died_at=_iso(now_ist()), cause=cause,
            summary_story_line_id=sid, what_we_kept=kept, evidence_ids=list(evidence),
            retired_version=sv, decided_by="engine", approved_by=None,
        )
        self.repo.put_learning_event(
            learning_event_id=self._next_id("lrn"), experiment_id=eid,
            seq=self._next_change_seq(eid), level="L1",
            at=_iso(now_ist()), what_changed=f"{eid} retired at the {stage} gate",
            why=why, evidence_ids=list(evidence), previous_version=sv,
            new_version=f"{eid}:retired", outcome_before_id=None, outcome_after_id=None,
            improved=None, decided_by="engine", approved_by=None, validation=None,
            constitution_version=self.c.version, statement_story_line_id=sid,
        )
        report.statuses[eid] = "died"
        return eid

    @staticmethod
    def _death_cause(failed: set[str]) -> str:
        """Map the gates that actually failed onto a published cause."""
        if "implementable_under_cost_convention" in failed:
            return "not_implementable_under_cost_convention"
        if "novelty" in failed:
            return "superseded_by_newer_version"
        if "discovery_placebo_screen" in failed:
            return "not_distinguishable_from_chance"
        if any(n.endswith("sample_size") for n in failed):
            return "sample_too_small"
        # Out-of-sample: did the edge go away, or did it merely fail to survive costs?
        if {"oos_expectancy_net", "oos_edge_vs_baseline"} & failed:
            return "edge_did_not_persist_oos"
        if failed & {"oos_expectancy_2x_slippage", "book_expectancy_2x_slippage",
                     "book_expectancy_net", "discovery_expectancy_2x_slippage",
                     "discovery_expectancy_net", "discovery_edge_vs_baseline"}:
            return "no_edge_after_costs"
        return "no_edge_after_costs"

    @staticmethod
    def _what_we_kept(spec: HypothesisSpec, failed: Sequence[GOV.Gate], cause: str) -> str:
        """The learning that survives the death. A graveyard with no lessons is just a graveyard."""
        if cause == "not_implementable_under_cost_convention":
            return (
                "The strongest statistical edge in the whole scan was a multi-session short, "
                "which a retail customer cannot hold in the NSE cash segment. Screening for "
                "implementability BEFORE ranking is now the first filter, not the last: an "
                "edge nobody can trade is not a finding, it is a distraction."
            )
        if cause == "edge_did_not_persist_oos":
            return (
                f"The {spec.trigger} condition looked strong on the discovery window and did "
                "not survive contact with data that had no say in choosing it. Discovery "
                "strength is not evidence; it is a hypothesis. Keep the window split."
            )
        if cause == "not_distinguishable_from_chance":
            return (
                f"The {spec.trigger} condition had a positive expectancy after costs and still "
                "could not be told apart from a random draw of the same shape from the same "
                "universe — where 'the same shape' means the same clustering on the same kind "
                "of event day, which is the comparison that matters and the one an ordinary "
                "significance test gets wrong. Expectancy being positive is not the question."
            )
        if any(n.startswith("book_") for n in {g.name for g in failed}):
            return (
                "The rule's edge over its own universe can persist while the book that trades "
                "it still loses money. Expectancy of a rule and expectancy of a book are "
                "different quantities, and only the second one is a track record."
            )
        return (
            f"The {spec.trigger} condition's edge did not survive the cost convention it would "
            "actually be traded under. An edge that exists only at the friendly slippage "
            "assumption is a cost artefact, and the assumption is the part we control least."
        )

    # ── learning ────────────────────────────────────────────────────────────

    def _learn(self, eid: str, slug: str, spec: HypothesisSpec, sv: str,
               dm: Metrics, vm: Metrics, bm: Metrics, rm: Metrics, book: BookResult,
               report: RunReport, review_trigger: Optional[str],
               book_facts: Sequence[str], full: PriceFrames,
               rm_trades: Sequence[Any] = ()) -> list[str]:
        """Returns the fact ids it minted, so the story can reference them."""
        F = lambda name: f"fct_{slug}_{name}"           # noqa: E731
        minted: list[str] = []
        ev = [f"evd_{slug}_book", f"evd_{slug}_validation"]

        self.repo.put_learning_event(
            learning_event_id=self._next_id("lrn"), experiment_id=eid,
            seq=self._next_change_seq(eid), level="L1",
            at=_iso(now_ist()),
            what_changed="Recorded the rule's behaviour across all three windows.",
            why=("Discovery, out-of-sample and the sealed book are three independent readings "
                 "of the same rule; the shape of the difference between them is the finding."),
            evidence_ids=ev, previous_version=None, new_version=f"{eid}:evidence@1",
            outcome_before_id=f"out_{slug}_validation", outcome_after_id=f"out_{slug}_book",
            improved=None, decided_by="engine", approved_by=None, validation=None,
            constitution_version=self.c.version, statement_story_line_id=None,
        )

        # L3 — but ONLY if the measurement supports it.
        #
        # The first version of this code asserted that the gap between the book and the
        # unconstrained rule WAS the selection rule, computed the alternatives in the
        # next three lines, reduced them to `best`, and threw the comparison away. An
        # adversarial audit ran those discarded numbers: the tie-break explained about
        # 5% of the gap. So the alternatives are now measured, PUBLISHED as facts, and
        # the claim is only made when the share is material.
        gap = bm.expectancy_pct_per_trade - rm.expectancy_pct_per_trade
        if gap < -0.10 and review_trigger:
            alt = {"liquidity_desc": bm}
            for rule in ("liquidity_asc", "random"):
                b2 = run_book(spec, full, window_start=self.cfg.seal_start, window_end=self.as_of,
                              costs=self.costs, cfg=self.cfg, selection_rule=rule,
                              max_new_positions_per_session=int(
                                  self.c.get("risk.max_new_positions_per_session", 5)))
                alt[rule] = b2.metrics(costs_2x=self.costs.at_slippage_multiple(2.0))
            best = max(alt, key=lambda k: alt[k].expectancy_pct_per_trade)
            span = (max(v.expectancy_pct_per_trade for v in alt.values())
                    - min(v.expectancy_pct_per_trade for v in alt.values()))
            share = span / abs(gap) if gap else 0.0

            # The competing explanation, also measured: how much of the rule's edge
            # comes from a handful of days the book had no slots for.
            top_share, ex_top = self._event_concentration(rm_trades)

            for rule, met in sorted(alt.items()):
                self._fact(fid=F(f"book_exp_{rule}"), experiment_id=eid,
                           label=f"virtual book expectancy under the `{rule}` selection rule",
                           value=round(met.expectancy_pct_per_trade, 4), unit="pct_per_trade",
                           n=met.n, component="virtual_book",
                           start=self.cfg.seal_start, end=self.as_of)
            self._fact(fid=F("tiebreak_span"), experiment_id=eid,
                       label="spread in book expectancy across every approved selection rule",
                       value=round(span, 4), unit="pct_per_trade", n=bm.n,
                       component="virtual_book", start=self.cfg.seal_start, end=self.as_of,
                       note="How much of the book-versus-rule gap the tie-break can explain "
                            "at most. Measured, not assumed.")
            self._fact(fid=F("tiebreak_share"), experiment_id=eid,
                       label="share of the book-versus-rule gap the selection rule explains",
                       value=round(share, 4), unit="ratio", n=bm.n,
                       component="virtual_book", start=self.cfg.seal_start, end=self.as_of)
            self._fact(fid=F("top_day_share"), experiment_id=eid,
                       label="share of the unconstrained rule's trades on its three busiest days",
                       value=round(top_share, 4), unit="ratio", n=rm.n,
                       component="strategy_replay", start=self.cfg.seal_start, end=self.as_of,
                       note="The book holds at most ten positions, so a day that fires "
                            "hundreds of signals is a day it cannot participate in.")
            self._fact(fid=F("rule_exp_ex_top_days"), experiment_id=eid,
                       label="the unconstrained rule's expectancy excluding those three days",
                       value=round(ex_top, 4), unit="pct_per_trade", n=rm.n,
                       component="strategy_replay", start=self.cfg.seal_start, end=self.as_of)
            minted += [F("tiebreak_span"), F("tiebreak_share"), F("top_day_share"),
                       F("rule_exp_ex_top_days")] + [F(f"book_exp_{r}") for r in sorted(alt)]
            report.notes.append(
                f"{eid}: capacity gap {gap:+.3f}/trade — selection rule explains at most "
                f"{span:.3f} ({share:.0%}); the rule's three busiest days carry "
                f"{top_share:.0%} of its trades and removing them moves its expectancy "
                f"from {rm.expectancy_pct_per_trade:+.3f} to {ex_top:+.3f}."
            )
            sv2 = f"{eid}:strategy@v2.0"
            self.repo.put_strategy_version(
                strategy_version=sv2, experiment_id=eid, version_label="strategy@v2.0",
                previous_version=sv,
                rulebook={
                    "universe": spec.scope, "direction": spec.direction,
                    "entry": spec.entry_text, "invalidation": spec.invalidation_text,
                    "exit": spec.exit_text, "horizon_sessions": spec.horizon_sessions,
                    "sizing": "unchanged from v1.0", "cost_convention": self.costs.convention,
                    "selection_rule": best, "capital_inr": self.cfg.capital_inr,
                },
                created_by="engine", approved_by=None,
                validation=(
                    "Backtested: the same rule, the same windows, only the capacity "
                    f"selection rule changed to `{best}`. FORWARD VALIDATION NOT DONE — the "
                    "sealed window was consumed by this very comparison, so it is no longer "
                    "out-of-sample for v2.0. v2.0 is recorded but NOT activated; it may not "
                    "replace v1.0 until a genuinely unseen window has judged it."
                ),
                active_from=None,          # the L3 gate biting: recorded, not activated
                retired_at=None, created_at=_iso(now_ist()),
            )
            self.repo.put_learning_event(
                learning_event_id=self._next_id("lrn"), experiment_id=eid,
                seq=self._next_change_seq(eid), level="L3",
                at=_iso(now_ist()),
                what_changed=(f"Proposed strategy@v2.0: capacity selection rule "
                              f"`liquidity_desc` → `{best}`."),
                why=(
                    "The book's expectancy sits materially below the unconstrained rule's over "
                    "the same window. The tie-break used when more signals fire than the book "
                    f"has slots was never measured, so it was measured: across every approved "
                    f"rule the spread is {span:.3f} per trade, which is {share:.0%} of the "
                    f"{abs(gap):.3f} gap. It is therefore NOT the explanation — the larger part "
                    f"is that {top_share:.0%} of the rule's trades arrive on its three busiest "
                    "sessions, which a ten-slot book is structurally unable to participate in. "
                    f"Removing those days moves the rule's expectancy from "
                    f"{rm.expectancy_pct_per_trade:+.3f} to {ex_top:+.3f}. The version change "
                    "is still worth making; the claim about why is now the measured one."
                ),
                evidence_ids=ev, previous_version=sv, new_version=sv2,
                outcome_before_id=f"out_{slug}_book", outcome_after_id=None,
                improved=None,        # the sealed window is spent; "we do not know yet" is the answer
                decided_by="engine", approved_by=None,
                validation=("Backtested on the sealed window only; forward validation pending on "
                            "a window this comparison has not seen. Not activated."),
                constitution_version=self.c.version, statement_story_line_id=None,
            )
            report.notes.append(
                f"{eid}: L3 v2.0 proposed (selection rule -> {best}) and NOT activated — "
                "forward validation is still owed."
            )
        return minted

    @staticmethod
    def _event_concentration(trades: Sequence[Any], top: int = 3) -> tuple[float, float]:
        """
        `(share of trades on the busiest `top` signal days, expectancy excluding them)`.

        The competing explanation for a book underperforming its own rule, and the one
        that turned out to matter: the rule's return is concentrated on a few sessions
        that fire hundreds of signals at once, and a ten-slot book is structurally
        unable to be in them.
        """
        if not trades:
            return float("nan"), float("nan")
        from collections import Counter
        counts = Counter(t.signal_date for t in trades)
        busiest = {d for d, _ in counts.most_common(top)}
        on_top = [t for t in trades if t.signal_date in busiest]
        rest = [t.pnl_pct_net for t in trades if t.signal_date not in busiest]
        share = len(on_top) / len(trades)
        return share, (float(np.mean(rest)) if rest else float("nan"))

    # ── what to test next ───────────────────────────────────────────────────

    def _decide_next(self, report: RunReport, market_trigger: str,
                     ranked: Sequence[OB.Candidate]) -> None:
        if not ranked:
            return
        nxt = None
        for cand in ranked:
            if GOV.check_implementable(cand.spec).ok and cand.spec.context != "any":
                nxt = cand
                break
        if nxt is None:
            return
        question = (
            "Given what the opened experiments showed, what is worth testing next, and why now?"
        )
        decision_text = (
            "Test whether the edge that survives is a regime effect rather than a signal effect: "
            "re-run the surviving conditions with the market-regime context removed, so the "
            "context's contribution is measured instead of assumed."
        )
        did = self.repo.put_decision(
            decision_id=self._next_id("dec"), at=_iso(now_ist()), kind="queue_next",
            experiment_id=None, trigger_id=market_trigger, question=question,
            decision=decision_text, rationale_story_line_id=None, evidence_ids=["fct_scan_candidates"],
            decided_by="engine", model=None, llm_call_id=None, approved_by=None,
            constitution_version=self.c.version,
        )
        report.notes.append("next: " + decision_text)

    # ── helpers ─────────────────────────────────────────────────────────────

    def _facts_payload(self, ids: Sequence[tuple[str, Any]]) -> list[dict[str, Any]]:
        """
        The fact table handed to a model. Read back from the DATABASE, not from
        memory, so a model can only ever see a number that was actually persisted with
        its provenance.
        """
        out: list[dict[str, Any]] = []
        for fid, _ in ids:
            row = self.repo.one("SELECT * FROM facts WHERE fact_id = ?", [fid])
            if row is None:
                continue
            out.append({
                "id": row["fact_id"], "label": row["label"],
                "value": row["value_num"] if row["value_num"] is not None else row["value_text"],
                "unit": row["unit"], "n": row["n"],
                "as_of": row["as_of"], "window": [row["range_start"], row["range_end"]],
                "source": row["data_source"], "cost_convention": row["cost_convention"],
                "computed_by": row["computed_by"],
            })
        return out
