"""
`EngineStore` — the P0 contract, served from the authoritative tables the loop wrote.

The whole point of the P0 seam: the router, the schemas and the guardrail tests do not
change. The only thing that changes is where the rows come from. If a payload
assembled here breaks a product law, the Pydantic validators refuse to serialise it —
so the honesty rules are enforced against ENGINE output exactly as they were against
fixtures.

Reads only. There is no INSERT in this class, and the tables would refuse one anyway.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Optional

from .engine.repository import Repository
from .schemas import (
    Author, ChangeLogEntry, DateRange, DeathCause, Evidence, EvidenceKind,
    ExperimentDetail, ExperimentListResponse, ExperimentStatus, ExperimentSummary, Fact,
    Learning, LearningLevel, LearningsResponse, LlmUsage, LoopResponse, LoopStage,
    ModelUsage, NextTest, PerformanceBlock, PostMortem, Provenance, Rulebook, Spark,
    StoryBeat, StoryLine, Trigger, TriggerType, Unit, VirtualBook, VirtualTrade,
    Confidence,
)

#: `died` first, `promoted` last. Losers lead, always (CLAUDE.md).
STATUS_ORDER = {
    ExperimentStatus.died: 0, ExperimentStatus.queued: 1, ExperimentStatus.testing: 2,
    ExperimentStatus.validating: 3, ExperimentStatus.promising: 4,
    ExperimentStatus.promoted: 5,
}
STAGE_FOR_STATUS = {
    ExperimentStatus.queued: LoopStage.hypothesize,
    ExperimentStatus.testing: LoopStage.test,
    ExperimentStatus.validating: LoopStage.interpret,
    ExperimentStatus.promising: LoopStage.track,
    ExperimentStatus.promoted: LoopStage.track,
    ExperimentStatus.died: LoopStage.learn,
}


def _d(v: Any) -> date:
    return date.fromisoformat(str(v)[:10])


def _dt(v: Any) -> datetime:
    return datetime.fromisoformat(str(v))


def _arr(v: Any) -> list[str]:
    if not v:
        return []
    return list(v) if isinstance(v, (list, tuple)) else list(json.loads(v))


class EngineStore:
    """P1 store: the real research loop's output, read back through the P0 contract."""

    def __init__(self, db_path: str) -> None:
        self.repo = Repository(db_path)
        self.source_name = f"pathfinder engine (P1) — authoritative tables at {db_path}"

    # ── row → model ─────────────────────────────────────────────────────────

    @staticmethod
    def _provenance(r: Any) -> Provenance:
        return Provenance(
            data_source=r["data_source"],
            date_range=DateRange(start=_d(r["range_start"]), end=_d(r["range_end"])),
            as_of=_d(r["as_of"]), cost_convention=r["cost_convention"],
            computed_by=r["computed_by"], computed_at=_dt(r["computed_at"]),
            universe=(r["universe"] if "universe" in r.keys() else None),
        )

    def _fact(self, r: Any) -> Fact:
        value: float | int | str = (
            r["value_num"] if r["value_num"] is not None else str(r["value_text"])
        )
        return Fact(
            id=r["fact_id"], label=r["label"], value=value, unit=Unit(r["unit"]),
            n=r["n"], provenance=self._provenance(r), note=r["note"],
        )

    def _facts_for(self, experiment_id: Optional[str]) -> list[Fact]:
        if experiment_id is None:
            rows = self.repo.query("SELECT * FROM facts WHERE experiment_id IS NULL ORDER BY fact_id")
        else:
            rows = self.repo.query(
                "SELECT * FROM facts WHERE experiment_id = ? OR experiment_id IS NULL "
                "ORDER BY fact_id", [experiment_id])
        # Series facts are carried as text and are not renderable as a number.
        return [self._fact(r) for r in rows if not str(r["fact_id"]).endswith("_curve")]

    @staticmethod
    def _perf(r: Any) -> PerformanceBlock:
        return PerformanceBlock(
            basis=r["basis"], label=r["label"],
            expectancy_pct_per_trade=r["expectancy_pct_per_trade"],
            expectancy_2x_slippage_pct_per_trade=r["expectancy_2x_slippage_pct_per_trade"],
            total_return_pct=r["total_return_pct"],
            max_drawdown_pct=r["max_drawdown_pct"],
            current_drawdown_pct=r["current_drawdown_pct"],
            win_rate_pct=r["win_rate_pct"], avg_win_pct=r["avg_win_pct"],
            avg_loss_pct=r["avg_loss_pct"], payoff_ratio=r["payoff_ratio"],
            n=r["n"], occurrences=r["occurrences"],
            provenance=EngineStore._provenance(r),
        )

    def _outcome(self, experiment_id: str, basis_suffix: str) -> Optional[Any]:
        slug = experiment_id.replace("exp_", "e")
        return self.repo.one("SELECT * FROM outcomes WHERE outcome_id = ?",
                             [f"out_{slug}_{basis_suffix}"])

    @staticmethod
    def _story(r: Any) -> StoryLine:
        return StoryLine(
            beat=StoryBeat(r["beat"]), headline=r["headline"], body=r["body"],
            produced_by=Author(r["produced_by"]), model=r["model"],
            prompt_version=r["prompt_version"], at=_dt(r["at"]),
            fact_refs=_arr(r["fact_ids"]),
        )

    @staticmethod
    def _trigger(r: Any) -> Trigger:
        return Trigger(
            id=r["trigger_id"], type=TriggerType(r["type"]), description=r["description"],
            fired_at=_dt(r["fired_at"]), fired_by="engine", fact_refs=_arr(r["fact_ids"]),
        )

    @staticmethod
    def _trade(r: Any) -> VirtualTrade:
        return VirtualTrade(
            id=r["trade_id"], symbol=r["symbol"], direction=r["direction"],
            signal_date=_d(r["signal_date"]), entry_date=_d(r["entry_date"]),
            entry_price=r["entry_price"],
            exit_date=(_d(r["exit_date"]) if r["exit_date"] else None),
            exit_price=r["exit_price"], exit_reason=r["exit_reason"],
            holding_sessions=r["holding_sessions"], pnl_pct_gross=r["pnl_pct_gross"],
            pnl_pct_net=r["pnl_pct_net"], costs_pct=r["costs_pct"],
            slippage_bps=r["slippage_bps"], mfe_pct=r["mfe_pct"], mae_pct=r["mae_pct"],
            as_of=_d(r["as_of"]),
        )

    def _spark(self, experiment_id: str) -> Optional[Spark]:
        slug = experiment_id.replace("exp_", "e")
        r = self.repo.one("SELECT * FROM facts WHERE fact_id = ?", [f"fct_{slug}_book_curve"])
        if r is None or not r["value_text"]:
            return None
        pts = json.loads(r["value_text"])
        if len(pts) < 2:
            return None
        return Spark(
            basis="virtual_equity_pct", points=[float(x) for x in pts],
            date_range=DateRange(start=_d(r["range_start"]), end=_d(r["range_end"])),
            as_of=_d(r["as_of"]),
        )

    # ── the four reads ──────────────────────────────────────────────────────

    def _summary(self, r: Any) -> ExperimentSummary:
        eid = r["experiment_id"]
        hyp = self.repo.one("SELECT * FROM hypotheses WHERE hypothesis_id = ?", [r["hypothesis_id"]])
        hist = self._outcome(eid, "validation") or self._outcome(eid, "discovery")
        book = self._outcome(eid, "book")
        sv = self.repo.one(
            "SELECT strategy_version FROM strategy_versions WHERE experiment_id = ? "
            "ORDER BY created_at LIMIT 1", [eid])
        return ExperimentSummary(
            id=eid, hypothesis=hyp["statement"], status=ExperimentStatus(r["status"]),
            opened_at=_dt(r["opened_at"]),
            strategy_version=(sv["strategy_version"] if sv else None),
            constitution_version=r["constitution_version"],
            historical_return=(self._perf(hist) if hist else None),
            virtual_return=(self._perf(book) if book else None),
            occurrences=(hist["occurrences"] if hist else None),
            n=(book["n"] if book else (hist["n"] if hist else None)),
            spark=self._spark(eid), as_of=_dt(r["status_at"]),
        )

    def experiments(self, status: Optional[ExperimentStatus] = None) -> ExperimentListResponse:
        rows = self.repo.query("SELECT * FROM experiments_current")
        items = [self._summary(r) for r in rows]
        if status is not None:
            items = [i for i in items if i.status == status]
        # L-7 at the LIST level: the graveyard leads, the winners come last.
        items.sort(key=lambda i: (STATUS_ORDER[i.status], i.id))
        return ExperimentListResponse(
            as_of=self._as_of(), status_filter=status, count=len(items), items=items,
        )

    def experiment(self, experiment_id: str) -> Optional[ExperimentDetail]:
        r = self.repo.one("SELECT * FROM experiments_current WHERE experiment_id = ?",
                          [experiment_id])
        if r is None:
            return None
        eid = experiment_id
        summary = self._summary(r)
        hyp = self.repo.one("SELECT * FROM hypotheses WHERE hypothesis_id = ?", [r["hypothesis_id"]])
        sv = self.repo.one(
            "SELECT * FROM strategy_versions WHERE experiment_id = ? ORDER BY created_at LIMIT 1",
            [eid])
        rb = json.loads(sv["rulebook"])

        facts = self._facts_for(eid)
        known = {f.id for f in facts}
        story = [self._story(x) for x in self.repo.query(
            "SELECT * FROM story_lines WHERE experiment_id = ? ORDER BY at, story_line_id", [eid])]
        beat_order = {b.value: i for i, b in enumerate(StoryBeat)}
        story.sort(key=lambda s: beat_order[s.beat.value])

        evidence: list[Evidence] = []
        for e in self.repo.query("SELECT * FROM evidence WHERE experiment_id = ? ORDER BY evidence_id", [eid]):
            out = (self.repo.one("SELECT * FROM outcomes WHERE outcome_id = ?", [e["outcome_id"]])
                   if e["outcome_id"] else None)
            evidence.append(Evidence(
                id=e["evidence_id"], kind=EvidenceKind(e["kind"]), title=e["title"],
                label=e["label"], performance=(self._perf(out) if out else None),
                fact_refs=[f for f in _arr(e["fact_ids"]) if f in known],
                provenance=self._provenance(e),
            ))

        book_row = self._outcome(eid, "book")
        virtual_book = None
        if book_row is not None:
            trades = [self._trade(t) for t in self.repo.query(
                "SELECT * FROM trades WHERE experiment_id = ? AND exit_date IS NOT NULL "
                "ORDER BY pnl_pct_net ASC", [eid])]
            virtual_book = VirtualBook(
                metrics=self._perf(book_row),
                capital_inr=float(rb.get("capital_inr", 1_000_000.0)),
                ledger_losers_first=trades, open_positions=[],
            )

        change_log = [self._change(c) for c in self.repo.query(
            "SELECT * FROM learning_events WHERE experiment_id = ? ORDER BY seq", [eid])]

        pm_row = self.repo.one("SELECT * FROM post_mortems WHERE experiment_id = ?", [eid])
        post_mortem = None
        if pm_row is not None:
            sl = self.repo.one("SELECT * FROM story_lines WHERE story_line_id = ?",
                               [pm_row["summary_story_line_id"]])
            post_mortem = PostMortem(
                died_at=_dt(pm_row["died_at"]), cause=DeathCause(pm_row["cause"]),
                summary=self._story(sl), what_we_kept=pm_row["what_we_kept"],
                evidence_refs=_arr(pm_row["evidence_ids"]),
                retired_version=pm_row["retired_version"],
                decided_by=Author(pm_row["decided_by"]), approved_by=pm_row["approved_by"],
            )

        triggers = [self._trigger(t) for t in self.repo.query(
            "SELECT * FROM triggers WHERE experiment_id = ? ORDER BY fired_at", [eid])]

        return ExperimentDetail(
            **summary.model_dump(),
            question=hyp["question"], rationale=hyp["rationale"],
            rulebook=Rulebook(
                universe=rb["universe"], direction=rb["direction"], entry=rb["entry"],
                invalidation=rb["invalidation"], exit=rb["exit"],
                horizon_sessions=int(rb["horizon_sessions"]), sizing=rb["sizing"],
                cost_convention=rb["cost_convention"],
            ),
            stage=STAGE_FOR_STATUS[summary.status],
            story=story or [self._placeholder_line(eid)],
            facts=facts, evidence=evidence, virtual_book=virtual_book,
            change_log=change_log, post_mortem=post_mortem, triggers=triggers,
            llm_usage=self._llm_usage(experiment_id=eid),
            next_review=None,
        )

    def _change(self, c: Any) -> ChangeLogEntry:
        before = (self.repo.one("SELECT * FROM outcomes WHERE outcome_id = ?", [c["outcome_before_id"]])
                  if c["outcome_before_id"] else None)
        after = (self.repo.one("SELECT * FROM outcomes WHERE outcome_id = ?", [c["outcome_after_id"]])
                 if c["outcome_after_id"] else None)
        return ChangeLogEntry(
            seq=c["seq"], level=LearningLevel(c["level"]), at=_dt(c["at"]),
            what_changed=c["what_changed"], why=c["why"],
            evidence_refs=_arr(c["evidence_ids"]), previous_version=c["previous_version"],
            new_version=c["new_version"],
            performance_before=(self._perf(before) if before else None),
            performance_after=(self._perf(after) if after else None),
            improved=(None if c["improved"] is None else bool(c["improved"])),
            decided_by=Author(c["decided_by"]), approved_by=c["approved_by"],
            constitution_version=c["constitution_version"], validation=c["validation"],
        )

    @staticmethod
    def _placeholder_line(eid: str) -> StoryLine:
        return StoryLine(
            beat=StoryBeat.noticed, headline="This experiment has no narrative yet",
            body="The engine has recorded evidence for this experiment but no story beat "
                 "has been written for it.",
            produced_by=Author.engine, at=datetime.now(), fact_refs=[],
        )

    def _as_of(self) -> datetime:
        r = self.repo.one("SELECT as_of FROM research_cycles ORDER BY as_of DESC LIMIT 1")
        return _dt(r["as_of"]) if r else datetime.now()

    def _llm_usage(self, *, experiment_id: Optional[str] = None) -> Optional[LlmUsage]:
        if experiment_id:
            rows = self.repo.query(
                "SELECT model, COUNT(*) c, SUM(input_tokens) i, SUM(output_tokens) o, "
                "SUM(cache_read_input_tokens) cr, SUM(cache_creation_input_tokens) cc, "
                "SUM(cost_usd) cost FROM llm_calls WHERE experiment_id = ? GROUP BY model",
                [experiment_id])
            window = "experiment_lifetime"
        else:
            rows = self.repo.query(
                "SELECT model, COUNT(*) c, SUM(input_tokens) i, SUM(output_tokens) o, "
                "SUM(cache_read_input_tokens) cr, SUM(cache_creation_input_tokens) cc, "
                "SUM(cost_usd) cost FROM llm_calls GROUP BY model")
            window = "today_ist"
        by_model = [ModelUsage(
            model=r["model"], calls=r["c"], input_tokens=r["i"] or 0, output_tokens=r["o"] or 0,
            cache_read_input_tokens=r["cr"] or 0, cache_creation_input_tokens=r["cc"] or 0,
            cost_usd=float(r["cost"] or 0.0)) for r in rows]
        total = sum(m.cost_usd for m in by_model)
        cv = self.repo.one("SELECT document FROM constitution_versions ORDER BY rowid DESC LIMIT 1")
        daily = None
        if cv:
            daily = float(json.loads(cv["document"]).get("llm", {}).get("daily_budget_usd", 0.0)) or None
        return LlmUsage(
            window=window, by_model=by_model, total_cost_usd=total,
            daily_budget_usd=daily,
            budget_used_pct=(100.0 * total / daily if daily else None),
            as_of=self._as_of(),
        )

    def loop(self) -> LoopResponse:
        cyc = self.repo.one("SELECT * FROM research_cycles ORDER BY as_of DESC, rowid DESC LIMIT 1")
        if cyc is None:
            raise RuntimeError("no research cycle recorded — run scripts/run_pathfinder_loop.py")
        # The CYCLE's own story is the one with no experiment attached. Experiment
        # beats also carry the cycle id (they happened during it), and including them
        # here made the loop endpoint serve one experiment's narrative as the whole
        # loop's — which reads fine and is wrong.
        story = [self._story(r) for r in self.repo.query(
            "SELECT * FROM story_lines WHERE cycle_id = ? AND experiment_id IS NULL "
            "ORDER BY at, story_line_id", [cyc["cycle_id"]])]
        beat_order = {b.value: i for i, b in enumerate(StoryBeat)}
        seen: set[str] = set()
        ordered: list[StoryLine] = []
        for s in sorted(story, key=lambda s: beat_order[s.beat.value]):
            if s.beat.value in seen:
                continue
            seen.add(s.beat.value)
            ordered.append(s)

        facts = self._facts_for(None)
        known = {f.id for f in facts}
        # A cycle-level line may reference an experiment-level fact; carry those too,
        # rather than dropping the reference and leaving a dangling token.
        for s in ordered:
            for ref in s.fact_refs:
                if ref not in known:
                    row = self.repo.one("SELECT * FROM facts WHERE fact_id = ?", [ref])
                    if row is not None:
                        facts.append(self._fact(row))
                        known.add(ref)

        counts = {s: 0 for s in ExperimentStatus}
        for r in self.repo.query("SELECT status, COUNT(*) c FROM experiments_current GROUP BY status"):
            counts[ExperimentStatus(r["status"])] = r["c"]

        trg = (self.repo.one("SELECT * FROM triggers WHERE trigger_id = ?", [cyc["trigger_id"]])
               if cyc["trigger_id"] else None)
        trigger = self._trigger(trg) if trg else None
        if trigger:
            trigger = Trigger(**{**trigger.model_dump(),
                                 "fact_refs": [f for f in trigger.fact_refs if f in known]})

        return LoopResponse(
            as_of=_dt(cyc["as_of"]), cycle_id=cyc["cycle_id"], stage=LoopStage(cyc["stage"]),
            constitution_version=cyc["constitution_version"],
            active_experiment_id=cyc["active_experiment_id"], trigger=trigger,
            story=ordered or [self._placeholder_line("cycle")], facts=facts, counts=counts,
            llm_usage=self._llm_usage(),
        )

    def learnings(self) -> LearningsResponse:
        rows = self.repo.query(
            "SELECT * FROM learning_events ORDER BY at DESC, learning_event_id DESC")
        facts = self._facts_for(None)
        known = {f.id for f in facts}
        learned: list[Learning] = []
        for i, c in enumerate(rows, start=1):
            sl = (self.repo.one("SELECT * FROM story_lines WHERE story_line_id = ?",
                                [c["statement_story_line_id"]])
                  if c["statement_story_line_id"] else None)
            statement = self._story(sl) if sl else StoryLine(
                beat=StoryBeat.learning, headline=c["what_changed"][:118],
                body=c["why"], produced_by=Author.engine, at=_dt(c["at"]), fact_refs=[],
            )
            refs = [r for r in statement.fact_refs if r in known]
            n = None
            if c["experiment_id"]:
                o = self._outcome(c["experiment_id"], "book") or self._outcome(c["experiment_id"], "validation")
                n = o["n"] if o else None
            learned.append(Learning(
                id=f"lrn_{i:04d}", statement=StoryLine(**{**statement.model_dump(), "fact_refs": refs}),
                level=LearningLevel(c["level"]), learned_at=_dt(c["at"]),
                from_experiments=[c["experiment_id"] or "cycle"],
                evidence_refs=_arr(c["evidence_ids"]), fact_refs=refs,
                confidence=(Confidence.provisional if c["improved"] is None else Confidence.supported),
                n=n, applied_in=[c["new_version"]],
            ))

        testing_next: list[NextTest] = []
        for i, d in enumerate(self.repo.query(
                "SELECT * FROM decisions WHERE kind = 'queue_next' ORDER BY at DESC"), start=1):
            testing_next.append(NextTest(
                id=f"nxt_{i:04d}", question=d["question"],
                why_now=StoryLine(
                    beat=StoryBeat.next, headline="What the loop decided to test next",
                    body=d["decision"], produced_by=Author(d["decided_by"]),
                    model=d["model"], at=_dt(d["at"]), fact_refs=[]),
                planned_test=d["decision"],
                blocked_by=None, queued_experiment_id=d["experiment_id"], fact_refs=[],
            ))

        cv = self.repo.one("SELECT version FROM constitution_versions ORDER BY rowid DESC LIMIT 1")
        return LearningsResponse(
            as_of=self._as_of(), constitution_version=(cv["version"] if cv else "unknown"),
            learned=learned, testing_next=testing_next, facts=facts,
        )
