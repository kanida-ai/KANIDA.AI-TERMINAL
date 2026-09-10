"""
Pathfinder P0 — HONEST fixtures.

These are hand-authored sample payloads, not engine output. They exist so the
frontend track (P2) can build the real screens while the engine track (P1) builds
the real loop. Every fixture is constructed THROUGH the Pydantic models in
schemas.py, so a fixture that breaks a product law fails at import.

WHAT "HONEST" MEANS HERE (these fixtures are deliberately unflattering):
  * 6 experiments, one per lifecycle status, incl. a died-with-post-mortem.
  * 2 of the 6 fail the 2x-slippage gate. That is the realistic hit rate.
  * The forward virtual book is WEAKER than the historical replay in 5 of 5
    cases that have one — which is what actually happens.
  * Small n is everywhere and flagged: one greyed (n<20), two flagged (n<50).
  * Drawdowns are larger than returns in several places. They are never hidden.
  * No number appears in LLM-authored prose; every one is a fact reference.

NOT REAL RESULTS. Nothing here was traded, mined, or measured. P1 replaces this
whole module with the Postgres-backed store.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional

from .schemas import (
    Author,
    ChangeLogEntry,
    Confidence,
    DateRange,
    DeathCause,
    Direction,
    Evidence,
    EvidenceKind,
    ExperimentDetail,
    ExperimentListResponse,
    ExperimentStatus,
    ExperimentSummary,
    Fact,
    Learning,
    LearningLevel,
    LearningsResponse,
    LlmUsage,
    LoopResponse,
    LoopStage,
    ModelUsage,
    NextTest,
    PerformanceBlock,
    PostMortem,
    Provenance,
    Rulebook,
    SIMULATED_LABEL,
    Spark,
    StoryBeat,
    StoryLine,
    Trigger,
    TriggerType,
    Unit,
    VIRTUAL_LABEL,
    VirtualBook,
    VirtualTrade,
)

# ── The point-in-time frame every fixture number is stamped with ─────────────
# Memory rule: every time/date in this product is IST (Asia/Kolkata), computed
# explicitly. The build machine is Pacific — never rely on local time.
IST = timezone(timedelta(hours=5, minutes=30))

AS_OF_TS = datetime(2026, 9, 8, 16, 5, 0, tzinfo=IST)   # after the 15:30 close
AS_OF = AS_OF_TS.date()

DISCOVERY = DateRange(start=date(2018, 1, 1), end=date(2025, 12, 31))
FORWARD = DateRange(start=date(2026, 1, 5), end=AS_OF)

DATA_SOURCE = "kite_eod_adjusted_nifty500"
UNIVERSE_PIT = "nifty500_pit"
COSTS_V3 = (
    "costs_v3: brokerage+STT+exchange+GST+stamp, slippage 10bps, "
    "entry next open, exit at rule"
)
REPLAY_ENGINE = "replay_engine@1.4.0"
BOOK_ENGINE = "book_engine@2.3.1"
OBSERVER = "regime_observer@0.9.2"

CONSTITUTION = "constitution@1.3.0"
CAPITAL_INR = 1_000_000.0   # virtual only

MODEL_REASON = "claude-sonnet-5"
MODEL_NARRATE = "claude-haiku-4-5"


# ── Small builders ───────────────────────────────────────────────────────────

def _prov(
    window: DateRange,
    computed_by: str = BOOK_ENGINE,
    *,
    as_of: date = AS_OF,
    computed_at: datetime = AS_OF_TS,
) -> Provenance:
    return Provenance(
        data_source=DATA_SOURCE,
        date_range=window,
        as_of=as_of,
        cost_convention=COSTS_V3,
        computed_by=computed_by,
        computed_at=computed_at,
        universe=UNIVERSE_PIT,
    )


def _fact(
    fid: str,
    label: str,
    value,
    unit: Unit,
    n: Optional[int] = None,
    window: DateRange = DISCOVERY,
    computed_by: str = BOOK_ENGINE,
    note: Optional[str] = None,
) -> Fact:
    return Fact(
        id=fid, label=label, value=value, unit=unit, n=n,
        provenance=_prov(window, computed_by), note=note,
    )


def _hist(
    *, expectancy: float, expectancy_2x: float, total_return: Optional[float],
    max_dd: float, cur_dd: float, n: int, occurrences: int,
    win_rate: Optional[float] = None, avg_win: Optional[float] = None,
    avg_loss: Optional[float] = None,
) -> PerformanceBlock:
    return PerformanceBlock(
        basis="historical_replay",
        label=SIMULATED_LABEL,
        expectancy_pct_per_trade=expectancy,
        expectancy_2x_slippage_pct_per_trade=expectancy_2x,
        total_return_pct=total_return,
        max_drawdown_pct=max_dd,
        current_drawdown_pct=cur_dd,
        win_rate_pct=win_rate,
        avg_win_pct=avg_win,
        avg_loss_pct=avg_loss,
        payoff_ratio=(round(avg_win / abs(avg_loss), 2) if avg_win and avg_loss else None),
        n=n,
        occurrences=occurrences,
        provenance=_prov(DISCOVERY, REPLAY_ENGINE),
    )


def _virt(
    *, expectancy: float, expectancy_2x: float, total_return: Optional[float],
    max_dd: float, cur_dd: float, n: int, occurrences: Optional[int] = None,
    win_rate: Optional[float] = None, avg_win: Optional[float] = None,
    avg_loss: Optional[float] = None, window: DateRange = FORWARD,
) -> PerformanceBlock:
    return PerformanceBlock(
        basis="virtual_book",
        label=VIRTUAL_LABEL,
        expectancy_pct_per_trade=expectancy,
        expectancy_2x_slippage_pct_per_trade=expectancy_2x,
        total_return_pct=total_return,
        max_drawdown_pct=max_dd,
        current_drawdown_pct=cur_dd,
        win_rate_pct=win_rate,
        avg_win_pct=avg_win,
        avg_loss_pct=avg_loss,
        payoff_ratio=(round(avg_win / abs(avg_loss), 2) if avg_win and avg_loss else None),
        n=n,
        occurrences=occurrences,
        provenance=_prov(window, BOOK_ENGINE),
    )


def _trade(
    tid: str, symbol: str, direction: Direction, signal: date, entry: date,
    entry_px: float, exit_d: Optional[date], exit_px: Optional[float],
    reason: Optional[str], net: Optional[float], gross: Optional[float] = None,
    mfe: Optional[float] = None, mae: Optional[float] = None,
) -> VirtualTrade:
    return VirtualTrade(
        id=tid, symbol=symbol, direction=direction,
        signal_date=signal, entry_date=entry, entry_price=entry_px,
        exit_date=exit_d, exit_price=exit_px, exit_reason=reason,
        holding_sessions=((exit_d - entry).days if exit_d else None),
        pnl_pct_gross=gross, pnl_pct_net=net,
        costs_pct=(round(gross - net, 3) if (gross is not None and net is not None) else None),
        slippage_bps=10.0, mfe_pct=mfe, mae_pct=mae, as_of=AS_OF,
    )


def _line(
    beat: StoryBeat, headline: str, body: str, *, author: Author,
    model: Optional[str] = None, at: datetime = AS_OF_TS,
    refs: Optional[list[str]] = None, prompt_version: Optional[str] = None,
) -> StoryLine:
    return StoryLine(
        beat=beat, headline=headline, body=body, produced_by=author, model=model,
        prompt_version=prompt_version, at=at, fact_refs=refs or [],
    )


def _spark(points: list[float], window: DateRange, basis="virtual_equity_pct") -> Spark:
    return Spark(basis=basis, points=points, date_range=window, as_of=AS_OF)


# ═════════════════════════════════════════════════════════════════════════════
#  exp_0003 — PROMOTED
#  Pullback-on-dry-volume. The one that worked, and is still honest about it:
#  forward expectancy is barely a third of the historical replay.
# ═════════════════════════════════════════════════════════════════════════════

E3_HIST = _hist(
    expectancy=0.31, expectancy_2x=0.19, total_return=None,
    max_dd=11.4, cur_dd=0.0, n=184, occurrences=406,
    win_rate=47.8, avg_win=1.62, avg_loss=-0.89,
)
E3_VIRT = _virt(
    expectancy=0.18, expectancy_2x=0.06, total_return=4.1,
    max_dd=7.9, cur_dd=5.1, n=61, occurrences=98,
    win_rate=44.3, avg_win=1.48, avg_loss=-0.85,
)

E3_FACTS = [
    _fact("fct_e3_hist_exp", "expectancy per trade, historical replay, net of costs",
          0.31, Unit.pct_per_trade, 184),
    _fact("fct_e3_hist_exp_2x", "expectancy per trade at 2x slippage, historical replay",
          0.19, Unit.pct_per_trade, 184),
    _fact("fct_e3_hist_dd", "worst drawdown, historical replay", 11.4, Unit.pct, 184),
    _fact("fct_e3_virt_exp", "expectancy per trade, forward virtual book, net of costs",
          0.18, Unit.pct_per_trade, 61, FORWARD),
    _fact("fct_e3_virt_exp_2x", "expectancy per trade at 2x slippage, forward virtual book",
          0.06, Unit.pct_per_trade, 61, FORWARD),
    _fact("fct_e3_virt_dd", "worst drawdown, forward virtual book", 7.9, Unit.pct, 61, FORWARD),
    _fact("fct_e3_virt_cur_dd", "drawdown from peak right now, forward virtual book",
          5.1, Unit.pct, 61, FORWARD),
    _fact("fct_e3_virt_n", "closed virtual trades so far", 61, Unit.count, 61, FORWARD),
    _fact("fct_e3_dryvol_exp", "expectancy when pullback volume is below the twenty-session median",
          0.31, Unit.pct_per_trade, 184),
    _fact("fct_e3_wetvol_exp", "expectancy when pullback volume is above that median",
          -0.04, Unit.pct_per_trade, 222,
          note="The contrast is the whole edge: it is a volume story, not a price story."),
    _fact("fct_e3_v2_gain", "expectancy improvement of version three over version two, forward",
          0.07, Unit.pct_per_trade, 61, FORWARD),
]

E3_EVIDENCE = [
    Evidence(
        id="evd_e3_replay", kind=EvidenceKind.historical_replay,
        title="Strategy-replay over the discovery window, under the exact traded rule",
        label=SIMULATED_LABEL, performance=E3_HIST,
        fact_refs=["fct_e3_hist_exp", "fct_e3_hist_exp_2x", "fct_e3_hist_dd"],
        provenance=_prov(DISCOVERY, REPLAY_ENGINE),
    ),
    Evidence(
        id="evd_e3_volsplit", kind=EvidenceKind.regime_split,
        title="Split by pullback volume vs its twenty-session median",
        label=SIMULATED_LABEL, performance=None,
        fact_refs=["fct_e3_dryvol_exp", "fct_e3_wetvol_exp"],
        provenance=_prov(DISCOVERY, REPLAY_ENGINE),
        interpretation=_line(
            StoryBeat.learning,
            "The edge lives in the volume condition, not in the pullback",
            "Pullbacks on drying volume carry an expectancy of {{fact:fct_e3_dryvol_exp}}. "
            "The same price shape on rising volume carries {{fact:fct_e3_wetvol_exp}}. "
            "The price pattern on its own is not the edge; the participation collapse is.",
            author=Author.llm, model=MODEL_REASON, prompt_version="interpret@2",
            refs=["fct_e3_dryvol_exp", "fct_e3_wetvol_exp"],
        ),
    ),
    Evidence(
        id="evd_e3_forward", kind=EvidenceKind.forward_virtual,
        title="Forward virtual book since promotion",
        label=VIRTUAL_LABEL, performance=E3_VIRT,
        fact_refs=["fct_e3_virt_exp", "fct_e3_virt_exp_2x", "fct_e3_virt_dd",
                   "fct_e3_virt_cur_dd", "fct_e3_virt_n"],
        provenance=_prov(FORWARD, BOOK_ENGINE),
    ),
]

E3_LEDGER = [
    _trade("trd_e3_01", "DEEPAKNTR", Direction.long, date(2026, 2, 10), date(2026, 2, 11),
           2118.40, date(2026, 2, 16), 1974.05, "stop", -7.12, -6.82, 0.44, -7.31),
    _trade("trd_e3_02", "TATAELXSI", Direction.long, date(2026, 4, 22), date(2026, 4, 23),
           6402.00, date(2026, 4, 28), 6088.50, "stop", -5.19, -4.90, 1.02, -5.44),
    _trade("trd_e3_03", "PIIND", Direction.long, date(2026, 6, 3), date(2026, 6, 4),
           3745.10, date(2026, 6, 9), 3612.75, "stop", -3.83, -3.53, 0.60, -4.01),
    _trade("trd_e3_04", "AUBANK", Direction.long, date(2026, 7, 15), date(2026, 7, 16),
           721.35, date(2026, 7, 21), 706.90, "horizon", -2.30, -2.00, 0.71, -2.88),
    _trade("trd_e3_05", "COFORGE", Direction.long, date(2026, 8, 26), date(2026, 8, 27),
           8914.00, date(2026, 9, 1), 8871.30, "horizon", -0.78, -0.48, 0.92, -1.66),
    _trade("trd_e3_06", "CUMMINSIND", Direction.long, date(2026, 3, 11), date(2026, 3, 12),
           3488.55, date(2026, 3, 17), 3529.20, "horizon", 0.87, 1.17, 1.55, -0.62),
    _trade("trd_e3_07", "PERSISTENT", Direction.long, date(2026, 5, 6), date(2026, 5, 7),
           5760.25, date(2026, 5, 12), 5885.40, "trail", 1.87, 2.17, 2.64, -0.41),
    _trade("trd_e3_08", "POLYCAB", Direction.long, date(2026, 1, 21), date(2026, 1, 22),
           6640.00, date(2026, 1, 27), 6885.75, "trail", 3.40, 3.70, 4.12, -0.55),
]

E3_CHANGE_LOG = [
    ChangeLogEntry(
        seq=1, level=LearningLevel.L1, at=datetime(2026, 3, 6, 18, 40, tzinfo=IST),
        what_changed="Appended the first thirty forward observations to the evidence base.",
        why="The observation-threshold trigger fired at thirty new closed virtual trades.",
        evidence_refs=["evd_e3_forward"],
        previous_version=None, new_version="evidence@2026-03-06",
        improved=None, decided_by=Author.engine, constitution_version=CONSTITUTION,
    ),
    ChangeLogEntry(
        seq=2, level=LearningLevel.L2, at=datetime(2026, 4, 2, 18, 40, tzinfo=IST),
        what_changed=(
            "Tightened the volume-dry-up threshold from 0.90x to 0.75x of the "
            "twenty-session median volume."
        ),
        why=(
            "The volume split showed the edge concentrated in the driest bucket; "
            "0.75x sits inside the Constitution-approved range of 0.60x to 1.00x."
        ),
        evidence_refs=["evd_e3_volsplit"],
        previous_version="strategy@v2.0", new_version="strategy@v2.1",
        performance_before=None, performance_after=None, improved=None,
        decided_by=Author.llm, constitution_version=CONSTITUTION,
    ),
    ChangeLogEntry(
        seq=3, level=LearningLevel.L3, at=datetime(2026, 5, 19, 18, 40, tzinfo=IST),
        what_changed=(
            "Created strategy version three: added an index-regime filter that skips "
            "signals on days the breadth composite is in its lowest decile."
        ),
        why=(
            "Nine of the eleven worst forward trades landed on lowest-decile breadth days. "
            "Removing them is a rule change, not a parameter tweak, so it needs a version."
        ),
        evidence_refs=["evd_e3_replay", "evd_e3_forward"],
        previous_version="strategy@v2.1", new_version="strategy@v3.0",
        performance_before=None, performance_after=E3_VIRT, improved=True,
        decided_by=Author.llm, approved_by="founder",
        constitution_version=CONSTITUTION,
        validation=(
            "Backtested over the discovery window (2018-01-01 to 2025-12-31) AND "
            "forward-validated on the untouched 2026 virtual book for eight weeks "
            "before replacing v2.1. Both legs beat the incumbent net of costs and at "
            "2x slippage; the replacement was then approved by the founder."
        ),
    ),
]

EXP_0003 = ExperimentDetail(
    id="exp_0003",
    hypothesis=(
        "In an established uptrend, a 3-session pullback that arrives on volume below "
        "0.75x its 20-session median is followed by a bounce more often than the same "
        "pullback on rising volume."
    ),
    question="Does *who is selling* matter more than *how far it fell*?",
    rationale=(
        "The observer kept flagging pullbacks with near-identical price geometry and "
        "opposite outcomes. Price shape alone could not separate them, so the agent asked "
        "what else differed. Participation was the first candidate."
    ),
    status=ExperimentStatus.promoted,
    stage=LoopStage.track,
    opened_at=datetime(2025, 11, 18, 18, 40, tzinfo=IST),
    strategy_version="strategy@v3.0",
    constitution_version=CONSTITUTION,
    historical_return=E3_HIST,
    virtual_return=E3_VIRT,
    occurrences=98,
    n=61,
    spark=_spark([0.0, 1.4, 2.2, 0.9, 2.6, 3.8, 3.1, 5.5, 6.9, 5.2, 4.4, 4.1], FORWARD),
    as_of=AS_OF_TS,
    rulebook=Rulebook(
        universe=UNIVERSE_PIT,
        direction=Direction.long,
        entry=(
            "Close above the 50-session average AND 3 consecutive lower closes AND "
            "pullback volume < 0.75x the 20-session median AND breadth composite not in "
            "its lowest decile. Fill at the NEXT session open."
        ),
        invalidation=(
            "A close below the low of the pullback's last session, or the 50-session "
            "average being lost. There is no price target — the idea is either still "
            "valid or it is not."
        ),
        exit="Hard stop at -4% from entry; trail the 10-session low after +2%; hard horizon exit at 5 sessions.",
        horizon_sessions=5,
        sizing="Equal-weight, 2% of virtual capital per idea, maximum 8 concurrent.",
        cost_convention=COSTS_V3,
    ),
    facts=E3_FACTS,
    evidence=E3_EVIDENCE,
    virtual_book=VirtualBook(
        metrics=E3_VIRT, capital_inr=CAPITAL_INR, ledger_losers_first=E3_LEDGER,
    ),
    change_log=E3_CHANGE_LOG,
    triggers=[
        Trigger(
            id="trg_e3_obs30", type=TriggerType.observation_threshold,
            description="The virtual book reached thirty new closed observations since the last review.",
            fired_at=datetime(2026, 3, 6, 18, 35, tzinfo=IST),
            fact_refs=["fct_e3_virt_n"],
        ),
    ],
    next_review=Trigger(
        id="trg_e3_next", type=TriggerType.performance_deviation,
        description=(
            "Wake the reviewer if forward expectancy falls below the 2x-slippage line, "
            "or if the current drawdown exceeds the historical worst."
        ),
        fired_at=AS_OF_TS, fact_refs=["fct_e3_virt_cur_dd", "fct_e3_hist_dd"],
    ),
    story=[
        _line(StoryBeat.noticed,
              "Two pullbacks that looked identical did the opposite thing",
              "The observer flagged a cluster of uptrend pullbacks with near-identical "
              "price geometry and opposite outcomes. Price shape alone could not tell "
              "them apart, so something outside price had to be doing the work.",
              author=Author.llm, model=MODEL_REASON, prompt_version="observe@3",
              at=datetime(2025, 11, 18, 18, 41, tzinfo=IST)),
        _line(StoryBeat.hypothesis,
              "I am testing whether a volume collapse is what separates them",
              "If the sellers have simply gone quiet rather than turned aggressive, the "
              "pullback should resolve upward more often. The split on the discovery "
              "window is stark: {{fact:fct_e3_dryvol_exp}} on dry volume against "
              "{{fact:fct_e3_wetvol_exp}} on rising volume.",
              author=Author.llm, model=MODEL_REASON, prompt_version="hypothesize@4",
              at=datetime(2025, 11, 18, 18, 42, tzinfo=IST),
              refs=["fct_e3_dryvol_exp", "fct_e3_wetvol_exp"]),
        _line(StoryBeat.experiment,
              "Virtual capital, next-open fills, one rule, no discretion",
              "Every qualifying signal is filled at the next session open, stopped at a "
              "fixed distance, trailed on the ten-session low, and force-exited at the "
              "horizon. Costs and slippage are charged on every simulated trade.",
              author=Author.engine, at=datetime(2025, 11, 18, 18, 43, tzinfo=IST)),
        _line(StoryBeat.outcome,
              "Forward is real but roughly a third of what history promised",
              "The historical replay expects {{fact:fct_e3_hist_exp}} per trade. The "
              "forward virtual book has delivered {{fact:fct_e3_virt_exp}} across "
              "{{fact:fct_e3_virt_n}} closed trades, and it is sitting "
              "{{fact:fct_e3_virt_cur_dd}} below its own peak right now. At doubled "
              "slippage the forward edge thins to {{fact:fct_e3_virt_exp_2x}}.",
              author=Author.llm, model=MODEL_NARRATE, prompt_version="narrate@5",
              refs=["fct_e3_hist_exp", "fct_e3_virt_exp", "fct_e3_virt_n",
                    "fct_e3_virt_cur_dd", "fct_e3_virt_exp_2x"]),
        _line(StoryBeat.learning,
              "I removed the worst breadth days and versioned the strategy",
              "Most of the deepest forward losses landed on the weakest breadth days. "
              "Skipping those is a rule change, not a knob, so it became a new strategy "
              "version — backtested and then forward-validated before it was allowed to "
              "replace the old one. The measured forward improvement was "
              "{{fact:fct_e3_v2_gain}} per trade.",
              author=Author.llm, model=MODEL_REASON, prompt_version="reflect@4",
              refs=["fct_e3_v2_gain"]),
        _line(StoryBeat.next,
              "Next I want to know whether the edge is really a sector effect",
              "The winners cluster in a small number of sectors. Before trusting the "
              "rule further I want a sector-neutral replay: if the edge survives with "
              "sector effects removed, it is a behavioural edge; if it does not, I have "
              "been renting a sector tailwind and calling it a signal.",
              author=Author.llm, model=MODEL_REASON, prompt_version="decide_next@3"),
    ],
    llm_usage=LlmUsage(
        window="experiment_lifetime",
        by_model=[
            ModelUsage(model=MODEL_REASON, calls=34, input_tokens=486_200,
                       output_tokens=41_800, cache_read_input_tokens=402_500,
                       cache_creation_input_tokens=52_100, cost_usd=0.72),
            ModelUsage(model=MODEL_NARRATE, calls=61, input_tokens=298_400,
                       output_tokens=52_900, cache_read_input_tokens=241_000,
                       cache_creation_input_tokens=31_600, cost_usd=0.36),
        ],
        total_cost_usd=1.08, as_of=AS_OF_TS,
    ),
)


# ═════════════════════════════════════════════════════════════════════════════
#  exp_0009 — VALIDATING. History looked fine; forward is nearly flat and the
#  2x-slippage line is already negative. n=27 → flagged.
# ═════════════════════════════════════════════════════════════════════════════

E9_HIST = _hist(expectancy=0.22, expectancy_2x=0.09, total_return=None,
                max_dd=13.6, cur_dd=0.0, n=142, occurrences=318,
                win_rate=52.1, avg_win=1.11, avg_loss=-0.75)
E9_VIRT = _virt(expectancy=0.05, expectancy_2x=-0.04, total_return=0.6,
                max_dd=6.2, cur_dd=6.2, n=27, occurrences=44,
                win_rate=48.1, avg_win=0.94, avg_loss=-0.77)

E9_FACTS = [
    _fact("fct_e9_hist_exp", "expectancy per trade, historical replay", 0.22, Unit.pct_per_trade, 142),
    _fact("fct_e9_hist_dd", "worst drawdown, historical replay", 13.6, Unit.pct, 142),
    _fact("fct_e9_virt_exp", "expectancy per trade, forward virtual book", 0.05, Unit.pct_per_trade, 27, FORWARD),
    _fact("fct_e9_virt_exp_2x", "expectancy per trade at 2x slippage, forward", -0.04, Unit.pct_per_trade, 27, FORWARD),
    _fact("fct_e9_virt_n", "closed virtual trades so far", 27, Unit.count, 27, FORWARD),
    _fact("fct_e9_needed_n", "closed trades required before this experiment is judged",
          50, Unit.count, 27, FORWARD, note="Constitution gate: no promotion below fifty closed trades."),
]

E9_EVIDENCE = [
    Evidence(id="evd_e9_replay", kind=EvidenceKind.historical_replay,
             title="Strategy-replay over the discovery window", label=SIMULATED_LABEL,
             performance=E9_HIST, fact_refs=["fct_e9_hist_exp", "fct_e9_hist_dd"],
             provenance=_prov(DISCOVERY, REPLAY_ENGINE)),
    Evidence(id="evd_e9_forward", kind=EvidenceKind.forward_virtual,
             title="Forward virtual book", label=VIRTUAL_LABEL, performance=E9_VIRT,
             fact_refs=["fct_e9_virt_exp", "fct_e9_virt_exp_2x", "fct_e9_virt_n"],
             provenance=_prov(FORWARD, BOOK_ENGINE)),
    Evidence(id="evd_e9_costs", kind=EvidenceKind.cost_sensitivity,
             title="Doubled-slippage sensitivity run", label=VIRTUAL_LABEL, performance=None,
             fact_refs=["fct_e9_virt_exp", "fct_e9_virt_exp_2x"],
             provenance=_prov(FORWARD, BOOK_ENGINE),
             interpretation=_line(
                 StoryBeat.outcome,
                 "The forward edge does not survive doubled slippage",
                 "Forward expectancy of {{fact:fct_e9_virt_exp}} falls to "
                 "{{fact:fct_e9_virt_exp_2x}} when slippage is doubled. An edge that "
                 "only exists at the friendly cost assumption is a cost artefact, not "
                 "an edge — but the sample is still too small to conclude anything.",
                 author=Author.llm, model=MODEL_REASON, prompt_version="interpret@2",
                 refs=["fct_e9_virt_exp", "fct_e9_virt_exp_2x"])),
]

E9_LEDGER = [
    _trade("trd_e9_01", "HINDCOPPER", Direction.short, date(2026, 3, 4), date(2026, 3, 5),
           412.60, date(2026, 3, 6), 431.15, "stop", -4.80, -4.50, 0.35, -4.92),
    _trade("trd_e9_02", "IEX", Direction.short, date(2026, 6, 17), date(2026, 6, 18),
           228.35, date(2026, 6, 19), 235.90, "stop", -3.61, -3.31, 0.22, -3.74),
    _trade("trd_e9_03", "NAUKRI", Direction.short, date(2026, 8, 5), date(2026, 8, 6),
           1780.00, date(2026, 8, 7), 1802.40, "horizon", -1.56, -1.26, 0.48, -1.71),
    _trade("trd_e9_04", "BSE", Direction.short, date(2026, 2, 19), date(2026, 2, 20),
           2965.20, date(2026, 2, 23), 2951.05, "horizon", 0.18, 0.48, 0.94, -0.72),
    _trade("trd_e9_05", "MAZDOCK", Direction.short, date(2026, 7, 8), date(2026, 7, 9),
           3210.75, date(2026, 7, 10), 3155.90, "horizon", 1.41, 1.71, 2.05, -0.31),
]

EXP_0009 = ExperimentDetail(
    id="exp_0009",
    hypothesis=(
        "A close in the top decile of the 20-session range on below-median volume "
        "mean-reverts over the following 2 sessions."
    ),
    question="Is a high close nobody participated in an exhaustion signal?",
    rationale=(
        "Strength without participation has shown up repeatedly in the observer's "
        "unusual-condition log. This tests whether it is tradeable rather than merely visible."
    ),
    status=ExperimentStatus.validating,
    stage=LoopStage.track,
    opened_at=datetime(2026, 4, 14, 18, 40, tzinfo=IST),
    strategy_version="strategy@v1.0",
    constitution_version=CONSTITUTION,
    historical_return=E9_HIST,
    virtual_return=E9_VIRT,
    occurrences=44, n=27,
    spark=_spark([0.0, 0.9, 1.6, 0.4, -0.8, 0.3, 1.1, 0.6, 0.9, 0.6], FORWARD),
    as_of=AS_OF_TS,
    rulebook=Rulebook(
        universe=UNIVERSE_PIT, direction=Direction.short,
        entry=("Close in the top decile of the 20-session range AND session volume below "
               "the 20-session median. Fill at the NEXT session open."),
        invalidation="A close above the signal session's high. No price target is set.",
        exit="Hard stop at +3% against the position; horizon exit at 2 sessions.",
        horizon_sessions=2,
        sizing="Equal-weight, 1.5% of virtual capital per idea, maximum 6 concurrent.",
        cost_convention=COSTS_V3,
    ),
    facts=E9_FACTS, evidence=E9_EVIDENCE,
    virtual_book=VirtualBook(metrics=E9_VIRT, capital_inr=CAPITAL_INR,
                             ledger_losers_first=E9_LEDGER),
    change_log=[
        ChangeLogEntry(
            seq=1, level=LearningLevel.L1, at=datetime(2026, 7, 30, 18, 40, tzinfo=IST),
            what_changed="Appended the first twenty-five forward observations.",
            why="Routine evidence accumulation; no rule was touched.",
            evidence_refs=["evd_e9_forward"], previous_version=None,
            new_version="evidence@2026-07-30", improved=None,
            decided_by=Author.engine, constitution_version=CONSTITUTION,
        ),
    ],
    triggers=[
        Trigger(id="trg_e9_dev", type=TriggerType.performance_deviation,
                description="Forward expectancy fell materially below the historical replay.",
                fired_at=datetime(2026, 8, 21, 18, 35, tzinfo=IST),
                fact_refs=["fct_e9_hist_exp", "fct_e9_virt_exp"]),
    ],
    next_review=Trigger(
        id="trg_e9_next", type=TriggerType.observation_threshold,
        description="Judge this experiment when the closed-trade count reaches the Constitution's minimum.",
        fired_at=AS_OF_TS, fact_refs=["fct_e9_virt_n", "fct_e9_needed_n"],
    ),
    story=[
        _line(StoryBeat.noticed, "Strength that nobody showed up for",
              "The observer keeps logging sessions that close near the top of their "
              "recent range on unusually thin volume. That combination is odd enough to "
              "be worth a question.",
              author=Author.llm, model=MODEL_REASON, prompt_version="observe@3",
              at=datetime(2026, 4, 14, 18, 41, tzinfo=IST)),
        _line(StoryBeat.hypothesis, "I am testing whether thin strength fades",
              "If a high close is not backed by participation, the move may be the last "
              "of the buyers rather than the first. The historical replay is mildly "
              "supportive at {{fact:fct_e9_hist_exp}}, with a worst drawdown of "
              "{{fact:fct_e9_hist_dd}}.",
              author=Author.llm, model=MODEL_REASON, prompt_version="hypothesize@4",
              at=datetime(2026, 4, 14, 18, 42, tzinfo=IST),
              refs=["fct_e9_hist_exp", "fct_e9_hist_dd"]),
        _line(StoryBeat.experiment, "Short side, tight horizon, virtual capital",
              "Each signal is filled at the next open, stopped on a close above the "
              "signal session's high, and force-exited at the horizon regardless.",
              author=Author.engine, at=datetime(2026, 4, 14, 18, 43, tzinfo=IST)),
        _line(StoryBeat.outcome, "Forward is close to nothing, and negative at doubled slippage",
              "Across {{fact:fct_e9_virt_n}} closed virtual trades the forward "
              "expectancy is {{fact:fct_e9_virt_exp}}, which becomes "
              "{{fact:fct_e9_virt_exp_2x}} once slippage is doubled. The book is at its "
              "own worst drawdown right now.",
              author=Author.llm, model=MODEL_NARRATE, prompt_version="narrate@5",
              refs=["fct_e9_virt_n", "fct_e9_virt_exp", "fct_e9_virt_exp_2x"]),
        _line(StoryBeat.learning, "I am not changing anything yet, and that is the point",
              "The sample is below the Constitution's minimum of "
              "{{fact:fct_e9_needed_n}} closed trades, so any change I made now would be "
              "fitting noise. The honest action is to keep collecting and to say out loud "
              "that this one is not working so far.",
              author=Author.llm, model=MODEL_REASON, prompt_version="reflect@4",
              refs=["fct_e9_needed_n"]),
        _line(StoryBeat.next, "If it fails, I want to know whether the shape was ever the point",
              "Should this die, the follow-up is whether thin strength predicts a "
              "widening of the spread rather than a direction — a volatility question "
              "wearing a direction costume.",
              author=Author.llm, model=MODEL_REASON, prompt_version="decide_next@3"),
    ],
    llm_usage=LlmUsage(
        window="experiment_lifetime",
        by_model=[
            ModelUsage(model=MODEL_REASON, calls=11, input_tokens=158_900,
                       output_tokens=13_200, cache_read_input_tokens=131_400,
                       cache_creation_input_tokens=18_700, cost_usd=0.24),
            ModelUsage(model=MODEL_NARRATE, calls=19, input_tokens=92_300,
                       output_tokens=16_100, cache_read_input_tokens=74_800,
                       cache_creation_input_tokens=9_900, cost_usd=0.11),
        ],
        total_cost_usd=0.35, as_of=AS_OF_TS,
    ),
)


# ═════════════════════════════════════════════════════════════════════════════
#  exp_0011 — PROMISING. The best-looking live one, and still only n=34.
# ═════════════════════════════════════════════════════════════════════════════

E11_HIST = _hist(expectancy=0.27, expectancy_2x=0.14, total_return=None,
                 max_dd=9.8, cur_dd=0.0, n=96, occurrences=241,
                 win_rate=51.0, avg_win=1.34, avg_loss=-0.84)
E11_VIRT = _virt(expectancy=0.21, expectancy_2x=0.08, total_return=2.4,
                 max_dd=4.6, cur_dd=1.9, n=34, occurrences=52,
                 win_rate=50.0, avg_win=1.19, avg_loss=-0.77)

E11_FACTS = [
    _fact("fct_e11_hist_exp", "expectancy per trade, historical replay", 0.27, Unit.pct_per_trade, 96),
    _fact("fct_e11_virt_exp", "expectancy per trade, forward virtual book", 0.21, Unit.pct_per_trade, 34, FORWARD),
    _fact("fct_e11_virt_exp_2x", "expectancy per trade at 2x slippage, forward", 0.08, Unit.pct_per_trade, 34, FORWARD),
    _fact("fct_e11_virt_dd", "worst drawdown, forward virtual book", 4.6, Unit.pct, 34, FORWARD),
    _fact("fct_e11_virt_n", "closed virtual trades so far", 34, Unit.count, 34, FORWARD),
    _fact("fct_e11_breadth", "index sessions in the discovery window that met the down-day condition",
          188, Unit.count, 96),
]

E11_EVIDENCE = [
    Evidence(id="evd_e11_replay", kind=EvidenceKind.historical_replay,
             title="Strategy-replay over the discovery window", label=SIMULATED_LABEL,
             performance=E11_HIST, fact_refs=["fct_e11_hist_exp", "fct_e11_breadth"],
             provenance=_prov(DISCOVERY, REPLAY_ENGINE)),
    Evidence(id="evd_e11_forward", kind=EvidenceKind.forward_virtual,
             title="Forward virtual book", label=VIRTUAL_LABEL, performance=E11_VIRT,
             fact_refs=["fct_e11_virt_exp", "fct_e11_virt_exp_2x", "fct_e11_virt_dd",
                        "fct_e11_virt_n"],
             provenance=_prov(FORWARD, BOOK_ENGINE)),
]

E11_LEDGER = [
    _trade("trd_e11_01", "GRANULES", Direction.long, date(2026, 5, 12), date(2026, 5, 13),
           612.40, date(2026, 5, 18), 587.05, "stop", -4.44, -4.14, 0.30, -4.61),
    _trade("trd_e11_02", "KEI", Direction.long, date(2026, 7, 2), date(2026, 7, 3),
           4180.00, date(2026, 7, 8), 4062.55, "stop", -3.11, -2.81, 0.63, -3.28),
    _trade("trd_e11_03", "APLAPOLLO", Direction.long, date(2026, 8, 12), date(2026, 8, 13),
           1655.30, date(2026, 8, 18), 1638.20, "horizon", -1.33, -1.03, 0.55, -1.62),
    _trade("trd_e11_04", "SUPREMEIND", Direction.long, date(2026, 3, 25), date(2026, 3, 26),
           5240.00, date(2026, 3, 31), 5266.40, "horizon", 0.20, 0.50, 1.24, -0.88),
    _trade("trd_e11_05", "ASTRAL", Direction.long, date(2026, 6, 24), date(2026, 6, 25),
           1490.75, date(2026, 6, 30), 1521.10, "trail", 1.74, 2.04, 2.36, -0.44),
    _trade("trd_e11_06", "DIXON", Direction.long, date(2026, 2, 4), date(2026, 2, 5),
           18420.00, date(2026, 2, 10), 19038.60, "trail", 3.06, 3.36, 3.80, -0.51),
]

EXP_0011 = ExperimentDetail(
    id="exp_0011",
    hypothesis=(
        "After an index-wide down day, stocks that did NOT make a new 10-session low "
        "outperform the index over the next 3 sessions."
    ),
    question="Does refusing to break down on a bad day mean anything?",
    rationale=(
        "A relative-strength question the observer can pose cheaply every single "
        "down day, which makes it a good candidate for continuous evidence accumulation."
    ),
    status=ExperimentStatus.promising,
    stage=LoopStage.review,
    opened_at=datetime(2026, 2, 2, 18, 40, tzinfo=IST),
    strategy_version="strategy@v1.2",
    constitution_version=CONSTITUTION,
    historical_return=E11_HIST, virtual_return=E11_VIRT,
    occurrences=52, n=34,
    spark=_spark([0.0, 0.7, 1.9, 1.1, 2.8, 3.4, 2.2, 3.1, 2.4], FORWARD),
    as_of=AS_OF_TS,
    rulebook=Rulebook(
        universe=UNIVERSE_PIT, direction=Direction.long,
        entry=("Index closes down more than 1% AND the stock did not print a new "
               "10-session low that session. Fill at the NEXT session open."),
        invalidation="A subsequent close below the 10-session low. No price target is set.",
        exit="Hard stop at -3.5%; horizon exit at 3 sessions.",
        horizon_sessions=3,
        sizing="Equal-weight, 2% of virtual capital per idea, maximum 10 concurrent.",
        cost_convention=COSTS_V3,
    ),
    facts=E11_FACTS, evidence=E11_EVIDENCE,
    virtual_book=VirtualBook(metrics=E11_VIRT, capital_inr=CAPITAL_INR,
                             ledger_losers_first=E11_LEDGER),
    change_log=[
        ChangeLogEntry(
            seq=1, level=LearningLevel.L1, at=datetime(2026, 6, 11, 18, 40, tzinfo=IST),
            what_changed="Appended thirty forward observations to the evidence base.",
            why="Observation-threshold trigger.", evidence_refs=["evd_e11_forward"],
            previous_version=None, new_version="evidence@2026-06-11", improved=None,
            decided_by=Author.engine, constitution_version=CONSTITUTION),
        ChangeLogEntry(
            seq=2, level=LearningLevel.L2, at=datetime(2026, 8, 4, 18, 40, tzinfo=IST),
            what_changed="Widened the index down-day threshold from 1.5% to 1.0%.",
            why=("Too few qualifying days at the tighter threshold; the wider one stays "
                 "inside the approved range of 0.75% to 2.00% and roughly doubles the sample."),
            evidence_refs=["evd_e11_replay"],
            previous_version="strategy@v1.1", new_version="strategy@v1.2",
            improved=None, decided_by=Author.llm, constitution_version=CONSTITUTION),
    ],
    triggers=[
        Trigger(id="trg_e11_obs", type=TriggerType.observation_threshold,
                description="Reached thirty new closed observations since the last review.",
                fired_at=datetime(2026, 6, 11, 18, 35, tzinfo=IST),
                fact_refs=["fct_e11_virt_n"]),
    ],
    next_review=Trigger(
        id="trg_e11_next", type=TriggerType.observation_threshold,
        description="Review again after the next thirty closed observations.",
        fired_at=AS_OF_TS, fact_refs=["fct_e11_virt_n"],
    ),
    story=[
        _line(StoryBeat.noticed, "On bad days, some names simply refuse to break",
              "On broad down days a consistent minority of names decline to print a new "
              "short-term low. The observer can see this cheaply, every time it happens.",
              author=Author.llm, model=MODEL_REASON, prompt_version="observe@3",
              at=datetime(2026, 2, 2, 18, 41, tzinfo=IST)),
        _line(StoryBeat.hypothesis, "I am testing whether that refusal is information",
              "The discovery window supports it mildly, at {{fact:fct_e11_hist_exp}} per "
              "trade across enough qualifying index sessions to be worth forward testing.",
              author=Author.llm, model=MODEL_REASON, prompt_version="hypothesize@4",
              at=datetime(2026, 2, 2, 18, 42, tzinfo=IST),
              refs=["fct_e11_hist_exp"]),
        _line(StoryBeat.experiment, "Long side, three-session horizon, virtual capital",
              "Signals are filled at the next open, stopped at a fixed distance, and "
              "force-exited at the horizon.",
              author=Author.engine, at=datetime(2026, 2, 2, 18, 43, tzinfo=IST)),
        _line(StoryBeat.outcome, "Forward is holding up, on a sample too small to trust",
              "Forward expectancy is {{fact:fct_e11_virt_exp}} across "
              "{{fact:fct_e11_virt_n}} closed trades, with a worst drawdown of "
              "{{fact:fct_e11_virt_dd}}. At doubled slippage it thins to "
              "{{fact:fct_e11_virt_exp_2x}} — still positive, which is the part that "
              "makes this one interesting rather than merely encouraging.",
              author=Author.llm, model=MODEL_NARRATE, prompt_version="narrate@5",
              refs=["fct_e11_virt_exp", "fct_e11_virt_n", "fct_e11_virt_dd",
                    "fct_e11_virt_exp_2x"]),
        _line(StoryBeat.learning, "I widened the trigger to earn a sample, not a result",
              "The original down-day threshold produced too few opportunities to learn "
              "from. Widening it inside the approved range was a parameter change, not a "
              "new idea, so it did not need a new strategy version.",
              author=Author.llm, model=MODEL_REASON, prompt_version="reflect@4"),
        _line(StoryBeat.next, "Next: is this just the low-volatility names?",
              "Refusing to make a new low may simply be what a quiet stock does on a "
              "loud day. The next test neutralises volatility and asks whether anything "
              "is left.",
              author=Author.llm, model=MODEL_REASON, prompt_version="decide_next@3"),
    ],
    llm_usage=LlmUsage(
        window="experiment_lifetime",
        by_model=[
            ModelUsage(model=MODEL_REASON, calls=16, input_tokens=214_500,
                       output_tokens=19_400, cache_read_input_tokens=178_200,
                       cache_creation_input_tokens=24_100, cost_usd=0.31),
            ModelUsage(model=MODEL_NARRATE, calls=27, input_tokens=131_700,
                       output_tokens=22_800, cache_read_input_tokens=107_900,
                       cache_creation_input_tokens=13_400, cost_usd=0.15),
        ],
        total_cost_usd=0.46, as_of=AS_OF_TS,
    ),
)


# ═════════════════════════════════════════════════════════════════════════════
#  exp_0014 — TESTING. n=11 → GREYED. Historical already fails the 2x gate and
#  forward is negative. This is what most experiments actually look like.
# ═════════════════════════════════════════════════════════════════════════════

E14_HIST = _hist(expectancy=0.12, expectancy_2x=-0.01, total_return=None,
                 max_dd=15.2, cur_dd=0.0, n=58, occurrences=131,
                 win_rate=44.8, avg_win=1.28, avg_loss=-0.82)
E14_VIRT = _virt(expectancy=-0.06, expectancy_2x=-0.22, total_return=-0.7,
                 max_dd=3.1, cur_dd=3.1, n=11, occurrences=18,
                 win_rate=36.4, avg_win=1.02, avg_loss=-0.68,
                 window=DateRange(start=date(2026, 6, 1), end=AS_OF))

E14_FACTS = [
    _fact("fct_e14_hist_exp", "expectancy per trade, historical replay", 0.12, Unit.pct_per_trade, 58),
    _fact("fct_e14_hist_exp_2x", "expectancy per trade at 2x slippage, historical replay",
          -0.01, Unit.pct_per_trade, 58),
    _fact("fct_e14_hist_dd", "worst drawdown, historical replay", 15.2, Unit.pct, 58),
    _fact("fct_e14_virt_exp", "expectancy per trade, forward virtual book", -0.06,
          Unit.pct_per_trade, 11, DateRange(start=date(2026, 6, 1), end=AS_OF)),
    _fact("fct_e14_virt_n", "closed virtual trades so far", 11, Unit.count, 11,
          DateRange(start=date(2026, 6, 1), end=AS_OF),
          note="Below the greying threshold — treat as anecdote, not evidence."),
    _fact("fct_e14_min_n", "closed trades required before this can be judged", 50, Unit.count, 11,
          DateRange(start=date(2026, 6, 1), end=AS_OF)),
]

E14_EVIDENCE = [
    Evidence(id="evd_e14_replay", kind=EvidenceKind.historical_replay,
             title="Strategy-replay over the discovery window", label=SIMULATED_LABEL,
             performance=E14_HIST,
             fact_refs=["fct_e14_hist_exp", "fct_e14_hist_exp_2x", "fct_e14_hist_dd"],
             provenance=_prov(DISCOVERY, REPLAY_ENGINE)),
    Evidence(id="evd_e14_forward", kind=EvidenceKind.forward_virtual,
             title="Forward virtual book (very small sample)", label=VIRTUAL_LABEL,
             performance=E14_VIRT, fact_refs=["fct_e14_virt_exp", "fct_e14_virt_n"],
             provenance=_prov(DateRange(start=date(2026, 6, 1), end=AS_OF), BOOK_ENGINE)),
]

E14_LEDGER = [
    _trade("trd_e14_01", "IDFCFIRSTB", Direction.long, date(2026, 6, 9), date(2026, 6, 10),
           88.35, date(2026, 6, 11), 85.60, "stop", -3.41, -3.11, 0.18, -3.55),
    _trade("trd_e14_02", "ZOMATO", Direction.long, date(2026, 7, 23), date(2026, 7, 24),
           341.20, date(2026, 7, 25), 332.85, "stop", -2.75, -2.45, 0.24, -2.90),
    _trade("trd_e14_03", "BANDHANBNK", Direction.long, date(2026, 8, 19), date(2026, 8, 20),
           176.90, date(2026, 8, 21), 175.05, "horizon", -1.35, -1.05, 0.42, -1.58),
    _trade("trd_e14_04", "TRENT", Direction.long, date(2026, 9, 1), date(2026, 9, 2),
           7420.00, date(2026, 9, 3), 7466.10, "horizon", 0.32, 0.62, 1.05, -0.66),
    _trade("trd_e14_05", "LTIM", Direction.long, date(2026, 6, 30), date(2026, 7, 1),
           6015.50, date(2026, 7, 2), 6111.75, "horizon", 1.30, 1.60, 1.88, -0.29),
]

EXP_0014 = ExperimentDetail(
    id="exp_0014",
    hypothesis=(
        "A gap-down into a prior weekly demand zone that fills more than half the gap "
        "by 10:15 IST closes green on the day."
    ),
    question="Does an intraday repair of a gap predict the close?",
    rationale=(
        "The founder's observation list contains a version of this. It is being tested "
        "because it was asked for, not because the evidence looked promising."
    ),
    status=ExperimentStatus.testing,
    stage=LoopStage.track,
    opened_at=datetime(2026, 5, 26, 18, 40, tzinfo=IST),
    strategy_version="strategy@v1.0",
    constitution_version=CONSTITUTION,
    historical_return=E14_HIST, virtual_return=E14_VIRT,
    occurrences=18, n=11,
    spark=_spark([0.0, -0.4, 0.3, -0.9, -0.2, -0.7], DateRange(start=date(2026, 6, 1), end=AS_OF)),
    as_of=AS_OF_TS,
    rulebook=Rulebook(
        universe=UNIVERSE_PIT, direction=Direction.long,
        entry=("Open gaps down into a prior weekly demand zone AND more than half the gap "
               "is retraced by 10:15 IST. Fill at the 10:15 bar open."),
        invalidation="A print below the session low made before 10:15. No price target is set.",
        exit="Hard stop at the session low; exit at 15:20 IST the same session.",
        horizon_sessions=1,
        sizing="Equal-weight, 1% of virtual capital per idea, maximum 4 concurrent.",
        cost_convention=COSTS_V3,
    ),
    facts=E14_FACTS, evidence=E14_EVIDENCE,
    virtual_book=VirtualBook(metrics=E14_VIRT, capital_inr=CAPITAL_INR,
                             ledger_losers_first=E14_LEDGER),
    change_log=[],
    triggers=[
        Trigger(id="trg_e14_req", type=TriggerType.human_request,
                description="Queued from the founder's observation list rather than by the observer.",
                fired_at=datetime(2026, 5, 26, 18, 30, tzinfo=IST)),
    ],
    next_review=Trigger(
        id="trg_e14_next", type=TriggerType.observation_threshold,
        description="Review after thirty closed observations, or sooner if the drawdown breaches its limit.",
        fired_at=AS_OF_TS, fact_refs=["fct_e14_virt_n", "fct_e14_min_n"],
    ),
    story=[
        _line(StoryBeat.noticed, "A gap that gets repaired feels different from one that does not",
              "This one did not come from the observer. It came from the founder's list, "
              "and it is being tested for that reason alone.",
              author=Author.llm, model=MODEL_REASON, prompt_version="observe@3",
              at=datetime(2026, 5, 26, 18, 41, tzinfo=IST)),
        _line(StoryBeat.hypothesis, "I am testing it even though the history is thin",
              "The historical replay expectancy is {{fact:fct_e14_hist_exp}} per trade, "
              "which becomes {{fact:fct_e14_hist_exp_2x}} at doubled slippage — already "
              "negative before the forward test starts. The worst historical drawdown is "
              "{{fact:fct_e14_hist_dd}}.",
              author=Author.llm, model=MODEL_REASON, prompt_version="hypothesize@4",
              at=datetime(2026, 5, 26, 18, 42, tzinfo=IST),
              refs=["fct_e14_hist_exp", "fct_e14_hist_exp_2x", "fct_e14_hist_dd"]),
        _line(StoryBeat.experiment, "Intraday, one session, smallest allowed size",
              "Entry at the ten-fifteen bar, stop at the session low, exit before the "
              "close. Costs and slippage charged on both legs.",
              author=Author.engine, at=datetime(2026, 5, 26, 18, 43, tzinfo=IST)),
        _line(StoryBeat.outcome, "Negative so far, on a sample far too small to mean anything",
              "Forward expectancy is {{fact:fct_e14_virt_exp}} across only "
              "{{fact:fct_e14_virt_n}} closed trades. That is an anecdote. It is shown "
              "because hiding it until it looks better would be the dishonest choice.",
              author=Author.llm, model=MODEL_NARRATE, prompt_version="narrate@5",
              refs=["fct_e14_virt_exp", "fct_e14_virt_n"]),
        _line(StoryBeat.learning, "Nothing learned yet — and I will not pretend otherwise",
              "No parameter has been touched and no version has been cut. Below "
              "{{fact:fct_e14_min_n}} closed trades any change would be a story told "
              "about noise.",
              author=Author.llm, model=MODEL_REASON, prompt_version="reflect@4",
              refs=["fct_e14_min_n"]),
        _line(StoryBeat.next, "Keep collecting, and check the cost model first if it dies",
              "An intraday rule with a same-session round trip is unusually exposed to "
              "the cost assumption. If this dies, the first thing to re-examine is "
              "whether the assumption was ever realistic, not whether the idea was.",
              author=Author.llm, model=MODEL_REASON, prompt_version="decide_next@3"),
    ],
    llm_usage=LlmUsage(
        window="experiment_lifetime",
        by_model=[
            ModelUsage(model=MODEL_REASON, calls=6, input_tokens=84_100, output_tokens=7_300,
                       cache_read_input_tokens=69_400, cache_creation_input_tokens=11_200,
                       cost_usd=0.13),
            ModelUsage(model=MODEL_NARRATE, calls=8, input_tokens=41_600, output_tokens=7_100,
                       cache_read_input_tokens=33_800, cache_creation_input_tokens=5_200,
                       cost_usd=0.05),
        ],
        total_cost_usd=0.18, as_of=AS_OF_TS,
    ),
)


# ═════════════════════════════════════════════════════════════════════════════
#  exp_0015 — QUEUED. No numbers at all. The schema forbids them.
# ═════════════════════════════════════════════════════════════════════════════

E15_TRIGGER = Trigger(
    id="trg_e15_unusual", type=TriggerType.unusual_market_condition,
    description=(
        "The advance/decline line diverged from the index for five consecutive "
        "sessions — a condition the observer rates as unusual against its own history."
    ),
    fired_at=datetime(2026, 9, 5, 18, 35, tzinfo=IST),
    fact_refs=["fct_e15_divergence_days"],
)

E15_FACTS = [
    _fact("fct_e15_divergence_days",
          "consecutive sessions of advance/decline divergence from the index",
          5, Unit.sessions, None, DateRange(start=date(2026, 8, 31), end=AS_OF), OBSERVER),
    _fact("fct_e15_similar_past",
          "similar past experiments found by associative recall", 2, Unit.count, None,
          DateRange(start=date(2018, 1, 1), end=AS_OF), "memory_recall@0.6.0",
          note="pgvector recall only — a pointer to prior work, never evidence in itself."),
]

EXP_0015 = ExperimentDetail(
    id="exp_0015",
    hypothesis=(
        "A widening advance/decline divergence at the index level changes the base rate "
        "of single-stock breakout follow-through."
    ),
    question="When the index and its breadth disagree, do breakouts still work?",
    rationale=(
        "The observer flagged the divergence; associative recall found two prior "
        "experiments in the neighbourhood, neither of which asked this question. That "
        "novelty check is why a new experiment was justified rather than a re-run."
    ),
    status=ExperimentStatus.queued,
    stage=LoopStage.hypothesize,
    opened_at=datetime(2026, 9, 5, 18, 40, tzinfo=IST),
    strategy_version=None,
    constitution_version=CONSTITUTION,
    historical_return=None, virtual_return=None,
    occurrences=None, n=None, spark=None,
    as_of=AS_OF_TS,
    rulebook=Rulebook(
        universe=UNIVERSE_PIT, direction=Direction.long,
        entry=("PROVISIONAL, not yet run: breakout above a 20-session high, conditioned "
               "on the sign of the index advance/decline divergence. Fill at the NEXT "
               "session open."),
        invalidation="PROVISIONAL: a close back inside the 20-session range.",
        exit="PROVISIONAL: hard stop at -4%; horizon exit at 5 sessions.",
        horizon_sessions=5,
        sizing="Not yet allocated — a queued experiment holds no virtual capital.",
        cost_convention=COSTS_V3,
    ),
    facts=E15_FACTS,
    evidence=[
        Evidence(id="evd_e15_novelty", kind=EvidenceKind.novelty_check,
                 title="Associative recall: have I explored something like this before?",
                 label=SIMULATED_LABEL, performance=None,
                 fact_refs=["fct_e15_similar_past"],
                 provenance=_prov(DateRange(start=date(2018, 1, 1), end=AS_OF),
                                  "memory_recall@0.6.0")),
    ],
    virtual_book=None,
    change_log=[],
    triggers=[E15_TRIGGER],
    next_review=Trigger(
        id="trg_e15_next", type=TriggerType.scheduled_review,
        description="Run the deterministic replay at the next evening research window.",
        fired_at=AS_OF_TS,
    ),
    story=[
        _line(StoryBeat.noticed, "The index and its breadth have been disagreeing",
              "For several consecutive sessions the advance/decline line has pulled away "
              "from the index. The observer rates that as unusual against its own history "
              "— {{fact:fct_e15_divergence_days}} in a row.",
              author=Author.llm, model=MODEL_REASON, prompt_version="observe@3",
              at=datetime(2026, 9, 5, 18, 41, tzinfo=IST),
              refs=["fct_e15_divergence_days"]),
        _line(StoryBeat.hypothesis, "I want to know if breakouts still mean what they meant",
              "Before proposing this I searched my own memory for prior work in the same "
              "neighbourhood and found {{fact:fct_e15_similar_past}}, neither of which "
              "asked this. That is why this is a new question rather than a repeat.",
              author=Author.llm, model=MODEL_REASON, prompt_version="hypothesize@4",
              at=datetime(2026, 9, 5, 18, 42, tzinfo=IST),
              refs=["fct_e15_similar_past"]),
        _line(StoryBeat.experiment, "Nothing has been run yet",
              "This experiment is queued. No replay has been computed, no virtual capital "
              "has been allocated, and no result exists to show.",
              author=Author.engine, at=datetime(2026, 9, 5, 18, 43, tzinfo=IST)),
        _line(StoryBeat.outcome, "There is no outcome, and there should not be one",
              "An experiment that has not run has nothing to report. Anything shown here "
              "would be invented.",
              author=Author.engine, at=datetime(2026, 9, 5, 18, 43, tzinfo=IST)),
        _line(StoryBeat.learning, "Nothing learned",
              "Learning starts after evidence, not before it.",
              author=Author.engine, at=datetime(2026, 9, 5, 18, 43, tzinfo=IST)),
        _line(StoryBeat.next, "The replay runs at the next evening research window",
              "The deterministic replay over the discovery window runs first. Only if it "
              "survives the cost gate does this experiment get virtual capital.",
              author=Author.llm, model=MODEL_REASON, prompt_version="decide_next@3"),
    ],
    llm_usage=LlmUsage(
        window="experiment_lifetime",
        by_model=[
            ModelUsage(model=MODEL_REASON, calls=2, input_tokens=28_400, output_tokens=2_100,
                       cache_read_input_tokens=24_900, cache_creation_input_tokens=3_100,
                       cost_usd=0.04),
        ],
        total_cost_usd=0.04, as_of=AS_OF_TS,
    ),
)


# ═════════════════════════════════════════════════════════════════════════════
#  exp_0007 — DIED, with a published post-mortem. The graveyard is the product.
#  A genuinely good-looking pre-cost edge that costs eat entirely.
# ═════════════════════════════════════════════════════════════════════════════

E7_HIST = _hist(expectancy=0.24, expectancy_2x=-0.03, total_return=None,
                max_dd=18.9, cur_dd=0.0, n=213, occurrences=487,
                win_rate=55.4, avg_win=0.96, avg_loss=-0.66)
E7_VIRT = _virt(expectancy=-0.11, expectancy_2x=-0.29, total_return=-5.3,
                max_dd=12.7, cur_dd=12.7, n=48, occurrences=76,
                win_rate=47.9, avg_win=0.88, avg_loss=-0.98,
                window=DateRange(start=date(2026, 1, 5), end=date(2026, 8, 21)))

E7_WINDOW = DateRange(start=date(2026, 1, 5), end=date(2026, 8, 21))

E7_FACTS = [
    _fact("fct_e7_hist_exp", "expectancy per trade, historical replay, net of costs",
          0.24, Unit.pct_per_trade, 213),
    _fact("fct_e7_hist_exp_2x", "expectancy per trade at 2x slippage, historical replay",
          -0.03, Unit.pct_per_trade, 213),
    _fact("fct_e7_gross_exp", "expectancy per trade BEFORE costs and slippage",
          0.41, Unit.pct_per_trade, 213,
          note="The gap between this and the net number is the entire story."),
    _fact("fct_e7_cost_drag", "average round-trip cost and slippage charged per trade",
          0.17, Unit.pct_per_trade, 213),
    _fact("fct_e7_hist_dd", "worst drawdown, historical replay", 18.9, Unit.pct, 213),
    _fact("fct_e7_virt_exp", "expectancy per trade, forward virtual book",
          -0.11, Unit.pct_per_trade, 48, E7_WINDOW),
    _fact("fct_e7_virt_dd", "worst drawdown, forward virtual book", 12.7, Unit.pct, 48, E7_WINDOW),
    _fact("fct_e7_virt_n", "closed virtual trades before it was killed", 48, Unit.count, 48, E7_WINDOW),
    _fact("fct_e7_spread_widening", "average opening spread on qualifying gap-up sessions "
          "against the same names on ordinary sessions", 2.3, Unit.x, 213,
          note="The survivor of this experiment: the gap is a liquidity event."),
]

E7_EVIDENCE = [
    Evidence(id="evd_e7_replay", kind=EvidenceKind.historical_replay,
             title="Strategy-replay over the discovery window", label=SIMULATED_LABEL,
             performance=E7_HIST,
             fact_refs=["fct_e7_hist_exp", "fct_e7_hist_exp_2x", "fct_e7_hist_dd"],
             provenance=_prov(DISCOVERY, REPLAY_ENGINE)),
    Evidence(id="evd_e7_costs", kind=EvidenceKind.cost_sensitivity,
             title="Where the edge went: gross versus net, and the 2x-slippage run",
             label=SIMULATED_LABEL, performance=None,
             fact_refs=["fct_e7_gross_exp", "fct_e7_cost_drag", "fct_e7_hist_exp_2x",
                        "fct_e7_spread_widening"],
             provenance=_prov(DISCOVERY, REPLAY_ENGINE),
             interpretation=_line(
                 StoryBeat.learning,
                 "The pattern was real; the tradeable edge was not",
                 "Before costs the rule expects {{fact:fct_e7_gross_exp}} per trade. "
                 "Costs and slippage take {{fact:fct_e7_cost_drag}} of that, and doubling "
                 "slippage pushes it to {{fact:fct_e7_hist_exp_2x}}. The qualifying "
                 "sessions are exactly the sessions where the spread widens — by "
                 "{{fact:fct_e7_spread_widening}} against ordinary sessions in the same "
                 "names. The condition that creates the signal is the condition that "
                 "makes it expensive to trade.",
                 author=Author.llm, model=MODEL_REASON, prompt_version="interpret@2",
                 refs=["fct_e7_gross_exp", "fct_e7_cost_drag", "fct_e7_hist_exp_2x",
                       "fct_e7_spread_widening"])),
    Evidence(id="evd_e7_forward", kind=EvidenceKind.forward_virtual,
             title="Forward virtual book up to the day it was killed", label=VIRTUAL_LABEL,
             performance=E7_VIRT,
             fact_refs=["fct_e7_virt_exp", "fct_e7_virt_dd", "fct_e7_virt_n"],
             provenance=_prov(E7_WINDOW, BOOK_ENGINE)),
]

E7_LEDGER = [
    _trade("trd_e7_01", "RVNL", Direction.long, date(2026, 2, 26), date(2026, 2, 27),
           498.20, date(2026, 2, 27), 471.35, "stop", -5.69, -5.39, 0.21, -5.82),
    _trade("trd_e7_02", "SUZLON", Direction.long, date(2026, 4, 8), date(2026, 4, 9),
           78.45, date(2026, 4, 9), 74.90, "stop", -4.83, -4.53, 0.14, -4.96),
    _trade("trd_e7_03", "JIOFIN", Direction.long, date(2026, 6, 12), date(2026, 6, 15),
           392.10, date(2026, 6, 15), 379.55, "stop", -3.50, -3.20, 0.26, -3.68),
    _trade("trd_e7_04", "IRFC", Direction.long, date(2026, 7, 20), date(2026, 7, 21),
           168.75, date(2026, 7, 21), 166.40, "horizon", -1.69, -1.39, 0.31, -1.94),
    _trade("trd_e7_05", "HAL", Direction.long, date(2026, 8, 18), date(2026, 8, 19),
           5240.00, date(2026, 8, 19), 5227.10, "horizon", -0.55, -0.25, 0.62, -1.12),
    _trade("trd_e7_06", "BEL", Direction.long, date(2026, 3, 17), date(2026, 3, 18),
           412.85, date(2026, 3, 18), 416.20, "horizon", 0.51, 0.81, 1.18, -0.44),
    _trade("trd_e7_07", "COCHINSHIP", Direction.long, date(2026, 5, 28), date(2026, 5, 29),
           2180.40, date(2026, 5, 29), 2216.75, "horizon", 1.37, 1.67, 2.01, -0.38),
]

EXP_0007 = ExperimentDetail(
    id="exp_0007",
    hypothesis=(
        "A gap-up above the prior session's high on more than 2x median volume "
        "continues into the close."
    ),
    question="Does an aggressive open keep going?",
    rationale=(
        "One of the most commonly repeated claims in retail trading. Worth killing "
        "carefully and in public rather than leaving it to fold-lore."
    ),
    status=ExperimentStatus.died,
    stage=LoopStage.learn,
    opened_at=datetime(2025, 12, 3, 18, 40, tzinfo=IST),
    strategy_version="strategy@v1.1",
    constitution_version=CONSTITUTION,
    historical_return=E7_HIST, virtual_return=E7_VIRT,
    occurrences=76, n=48,
    spark=_spark([0.0, -1.2, 0.4, -2.1, -3.6, -2.8, -4.9, -5.3], E7_WINDOW),
    as_of=AS_OF_TS,
    rulebook=Rulebook(
        universe=UNIVERSE_PIT, direction=Direction.long,
        entry=("Open above the prior session's high AND first-hour volume above 2x the "
               "20-session median. Fill at the NEXT session open after the signal bar."),
        invalidation="A trade back below the prior session's high. No price target was ever set.",
        exit="Hard stop at -3%; exit at the close of the entry session.",
        horizon_sessions=1,
        sizing="Equal-weight, 2% of virtual capital per idea, maximum 6 concurrent.",
        cost_convention=COSTS_V3,
    ),
    facts=E7_FACTS, evidence=E7_EVIDENCE,
    virtual_book=VirtualBook(metrics=E7_VIRT, capital_inr=CAPITAL_INR,
                             ledger_losers_first=E7_LEDGER),
    change_log=[
        ChangeLogEntry(
            seq=1, level=LearningLevel.L2, at=datetime(2026, 4, 30, 18, 40, tzinfo=IST),
            what_changed="Raised the volume multiple from 1.5x to 2.0x of the twenty-session median.",
            why=("An attempt to keep only the most aggressive opens after the first thirty "
                 "forward trades came in negative. Inside the approved range of 1.25x to 3.0x."),
            evidence_refs=["evd_e7_forward"],
            previous_version="strategy@v1.0", new_version="strategy@v1.1",
            performance_before=None, performance_after=E7_VIRT, improved=False,
            decided_by=Author.llm, constitution_version=CONSTITUTION),
        ChangeLogEntry(
            seq=2, level=LearningLevel.L1, at=datetime(2026, 8, 21, 18, 40, tzinfo=IST),
            what_changed="Recorded the cost-sensitivity evidence and closed the evidence base.",
            why="The parameter change did not repair the forward book; the cause was elsewhere.",
            evidence_refs=["evd_e7_costs", "evd_e7_forward"],
            previous_version="evidence@2026-04-30", new_version="evidence@2026-08-21",
            improved=False, decided_by=Author.engine, constitution_version=CONSTITUTION),
    ],
    post_mortem=PostMortem(
        died_at=datetime(2026, 8, 21, 18, 45, tzinfo=IST),
        cause=DeathCause.no_edge_after_costs,
        summary=_line(
            StoryBeat.outcome,
            "Killed: the signal is real, the trade is not",
            "The pattern exists. Before costs it expects {{fact:fct_e7_gross_exp}} per "
            "trade. After the cost convention it expects {{fact:fct_e7_hist_exp}}, and at "
            "doubled slippage {{fact:fct_e7_hist_exp_2x}}. Forward, across "
            "{{fact:fct_e7_virt_n}} closed virtual trades, it delivered "
            "{{fact:fct_e7_virt_exp}} with a worst drawdown of {{fact:fct_e7_virt_dd}}. "
            "A parameter change was tried and did not help, which was the signal that the "
            "problem was structural rather than a badly chosen threshold.",
            author=Author.llm, model=MODEL_REASON, prompt_version="post_mortem@2",
            at=datetime(2026, 8, 21, 18, 45, tzinfo=IST),
            refs=["fct_e7_gross_exp", "fct_e7_hist_exp", "fct_e7_hist_exp_2x",
                  "fct_e7_virt_n", "fct_e7_virt_exp", "fct_e7_virt_dd"]),
        what_we_kept=(
            "Gap continuation is a liquidity story, not a direction story: the sessions "
            "that produce the signal are the sessions where the spread widens most. That "
            "learning was carried into exp_0011's cost handling and is why every new "
            "intraday hypothesis now has to clear the 2x-slippage gate before it is "
            "allowed any virtual capital at all."
        ),
        evidence_refs=["evd_e7_costs", "evd_e7_forward"],
        retired_version="strategy@v1.1",
        decided_by=Author.llm, approved_by="founder",
    ),
    triggers=[
        Trigger(id="trg_e7_dev", type=TriggerType.performance_deviation,
                description=("Forward expectancy crossed below zero and stayed there across "
                             "two consecutive review windows."),
                fired_at=datetime(2026, 8, 21, 18, 35, tzinfo=IST),
                fact_refs=["fct_e7_virt_exp", "fct_e7_hist_exp"]),
    ],
    next_review=None,
    story=[
        _line(StoryBeat.noticed, "The most repeated claim in retail trading",
              "Everyone says an aggressive gap-up keeps going. The observer sees these "
              "sessions constantly, so the claim is cheap to test and expensive to leave "
              "untested.",
              author=Author.llm, model=MODEL_REASON, prompt_version="observe@3",
              at=datetime(2025, 12, 3, 18, 41, tzinfo=IST)),
        _line(StoryBeat.hypothesis, "I am testing the claim as stated, not a flattering version of it",
              "The rule is written the way the claim is usually made, with no added "
              "filters to help it. Gross of costs, the discovery window agrees: "
              "{{fact:fct_e7_gross_exp}} per trade.",
              author=Author.llm, model=MODEL_REASON, prompt_version="hypothesize@4",
              at=datetime(2025, 12, 3, 18, 42, tzinfo=IST),
              refs=["fct_e7_gross_exp"]),
        _line(StoryBeat.experiment, "Same-session round trip on virtual capital",
              "Entry at the next open, hard stop, exit at the close of the entry session. "
              "Full costs and slippage on both legs.",
              author=Author.engine, at=datetime(2025, 12, 3, 18, 43, tzinfo=IST)),
        _line(StoryBeat.outcome, "Costs ate all of it, then the forward book went negative",
              "The cost convention takes {{fact:fct_e7_cost_drag}} of the gross expectancy "
              "per trade. Forward, the book lost money: {{fact:fct_e7_virt_exp}} per trade "
              "over {{fact:fct_e7_virt_n}} closed trades, with a worst drawdown of "
              "{{fact:fct_e7_virt_dd}}.",
              author=Author.llm, model=MODEL_NARRATE, prompt_version="narrate@5",
              at=datetime(2026, 8, 21, 18, 44, tzinfo=IST),
              refs=["fct_e7_cost_drag", "fct_e7_virt_exp", "fct_e7_virt_n",
                    "fct_e7_virt_dd"]),
        _line(StoryBeat.learning, "I killed it, and I kept the reason",
              "The surviving learning is that the gap is a liquidity event: the spread on "
              "qualifying sessions is {{fact:fct_e7_spread_widening}} what it is on "
              "ordinary sessions in the same names. Every intraday hypothesis proposed "
              "since then has to clear the doubled-slippage gate before it is given any "
              "virtual capital.",
              author=Author.llm, model=MODEL_REASON, prompt_version="reflect@4",
              at=datetime(2026, 8, 21, 18, 45, tzinfo=IST),
              refs=["fct_e7_spread_widening"]),
        _line(StoryBeat.next, "Next: ask the liquidity question directly",
              "If the gap is really a spread event, the interesting question is not "
              "whether price continues but whether the spread is predictable. That is a "
              "different experiment, and it is queued.",
              author=Author.llm, model=MODEL_REASON, prompt_version="decide_next@3",
              at=datetime(2026, 8, 21, 18, 46, tzinfo=IST)),
    ],
    llm_usage=LlmUsage(
        window="experiment_lifetime",
        by_model=[
            ModelUsage(model=MODEL_REASON, calls=28, input_tokens=402_800, output_tokens=36_100,
                       cache_read_input_tokens=334_200, cache_creation_input_tokens=44_800,
                       cost_usd=0.60),
            ModelUsage(model=MODEL_NARRATE, calls=41, input_tokens=201_500, output_tokens=34_700,
                       cache_read_input_tokens=163_900, cache_creation_input_tokens=20_800,
                       cost_usd=0.24),
        ],
        total_cost_usd=0.84, as_of=AS_OF_TS,
    ),
)


# ── The catalogue ────────────────────────────────────────────────────────────

EXPERIMENTS: dict[str, ExperimentDetail] = {
    e.id: e for e in (EXP_0003, EXP_0009, EXP_0011, EXP_0014, EXP_0015, EXP_0007)
}

#: Presentation order for the list endpoint: the honest order is
#: died and struggling first, promoted last — losers first, everywhere.
LIST_ORDER = ["exp_0007", "exp_0014", "exp_0009", "exp_0015", "exp_0011", "exp_0003"]


def _summary(detail: ExperimentDetail) -> ExperimentSummary:
    return ExperimentSummary.model_validate(
        detail.model_dump(mode="python", include=set(ExperimentSummary.model_fields))
    )


def experiment_list(status: Optional[ExperimentStatus] = None) -> ExperimentListResponse:
    items = [_summary(EXPERIMENTS[eid]) for eid in LIST_ORDER]
    if status is not None:
        items = [i for i in items if i.status == status]
    return ExperimentListResponse(
        as_of=AS_OF_TS, status_filter=status, count=len(items), items=items
    )


def experiment_detail(experiment_id: str) -> Optional[ExperimentDetail]:
    return EXPERIMENTS.get(experiment_id)


# ── The live loop ────────────────────────────────────────────────────────────

LOOP_FACTS = [
    _fact("fct_loop_divergence", "consecutive sessions of advance/decline divergence",
          5, Unit.sessions, None, DateRange(start=date(2026, 8, 31), end=AS_OF), OBSERVER),
    _fact("fct_loop_scanned", "conditions the observer evaluated today", 1_482_000,
          Unit.count, None, DateRange(start=AS_OF, end=AS_OF), OBSERVER,
          note="Deterministic observation is cheap and continuous; the LLM is not."),
    _fact("fct_loop_llm_calls", "times the observer judged a condition worth waking the LLM for",
          3, Unit.count, None, DateRange(start=AS_OF, end=AS_OF), OBSERVER),
    _fact("fct_loop_live", "experiments currently holding virtual capital", 4, Unit.count,
          None, DateRange(start=AS_OF, end=AS_OF), BOOK_ENGINE),
    _fact("fct_loop_died", "experiments killed and published to the graveyard so far",
          1, Unit.count, None, DateRange(start=date(2025, 11, 1), end=AS_OF), BOOK_ENGINE),
    _fact("fct_e11_virt_exp_loop", "expectancy per trade of the strongest live experiment",
          0.21, Unit.pct_per_trade, 34, FORWARD),
    _fact("fct_e11_virt_n_loop", "closed virtual trades behind that number", 34, Unit.count,
          34, FORWARD),
    _fact("fct_loop_similar", "similar past experiments returned by associative recall",
          2, Unit.count, None, DateRange(start=date(2018, 1, 1), end=AS_OF),
          "memory_recall@0.6.0"),
]

LOOP = LoopResponse(
    as_of=AS_OF_TS,
    cycle_id="cyc_2026_09_08_01",
    stage=LoopStage.hypothesize,
    constitution_version=CONSTITUTION,
    active_experiment_id="exp_0015",
    trigger=Trigger(
        id="trg_loop_unusual", type=TriggerType.unusual_market_condition,
        description=("Advance/decline divergence held for five consecutive sessions — "
                     "unusual against the observer's own history, so the LLM was woken."),
        fired_at=datetime(2026, 9, 8, 16, 2, tzinfo=IST),
        fact_refs=["fct_loop_divergence"],
    ),
    story=[
        _line(StoryBeat.noticed, "The market's breadth stopped agreeing with its price",
              "I watch conditions continuously and cheaply — {{fact:fct_loop_scanned}} of "
              "them today alone — and almost all of them are unremarkable. This one was "
              "not: the advance/decline line has pulled away from the index for "
              "{{fact:fct_loop_divergence}} in a row. That is why I am awake; I was woken "
              "{{fact:fct_loop_llm_calls}} today in total.",
              author=Author.llm, model=MODEL_REASON, prompt_version="observe@3",
              refs=["fct_loop_scanned", "fct_loop_divergence", "fct_loop_llm_calls"]),
        _line(StoryBeat.hypothesis, "I am asking whether breakouts still mean what they meant",
              "Before proposing anything I searched my own past work and found "
              "{{fact:fct_loop_similar}} in the same neighbourhood — neither asked this "
              "question. So this is a new experiment rather than a re-run, and it is now "
              "queued as {{exp:exp_0015}}.",
              author=Author.llm, model=MODEL_REASON, prompt_version="hypothesize@4",
              refs=["fct_loop_similar"]),
        _line(StoryBeat.experiment, "Four experiments are live on virtual capital right now",
              "{{fact:fct_loop_live}} experiments currently hold virtual money, each under "
              "its own fixed rule, filled at the next open and charged full costs and "
              "slippage. Nothing here is a real order.",
              author=Author.llm, model=MODEL_NARRATE, prompt_version="narrate@5",
              refs=["fct_loop_live"]),
        _line(StoryBeat.outcome, "The strongest one is barely past being an anecdote",
              "My best live result is an expectancy of {{fact:fct_e11_virt_exp_loop}} per "
              "trade across {{fact:fct_e11_virt_n_loop}} closed trades. That sample is "
              "still small enough that I would not act on it, and I am saying so rather "
              "than rounding it up into a claim.",
              author=Author.llm, model=MODEL_NARRATE, prompt_version="narrate@5",
              refs=["fct_e11_virt_exp_loop", "fct_e11_virt_n_loop"]),
        _line(StoryBeat.learning, "What I learned most recently came from a failure",
              "{{fact:fct_loop_died}} experiment has been killed and published so far. Its "
              "lesson — that a gap is a liquidity event rather than a direction event — is "
              "now a gate every new intraday hypothesis has to pass before it is given any "
              "virtual capital.",
              author=Author.llm, model=MODEL_REASON, prompt_version="reflect@4",
              refs=["fct_loop_died"]),
        _line(StoryBeat.next, "Tonight I run the replay on the divergence question",
              "The deterministic replay runs first, over the discovery window only. If it "
              "does not clear the doubled-slippage gate, {{exp:exp_0015}} dies before it ever holds "
              "virtual capital — and that outcome gets published too.",
              author=Author.llm, model=MODEL_REASON, prompt_version="decide_next@3"),
    ],
    facts=LOOP_FACTS,
    counts={
        ExperimentStatus.queued: 1,
        ExperimentStatus.testing: 1,
        ExperimentStatus.validating: 1,
        ExperimentStatus.promising: 1,
        ExperimentStatus.promoted: 1,
        ExperimentStatus.died: 1,
    },
    llm_usage=LlmUsage(
        window="today_ist",
        by_model=[
            ModelUsage(model=MODEL_REASON, calls=3, input_tokens=41_200, output_tokens=3_400,
                       cache_read_input_tokens=36_800, cache_creation_input_tokens=3_100,
                       cost_usd=0.05),
            ModelUsage(model=MODEL_NARRATE, calls=6, input_tokens=28_900, output_tokens=4_800,
                       cache_read_input_tokens=24_100, cache_creation_input_tokens=3_400,
                       cost_usd=0.03),
        ],
        total_cost_usd=0.08, daily_budget_usd=5.00, budget_used_pct=1.6, as_of=AS_OF_TS,
    ),
)


# ── Learnings ────────────────────────────────────────────────────────────────

LEARNING_FACTS = [
    _fact("fct_lrn_dryvol_gap", "expectancy gap between dry-volume and rising-volume pullbacks",
          0.35, Unit.pct_per_trade, 406,
          note="Difference of two deterministic replays over the same window."),
    _fact("fct_lrn_spread", "opening spread on gap-up sessions versus ordinary sessions",
          2.3, Unit.x, 213),
    _fact("fct_lrn_decay", "share of the historical replay expectancy that has survived forward, "
          "averaged across live experiments", 62.0, Unit.pct, 122, FORWARD,
          note="Aggregated over the four experiments that have a forward book."),
    _fact("fct_lrn_2x_failures", "experiments so far whose edge did not survive doubled slippage",
          2, Unit.count, 6, DISCOVERY),
    _fact("fct_e15_divergence_days", "consecutive sessions of advance/decline divergence", 5,
          Unit.sessions, None, DateRange(start=date(2026, 8, 31), end=AS_OF), OBSERVER),
    _fact("fct_nxt_sector_share", "share of the promoted experiment's winning trades "
          "concentrated in its three largest sectors", 71.0, Unit.pct, 61, FORWARD),
]

LEARNINGS = LearningsResponse(
    as_of=AS_OF_TS,
    constitution_version=CONSTITUTION,
    learned=[
        Learning(
            id="lrn_0001",
            statement=_line(
                StoryBeat.learning,
                "Participation separates pullbacks that price geometry cannot",
                "The same pullback shape carries a materially different expectancy "
                "depending on whether volume is drying up or rising — a gap of "
                "{{fact:fct_lrn_dryvol_gap}} per trade. The price pattern was never the "
                "edge; the participation collapse was.",
                author=Author.llm, model=MODEL_REASON, prompt_version="reflect@4",
                refs=["fct_lrn_dryvol_gap"]),
            level=LearningLevel.L3, learned_at=datetime(2026, 5, 19, 18, 40, tzinfo=IST),
            from_experiments=["exp_0003"], evidence_refs=["evd_e3_volsplit"],
            fact_refs=["fct_lrn_dryvol_gap"], confidence=Confidence.supported,
            n=406, applied_in=["strategy@v2.1", "strategy@v3.0"]),
        Learning(
            id="lrn_0002",
            statement=_line(
                StoryBeat.learning,
                "A gap-up is a liquidity event wearing a direction costume",
                "The sessions that generate a gap-continuation signal are the same "
                "sessions where the spread widens — by {{fact:fct_lrn_spread}} against "
                "ordinary sessions in the same names. The condition that creates the "
                "signal is the condition that makes it expensive to trade.",
                author=Author.llm, model=MODEL_REASON, prompt_version="post_mortem@2",
                refs=["fct_lrn_spread"]),
            level=LearningLevel.L2, learned_at=datetime(2026, 8, 21, 18, 45, tzinfo=IST),
            from_experiments=["exp_0007"], evidence_refs=["evd_e7_costs"],
            fact_refs=["fct_lrn_spread"], confidence=Confidence.supported,
            n=213, applied_in=["strategy@v1.0"]),
        Learning(
            id="lrn_0003",
            statement=_line(
                StoryBeat.learning,
                "About a third of every historical edge disappears going forward",
                "Across the experiments with a forward book, only "
                "{{fact:fct_lrn_decay}} of the historical replay expectancy has survived "
                "into live virtual trading. I now discount every new historical result by "
                "that observed decay before deciding whether it is worth capital, and "
                "{{fact:fct_lrn_2x_failures}} experiments have already failed the doubled-"
                "slippage gate outright.",
                author=Author.llm, model=MODEL_REASON, prompt_version="reflect@4",
                refs=["fct_lrn_decay", "fct_lrn_2x_failures"]),
            level=LearningLevel.L1, learned_at=datetime(2026, 8, 28, 18, 40, tzinfo=IST),
            from_experiments=["exp_0003", "exp_0007", "exp_0009", "exp_0011"],
            evidence_refs=["evd_e3_forward", "evd_e7_forward", "evd_e9_forward",
                           "evd_e11_forward"],
            fact_refs=["fct_lrn_decay", "fct_lrn_2x_failures"],
            confidence=Confidence.provisional, n=122, applied_in=[]),
    ],
    testing_next=[
        NextTest(
            id="nxt_0001",
            question="When breadth and price disagree, does breakout follow-through change?",
            why_now=_line(
                StoryBeat.next,
                "Because the disagreement is unusual right now, not because it is due",
                "The advance/decline line has diverged from the index for "
                "{{fact:fct_e15_divergence_days}} in a row. The observer rates that as unusual "
                "against its own history, and my memory search found nothing that already "
                "answers it.",
                author=Author.llm, model=MODEL_REASON, prompt_version="decide_next@3",
                refs=["fct_e15_divergence_days"]),
            triggered_by=E15_TRIGGER,
            planned_test=("Deterministic replay of a 20-session-high breakout rule over "
                          "2018-01-01 to 2025-12-31, split by the sign of the index "
                          "advance/decline divergence, next-open fills, costs_v3, plus the "
                          "2x-slippage sensitivity run."),
            queued_experiment_id="exp_0015",
            fact_refs=["fct_e15_divergence_days"]),
        NextTest(
            id="nxt_0002",
            question="Is the promoted experiment's edge actually a sector tailwind?",
            why_now=_line(
                StoryBeat.next,
                "Its winners are concentrated enough to be suspicious",
                "{{fact:fct_nxt_sector_share}} of the promoted experiment's winning trades "
                "sit in its three largest sectors. If a sector-neutral replay kills the "
                "edge, I have been renting a tailwind and calling it a signal.",
                author=Author.llm, model=MODEL_REASON, prompt_version="decide_next@3",
                refs=["fct_nxt_sector_share"]),
            planned_test=("Re-run the exp_0003 replay with sector-neutral weighting over "
                          "the same discovery window and compare expectancy, both net of "
                          "costs and at 2x slippage."),
            blocked_by="Point-in-time sector mapping is not yet wired into the replay engine.",
            fact_refs=["fct_nxt_sector_share"]),
        NextTest(
            id="nxt_0003",
            question="If a gap is a spread event, is the spread itself predictable?",
            why_now=_line(
                StoryBeat.next,
                "The dead experiment left a better question than the one it asked",
                "Killing the gap-continuation idea produced a sharper question than it "
                "started with. That is the outcome I want from a death, and it is worth "
                "queueing on its own.",
                author=Author.llm, model=MODEL_REASON, prompt_version="decide_next@3"),
            planned_test=("Measure realised opening spread as the dependent variable on "
                          "the same qualifying sessions, over the discovery window only."),
            blocked_by="Requires tick-level spread history the current data layer does not expose."),
    ],
    facts=LEARNING_FACTS,
)
