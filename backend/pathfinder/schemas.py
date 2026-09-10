"""
Pathfinder API contract — Pydantic models.

THIS FILE IS THE SOURCE OF TRUTH FOR `docs/openapi.yaml`.
Regenerate the spec after any change:  python scripts/gen_openapi.py

Design laws encoded in the *schema* (not just in prose), from CLAUDE.md +
docs/sessions/PATHFINDER.md:

  L-1  Expectancy is the hero.  `expectancy_pct_per_trade` is REQUIRED on every
       performance block; `win_rate_pct` is Optional and never structurally
       required.
  L-2  Every return is paired with its drawdown.  A performance block cannot be
       constructed without `max_drawdown_pct` and `current_drawdown_pct`.
  L-3  No promises, no target prices.  There is deliberately NO `target`,
       `target_price`, `expected_return` or `projection` field anywhere in this
       contract.  A content lint (backend/tests/test_pathfinder_p0.py) enforces it.
  L-4  Every number carries n + date range + data source + cost convention.
       `Provenance` is required on every `Fact` and every `PerformanceBlock`.
  L-5  Point-in-time is law.  Every `Provenance` carries `as_of`; nothing may be
       computed from data after it.
  L-6  n<20 greyed, n<50 flagged.  `sample_flag` is DERIVED from n by
       `sample_flag_for(n)` — a client never has to guess.
  L-7  Losers first.  The virtual ledger field is literally named
       `ledger_losers_first` and is validated ascending by net P&L.
  L-8  THE LLM NEVER CALCULATES.  Any story text with `produced_by == "llm"`
       must contain NO literal numerals — only `{{fact:<id>}}` references that
       resolve to a deterministic `Fact`.  This is the machine-checkable form of
       the core principle.
  L-9  The Constitution is human-only.  A change-log entry at level L4 must have
       `decided_by == "human"`.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from enum import Enum
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ── Constants the whole product shares ───────────────────────────────────────

SIMULATED_LABEL = "Simulated · Not traded · Not PaRRVA-verified"
VIRTUAL_LABEL = "Virtual money · Forward-tracked · Not traded · Not PaRRVA-verified"
RESEARCH_DISCLOSURE = (
    "Pathfinder is a research lab. Everything here is a hypothesis under test with "
    "virtual money, not advice and not a recommendation. No returns are promised. "
    "Kanida never places an order on your behalf."
)

#: n thresholds from CLAUDE.md — n<20 greyed, n<50 flagged.
N_GREY_BELOW = 20
N_FLAG_BELOW = 50

#: The ONLY way a number may appear in LLM-authored prose.
FACT_REF_RE = re.compile(r"\{\{fact:(?P<id>fct_[a-z0-9_]+)\}\}")
#: Reference tokens allowed in prose (ids, versions) — stripped before the
#: "no bare numerals" check.
REF_TOKEN_RE = re.compile(r"\{\{(?:fact|exp|evd|ver):[a-zA-Z0-9_.\-]+\}\}")


def sample_flag_for(n: Optional[int]) -> "SampleFlag":
    """The single, product-wide n-flag rule. Never re-implement this in a client."""
    if n is None:
        return SampleFlag.unknown
    if n < N_GREY_BELOW:
        return SampleFlag.greyed
    if n < N_FLAG_BELOW:
        return SampleFlag.flagged
    return SampleFlag.ok


# ── Enums ────────────────────────────────────────────────────────────────────

class ExperimentStatus(str, Enum):
    """The Pathfinder lifecycle. `died` is a first-class, published outcome."""
    queued = "queued"
    testing = "testing"
    validating = "validating"
    promising = "promising"
    promoted = "promoted"
    died = "died"


class LoopStage(str, Enum):
    """Where the loop currently is. Mirrors the core-principle pipeline."""
    observe = "observe"
    hypothesize = "hypothesize"
    test = "test"
    interpret = "interpret"
    trade = "trade"
    track = "track"
    review = "review"
    learn = "learn"
    next = "next"


class StoryBeat(str, Enum):
    """The six beats of the living-research-loop story. Order is the UX law."""
    noticed = "noticed"          # "I noticed X"
    hypothesis = "hypothesis"    # "I am testing Y because of this evidence"
    experiment = "experiment"    # "here is the virtual experiment"
    outcome = "outcome"          # "here is what happened"
    learning = "learning"        # "here is what I learned and changed"
    next = "next"                # "here is what I will test next"


class Author(str, Enum):
    engine = "engine"   # deterministic Python/SQL — the ONLY thing that may compute
    llm = "llm"         # proposes / interprets / reflects / narrates — never computes
    human = "human"     # the founder / RA — the only author allowed at L4


class SampleFlag(str, Enum):
    ok = "ok"            # n >= 50
    flagged = "flagged"  # 20 <= n < 50
    greyed = "greyed"    # n < 20
    unknown = "unknown"  # no sample yet (a queued experiment)


class Unit(str, Enum):
    pct = "pct"
    pct_per_trade = "pct_per_trade"
    bps = "bps"
    count = "count"
    ratio = "ratio"
    days = "days"
    sessions = "sessions"
    x = "x"
    inr = "inr"
    text = "text"


class LearningLevel(str, Enum):
    """The 4-level learning hierarchy. L4 is human-controlled."""
    L1 = "L1"  # evidence — new observations appended automatically
    L2 = "L2"  # parameter — tuned within approved ranges
    L3 = "L3"  # strategy — a new version; must backtest + forward-validate to replace
    L4 = "L4"  # constitution — HUMAN ONLY


class Direction(str, Enum):
    long = "long"
    short = "short"


class TriggerType(str, Enum):
    """Autonomy = continuous cheap observation, intelligent LLM activation."""
    unusual_market_condition = "unusual_market_condition"
    observation_threshold = "observation_threshold"      # e.g. 30 new observations
    performance_deviation = "performance_deviation"
    scheduled_review = "scheduled_review"
    human_request = "human_request"


class DeathCause(str, Enum):
    no_edge_after_costs = "no_edge_after_costs"
    #: Added in P1. The discovery scan's single strongest candidate was a multi-session
    #: short — not executable in the NSE cash segment under the delivery cost
    #: convention. A statistical result nobody can trade needs its own honest cause;
    #: filing it under `data_quality` would have been a lie about why it died.
    not_implementable_under_cost_convention = "not_implementable_under_cost_convention"
    #: Added in P1 after an audit. A positive expectancy that cannot be told apart from
    #: a same-size random draw is not "no edge after costs" and it is not "too small a
    #: sample" — it is a result the evidence does not support. Filing it as either of
    #: the others was false, and the cause is customer-facing copy.
    not_distinguishable_from_chance = "not_distinguishable_from_chance"
    edge_did_not_persist_oos = "edge_did_not_persist_oos"
    sample_too_small = "sample_too_small"
    data_quality = "data_quality"
    regime_dependent_and_regime_gone = "regime_dependent_and_regime_gone"
    superseded_by_newer_version = "superseded_by_newer_version"


class EvidenceKind(str, Enum):
    historical_replay = "historical_replay"
    forward_virtual = "forward_virtual"
    regime_split = "regime_split"
    cost_sensitivity = "cost_sensitivity"
    placebo = "placebo"
    novelty_check = "novelty_check"


class Confidence(str, Enum):
    provisional = "provisional"
    supported = "supported"
    strong = "strong"


# ── Value objects ────────────────────────────────────────────────────────────

class DateRange(BaseModel):
    model_config = ConfigDict(extra="forbid")
    start: date
    end: date

    @model_validator(mode="after")
    def _ordered(self) -> "DateRange":
        if self.end < self.start:
            raise ValueError("date_range.end precedes date_range.start")
        return self


class Provenance(BaseModel):
    """L-4 + L-5: where a number came from, and the `as_of` it was computed at."""
    model_config = ConfigDict(extra="forbid")

    data_source: str = Field(
        ..., description="Dataset identifier, e.g. 'kite_eod_adjusted_nifty500'."
    )
    date_range: DateRange = Field(..., description="The window the number was computed over.")
    as_of: date = Field(
        ...,
        description="Point-in-time cutoff. No input to this number post-dates it.",
    )
    cost_convention: str = Field(
        ...,
        description=(
            "Versioned cost + slippage convention applied, e.g. "
            "'costs_v3: brokerage+STT+exchange+GST+stamp, slippage 10bps, entry next open'."
        ),
    )
    computed_by: str = Field(
        ...,
        description=(
            "The DETERMINISTIC component that computed it, e.g. 'book_engine@2.3.1'. "
            "A model id must never appear here — the LLM does not compute."
        ),
    )
    computed_at: datetime = Field(..., description="IST timestamp of the computation.")
    universe: Optional[str] = Field(
        None, description="Point-in-time universe, e.g. 'nifty500_pit'."
    )


class Fact(BaseModel):
    """
    A single deterministic number. THE ONLY PLACE A NUMBER MAY ORIGINATE.

    LLM prose references a fact by id; the client substitutes the rendered value.
    This is what makes "the LLM never calculates" machine-checkable.
    """
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., pattern=r"^fct_[a-z0-9_]+$")
    label: str = Field(..., description="Human label, e.g. 'expectancy per trade, net of costs'.")
    value: float | int | str
    unit: Unit
    n: Optional[int] = Field(
        None, ge=0, description="Sample size behind the number. Required for statistics."
    )
    sample_flag: SampleFlag = SampleFlag.unknown
    provenance: Provenance
    note: Optional[str] = None

    @model_validator(mode="after")
    def _derive_and_check(self) -> "Fact":
        # L-6: the flag is derived, never asserted by hand.
        self.sample_flag = sample_flag_for(self.n)
        # L-4: a statistic without an n is not a number we are allowed to show.
        if self.unit in {Unit.pct, Unit.pct_per_trade, Unit.ratio, Unit.x} and self.n is None:
            raise ValueError(f"fact {self.id}: statistic of unit {self.unit} requires n")
        return self


class PerformanceBlock(BaseModel):
    """
    L-1 + L-2. Expectancy is required and first; every return is structurally
    paired with a drawdown. There is NO target/projection field, by design.
    """
    model_config = ConfigDict(extra="forbid")

    basis: Literal["historical_replay", "virtual_book"] = Field(
        ...,
        description=(
            "historical_replay = strategy-replay over history under the exact "
            "entry/exit/horizon actually traded (never hold-to-close). "
            "virtual_book = forward, virtual money, marked daily."
        ),
    )
    label: str = Field(
        ...,
        description="Mandatory honesty label — the simulated or virtual-money label.",
    )

    # HERO METRIC — first, and required.
    expectancy_pct_per_trade: float = Field(
        ..., description="Expected value per trade, %, NET of the stated cost convention."
    )
    expectancy_2x_slippage_pct_per_trade: float = Field(
        ..., description="Same expectancy re-run at 2x slippage (the sensitivity gate)."
    )

    # Every return, paired with its drawdown.
    total_return_pct: Optional[float] = Field(
        None, description="Cumulative return of the book over date_range, net of costs."
    )
    max_drawdown_pct: float = Field(
        ..., ge=0, description="Worst peak-to-trough drawdown over date_range. ALWAYS shown."
    )
    current_drawdown_pct: float = Field(
        ..., ge=0, description="Drawdown from the running peak as of `provenance.as_of`."
    )

    # Supporting — deliberately NOT hero.
    win_rate_pct: Optional[float] = Field(
        None, ge=0, le=100,
        description="Supporting only. Never render this as the headline metric.",
    )
    avg_win_pct: Optional[float] = None
    avg_loss_pct: Optional[float] = Field(None, le=0, description="Negative by convention.")
    payoff_ratio: Optional[float] = Field(None, ge=0)

    n: int = Field(..., ge=0, description="Closed trades behind these numbers.")
    occurrences: Optional[int] = Field(
        None, ge=0,
        description="Raw pattern occurrences observed (>= n; some are not tradeable).",
    )
    sample_flag: SampleFlag = SampleFlag.unknown
    provenance: Provenance

    @model_validator(mode="after")
    def _derive_and_check(self) -> "PerformanceBlock":
        self.sample_flag = sample_flag_for(self.n)
        expected = SIMULATED_LABEL if self.basis == "historical_replay" else VIRTUAL_LABEL
        if self.label != expected:
            raise ValueError(f"basis {self.basis} must carry the label: {expected}")
        if self.occurrences is not None and self.occurrences < self.n:
            raise ValueError("occurrences cannot be fewer than closed trades n")
        return self


class Spark(BaseModel):
    """A sparkline series. Points are cumulative % to date — never a projection."""
    model_config = ConfigDict(extra="forbid")

    basis: Literal["virtual_equity_pct", "historical_equity_pct"]
    points: list[float] = Field(..., min_length=2, max_length=400)
    date_range: DateRange
    as_of: date


class Trigger(BaseModel):
    """What woke the LLM up. Always fired by the deterministic observer."""
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., pattern=r"^trg_[a-z0-9_]+$")
    type: TriggerType
    description: str
    fired_at: datetime
    fired_by: Literal["engine"] = Field(
        "engine",
        description="Only the deterministic observer fires a trigger — never a clock, never the LLM.",
    )
    fact_refs: list[str] = Field(default_factory=list)


class StoryLine(BaseModel):
    """
    One beat of the loop story.

    L-8: if `produced_by == "llm"`, `headline` and `body` must contain no literal
    numerals — every number is a fact reference resolved by the client.
    """
    model_config = ConfigDict(extra="forbid")

    beat: StoryBeat
    headline: str
    body: str
    produced_by: Author
    model: Optional[str] = Field(
        None, description="Model id, when produced_by == 'llm' (e.g. 'claude-sonnet-5')."
    )
    prompt_version: Optional[str] = None
    at: datetime
    fact_refs: list[str] = Field(
        default_factory=list, description="Every fact id referenced in `body`/`headline`."
    )

    @model_validator(mode="after")
    def _llm_never_computes(self) -> "StoryLine":
        if self.produced_by == Author.llm:
            if not self.model:
                raise ValueError("llm-authored story line must name its model")
            for field_name in ("headline", "body"):
                text = REF_TOKEN_RE.sub("", getattr(self, field_name))
                if any(ch.isdigit() for ch in text):
                    raise ValueError(
                        f"L-8 violation in StoryLine.{field_name} ({self.beat}): "
                        "LLM-authored text may not contain a literal numeral — "
                        "reference a deterministic fact instead."
                    )
            refs: set[str] = set()
            for field_name in ("headline", "body"):
                refs |= {m.group("id") for m in FACT_REF_RE.finditer(getattr(self, field_name))}
            missing = refs - set(self.fact_refs)
            if missing:
                raise ValueError(f"fact refs used but not declared: {sorted(missing)}")
        elif self.model:
            raise ValueError("only an llm-authored story line may name a model")
        return self


class VirtualTrade(BaseModel):
    """One virtual trade. Entry = next open after the signal bar (quant law)."""
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., pattern=r"^trd_[a-z0-9_]+$")
    symbol: str
    direction: Direction
    signal_date: date = Field(..., description="Bar that produced the signal.")
    entry_date: date = Field(..., description="Next session after signal_date.")
    entry_price: float = Field(..., gt=0, description="Adjusted open of entry_date.")
    exit_date: Optional[date] = None
    exit_price: Optional[float] = Field(None, gt=0)
    exit_reason: Optional[str] = Field(
        None, description="stop | trail | horizon | invalidation | open"
    )
    holding_sessions: Optional[int] = Field(None, ge=0)
    pnl_pct_gross: Optional[float] = None
    pnl_pct_net: Optional[float] = Field(
        None, description="After the stated cost convention. The ledger sorts on this."
    )
    costs_pct: Optional[float] = Field(None, ge=0)
    slippage_bps: Optional[float] = Field(None, ge=0)
    mfe_pct: Optional[float] = None
    mae_pct: Optional[float] = None
    as_of: date

    @model_validator(mode="after")
    def _pit(self) -> "VirtualTrade":
        if self.entry_date <= self.signal_date:
            raise ValueError("entry must be the NEXT session after the signal bar")
        if self.exit_date and self.exit_date < self.entry_date:
            raise ValueError("exit precedes entry")
        return self


class Evidence(BaseModel):
    """A deterministic evidence bundle the LLM may interpret (never produce)."""
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., pattern=r"^evd_[a-z0-9_]+$")
    kind: EvidenceKind
    title: str
    label: str = Field(..., description="The simulated or virtual-money honesty label.")
    performance: Optional[PerformanceBlock] = None
    fact_refs: list[str] = Field(default_factory=list)
    provenance: Provenance
    interpretation: Optional[StoryLine] = Field(
        None, description="Optional LLM reading of THIS bundle. Subject to L-8."
    )


class ChangeLogEntry(BaseModel):
    """
    The governance record. Every change: what → why → evidence → previous version
    → new version → did performance improve. Also customer-facing copy.
    """
    model_config = ConfigDict(extra="forbid")

    seq: int = Field(..., ge=1)
    level: LearningLevel
    at: datetime
    what_changed: str
    why: str
    evidence_refs: list[str] = Field(..., min_length=1)
    previous_version: Optional[str] = Field(
        None, description="Null only for the very first version of a thing."
    )
    new_version: str
    performance_before: Optional[PerformanceBlock] = None
    performance_after: Optional[PerformanceBlock] = None
    improved: Optional[bool] = Field(
        None, description="null = not yet enough forward evidence to say. Never guessed."
    )
    decided_by: Author
    approved_by: Optional[str] = Field(None, description="Human approver, when required.")
    constitution_version: str
    validation: Optional[str] = Field(
        None,
        description="For L3: how the new version was backtested AND forward-validated before replacing.",
    )

    @model_validator(mode="after")
    def _governance(self) -> "ChangeLogEntry":
        # L-9: the agent learns INSIDE the Constitution; it cannot rewrite it.
        if self.level == LearningLevel.L4 and self.decided_by != Author.human:
            raise ValueError("L4 (Constitution) changes are human-only")
        if self.level == LearningLevel.L4 and not self.approved_by:
            raise ValueError("L4 changes require a named human approver")
        if self.level == LearningLevel.L3 and not self.validation:
            raise ValueError("L3 (strategy) changes require a stated backtest + forward validation")
        if self.previous_version is not None and self.previous_version == self.new_version:
            raise ValueError("a change must move the version")
        return self


class PostMortem(BaseModel):
    """Mandatory when status == died. Published — the graveyard is a feature."""
    model_config = ConfigDict(extra="forbid")

    died_at: datetime
    cause: DeathCause
    summary: StoryLine
    what_we_kept: str = Field(..., description="The learning that survives the death.")
    evidence_refs: list[str] = Field(..., min_length=1)
    retired_version: str
    decided_by: Author
    approved_by: Optional[str] = None


class ModelUsage(BaseModel):
    """Per-model token + cost instrumentation, straight from the provider response."""
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: str
    calls: int = Field(..., ge=0)
    input_tokens: int = Field(..., ge=0)
    output_tokens: int = Field(..., ge=0)
    cache_read_input_tokens: int = Field(..., ge=0)
    cache_creation_input_tokens: int = Field(..., ge=0)
    cost_usd: float = Field(..., ge=0)


class LlmUsage(BaseModel):
    """Real token instrumentation from day 1 + the hard daily cap."""
    model_config = ConfigDict(extra="forbid")

    window: Literal["today_ist", "experiment_lifetime"]
    by_model: list[ModelUsage]
    total_cost_usd: float = Field(..., ge=0)
    daily_budget_usd: Optional[float] = Field(None, ge=0)
    budget_used_pct: Optional[float] = Field(None, ge=0)
    as_of: datetime


# ── Resource models ──────────────────────────────────────────────────────────

class ExperimentSummary(BaseModel):
    """List-row shape. The P0 brief's field list, made honest."""
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., pattern=r"^exp_[a-z0-9_]+$")
    hypothesis: str
    status: ExperimentStatus
    opened_at: datetime
    strategy_version: Optional[str] = None
    constitution_version: str
    historical_return: Optional[PerformanceBlock] = Field(
        None, description="Strategy-replay over the discovery window. Null while queued."
    )
    virtual_return: Optional[PerformanceBlock] = Field(
        None, description="The forward virtual book. Null until the first closed trade."
    )
    occurrences: Optional[int] = Field(None, ge=0)
    n: Optional[int] = Field(None, ge=0)
    sample_flag: SampleFlag = SampleFlag.unknown
    spark: Optional[Spark] = None
    as_of: datetime

    @model_validator(mode="after")
    def _derive(self) -> "ExperimentSummary":
        self.sample_flag = sample_flag_for(self.n)
        if self.status == ExperimentStatus.queued and (
            self.historical_return or self.virtual_return
        ):
            raise ValueError("a queued experiment has no results yet")
        return self


class Rulebook(BaseModel):
    """The deterministic rule under test. No target price — invalidation instead."""
    model_config = ConfigDict(extra="forbid")

    universe: str
    direction: Direction
    entry: str = Field(..., description="Signal condition. Fill = NEXT OPEN after the signal bar.")
    invalidation: str = Field(..., description="What makes the idea wrong. Replaces 'target price'.")
    exit: str = Field(..., description="Deterministic exit: stop / trail / horizon.")
    horizon_sessions: int = Field(..., ge=1)
    sizing: str
    cost_convention: str


class VirtualBook(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metrics: PerformanceBlock
    capital_inr: float = Field(..., gt=0, description="Virtual capital. Never real money.")
    ledger_losers_first: list[VirtualTrade] = Field(
        ...,
        description="L-7: closed trades sorted ASCENDING by pnl_pct_net — worst first, always.",
    )
    open_positions: list[VirtualTrade] = Field(default_factory=list)

    @model_validator(mode="after")
    def _losers_first(self) -> "VirtualBook":
        if any(t.exit_date is None or t.pnl_pct_net is None for t in self.ledger_losers_first):
            raise ValueError("ledger_losers_first holds CLOSED trades with a net P&L only")
        closed = [t.pnl_pct_net for t in self.ledger_losers_first]
        if closed != sorted(closed):
            raise ValueError("L-7 violation: ledger_losers_first must be ascending by pnl_pct_net")
        if any(t.exit_date is not None for t in self.open_positions):
            raise ValueError("open_positions must be open")
        return self


class ExperimentDetail(ExperimentSummary):
    """The full journey — this is the screen the whole product is about."""
    model_config = ConfigDict(extra="forbid")

    question: str = Field(..., description="The question the agent asked itself.")
    rationale: str
    rulebook: Rulebook
    stage: LoopStage
    story: list[StoryLine] = Field(..., min_length=1, description="The loop story, in beat order.")
    facts: list[Fact] = Field(..., description="Every fact referenced anywhere in this payload.")
    evidence: list[Evidence] = Field(default_factory=list)
    virtual_book: Optional[VirtualBook] = None
    change_log: list[ChangeLogEntry] = Field(
        default_factory=list,
        description="Append-only L1–L3 history (an L4 change is recorded, never authored here).",
    )
    post_mortem: Optional[PostMortem] = None
    triggers: list[Trigger] = Field(default_factory=list)
    llm_usage: Optional[LlmUsage] = None
    next_review: Optional[Trigger] = Field(
        None, description="The condition that will next wake the LLM for this experiment."
    )
    disclosure: str = RESEARCH_DISCLOSURE

    @model_validator(mode="after")
    def _integrity(self) -> "ExperimentDetail":
        if self.status == ExperimentStatus.died and self.post_mortem is None:
            raise ValueError("a died experiment MUST publish a post-mortem")
        if self.status != ExperimentStatus.died and self.post_mortem is not None:
            raise ValueError("only a died experiment carries a post-mortem")
        known = {f.id for f in self.facts}
        used: set[str] = set()
        for line in self.story:
            used |= set(line.fact_refs)
        for ev in self.evidence:
            used |= set(ev.fact_refs)
            if ev.interpretation:
                used |= set(ev.interpretation.fact_refs)
        for trg in self.triggers:
            used |= set(trg.fact_refs)
        if self.next_review:
            used |= set(self.next_review.fact_refs)
        if self.post_mortem:
            used |= set(self.post_mortem.summary.fact_refs)
        dangling = used - known
        if dangling:
            raise ValueError(f"unresolvable fact refs: {sorted(dangling)}")
        seqs = [c.seq for c in self.change_log]
        if seqs != sorted(seqs) or len(set(seqs)) != len(seqs):
            raise ValueError("change_log must be append-only: strictly increasing seq")
        return self


class LoopResponse(BaseModel):
    """GET /api/pathfinder/loop — the live loop, AS A STORY, plus its supporting facts."""
    model_config = ConfigDict(extra="forbid")

    as_of: datetime
    cycle_id: str = Field(..., pattern=r"^cyc_[a-z0-9_\-]+$")
    stage: LoopStage
    constitution_version: str
    active_experiment_id: Optional[str] = None
    trigger: Optional[Trigger] = Field(None, description="What woke the LLM for this cycle.")
    story: list[StoryLine] = Field(
        ...,
        min_length=1,
        description="The beats in order: noticed → hypothesis → experiment → outcome → learning → next.",
    )
    facts: list[Fact]
    counts: dict[ExperimentStatus, int] = Field(
        ..., description="Experiment count by status — the edge-discovery pipeline."
    )
    llm_usage: Optional[LlmUsage] = None
    disclosure: str = RESEARCH_DISCLOSURE

    @model_validator(mode="after")
    def _resolvable(self) -> "LoopResponse":
        known = {f.id for f in self.facts}
        used: set[str] = set()
        for line in self.story:
            used |= set(line.fact_refs)
        if self.trigger:
            used |= set(self.trigger.fact_refs)
        dangling = used - known
        if dangling:
            raise ValueError(f"unresolvable fact refs: {sorted(dangling)}")
        return self


class ExperimentListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of: datetime
    status_filter: Optional[ExperimentStatus] = None
    count: int = Field(..., ge=0)
    items: list[ExperimentSummary]
    disclosure: str = RESEARCH_DISCLOSURE


class Learning(BaseModel):
    """Something Pathfinder now believes, and what it is applied in."""
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., pattern=r"^lrn_[a-z0-9_]+$")
    statement: StoryLine
    level: LearningLevel
    learned_at: datetime
    from_experiments: list[str] = Field(..., min_length=1)
    evidence_refs: list[str] = Field(default_factory=list)
    fact_refs: list[str] = Field(default_factory=list)
    confidence: Confidence
    n: Optional[int] = Field(None, ge=0)
    sample_flag: SampleFlag = SampleFlag.unknown
    applied_in: list[str] = Field(
        default_factory=list, description="Strategy versions this learning changed."
    )

    @model_validator(mode="after")
    def _derive(self) -> "Learning":
        self.sample_flag = sample_flag_for(self.n)
        return self


class NextTest(BaseModel):
    """What the agent decided to test next, and why now."""
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., pattern=r"^nxt_[a-z0-9_]+$")
    question: str
    why_now: StoryLine
    triggered_by: Optional[Trigger] = None
    planned_test: str = Field(..., description="The DETERMINISTIC test that will be run.")
    blocked_by: Optional[str] = None
    queued_experiment_id: Optional[str] = None
    fact_refs: list[str] = Field(default_factory=list)


class LearningsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of: datetime
    constitution_version: str
    learned: list[Learning]
    testing_next: list[NextTest]
    facts: list[Fact]
    disclosure: str = RESEARCH_DISCLOSURE

    @model_validator(mode="after")
    def _resolvable(self) -> "LearningsResponse":
        known = {f.id for f in self.facts}
        used: set[str] = set()
        for lr in self.learned:
            used |= set(lr.fact_refs) | set(lr.statement.fact_refs)
        for nx in self.testing_next:
            used |= set(nx.fact_refs) | set(nx.why_now.fact_refs)
            if nx.triggered_by:
                used |= set(nx.triggered_by.fact_refs)
        dangling = used - known
        if dangling:
            raise ValueError(f"unresolvable fact refs: {sorted(dangling)}")
        return self


# ── Errors (guarded — never leak internals) ──────────────────────────────────

class ErrorBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: str = Field(..., description="Stable machine code, e.g. 'not_found'.")
    message: str = Field(..., description="Safe, human-readable. Never a stack trace or SQL.")
    request_id: Optional[str] = None


class ErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    error: ErrorBody


ExperimentIdPath = Annotated[
    str, Field(pattern=r"^exp_[a-z0-9_]+$", description="Experiment id, e.g. 'exp_0007'.")
]
