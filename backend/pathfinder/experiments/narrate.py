"""
The seven-line customer narrative, told from computed facts only.

    I noticed -> I researched -> history showed -> I decided to test with virtual money ->
    what happened -> what I learned and changed -> what I'm testing next

Every beat is a `Narrative` (digit-free; numbers only as `{{fact:…}}` tokens). The engine's
template writes all seven; when a model is configured it may re-write a beat's BODY under the
engine's headline through the S1 gateway contract (`enforce_narrate(strict=True)`: at least
one fact reference, no number word without a fact in its sentence, no digit anywhere), and the
card records who wrote it. A model that cannot satisfy the contract is replaced by the engine
template, visibly (`produced_by = "engine"`). The model never sees a number it may not cite.

Spec addendum 6 holds on every line whoever wrote it: the public card names a THEME and its
evidence; it never names a constituent and never reads like an instruction
(`schemas.PUBLIC_CARD_BANNED_RE`, enforced by the `ExperimentCard` contract).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from ..llm.contracts import enforce_narrate
from ..llm.gateway import Budget, GatewayError, PathfinderLLM
from ..schemas import FACT_REF_RE, PUBLIC_CARD_BANNED_RE, Author, ExperimentBeat, ExperimentStoryLine, Fact

PROMPT_VERSION = "pathfinder.experiments@s2.0.0"
CONSTITUTION_VERSION = "pathfinder_constitution@draft"
#: The gateway's six beats, for the seven lines.
GATEWAY_BEAT = {
    ExperimentBeat.noticed: "noticed", ExperimentBeat.researched: "hypothesis",
    ExperimentBeat.history_showed: "hypothesis", ExperimentBeat.decided: "experiment",
    ExperimentBeat.happened: "outcome", ExperimentBeat.learned: "learning", ExperimentBeat.next: "next",
}


def _refs(text: str) -> list[str]:
    return sorted({m.group("id") for m in FACT_REF_RE.finditer(text)})


@dataclass
class Beat:
    beat: ExperimentBeat
    headline: str
    body: str


class T:
    """Token helper over a fact table: `t('name')` -> `{{fact:<id>}}`; missing facts raise loudly."""

    def __init__(self, facts: list[Fact]) -> None:
        self.by_suffix: dict[str, str] = {}
        self.ids = {f.id for f in facts}
        for f in facts:
            self.by_suffix.setdefault(f.id, f.id)

    def __call__(self, fid: str) -> str:
        if fid not in self.ids:
            raise KeyError(f"fact {fid} is not on the card")
        return "{{fact:" + fid + "}}"

    def has(self, fid: str) -> bool:
        return fid in self.ids


class ExperimentNarrator:
    """Engine template first; a model may rewrite a body under the engine's headline."""

    def __init__(self, llm: Optional[PathfinderLLM] = None, *, provider_name: str = "none",
                 budget: Optional[Budget] = None) -> None:
        self._llm = llm
        self.provider_name = provider_name
        self._budget = budget or Budget(daily_cost_usd=0.0)
        self.failures: list[str] = []
        self.llm_calls = 0

    def lines(self, beats: list[Beat], facts: list[Fact], *, key: str, at: datetime,
              context: dict[str, Any]) -> list[ExperimentStoryLine]:
        out: list[ExperimentStoryLine] = []
        table = [{"id": f.id, "label": f.label, "value": f.value, "unit": f.unit.value, "n": f.n,
                  "sample_flag": f.sample_flag.value} for f in facts]
        for b in beats:
            line = ExperimentStoryLine(beat=b.beat, headline=b.headline, body=b.body, produced_by=Author.engine,
                                       at=at, fact_refs=_refs(b.body))
            if self._llm is not None:
                try:
                    r = self._llm.narrate(
                        beat=GATEWAY_BEAT[b.beat], facts=table,
                        context=[{**context, "beat": b.beat.value, "engine_draft": b.body}],
                        constitution_version=CONSTITUTION_VERSION, prompt_version=PROMPT_VERSION,
                        budget=self._budget, key=f"{key}:{b.beat.value}")
                    self.llm_calls += 1
                    enforce_narrate({"beat": r.beat, "headline": r.headline, "body": r.body, "fact_refs": list(r.fact_refs)},
                                    table, strict=True)
                    # addendum 6 at the source (audit finding 8): a body that reads like a trade instruction is
                    # replaced by the engine beat here, never discovered at read time
                    m = PUBLIC_CARD_BANNED_RE.search(r.body)
                    if m:
                        raise ValueError(f"model body reads like a trade instruction ({m.group(0)!r})")
                    line = ExperimentStoryLine(beat=b.beat, headline=b.headline, body=r.body, produced_by=Author.llm,
                                               model=r.model, prompt_version=PROMPT_VERSION, at=at, fact_refs=list(r.fact_refs))
                except (GatewayError, ValueError) as e:
                    self.failures.append(f"{key}:{b.beat.value}: {e}")
            out.append(line)
        return out


# ── the engine template ───────────────────────────────────────────────────────

def engine_beats(t: T, *, family_id: str, s1: dict[str, str], story: dict[str, Any]) -> list[Beat]:
    """
    Seven digit-free beats. `s1` maps the S1 card's fact NAMES to fact ids on this card;
    `story` carries the story facts' ids (minted by loop.story_facts) and a few engine labels.
    """
    is_dip = family_id == "dip_bounce"
    fell = "fell" if is_dip else "jumped"
    group = "the bounce after a hard one-day fall" if is_dip else ("fading the day's biggest jump" if family_id == "surge_fade"
                                                                 else "riding the day's biggest jump")
    # 1. noticed
    noticed_body = (
        f"{t(s1['subject'])} {fell} {t(s1['move_today'])} on the session this began. Rather than judge one stock on one day, "
        f"the research card measured every stock in the universe after a one-day move of at least {t(s1['threshold'])}: across "
        f"{t(s1['cases'])} past cases the next session was higher {t(s1['hit_1'])} of the time with an expectancy of "
        f"{t(s1['expectancy_1'])} net of costs — as a group, no edge to act on."
    )
    # 2. researched
    researched_body = (
        f"The card's own follow-up question was whether that answer depends on the kind of day the move came on. I evaluated "
        f"{t(story['trials_opening'])} variants of the rule over a closed set of conditions and horizons — every one is on the "
        f"record — and {t(story['passing_opening'])} cleared the gate: expectancy net of costs and at twice the slippage on the "
        f"whole sealed history, an edge over every stock-session in the same window, a day-blocked placebo, and the same edge "
        f"again on the trailing window alone. The bar the best of that many trials must clear to be called significant is "
        f"recorded next to the result."
    )
    # 3. history showed
    hist_body = (
        f"The rule I kept looks at {group} {t(story['condition_label'])}, held for {t(story['horizon'])}. On the whole sealed "
        f"history it returned {t(story['expectancy_net'])} per trade net of costs and slippage across {t(story['n'])} cases on "
        f"{t(story['signal_days'])} sessions, {t(story['hit_rate'])} of them positive, an edge of {t(story['edge'])} over every "
        f"stock-session in the same window; on the trailing window alone it returned {t(story['trailing_expectancy'])} across "
        f"{t(story['trailing_n'])} cases — a persistence check, not an independent holdout: that window was seen for all "
        f"{t(story['trailing_looks'])} variants before this one was chosen. The day-blocked placebo puts the chance of a random "
        f"draw this good at {t(story['placebo_p'])}."
        + (f" On the window before {t(story['discovery_end'])} alone the edge does not survive twice the slippage, and on "
           f"independent signal days it is not yet distinguishable from chance — both are on the record, and both are what the "
           f"virtual test exists to settle." if story.get("advisories_failed") else "")
    )
    # 4. decided
    decided_body = (
        f"I decided to test it with virtual money: {t(story['capital'])} of research capital, at most {t(story['fraction'])} in any "
        f"one position, tracked in periods of {t(story['period_sessions'])} sessions. The grading rule was frozen before the first "
        f"session: right if the period's mean net result clears one standard error above zero, wrong if it falls one below, "
        f"inconclusive inside, and nothing to grade if the rule never fired."
    )
    # 5. happened
    st = story["state_of_period"]
    if st == "graded":
        happened_body = (
            f"Period {t(story['period_no'])} closed {t(story['closed_trades'])} virtual trades from {t(story['period_signal_days'])} "
            f"sessions: {t(story['actual_net'])} per trade against the frozen expectation of {t(story['expected_net'])}. The book "
            f"returned {t(story['book_return'])} over the period with a worst drawdown of {t(story['max_drawdown'])}; the worst "
            f"trade lost {t(story['worst_trade'])} and the best made {t(story['best_trade'])}."
        )
    elif st == "void":
        happened_body = (
            f"Period {t(story['period_no'])} closed no virtual trade: {t(story['void_reason'])}. Nothing was graded and nothing "
            f"was counted; the period is on the record as void."
        )
    elif st == "open":
        happened_body = (
            f"The current period is still running: {t(story['open_positions'])} positions open and {t(story['closed_so_far'])} "
            f"closed so far, the book at {t(story['book_return_now'])} with a worst drawdown so far of {t(story['max_dd_now'])}, "
            f"marked to the latest close."
        )
    else:
        happened_body = "Virtual money is committed; the first period opens on the next session and nothing has happened yet."
    # 6. learned
    if story.get("learned_body"):
        learned_body = story["learned_body"]
    else:
        learned_body = "Nothing to learn yet: no period has completed."
    # 7. next
    next_body = story["next_body"]
    return [
        Beat(ExperimentBeat.noticed, "I noticed a hard one-day move and asked the group question" if is_dip
             else "I noticed a big one-day jump and asked the group question", noticed_body),
        Beat(ExperimentBeat.researched, "I researched whether the answer depends on the day", researched_body),
        Beat(ExperimentBeat.history_showed, "History showed an edge under one condition, with its caveats", hist_body),
        Beat(ExperimentBeat.decided, "I decided to test it with virtual money under a frozen rule", decided_body),
        Beat(ExperimentBeat.happened, story["happened_headline"], happened_body),
        Beat(ExperimentBeat.learned, story["learned_headline"], learned_body),
        Beat(ExperimentBeat.next, story["next_headline"], next_body),
    ]
