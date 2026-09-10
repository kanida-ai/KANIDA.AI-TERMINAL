"""
Usefulness — the deterministic ranking and the publication threshold.

Spec: rank by usefulness, novelty, evidence strength and trader relevance; publish only
what clears the usefulness threshold; NO minimum count, NO padding. Every input below is
computed by the engine; the LLM may only veto a card that already cleared the threshold
(narrate.Selector) — it can never promote one that did not.

    total = 0.30 * evidence_strength + 0.20 * novelty + 0.30 * trader_relevance + 0.20 * magnitude

    evidence_strength   sqrt(min(1, n/100)) * (0.5 + 0.5 * min(1, |z|/3))
                        n = primary sample, z = binomial z of the base rate vs chance
    novelty             1.0 never published in the lookback; 0.6 same subject, different
                        decision; 0.25 same (template, subject, decision) — plus the regime
                        state for the market card, so a changed regime is news again
    trader_relevance    template weight * decision weight
    magnitude           how unusual today's observation is (template-specific, 0..1)

FOUNDER INPUT: the weights, the decision weights and the threshold (config) are the
prototype-era defaults and are marked for review.
"""
from __future__ import annotations

from typing import Callable

import numpy as np

from ..schemas import Decision, UsefulnessScore
from .config import RANKING_VERSION, ResearchConfig
from .library import CardDraft, QuestionTemplate

WEIGHTS = {"evidence": 0.30, "novelty": 0.20, "relevance": 0.30, "magnitude": 0.20}

#: FOUNDER INPUT — how actionable each decision is for a trader. "Do not chase" is a
#: decision a trader can act on, so NO TRADE / REJECT rank close to a call.
DECISION_WEIGHT = {
    Decision.virtual_long: 1.0, Decision.virtual_short: 1.0, Decision.reject: 0.9,
    Decision.no_trade: 0.9, Decision.new_experiment: 0.7, Decision.watch: 0.8,
    Decision.continue_: 0.7,
}

NoveltyFn = Callable[[str], float]      # novelty_key -> 0..1


def novelty_key(draft: CardDraft, regime_state: str) -> str:
    key = f"{draft.template_id}|{draft.subject}|{draft.decision.value}"
    if draft.template_id == "market_regime":
        key += f"|{regime_state}"
    return key


def evidence_strength(n: int, z: float) -> float:
    sample = float(np.sqrt(min(1.0, n / 100.0))) if n > 0 else 0.0
    clarity = min(1.0, abs(z) / 3.0)
    return sample * (0.5 + 0.5 * clarity)


def score(draft: CardDraft, template: QuestionTemplate, *, novelty: float,
          cfg: ResearchConfig) -> UsefulnessScore:
    ev = evidence_strength(draft.n, draft.evidence_z)
    rel = template.trader_relevance * DECISION_WEIGHT[draft.decision]
    mag = float(min(1.0, max(0.0, draft.magnitude)))
    nov = float(min(1.0, max(0.0, novelty)))
    total = (WEIGHTS["evidence"] * ev + WEIGHTS["novelty"] * nov
             + WEIGHTS["relevance"] * rel + WEIGHTS["magnitude"] * mag)
    return UsefulnessScore(
        total=round(min(1.0, total), 4), evidence_strength=round(ev, 4), novelty=round(nov, 4),
        trader_relevance=round(rel, 4), magnitude=round(mag, 4),
        threshold=cfg.usefulness_threshold, version=RANKING_VERSION,
    )
