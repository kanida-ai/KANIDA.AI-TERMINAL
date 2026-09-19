from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class QuestionTemplate:
    template_id: str
    version: int
    family: str
    question: str
    parameters: dict[str, Any]
    computation: str
    evidence_contract: tuple[str, ...]
    experimentable: bool


# This is the only research vocabulary the engine may execute. Adding a question
# means adding a reviewed template and a deterministic implementation with the
# same id. Narrative code cannot create an ad-hoc statistical test.
QUESTION_LIBRARY: tuple[QuestionTemplate, ...] = (
    QuestionTemplate(
        template_id="MKT_BREADTH_TAIL",
        version=1,
        family="market",
        question="When market breadth is this extreme in the current regime, what usually happens over the next week?",
        parameters={"tail_percentile": 0.15, "horizon_sessions": 5},
        computation="market_breadth_tail",
        evidence_contract=("whole_market", "same_regime", "null_calibration", "cost_hurdle"),
        experimentable=True,
    ),
    QuestionTemplate(
        template_id="MKT_DISPERSION_SPIKE",
        version=1,
        family="anomaly",
        question="Does unusually wide stock dispersion precede a larger market move?",
        parameters={"tail_percentile": 0.85, "horizon_sessions": 5},
        computation="market_dispersion_spike",
        evidence_contract=("whole_market", "same_regime", "null_calibration", "cost_hurdle"),
        experimentable=False,
    ),
    QuestionTemplate(
        template_id="SECTOR_LAGGARD_FLIP",
        version=1,
        family="sector",
        question="When a recent sector laggard suddenly leads the market, does the rotation persist?",
        parameters={"lookback_sessions": 20, "leader_count": 2, "horizon_sessions": 5},
        computation="sector_laggard_flip",
        evidence_contract=("sector", "cross_sector_analogs", "null_calibration", "cost_hurdle"),
        experimentable=True,
    ),
    QuestionTemplate(
        template_id="SECTOR_LEADERSHIP_CONTINUATION",
        version=1,
        family="sector",
        question="Does the strongest sector trend continue for another week after costs?",
        parameters={"lookback_sessions": 5, "horizon_sessions": 5},
        computation="sector_leadership_continuation",
        evidence_contract=("sector", "cross_sector_analogs", "null_calibration", "cost_hurdle"),
        experimentable=True,
    ),
    QuestionTemplate(
        template_id="STOCK_SHOCK_REVERSAL",
        version=1,
        family="stock_behaviour",
        question="After a stock moves this far from its own normal range, does it reverse or continue?",
        parameters={"z_threshold": 2.5, "horizon_sessions": 3},
        computation="stock_shock_reversal",
        evidence_contract=("similar_stocks", "same_sector", "null_calibration", "cost_hurdle"),
        experimentable=True,
    ),
    QuestionTemplate(
        template_id="STOCK_DOWNSIDE_SHOCK_BASKET",
        version=1,
        family="stock_behaviour",
        question="Do stocks experiencing a downside move beyond 2.5σ rebound over the next three sessions?",
        parameters={"z_threshold": -2.5, "horizon_sessions": 3, "max_basket_size": 10},
        computation="stock_downside_shock_basket",
        evidence_contract=("similar_stocks", "same_regime", "null_calibration", "cost_hurdle"),
        experimentable=True,
    ),
    QuestionTemplate(
        template_id="STOCK_UPSIDE_SHOCK_BASKET",
        version=1,
        family="stock_behaviour",
        question="Do stocks experiencing an upside move beyond 2.5σ reverse over the next three sessions?",
        parameters={"z_threshold": 2.5, "horizon_sessions": 3, "max_basket_size": 10},
        computation="stock_upside_shock_basket",
        evidence_contract=("similar_stocks", "same_regime", "null_calibration", "cost_hurdle"),
        experimentable=True,
    ),
    QuestionTemplate(
        template_id="STOCK_VOLUME_SURGE_REVERSAL",
        version=1,
        family="stock_behaviour",
        question="After a stock rises at least 2% on twice-normal volume, does the move reverse over the next week?",
        parameters={"minimum_return": 0.02, "volume_multiple": 2.0, "horizon_sessions": 5, "max_basket_size": 10},
        computation="stock_volume_surge_reversal",
        evidence_contract=("similar_stocks", "same_regime", "null_calibration", "cost_hurdle"),
        experimentable=True,
    ),
    QuestionTemplate(
        template_id="QUIET_VOLUME_PRESSURE",
        version=1,
        family="anomaly",
        question="When volume surges but price barely moves, does a larger move follow?",
        parameters={"volume_multiple": 3.0, "max_abs_return": 0.015, "horizon_sessions": 5},
        computation="quiet_volume_pressure",
        evidence_contract=("similar_stocks", "same_sector", "null_calibration", "cost_hurdle"),
        experimentable=False,
    ),
)


def library_manifest() -> list[dict[str, Any]]:
    return [template.__dict__.copy() for template in QUESTION_LIBRARY]
