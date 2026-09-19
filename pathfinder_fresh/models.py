from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


Grade = Literal["RIGHT", "WRONG", "INCONCLUSIVE", "PENDING"]
Provenance = Literal["same_stock", "similar_stocks", "sector", "whole_market"]


@dataclass(frozen=True)
class Policy:
    history_start: str = "2018-01-01"
    cost_bps_round_trip: float = 30.0
    publication_threshold: float = 62.0
    minimum_sample: int = 40
    null_trials: int = 400
    max_published_per_edition: int = 12
    max_experiments_per_edition: int = 2
    virtual_capital_per_experiment: float = 100_000.0
    max_hypothesis_versions: int = 3
    minimum_trials_per_version: int = 5
    minimum_oos_trials_for_promotion: int = 20
    incumbent_net_return: float = 0.0
    incumbent_max_drawdown: float = 0.10
    public_constituents_require_ra_review: bool = True

    @property
    def cost_rate(self) -> float:
        return self.cost_bps_round_trip / 10_000.0


@dataclass(frozen=True)
class GradingRule:
    kind: Literal["directional", "relative", "absolute_move", "no_trade"]
    horizon_sessions: int
    direction: Literal["LONG", "SHORT", "NONE"]
    right_if: str
    wrong_if: str
    inconclusive_if: str
    hurdle: float


@dataclass
class Evidence:
    provenance: Provenance
    sample_size: int
    period_start: str
    period_end: str
    regime: str
    comparison_group: str
    cost_hurdle: float
    observed_metric: float
    comparison_metric: float
    net_edge: float
    win_rate: float | None
    null_calibrated_p: float
    outcome_metric: str
    data_source: str


@dataclass
class EvaluationPlan:
    kind: Literal["directional", "relative", "absolute_move", "no_trade"]
    symbols: list[str]
    benchmark_symbols: list[str]
    direction: Literal["LONG", "SHORT", "NONE"]
    horizon_sessions: int
    hurdle: float
    expected_return: float
    internal_subject: str


@dataclass
class Finding:
    finding_id: str
    edition_date: str
    template_id: str
    template_version: int
    family: str
    question: str
    title: str
    observation: str
    history: str
    decision: str
    why_now: str
    usefulness_score: float
    novelty_score: float
    evidence_score: float
    trader_relevance_score: float
    publication_score: float
    evidence: Evidence
    grading_rule: GradingRule
    evaluation: EvaluationPlan
    experiment_eligible: bool
    public_subject: str
    compliance_label: str
    internal: dict[str, Any] = field(default_factory=dict)

    def as_dict(self, public: bool = False) -> dict[str, Any]:
        value = asdict(self)
        if public:
            value.pop("internal", None)
            if self.compliance_label == "RA_REVIEW_REQUIRED":
                value["evaluation"]["symbols"] = []
                value["evaluation"]["benchmark_symbols"] = []
                value["evaluation"]["internal_subject"] = "Withheld pending RA review"
        return value


@dataclass
class Edition:
    as_of: str
    scanned_symbols: int
    candidate_count: int
    published: list[Finding]
    suppressed_count: int
    usefulness_threshold: float
    market_regime: str
    data_quality: dict[str, Any]

    def as_dict(self, public: bool = False) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "scanned_symbols": self.scanned_symbols,
            "candidate_count": self.candidate_count,
            "published_count": len(self.published),
            "suppressed_count": self.suppressed_count,
            "usefulness_threshold": self.usefulness_threshold,
            "market_regime": self.market_regime,
            "data_quality": self.data_quality,
            "published": [finding.as_dict(public=public) for finding in self.published],
        }
