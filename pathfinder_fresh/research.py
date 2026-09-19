from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from .market import MarketData
from .models import Evidence, EvaluationPlan, Finding, GradingRule, Policy
from .questions import QuestionTemplate


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return float(max(low, min(high, value)))


def _pct(value: float) -> str:
    return f"{value * 100:+.2f}%"


def _stable_seed(*parts: object) -> int:
    value = "|".join(map(str, parts)).encode("utf-8")
    return int(hashlib.sha256(value).hexdigest()[:16], 16) % (2**32)


def _stable_id(*parts: object, size: int = 20) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode("utf-8")).hexdigest()[:size]


def null_calibrated_p(
    conditional: np.ndarray,
    comparison: np.ndarray,
    trials: int,
    seed: int,
) -> float:
    """Empirical median-difference calibration against matched null samples."""
    conditional = conditional[np.isfinite(conditional)]
    comparison = comparison[np.isfinite(comparison)]
    if len(conditional) < 5 or len(comparison) < 10:
        return 1.0
    observed = abs(float(np.median(conditional) - np.median(comparison)))
    rng = np.random.default_rng(seed)
    sample_size = min(len(conditional), 500)
    extreme = 0
    for _ in range(trials):
        first = rng.choice(comparison, size=sample_size, replace=True)
        second = rng.choice(comparison, size=sample_size, replace=True)
        if abs(float(np.median(first) - np.median(second))) >= observed:
            extreme += 1
    return (extreme + 1.0) / (trials + 1.0)


def edge_calibrated_p(values: np.ndarray, hurdle: float, trials: int, seed: int) -> float:
    """One-sided bootstrap null: the population median equals the cost hurdle."""
    values = values[np.isfinite(values)]
    if len(values) < 5:
        return 1.0
    observed_edge = float(np.median(values) - hurdle)
    if observed_edge <= 0:
        return 1.0
    centered_null = values - np.median(values) + hurdle
    rng = np.random.default_rng(seed)
    sample_size = min(len(values), 2_000)
    extreme = 0
    for _ in range(trials):
        sample = rng.choice(centered_null, size=sample_size, replace=True)
        if float(np.median(sample) - hurdle) >= observed_edge:
            extreme += 1
    return (extreme + 1.0) / (trials + 1.0)


class Researcher:
    def __init__(self, market: MarketData, policy: Policy):
        self.market = market
        self.policy = policy
        self._computations: dict[str, Callable[[QuestionTemplate, pd.Timestamp], Finding | None]] = {
            "market_breadth_tail": self._market_breadth_tail,
            "market_dispersion_spike": self._market_dispersion_spike,
            "sector_laggard_flip": self._sector_laggard_flip,
            "sector_leadership_continuation": self._sector_leadership_continuation,
            "stock_shock_reversal": self._stock_shock_reversal,
            "stock_downside_shock_basket": self._stock_downside_shock_basket,
            "stock_upside_shock_basket": self._stock_upside_shock_basket,
            "stock_volume_surge_reversal": self._stock_volume_surge_reversal,
            "quiet_volume_pressure": self._quiet_volume_pressure,
        }

    def compute(self, template: QuestionTemplate, as_of: str | pd.Timestamp) -> Finding | None:
        day = self.market.resolve_date(str(as_of)[:10])
        computation = self._computations.get(template.computation)
        if computation is None:
            raise ValueError(f"Template {template.template_id} has no reviewed computation")
        return computation(template, day)

    def _history_market(self, day: pd.Timestamp, horizon: int, regime: str) -> pd.DataFrame:
        frame = self.market.market
        return frame[(frame.index < day) & (frame[f"outcome_date_{horizon}"] <= day) & (frame["regime"] == regime)].copy()

    def _history_sector(self, day: pd.Timestamp, horizon: int, regime: str) -> pd.DataFrame:
        frame = self.market.sectors
        return frame[(frame["date"] < day) & (frame[f"outcome_date_{horizon}"] <= day) & (frame["regime"] == regime)].copy()

    def _history_stock(self, day: pd.Timestamp, horizon: int, sector: str) -> pd.DataFrame:
        frame = self.market.bars
        return frame[(frame["date"] < day) & (frame[f"outcome_date_{horizon}"] <= day) & (frame["sector"] == sector)].copy()

    def _grade_rule(self, kind: str, direction: str, horizon: int, hurdle: float) -> GradingRule:
        hurdle_text = f"{hurdle * 100:.2f}%"
        if kind == "absolute_move":
            return GradingRule(
                kind="absolute_move", horizon_sessions=horizon, direction="NONE", hurdle=hurdle,
                right_if=f"Absolute basket move exceeds the frozen {hurdle_text} threshold.",
                wrong_if=f"Absolute basket move is below 75% of the frozen {hurdle_text} threshold.",
                inconclusive_if="Absolute move lands between the Right and Wrong boundaries.",
            )
        if kind == "no_trade":
            return GradingRule(
                kind="no_trade", horizon_sessions=horizon, direction=direction, hurdle=hurdle,
                right_if=f"The rejected trade loses by more than {hurdle_text} after costs.",
                wrong_if=f"The rejected trade wins by more than {hurdle_text} after costs.",
                inconclusive_if=f"The outcome is inside ±{hurdle_text} after costs.",
            )
        basis = "relative return" if kind == "relative" else "net return"
        return GradingRule(
            kind=kind, horizon_sessions=horizon, direction=direction, hurdle=hurdle,
            right_if=f"Direction-adjusted {basis} is above +{hurdle_text}.",
            wrong_if=f"Direction-adjusted {basis} is below -{hurdle_text}.",
            inconclusive_if=f"Direction-adjusted {basis} is inside ±{hurdle_text}.",
        )

    def _build(
        self,
        *,
        template: QuestionTemplate,
        day: pd.Timestamp,
        title: str,
        observation: str,
        history_text: str,
        decision: str,
        why_now: str,
        provenance: str,
        conditional: pd.Series,
        comparison: pd.Series,
        regime: str,
        comparison_group: str,
        outcome_metric: str,
        novelty: float,
        relevance: float,
        grading_kind: str,
        direction: str,
        horizon: int,
        symbols: list[str],
        benchmark_symbols: list[str],
        public_subject: str,
        internal_subject: str,
        compliance_label: str,
        experiment_eligible: bool,
        absolute_metric: bool = False,
        conditional_preoriented: bool = False,
        forced_hurdle: float | None = None,
        internal: dict | None = None,
    ) -> Finding:
        conditional = pd.Series(conditional).dropna().astype(float)
        comparison = pd.Series(comparison).dropna().astype(float)
        observed = float(conditional.median())
        baseline = float(comparison.median()) if len(comparison) else 0.0
        direction_multiplier = -1.0 if direction == "SHORT" else 1.0
        cost = self.policy.cost_rate
        directed = conditional if conditional_preoriented else direction_multiplier * conditional
        net_edge = (observed - baseline if absolute_metric else float(directed.median())) - (0.0 if absolute_metric else cost)
        win_rate = None if absolute_metric else float((directed > cost).mean())
        if absolute_metric:
            p_value = null_calibrated_p(
                conditional.to_numpy(), comparison.to_numpy(), self.policy.null_trials,
                _stable_seed(template.template_id, day.date()),
            )
        else:
            p_value = edge_calibrated_p(
                directed.to_numpy(), cost, self.policy.null_trials,
                _stable_seed(template.template_id, day.date()),
            )
        evidence_score = _clamp(12 + 17 * math.log10(max(len(conditional), 1)) + 30 * (1 - p_value))
        ratio = abs(net_edge) / max(cost, 0.0001)
        usefulness = _clamp(34 + 24 * min(ratio, 2.0) + (8 if len(conditional) >= 100 else 0))
        publication = 0.36 * usefulness + 0.28 * evidence_score + 0.18 * novelty + 0.18 * relevance
        hurdle = forced_hurdle if forced_hurdle is not None else cost
        source_name = Path(self.market.db_path).name
        finding_id = _stable_id(day.strftime("%Y-%m-%d"), template.template_id, template.version, internal_subject, direction)
        evidence = Evidence(
            provenance=provenance,
            sample_size=int(len(conditional)),
            period_start=(conditional.index.min().strftime("%Y-%m-%d") if isinstance(conditional.index, pd.DatetimeIndex) else self.policy.history_start),
            period_end=(conditional.index.max().strftime("%Y-%m-%d") if isinstance(conditional.index, pd.DatetimeIndex) else day.strftime("%Y-%m-%d")),
            regime=regime,
            comparison_group=comparison_group,
            cost_hurdle=cost,
            observed_metric=observed,
            comparison_metric=baseline,
            net_edge=net_edge,
            win_rate=win_rate,
            null_calibrated_p=p_value,
            outcome_metric=outcome_metric,
            data_source=source_name,
        )
        evaluation = EvaluationPlan(
            kind=grading_kind,
            symbols=symbols,
            benchmark_symbols=benchmark_symbols,
            direction=direction,
            horizon_sessions=horizon,
            hurdle=hurdle,
            expected_return=observed,
            internal_subject=internal_subject,
        )
        if experiment_eligible and p_value > 0.10:
            experiment_eligible = False
            decision = "NO VIRTUAL DEPLOYMENT — NULL CALIBRATION DID NOT CLEAR THE 10% GATE"
            grading_kind = "no_trade"
        return Finding(
            finding_id=finding_id,
            edition_date=day.strftime("%Y-%m-%d"),
            template_id=template.template_id,
            template_version=template.version,
            family=template.family,
            question=template.question,
            title=title,
            observation=observation,
            history=history_text,
            decision=decision,
            why_now=why_now,
            usefulness_score=round(usefulness, 2),
            novelty_score=round(novelty, 2),
            evidence_score=round(evidence_score, 2),
            trader_relevance_score=round(relevance, 2),
            publication_score=round(publication, 2),
            evidence=evidence,
            grading_rule=self._grade_rule(grading_kind, direction, horizon, hurdle),
            evaluation=evaluation,
            experiment_eligible=experiment_eligible,
            public_subject=public_subject,
            compliance_label=compliance_label,
            internal=internal or {},
        )

    def _market_breadth_tail(self, template: QuestionTemplate, day: pd.Timestamp) -> Finding | None:
        horizon = int(template.parameters["horizon_sessions"])
        tail = float(template.parameters["tail_percentile"])
        current = self.market.market.loc[day]
        percentile = float(current["breadth_percentile"])
        if tail < percentile < 1 - tail:
            return None
        regime = str(current["regime"])
        history = self._history_market(day, horizon, regime)
        lower_tail = percentile <= tail
        mask = history["breadth_percentile"] <= tail if lower_tail else history["breadth_percentile"] >= 1 - tail
        conditional = history.loc[mask, f"fwd_{horizon}"]
        comparison = history[f"fwd_{horizon}"]
        if len(conditional.dropna()) < self.policy.minimum_sample:
            return None
        median = float(conditional.median())
        direction = "LONG" if median >= 0 else "SHORT"
        directed_net = abs(median) - self.policy.cost_rate
        eligible = directed_net > 0 and len(conditional) >= self.policy.minimum_sample
        decision = f"VIRTUAL {direction} TEST" if eligible else "NO TRADE — BELOW EVIDENCE GATE"
        breadth = float(current["breadth"])
        tone = "only" if lower_tail else "fully"
        return self._build(
            template=template, day=day,
            title="Breadth has moved into a historical tail",
            observation=f"{tone.capitalize()} {breadth:.0%} of the Nifty 500 universe closed higher; this sits in the {'bottom' if lower_tail else 'top'} 15% of observed breadth.",
            history_text=f"In the same {regime} regime, {len(conditional.dropna())} comparable breadth sessions had a median next-week market return of {_pct(median)}.",
            decision=decision,
            why_now="Breadth is a market-wide condition, so it belongs at the front of the edition when its evidence clears the usefulness gate.",
            provenance="whole_market", conditional=conditional, comparison=comparison, regime=regime,
            comparison_group=f"All other Nifty 500 breadth observations in {regime}",
            outcome_metric=f"Equal-weight market return from next open through session +{horizon}",
            novelty=max(percentile, 1 - percentile) * 100, relevance=96,
            grading_kind="directional" if eligible else "no_trade", direction=direction, horizon=horizon,
            symbols=self.market.liquid_symbols(day, limit=10), benchmark_symbols=[],
            public_subject="Nifty 500 market breadth", internal_subject="Nifty 500 liquid market basket",
            compliance_label="RESEARCH_EXPERIMENT_NOT_RECOMMENDATION", experiment_eligible=eligible,
            internal={"breadth": breadth, "breadth_percentile": percentile, "tail": "lower" if lower_tail else "upper"},
        )

    def _market_dispersion_spike(self, template: QuestionTemplate, day: pd.Timestamp) -> Finding | None:
        horizon = int(template.parameters["horizon_sessions"])
        threshold = float(template.parameters["tail_percentile"])
        current = self.market.market.loc[day]
        percentile = float(current["dispersion_percentile"])
        if percentile < threshold:
            return None
        regime = str(current["regime"])
        history = self._history_market(day, horizon, regime)
        conditional = history.loc[history["dispersion_percentile"] >= threshold, f"fwd_{horizon}"].abs()
        comparison = history[f"fwd_{horizon}"].abs()
        if len(conditional.dropna()) < self.policy.minimum_sample:
            return None
        observed, baseline = float(conditional.median()), float(comparison.median())
        hurdle = max(self.policy.cost_rate, baseline * 1.25)
        decision = "WATCH FOR EXPANSION" if observed > hurdle else "NO EDGE — DISPERSION ALONE IS NOT ENOUGH"
        return self._build(
            template=template, day=day,
            title="Stock-to-stock dispersion is unusually wide",
            observation=f"Cross-sectional dispersion reached the {percentile:.0%} historical percentile even though the index-level move can hide it.",
            history_text=f"Comparable high-dispersion sessions produced a median absolute market move of {_pct(observed)} over five sessions versus {_pct(baseline)} normally.",
            decision=decision,
            why_now="Wide dispersion changes the opportunity set, but this template does not infer direction or publish a trade basket.",
            provenance="whole_market", conditional=conditional, comparison=comparison, regime=regime,
            comparison_group=f"All Nifty 500 sessions in {regime}",
            outcome_metric=f"Absolute equal-weight market return from next open through session +{horizon}",
            novelty=percentile * 100, relevance=88, grading_kind="absolute_move", direction="NONE", horizon=horizon,
            symbols=self.market.liquid_symbols(day, limit=20), benchmark_symbols=[],
            public_subject="Nifty 500 dispersion", internal_subject="Nifty 500 liquid market basket",
            compliance_label="RESEARCH_ONLY_NO_USER_EXECUTION", experiment_eligible=False,
            absolute_metric=True, forced_hurdle=hurdle,
            internal={"dispersion": float(current["dispersion"]), "dispersion_percentile": percentile},
        )

    def _sector_laggard_flip(self, template: QuestionTemplate, day: pd.Timestamp) -> Finding | None:
        horizon = int(template.parameters["horizon_sessions"])
        leaders = int(template.parameters["leader_count"])
        current = self.market.sectors[(self.market.sectors["date"] == day) & (self.market.sectors["member_count"] >= 5)]
        candidates = current[(current["daily_rank"] <= leaders) & (current["past_20_rank"] >= 0.75)]
        if candidates.empty:
            return None
        row = candidates.sort_values(["daily_rank", "past_20_rank"], ascending=[True, False]).iloc[0]
        sector = str(row["sector"])
        regime = str(row["regime"])
        history = self._history_sector(day, horizon, regime)
        mask = (history["member_count"] >= 5) & (history["daily_rank"] <= leaders) & (history["past_20_rank"] >= 0.75)
        conditional = history.loc[mask].set_index("date")[f"relative_fwd_{horizon}"]
        comparison = history[history["member_count"] >= 5].set_index("date")[f"relative_fwd_{horizon}"]
        if len(conditional.dropna()) < self.policy.minimum_sample:
            return None
        median = float(conditional.median())
        eligible = median > self.policy.cost_rate and len(conditional) >= self.policy.minimum_sample
        decision = "OPEN VIRTUAL SECTOR-ROTATION EXPERIMENT" if eligible else "WATCH — ROTATION DOES NOT CLEAR COSTS"
        return self._build(
            template=template, day=day,
            title=f"A recent laggard has flipped into sector leadership",
            observation=f"{sector} ranked among today's top {leaders} sectors after sitting in the bottom quartile over the previous month.",
            history_text=f"Across sectors, {len(conditional.dropna())} comparable flips delivered median five-session excess return of {_pct(median)} versus the equal-weight Nifty 500.",
            decision=decision,
            why_now="The condition is a rotation, not merely a strong day: recent relative weakness is part of the frozen template.",
            provenance="sector", conditional=conditional, comparison=comparison, regime=regime,
            comparison_group="All liquid sectors in the same market regime, benchmarked to equal-weight Nifty 500",
            outcome_metric=f"Sector basket excess return from next open through session +{horizon}",
            novelty=90, relevance=92, grading_kind="relative" if eligible else "no_trade", direction="LONG", horizon=horizon,
            symbols=self.market.liquid_symbols(day, sector=sector, limit=10), benchmark_symbols=self.market.nifty50_symbols(day),
            public_subject=sector, internal_subject=f"Top-liquidity {sector} basket",
            compliance_label="RESEARCH_EXPERIMENT_NOT_RECOMMENDATION", experiment_eligible=eligible,
            internal={"sector": sector, "daily_rank": float(row["daily_rank"]), "past_20_rank_percentile": float(row["past_20_rank"])},
        )

    def _sector_leadership_continuation(self, template: QuestionTemplate, day: pd.Timestamp) -> Finding | None:
        horizon = int(template.parameters["horizon_sessions"])
        current = self.market.sectors[(self.market.sectors["date"] == day) & (self.market.sectors["member_count"] >= 5)].copy()
        if current.empty:
            return None
        row = current.sort_values("past_5", ascending=False).iloc[0]
        if pd.isna(row["past_5"]):
            return None
        sector = str(row["sector"])
        regime = str(row["regime"])
        history = self._history_sector(day, horizon, regime)
        history = history[history["member_count"] >= 5].copy()
        history["past_5_leader"] = history.groupby("date", observed=True)["past_5"].rank(method="min", ascending=False)
        conditional = history[history["past_5_leader"] == 1].set_index("date")[f"relative_fwd_{horizon}"]
        comparison = history.set_index("date")[f"relative_fwd_{horizon}"]
        if len(conditional.dropna()) < self.policy.minimum_sample:
            return None
        median = float(conditional.median())
        eligible = median > self.policy.cost_rate and len(conditional) >= self.policy.minimum_sample
        decision = "CONTINUE WITH A VIRTUAL THEME TEST" if eligible else "NO TRADE — LEADERSHIP IS ALREADY PRICED"
        return self._build(
            template=template, day=day,
            title=f"{sector} has the strongest one-week sector trend",
            observation=f"The sector's equal-weight basket gained {_pct(float(row['past_5']))} over the prior five sessions, the best current sector trend.",
            history_text=f"Prior five-session sector leaders produced a median next-week excess return of {_pct(median)} across {len(conditional.dropna())} same-regime cases.",
            decision=decision,
            why_now="Leadership is measured before today's close and compared with every other liquid sector, not selected by narrative.",
            provenance="sector", conditional=conditional, comparison=comparison, regime=regime,
            comparison_group="All sector/session observations in the same regime, benchmarked to equal-weight Nifty 500",
            outcome_metric=f"Sector basket excess return from next open through session +{horizon}",
            novelty=78, relevance=90, grading_kind="relative" if eligible else "no_trade", direction="LONG", horizon=horizon,
            symbols=self.market.liquid_symbols(day, sector=sector, limit=10), benchmark_symbols=self.market.nifty50_symbols(day),
            public_subject=sector, internal_subject=f"Top-liquidity {sector} basket",
            compliance_label="RESEARCH_EXPERIMENT_NOT_RECOMMENDATION", experiment_eligible=eligible,
            internal={"sector": sector, "past_5": float(row["past_5"])},
        )

    def _stock_shock_reversal(self, template: QuestionTemplate, day: pd.Timestamp) -> Finding | None:
        horizon = int(template.parameters["horizon_sessions"])
        threshold = float(template.parameters["z_threshold"])
        current = self.market.current_bars(day).dropna(subset=["return_z_60", "ret_1"])
        if current.empty:
            return None
        row = current.loc[current["return_z_60"].abs().idxmax()]
        z_value = float(row["return_z_60"])
        if abs(z_value) < threshold:
            return None
        symbol, sector = str(row["symbol"]), str(row["sector"])
        history = self._history_stock(day, horizon, sector)
        shock_up = z_value > 0
        mask = history["return_z_60"] >= threshold if shock_up else history["return_z_60"] <= -threshold
        direction_multiplier = -1.0 if shock_up else 1.0
        conditional = (history.loc[mask].set_index("date")[f"fwd_{horizon}"] * direction_multiplier)
        comparison = (history.set_index("date")[f"fwd_{horizon}"] * direction_multiplier)
        if len(conditional.dropna()) < self.policy.minimum_sample:
            return None
        median = float(conditional.median())
        direction = "SHORT" if shock_up else "LONG"
        eligible = median > self.policy.cost_rate and len(conditional) >= self.policy.minimum_sample
        decision = f"VIRTUAL {direction} REVERSAL TEST" if eligible else "NO TRADE — THE SHOCK IS INTERESTING, NOT ACTIONABLE"
        return self._build(
            template=template, day=day,
            title="A stock move has broken far outside its normal range",
            observation=f"A {sector} constituent moved {_pct(float(row['ret_1']))} today, a {z_value:+.1f}σ event versus its own prior 60-session distribution.",
            history_text=f"Across same-sector stocks, {len(conditional.dropna())} comparable {'upside' if shock_up else 'downside'} shocks had a median three-session reversal return of {_pct(median)} before costs.",
            decision=decision,
            why_now="The public card stays at sector level until RA review; the internal evidence preserves the exact stock and comparison set.",
            provenance="similar_stocks", conditional=conditional, comparison=comparison,
            regime=self.market.regime(day), comparison_group=f"All {sector} stocks with a same-direction ≥{threshold:.1f}σ shock",
            outcome_metric=f"Direction-adjusted stock return from next open through session +{horizon}",
            novelty=min(100, abs(z_value) / 4 * 100), relevance=82,
            grading_kind="directional" if eligible else "no_trade", direction=direction, horizon=horizon,
            symbols=[symbol], benchmark_symbols=[], public_subject=f"{sector} stock anomaly", internal_subject=symbol,
            compliance_label="RA_REVIEW_REQUIRED", experiment_eligible=eligible,
            conditional_preoriented=True,
            internal={"symbol": symbol, "company": str(row["company"]), "sector": sector, "z_score": z_value},
        )

    def _stock_shock_basket(self, template: QuestionTemplate, day: pd.Timestamp, direction: str) -> Finding | None:
        horizon = int(template.parameters["horizon_sessions"])
        threshold = float(template.parameters["z_threshold"])
        maximum = int(template.parameters["max_basket_size"])
        current = self.market.current_bars(day).dropna(subset=["return_z_60", "ret_1"])
        if direction == "LONG":
            signals = current[current["return_z_60"] <= threshold].sort_values("return_z_60").head(maximum)
            event_description = "downside"
            history_mask_operator = lambda values: values <= threshold
            direction_multiplier = 1.0
        else:
            signals = current[current["return_z_60"] >= threshold].sort_values("return_z_60", ascending=False).head(maximum)
            event_description = "upside"
            history_mask_operator = lambda values: values >= threshold
            direction_multiplier = -1.0
        if signals.empty:
            return None
        regime = self.market.regime(day)
        history = self.market.bars[
            (self.market.bars["date"] < day)
            & (self.market.bars[f"outcome_date_{horizon}"] <= day)
        ].copy()
        history = history[history["date"].map(self.market.market["regime"]) == regime]
        mask = history_mask_operator(history["return_z_60"])
        conditional = history.loc[mask].set_index("date")[f"fwd_{horizon}"] * direction_multiplier
        comparison = history.set_index("date")[f"fwd_{horizon}"] * direction_multiplier
        if len(conditional.dropna()) < self.policy.minimum_sample:
            return None
        median = float(conditional.median())
        eligible = median > self.policy.cost_rate and len(conditional) >= self.policy.minimum_sample
        decision = f"VIRTUAL {direction} RESEARCH BASKET" if eligible else "WATCHLIST ONLY — HISTORICAL EDGE DID NOT CLEAR COSTS"
        symbols = signals["symbol"].tolist()
        return self._build(
            template=template, day=day,
            title=f"A cross-stock {event_description}-shock basket has formed",
            observation=(
                f"{len(symbols)} stocks closed beyond the reviewed {abs(threshold):.1f}σ {event_description} threshold; "
                "the basket contains only stocks that independently met the frozen rule."
            ),
            history_text=(
                f"Across {len(conditional.dropna()):,} same-regime stock shocks, the direction-adjusted median "
                f"three-session return was {_pct(median)} before the {_pct(self.policy.cost_rate)} cost hurdle."
            ),
            decision=decision,
            why_now="This is a computed cross-stock condition with fixed membership rules, not an analyst-selected list.",
            provenance="similar_stocks", conditional=conditional, comparison=comparison, regime=regime,
            comparison_group=f"All Nifty 500 stock/session observations in {regime}",
            outcome_metric=f"Equal-weight, direction-adjusted basket return from next open through session +{horizon}",
            novelty=min(100, 72 + len(symbols) * 2), relevance=95,
            grading_kind="directional" if eligible else "no_trade", direction=direction, horizon=horizon,
            symbols=symbols, benchmark_symbols=[],
            public_subject=f"Nifty 500 {event_description}-shock basket", internal_subject="|".join(symbols),
            compliance_label="RA_REVIEW_REQUIRED", experiment_eligible=eligible,
            conditional_preoriented=True,
            internal={
                "symbols": symbols,
                "z_scores": {str(row.symbol): float(row.return_z_60) for row in signals.itertuples()},
                "threshold": threshold,
                "selection": f"Most extreme {maximum} qualifying stocks",
            },
        )

    def _stock_downside_shock_basket(self, template: QuestionTemplate, day: pd.Timestamp) -> Finding | None:
        return self._stock_shock_basket(template, day, "LONG")

    def _stock_upside_shock_basket(self, template: QuestionTemplate, day: pd.Timestamp) -> Finding | None:
        return self._stock_shock_basket(template, day, "SHORT")

    def _stock_volume_surge_reversal(self, template: QuestionTemplate, day: pd.Timestamp) -> Finding | None:
        horizon = int(template.parameters["horizon_sessions"])
        minimum_return = float(template.parameters["minimum_return"])
        volume_multiple = float(template.parameters["volume_multiple"])
        maximum = int(template.parameters["max_basket_size"])
        current = self.market.current_bars(day).dropna(subset=["volume_ratio", "ret_1"])
        signals = current[
            (current["volume_ratio"] >= volume_multiple) & (current["ret_1"] >= minimum_return)
        ].sort_values(["volume_ratio", "ret_1"], ascending=False).head(maximum)
        if signals.empty:
            return None
        regime = self.market.regime(day)
        history = self.market.bars[
            (self.market.bars["date"] < day)
            & (self.market.bars[f"outcome_date_{horizon}"] <= day)
        ].copy()
        history = history[history["date"].map(self.market.market["regime"]) == regime]
        mask = (history["volume_ratio"] >= volume_multiple) & (history["ret_1"] >= minimum_return)
        conditional = -history.loc[mask].set_index("date")[f"fwd_{horizon}"]
        comparison = -history.set_index("date")[f"fwd_{horizon}"]
        if len(conditional.dropna()) < self.policy.minimum_sample:
            return None
        median = float(conditional.median())
        eligible = median > self.policy.cost_rate
        symbols = signals["symbol"].tolist()
        decision = "VIRTUAL SHORT RESEARCH BASKET" if eligible else "WATCHLIST ONLY — REVERSAL DID NOT CLEAR COSTS"
        return self._build(
            template=template, day=day,
            title="A high-volume upside-reversal basket has formed",
            observation=(
                f"{len(symbols)} stocks entered the basket after rising at least {minimum_return:.0%} on "
                f"at least {volume_multiple:.1f}× prior median volume."
            ),
            history_text=(
                f"Across {len(conditional.dropna()):,} same-regime events, short-side five-session return "
                f"had a median of {_pct(median)} before the {_pct(self.policy.cost_rate)} cost hurdle."
            ),
            decision=decision,
            why_now="Price direction and volume confirmation are both required by the frozen template; basket members are ranked by volume abnormality.",
            provenance="similar_stocks", conditional=conditional, comparison=comparison, regime=regime,
            comparison_group=f"All Nifty 500 stock/session observations in {regime}",
            outcome_metric=f"Equal-weight short basket return from next open through session +{horizon}",
            novelty=min(100, 75 + len(symbols) * 2), relevance=96,
            grading_kind="directional" if eligible else "no_trade", direction="SHORT", horizon=horizon,
            symbols=symbols, benchmark_symbols=[],
            public_subject="Nifty 500 high-volume upside basket", internal_subject="|".join(symbols),
            compliance_label="RA_REVIEW_REQUIRED", experiment_eligible=eligible,
            conditional_preoriented=True,
            internal={
                "symbols": symbols,
                "volume_ratios": {str(row.symbol): float(row.volume_ratio) for row in signals.itertuples()},
                "minimum_return": minimum_return,
                "volume_multiple": volume_multiple,
                "selection": f"Highest volume multiple, capped at {maximum}",
            },
        )

    def _quiet_volume_pressure(self, template: QuestionTemplate, day: pd.Timestamp) -> Finding | None:
        horizon = int(template.parameters["horizon_sessions"])
        volume_multiple = float(template.parameters["volume_multiple"])
        max_abs_return = float(template.parameters["max_abs_return"])
        current = self.market.current_bars(day).dropna(subset=["volume_ratio", "ret_1"])
        candidates = current[(current["volume_ratio"] >= volume_multiple) & (current["ret_1"].abs() <= max_abs_return)]
        if candidates.empty:
            return None
        row = candidates.sort_values("volume_ratio", ascending=False).iloc[0]
        symbol, sector = str(row["symbol"]), str(row["sector"])
        history = self._history_stock(day, horizon, sector)
        mask = (history["volume_ratio"] >= volume_multiple) & (history["ret_1"].abs() <= max_abs_return)
        conditional = history.loc[mask].set_index("date")[f"fwd_{horizon}"].abs()
        comparison = history.set_index("date")[f"fwd_{horizon}"].abs()
        if len(conditional.dropna()) < self.policy.minimum_sample:
            return None
        observed, baseline = float(conditional.median()), float(comparison.median())
        hurdle = max(self.policy.cost_rate, baseline * 1.25)
        decision = "WATCH — DIRECTION IS UNRESOLVED" if observed > hurdle else "NO EDGE — HIGH VOLUME DID NOT LEAD TO EXPANSION"
        return self._build(
            template=template, day=day,
            title="Heavy volume arrived without a decisive price move",
            observation=f"A {sector} constituent traded at {float(row['volume_ratio']):.1f}× its prior 20-session median volume while closing {_pct(float(row['ret_1']))}.",
            history_text=f"Across {len(conditional.dropna())} same-sector quiet-volume events, the median absolute five-session move was {_pct(observed)} versus {_pct(baseline)} normally.",
            decision=decision,
            why_now="The template tests movement magnitude only; it will not convert attention into an invented directional call.",
            provenance="similar_stocks", conditional=conditional, comparison=comparison,
            regime=self.market.regime(day), comparison_group=f"All {sector} stock/session observations",
            outcome_metric=f"Absolute stock return from next open through session +{horizon}",
            novelty=min(100, float(row["volume_ratio"]) / 6 * 100), relevance=76,
            grading_kind="absolute_move", direction="NONE", horizon=horizon,
            symbols=[symbol], benchmark_symbols=[], public_subject=f"{sector} volume anomaly", internal_subject=symbol,
            compliance_label="RA_REVIEW_REQUIRED", experiment_eligible=False,
            absolute_metric=True, forced_hurdle=hurdle,
            internal={"symbol": symbol, "company": str(row["company"]), "sector": sector, "volume_ratio": float(row["volume_ratio"])},
        )
