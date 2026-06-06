from __future__ import annotations

from collections import defaultdict, deque
from itertools import combinations
from typing import Any, Iterable

from engine.learning.evidence_scoring import PatternStats
from engine.trend.trend_features import TrendFeatureEngine


GroupKey = tuple[str, str, str, str, str]


class SignalPatternMiner:
    """Mines same-bar, short sequential, and cross-timeframe signal patterns."""

    def __init__(self, trend_engine: TrendFeatureEngine, config: dict[str, Any]):
        self.trend_engine = trend_engine
        self.config = config

    def mine(self, rows: Iterable[dict[str, Any]]) -> dict[tuple[Any, ...], PatternStats]:
        grouped: dict[GroupKey, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[
                (
                    row["market"],
                    row["ticker"],
                    row["timeframe"],
                    row["signal_date"],
                    row["bias"],
                )
            ].append(row)

        stats: dict[tuple[Any, ...], PatternStats] = {}
        ordered_groups = sorted(grouped.items(), key=lambda x: x[0])
        self._mine_same_bar(ordered_groups, stats)
        if self.config["pattern_mining"]["sequential"]["enabled"]:
            self._mine_sequences(ordered_groups, stats)
        if self.config["pattern_mining"]["cross_timeframe"]["enabled"]:
            self._mine_cross_timeframe(ordered_groups, stats)
        return stats

    def _mine_same_bar(
        self,
        ordered_groups: list[tuple[GroupKey, list[dict[str, Any]]]],
        stats: dict[tuple[Any, ...], PatternStats],
    ) -> None:
        opts = self.config["pattern_mining"]["same_bar"]
        max_size = int(opts["max_combo_size"])
        max_per_group = int(opts["max_signals_per_group"])
        for group_key, group_rows in ordered_groups:
            market, ticker, timeframe, signal_date, bias = group_key
            context = self._context_tuple(market, ticker, signal_date, timeframe)
            signals = self._top_signals(group_rows, max_per_group)
            outcome = self._group_outcome(group_rows)
            for size in range(1, min(max_size, len(signals)) + 1):
                for combo in combinations(signals, size):
                    pattern_key = " + ".join(combo)
                    key = (
                        market,
                        ticker,
                        timeframe,
                        bias,
                        "same_bar",
                        size,
                        pattern_key,
                        *context,
                    )
                    self._add(stats, key, "same_bar", size, outcome, signal_date, 1.0)

    def _mine_sequences(
        self,
        ordered_groups: list[tuple[GroupKey, list[dict[str, Any]]]],
        stats: dict[tuple[Any, ...], PatternStats],
    ) -> None:
        opts = self.config["pattern_mining"]["sequential"]
        max_signals = int(opts["max_signals_per_bar"])
        max_len = int(opts["max_sequence_length"])
        windows = sorted(int(x) for x in opts["lookback_bars"])
        by_stream: dict[tuple[str, str, str, str], list[tuple[GroupKey, list[dict[str, Any]]]]] = defaultdict(list)
        for item in ordered_groups:
            market, ticker, timeframe, _date, bias = item[0]
            by_stream[(market, ticker, timeframe, bias)].append(item)

        for stream_groups in by_stream.values():
            recent: deque[tuple[str, tuple[str, ...], float, int]] = deque(maxlen=max(windows))
            for group_key, group_rows in sorted(stream_groups, key=lambda x: x[0][3]):
                market, ticker, timeframe, signal_date, bias = group_key
                context = self._context_tuple(market, ticker, signal_date, timeframe)
                signals = tuple(self._top_signals(group_rows, max_signals))
                outcome = self._group_outcome(group_rows)
                if not signals:
                    continue
                for lookback in windows:
                    hist = list(recent)[-lookback + 1 :]
                    if not hist:
                        continue
                    sequence_parts = [self._compact_signature(signals)]
                    for _date, prev_signals, decay_weight, _age in reversed(hist[-(max_len - 1) :]):
                        sequence_parts.append(self._compact_signature(prev_signals))
                    if len(sequence_parts) < 2:
                        continue
                    pattern_key = " <- ".join(sequence_parts)
                    recency_weight = 1.0 + sum(item[2] for item in hist[-(max_len - 1) :])
                    key = (
                        market,
                        ticker,
                        timeframe,
                        bias,
                        "sequence",
                        len(sequence_parts),
                        pattern_key,
                        *context,
                    )
                    self._add(
                        stats,
                        key,
                        "sequence",
                        len(sequence_parts),
                        outcome,
                        signal_date,
                        recency_weight,
                    )
                recent.append((signal_date, signals, 0.70, 1))

    def _mine_cross_timeframe(
        self,
        ordered_groups: list[tuple[GroupKey, list[dict[str, Any]]]],
        stats: dict[tuple[Any, ...], PatternStats],
    ) -> None:
        opts = self.config["pattern_mining"]["cross_timeframe"]
        daily_tf = opts["daily_timeframe"]
        weekly_tf = opts["weekly_timeframe"]
        max_per_group = int(self.config["pattern_mining"]["same_bar"]["max_signals_per_group"])
        for group_key, group_rows in ordered_groups:
            market, ticker, timeframe, signal_date, bias = group_key
            if timeframe != daily_tf:
                continue
            context_full = self.trend_engine.get(market, ticker, signal_date)
            weekly_state = context_full.get("weekly_proxy_state", "weekly_proxy_unknown")
            context = self._context_tuple(market, ticker, signal_date, timeframe)
            signals = self._top_signals(group_rows, max_per_group)
            outcome = self._group_outcome(group_rows)
            for size in range(1, min(2, len(signals)) + 1):
                for combo in combinations(signals, size):
                    pattern_key = f"{weekly_tf}:{weekly_state} + {daily_tf}:{' + '.join(combo)}"
                    key = (
                        market,
                        ticker,
                        timeframe,
                        bias,
                        "cross_timeframe",
                        size + 1,
                        pattern_key,
                        *context,
                    )
                    self._add(
                        stats,
                        key,
                        "cross_timeframe",
                        size + 1,
                        outcome,
                        signal_date,
                        1.15,
                    )

    def _context_tuple(
        self, market: str, ticker: str, signal_date: str, timeframe: str
    ) -> tuple[str, str, str, str, str, str]:
        feat = self.trend_engine.get(market, ticker, signal_date)
        return (
            feat.get("composite_trend_state", "trend_missing"),
            feat.get("weekly_proxy_state", "weekly_proxy_unknown"),
            feat.get("market_context_state", "market_unknown"),
            feat.get("sector_context_state", "sector_unknown"),
            feat.get("volatility_regime", "vol_unknown"),
            feat.get("sector", "UNKNOWN"),
        )

    def _top_signals(self, rows: list[dict[str, Any]], limit: int) -> list[str]:
        rank = {"high_conviction": 0, "core_active": 1, "steady": 2, "watch": 3, None: 4}
        seen = {}
        for row in rows:
            name = row["strategy_name"]
            quality = (
                rank.get(row.get("tier"), 4),
                -(row.get("fitness_score") or 0.0),
                name,
            )
            if name not in seen or quality < seen[name]:
                seen[name] = quality
        return [name for name, _ in sorted(seen.items(), key=lambda kv: kv[1])[:limit]]

    def _group_outcome(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        # Outcome is attached per signal. For a co-firing group, use the mean
        # direction-adjusted return across involved signal outcomes.
        returns = [r["directional_return_15d"] for r in rows if r["directional_return_15d"] is not None]
        ret = sum(returns) / len(returns) if returns else 0.0
        return {"ret": ret, "win": int(ret > 0)}

    def _add(
        self,
        stats: dict[tuple[Any, ...], PatternStats],
        key: tuple[Any, ...],
        pattern_type: str,
        size: int,
        outcome: dict[str, Any],
        date: str,
        recency_weight: float,
    ) -> None:
        if key not in stats:
            stats[key] = PatternStats(key=key, pattern_type=pattern_type, pattern_size=size)
        stats[key].add(float(outcome["ret"]), int(outcome["win"]), date, recency_weight)

    def _compact_signature(self, signals: tuple[str, ...]) -> str:
        return "[" + " & ".join(signals) + "]"
