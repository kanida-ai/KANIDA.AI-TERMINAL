from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from engine.config import load_config
from engine.outcome_first.snapshot_store import latest_rows, latest_run
from engine.outcome_first.trust import apply_trust


class PrototypeDataService:
    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or load_config()
        self.outputs = self.config["outputs_path"] / "reports"
        self.discovery = self.config["outputs_path"] / "discovery"

    def load_all(self) -> dict[str, Any]:
        trusted_rows = self._read_csv("trusted_patterns_by_stock.csv")
        trusted_sets = self._read_csv("trusted_pattern_sets.csv")
        live_matches = self._read_csv("live_matches.csv")
        outcome_live = self._outcome_live_rows()
        outcome_patterns = self._outcome_pattern_rows()
        clusters = self._read_csv("portfolio_clusters.csv")
        top_combined = self._read_csv("top_patterns_combined.csv")
        return {
            "summary": self.summary(trusted_rows, trusted_sets, live_matches, clusters, outcome_live, outcome_patterns),
            "vision_audit": self.vision_audit(live_matches, clusters, trusted_sets, outcome_live, outcome_patterns),
            "stocks": self.stock_cards(trusted_sets),
            "trusted_patterns": trusted_rows,
            "live_matches": live_matches,
            "outcome_live": outcome_live,
            "outcome_patterns": outcome_patterns,
            "snapshot_status": self.snapshot_status(),
            "clusters": clusters,
            "top_patterns": top_combined,
            "outcome_markdown": self._read_text(self.outputs / "outcome_first_live_opportunities.md"),
            "insights_markdown": self._read_text(self.outputs / "human_readable_insights.md"),
            "discovery_markdown": self._read_text(self.discovery / "source_discovery.md"),
        }

    def summary(
        self,
        trusted_rows: list[dict[str, Any]],
        trusted_sets: list[dict[str, Any]],
        live_matches: list[dict[str, Any]],
        clusters: list[dict[str, Any]],
        outcome_live: list[dict[str, Any]],
        outcome_patterns: list[dict[str, Any]],
    ) -> dict[str, Any]:
        stock_keys = {(r["market"], r["ticker"], r["timeframe"], r["bias"]) for r in trusted_sets}
        by_market: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "trusted_stocks": 0,
            "trusted_patterns": 0,
            "live_opportunities": 0,
            "outcome_opportunities": 0,
            "clusters": 0,
        })
        for market, _ticker, _tf, _bias in stock_keys:
            by_market[market]["trusted_stocks"] += 1
        for row in trusted_rows:
            by_market[row["market"]]["trusted_patterns"] += 1
        for row in live_matches:
            by_market[row["market"]]["live_opportunities"] += 1
        for row in outcome_live:
            by_market[row["market"]]["outcome_opportunities"] += 1
        for row in clusters:
            by_market[row["market"]]["clusters"] += 1
        combined = {
            "trusted_stocks": len(stock_keys),
            "trusted_patterns": len(trusted_rows),
            "live_opportunities": len(live_matches),
            "outcome_opportunities": len(outcome_live),
            "outcome_patterns": len(outcome_patterns),
            "clusters": len(clusters),
        }
        return {"combined": combined, "markets": dict(by_market)}

    def vision_audit(
        self,
        live_matches: list[dict[str, Any]],
        clusters: list[dict[str, Any]],
        trusted_sets: list[dict[str, Any]],
        outcome_live: list[dict[str, Any]],
        outcome_patterns: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        trusted_count = len(trusted_sets)
        live_count = len(live_matches)
        outcome_count = len(outcome_live)
        return [
            {
                "principle": 1,
                "title": "Stock-Specific Intelligence",
                "status": "complete",
                "detail": f"Outcome-first patterns and signal playbooks are retained per stock. Outcome-first learned patterns: {len(outcome_patterns)}.",
            },
            {
                "principle": 2,
                "title": "Trend-Conditioned Learning",
                "status": "complete",
                "detail": "Outcome-first behavior patterns include prior trend, flow, volatility, moving-average position, breakout state, candle, volume, and support/resistance.",
            },
            {
                "principle": 3,
                "title": "Outcome-First Discovery",
                "status": "complete",
                "detail": "The primary learner starts from future rally/fall events and looks backward for recurring pre-move behavior patterns.",
            },
            {
                "principle": 4,
                "title": "Signals Are Descriptive Only",
                "status": "complete",
                "detail": "Predefined signals are no longer the primary starting point for the outcome-first opportunity list.",
            },
            {
                "principle": 5,
                "title": "Focus on Outcomes (15-day moves)",
                "status": "complete",
                "detail": "The outcome-first engine evaluates +3%, +5%, and +10% / -3%, -5%, and -10% targets over 15-day and 20-day windows.",
            },
            {
                "principle": 6,
                "title": "Strict Filtering",
                "status": "complete" if live_count > 0 else "partial",
                "detail": "Minimum support, complexity penalty, decay checks, support gating, and fallback opportunity surfacing are active.",
            },
            {
                "principle": 7,
                "title": "Stock-Specific Trusted Pattern Sets",
                "status": "complete",
                "detail": "Keep/watch/test/retire is exported into stock-specific trusted pattern set reports.",
            },
            {
                "principle": 8,
                "title": "Live Matching",
                "status": "complete",
                "detail": f"Outcome-first live resemblance matcher is active. Current outcome-first opportunities: {outcome_count}.",
            },
            {
                "principle": 9,
                "title": "Actionable Insights",
                "status": "complete" if outcome_count > 0 else "partial",
                "detail": "Outcome-first opportunities include target move, window, baseline probability, learned probability, lift, similarity, tier, and narrative summary.",
            },
            {
                "principle": 10,
                "title": "Autonomous Quant Agent",
                "status": "pending",
                "detail": f"Learning + matching are built, but continuous scheduling, alerting, and guided exploration layer are not finished. Current portfolio clusters: {len(clusters)}.",
            },
        ]

    def stock_cards(self, trusted_sets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in trusted_sets:
            grouped[(row["market"], row["ticker"], row["timeframe"], row["bias"])].append(row)
        cards = []
        for (market, ticker, timeframe, bias), rows in grouped.items():
            rows.sort(key=lambda r: (int(r["trusted_pattern_rank"]), -float(r["evidence_score"])))
            cards.append(
                {
                    "market": market,
                    "ticker": ticker,
                    "timeframe": timeframe,
                    "bias": bias,
                    "trusted_pattern_count": len(rows),
                    "best_evidence_score": max(float(r["evidence_score"]) for r in rows),
                    "top_trend_state": rows[0]["trend_state"],
                    "top_pattern": rows[0]["pattern"],
                    "avg_win_rate": sum(float(r["win_rate"]) for r in rows) / len(rows),
                    "avg_directional_return": sum(float(r["avg_directional_return"]) for r in rows) / len(rows),
                }
            )
        cards.sort(key=lambda r: (-r["best_evidence_score"], -r["trusted_pattern_count"], r["market"], r["ticker"]))
        return cards

    def filter_market(self, rows: list[dict[str, Any]], market: str | None) -> list[dict[str, Any]]:
        if not market or market.upper() == "COMBINED":
            return rows
        return [row for row in rows if row.get("market") == market]

    def outcome_opportunities(self, market: str | None = None, direction: str | None = None) -> list[dict[str, Any]]:
        rows = self.filter_market(self._outcome_live_rows(), market)
        if direction and direction.upper() != "ALL":
            rows = [row for row in rows if row.get("direction") == direction]
        return rows

    def snapshot_status(self) -> dict[str, Any]:
        run = latest_run(self.config["outputs_path"])
        return run or {"status": "missing", "message": "No snapshot DB run found"}

    def stock_outcome_context(self, market: str, ticker: str) -> dict[str, Any]:
        live = [
            row for row in self._outcome_live_rows()
            if row.get("market") == market and row.get("ticker") == ticker
        ]
        patterns = [
            row for row in self._outcome_pattern_rows()
            if row.get("market") == market and row.get("ticker") == ticker
        ]
        live.sort(key=lambda r: -float(r.get("decision_score", 0) or 0))
        patterns.sort(key=lambda r: -float(r.get("opportunity_score", 0) or 0))
        retired_or_weak = [
            row for row in patterns
            if str(row.get("credibility", "")) in {"exploratory"} or int(float(row.get("decay_flag", 0) or 0)) == 1
        ][:20]
        return {
            "market": market,
            "ticker": ticker,
            "current_outcome_opportunities": live[:10],
            "strongest_historical_patterns": patterns[:20],
            "weak_or_decaying_patterns": retired_or_weak,
            "chat_context_note": (
                "Use this as grounded context for a future LLM answer. "
                "Do not answer from generic market knowledge when these rows are present."
            ),
        }

    def stock_detail(
        self, market: str, ticker: str, timeframe: str | None = None, bias: str | None = None
    ) -> dict[str, Any]:
        trusted_rows = self._read_csv("trusted_pattern_sets.csv")
        live_matches = self._read_csv("live_matches.csv")
        selected = [
            row for row in trusted_rows
            if row["market"] == market and row["ticker"] == ticker
            and (timeframe is None or row["timeframe"] == timeframe)
            and (bias is None or row["bias"] == bias)
        ]
        selected.sort(
            key=lambda r: (
                r["timeframe"],
                r["bias"],
                int(r.get("trusted_pattern_rank", 9999)),
            )
        )
        live = [
            row for row in live_matches
            if row["market"] == market and row["ticker"] == ticker
            and (timeframe is None or row["timeframe"] == timeframe)
            and (bias is None or row["bias"] == bias)
        ]
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in selected:
            grouped[f"{row['timeframe']}|{row['bias']}|{row['trend_state']}"].append(row)
        trend_buckets = []
        for key, rows in grouped.items():
            tf, row_bias, trend_state = key.split("|", 2)
            rows.sort(key=lambda r: int(r["trusted_pattern_rank"]))
            trend_buckets.append(
                {
                    "timeframe": tf,
                    "bias": row_bias,
                    "trend_state": trend_state,
                    "patterns": rows,
                }
            )
        trend_buckets.sort(key=lambda x: (x["timeframe"], x["bias"], x["trend_state"]))
        headline = None
        if selected:
            best = sorted(selected, key=lambda r: (-float(r["evidence_score"]), int(r["trusted_pattern_rank"])))[0]
            headline = (
                f"{ticker} has {len(selected)} trusted stock-specific patterns. "
                f"Best learned condition: {best['pattern']} in {best['trend_state']} "
                f"with {float(best['win_rate']):.1%} win rate and "
                f"{float(best['avg_directional_return']):.2%} average directional 15-day return."
            )
        return {
            "market": market,
            "ticker": ticker,
            "timeframe": timeframe,
            "bias": bias,
            "headline": headline,
            "trend_buckets": trend_buckets,
            "live_matches": live,
        }

    def _read_csv(self, name: str) -> list[dict[str, Any]]:
        path = self.outputs / name
        if not path.exists() or path.stat().st_size == 0:
            return []
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return [self._coerce(dict(row)) for row in reader]

    def _outcome_live_rows(self) -> list[dict[str, Any]]:
        rows = latest_rows(self.config["outputs_path"], "outcome_first_live_opportunities")
        if not rows:
            rows = self._read_csv("outcome_first_live_opportunities.csv")
        return [self._ensure_trust(row) for row in rows]

    def _outcome_pattern_rows(self) -> list[dict[str, Any]]:
        rows = latest_rows(self.config["outputs_path"], "outcome_first_patterns")
        if not rows:
            rows = self._read_csv("outcome_first_patterns.csv")
        return [self._ensure_trust(row) for row in rows]

    def _ensure_trust(self, row: dict[str, Any]) -> dict[str, Any]:
        if "display_probability" in row and row.get("display_probability") not in ("", None):
            return row
        return apply_trust(row)

    def _coerce(self, row: dict[str, Any]) -> dict[str, Any]:
        for key, value in list(row.items()):
            if value in ("", None):
                continue
            try:
                if "." in value:
                    row[key] = float(value)
                elif value.isdigit():
                    row[key] = int(value)
            except Exception:
                pass
        return row

    def _read_text(self, path: Path) -> str:
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8")


def to_json_bytes(payload: dict[str, Any] | list[dict[str, Any]]) -> bytes:
    return json.dumps(payload, ensure_ascii=True, indent=2).encode("utf-8")
