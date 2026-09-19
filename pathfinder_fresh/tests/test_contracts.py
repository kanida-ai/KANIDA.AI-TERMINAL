from __future__ import annotations

import ast
from dataclasses import asdict
from pathlib import Path
import unittest

from pathfinder_fresh.engine import PathfinderEngine
from pathfinder_fresh.models import Evidence, EvaluationPlan, Finding, GradingRule
from pathfinder_fresh.questions import QUESTION_LIBRARY
from pathfinder_fresh.recommendations import build_research_queues
from pathfinder_fresh.research import edge_calibrated_p, null_calibrated_p


class ContractTests(unittest.TestCase):
    def test_question_library_has_unique_ids_and_known_computations(self):
        ids = [template.template_id for template in QUESTION_LIBRARY]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all(template.version >= 1 for template in QUESTION_LIBRARY))
        self.assertTrue(all(template.evidence_contract for template in QUESTION_LIBRARY))

    def test_null_calibration_is_deterministic(self):
        import numpy as np

        conditional = np.array([0.01, 0.02, 0.03, 0.04, 0.05])
        comparison = np.linspace(-0.03, 0.03, 100)
        first = null_calibrated_p(conditional, comparison, trials=50, seed=7)
        second = null_calibrated_p(conditional, comparison, trials=50, seed=7)
        self.assertEqual(first, second)
        self.assertGreaterEqual(first, 0)
        self.assertLessEqual(first, 1)

    def test_directional_edge_calibration_respects_cost_hurdle(self):
        import numpy as np

        strong = edge_calibrated_p(np.repeat(0.02, 100), hurdle=0.003, trials=50, seed=9)
        weak = edge_calibrated_p(np.repeat(0.002, 100), hurdle=0.003, trials=50, seed=9)
        self.assertLess(strong, 0.1)
        self.assertEqual(weak, 1.0)

    def test_public_snapshot_masks_ra_constituents(self):
        evidence = Evidence(
            provenance="similar_stocks", sample_size=100, period_start="2020-01-01", period_end="2025-01-01",
            regime="range/normal_vol", comparison_group="peers", cost_hurdle=0.003,
            observed_metric=0.01, comparison_metric=0.0, net_edge=0.007, win_rate=0.6,
            null_calibrated_p=0.05, outcome_metric="return", data_source="kanida.db",
        )
        grading = GradingRule(
            kind="directional", horizon_sessions=5, direction="LONG", hurdle=0.003,
            right_if="right", wrong_if="wrong", inconclusive_if="inside",
        )
        evaluation = EvaluationPlan(
            kind="directional", symbols=["SECRET"], benchmark_symbols=[], direction="LONG",
            horizon_sessions=5, hurdle=0.003, expected_return=0.01, internal_subject="SECRET",
        )
        finding = Finding(
            finding_id="f", edition_date="2025-01-01", template_id="STOCK_SHOCK_REVERSAL", template_version=1,
            family="stock_behaviour", question="q", title="t", observation="o", history="h", decision="d",
            why_now="w", usefulness_score=80, novelty_score=80, evidence_score=80,
            trader_relevance_score=80, publication_score=80, evidence=evidence, grading_rule=grading,
            evaluation=evaluation, experiment_eligible=True, public_subject="sector anomaly",
            compliance_label="RA_REVIEW_REQUIRED", internal={"symbol": "SECRET"},
        )
        snapshot = {"latest_edition": {"findings": [finding.as_dict()]}, "experiments": []}
        public = PathfinderEngine.public_snapshot(snapshot)
        masked = public["latest_edition"]["findings"][0]
        self.assertNotIn("internal", masked)
        self.assertEqual(masked["evaluation"]["symbols"], [])

    def test_clean_room_package_does_not_import_legacy_pathfinder(self):
        package = Path(__file__).resolve().parents[1]
        for source in package.glob("*.py"):
            tree = ast.parse(source.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    self.assertFalse(node.module == "pathfinder" or node.module.startswith("pathfinder."))
                if isinstance(node, ast.Import):
                    self.assertFalse(any(alias.name == "pathfinder" or alias.name.startswith("pathfinder.") for alias in node.names))

    def test_long_and_short_require_experiment_gate(self):
        finding = {
            "finding_id": "x", "publication_score": 80, "public_subject": "Sector",
            "title": "Computed result", "decision": "NO TRADE", "experiment_eligible": False,
            "evaluation": {"symbols": ["ABC"], "direction": "LONG", "horizon_sessions": 5},
            "evidence": {
                "provenance": "sector", "observed_metric": 0.01, "net_edge": 0.007,
                "sample_size": 100, "null_calibrated_p": 0.05, "cost_hurdle": 0.003,
            },
        }
        queues = build_research_queues({"findings": [finding]})
        self.assertEqual(queues["long"], [])
        self.assertEqual(queues["short"], [])
        self.assertEqual([item["symbol"] for item in queues["watchlist"]], ["ABC"])

    def test_directional_queue_takes_precedence_over_watchlist(self):
        evidence = {
            "provenance": "similar_stocks", "observed_metric": 0.01, "net_edge": 0.007,
            "sample_size": 100, "null_calibrated_p": 0.05, "cost_hurdle": 0.003,
        }
        base = {
            "finding_id": "watch", "publication_score": 90, "public_subject": "Sector",
            "title": "Watch result", "decision": "WATCH", "experiment_eligible": False,
            "evaluation": {"symbols": ["ABC"], "direction": "NONE", "horizon_sessions": 5},
            "evidence": evidence,
        }
        actionable = {
            "finding_id": "long", "publication_score": 80, "public_subject": "Market basket",
            "title": "Long result", "decision": "VIRTUAL LONG", "experiment_eligible": True,
            "evaluation": {"symbols": ["ABC"], "direction": "LONG", "horizon_sessions": 5},
            "evidence": evidence,
        }
        queues = build_research_queues({"findings": [base, actionable]})
        self.assertEqual([item["symbol"] for item in queues["long"]], ["ABC"])
        self.assertEqual(queues["watchlist"], [])


if __name__ == "__main__":
    unittest.main()
