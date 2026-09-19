from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from typing import Any

from .market import MarketData
from .models import Edition, Finding, Policy
from .questions import QUESTION_LIBRARY, library_manifest
from .recommendations import build_research_queues
from .registry import Registry
from .research import Researcher
from .validation import validate_snapshots


class PathfinderEngine:
    """Observe -> approved question -> compute -> publish -> test -> grade -> learn."""

    def __init__(self, db_path: str | Path, state_path: str | Path, policy: Policy | None = None, through: str | None = None):
        self.policy = policy or Policy()
        self.market = MarketData(db_path, self.policy, through=through)
        self.researcher = Researcher(self.market, self.policy)
        self.registry = Registry(state_path, self.policy)

    def close(self) -> None:
        self.registry.close()

    def run_edition(self, as_of: str | None = None, launch_experiments: bool = True) -> Edition:
        day = self.market.resolve_date(as_of)
        day_text = day.strftime("%Y-%m-%d")
        self.registry.settle_due(self.market, day_text)
        candidates: list[Finding] = []
        for template in QUESTION_LIBRARY:
            finding = self.researcher.compute(template, day)
            if finding is not None:
                candidates.append(finding)
        candidates.sort(key=lambda finding: (-finding.publication_score, finding.template_id))
        qualified = [
            finding for finding in candidates
            if finding.publication_score >= self.policy.publication_threshold
            and finding.evidence.sample_size >= self.policy.minimum_sample
        ]
        published = qualified[: self.policy.max_published_per_edition]
        edition = Edition(
            as_of=day_text,
            scanned_symbols=int(self.market.current_bars(day)["symbol"].nunique()),
            candidate_count=len(candidates),
            published=published,
            suppressed_count=len(candidates) - len(published),
            usefulness_threshold=self.policy.publication_threshold,
            market_regime=self.market.regime(day),
            data_quality=self.market.public_universe(day),
        )
        self.registry.save_edition(edition, self.market)
        if launch_experiments:
            launched = 0
            for finding in published:
                if launched >= self.policy.max_experiments_per_edition:
                    break
                if self.registry.launch_experiment(finding, self.market) is not None:
                    launched += 1
        return edition

    def replay(self, start: str, end: str | None = None, every_n_sessions: int = 5) -> list[Edition]:
        if every_n_sessions < 1:
            raise ValueError("every_n_sessions must be positive")
        end_day = self.market.resolve_date(end)
        sessions = self.market.sessions_between(start, end_day.strftime("%Y-%m-%d"))
        if not sessions:
            raise ValueError("Replay window contains no market sessions")
        edition_days = sessions[::every_n_sessions]
        if edition_days[-1] != sessions[-1]:
            edition_days.append(sessions[-1])
        editions = [self.run_edition(day.strftime("%Y-%m-%d")) for day in edition_days]
        self.registry.settle_due(self.market, end_day.strftime("%Y-%m-%d"))
        return editions

    def snapshot(self) -> dict[str, Any]:
        latest = self.registry.latest_edition()
        return {
            "engine": {
                "identity": "pathfinder_fresh_v1",
                "principle": "Compute market evidence first; narrate only verified results.",
                "market_data_source": str(self.market.db_path),
                "policy": asdict(self.policy),
                "question_library": library_manifest(),
            },
            "latest_edition": latest,
            "research_recommendations": build_research_queues(latest),
            "scoreboard": self.registry.scoreboard(),
            "recent_outcomes": self.registry.recent_outcomes(),
            "experiments": self.registry.experiment_registry(),
        }

    @staticmethod
    def public_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
        public = json.loads(json.dumps(snapshot))
        latest = public.get("latest_edition") or {}
        for finding in latest.get("findings", []):
            finding.pop("internal", None)
            if finding.get("compliance_label") == "RA_REVIEW_REQUIRED":
                finding["evaluation"]["symbols"] = []
                finding["evaluation"]["benchmark_symbols"] = []
                finding["evaluation"]["internal_subject"] = "Withheld pending RA review"
        for hypothesis in public.get("experiments", []):
            for run in hypothesis.get("runs", []):
                evaluation = run.get("evaluation", {})
                evaluation["symbols"] = []
                evaluation["benchmark_symbols"] = []
                evaluation["internal_subject"] = "Constituents withheld pending RA review"
                run.pop("evaluation_json", None)
            for version in hypothesis.get("versions", []):
                version.pop("rules_json", None)
        recommendation_counts = {
            name: len(items) for name, items in public.get("research_recommendations", {}).items()
        }
        public["research_recommendations"] = {
            "visibility": "Constituent symbols withheld pending RA review",
            "counts": recommendation_counts,
        }
        return public

    def write_outputs(self, output_dir: str | Path) -> dict[str, Path]:
        directory = Path(output_dir).resolve()
        directory.mkdir(parents=True, exist_ok=True)
        snapshot = self.snapshot()
        public = self.public_snapshot(snapshot)
        validate_snapshots(snapshot, public)
        audit_path = directory / "pathfinder_audit.json"
        public_path = directory / "pathfinder_public.json"
        report_path = directory / "pathfinder_edition.txt"
        audit_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
        public_path.write_text(json.dumps(public, indent=2, ensure_ascii=False), encoding="utf-8")
        report_path.write_text(self.render_report(snapshot), encoding="utf-8")
        return {"audit": audit_path, "public": public_path, "report": report_path}

    @staticmethod
    def render_report(snapshot: dict[str, Any]) -> str:
        edition = snapshot.get("latest_edition")
        if not edition:
            return "PATHFINDER — no edition has been computed.\n"
        board = snapshot["scoreboard"]
        lines = [
            "PATHFINDER — AFTER-CLOSE RESEARCH EDITION",
            f"As of {edition['as_of']} · regime {edition['market_regime']}",
            (
                f"Scanned {edition['scanned_symbols']} stocks · computed {edition['candidate_count']} candidates · "
                f"published {edition['published_count']} · suppressed {edition['suppressed_count']} below the usefulness gate"
            ),
            "No minimum card count. Nothing is published to fill space.",
            "",
            "PRIMARY OUTPUT",
        ]
        findings = edition.get("findings", [])
        queues = snapshot.get("research_recommendations", {"long": [], "short": [], "watchlist": []})
        lines.extend([
            "",
            "STOCK RESEARCH RECOMMENDATIONS — INTERNAL RA REVIEW QUEUE",
            "Research classifications only. No entry price, target, stop, position size, or user execution instruction.",
            "",
            f"LONG — {len(queues['long'])} stocks",
        ])
        if not queues["long"]:
            lines.append("None. No published stock basket cleared every research deployment gate.")
        for item in queues["long"]:
            lines.append(
                f"{item['symbol']} · n={item['sample_size']} · historical net edge {item['net_edge'] * 100:+.2f}% · "
                f"p={item['null_calibrated_p']:.3f} · {item['horizon_sessions']} sessions · PENDING RA REVIEW"
            )
        lines.extend(["", f"SHORT — {len(queues['short'])} stocks"])
        if not queues["short"]:
            lines.append("None. No published stock basket cleared every research deployment gate.")
        for item in queues["short"]:
            lines.append(
                f"{item['symbol']} · n={item['sample_size']} · historical net edge {item['net_edge'] * 100:+.2f}% · "
                f"p={item['null_calibrated_p']:.3f} · {item['horizon_sessions']} sessions · PENDING RA REVIEW"
            )
        lines.extend(["", f"WATCHLIST — {len(queues['watchlist'])} stocks"])
        if not queues["watchlist"]:
            lines.append("None. No stock-level or sector-level observation cleared today's publication threshold.")
        for item in queues["watchlist"]:
            lines.append(
                f"{item['symbol']} · {item['public_subject']} · {item['research_reason']} · "
                f"n={item['sample_size']} · p={item['null_calibrated_p']:.3f} · "
                f"reason for watch-only: {item['decision']} · PENDING RA REVIEW"
            )
        lines.extend(["", "RESEARCH FINDINGS AND EVIDENCE"])
        if not findings:
            lines.append("No finding cleared today's publication threshold.")
        for index, finding in enumerate(findings, start=1):
            evidence = finding["evidence"]
            outcome = finding.get("outcome", {})
            prefix = "NOW" if index <= 3 else "DISCOVER"
            lines.extend([
                "",
                f"[{prefix} {index}] {finding['title']}",
                f"I noticed: {finding['observation']}",
                f"I asked: {finding['question']}",
                f"History showed: {finding['history']}",
                f"Decision: {finding['decision']}",
                (
                    f"Evidence: {evidence['provenance']} · n={evidence['sample_size']} · "
                    f"period {evidence['period_start']} to {evidence['period_end']} · regime {evidence['regime']}"
                ),
                (
                    f"Comparison: {evidence['comparison_group']} · cost hurdle {evidence['cost_hurdle'] * 100:.2f}% · "
                    f"null-calibrated p={evidence['null_calibrated_p']:.3f}"
                ),
                f"Frozen grade: {finding['grading_rule']['right_if']} Outcome: {outcome.get('grade', 'PENDING')}",
                f"Publication score: {finding['publication_score']:.1f}/100 · {finding['compliance_label']}",
            ])
        lines.extend([
            "",
            "PUBLIC SCOREBOARD",
            f"Right {board['RIGHT']} · Wrong {board['WRONG']} · Inconclusive {board['INCONCLUSIVE']} · n={board['n']} · Pending {board['PENDING']}",
            "",
            "EXPERIMENT MEMORY",
        ])
        experiments = snapshot.get("experiments", [])
        if not experiments:
            lines.append("No hypothesis has cleared the virtual-capital gate yet.")
        for hypothesis in experiments[:8]:
            gate = hypothesis["promotion_gate"]
            lines.append(
                f"{hypothesis['hypothesis_id']} · {hypothesis['public_subject']} · v{hypothesis['current_version']} · "
                f"versions registered {hypothesis['versions_registered']}/{hypothesis['max_versions']} · "
                f"variants tried {hypothesis['variants_tried']} · forward trials {hypothesis['trial_count']} · status {hypothesis['status']}"
            )
            completed = [run for run in hypothesis["runs"] if run["status"] == "COMPLETED"]
            if completed:
                last = completed[-1]
                lines.append(
                    f"  Last trial v{last['version']}: historical expectation {last['expected_return'] * 100:+.2f}% · "
                    f"forward result {last['actual_return'] * 100:+.2f}% · {last['grade']}"
                )
            current_version = next(
                (version for version in hypothesis["versions"] if version["version"] == hypothesis["current_version"]),
                None,
            )
            if current_version and hypothesis["current_version"] > 1:
                lines.append(f"  Learned and changed: {current_version['change_note']}. {current_version['why_changed']}")
            lines.append(
                f"  Promotion gate: {'PASS' if gate['eligible'] else 'HOLD'} · OOS trials {gate['oos_trials']}/{gate['minimum_oos_trials']} · "
                f"positive edge={gate['positive_oos_edge']} · beats incumbent={gate['beats_incumbent_net_return']} · lower drawdown={gate['lower_drawdown_than_incumbent']}"
            )
            lines.append(f"  Retirement rule: {hypothesis['retirement_rule']}")
        lines.extend([
            "",
            "Research recommendations are internal classifications, not personalized investment advice or execution instructions.",
            "No user entry, target, stop, or position-size instruction is produced. Public constituents remain withheld until RA review.",
        ])
        return "\n".join(lines) + "\n"
