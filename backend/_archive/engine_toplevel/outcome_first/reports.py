from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from engine.outcome_first.snapshot_store import write_snapshot


LEARNED_FIELDS = [
    "market",
    "ticker",
    "direction",
    "target_move",
    "forward_window",
    "pattern_size",
    "behavior_pattern",
    "occurrences",
    "hits",
    "baseline_probability",
    "target_probability",
    "raw_probability",
    "probability_ci_low",
    "probability_ci_high",
    "trusted_probability",
    "display_probability",
    "credibility",
    "lift",
    "avg_forward_return",
    "recent_probability",
    "stability",
    "opportunity_score",
    "decay_flag",
    "tier",
]

LIVE_FIELDS = LEARNED_FIELDS + [
    "latest_date",
    "current_close",
    "current_behavior",
    "current_atoms",
    "similarity",
    "decision_score",
    "setup_summary",
]


def write_outcome_first_reports(
    outputs: Path,
    learned: list[dict[str, Any]],
    live: list[dict[str, Any]],
    snapshot_run_id: int | None = None,
) -> None:
    reports = outputs / "reports"
    write_csv(reports / "outcome_first_patterns.csv", learned, LEARNED_FIELDS)
    write_csv(reports / "outcome_first_live_opportunities.csv", live, LIVE_FIELDS)
    write_live_markdown(reports / "outcome_first_live_opportunities.md", live)
    write_learned_markdown(reports / "outcome_first_patterns.md", learned)
    if snapshot_run_id is not None:
        write_snapshot(outputs, snapshot_run_id, learned, live)


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_live_markdown(path: Path, rows: list[dict[str, Any]]) -> None:
    lines = ["# Outcome-first live opportunities", ""]
    if not rows:
        lines.append("No outcome-first live opportunities passed similarity filters.")
    else:
        lines.append("| Rank | Market | Ticker | Direction | Target | Window | Tier | Trusted Prob | Raw Hit | CI | Baseline | Lift | Avg Return | Similarity | Decision |")
        lines.append("|---:|---|---|---|---:|---:|---|---:|---:|---|---:|---:|---:|---:|---:|")
        for i, row in enumerate(rows, 1):
            lines.append(
                f"| {i} | {row['market']} | {row['ticker']} | {row['direction']} | "
                f"{float(row['target_move']):.0%} | {int(row['forward_window'])}d | {row['tier']} | "
                f"{float(row.get('display_probability', row['target_probability'])):.1%} | {float(row['target_probability']):.1%} | "
                f"{float(row.get('probability_ci_low', 0.0)):.1%}-{float(row.get('probability_ci_high', 0.0)):.1%} | "
                f"{float(row['baseline_probability']):.1%} | "
                f"{float(row['lift']):.2f}x | {float(row['avg_forward_return']):.2%} | "
                f"{float(row['similarity']):.0%} | {float(row['decision_score']):.3f} |"
            )
        lines.append("")
        lines.append("## Setup notes")
        for row in rows[:30]:
            lines.append(f"- {row['setup_summary']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_learned_markdown(path: Path, rows: list[dict[str, Any]]) -> None:
    lines = ["# Outcome-first learned behavior patterns", ""]
    if not rows:
        lines.append("No learned patterns passed filters.")
    else:
        lines.append("| Rank | Market | Ticker | Direction | Target | Window | Pattern | Occ | Trusted Prob | Raw Hit | Baseline | Lift | Score |")
        lines.append("|---:|---|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|")
        for i, row in enumerate(rows[:100], 1):
            lines.append(
                f"| {i} | {row['market']} | {row['ticker']} | {row['direction']} | "
                f"{float(row['target_move']):.0%} | {int(row['forward_window'])}d | {row['behavior_pattern']} | "
                f"{row['occurrences']} | {float(row.get('display_probability', row['target_probability'])):.1%} | {float(row['target_probability']):.1%} | "
                f"{float(row['baseline_probability']):.1%} | {float(row['lift']):.2f}x | "
                f"{float(row['opportunity_score']):.3f} |"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
