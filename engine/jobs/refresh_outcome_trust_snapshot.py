from __future__ import annotations

import csv
from pathlib import Path

from engine.config import load_config
from engine.outcome_first.reports import write_outcome_first_reports
from engine.outcome_first.snapshot_store import finish_run, start_run
from engine.outcome_first.trust import apply_trust


def main() -> None:
    config = load_config()
    reports = config["outputs_path"] / "reports"
    learned = [apply_trust(row) for row in read_csv(reports / "outcome_first_patterns.csv")]
    live = []
    for row in read_csv(reports / "outcome_first_live_opportunities.csv"):
        enriched = apply_trust(row)
        enriched["setup_summary"] = trusted_summary(enriched)
        live.append(enriched)
    run_id = start_run(config["outputs_path"], "outcome_first_trust_refresh")
    write_outcome_first_reports(config["outputs_path"], learned, live, snapshot_run_id=run_id)
    finish_run(config["outputs_path"], run_id, "success", len(learned), len(live), "trust refresh from existing CSV")
    print(f"Trust refresh complete: {len(learned)} patterns, {len(live)} live opportunities")


def read_csv(path: Path) -> list[dict[str, object]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def trusted_summary(row: dict[str, object]) -> str:
    direction = str(row["direction"])
    verb = "rally" if direction == "rally" else "fall"
    side = "upside" if direction == "rally" else "downside"
    move = f"+{float(row['target_move']):.0%}" if direction == "rally" else f"-{float(row['target_move']):.0%}"
    caveat = credibility_phrase(str(row["credibility"]), int(float(row["occurrences"])))
    return (
        f"{row['ticker']} is showing a familiar {side} setup. "
        f"The current tape resembles {float(row['similarity']):.0%} of a historical pre-{verb} pattern "
        f"that previously led to {move} within {int(float(row['forward_window']))} trading days. "
        f"Raw hit rate was {float(row['target_probability']):.1%}, but Kanida trims that to a trust-adjusted "
        f"{float(row['display_probability']):.1%} after accounting for sample size "
        f"(95% confidence band: {float(row['probability_ci_low']):.1%}-{float(row['probability_ci_high']):.1%}). "
        f"The stock's normal baseline for this move is {float(row['baseline_probability']):.1%}, so the pattern shows "
        f"{float(row['lift']):.2f}x lift with {float(row['avg_forward_return']):.2%} average directional follow-through. "
        f"{caveat}"
    )


def credibility_phrase(credibility: str, occurrences: int) -> str:
    if credibility == "strong":
        return f"Evidence is strong across {occurrences} historical matches."
    if credibility == "solid":
        return f"Evidence is solid across {occurrences} historical matches."
    if credibility == "thin_but_interesting":
        return f"Evidence is promising but sample-sensitive across {occurrences} historical matches."
    return f"Evidence is exploratory across {occurrences} historical matches."


if __name__ == "__main__":
    main()
