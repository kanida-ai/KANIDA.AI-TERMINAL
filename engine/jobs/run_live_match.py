from __future__ import annotations

import csv
from pathlib import Path

from engine.config import load_config
from engine.learning.conditional_dataset import load_recent_signal_rows
from engine.learning.stock_pattern_learner import FIELD_NAMES
from engine.live.live_matcher import current_matches
from engine.patterns.signal_pattern_miner import SignalPatternMiner
from engine.readers.sqlite_reader import ReadOnlySQLite
from engine.reports.report_builder import write_live_reports
from engine.trend.trend_features import TrendFeatureEngine


def main() -> None:
    config = load_config()
    learned_path = config["outputs_path"] / "reports" / "learned_pattern_candidates.csv"
    if not learned_path.exists():
        raise SystemExit(
            "Missing learned_patterns_all.csv. Run run_recalibration first."
        )
    learned_rows = read_learned_rows(learned_path)
    with ReadOnlySQLite(config["signals_db_path"]) as db:
        print("Building in-memory trend/context features...")
        trend_engine = TrendFeatureEngine(db, config["markets"]).build()
        print("Loading latest signal rows for dry-run matching...")
        live_rows = load_recent_signal_rows(db, config)
        print(f"Loaded {len(live_rows)} live/recent signal rows.")
        miner = SignalPatternMiner(trend_engine, config)
        matches, clusters = current_matches(learned_rows, live_rows, miner, config)
        write_live_reports(config["outputs_path"], matches, clusters, config)
    print(f"Live dry-run complete: {config['outputs_path'] / 'reports'}")


def read_learned_rows(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = [dict(row) for row in reader]
    for row in rows:
        for field in FIELD_NAMES:
            row.setdefault(field, "")
        for number_field in [
            "occurrences",
            "pattern_size",
            "win_rate",
            "avg_directional_return",
            "weighted_directional_return",
            "recent_win_rate",
            "recent_avg_directional_return",
            "return_stddev",
            "stability",
            "decay_winrate_delta",
            "decay_return_delta",
            "decay_flag",
            "min_support",
            "evidence_score",
        ]:
            if number_field in row and row[number_field] != "":
                row[number_field] = float(row[number_field])
    return rows


if __name__ == "__main__":
    main()
