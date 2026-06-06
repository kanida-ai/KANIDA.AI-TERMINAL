from __future__ import annotations

from engine.config import load_config
from engine.learning.conditional_dataset import load_learning_rows
from engine.learning.stock_pattern_learner import summarize_stats
from engine.patterns.signal_pattern_miner import SignalPatternMiner
from engine.readers.sqlite_reader import ReadOnlySQLite
from engine.reports.report_builder import write_learning_reports
from engine.trend.trend_features import TrendFeatureEngine


def main() -> None:
    config = load_config()
    with ReadOnlySQLite(config["signals_db_path"]) as db:
        print("Building in-memory trend/context features...")
        trend_engine = TrendFeatureEngine(db, config["markets"]).build()
        print("Loading completed signal/outcome rows...")
        rows = load_learning_rows(db, config)
        print(f"Loaded {len(rows)} signal rows.")
        miner = SignalPatternMiner(trend_engine, config)
        print("Mining same-bar, sequential, and cross-timeframe patterns...")
        stats = miner.mine(rows)
        print(f"Mined {len(stats)} conditional pattern states.")
        learned_rows = summarize_stats(stats, config)
        write_learning_reports(config["outputs_path"], learned_rows, config)
    print(f"Recalibration complete: {config['outputs_path'] / 'reports'}")


if __name__ == "__main__":
    main()
