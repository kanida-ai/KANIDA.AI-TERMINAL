from __future__ import annotations

from engine.config import load_config
from engine.outcome_first.learner import learn_outcome_patterns
from engine.outcome_first.live import rank_live_outcome_opportunities
from engine.outcome_first.reader import load_ohlcv_by_stock
from engine.outcome_first.reports import write_outcome_first_reports
from engine.outcome_first.snapshot_store import finish_run, start_run
from engine.readers.sqlite_reader import ReadOnlySQLite


def main() -> None:
    config = load_config()
    run_id = start_run(config["outputs_path"], "outcome_first")
    try:
        with ReadOnlySQLite(config["signals_db_path"]) as db:
            print("Loading OHLCV history...")
            ohlcv_by_stock = load_ohlcv_by_stock(db, config["markets"])
        print(f"Loaded {len(ohlcv_by_stock)} stock histories.")
        print("Learning outcome-first behavior patterns...")
        learned = learn_outcome_patterns(ohlcv_by_stock, config)
        print(f"Learned {len(learned)} stock-specific outcome patterns.")
        print("Ranking current resemblance opportunities...")
        live = rank_live_outcome_opportunities(learned, ohlcv_by_stock, config)
        print(f"Ranked {len(live)} live outcome-first opportunities.")
        write_outcome_first_reports(config["outputs_path"], learned, live, snapshot_run_id=run_id)
        finish_run(config["outputs_path"], run_id, "success", len(learned), len(live), "completed")
        print(f"Outcome-first reports complete: {config['outputs_path'] / 'reports'}")
    except Exception as exc:
        finish_run(config["outputs_path"], run_id, "failed", 0, 0, str(exc))
        raise


if __name__ == "__main__":
    main()
