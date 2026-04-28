"""
KANIDA.AI Production Scheduler
================================
Runs daily after NSE market close (15:30 IST):
  1. Fetch latest OHLCV data from Zerodha
  2. Pattern learning
  3. Backtest
  4. Execution analysis  →  populates live_opportunities

Usage (run from project root):
    python backend/scheduler.py               # run pipeline once now
    python backend/scheduler.py --loop        # run daily at 16:00 IST
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT   = Path(__file__).parent.parent
PYTHON = sys.executable

IST = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("kanida.scheduler")

PIPELINE_STEPS = [
    {
        "name": "OHLCV Fetch",
        "cmd":  ["python", "data/ingest/fetch_fno_kite.py"],
        "log":  "outputs/scheduler_1_fetch.log",
    },
    {
        "name": "Pattern Learning",
        "cmd":  ["python", "engine/jobs/run_learning.py"],
        "log":  "outputs/scheduler_2_learning.log",
    },
    {
        "name": "Backtest",
        "cmd":  ["python", "engine/backtest/run_backtest.py"],
        "log":  "outputs/scheduler_3_backtest.log",
    },
    {
        "name": "Execution Analysis",
        "cmd":  ["python", "engine/backtest/run_execution_analysis.py"],
        "log":  "outputs/scheduler_4_execution.log",
    },
]


def _run_step(step: dict) -> bool:
    cmd      = [PYTHON] + step["cmd"][1:]
    log_path = ROOT / step["log"]
    log_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("START  %s", step["name"])
    t0 = time.time()

    with open(log_path, "w", encoding="utf-8") as fh:
        result = subprocess.run(
            cmd,
            cwd=str(ROOT),
            stdout=fh,
            stderr=subprocess.STDOUT,
            text=True,
        )

    elapsed = time.time() - t0
    ok      = result.returncode == 0
    log.info("FINISH %s  [%.0fs]  %s", step["name"], elapsed, "OK" if ok else f"FAILED rc={result.returncode}")

    if not ok:
        # Print last 30 lines of log on failure
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        for ln in lines[-30:]:
            log.error("  %s", ln)

    return ok


def run_pipeline() -> bool:
    log.info("=" * 60)
    log.info("KANIDA.AI pipeline starting — %s", datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"))
    log.info("=" * 60)

    for step in PIPELINE_STEPS:
        if not _run_step(step):
            log.error("Pipeline aborted at: %s", step["name"])
            return False

    log.info("Pipeline complete — live_opportunities updated.")
    return True


def _seconds_until_next_run(hour: int = 16, minute: int = 0) -> float:
    """Seconds until the next HH:MM IST on a weekday."""
    now = datetime.now(IST)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    # Skip to Monday if target falls on weekend
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def main() -> None:
    loop = "--loop" in sys.argv

    if not loop:
        sys.exit(0 if run_pipeline() else 1)

    log.info("Scheduler started — will run daily at 16:00 IST on weekdays.")
    while True:
        wait = _seconds_until_next_run(hour=16, minute=0)
        next_run = datetime.now(IST) + timedelta(seconds=wait)
        log.info("Next run: %s (in %.0f minutes)", next_run.strftime("%Y-%m-%d %H:%M IST"), wait / 60)
        time.sleep(wait)

        now = datetime.now(IST)
        if now.weekday() >= 5:
            log.info("Weekend — skipping pipeline.")
            continue

        run_pipeline()


if __name__ == "__main__":
    main()
