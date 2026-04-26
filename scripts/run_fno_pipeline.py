"""
KANIDA.AI — Full F&O Expansion Pipeline Runner
================================================
Runs all 4 steps in sequence after OHLCV data is already fetched:
  1. Pattern learning  (engine/jobs/run_learning.py)
  2. Backtest          (engine/backtest/run_backtest.py)
  3. Execution analysis(engine/backtest/run_execution_analysis.py)

Usage:
    python scripts/run_fno_pipeline.py

Total estimated time: 20-40 minutes for 188 stocks.
"""
from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
PYTHON = sys.executable

STEPS = [
    {
        "name": "Pattern Learning",
        "cmd":  ["python", "engine/jobs/run_learning.py"],
        "log":  "outputs/pipeline_1_learning.log",
    },
    {
        "name": "Backtest",
        "cmd":  ["python", "engine/backtest/run_backtest.py"],
        "log":  "outputs/pipeline_2_backtest.log",
    },
    {
        "name": "Execution Analysis",
        "cmd":  ["python", "engine/backtest/run_execution_analysis.py"],
        "log":  "outputs/pipeline_3_execution.log",
    },
]


def run_step(step: dict) -> bool:
    name     = step["name"]
    cmd      = [PYTHON] + step["cmd"][1:]
    log_path = ROOT / step["log"]
    log_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*65}")
    print(f"  STEP: {name}")
    print(f"  CMD : {' '.join(step['cmd'])}")
    print(f"  LOG : {log_path}")
    print(f"  Start: {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*65}")

    t0 = time.time()
    with open(log_path, "w", encoding="utf-8") as log:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )

    elapsed = time.time() - t0
    status  = "OK" if proc.returncode == 0 else f"FAILED (rc={proc.returncode})"
    print(f"  Finished: {datetime.now().strftime('%H:%M:%S')}  [{elapsed:.0f}s]  {status}")

    # Print last 20 lines of log regardless of outcome
    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    print(f"\n  --- last {min(20, len(lines))} lines of log ---")
    for ln in lines[-20:]:
        print(f"  {ln}")

    return proc.returncode == 0


def main() -> None:
    print("=" * 65)
    print("KANIDA.AI — F&O Expansion Pipeline")
    print(f"Python : {PYTHON}")
    print(f"Root   : {ROOT}")
    print(f"Start  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    results = {}
    for step in STEPS:
        ok = run_step(step)
        results[step["name"]] = ok
        if not ok:
            print(f"\n[ABORT] {step['name']} failed — stopping pipeline.")
            print(f"Check log: {ROOT / step['log']}")
            break

    print("\n" + "=" * 65)
    print("PIPELINE SUMMARY")
    print("=" * 65)
    for name, ok in results.items():
        mark = "✓" if ok else "✗"
        print(f"  {mark}  {name}")
    all_ok = all(results.values()) and len(results) == len(STEPS)
    print(f"\n  Status: {'ALL STEPS COMPLETE' if all_ok else 'INCOMPLETE — check logs'}")
    print("=" * 65)

    if all_ok:
        print("""
  Next: restart the backend to serve the new data:
    Get-Process python* | ForEach-Object { taskkill /F /T /PID $_.Id }
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
""")


if __name__ == "__main__":
    main()
