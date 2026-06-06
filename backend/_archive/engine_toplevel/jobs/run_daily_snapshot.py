from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path

from engine.config import load_config


JOBS = [
    ["-m", "engine.jobs.run_outcome_first"],
    ["-m", "engine.jobs.run_recalibration"],
    ["-m", "engine.jobs.run_live_match"],
]


def main() -> None:
    config = load_config()
    logs = config["outputs_path"] / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    status_path = logs / "daily_snapshot_status.json"
    entries = []
    ok = True
    for job in JOBS:
        name = job[-1].split(".")[-1]
        started = datetime.now().isoformat(timespec="seconds")
        log_path = logs / f"{stamp}_{name}.log"
        err_path = logs / f"{stamp}_{name}.err.log"
        with log_path.open("w", encoding="utf-8") as out, err_path.open("w", encoding="utf-8") as err:
            proc = subprocess.run(
                [sys.executable, *job],
                cwd=str(Path(__file__).resolve().parents[2]),
                stdout=out,
                stderr=err,
                text=True,
                check=False,
            )
        finished = datetime.now().isoformat(timespec="seconds")
        entries.append(
            {
                "job": name,
                "started_at": started,
                "finished_at": finished,
                "returncode": proc.returncode,
                "stdout_log": str(log_path),
                "stderr_log": str(err_path),
            }
        )
        if proc.returncode != 0:
            ok = False
            break
    status_path.write_text(
        __import__("json").dumps(
            {
                "status": "success" if ok else "failed",
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "jobs": entries,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    if not ok:
        raise SystemExit(1)
    print(f"Daily snapshot complete: {status_path}")


if __name__ == "__main__":
    main()
