"""
Run one turn of the Pathfinder loop and persist it.

    python scripts/run_pathfinder_loop.py --fresh

`--fresh` deletes the run database first. Without it the run appends to whatever is
there, which is what a real daily run does.

Nothing here computes. It wires the pieces together and prints what the engine found.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from pathfinder.engine.config import load_config                     # noqa: E402
from pathfinder.engine.costs import DEFAULT_COSTS                    # noqa: E402
from pathfinder.engine.governance import load_constitution           # noqa: E402
from pathfinder.engine.loop import PathfinderLoop                    # noqa: E402
from pathfinder.engine.repository import Repository                  # noqa: E402
from pathfinder.llm.factory import build_llm                         # noqa: E402

MIGRATION = ROOT / "migrations" / "0002_pathfinder_sqlite.sql"


def main() -> int:
    ap = argparse.ArgumentParser(description="Run one turn of the Pathfinder research loop.")
    ap.add_argument("--fresh", action="store_true", help="delete the run database first")
    ap.add_argument("--llm", default=None, help="auto | live | recorded | none")
    args = ap.parse_args()

    cfg = load_config()
    if args.fresh and Path(cfg.run_db).exists():
        Path(cfg.run_db).unlink()

    constitution = load_constitution(cfg.constitution_path)
    repo = Repository(cfg.run_db)
    repo.migrate(MIGRATION)
    llm, provider, ledger = build_llm(constitution, mode=args.llm)

    loop = PathfinderLoop(
        cfg=cfg, costs=DEFAULT_COSTS, repo=repo, llm=llm, constitution=constitution,
        budget=ledger.budget, llm_provider_name=provider, ledger=ledger,
    )
    report = loop.run()

    print("=" * 96)
    print(f"PATHFINDER LOOP · cycle {report.cycle_id} · as_of {report.as_of} "
          f"· constitution {constitution.version}")
    print(f"LLM provider: {report.llm_provider} · candidates screened: {report.candidates_screened}")
    print("=" * 96)
    print("\nGATES")
    for line in report.gate_log:
        print("  " + line)
    print("\nSTATUS")
    for eid, st in report.statuses.items():
        print(f"  {eid}: {st}")
    if report.notes:
        print("\nNOTES")
        for n in report.notes:
            print("  - " + n)
    if report.narration_failures:
        print(f"\nNARRATION: {len(report.narration_failures)} beat(s) fell back to the engine "
              "(recorded on every story line as produced_by='engine')")
        for f in report.narration_failures[:5]:
            print("  - " + f)
    print("\nCHAIN INTEGRITY")
    for table, ok, msg in repo.verify_all_chains():
        print(f"  {'OK ' if ok else 'BAD'} {msg}")
    repo.close()
    print(f"\nwritten to {cfg.run_db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
