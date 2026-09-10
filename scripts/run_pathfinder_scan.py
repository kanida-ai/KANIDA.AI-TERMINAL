"""
Run the Pathfinder after-close scan (Session S1) and persist the edition.

    python scripts/run_pathfinder_scan.py                       # latest close, engine narration
    python scripts/run_pathfinder_scan.py --date 2026-07-29     # a specific close (point-in-time)
    python scripts/run_pathfinder_scan.py --backfill 10         # the 10 sessions before --date, then --date
    python scripts/run_pathfinder_scan.py --fresh               # delete the research store first
    python scripts/run_pathfinder_scan.py --llm none|recorded|auto|live

Every scan is sealed at its date: a backfilled edition for D is computed from bars <= D only,
and the grading pass at each later date grades what completed by then. Editions are
append-only — a date that already has one is skipped (its due findings are still graded).

Nothing here computes. It wires the pieces together and prints what the engine found.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")   # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    pass

from pathfinder.research.config import load_config              # noqa: E402
from pathfinder.research.data import MarketData                 # noqa: E402
from pathfinder.research.narrate import Narrator                # noqa: E402
from pathfinder.research.scan import run_scan                   # noqa: E402
from pathfinder.research.store import ResearchStore             # noqa: E402


def _narrator(mode: str) -> Narrator:
    if mode == "none":
        return Narrator(None, provider_name="none")
    from pathfinder.engine.governance import load_constitution
    from pathfinder.llm.factory import build_llm
    from pathfinder.research.config import REPO_ROOT
    constitution = load_constitution(str(REPO_ROOT / "config" / "pathfinder_constitution.yaml"))
    llm, provider, ledger = build_llm(constitution, mode=mode)
    if provider == "none":
        return Narrator(None, provider_name="none")
    return Narrator(llm, provider_name=provider, budget=ledger.budget)


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the Pathfinder after-close research scan.")
    ap.add_argument("--date", default=None, help="edition close date YYYY-MM-DD (default: last bar)")
    ap.add_argument("--backfill", type=int, default=0, help="also scan the N sessions before --date")
    ap.add_argument("--fresh", action="store_true", help="delete the research store first")
    ap.add_argument("--llm", default="none", help="none | auto | recorded | live (default none)")
    args = ap.parse_args()

    cfg = load_config()
    if args.fresh and Path(cfg.research_db).exists():
        Path(cfg.research_db).unlink()
    store = ResearchStore(cfg.research_db)
    narrator = _narrator(args.llm)

    full = MarketData.load(cfg, as_of=args.date)
    sessions = full.sessions
    target = full.as_of
    i = sessions.index(target)
    dates = sessions[max(0, i - args.backfill): i + 1]

    for d in dates:
        md = full.sealed(d)
        rep = run_scan(cfg, store, md=md, narrator=narrator)
        print("=" * 96)
        if rep.skipped:
            print(f"EDITION {rep.edition_date}: already published (append-only) — graded {len(rep.graded)} due finding(s)")
        else:
            print(f"EDITION {rep.edition_date} · data as of {rep.data_as_of} · regime {rep.regime}")
            print(f"scanned {rep.universe_scanned} names · {rep.candidates} candidate(s) · "
                  f"{len(rep.published)} published · threshold {cfg.usefulness_threshold:.2f} · "
                  f"narration: {rep.llm_provider}")
            for line in rep.published:
                print("  PUBLISHED  " + line)
            for line in rep.held:
                print("  HELD       " + line)
            for line in rep.below_threshold:
                print("  BELOW      " + line)
            if rep.narration_failures:
                print(f"  narration fell back to the engine on {len(rep.narration_failures)} card(s)")
        for fid, v in rep.graded:
            print(f"  GRADED     {fid} -> {v.upper()}")
        sb = store.scoreboard(rep.data_as_of)
        print(f"  SCOREBOARD Right {sb.right} · Wrong {sb.wrong} · Inconclusive {sb.inconclusive} · "
              f"n={sb.n} · pending {sb.pending}")
    store.close()
    print(f"\nwritten to {cfg.research_db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
