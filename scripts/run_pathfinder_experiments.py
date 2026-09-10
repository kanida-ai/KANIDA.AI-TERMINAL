"""
Run the Pathfinder EXPERIMENT LOOP (Session S2) over the S1 research store's editions.

    python scripts/run_pathfinder_experiments.py                    # every S1 edition not yet stepped, in order
    python scripts/run_pathfinder_experiments.py --date 2026-07-29  # up to and including this edition
    python scripts/run_pathfinder_experiments.py --archive-store --i-understand-this-archives-the-store
    python scripts/run_pathfinder_experiments.py --llm none|auto|recorded|live

Each step is sealed at its edition date: the frame the step sees holds no later bar, and every
row it writes says whether it was generated on that session's own date (forward) or after it
(a SIMULATED BACKFILL — S1 second audit A1). The registry is append-only and hash-chained; it
is never deleted by this script — `--archive-store` renames it aside.

Prerequisite: the S1 store (scripts/run_pathfinder_scan.py) holds the editions to step over.
Nothing here computes. It wires the pieces together and prints what the loop did.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")   # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    pass

from pathfinder.engine.governance import load_constitution                   # noqa: E402
from pathfinder.experiments.config import load_experiment_config             # noqa: E402
from pathfinder.experiments.loop import run_experiment_step                  # noqa: E402
from pathfinder.experiments.narrate import ExperimentNarrator                # noqa: E402
from pathfinder.experiments.store import ExperimentStore                     # noqa: E402
from pathfinder.research.config import load_config                           # noqa: E402
from pathfinder.research.data import MarketData                              # noqa: E402
from pathfinder.research.store import ResearchStore                          # noqa: E402

ACK = "--i-understand-this-archives-the-store"


def archive_store(path: str, *, acknowledged: bool, now: datetime | None = None) -> Path | None:
    p = Path(path)
    if not p.exists():
        return None
    if not acknowledged:
        raise SystemExit(f"refusing to archive {p}: the registry is append-only; pass {ACK} to move it aside")
    stamp = (now or datetime.now()).strftime("%Y%m%dT%H%M%S")
    target = p.with_name(f"{p.name}.archived-{stamp}")
    k = 1
    while target.exists():
        target = p.with_name(f"{p.name}.archived-{stamp}-{k}")
        k += 1
    p.rename(target)
    return target


def _narrator(mode: str, constitution) -> ExperimentNarrator:
    if mode == "none":
        return ExperimentNarrator(None, provider_name="none")
    from pathfinder.llm.factory import build_llm
    llm, provider, ledger = build_llm(constitution, mode=mode)
    if provider == "none":
        return ExperimentNarrator(None, provider_name="none")
    return ExperimentNarrator(llm, provider_name=provider, budget=ledger.budget)


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the Pathfinder experiment loop over the S1 editions.")
    ap.add_argument("--date", default=None, help="step every S1 edition up to and including this date (default: all)")
    ap.add_argument("--archive-store", action="store_true", help="move the registry aside (renamed, never deleted)")
    ap.add_argument(ACK, dest="ack_archive", action="store_true")
    ap.add_argument("--llm", default="none", help="none | auto | recorded | live (default none)")
    args = ap.parse_args()

    rcfg, xcfg = load_config(), load_experiment_config()
    if args.archive_store:
        moved = archive_store(xcfg.experiments_db, acknowledged=args.ack_archive)
        if moved is not None:
            print(f"archived the previous registry to {moved}")
    if not Path(rcfg.research_db).exists():
        raise SystemExit(f"no S1 research store at {rcfg.research_db}; run scripts/run_pathfinder_scan.py first")
    rstore = ResearchStore(rcfg.research_db, claim_tolerance_pp=rcfg.claim_tolerance_pp)
    xstore = ExperimentStore(xcfg.experiments_db)
    constitution = load_constitution(xcfg.constitution_path)
    narrator = _narrator(args.llm, constitution)
    editions = [d for d in rstore.editions() if args.date is None or d <= args.date]
    if not editions:
        raise SystemExit("no S1 editions to step over")
    full = MarketData.load(rcfg, as_of=editions[-1])
    print(f"S1 editions {editions[0]} .. {editions[-1]} ({len(editions)}) · data through {full.data_through} · "
          f"constitution {constitution.version} ({'signed' if constitution.is_signed else 'UNSIGNED'}) · engine {xcfg.engine_version}")
    for d in editions:
        if xstore.has_edition(d):
            continue
        md = full.sealed(d)
        rep = run_experiment_step(rcfg, xcfg, rstore, xstore, md=md, constitution=constitution, narrator=narrator)
        label = "BACKFILLED (simulated — not a forward record)" if rep.backfilled else "FORWARD (generated on its session date)"
        print("=" * 96)
        print(f"STEP {rep.edition_date} · {label}")
        for line in rep.opened:
            print("  OPENED     " + line)
        for line in rep.declined:
            print("  DECLINED   " + line)
        for line in rep.tracked:
            print("  TRACKED    " + line)
        for eid, v, p, verdict in rep.graded:
            print(f"  GRADED     {eid} v{v} period {p} -> {verdict.upper()}")
        for line in rep.revised:
            print("  REVISED    " + line)
        for line in rep.buried:
            print("  BURIED     " + line)
        for line in rep.proposed:
            print("  PROPOSED   " + line)
        if rep.narrated:
            print(f"  NARRATED   {', '.join(rep.narrated)} (provider {rep.llm_provider})")
        sb = xstore.scoreboard(d)
        print(f"  SCOREBOARD Right {sb.right} · Wrong {sb.wrong} · Inconclusive {sb.inconclusive} · n={sb.n} · void {sb.void} · "
              f"pending {sb.pending} · forward {sb.forward.n} · backfilled {sb.backfilled.n} · testing {sb.experiments_testing} · "
              f"buried {sb.experiments_buried} · proposed {sb.experiments_proposed} · trials {sb.trials_total} · {sb.record_label}")
    ok = xstore.verify_all_chains()
    for t, good, msg in ok:
        print(("  CHAIN OK   " if good else "  CHAIN BAD  ") + msg)
    xstore.close()
    rstore.close()
    print(f"\nwritten to {xcfg.experiments_db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
