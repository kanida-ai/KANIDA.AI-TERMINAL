"""
Run the Pathfinder after-close scan (Session S1) and persist the edition.

    python scripts/run_pathfinder_scan.py                       # latest close, engine narration
    python scripts/run_pathfinder_scan.py --date 2026-07-29     # a specific close (point-in-time)
    python scripts/run_pathfinder_scan.py --backfill 10         # the 10 sessions before --date, then --date
    python scripts/run_pathfinder_scan.py --archive-store --i-understand-this-archives-the-store
                                                                # move the store aside (never deleted), start empty
    python scripts/run_pathfinder_scan.py --llm none|recorded|auto|live

Every scan is sealed at its date: a backfilled edition for D is computed from bars <= D only,
and the grading pass at each later date grades what completed by then. Editions are
append-only — a date that already has one is skipped (its due findings are still graded).

An edition generated after its own session date is a SIMULATED BACKFILL and is labelled so
(S1 second audit A1); only a scan run on the session's own date is a forward record.

The store is append-only and is NEVER deleted by this script (A8): `--archive-store` renames it
to `<store>.archived-<timestamp>` and requires the explicit acknowledgement flag.

Nothing here computes. It wires the pieces together and prints what the engine found.
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

from pathfinder.research.config import load_config              # noqa: E402
from pathfinder.research.data import MarketData                 # noqa: E402
from pathfinder.research.narrate import Narrator                # noqa: E402
from pathfinder.research.scan import run_scan                   # noqa: E402
from pathfinder.research.store import ResearchStore             # noqa: E402

ACK = "--i-understand-this-archives-the-store"


def archive_store(path: str, *, acknowledged: bool, now: datetime | None = None) -> Path | None:
    """Rename the store aside. Never deletes. Refuses without the explicit acknowledgement."""
    p = Path(path)
    if not p.exists():
        return None
    if not acknowledged:
        raise SystemExit(f"refusing to archive {p}: the store is append-only; pass {ACK} to move it aside")
    stamp = (now or datetime.now()).strftime("%Y%m%dT%H%M%S")
    target = p.with_name(f"{p.name}.archived-{stamp}")
    k = 1
    while target.exists():
        target = p.with_name(f"{p.name}.archived-{stamp}-{k}")
        k += 1
    p.rename(target)
    return target


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
    ap.add_argument("--archive-store", action="store_true",
                    help="move the existing research store aside (renamed, never deleted) and start empty")
    ap.add_argument(ACK, dest="ack_archive", action="store_true",
                    help="required with --archive-store: acknowledge the append-only store is being moved aside")
    ap.add_argument("--fresh", action="store_true", help=argparse.SUPPRESS)   # removed (A8): it deleted the store
    ap.add_argument("--llm", default="none", help="none | auto | recorded | live (default none)")
    args = ap.parse_args()
    if args.fresh:
        raise SystemExit("--fresh was removed: it deleted an append-only store. Use --archive-store " + ACK)

    cfg = load_config()
    if args.archive_store:
        moved = archive_store(cfg.research_db, acknowledged=args.ack_archive)
        if moved is not None:
            print(f"archived the previous store to {moved}")
    store = ResearchStore(cfg.research_db, claim_tolerance_pp=cfg.claim_tolerance_pp)
    narrator = _narrator(args.llm)

    full = MarketData.load(cfg, as_of=args.date)
    sessions = full.sessions
    target = full.as_of
    i = sessions.index(target)
    dates = sessions[max(0, i - args.backfill): i + 1]
    if full.exclusions:
        print(f"data rules: {full.exclusions.as_dict()} · data through {full.data_through}")

    for d in dates:
        md = full.sealed(d)
        rep = run_scan(cfg, store, md=md, narrator=narrator)
        print("=" * 96)
        label = "BACKFILLED (simulated — not a forward record)" if rep.backfilled else "FORWARD (generated on its session date)"
        if rep.skipped:
            print(f"EDITION {rep.edition_date}: already published (append-only) — graded {len(rep.graded)} due finding(s)")
        else:
            print(f"EDITION {rep.edition_date} · data as of {rep.data_as_of} · regime {rep.regime} · {label}")
            print(f"scanned {rep.universe_scanned} names · {rep.candidates} candidate(s) · "
                  f"{len(rep.published)} published · {len(rep.continued)} continued · threshold "
                  f"{cfg.usefulness_threshold:.2f} · narration: {rep.llm_provider}")
            for line in rep.published:
                print("  PUBLISHED  " + line)
            for line in rep.continued:
                print("  CONTINUE   " + line)
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
              f"n={sb.n} (independent; {sb.n_total} grade rows, {sb.regraded} re-grades folded, "
              f"{sb.continued} continuations folded, {sb.void} void) · "
              f"forward {sb.forward.n} · backfilled {sb.backfilled.n} · pending {sb.pending} · {sb.record_label}")
    store.close()
    print(f"\nwritten to {cfg.research_db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
