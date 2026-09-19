"""One entry point for the repair workflow.

    python -m market_data.repair.cli wait          # block until W1/W2 import
    python -m market_data.repair.cli diagnose
    python -m market_data.repair.cli plan
    python -m market_data.repair.cli refresh
    python -m market_data.repair.cli reconcile
    python -m market_data.repair.cli report
    python -m market_data.repair.cli unify-basis   # one basis per symbol
    python -m market_data.repair.cli freeze        # the snapshot the rerun reads
    python -m market_data.repair.cli all           # the whole chain

``unify-basis`` and ``freeze`` are the follow-up pass: the first re-fetches the
full history of every symbol the store *measures* to be on two adjustment bases
(re-fetching only disputed windows left some symbols split), the second freezes
a snapshot over NIFTY 500 minus quarantined symbols with the repair's unusable
labels excluded.  They are not in ``all`` because ``unify-basis`` costs a real
fetch and ``freeze`` should run once, after it.

``all`` stops at the first failing stage rather than reporting on a stage that
did not happen.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional, Sequence

from market_data.repair import ARTIFACTS, MARKET15_DB, dependencies_ready

REQUIRED = ("market_data.provider", "market_data.kite_provider",
            "market_data.store", "market_data.validate",
            "market_data.aggregate", "market_data.calendar")


def cmd_wait(a) -> int:
    """Poll until W1's provider and W2's store/validate import cleanly."""
    deadline = time.time() + a.timeout
    while True:
        ready = dependencies_ready()
        missing = [m for m in REQUIRED if not ready.get(m)]
        if not missing:
            print(json.dumps({"ready": True, "modules": ready}, indent=2))
            return 0
        if time.time() > deadline:
            print(json.dumps({"ready": False, "missing": missing}, indent=2))
            return 1
        print(f"[wait] still missing: {', '.join(missing)}", flush=True)
        time.sleep(a.interval)


def cmd_diagnose(a) -> int:
    from market_data.repair import diagnose

    diagnose.run(a.symbols, workers=a.workers, out_dir=Path(a.artifacts) / "diagnose")
    return 0


def cmd_plan(a) -> int:
    from market_data.repair import plan as P

    p = P.build_plan(diagnose_dir=Path(a.artifacts) / "diagnose", offline=a.offline)
    print(p.render())
    print(f"\nwritten: {P.save(p, Path(a.artifacts) / 'plan' / 'fetch_plan.json')}")
    return 0


def cmd_refresh(a) -> int:
    from market_data.repair import plan as P, refresh as R

    p = P.load(Path(a.artifacts) / "plan" / "fetch_plan.json")
    R.run(p, db=Path(a.db), threads=a.threads, limit=a.limit,
          resume=not a.no_resume, run_id=a.run_id,
          out_dir=Path(a.artifacts) / "refresh")
    return 0


def cmd_reconcile(a) -> int:
    from market_data.repair import reconcile as RC

    RC.run(a.symbols, market15=Path(a.db), run_id=a.run_id, write=not a.dry_run,
           out_dir=Path(a.artifacts) / "reconcile")
    return 0


def cmd_report(a) -> int:
    from market_data.repair import report as RP

    d = RP.build(artifacts=Path(a.artifacts), market15=Path(a.db),
                 snapshot=not a.no_snapshot, snapshot_id=a.snapshot_id)
    if d.get("snapshot"):
        print(f"snapshot_id: {d['snapshot']['snapshot_id']}")
    return 0


def cmd_unify_basis(a) -> int:
    from market_data.repair import unify_basis as UB

    out = UB.run(db=Path(a.db), symbols=a.symbols, threads=a.threads,
                 run_id=a.run_id, dry_run=a.dry_run,
                 allow_concurrent=a.allow_concurrent,
                 out_dir=Path(a.artifacts) / "unify_basis")
    print(json.dumps({k: v for k, v in out.items()
                      if k not in ("evidence", "verification")},
                     indent=2, default=str))
    return 0


def cmd_freeze(a) -> int:
    from market_data.repair import freeze_rerun as FR

    out = FR.run(db=Path(a.db), snapshot_id=a.snapshot_id, start=a.start,
                 end=a.end, allow_concurrent=a.allow_concurrent,
                 verify=not a.no_verify, out_dir=Path(a.artifacts) / "freeze")
    print(json.dumps(out["snapshot"], indent=2, default=str))
    return 0


def cmd_all(a) -> int:
    for fn in (cmd_diagnose, cmd_plan, cmd_refresh, cmd_reconcile, cmd_report):
        rc = fn(a)
        if rc:
            print(f"[all] stopped: {fn.__name__} returned {rc}", file=sys.stderr)
            return rc
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="market_data.repair")
    ap.add_argument("--artifacts", default=str(ARTIFACTS))
    ap.add_argument("--db", default=str(MARKET15_DB))
    sub = ap.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser("wait"); w.set_defaults(fn=cmd_wait)
    w.add_argument("--interval", type=float, default=60.0)
    w.add_argument("--timeout", type=float, default=3600.0)

    d = sub.add_parser("diagnose"); d.set_defaults(fn=cmd_diagnose)
    d.add_argument("--symbols", nargs="*")
    d.add_argument("--workers", type=int, default=10)

    p = sub.add_parser("plan"); p.set_defaults(fn=cmd_plan)
    p.add_argument("--offline", action="store_true")

    r = sub.add_parser("refresh"); r.set_defaults(fn=cmd_refresh)
    r.add_argument("--threads", type=int, default=6)
    r.add_argument("--limit", type=int)
    r.add_argument("--no-resume", action="store_true")
    r.add_argument("--run-id")

    c = sub.add_parser("reconcile"); c.set_defaults(fn=cmd_reconcile)
    c.add_argument("--symbols", nargs="*")
    c.add_argument("--run-id")
    c.add_argument("--dry-run", action="store_true")

    o = sub.add_parser("report"); o.set_defaults(fn=cmd_report)
    o.add_argument("--no-snapshot", action="store_true")
    o.add_argument("--snapshot-id")

    u = sub.add_parser("unify-basis"); u.set_defaults(fn=cmd_unify_basis)
    u.add_argument("--symbols", nargs="*")
    u.add_argument("--threads", type=int, default=4)
    u.add_argument("--run-id")
    u.add_argument("--dry-run", action="store_true")
    u.add_argument("--allow-concurrent", action="store_true")

    f = sub.add_parser("freeze"); f.set_defaults(fn=cmd_freeze)
    f.add_argument("--snapshot-id")
    f.add_argument("--start")
    f.add_argument("--end")
    f.add_argument("--allow-concurrent", action="store_true")
    f.add_argument("--no-verify", action="store_true")

    al = sub.add_parser("all"); al.set_defaults(fn=cmd_all)
    al.add_argument("--symbols", nargs="*")
    al.add_argument("--workers", type=int, default=10)
    al.add_argument("--threads", type=int, default=6)
    al.add_argument("--limit", type=int)
    al.add_argument("--offline", action="store_true")
    al.add_argument("--no-resume", action="store_true")
    al.add_argument("--run-id")
    al.add_argument("--dry-run", action="store_true")
    al.add_argument("--no-snapshot", action="store_true")
    al.add_argument("--snapshot-id")

    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    raise SystemExit(main())
