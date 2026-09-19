"""CLI for the derivatives metrics layer (worker D2).

Separate from D1's ``cli.py`` on purpose: that one captures and stores, this one
only computes from what is already stored and prints the read shapes the
Derivative tab will call.  Nothing here fetches from a vendor.

    py -m market_data.derivatives.metrics_cli floors
    py -m market_data.derivatives.metrics_cli compute --at "2026-09-18 11:15:00"
    py -m market_data.derivatives.metrics_cli unusual --limit 10
    py -m market_data.derivatives.metrics_cli chain --underlying RELIANCE --expiry 2026-09-25
    py -m market_data.derivatives.metrics_cli oi-by-strike --underlying NIFTY --expiry 2026-09-25
    py -m market_data.derivatives.metrics_cli index
    py -m market_data.derivatives.metrics_cli futures

Every subcommand prints JSON on stdout: the same payload the API layer serves,
so what D3 builds against is exactly what is printed here.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data.derivatives import metrics as M  # noqa: E402


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="metrics_cli", description=__doc__)
    p.add_argument("--db", default=str(M.DEFAULT_DB_PATH), help="path to db/derivatives.db")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("floors", help="print the liquidity floors in force")

    c = sub.add_parser("compute", help="compute + store every signal for one 15-minute mark")
    c.add_argument("--at", type=_dt, required=True, help="'YYYY-MM-DD HH:MM:SS'")
    c.add_argument("--underlying", action="append", dest="underlyings")
    c.add_argument("--dry-run", action="store_true", help="compute but do not write")

    u = sub.add_parser("unusual", help="card 1: unusual activity per underlying")
    u.add_argument("--at", type=_dt)
    u.add_argument("--underlying", action="append", dest="underlyings")
    u.add_argument("--min-dte", type=int, dest="min_dte")
    u.add_argument("--limit", type=int, default=25)

    for name, help_text in (("chain", "card 2: option chain"), ("oi-by-strike", "card 3: OI by strike")):
        s = sub.add_parser(name, help=help_text)
        s.add_argument("--underlying", required=True)
        s.add_argument("--expiry", type=_date, required=True)
        s.add_argument("--at", type=_dt)

    i = sub.add_parser("index", help="card 4: index dashboard (PCR, max pain, OI shift)")
    i.add_argument("--at", type=_dt)
    i.add_argument("--underlying", action="append", dest="underlyings")

    f = sub.add_parser("futures", help="card 5: futures OI build-up table")
    f.add_argument("--at", type=_dt)
    f.add_argument("--underlying", action="append", dest="underlyings")
    f.add_argument("--all", action="store_true", help="include rows below the floors")
    f.add_argument("--limit", type=int, default=100)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "floors":
        print(M.dumps(M.describe_floors()))
        return 0

    write = args.cmd == "compute" and not args.dry_run
    try:
        conn = M.connect(args.db, readonly=not write)
    except (LookupError, sqlite3.OperationalError) as exc:
        print(M.dumps({"error": str(exc), "db": args.db}))
        return 2
    try:
        if args.cmd == "compute":
            res = M.compute_for_mark(
                conn, args.at, underlyings=args.underlyings, write=write
            )
            print(M.dumps({
                "captured_at": res.captured_at,
                "contract_rows": len(res.contract_rows),
                "underlying_rows": len(res.underlying_rows),
                "futures": len(res.futures),
                "written": res.written,
                "skipped_no_snapshot": res.skipped_no_snapshot,
                "floors": M.describe_floors(),
            }))
        elif args.cmd == "unusual":
            print(M.dumps(M.read_unusual_activity(
                conn, as_of=args.at, underlyings=args.underlyings,
                min_days_to_expiry=args.min_dte, limit=args.limit,
            )))
        elif args.cmd == "chain":
            print(M.dumps(M.read_option_chain(conn, args.underlying, args.expiry, as_of=args.at)))
        elif args.cmd == "oi-by-strike":
            print(M.dumps(M.read_oi_by_strike(conn, args.underlying, args.expiry, as_of=args.at)))
        elif args.cmd == "index":
            kwargs = {"as_of": args.at}
            if args.underlyings:
                kwargs["underlyings"] = args.underlyings
            print(M.dumps(M.read_index_summary(conn, **kwargs)))
        elif args.cmd == "futures":
            print(M.dumps(M.read_futures_buildup(
                conn, as_of=args.at, underlyings=args.underlyings,
                only_floor_passing=not args.all, limit=args.limit,
            )))
    except LookupError as exc:
        print(M.dumps({"error": str(exc)}))
        return 2
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
