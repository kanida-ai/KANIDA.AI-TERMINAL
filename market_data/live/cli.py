"""Operator front end for the live 15-minute ingest loop.

    python -m market_data.live.cli status
    python -m market_data.live.cli once   --symbols RELIANCE,TCS
    python -m market_data.live.cli run    --interval 300
    python -m market_data.live.cli plan   --limit 20
    python -m market_data.live.cli quarantine list
    python -m market_data.live.cli quarantine sync          (probe the universe)
    python -m market_data.live.cli quarantine release LTIM

Symbols the provider cannot serve at all are skipped by default (they produced
one error per cycle each and nothing else, which made ``/api/state`` report a
data warning about the loop rather than about the data).  ``--include-quarantined``
puts them back in; either way they are re-probed once a day and released
automatically the moment the provider serves them.

`run` is the long-lived loop: one catch-up pass, then a cycle every
``--interval`` seconds while the market is open, idling to the next open
outside hours.  Ctrl-C stops it cleanly; the next start resumes from what the
store holds, because the plan is derived from stored state, not from memory.

Only one writer at a time: the loop takes the store's advisory writer lock
unless ``--allow-concurrent`` is given (which you want only when you know the
other writer is working on different symbols).
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
from datetime import datetime

from market_data.live.calendar_ext import live_calendar, nifty500
from market_data.live.ingest import (
    LiveIngest,
    now_ist,
    daily_coverage,
    latest_completed_bar,
    latest_live_bar,
    plan_symbol,
)
from market_data.quarantine import DEFAULT_RECHECK_HOURS, NOT_IN_INSTRUMENT_LIST
from market_data.store import DEFAULT_DB_PATH, MarketStore, WriterLock, open_readonly


def _log(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _universe(args):
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] \
        if args.symbols else None
    uni = nifty500(index_column=args.index, symbols=symbols)
    if args.limit:
        uni = uni[: args.limit]
    return uni


def _build(args) -> LiveIngest:
    from market_data import get_provider

    store = MarketStore(args.db)
    provider = get_provider(args.provider)
    return LiveIngest(store, provider, _universe(args), live_calendar(),
                      workers=args.workers, backfill_sessions=args.backfill_sessions,
                      daily_history_days=args.daily_history_days,
                      fetch_daily=not args.no_daily,
                      include_quarantined=args.include_quarantined,
                      quarantine_recheck_hours=args.quarantine_recheck_hours)


def cmd_status(args) -> int:
    store = open_readonly(args.db)
    try:
        newest = latest_live_bar(store)
        cov = store.con.execute(
            "SELECT COUNT(DISTINCT symbol), COUNT(*) FROM candles_15m").fetchone()
        runs = [dict(r) for r in store.con.execute(
            "SELECT run_id,started_at,finished_at,requests,rows,errors,status "
            "FROM ingest_runs ORDER BY started_at DESC LIMIT 5")]
        try:
            daily = store.con.execute(
                "SELECT COUNT(DISTINCT symbol), COUNT(*), MAX(session_date) "
                "FROM daily_bars").fetchone()
        except Exception:
            daily = (0, 0, None)
        out = {
            "db": str(store.path),
            "symbols": cov[0], "rows_15m": cov[1],
            "newest_bar_start": newest.isoformat(sep=" ") if newest else None,
            "daily_symbols": daily[0], "daily_rows": daily[1],
            "daily_through": daily[2],
            "last_cycle": store.get_meta("live.last_cycle"),
            "last_cycle_at": store.get_meta("live.last_cycle_finished_at"),
            "recent_runs": runs,
        }
        print(json.dumps(out, indent=2, default=str))
    finally:
        store.close()
    return 0


def cmd_plan(args) -> int:
    from market_data import get_provider

    store = open_readonly(args.db)
    provider = get_provider(args.provider)
    calendar = live_calendar()
    latest = latest_completed_bar(provider, calendar, now_ist())
    rows = []
    for info in _universe(args):
        row = store.con.execute("SELECT MAX(bar_start) FROM candles_15m WHERE symbol=?",
                                (info.symbol,)).fetchone()
        last = datetime.fromisoformat(row[0]) if row and row[0] else None
        plan = plan_symbol(info.symbol, info.instrument_id, last, latest, calendar,
                           args.backfill_sessions)
        n_daily, daily_last = daily_coverage(store, info.symbol) \
            if _has_daily(store) else (0, None)
        rows.append({"symbol": info.symbol, "reason": plan.reason,
                     "last_stored": last.isoformat(sep=" ") if last else None,
                     "from": plan.start.isoformat(sep=" ") if plan.start else None,
                     "to": plan.end.isoformat(sep=" ") if plan.end else None,
                     "daily_rows": n_daily,
                     "daily_through": daily_last.isoformat() if daily_last else None})
    store.close()
    print(json.dumps({"latest_completed_bar":
                      latest.isoformat(sep=" ") if latest else None,
                      "symbols": len(rows),
                      "to_fetch": sum(r["reason"] in ("append", "backfill") for r in rows),
                      "plans": rows}, indent=2))
    return 0


def cmd_quarantine(args) -> int:
    """List / probe / hand-edit the persisted quarantine."""
    from market_data import get_provider
    from market_data.quarantine import due_symbols, summarise, sync

    action = args.action
    if action == "list":
        store = open_readonly(args.db)
        try:
            rows = summarise(store)
            print(json.dumps({"quarantined": len(rows),
                              "due_for_recheck": sorted(due_symbols(
                                  store.quarantined(),
                                  hours=args.quarantine_recheck_hours)),
                              "symbols": rows}, indent=2, default=str))
        finally:
            store.close()
        return 0

    store = MarketStore(args.db)
    lock = None
    try:
        if not args.allow_concurrent:
            lock = WriterLock(store.path).acquire()
        if action == "sync":
            provider = get_provider(args.provider)
            symbols = [i.symbol for i in _universe(args)]
            out = sync(store, provider, symbols, run_id=args.run_id)
            print(json.dumps({k: sorted(v) if k != "ok" else len(v)
                              for k, v in out.items()}, indent=2))
        elif action == "add":
            for symbol in args.symbols_positional:
                store.quarantine(symbol, reason=args.reason,
                                 detail=args.detail or args.reason,
                                 run_id=args.run_id)
            print(json.dumps(summarise(store), indent=2, default=str))
        elif action == "release":
            for symbol in args.symbols_positional:
                store.release_quarantine(symbol, detail=args.detail or
                                         "released by operator", run_id=args.run_id)
            print(json.dumps(summarise(store), indent=2, default=str))
        else:  # pragma: no cover - argparse rejects anything else
            raise SystemExit(f"unknown quarantine action {action!r}")
    finally:
        if lock:
            lock.release()
        store.close()
    return 0


def _has_daily(store) -> bool:
    return bool(store.con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='daily_bars'").fetchone())


def cmd_once(args) -> int:
    ingest = _build(args)
    lock = None
    try:
        if not args.allow_concurrent:
            lock = WriterLock(ingest.store.path).acquire()
        result = ingest.run_cycle(do_daily=True if args.daily else None)
        print(json.dumps(result.as_dict(), indent=2))
        if args.verbose_symbols:
            for r in result.per_symbol:
                if r.written or r.error:
                    print(f"  {r.symbol:<14} {r.status:<10} fetched={r.fetched} "
                          f"written={r.written} restated={r.restated} "
                          f"last={r.last_bar} {r.error or ''}")
        return 0 if not result.errors else 1
    finally:
        if lock:
            lock.release()
        ingest.store.close()
    return 0


def cmd_run(args) -> int:
    ingest = _build(args)
    stop = threading.Event()

    def _stop(*_):
        logging.getLogger("market_data.live").info("stop requested; finishing cycle")
        stop.set()

    signal.signal(signal.SIGINT, _stop)
    try:
        signal.signal(signal.SIGTERM, _stop)
    except (AttributeError, ValueError):  # pragma: no cover - platform dependent
        pass

    lock = None
    try:
        if not args.allow_concurrent:
            lock = WriterLock(ingest.store.path).acquire()
        results = ingest.run_forever(interval_seconds=args.interval, stop=stop,
                                     max_cycles=args.max_cycles)
        print(json.dumps({"cycles": len(results),
                          "rows": sum(r.rows for r in results),
                          "requests": sum(r.requests for r in results)}, indent=2))
        return 0
    finally:
        if lock:
            lock.release()
        ingest.store.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="market_data.live.cli",
                                description="Incremental live 15-minute ingest")
    p.add_argument("--db", default=str(DEFAULT_DB_PATH))
    p.add_argument("--provider", default=None,
                   help="override $MARKET_DATA_PROVIDER (kite|vendor15|fake)")
    p.add_argument("--index", default="in_nifty500")
    p.add_argument("--symbols", default=None, help="comma-separated subset")
    p.add_argument("--limit", type=int, default=0, help="first N universe members")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--backfill-sessions", type=int, default=10, dest="backfill_sessions")
    p.add_argument("--daily-history-days", type=int, default=2200,
                   dest="daily_history_days")
    p.add_argument("--no-daily", action="store_true",
                   help="skip the provider daily-bar pass (1D/1W closes)")
    p.add_argument("--allow-concurrent", action="store_true",
                   help="do not take the store's advisory writer lock")
    p.add_argument("--include-quarantined", action="store_true",
                   dest="include_quarantined",
                   help="fetch symbols the provider has been unable to serve "
                        "(they are skipped by default and re-probed daily)")
    p.add_argument("--quarantine-recheck-hours", type=float,
                   default=DEFAULT_RECHECK_HOURS, dest="quarantine_recheck_hours",
                   help="how long a quarantine stands before it is re-probed")
    p.add_argument("--run-id", default=None,
                   help="tag quarantine changes with this run id")
    p.add_argument("--log-level", default="INFO")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="what the store holds right now").set_defaults(
        func=cmd_status)
    pl = sub.add_parser("plan", help="what the next cycle would request (no I/O writes)")
    pl.set_defaults(func=cmd_plan)
    once = sub.add_parser("once", help="run exactly one cycle")
    once.add_argument("--daily", action="store_true",
                      help="force the daily pass even mid-session")
    once.add_argument("--verbose-symbols", action="store_true")
    once.set_defaults(func=cmd_once)
    run = sub.add_parser("run", help="run the loop until stopped")
    run.add_argument("--interval", type=float, default=300.0)
    run.add_argument("--max-cycles", type=int, default=None, dest="max_cycles")
    run.set_defaults(func=cmd_run)
    q = sub.add_parser("quarantine",
                       help="symbols the provider cannot serve (list|sync|add|release)")
    q.add_argument("action", choices=("list", "sync", "add", "release"))
    q.add_argument("symbols_positional", nargs="*", metavar="SYMBOL",
                   help="symbols for add/release")
    q.add_argument("--reason", default=NOT_IN_INSTRUMENT_LIST)
    q.add_argument("--detail", default="")
    q.set_defaults(func=cmd_quarantine)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    _log(args.log_level)
    func = getattr(args, "func", cmd_status)
    return func(args)


if __name__ == "__main__":
    sys.exit(main())
