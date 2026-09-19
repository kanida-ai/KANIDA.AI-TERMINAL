"""Command line for the derivatives capture pipeline.

    python -m market_data.derivatives.cli status
    python -m market_data.derivatives.cli plan
    python -m market_data.derivatives.cli once [--mark 09:45] [--force]
    python -m market_data.derivatives.cli run   [--days 1] [--until 15:50]
    python -m market_data.derivatives.cli backfill [--workers 4] [--max-contracts N]
    python -m market_data.derivatives.cli backfill-daily [--workers 4]
    python -m market_data.derivatives.cli prune [--dry-run]   # also runs daily inside `run`
    python -m market_data.derivatives.cli rollup [--date YYYY-MM-DD]

Run from the repo root with ``market_scanner/.venv/Scripts/python.exe``.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, time

from . import config
from .capture import (
    DerivativesCapture,
    SessionDays,
    _fmt,
    build,
    marks_for,
    now_ist,
    snapshot_id_for,
)
from .store import DerivativesStore, open_readonly


def _log(level: str, log_file: str | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        handlers=handlers, force=True)
    logging.Formatter.converter = lambda *a: (now_ist().timetuple())


def _parse_mark(text: str, day: date) -> datetime:
    t = time.fromisoformat(text if len(text) > 5 else text + ":00")
    return datetime.combine(day, t.replace(second=0, microsecond=0))


# ── commands ─────────────────────────────────────────────────────────────────

def cmd_status(args) -> int:
    try:
        store = open_readonly(args.db)
    except Exception:
        store = DerivativesStore(args.db)
    st = store.status()
    today = now_ist().date()
    caps = store.captures_for(today.isoformat())
    st["today"] = today.isoformat()
    st["marks_today"] = len(marks_for(today))
    st["captured_today"] = sum(1 for r in caps.values() if r["status"] == "ok")
    st["missed_today"] = sum(1 for r in caps.values() if r["status"] == "missed")
    last = store.recent_captures(1)
    if last:
        r = last[0]
        st["last_capture"] = {k: r[k] for k in (
            "mark_at", "status", "rows_written", "rows_skipped", "requests",
            "wall_seconds", "lag_seconds")}
    if args.json:
        print(json.dumps(st, indent=2, default=str))
    else:
        for k, v in st.items():
            print(f"{k:22} {v}")
    return 0


def cmd_plan(args) -> int:
    cap = build(db_path=args.db)
    scope = cap.ensure_scope(refresh=args.refresh)
    counts = scope.counts()
    day = now_ist().date()
    marks = marks_for(day)
    batches = -(-len(scope.contracts) // config.QUOTE_BATCH)
    rate = getattr(cap.provider, "rate_limit_per_second", 3.0) or 3.0
    print(f"listed NFO instruments      {scope.total_listed}")
    print(f"expired/ignored             {scope.dropped_expired}")
    print(f"in scope (front {config.EXPIRIES_PER_UNDERLYING} expiries)  "
          f"{counts['total']}  (CE {counts['CE']} · PE {counts['PE']} · FUT {counts['FUT']})")
    print(f"underlyings                 {counts['underlyings']}")
    print(f"quote requests per cycle    {batches} (+1 spot) at {config.QUOTE_BATCH}/call")
    print(f"limiter time per cycle      ~{(batches + 1) / rate:.1f}s at {rate:g} req/s")
    print(f"marks per session           {len(marks)}  ({_fmt(marks[0][0])[11:16]} … "
          f"{_fmt(marks[-2][0])[11:16]} + {_fmt(marks[-1][0])[11:16]} post-close)")
    print(f"rows per session (max)      {counts['total'] * len(marks):,}")
    if args.expiries:
        for (u, kind), exps in sorted(scope.expiries.items())[:args.expiries]:
            print(f"  {u:14} {kind:3} {[e.isoformat() for e in exps]}")
    return 0


def cmd_once(args) -> int:
    cap = build(db_path=args.db)
    mark = _parse_mark(args.mark, now_ist().date()) if args.mark else None
    with cap.store.writer_lock():
        res = cap.run_once(mark, force=args.force)
    print(res.line())
    return 0 if res.status in ("ok", "already_done", "no_mark_due") else 1


def cmd_run(args) -> int:
    cap = build(db_path=args.db, prune=not args.no_prune)
    until = _parse_mark(args.until, now_ist().date()) if args.until else None
    with cap.store.writer_lock():
        results = cap.run_forever(days=args.days, until=until)
    ok = sum(1 for r in results if r.status == "ok")
    print(f"{ok}/{len(results)} cycles ok")
    return 0


def cmd_backfill(args) -> int:
    from .backfill import run_backfill

    if args.rate:
        # Kite counts requests per API key, and the process-wide bucket cannot
        # know about a second process.  Lowering this process's rate is how the
        # backfill stays a good citizen while the capture loop (and the live
        # equity loop) hold their own buckets.
        from market_data.ratelimit import get_limiter

        get_limiter("kite", args.rate).lower_rate_to(args.rate)
    cap = build(db_path=args.db)
    res = run_backfill(cap, workers=args.workers, max_contracts=args.max_contracts,
                       resume=not args.no_resume, calendar_days=args.calendar_days,
                       underlyings=(args.underlyings.split(",") if args.underlyings else None),
                       no_floor=args.all_contracts)
    print(res.line())
    print("floor:", res.floor)
    return 0 if res.errors == 0 else 1


def cmd_backfill_daily(args) -> int:
    """The front contract's own daily history, one request per in-scope future."""
    from .backfill import run_daily_backfill

    if args.rate:
        from market_data.ratelimit import get_limiter

        get_limiter("kite", args.rate).lower_rate_to(args.rate)
    cap = build(db_path=args.db)
    with cap.store.writer_lock():
        res = run_daily_backfill(
            cap, workers=args.workers, calendar_days=args.calendar_days,
            resume=not args.no_resume, max_contracts=args.max_contracts,
            underlyings=(args.underlyings.split(",") if args.underlyings else None))
    print(res.line())
    print("scope:", res.floor)
    return 0 if res.errors == 0 else 1


def cmd_seed(args) -> int:
    """Rebuild a past session's marks from candles (clearly labelled as such)."""
    from .seed_from_candles import seed_session

    cap = build(db_path=args.db)
    cap.ensure_scope()
    session = date.fromisoformat(args.date)
    out = seed_session(cap.store, cap.provider, session=session,
                       underlyings=(args.underlyings.split(",") if args.underlyings else None))
    print(json.dumps(out, indent=2, default=str))
    return 0


def cmd_oi_series(args) -> int:
    """The per-strike delta-OI series the chart grid draws."""
    from .read_api import strike_oi_series

    store = open_readonly(args.db)
    out = strike_oi_series(store, args.underlying, expiry=args.expiry,
                           session=args.date, width=args.width, as_of=args.as_of)
    if args.compact:
        for c in out["contracts"]:
            pts = [p["delta_oi"] for p in c["points"]]
            print(f"{c['tradingsymbol']:26} {c['atm_offset']:+d} {c['direction']:12} "
                  f"{len(c['points'])} marks  last dOI={pts[-1] if pts else None}")
        print("as_of", out["as_of"], "spot", out["spot"], "atm", out["atm_strike"])
    else:
        print(json.dumps(out, indent=2, default=str))
    return 0


def cmd_prune(args) -> int:
    """Retention by hand.  The capture loop runs the same pass once a day."""
    store = DerivativesStore(args.db)
    out = store.prune(raw_days=args.raw_days, metric_days=args.metric_days,
                      candle_days=args.candle_days, dry_run=args.dry_run)
    print(json.dumps(out, indent=2))
    return 0


def cmd_rollup(args) -> int:
    store = DerivativesStore(args.db)
    day = args.date or (now_ist().date()).isoformat()
    n = store.rollup_day(day)
    print(f"daily_rollups touched for {day}: {n}")
    return 0


# ── parser ───────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("market_data.derivatives",
                                description="F&O 15-minute capture (spec §1/§2)")
    p.add_argument("--db", default=str(config.DEFAULT_DB_PATH))
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--log-file", default=None)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("status", help="what is in the store and what ran today")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("plan", help="scope and the measured cost of one cycle")
    s.add_argument("--refresh", action="store_true", help="re-fetch the NFO dump")
    s.add_argument("--expiries", type=int, default=0, help="print N expiry ladders")
    s.set_defaults(func=cmd_plan)

    s = sub.add_parser("once", help="capture one mark")
    s.add_argument("--mark", help="HH:MM (default: the newest mark that is due)")
    s.add_argument("--force", action="store_true", help="re-capture a mark already stored")
    s.set_defaults(func=cmd_once)

    s = sub.add_parser("run", help="the capture loop (retention runs daily inside it)")
    s.add_argument("--days", type=int, default=None, help="stop after N days")
    s.add_argument("--until", help="HH:MM today")
    s.add_argument("--no-prune", action="store_true",
                   help="do not run the daily retention pass inside the loop")
    s.set_defaults(func=cmd_run)

    s = sub.add_parser("backfill", help="10 sessions of 15-minute candles with OI")
    s.add_argument("--workers", type=int, default=4)
    s.add_argument("--max-contracts", type=int, default=None)
    s.add_argument("--calendar-days", type=int, default=config.BACKFILL_CALENDAR_DAYS)
    s.add_argument("--no-resume", action="store_true")
    s.add_argument("--rate", type=float, default=None,
                   help="cap this process at N Kite requests/second")
    s.add_argument("--underlyings", help="comma separated, e.g. NIFTY,BANKNIFTY")
    s.add_argument("--all-contracts", action="store_true",
                   help="ignore the liquidity floor (complete chain for --underlyings)")
    s.set_defaults(func=cmd_backfill)

    s = sub.add_parser("backfill-daily",
                       help="the in-scope futures' own DAILY candles with OI")
    s.add_argument("--workers", type=int, default=4)
    s.add_argument("--calendar-days", type=int, default=config.DAILY_CALENDAR_DAYS,
                   help="how far back to ask; a future's whole life is ~3 months")
    s.add_argument("--max-contracts", type=int, default=None)
    s.add_argument("--no-resume", action="store_true")
    s.add_argument("--rate", type=float, default=None,
                   help="cap this process at N Kite requests/second")
    s.add_argument("--underlyings", help="comma separated, e.g. NIFTY,BANKNIFTY")
    s.set_defaults(func=cmd_backfill_daily)

    s = sub.add_parser("seed", help="rebuild a past session's marks from candles_15m")
    s.add_argument("--date", required=True, help="YYYY-MM-DD, a session that has closed")
    s.add_argument("--underlyings", help="comma separated")
    s.set_defaults(func=cmd_seed)

    s = sub.add_parser("oi-series", help="per-strike delta-OI since previous close")
    s.add_argument("--underlying", required=True)
    s.add_argument("--expiry", help="YYYY-MM-DD (default: the front in-scope expiry)")
    s.add_argument("--date", help="session YYYY-MM-DD (default: the newest in the store)")
    s.add_argument("--as-of", help="mark 'YYYY-MM-DD HH:MM:SS' (default: newest trading mark)")
    s.add_argument("--width", type=int, default=4)
    s.add_argument("--compact", action="store_true")
    s.set_defaults(func=cmd_oi_series)

    s = sub.add_parser(
        "prune",
        help=(f"retention: snapshots {config.RAW_SNAPSHOT_DAYS}d, metrics "
              f"{config.METRICS_DAYS}d, candles {config.CANDLE_DAYS}d "
              "(the capture loop runs this daily on its own)"))
    s.add_argument("--raw-days", type=int, default=config.RAW_SNAPSHOT_DAYS)
    s.add_argument("--metric-days", type=int, default=config.METRICS_DAYS)
    s.add_argument("--candle-days", type=int, default=config.CANDLE_DAYS,
                   help="candles_15m retention; this table used to be unbounded")
    s.add_argument("--dry-run", action="store_true",
                   help="count and log what would go; delete nothing")
    s.set_defaults(func=cmd_prune)

    s = sub.add_parser("rollup", help="build the keep-for-good daily rows")
    s.add_argument("--date", help="YYYY-MM-DD (default today)")
    s.set_defaults(func=cmd_rollup)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    _log(args.log_level, args.log_file)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
