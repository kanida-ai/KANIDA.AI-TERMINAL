"""CLI: fetch / status / expiries / members for the NSE F&O bhavcopy archive.

    python -m market_data.bhavcopy.cli fetch --from 2016-01-01 --to 2026-09-25
    python -m market_data.bhavcopy.cli status [--missing 50]
    python -m market_data.bhavcopy.cli expiries --symbol BANKNIFTY [--from 2019-01-01 --to 2019-12-31]
    python -m market_data.bhavcopy.cli members --date 2019-03-07
"""

from __future__ import annotations

import argparse
import fcntl
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from market_data.bhavcopy import fetch as F
from market_data.bhavcopy import load as L


def weekdays(start: date, end: date, include_weekends: bool = False):
    d = start
    while d <= end:
        if include_weekends or d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def _p(*a) -> None:
    print(*a, flush=True)


def run_fetch(conn, client: F.NSEClient, days, raw_dir: Path,
              skip_missing: bool = False, log=_p) -> dict:
    """Fetch and load each day. Returns counters. Raises F.Blocked to stop."""
    done = {r[0]: r[1] for r in conn.execute("SELECT trade_date, status FROM fetch_log")}
    c = defaultdict(int)
    days = list(days)
    t0 = time.monotonic()
    for n, d in enumerate(days, 1):
        iso = d.isoformat()
        st = done.get(iso)
        if st == "ok" or (skip_missing and st == "holiday_or_missing"):
            c["skipped"] += 1
            continue
        res = F.fetch_day(client, d, raw_dir)
        if res.status == "ok":
            try:
                rows = L.parse_zip(res.content)
                k = L.load_day(conn, iso, rows, res.url, res.http_status,
                               note=f"{res.source_format}{' (raw cache)' if res.from_cache else ''}")
                c["ok"] += 1
                c["rows"] += k
                log(f"[{n}/{len(days)}] {iso} ok {k} rows ({res.source_format})")
            except L.ParseError as e:
                L.record_failure(conn, iso, res.url, "error", res.http_status, f"parse: {e}")
                c["error"] += 1
                log(f"[{n}/{len(days)}] {iso} ERROR parse: {e}")
        else:
            L.record_failure(conn, iso, res.url, res.status, res.http_status, res.note)
            c[res.status] += 1
            log(f"[{n}/{len(days)}] {iso} {res.status}: {res.note}")
        if n % 100 == 0:
            log(f"  progress {n}/{len(days)} in {time.monotonic() - t0:.0f}s {dict(c)}")
    return dict(c)


def cmd_fetch(a) -> int:
    start, end = date.fromisoformat(a.date_from), date.fromisoformat(a.date_to)
    if a.dates:
        days = [date.fromisoformat(x) for x in a.dates.split(",")]
    else:
        days = list(weekdays(start, end, a.include_weekends))
    lock_path = Path(a.raw_dir) / ".fetch.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = open(lock_path, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        _p("another bhavcopy fetch is already running (lock held) - exiting")
        return 3
    conn = L.connect(a.db)
    client = F.NSEClient(min_interval=a.min_interval)
    _p(f"bhavcopy fetch: {len(days)} days {days[0] if days else '-'}..{days[-1] if days else '-'}"
       f" db={a.db} raw={a.raw_dir}")
    try:
        c = run_fetch(conn, client, days, Path(a.raw_dir), a.skip_missing)
    except F.Blocked as e:
        _p(f"STOPPED: NSE is blocking requests: {e}. Re-run later; completed days are kept.")
        return 2
    _p(f"done: {c}")
    return 0


def cmd_status(a) -> int:
    conn = L.connect(a.db)
    total = conn.execute("SELECT COUNT(*) FROM fo_daily").fetchone()[0]
    _p(f"db: {a.db}\nfo_daily rows: {total:,}")
    _p(f"{'year':<6}{'logged':>8}{'ok':>6}{'hol/miss':>10}{'error':>7}{'rows':>12}  first..last ok")
    for y, n, ok, hm, er, rows, f, l in conn.execute("""
        SELECT substr(trade_date,1,4), COUNT(*),
               SUM(status='ok'), SUM(status='holiday_or_missing'), SUM(status='error'),
               SUM(rows),
               MIN(CASE WHEN status='ok' THEN trade_date END),
               MAX(CASE WHEN status='ok' THEN trade_date END)
        FROM fetch_log GROUP BY 1 ORDER BY 1"""):
        _p(f"{y:<6}{n:>8}{ok:>6}{hm:>10}{er:>7}{rows:>12,}  {f}..{l}")
    _p("rows by instrument:")
    for ins, n, fmt in conn.execute(
            "SELECT instrument, COUNT(*), GROUP_CONCAT(DISTINCT source_format) FROM fo_daily GROUP BY 1 ORDER BY 2 DESC"):
        _p(f"  {ins:<8}{n:>12,}  {fmt}")
    miss = conn.execute(
        "SELECT trade_date, status, http_status, note FROM fetch_log WHERE status!='ok' "
        "ORDER BY trade_date").fetchall()
    _p(f"not-ok days: {len(miss)} (showing {'all' if a.missing <= 0 else f'last {a.missing}'})")
    for r in (miss if a.missing <= 0 else miss[-a.missing:]):
        _p(f"  {r[0]} ({date.fromisoformat(r[0]):%a}) {r[1]:<19} http={r[2]} {r[3]}")
    return 0


def cmd_expiries(a) -> int:
    conn = L.connect(a.db)
    rows = L.expiries(conn, a.symbol)
    if a.date_from:
        rows = [r for r in rows if r[0] >= a.date_from]
    if a.date_to:
        rows = [r for r in rows if r[0] <= a.date_to]
    _p(f"{a.symbol}: {len(rows)} option expiries (expiry, instrument, first_listed, last_listed, days)")
    for r in rows:
        _p(f"  {r[0]} ({date.fromisoformat(r[0]):%a}) {r[1]} {r[2]}..{r[3]} {r[4]}d")
    return 0


def cmd_members(a) -> int:
    conn = L.connect(a.db)
    st = L.log_status(conn, a.date)
    if st != "ok":
        _p(f"{a.date}: no loaded bhavcopy (fetch_log status={st}) - membership unknown, not empty")
        return 1
    m = L.members(conn, a.date)
    _p(f"{a.date}: {len(m)} OPTSTK symbols")
    _p(" ".join(m))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="market_data.bhavcopy.cli", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(L.DEFAULT_DB))
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch", help="download + load weekdays in a range (resumable)")
    f.add_argument("--from", dest="date_from", default="2016-01-01")
    f.add_argument("--to", dest="date_to", default=date.today().isoformat())
    f.add_argument("--dates", help="comma-separated explicit dates (overrides the range)")
    f.add_argument("--include-weekends", action="store_true",
                   help="also try Sat/Sun (special sessions such as Budget day or Muhurat)")
    f.add_argument("--skip-missing", action="store_true",
                   help="also skip days already logged holiday_or_missing")
    f.add_argument("--raw-dir", default=str(L.DEFAULT_RAW))
    f.add_argument("--min-interval", type=float, default=1.1, help="seconds between requests (>=1)")
    f.set_defaults(fn=cmd_fetch)
    s = sub.add_parser("status", help="coverage per year, missing days, row counts")
    s.add_argument("--missing", type=int, default=40, help="how many not-ok days to list (0 = all)")
    s.set_defaults(fn=cmd_status)
    e = sub.add_parser("expiries", help="distinct option expiries per underlying")
    e.add_argument("--symbol", required=True)
    e.add_argument("--from", dest="date_from")
    e.add_argument("--to", dest="date_to")
    e.set_defaults(fn=cmd_expiries)
    m = sub.add_parser("members", help="OPTSTK symbols listed on a day")
    m.add_argument("--date", required=True)
    m.set_defaults(fn=cmd_members)
    a = ap.parse_args(argv)
    if getattr(a, "min_interval", 1.0) < 1.0:
        ap.error("--min-interval must be >= 1.0 (NSE politeness)")
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
