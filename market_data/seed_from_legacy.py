"""Seed `db/market15.db` from the existing rows in `db/kanida.db`.

    5-minute -> 15-minute is exact (3 rows per bar) and ~5x cheaper to read
    than 1-minute. 1-minute is used only as a *fallback* for sessions where the
    5-minute table has nothing.

Everything written here is labelled for what it is:

    vendor_id           = 'legacy_kanida_db'
    adjustment_basis_id = 'legacy_unknown'      <- the basis is genuinely unknown
    revision            = 1

`validate.py` runs over every symbol so the *pre-existing* defects (PIIND 2019
et al.) are flagged in `quality_flags` / `quality_findings` rather than hidden.
No row is dropped and no row is invented.

`db/kanida.db` is opened `mode=ro` + `query_only=ON`. It is never written.

Usage
-----
    python -m market_data.seed_from_legacy --symbols PIIND,RELIANCE --workers 4
    python -m market_data.seed_from_legacy --all --workers 8
    python -m market_data.seed_from_legacy --all --resume
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import zlib
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data.aggregate import Bar, to_15m  # noqa: E402
from market_data.calendar import RegimeBook, SessionCalendar  # noqa: E402
from market_data.store import MarketStore, WriterLock  # noqa: E402
from market_data import validate as V  # noqa: E402

LEGACY_DB = ROOT / "db" / "kanida.db"
TARGET_DB = ROOT / "db" / "market15.db"
CALENDAR_CACHE = ROOT / "db" / "market15_calendar.json"

VENDOR_ID = "legacy_kanida_db"
ADJUSTMENT_BASIS_ID = "legacy_unknown"
SEED_REVISION = 1
INTRADAY_START = date(2015, 2, 2)   # first bar in ohlc_5min

_CAL: SessionCalendar | None = None


# ---------------------------------------------------------------------------
# read-only access to the legacy DB
# ---------------------------------------------------------------------------
def legacy_connect(path=LEGACY_DB) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{Path(path).as_posix()}?mode=ro", uri=True,
                          timeout=120)
    con.execute("PRAGMA query_only=ON")
    return con


def load_universe(con: sqlite3.Connection, index_column: str = "in_nifty500"
                  ) -> list[tuple[str, int | None, int]]:
    rows = con.execute(
        f"SELECT symbol, kite_token, COALESCE(is_fno,0) FROM instrument_labels "
        f"WHERE {index_column}=1 AND is_active=1 AND exchange='NSE' "
        f"ORDER BY symbol").fetchall()
    return [(r[0], r[1], int(r[2] or 0)) for r in rows]


def instrument_id_for(symbol: str, token) -> int:
    if token:
        return int(token)
    return int(zlib.crc32(symbol.encode()) & 0x7FFFFFFF)


def _fetch(con, table, symbol, start=None, end=None):
    sql = (f"SELECT bar_time, open, high, low, close, volume FROM {table} "
           f"WHERE symbol=?")
    params: list = [symbol]
    if start:
        sql += " AND bar_time>=?"
        params.append(str(start))
    if end:
        sql += " AND bar_time<?"
        params.append(str(end))
    sql += " ORDER BY bar_time"
    return con.execute(sql, params).fetchall()


def _contiguous_ranges(days: list[date]) -> list[tuple[date, date]]:
    out: list[tuple[date, date]] = []
    for d in sorted(days):
        if out and (d - out[-1][1]).days <= 3:
            out[-1] = (out[-1][0], d)
        else:
            out.append((d, d))
    return out


# ---------------------------------------------------------------------------
# per-symbol work (runs in a worker process; reads only)
# ---------------------------------------------------------------------------
def _calendar() -> SessionCalendar:
    global _CAL
    if _CAL is None:
        _CAL = SessionCalendar.load(CALENDAR_CACHE)
    return _CAL


def build_symbol(symbol: str, token, legacy_db=LEGACY_DB,
                 is_fno: int = 0) -> dict:
    """Build validated 15m bars for one symbol. Pure read; returns plain data."""
    t0 = time.time()
    cal = _calendar()
    con = legacy_connect(legacy_db)
    try:
        five = _fetch(con, "ohlc_5min", symbol)
        bars = to_15m(five, 5, cal)
        n_from_5m = len(bars)

        daily_rows = _fetch(con, "ohlc_daily", symbol)
        daily = {}
        for bt, o, h, l, c, v in daily_rows:
            d = datetime.fromisoformat(bt).date()
            daily[d] = {"open": o, "high": h, "low": l, "close": c, "volume": v}

        # 1-minute fallback, only for sessions the 5-minute table misses
        have_days = {b.bar_start.date() for b in bars}
        want_days = sorted(d for d in daily
                           if d >= INTRADAY_START and cal.is_session(d)
                           and d not in have_days)
        n_from_1m = 0
        fallback_days = 0
        if want_days:
            extra = []
            for a, b in _contiguous_ranges(want_days):
                rows = _fetch(con, "ohlc_1min", symbol,
                              f"{a} 00:00:00", f"{b + timedelta(days=1)} 00:00:00")
                if rows:
                    extra.extend(rows)
            if extra:
                got = to_15m(extra, 1, cal)
                got = [g for g in got if g.bar_start.date() not in have_days]
                n_from_1m = len(got)
                fallback_days = len({g.bar_start.date() for g in got})
                bars = sorted(bars + got, key=lambda x: x.bar_start)
    finally:
        con.close()

    if not bars:
        return {"symbol": symbol, "status": "skipped", "rows": 0,
                "reason": "no intraday rows in kanida.db",
                "elapsed": time.time() - t0}

    # Contract 2A: derive this symbol's session regime from its own bars
    # before validating, so a CAS stock's 24-bar day is not mistaken for a
    # short session. `is_fno` is recorded as a cross-check, never as the rule.
    last_by_day: dict = {}
    for b in bars:
        d = b.bar_start.date()
        if d not in last_by_day or b.bar_start > last_by_day[d]:
            last_by_day[d] = b.bar_start
    book = RegimeBook.from_observed_last_bars(symbol, last_by_day, cal,
                                              is_fno=bool(is_fno))
    symbol_cal = cal.for_symbol(symbol, book)

    findings = V.validate_symbol(symbol, bars, symbol_cal, daily)
    bars = V.apply_quality_flags(bars, findings)

    per_day = Counter(b.bar_start.date() for b in bars)
    payload = [(b.bar_start.isoformat(sep=" "), b.bar_end.isoformat(sep=" "),
                b.open, b.high, b.low, b.close, b.volume,
                1 if b.candle_complete else 0, b.quality_flags) for b in bars]
    return {
        "symbol": symbol,
        "instrument_id": instrument_id_for(symbol, token),
        "status": "done",
        "rows": len(payload),
        "payload": payload,
        "findings": [f.to_row() for f in findings],
        "summary": V.summarise(findings),
        "first_bar": payload[0][0],
        "last_bar": payload[-1][0],
        "days": len(per_day),
        "bars_per_day": dict(Counter(per_day.values())),
        "source_mix": f"5min={n_from_5m},1min={n_from_1m}",
        "regimes": book.to_rows(),
        "fallback_days": fallback_days,
        "flagged_rows": sum(1 for p in payload if p[8]),
        "elapsed": time.time() - t0,
    }


def _worker(args) -> dict:
    symbol, token, legacy_db, is_fno = args
    try:
        return build_symbol(symbol, token, legacy_db, is_fno)
    except Exception as exc:  # never let one symbol kill the run
        import traceback
        return {"symbol": symbol, "status": "error", "rows": 0,
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc()[-2000:]}


# ---------------------------------------------------------------------------
# driver (single writer)
# ---------------------------------------------------------------------------
def ensure_calendar(legacy_db=LEGACY_DB, refresh=False) -> SessionCalendar:
    cal = SessionCalendar.from_kanida_db(legacy_db, cache_path=CALENDAR_CACHE,
                                         refresh=refresh)
    return cal


def seed(symbols: list[tuple[str, int | None]], *, target_db=TARGET_DB,
         legacy_db=LEGACY_DB, workers: int = 4, resume: bool = True,
         run_id: str | None = None, store_findings: bool = True,
         log=print) -> dict:
    lock = WriterLock(target_db).acquire()
    store = MarketStore(target_db)
    try:
        run_id = store.start_run(run_id, provider=VENDOR_ID, plan={
            "source": str(legacy_db), "universe_size": len(symbols),
            "workers": workers, "revision": SEED_REVISION,
            "vendor_id": VENDOR_ID, "adjustment_basis_id": ADJUSTMENT_BASIS_ID})
        store.set_meta("seed_source", str(legacy_db))
        store.set_meta("seed_run_id", run_id)

        todo = list(symbols)
        if resume:
            done = store.done_symbols()
            todo = [row for row in todo if row[0] not in done]
            if done:
                log(f"[seed] resume: {len(done)} symbol(s) already done, "
                    f"{len(todo)} to go")

        totals = {"rows": 0, "symbols": 0, "errors": 0, "skipped": 0,
                  "findings": 0, "flagged_rows": 0, "cas_symbols": 0}
        per_symbol: list[dict] = []
        bars_per_day = Counter()
        t0 = time.time()
        jobs = [(s, t, str(legacy_db), f) for s, t, f in todo]

        def handle(res: dict) -> None:
            sym = res["symbol"]
            if res["status"] == "error":
                totals["errors"] += 1
                store.mark_progress(sym, run_id=run_id, status="error",
                                    error=res.get("error"))
                log(f"[seed] ERROR {sym}: {res.get('error')}")
                return
            if res["status"] == "skipped":
                totals["skipped"] += 1
                store.mark_progress(sym, run_id=run_id, status="skipped",
                                    error=res.get("reason"))
                log(f"[seed] skip  {sym}: {res.get('reason')}")
                return
            bars = [Bar(datetime.fromisoformat(p[0]), datetime.fromisoformat(p[1]),
                        p[2], p[3], p[4], p[5], p[6], bool(p[7]), p[8])
                    for p in res["payload"]]
            try:
                written, _rev = store.upsert_candles(
                    sym, res["instrument_id"], bars, vendor_id=VENDOR_ID,
                    adjustment_basis_id=ADJUSTMENT_BASIS_ID,
                    revision=SEED_REVISION, run_id=run_id)
            except Exception as exc:
                # one unwritable symbol must never abort a 500-symbol run
                totals["errors"] += 1
                msg = f"write failed: {type(exc).__name__}: {exc}"
                store.mark_progress(sym, run_id=run_id, status="error", error=msg)
                log(f"[seed] ERROR {sym}: {msg}")
                return
            if res.get("regimes"):
                store.record_regimes(res["regimes"], run_id=run_id)
                totals["cas_symbols"] += 1
            n_find = 0
            if store_findings:
                n_find = store.record_findings(
                    [_FindingRow(r) for r in res["findings"]], run_id=run_id)
            store.mark_progress(sym, run_id=run_id, status="done", rows=written,
                                first_bar=res["first_bar"], last_bar=res["last_bar"],
                                source_mix=res["source_mix"],
                                findings=len(res["findings"]))
            totals["rows"] += written
            totals["symbols"] += 1
            totals["findings"] += len(res["findings"])
            totals["flagged_rows"] += res["flagged_rows"]
            for k, v in res["bars_per_day"].items():
                bars_per_day[int(k)] += v
            res.pop("payload", None)
            per_symbol.append(res)
            done_n = totals["symbols"] + totals["errors"] + totals["skipped"]
            rate = done_n / max(time.time() - t0, 1e-9)
            eta = (len(jobs) - done_n) / rate if rate else 0
            log(f"[seed] {done_n}/{len(jobs)} {sym}: {written} rows, "
                f"{res['days']} days, {res['flagged_rows']} flagged, "
                f"{len(res['findings'])} findings, {res['source_mix']}, "
                f"{res['elapsed']:.1f}s  (ETA {eta / 60:.1f} min)")

        if workers <= 1:
            for job in jobs:
                handle(_worker(job))
        else:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_worker, job): job[0] for job in jobs}
                for fut in as_completed(futures):
                    handle(fut.result())

        elapsed = time.time() - t0
        store.finish_run(run_id, requests=len(jobs), rows=totals["rows"],
                         errors=totals["errors"],
                         status="ok" if not totals["errors"] else "partial")
        return {"run_id": run_id, "elapsed_sec": elapsed, **totals,
                "bars_per_day": dict(sorted(bars_per_day.items())),
                "per_symbol": per_symbol}
    finally:
        store.close()
        lock.release()


class _FindingRow(dict):
    """Adapter so store.record_findings can consume already-serialised rows."""

    def to_row(self):
        return self


# ---------------------------------------------------------------------------
# reporting helpers
# ---------------------------------------------------------------------------
def report(result: dict, top: int = 15) -> str:
    lines = [
        f"run_id            : {result['run_id']}",
        f"symbols seeded    : {result['symbols']} "
        f"(errors {result['errors']}, skipped {result['skipped']})",
        f"rows written      : {result['rows']:,}",
        f"elapsed           : {result['elapsed_sec'] / 60:.2f} min",
        f"flagged rows      : {result['flagged_rows']:,}",
        f"CAS-regime symbols: {result.get('cas_symbols', 0)}",
        f"findings          : {result['findings']:,}",
        "bars/day distribution (bars_in_day: day_count):",
        "  " + ", ".join(f"{k}:{v}" for k, v in result["bars_per_day"].items()),
    ]
    by_sym = sorted(result["per_symbol"],
                    key=lambda r: -r["summary"]["total"])[:top]
    lines.append(f"top {top} symbols by finding count:")
    for r in by_sym:
        lines.append(f"  {r['symbol']:<14} {r['summary']['total']:>6} findings "
                     f"{r['summary']['by_severity']} {r['summary']['by_code']}")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbols", help="comma separated symbols (pilot mode)")
    ap.add_argument("--all", action="store_true", help="whole NIFTY 500 universe")
    ap.add_argument("--index-column", default="in_nifty500")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=max(os.cpu_count() // 2, 1))
    ap.add_argument("--legacy-db", default=str(LEGACY_DB))
    ap.add_argument("--target-db", default=str(TARGET_DB))
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--refresh-calendar", action="store_true")
    ap.add_argument("--json-out", help="write the run summary here")
    args = ap.parse_args(argv)

    cal = ensure_calendar(args.legacy_db, refresh=args.refresh_calendar)
    print(f"[seed] calendar: {json.dumps(cal.summary())[:400]}")

    con = legacy_connect(args.legacy_db)
    try:
        universe = load_universe(con, args.index_column)
    finally:
        con.close()
    by_symbol = {row[0]: row for row in universe}

    if args.symbols:
        wanted = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        symbols = [by_symbol.get(s, (s, None, 0)) for s in wanted]
    elif args.all:
        symbols = universe
    else:
        ap.error("pass --symbols or --all")
    if args.limit:
        symbols = symbols[:args.limit]

    print(f"[seed] {len(symbols)} symbol(s), {args.workers} worker(s) -> "
          f"{args.target_db}")
    result = seed(symbols, target_db=Path(args.target_db),
                  legacy_db=Path(args.legacy_db), workers=args.workers,
                  resume=not args.no_resume)
    print(report(result))
    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps({k: v for k, v in result.items() if k != "per_symbol"} |
                       {"per_symbol": [{k: v for k, v in r.items()
                                        if k != "findings"}
                                       for r in result["per_symbol"]]},
                       indent=2, default=str), encoding="utf-8")
        print(f"[seed] summary -> {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
