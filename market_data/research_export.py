"""Export a frozen ``db/market15.db`` snapshot as pattern-research frozen histories.

What this does
--------------
``market_scanner/pattern_research/runner.py`` consumes *frozen per-symbol
histories*: one gzipped ``{timeframe: [bars, quality]}`` blob per symbol under
``market_scanner/output/history/<source_run>/``, plus a ``runs`` row and a
``stocks`` row per symbol in ``market_scanner/output/backtests.sqlite3``.  Those
were produced from ``db/kanida.db`` by ``market_scanner/backtest_worker.py``.

This module produces exactly the same artefacts from the **clean** 15-minute
store instead, through the scanner's own helpers so the hashes and structure
match byte for byte:

* candles      -- ``MarketStore.read_window(snapshot_id=...)``: exactly the rows
                  the frozen snapshot pinned, latest revision, nothing else;
* 1H / 4H      -- ``market_data.aggregate`` via ``data.aggregate_market15``,
                  driven by the symbol's own ``SymbolCalendar`` so the CAS
                  regime (contract 2A) sets the session end per symbol per date;
* 1D / 1W      -- the **provider's own daily bars**, never the intraday close
                  (contract 2A: a CAS stock's official close is an auction price
                  that does not exist in the intraday series, and the pre-CAS
                  close was a 30-minute VWAP);
* exclusions   -- rows the repair labelled ``vendor_zero_print``,
                  ``vendor_bad_print``, ``wrong_instrument`` or ``unresolved``
                  are dropped *before* aggregation and counted per symbol.  A
                  labelled bar never silently becomes a candle.

Nothing is written to ``db/market15.db`` or ``db/kanida.db``; both are opened
read-only.  Nothing is interpolated: a symbol or timeframe we cannot build is
reported with a status, never filled in.

Provider daily history
----------------------
``daily_bars`` only holds what the live ingest backfilled (2021-07-05 onward at
the time of writing), while the repair pass archived the vendor's **full** daily
series (2013-01-01 onward) as sha256-verified payloads in ``raw_archive``.  The
daily series used here is ``daily_bars`` where it has the session and the
verified archive before that; the two are compared on their overlap and any
disagreement is reported per symbol (the stored table wins) rather than merged
silently.

Reused instrument tokens
------------------------
The repair classified 10 symbols as ``wrong_instrument``: the Kite token had an
earlier life under a different instrument.  Its record is a per-symbol
``vendor_first_daily_session``.  That alone is not always enough -- STARHEALTH
and DELHIVERY carry the reused token's block in the vendor's *daily* series too
-- so the usable start of such a symbol is

    max(vendor_first_daily_session, first session after the last gap of
        >= ``TOKEN_GAP_DAYS`` calendar days in its own session history)

and every 15-minute **and** daily bar before it is excluded under the same
``wrong_instrument`` label.  ``TOKEN_GAP_DAYS`` is declared here, before any
return was looked at, and is applied only to the symbols the repair had already
classified on independent evidence.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sqlite3
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: Labels whose rows must never become a candle (contract section 3 / repair report).
EXCLUDED_LABELS = ('vendor_zero_print', 'vendor_bad_print', 'wrong_instrument', 'unresolved')

#: A gap this long in a symbol's own session history separates two lives of a
#: reused instrument token.  Only applied to symbols the repair already
#: classified ``wrong_instrument``.  FORCEMOT's genuine 2023-10-26..2024-02-13
#: suspension is 110 days, well inside this.
TOKEN_GAP_DAYS = 180

#: Session dates are derived from these always-traded names, exactly as
#: ``SessionCalendar.from_kanida_db`` does.
REFERENCE_SYMBOLS = ('RELIANCE', 'INFY', 'TCS', 'HDFCBANK', 'ICICIBANK')

VERDICTS = ROOT / 'market_data' / 'repair' / 'artifacts' / 'reconcile' / 'verdicts.jsonl'
RECONCILE_SUMMARY = ROOT / 'market_data' / 'repair' / 'artifacts' / 'reconcile' / 'summary.json'
MARKET15 = ROOT / 'db' / 'market15.db'


def stamp():
    return datetime.now().isoformat(sep=' ', timespec='seconds')


def dumps(value):
    return json.dumps(value, allow_nan=False, separators=(',', ':'), sort_keys=True)


# --------------------------------------------------------------------- labels
def read_only(path=MARKET15):
    con = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=120)
    con.execute('PRAGMA query_only=ON')
    con.row_factory = sqlite3.Row
    return con


def labelled_bars(path=VERDICTS):
    """{symbol: {bar_start: label}} for every per-bar exclusion label."""
    out: dict[str, dict[str, str]] = {}
    counts: dict[str, int] = {}
    with open(path, encoding='utf-8') as handle:
        for line in handle:
            row = json.loads(line)
            if row['klass'] not in EXCLUDED_LABELS:
                continue
            out.setdefault(row['symbol'], {})[row['bar_start']] = row['klass']
            counts[row['klass']] = counts.get(row['klass'], 0) + 1
    return out, counts


def session_dates(con, symbol):
    """Every date the symbol has a bar for, intraday or provider-daily."""
    days = {r[0] for r in con.execute(
        'SELECT DISTINCT substr(bar_start,1,10) FROM candles_15m WHERE symbol=?', (symbol,))}
    days |= {r[0] for r in con.execute(
        'SELECT DISTINCT session_date FROM daily_bars WHERE symbol=?', (symbol,))}
    return sorted(date.fromisoformat(d) for d in days)


def token_usable_start(con, symbol, vendor_first_daily, gap_days=TOKEN_GAP_DAYS):
    """Where a reused token's *current* instrument life begins."""
    days = session_dates(con, symbol)
    start = date.fromisoformat(vendor_first_daily) if vendor_first_daily else (days[0] if days else None)
    for older, newer in zip(days, days[1:]):
        if (newer - older).days >= gap_days and newer > (start or newer):
            start = newer
    return start


def wrong_instrument_starts(con, path=RECONCILE_SUMMARY):
    """{symbol: usable_start_date} for the repair's reused-token symbols."""
    detail = (json.loads(Path(path).read_text(encoding='utf-8')).get('wrong_instrument') or {}).get('detail') or []
    out = {}
    for entry in detail:
        out[entry['symbol']] = dict(
            usable_start=token_usable_start(con, entry['symbol'], entry.get('vendor_first_daily_session')),
            vendor_first_daily_session=entry.get('vendor_first_daily_session'),
            sessions=entry.get('sessions'), held_bars=entry.get('held_bars'))
    return out


# ------------------------------------------------------------------- calendar
def reference_daily_days(con, symbols=REFERENCE_SYMBOLS):
    """Session dates the reference universe traded, from the provider daily series."""
    days = set()
    for symbol in symbols:
        days |= {r[0] for r in con.execute(
            'SELECT DISTINCT session_date FROM daily_bars WHERE symbol=?', (symbol,))}
        for row in con.execute("SELECT payload_path, sha256 FROM raw_archive "
                               "WHERE timeframe='day' AND symbol=?", (symbol,)):
            for bar in verified_payload(row['payload_path'], row['sha256']):
                days.add(bar['bar_start'][:10])
    return sorted(date.fromisoformat(d) for d in days)


def research_calendar(con, through):
    """The live calendar, back-extended over the provider-daily era.

    ``db/market15_calendar.json`` starts where ``db/kanida.db``'s intraday
    history starts (2015-02-02).  The provider's daily series reaches back to
    2013-01-01, so 1D/1W would otherwise be dropped -- or worse, flagged as one
    long gap -- over two years for which we hold real bars.  A date is a session
    iff the reference universe traded it: the same rule
    ``SessionCalendar.from_bar_times`` already uses, fed from the daily series.
    """
    from market_data.calendar import SessionCalendar, regular_session
    from market_data.live.calendar_ext import live_calendar

    base = live_calendar(through=through)
    sessions = list(base.sessions())
    known = {s.day for s in sessions}
    added = 0
    for day in reference_daily_days(con):
        if day in known:
            continue
        sessions.append(regular_session(day))
        added += 1
    return SessionCalendar(sessions), added


def symbol_calendar(calendar, symbol, cas_from):
    """The calendar as this symbol experiences it: CAS ends its session at 15:15."""
    from market_data.calendar import RegimeBook, SessionRegime, REGIME_CAS
    book = RegimeBook([SessionRegime(symbol, cas_from, REGIME_CAS, 'stored')]) if cas_from else RegimeBook()
    return calendar.for_symbol(symbol, book)


# ---------------------------------------------------------------- daily bars
def verified_payload(path, sha256):
    raw = Path(path).read_bytes()
    decompressed = gzip.decompress(raw)
    if hashlib.sha256(decompressed).hexdigest() != sha256:
        raise ValueError('raw_archive payload checksum mismatch: ' + str(path))
    return json.loads(decompressed).get('rows') or []


def provider_daily(con, symbol, calendar=None):
    """Merged provider daily bars + how the two sources compared.

    ``daily_bars`` wins wherever it has the session; the sha256-verified archive
    supplies the sessions it does not reach.  Disagreements on the overlap are
    counted and reported, never averaged away.
    """
    from market_data.aggregate import Bar

    stored = {}
    for r in con.execute(
            'SELECT session_date,bar_start,bar_end,open,high,low,close,volume,quality_flags '
            'FROM daily_bars d WHERE symbol=? AND revision=(SELECT MAX(d2.revision) FROM '
            'daily_bars d2 WHERE d2.instrument_id=d.instrument_id AND d2.session_date=d.session_date)',
            (symbol,)):
        stored[r['session_date']] = (r['bar_start'], r['bar_end'], float(r['open']), float(r['high']),
                                     float(r['low']), float(r['close']), int(r['volume'] or 0),
                                     r['quality_flags'] or '')
    archived, payloads = {}, 0
    for row in con.execute("SELECT payload_path, sha256 FROM raw_archive WHERE timeframe='day' "
                           "AND symbol=? ORDER BY fetched_at, start", (symbol,)):
        payloads += 1
        for bar in verified_payload(row['payload_path'], row['sha256']):
            day = bar['bar_start'][:10]
            archived[day] = (_naive(bar['bar_start']), _naive(bar['bar_end']), float(bar['open']),
                             float(bar['high']), float(bar['low']), float(bar['close']),
                             int(bar.get('volume') or 0), '')
    overlap = set(stored) & set(archived)
    disagree = sum(1 for d in overlap
                   if tuple(round(v, 6) if isinstance(v, float) else v for v in stored[d][2:7])
                   != tuple(round(v, 6) if isinstance(v, float) else v for v in archived[d][2:7]))
    merged = dict(archived)
    merged.update(stored)
    bars, restamped = [], 0
    for day in sorted(merged):
        start, end, o, h, l, c, v, flags = merged[day]
        start, end = _naive(start), _naive(end)
        # The archived payloads stamp every daily bar on the regular 09:15-15:30
        # window, including Muhurat evenings, while the stored `daily_bars` rows
        # carry the real window. Label the bar with the session the calendar
        # observed, so the 1D series and its gap flags agree with the calendar
        # that made them. Only the label moves; no price or volume is touched.
        session = calendar.session(day_of(day)) if calendar else None
        if session and (session.start, session.end) != (start, end):
            start, end = session.start, session.end
            restamped += 1
        bars.append(Bar(start, end, o, h, l, c, v, True, flags))
    return bars, dict(daily_session_restamped=restamped,
                      daily_rows_stored=len(stored), daily_rows_archived=len(archived),
                      daily_archive_payloads=payloads, daily_overlap=len(overlap),
                      daily_overlap_disagreements=disagree, daily_rows_used=len(bars),
                      daily_from_archive_only=len(set(archived) - set(stored)))


IST = timezone(timedelta(hours=5, minutes=30), 'IST')


def day_of(value):
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def _naive(value):
    """Naive IST, exactly as the store and every legacy table hold it."""
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return parsed.astimezone(IST).replace(tzinfo=None) if parsed.tzinfo else parsed


# ---------------------------------------------------------------------- build
def build_symbol(job, symbol):
    """One symbol's frozen history, exactly as ``backtest_worker`` would store it."""
    from market_scanner.backtest_store import save_history
    from market_scanner.data import aggregate_market15
    from market_data.store import MarketStore

    started = time.monotonic()
    cutoff = datetime.fromisoformat(job['cutoff'])
    calendar = _calendar(job)
    excluded_bars = job['excluded_bars'].get(symbol, {})
    token = job['token_starts'].get(symbol)
    usable_start = date.fromisoformat(token['usable_start']) if token and token.get('usable_start') else None

    store = MarketStore(job['market15'], read_only=True)
    try:
        rows = store.read_bars(symbol, snapshot_id=job['snapshot_id'])
        daily, daily_stats = provider_daily(store.con, symbol, calendar)
    finally:
        store.close()

    dropped = {label: 0 for label in EXCLUDED_LABELS}
    kept = []
    for bar in rows:
        label = excluded_bars.get(bar.bar_start.isoformat(sep=' '))
        if label:
            dropped[label] += 1
            continue
        if usable_start and bar.bar_start.date() < usable_start:
            dropped['wrong_instrument'] += 1
            continue
        kept.append(bar)
    outside = sum(1 for b in kept if calendar.session(b.bar_start.date()) is None
                  or not calendar.session(b.bar_start.date()).contains(b.bar_start))
    daily_kept = []
    for bar in daily:
        if usable_start and bar.bar_start.date() < usable_start:
            dropped['wrong_instrument'] += 1
            continue
        daily_kept.append(bar)

    cas_from = job['cas'].get(symbol)
    if isinstance(cas_from, str):
        cas_from = date.fromisoformat(cas_from)
    view = symbol_calendar(calendar, symbol, cas_from)
    frames, info = {}, {}
    limit = 10 ** 9
    for tf in job['timeframes']:
        source = kept if tf in ('1H', '4H') else daily_kept
        bars, quality = aggregate_market15(source, tf, view, None, cutoff, limit)
        frames[tf] = [bars, quality]
        info[tf] = dict(bars=len(bars), quality=quality,
                        first=bars[0]['time'] if bars else None,
                        last=bars[-1]['end'] if bars else None)
    digest = save_history(job['source_run'], symbol, frames)
    return dict(symbol=symbol, status='complete', history_sha256=digest, timeframes=info,
                seconds=round(time.monotonic() - started, 2),
                source_rows=dict(intraday_snapshot=len(rows), intraday_used=len(kept),
                                 intraday_outside_calendar_session=outside, **daily_stats),
                excluded=dropped, excluded_total=sum(dropped.values()),
                wrong_instrument_usable_start=(usable_start.isoformat() if usable_start else None),
                cas_from=(cas_from.isoformat() if isinstance(cas_from, date) else cas_from),
                first_bar=min((v['first'] for v in info.values() if v['first']), default=None),
                last_bar=max((v['last'] for v in info.values() if v['last']), default=None))


_CALENDARS = {}


def _calendar(job):
    """Per-process cache of the exported calendar (one JSON read per worker)."""
    path = job['calendar_path']
    if path not in _CALENDARS:
        from market_data.calendar import SessionCalendar
        _CALENDARS[path] = SessionCalendar.load(path)
    return _CALENDARS[path]


# --------------------------------------------------------------------- verify
def load_frames(source_run, symbol):
    from market_scanner.backtest_store import history_path
    return json.loads(gzip.decompress(history_path(source_run, symbol).read_bytes()))


def verify(args):
    """Two facts, checked against the export that was actually written.

    1. **Independent aggregation.** For the probe symbols, re-read the snapshot
       rows, re-apply the exclusions and call ``market_data.aggregate.aggregate``
       directly -- not through the scanner adapter that produced the export.
       Every exported 1H/4H bar must match bar for bar, field for field.
    2. **Session containment.** Every exported bar of every symbol must lie
       inside a session the calendar knows, under that symbol's own regime: a
       1H/4H bar within its session window (15:15 for a CAS stock from
       2026-08-03), a 1D bar exactly on the session bounds.
    """
    from market_data.aggregate import aggregate as md_aggregate
    from market_data.calendar import CAS_CLOSE, SessionCalendar
    from market_data.store import MarketStore

    root = Path(args.output_root) / 'output' / 'history'
    manifest = json.loads((root / (args.source_run + '_manifest.json')).read_text(encoding='utf-8'))
    calendar = SessionCalendar.load(manifest['calendar']['path'])
    cutoff = datetime.fromisoformat(manifest['cutoff'])
    excluded_bars, _ = labelled_bars()
    report = dict(source_run=args.source_run, snapshot_id=manifest['snapshot_id'],
                  checked_at=stamp(), aggregation_checked=[], aggregation_mismatches=[],
                  symbols_checked=0, bars_checked=0, outside_session=[],
                  unknown_session=[], regime_violations=[])

    def view_for(symbol):
        cas_from = manifest['per_symbol'][symbol].get('cas_from')
        return symbol_calendar(calendar, symbol, date.fromisoformat(cas_from) if cas_from else None)

    fields = ('time', 'end', 'open', 'high', 'low', 'close', 'volume')
    for symbol in (args.verify_symbols or sorted(manifest['per_symbol'])[:3]):
        store = MarketStore(args.market15, read_only=True)
        try:
            rows = store.read_bars(symbol, snapshot_id=manifest['snapshot_id'])
        finally:
            store.close()
        token = (manifest['wrong_instrument_rule']['symbols'] or {}).get(symbol) or {}
        usable = date.fromisoformat(token['usable_start']) if token.get('usable_start') else None
        drop = excluded_bars.get(symbol, {})
        kept = [b for b in rows if not drop.get(b.bar_start.isoformat(sep=' '))
                and not (usable and b.bar_start.date() < usable)]
        view = view_for(symbol)
        frames = load_frames(args.source_run, symbol)
        for tf in ('1H', '4H'):
            expected = [b.as_dict() for b in md_aggregate(kept, tf, view, as_of=cutoff)
                        if b.candle_complete and b.bar_end <= cutoff]
            actual = frames[tf][0]
            report['aggregation_checked'].append(
                dict(symbol=symbol, timeframe=tf, independent=len(expected), exported=len(actual)))
            differing = next((i for i, (e, a) in enumerate(zip(expected, actual))
                              if tuple(e[f] for f in fields) != tuple(a[f] for f in fields)), None)
            if len(expected) != len(actual) or differing is not None:
                report['aggregation_mismatches'].append(
                    dict(symbol=symbol, timeframe=tf, independent=len(expected), exported=len(actual),
                         first_differing_index=differing,
                         independent_bar=expected[differing] if differing is not None else None,
                         exported_bar=actual[differing] if differing is not None else None))

    for symbol in sorted(manifest['per_symbol']):
        view = view_for(symbol)
        cas_from = manifest['per_symbol'][symbol].get('cas_from')
        cas_from = date.fromisoformat(cas_from) if cas_from else None
        frames = load_frames(args.source_run, symbol)
        report['symbols_checked'] += 1
        for tf, (bars, _quality) in frames.items():
            for bar in bars:
                report['bars_checked'] += 1
                start = datetime.fromisoformat(bar['time'])
                end = datetime.fromisoformat(bar['end'])
                session = view.session(start.date()) if tf in ('1H', '4H') else calendar.session(start.date())
                if session is None:
                    report['unknown_session'].append(dict(symbol=symbol, timeframe=tf, bar=bar['time']))
                    continue
                if tf in ('1H', '4H'):
                    if start < session.start or end > session.end:
                        report['outside_session'].append(
                            dict(symbol=symbol, timeframe=tf, bar=bar['time'], bar_end=bar['end'],
                                 session=[session.start.isoformat(sep=' '), session.end.isoformat(sep=' ')]))
                    if cas_from and start.date() >= cas_from and end.time() > CAS_CLOSE:
                        report['regime_violations'].append(
                            dict(symbol=symbol, timeframe=tf, bar=bar['time'], bar_end=bar['end']))
                elif tf == '1D' and (start, end) != (session.start, session.end):
                    report['outside_session'].append(
                        dict(symbol=symbol, timeframe=tf, bar=bar['time'], bar_end=bar['end'],
                             session=[session.start.isoformat(sep=' '), session.end.isoformat(sep=' ')]))

    report['passed'] = not (report['aggregation_mismatches'] or report['outside_session']
                            or report['unknown_session'] or report['regime_violations'])
    for field in ('outside_session', 'unknown_session', 'regime_violations'):
        report[field + '_count'] = len(report[field])
        report[field] = report[field][:10]
    path = root / (args.source_run + '_verification.json')
    path.write_text(json.dumps(report, indent=1, sort_keys=True), encoding='utf-8')
    print(dumps(dict(verification=str(path), passed=report['passed'],
                     symbols=report['symbols_checked'], bars=report['bars_checked'],
                     aggregation_mismatches=len(report['aggregation_mismatches']),
                     outside_session=report['outside_session_count'],
                     unknown_session=report['unknown_session_count'],
                     regime_violations=report['regime_violations_count'])), flush=True)
    return report


# ----------------------------------------------------------------------- main
def universe(con, snapshot_id):
    members = [r[0] for r in con.execute(
        'SELECT DISTINCT symbol FROM snapshot_members WHERE snapshot_id=? ORDER BY symbol',
        (snapshot_id,))]
    quarantined = {r[0]: dict(reason=r[1], detail=r[2]) for r in con.execute(
        "SELECT symbol, reason, detail FROM quarantine WHERE status='quarantined'")}
    return [s for s in members if s not in quarantined], quarantined


def cutoff_for(con, snapshot_id, now=None):
    """End of the last session the snapshot covers that has actually closed."""
    now = now or datetime.now()
    last_member = con.execute('SELECT MAX(bar_start) FROM snapshot_members WHERE snapshot_id=?',
                              (snapshot_id,)).fetchone()[0]
    closed = con.execute(
        'SELECT MAX(bar_end) FROM daily_bars WHERE symbol IN (%s) AND bar_end<=?'
        % ','.join('?' * len(REFERENCE_SYMBOLS)),
        (*REFERENCE_SYMBOLS, now.isoformat(sep=' '))).fetchone()[0]
    candidates = [datetime.fromisoformat(c) for c in (closed,) if c]
    if last_member:
        day = datetime.fromisoformat(last_member).date()
        candidates.append(datetime.combine(day, datetime.min.time()) + timedelta(hours=15, minutes=30))
    return min(candidates)


def export(args):
    con = read_only(args.market15)
    snapshot = con.execute('SELECT * FROM snapshots WHERE snapshot_id=?', (args.snapshot,)).fetchone()
    if snapshot is None:
        raise SystemExit('unknown snapshot ' + args.snapshot)
    if snapshot['status'] != 'frozen':
        raise SystemExit('snapshot %s is %s, not frozen' % (args.snapshot, snapshot['status']))
    symbols, quarantined = universe(con, args.snapshot)
    if args.symbols:
        symbols = [s for s in symbols if s in set(args.symbols)]
    excluded_bars, label_counts = labelled_bars()
    token_starts = wrong_instrument_starts(con)
    cutoff = cutoff_for(con, args.snapshot)
    calendar, back_extended = research_calendar(con, cutoff.date() + timedelta(days=30))
    calendar_path = Path(args.output_root) / 'output' / 'history' / (args.source_run + '_calendar.json')
    calendar_path.parent.mkdir(parents=True, exist_ok=True)
    calendar.save(calendar_path)
    cas = {r[0]: r[1] for r in con.execute(
        "SELECT symbol, MIN(effective_from) FROM session_regimes WHERE regime='cas' GROUP BY symbol")}
    store_exclusions = [dict(r) for r in con.execute(
        'SELECT label, rows, symbols, source, note FROM snapshot_exclusions WHERE snapshot_id=?',
        (args.snapshot,))]
    con.close()

    job = dict(snapshot_id=args.snapshot, source_run=args.source_run, market15=str(args.market15),
               cutoff=cutoff.isoformat(sep=' '), timeframes=['1H', '4H', '1D', '1W'],
               excluded_bars=excluded_bars,
               token_starts={s: dict(v, usable_start=v['usable_start'].isoformat() if v['usable_start'] else None)
                             for s, v in token_starts.items()},
               cas=cas, calendar_path=str(calendar_path))

    results, errors = {}, {}
    started = time.monotonic()
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        pending = {pool.submit(build_symbol, job, s): s for s in symbols}
        for done, future in enumerate(as_completed(pending), 1):
            symbol = pending[future]
            try:
                results[symbol] = future.result()
            except Exception as exc:
                errors[symbol] = dict(error=str(exc), traceback=traceback.format_exc())
                print(dumps(dict(symbol=symbol, error=str(exc))), flush=True)
            if done % 25 == 0 or done == len(pending):
                print(dumps(dict(done=done, total=len(pending), errors=len(errors),
                                 seconds=round(time.monotonic() - started, 1))), flush=True)

    manifest = dict(
        source_run=args.source_run, snapshot_id=args.snapshot,
        snapshot_checksum=snapshot['checksum'], snapshot_created_at=snapshot['created_at'],
        snapshot_universe=snapshot['universe'], snapshot_first_bar=snapshot['first_bar'],
        snapshot_last_bar=snapshot['last_bar'], snapshot_row_count=snapshot['row_count'],
        snapshot_symbol_count=snapshot['symbol_count'],
        adjustment_basis_id=snapshot['adjustment_basis_id'], provider=snapshot['provider'],
        exported_at=stamp(), cutoff=job['cutoff'], timeframes=job['timeframes'],
        symbols=len(results), errors=errors,
        quarantined_symbols=quarantined, snapshot_exclusions_in_store=store_exclusions,
        excluded_labels=list(EXCLUDED_LABELS),
        excluded_label_rows_available=label_counts,
        excluded_rows_applied={label: sum(r['excluded'][label] for r in results.values())
                               for label in EXCLUDED_LABELS},
        wrong_instrument_rule=dict(gap_days=TOKEN_GAP_DAYS, symbols=job['token_starts']),
        aggregation=dict(
            intraday='1H/4H from candles_15m via market_data.aggregate, bucketed by the '
                     "symbol's own SessionCalendar (CAS regime, contract 2A)",
            daily='1D from the provider daily series (daily_bars, extended before its first '
                  'session by the sha256-verified raw_archive day payloads); 1W = Mon-Fri '
                  'weeks built from that 1D series (market_data.aggregate.weekly_from_daily)',
            four_hour_buckets='09:15-13:15 and 13:15-session end',
            completeness='incomplete buckets are dropped, never padded'),
        calendar=dict(path=str(calendar_path), sessions=len(calendar.sessions()),
                      first=str(calendar.first_day), last=str(calendar.last_day),
                      back_extended_sessions=back_extended,
                      back_extension_rule='a date the reference universe (%s) has a provider '
                                          'daily bar for is a session' % ','.join(REFERENCE_SYMBOLS)),
        per_symbol={s: {k: v for k, v in r.items() if k != 'timeframes'} for s, r in results.items()},
        bars={tf: sum(r['timeframes'][tf]['bars'] for r in results.values()) for tf in job['timeframes']},
        seconds=round(time.monotonic() - started, 1))
    path = Path(args.output_root) / 'output' / 'history' / (args.source_run + '_manifest.json')
    path.write_text(json.dumps(manifest, indent=1, sort_keys=True), encoding='utf-8')
    print(dumps(dict(manifest=str(path), symbols=len(results), errors=len(errors),
                     bars=manifest['bars'], excluded=manifest['excluded_rows_applied'],
                     seconds=manifest['seconds'])), flush=True)
    return manifest, results


def register(args, results, manifest):
    """Write the ``runs`` + ``stocks`` rows ``runner.prepare`` reads."""
    from market_scanner.backtest_store import initialize, connection, dumps as bdumps, save_stock
    initialize()
    run = dict(id=args.source_run, status='complete', done=len(results), total=len(results),
               through=manifest['cutoff'], started_at=manifest['exported_at'],
               updated_at=manifest['exported_at'], completed_at=manifest['exported_at'],
               source_latest=manifest['cutoff'], source='db/market15.db snapshot ' + args.snapshot,
               snapshot_id=args.snapshot, snapshot_checksum=manifest['snapshot_checksum'],
               excluded_labels=list(EXCLUDED_LABELS),
               excluded_rows=manifest['excluded_rows_applied'],
               rules='Frozen export of clean market15 snapshot; detectors and rules are the '
                     "research runner's, not this export's",
               scope=manifest['snapshot_universe'] + ' minus quarantined symbols',
               manifest=str(Path(args.output_root) / 'output' / 'history' / (args.source_run + '_manifest.json')),
               workers=args.workers, failed=len(manifest['errors']))
    for symbol, result in results.items():
        save_stock(args.source_run, symbol, dict(result, cells=[]))
    with connection() as con:
        con.execute('INSERT OR REPLACE INTO runs VALUES(?,?)', (run['id'], bdumps(run)))
        previous = con.execute("SELECT value FROM settings WHERE key='active_run'").fetchone()
        if args.activate:
            con.execute("INSERT OR REPLACE INTO settings VALUES('active_run',?)", (run['id'],))
    print(dumps(dict(registered=run['id'], stocks=len(results),
                     previous_active_run=previous[0] if previous else None,
                     active_run_set=bool(args.activate))), flush=True)
    return previous[0] if previous else None


def restore_active_run(run_id):
    from market_scanner.backtest_store import connection
    with connection() as con:
        con.execute("INSERT OR REPLACE INTO settings VALUES('active_run',?)", (run_id,))
    print(dumps(dict(active_run_restored=run_id)), flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', default='')
    parser.add_argument('--source-run', required=True)
    parser.add_argument('--market15', default=str(MARKET15))
    parser.add_argument('--output-root', default=str(ROOT / 'market_scanner'))
    parser.add_argument('--symbols', nargs='+')
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--activate', action='store_true',
                        help='point settings.active_run at the new source run')
    parser.add_argument('--restore-active-run', help='restore settings.active_run and exit')
    parser.add_argument('--verify', action='store_true', help='verify a finished export and exit')
    parser.add_argument('--verify-symbols', nargs='+')
    args = parser.parse_args(argv)
    if args.restore_active_run:
        restore_active_run(args.restore_active_run)
        return 0
    if args.verify:
        return 0 if verify(args)['passed'] else 1
    if not args.snapshot:
        parser.error('--snapshot is required')
    manifest, results = export(args)
    register(args, results, manifest)
    return 1 if manifest['errors'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
