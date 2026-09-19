"""`/api/state.data_status` -- where the prices come from and how current they are.

One compact, additive object on the state the app already polls every 60 s, so
the owner can confirm live data is flowing without reading a log file.

Everything here is **read**: the values come from what the live ingest loop
already records in `db/market15.db` (the `meta` keys
``live.latest_bar_start`` / ``live.latest_bar_end`` /
``live.last_cycle_finished_at`` / ``live.last_cycle`` and the ``ingest_runs``
table -- see `docs/LIVE_INGEST.md` section 3.6), from the scanner's own session
calendar, and from the configured provider id.  Nothing is invented: a value we
cannot read comes back as ``None`` with a sibling ``note`` that says why.

Two rules this file exists to keep visible:

* **A vendor delay is not staleness.**  `market_data`'s 15-minute vendor
  declares ``delay_seconds=900`` by design (contract section 5); the newest bar
  being 15 minutes behind the clock is then correct, not stale.  The delay is
  reported so the app can subtract it before it shouts.
* **Stalled is not stale.**  "Stale" means the newest bar is behind the newest
  session the calendar expects.  "Stalled" means the ingest loop has not
  completed a cycle recently *while the market is open* -- the store can be
  perfectly current one minute and stalled the next.

Cost: one read-only SQLite open of `db/market15.db` per call, memoised for
``CACHE_SECONDS``.  `db/market15.db` and `db/kanida.db` are never written here.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

from .data import LEGACY, MARKET15, now_ist

#: `ingest_runs` and the `live.*` meta stamps are written in UTC
#: (`market_data.store.utcnow`); every timestamp this module publishes is IST,
#: so the offset is applied once, here.
IST_OFFSET = timedelta(hours=5, minutes=30)
BAR = timedelta(minutes=15)

#: How long a market15 read is reused.  `/api/state` is polled every 60 s by
#: every open tab; the store's answer does not move faster than a cycle.
CACHE_SECONDS = 20.0

#: With the market open, no completed cycle within this many minutes reads as
#: stalled.  The loop's own default interval is 300 s and a full NIFTY 500
#: 15-minute pass takes ~3 min (LIVE_INGEST.md section 4), so 20 minutes is a
#: missed bar, not a slow one.
DEFAULT_STALL_MINUTES = 20

PROVIDER_LABELS = {
    'kite': 'Zerodha Kite',
    'vendor15': '15-minute delayed vendor',
    'fake': 'Test fixtures',
}

SOURCE_LABELS = {
    LEGACY: 'Stored research scan',
    MARKET15: 'Live 15-minute store',
}

_CACHE: dict = {}
_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# provider
# ---------------------------------------------------------------------------
def provider_id():
    """The configured vendor id -- the same env `market_data.get_provider` reads."""
    return (os.environ.get('MARKET_DATA_PROVIDER') or 'kite').strip().lower()


def provider_delay_seconds(pid):
    """Seconds the vendor publishes behind real time, or None if we cannot say.

    Mirrors what the provider classes declare (`kite` 0, `vendor15`
    ``$VENDOR15_DELAY_SECONDS`` defaulting to 900) without constructing one --
    a provider's constructor wants credentials, and `/api/state` must never
    need them.
    """
    if pid == 'vendor15':
        try:
            return max(0, int(os.environ.get('VENDOR15_DELAY_SECONDS') or 900))
        except (TypeError, ValueError):
            return 900
    if pid in ('kite', 'fake'):
        return 0
    return None


def stall_minutes():
    try:
        return max(1, int(os.environ.get('SCANNER_INGEST_STALL_MINUTES') or DEFAULT_STALL_MINUTES))
    except (TypeError, ValueError):
        return DEFAULT_STALL_MINUTES


# ---------------------------------------------------------------------------
# time helpers (pure)
# ---------------------------------------------------------------------------
def _ist(value):
    """A stored UTC stamp as naive IST, or None when it is unusable."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', ''))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(tz=None).replace(tzinfo=None)
    return parsed + IST_OFFSET


def _text(value):
    return value.isoformat(sep=' ') if isinstance(value, datetime) else (value or None)


def _minutes(seconds):
    if seconds is None:
        return 'at an unknown time'
    minutes = int(seconds) // 60
    return 'less than a minute ago' if minutes < 1 else f'{minutes} minute{"" if minutes == 1 else "s"} ago'


def next_session(calendar, first_day, limit=20):
    """The first session on or after `first_day`, as (open, close); else None."""
    for offset in range(limit):
        found = calendar.session(first_day + timedelta(days=offset))
        if found:
            return found
    return None


def session_status(calendar, now):
    """Where the NSE day is, in IST: pre_open | open | closed | holiday.

    `reason` distinguishes a weekend from an exchange holiday; both read as
    `holiday` because the app only ever asks "should a bar be arriving?".
    """
    today = now.date()
    session = calendar.session(today)
    if session is None:
        weekend = today.weekday() >= 5
        upcoming = next_session(calendar, today + timedelta(days=1))
        return {'state': 'holiday', 'reason': 'weekend' if weekend else 'exchange_holiday',
                'date': today.isoformat(), 'open': None, 'close': None,
                'next_open': _text(upcoming[0]) if upcoming else None,
                'note': 'NSE is closed today ({}).'.format('weekend' if weekend else 'exchange holiday')}
    start, end = session
    if now < start:
        state, upcoming, note = 'pre_open', session, 'The session has not opened yet.'
    elif now <= end:
        state, upcoming, note = 'open', next_session(calendar, today + timedelta(days=1)), None
    else:
        state, upcoming, note = 'closed', next_session(calendar, today + timedelta(days=1)), \
            'The session has closed; the newest bar will not advance until the next open.'
    return {'state': state, 'reason': None, 'date': today.isoformat(),
            'open': _text(start), 'close': _text(end),
            'next_open': _text(upcoming[0]) if upcoming else None, 'note': note}


def next_bar_end(calendar, now, limit=20):
    """End of the next 15-minute bar the exchange will complete, naive IST."""
    for offset in range(limit):
        session = calendar.session(now.date() + timedelta(days=offset))
        if not session:
            continue
        start, end = session
        stamp = start + BAR
        while stamp <= end:
            if stamp > now:
                return stamp
            stamp += BAR
    return None


# ---------------------------------------------------------------------------
# the market15 store (read-only, memoised)
# ---------------------------------------------------------------------------
def read_market15(path, now=None):
    """What the live loop last recorded.  Never raises; reports the failure."""
    key = str(path)
    now = now or time.monotonic()
    with _LOCK:
        cached = _CACHE.get(key)
        if cached and cached[0] > now:
            return cached[1]
    value = _read_market15(path)
    with _LOCK:
        _CACHE[key] = (now + CACHE_SECONDS, value)
    return value


def clear_cache():
    with _LOCK:
        _CACHE.clear()


#: Columns of the live store's `quarantine` table this module publishes.
QUARANTINE_COLUMNS = ('symbol', 'reason', 'detail', 'first_seen', 'last_checked', 'checks')


def _read_quarantine(con):
    """Symbols the provider cannot serve, or None if the store predates the table.

    `market_data.live.ingest` skips these, which is why `last_run.errors` can be
    0 while six NIFTY 500 names are not advancing. Reporting the count as part of
    the same object keeps that from being an invisible omission: the panel can
    say "6 symbols unavailable" instead of showing a clean bill of health.
    """
    try:
        rows = con.execute(
            f"SELECT {','.join(QUARANTINE_COLUMNS)} FROM quarantine "
            f"WHERE status='quarantined' ORDER BY symbol").fetchall()
    except sqlite3.Error:
        return None
    return [dict(zip(QUARANTINE_COLUMNS, r)) for r in rows]


def _read_market15(path):
    out = {'latest_bar_start': None, 'latest_bar_end': None, 'cycle': None,
           'cycle_at': None, 'run': None, 'finished_run': None,
           'quarantine': None, 'error': None}
    try:
        con = sqlite3.connect(f'file:{Path(path).as_posix()}?mode=ro', uri=True, timeout=10)
    except sqlite3.Error as error:
        out['error'] = f'{path} could not be opened read-only ({error}).'
        return out
    try:
        con.execute('PRAGMA query_only=ON')
        meta = {}
        try:
            for name, value in con.execute(
                    "SELECT key,value FROM meta WHERE key LIKE 'live.%'"):
                meta[name] = value
        except sqlite3.Error:
            pass
        out['latest_bar_start'] = meta.get('live.latest_bar_start')
        out['latest_bar_end'] = meta.get('live.latest_bar_end')
        out['cycle_at'] = meta.get('live.last_cycle_finished_at')
        raw = meta.get('live.last_cycle')
        if raw:
            try:
                out['cycle'] = json.loads(raw)
            except (TypeError, ValueError):
                out['cycle'] = None
        columns = ('run_id', 'started_at', 'finished_at', 'provider', 'requests', 'rows',
                   'errors', 'status')
        sql = f"SELECT {','.join(columns)} FROM ingest_runs {{}}ORDER BY started_at DESC LIMIT 1"
        try:
            # The newest run, and separately the newest run that actually *finished* -- a cycle
            # in progress must not read as a missing one (that is what "stalled" is for).
            newest = con.execute(sql.format('')).fetchone()
            done = con.execute(sql.format('WHERE finished_at IS NOT NULL ')).fetchone()
        except sqlite3.Error:
            newest = done = None
            out['error'] = 'This store has no ingest_runs table; the live ingest loop has never written to it.'
        if newest:
            out['run'] = dict(zip(columns, newest))
        if done:
            out['finished_run'] = dict(zip(columns, done))
        out['quarantine'] = _read_quarantine(con)
    except sqlite3.Error as error:  # pragma: no cover - defensive
        out['error'] = f'{path} could not be read ({error}).'
    finally:
        con.close()
    return out


# ---------------------------------------------------------------------------
# the object itself
# ---------------------------------------------------------------------------
def last_run_block(store, session):
    """The store's last ingest cycle, or None plus the reason we have nothing."""
    if store.get('error') and not store.get('run'):
        return None, store['error']
    run, cycle = store.get('run'), store.get('cycle') or {}
    if not run and not cycle:
        return None, ('No ingest run is recorded in this store yet -- the live ingest loop '
                      '(market_data.live.cli run) has not completed a cycle against it.')
    # `ingest_runs` wins when it has a row: the loop's `meta` stats describe the last cycle that
    # *finished*, so mixing them in would hide a cycle that is currently in flight.
    if run:
        started, finished = _ist(run.get('started_at')), _ist(run.get('finished_at'))
    else:
        started = _ist(cycle.get('started_at'))
        finished = _ist(cycle.get('finished_at') or store.get('cycle_at'))
    # The newest cycle that actually finished; equal to `finished` unless one is in flight.
    done = store.get('finished_run') or {}
    success = _ist(done.get('finished_at')) or finished \
        or _ist(cycle.get('finished_at') or store.get('cycle_at'))
    age = lambda v: None if v is None else max(0, int((session['now'] - v).total_seconds()))
    symbols = cycle.get('symbols')
    bars = (run or {}).get('rows')
    if bars is None:
        bars = cycle.get('rows')
    errors = (run or {}).get('errors')
    if errors is None:
        errors = cycle.get('errors')
    notes = []
    if symbols is None:
        notes.append('Symbol count is unavailable: it is only in the loop\'s last-cycle stats, '
                     'which this store does not hold; ingest_runs does not record one.')
    running = finished is None and started is not None
    if running:
        notes.append('This cycle is still running; the counters are from the last finished one.')
    return {
        'run_id': (run or {}).get('run_id') or cycle.get('run_id'),
        'started_at': _text(started),
        'finished_at': _text(finished),
        'status': (run or {}).get('status'),
        'provider': (run or {}).get('provider'),
        'symbols': symbols,
        'requests': (run or {}).get('requests', cycle.get('requests')),
        'bars_written': bars,
        'daily_bars_written': cycle.get('daily_rows'),
        'errors': errors,
        'seconds': cycle.get('seconds'),
        'running': running,
        'age_seconds': age(finished),
        'started_age_seconds': age(started),
        'last_success_at': _text(success),
        'last_success_age_seconds': age(success),
        'note': ' '.join(notes) or None,
    }, None


def quarantine_block(store, live):
    """What the live loop is deliberately not fetching, and why.

    Without this the loop's own fix is invisible in the wrong direction: the
    error count drops to 0 because those symbols are skipped, and nothing on
    `/api/state` says their data has stopped advancing. A skipped symbol is
    *data unavailable*, not data that is fine.
    """
    if not live:
        return {'count': 0, 'symbols': [],
                'note': 'Only the live 15-minute store has a quarantine.'}
    rows = store.get('quarantine')
    if rows is None:
        return {'count': None, 'symbols': [],
                'note': ('This store has no quarantine table, so no symbol is being '
                         'skipped and none can be reported.')}
    if not rows:
        return {'count': 0, 'symbols': [],
                'note': 'The provider is serving every symbol in the universe.'}
    names = ', '.join(r['symbol'] for r in rows)
    return {
        'count': len(rows),
        'symbols': rows,
        'note': ('{} symbol{} cannot be fetched from this provider at all, so {} data '
                 'is unavailable and stops at the last bar already stored: {}. They are '
                 're-checked once a day and return automatically. They are excluded from '
                 'the ingest cycle, so they do not appear in `last_run.errors`.').format(
                     len(rows), '' if len(rows) == 1 else 's',
                     'its' if len(rows) == 1 else 'their', names),
    }


def _stamp(value):
    """A metadata stamp (str or datetime) as 'YYYY-MM-DD HH:MM:SS', or None."""
    if not value:
        return None
    return (value.isoformat(sep=' ') if isinstance(value, datetime) else str(value))[:19]


def patterns_block(scanner, now, live):
    """When patterns were last brought up to date, and when they next will be.

    Prices (`latest_bar`) advance every 15 minutes, but the scanner only
    rescans a timeframe when its candle completes -- 1H hourly from 10:15,
    4H at 13:15 and the close, 1D/1W after the close (engine.watch).  So a
    15-minute bar newer than the newest scanned 1H candle is normal, not stale.
    This block says which candle each timeframe has actually scanned, which
    candle the calendar says it should have scanned by now, and whether that
    gap is a scan in progress (``updating``) or genuinely ``behind`` (the
    candle closed more than the stall limit ago and is still unscanned).
    """
    frames = (getattr(scanner, 'metadata', None) or {}).get('timeframes', {}) or {}
    timeframes = list(scanner.config['timeframes'])
    scanned = {tf: _stamp((frames.get(tf) or {}).get('as_of')) for tf in timeframes}
    known = [v for v in scanned.values() if v]
    latest = max(known) if known else None
    if not live:
        return {'latest': latest, 'next_update': None, 'up_to_date': None,
                'updating': [], 'behind': [],
                'by_timeframe': {tf: {'scanned': scanned[tf], 'scanned_at': _stamp(
                    (frames.get(tf) or {}).get('finished_at')), 'expected': None,
                    'next_close': None, 'due': None, 'behind': None, 'scanning': None,
                    'overdue_seconds': None} for tf in timeframes},
                'note': ('The stored research source does not advance on a candle schedule, so '
                         'no pattern update is expected.')}
    calendar = scanner.calendar
    settle = timedelta(seconds=int((scanner.config or {}).get('settlement_delay_seconds') or 0))
    grace = stall_minutes() * 60
    progress = getattr(scanner, 'progress', None) or {}
    scanning = set(progress.get('timeframes') or []) if progress.get('running') else set()
    by_tf, updating, behind, nexts = {}, [], [], []
    for tf in timeframes:
        expected = calendar.latest_close(tf, now - settle)
        next_close = calendar.next_close(tf, now)
        if next_close:
            nexts.append(next_close)
        due = bool(expected and (scanned[tf] or '') < expected)
        overdue = None
        if due:
            overdue = max(0, int((now - datetime.fromisoformat(expected)).total_seconds()))
        late = bool(due and overdue is not None and overdue > grace)
        if late:
            behind.append(tf)
        elif due:
            updating.append(tf)
        by_tf[tf] = {'scanned': scanned[tf],
                     'scanned_at': _stamp((frames.get(tf) or {}).get('finished_at')),
                     'expected': expected, 'next_close': next_close, 'due': due,
                     'behind': late, 'scanning': tf in scanning, 'overdue_seconds': overdue}
    if behind:
        note = ('Behind: {} has a completed candle that closed more than {} minutes ago and has '
                'not been scanned.').format(', '.join(behind), grace // 60)
    elif updating:
        note = ('A candle for {} has just completed; the scan normally lands within a few '
                'minutes.').format(', '.join(updating))
    else:
        note = 'Every timeframe has scanned its newest completed candle.'
    return {'latest': latest, 'next_update': min(nexts) if nexts else None,
            'up_to_date': not behind and not updating, 'updating': updating, 'behind': behind,
            'by_timeframe': by_tf, 'note': note}


def _patterns_or_note(scanner, now, live):
    try:
        return patterns_block(scanner, now, live)
    except Exception as exc:  # noqa: BLE001 - never take the rest of data_status down
        return {'latest': None, 'next_update': None, 'up_to_date': None, 'updating': [],
                'behind': [], 'by_timeframe': {}, 'error': str(exc),
                'note': 'The pattern scan schedule could not be read; see the scanner log.'}


def data_status(scanner, source_latest='', source_stale=False, now=None):
    """The `data_status` object for `/api/state`.  Additive; never raises."""
    now = (now or now_ist()).replace(microsecond=0)
    calendar = scanner.calendar
    source = scanner.candle_source
    live = source == MARKET15
    pid = provider_id()
    delay = provider_delay_seconds(pid)
    session = session_status(calendar, now)
    session_ctx = dict(session, now=now)

    store = read_market15(scanner.market15) if live else {}
    latest_start = (store.get('latest_bar_start') if live else None) or None
    latest_end = (store.get('latest_bar_end') if live else None) or None
    bar_note = None
    if live:
        if not latest_start:
            latest_start = source_latest or None
            bar_note = ('The live loop has not published live.latest_bar_start in this store, so '
                        'this is the newest bar the scanner itself found.' if latest_start else
                        'Unavailable: the live loop has not published live.latest_bar_start and '
                        'the scanner found no completed bar in this store.')
        if latest_start and not latest_end:
            try:
                latest_end = _text(datetime.fromisoformat(latest_start) + BAR)
            except (TypeError, ValueError):
                latest_end = None
    else:
        latest_start = source_latest or None
        latest_end = None
        bar_note = ('The stored research source keeps daily bars only, so this is a session date, '
                    'not a 15-minute bar; no bar end is recorded.')

    frames = scanner.metadata.get('timeframes', {}) or {}
    by_timeframe = {tf: (frames.get(tf, {}).get('as_of') or None)
                    for tf in scanner.config['timeframes']}

    last_run, run_note = last_run_block(store, session_ctx) if live else (
        None, 'The stored research source is refreshed by an operator run, not by the live ingest loop.')

    if live:
        target = next_bar_end(calendar, now)
        if target is None:
            refresh = {'at': None, 'in_seconds': None,
                       'note': 'No trading session was found in the next 20 days.'}
        else:
            at = target + timedelta(seconds=delay or 0)
            refresh = {'at': _text(at), 'in_seconds': max(0, int((at - now).total_seconds())),
                       'note': ('The bar closes at {}; the loop then needs a cycle (a few minutes '
                                'for NIFTY 500) to write it.').format(_text(target))
                       if not delay else
                       ('The bar closes at {} and this vendor publishes {} s later by design; the '
                        'loop then needs a cycle to write it.').format(_text(target), delay)}
    else:
        refresh = {'at': None, 'in_seconds': None,
                   'note': 'The stored research source advances only when an operator refreshes it.'}

    stall_after = stall_minutes()
    stalled, stall_note = False, None
    if not live:
        stall_note = 'Only the live 15-minute store is expected to refresh on its own.'
    elif session['state'] != 'open':
        stall_note = 'Ingestion is only expected to run while the market is open.'
    elif last_run is None:
        stalled, stall_note = True, run_note
    elif last_run['running'] and (last_run['started_age_seconds'] or 0) <= stall_after * 60:
        # A cycle in flight is the loop working, not a missing one.
        stall_note = 'A cycle is running now; it started {}.'.format(
            _minutes(last_run['started_age_seconds']))
    elif last_run['last_success_age_seconds'] is None:
        stalled = True
        stall_note = 'No ingest cycle in this store has ever recorded a finish.'
    elif last_run['last_success_age_seconds'] > stall_after * 60:
        stalled = True
        stall_note = ('The market is open but the last ingest cycle finished {} minutes ago '
                      '(limit {}).').format(last_run['last_success_age_seconds'] // 60, stall_after)
    elif last_run.get('errors'):
        stall_note = 'The last cycle completed with errors; some symbols may be behind.'

    return {
        'version': 1,
        'as_of': _text(now),
        'timezone': 'Asia/Kolkata',
        'provider': {
            'id': pid,
            'label': PROVIDER_LABELS.get(pid, pid),
            'delay_seconds': delay,
            'note': (None if delay is not None else
                     'This provider is not one market_data ships, so its publication delay is unknown.'),
        },
        'source': {
            'kind': source,
            'label': SOURCE_LABELS.get(source, source),
            'live': live,
            'store': str(scanner.market15.name) if live and scanner.market15 else 'kanida.db',
            'note': (None if live else
                     'SCANNER_CANDLE_SOURCE=legacy: candles come from the frozen research store, '
                     'not from a live feed.'),
        },
        'latest_bar': {
            'start': latest_start,
            'end': latest_end,
            'by_timeframe': by_timeframe,
            'note': bar_note or ('Per-timeframe values are the newest candle end the last scan '
                                 'saw; expected closes are in `schedule`.'),
        },
        # Additive: what the pattern scan has covered, separate from the price bar above.
        'patterns': _patterns_or_note(scanner, now, live),
        'last_run': last_run,
        'last_run_note': run_note,
        'quarantine': quarantine_block(store, live),
        'next_refresh': refresh,
        'session': {k: v for k, v in session.items() if k != 'now'},
        'stalled': {'value': stalled, 'after_minutes': stall_after, 'note': stall_note},
        'stale': {'value': bool(source_stale),
                  'note': ('The newest bar is behind the newest session the calendar expects.'
                           if source_stale else None)},
    }
