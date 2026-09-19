"""Read-only OHLC adapter; exchange-local completed candles, never interpolated.

Two candle sources, chosen by ``$SCANNER_CANDLE_SOURCE``:

``legacy`` (default)
    ``db/kanida.db`` -- ``ohlc_5min`` aggregated to 1H/4H, ``ohlc_daily`` to
    1D/1W.  Unchanged; this is what the frozen research and every existing
    snapshot were built on.
``market15``
    ``db/market15.db`` -- the revisioned 15-minute store (contract section 2)
    aggregated through ``market_data.aggregate``, with 1D/1W taken from the
    provider's own daily bars because a CAS stock's official close is an
    auction price that exists nowhere in the intraday series (contract 2A).

Both sources hand the detectors **the same record**::

    {'time','end','open','high','low','close','volume','gap'}

with the same semantics -- completed buckets only, and ``gap`` set when a
candle does not follow the previous one's expected close or opens more than 35%
away from it.  The gap flag is computed by one shared function
(:func:`finish_bars`) so the two sources cannot drift apart.
"""
from __future__ import annotations

import json
import os
import sqlite3
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
IST = timezone(timedelta(hours=5, minutes=30))
TIMEFRAMES = ('1H', '4H', '1D', '1W')

LEGACY = 'legacy'
MARKET15 = 'market15'
#: 09:15-15:15 continuous trading for a CAS stock, from this date (contract 2A).
CAS_START = date(2026, 8, 3)
CAS_CLOSE = time(15, 15)


def candle_source(config=None):
    """``legacy`` (default) or ``market15``. Env wins over config."""
    value = (os.environ.get('SCANNER_CANDLE_SOURCE')
             or (config or {}).get('candle_source') or LEGACY)
    value = str(value).strip().lower()
    if value not in (LEGACY, MARKET15):
        raise ValueError(f"SCANNER_CANDLE_SOURCE must be {LEGACY!r} or {MARKET15!r}, not {value!r}")
    return value


def market15_path(config=None):
    return Path(os.environ.get('SCANNER_MARKET15_DB')
                or (config or {}).get('market15_database')
                or (ROOT.parent / 'db' / 'market15.db')).resolve()


def now_ist():
    return datetime.now(IST).replace(tzinfo=None)


def load_config():
    return json.loads((ROOT / 'config.json').read_text(encoding='utf-8'))


class Calendar:
    def __init__(self, config, observed=()):
        self.config = config
        self.observed = set(observed)
        self.first_observed = min(self.observed, default=None)
        self.last_observed = max(self.observed, default='1900-01-01')
        self.holidays = set(config['holidays'])
        self.special = config['special_sessions']

    def session(self, day: date):
        key = day.isoformat()
        if key in self.special:
            return tuple(datetime.combine(day, time.fromisoformat(t)) for t in self.special[key])
        if key in self.config.get('closed_special_dates', []):
            return None
        if key <= self.last_observed and day.year not in self.config['calendar_years']:
            is_open = key in self.observed
        else:
            is_open = day.weekday() < 5 and key not in self.holidays
        return (datetime.combine(day, time(9, 15)), datetime.combine(day, time(15, 30))) if is_open else None

    def week_sessions(self, day):
        monday = day - timedelta(days=day.weekday())
        return [s for offset in range(7) if (s := self.session(monday + timedelta(days=offset)))]

    def intervals(self, day, timeframe):
        session = self.session(day)
        if not session:
            return []
        start, end = session
        if timeframe == '1D':
            return [(start, end)]
        if timeframe == '1W':
            week = self.week_sessions(day)
            return [(week[0][0], week[-1][1])] if day == week[-1][1].date() else []
        result = []
        step = timedelta(hours=1 if timeframe == '1H' else 4)
        while start < end:
            stop = min(start + step, end)
            result.append((start, stop))
            start = stop
        return result

    def latest_close(self, timeframe, asof):
        for offset in range(20):
            day = asof.date() - timedelta(days=offset)
            eligible = [end for _, end in self.intervals(day, timeframe) if end <= asof]
            if eligible:
                return max(eligible).isoformat(sep=' ')
        return None

    def next_close(self, timeframe, asof):
        for offset in range(20):
            day = asof.date() + timedelta(days=offset)
            eligible = [end for _, end in self.intervals(day, timeframe) if end > asof]
            if eligible:
                return min(eligible).isoformat(sep=' ')
        return None


class ClosingConnection(sqlite3.Connection):
    """sqlite3's default context manager commits but does not close the handle."""
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


def connect_source(path):
    con = sqlite3.connect(f'file:{Path(path).resolve().as_posix()}?mode=ro', uri=True, timeout=30, factory=ClosingConnection)
    con.execute('PRAGMA query_only=ON')
    return con


def universe(con):
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute("""SELECT symbol, COALESCE(company,company_name,symbol) AS company,
        COALESCE(sector,'Unclassified') AS sector, instrument_type,
        (SELECT max(bar_time) FROM ohlc_daily d WHERE d.symbol=l.symbol) AS daily_latest,
        (SELECT max(bar_time) FROM ohlc_5min i WHERE i.symbol=l.symbol) AS intraday_latest
        FROM instrument_labels l WHERE exchange='NSE' AND instrument_type IN ('STOCK','EQ')
        AND is_active=1 ORDER BY symbol""")]
    con.row_factory = None
    return rows


def observed_sessions(con):
    return [r[0][:10] for r in con.execute("SELECT bar_time FROM ohlc_daily WHERE symbol IN ('RELIANCE','AMRUTANJAN')")]


def load_rows(con, symbol, intraday, cutoff, limit=None):
    table = 'ohlc_5min' if intraday else 'ohlc_daily'
    # Existing (symbol, bar_time) primary-key index bounds every read.
    # `limit` stays None on the legacy pattern path, so that read is unchanged;
    # the research pattern set raises it to the measured warm-up (see
    # market_scanner/pattern_live.py).
    rows = con.execute(f"SELECT bar_time,open,high,low,close,volume FROM {table} WHERE symbol=? AND bar_time<=? ORDER BY bar_time DESC LIMIT ?",
                       (symbol, cutoff.isoformat(sep=' '), limit or (11000 if intraday else 1500))).fetchall()
    return list(reversed(rows))


def valid_ohlc(row):
    import math
    _, o, h, l, c, v = row
    return all(isinstance(x, (int, float)) and math.isfinite(x) for x in row[1:]) and min(o, h, l, c) > 0 and v >= 0 and h >= max(o, c, l) and l <= min(o, c)


def aggregate(rows, timeframe, calendar, cutoff, limit=260):
    groups = defaultdict(list)
    unknown_weeks = set()
    invalid = 0
    for row in rows:
        if not valid_ohlc(row):
            invalid += 1
            continue
        stamp = datetime.fromisoformat(row[0])
        if stamp.tzinfo:
            stamp = stamp.astimezone(IST).replace(tzinfo=None)
        session = calendar.session(stamp.date())
        if not session:
            continue
        if timeframe in ('1H', '4H'):
            if not session[0] <= stamp < session[1] or stamp.minute % 5 or stamp.second:
                continue
            minutes = 60 if timeframe == '1H' else 240
            bucket = int((stamp - session[0]).total_seconds() // (minutes * 60))
            start = session[0] + timedelta(minutes=bucket * minutes)
            end = min(start + timedelta(minutes=minutes), session[1])
            expected = int((end - start).total_seconds() // 300)
        elif timeframe == '1D':
            start, end = session
            expected = 1
        else:
            monday = stamp.date() - timedelta(days=stamp.weekday())
            # The observed calendar cannot establish whether earlier weekdays
            # in its first week were holidays or simply absent from the data.
            # Reject that edge week instead of treating missing days as closed.
            if calendar.first_observed and monday.isoformat() < calendar.first_observed and monday.year not in calendar.config['calendar_years']:
                unknown_weeks.add(monday)
                continue
            week = calendar.week_sessions(stamp.date())
            start, end, expected = week[0][0], week[-1][1], len(week)
        if end > cutoff:
            continue
        groups[(start, end, expected)].append((stamp, row))
    bars = []
    incomplete = len(unknown_weeks)
    for (start, end, expected), group in sorted(groups.items()):
        stamps = {r[0] for r in group}
        complete = len(stamps) == expected and len(group) == expected
        if timeframe in ('1H', '4H'):
            complete &= all(start + timedelta(minutes=5*i) in stamps for i in range(expected))
        elif timeframe == '1W':
            complete &= {s[0].date() for s in calendar.week_sessions(start.date())} == {s.date() for s in stamps}
        if not complete:
            incomplete += 1
            continue
        source = [r[1] for r in group]
        bars.append({'time':start.isoformat(sep=' '), 'end':end.isoformat(sep=' '),
                     'open':source[0][1], 'high':max(r[2] for r in source),
                     'low':min(r[3] for r in source), 'close':source[-1][4],
                     'volume':sum(r[5] for r in source), 'gap':False})
    bars = finish_bars(bars, timeframe, calendar, limit)
    return bars, {'invalid_rows':invalid, 'incomplete_buckets':incomplete, 'gaps':sum(b['gap'] for b in bars)}


def finish_bars(bars, timeframe, calendar, limit=260):
    """Trim to `limit` and set `gap`. Shared by both candle sources, so the
    record handed to a detector has identical semantics whichever store it
    came from."""
    bars = bars[-limit:]
    for previous, current in zip(bars, bars[1:]):
        next_expected = calendar.next_close(timeframe, datetime.fromisoformat(previous['end']))
        # A detector may not span an omitted/missing candle or an extreme unadjusted price discontinuity.
        current['gap'] = current['end'] != next_expected or abs(current['open'] / previous['close'] - 1) > .35
    return bars


# ---------------------------------------------------------------------------
# market15 source (db/market15.db, contract sections 2 and 2A)
# ---------------------------------------------------------------------------
class SymbolCalendarView(Calendar):
    """The scanner calendar as one symbol experiences it.

    From 2026-08-03 a cash stock with F&O contracts stops continuous trading at
    15:15 and its close is struck in the 15:30-15:35 auction (contract 2A), so
    its last 1H bucket is 14:15-15:15 and its last 4H bucket 13:15-15:15. Every
    derived method (`intervals`, `week_sessions`, `latest_close`, `next_close`)
    goes through `session`, so overriding that is enough.
    """

    def __init__(self, base, cas_from=None):
        self.__dict__.update(base.__dict__)
        self.cas_from = cas_from

    def session(self, day: date):
        window = super().session(day)
        if window and self.cas_from and day >= self.cas_from and window[1].time() == time(15, 30):
            return (window[0], datetime.combine(day, CAS_CLOSE))
        return window


def open_market15(path=None, config=None):
    """Read-only handle on the 15-minute store. One per scan thread."""
    from market_data.store import MarketStore
    return MarketStore(path or market15_path(config), read_only=True)


def market15_symbols(store):
    return {r[0] for r in store.con.execute('SELECT DISTINCT symbol FROM candles_15m')}


def market15_latest(store):
    """Newest completed `bar_start` the store holds; O(1) where the live loop
    has published it, otherwise a bounded index lookup."""
    from market_data.live.ingest import latest_live_bar
    return latest_live_bar(store)


def market15_coverage(store):
    """{symbol: (intraday_latest, daily_latest)} -- one pass per scan, not per poll."""
    intraday = {r[0]: r[1] for r in store.con.execute(
        'SELECT symbol, MAX(bar_start) FROM candles_15m GROUP BY symbol')}
    try:
        daily = {r[0]: r[1] for r in store.con.execute(
            'SELECT symbol, MAX(bar_end) FROM daily_bars GROUP BY symbol')}
    except sqlite3.Error:
        daily = {}
    return {s: (intraday.get(s), daily.get(s)) for s in set(intraday) | set(daily)}


def market15_snapshot(store):
    """Newest frozen snapshot id in the 15-minute store, or None.

    Recorded on every live detection as the input identity, so a detection can
    always be traced back to the bytes it was computed from (evidence contract
    section 4). Absent is absent -- it is never guessed."""
    try:
        row = store.con.execute(
            'SELECT snapshot_id FROM snapshots ORDER BY created_at DESC LIMIT 1').fetchone()
    except sqlite3.Error:
        return None
    return row[0] if row else None


def market15_has_daily(store):
    """Whether the live ingest's provider-daily table exists yet. Without it
    there are no 1D/1W candles in market15 mode -- a CAS stock's official close
    lives only there (contract 2A), so we never fall back to an intraday close."""
    return bool(store.con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='daily_bars'").fetchone())


def market15_regimes(store):
    """{symbol: first CAS date} from the persisted session regimes."""
    out = {}
    try:
        rows = store.con.execute(
            "SELECT symbol, MIN(effective_from) FROM session_regimes "
            "WHERE regime='cas' GROUP BY symbol")
    except sqlite3.Error:
        return out
    for symbol, first in rows:
        if first:
            out[symbol] = date.fromisoformat(first)
    return out


def load_market15(store, symbol, intraday, cutoff, sessions=170, daily_bars=1600):
    """The rows one symbol needs, bounded exactly like the legacy loader.

    `intraday` selects the 15-minute series (1H/4H); otherwise the provider's
    own daily bars (1D/1W) -- the 1D close of a CAS stock is only ever there.
    """
    if intraday:
        start = cutoff - timedelta(days=int(sessions * 1.45))
        return store.read_bars(symbol, start, cutoff)
    if not market15_has_daily(store):
        return []
    from market_data.live.ingest import read_daily_bars
    bars = read_daily_bars(store, symbol, end=cutoff.date())
    return bars[-daily_bars:]


class MarketDataCalendarAdapter:
    """`finish_bars` needs one thing: "what close should follow this one?".

    For market15 that question must be answered by the **same** calendar that
    made the buckets -- `market_data.calendar` -- or the bars and the `gap`
    flag disagree with each other.  The two calendars differ deliberately on
    two kinds of day: Muhurat (market_data uses the observed evening window,
    the scanner's config calendar assumes 09:15-15:30) and budget/DR Saturdays
    (market_data keeps 1W strictly Mon-Fri, the scanner's week walks all seven
    days).  Asking the scanner's calendar produced ~14 phantom 1W gaps per 260
    weeks; asking this one produces none.
    """

    def __init__(self, symbol_calendar):
        self.symbol_calendar = symbol_calendar

    def next_close(self, timeframe, asof):
        from market_data.aggregate import buckets_for
        for offset in range(21):
            day = asof.date() + timedelta(days=offset)
            ends = [end for _, end, _ in buckets_for(timeframe, self.symbol_calendar, [day])
                    if end > asof]
            if ends:
                return min(ends).isoformat(sep=' ')
        return None


def _valid_bar(bar):
    import math
    values = (bar.open, bar.high, bar.low, bar.close)
    return (all(isinstance(x, (int, float)) and math.isfinite(x) for x in values)
            and min(values) > 0 and bar.volume >= 0
            and bar.high >= max(bar.open, bar.close, bar.low)
            and bar.low <= min(bar.open, bar.close))


def aggregate_market15(rows, timeframe, symbol_calendar, view=None, cutoff=None, limit=260):
    """15-minute (or daily) store rows -> the detector candle records.

    `symbol_calendar` is a `market_data.calendar.SymbolCalendar`: it drives the
    buckets, the per-symbol/per-date expected bar count (contract 2A) *and* the
    `gap` rule, so the candles and their gap flags always come from one
    calendar.  `view` is accepted for symmetry with the legacy call and is
    unused.
    """
    from market_data.aggregate import aggregate as md_aggregate, weekly_from_daily

    clean, invalid = [], 0
    for bar in rows or []:
        if _valid_bar(bar):
            clean.append(bar)
        else:
            invalid += 1

    if timeframe in ('1H', '4H'):
        # intraday buckets follow the symbol's own session end (15:15 under CAS)
        gap_calendar = symbol_calendar
        buckets = md_aggregate(clean, timeframe, symbol_calendar, as_of=cutoff)
    else:
        # 1D/1W come from the provider's daily bar, whose close IS the 15:30-15:35
        # auction price, so a CAS day's 1D candle still ends at the market-wide
        # 15:30 -- exactly as the legacy `ohlc_daily` path has always emitted it.
        gap_calendar = getattr(symbol_calendar, 'calendar', symbol_calendar)
        daily = [b for b in clean if b.bar_end <= cutoff]
        buckets = daily if timeframe == '1D' else weekly_from_daily(daily, gap_calendar)

    bars, incomplete = [], 0
    for bucket in buckets:
        if bucket.bar_end > cutoff:
            continue
        if not bucket.candle_complete:
            incomplete += 1
            continue
        bars.append({'time':bucket.bar_start.isoformat(sep=' '), 'end':bucket.bar_end.isoformat(sep=' '),
                     'open':bucket.open, 'high':bucket.high, 'low':bucket.low,
                     'close':bucket.close, 'volume':bucket.volume, 'gap':False})
    bars = finish_bars(bars, timeframe, MarketDataCalendarAdapter(gap_calendar), limit)
    return bars, {'invalid_rows':invalid, 'incomplete_buckets':incomplete, 'gaps':sum(b['gap'] for b in bars)}
