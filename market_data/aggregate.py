"""Pure, deterministic aggregation of 15-minute bars.

Contract: docs/DATA_PIPELINE_CONTRACT.md section 2.

    15m -> 1H  (4 x 15m; the session's trailing 15:15-15:30 bucket is a
                session-bounded 1-hour bucket holding a single 15m bar)
        -> 4H  (buckets 09:15-13:15 and 13:15-15:30, matching the frozen
                research in market_scanner/output/expanded_research/...)
        -> 1D  (the session)
        -> 1W  (Mon-Fri)

Rules
-----
* open  = first constituent's open (by bar_start)
* high  = max of constituent highs
* low   = min of constituent lows
* close = last constituent's close (by bar_start)
* volume = sum
* A bucket missing any expected constituent, holding an incomplete
  constituent, or not yet closed is emitted with ``candle_complete=False``.
* **No synthetic bars.** A bucket with zero constituents is not emitted.
* Every function here is pure: same inputs -> same outputs, no I/O, no clock.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence

from market_data.calendar import BAR_MINUTES, Session, SessionCalendar

# W1 owns market_data/types.py (RawCandle). We only need it for the adapter
# below, so the import is optional -- this module works without it.
try:  # pragma: no cover - depends on W1's file existing
    from market_data.types import RawCandle  # type: ignore
except Exception:  # pragma: no cover
    RawCandle = None  # type: ignore

TIMEFRAMES = ("15m", "1H", "4H", "1D", "1W")
FOUR_HOUR_SPLIT_MINUTE = (13, 15)  # 13:15 -- the frozen-research 4H boundary

# A session whose *tail* bars are genuinely missing, measured against the
# session's own regime grid (contract 2A). A CAS stock that runs to 15:15 from
# 2026-08-03 is NOT truncated -- 15:15 is its session end and 24 bars is its
# expected count; only a bar missing from the regime's grid counts here.
FLAG_SESSION_TRUNCATED = "session_truncated"
FLAG_FROM_PROVIDER_DAILY = "from_provider_daily"

#: SQLite stores integers as signed 64-bit. A legacy volume above this
#: cannot be bound as an INTEGER; validate.py flags it and store.py writes
#: it as a REAL rather than dropping the row.
SQLITE_MAX_INT = 2 ** 63 - 1


@dataclass(frozen=True)
class Bar:
    """One OHLCV bar. Immutable so aggregation stays referentially transparent."""

    bar_start: datetime
    bar_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    candle_complete: bool = True
    quality_flags: str = ""
    constituents: int = 1

    def as_dict(self) -> dict:
        return {
            "time": self.bar_start.isoformat(sep=" "),
            "end": self.bar_end.isoformat(sep=" "),
            "open": self.open, "high": self.high, "low": self.low,
            "close": self.close, "volume": self.volume,
            "candle_complete": self.candle_complete,
            "quality_flags": self.quality_flags,
        }

    def with_flags(self, flags: str) -> "Bar":
        return replace(self, quality_flags=flags)


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------
#: W1's provider layer stamps every bar tz-aware IST; the store and every
#: legacy table use naive IST. Convert explicitly -- never via the machine's
#: local timezone, which is not guaranteed to be IST.
_IST = timezone(timedelta(hours=5, minutes=30), "IST")

#: W1's RawCandle.quality_flags is an int bitmask (market_data.types.QualityFlag);
#: our column is a comma-separated name list. Same vocabulary, two encodings.
QUALITY_BITS = (
    (1 << 0, "ohlc_order"), (1 << 1, "non_positive"), (1 << 2, "zero_volume"),
    (1 << 3, "off_grid"), (1 << 4, "outside_session"), (1 << 5, "partial_bar"),
    (1 << 6, "vendor_suspect"), (1 << 7, "session_truncated"),
)


def _to_datetime(value) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(_IST).replace(tzinfo=None)
    return datetime.fromisoformat(str(value))


def normalise_quality_flags(value) -> str:
    """Accept either an int bitmask (W1) or a comma-separated string (ours)."""
    if value in (None, "", 0):
        return ""
    if isinstance(value, bool):
        return ""
    if isinstance(value, int):
        names = [name for bit, name in QUALITY_BITS if value & bit]
        leftover = value & ~sum(bit for bit, _ in QUALITY_BITS)
        if leftover:
            names.append(f"bit_{leftover}")
        return ",".join(sorted(names))
    return ",".join(sorted(filter(None, str(value).split(","))))


def bar_from_raw(raw) -> Bar:
    """Adapt a provider ``RawCandle`` (W1) or a 6-tuple/mapping into a ``Bar``.

    Accepts anything exposing ``bar_start``/``ts``/``date``/``timestamp`` plus
    open/high/low/close/volume, a mapping with those keys, or the legacy
    ``(bar_time, open, high, low, close, volume)`` tuple used by kanida.db.
    """
    if isinstance(raw, Bar):
        return raw
    if isinstance(raw, (tuple, list)):
        bt, o, h, l, c, v = raw[:6]
        start = _to_datetime(bt)
        return Bar(start, start + timedelta(minutes=BAR_MINUTES),
                   float(o), float(h), float(l), float(c), int(v or 0))
    get = (raw.get if isinstance(raw, dict) else lambda k, d=None: getattr(raw, k, d))
    bt = None
    for key in ("bar_start", "ts", "timestamp", "date", "time", "bar_time"):
        bt = get(key)
        if bt is not None:
            break
    if bt is None:
        raise ValueError(f"cannot find a timestamp on {raw!r}")
    start = _to_datetime(bt)
    end = get("bar_end")
    return Bar(
        start,
        _to_datetime(end) if end else start + timedelta(minutes=BAR_MINUTES),
        float(get("open")), float(get("high")), float(get("low")),
        float(get("close")), int(get("volume") or 0),
        bool(get("candle_complete", True)),
        normalise_quality_flags(get("quality_flags", "")),
    )


# ---------------------------------------------------------------------------
# minute rows -> 15m
# ---------------------------------------------------------------------------
def floor_to_grid(stamp: datetime, minutes: int = BAR_MINUTES,
                  anchor: datetime | None = None) -> datetime:
    """Floor `stamp` onto a `minutes` grid anchored at `anchor` (default midnight)."""
    if anchor is None:
        base = stamp.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        base = anchor
    delta = int((stamp - base).total_seconds() // 60)
    return base + timedelta(minutes=(delta // minutes) * minutes)


def to_15m(rows: Iterable, source_minutes: int,
           calendar: SessionCalendar | None = None) -> list[Bar]:
    """Aggregate 1-minute or 5-minute rows onto the 15-minute grid.

    Both 1m and 5m divide 15m exactly, so this is lossless: the resulting OHLCV
    is identical whichever source interval is used (proved in
    tests/test_aggregate.py against real kanida.db rows).
    """
    if 15 % source_minutes:
        raise ValueError(f"{source_minutes}m does not divide 15m exactly")
    expected = 15 // source_minutes
    buckets: dict[datetime, list[Bar]] = {}
    for row in rows:
        bar = bar_from_raw(row)
        if calendar is not None:
            session = calendar.session(bar.bar_start.date())
            if session is None or not session.contains(bar.bar_start):
                continue
            anchor = session.start
        else:
            anchor = None
        key = floor_to_grid(bar.bar_start, BAR_MINUTES, anchor)
        buckets.setdefault(key, []).append(bar)

    out: list[Bar] = []
    for start in sorted(buckets):
        group = sorted(buckets[start], key=lambda b: b.bar_start)
        end = start + timedelta(minutes=BAR_MINUTES)
        if calendar is not None:
            session = calendar.session(start.date())
            if session is not None:
                end = min(end, session.end)
        distinct = len({b.bar_start for b in group})
        out.append(Bar(
            bar_start=start,
            bar_end=end,
            open=group[0].open,
            high=max(b.high for b in group),
            low=min(b.low for b in group),
            close=group[-1].close,
            volume=sum(b.volume for b in group),
            candle_complete=distinct == expected,
            constituents=len(group),
        ))
    return out


# ---------------------------------------------------------------------------
# bucket geometry
# ---------------------------------------------------------------------------
def hour_buckets(session: Session) -> list[tuple[datetime, datetime]]:
    """1H buckets, clipped to the session close.

    For the regular 09:15-15:30 session this is six 60-minute buckets plus the
    trailing 15:15-15:30 bucket (one 15m bar) -- exactly what the frozen
    research history contains.
    """
    out, cur = [], session.start
    while cur < session.end:
        out.append((cur, min(cur + timedelta(minutes=60), session.end)))
        cur += timedelta(minutes=60)
    return out


def four_hour_buckets(session: Session) -> list[tuple[datetime, datetime]]:
    """4H buckets: 09:15-13:15 and 13:15-15:30 for a regular session."""
    split = session.start.replace(hour=FOUR_HOUR_SPLIT_MINUTE[0],
                                  minute=FOUR_HOUR_SPLIT_MINUTE[1],
                                  second=0, microsecond=0)
    if session.start < split < session.end:
        return [(session.start, split), (split, session.end)]
    return [(session.start, session.end)]


def day_buckets(session: Session) -> list[tuple[datetime, datetime]]:
    return [(session.start, session.end)]


def _expected_15m(start: datetime, end: datetime) -> int:
    return max(int((end - start).total_seconds() // (BAR_MINUTES * 60)), 0)


def buckets_for(timeframe: str, calendar: SessionCalendar,
                days: Sequence[date]) -> list[tuple[datetime, datetime, int]]:
    """(start, end, expected_15m_bars) for every bucket over `days`."""
    out: list[tuple[datetime, datetime, int]] = []
    if timeframe == "1W":
        weeks: dict[date, list[Session]] = {}
        for d in days:
            session = calendar.session(d)
            if session is None or d.weekday() >= 5:
                continue
            monday = d - timedelta(days=d.weekday())
            weeks.setdefault(monday, [])
        for monday in sorted(weeks):
            wk = calendar.week_sessions(monday)
            if not wk:
                continue
            expected = sum(s.expected_bars for s in wk)
            out.append((wk[0].start, wk[-1].end, expected))
        return out

    maker = {"1H": hour_buckets, "4H": four_hour_buckets,
             "1D": day_buckets}[timeframe]
    for d in days:
        session = calendar.session(d)
        if session is None:
            continue
        for start, end in maker(session):
            out.append((start, end, _expected_15m(start, end)))
    return out


# ---------------------------------------------------------------------------
# 15m -> higher timeframe
# ---------------------------------------------------------------------------
def truncated_sessions(bars_15m: Sequence[Bar],
                       calendar: SessionCalendar) -> dict[date, datetime]:
    """Days whose *trailing* 15m bars are missing -> the first missing bar_start.

    Only a clean suffix of the session grid counts as truncation; an interior
    hole is a different defect (SESSION_BAR_COUNT) and is not reported here.
    """
    by_day: dict[date, set[datetime]] = {}
    for b in bars_15m:
        by_day.setdefault(b.bar_start.date(), set()).add(b.bar_start)
    out: dict[date, datetime] = {}
    for day, present in by_day.items():
        session = calendar.session(day)
        if session is None:
            continue
        grid = session.bar_starts()
        k = 0
        while k < len(grid) and grid[len(grid) - 1 - k] not in present:
            k += 1
        if k:
            out[day] = grid[len(grid) - k]
    return out


def aggregate(bars_15m: Sequence, timeframe: str, calendar: SessionCalendar,
              as_of: datetime | None = None,
              require_complete_constituents: bool = True,
              flag_truncated: bool = True) -> list[Bar]:
    """Aggregate 15-minute bars into `timeframe`.

    Deterministic and side-effect free. Bars outside a known session are
    ignored (they cannot be placed in a bucket without inventing a session).

    `as_of`: any bucket whose end is after `as_of` is emitted with
    ``candle_complete=False`` (the trailing, still-forming bucket).
    """
    if timeframe == "15m":
        return [bar_from_raw(b) for b in bars_15m]
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"unknown timeframe {timeframe!r}")

    normalised: list[Bar] = []
    for raw in bars_15m:
        bar = bar_from_raw(raw)
        session = calendar.session(bar.bar_start.date())
        if session is None or not session.contains(bar.bar_start):
            continue
        normalised.append(bar)
    normalised.sort(key=lambda b: b.bar_start)
    if not normalised:
        return []

    days = sorted({b.bar_start.date() for b in normalised})
    if timeframe == "1W":
        # a week bucket needs the whole Mon-Fri span of every touched week
        days = sorted({d for b in normalised
                       for d in _week_days(calendar, b.bar_start.date())})

    starts = [b.bar_start for b in normalised]
    cut = truncated_sessions(normalised, calendar) if flag_truncated else {}

    out: list[Bar] = []
    for start, end, expected in buckets_for(timeframe, calendar, days):
        lo = bisect_left(starts, start)
        hi = bisect_left(starts, end)
        group = normalised[lo:hi]
        if not group:
            continue  # no synthetic bars
        complete = len(group) == expected and len({b.bar_start for b in group}) == expected
        if require_complete_constituents:
            complete = complete and all(b.candle_complete for b in group)
        if as_of is not None and end > as_of:
            complete = False
        flags: set[str] = set()
        for b in group:
            flags.update(filter(None, b.quality_flags.split(",")))
        # a bucket that loses time to a truncated session is never "complete"
        if cut:
            if timeframe == "1W":
                hit = any(start.date() <= d <= end.date() for d in cut)
            else:
                point = cut.get(start.date())
                hit = point is not None and end > point
            if hit:
                flags.add(FLAG_SESSION_TRUNCATED)
                complete = False
        out.append(Bar(
            bar_start=start,
            bar_end=end,
            open=group[0].open,
            high=max(b.high for b in group),
            low=min(b.low for b in group),
            close=group[-1].close,
            volume=sum(b.volume for b in group),
            candle_complete=complete,
            quality_flags=",".join(sorted(flags)),
            constituents=len(group),
        ))
    out.sort(key=lambda b: b.bar_start)
    return out


def _week_days(calendar: SessionCalendar, day: date) -> list[date]:
    return [s.day for s in calendar.week_sessions(day)]


def aggregate_all(bars_15m: Sequence, calendar: SessionCalendar,
                  timeframes: Sequence[str] = ("1H", "4H", "1D", "1W"),
                  as_of: datetime | None = None) -> dict[str, list[Bar]]:
    return {tf: aggregate(bars_15m, tf, calendar, as_of=as_of)
            for tf in timeframes}


def daily_series(bars_15m: Sequence, calendar: SessionCalendar,
                 daily: "Mapping[date, Mapping[str, float]] | None" = None,
                 prefer_provider_daily: bool = True,
                 as_of: datetime | None = None) -> list[Bar]:
    """1D bars, taking the **provider's own daily bar** where the intraday
    series cannot carry the session's official close (contract 2A).

    Two cases use the provider's daily bar:

    1. **CAS session** -- from 2026-08-03 a cash stock with F&O contracts stops
       continuous trading at 15:15 and its official close is the auction
       equilibrium price struck at 15:30-15:35. That price is *not a bar in the
       intraday series at all*, and the ~1-3% of the day's volume that trades
       in the auction is not in the intraday rows either. The session is
       complete under its regime; the daily bar is simply the only place the
       official close exists.
    2. **A genuinely incomplete session** -- bars missing from the regime's own
       grid.

    Otherwise the intraday aggregate is kept as-is. Nothing is synthesised: if
    no provider daily bar exists we keep the intraday bar and its flags.
    """
    out: list[Bar] = []
    for bar in aggregate(bars_15m, "1D", calendar, as_of=as_of):
        session = calendar.session(bar.bar_start.date())
        is_cas = bool(session) and not session.official_close_in_series
        needs_provider = is_cas or not bar.candle_complete
        # never substitute a daily bar for a session that has not closed yet --
        # that would be look-ahead, not a repair.
        closed = as_of is None or bar.bar_end <= as_of
        if prefer_provider_daily and daily and needs_provider and closed:
            ref = daily.get(bar.bar_start.date())
            if ref:
                flags = set(filter(None, bar.quality_flags.split(",")))
                flags.add(FLAG_FROM_PROVIDER_DAILY)
                out.append(Bar(
                    bar.bar_start, bar.bar_end,
                    float(ref["open"]), float(ref["high"]), float(ref["low"]),
                    float(ref["close"]), int(ref.get("volume") or 0),
                    # the substituted content IS the provider's complete daily
                    # bar; the flags record that the intraday series was short.
                    candle_complete=True,
                    quality_flags=",".join(sorted(flags)),
                    constituents=bar.constituents))
                continue
        out.append(bar)
    return out


def weekly_from_daily(daily_bars: Sequence[Bar],
                      calendar: SessionCalendar) -> list[Bar]:
    """1W (Mon-Fri) built from a 1D series.

    Building the week from `daily_series` output rather than straight from 15m
    is what carries a CAS stock's official weekly close, which never appears in
    the intraday series.
    """
    weeks: dict[date, list[Bar]] = {}
    for b in sorted(daily_bars, key=lambda x: x.bar_start):
        d = b.bar_start.date()
        if d.weekday() >= 5:
            continue
        weeks.setdefault(d - timedelta(days=d.weekday()), []).append(b)

    out: list[Bar] = []
    for monday in sorted(weeks):
        group = weeks[monday]
        sessions = calendar.week_sessions(monday)
        expected = len(sessions)
        flags: set[str] = set()
        for b in group:
            flags.update(filter(None, b.quality_flags.split(",")))
        out.append(Bar(
            bar_start=group[0].bar_start,
            bar_end=group[-1].bar_end,
            open=group[0].open,
            high=max(b.high for b in group),
            low=min(b.low for b in group),
            close=group[-1].close,
            volume=sum(b.volume for b in group),
            candle_complete=(len(group) == expected
                             and all(b.candle_complete for b in group)),
            quality_flags=",".join(sorted(flags)),
            constituents=len(group)))
    return out


def aggregate_from_minutes(rows: Iterable, source_minutes: int, timeframe: str,
                           calendar: SessionCalendar,
                           as_of: datetime | None = None) -> list[Bar]:
    """1m/5m rows -> `timeframe`, via the 15-minute base. Used by the
    equivalence test that proves 15m aggregation is lossless."""
    base = to_15m(rows, source_minutes, calendar)
    if timeframe == "15m":
        return base
    return aggregate(base, timeframe, calendar, as_of=as_of)
