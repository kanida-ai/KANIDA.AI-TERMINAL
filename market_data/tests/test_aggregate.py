"""Tests for market_data/aggregate.py and market_data/calendar.py.

Includes two tests against *real* data (skipped if the files are absent):

* 15m built from 5-minute rows == 15m built from 1-minute rows (kanida.db,
  opened read-only) -- proves the 15-minute base is lossless;
* 4H (and 1H) aggregation reproduces the frozen research history under
  market_scanner/output/expanded_research/8ae6ddc251e80668239e/history/.
"""

from __future__ import annotations

import dataclasses
import gzip
import json
import random
import sqlite3
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data.aggregate import (  # noqa: E402
    Bar, aggregate, aggregate_all, daily_series, four_hour_buckets, hour_buckets,
    to_15m, truncated_sessions, weekly_from_daily)
from market_data.calendar import (  # noqa: E402
    REGIME_CAS, REGULAR_BAR_COUNT, RegimeBook, SessionCalendar, SessionRegime,
    regular_session)

LEGACY_DB = ROOT / "db" / "kanida.db"
FROZEN_HISTORY = (ROOT / "market_scanner" / "output" / "expanded_research" /
                  "8ae6ddc251e80668239e" / "history" /
                  "62f495579989a3f79360631344654cf1eb76dd0fa54fbaac84f43365b494a8d2"
                  ".json.gz")          # PIIND, per docs/pattern_research/PIIND_SOURCE_AUDIT.md
FROZEN_SYMBOL = "PIIND"

DAYS = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]
CAL = SessionCalendar([regular_session(d) for d in DAYS])


def make_session(day: date, base=100.0) -> list[Bar]:
    """25 synthetic 15m bars for one regular session."""
    session = regular_session(day)
    out = []
    for i, s in enumerate(session.bar_starts()):
        o = base + i
        out.append(Bar(s, s + timedelta(minutes=15), o, o + 2, o - 2, o + 1,
                       100 + i))
    return out


# ---------------------------------------------------------------------------
# calendar
# ---------------------------------------------------------------------------
def test_regular_session_is_25_bars():
    s = regular_session(date(2024, 1, 2))
    assert s.expected_bars == REGULAR_BAR_COUNT == 25
    starts = s.bar_starts()
    assert starts[0].strftime("%H:%M") == "09:15"
    assert starts[-1].strftime("%H:%M") == "15:15"
    assert len(starts) == 25


def test_holidays_are_derived_from_the_data():
    cal = SessionCalendar([regular_session(d) for d in
                           (date(2024, 1, 22), date(2024, 1, 23),
                            date(2024, 1, 25))])
    assert cal.is_session(date(2024, 1, 24)) is False
    assert cal.is_holiday(date(2024, 1, 24)) is True       # weekday, no bars
    assert cal.holidays() == [date(2024, 1, 24)]


def test_special_sessions_are_explicit_not_corruption():
    # Muhurat: an evening session derived from the observed bars.
    stamps = [datetime(2023, 11, 12, 18, 15) + timedelta(minutes=15 * i)
              for i in range(4)]
    cal = SessionCalendar.from_bar_times(stamps)
    s = cal.session(date(2023, 11, 12))
    assert s.kind == "muhurat" and s.expected_bars == 4
    assert s.start.strftime("%H:%M") == "18:15" and s.end.strftime("%H:%M") == "19:15"
    # Budget Saturday: regular hours on a weekend, labelled, 25 bars.
    cal2 = SessionCalendar.from_bar_times([datetime(2020, 2, 1, 9, 15)])
    s2 = cal2.session(date(2020, 2, 1))
    assert s2.kind == "special_weekend" and s2.expected_bars == 25
    assert "Budget" in s2.label


# ---------------------------------------------------------------------------
# bucket geometry
# ---------------------------------------------------------------------------
def test_hour_buckets_include_the_trailing_15_minute_bucket():
    hb = hour_buckets(regular_session(date(2024, 1, 2)))
    assert [(a.strftime("%H:%M"), b.strftime("%H:%M")) for a, b in hb] == [
        ("09:15", "10:15"), ("10:15", "11:15"), ("11:15", "12:15"),
        ("12:15", "13:15"), ("13:15", "14:15"), ("14:15", "15:15"),
        ("15:15", "15:30")]


def test_four_hour_buckets_match_the_frozen_research_boundaries():
    fb = four_hour_buckets(regular_session(date(2024, 1, 2)))
    assert [(a.strftime("%H:%M"), b.strftime("%H:%M")) for a, b in fb] == [
        ("09:15", "13:15"), ("13:15", "15:30")]


# ---------------------------------------------------------------------------
# aggregation rules
# ---------------------------------------------------------------------------
def test_ohlc_and_volume_rules():
    bars = make_session(DAYS[0])
    four = aggregate(bars, "4H", CAL)
    assert len(four) == 2
    first = four[0]
    assert first.constituents == 16 and first.candle_complete
    assert first.open == bars[0].open
    assert first.close == bars[15].close
    assert first.high == max(b.high for b in bars[:16])
    assert first.low == min(b.low for b in bars[:16])
    assert first.volume == sum(b.volume for b in bars[:16])
    second = four[1]
    assert second.constituents == 9 and second.candle_complete
    assert second.close == bars[-1].close

    one_h = aggregate(bars, "1H", CAL)
    assert len(one_h) == 7
    assert [b.constituents for b in one_h] == [4, 4, 4, 4, 4, 4, 1]
    assert all(b.candle_complete for b in one_h)

    day = aggregate(bars, "1D", CAL)
    assert len(day) == 1 and day[0].volume == sum(b.volume for b in bars)
    assert day[0].open == bars[0].open and day[0].close == bars[-1].close


def test_weekly_bucket_is_monday_to_friday():
    bars = [b for d in DAYS for b in make_session(d)]
    week = aggregate(bars, "1W", CAL)
    assert len(week) == 1
    w = week[0]
    assert w.bar_start == datetime(2024, 1, 2, 9, 15)      # Tue (Mon 1 Jan holiday)
    assert w.bar_end == datetime(2024, 1, 5, 15, 30)       # Fri close
    assert w.volume == sum(b.volume for b in bars)
    # only 4 of the 5 weekdays exist in this calendar -> the week is complete
    # because the calendar says Monday was not a session.
    assert w.candle_complete is True


def test_incomplete_and_trailing_buckets_are_flagged_not_dropped():
    bars = make_session(DAYS[0])
    partial = bars[:20]                       # session cut short at 14:15
    four = aggregate(partial, "4H", CAL)
    assert len(four) == 2                     # both buckets still emitted
    assert four[0].candle_complete is True
    assert four[1].candle_complete is False   # 4 of 9 constituents
    assert four[1].constituents == 4

    # as_of before the close -> the trailing bucket is still forming
    four_asof = aggregate(bars, "4H", CAL, as_of=datetime(2024, 1, 2, 14, 0))
    assert four_asof[0].candle_complete is True
    assert four_asof[1].candle_complete is False

    # an incomplete 15m constituent poisons its bucket's completeness
    poisoned = list(bars)
    poisoned[0] = dataclasses.replace(bars[0], candle_complete=False)
    assert aggregate(poisoned, "4H", CAL)[0].candle_complete is False


def test_truncated_session_flags_the_buckets_that_lose_time():
    """Kite intraday has stopped at 15:00 since 2026-08-03; the 15:15-15:30
    slice is missing. Buckets that lose time must be incomplete and flagged,
    and the missing bar must never be synthesised."""
    bars = make_session(DAYS[0])[:24]              # last bar_start 15:00
    assert truncated_sessions(bars, CAL) == {DAYS[0]: datetime(2024, 1, 2, 15, 15)}

    four = aggregate(bars, "4H", CAL)
    assert four[0].candle_complete is True and four[0].quality_flags == ""
    assert four[1].candle_complete is False
    assert four[1].quality_flags == "session_truncated"
    assert four[1].constituents == 8               # 9 expected, nothing invented

    one_h = aggregate(bars, "1H", CAL)
    assert len(one_h) == 6                         # the 15:15 stub is NOT emitted
    assert all(b.candle_complete for b in one_h[:5])
    assert one_h[5].candle_complete is True        # 14:15-15:15 is untouched

    day = aggregate(bars, "1D", CAL)[0]
    assert day.candle_complete is False and "session_truncated" in day.quality_flags
    assert day.volume == sum(b.volume for b in bars)


def test_daily_series_prefers_the_provider_daily_bar_when_truncated():
    bars = make_session(DAYS[0])[:24]
    provider = {DAYS[0]: {"open": 100.0, "high": 130.0, "low": 95.0,
                          "close": 128.0, "volume": 999_999}}
    plain = aggregate(bars, "1D", CAL)[0]
    assert plain.volume < provider[DAYS[0]]["volume"]
    d = daily_series(bars, CAL, provider)[0]
    assert d.volume == 999_999 and d.high == 130.0 and d.close == 128.0
    assert "from_provider_daily" in d.quality_flags
    assert "session_truncated" in d.quality_flags
    assert d.candle_complete is True
    # a complete session keeps the intraday aggregate
    full = daily_series(make_session(DAYS[0]), CAL, provider)[0]
    assert full.quality_flags == ""
    assert full.volume == sum(b.volume for b in make_session(DAYS[0]))
    # with no provider daily bar we keep the flagged, incomplete intraday bar
    kept = daily_series(bars, CAL, None)[0]
    assert kept.candle_complete is False and kept.volume == plain.volume


def test_no_synthetic_bars():
    """A day with no rows produces no bar; a missing hour produces no bar."""
    bars = make_session(DAYS[0]) + make_session(DAYS[2])
    day = aggregate(bars, "1D", CAL)
    assert [b.bar_start.date() for b in day] == [DAYS[0], DAYS[2]]
    hole_start = datetime(2024, 1, 2, 10, 15)
    hole_end = datetime(2024, 1, 2, 11, 15)
    holed = [b for b in make_session(DAYS[0])
             if not (hole_start <= b.bar_start < hole_end)]  # drop the whole bucket
    one_h = aggregate(holed, "1H", CAL)
    assert datetime(2024, 1, 2, 10, 15) not in [b.bar_start for b in one_h]
    assert len(one_h) == 6


def test_bars_outside_any_known_session_are_ignored_not_placed():
    bars = make_session(DAYS[0])
    stray = Bar(datetime(2024, 1, 6, 11, 0), datetime(2024, 1, 6, 11, 15),
                1, 2, 0.5, 1.5, 10)           # Saturday, no session
    assert len(aggregate(bars + [stray], "1D", CAL)) == 1


def test_aggregation_is_deterministic_and_order_independent():
    bars = [b for d in DAYS for b in make_session(d)]
    shuffled = bars[:]
    random.Random(7).shuffle(shuffled)
    for tf in ("1H", "4H", "1D", "1W"):
        assert aggregate(bars, tf, CAL) == aggregate(shuffled, tf, CAL)
        assert aggregate(bars, tf, CAL) == aggregate(bars, tf, CAL)
    assert set(aggregate_all(bars, CAL)) == {"1H", "4H", "1D", "1W"}


def test_to_15m_rejects_a_source_interval_that_does_not_divide_15():
    with pytest.raises(ValueError):
        to_15m([], 7, CAL)


# ---------------------------------------------------------------------------
# real-data proofs
# ---------------------------------------------------------------------------
def _ro(path):
    con = sqlite3.connect(f"file:{Path(path).as_posix()}?mode=ro", uri=True,
                          timeout=120)
    con.execute("PRAGMA query_only=ON")
    return con


def _rows(con, table, symbol, start, end):
    return con.execute(
        f"SELECT bar_time,open,high,low,close,volume FROM {table} "
        f"WHERE symbol=? AND bar_time>=? AND bar_time<? ORDER BY bar_time",
        (symbol, start, end)).fetchall()


@pytest.mark.skipif(not LEGACY_DB.exists(), reason="db/kanida.db not present")
@pytest.mark.parametrize("symbol,start,end", [
    ("RELIANCE", "2021-03-01", "2021-04-01"),
    ("PIIND", "2019-09-01", "2019-09-15"),
])
def test_15m_from_5m_equals_15m_from_1m_on_real_rows(symbol, start, end):
    """The 15-minute base reproduces 1-minute aggregation exactly.

    This is the contract's requirement that 1H/4H/1D/1W built from 15m have
    identical OHLC to the same window aggregated from 1-minute rows.
    """
    con = _ro(LEGACY_DB)
    try:
        five = _rows(con, "ohlc_5min", symbol, start, end)
        one = _rows(con, "ohlc_1min", symbol, start, end)
    finally:
        con.close()
    if not five or not one:
        pytest.skip(f"no legacy rows for {symbol} {start}..{end}")

    cal = SessionCalendar.from_bar_times(
        datetime.fromisoformat(r[0]) for r in one)
    from_5 = {b.bar_start: b for b in to_15m(five, 5, cal)}
    from_1 = {b.bar_start: b for b in to_15m(one, 1, cal)}
    assert from_5 and set(from_5) == set(from_1)
    for k in from_5:
        a, b = from_5[k], from_1[k]
        assert (a.open, a.high, a.low, a.close) == (b.open, b.high, b.low, b.close), k
        assert a.volume == b.volume, k

    for tf in ("1H", "4H", "1D"):
        agg5 = aggregate(list(from_5.values()), tf, cal)
        agg1 = aggregate(list(from_1.values()), tf, cal)
        assert [(x.bar_start, x.open, x.high, x.low, x.close, x.volume) for x in agg5] \
            == [(x.bar_start, x.open, x.high, x.low, x.close, x.volume) for x in agg1]


@pytest.mark.skipif(not (LEGACY_DB.exists() and FROZEN_HISTORY.exists()),
                    reason="kanida.db or the frozen research history is not present")
@pytest.mark.parametrize("timeframe", ["4H", "1H"])
def test_reproduces_the_frozen_research_history(timeframe):
    """Our 15m -> 4H/1H must equal the frozen research candles bar for bar.

    The frozen file is the PIIND history used by expanded run
    8ae6ddc251e80668239e (see docs/pattern_research/PIIND_SOURCE_AUDIT.md).
    Its 4H boundaries are 09:15-13:15 and 13:15-15:30; matching it is how we
    verify the bucket definition, including the defects it carries.
    """
    frozen = json.load(gzip.open(FROZEN_HISTORY, "rt"))[timeframe][0]
    con = _ro(LEGACY_DB)
    try:
        five = _rows(con, "ohlc_5min", FROZEN_SYMBOL, "2015-01-01", "2030-01-01")
    finally:
        con.close()
    cal = SessionCalendar.from_kanida_db(LEGACY_DB)
    mine = {b.bar_start.isoformat(sep=" "): b
            for b in aggregate(to_15m(five, 5, cal), timeframe, cal)
            if b.candle_complete}
    ref = {b["time"]: b for b in frozen}
    common = sorted(set(mine) & set(ref))
    assert len(common) >= 5000, f"only {len(common)} overlapping {timeframe} bars"
    assert len(common) == len(ref), "frozen bars missing from our aggregation"
    for k in common:
        r, m = ref[k], mine[k]
        assert (round(r["open"], 6), round(r["high"], 6), round(r["low"], 6),
                round(r["close"], 6), r["volume"]) == \
               (round(m.open, 6), round(m.high, 6), round(m.low, 6),
                round(m.close, 6), m.volume), k
        assert r["end"] == m.bar_end.isoformat(sep=" "), k

    # and the known-bad PIIND 4H candle is reproduced exactly, defect included
    if timeframe == "4H":
        bad = mine["2019-09-05 09:15:00"]
        assert (bad.open, bad.high, bad.low, bad.close, bad.volume) == \
            (1190.0, 1218.0, 65.35, 1213.0, 185490)


# ---------------------------------------------------------------------------
# session regimes -- NSE Closing Auction Session, contract 2A
# ---------------------------------------------------------------------------
CAS_DAYS = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5),
            date(2026, 8, 6), date(2026, 8, 7)]
CAS_CAL = SessionCalendar([regular_session(d) for d in CAS_DAYS])


def cas_book(symbol="ABB"):
    return RegimeBook([SessionRegime(symbol, date(2026, 8, 3), REGIME_CAS,
                                     "observed", "verified 2026-09-15")])


def make_cas_session(day: date, base=100.0) -> list[Bar]:
    """24 bars: continuous trading ends at 15:15 for a CAS stock."""
    session = regular_session(day).under_regime(REGIME_CAS)
    assert session.expected_bars == 24
    return [Bar(s, s + timedelta(minutes=15), base + i, base + i + 2,
                base + i - 2, base + i + 1, 100 + i)
            for i, s in enumerate(session.bar_starts())]


def test_cas_session_ends_at_1515_with_24_bars():
    s = regular_session(date(2026, 8, 4)).under_regime(REGIME_CAS)
    assert s.regime == REGIME_CAS and s.expected_bars == 24
    assert s.end.strftime("%H:%M") == "15:15"
    assert s.bar_starts()[-1].strftime("%H:%M") == "15:00"
    assert s.official_close_in_series is False
    # the regime only applies from the CAS start date
    before = regular_session(date(2026, 7, 31)).under_regime(REGIME_CAS)
    assert before.expected_bars == 25 and before.regime == "regular"
    # and it never reshapes a Muhurat/short session
    muh = SessionCalendar.from_bar_times(
        [datetime(2025, 10, 21, 13, 45)]).session(date(2025, 10, 21))
    assert muh.under_regime(REGIME_CAS) is muh


def test_cas_day_is_complete_not_truncated():
    """24 bars is the whole session for a CAS stock -- not a missing bar."""
    bars = make_cas_session(CAS_DAYS[0])
    cal = CAS_CAL.for_symbol("ABB", cas_book())
    assert truncated_sessions(bars, cal) == {}

    four = aggregate(bars, "4H", cal)
    assert len(four) == 2
    assert [(b.bar_start.strftime("%H:%M"), b.bar_end.strftime("%H:%M"))
            for b in four] == [("09:15", "13:15"), ("13:15", "15:15")]
    assert four[1].constituents == 8 and four[1].candle_complete is True
    assert four[1].quality_flags == ""

    one_h = aggregate(bars, "1H", cal)
    assert len(one_h) == 6 and all(b.candle_complete for b in one_h)
    assert aggregate(bars, "1D", cal)[0].candle_complete is True

    # the same 24 bars judged with the market-wide calendar look truncated --
    # which is exactly the false flag the regime prevents
    assert aggregate(bars, "1D", CAS_CAL)[0].candle_complete is False


def test_a_cas_day_missing_a_real_bar_is_still_incomplete():
    bars = make_cas_session(CAS_DAYS[0])[:-1]          # stops at 14:45
    cal = CAS_CAL.for_symbol("ABB", cas_book())
    assert truncated_sessions(bars, cal) == {CAS_DAYS[0]: datetime(2026, 8, 3, 15, 0)}
    assert aggregate(bars, "1D", cal)[0].candle_complete is False


def test_regime_is_derived_from_the_bars_not_from_is_fno():
    last = {d: datetime.combine(d, time(15, 0)) for d in CAS_DAYS}
    book = RegimeBook.from_observed_last_bars("ABB", last, CAS_CAL, is_fno=True)
    assert book.regime("ABB", date(2026, 8, 5)) == REGIME_CAS
    assert book.regime("ABB", date(2026, 7, 31)) == "regular"
    assert "is_fno=True" in book.entries()[0].evidence
    # a non-CAS stock keeps 15:15 last bars and stays regular, even if is_fno
    last_full = {d: datetime.combine(d, time(15, 15)) for d in CAS_DAYS}
    assert len(RegimeBook.from_observed_last_bars("ABDL", last_full, CAS_CAL,
                                                  is_fno=True)) == 0


def test_daily_and_weekly_take_the_official_close_from_the_daily_bar():
    """A CAS stock's official close is an auction price, not an intraday bar."""
    bars = [b for d in CAS_DAYS for b in make_cas_session(d)]
    cal = CAS_CAL.for_symbol("ABB", cas_book())
    provider = {d: {"open": 100.0, "high": 140.0, "low": 90.0,
                    "close": 131.0 + i, "volume": 500_000}
                for i, d in enumerate(CAS_DAYS)}
    d1 = daily_series(bars, cal, provider)
    assert len(d1) == 5
    assert all("from_provider_daily" in b.quality_flags for b in d1)
    assert all(b.candle_complete for b in d1)
    assert d1[-1].close == 135.0
    intraday_only = aggregate(bars, "1D", cal)
    assert intraday_only[-1].close != d1[-1].close
    assert intraday_only[-1].volume < d1[-1].volume      # the auction volume

    w = weekly_from_daily(d1, cal)
    assert len(w) == 1
    assert w[0].close == d1[-1].close and w[0].open == d1[0].open
    assert w[0].volume == sum(b.volume for b in d1)
    assert w[0].candle_complete is True
