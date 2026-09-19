"""Tests for market_data/validate.py.

The regression test at the bottom is the point of the whole module: the real
PIIND 2019-09-04..06 rows from db/kanida.db (read-only) MUST be flagged. The
old `market_scanner/data.py` checks passed them, which is how a fabricated
50.9% short return entered the frozen research.
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data import validate as V  # noqa: E402
from market_data.aggregate import Bar, to_15m  # noqa: E402
from market_data.calendar import (  # noqa: E402
    REGIME_CAS, RegimeBook, SessionCalendar, SessionRegime, regular_session)

LEGACY_DB = ROOT / "db" / "kanida.db"
DAY = date(2024, 1, 2)
CAL = SessionCalendar([regular_session(DAY)])


def session_bars(base=1000.0, n=25) -> list[Bar]:
    s = regular_session(DAY)
    out = []
    for i, t in enumerate(s.bar_starts()[:n]):
        p = base + i
        out.append(Bar(t, t + timedelta(minutes=15), p, p + 2, p - 2, p + 1, 500))
    return out


def codes(findings) -> set[str]:
    return {f.code for f in findings}


# ---------------------------------------------------------------------------
# contract item 3: ordering / positivity
# ---------------------------------------------------------------------------
def test_ordering_and_positivity():
    bars = session_bars()
    t = bars[0].bar_start
    bad_order = Bar(t, t + timedelta(minutes=15), 100, 99, 101, 100, 10)   # h<o, l>o
    negative = Bar(t, t + timedelta(minutes=15), -1, 2, -3, 1, 10)
    nan = Bar(t, t + timedelta(minutes=15), float("nan"), 1, 1, 1, 10)
    assert "OHLC_ORDER" in codes(V.check_ordering("X", [bad_order]))
    assert "NON_POSITIVE" in codes(V.check_ordering("X", [negative]))
    assert "NON_FINITE" in codes(V.check_ordering("X", [nan]))
    assert V.check_ordering("X", bars) == []


def test_clean_session_produces_no_findings():
    assert V.validate_symbol("X", session_bars(), CAL) == []


# ---------------------------------------------------------------------------
# contract item 1: intrabucket discontinuity  (the PIIND-shaped defect)
# ---------------------------------------------------------------------------
def test_intrabucket_discontinuity_catches_an_isolated_price_break():
    bars = session_bars(base=1192.0)
    t = bars[10].bar_start
    bars[10] = Bar(t, t + timedelta(minutes=15), 1193.9, 1193.9, 65.35, 1192.0, 3280)
    found = V.check_intrabucket_discontinuity("X", bars)
    assert [f.bar_start for f in found] == [t.isoformat(sep=" ")]
    f = found[0]
    assert f.code == "INTRABUCKET_DISCONTINUITY" and f.severity == "error"
    assert f.evidence["field"] == "low" and f.evidence["value"] == 65.35
    assert f.evidence["deviation"] > 0.9
    assert f.flag == "intrabucket_discontinuity"


def test_a_normal_volatile_day_is_not_flagged():
    """A genuine +/-10% intraday swing must not trip the discontinuity check."""
    s = regular_session(DAY)
    bars = []
    for i, t in enumerate(s.bar_starts()):
        p = 100.0 * (1 + 0.09 * (i - 12) / 12)     # -9% .. +9% around the median
        bars.append(Bar(t, t + timedelta(minutes=15), p, p * 1.005, p * 0.995, p, 500))
    assert V.check_intrabucket_discontinuity("X", bars) == []


# ---------------------------------------------------------------------------
# contract item 2: intraday vs daily reconciliation
# ---------------------------------------------------------------------------
def test_intraday_vs_daily_bounds():
    bars = session_bars(base=1192.0)
    t = bars[5].bar_start
    bars[5] = Bar(t, t + timedelta(minutes=15), 1192, 1192, 65.35, 1192, 100)
    t2 = bars[6].bar_start
    bars[6] = Bar(t2, t2 + timedelta(minutes=15), 1192, 5000.0, 1191, 1192, 100)
    daily = {DAY: {"open": 1190.0, "high": 1218.0, "low": 1183.1, "close": 1199.1,
                   "volume": 229598}}
    found = V.check_intraday_vs_daily("X", bars, daily)
    assert codes(found) == {"INTRADAY_BELOW_DAILY_LOW", "INTRADAY_ABOVE_DAILY_HIGH"}
    low = [f for f in found if f.code == "INTRADAY_BELOW_DAILY_LOW"][0]
    assert low.evidence["daily_low"] == 1183.1 and low.evidence["intraday_low"] == 65.35
    assert low.flag == "intraday_vs_daily"
    assert V.check_intraday_vs_daily("X", session_bars(base=1192.0), daily) == []


def test_missing_daily_reference_is_reported_not_ignored():
    found = V.check_intraday_vs_daily("X", session_bars(), {})
    assert codes(found) == {"DAILY_RECONCILE_MISSING"}
    assert found[0].severity == "warn"


# ---------------------------------------------------------------------------
# contract item 4: session length, duplicates, zero-volume prints
# ---------------------------------------------------------------------------
def test_session_bar_count_and_off_grid():
    holed = session_bars()[:8] + session_bars()[13:]     # interior hole, not a tail
    found = V.check_session_bars("X", holed, CAL)
    assert "SESSION_BAR_COUNT" in codes(found)
    ev = [f for f in found if f.code == "SESSION_BAR_COUNT"][0].evidence
    assert ev["present"] == 20 and ev["expected"] == 25

    off = datetime(2024, 1, 2, 9, 22)
    stray = Bar(off, off + timedelta(minutes=15), 1, 2, 0.5, 1.5, 1)
    assert "OFF_GRID_BAR" in codes(V.check_session_bars("X", session_bars() + [stray], CAL))

    outside = datetime(2024, 1, 2, 16, 15)
    late = Bar(outside, outside + timedelta(minutes=15), 1, 2, 0.5, 1.5, 1)
    assert "BAR_OUTSIDE_SESSION" in codes(
        V.check_session_bars("X", session_bars() + [late], CAL))

    holiday = datetime(2024, 1, 3, 9, 15)
    ghost = Bar(holiday, holiday + timedelta(minutes=15), 1, 2, 0.5, 1.5, 1)
    assert "UNKNOWN_SESSION_DAY" in codes(V.check_session_bars("X", [ghost], CAL))


def test_muhurat_session_is_not_reported_as_a_short_session():
    stamps = [datetime(2023, 11, 12, 18, 15) + timedelta(minutes=15 * i)
              for i in range(4)]
    cal = SessionCalendar.from_bar_times(stamps)
    bars = [Bar(s, s + timedelta(minutes=15), 100, 101, 99, 100, 10) for s in stamps]
    assert V.check_session_bars("X", bars, cal) == []


def test_duplicates():
    bars = session_bars()
    dup_same = bars[3]
    t = bars[4].bar_start
    dup_diff = Bar(t, t + timedelta(minutes=15), 1, 2, 0.5, 1.5, 9)
    found = V.check_duplicates("X", bars + [dup_same, dup_diff])
    assert [f.code for f in found] == ["DUPLICATE_BAR", "DUPLICATE_BAR"]
    assert {f.severity for f in found} == {"warn", "error"}


def test_zero_volume_print_with_a_price_move():
    bars = session_bars()
    t = bars[2].bar_start
    bars[2] = Bar(t, t + timedelta(minutes=15), 1000, 1010, 990, 1005, 0)
    found = V.check_zero_volume_moves("X", bars)
    assert [f.code for f in found] == ["ZERO_VOLUME_PRICE_MOVE"]
    flat = Bar(t, t + timedelta(minutes=15), 1000, 1000, 1000, 1000, 0)
    assert V.check_zero_volume_moves("X", [flat]) == []


# ---------------------------------------------------------------------------
# contract item 5: cross-timeframe agreement
# ---------------------------------------------------------------------------
def _daily_from(bars, **over):
    d = {"open": bars[0].open, "high": max(b.high for b in bars),
         "low": min(b.low for b in bars), "close": bars[-1].close,
         "volume": sum(b.volume for b in bars)}
    d.update(over)
    return {DAY: d}


def test_cross_timeframe_compares_range_containment_and_volume():
    bars = session_bars(base=1192.0)
    assert V.check_cross_timeframe("X", bars, _daily_from(bars), CAL) == []
    t = bars[9].bar_start
    bars[9] = Bar(t, t + timedelta(minutes=15), 1192, 1192, 65.35, 1192, 500)
    found = V.check_cross_timeframe("X", bars, _daily_from(session_bars(base=1192.0)), CAL)
    assert [f.code for f in found] == ["CROSS_TF_RANGE_BREAK"]
    assert found[0].evidence["side"] == "low" and found[0].severity == "error"


def test_cross_timeframe_close_difference_is_information_not_an_error():
    """NSE's daily close is the last-30-minute VWAP, not the last trade."""
    bars = session_bars(base=1305.0)
    # a normal last-30-min-VWAP difference is not reported at all
    assert V.check_cross_timeframe(
        "X", bars, _daily_from(bars, close=bars[-1].close * 1.0021), CAL) == []
    # a large one is reported, but only as information
    daily = _daily_from(bars, close=bars[-1].close * 1.015)     # +1.5%
    found = V.check_cross_timeframe("X", bars, daily, CAL)
    assert [f.code for f in found] == ["CROSS_TF_CLOSE_DIFF"]
    assert found[0].severity == "info"
    # and it must not flag any 15-minute row
    assert all(b.quality_flags == "" for b in V.apply_quality_flags(bars, found))


def test_cross_timeframe_volume_gap_is_skipped_on_a_truncated_session():
    full = session_bars(base=1000.0)
    daily = _daily_from(full)
    truncated = full[:24]                       # data stops at 15:00
    found = V.check_cross_timeframe("X", truncated, daily, CAL)
    assert "CROSS_TF_VOLUME_GAP" not in codes(found)
    # while a real volume gap on a complete session is reported
    found2 = V.check_cross_timeframe("X", full, _daily_from(full, volume=99999), CAL)
    assert "CROSS_TF_VOLUME_GAP" in codes(found2)


def test_truncated_session_is_reported_as_truncation_not_a_hole():
    full = session_bars()
    found = V.check_session_bars("X", full[:24], CAL)
    assert [f.code for f in found] == ["SESSION_TRUNCATED"]
    ev = found[0].evidence
    assert ev["missing_tail_bars"] == 1 and ev["truncated_from"].endswith("15:15:00")
    # an interior hole is still SESSION_BAR_COUNT
    holed = full[:10] + full[11:]
    assert [f.code for f in V.check_session_bars("X", holed, CAL)] == ["SESSION_BAR_COUNT"]


# ---------------------------------------------------------------------------
# flag application: never drop, never invent
# ---------------------------------------------------------------------------
def test_apply_quality_flags_keeps_every_row():
    bars = session_bars(base=1192.0)
    t = bars[10].bar_start
    bars[10] = Bar(t, t + timedelta(minutes=15), 1193.9, 1193.9, 65.35, 1192.0, 3280)
    daily = {DAY: {"open": 1190.0, "high": 1218.0, "low": 1183.1, "close": 1199.1,
                   "volume": 1}}
    findings = V.validate_symbol("X", bars, CAL, daily)
    flagged = V.apply_quality_flags(bars, findings)
    assert len(flagged) == len(bars)
    assert [b.bar_start for b in flagged] == [b.bar_start for b in bars]
    assert [b.low for b in flagged] == [b.low for b in bars]      # values untouched
    assert flagged[10].quality_flags != ""
    assert "intrabucket_discontinuity" in flagged[10].quality_flags
    assert "intraday_vs_daily" in flagged[10].quality_flags
    assert sum(1 for b in flagged if b.quality_flags) == 1
    s = V.summarise(findings)
    assert s["worst_severity"] == "error" and s["total"] >= 2


# ---------------------------------------------------------------------------
# REGRESSION: the real PIIND rows must be flagged
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not LEGACY_DB.exists(), reason="db/kanida.db not present")
def test_real_piind_2019_09_04_to_06_is_flagged():
    con = sqlite3.connect(f"file:{LEGACY_DB.as_posix()}?mode=ro", uri=True,
                          timeout=120)
    con.execute("PRAGMA query_only=ON")
    try:
        five = con.execute(
            "SELECT bar_time,open,high,low,close,volume FROM ohlc_5min "
            "WHERE symbol='PIIND' AND bar_time>=? AND bar_time<? ORDER BY bar_time",
            ("2019-09-04 00:00:00", "2019-09-07 00:00:00")).fetchall()
        daily_rows = con.execute(
            "SELECT bar_time,open,high,low,close,volume FROM ohlc_daily "
            "WHERE symbol='PIIND' AND bar_time>=? AND bar_time<? ORDER BY bar_time",
            ("2019-09-04", "2019-09-07")).fetchall()
    finally:
        con.close()
    if not five or not daily_rows:
        pytest.skip("PIIND legacy rows are not present in this database")
    assert len(five) == 225, f"expected the audited 225 five-minute rows, got {len(five)}"

    cal = SessionCalendar.from_kanida_db(LEGACY_DB)
    bars = to_15m(five, 5, cal)
    daily = {datetime.fromisoformat(r[0]).date():
             {"open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]}
             for r in daily_rows}
    # the audit's numbers, straight from the source
    assert daily[date(2019, 9, 5)]["low"] == 1183.1
    assert min(b.low for b in bars) == 65.35

    findings = V.validate_symbol("PIIND", bars, cal, daily)
    found = codes(findings)
    # the three checks market_scanner/data.py did not have
    assert "INTRABUCKET_DISCONTINUITY" in found
    assert "INTRADAY_BELOW_DAILY_LOW" in found
    assert "CROSS_TF_RANGE_BREAK" in found
    assert V.summarise(findings)["worst_severity"] == "error"
    assert [f.severity for f in findings if f.code == "CROSS_TF_RANGE_BREAK"] \
        == ["error", "error"]

    by_bar = {}
    for f in findings:
        by_bar.setdefault(f.bar_start, set()).add(f.code)
    # 2019-09-05 11:15 holds the 65.35 print (1-minute row at 11:22)
    assert "INTRABUCKET_DISCONTINUITY" in by_bar["2019-09-05 11:15:00"]
    assert "INTRADAY_BELOW_DAILY_LOW" in by_bar["2019-09-05 11:15:00"]
    # 2019-09-05 11:00 holds the 65.85 print (1-minute row at 11:04)
    assert "INTRADAY_BELOW_DAILY_LOW" in by_bar["2019-09-05 11:00:00"]
    # the previous session's 66.70 print (1-minute row at 10:51) too
    assert "INTRABUCKET_DISCONTINUITY" in by_bar["2019-09-04 10:45:00"]
    # 6 September is clean in the source and must stay clean
    assert not [k for k in by_bar if k.startswith("2019-09-06")]

    flagged = V.apply_quality_flags(bars, findings)
    assert len(flagged) == len(bars)                      # nothing dropped
    assert min(b.low for b in flagged) == 65.35           # nothing rewritten
    bad = [b for b in flagged if b.bar_start == datetime(2019, 9, 5, 11, 15)][0]
    assert "intrabucket_discontinuity" in bad.quality_flags
    assert "intraday_vs_daily" in bad.quality_flags


# ---------------------------------------------------------------------------
# session regimes -- NSE Closing Auction Session, contract 2A
# ---------------------------------------------------------------------------
CAS_DAY = date(2026, 8, 4)
CAS_CAL = SessionCalendar([regular_session(CAS_DAY)])
CAS_BOOK = RegimeBook([SessionRegime("ABB", date(2026, 8, 3), REGIME_CAS,
                                     "observed", "verified 2026-09-15")])


def cas_bars(base=1000.0, n=24):
    s = regular_session(CAS_DAY).under_regime(REGIME_CAS)
    return [Bar(t, t + timedelta(minutes=15), base + i, base + i + 2,
                base + i - 2, base + i + 1, 500)
            for i, t in enumerate(s.bar_starts()[:n])]


def test_cas_24_bar_day_is_not_a_short_session():
    cal = CAS_CAL.for_symbol("ABB", CAS_BOOK)
    assert V.check_session_bars("ABB", cas_bars(), cal) == []
    # without the regime the same day looks truncated -- the false flag the
    # contract 2A warns about
    assert [f.code for f in V.check_session_bars("ABB", cas_bars(), CAS_CAL)] \
        == ["SESSION_TRUNCATED"]
    # a bar genuinely missing from the CAS grid is still reported
    assert [f.code for f in V.check_session_bars("ABB", cas_bars(n=23), cal)] \
        == ["SESSION_TRUNCATED"]


def test_validating_past_the_cas_start_without_a_regime_says_so():
    findings = V.validate_symbol("ABB", cas_bars(), CAS_CAL)
    assert "REGIME_NOT_APPLIED" in codes(findings)
    cal = CAS_CAL.for_symbol("ABB", CAS_BOOK)
    assert "REGIME_NOT_APPLIED" not in codes(V.validate_symbol("ABB", cas_bars(), cal))


def test_cas_close_difference_is_not_reported_at_all():
    """A CAS stock's official close is an auction price that is not in the
    intraday series, so comparing it with the last intraday close is
    meaningless -- not even an info finding."""
    bars = cas_bars()
    cal = CAS_CAL.for_symbol("ABB", CAS_BOOK)
    daily = {CAS_DAY: {"open": bars[0].open,
                       "high": max(b.high for b in bars),
                       "low": min(b.low for b in bars),
                       "close": bars[-1].close * 1.03,     # auction price
                       "volume": int(sum(b.volume for b in bars) * 1.02)}}
    found = V.check_cross_timeframe("ABB", bars, daily, cal)
    assert "CROSS_TF_CLOSE_DIFF" not in codes(found)
    # ~2% of the day's volume trading in the auction is not a defect either
    assert "CROSS_TF_VOLUME_GAP" not in codes(found)
    assert V.validate_symbol("ABB", bars, cal, daily) == []
