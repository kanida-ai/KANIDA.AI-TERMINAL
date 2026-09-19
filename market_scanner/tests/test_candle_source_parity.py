"""`SCANNER_CANDLE_SOURCE=market15` must hand the detectors the same candles.

The detectors and the geometry they build are unchanged by this work, so the
only thing that can break them is the candle record itself.  These tests pin
that record: for one symbol over one pre-CAS window, the 1H / 4H / 1D candles
built from `db/market15.db` through `market_data.aggregate` are **identical**
-- field for field, including the `gap` flag -- to the ones the legacy
`db/kanida.db` path builds from `ohlc_5min` / `ohlc_daily`.

Post-2026-08-03 the two sources legitimately differ for an F&O stock, because
continuous trading ends at 15:15 and the close is struck in the 15:30-15:35
auction (contract 2A).  That difference is asserted explicitly rather than
waved away: the market15 source must call a 24-bar CAS session *complete*,
with a 14:15-15:15 final 1H bucket and a 13:15-15:15 final 4H bucket, where the
market-wide calendar would still be waiting for 15:15-15:30.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pytest

from market_data.aggregate import to_15m
from market_data.calendar import (
    CAS_START,
    REGIME_CAS,
    RegimeBook,
    SessionCalendar,
    SessionRegime,
    regular_session,
)
from market_data.store import MarketStore
from market_scanner.data import (
    Calendar,
    SymbolCalendarView,
    aggregate,
    aggregate_market15,
    candle_source,
    load_config,
    load_market15,
    load_rows,
)
from market_data.live.ingest import DAILY_TABLE, ensure_live_schema

ROOT = Path(__file__).resolve().parents[2]
KANIDA_DB = ROOT / "db" / "kanida.db"

# A pre-CAS window: every stock still traded 09:15-15:30, so the two sources
# must agree exactly.
SYMBOL = "RELIANCE"
WINDOW_START = "2026-04-01"
WINDOW_END = "2026-07-26"
CUTOFF = datetime(2026, 7, 25, 16, 0)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _legacy_rows(table: str, symbol: str):
    con = sqlite3.connect(f"file:{KANIDA_DB.as_posix()}?mode=ro", uri=True, timeout=60)
    try:
        con.execute("PRAGMA query_only=ON")
        return con.execute(
            f"SELECT bar_time,open,high,low,close,volume FROM {table} WHERE symbol=? "
            f"AND bar_time>=? AND bar_time<? ORDER BY bar_time",
            (symbol, WINDOW_START, WINDOW_END)).fetchall()
    finally:
        con.close()


@pytest.fixture(scope="module")
def sources(tmp_path_factory):
    """(legacy rows, a market15 store holding the same window) or a skip."""
    if not KANIDA_DB.exists():
        pytest.skip("db/kanida.db is not available")
    five = _legacy_rows("ohlc_5min", SYMBOL)
    daily = _legacy_rows("ohlc_daily", SYMBOL)
    if len(five) < 1000 or len(daily) < 40:
        pytest.skip(f"not enough legacy history for {SYMBOL} in the parity window")

    stamps = [datetime.fromisoformat(r[0]) for r in five]
    md_calendar = SessionCalendar.from_bar_times(stamps)

    store = MarketStore(tmp_path_factory.mktemp("m15") / "market15.db")
    ensure_live_schema(store)
    bars15 = to_15m(five, 5, md_calendar)
    store.upsert_candles(SYMBOL, 738561, bars15, vendor_id="parity-fixture",
                         adjustment_basis_id="legacy_unknown", revision=1)
    rows = []
    for bar_time, o, h, l, c, v in daily:
        day = datetime.fromisoformat(bar_time).date()
        session = md_calendar.session(day)
        if session is None:
            continue
        rows.append((738561, SYMBOL, "NSE", day.isoformat(),
                     session.start.isoformat(sep=" "), session.end.isoformat(sep=" "),
                     o, h, l, c, int(v or 0), "", "legacy_unknown", "parity-fixture",
                     None, None, 1))
    with store.transaction() as con:
        con.executemany(
            f"INSERT OR REPLACE INTO {DAILY_TABLE} (instrument_id,symbol,exchange,"
            f"session_date,bar_start,bar_end,open,high,low,close,volume,quality_flags,"
            f"adjustment_basis_id,vendor_id,fetched_at,run_id,revision) "
            f"VALUES ({','.join('?' * 17)})", rows)
    yield five, daily, store, md_calendar
    store.close()


_OBSERVED: list | None = None


def _scanner_calendar():
    """The scanner's own calendar, built exactly as `Scanner.__init__` builds it.

    The observed-session list matters: without it the config holiday list only
    covers `calendar_years` (2026), and every pre-2026 holiday reads as a
    trading day.
    """
    global _OBSERVED
    config = load_config()
    if _OBSERVED is None:
        con = sqlite3.connect(f"file:{KANIDA_DB.as_posix()}?mode=ro", uri=True, timeout=60)
        try:
            con.execute("PRAGMA query_only=ON")
            _OBSERVED = [r[0][:10] for r in con.execute(
                "SELECT bar_time FROM ohlc_daily WHERE symbol IN ('RELIANCE','AMRUTANJAN')")]
        finally:
            con.close()
    return Calendar(config, _OBSERVED), config


# ---------------------------------------------------------------------------
# the switch itself
# ---------------------------------------------------------------------------
def test_the_default_candle_source_is_legacy(monkeypatch):
    monkeypatch.delenv("SCANNER_CANDLE_SOURCE", raising=False)
    assert candle_source() == "legacy"
    assert candle_source({}) == "legacy"


def test_the_candle_source_switch_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("SCANNER_CANDLE_SOURCE", "market15")
    assert candle_source() == "market15"
    monkeypatch.setenv("SCANNER_CANDLE_SOURCE", "nonsense")
    with pytest.raises(ValueError):
        candle_source()


# ---------------------------------------------------------------------------
# pre-CAS parity
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("timeframe", ["1H", "4H", "1D"])
def test_market15_and_legacy_produce_identical_candles_before_cas(sources, timeframe):
    five, daily, store, md_calendar = sources
    calendar, config = _scanner_calendar()
    limit = config["history_bars"]

    legacy_rows = five if timeframe in ("1H", "4H") else daily
    legacy_bars, legacy_quality = aggregate(legacy_rows, timeframe, calendar, CUTOFF, limit)

    symbol_calendar = md_calendar.for_symbol(SYMBOL, RegimeBook())
    view = SymbolCalendarView(calendar, None)
    rows = load_market15(store, SYMBOL, timeframe in ("1H", "4H"), CUTOFF,
                         sessions=400, daily_bars=limit * 6)
    m15_bars, m15_quality = aggregate_market15(rows, timeframe, symbol_calendar, view,
                                               CUTOFF, limit)

    assert legacy_bars, "the legacy source produced no candles; the window is wrong"
    assert len(m15_bars) == len(legacy_bars)
    for left, right in zip(legacy_bars, m15_bars):
        assert left["time"] == right["time"]
        assert left["end"] == right["end"]
        for field in ("open", "high", "low", "close"):
            assert left[field] == pytest.approx(right[field], abs=1e-9), \
                f"{timeframe} {left['time']} {field}"
        assert int(left["volume"]) == int(right["volume"])
        assert left["gap"] == right["gap"]
    assert m15_quality["gaps"] == legacy_quality["gaps"]


def test_the_two_calendars_agree_on_which_days_are_sessions(sources):
    _, _, _, md_calendar = sources
    calendar, _ = _scanner_calendar()
    day, last = date.fromisoformat(WINDOW_START), date.fromisoformat(WINDOW_END)
    mismatches = []
    while day < last:
        legacy_open = calendar.session(day) is not None
        m15_open = md_calendar.session(day) is not None
        if legacy_open != m15_open:
            mismatches.append((day.isoformat(), legacy_open, m15_open))
        day += timedelta(days=1)
    assert mismatches == []


def test_the_market15_record_has_exactly_the_legacy_shape(sources):
    _, _, store, md_calendar = sources
    calendar, config = _scanner_calendar()
    bars, quality = aggregate_market15(
        load_market15(store, SYMBOL, True, CUTOFF, sessions=400), "1H",
        md_calendar.for_symbol(SYMBOL, RegimeBook()), SymbolCalendarView(calendar, None),
        CUTOFF, config["history_bars"])
    assert bars
    assert set(bars[0]) == {"time", "end", "open", "high", "low", "close", "volume", "gap"}
    assert set(quality) == {"invalid_rows", "incomplete_buckets", "gaps"}
    assert all(isinstance(b["gap"], bool) for b in bars)


# ---------------------------------------------------------------------------
# post-CAS: the documented difference (contract 2A)
# ---------------------------------------------------------------------------
@pytest.fixture()
def cas_store(tmp_path):
    """One F&O symbol, three post-CAS sessions of 24 bars ending 15:00-15:15."""
    days = [d for d in (CAS_START + timedelta(days=i) for i in range(5))
            if d.weekday() < 5][:3]
    md_calendar = SessionCalendar([regular_session(d) for d in days])
    store = MarketStore(tmp_path / "cas.db")
    ensure_live_schema(store)
    price = 100.0
    bars = []
    for d in days:
        stamp = datetime.combine(d, time(9, 15))
        while stamp < datetime.combine(d, time(15, 15)):   # 24 bars, CAS session
            price += 0.5
            bars.append((stamp.isoformat(sep=" "), price, price + 1, price - 1, price, 1000))
            stamp += timedelta(minutes=15)
    store.upsert_candles("CASCO", 4242, bars, vendor_id="parity-fixture",
                         adjustment_basis_id="test", revision=1)
    yield store, md_calendar, days
    store.close()


def test_a_cas_session_is_complete_under_its_own_regime(cas_store):
    store, md_calendar, days = cas_store
    calendar, config = _scanner_calendar()
    book = RegimeBook([SessionRegime("CASCO", days[0], REGIME_CAS, "observed")])
    cutoff = datetime.combine(days[-1], time(16, 0))

    rows = load_market15(store, "CASCO", True, cutoff, sessions=30)
    hourly, quality = aggregate_market15(
        rows, "1H", md_calendar.for_symbol("CASCO", book),
        SymbolCalendarView(calendar, days[0]), cutoff, config["history_bars"])

    last = hourly[-1]
    assert last["time"].endswith("14:15:00") and last["end"].endswith("15:15:00"), \
        "a CAS stock's last 1H bucket runs 14:15-15:15, not 15:15-15:30"
    assert quality["incomplete_buckets"] == 0, \
        "24 bars to 15:15 is a complete CAS session, not a truncated one"
    assert not any(b["gap"] for b in hourly[1:]), \
        "consecutive CAS sessions must not read as gapped"

    four_hour, _ = aggregate_market15(
        rows, "4H", md_calendar.for_symbol("CASCO", book),
        SymbolCalendarView(calendar, days[0]), cutoff, config["history_bars"])
    assert four_hour[-1]["time"].endswith("13:15:00")
    assert four_hour[-1]["end"].endswith("15:15:00")


def test_without_the_regime_a_cas_4h_closing_bucket_is_wrongly_dropped(cas_store):
    """The documented pre/post-CAS difference, stated as a test.

    Treat an F&O stock as a regular 15:30 session and every post-CAS closing 4H
    bucket is dropped as incomplete -- it is short the 15:15-15:30 bar that no
    longer exists.  That is exactly why the regime must be stored per symbol
    and per date (contract 2A) rather than assumed market-wide.

    1H is not affected: its 14:15-15:15 bucket happens to end on the CAS close,
    so 1H survives the misclassification by luck.  4H does not.
    """
    store, md_calendar, days = cas_store
    calendar, config = _scanner_calendar()
    cutoff = datetime.combine(days[-1], time(16, 0))
    rows = load_market15(store, "CASCO", True, cutoff, sessions=30)

    with_regime, ok_quality = aggregate_market15(
        rows, "4H", md_calendar.for_symbol(
            "CASCO", RegimeBook([SessionRegime("CASCO", days[0], REGIME_CAS)])),
        SymbolCalendarView(calendar, days[0]), cutoff, config["history_bars"])
    without_regime, bad_quality = aggregate_market15(
        rows, "4H", md_calendar.for_symbol("CASCO", RegimeBook()),
        SymbolCalendarView(calendar, None), cutoff, config["history_bars"])

    assert ok_quality["incomplete_buckets"] == 0
    assert with_regime[-1]["end"].endswith("15:15:00")
    assert len(with_regime) == len(without_regime) + len(days)
    assert bad_quality["incomplete_buckets"] == len(days)


def test_1d_comes_from_the_provider_daily_bar_not_the_intraday_close(cas_store):
    """Contract 2A: a CAS stock's official close is the 15:30-15:35 auction
    price, which is *not a bar in the intraday series*.  The market15 source
    must therefore take 1D from the provider's own daily bar; if it ever fell
    back to the last intraday close this test fails."""
    store, md_calendar, days = cas_store
    calendar, config = _scanner_calendar()
    cutoff = datetime.combine(days[-1], time(16, 0))
    intraday_close = store.read_bars("CASCO")[-1].close
    auction_close = intraday_close + 3.25            # struck after the last bar

    day = days[-1]
    session = md_calendar.session(day)
    with store.transaction() as con:
        con.execute(
            f"INSERT OR REPLACE INTO {DAILY_TABLE} (instrument_id,symbol,exchange,"
            f"session_date,bar_start,bar_end,open,high,low,close,volume,quality_flags,"
            f"adjustment_basis_id,vendor_id,revision) VALUES ({','.join('?' * 15)})",
            (4242, "CASCO", "NSE", day.isoformat(),
             session.start.isoformat(sep=" "), session.end.isoformat(sep=" "),
             100.0, auction_close + 1, 99.0, auction_close, 99999, "", "test",
             "parity-fixture", 1))

    daily_rows = load_market15(store, "CASCO", False, cutoff)
    bars, _ = aggregate_market15(
        daily_rows, "1D", md_calendar.for_symbol(
            "CASCO", RegimeBook([SessionRegime("CASCO", days[0], REGIME_CAS)])),
        SymbolCalendarView(calendar, days[0]), cutoff, config["history_bars"])

    assert bars and bars[-1]["time"].startswith(day.isoformat())
    assert bars[-1]["close"] == pytest.approx(auction_close)
    assert bars[-1]["close"] != pytest.approx(intraday_close)
    assert int(bars[-1]["volume"]) == 99999


# ---------------------------------------------------------------------------
# 1W: the one place the two calendars deliberately disagree
# ---------------------------------------------------------------------------
WEEKLY_START = "2023-01-01"

#: Weeks where the legacy scanner's week walks into the weekend or flattens a
#: Muhurat session, and `market_data` does not. Legacy's `week_sessions`
#: iterates all seven days, so a budget/DR Saturday ends its week; and its
#: `session()` returns a flat 09:15-15:30 for a Muhurat evening it has no entry
#: for. `market_data` keeps 1W strictly Mon-Fri (contract section 2) and uses
#: the observed Muhurat window. Listed, not waved away: switching the scanner
#: to market15 moves these week candles.
KNOWN_WEEKLY_DIFFERENCES = {
    "2023-11-06 09:15:00": ("2023-11-12 15:30:00", "2023-11-10 15:30:00"),
    "2024-01-15 09:15:00": ("2024-01-20 15:30:00", "2024-01-19 15:30:00"),
    "2024-02-26 09:15:00": ("2024-03-02 15:30:00", "2024-03-01 15:30:00"),
    "2024-05-13 09:15:00": ("2024-05-18 15:30:00", "2024-05-17 15:30:00"),
    "2024-10-28 09:15:00": ("2024-11-01 15:30:00", "2024-11-01 19:00:00"),
    "2025-01-27 09:15:00": ("2025-02-01 15:30:00", "2025-01-31 15:30:00"),
    "2026-01-27 09:15:00": ("2026-02-01 15:30:00", "2026-01-30 15:30:00"),
}


@pytest.fixture(scope="module")
def weekly_sources(tmp_path_factory):
    if not KANIDA_DB.exists():
        pytest.skip("db/kanida.db is not available")
    con = sqlite3.connect(f"file:{KANIDA_DB.as_posix()}?mode=ro", uri=True, timeout=60)
    try:
        con.execute("PRAGMA query_only=ON")
        daily = con.execute(
            "SELECT bar_time,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? "
            "AND bar_time>=? AND bar_time<? ORDER BY bar_time",
            (SYMBOL, WEEKLY_START, WINDOW_END)).fetchall()
        stamps = [datetime.fromisoformat(r[0]) for r in con.execute(
            "SELECT bar_time FROM ohlc_5min WHERE symbol=? AND bar_time>=? AND bar_time<? "
            "ORDER BY bar_time", (SYMBOL, WEEKLY_START, WINDOW_END))]
    finally:
        con.close()
    if len(daily) < 400 or len(stamps) < 20000:
        pytest.skip("not enough legacy history for the weekly window")

    md_calendar = SessionCalendar.from_bar_times(stamps)
    store = MarketStore(tmp_path_factory.mktemp("m15w") / "market15.db")
    ensure_live_schema(store)
    rows = []
    for bar_time, o, h, l, c, v in daily:
        day = datetime.fromisoformat(bar_time).date()
        session = md_calendar.session(day)
        if session is None:
            continue
        rows.append((738561, SYMBOL, "NSE", day.isoformat(),
                     session.start.isoformat(sep=" "), session.end.isoformat(sep=" "),
                     o, h, l, c, int(v or 0), "", "legacy_unknown", "parity-fixture",
                     None, None, 1))
    with store.transaction() as con2:
        con2.executemany(
            f"INSERT OR REPLACE INTO {DAILY_TABLE} (instrument_id,symbol,exchange,"
            f"session_date,bar_start,bar_end,open,high,low,close,volume,quality_flags,"
            f"adjustment_basis_id,vendor_id,fetched_at,run_id,revision) "
            f"VALUES ({','.join('?' * 17)})", rows)
    yield daily, store, md_calendar
    store.close()


def test_weekly_candles_differ_only_on_the_documented_special_weeks(weekly_sources):
    daily, store, md_calendar = weekly_sources
    calendar, config = _scanner_calendar()
    limit = config["history_bars"]

    legacy_bars, _ = aggregate(daily, "1W", calendar, CUTOFF, limit)
    m15_bars, m15_quality = aggregate_market15(
        load_market15(store, SYMBOL, False, CUTOFF, daily_bars=2000), "1W",
        md_calendar.for_symbol(SYMBOL, RegimeBook()), None, CUTOFF, limit)

    left = {b["time"]: b for b in legacy_bars}
    right = {b["time"]: b for b in m15_bars}
    assert set(left) == set(right), "the two sources must bucket the same weeks"
    assert len(left) > 150, "the weekly window is too short to be meaningful"

    differences = {t: (left[t]["end"], right[t]["end"])
                   for t in left if left[t]["end"] != right[t]["end"]}
    assert differences == KNOWN_WEEKLY_DIFFERENCES

    # everything else -- including the gap flag on every week -- matches
    assert [t for t in left if left[t]["gap"] != right[t]["gap"]] == []
    assert m15_quality["gaps"] == 0


def test_weekly_buckets_never_run_into_the_weekend(weekly_sources):
    """Contract section 2: 1W is Mon-Fri. A budget or DR Saturday is a session,
    but it is not the end of a week candle."""
    _, store, md_calendar = weekly_sources
    config = load_config()
    bars, _ = aggregate_market15(
        load_market15(store, SYMBOL, False, CUTOFF, daily_bars=2000), "1W",
        md_calendar.for_symbol(SYMBOL, RegimeBook()), None, CUTOFF,
        config["history_bars"])
    assert all(datetime.fromisoformat(b["end"]).weekday() < 5 for b in bars)
    assert all(datetime.fromisoformat(b["time"]).weekday() < 5 for b in bars)


# ---------------------------------------------------------------------------
# freshness + rescan trigger
# ---------------------------------------------------------------------------
def test_the_freshness_probe_advances_when_a_new_bar_lands(cas_store):
    """What the watcher polls. `market15_latest` must move the moment the live
    loop writes a newer completed bar -- that change is the rescan trigger."""
    from market_scanner.data import market15_latest

    store, md_calendar, days = cas_store
    before = market15_latest(store)
    newer = datetime.combine(days[-1] + timedelta(days=7), time(9, 15))
    store.upsert_candles("RELIANCE", 738561,
                         [(newer.isoformat(sep=" "), 1.0, 1.0, 1.0, 1.0, 1)],
                         vendor_id="parity-fixture", adjustment_basis_id="test",
                         revision=1)
    after = market15_latest(store)
    assert after == newer and after != before


def test_source_freshness_reports_the_newest_market15_bar_and_clears_the_stale_gate():
    """`/api/state` `source_latest`/`source_stale` under market15: the pilot's
    DATA_STALE gate reads the date out of `source_latest`, so it must be the
    newest completed bar, not the legacy daily table's last row."""
    from market_scanner.data import MARKET15, LEGACY
    from market_scanner.engine import Scanner

    class Stub:
        source_freshness = Scanner.source_freshness
        stocks = [{"daily_latest": "2026-07-29 00:00:00"}]

    current = Stub()
    current.candle_source = MARKET15
    current.market15_latest = datetime(2026, 9, 16, 15, 15)
    schedule = {"1D": {"latest_expected": "2026-09-16 15:30:00"}}
    assert current.source_freshness(schedule) == ("2026-09-16 15:15:00", False)

    behind = Stub()
    behind.candle_source = MARKET15
    behind.market15_latest = datetime(2026, 9, 10, 15, 15)
    assert behind.source_freshness(schedule) == ("2026-09-10 15:15:00", True)

    legacy = Stub()
    legacy.candle_source = LEGACY
    legacy.market15_latest = datetime(2026, 9, 16, 15, 15)
    assert legacy.source_freshness(schedule) == ("2026-07-29 00:00:00", True)
