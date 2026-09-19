"""Live ingest: planning, idempotency, restatement, daily bars, regimes.

Everything here runs against ``FakeProvider`` and a throwaway store -- no
network, no clock, no ``db/kanida.db``.  The point is that the *plan* is a pure
function of stored state, which is what makes the loop resumable and
crash-safe: kill it anywhere and the next cycle asks for exactly the bars that
are still missing.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from market_data.calendar import (
    CAS_START,
    REGIME_CAS,
    SessionCalendar,
    regular_session,
)
from market_data import quarantine as qmod
from market_data.fake_provider import FakeProvider
from market_data.live.calendar_ext import SymbolInfo, extend_calendar
from market_data.live.ingest import (
    DAILY_TABLE,
    LiveIngest,
    latest_completed_bar,
    latest_live_bar,
    plan_symbol,
    read_daily_bars,
)
from market_data.store import MarketStore

SYMBOLS = ("RELIANCE", "TCS")
BAR = timedelta(minutes=15)



def quarantined_stamp(hours_ago: float = 1.0) -> str:
    """A `last_checked` stamp that is genuinely `hours_ago` old.

    `store.quarantine()` stamps in **UTC** and `quarantine.due_for_recheck()`
    compares against UTC wall time, so a quarantine's age is the one thing in
    this module that is not a pure function of the fixture's fake `now`.
    Deriving the stamp from the fake clock made
    `test_a_quarantined_symbol_is_skipped...` pass on the day it was written and
    fail every day after: by 2026-09-19 the 2026-09-16 stamp was three days old,
    past `DEFAULT_RECHECK_HOURS = 24`, so the symbol came back as due-for-recheck
    instead of being skipped.  Anchor the stamp to real UTC instead, which is
    what the code under test actually reads.
    """
    return (datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
            - timedelta(hours=hours_ago)).isoformat(sep=" ")


def build_calendar(first=date(2026, 9, 1), last=date(2026, 9, 16)) -> SessionCalendar:
    days, day = [], first
    while day <= last:
        if day.weekday() < 5:
            days.append(day)
        day += timedelta(days=1)
    return SessionCalendar([regular_session(d) for d in days])


@pytest.fixture()
def rig(tmp_path):
    now = datetime(2026, 9, 16, 11, 7)
    calendar = build_calendar()
    provider = FakeProvider(symbols=SYMBOLS, now=now)
    store = MarketStore(tmp_path / "live.db")
    universe = [SymbolInfo("RELIANCE", 700000, True),
                SymbolInfo("TCS", 700001, False)]
    ingest = LiveIngest(store, provider, universe, calendar, backfill_sessions=3,
                        workers=2, clock=lambda: now)
    yield ingest, now
    store.close()


# ---------------------------------------------------------------------------
# planning
# ---------------------------------------------------------------------------
def test_plan_appends_from_the_bar_after_the_last_stored_one():
    calendar = build_calendar()
    last = datetime(2026, 9, 16, 10, 30)
    latest = datetime(2026, 9, 16, 10, 45)
    plan = plan_symbol("RELIANCE", 1, last, latest, calendar)
    assert plan.reason == "append"
    assert plan.start == last + BAR and plan.end == latest
    assert plan.wanted


def test_plan_is_a_no_op_when_the_store_is_current():
    calendar = build_calendar()
    latest = datetime(2026, 9, 16, 10, 45)
    plan = plan_symbol("RELIANCE", 1, latest, latest, calendar)
    assert plan.reason == "up_to_date" and not plan.wanted


def test_plan_never_asks_for_a_bar_ahead_of_the_last_completed_one():
    calendar = build_calendar()
    latest = datetime(2026, 9, 16, 10, 45)
    plan = plan_symbol("RELIANCE", 1, datetime(2026, 9, 16, 12, 0), latest, calendar)
    assert not plan.wanted


def test_plan_backfills_a_bounded_number_of_sessions_for_a_new_symbol():
    calendar = build_calendar()
    latest = datetime(2026, 9, 16, 10, 45)
    plan = plan_symbol("NEWCO", 1, None, latest, calendar, backfill_sessions=3)
    assert plan.reason == "backfill"
    # 16 Sep is a Wednesday: the third session back is Monday 14 Sep.
    assert plan.start == datetime(2026, 9, 14, 9, 15)
    assert plan.end == latest


def test_plan_does_nothing_without_a_completed_bar():
    plan = plan_symbol("RELIANCE", 1, None, None, build_calendar())
    assert plan.reason == "no_session" and not plan.wanted


def test_latest_completed_bar_lands_on_a_real_session_not_a_holiday():
    # 2026-09-16 is a session here; 2026-09-17 deliberately is not.
    calendar = SessionCalendar([regular_session(date(2026, 9, 16))])
    provider = FakeProvider(symbols=SYMBOLS)
    stamp = latest_completed_bar(provider, calendar, datetime(2026, 9, 17, 11, 0))
    assert stamp == datetime(2026, 9, 16, 15, 15)


def test_latest_completed_bar_honours_a_delayed_vendor():
    calendar = build_calendar()
    now = datetime(2026, 9, 16, 11, 7)
    prompt = FakeProvider(symbols=SYMBOLS, now=now)
    delayed = FakeProvider(symbols=SYMBOLS, now=now, delay_seconds=900)
    assert latest_completed_bar(prompt, calendar, now) == datetime(2026, 9, 16, 10, 45)
    assert latest_completed_bar(delayed, calendar, now) == datetime(2026, 9, 16, 10, 30)


# ---------------------------------------------------------------------------
# cycles
# ---------------------------------------------------------------------------
def test_a_cycle_writes_only_completed_bars_and_stops_at_the_latest_one(rig):
    ingest, now = rig
    result = ingest.run_cycle(now)
    assert result.errors == 0 and result.rows > 0
    rows = ingest.store.read_window("RELIANCE")
    assert rows, "expected bars for RELIANCE"
    assert all(r.candle_complete for r in rows)
    assert max(r.bar_start for r in rows) == datetime(2026, 9, 16, 10, 45)
    assert all(r.vendor_id == "fake" for r in rows)


def test_re_running_a_cycle_costs_nothing_and_writes_nothing(rig):
    ingest, now = rig
    first = ingest.run_cycle(now)
    before = ingest.store.con.execute("SELECT COUNT(*) FROM candles_15m").fetchone()[0]
    second = ingest.run_cycle(now)
    after = ingest.store.con.execute("SELECT COUNT(*) FROM candles_15m").fetchone()[0]
    assert first.rows > 0
    assert second.rows == 0 and second.requests == 0 and second.up_to_date == 2
    assert after == before


def test_a_crashed_cycle_resumes_from_what_the_store_holds(rig):
    ingest, now = rig
    # a partial first pass: only RELIANCE got written
    partial = [i for i in ingest.universe if i.symbol == "RELIANCE"]
    ingest.universe = partial
    ingest.run_cycle(now)
    reliance = ingest.store.con.execute(
        "SELECT COUNT(*) FROM candles_15m WHERE symbol='RELIANCE'").fetchone()[0]
    # restart with the full universe: RELIANCE is skipped, TCS is fetched
    ingest.universe = [SymbolInfo("RELIANCE", 700000, True), SymbolInfo("TCS", 700001, False)]
    result = ingest.run_cycle(now)
    assert result.up_to_date == 1 and result.considered == 1
    assert ingest.store.con.execute(
        "SELECT COUNT(*) FROM candles_15m WHERE symbol='RELIANCE'").fetchone()[0] == reliance
    assert ingest.store.con.execute(
        "SELECT COUNT(*) FROM candles_15m WHERE symbol='TCS'").fetchone()[0] > 0


def test_a_vendor_restatement_becomes_a_new_revision_plus_a_correction(rig):
    ingest, now = rig
    ingest.run_cycle(now)
    bar = ingest.store.read_window("RELIANCE")[-1]
    # rewrite one stored bar so the next fetch disagrees with it
    ingest.store.con.execute(
        "UPDATE candles_15m SET close=? WHERE symbol='RELIANCE' AND bar_start=?",
        (bar.close + 7.5, bar.bar_start.isoformat(sep=" ")))
    plan = plan_symbol("RELIANCE", 700000, bar.bar_start - BAR, bar.bar_start,
                       ingest.calendar)
    fetched = ingest.provider.candles("RELIANCE", "15minute", plan.start, plan.end)
    res = ingest._apply(plan, fetched, "test-run")
    assert res.restated == 1
    revisions = [r[0] for r in ingest.store.con.execute(
        "SELECT revision FROM candles_15m WHERE symbol='RELIANCE' AND bar_start=?",
        (bar.bar_start.isoformat(sep=" "),))]
    assert sorted(revisions) == [1, 2], "the old revision must still be there"
    assert ingest.store.read_window("RELIANCE", bar.bar_start,
                                    bar.bar_start + BAR)[0].close == pytest.approx(bar.close)
    corrections = ingest.store.con.execute(
        "SELECT field, old_value, new_value FROM corrections WHERE symbol='RELIANCE'").fetchall()
    assert [c[0] for c in corrections] == ["close"]


def test_the_daily_pass_stores_the_providers_own_daily_bars(rig):
    ingest, _ = rig
    after_close = datetime(2026, 9, 16, 16, 0)
    ingest.clock = lambda: after_close
    ingest.provider = FakeProvider(symbols=SYMBOLS, now=after_close)
    result = ingest.run_cycle(after_close)
    assert result.daily_rows > 0
    daily = read_daily_bars(ingest.store, "RELIANCE")
    assert daily and all(b.bar_start.time() == datetime.min.time().replace(hour=9, minute=15)
                         for b in daily)
    assert max(b.bar_start.date() for b in daily) == date(2026, 9, 16)
    # daily bars never land in the 15-minute table
    assert ingest.store.con.execute(
        "SELECT COUNT(*) FROM candles_15m WHERE bar_end LIKE '% 15:30:00' "
        "AND bar_start LIKE '% 09:15:00'").fetchone()[0] == 0


def test_the_daily_pass_is_skipped_while_the_session_is_open(rig):
    ingest, now = rig
    result = ingest.run_cycle(now)
    assert ingest.session_state(now)["phase"] == "in_session"
    assert result.daily_rows == 0


def test_freshness_meta_tracks_the_newest_completed_bar(rig):
    ingest, now = rig
    ingest.run_cycle(now)
    assert latest_live_bar(ingest.store, reference_symbols=("RELIANCE",)) == \
        datetime(2026, 9, 16, 10, 45)


def test_session_state_knows_pre_open_in_session_and_post_close(rig):
    ingest, _ = rig
    assert ingest.session_state(datetime(2026, 9, 16, 8, 0))["phase"] == "pre_open"
    assert ingest.session_state(datetime(2026, 9, 16, 12, 0))["phase"] == "in_session"
    assert ingest.session_state(datetime(2026, 9, 16, 16, 0))["phase"] == "post_close"
    assert ingest.session_state(datetime(2026, 9, 19, 12, 0))["phase"] == "closed"  # Saturday


# ---------------------------------------------------------------------------
# contract 2A: CAS regimes derived from the bars, never from a static list
# ---------------------------------------------------------------------------
def test_a_cas_regime_is_derived_from_the_observed_last_bars(tmp_path):
    days = [d for d in (CAS_START + timedelta(days=i) for i in range(12))
            if d.weekday() < 5]
    calendar = SessionCalendar([regular_session(d) for d in days])
    store = MarketStore(tmp_path / "regimes.db")
    rows = []
    for d in days:
        # F&O stock: continuous trading stops at 15:15, so the last bar starts 15:00
        for last in (datetime.combine(d, datetime.min.time().replace(hour=15, minute=0)),):
            rows.append(("CASCO", last))
        rows.append(("PLAINCO",
                     datetime.combine(d, datetime.min.time().replace(hour=15, minute=15))))
    for symbol, stamp in rows:
        store.con.execute(
            "INSERT INTO candles_15m (instrument_id,symbol,exchange,bar_start,bar_end,"
            "open,high,low,close,volume,candle_complete,quality_flags,"
            "adjustment_basis_id,vendor_id,revision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (1 if symbol == "CASCO" else 2, symbol, "NSE", stamp.isoformat(sep=" "),
             (stamp + BAR).isoformat(sep=" "), 1, 1, 1, 1, 1, 1, "", "x", "fake", 1))
    ingest = LiveIngest(store, FakeProvider(symbols=("CASCO",)),
                        [SymbolInfo("CASCO", 1, True), SymbolInfo("PLAINCO", 2, False)],
                        calendar)
    assert ingest._record_regimes(ingest.universe, "run") == 1
    book = store.regime_book()
    assert book.regime("CASCO", days[-1]) == REGIME_CAS
    assert book.regime("PLAINCO", days[-1]) == "regular"
    store.close()


# ---------------------------------------------------------------------------
# forward calendar
# ---------------------------------------------------------------------------
def test_the_calendar_is_projected_forward_but_never_rewrites_observed_days():
    base = SessionCalendar([regular_session(date(2026, 9, 14))])
    extended = extend_calendar(base, date(2026, 9, 21),
                               holidays=["2026-09-17"], closed=[], special={})
    assert extended.session(date(2026, 9, 14)) is not None       # observed, untouched
    assert extended.session(date(2026, 9, 16)) is not None       # projected weekday
    assert extended.session(date(2026, 9, 17)) is None           # projected holiday
    assert extended.session(date(2026, 9, 19)) is None           # Saturday
    assert extended.session(date(2026, 9, 13)) is None           # never backfilled
    assert extended.last_day == date(2026, 9, 21)


# ---------------------------------------------------------------------------
# quarantine: a symbol the provider cannot serve must cost 0 errors, not 1/cycle
# ---------------------------------------------------------------------------
GONE = SymbolInfo("LTIM", 700099, True)


@pytest.fixture()
def rig_with_gone(tmp_path):
    """RELIANCE + TCS the provider knows, LTIM it does not."""
    now = datetime(2026, 9, 16, 11, 7)
    provider = FakeProvider(symbols=SYMBOLS, now=now)
    store = MarketStore(tmp_path / "live.db")
    universe = [SymbolInfo("RELIANCE", 700000, True),
                SymbolInfo("TCS", 700001, False), GONE]
    ingest = LiveIngest(store, provider, universe, build_calendar(),
                        backfill_sessions=3, workers=2, clock=lambda: now)
    yield ingest, now
    store.close()


def test_an_unservable_symbol_costs_one_error_per_cycle_until_it_is_quarantined(rig_with_gone):
    """The behaviour being fixed: `errors` is what `/api/state` warns on."""
    ingest, now = rig_with_gone
    result = ingest.run_cycle(now)
    assert result.errors == 1
    assert [r.symbol for r in result.per_symbol if r.status == "error"] == ["LTIM"]


def test_a_quarantined_symbol_is_skipped_and_the_cycle_reports_zero_errors(rig_with_gone):
    ingest, now = rig_with_gone
    ingest.store.quarantine("LTIM", reason=qmod.NOT_IN_INSTRUMENT_LIST,
                            detail="not in the provider's instrument list",
                            when=quarantined_stamp(1))
    result = ingest.run_cycle(now)
    assert result.errors == 0
    assert result.quarantined_skipped == 1
    assert result.symbols == 2
    assert "LTIM" not in {r.symbol for r in result.per_symbol}


def test_the_skip_does_not_change_coverage_of_the_other_symbols(rig_with_gone):
    """Same bars as a universe that never had the bad symbol in it."""
    ingest, now = rig_with_gone
    ingest.store.quarantine("LTIM", reason=qmod.NOT_IN_INSTRUMENT_LIST,
                            when=quarantined_stamp(1))
    result = ingest.run_cycle(now)
    counts = dict(ingest.store.con.execute(
        "SELECT symbol, COUNT(*) FROM candles_15m GROUP BY symbol"))
    assert set(counts) == {"RELIANCE", "TCS"}
    assert result.rows == sum(counts.values())


def test_include_quarantined_puts_it_back_in(rig_with_gone):
    ingest, now = rig_with_gone
    ingest.store.quarantine("LTIM", reason=qmod.NOT_IN_INSTRUMENT_LIST,
                            when=quarantined_stamp(1))
    ingest.include_quarantined = True
    result = ingest.run_cycle(now)
    assert result.quarantined_skipped == 0 and result.errors == 1


def test_a_quarantine_due_a_recheck_is_probed_and_its_failure_is_not_an_error(rig_with_gone):
    ingest, now = rig_with_gone
    ingest.store.quarantine("LTIM", reason=qmod.NOT_IN_INSTRUMENT_LIST,
                            when="2026-09-01 00:00:00")
    result = ingest.run_cycle(now)
    assert result.quarantine_rechecked == 1
    assert result.quarantine_still_unavailable == 1
    assert result.errors == 0, "a confirmed quarantine is not a cycle failure"
    row = ingest.store.get_quarantine("LTIM")
    assert row["status"] == "quarantined" and row["checks"] == 2
    assert row["last_error"]


def test_a_symbol_the_provider_serves_again_is_released_on_the_daily_recheck(rig_with_gone):
    ingest, now = rig_with_gone
    ingest.store.quarantine("TCS", reason=qmod.NOT_IN_INSTRUMENT_LIST,
                            when="2026-09-01 00:00:00")
    result = ingest.run_cycle(now)
    assert result.quarantine_released == 1
    # LTIM is still un-quarantined here, so it still counts; TCS does not.
    assert [r.symbol for r in result.per_symbol if r.status == "error"] == ["LTIM"]
    assert ingest.store.get_quarantine("TCS")["status"] == "released"
    assert ingest.store.con.execute(
        "SELECT COUNT(*) FROM candles_15m WHERE symbol='TCS'").fetchone()[0] > 0


def test_the_daily_pass_also_skips_a_quarantined_symbol(rig_with_gone):
    ingest, _ = rig_with_gone
    after_close = datetime(2026, 9, 16, 16, 0)
    ingest.clock = lambda: after_close
    ingest.provider = FakeProvider(symbols=SYMBOLS, now=after_close)
    ingest.store.quarantine("LTIM", reason=qmod.NOT_IN_INSTRUMENT_LIST,
                            when=(after_close - timedelta(hours=1)).isoformat(sep=" "))
    ingest.run_cycle(after_close)
    assert ingest.store.con.execute(
        "SELECT COUNT(*) FROM daily_bars WHERE symbol='LTIM'").fetchone()[0] == 0


def test_a_store_with_no_quarantine_table_still_ingests(rig_with_gone):
    """The skip must never be the reason the loop stops working."""
    ingest, now = rig_with_gone
    ingest.store.con.execute("DROP TABLE quarantine")
    assert ingest.effective_universe() == (list(ingest.universe), set(), 0)
    assert ingest.run_cycle(now).rows > 0


def test_a_recheck_that_asked_for_nothing_still_advances_last_checked(rig_with_gone):
    """Otherwise the symbol is re-included on every cycle instead of once a day."""
    ingest, now = rig_with_gone
    ingest.run_cycle(now)                      # bring TCS fully up to date
    ingest.store.quarantine("TCS", reason=qmod.NOT_IN_INSTRUMENT_LIST,
                            when="2026-09-01 00:00:00")
    result = ingest.run_cycle(now)
    assert result.quarantine_rechecked == 1
    assert result.quarantine_released == 0     # nothing was fetched, so nothing is proven
    row = ingest.store.get_quarantine("TCS")
    assert row["status"] == "quarantined"
    assert row["last_checked"] > "2026-09-01 00:00:00"
    assert "not probed" in row["last_error"]


def test_a_cycle_survives_another_writer_holding_the_store(tmp_path):
    """The exact failure of 2026-09-16: `start_run` raised "database is locked"
    while a maintenance pass held the write lock, and the loop died."""
    import sqlite3
    import threading

    path = tmp_path / "live.db"
    now = datetime(2026, 9, 16, 11, 7)
    store = MarketStore(path, timeout=0.05, write_deadline=30.0)
    ingest = LiveIngest(store, FakeProvider(symbols=SYMBOLS, now=now),
                        [SymbolInfo("RELIANCE", 700000, True)],
                        build_calendar(), backfill_sessions=3, workers=1,
                        clock=lambda: now)
    ready, done = threading.Event(), threading.Event()

    def hold():
        con = sqlite3.connect(str(path), timeout=30, isolation_level=None)
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("BEGIN IMMEDIATE")
            con.execute("CREATE TABLE IF NOT EXISTS _blocker (x INTEGER)")
            ready.set()
            done.wait(30)
            con.execute("COMMIT")
        finally:
            con.close()

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert ready.wait(10)
    try:
        threading.Timer(1.5, done.set).start()
        result = ingest.run_cycle(now)          # this used to raise, not return
        assert result.errors == 0 and result.rows > 0
        assert result.busy_waits >= 1, "the wait must be reported, not hidden"
    finally:
        done.set()
        holder.join(timeout=10)
        store.close()
