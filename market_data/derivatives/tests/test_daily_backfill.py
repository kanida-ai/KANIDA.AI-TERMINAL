"""The front contract's OWN daily candles with OI (`candles_day`).

What is being protected here, in one line: a daily bar is the vendor's own bar
for THAT contract — not a stitched continuous series, not the underlying, and
not our roll-up of the 15-minute bars — and a session the vendor did not send
is recorded as absent rather than invented.
"""
from datetime import date, timedelta

import pytest

from market_data.derivatives import config
from market_data.derivatives.backfill import (
    TIMEFRAME,
    TIMEFRAME_DAY,
    Backfiller,
    futures_candidates,
    run_backfill,
    run_daily_backfill,
)
from market_data.derivatives.capture import DerivativesCapture
from market_data.derivatives.fake_nfo import FakeNFOClient, FakeNFOProvider
from market_data.derivatives.store import DerivativesStore

TODAY = date(2026, 9, 18)


def _capture(tmp_path, **client_kw):
    store = DerivativesStore(tmp_path / "derivatives.db")
    provider = FakeNFOProvider(FakeNFOClient(today=TODAY, **client_kw))
    capture = DerivativesCapture(store, provider, cache_dir=tmp_path / "cache")
    capture.ensure_scope(TODAY)
    return capture


@pytest.fixture
def cap(tmp_path):
    return _capture(tmp_path)


def _futures(cap):
    return {int(r["instrument_token"]): r["tradingsymbol"]
            for r in cap.store.con.execute(
                "SELECT instrument_token, tradingsymbol FROM contracts "
                "WHERE in_scope=1 AND instrument_type='FUT'")}


# ── the scope ────────────────────────────────────────────────────────────────

def test_only_in_scope_futures_are_asked_for(cap):
    picked = {c.token for c in futures_candidates(cap.store)}
    assert picked == set(_futures(cap))
    kinds = {r[0] for r in cap.store.con.execute(
        "SELECT instrument_type FROM contracts WHERE instrument_token IN "
        f"({','.join(str(t) for t in picked)})")}
    assert kinds == {"FUT"}, "the daily chart is a futures chart; options are not fetched"


def test_reading_the_scope_writes_nothing(cap):
    """The capture loop owns `contracts`; a daily fetch may not re-flag it underneath."""
    before = list(cap.store.con.execute(
        "SELECT instrument_token, in_scope, last_seen FROM contracts ORDER BY 1"))
    futures_candidates(cap.store)
    after = list(cap.store.con.execute(
        "SELECT instrument_token, in_scope, last_seen FROM contracts ORDER BY 1"))
    assert [tuple(r) for r in before] == [tuple(r) for r in after]


def test_underlyings_filter_narrows_the_fetch(cap):
    picked = {c.contract.underlying for c in futures_candidates(cap.store, underlyings=["nifty"])}
    assert picked == {"NIFTY"}


# ── the fetch ────────────────────────────────────────────────────────────────

def test_daily_bars_land_in_candles_day_keyed_by_the_session(cap):
    res = run_daily_backfill(cap, workers=2)
    assert res.errors == 0 and res.rows > 0
    assert cap.provider.kite.calls["historical_data"] == res.attempted == len(_futures(cap))
    rows = list(cap.store.con.execute(
        "SELECT * FROM candles_day ORDER BY instrument_token, session_date"))
    assert len(rows) == res.rows
    # keyed by the DAY, with no time on it: a daily bar did not open at midnight
    assert all(len(r["session_date"]) == 10 for r in rows)
    assert all(r["oi"] is not None for r in rows)
    assert all(r["vendor_id"] == "kite" and r["fetched_at"] for r in rows)
    # one bar per contract per session, never two
    pairs = [(r["instrument_token"], r["session_date"]) for r in rows]
    assert len(pairs) == len(set(pairs))


def test_the_daily_fetch_never_touches_the_15_minute_table(cap):
    run_daily_backfill(cap, workers=2)
    assert cap.store.candle_count() == 0, "candles_15m belongs to the 15-minute backfill"
    assert cap.store.daily_candle_count() > 0


def test_the_two_backfills_do_not_collide(cap):
    fifteen = run_backfill(cap, workers=2)
    daily = run_daily_backfill(cap, workers=2)
    assert fifteen.rows > 0 and daily.rows > 0
    assert cap.store.candle_count() == fifteen.rows
    assert cap.store.daily_candle_count() == daily.rows
    ledger = {(r[0], r[1]) for r in cap.store.con.execute(
        "SELECT timeframe, status FROM backfill_progress GROUP BY 1, 2")}
    assert (TIMEFRAME, "ok") in ledger and (TIMEFRAME_DAY, "ok") in ledger


def test_the_daily_fetch_is_resumable(cap):
    first = run_daily_backfill(cap, workers=2)
    calls = cap.provider.kite.calls["historical_data"]
    second = run_daily_backfill(cap, workers=2)
    assert second.skipped_done == first.candidates and second.attempted == 0
    assert cap.provider.kite.calls["historical_data"] == calls


def test_a_rerun_replaces_like_with_like(cap):
    first = run_daily_backfill(cap, workers=2)
    again = run_daily_backfill(cap, workers=2, resume=False)
    assert again.rows == first.rows
    assert cap.store.daily_candle_count() == first.rows, "a rerun may never double-count"


# ── the honesty rules ────────────────────────────────────────────────────────

def test_a_contract_with_no_history_is_recorded_empty_never_invented(tmp_path):
    """A future listed only today has one session, not sixty, and it says so."""
    cap = _capture(tmp_path)
    tokens = sorted(_futures(cap))
    silent, young = tokens[0], tokens[1]
    cap.provider.kite.listed_on = {silent: TODAY + timedelta(days=30),   # never traded yet
                                   young: TODAY - timedelta(days=2)}
    res = run_daily_backfill(cap, workers=2)
    assert res.empty == 1
    status = dict(cap.store.con.execute(
        "SELECT instrument_token, status FROM backfill_progress WHERE timeframe=?",
        (TIMEFRAME_DAY,)))
    assert status[silent] == "empty" and status[young] == "ok"
    assert cap.store.daily_candle_count(silent) == 0, "an empty answer is never filled in"
    assert cap.store.daily_candle_count(young) == 3   # Wed, Thu, Fri


def test_open_interest_the_vendor_did_not_send_stays_null(tmp_path):
    cap = _capture(tmp_path)
    blind = sorted(_futures(cap))[0]
    cap.provider.kite.no_oi_tokens = {blind}
    run_daily_backfill(cap, workers=2)
    ois = [r[0] for r in cap.store.con.execute(
        "SELECT oi FROM candles_day WHERE instrument_token=?", (blind,))]
    assert ois and all(o is None for o in ois), "unknown open interest is NULL, never 0"
    others = [r[0] for r in cap.store.con.execute(
        "SELECT oi FROM candles_day WHERE instrument_token<>?", (blind,))]
    assert others and all(o is not None for o in others)


def test_a_session_the_exchange_was_shut_is_simply_absent(tmp_path):
    """A day nothing traded is not fetched and not invented; it is just not there."""
    holiday = date(2026, 9, 16)
    cap = _capture(tmp_path, no_trade_days=[holiday])
    run_daily_backfill(cap, workers=2)
    days = {r[0] for r in cap.store.con.execute(
        "SELECT DISTINCT session_date FROM candles_day")}
    assert holiday.isoformat() not in days
    assert (holiday - timedelta(days=1)).isoformat() in days
    assert (holiday + timedelta(days=1)).isoformat() in days


def test_a_vendor_failure_is_an_error_row_not_a_silent_zero(tmp_path):
    cap = _capture(tmp_path)
    cap.provider.fail_times = 1
    res = run_daily_backfill(cap, workers=1)
    assert res.errors == 1
    rows = list(cap.store.con.execute(
        "SELECT status, rows, error FROM backfill_progress WHERE timeframe=? AND status='error'",
        (TIMEFRAME_DAY,)))
    assert len(rows) == 1 and rows[0]["rows"] == 0 and rows[0]["error"]
    # and the failed contract is NOT in the resume set, so the next run retries it
    assert cap.store.backfill_done(timeframe=TIMEFRAME_DAY,
                                   through_date=TODAY.isoformat()) != set(_futures(cap))


def test_the_window_reaches_back_past_a_contracts_first_session(cap):
    """One request per contract: Kite serves ~2000 days of daily history at a time."""
    bf = Backfiller(cap.store, cap.provider, workers=1, timeframe=TIMEFRAME_DAY)
    assert bf.write_rows == cap.store.write_daily_candles
    res = run_daily_backfill(cap, workers=1)
    assert res.requests == res.attempted
    assert (res.through_date - res.from_date).days == config.DAILY_CALENDAR_DAYS


# ── the chart reader over the same store ─────────────────────────────────────

def _front(cap, underlying="NIFTY"):
    from market_data.derivatives.read_api import front_future
    return front_future(cap.store.con, underlying, today=TODAY)


def test_the_chart_reads_the_front_contracts_own_daily_history(cap):
    from market_data.derivatives.read_api import futures_chart_series

    run_daily_backfill(cap, workers=2)
    out = futures_chart_series(cap.store.con, "NIFTY", interval="1d", today=TODAY)
    front = _front(cap)
    assert out["contract"]["instrument_token"] == front["instrument_token"]
    assert out["contract"]["expiry"] >= TODAY.isoformat(), "the front contract has not expired"
    assert out["sessions"] == out["bars"] > 0 and out["gaps"] == 0
    ats = [c["at"] for c in out["candles"]]
    assert ats == sorted(ats) and len(ats) == len(set(ats))
    assert all(len(a) == 10 for a in ats), "a daily bar is keyed by its day"
    assert out["session"] == ats[-1] and out["as_of"] == ats[-1]
    assert out["intervals"]["1d"]["available"] is True
    assert out["intervals"]["15m"]["available"] is False, "no 15-minute backfill has run here"


def test_a_session_this_contract_is_missing_keeps_its_slot(cap):
    """A day some OTHER contract traded, that this one has no bar for, is an empty slot."""
    from market_data.derivatives.read_api import futures_chart_series

    run_daily_backfill(cap, workers=2)
    front = int(_front(cap)["instrument_token"])
    days = [r[0] for r in cap.store.con.execute(
        "SELECT session_date FROM candles_day WHERE instrument_token=? ORDER BY 1", (front,))]
    hole = days[len(days) // 2]
    cap.store.write("DELETE FROM candles_day WHERE instrument_token=? AND session_date=?",
                    (front, hole))
    out = futures_chart_series(cap.store.con, "NIFTY", interval="1d", today=TODAY)
    ats = [c["at"] for c in out["candles"]]
    assert ats == days, "the slot stays, so the neighbours never become adjacent"
    slot = [c for c in out["candles"] if c["at"] == hole][0]
    assert slot == {"at": hole, "open": None, "high": None, "low": None, "close": None,
                    "volume": None, "oi": None, "gap": True}
    assert out["bars"] == len(days) - 1 and out["gaps"] == 1
    assert out["sessions"] == len(days) - 1, "an empty slot is not a session we have"


def test_a_day_no_contract_traded_is_not_a_gap(cap):
    """Weekends and holidays never enter the grid: the store knows of no session there."""
    from market_data.derivatives.read_api import futures_chart_series

    run_daily_backfill(cap, workers=2)
    out = futures_chart_series(cap.store.con, "NIFTY", interval="1d", today=TODAY)
    for candle in out["candles"]:
        assert date.fromisoformat(candle["at"]).weekday() < 5
    assert out["gaps"] == 0


def test_the_grid_cannot_invent_a_session_the_whole_store_is_missing(tmp_path):
    """The stated boundary: a day absent from EVERY contract reads as no session, not as a gap."""
    from market_data.derivatives.read_api import futures_chart_series

    holiday = date(2026, 9, 16)
    cap = _capture(tmp_path, no_trade_days=[holiday])
    run_daily_backfill(cap, workers=2)
    out = futures_chart_series(cap.store.con, "NIFTY", interval="1d", today=TODAY)
    assert holiday.isoformat() not in [c["at"] for c in out["candles"]]
    assert out["gaps"] == 0


def test_an_unknown_underlying_has_no_contract_and_no_candles(cap):
    from market_data.derivatives.read_api import futures_chart_series

    out = futures_chart_series(cap.store.con, "NOSUCH", interval="1d", today=TODAY)
    assert out["contract"] is None and out["candles"] == [] and out["sessions"] == 0


def test_a_bad_interval_is_refused_by_the_reader(cap):
    from market_data.derivatives.read_api import futures_chart_series

    with pytest.raises(ValueError):
        futures_chart_series(cap.store.con, "NIFTY", interval="1h", today=TODAY)
