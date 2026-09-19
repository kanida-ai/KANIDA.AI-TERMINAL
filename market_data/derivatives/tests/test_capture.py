"""The 15-minute snapshot cycle: what is written, what is refused, and resume."""
from datetime import date, datetime, timedelta

import pytest

from market_data.derivatives import config
from market_data.derivatives.capture import (
    DerivativesCapture,
    SessionDays,
    marks_for,
    snapshot_id_for,
)
from market_data.derivatives.fake_nfo import FakeNFOClient, FakeNFOProvider
from market_data.derivatives.store import DerivativesStore

TODAY = date(2026, 9, 18)
MARK = datetime(2026, 9, 18, 9, 45)


class AlwaysSession(SessionDays):
    def is_session(self, day):
        return True


class _Clock:
    """A fake wall clock for the loop: `sleep` moves time instead of passing it."""

    def __init__(self, start):
        self.now = start

    def __call__(self):
        return self.now

    def sleep(self, _seconds, minutes=1):
        self.now += timedelta(minutes=minutes)


def run_loop(cap, clock, **kw):
    import market_data.derivatives.capture as capture_mod

    original = capture_mod.now_ist
    capture_mod.now_ist = clock
    try:
        return cap.run_forever(sleep=clock.sleep, **kw)
    finally:
        capture_mod.now_ist = original


@pytest.fixture
def cap(tmp_path):
    store = DerivativesStore(tmp_path / "derivatives.db")
    provider = FakeNFOProvider(FakeNFOClient(today=TODAY))
    capture = DerivativesCapture(store, provider, sessions=AlwaysSession(),
                                 cache_dir=tmp_path / "cache")
    capture.ensure_scope(TODAY)
    return capture


# ── the mark grid (session correctness) ──────────────────────────────────────

def test_marks_are_the_fifteen_minute_closes_plus_one_after_the_close():
    marks = marks_for(TODAY)
    times = [m.strftime("%H:%M") for m, _ in marks]
    assert times[0] == "09:30"
    assert times[-2] == "15:30"
    assert times[-1] == "15:45"
    assert marks[-1][1] == config.MARK_POST_CLOSE
    assert all(k == config.MARK_BAR_CLOSE for _, k in marks[:-1])
    # 09:30 … 15:30 inclusive = 25 bar closes, + the post-close mark
    assert len(marks) == 26
    gaps = {(b[0] - a[0]).total_seconds() for a, b in zip(marks, marks[1:])}
    assert gaps == {900.0}


def test_a_non_session_day_has_no_cycle(cap):
    class NoSession(SessionDays):
        def is_session(self, day):
            return False

    cap.sessions = NoSession()
    clock = _Clock(datetime(2026, 9, 18, 9, 29))
    results = run_loop(cap, clock, until=datetime(2026, 9, 18, 16, 0))
    assert results == []
    assert cap.store.snapshot_count() == 0
    assert cap.store.captures_for("2026-09-18") == {}


# ── one cycle ────────────────────────────────────────────────────────────────

def test_a_cycle_writes_one_row_per_contract_with_provenance(cap):
    res = cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    assert res.status == "ok"
    rows = cap.store.read_snapshots(captured_at="2026-09-18 09:45:00")
    assert len(rows) == res.rows_written == len(cap.scope.contracts)
    row = rows[0]
    assert row["vendor_id"] == "kite"
    assert row["snapshot_id"] == snapshot_id_for(MARK) == "cap_20260918T0945"
    assert row["fetched_at"]
    assert row["source"] == "kite.quote"
    assert row["mark_kind"] == "bar_close"


def test_depth_and_ohlc_land_in_the_right_columns(cap):
    cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    row = cap.store.read_snapshots(captured_at="2026-09-18 09:45:00")[0]
    quote = cap.provider.kite.quote([row["instrument_token"]])[str(row["instrument_token"])]
    assert row["bid"] == pytest.approx(quote["depth"]["buy"][0]["price"])
    assert row["ask"] == pytest.approx(quote["depth"]["sell"][0]["price"])
    assert row["bid_quantity"] == quote["depth"]["buy"][0]["quantity"]
    assert row["prev_close"] == pytest.approx(quote["ohlc"]["close"])
    assert row["day_open"] == pytest.approx(quote["ohlc"]["open"])


def test_volume_is_the_cumulative_day_volume_from_the_quote(cap):
    """Contract with D2: snapshots.volume is Kite's day-cumulative volume."""
    cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    row = cap.store.read_snapshots(captured_at="2026-09-18 09:45:00")[0]
    token = row["instrument_token"]
    quote = cap.provider.kite.quote([token])[str(token)]
    assert row["volume"] == quote["volume"]
    assert row["oi"] == quote["oi"]


def test_a_contract_the_vendor_did_not_answer_for_gets_no_row(cap):
    absent = cap.scope.contracts[0].instrument_token
    cap.provider.kite.missing_tokens = {absent}
    res = cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    tokens = {r["instrument_token"] for r in cap.store.read_snapshots()}
    assert absent not in tokens
    assert res.status == "partial"        # and it says so, rather than claiming ok


def test_a_contract_with_no_oi_no_volume_and_no_price_is_skipped_and_counted(cap):
    dead = cap.scope.contracts[1].instrument_token
    cap.provider.kite.dead_tokens = {dead}
    res = cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    assert res.rows_skipped == 1
    assert dead not in {r["instrument_token"] for r in cap.store.read_snapshots()}
    assert cap.store.capture(res.snapshot_id)["rows_skipped"] == 1


def test_quotes_are_batched_within_the_vendor_limit(cap):
    cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    assert cap.provider.kite.quote_batches
    assert max(cap.provider.kite.quote_batches) <= config.QUOTE_BATCH


def test_underlying_rows_carry_spot_and_the_sums_of_what_was_captured(cap):
    cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    rows = list(cap.store.con.execute(
        "SELECT * FROM underlying_snapshots WHERE captured_at='2026-09-18 09:45:00'"))
    assert {r["underlying"] for r in rows} == set(cap.scope.underlyings)
    for r in rows:
        assert r["spot"] is not None and r["spot_symbol"]
        assert r["fut_price"] is not None and r["fut_token"]
        assert r["ce_contracts"] and r["pe_contracts"]
        # the derived ratios belong to the metrics worker, not to capture
        assert r["pcr_oi"] is None and r["max_pain_strike"] is None
    snaps = cap.store.read_snapshots(captured_at="2026-09-18 09:45:00")
    by_token = cap.scope.by_token()
    for r in rows:
        ce = sum(s["oi"] for s in snaps
                 if by_token[s["instrument_token"]].underlying == r["underlying"]
                 and by_token[s["instrument_token"]].instrument_type == "CE")
        assert r["total_ce_oi"] == ce


# ── honesty about time ───────────────────────────────────────────────────────

def test_a_mark_past_its_grace_window_is_missed_not_back_filled(cap):
    late = MARK + timedelta(seconds=config.MARK_GRACE_SECONDS + 60)
    res = cap.run_once(now=late)
    assert res.status == "no_mark_due"
    caps = cap.store.captures_for("2026-09-18")
    assert caps["2026-09-18 09:30:00"]["status"] == "missed"
    assert caps["2026-09-18 09:45:00"]["status"] == "missed"
    assert cap.store.snapshot_count() == 0     # nothing invented for those marks


def test_a_mark_is_not_captured_before_it_happens(cap):
    capturable, missed = cap.due_marks(now=MARK - timedelta(minutes=1))
    assert MARK not in [m for m, _ in capturable + missed]


def test_the_newest_due_mark_is_the_one_captured(cap):
    """At 09:46 the 09:45 mark is fresh; the 09:30 one is 16 minutes stale."""
    now = datetime(2026, 9, 18, 9, 46)
    capturable, missed = cap.due_marks(now=now)
    assert [m.strftime("%H:%M") for m, _ in capturable] == ["09:45"]
    assert [m.strftime("%H:%M") for m, _ in missed] == ["09:30"]
    res = cap.run_once(now=now)
    assert res.mark == MARK
    assert cap.store.captures_for("2026-09-18")["2026-09-18 09:30:00"]["status"] == "missed"


# ── resume / crash safety ────────────────────────────────────────────────────

def test_an_already_captured_mark_is_not_re_quoted(cap):
    cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    calls = cap.provider.kite.calls["quote"]
    again = cap.run_once(MARK, now=MARK + timedelta(seconds=60))
    assert again.status == "already_done"
    assert cap.provider.kite.calls["quote"] == calls


def test_a_crashed_cycle_is_resumed_not_left_half_written(cap):
    sid = snapshot_id_for(MARK)
    cap.store.capture_begin(snapshot_id=sid, mark_at="2026-09-18 09:45:00",
                            mark_kind="bar_close", session_date="2026-09-18",
                            planned=len(cap.scope.contracts))
    assert cap.store.capture(sid)["status"] == "running"
    res = cap.run_once(MARK, now=MARK + timedelta(seconds=60))
    assert res.status == "ok"
    assert cap.store.capture(sid)["status"] == "ok"
    assert cap.store.snapshot_count(captured_at="2026-09-18 09:45:00") == res.rows_written


def test_re_capturing_the_same_mark_replaces_rather_than_duplicates(cap):
    first = cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    second = cap.run_once(MARK, now=MARK + timedelta(seconds=60), force=True)
    assert second.status == "ok"
    assert cap.store.snapshot_count() == first.rows_written


def test_a_vendor_failure_is_recorded_and_re_runnable(cap):
    cap.provider.fail_times = 1
    res = cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    assert res.status == "error" and "simulated" in res.error
    assert cap.store.capture(res.snapshot_id)["status"] == "error"
    assert cap.store.snapshot_count() == 0
    ok = cap.run_once(MARK, now=MARK + timedelta(seconds=60))
    assert ok.status == "ok" and ok.rows_written > 0


def test_the_loop_captures_every_mark_of_a_session(cap):
    real_marks = marks_for(TODAY)
    clock = _Clock(datetime(2026, 9, 18, 9, 29))
    run_loop(cap, clock, until=datetime(2026, 9, 18, 15, 50))

    caps = cap.store.captures_for("2026-09-18")
    assert len(caps) == len(real_marks)
    assert all(r["status"] == "ok" for r in caps.values())
    assert cap.store.snapshot_count() == len(real_marks) * len(cap.scope.contracts)
