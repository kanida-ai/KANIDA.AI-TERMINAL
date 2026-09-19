"""The 15-minute snapshot cycle: what is written, what is refused, and resume."""
import logging
import sqlite3
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
    """Provenance survived being normalised out of the row (2026-09-19).

    `snapshot_id` and `fetched_at` left `snapshots` -- 38 bytes on 27,238 rows a
    mark -- and live in `captures`, one row per mark.  The claim being tested is
    that this is a move and not a loss: every stored mark must still resolve to
    exactly one capture, and `vendor_id` must still be true on the row itself.
    """
    res = cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    assert res.status == "ok"
    rows = cap.store.read_snapshots(captured_at="2026-09-18 09:45:00")
    assert len(rows) == res.rows_written == len(cap.scope.contracts)
    row = rows[0]
    assert row["vendor_id"] == "kite"
    assert row["source"] == "kite.quote"
    assert row["mark_kind"] == "bar_close"
    assert row.keys() and "snapshot_id" not in row.keys()
    assert "fetched_at" not in row.keys()

    capture = cap.store.con.execute(
        "SELECT * FROM captures WHERE mark_at=?", (row["captured_at"],)).fetchone()
    assert capture is not None, "a stored mark with no capture row has no provenance"
    assert capture["snapshot_id"] == snapshot_id_for(MARK) == "cap_20260918T0945"
    assert capture["vendor_id"] == "kite"
    assert capture["started_at"] and capture["finished_at"]


def test_the_retired_columns_are_not_written_and_not_faked(cap):
    """The eleven columns retired on 2026-09-19 are gone from a new store.

    Gone, not NULL-filled: a column that still exists costs a byte a row, and
    the point of the exercise was the bytes.  Everything the Derivative tab and
    the metrics worker actually read is still here -- `last_trade_time` in
    particular, which STORAGE_PLAN.md listed as droppable and which the tab's IV
    staleness gate reads.
    """
    from market_data.derivatives.store import RETIRED_SNAPSHOT_COLUMNS

    cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    have = {r[1] for r in cap.store.con.execute("PRAGMA table_info(snapshots)")}
    assert have & RETIRED_SNAPSHOT_COLUMNS == set()
    assert have >= {"instrument_token", "captured_at", "mark_kind", "last_price",
                    "average_price", "volume", "oi", "day_open", "day_high",
                    "day_low", "prev_close", "last_trade_time", "source",
                    "vendor_id"}
    assert set(cap.store.snapshot_write_columns()) == have


def test_ohlc_lands_in_the_right_columns(cap):
    """The session OHLC fields, which the daily roll-up and the backfill read.

    This test used to assert the book columns too (`bid`, `ask`,
    `bid_quantity`).  They were retired on 2026-09-19: the capture only ever
    kept level zero of the depth, and no calculation anywhere consumed it.
    """
    cap.run_once(MARK, now=MARK + timedelta(seconds=30))
    row = cap.store.read_snapshots(captured_at="2026-09-18 09:45:00")[0]
    quote = cap.provider.kite.quote([row["instrument_token"]])[str(row["instrument_token"])]
    assert row["prev_close"] == pytest.approx(quote["ohlc"]["close"])
    assert row["day_open"] == pytest.approx(quote["ohlc"]["open"])
    assert row["day_high"] == pytest.approx(quote["ohlc"]["high"])
    assert row["day_low"] == pytest.approx(quote["ohlc"]["low"])


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


# ── retention runs from the loop ─────────────────────────────────────────────
#
# The gap this closes: `store.prune()` has existed since the store did, and
# nothing anywhere called it.  No scheduler, no task, no service — so the
# retention windows in `config` were a document, not a behaviour, and
# `candles_15m` grew without any bound at all.  Everything below runs against a
# throwaway store in tmp_path; nothing here opens db/derivatives.db.


def _old_and_new(cap, old_day, today):
    """A session far outside every window, plus one inside all of them."""
    from market_data.derivatives.store import SNAPSHOT_COLUMNS, CANDLE_COLUMNS

    def snap(token, at):
        v = dict(zip(SNAPSHOT_COLUMNS, [None] * len(SNAPSHOT_COLUMNS)))
        v.update(instrument_token=token, captured_at=at, mark_kind="bar_close",
                 last_price=10.0, volume=100, oi=200, source="kite.quote",
                 vendor_id="kite", fetched_at="x", snapshot_id="y")
        return tuple(v[c] for c in SNAPSHOT_COLUMNS)

    def candle(token, at):
        v = dict(zip(CANDLE_COLUMNS, [None] * len(CANDLE_COLUMNS)))
        v.update(instrument_token=token, bar_start=at, open=1.0, high=2.0, low=1.0,
                 close=2.0, volume=10, oi=20, vendor_id="kite", fetched_at="x")
        return tuple(v[c] for c in CANDLE_COLUMNS)

    cap.store.write_snapshots([snap(4242, f"{old_day} 09:30:00")])
    cap.store.write_candles([candle(4242, f"{old_day} 09:15:00")])
    cap.store.write_snapshots([snap(4242, f"{today} 09:30:00")])


def test_the_loop_prunes_once_the_day_has_no_mark_left(cap):
    """The whole point: nobody has to remember to run retention.

    A session from January is outside every window; today's mark is inside all
    of them.  The loop runs a full session and, when the day is finished, prunes
    of its own accord: the old day goes, today does not, and the day survives as
    a roll-up rather than vanishing.
    """
    old_day = "2026-01-02"
    _old_and_new(cap, old_day, TODAY.isoformat())
    assert cap.store.snapshot_count() == 2 and cap.store.candle_count(4242) == 1

    clock = _Clock(datetime(2026, 9, 18, 9, 29))
    run_loop(cap, clock, until=datetime(2026, 9, 18, 16, 30))

    assert cap._pruned_on == TODAY
    left = {r[0] for r in cap.store.con.execute(
        "SELECT DISTINCT substr(captured_at,1,10) FROM snapshots")}
    assert old_day not in left, "the January mark should have been pruned"
    assert TODAY.isoformat() in left, "today's marks must survive"
    assert cap.store.candle_count(4242) == 0
    rolled = cap.store.con.execute(
        "SELECT * FROM daily_rollups WHERE session_date=?", (old_day,)).fetchone()
    assert rolled is not None, "pruning must cost resolution, never the day"


def test_the_loop_prunes_once_a_day_and_not_once_a_cycle(cap):
    """26 marks a day must not mean 26 prunes a day."""
    calls = []
    real = cap.store.prune
    cap.store.prune = lambda **kw: (calls.append(kw), real(**kw))[1]

    _old_and_new(cap, "2026-01-02", TODAY.isoformat())
    clock = _Clock(datetime(2026, 9, 18, 9, 29))
    run_loop(cap, clock, until=datetime(2026, 9, 18, 18, 0))
    assert len(calls) == 1


def test_retention_can_be_switched_off(cap):
    cap.prune_enabled = False
    _old_and_new(cap, "2026-01-02", TODAY.isoformat())
    clock = _Clock(datetime(2026, 9, 18, 9, 29))
    run_loop(cap, clock, until=datetime(2026, 9, 18, 16, 30))
    assert cap.store.snapshot_count() == 2 + len(marks_for(TODAY)) * len(
        cap.scope.contracts)
    assert cap.store.candle_count(4242) == 1


def test_a_prune_that_raises_does_not_stop_the_capture_loop(cap):
    """Retention is housekeeping.  It must never be why capturing stops."""
    def boom(**_kw):
        raise sqlite3.OperationalError("database is locked")

    cap.store.prune = boom
    clock = _Clock(datetime(2026, 9, 18, 9, 29))
    results = run_loop(cap, clock, until=datetime(2026, 9, 18, 16, 30))
    assert len(results) == len(marks_for(TODAY))
    assert all(r.status == "ok" for r in results)


def test_a_prune_waits_for_the_writer_instead_of_failing(cap):
    """`store.transaction()` is BEGIN IMMEDIATE behind `retry_while_busy`.

    A capture mid-write means the prune's first BEGIN IMMEDIATE comes back BUSY.
    It must wait that out — a retention pass that dies on a busy store would
    silently stop pruning for the rest of the store's life, which is exactly how
    the windows came to be a document in the first place.
    """
    _old_and_new(cap, "2026-01-02", TODAY.isoformat())
    real = cap.store.con
    state = {"busy": 2}

    class BusyWriter:
        """The real connection, but the first two BEGIN IMMEDIATE come back BUSY."""

        def __getattr__(self, name):
            return getattr(real, name)

        def execute(self, sql, *a, **kw):
            if sql.startswith("BEGIN IMMEDIATE") and state["busy"] > 0:
                state["busy"] -= 1
                raise sqlite3.OperationalError("database is locked")
            return real.execute(sql, *a, **kw)

    cap.store.con = BusyWriter()
    try:
        out = cap.maybe_prune(datetime(2026, 9, 18, 16, 0), force=True)
    finally:
        cap.store.con = real

    assert state["busy"] == 0, "the writer lock was never actually contended"
    assert out is not None and out["snapshots_deleted"] == 1
    assert cap.store.snapshot_count() == 1


def test_the_prune_says_what_it_removed(cap, caplog):
    """Never silent: a removed session is logged with its row count."""
    _old_and_new(cap, "2026-01-02", TODAY.isoformat())
    with caplog.at_level(logging.INFO, logger="market_data.derivatives.store"):
        cap.maybe_prune(datetime(2026, 9, 18, 16, 0), force=True)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "2026-01-02" in text
    assert "snapshots" in text and "candles_15m" in text


def test_a_pass_that_removed_nothing_says_so_too(cap, caplog):
    """"Nothing to do" has to be visible, or a broken prune looks like a quiet one."""
    _old_and_new(cap, (TODAY - timedelta(days=3)).isoformat(), TODAY.isoformat())
    with caplog.at_level(logging.INFO, logger="market_data.derivatives.store"):
        out = cap.maybe_prune(datetime(2026, 9, 18, 16, 0), force=True)
    assert out["snapshots_deleted"] == 0 and out["candles_deleted"] == 0
    assert "store unchanged" in "\n".join(r.getMessage() for r in caplog.records)
