"""The store's guarantees: idempotence, provenance, roll-ups, retention."""
from datetime import date, datetime, timedelta

import pytest

from market_data.derivatives import config
from market_data.derivatives.store import (
    CANDLE_COLUMNS,
    SNAPSHOT_COLUMNS,
    DerivativesStore,
    open_readonly,
)
from market_data.derivatives.capture import DerivativesCapture, SessionDays
from market_data.derivatives.fake_nfo import FakeNFOClient, FakeNFOProvider

TODAY = date(2026, 9, 18)


@pytest.fixture
def store(tmp_path):
    return DerivativesStore(tmp_path / "derivatives.db")


def _snapshot_row(token, captured_at, **over):
    values = dict(zip(SNAPSHOT_COLUMNS, [None] * len(SNAPSHOT_COLUMNS)))
    values.update(instrument_token=token, captured_at=captured_at,
                  mark_kind="bar_close", last_price=100.0, average_price=99.0,
                  volume=1000, oi=5000, source="kite.quote", vendor_id="kite",
                  # still supplied: `write_snapshots` drops what the live table
                  # no longer has, and keeps these on a pre-2026-09-19 file
                  # where they are NOT NULL.
                  fetched_at="2026-09-18T04:00:00", snapshot_id="cap_x")
    values.update(over)
    return tuple(values[c] for c in SNAPSHOT_COLUMNS)


def _candle_row(token, bar_start, **over):
    values = dict(zip(CANDLE_COLUMNS, [None] * len(CANDLE_COLUMNS)))
    values.update(instrument_token=token, bar_start=bar_start, open=10.0, high=12.0,
                  low=9.0, close=11.0, volume=100, oi=500, vendor_id="kite",
                  fetched_at="2026-09-18T04:00:00", snapshot_id="bf_x")
    values.update(over)
    return tuple(values[c] for c in CANDLE_COLUMNS)


# ── schema ───────────────────────────────────────────────────────────────────

def test_every_table_the_spec_names_exists(store):
    names = {r[0] for r in store.con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"contracts", "snapshots", "candles_15m", "underlying_snapshots",
            "metrics", "daily_rollups", "captures", "backfill_progress",
            "runs", "meta"} <= names


def test_the_indexes_the_metrics_worker_reads_by_exist(store):
    """Agreed with D2: these are the lookups the §3 signals make."""
    idx = {r[0]: r[1] for r in store.con.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='index'")}
    plans = [
        "SELECT * FROM snapshots WHERE captured_at='x'",
        "SELECT * FROM snapshots WHERE instrument_token=1 AND captured_at='x'",
        "SELECT * FROM candles_15m WHERE instrument_token=1 AND bar_start='x'",
        "SELECT * FROM contracts WHERE underlying='NIFTY'",
    ]
    for sql in plans:
        plan = " ".join(str(r[-1]) for r in store.con.execute("EXPLAIN QUERY PLAN " + sql))
        assert "SCAN" not in plan.upper() or "USING" in plan.upper(), (sql, plan)


def test_provenance_columns_are_not_nullable_where_it_matters(store):
    info = {r[1]: r for r in store.con.execute("PRAGMA table_info(snapshots)")}
    for col in ("vendor_id", "source"):
        assert info[col]["notnull"] == 1, col


def test_a_marks_provenance_is_recoverable_from_captures(store):
    """`fetched_at` and `snapshot_id` left the row on 2026-09-19.

    They were 38 bytes on every one of 27,238 rows a mark.  They are not gone:
    `captures` carries them once per mark, and `captured_at = mark_at` is an
    exact join because a mark has exactly one capture.  Assert the join, not the
    column -- the guarantee is that provenance is reachable, not that it is
    repeated 27,238 times.
    """
    mark = "2026-09-18 09:30:00"
    store.capture_begin(snapshot_id="cap_20260918T0930", mark_at=mark,
                        mark_kind="bar_close", session_date="2026-09-18",
                        planned=1)
    store.write_snapshots([_snapshot_row(1, mark)])
    row = store.read_snapshots()[0]
    assert "snapshot_id" not in row.keys() and "fetched_at" not in row.keys()
    capture = store.con.execute("SELECT * FROM captures WHERE mark_at=?",
                                (row["captured_at"],)).fetchone()
    assert capture["snapshot_id"] == "cap_20260918T0930"
    assert capture["started_at"]


# ── idempotence ──────────────────────────────────────────────────────────────

def test_the_same_mark_twice_updates_instead_of_duplicating(store):
    store.write_snapshots([_snapshot_row(1, "2026-09-18 09:30:00", last_price=10.0)])
    store.write_snapshots([_snapshot_row(1, "2026-09-18 09:30:00", last_price=11.0)])
    rows = store.read_snapshots()
    assert len(rows) == 1 and rows[0]["last_price"] == 11.0


def test_candles_are_keyed_by_contract_and_bar(store):
    store.write_candles([_candle_row(1, "2026-09-17 09:15:00", close=1.0)])
    store.write_candles([_candle_row(1, "2026-09-17 09:15:00", close=2.0),
                         _candle_row(1, "2026-09-17 09:30:00", close=3.0)])
    assert store.candle_count(1) == 2


def test_a_contract_that_leaves_the_dump_keeps_its_row_but_leaves_scope(tmp_path):
    store = DerivativesStore(tmp_path / "d.db")
    provider = FakeNFOProvider(FakeNFOClient(today=TODAY))
    cap = DerivativesCapture(store, provider, cache_dir=tmp_path / "c")
    scope = cap.ensure_scope(TODAY)
    before = store.contract_count()
    assert store.contract_count(in_scope=True) == len(scope.contracts)

    # a month later the front expiry has gone
    later = TODAY + timedelta(days=40)
    cap.ensure_scope(later, refresh=True)
    assert store.contract_count() == before          # nothing was deleted
    kept = {int(r["instrument_token"]) for r in store.scope_rows()}
    assert kept and kept != set(scope.tokens)


# ── the run ledger ───────────────────────────────────────────────────────────

def test_a_missed_mark_is_recorded_and_never_overwritten_by_a_later_guess(store):
    store.record_missed(snapshot_id="cap_1", mark_at="2026-09-18 09:30:00",
                        mark_kind="bar_close", session_date="2026-09-18",
                        reason="loop was down")
    store.record_missed(snapshot_id="cap_1", mark_at="2026-09-18 09:30:00",
                        mark_kind="bar_close", session_date="2026-09-18",
                        reason="something else")
    rows = store.captures_for("2026-09-18")
    assert rows["2026-09-18 09:30:00"]["status"] == "missed"
    assert rows["2026-09-18 09:30:00"]["error"] == "loop was down"
    assert store.snapshot_count() == 0


def test_backfill_progress_is_the_resume_set(store):
    store.mark_backfill(token=1, timeframe="15minute", from_date="2026-09-01",
                        through_date="2026-09-18", rows=250, status="ok")
    store.mark_backfill(token=2, timeframe="15minute", from_date="2026-09-01",
                        through_date="2026-09-18", rows=0, status="error",
                        error="boom")
    done = store.backfill_done(timeframe="15minute", through_date="2026-09-18")
    assert done == {1}                        # the failure is retried, not skipped


# ── roll-ups and retention (spec §2) ─────────────────────────────────────────

def test_a_daily_rollup_is_built_from_the_candles(store):
    for i, (t, o, h, l, c, v, oi) in enumerate([
            ("09:15:00", 10, 12, 9, 11, 100, 500),
            ("09:30:00", 11, 15, 10, 14, 200, 600),
            ("09:45:00", 14, 16, 8, 9, 300, 400)]):
        store.write_candles([_candle_row(1, f"2026-09-17 {t}", open=o, high=h, low=l,
                                         close=c, volume=v, oi=oi)])
    assert store.rollup_day("2026-09-17") >= 1
    row = store.con.execute("SELECT * FROM daily_rollups WHERE instrument_token=1").fetchone()
    assert (row["open"], row["high"], row["low"], row["close"]) == (10, 16, 8, 9)
    assert row["volume"] == 600 and row["bars"] == 3
    assert row["oi_open"] == 500 and row["oi_close"] == 400
    assert row["built_from"] == "candles_15m"


def test_pruning_rolls_a_session_up_before_deleting_its_raw_rows(store):
    old_day = date(2026, 1, 2)
    for hh in ("09:30:00", "09:45:00"):
        store.write_snapshots([_snapshot_row(7, f"{old_day.isoformat()} {hh}",
                                             volume=900, oi=300, day_high=20.0,
                                             day_low=5.0, day_open=6.0)])
    store.write_candles([_candle_row(7, f"{old_day.isoformat()} 09:15:00")])
    # today's mark, so the store is not eight months stale (see the guard test)
    store.write_snapshots([_snapshot_row(7, f"{TODAY.isoformat()} 09:30:00")])

    out = store.prune(today=TODAY)
    assert out["snapshots_deleted"] == 2
    assert store.snapshot_count() == 1            # today's mark is untouched
    kept = store.con.execute("SELECT * FROM daily_rollups WHERE instrument_token=7").fetchone()
    assert kept is not None                       # the day survives as a roll-up
    assert out["rolled_up_sessions"] >= 1


def test_candles_are_pruned_too_and_the_session_survives_as_a_rollup(store):
    """`candles_15m` used to be pruned by nothing at all -- ~8.9 GB a year.

    It is now on `config.CANDLE_DAYS`, and like the raw marks it is rolled up
    before it goes, so the session is kept and only the 15-minute resolution
    inside it is lost.
    """
    old_day = date(2026, 1, 2)
    store.write_candles([_candle_row(7, f"{old_day.isoformat()} 09:15:00"),
                         _candle_row(7, f"{old_day.isoformat()} 09:30:00")])
    store.write_snapshots([_snapshot_row(7, f"{TODAY.isoformat()} 09:30:00")])

    out = store.prune(today=TODAY)
    assert out["candles_deleted"] == 2
    assert store.candle_count(7) == 0
    kept = store.con.execute(
        "SELECT * FROM daily_rollups WHERE instrument_token=7 AND session_date=?",
        (old_day.isoformat(),)).fetchone()
    assert kept is not None and kept["built_from"] == "candles_15m"


def test_a_recent_candle_inside_the_window_is_kept(store):
    recent = (TODAY - timedelta(days=5)).isoformat()
    store.write_candles([_candle_row(9, f"{recent} 09:15:00")])
    store.write_snapshots([_snapshot_row(9, f"{TODAY.isoformat()} 09:30:00")])
    out = store.prune(today=TODAY)
    assert out["candles_deleted"] == 0
    assert store.candle_count(9) == 1


def test_a_cutoff_that_would_reach_the_served_session_is_refused(store):
    """The one thing retention must never do: delete what the tab is showing.

    A store whose newest mark is older than the retention window would be
    emptied wholesale by a naive `captured_at < cutoff`.  That is not a prune,
    it is a wipe, and it takes the Derivative tab's "as of" with it.
    """
    store.write_snapshots([_snapshot_row(7, "2026-01-02 09:30:00")])
    assert store.serving_session() == "2026-01-02"
    with pytest.raises(ValueError, match="serving"):
        store.prune(today=TODAY)
    assert store.snapshot_count() == 1


def test_a_retention_window_below_the_floor_is_refused(store):
    store.write_snapshots([_snapshot_row(7, f"{TODAY.isoformat()} 09:30:00")])
    with pytest.raises(ValueError, match="floor"):
        store.prune(today=TODAY, raw_days=1)
    assert store.snapshot_count() == 1


def test_a_dry_run_counts_and_deletes_nothing(store):
    old_day = date(2026, 1, 2)
    store.write_snapshots([_snapshot_row(7, f"{old_day.isoformat()} 09:30:00")])
    store.write_candles([_candle_row(7, f"{old_day.isoformat()} 09:15:00")])
    store.write_snapshots([_snapshot_row(7, f"{TODAY.isoformat()} 09:30:00")])

    out = store.prune(today=TODAY, dry_run=True)
    assert out["dry_run"] is True
    assert out["snapshots_deleted"] == 1 and out["candles_deleted"] == 1
    assert store.snapshot_count() == 2 and store.candle_count(7) == 1


def test_recent_raw_rows_are_not_pruned(store):
    recent = (TODAY - timedelta(days=5)).isoformat()
    store.write_snapshots([_snapshot_row(8, f"{recent} 09:30:00")])
    out = store.prune(today=TODAY)
    assert out["snapshots_deleted"] == 0
    assert store.snapshot_count() == 1


def test_each_table_is_cut_at_its_own_window(store):
    """The windows are separate numbers and each table is held to its own.

    They were 90 / 365 / never until 2026-09-19 and are now
    `config.RAW_SNAPSHOT_DAYS` / `METRICS_DAYS` / `CANDLE_DAYS`.  This asserts
    the constants are honoured, not the numbers themselves, so changing a window
    is a one-line change in `config` and not a test rewrite.
    """
    store.write_snapshots([_snapshot_row(1, f"{TODAY.isoformat()} 09:30:00")])
    inside = (TODAY - timedelta(days=config.METRICS_DAYS - 5)).isoformat()
    outside = (TODAY - timedelta(days=config.METRICS_DAYS + 5)).isoformat()
    for key, day in (("X", outside), ("Y", inside)):
        store.con.execute(
            "INSERT INTO metrics (scope, metric_key, captured_at, underlying) "
            "VALUES ('contract',?,?,'RELIANCE')", (key, f"{day} 09:30:00"))

    out = store.prune(today=TODAY)
    assert out["metrics_deleted"] == 1
    assert {r[0] for r in store.con.execute("SELECT metric_key FROM metrics")} == {"Y"}
    assert out["cutoffs"] == {
        "snapshots": (TODAY - timedelta(days=config.RAW_SNAPSHOT_DAYS)).isoformat(),
        "metrics": (TODAY - timedelta(days=config.METRICS_DAYS)).isoformat(),
        "candles_15m": (TODAY - timedelta(days=config.CANDLE_DAYS)).isoformat(),
        "underlying_snapshots": (
            TODAY - timedelta(days=config.UNDERLYING_SNAPSHOT_DAYS)).isoformat(),
    }
    assert out["raw_cutoff"] == out["cutoffs"]["snapshots"]


# ── access ───────────────────────────────────────────────────────────────────

def test_a_read_only_store_cannot_write(tmp_path):
    path = tmp_path / "d.db"
    DerivativesStore(path).close()
    ro = open_readonly(path)
    with pytest.raises(Exception):
        ro.con.execute("INSERT INTO meta (key, value) VALUES ('a','b')")


def test_the_writer_lock_stops_a_second_writer(store):
    lock = store.writer_lock()
    lock.acquire()
    try:
        with pytest.raises(RuntimeError):
            store.writer_lock().acquire()
    finally:
        lock.release()


# ── the mark a screen should say "as of" ─────────────────────────────────────

def test_the_latest_trading_mark_ignores_the_post_close_mark(store):
    store.write_snapshots([_snapshot_row(1, "2026-09-17 15:30:00", mark_kind="bar_close")])
    store.write_snapshots([_snapshot_row(1, "2026-09-17 15:45:00", mark_kind="post_close")])
    assert store.latest_trading_mark() == "2026-09-17 15:30:00"
    assert store.status()["last_mark"] == "2026-09-17 15:45:00"
    assert store.status()["latest_trading_mark"] == "2026-09-17 15:30:00"


def test_an_older_database_gains_new_columns_without_losing_rows(tmp_path):
    path = tmp_path / "old.db"
    first = DerivativesStore(path)
    first.con.execute("ALTER TABLE snapshots DROP COLUMN average_price_est")
    first.close()
    reopened = DerivativesStore(path)          # migration runs on open
    cols = {r[1] for r in reopened.con.execute("PRAGMA table_info(snapshots)")}
    assert "average_price_est" in cols
    reopened.write_snapshots([_snapshot_row(5, "2026-09-17 09:30:00")])
    assert reopened.snapshot_count() == 1
