"""Tests for market_data/store.py (db/market15.db)."""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data.aggregate import Bar  # noqa: E402
from market_data.store import MarketStore, WriterLock  # noqa: E402

INSTRUMENT = 6191105
SYMBOL = "TESTCO"


def _bars(n=4, start="2024-01-02 09:15:00", base=100.0):
    t0 = datetime.fromisoformat(start)
    out = []
    for i in range(n):
        s = t0 + timedelta(minutes=15 * i)
        p = base + i
        out.append(Bar(s, s + timedelta(minutes=15), p, p + 1, p - 1, p + 0.5,
                       1000 + i))
    return out


@pytest.fixture()
def store(tmp_path):
    st = MarketStore(tmp_path / "market15.db")
    yield st
    st.close()


def test_schema_tables_and_indexes(store):
    names = {r[0] for r in store.con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"candles_15m", "raw_archive", "corrections", "snapshots",
            "ingest_runs"} <= names
    idx = {r[0] for r in store.con.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "idx_candles15_sym_bar" in idx      # (symbol, bar_start)
    assert "idx_candles15_snapshot" in idx     # (snapshot_id)
    mode = store.con.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"


def test_upsert_and_read_window(store):
    written, rev = store.upsert_candles(
        SYMBOL, INSTRUMENT, _bars(4), vendor_id="fake", adjustment_basis_id="basis0")
    assert (written, rev) == (4, 1)
    rows = store.read_window(SYMBOL)
    assert len(rows) == 4
    assert rows[0].bar_start == datetime(2024, 1, 2, 9, 15)
    assert rows[0].vendor_id == "fake"
    assert rows[0].adjustment_basis_id == "basis0"
    # windowing is half-open [start, end)
    mid = store.read_window(SYMBOL, "2024-01-02 09:30:00", "2024-01-02 10:00:00")
    assert [r.bar_start.strftime("%H:%M") for r in mid] == ["09:30", "09:45"]


def test_new_revision_wins_and_history_is_kept(store):
    store.upsert_candles(SYMBOL, INSTRUMENT, _bars(4), vendor_id="fake",
                         adjustment_basis_id="basis0")
    fixed = list(_bars(4))
    fixed[1] = Bar(fixed[1].bar_start, fixed[1].bar_end, 101.0, 102.0, 100.0,
                   101.5, 1001, True, "corrected")
    written, rev = store.upsert_candles(SYMBOL, INSTRUMENT, [fixed[1]],
                                        vendor_id="kite",
                                        adjustment_basis_id="basis1")
    assert rev == 2
    latest = {r.bar_start: r for r in store.read_window(SYMBOL)}
    assert latest[fixed[1].bar_start].revision == 2
    assert latest[fixed[1].bar_start].vendor_id == "kite"
    assert len(latest) == 4                      # other bars still at revision 1
    old = store.read_window(SYMBOL, revision=1)
    assert len(old) == 4 and all(r.revision == 1 for r in old)
    assert store.con.execute("SELECT COUNT(*) FROM candles_15m").fetchone()[0] == 5


def test_corrections_carry_evidence(store):
    req = store.archive_raw(provider="kite", symbol=SYMBOL, timeframe="15minute",
                            start="2024-01-02 09:15:00", end="2024-01-02 15:30:00",
                            payload_path="raw/x.json.gz", sha256="deadbeef", rows=25)
    cid = store.record_correction(symbol=SYMBOL, timeframe="15m",
                                  bar_start="2024-01-02 09:30:00", field="low",
                                  old_value=65.35, new_value=1183.10,
                                  reason="source error confirmed against re-fetch",
                                  evidence_request_id=req, old_revision=1,
                                  new_revision=2)
    row = store.con.execute("SELECT * FROM corrections WHERE id=?", (cid,)).fetchone()
    assert row["evidence_request_id"] == req
    assert row["old_value"] == "65.35" and row["new_value"] == "1183.1"
    arch = store.con.execute("SELECT * FROM raw_archive WHERE request_id=?",
                             (req,)).fetchone()
    assert arch["sha256"] == "deadbeef"


def test_snapshot_freeze_checksum_and_pinning(store):
    store.upsert_candles(SYMBOL, INSTRUMENT, _bars(4), vendor_id="fake",
                         adjustment_basis_id="basis0")
    snap = store.create_snapshot(universe="TEST5", adjustment_basis_id="basis0",
                                 provider="fake")
    frozen = store.freeze_snapshot(snap)
    assert frozen["status"] == "frozen"
    assert frozen["row_count"] == 4 and frozen["symbol_count"] == 1
    assert len(frozen["checksum"]) == 64
    assert store.verify_snapshot(snap) is True
    # the snapshot keeps reading revision 1 after a revision 2 lands
    b = _bars(1)[0]
    store.upsert_candles(SYMBOL, INSTRUMENT,
                         [Bar(b.bar_start, b.bar_end, 1, 2, 0.5, 1.5, 7)],
                         vendor_id="kite", adjustment_basis_id="basis1")
    assert store.read_window(SYMBOL)[0].revision == 2
    pinned = store.read_window(SYMBOL, snapshot_id=snap)
    assert pinned[0].revision == 1 and pinned[0].open == 100.0
    assert store.verify_snapshot(snap) is True          # content unchanged
    # a different content set must produce a different checksum
    snap2 = store.create_snapshot(universe="TEST5")
    assert store.freeze_snapshot(snap2)["checksum"] != frozen["checksum"]


def test_ingest_run_lifecycle(store):
    run = store.start_run(provider="legacy_kanida_db", plan={"symbols": 5})
    store.finish_run(run, requests=5, rows=123, errors=0)
    row = store.con.execute("SELECT * FROM ingest_runs WHERE run_id=?",
                            (run,)).fetchone()
    assert row["status"] == "ok" and row["rows"] == 123 and row["finished_at"]


def test_readonly_store_cannot_write(tmp_path):
    path = tmp_path / "market15.db"
    with MarketStore(path) as st:
        st.upsert_candles(SYMBOL, INSTRUMENT, _bars(2), vendor_id="fake",
                          adjustment_basis_id="basis0")
    ro = MarketStore(path, read_only=True)
    try:
        assert len(ro.read_window(SYMBOL)) == 2
        with pytest.raises((RuntimeError, sqlite3.OperationalError)):
            ro.upsert_candles(SYMBOL, INSTRUMENT, _bars(1), vendor_id="fake",
                              adjustment_basis_id="basis0")
    finally:
        ro.close()


def test_writer_lock_is_exclusive(tmp_path):
    path = tmp_path / "market15.db"
    lock = WriterLock(path).acquire()
    try:
        with pytest.raises(RuntimeError):
            WriterLock(path).acquire()
    finally:
        lock.release()
    WriterLock(path).acquire().release()       # released -> acquirable again


def test_quality_flags_round_trip(store):
    bars = _bars(2)
    flagged = Bar(bars[0].bar_start, bars[0].bar_end, 100, 101, 65.35, 100.5,
                  10, True, "intrabucket_discontinuity,intraday_vs_daily")
    store.upsert_candles(SYMBOL, INSTRUMENT, [flagged, bars[1]], vendor_id="fake",
                         adjustment_basis_id="basis0")
    rows = store.read_window(SYMBOL)
    assert rows[0].quality_flags == "intrabucket_discontinuity,intraday_vs_daily"
    assert store.read_window(SYMBOL, include_flagged=False)[0].bar_start == \
        bars[1].bar_start
    cov = store.coverage(SYMBOL)[0]
    assert cov["rows"] == 2 and cov["flagged"] == 1


def test_a_volume_too_large_for_sqlite_is_stored_not_dropped(store):
    """A real defect in db/kanida.db: a legacy volume above 2**63-1.

    The row must still be written (nothing is ever dropped) even though SQLite
    cannot hold the integer exactly; validate.py flags it separately.
    """
    b = _bars(1)[0]
    huge = Bar(b.bar_start, b.bar_end, 100.0, 101.0, 99.0, 100.5, 2 ** 70,
               True, "volume_out_of_range")
    written, _ = store.upsert_candles(SYMBOL, INSTRUMENT, [huge],
                                      vendor_id="legacy_kanida_db",
                                      adjustment_basis_id="legacy_unknown")
    assert written == 1
    row = store.read_window(SYMBOL)[0]
    assert row.open == 100.0 and row.quality_flags == "volume_out_of_range"
    assert float(row.volume) == float(2 ** 70)


# ---------------------------------------------------------------------------
# label-based snapshot exclusions
# ---------------------------------------------------------------------------
def test_labelled_rows_are_excluded_from_a_snapshot_not_deleted(store):
    """The repair labelled rows unusable (`vendor_zero_print`, `wrong_instrument`
    ...) but kept them: deleting them would destroy the evidence for the label.
    A research snapshot therefore has to drop them by label."""
    store.upsert_candles(SYMBOL, INSTRUMENT, _bars(6), vendor_id="fake",
                         adjustment_basis_id="basis0")
    snap = store.create_snapshot(universe="TEST5")
    removed = store.exclude_snapshot_rows(
        snap, label="vendor_zero_print",
        bars=[(SYMBOL, "2024-01-02 09:30:00"), (SYMBOL, "2024-01-02 09:45:00")],
        source="verdicts.jsonl", note="two zero prints")
    assert removed["rows"] == 2 and removed["symbols_touched"] == 1
    assert len(store.read_window(SYMBOL, snapshot_id=snap)) == 4
    assert len(store.read_window(SYMBOL)) == 6, "the rows themselves must remain"
    (logged,) = store.snapshot_exclusions(snap)
    assert logged["label"] == "vendor_zero_print" and logged["rows"] == 2
    assert logged["source"] == "verdicts.jsonl"


def test_a_span_exclusion_drops_a_reused_token_window(store):
    """`wrong_instrument` is a window, not a bar list: everything up to the
    vendor's own first daily bar belongs to the token's earlier life."""
    store.upsert_candles(SYMBOL, INSTRUMENT, _bars(6), vendor_id="fake",
                         adjustment_basis_id="basis0")
    snap = store.create_snapshot(universe="TEST5")
    removed = store.exclude_snapshot_rows(
        snap, label="wrong_instrument",
        spans=[(SYMBOL, "2024-01-02 09:15:00", "2024-01-02 10:00:00")])
    assert removed["rows"] == 3            # 09:15, 09:30, 09:45 -- end is exclusive
    assert [r.bar_start.strftime("%H:%M")
            for r in store.read_window(SYMBOL, snapshot_id=snap)] == ["10:00", "10:15", "10:30"]


def test_excluding_every_row_of_a_symbol_drops_it_from_the_symbol_count(store):
    store.upsert_candles(SYMBOL, INSTRUMENT, _bars(2), vendor_id="fake",
                         adjustment_basis_id="basis0")
    store.upsert_candles("OTHERCO", INSTRUMENT + 1, _bars(2), vendor_id="fake",
                         adjustment_basis_id="basis0")
    snap = store.create_snapshot(universe="TEST5")
    removed = store.exclude_snapshot_rows(
        snap, label="wrong_instrument",
        spans=[(SYMBOL, "2024-01-01 00:00:00", "2024-01-03 00:00:00")])
    assert removed["rows"] == 2 and removed["symbols"] == 1
    assert store.freeze_snapshot(snap)["symbol_count"] == 1


def test_a_frozen_snapshot_cannot_be_edited(store):
    """Its checksum is an attestation; changing the content would void it."""
    store.upsert_candles(SYMBOL, INSTRUMENT, _bars(4), vendor_id="fake",
                         adjustment_basis_id="basis0")
    snap = store.create_snapshot(universe="TEST5")
    store.freeze_snapshot(snap)
    with pytest.raises(RuntimeError, match="frozen"):
        store.exclude_snapshot_rows(snap, label="unresolved",
                                    bars=[(SYMBOL, "2024-01-02 09:15:00")])
    assert store.verify_snapshot(snap) is True


def test_excluding_a_row_changes_the_checksum(store):
    store.upsert_candles(SYMBOL, INSTRUMENT, _bars(4), vendor_id="fake",
                         adjustment_basis_id="basis0")
    a = store.create_snapshot(universe="TEST5")
    full = store.freeze_snapshot(a)["checksum"]
    b = store.create_snapshot(universe="TEST5")
    store.exclude_snapshot_rows(b, label="unresolved",
                                bars=[(SYMBOL, "2024-01-02 09:15:00")])
    assert store.freeze_snapshot(b)["checksum"] != full


def test_excluding_a_bar_the_snapshot_never_had_removes_nothing(store):
    """Labels cover symbols outside this universe; they must be a no-op, not an
    error, and must not be counted as rows we removed."""
    store.upsert_candles(SYMBOL, INSTRUMENT, _bars(2), vendor_id="fake",
                         adjustment_basis_id="basis0")
    snap = store.create_snapshot(universe="TEST5")
    removed = store.exclude_snapshot_rows(
        snap, label="unresolved", bars=[("NOTINUNIVERSE", "2024-01-02 09:15:00")])
    assert removed["rows"] == 0
    assert removed["symbols_touched"] == 0, "a symbol outside the snapshot is a no-op"
    assert removed["symbols_labelled"] == 1
    assert len(store.read_window(SYMBOL, snapshot_id=snap)) == 2


# ---------------------------------------------------------------------------
# a busy store is waited out, never fatal
# ---------------------------------------------------------------------------
# On 2026-09-16 the live ingest loop died mid-cycle with
# `sqlite3.OperationalError: database is locked` raised from `start_run`, while
# a maintenance pass held the write lock for minutes. SQLite serialises
# writers by design, so "another writer has it" is a normal condition and must
# cost the loop time, not its life.

def _hold_write_lock(path, seconds, ready, done):
    """Hold a real SQLite write transaction on `path` for `seconds`."""
    con = sqlite3.connect(path, timeout=30, isolation_level=None)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("BEGIN IMMEDIATE")
        con.execute("CREATE TABLE IF NOT EXISTS _blocker (x INTEGER)")
        ready.set()
        done.wait(seconds)
        con.execute("COMMIT")
    finally:
        con.close()


def test_a_write_blocked_past_the_sqlite_timeout_retries_instead_of_raising(tmp_path):
    import threading

    path = tmp_path / "market15.db"
    MarketStore(path).close()                       # create the schema first
    ready, done = threading.Event(), threading.Event()
    holder = threading.Thread(target=_hold_write_lock,
                              args=(str(path), 30.0, ready, done), daemon=True)
    holder.start()
    assert ready.wait(10)
    # per-attempt SQLite timeout of 50 ms: without the retry this raises at once
    store = MarketStore(path, timeout=0.05, write_deadline=30.0, create=False)
    try:
        threading.Timer(1.5, done.set).start()      # the other writer lets go
        run = store.start_run(provider="kite", plan={"symbols": 1})
        assert store.con.execute("SELECT COUNT(*) FROM ingest_runs WHERE run_id=?",
                                 (run,)).fetchone()[0] == 1
        assert store.busy_retries >= 1, "it should have had to wait at least once"
    finally:
        done.set()
        holder.join(timeout=10)
        store.close()


def test_a_write_still_gives_up_once_the_deadline_passes(tmp_path):
    """Waiting forever would be a hang; the vendor's own error must surface."""
    import threading

    path = tmp_path / "market15.db"
    MarketStore(path).close()
    ready, done = threading.Event(), threading.Event()
    holder = threading.Thread(target=_hold_write_lock,
                              args=(str(path), 5.0, ready, done), daemon=True)
    holder.start()
    assert ready.wait(10)
    store = MarketStore(path, timeout=0.05, write_deadline=0.3, create=False)
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked|busy"):
            store.start_run(provider="kite")
    finally:
        done.set()
        holder.join(timeout=10)
        store.close()


def test_a_real_error_is_not_retried(tmp_path):
    """Retrying a missing table would turn a loud bug into a 15-minute hang."""
    store = MarketStore(tmp_path / "market15.db")
    try:
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            store.write("INSERT INTO nope (x) VALUES (1)")
        assert store.busy_retries == 0
    finally:
        store.close()


def test_retry_while_busy_backs_off_geometrically_and_stops_at_the_deadline():
    """Pure, with an injected clock -- no sleeping, no database."""
    from market_data.store import retry_while_busy

    waits, clock = [], [0.0]
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 4:
            raise sqlite3.OperationalError("database is locked")
        return "written"

    out = retry_while_busy(flaky, deadline_seconds=60,
                           timer=lambda: clock[0],
                           sleep=lambda s: (waits.append(s),
                                            clock.__setitem__(0, clock[0] + s)))
    assert out == "written"
    assert waits == [0.25, 0.5, 1.0]

    def never():
        raise sqlite3.OperationalError("database is locked")

    clock[0] = 0.0
    with pytest.raises(sqlite3.OperationalError):
        retry_while_busy(never, deadline_seconds=1.0, timer=lambda: clock[0],
                         sleep=lambda s: clock.__setitem__(0, clock[0] + s))


def test_is_busy_error_only_matches_contention():
    from market_data.store import is_busy_error

    assert is_busy_error(sqlite3.OperationalError("database is locked"))
    assert is_busy_error(sqlite3.OperationalError("database is busy"))
    assert not is_busy_error(sqlite3.OperationalError("no such column: x"))
    assert not is_busy_error(sqlite3.IntegrityError("database is locked"))


def test_a_transaction_retries_the_begin_but_never_replays_its_body(tmp_path):
    """Re-running the body would double-apply whatever already succeeded."""
    import threading

    path = tmp_path / "market15.db"
    MarketStore(path).close()
    ready, done = threading.Event(), threading.Event()
    holder = threading.Thread(target=_hold_write_lock,
                              args=(str(path), 30.0, ready, done), daemon=True)
    holder.start()
    assert ready.wait(10)
    store = MarketStore(path, timeout=0.05, write_deadline=30.0, create=False)
    try:
        threading.Timer(1.0, done.set).start()
        bodies = 0
        with store.transaction() as con:
            bodies += 1
            con.executemany(
                "INSERT INTO quality_findings (run_id,symbol,timeframe,code,"
                "severity,message,created_at) VALUES (?,?,?,?,?,?,?)",
                [("r", SYMBOL, "15minute", "X", "info", "m", "2026-09-16")])
        assert bodies == 1
        assert store.con.execute(
            "SELECT COUNT(*) FROM quality_findings").fetchone()[0] == 1
        assert store.busy_retries >= 1
    finally:
        done.set()
        holder.join(timeout=10)
        store.close()
