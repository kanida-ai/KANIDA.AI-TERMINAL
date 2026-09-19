"""`db/market15.db` -- the revisioned 15-minute candle store.

Contract: docs/DATA_PIPELINE_CONTRACT.md section 2.

Key properties
--------------
* **Revisioned, never overwritten.** `upsert_candles` writes a *new revision*;
  the previous rows stay. Reads resolve "latest revision wins" unless a
  revision or snapshot is pinned.
* **Provenance.** Every row can point at a `raw_archive.request_id` and an
  `ingest_runs.run_id`. Every changed value gets a `corrections` row with the
  evidence id -- the thing the PIIND audit could not find.
* **Snapshots are frozen with a checksum** over their exact content, so a
  research run can prove which bytes it read.
* **WAL, one writer, many readers.** Readers open `mode=ro` + `query_only=ON`
  and are never blocked by the writer. A writer takes an advisory lock file so
  two seeders cannot interleave.
* **A busy store is waited out, not fatal.** SQLite serialises writers, and a
  maintenance pass (a full-history re-fetch, a 29-million-row snapshot) holds
  the write lock for *minutes*. A bare `busy_timeout` turns that into
  `sqlite3.OperationalError: database is locked` and kills whatever hit it --
  which is exactly how the live ingest loop died mid-cycle on 2026-09-16
  against `start_run`. Every write here therefore retries with backoff up to
  `write_deadline` seconds, and only then gives up. Retrying is safe because a
  statement that could not take the lock did nothing at all.

`db/kanida.db` is NEVER opened for writing anywhere in this package.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from market_data.aggregate import SQLITE_MAX_INT, Bar, bar_from_raw

LOG = logging.getLogger("market_data.store")

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "db" / "market15.db"

#: Seconds SQLite itself waits for the write lock inside one statement.
DEFAULT_BUSY_TIMEOUT = 60.0

#: Seconds a write keeps *retrying* before it gives up, across attempts.
#: Sized off the real maintenance passes that hold the lock: the repair's
#: snapshot freeze ran ~8 minutes, a full-history re-fetch ~2. A live cycle that
#: waits 15 minutes and then writes is a late cycle; one that raises is a dead
#: loop, and the loop is what keeps the app's data moving.
DEFAULT_WRITE_DEADLINE = 900.0

CANDLE_COLUMNS = (
    "instrument_id", "symbol", "exchange", "bar_start", "bar_end",
    "open", "high", "low", "close", "volume", "candle_complete",
    "quality_flags", "adjustment_basis_id", "vendor_id", "vendor_revision",
    "fetched_at", "snapshot_id", "revision", "source_request_id", "run_id",
)


def utcnow() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def _ts(value) -> str:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


def _volume_for_sqlite(volume):
    """Bind a volume SQLite can actually store.

    SQLite integers are signed 64-bit. `db/kanida.db` contains legacy volumes
    above that, which would raise OverflowError and abort the write. Rather
    than drop the row (never) or invent a value (never), the volume is stored
    as a REAL; validate.py has already flagged the row `volume_out_of_range`
    and kept the exact figure in the finding's evidence.
    """
    v = int(volume)
    return v if -SQLITE_MAX_INT - 1 <= v <= SQLITE_MAX_INT else float(v)


def is_busy_error(exc: BaseException) -> bool:
    """Is this the "someone else is writing" error, rather than a real fault?

    Only these two are safe to retry: SQLite raises them *before* running the
    statement, so nothing partial happened. Every other `OperationalError`
    (a missing table, a bad column) must surface immediately -- retrying those
    would turn a loud bug into a 15-minute hang.
    """
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    text = str(exc).lower()
    return "database is locked" in text or "database is busy" in text


def retry_while_busy(operation, *, deadline_seconds: float,
                     first_wait: float = 0.25, factor: float = 2.0,
                     max_wait: float = 10.0, on_retry=None,
                     timer=time.monotonic, sleep=time.sleep):
    """Run `operation`, waiting out another writer until `deadline_seconds`.

    Backs off geometrically and re-raises the vendor's own exception when the
    deadline would be crossed, so "we waited and it never freed up" is still a
    loud failure and never a silent success.
    """
    started = timer()
    wait = first_wait
    attempt = 0
    while True:
        attempt += 1
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            if not is_busy_error(exc):
                raise
            if (timer() - started) + wait > deadline_seconds:
                raise
            if on_retry is not None:
                on_retry(attempt, wait, exc)
            sleep(wait)
            wait = min(wait * factor, max_wait)


def coerce_instrument_id(value):
    """Keep one key shape for `instrument_id`.

    W1's `RawCandle.instrument_id` is a *str*; the legacy Kite token is an int.
    SQLite is dynamically typed, so '6191105' and 6191105 would be two
    different primary keys. Anything that looks like an integer is stored as
    one; anything else is stored verbatim.
    """
    if isinstance(value, bool):
        raise TypeError("instrument_id must not be a bool")
    if isinstance(value, int):
        return value
    text = str(value).strip()
    return int(text) if text.lstrip("-").isdigit() else text


@dataclass(frozen=True)
class StoredCandle:
    instrument_id: int
    symbol: str
    exchange: str
    bar_start: datetime
    bar_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    candle_complete: bool
    quality_flags: str
    adjustment_basis_id: str
    vendor_id: str
    revision: int
    snapshot_id: str | None = None

    def to_bar(self) -> Bar:
        return Bar(self.bar_start, self.bar_end, self.open, self.high, self.low,
                   self.close, self.volume, self.candle_complete,
                   self.quality_flags)


class WriterLock:
    """Advisory single-writer lock (a lock file beside the DB).

    SQLite's WAL already serialises writers; this catches the *operational*
    mistake of two seeders running at once, and tells you which PID holds it.
    """

    def __init__(self, db_path: str | os.PathLike, stale_seconds: float = 6 * 3600):
        self.path = Path(f"{os.fspath(db_path)}.writer.lock")
        self.stale_seconds = stale_seconds
        self._held = False

    def acquire(self) -> "WriterLock":
        if self.path.exists():
            age = time.time() - self.path.stat().st_mtime
            holder = self.path.read_text(encoding="utf-8", errors="replace").strip()
            if age > self.stale_seconds or not _pid_alive(holder):
                self.path.unlink(missing_ok=True)
            else:
                raise RuntimeError(
                    f"market15 writer lock held by {holder} ({age:.0f}s old): {self.path}")
        fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, f"pid={os.getpid()} at={utcnow()}".encode())
        finally:
            os.close(fd)
        self._held = True
        return self

    def release(self) -> None:
        if self._held:
            self.path.unlink(missing_ok=True)
            self._held = False

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *exc):
        self.release()
        return False


def _pid_alive(holder: str) -> bool:
    try:
        pid = int(holder.split("pid=")[1].split()[0])
    except Exception:
        return True  # unparseable -> assume live, fail loudly
    if os.name == "nt":
        import subprocess
        try:
            out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"],
                                 capture_output=True, text=True, timeout=10).stdout
            return str(pid) in out
        except Exception:
            return True
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


class MarketStore:
    """Read/write access to `db/market15.db`."""

    def __init__(self, path: str | os.PathLike = DEFAULT_DB_PATH,
                 read_only: bool = False, timeout: float = DEFAULT_BUSY_TIMEOUT,
                 create: bool | None = None,
                 write_deadline: float = DEFAULT_WRITE_DEADLINE):
        self.path = Path(path)
        self.read_only = read_only
        #: total seconds a write waits out another writer before giving up
        self.write_deadline = write_deadline
        #: how many times a write has had to wait -- surfaced by the callers
        #: that care (the live loop logs it) so "slow" never looks like "fine"
        self.busy_retries = 0
        if read_only:
            uri = f"file:{self.path.as_posix()}?mode=ro"
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            uri = f"file:{self.path.as_posix()}"
        self.con = sqlite3.connect(uri, uri=True, timeout=timeout,
                                   isolation_level=None)
        self.con.row_factory = sqlite3.Row
        self.con.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
        if read_only:
            self.con.execute("PRAGMA query_only=ON")
        else:
            self.con.execute("PRAGMA journal_mode=WAL")
            self.con.execute("PRAGMA synchronous=NORMAL")
            self.con.execute("PRAGMA foreign_keys=ON")
            self.con.execute("PRAGMA temp_store=MEMORY")
            self.con.execute("PRAGMA cache_size=-262144")  # ~256 MB
            if create is not False:
                self.init_schema()

    # -- busy handling ------------------------------------------------------
    def _on_retry(self, attempt: int, wait: float, exc: Exception) -> None:
        self.busy_retries += 1
        LOG.warning("%s is busy (%s); attempt %d, retrying in %.1fs "
                    "(deadline %.0fs)", self.path.name, exc, attempt, wait,
                    self.write_deadline)

    def retrying(self, operation, **kw):
        """Run a write, waiting out whoever else holds the lock."""
        return retry_while_busy(operation, deadline_seconds=self.write_deadline,
                                on_retry=self._on_retry, **kw)

    def write(self, sql: str, params: Sequence = ()) -> sqlite3.Cursor:
        """One autocommit statement, retried while the store is busy.

        Every mutating statement in this class goes through here or through
        `transaction()`. A plain `self.con.execute` on a write would be the
        failure mode this exists to remove.
        """
        return self.retrying(lambda: self.con.execute(sql, params))

    # -- lifecycle ----------------------------------------------------------
    def init_schema(self) -> None:
        # `CREATE TABLE IF NOT EXISTS` still takes the write lock, so opening a
        # store while a maintenance pass runs would otherwise raise before the
        # caller had done anything at all.
        self.retrying(
            lambda: self.con.executescript(SCHEMA_PATH.read_text(encoding="utf-8")))

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """A write transaction that waits out another writer instead of dying.

        Only `BEGIN IMMEDIATE` and `COMMIT` are retried, and both are safe to
        retry: a `BEGIN` that could not take the lock started nothing, and a
        `COMMIT` that could not take it leaves the transaction open and
        unchanged. The statements in between are never re-run, so nothing is
        applied twice.
        """
        self.retrying(lambda: self.con.execute("BEGIN IMMEDIATE"))
        try:
            yield self.con
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        else:
            self.retrying(lambda: self.con.execute("COMMIT"))

    # -- writes -------------------------------------------------------------
    def next_revision(self, instrument_id: int, start=None, end=None) -> int:
        sql = "SELECT COALESCE(MAX(revision),0) FROM candles_15m WHERE instrument_id=?"
        params: list = [coerce_instrument_id(instrument_id)]
        if start is not None:
            sql += " AND bar_start>=?"
            params.append(_ts(start))
        if end is not None:
            sql += " AND bar_start<?"
            params.append(_ts(end))
        return int(self.con.execute(sql, params).fetchone()[0]) + 1

    def upsert_candles(self, symbol: str, instrument_id: int, bars: Iterable,
                       *, vendor_id: str, adjustment_basis_id: str,
                       exchange: str = "NSE", revision: int | None = None,
                       vendor_revision: str | None = None,
                       fetched_at: str | None = None,
                       snapshot_id: str | None = None,
                       source_request_id: str | None = None,
                       run_id: str | None = None,
                       batch: int = 20_000) -> tuple[int, int]:
        """Write `bars` as a **new revision**. Returns (rows_written, revision).

        Existing rows are left untouched; `read_window` resolves latest-wins.
        Passing an explicit `revision` (e.g. 1 for the legacy seed) is allowed
        and is idempotent via INSERT OR REPLACE on the (instrument, bar, rev) PK.
        """
        if self.read_only:
            raise RuntimeError("store opened read-only")
        instrument_id = coerce_instrument_id(instrument_id)
        bars = [bar_from_raw(b) for b in bars]
        if not bars:
            return 0, revision or 0
        rev = revision if revision is not None else self.next_revision(instrument_id)
        fetched_at = fetched_at or utcnow()
        sql = (f"INSERT OR REPLACE INTO candles_15m ({','.join(CANDLE_COLUMNS)}) "
               f"VALUES ({','.join('?' * len(CANDLE_COLUMNS))})")
        rows = [
            (instrument_id, symbol, exchange,
             b.bar_start.isoformat(sep=" "), b.bar_end.isoformat(sep=" "),
             float(b.open), float(b.high), float(b.low), float(b.close),
             _volume_for_sqlite(b.volume), 1 if b.candle_complete else 0,
             b.quality_flags,
             adjustment_basis_id, vendor_id, vendor_revision, fetched_at,
             snapshot_id, rev, source_request_id, run_id)
            for b in bars
        ]
        written = 0
        for i in range(0, len(rows), batch):
            chunk = rows[i:i + batch]
            with self.transaction() as con:
                con.executemany(sql, chunk)
            written += len(chunk)
        return written, rev

    def record_correction(self, *, symbol: str, timeframe: str, bar_start,
                          field: str, old_value, new_value, reason: str,
                          evidence_request_id: str | None = None,
                          run_id: str | None = None,
                          old_revision: int | None = None,
                          new_revision: int | None = None) -> int:
        """Record one field change with its evidence. Never edits in place."""
        cur = self.write(
            "INSERT INTO corrections (symbol,timeframe,bar_start,field,old_value,"
            "new_value,reason,evidence_request_id,run_id,old_revision,new_revision,"
            "created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (symbol, timeframe, _ts(bar_start), field,
             None if old_value is None else str(old_value),
             None if new_value is None else str(new_value),
             reason, evidence_request_id, run_id, old_revision, new_revision,
             utcnow()))
        return int(cur.lastrowid)

    def archive_raw(self, *, provider: str, symbol: str, timeframe: str,
                    start, end, payload_path: str, sha256: str,
                    request_id: str | None = None, rows: int | None = None,
                    run_id: str | None = None, fetched_at: str | None = None) -> str:
        request_id = request_id or uuid.uuid4().hex
        self.write(
            "INSERT OR REPLACE INTO raw_archive (request_id,provider,symbol,timeframe,"
            "start,end,fetched_at,sha256,payload_path,run_id,rows) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (request_id, provider, symbol, timeframe, _ts(start), _ts(end),
             fetched_at or utcnow(), sha256, payload_path, run_id, rows))
        return request_id

    def record_findings(self, findings: Iterable, run_id: str | None = None) -> int:
        rows = []
        for f in findings:
            r = f.to_row() if hasattr(f, "to_row") else dict(f)
            rows.append((run_id, r["symbol"], r["timeframe"], r.get("bar_start"),
                         r["code"], r["severity"], r["message"], r.get("evidence"),
                         utcnow()))
        if not rows:
            return 0
        with self.transaction() as con:
            con.executemany(
                "INSERT INTO quality_findings (run_id,symbol,timeframe,bar_start,"
                "code,severity,message,evidence,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                rows)
        return len(rows)

    # -- reads --------------------------------------------------------------
    def read_window(self, symbol: str, start=None, end=None, *,
                    revision: int | None = None,
                    snapshot_id: str | None = None,
                    include_flagged: bool = True) -> list[StoredCandle]:
        """Rows for `symbol` in [start, end).

        * `snapshot_id` -> exactly the rows frozen into that snapshot;
        * `revision`    -> that revision only;
        * neither       -> latest revision per bar_start.
        """
        params: list = []
        where = ["c.symbol=?"]
        params.append(symbol)
        if start is not None:
            where.append("c.bar_start>=?")
            params.append(_ts(start))
        if end is not None:
            where.append("c.bar_start<?")
            params.append(_ts(end))
        if not include_flagged:
            where.append("c.quality_flags=''")

        if snapshot_id is not None:
            sql = ("SELECT c.* FROM candles_15m c JOIN snapshot_members m "
                   "ON m.instrument_id=c.instrument_id AND m.bar_start=c.bar_start "
                   "AND m.revision=c.revision WHERE m.snapshot_id=? AND "
                   + " AND ".join(where) + " ORDER BY c.bar_start")
            params = [snapshot_id] + params
        elif revision is not None:
            where.append("c.revision=?")
            params.append(revision)
            sql = ("SELECT c.* FROM candles_15m c WHERE " + " AND ".join(where)
                   + " ORDER BY c.bar_start")
        else:
            sql = ("SELECT c.* FROM candles_15m c WHERE " + " AND ".join(where)
                   + " AND c.revision=(SELECT MAX(c2.revision) FROM candles_15m c2 "
                     "WHERE c2.instrument_id=c.instrument_id AND c2.bar_start=c.bar_start)"
                     " ORDER BY c.bar_start")
        return [_to_stored(r) for r in self.con.execute(sql, params)]

    def read_bars(self, symbol: str, start=None, end=None, **kw) -> list[Bar]:
        return [c.to_bar() for c in self.read_window(symbol, start, end, **kw)]

    def symbols(self) -> list[str]:
        return [r[0] for r in self.con.execute(
            "SELECT DISTINCT symbol FROM candles_15m ORDER BY symbol")]

    def coverage(self, symbol: str | None = None) -> list[dict]:
        sql = ("SELECT symbol, COUNT(*) AS rows, MIN(bar_start) AS first_bar, "
               "MAX(bar_start) AS last_bar, MAX(revision) AS max_revision, "
               "SUM(quality_flags<>'') AS flagged FROM candles_15m ")
        params: tuple = ()
        if symbol:
            sql += "WHERE symbol=? "
            params = (symbol,)
        sql += "GROUP BY symbol ORDER BY symbol"
        return [dict(r) for r in self.con.execute(sql, params)]

    # -- snapshots ----------------------------------------------------------
    def create_snapshot(self, snapshot_id: str | None = None, *, universe: str,
                        adjustment_basis_id: str | None = None,
                        provider: str | None = None,
                        symbols: Sequence[str] | None = None,
                        start=None, end=None, notes: str = "") -> str:
        """Pin the current latest revision of every matching bar into a snapshot.

        This is deliberately **one transaction**: a snapshot pinned symbol by
        symbol would be pinned at different instants, and "which bytes did the
        research read" is the whole point of the table. Over the real store
        (~35 M rows) that holds the write lock for several minutes, which is
        why every other writer retries instead of raising -- see
        `DEFAULT_WRITE_DEADLINE`, which is sized to outlast this.
        """
        if self.read_only:
            raise RuntimeError("store opened read-only")
        snapshot_id = snapshot_id or f"snap_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"
        where = ["c.revision=(SELECT MAX(c2.revision) FROM candles_15m c2 "
                 "WHERE c2.instrument_id=c.instrument_id AND c2.bar_start=c.bar_start)"]
        params: list = []
        if symbols:
            where.append(f"c.symbol IN ({','.join('?' * len(symbols))})")
            params += list(symbols)
        if start is not None:
            where.append("c.bar_start>=?")
            params.append(_ts(start))
        if end is not None:
            where.append("c.bar_start<?")
            params.append(_ts(end))
        with self.transaction() as con:
            con.execute("INSERT OR REPLACE INTO snapshots (snapshot_id,created_at,"
                        "universe,adjustment_basis_id,provider,status,notes) "
                        "VALUES (?,?,?,?,?, 'open', ?)",
                        (snapshot_id, utcnow(), universe, adjustment_basis_id,
                         provider, notes))
            con.execute("DELETE FROM snapshot_members WHERE snapshot_id=?",
                        (snapshot_id,))
            con.execute(
                "INSERT INTO snapshot_members (snapshot_id,instrument_id,symbol,"
                "bar_start,revision) SELECT ?, c.instrument_id, c.symbol, "
                "c.bar_start, c.revision FROM candles_15m c WHERE "
                + " AND ".join(where), [snapshot_id] + params)
        return snapshot_id

    def exclude_snapshot_rows(self, snapshot_id: str, *, label: str,
                              bars: Iterable[tuple] = (),
                              spans: Iterable[tuple] = (),
                              source: str = "", note: str = "",
                              batch: int = 5_000) -> dict:
        """Drop labelled rows from an **open** snapshot and record the fact.

        `bars`  -- (symbol, bar_start) pairs;
        `spans` -- (symbol, start, end) with `start <= bar_start < end`.

        The rows stay in `candles_15m`: a label the repair attached ("the vendor
        and we hold the same zero print", "this token had an earlier life") is a
        statement about the data, and deleting the data would destroy the
        evidence for it.  A research snapshot excludes them instead, and this
        method writes a `snapshot_exclusions` row so the exclusion is visible
        rather than looking like a coverage hole.

        The deletes are keyed on `instrument_id`, resolved from the symbol here,
        because that is the snapshot's primary key. Deleting on `symbol` -- which
        is how the labels arrive -- would scan the snapshot's whole member set
        once per labelled bar.
        """
        if self.read_only:
            raise RuntimeError("store opened read-only")
        row = self.con.execute("SELECT status FROM snapshots WHERE snapshot_id=?",
                               (snapshot_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown snapshot {snapshot_id!r}")
        if row["status"] == "frozen":
            raise RuntimeError(
                f"snapshot {snapshot_id} is frozen; its content is what its "
                f"checksum attests to and must not change")
        bars = list(bars)
        spans = list(spans)
        touched = {s for s, _ in bars} | {s for s, _, _ in spans}
        ids = {s: self._instrument_ids(s) for s in touched}
        # Which of them the snapshot actually holds, so a label naming a symbol
        # outside this universe is not later counted as a symbol we removed.
        present = {s for s in touched
                   if ids[s] and self._snapshot_has(snapshot_id, ids[s])}
        pairs = [(snapshot_id, i, _ts(b)) for s, b in bars for i in ids[s]]
        windows = [(snapshot_id, i, _ts(a), _ts(b))
                   for s, a, b in spans for i in ids[s]]
        # `total_changes` is exact and O(1); counting the snapshot's members
        # before and after would be two 28-million-row scans per label.
        changes_before = self.con.total_changes
        for i in range(0, len(pairs), batch):
            with self.transaction() as con:
                con.executemany(
                    "DELETE FROM snapshot_members WHERE snapshot_id=? AND "
                    "instrument_id=? AND bar_start=?", pairs[i:i + batch])
        for i in range(0, len(windows), batch):
            with self.transaction() as con:
                con.executemany(
                    "DELETE FROM snapshot_members WHERE snapshot_id=? AND "
                    "instrument_id=? AND bar_start>=? AND bar_start<?",
                    windows[i:i + batch])
        removed = {"label": label,
                   "rows": self.con.total_changes - changes_before,
                   "symbols": sum(1 for s in present
                                  if not self._snapshot_has(snapshot_id, ids[s])),
                   "symbols_touched": len(present),
                   "symbols_labelled": len(touched)}
        self.write(
            "INSERT OR REPLACE INTO snapshot_exclusions (snapshot_id,label,rows,"
            "symbols,source,note,created_at) VALUES (?,?,?,?,?,?,?)",
            (snapshot_id, label, removed["rows"], removed["symbols"], source,
             note, utcnow()))
        return removed

    def _instrument_ids(self, symbol: str) -> list:
        """Every instrument id `symbol` has ever been stored under.

        All of them, not just one: a symbol whose token was reused has more
        than one, and an exclusion that missed the second would leave exactly
        the rows it was meant to drop. Served by `idx_candles15_sym_bar` and
        run once per labelled *symbol*, not once per labelled bar.
        """
        return [r[0] for r in self.con.execute(
            "SELECT DISTINCT instrument_id FROM candles_15m WHERE symbol=?",
            (symbol,))]

    def _snapshot_has(self, snapshot_id: str, instrument_ids: Sequence) -> bool:
        """PK-prefix seek -- never a scan; once per labelled symbol."""
        for iid in instrument_ids:
            if self.con.execute(
                    "SELECT 1 FROM snapshot_members WHERE snapshot_id=? AND "
                    "instrument_id=? LIMIT 1", (snapshot_id, iid)).fetchone():
                return True
        return False

    def _snapshot_counts(self, snapshot_id: str) -> dict:
        """Full counts. A scan of the snapshot's members -- call it sparingly."""
        r = self.con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT symbol) FROM snapshot_members "
            "WHERE snapshot_id=?", (snapshot_id,)).fetchone()
        return {"rows": int(r[0] or 0), "symbols": int(r[1] or 0)}

    def snapshot_exclusions(self, snapshot_id: str) -> list[dict]:
        return [dict(r) for r in self.con.execute(
            "SELECT label, rows, symbols, source, note, created_at FROM "
            "snapshot_exclusions WHERE snapshot_id=? ORDER BY label",
            (snapshot_id,))]

    def snapshot_checksum(self, snapshot_id: str) -> tuple[str, dict]:
        """sha256 over the snapshot's exact content (streamed, order-stable)."""
        h = hashlib.sha256()
        h.update(f"market15/v1/{snapshot_id}\n".encode())
        rows = symbols = 0
        first = last = None
        seen: set[str] = set()
        cur = self.con.execute(
            "SELECT c.symbol, c.instrument_id, c.bar_start, c.bar_end, c.open, "
            "c.high, c.low, c.close, c.volume, c.candle_complete, c.quality_flags, "
            "c.revision, c.vendor_id, c.adjustment_basis_id "
            "FROM candles_15m c JOIN snapshot_members m "
            "ON m.instrument_id=c.instrument_id AND m.bar_start=c.bar_start "
            "AND m.revision=c.revision WHERE m.snapshot_id=? "
            "ORDER BY c.symbol, c.bar_start, c.revision", (snapshot_id,))
        for r in cur:
            line = "|".join((
                r["symbol"], str(r["instrument_id"]), r["bar_start"], r["bar_end"],
                f"{r['open']:.6f}", f"{r['high']:.6f}", f"{r['low']:.6f}",
                f"{r['close']:.6f}", str(r["volume"]), str(r["candle_complete"]),
                r["quality_flags"] or "", str(r["revision"]),
                r["vendor_id"] or "", r["adjustment_basis_id"] or ""))
            h.update(line.encode())
            h.update(b"\n")
            rows += 1
            if r["symbol"] not in seen:
                seen.add(r["symbol"])
                symbols += 1
            if first is None or r["bar_start"] < first:
                first = r["bar_start"]
            if last is None or r["bar_start"] > last:
                last = r["bar_start"]
        return h.hexdigest(), {"row_count": rows, "symbol_count": symbols,
                               "first_bar": first, "last_bar": last}

    def freeze_snapshot(self, snapshot_id: str) -> dict:
        """Compute + store the checksum and mark the snapshot frozen."""
        row = self.con.execute("SELECT status FROM snapshots WHERE snapshot_id=?",
                               (snapshot_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown snapshot {snapshot_id!r}")
        if row["status"] == "frozen":
            return dict(self.con.execute(
                "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,)).fetchone())
        checksum, stats = self.snapshot_checksum(snapshot_id)
        with self.transaction() as con:
            con.execute(
                "UPDATE snapshots SET checksum=?, row_count=?, symbol_count=?, "
                "first_bar=?, last_bar=?, status='frozen', frozen_at=? "
                "WHERE snapshot_id=?",
                (checksum, stats["row_count"], stats["symbol_count"],
                 stats["first_bar"], stats["last_bar"], utcnow(), snapshot_id))
        return dict(self.con.execute("SELECT * FROM snapshots WHERE snapshot_id=?",
                                     (snapshot_id,)).fetchone())

    def verify_snapshot(self, snapshot_id: str) -> bool:
        """Recompute the checksum and compare with the frozen one."""
        row = self.con.execute("SELECT checksum FROM snapshots WHERE snapshot_id=?",
                               (snapshot_id,)).fetchone()
        if row is None or not row["checksum"]:
            return False
        return self.snapshot_checksum(snapshot_id)[0] == row["checksum"]

    # -- ingest runs --------------------------------------------------------
    def start_run(self, run_id: str | None = None, *, provider: str,
                  plan: str | dict = "") -> str:
        run_id = run_id or f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
        if isinstance(plan, dict):
            plan = json.dumps(plan, sort_keys=True, default=str)
        self.write(
            "INSERT OR REPLACE INTO ingest_runs (run_id,started_at,provider,plan,status)"
            " VALUES (?,?,?,?, 'running')", (run_id, utcnow(), provider, plan))
        return run_id

    def finish_run(self, run_id: str, *, requests: int = 0, rows: int = 0,
                   errors: int = 0, status: str = "ok") -> None:
        self.write(
            "UPDATE ingest_runs SET finished_at=?, requests=?, rows=?, errors=?, "
            "status=? WHERE run_id=?",
            (utcnow(), requests, rows, errors, status, run_id))

    # -- seed progress (resumability) --------------------------------------
    def mark_progress(self, symbol: str, *, run_id: str | None, status: str,
                      rows: int = 0, first_bar=None, last_bar=None,
                      source_mix: str = "", findings: int = 0,
                      error: str | None = None) -> None:
        self.write(
            "INSERT OR REPLACE INTO seed_progress (symbol,run_id,status,rows,"
            "first_bar,last_bar,source_mix,findings,error,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (symbol, run_id, status, rows,
             _ts(first_bar) if first_bar else None,
             _ts(last_bar) if last_bar else None,
             source_mix, findings, error, utcnow()))

    def done_symbols(self) -> set[str]:
        return {r[0] for r in self.con.execute(
            "SELECT symbol FROM seed_progress WHERE status='done'")}

    # -- session regimes (contract 2A) --------------------------------------
    def record_regimes(self, entries: Iterable, run_id: str | None = None) -> int:
        """Persist SessionRegime records. Same (symbol, effective_from) replaces."""
        rows = []
        for e in entries:
            d = e.to_dict() if hasattr(e, "to_dict") else dict(e)
            rows.append((d["symbol"], d["effective_from"], d["regime"],
                         d.get("source", "observed"), d.get("evidence", ""),
                         run_id, utcnow()))
        if not rows:
            return 0
        with self.transaction() as con:
            con.executemany(
                "INSERT OR REPLACE INTO session_regimes (symbol,effective_from,"
                "regime,source,evidence,run_id,updated_at) VALUES (?,?,?,?,?,?,?)",
                rows)
        return len(rows)

    def load_regimes(self, symbol: str | None = None) -> list[dict]:
        sql = ("SELECT symbol, effective_from, regime, source, evidence "
               "FROM session_regimes ")
        params: tuple = ()
        if symbol:
            sql += "WHERE symbol=? "
            params = (symbol,)
        sql += "ORDER BY symbol, effective_from"
        return [dict(r) for r in self.con.execute(sql, params)]

    def regime_book(self, symbol: str | None = None):
        """The persisted regimes as a `calendar.RegimeBook`."""
        from market_data.calendar import RegimeBook
        return RegimeBook.from_rows(self.load_regimes(symbol))

    # -- quarantine ---------------------------------------------------------
    #
    # A symbol the provider cannot serve at all.  Two things are written on
    # every state change: this table (the queryable list the live loop reads)
    # and a `corrections` row with `field='quarantine_status'` -- the exact
    # mechanism `market_data/repair/reconcile.py` already used for
    # `wrong_instrument`, so the audit trail stays single.
    def quarantine(self, symbol: str, *, reason: str, detail: str = "",
                   provider: str | None = None, run_id: str | None = None,
                   error: str | None = None, when: str | None = None) -> dict:
        """Quarantine `symbol` (idempotent; re-quarantining only re-checks it)."""
        if self.read_only:
            raise RuntimeError("store opened read-only")
        now = when or utcnow()
        have = self.get_quarantine(symbol)
        if have and have["status"] == "quarantined":
            return self.touch_quarantine(symbol, error=error, when=now)
        first_seen = (have or {}).get("first_seen") or now
        self.write(
            "INSERT OR REPLACE INTO quarantine (symbol,reason,detail,status,"
            "first_seen,last_checked,checks,last_error,released_at,provider,run_id) "
            "VALUES (?,?,?, 'quarantined', ?,?,?,?, NULL, ?,?)",
            (symbol, reason, detail, first_seen, now,
             int((have or {}).get("checks") or 0) + 1, error, provider, run_id))
        self.record_correction(
            symbol=symbol, timeframe="15minute",
            bar_start=self._quarantine_bar(symbol, now),
            field="quarantine_status",
            old_value=(have or {}).get("status") or "active",
            new_value=f"quarantined:{reason}",
            reason=detail or reason, run_id=run_id)
        return self.get_quarantine(symbol)

    def touch_quarantine(self, symbol: str, *, error: str | None = None,
                         when: str | None = None) -> dict:
        """Record that we re-checked `symbol` and it is still unavailable."""
        if self.read_only:
            raise RuntimeError("store opened read-only")
        self.write(
            "UPDATE quarantine SET last_checked=?, checks=checks+1, last_error=? "
            "WHERE symbol=?", (when or utcnow(), error, symbol))
        return self.get_quarantine(symbol)

    def release_quarantine(self, symbol: str, *, detail: str = "",
                           run_id: str | None = None,
                           when: str | None = None) -> dict | None:
        """The provider serves it again -- let it back into the universe."""
        if self.read_only:
            raise RuntimeError("store opened read-only")
        have = self.get_quarantine(symbol)
        if not have or have["status"] != "quarantined":
            return have
        now = when or utcnow()
        self.write(
            "UPDATE quarantine SET status='released', released_at=?, "
            "last_checked=?, checks=checks+1, last_error=NULL WHERE symbol=?",
            (now, now, symbol))
        self.record_correction(
            symbol=symbol, timeframe="15minute",
            bar_start=self._quarantine_bar(symbol, now),
            field="quarantine_status",
            old_value=f"quarantined:{have['reason']}", new_value="active",
            reason=detail or "the provider served this symbol again on re-check",
            run_id=run_id)
        return self.get_quarantine(symbol)

    def get_quarantine(self, symbol: str) -> dict | None:
        r = self.con.execute("SELECT * FROM quarantine WHERE symbol=?",
                             (symbol,)).fetchone()
        return dict(r) if r else None

    def quarantined(self, *, status: str | None = "quarantined") -> dict:
        """``{symbol: row}`` -- what the live universe should skip."""
        sql = "SELECT * FROM quarantine"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        return {r["symbol"]: dict(r) for r in self.con.execute(sql + " ORDER BY symbol",
                                                               params)}

    def _quarantine_bar(self, symbol: str, when: str) -> str:
        """The bar a whole-symbol quarantine is anchored to.

        `corrections.bar_start` is NOT NULL and means "the bar this is about".
        A symbol-wide decision has no single bar, so it is anchored to the last
        bar we will ever hold for it -- and, when we hold none, to the decision
        time itself rather than to an invented bar.
        """
        row = self.con.execute(
            "SELECT MAX(bar_start) FROM candles_15m WHERE symbol=?", (symbol,)).fetchone()
        return row[0] if row and row[0] else when.replace("T", " ")

    def get_meta(self, key: str) -> str | None:
        r = self.con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r[0] if r else None

    def set_meta(self, key: str, value: str) -> None:
        self.write("INSERT OR REPLACE INTO meta (key,value) VALUES (?,?)",
                   (key, value))


def _to_stored(r: sqlite3.Row) -> StoredCandle:
    return StoredCandle(
        instrument_id=r["instrument_id"], symbol=r["symbol"], exchange=r["exchange"],
        bar_start=datetime.fromisoformat(r["bar_start"]),
        bar_end=datetime.fromisoformat(r["bar_end"]),
        open=r["open"], high=r["high"], low=r["low"], close=r["close"],
        volume=r["volume"], candle_complete=bool(r["candle_complete"]),
        quality_flags=r["quality_flags"] or "",
        adjustment_basis_id=r["adjustment_basis_id"], vendor_id=r["vendor_id"],
        revision=r["revision"], snapshot_id=r["snapshot_id"])


def open_readonly(path: str | os.PathLike = DEFAULT_DB_PATH) -> MarketStore:
    return MarketStore(path, read_only=True)


def sha256_file(path: str | os.PathLike) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
