"""``db/derivatives.db`` — the F&O capture store (spec §2).

Properties that matter here, all of them the same ones ``market_data/store.py``
established for ``db/market15.db``:

* **One writer, WAL, readers never blocked.**  A writer takes the advisory lock
  file beside the database, so two capture loops cannot interleave.
* **A busy store is waited out, not fatal.**  Every write goes through
  ``retry_while_busy`` (reused, not re-implemented) so a long prune cannot kill
  the capture loop mid-session.
* **Idempotent by key.**  A snapshot row is keyed by (contract, mark), a candle
  by (contract, bar_start).  Re-running a mark replaces like with like and can
  never double-count; it also cannot invent a mark that was never quoted.
* **Provenance on every row** — ``vendor_id``, ``fetched_at``, ``snapshot_id``.

``db/kanida.db`` and ``db/market15.db`` are never opened here at all.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Sequence

from market_data.store import WriterLock, retry_while_busy

from . import config

LOG = logging.getLogger("market_data.derivatives.store")

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
DEFAULT_BUSY_TIMEOUT = 60.0
DEFAULT_WRITE_DEADLINE = 600.0

SNAPSHOT_COLUMNS = (
    "instrument_token", "captured_at", "mark_kind", "last_price", "average_price",
    "volume", "oi", "oi_day_high", "oi_day_low", "buy_quantity", "sell_quantity",
    "bid", "ask", "bid_quantity", "ask_quantity", "day_open", "day_high", "day_low",
    "prev_close", "last_trade_time", "exchange_time", "source", "vendor_id",
    "fetched_at", "snapshot_id", "average_price_est",
)

#: Retired 2026-09-19: written every mark, read by nothing that survives.  Each
#: one was traced to its readers before it qualified (the evidence is in the
#: `snapshots` comment in `schema.sql`); measured over the 265,601 rows then on
#: disk they were 90.99 of the record's 214.86 bytes.
#:
#: `bid`, `ask`, `buy_quantity` and `sell_quantity` do have a SELECT: they are
#: loaded into `metrics.Snapshot` and no field of that dataclass is ever read.
#: `fetched_at` and `snapshot_id` are not lost — they move to `captures`, one
#: row per mark, joined on `captured_at = mark_at`.
#:
#: This changes what is WRITTEN from now on.  Nothing drops a column and nothing
#: rewrites a row: an older database keeps its 26 columns and its history, and
#: `write_snapshots` keeps filling any of these that the live table declares
#: NOT NULL, because a legacy table would otherwise reject the insert.
RETIRED_SNAPSHOT_COLUMNS = frozenset({
    "exchange_time", "oi_day_high", "oi_day_low", "ask_quantity",
    "bid", "ask", "bid_quantity", "buy_quantity", "sell_quantity",
    "fetched_at", "snapshot_id",
})

#: Columns added after the first database was created, as (table, column, type).
#: `CREATE TABLE IF NOT EXISTS` cannot add a column to a table that exists, so
#: they are applied explicitly and idempotently on every open.
#:
#: `spot_source` (2026-09-20): a spot is not always the F&O vendor's own quote at
#: the mark.  Friday 18 Sep 2026 is the case that forced it — the capture died at
#: 11:30 and the afternoon was rebuilt afterwards, so the spots for 11:45 onward
#: had to come from our own equity store.  Without a source column a reader
#: cannot tell a captured spot from a reconstructed one, and the two are not the
#: same number.  NULL means "not recorded", never "captured".
MIGRATIONS = (
    ("snapshots", "average_price_est", "REAL"),
    ("underlying_snapshots", "spot_source", "TEXT"),
    ("metrics", "spot_source", "TEXT"),
)

UNDERLYING_COLUMNS = (
    "underlying", "captured_at", "mark_kind", "spot", "spot_symbol", "spot_source",
    "fut_price", "fut_token", "total_ce_oi", "total_pe_oi", "total_ce_volume",
    "total_pe_volume", "ce_contracts", "pe_contracts", "vendor_id", "fetched_at",
    "snapshot_id",
)

CANDLE_COLUMNS = (
    "instrument_token", "bar_start", "open", "high", "low", "close", "volume",
    "oi", "vendor_id", "fetched_at", "snapshot_id",
)

#: The vendor's own DAILY bar for the same contract.  Same shape as
#: CANDLE_COLUMNS with the session date in place of the bar start, so the two
#: writers stay recognisably the same thing at two cadences.
DAILY_CANDLE_COLUMNS = (
    "instrument_token", "session_date", "open", "high", "low", "close", "volume",
    "oi", "vendor_id", "fetched_at", "snapshot_id",
)

CONTRACT_COLUMNS = (
    "instrument_token", "tradingsymbol", "underlying", "instrument_type", "strike",
    "expiry", "lot_size", "tick_size", "exchange", "segment", "first_seen",
    "last_seen", "in_scope", "vendor_id", "fetched_at", "snapshot_id",
)


def utcnow() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def _placeholders(cols: Sequence[str]) -> str:
    return ", ".join("?" for _ in cols)


class DerivativesStore:
    """Read/write access to ``db/derivatives.db``."""

    def __init__(self, path: str | os.PathLike = config.DEFAULT_DB_PATH, *,
                 read_only: bool = False, timeout: float = DEFAULT_BUSY_TIMEOUT,
                 create: bool = True,
                 write_deadline: float = DEFAULT_WRITE_DEADLINE):
        self.path = Path(path)
        self.read_only = read_only
        self.write_deadline = write_deadline
        self.busy_retries = 0
        #: resolved once per open by `snapshot_write_columns`; a schema change
        #: goes through `init_schema`, which clears it.
        self._snapshot_columns: tuple[str, ...] | None = None
        if read_only:
            uri = f"file:{self.path.as_posix()}?mode=ro"
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            uri = f"file:{self.path.as_posix()}"
        self.con = sqlite3.connect(uri, uri=True, timeout=timeout, isolation_level=None)
        self.con.row_factory = sqlite3.Row
        self.con.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
        if read_only:
            self.con.execute("PRAGMA query_only=ON")
        else:
            self.con.execute("PRAGMA journal_mode=WAL")
            self.con.execute("PRAGMA synchronous=NORMAL")
            self.con.execute("PRAGMA temp_store=MEMORY")
            self.con.execute("PRAGMA cache_size=-131072")  # ~128 MB
            if create:
                self.init_schema()

    # ── plumbing ────────────────────────────────────────────────────────────

    def _on_retry(self, attempt: int, wait: float, exc: Exception) -> None:
        self.busy_retries += 1
        LOG.warning("%s is busy (%s); attempt %d, retrying in %.1fs",
                    self.path.name, exc, attempt, wait)

    def retrying(self, operation, **kw):
        return retry_while_busy(operation, deadline_seconds=self.write_deadline,
                                on_retry=self._on_retry, **kw)

    def write(self, sql: str, params: Sequence = ()) -> sqlite3.Cursor:
        return self.retrying(lambda: self.con.execute(sql, params))

    def init_schema(self) -> None:
        self._snapshot_columns = None
        self.retrying(lambda: self.con.executescript(
            SCHEMA_PATH.read_text(encoding="utf-8")))
        for table, column, coltype in MIGRATIONS:
            have = {r[1] for r in self.con.execute(f"PRAGMA table_info({table})")}
            if column not in have:
                LOG.info("adding %s.%s", table, column)
                self.write(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.retrying(lambda: self.con.execute("BEGIN IMMEDIATE"))
        try:
            yield self.con
        except Exception:
            # SQLite may already have rolled the transaction back itself (a
            # statement that hit the write lock does that), and then ROLLBACK
            # raises "cannot rollback - no transaction is active" — which would
            # replace the real failure with a misleading one.  That is exactly
            # what masked a busy-store error in the 09:30 cycle on 2026-09-18.
            try:
                self.con.execute("ROLLBACK")
            except sqlite3.OperationalError as rollback_error:
                LOG.debug("rollback was unnecessary (%s)", rollback_error)
            raise
        else:
            self.retrying(lambda: self.con.execute("COMMIT"))

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

    def writer_lock(self) -> WriterLock:
        """The advisory single-writer lock for this database file."""
        return WriterLock(self.path)

    # ── contracts ───────────────────────────────────────────────────────────

    def sync_contracts(self, contracts: Iterable, *, in_scope_tokens: Iterable[int] = (),
                       snapshot_id: str | None = None, seen_at: str | None = None) -> int:
        """Upsert the instrument list and re-flag the in-scope set.

        ``first_seen`` is preserved for a contract we already know; ``last_seen``
        always moves forward.  A contract that has left the dump keeps its row
        (its snapshots still point at it) and simply stops being in scope —
        which is the whole reason this store exists: expired contracts vanish
        from Kite.
        """
        seen_at = seen_at or utcnow()
        scope = set(int(t) for t in in_scope_tokens)
        rows = []
        for c in contracts:
            rows.append(c.as_row(in_scope=c.instrument_token in scope,
                                 snapshot_id=snapshot_id, first_seen=seen_at,
                                 fetched_at=seen_at))
        sql = (
            f"INSERT INTO contracts ({', '.join(CONTRACT_COLUMNS)}) "
            f"VALUES ({_placeholders(CONTRACT_COLUMNS)}) "
            "ON CONFLICT(instrument_token) DO UPDATE SET "
            "tradingsymbol=excluded.tradingsymbol, underlying=excluded.underlying, "
            "instrument_type=excluded.instrument_type, strike=excluded.strike, "
            "expiry=excluded.expiry, lot_size=excluded.lot_size, "
            "tick_size=excluded.tick_size, exchange=excluded.exchange, "
            "segment=excluded.segment, last_seen=excluded.last_seen, "
            "in_scope=excluded.in_scope, vendor_id=excluded.vendor_id, "
            "fetched_at=excluded.fetched_at"
        )
        with self.transaction() as con:
            con.execute("UPDATE contracts SET in_scope=0 WHERE in_scope=1")
            con.executemany(sql, rows)
        return len(rows)

    def scope_rows(self) -> list[sqlite3.Row]:
        return list(self.con.execute(
            "SELECT * FROM contracts WHERE in_scope=1 ORDER BY underlying, "
            "instrument_type, expiry, strike"))

    def contract(self, token: int) -> sqlite3.Row | None:
        cur = self.con.execute("SELECT * FROM contracts WHERE instrument_token=?", (int(token),))
        return cur.fetchone()

    def contract_count(self, *, in_scope: bool | None = None) -> int:
        sql = "SELECT COUNT(*) FROM contracts"
        params: tuple = ()
        if in_scope is not None:
            sql += " WHERE in_scope=?"
            params = (1 if in_scope else 0,)
        return int(self.con.execute(sql, params).fetchone()[0])

    # ── captures (the run ledger) ───────────────────────────────────────────

    def capture_begin(self, *, snapshot_id: str, mark_at: str, mark_kind: str,
                      session_date: str, planned: int) -> None:
        self.write(
            "INSERT INTO captures (snapshot_id, mark_at, mark_kind, session_date, "
            "started_at, status, contracts_planned, vendor_id) "
            "VALUES (?,?,?,?,?,'running',?,?) "
            "ON CONFLICT(snapshot_id) DO UPDATE SET started_at=excluded.started_at, "
            "status='running', contracts_planned=excluded.contracts_planned, error=NULL",
            (snapshot_id, mark_at, mark_kind, session_date, utcnow(), int(planned),
             config.VENDOR_ID))

    def capture_finish(self, snapshot_id: str, *, status: str, rows_written: int = 0,
                       rows_skipped: int = 0, underlyings_written: int = 0,
                       requests: int = 0, wall_seconds: float | None = None,
                       lag_seconds: float | None = None, error: str | None = None) -> None:
        self.write(
            "UPDATE captures SET finished_at=?, status=?, rows_written=?, "
            "rows_skipped=?, underlyings_written=?, requests=?, wall_seconds=?, "
            "lag_seconds=?, error=? WHERE snapshot_id=?",
            (utcnow(), status, int(rows_written), int(rows_skipped),
             int(underlyings_written), int(requests), wall_seconds, lag_seconds,
             error, snapshot_id))

    def record_missed(self, *, snapshot_id: str, mark_at: str, mark_kind: str,
                      session_date: str, reason: str) -> None:
        """A mark that went by uncaptured is recorded as missed, never filled."""
        self.write(
            "INSERT INTO captures (snapshot_id, mark_at, mark_kind, session_date, "
            "started_at, finished_at, status, error) VALUES (?,?,?,?,?,?, 'missed', ?) "
            "ON CONFLICT(snapshot_id) DO NOTHING",
            (snapshot_id, mark_at, mark_kind, session_date, utcnow(), utcnow(), reason))

    def captures_for(self, session_date: str) -> dict[str, sqlite3.Row]:
        cur = self.con.execute(
            "SELECT * FROM captures WHERE session_date=? ORDER BY mark_at", (session_date,))
        return {r["mark_at"]: r for r in cur}

    def capture(self, snapshot_id: str) -> sqlite3.Row | None:
        return self.con.execute("SELECT * FROM captures WHERE snapshot_id=?",
                                (snapshot_id,)).fetchone()

    def recent_captures(self, limit: int = 10) -> list[sqlite3.Row]:
        return list(self.con.execute(
            "SELECT * FROM captures ORDER BY mark_at DESC LIMIT ?", (int(limit),)))

    # ── rows ────────────────────────────────────────────────────────────────

    def snapshot_write_columns(self) -> tuple[str, ...]:
        """Which of ``SNAPSHOT_COLUMNS`` this database will actually be given.

        Two shapes have to work at once and neither may be guessed at:

        * a store created from today's ``schema.sql`` has no retired column, so
          they are simply absent from the INSERT;
        * a store created before 2026-09-19 still has all 26, and three of the
          retired ones (``fetched_at``, ``snapshot_id``) are declared NOT NULL
          there — omitting them would make every insert fail.  Those keep being
          written, and the fact is logged once so the gap between "retired" and
          "still costing bytes on this file" is never invisible.
        """
        if self._snapshot_columns is not None:
            return self._snapshot_columns
        info = list(self.con.execute("PRAGMA table_info(snapshots)"))
        live = {r[1] for r in info}
        required = {r[1] for r in info if r[3] and r[4] is None}
        keep, still_required = [], []
        for c in SNAPSHOT_COLUMNS:
            if c not in live:
                continue
            if c in RETIRED_SNAPSHOT_COLUMNS:
                if c not in required:
                    continue
                still_required.append(c)
            keep.append(c)
        if still_required:
            LOG.info("snapshots: %s are retired but this database declares them "
                     "NOT NULL, so they are still written; reclaiming them needs a "
                     "table rebuild (%s)", ", ".join(still_required), self.path.name)
        self._snapshot_columns = tuple(keep)
        return self._snapshot_columns

    def write_snapshots(self, rows: Sequence[Sequence]) -> int:
        """Write mark rows given in ``SNAPSHOT_COLUMNS`` order.

        Callers keep building the full tuple; this decides what of it the
        database gets.  A retired column that still exists on an older file is
        left alone on conflict rather than being overwritten with a NULL —
        re-capturing a mark must not rewrite history it no longer maintains.
        """
        if not rows:
            return 0
        cols = self.snapshot_write_columns()
        if cols != SNAPSHOT_COLUMNS:
            take = [SNAPSHOT_COLUMNS.index(c) for c in cols]
            rows = [tuple(r[i] for i in take) for r in rows]
        updatable = [c for c in cols if c not in ("instrument_token", "captured_at")]
        sql = (f"INSERT INTO snapshots ({', '.join(cols)}) "
               f"VALUES ({_placeholders(cols)}) "
               "ON CONFLICT(instrument_token, captured_at) DO UPDATE SET "
               + ", ".join(f"{c}=excluded.{c}" for c in updatable))
        with self.transaction() as con:
            con.executemany(sql, rows)
        return len(rows)

    def write_underlying_snapshots(self, rows: Sequence[Sequence]) -> int:
        if not rows:
            return 0
        sql = (f"INSERT INTO underlying_snapshots ({', '.join(UNDERLYING_COLUMNS)}) "
               f"VALUES ({_placeholders(UNDERLYING_COLUMNS)}) "
               "ON CONFLICT(underlying, captured_at) DO UPDATE SET "
               + ", ".join(f"{c}=excluded.{c}" for c in UNDERLYING_COLUMNS[2:]))
        with self.transaction() as con:
            con.executemany(sql, rows)
        return len(rows)

    def write_candles(self, rows: Sequence[Sequence]) -> int:
        if not rows:
            return 0
        sql = (f"INSERT INTO candles_15m ({', '.join(CANDLE_COLUMNS)}) "
               f"VALUES ({_placeholders(CANDLE_COLUMNS)}) "
               "ON CONFLICT(instrument_token, bar_start) DO UPDATE SET "
               + ", ".join(f"{c}=excluded.{c}" for c in CANDLE_COLUMNS[2:]))
        with self.transaction() as con:
            con.executemany(sql, rows)
        return len(rows)

    def write_daily_candles(self, rows: Sequence[Sequence]) -> int:
        """The vendor's own daily bars, keyed (contract, session_date).

        Idempotent exactly as ``write_candles`` is: re-running a day replaces
        like with like.  ``oi`` is written as it arrived — ``None`` stays
        ``None``, because a session whose open interest the vendor did not send
        is unknown, not zero.
        """
        if not rows:
            return 0
        sql = (f"INSERT INTO candles_day ({', '.join(DAILY_CANDLE_COLUMNS)}) "
               f"VALUES ({_placeholders(DAILY_CANDLE_COLUMNS)}) "
               "ON CONFLICT(instrument_token, session_date) DO UPDATE SET "
               + ", ".join(f"{c}=excluded.{c}" for c in DAILY_CANDLE_COLUMNS[2:]))
        with self.transaction() as con:
            con.executemany(sql, rows)
        return len(rows)

    def daily_candle_count(self, token: int | None = None) -> int:
        if token is None:
            return int(self.con.execute("SELECT COUNT(*) FROM candles_day").fetchone()[0])
        return int(self.con.execute(
            "SELECT COUNT(*) FROM candles_day WHERE instrument_token=?",
            (int(token),)).fetchone()[0])

    def latest_trading_mark(self, *, session_date: str | None = None) -> str | None:
        """The newest mark at which trading was actually happening.

        The post-close mark exists so the terminal 15:30 NFO bar is not lost,
        but nothing trades at it and the cash index has no bar there — so a
        screen built on it reads "no spot" and "nothing clears the floors",
        which is true and useless.  Anything rendering "as of" should ask for
        this instead of MAX(captured_at).
        """
        sql = ("SELECT MAX(captured_at) FROM snapshots WHERE mark_kind='bar_close'")
        params: tuple = ()
        if session_date:
            sql += " AND substr(captured_at,1,10)=?"
            params = (session_date,)
        row = self.con.execute(sql, params).fetchone()
        return row[0] if row and row[0] else None

    def snapshot_count(self, *, captured_at: str | None = None) -> int:
        if captured_at:
            return int(self.con.execute(
                "SELECT COUNT(*) FROM snapshots WHERE captured_at=?",
                (captured_at,)).fetchone()[0])
        return int(self.con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0])

    def read_snapshots(self, *, captured_at: str | None = None,
                       token: int | None = None) -> list[sqlite3.Row]:
        sql = "SELECT * FROM snapshots"
        where, params = [], []
        if captured_at:
            where.append("captured_at=?")
            params.append(captured_at)
        if token is not None:
            where.append("instrument_token=?")
            params.append(int(token))
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY captured_at, instrument_token"
        return list(self.con.execute(sql, params))

    def candle_count(self, token: int | None = None) -> int:
        if token is None:
            return int(self.con.execute("SELECT COUNT(*) FROM candles_15m").fetchone()[0])
        return int(self.con.execute(
            "SELECT COUNT(*) FROM candles_15m WHERE instrument_token=?",
            (int(token),)).fetchone()[0])

    # ── backfill bookkeeping ────────────────────────────────────────────────

    def mark_backfill(self, *, token: int, timeframe: str, from_date: str,
                      through_date: str, rows: int, status: str,
                      error: str | None = None, run_id: str | None = None) -> None:
        self.write(
            "INSERT INTO backfill_progress (instrument_token, timeframe, from_date, "
            "through_date, rows, status, error, run_id, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(instrument_token, timeframe, through_date) DO UPDATE SET "
            "from_date=excluded.from_date, rows=excluded.rows, status=excluded.status, "
            "error=excluded.error, run_id=excluded.run_id, updated_at=excluded.updated_at",
            (int(token), timeframe, from_date, through_date, int(rows), status,
             error, run_id, utcnow()))

    def backfill_done(self, *, timeframe: str, through_date: str) -> set[int]:
        """Tokens already backfilled through ``through_date`` — the resume set."""
        cur = self.con.execute(
            "SELECT instrument_token FROM backfill_progress WHERE timeframe=? "
            "AND through_date=? AND status IN ('ok','empty')", (timeframe, through_date))
        return {int(r[0]) for r in cur}

    def backfill_summary(self, run_id: str | None = None) -> dict:
        sql = ("SELECT status, COUNT(*) n, COALESCE(SUM(rows),0) rows "
               "FROM backfill_progress")
        params: tuple = ()
        if run_id:
            sql += " WHERE run_id=?"
            params = (run_id,)
        sql += " GROUP BY status"
        return {r["status"]: {"contracts": r["n"], "rows": r["rows"]}
                for r in self.con.execute(sql, params)}

    # ── runs ────────────────────────────────────────────────────────────────

    def start_run(self, run_id: str, kind: str, detail: str = "") -> str:
        self.write("INSERT INTO runs (run_id, kind, started_at, status, detail) "
                   "VALUES (?,?,?,'running',?) ON CONFLICT(run_id) DO UPDATE SET "
                   "started_at=excluded.started_at, status='running'",
                   (run_id, kind, utcnow(), detail))
        return run_id

    def finish_run(self, run_id: str, *, status: str = "ok", requests: int = 0,
                   rows: int = 0, detail: str | None = None) -> None:
        self.write("UPDATE runs SET finished_at=?, status=?, requests=?, rows=?, "
                   "detail=COALESCE(?, detail) WHERE run_id=?",
                   (utcnow(), status, int(requests), int(rows), detail, run_id))

    # ── meta ────────────────────────────────────────────────────────────────

    def get_meta(self, key: str) -> str | None:
        row = self.con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self.write("INSERT INTO meta (key, value) VALUES (?,?) "
                   "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

    # ── daily roll-ups and retention (spec §2) ──────────────────────────────

    def rollup_day(self, session_date: str) -> int:
        """Build the keep-for-good daily row for one session.

        Built from ``candles_15m`` where they exist (they are the exchange's own
        bars); otherwise from that session's snapshots, which is a coarser but
        honest summary — ``built_from`` says which, on every row.
        """
        with self.transaction() as con:
            cur = con.execute(
                """
                INSERT INTO daily_rollups (instrument_token, session_date, underlying,
                    instrument_type, expiry, strike, open, high, low, close, volume,
                    oi_open, oi_close, premium_rs, bars, marks, built_from, vendor_id,
                    fetched_at)
                SELECT c.instrument_token, ?, k.underlying, k.instrument_type, k.expiry,
                       k.strike,
                       (SELECT open  FROM candles_15m x WHERE x.instrument_token=c.instrument_token
                          AND substr(x.bar_start,1,10)=? ORDER BY x.bar_start LIMIT 1),
                       MAX(c.high), MIN(c.low),
                       (SELECT close FROM candles_15m x WHERE x.instrument_token=c.instrument_token
                          AND substr(x.bar_start,1,10)=? ORDER BY x.bar_start DESC LIMIT 1),
                       SUM(c.volume),
                       (SELECT oi FROM candles_15m x WHERE x.instrument_token=c.instrument_token
                          AND substr(x.bar_start,1,10)=? ORDER BY x.bar_start LIMIT 1),
                       (SELECT oi FROM candles_15m x WHERE x.instrument_token=c.instrument_token
                          AND substr(x.bar_start,1,10)=? ORDER BY x.bar_start DESC LIMIT 1),
                       NULL, COUNT(*), NULL, 'candles_15m', ?, ?
                FROM candles_15m c
                LEFT JOIN contracts k ON k.instrument_token = c.instrument_token
                WHERE substr(c.bar_start,1,10) = ?
                GROUP BY c.instrument_token
                ON CONFLICT(instrument_token, session_date) DO UPDATE SET
                    open=excluded.open, high=excluded.high, low=excluded.low,
                    close=excluded.close, volume=excluded.volume,
                    oi_open=excluded.oi_open, oi_close=excluded.oi_close,
                    bars=excluded.bars, built_from=excluded.built_from,
                    fetched_at=excluded.fetched_at
                """,
                (session_date, session_date, session_date, session_date, session_date,
                 config.VENDOR_ID, utcnow(), session_date))
            from_candles = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

            cur2 = con.execute(
                """
                INSERT INTO daily_rollups (instrument_token, session_date, underlying,
                    instrument_type, expiry, strike, open, high, low, close, volume,
                    oi_open, oi_close, premium_rs, bars, marks, built_from, vendor_id,
                    fetched_at)
                SELECT s.instrument_token, ?, k.underlying, k.instrument_type, k.expiry,
                       k.strike,
                       (SELECT day_open FROM snapshots x WHERE x.instrument_token=s.instrument_token
                          AND substr(x.captured_at,1,10)=? ORDER BY x.captured_at DESC LIMIT 1),
                       MAX(s.day_high), MIN(s.day_low),
                       (SELECT last_price FROM snapshots x WHERE x.instrument_token=s.instrument_token
                          AND substr(x.captured_at,1,10)=? ORDER BY x.captured_at DESC LIMIT 1),
                       MAX(s.volume),
                       (SELECT oi FROM snapshots x WHERE x.instrument_token=s.instrument_token
                          AND substr(x.captured_at,1,10)=? ORDER BY x.captured_at LIMIT 1),
                       (SELECT oi FROM snapshots x WHERE x.instrument_token=s.instrument_token
                          AND substr(x.captured_at,1,10)=? ORDER BY x.captured_at DESC LIMIT 1),
                       NULL, NULL, COUNT(*), 'snapshots', ?, ?
                FROM snapshots s
                LEFT JOIN contracts k ON k.instrument_token = s.instrument_token
                WHERE substr(s.captured_at,1,10) = ?
                  AND s.instrument_token NOT IN (
                        SELECT instrument_token FROM daily_rollups WHERE session_date=?)
                GROUP BY s.instrument_token
                """,
                (session_date, session_date, session_date, session_date, session_date,
                 config.VENDOR_ID, utcnow(), session_date, session_date))
            from_snaps = cur2.rowcount if cur2.rowcount and cur2.rowcount > 0 else 0

            # premium traded for the candle-built rows, from the same session's
            # snapshots when we have them (volume × average_price, spec §3.4).
            con.execute(
                """
                UPDATE daily_rollups SET
                  premium_rs = COALESCE(premium_rs, (
                     SELECT MAX(y.volume * y.average_price) FROM snapshots y
                      WHERE y.instrument_token = daily_rollups.instrument_token
                        AND substr(y.captured_at,1,10) = ?)),
                  marks = COALESCE(marks, (
                     SELECT COUNT(*) FROM snapshots y
                      WHERE y.instrument_token = daily_rollups.instrument_token
                        AND substr(y.captured_at,1,10) = ?))
                WHERE session_date = ?
                """,
                (session_date, session_date, session_date))
        return from_candles + from_snaps

    # ── retention ───────────────────────────────────────────────────────────

    def serving_session(self) -> str | None:
        """The session the Derivative tab is reading right now, or None.

        The tab's "as of" is ``latest_trading_mark()``; every screen it draws is
        built from that mark's session.  Retention uses it as a floor: a cutoff
        that reaches this day would delete the readings under the tab's feet.
        """
        mark = self.latest_trading_mark()
        return mark[:10] if mark else None

    def _days_before(self, table: str, column: str, cutoff: str) -> list[str]:
        """The distinct session days in `table` strictly older than `cutoff`."""
        return [r[0] for r in self.con.execute(
            f"SELECT DISTINCT substr({column},1,10) d FROM {table} "
            f"WHERE {column} < ? ORDER BY d", (cutoff,))]

    def _delete_day(self, table: str, column: str, day: str) -> int:
        """Delete one session day, in its own write transaction.

        One day at a time on purpose.  ``market_data/store.py`` records how the
        live ingest loop died on 2026-09-16: a maintenance pass held the single
        write lock for minutes.  A day is one range scan on the table's own time
        index, and the writer is handed back between days, so a capture mark
        that falls due mid-prune waits seconds rather than minutes.
        """
        nxt = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        # End any read transaction the scan above left open before asking for
        # the writer: in WAL a read snapshot cannot be upgraded, SQLite answers
        # BUSY at once and busy_timeout never gets its chance.  Same order as
        # `metrics.write_metric_rows`.  `transaction()` then takes BEGIN
        # IMMEDIATE through `retry_while_busy`, which waits out a capture cycle.
        self.con.rollback()
        with self.transaction() as con:
            cur = con.execute(
                f"DELETE FROM {table} WHERE {column} >= ? AND {column} < ?", (day, nxt))
            n = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        if n:
            LOG.info("retention: %s %s -- %d rows removed", table, day, n)
        return n

    def prune(self, *, today: date | None = None,
              raw_days: int = config.RAW_SNAPSHOT_DAYS,
              metric_days: int = config.METRICS_DAYS,
              candle_days: int | None = config.CANDLE_DAYS,
              underlying_days: int | None = None,
              rollup_first: bool = True,
              dry_run: bool = False) -> dict:
        """Retention (spec §2, revised by ``kanida-app/docs/STORAGE_PLAN.md`` §5.5).

        Windows, in days, all of them named constants in ``config``::

            snapshots            RAW_SNAPSHOT_DAYS         the raw input
            metrics              METRICS_DAYS              what the tab serves
            candles_15m          CANDLE_DAYS               was never pruned at all
            underlying_snapshots UNDERLYING_SNAPSHOT_DAYS  2,013 rows; barely matters
            daily_rollups        kept for good             expired contracts never come back
            contracts            kept for good             needed to read expiries back
            captures             kept for good             the only record of a quiet mark

        Three things this will not do.

        1. **It never rolls resolution away without keeping the day.** Every
           session about to lose raw rows or candles is rolled up into
           ``daily_rollups`` first, so pruning costs resolution, never the day.
        2. **It never touches what the tab is serving.** Each cutoff is checked
           against :meth:`serving_session`; a window that would reach the live
           session raises rather than running.  A window shorter than
           ``config.MIN_RETENTION_DAYS`` raises too, so a typo cannot empty the
           store.
        3. **It is never silent.** Every day removed is logged with its row
           count, and a pass that removed nothing says so.

        ``dry_run=True`` counts and logs without deleting.
        """
        today = today or date.today()
        windows = {
            "snapshots": raw_days,
            "metrics": metric_days,
            "candles_15m": candle_days,
            "underlying_snapshots": (underlying_days
                                     if underlying_days is not None
                                     else config.UNDERLYING_SNAPSHOT_DAYS),
        }
        column = {"snapshots": "captured_at", "metrics": "captured_at",
                  "candles_15m": "bar_start", "underlying_snapshots": "captured_at"}
        cutoff: dict[str, str] = {}
        serving = self.serving_session()
        for table, days in windows.items():
            if days is None:
                continue
            if days < config.MIN_RETENTION_DAYS:
                raise ValueError(
                    f"retention window for {table} is {days} days; the floor is "
                    f"{config.MIN_RETENTION_DAYS} (config.MIN_RETENTION_DAYS)")
            cut = (today - timedelta(days=days)).isoformat()
            if serving is not None and cut > serving:
                raise ValueError(
                    f"retention cutoff for {table} is {cut}, which is not older than "
                    f"the session the Derivative tab is serving ({serving}); refusing")
            cutoff[table] = cut

        out: dict = {"today": today.isoformat(), "serving_session": serving,
                     "dry_run": bool(dry_run), "cutoffs": cutoff,
                     # the two the caller had before candles joined the pass
                     "raw_cutoff": cutoff.get("snapshots"),
                     "metrics_cutoff": cutoff.get("metrics"),
                     "candles_cutoff": cutoff.get("candles_15m"),
                     "rolled_up_sessions": 0, "snapshots_deleted": 0,
                     "underlying_deleted": 0, "metrics_deleted": 0,
                     "candles_deleted": 0}

        # Roll up every session that is about to lose detail, from either source.
        if rollup_first and not dry_run:
            days_to_roll = set()
            if "snapshots" in cutoff:
                days_to_roll |= set(self._days_before(
                    "snapshots", "captured_at", cutoff["snapshots"]))
            if "candles_15m" in cutoff:
                days_to_roll |= set(self._days_before(
                    "candles_15m", "bar_start", cutoff["candles_15m"]))
            for d in sorted(days_to_roll):
                self.rollup_day(d)
            out["rolled_up_sessions"] = len(days_to_roll)
            if days_to_roll:
                LOG.info("retention: rolled up %d session(s) before deleting anything",
                         len(days_to_roll))

        key = {"snapshots": "snapshots_deleted", "metrics": "metrics_deleted",
               "candles_15m": "candles_deleted",
               "underlying_snapshots": "underlying_deleted"}
        for table, cut in cutoff.items():
            days = self._days_before(table, column[table], cut)
            if not days:
                continue
            total = 0
            for d in days:
                if dry_run:
                    nxt = (date.fromisoformat(d) + timedelta(days=1)).isoformat()
                    total += int(self.con.execute(
                        f"SELECT COUNT(*) FROM {table} "
                        f"WHERE {column[table]} >= ? AND {column[table]} < ?",
                        (d, nxt)).fetchone()[0])
                else:
                    total += self._delete_day(table, column[table], d)
            out[key[table]] = total
            LOG.info("retention: %s %s %d row(s) across %d session(s) older than %s",
                     "would remove" if dry_run else "removed", table, total, len(days), cut)

        removed = sum(out[k] for k in key.values())
        if not removed:
            LOG.info("retention: nothing older than %s; store unchanged",
                     ", ".join(f"{t} {c}" for t, c in sorted(cutoff.items())))
        return out

    # ── status ──────────────────────────────────────────────────────────────

    def status(self) -> dict:
        def one(sql, *params):
            row = self.con.execute(sql, params).fetchone()
            return row[0] if row else None

        size = self.path.stat().st_size if self.path.exists() else 0
        return {
            "db": str(self.path),
            "size_mb": round(size / 1e6, 1),
            "contracts": self.contract_count(),
            "contracts_in_scope": self.contract_count(in_scope=True),
            "snapshot_rows": one("SELECT COUNT(*) FROM snapshots") or 0,
            "snapshot_marks": one("SELECT COUNT(DISTINCT captured_at) FROM snapshots") or 0,
            "first_mark": one("SELECT MIN(captured_at) FROM snapshots"),
            "last_mark": one("SELECT MAX(captured_at) FROM snapshots"),
            "latest_trading_mark": self.latest_trading_mark(),
            "candle_rows": one("SELECT COUNT(*) FROM candles_15m") or 0,
            "candle_contracts": one("SELECT COUNT(DISTINCT instrument_token) FROM candles_15m") or 0,
            "candle_first": one("SELECT MIN(bar_start) FROM candles_15m"),
            "candle_last": one("SELECT MAX(bar_start) FROM candles_15m"),
            "daily_candle_rows": one("SELECT COUNT(*) FROM candles_day") or 0,
            "daily_candle_contracts": one("SELECT COUNT(DISTINCT instrument_token) FROM candles_day") or 0,
            "daily_candle_first": one("SELECT MIN(session_date) FROM candles_day"),
            "daily_candle_last": one("SELECT MAX(session_date) FROM candles_day"),
            "underlying_rows": one("SELECT COUNT(*) FROM underlying_snapshots") or 0,
            "metric_rows": one("SELECT COUNT(*) FROM metrics") or 0,
            "rollup_rows": one("SELECT COUNT(*) FROM daily_rollups") or 0,
            "captures_ok": one("SELECT COUNT(*) FROM captures WHERE status='ok'") or 0,
            "captures_missed": one("SELECT COUNT(*) FROM captures WHERE status='missed'") or 0,
            "captures_error": one("SELECT COUNT(*) FROM captures WHERE status IN ('error','partial')") or 0,
        }


def apply_migrations(con: sqlite3.Connection) -> list[str]:
    """Apply `MIGRATIONS` to an already-open connection; return what was added.

    `DerivativesStore.init_schema` is the usual route, but it takes the store's
    own connection and its whole schema script with it.  A tool that holds a
    plain connection to the same file -- the metrics worker and the spot
    backfill both do, because the advisory writer lock belongs to the capture
    loop -- needs the columns without the rest.  Idempotent: a column that is
    already there is left alone, and an absent TABLE is skipped rather than
    created, because creating one here would be this function inventing a schema
    it does not own.
    """
    added: list[str] = []
    for table, column, coltype in MIGRATIONS:
        info = list(con.execute(f"PRAGMA table_info({table})"))
        if not info:
            continue
        if column in {r[1] for r in info}:
            continue
        LOG.info("adding %s.%s", table, column)
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
        added.append(f"{table}.{column}")
    if added:
        con.commit()
    return added


def open_readonly(path: str | os.PathLike = config.DEFAULT_DB_PATH) -> DerivativesStore:
    return DerivativesStore(path, read_only=True, create=False)


__all__ = ["DerivativesStore", "open_readonly", "apply_migrations", "SNAPSHOT_COLUMNS",
           "RETIRED_SNAPSHOT_COLUMNS",
           "UNDERLYING_COLUMNS", "CANDLE_COLUMNS", "DAILY_CANDLE_COLUMNS",
           "CONTRACT_COLUMNS", "utcnow"]
