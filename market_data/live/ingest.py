"""Incremental live ingest: NIFTY 500 -> `db/market15.db` (contract sections 1, 2, 2A, 5).

One cycle, per symbol:

1. **Plan** -- ask the store for the symbol's last stored ``bar_start`` and ask
   the provider for the newest bar that is complete *and* published
   (``latest_completed_bar``, which honours the vendor's ``delay_seconds``).
   The window to request is everything strictly after the former up to and
   including the latter.  Nothing to fetch is the normal case and costs zero
   requests.
2. **Fetch** through the provider, which serialises every call through the one
   process-wide token bucket (Kite ~3 req/s).
3. **Write** through ``market_data.store``:
   * a bar we have never seen is written at revision 1;
   * a bar whose values the vendor has *restated* is written as a **new
     revision** with one ``corrections`` row per changed field -- never an
     in-place edit;
   * a bar that is byte-identical to what we hold is not rewritten at all,
     which is what makes re-running a cycle free and idempotent.
4. **Daily bars** -- after the session has closed, the provider's own ``day``
   bar for each symbol.  This is not a convenience: from 2026-08-03 a CAS
   stock's official close is the 15:30-15:35 auction price, which *does not
   exist anywhere in the intraday series* (contract 2A).  1D/1W candles must
   come from these bars.
5. **Session regimes** -- which symbols stopped continuous trading at 15:15 is
   derived from the bars themselves (``RegimeBook.from_observed_last_bars``)
   and persisted, so the expected bar count stays per-symbol-per-date.
6. **Quarantine** -- a symbol the provider cannot serve at all (not in its
   instrument list) is skipped, because asking for it every cycle produced one
   error per cycle per symbol and nothing else.  That error count is what
   ``/api/state.data_status.last_run.errors`` publishes and what the app's data
   panel turns into "some symbols may be behind", so leaving it in place makes
   the app warn about a problem that does not exist.  The skip is **not**
   permanent: a quarantined symbol is re-probed once a day
   (``quarantine_recheck_hours``) and released the moment the provider serves
   it, and a failed re-probe is recorded on the quarantine row rather than
   counted as a cycle error.  ``include_quarantined=True`` disables the skip.

Crash safety and resumability come from the plan being a pure function of what
is already in the store: kill the loop at any point and the next cycle asks for
exactly the bars that are still missing.  ``db/kanida.db`` is opened read-only
for the universe and never written.

No credential, token or token fingerprint is logged here; the provider owns
that and already redacts it.
"""

from __future__ import annotations

import logging
import threading
import time as _time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Iterable, Sequence

from market_data.aggregate import Bar, bar_from_raw
from market_data.calendar import (
    CAS_START,
    REGIME_CAS,
    RegimeBook,
    SessionCalendar,
)
from market_data.live.calendar_ext import SymbolInfo, live_calendar, nifty500
from market_data.quarantine import DEFAULT_RECHECK_HOURS, due_symbols
from market_data.store import MarketStore, utcnow
from market_data.types import IST, ProviderError, to_ist

LOG = logging.getLogger("market_data.live")


def now_ist() -> datetime:
    """Naive IST -- the only clock this module uses.

    ``datetime.now()`` is the *machine's* local time, which is not IST on any
    of our boxes.  Using it silently puts the loop a whole session out (it
    reads 21:04 PDT as 21:04 IST and concludes the market closed hours ago), so
    the IST conversion is explicit and centralised here.
    """
    return datetime.now(IST).replace(tzinfo=None)

BASE_TIMEFRAME = "15minute"
DAILY_TIMEFRAME = "day"
BAR = timedelta(minutes=15)

#: Extra seconds after a bar's end before we believe the vendor has published
#: it.  Independent of, and added to, the provider's own ``delay_seconds``.
DEFAULT_SETTLE_SECONDS = 60

DAILY_TABLE = "daily_bars"

#: Additive -- it creates one new table and touches nothing W2 owns.  The
#: contract's section 2 does not name a daily table; 2A requires one, because
#: a CAS stock's official close exists only in the provider's daily bar.
LIVE_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
    instrument_id       INTEGER NOT NULL,
    symbol              TEXT    NOT NULL,
    exchange            TEXT    NOT NULL DEFAULT 'NSE',
    session_date        TEXT    NOT NULL,      -- 'YYYY-MM-DD'
    bar_start           TEXT    NOT NULL,      -- session open, naive IST
    bar_end             TEXT    NOT NULL,      -- session close, naive IST
    open                REAL    NOT NULL,
    high                REAL    NOT NULL,
    low                 REAL    NOT NULL,
    close               REAL    NOT NULL,
    volume              INTEGER NOT NULL,
    quality_flags       TEXT    NOT NULL DEFAULT '',
    adjustment_basis_id TEXT    NOT NULL,
    vendor_id           TEXT    NOT NULL,
    fetched_at          TEXT,
    run_id              TEXT,
    revision            INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (instrument_id, session_date, revision)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_daily_sym_day ON {DAILY_TABLE} (symbol, session_date);
"""

META_LATEST_BAR = "live.latest_bar_start"
META_LATEST_END = "live.latest_bar_end"
META_CYCLE_AT = "live.last_cycle_finished_at"
META_CYCLE_STATS = "live.last_cycle"


def ensure_live_schema(store: MarketStore) -> None:
    """Create the live-only tables.  Idempotent; safe beside the seeder.

    ``CREATE TABLE IF NOT EXISTS`` takes the write lock even when it creates
    nothing, so this goes through the store's busy-retry: constructing a
    ``LiveIngest`` while a maintenance pass holds the lock must not raise before
    the loop has done anything.
    """
    if store.read_only:
        return
    store.retrying(lambda: store.con.executescript(LIVE_SCHEMA))


# ---------------------------------------------------------------------------
# planning (pure)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SymbolPlan:
    """What one symbol needs this cycle.  A pure function of stored state."""

    symbol: str
    instrument_id: int
    start: datetime | None       # first bar_start to request (naive IST)
    end: datetime | None         # last bar_start to request (naive IST)
    last_stored: datetime | None
    reason: str                  # append | backfill | up_to_date | no_session

    @property
    def wanted(self) -> bool:
        return self.start is not None and self.end is not None and self.start <= self.end

    def __bool__(self) -> bool:  # pragma: no cover - convenience
        return self.wanted


def plan_symbol(symbol: str, instrument_id: int, last_stored: datetime | None,
                latest_bar: datetime | None, calendar: SessionCalendar,
                backfill_sessions: int = 10) -> SymbolPlan:
    """Decide the window to request for `symbol`.

    * `last_stored` -- newest ``bar_start`` already in the store, or None.
    * `latest_bar`  -- newest ``bar_start`` the provider considers complete and
      published (see :func:`latest_completed_bar`).

    A symbol with no history at all is backfilled from `backfill_sessions`
    sessions back -- enough for the live loop to be useful on its own, never a
    substitute for the seed.
    """
    if latest_bar is None:
        return SymbolPlan(symbol, instrument_id, None, None, last_stored, "no_session")
    if last_stored is None:
        days = calendar.trading_days(latest_bar.date() - timedelta(days=backfill_sessions * 3),
                                     latest_bar.date())
        days = days[-backfill_sessions:] if days else []
        if not days:
            return SymbolPlan(symbol, instrument_id, None, None, None, "no_session")
        session = calendar.session(days[0])
        start = session.start if session else datetime.combine(days[0], datetime.min.time())
        return SymbolPlan(symbol, instrument_id, start, latest_bar, None, "backfill")
    start = last_stored + BAR
    if start > latest_bar:
        return SymbolPlan(symbol, instrument_id, None, None, last_stored, "up_to_date")
    return SymbolPlan(symbol, instrument_id, start, latest_bar, last_stored, "append")


def latest_completed_bar(provider, calendar: SessionCalendar,
                         now: datetime | None = None) -> datetime | None:
    """Newest ``bar_start`` that is complete, published *and* on a real session.

    The provider answers the delay question (a 15-minute delayed vendor is
    "delayed by design", not stale).  The calendar answers the holiday question
    -- the provider layer deliberately knows only weekends, so its own answer
    can point at a holiday.  We take the provider's bar and walk it back onto
    the newest session the calendar actually has.
    """
    now = now or now_ist()
    try:
        raw = to_ist(provider.latest_completed_bar(BASE_TIMEFRAME, now)).replace(tzinfo=None)
    except ProviderError:
        return None
    cutoff = now - timedelta(seconds=getattr(provider, "delay_seconds", 0))
    day = raw.date()
    for _ in range(21):
        session = calendar.session(day)
        if session is not None:
            for start in reversed(session.bar_starts()):
                if start <= raw and start + BAR <= cutoff:
                    return start
        day -= timedelta(days=1)
    return None


# ---------------------------------------------------------------------------
# results
# ---------------------------------------------------------------------------
@dataclass
class SymbolResult:
    symbol: str
    status: str = "ok"           # ok | up_to_date | error | no_session
    requests: int = 0
    fetched: int = 0
    written: int = 0
    restated: int = 0
    skipped_incomplete: int = 0
    daily_written: int = 0
    last_bar: datetime | None = None
    error: str | None = None


@dataclass
class CycleResult:
    run_id: str
    started_at: str
    finished_at: str = ""
    symbols: int = 0
    considered: int = 0
    requests: int = 0
    rows: int = 0
    restated: int = 0
    daily_rows: int = 0
    errors: int = 0
    up_to_date: int = 0
    seconds: float = 0.0
    latest_bar: datetime | None = None
    regimes_recorded: int = 0
    quarantined_skipped: int = 0
    quarantine_rechecked: int = 0
    quarantine_released: int = 0
    quarantine_still_unavailable: int = 0
    busy_waits: int = 0
    per_symbol: list[SymbolResult] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "run_id": self.run_id, "started_at": self.started_at,
            "finished_at": self.finished_at, "symbols": self.symbols,
            "considered": self.considered, "requests": self.requests,
            "rows": self.rows, "restated": self.restated,
            "daily_rows": self.daily_rows, "errors": self.errors,
            "up_to_date": self.up_to_date, "seconds": round(self.seconds, 2),
            "latest_bar": self.latest_bar.isoformat(sep=" ") if self.latest_bar else None,
            "regimes_recorded": self.regimes_recorded,
            "quarantined_skipped": self.quarantined_skipped,
            "quarantine_rechecked": self.quarantine_rechecked,
            "quarantine_released": self.quarantine_released,
            "quarantine_still_unavailable": self.quarantine_still_unavailable,
            "busy_waits": self.busy_waits,
        }


# ---------------------------------------------------------------------------
# daily bars
# ---------------------------------------------------------------------------
def read_daily_bars(store: MarketStore, symbol: str, start: date | None = None,
                    end: date | None = None) -> list[Bar]:
    """Latest-revision daily bars for `symbol` as aggregate ``Bar``s."""
    sql = (f"SELECT bar_start, bar_end, open, high, low, close, volume, "
           f"quality_flags FROM {DAILY_TABLE} d WHERE symbol=?")
    params: list = [symbol]
    if start is not None:
        sql += " AND session_date>=?"
        params.append(start.isoformat())
    if end is not None:
        sql += " AND session_date<=?"
        params.append(end.isoformat())
    sql += (" AND revision=(SELECT MAX(d2.revision) FROM " + DAILY_TABLE + " d2 "
            "WHERE d2.instrument_id=d.instrument_id AND d2.session_date=d.session_date)"
            " ORDER BY session_date")
    out: list[Bar] = []
    for r in store.con.execute(sql, params):
        out.append(Bar(datetime.fromisoformat(r[0]), datetime.fromisoformat(r[1]),
                       float(r[2]), float(r[3]), float(r[4]), float(r[5]),
                       int(r[6] or 0), True, r[7] or ""))
    return out


def daily_coverage(store: MarketStore, symbol: str) -> tuple[int, date | None]:
    row = store.con.execute(
        f"SELECT COUNT(*), MAX(session_date) FROM {DAILY_TABLE} WHERE symbol=?",
        (symbol,)).fetchone()
    n = int(row[0] or 0)
    return n, date.fromisoformat(row[1]) if row[1] else None


# ---------------------------------------------------------------------------
# the loop
# ---------------------------------------------------------------------------
class LiveIngest:
    """One cycle = one incremental pass over the universe.  One writer."""

    def __init__(self, store: MarketStore, provider, universe: Sequence[SymbolInfo],
                 calendar: SessionCalendar, *,
                 backfill_sessions: int = 10,
                 daily_history_days: int = 2200,
                 min_daily_history: int = 200,
                 workers: int = 4,
                 settle_seconds: int = DEFAULT_SETTLE_SECONDS,
                 fetch_daily: bool = True,
                 include_quarantined: bool = False,
                 quarantine_recheck_hours: float = DEFAULT_RECHECK_HOURS,
                 clock: Callable[[], datetime] = now_ist):
        if store.read_only:
            raise RuntimeError("live ingest needs a writable store")
        self.store = store
        self.provider = provider
        self.universe = list(universe)
        self.calendar = calendar
        self.backfill_sessions = backfill_sessions
        self.daily_history_days = daily_history_days
        self.min_daily_history = min_daily_history
        self.workers = max(1, int(workers))
        self.settle_seconds = settle_seconds
        self.fetch_daily = fetch_daily
        self.include_quarantined = include_quarantined
        self.quarantine_recheck_hours = quarantine_recheck_hours
        self.clock = clock
        self.vendor_id = getattr(provider, "provider_id", "unknown")
        self.adjustment_basis_id = getattr(provider, "adjustment_basis_id",
                                           f"{self.vendor_id}-unknown")
        ensure_live_schema(store)

    # -- session awareness --------------------------------------------------
    def session_state(self, now: datetime | None = None) -> dict:
        """Where we are relative to today's session (naive IST throughout)."""
        now = now or self.clock()
        session = self.calendar.session(now.date())
        if session is None:
            nxt = self.calendar.next_session(now.date())
            return {"open": False, "phase": "closed", "session": None,
                    "next_open": nxt.start if nxt else None}
        settle = timedelta(seconds=self.settle_seconds)
        if now < session.start:
            return {"open": False, "phase": "pre_open", "session": session,
                    "next_open": session.start}
        if now < session.end + settle:
            return {"open": True, "phase": "in_session", "session": session,
                    "next_open": session.start}
        nxt = self.calendar.next_session(now.date())
        return {"open": False, "phase": "post_close", "session": session,
                "next_open": nxt.start if nxt else None}

    # -- reads --------------------------------------------------------------
    def last_stored(self, symbol: str) -> datetime | None:
        row = self.store.con.execute(
            "SELECT MAX(bar_start) FROM candles_15m WHERE symbol=?", (symbol,)).fetchone()
        return datetime.fromisoformat(row[0]) if row and row[0] else None

    def _existing(self, symbol: str, start: datetime, end: datetime) -> dict:
        rows = self.store.read_window(symbol, start, end + BAR)
        return {c.bar_start: c for c in rows}

    # -- quarantine ---------------------------------------------------------
    def _quarantine_rows(self) -> dict:
        """The persisted quarantine, or empty if this store has no table yet."""
        try:
            return self.store.quarantined()
        except Exception:  # noqa: BLE001 - an older store must still ingest
            LOG.debug("no quarantine table in %s; nothing is skipped", self.store.path)
            return {}

    def effective_universe(self) -> tuple[list[SymbolInfo], set, int]:
        """`(universe_to_fetch, symbols_being_re-probed, symbols_skipped)`.

        A quarantined symbol is dropped from the cycle except on the day its
        re-check falls due, when it goes back in so the quarantine can end by
        evidence instead of by hand.
        """
        if self.include_quarantined:
            return list(self.universe), set(), 0
        rows = self._quarantine_rows()
        if not rows:
            return list(self.universe), set(), 0
        due = due_symbols(rows, hours=self.quarantine_recheck_hours)
        keep, skipped = [], 0
        for info in self.universe:
            if info.symbol in rows and info.symbol not in due:
                skipped += 1
                continue
            keep.append(info)
        return keep, {i.symbol for i in keep if i.symbol in rows}, skipped

    # -- one symbol ---------------------------------------------------------
    def _fetch(self, plan: SymbolPlan) -> tuple[SymbolPlan, list, str | None]:
        """Provider I/O only -- runs on the pool, writes nothing."""
        try:
            bars = self.provider.candles(plan.symbol, BASE_TIMEFRAME, plan.start, plan.end)
        except Exception as exc:  # noqa: BLE001 - reported, never fabricated over
            return plan, [], f"{type(exc).__name__}: {exc}"
        return plan, bars, None

    def _apply(self, plan: SymbolPlan, raw_bars: Sequence, run_id: str) -> SymbolResult:
        """The single-writer half: compare against the store, then write."""
        res = SymbolResult(plan.symbol)
        bars: list[Bar] = []
        for raw in raw_bars:
            bar = bar_from_raw(raw)
            if not bar.candle_complete:
                res.skipped_incomplete += 1
                continue
            if bar.bar_start < plan.start or bar.bar_start > plan.end:
                continue
            bars.append(bar)
        res.fetched = len(bars)
        if not bars:
            res.status = "up_to_date" if plan.reason == "append" else "ok"
            res.last_bar = plan.last_stored
            return res

        existing = self._existing(plan.symbol, bars[0].bar_start, bars[-1].bar_start)
        fresh: list[Bar] = []
        restated: dict[int, list[Bar]] = {}
        for bar in bars:
            have = existing.get(bar.bar_start)
            if have is None:
                fresh.append(bar)
                continue
            diffs = _diff(have, bar)
            if not diffs:
                continue
            restated.setdefault(have.revision + 1, []).append(bar)
            for field_name, old, new in diffs:
                self.store.record_correction(
                    symbol=plan.symbol, timeframe="15m", bar_start=bar.bar_start,
                    field=field_name, old_value=old, new_value=new,
                    reason=f"vendor restatement observed by live ingest ({self.vendor_id})",
                    run_id=run_id, old_revision=have.revision,
                    new_revision=have.revision + 1)

        if fresh:
            written, _ = self.store.upsert_candles(
                plan.symbol, plan.instrument_id, fresh, vendor_id=self.vendor_id,
                adjustment_basis_id=self.adjustment_basis_id, revision=1,
                run_id=run_id)
            res.written += written
        for revision, group in restated.items():
            written, _ = self.store.upsert_candles(
                plan.symbol, plan.instrument_id, group, vendor_id=self.vendor_id,
                adjustment_basis_id=self.adjustment_basis_id, revision=revision,
                run_id=run_id)
            res.written += written
            res.restated += written
        res.last_bar = bars[-1].bar_start
        return res

    # -- daily --------------------------------------------------------------
    def _daily_plan(self, info: SymbolInfo, through: date) -> tuple[date, date] | None:
        n, last = daily_coverage(self.store, info.symbol)
        if n < self.min_daily_history:
            return through - timedelta(days=self.daily_history_days), through
        if last is None or last < through:
            return (last + timedelta(days=1)) if last else through, through
        return None

    def _fetch_daily(self, info: SymbolInfo, window: tuple[date, date]):
        start = datetime.combine(window[0], datetime.min.time())
        end = datetime.combine(window[1], datetime.min.time())
        try:
            return info, self.provider.candles(info.symbol, DAILY_TIMEFRAME, start, end), None
        except Exception as exc:  # noqa: BLE001
            return info, [], f"{type(exc).__name__}: {exc}"

    def _apply_daily(self, info: SymbolInfo, raw_bars: Sequence, run_id: str,
                     through: date) -> int:
        rows = []
        fetched = utcnow()
        for raw in raw_bars:
            bar = bar_from_raw(raw)
            day = bar.bar_start.date()
            if day > through:
                continue
            session = self.calendar.session(day)
            if session is None:
                continue
            rows.append((info.instrument_id, info.symbol, "NSE", day.isoformat(),
                         session.start.isoformat(sep=" "), session.end.isoformat(sep=" "),
                         float(bar.open), float(bar.high), float(bar.low),
                         float(bar.close), int(bar.volume or 0), bar.quality_flags,
                         self.adjustment_basis_id, self.vendor_id, fetched, run_id, 1))
        if not rows:
            return 0
        with self.store.transaction() as con:
            con.executemany(
                f"INSERT OR REPLACE INTO {DAILY_TABLE} (instrument_id,symbol,exchange,"
                f"session_date,bar_start,bar_end,open,high,low,close,volume,"
                f"quality_flags,adjustment_basis_id,vendor_id,fetched_at,run_id,revision)"
                f" VALUES ({','.join('?' * 17)})", rows)
        return len(rows)

    # -- regimes ------------------------------------------------------------
    def _record_regimes(self, symbols: Iterable[SymbolInfo], run_id: str) -> int:
        """Derive CAS empirically from the bars we hold (contract 2A)."""
        entries = []
        since = CAS_START.isoformat()
        for info in symbols:
            rows = self.store.con.execute(
                "SELECT substr(bar_start,1,10) AS d, MAX(bar_start) FROM candles_15m "
                "WHERE symbol=? AND bar_start>=? GROUP BY d", (info.symbol, since)).fetchall()
            if not rows:
                continue
            observed = {date.fromisoformat(r[0]): datetime.fromisoformat(r[1])
                        for r in rows}
            book = RegimeBook.from_observed_last_bars(
                info.symbol, observed, self.calendar, is_fno=info.is_fno)
            entries.extend(book.entries())
        if not entries:
            return 0
        return self.store.record_regimes(entries, run_id=run_id)

    # -- a cycle ------------------------------------------------------------
    def run_cycle(self, now: datetime | None = None, *,
                  do_daily: bool | None = None) -> CycleResult:
        now = now or self.clock()
        began = _time.monotonic()
        latest = latest_completed_bar(self.provider, self.calendar, now)
        state = self.session_state(now)
        if do_daily is None:
            do_daily = self.fetch_daily and state["phase"] in ("post_close", "closed",
                                                               "pre_open")
        plan_note = {"universe": len(self.universe), "latest_bar":
                     latest.isoformat(sep=" ") if latest else None,
                     "phase": state["phase"], "daily": bool(do_daily),
                     "provider": self.vendor_id}
        busy_before = int(getattr(self.store, "busy_retries", 0))
        universe, rechecking, skipped = self.effective_universe()
        plan_note["quarantined_skipped"] = skipped
        plan_note["quarantine_rechecks"] = sorted(rechecking)
        run_id = self.store.start_run(provider=self.vendor_id, plan=plan_note)
        result = CycleResult(run_id=run_id, started_at=utcnow(),
                             symbols=len(universe), latest_bar=latest,
                             quarantined_skipped=skipped,
                             quarantine_rechecked=len(rechecking))
        LOG.info("cycle %s start: %s symbols (%s quarantined skipped, %s due a "
                 "re-check), latest completed bar %s, phase %s", run_id,
                 len(universe), skipped, len(rechecking), latest, state["phase"])

        plans = [plan_symbol(i.symbol, i.instrument_id, self.last_stored(i.symbol),
                             latest, self.calendar, self.backfill_sessions)
                 for i in universe]
        wanted = [p for p in plans if p.wanted]
        result.up_to_date = len(plans) - len(wanted)
        result.considered = len(wanted)
        touched: list[SymbolInfo] = []
        by_symbol = {i.symbol: i for i in universe}

        done = 0
        requests_before = int(getattr(self.provider, "requests_made", 0))
        with ThreadPoolExecutor(max_workers=self.workers,
                                thread_name_prefix="live-fetch") as pool:
            for plan, bars, error in pool.map(self._fetch, wanted):
                done += 1
                if error:
                    if plan.symbol in rechecking:
                        # A quarantined symbol we re-probed today and the
                        # provider still cannot serve.  That is the quarantine
                        # being confirmed, not the cycle failing, so it is
                        # recorded on the quarantine row and NOT counted in
                        # `errors` -- which is the number the app warns on.
                        result.quarantine_still_unavailable += 1
                        self.store.touch_quarantine(plan.symbol, error=error)
                        result.per_symbol.append(SymbolResult(
                            plan.symbol, "quarantined", error=error))
                        LOG.info("%s: still unavailable, quarantine stands (%s)",
                                 plan.symbol, error)
                        continue
                    result.errors += 1
                    result.per_symbol.append(SymbolResult(plan.symbol, "error",
                                                          error=error))
                    self.store.mark_progress(plan.symbol, run_id=run_id,
                                             status="error", error=error)
                    LOG.warning("%s: %s", plan.symbol, error)
                    continue
                if plan.symbol in rechecking:
                    self.store.release_quarantine(
                        plan.symbol, run_id=run_id,
                        detail=(f"the provider returned {len(bars)} bars for "
                                f"{plan.start} .. {plan.end} on the daily re-check"))
                    result.quarantine_released += 1
                    LOG.info("%s: provider serves it again; quarantine released",
                             plan.symbol)
                try:
                    res = self._apply(plan, bars, run_id)
                except Exception as exc:  # noqa: BLE001
                    result.errors += 1
                    LOG.exception("%s: write failed", plan.symbol)
                    result.per_symbol.append(SymbolResult(plan.symbol, "error",
                                                          error=str(exc)))
                    continue
                result.rows += res.written
                result.restated += res.restated
                result.per_symbol.append(res)
                if res.written:
                    touched.append(by_symbol[plan.symbol])
                if done % 100 == 0:
                    LOG.info("cycle %s: %s/%s symbols, %s rows", run_id, done,
                             len(wanted), result.rows)

        result.requests = (int(getattr(self.provider, "requests_made", 0))
                           - requests_before) or len(wanted)

        # A symbol due a re-check whose plan asked for nothing was never
        # probed, so nothing can be concluded about it -- but `last_checked`
        # still has to move, or it would be re-included on every single cycle
        # forever instead of once a day.
        probed = {p.symbol for p in wanted}
        for symbol in rechecking - probed:
            self.store.touch_quarantine(
                symbol, error="not probed: the plan asked for no window this cycle")

        if do_daily and latest is not None:
            result.daily_rows = self._daily_pass(latest.date(), run_id, universe)

        try:
            result.regimes_recorded = self._record_regimes(touched or universe, run_id)
        except Exception:  # noqa: BLE001
            LOG.exception("regime derivation failed (bars are unaffected)")

        newest = max((r.last_bar for r in result.per_symbol if r.last_bar),
                     default=None)
        if newest is not None:
            self._publish_freshness(newest, result)
        result.seconds = _time.monotonic() - began
        result.finished_at = utcnow()
        # A cycle that had to wait out another writer is a *slow* cycle, not a
        # healthy one; publishing the count keeps that from being invisible.
        result.busy_waits = int(getattr(self.store, "busy_retries", 0)) - busy_before
        if result.busy_waits:
            LOG.warning("cycle %s waited for the store %d time(s); another "
                        "writer is holding it", run_id, result.busy_waits)
        self.store.finish_run(run_id, requests=result.requests, rows=result.rows,
                              errors=result.errors,
                              status="ok" if not result.errors else "partial")
        LOG.info("cycle %s done: %s requests, %s rows (%s restated), %s daily rows, "
                 "%s errors, %s quarantined skipped (%s released, %s still "
                 "unavailable), %.1fs, newest bar %s", run_id, result.requests,
                 result.rows, result.restated, result.daily_rows, result.errors,
                 result.quarantined_skipped, result.quarantine_released,
                 result.quarantine_still_unavailable, result.seconds, newest)
        return result

    def _daily_pass(self, through: date, run_id: str,
                    universe: Sequence[SymbolInfo] | None = None) -> int:
        jobs = []
        for info in (self.universe if universe is None else universe):
            window = self._daily_plan(info, through)
            if window:
                jobs.append((info, window))
        if not jobs:
            return 0
        LOG.info("daily pass: %s symbols through %s", len(jobs), through)
        total = 0
        with ThreadPoolExecutor(max_workers=self.workers,
                                thread_name_prefix="live-daily") as pool:
            for info, bars, error in pool.map(lambda j: self._fetch_daily(*j), jobs):
                if error:
                    LOG.warning("%s daily: %s", info.symbol, error)
                    continue
                total += self._apply_daily(info, bars, run_id, through)
        return total

    def _publish_freshness(self, newest: datetime, result: CycleResult) -> None:
        """Advertise the newest completed bar for the scanner's freshness gate."""
        import json

        previous = self.store.get_meta(META_LATEST_BAR)
        value = newest.isoformat(sep=" ")
        if previous is None or previous < value:
            self.store.set_meta(META_LATEST_BAR, value)
            self.store.set_meta(META_LATEST_END, (newest + BAR).isoformat(sep=" "))
        self.store.set_meta(META_CYCLE_AT, utcnow())
        self.store.set_meta(META_CYCLE_STATS, json.dumps(result.as_dict()))

    # -- forever ------------------------------------------------------------
    def run_forever(self, interval_seconds: float = 300.0,
                    stop: threading.Event | None = None,
                    max_cycles: int | None = None) -> list[CycleResult]:
        """Catch-up pass, then a cycle every `interval_seconds` while the
        market is open; outside hours it sleeps to the next open."""
        stop = stop or threading.Event()
        out: list[CycleResult] = []
        cycles = 0
        LOG.info("live ingest: catch-up pass")
        out.append(self.run_cycle())
        cycles += 1
        while not stop.is_set() and (max_cycles is None or cycles < max_cycles):
            now = self.clock()
            state = self.session_state(now)
            if state["open"]:
                wait = interval_seconds
            else:
                nxt = state["next_open"]
                wait = max((nxt - now).total_seconds(), 60.0) if nxt else 900.0
                wait = min(wait, 1800.0)
                LOG.info("market closed (%s); idling %.0fs", state["phase"], wait)
            if stop.wait(wait):
                break
            out.append(self.run_cycle())
            cycles += 1
        return out


def _diff(have, bar: Bar) -> list[tuple[str, object, object]]:
    """Fields where the vendor's new bar disagrees with what we hold."""
    out = []
    for name in ("open", "high", "low", "close"):
        old, new = float(getattr(have, name)), float(getattr(bar, name))
        if abs(old - new) > 1e-9:
            out.append((name, old, new))
    if int(have.volume or 0) != int(bar.volume or 0):
        out.append(("volume", int(have.volume or 0), int(bar.volume or 0)))
    return out


#: Always-traded reference names for the freshness probe.  The list deliberately
#: mixes F&O and non-F&O: from 2026-08-03 an F&O stock's last 15-minute bar
#: starts at 15:00, so an all-F&O reference set would under-report the newest
#: completed bar by one slot every day (contract 2A).
FRESHNESS_REFERENCES = ("RELIANCE", "INFY", "TCS", "HDFCBANK", "ICICIBANK",
                        "3MINDIA", "ABBOTINDIA")


def latest_live_bar(store: MarketStore,
                    reference_symbols: Sequence[str] = FRESHNESS_REFERENCES
                    ) -> datetime | None:
    """Newest completed ``bar_start`` the store holds.

    Prefers the ``meta`` value the live loop publishes (O(1)); falls back to a
    bounded index lookup over a few always-traded reference symbols so the
    answer is still right when only the seeder has run.
    """
    value = store.get_meta(META_LATEST_BAR)
    best = datetime.fromisoformat(value) if value else None
    for symbol in reference_symbols:
        row = store.con.execute(
            "SELECT MAX(bar_start) FROM candles_15m WHERE symbol=?", (symbol,)).fetchone()
        if row and row[0]:
            stamp = datetime.fromisoformat(row[0])
            if best is None or stamp > best:
                best = stamp
    return best


def build(*, db_path=None, provider_id: str | None = None,
          symbols: Sequence[str] | None = None, index_column: str = "in_nifty500",
          workers: int = 4, **kwargs) -> LiveIngest:
    """Wire a `LiveIngest` from the environment (the CLI's one constructor)."""
    from market_data import get_provider
    from market_data.store import DEFAULT_DB_PATH

    store = MarketStore(db_path or DEFAULT_DB_PATH)
    provider = get_provider(provider_id)
    universe = nifty500(index_column=index_column, symbols=symbols)
    calendar = live_calendar()
    return LiveIngest(store, provider, universe, calendar, workers=workers, **kwargs)
