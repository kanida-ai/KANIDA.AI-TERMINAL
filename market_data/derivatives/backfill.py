"""10 sessions of 15-minute candles **with OI**, so the time-of-day baseline
exists on day one instead of in two weeks (spec §1, last bullet).

``kite.historical_data(token, from, to, '15minute', oi=True)`` returns candles
carrying open interest for live contracts — verified 2026-09-18 (RELIANCE
futures 208 bars over 8 sessions, an option 234 bars over 10).  Kite serves ~100
days of 15-minute data per request, so **10 sessions is one request per
contract** and the whole cost is simply the number of contracts we ask for.

Which contracts?  All in-scope futures, plus the options that clear the
backfill floor.  That floor is deliberately *not* the spec's ₹2 crore traded
premium: before the open nothing has traded today, so the only honest screen at
that hour is standing open interest and a real price.  The traded-premium floor
of §3 belongs to the screen, not to the fetch, and the fetch that feeds it must
not pre-filter on a number it cannot know yet.
"""
from __future__ import annotations

import logging
import time as _time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Iterable, Mapping, Sequence

from . import config
from .capture import DerivativesCapture, now_ist, _fmt
from .instruments import Contract, Scope, kite_call
from .store import DerivativesStore, utcnow

LOG = logging.getLogger("market_data.derivatives.backfill")

TIMEFRAME = "15minute"
#: The vendor's own daily bar.  Kite serves ~2000 days of daily history per
#: request, so one contract's WHOLE life is a single request.
TIMEFRAME_DAY = "day"
#: How a bar of each timeframe is stamped in the store.  A 15-minute bar is
#: keyed by the minute it starts; a daily bar is keyed by its trading day, and
#: carrying a 00:00 time on it would only invite someone to read it as a bar
#: that opened at midnight.
STAMP_FORMAT = {TIMEFRAME: "%Y-%m-%d %H:%M:%S", TIMEFRAME_DAY: "%Y-%m-%d"}


@dataclass
class Candidate:
    contract: Contract
    oi: int = 0
    price: float = 0.0
    reason: str = ""

    @property
    def token(self) -> int:
        return self.contract.instrument_token


@dataclass
class BackfillResult:
    run_id: str
    from_date: date
    through_date: date
    candidates: int = 0
    attempted: int = 0
    skipped_done: int = 0
    rows: int = 0
    empty: int = 0
    errors: int = 0
    requests: int = 0
    wall_seconds: float = 0.0
    floor: str = ""

    def line(self) -> str:
        return (f"backfill {self.run_id}: {self.rows} candle rows for "
                f"{self.attempted} contracts ({self.candidates} candidates, "
                f"{self.skipped_done} already done, {self.empty} empty, "
                f"{self.errors} errors) in {self.requests} requests, "
                f"{self.wall_seconds:.1f}s; window {self.from_date}..{self.through_date}")


def _price_of(row: Mapping) -> float:
    for key in ("last_price", "prev_close", "close"):
        v = row[key] if key in row.keys() else None
        if v:
            return float(v)
    return 0.0


def latest_liquidity(store: DerivativesStore, *, max_age_hours: float = 20.0,
                     quote_only: bool = True) -> dict[int, tuple[int, float]]:
    """``token -> (oi, price)`` from the most recent stored mark, if it is fresh.

    Using what we already captured means the backfill screen costs **zero extra
    requests** on a day the loop has run.  An empty result means "no fresh
    snapshot", and the caller then quotes the scope itself.
    """
    row = store.con.execute("SELECT MAX(captured_at) FROM snapshots").fetchone()
    latest = row[0] if row else None
    if not latest:
        return {}
    try:
        stamp = datetime.strptime(latest, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return {}
    if (now_ist() - stamp) > timedelta(hours=max_age_hours):
        return {}
    out: dict[int, tuple[int, float]] = {}
    sql = ("SELECT instrument_token, oi, last_price, prev_close FROM snapshots "
           "WHERE captured_at=?")
    if quote_only:
        # A mark reconstructed from candles covers only the underlyings that
        # were seeded, so it must not be mistaken for a screen of the whole
        # scope (that silently reduced the backfill to futures once).
        sql += " AND source='kite.quote'"
    for r in store.con.execute(sql, (latest,)):
        out[int(r["instrument_token"])] = (int(r["oi"] or 0), _price_of(r))
    LOG.info("liquidity screen from the %s mark (%d contracts)", latest, len(out))
    return out


def quote_liquidity(capture: DerivativesCapture, scope: Scope) -> dict[int, tuple[int, float]]:
    """``token -> (oi, price)`` straight from a quote sweep of the scope."""
    quotes = capture._quote_tokens(scope.tokens)
    out: dict[int, tuple[int, float]] = {}
    for token, q in quotes.items():
        price = q.get("last_price") or (q.get("ohlc") or {}).get("close") or 0.0
        out[int(token)] = (int(q.get("oi") or 0), float(price or 0.0))
    return out


def select_candidates(scope: Scope, liquidity: Mapping[int, tuple[int, float]], *,
                      min_oi_lots: int = config.BACKFILL_MIN_OI_LOTS,
                      min_price: float = config.BACKFILL_MIN_PRICE,
                      max_contracts: int | None = None,
                      include_all_futures: bool = True) -> list[Candidate]:
    """The backfill list, most liquid first so a budget cut keeps the best rows."""
    out: list[Candidate] = []
    for c in scope.contracts:
        oi, price = liquidity.get(c.instrument_token, (0, 0.0))
        if c.instrument_type == "FUT" and include_all_futures:
            out.append(Candidate(c, oi, price, "future"))
            continue
        if oi >= min_oi_lots * max(1, c.lot_size) and price >= min_price:
            out.append(Candidate(c, oi, price, "oi+price floor"))
    out.sort(key=lambda x: (x.contract.instrument_type != "FUT", -(x.oi * max(x.price, 0.0))))
    if max_contracts is not None:
        out = out[:max_contracts]
    return out


class Backfiller:
    def __init__(self, store: DerivativesStore, provider, *, workers: int = 4,
                 timeframe: str = TIMEFRAME):
        self.store = store
        self.provider = provider
        self.workers = max(1, int(workers))
        self.timeframe = timeframe

    @property
    def write_rows(self):
        """The store writer this timeframe belongs to.

        One fetch path, two tables: 15-minute bars go to ``candles_15m``, daily
        bars to ``candles_day``.  Resolved here rather than at the call site so
        a new timeframe cannot quietly land in the wrong table.
        """
        if self.timeframe == TIMEFRAME_DAY:
            return self.store.write_daily_candles
        return self.store.write_candles

    def fetch_rows(self, candidate: Candidate, start: date, end: date,
                   run_id: str) -> tuple[int, list[tuple], str | None]:
        """Network only — **no database access**.

        This runs on a worker thread, and an ``sqlite3`` connection belongs to
        the thread that made it.  Writing from here is what broke the first
        backfill run: every worker raised ``SQLite objects created in a thread
        can only be used in that same thread``.  Fetching fans out; writing
        stays on the one thread that owns the connection.
        """
        token = candidate.token
        frm = datetime.combine(start, datetime.min.time())
        to = datetime.combine(end, datetime.max.time().replace(microsecond=0))
        try:
            raw = kite_call(self.provider, lambda k: k.historical_data(
                token, frm, to, self.timeframe, oi=True))
        except Exception as e:  # noqa: BLE001 — recorded per contract, never fatal
            return token, [], f"{type(e).__name__}: {e}"
        fetched_at = utcnow()
        rows = []
        for c in raw or []:
            stamp = c.get("date")
            if stamp is None:
                continue
            bar_start = stamp.strftime(
                STAMP_FORMAT.get(self.timeframe, "%Y-%m-%d %H:%M:%S"))
            rows.append((token, bar_start, c.get("open"), c.get("high"), c.get("low"),
                         c.get("close"), int(c.get("volume") or 0),
                         int(c["oi"]) if c.get("oi") is not None else None,
                         config.VENDOR_ID, fetched_at, run_id))
        return token, rows, None

    def store_rows(self, token: int, rows: Sequence[tuple], start: date, end: date,
                   run_id: str, error: str | None) -> tuple[int, str]:
        """The writing half, always on the caller's (single) thread."""
        if error is not None:
            self.store.mark_backfill(token=token, timeframe=self.timeframe,
                                     from_date=start.isoformat(),
                                     through_date=end.isoformat(), rows=0,
                                     status="error", error=error, run_id=run_id)
            return 0, "error"
        written = self.write_rows(list(rows))
        status = "ok" if written else "empty"
        self.store.mark_backfill(token=token, timeframe=self.timeframe,
                                 from_date=start.isoformat(), through_date=end.isoformat(),
                                 rows=written, status=status, run_id=run_id)
        return written, status

    def run(self, candidates: Sequence[Candidate], *, start: date, end: date,
            run_id: str | None = None, resume: bool = True,
            progress_every: int = 200) -> BackfillResult:
        run_id = run_id or f"bf_{now_ist():%Y%m%dT%H%M%S}"
        self.store.start_run(run_id, "backfill",
                             f"{len(candidates)} candidates {start}..{end}")
        done = self.store.backfill_done(timeframe=self.timeframe,
                                        through_date=end.isoformat()) if resume else set()
        todo = [c for c in candidates if c.token not in done]
        result = BackfillResult(run_id=run_id, from_date=start, through_date=end,
                                candidates=len(candidates),
                                skipped_done=len(candidates) - len(todo))
        started = _time.monotonic()
        requests_before = int(getattr(self.provider, "requests_made", 0) or 0)
        LOG.info("backfill %s: %d contracts to fetch (%d already done), %s..%s, "
                 "%d workers", run_id, len(todo), result.skipped_done, start, end,
                 self.workers)

        def work(candidate: Candidate):
            return self.fetch_rows(candidate, start, end, run_id)

        completed = 0
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for token, rows, err in pool.map(work, todo):
                written, status = self.store_rows(token, rows, start, end, run_id, err)
                completed += 1
                result.attempted += 1
                result.rows += written
                if status == "empty":
                    result.empty += 1
                elif status == "error":
                    result.errors += 1
                    if result.errors <= 3:
                        LOG.warning("backfill %s: token %s failed: %s", run_id, token, err)
                if progress_every and completed % progress_every == 0:
                    elapsed = _time.monotonic() - started
                    rate = completed / elapsed if elapsed else 0
                    eta = (len(todo) - completed) / rate if rate else 0
                    LOG.info("backfill %s: %d/%d contracts, %d rows, %.1f/s, ETA %.0fs",
                             run_id, completed, len(todo), result.rows, rate, eta)
        result.wall_seconds = _time.monotonic() - started
        result.requests = int(getattr(self.provider, "requests_made", 0) or 0) - requests_before
        self.store.finish_run(run_id, status="ok" if not result.errors else "partial",
                              requests=result.requests, rows=result.rows,
                              detail=result.line())
        LOG.info("%s", result.line())
        return result


def run_backfill(capture: DerivativesCapture, *, sessions: int = config.BACKFILL_SESSIONS,
                 calendar_days: int = config.BACKFILL_CALENDAR_DAYS,
                 workers: int = 4, max_contracts: int | None = None,
                 resume: bool = True, on: date | None = None,
                 quote_if_stale: bool = True,
                 underlyings: Sequence[str] | None = None,
                 no_floor: bool = False) -> BackfillResult:
    """Resolve scope → screen for liquidity → fetch 10 sessions per contract."""
    on = on or now_ist().date()
    scope = capture.ensure_scope(on)
    if underlyings:
        from .instruments import select_scope

        wanted = {u.upper() for u in underlyings}
        scope = select_scope(scope.contracts, on, underlyings=sorted(wanted))
        LOG.info("backfill restricted to %s: %s", ", ".join(sorted(wanted)),
                 scope.counts())
    liquidity = latest_liquidity(capture.store)
    source = "stored snapshot"
    if liquidity and len(liquidity) < 0.8 * len(scope.contracts):
        LOG.info("the stored mark covers only %d of %d in-scope contracts — "
                 "quoting the scope instead", len(liquidity), len(scope.contracts))
        liquidity = {}
    if not liquidity and quote_if_stale:
        LOG.info("no fresh snapshot for the liquidity screen — quoting the scope")
        liquidity = quote_liquidity(capture, scope)
        source = "live quote sweep"
    if no_floor:
        candidates = select_candidates(scope, liquidity, min_oi_lots=0, min_price=0.0,
                                       max_contracts=max_contracts)
        floor = ("no floor: every in-scope contract of this underlying set, so the "
                 "option chain has no holes (untraded strikes come back empty)")
    else:
        candidates = select_candidates(scope, liquidity, max_contracts=max_contracts)
        floor = (f"futures: all in scope; options: OI >= 1 lot and price >= "
                 f"Rs{config.BACKFILL_MIN_PRICE:.0f} ({source})"
                 + (f", capped at {max_contracts} most liquid" if max_contracts else ""))
    LOG.info("backfill candidates: %d of %d in scope (%s)",
             len(candidates), len(scope.contracts), floor)
    end = on
    start = on - timedelta(days=calendar_days)
    bf = Backfiller(capture.store, capture.provider, workers=workers)
    result = bf.run(candidates, start=start, end=end, resume=resume)
    result.floor = floor
    return result


# ── the front contract's OWN daily history (the Derivative tab's price chart) ──
#
# Decided, and not re-opened here: "daily" means the daily bars of the SAME
# futures contract the 15-minute chart draws — not a stitched continuous
# series and not the underlying's index.  A future is listed roughly three
# months before it expires, so a live front contract carries ~60 sessions, and
# the day it rolls the new front contract starts again from almost nothing.
# That consequence is not hidden by this fetch; it is reported by the reader,
# which states the session count and the contract the sessions belong to.


def futures_candidates(store: DerivativesStore, *,
                       underlyings: Sequence[str] | None = None) -> list[Candidate]:
    """The in-scope FUTURES the store already knows, as backfill candidates.

    The scope is resolved by the capture loop and persisted on ``contracts``;
    reading it back costs no request and, unlike ``ensure_scope``, writes
    nothing — so a daily fetch cannot re-flag the in-scope set underneath a
    capture cycle that is running.  No liquidity screen applies: §1 puts every
    in-scope future in scope, and a contract that has simply not traded comes
    back empty and is recorded as empty.
    """
    sql = ("SELECT instrument_token, tradingsymbol, underlying, instrument_type, "
           "strike, expiry, lot_size, tick_size, exchange, segment FROM contracts "
           "WHERE in_scope=1 AND instrument_type='FUT'")
    params: list = []
    if underlyings:
        wanted = sorted({u.strip().upper() for u in underlyings if u and u.strip()})
        if wanted:
            sql += f" AND underlying IN ({','.join('?' * len(wanted))})"
            params = list(wanted)
    sql += " ORDER BY underlying, expiry"
    out: list[Candidate] = []
    for r in store.con.execute(sql, params):
        try:
            expiry = date.fromisoformat(str(r["expiry"])[:10])
        except (TypeError, ValueError):
            LOG.warning("skipping %s: unreadable expiry %r", r["tradingsymbol"], r["expiry"])
            continue
        contract = Contract(
            instrument_token=int(r["instrument_token"]),
            tradingsymbol=str(r["tradingsymbol"]),
            underlying=str(r["underlying"]),
            instrument_type=str(r["instrument_type"]),
            strike=float(r["strike"] or 0.0),
            expiry=expiry,
            lot_size=int(r["lot_size"] or 0),
            tick_size=float(r["tick_size"] or 0.05),
            exchange=str(r["exchange"] or config.EXCHANGE),
            segment=str(r["segment"] or ""),
        )
        out.append(Candidate(contract, reason="in-scope future"))
    return out


def run_daily_backfill(capture: DerivativesCapture, *,
                       calendar_days: int = config.DAILY_CALENDAR_DAYS,
                       workers: int = 4, resume: bool = True,
                       on: date | None = None,
                       underlyings: Sequence[str] | None = None,
                       max_contracts: int | None = None) -> BackfillResult:
    """One daily-history request per in-scope futures contract.

    Same machinery as the 15-minute backfill — the same ``Backfiller``, the same
    shared 3 req/s limiter through ``kite_call``, the same ``backfill_progress``
    resume ledger (under ``timeframe='day'``) and the same rule that an empty
    answer is *recorded* as empty and never invented.
    """
    on = on or now_ist().date()
    candidates = futures_candidates(capture.store, underlyings=underlyings)
    if not candidates:
        # Nothing in the store yet: resolve the scope the way the 15-minute
        # backfill does, then read it back.
        LOG.info("no in-scope futures in the store — resolving the scope first")
        capture.ensure_scope(on)
        candidates = futures_candidates(capture.store, underlyings=underlyings)
    if max_contracts is not None:
        candidates = candidates[:max_contracts]
    floor = (f"every in-scope futures contract{' of ' + ','.join(sorted({u.upper() for u in underlyings})) if underlyings else ''} "
             f"— no liquidity floor: a future that did not trade comes back empty and is recorded as empty"
             + (f", capped at {max_contracts}" if max_contracts else ""))
    LOG.info("daily backfill: %d in-scope futures (%s)", len(candidates), floor)
    bf = Backfiller(capture.store, capture.provider, workers=workers,
                    timeframe=TIMEFRAME_DAY)
    result = bf.run(candidates, start=on - timedelta(days=calendar_days), end=on,
                    run_id=f"bfd_{now_ist():%Y%m%dT%H%M%S}", resume=resume)
    result.floor = floor
    return result


__all__ = ["Backfiller", "BackfillResult", "Candidate", "run_backfill",
           "select_candidates", "latest_liquidity", "quote_liquidity", "TIMEFRAME",
           "TIMEFRAME_DAY", "STAMP_FORMAT", "futures_candidates", "run_daily_backfill"]
