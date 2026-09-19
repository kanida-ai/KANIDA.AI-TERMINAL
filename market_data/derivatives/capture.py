"""The 15-minute snapshot loop (spec §1/§2).

One cycle = one **mark** (09:30, 09:45 … 15:30, plus a post-close mark).  At a
mark every in-scope contract is quoted and stored raw, with provenance.

What this refuses to do
-----------------------
* **Fabricate a mark.**  A quote taken 20 minutes late is not the 09:45 mark —
  its cumulative volume and OI belong to a different part of the day.  Past a
  grace window the mark is written to `captures` as ``missed`` and left empty.
* **Invent a number.**  A field the vendor did not send is stored NULL.  A
  contract that did not come back in the quote response is simply absent.
* **Build a second Kite client or a second rate budget.**  Every request goes
  through the shared ``KiteProvider`` call path and its 3 req/s limiter.

Cost of a cycle: 27,260 in-scope contracts ÷ 500 per ``quote()`` = 55 requests,
plus 1 for the spot leg — about 19 s of limiter time at 3 req/s.
"""
from __future__ import annotations

import logging
import time as _time
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable, Mapping, Sequence

from . import config
from .instruments import (
    Contract,
    Scope,
    fetch_nfo_instruments,
    kite_call,
    select_scope,
    spot_map,
)
from .store import DerivativesStore, utcnow

LOG = logging.getLogger("market_data.derivatives.capture")

IST_OFFSET = timedelta(hours=5, minutes=30)


def now_ist() -> datetime:
    """Naive IST wall clock, the same convention the rest of the repo stores."""
    return datetime.now(timezone.utc).replace(tzinfo=None) + IST_OFFSET


def _fmt(stamp: datetime) -> str:
    return stamp.strftime("%Y-%m-%d %H:%M:%S")


def snapshot_id_for(mark: datetime) -> str:
    return f"cap_{mark:%Y%m%dT%H%M}"


# ── the mark grid ────────────────────────────────────────────────────────────

def marks_for(day: date) -> list[tuple[datetime, str]]:
    """Every capture mark of one session, in order.

    09:30 … 15:30 are the closes of the 15-minute candles of the NFO continuous
    session, and the post-close mark is the close of the terminal 15:30 bar that
    Kite's NFO 15-minute history carries (26 bars a session, verified
    2026-09-18).
    """
    out: list[tuple[datetime, str]] = []
    cur = datetime.combine(day, config.FIRST_MARK)
    last = datetime.combine(day, config.LAST_MARK)
    while cur <= last:
        out.append((cur, config.MARK_BAR_CLOSE))
        cur += timedelta(minutes=config.BAR_MINUTES)
    out.append((datetime.combine(day, config.POST_CLOSE_MARK), config.MARK_POST_CLOSE))
    return out


def _live_calendar(through: date):
    """The shared session calendar, extended forward.  Never a second calendar."""
    try:
        from market_data.live.calendar_ext import live_calendar

        return live_calendar(through=through)
    except Exception as e:  # pragma: no cover - environment dependent
        LOG.warning("session calendar unavailable (%s); falling back to weekdays",
                    type(e).__name__)
        return None


class SessionDays:
    """Is this date a trading session?  Answered by `market_data.calendar`."""

    def __init__(self, calendar=None, *, weekend_only_fallback: bool = True):
        self._cal = calendar
        self._fallback = weekend_only_fallback
        self._loaded_through: date | None = None

    def _ensure(self, day: date):
        if self._cal is not None and (self._loaded_through is None or day <= self._loaded_through):
            return
        if self._cal is None and self._loaded_through is not None:
            return
        self._cal = _live_calendar(day + timedelta(days=30))
        self._loaded_through = day + timedelta(days=30)

    def is_session(self, day: date) -> bool:
        self._ensure(day)
        if self._cal is not None:
            try:
                return bool(self._cal.is_session(day))
            except Exception:
                pass
        return day.weekday() < 5 if self._fallback else False

    def next_session(self, day: date, *, limit: int = 30) -> date:
        d = day
        for _ in range(limit):
            if self.is_session(d):
                return d
            d += timedelta(days=1)
        return day


# ── one cycle ────────────────────────────────────────────────────────────────

@dataclass
class CycleResult:
    mark: datetime
    mark_kind: str
    snapshot_id: str
    status: str
    contracts_planned: int = 0
    quoted: int = 0
    rows_written: int = 0
    rows_skipped: int = 0
    underlyings_written: int = 0
    requests: int = 0
    wall_seconds: float = 0.0
    lag_seconds: float = 0.0
    error: str | None = None

    def as_dict(self) -> dict:
        d = self.__dict__.copy()
        d["mark"] = _fmt(self.mark)
        return d

    def line(self) -> str:
        return (f"{_fmt(self.mark)} [{self.mark_kind}] {self.status}: "
                f"{self.rows_written} rows (+{self.underlyings_written} underlying, "
                f"{self.rows_skipped} empty skipped) from {self.quoted}/{self.contracts_planned} "
                f"quoted in {self.requests} requests, {self.wall_seconds:.1f}s wall, "
                f"lag {self.lag_seconds:.0f}s"
                + (f" — {self.error}" if self.error else ""))


def _num(value):
    """A number the vendor actually sent, or None.  Never a substituted zero."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    return None


def _int(value):
    v = _num(value)
    return int(v) if v is not None else None


def _depth_side(quote: Mapping, side: str) -> tuple[float | None, int | None]:
    depth = quote.get("depth") or {}
    levels = depth.get(side) or []
    if not levels:
        return None, None
    top = levels[0] or {}
    price = _num(top.get("price"))
    qty = _int(top.get("quantity"))
    if price == 0 and qty in (0, None):
        return None, qty  # an empty book is not a ₹0 bid
    return price, qty


def _stamp(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.year <= 1971:      # Kite's "no tick yet" sentinel
            return None
        return _fmt(value)
    text = str(value).strip()
    return text or None


class DerivativesCapture:
    """Snapshot every in-scope contract, one mark at a time."""

    def __init__(self, store: DerivativesStore, provider, *, scope: Scope | None = None,
                 skip_empty: bool = config.SKIP_EMPTY_CONTRACTS,
                 batch_size: int = config.QUOTE_BATCH,
                 grace_seconds: float = config.MARK_GRACE_SECONDS,
                 lag_seconds: float = config.MARK_LAG_SECONDS,
                 sessions: SessionDays | None = None,
                 capture_spot: bool = True, cache_dir=None, use_cache: bool = True,
                 prune: bool = True, prune_after: time | None = None):
        self.store = store
        self.provider = provider
        self.skip_empty = skip_empty
        self.batch_size = int(batch_size)
        self.grace_seconds = float(grace_seconds)
        self.lag_seconds = float(lag_seconds)
        self.sessions = sessions or SessionDays()
        self.capture_spot = capture_spot
        #: the on-disk NFO dump cache.  Tests point this at a temp directory (or
        #: switch it off) so a fixture universe can never leak into the real one.
        self.cache_dir = cache_dir
        self.use_cache = use_cache
        self.scope: Scope | None = scope
        self._scope_day: date | None = scope.on if scope else None
        self._spot: dict[str, tuple[str, int]] = {}
        self._spot_day: date | None = None
        #: retention runs from this loop, once a calendar day.  Before this the
        #: prune existed in `store.py` and nothing anywhere called it, so the
        #: 90/365/never windows were a document, not a behaviour.
        self.prune_enabled = bool(prune)
        self.prune_after = prune_after or config.PRUNE_AFTER
        self._pruned_on: date | None = None

    # -- scope ---------------------------------------------------------------

    def ensure_scope(self, on: date | None = None, *, refresh: bool = False) -> Scope:
        """Resolve (and persist) the front-two-expiry scope for ``on``."""
        on = on or now_ist().date()
        if self.scope is not None and self._scope_day == on and not refresh:
            return self.scope
        contracts = fetch_nfo_instruments(self.provider, refresh=refresh,
                                          cache_dir=self.cache_dir,
                                          use_cache=self.use_cache)
        scope = select_scope(contracts, on)
        self.store.sync_contracts(contracts, in_scope_tokens=scope.tokens,
                                  snapshot_id=None)
        self.store.set_meta("scope_resolved_on", on.isoformat())
        self.store.set_meta("scope_counts", str(scope.counts()))
        self.scope = scope
        self._scope_day = on
        LOG.info("scope for %s: %s (of %d listed NFO instruments)",
                 on, scope.counts(), scope.total_listed)
        return scope

    def ensure_spot(self, scope: Scope) -> dict[str, tuple[str, int]]:
        if not self.capture_spot:
            return {}
        if self._spot and self._spot_day == scope.on:
            return self._spot
        self._spot = spot_map(self.provider, scope.underlyings)
        self._spot_day = scope.on
        return self._spot

    # -- marks ---------------------------------------------------------------

    def due_marks(self, now: datetime | None = None, day: date | None = None
                  ) -> tuple[list[tuple[datetime, str]], list[tuple[datetime, str]]]:
        """``(capturable, missed)`` marks of ``day`` that are not already done."""
        now = now or now_ist()
        day = day or now.date()
        done = self.store.captures_for(day.isoformat())
        capturable, missed = [], []
        for mark, kind in marks_for(day):
            row = done.get(_fmt(mark))
            if row is not None and row["status"] in ("ok", "missed"):
                continue
            ready_at = mark + timedelta(seconds=self.lag_seconds)
            if now < ready_at:
                continue
            if (now - mark).total_seconds() <= self.grace_seconds:
                capturable.append((mark, kind))
            else:
                missed.append((mark, kind))
        return capturable, missed

    def record_missed(self, marks: Iterable[tuple[datetime, str]], reason: str) -> int:
        n = 0
        for mark, kind in marks:
            self.store.record_missed(snapshot_id=snapshot_id_for(mark), mark_at=_fmt(mark),
                                     mark_kind=kind, session_date=mark.date().isoformat(),
                                     reason=reason)
            LOG.warning("mark %s recorded as MISSED (%s)", _fmt(mark), reason)
            n += 1
        return n

    # -- the cycle ------------------------------------------------------------

    def run_once(self, mark: datetime | None = None, *, kind: str | None = None,
                 now: datetime | None = None, force: bool = False) -> CycleResult:
        now = now or now_ist()
        if mark is None:
            capturable, missed = self.due_marks(now)
            self.record_missed(missed, "not captured within the grace window")
            if not capturable:
                return CycleResult(mark=now, mark_kind="none",
                                   snapshot_id="", status="no_mark_due")
            mark, kind = capturable[-1]
        kind = kind or config.MARK_BAR_CLOSE
        sid = snapshot_id_for(mark)

        existing = self.store.capture(sid)
        if existing is not None and existing["status"] == "ok" and not force:
            LOG.info("mark %s already captured (%s rows) — skipping",
                     _fmt(mark), existing["rows_written"])
            return CycleResult(mark=mark, mark_kind=kind, snapshot_id=sid,
                               status="already_done",
                               rows_written=existing["rows_written"] or 0)

        scope = self.ensure_scope(mark.date())
        spot = self.ensure_spot(scope)
        by_token = scope.by_token()
        lag = (now - mark).total_seconds()

        self.store.capture_begin(snapshot_id=sid, mark_at=_fmt(mark), mark_kind=kind,
                                 session_date=mark.date().isoformat(),
                                 planned=len(scope.contracts))
        started = _time.monotonic()
        requests_before = int(getattr(self.provider, "requests_made", 0) or 0)
        result = CycleResult(mark=mark, mark_kind=kind, snapshot_id=sid, status="running",
                             contracts_planned=len(scope.contracts), lag_seconds=lag)
        try:
            quotes = self._quote_tokens(scope.tokens)
            result.quoted = len(quotes)
            rows, skipped, agg = self._build_rows(quotes, by_token, mark, kind, sid)
            result.rows_skipped = skipped
            spot_quotes = self._quote_tokens([t for _, t in spot.values()]) if spot else {}
            u_rows = self._build_underlying_rows(agg, spot, spot_quotes, scope, mark,
                                                 kind, sid)
            result.rows_written = self.store.write_snapshots(rows)
            if kind == config.MARK_BAR_CLOSE and result.rows_written:
                self.store.set_meta("latest_trading_mark", _fmt(mark))
            result.underlyings_written = self.store.write_underlying_snapshots(u_rows)
            result.status = "ok" if result.quoted >= len(scope.tokens) else "partial"
        except Exception as e:  # noqa: BLE001 — recorded, then re-raised to the caller's policy
            result.status = "error"
            result.error = f"{type(e).__name__}: {e}"
            LOG.exception("cycle %s failed", sid)
        finally:
            result.wall_seconds = _time.monotonic() - started
            result.requests = int(getattr(self.provider, "requests_made", 0) or 0) - requests_before
            self.store.capture_finish(sid, status=result.status,
                                      rows_written=result.rows_written,
                                      rows_skipped=result.rows_skipped,
                                      underlyings_written=result.underlyings_written,
                                      requests=result.requests,
                                      wall_seconds=result.wall_seconds,
                                      lag_seconds=result.lag_seconds,
                                      error=result.error)
        LOG.info("%s", result.line())
        return result

    # -- requests -------------------------------------------------------------

    def _quote_tokens(self, tokens: Sequence[int]) -> dict[int, Mapping]:
        out: dict[int, Mapping] = {}
        tokens = [int(t) for t in tokens]
        for i in range(0, len(tokens), self.batch_size):
            chunk = tokens[i:i + self.batch_size]
            raw = kite_call(self.provider, lambda k, c=chunk: k.quote(c))
            for key, value in (raw or {}).items():
                try:
                    token = int(value.get("instrument_token") or key)
                except (TypeError, ValueError):
                    continue
                out[token] = value
        return out

    # -- rows -----------------------------------------------------------------

    def _build_rows(self, quotes: Mapping[int, Mapping], by_token: Mapping[int, Contract],
                    mark: datetime, kind: str, sid: str):
        fetched_at = utcnow()
        captured_at = _fmt(mark)
        rows: list[tuple] = []
        skipped = 0
        agg: dict[str, dict] = {}
        for token, q in quotes.items():
            contract = by_token.get(token)
            last_price = _num(q.get("last_price"))
            volume = _int(q.get("volume"))
            oi = _int(q.get("oi"))
            if self.skip_empty and not (oi or 0) and not (volume or 0) and not (last_price or 0):
                skipped += 1
            else:
                ohlc = q.get("ohlc") or {}
                bid, bid_qty = _depth_side(q, "buy")
                ask, ask_qty = _depth_side(q, "sell")
                rows.append((
                    token, captured_at, kind, last_price, _num(q.get("average_price")),
                    volume, oi, _int(q.get("oi_day_high")), _int(q.get("oi_day_low")),
                    _int(q.get("buy_quantity")), _int(q.get("sell_quantity")),
                    bid, ask, bid_qty, ask_qty,
                    _num(ohlc.get("open")), _num(ohlc.get("high")), _num(ohlc.get("low")),
                    _num(ohlc.get("close")), _stamp(q.get("last_trade_time")),
                    _stamp(q.get("timestamp")), "kite.quote", config.VENDOR_ID,
                    fetched_at, sid,
                    None,   # average_price_est: a live quote carries the real one
                ))
            if contract is None:
                continue
            slot = agg.setdefault(contract.underlying, {
                "ce_oi": 0, "pe_oi": 0, "ce_vol": 0, "pe_vol": 0, "ce_n": 0, "pe_n": 0,
                "fut_price": None, "fut_token": None, "fut_expiry": None})
            if contract.instrument_type == "CE":
                slot["ce_oi"] += oi or 0
                slot["ce_vol"] += volume or 0
                slot["ce_n"] += 1
            elif contract.instrument_type == "PE":
                slot["pe_oi"] += oi or 0
                slot["pe_vol"] += volume or 0
                slot["pe_n"] += 1
            else:  # front future = the nearest expiry we hold for this underlying
                if slot["fut_expiry"] is None or contract.expiry < slot["fut_expiry"]:
                    slot["fut_expiry"] = contract.expiry
                    slot["fut_price"] = last_price
                    slot["fut_token"] = token
        return rows, skipped, agg

    def _build_underlying_rows(self, agg: Mapping[str, dict],
                               spot: Mapping[str, tuple[str, int]],
                               spot_quotes: Mapping[int, Mapping], scope: Scope,
                               mark: datetime, kind: str, sid: str) -> list[tuple]:
        fetched_at = utcnow()
        captured_at = _fmt(mark)
        rows = []
        for underlying, slot in sorted(agg.items()):
            spot_symbol, spot_token = spot.get(underlying, (None, None))
            spot_price = None
            if spot_token is not None:
                q = spot_quotes.get(int(spot_token))
                if q is not None:
                    spot_price = _num(q.get("last_price"))
            rows.append((
                underlying, captured_at, kind, spot_price, spot_symbol,
                slot["fut_price"], slot["fut_token"],
                slot["ce_oi"], slot["pe_oi"], slot["ce_vol"], slot["pe_vol"],
                slot["ce_n"], slot["pe_n"], config.VENDOR_ID, fetched_at, sid,
            ))
        return rows

    # -- the loop -------------------------------------------------------------

    # -- retention -----------------------------------------------------------
    def maybe_prune(self, now: datetime | None = None, *, force: bool = False) -> dict | None:
        """Run the retention pass if it is due, and return what it removed.

        Due means: retention is on, the day's capturing is finished (the caller
        reaches this only when no mark is left today, and ``prune_after`` is a
        second floor under that), and this calendar day has not already had its
        pass.  Returns ``None`` when it was not due, so "did not run" and "ran
        and removed nothing" stay distinguishable.

        The pass takes the single writer one session-day at a time through
        ``store.transaction()``, which is ``BEGIN IMMEDIATE`` behind
        ``retry_while_busy`` — so if a mark is mid-write the prune waits for it
        rather than failing, and hands the writer back between days.

        A failure here is logged and swallowed: retention is housekeeping and
        must never be the reason a capture loop stops capturing.
        """
        now = now or now_ist()
        if not force:
            if not self.prune_enabled:
                return None
            if now.time() < self.prune_after:
                return None
            if self._pruned_on == now.date():
                return None
        self._pruned_on = now.date()
        try:
            out = self.store.prune(today=now.date())
        except Exception:
            LOG.exception("retention pass failed; the capture loop continues")
            return None
        LOG.info("retention: snapshots -%d, metrics -%d, candles -%d, underlying -%d "
                 "(rolled up %d session(s); serving %s)",
                 out["snapshots_deleted"], out["metrics_deleted"],
                 out["candles_deleted"], out["underlying_deleted"],
                 out["rolled_up_sessions"], out["serving_session"])
        return out

    def run_forever(self, *, days: int | None = None, until: datetime | None = None,
                    poll_seconds: float = 5.0, sleep=_time.sleep) -> list[CycleResult]:
        """Capture every mark, session after session, until ``until``/``days``.

        Crash-safe by construction: the ledger in ``captures`` is what decides
        what still needs doing, so restarting mid-session picks up exactly where
        it stopped and never re-quotes a mark it already has.
        """
        out: list[CycleResult] = []
        first_day = now_ist().date()
        LOG.info("capture loop starting; marks/day=%d, grace=%.0fs, lag=%.0fs, db=%s",
                 len(marks_for(first_day)), self.grace_seconds, self.lag_seconds,
                 self.store.path)
        while True:
            now = now_ist()
            if until is not None and now >= until:
                LOG.info("loop reached its end time (%s)", _fmt(until))
                return out
            if days is not None and (now.date() - first_day).days >= days:
                LOG.info("loop finished %d day(s)", days)
                return out
            today = now.date()
            if not self.sessions.is_session(today):
                nxt = self.sessions.next_session(today + timedelta(days=1))
                wake = datetime.combine(nxt, config.FIRST_MARK)
                LOG.info("%s is not a session; sleeping until %s", today, _fmt(wake))
                self._sleep_until(wake, sleep, until)
                continue

            capturable, missed = self.due_marks(now, today)
            self.record_missed(missed, "not captured within the grace window")
            if capturable:
                mark, kind = capturable[-1]
                for older_mark, older_kind in capturable[:-1]:
                    self.record_missed([(older_mark, older_kind)],
                                       "superseded by a newer due mark")
                try:
                    out.append(self.run_once(mark, kind=kind, now=now))
                except Exception:  # pragma: no cover - the loop outlives one cycle
                    LOG.exception("cycle raised; the loop continues")
                continue

            nxt = self._next_mark_after(now)
            if nxt is None:
                # The day's last mark is captured and nothing else is due today.
                # That is when retention runs: it cannot delay a mark, because
                # there is no mark left to delay, and the loop is about to sleep
                # to tomorrow's open anyway.
                self.maybe_prune(now)
                nxt_day = self.sessions.next_session(today + timedelta(days=1))
                nxt = datetime.combine(nxt_day, config.FIRST_MARK)
            wake = nxt + timedelta(seconds=self.lag_seconds)
            LOG.info("next mark %s (waking %s)", _fmt(nxt), _fmt(wake))
            self._sleep_until(wake, sleep, until, poll_seconds)
        return out

    def _next_mark_after(self, now: datetime) -> datetime | None:
        for mark, _kind in marks_for(now.date()):
            if mark + timedelta(seconds=self.lag_seconds) > now:
                return mark
        return None

    def _sleep_until(self, wake: datetime, sleep, until: datetime | None,
                     poll_seconds: float = 5.0) -> None:
        while True:
            now = now_ist()
            if now >= wake:
                return
            if until is not None and now >= until:
                return
            sleep(min(poll_seconds, max(0.2, (wake - now).total_seconds())))


def build(*, db_path=None, provider_id: str = "kite", provider=None,
          **kwargs) -> DerivativesCapture:
    """The wiring one place, so the CLI and the tests agree."""
    from market_data import get_provider

    store = DerivativesStore(db_path or config.DEFAULT_DB_PATH)
    provider = provider or get_provider(provider_id)
    return DerivativesCapture(store, provider, **kwargs)


__all__ = ["DerivativesCapture", "CycleResult", "SessionDays", "marks_for",
           "snapshot_id_for", "now_ist", "build"]
