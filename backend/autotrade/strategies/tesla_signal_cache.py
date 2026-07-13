"""Falcon Tesla — process-level signal cache + once-per-minute refresher.

WHY: the raw signal recompute (tesla_short_engine.compute_live_signals) is a
full-universe order-flow rescore. Even the optimized fast path takes a few
seconds — FAR too long to run inline on the 5s AutoTrade tick (a synchronous
pandas recompute inside the async tick would block the whole event loop and
stall EVERY session). The order-flow poll writes 1-min bars, so the signals can
only change once per minute anyway.

DESIGN (never blocks the tick):
  * The tesla tick calls `refresh_if_needed(...)` (returns in µs) which triggers
    a recompute in a BACKGROUND thread AT MOST once per 1-min bar — guarded so a
    2nd call within the same minute (or while a refresh is already in flight)
    does nothing.
  * The tick then calls `get_signals(...)` which reads the last cached result
    (a few ms) and reports whether it is STALE (older than the staleness bound).
  * STALENESS SAFETY (C5 pattern): if the cache has not been refreshed within
    `staleness_bound_sec` (a stalled/failing refresher, or a cold start), the
    caller ABSTAINS from NEW seat entries and pages — it does NOT block, and
    EXITS/square-off are unaffected (they never consult signals).
  * The recompute NEVER raises into the caller: a failed refresh leaves the last
    good cache (which then ages into staleness) and logs; the tick sees [].

The heavy recompute is `_recompute`, swapped out in tests (spy/stub) so the
cache logic is verified without the poll DB. Each recompute opens its OWN
read-only sqlite connection (thread-safe; no shared handles).
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import tesla_short_engine as _tse

IST = timezone(timedelta(hours=5, minutes=30))
log = logging.getLogger("autotrade.tesla")


@dataclass
class _SignalEntry:
    minute_key: str                 # the 1-min bar bucket this result is for
    signals: List[Any]              # list[TeslaSignal]
    computed_at: datetime           # when this refresh SUCCEEDED (IST)
    result: Any = None              # the full TeslaSignalResult (diagnostics)


_SIGNAL_CACHE: Dict[Tuple[str, int, str], _SignalEntry] = {}
_INFLIGHT: Dict[Tuple[str, int, str], bool] = {}
_LOCK = threading.RLock()


# ── keys / helpers ───────────────────────────────────────────────────────────

def _cache_key(db_path: Optional[str], personality_window_days: int,
               min_grade: str) -> Tuple[str, int, str]:
    dbk = Path(db_path).as_posix() if db_path else "__default__"
    return (dbk, int(personality_window_days), str(min_grade))


def _minute_bucket(now: datetime) -> str:
    return now.strftime("%Y-%m-%d %H:%M")


def reset_cache() -> None:
    """Drop the signal cache (tests / forced cold start)."""
    with _LOCK:
        _SIGNAL_CACHE.clear()
        _INFLIGHT.clear()


# ── the heavy recompute (swappable in tests) ─────────────────────────────────

def _recompute(*, db_path: Optional[str], personality_window_days: int,
               min_grade: str, cooldown_minutes: int, as_of: str) -> Any:
    """Run the optimized once-per-minute signal recompute. Opens its own RO DB
    connection. Swap this out in tests to avoid the poll DB."""
    return _tse.compute_live_signals_fast(
        as_of=as_of,
        db_path=(Path(db_path) if db_path else None),
        personality_window_days=int(personality_window_days),
        min_grade=min_grade,
        cooldown_minutes=int(cooldown_minutes),
        latest_only=True)


def _do_refresh(key: Tuple[str, int, str], minute_key: str, now: datetime, *,
                db_path: Optional[str], personality_window_days: int,
                min_grade: str, cooldown_minutes: int) -> None:
    """Recompute + store. NEVER raises (best-effort). Clears the in-flight flag
    when done so the next minute can refresh again."""
    try:
        res = _recompute(
            db_path=db_path, personality_window_days=personality_window_days,
            min_grade=min_grade, cooldown_minutes=cooldown_minutes,
            as_of=now.strftime("%Y-%m-%d %H:%M"))
        with _LOCK:
            _SIGNAL_CACHE[key] = _SignalEntry(
                minute_key=minute_key, signals=list(res.signals),
                computed_at=now, result=res)
    except Exception as e:  # never propagate into a tick/thread
        log.warning("tesla signal refresh failed (key=%s): %s", key, e)
    finally:
        with _LOCK:
            _INFLIGHT[key] = False


def refresh_if_needed(*, db_path: Optional[str] = None,
                      personality_window_days: int = 5,
                      min_grade: str = "A++", cooldown_minutes: int = 30,
                      now: Optional[datetime] = None,
                      block: bool = False) -> bool:
    """Trigger a signal recompute AT MOST once per 1-min bar. Returns True when a
    refresh was triggered (this call), False when the once-per-minute / in-flight
    guard short-circuited. Non-blocking by default (spawns a daemon thread);
    block=True runs it synchronously (tests). NEVER raises."""
    now = now or datetime.now(IST)
    key = _cache_key(db_path, personality_window_days, min_grade)
    minute_key = _minute_bucket(now)
    with _LOCK:
        entry = _SIGNAL_CACHE.get(key)
        if entry is not None and entry.minute_key == minute_key:
            return False                    # already have THIS minute
        if _INFLIGHT.get(key):
            return False                    # a refresh is already running
        _INFLIGHT[key] = True
    kw = dict(db_path=db_path, personality_window_days=personality_window_days,
              min_grade=min_grade, cooldown_minutes=cooldown_minutes)
    if block:
        _do_refresh(key, minute_key, now, **kw)
    else:
        try:
            t = threading.Thread(
                target=_do_refresh, args=(key, minute_key, now), kwargs=kw,
                name="tesla-signal-refresh", daemon=True)
            t.start()
        except Exception as e:  # thread spawn failure must not crash the tick
            with _LOCK:
                _INFLIGHT[key] = False
            log.warning("tesla signal refresh thread spawn failed: %s", e)
            return False
    return True


def get_signals(*, db_path: Optional[str] = None,
                personality_window_days: int = 5, min_grade: str = "A++",
                now: Optional[datetime] = None,
                staleness_bound_sec: int = 90) -> Tuple[List[Any], bool]:
    """Read the last cached signals + whether they are STALE.

    Returns (signals, stale). A cold cache (never refreshed) → ([], True). A
    cache older than staleness_bound_sec → (last_signals, True). The caller must
    NOT enter new seats when stale is True (but exits/square-off run regardless).
    staleness_bound_sec <= 0 disables the staleness gate (never stale)."""
    now = now or datetime.now(IST)
    key = _cache_key(db_path, personality_window_days, min_grade)
    with _LOCK:
        entry = _SIGNAL_CACHE.get(key)
    if entry is None:
        return [], True                     # cold start → abstain from entries
    if staleness_bound_sec is None or int(staleness_bound_sec) <= 0:
        return list(entry.signals), False   # gate disabled
    age = (now - entry.computed_at).total_seconds()
    return list(entry.signals), (age > int(staleness_bound_sec))
