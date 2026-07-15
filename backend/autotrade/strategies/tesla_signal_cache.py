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

OFF-PROCESS (why the background thread no longer stalls the event loop): the
backend is a SINGLE-worker uvicorn (one async loop). `compute_live_signals_fast`
is heavy pandas — CPU work that HOLDS the GIL — so running it on a daemon thread
still starves every other API request. So the non-blocking refresh path now runs
the recompute in a SEPARATE PROCESS via `autotrade.compute_offload.run_offloaded`
(own interpreter + own GIL → zero event-loop contention). The daemon thread just
submits + WAITS on the subprocess (waiting releases the GIL). The subprocess-safe
entry point is the TOP-LEVEL `_recompute_worker` (spawn re-imports THIS module by
qualified name — it imports cleanly, no app/uvicorn side effect); `TeslaSignal`/
`TeslaSignalResult` are plain picklable dataclasses so the result crosses the
process boundary intact. `block=True` (tests) still runs `_recompute` INLINE with
NO pool so the swappable-`_recompute` spy pattern stays hermetic. A failed /
timed-out / crashed offload leaves the last-good cache (ages into staleness) and
NEVER raises into the tick — identical best-effort semantics as before.
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


_SIGNAL_CACHE: Dict[Tuple[str, int, str, bool, bool, bool], _SignalEntry] = {}
_INFLIGHT: Dict[Tuple[str, int, str, bool, bool, bool], bool] = {}
_LOCK = threading.RLock()


# ── keys / helpers ───────────────────────────────────────────────────────────

def _cache_key(db_path: Optional[str], personality_window_days: int,
               min_grade: str,
               did_layer_enabled: bool = False,
               vectorized: bool = False,
               incremental: bool = False,
               ) -> Tuple[str, int, str, bool, bool, bool]:
    # did_layer_enabled is part of the key so the v2 and v3 signal sets never
    # collide in the process cache. vectorized is ALSO part of the key so a
    # loop-path session and a vectorised-path session never share a cache entry
    # (they are byte-identical by proof, but keying them apart keeps the flip
    # observable and lossless if the proof is ever wrong). incremental (round-2
    # incremental infer read) is keyed the same way, for the same reason.
    dbk = Path(db_path).as_posix() if db_path else "__default__"
    return (dbk, int(personality_window_days), str(min_grade),
            bool(did_layer_enabled), bool(vectorized), bool(incremental))


def _minute_bucket(now: datetime) -> str:
    return now.strftime("%Y-%m-%d %H:%M")


def reset_cache() -> None:
    """Drop the signal cache (tests / forced cold start)."""
    with _LOCK:
        _SIGNAL_CACHE.clear()
        _INFLIGHT.clear()


# ── the heavy recompute (swappable in tests) ─────────────────────────────────

def _recompute(*, db_path: Optional[str], personality_window_days: int,
               min_grade: str, cooldown_minutes: int, as_of: str,
               did_layer_enabled: bool = False,
               vectorized: bool = False,
               incremental: bool = False) -> Any:
    """Run the optimized once-per-minute signal recompute. Opens its own RO DB
    connection. Swap this out in tests to avoid the poll DB. vectorized=False
    (DEFAULT) uses the committed per-group loop; True uses the sub-10s vectorised
    feature pipeline (byte-identical, default-OFF until the parity proof clears).
    incremental=True (round-2) reads only the new infer-day minute(s) from an
    in-process bar cache (byte-identical; default-OFF until its proof clears)."""
    return _tse.compute_live_signals_fast(
        as_of=as_of,
        db_path=(Path(db_path) if db_path else None),
        personality_window_days=int(personality_window_days),
        min_grade=min_grade,
        cooldown_minutes=int(cooldown_minutes),
        did_layer_enabled=bool(did_layer_enabled),
        vectorized=bool(vectorized),
        incremental=bool(incremental),
        latest_only=True)


def _recompute_worker(**kwargs: Any) -> Any:
    """TOP-LEVEL, picklable entry point for the OFF-PROCESS recompute.

    Runs inside a spawned worker process (its OWN GIL) — see
    `autotrade.compute_offload.run_offloaded`. Just calls the module-level
    `_recompute` (which opens its own read-only sqlite connection) and returns a
    `TeslaSignalResult` (a plain picklable dataclass). MUST stay top-level and
    importable by qualified name (`spawn` re-imports this module in a fresh
    interpreter). NOTE: a test that monkeypatches `sc._recompute` does NOT affect
    this worker — the child re-imports a clean module — which is why the block=True
    (inline) path is the one used to exercise the swappable `_recompute` spy."""
    return _recompute(**kwargs)


def _do_refresh(key: Tuple[str, int, str, bool, bool, bool], minute_key: str,
                now: datetime, *,
                db_path: Optional[str], personality_window_days: int,
                min_grade: str, cooldown_minutes: int,
                did_layer_enabled: bool = False,
                vectorized: bool = False,
                incremental: bool = False,
                use_pool: bool = True) -> None:
    """Recompute + store. NEVER raises (best-effort). Clears the in-flight flag
    when done so the next minute can refresh again.

    `use_pool=True` (the default; the non-blocking daemon-thread path) runs the
    recompute in a SEPARATE PROCESS via `run_offloaded` so the heavy pandas work
    can't hold the event-loop GIL. `use_pool=False` (the block=True test path)
    runs `_recompute` INLINE with no pool, so the swappable-`_recompute` spy stays
    hermetic. A broken/timed-out/failed offload is caught here (best-effort): the
    last-good cache is left untouched and ages into staleness; nothing propagates."""
    rkw = dict(db_path=db_path, personality_window_days=personality_window_days,
               min_grade=min_grade, cooldown_minutes=cooldown_minutes,
               did_layer_enabled=did_layer_enabled, vectorized=vectorized,
               incremental=incremental,
               as_of=now.strftime("%Y-%m-%d %H:%M"))
    try:
        if use_pool:
            # OFF-PROCESS: own interpreter/GIL → zero event-loop contention. The
            # calling daemon thread only WAITS here (releases the GIL). Any
            # failure surfaces as OffloadError, caught by the except below.
            from ..compute_offload import run_offloaded
            res = run_offloaded(_recompute_worker, **rkw)
        else:
            # INLINE (block=True test path): swappable `_recompute` spy, NO pool.
            res = _recompute(**rkw)
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
                      did_layer_enabled: bool = False,
                      vectorized: bool = False,
                      incremental: bool = False,
                      now: Optional[datetime] = None,
                      block: bool = False) -> bool:
    """Trigger a signal recompute AT MOST once per 1-min bar. Returns True when a
    refresh was triggered (this call), False when the once-per-minute / in-flight
    guard short-circuited. Non-blocking by default (spawns a daemon thread);
    block=True runs it synchronously (tests). NEVER raises."""
    now = now or datetime.now(IST)
    key = _cache_key(db_path, personality_window_days, min_grade,
                     did_layer_enabled, vectorized, incremental)
    minute_key = _minute_bucket(now)
    with _LOCK:
        entry = _SIGNAL_CACHE.get(key)
        if entry is not None and entry.minute_key == minute_key:
            return False                    # already have THIS minute
        if _INFLIGHT.get(key):
            return False                    # a refresh is already running
        _INFLIGHT[key] = True
    kw = dict(db_path=db_path, personality_window_days=personality_window_days,
              min_grade=min_grade, cooldown_minutes=cooldown_minutes,
              did_layer_enabled=did_layer_enabled, vectorized=vectorized,
              incremental=incremental)
    if block:
        # INLINE, no pool — keeps tests hermetic (swappable `_recompute` spy).
        _do_refresh(key, minute_key, now, use_pool=False, **kw)
    else:
        try:
            # Daemon thread submits the recompute to the OFF-PROCESS pool and
            # WAITS on it (releasing the GIL) — the event loop is never stalled.
            t = threading.Thread(
                target=_do_refresh, args=(key, minute_key, now),
                kwargs={**kw, "use_pool": True},
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
                did_layer_enabled: bool = False,
                vectorized: bool = False,
                incremental: bool = False,
                now: Optional[datetime] = None,
                staleness_bound_sec: int = 90) -> Tuple[List[Any], bool]:
    """Read the last cached signals + whether they are STALE.

    Returns (signals, stale). A cold cache (never refreshed) → ([], True). A
    cache older than staleness_bound_sec → (last_signals, True). The caller must
    NOT enter new seats when stale is True (but exits/square-off run regardless).
    staleness_bound_sec <= 0 disables the staleness gate (never stale)."""
    now = now or datetime.now(IST)
    key = _cache_key(db_path, personality_window_days, min_grade,
                     did_layer_enabled, vectorized, incremental)
    with _LOCK:
        entry = _SIGNAL_CACHE.get(key)
    if entry is None:
        return [], True                     # cold start → abstain from entries
    if staleness_bound_sec is None or int(staleness_bound_sec) <= 0:
        return list(entry.signals), False   # gate disabled
    age = (now - entry.computed_at).total_seconds()
    return list(entry.signals), (age > int(staleness_bound_sec))
