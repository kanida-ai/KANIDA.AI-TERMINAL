"""Reusable OFF-PROCESS compute offload for heavy, GIL-holding CPU work.

WHY: the backend runs a SINGLE-worker uvicorn (one async event loop). Heavy
pandas/numpy CPU work (e.g. the Tesla full-universe order-flow rescore) holds
the CPython GIL, so running it on a background *thread* still STALLS the event
loop — every other API request (stock picker, sizing preview, live panel) queues
behind it. Running it in a separate PROCESS (its own interpreter + own GIL)
removes ALL event-loop contention: the submitting thread only WAITS on the
subprocess result, and waiting releases the GIL.

USAGE — PICKLABILITY CONTRACT (read this before calling):
  * `fn` MUST be a TOP-LEVEL, importable function (referenced as
    ``module.qualname``): NOT a lambda, NOT a closure, NOT a nested function,
    NOT a bound method. On Windows the pool uses the 'spawn' start method — each
    worker is a FRESH interpreter that RE-IMPORTS `fn`'s module by qualified
    name. That module MUST import cleanly with NO import-time side effects (no
    server startup, no FastAPI/uvicorn import) and MUST be importable from the
    child's ``sys.path`` (spawn propagates the parent's ``sys.path``, so a
    process launched from the ``backend`` dir — uvicorn or pytest — can import
    the ``autotrade`` package in the child).
  * All kwargs passed via `run_offloaded` MUST be picklable (str / num / bool /
    None / list / dict / dataclass-of-those — NO DataFrame, NO sqlite
    connection, NO open file handle). The RETURN value must be picklable too.

LIFECYCLE:
  * The pool is a module-level, LAZILY-created (first call, NEVER at import),
    REUSED singleton — created under a lock, reused across all calls (never
    created/destroyed per call). `max_workers=2`.
  * A broken pool (a worker crashed) is detected and REBUILT on the next call,
    so one crash doesn't poison every subsequent call.
  * `shutdown()` is registered with `atexit` so workers never leak.

Every failure mode (submit failure / worker crash / BrokenProcessPool / timeout /
pickling error / an exception raised INSIDE `fn`) is surfaced as a clean,
catchable `OffloadError` — the caller degrades gracefully and NEVER crashes.
"""
from __future__ import annotations

import atexit
import logging
import threading
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import TimeoutError as _FutureTimeout
from concurrent.futures.process import BrokenProcessPool
from typing import Any, Callable, Optional

log = logging.getLogger("autotrade.compute_offload")

_MAX_WORKERS = 2

# Module-level singleton pool + its guard. Created lazily on first use, NEVER at
# import time (importing this module — including in a spawned child — must have
# no side effects).
_pool: Optional[ProcessPoolExecutor] = None
_pool_lock = threading.Lock()


class OffloadError(RuntimeError):
    """An off-process computation could not be completed.

    Raised for ANY failure (worker crash / BrokenProcessPool / timeout /
    pickling error / an exception inside the offloaded fn). Callers catch this
    single type and degrade gracefully — the raw underlying error never escapes.
    """


# ── pool lifecycle ───────────────────────────────────────────────────────────

def _get_pool() -> ProcessPoolExecutor:
    """Return the singleton pool, creating it on first use (under the lock)."""
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ProcessPoolExecutor(max_workers=_MAX_WORKERS)
            log.info("compute_offload: created ProcessPoolExecutor(max_workers=%d)",
                     _MAX_WORKERS)
        return _pool


def _reset_pool_locked() -> None:
    """Tear down the current pool and drop the reference so the next call
    rebuilds it. MUST be called while holding `_pool_lock`."""
    global _pool
    old = _pool
    _pool = None
    if old is not None:
        try:
            old.shutdown(wait=False, cancel_futures=True)
        except Exception as e:  # noqa: BLE001 — never let teardown raise
            log.warning("compute_offload: pool shutdown during reset failed: %s", e)


def reset_pool() -> None:
    """Force a pool rebuild on the next call (tests / recovery). Never raises."""
    with _pool_lock:
        _reset_pool_locked()


def shutdown() -> None:
    """Shut the pool down (registered via atexit so workers don't leak)."""
    with _pool_lock:
        _reset_pool_locked()


atexit.register(shutdown)


# ── the public entry point ───────────────────────────────────────────────────

def run_offloaded(fn: Callable[..., Any], /, timeout: Optional[float] = None,
                  **kwargs: Any) -> Any:
    """Run ``fn(**kwargs)`` in a separate PROCESS and return its result.

    `fn` MUST be a top-level, importable, picklable function and every kwarg +
    the return value MUST be picklable (see module docstring). `timeout` is in
    seconds (None = wait indefinitely).

    Raises `OffloadError` on ANY failure (submit failure, pickling error, worker
    crash / BrokenProcessPool, timeout, or an exception raised inside `fn`). On a
    broken pool (or a timeout that may have wedged a worker) the pool is REBUILT
    so the next call starts clean.
    """
    # ── submit ──
    try:
        pool = _get_pool()
        future = pool.submit(fn, **kwargs)
    except BrokenProcessPool as e:
        with _pool_lock:
            _reset_pool_locked()
        raise OffloadError("process pool was broken at submit (rebuilt): "
                           f"{e}") from e
    except Exception as e:  # e.g. pool already shut down
        raise OffloadError(f"failed to submit offloaded fn: {e}") from e

    # ── await the result ──
    try:
        return future.result(timeout=timeout)
    except _FutureTimeout as e:
        # A timed-out worker may still be spinning; rebuild the pool to reclaim
        # it rather than leave a wedged worker occupying a slot.
        future.cancel()
        with _pool_lock:
            _reset_pool_locked()
        raise OffloadError(
            f"offloaded fn timed out after {timeout}s (pool rebuilt)") from e
    except BrokenProcessPool as e:
        # Worker died mid-compute (segfault / OOM / os._exit). Rebuild so the
        # NEXT call is clean.
        with _pool_lock:
            _reset_pool_locked()
        raise OffloadError("process pool broke during execution (rebuilt): "
                           f"{e}") from e
    except Exception as e:
        # An exception raised INSIDE fn (re-raised by future.result), or an
        # unpicklable result. The pool is still healthy — no rebuild.
        raise OffloadError(f"offloaded fn raised: {e!r}") from e
