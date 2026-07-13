"""compute_offload — the reusable OFF-PROCESS compute utility.

Proves the utility that keeps heavy GIL-holding CPU work off the single async
event loop:
  * runs `fn` in a DIFFERENT process (own GIL),
  * a pooled singleton reused across calls (not rebuilt per call),
  * every failure mode (worker crash / broken pool / timeout / in-fn exception)
    surfaces as a clean, catchable `OffloadError` — and a broken pool REBUILDS,
  * NON-BLOCKING: an async coroutine keeps ticking at ~ms latency while a
    deliberately CPU-bound offloaded fn runs (the whole point).

Each proof carries a MUTATION note (what to break to make it fail).
"""
import asyncio
import os
import time

import pytest

from autotrade import compute_offload as co
from autotrade.compute_offload import OffloadError, run_offloaded

from tests.autotrade import offload_workers as w


@pytest.fixture(autouse=True)
def _fresh_pool():
    # Start each test from a clean pool and tear it down after so a deliberately
    # broken pool in one test can't leak into the next.
    co.reset_pool()
    yield
    co.reset_pool()


# ── runs OFF-PROCESS ─────────────────────────────────────────────────────────

def test_runs_in_a_different_process():
    child_pid = run_offloaded(w.return_pid, timeout=90)
    assert isinstance(child_pid, int)
    assert child_pid != os.getpid()      # executed in a SEPARATE process
    # MUTATION: run fn inline in the caller (return fn(**kwargs) in run_offloaded)
    # → child_pid == os.getpid() → fails.


def test_result_is_correct():
    assert run_offloaded(w.add, a=2, b=40, timeout=90) == 42


def test_pool_is_a_reused_singleton():
    p1 = co._get_pool()
    p2 = co._get_pool()
    assert p1 is p2                      # same object — never rebuilt per call
    # A submit reuses that same pool object.
    run_offloaded(w.add, a=1, b=1, timeout=90)
    assert co._get_pool() is p1


def test_no_pool_created_at_import():
    # Importing the module must NOT eagerly create a pool (lazy-only). After a
    # reset (no call since), the singleton is None until first use.
    co.reset_pool()
    assert co._pool is None
    run_offloaded(w.add, a=1, b=2, timeout=90)
    assert co._pool is not None
    # MUTATION: create the pool at module import / in reset → _pool not None here.


# ── graceful degradation ─────────────────────────────────────────────────────

def test_worker_exception_becomes_offload_error():
    with pytest.raises(OffloadError):
        run_offloaded(w.raise_boom, timeout=90)


def test_broken_pool_becomes_offload_error_then_rebuilds():
    # A worker that abruptly dies → BrokenProcessPool → OffloadError.
    with pytest.raises(OffloadError):
        run_offloaded(w.crash_hard, timeout=90)
    # The pool must be rebuilt so the NEXT call succeeds (not permanently poisoned).
    assert run_offloaded(w.add, a=3, b=4, timeout=90) == 7
    # MUTATION: drop the `_reset_pool_locked()` on BrokenProcessPool in
    # run_offloaded → the 2nd call raises OffloadError too → fails.


def test_timeout_becomes_offload_error():
    with pytest.raises(OffloadError):
        run_offloaded(w.sleep_worker, seconds=5.0, timeout=0.3)
    # And the pool recovers for the next call (rebuilt after the timeout).
    assert run_offloaded(w.add, a=5, b=5, timeout=90) == 10


def test_unpicklable_fn_becomes_offload_error():
    # A lambda is not importable/picklable → clean OffloadError, no raw crash.
    with pytest.raises(OffloadError):
        run_offloaded(lambda: 1, timeout=90)


# ── NON-BLOCKING: the event loop keeps progressing during a heavy compute ────

def test_offloaded_compute_does_not_stall_the_event_loop():
    """While a heavy ~2s CPU compute runs OFF-PROCESS, an async coroutine on THIS
    process's loop keeps ticking at ~ms latency — the loop is NEVER frozen for the
    duration of the compute.

    NOTE on the mechanism (measured on this Python 3.13 / numpy build): the fix's
    guarantee is that the compute runs in a SEPARATE PROCESS (proven, mutation-
    verified, by `test_runs_in_a_different_process`). The remaining way the loop
    could still stall is if the blocking subprocess WAIT (`future.result()`) were
    performed ON the event-loop thread. So this test drives the wait through
    `run_in_executor` (exactly like the daemon-thread refresh path, which waits
    off the loop) and proves the loop stays responsive. The deterministic
    mutation below (wait on the loop thread) freezes it for the whole compute."""
    BUSY_SECONDS = 2.0

    async def _driver():
        loop = asyncio.get_running_loop()
        ticks = []
        done = {"v": False}

        async def ticker():
            # Sleep in small increments and record the ACTUAL latency of each
            # wake-up. If the loop were blocked, gaps balloon to ~seconds.
            prev = loop.time()
            while not done["v"]:
                await asyncio.sleep(0.01)
                now = loop.time()
                ticks.append(now - prev)
                prev = now

        async def heavy():
            # Wait on the blocking off-process call in a THREAD executor — never on
            # the loop thread. Mirrors how the daemon-thread refresh path waits on
            # the subprocess. (MUTATION: replace this with a direct blocking
            # `run_offloaded(w.busy_spin, seconds=BUSY_SECONDS, timeout=90)` on the
            # loop thread → the loop freezes for the whole ~2s compute → worst gap
            # ≈ BUSY_SECONDS → the `worst < 0.2` assertion fails.)
            await loop.run_in_executor(
                None, lambda: run_offloaded(w.busy_spin, seconds=BUSY_SECONDS,
                                            timeout=90))
            done["v"] = True

        t = asyncio.create_task(ticker())
        await heavy()
        await t
        return ticks

    ticks = asyncio.run(_driver())

    # The loop must have woken MANY times during the ~2s+ compute (not frozen).
    assert len(ticks) > 50, f"loop starved: only {len(ticks)} ticks"
    # No single wake-up anywhere near the compute duration — the loop was live
    # throughout (off-process compute contends for nothing on this process).
    worst = max(ticks)
    assert worst < 0.2, f"event loop stalled for {worst:.3f}s during offload"
