"""Top-level, picklable worker functions for the compute_offload tests.

These live in a DEDICATED, lightweight module (NOT the test module) so a spawned
worker can re-import them by qualified name (``tests.autotrade.offload_workers``)
without dragging in the heavy test-module imports (session.py etc.). Each fn is a
plain top-level function with picklable args/returns — the exact contract
`run_offloaded` requires.
"""
import os
import time


def return_pid(**kwargs):
    """Return the PID of the process this runs in (proves off-process execution)."""
    return os.getpid()


def add(a, b):
    """Trivial picklable compute (sanity)."""
    return a + b


def busy_spin(seconds=1.0, **kwargs):
    """GIL-HOLDING CPU work (faithful to the real pandas/numpy recompute) for
    ~`seconds`.

    Uses ``numpy.sort`` — a single C call that holds the CPython GIL for its WHOLE
    duration WITHOUT releasing at the interpreter switch interval. A pure-Python
    busy loop would NOT demonstrate event-loop starvation (CPython yields the GIL
    every ~5ms), so it would falsely "pass" even when run in-thread. numpy/pandas
    C loops (the actual Tesla recompute) do NOT yield — so IN A THREAD they freeze
    the async loop, and OFF-PROCESS they do not. That is exactly the contrast the
    non-blocking test asserts.
    """
    import numpy as np
    end = time.perf_counter() + float(seconds)
    n = 8_000_000            # ~1.2s per sort on this box; holds the GIL throughout
    iters = 0
    while time.perf_counter() < end:
        np.sort(np.random.rand(n))
        iters += 1
    return {"pid": os.getpid(), "iters": iters}


def sleep_worker(seconds=1.0, **kwargs):
    """Idle sleep (low CPU) — for the timeout test, so a timed-out orphan worker
    waits quietly instead of pegging a core (which could perturb timing-sensitive
    neighbouring tests)."""
    time.sleep(float(seconds))
    return os.getpid()


def raise_boom(**kwargs):
    """Raise inside the worker → run_offloaded must surface OffloadError."""
    raise ValueError("boom from worker")


def crash_hard(**kwargs):
    """Kill the worker process abruptly → BrokenProcessPool → OffloadError."""
    os._exit(1)
