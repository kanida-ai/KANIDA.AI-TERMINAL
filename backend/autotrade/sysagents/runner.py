"""The monitor run-loop — a single daemon thread that runs the orchestrator on a
fixed cadence, ONLY while the layer is active, and is always stoppable.

SAFETY:
  * start() is a NO-OP unless the master flag is on (SYSAGENTS_ENABLED). So on a
    default (flag-off) boot the thread never starts — the layer is fully inert.
  * The loop re-checks flags.layer_active() every cycle: flip the kill-switch or
    the master flag off and the loop exits on its next wake (bounded by the
    interval). It is also stoppable in-process via stop().
  * Each cycle is wrapped so an exception can never kill the loop.
  * Idempotent: a second start() while running is a no-op.

Phase 1 has no actions, so the loop only ever observes + pages; stopping it only
stops observation + paging.
"""
from __future__ import annotations

import logging
import threading

from . import flags, orchestrator

log = logging.getLogger("kanida.sysagents.runner")

_THREAD = None
_STOP = threading.Event()
_LOCK = threading.Lock()


def _loop() -> None:
    log.info("sysagents: run-loop started (interval=%ss)", flags.run_interval_sec())
    while not _STOP.is_set():
        if not flags.layer_active():
            log.info("sysagents: layer no longer active — run-loop exiting")
            break
        try:
            view = orchestrator.run_once(page=True, persist=True)
            log.debug("sysagents: run status=%s", view.get("status"))
        except Exception as e:  # noqa: BLE001 — a cycle error never kills the loop
            log.warning("sysagents: run cycle raised (ignored): %s", e)
        # Wait the interval, but wake immediately on stop().
        _STOP.wait(timeout=flags.run_interval_sec())
    log.info("sysagents: run-loop stopped")


def start() -> bool:
    """Start the run-loop if the layer is active and it is not already running.
    Returns True if a thread was started, else False (disabled / already running).
    Never raises."""
    global _THREAD
    if not flags.layer_active():
        log.info("sysagents: start() skipped — layer not active (default-off)")
        return False
    with _LOCK:
        if _THREAD is not None and _THREAD.is_alive():
            return False
        _STOP.clear()
        _THREAD = threading.Thread(target=_loop, name="sysagents-runner",
                                   daemon=True)
        _THREAD.start()
        return True


def stop(timeout: float = 5.0) -> None:
    """Signal the run-loop to stop and join briefly. Never raises."""
    global _THREAD
    _STOP.set()
    with _LOCK:
        t = _THREAD
    if t is not None and t.is_alive():
        t.join(timeout=timeout)


def is_running() -> bool:
    return _THREAD is not None and _THREAD.is_alive()
