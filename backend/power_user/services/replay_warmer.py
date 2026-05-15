"""Featured replay cache pre-warmer — Sprint 5c-2.

The landing page renders 3 featured replay cards. Each card's payload comes
from `falcon_replay_cache`. Cold-load through `build_replay_payload()` takes
~2.26s end-to-end (compute_top_n + outcomes join + per-pick narrative). That's
visible jank if the very first visitor on a fresh boot triggers the compute.

This module runs a daemon thread that re-checks every 6 hours that all
featured replays are pre-cached. On miss, it computes + stores. Idempotent —
already-cached rows are skipped. The boot lifespan calls precompute_featured()
once at startup, so this is the safety net for:

  * a code change in the explainer that invalidated the cached payload
  * an operator-issued cache flush
  * a long-running deployment where the cache rotated out
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

log = logging.getLogger("kanida.power_user.replay_warmer")
IST = timezone(timedelta(hours=5, minutes=30))

WARM_INTERVAL_SEC = 6 * 60 * 60      # 6 hours


_lock   = threading.Lock()
_stop   = threading.Event()
_thread: Optional[threading.Thread] = None

_state: Dict[str, Any] = {
    "started":      False,
    "started_at":   None,
    "last_run_at":  None,
    "last_summary": None,
    "next_run_at":  None,
    "last_error":   None,
}


def start() -> bool:
    """Launch the warmer thread. Idempotent — second call returns False."""
    global _thread
    if _thread is not None and _thread.is_alive():
        return False
    _stop.clear()
    _thread = threading.Thread(target=_run, name="replay-warmer", daemon=True)
    _thread.start()
    _state["started"]    = True
    _state["started_at"] = _now_ist().isoformat()
    log.info("replay_warmer: thread launched (interval=%ds)", WARM_INTERVAL_SEC)
    return True


def stop(timeout: float = 5.0) -> None:
    _stop.set()
    if _thread and _thread.is_alive():
        _thread.join(timeout=timeout)


def status() -> Dict[str, Any]:
    return dict(_state)


def _now_ist() -> datetime:
    return datetime.now(IST)


def warm_once(force: bool = False) -> Dict[str, Any]:
    """Run one warm pass. Returns the precompute summary. Safe to call ad-hoc
    from an admin endpoint — uses a fresh connection each invocation."""
    from ..config import POWER_DB_PATH
    from .replay_cache import precompute_featured

    con = sqlite3.connect(POWER_DB_PATH, timeout=30.0, check_same_thread=False)
    try:
        summary = precompute_featured(con, force=force)
        return summary
    finally:
        con.close()


def _run() -> None:
    """Daemon loop. Runs precompute_featured() every WARM_INTERVAL_SEC."""
    if not _lock.acquire(blocking=False):
        log.warning("replay_warmer: another instance holds the lock — exiting")
        return
    try:
        # First pass runs ~immediately (covers the case where lifespan
        # precompute partially failed). _stop.wait(0.5) is the tiny pause
        # so we don't race lifespan's own boot precompute.
        if _stop.wait(0.5):
            return
        while not _stop.is_set():
            try:
                summary = warm_once(force=False)
                _state["last_run_at"]  = _now_ist().isoformat()
                _state["last_summary"] = summary
                _state["last_error"]   = None
                log.info("replay_warmer: pass complete computed=%d skipped=%d errors=%d",
                          len(summary.get("computed", [])),
                          len(summary.get("skipped", [])),
                          len(summary.get("errors", [])))
            except Exception as e:
                _state["last_error"] = str(e)[:200]
                log.exception("replay_warmer: pass crashed (non-fatal)")

            next_run = _now_ist() + timedelta(seconds=WARM_INTERVAL_SEC)
            _state["next_run_at"] = next_run.isoformat()

            if _stop.wait(WARM_INTERVAL_SEC):
                return
    finally:
        _lock.release()
        log.info("replay_warmer: thread exiting")
