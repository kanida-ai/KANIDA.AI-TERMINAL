"""V7 daily pipeline orchestration — runs A2 -> A3 -> A4 in sequence.

Operator workflow:
  1. Operator refreshes Kite token at /falcon/admin (or via OAuth callback).
  2. admin_router.refresh_token() calls kick_off_v7_pipeline_if_stale().
  3. If today's daily_signals hasn't already succeeded (IST), this kicks off
     the 3-step V7 pipeline in a daemon thread:
       - daily_data_refresh (A2) — fetch latest Kite bars
       - daily_features     (A3) — compute features for any gap dates
       - daily_signals      (A4) — emit top-25 picks for the latest date
  4. The whole pipeline runs in ~30-60s. Operator polls /falcon/admin/runs.

Why this matters (bug fixed 2026-05-10):
  The original 16:05 IST cron (in main.py PIPELINE_STEPS) silently aborts
  on a bad Kite token. If the token expired Friday night, Saturday's auto-
  run aborts → no Friday signals. Now the operator can refresh anytime and
  immediately get a clean run.

Lock semantics:
  Single global lock (`_v7_pipeline_lock`). If a pipeline is already running,
  subsequent kick_off calls return False immediately. Prevents collision
  with the legacy 16:05 cron OR a manual /admin/rerun click.

Idempotency:
  Each step is itself idempotent (data_refresh fetches only NEW bars,
  features fills gap dates, signals upserts on signal_date+symbol+engine).
  Re-running mid-pipeline is safe.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Tuple

from ..db import falcon_conn

log = logging.getLogger("kanida.falcon.jobs.pipeline")
IST = timezone(timedelta(hours=5, minutes=30))

_v7_pipeline_lock = threading.Lock()


def _today_signals_completed_ist() -> bool:
    """True if today's IST daily_signals job has a 'success' audit row.
    Uses date(started_at) match — assumes started_at is stored as ISO string
    that lex-compares correctly (which falcon_signal_runs does)."""
    today_ist = datetime.now(IST).date().isoformat()
    with falcon_conn() as con:
        row = con.execute(
            """SELECT 1 FROM falcon_signal_runs
                WHERE job_name = 'daily_signals'
                  AND status   = 'success'
                  AND date(started_at) = ?
                LIMIT 1""",
            (today_ist,)
        ).fetchone()
    return row is not None


def is_pipeline_running() -> bool:
    """True if a pipeline run is currently in progress."""
    return _v7_pipeline_lock.locked()


def kick_off_v7_pipeline_if_stale(reason: str = "manual") -> dict:
    """Non-blocking. If today's daily_signals hasn't succeeded AND no other
    pipeline run is in flight, spawn a daemon thread that runs the 3-step
    V7 pipeline. Returns dict describing the decision.

    Args:
      reason — short string for the audit log (e.g. 'token_refresh', '16:05_cron')

    Returns:
      {kicked_off: bool, reason_skipped: str | None}
    """
    if _today_signals_completed_ist():
        return {"kicked_off": False, "reason_skipped": "ALREADY_COMPLETED_TODAY"}
    if not _v7_pipeline_lock.acquire(blocking=False):
        return {"kicked_off": False, "reason_skipped": "ALREADY_RUNNING"}

    # Lock acquired. Spawn the worker thread; it releases the lock on exit.
    t = threading.Thread(
        target=_run_v7_pipeline_holding_lock,
        args=(reason,),
        daemon=True,
        name="v7-pipeline-runner",
    )
    t.start()
    log.info("V7 pipeline kicked off (reason=%s)", reason)
    return {"kicked_off": True, "reason_skipped": None}


def _run_v7_pipeline_holding_lock(reason: str) -> None:
    """Sequential A2 -> A3 -> A4 with logging. Lock MUST be already held by caller.
    Releases lock in finally — never blocks future kick-offs on a crash."""
    try:
        # Lazy imports — avoids circular at module load time and keeps this
        # module cheap to import for callers who only check is_pipeline_running().
        from .daily_data_refresh import run as run_data
        from .daily_features     import run as run_features
        from .daily_signals      import run as run_signals

        steps: List[Tuple[str, Callable[[], dict]]] = [
            ("daily_data_refresh", run_data),
            ("daily_features",     run_features),
            ("daily_signals",      run_signals),
        ]
        log.info("V7 pipeline START (reason=%s) — running %d steps", reason, len(steps))
        for name, fn in steps:
            try:
                t0 = datetime.now(IST)
                result = fn() or {}
                dt = (datetime.now(IST) - t0).total_seconds()
                log.info("V7 pipeline: %s OK in %.1fs (status=%s)",
                          name, dt, result.get("status", "ok"))
                # If a step reports partial/failed, halt the chain — the operator
                # should see a partial outcome rather than cascading bad state into
                # signal generation.
                if result.get("status") and result["status"] not in ("success", "ok"):
                    log.warning("V7 pipeline: %s returned status=%s — halting chain",
                                 name, result["status"])
                    return
            except Exception as e:
                log.exception("V7 pipeline: %s CRASHED (chain halted): %s", name, e)
                return
        log.info("V7 pipeline COMPLETE (reason=%s)", reason)
    finally:
        _v7_pipeline_lock.release()
