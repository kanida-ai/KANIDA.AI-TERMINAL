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
from typing import Callable, List, Optional, Tuple

from ..db import falcon_conn

log = logging.getLogger("kanida.falcon.jobs.pipeline")
IST = timezone(timedelta(hours=5, minutes=30))

_v7_pipeline_lock = threading.Lock()

# 16:05 IST is the canonical "today's bars are final, EOD pipeline window opens"
# moment. Before this on a weekday we have NO expectation of today's signals
# existing — so freshness checks short-circuit to "fresh enough". After this
# on a weekday, MAX(signal_date) MUST equal today.
EOD_WINDOW_HOUR   = 16
EOD_WINDOW_MINUTE = 5


def expected_signal_date_iso(now_ist: Optional[datetime] = None) -> Optional[str]:
    """The signal_date that SHOULD exist in falcon_signals_live for the current
    IST window, or None if no expectation has been triggered yet.

    Rules:
      - Weekend → None (market closed, no expectation)
      - Weekday before 16:05 IST → None (today's bars not final yet; the
        latest signal_date will legitimately still be the previous trading day)
      - Weekday on/after 16:05 IST → today's ISO date

    NSE holidays are not modelled here. On a holiday this still returns today,
    so freshness checks fail until tomorrow's EOD window opens (when the
    previous-trading-day date satisfies the check). The retry loop's 19:00 IST
    cutoff caps wasted retries to ~10 × 15 min, all short-circuiting at
    daily_data_refresh (no new bars to fetch). Acceptable.
    """
    n = now_ist or datetime.now(IST)
    if n.weekday() >= 5:
        return None
    if (n.hour, n.minute) < (EOD_WINDOW_HOUR, EOD_WINDOW_MINUTE):
        return None
    return n.date().isoformat()


def _latest_emitted_signal_date_iso() -> Optional[str]:
    """MAX(signal_date) from falcon_signals_live, or None on empty/error.
    Errors fail open (return None) → _signals_fresh_for_now() returns False
    → kick-off fires. Safer than masking real staleness."""
    try:
        with falcon_conn() as con:
            row = con.execute(
                "SELECT MAX(signal_date) FROM falcon_signals_live"
            ).fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:
        log.warning("_latest_emitted_signal_date_iso: query failed (%s) — fail-open", e)
        return None


def _signals_fresh_for_now() -> bool:
    """Replaces _today_signals_completed_ist (renamed 2026-05-27 fix).

    OLD gate (broken): 'did a daily_signals job that STARTED today succeed?'
      Bug: a manual or boot-catchup run firing BEFORE 16:05 IST emits
      yesterday's signal_date (market still open, today's bars not final),
      writes started_at=today, falsely marks the day "complete" — blocking
      the legitimate 16:05 IST EOD cron from ever firing. Real impact on
      2026-05-27: stale 2026-05-26 picks frozen on /power/today all day
      because a 13:46 IST manual run set the "completed" flag.

    NEW gate: 'is the EMITTED signal_date as fresh as it should be?'
      Source of truth: falcon_signals_live.MAX(signal_date) vs the date
      expected for the current IST window. Independent of WHEN runs fired,
      WHAT their started_at was, or whether the legacy pipeline status dict
      thinks today is done. Catches stale-success silently.
    """
    expected = expected_signal_date_iso()
    if expected is None:
        # Weekend or pre-16:05 weekday — no expectation. Treat as fresh so
        # callers short-circuit. Re-evaluates when the next EOD window opens.
        return True
    latest = _latest_emitted_signal_date_iso()
    return bool(latest) and latest >= expected


# Backwards-compat alias — test_known_failures.py imports this name to assert
# it's callable (regression guard for the 2026-05-13 boot-catchup contract).
# The semantically-meaningful callers (kick_off_v7_pipeline_if_stale) use the
# new function directly.
def _today_signals_completed_ist() -> bool:
    """Deprecated alias for _signals_fresh_for_now(). Kept callable for tests."""
    return _signals_fresh_for_now()


def is_pipeline_running() -> bool:
    """True if a pipeline run is currently in progress."""
    return _v7_pipeline_lock.locked()


def kick_off_v7_pipeline_if_stale(reason: str = "manual") -> dict:
    """Non-blocking. If today's signals aren't fresh AND no other pipeline run
    is in flight, spawn a daemon thread that runs the 3-step V7 pipeline.

    Args:
      reason — short string for the audit log (e.g. 'token_refresh', '16:05_cron')

    Returns:
      {kicked_off: bool, reason_skipped: str | None}
    """
    if _signals_fresh_for_now():
        return {"kicked_off": False, "reason_skipped": "ALREADY_FRESH"}
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
    """Sequential A2 -> A3 -> A4 -> top10_audit with logging. Lock MUST be
    already held by caller. Releases lock in finally — never blocks future
    kick-offs on a crash."""
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
                status = result.get("status", "ok")
                log.info("V7 pipeline: %s OK in %.1fs (status=%s)", name, dt, status)

                # Fix #6 (2026-05-25): differentiate 'failed' (hard halt) from
                # 'partial' (soft warn + continue). Previously the chain halted
                # on ANY non-success/non-ok status, which meant daily_features
                # returning 'partial' for a historical gap (e.g. 2026-05-08 has
                # 0/500 features due to a long-past ProcessPool crash) would
                # block today's signals from generating — even though today's
                # data was 100% fine. The user-facing impact: empty /power/today
                # for an entire day because of an old unrelated catch-up gap.
                #
                # New rules:
                #   - status='failed' → halt (step itself decided things are
                #     too broken to proceed)
                #   - status='partial' → warn + continue (today's data may be
                #     fine; let the next step decide based on what it sees)
                #   - any other unknown status → halt defensively
                if status == "failed":
                    log.warning("V7 pipeline: %s status=failed — halting chain", name)
                    return
                elif status == "partial":
                    log.warning("V7 pipeline: %s status=partial — continuing chain "
                                 "(today's data may still be usable; downstream step "
                                 "will fail loudly if not). notes=%s",
                                 name, str(result.get("notes", ""))[:200])
                elif status not in ("success", "ok"):
                    log.warning("V7 pipeline: %s returned unknown status=%s — "
                                 "halting chain (defensive)", name, status)
                    return
            except Exception as e:
                log.exception("V7 pipeline: %s CRASHED (chain halted): %s", name, e)
                return

        # F4 (2026-05-24): step 4 — Falcon Top 10 audit-trail population + close-job.
        # ISOLATED from the chain's halt semantics: any failure here is logged
        # but does NOT roll back the user-visible daily_signals success. Audit
        # is an internal drift-monitor; users see picks via daily_signals,
        # not via this table. Run AFTER daily_signals so today's emissions
        # are visible to the populate step + AFTER daily_data_refresh so
        # ohlc_daily has fresh bars for the close-job exit walk.
        try:
            from .top10_audit import run as run_audit
            t0 = datetime.now(IST)
            audit_result = run_audit() or {}
            dt = (datetime.now(IST) - t0).total_seconds()
            log.info("V7 pipeline: top10_audit OK in %.1fs (status=%s n_inserted=%s n_closed=%s)",
                      dt, audit_result.get("status"),
                      audit_result.get("n_inserted"), audit_result.get("n_closed"))
        except Exception as e:
            log.exception("V7 pipeline: top10_audit CRASHED (NON-fatal, chain continues): %s", e)

        # 2026-05-27: step 5 — Co-Trader portfolio EOD update. Previously this
        # ran ONLY inside _run_pipeline_sync (legacy chain) which aborted on
        # any bad-token preflight. Result: Co-Trading tab data stayed frozen
        # at the last legacy-success date — May 18 — even though V7 kept
        # writing fresh signals. Moving it into the V7 chain means it runs on:
        #   - 16:05 IST scheduled cron
        #   - admin "Run pipeline now" manual trigger
        #   - boot catch-up via kick_off_v7_pipeline_if_stale
        # Same isolation rule as audit step: NON-fatal failures here do not
        # block the V7 chain's success.
        try:
            from power_user.services import portfolio_engine
            from power_user.config import POWER_DB_PATH as _PDB
            import sqlite3 as _sq3
            today_iso = datetime.now(IST).date().isoformat()
            _pcon = _sq3.connect(_PDB, timeout=60.0)
            _pcon.row_factory = _sq3.Row
            try:
                t0 = datetime.now(IST)
                psum = portfolio_engine.run_eod_for_date(_pcon, today_iso)
                dt = (datetime.now(IST) - t0).total_seconds()
                ok = sum(1 for r in psum.get("portfolios", []) if "error" not in r)
                tot = len(psum.get("portfolios", []))
                log.info("V7 pipeline: portfolio_engine EOD OK in %.1fs (%d/%d portfolios for %s)",
                          dt, ok, tot, today_iso)
            finally:
                _pcon.close()
        except Exception as e:
            log.exception("V7 pipeline: portfolio_engine EOD CRASHED (NON-fatal, chain continues): %s", e)

        log.info("V7 pipeline COMPLETE (reason=%s)", reason)
    finally:
        _v7_pipeline_lock.release()
