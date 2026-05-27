"""Generate today's Falcon signals + persist + notify.

Important — defaults are resolved at CALL time, not import time. A previous
bug (2026-05-12): bumping TOP_N_DEFAULT in config.py had no effect on the
already-running backend because Python captured the value when daily_signals
was first imported. Reading from config inside the function body fixes that.

Postflight invariant (2026-05-27):
  After generate_signals_for_date returns, verify the emitted signal_date is
  at least the expected one for the current IST window. If it's not, the run
  succeeded technically (no exception, rows upserted) but produced STALE
  data — typically because daily_data_refresh fell back to yesterday's bars
  (bad token, Kite outage, market still open). Raise StaleSignalsError so
  log_job records status='failed', the V7 chain halts, and the retry loop
  in _schedule_daily_pipeline picks it up within 15 min instead of leaving
  a false-success row that blocks future runs.
"""
from ._run_log import log_job
from ..services.notification import queue_signal_notification
from ..services.signal_runner import generate_signals_for_date


class StaleSignalsError(RuntimeError):
    """Raised by run() when emitted signal_date is older than expected for
    the current IST window. Caught by log_job context manager → records
    status='failed' in falcon_signal_runs → V7 chain treats as failure →
    retry loop kicks in 15 min later."""


def run(top_n: int | None = None, min_fires: int | None = None) -> dict:
    # Resolve defaults at call time so config edits don't require a restart.
    from ..config import TOP_N_DEFAULT, MIN_FIRES_DEFAULT
    if top_n is None:
        top_n = TOP_N_DEFAULT
    if min_fires is None:
        min_fires = MIN_FIRES_DEFAULT

    with log_job("daily_signals") as state:
        result = generate_signals_for_date(top_n=top_n, min_fires=min_fires)
        if "error" in result:
            state["notes"] = result["error"]
            return result
        state["rows"]  = result.get("n_picks", 0)
        emitted = result.get("signal_date")
        state["notes"] = (f"{result['n_picks']} picks emitted for "
                            f"signal_date={emitted}, "
                            f"entry_date={result.get('entry_date')}")

        # Postflight: did we actually advance signal_date to the expected one?
        # If not, refuse to mark the run "success" — that false-success was
        # the 2026-05-27 root cause (a 13:46 IST run wrote stale-OK to the
        # audit log, suppressing the 16:05 EOD cron).
        try:
            from ._pipeline import expected_signal_date_iso
            expected = expected_signal_date_iso()
        except Exception:
            expected = None
        if expected and emitted and emitted < expected:
            msg = (f"STALE_SIGNALS: emitted signal_date={emitted} but expected "
                   f">= {expected} for current IST window. Upstream data is "
                   f"likely stale (bad Kite token, market still open, or "
                   f"daily_data_refresh fell back).")
            state["notes"] += f" — {msg}"
            # Notification is intentionally NOT queued for stale emissions
            # (we don't want users pinged about yesterday's picks at 4 PM).
            raise StaleSignalsError(msg)

        try:
            queue_signal_notification(result)
        except Exception as e:
            state["notes"] += f" (notification failed: {e})"
        return result


if __name__ == "__main__":
    print(run())
