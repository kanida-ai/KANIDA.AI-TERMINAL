"""Generate today's Falcon signals + persist + notify.

Important — defaults are resolved at CALL time, not import time. A previous
bug (2026-05-12): bumping TOP_N_DEFAULT in config.py had no effect on the
already-running backend because Python captured the value when daily_signals
was first imported. Reading from config inside the function body fixes that.
"""
from ._run_log import log_job
from ..services.notification import queue_signal_notification
from ..services.signal_runner import generate_signals_for_date


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
        state["notes"] = (f"{result['n_picks']} picks emitted for "
                            f"signal_date={result['signal_date']}, "
                            f"entry_date={result.get('entry_date')}")
        try:
            queue_signal_notification(result)
        except Exception as e:
            state["notes"] += f" (notification failed: {e})"
        return result


if __name__ == "__main__":
    print(run())
