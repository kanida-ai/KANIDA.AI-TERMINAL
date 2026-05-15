"""Run-log helper — every cron job uses this to write to falcon_signal_runs."""
from contextlib import contextmanager
from typing import Iterator

from ..db import falcon_conn


@contextmanager
def log_job(job_name: str) -> Iterator[dict]:
    """Context manager that creates a 'running' row, then updates to
    success/failed on exit."""
    state = {"rows": 0, "notes": ""}
    with falcon_conn() as con:
        cur = con.execute(
            "INSERT INTO falcon_signal_runs (job_name) VALUES (?)", (job_name,)
        )
        run_id = cur.lastrowid
        con.commit()

    try:
        yield state
    except Exception as e:
        with falcon_conn() as con:
            con.execute("""
                UPDATE falcon_signal_runs
                   SET finished_at = datetime('now'), status='failed', error=?
                 WHERE id=?
            """, (str(e), run_id))
            con.commit()
        raise
    else:
        with falcon_conn() as con:
            con.execute("""
                UPDATE falcon_signal_runs
                   SET finished_at = datetime('now'), status='success',
                       rows_affected = ?, notes = ?
                 WHERE id=?
            """, (int(state.get("rows") or 0), state.get("notes") or "", run_id))
            con.commit()
