"""Daily Kite OHLC refresh — incremental fetch for new bars only.

Fast path. For each Falcon-tracked symbol, fetch any bars since MAX(trade_date)
that's already in DB. Multi-worker, rate-limited 5 rps.
"""
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Ensure the offline universe_engine module is importable for fetcher reuse.
ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT / "universe_engine"))

from ._run_log import log_job
from ..db import falcon_conn


def run() -> dict:
    with log_job("daily_data_refresh") as state:
        from engine.daily_eq_fetch import fetch_eq_daily

        with falcon_conn() as con:
            symbols = [r[0] for r in con.execute(
                "SELECT symbol FROM universe_master WHERE is_active = 1"
            )]
            latest = con.execute(
                "SELECT MAX(trade_date) FROM ohlc_daily"
            ).fetchone()[0]

        if not latest:
            state["notes"] = "no existing data — full backfill required, skipping daily delta"
            return {"status": "skipped", "reason": "no existing data"}

        # Fetch from latest+1 to today
        start = (date.fromisoformat(latest) + timedelta(days=1)).isoformat()
        end   = date.today().isoformat()
        if start > end:
            state["notes"] = f"already up to date through {latest}"
            return {"status": "noop", "latest": latest}

        from ..config import FALCON_DB
        s = fetch_eq_daily(FALCON_DB, symbols, start, end, n_workers=24, rps=5)
        # Surface a SAMPLE of fetch/auth errors so a systematic failure
        # (IP / historical-data permission / rate-limit) is diagnosable from the
        # logs instead of just a count. (cloud #-pipeline bring-up)
        _errs = [d.get("error") for d in (s.get("details") or {}).values()
                 if d.get("status") in ("fetch_error", "auth_error") and d.get("error")]
        if _errs:
            from collections import Counter
            for _msg, _cnt in Counter(_errs).most_common(3):
                print(f"[daily_data_refresh] fetch error x{_cnt}: {str(_msg)[:400]}", flush=True)
        s.pop("details", None)  # keep the returned/audited summary compact
        state["rows"]  = s.get("rows_total", 0)
        state["notes"] = f"fetched {s.get('rows_total', 0)} new bars across {len(symbols)} symbols, {start} -> {end}"
        return {"status": "success", **s}


if __name__ == "__main__":
    print(run())
