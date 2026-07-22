"""Daily Kite OHLC refresh — incremental fetch for new bars only.

Fast path. For each Falcon-tracked symbol, fetch any bars since MAX(trade_date)
that's already in DB. Multi-worker, rate-limited 5 rps.
"""
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Daily bars are only FINAL after the 15:30 IST close. Fetch nothing past the
# last COMPLETED session — otherwise Kite returns today's still-forming intraday
# candle and it poisons the day's features + signals. Small settle margin.
_MARKET_CLOSE_SETTLE_IST = (15, 40)   # (hh, mm) IST after which today's daily bar is final
_REFETCH_LOOKBACK_DAYS   = 4          # re-fetch a trailing window so a FINAL bar
                                      # overwrites any PARTIAL bar (fetcher = OR REPLACE)

try:
    from zoneinfo import ZoneInfo
    _IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    _IST = None


def _now_ist() -> datetime:
    # Container clock is UTC; compute IST explicitly (never rely on TZ env).
    if _IST is not None:
        return datetime.now(_IST)
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


def _last_completed_session_iso() -> str:
    """ISO date of the last session whose daily bar is FINAL: today iff we are
    past the 15:40 IST settle, else the previous calendar day. Weekends/holidays
    are not modelled here — a weekend date simply yields no bars from Kite
    (harmless empty fetch), matching the pre-existing behaviour."""
    n = _now_ist()
    if (n.hour, n.minute) >= _MARKET_CLOSE_SETTLE_IST:
        return n.date().isoformat()
    return (n.date() - timedelta(days=1)).isoformat()

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

        # Window: end = last COMPLETED session (never today's forming bar);
        # start = a short trailing window back from MAX(trade_date) so a FINAL
        # bar OVERWRITES any PARTIAL bar a mid-session run may have written
        # (fetcher uses INSERT OR REPLACE). This is what makes the pipeline
        # self-heal instead of freezing a bad intraday candle.
        end   = _last_completed_session_iso()
        start = (date.fromisoformat(latest) - timedelta(days=_REFETCH_LOOKBACK_DAYS)).isoformat()
        if start > end:
            state["notes"] = f"no completed session past {latest} yet (pre-close) — nothing to fetch"
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
        state["notes"] = f"refreshed {s.get('rows_total', 0)} bars across {len(symbols)} symbols, {start} -> {end} (trailing re-fetch, final-bar overwrite)"
        return {"status": "success", **s}


if __name__ == "__main__":
    print(run())
