"""Refresh ohlc_weekly + falcon_features for new dates only.

Catch-up logic (added 2026-05-10 after Friday-skipped bug):
  Instead of trusting `MAX(falcon_features.trade_date)` as the cursor (which
  hides partial failures — 50/500 stocks computed but the rest silently
  errored, the cursor still advances), we explicitly enumerate dates in
  ohlc_daily that:
    (a) have NO features yet (missing date), OR
    (b) are under-filled (< 50% of active universe) — likely a partial run
  Then re-run feature extraction across the gap. After running, we VERIFY
  each requested date now has the expected count. Dates that remain
  empty/under-filled get logged into the run audit's `error` column so
  operators can see partial failures (e.g. "Saturday 14:05 wrote 0 features
  for 2026-05-08 -> 0.0s ProcessPool crash silently swallowed").

Operator-visible behavior:
  - notes: "weekly: N · features: M (caught up K dates: 2026-05-08, ...)"
  - error (set on partial fail): "PARTIAL: 2026-05-08 has 0/500 features
    after catch-up; ProcessPool likely failed silently"
"""
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple

ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT / "universe_engine"))

from ._run_log import log_job
from ..db import falcon_conn

# A date is "under-filled" if its feature row count is below this fraction
# of the active universe size. Real runs typically yield ~93-95% of universe
# (some stocks lack required look-back history). 50% is a permissive lower
# bound — anything below it is almost certainly a silent failure.
UNDERFILL_THRESHOLD = 0.50

# Catch-up only looks at dates within this many days of the latest ohlc_daily
# date. Prevents the auto-heal from accidentally back-filling years of
# history (production retention is ~30 days; we cap to 60 days for safety).
# If you genuinely need to back-fill older history, run extract_universe_features
# directly with explicit start/end dates — not via this catch-up cron.
CATCHUP_WINDOW_DAYS = 60

# Always FORCE-recompute the most recent N trade_dates, even when they already
# have a full feature set. Row PRESENCE can't detect a bar whose VALUE was
# corrected after features were first computed (e.g. a partial intraday bar later
# overwritten by the final EOD bar — the daily_data_refresh trailing re-fetch).
# extract_universe_features is idempotent (ON CONFLICT), so recompute is safe and
# this makes bar corrections propagate features -> signals with no manual delete.
RECOMPUTE_TRAILING_DAYS = 4


def _refresh_weekly(con: sqlite3.Connection) -> int:
    """Rebuild ohlc_weekly for the latest week only."""
    sql = """
        WITH latest_week AS (
            SELECT date(MAX(trade_date), 'weekday 0', '-6 days') AS wk_start
            FROM ohlc_daily
        )
        DELETE FROM ohlc_weekly
        WHERE week_start IN (SELECT wk_start FROM latest_week);
    """
    con.execute(sql)
    sql_insert = """
        INSERT INTO ohlc_weekly (symbol, week_start, open, high, low, close, volume, n_days)
        WITH latest_week AS (
            SELECT date(MAX(trade_date), 'weekday 0', '-6 days') AS wk_start
            FROM ohlc_daily
        ),
        ranked AS (
            SELECT
                symbol, trade_date, open, high, low, close, volume,
                date(trade_date, 'weekday 0', '-6 days') AS week_start,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, date(trade_date, 'weekday 0', '-6 days')
                    ORDER BY trade_date ASC) AS rn_asc,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, date(trade_date, 'weekday 0', '-6 days')
                    ORDER BY trade_date DESC) AS rn_desc
            FROM ohlc_daily
            WHERE date(trade_date, 'weekday 0', '-6 days')
                  IN (SELECT wk_start FROM latest_week)
        )
        SELECT
            symbol, week_start,
            MAX(CASE WHEN rn_asc = 1 THEN open END) AS open,
            MAX(high) AS high,
            MIN(low)  AS low,
            MAX(CASE WHEN rn_desc = 1 THEN close END) AS close,
            SUM(volume) AS volume,
            COUNT(*) AS n_days
        FROM ranked
        GROUP BY symbol, week_start
    """
    cur = con.execute(sql_insert)
    con.commit()
    return cur.rowcount or 0


def _identify_gap_dates(con: sqlite3.Connection, n_active: int) -> Tuple[List[str], List[Tuple[str, int]]]:
    """Find dates needing (re)computation, BOUNDED to the catch-up window.

    Returns:
      missing  — recent dates in ohlc_daily with ZERO feature rows (never run)
      partial  — (date, count) recent dates with features but below
                 UNDERFILL_THRESHOLD (likely a silent ProcessPool failure)

    Catch-up window: only ohlc_daily dates within CATCHUP_WINDOW_DAYS of the
    latest available trade_date are considered. This prevents accidental
    back-fill of years of history when running on a fresh-ish DB.
    """
    # Anchor the window to the LATEST trade_date in ohlc_daily, not "today" —
    # ensures the cron behaves the same whether run on Saturday afternoon
    # (after Friday's bars exist) or Wednesday during the week.
    last_daily = con.execute("SELECT MAX(trade_date) FROM ohlc_daily").fetchone()[0]
    if not last_daily:
        return [], []
    window_start = (date.fromisoformat(last_daily) - timedelta(days=CATCHUP_WINDOW_DAYS)).isoformat()

    # Missing dates: in ohlc (within window), not in features at all
    missing_rows = con.execute("""
        SELECT DISTINCT o.trade_date
          FROM ohlc_daily o
          LEFT JOIN (SELECT DISTINCT trade_date FROM falcon_features) f
            ON f.trade_date = o.trade_date
         WHERE f.trade_date IS NULL
           AND o.trade_date >= ?
         ORDER BY o.trade_date
    """, (window_start,)).fetchall()
    missing = [r[0] for r in missing_rows]

    # Partial dates: features exist (within window) but suspiciously few
    threshold_rows = max(1, int(n_active * UNDERFILL_THRESHOLD))
    partial_rows = con.execute("""
        SELECT trade_date, COUNT(*) AS n
          FROM falcon_features
         WHERE trade_date >= ?
      GROUP BY trade_date
        HAVING COUNT(*) < ?
      ORDER BY trade_date
    """, (window_start, threshold_rows)).fetchall()
    partial = [(r[0], r[1]) for r in partial_rows]

    # Force-recompute the most recent RECOMPUTE_TRAILING_DAYS trade_dates so a
    # CORRECTED bar (partial -> final EOD overwrite) refreshes its features even
    # though a full feature set already exists for that date. Folded into
    # `missing` (the recompute set); extract_universe_features overwrites via
    # ON CONFLICT, so re-doing an up-to-date date is a cheap no-op in effect.
    force_rows = con.execute(
        "SELECT DISTINCT trade_date FROM ohlc_daily WHERE trade_date >= ? "
        "ORDER BY trade_date DESC LIMIT ?",
        (window_start, RECOMPUTE_TRAILING_DAYS),
    ).fetchall()
    missing = sorted(set(missing) | {r[0] for r in force_rows})

    return missing, partial


def _refresh_features() -> dict:
    """Compute features for all gap dates (missing OR under-filled).

    Returns dict with:
      rows_written   — total rows added across all gap dates
      gap_dates      — dates we attempted to fill
      remaining_gaps — dates that still failed verification post-run
                       (operator alert)
    """
    from engine.falcon_features import extract_universe_features  # noqa: F401
    from ..config import FALCON_DB

    with falcon_conn() as con:
        rows = con.execute(
            "SELECT symbol FROM universe_master WHERE is_active = 1"
        ).fetchall()
        symbols = [r[0] for r in rows]
        n_active = len(symbols)

        last_daily = con.execute("SELECT MAX(trade_date) FROM ohlc_daily").fetchone()[0]
        if not last_daily or not symbols:
            return {"rows_written": 0, "gap_dates": [], "remaining_gaps": []}

        missing, partial = _identify_gap_dates(con, n_active)

    gap_dates = sorted(set(missing) | {d for d, _ in partial})
    if not gap_dates:
        return {"rows_written": 0, "gap_dates": [], "remaining_gaps": []}

    # Compute features across the full gap range. extract_universe_features
    # walks the full window per worker but only writes rows it can compute,
    # so passing min..max is safe (idempotent ON CONFLICT in the schema).
    start = gap_dates[0]
    end   = gap_dates[-1]
    print(f"[daily_features] gap detected: {len(gap_dates)} dates "
          f"({len(missing)} missing, {len(partial)} partial). "
          f"Computing range {start}..{end}", flush=True)
    s = extract_universe_features(FALCON_DB, symbols, start, end, n_workers=24)
    rows_written = int(s.get("rows_total", 0))

    # POST-RUN VERIFICATION: did each gap date actually get filled?
    threshold_rows = max(1, int(n_active * UNDERFILL_THRESHOLD))
    remaining_gaps: List[Tuple[str, int]] = []
    with falcon_conn() as con:
        for d in gap_dates:
            cnt = con.execute(
                "SELECT COUNT(*) FROM falcon_features WHERE trade_date=?", (d,)
            ).fetchone()[0]
            if cnt < threshold_rows:
                remaining_gaps.append((d, cnt))

    return {
        "rows_written":   rows_written,
        "gap_dates":      gap_dates,
        "remaining_gaps": remaining_gaps,
        "n_active":       n_active,
    }


def run() -> dict:
    with log_job("daily_features") as state:
        with falcon_conn() as con:
            t0 = time.time()
            wk_rows = _refresh_weekly(con)
            t1 = time.time()
        feat = _refresh_features()
        t2 = time.time()

        feat_rows = feat["rows_written"]
        gap_dates = feat.get("gap_dates", [])
        remaining = feat.get("remaining_gaps", [])

        state["rows"] = wk_rows + feat_rows

        notes_parts = [
            f"weekly: {wk_rows} ({t1-t0:.1f}s)",
            f"features: {feat_rows} ({t2-t1:.1f}s)",
        ]
        if gap_dates:
            preview = ", ".join(gap_dates[:3]) + ("..." if len(gap_dates) > 3 else "")
            notes_parts.append(f"caught up {len(gap_dates)} date(s): {preview}")
        state["notes"] = "  -  ".join(notes_parts)

        # Operator alert: if any gap date STILL didn't fill, surface it loudly.
        if remaining:
            n_active = feat.get("n_active", 0)
            details = ", ".join(f"{d}={cnt}/{n_active}" for d, cnt in remaining[:5])
            state["error"] = (
                f"PARTIAL: {len(remaining)} date(s) still under-filled after catch-up. "
                f"Likely ProcessPool silently crashed (Kite token expired during run? "
                f"OOM? worker import error?). Details: {details}. "
                f"Re-run daily_features manually OR check logs for worker errors."
            )

        return {
            "status":         "success" if not remaining else "partial",
            "weekly_rows":    wk_rows,
            "features_rows":  feat_rows,
            "gap_dates":      gap_dates,
            "remaining_gaps": remaining,
        }


if __name__ == "__main__":
    print(run())
