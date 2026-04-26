"""
KANIDA Signals — Calibration Engine
=====================================
Full calibration cycle for one stock or all stocks:

  1. Measure outcomes for any completed-but-unmeasured signal events
     (fills ret_1d..ret_30d, mfe, mae from ohlc_daily)
  2. Recompute stock_signal_fitness
  3. Update signal_roster
  4. Log calibration_runs audit row

Designed to run weekly (or on demand).
One logical worker per stock. All stocks run in a thread pool.
"""

import bisect
import sqlite3
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta, timezone
from typing import Optional

from .db import get_conn, SIGNALS_DB_PATH
from . import fitness as fitness_mod
from . import roster as roster_mod


# ──────────────────────────────────────────────────────────────────
# OUTCOME MEASUREMENT  (fill ret_1d..ret_30d, mfe, mae from OHLC)
# ──────────────────────────────────────────────────────────────────

_HORIZONS      = [1, 3, 5, 10, 15, 30]
_WIN_HORIZONS  = {5: "win_5d", 15: "win_15d", 30: "win_30d"}
_RET_COLS      = {h: f"ret_{h}d" for h in _HORIZONS}


def _col(h: int) -> str:
    return f"ret_{h}d"


def _load_ohlc(ticker: str, market: str, conn: sqlite3.Connection):
    """
    Load all OHLC for one stock into sorted parallel lists.
    Returns (dates, highs, lows, closes) — all same length, sorted ASC.
    """
    rows = conn.execute(
        """SELECT trade_date, high, low, close
           FROM ohlc_daily WHERE ticker=? AND market=?
           ORDER BY trade_date ASC""",
        (ticker, market),
    ).fetchall()
    if not rows:
        return [], [], [], []
    dates  = [r[0] for r in rows]
    highs  = [r[1] for r in rows]
    lows   = [r[2] for r in rows]
    closes = [r[3] for r in rows]
    return dates, highs, lows, closes


def _compute_row_updates(
    signal_date: str,
    entry_price: float,
    bias: str,
    existing_rets: dict,    # {horizon: current_value_or_None}
    existing_wins: dict,    # {horizon: current_value_or_None}
    existing_mfe: Optional[float],
    existing_mae: Optional[float],
    ohlc_dates: list,
    ohlc_highs: list,
    ohlc_lows: list,
    ohlc_closes: list,
) -> dict:
    """
    Compute updates for a single signal_outcomes row.

    Rules:
    - Each horizon is computed independently.
    - Existing non-NULL values are NEVER overwritten.
    - Returns a dict of {column: value} to SET, plus 'is_complete'.
    - Returns empty dict if nothing new can be computed.
    """
    # Find OHLC starting the day AFTER signal_date
    idx = bisect.bisect_right(ohlc_dates, signal_date)
    available = len(ohlc_dates) - idx
    if available == 0:
        return {}

    updates = {}

    # Return horizons
    for h in _HORIZONS:
        col = _RET_COLS[h]
        if existing_rets.get(h) is not None:
            continue                            # already filled — never overwrite
        if available >= h:
            ret = round((ohlc_closes[idx + h - 1] - entry_price) / entry_price * 100, 4)
            updates[col] = ret

            # Win flag for this horizon (if applicable and not yet set)
            if h in _WIN_HORIZONS:
                win_col = _WIN_HORIZONS[h]
                if existing_wins.get(h) is None:
                    updates[win_col] = 1 if (ret > 0 if bias == "bullish" else ret < 0) else 0

    # MFE / MAE — only fill if both are currently NULL
    window = min(available, 30)
    if existing_mfe is None and existing_mae is None and window > 0:
        sl = slice(idx, idx + window)
        if bias == "bullish":
            fav = [(ohlc_highs[i]  - entry_price) / entry_price * 100 for i in range(idx, idx + window)]
            adv = [(entry_price - ohlc_lows[i])   / entry_price * 100 for i in range(idx, idx + window)]
        else:
            fav = [(entry_price - ohlc_lows[i])   / entry_price * 100 for i in range(idx, idx + window)]
            adv = [(ohlc_highs[i]  - entry_price) / entry_price * 100 for i in range(idx, idx + window)]

        if fav and adv:
            mfe = max(fav)
            mae = max(adv)
            updates["mfe_pct"] = round(mfe, 4)
            updates["mae_pct"] = round(mae, 4)
            updates["mfe_day"] = fav.index(mfe) + 1
            updates["mae_day"] = adv.index(mae) + 1

    # is_complete: mark 1 if 30 trading days are now available
    if available >= 30:
        updates["is_complete"] = 1

    return updates


def fill_missing_outcomes(
    ticker: str,
    market: str,
    conn: sqlite3.Connection,
) -> int:
    """
    Weekly full-completion pass: find rows where is_complete=0 AND signal
    is >= 30 trading days old, then fill ALL horizons.
    Returns row count updated.
    """
    counts, _ = fill_incremental_outcomes(ticker, market, conn, only_incomplete=True)
    return sum(counts.values())


def fill_incremental_outcomes(
    ticker: str,
    market: str,
    conn: sqlite3.Connection,
    only_incomplete: bool = False,
) -> tuple[dict, int]:
    """
    Fill NULL return horizons for signal_outcome rows of one stock.

    Behaviour:
    - Works on ALL rows (is_complete=0 AND is_complete=1) unless
      only_incomplete=True restricts to is_complete=0.
    - Each horizon is filled independently — no horizon blocks another.
    - Never overwrites an existing non-NULL value.
    - Loads OHLC once per stock (efficient for bulk backfill).

    Returns (per_horizon_counts, total_rows_updated).
    """
    today = date.today().isoformat()

    if only_incomplete:
        where = "is_complete=0 AND signal_date < ?"
        params = (ticker, market, today)
    else:
        where = (
            "signal_date < ? "
            "AND (ret_1d IS NULL OR ret_3d IS NULL OR ret_5d IS NULL "
            "     OR ret_10d IS NULL OR ret_30d IS NULL)"
        )
        params = (ticker, market, today)

    pending = conn.execute(
        f"""SELECT id, signal_date, entry_price, bias,
                   ret_1d, ret_3d, ret_5d, ret_10d, ret_15d, ret_30d,
                   win_5d, win_15d, win_30d,
                   mfe_pct, mae_pct
            FROM signal_outcomes
            WHERE ticker=? AND market=? AND {where}""",
        params,
    ).fetchall()

    if not pending:
        return {h: 0 for h in _HORIZONS}, 0

    ohlc_dates, ohlc_highs, ohlc_lows, ohlc_closes = _load_ohlc(ticker, market, conn)
    if not ohlc_dates:
        return {h: 0 for h in _HORIZONS}, 0

    now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    horizon_counts = {h: 0 for h in _HORIZONS}
    rows_updated   = 0
    batch = []

    for row in pending:
        oid        = row[0]
        sig_date   = row[1]
        entry      = row[2]
        bias       = row[3]
        ex_rets    = {1: row[4], 3: row[5], 5: row[6], 10: row[7], 15: row[8], 30: row[9]}
        ex_wins    = {5: row[10], 15: row[11], 30: row[12]}
        ex_mfe     = row[13]
        ex_mae     = row[14]

        upd = _compute_row_updates(
            sig_date, entry, bias,
            ex_rets, ex_wins, ex_mfe, ex_mae,
            ohlc_dates, ohlc_highs, ohlc_lows, ohlc_closes,
        )

        if not upd:
            continue

        # Count per horizon
        for h in _HORIZONS:
            if _RET_COLS[h] in upd:
                horizon_counts[h] += 1

        upd["measured_at"] = now_str
        set_clause = ", ".join(f"{k}=?" for k in upd)
        vals       = list(upd.values()) + [oid]
        batch.append((set_clause, vals))
        rows_updated += 1

    with conn:
        for set_clause, vals in batch:
            conn.execute(
                f"UPDATE signal_outcomes SET {set_clause} WHERE id=?", vals
            )

    return horizon_counts, rows_updated


# ──────────────────────────────────────────────────────────────────
# SINGLE-STOCK CALIBRATION
# ──────────────────────────────────────────────────────────────────

def calibrate_stock(
    ticker: str,
    market: str,
    db_path: str = SIGNALS_DB_PATH,
) -> dict:
    """
    Full calibration cycle for one stock.
    Returns a summary dict suitable for calibration_runs.
    """
    t0 = time.time()
    conn = get_conn(db_path)

    # 1. Fill any unmeasured outcomes
    outcomes_filled = fill_missing_outcomes(ticker, market, conn)

    # 2. Recompute fitness
    fitness_rows = fitness_mod.compute_for_stock(ticker, market, conn)

    # 3. Update roster — capture before/after for change tracking
    before = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT strategy_name||'|'||bias||'|'||timeframe, status FROM signal_roster WHERE ticker=? AND market=?",
            (ticker, market)
        ).fetchall()
    }

    changes = roster_mod.update_roster_for_stock(ticker, market, conn)

    after = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT strategy_name||'|'||bias||'|'||timeframe, status FROM signal_roster WHERE ticker=? AND market=?",
            (ticker, market)
        ).fetchall()
    }

    # 4. Fitness distribution for this stock
    scores = [r[0] for r in conn.execute(
        "SELECT fitness_score FROM stock_signal_fitness WHERE ticker=? AND market=?",
        (ticker, market)
    ).fetchall()]

    p25 = p50 = p75 = None
    if scores:
        s = sorted(scores)
        n = len(s)
        p25 = round(s[int(n * 0.25)], 2)
        p50 = round(s[int(n * 0.50)], 2)
        p75 = round(s[int(n * 0.75)], 2)

    top = conn.execute(
        "SELECT strategy_name||' ('||bias||')', fitness_score FROM stock_signal_fitness "
        "WHERE ticker=? AND market=? ORDER BY fitness_score DESC LIMIT 1",
        (ticker, market)
    ).fetchone()

    elapsed_ms = int((time.time() - t0) * 1000)
    run_date = date.today().isoformat()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with conn:
        conn.execute(
            """INSERT OR REPLACE INTO calibration_runs (
                ticker, market, run_date, signals_evaluated,
                promoted_to_active, demoted_from_active,
                promoted_to_watchlist, newly_retired, unchanged,
                fitness_p25, fitness_p50, fitness_p75,
                top_signal, top_signal_fitness, run_duration_ms
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                ticker, market, run_date,
                changes.get("evaluated", 0),
                changes.get("promoted_to_active", 0),
                changes.get("demoted_from_active", 0),
                changes.get("promoted_to_watchlist", 0),
                changes.get("newly_retired", 0),
                changes.get("unchanged", 0),
                p25, p50, p75,
                top[0] if top else None,
                top[1] if top else None,
                elapsed_ms,
            )
        )

    conn.close()
    return {
        "ticker": ticker,
        "outcomes_filled": outcomes_filled,
        "fitness_rows": fitness_rows,
        "changes": changes,
        "elapsed_ms": elapsed_ms,
    }


# ──────────────────────────────────────────────────────────────────
# FULL BATCH CALIBRATION
# ──────────────────────────────────────────────────────────────────

def run_full_calibration(
    market: str = "NSE",
    db_path: str = SIGNALS_DB_PATH,
    workers: int = 20,
) -> None:
    conn = get_conn(db_path)
    tickers = [r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM signal_events WHERE market=? ORDER BY ticker", (market,)
    ).fetchall()]
    conn.close()

    if not tickers:
        print("No signal_events found. Run backfill first.")
        return

    print(f"\nFull Calibration — {market} — {len(tickers)} tickers — {workers} workers")
    print(f"  Steps per stock: fill outcomes -> fitness -> roster -> audit log\n")
    t0 = time.time()

    totals = {
        "outcomes_filled": 0, "fitness_rows": 0,
        "promoted_to_active": 0, "demoted_from_active": 0,
        "newly_retired": 0, "evaluated": 0,
    }
    done = 0

    def _worker(ticker: str):
        return calibrate_stock(ticker, market, db_path)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, t): t for t in tickers}
        for fut in as_completed(futures):
            result = fut.result()
            done += 1
            totals["outcomes_filled"] += result["outcomes_filled"]
            totals["fitness_rows"]    += result["fitness_rows"]
            ch = result["changes"]
            for k in ("promoted_to_active", "demoted_from_active", "newly_retired", "evaluated"):
                totals[k] += ch.get(k, 0)

            if done % 20 == 0 or done == len(tickers):
                print(f"  [{done}/{len(tickers)}] fitness={totals['fitness_rows']:,} "
                      f"active={totals['promoted_to_active']} "
                      f"retired={totals['newly_retired']}")

    elapsed = time.time() - t0
    print(f"\n  Finished in {elapsed:.1f}s")
    print(f"  Outcomes measured  : {totals['outcomes_filled']:,}")
    print(f"  Fitness rows       : {totals['fitness_rows']:,}")
    print(f"  Signals evaluated  : {totals['evaluated']:,}")
    print(f"  Promoted active    : {totals['promoted_to_active']:,}")
    print(f"  Newly retired      : {totals['newly_retired']:,}")
    print(f"  Demoted            : {totals['demoted_from_active']:,}")

    conn = get_conn(db_path)
    print("\n-- Final Roster --------------------------------------------")
    for r in conn.execute(
        "SELECT status, COUNT(*) FROM signal_roster WHERE market=? GROUP BY status ORDER BY COUNT(*) DESC",
        (market,)
    ).fetchall():
        print(f"  {r[0]:<12} {r[1]:>6,}")
    conn.close()


# ──────────────────────────────────────────────────────────────────
# BATCH RUNNERS — daily fill + one-time horizon backfill
# ──────────────────────────────────────────────────────────────────

def _batch_fill(
    market: str,
    db_path: str,
    workers: int,
    label: str,
    only_incomplete: bool,
) -> dict:
    """Shared parallel runner for both daily fill and bulk backfill."""
    conn = get_conn(db_path)
    tickers = [r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM signal_outcomes WHERE market=? ORDER BY ticker",
        (market,),
    ).fetchall()]
    conn.close()

    if not tickers:
        print(f"  {label} — no signal_outcomes found for {market}")
        return {}

    t0 = time.time()
    totals    = {h: 0 for h in _HORIZONS}
    total_rows = 0

    def _worker(ticker: str) -> tuple[dict, int]:
        c = get_conn(db_path)
        try:
            return fill_incremental_outcomes(ticker, market, c, only_incomplete=only_incomplete)
        finally:
            c.close()

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, t): t for t in tickers}
        for fut in as_completed(futures):
            h_counts, rows = fut.result()
            done += 1
            total_rows += rows
            for h in _HORIZONS:
                totals[h] += h_counts.get(h, 0)
            if done % 50 == 0 or done == len(tickers):
                print(f"  [{done}/{len(tickers)}] rows updated so far: {total_rows:,}")

    elapsed = time.time() - t0
    print(f"\n  {label} — {market} — {total_rows:,} rows updated in {elapsed:.1f}s")
    print(f"  Per-horizon fills:")
    for h in _HORIZONS:
        print(f"    ret_{h}d : {totals[h]:>8,}")
    return totals


def run_daily_outcome_fill(
    market: str = "NSE",
    db_path: str = SIGNALS_DB_PATH,
    workers: int = 20,
) -> int:
    """
    Daily batch: fill NULL return horizons for recent (is_complete=0) signals.
    Runs after evening OHLC sync.
    Returns total rows updated.
    """
    totals = _batch_fill(market, db_path, workers,
                         label="Daily outcome fill", only_incomplete=True)
    return sum(totals.values())


def run_backfill_all_horizons(
    market: str = "NSE",
    db_path: str = SIGNALS_DB_PATH,
    workers: int = 20,
) -> dict:
    """
    One-time (or periodic) bulk backfill: fill ALL NULL return horizons across
    ALL signal_outcome rows regardless of is_complete status.

    Use this to fix rows that were backfilled from the legacy DB with only
    ret_15d populated.  Safe to re-run — never overwrites non-NULL values.
    Returns per-horizon fill counts.
    """
    return _batch_fill(market, db_path, workers,
                       label="Bulk horizon backfill", only_incomplete=False)
