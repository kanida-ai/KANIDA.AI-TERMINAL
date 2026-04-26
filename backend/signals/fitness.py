"""
KANIDA Signals — Stock-Signal Fitness Computation
==================================================
Computes stock_signal_fitness for every (ticker, strategy, bias, timeframe).

Fitness formula:
  accuracy_score  = Wilson lower bound of win_rate_15d (90% CI)
  return_score    = sigmoid(avg_ret_15d / max(stddev_ret, 1.0))
  frequency_score = min(appearances_per_year / 24, 1.0)
  recency_score   = 0.5 + 0.5 * clamp(recency_decay, -1, 1)

  raw_fitness     = (0.35 * accuracy + 0.30 * return + 0.15 * freq + 0.20 * recency) * 100
  confidence      = min(1.0, sqrt(total_appearances / 30))
  fitness_score   = raw_fitness * confidence

Trend-state conditioned win rates are computed separately for
UPTREND / DOWNTREND / RANGE using trend_state_at_signal.

All computation is per-stock and can run in a thread pool.
"""

import math
import sqlite3
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from .db import get_conn, SIGNALS_DB_PATH

RECENT_DAYS = 90     # window for "recent" performance
MIN_YEARS   = 3.0    # assumed history length when computing frequency score


# ──────────────────────────────────────────────────────────────────
# MATH HELPERS
# ──────────────────────────────────────────────────────────────────

def wilson_lower(wins: int, n: int, z: float = 1.645) -> float:
    """Lower bound of Wilson score interval (90% confidence by default)."""
    if n == 0:
        return 0.0
    p = wins / n
    z2 = z * z
    center = p + z2 / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z2 / (4 * n * n))
    denom  = 1 + z2 / n
    return max(0.0, min(1.0, (center - margin) / denom))


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-max(-10.0, min(10.0, x))))


def return_score(avg_ret: float, stddev_ret: float) -> float:
    """Sigmoid-based score: positive avg return → above 0.5, negative → below."""
    denom = max(abs(stddev_ret), 1.0)
    return sigmoid(avg_ret / denom)


def confidence_mult(n: int) -> float:
    """sqrt(n/30) capped at 1.0. Reaches 1.0 at 30 appearances."""
    return min(1.0, math.sqrt(n / 30.0))


def _safe_median(vals: list[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    mid = len(s) // 2
    return (s[mid - 1] + s[mid]) / 2 if len(s) % 2 == 0 else s[mid]


def _safe_std(vals: list[float]) -> float:
    if len(vals) < 2:
        return 1.0
    return statistics.stdev(vals)


# ──────────────────────────────────────────────────────────────────
# PER-STOCK COMPUTATION
# ──────────────────────────────────────────────────────────────────

def compute_for_stock(
    ticker: str,
    market: str,
    conn: sqlite3.Connection,
) -> int:
    """
    Compute and upsert fitness rows for all strategies of one stock.
    Returns the number of fitness rows written.
    """
    from datetime import date, timedelta
    cutoff_recent = (
        date.today() - timedelta(days=RECENT_DAYS)
    ).strftime("%Y-%m-%d")

    # Pull all completed outcomes for this stock
    rows = conn.execute(
        """
        SELECT
            timeframe, strategy_name, bias,
            signal_date, ret_15d, win_15d,
            trend_state_at_signal
        FROM signal_outcomes
        WHERE ticker     = ?
          AND market     = ?
          AND win_15d    IS NOT NULL
          AND ret_15d    IS NOT NULL
        """,
        (ticker, market),
    ).fetchall()

    if not rows:
        return 0

    # Group by (timeframe, strategy_name, bias)
    from collections import defaultdict
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        key = (r[0], r[1], r[2])  # timeframe, strategy_name, bias
        groups[key].append(r)

    # First and last signal dates per group
    first_dates = conn.execute(
        """SELECT timeframe, strategy_name, bias, MIN(signal_date), MAX(signal_date)
           FROM signal_outcomes
           WHERE ticker=? AND market=?
           GROUP BY timeframe, strategy_name, bias""",
        (ticker, market),
    ).fetchall()
    date_map = {(r[0], r[1], r[2]): (r[3], r[4]) for r in first_dates}

    # MFE/MAE averages
    excursion_rows = conn.execute(
        """SELECT timeframe, strategy_name, bias,
                  AVG(mfe_pct), AVG(mae_pct)
           FROM signal_outcomes
           WHERE ticker=? AND market=?
             AND mfe_pct IS NOT NULL AND mae_pct IS NOT NULL
           GROUP BY timeframe, strategy_name, bias""",
        (ticker, market),
    ).fetchall()
    excursion_map = {(r[0], r[1], r[2]): (r[3], r[4]) for r in excursion_rows}

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    fitness_rows = []

    for (tf, strat, bias), records in groups.items():
        n = len(records)
        rets  = [r[4] for r in records]
        wins  = [r[5] for r in records]
        dates = [r[3] for r in records]
        states = [r[6] for r in records]

        total_wins = sum(w for w in wins if w is not None)

        # ── Full-history stats ──────────────────────────────────
        avg_ret    = sum(rets) / n
        med_ret    = _safe_median(rets)
        std_ret    = _safe_std(rets)
        win_rate   = total_wins / n

        # ── Recent stats ────────────────────────────────────────
        recent = [(r, w) for r, w, d in zip(rets, wins, dates) if d >= cutoff_recent]
        r_n          = len(recent)
        r_wins       = sum(w for _, w in recent if w is not None)
        recent_wr    = r_wins / r_n if r_n > 0 else win_rate
        recent_avg   = sum(r for r, _ in recent) / r_n if r_n > 0 else avg_ret

        # ── Recency decay ───────────────────────────────────────
        if win_rate > 0:
            decay = (recent_wr - win_rate) / win_rate
        else:
            decay = 0.0
        decay = max(-1.0, min(1.0, decay))

        # ── Trend-conditioned breakdown ──────────────────────────
        up_recs = [(r, w) for r, w, s in zip(rets, wins, states) if s == "UPTREND"]
        dn_recs = [(r, w) for r, w, s in zip(rets, wins, states) if s == "DOWNTREND"]
        rg_recs = [(r, w) for r, w, s in zip(rets, wins, states) if s == "RANGE"]

        def _wr(recs):
            if not recs:
                return None
            w = sum(x[1] for x in recs if x[1] is not None)
            return w / len(recs)

        wr_up = _wr(up_recs)
        wr_dn = _wr(dn_recs)
        wr_rg = _wr(rg_recs)

        # Best trend state
        candidates = {
            k: v for k, v in
            {"UPTREND": wr_up, "DOWNTREND": wr_dn, "RANGE": wr_rg}.items()
            if v is not None
        }
        best_ts = max(candidates, key=lambda k: candidates[k]) if candidates else None

        # ── Frequency score ──────────────────────────────────────
        first_d, last_d = date_map.get((tf, strat, bias), (dates[0], dates[-1]))
        try:
            from datetime import date as ddate
            d0 = ddate.fromisoformat(first_d)
            d1 = ddate.fromisoformat(last_d)
            years = max(0.5, (d1 - d0).days / 365.25)
        except Exception:
            years = MIN_YEARS
        app_per_year = n / years
        freq = min(1.0, app_per_year / 24.0)

        # ── Composite scores ─────────────────────────────────────
        acc   = wilson_lower(int(total_wins), n)
        ret_s = return_score(avg_ret, std_ret)
        rec_s = 0.5 + 0.5 * decay
        conf  = confidence_mult(n)

        raw = (0.35 * acc + 0.30 * ret_s + 0.15 * freq + 0.20 * rec_s) * 100
        final_fitness = round(raw * conf, 2)

        # ── Excursion ────────────────────────────────────────────
        avg_mfe, avg_mae = excursion_map.get((tf, strat, bias), (None, None))
        mfe_mae_ratio = None
        if avg_mfe is not None and avg_mae is not None and avg_mae != 0:
            mfe_mae_ratio = round(avg_mfe / abs(avg_mae), 3)

        fitness_rows.append((
            ticker, market, tf, strat, bias,
            n, r_n,
            round(win_rate,  4), round(acc, 4),
            round(avg_ret,   4), round(med_ret, 4), round(std_ret, 4),
            round(recent_wr, 4), round(recent_avg, 4),
            round(wr_up, 4) if wr_up is not None else None,
            round(wr_dn, 4) if wr_dn is not None else None,
            round(wr_rg, 4) if wr_rg is not None else None,
            len(up_recs), len(dn_recs), len(rg_recs),
            round(avg_mfe, 4) if avg_mfe is not None else None,
            round(avg_mae, 4) if avg_mae is not None else None,
            mfe_mae_ratio,
            round(acc,   4), round(ret_s, 4),
            round(freq,  4), round(rec_s, 4),
            round(conf,  4), round(raw,   2), final_fitness,
            best_ts, first_d, last_d,
            now_str,
        ))

    with conn:
        conn.executemany(
            """INSERT OR REPLACE INTO stock_signal_fitness (
                ticker, market, timeframe, strategy_name, bias,
                total_appearances, recent_appearances,
                win_rate_15d, wilson_lower_15d,
                avg_ret_15d, median_ret_15d, stddev_ret_15d,
                recent_win_rate_15d, recent_avg_ret_15d,
                win_rate_in_uptrend, win_rate_in_downtrend, win_rate_in_range,
                appearances_in_uptrend, appearances_in_downtrend, appearances_in_range,
                avg_mfe_pct, avg_mae_pct, mfe_mae_ratio,
                accuracy_score, return_score, frequency_score, recency_score,
                confidence_multiplier, raw_fitness, fitness_score,
                best_trend_state, first_signal_date, last_signal_date,
                last_calibrated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            fitness_rows,
        )

    return len(fitness_rows)


# ──────────────────────────────────────────────────────────────────
# BATCH RUNNER
# ──────────────────────────────────────────────────────────────────

def run_all(
    market: str = "NSE",
    db_path: str = SIGNALS_DB_PATH,
    workers: int = 20,
) -> None:
    conn_main = get_conn(db_path)
    tickers = [r[0] for r in conn_main.execute(
        "SELECT DISTINCT ticker FROM signal_outcomes WHERE market=? ORDER BY ticker", (market,)
    ).fetchall()]
    conn_main.close()

    if not tickers:
        print("No signal_outcomes found. Run backfill and OHLC sync first.")
        return

    print(f"\nFitness Computation — {market} — {len(tickers)} tickers — {workers} workers")
    t0 = time.time()
    total = 0
    done  = 0

    def _worker(ticker: str) -> tuple[str, int]:
        c = get_conn(db_path)
        try:
            return ticker, compute_for_stock(ticker, market, c)
        except Exception as exc:
            print(f"  [FAIL] {ticker}: {exc}")
            return ticker, 0
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, t): t for t in tickers}
        for fut in as_completed(futures):
            _, n = fut.result()
            done  += 1
            total += n
            if done % 20 == 0 or done == len(tickers):
                print(f"  [{done}/{len(tickers)}] {total:,} fitness rows")

    elapsed = time.time() - t0
    print(f"\n  Done in {elapsed:.1f}s — {total:,} stock_signal_fitness rows written")

    # Print score distribution summary
    conn_main = get_conn(db_path)
    print("\n── Fitness Score Distribution ──────────────────────────────")
    dist = conn_main.execute("""
        SELECT
            CASE
                WHEN fitness_score >= 70 THEN '70–100'
                WHEN fitness_score >= 55 THEN '55–70'
                WHEN fitness_score >= 40 THEN '40–55'
                WHEN fitness_score >= 25 THEN '25–40'
                ELSE                          '0–25'
            END  AS bucket,
            COUNT(*) AS cnt,
            ROUND(AVG(total_appearances), 0) AS avg_n,
            ROUND(AVG(win_rate_15d), 3) AS avg_wr
        FROM stock_signal_fitness WHERE market=?
        GROUP BY bucket ORDER BY bucket DESC
    """, (market,)).fetchall()
    print(f"\n  {'Bucket':<10} {'Count':>8}  {'Avg n':>7}  {'Avg WR':>8}")
    print("  " + "-" * 38)
    for r in dist:
        print(f"  {r[0]:<10} {r[1]:>8,}  {int(r[2] or 0):>7}  {(r[3] or 0):.3f}")
    conn_main.close()
