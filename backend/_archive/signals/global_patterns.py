"""
KANIDA Signals — Global Signal Pattern Discovery
=================================================
Aggregates stock_signal_fitness across all tickers to find signals
that work broadly (or in specific regimes) across many stocks.

Output: signal_global_patterns — one row per (market, timeframe, strategy, bias).

Used for cross-stock discovery:
  "This signal is active on 47/70 US stocks in UPTREND —
   test it on the remaining 23 where it hasn't been tried yet."

Run after weekly calibration (fitness + roster are already fresh).
"""

import time
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

from .db import get_conn, SIGNALS_DB_PATH


def compute_global_patterns(
    market: str,
    conn: sqlite3.Connection,
) -> int:
    """
    Aggregate fitness data across all tickers for one market.
    Returns number of pattern rows upserted.
    """
    rows = conn.execute(
        """
        SELECT
            f.timeframe, f.strategy_name, f.bias,
            f.fitness_score, f.best_trend_state,
            COALESCE(r.status, 'none') AS roster_status
        FROM stock_signal_fitness f
        LEFT JOIN signal_roster r
            ON  r.ticker        = f.ticker
            AND r.market        = f.market
            AND r.timeframe     = f.timeframe
            AND r.strategy_name = f.strategy_name
            AND r.bias          = f.bias
        WHERE f.market = ?
        """,
        (market,),
    ).fetchall()

    if not rows:
        return 0

    # Group by (timeframe, strategy_name, bias)
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        key = (r[0], r[1], r[2])
        groups[key].append(r)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    pattern_rows = []

    for (tf, strat, bias), records in groups.items():
        scores = sorted(r[3] for r in records)
        n = len(scores)

        stocks_active = sum(1 for r in records if r[5] == "active")

        p25 = scores[max(0, n // 4)]
        p50 = scores[n // 2]
        p75 = scores[min(n - 1, 3 * n // 4)]

        # Best trend state: most common best_trend_state among active tickers
        active_trend_states = [r[4] for r in records if r[5] == "active" and r[4]]
        if active_trend_states:
            best_ts = max(set(active_trend_states), key=active_trend_states.count)
        else:
            all_trend_states = [r[4] for r in records if r[4]]
            best_ts = max(set(all_trend_states), key=all_trend_states.count) if all_trend_states else None

        pattern_rows.append((
            market, tf, strat, bias,
            n,
            stocks_active,
            round(p50, 2),
            round(p25, 2),
            round(p75, 2),
            best_ts,
            now_str,
        ))

    with conn:
        conn.executemany(
            """INSERT OR REPLACE INTO signal_global_patterns (
                market, timeframe, strategy_name, bias,
                stocks_tested, stocks_active,
                median_fitness, p25_fitness, p75_fitness,
                best_trend_state, last_updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            pattern_rows,
        )

    return len(pattern_rows)


def run_all(
    market: str = "NSE",
    db_path: str = SIGNALS_DB_PATH,
) -> None:
    """Compute and print global pattern summary for a market."""
    t0 = time.time()
    conn = get_conn(db_path)
    n = compute_global_patterns(market, conn)

    print(f"\nGlobal Patterns — {market} — {n} signal patterns computed in {time.time()-t0:.1f}s")
    print(f"\n{'Strategy':<30} {'Bias':<10} {'Tf':<5} {'Tested':>8} {'Active':>8} "
          f"{'Median':>8} {'P25':>6} {'P75':>6} {'BestTrend'}")
    print("─" * 90)

    rows = conn.execute(
        """SELECT strategy_name, bias, timeframe,
                  stocks_tested, stocks_active,
                  median_fitness, p25_fitness, p75_fitness, best_trend_state
           FROM signal_global_patterns
           WHERE market=?
           ORDER BY stocks_active DESC, median_fitness DESC
           LIMIT 30""",
        (market,),
    ).fetchall()

    for r in rows:
        print(f"  {r[0]:<28} {r[1]:<10} {r[2]:<5} {r[3]:>8} {r[4]:>8} "
              f"{r[5] or 0:>8.1f} {r[6] or 0:>6.1f} {r[7] or 0:>6.1f} "
              f"  {r[8] or '—'}")

    conn.close()
