"""
KANIDA Signals — Global Discovery Auto-Test
=============================================
Seeds probationary roster entries (status='test') on stocks that have NOT
yet evaluated a globally-strong signal.

Rationale (from vision):
  "If a signal is working effectively across multiple stocks... that
   signal should be marked as globally promising and tested for suitability
   on other stocks."

Flow
----
1. Read signal_global_patterns for each market.
2. Keep patterns where:
       stocks_active  >= MIN_ACTIVE_STOCKS   (default 5)
       median_fitness >= MIN_MEDIAN_FITNESS  (default 50)
3. For every ticker in sector_mapping that has NO roster row yet for
   that (timeframe, strategy, bias), insert one with status='test'.
4. Next nightly cycle → that stock's worker will fire signal_events for
   this strategy if the pattern matches, outcomes will accumulate, and
   the weekly fitness job will give it a real score.

This is the cross-pollination mechanism — it turns "strong on 40 stocks"
into a concrete probation on the other 150.

Never overwrites existing roster rows. Idempotent.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from .db import get_conn


MIN_ACTIVE_STOCKS    = 5
MIN_MEDIAN_FITNESS   = 50.0
MAX_SEEDS_PER_RUN    = 50_000   # safety cap


def seed_discovery_roster(
    market: str,
    conn: Optional[sqlite3.Connection] = None,
    min_active_stocks: int = MIN_ACTIVE_STOCKS,
    min_median_fitness: float = MIN_MEDIAN_FITNESS,
) -> dict:
    """
    For each globally-strong pattern in `market`, insert status='test'
    roster rows on tickers that don't yet have one.

    Returns summary counts.
    """
    own_conn = False
    if conn is None:
        conn = get_conn()
        own_conn = True

    try:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        patterns = conn.execute(
            """SELECT timeframe, strategy_name, bias,
                      stocks_tested, stocks_active, median_fitness, best_trend_state
               FROM signal_global_patterns
               WHERE market=?
                 AND stocks_active  >= ?
                 AND median_fitness >= ?
               ORDER BY stocks_active DESC, median_fitness DESC""",
            (market, min_active_stocks, min_median_fitness),
        ).fetchall()

        # All covered stocks for this market
        tickers = [r[0] for r in conn.execute(
            """SELECT ticker FROM sector_mapping
               WHERE market=? AND (is_index IS NULL OR is_index=0)""",
            (market,),
        ).fetchall()]

        summary = {
            "market":                 market,
            "patterns_considered":    len(patterns),
            "tickers_in_scope":       len(tickers),
            "seeds_inserted":         0,
            "seeds_skipped_existing": 0,
            "sample_seeds":           [],
        }

        if not patterns or not tickers:
            return summary

        to_insert: list[tuple] = []

        for (tf, strat, bias, n_tested, n_active, med_fit, best_ts) in patterns:
            # Tickers that already have a roster row for this (tf, strat, bias)
            existing = {
                r[0] for r in conn.execute(
                    """SELECT ticker FROM signal_roster
                       WHERE market=? AND timeframe=? AND strategy_name=? AND bias=?""",
                    (market, tf, strat, bias),
                ).fetchall()
            }

            for ticker in tickers:
                if ticker in existing:
                    summary["seeds_skipped_existing"] += 1
                    continue

                to_insert.append((
                    ticker, market, tf, strat, bias,
                    "test",          # status
                    0.0,             # fitness_score (probationary)
                    0.0,             # fitness_at_promotion
                    0,               # appearances_since_promotion
                    None,            # win_rate_since_promotion
                    None,            # trend_gate (no restriction at test stage)
                    None,            # previous_status
                    now_str,         # status_changed_at
                    f"global_discovery:median_fit={med_fit:.1f},n_active={n_active}",
                    now_str,         # last_calibrated_at
                ))
                if len(summary["sample_seeds"]) < 8:
                    summary["sample_seeds"].append(
                        (ticker, tf, strat, bias, round(med_fit, 1), n_active)
                    )

                if len(to_insert) >= MAX_SEEDS_PER_RUN:
                    break

            if len(to_insert) >= MAX_SEEDS_PER_RUN:
                break

        if to_insert:
            # INSERT OR IGNORE guards against race with concurrent roster updates
            with conn:
                conn.executemany(
                    """INSERT OR IGNORE INTO signal_roster (
                        ticker, market, timeframe, strategy_name, bias,
                        status, fitness_score, fitness_at_promotion,
                        appearances_since_promotion, win_rate_since_promotion,
                        trend_gate, previous_status, status_changed_at,
                        demotion_reason, last_calibrated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    to_insert,
                )
            # Count how many actually inserted (vs ignored due to UNIQUE)
            # Rough: MAX_SEEDS_PER_RUN cap + existing check make this usually tight.
            summary["seeds_inserted"] = len(to_insert)

        return summary

    finally:
        if own_conn:
            conn.close()


def run(market: str,
        min_active_stocks: int = MIN_ACTIVE_STOCKS,
        min_median_fitness: float = MIN_MEDIAN_FITNESS) -> dict:
    print(f"\n-- global_discovery seed (market={market}, "
          f"min_active_stocks={min_active_stocks}, "
          f"min_median_fitness={min_median_fitness}) --")
    s = seed_discovery_roster(
        market,
        min_active_stocks=min_active_stocks,
        min_median_fitness=min_median_fitness,
    )
    print(f"  patterns considered    : {s['patterns_considered']:,}")
    print(f"  tickers in scope       : {s['tickers_in_scope']:,}")
    print(f"  seeds inserted         : {s['seeds_inserted']:,}")
    print(f"  seeds skipped existing : {s['seeds_skipped_existing']:,}")
    if s["sample_seeds"]:
        print(f"  sample seeds (ticker, tf, strat, bias, median_fit, n_active):")
        for sd in s["sample_seeds"]:
            print(f"    {sd}")
    return s
