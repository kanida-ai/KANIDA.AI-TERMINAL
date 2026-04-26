"""
KANIDA Signals — Roster Feedback from Paper Trades
====================================================
Closes the autonomous loop: live paper-trade outcomes feed back into the roster.

For every roster row we:
  1. Count paper_trades entered since the row's status_changed_at  →
     appearances_since_promotion.
  2. Among those that have closed (status != 'open'), compute the
     win fraction → win_rate_since_promotion.
  3. Optionally auto-demote an 'active' row whose live performance has
     decayed (win rate below threshold with enough live samples).

Demotion rules (active → watchlist):
  - appearances_since_promotion >= MIN_LIVE_SAMPLES (default 10)
  - win_rate_since_promotion    <  DEMOTE_WIN_RATE  (default 0.40)
  - demotion_reason logged      =  'paper_decay'

Never promotes from this module — promotion remains the job of roster.py
(which reads historical signal_outcomes and Wilson-bounded fitness).
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from .db import get_conn


MIN_LIVE_SAMPLES = 10
DEMOTE_WIN_RATE  = 0.40


# ──────────────────────────────────────────────────────────────────
# CORE UPDATE
# ──────────────────────────────────────────────────────────────────

def update_roster_feedback(
    conn: Optional[sqlite3.Connection] = None,
    market: Optional[str] = None,
    auto_demote: bool = True,
) -> dict:
    """
    Recompute appearances_since_promotion + win_rate_since_promotion
    for every roster row. Returns summary counts.

    Parameters
    ----------
    market : if provided, limit to that market (else all rows).
    auto_demote : if True, demote active rows whose live win rate has decayed.
    """
    own_conn = False
    if conn is None:
        conn = get_conn()
        own_conn = True

    try:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Pull all roster rows + paper_trade stats in one pass.
        # Paper trade is counted if entry_date >= status_changed_at's DATE portion.
        where_market = " AND r.market = ?" if market else ""
        params = (market,) if market else ()

        rows = conn.execute(
            f"""
            SELECT
                r.ticker, r.market, r.timeframe, r.strategy_name, r.bias,
                r.status, r.status_changed_at,
                COUNT(pt.id)                                       AS total_trades,
                SUM(CASE WHEN pt.status != 'open' THEN 1 ELSE 0 END) AS closed_trades,
                SUM(CASE WHEN pt.status != 'open' AND pt.win = 1 THEN 1 ELSE 0 END) AS wins
            FROM signal_roster r
            LEFT JOIN paper_trades pt
              ON  pt.ticker        = r.ticker
              AND pt.market        = r.market
              AND pt.timeframe     = r.timeframe
              AND pt.strategy_name = r.strategy_name
              AND pt.bias          = r.bias
              AND pt.entry_date   >= SUBSTR(r.status_changed_at, 1, 10)
            WHERE 1=1 {where_market}
            GROUP BY r.ticker, r.market, r.timeframe, r.strategy_name, r.bias
            """,
            params,
        ).fetchall()

        summary = {
            "rows_examined":        0,
            "rows_updated":         0,
            "rows_with_live_data":  0,
            "active_demoted":       0,
            "demotion_samples":     [],
        }

        for row in rows:
            (ticker, mkt, tf, strat, bias, status, changed_at,
             total_trades, closed_trades, wins) = row
            total_trades  = int(total_trades or 0)
            closed_trades = int(closed_trades or 0)
            wins          = int(wins or 0)

            summary["rows_examined"] += 1
            if total_trades > 0:
                summary["rows_with_live_data"] += 1

            win_rate = (wins / closed_trades) if closed_trades > 0 else None

            # Update appearances + win_rate
            with conn:
                conn.execute(
                    """UPDATE signal_roster
                       SET appearances_since_promotion = ?,
                           win_rate_since_promotion    = ?,
                           last_calibrated_at          = ?
                       WHERE ticker=? AND market=? AND timeframe=?
                         AND strategy_name=? AND bias=?""",
                    (total_trades, win_rate, now_str,
                     ticker, mkt, tf, strat, bias),
                )
            summary["rows_updated"] += 1

            # Auto-demote: active → watchlist on live decay
            if (auto_demote
                and status == "active"
                and closed_trades >= MIN_LIVE_SAMPLES
                and win_rate is not None
                and win_rate < DEMOTE_WIN_RATE):
                with conn:
                    conn.execute(
                        """UPDATE signal_roster
                           SET status              = 'watchlist',
                               previous_status     = 'active',
                               status_changed_at   = ?,
                               demotion_reason     = ?,
                               appearances_since_promotion = 0,
                               win_rate_since_promotion    = NULL
                           WHERE ticker=? AND market=? AND timeframe=?
                             AND strategy_name=? AND bias=?""",
                        (now_str,
                         f"paper_decay:win_rate={win_rate:.2f},n={closed_trades}",
                         ticker, mkt, tf, strat, bias),
                    )
                summary["active_demoted"] += 1
                if len(summary["demotion_samples"]) < 10:
                    summary["demotion_samples"].append(
                        (ticker, mkt, strat, bias,
                         round(win_rate, 3), closed_trades)
                    )

        return summary

    finally:
        if own_conn:
            conn.close()


# ──────────────────────────────────────────────────────────────────
# CLI-friendly entrypoint
# ──────────────────────────────────────────────────────────────────

def run(market: Optional[str] = None, auto_demote: bool = True) -> dict:
    print(f"\n── roster_feedback from paper_trades "
          f"(market={market or 'ALL'}, auto_demote={auto_demote}) ──")
    s = update_roster_feedback(market=market, auto_demote=auto_demote)
    print(f"  roster rows examined       : {s['rows_examined']:,}")
    print(f"  roster rows updated        : {s['rows_updated']:,}")
    print(f"  rows with live paper data  : {s['rows_with_live_data']:,}")
    print(f"  active rows auto-demoted   : {s['active_demoted']:,}")
    if s["demotion_samples"]:
        print(f"  sample demotions (ticker, market, strat, bias, win_rate, n):")
        for sample in s["demotion_samples"]:
            print(f"    {sample}")
    return s
