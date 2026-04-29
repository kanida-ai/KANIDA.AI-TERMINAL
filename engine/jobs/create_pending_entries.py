"""
KANIDA.AI — Create Pending Trade Entries from Live Opportunities
================================================================
Bridges the gap between live_opportunities (pattern detections) and trade_log
(live trades visible in the UI).

The backtest engine cannot create pending entries for the latest signal date
because the next bar (entry bar) does not exist yet. This script fills that gap.

For each rally signal in live_opportunities:
  - Computes entry_date = next NSE trading day after latest_date
  - Uses current_close as approximate entry price
  - Computes TP / SL from target_move and RR ratio
  - Inserts into trade_log with exit_date=NULL (pending entry)

This script is idempotent: it deletes any existing pending_entry rows for
today's signal batch before re-inserting.

Usage:
    python engine/jobs/create_pending_entries.py

Run AFTER: run_learning.py  (needs fresh live_opportunities rows)
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT    = Path(__file__).parent.parent.parent
DB_PATH = ROOT / "data" / "db" / "kanida_quant.db"
sys.path.insert(0, str(ROOT))

RR_RATIO = 2.0   # must match run_backtest.py


# ── Trading calendar helpers ──────────────────────────────────────────────────

_NSE_HOLIDAYS_2026 = {
    "2026-01-26",  # Republic Day
    "2026-03-02",  # Holi
    "2026-04-10",  # Good Friday (tentative)
    "2026-04-14",  # Dr. Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-08-15",  # Independence Day
    "2026-10-02",  # Gandhi Jayanti
    "2026-11-04",  # Diwali Laxmi Puja (tentative)
    "2026-11-25",  # Guru Nanak Jayanti (tentative)
    "2026-12-25",  # Christmas
}


def _next_trading_day(from_date: date) -> date:
    """Return the next NSE trading day (Mon–Fri, not an NSE holiday)."""
    d = from_date + timedelta(days=1)
    for _ in range(10):
        if d.weekday() < 5 and d.isoformat() not in _NSE_HOLIDAYS_2026:
            return d
        d += timedelta(days=1)
    return d  # fallback — should never reach here


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    today_str = date.today().isoformat()
    con       = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    print("=" * 60)
    print("KANIDA.AI — Create Pending Entries from Live Signals")
    print(f"  Today  : {today_str}")
    print(f"  DB     : {DB_PATH}")
    print("=" * 60)

    # ── Load latest rally live_opportunities ──────────────────────────────────
    opps = con.execute("""
        SELECT lo.*, pl.forward_window, pl.opportunity_score AS pl_opp_score
        FROM live_opportunities lo
        LEFT JOIN pattern_library pl ON (
            pl.market = lo.market
            AND pl.ticker = lo.ticker
            AND pl.direction = lo.direction
            AND pl.behavior_pattern = lo.behavior_pattern
        )
        WHERE lo.direction = 'rally'
        ORDER BY lo.decision_score DESC
    """).fetchall()

    all_rally = [dict(r) for r in opps]
    print(f"\nFound {len(all_rally)} rally opportunity rows in live_opportunities")

    # Deduplicate: keep the highest decision_score entry per ticker
    best: dict[str, dict] = {}
    for o in all_rally:
        t = o["ticker"]
        if t not in best or float(o.get("decision_score") or 0) > float(best[t].get("decision_score") or 0):
            best[t] = o
    rally_opps = list(best.values())
    print(f"  After dedup (best per ticker): {len(rally_opps)} unique tickers")

    if not rally_opps:
        print("  Nothing to do — no rally signals.")
        con.close()
        return

    # ── Determine signal batch date (latest_date across all opps) ─────────────
    batch_date = max(o["latest_date"][:10] for o in rally_opps)
    print(f"  Signal batch date : {batch_date}")

    # Compute entry_date = next NSE trading day after batch_date
    entry_date = _next_trading_day(date.fromisoformat(batch_date)).isoformat()
    print(f"  Entry date        : {entry_date}")

    # ── Delete old pending entries for this signal batch ──────────────────────
    deleted = con.execute("""
        DELETE FROM trade_log
        WHERE trade_type = 'backtest'
          AND direction = 'long'
          AND signal_date = ?
          AND exit_date IS NULL
    """, (batch_date,)).rowcount
    con.commit()
    if deleted:
        print(f"  Deleted {deleted} old pending-entry rows for {batch_date}")

    # ── Insert new pending entries ────────────────────────────────────────────
    rows_to_insert = []
    skipped = []

    for opp in rally_opps:
        ticker        = opp["ticker"]
        market        = opp["market"] or "NSE"
        target_move   = float(opp["target_move"] or 0.05)
        entry_p       = float(opp["current_close"] or 0)
        forward_window = int(opp.get("forward_window") or opp.get("pl_opp_score") or 20)
        # ↑ forward_window may be null if no pattern_library match; default 20 bars

        if entry_p <= 0:
            # Try to look up from ohlc_daily
            row = con.execute(
                "SELECT close FROM ohlc_daily WHERE market=? AND ticker=? ORDER BY trade_date DESC LIMIT 1",
                (market, ticker)
            ).fetchone()
            if row:
                entry_p = float(row["close"])

        if entry_p <= 0:
            skipped.append(ticker)
            continue

        tp = round(entry_p * (1 + target_move), 2)
        sl = round(entry_p * (1 - target_move / RR_RATIO), 2)

        notes = json.dumps({
            "bucket":              "standard",
            "signal_type":         "AI Pattern",
            "pattern":             opp.get("behavior_pattern", ""),
            "overlap":             round(float(opp.get("similarity") or 0), 3),
            "year":                batch_date[:4],
            "mfe_pct":             0.0,
            "mae_pct":             0.0,
            "mpi_pct":             0.0,
            "post_5d_pct":         0.0,
            "tier":                opp.get("tier", "medium"),
            "credibility":         opp.get("credibility", ""),
            "timeframe":           "1D",
            "signal_time":         f"{batch_date} 15:30:00 IST",
            "entry_time":          f"{entry_date} 09:15:00 IST",
            "signal_to_entry_mins": 1065,
            "delay_label":         "overnight",
            "reason_code":         "FRESH_SIGNAL",
            "multi_pattern_count": 1,
            "opportunity_score":   round(float(opp.get("opportunity_score") or 0), 4),
            "decision_score":      round(float(opp.get("decision_score") or 0), 4),
            "setup_summary":       opp.get("setup_summary", ""),
        })

        rows_to_insert.append({
            "market":            market,
            "ticker":            ticker,
            "trade_type":        "backtest",
            "direction":         "long",
            "signal_date":       batch_date,
            "entry_date":        entry_date,
            "entry_price":       round(entry_p, 2),
            "target_price":      tp,
            "stop_price":        sl,
            "exit_date":         None,
            "exit_price":        None,
            "exit_reason":       None,
            "days_held":         None,
            "pnl_pct":           None,
            "risk_reward_ratio": RR_RATIO,
            "notes":             notes,
        })

    sql = """
        INSERT INTO trade_log (
            market, ticker, trade_type, direction, signal_date, entry_date,
            entry_price, target_price, stop_price, exit_date, exit_price,
            exit_reason, days_held, pnl_pct, risk_reward_ratio, notes
        ) VALUES (
            :market, :ticker, :trade_type, :direction, :signal_date, :entry_date,
            :entry_price, :target_price, :stop_price, :exit_date, :exit_price,
            :exit_reason, :days_held, :pnl_pct, :risk_reward_ratio, :notes
        )
    """
    con.executemany(sql, rows_to_insert)
    con.commit()

    print(f"\n  Inserted {len(rows_to_insert)} pending-entry trades")
    if skipped:
        print(f"  Skipped {len(skipped)} (no price): {', '.join(skipped)}")

    print("\n  Pending entries created:")
    for r in rows_to_insert:
        print(f"    {r['ticker']:<12} entry={r['entry_date']} @ {r['entry_price']:.2f}"
              f"  TP={r['target_price']:.2f}  SL={r['stop_price']:.2f}")

    # ── Verify ────────────────────────────────────────────────────────────────
    count = con.execute(
        "SELECT COUNT(*) FROM trade_log WHERE trade_type='backtest' AND direction='long' AND exit_date IS NULL"
    ).fetchone()[0]
    print(f"\n  Total pending entries in trade_log: {count}")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
