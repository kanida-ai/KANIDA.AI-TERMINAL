"""
KANIDA.AI — Execution Intelligence Analysis

Runs the execution decision engine over every historical trade in trade_log.
Produces the execution_log table, which powers the "Execution IQ" dashboard.

Usage:
    python engine/backtest/run_execution_analysis.py [--dry-run]

Steps:
  1. Create / refresh execution_log table
  2. For each trade_log row:
       - get signal-day OHLCV (prev_close)
       - get entry-day OHLCV  (open/high/low/close)
       - get NIFTY entry-day open/close (from ohlc_daily — populated by Kite ingest)
       - call execution_engine.analyze()
       - compute smart P&L: (exit_price - smart_entry) / smart_entry * 100
  3. Bulk-insert into execution_log
  4. Print comparison summary: blind vs smart

Column note: trade_log primary key is 'id' (not 'trade_id').
NIFTY data: fetched via Kite Connect in the daily OHLCV pipeline (fetch_fno_kite.py).
            yfinance is NOT used here. If NIFTY data is missing, NIFTY context is
            skipped gracefully — trade P&L analysis still runs correctly.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

# ── Path setup ───────────────────────────────────────────────────────────────
ROOT  = Path(__file__).parent.parent.parent
DB    = ROOT / "data" / "db" / "kanida_quant.db"
sys.path.insert(0, str(ROOT))

from engine.backtest.execution_engine import analyze, Exec, NO_TRADE_CODES


# ── Schema ────────────────────────────────────────────────────────────────────

CREATE_EXEC_LOG = """
CREATE TABLE IF NOT EXISTS execution_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_log_id     INTEGER NOT NULL,           -- FK → trade_log.id
    ticker           TEXT    NOT NULL,
    direction        TEXT    NOT NULL,
    signal_date      TEXT    NOT NULL,
    entry_date       TEXT    NOT NULL,

    -- Execution decision
    exec_code        TEXT    NOT NULL,
    trade_taken      INTEGER NOT NULL,           -- 1 = trade taken, 0 = no-trade
    entry_window     TEXT,
    exec_notes       TEXT,

    -- Gap / day-move inputs
    prev_close       REAL,
    entry_open       REAL,
    entry_high       REAL,
    entry_low        REAL,
    entry_close      REAL,
    gap_pct          REAL,
    gap_category     TEXT,
    day_move_pct     REAL,
    day_range_pct    REAL,

    -- NIFTY context
    nifty_open       REAL,
    nifty_close      REAL,
    nifty_day_move   REAL,
    nifty_is_weak    INTEGER,                    -- 0/1
    rs_vs_nifty      REAL,                       -- stock day_move - nifty day_move

    -- P&L comparison
    blind_entry_price  REAL,
    smart_entry_price  REAL,
    exit_price         REAL,
    blind_pnl_pct      REAL,
    smart_pnl_pct      REAL,                    -- NULL if no-trade
    pnl_improvement    REAL,                    -- smart - blind (NULL if no-trade)

    created_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(trade_log_id)
)
"""

CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_execlog_ticker ON execution_log(ticker);
CREATE INDEX IF NOT EXISTS idx_execlog_exec_code ON execution_log(exec_code);
CREATE INDEX IF NOT EXISTS idx_execlog_trade_taken ON execution_log(trade_taken);
"""


# ── OHLC lookup ───────────────────────────────────────────────────────────────

def _load_ohlc(conn: sqlite3.Connection, ticker: str) -> dict[str, dict]:
    """Return {trade_date: {open,high,low,close}} for a ticker."""
    cur = conn.cursor()
    cur.execute("""
        SELECT trade_date, open, high, low, close
        FROM ohlc_daily WHERE ticker = ?
        ORDER BY trade_date
    """, (ticker,))
    return {
        row[0]: {"open": row[1], "high": row[2], "low": row[3], "close": row[4]}
        for row in cur.fetchall()
    }


def _prev_trading_date(ohlc_map: dict, target_date: str) -> Optional[str]:
    """Return the most-recent trading date before target_date."""
    dates = sorted(ohlc_map.keys())
    for d in reversed(dates):
        if d < target_date:
            return d
    return None


# ── Core runner ───────────────────────────────────────────────────────────────

def run(dry_run: bool = False) -> None:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # 1. Schema — drop and recreate to pick up schema changes cleanly
    print("\n[1/3] Creating execution_log table ...")
    conn.execute("DROP TABLE IF EXISTS execution_log")
    conn.executescript(CREATE_EXEC_LOG)
    for stmt in CREATE_INDEX.strip().split("\n"):
        if stmt.strip():
            conn.execute(stmt.strip())
    conn.commit()

    # 2. Load all OHLC data into memory (one query per ticker)
    print("[2/3] Loading OHLC data for all tickers …")
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT ticker FROM trade_log")
    stock_tickers = [r[0] for r in cur.fetchall()]
    ohlc: dict[str, dict[str, dict]] = {t: _load_ohlc(conn, t) for t in stock_tickers}
    ohlc["NIFTY50"] = _load_ohlc(conn, "NIFTY50")
    print(f"  Tickers: {stock_tickers}")

    # 3. Iterate trades
    print("[3/3] Running execution analysis over trade_log …")
    cur.execute("""
        SELECT id, ticker, direction, signal_date, entry_date,
               entry_price, exit_price, notes
        FROM trade_log
        ORDER BY id
    """)
    trades = cur.fetchall()
    print(f"  Total trades: {len(trades)}")

    rows_to_insert: list[dict] = []
    skipped = 0

    for trade in trades:
        trade_id   = trade["id"]
        ticker     = trade["ticker"]
        direction  = trade["direction"]  # 'rally' or 'decline' or 'long'/'short'
        signal_date = trade["signal_date"][:10]
        entry_date  = trade["entry_date"][:10]
        blind_entry = float(trade["entry_price"])
        exit_price  = float(trade["exit_price"])

        # Normalise direction to long/short
        if direction in ("rally", "long"):
            dir_norm = "long"
        else:
            dir_norm = "short"

        stock_ohlc  = ohlc.get(ticker, {})
        nifty_ohlc  = ohlc.get("NIFTY50", {})

        # Get prev_close = signal_date's close (the bar the pattern fired on)
        sig_bar  = stock_ohlc.get(signal_date)
        prev_close = float(sig_bar["close"]) if sig_bar else None

        # Get entry-day OHLCV
        entry_bar = stock_ohlc.get(entry_date)
        if not entry_bar or prev_close is None:
            skipped += 1
            continue

        entry_open  = float(entry_bar["open"])
        entry_high  = float(entry_bar["high"])
        entry_low   = float(entry_bar["low"])
        entry_close = float(entry_bar["close"])

        # NIFTY context
        nifty_bar  = nifty_ohlc.get(entry_date)
        nifty_open = float(nifty_bar["open"])  if nifty_bar else None
        nifty_close= float(nifty_bar["close"]) if nifty_bar else None

        # Run engine
        result = analyze(
            direction=dir_norm,
            prev_close=prev_close,
            entry_open=entry_open,
            entry_high=entry_high,
            entry_low=entry_low,
            entry_close=entry_close,
            nifty_open=nifty_open,
            nifty_close=nifty_close,
        )

        # P&L calcs
        is_long = dir_norm == "long"

        def pnl(entry: float, exit_p: float) -> float:
            if is_long:
                return (exit_p - entry) / entry * 100
            else:
                return (entry - exit_p) / entry * 100

        blind_pnl = pnl(blind_entry, exit_price)
        smart_pnl: Optional[float] = None
        pnl_imp:   Optional[float] = None

        if result.trade_taken and result.entry_price:
            smart_pnl = pnl(result.entry_price, exit_price)
            pnl_imp   = smart_pnl - blind_pnl

        rows_to_insert.append({
            "trade_log_id":    trade_id,
            "ticker":          ticker,
            "direction":       dir_norm,
            "signal_date":     signal_date,
            "entry_date":      entry_date,
            "exec_code":       result.exec_code,
            "trade_taken":     1 if result.trade_taken else 0,
            "entry_window":    result.entry_window,
            "exec_notes":      result.notes,
            "prev_close":      prev_close,
            "entry_open":      entry_open,
            "entry_high":      entry_high,
            "entry_low":       entry_low,
            "entry_close":     entry_close,
            "gap_pct":         result.gap_pct,
            "gap_category":    result.gap_category,
            "day_move_pct":    result.day_move_pct,
            "day_range_pct":   result.day_range_pct,
            "nifty_open":      nifty_open,
            "nifty_close":     nifty_close,
            "nifty_day_move":  result.nifty_day_move,
            "nifty_is_weak":   1 if result.nifty_is_weak else 0,
            "rs_vs_nifty":     result.rs_vs_nifty,
            "blind_entry_price":  blind_entry,
            "smart_entry_price":  result.entry_price,
            "exit_price":         exit_price,
            "blind_pnl_pct":      blind_pnl,
            "smart_pnl_pct":      smart_pnl,
            "pnl_improvement":    pnl_imp,
        })

    print(f"  Processed: {len(rows_to_insert)}  |  Skipped (missing OHLC): {skipped}")

    if not dry_run:
        cur.executemany("""
            INSERT INTO execution_log (
                trade_log_id, ticker, direction, signal_date, entry_date,
                exec_code, trade_taken, entry_window, exec_notes,
                prev_close, entry_open, entry_high, entry_low, entry_close,
                gap_pct, gap_category, day_move_pct, day_range_pct,
                nifty_open, nifty_close, nifty_day_move, nifty_is_weak, rs_vs_nifty,
                blind_entry_price, smart_entry_price, exit_price,
                blind_pnl_pct, smart_pnl_pct, pnl_improvement
            ) VALUES (
                :trade_log_id, :ticker, :direction, :signal_date, :entry_date,
                :exec_code, :trade_taken, :entry_window, :exec_notes,
                :prev_close, :entry_open, :entry_high, :entry_low, :entry_close,
                :gap_pct, :gap_category, :day_move_pct, :day_range_pct,
                :nifty_open, :nifty_close, :nifty_day_move, :nifty_is_weak, :rs_vs_nifty,
                :blind_entry_price, :smart_entry_price, :exit_price,
                :blind_pnl_pct, :smart_pnl_pct, :pnl_improvement
            )
            ON CONFLICT(trade_log_id) DO UPDATE SET
                exec_code=excluded.exec_code, trade_taken=excluded.trade_taken,
                entry_window=excluded.entry_window, exec_notes=excluded.exec_notes,
                gap_pct=excluded.gap_pct, gap_category=excluded.gap_category,
                day_move_pct=excluded.day_move_pct, day_range_pct=excluded.day_range_pct,
                nifty_day_move=excluded.nifty_day_move, nifty_is_weak=excluded.nifty_is_weak,
                rs_vs_nifty=excluded.rs_vs_nifty,
                blind_entry_price=excluded.blind_entry_price,
                smart_entry_price=excluded.smart_entry_price,
                blind_pnl_pct=excluded.blind_pnl_pct,
                smart_pnl_pct=excluded.smart_pnl_pct,
                pnl_improvement=excluded.pnl_improvement
        """, rows_to_insert)
        conn.commit()
        print(f"  Inserted/updated {len(rows_to_insert)} rows in execution_log")
    else:
        print("  [DRY-RUN] No changes written")

    # ── Summary ────────────────────────────────────────────────────────────────
    _print_summary(rows_to_insert)
    conn.close()


def _print_summary(rows: list[dict]) -> None:
    total = len(rows)
    if total == 0:
        print("\nNo data to summarise.")
        return

    taken = [r for r in rows if r["trade_taken"]]
    skipped_exec = [r for r in rows if not r["trade_taken"]]

    # Blind stats (all 693 trades as-is)
    blind_pnls   = [r["blind_pnl_pct"] for r in rows]
    blind_wins   = sum(1 for p in blind_pnls if p > 0)
    blind_avg    = sum(blind_pnls) / len(blind_pnls)

    # Smart stats (only taken trades)
    smart_pnls   = [r["smart_pnl_pct"] for r in taken if r["smart_pnl_pct"] is not None]
    smart_wins   = sum(1 for p in smart_pnls if p > 0)
    smart_avg    = sum(smart_pnls) / len(smart_pnls) if smart_pnls else 0

    # Exec code distribution
    code_counts: dict[str, int] = {}
    for r in rows:
        code_counts[r["exec_code"]] = code_counts.get(r["exec_code"], 0) + 1

    print("\n" + "=" * 60)
    print("  EXECUTION INTELLIGENCE - COMPARISON SUMMARY")
    print("=" * 60)
    print(f"  Total signals analysed : {total}")
    print(f"  Trades taken (smart)   : {len(taken)}  ({len(taken)/total*100:.1f}%)")
    print(f"  Trades skipped         : {len(skipped_exec)}  ({len(skipped_exec)/total*100:.1f}%)")
    print()
    print(f"  BLIND ENTRY (9:15, every signal)")
    print(f"    Win rate : {blind_wins/total*100:.1f}%  ({blind_wins}/{total})")
    print(f"    Avg P&L  : {blind_avg:+.2f}%")
    print()
    print(f"  SMART ENTRY (engine-filtered)")
    print(f"    Win rate : {smart_wins/len(taken)*100:.1f}%  ({smart_wins}/{len(taken)})")
    print(f"    Avg P&L  : {smart_avg:+.2f}%")
    print()
    print("  Execution code distribution:")
    for code, cnt in sorted(code_counts.items(), key=lambda x: -x[1]):
        bar = "#" * (cnt * 20 // total)
        print(f"    {code:<28} {cnt:>4}  {bar}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run execution intelligence analysis")
    parser.add_argument("--dry-run", action="store_true",
                        help="Analyse without writing to DB")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
