"""
Setup script — runs DB init + bootstraps OHLC by copying from the main engine's DB.

Why bootstrap from main engine: avoids re-pulling 200 stocks × 6 years from Kite (~15min).
We import the same cleansed bars the main engine has been using. For ongoing daily
updates, the universe-engine fetcher (engine/data_fetch.py) will pull deltas directly
from Zerodha Kite.

Run once:  python scripts/setup.py
"""
from __future__ import annotations
import sqlite3, sys
from pathlib import Path

ROOT       = Path(__file__).resolve().parent.parent
PROJECT    = ROOT.parent  # main engine root
sys.path.insert(0, str(ROOT))

from data.db.init_db import main as init_db_main


MAIN_DB     = PROJECT / "data" / "db" / "kanida_quant.db"
UNIV_DB     = ROOT / "data" / "db" / "kanida_universe.db"


def bootstrap_ohlc():
    if not MAIN_DB.exists():
        print(f"  WARN: main engine DB not found at {MAIN_DB}; skipping bootstrap")
        return 0
    print(f"  Source: {MAIN_DB}")
    print(f"  Target: {UNIV_DB}")
    src = sqlite3.connect(MAIN_DB)
    dst = sqlite3.connect(UNIV_DB)
    src.row_factory = sqlite3.Row

    # Copy ohlc_daily for symbols that exist in our universe_master
    universe_syms = set(r[0] for r in dst.execute("SELECT symbol FROM universe_master"))
    print(f"  Universe master: {len(universe_syms)} symbols")

    # Also include NIFTY50 unconditionally (regime calculations need it)
    universe_syms.add("NIFTY50")

    # Pull OHLC from main engine's ohlc_daily (column = ticker, not symbol)
    rows = src.execute("""
        SELECT ticker AS symbol, trade_date, open, high, low, close, volume, quality_flag
        FROM ohlc_daily
        WHERE quality_flag != 'rejected'
        ORDER BY ticker, trade_date
    """).fetchall()
    print(f"  Reading {len(rows)} OHLC rows from main engine ...")

    dst.execute("DELETE FROM ohlc_daily")
    payload = []
    for r in rows:
        if r["symbol"] in universe_syms:
            payload.append((r["symbol"], r["trade_date"], r["open"], r["high"],
                            r["low"], r["close"], r["volume"], r["quality_flag"]))
    dst.executemany("""
        INSERT INTO ohlc_daily (symbol, trade_date, open, high, low, close, volume, quality_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, payload)
    dst.commit()

    sym_count = dst.execute("SELECT COUNT(DISTINCT symbol) FROM ohlc_daily").fetchone()[0]
    bar_count = dst.execute("SELECT COUNT(*) FROM ohlc_daily").fetchone()[0]
    print(f"  Imported {bar_count} bars across {sym_count} symbols (incl. NIFTY50)")

    # Date range
    rng = dst.execute("SELECT MIN(trade_date), MAX(trade_date) FROM ohlc_daily").fetchone()
    print(f"  Date range: {rng[0]} → {rng[1]}")

    src.close(); dst.close()
    return bar_count


def main():
    print("=" * 60)
    print("Kanida Universe Engine — setup")
    print("=" * 60)
    print("\n[1/2] Initialising DB schema ...")
    init_db_main()

    print("\n[2/2] Bootstrapping OHLC from main engine ...")
    n = bootstrap_ohlc()

    print("\nDone.\n")
    print("Next:")
    print("  python scripts/run_smoke_test.py --month 2026-03")
    print("  python scripts/run_pipeline_a.py --start 2024-07 --end 2026-04")
    print("  python scripts/run_pipeline_b.py")


if __name__ == "__main__":
    main()
