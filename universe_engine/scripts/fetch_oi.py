"""
Fetch daily OHLC + OI for all F&O symbols in our universe over the
intraday-DB window. Runs once; idempotent (INSERT OR REPLACE).

Usage:
    python scripts/fetch_oi.py
    python scripts/fetch_oi.py --start 2025-11-01 --end 2026-04-30 --workers 16
"""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.oi_fetch import fetch_oi_for_symbols, ensure_oi_schema

DB = ROOT / "data" / "db" / "kanida_universe.db"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-11-01")
    ap.add_argument("--end",   default="2026-04-30")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--rps", type=float, default=5.0)
    ap.add_argument("--index", default="in_nifty200")
    args = ap.parse_args()

    if not DB.exists(): sys.exit(f"ERROR: DB not found: {DB}")

    con = sqlite3.connect(DB, timeout=60.0)
    ensure_oi_schema(con)
    rows = con.execute(f"""
        SELECT symbol FROM universe_master
        WHERE is_active=1 AND {args.index}=1 ORDER BY symbol
    """).fetchall()
    symbols = [r[0] for r in rows]
    con.close()
    print(f"DB:        {DB}")
    print(f"Universe:  {len(symbols)} symbols ({args.index})")
    print(f"Window:    {args.start}  ->  {args.end}")

    s = fetch_oi_for_symbols(DB, symbols, args.start, args.end,
                              n_workers=args.workers, rps=args.rps)

    # Quick coverage sanity
    con = sqlite3.connect(DB)
    print("\nOI coverage by date (last 5 days):")
    rows = con.execute("""
        SELECT trade_date, COUNT(DISTINCT symbol), SUM(oi)
        FROM ohlc_futures_daily
        GROUP BY trade_date
        ORDER BY trade_date DESC LIMIT 5
    """).fetchall()
    for d, n, oi in rows:
        print(f"  {d}: {n} symbols, total OI = {oi:,}")
    con.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
