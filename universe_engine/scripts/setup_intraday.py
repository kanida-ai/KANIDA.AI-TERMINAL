"""
Setup intraday data:
  1. Ensure intraday DB schema (ohlc_5min, ohlc_15min, ohlc_30min)
  2. Fetch 1m bars from Zerodha Kite for the last `--months` months
     for every active symbol in universe_master  (multi-worker, rate-limited)
  3. Resample 1m -> 5m, 15m, 30m  (multi-worker)

Usage:
    python scripts/setup_intraday.py --months 6  --workers 16  --rps 5

Notes:
  * If your KITE_ACCESS_TOKEN is expired (Kite tokens last one trading day),
    refresh it via https://kite.trade/connect/login?api_key=<KEY>&v=3 and update
    config/.env, then re-run.
"""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.data_fetch import fetch_1m_for_symbols
from engine.resample   import ensure_intraday_schema, resample_all_symbols

DB = ROOT / "data" / "db" / "kanida_universe.db"


def get_universe_symbols(con: sqlite3.Connection, index_col: str = "in_nifty200"):
    rows = con.execute(f"""
        SELECT symbol FROM universe_master
        WHERE is_active = 1 AND {index_col} = 1
        ORDER BY symbol
    """).fetchall()
    return [r[0] for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=6)
    ap.add_argument("--workers", type=int, default=16,
                    help="thread workers for fetcher (10–48)")
    ap.add_argument("--rps", type=float, default=5.0,
                    help="Kite request-per-second cap (don't exceed 10)")
    ap.add_argument("--index", default="in_nifty200")
    ap.add_argument("--skip-fetch", action="store_true",
                    help="skip 1m fetch (only resample existing 1m table)")
    ap.add_argument("--skip-resample", action="store_true")
    args = ap.parse_args()

    if not DB.exists():
        print(f"ERROR: DB not found at {DB}\n  Run scripts/setup.py first.")
        return 1

    con = sqlite3.connect(DB, timeout=60.0)
    print(f"DB: {DB}")
    ensure_intraday_schema(con)

    symbols = get_universe_symbols(con, args.index)
    print(f"Universe: {len(symbols)} symbols (filter = {args.index})")
    con.close()

    if not args.skip_fetch:
        print("\n[1/2] Fetching 1m bars from Kite ...")
        fetch_1m_for_symbols(DB, symbols,
                             months_back=args.months,
                             n_workers=args.workers,
                             rps=args.rps)

    if not args.skip_resample:
        print("\n[2/2] Resampling 1m -> 5m / 15m / 30m ...")
        resample_all_symbols(DB, symbols,
                              tfs_min=(5, 15, 30),
                              n_workers=args.workers)

    # Quick summary
    con = sqlite3.connect(DB)
    print("\nIntraday data summary:")
    for tbl in ("ohlc_1min", "ohlc_5min", "ohlc_15min", "ohlc_30min"):
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        sym_cnt = con.execute(f"SELECT COUNT(DISTINCT symbol) FROM {tbl}").fetchone()[0]
        rng = con.execute(f"SELECT MIN(bar_time), MAX(bar_time) FROM {tbl}").fetchone()
        print(f"  {tbl:<14} {cnt:>10,} bars · {sym_cnt} symbols · {rng[0]} → {rng[1]}")
    con.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
