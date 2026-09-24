"""
Retry pass: re-fetch 1m bars for symbols that came up incomplete in the main fetch.

After scripts/setup_intraday.py finishes, run this. It:
  1. Counts rows-per-symbol in ohlc_1min.
  2. Identifies symbols below MIN_BARS (default 40,000 = ~107 trading days).
  3. Re-fetches those with reduced concurrency to avoid Kite rate-limit clusters.
  4. Reports which symbols are still incomplete after retry.

Usage:
    python scripts/retry_failed_fetch.py [--workers 4] [--rps 2] [--months 6]
"""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.data_fetch import fetch_1m_for_symbols

DB = ROOT / "data" / "db" / "kanida_universe.db"
MIN_BARS_THRESHOLD = 40_000   # ~107 trading days of 1m bars


def find_incomplete_symbols(con: sqlite3.Connection,
                             min_bars: int = MIN_BARS_THRESHOLD) -> list[str]:
    rows = con.execute("""
        SELECT um.symbol, COALESCE(c.cnt, 0) AS cnt
        FROM universe_master um
        LEFT JOIN (
            SELECT symbol, COUNT(*) AS cnt FROM ohlc_1min GROUP BY symbol
        ) c ON c.symbol = um.symbol
        WHERE um.is_active = 1 AND um.in_nifty200 = 1
        ORDER BY cnt
    """).fetchall()
    incomplete = [r[0] for r in rows if r[1] < min_bars]
    return incomplete, [(r[0], r[1]) for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4,
                    help="reduced parallelism (default 4)")
    ap.add_argument("--rps", type=float, default=2.0,
                    help="reduced rate (default 2 req/sec)")
    ap.add_argument("--months", type=int, default=6)
    ap.add_argument("--min-bars", type=int, default=MIN_BARS_THRESHOLD)
    args = ap.parse_args()

    if not DB.exists():
        sys.exit(f"ERROR: DB not found at {DB}")

    con = sqlite3.connect(DB)
    incomplete, all_counts = find_incomplete_symbols(con, args.min_bars)
    print(f"Total symbols in universe: {len(all_counts)}")
    print(f"Below threshold ({args.min_bars:,} bars): {len(incomplete)}")
    if incomplete:
        print(f"  {incomplete[:30]}{'...' if len(incomplete) > 30 else ''}")
    con.close()

    if not incomplete:
        print("\nAll symbols complete. No retry needed.")
        return 0

    print(f"\nRetrying with reduced concurrency (workers={args.workers}, rps={args.rps}) ...")
    summary = fetch_1m_for_symbols(DB, incomplete,
                                    months_back=args.months,
                                    n_workers=max(args.workers, 1),
                                    rps=args.rps)

    # Re-check
    con = sqlite3.connect(DB)
    incomplete2, _ = find_incomplete_symbols(con, args.min_bars)
    con.close()
    print(f"\nAfter retry: {len(incomplete2)} symbols still incomplete")
    if incomplete2:
        print(f"  Still missing: {incomplete2}")
        print("  Run again with --workers 2 --rps 1 if needed.")
    else:
        print("  All symbols now complete.")


if __name__ == "__main__":
    sys.exit(main() or 0)
