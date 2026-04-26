"""
Load US stock sector mapping from yfinance directly.

yfinance .info provides accurate sector, industry, and company name for US stocks.
Populates sector_mapping with market='US'.

Usage:
    # Tickers on command line
    python load_us_sector_mapping.py --tickers AAPL MSFT NVDA GOOGL AMZN

    # Tickers from a plain text file (one per line)
    python load_us_sector_mapping.py --file us_tickers.txt

    # Both
    python load_us_sector_mapping.py --file us_tickers.txt --tickers TSLA META

    # Preview without writing
    python load_us_sector_mapping.py --file us_tickers.txt --dry-run

yfinance sector values are used as-is. If a ticker returns no sector
(e.g. ETF, index fund), the sector is recorded as 'Unknown'.
"""

import argparse
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..")))

import yfinance as yf
from backend.signals.db import get_conn, SIGNALS_DB_PATH, init_db


def fetch_info(ticker: str) -> dict:
    """Fetch sector + company name from yfinance for one US ticker."""
    try:
        info = yf.Ticker(ticker).info
        return {
            "ticker":       ticker,
            "company_name": info.get("longName") or info.get("shortName") or ticker,
            "sector":       info.get("sector") or "Unknown",
            "industry":     info.get("industry") or "",
        }
    except Exception as exc:
        return {
            "ticker":       ticker,
            "company_name": ticker,
            "sector":       "Unknown",
            "industry":     "",
            "error":        str(exc),
        }


def load_tickers_from_file(path: str) -> list[str]:
    with open(path, encoding="utf-8") as f:
        return [line.strip().upper() for line in f if line.strip() and not line.startswith("#")]


def run(
    tickers: list[str],
    db_path: str = SIGNALS_DB_PATH,
    workers: int = 10,
    dry_run: bool = False,
) -> None:
    tickers = sorted(set(tickers))
    print(f"\nUS Sector Mapping -- {len(tickers)} tickers -- {workers} workers")
    if dry_run:
        print("  [DRY RUN] no writes\n")

    init_db(db_path)

    results = []
    failed = []
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_info, t): t for t in tickers}
        done = 0
        for fut in as_completed(futures):
            info = fut.result()
            done += 1
            if "error" in info:
                failed.append(info["ticker"])
                print(f"  [FAIL] {info['ticker']}: {info['error']}")
            else:
                results.append(info)
            if done % 20 == 0 or done == len(tickers):
                print(f"  [{done}/{len(tickers)}] fetched")

    print(f"\n  Fetched in {time.time()-t0:.1f}s")
    print(f"  OK: {len(results)}   Failed: {len(failed)}")

    if dry_run:
        print("\n-- Preview --")
        for r in sorted(results, key=lambda x: x["sector"]):
            print(f"  {r['ticker']:<10} {r['sector']:<35} {r['company_name']}")
        return

    # Write to sector_mapping
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn(db_path)
    rows = [
        (r["ticker"], "US", r["company_name"], r["sector"], 0, None, now_str)
        for r in results
    ]
    with conn:
        conn.executemany(
            """INSERT OR REPLACE INTO sector_mapping
               (ticker, market, company_name, sector, is_index, yf_symbol, added_at)
               VALUES (?,?,?,?,?,?,?)""",
            rows,
        )
    conn.close()

    print(f"\n  Wrote {len(rows)} rows to sector_mapping (market='US')")

    # Summary by sector
    conn = get_conn(db_path)
    print("\n-- US Stocks by Sector --------------------------------")
    for r in conn.execute(
        """SELECT sector, COUNT(*) AS cnt
           FROM sector_mapping
           WHERE market='US' AND is_index=0
           GROUP BY sector ORDER BY cnt DESC"""
    ).fetchall():
        print(f"  {r[0]:<40} {r[1]:>4}")

    total = conn.execute(
        "SELECT COUNT(*) FROM sector_mapping WHERE market='US' AND is_index=0"
    ).fetchone()[0]
    print(f"\n  Total US stocks in sector_mapping: {total}")
    conn.close()

    if failed:
        print(f"\n  Failed tickers ({len(failed)}): {', '.join(failed)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", nargs="+", default=[],
                        help="US ticker symbols (e.g. AAPL MSFT NVDA)")
    parser.add_argument("--file",    default=None,
                        help="Path to text file with one ticker per line")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--db",      default=SIGNALS_DB_PATH)
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and preview without writing to DB")
    args = parser.parse_args()

    all_tickers = [t.upper() for t in args.tickers]
    if args.file:
        all_tickers += load_tickers_from_file(args.file)

    if not all_tickers:
        print("No tickers provided. Use --tickers or --file.")
        sys.exit(1)

    run(all_tickers, args.db, args.workers, args.dry_run)
