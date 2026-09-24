"""
Expand universe from current 148 to Nifty 500 (~503 EQ stocks).
  1. Add new symbols to universe_master with in_nifty500=1
  2. Fetch daily OHLC for new symbols going back 2+ years
  3. Existing symbols already have 6 years of data — leave as-is.
"""
from __future__ import annotations
import argparse, json, sqlite3, sys, urllib.request, csv, io
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.daily_eq_fetch import fetch_eq_daily

DB = ROOT / "data" / "db" / "kanida_universe.db"
NIFTY500_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"


def fetch_nifty500_symbols():
    req = urllib.request.Request(NIFTY500_URL, headers={'User-Agent':'Mozilla/5.0'})
    data = urllib.request.urlopen(req, timeout=30).read().decode('utf-8','ignore')
    rows = list(csv.DictReader(io.StringIO(data)))
    return sorted({r['Symbol'].strip() for r in rows
                    if r.get('Symbol') and r.get('Series','').strip()=='EQ'})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default=date.today().isoformat())
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--rps", type=float, default=5.0)
    args = ap.parse_args()

    print(f"DB: {DB}")
    print(f"Window: {args.start} -> {args.end}")
    print()

    nifty500 = fetch_nifty500_symbols()
    print(f"Nifty 500 EQ symbols fetched: {len(nifty500)}")

    con = sqlite3.connect(DB, timeout=60.0)
    existing = set(r[0] for r in con.execute(
        "SELECT symbol FROM universe_master WHERE is_active=1"))
    print(f"Existing universe: {len(existing)}")

    new_symbols = [s for s in nifty500 if s not in existing]
    print(f"New symbols to fetch: {len(new_symbols)}")

    # Step 1: insert new symbols into universe_master
    payload = [(s, 'NSE', None, 0, 0, 0, 1, 1, args.start, None, 'auto-added: nifty500 expansion')
                 for s in new_symbols]
    con.executemany("""
        INSERT OR IGNORE INTO universe_master
            (symbol, exchange, sector, in_nifty50, in_nifty100, in_nifty200,
             in_nifty500, is_active, effective_from, effective_to, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, payload)
    con.commit()
    n_added = con.execute("SELECT COUNT(*) FROM universe_master WHERE is_active=1").fetchone()[0]
    print(f"universe_master after insert: {n_added} active symbols")
    con.close()

    # Step 2: fetch daily OHLC for new symbols
    if new_symbols:
        print(f"\nFetching daily bars for {len(new_symbols)} new symbols ...")
        s = fetch_eq_daily(DB, new_symbols, args.start, args.end,
                            n_workers=args.workers, rps=args.rps)

    # Coverage check
    print("\nFinal coverage:")
    con = sqlite3.connect(DB)
    r = con.execute("""
        SELECT COUNT(DISTINCT symbol), MIN(trade_date), MAX(trade_date), COUNT(*)
        FROM ohlc_daily
    """).fetchone()
    print(f"  ohlc_daily: {r[0]} symbols, {r[1]} -> {r[2]}, {r[3]:,} bars")
    n2yr = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT symbol, MIN(trade_date) AS first_d FROM ohlc_daily
            GROUP BY symbol HAVING first_d <= ?
        )
    """, (args.start,)).fetchone()[0]
    print(f"  symbols with >= 2yr history: {n2yr}")
    con.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
