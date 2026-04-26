"""
Load NSE F&O sector mapping into kanida_signals.db.

Usage:
    python load_sector_mapping.py --csv <path_to_csv>

The CSV must have columns: company_name, ticker, sector
All rows are treated as market='NSE'.

Also seeds index_metadata with the standard NSE and US indices.

After loading, prints:
  - Count by sector
  - Tickers in sector_mapping but NOT yet in signal_events (new to DB)
  - Tickers in signal_events but NOT in sector_mapping (DB has them, CSV doesn't)
"""

import argparse
import csv
import sys
import os
from datetime import datetime, timezone

# Allow running from repo root or scripts/ directory
sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..")))

from backend.signals.db import get_conn, SIGNALS_DB_PATH, init_db


# ── NSE Index definitions ──────────────────────────────────────────
NSE_INDICES = [
    ("NIFTY",       "NSE", "NIFTY 50",               "^NSEI",                 "Broad Market"),
    ("BANKNIFTY",   "NSE", "BANK NIFTY",              "^NSEBANK",              "Banking & Financials"),
    ("FINNIFTY",    "NSE", "NIFTY FINANCIAL SERVICES","NIFTY_FIN_SERVICE.NS",  "Banking & Financials"),
    ("MIDCPNIFTY",  "NSE", "NIFTY MIDCAP SELECT",     "NIFTY_MID_SELECT.NS",   "Broad Market"),
    ("NIFTYNXT50",  "NSE", "NIFTY NEXT 50",           "^NSMIDCP50",            "Broad Market"),
]

# US indices for future use
US_INDICES = [
    ("SPX", "US", "S&P 500",       "^GSPC",  "Broad Market"),
    ("NDX", "US", "NASDAQ 100",    "^NDX",   "Technology"),
    ("DJI", "US", "Dow Jones",     "^DJI",   "Broad Market"),
    ("RUT", "US", "Russell 2000",  "^RUT",   "Broad Market"),
]


def load_csv(csv_path: str) -> list[dict]:
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row["ticker"].strip()
            company = row["company_name"].strip()
            sector = row["sector"].strip()
            if ticker:
                rows.append({"ticker": ticker, "company_name": company, "sector": sector})
    return rows


def run(csv_path: str, db_path: str = SIGNALS_DB_PATH) -> None:
    # Ensure new tables exist
    init_db(db_path)

    rows = load_csv(csv_path)
    print(f"\nLoaded {len(rows)} rows from CSV")

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn(db_path)

    # ── 1. Insert sector_mapping (NSE stocks) ─────────────────────
    stock_rows = [
        (r["ticker"], "NSE", r["company_name"], r["sector"], 0, None, now_str)
        for r in rows
    ]
    with conn:
        conn.executemany(
            """INSERT OR REPLACE INTO sector_mapping
               (ticker, market, company_name, sector, is_index, yf_symbol, added_at)
               VALUES (?,?,?,?,?,?,?)""",
            stock_rows,
        )

    # ── 2. Insert NSE indices into both tables ─────────────────────
    index_sector_rows = []
    for ticker, market, name, yf_sym, sector in NSE_INDICES + US_INDICES:
        index_sector_rows.append(
            (ticker, market, f"Index: {name}", sector, 1, yf_sym, now_str)
        )
    with conn:
        conn.executemany(
            """INSERT OR REPLACE INTO sector_mapping
               (ticker, market, company_name, sector, is_index, yf_symbol, added_at)
               VALUES (?,?,?,?,?,?,?)""",
            index_sector_rows,
        )
        conn.executemany(
            """INSERT OR REPLACE INTO index_metadata
               (ticker, market, index_name, yf_symbol, sector, is_active, added_at)
               VALUES (?,?,?,?,?,1,?)""",
            [
                (ticker, market, name, yf_sym, sector, now_str)
                for ticker, market, name, yf_sym, sector in NSE_INDICES + US_INDICES
            ],
        )

    # ── 3. Validation counts ───────────────────────────────────────
    total_sm = conn.execute(
        "SELECT COUNT(*) FROM sector_mapping WHERE market='NSE' AND is_index=0"
    ).fetchone()[0]
    total_idx = conn.execute(
        "SELECT COUNT(*) FROM index_metadata"
    ).fetchone()[0]

    print(f"\nRows in sector_mapping (NSE stocks) : {total_sm}")
    print(f"Rows in index_metadata              : {total_idx}")

    # ── 4. Count by sector ─────────────────────────────────────────
    print("\n-- NSE F&O Stocks by Sector --------------------------------")
    for r in conn.execute(
        """SELECT sector, COUNT(*) AS cnt
           FROM sector_mapping
           WHERE market='NSE' AND is_index=0
           GROUP BY sector ORDER BY cnt DESC"""
    ).fetchall():
        print(f"  {r[0]:<40} {r[1]:>4}")

    # ── 5. Tickers in CSV not yet in signal_events (new to DB) ─────
    missing_from_se = conn.execute(
        """SELECT sm.ticker, sm.sector
           FROM sector_mapping sm
           WHERE sm.market='NSE' AND sm.is_index=0
             AND sm.ticker NOT IN (
                 SELECT DISTINCT ticker FROM signal_events WHERE market='NSE'
             )
           ORDER BY sm.sector, sm.ticker"""
    ).fetchall()

    print(f"\n-- Tickers in sector_mapping but NOT in signal_events : {len(missing_from_se)} ----")
    for r in missing_from_se:
        print(f"  {r[0]:<20} {r[1]}")

    # ── 6. Tickers in signal_events but not in sector_mapping ──────
    orphan_tickers = conn.execute(
        """SELECT DISTINCT se.ticker
           FROM signal_events se
           WHERE se.market='NSE'
             AND se.ticker NOT IN (
                 SELECT ticker FROM sector_mapping WHERE market='NSE'
             )
           ORDER BY se.ticker"""
    ).fetchall()

    print(f"\n-- Tickers in signal_events but NOT in sector_mapping : {len(orphan_tickers)} ----")
    for r in orphan_tickers:
        print(f"  {r[0]}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv",  required=True, help="Path to nse_fo_sector_mapping_normalized.csv")
    parser.add_argument("--db",   default=SIGNALS_DB_PATH, help="Path to kanida_signals.db")
    args = parser.parse_args()
    run(args.csv, args.db)
