"""
Falcon Engine — universe + sector mapping foundation.

Loads Nifty 500 industry from NSE CSV, persists in falcon_sectors table.
Provides survivorship-aware queries: only stocks active and liquid as of each
date are eligible for mining/testing.
"""
from __future__ import annotations
import csv, io, sqlite3, urllib.request
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Set

NIFTY500_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"


SCHEMA = """
CREATE TABLE IF NOT EXISTS falcon_sectors (
    symbol     TEXT PRIMARY KEY,
    sector     TEXT NOT NULL,
    sub_sector TEXT,
    company    TEXT
);
CREATE INDEX IF NOT EXISTS idx_falcon_sectors_sector ON falcon_sectors(sector);
"""


def ensure_schema(con: sqlite3.Connection):
    con.executescript(SCHEMA)
    con.commit()


def fetch_nifty500_with_industry() -> List[Dict]:
    req = urllib.request.Request(NIFTY500_URL, headers={'User-Agent':'Mozilla/5.0'})
    data = urllib.request.urlopen(req, timeout=30).read().decode('utf-8','ignore')
    rows = list(csv.DictReader(io.StringIO(data)))
    return [{
        "symbol":  r['Symbol'].strip(),
        "company": r.get('Company Name','').strip(),
        "sector":  r.get('Industry','Unknown').strip() or "Unknown",
    } for r in rows if r.get('Symbol') and r.get('Series','').strip() == 'EQ']


def populate_sectors(con: sqlite3.Connection):
    """Idempotent: re-fetches and replaces sector mapping for current Nifty 500."""
    ensure_schema(con)
    rows = fetch_nifty500_with_industry()
    con.executemany("""
        INSERT OR REPLACE INTO falcon_sectors (symbol, sector, sub_sector, company)
        VALUES (?, ?, NULL, ?)
    """, [(r['symbol'], r['sector'], r['company']) for r in rows])
    con.commit()
    return len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Survivorship-aware eligibility
# ─────────────────────────────────────────────────────────────────────────────

def get_first_bar_dates(con: sqlite3.Connection,
                          symbols: Optional[List[str]] = None) -> Dict[str, date]:
    """First daily bar per symbol — proxy for listing date."""
    where = ""
    args: list = []
    if symbols:
        where = "WHERE symbol IN (" + ",".join("?" * len(symbols)) + ")"
        args = list(symbols)
    rows = con.execute(f"""
        SELECT symbol, MIN(trade_date) FROM ohlc_daily {where}
        GROUP BY symbol
    """, args).fetchall()
    return {s: date.fromisoformat(d) for s, d in rows if d}


def liquidity_filter(con: sqlite3.Connection, on_date: str,
                      min_avg_value_cr: float = 5.0,
                      lookback_days: int = 60) -> Set[str]:
    """Symbols whose 60-day avg daily traded value (close × volume) is at least
    `min_avg_value_cr` crores, as of `on_date`. Liquidity proxy.
    Default ₹5 cr / day = ₹50 lakh slug fillable easily."""
    rows = con.execute(f"""
        SELECT symbol, AVG(close * volume) AS avg_val
        FROM ohlc_daily
        WHERE trade_date >= date(?, '-{lookback_days} days') AND trade_date <= ?
        GROUP BY symbol
    """, (on_date, on_date)).fetchall()
    threshold = min_avg_value_cr * 1e7
    return {s for s, v in rows if v and v >= threshold}


def eligible_universe(con: sqlite3.Connection, on_date: str,
                        min_avg_value_cr: float = 5.0,
                        min_listed_days: int = 252) -> Set[str]:
    """Returns set of symbols eligible for trading/mining as of `on_date`.
    Requires (a) listed at least `min_listed_days` ago, (b) liquid by ₹-value
    threshold over last 60 days, (c) sector is mapped."""
    on = date.fromisoformat(on_date)
    cutoff_listing = on.toordinal() - min_listed_days

    listings = get_first_bar_dates(con)
    listed_ok = {s for s, d in listings.items() if d.toordinal() <= cutoff_listing}

    liquid = liquidity_filter(con, on_date, min_avg_value_cr=min_avg_value_cr)

    sector_rows = con.execute("SELECT symbol FROM falcon_sectors").fetchall()
    sectored = {r[0] for r in sector_rows}

    return listed_ok & liquid & sectored


def get_sector_for(con: sqlite3.Connection, symbols: List[str]) -> Dict[str, str]:
    rows = con.execute(
        f"SELECT symbol, sector FROM falcon_sectors WHERE symbol IN ({','.join('?'*len(symbols))})",
        list(symbols)
    ).fetchall()
    return {s: sec for s, sec in rows}


def get_sector_constituents(con: sqlite3.Connection, sector: str) -> List[str]:
    rows = con.execute("SELECT symbol FROM falcon_sectors WHERE sector = ?",
                        (sector,)).fetchall()
    return [r[0] for r in rows]


def all_sectors(con: sqlite3.Connection) -> List[str]:
    rows = con.execute("SELECT DISTINCT sector FROM falcon_sectors ORDER BY sector").fetchall()
    return [r[0] for r in rows]
