"""
NEW data classes fetched from NSE (not derivable from price): EARNINGS/RESULTS dates
and DELIVERY %. These target the failure mode found in v1/v2 — next-day extreme
movers are event-driven, which price/volume/RS cannot see.

  • corp_earnings_dates(symbol, result_date, purpose)  <- corporate-board-meetings API
  • delivery_daily(symbol, trade_date, deliv_pct, ttl_qty, deliv_qty) <- sec_bhavdata_full

Earnings dates are announced days ahead, so "results scheduled for T+1" is known at the
EOD of T (no lookahead). Delivery % is an EOD figure for day T.
"""
from __future__ import annotations

import io
import sqlite3
import time
from datetime import date, timedelta
from typing import List, Optional

import pandas as pd
import requests

_H = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/json,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

EARN_SCHEMA = """
CREATE TABLE IF NOT EXISTS corp_earnings_dates (
    symbol      TEXT NOT NULL,
    result_date TEXT NOT NULL,
    purpose     TEXT,
    fetched_at  TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, result_date)
)"""

DELIV_SCHEMA = """
CREATE TABLE IF NOT EXISTS delivery_daily (
    symbol     TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    deliv_pct  REAL,
    ttl_qty    REAL,
    deliv_qty  REAL,
    PRIMARY KEY (symbol, trade_date)
)"""


def _session():
    s = requests.Session()
    s.headers.update(_H)
    try:
        s.get("https://www.nseindia.com", timeout=15)
        time.sleep(0.8)
        s.get("https://www.nseindia.com/all-reports", timeout=15)
    except Exception:
        pass
    return s


def _ddmmyyyy(d: date) -> str:
    return d.strftime("%d-%m-%Y")


# ── earnings / board meetings ───────────────────────────────────────────────────

RESULT_KEYS = ("financial result", "audited", "unaudited", "quarterly result")


def fetch_board_meetings(s, frm: date, to: date) -> pd.DataFrame:
    url = ("https://www.nseindia.com/api/corporate-board-meetings?index=equities"
           f"&from_date={_ddmmyyyy(frm)}&to_date={_ddmmyyyy(to)}")
    r = s.get(url, timeout=30, headers={"Referer": "https://www.nseindia.com/companies-listing/corporate-filings-board-meetings"})
    r.raise_for_status()
    j = r.json()
    data = j.get("data", j) if isinstance(j, dict) else j
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    return df


def backfill_earnings(con: sqlite3.Connection, start: str = "2022-01-01",
                      end: Optional[str] = None, results_only: bool = True) -> int:
    con.execute(EARN_SCHEMA)
    s = _session()
    d0 = date.fromisoformat(start)
    d1 = date.today() if not end else date.fromisoformat(end)
    n = 0
    cur = d0
    while cur < d1:
        nxt = min(cur + timedelta(days=30), d1)
        for attempt in range(3):
            try:
                df = fetch_board_meetings(s, cur, nxt)
                break
            except Exception as e:
                if attempt == 2:
                    print(f"  {cur}..{nxt} FAIL {e}")
                    df = pd.DataFrame()
                time.sleep(2)
                s = _session()
        if not df.empty:
            sym_col = "bm_symbol" if "bm_symbol" in df.columns else "symbol"
            dt_col = "bm_date" if "bm_date" in df.columns else "date"
            pur_col = "bm_purpose" if "bm_purpose" in df.columns else "purpose"
            desc_col = "bm_desc" if "bm_desc" in df.columns else pur_col
            rows = []
            for _, r in df.iterrows():
                blob = f"{str(r.get(pur_col,''))} {str(r.get(desc_col,''))}".lower()
                if results_only and not any(k in blob for k in RESULT_KEYS):
                    continue
                try:
                    rd = pd.to_datetime(r[dt_col]).date().isoformat()
                except Exception:
                    continue
                rows.append((str(r[sym_col]).strip(), rd, str(r.get(pur_col, ""))[:120]))
            if rows:
                con.executemany(
                    "INSERT OR REPLACE INTO corp_earnings_dates(symbol,result_date,purpose) VALUES (?,?,?)",
                    rows)
                con.commit()
                n += len(rows)
        print(f"  {cur}..{nxt}: +{0 if df.empty else len(rows)} (total {n})", flush=True)
        time.sleep(0.7)
        cur = nxt + timedelta(days=1)
    return n


# ── delivery % ──────────────────────────────────────────────────────────────────

def fetch_delivery_day(s, d: date) -> pd.DataFrame:
    url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d.strftime('%d%m%Y')}.csv"
    r = s.get(url, timeout=25, headers={"Referer": "https://www.nseindia.com/all-reports"})
    if r.status_code != 200 or len(r.content) < 1000:
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip() for c in df.columns]
    df["SERIES"] = df["SERIES"].astype(str).str.strip()
    df = df[df["SERIES"] == "EQ"]
    out = pd.DataFrame({
        "symbol": df["SYMBOL"].astype(str).str.strip(),
        "trade_date": d.isoformat(),
        "deliv_pct": pd.to_numeric(df["DELIV_PER"], errors="coerce"),
        "ttl_qty": pd.to_numeric(df["TTL_TRD_QNTY"], errors="coerce"),
        "deliv_qty": pd.to_numeric(df["DELIV_QTY"], errors="coerce"),
    })
    return out.dropna(subset=["deliv_pct"])


def backfill_delivery(con: sqlite3.Connection, trade_dates: List[str]) -> int:
    con.execute(DELIV_SCHEMA)
    s = _session()
    have = {r[0] for r in con.execute("SELECT DISTINCT trade_date FROM delivery_daily")}
    todo = [d for d in trade_dates if d not in have]
    n = 0
    for i, ds in enumerate(todo):
        d = date.fromisoformat(ds)
        for attempt in range(2):
            try:
                df = fetch_delivery_day(s, d)
                break
            except Exception:
                df = pd.DataFrame()
                time.sleep(1.5)
                s = _session()
        if not df.empty:
            con.executemany(
                "INSERT OR REPLACE INTO delivery_daily(symbol,trade_date,deliv_pct,ttl_qty,deliv_qty) "
                "VALUES (?,?,?,?,?)",
                list(df.itertuples(index=False, name=None)))
            con.commit()
            n += len(df)
        if i % 25 == 0:
            print(f"  delivery {ds} ({i+1}/{len(todo)}) total_rows={n}", flush=True)
        time.sleep(0.4)
    return n


if __name__ == "__main__":
    import sys
    from persona_engine import db
    con = db.connect()
    what = sys.argv[1] if len(sys.argv) > 1 else "earnings"
    if what == "earnings":
        n = backfill_earnings(con, start=sys.argv[2] if len(sys.argv) > 2 else "2022-01-01",
                              end=sys.argv[3] if len(sys.argv) > 3 else None)
        print("earnings rows:", n,
              "| distinct symbols:", con.execute("SELECT COUNT(DISTINCT symbol) FROM corp_earnings_dates").fetchone()[0])
    elif what == "delivery":
        dates = [r[0] for r in con.execute(
            "SELECT DISTINCT trade_date FROM ohlc_daily WHERE trade_date>=? ORDER BY trade_date",
            (sys.argv[2] if len(sys.argv) > 2 else "2024-01-01",))]
        n = backfill_delivery(con, dates)
        print("delivery rows added:", n)
    con.close()
