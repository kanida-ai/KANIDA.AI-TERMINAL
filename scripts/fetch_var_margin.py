"""Fetch NSE daily security-wise VaR+ELM margin files (C_VAR1) over a window and build a
per-stock margin time-series -> table nse_var_margin. This is the REAL historical
'intraday margin / leverage' data needed to test the margin-signal hypothesis.

File: https://archives.nseindia.com/archives/nsccl/var/C_VAR1_<DDMMYYYY>_6.DAT
Row (type 20): 20,SYMBOL,SERIES,ISIN,varComputed,_,varApplicable,ELM,_,TOTAL%
TOTAL% (last) = applicable margin % = the leverage proxy (higher = less leverage).
"""
import sqlite3, time, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"}
WIN_START, WIN_END = "2026-01-01", "2026-07-03"

SCHEMA = """
CREATE TABLE IF NOT EXISTS nse_var_margin (
    trade_date TEXT NOT NULL, symbol TEXT NOT NULL, series TEXT,
    var_pct REAL, elm_pct REAL, total_pct REAL,
    PRIMARY KEY (trade_date, symbol, series)
);
CREATE INDEX IF NOT EXISTS idx_var_sym_dt ON nse_var_margin(symbol, trade_date);
"""


def trading_dates():
    con = sqlite3.connect(str(DB))
    ds = [r[0] for r in con.execute(
        "SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min "
        "WHERE substr(bar_time,1,10) BETWEEN ? AND ? ORDER BY d",
        (WIN_START, WIN_END)).fetchall()]
    con.close()
    return ds


def fetch_one(d):
    dd = d[8:10] + d[5:7] + d[0:4]   # YYYY-MM-DD -> DDMMYYYY
    url = f"https://archives.nseindia.com/archives/nsccl/var/C_VAR1_{dd}_6.DAT"
    try:
        r = requests.get(url, headers=H, timeout=25)
        if r.status_code != 200 or "html" in r.headers.get("content-type", ""):
            return d, []
    except Exception:
        return d, []
    rows = []
    for line in r.text.splitlines():
        f = line.split(",")
        if len(f) >= 10 and f[0] == "20":
            try:
                rows.append((d, f[1].strip(), f[2].strip(), float(f[6]), float(f[7]), float(f[9])))
            except ValueError:
                continue
    return d, rows


def main():
    con = sqlite3.connect(str(DB)); con.executescript(SCHEMA); con.commit()
    have = set(r[0] for r in con.execute("SELECT DISTINCT trade_date FROM nse_var_margin").fetchall())
    con.close()
    dates = [d for d in trading_dates() if d not in have]
    print(f"[var] {len(dates)} dates to fetch ({WIN_START}..{WIN_END})", flush=True)
    done = 0; total = 0; empty = 0; t0 = time.time()
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(fetch_one, d): d for d in dates}
        buf = []
        for fu in as_completed(futs):
            d, rows = fu.result(); done += 1
            if not rows: empty += 1
            buf.extend(rows); total += len(rows)
            if len(buf) >= 20000:
                con = sqlite3.connect(str(DB), timeout=60)
                con.executemany("INSERT OR IGNORE INTO nse_var_margin VALUES (?,?,?,?,?,?)", buf); con.commit(); con.close()
                buf = []
            if done % 30 == 0:
                print(f"  [{done}/{len(dates)}] rows={total:,} empty={empty} elapsed={time.time()-t0:.0f}s", flush=True)
        if buf:
            con = sqlite3.connect(str(DB), timeout=60)
            con.executemany("INSERT OR IGNORE INTO nse_var_margin VALUES (?,?,?,?,?,?)", buf); con.commit(); con.close()
    con = sqlite3.connect(str(DB))
    n, nd, ns = con.execute("SELECT count(*), count(distinct trade_date), count(distinct symbol) FROM nse_var_margin").fetchone()
    d1, d2 = con.execute("SELECT min(trade_date), max(trade_date) FROM nse_var_margin").fetchone()
    con.close()
    print(f"[var] DONE: {n:,} rows | {nd} dates ({d1}..{d2}) | {ns} symbols | empty-days {empty} | {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
