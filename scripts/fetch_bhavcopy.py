"""Fetch NSE daily all-stock bhavcopy (sec_bhavdata_full) over the VaR-margin window ->
table nse_bhav_daily. Gives daily OHLC for EVERY stock (incl the small/mid-caps where
margin hikes actually happen), so the margin event study covers the full event set."""
import sqlite3, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"}
MON = {m: f"{i:02d}" for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}
SCHEMA = """
CREATE TABLE IF NOT EXISTS nse_bhav_daily (
    trade_date TEXT NOT NULL, symbol TEXT NOT NULL, series TEXT,
    open REAL, high REAL, low REAL, close REAL, prev_close REAL,
    PRIMARY KEY (trade_date, symbol, series)
);
CREATE INDEX IF NOT EXISTS idx_bhav_sym_dt ON nse_bhav_daily(symbol, trade_date);
"""


def dates_from_margin():
    con = sqlite3.connect(str(DB))
    ds = [r[0] for r in con.execute("SELECT DISTINCT trade_date FROM nse_var_margin ORDER BY trade_date")]
    con.close()
    return ds


def fetch_one(d):
    dd = d[8:10] + d[5:7] + d[0:4]
    url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{dd}.csv"
    try:
        r = requests.get(url, headers=H, timeout=25)
        if r.status_code != 200 or "html" in r.headers.get("content-type", ""):
            return d, []
    except Exception:
        return d, []
    rows = []
    lines = r.text.splitlines()
    for line in lines[1:]:
        f = [x.strip() for x in line.split(",")]
        if len(f) < 9:
            continue
        try:
            sym, ser, dt = f[0], f[1], f[2]           # DD-MON-YYYY
            if ser not in ("EQ", "BE", "SM", "ST"):
                continue
            iso = f"{dt[7:11]}-{MON[dt[3:6].upper()]}-{dt[0:2]}"
            rows.append((iso, sym, ser, float(f[4]), float(f[5]), float(f[6]), float(f[8]), float(f[3])))
        except (ValueError, KeyError, IndexError):
            continue
    return d, rows


def main():
    con = sqlite3.connect(str(DB)); con.executescript(SCHEMA); con.commit()
    have = set(r[0] for r in con.execute("SELECT DISTINCT trade_date FROM nse_bhav_daily"))
    con.close()
    dates = [d for d in dates_from_margin() if d not in have]
    print(f"[bhav] {len(dates)} dates to fetch", flush=True)
    done = total = empty = 0; t0 = time.time(); buf = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(fetch_one, d): d for d in dates}
        for fu in as_completed(futs):
            d, rows = fu.result(); done += 1
            if not rows: empty += 1
            buf.extend(rows); total += len(rows)
            if len(buf) >= 15000:
                con = sqlite3.connect(str(DB), timeout=60)
                con.executemany("INSERT OR IGNORE INTO nse_bhav_daily VALUES (?,?,?,?,?,?,?,?)", buf); con.commit(); con.close(); buf = []
            if done % 30 == 0:
                print(f"  [{done}/{len(dates)}] rows={total:,} elapsed={time.time()-t0:.0f}s", flush=True)
    if buf:
        con = sqlite3.connect(str(DB), timeout=60)
        con.executemany("INSERT OR IGNORE INTO nse_bhav_daily VALUES (?,?,?,?,?,?,?,?)", buf); con.commit(); con.close()
    con = sqlite3.connect(str(DB))
    n, nd, ns = con.execute("SELECT count(*), count(distinct trade_date), count(distinct symbol) FROM nse_bhav_daily").fetchone()
    con.close()
    print(f"[bhav] DONE: {n:,} rows | {nd} dates | {ns} symbols | empty {empty} | {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
