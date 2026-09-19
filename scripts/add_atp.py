"""
Derive ATP (average traded price) at the 1-min level and store it in ohlc_1min.atp.
ATP here = running intraday VWAP (resets each day):
    typical = (high+low+close)/3 ;  atp[t] = cumsum(typical*volume)/cumsum(volume)  within the day
This matches the terminal's "average traded price so far today". (Kite historical has no true
tick-ATP; this minute-VWAP proxy is within a small fraction of a % of it.)

Idempotent: adds the column if missing, recomputes per symbol. Run AFTER the backfill.
Optionally also fills ohlc_5min.atp and ohlc_daily.atp (day VWAP).

Run: PYTHONIOENCODING=utf-8 python add_atp.py [table ...]   (default: ohlc_1min ohlc_5min ohlc_daily)
"""
import sys, sqlite3
from datetime import datetime
from pathlib import Path
import numpy as np, pandas as pd

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")


def log(m): print(f"{datetime.now():%H:%M:%S} {m}", flush=True)


def ensure_col(con, table):
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    if "atp" not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN atp REAL")
        con.commit()


def fill(table):
    con = sqlite3.connect(str(DB), timeout=120)
    con.execute("PRAGMA journal_mode=WAL")
    ensure_col(con, table)
    syms = [r[0] for r in con.execute(f"SELECT DISTINCT symbol FROM {table} ORDER BY symbol").fetchall()]
    log(f"[{table}] {len(syms)} symbols")
    for n, s in enumerate(syms, 1):
        df = pd.read_sql(f"SELECT bar_time,high,low,close,volume FROM {table} WHERE symbol=? ORDER BY bar_time",
                         con, params=[s])
        if df.empty:
            continue
        df["day"] = df["bar_time"].str[:10]
        typ = (df["high"] + df["low"] + df["close"]) / 3.0
        vol = df["volume"].clip(lower=0).fillna(0)
        tv = typ * vol
        g = df.groupby("day", sort=False)
        cum_tv = tv.groupby(df["day"], sort=False).cumsum()
        cum_v = vol.groupby(df["day"], sort=False).cumsum()
        atp = np.where(cum_v > 0, cum_tv / cum_v, typ)          # fall back to typical when no volume yet
        upd = list(zip([round(float(a), 4) for a in atp], [s] * len(df), df["bar_time"].tolist()))
        con.executemany(f"UPDATE {table} SET atp=? WHERE symbol=? AND bar_time=?", upd)
        con.commit()
        if n % 50 == 0:
            log(f"  [{table}] {n}/{len(syms)} (last {s})")
    filled = con.execute(f"SELECT count(*) FROM {table} WHERE atp IS NOT NULL").fetchone()[0]
    tot = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    log(f"[{table}] DONE: atp filled {filled:,}/{tot:,} rows")
    con.close()


def main():
    tables = sys.argv[1:] or ["ohlc_1min", "ohlc_5min", "ohlc_daily"]
    for t in tables:
        fill(t)
    log("ATP FILL COMPLETE")


if __name__ == "__main__":
    main()
