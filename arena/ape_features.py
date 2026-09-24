"""APE (Accumulation Pattern Engine) — STEP 1: multi-timescale feature table from 1-minute data.
The operator's teaching (Addendum C): institutional accumulation shows up in WHEN volume trades within the day
(certain hours) + daily/weekly/monthly volume shape. So per (symbol, day) we roll 1-min up to daily OHLCV AND
intraday-hour volume-distribution features: volume-into-close, first-hour share, buy-pressure, close-vs-VWAP,
close-strength. Cached to a pickle for STEP 2 (episode/label/model). Point-in-time by construction (daily bars).
READ-ONLY on data."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
CACHE = os.path.join(ROOT, "arena", "ape_daily_features.pkl")
D0, D1 = "2024-01-01", "2026-07-31"

def build():
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = sorted(set(r[0] for r in oc.execute(
        "SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1")) &
        set(r[0] for r in oc.execute("SELECT DISTINCT symbol FROM ohlc_1min WHERE substr(bar_time,1,10)='2026-05-15'")))
    print(f"symbols with 1-min: {len(syms)}", flush=True)
    rows = []
    for i, s in enumerate(syms):
        df = pd.read_sql_query(
            "SELECT substr(bar_time,1,10) d, substr(bar_time,12,5) hm, open,high,low,close,volume "
            "FROM ohlc_1min WHERE symbol=? AND substr(bar_time,1,10) BETWEEN ? AND ? ORDER BY bar_time",
            oc, params=(s, D0, D1))
        if df.empty: continue
        df["pv"] = df.close * df.volume; df["upv"] = np.where(df.close > df.open, df.volume, 0.0)
        for d, g in df.groupby("d"):
            vol = g.volume.sum()
            if vol <= 0: continue
            vwap = g.pv.sum() / vol
            hi, lo, cl, op = g.high.max(), g.low.min(), g.close.iloc[-1], g.open.iloc[0]
            rows.append(dict(symbol=s, d=d, open=op, high=hi, low=lo, close=cl, volume=vol,
                             last_hr_share=g.loc[g.hm >= "14:30", "volume"].sum() / vol,     # volume-into-close (certain hours)
                             first_hr_share=g.loc[g.hm <= "10:15", "volume"].sum() / vol,
                             up_ratio=g.upv.sum() / vol,                                       # buy-pressure (up-min vol share)
                             cvv=cl / vwap - 1,                                                # close vs day VWAP
                             cstr=(cl - lo) / (hi - lo) if hi > lo else 0.5))                  # close strength
        if (i + 1) % 50 == 0: print(f"  {i+1}/{len(syms)} symbols", flush=True)
    oc.close()
    T = pd.DataFrame(rows).sort_values(["symbol", "d"]).reset_index(drop=True)
    T.to_pickle(CACHE)
    print(f"saved {len(T):,} symbol-days for {T.symbol.nunique()} symbols -> {CACHE}", flush=True)
    return T

if __name__ == "__main__":
    build()
