"""APE — decisive test, PART A: discrete 1-MINUTE microstructure EVENT flags per (symbol, day).
Not daily averages — actual events in the tape, vs a TRAILING per-stock baseline (point-in-time, no lookahead):
  ABSORPTION  = a minute with volume >= 3x the stock's trailing-60d avg minute-volume, whose bar LOW is in the
                bottom 20% of the day's range AND that CLOSES in the top 40% of its own bar (big buyer eating supply
                at the low without letting price mark down) — the HFCL-Dec tell.
  LATE-BUY    = last-hour up-minute vs down-minute volume imbalance (institutional footprint into the close).
  DELTA       = whole-day up-min vs down-min volume imbalance (buy pressure).
Cached to ape_micro_features.pkl for PART B (the case-control horse-race). READ-ONLY on data."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
CACHE = os.path.join(ROOT, "arena", "ape_micro_features.pkl")
D0, D1 = "2024-01-01", "2026-07-31"

def build():
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = sorted(set(r[0] for r in oc.execute("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1")) &
                  set(r[0] for r in oc.execute("SELECT DISTINCT symbol FROM ohlc_1min WHERE substr(bar_time,1,10)='2026-05-15'")))
    print(f"symbols: {len(syms)}", flush=True)
    rows = []
    for i, s in enumerate(syms):
        df = pd.read_sql_query("SELECT substr(bar_time,1,10) d, substr(bar_time,12,5) hm, open,high,low,close,volume "
                               "FROM ohlc_1min WHERE symbol=? AND substr(bar_time,1,10) BETWEEN ? AND ? ORDER BY bar_time",
                               oc, params=(s, D0, D1))
        if df.empty: continue
        dvol = df.groupby("d")["volume"].sum()
        minbase = (dvol.rolling(60).mean().shift(1) / 375.0)          # trailing point-in-time avg minute-volume
        for d, g in df.groupby("d"):
            mb = minbase.get(d, np.nan)
            if not (mb == mb) or mb <= 0: continue
            dl, dh = g.low.min(), g.high.max(); rng = dh - dl
            if rng <= 0: continue
            brng = (g.high - g.low).replace(0, np.nan)
            spike = g.volume >= 3 * mb
            close_strong = (g.close - g.low) / brng > 0.6
            near_low = g.low <= dl + 0.20 * rng
            n_absorb = int((spike & close_strong & near_low).sum())
            last = g[g.hm >= "14:30"]
            up_l = last.loc[last.close > last.open, "volume"].sum(); dn_l = last.loc[last.close < last.open, "volume"].sum()
            upv = g.loc[g.close > g.open, "volume"].sum(); dnv = g.loc[g.close < g.open, "volume"].sum(); tv = g.volume.sum()
            rows.append(dict(symbol=s, d=d, n_absorb=n_absorb, n_spike=int(spike.sum()),
                             late_imb=up_l / (dn_l + 1.0), delta=(upv - dnv) / (tv + 1.0)))
        if (i + 1) % 50 == 0: print(f"  {i+1}/{len(syms)}", flush=True)
    oc.close()
    T = pd.DataFrame(rows).sort_values(["symbol", "d"]).reset_index(drop=True)
    T.to_pickle(CACHE)
    print(f"saved {len(T):,} symbol-days -> {CACHE} | absorb>0 on {(T.n_absorb>0).mean():.1%} of days", flush=True)

if __name__ == "__main__":
    build()
