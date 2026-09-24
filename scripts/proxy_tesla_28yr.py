"""2.8-year PROXY-TESLA short engine + cost-adjusted walk-forward.
Rebuilds Tesla's short gate from Step-1-validated OHLCV proxies (tape/book dropped):
  REGIME  = market down-breadth >=55% AND market median from-open <0  (native, cross-sectional)
  VWAP    = close below running VWAP by >=8 bps               (perfect proxy for close-ATP gap, r=1.00)
  AGGR    = close-location <=0.35 (closing near lows)         (proxy for net_aggression, r~0.5)
  SHOCK   = causal volume_ratio >=2.5 (vs trailing 20-min)
  WEAK    = down >=0.2% from open
Entry t, SHORT, exit t+15m. Fixed rule thresholds (NOT fitted) -> per-year results are OOS by
construction. Reported gross and NET at realistic short-side cost. Sample = ~90 days spread
across the 2.8yr (all regimes)."""
import sqlite3, pickle
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
CACHE = ROOT / "docs" / "ops" / "_proxy_tesla_sample.pkl"
N_DAYS = 90
HOLD = 15
COSTS = [0.10, 0.12, 0.15]   # % round-trip short (STT+brokerage+slippage)


def extract():
    con = sqlite3.connect(str(DB), timeout=180); con.execute("PRAGMA query_only=1")
    alld = [r[0] for r in con.execute("SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min ORDER BY d")]
    rng = np.random.default_rng(11)
    pick = sorted(rng.choice(alld, size=min(N_DAYS, len(alld)), replace=False).tolist())
    frames = []
    for d in pick:
        q = ("SELECT symbol, substr(bar_time,12,5) hm, open,high,low,close,volume "
             "FROM ohlc_1min WHERE bar_time BETWEEN ? AND ?")
        f = pd.read_sql_query(q, con, params=(d + " 09:15:00", d + " 15:29:00")); f["day"] = d
        frames.append(f)
    con.close()
    df = pd.concat(frames, ignore_index=True)
    pickle.dump(df, open(CACHE, "wb"))
    return df


def build(df):
    df = df.sort_values(["symbol", "day", "hm"]).reset_index(drop=True)
    g = df.groupby(["symbol", "day"], sort=False)
    df["o915"] = g["open"].transform("first")
    df["ret_open"] = df.close / df.o915 - 1
    typ = (df.high + df.low + df.close) / 3.0
    df["vwap"] = (typ * df.volume).groupby([df.symbol, df.day]).cumsum() / g["volume"].cumsum().replace(0, np.nan)
    df["vwapdev_bps"] = (df.close - df.vwap) / df.close * 1e4
    df["closeloc"] = (df.close - df.low) / (df.high - df.low).replace(0, np.nan)
    df["volbase"] = g["volume"].transform(lambda x: x.shift(1).rolling(20, min_periods=5).mean())
    df["volratio"] = df.volume / df.volbase.replace(0, np.nan)
    df["fwd"] = g["close"].shift(-HOLD) / df.close - 1
    # cross-sectional market breadth per (day, minute)
    bd = df.groupby(["day", "hm"]).agg(mkt_med=("ret_open", "median"),
                                       down_breadth=("ret_open", lambda s: float((s < 0).mean() * 100)))
    df = df.merge(bd, on=["day", "hm"], how="left")
    return df


def gate(df):
    regime = (df.down_breadth >= 55) & (df.mkt_med < 0)
    stock = (df.vwapdev_bps <= -8) & (df.closeloc <= 0.35) & (df.volratio >= 2.5) & (df.ret_open <= -0.002)
    tod = (df.hm > "09:30") & (df.hm < "15:10")
    return regime & stock & tod & df.fwd.notna()


def report(sig, cost):
    sig = sig.copy()
    sig["net"] = (-sig.fwd * 100) - cost      # short return net of cost, %
    sig["year"] = sig.day.str[:4]
    n_days = sig.day.nunique()
    print(f"\n  net of {cost:.2f}% cost:   {'period':<10}{'signals':>9}{'sig/day':>9}{'shortWin%':>11}{'meanNet%':>10}{'total%':>9}{'PF':>6}")
    for per in ["2024", "2025", "2026", "ALL"]:
        d = sig if per == "ALL" else sig[sig.year == per]
        if len(d) == 0: continue
        w = d.net[d.net > 0]; l = d.net[d.net < 0]
        pf = w.sum() / -l.sum() if l.sum() < 0 else 99
        dd = d.day.nunique()
        print(f"                       {per:<10}{len(d):>9}{len(d)/max(1,dd):>9.1f}{(d.net>0).mean()*100:>10.1f}%"
              f"{d.net.mean():>10.3f}{d.net.sum():>9.0f}{pf:>6.2f}")


def main():
    if CACHE.exists():
        df = pickle.load(open(CACHE, "rb"))
        print(f"[*] cached sample: {len(df):,} bars", flush=True)
    else:
        print("[*] extracting ~90-day 2.8yr sample...", flush=True); df = extract()
    df = build(df)
    print(f"[*] {len(df):,} bars | {df.symbol.nunique()} stocks | {df.day.nunique()} days "
          f"{df.day.min()}..{df.day.max()}", flush=True)
    sig = df[gate(df)].copy()
    print(f"[*] SHORT signals fired: {len(sig):,}  ({len(sig)/df.day.nunique():.1f}/day)  "
          f"| gross mean fwd short {(-sig.fwd.mean()*100):+.3f}% | gross shortWin {(sig.fwd<0).mean()*100:.1f}%")
    for c in COSTS:
        report(sig, c)
    print("\n  (short return = -(fwd 15m %); NET = gross - cost. Rule thresholds fixed, so each year is out-of-sample.)")


if __name__ == "__main__":
    main()
