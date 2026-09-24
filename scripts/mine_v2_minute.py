"""V2 Falcon Intraday Alpha Mining — 1-MINUTE resolution, minute-scale holds. Separate from
v1/existing engine. Maps the FRONTIER of (win rate x net-return-after-cost x frequency) across
trigger families x holding horizons x long/short, ranked by expectancy/profit factor.

Every metric net of COST% round-trip. Hindsight-free: features at bar t, outcome from t+1..t+h.
Sample: ~50 days spread across 2.5yr (time-of-day + regime variety). F&O vs non-F&O split.
"""
import sqlite3, pickle
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
SAMP = ROOT / "docs" / "ops" / "_v2_minute_sample.pkl"
COST = 0.10           # % round-trip (brokerage+STT+slippage) — cash intraday
HORIZONS = [1, 3, 5, 10, 15, 30]


def extract(n_dates=50, seed=7):
    con = sqlite3.connect(str(DB), timeout=180); con.execute("PRAGMA query_only=1")
    alld = [r[0] for r in con.execute("SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min ORDER BY d")]
    rng = np.random.default_rng(seed)
    pick = sorted(rng.choice(alld, size=min(n_dates, len(alld)), replace=False).tolist())
    fno = set(r[0] for r in con.execute("SELECT DISTINCT symbol FROM fo_stock_master"))
    frames = []
    for d in pick:
        q = ("SELECT symbol, substr(bar_time,12,5) hm, open,high,low,close,volume "
             "FROM ohlc_1min WHERE bar_time BETWEEN ? AND ?")
        f = pd.read_sql_query(q, con, params=(d + " 09:15:00", d + " 15:29:00")); f["d"] = d
        frames.append(f)
    con.close()
    df = pd.concat(frames, ignore_index=True)
    df["fno"] = df.symbol.isin(fno)
    pickle.dump(df, open(SAMP, "wb"))
    return df


def features(df):
    df = df.sort_values(["symbol", "d", "hm"]).reset_index(drop=True)
    g = df.groupby(["symbol", "d"], sort=False)
    c = df.close
    df["ret1"] = g["close"].pct_change()
    df["relvol"] = df.volume / g["volume"].transform(lambda x: x.rolling(20, min_periods=5).mean())
    df["imp3"] = c / g["close"].shift(3) - 1
    up = (df.ret1 > 0).astype(int)
    df["consec_up"] = up * (up.groupby([df.symbol, df.d]).cumsum() -
                            up.groupby([df.symbol, df.d]).cumsum().where(up == 0).ffill().fillna(0))
    df["p10low"] = g["low"].transform(lambda x: x.rolling(10, min_periods=3).min().shift(1))
    df["p10high"] = g["high"].transform(lambda x: x.rolling(10, min_periods=3).max().shift(1))
    df["mm"] = df.hm.str[:2].astype(int)
    for h in HORIZONS:
        df[f"fwd{h}"] = g["close"].shift(-h) / c - 1
    # only bars with room to hold + past the opening noise
    df = df[(df.hm > "09:25") & (df.hm < "15:00")]
    return df


TRIGGERS = [
    ("VOL_SPIKE_UP (long)", "long", lambda d: (d.relvol > 3) & (d.ret1 > 0.001)),
    ("EXHAUSTION_UP (short)", "short", lambda d: (d.ret1 > 0.008) & (d.relvol > 5)),
    ("IMPULSE3_UP (long)", "long", lambda d: (d.imp3 > 0.006) & (d.relvol > 1.5)),
    ("MOMENTUM_3UP (long)", "long", lambda d: d.consec_up >= 3),
    ("MICRO_SWEEP_UP (long)", "long", lambda d: (d.low < d.p10low) & (d.close > d.p10low) & (d.ret1 > 0)),
    ("VOL_SPIKE_DN (short)", "short", lambda d: (d.relvol > 3) & (d.ret1 < -0.001)),
    ("EXHAUSTION_DN (long)", "long", lambda d: (d.ret1 < -0.008) & (d.relvol > 5)),
    ("BREAKOUT_10 (long)", "long", lambda d: (d.close > d.p10high) & (d.relvol > 2)),
]


def evaluate(df):
    rows = []
    for name, dirn, fn in TRIGGERS:
        m = fn(df).fillna(False)
        sub = df[m]
        for h in HORIZONS:
            r = sub[f"fwd{h}"].dropna()
            if len(r) < 200: continue
            sign = 1 if dirn == "long" else -1
            net = r.values * sign * 100 - COST
            w = net[net > 0]; l = net[net < 0]
            rows.append(dict(trigger=name, dir=dirn, h=h, n=len(net), wr=(net > 0).mean() * 100,
                             gross=r.mean() * sign * 100, net=net.mean(), med=np.median(net),
                             pf=w.sum() / -l.sum() if l.sum() < 0 else 99, avgw=w.mean() if len(w) else 0,
                             avgl=l.mean() if len(l) else 0))
    return pd.DataFrame(rows)


def main():
    if SAMP.exists():
        df = pickle.load(open(SAMP, "rb"))
    else:
        print("[*] extracting ~50 full-day 1-min sample...", flush=True); df = extract()
    print(f"[*] sample: {len(df):,} bars | {df.symbol.nunique()} symbols | {df.d.nunique()} days | "
          f"F&O {df[df.fno].symbol.nunique()} / non {df[~df.fno].symbol.nunique()}", flush=True)
    df = features(df)
    res = evaluate(df)
    res = res.sort_values("net", ascending=False)

    print("\n" + "=" * 118)
    print("V2 FRONTIER — every trigger x horizon, net of %.2f%% cost, ranked by NET return/trade" % COST)
    print("=" * 118)
    print(f"{'trigger':<24}{'dir':>6}{'h(min)':>7}{'n':>8}{'WR%':>7}{'gross%':>8}{'NET%':>8}{'med%':>7}{'PF':>6}{'avgW':>7}{'avgL':>7}")
    for _, r in res.iterrows():
        flag = "  <<" if (r.wr >= 80 and r.net > 0) else ""
        print(f"{r.trigger:<24}{r['dir']:>6}{r.h:>7}{r.n:>8}{r.wr:>6.1f}%{r.gross:>7.3f}%{r.net:>7.3f}%"
              f"{r.med:>6.3f}%{r.pf:>6.2f}{r.avgw:>7.3f}{r.avgl:>7.3f}{flag}")

    print("\nFRONTIER by WIN-RATE band (best NET in each band, PF>1 only):")
    good = res[(res.pf > 1)]
    for lo, hi in [(90, 101), (80, 90), (70, 80), (60, 70)]:
        b = good[(good.wr >= lo) & (good.wr < hi)]
        if len(b) == 0:
            print(f"  {lo}-{hi if hi<101 else 100}% WR: (none with PF>1 & net>0)"); continue
        r = b.sort_values("net").iloc[-1]
        print(f"  {lo}-{hi if hi<101 else 100}%% WR: {r.trigger} {r['dir']} h={r.h}m -> WR {r.wr:.1f}%, NET {r.net:+.3f}%/trade, PF {r.pf:.2f}, n={r.n}")


if __name__ == "__main__":
    main()
