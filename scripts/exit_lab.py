"""
EXIT LAB — same breakout entries, many EXIT rules. The exit is the one proven return lever, so we test
structure-based trails (ride the higher-lows, exit only on a real structure break) vs the fixed % trail.
Includes the user's method: ratchet the stop up to the PRIOR DAY's low/close. OOS era-split for honesty.
Run: python scripts/exit_lab.py
"""
from __future__ import annotations
import os, sys, sqlite3, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import arena_fast as A
import agent_jarvis_swing as J
KDB = str(ROOT / "db" / "kanida.db")
COST_RT = 0.30; VOL_MIN = 1.5; HARD = 7.0; MAXD = 250


def load_daily():
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily WHERE symbol!='NIFTY 50' ORDER BY symbol,bar_time", con); con.close()
    df["di"] = df["bar_time"].str[:10].str.replace("-", "").astype(np.int64)
    out = {}
    for sym, g in df.groupby("symbol", sort=False):
        c = g["close"].values.astype(float); h = g["high"].values.astype(float); l = g["low"].values.astype(float)
        o = g["open"].values.astype(float); vol = g["volume"].values.astype(float)
        if len(c) < 260: continue
        pc = np.roll(c, 1); pc[0] = c[0]
        tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
        atr = pd.Series(tr).rolling(14).mean().values
        cS = pd.Series(c)
        out[sym] = dict(di=g["di"].values, o=o, h=h, l=l, c=c, atr=atr,
                        hi60=pd.Series(h).rolling(60).max().shift(1).values,
                        vavg=pd.Series(vol).rolling(20).mean().shift(1).values, vol=vol,
                        ema20=cS.ewm(span=20).mean().values, ema10=cS.ewm(span=10).mean().values)
    return out


def sim(method, p, k):
    o, h, l, c, atr = p["o"], p["h"], p["l"], p["c"], p["atr"]
    if k + 1 >= len(c) or not (o[k + 1] > 0): return None
    entry = o[k + 1]; hard = entry * (1 - HARD / 100)
    end = min(k + 1 + MAXD, len(c)); peakh = entry; stop = hard
    for d in range(k + 1, end):
        if h[d] > peakh: peakh = h[d]
        armed = (d - k) > 3
        if method == "fixed25":
            cand = peakh * (1 - 25 / 100)
        elif method == "fixed15":
            cand = peakh * (1 - 15 / 100)
        elif method == "prior_low":                     # user's: ratchet to prior day's low
            cand = l[d - 1]
        elif method == "prior_low_atr":                 # prior day low minus 0.5 ATR (room)
            cand = l[d - 1] - 0.5 * atr[d - 1]
        elif method == "prior_close":                   # ratchet to prior day's close
            cand = c[d - 1] * 0.995
        elif method == "chandelier3":                   # peak high - 3 ATR
            cand = peakh - 3.0 * atr[d]
        elif method == "ema20":                         # structural: exit on close < EMA20
            if c[d] < p["ema20"][d] and armed:
                return (c[d] / entry - 1) * 100 - COST_RT, d - k
            cand = hard
        elif method == "swinglow10":
            cand = np.min(l[max(k + 1, d - 10):d]) if d > k + 1 else hard
        else:
            cand = hard
        if armed and cand > stop:
            stop = cand                                 # ratchet up only
        if l[d] <= stop:
            return (stop / entry - 1) * 100 - COST_RT, d - k
    return (c[end - 1] / entry - 1) * 100 - COST_RT, end - 1 - k


METHODS = ["fixed25", "prior_low", "prior_low_atr", "prior_close", "chandelier3", "ema20", "swinglow10"]


def main():
    t0 = time.time()
    D = load_daily()
    ev = {m: [] for m in METHODS}
    for sym, p in D.items():
        c = p["c"]; hi60 = p["hi60"]; n = len(c); di = p["di"]; k = 61
        while k < n - 1:
            if not (np.isfinite(hi60[k]) and c[k] > hi60[k] and c[k - 1] <= hi60[k - 1]):
                k += 1; continue
            va = p["vavg"][k]
            if not (np.isfinite(va) and va > 0) or (p["vol"][k] / va) < VOL_MIN:
                k += 1; continue
            hold_ref = None
            for m in METHODS:
                r = sim(m, p, k)
                if r is not None:
                    ev[m].append((sym, int(di[k]), int(di[min(k + r[1], n - 1)]), r[0], r[1]))
                    if m == "fixed25": hold_ref = r[1]
            k = k + 1 + (hold_ref if hold_ref else 5)
    print(f"  detected entries, simulated {len(METHODS)} exits in {time.time()-t0:.0f}s\n")
    print(f"{'exit method':<16}{'trades':>8}{'win%':>6}{'ret/trade':>10}{'avg hold':>10}{'CAGR':>7}{'maxDD':>8}{'Calmar':>8}")
    res = {}
    for m in METHODS:
        df = pd.DataFrame(ev[m], columns=["symbol", "entry_di", "exit_di", "cap_pct", "hold"])
        cur = J.swing_portfolio(df); met = A.metrics(cur); res[m] = (df, met)
        print(f"{m:<16}{len(df):>8}{(df.cap_pct>0).mean()*100:>5.0f}%{df.cap_pct.mean():>+9.2f}%{df.hold.mean():>9.0f}d{met['cagr']:>6}%{met['max_dd']:>7}%{str(met['calmar']):>8}")
    # OOS era-split for the best-Calmar method
    best = max(res, key=lambda m: (res[m][1]['calmar'] or 0))
    print(f"\n  best-Calmar exit = '{best}'. Out-of-sample era split:")
    df = res[best][0]
    print(f"  {'period':<16}{'trades':>8}{'ret/trade':>10}{'CAGR':>7}{'maxDD':>8}{'Calmar':>8}")
    for lo, hi, lab in [(20130101, 20200101, "2013-2019"), (20200101, 20230101, "2020-2022"), (20230101, 20270101, "2023-2026 OOS")]:
        s = df[(df.entry_di >= lo) & (df.entry_di < hi)]
        if len(s) < 20: continue
        m = A.metrics(J.swing_portfolio(s))
        print(f"  {lab:<16}{len(s):>8}{s.cap_pct.mean():>+9.2f}%{m['cagr']:>6}%{m['max_dd']:>7}%{str(m['calmar']):>8}")


if __name__ == "__main__":
    main()
