"""
BREAKOUT QUANTIFICATION ENGINE — richly characterises every breakout so we can learn (out-of-sample)
which dimensions actually separate the KALYANKJIL/HFCL winners from the duds. Maps the full list:
 - timeframe of breakout: 20d / 60d / 120d / 252d / all-time-high  (multi-day vs -month vs -year)
 - prior trend: last 5d / 20d / 60d slope; regime before break (downtrend / flat-base / uptrend)
 - base quality: width (tightness), volatility contraction, distance from 50/200 DMA
 - prior-breakout history: days since last 60d-high breakout, price then vs now
 - relative strength vs NIFTY (20d, 60d)
 - MICRO (1-min breakout day): day vol surge, biggest single-minute vol spike, CVD/accumulation,
   VWAP rising & close vs VWAP, what minute it broke, pre-move drift
Forward label = the let-winners-run swing return (25% trail / 120d). Writes reports/breakout_features.csv.
Run: python scripts/breakout_features.py
"""
from __future__ import annotations
import os, sys, sqlite3, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import arena_fast as A
KDB = str(ROOT / "db" / "kanida.db")
TRAIL_PCT, HARD_PCT, MAXDAYS = 25.0, 7.0, 120
COST_RT = 0.30; VOL_MIN = 1.5


def nifty_ret():
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    d = pd.read_sql_query("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50' ORDER BY bar_time", con); con.close()
    di = d["bar_time"].str[:10].str.replace("-", "").astype(np.int64).values
    c = d["close"].values.astype(float)
    r60 = np.full(len(c), np.nan); r20 = np.full(len(c), np.nan)
    for k in range(len(c)):
        if k >= 60 and c[k - 60] > 0: r60[k] = c[k] / c[k - 60] - 1
        if k >= 20 and c[k - 20] > 0: r20[k] = c[k] / c[k - 20] - 1
    return {int(di[k]): (r20[k], r60[k]) for k in range(len(c))}


def daily_pack():
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily WHERE symbol!='NIFTY 50' ORDER BY symbol,bar_time", con); con.close()
    df["di"] = df["bar_time"].str[:10].str.replace("-", "").astype(np.int64)
    out = {}
    for sym, g in df.groupby("symbol", sort=False):
        c = g["close"].values.astype(float); h = g["high"].values.astype(float); l = g["low"].values.astype(float)
        o = g["open"].values.astype(float); vol = g["volume"].values.astype(float); n = len(c)
        if n < 260: continue
        pc = np.roll(c, 1); pc[0] = c[0]
        tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
        atr = pd.Series(tr).rolling(20).mean().values
        S = pd.Series(h); hi = lambda m: S.rolling(m).max().shift(1).values
        cS = pd.Series(c)
        out[sym] = dict(di=g["di"].values, o=o, h=h, l=l, c=c, vol=vol, tr=tr, atr=atr,
                        hi20=hi(20), hi60=hi(60), hi120=hi(120), hi252=hi(252),
                        athi=cS.cummax().shift(1).values, sma50=cS.rolling(50).mean().values, sma200=cS.rolling(200).mean().values,
                        vavg=pd.Series(vol).rolling(20).mean().shift(1).values,
                        bw=((cS.rolling(60).max() - cS.rolling(60).min()).shift(1).values) / np.where(c > 0, c, np.nan),
                        atrc=atr / np.where(np.roll(atr, 60) > 0, np.roll(atr, 60), np.nan))
    return out


def swing_ret(o, h, l, c, k):
    if k + 1 >= len(c) or not (o[k + 1] > 0): return None
    entry = o[k + 1]; peak = entry; hard = entry * (1 - HARD_PCT / 100)
    end = min(k + 1 + MAXDAYS, len(c))
    for d in range(k + 1, end):
        if h[d] > peak: peak = h[d]
        stop = max(hard, peak * (1 - TRAIL_PCT / 100))
        if l[d] <= stop: return (stop / entry - 1) * 100 - COST_RT, d - k
    return (c[end - 1] / entry - 1) * 100 - COST_RT, end - 1 - k


def micro(sym, di, hi60):
    """1-min features on the breakout day: single-minute vol spike, CVD accumulation, VWAP rise, break timing."""
    z = A.load(sym)
    if z is None: return None
    ds = z["day_start"]; dd = z["day_id"]
    idx = np.where(dd == di)[0]
    if len(idx) == 0: return None
    k = int(idx[0]); a = int(ds[k]); b = int(ds[k + 1]) if k + 1 < len(ds) else len(z["date"])
    hm = z["hm"][a:b]; o = z["o"][a:b]; h = z["h"][a:b]; l = z["l"][a:b]; c = z["c"][a:b]; v = z["v"][a:b]
    if len(v) < 30 or v.sum() <= 0: return None
    bmask = np.where(h >= hi60)[0]
    bmin = int(hm[bmask[0]]) if len(bmask) else 1520
    bi = int(bmask[0]) if len(bmask) else len(v) - 1
    avgv = v.mean(); max1z = float(v.max() / avgv) if avgv > 0 else 0.0
    n_spike = int((v >= 4 * avgv).sum())                          # single-minute big-volume prints
    cl = np.where((h - l) > 0, (c - l) / (h - l), 0.5)
    cvd = float(np.sum(v * (2 * cl - 1)) / v.sum())               # buying pressure (accumulation)
    typ = (h + l + c) / 3.0; vwap = np.cumsum(typ * v) / np.cumsum(v)
    vwap_slope = float((vwap[-1] / vwap[max(bi, 1)] - 1) * 100)   # VWAP rising after the break
    close_vs_vwap = float((c[-1] / vwap[-1] - 1) * 100)
    premove = float((c[bi] / o[0] - 1) * 100) if bi >= 0 else 0.0 # drift into the break
    return max1z, n_spike, cvd, vwap_slope, close_vs_vwap, premove, bmin


def main():
    t0 = time.time()
    print("loading daily + nifty ...", flush=True)
    D = daily_pack(); NF = nifty_ret()
    rows = []
    for sym, p in D.items():
        c = p["c"]; hi60 = p["hi60"]; n = len(c); di = p["di"]
        last_bo_i = -999; last_bo_px = np.nan
        k = 61
        while k < n - 1:
            if not (np.isfinite(hi60[k]) and c[k] > hi60[k] and c[k - 1] <= hi60[k - 1]):
                k += 1; continue
            va = p["vavg"][k]
            if not (np.isfinite(va) and va > 0) or (p["vol"][k] / va) < VOL_MIN or not np.isfinite(p["bw"][k]):
                k += 1; continue
            r = swing_ret(p["o"], p["h"], p["l"], c, k)
            if r is None:
                k += 1; continue
            ret, hold = r
            # timeframe tier
            hh = p["h"][k]
            tier = ("ath" if (np.isfinite(p["athi"][k]) and hh >= p["athi"][k]) else
                    "y1" if (np.isfinite(p["hi252"][k]) and hh >= p["hi252"][k]) else
                    "m6" if (np.isfinite(p["hi120"][k]) and hh >= p["hi120"][k]) else "m3")
            ret5 = c[k] / c[k - 5] - 1 if c[k - 5] > 0 else 0.0
            ret20 = c[k] / c[k - 20] - 1 if c[k - 20] > 0 else 0.0
            ret60 = c[k] / c[k - 60] - 1 if c[k - 60] > 0 else 0.0
            regime = "down" if ret60 < -0.05 else "up" if ret60 > 0.15 else "flat"
            nf = NF.get(int(di[k]), (0.0, 0.0))
            rs20 = ret20 - (nf[0] if np.isfinite(nf[0]) else 0.0)
            rs60 = ret60 - (nf[1] if np.isfinite(nf[1]) else 0.0)
            dsl = k - last_bo_i if last_bo_i > 0 else 999
            pvl = (c[k] / last_bo_px - 1) if (np.isfinite(last_bo_px) and last_bo_px > 0) else 0.0
            m = micro(sym, int(di[k]), float(hi60[k]))
            max1z, nsp, cvd, vws, cvv, prem, bmin = m if m else (0, 0, 0, 0, 0, 0, 1520)
            rows.append((sym, int(di[k]), ret, hold, tier, regime,
                         round(ret5 * 100, 1), round(ret20 * 100, 1), round(ret60 * 100, 1),
                         round(float(p["bw"][k]), 3), round(float(p["atrc"][k]) if np.isfinite(p["atrc"][k]) else 1, 2),
                         round(p["c"][k] / p["sma50"][k] - 1, 3) if p["sma50"][k] > 0 else 0,
                         round(p["c"][k] / p["sma200"][k] - 1, 3) if p["sma200"][k] > 0 else 0,
                         round(rs20 * 100, 1), round(rs60 * 100, 1), dsl, round(pvl * 100, 1),
                         round(p["vol"][k] / va, 1), round(float(p["tr"][k] / p["atr"][k]) if p["atr"][k] > 0 else 0, 1),
                         round(max1z, 1), nsp, round(cvd, 3), round(vws, 2), round(cvv, 2), round(prem, 1), bmin))
            last_bo_i = k; last_bo_px = c[k]
            k = k + 1 + hold
    cols = ["symbol", "di", "fwd_ret", "hold", "tier", "regime", "ret5", "ret20", "ret60", "base_width",
            "atr_contract", "dist_sma50", "dist_sma200", "rs20", "rs60", "days_since_bo", "price_vs_last_bo",
            "vol_surge", "thrust", "max_1min_volz", "n_1min_spikes", "cvd", "vwap_slope", "close_vs_vwap",
            "premove", "break_minute"]
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(ROOT / "reports" / "breakout_features.csv", index=False)
    print(f"\n  quantified {len(df):,} breakouts with {len(cols)-4} features in {time.time()-t0:.0f}s")
    print(f"  written: reports/breakout_features.csv")


if __name__ == "__main__":
    main()
