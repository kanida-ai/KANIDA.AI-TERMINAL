"""Phase 1.5 Stage B — Layer-2 big-player absorption footprint, from the opening MINUTE bars.
Per stock-day (checkpoint 09:20, window 09:15-09:19): detect sweep -> absorption (volume at the
extreme bar) -> immediate reaction (reversal vs continuation). Build 4 footprints and test:
 (1) which footprint predicts SHORT profitability on DOWN-tape mornings (continuation vs reversal)?
 (2) does adding the footprint LIFT the short basket over the Phase-1 signals alone?
Long & short kept separate; SHORT is the live side (Phase-1.5).
"""
import pickle
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.linear_model import LogisticRegression

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
BARS = ["0915", "0916", "0917", "0918", "0919"]
N = 5


def build():
    m = pickle.load(open(ROOT / "docs" / "ops" / "_foor_minutes.pkl", "rb")).sort_values(["symbol", "date"])
    g = m.groupby("symbol")
    m["prev_L"] = g.fullL.shift(1); m["prev_H"] = g.fullH.shift(1)
    lows = m[[f"l{b}" for b in BARS]].values.astype(float)
    highs = m[[f"h{b}" for b in BARS]].values.astype(float)
    vols = m[[f"v{b}" for b in BARS]].values.astype(float)
    cC = m["c0919"].values.astype(float)
    WL = np.nanmin(lows, axis=1); WH = np.nanmax(highs, axis=1)
    ix = np.arange(len(m))
    lowbar = np.nanargmin(np.where(np.isnan(lows), np.inf, lows), axis=1)
    highbar = np.nanargmax(np.where(np.isnan(highs), -np.inf, highs), axis=1)
    meanv = np.nanmean(vols, axis=1); meanv = np.where(meanv > 0, meanv, np.nan)
    absv_low = vols[ix, lowbar] / meanv          # volume at the sweep-low bar (absorption)
    absv_high = vols[ix, highbar] / meanv
    rng = WH - WL; rng = np.where(rng > 0, rng, np.nan)
    react_up = (cC - WL) / rng                   # 1 = closed back at highs (recovered off low)
    react_dn = (WH - cC) / rng                   # 1 = closed back at lows (rejected off high)
    dn_sweep = ((WL < m.l0915.values) | (WL < m.prev_L.values)).astype(float)
    up_sweep = ((WH > m.h0915.values) | (WH > m.prev_H.values)).astype(float)
    m["short_cont"] = dn_sweep * absv_low * (1 - react_up)   # pushed down, holds low, vol -> SHORT
    m["short_rev"] = up_sweep * absv_high * react_dn         # swept up, absorbed, rolls down -> SHORT
    m["long_cont"] = up_sweep * absv_high * (1 - react_dn)   # LONG
    m["long_rev"] = dn_sweep * absv_low * react_up           # LONG
    m["short_fp"] = m[["short_cont", "short_rev"]].max(axis=1)
    m["rod"] = m.eod / cC - 1
    # Layer-1 tape from first 3 min (09:15-09:17)
    m["drive17"] = m.c0917 / m.o0915 - 1
    m["v3"] = m[["v0915", "v0916", "v0917"]].sum(axis=1)
    day = m.groupby("date").apply(lambda d: (d.drive17 * d.v3).sum() / max(d.v3.sum(), 1)).rename("vwmove").reset_index()
    lo, hi = day.vwmove.quantile(0.34), day.vwmove.quantile(0.66)
    day["tape"] = np.where(day.vwmove <= lo, "DOWN", np.where(day.vwmove >= hi, "UP", "UNCLEAR"))
    m = m.merge(day[["date", "tape"]], on="date", how="left")
    # merge Phase-1 short score
    sc = pickle.load(open(ROOT / "docs" / "ops" / "_foor_scored.pkl", "rb"))[["symbol", "date", "p_short"]]
    m = m.merge(sc, on=["symbol", "date"], how="left")
    return m.dropna(subset=["rod", "short_cont", "short_rev", "tape"])


def main():
    m = build()
    dn = m[m.tape == "DOWN"].copy()
    print(f"panel {len(m):,} stock-days | DOWN-tape {len(dn):,}")
    print("\n(1) SHORT footprint predictive power on DOWN-tape mornings (IC vs -rod = short profit):")
    for fp in ["short_cont", "short_rev", "short_fp"]:
        s = dn[[fp, "rod"]].dropna()
        ic = spearmanr(s[fp], -s.rod).correlation
        q5 = s[s[fp] >= s[fp].quantile(0.8)]
        print(f"   {fp:<11} IC {ic:+.3f} | top-quintile short capture {-q5.rod.mean()*100:+.3f}% (base {-s.rod.mean()*100:+.3f}%)")

    print("\n(2) LIFT — SHORT basket capture on DOWN-tape (rank by ...), N=%d:" % N)
    def basket(rankcol):
        caps = []
        for dt, g in dn.groupby("date"):
            if len(g) < 15 or g[rankcol].isna().all():
                continue
            caps.append(-g.nlargest(N, rankcol).rod.mean() * 100)
        caps = np.array(caps)
        return f"mean {caps.mean():+.3f}%  %pos {(caps>0).mean()*100:.0f}%  n={len(caps)}"
    print(f"   Phase-1 only (p_short)     : {basket('p_short')}")
    print(f"   Footprint only (short_fp)  : {basket('short_fp')}")
    print(f"   short_cont only            : {basket('short_cont')}")
    print(f"   short_rev only             : {basket('short_rev')}")
    # combined: walk-forward logistic p_short + footprints
    dn = dn.sort_values("date"); dn["ym"] = dn.date.str[:7]
    F = ["p_short", "short_cont", "short_rev"]
    dn2 = dn.dropna(subset=F).copy(); dn2["y"] = (dn2.rod <= -0.005).astype(int)
    dn2["p_combo"] = np.nan
    months = sorted(dn2.ym.unique())
    for i, ym in enumerate(months):
        if i < 4: continue
        tr = dn2[dn2.ym < ym]; te = dn2.index[dn2.ym == ym]
        if len(tr) < 2000 or len(te) == 0: continue
        mdl = LogisticRegression(max_iter=200).fit(tr[F].values, tr.y.values)
        dn2.loc[te, "p_combo"] = mdl.predict_proba(dn2.loc[te, F].values)[:, 1]
    caps = []
    for dt, g in dn2.dropna(subset=["p_combo"]).groupby("date"):
        if len(g) < 15: continue
        caps.append(-g.nlargest(N, "p_combo").rod.mean() * 100)
    caps = np.array(caps)
    print(f"   COMBO (p_short + footprint): mean {caps.mean():+.3f}%  %pos {(caps>0).mean()*100:.0f}%  n={len(caps)}")


if __name__ == "__main__":
    main()
