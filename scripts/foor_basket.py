"""Phase 1 go/no-go, part 2: the COMBINED Falcon score (walk-forward logistic) ranked into
top-N baskets. Tests whether the ranked LONG basket and SHORT basket (SEPARATELY) capture
the 0.5% rest-of-day target. Walk-forward: logistic refit monthly on trailing data only.
"""
import pickle
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
import foor_signal_power as SP

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
TGT = 0.005
FEATS = ["drive", "or_pos", "gap", "z_contraction", "z_dryup",
         "sweep_rej_up", "sweep_rej_dn", "brk_up", "brk_dn"]


def main():
    df = pickle.load(open(ROOT / "docs" / "ops" / "_foor_panel.pkl", "rb"))
    d = SP.build(df, "H20", "L20", "V20", "C20").copy()
    d = d.dropna(subset=FEATS + ["rod"])
    d["ym"] = d.date.str[:7]
    d["y_long"] = (d.rod >= TGT).astype(int)
    d["y_short"] = (d.rod <= -TGT).astype(int)
    months = sorted(d.ym.unique())
    d["p_long"] = np.nan; d["p_short"] = np.nan
    # walk-forward: for each month, train on ALL strictly-earlier data (>=3 months history)
    for i, ym in enumerate(months):
        if i < 3:
            continue
        tr = d[d.ym < ym]; te_idx = d.index[d.ym == ym]
        if len(tr) < 5000 or len(te_idx) == 0:
            continue
        Xtr = tr[FEATS].values; Xte = d.loc[te_idx, FEATS].values
        for tgt, col in [("y_long", "p_long"), ("y_short", "p_short")]:
            m = LogisticRegression(max_iter=200, C=1.0)
            m.fit(Xtr, tr[tgt].values)
            d.loc[te_idx, col] = m.predict_proba(Xte)[:, 1]
    d = d.dropna(subset=["p_long", "p_short"])
    print(f"walk-forward scored: {len(d):,} stock-days over {d.ym.nunique()} months\n")

    def basket(side, proba, is_short):
        print(f"=== {side} BASKET (rank by Falcon score = {proba}); capture = {'-rod' if is_short else '+rod'} ===")
        print(f"{'N':>4}{'days':>7}{'mean cap%':>11}{'P(cap>=0.5%)':>14}{'P(cap>0)':>10}{'avg MFE%':>10}")
        for N in (3, 5, 10):
            caps, mfes, hit, pos = [], [], 0, 0
            for dt, g in d.groupby("date"):
                if len(g) < 20:
                    continue
                top = g.nlargest(N, proba)
                cap = (-1 if is_short else 1) * top.rod.mean() * 100      # realized rest-of-day capture
                mfe = ((1 - top.postL / top.C20) if is_short else (top.postH / top.C20 - 1)).mean() * 100
                caps.append(cap); mfes.append(mfe)
                hit += cap >= 0.5; pos += cap > 0
            caps = np.array(caps)
            print(f"{N:>4}{len(caps):>7}{caps.mean():>11.3f}{hit/len(caps)*100:>13.1f}%{pos/len(caps)*100:>9.1f}%{np.mean(mfes):>10.3f}")
        print()
    basket("LONG", "p_long", False)
    basket("SHORT", "p_short", True)
    basket("CONTRA-LONG (short the up-ranked)", "p_long", True)
    # market-neutral spread: long the up-ranked + short the down-ranked (isolates signal from drift)
    print("=== MARKET-NEUTRAL spread (long p_long-top + short p_short-top) ===")
    for N in (3, 5):
        rets = []
        for dt, g in d.groupby("date"):
            if len(g) < 20:
                continue
            lt = g.nlargest(N, "p_long"); st = g.nlargest(N, "p_short")
            rets.append((lt.rod.mean() - st.rod.mean()) / 2 * 100)   # per-side capture
        rets = np.array(rets)
        print(f"  N={N}: mean {rets.mean():+.3f}%/side | %pos {(rets>0).mean()*100:.0f}%")


if __name__ == "__main__":
    main()
