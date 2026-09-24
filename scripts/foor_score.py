"""Cache the walk-forward Falcon scores + Layer-1 (first-3-min) market inputs, so the
two-layer analysis doesn't refit the logistic every run. Writes _foor_scored.pkl."""
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
    d["drive18"] = d.C18 / d.o0915 - 1          # first-3-min per-stock move (Layer 1 input)
    d["v18"] = d.V18                            # first-3-min volume (Layer 1 input)
    d = d.dropna(subset=FEATS + ["rod", "drive18", "v18"])
    d["ym"] = d.date.str[:7]
    d["y_long"] = (d.rod >= TGT).astype(int); d["y_short"] = (d.rod <= -TGT).astype(int)
    months = sorted(d.ym.unique())
    d["p_long"] = np.nan; d["p_short"] = np.nan
    for i, ym in enumerate(months):
        if i < 3:
            continue
        tr = d[d.ym < ym]; te = d.index[d.ym == ym]
        if len(tr) < 5000 or len(te) == 0:
            continue
        Xtr, Xte = tr[FEATS].values, d.loc[te, FEATS].values
        for tgt, col in [("y_long", "p_long"), ("y_short", "p_short")]:
            m = LogisticRegression(max_iter=200).fit(Xtr, tr[tgt].values)
            d.loc[te, col] = m.predict_proba(Xte)[:, 1]
    keep = d.dropna(subset=["p_long", "p_short"])[
        ["symbol", "date", "ym", "C20", "rod", "postH", "postL", "eod", "prev_C",
         "p_long", "p_short", "drive18", "v18"]].copy()
    pickle.dump(keep, open(ROOT / "docs" / "ops" / "_foor_scored.pkl", "wb"), protocol=4)
    print(f"[score] cached {len(keep):,} stock-days | {keep.ym.nunique()} months | {keep.date.min()}..{keep.date.max()}")


if __name__ == "__main__":
    main()
