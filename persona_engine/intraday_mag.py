"""
Intraday MAGNITUDE test: does '+/-X%' mean the stock MOVES X% either direction
(a volatility/'will-it-be-active' call) rather than a directional call?
Rank top-5 by P(|10:00->15:15 move| >= X); measure how many of 5 actually moved >=X%
(either way). Targets: 4/5@1%, 3/5@2%, 2/5@3%. Reuses intraday v2 features + stack.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from persona_engine import db, universe
from persona_engine.intraday_eod_v2 import build, stack_predict

NPICK = 5
TARGETS = {1.0: 4, 2.0: 3, 3.0: 2}


def run(con, fo):
    df, FEATS = build(con, fo)
    X = df[FEATS].replace([np.inf, -np.inf], np.nan)
    folds = [("2025-07-01", "2025-07-01", "2025-12-31"), ("2026-01-01", "2026-01-01", "2099")]
    print(f"INTRADAY MAGNITUDE (|move| either way, 10:00->15:15) | rows={len(df)}\n")
    for Xt, need in TARGETS.items():
        lab = (df["post_move"].abs() >= Xt).astype(int).values
        p5, dT, base = [], [], []
        for tr_end, lo, hi in folds:
            tr = (df["date"] < tr_end).values
            te = ((df["date"] >= lo) & (df["date"] <= hi)).values
            if tr.sum() < 3000 or te.sum() < 300:
                continue
            p = stack_predict(X, lab[tr], tr, te)
            d = df.loc[te, ["date"]].copy(); d["p"] = p; d["succ"] = lab[te]
            hits = [gg.sort_values("p", ascending=False).head(NPICK)["succ"].sum()
                    for _, gg in d.groupby("date")]
            p5.append(np.mean(hits)); dT.append(np.mean([h >= need for h in hits])*100)
            base.append(lab[te].mean()*100)
        mp = np.mean(p5); lift = (mp/5*100)/np.mean(base)
        flag = "  *** TARGET MET ***" if mp >= need else ""
        print(f"  |move|>={Xt:.0f}% (need {need}/5): p@5={mp:.2f} ({mp/5*100:.0f}%)  "
              f"base={np.mean(base):.1f}%  lift={lift:.2f}x  days>={need}of5={np.mean(dT):.0f}%{flag}", flush=True)


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("INTRADAY_MAG_DONE")
