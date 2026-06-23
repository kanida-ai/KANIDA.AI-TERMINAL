"""
Day-SELECTIVITY: the |move|>=1% magnitude model already hits 4/5 on ~30% of days.
Can we know those days in advance? Rank each day by the model's CONVICTION in its 5th
pick (min prob among the top-5). On the most-confident K% of days, what is precision@5?
If precision@5 >= 4 on a meaningful coverage, the easy target IS achievable selectively.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from persona_engine import db, universe
from persona_engine.intraday_eod_v2 import build, stack_predict

NPICK = 5


def run(con, fo, Xt=1.0):
    df, FEATS = build(con, fo)
    X = df[FEATS].replace([np.inf, -np.inf], np.nan)
    folds = [("2025-07-01", "2025-07-01", "2025-12-31"), ("2026-01-01", "2026-01-01", "2099")]
    rows = []
    for direction, sign in [("LONG", 1), ("SHORT", -1), ("MAG", 0)]:
        if sign == 1:
            lab = (df["post_move"] >= Xt).astype(int).values
        elif sign == -1:
            lab = (df["post_move"] <= -Xt).astype(int).values
        else:
            lab = (df["post_move"].abs() >= Xt).astype(int).values
        days = []
        for tr_end, lo, hi in folds:
            tr = (df["date"] < tr_end).values
            te = ((df["date"] >= lo) & (df["date"] <= hi)).values
            if tr.sum() < 3000 or te.sum() < 300:
                continue
            p = stack_predict(X, lab[tr], tr, te)
            d = df.loc[te, ["date"]].copy(); d["p"] = p; d["succ"] = lab[te]
            for dt, g in d.groupby("date"):
                top = g.sort_values("p", ascending=False).head(NPICK)
                days.append((top["p"].iloc[-1], top["succ"].sum()))   # 5th-pick conviction, hits
        dd = pd.DataFrame(days, columns=["conv", "hits"]).sort_values("conv", ascending=False)
        n = len(dd)
        for covpct in (100, 50, 30, 20, 10):
            k = max(1, int(n*covpct/100))
            sub = dd.head(k)
            rows.append((direction, covpct, round(sub["hits"].mean(), 2),
                         round((sub["hits"] >= 4).mean()*100, 0), k))
    print(f"DAY-SELECTIVITY @ |move|/move >= {Xt:.0f}%  (act only on most-confident days)\n")
    print("dir   coverage  avg_hits/5  %days>=4of5  ndays")
    for r in rows:
        flag = "  <== 4/5 avg" if r[2] >= 4 else ""
        print(f"  {r[0]:4s}  top-{r[1]:>3d}%   {r[2]:>5}      {r[3]:>4}%       {r[4]}{flag}", flush=True)


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo, Xt=1.0)
    con.close()
    print("SELECTIVE_DONE")
