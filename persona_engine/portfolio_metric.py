"""
PORTFOLIO metric: instead of 'how many of 5 individually hit +1%', measure the
equal-weight 5-stock BASKET return (10:00 entry -> 15:15 close). For LONG the basket
profit = mean(ret_close of 5 longs); for SHORT = -mean(ret_close of 5 shorts).
Report: avg basket return/day, % days basket >= +1/2/3%, win rate, cumulative.
Both the multi-ML Top-5 and the simple volatility-screen Top-5, walk-forward 2025/2026.
"""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db
from persona_engine.combined_model import stack
from persona_engine.block_experiments import feats, stage1_score, NCAND, NPICK


def basket_eval(test, direction, FE, train):
    col = "up_touch" if direction == "LONG" else "dn_touch"
    lab = ((train[col] >= 1) if direction == "LONG" else (train[col] <= -1)).astype(int).values
    p = stack(train[FE].replace([np.inf, -np.inf], np.nan), lab,
              test[FE].replace([np.inf, -np.inf], np.nan))
    t = test.copy(); t["p"] = p
    model_bask, screen_bask = [], []
    for dt, g in t.groupby("date"):
        if len(g) < 25:
            continue
        g = g.copy(); g["s1"] = stage1_score(g, direction)
        cand = g[g["s1"] <= NCAND]
        # model top-5
        pkm = cand.sort_values("p", ascending=False).head(NPICK)
        # simple volatility-screen top-5 (no model)
        pks = g.sort_values("morning_vol", ascending=False).head(NPICK)
        sign = 1 if direction == "LONG" else -1
        model_bask.append(sign*pkm["ret_close"].mean())
        screen_bask.append(sign*pks["ret_close"].mean())
    return np.array(model_bask), np.array(screen_bask)


def report(name, b):
    b = b[~np.isnan(b)]
    print(f"  {name:16s}: avg {b.mean():+.3f}%/day | win {np.mean(b>0)*100:.0f}% | "
          f">=+1% {np.mean(b>=1)*100:.0f}%  >=+2% {np.mean(b>=2)*100:.0f}%  >=+3% {np.mean(b>=3)*100:.0f}% "
          f"| cumulative {b.sum():+.1f}% over {len(b)} days")


def run():
    con = db.connect(read_only=True)
    df = pd.read_sql_query("SELECT * FROM block_features", con); con.close()
    df["year"] = df["date"].str[:4]; FE = feats(df)
    for direction in ("LONG", "SHORT"):
        print(f"================= {direction} basket (equal-weight Top-5, 10:00->15:15) =================")
        for ty in ("2025", "2026"):
            tr = df[df["year"] < ty]; te = df[df["year"] == ty]
            if len(tr) < 3000 or len(te) < 300:
                continue
            mb, sb = basket_eval(te, direction, FE, tr)
            print(f" {ty}:")
            report("multi-ML Top-5", mb)
            report("volatility Top-5", sb)
        print()


if __name__ == "__main__":
    run()
    print("PORT_DONE")
