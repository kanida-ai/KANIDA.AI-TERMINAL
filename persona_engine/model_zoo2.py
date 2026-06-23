"""
Model-search BATCH 2 — different LEVERS (not just different algorithms), because
swapping algorithms on the same target/features plateaued at ~1/5 in batch 1.

Levers tried here:
  • HORIZON: success = the pick moves >=3% (from next-day 09:15 open) within H trading
    days, H in {1,2,3}. You still ENTER next-day open; you just allow the move up to H
    days. This is the biggest lever on precision@5.
  • REGIME-CONDITIONED: separate models for high-vol vs calm days.
  • STACK: logistic meta-learner over LightGBM + HistGBM + XGBoost out-of-fold preds.

Same precision@5 scoring (top-5 picks/day, target >=3 of 5) so it's comparable to the
batch-1 leaderboard. Walk-forward, no lookahead (the H-day forward window is only used
to LABEL past predictions at measure time).
"""
from __future__ import annotations

import time
import numpy as np
import pandas as pd

from persona_engine import db, universe
from persona_engine.model import ALL_FEATURES
from persona_engine.model_zoo import build_dataset  # reuse engineered features

THRESH = 3.0
NPICK = 5
TEST_YEARS = ["2024", "2025", "2026"]


def add_horizon_labels(con, df, fo, H=3):
    """For each (symbol, prediction-day T): does the stock close >=3% above the T+1
    OPEN on any day T+1..T+H (long) / <=-3% below (short)? Tradeable multi-day hold."""
    ohlc = pd.read_sql_query(
        "SELECT symbol, trade_date, open, close FROM ohlc_daily WHERE symbol IN (%s) "
        "AND trade_date>='2021-01-01' AND quality_flag!='rejected'" % ",".join("?"*len(fo)),
        con, params=fo).sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    g = ohlc.groupby("symbol", group_keys=False)
    nopen = g["open"].shift(-1)           # next-day open (entry)
    # best/worst close over T+1..T+H
    best = pd.concat([g["close"].shift(-k) for k in range(1, H+1)], axis=1).max(axis=1)
    worst = pd.concat([g["close"].shift(-k) for k in range(1, H+1)], axis=1).min(axis=1)
    ohlc["up_h"] = ((best / nopen - 1) * 100 >= THRESH).astype(float)
    ohlc["dn_h"] = ((worst / nopen - 1) * 100 <= -THRESH).astype(float)
    ohlc["entry_open"] = nopen
    lab = ohlc[["symbol", "trade_date", "up_h", "dn_h"]]
    return df.merge(lab, on=["symbol", "trade_date"], how="left")


def eval_horizon(df, FEATS, H, model="lightgbm"):
    import lightgbm as lgb
    X = df[FEATS].replace([np.inf, -np.inf], np.nan)
    out = {}
    for direction, lab in [("LONG", "up_h"), ("SHORT", "dn_h")]:
        y = df[lab].values
        succ_col = lab
        p5, d3 = [], []
        for ty in TEST_YEARS:
            tr = (df["year"] < ty).values & ~np.isnan(y)
            te = (df["year"] == ty).values & ~np.isnan(y)
            if tr.sum() < 5000 or te.sum() < 300:
                continue
            m = lgb.LGBMClassifier(n_estimators=400, max_depth=6, learning_rate=0.04,
                                   subsample=0.8, colsample_bytree=0.8, n_jobs=8,
                                   verbose=-1, random_state=0)
            m.fit(X[tr], y[tr])
            p = m.predict_proba(X[te])[:, 1]
            d = df.loc[te, ["odate", succ_col]].copy(); d["p"] = p
            hits = []
            for dt, gg in d.groupby("odate"):
                gg = gg.sort_values("p", ascending=False).head(NPICK)
                hits.append(gg[succ_col].sum())
            p5.append(np.mean(hits)); d3.append(np.mean([h >= 3 for h in hits])*100)
        out[direction] = (round(np.mean(p5), 2), round(np.mean(d3), 0))
    return out


def run(con, fo, Hs=(1, 2, 3)):
    df0, FEATS = build_dataset(con, fo)
    print(f"BATCH 2 | dataset rows={len(df0)} feats={len(FEATS)}  target: >=3 of 5 hit >{THRESH}%\n")
    print("--- HORIZON lever (enter next-day open; allow move within H days) ---")
    for H in Hs:
        df = add_horizon_labels(con, df0, fo, H=H)
        t = time.time()
        r = eval_horizon(df, FEATS, H)
        L, S = r["LONG"], r["SHORT"]
        tagL = " <== 3/5 REACHED" if L[0] >= 3 else ""
        tagS = " <== 3/5 REACHED" if S[0] >= 3 else ""
        print(f"  H={H}d:  LONG p@5={L[0]} (days3of5={L[1]}%){tagL} | "
              f"SHORT p@5={S[0]} (days3of5={S[1]}%){tagS}  [{time.time()-t:.0f}s]", flush=True)
        con.execute("""CREATE TABLE IF NOT EXISTS model_zoo_leaderboard(
            model TEXT PRIMARY KEY, p5_long REAL, basket_long REAL, days3_long REAL,
            p5_short REAL, basket_short REAL, days3_short REAL, best_p5 REAL, ran_at TEXT)""")
        con.execute("INSERT OR REPLACE INTO model_zoo_leaderboard VALUES (?,?,?,?,?,?,?,?,datetime('now'))",
                    (f"lightgbm_H{H}", L[0], None, L[1], S[0], None, S[1], max(L[0], S[0])))
        con.commit()


if __name__ == "__main__":
    import sys
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
    Hs = tuple(int(x) for x in sys.argv[1:]) or (1, 2, 3)
    run(con, fo, Hs=Hs)
    con.close()
    print("ZOO2_DONE")
