"""
Model-search BATCH 3 — maximise real LIFT at locked horizons H=1 and H=3, using the
levers not yet tried: cross-sectional rank features, market-regime features, and a
stacked (soft-vote) ensemble of LightGBM+HistGBM+XGBoost.

Reports precision@5 AND lift vs base rate, so we track genuine skill, not base-rate
inflation. Walk-forward, no lookahead.
"""
from __future__ import annotations

import time
import numpy as np
import pandas as pd

from persona_engine import db, universe
from persona_engine.model import ALL_FEATURES
from persona_engine.model_zoo import build_dataset
from persona_engine.model_zoo2 import add_horizon_labels

THRESH = 3.0
NPICK = 5
TEST_YEARS = ["2024", "2025", "2026"]

XS_BASE = ["sig_ret_pct", "two_day_ret_pct", "roc_5", "roc_20", "rsi_14", "atr_20_pct",
           "vol_ratio_20d", "dist_high_20", "consol_days", "rs_index_20d", "deliv_pct"]


def add_xsec_regime(con, df, fo):
    # cross-sectional per-day ranks of key features
    for f in XS_BASE:
        if f in df.columns:
            df[f"xs_{f}"] = df.groupby("trade_date")[f].rank(pct=True)
    # market-regime features from NIFTY + universe breadth (point-in-time, by signal day)
    idx = pd.read_sql_query(
        "SELECT trade_date, close FROM ohlc_daily WHERE symbol='NIFTY50' ORDER BY trade_date", con)
    idx["nret20"] = idx["close"].pct_change(20)*100
    idx["nvol20"] = idx["close"].pct_change().rolling(20).std()*np.sqrt(252)*100
    breadth = df.groupby("trade_date")["sig_ret_pct"].apply(lambda s: (s > 0).mean()).rename("breadth").reset_index()
    medret = df.groupby("trade_date")["sig_ret_pct"].median().rename("med_univ_ret").reset_index()
    df = df.merge(idx[["trade_date", "nret20", "nvol20"]], on="trade_date", how="left")
    df = df.merge(breadth, on="trade_date", how="left").merge(medret, on="trade_date", how="left")
    xs = [c for c in df.columns if c.startswith("xs_")] + ["nret20", "nvol20", "breadth", "med_univ_ret"]
    return df, xs


def base_rate(df, H, direction):
    col = "up_h" if direction == "LONG" else "dn_h"
    m = df.dropna(subset=[col])
    m = m[m["year"].isin(TEST_YEARS)]
    return m[col].mean()*100


def eval_set(df, FEATS, H, direction, kind):
    import lightgbm as lgb
    from sklearn.ensemble import HistGradientBoostingClassifier
    import xgboost as xgb
    col = "up_h" if direction == "LONG" else "dn_h"
    X = df[FEATS].replace([np.inf, -np.inf], np.nan)
    y = df[col].values
    p5, d3 = [], []
    for ty in TEST_YEARS:
        tr = (df["year"] < ty).values & ~np.isnan(y)
        te = (df["year"] == ty).values & ~np.isnan(y)
        if tr.sum() < 5000 or te.sum() < 300:
            continue
        if kind == "stack":
            models = [
                lgb.LGBMClassifier(n_estimators=350, max_depth=6, learning_rate=0.04,
                                   subsample=0.8, colsample_bytree=0.8, n_jobs=8, verbose=-1, random_state=0),
                HistGradientBoostingClassifier(max_iter=300, max_depth=5, learning_rate=0.05,
                                               l2_regularization=1.0, random_state=0),
                xgb.XGBClassifier(n_estimators=350, max_depth=5, learning_rate=0.04, subsample=0.8,
                                  colsample_bytree=0.8, eval_metric="logloss", tree_method="hist",
                                  n_jobs=8, random_state=0)]
            ps = []
            for m in models:
                m.fit(X[tr], y[tr]); ps.append(m.predict_proba(X[te])[:, 1])
            p = np.mean(ps, axis=0)
        else:
            m = lgb.LGBMClassifier(n_estimators=400, max_depth=6, learning_rate=0.04,
                                   subsample=0.8, colsample_bytree=0.8, n_jobs=8, verbose=-1, random_state=0)
            m.fit(X[tr], y[tr]); p = m.predict_proba(X[te])[:, 1]
        d = df.loc[te, ["odate", col]].copy(); d["p"] = p
        hits = [gg.sort_values("p", ascending=False).head(NPICK)[col].sum() for _, gg in d.groupby("odate")]
        p5.append(np.mean(hits)); d3.append(np.mean([h >= 3 for h in hits])*100)
    return round(np.mean(p5), 2), round(np.mean(d3), 0)


def run(con, fo):
    df0, FEATS_base = build_dataset(con, fo)
    df0, XS = add_xsec_regime(con, df0, fo)
    FEATS_plus = FEATS_base + XS
    print(f"BATCH 3 | base feats={len(FEATS_base)} +xsec/regime={len(XS)} => plus={len(FEATS_plus)}\n")
    for H in (1, 3):
        df = add_horizon_labels(con, df0, fo, H=H)
        for direction in ("LONG", "SHORT"):
            br = base_rate(df, H, direction)
            for kind, feats, name in [("lgbm", FEATS_base, "base"),
                                      ("lgbm", FEATS_plus, "+xsec+regime"),
                                      ("stack", FEATS_plus, "stack+xsec+regime")]:
                t = time.time()
                p5, d3 = eval_set(df, feats, H, direction, kind)
                lift = p5/5*100/br if br else 0
                print(f"  H={H} {direction:5s} {name:18s}: p@5={p5} ({p5/5*100:.0f}%) "
                      f"base={br:.1f}% lift={lift:.2f}x days3of5={d3}%  [{time.time()-t:.0f}s]", flush=True)
        print()


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
    run(con, fo)
    con.close()
    print("ZOO3_DONE")
