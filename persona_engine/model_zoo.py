"""
CONTINUOUS MODEL-SEARCH harness (operator 2026-06-22: "try every model one by one
until the goal is met; mine continuously; deploy different AI technologies").

Goal per side per day: of the 5 top-confidence picks, >=3 close >3% (open->close) AND
the 5 average >=2% in direction. We rank models by precision@5 (avg hits of 5) and
basket avg, walk-forward, and append every result to a leaderboard (model_zoo_leaderboard).
Re-runnable / extensible — add configs and run again; the leaderboard accumulates.

Model families tried: HistGBM, RandomForest, ExtraTrees, XGBoost, LightGBM,
LogisticRegression(elasticnet), MLP neural net, kNN, plus class-imbalance handling and
calibration. Tree models use NaN natively; others get median-impute + scaling pipelines.
"""
from __future__ import annotations

import time
import numpy as np
import pandas as pd

from persona_engine import db, universe
from persona_engine.model import ALL_FEATURES

THRESH = 3.0
BASKET_TGT = 2.0
NPICK = 5
TEST_YEARS = ["2024", "2025", "2026"]
SUBSAMPLE_SLOW = 180_000


def build_dataset(con, fo):
    feats = pd.read_sql_query(
        "SELECT * FROM persona_signal_features WHERE symbol IN (%s) AND trade_date>='2021-01-01'"
        % ",".join("?"*len(fo)), con, params=fo)
    ev = pd.read_sql_query(
        "SELECT symbol,trade_date,earn_next1,earn_recent2,deliv_pct,deliv_z20,accum "
        "FROM persona_event_features WHERE symbol IN (%s)" % ",".join("?"*len(fo)), con, params=fo)
    feats = feats.merge(ev, on=["symbol", "trade_date"], how="left")
    # ---- expanded feature engineering (more patterns) ----
    feats = feats.sort_values(["symbol", "trade_date"])
    g = feats.groupby("symbol", group_keys=False)
    for lag in (1, 2, 3, 5):
        feats[f"sigret_l{lag}"] = g["sig_ret_pct"].shift(lag)
    for w in (5, 10):
        feats[f"retstd_{w}"] = g["sig_ret_pct"].transform(lambda s: s.rolling(w).std())
        feats[f"rsi_chg_{w}"] = g["rsi_14"].transform(lambda s: s - s.shift(w))
    feats["roc5_over_atr"] = feats["roc_5"] / feats["atr_20_pct"].replace(0, np.nan)
    feats["dist_high_x_vol"] = feats["dist_high_20"] * feats["vol_ratio_20d"]
    op = pd.read_sql_query(
        "SELECT symbol,trade_date,oc_full FROM persona_open_features WHERE symbol IN (%s)"
        % ",".join("?"*len(fo)), con, params=fo)
    cal = sorted(feats["trade_date"].unique())
    nxt = {cal[i]: cal[i+1] for i in range(len(cal)-1)}
    feats["odate"] = feats["trade_date"].map(nxt)
    df = feats.merge(op.rename(columns={"trade_date": "odate"}), on=["symbol", "odate"], how="inner")
    df = df.dropna(subset=["oc_full"]).reset_index(drop=True)
    df["year"] = df["odate"].str[:4]
    eng = [c for c in df.columns if c.startswith(("sigret_l", "retstd_", "rsi_chg_"))] + \
          ["roc5_over_atr", "dist_high_x_vol"]
    FEATS = [f for f in ALL_FEATURES if f in df.columns] + \
            [f for f in ["earn_next1", "earn_recent2", "deliv_pct", "deliv_z20", "accum"] if f in df.columns] + \
            eng
    return df, FEATS


# ---- model factory ----
def make_model(name):
    from sklearn.ensemble import (HistGradientBoostingClassifier, RandomForestClassifier,
                                  ExtraTreesClassifier)
    from sklearn.linear_model import LogisticRegression
    from sklearn.neural_network import MLPClassifier
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import StandardScaler
    import xgboost as xgb
    import lightgbm as lgb

    def pipe(est):
        return Pipeline([("imp", SimpleImputer(strategy="median")),
                         ("sc", StandardScaler()), ("est", est)])

    nat = True  # handles NaN natively
    if name == "histgbm":
        return HistGradientBoostingClassifier(max_iter=200, max_depth=4, learning_rate=0.06,
                                              l2_regularization=1.0, random_state=0), nat, False
    if name == "histgbm_deep":
        return HistGradientBoostingClassifier(max_iter=400, max_depth=6, learning_rate=0.03,
                                              l2_regularization=2.0, random_state=0), nat, False
    if name == "histgbm_balanced":
        return HistGradientBoostingClassifier(max_iter=250, max_depth=5, learning_rate=0.05,
                                              l2_regularization=1.0, class_weight="balanced",
                                              random_state=0), nat, False
    if name == "xgboost":
        return xgb.XGBClassifier(n_estimators=400, max_depth=5, learning_rate=0.04,
                                 subsample=0.8, colsample_bytree=0.8, eval_metric="logloss",
                                 tree_method="hist", n_jobs=8, random_state=0), nat, True
    if name == "xgboost_imb":
        return xgb.XGBClassifier(n_estimators=500, max_depth=6, learning_rate=0.03,
                                 subsample=0.8, colsample_bytree=0.7, eval_metric="logloss",
                                 tree_method="hist", n_jobs=8, random_state=0), nat, "spw"
    if name == "lightgbm":
        return lgb.LGBMClassifier(n_estimators=400, max_depth=6, learning_rate=0.04,
                                  subsample=0.8, colsample_bytree=0.8, n_jobs=8,
                                  verbose=-1, random_state=0), nat, False
    if name == "lightgbm_imb":
        return lgb.LGBMClassifier(n_estimators=500, num_leaves=63, learning_rate=0.03,
                                  subsample=0.8, colsample_bytree=0.7, is_unbalance=True,
                                  n_jobs=8, verbose=-1, random_state=0), nat, False
    if name == "randomforest":
        return RandomForestClassifier(n_estimators=300, max_depth=12, min_samples_leaf=20,
                                      class_weight="balanced", n_jobs=8, random_state=0), False, "sub"
    if name == "extratrees":
        return ExtraTreesClassifier(n_estimators=300, max_depth=14, min_samples_leaf=20,
                                    class_weight="balanced", n_jobs=8, random_state=0), False, "sub"
    if name == "logistic":
        return pipe(LogisticRegression(penalty="elasticnet", l1_ratio=0.3, C=0.5,
                                       solver="saga", max_iter=300, class_weight="balanced")), False, "sub"
    if name == "mlp":
        return pipe(MLPClassifier(hidden_layer_sizes=(64, 32), alpha=1e-3, max_iter=60,
                                  early_stopping=True, random_state=0)), False, "sub"
    if name == "knn":
        return pipe(KNeighborsClassifier(n_neighbors=200, weights="distance", n_jobs=8)), False, "sub"
    raise ValueError(name)


def evaluate(df, FEATS, name):
    X = df[FEATS].replace([np.inf, -np.inf], np.nan)
    res = {}
    for direction, sign in [("LONG", 1), ("SHORT", -1)]:
        lab = ((df["oc_full"] >= THRESH) if sign == 1 else (df["oc_full"] <= -THRESH)).astype(int).values
        p5s, baskets, d3 = [], [], []
        for ty in TEST_YEARS:
            tr = (df["year"] < ty).values
            te = (df["year"] == ty).values
            if tr.sum() < 5000 or te.sum() < 300:
                continue
            est, native, mode = make_model(name)
            rows = np.where(tr)[0]
            if mode == "sub" and len(rows) > SUBSAMPLE_SLOW:
                rng = np.random.default_rng(0)
                rows = rng.choice(rows, SUBSAMPLE_SLOW, replace=False)
            Xtr = X.iloc[rows]
            ytr = lab[rows]
            fit_kw = {}
            if mode == "spw":
                pos = ytr.sum(); neg = len(ytr)-pos
                est.set_params(scale_pos_weight=max(1.0, neg/max(1, pos)))
            if not native:
                Xtr = Xtr.fillna(Xtr.median(numeric_only=True))
            est.fit(Xtr, ytr, **fit_kw)
            Xte = X.iloc[np.where(te)[0]]
            if not native:
                Xte = Xte.fillna(Xtr.median(numeric_only=True))
            p = est.predict_proba(Xte)[:, 1]
            d = df.loc[te, ["odate", "oc_full"]].copy(); d["p"] = p
            d["succ"] = ((d["oc_full"] >= THRESH) if sign == 1 else (d["oc_full"] <= -THRESH)).astype(int)
            hits, bk, n3 = [], [], 0
            for dt, gg in d.groupby("odate"):
                gg = gg.sort_values("p", ascending=False).head(NPICK)
                h = gg["succ"].sum(); b = gg["oc_full"].mean()
                hits.append(h); bk.append(b)
                if h >= 3: n3 += 1
            p5s.append(np.mean(hits)); baskets.append(np.mean(bk)); d3.append(n3/len(hits)*100)
        res[direction] = (round(np.mean(p5s), 2), round(np.mean(baskets), 2), round(np.mean(d3), 0))
    return res


def run(con, fo, models, verbose=True):
    con.execute("""CREATE TABLE IF NOT EXISTS model_zoo_leaderboard(
        model TEXT, p5_long REAL, basket_long REAL, days3_long REAL,
        p5_short REAL, basket_short REAL, days3_short REAL, best_p5 REAL,
        ran_at TEXT DEFAULT (datetime('now')), PRIMARY KEY(model))""")
    df, FEATS = build_dataset(con, fo)
    if verbose:
        print(f"dataset rows={len(df)} feats={len(FEATS)} | target >=3/5 hit >{THRESH}% & basket>={BASKET_TGT}%\n")
    for name in models:
        t = time.time()
        try:
            r = evaluate(df, FEATS, name)
        except Exception as e:
            print(f"  {name:16s} FAILED: {e}", flush=True)
            continue
        L, S = r["LONG"], r["SHORT"]
        best = max(L[0], S[0])
        con.execute("INSERT OR REPLACE INTO model_zoo_leaderboard VALUES (?,?,?,?,?,?,?,?,datetime('now'))",
                    (name, L[0], L[1], L[2], S[0], S[1], S[2], best))
        con.commit()
        if verbose:
            print(f"  {name:16s} LONG p@5={L[0]} basket={L[1]}% d3of5={L[2]}% | "
                  f"SHORT p@5={S[0]} basket={S[1]}% d3of5={S[2]}%  [{time.time()-t:.0f}s]", flush=True)
    return True


if __name__ == "__main__":
    import sys
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
    batch = sys.argv[1:] or ["histgbm", "histgbm_deep", "histgbm_balanced", "xgboost",
                             "xgboost_imb", "lightgbm", "lightgbm_imb", "randomforest",
                             "extratrees", "logistic", "mlp"]
    run(con, fo, batch, verbose=True)
    print("\n=== LEADERBOARD (by best precision@5; target=3.0/5) ===")
    for r in con.execute("SELECT model,p5_long,basket_long,p5_short,basket_short,best_p5 "
                         "FROM model_zoo_leaderboard ORDER BY best_p5 DESC"):
        print(f"  {r[0]:16s} L:{r[1]}/5 ({r[2]}%)  S:{r[3]}/5 ({r[4]}%)  best={r[5]}/5")
    con.close()
    print("ZOO_DONE")
