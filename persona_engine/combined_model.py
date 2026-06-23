"""
COMBINED multi-ML directional system — synthesis of ALL operator insights.

Insights folded in:
 - intraday, no carryover: observe 9:15-10:00, enter 10:00, exit 15:15
 - directional: LONG -> up_touch>=+1% ; SHORT -> dn_touch<=-1%  (top-5/side)
 - morning microstructure + volatility (movers)
 - REVERSAL + interaction features (gainer-fade / loser-bounce, stock x market)
 - market & sector regime (dominant driver)
 - multi-ML: stacked LightGBM + XGBoost + HistGBM + MLP -> logistic meta-learner
Walk-forward; reports precision@5 by year + overall. Target 4.5/5 @ +/-1%.
"""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db, universe
from persona_engine.intraday_eod import morning_features
from persona_engine.touch_tradelog import touch_window
from persona_engine.model import ALL_FEATURES

NPICK = 5
FOLDS = [("2025-07-01", "2025-07-01", "2025-12-31"), ("2026-01-01", "2026-01-01", "2099")]


def build(con, fo):
    mf = morning_features(con, fo)
    sect = {r["symbol"]: (r["sector"] or "UNK") for r in con.execute("SELECT symbol,sector FROM falcon_sectors")}
    mf["sector"] = mf["symbol"].map(sect).fillna("UNK")
    g = mf.groupby("date")["m_ret"]
    mf["mkt_m_ret"] = g.transform("mean"); mf["mkt_breadth"] = g.transform(lambda s: (s > 0).mean())
    mf["mkt_disp"] = g.transform("std")
    mf["rel_mkt"] = mf["m_ret"] - mf["mkt_m_ret"]
    mf["rel_sec"] = mf["m_ret"] - mf.groupby(["date", "sector"])["m_ret"].transform("mean")
    # interaction / reversal / setup features
    mf["mret_x_mkt"] = mf["m_ret"] * mf["mkt_m_ret"]            # move with/against market
    mf["mret_x_breadth"] = mf["m_ret"] * (mf["mkt_breadth"]-0.5)
    mf["rev_signal"] = -mf["m_ret"]                            # reversal lean
    mf["near_high"] = mf["m_loc"]; mf["vwap_pos"] = mf["m_vwap_dev"]
    mf["abs_mret"] = mf["m_ret"].abs()
    # prior-day daily context
    daily = pd.read_sql_query("SELECT * FROM persona_signal_features WHERE symbol IN (%s)"
                              % ",".join("?"*len(fo)), con, params=fo).sort_values(["symbol", "trade_date"])
    daily["date"] = daily.groupby("symbol")["trade_date"].shift(-1)
    dcols = [f for f in ALL_FEATURES if f in daily.columns]
    mf = mf.merge(daily[["symbol", "date"]+dcols], on=["symbol", "date"], how="left")
    # outcomes
    tw = touch_window(con, fo)
    tw["up_touch"] = (tw["hi"]/tw["entry"]-1)*100
    tw["dn_touch"] = (tw["lo"]/tw["entry"]-1)*100
    d = mf.merge(tw[["symbol", "date", "up_touch", "dn_touch"]], on=["symbol", "date"], how="inner")
    d = d.dropna(subset=["up_touch", "m_volat"]).reset_index(drop=True)
    MF = ["m_ret", "m_range", "m_loc", "m_vol", "m_volat", "m_vwap_dev", "m_upbars", "m_last15",
          "mkt_m_ret", "mkt_breadth", "mkt_disp", "rel_mkt", "rel_sec", "mret_x_mkt",
          "mret_x_breadth", "rev_signal", "near_high", "vwap_pos", "abs_mret"]
    FEATS = MF + dcols
    return d, FEATS


def stack(Xtr, ytr, Xte):
    import lightgbm as lgb, xgboost as xgb
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.neural_network import MLPClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    # time-based meta split: last 25% of train for meta
    n = len(Xtr); cut = int(n*0.75)
    base_tr, meta_tr = slice(0, cut), slice(cut, n)
    bases = {
        "lgb": lgb.LGBMClassifier(n_estimators=350, max_depth=6, learning_rate=0.04, subsample=0.8,
                                  colsample_bytree=0.8, n_jobs=8, verbose=-1, random_state=0),
        "xgb": xgb.XGBClassifier(n_estimators=350, max_depth=6, learning_rate=0.04, subsample=0.8,
                                 colsample_bytree=0.8, eval_metric="logloss", tree_method="hist",
                                 n_jobs=8, random_state=0),
        "hgb": HistGradientBoostingClassifier(max_iter=300, max_depth=6, learning_rate=0.05,
                                              l2_regularization=1.0, random_state=0),
        "mlp": Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler()),
                         ("m", MLPClassifier(hidden_layer_sizes=(64, 32), alpha=1e-3, max_iter=60,
                                             early_stopping=True, random_state=0))]),
    }
    meta_X, te_X = [], []
    for name, est in bases.items():
        est.fit(Xtr.iloc[base_tr], ytr[base_tr])
        meta_X.append(est.predict_proba(Xtr.iloc[meta_tr])[:, 1])
        te_X.append(est.predict_proba(Xte)[:, 1])
    meta = LogisticRegression(max_iter=200)
    meta.fit(np.column_stack(meta_X), ytr[meta_tr])
    return meta.predict_proba(np.column_stack(te_X))[:, 1]


def run(con, fo):
    d, FEATS = build(con, fo)
    X = d[FEATS].replace([np.inf, -np.inf], np.nan)
    print(f"rows={len(d)} feats={len(FEATS)} dates={d['date'].nunique()}\n")
    for direction in ("LONG", "SHORT"):
        col = "up_touch" if direction == "LONG" else "dn_touch"
        lab = ((d[col] >= 1) if direction == "LONG" else (d[col] <= -1)).astype(int).values
        p5_by = {}
        allhits = []
        for tr_end, lo, hi in FOLDS:
            tr = (d["date"] < tr_end).values; te = ((d["date"] >= lo) & (d["date"] <= hi)).values
            if tr.sum() < 3000 or te.sum() < 300: continue
            p = stack(X[tr], lab[tr], X[te])
            g = d.loc[te, ["date", col]].copy(); g["p"] = p
            for dt, gg in g.groupby("date"):
                pk = gg.sort_values("p", ascending=False).head(NPICK)
                h = (pk[col] >= 1).sum() if direction == "LONG" else (pk[col] <= -1).sum()
                allhits.append((dt[:4], h))
        df = pd.DataFrame(allhits, columns=["yr", "h"])
        overall = df["h"].mean()
        byyr = df.groupby("yr")["h"].mean().round(2).to_dict()
        print(f"{direction} (+/-1% directional): COMBINED multi-ML p@5 = {overall:.2f}/5   by year={byyr}")


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("COMBINED_DONE")
