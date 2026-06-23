"""
INTRADAY v2 — full-arsenal push at the graduated targets (operator 2026-06-23):
  4/5 hit +/-1% , 3/5 hit +/-2% , 2/5 hit +/-3%  (10:00 -> 15:15, no carryover).

New levers vs v1:
  • MARKET morning context: cross-sectional mean morning return, breadth (% up by 10:00),
    DISPERSION (cross-sectional std of morning returns -> how 'active' the day is).
  • SECTOR morning context + stock-relative-to-market / relative-to-sector features.
  • STACKED ensemble (LightGBM + HistGBM + XGBoost), probability-ranked top-5.
Reports precision@5 + % days reaching each target, both directions, X in {1,2,3}.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from persona_engine import db, universe
from persona_engine.model import ALL_FEATURES
from persona_engine.intraday_eod import morning_features

NPICK = 5
TARGETS = {1.0: 4, 2.0: 3, 3.0: 2}   # move% -> required hits of 5


def build(con, fo):
    mf = morning_features(con, fo)            # symbol,date,px1000,m_*, px1515, post_move
    sectors = {r["symbol"]: (r["sector"] or "UNK")
               for r in con.execute("SELECT symbol, sector FROM falcon_sectors")}
    mf["sector"] = mf["symbol"].map(sectors).fillna("UNK")
    # ---- market & sector MORNING context (cross-sectional, same morning) ----
    g = mf.groupby("date")["m_ret"]
    mf["mkt_m_ret"] = g.transform("mean")
    mf["mkt_breadth"] = g.transform(lambda s: (s > 0).mean())
    mf["mkt_disp"] = g.transform("std")
    mf["rel_mkt"] = mf["m_ret"] - mf["mkt_m_ret"]
    sec = mf.groupby(["date", "sector"])["m_ret"].transform("mean")
    mf["rel_sec"] = mf["m_ret"] - sec
    mf["sec_m_ret"] = sec
    # prior-day daily context
    daily = pd.read_sql_query(
        "SELECT * FROM persona_signal_features WHERE symbol IN (%s)" % ",".join("?"*len(fo)),
        con, params=fo).sort_values(["symbol", "trade_date"])
    daily["date"] = daily.groupby("symbol")["trade_date"].shift(-1)
    dcols = [f for f in ALL_FEATURES if f in daily.columns]
    df = mf.merge(daily[["symbol", "date"]+dcols], on=["symbol", "date"], how="left")
    df["ym"] = df["date"].str[:7]
    MF = ["m_ret", "m_range", "m_loc", "m_vol", "m_volat", "m_vwap_dev", "m_upbars",
          "m_last15", "mkt_m_ret", "mkt_breadth", "mkt_disp", "rel_mkt", "rel_sec", "sec_m_ret"]
    FEATS = MF + dcols
    return df, FEATS


def stack_predict(X, ytr, tr, te):
    import lightgbm as lgb
    from sklearn.ensemble import HistGradientBoostingClassifier
    import xgboost as xgb
    ps = []
    for m in [
        lgb.LGBMClassifier(n_estimators=400, max_depth=6, learning_rate=0.04, subsample=0.8,
                           colsample_bytree=0.8, n_jobs=8, verbose=-1, random_state=0),
        HistGradientBoostingClassifier(max_iter=350, max_depth=6, learning_rate=0.05,
                                       l2_regularization=1.0, random_state=0),
        xgb.XGBClassifier(n_estimators=400, max_depth=6, learning_rate=0.04, subsample=0.8,
                          colsample_bytree=0.8, eval_metric="logloss", tree_method="hist",
                          n_jobs=8, random_state=0)]:
        m.fit(X[tr], ytr); ps.append(m.predict_proba(X[te])[:, 1])
    return np.mean(ps, axis=0)


def run(con, fo):
    df, FEATS = build(con, fo)
    X = df[FEATS].replace([np.inf, -np.inf], np.nan)
    folds = [("2025-07-01", "2025-07-01", "2025-12-31"), ("2026-01-01", "2026-01-01", "2099")]
    print(f"INTRADAY v2 | rows={len(df)} feats={len(FEATS)} | targets: 4/5@1%, 3/5@2%, 2/5@3%\n")
    for direction in ("LONG", "SHORT"):
        print(f"--- {direction} ---")
        for X_thr, need in TARGETS.items():
            lab = ((df["post_move"] >= X_thr) if direction == "LONG"
                   else (df["post_move"] <= -X_thr)).astype(int).values
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
            if p5:
                mp = np.mean(p5); lift = (mp/5*100)/np.mean(base)
                flag = "  *** TARGET MET ***" if mp >= need else ""
                print(f"  +/-{X_thr:.0f}% (need {need}/5): p@5={mp:.2f} ({mp/5*100:.0f}%)  "
                      f"base={np.mean(base):.1f}%  lift={lift:.2f}x  days>={need}of5={np.mean(dT):.0f}%{flag}", flush=True)
        print()


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("INTRADAY2_DONE")
