"""
Magnitude-optimised model: since a plain morning-volatility rank (3.08/5 @1%) beat the
full ML model (2.8), build a model PURPOSE-BUILT for 'will it move >=X% either way' using
only volatility/activity features, and compare to the single-feature screen. Also try a
simple blended rank. Target: push p@5 @1% toward 4.5, @2% toward 3.
"""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db, universe
from persona_engine.intraday_eod import morning_features

NPICK = 5
FOLDS = [("2025-07-01", "2025-07-01", "2025-12-31"), ("2026-01-01", "2026-01-01", "2099")]


def prep(con, fo):
    mf = morning_features(con, fo).dropna(subset=["m_ret", "post_move"])
    d = pd.read_sql_query("SELECT symbol,trade_date,atr_20_pct,vol_ratio_20d,atr_5_vs_20,"
                          "range_pct,n_sub_2_5_range_7d FROM persona_signal_features "
                          "WHERE symbol IN (%s)" % ",".join("?"*len(fo)), con, params=fo) \
        if False else pd.read_sql_query(
            "SELECT symbol,trade_date,atr_20_pct,vol_ratio_20d FROM persona_signal_features "
            "WHERE symbol IN (%s)" % ",".join("?"*len(fo)), con, params=fo)
    d = d.sort_values(["symbol", "trade_date"]); d["date"] = d.groupby("symbol")["trade_date"].shift(-1)
    mf = mf.merge(d[["symbol", "date", "atr_20_pct", "vol_ratio_20d"]], on=["symbol", "date"], how="left")
    mf["abs_mret"] = mf["m_ret"].abs()
    mf["mkt_disp"] = mf.groupby("date")["m_ret"].transform("std")
    return mf


def p5(mf, scorecol, Xs=(1, 2)):
    out = {}
    for X in Xs:
        hits = []
        for dt, g in mf.groupby("date"):
            g = g.dropna(subset=[scorecol])
            if len(g) < 30: continue
            picks = g.sort_values(scorecol, ascending=False).head(NPICK)
            hits.append((picks["post_move"].abs() >= X).sum())
        out[X] = round(np.mean(hits), 2)
    return out


def model_p5(mf, feats, Xs=(1, 2)):
    import lightgbm as lgb
    res = {X: [] for X in Xs}
    for X in Xs:
        lab = (mf["post_move"].abs() >= X).astype(int).values
        Xm = mf[feats].replace([np.inf, -np.inf], np.nan)
        for tr_end, lo, hi in FOLDS:
            tr = (mf["date"] < tr_end).values; te = ((mf["date"] >= lo) & (mf["date"] <= hi)).values
            if tr.sum() < 3000 or te.sum() < 300: continue
            m = lgb.LGBMClassifier(n_estimators=400, max_depth=6, learning_rate=0.04, subsample=0.8,
                                   colsample_bytree=0.8, n_jobs=8, verbose=-1, random_state=0)
            m.fit(Xm[tr], lab[tr])
            d = mf.loc[te, ["date", "post_move"]].copy(); d["p"] = m.predict_proba(Xm[te])[:, 1]
            hits = [gg.sort_values("p", ascending=False).head(NPICK)["post_move"].abs().ge(X).sum()
                    for _, gg in d.groupby("date")]
            res[X].append(np.mean(hits))
    return {X: round(np.mean(v), 2) for X, v in res.items() if v}


def run(con, fo):
    mf = prep(con, fo)
    # blended rank of top magnitude signals
    mf["blend"] = (mf["m_volat"].rank(pct=True) + mf["abs_mret"].rank(pct=True)
                   + mf["atr_20_pct"].rank(pct=True) + mf["mkt_disp"].rank(pct=True)) / 4
    print(f"rows={len(mf)}\n")
    print("single morning-volatility rank:", p5(mf, "m_volat"))
    print("blended-rank (volat+|mret|+atr+disp):", p5(mf, "blend"))
    feats = ["m_volat", "abs_mret", "atr_20_pct", "m_vol", "m_range", "m_loc", "m_vwap_dev",
             "vol_ratio_20d", "mkt_disp"]
    print("magnitude-optimised ML model:", model_p5(mf, feats))


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("MAGOPT_DONE")
