"""
INTRADAY same-day (no carryover) model (operator 2026-06-23):
  observe 9:15->10:00 -> predict direction at 10:00 -> enter 10:00, EXIT 15:15.
  Of the top-5 highest-confidence picks, what % moved >=+/-X% (X=3,4,5) from 10:00->15:15?

Features (per symbol, day): morning microstructure from the 9:15-10:00 window + prior-day
daily context. Target: (px_15:15 / px_10:00 - 1). Walk-forward on ohlc_1min (2024-05..2026-05).
Reports precision@5, base rate, lift, % days hitting 3/5 — long & short, X in {3,4,5}.
"""
from __future__ import annotations

import time
import numpy as np
import pandas as pd

from persona_engine import db, universe
from persona_engine.model import ALL_FEATURES

NPICK = 5


def morning_features(con, fo):
    # all 1-min bars 09:15..10:00 + the 15:15 exit bar, 2024+
    rows = con.execute(
        "SELECT symbol, bar_time, open, high, low, close, volume FROM ohlc_1min "
        "WHERE substr(bar_time,12,5) <= '10:00' AND substr(bar_time,12,5) >= '09:15' "
        "AND symbol IN (%s)" % ",".join("?"*len(fo)), list(fo)).fetchall()
    m = pd.DataFrame(rows, columns=["symbol", "bar_time", "o", "h", "l", "c", "v"])
    m["date"] = m["bar_time"].str[:10]; m["tod"] = m["bar_time"].str[11:16]
    m = m.sort_values(["symbol", "date", "tod"])
    grp = m.groupby(["symbol", "date"], sort=False)
    m["ret"] = grp["c"].pct_change()
    m["cv"] = m["c"] * m["v"]
    # vectorised per (symbol,date) aggregations
    a = grp.agg(o0=("o", "first"), px1000=("c", "last"), hi=("h", "max"), lo=("l", "min"),
                m_vol=("v", "sum"), cv=("cv", "sum"),
                m_volat=("ret", "std"), m_upbars=("ret", lambda s: (s > 0).mean())).reset_index()
    # price at 09:45 for last-15-min momentum
    px945 = m[m["tod"] == "09:45"][["symbol", "date", "c"]].rename(columns={"c": "px945"})
    a = a.merge(px945, on=["symbol", "date"], how="left")
    a["m_ret"] = (a["px1000"]/a["o0"]-1)*100
    a["m_range"] = (a["hi"]-a["lo"])/a["o0"]*100
    a["m_loc"] = np.where(a["hi"] > a["lo"], (a["px1000"]-a["lo"])/(a["hi"]-a["lo"]), 0.5)
    a["m_vwap_dev"] = (a["px1000"]/(a["cv"]/a["m_vol"].replace(0, np.nan))-1)*100
    a["m_volat"] = a["m_volat"]*100
    a["m_last15"] = (a["px1000"]/a["px945"]-1)*100
    feat = a[["symbol", "date", "px1000", "m_ret", "m_range", "m_loc", "m_vol",
              "m_volat", "m_vwap_dev", "m_upbars", "m_last15"]]
    # exit bar 15:15
    ex = con.execute(
        "SELECT symbol, substr(bar_time,1,10) d, open FROM ohlc_1min "
        "WHERE substr(bar_time,12,5)='15:15' AND symbol IN (%s)" % ",".join("?"*len(fo)),
        list(fo)).fetchall()
    exd = pd.DataFrame(ex, columns=["symbol", "date", "px1515"])
    feat = feat.merge(exd, on=["symbol", "date"], how="inner")
    feat["post_move"] = (feat["px1515"]/feat["px1000"]-1)*100
    return feat.dropna(subset=["post_move", "px1000"])


def build(con, fo):
    mf = morning_features(con, fo)
    # prior-day daily context (features as of the PREVIOUS trading day)
    daily = pd.read_sql_query(
        "SELECT * FROM persona_signal_features WHERE symbol IN (%s)" % ",".join("?"*len(fo)),
        con, params=fo).sort_values(["symbol", "trade_date"])
    daily["join_date"] = daily.groupby("symbol")["trade_date"].shift(-1)  # features known before this day
    dcols = [f for f in ALL_FEATURES if f in daily.columns]
    df = mf.merge(daily[["symbol", "join_date"]+dcols].rename(columns={"join_date": "date"}),
                  on=["symbol", "date"], how="left")
    df["ym"] = df["date"].str[:7]
    MFEATS = ["m_ret", "m_range", "m_loc", "m_vol", "m_volat", "m_vwap_dev", "m_upbars", "m_last15"]
    FEATS = MFEATS + dcols
    return df, FEATS


def evaluate(df, FEATS, thresh, direction, folds):
    import lightgbm as lgb
    col = "post_move"
    lab = ((df[col] >= thresh) if direction == "LONG" else (df[col] <= -thresh)).astype(int).values
    X = df[FEATS].replace([np.inf, -np.inf], np.nan)
    p5, d3, base = [], [], []
    for tr_end, te_lo, te_hi in folds:
        tr = (df["date"] < tr_end).values
        te = ((df["date"] >= te_lo) & (df["date"] <= te_hi)).values
        if tr.sum() < 3000 or te.sum() < 300:
            continue
        m = lgb.LGBMClassifier(n_estimators=350, max_depth=6, learning_rate=0.04,
                               subsample=0.8, colsample_bytree=0.8, n_jobs=8, verbose=-1, random_state=0)
        m.fit(X[tr], lab[tr])
        p = m.predict_proba(X[te])[:, 1]
        d = df.loc[te, ["date", col]].copy(); d["p"] = p
        d["succ"] = ((d[col] >= thresh) if direction == "LONG" else (d[col] <= -thresh)).astype(int)
        hits = [gg.sort_values("p", ascending=False).head(NPICK)["succ"].sum() for _, gg in d.groupby("date")]
        p5.append(np.mean(hits)); d3.append(np.mean([h >= 3 for h in hits])*100)
        base.append(d["succ"].mean()*100)
    if not p5:
        return None
    return round(np.mean(p5), 2), round(np.mean(d3), 0), round(np.mean(base), 2)


def run(con, fo):
    df, FEATS = build(con, fo)
    print(f"INTRADAY 10:00->15:15 | rows={len(df)} dates={df['date'].nunique()} "
          f"({df['date'].min()}..{df['date'].max()}) feats={len(FEATS)}\n")
    folds = [("2025-07-01", "2025-07-01", "2025-12-31"),
             ("2026-01-01", "2026-01-01", "2026-12-31")]
    import sys
    THR = [float(x) for x in sys.argv[1:]] or [3.0, 4.0, 5.0]
    for direction in ("LONG", "SHORT"):
        print(f"--- {direction} (move {'>=' if direction=='LONG' else '<=-'}X% from 10:00 to 15:15) ---")
        for X in THR:
            r = evaluate(df, FEATS, X, direction, folds)
            if r:
                p5, d3, base = r
                lift = (p5/5*100)/base if base else 0
                print(f"  X={X:.0f}%: p@5={p5} ({p5/5*100:.0f}%)  base={base}%  lift={lift:.2f}x  days3of5={d3}%", flush=True)
        print()


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("INTRADAY_DONE")
