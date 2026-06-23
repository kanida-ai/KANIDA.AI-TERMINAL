"""
F&O THRESHOLD engine (objective revised by operator 2026-06-22):
identify next-day high-conviction moves rather than exact Top-10 ranking.

  • Long Futures : stocks most likely to close next day >= +THRESH% (default +5%)
  • Short Futures: stocks most likely to close next day <= -THRESH%

A walk-forward gradient-boosted classifier (the persona/stock agent's learned model)
outputs P(up5) and P(dn5) per stock per EOD; the persona agent ranks them into Long
and Short lists. Sector context enters as features (rs_sector_*, sector_rank). Success
= the predicted stock actually reaches the threshold next day.

Metrics per the operator's table: Long/Short Hit Rate, Precision, Missed Movers,
False Positives, plus Recall and lift vs base rate. Rule learning = model feature
importances + hit/miss/FP differentiators.

Closed loop: the model is RE-TRAINED walk-forward (train window strictly before the
test period), so it keeps learning from realised outcomes; no lookahead (P1/P3).
"""
from __future__ import annotations

import json
import sqlite3
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from persona_engine import db
from persona_engine.model import ALL_FEATURES

THRESH = 5.0          # +/- % move that defines a "high-conviction" success
TOPN = 10             # default ranked-list size
CONVICTION_NS = (3, 5, 10)  # also report precision at these list sizes

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS fo_threshold_predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_date TEXT NOT NULL, direction TEXT NOT NULL, rank INTEGER,
        symbol TEXT NOT NULL, sector TEXT, prob REAL, threshold_pct REAL,
        model_version TEXT, created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(prediction_date,direction,symbol))""",
    "CREATE INDEX IF NOT EXISTS ix_ftp_date ON fo_threshold_predictions(prediction_date)",
    """CREATE TABLE IF NOT EXISTS fo_threshold_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_date TEXT NOT NULL, outcome_date TEXT, direction TEXT,
        symbol TEXT NOT NULL, prob REAL, actual_move REAL, threshold_pct REAL,
        hit INTEGER, false_positive INTEGER, created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(prediction_date,direction,symbol))""",
    "CREATE INDEX IF NOT EXISTS ix_fto_date ON fo_threshold_outcomes(prediction_date)",
    """CREATE TABLE IF NOT EXISTS fo_threshold_missed (
        outcome_date TEXT NOT NULL, direction TEXT, symbol TEXT, actual_move REAL,
        prob REAL, created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(outcome_date,direction,symbol))""",
    """CREATE TABLE IF NOT EXISTS fo_threshold_feature_importance (
        trained_for TEXT, direction TEXT, feature TEXT, importance REAL,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(trained_for,direction,feature))""",
]


def _build_dataset(con, fo_universe, start, target="chart"):
    feats = pd.read_sql_query(
        "SELECT * FROM persona_signal_features WHERE symbol IN (%s) AND trade_date>=?"
        % ",".join("?"*len(fo_universe)), con, params=fo_universe+[start])
    openf = pd.read_sql_query(
        "SELECT symbol,trade_date,prev_close,day_close,oc_full FROM persona_open_features "
        "WHERE symbol IN (%s) AND trade_date>=?" % ",".join("?"*len(fo_universe)),
        con, params=fo_universe+[start])
    # NEW non-price features for the (non-linear) classifier to exploit
    try:
        ev = pd.read_sql_query(
            "SELECT symbol,trade_date,earn_next1,earn_recent2,deliv_pct,deliv_z20,accum "
            "FROM persona_event_features WHERE symbol IN (%s) AND trade_date>=?"
            % ",".join("?"*len(fo_universe)), con, params=fo_universe+[start])
        feats = feats.merge(ev, on=["symbol", "trade_date"], how="left")
    except Exception:
        pass
    cal = sorted(feats["trade_date"].unique())
    nxt = {cal[i]: cal[i+1] for i in range(len(cal)-1)}
    feats["odate"] = feats["trade_date"].map(nxt)
    o = openf.rename(columns={"trade_date": "odate"})
    df = feats.merge(o, on=["symbol", "odate"], how="inner")
    if target == "chart":
        df["move"] = (df["day_close"]/df["prev_close"]-1)*100
    else:  # open->close (capturable)
        df["move"] = df["oc_full"]
    df = df.dropna(subset=["move"])
    df["up"] = (df["move"] >= THRESH).astype(int)
    df["dn"] = (df["move"] <= -THRESH).astype(int)
    df["year"] = df["odate"].str[:4]
    return df


def _fit_predict_year(df, feats_cols, label, test_year):
    from sklearn.ensemble import HistGradientBoostingClassifier
    tr = df["year"] < test_year
    te = df["year"] == test_year
    if tr.sum() < 5000 or te.sum() < 300:
        return None, None
    X = df[feats_cols].replace([np.inf, -np.inf], np.nan)
    m = HistGradientBoostingClassifier(max_iter=250, max_depth=4, learning_rate=0.06,
                                       l2_regularization=1.0, random_state=0)
    m.fit(X[tr], df.loc[tr, label])
    p = m.predict_proba(X[te])[:, 1]
    # permutation-free importance proxy: use the model's training via a quick
    # impurity-like ranking from a shallow surrogate is costly; instead rank features
    # by |corr(feature_rank, label)| on train (stable, cheap).
    imp = {}
    for f in feats_cols:
        s = df.loc[tr, [f, label]].dropna()
        if len(s) > 1000 and s[f].std() > 0:
            imp[f] = abs(np.corrcoef(s[f].rank(), s[label])[0, 1])
    return p, imp


def run_threshold(con, fo_universe, start="2022-01-01", target="chart",
                  topn=TOPN, persist=True, verbose=True) -> Dict:
    for stmt in SCHEMA:
        con.execute(stmt)
    df = _build_dataset(con, fo_universe, start, target=target)
    EVENT_FEATS = ["earn_next1", "earn_recent2", "deliv_pct", "deliv_z20", "accum"]
    feats_cols = [f for f in ALL_FEATURES if f in df.columns]
    feats_cols += [f for f in EVENT_FEATS if f in df.columns]
    sectors = df.groupby("symbol")["sector"].last().to_dict()

    pred_rows, out_rows, miss_rows, imp_rows = [], [], [], []
    per_year = {}
    for ty in ["2023", "2024", "2025", "2026"]:
        res = {}
        for direction, label in [("LONG", "up"), ("SHORT", "dn")]:
            p, imp = _fit_predict_year(df, feats_cols, label, ty)
            if p is None:
                continue
            te = df["year"] == ty
            d = df.loc[te, ["trade_date", "odate", "symbol", "sector", "move", label]].copy()
            d["p"] = p
            metrics = _measure(d, label, direction, ty, topn, pred_rows, out_rows,
                               miss_rows)
            res[direction] = metrics
            for f, v in sorted(imp.items(), key=lambda kv: -kv[1])[:15]:
                imp_rows.append((ty, direction, f, round(float(v), 4)))
        per_year[ty] = res

    if persist:
        _persist(con, pred_rows, out_rows, miss_rows, imp_rows)
    if verbose:
        _print(per_year, target)
    return {"per_year": per_year}


def _measure(d, label, direction, ty, topn, pred_rows, out_rows, miss_rows):
    """Per-day ranked list + metrics, accumulated over the test year."""
    prec_at = {n: [] for n in CONVICTION_NS}
    hits_top, sig_top, recall, n_movers_l, fp_top = [], [], [], [], []
    thr_sign = 1 if direction == "LONG" else -1
    for dt, g in d.groupby("odate"):
        g = g.sort_values("p", ascending=False)
        movers = int(g[label].sum())
        for n in CONVICTION_NS:
            top = g.head(n)
            prec_at[n].append(top[label].mean())
        topN = g.head(topn)
        h = int(topN[label].sum())
        hits_top.append(h); sig_top.append(len(topN)); fp_top.append(len(topN)-h)
        n_movers_l.append(movers)
        recall.append(h/movers if movers else np.nan)
        # store predictions/outcomes (top-N)
        for rank, (_, r) in enumerate(topN.iterrows(), 1):
            hit = int(r[label] == 1)
            pred_rows.append((r["trade_date"], direction, rank, r["symbol"],
                              r.get("sector"), float(r["p"]), THRESH, f"thr-{ty}"))
            out_rows.append((r["trade_date"], dt, direction, r["symbol"], float(r["p"]),
                             float(r["move"]), THRESH, hit, 1-hit))
        # missed movers (actual movers not in top-N)
        miss = g[g[label] == 1].iloc[topn:]
        for _, r in miss.iterrows():
            miss_rows.append((dt, direction, r["symbol"], float(r["move"]), float(r["p"])))
    base = d[label].mean()
    return {
        "precision_at": {n: round(np.nanmean(v)*100, 1) for n, v in prec_at.items()},
        "hit_rate_topN": round(np.mean(hits_top), 2),
        "precision_topN": round(np.nansum(hits_top)/max(1, np.nansum(sig_top))*100, 1),
        "recall": round(np.nanmean(recall)*100, 1),
        "avg_movers_per_day": round(np.mean(n_movers_l), 2),
        "avg_false_pos_topN": round(np.mean(fp_top), 2),
        "base_rate": round(base*100, 2),
        "lift": round((np.nansum(hits_top)/max(1, np.nansum(sig_top)))/max(1e-9, base), 1),
    }


def _persist(con, pred_rows, out_rows, miss_rows, imp_rows):
    cur = con.cursor()
    for t in ["fo_threshold_predictions", "fo_threshold_outcomes",
              "fo_threshold_missed", "fo_threshold_feature_importance"]:
        cur.execute(f"DELETE FROM {t}")
    # pred_rows store: fix odate_pred placeholder (we used outcome date dt as pred? no)
    # predictions are keyed by prediction date = the EOD before dt; but we ranked on dt's
    # feature rows whose trade_date is the prediction day. Recover via outcomes table use dt.
    cur.executemany(
        "INSERT OR REPLACE INTO fo_threshold_predictions"
        "(prediction_date,direction,rank,symbol,sector,prob,threshold_pct,model_version) "
        "VALUES (?,?,?,?,?,?,?,?)",
        [(p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]) for p in pred_rows])
    cur.executemany(
        "INSERT OR REPLACE INTO fo_threshold_outcomes"
        "(prediction_date,outcome_date,direction,symbol,prob,actual_move,threshold_pct,hit,false_positive) "
        "VALUES (?,?,?,?,?,?,?,?,?)", out_rows)
    cur.executemany(
        "INSERT OR REPLACE INTO fo_threshold_missed"
        "(outcome_date,direction,symbol,actual_move,prob) VALUES (?,?,?,?,?)", miss_rows)
    cur.executemany(
        "INSERT OR REPLACE INTO fo_threshold_feature_importance"
        "(trained_for,direction,feature,importance) VALUES (?,?,?,?)", imp_rows)
    con.commit()


def _print(per_year, target):
    print(f"\n===== F&O THRESHOLD (+/-{THRESH}% next day, target={target}) =====")
    print("(precision = predicted hits / signals; base = unconditional rate)")
    for ty, res in per_year.items():
        if not res:
            continue
        print(f"\n  --- {ty} ---")
        for direction, m in res.items():
            print(f"  {direction:5s}: prec@3={m['precision_at'][3]}%  @5={m['precision_at'][5]}%  "
                  f"@10={m['precision_at'][10]}%  | hit {m['hit_rate_topN']}/10  "
                  f"recall={m['recall']}%  FP/day={m['avg_false_pos_topN']}  "
                  f"base={m['base_rate']}%  lift={m['lift']}x")
    print("=" * 64)


if __name__ == "__main__":
    import sys
    from persona_engine import universe
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
    tgt = sys.argv[1] if len(sys.argv) > 1 else "chart"
    run_threshold(con, fo, start="2022-01-01", target=tgt, verbose=True)
    con.close()
    print("THR_ENGINE_DONE")
