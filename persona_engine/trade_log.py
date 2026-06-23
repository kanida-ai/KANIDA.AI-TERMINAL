"""
Daily intraday TRADE LOG with per-trade explanation + day-by-day learning.

Model: predict at 10:00 (from 9:15-10:00 microstructure + market context + prior-day
context), enter 10:00, EXIT 15:15 (no carryover). Each day: top-5 LONG (P up>=1%) and
top-5 SHORT (P down>=1%). Base = stacked LGBM+HistGBM+XGB trained on data BEFORE the
test window. On top, a per-stock ONLINE reliability prior (EWMA of that stock's recent
hit/miss) is updated EVERY day and fed into the NEXT day's ranking -> the explicit
"learned yesterday, applied today" loop.

Outputs (outputs/persona_findings/):
  TradeLog_Intraday_<window>.xlsx  -> long_trades, short_trades, daily_summary,
                                      feature_importance, monthly
And returns a structured object the docx report builder consumes.
"""
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

from persona_engine import db, universe
from persona_engine.intraday_eod_v2 import build

THR = 1.0          # +/-1% target
NPICK = 5
ALPHA = 0.25       # online reliability learning rate
WPRIOR = 0.30      # weight of the online prior in the blended score
TEST_START = "2026-01-01"

LABELS = {
    "m_ret": "morning move", "m_vol": "morning volume", "m_volat": "morning volatility",
    "m_loc": "near morning high/low", "m_vwap_dev": "vs morning VWAP",
    "m_range": "morning range", "m_last15": "9:45-10:00 push", "m_upbars": "up-bar share",
    "mkt_breadth": "market breadth", "mkt_disp": "market activity(dispersion)",
    "mkt_m_ret": "market trend", "rel_mkt": "strength vs market", "rel_sec": "strength vs sector",
    "vol_ratio_20d": "volume vs 20d", "atr_20_pct": "recent volatility",
    "rs_index_20d": "20d rel-strength", "dist_high_20": "dist from 20d high",
    "roc_5": "5d momentum", "rsi_14": "RSI",
}
DRIVER_FEATS = list(LABELS.keys())


def _stack(X, ytr, tr, te):
    import lightgbm as lgb
    from sklearn.ensemble import HistGradientBoostingClassifier
    import xgboost as xgb
    ps, imps = [], None
    lg = lgb.LGBMClassifier(n_estimators=400, max_depth=6, learning_rate=0.04, subsample=0.8,
                            colsample_bytree=0.8, n_jobs=8, verbose=-1, random_state=0)
    lg.fit(X[tr], ytr); ps.append(lg.predict_proba(X[te])[:, 1])
    imps = pd.Series(lg.feature_importances_, index=X.columns)
    hg = HistGradientBoostingClassifier(max_iter=350, max_depth=6, learning_rate=0.05,
                                        l2_regularization=1.0, random_state=0)
    hg.fit(X[tr], ytr); ps.append(hg.predict_proba(X[te])[:, 1])
    xg = xgb.XGBClassifier(n_estimators=400, max_depth=6, learning_rate=0.04, subsample=0.8,
                           colsample_bytree=0.8, eval_metric="logloss", tree_method="hist",
                           n_jobs=8, random_state=0)
    xg.fit(X[tr], ytr); ps.append(xg.predict_proba(X[te])[:, 1])
    return np.mean(ps, axis=0), imps


def _drivers(day_df, idx, feats_present, k=3):
    """Top-k features where this pick is most extreme (favourable) vs the day's universe."""
    out = []
    row = day_df.loc[idx]
    for f in feats_present:
        col = day_df[f]
        if col.notna().sum() < 10:
            continue
        pct = (col.rank(pct=True)).loc[idx]
        out.append((f, abs(pct-0.5), pct, row[f]))
    out.sort(key=lambda t: -t[1])
    parts = []
    for f, _, pct, val in out[:k]:
        hl = "high" if pct > 0.5 else "low"
        parts.append(f"{LABELS.get(f,f)} {hl} ({val:+.2f}, {pct*100:.0f}pct)")
    return "; ".join(parts)


def run(con, fo, test_start=TEST_START):
    df, FEATS = build(con, fo)
    X = df[FEATS].replace([np.inf, -np.inf], np.nan)
    drv_feats = [f for f in DRIVER_FEATS if f in df.columns]

    results = {}
    for direction, sign in [("LONG", 1), ("SHORT", -1)]:
        lab = ((df["post_move"] >= THR) if sign == 1 else (df["post_move"] <= -THR)).astype(int).values
        tr = (df["date"] < test_start).values
        te = (df["date"] >= test_start).values
        p, imps = _stack(X, lab[tr], tr, te)
        d = df.loc[te, ["date", "symbol", "sector", "px1000", "px1515", "post_move"]].copy()
        d["p_base"] = p
        d["succ"] = lab[te]
        d = d.reset_index(drop=True)
        # online per-stock reliability prior, updated daily, applied next day
        prior = {}
        rows = []
        for dt in sorted(d["date"].unique()):
            g = d[d["date"] == dt].copy()
            g["prior"] = g["symbol"].map(lambda s: prior.get(s, np.nan))
            base_mean = np.nanmean(list(prior.values())) if prior else g["p_base"].mean()
            g["prior"] = g["prior"].fillna(base_mean)
            g["score"] = (1-WPRIOR)*g["p_base"] + WPRIOR*g["prior"]
            top = g.sort_values("score", ascending=False).head(NPICK)
            gi_idx = df.loc[te].reset_index(drop=True)
            for rank, (_, r) in enumerate(top.iterrows(), 1):
                move = r["post_move"] if sign == 1 else -r["post_move"]  # gain in trade direction
                hit = int((r["post_move"] >= THR) if sign == 1 else (r["post_move"] <= -THR))
                day_universe = df.loc[te].reset_index(drop=True)
                day_universe = day_universe[day_universe["date"] == dt]
                drivers = _drivers(day_universe, day_universe.index[day_universe["symbol"] == r["symbol"]][0],
                                   drv_feats)
                prev_prior = prior.get(r["symbol"], None)
                rows.append({
                    "date": dt, "direction": direction, "rank": rank, "symbol": r["symbol"],
                    "sector": r["sector"], "entry_1000": round(r["px1000"], 2),
                    "exit_1515": round(r["px1515"], 2), "move_pct_in_dir": round(move, 2),
                    "hit_1pct": hit, "confidence": round(r["score"], 3),
                    "reliability_prior": (round(prev_prior, 2) if prev_prior is not None else None),
                    "why_picked": drivers,
                })
            # ---- LEARN: update each picked stock's reliability from today's outcome ----
            for _, r in top.iterrows():
                hit = int((r["post_move"] >= THR) if sign == 1 else (r["post_move"] <= -THR))
                prior[r["symbol"]] = (1-ALPHA)*prior.get(r["symbol"], r["p_base"]) + ALPHA*hit
        results[direction] = {"trades": pd.DataFrame(rows), "imps": imps}
    return results, df.loc[df["date"] >= test_start]


def daily_summary(results):
    L = results["LONG"]["trades"]; S = results["SHORT"]["trades"]
    out = []
    for dt in sorted(set(L["date"]) | set(S["date"])):
        ld = L[L["date"] == dt]; sd = S[S["date"] == dt]
        lh, sh = int(ld["hit_1pct"].sum()), int(sd["hit_1pct"].sum())
        # data-derived lesson: which driver feature most separated hits from misses today
        out.append({
            "date": dt, "long_hits_of5": lh, "short_hits_of5": sh,
            "long_avg_move": round(ld["move_pct_in_dir"].mean(), 2),
            "short_avg_move": round(sd["move_pct_in_dir"].mean(), 2),
            "combined_hits_of10": lh+sh,
        })
    return pd.DataFrame(out)


def export(results, summary, window="2026"):
    OUT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_findings")
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"TradeLog_Intraday_{window}.xlsx"
    impL = results["LONG"]["imps"].sort_values(ascending=False).head(20).rename("LONG_importance")
    impS = results["SHORT"]["imps"].sort_values(ascending=False).head(20).rename("SHORT_importance")
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        results["LONG"]["trades"].to_excel(xl, sheet_name="long_trades", index=False)
        results["SHORT"]["trades"].to_excel(xl, sheet_name="short_trades", index=False)
        summary.to_excel(xl, sheet_name="daily_summary", index=False)
        impL.to_frame().join(impS, how="outer").to_excel(xl, sheet_name="feature_importance")
        summary.assign(month=summary["date"].str[:7]).groupby("month").agg(
            long=("long_hits_of5", "mean"), short=("short_hits_of5", "mean"),
            days=("date", "count")).round(2).to_excel(xl, sheet_name="monthly")
    return path


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    results, _ = run(con, fo)
    summ = daily_summary(results)
    path = export(results, summ)
    L = results["LONG"]["trades"]; S = results["SHORT"]["trades"]
    print(f"days={summ.shape[0]}  long_trades={len(L)} short_trades={len(S)}")
    print(f"avg long hits/5={summ['long_hits_of5'].mean():.2f}  short hits/5={summ['short_hits_of5'].mean():.2f}")
    print("best long days:\n", summ.sort_values('long_hits_of5', ascending=False).head(3).to_string(index=False))
    print("XLSX ->", path)
    con.close()
    print("TRADELOG_DONE")
