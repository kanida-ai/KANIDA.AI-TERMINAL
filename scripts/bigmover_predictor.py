"""
+5%-BY-EOD PREDICTOR  (walk-forward, out-of-sample, no leakage)
================================================================
Goal: at 10:00, rank liquid stocks by P(close >= +5% today) using ONLY info known
by 10:00 -- intraday price action/volume (open->10:00) + the prior-day Falcon setup
context. Enter the top few, capture 10:00->close. Honest OOS test vs the 2% base rate
and vs pure momentum.

Dataset: outputs/_bigmover_1000_dataset.parquet (from bigmover_feasibility.py)
Output:  outputs/BigMover_Predictor.xlsx
"""
import sqlite3, bisect
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from falcon_signal_replay import load_patterns, FEATURE_COLS, FIDX, rule_mask

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"
DATA = ROOT / "outputs" / "_bigmover_1000_dataset.parquet"
OUT = ROOT / "outputs" / "BigMover_Predictor.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
CTX = ["roc_60", "roc_20", "weekly_close_loc", "dist_high_252", "dist_sma_200",
       "rsi_14", "atr_20_pct", "weekly_breakout_20w"]
T = "10:00"


def per_stock_scores(con, patterns, date):
    rows = con.execute(f"SELECT symbol, {', '.join(FEATURE_COLS)} FROM falcon_features WHERE trade_date=?",
                       (date,)).fetchall()
    if not rows:
        return {}, {}
    syms = [r[0] for r in rows]
    X = np.full((len(syms), len(FEATURE_COLS)), np.nan)
    for i, r in enumerate(rows):
        X[i] = [v if v is not None else np.nan for v in r[1:]]
    yr = int(date[:4]); elig = [p for p in patterns if int(p["mined_year"]) < yr]
    fire = np.zeros(len(syms), np.int32); score = np.zeros(len(syms))
    for p in elig:
        m = rule_mask(p["rule"], X)
        if m.any():
            fire += m.astype(np.int32); score += m.astype(float) * p["oos_lift"]
    sc = {syms[i]: (score[i], int(fire[i])) for i in range(len(syms))}
    ctx = {syms[i]: {c: X[i, FIDX[c]] for c in CTX} for i in range(len(syms))}
    return sc, ctx


def main():
    df = pd.read_parquet(DATA)
    df.columns = [c.replace(":", "") for c in df.columns]   # ret_10:00 -> ret_1000 (itertuples-safe)
    print(f"[*] dataset {len(df):,} rows", flush=True)
    con = sqlite3.connect(str(SLIM))
    patterns = load_patterns(con)
    cal = [r[0] for r in con.execute("SELECT DISTINCT trade_date FROM ohlc_daily ORDER BY 1")]

    def prev_td(d):
        i = bisect.bisect_left(cal, d)
        return cal[i - 1] if i > 0 else None

    dates = sorted(df["date"].unique())
    prior_map = {d: prev_td(d) for d in dates}
    need_prior = sorted(set(v for v in prior_map.values() if v))
    print(f"[*] enriching with prior-day Falcon context for {len(need_prior)} days ...", flush=True)
    PSC, PCTX, POHLC = {}, {}, {}
    for k, p in enumerate(need_prior):
        sc, ctx = per_stock_scores(con, patterns, p)
        PSC[p] = sc; PCTX[p] = ctx
        POHLC[p] = {r[0]: (r[1], r[2], r[3]) for r in con.execute(
            "SELECT symbol, close, high, volume FROM ohlc_daily WHERE trade_date=?", (p,))}
        if (k + 1) % 100 == 0:
            print(f"  [{k+1}/{len(need_prior)}]", flush=True)
    con.close()

    # build features
    rec = []
    for r in df.itertuples(index=False):
        p = prior_map.get(r.date)
        if not p or r.symbol not in PSC.get(p, {}):
            continue
        sc, nf = PSC[p][r.symbol]
        ctx = PCTX[p].get(r.symbol, {})
        oh = POHLC[p].get(r.symbol)
        if not oh or oh[0] is None or oh[0] <= 0:
            continue
        pclose, phigh, pvol = oh
        ret_t = r.ret_1000; hi_t = r.hi_1000
        px_t = r.px_1000; vol_t = r.vol_1000
        d = {"date": r.date, "symbol": r.symbol,
             "ret_1000": ret_t, "ret_0945": r.ret_0945, "ret_0930": r.ret_0930,
             "accel1": (r.ret_0945 - r.ret_0930), "accel2": (ret_t - r.ret_0945),
             "hi_1000": hi_t, "range_pos": (ret_t / hi_t) if hi_t and hi_t > 0 else 0.0,
             "gap": (r.open / pclose - 1) * 100,
             "brk_prior_high": (px_t / phigh - 1) * 100 if phigh else np.nan,
             "vol_pace": (vol_t / pvol) if pvol and pvol > 0 else np.nan,
             "f_score": sc, "f_fires": nf,
             "label": 1 if r.ret_full >= 5 else 0,
             "capture": r.cap_1000_close}
        for c in CTX:
            d[c] = ctx.get(c, np.nan)
        rec.append(d)
    D = pd.DataFrame(rec)
    D["dt"] = pd.to_datetime(D["date"]); D["year"] = D.dt.dt.year
    D["q"] = D.dt.dt.to_period("Q").astype(str)
    feats = ["ret_1000", "ret_0945", "ret_0930", "accel1", "accel2", "hi_1000",
             "range_pos", "gap", "brk_prior_high", "vol_pace", "f_score", "f_fires"] + CTX
    print(f"[*] modelling rows {len(D):,}  base rate {D.label.mean()*100:.2f}%", flush=True)

    # walk-forward by quarter (train on all prior quarters, >=2 quarters history)
    quarters = sorted(D["q"].unique())
    D["proba"] = np.nan
    for i, q in enumerate(quarters):
        if i < 2:
            continue
        tr = D[D["q"].isin(quarters[:i])]; te = D[D["q"] == q]
        if len(tr) < 5000 or te.empty:
            continue
        clf = HistGradientBoostingClassifier(max_iter=300, max_depth=4, learning_rate=0.05,
                                             l2_regularization=1.0, random_state=0)
        clf.fit(tr[feats], tr["label"])
        D.loc[D["q"] == q, "proba"] = clf.predict_proba(te[feats])[:, 1]
    oos = D[D["proba"].notna()].copy()
    print(f"[*] OOS rows {len(oos):,}  span {oos.date.min()}..{oos.date.max()}", flush=True)

    # rank per day, take top-K; compare model vs pure-momentum baseline
    def evaluate(score_col, K):
        rows = []
        for dt, g in oos.groupby("date"):
            gg = g.sort_values(score_col, ascending=False).head(K)
            rows.append({"date": dt, "hit": gg.label.mean(), "cap": gg.capture.mean(),
                         "year": gg.dt.dt.year.iloc[0]})
        r = pd.DataFrame(rows)
        return r

    summary = []
    for name, col in [("MODEL P(+5%)", "proba"), ("baseline: 10:00 return", "ret_1000")]:
        for K in (5, 10):
            r = evaluate(col, K)
            summary.append({"selector": name, "topK": K,
                            "picks/day": K,
                            "hit_rate_%": round(r.hit.mean() * 100, 1),
                            "avg_capture_%": round(r.cap.mean(), 3),
                            "portfolio_WR>0_%": round((r.cap > 0).mean() * 100, 1),
                            "days": len(r)})
    summ = pd.DataFrame(summary)

    # model top-5 by year
    r5 = evaluate("proba", 5)
    yearly = r5.groupby("year").agg(days=("hit", "size"),
                                    hit_rate_pct=("hit", lambda x: round(x.mean()*100, 1)),
                                    avg_capture_pct=("cap", lambda x: round(x.mean(), 3)),
                                    WR_pos_pct=("cap", lambda x: round((x>0).mean()*100, 1))).reset_index()

    base = round(D.label.mean() * 100, 2)
    print("\n=== +5%-BY-EOD PREDICTOR (OOS) ===  base rate {:.2f}%".format(base))
    print(summ.to_string(index=False))
    print("\n=== MODEL top-5 by year (OOS) ===")
    print(yearly.to_string(index=False))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        pd.DataFrame([{"base_rate_%": base, "oos_rows": len(oos),
                       "oos_span": f"{oos.date.min()}..{oos.date.max()}"}]).to_excel(xl, "0_Setup", index=False)
        summ.to_excel(xl, "1_Model_vs_Baseline", index=False)
        yearly.to_excel(xl, "2_Model_Top5_Yearly", index=False)
        # daily log of model top-5
        rows = []
        for dt, g in oos.groupby("date"):
            for _, x in g.sort_values("proba", ascending=False).head(5).iterrows():
                rows.append({"date": dt, "symbol": x.symbol, "P(+5%)": round(x.proba, 3),
                             "ret@10:00": round(x.ret_1000, 2),
                             "vol_pace": round(x.vol_pace, 2) if pd.notna(x.vol_pace) else None,
                             "f_score": round(x.f_score, 0),
                             "captured_to_close%": round(x.capture, 2), "hit_+5%": int(x.label)})
        pd.DataFrame(rows).to_excel(xl, "3_Model_Top5_Daily", index=False)
    print(f"\n[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")


if __name__ == "__main__":
    main()
