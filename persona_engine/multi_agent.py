"""
MULTI-AGENT per-stock ensemble + continuous-improvement loop (operator 2026-06-22).

Target (per day, each side independently):
  • >=3 of the 5 top-confidence picks move >3% (open->close) in the pick's direction
  • the 5 picks average >=2% in the pick's direction (long >=+2%, short <=-2%)

Agents per stock (the "multi-agents per stock"):
  G   global GBM (whole F&O universe — law of scaling)
  B1..B3 bagged GBMs (row + feature subsets — diversity/robustness)
  S   sector GBM (the stock's sector pool)
  P   per-stock prior: EWMA of the stock's realised hit-rate, updated EVERY day
Ensemble confidence = mean(G,B1..B3,S) blended with P. Persona ranks -> top-5 long
(P(up>=3%)) + top-5 short (P(down>=3%)).

The loop reports three enrichment ROUNDS so you can see whether more agents move the
needle: R1 global-only -> R2 +ensemble+sector -> R3 +per-stock prior. Walk-forward,
yearly retrain on strictly-past data (no lookahead).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from persona_engine import db, universe
from persona_engine.model import ALL_FEATURES

THRESH = 3.0
BASKET_TGT = 2.0
NPICK = 5


def _data(con, fo):
    feats = pd.read_sql_query(
        "SELECT * FROM persona_signal_features WHERE symbol IN (%s) AND trade_date>='2021-01-01'"
        % ",".join("?"*len(fo)), con, params=fo)
    ev = pd.read_sql_query(
        "SELECT symbol,trade_date,earn_next1,earn_recent2,deliv_pct,deliv_z20,accum "
        "FROM persona_event_features WHERE symbol IN (%s)" % ",".join("?"*len(fo)), con, params=fo)
    feats = feats.merge(ev, on=["symbol", "trade_date"], how="left")
    op = pd.read_sql_query(
        "SELECT symbol,trade_date,oc_full FROM persona_open_features WHERE symbol IN (%s)"
        % ",".join("?"*len(fo)), con, params=fo)
    cal = sorted(feats["trade_date"].unique())
    nxt = {cal[i]: cal[i+1] for i in range(len(cal)-1)}
    feats["odate"] = feats["trade_date"].map(nxt)
    df = feats.merge(op.rename(columns={"trade_date": "odate"}), on=["symbol", "odate"], how="inner")
    df = df.dropna(subset=["oc_full"]).reset_index(drop=True)
    df["year"] = df["odate"].str[:4]
    return df


def _agent_preds(df, FEATS, label, ty):
    """Return ensemble P (mean of G,B1..B3,S) for test-year rows, + the global-only P."""
    from sklearn.ensemble import HistGradientBoostingClassifier
    tr = (df["year"] < ty).values
    te = (df["year"] == ty).values
    if tr.sum() < 5000 or te.sum() < 300:
        return None, None
    X = df[FEATS].replace([np.inf, -np.inf], np.nan)
    y = df[label].values
    rng_rows = np.where(tr)[0]

    def fit_pred(rows, cols):
        m = HistGradientBoostingClassifier(max_iter=150, max_depth=4, learning_rate=0.07,
                                           l2_regularization=1.0, random_state=0)
        m.fit(X.iloc[rows][cols], y[rows])
        return m.predict_proba(X.loc[te, cols])[:, 1]

    pg = fit_pred(rng_rows, FEATS)                                  # global
    preds = [pg]
    for seed in (1, 2, 3):                                          # bagged
        r = np.random.default_rng(seed)
        rows = r.choice(rng_rows, size=int(len(rng_rows)*0.7), replace=False)
        cols = list(r.choice(FEATS, size=max(6, int(len(FEATS)*0.8)), replace=False))
        preds.append(fit_pred(rows, cols))
    # sector agent: one model per sector, predict its own test rows
    psec = np.full(te.sum(), np.nan)
    te_idx = np.where(te)[0]
    sec_tr = df.loc[tr, "sector"]
    for sec in df.loc[te, "sector"].dropna().unique():
        rows = rng_rows[(sec_tr == sec).values]
        if len(rows) < 800:
            continue
        mask_te_sec = (df.loc[te, "sector"] == sec).values
        if mask_te_sec.sum() == 0:
            continue
        m = HistGradientBoostingClassifier(max_iter=150, max_depth=3, learning_rate=0.07,
                                           l2_regularization=1.0, random_state=0)
        m.fit(X.iloc[rows][FEATS], y[rows])
        psec[mask_te_sec] = m.predict_proba(X.iloc[te_idx[mask_te_sec]][FEATS])[:, 1]
    psec = np.where(np.isnan(psec), pg, psec)
    ensemble = np.mean(np.vstack(preds + [psec]), axis=0)
    return pg, ensemble


def _measure(df_te, score, direction):
    """precision@5 (avg hits/5) + basket@5 avg, per day over the test year."""
    d = df_te.copy(); d["score"] = score
    succ = (d["oc_full"] >= THRESH) if direction == "LONG" else (d["oc_full"] <= -THRESH)
    d["succ"] = succ.astype(int)
    hits, basket, n3, n2 = [], [], 0, 0
    for dt, g in d.groupby("odate"):
        g = g.sort_values("score", ascending=False).head(NPICK)
        h = g["succ"].sum(); b = g["oc_full"].mean()
        hits.append(h); basket.append(b)
        if h >= 3: n3 += 1
        if (b >= BASKET_TGT) if direction == "LONG" else (b <= -BASKET_TGT): n2 += 1
    ndays = len(hits)
    return {"p@5": round(np.mean(hits), 2), "prec%": round(np.mean(hits)/5*100, 0),
            "basket%": round(np.mean(basket), 2),
            "days_3of5%": round(n3/ndays*100, 0), "days_basket2%": round(n2/ndays*100, 0),
            "ndays": ndays}


def _stock_prior_blend(df_te, ensemble, label, alpha=0.06, w=0.15):
    """Apply the per-stock daily-learning prior causally over the test year."""
    d = df_te.reset_index(drop=True).copy(); d["ens"] = ensemble
    prior = {}
    out = np.empty(len(d))
    for dt, g in d.groupby("odate"):
        pr = np.array([prior.get(s, np.nan) for s in g["symbol"]])
        base = np.nanmean(list(prior.values())) if prior else float(g["ens"].mean())
        pr = np.where(np.isnan(pr), base, pr)
        out[g.index.values] = (1-w)*g["ens"].values + w*pr
        for s, yv in zip(g["symbol"], g[label].values):
            prior[s] = (1-alpha)*prior.get(s, yv) + alpha*yv
    return out


def run(con, fo, verbose=True):
    df = _data(con, fo)
    FEATS = [f for f in ALL_FEATURES if f in df.columns] + \
            [f for f in ["earn_next1", "earn_recent2", "deliv_pct", "deliv_z20", "accum"] if f in df.columns]
    results = {}
    for direction, label in [("LONG", "up3"), ("SHORT", "dn3")]:
        df[label] = ((df["oc_full"] >= THRESH) if direction == "LONG"
                     else (df["oc_full"] <= -THRESH)).astype(int)
        for ty in ["2023", "2024", "2025", "2026"]:
            pg, ens = _agent_preds(df, FEATS, label, ty)
            if pg is None:
                continue
            te = df["year"] == ty
            dte = df.loc[te]
            r1 = _measure(dte, pg, direction)                          # global only
            r2 = _measure(dte, ens, direction)                        # +ensemble+sector
            blended = _stock_prior_blend(dte.reset_index(drop=True), ens, label)
            r3 = _measure(dte.reset_index(drop=True), blended, direction)  # +per-stock prior
            results[(direction, ty)] = (r1, r2, r3)
            if verbose:
                print(f"{direction} {ty}: "
                      f"R1 p@5={r1['p@5']} basket={r1['basket%']}% | "
                      f"R2 p@5={r2['p@5']} basket={r2['basket%']}% | "
                      f"R3 p@5={r3['p@5']} basket={r3['basket%']}%  "
                      f"(R3 days hitting 3of5={r3['days_3of5%']}%, basket>=2%={r3['days_basket2%']}%)", flush=True)
    return results


if __name__ == "__main__":
    con = db.connect(read_only=True)
    fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
    print(f"multi-agent ensemble | target: >=3/5 hit >{THRESH}% AND basket >={BASKET_TGT}% | {len(fo)} stocks")
    print("R1=global  R2=+bagged+sector  R3=+per-stock daily prior\n")
    run(con, fo, verbose=True)
    con.close()
    print("\nMULTIAGENT_DONE")
