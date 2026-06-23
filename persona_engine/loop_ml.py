"""
LOOP-ML hierarchical per-stock agent engine (operator direction 2026-06-22).

Goal: drive the captured Top-10 basket return (entered next-day 09:15 open, exited
15:30 close) toward a milestone fraction (25 -> 50 -> 75 -> 80 -> 90%) of the
perfect-pick BASELINE ceiling, with a fixed-capital P&L view (Rs 1L/pick =>
Rs 10L long + Rs 10L short, churned daily).

Architecture (hierarchical, additive — nothing existing touched):
  • GLOBAL agent  — a gradient-boosted model mined on the FULL pooled OHLC feature
                    history (law of scaling). Re-trained walk-forward on an expanding
                    window (quarterly) — strictly past data only (no lookahead).
  • STOCK agents  — one online learner per stock holding an EWMA of that stock's
                    realised residual (actual_oc - global_pred). Updated EVERY trading
                    day from the day's outcome — the daily feedback / "gems" that make
                    each agent specialise and compound as data accumulates.
  • SECTOR context— sector momentum/RS already lives in the feature set.
  • PERSONA agent — ranks final prediction (global + stock residual) -> Long Top-10
                    (highest expected open->close) + Short Top-10 (lowest).

Walk-forward: train on full 2021 -> predict 2022 day 1 onward -> feed each day's
outcome back. Archive every signal+outcome.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from persona_engine import db
from persona_engine.model import ALL_FEATURES

CAPITAL_PER_PICK = 100_000     # Rs 1L per pick
N_PICKS = 10                   # 10 long + 10 short => Rs 10L each side
RESID_ALPHA = 0.06             # per-stock online learning rate (EWMA)
RETRAIN_FREQ = "Q"            # expanding-window retrain cadence


def _load(con, fo_universe, start="2021-01-01"):
    feats = pd.read_sql_query(
        "SELECT * FROM persona_signal_features WHERE symbol IN (%s) AND trade_date>=?"
        % ",".join("?"*len(fo_universe)), con, params=fo_universe+[start])
    openf = pd.read_sql_query(
        "SELECT symbol,trade_date,oc_full FROM persona_open_features WHERE symbol IN (%s) AND trade_date>=?"
        % ",".join("?"*len(fo_universe)), con, params=fo_universe+[start])
    cal = sorted(feats["trade_date"].unique())
    nxt = {cal[i]: cal[i+1] for i in range(len(cal)-1)}
    feats["odate"] = feats["trade_date"].map(nxt)
    o = openf.rename(columns={"trade_date": "odate", "oc_full": "target"})
    df = feats.merge(o, on=["symbol", "odate"], how="inner").dropna(subset=["target"])
    return df


def _quarter(d: str) -> str:
    y, m = int(d[:4]), int(d[5:7])
    return f"{y}Q{(m-1)//3+1}"


def run_loop(con, fo_universe, train_end_2021=True, walk_start="2022-01-01",
             end: Optional[str] = None, verbose=True, archive=True) -> Dict:
    from sklearn.ensemble import HistGradientBoostingRegressor

    df = _load(con, fo_universe, start="2021-01-01")
    if end:
        df = df[df["odate"] <= end]
    feats_cols = [f for f in ALL_FEATURES if f in df.columns]
    X_all = df[feats_cols].replace([np.inf, -np.inf], np.nan)
    df = df.reset_index(drop=True)
    X_all = X_all.reset_index(drop=True)

    # group rows by the EOD signal day; picks act on 'odate'
    by_day = {d: idx.values for d, idx in df.groupby("trade_date").groups.items()}
    sig_days = sorted([d for d in by_day if d >= walk_start])

    stock_resid: Dict[str, float] = {}
    model = None
    cur_q = None

    daily = []          # (odate, long_basket_ret, short_basket_ret, long_pl, short_pl)
    archive_rows = []

    for T in sig_days:
        q = _quarter(T)
        if q != cur_q:
            # retrain global model on ALL data with odate strictly before T
            mask = (df["odate"] < T).values
            if mask.sum() > 4000:
                model = HistGradientBoostingRegressor(
                    max_iter=300, max_depth=4, learning_rate=0.05,
                    l2_regularization=1.0, random_state=0)
                model.fit(X_all[mask], df.loc[mask, "target"])
                cur_q = q
        if model is None:
            continue

        idx = by_day[T]
        g = df.loc[idx]
        gp = model.predict(X_all.loc[idx])              # aligned to idx order
        syms = g["symbol"].values
        resid = np.array([stock_resid.get(s, 0.0) for s in syms])
        final = gp + resid
        order = np.argsort(-final)
        long_pos = order[:N_PICKS]
        short_pos = order[-N_PICKS:]
        long_idx = idx[long_pos]
        short_idx = idx[short_pos]

        lr = df.loc[long_idx, "target"].mean()
        sr = df.loc[short_idx, "target"].mean()
        odate = g["odate"].iloc[0]
        long_pl = CAPITAL_PER_PICK * df.loc[long_idx, "target"].sum() / 100.0
        short_pl = CAPITAL_PER_PICK * (-df.loc[short_idx, "target"]).sum() / 100.0
        daily.append((odate, lr, sr, long_pl, short_pl))

        if archive:
            for k in long_pos:
                archive_rows.append((T, odate, "LONG", syms[k], float(final[k]),
                                     float(df.at[idx[k], "target"])))
            for k in short_pos:
                archive_rows.append((T, odate, "SHORT", syms[k], float(final[k]),
                                     float(df.at[idx[k], "target"])))
        # ---- daily feedback: EVERY stock agent learns every day from its own
        #      realised residual (actual_oc - global_pred), not just the picks ----
        targets = df.loc[idx, "target"].values
        for k in range(len(idx)):
            s = syms[k]
            err = targets[k] - gp[k]
            stock_resid[s] = (1-RESID_ALPHA)*stock_resid.get(s, 0.0) + RESID_ALPHA*err

    res = pd.DataFrame(daily, columns=["odate", "long_ret", "short_ret", "long_pl", "short_pl"])
    res["ym"] = res["odate"].str[:7]
    out = _summary(res, verbose)
    if archive and archive_rows:
        _archive(con, archive_rows)
    return {"daily": res, "monthly": out}


def _summary(res, verbose):
    g = res.groupby("ym").agg(
        long_ret=("long_ret", "mean"), short_ret=("short_ret", "mean"),
        long_pl=("long_pl", "sum"), short_pl=("short_pl", "sum"),
        days=("odate", "count")).reset_index()
    g["short_pl_profit"] = g["short_pl"]            # already profit (=-move)
    g["total_pl"] = g["long_pl"] + g["short_pl_profit"]
    if verbose:
        tot_l = g["long_pl"].sum(); tot_s = g["short_pl_profit"].sum()
        print(f"\n=== LOOP-ML capital sim (Rs1L/pick, Rs10L long + Rs10L short, daily churn) ===")
        print(f"  months={len(g)}  total LONG P&L=Rs{tot_l:,.0f}  total SHORT P&L=Rs{tot_s:,.0f}  "
              f"combined=Rs{tot_l+tot_s:,.0f}")
        print(f"  avg LONG basket open->close/day = {res['long_ret'].mean():+.3f}%  "
              f"avg SHORT basket = {res['short_ret'].mean():+.3f}% (profit if negative)")
    return g


def _archive(con, rows):
    con.execute("""CREATE TABLE IF NOT EXISTS loop_ml_signals(
        signal_date TEXT, outcome_date TEXT, direction TEXT, symbol TEXT,
        score REAL, actual_oc REAL, created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(signal_date,direction,symbol))""")
    con.execute("DELETE FROM loop_ml_signals")
    con.executemany(
        "INSERT OR REPLACE INTO loop_ml_signals(signal_date,outcome_date,direction,symbol,score,actual_oc) "
        "VALUES (?,?,?,?,?,?)", rows)
    con.commit()


# ---- captured % of baseline grid ----
MON = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]


def captured_vs_baseline(con, fo_universe, res: pd.DataFrame):
    """Monthly captured basket return vs the perfect-pick baseline, both sides, with
    milestone (% of baseline) for LONG and SHORT."""
    openf = pd.read_sql_query(
        "SELECT symbol,trade_date,oc_full FROM persona_open_features WHERE symbol IN (%s)"
        % ",".join("?"*len(fo_universe)), con, params=fo_universe).dropna(subset=["oc_full"])
    bl = []
    for dt, g in openf.groupby("trade_date"):
        if len(g) < 10:
            continue
        bl.append((dt[:7], g.nlargest(10, "oc_full")["oc_full"].mean(),
                   g.nsmallest(10, "oc_full")["oc_full"].mean()))
    b = pd.DataFrame(bl, columns=["ym", "bl_long", "bl_short"]).groupby("ym").mean().reset_index()
    m = res.groupby("ym").agg(cap_long=("long_ret", "mean"), cap_short=("short_ret", "mean")).reset_index()
    j = m.merge(b, on="ym", how="left")
    j["long_capture_pct"] = (j["cap_long"]/j["bl_long"]*100).round(0)
    j["short_capture_pct"] = (j["cap_short"]/j["bl_short"]*100).round(0)  # both neg -> +%
    return j


def _grid(j, col):
    j = j.copy(); j["year"] = j["ym"].str[:4]; j["mon"] = j["ym"].str[5:7].astype(int)
    g = j.pivot(index="year", columns="mon", values=col).reindex(columns=range(1, 13))
    g.columns = MON
    return g.round(2)


if __name__ == "__main__":
    from persona_engine import universe
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
    out = run_loop(con, fo, walk_start="2022-01-01", end=None, verbose=True)
    res = out["daily"]
    j = captured_vs_baseline(con, fo, res)
    print("\n=== LONG: captured avg open->close % per month ===")
    print(_grid(j, "cap_long").to_string())
    print("\n=== LONG: capture % of baseline ===")
    print(_grid(j, "long_capture_pct").to_string())
    print("\n=== SHORT: captured avg open->close % per month (neg=profit) ===")
    print(_grid(j, "cap_short").to_string())
    print("\n=== SHORT: capture % of baseline ===")
    print(_grid(j, "short_capture_pct").to_string())
    OUT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_findings")
    path = OUT / "LoopML_Captured_vs_Baseline.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        out["monthly"].to_excel(xl, sheet_name="monthly_pnl", index=False)
        _grid(j, "cap_long").to_excel(xl, sheet_name="LONG_captured_%")
        _grid(j, "long_capture_pct").to_excel(xl, sheet_name="LONG_capture_pct")
        _grid(j, "cap_short").to_excel(xl, sheet_name="SHORT_captured_%")
        _grid(j, "short_capture_pct").to_excel(xl, sheet_name="SHORT_capture_pct")
    print("\nXLSX ->", path)
    con.close()
    print("LOOPML_DONE")
