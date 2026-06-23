"""
Disciplined experiments on block_features (operator spec):
 - Stage-1: each day take Top-20 candidates by move-potential (long: morning strength+vol;
   short: morning weakness+vol).
 - Stage-2: multi-ML stack ranks the 20 -> Top-5.
 - Measure: of Top-5, how many achieve +/-1/2/3% (10:00->15:15) by TOUCH (reached) and CLOSE.
 - Multi-split stability on 2024 (50/50, 60/40, 70/30, 80/20) then walk-forward 2025+.
 - Closed-loop explainable output: winners' vs losers' patterns, missed movers, learning.

Usage:
  python -m persona_engine.block_experiments splits   # 2024 multi-split stability
  python -m persona_engine.block_experiments wf        # walk-forward 2025+
"""
from __future__ import annotations
import sys
import numpy as np, pandas as pd
from persona_engine import db
from persona_engine.combined_model import stack

NPICK, NCAND = 5, 20
DROP = {"symbol", "date", "sector", "up_touch", "dn_touch", "ret_close", "px_1000"}


def load():
    con = db.connect(read_only=True)
    df = pd.read_sql_query("SELECT * FROM block_features", con); con.close()
    df["year"] = df["date"].str[:4]
    return df


def feats(df):
    return [c for c in df.columns if c not in DROP and c != "year" and df[c].dtype != object]


def stage1_score(g, direction):
    # move-potential candidate score: morning directional strength + volatility
    s = (g["b3_ret"].rank(pct=True) if direction == "LONG" else (-g["b3_ret"]).rank(pct=True))
    return (s + g["morning_vol"].rank(pct=True) + g.get("rel_mkt", g["b3_ret"]).rank(pct=True) *
            (1 if direction == "LONG" else -1)).rank(ascending=False)


def evaluate(train, test, direction, FE):
    col = "up_touch" if direction == "LONG" else "dn_touch"
    lab = ((train[col] >= 1) if direction == "LONG" else (train[col] <= -1)).astype(int).values
    p = stack(train[FE].replace([np.inf, -np.inf], np.nan), lab,
              test[FE].replace([np.inf, -np.inf], np.nan))
    t = test.copy(); t["p"] = p
    res = {1: [], 2: [], 3: []}; resC = {1: [], 2: [], 3: []}
    picks_all = []
    for dt, g in t.groupby("date"):
        if len(g) < 25:
            continue
        g = g.copy(); g["s1"] = stage1_score(g, direction)
        cand = g[g["s1"] <= NCAND]
        pk = cand.sort_values("p", ascending=False).head(NPICK)
        for X in (1, 2, 3):
            if direction == "LONG":
                res[X].append((pk["up_touch"] >= X).sum()); resC[X].append((pk["ret_close"] >= X).sum())
            else:
                res[X].append((pk["dn_touch"] <= -X).sum()); resC[X].append((pk["ret_close"] <= -X).sum())
        picks_all.append(pk.assign(date=dt))
    out = {f"touch{X}": round(np.mean(v), 2) for X, v in res.items()}
    out.update({f"close{X}": round(np.mean(v), 2) for X, v in resC.items()})
    return out, pd.concat(picks_all) if picks_all else pd.DataFrame()


def run_splits():
    df = load(); FE = feats(df)
    d24 = df[df["year"] == "2024"].sort_values("date")
    dates = sorted(d24["date"].unique())
    print(f"2024 intraday days available: {len(dates)} ({dates[0]}..{dates[-1]}); features={len(FE)}\n")
    splits = {"50/50": 0.5, "60/40": 0.6, "70/30": 0.7, "80/20": 0.8}
    for direction in ("LONG", "SHORT"):
        print(f"================= {direction} =================")
        print("split   touch+1  touch+2  touch+3  | close+1 close+2 close+3")
        for name, frac in splits.items():
            cut = int(len(dates)*frac)
            tr = d24[d24["date"].isin(dates[:cut])]; te = d24[d24["date"].isin(dates[cut:])]
            o, _ = evaluate(tr, te, direction, FE)
            print(f"{name:6s}  {o['touch1']:>5}   {o['touch2']:>5}   {o['touch3']:>5}   |  "
                  f"{o['close1']:>5}  {o['close2']:>5}  {o['close3']:>5}", flush=True)
        print()


def run_wf():
    df = load(); FE = feats(df)
    print(f"WALK-FORWARD 2025+ (train on all prior, test the year). features={len(FE)}\n")
    for direction in ("LONG", "SHORT"):
        print(f"--- {direction} ---")
        for ty in ("2025", "2026"):
            tr = df[df["year"] < ty]; te = df[df["year"] == ty]
            if len(tr) < 3000 or len(te) < 300:
                continue
            o, picks = evaluate(tr, te, direction, FE)
            print(f"{ty}: Top-5 touch +1%={o['touch1']}  +2%={o['touch2']}  +3%={o['touch3']}  | "
                  f"close +1%={o['close1']}  +2%={o['close2']}", flush=True)
            # learning: winners vs losers feature contrast (top-5 only)
            col = "up_touch" if direction == "LONG" else "dn_touch"
            picks["win"] = (picks[col] >= 1) if direction == "LONG" else (picks[col] <= -1)
            contrast = []
            for f in ["accel", "vol_expand", "compression", "breakout_up", "breakout_dn",
                      "trend_consist", "morning_vol", "rel_mkt", "mkt_breadth", "b3_vwapdev"]:
                if f in picks:
                    w = picks[picks["win"]][f].mean(); l = picks[~picks["win"]][f].mean()
                    contrast.append((f, round(w, 3), round(l, 3)))
            print(f"   winners vs losers (feature, win_avg, loss_avg):")
            for f, w, l in contrast:
                print(f"     {f:14s} win={w:>7} loss={l:>7}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "splits"
    if mode == "splits":
        run_splits()
    else:
        run_wf()
    print("EXP_DONE")
