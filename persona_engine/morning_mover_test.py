"""
Test the operator's idea (2026-06-23): pre-filter to the morning's TOP MOVERS
(9:15->10:00 top gainers/losers), then look at what they do 10:00->15:15.
Do morning movers CONTINUE (gap-and-go) or FADE (reverse)? And can we pick 5 that
run >=2-3%?

Naive analysis first (rank purely by morning move), then a model that ranks WITHIN
the morning movers. ohlc_1min 2024-2026.
"""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db, universe
from persona_engine.intraday_eod import morning_features

def run(con, fo):
    mf = morning_features(con, fo)            # symbol,date,px1000,m_ret,post_move,...
    mf = mf.dropna(subset=["m_ret", "post_move"])
    print(f"rows={len(mf)} dates={mf['date'].nunique()} ({mf['date'].min()}..{mf['date'].max()})\n")

    # ---- NAIVE: rank by morning move; what do top-K morning gainers/losers do after 10:00? ----
    print("=== MORNING GAINERS (top-K by 9:15->10:00 move): what they do 10:00->15:15 ===")
    print("K   avg_post%  %cont(up)  %>=+1%  %>=+2%  %>=+3%   (per-day avg of K picks)")
    for K in (5, 10, 20):
        rows = []
        for dt, g in mf.groupby("date"):
            if len(g) < 30: continue
            top = g.nlargest(K, "m_ret")
            rows.append([top["post_move"].mean(), (top["post_move"] > 0).mean(),
                         (top["post_move"] >= 1).mean(), (top["post_move"] >= 2).mean(),
                         (top["post_move"] >= 3).mean()])
        a = np.array(rows).mean(axis=0)
        print(f"{K:<3d} {a[0]:+6.2f}    {a[1]*100:4.0f}%     {a[2]*100:4.0f}%   {a[3]*100:4.0f}%   {a[4]*100:4.0f}%")
    print("\n=== MORNING LOSERS (bottom-K): what they do 10:00->15:15 ===")
    print("K   avg_post%  %cont(dn)  %<=-1%  %<=-2%  %<=-3%")
    for K in (5, 10, 20):
        rows = []
        for dt, g in mf.groupby("date"):
            if len(g) < 30: continue
            bot = g.nsmallest(K, "m_ret")
            rows.append([bot["post_move"].mean(), (bot["post_move"] < 0).mean(),
                         (bot["post_move"] <= -1).mean(), (bot["post_move"] <= -2).mean(),
                         (bot["post_move"] <= -3).mean()])
        a = np.array(rows).mean(axis=0)
        print(f"{K:<3d} {a[0]:+6.2f}    {a[1]*100:4.0f}%     {a[2]*100:4.0f}%   {a[3]*100:4.0f}%   {a[4]*100:4.0f}%")

    # ---- top-5 morning movers as the literal pick (naive strategy) ----
    print("\n=== NAIVE STRATEGY: pick the 5 biggest morning movers each side ===")
    for side, asc, lab, thr_dir in [("LONG", False, "post_move", 1), ("SHORT", True, "post_move", -1)]:
        h2 = h3 = n = 0; p2 = []; p3 = []
        for dt, g in mf.groupby("date"):
            if len(g) < 30: continue
            picks = g.nlargest(5, "m_ret") if side == "LONG" else g.nsmallest(5, "m_ret")
            if side == "LONG":
                p2.append((picks["post_move"] >= 2).sum()); p3.append((picks["post_move"] >= 3).sum())
            else:
                p2.append((picks["post_move"] <= -2).sum()); p3.append((picks["post_move"] <= -3).sum())
        print(f"  {side}: of 5 biggest morning movers -> avg {np.mean(p2):.2f}/5 continue >=2%, "
              f"{np.mean(p3):.2f}/5 continue >=3%")
    con_ = con


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("MOVER_DONE")
