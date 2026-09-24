# -*- coding: utf-8 -*-
"""Full January-2026 beta-adjusted validation of THE RING. Every trading day, every 30-min checkpoint:
generate Buy/Short candidates, compute their forward move to close, and subtract the MARKET average
(all Nifty-50 stocks) to isolate SELECTION from beta. Buy_edge = buy_fwd - market; Short_edge =
market - short_fwd (positive = candidate fell more than the average stock)."""
import os, sqlite3
import numpy as np, pandas as pd
from ndp import ring
MDB = ring.MDB
CKPTS = ["09:45", "10:15", "10:45", "11:15", "11:45", "12:15", "12:45", "13:15", "13:45"]


def jan_days():
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    d = pd.read_sql_query("SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min WHERE bar_time LIKE '2026-01-%' ORDER BY d", con).d.tolist(); con.close()
    return d


def candidates(B):
    buys = B[(B.mv > 0) & (B.state.isin(["IGNITING", "TRENDING"])) & (B.room > 0.25) & (B.cont >= 60)]
    shorts = B[(B.mv < 0) & (B.state.isin(["IGNITING", "TRENDING"])) & (B.room > 0.25) & (B.cont >= 60)]
    return buys, shorts


def main():
    syms = [s for s in ring.W]
    base = ring.baselines(syms); syms = [s for s in syms if s in base]
    days = jan_days()
    print(f"THE RING — beta-adjusted validation  ·  {len(days)} January-2026 days  ·  {len(syms)} stocks  ·  {len(CKPTS)} checkpoints/day")
    rows = []
    for day in days:
        b = ring.load_day(syms, day)
        if b.empty: continue
        close_px = b.sort_values("bar_time").groupby("symbol").close.last().to_dict()
        idx_close = None
        for ts in CKPTS:
            B = ring.board(b, base, ts)
            if B.empty: continue
            B = B[B.stock.isin(close_px)]
            B["fwd"] = [(close_px[r.stock]/r.px-1)*100 for _, r in B.iterrows()]
            mkt = B.fwd.mean()                      # market beta = avg forward move of all stocks
            buys, shorts = candidates(B)
            bf = buys.fwd.mean() if len(buys) else np.nan
            sf = shorts.fwd.mean() if len(shorts) else np.nan
            rows.append(dict(day=day, ts=ts, mkt=mkt, n_buy=len(buys), buy_fwd=bf,
                             n_short=len(shorts), short_fwd=sf,
                             buy_edge=bf-mkt if bf == bf else np.nan,
                             short_edge=mkt-sf if sf == sf else np.nan))
    R = pd.DataFrame(rows)
    print(f"\n{'='*84}\nBETA-ADJUSTED EDGE (candidate forward move minus the average Nifty-50 stock)\n{'='*84}")
    be = R.buy_edge.dropna(); se = R.short_edge.dropna()
    print(f"  BUY  edge: mean {be.mean():+.3f}%  ·  positive {(be>0).mean()*100:.0f}% of {len(be)} obs  ·  median {be.median():+.3f}%")
    print(f"  SHORT edge: mean {se.mean():+.3f}%  ·  positive {(se>0).mean()*100:.0f}% of {len(se)} obs  ·  median {se.median():+.3f}%")
    print(f"  (raw: BUY cands {R.buy_fwd.mean():+.2f}% vs mkt {R.mkt.mean():+.2f}%  ·  SHORT cands {R.short_fwd.mean():+.2f}% vs mkt {R.mkt.mean():+.2f}%)")
    print(f"\n{'─'*84}\nBY DAY (avg edge across checkpoints):")
    print(f"  {'day':<12}{'mkt%':>7}{'n_buy':>7}{'buy_edge':>10}{'n_short':>8}{'short_edge':>12}")
    for day, g in R.groupby("day"):
        print(f"  {day:<12}{g.mkt.mean():>+7.2f}{int(g.n_buy.mean()):>7}{g.buy_edge.mean():>+10.3f}{int(g.n_short.mean()):>8}{g.short_edge.mean():>+12.3f}")
    R.to_csv(os.path.expanduser("~")+"/Downloads/RING_JAN2026_VALIDATION.csv", index=False)
    # verdict
    print(f"\n{'='*84}")
    bpos = (be > 0).mean(); spos = (se > 0).mean()
    combo = pd.concat([be, se])
    print(f"VERDICT: combined selection edge mean {combo.mean():+.3f}%/checkpoint, positive {(combo>0).mean()*100:.0f}% of the time")
    if combo.mean() > 0.05 and (combo > 0).mean() > 0.55:
        print("  -> The Ring shows SELECTION skill beyond beta (candidates beat the average stock).")
    elif abs(combo.mean()) <= 0.05:
        print("  -> No selection edge beyond beta: the candidates move ~with the market (it was mostly beta).")
    else:
        print("  -> Selection is NEGATIVE vs beta (candidates underperform the average stock).")
    print("saved -> ~/Downloads/RING_JAN2026_VALIDATION.csv")


if __name__ == "__main__":
    main()
