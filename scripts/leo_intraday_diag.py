"""
LEO INTRADAY DIAGNOSTIC — why does 'ride the morning hot sector to the close' lose? Two checks:
  1) ARTIFACT: is the negative intraday drift a 9:15 opening-auction print? Compare open915->close vs
     open920->close for the whole universe (and show overnight close->open for context).
  2) CONTINUATION vs REVERSAL: after the leader is confirmed at T=9:30, do its top-3 momentum names keep
     running (T->T+30, +60, ... ) or fade? Compare leader-top3 to the universe avg over each segment.
     If momentum continues for a short window then reverses, the right EXIT is short, not the close.
Run: python scripts/leo_intraday_diag.py
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import leo_intraday as LI

CHK = [920, 930, 940, 950, 1000, 1015, 1030, 1100, 1130, 1200]


def main():
    df = LI.load_cache()
    # ---- 1) drift / artifact check ----
    print("=== INTRADAY DRIFT (universe mean, no costs) ===")
    for oc in ["open", "p920", "p930", "p1000"]:
        r = (df["close"] / df[oc] - 1.0)
        r = r[np.isfinite(r)]
        print(f"  {oc:>6}->close : avg {r.mean()*100:+.3f}%/day   (x250 ~ {r.mean()*250*100:+.0f}%/yr)")
    # overnight vs intraday: need prev close -> today open per symbol
    df2 = df.sort_values(["symbol", "day"]).copy()
    df2["prev_close"] = df2.groupby("symbol")["close"].shift(1)
    on = (df2["open"] / df2["prev_close"] - 1.0); on = on[np.isfinite(on)]
    intr = (df2["close"] / df2["open"] - 1.0); intr = intr[np.isfinite(intr)]
    print(f"  overnight (prevclose->open): {on.mean()*100:+.3f}%/day    intraday(open->close): {intr.mean()*100:+.3f}%/day")
    print("  => if overnight >> intraday, the market's drift is OVERNIGHT; long-intraday fights a headwind.\n")

    # ---- 2) continuation vs reversal after leader confirmed at T=930 ----
    T = 930
    d = LI._prep_T(df, T)                     # has mom(open->T), srank
    lead = d[d["srank"] <= 1].copy()
    lead["prank"] = lead.groupby("day")["mom"].rank(ascending=False, method="first")
    picks = lead[lead["prank"] <= 3][["day", "sector"]].copy()
    picks_key = set(map(tuple, picks.values))
    # bring checkpoint prices for picked rows: rejoin on day+sector+ pick identity is messy; instead recompute
    # simplest: mark picked stock-days by index from lead
    picked_idx = lead[lead["prank"] <= 3].index
    base = df.loc[picked_idx]                 # the picked stock-days with all checkpoint columns
    print(f"=== CONTINUATION after leader confirmed at 9:30 (top sector, top-3 names, n={len(base)} stock-days) ===")
    print(f"  {'exit':>7}{'leader top3 ret from 9:30':>26}{'universe ret from 9:30':>26}{'momentum edge':>16}")
    exits = [1000, 1015, 1030, 1100, 1130, 1200, "close"]
    for E in exits:
        ecol = "close" if E == "close" else f"p{E}"
        lead_ret = (base[ecol] / base["p930"] - 1.0)
        lead_ret = lead_ret[np.isfinite(lead_ret)]
        uni = (df[ecol] / df["p930"] - 1.0); uni = uni[np.isfinite(uni)]
        edge = lead_ret.mean() - uni.mean()
        print(f"  {str(E):>7}{lead_ret.mean()*100:>+24.3f}%{uni.mean()*100:>+24.3f}%{edge*100:>+15.3f}%")
    print("\n  edge>0 & rising = momentum CONTINUES; edge<0 = morning leaders FADE (intraday reversal).")


if __name__ == "__main__":
    main()
