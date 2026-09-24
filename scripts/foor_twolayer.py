"""Phase 1.5 Stage A — two-layer market-direction upgrade.
LAYER 1: read the day's tape direction from the first 3 min (breadth + volume-weighted move
across the F&O universe). Classify DOWN / UP / UNCLEAR.
Then compare:
  Mode 1 (adaptive)  : DOWN->short basket, UP->long basket, UNCLEAR->flat.
  Mode 2 (short-gated): DOWN->short basket, else flat.
  Baseline (Phase-1) : short basket EVERY day (the fragile version).
THE test: split every result by market REGIME (up-months vs down-months) — does the edge now
hold in BOTH? Long & short reported separately. Walk-forward stock scores (cached).
"""
import pickle
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
N = 5


def main():
    d = pickle.load(open(ROOT / "docs" / "ops" / "_foor_scored.pkl", "rb"))
    # ---- LAYER 1: per-day market-direction read (first 3 min) ----
    day = d.groupby("date").apply(lambda g: pd.Series({
        "breadth": (g.drive18 > 0).mean(),
        "vwmove": (g.drive18 * g.v18).sum() / max(g.v18.sum(), 1),
        "agg_rod": g.rod.mean(),                      # market's rest-of-day (what we'd trade)
        "mkt_c2c": (g.eod / g.prev_C - 1).mean(),     # market close-to-close (for regime)
    })).reset_index()
    lo, hi = day.vwmove.quantile(0.34), day.vwmove.quantile(0.66)
    day["tape"] = np.where(day.vwmove <= lo, "DOWN", np.where(day.vwmove >= hi, "UP", "UNCLEAR"))
    print(f"days: {len(day)} | tape split: " + ", ".join(f"{k} {v}" for k, v in day.tape.value_counts().items()))

    # ---- Q1: does the Layer-1 read predict the day's tradeable direction? ----
    ic = spearmanr(day.vwmove, day.agg_rod).correlation
    print(f"\nLAYER 1 validation: corr(vwmove, market rest-of-day) = {ic:+.3f}")
    print(f"{'tape':<9}{'days':>6}{'mkt rest-of-day%':>18}{'% days mkt down':>18}")
    for t in ["DOWN", "UNCLEAR", "UP"]:
        s = day[day.tape == t]
        print(f"{t:<9}{len(s):>6}{s.agg_rod.mean()*100:>17.3f}{(s.agg_rod<0).mean()*100:>17.0f}%")

    # ---- regime: market trend index -> up/down month ----
    day = day.sort_values("date")
    day["mkt_idx"] = (1 + day.mkt_c2c.fillna(0)).cumprod()
    day["ym"] = day.date.str[:7]
    mret = day.groupby("ym").mkt_c2c.sum()
    up_months = set(mret[mret > 0].index); dn_months = set(mret[mret <= 0].index)
    tape_map = dict(zip(day.date, day.tape)); regime_map = {m: ("UP-mo" if m in up_months else "DOWN-mo") for m in mret.index}
    print(f"\nregime: {len(up_months)} up-months, {len(dn_months)} down-months")

    # ---- baskets per day ----
    daily = []
    for dt, g in d.groupby("date"):
        if len(g) < 20:
            continue
        tape = tape_map.get(dt, "UNCLEAR")
        short_cap = -g.nlargest(N, "p_short").rod.mean() * 100
        long_cap = g.nlargest(N, "p_long").rod.mean() * 100
        m1 = short_cap if tape == "DOWN" else long_cap if tape == "UP" else 0.0
        m1_traded = tape in ("DOWN", "UP")
        m2 = short_cap if tape == "DOWN" else 0.0
        m2_traded = tape == "DOWN"
        daily.append(dict(date=dt, ym=dt[:7], regime=regime_map.get(dt[:7], "?"), tape=tape,
                          short_cap=short_cap, long_cap=long_cap,
                          m1=m1, m1_traded=m1_traded, m2=m2, m2_traded=m2_traded))
    bt = pd.DataFrame(daily)

    def stat(x):
        x = np.asarray(x, float)
        return f"mean {x.mean():+.3f}%  %pos {(x>0).mean()*100:.0f}%  n={len(x)}"

    print("\n================ MODE COMPARISON (N=%d, capture per trading day) ================" % N)
    print(f"  BASELINE short-every-day : {stat(bt.short_cap)}")
    print(f"  MODE 2 short-gated (DOWN): {stat(bt[bt.m2_traded].m2)}   [flat {int((~bt.m2_traded).sum())} days]")
    print(f"  MODE 1 adaptive (traded) : {stat(bt[bt.m1_traded].m1)}   [flat {int((~bt.m1_traded).sum())} days]")
    print(f"    - MODE1 LONG legs (UP-tape) : {stat(bt[bt.tape=='UP'].long_cap)}")
    print(f"    - MODE1 SHORT legs (DOWN)   : {stat(bt[bt.tape=='DOWN'].short_cap)}")

    print("\n================ THE ROBUSTNESS TEST — by market regime ================")
    print(f"{'':<26}{'UP-months':>26}{'DOWN-months':>26}")
    def by_regime(mask_traded, capcol, label):
        u = bt[(bt.regime == 'UP-mo') & mask_traded][capcol]; dn = bt[(bt.regime == 'DOWN-mo') & mask_traded][capcol]
        print(f"{label:<26}{stat(u):>26}{stat(dn):>26}")
    by_regime(pd.Series(True, index=bt.index), "short_cap", "BASELINE short-always")
    by_regime(bt.m2_traded, "m2", "MODE 2 short-gated")
    by_regime(bt.m1_traded, "m1", "MODE 1 adaptive")
    print("\nKEY: if BASELINE loses/weakens in UP-months but MODE1/2 stay positive across BOTH,")
    print("     the down-drift fragility is removed. LONG & SHORT legs shown separately above.")


if __name__ == "__main__":
    main()
