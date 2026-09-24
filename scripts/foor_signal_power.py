"""Phase 1 go/no-go: do the opening signals predict rest-of-day DIRECTION?
Walk-forward per-stock baselines (no lookahead: z-scores use each stock's TRAILING history,
shifted). Signals kept directionally explicit; sweep-REJECT vs breakout-HOLD separated.
Everything reported SEPARATELY for LONG (capture rod>=+0.5%) and SHORT (rod<=-0.5%).
Primary checkpoint 09:20; also compares 09:18 / 09:22.
"""
import pickle
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
PANEL = ROOT / "docs" / "ops" / "_foor_panel.pkl"
TGT = 0.005    # 0.5% capture threshold


def build(df, H, L, V, C):
    d = df.copy()
    d = d.sort_values(["symbol", "date"])
    g = d.groupby("symbol")
    d["prev_C"] = g.eod.shift(1); d["prev_H"] = g.fullH.shift(1); d["prev_L"] = g.fullL.shift(1)
    d["orpct"] = (d[H] - d[L]) / d[C]
    d["logv"] = np.log(d[V].clip(lower=1))
    # walk-forward per-stock baselines (trailing 60d, shifted -> no lookahead)
    for col, base in [("orpct", "orpct"), ("logv", "logv")]:
        m = g[col].transform(lambda s: s.shift(1).rolling(60, min_periods=20).mean())
        sd = g[col].transform(lambda s: s.shift(1).rolling(60, min_periods=20).std())
        d[f"z_{base}"] = (d[col] - m) / sd
    d["z_contraction"] = -d["z_orpct"]     # high = tighter than the stock's norm (contraction)
    d["z_dryup"] = -d["z_logv"]            # high = quieter than the stock's norm (dry-up)
    d["drive"] = d[C] / d.o0915 - 1        # opening drive direction
    d["or_pos"] = (d[C] - d[L]) / (d[H] - d[L]).replace(0, np.nan)   # location in opening range
    d["gap"] = d.o0915 / d.prev_C - 1
    d["sweep_rej_up"] = ((d[H] > d.prev_H) & (d[C] < d.prev_H)).astype(int)   # bearish (upside rejected)
    d["sweep_rej_dn"] = ((d[L] < d.prev_L) & (d[C] > d.prev_L)).astype(int)   # bullish (downside rejected)
    d["brk_up"] = ((d[H] > d.prev_H) & (d[C] > d.prev_H)).astype(int)          # bullish continuation
    d["brk_dn"] = ((d[L] < d.prev_L) & (d[C] < d.prev_L)).astype(int)          # bearish continuation
    d["rod"] = d.eod / d[C] - 1                                                # rest-of-day (target)
    return d.dropna(subset=["rod", "prev_C"])


def report(d, label):
    print(f"\n================ {label} (n={len(d):,} stock-days) ================")
    print("Base rates: rod mean %+.3f | P(long +0.5%%) %.1f%% | P(short -0.5%%) %.1f%%" %
          (d.rod.mean() * 100, (d.rod >= TGT).mean() * 100, (d.rod <= -TGT).mean() * 100))
    # continuous signals: Spearman IC with rod + directional hit-rates by extreme quintile
    print(f"\n{'signal':<16}{'IC(rod)':>9}{'Q1 rod%':>9}{'Q5 rod%':>9}{'Q5 long%':>9}{'Q1 short%':>10}")
    for sig in ["drive", "or_pos", "gap", "z_contraction", "z_dryup"]:
        s = d[[sig, "rod"]].dropna()
        ic = spearmanr(s[sig], s.rod).correlation
        try:
            s["q"] = pd.qcut(s[sig].rank(method="first"), 5, labels=False)
        except Exception:
            continue
        q1, q5 = s[s.q == 0], s[s.q == 4]
        print(f"{sig:<16}{ic:>+9.3f}{q1.rod.mean()*100:>9.3f}{q5.rod.mean()*100:>9.3f}"
              f"{(q5.rod>=TGT).mean()*100:>9.1f}{(q1.rod<=-TGT).mean()*100:>10.1f}")
    # binary sweep/breakout: mean rod flagged vs base
    print(f"\n{'flag':<16}{'n':>7}{'rod% flag':>11}{'rod% base':>11}{'long% flag':>11}{'short% flag':>12}")
    base_long = (d.rod >= TGT).mean() * 100; base_short = (d.rod <= -TGT).mean() * 100
    for sig in ["sweep_rej_up", "sweep_rej_dn", "brk_up", "brk_dn"]:
        f = d[d[sig] == 1]
        if len(f) < 30: continue
        print(f"{sig:<16}{len(f):>7}{f.rod.mean()*100:>11.3f}{d.rod.mean()*100:>11.3f}"
              f"{(f.rod>=TGT).mean()*100:>11.1f}{(f.rod<=-TGT).mean()*100:>12.1f}")
    print(f"   (baseline: long {base_long:.1f}% | short {base_short:.1f}%)")


def main():
    df = pickle.load(open(PANEL, "rb"))
    print(f"panel: {len(df):,} stock-days | {df.symbol.nunique()} stocks | {df.date.min()}..{df.date.max()}")
    for cp, (H, L, V, C) in [("09:20", ("H20", "L20", "V20", "C20")),
                             ("09:18", ("H18", "L18", "V18", "C18")),
                             ("09:22", ("H22", "L22", "V22", "C22"))]:
        d = build(df, H, L, V, C)
        report(d, f"CHECKPOINT {cp}")


if __name__ == "__main__":
    main()
