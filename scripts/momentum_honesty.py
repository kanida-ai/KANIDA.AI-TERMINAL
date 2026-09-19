"""
MOMENTUM HONESTY DECOMPOSITION. The 40%+ CAGR of top-N momentum on the 441 survivor-universe is
almost entirely SURVIVOR BETA, not a tradeable momentum alpha. This proves it by decomposing:

  EW-all      = equal-weight ALL eligible names, monthly rebal = the survivor universe's OWN return.
  top-N       = momentum longs.        bottom-N = momentum losers (same biased universe).
  spread      = top-N - bottom-N       = survivorship-ROBUST momentum signal (both legs share the bias).
  top - EWall = momentum's MARGINAL contribution over just holding the (biased) universe.

If EW-all is itself ~25-30% CAGR, the universe is the artifact; the honest momentum edge = the SPREAD
and the top-minus-EW margin, NOT the headline 40%. A real unleveraged investor in a survivor-free
universe would earn far less. We report the margin and spread as the defensible numbers.

Run: python scripts/momentum_honesty.py
"""
from __future__ import annotations
import sys, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC
import momentum_port as MP


def run_bucket(o, c, dvol20, dates, me, lb, which, N=20, cost_side=MP.COST_SIDE):
    """which in {'top','bottom','ew'}. Returns monthly net return series."""
    pos_index = {d: i for i, d in enumerate(dates)}
    entries = []
    for mdate in me:
        i = pos_index.get(pd.Timestamp(mdate))
        if i is None or i + 1 >= len(dates):
            continue
        entries.append((i, i + 1))
    rets = {}; prev = set()
    for j in range(len(entries) - 1):
        sig_i, ent_i = entries[j]; nxt_ent_i = entries[j + 1][1]
        mom = MP.momentum_signal(c, sig_i, lb)
        if mom is None:
            continue
        elig = (c.iloc[sig_i] > MP.MIN_PRICE) & (dvol20.iloc[sig_i] > MP.MIN_DVOL) & mom.notna()
        m = mom[elig]
        if len(m) < 2 * N:
            continue
        ranked = m.sort_values(ascending=False)
        if which == "top":
            picks = list(ranked.index[:N])
        elif which == "bottom":
            picks = list(ranked.index[-N:])
        else:  # ew = all eligible
            picks = list(ranked.index)
        oe = o.iloc[ent_i][picks].values.astype(float)
        ox = o.iloc[nxt_ent_i][picks].values.astype(float)
        valid = (oe > 0) & (ox > 0)
        r = np.where(valid, ox / oe - 1.0, 0.0)
        gross = r.mean()
        held = set(picks)
        nn = len(picks)
        enter_frac = len(held - prev) / nn if nn else 0
        exit_frac = len(prev - held) / max(len(prev), 1) if prev else (1 if held else 0)
        cost = (enter_frac + exit_frac) * cost_side
        rets[dates[ent_i]] = gross - cost
        prev = held
    return pd.Series(rets).sort_index()


def cagr_of(s):
    m, _ = MP.fund_from_monthly(s)
    return m


def main():
    t0 = time.time()
    o, c, dvol20, ma200, idxc, idx_ma200, dates, me = MP.build_matrices()
    print(f"loaded. survivor universe = {c.shape[1]} names.  [{time.time()-t0:.0f}s]\n")

    for lb in ["6_1", "12_1"]:
        print(f"=== lookback {lb} (N=20) ===")
        top = run_bucket(o, c, dvol20, dates, me, lb, "top", 20)
        bot = run_bucket(o, c, dvol20, dates, me, lb, "bottom", 20)
        ew = run_bucket(o, c, dvol20, dates, me, lb, "ew", 20)
        mt, mb, mw = cagr_of(top), cagr_of(bot), cagr_of(ew)
        print(f"  {'bucket':<26}{'CAGR%':>7}{'maxDD%':>8}{'Calmar':>7}{'Shrp_m':>7}")
        print(f"  {'EW-all (survivor beta)':<26}{mw['CAGR_%']:>6}%{mw['maxDD_%']:>7}%{str(mw['calmar']):>7}{mw['sharpe_m']:>7}")
        print(f"  {'top-N momentum':<26}{mt['CAGR_%']:>6}%{mt['maxDD_%']:>7}%{str(mt['calmar']):>7}{mt['sharpe_m']:>7}")
        print(f"  {'bottom-N (losers)':<26}{mb['CAGR_%']:>6}%{mb['maxDD_%']:>7}%{str(mb['calmar']):>7}{mb['sharpe_m']:>7}")
        # margin: top monthly minus EW monthly (aligned)
        al = pd.concat([top.rename("t"), ew.rename("w"), bot.rename("b")], axis=1).dropna()
        marg = al["t"] - al["w"]                    # momentum's monthly excess over the biased universe
        spread = al["t"] - al["b"]                  # long-short momentum spread
        ann_marg = marg.mean() * 12 * 100
        ann_spread = spread.mean() * 12 * 100
        # t-stats
        tm = marg.mean() / marg.std() * np.sqrt(len(marg))
        ts = spread.mean() / spread.std() * np.sqrt(len(spread))
        print(f"  --> top MINUS EW-all margin: {ann_marg:+.1f}%/yr (t={tm:.1f})   "
              f"long-short SPREAD: {ann_spread:+.1f}%/yr (t={ts:.1f})")
        print(f"      => of top-N's {mt['CAGR_%']}% CAGR, ~{mw['CAGR_%']}pp is survivor-universe beta; "
              f"momentum's own margin ~{ann_marg:.0f}%/yr.\n")

    # How biased is the universe? EW-all vs NIFTY, and how many names are up-only survivors
    ew = run_bucket(o, c, dvol20, dates, me, "6_1", "ew", 20)
    mw = cagr_of(ew)
    if idxc is not None:
        bh = idxc.dropna(); bh = bh[(bh.index >= ew.index[0])]
        mbh = DC.curve_metrics(bh, cap0=bh.iloc[0])
        print(f"SURVIVOR-BETA MAGNITUDE: EW-all-441 = {mw['CAGR_%']}% CAGR vs NIFTY50 buy-hold {mbh['CAGR_%']}% "
              f"=> the universe alone carries {round(mw['CAGR_%']-mbh['CAGR_%'],1)}pp of survivor/small-cap beta.")
    print(f"\n  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
