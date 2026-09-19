"""
COMBO + GAP SCREEN.
(A) BLEND: momentum (buys strength) and mean-reversion (buys weakness) are timing-opposite => a 50/50
    blend of their return streams should cut drawdown hard while keeping most of the return. Test it,
    report correlation and the blended Calmar vs each alone.
(B) GAP screen: quick close->open overnight edge (does buying at close and selling next open pay?),
    and a gap-down-reversal variant. Kept lightweight; deepened only if it rivals momentum/MR.

Run: python scripts/combo_lab.py
"""
from __future__ import annotations
import sys, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC, mr_lab as M, momentum_port as MP


def mr_monthly():
    panel = DC.load_panel(); panel = panel[panel.symbol != DC.INDEX]
    D = {s: M.prep(g.sort_values("date").reset_index(drop=True)) for s, g in panel.groupby("symbol", sort=False)}
    tr = M.sim_trades(D, "down3", "tgt4")
    _, eq = DC.slot_fund([(a, b, c) for a, b, c, _ in tr], 20)
    mo = eq.resample("ME").last()
    return mo.pct_change(fill_method=None).dropna()


def mom_monthly():
    o, c, dvol20, ma200, idxc, idx_ma200, dates, me = MP.build_matrices()
    s, _ = MP.run(o, c, dvol20, ma200, idxc, idx_ma200, dates, me, lb="6_1", N=20)
    s.index = pd.to_datetime(s.index)
    return s.resample("ME").sum() if False else s.groupby(pd.PeriodIndex(s.index, freq="M")).apply(lambda x: (1 + x).prod() - 1)


def report(mo, label):
    eq = (1 + mo).cumprod()
    eq.index = pd.to_datetime([str(p) if not hasattr(p, "to_timestamp") else p.to_timestamp("M") for p in eq.index])
    m = DC.curve_metrics(eq, cap0=1.0)
    print(f"  {label:<22}CAGR {m['CAGR_%']:>6}%   DD {m['maxDD_%']:>6}%   Calmar {str(m['calmar']):>5}   Sharpe_m {m['sharpe_m']:>5}")
    return m


def gap_screen():
    fields, idxc = DC.wide_all()
    o, c, v = fields["o"], fields["c"], fields["v"]
    dvol20 = (c * v).rolling(20).mean()
    liq = dvol20 > MP.MIN_DVOL
    pc = c.shift(1)
    # overnight: buy every eligible name at close t-1, sell at open t. mean across names per day.
    on = (o / pc - 1.0)
    on = on.where(liq.shift(1) & (pc > MP.MIN_PRICE))
    on_daily = on.mean(axis=1).dropna()
    # gap-down reversal: names that gapped down >2% at open -> buy at open, sell at close same day
    gd = (o / pc - 1.0) < -0.02
    intraday = (c / o - 1.0).where(gd & liq.shift(1) & (pc > MP.MIN_PRICE))
    gdr_daily = intraday.mean(axis=1).dropna()
    print("(B) GAP / OVERNIGHT screen (gross, before costs; costs ~0.20-0.30% would erase thin edges):")
    for name, d in [("overnight close->open", on_daily), ("gap-down intraday reversal", gdr_daily)]:
        ann = d.mean() * 252 * 100
        tstat = d.mean() / d.std() * np.sqrt(len(d)) if d.std() > 0 else 0
        print(f"   {name:<28} mean/day {d.mean()*100:+.3f}%  ann {ann:+.1f}%  t={tstat:.1f}  "
              f"(needs > ~50%/yr gross to beat momentum after costs)")


def main():
    t0 = time.time()
    print("=== (A) BLEND momentum + mean-reversion (monthly return streams) ===")
    mr = mr_monthly(); mom = mom_monthly()
    mr.index = pd.PeriodIndex(mr.index, freq="M");
    if not isinstance(mom.index, pd.PeriodIndex):
        mom.index = pd.PeriodIndex(pd.to_datetime(mom.index), freq="M")
    al = pd.concat([mr.rename("mr"), mom.rename("mom")], axis=1).dropna()
    corr = al["mr"].corr(al["mom"])
    print(f"  monthly return correlation(MR, MOM) = {corr:+.2f}   (negative/low => strong diversification)\n")
    m_mr = report(al["mr"], "mean-reversion only")
    m_mom = report(al["mom"], "momentum only")
    blend = 0.5 * al["mr"] + 0.5 * al["mom"]
    m_bl = report(blend, "50/50 blend")
    # vol-target-ish: 60 mom / 40 mr
    for wm in [0.4, 0.6, 0.7]:
        report(wm * al["mom"] + (1 - wm) * al["mr"], f"{int(wm*100)}mom/{int((1-wm)*100)}mr blend")
    print()
    gap_screen()
    print(f"\n  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
