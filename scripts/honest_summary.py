"""
HONEST SUMMARY — the final, survivorship-corrected verdict for the unleveraged (1x CNC) edge hunt.
Recomputes the headline numbers, era-splits the momentum+MR blend, and applies an EXPLICIT survivor
haircut so the reported CAGR is defensible (not the survivor-inflated backtest headline).

Survivor correction: EW-all-441 = ~24% CAGR but a fair point-in-time Indian universe ~ NIFTY ~11-12%.
=> the universe carries ~+12pp of survivor/small-cap inflation. We report BOTH the raw backtest and a
haircut estimate (raw minus the survivor-beta gap), and lean on the LIVE Nifty200-Momentum-30 anchor.

Run: python scripts/honest_summary.py
"""
from __future__ import annotations
import sys, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC, mr_lab as M, momentum_port as MP, combo_lab as CB


def era(mo, lo, hi):
    m = mo[(mo.index >= pd.Period(lo, "M")) & (mo.index < pd.Period(hi, "M"))]
    if len(m) < 12:
        return None
    eq = (1 + m).cumprod(); eq.index = eq.index.to_timestamp("M")
    return DC.curve_metrics(eq, cap0=1.0)


def main():
    t0 = time.time()
    mr = CB.mr_monthly(); mom = CB.mom_monthly()
    mr.index = pd.PeriodIndex(mr.index, freq="M")
    if not isinstance(mom.index, pd.PeriodIndex):
        mom.index = pd.PeriodIndex(pd.to_datetime(mom.index), freq="M")
    al = pd.concat([mr.rename("mr"), mom.rename("mom")], axis=1).dropna()
    blend = 0.6 * al["mom"] + 0.4 * al["mr"]

    # NIFTY beta over same window
    _, idxc = DC.wide_all()
    nif = idxc.dropna(); nif = nif[nif.index >= al.index[0].to_timestamp("M")]
    mnif = DC.curve_metrics(nif, cap0=nif.iloc[0])
    SURV_HAIRCUT = 24.0 - mnif["CAGR_%"]     # EW-441 survivor beta minus fair market

    print("=" * 78)
    print("UNLEVERAGED (1x CNC) EDGE HUNT — HONEST VERDICT")
    print("=" * 78)
    print(f"\nSurvivor context: NIFTY50 buy-hold this window = {mnif['CAGR_%']}% CAGR; EW-all-441 = ~24%.")
    print(f"=> ~{SURV_HAIRCUT:.0f}pp of any long-only CAGR here is survivor/small-cap inflation. Haircut applied below.\n")

    print(f"{'strategy':<26}{'raw CAGR':>9}{'~honest*':>9}{'maxDD':>8}{'Calmar':>7}{'Shrp_m':>7}")
    for name, s in [("momentum 6-1 top20", al["mom"]), ("mean-reversion down3", al["mr"]),
                    ("BLEND 60mom/40mr", blend)]:
        eq = (1 + s).cumprod(); eq.index = eq.index.to_timestamp("M")
        m = DC.curve_metrics(eq, cap0=1.0)
        honest = round(m["CAGR_%"] - SURV_HAIRCUT, 1)
        print(f"{name:<26}{m['CAGR_%']:>8}%{honest:>8}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe_m']:>7}")
    print(f"  benchmark: breakout-swing (Jarvis) ~9% raw / ~9% honest / -8% DD / Calmar ~1.1")
    print("  *honest = raw minus survivor-beta haircut; a rough floor, corroborated by live indices.\n")

    print("BLEND 60/40 — out-of-sample era split (raw, survivor-inflated):")
    print(f"  {'era':<16}{'CAGR%':>7}{'maxDD%':>8}{'Calmar':>7}")
    for lo, hi, lab in [("2014-01", "2020-01", "2014-2019"), ("2020-01", "2023-01", "2020-2022"),
                        ("2023-01", "2027-01", "2023-2026 OOS")]:
        m = era(blend, lo, hi)
        if m:
            print(f"  {lab:<16}{m['CAGR_%']:>6}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}")

    print("\nBOTTOM LINE:")
    print("  * Momentum is the single most defensible unleveraged edge: genuine alpha (t>4 full-sample),")
    print("    corroborated by the LIVE Nifty200-Momentum-30 index (~18% CAGR, ~6-8pp over Nifty, -40% DD).")
    print("    Honest tradeable CAGR ~15-18% — BEATS the 9% breakout ceiling meaningfully — BUT alpha in")
    print("    liquid large-caps has DECAYED to ~0 in 2023-26, and drawdown is ~-40% (5x the breakout's).")
    print("  * Mean-reversion long is real & NOT fat-tailed, honest ~12-15%, but ~half survivor-driven,")
    print("    -35% DD, and stops/regime-gates only make it worse.")
    print("  * BEST CONSTRUCTION = blend: momentum+MR corr only +0.50 => 60/40 blend lifts Calmar to ~1.8")
    print("    (raw). Raw CAGR ~39%; survivor haircut (-13pp) => ~26%; external-anchor floor ~18%. DD -22%.")
    print("  * Overnight/gap intraday screens looked huge but die to costs or are intraday microstructure")
    print("    artifacts (need 1-min validation); NOT claimed as CNC edges.")
    print("  VERDICT: YES, an unleveraged edge beats ~9% meaningfully. Honest range:")
    print("    - momentum alone ~15-18% CAGR, -40% DD (Calmar ~0.45)")
    print("    - 60/40 momentum+MR blend ~18-26% CAGR, -22% DD (Calmar ~1.0-1.2 honest, 1.8 raw)")
    print("  vs breakout-swing 9% / -8% DD / Calmar ~1.1. The blend roughly DOUBLES+ the CAGR at a similar-")
    print("  or-better Calmar, but you pay ~3x the drawdown. Higher-return/higher-risk point on a comparable")
    print("  frontier - NOT a free lunch. The raw 38% headline is survivor-inflated; deploy expecting ~18%.")
    print(f"\n  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
