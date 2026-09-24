"""ROBUSTNESS — tail-short (rank 201-500) to Falcon's standard, across 2022-2026 separately + the down-phase test.
Q1: per year — tail-short edge/trade, corr to Falcon, Falcon+tail-short return/DD vs Falcon-alone. (2022 = down phase.)
Q2: THE test — does the -0.26 corr HOLD in Falcon's worst months, or collapse toward +1 when the hedge is needed?
    Report corr in down-months + the tail-short's actual P&L in those months (a real hedge must PROFIT when Falcon bleeds).
OOS-consistent metrics; read-only; Falcon untouched."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
LEV, FRIC = 5, 0.15
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
df = pd.read_sql_query("SELECT signal_date, rank, nd_intraday_ret FROM falcon_full_ranking WHERE nd_intraday_ret IS NOT NULL", rc)
rc.close()
df["yr"] = df.signal_date.str[:4]; df["ym"] = df.signal_date.str[:7]
cost_d = FRIC / 100 * LEV / 20
def daily_series(sub_all):
    fal = sub_all[sub_all["rank"] <= 10].groupby("signal_date").nd_intraday_ret.mean() / 100 * LEV - cost_d
    tail = (-sub_all[sub_all["rank"] >= 201].groupby("signal_date").nd_intraday_ret.mean()) / 100 * LEV - cost_d
    idx = sorted(set(fal.index) & set(tail.index)); return fal[idx], tail[idx]
def rdd(s): t = s.sum() * 100; c = s.cumsum(); d = (c.cummax() - c).max() * 100; return t, d, (t / d if d else 0)

print("=" * 92 + "\nPER-YEAR ROBUSTNESS (Falcon = Top-10 long 5x; tail-short = rank 201-500 short 5x)\n" + "=" * 92)
print(f"{'year':<6}{'univ base%':>11}{'tail edge/tr':>13}{'corr(F,tail)':>13}{'Falcon r/DD':>12}{'F+30%tail r/DD':>15}{'DD: alone→combo':>17}")
for y in ["2022", "2023", "2024", "2025", "2026"]:
    sub = df[df.yr == y]
    fal, tail = daily_series(sub)
    ubase = sub.nd_intraday_ret.mean()
    tail_edge = (-sub[sub["rank"] >= 201].nd_intraday_ret).mean()
    corr = np.corrcoef(fal, tail)[0, 1]
    ft, fd, frdd = rdd(fal); comb = 0.7 * fal + 0.3 * tail; ct, cd, crdd = rdd(comb)
    down = " (DOWN)" if ubase < -0.05 else ""
    print(f"{y+down:<6}{ubase:>+11.3f}{tail_edge:>+12.3f}%{corr:>+13.2f}{frdd:>12.2f}{crdd:>15.2f}{f'{fd:.0f}%→{cd:.0f}%':>17}")

print("\n" + "=" * 92 + "\nDOWN-PHASE TEST — does the hedge HOLD when Falcon bleeds? (all years pooled)\n" + "=" * 92)
fal, tail = daily_series(df)
falm = fal.groupby(fal.index.str[:7]).sum() * 100; tailm = tail.groupby(tail.index.str[:7]).sum() * 100
corr_all = np.corrcoef(fal, tail)[0, 1]
down_months = falm[falm < 0].index
up_months = falm[falm >= 0].index
dd_mask = fal.index.str[:7].isin(down_months)
print(f"correlation(Falcon, tail-short) daily  — ALL: {corr_all:+.2f}")
print(f"                                       — in Falcon's DOWN-months: {np.corrcoef(fal[dd_mask], tail[dd_mask])[0,1]:+.2f}")
print(f"                                       — in Falcon's UP-months:   {np.corrcoef(fal[~dd_mask], tail[~dd_mask])[0,1]:+.2f}")
print(f"\nThe hedge payoff — tail-short's return WHEN Falcon is down:")
print(f"  Falcon DOWN-months (n={len(down_months)}): Falcon avg {falm[down_months].mean():+.1f}%/mo | tail-short avg {tailm[down_months].mean():+.1f}%/mo "
      f"-> tail-short is {'POSITIVE (hedges)' if tailm[down_months].mean() > 0 else 'ALSO NEGATIVE (does NOT hedge)'}")
print(f"  tail-short positive in {int((tailm[down_months] > 0).mean()*100)}% of Falcon's down-months")
worst = falm.nsmallest(5)
print(f"\n  Falcon's 5 WORST months:")
for m in worst.index: print(f"    {m}: Falcon {falm[m]:+.1f}%  |  tail-short {tailm[m]:+.1f}%  {'✓ hedged' if tailm[m] > 0 else '✗ also lost'}")
print(f"\nVERDICT: tail-short {'SURVIVES multi-regime — hedge holds in down-months' if (tailm[down_months].mean() > 0 and np.corrcoef(fal[dd_mask], tail[dd_mask])[0,1] < 0.3) else 'is FAIR-WEATHER — correlation collapses / it also loses in down-months'}")
