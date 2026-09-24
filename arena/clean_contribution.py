"""CLEAN contribution test: Falcon + TAIL-SHORT-ONLY (isolate the orthogonal leg; no long double-count).
Capital-allocation blends (each sleeve <=5x, respecting the 5x cap). Report total, maxDD, return/DD, correlation.
Also emit the standalone market-neutral book as a tracked product. OOS 2026, read-only, Falcon untouched."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
LEV, FRIC = 5, 0.15
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
df = pd.read_sql_query("SELECT signal_date, rank, symbol, nd_intraday_ret FROM falcon_full_ranking WHERE signal_date>='2026-01-01' AND nd_intraday_ret IS NOT NULL", rc)
rc.close()
def dd_of(s): cum = s.cumsum(); return (cum.cummax() - cum).max()
def w(r): return 3.0 if r <= 10 else (2.0 if r <= 50 else 1.0)
df["lw"] = df["rank"].map(w)
long10 = df[df["rank"] <= 10].groupby("signal_date").nd_intraday_ret.mean() / 100
longwt = df[df["rank"] <= 100].groupby("signal_date").apply(lambda g: np.average(g.nd_intraday_ret, weights=g.lw)) / 100
shorttail = (-df[df["rank"] >= 201].groupby("signal_date").nd_intraday_ret.mean()) / 100
idx = sorted(set(long10.index) & set(longwt.index) & set(shorttail.index)); long10, longwt, shorttail = long10[idx], longwt[idx], shorttail[idx]
cost_d = FRIC / 100 * LEV / 20
falcon = long10 * LEV - cost_d
tailshort = shorttail * LEV - cost_d                     # tail-short sleeve at 5x (its own capital)
mktneutral = longwt * (LEV / 2) + shorttail * (LEV / 2) - cost_d
def stats(s): t = s.sum() * 100; d = dd_of(s) * 100; return t, d, (t / d if d else 0)

print("=" * 76 + "\nCLEAN CONTRIBUTION — Falcon + TAIL-SHORT-ONLY (orthogonal leg isolated, OOS 2026, 5x)\n" + "=" * 76)
print(f"correlation  tail-short vs Falcon daily P&L: {np.corrcoef(falcon, tailshort)[0,1]:+.2f}   (the orthogonal leg)\n")
ft, fd, frdd = stats(falcon)
print(f"{'capital split':<26}{'OOS total%':>11}{'maxDD%':>9}{'return/DD':>10}{'ret give-up':>13}{'DD cut':>9}")
for wf in [1.0, 0.9, 0.8, 0.7, 0.6, 0.5]:
    b = wf * falcon + (1 - wf) * tailshort; t, d, rdd = stats(b)
    tag = "  <- Falcon-alone" if wf == 1.0 else ""
    print(f"{f'{wf:.0%} Falcon / {1-wf:.0%} tail-short':<26}{t:>+11.0f}{d:>9.1f}{rdd:>10.2f}{(t-ft)/ft*100:>+12.0f}%{(fd-d)/fd*100:>+8.0f}%{tag}")

print("\nvs the FULL combo (for reference — it double-counted the long leg):")
for wf in [0.7, 0.5]:
    b = wf * falcon + (1 - wf) * mktneutral; t, d, rdd = stats(b)
    print(f"  {wf:.0%} Falcon / {1-wf:.0%} full-combo:  total {t:+.0f}%  maxDD {d:.1f}%  r/DD {rdd:.2f}  (ret give-up {(t-ft)/ft*100:+.0f}%)")

print("\n" + "=" * 76 + "\nSTANDALONE PRODUCT — Market-Neutral book (long 1-100 wtd + short 201-500)\n" + "=" * 76)
t, d, rdd = stats(mktneutral)
print(f"  OOS total {t:+.0f}%  |  maxDD {d:.1f}%  |  return/DD {rdd:.2f}  |  corr to Falcon {np.corrcoef(falcon,mktneutral)[0,1]:+.2f}")
mm = (pd.Series(mktneutral.values, index=[x[:7] for x in idx]).groupby(level=0).sum() * 100)
print("  monthly (5x %):", {m: round(v) for m, v in mm.items()})
import pickle
pickle.dump({"falcon": (ft, fd, frdd), "tail_only_blends": {wf: stats(wf * falcon + (1 - wf) * tailshort) for wf in [1.0, 0.8, 0.7, 0.6, 0.5]},
             "corr_tail": float(np.corrcoef(falcon, tailshort)[0, 1]), "mktneutral": stats(mktneutral),
             "mn_monthly": {m: round(v) for m, v in mm.items()}}, open(os.path.join(ROOT, "arena", "clean_contrib.pkl"), "wb"))
print("\nsaved clean_contrib.pkl")
