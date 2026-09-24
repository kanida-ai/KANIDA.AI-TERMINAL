"""FALCON per-stock pattern scoring -> stock ranking, 2024, LEAK-FREE.
Pipeline step 2-4: using the fire-ledger, freeze each pattern's performance from history <=2023 (overall AND
per-stock), then for every 2024 signal date score each firing pattern by Relevance (worked for THIS stock),
Recency (worked in 2023), Frequency (enough occurrences), Performance (win/return/downside). Combine per stock
-> stock score -> top-15. Fully leak-free: no 2024 data used in any statistic. Evaluate 2024 top-15 next-day basket.
"""
import os, sys, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))
L["yr"] = L.signal_date.str[:4]
print(f"ledger {len(L):,} fires · 2024 fires {int((L.yr=='2024').sum()):,}")

# ---- freeze stats from history <= 2023 (leak-free for 2024) ----
H = L[L.yr <= "2023"]
pp = H.groupby("pattern_id").agg(n=("next_oc", "size"), avg=("next_oc", "mean"),
                                 win=("next_oc", lambda x: (x > 0).mean())).reset_index()
rec = H[H.yr == "2023"].groupby("pattern_id").next_oc.mean().rename("rec2023").reset_index()   # recency
pp = pp.merge(rec, on="pattern_id", how="left")
sp = H.groupby(["symbol", "pattern_id"]).agg(ns=("next_oc", "size"), avgs=("next_oc", "mean")).reset_index()
print(f"frozen: {len(pp)} pattern stats, {len(sp):,} stock-pattern stats (<=2023)")

# ---- score every 2024 fire ----
F = L[L.yr == "2024"].merge(pp, on="pattern_id", how="left").merge(sp, on=["symbol", "pattern_id"], how="left")
K = 10.0                                   # shrinkage strength toward the pattern's overall avg
F["ns"] = F.ns.fillna(0); F["avgs"] = F.avgs.fillna(F["avg"])
# RELEVANCE: stock-specific return shrunk toward overall (few stock samples -> trust overall)
F["rel"] = (F.ns * F.avgs + K * F["avg"]) / (F.ns + K)
# FREQUENCY: confidence from #occurrences
F["conf"] = np.log1p(F.n) / np.log1p(pp.n.median())
# RECENCY: patterns that recently failed get down-weighted
F["recw"] = np.where(F.rec2023.fillna(F["avg"]) > -0.3, 1.0, 0.35)
# PERFORMANCE already inside rel/avg. Per-firing-pattern score:
F["pscore"] = F.rel * F.conf * F.recw

# ---- combine per stock (SUM of firing-pattern scores) + count ----
G = F.groupby(["symbol", "signal_date"]).agg(stock_score=("pscore", "sum"), npat=("pscore", "size"),
                                             next_oc=("next_oc", "first")).reset_index()
print(f"2024 stock-days scored: {len(G):,}")

# ---- rank per day, evaluate top-15 next-day basket ----
def evaltop(df, k=15, min_pat=1):
    df = df[df.npat >= min_pat]
    rows = []
    for sd, g in df.groupby("signal_date"):
        g = g.sort_values("stock_score", ascending=False).head(k)
        rows.append(dict(signal_date=sd, mo=sd[:7], ret=np.nanmean(g.next_oc.values), n=len(g)))
    R = pd.DataFrame(rows)
    return R
R = evaltop(G, 15)
print("\n===== 2024 top-15 (ranked by per-stock pattern score), LEAK-FREE, next-day open->close =====")
print(f"{'month':<9}{'days':>5}{'1x sum':>9}{'5x sum':>9}{'avg/day':>9}{'win%days':>9}")
for mo, g in R.groupby("mo"):
    print(f"  {mo:<7}{len(g):>5}{g.ret.sum():>+8.1f}%{g.ret.sum()*5:>+8.1f}%{g.ret.mean():>+8.2f}%{(g.ret>0).mean()*100:>8.0f}%")
print(f"\n  YEAR: 1x {R.ret.sum():+.1f}%  5x {R.ret.sum()*5:+.1f}%  · daily avg {R.ret.mean():+.3f}% · day win-rate {(R.ret>0).mean()*100:.0f}%")
# baseline: universe average next-day move that year (what a random top-15 gets)
uni = L[L.yr == "2024"].groupby("signal_date").next_oc.mean()
print(f"  baseline (all-fires avg next-day/day): {uni.mean():+.3f}%   -> our edge vs baseline: {R.ret.mean()-uni.mean():+.3f}%/day")
