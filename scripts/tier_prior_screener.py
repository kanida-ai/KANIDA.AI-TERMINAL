"""Operator's tiered pattern-stock PRIOR score, built exactly as specified. Prior tiers from <=2023, scored on
2024 (out-of-sample). For each stock-day: fired patterns -> join prior cell -> log(1+n)*avg_d1*tier_mult, summed.
Rank by pattern_stock_prior_score; measure top-N next-day (D1) return vs baseline. Leak-free.
"""
import os, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date", "next_oc"]].dropna(subset=["next_oc"])
L = L[L.signal_date >= "2022-01-01"].rename(columns={"d1": "next_oc"}); L["yr"] = L.signal_date.str[:4]
L = L.rename(columns={"next_oc": "d1"})

# ---- prior from <=2023 ----
P = L[L.yr <= "2023"].groupby(["pattern_id", "symbol"]).agg(n=("d1", "size"), avg=("d1", "mean")).reset_index()
def tier(r):
    if r.avg >= 10: return "D", 0.0
    if r.avg >= 2 and r.n >= 10: return "A", 1.00
    if 1 <= r.avg < 2 and r.n >= 20: return "B", 0.65
    if r.avg >= 5 and r.n >= 5: return "C", 0.35
    return "-", 0.0
tm = P.apply(tier, axis=1)
P["tier"] = [t[0] for t in tm]; P["mult"] = [t[1] for t in tm]
P["cell_score"] = np.log1p(P.n) * P.avg * P.mult
PRI = P[P.mult > 0][["pattern_id", "symbol", "cell_score", "tier"]]
print(f"prior cells in a scoring tier (<=2023): {len(PRI):,}")
print("  by tier:", P[P.mult > 0].tier.value_counts().to_dict(), flush=True)

# ---- score 2024 stock-days ----
T = L[L.yr == "2024"].merge(PRI, on=["pattern_id", "symbol"], how="left")
T["cell_score"] = T.cell_score.fillna(0.0)
G = T.groupby(["symbol", "signal_date"]).agg(prior=("cell_score", "sum"), d1=("d1", "first"), npat=("cell_score", "size")).reset_index()
print(f"2024 stock-days scored: {len(G):,}  ·  with any prior boost: {int((G.prior>0).sum()):,}\n")

def ev(df, k):
    hit = []; base = []
    for sd, g in df.groupby("signal_date"):
        g = g.sort_values("prior", ascending=False)
        hit.append(g.head(k).d1.mean()); base.append(g.d1.mean())
    return np.nanmean(hit), np.nanmean(base)
print("2024 OUT-OF-SAMPLE — rank by pattern_stock_prior_score, next-day (D1) return:")
print(f"  {'top-N':>7}{'top D1':>9}{'baseline D1':>13}{'edge':>8}")
for k in [10, 15, 20, 30]:
    t, b = ev(G, k)
    print(f"  {k:>7}{t:>+8.2f}%{b:>+12.2f}%{t-b:>+7.2f}%")
# only among days/stocks that actually got a prior boost (the intended use)
Gp = G[G.prior > 0]
print(f"\n  restricted to stocks WITH a prior boost ({Gp.symbol.nunique()} stocks):")
for k in [10, 15, 20]:
    t, b = ev(Gp, k)
    print(f"  top-{k}: {t:+.2f}% D1  vs baseline {b:+.2f}%  edge {t-b:+.2f}%")
# 5x for the boosted top-15 (intraday MIS)
t15, b15 = ev(Gp, 15)
print(f"\n  boosted top-15 intraday: {t15:+.2f}% (1X)  {t15*5:+.2f}% (5X)/day  ·  edge vs baseline {t15-b15:+.2f}%/day")
