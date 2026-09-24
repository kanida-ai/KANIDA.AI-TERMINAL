"""Does the pattern->stock consistency PRIOR persist out-of-sample? Build the prior on 2022-2023 (train),
test on 2024. For each (pattern,stock): train avg D1 + test avg D1. Ask:
  - do 'prior-consistent' cells (train avg_d1>0.5, n>=3) keep avg_d1>0.5 in 2024? (persistence)
  - do they beat 'prior-inconsistent' cells (same patterns) in 2024? (predictive value)
  - how much is beta? (compare to the market's own avg D1 that year)
Also beta-adjust the strong stock names. Read-only.
"""
import os, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date", "next_oc"]].dropna(subset=["next_oc"])
L = L[L.signal_date >= "2022-01-01"].rename(columns={"next_oc": "d1"}); L["yr"] = L.signal_date.str[:4]
tr = L[L.yr <= "2023"]; te = L[L.yr == "2024"]
mkt_tr = tr.d1.mean(); mkt_te = te.d1.mean()
print(f"market avg D1 (drift): train(22-23) {mkt_tr:+.3f}%   test(2024) {mkt_te:+.3f}%\n")

PT = tr.groupby(["pattern_id", "symbol"]).agg(ntr=("d1", "size"), atr=("d1", "mean")).reset_index()
PE = te.groupby(["pattern_id", "symbol"]).agg(nte=("d1", "size"), ate=("d1", "mean")).reset_index()
M = PT.merge(PE, on=["pattern_id", "symbol"], how="inner")   # cells present in both years
M = M[(M.ntr >= 3) & (M.nte >= 3)]
cons = M[M.atr > 0.5]; incons = M[M.atr <= 0.5]
print(f"cells present both years (ntr>=3, nte>=3): {len(M):,}")
print(f"  prior-CONSISTENT (train avg>0.5): {len(cons):,}   prior-inconsistent: {len(incons):,}\n")
print("=== PERSISTENCE (do prior-consistent cells stay good in 2024?) ===")
print(f"  prior-consistent cells: 2024 avg D1 {cons.ate.mean():+.3f}%   ·  % still >0.5% in 2024: {(cons.ate>0.5).mean()*100:.0f}%")
print(f"  prior-inconsistent cells: 2024 avg D1 {incons.ate.mean():+.3f}%   ·  % >0.5% in 2024: {(incons.ate>0.5).mean()*100:.0f}%")
print(f"  market (all fires) 2024 avg D1: {mkt_te:+.3f}%")
print(f"\n  => predictive gap (consistent - inconsistent) in 2024: {cons.ate.mean()-incons.ate.mean():+.3f}%")
print(f"  => beta-adjusted edge of prior-consistent (vs market 2024): {cons.ate.mean()-mkt_te:+.3f}%")
# correlation of train vs test avg
r = np.corrcoef(M.atr, M.ate)[0, 1]
print(f"  => correlation(train avg_d1, 2024 avg_d1) across cells: {r:+.2f}  (high=persistent, ~0=noise)")

# beta-adjust the strong-stock list
print("\n=== strong stocks: raw vs beta-adjusted (2022-2024 avg D1 minus market) ===")
mkt_all = L.d1.mean()
ss = L.groupby("symbol").agg(n=("d1", "size"), avg=("d1", "mean")).reset_index()
ss = ss[ss.n >= 2000]; ss["edge_vs_mkt"] = ss.avg - mkt_all
for sym in ["KFINTECH", "ITI", "BSE", "RVNL", "BDL", "GRAVITA", "POONAWALLA", "ERIS", "VIJAYA"]:
    r = ss[ss.symbol == sym]
    if len(r): print(f"  {sym:<12} raw avg_d1 {r.avg.iloc[0]:+.3f}%   market {mkt_all:+.3f}%   real edge {r.edge_vs_mkt.iloc[0]:+.3f}%")
