"""Diagnose: does D1-INTRADAY-aligned per-stock pattern scoring surface the operator's ACTUAL Dec+Jan winners?
Score patterns by their per-stock D1 (next-day open->close) performance (leak-free, stats <=2024-11-30), rank,
and measure recall of the operator's real winning trades + the top-N D1 5x basket. This tests horizon-alignment.
"""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
K = 10.0; CUT = "2024-11-30"
picks = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "mined_year", "symbol", "signal_date", "next_oc"]]
L = L[L.mined_year.astype(str) < "2025"]

# leak-free D1 stats from fires <= CUT
H = L[L.signal_date <= CUT].dropna(subset=["next_oc"])
pp = H.groupby("pattern_id").agg(perf=("next_oc", "mean")).reset_index()
rec = H[H.signal_date >= "2024-01-01"].groupby("pattern_id").next_oc.mean().rename("rec").reset_index()
sp = H.groupby(["symbol", "pattern_id"]).agg(ns=("next_oc", "size"), avgs=("next_oc", "mean")).reset_index()

# fires on the operator's signal days (Dec24+Jan25) come from the ledger (they're in 2024) + we need Jan25 fires too.
# The ledger only has <=2024. Jan-2025 signal dates are NOT in it -> compute recall on Dec-2024 trades (in ledger)
# plus score using ledger fires for Dec; for Jan we note the ledger stops at 2024-12-31.
cal_ok = [d for d in picks.trade_date.unique() if d <= "2024-12-31"]
# build: for each operator signal day (prev trading day), score all stocks that fired
import bisect
alldates = sorted(L.signal_date.unique())
def prev(d):
    i = bisect.bisect_left(alldates, d)
    return alldates[i - 1] if i > 0 else None
# winners per signal day (operator, stock_ret_pct>0.3)
wins = {}
for _, p in picks.iterrows():
    sd = prev(p.trade_date)
    if sd and p.trade_date <= "2024-12-31" and p.stock_ret_pct > 0.3:
        wins.setdefault(sd, set()).add(p.symbol)

# score fires on those signal days
sig = sorted(wins)
FS = L[L.signal_date.isin(sig)].merge(pp, on="pattern_id").merge(rec, on="pattern_id", how="left").merge(sp, on=["symbol", "pattern_id"], how="left")
FS["ns"] = FS.ns.fillna(0.0); FS["avgs"] = FS.avgs.fillna(FS.perf)
FS["rel"] = (FS.ns * FS.avgs + K * FS.perf) / (FS.ns + K)
FS["recw"] = np.where(FS.rec.isna(), 1.0, np.where(FS.rec > 0, 1.0, 0.5))
FS["pscore"] = FS.rel * (1.0 + np.log1p(FS.ns)) * FS.recw
G = FS.groupby(["symbol", "signal_date"]).agg(score=("pscore", "sum"), d1=("next_oc", "first")).reset_index()

print(f"D1-aligned scoring on {len(sig)} Dec-2024 operator signal days (leak-free stats <= {CUT})\n")
for topN in [15, 20, 30]:
    hit = tot = 0; bask = []
    for sd in sig:
        g = G[G.signal_date == sd].sort_values("score", ascending=False)
        top = g.head(topN); ws = wins[sd]; tot += len(ws)
        hit += sum(s in set(top.symbol) for s in ws)
        bask.append(np.nanmean(g.head(15).d1.values))
    print(f"  top-{topN}: recall of operator winners {hit}/{tot} = {hit/tot*100:.0f}%   |  top-15 D1 basket {np.nanmean(bask):+.2f}% (1X) {np.nanmean(bask)*5:+.2f}% (5X)")
# how many operator winners even fired ANY pattern
opw = set()
for sd in sig: opw |= {(sd, s) for s in wins[sd]}
fired = set(zip(G.signal_date, G.symbol))
cov = sum((sd, s) in fired for sd, s in opw)
print(f"\n  operator winners that fired >=1 eligible pattern (in scored universe): {cov}/{len(opw)} = {cov/len(opw)*100:.0f}%")
