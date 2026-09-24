"""Is the ISSUE the aggregation? Test how we combine firing-pattern scores into a stock score:
sum / mean / top-5 mean / max — and which best recalls the operator's ACTUAL Dec winners. Leak-free (D1 stats <=2024-11-30).
"""
import os, bisect, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
K = 10.0; CUT = "2024-11-30"
picks = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "mined_year", "symbol", "signal_date", "next_oc"]]
L = L[L.mined_year.astype(str) < "2025"]
H = L[L.signal_date <= CUT].dropna(subset=["next_oc"])
pp = H.groupby("pattern_id").agg(perf=("next_oc", "mean")).reset_index()
rec = H[H.signal_date >= "2024-01-01"].groupby("pattern_id").next_oc.mean().rename("rec").reset_index()
sp = H.groupby(["symbol", "pattern_id"]).agg(ns=("next_oc", "size"), avgs=("next_oc", "mean")).reset_index()
alldates = sorted(L.signal_date.unique())
def prev(d):
    i = bisect.bisect_left(alldates, d); return alldates[i - 1] if i > 0 else None
wins = {}
for _, p in picks.iterrows():
    sd = prev(p.trade_date)
    if sd and p.trade_date <= "2024-12-31" and p.stock_ret_pct > 0.3: wins.setdefault(sd, set()).add(p.symbol)
sig = sorted(wins)
FS = L[L.signal_date.isin(sig)].merge(pp, on="pattern_id").merge(rec, on="pattern_id", how="left").merge(sp, on=["symbol", "pattern_id"], how="left")
FS["ns"] = FS.ns.fillna(0.0); FS["avgs"] = FS.avgs.fillna(FS.perf)
FS["rel"] = (FS.ns * FS.avgs + K * FS.perf) / (FS.ns + K)
FS["recw"] = np.where(FS.rec.isna(), 1.0, np.where(FS.rec > 0, 1.0, 0.5))
FS["pscore"] = FS.rel * (1.0 + np.log1p(FS.ns)) * FS.recw

def agg(gr, how):
    v = gr.pscore.values
    if how == "sum": return v.sum()
    if how == "mean": return v.mean()
    if how == "top5": return np.sort(v)[-5:].mean()
    if how == "max": return v.max()
    if how == "top3_rel": return np.sort(gr.rel.values)[-3:].mean()  # best-3 by relevance only
G = FS.groupby(["symbol", "signal_date"])
agg_df = pd.DataFrame({"symbol": [k[0] for k in G.groups], "signal_date": [k[1] for k in G.groups]})
for how in ["sum", "mean", "top5", "max", "top3_rel"]:
    agg_df[how] = [agg(FS.iloc[idx], how) for idx in G.indices.values()]

print(f"recall of operator winners (Dec-2024, {len(sig)} days) under different aggregations:\n")
print(f"  {'aggregation':<14}{'top-15':>8}{'top-30':>8}")
for how in ["sum", "mean", "top5", "max", "top3_rel"]:
    hit15 = hit30 = tot = 0
    for sd in sig:
        d = agg_df[agg_df.signal_date == sd].sort_values(how, ascending=False)
        ws = wins[sd]; tot += len(ws)
        hit15 += sum(s in set(d.head(15).symbol) for s in ws); hit30 += sum(s in set(d.head(30).symbol) for s in ws)
    print(f"  {how:<14}{hit15/tot*100:>6.0f}%{hit30/tot*100:>7.0f}%")
