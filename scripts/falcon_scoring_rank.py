"""PATH 2 — Falcon per-stock pattern SCORING -> stock RANKING, leak-free, 6-day horizon, tested on 2024.
Score each firing pattern p on stock s by:
  Performance = p's 6-day avg return (overall, <=2023)
  Relevance   = p's 6-day avg return FOR THIS STOCK s, shrunk toward overall (Bayesian, K=10)
  Frequency   = confidence from how often p fired on s  (1 + log1p(ns))
  Recency     = down-weight patterns that did poorly in 2023
  pscore = relevance * frequency_conf * recency
Combined stock score = SUM of pscore over all patterns firing on the stock (that's the pattern-combination effect).
Rank per signal day -> top-15. Leak-free: every statistic uses ONLY fires <=2023; patterns gated mined_year<2024.
Evaluate 2024 top-15 6-day return (1X positional) vs baseline (avg of all pattern-firing stocks that day).
"""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
K = 10.0
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "mined_year", "symbol", "signal_date"]]
print(f"ledger {len(L):,} fires", flush=True)

# ---- 6-day return per (symbol, signal_date): entry open S+1, exit close S+6 ----
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily "
                       "WHERE trade_date>='2018-12-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con); con.close()
fw = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values
    entry = np.roll(o, -1); entry[-1] = np.nan; ex = np.roll(c, -6); ex[-6:] = np.nan
    fw.append(pd.DataFrame({"symbol": s, "signal_date": td, "r6": (ex - entry) / entry * 100}))
FW = pd.concat(fw, ignore_index=True)
L = L.merge(FW, on=["symbol", "signal_date"], how="left")
L = L[L.mined_year.astype(str) < "2024"]          # leak-free: only patterns mined before 2024
L["yr"] = L.signal_date.str[:4]
print(f"after mined<2024 gate: {len(L):,} fires · patterns {L.pattern_id.nunique()}", flush=True)

# ---- freeze stats from <=2023 ----
H = L[L.yr <= "2023"].dropna(subset=["r6"])
pp = H.groupby("pattern_id").agg(pn=("r6", "size"), perf=("r6", "mean")).reset_index()
rec = H[H.yr == "2023"].groupby("pattern_id").r6.mean().rename("rec23").reset_index()
sp = H.groupby(["symbol", "pattern_id"]).agg(ns=("r6", "size"), avgs=("r6", "mean")).reset_index()
print(f"frozen: {len(pp)} patterns, {len(sp):,} stock-pattern cells (<=2023)", flush=True)

# ---- score 2024 fires ----
T = L[(L.yr == "2024")].dropna(subset=["r6"]).merge(pp, on="pattern_id", how="left").merge(rec, on="pattern_id", how="left").merge(sp, on=["symbol", "pattern_id"], how="left")
T = T[T.perf.notna()]
T["ns"] = T.ns.fillna(0.0); T["avgs"] = T.avgs.fillna(T.perf)
T["rel"] = (T.ns * T.avgs + K * T.perf) / (T.ns + K)                # relevance (shrunk)
T["fconf"] = 1.0 + np.log1p(T.ns)                                    # frequency confidence
T["recw"] = np.where(T.rec23.isna(), 1.0, np.where(T.rec23 > 0, 1.0, 0.5))   # recency
T["pscore"] = T.rel * T.fconf * T.recw
print("scored 2024 fires", flush=True)

G = T.groupby(["symbol", "signal_date"]).agg(score=("pscore", "sum"), npat=("pscore", "size"), r6=("r6", "first")).reset_index()
G["mo"] = G.signal_date.str[:2].radd(G.signal_date.str[:7].str[:7])  # keep yyyy-mm
G["mo"] = G.signal_date.str[:7]

def evaluate(df, k=15, min_pat=1):
    df = df[df.npat >= min_pat]
    rows = []
    for sd, g in df.groupby("signal_date"):
        g = g.sort_values("score", ascending=False)
        top = g.head(k)
        rows.append(dict(signal_date=sd, mo=sd[:7], top_ret=top.r6.mean(), top_wr=(top.r6 > 0).mean() * 100,
                         base_ret=g.r6.mean(), n=len(g)))
    return pd.DataFrame(rows)

R = evaluate(G, 15)
print("\n===== 2024 TOP-15 by pattern-score, 6-DAY HOLD (1X, positional), LEAK-FREE =====")
print(f"{'month':<9}{'top15 avg6d':>13}{'pick WR%':>10}{'baseline6d':>12}{'edge':>8}")
for mo, g in R.groupby("mo"):
    print(f"  {mo:<7}{g.top_ret.mean():>+11.2f}%{g.top_wr.mean():>9.0f}%{g.base_ret.mean():>+11.2f}%{(g.top_ret.mean()-g.base_ret.mean()):>+7.2f}%")
print("-" * 52)
print(f"  {'YEAR':<7}{R.top_ret.mean():>+11.2f}%{R.top_wr.mean():>9.0f}%{R.base_ret.mean():>+11.2f}%{(R.top_ret.mean()-R.base_ret.mean()):>+7.2f}%")
print(f"\n  top-15 avg 6-day trade return: {R.top_ret.mean():+.2f}%   ·   pick win-rate: {R.top_wr.mean():.0f}%")
print(f"  baseline (all pattern-firing stocks, 6-day): {R.base_ret.mean():+.2f}%")
print(f"  EDGE of the ranking over baseline: {R.top_ret.mean()-R.base_ret.mean():+.2f}% per trade")
print(f"  days where top-15 beat baseline: {(R.top_ret>R.base_ret).mean()*100:.0f}%")
