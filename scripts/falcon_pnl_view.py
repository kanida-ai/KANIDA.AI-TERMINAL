"""Monthly P&L view of the leak-free Falcon scoring rank (2024). Fixed CAP per stock.
D1 hold shown at 1X and 5X (intraday MIS); D2-D6 holds at 1X only (positional, no MIS leverage).
Runs top-15 / top-20 / top-30 so we can see the cutoff tradeoff. Leak-free (stats <=2023, patterns mined<2024).
"""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
K = 10.0; CAP = 100000; HOLDS = [1, 2, 3, 5, 6]
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "mined_year", "symbol", "signal_date"]]

# forward returns for all holds
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>='2018-12-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con); con.close()
fw = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values
    entry = np.roll(o, -1); entry[-1] = np.nan; d = {"symbol": s, "signal_date": td}
    for N in HOLDS:
        ex = np.roll(c, -N); ex[-N:] = np.nan; d[f"r{N}"] = (ex - entry) / entry * 100
    fw.append(pd.DataFrame(d))
FW = pd.concat(fw, ignore_index=True)
L = L.merge(FW[["symbol", "signal_date", "r6"]], on=["symbol", "signal_date"], how="left")
L = L[L.mined_year.astype(str) < "2024"]; L["yr"] = L.signal_date.str[:4]

# frozen <=2023 stats
H = L[L.yr <= "2023"].dropna(subset=["r6"])
pp = H.groupby("pattern_id").agg(perf=("r6", "mean")).reset_index()
rec = H[H.yr == "2023"].groupby("pattern_id").r6.mean().rename("rec23").reset_index()
sp = H.groupby(["symbol", "pattern_id"]).agg(ns=("r6", "size"), avgs=("r6", "mean")).reset_index()
# score 2024
T = L[L.yr == "2024"].dropna(subset=["r6"]).merge(pp, on="pattern_id").merge(rec, on="pattern_id", how="left").merge(sp, on=["symbol", "pattern_id"], how="left")
T["ns"] = T.ns.fillna(0.0); T["avgs"] = T.avgs.fillna(T.perf)
T["rel"] = (T.ns * T.avgs + K * T.perf) / (T.ns + K)
T["recw"] = np.where(T.rec23.isna(), 1.0, np.where(T.rec23 > 0, 1.0, 0.5))
T["pscore"] = T.rel * (1.0 + np.log1p(T.ns)) * T.recw
G = T.groupby(["symbol", "signal_date"]).agg(score=("pscore", "sum")).reset_index()
G = G.merge(FW, on=["symbol", "signal_date"], how="left"); G["mo"] = G.signal_date.str[:7]
print(f"scored stock-days: {len(G):,}", flush=True)

def monthly_pnl(topN):
    picks = []
    for sd, g in G.groupby("signal_date"):
        picks.append(g.sort_values("score", ascending=False).head(topN))
    P = pd.concat(picks); P["mo"] = P.signal_date.str[:7]
    rows = []
    for mo, m in P.groupby("mo"):
        row = {"month": mo, "trades": len(m)}
        row["D1_1X"] = (m.r1 / 100 * CAP).sum()
        row["D1_5X"] = (m.r1 / 100 * CAP * 5).sum()
        for N in [2, 3, 5, 6]: row[f"D{N}_1X"] = (m[f"r{N}"] / 100 * CAP).sum()
        rows.append(row)
    R = pd.DataFrame(rows)
    tot = {"month": "YEAR", "trades": R.trades.sum()}
    for c in R.columns:
        if c not in ("month", "trades"): tot[c] = R[c].sum()
    R = pd.concat([R, pd.DataFrame([tot])], ignore_index=True)
    return R, P

for N in [15]:
    R, _ = monthly_pnl(N)
    print(f"\n===== MONTHLY P&L, TOP-{N}, Rs {CAP:,}/stock  (D1 = 1X & 5X ; D2-D6 = 1X positional) =====")
    disp = R.copy()
    for c in disp.columns:
        if c not in ("month", "trades"): disp[c] = disp[c].map(lambda x: f"{x:>12,.0f}")
    print(disp.to_string(index=False))

# cutoff comparison
print(f"\n===== TOP-15 vs 20 vs 30  (year totals, Rs {CAP:,}/stock) =====")
print(f"{'topN':<6}{'trades':>8}{'D1_1X avg%':>12}{'D1 5X PnL':>14}{'D6_1X avg%':>12}{'D6 1X PnL':>14}")
for N in [15, 20, 30]:
    _, P = monthly_pnl(N)
    print(f"  {N:<4}{len(P):>8}{P.r1.mean():>+11.2f}%{(P.r1/100*CAP*5).sum():>14,.0f}{P.r6.mean():>+11.2f}%{(P.r6/100*CAP).sum():>14,.0f}")
