"""Roll up all 865 Falcon patterns over multi-day holds, and build the PATTERN->STOCK CONSISTENCY LIBRARY.
Entry = next-day OPEN (T), hold N days = exit CLOSE of T+N-1, for N in 1,2,3,5,6. Leak-free ledger.
Outputs (research_outputs/falcon_pattern_backtest/):
  pattern_holds.csv             one row per pattern: fires, #stocks, win/avg per hold, best hold, #consistent stocks
  pattern_stock_library.parquet one row per (pattern,stock): fires, win/avg per hold, best hold, recency, CONSISTENT flag
This library is the lookup for scoring: pattern P fires on stock S -> is P consistent on S, best hold, win/avg, freq, recency.
"""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
HOLDS = [1, 2, 3, 5, 6]
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]]
print(f"ledger {len(L):,} fires", flush=True)

# ---- forward returns per (symbol, date): entry open S+1, exit close S+N ----
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily "
                       "WHERE trade_date>='2018-12-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con); con.close()
fw = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values
    entry = np.roll(o, -1); entry[-1] = np.nan
    d = {"symbol": s, "signal_date": td}
    for N in HOLDS:
        ex = np.roll(c, -N); ex[-N:] = np.nan; d[f"r{N}"] = (ex - entry) / entry * 100
    fw.append(pd.DataFrame(d))
FW = pd.concat(fw, ignore_index=True)
L = L.merge(FW, on=["symbol", "signal_date"], how="left")
for N in HOLDS: L[f"w{N}"] = np.where(L[f"r{N}"].notna(), (L[f"r{N}"] > 0).astype(float), np.nan)
L["recent"] = (L.signal_date.str[:4] >= "2023").astype("int8")
print("forward returns merged", flush=True)

def rollup(df, keys):
    named = {}
    for N in HOLDS: named[f"avg{N}"] = (f"r{N}", "mean"); named[f"win{N}"] = (f"w{N}", "mean")
    named["n"] = ("r1", "count"); named["last_date"] = ("signal_date", "max"); named["n_recent"] = ("recent", "sum")
    R = df.groupby(keys).agg(**named).reset_index()
    A = R[[f"avg{N}" for N in HOLDS]].to_numpy(dtype=float)
    Wm = R[[f"win{N}" for N in HOLDS]].to_numpy(dtype=float) * 100
    bi = np.nanargmax(np.where(np.isnan(A), -1e9, A), axis=1); ar = np.arange(len(R))
    R["best_hold"] = np.array(HOLDS)[bi]; R["best_avg"] = np.round(A[ar, bi], 2); R["best_win"] = np.round(Wm[ar, bi], 0)
    for N in HOLDS: R[f"win{N}"] = (R[f"win{N}"] * 100).round(0); R[f"avg{N}"] = R[f"avg{N}"].round(2)
    return R

print("rolling up patterns...", flush=True)
PH = rollup(L, "pattern_id"); PH["nstocks"] = L.groupby("pattern_id").symbol.nunique().reindex(PH.pattern_id).values
print("building pattern-stock library...", flush=True)
SPL = rollup(L, ["pattern_id", "symbol"])
SPL["consistent"] = ((SPL.n >= 5) & (SPL.best_win > 55) & (SPL.best_avg > 0)).astype(int)
PH["n_consistent_stocks"] = SPL[SPL.consistent == 1].groupby("pattern_id").size().reindex(PH.pattern_id).fillna(0).astype(int).values
PH.to_csv(os.path.join(OUT, "pattern_holds.csv"), index=False)
SPL.to_parquet(os.path.join(OUT, "pattern_stock_library.parquet"))
print(f"\nsaved pattern_holds.csv ({len(PH)} patterns) + pattern_stock_library.parquet ({len(SPL):,} pattern-stock cells)\n", flush=True)

print("Which hold period does each pattern pay best on (of 865):")
print(PH.best_hold.value_counts().sort_index().to_string())
print(f"\nmedian best-hold avg per pattern: {PH.best_avg.median():+.2f}%   ·   patterns with positive best-hold edge: {(PH.best_avg>0).sum()}/865")
print(f"total CONSISTENT (pattern,stock) pairs: {int(SPL.consistent.sum()):,}   ·   median consistent-stocks/pattern: {int(PH.n_consistent_stocks.median())}")
print("\ntop 10 patterns by #consistent stocks:")
print(PH.sort_values("n_consistent_stocks", ascending=False)[["pattern_id", "n", "nstocks", "best_hold", "best_avg", "best_win", "n_consistent_stocks"]].head(10).to_string(index=False))
print("\nexample library rows (FALCPAT_8619 consistent stocks):")
ex = SPL[(SPL.pattern_id == 8619) & (SPL.consistent == 1)].sort_values("best_avg", ascending=False)
print(ex[["symbol", "n", "best_hold", "best_win", "best_avg", "n_recent", "last_date"]].head(10).to_string(index=False))
