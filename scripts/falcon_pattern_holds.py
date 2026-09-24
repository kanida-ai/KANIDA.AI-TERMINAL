"""FALCPAT_8619 (or PID env) — multi-day holds. Entry = next-day OPEN (T). Hold N days = exit at CLOSE of T+N-1.
Holds 1,2,3,5,6 days. Plain + hard-stop config. All fires vs the stocks where it's consistent. Leak-free fires
(PIT-weekly ledger); returns are pure price outcomes. Descriptive pattern characterization.
"""
import os, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
import sqlite3
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
PID = int(os.environ.get("PID", "8619"))
HOLDS = [1, 2, 3, 5, 6]; STOP = 5.0
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))
fires = L[L.pattern_id == PID][["symbol", "signal_date", "next_oc"]].copy()
print(f"FALCPAT_{PID}: {len(fires):,} fires on {fires.symbol.nunique()} stocks")

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily "
                       "WHERE trade_date>='2018-12-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con)
con.close()

# per (symbol, signal_date): entry=open(T=S+1); for each N: exit=close(S+N); minlow over S+1..S+N
rows = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    o = g.open.values.astype(float); c = g.close.values.astype(float); lw = g.low.values.astype(float); td = g.trade_date.values; n = len(c)
    entry = np.roll(o, -1); entry[-1] = np.nan   # open of S+1
    d = {"symbol": s, "trade_date": td, "entry": entry}
    runmin = np.full(n, np.inf)
    for N in range(1, max(HOLDS) + 1):
        exitc = np.roll(c, -N); exitc[-N:] = np.nan
        # running min low from S+1..S+N
        lN = np.roll(lw, -N); lN[-N:] = np.nan
        runmin = np.minimum(runmin, lN)
        retN = (exitc - entry) / entry * 100
        # hard stop: if min low in window breached entry*(1-STOP%), exit at -STOP
        breach = runmin <= entry * (1 - STOP / 100)
        retN_stop = np.where(breach, -STOP, retN)
        d[f"ret{N}"] = retN; d[f"rets{N}"] = retN_stop
    rows.append(pd.DataFrame(d))
FW = pd.concat(rows, ignore_index=True)
M = fires.merge(FW, left_on=["symbol", "signal_date"], right_on=["symbol", "trade_date"], how="left")

# consistent stocks = 1-day win>55%, >=5 fires, positive avg (from this pattern's own fires)
ps = M.groupby("symbol").agg(nn=("ret1", "size"), w1=("ret1", lambda x: (x > 0).mean()), a1=("ret1", "mean"))
cons = set(ps[(ps.nn >= 5) & (ps.w1 > 0.55) & (ps.a1 > 0)].index)
Mc = M[M.symbol.isin(cons)]
print(f"consistent stocks (1-day): {len(cons)}  ·  their fires: {len(Mc):,}\n")

def line(df, col):
    x = df[col].dropna()
    if len(x) == 0: return "   -"
    return f"{(x>0).mean()*100:>5.0f}%{x.mean():>+8.2f}%{x.mean()*5:>+8.2f}%"
def block(df, tag):
    print(f"===== {tag} ({len(df):,} fires) =====")
    print(f"  {'hold':<8}{'PLAIN win%':>11}{'avg%':>8}{'5x%':>8}   {'STOP-5% win%':>13}{'avg%':>8}{'5x%':>8}")
    for N in HOLDS:
        pl = line(df, f"ret{N}"); st = line(df, f"rets{N}")
        print(f"  {N}-day  {pl}     {st}")
    print()
block(M, "ALL FIRES")
block(Mc, f"CONSISTENT STOCKS only ({len(cons)} names)")
