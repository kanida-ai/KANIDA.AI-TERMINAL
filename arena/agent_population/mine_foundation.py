"""VECTORIZED MINING FOUNDATION — build outcome matrices for INTRADAY (same-day) & BTST (next-day) across 3 entry
times (09:15 / 09:45 / 10:00), plus the BASELINE (buy-the-universe, no pattern) for each. Everything numpy, no per-row
loops in the hot path. Caches entry prices + outcomes so the miner reuses them. Read-only. Window 2024-05..2026-07-10."""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
AP = os.path.dirname(os.path.abspath(__file__))
LO, HI = "2024-05-13", "2026-07-10"; ENTRY_TIMES = ["09:15", "09:45", "10:00"]; COST_ID = 0.15; COST_BTST = 0.30

print("1) daily closes (exits) ...", flush=True)
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE trade_date>=? AND trade_date<=? ORDER BY symbol,trade_date", uc, params=(LO, "2026-08-15")); uc.close()
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
close = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}

print("2) entry prices at 09:15 / 09:45 / 10:00 (targeted 1-min pull) ...", flush=True)
mc = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
q = ("SELECT symbol, substr(bar_time,1,10) d, substr(bar_time,12,5) hm, open FROM ohlc_1min "
     "WHERE substr(bar_time,12,5) IN ('09:15','09:45','10:00') AND substr(bar_time,1,10) BETWEEN ? AND ?")
ep = pd.read_sql_query(q, mc, params=(LO, HI)); mc.close()
ep = ep.pivot_table(index=["symbol", "d"], columns="hm", values="open", aggfunc="first").reset_index()
print(f"   entry-price rows: {len(ep)}  (symbol-days with 1-min)")

# outcome matrices, fully vectorized ---------------------------------------------------------------
# for each (symbol, signal_date) the entry day = signal_date itself (we mine on features KNOWN at prior close,
# but for a clean pattern the FEATURE is measured at day-1 close and we ENTER intraday on day D). Here we build the
# per-day trade outcome for a stock ENTERED on day D at time T:
ep = ep.rename(columns={"d": "trade_date", "09:15": "e915", "09:45": "e945", "10:00": "e1000"})
ep["c_same"] = [close.get((s, d)) for s, d in zip(ep.symbol, ep.trade_date)]                 # same-day close
ep["nd"] = ep.trade_date.map(lambda d: alldays[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(alldays) else None)
ep["c_next"] = [close.get((s, d)) if d else None for s, d in zip(ep.symbol, ep.nd)]           # next-day close
for t, col in [("09:15", "e915"), ("09:45", "e945"), ("10:00", "e1000")]:
    e = ep[col].values.astype(float)
    ep[f"ID_{t}"] = np.where(e > 0, (ep.c_same.values.astype(float)/e - 1)*100 - COST_ID, np.nan)      # intraday same-day
    ep[f"BT_{t}"] = np.where(e > 0, (ep.c_next.values.astype(float)/e - 1)*100 - COST_BTST, np.nan)     # BTST next-day
ep["yr"] = ep.trade_date.str[:4].astype(int)
OUT = ep[["symbol", "trade_date", "yr"] + [f"ID_{t}" for t in ENTRY_TIMES] + [f"BT_{t}" for t in ENTRY_TIMES]]
OUT.to_pickle(os.path.join(AP, "_mine_outcomes.pkl"))
print(f"   outcomes cached: {len(OUT)} rows -> _mine_outcomes.pkl")

# BASELINE — buy the whole universe at each entry time/structure (no pattern) -----------------------
def stats(v):
    v = v[~np.isnan(v)]; return (len(v), np.mean(v), (v > 0).mean()*100)
print("\n" + "="*66)
print("BASELINE — buy EVERY stock, no pattern (avg net return per trade, %)")
print("="*66)
print(f"  {'structure':<22}{'entry':>8}{'trades':>9}{'avg%':>8}{'win%':>7}")
for struct, pref, lab in [("ID", "ID_", "INTRADAY same-day"), ("BT", "BT_", "BTST next-day")]:
    for t in ENTRY_TIMES:
        n, avg, win = stats(OUT[f"{pref}{t}"].values)
        print(f"  {lab:<22}{t:>8}{n:>9}{avg:>+8.3f}{win:>6.0f}%")
print("\n  (this is the number a MINED pattern must BEAT. Negative baseline = the average stock loses;")
print("   a good pattern finds the subset that wins. Mining next.)")
# split baseline by train/OOS for reference
print("\n  train(<=2025) vs OOS(2026) baseline avg% — is the tape itself up or down?")
for struct, pref, lab in [("ID", "ID_", "INTRADAY"), ("BT", "BT_", "BTST")]:
    for t in ENTRY_TIMES:
        tr = OUT[OUT.yr <= 2025][f"{pref}{t}"].values; oos = OUT[OUT.yr == 2026][f"{pref}{t}"].values
        print(f"    {lab:<9}{t}:  train {np.nanmean(tr):+.3f}%   2026 {np.nanmean(oos):+.3f}%")
