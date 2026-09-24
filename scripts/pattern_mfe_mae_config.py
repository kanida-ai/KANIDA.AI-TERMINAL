"""Per-pattern MFE/MAE by hold day + exit-config menu. Entry = next-day OPEN. For hold day N (1..6):
  MFE_N = max favorable move (best (high-entry)/entry over days 1..N),  MAE_N = max adverse (worst (low-entry)/entry).
Config menu (exit-first, checked stop-then-target each day, over the 6-day window): plain, wide-stop, and 3 target/stop combos.
Leak-free ledger. Two memory passes. Outputs pattern_mfe_mae_config.csv (all 865) + FALCPAT_8619 detail.
"""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
HOLDS = [1, 2, 3, 5, 6]; MAXD = 6
CONFIGS = [("plain6", None, None), ("widestop_s8", None, 8.0), ("t8_s6", 8.0, 6.0), ("t12_s8", 12.0, 8.0), ("t6_s5", 6.0, 5.0)]
PH = pd.read_csv(os.path.join(OUT, "pattern_holds.csv"))[["pattern_id", "best_hold", "n_consistent_stocks"]]

# ---- per (symbol,date): daily hi_d/lo_d/cl_d (rel to entry), MFE_N/MAE_N, config outcomes ----
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily "
                       "WHERE trade_date>='2018-12-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con); con.close()
recs = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    o = g.open.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values; n = len(c)
    entry = np.roll(o, -1); entry[-1] = np.nan
    d = {"symbol": s, "signal_date": td}
    hid = {}; lod = {}; cld = {}
    for k in range(1, MAXD + 1):
        hh = np.roll(h, -k); hh[-k:] = np.nan; ll = np.roll(l, -k); ll[-k:] = np.nan; cc = np.roll(c, -k); cc[-k:] = np.nan
        hid[k] = (hh - entry) / entry * 100; lod[k] = (ll - entry) / entry * 100; cld[k] = (cc - entry) / entry * 100
    # MFE_N / MAE_N cumulative
    mfe = np.full(n, -np.inf); mae = np.full(n, np.inf)
    for N in range(1, MAXD + 1):
        mfe = np.fmax(mfe, hid[N]); mae = np.fmin(mae, lod[N])
        if N in HOLDS: d[f"mfe{N}"] = mfe.copy(); d[f"mae{N}"] = mae.copy()
    d["cl6"] = cld[6]
    # config outcomes (exit-first over 6 days)
    for name, tgt, stp in CONFIGS:
        if name == "plain6": d[name] = cld[6]; continue
        ret = np.full(n, np.nan); exited = np.zeros(n, bool)
        for k in range(1, MAXD + 1):
            if stp is not None:
                sh = (~exited) & (lod[k] <= -stp); ret[sh] = -stp; exited |= sh
            if tgt is not None:
                th = (~exited) & (hid[k] >= tgt); ret[th] = tgt; exited |= th
        rem = ~exited & np.isfinite(cld[6]); ret[rem] = cld[6][rem]
        d[name] = ret
    recs.append(pd.DataFrame(d))
FW = pd.concat(recs, ignore_index=True)
for c in FW.columns:
    if c not in ("symbol", "signal_date"): FW[c] = FW[c].replace([np.inf, -np.inf], np.nan).astype("float32")
print(f"FW built {len(FW):,} (symbol,date) rows", flush=True)

Lkeys = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]]

# ---- Pass A: MFE/MAE per hold per pattern ----
mm_cols = [f"mfe{N}" for N in HOLDS] + [f"mae{N}" for N in HOLDS]
A = Lkeys.merge(FW[["symbol", "signal_date"] + mm_cols], on=["symbol", "signal_date"], how="left")
MM = A.groupby("pattern_id")[mm_cols].mean().round(2).reset_index()
del A
# ---- Pass B: config returns + win rates per pattern ----
cfg_cols = [c[0] for c in CONFIGS]
B = Lkeys.merge(FW[["symbol", "signal_date"] + cfg_cols], on=["symbol", "signal_date"], how="left")
agg = {}
for c in cfg_cols: agg[c + "_ret"] = (c, "mean"); agg[c + "_wr"] = (c, lambda x: (x > 0).mean() * 100)
CF = B.groupby("pattern_id").agg(**agg).reset_index()
for c in cfg_cols: CF[c + "_ret"] = CF[c + "_ret"].round(2); CF[c + "_wr"] = CF[c + "_wr"].round(0)
del B
R = PH.merge(MM, on="pattern_id").merge(CF, on="pattern_id")
# best config by avg return
retc = [c + "_ret" for c in cfg_cols]
R["best_config"] = [cfg_cols[i] for i in R[retc].to_numpy(float).argmax(1)]
R["best_config_ret"] = R[retc].max(1).round(2)
R["pattern"] = "FALCPAT_" + R.pattern_id.astype(str)
R.to_csv(os.path.join(OUT, "pattern_mfe_mae_config.csv"), index=False)
print(f"saved pattern_mfe_mae_config.csv ({len(R)} patterns)\n", flush=True)

print("MFE / MAE by hold day (avg across all patterns):")
for N in HOLDS:
    print(f"  hold {N}d:  avg MFE {R[f'mfe{N}'].mean():+.2f}%   avg MAE {R[f'mae{N}'].mean():+.2f}%")
print("\nWhich exit config wins most across the 865 patterns:")
print(R.best_config.value_counts().to_string())
print("\nFALCPAT_8619 — MFE/MAE by hold day:")
r = R[R.pattern_id == 8619].iloc[0]
for N in HOLDS: print(f"  hold {N}d:  MFE {r[f'mfe{N}']:+.2f}%   MAE {r[f'mae{N}']:+.2f}%")
print("\nFALCPAT_8619 — exit configs (6-day window):")
for c in cfg_cols: print(f"  {c:<14} avg {r[c+'_ret']:+.2f}%   win {r[c+'_wr']:.0f}%")
print(f"  => best config: {r.best_config} ({r.best_config_ret:+.2f}%)")
