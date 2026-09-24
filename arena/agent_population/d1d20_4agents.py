"""D1-D20 forward path for all 4 agents, on POINT-IN-TIME (clean) signals, 2026 true-OOS.
Entry = next-day open after each signal. For each day held D1..D20: avg close-return, avg running MFE (best),
avg running MAE (worst), % in profit. This is the exit study (target / stop / hold). Read-only."""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
AGENTS = {"Bedrock": 8787, "Vectoyx": 8349, "Nanoro": 8407, "Darayx": 7695}; NDAY = 20

print("loading + point-in-time features ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
tax = pd.read_sql_query("SELECT pattern_id,rule_json FROM falcon_pattern_taxonomy WHERE pattern_id IN (8787,8349,8407,7695)", con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-12-01' AND trade_date<='2026-07-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-09-15' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values; cl = g.close.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (cl-lo)/(hi-lo), np.nan), weekly_range_pct=np.where(cl > 0, (hi-lo)/cl*100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (cl/sm-1)*100, np.nan), weekly_breakout_20w=np.where(ph == ph, (cl > ph).astype(float), np.nan))))
FC = feat.drop(columns=["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
FC = FC[(FC.trade_date >= "2026-01-01") & (FC.trade_date <= "2026-07-31")]

# per-symbol OHLC arrays for fast forward slicing
SYM = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    SYM[s] = dict(dates=list(g.trade_date), o=g.open.values.astype(float), h=g.high.values, l=g.low.values, c=g.close.values, idx={d: i for i, d in enumerate(g.trade_date)})

def paths_for(rule):
    m = np.ones(len(FC), bool)
    for f, op, thr in rule: m &= OPS[op](FC[f].values, thr)
    sig = FC.loc[m, ["symbol", "trade_date"]]
    CL = np.full((len(sig), NDAY), np.nan); MF = np.full((len(sig), NDAY), np.nan); MA = np.full((len(sig), NDAY), np.nan)
    k = 0
    for r in sig.itertuples():
        S = SYM.get(r.symbol); i = S["idx"].get(r.trade_date) if S else None
        if i is None or i+1+NDAY > len(S["dates"]): continue
        e = S["o"][i+1]                                   # D1 = next-day open
        if e <= 0: continue
        hh = S["h"][i+1:i+1+NDAY]; ll = S["l"][i+1:i+1+NDAY]; cc = S["c"][i+1:i+1+NDAY]
        CL[k] = (cc/e-1)*100
        MF[k] = (np.maximum.accumulate(hh)/e-1)*100
        MA[k] = (np.minimum.accumulate(ll)/e-1)*100
        k += 1
    return CL[:k], MF[:k], MA[:k]

for name, pid in AGENTS.items():
    rule = json.loads(tax[tax.pattern_id == pid].rule_json.iloc[0])
    CL, MF, MA = paths_for(rule)
    print("\n" + "="*70)
    print(f"{name}  —  {CL.shape[0]} signals (point-in-time, 2026)  ·  entry = next-day open")
    print("="*70)
    print(f"  {'Day':<5}{'avg close%':>12}{'avg MFE%':>11}{'avg MAE%':>11}{'% in profit':>13}")
    for d in range(NDAY):
        c = CL[:, d]; c = c[~np.isnan(c)]
        print(f"  D{d+1:<4}{np.nanmean(CL[:,d]):>+12.2f}{np.nanmean(MF[:,d]):>+11.2f}{np.nanmean(MA[:,d]):>+11.2f}{(c>0).mean()*100:>12.0f}%")
