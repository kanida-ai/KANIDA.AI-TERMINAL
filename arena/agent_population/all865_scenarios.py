"""ALL 865 patterns · two-scenario four-portfolio performance · Jan-Jul 2026 · point-in-time · Nifty-500 · Rs25k/stock · net.
Portfolios: Intraday-Long, Intraday-Short (exit same-day close), BTST (next-day close), 5-day (5th-session close).
Outcomes precomputed ONCE per (symbol, signal-day); each of the 865 rules scored against them (vectorized).
-> docs/ops/ALL865_SCENARIOS.xlsx (per-pattern table + summary). Read-only."""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
COST_ID = 0.15; COST_POS = 0.30; LO, HI = "2026-01-01", "2026-07-31"; MINSIG = 20

print("loading + point-in-time features + outcomes ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
tax = pd.read_sql_query("SELECT pattern_id,target,regime,mined_year,rule_json FROM falcon_pattern_taxonomy", con)
n500 = set(pd.read_sql_query("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1", con).symbol)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-11-01' AND trade_date<=?", con, params=(HI,))
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-09-15' ORDER BY symbol,trade_date", con); con.close()
cn = dict(pd.read_sql_query("SELECT agent_id,codename FROM agent_summary", sqlite3.connect("file:"+os.path.join(AP,"arena_metrics.db").replace("\\","/")+"?mode=ro",uri=True)).values)
# point-in-time weekly features
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
FC = FC[(FC.symbol.isin(n500)) & (FC.trade_date >= LO) & (FC.trade_date <= HI)].reset_index(drop=True)

# outcomes per panel row (once)
SYM = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    SYM[s] = dict(o=g.open.values.astype(float), h=g.high.values, l=g.low.values, c=g.close.values, idx={d: i for i, d in enumerate(g.trade_date)}, dates=list(g.trade_date))
idl = np.full(len(FC), np.nan); ids = np.full(len(FC), np.nan); bt = np.full(len(FC), np.nan); h5 = np.full(len(FC), np.nan); mf = np.full(len(FC), np.nan); ma = np.full(len(FC), np.nan)
for k, (sym, d) in enumerate(zip(FC.symbol.values, FC.trade_date.values)):
    S = SYM.get(sym); i = S["idx"].get(d) if S else None
    if i is None or i+6 >= len(S["dates"]): continue
    e = S["o"][i+1]
    if e <= 0: continue
    idl[k] = (S["c"][i+1]/e-1)*100-COST_ID; ids[k] = (e/S["c"][i+1]-1)*100-COST_ID
    bt[k] = (S["c"][i+2]/e-1)*100-COST_POS; h5[k] = (S["c"][i+5]/e-1)*100-COST_POS
    mf[k] = (S["h"][i+1:i+6].max()/e-1)*100; ma[k] = (S["l"][i+1:i+6].min()/e-1)*100
OUTM = {"id_long": idl, "id_short": ids, "btst": bt, "hold5": h5}
valid = ~np.isnan(idl)
print(f"  panel rows {len(FC)} (valid outcomes {valid.sum()}) · scoring {len(tax)} patterns ...", flush=True)

X = {c: FC[c].values for c in FC.columns if FC[c].dtype.kind in "fi"}
rows = []
for a in tax.itertuples():
    try: rule = json.loads(a.rule_json)
    except Exception: continue
    m = valid.copy(); ok = True
    for f, op, thr in rule:
        if f not in X: ok = False; break
        m &= OPS[op](X[f], thr)
    if not ok: continue
    n = int(m.sum())
    if n < MINSIG: continue
    r = dict(pattern_id=a.pattern_id, agent=cn.get(f"FALCPAT_{a.pattern_id}", ""), regime=a.regime, n=n)
    for lab, v in OUTM.items():
        vv = v[m]; r[f"{lab}_avg"] = round(float(np.nanmean(vv)), 3); r[f"{lab}_win"] = round(float((vv > 0).mean()*100), 0)
    rows.append(r)
R = pd.DataFrame(rows)
print(f"  scored {len(R)} patterns with >={MINSIG} signals\n")

print("="*82)
print(f"ALL 865 PATTERNS · Jan-Jul 2026 · {len(R)} with >={MINSIG} signals · avg %/trade, net")
print("="*82)
print(f"  {'portfolio':<16}{'% patterns +ve':>16}{'mean avg%':>12}{'median avg%':>13}{'best avg%':>11}")
for lab in ["id_long", "id_short", "btst", "hold5"]:
    c = R[f"{lab}_avg"]; pos = (c > 0).mean()*100
    print(f"  {lab:<16}{pos:>15.0f}%{c.mean():>+12.3f}{c.median():>+13.3f}{c.max():>+11.2f}")
print(f"\n  -> best structure by share of profitable patterns: {max(['id_long','id_short','btst','hold5'], key=lambda l:(R[l+'_avg']>0).mean())}")
for lab, title in [("hold5", "5-DAY"), ("btst", "BTST"), ("id_short", "INTRADAY-SHORT"), ("id_long", "INTRADAY-LONG")]:
    print(f"\n  TOP 8 by {title} (avg%/trade, win%, n):")
    for _, x in R.sort_values(f"{lab}_avg", ascending=False).head(8).iterrows():
        print(f"    {('FALCPAT_%d'%x.pattern_id):<14}{str(x.agent):<12}{x[lab+'_avg']:>+7.2f}%  win {x[lab+'_win']:>3.0f}%  n={int(x.n)}")
out = os.path.join(ROOT, "docs", "ops", "ALL865_SCENARIOS.xlsx")
R.sort_values("hold5_avg", ascending=False).to_excel(out, index=False)
print(f"\nExcel (all {len(R)} patterns x 4 portfolios) -> {out}")
