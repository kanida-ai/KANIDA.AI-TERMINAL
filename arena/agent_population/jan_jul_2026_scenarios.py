"""JAN-JUL 2026 two-scenario MONTHLY analysis for the 4 agents (point-in-time, Nifty-500, Rs25k/stock, net).
Scenario 1 INTRADAY: Long / Short (enter 9:15=open, exit same-day close).  Scenario 2 POSITIONAL (CNC long):
BTST (next-day close) / 5-day (5th-session close). Monthly avg return + win% per portfolio. -> Excel. Read-only."""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
AGENTS = {"Bedrock": 8787, "Vectoyx": 8349, "Nanoro": 8407, "Darayx": 7695}
ALLOC = 25000.0; COST_ID = 0.15; COST_POS = 0.30; LO, HI = "2026-01-01", "2026-07-31"

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
tax = pd.read_sql_query("SELECT pattern_id,rule_json FROM falcon_pattern_taxonomy WHERE pattern_id IN (8787,8349,8407,7695)", con)
n500 = set(pd.read_sql_query("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1", con).symbol)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-11-01' AND trade_date<=?", con, params=(HI,))
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
FC = FC[(FC.symbol.isin(n500)) & (FC.trade_date >= LO) & (FC.trade_date <= HI)]
SYM = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    SYM[s] = dict(o=g.open.values.astype(float), h=g.high.values, l=g.low.values, c=g.close.values, idx={d: i for i, d in enumerate(g.trade_date)}, dates=list(g.trade_date))

def trades_for(rule):
    m = np.ones(len(FC), bool)
    for f, op, thr in rule: m &= OPS[op](FC[f].values, thr)
    out = []
    for r in FC.loc[m, ["symbol", "trade_date"]].itertuples():
        S = SYM.get(r.symbol); i = S["idx"].get(r.trade_date) if S else None
        if i is None or i+6 >= len(S["dates"]): continue
        e = S["o"][i+1]
        if e <= 0: continue
        out.append(dict(symbol=r.symbol, m=r.trade_date[:7],
            id_long=(S["c"][i+1]/e-1)*100-COST_ID, id_short=(e/S["c"][i+1]-1)*100-COST_ID,
            btst=(S["c"][i+2]/e-1)*100-COST_POS, hold5=(S["c"][i+5]/e-1)*100-COST_POS,
            mfe5=(S["h"][i+1:i+6].max()/e-1)*100, mae5=(S["l"][i+1:i+6].min()/e-1)*100))
    return pd.DataFrame(out)

PORT = [("ID-Long", "id_long"), ("ID-Short", "id_short"), ("BTST", "btst"), ("5-day", "hold5")]
months = [f"2026-{m:02d}" for m in range(1, 8)]
xl = {}
print("JAN-JUL 2026 · 4 agents · Nifty-500 · point-in-time · Rs25k/stock · net · avg return %/trade (win% in ())\n")
for name, pid in AGENTS.items():
    T = trades_for(json.loads(tax[tax.pattern_id == pid].rule_json.iloc[0]))
    if T.empty: print(f"{name}: no signals\n"); continue
    print(f"================ {name}  ({len(T)} trades) ================")
    print(f"  {'Month':<9}{'sigs':>6}" + "".join(f"{p[0]:>16}" for p in PORT))
    rows = []
    for mo in months:
        g = T[T.m == mo]
        if g.empty: continue
        cells = [f"{g[c].mean():+.2f} ({(g[c]>0).mean()*100:.0f}%)" for _, c in PORT]
        print(f"  {mo:<9}{len(g):>6}" + "".join(f"{c:>16}" for c in cells))
        rows.append(dict(month=mo, signals=len(g), **{p[0]: round(T[T.m == mo][p[1]].mean(), 2) for p in PORT}))
    tot = [f"{T[c].mean():+.2f} ({(T[c]>0).mean()*100:.0f}%)" for _, c in PORT]
    print(f"  {'TOTAL':<9}{len(T):>6}" + "".join(f"{c:>16}" for c in tot))
    print(f"    5-day avg MFE {T.mfe5.mean():+.2f}%  avg MAE {T.mae5.mean():+.2f}%\n")
    xl[name] = pd.DataFrame(rows)

out = os.path.join(ROOT, "docs", "ops", "JAN_JUL_2026_SCENARIOS.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    for name, df in xl.items(): df.to_excel(w, f"{name}_monthly"[:31], index=False)
print(f"Excel -> {out}")
