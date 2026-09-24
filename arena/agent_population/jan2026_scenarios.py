"""JAN-2026 two-scenario analysis for the 4 agents (point-in-time features, Nifty-500).
Signal = EOD each trading day in Jan 2026 -> trade next day 09:15 (=day open). Rs25,000/stock.
Scenario 1 INTRADAY (MIS, exit same-day close): Long + Short.
Scenario 2 POSITIONAL (CNC long): BTST (exit next-day close) + 5-day (exit 5th-session close) + MFE/MAE.
Separate trade logs + portfolio stats. -> docs/ops/JAN2026_SCENARIOS.xlsx. Read-only."""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
AGENTS = {"Bedrock": 8787, "Vectoyx": 8349, "Nanoro": 8407, "Darayx": 7695}
ALLOC = 25000.0; COST_ID = 0.15; COST_POS = 0.30

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
tax = pd.read_sql_query("SELECT pattern_id,rule_json FROM falcon_pattern_taxonomy WHERE pattern_id IN (8787,8349,8407,7695)", con)
n500 = set(pd.read_sql_query("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1", con).symbol)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-11-01' AND trade_date<='2026-01-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-03-15' ORDER BY symbol,trade_date", con); con.close()
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
FC = FC[(FC.symbol.isin(n500)) & (FC.trade_date >= "2026-01-01") & (FC.trade_date <= "2026-01-31")]

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
        e = S["o"][i+1]                                     # entry = next-day open (09:15)
        if e <= 0: continue
        c_id = S["c"][i+1]; c_bt = S["c"][i+2]; c_5 = S["c"][i+5]
        hi5 = S["h"][i+1:i+6].max(); lo5 = S["l"][i+1:i+6].min()
        out.append(dict(symbol=r.symbol, signal_date=r.trade_date, entry_date=S["dates"][i+1], entry=round(e, 2),
            id_long=(c_id/e-1)*100-COST_ID, id_short=(e/c_id-1)*100-COST_ID,
            btst=(c_bt/e-1)*100-COST_POS, hold5=(c_5/e-1)*100-COST_POS,
            mfe5=(hi5/e-1)*100, mae5=(lo5/e-1)*100))
    return pd.DataFrame(out)

def stat(v):
    v = np.array(v); return dict(n=len(v), avg=v.mean(), win=(v > 0).mean()*100, pnl=(v/100*ALLOC).sum())

logs = {}
print("JAN 2026 — 4 agents · Nifty-500 · point-in-time · Rs25,000/stock · net of costs\n")
for name, pid in AGENTS.items():
    T = trades_for(json.loads(tax[tax.pattern_id == pid].rule_json.iloc[0]))
    logs[name] = T
    if T.empty: print(f"=== {name}: no Jan-2026 signals ===\n"); continue
    L = stat(T.id_long); Sh = stat(T.id_short); B = stat(T.btst); H = stat(T.hold5)
    print(f"================ {name}  ({len(T)} signals, {T.entry_date.nunique()} trade-days) ================")
    print("  SCENARIO 1 — INTRADAY (enter 9:15, exit same-day close)")
    print(f"    Long  : avg {L['avg']:+.2f}%  win {L['win']:.0f}%  P&L Rs{L['pnl']:+,.0f}  ({L['n']} trades)")
    print(f"    Short : avg {Sh['avg']:+.2f}%  win {Sh['win']:.0f}%  P&L Rs{Sh['pnl']:+,.0f}  ({Sh['n']} trades)")
    print(f"    -> better intraday side: {'LONG' if L['avg']>Sh['avg'] else 'SHORT'}")
    print("  SCENARIO 2 — POSITIONAL (CNC long, enter 9:15)")
    print(f"    BTST  : avg {B['avg']:+.2f}%  win {B['win']:.0f}%  P&L Rs{B['pnl']:+,.0f}")
    print(f"    5-day : avg {H['avg']:+.2f}%  win {H['win']:.0f}%  P&L Rs{H['pnl']:+,.0f}  · avg MFE {T.mfe5.mean():+.2f}%  avg MAE {T.mae5.mean():+.2f}%")
    print(f"    -> better positional: {'BTST' if B['avg']>H['avg'] else '5-DAY'}\n")

out = os.path.join(ROOT, "docs", "ops", "JAN2026_SCENARIOS.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    summ = []
    for name, T in logs.items():
        if T.empty: continue
        for lab, col, cost in [("Intraday-Long", "id_long", 0), ("Intraday-Short", "id_short", 0), ("BTST", "btst", 0), ("5-day", "hold5", 0)]:
            s = stat(T[col]); summ.append(dict(agent=name, portfolio=lab, trades=s["n"], avg_ret_pct=round(s["avg"], 2), win_pct=round(s["win"], 0), pnl_rs=round(s["pnl"], 0)))
        T.round(2).to_excel(w, f"{name}_log"[:31], index=False)
    pd.DataFrame(summ).to_excel(w, "Summary", index=False)
print(f"Excel (trade logs + summary) -> {out}")
