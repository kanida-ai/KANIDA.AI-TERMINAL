"""REBUILD the 4 agents on POINT-IN-TIME features. Recompute the 4 weekly columns WEEK-TO-DATE (Monday->today,
validated 0.0000 vs the live engine) and substitute them; keep each agent's exact rule. Then measure the honest
2026 (true-OOS) forward performance of each — LEAKY-STORED vs POINT-IN-TIME, side by side.
Outcome = native target: buy next open, WIN if High reaches +X% within Y days else exit at Y-day close, net 0.15%.
Read-only."""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}; COST = 0.15
AGENTS = {"Bedrock": 8787, "Vectoyx": 8349, "Nanoro": 8407, "Darayx": 7695}
TARGETS = {"hit_25pc_30d": (25, 30), "hit_10pc_20d": (10, 20)}

print("loading + recomputing point-in-time weekly features ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
tax = pd.read_sql_query("SELECT pattern_id,target,rule_json FROM falcon_pattern_taxonomy WHERE pattern_id IN (8787,8349,8407,7695)", con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2026-07-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-09-15' ORDER BY symbol,trade_date", con); con.close()
# week-to-date recompute (validated 0.0000 vs live)
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values; cl = g.close.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (cl-lo)/(hi-lo), np.nan), weekly_range_pct=np.where(cl > 0, (hi-lo)/cl*100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (cl/sm-1)*100, np.nan), weekly_breakout_20w=np.where(ph == ph, (cl > ph).astype(float), np.nan))))
WTD = pd.concat(rec, ignore_index=True)
FEAT_CLEAN = feat.drop(columns=["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]).merge(WTD, on=["symbol", "trade_date"], how="left")

# native target outcomes (buy next open; hit if High>=+X% within Y days else Y-day close close; net cost)
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
outs = []
for s, g in oh.groupby("symbol", sort=False):
    O = g.open.values.astype(float); H = g.high.values; C = g.close.values; entry = np.roll(O, -1); entry[-1] = np.nan
    d = {"symbol": s, "trade_date": g.trade_date.values}
    for tn, (pct, Hh) in TARGETS.items():
        fmax = pd.Series(H).rolling(Hh).max().shift(-Hh).values; cH = pd.Series(C).shift(-Hh).values
        hit = fmax >= entry*(1+pct/100); ret = np.where(hit, float(pct), (cH-entry)/entry*100) - COST
        bad = np.isnan(entry) | np.isnan(cH); ret[bad] = np.nan; hh = hit.astype(float); hh[bad] = np.nan
        d[f"ret_{tn}"] = ret; d[f"hit_{tn}"] = hh
    outs.append(pd.DataFrame(d))
OUT = pd.concat(outs, ignore_index=True)
Mleak = feat.merge(OUT, on=["symbol", "trade_date"], how="inner"); Mclean = FEAT_CLEAN.merge(OUT, on=["symbol", "trade_date"], how="inner")
Mleak["yr"] = Mleak.trade_date.str[:4].astype(int); Mclean["yr"] = Mclean.trade_date.str[:4].astype(int)

def evaluate(M, rule, tgt, year):
    m = np.ones(len(M), bool)
    for f, op, thr in rule:
        if f not in M.columns: return None
        m &= OPS[op](M[f].values, thr)
    idx = m & (M.yr.values == year)
    r = M.loc[idx, f"ret_{tgt}"].dropna(); h = M.loc[idx, f"hit_{tgt}"].dropna()
    if len(r) < 1: return dict(n=0)
    return dict(n=len(r), hit=h.mean()*100, ret=r.mean(), win=(r > 0).mean()*100)

print("\n" + "="*88)
print("REBUILT 4 AGENTS · point-in-time features · 2026 TRUE-OOS forward performance (native target)")
print("="*88)
print(f"  {'agent':<10}{'target':<14}{'':<6}{'fires':>7}{'hit%':>7}{'avg ret%':>10}{'win%':>7}")
for name, pid in AGENTS.items():
    row = tax[tax.pattern_id == pid].iloc[0]; rule = json.loads(row.rule_json); tgt = row.target
    uses_wk = any(f.startswith("weekly_") for f, _, _ in rule)
    L = evaluate(Mleak, rule, tgt, 2026); C = evaluate(Mclean, rule, tgt, 2026)
    tag = "(no weekly - unchanged)" if not uses_wk else "(weekly -> point-in-time)"
    print(f"\n  {name}  {tag}")
    for lab, R in [("LEAKY-stored", L), ("POINT-IN-TIME", C)]:
        if R and R["n"]:
            print(f"    {'':<24}{lab:<15}{R['n']:>7}{R['hit']:>6.0f}%{R['ret']:>+10.2f}{R['win']:>6.0f}%")
        else:
            print(f"    {'':<24}{lab:<15}{'0':>7}   (no fires)")
    if L and C and L.get("n") and C.get("n"):
        print(f"    -> leak impact: fires {L['n']}->{C['n']}, hit {L['hit']:.0f}%->{C['hit']:.0f}%, avg ret {L['ret']:+.2f}%->{C['ret']:+.2f}%")
print("\n  (all patterns mined <=2025, so 2026 is genuine out-of-sample. Bedrock uses no weekly -> identical both columns.)")
