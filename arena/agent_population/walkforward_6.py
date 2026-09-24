"""WALK-FORWARD validation of 6 patterns (Atlas, Talon, Phi, Beta, Lambda + Bedrock) on the 5-day hold,
point-in-time features, Nifty-500, net 0.30%. Per YEAR (2023-2026), marking IN-SAMPLE (year<=mined) vs OOS
(year>mined). A pattern is durable only if it stays positive in its OOS years. Read-only."""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}; COST = 0.30
IDS = {9499: "Atlas", 9189: "Talon", 6467: "Phi", 8734: "Beta", 9584: "Lambda", 8787: "Bedrock"}

print("loading (2023-2026) + point-in-time features + 5-day outcome ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
tax = pd.read_sql_query("SELECT pattern_id,mined_year,rule_json FROM falcon_pattern_taxonomy WHERE pattern_id IN (9499,9189,6467,8734,9584,8787)", con)
n500 = set(pd.read_sql_query("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1", con).symbol)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2023-01-01' AND trade_date<='2026-07-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2022-06-01' AND trade_date<='2026-09-15' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []; out5 = []
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    O = g.open.values.astype(float); C = g.close.values.astype(float); cl = C
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (cl-lo)/(hi-lo), np.nan), weekly_range_pct=np.where(cl > 0, (hi-lo)/cl*100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (cl/sm-1)*100, np.nan), weekly_breakout_20w=np.where(ph == ph, (cl > ph).astype(float), np.nan))))
    en = np.roll(O, -1); ex = np.roll(C, -5); ret5 = (ex/en - 1)*100 - COST; ret5[-5:] = np.nan; ret5[en <= 0] = np.nan
    out5.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values, ret5=ret5)))
WTD = pd.concat(rec, ignore_index=True); OUT = pd.concat(out5, ignore_index=True)
FC = feat.drop(columns=["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]).merge(WTD, on=["symbol", "trade_date"], how="left").merge(OUT, on=["symbol", "trade_date"], how="left")
FC = FC[FC.symbol.isin(n500)].reset_index(drop=True); FC["yr"] = FC.trade_date.str[:4].astype(int)
X = {c: FC[c].values for c in FC.columns if FC[c].dtype.kind in "fi"}; R5 = FC.ret5.values; YR = FC.yr.values
YEARS = [2023, 2024, 2025, 2026]
print(f"panel {len(FC)} rows · point-in-time · 5-day hold, net {COST}%\n" + "="*76)
for a in tax.itertuples():
    rule = json.loads(a.rule_json); wk = any(f.startswith("weekly_") for f, _, _ in rule)
    m = np.ones(len(FC), bool); ok = True
    for f, op, thr in rule:
        if f not in X: ok = False; break
        m &= OPS[op](X[f], thr)
    print(f"\n{IDS[a.pattern_id]}  (FALCPAT_{a.pattern_id}, mined {a.mined_year}, {'weekly->PIT' if wk else 'daily-only'})")
    print(f"  {'year':<7}{'n':>7}{'5d avg%':>10}{'win%':>7}   sample")
    for y in YEARS:
        idx = m & (YR == y) & ~np.isnan(R5)
        v = R5[idx]
        if len(v) < 10: print(f"  {y:<7}{len(v):>7}       —       —   {'OOS' if y>a.mined_year else 'in-sample'}"); continue
        print(f"  {y:<7}{len(v):>7}{v.mean():>+10.2f}{(v>0).mean()*100:>6.0f}%   {'OOS <<' if y>a.mined_year else 'in-sample'}")
    # OOS-only verdict
    oosm = m & (YR > a.mined_year) & ~np.isnan(R5); vo = R5[oosm]
    yrs_oos = sorted(set(YR[oosm]))
    oos_by_yr = [R5[m & (YR == y) & ~np.isnan(R5)].mean() for y in yrs_oos if (m & (YR == y)).sum() >= 10]
    allpos = all(x > 0 for x in oos_by_yr) if oos_by_yr else False
    print(f"  -> OOS years {yrs_oos}: overall {vo.mean():+.2f}% ; every OOS year positive? {'YES - durable' if allpos and len(oos_by_yr)>=2 else ('single OOS year only' if len(oos_by_yr)==1 else 'NO')}")
