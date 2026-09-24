"""Falcon TOP-15 daily signal names + TIER, LEAK-FREE (point-in-time week-to-date features), for every trading day
in Jan 2026 and Jul 2026. Uses the production pattern set + score (validated bit-exact vs falcon_signals_live);
weekly features recomputed week-to-date so mid-week days don't leak. Saves to Downloads. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}; TOPN = 15; MINF = 10
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}
def classify(sret, twoday, rng, avg_lift, trend3_20, turn_pct):
    if sret is None or not np.isfinite(sret): return "UNKNOWN"
    if sret > 10: return "AVOID"
    if sret > 7 and np.isfinite(turn_pct or np.nan) and turn_pct >= 0.75: return "AVOID"
    if sret <= 2 and np.isfinite(twoday or np.nan) and twoday < -5 and avg_lift and avg_lift > 15: return "PREMIUM-Pullback"
    if sret <= 2 and np.isfinite(rng or np.nan) and rng < 2 and avg_lift and avg_lift > 15: return "PREMIUM-Compression"
    if sret <= 2 and np.isfinite(trend3_20 or np.nan) and trend3_20 < 0.9: return "ENTERPRISE-Dryup"
    if sret <= 2 and np.isfinite(turn_pct or np.nan) and turn_pct < 0.75: return "GOLD"
    if sret <= 2: return "GOLD-baseline"
    if sret <= 5: return "STANDARD"
    return "STANDARD-weak"

print("loading + point-in-time features ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
n500 = set(pd.read_sql_query("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1", con).symbol)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-11-01' AND trade_date<='2026-07-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-07-31' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values; l = g.low.values; v = g.volume.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c-lo)/(hi-lo), np.nan), weekly_range_pct=np.where(c > 0, (hi-lo)/c*100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c/sm-1)*100, np.nan), weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c/pc-1)*100; rng = (h-l)/pc*100; twoday = (c/c2-1)*100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values; tr3 = np.where(av20 > 0, av3/av20, np.nan)
    tp = pd.Series(c*v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    for i, d in enumerate(g.trade_date.values): TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])
FC = feat.drop(columns=["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")

def top15(day):
    fd = FC[(FC.trade_date == day) & (FC.symbol.isin(n500))]
    if fd.empty: return []
    syms = fd.symbol.values; n = len(fd); score = np.zeros(n); fire = np.zeros(n)
    for p in pats:
        m = np.ones(n, bool); ok = True
        for f, op, th in p["rule"]:
            if f not in fd.columns: ok = False; break
            m &= OPS[op](fd[f].values, th)
        if not ok: continue
        score += m*(float(p["oos_lift"]) if p["oos_lift"] is not None else 0.0); fire += m
    idx = np.where(fire >= MINF)[0]
    order = idx[np.argsort(-score[idx])][:TOPN]
    rows = []
    for rk, i in enumerate(order, 1):
        al = score[i]/max(fire[i], 1); tf = TF.get((syms[i], day), (np.nan,)*5)
        tier = classify(tf[0], tf[2], tf[1], al, tf[3], tf[4])
        rows.append(dict(signal_date=day, rank=rk, symbol=syms[i], n_fires=int(fire[i]), score=round(float(score[i]), 1),
                         avg_lift=round(float(al), 2), tier=tier, high_tier=("YES" if tier in HIGH else "")))
    return rows

months = {"January": ("2026-01-01", "2026-01-31"), "July": ("2026-07-01", "2026-07-31")}
DL = os.path.join(os.path.expanduser("~"), "Downloads"); os.makedirs(DL, exist_ok=True)
out = os.path.join(DL, "FALCON_TOP15_JAN_JUL_2026_PIT_N500.xlsx")
sheets = {}
with pd.ExcelWriter(out, engine="openpyxl") as w:
    for mname, (lo, hi) in months.items():
        days = [d for d in sorted(FC[(FC.trade_date >= lo) & (FC.trade_date <= hi)].trade_date.unique())]
        allrows = []
        for d in days: allrows += top15(d)
        df = pd.DataFrame(allrows); sheets[mname] = df
        df.to_excel(w, mname, index=False)
        print(f"\n{mname} 2026: {df.signal_date.nunique()} trading days, {len(df)} picks")
        print(df[df.signal_date == days[0]][["signal_date", "rank", "symbol", "n_fires", "tier"]].to_string(index=False))
print(f"\nSaved -> {out}")
