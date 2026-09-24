"""Plain: FIVESTAR Jan-23 -> number of patterns fired, sum of lifts, avg. Compare to the #1 and #15 stocks.
Then count how many OTHER stocks that day look like FIVESTAR (same pullback shape / fire my rules). Read-only.
"""
import os, sys, sqlite3, warnings, json
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
SD = "2025-01-23"
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con); feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; SHP = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); v = g.volume.values.astype(float); td = g.trade_date.values; n = len(c)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=td, weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (h - l) / c * 100, np.nan), weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan; hi20 = pd.Series(h).rolling(20).max().values; lo20 = pd.Series(l).rolling(20).min().values
    base_tight20 = (hi20 - lo20) / c * 100; dh20 = (c / hi20 - 1) * 100; run20 = pd.Series(c).pct_change(20).values * 100
    trp = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc))) / c * 100; atrp = pd.Series(trp).rolling(20).mean().values
    av20 = pd.Series(v).rolling(20).mean().values; v3_20 = pd.Series(v).rolling(3).mean().values / av20; v5_20 = pd.Series(v).rolling(5).mean().values / av20
    sma200 = pd.Series(c).rolling(200).mean().values; d_sma200 = (c / sma200 - 1) * 100
    wk = g.wk.values; grp = pd.Series(wk).ne(pd.Series(wk).shift()).cumsum().values; wtd_pos = np.full(n, np.nan)
    for gg in np.unique(grp):
        ii = np.where(grp == gg)[0]; wh2 = np.maximum.accumulate(h[ii]); wl2 = np.minimum.accumulate(l[ii]); wtd_pos[ii] = np.where(wh2 > wl2, (c[ii] - wl2) / (wh2 - wl2), np.nan)
    for idx in range(n):
        if td[idx] == SD: SHP[s] = dict(base_tight20=base_tight20[idx], dh20=dh20[idx], run20=run20[idx], atrp=atrp[idx], v3_20=v3_20[idx], v5_20=v5_20[idx], d_sma200=d_sma200[idx], wtd_pos=wtd_pos[idx])
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
rdf = pd.read_csv(os.path.join(ROOT, "arena", "agent_population", "operator_mined_rules.csv")); MYR = [json.loads(r.rule_json) for _, r in rdf.iterrows()]
def op_hits(f):
    if not f: return 0
    n = 0
    for rule in MYR:
        ok = True
        for ft, op, th in rule:
            val = f.get(ft)
            if val is None or (isinstance(val, float) and val != val): ok = False; break
            if op == "<=" and not val <= th: ok = False; break
            if op == ">" and not val > th: ok = False; break
        n += ok
    return n
fd = FCpit[FCpit.trade_date == SD]; syms = fd.symbol.values
X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
for j, col in enumerate(FR.FEATURE_COLS):
    if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
yr = int(SD[:4]); ll = [[] for _ in range(len(syms))]
for p in pats:
    if int(p["mined_year"]) >= yr: continue
    m = FR.rule_mask(p["rule"], X)
    for i in np.where(m)[0]: ll[i].append(p["oos_lift"] or 0)
rows = []
for i, s in enumerate(syms):
    lifts = ll[i]; nf = len(lifts)
    if nf < 10: continue
    rows.append(dict(symbol=s, n_pat=nf, sum_lift=round(sum(lifts), 0), avg_lift=round(sum(lifts) / nf, 2)))
B = pd.DataFrame(rows).sort_values("sum_lift", ascending=False).head(100)
B = B.sort_values("avg_lift", ascending=False).reset_index(drop=True); B["rank"] = B.index + 1
fs = B[B.symbol == "FIVESTAR"].iloc[0]
print("========== PLAIN: what the numbers mean (FIVESTAR, Jan-23) ==========")
print(f"  number of patterns fired = how many of Falcon's rules were TRUE for the stock")
print(f"  sum of lifts             = add up the 'lift' of every rule that fired")
print(f"  avg lift (Falcon's rank) = sum of lifts / number of patterns\n")
print(f"  FIVESTAR:  {fs.n_pat} patterns fired,  sum of lifts = {fs.sum_lift:.0f},  avg = {fs.avg_lift}  -> rank #{fs['rank']}")
print(f"  #1 stock ({B.iloc[0].symbol}):  {B.iloc[0].n_pat} patterns,  sum = {B.iloc[0].sum_lift:.0f},  avg = {B.iloc[0].avg_lift}")
print(f"  #15 stock ({B.iloc[14].symbol}, the cutoff): {B.iloc[14].n_pat} patterns, sum = {B.iloc[14].sum_lift:.0f}, avg = {B.iloc[14].avg_lift}")
print(f"\n  => FIVESTAR has a BIG sum ({fs.sum_lift:.0f}) but firing {fs.n_pat} patterns DIVIDES it down to avg {fs.avg_lift},")
print(f"     which is below the #15 cutoff ({B.iloc[14].avg_lift}). Sum ranks it high, avg ranks it low.\n")
# ---- look-alikes ----
ah = {s: op_hits(SHP.get(s)) for s in syms}
allsh = pd.DataFrame([dict(symbol=s, **SHP[s], op=ah.get(s, 0)) for s in SHP])
like_shape = allsh[(allsh.base_tight20 > 15) & (allsh.dh20 < -8) & (allsh.wtd_pos > 0.5)]
like_rules5 = allsh[allsh.op >= 5]; like_rules3 = allsh[allsh.op >= 3]
near_avg = B[(B.avg_lift >= 8.5) & (B.avg_lift <= 10.5)]
print("========== how many stocks look like FIVESTAR on Jan-23 ==========")
print(f"  same pullback SHAPE (wide base + pulled back + top of week):  {len(like_shape)} stocks")
print(f"  fire >=5 of my operator rules (like FIVESTAR's 5):            {len(like_rules5)} stocks")
print(f"  fire >=3 of my operator rules:                               {len(like_rules3)} stocks")
print(f"  Falcon avg-lift near FIVESTAR's (8.5-10.5), i.e. ranked ~same: {len(near_avg)} stocks")
print(f"\n  a few that look just like FIVESTAR: {', '.join(like_shape.symbol.head(12).tolist())}")
