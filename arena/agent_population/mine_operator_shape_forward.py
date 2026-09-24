"""MINE the operator's winning SETUP (base->breakout->pullback->re-breakout) using ONLY daily OHLCV + WTD + MTD.
Reuses the Falcon v7.1 miner method: shallow decision trees -> leaf-path rules with precision / support /
lift-over-base-rate. Label = operator winners (next-day open->close > 0.3%). Mine on a TRAIN window, then score
every rule's lift OUT-OF-SAMPLE so only durable rules survive. Leak-free (PIT features, forward split). Read-only.
"""
import os, sys, sqlite3, warnings, importlib.util, json
from collections import defaultdict
import numpy as np, pandas as pd
from sklearn.tree import DecisionTreeClassifier, _tree
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WIN_THR = 0.3
spec = importlib.util.spec_from_file_location("op8", os.path.join(ROOT, "arena", "agent_population", "operator_picks_8mo.py"))
op8 = importlib.util.module_from_spec(spec); spec.loader.exec_module(op8); PICKS = op8.PICKS

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2023-06-01' AND trade_date<='2026-07-31' ORDER BY symbol,trade_date", con)
con.close()
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None

# ---- SHAPE features (daily OHLCV + WTD + MTD only) ----
FEATS = ["run20", "run40", "run60", "dh5", "dh10", "dh20", "dh40", "dh60", "dl20", "dl40",
         "d_sma50", "d_sma200", "slope20", "slope50", "atrp", "atr5v20", "rng_contract",
         "v3_20", "v5_20", "v_ratio", "cloc1", "cloc3", "updays5", "updays10", "downstreak",
         "base_tight20", "days_since_hi20", "wtd_ret", "wtd_pos", "wtd_daysup", "mtd_ret", "mtd_pos", "mtd_daysup"]
rows = []; OC = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float)
    o = g.open.values.astype(float); v = g.volume.values.astype(float); td = g.trade_date.values; n = len(c)
    dt = pd.to_datetime(g.trade_date); iso = dt.dt.isocalendar()
    wk = (iso.year.astype(int) * 100 + iso.week.astype(int)).values; mo = dt.dt.strftime("%Y-%m").values
    pc = np.roll(c, 1); pc[0] = np.nan; ret1 = (c / pc - 1) * 100
    rr = lambda k: (pd.Series(c).pct_change(k).values) * 100
    run20, run40, run60 = rr(20), rr(40), rr(60)
    def dh(k): m = pd.Series(h).rolling(k).max().values; return (c / m - 1) * 100
    def dl(k): m = pd.Series(l).rolling(k).min().values; return (c / m - 1) * 100
    dh5, dh10, dh20, dh40, dh60 = dh(5), dh(10), dh(20), dh(40), dh(60); dl20, dl40 = dl(20), dl(40)
    sma50 = pd.Series(c).rolling(50).mean().values; sma200 = pd.Series(c).rolling(200).mean().values
    d_sma50 = (c / sma50 - 1) * 100; d_sma200 = (c / sma200 - 1) * 100
    sma20 = pd.Series(c).rolling(20).mean().values
    slope20 = (sma20 / np.roll(sma20, 5) - 1) * 100; slope20[:25] = np.nan
    slope50 = (sma50 / np.roll(sma50, 5) - 1) * 100; slope50[:55] = np.nan
    tr = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc))); trp = tr / c * 100
    atrp = pd.Series(trp).rolling(20).mean().values; atr5 = pd.Series(trp).rolling(5).mean().values
    atr5v20 = atr5 / atrp
    rng_contract = pd.Series((h - l) / c * 100).rolling(3).mean().values / (pd.Series((h - l) / c * 100).rolling(15).mean().values + 1e-9)
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values; av5 = pd.Series(v).rolling(5).mean().values
    v3_20 = av3 / av20; v5_20 = av5 / av20; v_ratio = v / av20
    cloc = np.where(h > l, (c - l) / (h - l), np.nan); cloc1 = cloc; cloc3 = pd.Series(cloc).rolling(3).mean().values
    updays5 = pd.Series(ret1 > 0).rolling(5).sum().values; updays10 = pd.Series(ret1 > 0).rolling(10).sum().values
    neg = (ret1 < 0).astype(int); ds = np.zeros(n)
    for i in range(n):
        k = 0
        while i - k >= 0 and neg[i - k] == 1: k += 1
        ds[i] = k
    hi20s = pd.Series(h).rolling(20).max().values; lo20s = pd.Series(l).rolling(20).min().values
    base_tight20 = (hi20s - lo20s) / c * 100
    days_since_hi20 = np.full(n, np.nan)
    for i in range(n):
        if i >= 19:
            w = h[i - 19:i + 1]; days_since_hi20[i] = 19 - int(np.argmax(w))
    def cw(key):
        r = np.full(n, np.nan); p = np.full(n, np.nan); du = np.full(n, np.nan); ser = pd.Series(key); grp = ser.ne(ser.shift()).cumsum()
        for _, idx in pd.Series(range(n)).groupby(grp.values):
            ii = idx.values; wh = np.maximum.accumulate(h[ii]); wl = np.minimum.accumulate(l[ii])
            base = pc[ii[0]] if not np.isnan(pc[ii[0]]) else c[ii[0]]
            r[ii] = (c[ii] / base - 1) * 100; p[ii] = np.where(wh > wl, (c[ii] - wl) / (wh - wl), np.nan); du[ii] = np.cumsum((ret1[ii] > 0).astype(float))
        return r, p, du
    wtd_ret, wtd_pos, wtd_daysup = cw(wk); mtd_ret, mtd_pos, mtd_daysup = cw(mo)
    loc = dict(run20=run20, run40=run40, run60=run60, dh5=dh5, dh10=dh10, dh20=dh20, dh40=dh40, dh60=dh60,
               dl20=dl20, dl40=dl40, d_sma50=d_sma50, d_sma200=d_sma200, slope20=slope20, slope50=slope50,
               atrp=atrp, atr5v20=atr5v20, rng_contract=rng_contract, v3_20=v3_20, v5_20=v5_20, v_ratio=v_ratio,
               cloc1=cloc1, cloc3=cloc3, updays5=updays5, updays10=updays10, downstreak=ds,
               base_tight20=base_tight20, days_since_hi20=days_since_hi20, wtd_ret=wtd_ret, wtd_pos=wtd_pos,
               wtd_daysup=wtd_daysup, mtd_ret=mtd_ret, mtd_pos=mtd_pos, mtd_daysup=mtd_daysup)
    for i in range(n):
        OC[(s, td[i])] = ((c[i] - o[i]) / o[i] * 100) if o[i] > 0 else np.nan
        rows.append((s, td[i], *[loc[k][i] for k in FEATS]))
P = pd.DataFrame(rows, columns=["symbol", "trade_date"] + FEATS)
LOWR = {}
for s, g in oh.groupby("symbol", sort=False):
    for _, r in g.iterrows(): LOWR[(s, r.trade_date)] = ((r.low - r.open) / r.open * 100) if r.open > 0 else np.nan
print(f"shape-feature panel {len(P):,} rows")

# ---- load durable mined rules (built on <=May'25 only) and score FORWARD Jun'25->Jul'26 ----
import ast
rdf = pd.read_csv(os.path.join(ROOT, "arena", "agent_population", "operator_mined_rules.csv"))
RULES = [(json.loads(r.rule_json), float(r.lift_te)) for _, r in rdf.iterrows()]
print(f"loaded {len(RULES)} durable mined rules")
def rmask(df, rule):
    m = np.ones(len(df), bool)
    for f, op, th in rule:
        col = df[f].values; m &= (col <= th) if op == "<=" else (col > th); m &= ~np.isnan(col)
    return m
def score(day):
    sc = np.zeros(len(day))
    for rule, lift in RULES: sc += rmask(day, rule).astype(float) * np.log(lift)
    return sc
STOP = 4.0
fwd_days = [d for d in cal if "2025-06-01" <= d <= "2026-07-27" and nxt(d)]
recs = []
for sd in fwd_days:
    day = P[P.trade_date == sd].copy()
    if len(day) < 30: continue
    day["sc"] = score(day)
    breadth = float((day.d_sma50.values > 0).mean())          # regime: share of universe above 50-DMA
    br20 = float((day.run20.values > 0).mean())               # share up over 20d
    top = day.sort_values("sc", ascending=False).head(15); td = nxt(sd)
    oc = np.array([OC.get((s, td), np.nan) for s in top.symbol]); low = np.array([LOWR.get((s, td), np.nan) for s in top.symbol])
    stopped = np.where(low <= -STOP, -STOP, oc)
    recs.append(dict(mo=td[:7], ret1x=np.nanmean(oc), ret1x_stop=np.nanmean(stopped), breadth=breadth, br20=br20))
D = pd.DataFrame(recs); D["ret5x"] = D.ret1x * 5; D["ret5x_stop"] = D.ret1x_stop * 5
print("\n===== MINED-SHAPE SCREENER, LEAK-PROOF FORWARD (top-15, Jun'25->Jul'26) =====")
print(f"{'month':<9}{'1x':>8}{'5x':>8}{'win%':>7}{'avg breadth':>12}")
for mo, g in D.groupby("mo"):
    print(f"  {mo:<7}{g.ret1x.sum():>+7.1f}%{g.ret5x.sum():>+7.1f}%{(g.ret1x>0).mean()*100:>6.0f}%{g.breadth.mean()*100:>11.0f}%")
print(f"\n  UNGATED TOTAL {len(D)} days · 1x {D.ret1x.sum():+.0f}% · 5x {D.ret5x.sum():+.0f}% · daily avg 1x {D.ret1x.mean():+.2f}%")
print("\n  --- REGIME-GATED (only trade when breadth >= threshold; else flat) ---")
print(f"  {'gate':<20}{'days traded':>12}{'1x tot':>9}{'5x tot':>9}{'daily1x':>9}{'win%':>7}")
for col, thr in [("breadth", 0.45), ("breadth", 0.50), ("breadth", 0.55), ("br20", 0.45), ("br20", 0.55)]:
    g = D[D[col] >= thr]
    if len(g) == 0: continue
    print(f"  {col}>={thr:<15}{len(g):>12}{g.ret1x.sum():>+8.1f}%{g.ret5x.sum():>+8.1f}%{g.ret1x.mean():>+8.2f}%{(g.ret1x>0).mean()*100:>6.0f}%")
import sys as _s; _s.exit(0)
# (unreachable legacy below)
sig = []
for tdte, syms in PICKS.items():
    s = prev(tdte)
    if s is None: continue
    day = P[P.trade_date == s].copy()
    if day.empty: continue
    won = {sym for sym in syms if not np.isnan(OC.get((sym, tdte), np.nan)) and OC.get((sym, tdte), -9) > WIN_THR}
    day["y"] = day.symbol.isin(won).astype(int); day["sd"] = s; day["mo"] = tdte[:7]
