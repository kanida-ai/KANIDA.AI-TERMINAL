"""Capture the BOX-1 setup: tight-base coil near highs + volume dry-up (FIVESTAR Dec-13 -> Dec-16 breakout).
Mine INSIDE the low-vol coil universe (not the volatile movers the last run found). Measure, leak-free, whether
this coil state actually lifts the next-day >=3% up-move rate, and whether a durable sub-rule fires on FIVESTAR
Dec-13. Only daily OHLCV + WTD + MTD. Honest: reports the real lift, elevated or not.
"""
import os, sqlite3, warnings
import numpy as np, pandas as pd
from sklearn.tree import DecisionTreeClassifier, _tree
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
MOVE_THR = 3.0
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2023-06-01' AND trade_date<='2025-06-30' ORDER BY symbol,trade_date", con)
con.close()
F = ["base_tight20", "dh20", "dh10", "dh5", "atr5v20", "rng_contract", "v_ratio", "v3_20", "v5_20",
     "run20", "run40", "days_since_hi20", "d_sma50", "wtd_pos", "wtd_ret", "updays10", "cloc3"]
rows = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); o = g.open.values.astype(float); v = g.volume.values.astype(float)
    td = g.trade_date.values; n = len(c); dt = pd.to_datetime(g.trade_date); iso = dt.dt.isocalendar(); wk = (iso.year.astype(int) * 100 + iso.week.astype(int)).values
    pc = np.roll(c, 1); pc[0] = np.nan; ret1 = (c / pc - 1) * 100
    hi20 = pd.Series(h).rolling(20).max().values; lo20 = pd.Series(l).rolling(20).min().values
    base_tight20 = (hi20 - lo20) / c * 100; dh20 = (c / hi20 - 1) * 100
    dh10 = (c / pd.Series(h).rolling(10).max().values - 1) * 100; dh5 = (c / pd.Series(h).rolling(5).max().values - 1) * 100
    trp = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc))) / c * 100
    atrp = pd.Series(trp).rolling(20).mean().values; atr5v20 = pd.Series(trp).rolling(5).mean().values / atrp
    rng_contract = pd.Series((h - l) / c * 100).rolling(3).mean().values / (pd.Series((h - l) / c * 100).rolling(15).mean().values + 1e-9)
    av20 = pd.Series(v).rolling(20).mean().values; v_ratio = v / av20; v3_20 = pd.Series(v).rolling(3).mean().values / av20; v5_20 = pd.Series(v).rolling(5).mean().values / av20
    run20 = pd.Series(c).pct_change(20).values * 100; run40 = pd.Series(c).pct_change(40).values * 100
    sma50 = pd.Series(c).rolling(50).mean().values; d_sma50 = (c / sma50 - 1) * 100
    dsh = np.full(n, np.nan)
    for i in range(n):
        if i >= 19: dsh[i] = 19 - int(np.argmax(h[i - 19:i + 1]))
    updays10 = pd.Series(ret1 > 0).rolling(10).sum().values
    cloc = np.where(h > l, (c - l) / (h - l), np.nan); cloc3 = pd.Series(cloc).rolling(3).mean().values
    wtd_pos = np.full(n, np.nan); wtd_ret = np.full(n, np.nan); ser = pd.Series(wk); grp = ser.ne(ser.shift()).cumsum()
    for _, idx in pd.Series(range(n)).groupby(grp.values):
        ii = idx.values; wh = np.maximum.accumulate(h[ii]); wl = np.minimum.accumulate(l[ii]); base = pc[ii[0]] if not np.isnan(pc[ii[0]]) else c[ii[0]]
        wtd_pos[ii] = np.where(wh > wl, (c[ii] - wl) / (wh - wl), np.nan); wtd_ret[ii] = (c[ii] / base - 1) * 100
    ocn = np.roll((c - o) / o * 100, -1); ocn[-1] = np.nan
    vals = dict(base_tight20=base_tight20, dh20=dh20, dh10=dh10, dh5=dh5, atr5v20=atr5v20, rng_contract=rng_contract,
                v_ratio=v_ratio, v3_20=v3_20, v5_20=v5_20, run20=run20, run40=run40, days_since_hi20=dsh, d_sma50=d_sma50,
                wtd_pos=wtd_pos, wtd_ret=wtd_ret, updays10=updays10, cloc3=cloc3)
    for i in range(n): rows.append((s, td[i], td[i][:7], ocn[i], *[vals[k][i] for k in F]))
P = pd.DataFrame(rows, columns=["symbol", "trade_date", "mo", "ocn"] + F); P["y"] = (P.ocn >= MOVE_THR).astype(int)
base = P.y.mean()
print(f"panel {len(P):,} · overall >= {MOVE_THR}% next-day up-move rate = {base*100:.1f}%")

# ---- FIVESTAR Dec-13 profile as the coil template ----
fs = P[(P.symbol == "FIVESTAR") & (P.trade_date == "2024-12-13")].iloc[0]
print(f"\nFIVESTAR Dec-13 coil: base_tight20 {fs.base_tight20:.1f}  dh20 {fs.dh20:+.1f}  atr5v20 {fs.atr5v20:.2f}  v_ratio {fs.v_ratio:.2f}  v3_20 {fs.v3_20:.2f}  run20 {fs.run20:+.1f}  d_sma50 {fs.d_sma50:+.1f}")

# ---- does the COIL state lift the breakout rate? (leak-free: OOS Mar-May'25) ----
tr = P[P.mo <= "2025-02"]; te = P[P.mo.isin(["2025-03", "2025-04", "2025-05"])]; bt = tr.y.mean(); be = te.y.mean()
print(f"\n----- COIL-at-highs states: next-day >= {MOVE_THR}% up-move rate (train vs OOS test) -----")
print(f"  {'coil definition':<52}{'n_te':>7}{'rate_te':>9}{'lift_te':>9}")
coils = [
    ("base_tight20<10 & dh20>-6 (tight base at highs)", (P.base_tight20 < 10) & (P.dh20 > -6)),
    (" + atr5v20<0.95 (vol contracting)", (P.base_tight20 < 10) & (P.dh20 > -6) & (P.atr5v20 < 0.95)),
    (" + v_ratio<0.9 (volume dry-up)", (P.base_tight20 < 10) & (P.dh20 > -6) & (P.atr5v20 < 0.95) & (P.v_ratio < 0.9)),
    (" + d_sma50>0 (uptrend)", (P.base_tight20 < 10) & (P.dh20 > -6) & (P.atr5v20 < 0.95) & (P.d_sma50 > 0)),
    ("base_tight20<8 & dh10>-4 & atr5v20<0.9 (tighter)", (P.base_tight20 < 8) & (P.dh10 > -4) & (P.atr5v20 < 0.9)),
]
for name, mask in coils:
    tem = mask & P.mo.isin(["2025-03", "2025-04", "2025-05"]); n_te = int(tem.sum())
    rate = P.y[tem].mean() if n_te else np.nan
    fs_in = bool(mask[(P.symbol == "FIVESTAR") & (P.trade_date == "2024-12-13")].iloc[0])
    print(f"  {name:<52}{n_te:>7}{rate*100:>8.1f}%{rate/be:>8.2f}x   FIVESTAR Dec-13 in-set: {fs_in}")
print(f"\n  (OOS base rate = {be*100:.1f}%)")

# ---- mine INSIDE the coil universe for a durable breakout sub-rule ----
coilmask = (P.base_tight20 < 12) & (P.dh20 > -8)
C = P[coilmask]; ctr = C[C.mo <= "2025-02"]; cte = C[C.mo.isin(["2025-03", "2025-04", "2025-05"])]
print(f"\n----- mining inside coil universe ({len(C):,} rows, {ctr.y.mean()*100:.1f}% base) for durable breakout sub-rules -----")
Xc = ctr[F].fillna(-999).values; yc = ctr.y.values
def leaf_rules(t, names):
    tt = t.tree_; out = []
    def rec(nid, path):
        if tt.children_left[nid] == _tree.TREE_LEAF:
            nt = int(tt.n_node_samples[nid]); val = tt.value[nid][0]; pr = float(val[1]) if len(val) > 1 else 0
            out.append(dict(rule=path[:], npos=int(round(pr * nt)), prec=pr)); return
        f = names[tt.feature[nid]]; th = tt.threshold[nid]
        rec(tt.children_left[nid], path + [(f, "<=", round(th, 3))]); rec(tt.children_right[nid], path + [(f, ">", round(th, 3))])
    rec(0, []); return out
def rmask(df, rule):
    m = np.ones(len(df), bool)
    for f, op, th in rule:
        col = df[f].values; m &= (col <= th) if op == "<=" else (col > th); m &= ~np.isnan(col)
    return m
cand = {}
for seed in range(20):
    dtc = DecisionTreeClassifier(max_depth=3, min_samples_leaf=40, class_weight="balanced", random_state=seed, max_features=0.7).fit(Xc, yc)
    for lf in leaf_rules(dtc, F):
        if len(lf["rule"]) < 1 or lf["npos"] < 15 or lf["prec"] < ctr.y.mean() * 1.3: continue
        cand[tuple(sorted((f, op, th) for f, op, th in lf["rule"]))] = lf["rule"]
fs_row = P[(P.symbol == "FIVESTAR") & (P.trade_date == "2024-12-13")]
res = []
for rule in cand.values():
    mte = rmask(cte, rule)
    if mte.sum() < 10: continue
    res.append(dict(rule=rule, lift_te=cte.y.values[mte].mean() / cte.y.mean(), n_te=int(mte.sum()), fs=bool(rmask(fs_row, rule).all())))
R = pd.DataFrame(res).sort_values("lift_te", ascending=False) if res else pd.DataFrame()
dur = R[R.lift_te >= 1.2] if len(R) else R
print(f"  durable coil sub-rules (OOS lift>=1.2x): {len(dur)}  ·  of which FIRE on FIVESTAR Dec-13: {int(dur.fs.sum()) if len(dur) else 0}")
if len(dur):
    for _, r in dur.sort_values("fs", ascending=False).head(10).iterrows():
        print(f"    {'[FIVESTAR YES]' if r.fs else '[         no]'} lift {r.lift_te:.2f}x n_te {r.n_te}  {' & '.join(f'{f}{op}{th}' for f,op,th in r.rule)}")
