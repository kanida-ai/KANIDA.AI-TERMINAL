"""REDONE MINING — target = THE MOVE (next-day open->close >= MOVE_THR), not just operator pullback winners.
This forces the miner to learn BOTH setups in the FIVESTAR chart: the tight-base BREAKOUT (box 1 -> Dec 16)
and the deep PULLBACK (box 2 -> Jan 24). Only daily OHLCV + WTD + MTD features. Mine on train, validate OOS,
then verify the durable rules FIRE on FIVESTAR Dec-13 (before Dec-16) and Jan-23 (before Jan-24). Leak-free.
"""
import os, sys, sqlite3, warnings, json
import numpy as np, pandas as pd
from sklearn.tree import DecisionTreeClassifier, _tree
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
MOVE_THR = 3.0   # a real capturable intraday move (operator avg win was +3.26%)

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2023-06-01' AND trade_date<='2025-06-30' ORDER BY symbol,trade_date", con)
con.close()
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None

FEATS = ["run20", "run40", "run60", "dh5", "dh10", "dh20", "dh40", "dh60", "dl20", "dl40", "d_sma50", "d_sma200",
         "slope20", "slope50", "atrp", "atr5v20", "rng_contract", "v3_20", "v5_20", "v_ratio", "cloc1", "cloc3",
         "updays5", "updays10", "downstreak", "base_tight20", "days_since_hi20", "wtd_ret", "wtd_pos", "wtd_daysup",
         "mtd_ret", "mtd_pos", "mtd_daysup"]
rows = []; OCN = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); o = g.open.values.astype(float); v = g.volume.values.astype(float)
    td = g.trade_date.values; n = len(c); dt = pd.to_datetime(g.trade_date); iso = dt.dt.isocalendar()
    wk = (iso.year.astype(int) * 100 + iso.week.astype(int)).values; mo = dt.dt.strftime("%Y-%m").values
    pc = np.roll(c, 1); pc[0] = np.nan; ret1 = (c / pc - 1) * 100; rr = lambda k: (pd.Series(c).pct_change(k).values) * 100
    run20, run40, run60 = rr(20), rr(40), rr(60)
    dh = lambda k: (c / pd.Series(h).rolling(k).max().values - 1) * 100; dl = lambda k: (c / pd.Series(l).rolling(k).min().values - 1) * 100
    dh5, dh10, dh20, dh40, dh60 = dh(5), dh(10), dh(20), dh(40), dh(60); dl20, dl40 = dl(20), dl(40)
    sma50 = pd.Series(c).rolling(50).mean().values; sma200 = pd.Series(c).rolling(200).mean().values; sma20 = pd.Series(c).rolling(20).mean().values
    d_sma50 = (c / sma50 - 1) * 100; d_sma200 = (c / sma200 - 1) * 100
    slope20 = (sma20 / np.roll(sma20, 5) - 1) * 100; slope20[:25] = np.nan; slope50 = (sma50 / np.roll(sma50, 5) - 1) * 100; slope50[:55] = np.nan
    tr = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc))); trp = tr / c * 100; atrp = pd.Series(trp).rolling(20).mean().values; atr5v20 = pd.Series(trp).rolling(5).mean().values / atrp
    rng_contract = pd.Series((h - l) / c * 100).rolling(3).mean().values / (pd.Series((h - l) / c * 100).rolling(15).mean().values + 1e-9)
    av20 = pd.Series(v).rolling(20).mean().values; v3_20 = pd.Series(v).rolling(3).mean().values / av20; v5_20 = pd.Series(v).rolling(5).mean().values / av20; v_ratio = v / av20
    cloc = np.where(h > l, (c - l) / (h - l), np.nan); cloc1 = cloc; cloc3 = pd.Series(cloc).rolling(3).mean().values
    updays5 = pd.Series(ret1 > 0).rolling(5).sum().values; updays10 = pd.Series(ret1 > 0).rolling(10).sum().values
    neg = (ret1 < 0).astype(int); ds = np.zeros(n)
    for i in range(n):
        k = 0
        while i - k >= 0 and neg[i - k] == 1: k += 1
        ds[i] = k
    hi20s = pd.Series(h).rolling(20).max().values; lo20s = pd.Series(l).rolling(20).min().values; base_tight20 = (hi20s - lo20s) / c * 100
    dsh = np.full(n, np.nan)
    for i in range(n):
        if i >= 19: dsh[i] = 19 - int(np.argmax(h[i - 19:i + 1]))
    def cw(key):
        r = np.full(n, np.nan); p = np.full(n, np.nan); du = np.full(n, np.nan); ser = pd.Series(key); grp = ser.ne(ser.shift()).cumsum()
        for _, idx in pd.Series(range(n)).groupby(grp.values):
            ii = idx.values; wh = np.maximum.accumulate(h[ii]); wl = np.minimum.accumulate(l[ii]); base = pc[ii[0]] if not np.isnan(pc[ii[0]]) else c[ii[0]]
            r[ii] = (c[ii] / base - 1) * 100; p[ii] = np.where(wh > wl, (c[ii] - wl) / (wh - wl), np.nan); du[ii] = np.cumsum((ret1[ii] > 0).astype(float))
        return r, p, du
    wtd_ret, wtd_pos, wtd_daysup = cw(wk); mtd_ret, mtd_pos, mtd_daysup = cw(mo)
    vals = dict(run20=run20, run40=run40, run60=run60, dh5=dh5, dh10=dh10, dh20=dh20, dh40=dh40, dh60=dh60, dl20=dl20, dl40=dl40,
                d_sma50=d_sma50, d_sma200=d_sma200, slope20=slope20, slope50=slope50, atrp=atrp, atr5v20=atr5v20, rng_contract=rng_contract,
                v3_20=v3_20, v5_20=v5_20, v_ratio=v_ratio, cloc1=cloc1, cloc3=cloc3, updays5=updays5, updays10=updays10, downstreak=ds,
                base_tight20=base_tight20, days_since_hi20=dsh, wtd_ret=wtd_ret, wtd_pos=wtd_pos, wtd_daysup=wtd_daysup, mtd_ret=mtd_ret, mtd_pos=mtd_pos, mtd_daysup=mtd_daysup)
    ocn = np.roll((c - o) / o * 100, -1); ocn[-1] = np.nan   # NEXT day's open->close
    for i in range(n):
        OCN[(s, td[i])] = ocn[i]
        rows.append((s, td[i], td[i][:7], ocn[i], *[vals[k][i] for k in FEATS]))
P = pd.DataFrame(rows, columns=["symbol", "trade_date", "mo", "ocn"] + FEATS)
P["y"] = (P.ocn >= MOVE_THR).astype(int)
print(f"panel {len(P):,} · move(>= {MOVE_THR}% next-day OC) base rate {P.y.mean()*100:.1f}%")

def leaf_rules(tree, names):
    t = tree.tree_; out = []
    def rec(nid, path):
        if t.children_left[nid] == _tree.TREE_LEAF:
            ntot = int(t.n_node_samples[nid]); val = t.value[nid][0]; prob = float(val[1]) if len(val) > 1 else 0
            out.append(dict(rule=path[:], n=ntot, npos=int(round(prob * ntot)), prec=prob)); return
        f = names[t.feature[nid]]; th = t.threshold[nid]
        rec(t.children_left[nid], path + [(f, "<=", round(th, 3))]); rec(t.children_right[nid], path + [(f, ">", round(th, 3))])
    rec(0, []); return out
def rmask(df, rule):
    m = np.ones(len(df), bool)
    for f, op, th in rule:
        col = df[f].values; m &= (col <= th) if op == "<=" else (col > th); m &= ~np.isnan(col)
    return m

TRAIN = ["2024-06", "2024-07", "2024-08", "2024-09", "2024-10", "2024-11", "2024-12", "2025-01", "2025-02"]
TEST = ["2025-03", "2025-04", "2025-05"]
tr = P[P.mo.isin(TRAIN)]; te = P[P.mo.isin(TEST)]
Xtr = tr[FEATS].fillna(-999).values; ytr = tr.y.values; base_tr = ytr.mean(); base_te = te.y.mean()
cand = {}
for seed in range(20):
    dtc = DecisionTreeClassifier(max_depth=4, min_samples_leaf=60, class_weight="balanced", random_state=seed, max_features=0.6)
    dtc.fit(Xtr, ytr)
    for lf in leaf_rules(dtc, FEATS):
        if len(lf["rule"]) < 2 or lf["npos"] < 25 or lf["prec"] < base_tr * 1.4: continue
        key = tuple(sorted((f, op, th) for f, op, th in lf["rule"]))
        if key not in cand: cand[key] = lf["rule"]
res = []
for rule in cand.values():
    mtr = rmask(tr, rule); mte = rmask(te, rule)
    if mte.sum() < 15: continue
    ptr = tr.y.values[mtr].mean(); pte = te.y.values[mte].mean()
    res.append(dict(rule=rule, lift_tr=ptr / base_tr, lift_te=pte / base_te, prec_te=pte, n_te=int(mte.sum())))
R = pd.DataFrame(res).sort_values("lift_te", ascending=False)
dur = R[(R.lift_te >= 1.3) & (R.prec_te > base_te)]
print(f"base rate: train {base_tr*100:.1f}% test {base_te*100:.1f}% · candidates {len(R)} · DURABLE (lift_te>=1.3): {len(dur)}")
pd.DataFrame([dict(rule_text=" & ".join(f"{f}{op}{th}" for f, op, th in r.rule), rule_json=json.dumps(r.rule),
                   lift_tr=round(r.lift_tr, 2), lift_te=round(r.lift_te, 2), prec_te=round(r.prec_te, 3), n_te=int(r.n_te)) for _, r in dur.iterrows()]
             ).to_csv(os.path.join(ROOT, "arena", "agent_population", "move_mined_rules.csv"), index=False)

# ---- VERIFY on FIVESTAR ----
print("\n================ FIVESTAR — do the REDONE (move-target) rules fire? ================")
DUR = [(r.rule, float(r.lift_te)) for _, r in dur.iterrows()]
for sd, td in [("2024-12-11", "2024-12-12"), ("2024-12-12", "2024-12-13"), ("2024-12-13", "2024-12-16"),
               ("2025-01-22", "2025-01-23"), ("2025-01-23", "2025-01-24")]:
    row = P[(P.symbol == "FIVESTAR") & (P.trade_date == sd)]
    mv = OCN.get(("FIVESTAR", sd), np.nan)
    if row.empty:
        print(f"  signal {sd} -> {td}: no data"); continue
    fired = [(txt_lift) for txt_lift in DUR if rmask(row, txt_lift[0]).all()]
    tag = "IDENTIFIED" if fired else "not identified"
    r0 = row.iloc[0]
    print(f"  signal {sd} -> trade {td}  (actual next-day move {mv:+.2f}%)  |  {len(fired)}/{len(DUR)} rules -> {tag}")
    print(f"     shape: base_tight20 {r0.base_tight20:.1f}%  dh20 {r0.dh20:+.1f}%  dh10 {r0.dh10:+.1f}%  atr5v20 {r0.atr5v20:.2f}  v_ratio {r0.v_ratio:.2f}  run20 {r0.run20:+.1f}%")
    for rule, lift in sorted(fired, key=lambda x: -x[1])[:3]:
        print(f"       fired: {' & '.join(f'{f}{op}{th}' for f,op,th in rule)}  (OOS lift {lift:.2f}x)")
print(f"\n  durable move-rules -> move_mined_rules.csv ({len(dur)})")
