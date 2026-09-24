"""SHAPE-PROTOTYPE SCREENER — learn the operator's winning CHART SHAPE directly (no hand features, no DL).
Each stock/day = its last 40-day z-normalized close + log-volume sequence (the chart, as numbers). Per test
month, cluster prior-months' WINNER sequences into shape prototypes and NEUTRAL sequences into prototypes;
score each test stock by (distance to nearest neutral shape) - (distance to nearest winner shape): higher =
looks more like your winning charts than a random chart. Walk-forward, leak-free. Recall of operator winners.
"""
import os, sys, sqlite3, warnings, importlib.util
from collections import defaultdict
import numpy as np, pandas as pd
from sklearn.cluster import KMeans
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WIN_THR = 0.3; L = 40; KW = 40; KN = 60
spec = importlib.util.spec_from_file_location("op8", os.path.join(ROOT, "arena", "agent_population", "operator_picks_8mo.py"))
op8 = importlib.util.module_from_spec(spec); spec.loader.exec_module(op8); PICKS = op8.PICKS

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2024-04-01' AND trade_date<='2025-06-30' ORDER BY symbol,trade_date", con)
con.close()
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
sig_days = sorted({prev(t) for t in PICKS if prev(t)})
sig_set = set(sig_days)
SEQ = {}; RET_OC = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    c = g.close.values.astype(float); o = g.open.values.astype(float); v = g.volume.values.astype(float); td = g.trade_date.values
    lv = np.log1p(v)
    for i in range(len(c)):
        RET_OC[(s, td[i])] = ((c[i] - o[i]) / o[i] * 100) if o[i] > 0 else np.nan
        if td[i] in sig_set and i >= L - 1:
            cc = c[i - L + 1:i + 1]; vv = lv[i - L + 1:i + 1]
            if cc.std() > 0 and vv.std() > 0:
                SEQ[(s, td[i])] = np.concatenate([(cc - cc.mean()) / cc.std(), (vv - vv.mean()) / vv.std()])
print(f"sequences built: {len(SEQ):,} (len {2*L})")

# label winners per signal day
wins_by_sd = defaultdict(set); mo_of = {}
for tdte, syms in PICKS.items():
    sd = prev(tdte)
    if not sd: continue
    mo_of[sd] = tdte[:7]
    for sym in syms:
        r = RET_OC.get((sym, tdte), np.nan)
        if not np.isnan(r) and r > WIN_THR: wins_by_sd[sd].add(sym)

# assemble per-day matrices
day_syms = defaultdict(list)
for (s, d) in SEQ: day_syms[d].append(s)
months = sorted({mo_of[d] for d in sig_days if d in mo_of})

def month_of(d): return mo_of.get(d)
def winner_seqs(upto_months):
    W = []; N = []
    for d in sig_days:
        if month_of(d) not in upto_months: continue
        ws = wins_by_sd.get(d, set())
        for s in day_syms[d]:
            v = SEQ.get((s, d))
            if v is None: continue
            (W if s in ws else N).append(v)
    return np.array(W), np.array(N)

print("\n============ SHAPE-PROTOTYPE WALK-FORWARD ============")
print(f"{'test month':<12}{'winners':>9}{'r@15':>7}{'r@30':>7}{'top15 5x':>10}")
agg = {15: 0, 30: 0}; alltot = 0
for i in range(2, len(months)):
    trm = months[:i]; tem = months[i]
    W, N = winner_seqs(trm)
    if len(W) < KW or len(N) < KN: continue
    kmw = KMeans(n_clusters=KW, n_init=3, random_state=0).fit(W)
    idx = np.random.RandomState(0).choice(len(N), min(6000, len(N)), replace=False)
    kmn = KMeans(n_clusters=KN, n_init=3, random_state=0).fit(N[idx])
    Wc = kmw.cluster_centers_; Nc = kmn.cluster_centers_
    hit = {15: 0, 30: 0}; tot = 0; bask = []
    test_days = [d for d in sig_days if month_of(d) == tem]
    for d in test_days:
        syms = [s for s in day_syms[d] if (s, d) in SEQ]
        if not syms: continue
        M = np.array([SEQ[(s, d)] for s in syms])
        dW = np.sqrt(((M[:, None, :] - Wc[None, :, :]) ** 2).sum(2)).min(1)
        dN = np.sqrt(((M[:, None, :] - Nc[None, :, :]) ** 2).sum(2)).min(1)
        score = dN - dW
        order = [syms[k] for k in np.argsort(-score)]
        ws = wins_by_sd.get(d, set()); tot += len(ws)
        for K in hit: hit[K] += sum(s in set(order[:K]) for s in ws)
        bask.append(np.nanmean([RET_OC.get((s, cal[cidx[d] + 1]), np.nan) for s in order[:15]]))
    R = lambda K: hit[K] / tot * 100 if tot else 0
    agg[15] += R(15) / 100 * tot; agg[30] += R(30) / 100 * tot; alltot += tot
    print(f"  {tem:<10}{tot:>9}{R(15):>6.0f}%{R(30):>6.0f}%{np.nanmean(bask)*5:>+9.2f}%")
print(f"\n  OVERALL forward  @15 {agg[15]/alltot*100:.0f}%  @30 {agg[30]/alltot*100:.0f}%  (over {alltot})")
