"""OPERATOR SCREENER — walk-forward, rich trend features, day-relative ranking, 8 months of picks.
Goal: reliably rank the operator's winning trades into the daily top-15, and generalize forward.
Labels: operator picks (operator_picks_8mo, Oct24-May25) with a win proxy = next-day open->close > WIN_THR.
        (Dec+Jan also have EXACT returns from the ranked log; used to validate the proxy.)
Features (ALL leak-free as-of signal day S, then cross-sectionally rank-normalized PER DAY):
  prev-day (ret S, S-1, 2/3/5/20/60d), range/close-location, gaps, WTD (ret, position, days-up, vol),
  SMA distances+slopes (20/50/200), dist from 20/60d high + 20d low, ATR, volume ratios/turnover,
  relative strength vs universe (multi-window), + recency MEMORY of operator's own prior picks.
Model: HistGBM ranking winner-vs-rest, trained WALK-FORWARD (train past -> test next month). Reports recall@15
per test month + top-15 open->close 5x basket. Leak-free, production untouched.
"""
import os, sys, sqlite3, warnings, importlib.util
from collections import defaultdict
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WIN_THR = 0.3; MEM_LOOK, MEM_DECAY = 25, 0.90

# operator 8-month picks
spec = importlib.util.spec_from_file_location("op8", os.path.join(ROOT, "arena", "agent_population", "operator_picks_8mo.py"))
op8 = importlib.util.module_from_spec(spec); spec.loader.exec_module(op8)
PICKS = op8.PICKS  # trade_date -> [symbols]

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2023-06-01' AND trade_date<='2025-06-30' ORDER BY symbol,trade_date", con)
con.close()
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None

# ---------------- rich leak-free features per (symbol, date) ----------------
FEATS = ["ret1", "ret2", "ret3", "ret5", "ret20", "ret60", "rng1", "rng_avg5", "cloc1", "cloc_avg3",
         "gap1", "d_sma20", "d_sma50", "d_sma200", "slope20", "slope50", "d_hi20", "d_hi60", "d_lo20",
         "atrp", "v_ratio", "v3_20", "v5_20", "turn", "wtd_ret", "wtd_pos", "wtd_daysup", "wtd_vol",
         "updays10", "downstreak"]
rows = []; RET_OC = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float)
    o = g.open.values.astype(float); v = g.volume.values.astype(float); td = g.trade_date.values
    dt = pd.to_datetime(g.trade_date); iso = dt.dt.isocalendar()
    wk = (iso.year.astype(int) * 100 + iso.week.astype(int)).values
    n = len(c); pc = np.roll(c, 1); pc[0] = np.nan
    ret1 = (c / pc - 1) * 100
    def rk(k): a = np.roll(c, k); a[:k] = np.nan; return (c / a - 1) * 100
    ret2, ret3, ret5, ret20, ret60 = rk(2), rk(3), rk(5), rk(20), rk(60)
    rng1 = (h - l) / pc * 100; rng_avg5 = pd.Series(rng1).rolling(5).mean().values
    cloc1 = np.where(h > l, (c - l) / (h - l), np.nan); cloc_avg3 = pd.Series(cloc1).rolling(3).mean().values
    gap1 = (o - pc) / pc * 100
    sma20 = pd.Series(c).rolling(20).mean().values; sma50 = pd.Series(c).rolling(50).mean().values
    sma200 = pd.Series(c).rolling(200).mean().values
    d_sma20 = (c / sma20 - 1) * 100; d_sma50 = (c / sma50 - 1) * 100; d_sma200 = (c / sma200 - 1) * 100
    slope20 = (sma20 / np.roll(sma20, 5) - 1) * 100; slope20[:25] = np.nan
    slope50 = (sma50 / np.roll(sma50, 5) - 1) * 100; slope50[:55] = np.nan
    hi20 = pd.Series(h).rolling(20).max().values; hi60 = pd.Series(h).rolling(60).max().values
    lo20 = pd.Series(l).rolling(20).min().values
    d_hi20 = (c / hi20 - 1) * 100; d_hi60 = (c / hi60 - 1) * 100; d_lo20 = (c / lo20 - 1) * 100
    tr = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc))); atrp = pd.Series(tr / c * 100).rolling(20).mean().values
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values; av5 = pd.Series(v).rolling(5).mean().values
    v_ratio = v / av20; v3_20 = av3 / av20; v5_20 = av5 / av20
    turn = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    # WTD (within iso-week, cumulative)
    wk_s = pd.Series(wk); grp = wk_s.ne(wk_s.shift()).cumsum()
    wtd_ret = np.full(n, np.nan); wtd_pos = np.full(n, np.nan); wtd_daysup = np.full(n, np.nan); wtd_vol = np.full(n, np.nan)
    for _, idx in pd.Series(range(n)).groupby(grp.values):
        ii = idx.values; c0 = c[ii[0]]
        wh = np.maximum.accumulate(h[ii]); wl = np.minimum.accumulate(l[ii])
        wtd_ret[ii] = (c[ii] / (pc[ii[0]] if not np.isnan(pc[ii[0]]) else c0) - 1) * 100
        wtd_pos[ii] = np.where(wh > wl, (c[ii] - wl) / (wh - wl), np.nan)
        up = (ret1[ii] > 0).astype(float); wtd_daysup[ii] = np.cumsum(up)
        wtd_vol[ii] = np.cumsum(v[ii]) / (av20[ii] + 1e-9)
    updays10 = pd.Series(ret1 > 0).rolling(10).sum().values
    neg = (ret1 < 0).astype(int); ds = np.zeros(n)
    for i in range(n):
        k = 0
        while i - k >= 0 and neg[i - k] == 1: k += 1
        ds[i] = k
    F = dict(ret1=ret1, ret2=ret2, ret3=ret3, ret5=ret5, ret20=ret20, ret60=ret60, rng1=rng1, rng_avg5=rng_avg5,
             cloc1=cloc1, cloc_avg3=cloc_avg3, gap1=gap1, d_sma20=d_sma20, d_sma50=d_sma50, d_sma200=d_sma200,
             slope20=slope20, slope50=slope50, d_hi20=d_hi20, d_hi60=d_hi60, d_lo20=d_lo20, atrp=atrp,
             v_ratio=v_ratio, v3_20=v3_20, v5_20=v5_20, turn=turn, wtd_ret=wtd_ret, wtd_pos=wtd_pos,
             wtd_daysup=wtd_daysup, wtd_vol=wtd_vol, updays10=updays10, downstreak=ds)
    for i in range(n):
        RET_OC[(s, td[i])] = ((c[i] - o[i]) / o[i] * 100) if o[i] > 0 else np.nan
        rows.append((s, td[i], *[F[k][i] for k in FEATS]))
P = pd.DataFrame(rows, columns=["symbol", "trade_date"] + FEATS)
print(f"feature panel: {len(P):,} rows")

# ---------------- memory ----------------
sym_pickidx = {}
for tdte, syms in PICKS.items():
    s = prev(tdte)
    if s is None: continue
    for sym in syms: sym_pickidx.setdefault(sym, []).append(cidx[s])
for k in sym_pickidx: sym_pickidx[k] = sorted(sym_pickidx[k])
def memory(sym, i): return sum(MEM_DECAY ** (i - j) for j in sym_pickidx.get(sym, []) if 0 < i - j <= MEM_LOOK)

# ---------------- build signal-day panel with labels (win proxy) ----------------
FCidx = P.set_index(["trade_date", "symbol"]).sort_index()
sig = []
for tdte, syms in PICKS.items():
    s = prev(tdte)
    if s is None or s not in cidx: continue
    day = P[P.trade_date == s].copy()
    if day.empty: continue
    day["mem"] = [memory(sym, cidx[s]) for sym in day.symbol]
    # label: operator picked next day AND won (open->close proxy on trade day)
    won = set()
    for sym in syms:
        r = RET_OC.get((sym, tdte), np.nan)
        if not np.isnan(r) and r > WIN_THR: won.add(sym)
    day["y"] = day.symbol.isin(won).astype(int)
    day["sd"] = s; day["td"] = tdte; day["mo"] = tdte[:7]
    sig.append(day)
S = pd.concat(sig, ignore_index=True)
XC = FEATS + ["mem"]
# day-relative rank-normalize every feature
for f in XC: S[f + "_r"] = S.groupby("sd")[f].rank(pct=True)
RC = [f + "_r" for f in XC]
print(f"signal panel: {len(S):,} rows · winners {int(S.y.sum())} · months {sorted(S.mo.unique())}")

def recall_month(train, test):
    clf = HistGradientBoostingClassifier(max_iter=400, max_depth=3, min_samples_leaf=25,
                                         learning_rate=0.08, l2_regularization=1.0, random_state=0)
    clf.fit(train[RC].values, train.y.values)
    test = test.copy(); test["p"] = clf.predict_proba(test[RC].values)[:, 1]
    hit = tot = 0; bask = []
    for sd, g in test.groupby("sd"):
        g = g.sort_values("p", ascending=False); order = list(g.symbol)
        top = set(order[:15]); w = g[g.y == 1].symbol
        tot += len(w); hit += sum(s in top for s in w)
        bask.append(np.nanmean([RET_OC.get((s, g.td.iloc[0]), np.nan) for s in order[:15]]))
    return (hit / tot * 100 if tot else 0), tot, float(np.nanmean(bask)) * 5

months = sorted(S.mo.unique())
print("\n============ WALK-FORWARD (train on all prior months -> test next) ============")
print(f"{'test month':<12}{'train N':>9}{'winners':>9}{'recall@15':>11}{'top15 5x':>10}")
allhit = alltot = 0
for i in range(2, len(months)):
    tr = S[S.mo.isin(months[:i])]; te = S[S.mo == months[i]]
    if len(te) == 0 or te.y.sum() == 0: continue
    r, tot, bask = recall_month(tr, te)
    allhit += r / 100 * tot; alltot += tot
    print(f"  {months[i]:<10}{int(tr.y.sum()):>9}{tot:>9}{r:>10.0f}%{bask:>+9.2f}%")
print(f"\n  overall forward recall@15: {allhit/alltot*100:.0f}%  (over {alltot} winners)")
