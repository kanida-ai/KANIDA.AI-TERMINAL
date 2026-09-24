"""OPERATOR SCREENER — LEARNING-TO-RANK stack. Objective: rank the operator's winning trades above everyone
else THAT DAY (LambdaRank), on the full feature stack: prev-day + WTD + MTD trends, archetype strength scores
(continuation/breakout/pullback/dryup), Falcon winner-pool pattern hits, and recency memory. Walk-forward
(train prior months -> test next). All leak-free (PIT), day-relative rank-normalized. Production untouched.
"""
import os, sys, sqlite3, warnings, importlib.util
from collections import Counter, defaultdict
import numpy as np, pandas as pd
from lightgbm import LGBMRanker
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WIN_THR = 0.3; MEM_LOOK, MEM_DECAY = 25, 0.90; WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
spec = importlib.util.spec_from_file_location("op8", os.path.join(ROOT, "arena", "agent_population", "operator_picks_8mo.py"))
op8 = importlib.util.module_from_spec(spec); spec.loader.exec_module(op8); PICKS = op8.PICKS

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2023-06-01' AND trade_date<='2025-06-30' ORDER BY symbol,trade_date", con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-06-30'", con)
con.close()
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None

# ---------------- rich features incl WTD + MTD + archetype ----------------
BASE = ["ret1", "ret2", "ret3", "ret5", "ret20", "ret60", "rng1", "cloc1", "gap1", "d_sma20", "d_sma50",
        "d_sma200", "slope20", "slope50", "d_hi20", "d_hi60", "d_lo20", "atrp", "v_ratio", "v3_20", "turn",
        "wtd_ret", "wtd_pos", "wtd_daysup", "mtd_ret", "mtd_pos", "mtd_daysup", "updays10", "downstreak",
        "cont_str", "brk_str", "pb_str", "dry_str"]
rows = []; RET_OC = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float)
    o = g.open.values.astype(float); v = g.volume.values.astype(float); td = g.trade_date.values; n = len(c)
    dt = pd.to_datetime(g.trade_date); iso = dt.dt.isocalendar()
    wk = (iso.year.astype(int) * 100 + iso.week.astype(int)).values; mo = dt.dt.strftime("%Y-%m").values
    pc = np.roll(c, 1); pc[0] = np.nan; ret1 = (c / pc - 1) * 100
    kk = lambda k: (pd.Series(c).pct_change(k).values) * 100
    ret2, ret3, ret5, ret20, ret60 = kk(2), kk(3), kk(5), kk(20), kk(60)
    rng1 = (h - l) / pc * 100; cloc1 = np.where(h > l, (c - l) / (h - l), np.nan); gap1 = (o - pc) / pc * 100
    sma20 = pd.Series(c).rolling(20).mean().values; sma50 = pd.Series(c).rolling(50).mean().values; sma200 = pd.Series(c).rolling(200).mean().values
    d_sma20 = (c / sma20 - 1) * 100; d_sma50 = (c / sma50 - 1) * 100; d_sma200 = (c / sma200 - 1) * 100
    slope20 = (sma20 / np.roll(sma20, 5) - 1) * 100; slope20[:25] = np.nan
    slope50 = (sma50 / np.roll(sma50, 5) - 1) * 100; slope50[:55] = np.nan
    hi20 = pd.Series(h).rolling(20).max().values; hi60 = pd.Series(h).rolling(60).max().values; lo20 = pd.Series(l).rolling(20).min().values
    d_hi20 = (c / hi20 - 1) * 100; d_hi60 = (c / hi60 - 1) * 100; d_lo20 = (c / lo20 - 1) * 100
    tr = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc))); atrp = pd.Series(tr / c * 100).rolling(20).mean().values
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    v_ratio = v / av20; v3_20 = av3 / av20
    turn = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    def cumwin(key):
        r = np.full(n, np.nan); p = np.full(n, np.nan); du = np.full(n, np.nan)
        ser = pd.Series(key); grp = ser.ne(ser.shift()).cumsum()
        for _, idx in pd.Series(range(n)).groupby(grp.values):
            ii = idx.values; wh = np.maximum.accumulate(h[ii]); wl = np.minimum.accumulate(l[ii])
            base = pc[ii[0]] if not np.isnan(pc[ii[0]]) else c[ii[0]]
            r[ii] = (c[ii] / base - 1) * 100; p[ii] = np.where(wh > wl, (c[ii] - wl) / (wh - wl), np.nan)
            du[ii] = np.cumsum((ret1[ii] > 0).astype(float))
        return r, p, du
    wtd_ret, wtd_pos, wtd_daysup = cumwin(wk); mtd_ret, mtd_pos, mtd_daysup = cumwin(mo)
    updays10 = pd.Series(ret1 > 0).rolling(10).sum().values
    neg = (ret1 < 0).astype(int); ds = np.zeros(n)
    for i in range(n):
        k = 0
        while i - k >= 0 and neg[i - k] == 1: k += 1
        ds[i] = k
    relu = lambda x: np.clip(x, 0, None)
    cont_str = np.where((d_sma200 > 0) & (slope50 > 0), np.clip(ret60, 0, 60), 0.0)
    brk_str = relu(d_hi20 + 3) * relu(v_ratio - 1)
    pb_str = np.where(d_sma200 > 0, relu(-d_hi20 - 8), 0.0) * relu(-ret20)
    dry_str = relu(1 - v3_20) * relu(6 - atrp)
    loc = dict(ret1=ret1, ret2=ret2, ret3=ret3, ret5=ret5, ret20=ret20, ret60=ret60, rng1=rng1, cloc1=cloc1,
               gap1=gap1, d_sma20=d_sma20, d_sma50=d_sma50, d_sma200=d_sma200, slope20=slope20, slope50=slope50,
               d_hi20=d_hi20, d_hi60=d_hi60, d_lo20=d_lo20, atrp=atrp, v_ratio=v_ratio, v3_20=v3_20, turn=turn,
               wtd_ret=wtd_ret, wtd_pos=wtd_pos, wtd_daysup=wtd_daysup, mtd_ret=mtd_ret, mtd_pos=mtd_pos,
               mtd_daysup=mtd_daysup, updays10=updays10, downstreak=ds, cont_str=cont_str, brk_str=brk_str,
               pb_str=pb_str, dry_str=dry_str)
    for i in range(n):
        RET_OC[(s, td[i])] = ((c[i] - o[i]) / o[i] * 100) if o[i] > 0 else np.nan
        rows.append((s, td[i], *[loc[k][i] for k in BASE]))
P = pd.DataFrame(rows, columns=["symbol", "trade_date"] + BASE)
print(f"feature panel {len(P):,} rows · {len(BASE)} base features (incl WTD/MTD/archetype)")

# ---------------- Falcon winner-pool pattern hits (pool from Oct+Nov'24 winners; leak-free for Dec+) ----------------
dt2 = pd.to_datetime(feat.trade_date); iso2 = dt2.dt.isocalendar()
feat = feat.copy(); feat["wk"] = (iso2.year.astype(int) * 100 + iso2.week.astype(int)).values
ohw = oh[["symbol", "trade_date", "high", "low", "close"]].copy()
dtw = pd.to_datetime(ohw.trade_date); ohw["wk"] = (dtw.dt.isocalendar().year.astype(int) * 100 + dtw.dt.isocalendar().week.astype(int)).values
recw = []
for s, g in ohw.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values; c = g.close.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    recw.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan), weekly_range_pct=np.where(c > 0, (g.high.values - g.low.values) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan), weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns] + ["wk"]).merge(pd.concat(recw, ignore_index=True), on=["symbol", "trade_date"], how="left")
pats = FR.load_patterns(sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)); PM = {p["pattern_id"]: p for p in pats}
# frozen per-stock ledger (avg fwd return when a pattern fired ON that specific name)
_led = pd.read_csv(os.path.join(ROOT, "arena", "agent_population", "per_stock_pattern_ledger.csv"))
SP = {(r.symbol, int(r.pattern_id)): (float(r.avg_fwd_ret), int(r.n)) for r in _led.itertuples()}
def Xmat(fd):
    syms = fd.symbol.values; X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    return syms, X
pf = Counter()
for tdte, syms in PICKS.items():
    if tdte > "2024-11-30": continue
    sd = prev(tdte)
    for sym in syms:
        r = RET_OC.get((sym, tdte), np.nan)
        if np.isnan(r) or r <= WIN_THR: continue
        fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == sym)]
        if fr.empty: continue
        _, X = Xmat(fr); yr = int(sd[:4])
        for pp in pats:
            if int(pp["mined_year"]) < yr and FR.rule_mask(pp["rule"], X)[0]: pf[pp["pattern_id"]] += 1
POOL = [pid for pid, n in pf.items() if n >= 2]
print(f"Falcon winner-pool (from Oct-Nov'24 winners): {len(POOL)} patterns")
sig_days = sorted({prev(t) for t in PICKS if prev(t)})
poolrows = []
for sd in sig_days:
    fd = FCpit[FCpit.trade_date == sd]
    if fd.empty: continue
    syms, X = Xmat(fd); yr = int(sd[:4]); hits = np.zeros(len(syms)); lift = np.zeros(len(syms))
    ps = np.zeros(len(syms)); psden = np.zeros(len(syms))
    for pid in POOL:
        p = PM[pid]
        if int(p["mined_year"]) >= yr: continue
        mb = FR.rule_mask(p["rule"], X); m = mb.astype(float); hits += m; lift += m * (p["oos_lift"] or 0)
        for i in np.where(mb)[0]:
            cell = SP.get((syms[i], pid))
            if cell and cell[1] >= 3:
                w = np.log1p(cell[1]); ps[i] += cell[0] * w; psden[i] += w
    ps_score = np.where(psden > 0, ps / np.maximum(psden, 1e-9), np.nan)
    poolrows.append(pd.DataFrame(dict(symbol=syms, trade_date=sd, pool_hits=hits, pool_lift=lift, ps_score=ps_score)))
POOLF = pd.concat(poolrows, ignore_index=True)
P = P.merge(POOLF, on=["symbol", "trade_date"], how="left")
print("pool-hit feature merged")

# ---------------- memory + signal panel + labels ----------------
sym_pickidx = {}
for tdte, syms in PICKS.items():
    s = prev(tdte)
    if s: [sym_pickidx.setdefault(sym, []).append(cidx[s]) for sym in syms]
def memory(sym, i): return sum(MEM_DECAY ** (i - j) for j in sorted(sym_pickidx.get(sym, [])) if 0 < i - j <= MEM_LOOK)
FE = BASE + ["pool_hits", "pool_lift", "ps_score", "mem"]
sig = []
for tdte, syms in PICKS.items():
    s = prev(tdte)
    if s is None: continue
    day = P[P.trade_date == s].copy()
    if day.empty: continue
    day["mem"] = [memory(sym, cidx[s]) for sym in day.symbol]
    won = {sym for sym in syms if not np.isnan(RET_OC.get((sym, tdte), np.nan)) and RET_OC.get((sym, tdte), -9) > WIN_THR}
    allp = set(syms)
    day["y"] = np.where(day.symbol.isin(won), 2, np.where(day.symbol.isin(allp), 1, 0))  # graded: winner=2, other pick=1
    day["sd"] = s; day["td"] = tdte; day["mo"] = tdte[:7]
    sig.append(day)
S = pd.concat(sig, ignore_index=True)
for f in FE: S[f + "_r"] = S.groupby("sd")[f].rank(pct=True)
RC = [f + "_r" for f in FE]
print(f"signal panel {len(S):,} · winners {int((S.y==2).sum())} · features {len(RC)}")

def evalmonth(train, test):
    train = train.sort_values("sd"); grp = train.groupby("sd").size().values
    rk = LGBMRanker(objective="lambdarank", n_estimators=300, num_leaves=31, learning_rate=0.05,
                    min_child_samples=30, subsample=0.8, colsample_bytree=0.8, random_state=0, verbose=-1)
    rk.fit(train[RC].values, train.y.values, group=grp)
    test = test.copy(); test["p"] = rk.predict(test[RC].values)
    hit = {5: 0, 15: 0, 30: 0}; tot = 0; bask = []
    for sd, g in test.groupby("sd"):
        g = g.sort_values("p", ascending=False); order = list(g.symbol); w = g[g.y == 2].symbol; tot += len(w)
        for k in hit: hit[k] += sum(s in set(order[:k]) for s in w)
        bask.append(np.nanmean([RET_OC.get((s, g.td.iloc[0]), np.nan) for s in order[:15]]))
    R = lambda k: (hit[k] / tot * 100 if tot else 0)
    return R(5), R(15), R(30), tot, float(np.nanmean(bask)) * 5, rk

months = sorted(S.mo.unique())
print("\n============ LEARNING-TO-RANK (LambdaRank) WALK-FORWARD ============")
print(f"{'test month':<12}{'winners':>9}{'r@5':>6}{'r@15':>7}{'r@30':>7}{'top15 5x':>10}")
agg = {5: 0, 15: 0, 30: 0}; alltot = 0; last = None
for i in range(2, len(months)):
    tr = S[S.mo.isin(months[:i])]; te = S[S.mo == months[i]]
    if len(te) == 0 or (te.y == 2).sum() == 0: continue
    r5, r15, r30, tot, bask, last = evalmonth(tr, te)
    for k in (5, 15, 30): agg[k] += {5: r5, 15: r15, 30: r30}[k] / 100 * tot
    alltot += tot
    print(f"  {months[i]:<10}{tot:>9}{r5:>5.0f}%{r15:>6.0f}%{r30:>6.0f}%{bask:>+9.2f}%")
print(f"\n  OVERALL forward  @5 {agg[5]/alltot*100:.0f}%  @15 {agg[15]/alltot*100:.0f}%  @30 {agg[30]/alltot*100:.0f}%  (over {alltot} winners)")
imp = pd.Series(last.feature_importances_, index=FE).sort_values(ascending=False)
print("\n  top drivers:", " · ".join(f"{f}={int(v)}" for f, v in imp.head(12).items()))
