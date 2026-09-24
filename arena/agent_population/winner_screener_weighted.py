"""WEIGHTED WINNER SCREENER — optimize the weight on every ranking dimension to MATCH the operator's
daily manual picks into the top-15. Dimensions (all leak-free, rank-normalized per day):
  mem (recurring-name memory), 4 archetype pattern-lift rankings, overall winner-pool lift, feature-band match,
  tier gate, pullback tilt, momentum. Search non-negative weights to MAXIMIZE recall@15 of the winners on Dec,
  then apply the SAME weights to Jan (OOS). Also reports recall with memory weight forced to 0 (features-only),
  so we can see how much 'match' is discovery vs remembering. Leak-free, production untouched.
"""
import os, sys, sqlite3, warnings
from collections import Counter, defaultdict
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
POOL_MINSUP = 3; MEM_LOOK, MEM_DECAY = 20, 0.90
ARCHES = ["BREAKOUT", "CONTINUATION", "PULLBACK", "DRYUP"]
DIMS = ["mem", "a_BREAKOUT", "a_CONTINUATION", "a_PULLBACK", "a_DRYUP", "pool_lift", "band", "tier_ok", "pullback_tilt", "momo"]


def _ok(v): return v is not None and not (isinstance(v, float) and v != v)
def archetypes_of(f):
    dh = f.get("dist_high_20"); r20 = f.get("roc_20"); r60 = f.get("roc_60"); d200 = f.get("dist_sma_200")
    sl = f.get("slope_sma_50"); bo = f.get("weekly_breakout_20w"); wr = f.get("weekly_range_pct")
    tr = f.get("tr3"); atr = f.get("atr_20_pct"); out = set()
    if _ok(dh) and dh >= -3 and ((_ok(bo) and bo >= 1) or (_ok(r20) and r20 >= 8)): out.add("BREAKOUT")
    if _ok(d200) and d200 > 0 and _ok(sl) and sl > 0 and _ok(r60) and r60 > 5 and _ok(dh) and dh >= -10: out.add("CONTINUATION")
    if _ok(d200) and d200 > 0 and _ok(dh) and dh <= -8 and _ok(r20) and r20 <= 2: out.add("PULLBACK")
    if (_ok(wr) and wr < 4) or (_ok(tr) and tr < 0.85) or (_ok(atr) and atr < 3): out.add("DRYUP")
    return out

picks = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con); PM = {p["pattern_id"]: p for p in pats}
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-10-15' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; TF3 = {}; RET_OC = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float)
    l = g.low.values.astype(float); v = g.volume.values.astype(float); op = g.open.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    for i, d in enumerate(g.trade_date.values):
        TF3[(s, d)] = tr3[i]; RET_OC[(s, d)] = ((c[i] - op[i]) / op[i] * 100) if op[i] > 0 else np.nan
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(
    pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None
AF = ["dist_high_20", "roc_20", "roc_60", "dist_sma_200", "slope_sma_50", "weekly_breakout_20w",
      "weekly_range_pct", "atr_20_pct", "weekly_close_vs_sma20"]
def Xmat(fd):
    syms = fd.symbol.values; X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    return syms, X

# memory
sym_pickidx = {}
for _, p in picks.iterrows():
    s = prev(p.trade_date)
    if s: sym_pickidx.setdefault(p.symbol, []).append(cidx[s])
for k in sym_pickidx: sym_pickidx[k] = sorted(sym_pickidx[k])
def memory(sym, i): return sum(MEM_DECAY ** (i - j) for j in sym_pickidx.get(sym, []) if 0 < i - j <= MEM_LOOK)

# archetype pools from Dec winners
dec = picks[(picks.trade_date >= "2024-12-01") & (picks.trade_date <= "2024-12-31")]
winners = dec[dec.stock_ret_pct > 0]
arch_pat = {a: Counter() for a in ARCHES}
for _, p in winners.iterrows():
    sd = prev(p.trade_date)
    if not sd: continue
    fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == p.symbol)]
    if fr.empty: continue
    fdict = {f: pd.to_numeric(fr[f].iloc[0], errors="coerce") for f in AF if f in fr.columns}; fdict["tr3"] = TF3.get((p.symbol, sd), np.nan)
    ars = archetypes_of(fdict) or {"DRYUP"}
    _, X = Xmat(fr); yr = int(sd[:4])
    fired = [pp["pattern_id"] for pp in pats if int(pp["mined_year"]) < yr and FR.rule_mask(pp["rule"], X)[0]]
    for a in ars:
        for pid in fired: arch_pat[a][pid] += 1
pools = {a: [pid for pid, n in arch_pat[a].items() if n >= POOL_MINSUP] for a in ARCHES}
allpool = sorted(set().union(*[set(v) for v in pools.values()]))
bandvals = defaultdict(list)
for _, p in winners.iterrows():
    sd = prev(p.trade_date)
    fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == p.symbol)] if sd else None
    if fr is None or fr.empty: continue
    for f in AF:
        val = pd.to_numeric(fr[f].iloc[0], errors="coerce") if f in fr.columns else np.nan
        if _ok(val): bandvals[f].append(val)
bands = {f: (np.percentile(v, 10), np.percentile(v, 90)) for f, v in bandvals.items() if len(v) >= 10}

def day_dims(sd):
    fd = FCpit[FCpit.trade_date == sd]
    if fd.empty: return None
    syms, X = Xmat(fd); yr = int(sd[:4]); n = len(syms); i = cidx[sd]
    memb = {a: np.zeros(n, bool) for a in ARCHES}
    for k, sym in enumerate(syms):
        fdict = {f: pd.to_numeric(fd[f].iloc[k], errors="coerce") for f in AF if f in fd.columns}; fdict["tr3"] = TF3.get((sym, sd), np.nan)
        for a in archetypes_of(fdict): memb[a][k] = True
    # pattern lift per archetype + overall
    liftA = {a: np.zeros(n) for a in ARCHES}; poolall = np.zeros(n)
    firecache = {}
    for pid in allpool:
        p = PM[pid]
        if int(p["mined_year"]) >= yr: continue
        m = FR.rule_mask(p["rule"], X).astype(float); firecache[pid] = m
        poolall += m * (p["oos_lift"] or 0)
    for a in ARCHES:
        for pid in pools[a]:
            if pid in firecache: liftA[a] += firecache[pid] * (PM[pid]["oos_lift"] or 0)
        liftA[a][~memb[a]] = 0
    band = np.zeros(n)
    for f, (lo, hi) in bands.items():
        col = pd.to_numeric(fd[f], errors="coerce").values if f in fd.columns else np.full(n, np.nan)
        band += ((col >= lo) & (col <= hi)).astype(float)
    sret = np.array([pd.to_numeric(fd["sret"].iloc[k], errors="coerce") if "sret" in fd.columns else np.nan for k in range(n)]) if False else None
    wcvs = pd.to_numeric(fd["weekly_close_vs_sma20"], errors="coerce").values if "weekly_close_vs_sma20" in fd.columns else np.full(n, np.nan)
    momo = pd.to_numeric(fd["roc_20"], errors="coerce").values if "roc_20" in fd.columns else np.full(n, np.nan)
    tier_ok = np.zeros(n)  # simple: within any archetype
    for a in ARCHES: tier_ok = np.maximum(tier_ok, memb[a].astype(float))
    raw = dict(mem=np.array([memory(s, i) for s in syms]),
               a_BREAKOUT=liftA["BREAKOUT"], a_CONTINUATION=liftA["CONTINUATION"],
               a_PULLBACK=liftA["PULLBACK"], a_DRYUP=liftA["DRYUP"], pool_lift=poolall, band=band,
               tier_ok=tier_ok, pullback_tilt=-np.nan_to_num(wcvs, nan=0.0), momo=np.nan_to_num(momo, nan=0.0))
    M = np.column_stack([pd.Series(raw[d]).rank(pct=True).values for d in DIMS])  # rank-normalize each dim
    return dict(syms=syms, M=M, td=nxt(sd))

# build day tensors + winner masks
def month_days(lo, hi):
    sub = picks[(picks.trade_date >= lo) & (picks.trade_date <= hi)]
    wl = sub[sub.stock_ret_pct > 0]; wbd = defaultdict(set)
    for _, p in wl.iterrows():
        sd = prev(p.trade_date)
        if sd: wbd[sd].add(p.symbol)
    days = []
    for sd in sorted(wbd):
        dd = day_dims(sd)
        if dd is None: continue
        widx = np.array([k for k, s in enumerate(dd["syms"]) if s in wbd[sd]])
        days.append((dd, widx))
    return days
print("building day tensors...")
DEC = month_days("2024-12-01", "2024-12-31"); JAN = month_days("2025-01-01", "2025-01-31")

def recall15(days, w):
    hit = tot = 0; bask = []
    for dd, widx in days:
        sc = dd["M"] @ w
        order = np.argsort(-sc); top = set(order[:15].tolist())
        tot += len(widx); hit += sum(int(k in top) for k in widx)
        bask.append(np.nanmean([RET_OC.get((dd["syms"][k], dd["td"]), np.nan) for k in order[:15]]))
    return (hit / tot * 100 if tot else 0), float(np.nanmean(bask)) * 5

# optimize weights on DEC recall@15 (random search + coordinate ascent)
rng = np.random.RandomState(0); nD = len(DIMS)
best_w = np.ones(nD) / nD; best_r, _ = recall15(DEC, best_w)
for _ in range(4000):
    w = rng.dirichlet(np.ones(nD))
    r, _ = recall15(DEC, w)
    if r > best_r: best_r, best_w = r, w
for _ in range(6):
    for j in range(nD):
        for delta in (-0.15, -0.06, 0.06, 0.15, 0.3):
            w = best_w.copy(); w[j] = max(0, w[j] + delta); s = w.sum()
            if s <= 0: continue
            w /= s; r, _ = recall15(DEC, w)
            if r > best_r: best_r, best_w = r, w

dec_r, dec_b = recall15(DEC, best_w); jan_r, jan_b = recall15(JAN, best_w)
w0 = best_w.copy(); w0[DIMS.index("mem")] = 0
if w0.sum() > 0: w0 /= w0.sum()
dec_r0, _ = recall15(DEC, w0); jan_r0, _ = recall15(JAN, w0)

print("\n================ WEIGHT-OPTIMIZED SCREENER ================")
print("optimized weights (Dec recall@15 objective):")
for d, wv in sorted(zip(DIMS, best_w), key=lambda kv: -kv[1]): print(f"    {d:<16} {wv:.3f}")
print(f"\n  DEC (in-sample)  recall@15 {dec_r:.0f}%   top15 5x {dec_b:+.2f}%/day")
print(f"  JAN (out-of-samp) recall@15 {jan_r:.0f}%   top15 5x {jan_b:+.2f}%/day")
print(f"\n  --- memory weight FORCED to 0 (pure feature/pattern discovery) ---")
print(f"  DEC recall@15 {dec_r0:.0f}%   ·   JAN recall@15 {jan_r0:.0f}%")
print(f"\n  => 'match' from memory (remembering your names): DEC {dec_r-dec_r0:+.0f}pp  JAN {jan_r-jan_r0:+.0f}pp")
