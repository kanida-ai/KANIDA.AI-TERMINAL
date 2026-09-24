"""Operator-Winner Screen v4 — add SIGNAL-DAY INTRADAY-TAPE features (from 1-min bars) to the miner + memory.
Question: does how a name behaved intraday ON THE SIGNAL DAY (closing drive, late accumulation, finish-near-high,
VWAP strength, buying-pressure) separate the operator's FRESH winners from look-alike neutrals, where daily
scalar/path features failed? Leak-free (intraday of day S is fully known before the next-day 09:15 entry).
Prints: (1) univariate winner-vs-neutral lift of each intraday feature, (2) mined recall@15, split recur/fresh.
"""
import os, sys, sqlite3, warnings, itertools
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")   # 1-min
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
WIN_THR = 0.3; MEM_LOOK, MEM_DECAY = 20, 0.90
SCALAR = ["weekly_close_vs_sma20", "roc_20", "roc_60", "rsi_14", "dist_high_20", "dist_sma_200", "slope_sma_20"]
INTRA = ["id_ret", "id_lasthr", "id_firsthr", "id_cloc", "id_latevol", "id_vwapgap", "id_upfrac", "id_range", "id_retstd"]
FEATS = SCALAR + INTRA
LIFT_MIN, PUR, MINW, MIND, MINW2, TOPK1 = 1.8, 0.55, 10, 4, 6, 45


def _ok(v): return v is not None and not (isinstance(v, float) and v != v)
def classify(sr, td, rng, al, tr, tn):
    if _ok(sr) and sr > 10: return "AVOID"
    if _ok(sr) and sr <= 2 and _ok(tr) and tr < 0.9: return "ENTERPRISE-Dryup"
    if _ok(sr) and sr <= 2: return "GOLD"
    if _ok(sr) and sr <= 5: return "STANDARD"
    return "STANDARD-weak"

picks = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-11-15' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}; RET_OC = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); v = g.volume.values.astype(float); op = g.open.values.astype(float)
    h = g.high.values.astype(float); l = g.low.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c / pc - 1) * 100; rng = (h - l) / pc * 100; twoday = (c / c2 - 1) * 100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    for i, d in enumerate(g.trade_date.values):
        TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i])
        RET_OC[(s, d)] = ((c[i] - op[i]) / op[i] * 100) if op[i] > 0 else np.nan
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(
    pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None
pick_sd = {}
for _, p in picks.iterrows():
    s = prev(p.trade_date)
    if s: pick_sd.setdefault(s, []).append(p.symbol)
win_key, lose_key = set(), set()
for _, p in picks.iterrows():
    s = prev(p.trade_date)
    if s: (win_key if p.stock_ret_pct > WIN_THR else lose_key).add((s, p.symbol))
sig_days = sorted({prev(t) for t in picks.trade_date.unique() if prev(t)})
sym_pickidx = {}
for sd, syms in pick_sd.items():
    for sym in syms: sym_pickidx.setdefault(sym, []).append(cidx[sd])
for k in sym_pickidx: sym_pickidx[k] = sorted(sym_pickidx[k])
def memory(sym, sd):
    i = cidx[sd]; return sum(MEM_DECAY ** (i - j) for j in sym_pickidx.get(sym, []) if 0 < i - j <= MEM_LOOK)
def ever_before(sym, sd):
    i = cidx[sd]; return any(j < i for j in sym_pickidx.get(sym, []))

# ---------------- intraday features per (symbol, signal_day) ----------------
def build_intraday(S, mcon):
    q = ("SELECT symbol, substr(bar_time,12,8) tm, open,high,low,close,volume FROM ohlc_1min "
         f"WHERE bar_time>='{S} 09:15:00' AND bar_time<='{S} 15:29:59' ORDER BY symbol, bar_time")
    df = pd.read_sql_query(q, mcon)
    out = {}
    for sym, g in df.groupby("symbol", sort=False):
        if len(g) < 30: continue
        o = g.open.values; c = g.close.values; h = g.high.values; lw = g.low.values
        vv = g.volume.values.astype(float); tm = g.tm.values
        io, ic, ihi, ilo, tv = o[0], c[-1], h.max(), lw.min(), vv.sum()
        if io <= 0 or tv <= 0 or ihi <= ilo: continue
        vwap = float((c * vv).sum() / tv)
        r = np.diff(c) / c[:-1] * 100
        m1430 = tm <= "14:30:00"; m1015 = tm <= "10:15:00"; late = tm >= "14:30:00"
        c1430 = c[m1430][-1] if m1430.any() else np.nan
        c1015 = c[m1015][-1] if m1015.any() else np.nan
        out[sym] = dict(
            id_ret=(ic - io) / io * 100,
            id_lasthr=((ic - c1430) / c1430 * 100) if _ok(c1430) and c1430 > 0 else np.nan,
            id_firsthr=((c1015 - io) / io * 100) if _ok(c1015) and io > 0 else np.nan,
            id_cloc=(ic - ilo) / (ihi - ilo),
            id_latevol=vv[late].sum() / tv,
            id_vwapgap=(ic - vwap) / vwap * 100,
            id_upfrac=float((r > 0).mean()) if len(r) else np.nan,
            id_range=(ihi - ilo) / io * 100,
            id_retstd=float(np.nanstd(r)) if len(r) else np.nan)
    return out

print("computing intraday features per signal-day (1-min)...")
mcon = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
INTRAF = {}
for S in sig_days:
    d = build_intraday(S, mcon)
    for sym, f in d.items(): INTRAF[(sym, S)] = f
mcon.close()
print(f"  intraday feature rows: {len(INTRAF):,}")

# ---------------- panel ----------------
rows = []
for s in sig_days:
    fd = FCpit[FCpit.trade_date == s]
    if fd.empty: continue
    t = nxt(s)
    for _, r in fd.iterrows():
        sym = r.symbol; tf = TF.get((sym, s), (np.nan,) * 4); idf = INTRAF.get((sym, s))
        d = {c: (pd.to_numeric(r[c], errors="coerce") if c in fd.columns else np.nan) for c in SCALAR}
        d.update({k: (idf[k] if idf else np.nan) for k in INTRA})
        d["mem"] = memory(sym, s); d["recur"] = ever_before(sym, s)
        lab = 1 if (s, sym) in win_key else (-1 if (s, sym) in lose_key else 0)
        d.update(sd=s, symbol=sym, label=lab, ret_oc=RET_OC.get((sym, t), np.nan))
        rows.append(d)
P = pd.DataFrame(rows); P["mo"] = P.sd.str[:7]
for f in FEATS: P[f] = P[f].astype(float)
print(f"panel {len(P):,} · WIN {int((P.label==1).sum())} · with-intraday {int(P.id_ret.notna().sum()):,}")

# ---------------- (1) univariate winner-vs-neutral separation on intraday ----------------
print("\n----- intraday feature: WINNER vs NEUTRAL (does the tape separate them?) -----")
print(f"  {'feature':<12}{'win_mean':>10}{'neu_mean':>10}{'top-decile lift':>17}")
wmask = P.label == 1; nmask = P.label == 0
for f in INTRA:
    wv = P.loc[wmask, f].dropna(); nv = P.loc[nmask, f].dropna()
    if len(wv) < 20 or len(nv) < 100: continue
    # decile of neutral dist that maximizes winner concentration (both tails)
    best = 0
    for q, side in [(0.9, "hi"), (0.1, "lo")]:
        th = P[f].quantile(q)
        fired = P[(P[f] >= th)] if side == "hi" else P[(P[f] <= th)]
        base = wmask.mean(); wr = (fired.label == 1).mean()
        best = max(best, wr / base if base else 0)
    print(f"  {f:<12}{wv.mean():>10.2f}{nv.mean():>10.2f}{best:>17.2f}")

# ---------------- (2) mine + recall ----------------
def cutpoints(x):
    x = x[~np.isnan(x)]
    return [] if len(x) < 50 else sorted(set(np.round(np.quantile(x, np.linspace(0.1, 0.9, 9)), 3)))
def rmask(df, rule):
    m = np.ones(len(df), bool)
    for f, op, th in rule:
        col = df[f].values; m &= (col <= th) if op == "<=" else (col >= th); m &= ~np.isnan(col)
    return m
def score_rule(fit, rule, minw):
    m = rmask(fit, rule); lab = fit.label.values[m]
    wc = int((lab == 1).sum()); lo = int((lab == -1).sum()); n = int(m.sum())
    if wc < minw or n == 0: return None
    if len(set(fit.sd.values[m][lab == 1])) < MIND: return None
    base = (fit.label.values == 1).mean(); wr = wc / n
    if base <= 0 or wr / base < LIFT_MIN: return None
    pur = wc / (wc + lo) if (wc + lo) else 1.0
    if pur < PUR: return None
    return dict(rule=rule, lift=wr / base, w=wc, lo=lo, n=n, pur=pur)
def mine(fit):
    one = []
    for f in FEATS:
        for th in cutpoints(fit[f].values):
            for op in ("<=", ">="):
                r = score_rule(fit, [(f, op, th)], MINW)
                if r: one.append(r)
    one.sort(key=lambda r: -r["lift"]); lib = list(one)
    for a, b in itertools.combinations(one[:TOPK1], 2):
        if a["rule"][0][0] == b["rule"][0][0]: continue
        r = score_rule(fit, a["rule"] + b["rule"], MINW2)
        if r: lib.append(r)
    seen = {}
    for r in lib:
        k = tuple(sorted((f, op, round(th, 3)) for f, op, th in r["rule"]))
        if k not in seen or r["lift"] > seen[k]["lift"]: seen[k] = r
    return list(seen.values())
def patscore(df, lib):
    sc = np.zeros(len(df))
    for r in lib: sc += rmask(df, r["rule"]).astype(float) * np.log(r["lift"])
    return sc
def z(s):
    s = pd.Series(s).astype(float); mu, sd = s.mean(), s.std()
    return ((s - mu) / sd).fillna(0).values if sd and sd == sd else np.zeros(len(s))
def eval_screen(test, col):
    hit = hitR = hitF = tot = totR = totF = 0; bask = []
    for s, g in test.groupby("sd"):
        g = g.sort_values(col, ascending=False); bask.append(np.nanmean(g.head(15).ret_oc.values))
        ranks = {sym: i + 1 for i, sym in enumerate(g.symbol)}
        for _, rr in g[g.label == 1].iterrows():
            r = ranks[rr.symbol]; tot += 1; totR += rr.recur; totF += (not rr.recur)
            if r <= 15: hit += 1; hitR += rr.recur; hitF += (not rr.recur)
    R = lambda a, b: (a / b * 100 if b else 0)
    return dict(r15=R(hit, tot), rr=R(hitR, totR), rf=R(hitF, totF), totR=totR, totF=totF, bask=float(np.nanmean(bask)) * 5)
def run(fitm, testm, tag):
    fit = P[fitm]; test = P[testm].copy(); lib = mine(fit); test["pat"] = patscore(test, lib)
    npi = sum(any(f in set(INTRA) for f, _, _ in r["rule"]) for r in lib)
    print(f"\n[{tag}]  rules={len(lib)} ({npi} use an intraday feat)")
    for name, sc in {"intraday-pattern": z(test.pat), "mem+intraday": z(test.mem) + z(test.pat)}.items():
        test["_s"] = sc; e = eval_screen(test, "_s")
        print(f"    {name:<16} recall@15 {e['r15']:4.0f}% (recur {e['rr']:3.0f}%/{e['totR']} | fresh {e['rf']:3.0f}%/{e['totF']}) top15 5x {e['bask']:+.2f}%/day")
    return lib
allm = P.index.notna(); dec = P.mo == "2024-12"; jan = P.mo == "2025-01"
print("\n================= v4 INTRADAY-TAPE =================")
lib = run(allm, allm, "IN-SAMPLE fit=all"); run(dec, jan, "OOS Dec->Jan"); run(jan, dec, "OOS Jan->Dec")
print("\n----- top mined rules USING an intraday feature (fit=all) -----")
iset = set(INTRA)
for r in sorted([r for r in lib if any(f in iset for f, _, _ in r["rule"])], key=lambda r: -r["lift"])[:12]:
    print(f"    lift {r['lift']:4.1f} W{r['w']:>3} L{r['lo']:>3} N{r['n']:>4} pur{r['pur']:.2f}  " +
          " & ".join(f"{f}{op}{th}" for f, op, th in r["rule"]))
