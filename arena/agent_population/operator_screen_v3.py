"""Operator-Winner Screen v3 — add PATH-SHAPE (multi-bar sequence) features to the miner + keep the memory lane.
Tests whether the SHAPE of the last ~20 bars (tight pullback / drying volume / prior run / clean descent),
rather than single-day scalars, separates the operator's FRESH winners from look-alike neutrals. Leak-free, PIT.
"""
import os, sys, sqlite3, warnings, itertools
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
WIN_THR = 0.3; MEM_LOOK, MEM_DECAY = 20, 0.90
SCALAR = ["weekly_close_vs_sma20", "weekly_close_loc", "weekly_range_pct", "roc_20", "roc_60", "rsi_14",
          "dist_high_20", "dist_sma_50", "dist_sma_200", "slope_sma_20", "slope_sma_50", "atr_20_pct", "sret"]
PATH = ["downdays5", "downstreak", "rng_contract", "vol_contract5", "pull_slope5", "prerun20",
        "gap_today", "updays10", "cloc3", "pos20", "chop"]
FEATS = SCALAR + PATH
LIFT_MIN, PUR, MINW, MIND, MINW2, TOPK1 = 2.0, 0.55, 10, 4, 6, 50


def _ok(v): return v is not None and not (isinstance(v, float) and v != v)
def classify(sr, td, rng, al, tr, tn):
    if _ok(sr) and sr > 10: return "AVOID"
    if _ok(sr) and sr > 7 and _ok(tn) and tn >= 0.75: return "AVOID"
    if _ok(sr) and sr <= 2 and _ok(td) and td < -5 and _ok(al) and al > 15: return "PREMIUM-Pullback"
    if _ok(sr) and sr <= 2 and _ok(rng) and rng < 2 and _ok(al) and al > 15: return "PREMIUM-Compression"
    if _ok(sr) and sr <= 2 and _ok(tr) and tr < 0.9: return "ENTERPRISE-Dryup"
    if _ok(sr) and sr <= 2 and _ok(tn) and tn < 0.75: return "GOLD"
    if _ok(sr) and sr <= 2: return "GOLD-baseline"
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
rec = []; TF = {}; RET_OC = {}; PATHF = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float)
    l = g.low.values.astype(float); v = g.volume.values.astype(float); op = g.open.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c / pc - 1) * 100; rng = (h - l) / pc * 100; twoday = (c / c2 - 1) * 100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    av5 = pd.Series(v).rolling(5).mean().values; tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    tp = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    # ---- path-shape (all use bars up to and incl i) ----
    cs = pd.Series(c); ret = cs.pct_change() * 100; TR = pd.Series((h - l) / pc * 100)
    downdays5 = (ret < 0).rolling(5).sum().values
    neg = (ret < 0).astype(int).values
    downstreak = np.zeros(len(c))
    for i in range(len(c)):
        k = 0
        while i - k >= 0 and neg[i - k] == 1: k += 1
        downstreak[i] = k
    rng_contract = (TR.rolling(3).mean() / TR.rolling(15).mean()).values
    vol_contract5 = np.where(av20 > 0, av5 / av20, np.nan)
    pull_slope5 = ((cs - cs.shift(4)) / 4 / cs * 100).values
    prerun20 = (cs.shift(3) / cs.shift(23) - 1).values * 100
    gap_today = (op - pc) / pc * 100
    updays10 = (ret > 0).rolling(10).sum().values
    cloc = np.where(h > l, (c - l) / (h - l), np.nan); cloc3 = pd.Series(cloc).rolling(3).mean().values
    rmax20 = pd.Series(h).rolling(20).max().values; rmin20 = pd.Series(l).rolling(20).min().values
    pos20 = np.where(rmax20 > rmin20, (c - rmin20) / (rmax20 - rmin20), np.nan)
    chop = ret.rolling(10).apply(lambda w: np.sum(np.sign(w[1:]) != np.sign(w[:-1])), raw=True).values
    for i, d in enumerate(g.trade_date.values):
        TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])
        RET_OC[(s, d)] = ((c[i] - op[i]) / op[i] * 100) if op[i] > 0 else np.nan
        PATHF[(s, d)] = dict(downdays5=downdays5[i], downstreak=downstreak[i], rng_contract=rng_contract[i],
            vol_contract5=vol_contract5[i], pull_slope5=pull_slope5[i], prerun20=prerun20[i],
            gap_today=gap_today[i], updays10=updays10[i], cloc3=cloc3[i], pos20=pos20[i], chop=chop[i])
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
    i = cidx[sd]; m = 0.0
    for j in sym_pickidx.get(sym, []):
        age = i - j
        if 0 < age <= MEM_LOOK: m += MEM_DECAY ** age
    return m
def ever_before(sym, sd):
    i = cidx[sd]; return any(j < i for j in sym_pickidx.get(sym, []))

rows = []
for s in sig_days:
    fd = FCpit[FCpit.trade_date == s]
    if fd.empty: continue
    t = nxt(s)
    for _, r in fd.iterrows():
        sym = r.symbol; tf = TF.get((sym, s), (np.nan,) * 5); pf = PATHF.get((sym, s), {})
        d = {c: (pd.to_numeric(r[c], errors="coerce") if c in fd.columns else np.nan) for c in SCALAR if c != "sret"}
        d["sret"] = tf[0]; d.update({k: pf.get(k, np.nan) for k in PATH})
        d["tier"] = classify(tf[0], tf[2], tf[1], np.nan, tf[3], tf[4])
        d["mem"] = memory(sym, s); d["recur"] = ever_before(sym, s)
        lab = 1 if (s, sym) in win_key else (-1 if (s, sym) in lose_key else 0)
        d.update(sd=s, td=t, symbol=sym, label=lab, ret_oc=RET_OC.get((sym, t), np.nan))
        rows.append(d)
P = pd.DataFrame(rows); P["mo"] = P.sd.str[:7]
for f in FEATS: P[f] = P[f].astype(float)
print(f"panel {len(P):,} · WIN {int((P.label==1).sum())} · feats {len(FEATS)} ({len(PATH)} path-shape)")

def cutpoints(x):
    x = x[~np.isnan(x)]
    if len(x) < 50: return []
    return sorted(set(np.round(np.quantile(x, np.linspace(0.1, 0.9, 9)), 3)))
def rmask(df, rule):
    m = np.ones(len(df), bool)
    for f, op, th in rule:
        col = df[f].values; m &= (col <= th) if op == "<=" else (col >= th); m &= ~np.isnan(col)
    return m
def score_rule(fit, rule, minw):
    m = rmask(fit, rule); lab = fit.label.values[m]
    wc = int((lab == 1).sum()); lo = int((lab == -1).sum()); n = int(m.sum())
    if wc < minw or n == 0: return None
    days = set(fit.sd.values[m][lab == 1]) if wc else set()
    if len(days) < MIND: return None
    base = (fit.label.values == 1).mean(); wr = wc / n
    if base <= 0 or wr / base < LIFT_MIN: return None
    pur = wc / (wc + lo) if (wc + lo) else 1.0
    if pur < PUR: return None
    return dict(rule=rule, lift=wr / base, w=wc, lo=lo, n=n, pur=pur, days=len(days))
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
    return dict(r15=R(hit, tot), rr=R(hitR, totR), rf=R(hitF, totF), totR=totR, totF=totF,
                bask=float(np.nanmean(bask)) * 5)
def run(fitm, testm, tag):
    fit = P[fitm]; test = P[testm].copy(); lib = mine(fit); test["pat"] = patscore(test, lib)
    print(f"\n[{tag}]  rules={len(lib)}")
    for name, sc in {"pattern-only(path)": z(test.pat), "mem+pat(path)": z(test.mem) + z(test.pat)}.items():
        test["_s"] = sc; e = eval_screen(test, "_s")
        print(f"    {name:<20} recall@15 {e['r15']:4.0f}%  (recur {e['rr']:3.0f}%/{e['totR']} | fresh {e['rf']:3.0f}%/{e['totF']})   top15 5x {e['bask']:+.2f}%/day")
    # which lane do path features dominate?
    return lib
allm = P.index.notna(); dec = P.mo == "2024-12"; jan = P.mo == "2025-01"
print("\n================= OPERATOR-WINNER SCREEN v3 (path-shape) =================")
lib = run(allm, allm, "IN-SAMPLE fit=all")
run(dec, jan, "OOS fit=Dec->Jan"); run(jan, dec, "OOS fit=Jan->Dec")
print("\n----- top mined rules using a PATH-SHAPE feature (fit=all) -----")
pth = set(PATH)
for r in sorted([r for r in lib if any(f in pth for f, _, _ in r["rule"])], key=lambda r: -r["lift"])[:12]:
    rs = " & ".join(f"{f}{op}{th}" for f, op, th in r["rule"])
    print(f"    lift {r['lift']:4.1f} W{r['w']:>3} L{r['lo']:>3} N{r['n']:>4} pur{r['pur']:.2f}  {rs}")
