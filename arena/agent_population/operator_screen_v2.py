"""Operator-Winner Screen v2 — two-lane: MEMORY (recurring watchlist, leak-free) + PATTERN (mined winner rules).
Reverse-engineers the operator's PROFITABLE Dec+Jan trades into the daily top-15. Production untouched.

Lane MEMORY : recency-decayed intensity of the operator's OWN prior picks of a symbol (strictly earlier
              signal-days only). Captures the ~64% of winners that are recurring names. Fully leak-free:
              on a test day it can only see picks that already happened.
Lane PATTERN: v1 mined winner-vs-loser threshold rules (universe base-rate lift). Adds fresh-name discovery.

Score = Z(memory) + Z(pattern) [+ pullback tilt]. Rank full universe/day. Report recall@15 and the top-15
open->close 5x basket. In-sample (fit=all) AND time-split OOS (Dec->Jan, Jan->Dec). Also splits recall into
RECURRING winners (seen before) vs FRESH winners (never traded before) so we see what memory can/can't reach.
"""
import os, sys, sqlite3, warnings, itertools
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")

ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
WIN_THR = 0.3
MEM_LOOK, MEM_DECAY = 20, 0.90        # trading-day lookback + recency decay for the memory lane
FEATS = ["weekly_close_vs_sma20", "weekly_close_loc", "weekly_range_pct", "roc_20", "roc_60", "roc_5",
         "rsi_14", "dist_high_20", "dist_high_60", "dist_sma_50", "dist_sma_200", "slope_sma_20",
         "slope_sma_50", "atr_20_pct", "atr_5_vs_20", "sret", "twoday", "turn", "tr3"]
LIFT_MIN, PUR, MINW, MIND, MINW2, TOPK1 = 2.0, 0.55, 10, 4, 6, 45


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

# ---------------- load + PIT ----------------
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
    tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    tp = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    for i, d in enumerate(g.trade_date.values):
        TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])
        RET_OC[(s, d)] = ((c[i] - op[i]) / op[i] * 100) if op[i] > 0 else np.nan
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(
    pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None

# operator pick history keyed by signal-day (all picks = watchlist)
pick_sd = {}
for _, p in picks.iterrows():
    s = prev(p.trade_date)
    if s: pick_sd.setdefault(s, []).append(p.symbol)
win_key, lose_key = set(), set()
for _, p in picks.iterrows():
    s = prev(p.trade_date)
    if s: (win_key if p.stock_ret_pct > WIN_THR else lose_key).add((s, p.symbol))
sig_days = sorted({prev(t) for t in picks.trade_date.unique() if prev(t)})
# per-symbol sorted list of prior pick signal-days (as calendar indices) for leak-free memory
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
    i = cidx[sd]
    return any(j < i for j in sym_pickidx.get(sym, []))

# ---------------- panel ----------------
rows = []
for s in sig_days:
    fd = FCpit[FCpit.trade_date == s]
    if fd.empty: continue
    t = nxt(s)
    for _, r in fd.iterrows():
        sym = r.symbol; tf = TF.get((sym, s), (np.nan,) * 5)
        d = {c: (pd.to_numeric(r[c], errors="coerce") if c in fd.columns else np.nan)
             for c in FEATS if c not in ("sret", "twoday", "turn", "tr3")}
        d.update(sret=tf[0], twoday=tf[2], turn=tf[4], tr3=tf[3])
        d["tier"] = classify(tf[0], tf[2], tf[1], np.nan, tf[3], tf[4])
        d["mem"] = memory(sym, s); d["recur"] = ever_before(sym, s)
        lab = 1 if (s, sym) in win_key else (-1 if (s, sym) in lose_key else 0)
        d.update(sd=s, td=t, symbol=sym, label=lab, ret_oc=RET_OC.get((sym, t), np.nan))
        rows.append(d)
P = pd.DataFrame(rows); P["mo"] = P.sd.str[:7]
for f in FEATS: P[f] = P[f].astype(float)
w = P[P.label == 1]
print(f"panel {len(P):,} · WIN {len(w)} · LOSE {int((P.label==-1).sum())} · "
      f"recurring winners {int(w.recur.sum())}/{len(w)} ({w.recur.mean()*100:.0f}%)")

# ---------------- miner (v1) ----------------
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
    return dict(rule=rule, lift=wr / base, w=wc, lo=lo, n=n, days=len(days), pur=pur)
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
    hit = {15: 0}; hitR = {15: 0}; hitF = {15: 0}; tot = totR = totF = 0; bask = []
    for s, g in test.groupby("sd"):
        g = g.sort_values(col, ascending=False)
        bask.append(np.nanmean(g.head(15).ret_oc.values))
        ranks = {sym: i + 1 for i, sym in enumerate(g.symbol)}
        for _, rr in g[g.label == 1].iterrows():
            r = ranks[rr.symbol]; tot += 1
            if rr.recur: totR += 1
            else: totF += 1
            if r <= 15:
                hit[15] += 1; hitR[15] += rr.recur; hitF[15] += (not rr.recur)
    R = lambda h, t: (h / t * 100 if t else 0)
    return dict(r15=R(hit[15], tot), r15_recur=R(hitR[15], totR), r15_fresh=R(hitF[15], totF),
                tot=tot, totR=totR, totF=totF, bask=float(np.nanmean(bask)) * 5)

def run(fitm, testm, tag):
    fit = P[fitm]; test = P[testm].copy()
    lib = mine(fit)
    test["pat"] = patscore(test, lib)
    lanes = {"memory-only": z(test.mem), "pattern-only": z(test.pat),
             "mem+pat": z(test.mem) + z(test.pat),
             "mem+pat+pullback": z(test.mem) + z(test.pat) - 0.5 * z(test.weekly_close_vs_sma20.fillna(0))}
    print(f"\n[{tag}]  rules={len(lib)}")
    for name, sc in lanes.items():
        test["_s"] = sc; e = eval_screen(test, "_s")
        print(f"    {name:<20} recall@15 {e['r15']:4.0f}%  (recurring {e['r15_recur']:3.0f}% of {e['totR']} | "
              f"fresh {e['r15_fresh']:3.0f}% of {e['totF']})   top15 5x {e['bask']:+.2f}%/day")

allm = P.index.notna(); dec = P.mo == "2024-12"; jan = P.mo == "2025-01"
print("\n================= OPERATOR-WINNER SCREEN v2 (memory + pattern) =================")
run(allm, allm, "IN-SAMPLE fit=all -> test=all")
run(dec, jan, "OOS  fit=Dec -> test=Jan")
run(jan, dec, "OOS  fit=Jan -> test=Dec")
