"""Operator-Winner Screen v1 — reverse-engineer a SEPARATE mined pattern library that surfaces the
operator's PROFITABLE Dec+Jan manual trades into the daily top-15. Leak-free (PIT). Production untouched.

Label per (signal_date S, symbol), operator trades next day T=S+1:
  WIN  = operator picked on T and stock_ret_pct > WIN_THR (0.3, net of 5x cost)
  LOSE = operator picked on T and stock_ret_pct <= WIN_THR   (contrast class)
  NEUT = everyone else in the universe on S            (background base rate)

Mining: 1- and 2-condition threshold rules over leak-free features. A rule is kept only if, on the FIT split,
it fires on enough WIN instances across enough distinct days, beats the universe WIN base-rate by >=LIFT_MIN,
and is winner-pure among operator picks (WIN/(WIN+LOSE) >= PUR). Score(stock)=sum log(lift) over fired rules.
The universe base-rate denominator is what stops the library flooding the pool with neutral pullbacks.

Eval: rank the FULL universe each day by score; report recall@15 of WINs, plus the top-15 basket's
open->close 5x return (a money proxy that covers neutrals we never traded), vs Falcon + operator-all baselines.
Reported in-sample (fit=all) AND time-split OOS (fit Dec->test Jan ; fit Jan->test Dec).
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
FEATS = ["weekly_close_vs_sma20", "weekly_close_loc", "weekly_range_pct",
         "roc_20", "roc_60", "roc_5", "rsi_14",
         "dist_high_20", "dist_high_60", "dist_sma_50", "dist_sma_200",
         "slope_sma_20", "slope_sma_50", "atr_20_pct", "atr_5_vs_20",
         "sret", "twoday", "turn", "tr3"]
# mining knobs
LIFT_MIN, PUR, MINW, MIND, MINW2, TOPK1 = 2.0, 0.55, 10, 4, 6, 45


def _ok(v): return v is not None and not (isinstance(v, float) and v != v)
def classify(sret, twoday, rng, al, tr, turn):
    if _ok(sret) and sret > 10: return "AVOID"
    if _ok(sret) and sret > 7 and _ok(turn) and turn >= 0.75: return "AVOID"
    if _ok(sret) and sret <= 2 and _ok(twoday) and twoday < -5 and _ok(al) and al > 15: return "PREMIUM-Pullback"
    if _ok(sret) and sret <= 2 and _ok(rng) and rng < 2 and _ok(al) and al > 15: return "PREMIUM-Compression"
    if _ok(sret) and sret <= 2 and _ok(tr) and tr < 0.9: return "ENTERPRISE-Dryup"
    if _ok(sret) and sret <= 2 and _ok(turn) and turn < 0.75: return "GOLD"
    if _ok(sret) and sret <= 2: return "GOLD-baseline"
    if _ok(sret) and sret <= 5: return "STANDARD"
    return "STANDARD-weak"


# ---------------- load ----------------
picks = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query(
    "SELECT * FROM falcon_features WHERE trade_date>='2024-11-15' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query(
    "SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
    "WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()

# ---------------- PIT weekly + extras + open->close return ----------------
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}; RET_OC = {}
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
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c / pc - 1) * 100; rng = (h - l) / pc * 100; twoday = (c / c2 - 1) * 100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    tp = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    roc = (op > 0) & (c > 0)
    for i, d in enumerate(g.trade_date.values):
        TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])
        RET_OC[(s, d)] = ((c[i] - op[i]) / op[i] * 100) if op[i] > 0 else np.nan
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(
    pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
def next_td(d):
    i = cidx.get(d); return cal[i + 1] if (i is not None and i + 1 < len(cal)) else None
def prev_td(d):
    i = cidx.get(d); return cal[i - 1] if (i is not None and i - 1 >= 0) else None

# ---------------- panel over operator signal-days ----------------
win_key, lose_key = set(), set()
for _, p in picks.iterrows():
    s = prev_td(p.trade_date)
    if s is None: continue
    (win_key if p.stock_ret_pct > WIN_THR else lose_key).add((s, p.symbol))
sig_days = sorted({prev_td(t) for t in picks.trade_date.unique() if prev_td(t)})

rows = []
for s in sig_days:
    fd = FCpit[FCpit.trade_date == s]
    if fd.empty: continue
    t = next_td(s)
    for _, r in fd.iterrows():
        sym = r.symbol; tf = TF.get((sym, s), (np.nan,) * 5)
        d = {c: (pd.to_numeric(r[c], errors="coerce") if c in fd.columns else np.nan)
             for c in FEATS if c not in ("sret", "twoday", "turn", "tr3")}
        d.update(sret=tf[0], twoday=tf[2], turn=tf[4], tr3=tf[3])
        d["tier"] = classify(tf[0], tf[2], tf[1], np.nan, tf[3], tf[4])
        lab = 1 if (s, sym) in win_key else (-1 if (s, sym) in lose_key else 0)
        d.update(sd=s, td=t, symbol=sym, label=lab, ret_oc=RET_OC.get((sym, t), np.nan))
        rows.append(d)
P = pd.DataFrame(rows)
P["mo"] = P.sd.str[:7]
for f in FEATS: P[f] = P[f].astype(float)
print(f"panel: {len(P):,} stock-days · {P.sd.nunique()} signal-days · "
      f"WIN {int((P.label==1).sum())} · LOSE {int((P.label==-1).sum())} · NEUT {int((P.label==0).sum())}")
print(f"WIN by month: {P[P.label==1].groupby('mo').size().to_dict()}")

# ---------------- miner ----------------
def cutpoints(x):
    x = x[~np.isnan(x)]
    if len(x) < 50: return []
    qs = np.quantile(x, np.linspace(0.1, 0.9, 9))
    return sorted(set(np.round(qs, 3)))

def rule_mask(df, rule):
    m = np.ones(len(df), bool)
    for f, op, th in rule:
        col = df[f].values
        m &= (col <= th) if op == "<=" else (col >= th)
        m &= ~np.isnan(col)
    return m

def score_rule(fit, rule):
    m = rule_mask(fit, rule); lab = fit.label.values[m]
    w = int((lab == 1).sum()); lo = int((lab == -1).sum()); n = int(m.sum())
    if w < MINW or n == 0: return None
    days = fit.sd.values[m][lab == 1] if w else []
    if len(set(days)) < MIND: return None
    base = (fit.label.values == 1).mean()
    wr = w / n
    if base <= 0 or wr / base < LIFT_MIN: return None
    pur = w / (w + lo) if (w + lo) else 1.0
    if pur < PUR: return None
    return dict(rule=rule, lift=wr / base, w=w, lo=lo, n=n, days=len(set(days)), pur=pur)

def mine(fit):
    one = []
    for f in FEATS:
        for th in cutpoints(fit[f].values):
            for op in ("<=", ">="):
                r = score_rule(fit, [(f, op, th)])
                if r: one.append(r)
    one.sort(key=lambda r: -r["lift"])
    top = one[:TOPK1]
    lib = list(one)
    # 2-condition: pair top-1cond rules on DIFFERENT features, keep if purer & supported
    for a, b in itertools.combinations(top, 2):
        fa = a["rule"][0][0]; fb = b["rule"][0][0]
        if fa == fb: continue
        r = score_rule2(fit, a["rule"] + b["rule"])
        if r: lib.append(r)
    # dedup by rule signature, keep best lift
    seen = {}
    for r in lib:
        k = tuple(sorted((f, op, round(th, 3)) for f, op, th in r["rule"]))
        if k not in seen or r["lift"] > seen[k]["lift"]: seen[k] = r
    return list(seen.values())

def score_rule2(fit, rule):
    m = rule_mask(fit, rule); lab = fit.label.values[m]
    w = int((lab == 1).sum()); lo = int((lab == -1).sum()); n = int(m.sum())
    if w < MINW2 or n == 0: return None
    days = fit.sd.values[m][lab == 1] if w else []
    if len(set(days)) < MIND: return None
    base = (fit.label.values == 1).mean(); wr = w / n
    if base <= 0 or wr / base < LIFT_MIN: return None
    pur = w / (w + lo) if (w + lo) else 1.0
    if pur < PUR: return None
    return dict(rule=rule, lift=wr / base, w=w, lo=lo, n=n, days=len(set(days)), pur=pur)

def apply_score(df, lib):
    sc = np.zeros(len(df))
    for r in lib:
        sc += rule_mask(df, r["rule"]).astype(float) * np.log(r["lift"])
    return sc

# ---------------- eval ----------------
def eval_screen(test, score_col):
    hit = {15: 0, 30: 0}; tot = 0; bask = []
    for s, g in test.groupby("sd"):
        g = g.sort_values(score_col, ascending=False)
        top = g.head(15)
        bask.append(np.nanmean(top.ret_oc.values))
        ranks = {sym: i + 1 for i, sym in enumerate(g.symbol)}
        wins = g[g.label == 1].symbol
        for sym in wins:
            tot += 1
            for k in hit:
                if ranks[sym] <= k: hit[k] += 1
    r15 = hit[15] / tot * 100 if tot else 0; r30 = hit[30] / tot * 100 if tot else 0
    return r15, r30, tot, float(np.nanmean(bask)) * 5   # 5x basket

def run(fit_mask, test_mask, tag):
    fit = P[fit_mask]; test = P[test_mask]
    lib = mine(fit)
    test = test.copy(); test["opsc"] = apply_score(test, lib)
    r15, r30, tot, bask = eval_screen(test, "opsc")
    print(f"\n[{tag}]  library={len(lib)} rules  (fit WIN={int((fit.label==1).sum())})")
    print(f"    recall@15 {r15:4.0f}%   recall@30 {r30:4.0f}%   (over {tot} winners)   top15 5x open->close {bask:+.2f}%/day")
    return lib

dec = P.mo == "2024-12"; jan = P.mo == "2025-01"
print("\n================= OPERATOR-WINNER SCREEN v1 =================")
lib_all = run(P.index.notna(), P.index.notna(), "IN-SAMPLE fit=all -> test=all")
run(dec, jan, "OOS  fit=Dec -> test=Jan")
run(jan, dec, "OOS  fit=Jan -> test=Dec")

# baselines on all days: random-in-universe & tier-gate
print("\n----- baselines (test=all) -----")
def baseline(scorecol, tag, gate=None):
    t = P.copy()
    if gate is not None: t = t[gate(t)]
    t = t.copy(); t["b"] = scorecol(t)
    r15, r30, tot, bask = eval_screen(t, "b")
    print(f"    {tag:<28} recall@15 {r15:4.0f}%  top15 5x {bask:+.2f}%/day")
baseline(lambda t: -t.weekly_close_vs_sma20.fillna(1e9), "rank: most-negative wcvs20")
baseline(lambda t: -t.roc_20.fillna(1e9), "rank: most-negative roc_20")
baseline(lambda t: t.tr3.fillna(1e9).rsub(1e9), "rank: volume dry-up (low tr3)")

# show the top mined rules from the full-fit library
print("\n----- top 15 mined WIN rules (fit=all) by lift -----")
for r in sorted(lib_all, key=lambda r: -r["lift"])[:15]:
    rs = " & ".join(f"{f}{op}{th}" for f, op, th in r["rule"])
    print(f"    lift {r['lift']:4.1f}  W{r['w']:>3} L{r['lo']:>3} N{r['n']:>4} days{r['days']:>3} pur{r['pur']:.2f}  {rs}")
