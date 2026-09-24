"""PER-PATTERN WEIGHTED RANKER — give EACH winner-pool pattern its own trainable weight, fit the weights to move
the operator's winning stocks into the top-15, rank by the weighted sum. This is the literal 'add weights to the
patterns' ask, done maximally: a logistic learning-to-rank on the 182 pattern-fire indicators (winner vs rest),
optimized on Dec. Reported at several regularization strengths (capacity) so we SEE in-sample vs next-month.
Also a memory+patterns variant. Leak-free (PIT), production untouched.
"""
import os, sys, sqlite3, warnings
from collections import Counter, defaultdict
import numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
POOL_MINSUP = 3; MEM_LOOK, MEM_DECAY = 20, 0.90


def _ok(v): return v is not None and not (isinstance(v, float) and v != v)
picks = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con); PM = {p["pattern_id"]: p for p in pats}
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-10-15' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily "
                       "WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()
# PIT weekly for pattern feature-vector (rule_mask needs FEATURE_COLS incl weekly)
o2 = oh.merge(pd.read_sql_query("SELECT 1", sqlite3.connect(":memory:")).iloc[0:0], how="left") if False else oh.copy()
# recompute weekly PIT
ohw = pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",
                        sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True))
dt = pd.to_datetime(ohw.trade_date); ohw["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in ohw.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values; c = g.close.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None
RET_OC = {}
for s, g in oh.groupby("symbol", sort=False):
    for _, r in g.iterrows():
        RET_OC[(s, r.trade_date)] = ((r.close - r.open) / r.open * 100) if r.open > 0 else np.nan
sym_pickidx = {}
for _, p in picks.iterrows():
    s = prev(p.trade_date)
    if s: sym_pickidx.setdefault(p.symbol, []).append(cidx[s])
def memory(sym, i): return sum(MEM_DECAY ** (i - j) for j in sorted(sym_pickidx.get(sym, [])) if 0 < i - j <= MEM_LOOK)

# winner-pool patterns from Dec winners
dec = picks[(picks.trade_date >= "2024-12-01") & (picks.trade_date <= "2024-12-31")]
winners = dec[dec.stock_ret_pct > 0]
def Xmat(fd):
    syms = fd.symbol.values; X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    return syms, X
pf = Counter()
for _, p in winners.iterrows():
    sd = prev(p.trade_date)
    fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == p.symbol)] if sd else None
    if fr is None or fr.empty: continue
    _, X = Xmat(fr); yr = int(sd[:4])
    for pp in pats:
        if int(pp["mined_year"]) < yr and FR.rule_mask(pp["rule"], X)[0]: pf[pp["pattern_id"]] += 1
POOL = [pid for pid, n in pf.items() if n >= POOL_MINSUP]
print(f"winner-pool patterns weighted individually: {len(POOL)}")

def build(lo, hi):
    sub = picks[(picks.trade_date >= lo) & (picks.trade_date <= hi)]
    wl = sub[sub.stock_ret_pct > 0]; wbd = defaultdict(set)
    for _, p in wl.iterrows():
        sd = prev(p.trade_date)
        if sd: wbd[sd].add(p.symbol)
    days = []
    for sd in sorted(wbd):
        fd = FCpit[FCpit.trade_date == sd]
        if fd.empty: continue
        syms, X = Xmat(fd); yr = int(sd[:4]); n = len(syms)
        F = np.zeros((n, len(POOL)))
        for j, pid in enumerate(POOL):
            if int(PM[pid]["mined_year"]) < yr: F[:, j] = FR.rule_mask(PM[pid]["rule"], X).astype(float)
        mem = np.array([memory(s, cidx[sd]) for s in syms])
        y = np.array([1 if s in wbd[sd] else 0 for s in syms])
        days.append(dict(sd=sd, syms=syms, F=F, mem=mem, y=y, td=nxt(sd)))
    return days
DEC = build("2024-12-01", "2024-12-31"); JAN = build("2025-01-01", "2025-01-31")

def recall(days, coef, intercept, use_mem, memw=0.0):
    hit = tot = 0; bask = []
    for d in days:
        sc = d["F"] @ coef + intercept + (memw * d["mem"] if use_mem else 0)
        order = np.argsort(-sc); top = set(order[:15].tolist())
        tot += int(d["y"].sum()); hit += int(sum(d["y"][k] for k in order[:15]))
        bask.append(np.nanmean([RET_OC.get((d["syms"][k], d["td"]), np.nan) for k in order[:15]]))
    return (hit / tot * 100 if tot else 0), float(np.nanmean(bask)) * 5

Xtr = np.vstack([d["F"] for d in DEC]); ytr = np.concatenate([d["y"] for d in DEC])
memtr = np.concatenate([d["mem"] for d in DEC])
print("\n=========== PER-PATTERN WEIGHTS (fit on Dec) ===========")
print(f"{'model':<34}{'DEC r@15':>10}{'DEC 5x':>9}{'JAN r@15':>10}{'JAN 5x':>9}")
for C in [0.02, 0.2, 2.0, 100.0]:
    clf = LogisticRegression(C=C, class_weight="balanced", max_iter=3000, solver="liblinear")
    clf.fit(Xtr, ytr); co = clf.coef_[0]; ic = clf.intercept_[0]
    dr, db = recall(DEC, co, ic, False); jr, jb = recall(JAN, co, ic, False)
    print(f"  patterns only  C={C:<7}         {dr:>8.0f}%{db:>+8.2f}%{jr:>9.0f}%{jb:>+8.2f}%")
# patterns + memory
XtrM = np.column_stack([Xtr, memtr])
for C in [0.2, 100.0]:
    clf = LogisticRegression(C=C, class_weight="balanced", max_iter=3000, solver="liblinear")
    clf.fit(XtrM, ytr); co = clf.coef_[0][:-1]; memw = clf.coef_[0][-1]; ic = clf.intercept_[0]
    dr, db = recall(DEC, co, ic, True, memw); jr, jb = recall(JAN, co, ic, True, memw)
    print(f"  patterns+memory C={C:<7}         {dr:>8.0f}%{db:>+8.2f}%{jr:>9.0f}%{jb:>+8.2f}%")
print("\n(High C = less regularization = each pattern weighted freely to fit Dec winners.)")
