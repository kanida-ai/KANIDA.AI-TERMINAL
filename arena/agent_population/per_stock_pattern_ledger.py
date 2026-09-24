"""PER-STOCK PATTERN LEDGER — freeze, for each stock, which Falcon patterns actually PAID on that specific name.
For every (symbol, pattern): over history BEFORE the test window, the avg next-day open->close return, hit-rate
and sample count when that pattern fired ON THAT STOCK. Frozen + reproducible. Then score each stock daily by
its OWN frozen history for today's firing patterns (per-stock alpha), falling back to the pattern's global
return when a name has too little history. Walk-forward-safe (ledger built only from data before each test month).
Evaluates recall of the operator's winners vs a POOLED (global-pattern) baseline. Leak-free. Read-only.
"""
import os, sys, sqlite3, warnings, importlib.util
from collections import defaultdict
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
WIN_THR = 0.3; MIN_STOCK_N = 3   # min per-stock samples to trust the per-stock stat
spec = importlib.util.spec_from_file_location("op8", os.path.join(ROOT, "arena", "agent_population", "operator_picks_8mo.py"))
op8 = importlib.util.module_from_spec(spec); spec.loader.exec_module(op8); PICKS = op8.PICKS

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con); PM = {p["pattern_id"]: p for p in pats}
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2022-06-01' AND trade_date<='2025-06-30'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily "
                       "WHERE trade_date>='2021-06-01' AND trade_date<='2025-06-30' ORDER BY symbol,trade_date", con)
con.close()
dt = pd.to_datetime(oh.trade_date); oh["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; FWD = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values; c = g.close.values.astype(float)
    o = g.open.values.astype(float); td = g.trade_date.values
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=td,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (g.high.values - g.low.values) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    # forward return = NEXT day's open->close (the day the operator would trade)
    fret = np.roll((c - o) / o * 100, -1); fret[-1] = np.nan
    for i in range(len(td)): FWD[(s, td[i])] = fret[i]   # keyed by SIGNAL day
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
def Xmat(fd):
    syms = fd.symbol.values; X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    return syms, X

# winner-pool patterns (from operator winners before Dec'24, leak-free)
from collections import Counter
pf = Counter()
for tdte, syms in PICKS.items():
    if tdte > "2024-11-30": continue
    sd = prev(tdte)
    for sym in syms:
        r = FWD.get((sym, sd), np.nan) if sd else np.nan
        # winner if that pick actually went up next day
        rr = None
        if sd:
            fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == sym)]
            if not fr.empty:
                _, X = Xmat(fr); yr = int(sd[:4])
                for pp in pats:
                    if int(pp["mined_year"]) < yr and FR.rule_mask(pp["rule"], X)[0]: pf[pp["pattern_id"]] += 1
POOL = [pid for pid, n in pf.items() if n >= 2]
print(f"pool patterns: {len(POOL)}")

# ---------------- freeze the ledger from history < 2024-12-01 ----------------
LEDGER_END = "2024-12-01"
days_ledger = [d for d in cal if "2022-06-01" <= d < LEDGER_END]
stock_pat = defaultdict(list)   # (symbol,pid) -> [fwd returns]
glob_pat = defaultdict(list)    # pid -> [fwd returns] (global)
print(f"building ledger over {len(days_ledger)} history days...")
for sd in days_ledger:
    fd = FCpit[FCpit.trade_date == sd]
    if fd.empty: continue
    syms, X = Xmat(fd); yr = int(sd[:4])
    for pid in POOL:
        p = PM[pid]
        if int(p["mined_year"]) >= yr: continue
        m = FR.rule_mask(p["rule"], X)
        if not m.any(): continue
        for i in np.where(m)[0]:
            fr = FWD.get((syms[i], sd), np.nan)
            if not np.isnan(fr):
                stock_pat[(syms[i], pid)].append(fr); glob_pat[pid].append(fr)
# freeze means
SP = {k: (np.mean(v), len(v), np.mean(np.array(v) > 0)) for k, v in stock_pat.items() if len(v) >= 1}
GP = {pid: np.mean(v) for pid, v in glob_pat.items() if len(v) >= 20}
print(f"frozen per-stock (stock,pattern) cells: {len(SP):,} · global patterns: {len(GP)}")

def score_day(sd, per_stock=True):
    fd = FCpit[FCpit.trade_date == sd]
    if fd.empty: return {}
    syms, X = Xmat(fd); yr = int(sd[:4]); out = {}
    fires = {pid: FR.rule_mask(PM[pid]["rule"], X) for pid in POOL if int(PM[pid]["mined_year"]) < yr}
    for i, sym in enumerate(syms):
        num = 0.0; den = 0.0
        for pid, m in fires.items():
            if not m[i]: continue
            if per_stock and (sym, pid) in SP and SP[(sym, pid)][1] >= MIN_STOCK_N:
                er, n, _ = SP[(sym, pid)]; w = np.log1p(n); num += er * w; den += w
            elif pid in GP:
                num += GP[pid] * 0.5; den += 0.5
        out[sym] = num / den if den > 0 else -99
    return out

# ---------------- evaluate on operator Dec'24-May'25 winners ----------------
wins_by_sd = defaultdict(set)
for tdte, syms in PICKS.items():
    if tdte < "2024-12-01": continue
    sd = prev(tdte)
    if not sd: continue
    for sym in syms:
        r = FWD.get((sym, sd), np.nan)
        if not np.isnan(r) and r > WIN_THR: wins_by_sd[sd].add(sym)

def evaluate(per_stock, tag):
    hit = {15: 0, 30: 0}; tot = 0; bask = []
    for sd in sorted(wins_by_sd):
        sc = score_day(sd, per_stock)
        if not sc: continue
        order = [s for s, _ in sorted(sc.items(), key=lambda kv: -kv[1])]
        ws = wins_by_sd[sd]; tot += len(ws)
        for k in hit: hit[k] += sum(s in set(order[:k]) for s in ws)
        bask.append(np.nanmean([FWD.get((s, sd), np.nan) for s in order[:15]]))
    R = lambda k: hit[k] / tot * 100 if tot else 0
    print(f"  [{tag}]  recall@15 {R(15):.0f}%  @30 {R(30):.0f}%  (over {tot})  top15 5x {np.nanmean(bask)*5:+.2f}%/day")

print("\n============ PER-STOCK LEDGER vs POOLED ============")
evaluate(True, "PER-STOCK frozen ledger")
evaluate(False, "POOLED (global pattern return)")
# freeze the ledger to disk for reproducibility
led = pd.DataFrame([(s, pid, er, n, hit) for (s, pid), (er, n, hit) in SP.items()],
                   columns=["symbol", "pattern_id", "avg_fwd_ret", "n", "hit_rate"])
led.to_csv(os.path.join(ROOT, "arena", "agent_population", "per_stock_pattern_ledger.csv"), index=False)
print(f"\nfrozen ledger -> per_stock_pattern_ledger.csv ({len(led):,} rows)")
