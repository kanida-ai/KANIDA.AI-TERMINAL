"""WINNER-DNA SCREENER — build a single screening rule from the Falcon footprint of the operator's Dec-2024
WINNING trades, then run it across the whole universe. Measures: (1) does it reproduce the operator's winners,
(2) how selective is it (how many stocks/day pass), (3) are the OTHER stocks it surfaces actually profitable
(open->close 5x proxy). Leak-free (PIT features, patterns gated mined_year<signal year). Production untouched.

Winner DNA collected from each winner's signal-day (S = trade_day - 1):
  - PATTERN POOL : union of Falcon patterns that fired on the winners (with per-pattern winner-frequency + oos_lift)
  - TIER footprint : which signal-time tiers the winners occupied
  - FEATURE bands  : 10th-90th percentile range of key features across the winners
Screener score(stock) = sum oos_lift of fired winner-pool patterns. Then gate by tier / feature bands.
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
WIN_THR = 0.0                # "winning trade" = closed green; net>0.3 reported as robustness
KEYF = ["weekly_close_vs_sma20", "roc_20", "roc_60", "rsi_14", "dist_high_20", "dist_sma_200", "atr_20_pct"]


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
pats = FR.load_patterns(con); PM = {p["pattern_id"]: p for p in pats}
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-10-15' AND trade_date<='2025-01-31'", con)
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
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
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

def fired_patterns(sd, sym):
    fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == sym)]
    if fr.empty: return []
    X = np.full((1, len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fr.columns: X[0, j] = pd.to_numeric(fr[col].iloc[0], errors="coerce")
    yr = int(sd[:4])
    return [p["pattern_id"] for p in pats if int(p["mined_year"]) < yr and FR.rule_mask(p["rule"], X)[0]]

def feat_row(sd, sym):
    fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == sym)]
    if fr.empty: return {}
    return {f: pd.to_numeric(fr[f].iloc[0], errors="coerce") for f in KEYF if f in fr.columns}

# ---------------- collect Dec winner DNA ----------------
dec = picks[(picks.trade_date >= "2024-12-01") & (picks.trade_date <= "2024-12-31")]
winners = dec[dec.stock_ret_pct > WIN_THR]
pat_freq = Counter(); tier_freq = Counter(); featvals = defaultdict(list); nW = 0
for _, p in winners.iterrows():
    sd = prev(p.trade_date)
    if not sd: continue
    nW += 1
    for pid in fired_patterns(sd, p.symbol): pat_freq[pid] += 1
    fr = feat_row(sd, p.symbol)
    for f, val in fr.items():
        if _ok(val): featvals[f].append(val)
    tf = TF.get((p.symbol, sd), (np.nan,) * 5)
    tier_freq[classify(tf[0], tf[2], tf[1], np.nan, tf[3], tf[4])] += 1
# winner pool = patterns that fired on >= MINSUP winners
POOL_MINSUP = 3
pool = {pid: pat_freq[pid] for pid in pat_freq if pat_freq[pid] >= POOL_MINSUP}
bands = {f: (np.percentile(v, 10), np.percentile(v, 90)) for f, v in featvals.items() if len(v) >= 10}
win_tiers = {t for t, c in tier_freq.items() if c >= 2}
print(f"Dec winning trades: {nW}  ·  winner pattern POOL (fired on >={POOL_MINSUP} winners): {len(pool)} of {len(pat_freq)} distinct")
print(f"winner tiers: {dict(tier_freq)}")
print("winner feature bands (10-90 pct):")
for f, (lo, hi) in bands.items(): print(f"    {f:<22} [{lo:+.2f} , {hi:+.2f}]")
print("\ntop winner-pool patterns (freq on winners | oos_lift | rule):")
for pid, fq in sorted(pool.items(), key=lambda kv: -kv[1])[:12]:
    r = " & ".join(f"{f}{op}{round(th,2)}" for f, op, th in PM[pid]["rule"])
    print(f"    FALCPAT_{pid:<6} n={fq:<3} lift={PM[pid]['oos_lift']:.1f}  {r[:70]}")

# ---------------- apply screener across universe (Dec signal-days) ----------------
def score_day(sd):
    fd = FCpit[FCpit.trade_date == sd]
    if fd.empty: return pd.DataFrame()
    syms = fd.symbol.values
    X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    yr = int(sd[:4]); score = np.zeros(len(syms)); ct = np.zeros(len(syms), int)
    for pid in pool:
        p = PM[pid]
        if int(p["mined_year"]) >= yr: continue
        m = FR.rule_mask(p["rule"], X)
        score += m.astype(float) * (p["oos_lift"] or 0); ct += m.astype(int)
    d = pd.DataFrame(dict(symbol=syms, score=score, poolhits=ct))
    for f in KEYF:
        d[f] = pd.to_numeric(fd[f], errors="coerce").values if f in fd.columns else np.nan
    tiers = []
    for sym in syms:
        tf = TF.get((sym, sd), (np.nan,) * 5); tiers.append(classify(tf[0], tf[2], tf[1], np.nan, tf[3], tf[4]))
    d["tier"] = tiers; d["sd"] = sd; d["td"] = nxt(sd)
    return d

sig_days = sorted({prev(t) for t in dec.trade_date.unique() if prev(t)})
wins_by_day = defaultdict(set)
for _, p in winners.iterrows():
    sd = prev(p.trade_date)
    if sd: wins_by_day[sd].add(p.symbol)

def band_ok(row):
    for f, (lo, hi) in bands.items():
        v = row.get(f)
        if _ok(v) and not (lo <= v <= hi): return False
    return True

def evaluate(gate_tier, gate_band, min_hits, label):
    total_pass = 0; total_days = 0; capt = 0; totw = 0; extra_rets = []; extra_n = 0; extra_win = 0
    perday_pass = []
    for sd in sig_days:
        d = score_day(sd)
        if d.empty: continue
        sel = d[d.poolhits >= min_hits]
        if gate_tier: sel = sel[sel.tier.isin(win_tiers)]
        if gate_band: sel = sel[sel.apply(band_ok, axis=1)]
        sel = sel.sort_values("score", ascending=False)
        passset = set(sel.symbol); ws = wins_by_day.get(sd, set())
        capt += len(passset & ws); totw += len(ws)
        perday_pass.append(len(sel)); total_days += 1
        # the EXTRA (non-manual-pick) stocks the screen produced -> did they profit next day?
        manual_all = set(dec[dec.trade_date == nxt(sd)].symbol) if nxt(sd) else set()
        for _, r in sel.iterrows():
            if r.symbol in manual_all: continue
            ret = RET_OC.get((r.symbol, r.td), np.nan)
            if _ok(ret): extra_rets.append(ret); extra_n += 1; extra_win += (ret > 0)
    print(f"\n[{label}]  reproduce winners {capt}/{totw}={capt/totw*100:.0f}% · "
          f"passes/day avg {np.mean(perday_pass):.0f} (max {max(perday_pass)}) · "
          f"NEW stocks produced {extra_n} · their hit% {extra_win/max(extra_n,1)*100:.0f}% · "
          f"their avg 1x {np.mean(extra_rets) if extra_rets else 0:+.2f}% (5x {(np.mean(extra_rets) if extra_rets else 0)*5:+.2f}%)")

print("\n================ SCREENER RESULTS (Dec 2024, applied to full universe) ================")
evaluate(False, False, 1, "pool>=1 pattern, no gate")
evaluate(False, False, 3, "pool>=3 patterns, no gate")
evaluate(True,  False, 3, "pool>=3 + winner-tier gate")
evaluate(True,  True,  3, "pool>=3 + tier + feature-band gate")
evaluate(True,  True,  6, "pool>=6 + tier + feature-band gate (tight)")
