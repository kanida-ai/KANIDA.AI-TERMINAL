"""WINNER-DNA SCREENER, RANKED — sort -> rank -> invert to the head.
Build the winner pattern-pool DNA (as before), then for each day:
  1) SCORE every universe stock on each winner-DNA dimension (pool lift, pool hits, feature-band match, tier)
  2) rank-normalize (SCALE) each dimension cross-sectionally to [0,1]  (law-of-scaling: put all signals on one axis)
  3) COMBINE the scaled ranks into a composite
  4) SORT descending, assign RANK 1..N, take the HEAD (top-15)  = the INVERSE selection
Report recall of operator winners into the ranked top-15/30 + the top-15 open->close 5x basket, for Dec AND Jan.
Also writes the daily ranked screener output to Excel. Leak-free (PIT), production untouched.
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
KEYF = ["weekly_close_vs_sma20", "roc_20", "roc_60", "rsi_14", "dist_high_20", "dist_sma_200", "atr_20_pct"]
POOL_MINSUP = 3
OUT = os.path.join(os.path.expanduser("~"), "Downloads", "WINNER_DNA_SCREENER_RANKED.xlsx")


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

def Xmat(fd):
    syms = fd.symbol.values; X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    return syms, X

# ---- winner DNA from Dec winners (stock_ret_pct>0) ----
dec = picks[(picks.trade_date >= "2024-12-01") & (picks.trade_date <= "2024-12-31")]
winners = dec[dec.stock_ret_pct > 0]
pat_freq = Counter(); featvals = defaultdict(list); tier_freq = Counter()
for _, p in winners.iterrows():
    sd = prev(p.trade_date)
    if not sd: continue
    fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == p.symbol)]
    if fr.empty: continue
    _, X = Xmat(fr); yr = int(sd[:4])
    for pp in pats:
        if int(pp["mined_year"]) < yr and FR.rule_mask(pp["rule"], X)[0]: pat_freq[pp["pattern_id"]] += 1
    for f in KEYF:
        val = pd.to_numeric(fr[f].iloc[0], errors="coerce") if f in fr.columns else np.nan
        if _ok(val): featvals[f].append(val)
    tf = TF.get((p.symbol, sd), (np.nan,) * 5); tier_freq[classify(tf[0], tf[2], tf[1], np.nan, tf[3], tf[4])] += 1
pool = [pid for pid in pat_freq if pat_freq[pid] >= POOL_MINSUP]
bands = {f: (np.percentile(v, 10), np.percentile(v, 90)) for f, v in featvals.items() if len(v) >= 10}
win_tiers = {t for t, c in tier_freq.items() if c >= 2}
print(f"winner DNA: pool {len(pool)} patterns · tiers {win_tiers} · {len(bands)} feature bands")

def screen_day(sd):
    fd = FCpit[FCpit.trade_date == sd]
    if fd.empty: return pd.DataFrame()
    syms, X = Xmat(fd); yr = int(sd[:4])
    lift = np.zeros(len(syms)); hits = np.zeros(len(syms))
    for pid in pool:
        p = PM[pid]
        if int(p["mined_year"]) >= yr: continue
        m = FR.rule_mask(p["rule"], X); lift += m.astype(float) * (p["oos_lift"] or 0); hits += m.astype(float)
    band = np.zeros(len(syms))
    for f, (lo, hi) in bands.items():
        col = pd.to_numeric(fd[f], errors="coerce").values if f in fd.columns else np.full(len(syms), np.nan)
        band += ((col >= lo) & (col <= hi)).astype(float)
    tierv = []
    for sym in syms:
        tf = TF.get((sym, sd), (np.nan,) * 5); tierv.append(classify(tf[0], tf[2], tf[1], np.nan, tf[3], tf[4]))
    d = pd.DataFrame(dict(symbol=syms, pool_lift=lift, pool_hits=hits, band=band, tier=tierv))
    d["tier_ok"] = d.tier.isin(win_tiers).astype(float)
    # STEP 2 — rank-normalize (SCALE) each dimension cross-sectionally to [0,1]
    for col in ["pool_lift", "pool_hits", "band", "tier_ok"]:
        d[col + "_r"] = d[col].rank(pct=True)
    # STEP 3 — COMBINE scaled ranks into a composite
    d["composite"] = d[["pool_lift_r", "pool_hits_r", "band_r", "tier_ok_r"]].mean(axis=1)
    # STEP 4 — SORT desc, RANK, invert-to-head
    d = d.sort_values("composite", ascending=False).reset_index(drop=True)
    d["rank"] = np.arange(1, len(d) + 1)
    d["sd"] = sd; d["td"] = nxt(sd)
    return d

def run_month(name, lo, hi):
    sub = picks[(picks.trade_date >= lo) & (picks.trade_date <= hi)]
    wl = sub[sub.stock_ret_pct > 0]
    wins_by_day = defaultdict(set)
    for _, p in wl.iterrows():
        sd = prev(p.trade_date)
        if sd: wins_by_day[sd].add(p.symbol)
    sig_days = sorted(wins_by_day)
    outrows = []; hit15 = hit30 = tot = 0; bask = []
    for sd in sig_days:
        d = screen_day(sd)
        if d.empty: continue
        ws = wins_by_day[sd]; rk = dict(zip(d.symbol, d["rank"]))
        for sym in ws:
            if sym in rk:
                tot += 1; hit15 += rk[sym] <= 15; hit30 += rk[sym] <= 30
        top = d.head(15)
        bask.append(np.nanmean([RET_OC.get((s, top.td.iloc[0]), np.nan) for s in top.symbol]))
        mp = set(sub[sub.trade_date == nxt(sd)].symbol) if nxt(sd) else set()
        for _, r in top.iterrows():
            outrows.append(dict(signal_date=sd, trade_date=r.td, rank=int(r["rank"]), symbol=r.symbol,
                                composite=round(r.composite, 3), pool_lift=round(r.pool_lift, 1),
                                pool_hits=int(r.pool_hits), band_match=int(r.band), tier=r.tier,
                                my_pick=("YES" if r.symbol in mp else ""),
                                was_winner=("YES" if r.symbol in ws else ""),
                                ret_oc_next=round(RET_OC.get((r.symbol, r.td), np.nan), 2)))
    print(f"\n[{name}]  winners in ranked TOP-15 {hit15}/{tot}={hit15/tot*100:.0f}% · TOP-30 {hit30}/{tot}={hit30/tot*100:.0f}%"
          f" · top-15 open->close 5x basket {np.nanmean(bask)*5:+.2f}%/day")
    return pd.DataFrame(outrows)

print("\n============ RANKED WINNER-DNA SCREENER (sort->rank->head) ============")
decout = run_month("DEC 2024", "2024-12-01", "2024-12-31")
janout = run_month("JAN 2025", "2025-01-01", "2025-01-31")
with pd.ExcelWriter(OUT, engine="openpyxl") as xw:
    decout.to_excel(xw, "dec_top15", index=False); janout.to_excel(xw, "jan_top15", index=False)
print(f"\nsaved ranked daily top-15 -> {OUT}")
