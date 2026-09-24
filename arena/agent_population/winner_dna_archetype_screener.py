"""WINNER-DNA ARCHETYPE SCREENER — multiple rankings, combined.
Instead of pooling ALL Dec winners into one blurry DNA, segment them by SETUP ARCHETYPE (continuation /
breakout / pullback / dryup-compression), build a SEPARATE winner-pattern ranking per archetype, then COMBINE:
a stock's final score = its BEST archetype fit (max of the per-archetype scaled ranks), gated so a stock is only
scored by an archetype it actually belongs to. sort -> rank -> head(15). Leak-free (PIT). Production untouched.
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
POOL_MINSUP = 3
OUT = os.path.join(os.path.expanduser("~"), "Downloads", "WINNER_DNA_ARCHETYPE_SCREENER.xlsx")
ARCHES = ["BREAKOUT", "CONTINUATION", "PULLBACK", "DRYUP"]


def _ok(v): return v is not None and not (isinstance(v, float) and v != v)

def archetypes_of(f):
    """multi-label setup archetype from leak-free signal-time features (dict). f may have NaNs."""
    dh = f.get("dist_high_20"); r20 = f.get("roc_20"); r60 = f.get("roc_60")
    d200 = f.get("dist_sma_200"); sl = f.get("slope_sma_50"); bo = f.get("weekly_breakout_20w")
    wr = f.get("weekly_range_pct"); tr = f.get("tr3"); atr = f.get("atr_20_pct")
    out = set()
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
      "weekly_range_pct", "atr_20_pct", "rsi_14"]
def feats_at(sd, sym, row=None):
    r = row if row is not None else FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == sym)]
    if hasattr(r, "empty") and r.empty: return {}
    d = {f: (pd.to_numeric(r[f].iloc[0] if hasattr(r[f], "iloc") else r[f], errors="coerce")) for f in AF if f in (r.columns if hasattr(r, "columns") else r)}
    d["tr3"] = TF3.get((sym, sd), np.nan)
    return d
def Xmat(fd):
    syms = fd.symbol.values; X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    return syms, X

# ---- build per-archetype winner pools from Dec winners ----
dec = picks[(picks.trade_date >= "2024-12-01") & (picks.trade_date <= "2024-12-31")]
winners = dec[dec.stock_ret_pct > 0]
arch_pat = {a: Counter() for a in ARCHES}; arch_ct = Counter()
for _, p in winners.iterrows():
    sd = prev(p.trade_date)
    if not sd: continue
    fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == p.symbol)]
    if fr.empty: continue
    fdict = feats_at(sd, p.symbol, fr); ars = archetypes_of(fdict)
    if not ars: ars = {"DRYUP"}                       # fallback bucket
    _, X = Xmat(fr); yr = int(sd[:4])
    fired = [pp["pattern_id"] for pp in pats if int(pp["mined_year"]) < yr and FR.rule_mask(pp["rule"], X)[0]]
    for a in ars:
        arch_ct[a] += 1
        for pid in fired: arch_pat[a][pid] += 1
pools = {a: [pid for pid, n in arch_pat[a].items() if n >= POOL_MINSUP] for a in ARCHES}
print("winner archetype composition (Dec winners, multi-label):", dict(arch_ct))
for a in ARCHES: print(f"  {a:<13} pool {len(pools[a])} patterns")

def screen_day(sd):
    fd = FCpit[FCpit.trade_date == sd]
    if fd.empty: return pd.DataFrame()
    syms, X = Xmat(fd); yr = int(sd[:4]); n = len(syms)
    # per-archetype membership mask + pattern-lift score
    memb = {a: np.zeros(n, bool) for a in ARCHES}
    for i, sym in enumerate(syms):
        fdict = feats_at(sd, sym, fd.iloc[[i]])
        for a in archetypes_of(fdict): memb[a][i] = True
    d = pd.DataFrame({"symbol": syms}); d["sd"] = sd; d["td"] = nxt(sd)
    combscore = np.zeros(n); best_arch = np.array(["-"] * n, dtype=object)
    for a in ARCHES:
        lift = np.zeros(n)
        for pid in pools[a]:
            p = PM[pid]
            if int(p["mined_year"]) >= yr: continue
            lift += FR.rule_mask(p["rule"], X).astype(float) * (p["oos_lift"] or 0)
        lift[~memb[a]] = 0                                     # gate: only score if stock IS this archetype
        r = pd.Series(lift).rank(pct=True).values             # SCALE each archetype ranking to [0,1]
        r[~memb[a]] = 0
        better = r > combscore
        best_arch = np.where(better, a, best_arch); combscore = np.maximum(combscore, r)  # COMBINE = best-of
    d["score"] = combscore; d["arch"] = best_arch
    d = d.sort_values("score", ascending=False).reset_index(drop=True)
    d["rank"] = np.arange(1, len(d) + 1)
    return d

def run_month(name, lo, hi):
    sub = picks[(picks.trade_date >= lo) & (picks.trade_date <= hi)]
    wl = sub[sub.stock_ret_pct > 0]; wins_by_day = defaultdict(set)
    for _, p in wl.iterrows():
        sd = prev(p.trade_date)
        if sd: wins_by_day[sd].add(p.symbol)
    hit15 = hit30 = tot = 0; bask = []; outrows = []
    for sd in sorted(wins_by_day):
        d = screen_day(sd)
        if d.empty: continue
        ws = wins_by_day[sd]; rk = dict(zip(d.symbol, d["rank"]))
        for sym in ws:
            if sym in rk: tot += 1; hit15 += rk[sym] <= 15; hit30 += rk[sym] <= 30
        top = d.head(15); tdv = top.td.iloc[0]
        bask.append(np.nanmean([RET_OC.get((s, tdv), np.nan) for s in top.symbol]))
        mp = set(sub[sub.trade_date == nxt(sd)].symbol) if nxt(sd) else set()
        for _, r in top.iterrows():
            outrows.append(dict(signal_date=sd, trade_date=r.td, rank=int(r["rank"]), symbol=r.symbol,
                                archetype=r.arch, score=round(r.score, 3),
                                my_pick=("YES" if r.symbol in mp else ""),
                                was_winner=("YES" if r.symbol in ws else ""),
                                ret_oc_next=round(RET_OC.get((r.symbol, r.td), np.nan), 2)))
    print(f"\n[{name}]  winners in ranked TOP-15 {hit15}/{tot}={hit15/tot*100:.0f}% · TOP-30 {hit30/tot*100:.0f}%"
          f" · top-15 open->close 5x {np.nanmean(bask)*5:+.2f}%/day")
    return pd.DataFrame(outrows)

print("\n============ ARCHETYPE-COMBINED RANKED SCREENER ============")
decout = run_month("DEC 2024 (in-sample)", "2024-12-01", "2024-12-31")
janout = run_month("JAN 2025 (out-of-sample)", "2025-01-01", "2025-01-31")
with pd.ExcelWriter(OUT, engine="openpyxl") as xw:
    decout.to_excel(xw, "dec_top15", index=False); janout.to_excel(xw, "jan_top15", index=False)
print(f"\nsaved -> {OUT}")
