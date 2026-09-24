"""What ranking change brings FIVESTAR into the top bucket? For the dates Falcon ranked it low, re-rank the whole
universe under alternative metrics and a MERGE of Falcon (legacy) + my operator-shape rules (new), and report
FIVESTAR's rank under each. Leak-free (PIT). Read-only. Diagnosis only — proposes the change, doesn't alter prod.
"""
import os, sys, sqlite3, warnings, json
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
SYM = "FIVESTAR"; DATES = ["2025-01-23", "2025-01-30", "2024-12-24"]   # signal dates Falcon ranked FIVESTAR low

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con); PM = {p["pattern_id"]: p for p in pats}
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; SHP = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); v = g.volume.values.astype(float); td = g.trade_date.values; n = len(c)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=td,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan), weekly_range_pct=np.where(c > 0, (h - l) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan), weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    # shape features (only what my rules need) for target dates
    pc = np.roll(c, 1); pc[0] = np.nan
    hi20 = pd.Series(h).rolling(20).max().values; lo20 = pd.Series(l).rolling(20).min().values
    base_tight20 = (hi20 - lo20) / c * 100
    trp = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc))) / c * 100; atrp = pd.Series(trp).rolling(20).mean().values
    av20 = pd.Series(v).rolling(20).mean().values; v3_20 = pd.Series(v).rolling(3).mean().values / av20; v5_20 = pd.Series(v).rolling(5).mean().values / av20
    sma200 = pd.Series(c).rolling(200).mean().values; d_sma200 = (c / sma200 - 1) * 100
    wk = g.wk.values; ser = pd.Series(wk); grp = ser.ne(ser.shift()).cumsum().values
    wtd_pos = np.full(n, np.nan)
    for gg in np.unique(grp):
        ii = np.where(grp == gg)[0]; wh = np.maximum.accumulate(h[ii]); wl = np.minimum.accumulate(l[ii])
        wtd_pos[ii] = np.where(wh > wl, (c[ii] - wl) / (wh - wl), np.nan)
    for idx in range(n):
        if td[idx] in DATES:
            SHP[(s, td[idx])] = dict(base_tight20=base_tight20[idx], atrp=atrp[idx], v3_20=v3_20[idx], v5_20=v5_20[idx], d_sma200=d_sma200[idx], wtd_pos=wtd_pos[idx])
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")

rdf = pd.read_csv(os.path.join(ROOT, "arena", "agent_population", "operator_mined_rules.csv"))
MYRULES = [json.loads(r.rule_json) for _, r in rdf.iterrows()]
def op_hits(feats):
    if not feats: return 0
    n = 0
    for rule in MYRULES:
        ok = True
        for f, op, th in rule:
            val = feats.get(f)
            if val is None or (isinstance(val, float) and val != val): ok = False; break
            if op == "<=" and not (val <= th): ok = False; break
            if op == ">" and not (val > th): ok = False; break
        n += ok
    return n

def rank_of(sd):
    fd = FCpit[FCpit.trade_date == sd]; syms = fd.symbol.values
    X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    yr = int(sd[:4]); elig = [p for p in pats if int(p["mined_year"]) < yr]
    liftlist = [[] for _ in range(len(syms))]
    for p in elig:
        m = FR.rule_mask(p["rule"], X)
        for i in np.where(m)[0]: liftlist[i].append(p["oos_lift"] or 0)
    rows = []
    for i, s in enumerate(syms):
        lifts = sorted(liftlist[i], reverse=True); nf = len(lifts)
        if nf < 10: continue
        rows.append(dict(symbol=s, nf=nf, sum_lift=sum(lifts), avg_lift=sum(lifts) / nf,
                         top10_avg=np.mean(lifts[:10]), top5_avg=np.mean(lifts[:5]),
                         op=op_hits(SHP.get((s, sd)))))
    D = pd.DataFrame(rows)
    # legacy pool: top-100 by sum_lift (Falcon's first cut)
    D = D.sort_values("sum_lift", ascending=False).head(100).reset_index(drop=True)
    def rk(col, extra=None):
        d = D.copy()
        if extra is not None: d["_k"] = d[col].rank(pct=True) + extra * d["op"].rank(pct=True)
        else: d["_k"] = d[col]
        d = d.sort_values("_k", ascending=False).reset_index(drop=True)
        pos = d.index[d.symbol == SYM]
        return (int(pos[0]) + 1) if len(pos) else None
    return dict(nf=int(D[D.symbol == SYM].nf.iloc[0]) if (D.symbol == SYM).any() else 0,
                op=int(D[D.symbol == SYM].op.iloc[0]) if (D.symbol == SYM).any() else 0,
                avg_lift=rk("avg_lift"), sum_lift=rk("sum_lift"), top10=rk("top10_avg"), top5=rk("top5_avg"),
                merge=rk("top10_avg", extra=1.0))

TR = {"2025-01-23": ("2025-01-24", 6.18), "2025-01-30": ("2025-01-31", 5.13), "2024-12-24": ("2024-12-26", 4.47)}
print("FIVESTAR rank under each ranking metric (lower = better; top-15 = in bucket):\n")
print(f"{'signal':<12}{'next-day':>9}{'#pat':>6}{'#myrules':>9}{'avg_lift':>9}{'sum_lift':>9}{'top10avg':>9}{'top5avg':>9}{'MERGE':>7}")
for sd in DATES:
    r = rank_of(sd); nd = TR[sd][1]
    f = lambda x: (f"#{x}" if x else "out")
    print(f"{sd:<12}{nd:>+8.1f}%{r['nf']:>6}{r['op']:>9}{f(r['avg_lift']):>9}{f(r['sum_lift']):>9}{f(r['top10']):>9}{f(r['top5']):>9}{f(r['merge']):>7}")
print("\n  avg_lift = Falcon's CURRENT metric.  MERGE = rank by top10-avg-lift blended with my operator-rule hits.")
