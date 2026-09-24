"""FIVESTAR ONLY — did MY mined signal and FALCON's signal each identify FIVESTAR on the signal date, vs what
actually happened next day. Two windows: Dec-11/12/13 (move up into Dec-16) and Jan-22/23 (your Jan-24 +5.58%).
No universe backtest, no forward P&L. Leak-free (PIT). Read-only.
"""
import os, sys, sqlite3, warnings, json, importlib.util
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
SYM = "FIVESTAR"
TRADES = ["2024-12-17", "2024-12-18", "2024-12-20", "2024-12-26",
          "2025-01-27", "2025-01-28", "2025-01-29", "2025-01-30", "2025-01-31"]

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

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con); PM = {p["pattern_id"]: p for p in pats}
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
SIG = [(prev(t), t) for t in TRADES]

# ---- PIT weekly + tier inputs (all stocks) for Falcon ----
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); v = g.volume.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan), weekly_range_pct=np.where(c > 0, (h - l) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan), weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c / pc - 1) * 100; rng = (h - l) / pc * 100; twoday = (c / c2 - 1) * 100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values; tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    tp = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    for i, d in enumerate(g.trade_date.values): TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")

# ---- my SHAPE features for FIVESTAR ----
SFEAT = ["run20", "run40", "run60", "dh5", "dh10", "dh20", "dh40", "dh60", "dl20", "dl40", "d_sma50", "d_sma200",
         "slope20", "slope50", "atrp", "atr5v20", "rng_contract", "v3_20", "v5_20", "v_ratio", "cloc1", "cloc3",
         "updays5", "updays10", "downstreak", "base_tight20", "days_since_hi20", "wtd_ret", "wtd_pos", "wtd_daysup",
         "mtd_ret", "mtd_pos", "mtd_daysup"]
def shape_for(sym):
    g = oh[oh.symbol == sym].sort_values("trade_date").reset_index(drop=True)
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); o = g.open.values.astype(float); v = g.volume.values.astype(float)
    td = g.trade_date.values; n = len(c); dt = pd.to_datetime(g.trade_date); iso = dt.dt.isocalendar()
    wk = (iso.year.astype(int) * 100 + iso.week.astype(int)).values; mo = dt.dt.strftime("%Y-%m").values
    pc = np.roll(c, 1); pc[0] = np.nan; ret1 = (c / pc - 1) * 100; rr = lambda k: (pd.Series(c).pct_change(k).values) * 100
    run20, run40, run60 = rr(20), rr(40), rr(60)
    dh = lambda k: (c / pd.Series(h).rolling(k).max().values - 1) * 100; dl = lambda k: (c / pd.Series(l).rolling(k).min().values - 1) * 100
    dh5, dh10, dh20, dh40, dh60 = dh(5), dh(10), dh(20), dh(40), dh(60); dl20, dl40 = dl(20), dl(40)
    sma50 = pd.Series(c).rolling(50).mean().values; sma200 = pd.Series(c).rolling(200).mean().values; sma20 = pd.Series(c).rolling(20).mean().values
    d_sma50 = (c / sma50 - 1) * 100; d_sma200 = (c / sma200 - 1) * 100
    slope20 = (sma20 / np.roll(sma20, 5) - 1) * 100; slope20[:25] = np.nan; slope50 = (sma50 / np.roll(sma50, 5) - 1) * 100; slope50[:55] = np.nan
    tr = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc))); trp = tr / c * 100; atrp = pd.Series(trp).rolling(20).mean().values; atr5v20 = pd.Series(trp).rolling(5).mean().values / atrp
    rng_contract = pd.Series((h - l) / c * 100).rolling(3).mean().values / (pd.Series((h - l) / c * 100).rolling(15).mean().values + 1e-9)
    av20 = pd.Series(v).rolling(20).mean().values; v3_20 = pd.Series(v).rolling(3).mean().values / av20; v5_20 = pd.Series(v).rolling(5).mean().values / av20; v_ratio = v / av20
    cloc = np.where(h > l, (c - l) / (h - l), np.nan); cloc1 = cloc; cloc3 = pd.Series(cloc).rolling(3).mean().values
    updays5 = pd.Series(ret1 > 0).rolling(5).sum().values; updays10 = pd.Series(ret1 > 0).rolling(10).sum().values
    neg = (ret1 < 0).astype(int); ds = np.zeros(n)
    for i in range(n):
        k = 0
        while i - k >= 0 and neg[i - k] == 1: k += 1
        ds[i] = k
    hi20s = pd.Series(h).rolling(20).max().values; lo20s = pd.Series(l).rolling(20).min().values; base_tight20 = (hi20s - lo20s) / c * 100
    dsh = np.full(n, np.nan)
    for i in range(n):
        if i >= 19: dsh[i] = 19 - int(np.argmax(h[i - 19:i + 1]))
    def cw(key):
        r = np.full(n, np.nan); p = np.full(n, np.nan); du = np.full(n, np.nan); ser = pd.Series(key); grp = ser.ne(ser.shift()).cumsum()
        for _, idx in pd.Series(range(n)).groupby(grp.values):
            ii = idx.values; wh = np.maximum.accumulate(h[ii]); wl = np.minimum.accumulate(l[ii]); base = pc[ii[0]] if not np.isnan(pc[ii[0]]) else c[ii[0]]
            r[ii] = (c[ii] / base - 1) * 100; p[ii] = np.where(wh > wl, (c[ii] - wl) / (wh - wl), np.nan); du[ii] = np.cumsum((ret1[ii] > 0).astype(float))
        return r, p, du
    wtd_ret, wtd_pos, wtd_daysup = cw(wk); mtd_ret, mtd_pos, mtd_daysup = cw(mo)
    vals = dict(run20=run20, run40=run40, run60=run60, dh5=dh5, dh10=dh10, dh20=dh20, dh40=dh40, dh60=dh60, dl20=dl20, dl40=dl40,
                d_sma50=d_sma50, d_sma200=d_sma200, slope20=slope20, slope50=slope50, atrp=atrp, atr5v20=atr5v20, rng_contract=rng_contract,
                v3_20=v3_20, v5_20=v5_20, v_ratio=v_ratio, cloc1=cloc1, cloc3=cloc3, updays5=updays5, updays10=updays10, downstreak=ds,
                base_tight20=base_tight20, days_since_hi20=dsh, wtd_ret=wtd_ret, wtd_pos=wtd_pos, wtd_daysup=wtd_daysup, mtd_ret=mtd_ret, mtd_pos=mtd_pos, mtd_daysup=mtd_daysup)
    return {td[i]: {k: vals[k][i] for k in SFEAT} for i in range(n)}, {td[i]: (o[i], h[i], l[i], c[i]) for i in range(n)}
SHP, OHLC = shape_for(SYM)

# ---- my mined rules ----
rdf = pd.read_csv(os.path.join(ROOT, "arena", "agent_population", "operator_mined_rules.csv"))
RULES = [(json.loads(r.rule_json), float(r.lift_te), r.rule_text) for _, r in rdf.iterrows()]
def my_fired(feats):
    hits = []
    for rule, lift, txt in RULES:
        ok = True
        for f, op, th in rule:
            val = feats.get(f)
            if val is None or (isinstance(val, float) and val != val): ok = False; break
            if op == "<=" and not (val <= th): ok = False; break
            if op == ">" and not (val > th): ok = False; break
        if ok: hits.append((txt, lift))
    return hits

# ---- Falcon board rank for a date ----
def falcon(sd):
    fd = FCpit[FCpit.trade_date == sd]
    if fd.empty: return None
    syms = fd.symbol.values; X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    yr = int(sd[:4]); elig = [p for p in pats if int(p["mined_year"]) < yr]
    fire = np.zeros(len(syms), np.int32); score = np.zeros(len(syms))
    for p in elig:
        m = FR.rule_mask(p["rule"], X)
        if m.any(): fire += m.astype(np.int32); score += m.astype(np.float64) * p["oos_lift"]
    cands = [{"symbol": syms[i], "nf": int(fire[i]), "score": float(score[i])} for i in range(len(syms)) if fire[i] >= 10]
    cands.sort(key=lambda c: -c["score"]); ranked = sorted(cands[:100], key=lambda c: -(c["score"] / max(c["nf"], 1)))
    idx = {c["symbol"]: (r, c["nf"], c["score"] / max(c["nf"], 1)) for r, c in enumerate(ranked, 1)}
    fi = list(syms).index(SYM) if SYM in syms else None
    nf_all = int(fire[fi]) if fi is not None else 0
    tf = TF.get((SYM, sd), (np.nan,) * 5); tier = classify(tf[0], tf[2], tf[1], (idx[SYM][2] if SYM in idx else np.nan), tf[3], tf[4])
    return dict(nf=nf_all, rank=(idx[SYM][0] if SYM in idx else None), avg_lift=(round(idx[SYM][2], 2) if SYM in idx else None), tier=tier, cut15=(round(ranked[14]["score"] / max(ranked[14]["nf"], 1), 2) if len(ranked) >= 15 else None))

print(f"================ FIVESTAR — MY MINING vs FALCON, per signal date ================\n")
for sd, td in SIG:
    o, h, l, c = OHLC.get(td, (np.nan,) * 4); oc = (c - o) / o * 100 if o else np.nan
    prevc = OHLC.get(sd, (np.nan,) * 4)[3]
    print(f"--- signal {sd}  ->  trade {td} ---")
    print(f"  ACTUAL {td}: open {o:.1f} high {h:.1f} low {l:.1f} close {c:.1f}  | intraday open->close {oc:+.2f}%  (prev close {prevc:.1f})")
    fh = my_fired(SHP.get(sd, {})); sf = SHP.get(sd, {})
    print(f"  MY MINING (signal {sd}): {len(fh)}/{len(RULES)} shape-rules fired"
          + (f"  -> IDENTIFIED" if fh else "  -> NOT identified"))
    for txt, lift in sorted(fh, key=lambda x: -x[1])[:4]: print(f"       fired: {txt}   (OOS lift {lift:.2f}x)")
    if sf: print(f"       FIVESTAR shape: dh10 {sf['dh10']:+.1f}%  dh20 {sf['dh20']:+.1f}%  base_tight20 {sf['base_tight20']:.1f}%  run20 {sf['run20']:+.1f}%  wtd_ret {sf['wtd_ret']:+.1f}%")
    fc = falcon(sd)
    if fc:
        rk = f"#{fc['rank']}" if fc['rank'] else "OUTSIDE top-100"
        print(f"  FALCON (signal {sd}): patterns_fired {fc['nf']}  avg_lift {fc['avg_lift']}  tier {fc['tier']}  board_rank {rk}"
              + (f"  (top-15 cutoff avg_lift {fc['cut15']})" if fc['cut15'] else ""))
        print(f"       -> {'IN TOP-15' if (fc['rank'] and fc['rank']<=15) else ('RANKED but below 15' if fc['rank'] else 'NOT identified (fires<10 or outside top-100)')}")
    print()
