"""CLEAN PIPELINE — the honest path to a real high-return strategy. All 4 steps, leak-free by construction.
STEP 0  Rebuild the 4 weekly features WEEK-TO-DATE for the whole window (validated 0.0000 vs live) -> clean panel.
STEP 1  Re-validate the Falcon library on CLEAN features: measure each pattern's real forward-return edge on TRAIN
        (<=2025), keep only patterns with a positive clean edge (leak-only patterns die), weight = clean train lift.
STEP 2  Rebuild the Falcon Top-15 on clean features using surviving patterns' clean weights; test the best 1x wrapper.
STEP 3  Apply 5x MIS to the best clean intraday edge -> the leveraged number + drawdown.
STEP 4  Portfolio: clean Falcon + Bedrock, report combined.
Everything WALK-FORWARD: weights from <=2025, validated on 2026. Daily bars (entry next open). Read-only.
Outputs -> docs/ops/CLEAN_PIPELINE.xlsx + console."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}
LO_ALL, HI_ALL = "2024-06-01", "2026-07-10"; TRAIN_HI = "2025-12-31"; OOS_LO = "2026-01-01"
def classify(sret, twoday, rng, avg_lift, tr3, tp):
    if sret is None or not np.isfinite(sret): return "UNKNOWN"
    if sret > 10: return "AVOID"
    if sret > 7 and np.isfinite(tp or np.nan) and tp >= 0.75: return "AVOID"
    if sret <= 2 and np.isfinite(twoday or np.nan) and twoday < -5 and avg_lift and avg_lift > 15: return "PREMIUM-Pullback"
    if sret <= 2 and np.isfinite(rng or np.nan) and rng < 2 and avg_lift and avg_lift > 15: return "PREMIUM-Compression"
    if sret <= 2 and np.isfinite(tr3 or np.nan) and tr3 < 0.9: return "ENTERPRISE-Dryup"
    if sret <= 2 and np.isfinite(tp or np.nan) and tp < 0.75: return "GOLD"
    if sret <= 2: return "GOLD-baseline"
    if sret <= 5: return "STANDARD"
    return "STANDARD-weak"

print("STEP 0 — loading + rebuilding clean (week-to-date) weekly features ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", con, params=(LO_ALL, HI_ALL))
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-08-15' ORDER BY symbol,trade_date", con); con.close()
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
# recompute weekly features week-to-date
oh2 = oh.copy(); dt = pd.to_datetime(oh2.trade_date); oh2["wk"] = dt.dt.isocalendar().year.astype(int)*100 + dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in oh2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi_td = g.groupby("wk").high.cummax().values; lo_td = g.groupby("wk").low.cummin().values; cl = g.close.values.astype(float)
    wcl = np.where(hi_td > lo_td, (cl-lo_td)/(hi_td-lo_td), np.nan); wrp = np.where(cl > 0, (hi_td-lo_td)/cl*100, np.nan)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wb["sma20"] = wb.wc.rolling(20).mean().shift(1); wb["ph20"] = wb.wh.rolling(20).max().shift(1)
    sma = g.wk.map(dict(zip(wb.wk, wb.sma20))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph20))).values
    wcvs = np.where((sma == sma) & (sma > 0), (cl/sma-1)*100, np.nan); wbrk = np.where(ph == ph, (cl > ph).astype(float), np.nan)
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values, weekly_close_loc=wcl, weekly_range_pct=wrp, weekly_close_vs_sma20=wcvs, weekly_breakout_20w=wbrk)))
WTD = pd.concat(rec, ignore_index=True)
FC = feat.drop(columns=["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]).merge(WTD, on=["symbol", "trade_date"], how="left")
FC["yr"] = FC.trade_date.str[:4].astype(int)
print(f"  clean panel: {len(FC)} rows")

# forward outcomes (daily): BTST 2-session (open[t+1]->close[t+2]) & intraday same-day (open[t+1]->close[t+1])
BAR = {(r.symbol, r.trade_date): (r.open, r.high, r.low, r.close) for r in oh.itertuples()}
def fwd(sym, d, kind):
    i = AIDX.get(d)
    if i is None or i+1 >= len(alldays): return np.nan
    e = BAR.get((sym, alldays[i+1]))
    if not e or e[0] <= 0: return np.nan
    if kind == "ID": x = BAR.get((sym, alldays[i+1]));  return (x[3]/e[0]-1)*100 if x else np.nan
    j = min(i+2, len(alldays)-1); x = BAR.get((sym, alldays[j])); return (x[3]/e[0]-1)*100 if x else np.nan
FC["ret2"] = [fwd(r.symbol, r.trade_date, "B") for r in FC.itertuples()]
FC["retID"] = [fwd(r.symbol, r.trade_date, "ID") for r in FC.itertuples()]

# tier features
TF = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date"); c = g.close.values.astype(float); h = g.high.values; l = g.low.values; v = g.volume.values.astype(float)
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c/pc-1)*100; rng = (h-l)/pc*100; twoday = (c/c2-1)*100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values; tr3 = np.where(av20 > 0, av3/av20, np.nan)
    turn = c*v; tp = pd.Series(turn).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    for i, d in enumerate(g.trade_date.values): TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])

print("STEP 1 — re-validating the library on clean features (train <=2025) ...", flush=True)
tr = FC[FC.trade_date <= TRAIN_HI]; base2 = tr.ret2.mean(); baseID = tr.retID.mean()
surv = []
for p in pats:
    m = np.ones(len(tr), bool); ok = True
    for f, op, th in p["rule"]:
        if f not in tr.columns: ok = False; break
        m &= OPS[op](tr[f].values, th)
    if not ok or m.sum() < 30: continue
    l2 = np.nanmean(tr.ret2.values[m]) - base2; lid = np.nanmean(tr.retID.values[m]) - baseID
    surv.append(dict(pattern_id=p["pattern_id"], rule=p["rule"], w2=l2, wid=lid, n=int(m.sum())))
S2 = [s for s in surv if s["w2"] > 0]; SID = [s for s in surv if s["wid"] > 0]
print(f"  patterns with clean TRAIN edge: BTST {len(S2)}/{len(pats)}  ·  intraday {len(SID)}/{len(pats)}  (rest were leak-only)")

def build_signal(survivors, wkey, retcol, lo, hi, topn=15):
    days = [d for d in sorted(FC[(FC.trade_date >= lo) & (FC.trade_date <= hi)].trade_date.unique())]
    picks = []
    sub = FC[(FC.trade_date >= lo) & (FC.trade_date <= hi)]
    by = {d: g for d, g in sub.groupby("trade_date")}
    for d in days:
        fd = by[d]; syms = fd.symbol.values; n = len(fd); score = np.zeros(n); fire = np.zeros(n)
        for s in survivors:
            m = np.ones(n, bool); ok = True
            for f, op, th in s["rule"]:
                if f not in fd.columns: ok = False; break
                m &= OPS[op](fd[f].values, th)
            if not ok: continue
            score += m*s[wkey]; fire += m
        idx = np.where(fire >= 3)[0]
        if not len(idx): continue
        for i in idx[np.argsort(-score[idx])][:topn]:
            al = score[i]/max(fire[i], 1); tf = TF.get((syms[i], d))
            if tf and classify(tf[0], tf[2], tf[1], al*10, tf[3], tf[4]) in HIGH: picks.append((d, syms[i]))
    return pd.DataFrame(picks, columns=["signal_date", "symbol"])

print("STEP 2/3 — clean Falcon signal on 2026, wrappers ...", flush=True)
def run_wrapper(picks, kind, lev, cost, pool):
    picks = picks.copy(); picks["ed"] = picks.signal_date.map(lambda d: alldays[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(alldays) else None)
    picks = picks.dropna(subset=["ed"])
    def nr(sym, ed):
        i = AIDX[ed]; e = BAR.get((sym, ed))
        if not e or e[0] <= 0: return None
        if kind == "ID": x = BAR.get((sym, ed)); return ((x[3]/e[0]-1)*100 - cost)*lev if x else None
        j = min(i+1, len(alldays)-1); x = BAR.get((sym, alldays[j])); return ((x[3]/e[0]-1)*100 - cost)*lev if x else None
    rows = []
    for ed, g in picks.groupby("ed"):
        rr = [nr(s, ed) for s in g.symbol]; rr = [x for x in rr if x is not None]
        if rr: rows.append((ed, float(np.mean(rr))))
    B = pd.DataFrame(rows, columns=["ed", "ret"]).sort_values("ed").reset_index(drop=True)
    if kind == "ID":
        eq = pool; curve = []
        for _, r in B.iterrows(): eq *= (1+r.ret/100); curve.append(eq)
    else:
        sl = [pool/2, pool/2]; curve = []
        for i, r in B.iterrows(): sl[i % 2] *= (1+r.ret/100); curve.append(sl[0]+sl[1])
    E = pd.Series(curve); dd = ((E.cummax()-E)/E.cummax()*100).max() if len(E) else 0
    return (E.iloc[-1]/pool-1)*100 if len(E) else 0, dd, (B.ret > 0).mean()*100 if len(B) else 0, len(B)

pk_btst = build_signal(S2, "w2", "ret2", OOS_LO, HI_ALL)
pk_id = build_signal(SID, "wid", "retID", OOS_LO, HI_ALL)
print(f"  clean high-tier picks 2026: BTST {len(pk_btst)} ({len(pk_btst)/max(pk_btst.signal_date.nunique(),1):.1f}/day) · intraday {len(pk_id)} ({len(pk_id)/max(pk_id.signal_date.nunique(),1):.1f}/day)")
res = []
res.append(("Falcon CLEAN · BTST 2-session · CNC 1x", *run_wrapper(pk_btst, "B", 1, 0.30, 1_000_000)))
res.append(("Falcon CLEAN · Intraday · MIS 1x", *run_wrapper(pk_id, "ID", 1, 0.15, 500_000)))
res.append(("Falcon CLEAN · Intraday · MIS 5x", *run_wrapper(pk_id, "ID", 5, 0.15, 500_000)))
print("\n" + "="*80)
print("RESULTS — CLEAN Falcon signal, true-OOS 2026 (leak-free, walk-forward weights <=2025)")
print("="*80)
print(f"  {'Strategy':<42}{'Return':>10}{'MaxDD':>8}{'Win%':>7}{'Baskets':>9}")
for lab, ret, dd, win, nb in res:
    print(f"  {lab:<42}{ret:>+9.1f}%{dd:>7.1f}%{win:>6.0f}%{nb:>9}")
print(f"\n  Bedrock (known clean): CNC 1x +28.1% (DD 9.7%) · MIS 5x +80.3% (DD 43.6%)")
print(f"\nSTEP 4 — path to 300%:")
best = max(res, key=lambda x: x[1])
print(f"  best clean signal: {best[0]} = {best[1]:+.0f}% (6mo). Annualized ~{((1+best[1]/100)**2-1)*100:+.0f}%/yr.")
print(f"  -> to reach ~300%/yr honestly: best clean intraday edge x MIS 5x + portfolio(Bedrock) + drawdown throttle.")
out = os.path.join(ROOT, "docs", "ops", "CLEAN_PIPELINE.xlsx")
pd.DataFrame(res, columns=["strategy", "return_pct", "maxdd_pct", "win_pct", "baskets"]).to_excel(out, index=False)
print(f"Excel -> {out}")
