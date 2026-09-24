"""FALCON BTST OSCILLATOR — LEAK-FREE vs faithful-production, side by side.
Uses the production pattern set (falcon_signal_replay.load_patterns, oos_lift weights) so the scoring is faithful,
but adds the WEEKLY-FEATURE LEAK FIX: patterns using weekly_* fire ONLY on the last session of the ISO week
(weekly cols are constant within a week -> otherwise Monday 'knows' Friday). Runs BOTH modes:
  FAITHFUL  = production as-is (weekly leak present)   LEAKFREE = weekly patterns gated to week-end.
Same wrapper: CNC 1x · SPLIT entry avg(9:15,9:16) · 2-session hold · -6% stop · Rs10L/2-sleeve · 0.30% cost.
Validates the FAITHFUL scorer against the real replay on 2 days. Window 2026-01-01..2026-07-10."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
COST = 0.30; STOP = 6.0; TOPN = 15; POOL = 1_000_000.0; SLEEVE = POOL/2; MINF = 10
LO, HI = "2026-01-01", "2026-07-10"
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}
def classify(sret, twoday, rng, avg_lift, trend3_20, turn_pct):
    if sret is None or not np.isfinite(sret): return "UNKNOWN"
    if sret > 10: return "AVOID"
    if sret > 7 and turn_pct is not None and np.isfinite(turn_pct) and turn_pct >= 0.75: return "AVOID"
    if sret <= 2 and twoday is not None and np.isfinite(twoday) and twoday < -5 and avg_lift and avg_lift > 15: return "PREMIUM-Pullback"
    if sret <= 2 and rng is not None and np.isfinite(rng) and rng < 2 and avg_lift and avg_lift > 15: return "PREMIUM-Compression"
    if sret <= 2 and trend3_20 is not None and np.isfinite(trend3_20) and trend3_20 < 0.9: return "ENTERPRISE-Dryup"
    if sret <= 2 and turn_pct is not None and np.isfinite(turn_pct) and turn_pct < 0.75: return "GOLD"
    if sret <= 2: return "GOLD-baseline"
    if sret <= 5: return "STANDARD"
    return "STANDARD-weak"

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
for p in pats: p["weekly"] = any(f.startswith("weekly_") for f, op, th in p["rule"])
nwk = sum(p["weekly"] for p in pats)
print(f"production patterns: {len(pats)} · weekly-feature patterns: {nwk} ({nwk/len(pats)*100:.0f}%)")
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", con, params=("2025-12-01", HI))
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-08-15' ORDER BY symbol,trade_date", con)
con.close()
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
BAR = {(r.symbol, r.trade_date): (r.open, r.high, r.low, r.close) for r in oh.itertuples()}
sd = sorted(feat.trade_date.unique()); _dt = pd.to_datetime(pd.Series(sd)); _wk = _dt.dt.isocalendar().year.astype(str)+"-"+_dt.dt.isocalendar().week.astype(str)
WE = dict(zip(sd, (_wk.values != np.roll(_wk.values, -1))))
sig_days = [d for d in sd if LO <= d <= HI]

# tier features
TF = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date"); c = g.close.values.astype(float); h = g.high.values; l = g.low.values; v = g.volume.values.astype(float)
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c/pc-1)*100; rng = (h-l)/pc*100; twoday = (c/c2-1)*100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values; tr3 = np.where(av20 > 0, av3/av20, np.nan)
    turn = c*v; tp = pd.Series(turn).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    for i, d in enumerate(g.trade_date.values): TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])

def picks_for(weekend_fix):
    P = []
    for d in sig_days:
        fd = feat[feat.trade_date == d]
        if fd.empty: continue
        syms = fd.symbol.values; n = len(fd); score = np.zeros(n); fire = np.zeros(n); we = WE.get(d, False)
        for p in pats:
            if weekend_fix and p["weekly"] and not we: continue
            m = np.ones(n, bool); ok = True
            for f, op, th in p["rule"]:
                if f not in fd.columns: ok = False; break
                m &= OPS[op](fd[f].values, th)
            if not ok: continue
            lift = float(p["oos_lift"]) if p["oos_lift"] is not None else 0.0
            score += m*lift; fire += m
        idx = np.where(fire >= MINF)[0]
        if not len(idx): continue
        order = idx[np.argsort(-score[idx])][:TOPN]
        for i in order:
            al = score[i]/max(fire[i], 1); tf = TF.get((syms[i], d))
            if tf is None: continue
            if classify(tf[0], tf[2], tf[1], al, tf[3], tf[4]) in HIGH: P.append((d, syms[i]))
    HT = pd.DataFrame(P, columns=["signal_date", "symbol"])
    HT["entry_date"] = HT.signal_date.map(lambda d: alldays[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(alldays) else None)
    return HT.dropna(subset=["entry_date"])

# validate FAITHFUL scorer vs the true replay on 2 days
con2 = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
for d in ["2026-07-08", "2026-07-09"]:
    fd = feat[feat.trade_date == d]; syms = fd.symbol.values; n = len(fd); score = np.zeros(n); fire = np.zeros(n)
    for p in pats:
        m = np.ones(n, bool); ok = True
        for f, op, th in p["rule"]:
            if f not in fd.columns: ok = False; break
            m &= OPS[op](fd[f].values, th)
        if not ok: continue
        score += m*(float(p["oos_lift"]) if p["oos_lift"] is not None else 0.0); fire += m
    idx = np.where(fire >= MINF)[0]; mine = list(syms[idx[np.argsort(-score[idx])][:10]])
    repl = [c["symbol"] for c in FR.rank_for_date(con2, pats, d, min_fires=MINF, top_n=10)]
    print(f"  validate {d}: my top-10 vs replay overlap {len(set(mine)&set(repl))}/10")
con2.close()

# collect the SPLIT-entry 1-min once for the union of names
def wrapper(HT, label):
    mc = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True); o916 = {}
    for d, g in HT[["symbol", "entry_date"]].drop_duplicates().groupby("entry_date"):
        ss = g.symbol.tolist()
        q = ("SELECT symbol,bar_time,open FROM ohlc_1min WHERE substr(bar_time,1,10)=? AND substr(bar_time,12,5) IN ('09:15','09:16') AND symbol IN (%s)" % ",".join("?"*len(ss)))
        try: b = pd.read_sql_query(q, mc, params=[d]+ss)
        except Exception: b = pd.DataFrame()
        for s, gg in b.groupby("symbol"):
            o15 = gg[gg.bar_time.str[11:16] == "09:15"].open; o16 = gg[gg.bar_time.str[11:16] == "09:16"].open
            if len(o15) and len(o16): o916[(s, d)] = 0.5*float(o15.iloc[0])+0.5*float(o16.iloc[0])
            elif len(o15): o916[(s, d)] = float(o15.iloc[0])
    mc.close()
    def nr(sym, ed):
        ep = o916.get((sym, ed)) or (BAR[(sym, ed)][0] if (sym, ed) in BAR else None)
        if not ep or ep <= 0: return None
        i = AIDX[ed]; d2 = alldays[min(i+1, len(alldays)-1)]
        for dd in [ed, d2]:
            b = BAR.get((sym, dd))
            if b and b[2] <= ep*(1-STOP/100): return -STOP-COST
        b2 = BAR.get((sym, d2)); return (b2[3]/ep-1)*100-COST if b2 else None
    rows = []
    for ed, g in HT.groupby("entry_date"):
        rr = [nr(s, ed) for s in g.symbol]; rr = [x for x in rr if x is not None]
        if rr: rows.append((ed, float(np.mean(rr)), len(rr)))
    B = pd.DataFrame(rows, columns=["entry_date", "ret", "n"]).sort_values("entry_date").reset_index(drop=True)
    sl = [SLEEVE, SLEEVE]; eq = []
    for i, r in B.iterrows(): sl[i % 2] *= (1+r.ret/100.0); eq.append((r.entry_date, sl[0]+sl[1]))
    E = pd.DataFrame(eq, columns=["date", "equity"]); E["m"] = E.date.str[:7]
    fin = E.equity.iloc[-1]; dd = ((E.equity.cummax()-E.equity)/E.equity.cummax()*100).max()
    mo = E.groupby("m").equity.last(); mo = pd.concat([pd.Series({"2026-00": POOL}), mo]).pct_change().dropna()*100
    return dict(label=label, ret=(fin/POOL-1)*100, final=fin, dd=dd, win=(B.ret > 0).mean()*100, avgb=B.ret.mean(),
                names=B.n.mean(), baskets=len(B), monthly=mo)

HTf = picks_for(False); HTl = picks_for(True)
print(f"\n  faithful high-tier picks: {len(HTf)} ({len(HTf)/HTf.signal_date.nunique():.1f}/day)  |  leak-free: {len(HTl)} ({len(HTl)/HTl.signal_date.nunique():.1f}/day)")
Rf = wrapper(HTf, "FAITHFUL (weekly leak present)"); Rl = wrapper(HTl, "LEAK-FREE (weekly=week-end only)")
print("\n" + "="*78)
print("FALCON BTST OSCILLATOR — Rs10L · CNC 1x · 2-session · 2026 (Jan1-Jul10)")
print("="*78)
print(f"  {'':<22}{'FAITHFUL (prod)':>18}{'LEAK-FREE':>16}")
for lab, k in [("Return", "ret"), ("Max drawdown", "dd"), ("Basket win%", "win"), ("Avg basket%", "avgb"), ("Names/day", "names"), ("Baskets", "baskets")]:
    fmt = (lambda v: f"{v:+.1f}%") if lab in ("Return",) else (lambda v: f"{v:.1f}%") if "%" in lab or lab == "Max drawdown" else (lambda v: f"{v:.1f}")
    print(f"  {lab:<22}{fmt(Rf[k]):>18}{fmt(Rl[k]):>16}")
print(f"  {'Rs10L becomes':<22}{('Rs%s'%format(int(Rf['final']),',')):>18}{('Rs%s'%format(int(Rl['final']),',')):>16}")
print(f"\n  MONTHLY:")
print(f"  {'Month':<9}{'FAITHFUL':>12}{'LEAK-FREE':>12}")
for m in sorted(set(Rf['monthly'].index) | set(Rl['monthly'].index)):
    print(f"  {m[-2:]:<9}{Rf['monthly'].get(m, float('nan')):>+11.1f}%{Rl['monthly'].get(m, float('nan')):>+11.1f}%")
print(f"\n  >>> weekly-leak impact: {Rf['ret']:+.1f}% (faithful) -> {Rl['ret']:+.1f}% (leak-free)   "
      f"= the leak was worth {Rf['ret']-Rl['ret']:+.1f} pp")
out = os.path.join(ROOT, "docs", "ops", "FALCON_OSCILLATOR_LEAKFREE.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    pd.DataFrame({"month": Rf['monthly'].index, "faithful": Rf['monthly'].values}).to_excel(w, "Faithful", index=False)
    pd.DataFrame({"month": Rl['monthly'].index, "leakfree": Rl['monthly'].values}).to_excel(w, "LeakFree", index=False)
print(f"Excel -> {out}")
