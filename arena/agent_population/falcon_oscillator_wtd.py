"""FALCON BTST OSCILLATOR — PROPER leak-free = weekly features recomputed WEEK-TO-DATE (as the LIVE cron builds them),
NOT by disabling patterns. Confirms: old backfilled falcon_features had CONSTANT weekly_* across each week (leak);
live-era is week-to-date (clean). We rebuild the 4 weekly features from daily OHLC using the extractor's exact formulas
(universe_engine/engine/falcon_features.py), keep EVERY pattern active every day, then re-run the true production scorer.
Validates the recompute against known-clean live-era stored values. Window 2026-01-01..2026-07-10."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
COST = 0.30; STOP = 6.0; TOPN = 15; POOL = 1_000_000.0; SLEEVE = POOL/2; MINF = 10
LO, HI = "2026-01-01", "2026-07-10"
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}
def classify(sret, twoday, rng, avg_lift, trend3_20, turn_pct):
    if sret is None or not np.isfinite(sret): return "UNKNOWN"
    if sret > 10: return "AVOID"
    if sret > 7 and np.isfinite(turn_pct or np.nan) and turn_pct >= 0.75: return "AVOID"
    if sret <= 2 and np.isfinite(twoday or np.nan) and twoday < -5 and avg_lift and avg_lift > 15: return "PREMIUM-Pullback"
    if sret <= 2 and np.isfinite(rng or np.nan) and rng < 2 and avg_lift and avg_lift > 15: return "PREMIUM-Compression"
    if sret <= 2 and np.isfinite(trend3_20 or np.nan) and trend3_20 < 0.9: return "ENTERPRISE-Dryup"
    if sret <= 2 and np.isfinite(turn_pct or np.nan) and turn_pct < 0.75: return "GOLD"
    if sret <= 2: return "GOLD-baseline"
    if sret <= 5: return "STANDARD"
    return "STANDARD-weak"

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", con, params=("2025-11-01", HI))
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-08-15' ORDER BY symbol,trade_date", con)
con.close()

# ---- recompute the 4 weekly features WEEK-TO-DATE from daily OHLC (extractor formulas) ----
oh = oh.copy(); dt = pd.to_datetime(oh.trade_date)
oh["wk"] = dt.dt.isocalendar().year.astype(int)*100 + dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi_td = g.groupby("wk").high.cummax().values; lo_td = g.groupby("wk").low.cummin().values; cl = g.close.values.astype(float)
    wcl = np.where(hi_td > lo_td, (cl-lo_td)/(hi_td-lo_td), np.nan)
    wrp = np.where(cl > 0, (hi_td-lo_td)/cl*100, np.nan)
    # completed weekly bars (for sma20 / breakout): one row per week = last close, max high
    wk_bar = g.groupby("wk").agg(wclose=("close", "last"), whigh=("high", "max")).reset_index()
    wk_bar["sma20"] = wk_bar.wclose.rolling(20).mean().shift(1)                 # prior 20 completed weeks
    wk_bar["prior_high20"] = wk_bar.whigh.rolling(20).max().shift(1)
    wk_map_sma = dict(zip(wk_bar.wk, wk_bar.sma20)); wk_map_ph = dict(zip(wk_bar.wk, wk_bar.prior_high20))
    sma = g.wk.map(wk_map_sma).values; ph = g.wk.map(wk_map_ph).values
    wcvs = np.where((sma == sma) & (sma > 0), (cl/sma-1)*100, np.nan)
    wbrk = np.where(ph == ph, (cl > ph).astype(float), np.nan)
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
                                 weekly_close_loc=wcl, weekly_range_pct=wrp, weekly_close_vs_sma20=wcvs, weekly_breakout_20w=wbrk)))
WTD = pd.concat(rec, ignore_index=True)

# validate against known-clean LIVE-era stored values (2026-07-06..10) — should match closely
chk = feat[(feat.symbol == "RELIANCE") & (feat.trade_date.between("2026-07-06", "2026-07-10"))][["trade_date", "weekly_close_loc"]].merge(
      WTD[WTD.symbol == "RELIANCE"], on="trade_date")
print("VALIDATION — my week-to-date recompute vs stored live-era (RELIANCE 2026-07):")
for _, r in chk.iterrows():
    print(f"  {r.trade_date}: stored {r.weekly_close_loc_x:.4f}  recompute {r.weekly_close_loc_y:.4f}  diff {abs(r.weekly_close_loc_x-r.weekly_close_loc_y):.4f}")

# substitute corrected weekly features into the panel
feat = feat.drop(columns=["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]).merge(
       WTD, on=["symbol", "trade_date"], how="left")

# tier features
TF = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date"); c = g.close.values.astype(float); h = g.high.values; l = g.low.values; v = g.volume.values.astype(float)
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c/pc-1)*100; rng = (h-l)/pc*100; twoday = (c/c2-1)*100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values; tr3 = np.where(av20 > 0, av3/av20, np.nan)
    turn = c*v; tp = pd.Series(turn).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    for i, d in enumerate(g.trade_date.values): TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])
BAR = {(r.symbol, r.trade_date): (r.open, r.high, r.low, r.close) for r in oh.itertuples()}
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
sig_days = [d for d in sorted(feat.trade_date.unique()) if LO <= d <= HI]

# ---- run production scorer on the CORRECTED (week-to-date) features, all patterns active daily ----
def top_picks():
    P = []
    for d in sig_days:
        fd = feat[feat.trade_date == d]
        if fd.empty: continue
        syms = fd.symbol.values; n = len(fd); score = np.zeros(n); fire = np.zeros(n)
        for p in pats:
            m = np.ones(n, bool); ok = True
            for f, op, th in p["rule"]:
                if f not in fd.columns: ok = False; break
                m &= OPS[op](fd[f].values, th)
            if not ok: continue
            score += m*(float(p["oos_lift"]) if p["oos_lift"] is not None else 0.0); fire += m
        idx = np.where(fire >= MINF)[0]
        if not len(idx): continue
        for i in idx[np.argsort(-score[idx])][:TOPN]:
            al = score[i]/max(fire[i], 1); tf = TF.get((syms[i], d))
            if tf is None: continue
            if classify(tf[0], tf[2], tf[1], al, tf[3], tf[4]) in HIGH: P.append((d, syms[i]))
    HT = pd.DataFrame(P, columns=["signal_date", "symbol"])
    HT["entry_date"] = HT.signal_date.map(lambda d: alldays[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(alldays) else None)
    return HT.dropna(subset=["entry_date"])
HT = top_picks()
print(f"\n(week-to-date) high-tier picks: {len(HT)} · {len(HT)/HT.signal_date.nunique():.1f}/day on {HT.signal_date.nunique()} days")

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
final = E.equity.iloc[-1]; dd = ((E.equity.cummax()-E.equity)/E.equity.cummax()*100).max()
mo = E.groupby("m").equity.last(); mo = pd.concat([pd.Series({"2026-00": POOL}), mo]).pct_change().dropna()*100
print("\n" + "="*66)
print("FALCON BTST OSCILLATOR — PROPER LEAK-FREE (weekly features week-to-date)")
print("="*66)
print(f"  Rs10,00,000 -> Rs{final:,.0f}  = {(final/POOL-1)*100:+.1f}%   maxDD {dd:.1f}%  win {(B.ret>0).mean()*100:.0f}%  avg basket {B.ret.mean():+.2f}%")
print(f"\n  MONTHLY:")
run = POOL
for m, rr in mo.items(): run *= (1+rr/100); print(f"    {m:<9}{rr:>+8.1f}%   Rs{run:>12,.0f}")
print(f"\n  vs faithful (leaky backfill) +245.7%  |  vs my earlier crude 'disable' -25.5%")
