"""FALCON BTST OSCILLATOR — rebuilt HONESTLY from scratch (no existing results reused).
Reconstructs the Falcon Top-15 daily signal (rank stocks by SUM of lift_pp of firing promoted patterns), LEAK-FREE
(weekly-feature patterns fire only on the last session of the ISO week; in 2026 every pattern is post-mining = OOS),
tier-classifies with the Falcon rulebook, keeps HIGH-TIER only (~9 names). Then the wrapper EXACTLY per spec:
  CNC 1x (no leverage) · SPLIT 50:50 entry = avg(09:15 open, 09:16 open) · hold 2 sessions (buy Day-1 -> sell Day-2 close)
  -6% hard stop on blended cost · Rs10L pool = 2 x Rs5L sleeves, continuous daily roll · ~0.30% round-trip cost.
Real COMPOUNDED account (not sum-of-daily). Window 2026-01-01..2026-07-10 (matches Bedrock + 1-min data limit)."""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
AP = os.path.dirname(os.path.abspath(__file__))
COST = 0.30; STOP = 6.0; TOPN = 15; POOL = 1_000_000.0; SLEEVE = POOL/2
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

print("loading ...", flush=True)
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
tax = pd.read_sql_query("SELECT pattern_id,mined_year,lift_pp,rule_json FROM falcon_pattern_taxonomy", uc)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", uc, params=(LO, HI))
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<=? ORDER BY symbol,trade_date", uc, params=("2026-08-15",)); uc.close()
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
# week-end flag for the leak fix
sd = sorted(feat.trade_date.unique()); _dt = pd.to_datetime(pd.Series(sd)); _wk = _dt.dt.isocalendar().year.astype(str)+"-"+_dt.dt.isocalendar().week.astype(str)
WE = dict(zip(sd, (_wk.values != np.roll(_wk.values, -1)))); feat["_we"] = feat.trade_date.map(WE)

# --- 1) reconstruct Falcon score = sum of lift of firing patterns (leak-free) ---
print(f"  scoring {len(tax)} patterns over {len(feat)} rows ...", flush=True)
score = np.zeros(len(feat)); nfire = np.zeros(len(feat)); wearr = feat._we.values
for a in tax.itertuples():
    try: rule = json.loads(a.rule_json)
    except Exception: continue
    m = np.ones(len(feat), bool)
    ok = True
    for f, op, thr in rule:
        if f not in feat.columns: ok = False; break
        m &= OPS[op](feat[f].values, thr)
    if not ok: continue
    if any(f.startswith("weekly_") for f, _, _ in rule): m &= wearr           # weekly patterns fire week-end only
    score[m] += float(a.lift_pp) if a.lift_pp else 0.0; nfire[m] += 1
S = feat[["trade_date", "symbol"]].copy(); S["score"] = score; S["nfire"] = nfire
S["avg_lift"] = np.where(S.nfire > 0, S.score/S.nfire, np.nan)
S = S[S.nfire >= 10]                     # production min_fires=10 gate (signal_runner.py) — was the missing filter
# --- 2) rank per day by SUM-of-lift score, take Top-15 (production selection) ---
S["rank"] = S.groupby("trade_date").score.rank(method="first", ascending=False)
T15 = S[S["rank"] <= TOPN].copy()
print(f"  Top-{TOPN} rows: {len(T15)} across {T15.trade_date.nunique()} days")

# --- 3) tier features (backward windows only) + classify ---
tf = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date"); c = g.close.values.astype(float); h = g.high.values; l = g.low.values
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c/pc-1)*100; rng = (h-l)/pc*100; twoday = (c/c2-1)*100
    turn = c * 0  # turnover proxy unavailable w/o volume here; approximate turn_pct from range rank
    tf.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values, sret=sret, rng=rng, twoday=twoday)))
TF = pd.concat(tf, ignore_index=True)
# volume-based trend3_20 & turn_pct
vv = pd.read_sql_query("SELECT symbol,trade_date,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<=?",
                       sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro", uri=True), params=("2026-08-15",))
tv = []
for s, g in vv.groupby("symbol", sort=False):
    g = g.sort_values("trade_date"); v = g.volume.values.astype(float); c = g.close.values.astype(float)
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    tr3 = np.where(av20 > 0, av3/av20, np.nan)
    turn = c*v; tp = pd.Series(turn).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    tv.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values, trend3_20=tr3, turn_pct=tp)))
TV = pd.concat(tv, ignore_index=True)
T15 = T15.merge(TF, on=["symbol", "trade_date"], how="left").merge(TV, on=["symbol", "trade_date"], how="left")
T15["tier"] = [classify(r.sret, r.twoday, r.rng, r.avg_lift, r.trend3_20, r.turn_pct) for r in T15.itertuples()]
T15["high"] = T15.tier.isin(HIGH)
HT = T15[T15.high].copy()
print(f"  high-tier rows: {len(HT)} · avg {len(HT)/max(HT.trade_date.nunique(),1):.1f} names/day on {HT.trade_date.nunique()} days")

# --- 4) SPLIT entry (avg 9:15 & 9:16 open) for the high-tier names ---
BAR = {(r.symbol, r.trade_date): (r.open, r.high, r.low, r.close) for r in oh.itertuples()}
HT["entry_date"] = HT.trade_date.map(lambda d: alldays[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(alldays) else None)
HT = HT.dropna(subset=["entry_date"])
pairs = HT[["symbol", "entry_date"]].drop_duplicates()
mc = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
o916 = {}
for d, g in pairs.groupby("entry_date"):
    syms = g.symbol.tolist()
    q = ("SELECT symbol,bar_time,open FROM ohlc_1min WHERE substr(bar_time,1,10)=? AND substr(bar_time,12,5) IN ('09:15','09:16') AND symbol IN (%s)"
         % ",".join("?"*len(syms)))
    try: b = pd.read_sql_query(q, mc, params=[d]+syms)
    except Exception: b = pd.DataFrame()
    for s, gg in b.groupby("symbol"):
        o15 = gg[gg.bar_time.str[11:16] == "09:15"].open; o16 = gg[gg.bar_time.str[11:16] == "09:16"].open
        if len(o15) and len(o16): o916[(s, d)] = 0.5*float(o15.iloc[0]) + 0.5*float(o16.iloc[0])
        elif len(o15): o916[(s, d)] = float(o15.iloc[0])
mc.close()

def name_ret(sym, ed):
    """2-session CNC return: SPLIT entry -> Day-2 close, -6% hard stop, net cost."""
    ep = o916.get((sym, ed)) or (BAR[(sym, ed)][0] if (sym, ed) in BAR else None)
    if not ep or ep <= 0: return None
    i = AIDX[ed]; d2 = alldays[min(i+1, len(alldays)-1)]
    # -6% stop check across Day-1 and Day-2 lows
    for dd in [ed, d2]:
        b = BAR.get((sym, dd))
        if b and b[2] <= ep*(1-STOP/100): return -STOP - COST
    b2 = BAR.get((sym, d2))
    if not b2: return None
    return (b2[3]/ep - 1)*100 - COST

# --- 5) daily high-tier basket return, then 2-sleeve compounded Rs10L account ---
rows = []
for ed, g in HT.groupby("entry_date"):
    rets = [name_ret(s, ed) for s in g.symbol]
    rets = [r for r in rets if r is not None]
    if rets: rows.append((ed, np.mean(rets), len(rets)))
B = pd.DataFrame(rows, columns=["entry_date", "ret", "n"]).sort_values("entry_date").reset_index(drop=True)
sleeves = [SLEEVE, SLEEVE]; eq_curve = []
for i, r in B.iterrows():
    sl = i % 2; sleeves[sl] *= (1 + r.ret/100.0)
    eq_curve.append((r.entry_date, sleeves[0]+sleeves[1], r.ret, r.n))
E = pd.DataFrame(eq_curve, columns=["date", "equity", "basket_ret", "n_names"])
E["m"] = E.date.str[:7]
final = E.equity.iloc[-1]; dd = ((E.equity.cummax()-E.equity)/E.equity.cummax()*100).max()
mo = E.groupby("m").equity.last(); mo = pd.concat([pd.Series({"2026-00": POOL}), mo]).pct_change().dropna()*100
win = (B.ret > 0).mean()*100

print("\n" + "="*70)
print("FALCON BTST OSCILLATOR — HONEST REBUILD (Top-15 high-tier, CNC 1x, 2-session)")
print("="*70)
print(f"  Rs10,00,000 -> Rs{final:,.0f}   = {(final/POOL-1)*100:+.1f}%   over {LO}..{HI}")
print(f"  baskets {len(B)} · avg {B.n.mean():.1f} names · basket win-rate {win:.0f}% · avg basket {B.ret.mean():+.2f}% · MAX DD {dd:.1f}%")
print(f"\n  MONTHLY (Rs10L, compounded, net 0.30%):")
print(f"  {'Month':<9}{'Return':>10}{'End Equity':>16}")
run = POOL
for m, rr in mo.items():
    run = run*(1+rr/100); print(f"  {m:<9}{rr:>+9.1f}%{run:>16,.0f}")
print(f"  {'TOTAL':<9}{(final/POOL-1)*100:>+9.1f}%{final:>16,.0f}")
out = os.path.join(ROOT, "docs", "ops", "FALCON_OSCILLATOR_HONEST.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    E.to_excel(w, "Daily", index=False); B.to_excel(w, "Baskets", index=False)
    pd.DataFrame({"month": mo.index, "return_pct": mo.values}).to_excel(w, "Monthly", index=False)
print(f"\nExcel -> {out}")
