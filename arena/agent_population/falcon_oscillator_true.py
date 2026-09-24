"""FALCON BTST OSCILLATOR — built on the BIT-EXACT production signal (falcon_signal_replay.rank_for_date,
validated 10/10 vs falcon_signals_live). NOT a reconstruction. Production signal is already leak-safe
(eligible = mined_year < signal_year). We take the TRUE daily Top-15, tier-filter to HIGH-TIER (~9 names),
then run the wrapper exactly per spec:
  CNC 1x · SPLIT 50:50 entry = avg(09:15 open, 09:16 open) · hold 2 sessions (buy Day-1 -> sell Day-2 close)
  -6% hard stop on blended cost · Rs10L pool = 2 x Rs5L sleeves continuous roll · ~0.30% round-trip cost.
Real COMPOUNDED account. Window 2026-01-01..2026-07-10 (matches Bedrock + 1-min limit)."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
AP = os.path.dirname(os.path.abspath(__file__))
COST = 0.30; STOP = 6.0; TOPN = 15; POOL = 1_000_000.0; SLEEVE = POOL/2
LO, HI = "2026-01-01", "2026-07-10"
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
print(f"patterns loaded (production, after drawdown_bounce drop): {len(pats)}")
sig_days = [d for (d,) in con.execute("SELECT DISTINCT trade_date FROM falcon_features WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date", (LO, HI))]
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-08-15' ORDER BY symbol,trade_date", con); con.close()
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
BAR = {(r.symbol, r.trade_date): (r.open, r.high, r.low, r.close) for r in oh.itertuples()}

# tier features (backward only)
tf = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date"); c = g.close.values.astype(float); h = g.high.values; l = g.low.values; v = g.volume.values.astype(float)
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c/pc-1)*100; rng = (h-l)/pc*100; twoday = (c/c2-1)*100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    tr3 = np.where(av20 > 0, av3/av20, np.nan)
    turn = c*v; tp = pd.Series(turn).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    tf.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values, sret=sret, rng=rng, twoday=twoday, trend3_20=tr3, turn_pct=tp)))
TF = pd.concat(tf, ignore_index=True).set_index(["symbol", "trade_date"])

# --- TRUE daily Top-15 via production replay, then HIGH-TIER filter ---
con2 = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
picks = []
for d in sig_days:
    ranked = FR.rank_for_date(con2, pats, d, min_fires=10, top_n=TOPN)
    if not ranked: continue
    for c in ranked:
        sym = c["symbol"]; nf = c.get("n_fires", 0); sc = c.get("score", 0.0); al = sc/max(nf, 1)
        tfr = TF.loc[(sym, d)] if (sym, d) in TF.index else None
        if tfr is None: continue
        tier = classify(tfr.sret, tfr.twoday, tfr.rng, al, tfr.trend3_20, tfr.turn_pct)
        if tier in HIGH:
            picks.append(dict(signal_date=d, symbol=sym))
con2.close()
HT = pd.DataFrame(picks)
HT["entry_date"] = HT.signal_date.map(lambda d: alldays[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(alldays) else None)
HT = HT.dropna(subset=["entry_date"])
print(f"high-tier picks: {len(HT)} · avg {len(HT)/max(HT.signal_date.nunique(),1):.1f} names/day on {HT.signal_date.nunique()} days")

# SPLIT entry (avg 9:15 & 9:16 open)
mc = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
o916 = {}
for d, g in HT[["symbol", "entry_date"]].drop_duplicates().groupby("entry_date"):
    syms = g.symbol.tolist()
    q = ("SELECT symbol,bar_time,open FROM ohlc_1min WHERE substr(bar_time,1,10)=? AND substr(bar_time,12,5) IN ('09:15','09:16') AND symbol IN (%s)" % ",".join("?"*len(syms)))
    try: b = pd.read_sql_query(q, mc, params=[d]+syms)
    except Exception: b = pd.DataFrame()
    for s, gg in b.groupby("symbol"):
        o15 = gg[gg.bar_time.str[11:16] == "09:15"].open; o16 = gg[gg.bar_time.str[11:16] == "09:16"].open
        if len(o15) and len(o16): o916[(s, d)] = 0.5*float(o15.iloc[0]) + 0.5*float(o16.iloc[0])
        elif len(o15): o916[(s, d)] = float(o15.iloc[0])
mc.close()

def name_ret(sym, ed):
    ep = o916.get((sym, ed)) or (BAR[(sym, ed)][0] if (sym, ed) in BAR else None)
    if not ep or ep <= 0: return None
    i = AIDX[ed]; d2 = alldays[min(i+1, len(alldays)-1)]
    for dd in [ed, d2]:
        b = BAR.get((sym, dd))
        if b and b[2] <= ep*(1-STOP/100): return -STOP - COST
    b2 = BAR.get((sym, d2))
    return (b2[3]/ep - 1)*100 - COST if b2 else None

rows = []
for ed, g in HT.groupby("entry_date"):
    rets = [name_ret(s, ed) for s in g.symbol]; rets = [r for r in rets if r is not None]
    if rets: rows.append((ed, float(np.mean(rets)), len(rets)))
B = pd.DataFrame(rows, columns=["entry_date", "ret", "n"]).sort_values("entry_date").reset_index(drop=True)
sleeves = [SLEEVE, SLEEVE]; eq = []
for i, r in B.iterrows():
    sleeves[i % 2] *= (1 + r.ret/100.0); eq.append((r.entry_date, sleeves[0]+sleeves[1], r.ret, r.n))
E = pd.DataFrame(eq, columns=["date", "equity", "basket_ret", "n_names"]); E["m"] = E.date.str[:7]
final = E.equity.iloc[-1]; dd = ((E.equity.cummax()-E.equity)/E.equity.cummax()*100).max()
mo = E.groupby("m").equity.last(); mo = pd.concat([pd.Series({"2026-00": POOL}), mo]).pct_change().dropna()*100
print("\n" + "="*72)
print("FALCON BTST OSCILLATOR — TRUE production signal · CNC 1x · 2-session · Rs10L")
print("="*72)
print(f"  Rs10,00,000 -> Rs{final:,.0f}  = {(final/POOL-1)*100:+.1f}%   ({LO}..{HI})")
print(f"  baskets {len(B)} · avg {B.n.mean():.1f} names · basket win {(B.ret>0).mean()*100:.0f}% · avg basket {B.ret.mean():+.2f}% · MAX DD {dd:.1f}%")
print(f"\n  MONTHLY (compounded, net 0.30%):")
run = POOL
for m, rr in mo.items(): run *= (1+rr/100); print(f"    {m:<9}{rr:>+8.1f}%   Rs{run:>12,.0f}")
print(f"    {'TOTAL':<9}{(final/POOL-1)*100:>+8.1f}%   Rs{final:>12,.0f}")
out = os.path.join(ROOT, "docs", "ops", "FALCON_OSCILLATOR_TRUE.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    E.to_excel(w, "Daily", index=False); B.to_excel(w, "Baskets", index=False)
    pd.DataFrame({"month": mo.index, "return_pct": mo.values}).to_excel(w, "Monthly", index=False)
print(f"\nExcel -> {out}")
