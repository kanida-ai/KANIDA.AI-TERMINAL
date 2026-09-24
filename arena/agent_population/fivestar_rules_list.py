"""FIVESTAR only, one signal date: list EVERYTHING that fired TRUE in MY mining and EVERYTHING that fired TRUE
in FALCON, side by side, plus FIVESTAR's feature values and the actual next-day move. Plain. Read-only.
"""
import os, sys, sqlite3, warnings, json
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
SYM = "FIVESTAR"; SIG = "2025-01-23"; TRADE = "2025-01-24"

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con); PM = {p["pattern_id"]: p for p in pats}
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       "WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()
# PIT weekly for Falcon feature vector
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan), weekly_range_pct=np.where(c > 0, (h - l) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan), weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")

# ---- MY shape features for FIVESTAR on SIG ----
g = oh[oh.symbol == SYM].sort_values("trade_date").reset_index(drop=True)
c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); o = g.open.values.astype(float); v = g.volume.values.astype(float)
td = g.trade_date.values; i = list(td).index(SIG); pc = np.roll(c, 1); pc[0] = np.nan
hi20 = pd.Series(h).rolling(20).max().values; lo20 = pd.Series(l).rolling(20).min().values
F = {}
F["base_tight20"] = (hi20[i] - lo20[i]) / c[i] * 100
F["dh20"] = (c[i] / hi20[i] - 1) * 100; F["dh10"] = (c[i] / pd.Series(h).rolling(10).max().values[i] - 1) * 100
F["dh5"] = (c[i] / pd.Series(h).rolling(5).max().values[i] - 1) * 100
F["run20"] = (c[i] / c[i - 20] - 1) * 100; F["run40"] = (c[i] / c[i - 40] - 1) * 100
trp = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc))) / c * 100
F["atrp"] = pd.Series(trp).rolling(20).mean().values[i]; F["atr5v20"] = pd.Series(trp).rolling(5).mean().values[i] / F["atrp"]
av20 = pd.Series(v).rolling(20).mean().values; F["v_ratio"] = v[i] / av20[i]; F["v3_20"] = pd.Series(v).rolling(3).mean().values[i] / av20[i]; F["v5_20"] = pd.Series(v).rolling(5).mean().values[i] / av20[i]
sma200 = pd.Series(c).rolling(200).mean().values; F["d_sma200"] = (c[i] / sma200[i] - 1) * 100
sma50 = pd.Series(c).rolling(50).mean().values; F["d_sma50"] = (c[i] / sma50[i] - 1) * 100
cloc = np.where(h > l, (c - l) / (h - l), np.nan); F["cloc1"] = cloc[i]
dt2 = pd.to_datetime(g.trade_date); wk = (dt2.dt.isocalendar().year.astype(int) * 100 + dt2.dt.isocalendar().week.astype(int)).values
ser = pd.Series(wk); grp = ser.ne(ser.shift()).cumsum().values; mask = grp == grp[i]; wi = np.where(mask)[0]
wh = np.maximum.accumulate(h[wi]); wl = np.minimum.accumulate(l[wi]); pos = np.searchsorted(wi, i)
F["wtd_pos"] = (c[i] - wl[pos]) / (wh[pos] - wl[pos]) if wh[pos] > wl[pos] else np.nan
F["wtd_ret"] = (c[i] / (pc[wi[0]] if not np.isnan(pc[wi[0]]) else c[wi[0]]) - 1) * 100

# ---- MY rules that fired TRUE ----
rdf = pd.read_csv(os.path.join(ROOT, "arena", "agent_population", "operator_mined_rules.csv"))
def fires(rule):
    for f, op, th in rule:
        val = F.get(f)
        if val is None or (isinstance(val, float) and val != val): return False
        if op == "<=" and not (val <= th): return False
        if op == ">" and not (val > th): return False
    return True
myfired = [(json.loads(r.rule_json), r.rule_text, r.lift_te) for _, r in rdf.iterrows() if fires(json.loads(r.rule_json))]

# ---- FALCON patterns that fired TRUE ----
fr = FCpit[(FCpit.trade_date == SIG) & (FCpit.symbol == SYM)]
X = np.full((1, len(FR.FEATURE_COLS)), np.nan)
for j, col in enumerate(FR.FEATURE_COLS):
    if col in fr.columns: X[0, j] = pd.to_numeric(fr[col].iloc[0], errors="coerce")
yr = int(SIG[:4]); falc_fired = [p for p in pats if int(p["mined_year"]) < yr and FR.rule_mask(p["rule"], X)[0]]
def rtext(rule): return " & ".join(f"{f}{op}{round(th,2)}" for f, op, th in rule)
# actual
tr = oh[(oh.symbol == SYM) & (oh.trade_date == TRADE)].iloc[0]; oc = (tr.close - tr.open) / tr.open * 100

print(f"================ FIVESTAR — signal {SIG}  (traded {TRADE}, actual open->close {oc:+.2f}%) ================\n")
print("FIVESTAR's feature values that day:")
for k in ["base_tight20", "dh20", "dh10", "run20", "run40", "atrp", "atr5v20", "v_ratio", "v3_20", "d_sma200", "wtd_pos", "wtd_ret", "cloc1"]:
    print(f"    {k:<14} = {F[k]:+.2f}")
print(f"\n===== MY MINING — rules that said TRUE for FIVESTAR: {len(myfired)} of {len(rdf)} =====")
for rule, txt, lift in sorted(myfired, key=lambda x: -x[2]):
    print(f"    TRUE  (lift {lift:.2f}x)  {txt}")
print(f"\n===== FALCON — patterns that said TRUE for FIVESTAR: {len(falc_fired)} of {len(pats)} =====")
for p in sorted(falc_fired, key=lambda p: -(p["oos_lift"] or 0)):
    print(f"    TRUE  FALCPAT_{p['pattern_id']}  (lift {p['oos_lift']:.1f}, target {p['target']})  {rtext(p['rule'])[:78]}")
print(f"\n===== SIDE BY SIDE =====")
print(f"    MY MINING : {len(myfired)} rules TRUE  (library = {len(rdf)} rules)")
print(f"    FALCON    : {len(falc_fired)} patterns TRUE  (library = {len(pats)} patterns)")
print(f"    ACTUAL    : FIVESTAR {TRADE} = {oc:+.2f}%")
