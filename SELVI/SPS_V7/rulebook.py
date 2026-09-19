"""
Reproduce the SELECTION rulebook (point-in-time) and validate against the real picks.
All features computed from PAST data only; daily features used as-of prior close (shift 1),
weekly features as-of the prior COMPLETED week. A stock is selected on day t if it matches
ANY rule using data available before day t's 09:15 entry.

Stage 1 (this script): compute features, evaluate rules, report daily match counts and
OVERLAP with the traders' actual picks (the validation that the logic + point-in-time is right).

Run: PYTHONIOENCODING=utf-8 python rulebook.py
"""
import sqlite3, re
from pathlib import Path
import numpy as np, pandas as pd

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
LOG = r"C:\Users\SPS\Downloads\Tradelog_data.xlsx"
RULES = Path(__file__).resolve().parent / "rules.txt"

con = sqlite3.connect(str(DB))
lab = pd.read_sql("SELECT symbol,sector,instrument_type FROM instrument_labels", con)
d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily", con)
mk = pd.read_sql("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50'", con)
con.close()
d["date"] = pd.to_datetime(d["bar_time"])
piv = lambda c: d.pivot_table(index="date", columns="symbol", values=c).sort_index()
O, H, L, C, V = [piv(x) for x in ["open", "high", "low", "close", "volume"]]
mk["date"] = pd.to_datetime(mk["bar_time"]); MK = mk.set_index("date")["close"].reindex(C.index).ffill()
sector = dict(zip(lab.symbol, lab.sector))

# ---------- daily features ----------
pc = C.shift(1)
tr = pd.concat([H - L, (H - pc).abs(), (L - pc).abs()]).groupby(level=0).max()
tr = pd.concat([(H - L), (H - pc).abs(), (L - pc).abs()], axis=0)  # placeholder
TR = np.maximum(H - L, np.maximum((H - pc).abs(), (L - pc).abs()))
atr20 = TR.rolling(20).mean(); atr5 = TR.rolling(5).mean()
F = {}
F["atr_20_pct"] = atr20 / C * 100
F["atr_5_vs_20"] = atr5 / atr20
F["close_loc"] = (C - L) / (H - L).replace(0, np.nan)
F["vol_vs_20d"] = V / V.rolling(20).mean()
for n in (5, 20, 60):
    F[f"roc_{n}"] = (C / C.shift(n) - 1) * 100
for n in (10, 20, 60, 120, 252):
    F[f"dist_high_{n}"] = (C / H.rolling(n).max() - 1) * 100
for n in (20, 50, 200):
    sma = C.rolling(n).mean(); F[f"dist_sma_{n}"] = (C / sma - 1) * 100
for n in (20, 50):
    sma = C.rolling(n).mean(); F[f"slope_sma_{n}"] = (sma / sma.shift(5) - 1) * 100
# relative strength vs market & sector
for n in (20, 60):
    stock_roc = (C / C.shift(n) - 1) * 100
    mkt_roc = (MK / MK.shift(n) - 1) * 100
    F[f"rs_market_{n}d"] = stock_roc.sub(mkt_roc, axis=0)
    # sector roc = mean roc of sector peers
    secroc = pd.DataFrame(index=C.index, columns=C.columns, dtype=float)
    bysec = {}
    for s in C.columns:
        bysec.setdefault(sector.get(s, "NA"), []).append(s)
    for sec, members in bysec.items():
        m = stock_roc[members].mean(axis=1)
        for s in members:
            secroc[s] = m
    F[f"rs_sector_{n}d"] = stock_roc - secroc
# consolidation counts
rng = (H - L) / pc * 100
F["n_sub_3_range_7d"] = (rng < 3).rolling(7).sum()
F["n_sub_2_5_range_7d"] = (rng < 2.5).rolling(7).sum()
lowvol = (V < 0.75 * V.rolling(20).mean())
F["n_sub_75v_7d"] = lowvol.rolling(7).sum()
F["n_sub_75v_20d"] = lowvol.rolling(20).sum()

# ---------- weekly features (prior completed week) ----------
wk = d.pivot_table(index="date", columns="symbol", values="close").sort_index()
Wc = C.resample("W-FRI").last(); Wh = H.resample("W-FRI").max(); Wl = L.resample("W-FRI").min()
wcloc = (Wc - Wl) / (Wh - Wl).replace(0, np.nan)
wrange = (Wh - Wl) / Wl * 100
wsma20 = Wc.rolling(20).mean(); wcvs = (Wc / wsma20 - 1) * 100
def wk2daily(wf):
    return wf.shift(1).reindex(C.index, method="ffill")     # prior completed week, ffilled to daily
F["weekly_close_loc"] = wk2daily(wcloc)
F["weekly_range_pct"] = wk2daily(wrange)
F["weekly_close_vs_sma20"] = wk2daily(wcvs)

# all daily features used as-of prior close (shift 1); weekly already prior-week
DAILY = {"atr_20_pct","atr_5_vs_20","close_loc","vol_vs_20d","roc_5","roc_20","roc_60",
         "dist_high_10","dist_high_20","dist_high_60","dist_high_120","dist_high_252",
         "dist_sma_20","dist_sma_50","dist_sma_200","slope_sma_20","slope_sma_50",
         "rs_market_20d","rs_market_60d","rs_sector_20d","rs_sector_60d",
         "n_sub_3_range_7d","n_sub_2_5_range_7d","n_sub_75v_7d","n_sub_75v_20d"}
Ff = {k: (v.shift(1) if k in DAILY else v) for k, v in F.items()}

# ---------- parse & evaluate rules ----------
rules = [r.strip() for r in RULES.read_text().splitlines() if r.strip()]
def cond_mask(cond):
    m = re.match(r"([a-z0-9_]+)\s*(<=|>=|<|>)\s*(-?\d+\.?\d*)", cond.strip())
    f, op, val = m.group(1), m.group(2), float(m.group(3))
    x = Ff[f]
    return {"<=": x <= val, ">=": x >= val, "<": x < val, ">": x > val}[op]
selected = pd.DataFrame(False, index=C.index, columns=C.columns)
for r in rules:
    mm = None
    for cond in r.split("&"):
        c = cond_mask(cond); mm = c if mm is None else (mm & c)
    selected = selected | mm.fillna(False)

# ---------- validate vs actual picks ----------
t = pd.read_excel(LOG, sheet_name="F_T15_Trades", header=0); t["trade_date"] = pd.to_datetime(t["trade_date"])
actual = {dt: set(g["symbol"]) for dt, g in t.groupby("trade_date")}
match_cnt, ov_all = [], []
prec_num = prec_den = rec_num = rec_den = 0
for dt in sorted(actual):
    if dt not in selected.index: continue
    sel = set(selected.columns[selected.loc[dt].fillna(False).values])
    act = actual[dt] & set(C.columns)
    match_cnt.append(len(sel))
    if act:
        inter = len(sel & act)
        prec_num += inter; prec_den += len(sel) if sel else 0
        rec_num += inter; rec_den += len(act)
        ov_all.append(inter / len(act))
print(f"rules: {len(rules)} · features: {len(F)}")
print(f"avg stocks matched/day by rulebook: {np.mean(match_cnt):.1f} (median {np.median(match_cnt):.0f})")
print(f"OVERLAP with actual picks: recall {rec_num/max(rec_den,1)*100:.1f}% (of their picks matched a rule)"
      f" · precision {prec_num/max(prec_den,1)*100:.1f}% (of my matches were their picks)")
print(f"avg per-day recall: {np.mean(ov_all)*100:.1f}%")

# ---------- STAGE 2: does the candidate pool carry edge? test rankings ----------
print("\n--- pool edge test (open->EOD, buy 9:15 open, trade-log period) ---")
dts=[dt for dt in selected.index if "2024-05-01"<=dt.strftime("%Y-%m-%d")<="2026-07-31"]
oc=(C-O)/O*100   # open->EOD stock return %
def perf(rankfeat=None, asc=False, topn=15):
    day=[]
    for dt in dts:
        pool=[s for s in selected.columns if selected.loc[dt,s]]
        pool=[s for s in pool if not pd.isna(oc.loc[dt,s]) and not pd.isna(O.loc[dt,s])]
        if not pool: continue
        if rankfeat is not None:
            sc=Ff[rankfeat].loc[dt][pool].dropna()
            pick=list(sc.sort_values(ascending=asc).head(topn).index)
        else:
            pick=pool
        r=oc.loc[dt][pick].mean()
        if not pd.isna(r): day.append(r)
    a=np.array(day); return a.mean(), (a>0).mean()*100, len(a)
print(f"  {'ranking':28}{'avg ret/day%':>13}{'win-day%':>10}")
for lbl,rf,asc in [("ALL pool (~244)",None,False),("top15 atr_20_pct",'atr_20_pct',False),
   ("top15 weekly_range_pct",'weekly_range_pct',False),("top15 weekly_close_loc",'weekly_close_loc',False),
   ("top15 rs_market_20d",'rs_market_20d',False),("top15 vol_vs_20d",'vol_vs_20d',False),
   ("bot15 roc_5(pullback)",'roc_5',True),("top15 rs_sector_20d",'rs_sector_20d',False),
   ("bot15 dist_high_20",'dist_high_20',True)]:
    m,w,n=perf(rf,asc); print(f"  {lbl:28}{m:>12.3f}{w:>10.0f}")

# ---------- STAGE 3: test USER hypothesis (pullback + dry-up ranking) ----------
print("\n--- STAGE 3: pullback/dry-up ranking (user hypothesis) ---")
U130=set(x.strip() for x in (Path(__file__).resolve().parents[1]/"SPS_V3"/"universe_raw.txt").read_text().split())
ret1=((C/C.shift(1)-1)*100).shift(1); ret2=((C/C.shift(2)-1)*100).shift(1)
vdry=Ff['vol_vs_20d']; atr=Ff['atr_20_pct']
def perf2(universe, filt, rankf, asc, topn=15):
    day=[]
    for dt in dts:
        pool=[s for s in selected.columns if selected.loc[dt,s] and (universe is None or s in universe)]
        pool=[s for s in pool if not pd.isna(oc.loc[dt,s]) and not pd.isna(O.loc[dt,s])]
        if filt: pool=[s for s in pool if filt(dt,s)]
        if not pool: continue
        if rankf is not None:
            sc=rankf.loc[dt][pool].dropna(); pick=list(sc.sort_values(ascending=asc).head(topn).index)
        else: pick=pool[:topn]
        r=oc.loc[dt][pick].mean()
        if not pd.isna(r): day.append(r)
    a=np.array(day); return (a.mean(), (a>0).mean()*100, len(a)) if len(a) else (0,0,0)
tests=[
 ("full pool: pullback(twoday asc)",None,None,ret2,True),
 ("full pool: drying(vol<0.9)+pullback",None,lambda dt,s: vdry.loc[dt,s]<0.9,ret2,True),
 ("full pool: ret1<2 & vol<0.75, atr desc",None,lambda dt,s: (ret1.loc[dt,s]<2) and (vdry.loc[dt,s]<0.75),atr,False),
 ("U130: pullback(twoday asc)",U130,None,ret2,True),
 ("U130: drying(vol<0.9)+pullback",U130,lambda dt,s: vdry.loc[dt,s]<0.9,ret2,True),
 ("U130: ret1<2 & vol<0.75, atr desc",U130,lambda dt,s: (ret1.loc[dt,s]<2) and (vdry.loc[dt,s]<0.75),atr,False),
 ("U130: all pool (no rank)",U130,None,None,False),
 ("U130 ONLY (ignore rulebook)",U130,None,ret2,True),
]
print(f"  {'strategy':40}{'avg ret/day%':>13}{'win%':>7}{'n':>6}")
for lbl,u,fl,rf,asc in tests:
    m,w,n=perf2(u,fl,rf,asc); print(f"  {lbl:40}{m:>12.3f}{w:>7.0f}{n:>6}")
print("  (their actual picks: +1.2%/day)")
