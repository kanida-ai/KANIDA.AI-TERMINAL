"""JAN 2026 · Top-20 by rank (NO tier filter) · basket return held to D1..D7 + basket-level MFE/MAE.
Leak-free rebuild. Entry = next session 09:15 (=daily open). Equal-weight 20 names.
Dk return = mean over names of (close[Dk]/entry - 1)*100  (GROSS; net = gross - 0.30 round-trip).
Basket MFE/MAE = max/min over the 7-session hold of the BASKET's per-minute value
  (mean across names of close_t/entry-1) -> the true portfolio excursion, not avg of single-stock extremes.
Read-only. Jan is fully inside the 1-min window (ends 2026-07-10)."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
from collections import defaultdict
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
TOPN = 20; HOLD = 7; COST = 0.30

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-02-28' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []; DC = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values; cl = g.close.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close","last"), wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan), weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan), weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
    DC[s] = dict(o=g.open.values.astype(float), c=cl, idx={d:i for i,d in enumerate(g.trade_date)}, dates=list(g.trade_date), n=len(g))
FCpit = feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d:i for i,d in enumerate(cal)}

def top20(day):
    fd = FCpit[FCpit.trade_date==day]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(day[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*p["oos_lift"]
    cands=[{"symbol":syms[i],"score":float(score[i]),"nf":int(fire[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["nf"],1)))[:TOPN]
    return [c["symbol"] for c in ranked]

oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
sigdays = [d for d in sorted(FCpit[(FCpit.trade_date>="2026-01-01")&(FCpit.trade_date<="2026-01-31")].trade_date.unique())
           if cidx.get(d) is not None and cidx[d]+HOLD < len(cal)]
rows = []
for sd in sigdays:
    names = top20(sd)
    if not names: continue
    ci = cidx[sd]; d1 = cal[ci+1]; endd = cal[ci+HOLD]
    # daily D1..D7 basket returns
    dk = {k: [] for k in range(1, HOLD+1)}
    entries = {}
    for s in names:
        S = DC.get(s); i = S["idx"].get(d1) if S else None
        if i is None or i+HOLD-1 >= S["n"]: continue
        e = S["o"][i]
        if e <= 0: continue
        entries[s] = e
        for k in range(1, HOLD+1): dk[k].append((S["c"][i+k-1]/e-1)*100)
    if not entries: continue
    # basket-level MFE/MAE from 1-min: per-minute mean of (close/entry-1) across names
    bsum = defaultdict(float); bcnt = defaultdict(int)
    for s, e in entries.items():
        b = oc.execute("SELECT bar_time,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
                       (s, d1+" 09:15", endd+" 15:59")).fetchall()
        for t, c in b:
            if c and c > 0: bsum[t] += (c/e-1)*100; bcnt[t] += 1
    path = np.array([bsum[t]/bcnt[t] for t in sorted(bsum) if bcnt[t] >= max(3, len(entries)//2)])
    mfe = float(path.max()) if len(path) else np.nan; mae = float(path.min()) if len(path) else np.nan
    row = dict(signal_date=sd, entry_date=d1, n=len(entries),
               **{f"D{k}": round(float(np.mean(dk[k])), 2) for k in range(1, HOLD+1)},
               MFE=round(mfe, 2), MAE=round(mae, 2))
    rows.append(row)
oc.close()
R = pd.DataFrame(rows)

print("="*118)
print("  JAN 2026 · Falcon TOP-20 by rank (no tier) · equal-weight basket · GROSS % (net = −0.30)")
print("  Dk = exit whole basket at close of session k   ·   MFE/MAE = basket's own peak/trough over the 7-session hold")
print("="*118)
cols = [f"D{k}" for k in range(1, HOLD+1)]
print(f"  {'signal':<11}{'entry':<11}{'n':>3}" + "".join(f"{c:>7}" for c in cols) + f"{'MFE':>8}{'MAE':>8}")
for _, x in R.iterrows():
    print(f"  {x.signal_date:<11}{x.entry_date:<11}{int(x.n):>3}" + "".join(f"{x[c]:>+7.2f}" for c in cols) + f"{x.MFE:>+8.2f}{x.MAE:>+8.2f}")
print("  " + "-"*116)
print(f"  {'AVG':<25}{'':>3}" + "".join(f"{R[c].mean():>+7.2f}" for c in cols) + f"{R.MFE.mean():>+8.2f}{R.MAE.mean():>+8.2f}")
print(f"  {'% days positive':<25}{'':>3}" + "".join(f"{(R[c]>0).mean()*100:>6.0f}%" for c in cols))
best = max(cols, key=lambda c: R[c].mean())
print(f"\n  >> best hold day (avg basket gross): {best} = {R[best].mean():+.2f}%  (net {R[best].mean()-COST:+.2f}%)")
print(f"  >> avg basket MFE {R.MFE.mean():+.2f}%  ·  avg basket MAE {R.MAE.mean():+.2f}%  ·  avg heat-to-reward {abs(R.MAE.mean()/max(R.MFE.mean(),0.01)):.2f}")
out = os.path.join(os.path.expanduser("~"), "Downloads", "FALCON_JAN2026_TOP20_BASKET_PATH.xlsx")
R.to_excel(out, index=False); print(f"\n  -> {out}")
