"""1-MINUTE MFE/MAE excursion study for every Falcon TOP-20 signal (rank + tier attached), leak-free.
Jan 1 -> Jul 10 2026 (1-min data limit). Entry = next session 09:15 (1-min open). Forward window = 7 sessions.

Per signal instance, from the real 1-min path over the hold:
  naked_ret  = last-bar close in window / entry - 1 - cost     (pure hold, NO stop -> Config #2 at signal level)
  MFE        = max intraday (high/entry-1)   ;  MAE = min intraday (low/entry-1)
  adverse_first = MAE bar occurs BEFORE MFE bar  (a stop hits winners iff their dip precedes their run)
  giveback   = MFE - naked_ret   (peak-to-exit surrender -> designs trail giveaway)
STOP SWEEP on the same 1-min path: for stop in {none,-3..-10}, walk bars; first low<=entry*(1+s/100) -> exit
  at that level (gap: bar open if lower). Net return per stop -> shows how much each stop helps/destroys.
Aggregated by TIER and RANK bucket. Per-trade table saved for later dissection. Read-only.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")   # 1-min store
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
TOPN = 20; HOLD = 7; COST = 0.30; ONEMIN_END = "2026-07-10"
STOPS = [None, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -10.0]
GOLD = {"GOLD", "GOLD-baseline"}; ENTPREM = {"ENTERPRISE-Dryup", "PREMIUM-Pullback", "PREMIUM-Compression"}; HIGH = GOLD | ENTPREM


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


def tier_group(t): return "GOLD" if t in GOLD else ("ENT/PREM" if t in ENTPREM else ("AVOID" if t == "AVOID" else "STANDARD"))


print("loading + point-in-time features ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-07-10'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-07-31' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); v = g.volume.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close","last"), wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan), weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan), weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
    pc=np.roll(c,1);pc[0]=np.nan;c2=np.roll(c,2);c2[:2]=np.nan
    sret=(c/pc-1)*100;rng=(h-l)/pc*100;twoday=(c/c2-1)*100
    av20=pd.Series(v).rolling(20).mean().values;av3=pd.Series(v).rolling(3).mean().values;tr3=np.where(av20>0,av3/av20,np.nan)
    tp=pd.Series(c*v).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],rng[i],twoday[i],tr3[i],tp[i])
FCpit = feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d:i for i,d in enumerate(cal)}


def basket(day):
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
    cands=[{"symbol":syms[i],"n_fires":int(fire[i]),"score":float(score[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["n_fires"],1)))[:TOPN]
    out=[]
    for rk,c in enumerate(ranked,1):
        al=c["score"]/max(c["n_fires"],1); tf=TF.get((c["symbol"],day),(np.nan,)*5)
        out.append((c["symbol"], rk, classify(tf[0],tf[2],tf[1],al,tf[3],tf[4])))
    return out


oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
sigdays = [d for d in sorted(FCpit[(FCpit.trade_date>="2026-01-01")&(FCpit.trade_date<="2026-07-31")].trade_date.unique())
           if cidx.get(d) is not None and cidx[d]+1 < len(cal) and cal[cidx[d]+1] <= ONEMIN_END]
print(f"signal days with 1-min entry available: {len(sigdays)}  ({cal[cidx[sigdays[0]]+1]} .. {ONEMIN_END})", flush=True)

rows = []
for k, sd in enumerate(sigdays):
    bk = basket(sd)
    if not bk: continue
    ci = cidx[sd]; edate = cal[ci+1]
    end_i = min(ci+HOLD, len(cal)-1); end_date = min(cal[end_i], ONEMIN_END)
    for sym, rk, tier in bk:
        q = ("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? "
             "AND bar_time>=? AND bar_time<=? ORDER BY bar_time")
        b = oc.execute(q, (sym, edate+" 09:15", end_date+" 15:59")).fetchall()
        if len(b) < 5: continue
        e = b[0][1]
        if e is None or e <= 0: continue
        hi = np.array([x[2] for x in b], float); lo = np.array([x[3] for x in b], float); cl = np.array([x[4] for x in b], float)
        mfe = (np.nanmax(hi)/e - 1)*100; mae = (np.nanmin(lo)/e - 1)*100
        i_mfe = int(np.nanargmax(hi)); i_mae = int(np.nanargmin(lo))
        naked = (cl[-1]/e - 1)*100 - COST
        # stop sweep on the 1-min path
        stopres = {}
        for s in STOPS:
            if s is None: stopres["s_none"] = naked; continue
            lvl = e*(1+s/100.0); hit = np.where(lo <= lvl)[0]
            if len(hit):
                j = hit[0]; px = min(b[j][1], lvl) if b[j][1] is not None else lvl   # gap through -> bar open
                stopres[f"s_{int(-s)}"] = (px/e-1)*100 - COST
            else:
                stopres[f"s_{int(-s)}"] = naked
        rows.append(dict(signal_date=sd, entry_date=edate, symbol=sym, rank=rk, tier=tier, tgrp=tier_group(tier),
                         entry=round(e,2), mfe=round(mfe,2), mae=round(mae,2), naked_ret=round(naked,2),
                         adverse_first=int(i_mae < i_mfe), giveback=round(mfe-naked,2), nbars=len(b), **stopres))
oc.close()
R = pd.DataFrame(rows)
print(f"signal instances measured: {len(R)}\n")

# ---------------- REPORTS ----------------
def blk(title): print("="*100); print("  "+title); print("="*100)

blk("A. NAKED 7-day hold (NO STOP = Config #2) + MFE/MAE, by RANK bucket")
R["rkb"] = pd.cut(R["rank"], [0,5,10,15,20], labels=["1-5","6-10","11-15","16-20"])
print(f"  {'bucket':<8}{'n':>6}{'naked%':>9}{'win%':>7}{'MFE%':>8}{'MAE%':>8}{'givebk%':>9}{'advFirst%':>10}")
for b,g in R.groupby("rkb"):
    print(f"  {str(b):<8}{len(g):>6}{g.naked_ret.mean():>+9.2f}{(g.naked_ret>0).mean()*100:>6.0f}%{g.mfe.mean():>+8.2f}{g.mae.mean():>+8.2f}{g.giveback.mean():>+9.2f}{g.adverse_first.mean()*100:>9.0f}%")

blk("B. NAKED 7-day hold + MFE/MAE, by TIER group")
print(f"  {'tier':<10}{'n':>6}{'naked%':>9}{'win%':>7}{'MFE%':>8}{'MAE%':>8}{'givebk%':>9}{'advFirst%':>10}")
for b,g in R.groupby("tgrp"):
    print(f"  {b:<10}{len(g):>6}{g.naked_ret.mean():>+9.2f}{(g.naked_ret>0).mean()*100:>6.0f}%{g.mfe.mean():>+8.2f}{g.mae.mean():>+8.2f}{g.giveback.mean():>+9.2f}{g.adverse_first.mean()*100:>9.0f}%")

blk("C. WINNERS vs LOSERS — where does the adverse move sit? (stop-design core)")
for lab, sub in [("eventual WINNERS (naked>0)", R[R.naked_ret>0]), ("eventual LOSERS (naked<=0)", R[R.naked_ret<=0])]:
    print(f"  {lab:<28} n={len(sub):>4}  MFE {sub.mfe.mean():+.2f}%  MAE {sub.mae.mean():+.2f}%  adverse-first {sub.adverse_first.mean()*100:.0f}%")
    print(f"      MAE percentiles: p10 {sub.mae.quantile(.10):+.1f}  p25 {sub.mae.quantile(.25):+.1f}  p50 {sub.mae.quantile(.50):+.1f}")

blk("D. STOP SWEEP on the real 1-min path  (mean net % per trade + win% ; ALL Top-20 instances)")
print(f"  {'stop':<8}{'mean net%':>11}{'win%':>7}{'%stopped':>10}   note")
alln = len(R)
for s in STOPS:
    col = "s_none" if s is None else f"s_{int(-s)}"
    mn = R[col].mean(); wr = (R[col]>0).mean()*100
    if s is None: pct=0.0
    else: pct = (R[col] < R.naked_ret - 1e-6).mean()*100   # trades whose outcome changed = stopped early
    tag = "no stop (naked)" if s is None else ""
    print(f"  {(str(s)+'%') if s is not None else 'none':<8}{mn:>+11.3f}{wr:>6.0f}%{pct:>9.0f}%   {tag}")

out = os.path.join(os.path.expanduser("~"), "Downloads", "FALCON_MFE_MAE_1MIN_TOP20.xlsx")
R.drop(columns=["rkb"]).to_excel(out, index=False)
print(f"\nper-trade MFE/MAE table ({len(R)} rows, rank+tier+stop-sweep) -> {out}")
