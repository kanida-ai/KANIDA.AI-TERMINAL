"""CONFIG #3 — ENT/PREM tier only, NO hard stop, arm/floor/giveaway TRAIL swept on the real 1-min path.
Leak-free. Entry next session 09:15 (1-min open). Max hold 7 sessions. Cost 0.30%.

Trail (walked bar-by-bar, prior-bar peak -> no intrabar lookahead):
  peak = running max high ; arm when (peak/entry-1)%>=arm ; once armed lock = max(floor, peakProfit%-give)
  exit when a 1-min low <= entry*(1+lock/100)  (gap: fill at bar open if it opened below the level)
  never armed -> hold to day-7 close (NO stop).
Sweep arm x give (floor=0, plus a floor=1 row). Report per-trade net%, win%, %armed, avg hold, and MONTHLY.
Compares to NAKED (no trail, hold-to-close) and the MFE ceiling. Read-only.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
TOPN = 20; HOLD = 7; COST = 0.30; ONEMIN_END = "2026-07-10"
ENTPREM = {"ENTERPRISE-Dryup", "PREMIUM-Pullback", "PREMIUM-Compression"}
# (label, arm, give, floor)
CONFIGS = [("arm5/give3/floor0",5,3,0),("arm5/give2/floor0",5,2,0),("arm5/give4/floor0",5,4,0),
           ("arm3/give2/floor0",3,2,0),("arm3/give3/floor0",3,3,0),("arm7/give3/floor0",7,3,0),
           ("arm7/give4/floor0",7,4,0),("arm5/give3/floor1",5,3,1),("arm4/give2/floor1",4,2,1)]


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


def basket_entprem(day):
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
        ti=classify(tf[0],tf[2],tf[1],al,tf[3],tf[4])
        if ti in ENTPREM: out.append((c["symbol"], rk, ti))
    return out


def trail_exit(O,H,L,C,dates,e,arm,give,floor):
    peak=e; armed=False
    for i in range(len(O)):
        if armed:
            lock=max(floor,(peak/e-1)*100-give); lvl=e*(1+lock/100)
            if L[i]<=lvl:
                px=O[i] if O[i]<lvl else lvl
                return (px/e-1)*100-COST, len(set(dates[:i+1])), True
        if H[i]>peak: peak=H[i]
        if not armed and (peak/e-1)*100>=arm: armed=True
    return (C[-1]/e-1)*100-COST, len(set(dates)), armed


oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
sigdays = [d for d in sorted(FCpit[(FCpit.trade_date>="2026-01-01")&(FCpit.trade_date<="2026-07-10")].trade_date.unique())
           if cidx.get(d) is not None and cidx[d]+1 < len(cal) and cal[cidx[d]+1] <= ONEMIN_END]
POS = []
for sd in sigdays:
    bk = basket_entprem(sd)
    if not bk: continue
    ci=cidx[sd]; edate=cal[ci+1]; end_date=min(cal[min(ci+HOLD,len(cal)-1)], ONEMIN_END)
    for sym, rk, tier in bk:
        b = oc.execute("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
                       (sym, edate+" 09:15", end_date+" 15:59")).fetchall()
        if len(b)<5: continue
        e=b[0][1]
        if e is None or e<=0: continue
        O=np.array([x[1] for x in b],float);H=np.array([x[2] for x in b],float);L=np.array([x[3] for x in b],float);C=np.array([x[4] for x in b],float)
        dates=[x[0][:10] for x in b]
        POS.append(dict(sym=sym, edate=edate, exit_mo=edate[:7], rk=rk, O=O,H=H,L=L,C=C,dates=dates,
                        naked=(C[-1]/e-1)*100-COST, mfe=(H.max()/e-1)*100, e=e))
oc.close()
print(f"ENT/PREM signal instances with 1-min: {len(POS)}  ·  {sigdays[0]}..{ONEMIN_END}\n")

def agg(rets, holds, armed):
    r=np.array(rets); return r.mean(), (r>0).mean()*100, np.mean(holds), np.mean(armed)*100

print("="*96)
print("  CONFIG #3  ·  ENT/PREM tier · NO hard stop · arm/give trail on 1-min · max-hold 7 · net")
print("="*96)
print(f"  {'config':<20}{'n':>5}{'mean net%':>11}{'win%':>7}{'%armed':>8}{'avgHold':>9}")
naked=[p["naked"] for p in POS]
print(f"  {'NAKED (no trail)':<20}{len(POS):>5}{np.mean(naked):>+11.3f}{(np.array(naked)>0).mean()*100:>6.0f}%{'—':>8}{np.mean([len(set(p['dates'])) for p in POS]):>9.1f}")
print(f"  {'MFE ceiling':<20}{len(POS):>5}{np.mean([p['mfe'] for p in POS]):>+11.3f}{'—':>7}{'—':>8}{'—':>9}")
print("  "+"-"*94)
results={}
for lab,arm,give,floor in CONFIGS:
    rr=[];hh=[];aa=[]
    for p in POS:
        r,hd,ar=trail_exit(p["O"],p["H"],p["L"],p["C"],p["dates"],p["e"],arm,give,floor)
        rr.append(r);hh.append(hd);aa.append(ar)
    m,w,ah,pa=agg(rr,hh,aa); results[lab]=rr
    print(f"  {lab:<20}{len(rr):>5}{m:>+11.3f}{w:>6.0f}%{pa:>7.0f}%{ah:>9.1f}")

# best config -> monthly
best=max(results, key=lambda k: np.mean(results[k]))
print("\n"+"="*96); print(f"  MONTHLY — best config: {best}   (mean net% per trade by ENTRY month)"); print("="*96)
dfb=pd.DataFrame({"mo":[p["exit_mo"] for p in POS], "ret":results[best]})
print(f"  {'month':<9}{'trades':>7}{'mean net%':>11}{'win%':>7}{'sum net%':>10}")
for mo,g in dfb.groupby("mo"):
    print(f"  {mo:<9}{len(g):>7}{g.ret.mean():>+11.2f}{(g.ret>0).mean()*100:>6.0f}%{g.ret.sum():>+10.1f}")
print(f"  {'TOTAL':<9}{len(dfb):>7}{dfb.ret.mean():>+11.2f}{(dfb.ret>0).mean()*100:>6.0f}%{dfb.ret.sum():>+10.1f}")

out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_CONFIG3_TRAIL_SWEEP.xlsx")
pd.DataFrame([dict(config=k, n=len(v), mean_net=round(float(np.mean(v)),3), win=round(float((np.array(v)>0).mean()*100),0), total=round(float(np.sum(v)),1)) for k,v in results.items()]).to_excel(out,index=False)
print(f"\nsweep table -> {out}")
