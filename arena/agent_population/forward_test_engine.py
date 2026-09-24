"""FORWARD TEST (no leakage): lock the config tuned on Mar-May 2025, run Jun 2025 -> Jan 2026 OUT-OF-SAMPLE.
Config FROZEN: tier=ENT/PREM, selection=ATR+lowpx top-15, exit arm7/gb90/hard-3, 5x, cost 0.5%, intraday.
Leak-free (week-to-date tiers). Reports monthly 5x return, PF, win-days. Separate engine. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
from collections import defaultdict
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
ENTPREM={"ENTERPRISE-Dryup","PREMIUM-Pullback","PREMIUM-Compression"}
# FROZEN CONFIG
SIZE=15; ARM=7.0; GB=0.90; HARD=-3.0; COST5X=0.5; ONEMIN_END="2026-07-10"
def _ok(v): return v is not None and not (isinstance(v,float) and v!=v)
def classify(sret,twoday,rng,al,tr,turn):
    if _ok(sret) and sret>10: return "AVOID"
    if _ok(sret) and sret>7 and _ok(turn) and turn>=0.75: return "AVOID"
    if _ok(sret) and sret<=2 and _ok(twoday) and twoday<-5 and _ok(al) and al>15: return "PREMIUM-Pullback"
    if _ok(sret) and sret<=2 and _ok(rng) and rng<2 and _ok(al) and al>15: return "PREMIUM-Compression"
    if _ok(sret) and sret<=2 and _ok(tr) and tr<0.9: return "ENTERPRISE-Dryup"
    if _ok(sret) and sret<=2 and _ok(turn) and turn<0.75: return "GOLD"
    if _ok(sret) and sret<=2: return "GOLD-baseline"
    if _ok(sret) and sret<=5: return "STANDARD"
    return "STANDARD-weak"
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2026-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-01-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; TF={}; PX={}
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    c=g.close.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); v=g.volume.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
    pc=np.roll(c,1);pc[0]=np.nan;c2=np.roll(c,2);c2[:2]=np.nan
    sret=(c/pc-1)*100; rng=(h-l)/pc*100; twoday=(c/c2-1)*100
    a20=pd.Series(v).rolling(20).mean().values; a3=pd.Series(v).rolling(3).mean().values; tr=np.where(a20>0,a3/a20,np.nan)
    turn=pd.Series(c*v).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i]); PX[(s,d)]=c[i]
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
def pool(sd):
    fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: return pd.DataFrame()
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*p["oos_lift"]
    rows=[]
    for i in range(len(syms)):
        if fire[i]<10: continue
        al=score[i]/max(fire[i],1); tf=TF.get((syms[i],sd),(np.nan,)*5); t=classify(tf[0],tf[1],tf[2],al,tf[3],tf[4])
        if t not in ENTPREM: continue
        r=fd.iloc[i]; rows.append(dict(symbol=syms[i], price=PX.get((syms[i],sd),np.nan), atr=float(r.get("atr_20_pct",np.nan))))
    d=pd.DataFrame(rows)
    if d.empty: return d
    return d.assign(k=d.atr.rank(pct=True)+(-d.price).rank(pct=True)).sort_values("k",ascending=False).head(SIZE)
oc=sqlite3.connect("file:"+ODB.replace("\\","/")+"?mode=ro",uri=True)
fdays=[d for d in cal if "2025-06-01"<=d<="2026-01-31" and d<=ONEMIN_END]
res=[]
for td in fdays:
    ci=cidx[td]
    if ci-1<0: continue
    P=pool(cal[ci-1])
    if P.empty: continue
    syms=list(P.symbol)
    q="SELECT symbol,bar_time,open,close FROM ohlc_1min WHERE bar_time>=? AND bar_time<=? AND symbol IN (%s) ORDER BY symbol,bar_time"%",".join("?"*len(syms))
    permin={}; cur=None; ent=None
    for sym,t,o,c in oc.execute(q,(td+" 09:15",td+" 15:35",*syms)).fetchall():
        if sym!=cur: cur=sym; ent=o; permin[sym]={}
        if ent and ent>0 and c and c>0: permin[sym][t]=(c/ent-1)
    have=[s for s in syms if s in permin and permin[s]]
    if len(have)<3: continue
    bsum=defaultdict(float); bcnt=defaultdict(int)
    for s in have:
        for t,r in permin[s].items(): bsum[t]+=r; bcnt[t]+=1
    times=sorted(bsum); p=np.array([5*100*bsum[t]/bcnt[t] for t in times if bcnt[t]>=max(3,len(have)//2)])
    if len(p)==0: continue
    armed=False; peak=0.0; ex=None
    for r in p:
        if r<=HARD: ex=HARD; break
        if not armed and r>=ARM: armed=True
        if armed:
            peak=max(peak,r)
            if r<=peak*(1-GB): ex=r; break
    if ex is None: ex=p[-1]
    res.append(dict(td=td,mo=td[:7],ret5x=ex-COST5X))
oc.close()
D=pd.DataFrame(res)
print("="*82); print("  FORWARD TEST (OUT-OF-SAMPLE) · frozen config ATR+lowpx|EP-15 arm7/gb90/hard-3 · 5x"); print("  config tuned ONLY on Mar-May 2025; below is Jun-2025 -> Jan-2026, never seen"); print("="*82)
print(f"  {'month':<9}{'days':>6}{'ret5x%':>10}{'ret1x%':>9}{'winDays':>9}{'PF':>7}")
for mo in sorted(D.mo.unique()):
    g=D[D.mo==mo]; won=g[g.ret5x>0]; lost=g[g.ret5x<=0]; pf=won.ret5x.sum()/abs(lost.ret5x.sum()) if len(lost) and lost.ret5x.sum()!=0 else 9.9
    print(f"  {mo:<9}{len(g):>6}{g.ret5x.sum():>+10.1f}{g.ret5x.sum()/5:>+9.1f}{len(won):>4}/{len(g)}{pf:>7.2f}")
won=D[D.ret5x>0]; lost=D[D.ret5x<=0]; pf=won.ret5x.sum()/abs(lost.ret5x.sum())
print("  "+"-"*80)
print(f"  8-mo OOS: 5x {D.ret5x.sum():+.1f}%  (1x {D.ret5x.sum()/5:+.1f}%)  ·  win-days {len(won)}/{len(D)}  ·  PF {pf:.2f}  ·  avg {D.ret5x.mean():+.2f}%/day")
print(f"  worst day {D.ret5x.min():.1f}%  ·  in-sample (Mar-May) was +225%/3mo PF 3.75")
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_FORWARD_TEST_ENGINE.xlsx"); D.to_excel(out,index=False); print(f"\n  daily log -> {out}")
