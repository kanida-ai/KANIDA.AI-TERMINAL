"""ENGINE v3 — push toward operator. Best selection (ENT/PREM + vol/low-price) with looser/NO trail and
ATR-weighting + concentration. Mar-May 2025, 5x, operator +603%. Separate engine, leak-free. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
from collections import defaultdict
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}; ENTPREM={"ENTERPRISE-Dryup","PREMIUM-Pullback","PREMIUM-Compression"}; COST5X=0.5
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
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-05-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-05-31' ORDER BY symbol,trade_date",con); con.close()
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
    return pd.DataFrame(rows)
oc=sqlite3.connect("file:"+ODB.replace("\\","/")+"?mode=ro",uri=True)
trade_days=[d for d in cal if "2025-03-01"<=d<="2025-05-31"]; POOLS={}; DAYCACHE={}
for td in trade_days:
    ci=cidx[td]
    if ci-1<0: continue
    P=pool(cal[ci-1])
    if P.empty: continue
    POOLS[td]=P; syms=list(P.symbol)
    q="SELECT symbol,bar_time,open,close FROM ohlc_1min WHERE bar_time>=? AND bar_time<=? AND symbol IN (%s) ORDER BY symbol,bar_time"%",".join("?"*len(syms))
    permin={}; cur=None; ent=None
    for sym,t,o,c in oc.execute(q,(td+" 09:15",td+" 15:35",*syms)).fetchall():
        if sym!=cur: cur=sym; ent=o; permin[sym]={}
        if ent and ent>0 and c and c>0: permin[sym][t]=(c/ent-1)
    DAYCACHE[td]=permin
oc.close()
def path5x(td,names,weights):
    permin=DAYCACHE.get(td,{}); have=[s for s in names if s in permin and permin[s]]
    if len(have)<3: return None
    w={s:weights.get(s,1.0) for s in have}; wsum=sum(w.values())
    bsum=defaultdict(float); bcnt=defaultdict(int); bw=defaultdict(float)
    for s in have:
        for t,r in permin[s].items(): bsum[t]+=r*w[s]; bw[t]+=w[s]; bcnt[t]+=1
    times=sorted(bsum); p=np.array([5*100*bsum[t]/bw[t] for t in times if bcnt[t]>=max(3,len(have)//2)])
    return p if len(p) else None
def apply_exit(p,arm,gb,hard,notrail=False):
    armed=False; peak=0.0
    for r in p:
        if r<=hard: return hard-COST5X
        if not notrail:
            if not armed and r>=arm: armed=True
            if armed:
                peak=max(peak,r)
                if r<=peak*(1-gb): return r-COST5X
    return p[-1]-COST5X
def sel(P,name,sz):
    d=P
    if name=="ATR+lowpx": d=d.assign(k=d.atr.rank(pct=True)+(-d.price).rank(pct=True)).sort_values("k",ascending=False)
    elif name=="hi_ATR": d=d.sort_values("atr",ascending=False)
    return d.head(sz)
def run(name,sz,arm,gb,hard,notrail,wt):
    mo=defaultdict(list); allr=[]
    for td in trade_days:
        if td not in POOLS: continue
        d=sel(POOLS[td],name,sz); names=list(d.symbol)
        weights={r.symbol:(r.atr if (wt=="atr" and _ok(r.atr)) else 1.0) for r in d.itertuples()}
        p=path5x(td,names,weights)
        if p is None: continue
        r=apply_exit(p,arm,gb,hard,notrail); mo[td[:7]].append(r); allr.append(r)
    allr=np.array(allr); won=allr[allr>0]; lost=allr[allr<=0]; pf=won.sum()/abs(lost.sum()) if len(lost) and lost.sum()!=0 else 9.9
    return dict(tot=allr.sum(),pf=pf,wd=int((allr>0).sum()),n=len(allr),mar=sum(mo['2025-03']),apr=sum(mo['2025-04']),may=sum(mo['2025-05']))
print("="*100); print("  ENGINE v3 · ENT/PREM · vol/low-px · looser/NO trail + ATR-weight · 5x · Mar-May25 · operator +603%"); print("="*100)
print(f"  {'selection':<12}{'sz':>3}{'wt':>5}{'exit':<16}{'3mo%':>9}{'PF':>6}{'winD':>7}{'Mar/Apr/May':>20}")
res=[]
for name in ["ATR+lowpx","hi_ATR"]:
    for sz in [8,10,12,15]:
        for wt in ["eq","atr"]:
            for ex in [("arm7/gb80",7,0.8,-3,False),("arm7/gb90",7,0.9,-3,False),("NOTRAIL",0,0,-3,True),("NOTRAIL-2.5",0,0,-2.5,True),("arm5/gb80",5,0.8,-3,False)]:
                r=run(name,sz,ex[1],ex[2],ex[3],ex[4],wt); res.append((name,sz,wt,ex[0],r))
res.sort(key=lambda z:-z[4]['tot'])
for name,sz,wt,exn,r in res[:16]:
    mam=f"{r['mar']:+.0f}/{r['apr']:+.0f}/{r['may']:+.0f}"
    print(f"  {name:<12}{sz:>3}{wt:>5}  {exn:<14}{r['tot']:>+9.1f}{r['pf']:>6.2f}{r['wd']:>4}/{r['n']}{mam:>20}")
b=res[0]
print(f"\n  BEST v3: {b[0]}-{b[1]} {b[2]} {b[3]}  ->  3mo 5x {b[4]['tot']:+.1f}%  PF {b[4]['pf']:.2f}  ·  {b[4]['tot']/603.4*100:.0f}% of operator")
