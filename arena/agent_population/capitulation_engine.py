"""CAPITULATION-BOUNCE engine (operator's actual logic). High-tier pool + conditions:
down-streak (>=2/3d), vol>1.5x 20d-avg, red_1min count, red_1hr count, red-volume-share (1-min).
Rank candidates by capitulation intensity -> top-15 -> next-day bounce, exit arm6/gb60/hard-3, 5x.
Measure: recall@15 vs operator + next-day P(up) + returns, across 8 training months. Leak-free. Read-only."""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
from collections import defaultdict
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from operator_picks_8mo import PICKS
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); ODB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}
OPS={"<=":np.less_equal,"<":np.less,">":np.greater,">=":np.greater_equal}
ARM=6.0; GB=0.60; HARD=-3.0; COST5X=0.5; SIZE=15
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
rows865=con.execute("""SELECT c.mined_year,c.rule_json FROM falcon_promoted_patterns p JOIN falcon_pattern_candidates c ON p.pattern_id=c.pattern_id""").fetchall()
PATS=[]
for my,rj in rows865:
    try: PATS.append((my,[(f,op,th) for f,op,th in json.loads(rj)]))
    except: pass
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-05-01' AND trade_date<='2025-05-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-05-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; TF={}; DAILY={}
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
    a20d=pd.Series(v).rolling(20).mean().values; a3=pd.Series(v).rolling(3).mean().values; tr=np.where(a20d>0,a3/a20d,np.nan)
    turn=pd.Series(c*v).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    down=(c<pc).astype(int); ds=np.zeros(len(c))
    for i in range(1,len(c)): ds[i]=ds[i-1]+1 if down[i] else 0
    volr=v/np.where(a20d==0,1e-9,a20d)
    idx={d:i for i,d in enumerate(g.trade_date)}
    DAILY[s]=dict(ds=ds, volr=volr, c=c, o=g.open.values.astype(float), idx=idx, n=len(c), dates=list(g.trade_date))
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i])
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}; FIDX={c:i for i,c in enumerate(FR.FEATURE_COLS)}

def onemin_red(oc, sym, day):
    b=oc.execute("SELECT bar_time,open,close,volume FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
                 (sym, day+" 09:15", day+" 15:35")).fetchall()
    if len(b)<30: return None
    o=np.array([x[1] for x in b],float); c=np.array([x[2] for x in b],float); v=np.array([x[3] for x in b],float)
    red=c<o; tv=v.sum() if v.sum()>0 else 1e-9
    red_1min=int(red.sum()); red_vol_share=v[red].sum()/tv
    # hourly: bucket by hour
    hrs=[x[0][11:13] for x in b]; import collections
    hv=collections.defaultdict(lambda:[0,0])  # [open_first,close_last] approx via first/last
    hc=collections.defaultdict(list); ho=collections.defaultdict(list)
    for i,hh in enumerate(hrs): hc[hh].append(c[i]); ho[hh].append(o[i])
    red_1hr=sum(1 for hh in hc if hc[hh][-1]<ho[hh][0])
    return red_1min, red_vol_share, red_1hr

oc=sqlite3.connect("file:"+ODB.replace("\\","/")+"?mode=ro",uri=True)
def candidates(sd):
    fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); fire=np.zeros(len(syms),np.int32)
    for (my,rule) in PATS:
        if int(my)>=yr: continue
        m=np.ones(len(syms),bool); ok=True
        for f,op,th in rule:
            idx=FIDX.get(f)
            if idx is None: ok=False; break
            col=X[:,idx]; m&=OPS[op](col,th)&~np.isnan(col)
        if not ok or not m.any(): continue
        fire+=m.astype(np.int32)
    cands=[]
    for i in range(len(syms)):
        if fire[i]<10: continue
        s=syms[i]; tf=TF.get((s,sd),(np.nan,)*5)
        if classify(tf[0],tf[1],tf[2],0,tf[3],tf[4]) not in HIGH: continue
        D=DAILY.get(s); k=D["idx"].get(sd) if D else None
        if k is None: continue
        ds=D["ds"][k]; volr=D["volr"][k]
        if ds<2 or volr<1.5: continue   # capitulation gate: down>=2d & vol>1.5x
        cands.append((s, ds, volr))
    return cands

tdays=[td for td in sorted(PICKS) if cidx.get(td) and cidx[td]-1>=0]
# recall + returns for capitulation ranking (rank by red_vol_share desc, then ds, then volr)
def build_day(td):
    sd=cal[cidx[td]-1]; cands=candidates(sd)
    if not cands: return None
    syms=[c[0] for c in cands]
    q="SELECT symbol,bar_time,open,close,volume FROM ohlc_1min WHERE bar_time>=? AND bar_time<=? AND symbol IN (%s) ORDER BY symbol,bar_time"%",".join("?"*len(syms))
    red={}; cur=None; buf=[]
    def flush(sym,buf):
        if len(buf)<30: return
        o=np.array([x[0] for x in buf],float); c=np.array([x[1] for x in buf],float); v=np.array([x[2] for x in buf],float)
        r=c<o; tv=v.sum() if v.sum()>0 else 1e-9
        red[sym]=(int(r.sum()), v[r].sum()/tv)
    for sym,t,o,c,vv in oc.execute(q,(sd+" 09:15",sd+" 15:35",*syms)).fetchall():
        if sym!=cur:
            if cur is not None: flush(cur,buf)
            cur=sym; buf=[]
        if o and c and vv: buf.append((o,c,vv))
    if cur is not None: flush(cur,buf)
    scored=[]
    for (s,ds,volr) in cands:
        if s not in red: continue
        r1m,rvs=red[s]
        cap = rvs + 0.02*r1m + 0.1*ds   # capitulation intensity score
        scored.append((s,cap,ds,volr,r1m,rvs))
    scored.sort(key=lambda x:-x[1])
    return sd, scored[:SIZE]

print("running capitulation engine over 8 training months ...", flush=True)
rec_hit=rec_tot=0; day_rets=[]; pups=[]; monthly=defaultdict(list)
for td in tdays:
    r=build_day(td)
    if r is None: continue
    sd,top=r; names=[x[0] for x in top]
    picks=set(PICKS[td])
    for s in picks:
        rec_tot+=1
        if s in names: rec_hit+=1
    # returns next day (td) intraday with exits, using DAILY open/close + capitulation P(up)
    D1=[]
    for s in names:
        Dd=DAILY.get(s); k=Dd["idx"].get(td) if Dd else None
        if k is None: continue
        e=Dd["o"][k]; c=Dd["c"][k]
        if e>0: D1.append((c/e-1)*100)  # next-day close vs open (P up proxy)
    if D1: pups.append(np.mean([1 if x>0 else 0 for x in D1])*100)
    # basket 5x with exits on 1-min next day
    q="SELECT symbol,bar_time,open,close FROM ohlc_1min WHERE bar_time>=? AND bar_time<=? AND symbol IN (%s) ORDER BY symbol,bar_time"%",".join("?"*len(names))
    permin={}; cur=None; ent=None
    for sym,t,o,c in oc.execute(q,(td+" 09:15",td+" 15:35",*names)).fetchall():
        if sym!=cur: cur=sym; ent=o; permin[sym]={}
        if ent and ent>0 and c and c>0: permin[sym][t]=(c/ent-1)
    have=[s for s in names if s in permin and permin[s]]
    if len(have)>=3:
        bsum=defaultdict(float); bcnt=defaultdict(int)
        for s in have:
            for t,rr in permin[s].items(): bsum[t]+=rr; bcnt[t]+=1
        times=sorted(bsum); p=np.array([5*100*bsum[t]/bcnt[t] for t in times if bcnt[t]>=max(3,len(have)//2)])
        if len(p):
            armed=False;peak=0.0;ex=None
            for rr in p:
                if rr<=HARD: ex=HARD;break
                if not armed and rr>=ARM: armed=True
                if armed:
                    peak=max(peak,rr)
                    if rr<=peak*(1-GB): ex=rr;break
            if ex is None: ex=p[-1]
            monthly[td[:7]].append(ex-COST5X)
oc.close()
OP={"2024-10":104.4,"2024-11":141.3,"2024-12":163.9,"2025-01":170.4,"2025-02":59.2,"2025-03":211.7,"2025-04":170.9,"2025-05":220.8}
print("="*70)
print(f"  CAPITULATION ENGINE · recall@15 {rec_hit/rec_tot*100:.0f}%  ·  avg next-day P(up) {np.mean(pups):.0f}%")
print("="*70)
print(f"  {'month':<9}{'ENGINE 5x%':>12}{'OPERATOR':>11}{'days':>6}")
te=0
for mo in sorted(monthly):
    e=sum(monthly[mo]); te+=e
    print(f"  {mo:<9}{e:>+12.1f}{OP.get(mo,np.nan):>+11.1f}{len(monthly[mo]):>6}")
print("  "+"-"*68)
print(f"  8-mo ENGINE {te:+.1f}%  ·  OPERATOR {sum(OP.values()):+.1f}%  ·  {te/sum(OP.values())*100:.0f}% of operator")
