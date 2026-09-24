"""Rank high-tier pool by ALL-865-pattern confluence (as the operator does) and test recall@15 vs operator
picks on the 8 training months. Compare 865 vs my 684 set, and ranking keys (score / n_fires / avg_lift / maxlift).
Leak-free. Read-only."""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from operator_picks_8mo import PICKS
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}
OPS={"<=":np.less_equal,"<":np.less,">":np.greater,">=":np.greater_equal}
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
# ALL 865 promoted patterns with rules (no classification filter, no family drop)
rows865=con.execute("""SELECT c.pattern_id,c.mined_year,c.rule_json,p.avg_oos_year_lift_pp
    FROM falcon_promoted_patterns p JOIN falcon_pattern_candidates c ON p.pattern_id=c.pattern_id""").fetchall()
P865=[]
for pid,my,rj,lift in rows865:
    try: rule=[(f,op,th) for f,op,th in json.loads(rj)]
    except: continue
    P865.append({"pattern_id":pid,"mined_year":my,"rule":rule,"oos_lift":lift if lift is not None else 0.0})
P684=FR.load_patterns(con)
print(f"pattern sets: ALL={len(P865)}  ·  FR/684={len(P684)}")
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-05-01' AND trade_date<='2025-05-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-05-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; TF={}
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
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i])
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
FIDX={c:i for i,c in enumerate(FR.FEATURE_COLS)}

def rank_day(sd, patset, key):
    fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms)); maxl=np.zeros(len(syms))
    for p in patset:
        if int(p["mined_year"])>=yr: continue
        m=np.ones(len(syms),bool); ok=True
        for f,op,th in p["rule"]:
            idx=FIDX.get(f)
            if idx is None: ok=False; break
            col=X[:,idx]; m&=OPS[op](col,th)&~np.isnan(col)
        if not ok or not m.any(): continue
        L=float(p["oos_lift"]); fire+=m.astype(np.int32); score+=m.astype(np.float64)*L; maxl=np.where(m&(L>maxl),L,maxl)
    # high-tier filter
    out=[]
    for i in range(len(syms)):
        if fire[i]<10: continue
        al=score[i]/max(fire[i],1); tf=TF.get((syms[i],sd),(np.nan,)*5)
        if classify(tf[0],tf[1],tf[2],al,tf[3],tf[4]) not in HIGH: continue
        out.append((syms[i], score[i], fire[i], al, maxl[i]))
    kf={"score":1,"nf":2,"avgl":3,"maxl":4}[key]
    out.sort(key=lambda x:-x[kf])
    return [x[0] for x in out[:15]]

tdays=[td for td in sorted(PICKS) if cidx.get(td) and cidx[td]-1>=0]
def recall(patset,key):
    hit=tot=0
    for td in tdays:
        top=set(rank_day(cal[cidx[td]-1], patset, key)); picks=PICKS[td]
        for s in picks:
            tot+=1
            if s in top: hit+=1
    return hit/tot*100

print("\n  recall@15 vs operator (8 training months):")
print(f"  {'pattern set':<10}{'key':<8}{'recall@15':>10}")
for setname,ps in [("684",P684),("865",P865)]:
    for key in ["score","nf","avgl"]:
        print(f"  {setname:<10}{key:<8}{recall(ps,key):>9.0f}%")
