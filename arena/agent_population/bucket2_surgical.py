"""BUCKET-2 SURGICAL recalibration — Option 2. Touch ONLY the patterns that fire on the 15 bucket-2 picks
(rank 16-30), on a VERSIONED COPY (production untouched). INCREMENT 1 = weekly_close_loc ONLY.
Report exact rule change (current -> adjusted) for every touched pattern + per-pick arithmetic. Leak-free. Read-only DB."""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from weekly_dayofweek_fix import OPJAN
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
FEATURE=sys.argv[1] if len(sys.argv)>1 else "weekly_close_loc"   # INCREMENT feature
DELTAS={"weekly_close_loc":[0.05,0.10,0.15,0.20],"atr_20_pct":[0.1,0.2,0.3,0.5],
        "weekly_range_pct":[0.5,1.0,2.0,3.0]}.get(FEATURE,[0.05,0.10,0.15,0.20])
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
# ---- leak-free calendar week-to-date weekly features (validated baseline) ----
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
parts=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    parts.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
BASE=feat.drop(columns=WEEKLY).merge(pd.concat(parts,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
FI={c:i for i,c in enumerate(FR.FEATURE_COLS)}
def Xmat(sd):
    fd=BASE[BASE.trade_date==sd]
    if fd.empty: return None,None
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    return syms,X
def rank_all(sd, patterns):
    syms,X=Xmat(sd)
    if syms is None: return {},{},{}
    yr=int(sd[:4]); fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in patterns:
        if int(p["mined_year"])>=yr: continue
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*(p["oos_lift"] or 0)
    cands=[{"symbol":syms[i],"nf":int(fire[i]),"score":float(score[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["nf"],1)))
    rk={c["symbol"]:r for r,c in enumerate(ranked,1)}
    nf={c["symbol"]:c["nf"] for c in cands}; al={c["symbol"]:c["score"]/max(c["nf"],1) for c in cands}
    return rk,nf,al
sdmap={td:(cal[cidx[td]-1] if cidx.get(td) and cidx[td]-1>=0 else None) for td in OPJAN}
allpick=[(td,s) for td in OPJAN for s in OPJAN[td]]
# baseline ranks + bucket definitions
base={td:(rank_all(sdmap[td],pats)[0] if sdmap[td] else {}) for td in OPJAN}
matched=[(td,s) for td,s in allpick if sdmap[td] and base[td].get(s) and base[td][s]<=15]
bucket2=[(td,s) for td,s in allpick if sdmap[td] and 16<=(base[td].get(s) or 99)<=30]
# ---- identify the SURGICAL set: patterns that fire on ANY bucket-2 pick ----
target_ids=set()
for td,s in bucket2:
    sd=sdmap[td]; fd=BASE[(BASE.trade_date==sd)&(BASE.symbol==s)]
    if fd.empty: continue
    X=np.full((1,len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[0,j]=pd.to_numeric(fd[col].iloc[0],errors="coerce")
    yr=int(sd[:4])
    for p in pats:
        if int(p["mined_year"])>=yr: continue
        if FR.rule_mask(p["rule"],X)[0]: target_ids.add(p["pattern_id"])
n_wc=sum(1 for p in pats if p["pattern_id"] in target_ids and any(f==FEATURE and op in(">",">=") for f,op,_ in p["rule"]))
print(f"  surgical set: {len(target_ids)} patterns fire on the {len(bucket2)} bucket-2 picks; {n_wc} of them gate on {FEATURE}> ")
print(f"  baseline: matched(top15)={len(matched)}  bucket2(16-30)={len(bucket2)}\n")
def rulestr(r): return " & ".join(f"{f}{op}{round(th,3)}" for f,op,th in r)
def adjust(patterns, ids, feature, delta):
    out=[]; changes=[]
    for p in patterns:
        if p["pattern_id"] in ids:
            nr=[]; ch=False
            for f,op,th in p["rule"]:
                if f==feature and op in (">",">="): nr.append((f,op,round(th-delta,4))); ch=True
                else: nr.append((f,op,th))
            if ch: q=dict(p); q["rule"]=nr; out.append(q); changes.append((p["pattern_id"],p["oos_lift"],rulestr(p["rule"]),rulestr(nr)))
            else: out.append(p)
        else: out.append(p)
    return out, changes
print(f"  {FEATURE+' relax':<24}{'net top15':>10}{'bkt2->15':>10}{'kept57':>9}")
detail_delta=DELTAS[1]; detail=None
for delta in DELTAS:
    adj,_=adjust(pats,target_ids,FEATURE,delta)
    rk={td:(rank_all(sdmap[td],adj)[0] if sdmap[td] else {}) for td in OPJAN}
    net=sum(1 for td,s in allpick if sdmap[td] and rk[td].get(s) and rk[td][s]<=15)
    prom=sum(1 for td,s in bucket2 if rk[td].get(s) and rk[td][s]<=15)
    kept=sum(1 for td,s in matched if rk[td].get(s) and rk[td][s]<=15)
    print(f"  -{delta:<23.2f}{net:>10}{prom:>7}/{len(bucket2):<2}{kept:>6}/{len(matched)}")
    if delta==detail_delta: detail=rk
# ---- per-pick arithmetic at detail delta, with fires/avg_lift before vs after ----
adj,changes=adjust(pats,target_ids,FEATURE,detail_delta)
print(f"\n  --- per bucket-2 pick at weekly_close_loc -{detail_delta} (base -> adjusted) ---")
print(f"  {'symbol':<12}{'rank':>10}{'n_fires':>14}{'avg_lift':>16}")
for td,s in sorted(bucket2):
    sd=sdmap[td]
    _,nf0,al0=rank_all(sd,pats); _,nf1,al1=rank_all(sd,adj)
    r0=base[td].get(s); r1=detail[td].get(s)
    print(f"  {s:<12}{str(r0)+'->'+str(r1 if r1 else '--'):>10}{f'{nf0.get(s,0)}->{nf1.get(s,0)}':>14}{f'{al0.get(s,0):.2f}->{al1.get(s,0):.2f}':>16}")
# ---- write versioned copy + rule-diff sheet (track every change) ----
cp={"increment":FEATURE,"delta":detail_delta,"source":"production (unmodified)","note":"copy only; production patterns untouched",
    "adjusted_patterns":[{"pattern_id":pid,"oos_lift":ol,"current_rule":cur,"adjusted_rule":new} for pid,ol,cur,new in changes]}
cpath=os.path.join(AP,f"falcon_patterns_bucket2_{FEATURE}_v1.json")
with open(cpath,"w") as f: json.dump(cp,f,indent=2)
diff=pd.DataFrame([{"pattern":f"FALCPAT_{pid}","oos_lift":ol,"current_rule":cur,"adjusted_rule":new} for pid,ol,cur,new in changes]).sort_values("oos_lift",ascending=False)
xout=os.path.join(os.path.expanduser("~"),"Downloads",f"BUCKET2_{FEATURE.upper()}_RULE_CHANGES.xlsx")
diff.to_excel(xout,index=False)
print(f"\n  touched {len(changes)} patterns' {FEATURE} threshold (copy: {os.path.basename(cpath)})")
print(f"  rule-diff sheet -> {xout}")
print(f"\n  sample rule changes (current -> adjusted):")
for pid,ol,cur,new in sorted(changes,key=lambda x:-x[1])[:6]:
    print(f"    FALCPAT_{pid} (lift {ol}):\n        {cur}\n     -> {new}")
