import numpy as np, pandas as pd
from verify_kec_sapphire_backtest import (load_market, ro_connect, build_frame, load_patterns,
    max_lag, sim_short_day, mask_for, metrics, monthly, TGT)
cfg=pd.read_csv("../reports/verify_worker_config.csv").set_index("symbol")
ref=pd.read_csv("../reports/SEALED_reference_ALL.csv").set_index("symbol")
START,END=pd.Timestamp("2025-01-01"),pd.Timestamp("2026-07-31"); COST_MIS,LEV=0.08,5.0
def ucn(O,C,i,w,n):
    g=0.0;d=0
    for x in range(i+1,min(i+1+w,n)):
        o,c=O[x],C[x]
        if not(np.isfinite(o) and o>0): break
        g+=(o-c)/o*100.0; d+=1
    return (g-d*COST_MIS)*LEV if d else np.nan
def run(con,sym,market):
    r=cfg.loc[sym]; trail=(float(r.arm),float(r.floor),float(r.giveback),float(r.hard_stop))
    pats=load_patterns(sym,str(r.pattern_file)); frame,dd=build_frame(con,sym,market,max_t=max_lag(pats))
    O=frame["_o"].values;C=frame["_c"].values;yr=frame["year"].values;n=len(frame)
    idx=frame.index;ds=np.array([d.strftime("%Y-%m-%d") for d in idx]);er=np.full(n,np.nan)
    for i,d in enumerate(ds):
        if d in dd: er[i]=sim_short_day(dd[d],trail)[0]
    seg={}
    for _,row in pats.iterrows():
        w=int(TGT[row["target"]][1]);m=mask_for(frame,row["conds"]);seg.setdefault(w,np.zeros(n,bool));seg[w]|=m
    keep={}
    for w,mask in seg.items():
        cn=np.array([ucn(O,C,i,w,n) for i in range(n)]);pre=(yr<=2024)&mask;rr=cn[pre];rr=rr[np.isfinite(rr)]
        if len(rr)>=15 and rr.mean()>0: keep[w]=(rr.mean(),mask,cn)
    if not keep: return None
    order=sorted(keep,key=lambda w:-keep[w][0]);rep=np.where((idx>=START)&(idx<=END))[0];tr=[];pos=0
    while pos<len(rep):
        i=int(rep[pos]);ch=None
        for w in order:
            if keep[w][1][i] and np.isfinite(keep[w][2][i]): ch=w;break
        if ch is None: pos+=1;continue
        for x in range(i+1,min(i+1+ch,n)):
            if START<=idx[x]<=END and np.isfinite(er[x]): tr.append({"date":idx[x],"return":float(er[x])})
        pos+=ch
    return pd.DataFrame(tr)
rows=[]
with ro_connect() as con:
    market=load_market(con)
    for s in sorted(ref.index):
        t=run(con,s,market)
        if t is None or t.empty:
            rows.append({"symbol":s,"corr_total":0,"corr_mdd":0,"corr_days":0}); print(s,"ZERO",flush=True);continue
        m=metrics(t)
        rows.append({"symbol":s,"corr_total":round(m["total"],1),"corr_mdd":round(m["acct_mdd"],1),"corr_days":m["n"]})
        print(s,round(m["total"],1),flush=True)
out=pd.DataFrame(rows).set_index("symbol").join(ref[["total_return","account_mdd"]])
out["dTot_pct"]=((out.corr_total-out.total_return)/out.total_return.abs()*100).round(1)
out.to_csv("../reports/CORRECTED_reconciliation.csv")
print("DONE")
