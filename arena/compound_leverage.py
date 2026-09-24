"""Find a SURVIVABLE leverage: fixed book (ADV-cap 2% + VWAP, 80/20) at 1x/2x/3x/5x, Rs10cr.
Judge on COMPOUNDED return, WORST start-month outcome, and TRUE max drawdown. Read-only; Falcon untouched."""
import os, sqlite3, numpy as np, pandas as pd
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); RDB=os.path.join(ROOT,"data","db","falcon_research.db")
uc=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
od=pd.read_sql_query("SELECT symbol,trade_date,open,close,volume FROM ohlc_daily WHERE trade_date BETWEEN '2026-01-01' AND '2026-07-17'",uc); uc.close()
adv=od.assign(tv=od.close*od.volume).groupby("symbol").tv.median().to_dict()
op=od.pivot_table(index="trade_date",columns="symbol",values="open"); cp=od.pivot_table(index="trade_date",columns="symbol",values="close")
cal=sorted(od.trade_date.unique()); nextd={cal[i]:cal[i+1] for i in range(len(cal)-1)}
rc=sqlite3.connect("file:"+RDB.replace("\\","/")+"?mode=ro",uri=True)
rk=pd.read_sql_query("SELECT signal_date,rank,symbol FROM falcon_full_ranking WHERE signal_date BETWEEN '2025-12-31' AND '2026-07-16'",rc); rc.close()
perday={}
for sd,g in rk.groupby("signal_date"):
    ed=nextd.get(sd)
    if not ed or ed not in op.index: continue
    rows=[(int(r["rank"]),op.at[ed,r.symbol],cp.at[ed,r.symbol],adv[r.symbol]) for _,r in g.iterrows()
          if r.symbol in op.columns and op.at[ed,r.symbol]==op.at[ed,r.symbol] and cp.at[ed,r.symbol]==cp.at[ed,r.symbol] and adv.get(r.symbol,0)>0]
    rows.sort(); perday[ed]=rows
def cost_rs(tb,ts): return min(20,0.0003*tb)+min(20,0.0003*ts)+0.00025*ts+0.0000297*(tb+ts)+0.00003*tb+0.000001*(tb+ts)+0.18*(min(20,0.0003*tb)+min(20,0.0003*ts)+0.0000297*(tb+ts)+0.000001*(tb+ts))
def fill(entries,notional,sgn):
    rem=notional; g=c=0.0
    for(rk_,o,cl,a) in entries:
        if rem<=0: break
        ci=min(0.02*a,rem); qty=ci/o; tb=o*qty; ts=cl*qty
        g+=sgn*(cl-o)*qty; c+=cost_rs(tb,ts)+2*(2+800*(ci/a)*0.35)/1e4*ci; rem-=ci
    return g,c
def dret(cap,lev):
    rows=[]
    for ed,e in perday.items():
        gL,cL=fill([x for x in e if x[0]<=100],cap*0.8*lev,1); gS,cS=fill([x for x in e if x[0]>=201],cap*0.2*lev,-1)
        rows.append((ed,(gL+gS-cL-cS)/cap))
    D=pd.DataFrame(rows,columns=["d","r"]); D["ym"]=D.d.str[:7]; return D
MLBL={"2026-01":"Jan","2026-02":"Feb","2026-03":"Mar","2026-04":"Apr","2026-05":"May","2026-06":"Jun","2026-07":"Jul"}
def comp(D,ym):
    sub=D[D.ym>=ym]; eq=1.0; pk=1.0; mdd=0.0
    for r in sub.r:
        eq=max(eq*(1+r),0.0); pk=max(pk,eq); mdd=max(mdd,(pk-eq)/pk)
    return eq,mdd
CAP=1e8
print(f"{'lev':>4}{'worst day':>11}{'Jan-start':>11}{'maxDD':>8}{'WORST start-month (min end Rs100)':>36}")
for lev in [1,2,3,5]:
    D=dret(CAP,lev); wd=D.r.min()*100
    janeq,janmdd=comp(D,"2026-01")
    ends={MLBL[ym]:comp(D,ym)[0] for ym in MLBL}
    worst=min(ends,key=ends.get)
    print(f"{lev:>3}x{wd:>+10.1f}%{(janeq-1)*100:>+10.0f}%{janmdd*100:>7.0f}%     start {worst} -> Rs{ends[worst]*100:>6.0f} ({(ends[worst]-1)*100:>+4.0f}%)")
print("\nPer-start-month ENDING CAPITAL (Rs100 in), by leverage — the honest deployability test:")
print(f"{'start':<7}" + "".join(f"{'  '+f'{l}x':>9}" for l in [1,2,3,5]))
for ym in MLBL:
    row=f"{MLBL[ym]:<7}"
    for lev in [1,2,3,5]:
        eq,_=comp(dret(CAP,lev),ym); row+=f"{eq*100:>9.0f}"
    print(row)
