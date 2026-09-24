"""MEMORY-RECURRENCE test with the same persistence + luck framework. Hypothesis: stocks that RECUR in the
good-tier board carry edge (operator's 'memory' names, e.g. PGEL/SHYAMMETL/HUDCO). Define recurrence market-wide
(prior good-tier board appearances in the last 40 trading days), 3-5d swing, next-day-open entry, 2023..2026.
Tests: (1) dose-response (more recurrence -> more edge?), (2) recurring vs fresh by year, (3) persistence+luck
on recurring names. Also overlays the operator's own repeat names from the Dec24+Jan25 log. Leak-safe, read-only."""
import os, sqlite3, warnings
import numpy as np, pandas as pd
from scipy.stats import binom
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population")
import sys; sys.path.insert(0,AP)
from tier_no_falcon import classify_tier_no_falcon
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
GOOD={"PREMIUM-Pullback","GOLD","PREMIUM-Compression"}; PREM={"PREMIUM-Pullback","PREMIUM-Compression"}
LOOKBACK=40; RECUR_MIN=3
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2022-01-01' AND trade_date<='2026-07-27' ORDER BY symbol,trade_date",con); con.close()
adv=oh.assign(turn=oh.close*oh.volume).groupby("symbol").turn.mean(); keep=set(adv[adv>3e7].index); oh=oh[oh.symbol.isin(keep)]
# operator recurring names from the log
log=pd.read_csv(os.path.join(AP,"operator_ranked_log_dec24_jan25.tsv"),sep="\t")
opcnt=log.symbol.value_counts(); op_recurring=set(opcnt[opcnt>=3].index)
print(f"  operator repeat names (picked >=3x in Dec24+Jan25 log): {len(op_recurring)}  e.g. {', '.join(list(opcnt[opcnt>=3].index[:10]))}\n")
rows=[]
for sym,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); n=len(g)
    if n<80: continue
    o=g.open.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float); v=g.volume.values.astype(float); dts=g.trade_date.values
    pc=np.roll(c,1); pc[0]=np.nan; c2=np.roll(c,2); c2[:2]=np.nan
    sret=(c/pc-1)*100; twoday=(c/c2-1)*100; rng=(h-l)/pc*100
    trend=pd.Series(v).rolling(3).mean().values/pd.Series(v).rolling(20).mean().values
    turn=c*v; tp=pd.Series(turn).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    tiers=[classify_tier_no_falcon(sret[i],twoday[i],rng[i],trend[i],tp[i]) for i in range(n)]
    good=np.array([t in GOOD for t in tiers]); prem=np.array([t in PREM for t in tiers])
    prior_good=pd.Series(good.astype(int)).rolling(LOOKBACK).sum().shift(1).values
    for i in range(60,n-1):
        if not good[i]: continue
        ent=o[i+1]
        if ent<=0 or np.isnan(ent): continue
        r3=(c[min(i+3,n-1)]-ent)/ent*100; r5=(c[min(i+5,n-1)]-ent)/ent*100
        rows.append(dict(entry_date=dts[i+1],year=int(dts[i+1][:4]),symbol=sym,prem=bool(prem[i]),
            prior_appear=int(prior_good[i]) if not np.isnan(prior_good[i]) else 0,ret_3d=r3,ret_5d=r5))
D=pd.DataFrame(rows); D=D[(D.entry_date>="2023-01-01")&(D.entry_date<="2026-07-27")]
D["op_recur"]=D.symbol.isin(op_recurring)
def line(d): return f"n={len(d):>6}  win3={((d.ret_3d>0).mean()*100):>4.0f}%  avg3={d.ret_3d.mean():>+6.2f}%  avg5={d.ret_5d.mean():>+6.2f}%"
print("  (1) DOSE-RESPONSE — does MORE recurrence in the good-tier board = more edge? (3-5d swing)")
for lo,hi,lab in [(0,0,"fresh (0 prior in 40d)"),(1,2,"1-2 prior"),(3,5,"3-5 prior (recurring)"),(6,99,"6+ prior (strong memory)")]:
    d=D[(D.prior_appear>=lo)&(D.prior_appear<=hi)]
    if len(d): print(f"    {lab:<26}{line(d)}")
print("\n  (2) RECURRING (>=3 prior) vs FRESH (0) by YEAR:")
print(f"    {'year':<6}{'recurring n/win3/avg3':<34}{'fresh n/win3/avg3'}")
for y in [2023,2024,2025,2026]:
    r=D[(D.year==y)&(D.prior_appear>=RECUR_MIN)]; f=D[(D.year==y)&(D.prior_appear==0)]
    if len(r) or len(f):
        rs=f"{len(r)}/{(r.ret_3d>0).mean()*100:.0f}%/{r.ret_3d.mean():+.2f}%" if len(r) else "-"
        fs=f"{len(f)}/{(f.ret_3d>0).mean()*100:.0f}%/{f.ret_3d.mean():+.2f}%" if len(f) else "-"
        print(f"    {y:<6}{rs:<34}{fs}")
# (3) persistence + luck on RECURRING names
R=D[D.prior_appear>=RECUR_MIN]
g=R.groupby(["symbol","year"]).agg(n=("ret_3d","size"),win3=("ret_3d",lambda s:(s>0).mean()*100),avg3=("ret_3d","mean")).reset_index()
g["worked"]=(g.n>=3)&(g.win3>=60)&(g.avg3>1.0)
q=g[g.n>=3]; p=q["worked"].mean()
obs2=int((q.groupby("symbol")["worked"].sum()>=2).sum()); obs3=int((q.groupby("symbol")["worked"].sum()>=3).sum())
exp2=exp3=0.0
for sym,d in q.groupby("symbol"):
    k=len(d)
    if k>=2: exp2+=1-binom.cdf(1,k,p)
    if k>=3: exp3+=1-binom.cdf(2,k,p)
print(f"\n  (3) PERSISTENCE + LUCK on recurring names (worked = >=3 sig, win3>=60%, avg3>1%):")
print(f"    qualifying stock-years {len(q)} · base worked-rate p={p:.3f}")
print(f"    >=2 years worked: observed {obs2}  vs  expected-by-luck {exp2:.1f}")
print(f"    >=3 years worked: observed {obs3}  vs  expected-by-luck {exp3:.1f}")
# (4) operator's own repeat names
print(f"\n  (4) OPERATOR repeat names — their good-tier board swing edge 2023..2026:")
od=D[D.op_recur]
print(f"    {line(od)}   (vs all good-tier: {line(D)})")
oy=od.groupby("year").agg(n=("ret_3d","size"),win3=("ret_3d",lambda s:round((s>0).mean()*100)),avg3=("ret_3d","mean")).round(2)
print("    by year:");
for y,r in oy.iterrows(): print(f"      {y}: n={int(r['n'])} win3={r['win3']:.0f}% avg3={r['avg3']:+.2f}%")
out=os.path.join(os.path.expanduser("~"),"Downloads","MEMORY_RECURRENCE_PERSISTENCE.xlsx")
with pd.ExcelWriter(out) as xw:
    D.sort_values(["entry_date","symbol"]).to_excel(xw,sheet_name="all_board_appearances",index=False)
    g.sort_values(["symbol","year"]).to_excel(xw,sheet_name="recurring_by_year",index=False)
print(f"\n  detail -> {out}")
