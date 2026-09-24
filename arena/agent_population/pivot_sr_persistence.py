"""PERSISTENCE test of the High-Volume Pivot S/R next-day-BUY strategy, 3-5 day swing horizon, per YEAR
2023..2026. Goal: find stocks where the edge REPEATS across years (same stock profitable in multiple years) —
those are trustworthy per-stock strategies. A strategy need not work on all stocks; consistent on a few = win.
Leak-safe (40-40 pivot acted on only after confirmation; entry = next day open). Read-only."""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
PIVLEN=40; VOLMA=20; VOLMULT=1.2; ATRLEN=200
MIN_SIG=3; WIN_THR=60.0; AVG_THR=1.0   # "worked that year" = >=3 signals, 3d win>=60%, avg 3d>1%
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2022-01-01' AND trade_date<='2026-07-27' ORDER BY symbol,trade_date",con); con.close()
adv=oh.assign(turn=oh.close*oh.volume).groupby("symbol").turn.mean(); keep=set(adv[adv>3e7].index); oh=oh[oh.symbol.isin(keep)]
sigs=[]
for sym,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); n=len(g)
    if n<PIVLEN*2+60: continue
    o=g.open.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float); v=g.volume.values.astype(float); dts=g.trade_date.values
    tr=np.maximum(h-l,np.maximum(np.abs(h-np.roll(c,1)),np.abs(l-np.roll(c,1)))); tr[0]=h[0]-l[0]
    atr=pd.Series(tr).rolling(ATRLEN,min_periods=50).mean().values
    volsma=pd.Series(v).rolling(VOLMA).mean().values; hivol=v>volsma*VOLMULT
    cmax=pd.Series(h).rolling(PIVLEN*2+1,center=True).max().values; cmin=pd.Series(l).rolling(PIVLEN*2+1,center=True).min().values
    is_ph=(h==cmax)&hivol; is_pl=(l==cmin)&hivol
    r_top=r_bot=np.nan; r_broken=False; r_active=False; s_top=s_bot=np.nan; s_active=False
    for t in range(PIVLEN,n-1):
        j=t-PIVLEN
        if j>=0 and is_ph[j] and not np.isnan(atr[j]):
            bt=max(o[j],c[j]); r_bot=bt; r_top=bt+atr[j]; r_broken=False; r_active=True
        if j>=0 and is_pl[j] and not np.isnan(atr[j]):
            bb=min(o[j],c[j]); s_top=bb; s_bot=bb-atr[j]; s_active=True
        trig=None
        if r_active and not r_broken and c[t]>r_top: trig="ResBreakout"; r_broken=True
        elif r_active and r_broken and l[t-1]<=r_top and l[t]>r_top: trig="FlippedResRetest"
        elif s_active and c[t]>s_bot and l[t-1]<=s_top and l[t]>s_top: trig="SupportRetest"
        if trig:
            ent=o[t+1]
            if ent<=0: continue
            r3=(c[min(t+3,n-1)]-ent)/ent*100; r5=(c[min(t+5,n-1)]-ent)/ent*100
            sigs.append(dict(entry_date=dts[t+1],year=int(dts[t+1][:4]),symbol=sym,signal=trig,ret_3d_pct=r3,ret_5d_pct=r5))
S=pd.DataFrame(sigs); S=S[(S.entry_date>="2023-01-01")]
YEARS=[2023,2024,2025,2026]
print(f"  {len(S)} swing signals 2023..2026 on {S.symbol.nunique()} stocks\n")
print(f"  === strategy stability by year (3-day swing, all stocks) ===")
print(f"  {'year':<6}{'n':>7}{'3d win%':>9}{'avg 3d%':>9}{'avg 5d%':>9}")
for y in YEARS:
    d=S[S.year==y]
    if len(d): print(f"  {y:<6}{len(d):>7}{(d.ret_3d_pct>0).mean()*100:>8.0f}%{d.ret_3d_pct.mean():>8.2f}%{d.ret_5d_pct.mean():>8.2f}%")
# per stock-year
g=S.groupby(["symbol","year"]).agg(n=("signal","size"),win3=("ret_3d_pct",lambda s:(s>0).mean()*100),avg3=("ret_3d_pct","mean"),avg5=("ret_5d_pct","mean")).reset_index()
g["worked"]=(g.n>=MIN_SIG)&(g.win3>=WIN_THR)&(g.avg3>AVG_THR)
pers=g[g.worked].groupby("symbol").agg(years_worked=("year","nunique"),yrs=("year",lambda s:",".join(map(str,sorted(s)))),
    tot_sig=("n","sum"),mean_win3=("win3","mean"),mean_avg3=("avg3","mean")).reset_index()
pers=pers.sort_values(["years_worked","mean_avg3"],ascending=False)
print(f"\n  === STOCKS where the edge REPEATS (worked = >={MIN_SIG} sig, 3d-win>={WIN_THR:.0f}%, avg3d>{AVG_THR}%) ===")
print(f"  {'symbol':<13}{'#years':>7}{'which years':>16}{'tot sig':>9}{'avg win%':>10}{'avg 3d%':>9}")
for _,r in pers[pers.years_worked>=2].iterrows():
    print(f"  {r['symbol']:<13}{int(r['years_worked']):>7}{r['yrs']:>16}{int(r['tot_sig']):>9}{r['mean_win3']:>9.0f}%{r['mean_avg3']:>8.2f}%")
n3=int((pers.years_worked>=3).sum()); n2=int((pers.years_worked>=2).sum())
print(f"\n  persistent repeaters: {n2} stocks worked in >=2 years · {n3} worked in >=3 years (out of {S.symbol.nunique()} tested)")
# per-year matrix for the >=3-year repeaters
top=pers[pers.years_worked>=3].symbol.tolist()
if top:
    print(f"\n  === year-by-year avg 3d% for the >=3-year repeaters (blank = <3 signals that year) ===")
    mat=g[g.symbol.isin(top)].pivot_table(index="symbol",columns="year",values="avg3")
    cntmat=g[g.symbol.isin(top)].pivot_table(index="symbol",columns="year",values="n")
    print(f"  {'symbol':<13}"+"".join(f"{y:>10}" for y in YEARS))
    for sym in top:
        row=""
        for y in YEARS:
            a=mat.loc[sym,y] if (sym in mat.index and y in mat.columns and not pd.isna(mat.loc[sym,y])) else None
            nn=cntmat.loc[sym,y] if (sym in cntmat.index and y in cntmat.columns and not pd.isna(cntmat.loc[sym,y])) else 0
            row+=f"{(f'{a:+.1f}%({int(nn)})' if a is not None else '-'):>10}"
        print(f"  {sym:<13}{row}")
out=os.path.join(os.path.expanduser("~"),"Downloads","PIVOT_SR_PERSISTENCE_2023_2026.xlsx")
with pd.ExcelWriter(out) as xw:
    pers.to_excel(xw,sheet_name="persistent_stocks",index=False)
    g.sort_values(["symbol","year"]).to_excel(xw,sheet_name="stock_by_year",index=False)
    S.sort_values(["entry_date","symbol"]).to_excel(xw,sheet_name="all_signals",index=False)
print(f"\n  full persistence detail -> {out}")
