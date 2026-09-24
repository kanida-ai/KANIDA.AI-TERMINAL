"""High-Volume Pivot S/R Zones [BigBeluga] ported to a NEXT-DAY BUY backtest (daily bars).
Bullish triggers: (1) Resistance Breakout = close > high-volume 40-bar resistance zone top;
(2) Support Hold Retest = dip into high-volume support zone and bounce; (3) Flipped-Res Retest.
Leak-safe: a 40-40 pivot is confirmed only 40 bars later, and we act only after confirmation; entry = NEXT
day's open; exits at next-day EOD / +3d / +5d close. Reports per-signal + WHICH STOCKS work confidently. Read-only."""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
PIVLEN=40; VOLMA=20; VOLMULT=1.2; ATRLEN=200
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-01-01' AND trade_date<='2024-12-31' ORDER BY symbol,trade_date",con); con.close()
# liquidity filter: keep reasonably traded names
adv=oh.assign(turn=oh.close*oh.volume).groupby("symbol").turn.mean()
keep=set(adv[adv>3e7].index)
oh=oh[oh.symbol.isin(keep)]
sigs=[]
for sym,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); n=len(g)
    if n<PIVLEN*2+60: continue
    o=g.open.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float); v=g.volume.values.astype(float); dts=g.trade_date.values
    tr=np.maximum(h-l,np.maximum(np.abs(h-np.roll(c,1)),np.abs(l-np.roll(c,1)))); tr[0]=h[0]-l[0]
    atr=pd.Series(tr).rolling(ATRLEN,min_periods=50).mean().values
    volsma=pd.Series(v).rolling(VOLMA).mean().values; hivol=v>volsma*VOLMULT
    cmax=pd.Series(h).rolling(PIVLEN*2+1,center=True).max().values
    cmin=pd.Series(l).rolling(PIVLEN*2+1,center=True).min().values
    is_ph=(h==cmax)&hivol; is_pl=(l==cmin)&hivol
    # state
    r_top=r_bot=np.nan; r_broken=False; r_active=False
    s_top=s_bot=np.nan; s_active=False
    for t in range(PIVLEN, n-1):                      # need t+1 for entry
        j=t-PIVLEN                                    # pivot confirmed now if bar j was a pivot
        if j>=0 and is_ph[j] and not np.isnan(atr[j]):
            bt=max(o[j],c[j]); r_bot=bt; r_top=bt+atr[j]; r_broken=False; r_active=True
        if j>=0 and is_pl[j] and not np.isnan(atr[j]):
            bb=min(o[j],c[j]); s_top=bb; s_bot=bb-atr[j]; s_active=True
        trig=None
        if r_active and not r_broken and c[t]>r_top:
            trig="ResBreakout"; r_broken=True
        elif r_active and r_broken and l[t-1]<=r_top and l[t]>r_top:
            trig="FlippedResRetest"
        elif s_active and c[t]>s_bot and l[t-1]<=s_top and l[t]>s_top:
            trig="SupportRetest"
        if trig:
            ent=o[t+1]
            if ent<=0: continue
            r1=(c[t+1]-ent)/ent*100
            r3=(c[min(t+3,n-1)]-ent)/ent*100
            r5=(c[min(t+5,n-1)]-ent)/ent*100
            sigs.append(dict(signal_date=dts[t],entry_date=dts[t+1],symbol=sym,signal=trig,
                entry_open=round(ent,2),ret_nextday_pct=round(r1,2),ret_3d_pct=round(r3,2),ret_5d_pct=round(r5,2)))
S=pd.DataFrame(sigs)
S=S[S.entry_date<="2024-12-31"]
print(f"  {len(S)} next-day BUY signals over 2024 on {S.symbol.nunique()} stocks (daily, high-volume pivot S/R)\n")
print(f"  === by signal type (win% / avg return) ===")
print(f"  {'signal':<18}{'n':>6}{'nextday win%':>14}{'avg nextday%':>14}{'avg 3d%':>10}{'avg 5d%':>10}")
for sg,d in S.groupby("signal"):
    print(f"  {sg:<18}{len(d):>6}{(d.ret_nextday_pct>0).mean()*100:>13.0f}%{d.ret_nextday_pct.mean():>13.2f}%{d.ret_3d_pct.mean():>9.2f}%{d.ret_5d_pct.mean():>9.2f}%")
print(f"  {'ALL':<18}{len(S):>6}{(S.ret_nextday_pct>0).mean()*100:>13.0f}%{S.ret_nextday_pct.mean():>13.2f}%{S.ret_3d_pct.mean():>9.2f}%{S.ret_5d_pct.mean():>9.2f}%")
# WHICH STOCKS work confidently (>=4 signals, ranked by 3d win-rate then avg)
st=S.groupby("symbol").agg(n=("signal","size"),win3=("ret_3d_pct",lambda s:(s>0).mean()*100),
    avg3=("ret_3d_pct","mean"),avg_nd=("ret_nextday_pct","mean"),avgd5=("ret_5d_pct","mean")).reset_index()
st=st[st.n>=4].sort_values(["win3","avg3"],ascending=False)
print(f"\n  === stocks working CONFIDENTLY (>=4 signals, ranked by 3-day win-rate) — top 25 ===")
print(f"  {'symbol':<13}{'#sig':>5}{'3d win%':>9}{'avg 3d%':>9}{'avg nextday%':>14}{'avg 5d%':>9}")
for _,r in st.head(25).iterrows():
    print(f"  {r['symbol']:<13}{int(r['n']):>5}{r['win3']:>8.0f}%{r['avg3']:>8.2f}%{r['avg_nd']:>13.2f}%{r['avgd5']:>8.2f}%")
out=os.path.join(os.path.expanduser("~"),"Downloads","PIVOT_SR_NEXTDAY_2024.xlsx")
with pd.ExcelWriter(out) as xw:
    S.sort_values(["entry_date","symbol"]).to_excel(xw,sheet_name="all_signals",index=False)
    st.sort_values(["win3","avg3"],ascending=False).to_excel(xw,sheet_name="stocks_confidence",index=False)
    S.groupby("signal").agg(n=("symbol","size"),nextday_win=("ret_nextday_pct",lambda s:round((s>0).mean()*100,1)),
        avg_nextday=("ret_nextday_pct","mean"),avg_3d=("ret_3d_pct","mean"),avg_5d=("ret_5d_pct","mean")).round(2).to_excel(xw,sheet_name="by_signal")
print(f"\n  full signals + stock confidence -> {out}")
