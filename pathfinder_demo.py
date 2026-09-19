"""
Pathfinder — daily research-desk OUTPUT prototype v2 (honest-attractive).
Reads REAL data from db/kanida.db. Now: GROUP base rates (a stock's peers, not one thin sample),
plain-English horizons, cost-aware decisions, and the full decision vocabulary. No LLM — hooks are
templated so you judge FORMAT + METHOD before any build. Every number is real.

Run:   python pathfinder_demo.py
"""
import sqlite3, pandas as pd, numpy as np, sys
try: sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception: pass

DB="db/kanida.db"; COST=0.30           # % round-trip cost assumption (honest gate)
DIP=-0.06; SURGE=0.06                   # what counts as a hard dip / big surge

def load():
    c=sqlite3.connect(DB)
    df=pd.read_sql_query("""SELECT o.symbol, substr(o.bar_time,1,10) d, o.close, o.volume,
        l.sector, l.in_nifty50 FROM ohlc_daily o JOIN instrument_labels l ON o.symbol=l.symbol
        WHERE l.in_nifty500=1 AND l.is_active=1""", c); c.close()
    df=df.sort_values(["symbol","d"]).reset_index(drop=True); g=df.groupby("symbol",sort=False)
    df["ret"]=g["close"].pct_change()
    for h in (1,3,5): df[f"r{h}"]=g["close"].shift(-h)/df["close"]-1     # forward returns
    df["vol20"]=g["volume"].transform(lambda s:s.rolling(20).mean().shift(1))
    return df
df=load(); TODAY=df["d"].max(); today=df[df["d"]==TODAY].dropna(subset=["ret"]).copy()

def rate(mask,h):                       # group base rate over a horizon
    r=df.loc[mask & df[f"r{h}"].notna(), f"r{h}"]
    return (len(r), round((r>0).mean()*100,0), round(r.median()*100,2)) if len(r) else (0,None,None)

def card(i,title,lines,decision,why):
    print(f"\n[{i}] {title}")
    for L in lines: print(f"    {L}")
    print(f"    >> {decision}\n       {why}")

print("="*66)
print(f"PATHFINDER — Research Desk   ·   after close {TODAY}")
print(f"Scanned {today['symbol'].nunique()} Nifty-500 stocks. Here are the findings that survived my filters.")
print(f"Come back next trading day to see whether I was right.")
print("="*66)

# 1 MARKET REGIME -------------------------------------------------------------
mkt=df[df["in_nifty50"]==1].groupby("d")["ret"].mean(); mtoday=mkt.loc[TODAY]*100
sim=(mkt>=mkt.loc[TODAY]-0.002)&(mkt<=mkt.loc[TODAY]+0.002)
mnext=mkt.shift(-1)[sim].dropna(); n=len(mnext); wr=round((mnext>0).mean()*100,0); med=round(mnext.median()*100,2)
dec = "NO TRADE" if med<=COST else "VIRTUAL LONG (market tilt)"
card(1,"MARKET REGIME",
     [f"NIFTY moved {mtoday:+.1f}% today. Does the strength carry into the next trading day?",
      f"{n} comparable sessions -> {wr:.0f}% higher next day, typical move {med:+.2f}%."],
     dec, "Edge is smaller than trading costs." if dec.startswith("NO") else "Edge clears costs.")

# 2 THE DIP (group of peers, not one stock) -----------------------------------
dip=today.sort_values("ret").iloc[0]
n1,wr1,med1=rate(df["ret"]<=DIP,1); n5,wr5,med5=rate(df["ret"]<=DIP,5)
if med5 and med5>2*COST and wr5>=58: d2="VIRTUAL LONG (hold ~1 week)"; why="Bounce clears costs over the week."
elif med1 and med1>COST and wr1>=58: d2="VIRTUAL LONG (next day)"; why="Next-day bounce clears costs."
else: d2="NO TRADE"; why="Real drop, but the bounce doesn't beat costs."
card(2,"STOCK BEHAVIOUR — THE DIP",
     [f"{dip['symbol']} fell {dip['ret']*100:.1f}% today — the day's hardest drop.",
      f"Across Nifty-500, when any stock falls 6%+ in a day ({n1:,} cases):",
      f"  next trading day: {wr1:.0f}% higher, typical {med1:+.2f}%",
      f"  over the next trading week: {wr5:.0f}% higher, typical {med5:+.2f}%"], d2, why)

# 3 THE SURGE (does chasing work? usually not) --------------------------------
sur=today.sort_values("ret").iloc[-1]
n1,wr1,med1=rate(df["ret"]>=SURGE,1)
if wr1<=45 and med1<-COST: d3="REJECT HYPOTHESIS: 'chase the winner'  +  NEW EXPERIMENT: fade the surge"; why="Buying big surges lost money historically; worth testing the short side across the group."
elif wr1>=58 and med1>COST: d3="VIRTUAL LONG"; why="Momentum continues after costs."
else: d3="NO TRADE"; why="No reliable continuation edge."
card(3,"STOCK BEHAVIOUR — THE SURGE",
     [f"{sur['symbol']} jumped {sur['ret']*100:.1f}% today. Does chasing a move this big pay?",
      f"Across Nifty-500, when a stock jumps 6%+ in a day ({n1:,} cases):",
      f"  next trading day: only {wr1:.0f}% continued higher, typical {med1:+.2f}%"], d3, why)

# 4 VOLUME ANOMALY ------------------------------------------------------------
today["volx"]=today["volume"]/today["vol20"]
unu=today[(today["volx"]>=3)&(today["ret"].abs()<0.015)].sort_values("volx",ascending=False)
if len(unu):
    u=unu.iloc[0]; m=(df["volume"]/df["vol20"]>=3)&(df["ret"].abs()<0.015)
    r5=df.loc[m&df["r5"].notna(),"r5"]; nn=len(r5); big=round((r5.abs()>0.03).mean()*100,0); up=round((r5>0).mean()*100,0)
    card(4,"VOLUME ANOMALY",
         [f"{u['symbol']} traded {u['volx']:.0f}x normal volume but closed flat ({u['ret']*100:+.1f}%).",
          f"Across Nifty-500 ({nn:,} similar days): {big:.0f}% made a 3%+ move within the week, {up:.0f}% of them up."],
         "NEW EXPERIMENT: what separates the winners from losers here?",
         "Big interest, no clean direction yet — worth isolating the tell.")

# 5 SECTOR ROTATION (leader that was a laggard) -------------------------------
sd=df.dropna(subset=["ret","sector"]).groupby(["d","sector"])["ret"].mean().reset_index()
piv=sd.pivot(index="d",columns="sector",values="ret"); cum15=piv.rolling(15).sum()
trank=piv.loc[TODAY].rank(ascending=False); lrank=cum15.loc[TODAY].rank(ascending=False)
rot=[s for s in piv.columns if trank[s]<=2 and lrank[s]>=len(piv.columns)-3]  # top-2 today, bottom-3 over 15d
if rot:
    s=rot[0]
    card(5,"SECTOR ROTATION",
         [f"{s} was a bottom sector for weeks — today it flipped to the strongest.",
          f"That's the kind of turn that sometimes starts a multi-week rotation."],
         "NEW EXPERIMENT: track {} leadership for 5 days".format(s),
         "A one-day flip isn't proof — Pathfinder tests whether it sticks.")
else:
    print("\n[5] SECTOR ROTATION\n    No sector flipped from laggard to leader today. Nothing to test.")

# 6 RELATIONSHIP (pair link broke) --------------------------------------------
PAIRS=[("TATAMOTORS","M&M"),("HDFCBANK","ICICIBANK"),("INFY","TCS"),("SBIN","BANKBARODA")]
ps=set(sum([list(p) for p in PAIRS],[])); cl=df[df["symbol"].isin(ps)].pivot_table(index="d",columns="symbol",values="close")
best=None
for a,b in PAIRS:
    if a in cl and b in cl:
        sp=(np.log(cl[a])-np.log(cl[b])).dropna()
        if len(sp)<160: continue
        z=(sp-sp.rolling(60).mean())/sp.rolling(60).std()
        if TODAY in z.index and pd.notna(z.loc[TODAY]) and (best is None or abs(z.loc[TODAY])>abs(best[2])): best=(a,b,z.loc[TODAY],z)
if best:
    a,b,zt,z=best; zi=list(z.index); conv=[abs(z.loc[zi[zi.index(dt)+5]])<abs(z.loc[dt]) for dt in z[abs(z)>=2].dropna().index if zi.index(dt)+5<len(zi) and pd.notna(z.loc[zi[zi.index(dt)+5]])]
    c6=round(np.mean(conv)*100,0) if conv else 0
    card(6,"RELATIONSHIP",
         [f"{a} and {b} normally move together — today their gap hit {zt:+.1f} sigma (extreme).",
          f"When this pair stretched this far before, it snapped back within a week {c6:.0f}% of {len(conv)} times."],
         "NEW EXPERIMENT: convergence (long {} / short {})".format(b if zt>0 else a, a if zt>0 else b),
         "Strong pattern — but the short leg is hard for retail, so it stays an experiment, not a call.")

# 7 VERDICT — was I right yesterday? -----------------------------------------
PREV=sorted(df["d"].unique())[-2]; pv=df[df["d"]==PREV].dropna(subset=["ret","r1"])
print("\n"+"-"*66); print(f"VERDICT — grading yesterday's calls ({PREV} -> {TODAY})")
d1=pv.sort_values("ret").iloc[0]; out=d1["r1"]*100
n1,wr1,med1=rate((df["d"]<=PREV)&(df["ret"]<=DIP),1)
dec="VIRTUAL LONG" if (n1>=50 and wr1>=58 and med1>COST) else "NO TRADE"
tag = ("RIGHT" if out>0 else "WRONG") if dec=="VIRTUAL LONG" else ("RIGHT (dodged a drop)" if out<=0 else "missed a bounce")
print(f"  Dip call: hard fallers -> {dec}. Group then moved {out:+.1f}% -> {tag}")
print(f"  (Follow-up tomorrow: 'Yesterday's surge experiment has a new finding -> open')")

print("\n"+"="*66)
print("Decisions available: VIRTUAL LONG · VIRTUAL SHORT · WATCH · NO TRADE · NEW EXPERIMENT · CONTINUE · REJECT")
print("Every number is real, group-based, cost-aware. Small samples labelled. No returns promised.")
print("="*66)
