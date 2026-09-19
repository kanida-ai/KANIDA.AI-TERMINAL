"""
Pathfinder — "What theme is in play right now?" prototype (the new CORE).
Detects the dominant sector/theme cycle from REAL data, PROVES it with statistics, and hands a
watchlist of the leaders inside it. Honest: it can also say 'no clear theme'. No LLM; numbers real.
Run:  python pathfinder_theme.py
"""
import sqlite3, pandas as pd, numpy as np, sys
try: sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception: pass
WIN=15                                   # look-back window that defines "a cycle"

c=sqlite3.connect("db/kanida.db")
df=pd.read_sql_query("""SELECT o.symbol, substr(o.bar_time,1,10) d, o.close, l.sector
    FROM ohlc_daily o JOIN instrument_labels l ON o.symbol=l.symbol
    WHERE l.in_nifty500=1 AND l.is_active=1 AND l.sector IS NOT NULL""", c); c.close()
df=df.sort_values(["symbol","d"]).reset_index(drop=True); g=df.groupby("symbol",sort=False)
df["ret"]=g["close"].pct_change(); df["ma20"]=g["close"].transform(lambda s:s.rolling(20).mean())
df["r15"]=g["close"].shift(0)/g["close"].shift(WIN)-1
TODAY=df["d"].max()

# sectors with enough names only
big=[s for s,n in df[df["d"]==TODAY].groupby("sector")["symbol"].nunique().items() if n>=5]
d2=df[df["sector"].isin(big)]
sret=d2.groupby(["d","sector"])["ret"].mean().unstack()      # date x sector
mkt =d2.groupby("d")["ret"].mean()                            # market
roll=sret.rolling(WIN).sum(); mroll=mkt.rolling(WIN).sum()
rel =roll.sub(mroll,axis=0)                                   # sector relative strength, date x sector
rank=rel.rank(axis=1,ascending=False)

lead_today=rel.loc[TODAY].sort_values(ascending=False)
top=lead_today.index[0]

# ---- PROOF for the #1 theme ----
recent=sret.tail(WIN); days_out=int((recent[top]>mkt.tail(WIN)).sum())
sec_cum=(recent[top]+1).prod()-1; mkt_cum=(mkt.tail(WIN)+1).prod()-1
tdy=df[df["d"]==TODAY]; secn=tdy[tdy["sector"]==top].dropna(subset=["ma20"])
breadth=round((secn["close"]>secn["ma20"]).mean()*100,0)
# persistence base rate: when a sector led like this, did it stay top-3 five sessions later?
di=list(rel.index); ld=rel.idxmax(axis=1); hits=[]
for i,dt in enumerate(di[:-5]):
    s=ld.get(dt)
    if pd.isna(s): continue
    f=di[i+5]
    if pd.notna(rank.loc[f,s]): hits.append(rank.loc[f,s]<=3)
persist=round(np.mean(hits)*100,0); nP=len(hits)
# watchlist: strongest names inside the theme
wl=(tdy[tdy["sector"]==top].dropna(subset=["r15"]).sort_values("r15",ascending=False).head(4))

print("="*66)
print(f"PATHFINDER — What's in play right now?   ·   after close {TODAY}")
print("="*66)
print("\nTHE CURRENT MARKET CYCLE")
strong = sec_cum>mkt_cum and days_out>=WIN*0.6 and persist>=55
if strong:
    print(f"  >> {top.upper()} is the theme in play — and the data says it's a real rotation, not noise.")
else:
    print(f"  >> {top.upper()} is the day's leader, but the evidence for a *cycle* is thin — I'm watching, not calling it.")
print("\n  THE PROOF")
print(f"   - {top} beat the market on {days_out} of the last {WIN} sessions.")
print(f"   - {top} {sec_cum*100:+.1f}% vs market {mkt_cum*100:+.1f}% over that stretch.")
print(f"   - Broad, not one stock: {breadth:.0f}% of {top} names are above their 20-day average.")
print(f"   - History: when a sector led like this, it stayed a top-3 sector 5 days later {persist:.0f}% of {nP:,} times.")
print(f"\n  WATCH INSIDE {top.upper()} (tomorrow)")
for _,r in wl.iterrows(): print(f"   - {r['symbol']:<12} {r['r15']*100:+.0f}% over {WIN}d")
print(f"\n  >> FOCUS: {top if strong else 'no strong theme yet'}. I'll tell you the moment the evidence weakens.")

# ---- related / emerging themes ----
print("\nOTHER THEMES ON MY RADAR")
for s in lead_today.index[1:4]:
    dO=int((recent[s]>mkt.tail(WIN)).sum()); cum=(recent[s]+1).prod()-1
    state = "building" if (cum>mkt_cum and dO>=WIN*0.5) else "weak / fading"
    print(f"   - {s:<16} {cum*100:+.1f}% vs mkt ({dO}/{WIN} days) -> {state}")

# ---- verdict: was yesterday's theme call right? ----
PREV=di[-2]; y_top=rel.loc[PREV].idxmax(); y_rank_today=rank.loc[TODAY, y_top]
print("\n" + "-"*66)
print(f"VERDICT — yesterday I flagged {y_top} as the leader. Today it ranks #{int(y_rank_today)} "
      f"-> {'STILL LEADING' if y_rank_today<=3 else 'cooling off, evidence weakening'}")
print("="*66)
print("Every number is real, computed from db/kanida.db. Honest: it will say 'no theme' when there isn't one.")
