"""FALCON-FREE tier classifier — reconstruction of backend/power_user/services/signal_tier.py with ALL
Falcon-derived inputs removed. The ONLY Falcon input in the original was `avg_lift` (pattern score/n_fires),
used to gate the two PREMIUM tiers. Everything else (sret, 2-day return, range, volume dry-up, turnover) is
pure signal-day price/volume. This version drops avg_lift so ANY stock can be tiered from raw OHLC alone.

DOES NOT import, modify, or overwrite signal_tier.py. New/standalone. Read-only DB.
Leak-safe: every feature uses backward windows only (bars up to and including the signal day)."""
import os, sqlite3, warnings
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")

def _ok(v): return v is not None and not (isinstance(v,float) and v!=v)

# ── Falcon-free layered classifier (best -> worst; first match wins) ──────────
# CHANGED vs original: PREMIUM-Pullback / PREMIUM-Compression no longer require avg_lift>15.
# REMOVED input: avg_lift (the only Falcon-derived field). All others identical to signal_tier.py.
def classify_tier_no_falcon(sret, twoday, rng, trend3_20, turn_pct):
    if _ok(sret) and sret > 10:                                            return "AVOID"
    if _ok(sret) and sret > 7 and _ok(turn_pct) and turn_pct >= 0.75:      return "AVOID"
    if _ok(sret) and sret <= 2 and _ok(twoday) and twoday < -5:            return "PREMIUM-Pullback"    # was: + avg_lift>15
    if _ok(sret) and sret <= 2 and _ok(rng) and rng < 2:                   return "PREMIUM-Compression" # was: + avg_lift>15
    if _ok(sret) and sret <= 2 and _ok(trend3_20) and trend3_20 < 0.9:     return "ENTERPRISE-Dryup"
    if _ok(sret) and sret <= 2 and _ok(turn_pct) and turn_pct < 0.75:      return "GOLD"
    if _ok(sret) and sret <= 2:                                            return "GOLD-baseline"
    if _ok(sret) and sret <= 5:                                            return "STANDARD"
    if _ok(sret) and sret <= 10:                                           return "STANDARD-weak"
    return "STANDARD-weak"

HIGH_TIERS={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}

# ── signal-day features (identical maths to signal_tier._signal_day_features; NO Falcon) ──
def signal_day_features(bars):
    """bars: ascending list of dicts {close,high,low,volume}; last row == signal day."""
    n=len(bars)
    if n<2: return {}
    sd=bars[-1]; close,high,low,vol=sd["close"],sd["high"],sd["low"],sd["volume"]; pc=bars[-2]["close"]
    out={}
    if pc: out["sret"]=(close/pc-1)*100; out["rng"]=(high-low)/pc*100
    if n>=3 and bars[-3]["close"]: out["twoday"]=(close/bars[-3]["close"]-1)*100
    vols=[b["volume"] for b in bars if b["volume"] is not None]
    if len(vols)>=11:
        w20=vols[-20:]; a20=sum(w20)/len(w20)
        if a20>0: out["trend3_20"]=(sum(vols[-3:])/len(vols[-3:]))/a20
    turns=[b["close"]*b["volume"] for b in bars if b["close"] is not None and b["volume"] is not None]
    if len(turns)>=60:
        win=turns[-252:]; tt=turns[-1]; out["turn_pct"]=sum(1 for t in win if t<=tt)/len(win)
    return out

def tier_for(con, symbol, signal_date):
    rows=con.execute("SELECT close,high,low,volume FROM ohlc_daily WHERE symbol=? AND trade_date<=? ORDER BY trade_date",
                     (symbol,signal_date)).fetchall()
    bars=[{"close":r[0],"high":r[1],"low":r[2],"volume":r[3]} for r in rows][-260:]
    f=signal_day_features(bars)
    t=classify_tier_no_falcon(f.get("sret"),f.get("twoday"),f.get("rng"),f.get("trend3_20"),f.get("turn_pct"))
    return t,f

if __name__=="__main__":
    con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
    print("  FALCON-FREE tiers for the 3 examples (signal day, pure price/volume — no Falcon score):\n")
    print(f"  {'symbol':<11}{'signal_date':<13}{'sret%':>7}{'2day%':>8}{'rng%':>7}{'vol3/20':>9}{'turn%ile':>10}   -> tier")
    for sym,sd in [("FIVESTAR","2025-01-23"),("PNBHOUSING","2025-01-30"),("POLICYBZR","2025-01-22")]:
        t,f=tier_for(con,sym,sd)
        print(f"  {sym:<11}{sd:<13}{f.get('sret',float('nan')):>7.2f}{f.get('twoday',float('nan')):>8.2f}"
              f"{f.get('rng',float('nan')):>7.2f}{f.get('trend3_20',float('nan')):>9.2f}{f.get('turn_pct',float('nan')):>10.2f}   -> {t}")
    # whole-universe tier distribution on one signal day (shows it needs no Falcon input at all)
    day="2025-01-23"
    syms=[r[0] for r in con.execute("SELECT DISTINCT symbol FROM ohlc_daily WHERE trade_date=?",(day,)).fetchall()]
    from collections import Counter
    cnt=Counter()
    for s in syms:
        t,_=tier_for(con,s,day); cnt[t]+=1
    print(f"\n  whole-universe tier mix on {day} ({len(syms)} stocks, zero Falcon input):")
    for t in ["PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline","STANDARD","STANDARD-weak","AVOID"]:
        if cnt.get(t): print(f"    {t:<20}{cnt[t]:>4}")
    print(f"    {'HIGH-TIER total':<20}{sum(cnt[t] for t in HIGH_TIERS):>4}")
    con.close()
