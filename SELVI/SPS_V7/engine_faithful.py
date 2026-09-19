"""
Faithful implementation of the described engine:
  - compute features (point-in-time, WTD weekly) for all stocks as of close T
  - run all active patterns; a pattern fires if the stock passes every condition
  - only patterns with mined_year <= year(T) are available (leak-free)
  - score = sum(oos_lift of fired), n_fires = count; keep n_fires >= 10
  - RANK by AVERAGE lift (score / n_fires)  [the key step]
  - tier filter: exclude stocks that already ran up > +10% on day T (AVOID zone)
  - take top 15, buy next morning (open T+1)
Fast check: open->EOD return of the top-15 + recall vs the traders' actual picks.

Run: PYTHONIOENCODING=utf-8 python engine_faithful.py
"""
import sqlite3, re
import numpy as np, pandas as pd

DB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
PAT = r"C:\Users\SPS\Downloads\Falcon_Promoted_Patterns.xlsx"
LOG = r"C:\Users\SPS\Downloads\Tradelog_data.xlsx"
START, END = "2024-05-01", "2026-07-31"

con = sqlite3.connect(DB)
lab = pd.read_sql("SELECT symbol,sector FROM instrument_labels", con)
d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily", con)
mk = pd.read_sql("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50'", con); con.close()
d["date"] = pd.to_datetime(d["bar_time"]); piv = lambda c: d.pivot_table(index="date", columns="symbol", values=c).sort_index()
O, H, L, C, V = [piv(x) for x in ["open", "high", "low", "close", "volume"]]
mk["date"] = pd.to_datetime(mk["bar_time"]); MK = mk.set_index("date")["close"].reindex(C.index).ffill()
sector = dict(zip(lab.symbol, lab.sector))
pc = C.shift(1); TR = np.maximum(H - L, np.maximum((H - pc).abs(), (L - pc).abs()))
up = C.diff().clip(lower=0); dn = (-C.diff()).clip(lower=0)
F = {"atr_20_pct": TR.rolling(20).mean() / C * 100, "atr_5_vs_20": TR.rolling(5).mean() / TR.rolling(20).mean(),
     "close_loc": (C - L) / (H - L).replace(0, np.nan), "vol_vs_20d": V / V.rolling(20).mean(),
     "vol_5d_vs_20d": V.rolling(5).mean() / V.rolling(20).mean(),
     "range_pct": (H - L) / C * 100, "gap_pct": (O / pc - 1) * 100,
     "body_pct": (C - O).abs() / (H - L).replace(0, np.nan), "rsi_14": 100 - 100 / (1 + up.rolling(14).mean() / dn.rolling(14).mean())}
F["upper_wick_pct"] = (H - np.maximum(O, C)) / (H - L).replace(0, np.nan)
F["lower_wick_pct"] = (np.minimum(O, C) - L) / (H - L).replace(0, np.nan)
for n in (5, 20, 60): F[f"roc_{n}"] = (C / C.shift(n) - 1) * 100
for n in (10, 20, 60, 120, 252): F[f"dist_high_{n}"] = (C / H.rolling(n).max() - 1) * 100
for n in (20, 50, 200): F[f"dist_sma_{n}"] = (C / C.rolling(n).mean() - 1) * 100
for n in (20, 50): sma = C.rolling(n).mean(); F[f"slope_sma_{n}"] = (sma / sma.shift(5) - 1) * 100
for n in (20, 60):
    sr = (C / C.shift(n) - 1) * 100; mr = (MK / MK.shift(n) - 1) * 100; F[f"rs_market_{n}d"] = sr.sub(mr, axis=0)
    sec = pd.DataFrame(index=C.index, columns=C.columns, dtype=float); bs = {}
    for s in C.columns: bs.setdefault(sector.get(s, "NA"), []).append(s)
    for k, mem in bs.items():
        mm = sr[mem].mean(axis=1)
        for s in mem: sec[s] = mm
    F[f"rs_sector_{n}d"] = sr - sec
rng = (H - L) / pc * 100
F["n_sub_3_range_7d"] = (rng < 3).rolling(7).sum(); F["n_sub_2_5_range_7d"] = (rng < 2.5).rolling(7).sum()
lvm = (V < 0.75 * V.rolling(20).mean()); F["n_sub_75v_7d"] = lvm.rolling(7).sum(); F["n_sub_75v_20d"] = lvm.rolling(20).sum()
F["n_higher_highs_5d"] = (H.diff() > 0).rolling(5).sum(); F["n_higher_lows_5d"] = (L.diff() > 0).rolling(5).sum()
wkp = C.index.to_period("W")
F["weekly_close_loc"] = (C - L.groupby(wkp).cummin()) / (H.groupby(wkp).cummax() - L.groupby(wkp).cummin()).replace(0, np.nan)
F["weekly_range_pct"] = (H.groupby(wkp).cummax() - L.groupby(wkp).cummin()) / C * 100
Wc = C.resample("W-FRI").last(); wsma = Wc.rolling(20).mean().shift(1).reindex(C.index, method="ffill")
F["weekly_close_vs_sma20"] = (C / wsma - 1) * 100
F["weekly_breakout_20w"] = (C > H.resample("W-FRI").max().rolling(20).max().shift(1).reindex(C.index, method="ffill")).astype(float)

P = pd.read_excel(PAT, sheet_name="promoted_patterns"); P = P[P.engine_active]
years = pd.Series(C.index.year, index=C.index)
n_fires = pd.DataFrame(0.0, index=C.index, columns=C.columns)
sum_lift = pd.DataFrame(0.0, index=C.index, columns=C.columns)
used = skipped = 0
for _, r in P.iterrows():
    m = None; ok = True
    for cond in str(r["rule"]).split("&"):
        mm = re.match(r"([a-z0-9_]+)\s*(<=|>=|<|>)\s*(-?\d+\.?\d*)", cond.strip())
        if not mm or mm.group(1) not in F: ok = False; break
        f, op, val = mm.group(1), mm.group(2), float(mm.group(3)); x = F[f]
        c = {"<=": x <= val, ">=": x >= val, "<": x < val, ">": x > val}[op]; m = c if m is None else m & c
    if not ok or m is None: skipped += 1; continue
    used += 1
    avail = (years >= int(r["mined_year"])).values[:, None]        # leak-free: pattern exists by then
    fire = m.fillna(False).values & avail
    n_fires += fire; sum_lift += fire * float(r["oos_lift"])
print(f"patterns used {used}/{len(P)} (skipped {skipped} for unknown features)")
avg_lift = (sum_lift / n_fires.replace(0, np.nan)).where(n_fires >= 10)   # rank key + n_fires>=10 filter
day_move = (C / pc - 1) * 100                                             # signal-day move (for tier)
avoid = day_move > 10                                                     # AVOID zone
score = avg_lift.mask(avoid)

# picks for trade day T+1 = top15 avg_lift as of close T
oc = ((C - O) / O.replace(0, np.nan) * 100).replace([np.inf, -np.inf], np.nan)
oc = oc.where(oc.abs() < 50)      # drop bad-price blowups
t = pd.read_excel(LOG, sheet_name="F_T15_Trades", header=0); t["trade_date"] = pd.to_datetime(t["trade_date"])
actual = {dt: set(g["symbol"]) for dt, g in t.groupby("trade_date")}
dts = [dt for dt in C.index if START <= dt.strftime("%Y-%m-%d") <= END]
day_ret, recalls, npick = [], [], []
for i in range(1, len(C.index)):
    dt = C.index[i]
    if dt not in dts: continue
    prev = C.index[i - 1]
    sc = score.loc[prev].dropna()
    if sc.empty: continue
    pick = [s for s in sc.sort_values(ascending=False).index if not pd.isna(oc.loc[dt, s])][:15]
    if not pick: continue
    npick.append(len(pick)); day_ret.append(oc.loc[dt][pick].mean())
    act = actual.get(dt, set())
    if act: recalls.append(len(set(pick) & act) / len(act))
a = np.array(day_ret)
print(f"avg picks/day: {np.mean(npick):.1f} | days: {len(a)}")
print(f"TOP-15 by AVG-LIFT (leak-free) open->EOD: avg/day {a.mean():+.3f}%  win-day {(a>0).mean()*100:.0f}%")
print(f"recall vs traders' actual picks: {np.mean(recalls)*100:.1f}%")
print(f"(their logs: +1.2%/day · my prior rankings: all negative)")

# ---- full faithful run: top-15 avg-lift picks through the intraday basket trail ----
picks = {}
for i in range(1, len(C.index)):
    dt = C.index[i]
    if dt not in dts: continue
    prev = C.index[i-1]; sc = score.loc[prev].dropna()
    if sc.empty: continue
    pk = [s for s in sc.sort_values(ascending=False).index if not pd.isna(O.loc[dt, s])][:15]
    if pk: picks[dt] = pk
ARM,FLOOR,GIVE,HARD,RT,CAP = 0.06,0.02,0.05,0.03,0.0012,500000.0
def bexit(cp):
    peak=-9; armed=False
    for x in cp:
        peak=max(peak,x)
        if not armed and x<=-HARD: return -HARD
        if x>=ARM: armed=True
        if armed:
            fl=max(FLOOR,peak-GIVE)
            if x<=fl: return fl
    return cp[-1] if len(cp) else 0.0
con=sqlite3.connect(DB); rows=[]
for dt,syms in sorted(picks.items()):
    ds=dt.strftime("%Y-%m-%d")
    q=("SELECT symbol,bar_time,open,close FROM ohlc_1min WHERE symbol IN (%s) AND bar_time>=? AND bar_time<=? ORDER BY bar_time"%",".join("?"*len(syms)))
    m=pd.read_sql(q,con,params=list(syms)+[ds+" 09:15:00",ds+" 15:29:00"])
    if m.empty: continue
    m["tt"]=pd.to_datetime(m["bar_time"]); paths=[]
    for s in syms:
        g=m[m.symbol==s].set_index("tt")
        try: e=(float(g.loc[ds+" 09:15:00","open"])+float(g.loc[ds+" 09:16:00","open"]))/2
        except KeyError: continue
        if e<=0: continue
        paths.append(g.loc[ds+" 09:16:00":,"close"].astype(float)/e-1)
    if not paths: continue
    cp=5.0*pd.concat(paths,axis=1).ffill().mean(axis=1).to_numpy()
    ex=bexit(cp)-RT*5; rows.append({"date":dt,"ret5x":ex,"ret1x":ex/5,"pnl":CAP*ex})
con.close()
bt=pd.DataFrame(rows); bt["month"]=bt["date"].dt.strftime("%Y-%m")
print("\nFAITHFUL ENGINE (avg-lift top15) + intraday trail — monthly:")
print(f"{'month':9}{'days':>5}{'days_pos':>10}{'ret5x_pct':>11}{'ret1x_pct':>11}{'pnl_rs':>12}{'cum5x_pct':>11}")
cum=0
for mo,g in bt.groupby("month"):
    r5=g["ret5x"].sum()*100; r1=g["ret1x"].sum()*100; cum+=r5
    print(f"{mo:9}{len(g):>5}{int((g['ret1x']>0).sum()):>10}{r5:>10.1f}{r1:>10.1f}{g['pnl'].sum():>12,.0f}{cum:>10.1f}")
print("-"*69); print(f"{'TOTAL':9}{len(bt):>5}{int((bt['ret1x']>0).sum()):>10}{bt['ret5x'].sum()*100:>10.1f}{bt['ret1x'].sum()*100:>10.1f}{bt['pnl'].sum():>12,.0f}")
print(f"avg/mo 1x: {bt['ret1x'].sum()*100/max(len(bt.groupby('month')),1):.1f}%  (their logs ~23-26%/mo)")
