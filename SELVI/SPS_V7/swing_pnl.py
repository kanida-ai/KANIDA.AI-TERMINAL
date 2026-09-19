"""
Honest, leak-free SWING backtest on the Falcon promoted patterns.
Signal (close of day t, point-in-time, WTD weekly): stock matches any active
hit_15pc_20d pattern -> enter at OPEN t+1. Exit: +15% target / -8% stop / 20-day time.
Equal-weight book, up to MAXPOS concurrent. Reports monthly in the standard format.
Leverage columns: 1x (delivery) and 5x shown for format parity (swing can't use MIS 5x;
realistic MTF is ~4x) — treat 5x as an upper bound.
"""
import sqlite3, re
from pathlib import Path
import numpy as np, pandas as pd

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
PAT = r"C:\Users\SPS\Downloads\Falcon_Promoted_Patterns.xlsx"
CAP, MAXPOS, TARGET, STOP, TIME = 500_000.0, 20, 0.15, 0.08, 20
START, END = "2024-05-01", "2026-07-31"

con = sqlite3.connect(str(DB))
lab = pd.read_sql("SELECT symbol,sector FROM instrument_labels", con)
d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily", con)
mk = pd.read_sql("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50'", con); con.close()
d["date"] = pd.to_datetime(d["bar_time"]); piv = lambda c: d.pivot_table(index="date", columns="symbol", values=c).sort_index()
O, H, L, C, V = [piv(x) for x in ["open", "high", "low", "close", "volume"]]
mk["date"] = pd.to_datetime(mk["bar_time"]); MK = mk.set_index("date")["close"].reindex(C.index).ffill()
sector = dict(zip(lab.symbol, lab.sector))
pc = C.shift(1); TR = np.maximum(H - L, np.maximum((H - pc).abs(), (L - pc).abs()))
F = {"atr_20_pct": TR.rolling(20).mean() / C * 100, "atr_5_vs_20": TR.rolling(5).mean() / TR.rolling(20).mean(),
     "close_loc": (C - L) / (H - L).replace(0, np.nan), "vol_vs_20d": V / V.rolling(20).mean()}
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
lv = (V < 0.75 * V.rolling(20).mean()); F["n_sub_75v_7d"] = lv.rolling(7).sum(); F["n_sub_75v_20d"] = lv.rolling(20).sum()
wk = C.index.to_period("W")
F["weekly_close_loc"] = (C - L.groupby(wk).cummin()) / (H.groupby(wk).cummax() - L.groupby(wk).cummin()).replace(0, np.nan)
F["weekly_range_pct"] = (H.groupby(wk).cummax() - L.groupby(wk).cummin()) / C * 100
Wc = C.resample("W-FRI").last(); wsma = Wc.rolling(20).mean().shift(1).reindex(C.index, method="ffill")
F["weekly_close_vs_sma20"] = (C / wsma - 1) * 100

P = pd.read_excel(PAT, sheet_name="promoted_patterns")
pats = P[(P.target == "hit_15pc_20d") & (P.engine_active)]
def pmask(rule):
    m = None
    for cond in rule.split("&"):
        mm = re.match(r"([a-z0-9_]+)\s*(<=|>=|<|>)\s*(-?\d+\.?\d*)", cond.strip())
        if not mm or mm.group(1) not in F: return None
        f, op, val = mm.group(1), mm.group(2), float(mm.group(3)); x = F[f]
        c = {"<=": x <= val, ">=": x >= val, "<": x < val, ">": x > val}[op]; m = c if m is None else m & c
    return m
flagged = pd.DataFrame(False, index=C.index, columns=C.columns)
for _, r in pats.iterrows():
    m = pmask(str(r["rule"]))
    if m is not None: flagged = flagged | m.fillna(False)

# ---- daily event loop ----
dates = list(C.index)
idx = {dt: i for i, dt in enumerate(dates)}
pos = {}   # sym -> dict(entry_px, day_in, target, stop)
rows = []
for i in range(1, len(dates)):
    dt = dates[i]
    if not (START <= dt.strftime("%Y-%m-%d") <= END): continue
    prev = dates[i - 1]
    day_ret = []
    # MTM + exits
    for s in list(pos.keys()):
        p = pos[s]; o = O.at[dt, s] if s in O.columns else np.nan
        cprev = C.at[prev, s]; ccur = C.at[dt, s]; hi = H.at[dt, s]; lo = L.at[dt, s]
        if pd.isna(ccur): continue
        r = ccur / cprev - 1 if not pd.isna(cprev) else 0.0
        # exit conditions realized within the day
        exit_r = None
        if not pd.isna(hi) and hi >= p["target"]: exit_r = (p["target"] / cprev - 1)
        elif not pd.isna(lo) and lo <= p["stop"]: exit_r = (p["stop"] / cprev - 1)
        elif p["day_in"] <= i - TIME: exit_r = r
        day_ret.append(exit_r if exit_r is not None else r)
        p["held"] = i - p["day_in"]
        if exit_r is not None: del pos[s]
    # entries: yesterday's flagged, top by atr, into free slots at today's OPEN
    if len(pos) < MAXPOS:
        cand = [s for s in flagged.columns if flagged.at[prev, s] and s not in pos and not pd.isna(O.at[dt, s])]
        cand = sorted(cand, key=lambda s: -(F["atr_20_pct"].at[prev, s] if not pd.isna(F["atr_20_pct"].at[prev, s]) else 0))
        for s in cand[:MAXPOS - len(pos)]:
            e = O.at[dt, s]
            if pd.isna(e) or e <= 0: continue
            pos[s] = {"entry_px": e, "day_in": i, "target": e * (1 + TARGET), "stop": e * (1 - STOP)}
            day_ret.append(C.at[dt, s] / e - 1)   # entry-day return open->close
    day_ret = [x for x in day_ret if np.isfinite(x) and abs(x) < 0.5]   # drop bad-price blowups
    r1 = float(np.mean(day_ret)) if day_ret else 0.0
    rows.append({"date": dt, "held": len(pos), "ret1x": r1, "ret5x": 5 * r1, "pnl": CAP * 5 * r1})

bt = pd.DataFrame(rows); bt["month"] = bt["date"].dt.strftime("%Y-%m")
print(f"HONEST SWING (patterns hit_15pc_20d · +15% tgt/-8% stop/20d · up to {MAXPOS} concurrent · leak-free)")
print(f"{'month':9}{'days':>5}{'days_pos':>10}{'ret5x_pct':>11}{'ret1x_pct':>11}{'pnl_rs':>12}{'cum5x_pct':>11}")
cum = 0
for mo, g in bt.groupby("month"):
    r5 = g["ret5x"].sum() * 100; r1 = g["ret1x"].sum() * 100; cum += r5
    print(f"{mo:9}{len(g):>5}{int((g['ret1x']>0).sum()):>10}{r5:>10.1f}{r1:>10.1f}{g['pnl'].sum():>12,.0f}{cum:>10.1f}")
print("-" * 69)
print(f"{'TOTAL':9}{len(bt):>5}{int((bt['ret1x']>0).sum()):>10}{bt['ret5x'].sum()*100:>10.1f}{bt['ret1x'].sum()*100:>10.1f}{bt['pnl'].sum():>12,.0f}")
print(f"avg/mo 1x: {bt['ret1x'].sum()*100/max(len(bt.groupby('month')),1):.1f}%")
