"""INTRADAY EXIT ENGINE + MFE/MAE — from 1-min bars. Replaces the crude open->close proxy with a real
entry(09:15)->exit simulation under configurable target / hard-stop / trailing-stop, and measures the
Max Favorable Excursion (best unrealized gain) and Max Adverse Excursion (worst drawdown) of every trade.
Validated against the operator's ACTUAL logged exits (exit_px / exit_reason / stock_ret_pct) for Dec+Jan.
Leak-free by construction (only that day's tape). Read-only.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
log = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})

# ---- pull 1-min bars for the operator's traded (date, symbol) pairs, per date ----
mcon = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
paths = {}
for tdte, g in log.groupby("trade_date"):
    syms = tuple(sorted(set(g.symbol)))
    ph = ",".join("?" * len(syms))
    q = (f"SELECT symbol, substr(bar_time,12,5) tm, open,high,low,close FROM ohlc_1min "
         f"WHERE bar_time>='{tdte} 09:15:00' AND bar_time<='{tdte} 15:29:59' AND symbol IN ({ph}) ORDER BY symbol,bar_time")
    df = pd.read_sql_query(q, mcon, params=syms)
    for s, gg in df.groupby("symbol"):
        paths[(tdte, s)] = gg.reset_index(drop=True)
mcon.close()
print(f"loaded intraday paths for {len(paths)} operator (date,symbol) trades")

def simulate(bars, target=None, stop=None, trail_arm=None, trail_give=None):
    """entry = first bar open. returns (ret_pct, exit_reason, MFE, MAE)."""
    o = bars.open.values.astype(float); h = bars.high.values.astype(float)
    l = bars.low.values.astype(float); c = bars.close.values.astype(float)
    entry = o[0]
    if entry <= 0: return np.nan, "NA", np.nan, np.nan
    peak = entry; runmax = entry; runmin = entry; armed = False
    for i in range(len(c)):
        runmax = max(runmax, h[i]); runmin = min(runmin, l[i]); peak = max(peak, h[i])
        # hard stop (checked first = conservative)
        if stop is not None and l[i] <= entry * (1 - stop / 100):
            r = -stop; return r, "HARD_STOP", (runmax / entry - 1) * 100, (runmin / entry - 1) * 100
        if target is not None and h[i] >= entry * (1 + target / 100):
            return target, "TARGET", (runmax / entry - 1) * 100, (runmin / entry - 1) * 100
        if trail_arm is not None:
            if (peak / entry - 1) * 100 >= trail_arm: armed = True
            if armed and l[i] <= peak * (1 - trail_give / 100):
                r = (peak * (1 - trail_give / 100) / entry - 1) * 100
                return r, "TRAIL", (runmax / entry - 1) * 100, (runmin / entry - 1) * 100
    return (c[-1] / entry - 1) * 100, "EOD", (runmax / entry - 1) * 100, (runmin / entry - 1) * 100

# ---- MFE/MAE of operator trades (EOD, i.e. how much room each trade had) ----
rows = []
for _, t in log.iterrows():
    b = paths.get((t.trade_date, t.symbol))
    if b is None or len(b) < 10: continue
    r, _, mfe, mae = simulate(b)
    rows.append(dict(trade_date=t.trade_date, symbol=t.symbol, actual_ret=t.stock_ret_pct,
                     actual_reason=t.exit_reason, oc_ret=r, MFE=mfe, MAE=mae))
E = pd.DataFrame(rows)
win = E[E.actual_ret > 0.3]; los = E[E.actual_ret <= 0.3]
print("\n===== MFE / MAE (max favorable / adverse excursion, intraday, entry=09:15) =====")
print(f"  winners (n={len(win)}): avg MFE {win.MFE.mean():+.2f}%  avg MAE {win.MAE.mean():+.2f}%  median MFE {win.MFE.median():+.2f}%  MAE {win.MAE.median():+.2f}%")
print(f"  losers  (n={len(los)}): avg MFE {los.MFE.mean():+.2f}%  avg MAE {los.MAE.mean():+.2f}%")
print(f"  => winners run to +{win.MFE.median():.1f}% intraday but only dip {win.MAE.median():.1f}% first (favorable path-shape)")

# ---- calibrate exit config vs operator's ACTUAL returns ----
print("\n===== EXIT-CONFIG SWEEP vs your ACTUAL logged returns (avg ret/trade, all Dec+Jan) =====")
print(f"  your ACTUAL avg ret/trade: {log.stock_ret_pct.mean():+.2f}%  (1x) / {log.stock_ret_pct.mean()*5:+.2f}% (5x)")
print(f"  {'config':<44}{'avg ret':>9}{'5x':>8}{'win%':>7}")
configs = [
    ("open->close (my old proxy)", dict()),
    ("hard_stop 3%", dict(stop=3)),
    ("target 5% + stop 3%", dict(target=5, stop=3)),
    ("target 3% + stop 2%", dict(target=3, stop=2)),
    ("trail arm2% give1.5% + stop3%", dict(trail_arm=2, trail_give=1.5, stop=3)),
    ("trail arm3% give2% + stop3%", dict(trail_arm=3, trail_give=2, stop=3)),
    ("EOD + stop 4% (ride winners)", dict(stop=4)),
]
for name, cfg in configs:
    rs = []
    for _, t in log.iterrows():
        b = paths.get((t.trade_date, t.symbol))
        if b is None or len(b) < 10: continue
        r, _, _, _ = simulate(b, **cfg); rs.append(r)
    rs = np.array(rs)
    print(f"  {name:<44}{np.nanmean(rs):>+8.2f}%{np.nanmean(rs)*5:>+7.2f}%{(rs>0).mean()*100:>6.0f}%")
E.to_excel(os.path.join(os.path.expanduser("~"), "Downloads", "OPERATOR_MFE_MAE.xlsx"), index=False)
print("\n  per-trade MFE/MAE + exit sim -> Downloads/OPERATOR_MFE_MAE.xlsx")
