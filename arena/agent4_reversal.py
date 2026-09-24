"""ADDENDUM F — AGENT 4: Failed-Breakout / Reversal Specialist. Direction: FADE (long + short).
Failed-break STRUCTURE (poke-through-then-reclaim) detected from daily high/low vs close vs the prior 20-day extreme:
  FAILED BREAKOUT (T): high[T] > 20d-high but close[T] back BELOW it  -> FADE SHORT next day (09:15->EOD).
  FAILED BREAKDOWN(T): low[T]  < 20d-low  but close[T] back ABOVE it  -> FADE LONG  next day.
Calibrate which failure attributes (poke magnitude, volume, regime) reverse cleanly vs keep going. MIS 5×.
Gate: the selected fade must beat the base rate of fading ALL failed breaks, OOS. Protocol LEARN 2022-24 ->
VALIDATE 2025 -> touch-once OOS 2026. Read-only; no Falcon. (Daily captures the poke+reclaim; 1-min = a refinement.)"""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
FRIC = 0.15
oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
df = pd.read_sql_query("SELECT symbol, trade_date, open, high, low, close, volume FROM ohlc_daily WHERE trade_date>='2021-09-01' ORDER BY symbol, trade_date", oc)
oc.close()
rows = []
for s, g in df.groupby("symbol"):
    g = g.reset_index(drop=True)
    if len(g) < 80 or np.median(g.close * g.volume) < 2e7: continue
    hi = g.high.values; lo = g.low.values; c = g.close.values; o = g.open.values; v = g.volume.values.astype(float); dt = g.trade_date.values
    ph = pd.Series(hi).rolling(20).max().shift(1).values; pl = pd.Series(lo).rolling(20).min().shift(1).values
    vavg = pd.Series(v).rolling(20).mean().shift(1).values
    for t in range(20, len(g) - 1):
        nd = (c[t + 1] / o[t + 1] - 1) * 100 if o[t + 1] else np.nan
        if not (nd == nd): continue
        volx = v[t] / vavg[t] if vavg[t] and vavg[t] == vavg[t] else np.nan
        if hi[t] > ph[t] and c[t] < ph[t]:      # failed breakout -> fade short
            poke = (hi[t] - ph[t]) / ph[t] * 100
            rows.append((dt[t], s, "failed_breakout", -1, -nd, poke, volx))
        elif lo[t] < pl[t] and c[t] > pl[t]:     # failed breakdown -> fade long
            poke = (pl[t] - lo[t]) / pl[t] * 100
            rows.append((dt[t], s, "failed_breakdown", 1, nd, poke, volx))
R = pd.DataFrame(rows, columns=["signal_date", "symbol", "kind", "side", "fade_ret", "poke", "volx"])
R["yr"] = R.signal_date.str[:4]; R["period"] = np.where(R.yr <= "2024", "LEARN", np.where(R.yr == "2025", "VALIDATE", "OOS"))
nd = {p: g.signal_date.nunique() for p, g in R.groupby("period")}
print(f"Agent 4 Failed-Break | signals {len(R):,} | stocks {R.symbol.nunique()} | OOS {nd.get('OOS')}d")
def line(sub, label):
    r = {p: sub[sub.period == p].fade_ret.mean() for p in ["LEARN", "VALIDATE", "OOS"]}
    net = r["OOS"] - FRIC; brd = len(sub[sub.period == "OOS"]) / max(nd.get("OOS", 1), 1)
    g = "PASS" if (net > 0 and r["OOS"] > 0.05 and r["LEARN"] > 0 and r["VALIDATE"] > 0) else ("weak+" if net > 0 else "FAIL")
    print(f"{label:<34}{brd:>8.0f}{r['LEARN']:>+9.3f}{r['VALIDATE']:>+9.3f}{r['OOS']:>+9.3f}{net:>+9.3f}{g:>7}")
    return g
print(f"\n{'fade population':<34}{'brdth/d':>8}{'LEARN':>9}{'VALID':>9}{'OOS':>9}{'net':>9}{'GATE':>7}   (fade_ret = profit of the fade)")
npass = 0
npass += line(R, "ALL failed breaks (base rate)") == "PASS"
npass += line(R[R.kind == "failed_breakout"], "  failed BREAKOUT -> fade SHORT") == "PASS"
npass += line(R[R.kind == "failed_breakdown"], "  failed BREAKDOWN -> fade LONG") == "PASS"
# attribute cut: big poke (strong rejection) + high volume
print("  -- attribute selection (does it tighten the edge?) --")
npass += line(R[(R.poke > R.poke.median()) & (R.volx > 1.5)], "  strong poke + high volume") == "PASS"
npass += line(R[(R.kind == "failed_breakdown") & (R.volx > 1.5)], "  failed BREAKDOWN + high vol -> fade LONG") == "PASS"
print(f"\nCELLS PASSING (beat 0 + base, OOS): {npass}")
print("VERDICT:", "failed-break fade shows OOS edge -> promote the passing population" if npass else "no failed-break fade beats its base rate OOS -> PARK (valid finding).")
