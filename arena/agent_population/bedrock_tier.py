"""Apply the FALCON TIER RULEBOOK (from signal_tier.py) to Bedrock's signals, then test whether gating to HIGH-TIER
is a better selector than the dist_high_120 rank I was using. Faithful reproduction of _signal_day_features +
classify_from_rulebook (look-ahead safe: signal-day close known at EOD, entry next open)."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; AP = os.path.dirname(os.path.abspath(__file__))
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}

def classify(sret, twoday, rng, avg_lift, trend3_20, turn_pct):
    if sret is None or not np.isfinite(sret): return "UNKNOWN"
    if sret > 10: return "AVOID"
    if sret > 7 and turn_pct is not None and np.isfinite(turn_pct) and turn_pct >= 0.75: return "AVOID"
    if sret <= 2 and twoday is not None and np.isfinite(twoday) and twoday < -5 and avg_lift and avg_lift > 15: return "PREMIUM-Pullback"
    if sret <= 2 and rng is not None and np.isfinite(rng) and rng < 2 and avg_lift and avg_lift > 15: return "PREMIUM-Compression"
    if sret <= 2 and trend3_20 is not None and np.isfinite(trend3_20) and trend3_20 < 0.9: return "ENTERPRISE-Dryup"
    if sret <= 2 and turn_pct is not None and np.isfinite(turn_pct) and turn_pct < 0.75: return "GOLD"
    if sret <= 2: return "GOLD-baseline"
    if sret <= 5: return "STANDARD"
    return "STANDARD-weak"

G = pd.read_pickle(os.path.join(AP, "_bedrock_grid_full.pkl"))          # swing grid (has signal_date, symbol, rank, shadow rets)
GI = pd.read_pickle(os.path.join(AP, "_bedrock_intraday_grid.pkl"))
uc = sqlite3.connect("file:" + os.path.join(ROOT, "data", "db", "kanida_universe.db").replace("\\", "/") + "?mode=ro", uri=True)
lift = pd.read_sql_query("SELECT lift_pp FROM falcon_pattern_taxonomy WHERE pattern_id=8787", uc).lift_pp.iloc[0]
syms = sorted(set(G.symbol) | set(GI.symbol))
oh = pd.read_sql_query("SELECT symbol,trade_date,close,high,low,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND symbol IN (%s) ORDER BY symbol,trade_date"
                       % ",".join("?"*len(syms)), uc, params=syms); uc.close()
print(f"Bedrock avg_lift (pattern lift_pp) = {lift}")

# --- tier features per (symbol, signal_date), backward windows only ---
feat = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    c = g.close.values.astype(float); h = g.high.values; l = g.low.values; v = g.volume.values.astype(float)
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c/pc - 1)*100; rng = (h - l)/pc*100; twoday = (c/c2 - 1)*100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    trend3_20 = np.where(av20 > 0, av3/av20, np.nan)
    turn = c*v; tp = pd.Series(turn).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    feat.append(pd.DataFrame(dict(symbol=s, signal_date=g.trade_date.values, sret=sret, rng=rng, twoday=twoday,
                                  trend3_20=trend3_20, turn_pct=tp)))
F = pd.concat(feat, ignore_index=True)
F["tier"] = [classify(r.sret, r.twoday, r.rng, lift, r.trend3_20, r.turn_pct) for r in F.itertuples()]
F["high"] = F.tier.isin(HIGH).astype(int)

def tag(grid):
    return grid.merge(F[["symbol", "signal_date", "tier", "high", "sret"]], on=["symbol", "signal_date"], how="left")
G = tag(G); GI = tag(GI)

print("\n=== Bedrock signal distribution by TIER (2025-2026) + SWING D15 (OPEN) performance ===")
k = "OPEN|None|None|15"
d = G.dropna(subset=[k])
order = ["PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline", "STANDARD", "STANDARD-weak", "AVOID", "UNKNOWN"]
print(f"  {'tier':<20}{'signals':>9}{'win%':>7}{'avg%':>8}{'  <- high-tier'}")
for t in order:
    s = d[d.tier == t]
    if len(s) == 0: continue
    r = s[k].values
    print(f"  {t:<20}{len(s):>9}{(r>0).mean()*100:>6.0f}%{r.mean():>7.2f}%{'   HIGH' if t in HIGH else ''}")
allr = d[k].values; hr = d[d.high == 1][k].values
print(f"  {'-'*44}")
print(f"  {'ALL signals':<20}{len(allr):>9}{(allr>0).mean()*100:>6.0f}%{allr.mean():>7.2f}%")
print(f"  {'HIGH-TIER only':<20}{len(hr):>9}{(hr>0).mean()*100:>6.0f}%{hr.mean():>7.2f}%   <<< the gate")

print("\n=== INTRADAY (DIP entry, 1% stop) performance by tier ===")
ki = "DIP|None|1.0"
di = GI.dropna(subset=[ki])
for grp, lab in [(di, "ALL"), (di[di.high == 1], "HIGH-TIER")]:
    r = grp[ki].values
    print(f"  {lab:<14}{len(r):>7} signals · win {(r>0).mean()*100:>3.0f}% · avg {r.mean():+.2f}%")

# save tiered grids for the re-run
G.to_pickle(os.path.join(AP, "_bedrock_grid_full_tier.pkl"))
GI.to_pickle(os.path.join(AP, "_bedrock_intraday_grid_tier.pkl"))
print("\nsaved tiered grids -> _bedrock_grid_full_tier.pkl / _bedrock_intraday_grid_tier.pkl")
