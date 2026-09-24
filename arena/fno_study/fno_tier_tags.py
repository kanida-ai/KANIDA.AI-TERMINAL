"""STAGE 1 — tag every F&O stock on every signal day (2023-06 .. 2026-07) with:
detailed Falcon TIER (via the production signal_tier classifier + active rulebook), rank, score, avg_lift,
signal-day features (sret, twoday, rng, trend3_20, turn_pct), entry_context, regime, sector.
Keyed by (symbol, signal_date). Saved -> arena/fno_study/tier_tags.csv. Read-only sources; Falcon untouched."""
import os, sys, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "backend", "power_user", "services"))
import signal_tier as ST
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
UEDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")   # fo_stock_master lives here

fc = sqlite3.connect("file:" + UEDB.replace("\\", "/") + "?mode=ro", uri=True)
fo = [s for (s,) in fc.execute("SELECT DISTINCT symbol FROM fo_stock_master WHERE fo_eligible=1")
      if s not in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50")]
fc.close()
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
print(f"F&O tradable stocks: {len(fo)}")
ph = ",".join("?" * len(fo))
od = pd.read_sql_query(f"SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                       f"WHERE trade_date>='2023-06-01' AND symbol IN ({ph}) ORDER BY symbol,trade_date", uc, params=fo)
rules = ST.load_active_rulebook(uc); uc.close()
print(f"daily rows: {len(od):,}  active tier rules: {len(rules)}")

# rolling signal-day features per symbol (backward windows only)
def feats(g):
    g = g.sort_values("trade_date"); c = g.close; pc = c.shift(1)
    g["sret"] = (c / pc - 1) * 100
    g["rng"] = (g.high - g.low) / pc * 100
    g["twoday"] = (c / c.shift(2) - 1) * 100
    v = g.volume
    g["trend3_20"] = v.rolling(3).mean() / v.rolling(20).mean()
    turn = c * v
    g["turn_pct"] = turn.rolling(252, min_periods=60).apply(lambda x: (x <= x[-1]).mean(), raw=True)
    return g
od = od.groupby("symbol", group_keys=False).apply(feats)

# Falcon attributes per (symbol, signal_date)
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
rk = pd.read_sql_query("SELECT signal_date,symbol,rank,score,n_fires,avg_lift,entry_context,regime,sector "
                       "FROM falcon_full_ranking WHERE signal_date>='2024-01-01'", rc); rc.close()
rk = rk.rename(columns={"signal_date": "trade_date"})
m = od.merge(rk, on=["symbol", "trade_date"], how="left")

def tier(r):
    return ST.classify_from_rulebook({"sret": r.sret, "twoday": r.twoday, "rng": r.rng,
                                      "avg_lift": r.avg_lift, "trend3_20": r.trend3_20, "turn_pct": r.turn_pct}, rules)
m = m[m.trade_date >= "2024-05-01"].copy()
m["tier"] = m.apply(tier, axis=1)
out = m[["symbol", "trade_date", "tier", "rank", "score", "avg_lift", "sret", "twoday", "rng",
         "trend3_20", "turn_pct", "entry_context", "regime", "sector"]].rename(columns={"trade_date": "signal_date"})
p = os.path.join(ROOT, "arena", "fno_study", "tier_tags.csv"); out.to_csv(p, index=False)
print(f"\ntags rows: {len(out):,}  -> {p}")
print("\ntier distribution across all F&O signal-days 2024-05..2026-07:")
print(out.tier.value_counts().to_string())
print("\nranked (in Falcon top-list, avg_lift present):", out.avg_lift.notna().sum(), "of", len(out))
