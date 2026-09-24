"""FALCON pattern-level backtest (recreate). Inside the Falcon prediction system ONLY (no Arena).
For all 865 promoted patterns, find every historical (stock, signal_date) the pattern fired and the NEXT trading
day outcome (open->close). Every fire = one historical trade. Writes a fire-ledger + per-pattern and
per-(stock,pattern) performance tables. Uses Falcon's own falcon_promoted_patterns + falcon_features + ohlc_daily.

Step 1 of the pipeline:
  865 patterns -> pattern-level backtest -> (next: per-stock perf -> firing -> relevance/recency/frequency/
  performance scoring -> combined stock score -> Falcon ranking).  Built on history through 2024 first.
"""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUTDIR = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest"); os.makedirs(OUTDIR, exist_ok=True)
FEATURE_COLS = FR.FEATURE_COLS
START, END = "2019-01-01", "2024-12-31"   # history for the backtest (2024 test window has full priors)

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
# ---- all 865 promoted patterns (every classification, no family drop) ----
prows = con.execute("""
    SELECT c.pattern_id, c.mined_year, c.outcome_target, c.rule_json, p.classification, p.avg_oos_year_lift_pp
    FROM falcon_promoted_patterns p JOIN falcon_pattern_candidates c ON p.pattern_id=c.pattern_id
    ORDER BY p.avg_oos_year_lift_pp DESC""").fetchall()
PATS = [{"pattern_id": pid, "mined_year": int(my) if str(my).isdigit() else my, "target": tgt,
         "rule": [(f, op, th) for f, op, th in json.loads(rj)], "cls": cls, "oos_lift": lift}
        for pid, my, tgt, rj, cls, lift in prows]
print(f"loaded {len(PATS)} promoted patterns (all classifications)")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
feat = pd.read_sql_query(f"SELECT * FROM falcon_features WHERE trade_date>='{START}' AND trade_date<='{END}'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2018-01-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()

# ---- LEAK-FREE: recompute the 4 weekly features WEEK-TO-DATE (PIT), replace the backfilled stored ones ----
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
pit = pd.concat(rec, ignore_index=True)
feat = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(pit, on=["symbol", "trade_date"], how="left")
print("weekly features recomputed week-to-date (PIT) — leak-free")

# ---- next-day open->close per (symbol, signal_date) ----
oh["oc"] = np.where(oh.open > 0, (oh.close - oh.open) / oh.open * 100, np.nan)
oh["next_oc"] = oh.groupby("symbol")["oc"].shift(-1)
oh["next_date"] = oh.groupby("symbol")["trade_date"].shift(-1)
nret = oh.set_index(["symbol", "trade_date"])[["next_oc", "next_date"]]

feat = feat.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
syms = feat.symbol.values; dates = feat.trade_date.values
X = np.full((len(feat), len(FEATURE_COLS)), np.nan)
for j, col in enumerate(FEATURE_COLS):
    if col in feat.columns: X[:, j] = pd.to_numeric(feat[col], errors="coerce").values
# align next-day returns to the feature rows
key = list(zip(syms, dates))
nr = nret.reindex(key)
next_oc = nr.next_oc.values
print(f"feature panel rows {len(feat):,} · with next-day return {int(np.isfinite(next_oc).sum()):,}")

# ---- fire ledger: every pattern fire = one trade ----
led = []
for k, p in enumerate(PATS):
    m = FR.rule_mask(p["rule"], X)
    m &= np.isfinite(next_oc)
    idx = np.where(m)[0]
    if len(idx) == 0: continue
    led.append(pd.DataFrame({"pattern_id": p["pattern_id"], "mined_year": p["mined_year"],
                             "symbol": syms[idx], "signal_date": dates[idx], "next_oc": next_oc[idx]}))
    if (k + 1) % 100 == 0: print(f"  ...{k+1}/{len(PATS)} patterns processed")
L = pd.concat(led, ignore_index=True)
L.to_parquet(os.path.join(OUTDIR, "fire_ledger.parquet"))
print(f"\nFIRE LEDGER: {len(L):,} historical trades (pattern fires w/ next-day outcome)  [2019-2024]")
print(f"  distinct patterns that fired: {L.pattern_id.nunique()} / {len(PATS)}")
print(f"  fires per pattern: median {int(L.groupby('pattern_id').size().median())}  mean {int(L.groupby('pattern_id').size().mean())}")
print(f"  overall next-day win rate {(L.next_oc>0).mean()*100:.1f}%  avg {L.next_oc.mean():+.2f}%")

# ---- per-pattern performance ----
def agg(g):
    n = len(g); w = (g.next_oc > 0).mean(); ar = g.next_oc.mean()
    dn = g.next_oc[g.next_oc <= 0]
    return pd.Series(dict(n=n, win_rate=round(w * 100, 1), avg_ret=round(ar, 3),
                          loss_rate=round((g.next_oc <= 0).mean() * 100, 1),
                          avg_loss=round(dn.mean(), 3) if len(dn) else 0.0, worst=round(g.next_oc.min(), 2),
                          med=round(g.next_oc.median(), 3)))
PP = L.groupby("pattern_id").apply(agg).reset_index()
PP.to_csv(os.path.join(OUTDIR, "pattern_performance.csv"), index=False)
# ---- per (stock, pattern) performance ----
SP = L.groupby(["symbol", "pattern_id"]).apply(lambda g: pd.Series(dict(
    n=len(g), win_rate=round((g.next_oc > 0).mean() * 100, 1), avg_ret=round(g.next_oc.mean(), 3)))).reset_index()
SP.to_parquet(os.path.join(OUTDIR, "stock_pattern_performance.parquet"))
print(f"\nper-pattern perf -> pattern_performance.csv ({len(PP)} patterns)")
print(f"per-(stock,pattern) perf -> stock_pattern_performance.parquet ({len(SP):,} cells)")
print("\ntop 12 patterns by next-day avg return (n>=200):")
top = PP[PP.n >= 200].sort_values("avg_ret", ascending=False).head(12)
print(top.to_string(index=False))
print("\nbottom 6 (n>=200):")
print(PP[PP.n >= 200].sort_values("avg_ret").head(6).to_string(index=False))
