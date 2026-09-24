"""Pattern -> stock CONSISTENCY MATRIX. For each of 865 patterns, the stocks where avg D1 (next-day open->close,
1X) > 0.5% over 2022-01 .. 2025-05, with >=3 fires. Ledger covers 2022-2024; 2025 Jan-May fires computed fresh
(leak-free PIT weekly). Descriptive pattern characterization. Outputs long pairs + per-pattern stock lists.
"""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]; FC = FR.FEATURE_COLS
THR = 0.5; MIN_N = 3

# ---- 2022-2024 fires + D1 from existing leak-free ledger ----
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date", "next_oc"]]
L = L[L.signal_date >= "2022-01-01"].rename(columns={"next_oc": "d1"})
print(f"2022-2024 fires from ledger: {len(L):,}", flush=True)

# ---- 2025 Jan-May fires (fresh, leak-free PIT weekly) ----
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
prows = con.execute("SELECT c.pattern_id, c.rule_json FROM falcon_pattern_candidates c JOIN falcon_promoted_patterns p ON c.pattern_id=p.pattern_id").fetchall()
PATS = [(pid, [(f, op, th) for f, op, th in json.loads(rj)]) for pid, rj in prows]
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-11-01' AND trade_date<='2025-05-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-06-30' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; D1 = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); o = g.open.values.astype(float); td = g.trade_date.values
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=td,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan), weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan), weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    nd = np.roll((c - o) / o * 100, -1); nd[-1] = np.nan   # next-day open->close keyed by signal day
    for i in range(len(td)): D1[(s, td[i])] = nd[i]
FCp = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
FCp = FCp[(FCp.trade_date >= "2025-01-01") & (FCp.trade_date <= "2025-05-31")]
syms = FCp.symbol.values; dts = FCp.trade_date.values
X = np.full((len(FCp), len(FC)), np.nan)
for j, col in enumerate(FC):
    if col in FCp.columns: X[:, j] = pd.to_numeric(FCp[col], errors="coerce").values
f25 = []
for pid, rule in PATS:
    m = FR.rule_mask(rule, X)
    if m.any():
        f25.append(pd.DataFrame({"pattern_id": pid, "symbol": syms[m], "signal_date": dts[m],
                                 "d1": [D1.get((syms[i], dts[i]), np.nan) for i in np.where(m)[0]]}))
F25 = pd.concat(f25, ignore_index=True)
print(f"2025 Jan-May fires: {len(F25):,}", flush=True)

ALL = pd.concat([L, F25], ignore_index=True).dropna(subset=["d1"])
print(f"combined 2022..2025-May fires: {len(ALL):,}", flush=True)

# ---- per (pattern, stock): n, avg D1 ----
M = ALL.groupby(["pattern_id", "symbol"]).agg(n=("d1", "size"), avg_d1=("d1", "mean")).reset_index()
M["avg_d1"] = M.avg_d1.round(3)
CONS = M[(M.n >= MIN_N) & (M.avg_d1 > THR)].sort_values(["pattern_id", "avg_d1"], ascending=[True, False])
print(f"\nCONSISTENT (pattern,stock) cells [avg D1 > {THR}%, n>={MIN_N}]: {len(CONS):,}")
print(f"  patterns with >=1 consistent stock: {CONS.pattern_id.nunique()} of 865")
print(f"  median consistent stocks / pattern: {int(CONS.groupby('pattern_id').size().median())}")

by = CONS.groupby("pattern_id").agg(n_consistent=("symbol", "size"),
                                    stocks=("symbol", lambda x: ", ".join(x.head(40)))).reset_index()
by["pattern"] = "FALCPAT_" + by.pattern_id.astype(str)
xls = os.path.join(os.path.expanduser("~"), "Downloads", "PATTERN_STOCK_CONSISTENCY_MATRIX.xlsx")
with pd.ExcelWriter(xls, engine="openpyxl") as w:
    CONS.rename(columns={"pattern_id": "pattern"}).assign(pattern=lambda d: "FALCPAT_" + d.pattern.astype(str)).to_excel(w, "consistent_pairs", index=False)
    by[["pattern", "n_consistent", "stocks"]].sort_values("n_consistent", ascending=False).to_excel(w, "by_pattern", index=False)
print(f"\nsaved matrix -> {xls}")
print("\ntop 8 patterns by # consistent stocks:")
for _, r in by.sort_values("n_consistent", ascending=False).head(8).iterrows():
    print(f"  {r.pattern}: {r.n_consistent} stocks | {r.stocks[:90]}...")
print(f"\nexample FALCPAT_8347 consistent stocks (avg D1 > {THR}%):")
ex = CONS[CONS.pattern_id == 8347].head(20)
print(ex[["symbol", "n", "avg_d1"]].to_string(index=False))
