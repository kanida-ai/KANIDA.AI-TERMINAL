"""Re-run Path-2 scoring + monthly P&L on 2025 (stats frozen <=2024 -> ~all 865 patterns eligible). Leak-free:
PIT weekly features, patterns mined<2025, per-stock/pattern stats only from fires <=2024. D1 at 1X&5X, D2-D6 1X.
"""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
FC = FR.FEATURE_COLS; K = 10.0; CAP = 100000; HOLDS = [1, 2, 3, 5, 6]

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
prows = con.execute("SELECT c.pattern_id, c.mined_year, c.rule_json FROM falcon_promoted_patterns p JOIN falcon_pattern_candidates c ON p.pattern_id=c.pattern_id").fetchall()
PATS = [(pid, (int(my) if str(my).isdigit() else 9999), [(f, op, th) for f, op, th in json.loads(rj)]) for pid, my, rj in prows]
PATS = [p for p in PATS if p[1] < 2025]
print(f"eligible patterns (mined<2025): {len(PATS)}", flush=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-11-01' AND trade_date<='2025-12-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-03-31' ORDER BY symbol,trade_date", con); con.close()

# PIT weekly + forward returns
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; fw = []
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); o = g.open.values.astype(float); td = g.trade_date.values
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=td,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan), weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan), weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    entry = np.roll(o, -1); entry[-1] = np.nan; d = {"symbol": s, "signal_date": td}
    for N in HOLDS:
        ex = np.roll(c, -N); ex[-N:] = np.nan; d[f"r{N}"] = (ex - entry) / entry * 100
    fw.append(pd.DataFrame(d))
FW = pd.concat(fw, ignore_index=True)
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
FCpit = FCpit[(FCpit.trade_date >= "2025-01-01") & (FCpit.trade_date <= "2025-12-31")]
print("PIT features + returns ready", flush=True)

# 2025 fires
syms = FCpit.symbol.values; dates = FCpit.trade_date.values
X = np.full((len(FCpit), len(FC)), np.nan)
for j, col in enumerate(FC):
    if col in FCpit.columns: X[:, j] = pd.to_numeric(FCpit[col], errors="coerce").values
fires = []
for pid, my, rule in PATS:
    m = FR.rule_mask(rule, X)
    if m.any(): fires.append(pd.DataFrame({"pattern_id": pid, "symbol": syms[m], "signal_date": dates[m]}))
F25 = pd.concat(fires, ignore_index=True).merge(FW, on=["symbol", "signal_date"], how="left")
print(f"2025 fires: {len(F25):,}", flush=True)

# frozen stats <=2024 from existing ledger + its 6-day returns
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]].merge(
    FW[["symbol", "signal_date", "r6"]], on=["symbol", "signal_date"], how="left")
L["yr"] = L.signal_date.str[:4]; H = L.dropna(subset=["r6"])
pp = H.groupby("pattern_id").agg(perf=("r6", "mean")).reset_index()
recc = H[H.yr == "2024"].groupby("pattern_id").r6.mean().rename("rec").reset_index()
sp = H.groupby(["symbol", "pattern_id"]).agg(ns=("r6", "size"), avgs=("r6", "mean")).reset_index()

# score 2025
T = F25.dropna(subset=["r6"]).merge(pp, on="pattern_id").merge(recc, on="pattern_id", how="left").merge(sp, on=["symbol", "pattern_id"], how="left")
T["ns"] = T.ns.fillna(0.0); T["avgs"] = T.avgs.fillna(T.perf)
T["rel"] = (T.ns * T.avgs + K * T.perf) / (T.ns + K)
T["recw"] = np.where(T.rec.isna(), 1.0, np.where(T.rec > 0, 1.0, 0.5))
T["pscore"] = T.rel * (1.0 + np.log1p(T.ns)) * T.recw
G = T.groupby(["symbol", "signal_date"]).agg(score=("pscore", "sum")).reset_index().merge(FW, on=["symbol", "signal_date"], how="left")
G["mo"] = G.signal_date.str[:7]
print(f"scored 2025 stock-days: {len(G):,}\n", flush=True)

def monthly(topN):
    picks = pd.concat([g.sort_values("score", ascending=False).head(topN) for _, g in G.groupby("signal_date")])
    picks["mo"] = picks.signal_date.str[:7]
    rows = []
    for mo, m in picks.groupby("mo"):
        rows.append({"month": mo, "trades": len(m), "D1_1X": (m.r1 / 100 * CAP).sum(), "D1_5X": (m.r1 / 100 * CAP * 5).sum(),
                     "D2_1X": (m.r2 / 100 * CAP).sum(), "D3_1X": (m.r3 / 100 * CAP).sum(), "D5_1X": (m.r5 / 100 * CAP).sum(), "D6_1X": (m.r6 / 100 * CAP).sum()})
    R = pd.DataFrame(rows); tot = {"month": "YEAR", "trades": R.trades.sum()}
    for c in R.columns:
        if c not in ("month", "trades"): tot[c] = R[c].sum()
    return pd.concat([R, pd.DataFrame([tot])], ignore_index=True), picks

R, _ = monthly(15)
print(f"===== 2025 MONTHLY P&L, TOP-15, Rs {CAP:,}/stock (D1 1X&5X ; D2-D6 1X) =====")
disp = R.copy()
for c in disp.columns:
    if c not in ("month", "trades"): disp[c] = disp[c].map(lambda x: f"{x:,.0f}")
print(disp.to_string(index=False))
print(f"\n===== 2025 TOP-15 vs 20 vs 30 (year totals) =====")
print(f"{'topN':<6}{'trades':>8}{'D1_1X avg%':>12}{'D1 5X PnL':>14}{'D6_1X avg%':>12}{'D6 1X PnL':>14}")
for N in [15, 20, 30]:
    _, P = monthly(N)
    print(f"  {N:<4}{len(P):>8}{P.r1.mean():>+11.2f}%{(P.r1/100*CAP*5).sum():>14,.0f}{P.r6.mean():>+11.2f}%{(P.r6/100*CAP).sum():>14,.0f}")
# baseline
base = G.groupby("signal_date").r6.mean().mean()
top6 = pd.concat([g.sort_values("score", ascending=False).head(15) for _, g in G.groupby("signal_date")]).r6.mean()
print(f"\n  top-15 6d avg {top6:+.2f}%  vs baseline {base:+.2f}%  -> edge {top6-base:+.2f}%/trade")
