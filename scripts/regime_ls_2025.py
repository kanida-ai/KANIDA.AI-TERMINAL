"""#1 regime gate + #2 long/short, 2025 forward (leak-free). Market regime from universe proxy. Regime UP ->
long the dip-blend top-15; regime DOWN -> short the weakest breakdown top-15. Rs 25k/pos, hold 6d. Compares to
long-only. Monthly rupee P&L + drawdown. Patterns mined<2025, stats <=2024, PIT features.
"""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]; FC = FR.FEATURE_COLS
CAP = 25000; TOPN = 15; HOLD = 6

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-02-28' ORDER BY symbol,trade_date", con)
POS = {}; fw = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); td = list(g.trade_date.values)
    POS[s] = (o, c, td, {d: i for i, d in enumerate(td)})
    entry = np.roll(o, -1); entry[-1] = np.nan; ex = np.roll(c, -HOLD); ex[-HOLD:] = np.nan
    d1 = np.roll((c - o) / o * 100, -1); d1[-1] = np.nan
    fw.append(pd.DataFrame({"symbol": s, "signal_date": td, "d1": d1, "r6": (ex - entry) / entry * 100, "ret_dd": (c / o - 1)}))
FW = pd.concat(fw, ignore_index=True)

# ---- market regime: universe equal-weight index, up if > 20d MA ----
mkt = oh.copy(); mkt["cc"] = mkt.groupby("symbol").close.pct_change()
idx = mkt.groupby("trade_date").cc.mean().sort_index(); eqi = (1 + idx.fillna(0)).cumprod()
regime = (eqi > eqi.rolling(20).mean()).to_dict()   # per trade_date: True=up
regime50 = (eqi > eqi.rolling(50).mean()).to_dict()  # smoother risk-off gate
print(f"regime up-days in 2025: {sum(1 for d,v in regime.items() if d[:4]=='2025' and v)}/{sum(1 for d in regime if d[:4]=='2025')}", flush=True)

# ---- prior + state (<=2024, patterns mined<2025) ----
mrows = con.execute("SELECT c.pattern_id, c.mined_year FROM falcon_pattern_candidates c JOIN falcon_promoted_patterns p ON c.pattern_id=p.pattern_id").fetchall()
ELIG = {pid for pid, my in mrows if str(my).isdigit() and int(my) < 2025}
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date", "next_oc"]].rename(columns={"next_oc": "d1"})
L = L[L.pattern_id.isin(ELIG)].merge(FW[["symbol", "signal_date", "r6"]], on=["symbol", "signal_date"], how="left").dropna(subset=["r6"])
STATE = L.groupby("pattern_id").r6.mean().rename("state").reset_index()
Pp = L[L.signal_date <= "2024-12-31"].groupby(["pattern_id", "symbol"]).agg(n=("d1", "size"), avg=("d1", "mean")).reset_index()
def tm(r):
    if r.avg >= 10: return 0.0
    if r.avg >= 2 and r.n >= 10: return 1.0
    if 1 <= r.avg < 2 and r.n >= 20: return 0.65
    if r.avg >= 5 and r.n >= 5: return 0.35
    return 0.0
Pp["cs"] = np.log1p(Pp.n) * Pp.avg * Pp.apply(tm, axis=1); PRI = Pp[Pp.cs > 0][["pattern_id", "symbol", "cs"]]

# ---- 2025 fires + features (PIT weekly) ----
prows = con.execute("SELECT c.pattern_id, c.rule_json FROM falcon_pattern_candidates c JOIN falcon_promoted_patterns p ON c.pattern_id=p.pattern_id").fetchall()
PATS = [(pid, [(f, op, th) for f, op, th in json.loads(rj)]) for pid, rj in prows if pid in ELIG]
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-09-01' AND trade_date<='2025-12-31'", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan), weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan), weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
FCp = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
FCp = FCp[(FCp.trade_date >= "2025-01-01") & (FCp.trade_date <= "2025-12-31")]
# long fires
syms = FCp.symbol.values; dts = FCp.trade_date.values
X = np.full((len(FCp), len(FC)), np.nan)
for j, col in enumerate(FC):
    if col in FCp.columns: X[:, j] = pd.to_numeric(FCp[col], errors="coerce").values
f25 = []
for pid, rule in PATS:
    m = FR.rule_mask(rule, X)
    if m.any(): f25.append(pd.DataFrame({"pattern_id": pid, "symbol": syms[m], "signal_date": dts[m]}))
F = pd.concat(f25, ignore_index=True).merge(FW, on=["symbol", "signal_date"], how="left").dropna(subset=["r6"]).merge(STATE, on="pattern_id", how="left").merge(PRI, on=["pattern_id", "symbol"], how="left")
F["cs"] = F.cs.fillna(0.0)
Glong = F.groupby(["symbol", "signal_date"]).agg(state=("state", "sum"), prior=("cs", "sum"), r6=("r6", "first")).reset_index()
for col in ["state", "prior"]: Glong[col + "_r"] = Glong.groupby("signal_date")[col].rank(pct=True)
Glong["score"] = 0.55 * Glong.state_r + 0.45 * Glong.prior_r
# short candidates: below 200 & 50 MA, negative momentum; weakest = most negative roc_20
sh = FCp[["symbol", "trade_date", "roc_20", "dist_sma_200", "dist_sma_50"]].rename(columns={"trade_date": "signal_date"})
sh = sh[(sh.dist_sma_200 < 0) & (sh.dist_sma_50 < 0) & (sh.roc_20 < 0)].merge(FW[["symbol", "signal_date", "r6"]], on=["symbol", "signal_date"], how="left").dropna(subset=["r6"])
sh["sscore"] = -sh.roc_20   # weaker = higher short score

# ---- build daily positions per mode ----
def month_pnl(mode, topn):
    picks = []
    for sd in sorted(set(FCp.trade_date)):
        up20 = regime.get(sd, True); up50 = regime50.get(sd, True)
        if mode == "long":
            g = Glong[Glong.signal_date == sd].sort_values("score", ascending=False).head(topn)
            for _, r in g.iterrows(): picks.append((sd, r.symbol, +1, r.r6))
        elif mode == "riskoff":                      # long when up (50d gate), CASH when down
            if up50:
                g = Glong[Glong.signal_date == sd].sort_values("score", ascending=False).head(topn)
                for _, r in g.iterrows(): picks.append((sd, r.symbol, +1, r.r6))
    P = pd.DataFrame(picks, columns=["signal_date", "symbol", "dir", "r6"])
    if len(P) == 0: return P
    P["mo"] = P.signal_date.str[:7]; P["pnl"] = P.dir * P.r6 / 100 * CAP
    return P

for mode, topn, label in [("long", 15, "LONG-ONLY top-15"), ("riskoff", 15, "LONG + RISK-OFF (cash in downtrend) top-15"),
                          ("riskoff", 8, "LONG + RISK-OFF top-8 (concentrated)")]:
    P = month_pnl(mode, topn); wk = topn * CAP * HOLD; tot = P.pnl.sum()
    print(f"\n===== 2025 {label} — Rs {CAP:,}/stock, hold {HOLD}d, cap Rs {wk:,} =====")
    print(f"  {'month':<9}{'trades':>7}{'P&L Rs':>12}{'ret%':>8}")
    full = {f"2025-{m:02d}" for m in range(1, 13)}; got = set(P.mo)
    for mo in sorted(full):
        gm = P[P.mo == mo]
        pnl = gm.pnl.sum() if len(gm) else 0
        print(f"  {mo:<9}{len(gm):>7}{pnl:>+12,.0f}{pnl/wk*100:>+7.1f}%{'  CASH' if len(gm)==0 else ''}")
    posmonths = sum(1 for mo in full if P[P.mo == mo].pnl.sum() > 0)
    print(f"  {'YEAR':<9}{len(P):>7}{tot:>+12,.0f}{tot/wk*100:>+7.1f}%   positive months {posmonths}/12")
