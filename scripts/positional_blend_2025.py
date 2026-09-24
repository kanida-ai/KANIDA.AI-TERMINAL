"""Positional strategy, full blend, 2025 forward test (leak-free). Top-15/day, hold 6 days, Rs 25,000/stock (1X).
blend = 0.45*state + 0.35*prior + 0.20*memory - loser_penalty   (each rank-normalized per day)
  state  = sum over fired patterns of pattern's overall avg 6-day return (<=2024)     [raw EOD setup strength]
  prior  = tiered pattern-stock consistency score (<=2024)                            [stock-specific]
  memory = stock's trailing avg D1 over its fires in the prior 30 sessions            [self watchlist carry]
  penalty= stock's most recent prior fire lost > -1% D1                               [avoid repeat losers]
2025 fires computed fresh (PIT weekly). Monthly rupee P&L + max drawdown (one position per stock, daily equity).
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
# forward returns per (symbol, signal_date)
POS = {}; fw = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); td = list(g.trade_date.values)
    POS[s] = (o, c, td, {d: i for i, d in enumerate(td)})
    entry = np.roll(o, -1); entry[-1] = np.nan; ex = np.roll(c, -HOLD); ex[-HOLD:] = np.nan
    d1 = np.roll((c - o) / o * 100, -1); d1[-1] = np.nan
    fw.append(pd.DataFrame({"symbol": s, "signal_date": td, "d1": d1, "r6": (ex - entry) / entry * 100}))
FW = pd.concat(fw, ignore_index=True)

# ---- leak-free gate: only patterns MINED before 2025 ----
mrows = con.execute("SELECT c.pattern_id, c.mined_year FROM falcon_pattern_candidates c JOIN falcon_promoted_patterns p ON c.pattern_id=p.pattern_id").fetchall()
ELIG = {pid for pid, my in mrows if str(my).isdigit() and int(my) < 2025}
print(f"leak-free: patterns mined <2025 = {len(ELIG)} of 865 (excluded {865-len(ELIG)} mined in 2025)", flush=True)

# ---- prior (tiered) + state, from ledger <=2024 ----
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date", "next_oc"]].rename(columns={"next_oc": "d1"})
L = L[L.pattern_id.isin(ELIG)]
L = L.merge(FW[["symbol", "signal_date", "r6"]], on=["symbol", "signal_date"], how="left").dropna(subset=["r6"])
STATE = L.groupby("pattern_id").r6.mean().rename("state").reset_index()
Pp = L[L.signal_date <= "2024-12-31"].groupby(["pattern_id", "symbol"]).agg(n=("d1", "size"), avg=("d1", "mean")).reset_index()
def tm(r):
    if r.avg >= 10: return 0.0
    if r.avg >= 2 and r.n >= 10: return 1.0
    if 1 <= r.avg < 2 and r.n >= 20: return 0.65
    if r.avg >= 5 and r.n >= 5: return 0.35
    return 0.0
Pp["cs"] = np.log1p(Pp.n) * Pp.avg * Pp.apply(tm, axis=1)
PRI = Pp[Pp.cs > 0][["pattern_id", "symbol", "cs"]]

# ---- 2025 fires (leak-free PIT weekly) ----
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
syms = FCp.symbol.values; dts = FCp.trade_date.values
X = np.full((len(FCp), len(FC)), np.nan)
for j, col in enumerate(FC):
    if col in FCp.columns: X[:, j] = pd.to_numeric(FCp[col], errors="coerce").values
f25 = []
for pid, rule in PATS:
    m = FR.rule_mask(rule, X)
    if m.any(): f25.append(pd.DataFrame({"pattern_id": pid, "symbol": syms[m], "signal_date": dts[m]}))
F = pd.concat(f25, ignore_index=True).merge(FW, on=["symbol", "signal_date"], how="left").dropna(subset=["r6"])
F = F.merge(STATE, on="pattern_id", how="left").merge(PRI, on=["pattern_id", "symbol"], how="left")
F["cs"] = F.cs.fillna(0.0)
G = F.groupby(["symbol", "signal_date"]).agg(state=("state", "sum"), prior=("cs", "sum"), d1=("d1", "first"), r6=("r6", "first")).reset_index()

# ---- memory + penalty (leak-free, per stock trailing) using 2024Q4+2025 fires ----
hist = pd.concat([L[L.signal_date >= "2024-09-01"][["symbol", "signal_date", "d1"]], G[["symbol", "signal_date", "d1"]]]).drop_duplicates(["symbol", "signal_date"]).sort_values(["symbol", "signal_date"])
mem = {}; pen = {}
allcal = sorted(set(oh.trade_date)); cix = {d: i for i, d in enumerate(allcal)}
for s, g in hist.groupby("symbol"):
    g = g.sort_values("signal_date").reset_index(drop=True); ds = g.signal_date.values; dd = g.d1.values
    idxs = [cix.get(d, -1) for d in ds]
    for k in range(len(ds)):
        past = [(idxs[j], dd[j]) for j in range(k) if idxs[k] - idxs[j] >= 2 and idxs[k] - idxs[j] <= 30]
        mem[(s, ds[k])] = np.mean([v for _, v in past]) if past else 0.0
        prior_fire = [dd[j] for j in range(k) if idxs[k] - idxs[j] >= 2]
        pen[(s, ds[k])] = 1.0 if (prior_fire and prior_fire[-1] < -1.0) else 0.0
G["mem"] = [mem.get((s, d), 0.0) for s, d in zip(G.symbol, G.signal_date)]
G["pen"] = [pen.get((s, d), 0.0) for s, d in zip(G.symbol, G.signal_date)]
# rank-normalize per day, blend
for col in ["state", "prior", "mem"]: G[col + "_r"] = G.groupby("signal_date")[col].rank(pct=True)
G["score"] = 0.45 * G.state_r + 0.35 * G.prior_r + 0.20 * G.mem_r - 0.5 * G.pen
G["mo"] = G.signal_date.str[:7]
print(f"2025 scored stock-days: {len(G):,}", flush=True)

# ---- top-15/day, Rs25k, hold 6d: monthly rupee P&L + daily-equity drawdown ----
picks = pd.concat([d.sort_values("score", ascending=False).head(TOPN) for _, d in G.groupby("signal_date")])
picks["pnl"] = picks.r6 / 100 * CAP
# daily equity (one position per stock, mark daily) for drawdown
qmap = {}
for s, sd in zip(picks.symbol, picks.signal_date): qmap.setdefault(s, set()).add(sd)
dayr = []
for s, st in qmap.items():
    if s not in POS: continue
    o, c, td, idx = POS[s]; n = len(c); held = np.zeros(n, bool)
    for sd in st:
        if sd in idx:
            p = idx[sd]
            for k in range(1, HOLD + 1):
                if p + k < n: held[p + k] = True
    for j in range(n):
        if not held[j] or td[j][:4] != "2025": continue
        if j - 1 >= 0 and held[j - 1] and c[j - 1] > 0: dayr.append((td[j], (c[j] - c[j - 1]) / c[j - 1] * 100))
        elif o[j] > 0: dayr.append((td[j], (c[j] - o[j]) / o[j] * 100))
DR = pd.DataFrame(dayr, columns=["date", "ret"]); dp = DR.groupby("date").ret.mean().sort_index()
eq = (1 + dp / 100).cumprod(); dd = (eq / eq.cummax() - 1) * 100
wk = TOPN * CAP * HOLD
print(f"\n===== 2025 FORWARD — positional blend, Rs {CAP:,}/stock, top-{TOPN}, hold {HOLD}d, working cap Rs {wk:,} =====")
print(f"  {'month':<9}{'trades':>7}{'P&L Rs':>12}{'ret%':>8}{'maxDD%':>8}")
for mo, gm in picks.groupby("mo"):
    ddm = dd[dd.index.str[:7] == mo]
    print(f"  {mo:<9}{len(gm):>7}{gm.pnl.sum():>+12,.0f}{gm.pnl.sum()/wk*100:>+7.1f}%{(ddm.min() if len(ddm) else 0):>+7.1f}%")
tot = picks.pnl.sum()
print(f"  {'YEAR':<9}{len(picks):>7}{tot:>+12,.0f}{tot/wk*100:>+7.1f}%{dd.min():>+7.1f}%   (avg Rs/trade {tot/len(picks):+,.0f}, WR {(picks.r6>0).mean()*100:.0f}%)")
