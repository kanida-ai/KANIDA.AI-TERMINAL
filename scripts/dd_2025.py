"""2025 monthly return + drawdown for BOTH exits: (A) hold 6 days (let winners run, ~54% WR) and
(B) +3% take-profit / -6% stop (70% WR). Top-20 dip patterns ranked on 2022-23, quality gate, leak-free 2025
fires (PIT weekly). One position per stock at a time; daily equity curve -> monthly return + max drawdown.
"""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]; FC = FR.FEATURE_COLS
HOLD = 6; TGT = 3.0; STOP = 6.0

# ---- rank top-20 dip patterns on 2022-23 (quality-gated) ----
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
prows = con.execute("SELECT c.pattern_id, c.rule_json FROM falcon_pattern_candidates c JOIN falcon_promoted_patterns p ON c.pattern_id=p.pattern_id").fetchall()
RULES = {pid: [(f, op, th) for f, op, th in json.loads(rj)] for pid, rj in prows}
def is_dip(r):
    for f, op, th in r:
        if f == "rsi_14" and op == "<=" and th <= 50: return True
        if f in ("roc_5", "roc_20", "roc_60") and op == "<=" and th <= 3: return True
        if f in ("weekly_close_loc", "close_loc") and op == "<=" and th <= 0.5: return True
        if f.startswith("dist_high") and op == "<=" and th <= -5: return True
    return False
DIP = {pid for pid, r in RULES.items() if is_dip(r)}
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-02-28' ORDER BY symbol,trade_date", con)
qf24 = pd.read_sql_query("SELECT symbol,trade_date,dist_sma_200,slope_sma_50 FROM falcon_features WHERE trade_date>='2022-01-01' AND trade_date<='2023-12-31'", con)
fw = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values
    entry = np.roll(o, -1); entry[-1] = np.nan; ex = np.roll(c, -HOLD); ex[-HOLD:] = np.nan
    fw.append(pd.DataFrame({"symbol": s, "signal_date": td, "r6": (ex - entry) / entry * 100}))
FW = pd.concat(fw, ignore_index=True)
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]]
L = L[(L.pattern_id.isin(DIP)) & (L.signal_date >= "2022-01-01") & (L.signal_date <= "2023-12-31")]
q23 = qf24[(qf24.dist_sma_200 > 0) & (qf24.slope_sma_50 > 0)][["symbol", "trade_date"]].rename(columns={"trade_date": "signal_date"})
Q = L.merge(q23, on=["symbol", "signal_date"]).merge(FW, on=["symbol", "signal_date"], how="left").dropna(subset=["r6"])
pr = Q.groupby("pattern_id").agg(n=("r6", "size"), a=("r6", "mean")).reset_index()
TOP = pr[pr.n >= 40].sort_values("a", ascending=False).head(20).pattern_id.tolist()
print(f"top-20 dip patterns selected on 2022-23", flush=True)

# ---- 2025 fires (leak-free PIT weekly) + quality ----
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-11-01' AND trade_date<='2025-12-31'", con)
qf25 = pd.read_sql_query("SELECT symbol,trade_date,dist_sma_200,slope_sma_50 FROM falcon_features WHERE trade_date>='2025-01-01' AND trade_date<='2025-12-31'", con); con.close()
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
fire = np.zeros(len(FCp), bool)
for pid in TOP: fire |= FR.rule_mask(RULES[pid], X)
sig25 = pd.DataFrame({"symbol": syms[fire], "signal_date": dts[fire]}).drop_duplicates()
q25 = qf25[(qf25.dist_sma_200 > 0) & (qf25.slope_sma_50 > 0)][["symbol", "trade_date"]].rename(columns={"trade_date": "signal_date"})
sig25 = sig25.merge(q25, on=["symbol", "signal_date"])
print(f"2025 qualifying signals: {len(sig25):,}", flush=True)

# ---- per-stock simulation, one position at a time ----
qmap = {}
for s, sd in zip(sig25.symbol, sig25.signal_date): qmap.setdefault(s, set()).add(sd)
def simulate(version):
    rows = []
    for s, g in oh.groupby("symbol", sort=False):
        if s not in qmap: continue
        g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values; n = len(c)
        qidx = sorted(p for p in range(n) if td[p] in qmap[s]); last_exit = -1
        for p in qidx:
            e = p + 1
            if e <= last_exit or e >= n or o[e] <= 0: continue
            entry = o[e]
            for d in range(e, min(e + HOLD, n)):
                ref = entry if d == e else c[d - 1]
                if version == "B":
                    if l[d] <= entry * (1 - STOP / 100): rows.append((td[d], (entry * (1 - STOP / 100) - ref) / ref * 100)); last_exit = d; break
                    if h[d] >= entry * (1 + TGT / 100): rows.append((td[d], (entry * (1 + TGT / 100) - ref) / ref * 100)); last_exit = d; break
                rows.append((td[d], (c[d] - ref) / ref * 100))
                last_exit = d
    D = pd.DataFrame(rows, columns=["date", "ret"]); D = D[D.date.str[:4] == "2025"]
    daily = D.groupby("date").ret.mean().sort_index()
    eq = (1 + daily / 100).cumprod(); peak = eq.cummax(); dd = (eq / peak - 1) * 100
    return daily, eq, dd

def report(name, daily, eq, dd):
    print(f"\n===== {name} — 2025 monthly =====")
    print(f"  {'month':<9}{'return%':>9}{'end-DD%':>9}")
    mo = pd.DataFrame({"ret": daily, "dd": dd}); mo["m"] = mo.index.str[:7]
    for m, g in mo.groupby("m"):
        mret = ((1 + g["ret"] / 100).prod() - 1) * 100
        print(f"  {m:<9}{mret:>+8.1f}%{g['dd'].min():>+8.1f}%")
    tot = (eq.iloc[-1] - 1) * 100
    print(f"  {'YEAR':<9}{tot:>+8.1f}%{dd.min():>+8.1f}%  (max drawdown {dd.min():+.1f}%)")

dA, eA, ddA = simulate("A"); dB, eB, ddB = simulate("B")
report("A: hold 6 days (let winners run, ~54% WR)", dA, eA, ddA)
report("B: +3% target / -6% stop (70% WR)", dB, eB, ddB)
print(f"\n  A daily avg {dA.mean():+.3f}%  B daily avg {dB.mean():+.3f}%")
