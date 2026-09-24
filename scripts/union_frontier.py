"""Frontier: sweep K = top-K most-consistent DIP patterns (+ quality gate). Rank patterns on 2022-2023 (train),
evaluate the union on 2024 (test, out-of-selection). Corrected accounting: ONE position per stock per day.
Shows WR vs stocks/day vs daily-return for each K.
"""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
HOLD = 6
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
prows = con.execute("SELECT c.pattern_id, c.rule_json FROM falcon_pattern_candidates c JOIN falcon_promoted_patterns p ON c.pattern_id=p.pattern_id").fetchall()
def is_dip(rule):
    for f, op, th in rule:
        if f == "rsi_14" and op == "<=" and th <= 50: return True
        if f in ("roc_5", "roc_20", "roc_60") and op == "<=" and th <= 3: return True
        if f in ("weekly_close_loc", "close_loc") and op == "<=" and th <= 0.5: return True
        if f in ("dist_high_10", "dist_high_20", "dist_high_60", "dist_high_120", "dist_high_252") and op == "<=" and th <= -5: return True
    return False
DIP = {pid for pid, rj in prows if is_dip(json.loads(rj))}
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>='2021-06-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con)
qf = pd.read_sql_query("SELECT symbol,trade_date,dist_sma_200,slope_sma_50 FROM falcon_features WHERE trade_date>='2022-01-01' AND trade_date<='2024-12-31'", con); con.close()

# r6 per (symbol,date) + per-symbol arrays
SYM = {}
fw = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values
    SYM[s] = (o, c, list(td), {d: i for i, d in enumerate(td)})
    entry = np.roll(o, -1); entry[-1] = np.nan; ex = np.roll(c, -HOLD); ex[-HOLD:] = np.nan
    fw.append(pd.DataFrame({"symbol": s, "signal_date": td, "r6": (ex - entry) / entry * 100}))
FW = pd.concat(fw, ignore_index=True)

L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]]
L = L[(L.pattern_id.isin(DIP)) & (L.signal_date >= "2022-01-01")]
q = qf[(qf.dist_sma_200 > 0) & (qf.slope_sma_50 > 0)][["symbol", "trade_date"]].rename(columns={"trade_date": "signal_date"})
Q = L.merge(q, on=["symbol", "signal_date"], how="inner").merge(FW, on=["symbol", "signal_date"], how="left").dropna(subset=["r6"])
Q["yr"] = Q.signal_date.str[:4]
print(f"dip patterns {len(DIP)} · quality-gated dip fires 2022+ {len(Q):,}", flush=True)

# rank patterns on 2022-2023 (train) by avg 6-day return, min support
tr = Q[Q.yr <= "2023"]
pr = tr.groupby("pattern_id").agg(n=("r6", "size"), avg=("r6", "mean"), wr=("r6", lambda x: (x > 0).mean() * 100)).reset_index()
pr = pr[pr.n >= 40].sort_values("avg", ascending=False)
ranked = pr.pattern_id.tolist()
print(f"rankable dip patterns (>=40 train fires): {len(ranked)}\n", flush=True)

def port(qual_df):  # qual_df: qualifying (symbol, signal_date) with S in target year; returns daily portfolio stats
    dayrows = []
    for s, gg in qual_df.groupby("symbol"):
        if s not in SYM: continue
        o, c, td, idx = SYM[s]; n = len(c)
        pos = [idx[d] for d in gg.signal_date if d in idx]
        held = np.zeros(n, bool)
        for p in pos:
            for k in range(1, HOLD + 1):
                if p + k < n: held[p + k] = True
        for j in range(n):
            if not held[j]: continue
            if td[j][:4] != "2024": continue
            if j - 1 >= 0 and held[j - 1] and c[j - 1] > 0: dayrows.append((td[j], (c[j] - c[j - 1]) / c[j - 1] * 100))
            elif o[j] > 0: dayrows.append((td[j], (c[j] - o[j]) / o[j] * 100))
    if not dayrows: return 0, 0, 0
    D = pd.DataFrame(dayrows, columns=["date", "ret"]); p = D.groupby("date").ret.agg(["mean", "size"])
    return p["size"].mean(), p["mean"].mean(), (np.prod(1 + p["mean"] / 100) - 1) * 100

print("FRONTIER (patterns ranked on 2022-23, evaluated on 2024, 1X, 6-day hold, deduped):")
print(f"  {'K':>4}{'stocks/day':>12}{'trade WR24':>12}{'avg6d24':>10}{'daily%':>9}{'2024 cum%':>11}")
for K in [5, 10, 20, 40, 80, len(ranked)]:
    top = set(ranked[:K])
    te = Q[(Q.pattern_id.isin(top)) & (Q.yr == "2024")].drop_duplicates(["symbol", "signal_date"])
    spd, daily, cum = port(te[["symbol", "signal_date"]])
    wr = (te.r6 > 0).mean() * 100; avg = te.r6.mean()
    print(f"  {K:>4}{spd:>11.0f}{wr:>11.0f}%{avg:>+9.2f}%{daily:>+8.2f}%{cum:>+10.1f}%")
