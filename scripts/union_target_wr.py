"""Get to 70% WR honestly via the EXIT: take-profit target within the 6-day window instead of always holding 6d.
Dip + quality union (top-20 patterns ranked on 2022-23), evaluated on 2024 (OOS) and full 2022-24.
For each target T: WR = P(touch +T within 6d) + P(not touched but day-6 close>0); avg return under target-exit;
daily return. Shows the WR-vs-return tradeoff so we pick the 70% point. Corrected accounting. Honest.
"""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
HOLD = 6; STOP = 6.0
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
prows = con.execute("SELECT c.pattern_id, c.rule_json FROM falcon_pattern_candidates c JOIN falcon_promoted_patterns p ON c.pattern_id=p.pattern_id").fetchall()
def is_dip(r):
    for f, op, th in r:
        if f == "rsi_14" and op == "<=" and th <= 50: return True
        if f in ("roc_5", "roc_20", "roc_60") and op == "<=" and th <= 3: return True
        if f in ("weekly_close_loc", "close_loc") and op == "<=" and th <= 0.5: return True
        if f.startswith("dist_high") and op == "<=" and th <= -5: return True
    return False
DIP = {pid for pid, rj in prows if is_dip(json.loads(rj))}
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2021-06-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con)
qf = pd.read_sql_query("SELECT symbol,trade_date,dist_sma_200,slope_sma_50 FROM falcon_features WHERE trade_date>='2022-01-01' AND trade_date<='2024-12-31'", con); con.close()

# per (symbol,S): entry, MFE, close6
rows = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values; n = len(c)
    for p in range(n - HOLD):
        e = o[p + 1]
        if e <= 0: continue
        win_h = h[p + 1:p + 1 + HOLD]; win_l = l[p + 1:p + 1 + HOLD]
        rows.append((s, td[p], (win_h.max() - e) / e * 100, (win_l.min() - e) / e * 100, (c[p + HOLD] - e) / e * 100))
M = pd.DataFrame(rows, columns=["symbol", "signal_date", "mfe", "mae", "close6"])

L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]]
L = L[(L.pattern_id.isin(DIP)) & (L.signal_date >= "2022-01-01")]
q = qf[(qf.dist_sma_200 > 0) & (qf.slope_sma_50 > 0)][["symbol", "trade_date"]].rename(columns={"trade_date": "signal_date"})
Q = L.merge(q, on=["symbol", "signal_date"], how="inner").merge(M, on=["symbol", "signal_date"], how="left").dropna(subset=["close6"])
Q["yr"] = Q.signal_date.str[:4]
# rank dip patterns on 2022-23, keep top-20
tr = Q[Q.yr <= "2023"]; pr = tr.groupby("pattern_id").agg(n=("close6", "size"), a=("close6", "mean")).reset_index()
top = set(pr[pr.n >= 40].sort_values("a", ascending=False).head(20).pattern_id)
Q = Q[Q.pattern_id.isin(top)].drop_duplicates(["symbol", "signal_date"])
print(f"top-20 dip+quality union trades: {len(Q):,} (2024: {int((Q.yr=='2024').sum()):,})\n")

def analyze(df, tag):
    print(f"== {tag} ({len(df):,} trades) ==")
    print(f"  {'exit rule':<22}{'WR':>6}{'avg ret':>9}{'avg/6 daily':>13}")
    # baseline: always hold 6 days
    wr0 = (df.close6 > 0).mean() * 100
    print(f"  {'hold 6d (no target)':<22}{wr0:>5.0f}%{df.close6.mean():>+8.2f}%{df.close6.mean()/6:>+12.2f}%")
    for T in [1.5, 2, 2.5, 3, 4, 5]:
        hit = df.mfe >= T
        # win = hit target OR (not hit but closed positive); with wide stop: if mae<=-STOP and not hit -> stop loss
        stopped = (~hit) & (df.mae <= -STOP)
        ret = np.where(hit, T, np.where(stopped, -STOP, df.close6))
        win = (ret > 0)
        print(f"  {'target +' + str(T) + '% (stop ' + str(STOP) + ')':<22}{win.mean()*100:>5.0f}%{ret.mean():>+8.2f}%{ret.mean()/6:>+12.2f}%")
    print()

analyze(Q[Q.yr == "2024"], "2024 (out-of-selection)")
analyze(Q, "2022-2024 full")
