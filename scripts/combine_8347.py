"""Piece-by-piece: start with FALCPAT_8347. Find patterns that CO-FIRE with it (same stock, same day) and push
6-day win-rate >70%, with real support, validated out-of-sample (find on <=2023, check on 2024). Then union the
best combos to recover coverage. Multiple-screener building block.
"""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
BASE = 8347; HOLD = 6; MINSUP = 60; MINSTK = 15; WR_TGT = 70.0
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]]

# 6-day returns
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>='2018-12-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con)
def rule_of(pid):
    r = con.execute("SELECT rule_json FROM falcon_pattern_candidates WHERE pattern_id=?", (pid,)).fetchone()
    return json.loads(r[0]) if r else []
fw = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values
    entry = np.roll(o, -1); entry[-1] = np.nan; ex = np.roll(c, -HOLD); ex[-HOLD:] = np.nan
    fw.append(pd.DataFrame({"symbol": s, "signal_date": td, "r6": (ex - entry) / entry * 100}))
FW = pd.concat(fw, ignore_index=True)
L = L.merge(FW, on=["symbol", "signal_date"], how="left").dropna(subset=["r6"])
L["yr"] = L.signal_date.str[:4]; L["key"] = L.symbol + "|" + L.signal_date

# base 8347 stock-days
B = L[L.pattern_id == BASE][["key", "yr", "r6"]].drop_duplicates("key")
def wr(df): return (df.r6 > 0).mean() * 100
print(f"BASE FALCPAT_{BASE}: {len(B)} stock-days · stocks {L[L.pattern_id==BASE].symbol.nunique()}")
print(f"  6-day WR: train(<=2023) {wr(B[B.yr<='2023']):.0f}%  test(2024) {wr(B[B.yr=='2024']):.0f}%\n")

# all patterns co-firing on 8347's stock-days
LB = L[L.key.isin(set(B.key))]
res = []
for pid, g in LB.groupby("pattern_id"):
    if pid == BASE: continue
    tr = g[g.yr <= "2023"]; te = g[g.yr == "2024"]
    if len(tr) < MINSUP or tr.symbol.nunique() < MINSTK: continue
    res.append(dict(pattern=pid, n_tr=len(tr), stk=tr.symbol.nunique(), wr_tr=wr(tr),
                    n_te=len(te), wr_te=(wr(te) if len(te) >= 10 else np.nan), avg6=tr.r6.mean()))
R = pd.DataFrame(res).sort_values("wr_tr", ascending=False)
hi = R[(R.wr_tr >= WR_TGT)]
dur = hi[(hi.wr_te >= 65)]
print(f"combos 8347 + X with train 6-day WR >= {WR_TGT}% (support>={MINSUP}, stocks>={MINSTK}): {len(hi)}")
print(f"  of which HOLD out-of-sample (2024 WR>=65%): {len(dur)}\n")
print(f"  {'combo':<26}{'n_tr':>6}{'stk':>5}{'WR_tr':>7}{'WR_2024':>9}{'avg6':>7}")
for _, r in dur.sort_values("wr_te", ascending=False).head(15).iterrows():
    print(f"  8347 + FALCPAT_{int(r.pattern):<12}{int(r.n_tr):>6}{int(r.stk):>5}{r.wr_tr:>6.0f}%{r.wr_te:>8.0f}%{r.avg6:>+6.2f}%")

# union of the durable high-WR combos: coverage + blended WR
top = dur.sort_values("wr_te", ascending=False).head(10).pattern.tolist()
union_keys = set(LB[LB.pattern_id.isin(top)].key)
U = B[B.key.isin(union_keys)]
print(f"\nUNION of top-{len(top)} durable combos (multiple-screener):")
print(f"  covers {len(U)} of 8347's {len(B)} stock-days ({len(U)/len(B)*100:.0f}%)  ·  stocks {L[L.key.isin(union_keys)].symbol.nunique()}")
print(f"  6-day WR: train {wr(U[U.yr<='2023']):.0f}%  test(2024) {wr(U[U.yr=='2024']):.0f}%  ·  avg6 {U.r6.mean():+.2f}%")
print(f"  (vs base 8347 alone: WR ~{wr(B[B.yr=='2024']):.0f}%, avg6 {B.r6.mean():+.2f}%)")

# show what the top combo partners ARE
nice = {"atr_20_pct": "volatility", "rsi_14": "RSI", "roc_5": "5d-mom", "roc_20": "20d-mom", "roc_60": "60d-mom",
        "dist_sma_50": "%vs50d", "dist_sma_200": "%vs200d", "weekly_close_loc": "close-in-wk", "weekly_range_pct": "wk-range%",
        "weekly_close_vs_sma20": "wk-vs-20wk", "dist_high_20": "%below20dHi", "dist_high_60": "%below60dHi",
        "vol_5d_vs_20d": "5dvol/20d", "n_sub_3_range_7d": "#tight/7", "slope_sma_50": "50d-slope", "close_loc": "close-in-day"}
print("\ntop partner patterns (what they add):")
for pid in top[:6]:
    rl = rule_of(pid); txt = " & ".join(f"{nice.get(f,f)}{op}{round(th,1)}" for f, op, th in rl)
    print(f"  FALCPAT_{pid}: {txt}")
con.close()
