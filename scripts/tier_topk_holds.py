"""Top-30 dip patterns + quality gate + HIGH-tier overlay -> D1..D6 hold return view (WR + avg return),
D1 also at 5X. Rank patterns 2022-23, evaluate 2024 (OOS) and full 2022-24. Deduped stock-days. Leak-free.
"""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
K = 30; HOLDS = [1, 2, 3, 4, 5, 6]
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}
def _ok(v): return v is not None and not (isinstance(v, float) and v != v)
def tier(sr, tr, tn):
    if _ok(sr) and sr > 10: return "AVOID"
    if _ok(sr) and sr > 7 and _ok(tn) and tn >= 0.75: return "AVOID"
    if _ok(sr) and sr <= 2 and _ok(tr) and tr < 0.9: return "ENTERPRISE-Dryup"
    if _ok(sr) and sr <= 2 and _ok(tn) and tn < 0.75: return "GOLD"
    if _ok(sr) and sr <= 2: return "GOLD-baseline"
    if _ok(sr) and sr <= 5: return "STANDARD"
    return "STANDARD-weak"
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
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close,volume FROM ohlc_daily WHERE trade_date>='2021-06-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con)
qf = pd.read_sql_query("SELECT symbol,trade_date,dist_sma_200,slope_sma_50 FROM falcon_features WHERE trade_date>='2022-01-01' AND trade_date<='2024-12-31'", con); con.close()

fw = []; trows = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); v = g.volume.values.astype(float); td = g.trade_date.values; n = len(c)
    entry = np.roll(o, -1); entry[-1] = np.nan; d = {"symbol": s, "signal_date": td}
    for N in HOLDS:
        ex = np.roll(c, -N); ex[-N:] = np.nan; d[f"r{N}"] = (ex - entry) / entry * 100
    fw.append(pd.DataFrame(d))
    pc = np.roll(c, 1); pc[0] = np.nan; sret = (c / pc - 1) * 100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values; tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    turn = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    trows.append(pd.DataFrame({"symbol": s, "signal_date": td, "tier": [tier(sret[i], tr3[i], turn[i]) for i in range(n)]}))
FW = pd.concat(fw, ignore_index=True); TT = pd.concat(trows, ignore_index=True)

L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]]
L = L[(L.pattern_id.isin(DIP)) & (L.signal_date >= "2022-01-01")]
q = qf[(qf.dist_sma_200 > 0) & (qf.slope_sma_50 > 0)][["symbol", "trade_date"]].rename(columns={"trade_date": "signal_date"})
Q = L.merge(q, on=["symbol", "signal_date"], how="inner").merge(FW, on=["symbol", "signal_date"], how="left").merge(TT, on=["symbol", "signal_date"], how="left").dropna(subset=["r6"])
Q["yr"] = Q.signal_date.str[:4]; Q["high"] = Q.tier.isin(HIGH)
# rank dip patterns on 2022-23 (high-tier, quality), top-30
tr = Q[(Q.yr <= "2023") & (Q.high)]; pr = tr.groupby("pattern_id").agg(n=("r6", "size"), avg=("r6", "mean")).reset_index()
top = set(pr[pr.n >= 40].sort_values("avg", ascending=False).pattern_id.tolist()[:K])
print(f"top-{K} dip patterns selected. Overlay: quality gate + HIGH tier.\n", flush=True)

def show(df, tag):
    d = df.drop_duplicates(["symbol", "signal_date"])
    tdays = d.signal_date.nunique()
    print(f"== {tag} ==  qualifying stock-days {len(d):,} · stocks {d.symbol.nunique()} · ~{len(d)/tdays:.0f}/day")
    print(f"  {'hold':<7}{'WR':>7}{'avg 1X':>9}{'avg 5X':>9}")
    for N in HOLDS:
        x = d[f"r{N}"].dropna(); fivex = f"{x.mean()*5:>+7.2f}%" if N == 1 else "   (positional)"
        print(f"  D{N:<6}{(x>0).mean()*100:>6.0f}%{x.mean():>+8.2f}%{fivex:>9}")
    print()
sel24 = Q[(Q.pattern_id.isin(top)) & (Q.yr == "2024") & (Q.high)]
selall = Q[(Q.pattern_id.isin(top)) & (Q.high)]
show(sel24, "2024 (out-of-sample)")
show(selall, "2022-2024 (full)")
