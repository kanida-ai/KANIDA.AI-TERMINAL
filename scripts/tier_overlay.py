"""Overlay the operator TIER classification on the dip + quality-gate union, rerun WR/return on 2024 (OOS),
deduped one-position-per-stock-per-day. Also break WR down by tier. Leak-free (all tier inputs trailing/PIT).
"""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
HOLD = 6
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}
def _ok(v): return v is not None and not (isinstance(v, float) and v != v)
def tier(sr, td, rng, tr, tn):
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
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2021-06-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con)
qf = pd.read_sql_query("SELECT symbol,trade_date,dist_sma_200,slope_sma_50 FROM falcon_features WHERE trade_date>='2022-01-01' AND trade_date<='2024-12-31'", con); con.close()

SYM = {}; fw = []; trows = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float)
    h = g.high.values.astype(float); l = g.low.values.astype(float); v = g.volume.values.astype(float); td = g.trade_date.values; n = len(c)
    SYM[s] = (o, c, list(td), {d: i for i, d in enumerate(td)})
    entry = np.roll(o, -1); entry[-1] = np.nan; ex = np.roll(c, -HOLD); ex[-HOLD:] = np.nan
    fw.append(pd.DataFrame({"symbol": s, "signal_date": td, "r6": (ex - entry) / entry * 100}))
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c / pc - 1) * 100; twoday = (c / c2 - 1) * 100
    dt = pd.to_datetime(g.trade_date); wk = (dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)).values
    ser = pd.Series(wk); grp = ser.ne(ser.shift()).cumsum().values; rng = np.full(n, np.nan)
    for gg in np.unique(grp):
        ii = np.where(grp == gg)[0]; wh = np.maximum.accumulate(h[ii]); wl = np.minimum.accumulate(l[ii]); rng[ii] = (wh - wl) / c[ii] * 100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values; tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    turn = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    tiers = [tier(sret[i], twoday[i], rng[i], tr3[i], turn[i]) for i in range(n)]
    trows.append(pd.DataFrame({"symbol": s, "signal_date": td, "tier": tiers}))
FW = pd.concat(fw, ignore_index=True); TT = pd.concat(trows, ignore_index=True)

L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]]
L = L[(L.pattern_id.isin(DIP)) & (L.signal_date >= "2022-01-01")]
q = qf[(qf.dist_sma_200 > 0) & (qf.slope_sma_50 > 0)][["symbol", "trade_date"]].rename(columns={"trade_date": "signal_date"})
Q = L.merge(q, on=["symbol", "signal_date"], how="inner").merge(FW, on=["symbol", "signal_date"], how="left").merge(TT, on=["symbol", "signal_date"], how="left").dropna(subset=["r6"])
Q["yr"] = Q.signal_date.str[:4]; Q["high"] = Q.tier.isin(HIGH)
print(f"quality-gated dip fires 2022+ {len(Q):,} · HIGH-tier share {Q.high.mean()*100:.0f}%", flush=True)

# tier breakdown (2024, deduped by stock-day)
u24 = Q[Q.yr == "2024"].drop_duplicates(["symbol", "signal_date", "tier"]).drop_duplicates(["symbol", "signal_date"])
print("\nWR & return by TIER (2024, quality-gated dip stock-days):")
print(f"  {'tier':<22}{'n':>7}{'WR':>7}{'avg6d':>8}")
for t, g in u24.groupby("tier"):
    print(f"  {t:<22}{len(g):>7}{(g.r6>0).mean()*100:>6.0f}%{g.r6.mean():>+7.2f}%")

# rank patterns on 2022-23, sweep K, WITH high-tier overlay, eval 2024 deduped
tr = Q[(Q.yr <= "2023") & (Q.high)]
pr = tr.groupby("pattern_id").agg(n=("r6", "size"), avg=("r6", "mean")).reset_index()
pr = pr[pr.n >= 40].sort_values("avg", ascending=False); ranked = pr.pattern_id.tolist()
def port(qual_df):
    dayrows = []
    for s, gg in qual_df.groupby("symbol"):
        if s not in SYM: continue
        o, c, td, idx = SYM[s]; n = len(c); pos = [idx[d] for d in gg.signal_date if d in idx]
        held = np.zeros(n, bool)
        for p in pos:
            for k in range(1, HOLD + 1):
                if p + k < n: held[p + k] = True
        for j in range(n):
            if not held[j] or td[j][:4] != "2024": continue
            if j - 1 >= 0 and held[j - 1] and c[j - 1] > 0: dayrows.append((td[j], (c[j] - c[j - 1]) / c[j - 1] * 100))
            elif o[j] > 0: dayrows.append((td[j], (c[j] - o[j]) / o[j] * 100))
    if not dayrows: return 0, 0, 0
    D = pd.DataFrame(dayrows, columns=["date", "ret"]); p = D.groupby("date").ret.agg(["mean", "size"])
    return p["size"].mean(), p["mean"].mean(), (np.prod(1 + p["mean"] / 100) - 1) * 100
print("\nFRONTIER with HIGH-TIER overlay (rank 2022-23, test 2024, 1X, deduped):")
print(f"  {'K':>4}{'stocks/day':>12}{'WR24':>7}{'avg6d24':>9}{'daily%':>9}{'2024 cum%':>11}")
for K in [10, 20, 40, 80, len(ranked)]:
    top = set(ranked[:K])
    te = Q[(Q.pattern_id.isin(top)) & (Q.yr == "2024") & (Q.high)].drop_duplicates(["symbol", "signal_date"])
    spd, daily, cum = port(te[["symbol", "signal_date"]])
    print(f"  {K:>4}{spd:>11.0f}{(te.r6>0).mean()*100:>6.0f}%{te.r6.mean():>+8.2f}%{daily:>+8.2f}%{cum:>+10.1f}%")
