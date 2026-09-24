"""Rebuild the Falcon signal with POINT-IN-TIME (week-to-date) weekly features, using the exact production
scoring/ranking engine (falcon_signal_replay). Then VALIDATE the rebuild against the stored July signal:
if the point-in-time rebuild reproduces July's live ranks, the method is leak-free AND correct, so it can be
trusted for January (where nothing is stored). Read-only.

Engine = FR.rank_for_date logic exactly:
  eligible patterns: mined_year < signal_year
  score = sum(oos_lift) over firing patterns ; n_fires = count
  candidates: n_fires >= min_fires ; take top-100 by score ; FINAL rank by avg_lift = score/n_fires
The ONLY change vs stored-feature replay: the 4 weekly_* columns are recomputed week-to-date (no leak).
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
# stored features for the whole span we test/generate
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-11-01' AND trade_date<='2026-07-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-07-31' ORDER BY symbol,trade_date", con)
con.close()

# ---- point-in-time week-to-date weekly features (exactly the live-cron construction) ----
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
PIT = pd.concat(rec, ignore_index=True)
# feature frame with weekly cols REPLACED by point-in-time values
FCpit = feat.drop(columns=WEEKLY).merge(PIT, on=["symbol", "trade_date"], how="left")


def rebuild(fc, signal_date, min_fires=10, top_n=100):
    """FR.rank_for_date, but sourcing the feature matrix from dataframe `fc` (which carries PIT weeklies)."""
    fd = fc[fc.trade_date == signal_date]
    if fd.empty: return None
    syms = fd.symbol.values
    X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    yr = int(signal_date[:4]); elig = [p for p in pats if int(p["mined_year"]) < yr]
    fire = np.zeros(len(syms), np.int32); score = np.zeros(len(syms))
    for p in elig:
        m = FR.rule_mask(p["rule"], X)
        if not m.any(): continue
        fire += m.astype(np.int32); score += m.astype(np.float64) * p["oos_lift"]
    cands = [{"symbol": syms[i], "n_fires": int(fire[i]), "score": float(score[i])}
             for i in range(len(syms)) if fire[i] >= min_fires]
    cands.sort(key=lambda c: -c["score"]); top = cands[:top_n]
    ranked = sorted(top, key=lambda c: -(c["score"] / max(c["n_fires"], 1)))
    for pos, c in enumerate(ranked, 1):
        c["rank"] = pos; c["avg_lift"] = round(c["score"] / max(c["n_fires"], 1), 4)
    return ranked


# ---------- VALIDATION: point-in-time rebuild vs stored July signal ----------
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
jdays = [r[0] for r in con.execute("SELECT DISTINCT signal_date FROM falcon_signals_live ORDER BY signal_date")]
print("VALIDATION — point-in-time (leak-free) rebuild vs stored live July signal")
print("=" * 72)
tot_exact = tot_days = 0
for d in jdays:
    stored = {sym: rank for rank, sym in con.execute(
        "SELECT rank, symbol FROM falcon_signals_live WHERE signal_date=? ORDER BY rank", (d,))}
    st15 = [s for s, r in sorted(stored.items(), key=lambda kv: kv[1])][:15]
    rk = rebuild(FCpit, d)
    if not rk: continue
    rb15 = [c["symbol"] for c in rk[:15]]
    exact = sum(1 for a, b in zip(rb15, st15) if a == b)
    ov = len(set(rb15) & set(st15))
    tot_exact += exact; tot_days += 1
    print(f"  {d}: exact rank {exact:>2}/15   set overlap {ov:>2}/15")
print("=" * 72)
print(f"  MEAN exact-rank match across {tot_days} July days: {tot_exact/max(tot_days,1):.1f}/15")
print("\n  detail — 2026-07-24 point-in-time rebuild (leak-free) vs stored:")
rk = rebuild(FCpit, "2026-07-24")
stored = {sym: (rank, nf, sc) for rank, sym, nf, sc in con.execute(
    "SELECT rank,symbol,n_fires,score FROM falcon_signals_live WHERE signal_date='2026-07-24'")}
for c in rk[:15]:
    st = stored.get(c["symbol"])
    tag = f"stored(rank={st[0]},nf={st[1]},score={st[2]:.1f})" if st else "NOT IN STORED TOP"
    print(f"    rank {c['rank']:>2} {c['symbol']:12} nf={c['n_fires']:>3} score={c['score']:.1f}  vs {tag}")
con.close()
