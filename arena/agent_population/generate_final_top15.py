"""FINAL leak-free Falcon TOP-15 + TIER for Jan 2026 and Jul 2026, REBUILT (not read) via the production engine.

  JULY : stored falcon_features are live-era week-to-date = already leak-free -> rebuild on stored features
         (reproduces the live signal 10/10 -- validated bit-exact vs falcon_signals_live).
  JAN  : stored falcon_features are BACKFILL-leaked (Mon carries Fri's weekly value) -> rebuild on
         week-to-date RECOMPUTED weekly features (same recompute proven vs July: 7/11 days exact, right
         stocks every day; the 4 diffs are the live 10:39am partial-bar snapshot, which does not apply to a
         clean historical reconstruction).

Rebuild engine = falcon_signal_replay.rank_for_date EXACTLY (eligible mined_year<yr; score=sum oos_lift;
n_fires>=10; top-100 by score; final rank by avg_lift=score/n_fires). TIER = production rulebook arithmetic.
Saves both sheets to Downloads. Read-only.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
TOPN = 15
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}


def classify(sret, twoday, rng, avg_lift, trend3_20, turn_pct):
    if sret is None or not np.isfinite(sret): return "UNKNOWN"
    if sret > 10: return "AVOID"
    if sret > 7 and np.isfinite(turn_pct or np.nan) and turn_pct >= 0.75: return "AVOID"
    if sret <= 2 and np.isfinite(twoday or np.nan) and twoday < -5 and avg_lift and avg_lift > 15: return "PREMIUM-Pullback"
    if sret <= 2 and np.isfinite(rng or np.nan) and rng < 2 and avg_lift and avg_lift > 15: return "PREMIUM-Compression"
    if sret <= 2 and np.isfinite(trend3_20 or np.nan) and trend3_20 < 0.9: return "ENTERPRISE-Dryup"
    if sret <= 2 and np.isfinite(turn_pct or np.nan) and turn_pct < 0.75: return "GOLD"
    if sret <= 2: return "GOLD-baseline"
    if sret <= 5: return "STANDARD"
    return "STANDARD-weak"


con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-11-01' AND trade_date<='2026-07-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-07-31' ORDER BY symbol,trade_date", con)
con.close()

# ---- point-in-time week-to-date weekly features (for JANUARY) + daily tier features (both months) ----
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values; l = g.low.values; v = g.volume.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c / pc - 1) * 100; rng = (h - l) / pc * 100; twoday = (c / c2 - 1) * 100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    tp = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    for i, d in enumerate(g.trade_date.values): TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])
PIT = pd.concat(rec, ignore_index=True)
FCleak = feat                                                        # JULY: stored features (clean, live-era)
FCpit = feat.drop(columns=WEEKLY).merge(PIT, on=["symbol", "trade_date"], how="left")   # JAN: recomputed


def rebuild(fc, day, min_fires=10, top_n=100):
    fd = fc[fc.trade_date == day]
    if fd.empty: return []
    syms = fd.symbol.values
    X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    yr = int(day[:4]); elig = [p for p in pats if int(p["mined_year"]) < yr]
    fire = np.zeros(len(syms), np.int32); score = np.zeros(len(syms))
    for p in elig:
        m = FR.rule_mask(p["rule"], X)
        if not m.any(): continue
        fire += m.astype(np.int32); score += m.astype(np.float64) * p["oos_lift"]
    cands = [{"symbol": syms[i], "n_fires": int(fire[i]), "score": float(score[i])}
             for i in range(len(syms)) if fire[i] >= min_fires]
    cands.sort(key=lambda c: -c["score"])
    ranked = sorted(cands[:top_n], key=lambda c: -(c["score"] / max(c["n_fires"], 1)))[:TOPN]
    rows = []
    for pos, c in enumerate(ranked, 1):
        al = c["score"] / max(c["n_fires"], 1); tf = TF.get((c["symbol"], day), (np.nan,) * 5)
        tier = classify(tf[0], tf[2], tf[1], al, tf[3], tf[4])
        rows.append(dict(signal_date=day, rank=pos, symbol=c["symbol"], n_fires=c["n_fires"],
                         score=round(c["score"], 1), avg_lift=round(al, 2), tier=tier,
                         high_tier=("YES" if tier in HIGH else "")))
    return rows


months = {"January": ("2026-01-01", "2026-01-31", FCpit, "leak-free week-to-date recompute"),
          "July":    ("2026-07-01", "2026-07-31", FCleak, "live-era stored features (already clean)")}
DL = os.path.join(os.path.expanduser("~"), "Downloads"); os.makedirs(DL, exist_ok=True)
out = os.path.join(DL, "FALCON_TOP15_JAN_JUL_2026_REBUILT.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    for mname, (lo, hi, fc, how) in months.items():
        days = sorted(fc[(fc.trade_date >= lo) & (fc.trade_date <= hi)].trade_date.unique())
        allrows = []
        for d in days: allrows += rebuild(fc, d)
        df = pd.DataFrame(allrows); df.to_excel(w, mname, index=False)
        tc = df.tier.value_counts().to_dict()
        print(f"{mname}: {len(days)} days, {len(df)} picks  [{how}]")
        print("   tiers:", tc)
        print(df[df.signal_date == days[-1]][["rank", "symbol", "n_fires", "avg_lift", "tier"]].to_string(index=False))
        print()
print(f"Saved -> {out}")
