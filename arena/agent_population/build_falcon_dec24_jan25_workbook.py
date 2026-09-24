"""Dec-2024 + Jan-2025 Falcon workbook (3 sheets), LEAK-FREE point-in-time rebuild.

Sheet 'signals'  : full daily board. Ranks 1..100 are the VALIDATED production board
                   (n_fires>=10 -> top-100 by score -> re-rank by avg_lift); ranks 101+ are the
                   remaining eligible tail ordered by avg_lift (extra depth, clearly beyond the
                   validated top-100). Cols: signal_date, rank, symbol, n_fires, score, avg_lift, tier, high_tier.
Sheet 'overlay'  : operator's real Dec+Jan picks (explicit manual rank) vs the rebuilt Falcon rank + features.
                   Cols: trade_date, signal_date, symbol, my_rank, falcon_rank, avg_lift, n_fires,
                   cut15_avglift, gap_to_top15, atr_20_pct, weekly_close_loc, weekly_range_pct,
                   weekly_close_vs_sma20, roc_20, roc_60, rsi_14, dist_high_20, dist_sma_200, slope_sma_50.
                   falcon_rank/avg_lift/n_fires use the VALIDATED top-100 board (matches worked examples:
                   FIVESTAR 24, PNBHOUSING 25, POLICYBZR 27). gap_to_top15 = cut15_avglift - avg_lift.
                   weekly_* are PIT (leak-free); the rest are stored daily technicals as-of signal_date.
Sheet 'patterns' : per (signal_date, symbol) fired patterns for the overlay picks only.
                   Cols: signal_date, symbol, pattern, target, oos_lift, rule.

Read-only on DB. Saves one .xlsx (Downloads or --out).
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")

ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}
# stored daily technicals surfaced in the overlay (weekly_* come from the PIT recompute instead)
DAILY_FEATS = ["atr_20_pct", "roc_20", "roc_60", "rsi_14", "dist_high_20", "dist_sma_200", "slope_sma_50"]
SIG_LO, SIG_HI = "2024-12-01", "2025-01-31"     # board (signals sheet) signal_date window
OUT = sys.argv[sys.argv.index("--out") + 1] if "--out" in sys.argv else \
    os.path.join(os.path.expanduser("~"), "Downloads", "FALCON_DEC24_JAN25_WORKBOOK.xlsx")


def _ok(v): return v is not None and not (isinstance(v, float) and v != v)
def classify(sret, twoday, rng, al, tr, turn):
    if _ok(sret) and sret > 10: return "AVOID"
    if _ok(sret) and sret > 7 and _ok(turn) and turn >= 0.75: return "AVOID"
    if _ok(sret) and sret <= 2 and _ok(twoday) and twoday < -5 and _ok(al) and al > 15: return "PREMIUM-Pullback"
    if _ok(sret) and sret <= 2 and _ok(rng) and rng < 2 and _ok(al) and al > 15: return "PREMIUM-Compression"
    if _ok(sret) and sret <= 2 and _ok(tr) and tr < 0.9: return "ENTERPRISE-Dryup"
    if _ok(sret) and sret <= 2 and _ok(turn) and turn < 0.75: return "GOLD"
    if _ok(sret) and sret <= 2: return "GOLD-baseline"
    if _ok(sret) and sret <= 5: return "STANDARD"
    return "STANDARD-weak"
def rulestr(rule): return " & ".join(f"{f}{op}{round(th, 2)}" for f, op, th in rule)


# ---------------- load ----------------
picks = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})[["trade_date", "symbol", "manual_rank"]]
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query(
    "SELECT * FROM falcon_features WHERE trade_date>='2024-11-15' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query(
    "SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
    "WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()
print(f"patterns={len(pats)}  feature rows={len(feat)}  ohlc rows={len(oh)}  operator picks={len(picks)}")

# ---------------- PIT weekly + tier inputs ----------------
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float)
    l = g.low.values.astype(float); v = g.volume.values.astype(float)
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
    for i, d in enumerate(g.trade_date.values):
        TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])
FCpit = feat.drop(columns=[w for w in WEEKLY if w in feat.columns]).merge(
    pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")

cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
def prev_td(d):
    i = cidx.get(d)
    return cal[i - 1] if (i is not None and i - 1 >= 0) else None

# ---------------- board per signal_date (validated 1..100 + eligible tail) ----------------
def board(day):
    """Return list of dicts (rank, symbol, n_fires, score, avg_lift, tier, high_tier) for all eligible."""
    fd = FCpit[FCpit.trade_date == day]
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
    cands = [{"symbol": syms[i], "n_fires": int(fire[i]), "score": float(score[i]),
              "avg_lift": float(score[i]) / max(int(fire[i]), 1)}
             for i in range(len(syms)) if fire[i] >= 10]
    cands.sort(key=lambda c: -c["score"])
    top = sorted(cands[:100], key=lambda c: -c["avg_lift"])          # validated ranks 1..100
    top_syms = {c["symbol"] for c in top}
    tail = sorted([c for c in cands if c["symbol"] not in top_syms], key=lambda c: -c["avg_lift"])
    rows = []
    for pos, c in enumerate(top + tail, 1):
        tf = TF.get((c["symbol"], day), (np.nan,) * 5)
        tier = classify(tf[0], tf[2], tf[1], c["avg_lift"], tf[3], tf[4])
        rows.append(dict(signal_date=day, rank=pos, symbol=c["symbol"], n_fires=c["n_fires"],
                         score=round(c["score"], 1), avg_lift=round(c["avg_lift"], 2), tier=tier,
                         high_tier=("YES" if tier in HIGH else "")))
    return rows

# signal_dates needed = board window (Dec1..Jan31) U prev-day of every trade_date in the log
sig_days = set(d for d in cal if SIG_LO <= d <= SIG_HI)
for td in picks.trade_date.unique():
    s = prev_td(td)
    if s: sig_days.add(s)
sig_days = sorted(sig_days)
boards = {d: board(d) for d in sig_days}

# ---------------- Sheet 1: signals (board window only) ----------------
sig_rows = []
for d in sig_days:
    if SIG_LO <= d <= SIG_HI:
        sig_rows += boards[d]
S1 = pd.DataFrame(sig_rows, columns=["signal_date", "rank", "symbol", "n_fires", "score",
                                     "avg_lift", "tier", "high_tier"])

# ---------------- Sheet 2: overlay ----------------
def board_lookup(day):
    by = {r["symbol"]: r for r in boards.get(day, [])}
    ranked = [r for r in boards.get(day, []) if r["rank"] <= 100]     # validated 100 for rank/cut15
    cut15 = next((r["avg_lift"] for r in ranked if r["rank"] == 15), np.nan)
    return by, cut15

ov_rows = []
for _, p in picks.iterrows():
    td, sym, mr = p.trade_date, p.symbol, int(p.manual_rank)
    sd = prev_td(td)
    by, cut15 = board_lookup(sd) if sd else ({}, np.nan)
    r = by.get(sym)
    frank = r["rank"] if (r and r["rank"] <= 100) else None            # only report the validated 1..100 rank
    al = r["avg_lift"] if r else np.nan
    nf = r["n_fires"] if r else np.nan
    gap = (cut15 - al) if (_ok(cut15) and _ok(al)) else np.nan
    fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == sym)] if sd else FCpit.iloc[0:0]
    def gf(col):
        if fr.empty or col not in fr.columns: return np.nan
        return pd.to_numeric(fr[col].iloc[0], errors="coerce")
    ov_rows.append(dict(
        trade_date=td, signal_date=sd, symbol=sym, my_rank=mr, falcon_rank=frank,
        avg_lift=(round(al, 2) if _ok(al) else np.nan), n_fires=(int(nf) if _ok(nf) else np.nan),
        cut15_avglift=(round(cut15, 2) if _ok(cut15) else np.nan),
        gap_to_top15=(round(gap, 2) if _ok(gap) else np.nan),
        atr_20_pct=gf("atr_20_pct"),
        weekly_close_loc=gf("weekly_close_loc"), weekly_range_pct=gf("weekly_range_pct"),
        weekly_close_vs_sma20=gf("weekly_close_vs_sma20"),
        roc_20=gf("roc_20"), roc_60=gf("roc_60"), rsi_14=gf("rsi_14"),
        dist_high_20=gf("dist_high_20"), dist_sma_200=gf("dist_sma_200"), slope_sma_50=gf("slope_sma_50")))
S2 = pd.DataFrame(ov_rows, columns=["trade_date", "signal_date", "symbol", "my_rank", "falcon_rank",
    "avg_lift", "n_fires", "cut15_avglift", "gap_to_top15", "atr_20_pct", "weekly_close_loc",
    "weekly_range_pct", "weekly_close_vs_sma20", "roc_20", "roc_60", "rsi_14", "dist_high_20",
    "dist_sma_200", "slope_sma_50"])
for col in ["atr_20_pct", "weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20",
            "roc_20", "roc_60", "rsi_14", "dist_high_20", "dist_sma_200", "slope_sma_50"]:
    S2[col] = S2[col].astype(float).round(2)

# ---------------- Sheet 3: patterns (overlay picks only) ----------------
def fired(sd, sym):
    fr = FCpit[(FCpit.trade_date == sd) & (FCpit.symbol == sym)]
    if fr.empty: return []
    X = np.full((1, len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fr.columns: X[0, j] = pd.to_numeric(fr[col].iloc[0], errors="coerce")
    yr = int(sd[:4])
    hits = [p for p in pats if int(p["mined_year"]) < yr and FR.rule_mask(p["rule"], X)[0]]
    return sorted(hits, key=lambda p: -(p["oos_lift"] or 0))

pat_rows = []
seen = set()
for _, p in picks.iterrows():
    sd = prev_td(p.trade_date); sym = p.symbol
    if not sd or (sd, sym) in seen: continue
    seen.add((sd, sym))
    for pt in fired(sd, sym):
        pat_rows.append(dict(signal_date=sd, symbol=sym, pattern=f"FALCPAT_{pt['pattern_id']}",
                             target=pt["target"], oos_lift=round(pt["oos_lift"] or 0, 2),
                             rule=rulestr(pt["rule"])))
S3 = pd.DataFrame(pat_rows, columns=["signal_date", "symbol", "pattern", "target", "oos_lift", "rule"])

# ---------------- write ----------------
os.makedirs(os.path.dirname(OUT), exist_ok=True)
with pd.ExcelWriter(OUT, engine="openpyxl") as xw:
    S1.to_excel(xw, sheet_name="signals", index=False)
    S2.to_excel(xw, sheet_name="overlay", index=False)
    S3.to_excel(xw, sheet_name="patterns", index=False)

print(f"\nsignals : {len(S1)} rows across {S1.signal_date.nunique()} signal-days "
      f"(max rank/day {S1.groupby('signal_date')['rank'].max().max()})")
print(f"overlay : {len(S2)} picks | in Falcon top-15 {(S2.falcon_rank<=15).sum()} "
      f"| top-30 {(S2.falcon_rank<=30).sum()} | ranked(top-100) {S2.falcon_rank.notna().sum()} "
      f"| outside/not-eligible {S2.falcon_rank.isna().sum()}")
print(f"patterns: {len(S3)} rows across {S3.groupby(['signal_date','symbol']).ngroups} (signal_date,symbol) picks")
print("\nWorked-example check (expect FIVESTAR 24 / PNBHOUSING 25 / POLICYBZR 27):")
for td, sym in [("2025-01-24", "FIVESTAR"), ("2025-01-31", "PNBHOUSING"), ("2025-01-23", "POLICYBZR")]:
    r = S2[(S2.trade_date == td) & (S2.symbol == sym)]
    if not r.empty:
        x = r.iloc[0]
        print(f"  {sym:11} sig {x.signal_date}: my_rank {x.my_rank} / falcon {x.falcon_rank} "
              f"| avg_lift {x.avg_lift} n {x.n_fires} cut15 {x.cut15_avglift} gap {x.gap_to_top15}")
print(f"\nSaved -> {OUT}")
