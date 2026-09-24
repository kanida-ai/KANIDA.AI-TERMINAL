"""Generalized daily Falcon signal generator — LEAK-FREE point-in-time rebuild.

Generalizes generate_top15_jan2025.py to ANY month / date-range and ANY depth (top_N).
Method validated 10/10 vs stored falcon_signals_live (July 2026) and reproduced for Jan-2025:
  - production pattern set (falcon_signal_replay.load_patterns -> promoted, drawdown_bounce dropped)
  - weekly features RECOMPUTED week-to-date (mid-week days don't leak Friday's value)
  - score = sum(oos_lift over firing patterns), n_fires>=min_fires, take top pool by score,
    FINAL rank by avg_lift = score/n_fires (desc); top-N = first N
  - tier = production rulebook arithmetic (signal_tier.classify_signal_tier parity)

Rebuild only (reads no stored signal). Read-only on DB. Saves to Downloads (or --out).

For N<=100 with default pool the ranking is BIT-IDENTICAL to the validated generator.
For deeper pulls (--top-n 200) the score pre-filter pool auto-expands to max(100, top_n).

Usage:
  python generate_falcon_signals.py --month 2025-01
  python generate_falcon_signals.py --month 2025-01 --top-n 100
  python generate_falcon_signals.py --start 2025-03-01 --end 2025-05-31 --top-n 200
  python generate_falcon_signals.py --month 2026-06 --top-n 30 --out C:\\path\\out.xlsx
"""
import os, sys, argparse, sqlite3, warnings, calendar
from datetime import datetime, timedelta
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")

ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
HIGH = {"PREMIUM-Pullback", "PREMIUM-Compression", "ENTERPRISE-Dryup", "GOLD", "GOLD-baseline"}


def _ok(v):
    return v is not None and not (isinstance(v, float) and v != v)


def classify(sret, twoday, rng, avg_lift, trend3_20, turn_pct):
    """Parity with backend/power_user/services/signal_tier.classify_signal_tier."""
    if _ok(sret) and sret > 10: return "AVOID"
    if _ok(sret) and sret > 7 and _ok(turn_pct) and turn_pct >= 0.75: return "AVOID"
    if _ok(sret) and sret <= 2 and _ok(twoday) and twoday < -5 and _ok(avg_lift) and avg_lift > 15: return "PREMIUM-Pullback"
    if _ok(sret) and sret <= 2 and _ok(rng) and rng < 2 and _ok(avg_lift) and avg_lift > 15: return "PREMIUM-Compression"
    if _ok(sret) and sret <= 2 and _ok(trend3_20) and trend3_20 < 0.9: return "ENTERPRISE-Dryup"
    if _ok(sret) and sret <= 2 and _ok(turn_pct) and turn_pct < 0.75: return "GOLD"
    if _ok(sret) and sret <= 2: return "GOLD-baseline"
    if _ok(sret) and sret <= 5: return "STANDARD"
    return "STANDARD-weak"


def parse_args():
    ap = argparse.ArgumentParser(description="Leak-free Falcon daily signal generator")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--month", help="YYYY-MM (whole calendar month)")
    g.add_argument("--start", help="YYYY-MM-DD start of signal range (use with --end)")
    ap.add_argument("--end", help="YYYY-MM-DD end of signal range (required with --start)")
    ap.add_argument("--top-n", type=int, default=15, help="depth of the final ranked list (default 15)")
    ap.add_argument("--min-fires", type=int, default=10, help="minimum firing patterns to be eligible (default 10)")
    ap.add_argument("--pool-n", type=int, default=None,
                    help="score-prefilter pool before avg_lift re-rank (default max(100, top_n))")
    ap.add_argument("--out", default=None, help="output .xlsx path (default Downloads)")
    a = ap.parse_args()
    if a.month:
        y, m = int(a.month[:4]), int(a.month[5:7])
        last = calendar.monthrange(y, m)[1]
        a.start = f"{y:04d}-{m:02d}-01"; a.end = f"{y:04d}-{m:02d}-{last:02d}"
        a.tag = f"{calendar.month_abbr[m].upper()}_{y}"
    else:
        if not a.end: ap.error("--end is required with --start")
        a.tag = f"{a.start}_to_{a.end}"
    if a.pool_n is None: a.pool_n = max(100, a.top_n)
    return a


def load_data(start, end):
    """Load features for the signal window and ohlc with enough lookback for weekly/turnover rolls."""
    # ohlc lookback: 252-trading-day turnover percentile (~1yr) + 20-week rolls => ~550 calendar days
    ohlc_from = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=550)).strftime("%Y-%m-%d")
    con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
    pats = FR.load_patterns(con)
    feat = pd.read_sql_query(
        "SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", con, params=(start, end))
    oh = pd.read_sql_query(
        "SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
        "WHERE trade_date>=? AND trade_date<=? ORDER BY symbol,trade_date", con, params=(ohlc_from, end))
    con.close()
    return pats, feat, oh


def build_pit(feat, oh):
    """Recompute week-to-date weekly features + tier inputs point-in-time; merge over stored leaky cols."""
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
    return FCpit, TF


def rebuild(FCpit, TF, pats, day, top_n, min_fires, pool_n):
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
    cands = [{"symbol": syms[i], "n_fires": int(fire[i]), "score": float(score[i])}
             for i in range(len(syms)) if fire[i] >= min_fires]
    cands.sort(key=lambda c: -c["score"])
    ranked = sorted(cands[:pool_n], key=lambda c: -(c["score"] / max(c["n_fires"], 1)))[:top_n]
    rows = []
    for pos, c in enumerate(ranked, 1):
        al = c["score"] / max(c["n_fires"], 1); tf = TF.get((c["symbol"], day), (np.nan,) * 5)
        tier = classify(tf[0], tf[2], tf[1], al, tf[3], tf[4])
        rows.append(dict(signal_date=day, rank=pos, symbol=c["symbol"], n_fires=c["n_fires"],
                         score=round(c["score"], 1), avg_lift=round(al, 2), tier=tier,
                         high_tier=("YES" if tier in HIGH else "")))
    return rows


def main():
    a = parse_args()
    print(f"Loading (signal window {a.start}..{a.end}, top_n={a.top_n}, pool={a.pool_n}, min_fires={a.min_fires}) ...")
    pats, feat, oh = load_data(a.start, a.end)
    print(f"  patterns={len(pats)}  feature rows={len(feat)}  ohlc rows={len(oh)}")
    FCpit, TF = build_pit(feat, oh)
    days = sorted(FCpit[(FCpit.trade_date >= a.start) & (FCpit.trade_date <= a.end)].trade_date.unique())
    allrows = []
    for d in days:
        allrows += rebuild(FCpit, TF, pats, d, a.top_n, a.min_fires, a.pool_n)
    df = pd.DataFrame(allrows)
    out = a.out or os.path.join(os.path.expanduser("~"), "Downloads",
                                f"FALCON_TOP{a.top_n}_{a.tag}_PIT.xlsx")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_excel(out, index=False)
    print(f"\n{a.tag}: {len(days)} trading days, {len(df)} picks (leak-free point-in-time rebuild)")
    if not df.empty:
        print("tiers:", df.tier.value_counts().to_dict())
        for d in days[:3] + days[-1:]:
            print(f"\n{d}:")
            print(df[df.signal_date == d][["rank", "symbol", "n_fires", "avg_lift", "tier"]]
                  .head(15).to_string(index=False))
    print(f"\nSaved -> {out}")


if __name__ == "__main__":
    main()
