"""
Falcon V7 final stress pack:
  1. Pattern family dependency (ablation per cluster)
  2. Year-by-year drawdown
  3. Monthly return distribution
  4. Latest-date signals (live what-would-fire-today)
"""
from __future__ import annotations
import json, sqlite3, statistics, sys, time
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
from engine.falcon_portfolio import (
    load_panel_with_keys, load_promoted_patterns, compute_signals, simulate,
    PER_TRADE,
)

DB = ROOT / "data" / "db" / "kanida_universe.db"


def fmt_inr(x):
    if x is None: return "—"
    s = "-" if x < 0 else ""; x = abs(x)
    if x >= 1e7: return f"{s}₹{x/1e7:.2f} Cr"
    if x >= 1e5: return f"{s}₹{x/1e5:.2f} L"
    return f"{s}₹{x:,.0f}"


def hr(label):
    print(f"\n{'='*80}\n{label}\n{'='*80}")


# ───────────────────────────────────────────────────────────────────────────
# Pattern family classifier
# ───────────────────────────────────────────────────────────────────────────

def classify_pattern_families(rule):
    """Return set of family tags this pattern belongs to."""
    fams = set()
    for f, op, th in rule:
        # Weekly strong-close: weekly_close_loc > X (high)
        if f == "weekly_close_loc" and op == ">":
            fams.add("weekly_close")
        # High-ATR: atr_20_pct > X
        if f == "atr_20_pct" and op == ">":
            fams.add("high_atr")
        # Drawdown-bounce: dist_high_252 / dist_high_120 <= negative threshold
        if f in ("dist_high_252", "dist_high_120") and op == "<=" and th < -10:
            fams.add("drawdown_bounce")
        # Weekly range: weekly_range_pct > X (wide weekly)
        if f == "weekly_range_pct" and op == ">":
            fams.add("weekly_range")
    return fams


# ───────────────────────────────────────────────────────────────────────────
# Load panel + patterns once
# ───────────────────────────────────────────────────────────────────────────
print("[setup] Loading panel + patterns...")
t0 = time.time()
X, syms, dates, years = load_panel_with_keys(DB)
con = sqlite3.connect(DB)
all_patterns = load_promoted_patterns(con)
con.close()
print(f"  panel: {X.shape[0]:,} rows  ·  patterns: {len(all_patterns)}  "
       f"({time.time()-t0:.1f}s)")

# Tag each pattern with families
for p in all_patterns:
    p["families"] = classify_pattern_families(p["rule"])

# Family stats
family_counts = Counter()
for p in all_patterns:
    for fam in p["families"]: family_counts[fam] += 1
n_no_family = sum(1 for p in all_patterns if not p["families"])
print(f"\n  Pattern family membership (overlapping):")
for fam, n in family_counts.most_common():
    print(f"    {fam:20s}: {n}")
print(f"    (no family tag): {n_no_family}")


# ───────────────────────────────────────────────────────────────────────────
# Helper: run simulation with given pattern subset
# ───────────────────────────────────────────────────────────────────────────

def run_sim(patterns_subset, label, capture_trades=False):
    n_fires, sum_lift = compute_signals(X, years, patterns_subset)
    qualifying = (n_fires >= 2) & (years >= 2023)
    signals = []
    for i in np.where(qualifying)[0]:
        signals.append({
            "symbol": syms[i], "signal_date": dates[i],
            "n_fires": int(n_fires[i]), "score": float(sum_lift[i]),
        })
    signals.sort(key=lambda s: (s["signal_date"], -s["score"]))
    res = simulate(DB, signals, starting_capital=30_00_000.0,
                    max_open=25, hold_days=20, top_n_per_day=10)
    m = res["metrics"]
    print(f"  {label:35s}: P&L {fmt_inr(m['total_pnl']):>12s}  "
           f"ROI {m['return_pct']:+6.2f}%  WR {m['win_rate']:5.1f}%  "
           f"trades {m['trades_taken']:>5}  DD {m['max_dd_pct']:+5.1f}%")
    if capture_trades:
        return res
    return m


# ───────────────────────────────────────────────────────────────────────────
# Q1: Pattern family ablation
# ───────────────────────────────────────────────────────────────────────────
hr("Q1 — Pattern family dependency (ablation)")
print("Re-running sim with each cluster removed...\n")

# Baseline: all patterns + capture trades for later analysis
print("BASELINE:")
baseline = run_sim(all_patterns, "All 803 patterns", capture_trades=True)
print()

# Ablations
for fam in ("weekly_close", "high_atr", "drawdown_bounce", "weekly_range"):
    subset = [p for p in all_patterns if fam not in p["families"]]
    n_removed = len(all_patterns) - len(subset)
    label = f"WITHOUT {fam} ({n_removed} removed, {len(subset)} kept)"
    run_sim(subset, label)

# Bonus: only patterns NOT in any family (the "leftovers")
leftover = [p for p in all_patterns if not p["families"]]
if leftover:
    print()
    run_sim(leftover, f"ONLY 'leftover' patterns (no family) [{len(leftover)} patterns]")


# ───────────────────────────────────────────────────────────────────────────
# Q2: Year-by-year drawdown
# ───────────────────────────────────────────────────────────────────────────
hr("Q2 — Year-by-year drawdown")
timeline = baseline["timeline"]

by_year_tl = defaultdict(list)
for tl in timeline:
    by_year_tl[tl["date"][:4]].append(tl)

print("Per-year drawdown (within calendar year):")
print(f"{'Year':6s}  {'Days':>6s}  {'Start eq':>14s}  {'End eq':>14s}  {'Peak eq':>14s}  "
       f"{'Trough eq':>14s}  {'Max DD ₹':>14s}  {'Max DD %':>10s}")
for yr in sorted(by_year_tl):
    rows = by_year_tl[yr]
    start_eq = rows[0]["equity"]
    end_eq = rows[-1]["equity"]
    peak = start_eq
    max_dd = 0.0
    max_dd_pct = 0.0
    for r in rows:
        if r["equity"] > peak: peak = r["equity"]
        dd = r["equity"] - peak
        dd_pct = dd / peak * 100 if peak > 0 else 0
        if dd < max_dd: max_dd = dd
        if dd_pct < max_dd_pct: max_dd_pct = dd_pct
    trough = min(r["equity"] for r in rows)
    print(f"{yr:6s}  {len(rows):>6d}  {fmt_inr(start_eq):>14s}  {fmt_inr(end_eq):>14s}  "
           f"{fmt_inr(peak):>14s}  {fmt_inr(trough):>14s}  "
           f"{fmt_inr(max_dd):>14s}  {max_dd_pct:>9.2f}%")


# ───────────────────────────────────────────────────────────────────────────
# Q3: Monthly return distribution
# ───────────────────────────────────────────────────────────────────────────
hr("Q3 — Monthly return distribution")
monthly = baseline["metrics"]["monthly"]
months_pos = sum(1 for v in monthly.values() if v > 0)
months_neg = sum(1 for v in monthly.values() if v < 0)
months_flat = sum(1 for v in monthly.values() if v == 0)
worst_mo = min(monthly.items(), key=lambda x: x[1])
best_mo  = max(monthly.items(), key=lambda x: x[1])
median_mo = statistics.median(monthly.values())
mean_mo = statistics.mean(monthly.values())

# % return per month — using per-month exposure approximation
# We'll compute month-end equity from timeline
month_equity = {}
for tl in timeline:
    month_equity[tl["date"][:7]] = tl["equity"]
sorted_mos = sorted(month_equity.keys())
prev_eq = 30_00_000.0
month_returns_pct = {}
for mo in sorted_mos:
    eq = month_equity[mo]
    month_returns_pct[mo] = (eq / prev_eq - 1) * 100
    prev_eq = eq

mo_ret_3pos = sum(1 for r in month_returns_pct.values() if r >= 3)
mo_ret_3neg = sum(1 for r in month_returns_pct.values() if r <= -3)
mo_ret_5pos = sum(1 for r in month_returns_pct.values() if r >= 5)
mo_ret_5neg = sum(1 for r in month_returns_pct.values() if r <= -5)

print(f"Total months: {len(monthly)}")
print(f"  Positive months: {months_pos}  ({months_pos/len(monthly)*100:.1f}%)")
print(f"  Negative months: {months_neg}  ({months_neg/len(monthly)*100:.1f}%)")
print(f"  Flat months:     {months_flat}")
print()
print(f"  Best month   (₹): {best_mo[0]}  {fmt_inr(best_mo[1])}")
print(f"  Worst month  (₹): {worst_mo[0]}  {fmt_inr(worst_mo[1])}")
print(f"  Median month (₹): {fmt_inr(median_mo)}")
print(f"  Mean month   (₹): {fmt_inr(mean_mo)}")
print()
print(f"Months by % return on running equity:")
print(f"  >= +5% : {mo_ret_5pos}  ({mo_ret_5pos/len(month_returns_pct)*100:.1f}%)")
print(f"  >= +3% : {mo_ret_3pos}  ({mo_ret_3pos/len(month_returns_pct)*100:.1f}%)")
print(f"  <= -3% : {mo_ret_3neg}  ({mo_ret_3neg/len(month_returns_pct)*100:.1f}%)")
print(f"  <= -5% : {mo_ret_5neg}  ({mo_ret_5neg/len(month_returns_pct)*100:.1f}%)")
print()
print(f"All monthly returns (% on running equity):")
for mo in sorted_mos:
    flag = " ★" if month_returns_pct[mo] >= 5 else (" ⚠" if month_returns_pct[mo] <= -3 else "")
    print(f"  {mo}: {month_returns_pct[mo]:+6.2f}%{flag}")


# ───────────────────────────────────────────────────────────────────────────
# Q4: Latest-date signals (live what-would-fire)
# ───────────────────────────────────────────────────────────────────────────
hr("Q4 — Latest-date signals (live)")
latest_date = max(dates)
latest_year = int(latest_date[:4])
print(f"Latest feature date in DB: {latest_date} (year {latest_year})")
print(f"Eligible patterns: those mined BEFORE {latest_year}")

# Recompute fires with eligibility
n_fires_today, sum_lift_today = compute_signals(X, years, all_patterns)
# Find rows on latest date
mask_today = (dates == latest_date)
n_today = mask_today.sum()
print(f"\nStocks on {latest_date}: {n_today}")
print(f"Stocks with >= 2 patterns firing: {(mask_today & (n_fires_today >= 2)).sum()}")

today_idx = np.where(mask_today)[0]
sigs = []
for i in today_idx:
    if n_fires_today[i] >= 2:
        sigs.append({"symbol": syms[i], "n_fires": int(n_fires_today[i]),
                      "score": float(sum_lift_today[i])})
sigs.sort(key=lambda s: -s["score"])

print(f"\nTop 20 stocks ranked by aggregate score (sum of OOS lift of firing patterns):")
print(f"{'Rank':>4s}  {'Symbol':12s}  {'n_fires':>8s}  {'score':>10s}  Sector")
con = sqlite3.connect(DB)
sectors = dict(con.execute("SELECT symbol, sector FROM falcon_sectors").fetchall())
con.close()
for i, s in enumerate(sigs[:20]):
    sec = sectors.get(s["symbol"], "")[:30]
    print(f"  {i+1:>3}.  {s['symbol']:12s}  {s['n_fires']:>8d}  "
           f"{s['score']:>10.1f}  {sec}")

print(f"\nThese are what the engine would fire as buy candidates if today's signal date were tradable.")
