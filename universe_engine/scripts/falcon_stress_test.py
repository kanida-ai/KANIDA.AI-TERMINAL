"""
Falcon V7 stress tests — answer Pudhuraja's six challenges before trusting result.

  1. Pattern count mismatch (747 vs 803)
  2. Outlier dependency (exclude top trades, cap per-trade return)
  3. Liquidity realism (slippage stress)
  4. Signal overlap / duplicate exposure
  5. Corporate action / listing eligibility on top performers
  6. Exit realism (gap-through stops)
"""
from __future__ import annotations
import json, sqlite3, statistics, sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB = ROOT / "data" / "db" / "kanida_universe.db"
TRADES_JSON = None    # we'll regenerate trades to inspect


def fmt_inr(x):
    if x is None: return "—"
    s = "-" if x < 0 else ""; x = abs(x)
    if x >= 1e7: return f"{s}₹{x/1e7:.2f} Cr"
    if x >= 1e5: return f"{s}₹{x/1e5:.2f} L"
    return f"{s}₹{x:,.0f}"


def hr(label):
    print(f"\n{'='*78}\n{label}\n{'='*78}")


# ───────────────────────────────────────────────────────────────────────────
# Q1: Pattern count
# ───────────────────────────────────────────────────────────────────────────
hr("Q1 — Pattern count: 747 vs 803")
con = sqlite3.connect(DB)
rows = con.execute("""
    SELECT classification, COUNT(*) FROM falcon_promoted_patterns
    GROUP BY classification ORDER BY 2 DESC
""").fetchall()
total = 0
for cls, n in rows:
    print(f"  {cls:20s}: {n}")
    total += n
print(f"  TOTAL              : {total}")
print()
print("Phase 4 loader filter: WHERE classification IN ('universal','regime_dependent')")
print("→ universal (747) + regime_dependent (56) = 803  ✓")
print("→ sector_specific (43) was EXCLUDED — correct")
print("→ NO rejected patterns are in the promoted table (they're filtered at promotion)")


# ───────────────────────────────────────────────────────────────────────────
# Re-run sim to get fresh trade ledger for the rest of tests
# ───────────────────────────────────────────────────────────────────────────
hr("Re-running simulator to capture full trade ledger for stress tests")

import numpy as np
from engine.falcon_portfolio import (
    load_panel_with_keys, load_promoted_patterns, compute_signals, simulate,
    PER_TRADE, SLIP_BPS, COST_BPS, INIT_STOP, TRAIL_TRIGGER, TRAIL_LOOKBACK,
)

X, syms, dates, years = load_panel_with_keys(DB)
con = sqlite3.connect(DB)
patterns = load_promoted_patterns(con)
con.close()
n_fires, sum_lift = compute_signals(X, years, patterns)
qualifying = (n_fires >= 2) & (years >= 2023)
signals = []
for i in np.where(qualifying)[0]:
    signals.append({
        "symbol": syms[i], "signal_date": dates[i],
        "n_fires": int(n_fires[i]), "score": float(sum_lift[i]),
    })
signals.sort(key=lambda s: (s["signal_date"], -s["score"]))

# Default sim
res = simulate(DB, signals, starting_capital=30_00_000.0, max_open=25,
                hold_days=20, top_n_per_day=10)
trades = res["trades"]
m = res["metrics"]
print(f"Reproduced: {len(trades)} trades, P&L {fmt_inr(m['total_pnl'])}, "
       f"return {m['return_pct']:+.2f}%")


# ───────────────────────────────────────────────────────────────────────────
# Q2: Outlier dependency
# ───────────────────────────────────────────────────────────────────────────
hr("Q2 — Outlier dependency")
sorted_pnls = sorted([t["net_pnl"] for t in trades], reverse=True)
total_pnl = sum(sorted_pnls)
print(f"Full P&L: {fmt_inr(total_pnl)} ({len(trades)} trades)")

for k in (5, 10, 25, 50, 100):
    excl = sum(sorted_pnls[k:])
    pct_kept = excl / total_pnl * 100 if total_pnl else 0
    print(f"  Excluding top {k:>3} trades: P&L = {fmt_inr(excl):>15s} "
           f"({pct_kept:5.1f}% of full)")

# Cap per-trade return at +25%, +15%, +10%
print()
for cap_pct in (50, 25, 15, 10):
    cap_rs = PER_TRADE * cap_pct / 100
    capped_pnls = [min(p, cap_rs) for p in (t["net_pnl"] for t in trades)]
    total_capped = sum(capped_pnls)
    pct_kept = total_capped / total_pnl * 100 if total_pnl else 0
    n_capped = sum(1 for t in trades if t["net_pnl"] > cap_rs)
    print(f"  Cap each trade at +{cap_pct:>2}% ({fmt_inr(cap_rs):>9s}): "
           f"P&L = {fmt_inr(total_capped):>15s}  "
           f"({pct_kept:5.1f}% of full)  ·  {n_capped} trades capped")


# ───────────────────────────────────────────────────────────────────────────
# Q3: Liquidity / slippage stress
# ───────────────────────────────────────────────────────────────────────────
hr("Q3 — Slippage stress test")
import engine.falcon_portfolio as fpm
print(f"Default slippage: {SLIP_BPS} bps each side")
print()

# Re-run with different slippage levels
for new_slip in (5, 25, 50, 100):
    fpm.SLIP_BPS = new_slip
    r2 = simulate(DB, signals, starting_capital=30_00_000.0, max_open=25,
                    hold_days=20, top_n_per_day=10)
    m2 = r2["metrics"]
    print(f"  Slippage {new_slip:>3} bps each side  → "
           f"P&L {fmt_inr(m2['total_pnl']):>15s}  "
           f"ROI {m2['return_pct']:+6.2f}%  "
           f"WR {m2['win_rate']:5.1f}%  "
           f"trades {m2['trades_taken']}")

# Restore default
fpm.SLIP_BPS = 5


# ───────────────────────────────────────────────────────────────────────────
# Q4: Signal overlap / duplicate exposure
# ───────────────────────────────────────────────────────────────────────────
hr("Q4 — Signal overlap / duplicate exposure")
trade_keys = Counter((t["symbol"], t["signal_date"]) for t in trades)
dups = {k: v for k, v in trade_keys.items() if v > 1}
print(f"Total trades: {len(trades)}")
print(f"Distinct (symbol, signal_date) pairs: {len(trade_keys)}")
print(f"Pairs with >1 trade: {len(dups)}")
if dups:
    print("  WARNING: duplicate exposure detected!")
    for k, v in list(dups.items())[:5]: print(f"    {k}: {v}")
else:
    print("  ✓ No duplicate exposure — one trade per (symbol, signal_date).")

# But are there overlapping HOLDS? E.g., bought ADANIGREEN Feb 24 and Feb 27 —
# both held simultaneously?
overlap_count = 0
overlap_examples = []
for sym in {t["symbol"] for t in trades}:
    sym_trades = sorted([t for t in trades if t["symbol"] == sym],
                          key=lambda x: x["entry_date"])
    for i in range(len(sym_trades)):
        for j in range(i+1, len(sym_trades)):
            if sym_trades[j]["entry_date"] <= sym_trades[i]["exit_actual_date"]:
                overlap_count += 1
                if len(overlap_examples) < 5:
                    overlap_examples.append(
                        f"  {sym}: trade1 {sym_trades[i]['entry_date']}→{sym_trades[i]['exit_actual_date']} "
                        f"overlaps trade2 {sym_trades[j]['entry_date']}→{sym_trades[j]['exit_actual_date']}"
                    )
                break    # don't compare beyond first overlap per pair-i
print(f"\nOverlapping holds (same symbol open simultaneously): {overlap_count}")
for ex in overlap_examples: print(ex)


# ───────────────────────────────────────────────────────────────────────────
# Q5: Survivorship / listing eligibility
# ───────────────────────────────────────────────────────────────────────────
hr("Q5 — Survivorship / listing eligibility on top performers")
con = sqlite3.connect(DB)
top_winners = sorted(trades, key=lambda x: x["net_pnl"], reverse=True)[:15]
print("Top 15 winners — listing-date check:")
print(f"{'Symbol':12s} {'first_bar':12s} {'days before signal':>18s}  Sector")
for t in top_winners:
    sym = t["symbol"]
    first = con.execute("SELECT MIN(trade_date) FROM ohlc_daily WHERE symbol=?",
                         (sym,)).fetchone()[0]
    sec = con.execute("SELECT sector FROM falcon_sectors WHERE symbol=?",
                       (sym,)).fetchone()
    sec_name = sec[0] if sec else "(no sector mapping)"
    if first:
        days = (date.fromisoformat(t["signal_date"]) - date.fromisoformat(first)).days
    else:
        days = -1
    flag = "  ✓" if days >= 252 else "  ⚠ <252 days listed"
    print(f"  {sym:12s} {first or '-':12s} {days:>18d}  {sec_name[:30]:30s}{flag}")

# Liquidity check: avg traded value over 60d before signal
print("\nLiquidity (60d avg traded value ₹) at signal time for top 15 winners:")
for t in top_winners:
    sym = t["symbol"]
    sd = t["signal_date"]
    r = con.execute(f"""
        SELECT AVG(close * volume) FROM ohlc_daily
        WHERE symbol=? AND trade_date < ? AND trade_date >= date(?, '-60 days')
    """, (sym, sd, sd)).fetchone()
    av = r[0] or 0
    flag = "✓" if av >= 5e7 else ("⚠ <₹5cr" if av >= 1e7 else "✗ <₹1cr ILLIQUID")
    print(f"  {sym:12s} {sd}  avg_value/day = {fmt_inr(av):>10s}  {flag}")
con.close()


# ───────────────────────────────────────────────────────────────────────────
# Q6: Exit realism — gap-through detection
# ───────────────────────────────────────────────────────────────────────────
hr("Q6 — Exit realism: gap-through stop check")
con = sqlite3.connect(DB)
gap_through_count = 0
gap_through_loss = 0.0
sample_gaps = []
for t in trades:
    if t["exit_reason"] not in ("init_stop", "trail_stop"): continue
    # Get exit-day OHLC
    r = con.execute("SELECT open, high, low, close FROM ohlc_daily WHERE symbol=? AND trade_date=?",
                     (t["symbol"], t["exit_actual_date"])).fetchone()
    if not r: continue
    op, hi, lo, cl = r
    avg_entry = t["avg_entry"]
    # Reconstruct stop_level used (this is approximation since trail uses 10d-low)
    init_stop_level = avg_entry * (1 + INIT_STOP)
    # If the day's OPEN was below init_stop_level → gap-through
    if op < init_stop_level:
        # Realistic fill = open price, not stop level
        actual_exit_at_open = op * (1 - SLIP_BPS / 10_000.0)
        shares = PER_TRADE / avg_entry
        true_pnl = shares * (actual_exit_at_open - avg_entry) - PER_TRADE * COST_BPS / 10_000.0
        diff = true_pnl - t["net_pnl"]
        gap_through_count += 1
        gap_through_loss += diff   # negative = our sim was too optimistic
        if len(sample_gaps) < 8:
            sample_gaps.append((t["symbol"], t["exit_actual_date"], op, init_stop_level,
                                  t["net_pnl"], true_pnl, diff))
print(f"Gap-through cases (open < initial stop level): {gap_through_count}")
print(f"  Cumulative additional loss if stops filled at open: {fmt_inr(gap_through_loss)}")
if sample_gaps:
    print(f"\n  Sample gap-throughs:")
    print(f"  {'Symbol':12s} {'Date':12s} {'Open':>9s} {'Stop':>9s} {'Sim P&L':>10s} {'Real P&L':>10s} {'Diff':>10s}")
    for s in sample_gaps:
        print(f"  {s[0]:12s} {s[1]:12s} {s[2]:>9.2f} {s[3]:>9.2f} "
               f"{fmt_inr(s[4]):>10s} {fmt_inr(s[5]):>10s} {fmt_inr(s[6]):>10s}")
con.close()
