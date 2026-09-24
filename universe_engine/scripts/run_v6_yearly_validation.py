"""
V6 multi-year static validation — same rules, no learning, per-year breakdown.

Pudhuraja's instruction: validate that V6 edge is NOT a single-regime fluke.
Run on Nifty 500 with V6's exact rules (no shortlist filtering, no per-year
threshold tuning) for each calendar year separately. Identify recurring stocks
across years (informational only — DO NOT use as filter yet).

Outputs:
  - Per-year metrics (signals, hit rates, mean ret 20d/30d)
  - Per-year cash-constrained P&L (₹30L start, ₹1L/trade, max 20 concurrent, 20d fixed hold)
  - Recurring stocks ranked by year-coverage
  - Decision recommendation per the four scenarios Pudhuraja outlined
"""
from __future__ import annotations
import argparse, json, sqlite3, statistics, sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB        = ROOT / "data" / "db" / "kanida_universe.db"
SIGS_JSON = ROOT / "reports" / "_engine_v6_signals.json"
REPORT    = ROOT / "reports" / "ENGINE_V6_YEARLY_VALIDATION.md"

PER_TRADE = 100_000.0
MAX_OPEN  = 20
START_CAP = 30_00_000.0
HOLD_DAYS = 20
COST_BPS  = 30.0
SLIP_BPS  = 5.0


def fmt_inr(x):
    if x is None: return "—"
    s = "-" if x < 0 else ""; x = abs(x)
    if x >= 1e7: return f"{s}₹{x/1e7:.2f} Cr"
    if x >= 1e5: return f"{s}₹{x/1e5:.2f} L"
    return f"{s}₹{x:,.0f}"


def trading_days_after(con, sym, after, n):
    rows = con.execute("""SELECT trade_date FROM ohlc_daily
                          WHERE symbol=? AND trade_date>? ORDER BY trade_date LIMIT ?""",
                       (sym, after, n)).fetchall()
    return [r[0] for r in rows]


def close_on(con, sym, d):
    r = con.execute("SELECT close FROM ohlc_daily WHERE symbol=? AND trade_date=?",
                     (sym, d)).fetchone()
    return r[0] if r else None


def open_on(con, sym, d):
    r = con.execute("SELECT open FROM ohlc_daily WHERE symbol=? AND trade_date=?",
                     (sym, d)).fetchone()
    return r[0] if r else None


def all_trading_days(con, start, end):
    rows = con.execute("""SELECT DISTINCT trade_date FROM ohlc_daily
                          WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date""",
                       (start, end)).fetchall()
    return [r[0] for r in rows]


def per_year_signals(sigs):
    by_year = defaultdict(list)
    for s in sigs:
        yr = s["signal_date"][:4]
        by_year[yr].append(s)
    return dict(by_year)


def signal_summary(sigs, hold):
    """Aggregate metrics for a list of signals at given hold length."""
    key = f"ret_{hold}d"
    rets = [s[key] for s in sigs if key in s and s[key] is not None]
    if not rets: return {}
    pos = sum(1 for r in rets if r > 0)
    return {
        "n":       len(rets),
        "wr":      pos / len(rets) * 100,
        "mean":    statistics.mean(rets),
        "median":  statistics.median(rets),
        "hit_5":   sum(1 for r in rets if r >=  5) / len(rets) * 100,
        "hit_7":   sum(1 for r in rets if r >=  7) / len(rets) * 100,
        "hit_10":  sum(1 for r in rets if r >= 10) / len(rets) * 100,
        "hit_15":  sum(1 for r in rets if r >= 15) / len(rets) * 100,
        "best":    max(rets),
        "worst":   min(rets),
    }


def cash_sim(sigs, con, year_start, year_end,
              starting_capital=START_CAP, max_open=MAX_OPEN, hold=HOLD_DAYS):
    """Cash-constrained sim. Trades that would exit AFTER year_end are still
    included (signals fired in this year, P&L booked when they exit)."""

    # Build candidate trades: entry next-day open, exit at hold-day close
    candidates = []
    slip = SLIP_BPS / 10_000.0
    for s in sigs:
        days_after = trading_days_after(con, s["symbol"], s["signal_date"], hold + 5)
        if len(days_after) < hold: continue
        entry_date = days_after[0]
        exit_date  = days_after[hold - 1]
        op = open_on(con, s["symbol"], entry_date)
        cl = close_on(con, s["symbol"], exit_date)
        if op is None or cl is None or op <= 0: continue
        ep = op * (1 + slip)
        xp = cl * (1 - slip)
        shares = PER_TRADE / ep
        gross = shares * (xp - ep)
        fees = PER_TRADE * COST_BPS / 10_000.0
        net = gross - fees
        candidates.append({
            "symbol":      s["symbol"],
            "signal_date": s["signal_date"],
            "entry_date":  entry_date,
            "exit_date":   exit_date,
            "entry_price": ep,
            "shares":      shares,
            "net_pnl":     net,
            "ret_pct":     net / PER_TRADE * 100,
        })

    if not candidates:
        return {"trades": [], "skipped": [], "metrics": {}}

    # Sim window: from earliest entry to latest exit (may extend past year_end)
    by_entry = defaultdict(list)
    for c in candidates: by_entry[c["entry_date"]].append(c)
    sim_start = min(c["entry_date"] for c in candidates)
    sim_end   = max(c["exit_date"]  for c in candidates)
    days = all_trading_days(con, sim_start, sim_end)

    cash = starting_capital
    open_pos = []
    closed = []
    skipped = []
    peak_eq = starting_capital
    max_concurrent = 0
    timeline = []

    for d in days:
        # Exits
        for p in [x for x in open_pos if x["exit_date"] == d]:
            cash += PER_TRADE + p["net_pnl"]
            closed.append(p)
        open_pos = [x for x in open_pos if x["exit_date"] != d]

        # Entries
        for t in by_entry.get(d, []):
            if cash < PER_TRADE:
                skipped.append({**t, "reason": "cash"}); continue
            if len(open_pos) >= max_open:
                skipped.append({**t, "reason": "max_open"}); continue
            cash -= PER_TRADE
            open_pos.append(t)

        # MTM
        unrealized = 0.0
        for p in open_pos:
            cl = close_on(con, p["symbol"], d) or p["entry_price"]
            unrealized += p["shares"] * (cl * (1 - SLIP_BPS/10000) - p["entry_price"])
        equity = cash + len(open_pos) * PER_TRADE + unrealized
        if equity > peak_eq: peak_eq = equity
        max_concurrent = max(max_concurrent, len(open_pos))
        timeline.append({"date": d, "equity": equity, "n_open": len(open_pos),
                         "drawdown": equity - peak_eq})

    pnls = [t["net_pnl"] for t in closed]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    final_eq = timeline[-1]["equity"] if timeline else starting_capital
    dd = min((tl["drawdown"] for tl in timeline), default=0)
    dd_pct = dd / peak_eq * 100 if peak_eq > 0 else 0

    monthly = defaultdict(float)
    for t in closed: monthly[t["exit_date"][:7]] += t["net_pnl"]

    return {
        "trades": closed,
        "skipped": skipped,
        "metrics": {
            "starting_capital": starting_capital,
            "ending_equity":    final_eq,
            "total_pnl":        final_eq - starting_capital,
            "return_pct":       (final_eq / starting_capital - 1) * 100,
            "trades_taken":     len(closed),
            "trades_skipped":   len(skipped),
            "skip_cash":        sum(1 for s in skipped if s["reason"]=="cash"),
            "skip_max_open":    sum(1 for s in skipped if s["reason"]=="max_open"),
            "win_rate":         (sum(1 for p in pnls if p > 0)/len(pnls)*100) if pnls else 0,
            "avg_win":          statistics.mean(wins) if wins else 0,
            "avg_loss":         statistics.mean(losses) if losses else 0,
            "wlr":              (statistics.mean(wins)/abs(statistics.mean(losses)))
                                   if (wins and losses and statistics.mean(losses) != 0) else None,
            "best":             max(pnls) if pnls else 0,
            "worst":            min(pnls) if pnls else 0,
            "max_dd":           dd,
            "max_dd_pct":       dd_pct,
            "max_concurrent":   max_concurrent,
            "monthly":          dict(monthly),
        }
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="2023,2024,2025,2026")
    args = ap.parse_args()
    years = [y.strip() for y in args.years.split(",")]

    sigs = json.loads(SIGS_JSON.read_text())
    print(f"Loaded {len(sigs)} V6 signals.")

    by_year = per_year_signals(sigs)
    con = sqlite3.connect(DB)

    yearly_results = {}
    for yr in years:
        ys = by_year.get(yr, [])
        print(f"\n=== {yr}: {len(ys)} signals ===")
        if not ys: continue
        s20 = signal_summary(ys, 20)
        s30 = signal_summary(ys, 30)
        sim = cash_sim(ys, con, f"{yr}-01-01", f"{yr}-12-31")
        yearly_results[yr] = {"sig_count": len(ys), "s20": s20, "s30": s30, "sim": sim}
        print(f"  20d: WR={s20['wr']:.1f}%  hit+7={s20['hit_7']:.1f}%  hit+10={s20['hit_10']:.1f}%  hit+15={s20['hit_15']:.1f}%  mean={s20['mean']:+.2f}%")
        print(f"  30d: WR={s30['wr']:.1f}%  hit+7={s30['hit_7']:.1f}%  hit+10={s30['hit_10']:.1f}%  hit+15={s30['hit_15']:.1f}%  mean={s30['mean']:+.2f}%")
        m = sim["metrics"]
        print(f"  P&L sim: {fmt_inr(m['total_pnl'])}  ROI={m['return_pct']:+.2f}%  WR={m['win_rate']:.1f}%  trades={m['trades_taken']}/{m['trades_taken']+m['trades_skipped']}  DD={m['max_dd_pct']:+.2f}%")

    # Recurring stocks: top performers per year (≥3 signals/yr) and intersection
    yearly_top: Dict[str, set] = {}
    yearly_stock_stats = {}
    for yr in years:
        ys = by_year.get(yr, [])
        by_sym = defaultdict(list)
        for s in ys: by_sym[s["symbol"]].append(s)
        ranked = []
        for sym, group in by_sym.items():
            if len(group) < 3: continue
            sm = signal_summary(group, 20)
            if sm.get("n", 0) >= 3:
                ranked.append((sym, sm))
        ranked.sort(key=lambda x: (x[1]["hit_10"], x[1]["mean"]), reverse=True)
        yearly_stock_stats[yr] = ranked
        yearly_top[yr] = set(s for s, _ in ranked[:30])

    # Stocks appearing in top-30 of multiple years
    appearance_count = Counter()
    for yr, top_set in yearly_top.items():
        for s in top_set: appearance_count[s] += 1

    md = build_report(yearly_results, yearly_stock_stats, appearance_count, years)
    REPORT.write_text(md, encoding="utf-8")
    print(f"\nReport -> {REPORT}")


def build_report(yearly, yearly_stock_stats, appearance, years):
    L = ["# V6 Multi-Year Static Validation", ""]
    L.append(f"- Same V6 rules (5 base patterns × 6 confluences, ≥1 base AND ≥2 confluences fire)")
    L.append(f"- NO per-year tuning, NO shortlist filtering — pure rule application")
    L.append(f"- Per-year P&L: ₹30L start, ₹1L/trade, max 20 concurrent, 20d fixed hold")
    L.append("")

    L.append("## Per-year signal hit rates (20d hold)")
    L.append("")
    L.append("| Year | Signals | WR | Hit +7% | Hit +10% | Hit +15% | Mean | Median | Best | Worst |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for yr in years:
        if yr not in yearly: continue
        s = yearly[yr]["s20"]
        if not s: continue
        L.append(f"| {yr} | {s['n']} | {s['wr']:.1f}% | {s['hit_7']:.1f}% | "
                 f"{s['hit_10']:.1f}% | {s['hit_15']:.1f}% | {s['mean']:+.2f}% | "
                 f"{s['median']:+.2f}% | {s['best']:+.1f}% | {s['worst']:+.1f}% |")
    L.append("")

    L.append("## Per-year signal hit rates (30d hold)")
    L.append("")
    L.append("| Year | Signals | WR | Hit +7% | Hit +10% | Hit +15% | Mean | Median | Best | Worst |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for yr in years:
        if yr not in yearly: continue
        s = yearly[yr]["s30"]
        if not s: continue
        L.append(f"| {yr} | {s['n']} | {s['wr']:.1f}% | {s['hit_7']:.1f}% | "
                 f"{s['hit_10']:.1f}% | {s['hit_15']:.1f}% | {s['mean']:+.2f}% | "
                 f"{s['median']:+.2f}% | {s['best']:+.1f}% | {s['worst']:+.1f}% |")
    L.append("")

    L.append("## Per-year P&L sim (₹30L start, max 20 concurrent, 20d hold)")
    L.append("")
    L.append("| Year | Trades | Skipped | WR | Avg win | Avg loss | W/L | Total P&L | ROI | Max DD% | Max concurrent |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for yr in years:
        if yr not in yearly: continue
        m = yearly[yr]["sim"]["metrics"]
        if not m: continue
        wlr = f"{m['wlr']:.2f}" if m['wlr'] else "—"
        L.append(f"| {yr} | {m['trades_taken']} | {m['trades_skipped']} | {m['win_rate']:.1f}% | "
                 f"{fmt_inr(m['avg_win'])} | {fmt_inr(m['avg_loss'])} | {wlr} | "
                 f"{fmt_inr(m['total_pnl'])} | {m['return_pct']:+.2f}% | "
                 f"{m['max_dd_pct']:+.2f}% | {m['max_concurrent']} |")
    L.append("")

    # Monthly P&L per year
    L.append("## Monthly P&L by year")
    L.append("")
    for yr in years:
        if yr not in yearly: continue
        L.append(f"### {yr}")
        L.append("")
        L.append("| Month | P&L |")
        L.append("|---|---|")
        for mo in sorted(yearly[yr]["sim"]["metrics"].get("monthly", {})):
            L.append(f"| {mo} | {fmt_inr(yearly[yr]['sim']['metrics']['monthly'][mo])} |")
        L.append("")

    # Top stocks per year
    L.append("## Top 15 stocks per year (≥3 signals, by 20d hit +10%)")
    L.append("")
    for yr in years:
        rk = yearly_stock_stats.get(yr, [])
        if not rk: continue
        L.append(f"### {yr}")
        L.append("")
        L.append("| Symbol | n | WR | Mean 20d | Hit +10% | Hit +15% | Best | Worst |")
        L.append("|---|---|---|---|---|---|---|---|")
        for sym, sm in rk[:15]:
            L.append(f"| {sym} | {sm['n']} | {sm['wr']:.1f}% | {sm['mean']:+.2f}% | "
                     f"{sm['hit_10']:.1f}% | {sm['hit_15']:.1f}% | {sm['best']:+.1f}% | {sm['worst']:+.1f}% |")
        L.append("")

    # Recurring stocks
    L.append("## Recurring stocks (appear in top-30 of multiple years)")
    L.append("")
    L.append("| Symbol | Years in top-30 |")
    L.append("|---|---|")
    sorted_syms = sorted(appearance.items(), key=lambda x: -x[1])
    for sym, count in sorted_syms:
        if count < 2: continue
        L.append(f"| {sym} | {count} of {len(yearly)} |")
    L.append("")

    # Verdict
    L.append("## VERDICT — which scenario does the data fit?")
    L.append("")
    yrs_with_positive_pnl = [yr for yr in years if yr in yearly
                              and yearly[yr]["sim"]["metrics"].get("return_pct", 0) > 0]
    n_yrs = len([yr for yr in years if yr in yearly])
    rec_in_3plus = sum(1 for sym, c in appearance.items() if c >= max(3, n_yrs - 1))
    L.append(f"- Years with **positive P&L**: {len(yrs_with_positive_pnl)} of {n_yrs}: "
             f"{', '.join(yrs_with_positive_pnl) if yrs_with_positive_pnl else 'NONE'}")
    L.append(f"- Stocks recurring in **top-30 of (n_yrs-1)+ years**: {rec_in_3plus}")
    L.append("")
    L.append("Decision tree (Pudhuraja's framework):")
    L.append("- If V6 works **across years**: build V6.1 walk-forward / adaptive.")
    L.append("- If V6 worked in only **one hot year**: it is regime-dependent.")
    L.append("- If V6 works only on **a few stocks repeatedly**: build stock-personality engine.")
    L.append("- If V6 has many signals but **weak P&L**: improve exit/portfolio logic.")
    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
