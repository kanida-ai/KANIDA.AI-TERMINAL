"""
V4 hypothesis test: Pattern B only + Method A (blind 9:15 MOO entry) + EOD data.

Operational pitch: needs only daily bars + futures OI. No intraday late-vol.
No 9:30 confirmation. Place market-on-open buy at 9:15 next morning. Exit T+5.

This script reuses V3 signals JSON (which contains all signals where any of
B/C/D fired, with Method A and Method B outcomes pre-computed). We filter to
signals where Pattern B fired (overlap with C/D allowed) and report Method A
outcomes — both unconstrained and ₹30L cash-constrained.
"""
from __future__ import annotations
import argparse, json, sqlite3, statistics, sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB        = ROOT / "data" / "db" / "kanida_universe.db"
SIGS_JSON = ROOT / "reports" / "_engine_v3_signals.json"
REPORT    = ROOT / "reports" / "ENGINE_V4_PATTERN_B_REPORT.md"


PER_TRADE   = 100_000.0
START_CAP   = 30_00_000.0
MAX_OPEN    = 30
HOLD_DAYS   = 5
COST_BPS_RT = 30.0
SLIP_BPS    = 5.0


def fmt_inr(x):
    if x is None: return "—"
    s = "-" if x < 0 else ""; x = abs(x)
    if x >= 1e7: return f"{s}₹{x/1e7:.2f} Cr"
    if x >= 1e5: return f"{s}₹{x/1e5:.2f} L"
    return f"{s}₹{x:,.0f}"


def _all_trading_days(con, start, end):
    rows = con.execute("""SELECT DISTINCT trade_date FROM ohlc_daily
                          WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date""",
                       (start, end)).fetchall()
    return [r[0] for r in rows]


def _close_on(con, sym, d):
    r = con.execute("SELECT close FROM ohlc_daily WHERE symbol=? AND trade_date=?",
                     (sym, d)).fetchone()
    return r[0] if r else None


def _open_on(con, sym, d):
    r = con.execute("SELECT open FROM ohlc_daily WHERE symbol=? AND trade_date=?",
                     (sym, d)).fetchone()
    return r[0] if r else None


def _trading_days_after(con, sym, after, n):
    rows = con.execute("""SELECT trade_date FROM ohlc_daily
                          WHERE symbol=? AND trade_date>? ORDER BY trade_date LIMIT ?""",
                       (sym, after, n)).fetchall()
    return [r[0] for r in rows]


def build_trades_method_a(signals, con):
    """For each Pattern B signal, build a Method-A (next-day OPEN entry) trade
    held to T+5 close. Re-fetches OHLC from DB to be self-contained — does NOT
    rely on V3's pre-baked em_a_* values (which use the same logic but we want
    explicit clean numbers)."""
    trades = []
    for s in signals:
        days_after = _trading_days_after(con, s["symbol"], s["signal_date"], HOLD_DAYS + 5)
        if len(days_after) < HOLD_DAYS:
            continue
        entry_date = days_after[0]
        exit_date  = days_after[HOLD_DAYS - 1]
        op = _open_on(con, s["symbol"], entry_date)
        cl = _close_on(con, s["symbol"], exit_date)
        if op is None or cl is None or op <= 0:
            continue
        slip = SLIP_BPS / 10_000.0
        entry_price = op * (1 + slip)
        exit_price  = cl * (1 - slip)
        shares = PER_TRADE / entry_price
        gross_pnl = shares * (exit_price - entry_price)
        fees = PER_TRADE * COST_BPS_RT / 10_000.0
        net_pnl = gross_pnl - fees
        trades.append({
            "symbol":      s["symbol"],
            "signal_date": s["signal_date"],
            "patterns":    "+".join(sorted(s["patterns"])),
            "entry_date":  entry_date,
            "exit_date":   exit_date,
            "entry_price": round(entry_price, 4),
            "exit_price":  round(exit_price, 4),
            "shares":      round(shares, 4),
            "gross_pnl":   round(gross_pnl, 2),
            "fees":        round(fees, 2),
            "net_pnl":     round(net_pnl, 2),
            "ret_pct":     round(net_pnl / PER_TRADE * 100, 3),
            "oi_growth":   s.get("oi_growth_5d"),
            "n_sub_3":     s.get("n_sub_3"),
            "close_loc":   s.get("close_loc"),
        })
    return trades


def cash_constrained_sim(trades, con, starting_capital=START_CAP, max_open=MAX_OPEN):
    """Walk forward day-by-day with cash + concurrent caps. Return timeline + metrics."""
    if not trades:
        return {"trades": [], "skipped": [], "timeline": [], "metrics": {}}
    by_entry = defaultdict(list)
    for t in trades:
        by_entry[t["entry_date"]].append(t)

    sim_start = min(t["entry_date"] for t in trades)
    sim_end   = max(t["exit_date"] for t in trades)
    days = _all_trading_days(con, sim_start, sim_end)

    cash = starting_capital
    open_positions = []
    closed = []
    skipped = []
    timeline = []
    peak_eq = starting_capital
    max_concurrent = 0
    slip = SLIP_BPS / 10_000.0

    for d in days:
        # exits — ONLY for trades we actually entered (open_positions)
        exiters = [p for p in open_positions if p["exit_date"] == d]
        for p in exiters:
            cash += PER_TRADE + p["net_pnl"]
            closed.append(p)
        open_positions = [p for p in open_positions if p["exit_date"] != d]

        # entries
        for t in by_entry.get(d, []):
            if cash < PER_TRADE:
                skipped.append({**t, "reason": "insufficient_cash",
                                  "cash_at_skip": cash, "open_at_skip": len(open_positions)})
                continue
            if len(open_positions) >= max_open:
                skipped.append({**t, "reason": "max_concurrent",
                                  "cash_at_skip": cash, "open_at_skip": len(open_positions)})
                continue
            cash -= PER_TRADE
            open_positions.append(t)

        # MTM
        unrealized = 0.0
        for p in open_positions:
            cl = _close_on(con, p["symbol"], d)
            if cl is None: continue
            mtm = cl * (1 - slip)
            unrealized += p["shares"] * (mtm - p["entry_price"])

        equity = cash + len(open_positions) * PER_TRADE + unrealized
        if equity > peak_eq: peak_eq = equity
        dd = equity - peak_eq
        max_concurrent = max(max_concurrent, len(open_positions))

        timeline.append({
            "date":         d,
            "cash":         round(cash, 2),
            "deployed":     round(len(open_positions) * PER_TRADE, 2),
            "unrealized":   round(unrealized, 2),
            "equity":       round(equity, 2),
            "n_open":       len(open_positions),
            "drawdown":     round(dd, 2),
            "drawdown_pct": round(dd / peak_eq * 100, 3) if peak_eq > 0 else 0,
            "utilization":  round((len(open_positions)*PER_TRADE + unrealized)/equity, 4)
                              if equity > 0 else 0,
        })

    pnls = [t["net_pnl"] for t in closed]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    final = timeline[-1] if timeline else {"equity": starting_capital}
    monthly = defaultdict(float)
    for t in closed: monthly[t["exit_date"][:7]] += t["net_pnl"]
    avg_util = statistics.mean([tl["utilization"] for tl in timeline]) if timeline else 0
    peak_util = max((tl["utilization"] for tl in timeline), default=0)
    avg_dep = statistics.mean([tl["deployed"] for tl in timeline]) if timeline else 0
    avg_idle = statistics.mean([tl["cash"] for tl in timeline]) if timeline else 0
    dd_min = min((tl["drawdown"] for tl in timeline), default=0)
    dd_pct_min = min((tl["drawdown_pct"] for tl in timeline), default=0)

    return {
        "trades": closed, "skipped": skipped, "timeline": timeline,
        "metrics": {
            "starting_capital": starting_capital,
            "ending_equity": final["equity"],
            "total_pnl": final["equity"] - starting_capital,
            "return_on_starting_pct": (final["equity"] - starting_capital) / starting_capital * 100,
            "trades_taken": len(closed),
            "trades_skipped": len(skipped),
            "skipped_breakdown": {
                "insufficient_cash": sum(1 for s in skipped if s["reason"]=="insufficient_cash"),
                "max_concurrent": sum(1 for s in skipped if s["reason"]=="max_concurrent"),
            },
            "win_rate": (sum(1 for p in pnls if p > 0)/len(pnls)*100) if pnls else 0,
            "avg_win": statistics.mean(wins) if wins else 0,
            "avg_loss": statistics.mean(losses) if losses else 0,
            "win_loss_ratio": (statistics.mean(wins)/abs(statistics.mean(losses)))
                                if (wins and losses and statistics.mean(losses) != 0) else None,
            "best_trade": max(pnls) if pnls else 0,
            "worst_trade": min(pnls) if pnls else 0,
            "max_drawdown_rupees": dd_min,
            "max_drawdown_pct": dd_pct_min,
            "max_concurrent": max_concurrent,
            "avg_utilization": avg_util,
            "peak_utilization": peak_util,
            "avg_deployed": avg_dep,
            "avg_idle_cash": avg_idle,
            "monthly_pnl": dict(monthly),
        }
    }


def main():
    if not SIGS_JSON.exists():
        sys.exit(f"V3 signals JSON not found: {SIGS_JSON}")

    sigs = json.loads(SIGS_JSON.read_text())
    print(f"Loaded {len(sigs)} V3 signals.")

    # Filter to Pattern B fires (overlap allowed)
    b_sigs = [s for s in sigs if "B" in s["patterns"]]
    b_only = [s for s in sigs if s["patterns"] == ["B"]]
    print(f"Pattern B fires (overlap allowed): {len(b_sigs)}")
    print(f"Pattern B fires (alone, no overlap): {len(b_only)}")

    con = sqlite3.connect(DB)

    # Build Method-A trades
    trades_b      = build_trades_method_a(b_sigs, con)
    trades_b_only = build_trades_method_a(b_only, con)
    print(f"\nBuilt {len(trades_b)} Method-A trades from Pattern-B-overlap")
    print(f"Built {len(trades_b_only)} Method-A trades from Pattern-B-alone")

    # Unconstrained metrics (just trade outcomes)
    def trade_summary(trs):
        if not trs: return {}
        pnls = [t["net_pnl"] for t in trs]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        return {
            "n": len(trs),
            "win_rate": sum(1 for p in pnls if p > 0)/len(pnls)*100,
            "total_pnl": sum(pnls),
            "avg_win": statistics.mean(wins) if wins else 0,
            "avg_loss": statistics.mean(losses) if losses else 0,
            "wl_ratio": (statistics.mean(wins)/abs(statistics.mean(losses)))
                          if (wins and losses and statistics.mean(losses) != 0) else None,
            "best": max(pnls), "worst": min(pnls),
            "avg_ret_pct": statistics.mean([t["ret_pct"] for t in trs]),
            "hit_5pct": sum(1 for t in trs if t["ret_pct"] >= 5)/len(trs)*100,
            "hit_10pct": sum(1 for t in trs if t["ret_pct"] >= 10)/len(trs)*100,
        }

    sum_overlap = trade_summary(trades_b)
    sum_alone   = trade_summary(trades_b_only)

    # Cash-constrained (overlap variant)
    cc = cash_constrained_sim(trades_b, con)

    # Build report
    md = build_report(trades_b, trades_b_only, sum_overlap, sum_alone, cc)
    REPORT.write_text(md, encoding="utf-8")
    print(md)
    print(f"\nReport -> {REPORT}")


def build_report(t_overlap, t_alone, s_overlap, s_alone, cc):
    L = ["# Engine V4 — Pattern B only, Method A (blind 9:15 MOO), EOD data only", ""]
    L.append(f"- Hypothesis: Pattern B (Compression + strict OI) is the cleanest signal cluster.")
    L.append(f"- Operational pitch: needs only daily OHLC + futures OI. No intraday data, no 9:30 confirmation.")
    L.append(f"- Entry: market-on-open at 9:15 next day after EOD signal.")
    L.append(f"- Hold: T+5 close (5 trading days). Costs 30bps RT + 5bps slippage each side.")
    L.append("")

    # Method-A outcomes
    L.append("## Trade outcomes (no capital constraint)")
    L.append("")
    L.append("| Variant | n | WR | Hit +5% | Hit +10% | Mean ret | Avg win | Avg loss | W/L | Best | Worst | Total P&L |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for label, sm in (("B overlap (any B fire)", s_overlap),
                       ("B alone (only B, no C/D)", s_alone)):
        if not sm:
            L.append(f"| {label} | 0 | — | — | — | — | — | — | — | — | — | — |")
            continue
        L.append(f"| {label} | {sm['n']} | {sm['win_rate']:.1f}% | "
                 f"{sm['hit_5pct']:.1f}% | {sm['hit_10pct']:.1f}% | "
                 f"{sm['avg_ret_pct']:+.2f}% | {fmt_inr(sm['avg_win'])} | "
                 f"{fmt_inr(sm['avg_loss'])} | {sm['wl_ratio']:.2f} | "
                 f"{fmt_inr(sm['best'])} | {fmt_inr(sm['worst'])} | "
                 f"{fmt_inr(sm['total_pnl'])} |")
    L.append("")

    # Cash-constrained on B-overlap
    m = cc["metrics"]
    if m:
        L.append("## Cash-constrained simulation (B overlap, ₹30L start, 30-pos cap)")
        L.append("")
        L.append(f"- Starting capital: {fmt_inr(m['starting_capital'])}")
        L.append(f"- Ending equity: **{fmt_inr(m['ending_equity'])}**")
        L.append(f"- Total P&L: **{fmt_inr(m['total_pnl'])}**")
        L.append(f"- Return on starting: **{m['return_on_starting_pct']:+.2f}%**")
        L.append(f"- Trades taken: {m['trades_taken']}  ·  Skipped: {m['trades_skipped']} "
                 f"({m['skipped_breakdown']})")
        L.append(f"- Win rate: {m['win_rate']:.1f}%  ·  W/L: "
                 f"{m['win_loss_ratio']:.2f}" if m['win_loss_ratio'] else "—")
        L.append(f"- Best: {fmt_inr(m['best_trade'])}  ·  Worst: {fmt_inr(m['worst_trade'])}")
        L.append(f"- Max DD: {fmt_inr(m['max_drawdown_rupees'])} ({m['max_drawdown_pct']:+.2f}%)")
        L.append(f"- Max concurrent: {m['max_concurrent']} of 30")
        L.append(f"- Avg deployed: {fmt_inr(m['avg_deployed'])}  ·  Avg idle: {fmt_inr(m['avg_idle_cash'])}")
        L.append(f"- Avg util: {m['avg_utilization']*100:.1f}%  ·  Peak util: {m['peak_utilization']*100:.1f}%")
        L.append("")
        L.append("**Monthly P&L:**")
        L.append("")
        L.append("| Month | P&L |")
        L.append("|---|---|")
        for mo in sorted(m["monthly_pnl"]):
            L.append(f"| {mo} | {fmt_inr(m['monthly_pnl'][mo])} |")
        L.append("")

    # Comparison: V3 production vs V4
    L.append("## V3 production vs V4 (Pattern B only, Method A)")
    L.append("")
    L.append("| Engine | Signals | WR 5d | Hit +5% | Hit +10% | Mean ret | ROI on ₹30L | DD% |")
    L.append("|---|---|---|---|---|---|---|---|")
    L.append("| V3 (3-pat OR + Method B) | 83 | 65.1% | 31.3% | 6.0% | +1.71% | +3.53% | -1.63% |")
    if m:
        L.append(f"| **V4 (B-overlap, Method A)** | {m['trades_taken']} | "
                 f"{m['win_rate']:.1f}% | {s_overlap['hit_5pct']:.1f}% | "
                 f"{s_overlap['hit_10pct']:.1f}% | {s_overlap['avg_ret_pct']:+.2f}% | "
                 f"{m['return_on_starting_pct']:+.2f}% | {m['max_drawdown_pct']:+.2f}% |")
    L.append("")

    # Top trades
    L.append("## Top 10 winners (B-overlap, Method A)")
    L.append("")
    L.append("| Symbol | Entry | Exit | Patterns | OI 5d | Sub3-rng | P&L | Ret |")
    L.append("|---|---|---|---|---|---|---|---|")
    for t in sorted(t_overlap, key=lambda x: x["net_pnl"], reverse=True)[:10]:
        oi = f"{t['oi_growth']*100:+.0f}%" if t.get('oi_growth') is not None else "—"
        L.append(f"| {t['symbol']} | {t['entry_date']} | {t['exit_date']} | "
                 f"{t['patterns']} | {oi} | {t.get('n_sub_3','?')}/7 | "
                 f"{fmt_inr(t['net_pnl'])} | {t['ret_pct']:+.2f}% |")
    L.append("")
    L.append("## Bottom 10 losers (B-overlap, Method A)")
    L.append("")
    L.append("| Symbol | Entry | Exit | Patterns | OI 5d | Sub3-rng | P&L | Ret |")
    L.append("|---|---|---|---|---|---|---|---|")
    for t in sorted(t_overlap, key=lambda x: x["net_pnl"])[:10]:
        oi = f"{t['oi_growth']*100:+.0f}%" if t.get('oi_growth') is not None else "—"
        L.append(f"| {t['symbol']} | {t['entry_date']} | {t['exit_date']} | "
                 f"{t['patterns']} | {oi} | {t.get('n_sub_3','?')}/7 | "
                 f"{fmt_inr(t['net_pnl'])} | {t['ret_pct']:+.2f}% |")
    L.append("")
    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
