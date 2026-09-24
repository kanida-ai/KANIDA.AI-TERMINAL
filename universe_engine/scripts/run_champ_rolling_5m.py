"""
Rolling 5-month walk-forward driver for the championship-style backtest.

Run a chosen signal across every monthly-shifted 5-month window from
window_start_min → window_end_max. Each window is fully isolated: signals are
fit/computed only on bars BEFORE window_start; nothing leaks across windows.

Output:
  - Per-window scorecard JSON  → reports/_champ_rolling_5m.json
  - Markdown summary           → reports/CHAMP_ROLLING_5M_REPORT.md

Usage:
    python scripts/run_champ_rolling_5m.py --signal momentum_rs --top-n 5
"""
from __future__ import annotations
import argparse, json, sqlite3, sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.champ_universe import (
    UniverseFilter, get_active_universe, build_calendar_from_abnormal_moves,
    ensure_corp_action_table,
)
from engine.champ_backtest import run_window


DB     = ROOT / "data" / "db" / "kanida_universe.db"
RES    = ROOT / "reports" / "_champ_rolling_5m.json"
REPORT = ROOT / "reports" / "CHAMP_ROLLING_5M_REPORT.md"


def add_months(d: date, n: int) -> date:
    """Add n months to date d, clamping day to last-of-month if needed."""
    y, m = d.year, d.month + n
    while m > 12:
        m -= 12; y += 1
    while m < 1:
        m += 12; y -= 1
    # Day clamp
    import calendar
    last = calendar.monthrange(y, m)[1]
    return d.replace(year=y, month=m, day=min(d.day, last))


def windows_monthly(start: date, hard_end: date, length_months: int = 5):
    """Yield (window_start, window_end) for each monthly-shifted N-month window.
    Last window must end on or before hard_end."""
    cur = start
    out = []
    while True:
        wend = add_months(cur, length_months) - timedelta(days=1)
        if wend > hard_end:
            break
        out.append((cur, wend))
        cur = add_months(cur, 1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--signal", default="momentum_rs")
    ap.add_argument("--top-n",  type=int, default=5)
    ap.add_argument("--start",  default="2021-01-01")
    ap.add_argument("--end",    default="2026-04-30")
    ap.add_argument("--length-months", type=int, default=5)
    ap.add_argument("--capital", type=float, default=1_000_000)
    ap.add_argument("--cost-bps", type=float, default=30.0)
    ap.add_argument("--slip-bps", type=float, default=5.0)
    ap.add_argument("--index", default="in_nifty200")
    args = ap.parse_args()

    if not DB.exists():
        sys.exit(f"ERROR: DB not found at {DB}")

    print("=" * 70)
    print(f"Champ rolling {args.length_months}-mo  | signal={args.signal}  top-N={args.top_n}")
    print(f"  Window envelope: {args.start} → {args.end}")
    print("=" * 70)

    con = sqlite3.connect(DB, timeout=60.0)

    # Build / refresh corp-action exclusion calendar
    ensure_corp_action_table(con)
    n_events = con.execute("SELECT COUNT(*) FROM corp_action_calendar").fetchone()[0]
    if n_events == 0:
        print("[setup] Corp-action calendar empty; building from abnormal moves...")
        build_calendar_from_abnormal_moves(con)

    universe = get_active_universe(con, index_col=args.index)
    print(f"  Universe: {len(universe)} symbols ({args.index})")
    universe_filter = UniverseFilter(con, universe)
    print(f"  Corp-action blocks loaded: {len(universe_filter._blocked):,} (sym, date) pairs")

    wins = windows_monthly(date.fromisoformat(args.start),
                            date.fromisoformat(args.end),
                            args.length_months)
    print(f"  Rolling windows: {len(wins)}")

    all_results = []
    for i, (ws, we) in enumerate(wins, 1):
        print(f"\n[{i:>2}/{len(wins)}] {ws} → {we} ...", flush=True)
        try:
            r = run_window(con, args.signal, ws.isoformat(), we.isoformat(),
                            top_n=args.top_n,
                            starting_capital=args.capital,
                            cost_bps=args.cost_bps,
                            slippage_bps=args.slip_bps,
                            index_col=args.index,
                            universe_filter=universe_filter)
        except Exception as e:
            print(f"   ERROR: {e}")
            r = {"error": str(e), "window_start": ws.isoformat(),
                 "window_end": we.isoformat(), "signal": args.signal}
        if "summary" in r:
            s = r["summary"]
            print(f"   ret={s['return_pct']:+.1f}%  MDD={s['max_dd_pct']:+.1f}%  "
                   f"trades={s['n_trades']}  closed={s['n_closed']}", flush=True)
        all_results.append(r)

    RES.parent.mkdir(parents=True, exist_ok=True)
    RES.write_text(json.dumps(all_results, indent=1, default=str))
    print(f"\nResults JSON  → {RES}")

    md = build_report(all_results, args)
    REPORT.write_text(md, encoding="utf-8")
    print(f"Markdown      → {REPORT}")


# ── Report ────────────────────────────────────────────────────────────────────

def build_report(results: list, args) -> str:
    L = []
    L.append(f"# Champ Rolling {args.length_months}-Mo Walk-Forward — {args.signal}")
    L.append("")
    L.append(f"- Universe: {args.index}, top-N concentration = {args.top_n}")
    L.append(f"- Capital: ₹{args.capital:,.0f}  ·  Cost: {args.cost_bps} bps RT  ·  Slip: {args.slip_bps} bps")
    L.append(f"- Window envelope: {args.start} → {args.end}")
    L.append(f"- N windows: {len(results)}")
    L.append("")
    L.append("## How to read this")
    L.append("Each row is one rolling 5-month window. **No leakage**: signal scores are "
             "computed strictly from bars before window_start; positions are managed "
             "with same-day data only. Forced exits at window_end on the last close.")
    L.append("")

    # Hit-rate summary
    hits_100 = sum(1 for r in results if r.get("summary", {}).get("return_pct", 0) >= 100)
    hits_50  = sum(1 for r in results if r.get("summary", {}).get("return_pct", 0) >= 50)
    hits_200 = sum(1 for r in results if r.get("summary", {}).get("return_pct", 0) >= 200)
    hits_300 = sum(1 for r in results if r.get("summary", {}).get("return_pct", 0) >= 300)
    rets = [r["summary"]["return_pct"] for r in results if "summary" in r]
    if rets:
        rets_s = sorted(rets, reverse=True)
        median_ret = rets_s[len(rets_s)//2]
    else:
        median_ret = 0
    L.append("## Headline — championship-threshold hit rate")
    L.append("")
    L.append(f"| Threshold | Hit / total |")
    L.append(f"|---|---|")
    L.append(f"| ≥ +50%   | {hits_50}/{len(results)} |")
    L.append(f"| ≥ +100%  | {hits_100}/{len(results)} |")
    L.append(f"| ≥ +200%  | {hits_200}/{len(results)} |")
    L.append(f"| ≥ +300%  | {hits_300}/{len(results)} |")
    L.append(f"| Median window return | {median_ret:+.1f}% |")
    L.append("")

    # Per-window table
    L.append("## Per-window results")
    L.append("")
    L.append("| Window | Ret% | MDD% | Trades | Closed | Top trades | Worst trade |")
    L.append("|---|---|---|---|---|---|---|")
    for r in results:
        if "summary" not in r:
            L.append(f"| {r['window_start']}→{r['window_end']} | ERR | — | — | — | "
                     f"{r.get('error','?')} | — |")
            continue
        s = r["summary"]
        trades = sorted(r.get("trades", []), key=lambda t: t.get("ret_pct", 0), reverse=True)
        top3 = trades[:3]
        worst = trades[-1] if trades else None
        top_str = ", ".join(f"{t['symbol']}+{t['ret_pct']:.0f}%" for t in top3) or "—"
        worst_str = (f"{worst['symbol']}{worst['ret_pct']:+.0f}%/{worst['reason']}"
                      if worst else "—")
        L.append(f"| {r['window_start']}→{r['window_end']} | "
                 f"{s['return_pct']:+.1f}% | {s['max_dd_pct']:+.1f}% | "
                 f"{s['n_trades']} | {s['n_closed']} | {top_str} | {worst_str} |")
    L.append("")

    # Best-window deep-dive
    if rets:
        best = max(results, key=lambda r: r.get("summary", {}).get("return_pct", -999))
        L.append("## Best window — full trade list")
        L.append("")
        L.append(f"**{best['window_start']} → {best['window_end']}**  ·  "
                 f"Return: {best['summary']['return_pct']:+.1f}%  ·  "
                 f"MDD: {best['summary']['max_dd_pct']:+.1f}%")
        L.append("")
        L.append("| Symbol | Open | Close | Avg Entry | Exit | Return | Adds | Reason |")
        L.append("|---|---|---|---|---|---|---|---|")
        for t in sorted(best["trades"], key=lambda t: t.get("ret_pct", 0), reverse=True):
            L.append(f"| {t['symbol']} | {t.get('open','?')} | {t.get('close','?')} | "
                     f"₹{t.get('avg_entry','?')} | ₹{t.get('close_price','?')} | "
                     f"{t.get('ret_pct',0):+.1f}% | {t.get('n_pyramid_adds',0)} | "
                     f"{t.get('reason','?')} |")

    L.append("")
    L.append("## Failure modes (windows where return ≤ 0%)")
    L.append("")
    losers = [r for r in results if r.get("summary", {}).get("return_pct", 0) <= 0]
    L.append(f"{len(losers)} of {len(results)} windows ended flat or down.")
    if losers:
        L.append("")
        L.append("| Window | Ret% | MDD% | Worst trade |")
        L.append("|---|---|---|---|")
        for r in losers[:15]:
            s = r["summary"]
            trades = sorted(r.get("trades", []), key=lambda t: t.get("ret_pct", 0))
            worst = trades[0] if trades else None
            worst_str = (f"{worst['symbol']}{worst['ret_pct']:+.0f}%/{worst['reason']}"
                          if worst else "—")
            L.append(f"| {r['window_start']}→{r['window_end']} | "
                     f"{s['return_pct']:+.1f}% | {s['max_dd_pct']:+.1f}% | {worst_str} |")

    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main() or 0)
