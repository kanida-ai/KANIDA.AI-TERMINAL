"""
Build the walk-forward report from _walkfwd_trades.json.

Outputs WALKFWD_REPORT.md with:
- Headline summary (no-cap + day-12-cap)
- Monthly walk-forward summary
- Stock-level summary
- Atom-family generalization
- Portfolio metrics (max simul, max capital, MTM drawdown, longest losing streak)
- Regime-conditional analysis (NIFTY 60d realised vol bucket)
"""
from __future__ import annotations
import json, sqlite3, math, sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TRADES_JSON = ROOT / "scripts" / "_walkfwd_trades.json"
OUT_MD = ROOT / "WALKFWD_REPORT.md"

CAPITAL_PER_TRADE = 100_000.0
COST_BPS_RT       = 30
HOLD_CAP_DAYS     = 12

ATOM_FAMILIES = ("trend_20","flow","volatility","ma_position","breakout_state",
                 "volume","candle","sr_state","range_state","ma_slope","gap_state")


def add_inr(trades, pnl_field, exit_field):
    cum = 0.0
    by_exit = sorted(trades, key=lambda x: (x[exit_field], x["ticker"]))
    for t in by_exit:
        t[f"{pnl_field}_inr"] = round(t[pnl_field]/100*CAPITAL_PER_TRADE, 2)
        t[f"{pnl_field}_inr_net"] = round(t[f"{pnl_field}_inr"] - (COST_BPS_RT/10000)*CAPITAL_PER_TRADE, 2)
        cum += t[f"{pnl_field}_inr"]
        t[f"{pnl_field}_running_inr"] = round(cum, 2)
    return trades


def fmt_inr(x):
    sign = "-" if x < 0 else ""
    return f"{sign}₹{abs(x):,.0f}"


def fmt_pct(x): return f"{x:+.2f}%"


def compute_portfolio(trades, pnl_field, exit_field, days_field):
    """Compute portfolio-level stats: max simul, max cap, MTM drawdown, longest streak."""
    if not trades: return {}
    # Concurrency: count open positions per trading day
    # Trade is open from entry_date through exit_date inclusive
    open_count = defaultdict(int)
    all_dates = set()
    for t in trades:
        ed = t["entry_date"]; xd = t[exit_field]
        all_dates.add(ed); all_dates.add(xd)
    sorted_dates = sorted(all_dates)
    # Build a more detailed concurrency: walk every trading day in the test span
    span_start = min(t["entry_date"] for t in trades)
    span_end = max(t[exit_field] for t in trades)
    # Use the union of entry/exit dates as proxy "trading days"
    # Sufficient for max-concurrent estimation
    sweep = []
    for t in trades:
        sweep.append((t["entry_date"], +1))
        sweep.append((t[exit_field], -1))
    sweep.sort()
    cur=0; max_conc=0
    for d, delta in sweep:
        cur += delta
        max_conc = max(max_conc, cur)
    max_capital = max_conc * CAPITAL_PER_TRADE
    total_deployed = len(trades) * CAPITAL_PER_TRADE

    # MTM curve (closed P&L only, by exit date — simpler proxy)
    closed_by_date = defaultdict(float)
    for t in trades:
        closed_by_date[t[exit_field]] += t[f"{pnl_field}_inr"]
    realized = 0.0; peak = 0.0; max_dd = 0.0
    eq_curve = []
    for d in sorted(set(closed_by_date.keys())):
        realized += closed_by_date[d]
        if realized > peak: peak = realized
        dd = peak - realized
        max_dd = max(max_dd, dd)
        eq_curve.append({"date": d, "realized_inr": realized, "dd_inr": dd})

    # Longest losing streak (consecutive exits with non-positive P&L)
    by_exit = sorted(trades, key=lambda x: (x[exit_field], x["ticker"]))
    longest=cur=0
    for t in by_exit:
        if t[f"{pnl_field}_inr"] <= 0:
            cur += 1; longest = max(longest, cur)
        else:
            cur = 0

    return {
        "n": len(trades),
        "wins": sum(1 for t in trades if t["exit_reason"]=="tp" and pnl_field=="pnl_pct"
                    or pnl_field=="cap_pnl_pct" and t.get("cap_exit_reason","")=="tp"),
        "wr": sum(1 for t in trades if t[f"{pnl_field}_inr"]>0)/len(trades),
        "avg_pct": sum(t[pnl_field] for t in trades)/len(trades),
        "net_inr_gross": sum(t[f"{pnl_field}_inr"] for t in trades),
        "net_inr_30bps": sum(t[f"{pnl_field}_inr_net"] for t in trades),
        "total_deployed": total_deployed,
        "max_concurrent": max_conc,
        "max_capital": max_capital,
        "roc_max_capital_gross": (sum(t[f"{pnl_field}_inr"] for t in trades) / max_capital * 100) if max_capital>0 else 0,
        "roc_max_capital_net":   (sum(t[f"{pnl_field}_inr_net"] for t in trades) / max_capital * 100) if max_capital>0 else 0,
        "max_dd_inr": max_dd,
        "max_dd_pct_of_max_cap": (max_dd / max_capital * 100) if max_capital>0 else 0,
        "longest_losing_streak": longest,
        "eq_curve": eq_curve,
    }


def monthly_summary(trades, pnl_field, exit_field):
    by_m = defaultdict(lambda: {"n":0,"w":0,"l":0,"sum_pct":0.0,"sum_inr":0.0,"sum_inr_net":0.0})
    for t in trades:
        m = f"{t['test_year']}-{t['test_month']:02d}"
        s = by_m[m]; s["n"]+=1; s["sum_pct"]+=t[pnl_field]
        s["sum_inr"]+=t[f"{pnl_field}_inr"]; s["sum_inr_net"]+=t[f"{pnl_field}_inr_net"]
        if t[f"{pnl_field}_inr"]>0: s["w"]+=1
        else: s["l"]+=1
    return by_m


def stock_summary(trades, pnl_field):
    by_t = defaultdict(lambda: {"n":0,"w":0,"sum_pct":0.0,"sum_inr":0.0,"best":-1e9,"worst":1e9})
    for t in trades:
        s = by_t[t["ticker"]]; s["n"]+=1; s["sum_pct"]+=t[pnl_field]
        s["sum_inr"]+=t[f"{pnl_field}_inr"]
        if t[f"{pnl_field}_inr"]>0: s["w"]+=1
        if t[f"{pnl_field}_inr"]>s["best"]: s["best"]=t[f"{pnl_field}_inr"]
        if t[f"{pnl_field}_inr"]<s["worst"]: s["worst"]=t[f"{pnl_field}_inr"]
    return by_t


def atom_family_summary(trades, pnl_field):
    """For each atom family, count appearances in winners vs losers + WR by family."""
    fam = {f: {"trades":0,"wins":0,"sum_inr":0.0} for f in ATOM_FAMILIES}
    for t in trades:
        atoms = [a.strip() for a in str(t.get("pattern","")).split("+") if a.strip()]
        families_in_trade = set(a.split(":",1)[0] for a in atoms if ":" in a)
        for f in families_in_trade:
            if f not in fam: continue
            fam[f]["trades"] += 1
            fam[f]["sum_inr"] += t[f"{pnl_field}_inr"]
            if t[f"{pnl_field}_inr"]>0: fam[f]["wins"] += 1
    return fam


def regime_summary(trades, pnl_field):
    """Bucket by NIFTY 60-day realised vol at entry date. Compute per-bucket WR + avg."""
    con = sqlite3.connect(ROOT/"data"/"db"/"kanida_quant.db")
    con.row_factory = sqlite3.Row
    n50 = [dict(r) for r in con.execute(
        "SELECT trade_date, close FROM ohlc_daily WHERE ticker='NIFTY50' ORDER BY trade_date")]
    closes = {r["trade_date"]: r["close"] for r in n50}
    dates = sorted(closes.keys())
    # rolling 60-day realized vol
    rv = {}
    for i, d in enumerate(dates):
        if i < 60: continue
        rets = []
        for j in range(i-59, i+1):
            if j-1 < 0: continue
            p1 = closes.get(dates[j-1]); p2 = closes.get(dates[j])
            if p1 and p2 and p1>0:
                rets.append(math.log(p2/p1))
        if rets:
            mu = sum(rets)/len(rets)
            sd = (sum((x-mu)**2 for x in rets)/len(rets))**0.5
            rv[d] = sd * math.sqrt(252) * 100   # annualised %
    # bucket each trade
    rv_vals = sorted(rv.values())
    if not rv_vals: return {}
    p33 = rv_vals[len(rv_vals)//3]
    p66 = rv_vals[2*len(rv_vals)//3]
    buckets = defaultdict(lambda: {"n":0,"w":0,"sum_inr":0.0,"sum_pct":0.0})
    for t in trades:
        v = rv.get(t["entry_date"][:10])
        if v is None: continue
        b = "low_vol" if v<p33 else ("high_vol" if v>p66 else "mid_vol")
        s = buckets[b]; s["n"]+=1; s["sum_pct"]+=t[pnl_field]
        s["sum_inr"]+=t[f"{pnl_field}_inr"]
        if t[f"{pnl_field}_inr"]>0: s["w"]+=1
    con.close()
    return {"buckets":buckets, "p33":p33, "p66":p66}


def build_md(trades_nocap, trades_cap, port_nc, port_cap, mthly_nc, mthly_cap,
             stk_nc, fam_nc, reg_nc):
    L = []
    L.append("# Rolling Walk-forward — 2021-08 to 2026-04")
    L.append("")
    L.append("**Long-only · overlap ≥ 0.85 · trailing 18-month training · 4-week embargo · recency λ=0.95 · ₹1L per trade · 30 bps RT cost**")
    L.append("")
    L.append("Patterns are re-mined from scratch each month using only the 18 months of training data ending 4 weeks before the test month. The live test month is fully out-of-sample. Recency-weighting puts more weight on bars closer to the training-end anchor (most recent month ≈ 1.0, 12 months ago ≈ 0.54, 18 months ago ≈ 0.40). All bucket labels (Turbo/Super/Standard/Trap), tier, and engine 'smart entry' are excluded — only signal-time data drives selection.")
    L.append("")

    # Headline
    L.append("## Headline — both scenarios")
    L.append("")
    L.append("| | No-cap | Day-12 cap |")
    L.append("|---|---|---|")
    L.append(f"| Test months | {len(set((t['test_year'],t['test_month']) for t in trades_nocap))} | {len(set((t['test_year'],t['test_month']) for t in trades_cap))} |")
    L.append(f"| Total trades | {port_nc['n']} | {port_cap['n']} |")
    L.append(f"| Win rate (positive P&L) | {port_nc['wr']*100:.1f}% | {port_cap['wr']*100:.1f}% |")
    L.append(f"| Avg P&L per trade | {port_nc['avg_pct']:+.2f}% | {port_cap['avg_pct']:+.2f}% |")
    L.append(f"| **Net P&L gross** | **{fmt_inr(port_nc['net_inr_gross'])}** | **{fmt_inr(port_cap['net_inr_gross'])}** |")
    L.append(f"| Net P&L (after 30 bps RT) | {fmt_inr(port_nc['net_inr_30bps'])} | {fmt_inr(port_cap['net_inr_30bps'])} |")
    L.append(f"| Total deployed (sum) | {fmt_inr(port_nc['total_deployed'])} | {fmt_inr(port_cap['total_deployed'])} |")
    L.append(f"| Max concurrent open | {port_nc['max_concurrent']} | {port_cap['max_concurrent']} |")
    L.append(f"| **Max capital required** | **{fmt_inr(port_nc['max_capital'])}** | **{fmt_inr(port_cap['max_capital'])}** |")
    L.append(f"| RoC on max-capital (gross) | {port_nc['roc_max_capital_gross']:+.2f}% | {port_cap['roc_max_capital_gross']:+.2f}% |")
    L.append(f"| RoC on max-capital (net 30bps) | {port_nc['roc_max_capital_net']:+.2f}% | {port_cap['roc_max_capital_net']:+.2f}% |")
    L.append(f"| Max MTM drawdown (closed-equity) | {fmt_inr(port_nc['max_dd_inr'])} | {fmt_inr(port_cap['max_dd_inr'])} |")
    L.append(f"| Max DD (% of max-cap) | {port_nc['max_dd_pct_of_max_cap']:.2f}% | {port_cap['max_dd_pct_of_max_cap']:.2f}% |")
    L.append(f"| Longest losing streak | {port_nc['longest_losing_streak']} | {port_cap['longest_losing_streak']} |")
    L.append("")

    # Monthly walk-forward
    L.append("## Monthly walk-forward — every test month")
    L.append("")
    L.append("| Month | Trades | WR | Avg % | Net ₹ (gross) | Net ₹ (30bps) | Cap-12 Net ₹ |")
    L.append("|---|---|---|---|---|---|---|")
    months_sorted = sorted(set(mthly_nc.keys()))
    pos=neg=zero=0
    for m in months_sorted:
        s = mthly_nc[m]; sc = mthly_cap.get(m, {"sum_inr":0})
        n=s["n"] or 1
        L.append(f"| {m} | {s['n']} | {s['w']/n*100:.0f}% | {s['sum_pct']/n:+.2f}% | "
                 f"{fmt_inr(s['sum_inr'])} | {fmt_inr(s['sum_inr_net'])} | {fmt_inr(sc.get('sum_inr',0))} |")
        if s["sum_inr_net"]>0: pos+=1
        elif s["sum_inr_net"]<0: neg+=1
        else: zero+=1
    L.append("")
    L.append(f"**Net-positive months (after 30 bps cost): {pos} / {len(months_sorted)} ({pos/max(1,len(months_sorted))*100:.0f}%)**")
    L.append(f"Net-negative: {neg} | flat: {zero}")
    L.append("")
    if pos / max(1,len(months_sorted)) > 0.60:
        L.append("**Verdict marker:** ≥60% of months net-positive across walk-forward → genuine forward-persistent edge.")
    elif pos / max(1,len(months_sorted)) > 0.50:
        L.append("**Verdict marker:** marginal (~50% positive). Edge plausible but small; sensitive to costs and regime.")
    else:
        L.append("**Verdict marker:** <50% net-positive → no persistent forward edge. Engine is statistical noise after costs.")
    L.append("")

    # Year-cohort drift check
    L.append("### Edge drift across year cohorts (no-cap, gross)")
    L.append("")
    by_year = defaultdict(lambda: {"n":0,"w":0,"sum_inr":0.0})
    for m in months_sorted:
        y = m[:4]; s = mthly_nc[m]
        by_year[y]["n"]+=s["n"]; by_year[y]["w"]+=s["w"]; by_year[y]["sum_inr"]+=s["sum_inr"]
    L.append("| Year | Trades | WR | Net ₹ |")
    L.append("|---|---|---|---|")
    for y in sorted(by_year.keys()):
        s=by_year[y]; n=s["n"] or 1
        L.append(f"| {y} | {s['n']} | {s['w']/n*100:.0f}% | {fmt_inr(s['sum_inr'])} |")
    L.append("")

    # Stock-level
    L.append("## Stock-level summary (no-cap)")
    L.append("")
    L.append("| Ticker | Trades | WR | Avg % | Net ₹ | Best ₹ | Worst ₹ |")
    L.append("|---|---|---|---|---|---|---|")
    for tk in sorted(stk_nc.keys()):
        s = stk_nc[tk]; n=s["n"] or 1
        L.append(f"| {tk} | {s['n']} | {s['w']/n*100:.0f}% | {s['sum_pct']/n:+.2f}% | "
                 f"{fmt_inr(s['sum_inr'])} | {fmt_inr(s['best'])} | {fmt_inr(s['worst'])} |")
    L.append("")

    # Atom family
    L.append("## Atom-family generalization (no-cap)")
    L.append("")
    L.append("Each row = how many trades had at least one atom from this family in their winning pattern. Win rate is positive-P&L rate within those trades.")
    L.append("")
    L.append("| Family | Trades w/ atom | WR | Avg ₹ |")
    L.append("|---|---|---|---|")
    for f in ATOM_FAMILIES:
        s = fam_nc[f]; n = s["trades"] or 1
        L.append(f"| {f} | {s['trades']} | {s['wins']/n*100:.0f}% | {fmt_inr(s['sum_inr']/n)} |")
    L.append("")

    # Regime
    L.append("## Regime conditioning — NIFTY 60-day realised volatility (no-cap)")
    L.append("")
    if reg_nc:
        b = reg_nc["buckets"]
        L.append(f"Vol bucketed into terciles. Low/Mid/High thresholds: {reg_nc['p33']:.1f}% / {reg_nc['p66']:.1f}% (annualised).")
        L.append("")
        L.append("| Bucket | Trades | WR | Avg % | Net ₹ |")
        L.append("|---|---|---|---|---|")
        for k in ("low_vol","mid_vol","high_vol"):
            s = b.get(k, {"n":0,"w":0,"sum_pct":0,"sum_inr":0})
            n = s["n"] or 1
            L.append(f"| {k} | {s['n']} | {s['w']/n*100:.0f}% | {s['sum_pct']/n:+.2f}% | {fmt_inr(s['sum_inr'])} |")
        L.append("")

    # Closing tagline
    L.append("## One-line summary")
    L.append("")
    L.append(f"> Rolling-origin walk-forward across **{len(months_sorted)} test months** (2021-08 to 2026-04) "
             f"on **2026-style long-only signals (overlap ≥ 0.85)**, with monthly retraining on the trailing 18 "
             f"months under recency weighting (λ=0.95) and a 4-week embargo, produced **{port_nc['n']} trades**, "
             f"**{port_nc['wr']*100:.1f}% positive-P&L rate**, **{fmt_inr(port_nc['net_inr_gross'])} net P&L gross** "
             f"({fmt_inr(port_nc['net_inr_30bps'])} after 30 bps RT cost), **{fmt_inr(port_nc['max_capital'])} max capital required**, "
             f"**{port_nc['roc_max_capital_gross']:+.2f}% RoC on max-cap gross** ({port_nc['roc_max_capital_net']:+.2f}% net), "
             f"**{port_nc['max_dd_pct_of_max_cap']:.2f}% max drawdown**, and "
             f"**{pos}/{len(months_sorted)} months net-positive after costs**.")
    L.append("")
    OUT_MD.write_text("\n".join(L), encoding="utf-8")


def main():
    trades = json.loads(TRADES_JSON.read_text())
    print(f"Loaded {len(trades)} trades", flush=True)
    if not trades:
        print("No trades; nothing to report."); return

    # Build no-cap and day-12-cap projections
    nc = []
    for t in trades:
        c = dict(t); c["pnl_pct"] = t["pnl_pct"]
        c["pnl_pct_inr"] = round(c["pnl_pct"]/100*CAPITAL_PER_TRADE,2)
        c["pnl_pct_inr_net"] = round(c["pnl_pct_inr"] - (COST_BPS_RT/10000)*CAPITAL_PER_TRADE,2)
        nc.append(c)
    cap = []
    for t in trades:
        c = dict(t); c["cap_pnl_pct"] = t["cap_pnl_pct"]
        c["cap_pnl_pct_inr"] = round(c["cap_pnl_pct"]/100*CAPITAL_PER_TRADE,2)
        c["cap_pnl_pct_inr_net"] = round(c["cap_pnl_pct_inr"] - (COST_BPS_RT/10000)*CAPITAL_PER_TRADE,2)
        cap.append(c)

    port_nc  = compute_portfolio(nc,  "pnl_pct",     "exit_date",     "days_held")
    port_cap = compute_portfolio(cap, "cap_pnl_pct", "cap_exit_date", "cap_days_held")

    mthly_nc  = monthly_summary(nc,  "pnl_pct",     "exit_date")
    mthly_cap = monthly_summary(cap, "cap_pnl_pct", "cap_exit_date")

    stk_nc = stock_summary(nc, "pnl_pct")
    fam_nc = atom_family_summary(nc, "pnl_pct")
    reg_nc = regime_summary(nc, "pnl_pct")

    build_md(nc, cap, port_nc, port_cap, mthly_nc, mthly_cap, stk_nc, fam_nc, reg_nc)
    print(f"Wrote {OUT_MD}", flush=True)


if __name__ == "__main__":
    main()
