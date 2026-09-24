"""
Phase 1 + Phase 2 analysis on the existing walk-forward trade set:
  Phase 1 - Apply NIFTY 60d realised-vol regime gate (skip mid-vol trades).
  Phase 2 - Per-stock efficacy classification on label window (Aug 2021 - Dec 2024).
  Phase 3 - Validate classification on held-out window (Jan 2025 - Apr 2026).

Inputs:  scripts/_walkfwd_trades.json (14,514 trades from rolling walk-forward)
         data/db/kanida_quant.db      (NIFTY50 closes + ADV per ticker)
Outputs: STOCK_EFFICACY_REPORT.md, stock_efficacy.csv
"""
from __future__ import annotations
import json, sqlite3, math, sys
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TRADES_JSON = ROOT / "scripts" / "_walkfwd_trades.json"
DB          = ROOT / "data" / "db" / "kanida_quant.db"
REPORT_MD   = ROOT / "STOCK_EFFICACY_REPORT.md"
LEDGER_CSV  = ROOT / "stock_efficacy.csv"

CAPITAL_PER_TRADE = 100_000.0
COST_BPS_RT       = 30
LABEL_END         = "2024-12-31"
VAL_START         = "2025-01-01"

# Regime thresholds (NIFTY 60d realised vol, annualised %, terciles from full sample)
LOW_VOL_MAX  = 10.7
HIGH_VOL_MIN = 13.4

# Classification thresholds (label window only)
MIN_TRADES_LABEL    = 60
CORE_WR_MIN         = 0.38
CORE_PF_MIN         = 1.25
CORE_POS_MONTHS_MIN = 0.55
CORE_LOSS_STREAK_MAX_MONTHS = 3   # no >3 consecutive losing months
COND_PF_MIN         = 1.10
NOISY_PF_LO         = 0.95
NOISY_PF_HI         = 1.10
# (anything PF < NOISY_PF_LO at n>=MIN_TRADES_LABEL = "Avoid")


# ── data loading ─────────────────────────────────────────────────────────────

def load_nifty_vol_map():
    """Return {trade_date(YYYY-MM-DD): annualised_vol_pct} from rolling 60d realised vol."""
    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute(
        "SELECT trade_date, close FROM ohlc_daily WHERE ticker='NIFTY50' ORDER BY trade_date")]
    con.close()
    closes = {r["trade_date"]: r["close"] for r in rows}
    dates = sorted(closes.keys())
    rv = {}
    for i in range(60, len(dates)):
        rets = []
        for j in range(i-59, i+1):
            if j-1 < 0: continue
            p1 = closes[dates[j-1]]; p2 = closes[dates[j]]
            if p1 and p2 and p1 > 0:
                rets.append(math.log(p2/p1))
        if rets:
            mu = sum(rets)/len(rets)
            sd = math.sqrt(sum((x-mu)**2 for x in rets)/len(rets))
            rv[dates[i]] = sd * math.sqrt(252) * 100
    return rv


def load_adv_map():
    """Return {ticker: avg_daily_turnover_INR} over last 60 trading days."""
    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row
    cutoff = "2026-02-01"  # last ~60 trading days
    rows = list(con.execute(
        "SELECT ticker, AVG(close*volume) adv FROM ohlc_daily "
        "WHERE trade_date>=? GROUP BY ticker", (cutoff,)))
    con.close()
    return {r["ticker"]: float(r["adv"] or 0) for r in rows}


def classify_regime(vol):
    if vol is None:    return "unknown"
    if vol < LOW_VOL_MAX:  return "low_vol"
    if vol > HIGH_VOL_MIN: return "high_vol"
    return "mid_vol"


def add_regime(trades, vol_map):
    for t in trades:
        ed = t["entry_date"][:10]
        v  = vol_map.get(ed)
        # If exact match missing, find nearest preceding NIFTY day
        if v is None:
            for d in sorted(vol_map.keys(), reverse=True):
                if d <= ed:
                    v = vol_map[d]; break
        t["nifty_vol_pct"] = round(v, 2) if v is not None else None
        t["regime"] = classify_regime(v)
    return trades


def add_inr(trades):
    """Add ₹ P&L on ₹1L per trade, gross + net of 30 bps."""
    cost = (COST_BPS_RT/10000.0) * CAPITAL_PER_TRADE
    for t in trades:
        t["pnl_inr_gross"] = round(t["pnl_pct"]/100 * CAPITAL_PER_TRADE, 2)
        t["pnl_inr_net"]   = round(t["pnl_inr_gross"] - cost, 2)
    return trades


def add_liquidity_bucket(trades, adv_map):
    """Tag each trade with liquidity bucket of its underlying."""
    advs = sorted([v for v in adv_map.values() if v > 0])
    if not advs: return trades
    p33 = advs[len(advs)//3]; p66 = advs[2*len(advs)//3]
    for t in trades:
        a = adv_map.get(t["ticker"], 0)
        t["adv_inr"] = round(a, 0)
        t["adv_bucket"] = "low_liq" if a < p33 else ("high_liq" if a > p66 else "mid_liq")
    return trades, p33, p66


# ── Phase 1: Regime gate ─────────────────────────────────────────────────────

def portfolio_metrics(trades, name):
    if not trades:
        return {"name":name,"n":0}
    wins_inr = [t["pnl_inr_gross"] for t in trades if t["pnl_inr_gross"] > 0]
    losses_inr = [t["pnl_inr_gross"] for t in trades if t["pnl_inr_gross"] < 0]
    n=len(trades)
    pf = (sum(wins_inr) / -sum(losses_inr)) if losses_inr else float('inf')
    avg_win = (sum(wins_inr)/len(wins_inr)) if wins_inr else 0
    avg_loss = (sum(losses_inr)/len(losses_inr)) if losses_inr else 0
    payoff = (avg_win/-avg_loss) if avg_loss<0 else float('inf')

    # Concurrent
    sweep = []
    for t in trades:
        sweep.append((t["entry_date"][:10], +1))
        sweep.append((t["exit_date"][:10], -1))
    sweep.sort()
    cur=0; max_conc=0
    for d, delta in sweep:
        cur += delta; max_conc = max(max_conc, cur)
    max_capital = max_conc * CAPITAL_PER_TRADE

    # MTM closed-equity drawdown by exit date
    closed_by_date = defaultdict(float)
    for t in trades: closed_by_date[t["exit_date"][:10]] += t["pnl_inr_gross"]
    realized=peak=0.0; max_dd=0.0
    for d in sorted(closed_by_date):
        realized += closed_by_date[d]
        if realized > peak: peak = realized
        max_dd = max(max_dd, peak-realized)

    # Longest losing streak (consecutive non-positive ₹ exits)
    by_exit = sorted(trades, key=lambda x:(x["exit_date"], x["ticker"]))
    longest=cur=0
    for t in by_exit:
        if t["pnl_inr_gross"] <= 0: cur+=1; longest=max(longest,cur)
        else: cur=0

    # Per-month positive count
    by_m = defaultdict(float)
    for t in trades: by_m[t["entry_date"][:7]] += t["pnl_inr_net"]
    pos_months = sum(1 for v in by_m.values() if v>0)
    total_months = len(by_m)

    return {
        "name": name, "n": n,
        "wins": sum(1 for t in trades if t["pnl_inr_gross"]>0),
        "wr_inr": sum(1 for t in trades if t["pnl_inr_gross"]>0)/n,
        "wr_tp":  sum(1 for t in trades if t["exit_reason"]=="tp")/n,
        "avg_pct": sum(t["pnl_pct"] for t in trades)/n,
        "net_inr_gross": sum(t["pnl_inr_gross"] for t in trades),
        "net_inr_net":   sum(t["pnl_inr_net"]   for t in trades),
        "profit_factor": pf,
        "payoff_ratio": payoff,
        "avg_win_inr": avg_win, "avg_loss_inr": avg_loss,
        "max_concurrent": max_conc,
        "max_capital": max_capital,
        "roc_max_cap_gross": (sum(t["pnl_inr_gross"] for t in trades)/max_capital*100) if max_capital>0 else 0,
        "roc_max_cap_net":   (sum(t["pnl_inr_net"]   for t in trades)/max_capital*100) if max_capital>0 else 0,
        "max_dd_inr": max_dd,
        "max_dd_pct_of_maxcap": (max_dd/max_capital*100) if max_capital>0 else 0,
        "longest_losing_streak": longest,
        "pos_months": pos_months, "total_months": total_months,
        "pos_months_pct": (pos_months/total_months) if total_months>0 else 0,
    }


# ── Phase 2: Per-stock efficacy (label window) ───────────────────────────────

def per_stock_metrics(trades_label):
    """Return dict {ticker: metrics_dict} computed on label-window trades."""
    by_tk = defaultdict(list)
    for t in trades_label:
        by_tk[t["ticker"]].append(t)
    out = {}
    for tk, ts in by_tk.items():
        n=len(ts)
        if n<1: continue
        wins_inr = [t["pnl_inr_gross"] for t in ts if t["pnl_inr_gross"]>0]
        losses_inr = [t["pnl_inr_gross"] for t in ts if t["pnl_inr_gross"]<0]
        pf = (sum(wins_inr)/-sum(losses_inr)) if losses_inr else float('inf')
        avg_win = (sum(wins_inr)/len(wins_inr)) if wins_inr else 0
        avg_loss = (sum(losses_inr)/len(losses_inr)) if losses_inr else 0
        payoff = (avg_win/-avg_loss) if avg_loss<0 else float('inf')
        by_m = defaultdict(float)
        for t in ts: by_m[t["entry_date"][:7]] += t["pnl_inr_net"]
        pos_m = sum(1 for v in by_m.values() if v>0)
        tot_m = len(by_m)
        # longest consecutive losing months
        sorted_m = sorted(by_m)
        longest_loss_m = cur = 0
        for m in sorted_m:
            if by_m[m] <= 0: cur+=1; longest_loss_m = max(longest_loss_m, cur)
            else: cur=0
        # regime split
        reg_pnl = defaultdict(float); reg_n = defaultdict(int)
        for t in ts:
            reg_pnl[t["regime"]] += t["pnl_inr_gross"]
            reg_n[t["regime"]] += 1
        # liquidity bucket (ticker has one bucket)
        adv_bucket = ts[0].get("adv_bucket","?")
        adv_inr = ts[0].get("adv_inr", 0)

        max_dd_inr = 0.0; peak = 0.0; realized = 0.0
        by_exit_dates = sorted([(t["exit_date"][:10], t["pnl_inr_gross"]) for t in ts])
        for _, p in by_exit_dates:
            realized += p
            if realized > peak: peak = realized
            max_dd_inr = max(max_dd_inr, peak-realized)

        out[tk] = {
            "ticker": tk, "n": n,
            "wins": len(wins_inr),
            "losses": len(losses_inr),
            "wr_inr": len(wins_inr)/n,
            "avg_pct": sum(t["pnl_pct"] for t in ts)/n,
            "net_inr_gross": sum(t["pnl_inr_gross"] for t in ts),
            "net_inr_net": sum(t["pnl_inr_net"] for t in ts),
            "profit_factor": pf,
            "payoff_ratio": payoff,
            "avg_win_inr": avg_win, "avg_loss_inr": avg_loss,
            "pos_months": pos_m, "total_months": tot_m,
            "pos_months_pct": (pos_m/tot_m) if tot_m>0 else 0,
            "longest_losing_month_streak": longest_loss_m,
            "max_dd_inr": max_dd_inr,
            "regime_pnl_low": reg_pnl["low_vol"],
            "regime_pnl_high": reg_pnl["high_vol"],
            "regime_n_low":   reg_n["low_vol"],
            "regime_n_high":  reg_n["high_vol"],
            "adv_bucket": adv_bucket, "adv_inr": adv_inr,
        }
    return out


def classify_stock(m):
    if m["n"] < MIN_TRADES_LABEL:
        return "Insufficient_Data"
    pf = m["profit_factor"]
    pos_pct = m["pos_months_pct"]
    if pf < NOISY_PF_LO:
        return "Avoid"
    if NOISY_PF_LO <= pf < NOISY_PF_HI:
        return "Noisy"
    # PF >= 1.10 zone
    if (pf >= CORE_PF_MIN and m["wr_inr"] >= CORE_WR_MIN
            and pos_pct >= CORE_POS_MONTHS_MIN
            and m["longest_losing_month_streak"] <= CORE_LOSS_STREAK_MAX_MONTHS):
        return "Core_Winners"
    return "Conditional_Winners"


# ── Phase 3: Validation ──────────────────────────────────────────────────────

def validate(trades_val, classification):
    """Compute val-window metrics by stock-class."""
    by_class = defaultdict(list)
    for t in trades_val:
        cls = classification.get(t["ticker"], "Insufficient_Data")
        by_class[cls].append(t)
    return {cls: portfolio_metrics(ts, cls) for cls, ts in by_class.items()}


# ── Report ───────────────────────────────────────────────────────────────────

def fmt_inr(x):
    if x is None: return "-"
    sign = "-" if x < 0 else ""
    return f"{sign}₹{abs(x):,.0f}"

def fmt_pf(x):
    if x == float('inf'): return "inf"
    return f"{x:.2f}"


def write_csv(per_stock, classes):
    import csv as _csv
    fields = ["ticker","class","n","wins","losses","wr_inr","avg_pct",
              "net_inr_gross","net_inr_net","profit_factor","payoff_ratio",
              "avg_win_inr","avg_loss_inr","pos_months","total_months",
              "pos_months_pct","longest_losing_month_streak","max_dd_inr",
              "regime_pnl_low","regime_pnl_high","regime_n_low","regime_n_high",
              "adv_bucket","adv_inr"]
    with open(LEDGER_CSV, "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f)
        w.writerow(fields)
        for tk, m in sorted(per_stock.items()):
            row = [tk, classes.get(tk,"Insufficient_Data")] + [m.get(k,"") for k in fields[2:]]
            row = [fmt_pf(x) if k=="profit_factor" or k=="payoff_ratio" else x
                   for x,k in zip(row, fields)]
            w.writerow(row)


def build_report(all_trades, gated_trades, label_trades, val_trades,
                 per_stock_label, classification, val_metrics, p33_adv, p66_adv):
    L = []
    L.append("# Per-Stock Signal Efficacy & Persistence — Walk-forward OOS")
    L.append("")
    L.append("**Long-only · overlap ≥ 0.85 · 18-month rolling retraining · 4-week embargo · recency λ=0.95**")
    L.append("")
    L.append("Two-phase analysis on the 14,514-trade rolling walk-forward (2021-08 → 2026-04):")
    L.append("- **Phase 1.** NIFTY 60-day realised-vol regime gate. Skip mid-vol trades (vol in 10.7–13.4% band).")
    L.append("- **Phase 2.** Per-stock efficacy classification on **label window Aug 2021 – Dec 2024**.")
    L.append("- **Phase 3.** Validate classifications on **held-out window Jan 2025 – Apr 2026**. Core Winners must outperform on data they were not selected on.")
    L.append("")
    L.append("All ledger figures use ₹1,00,000 per trade, blind next-open entry, engine TP/SL, 30 bps round-trip cost. No bucket labels (Turbo/Super/Standard/Trap), no tier, no smart entry.")
    L.append("")

    # ── Phase 1 headline ─────────────────────────────────────────────────────
    full = portfolio_metrics(all_trades, "Full universe (no gate)")
    gated = portfolio_metrics(gated_trades, "Regime-gated (skip mid-vol)")
    skipped_n = full["n"] - gated["n"]
    L.append("## Phase 1 — Regime gate result")
    L.append("")
    L.append(f"Mid-vol skipped: **{skipped_n}** trades dropped of {full['n']} ({skipped_n/full['n']*100:.1f}%).")
    L.append("")
    L.append("| | Full universe | **Regime-gated** |")
    L.append("|---|---|---|")
    L.append(f"| Trades | {full['n']:,} | **{gated['n']:,}** |")
    L.append(f"| WR (positive ₹) | {full['wr_inr']*100:.1f}% | **{gated['wr_inr']*100:.1f}%** |")
    L.append(f"| Avg P&L per trade | {full['avg_pct']:+.2f}% | **{gated['avg_pct']:+.2f}%** |")
    L.append(f"| Profit factor | {fmt_pf(full['profit_factor'])} | **{fmt_pf(gated['profit_factor'])}** |")
    L.append(f"| Payoff ratio (avg-win/avg-loss) | {fmt_pf(full['payoff_ratio'])} | {fmt_pf(gated['payoff_ratio'])} |")
    L.append(f"| Net P&L gross | {fmt_inr(full['net_inr_gross'])} | **{fmt_inr(gated['net_inr_gross'])}** |")
    L.append(f"| Net P&L (after 30 bps) | {fmt_inr(full['net_inr_net'])} | **{fmt_inr(gated['net_inr_net'])}** |")
    L.append(f"| Max concurrent | {full['max_concurrent']} | {gated['max_concurrent']} |")
    L.append(f"| Max capital | {fmt_inr(full['max_capital'])} | **{fmt_inr(gated['max_capital'])}** |")
    L.append(f"| RoC max-cap gross | {full['roc_max_cap_gross']:+.2f}% | **{gated['roc_max_cap_gross']:+.2f}%** |")
    L.append(f"| RoC max-cap net | {full['roc_max_cap_net']:+.2f}% | **{gated['roc_max_cap_net']:+.2f}%** |")
    L.append(f"| Max DD ₹ (closed-equity) | {fmt_inr(full['max_dd_inr'])} | {fmt_inr(gated['max_dd_inr'])} |")
    L.append(f"| Max DD % of max-cap | {full['max_dd_pct_of_maxcap']:.2f}% | {gated['max_dd_pct_of_maxcap']:.2f}% |")
    L.append(f"| Longest losing streak | {full['longest_losing_streak']} | {gated['longest_losing_streak']} |")
    L.append(f"| Net-positive months | {full['pos_months']}/{full['total_months']} ({full['pos_months_pct']*100:.0f}%) | **{gated['pos_months']}/{gated['total_months']} ({gated['pos_months_pct']*100:.0f}%)** |")
    L.append("")

    if gated['net_inr_net'] > 0:
        L.append(f"**Verdict P1:** regime gate **flips strategy from net-negative to net-positive**. Δ = {fmt_inr(gated['net_inr_net'] - full['net_inr_net'])} P&L improvement net of costs.")
    else:
        L.append(f"**Verdict P1:** regime gate improves but does not fully cover costs. Δ = {fmt_inr(gated['net_inr_net'] - full['net_inr_net'])} P&L improvement net.")
    L.append("")

    # ── Phase 2 — per-stock label-window classification ───────────────────────
    L.append("## Phase 2 — Per-stock classification (label window Aug 2021 – Dec 2024)")
    L.append("")
    L.append(f"Run on **regime-gated** trades during the label window only. Classification thresholds:")
    L.append(f"- **Core Winners**: n ≥ {MIN_TRADES_LABEL}, WR ≥ {CORE_WR_MIN*100:.0f}%, profit factor ≥ {CORE_PF_MIN}, "
             f"≥{CORE_POS_MONTHS_MIN*100:.0f}% of months net-positive, no losing-month streak > {CORE_LOSS_STREAK_MAX_MONTHS}")
    L.append(f"- **Conditional Winners**: profit factor ≥ {COND_PF_MIN} but fails one of the Core checks")
    L.append(f"- **Noisy**: profit factor in [{NOISY_PF_LO}, {NOISY_PF_HI})")
    L.append(f"- **Avoid**: profit factor < {NOISY_PF_LO}")
    L.append(f"- **Insufficient Data**: n < {MIN_TRADES_LABEL}")
    L.append("")

    cls_count = defaultdict(int)
    for tk, c in classification.items(): cls_count[c] += 1
    L.append("### Class distribution")
    L.append("")
    L.append("| Class | Stocks |")
    L.append("|---|---|")
    for c in ("Core_Winners","Conditional_Winners","Noisy","Avoid","Insufficient_Data"):
        L.append(f"| {c.replace('_',' ')} | {cls_count.get(c,0)} |")
    L.append("")

    # Class membership lists
    by_cls = defaultdict(list)
    for tk in sorted(classification):
        by_cls[classification[tk]].append(tk)
    L.append("### Class members")
    L.append("")
    for c in ("Core_Winners","Conditional_Winners","Noisy","Avoid","Insufficient_Data"):
        L.append(f"**{c.replace('_',' ')}** ({len(by_cls[c])}): " + ", ".join(by_cls[c]) if by_cls[c] else f"**{c.replace('_',' ')}** (0): _none_")
        L.append("")

    # Master rank table — top 30 by net_inr_net
    L.append("### Master rank — top 30 by label-window net P&L (after 30 bps)")
    L.append("")
    L.append("| Ticker | Class | n | WR | Avg % | PF | Payoff | Net₹ gross | Net₹ net | PosMo | LosStreak | ADV bucket |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    ranked = sorted(per_stock_label.values(), key=lambda x: -x["net_inr_net"])
    for m in ranked[:30]:
        cls = classification.get(m["ticker"],"?").replace("_"," ")
        L.append(f"| {m['ticker']} | {cls} | {m['n']} | {m['wr_inr']*100:.0f}% | {m['avg_pct']:+.2f}% | "
                 f"{fmt_pf(m['profit_factor'])} | {fmt_pf(m['payoff_ratio'])} | "
                 f"{fmt_inr(m['net_inr_gross'])} | {fmt_inr(m['net_inr_net'])} | "
                 f"{m['pos_months']}/{m['total_months']} | {m['longest_losing_month_streak']} | {m['adv_bucket']} |")
    L.append("")

    # Bottom 15
    L.append("### Bottom 15 by label-window net P&L")
    L.append("")
    L.append("| Ticker | Class | n | WR | Avg % | PF | Net₹ gross | Net₹ net | LosStreak |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for m in ranked[-15:]:
        cls = classification.get(m["ticker"],"?").replace("_"," ")
        L.append(f"| {m['ticker']} | {cls} | {m['n']} | {m['wr_inr']*100:.0f}% | {m['avg_pct']:+.2f}% | "
                 f"{fmt_pf(m['profit_factor'])} | {fmt_inr(m['net_inr_gross'])} | "
                 f"{fmt_inr(m['net_inr_net'])} | {m['longest_losing_month_streak']} |")
    L.append("")

    # Liquidity-conditioned edge
    L.append("### Edge by liquidity bucket (label window, regime-gated)")
    L.append("")
    L.append(f"ADV terciles: low_liq < ₹{p33_adv/1e7:.1f} Cr | mid_liq | high_liq > ₹{p66_adv/1e7:.1f} Cr")
    L.append("")
    by_liq = defaultdict(list)
    for t in label_trades: by_liq[t.get("adv_bucket","?")].append(t)
    L.append("| Bucket | Trades | WR | Avg % | PF | Net ₹ gross | Net ₹ net |")
    L.append("|---|---|---|---|---|---|---|")
    for k in ("low_liq","mid_liq","high_liq"):
        ts = by_liq.get(k, [])
        if not ts: continue
        m = portfolio_metrics(ts, k)
        L.append(f"| {k} | {m['n']} | {m['wr_inr']*100:.0f}% | {m['avg_pct']:+.2f}% | "
                 f"{fmt_pf(m['profit_factor'])} | {fmt_inr(m['net_inr_gross'])} | {fmt_inr(m['net_inr_net'])} |")
    L.append("")

    # ── Phase 3 — Validation ─────────────────────────────────────────────────
    L.append("## Phase 3 — Validation on held-out window (Jan 2025 – Apr 2026)")
    L.append("")
    L.append("Apply the **labels from Phase 2** (frozen) to validation trades. Test: do Core Winners outperform Avoid (and the universe) on data the classifier never saw?")
    L.append("")
    L.append("| Class | Trades | WR | Avg % | PF | Net ₹ gross | Net ₹ net | PosMo |")
    L.append("|---|---|---|---|---|---|---|---|")
    for cls in ("Core_Winners","Conditional_Winners","Noisy","Avoid","Insufficient_Data"):
        m = val_metrics.get(cls, {"n":0})
        if m["n"]==0:
            L.append(f"| {cls.replace('_',' ')} | 0 | — | — | — | — | — | — |")
            continue
        L.append(f"| {cls.replace('_',' ')} | {m['n']} | {m['wr_inr']*100:.0f}% | {m['avg_pct']:+.2f}% | "
                 f"{fmt_pf(m['profit_factor'])} | {fmt_inr(m['net_inr_gross'])} | "
                 f"{fmt_inr(m['net_inr_net'])} | {m['pos_months']}/{m['total_months']} |")
    L.append("")

    cw = val_metrics.get("Core_Winners",{"n":0,"net_inr_net":0,"profit_factor":0})
    av = val_metrics.get("Avoid",{"n":0,"net_inr_net":0,"profit_factor":0})
    val_universe = portfolio_metrics(val_trades, "Universe (val window, gated)")
    L.append(f"### Validation diagnostics")
    L.append("")
    L.append(f"- **Universe (regime-gated, val window):** PF={fmt_pf(val_universe['profit_factor'])}, net = {fmt_inr(val_universe['net_inr_net'])}")
    L.append(f"- **Core Winners (val window):** PF={fmt_pf(cw['profit_factor'])}, net = {fmt_inr(cw['net_inr_net'])}")
    L.append(f"- **Avoid (val window):** PF={fmt_pf(av['profit_factor'])}, net = {fmt_inr(av['net_inr_net'])}")
    L.append("")

    if cw["n"]>0 and av["n"]>0:
        cw_per = cw["net_inr_net"] / cw["n"]
        av_per = av["net_inr_net"] / av["n"]
        univ_per = val_universe["net_inr_net"] / max(1,val_universe["n"])
        L.append(f"- **Per-trade ₹ net (val window):** Core {cw_per:+,.0f} | Universe {univ_per:+,.0f} | Avoid {av_per:+,.0f}")
        L.append("")
        if cw_per > univ_per and cw_per > av_per:
            L.append("**Verdict P3:** Core Winners outperform both the universe and the Avoid bucket on out-of-label data. **Per-stock classification carries real OOS edge.**")
        elif cw_per > av_per and abs(cw_per - univ_per) / max(abs(univ_per),1) < 0.10:
            L.append("**Verdict P3:** Core Winners and Avoid clearly separate, but Core ≈ universe. Classification correctly identifies losers but doesn't lift winners materially. Negative-list use only.")
        elif cw_per <= univ_per:
            L.append("**Verdict P3:** Core Winners do **not** outperform the universe in the held-out window. Classification labels were noise. Recommend trading the regime-gated universe broadly without per-stock filter.")
        else:
            L.append("**Verdict P3:** mixed. Detail in numbers above.")
    L.append("")

    # ── Final actionable list ────────────────────────────────────────────────
    L.append("## Active trade list (recommended)")
    L.append("")
    if cw["n"]>0:
        cw_per = cw["net_inr_net"] / cw["n"]
        univ_per = val_universe["net_inr_net"] / max(1,val_universe["n"])
        if cw_per > univ_per:
            L.append("Tickers labelled **Core Winners** in Phase 2, that **also** validated in Phase 3:")
            L.append("")
            L.append(", ".join(sorted(by_cls["Core_Winners"])))
        else:
            L.append("Phase 3 validation did not confirm Core Winners as an outperformer over the universe in the held-out window. Recommendation: **trade the full regime-gated universe** until further evidence accumulates.")
    L.append("")
    L.append("**Excluded under all scenarios:** tickers labelled **Avoid** (consistent negative profit factor across label window):")
    L.append("")
    L.append(", ".join(sorted(by_cls["Avoid"])) if by_cls["Avoid"] else "_none_")
    L.append("")

    # One-line summary
    L.append("## One-line summary")
    L.append("")
    L.append(f"> Rolling walk-forward 2021-08 → 2026-04, long-only, overlap ≥ 0.85, 18-month retraining: "
             f"applying NIFTY-vol regime gate cuts {skipped_n:,} mid-vol trades and shifts net P&L from "
             f"{fmt_inr(full['net_inr_net'])} to {fmt_inr(gated['net_inr_net'])} after 30 bps cost. "
             f"Per-stock classification on label window Aug 2021–Dec 2024 produced {cls_count.get('Core_Winners',0)} "
             f"Core Winners, {cls_count.get('Avoid',0)} Avoid; held-out validation Jan 2025–Apr 2026 shows Core net "
             f"{fmt_inr(cw['net_inr_net'])} ({cw['n']} trades, PF={fmt_pf(cw['profit_factor'])}) vs Avoid "
             f"{fmt_inr(av['net_inr_net'])} ({av['n']} trades, PF={fmt_pf(av['profit_factor'])}).")
    L.append("")

    REPORT_MD.write_text("\n".join(L), encoding="utf-8")


def main():
    print("Loading walk-forward trades ...", flush=True)
    trades = json.loads(TRADES_JSON.read_text())
    print(f"  {len(trades)} trades", flush=True)

    print("Computing NIFTY 60d realised vol ...", flush=True)
    vol_map = load_nifty_vol_map()
    print(f"  {len(vol_map)} days mapped", flush=True)

    print("Computing ADV per ticker ...", flush=True)
    adv_map = load_adv_map()
    print(f"  {len(adv_map)} tickers", flush=True)

    print("Annotating trades with regime + ADV + ₹ ...", flush=True)
    trades = add_inr(trades)
    trades = add_regime(trades, vol_map)
    trades, p33_adv, p66_adv = add_liquidity_bucket(trades, adv_map)

    # Phase 1
    gated = [t for t in trades if t["regime"] != "mid_vol"]
    print(f"  Full: {len(trades)}  | Gated: {len(gated)}  (skipped {len(trades)-len(gated)} mid-vol)", flush=True)

    # Phase 2 windows
    label = [t for t in gated if t["entry_date"][:10] <= LABEL_END]
    val   = [t for t in gated if t["entry_date"][:10] >= VAL_START]
    print(f"  Label window: {len(label)} trades | Validation window: {len(val)}", flush=True)

    print("Computing per-stock metrics ...", flush=True)
    per_stock_label = per_stock_metrics(label)
    classification = {tk: classify_stock(m) for tk, m in per_stock_label.items()}
    print(f"  {len(per_stock_label)} tickers classified", flush=True)

    print("Validating on held-out window ...", flush=True)
    val_metrics = validate(val, classification)

    print("Writing report + CSV ...", flush=True)
    write_csv(per_stock_label, classification)
    build_report(trades, gated, label, val, per_stock_label, classification, val_metrics, p33_adv, p66_adv)
    print(f"\n  -> {REPORT_MD}", flush=True)
    print(f"  -> {LEDGER_CSV}", flush=True)


if __name__ == "__main__":
    main()
