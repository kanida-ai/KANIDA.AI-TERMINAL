"""
Run the intraday multi-timeframe proof:
  1. Run walk-forward on a single test month for each of {1m, 5m, 15m, 30m}
  2. Cross-TF classify: promoted / rejected_overfit / no_go
  3. Generate INTRADAY_PROOF_REPORT.md
"""
from __future__ import annotations
import argparse, json, sqlite3, sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.walkforward_intraday import (
    run_intraday_walkforward, cross_tf_classify, DEFAULTS, BARS_PER_DAY
)
from engine.family import deflated_sharpe_ratio

DB     = ROOT / "data" / "db" / "kanida_universe.db"
REPORT = ROOT / "reports" / "INTRADAY_PROOF_REPORT.md"
TRADES_JSON = ROOT / "scripts" / "_intraday_trades.json"


def fmt_inr(x):
    sign = "-" if x < 0 else ""
    return f"{sign}₹{abs(x):,.0f}"


def fmt_pf(x):
    if x is None: return "—"
    if isinstance(x, float) and (x != x or x == float('inf')):
        return "inf"
    return f"{x:.2f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, help="YYYY-MM")
    ap.add_argument("--tfs", default="30min,15min,5min",
                    help="comma-list, subset of: 1min,5min,15min,30min")
    ap.add_argument("--capital", type=float, default=100_000)
    ap.add_argument("--cost-bps", type=float, default=30)
    args = ap.parse_args()

    test_year, test_month = map(int, args.month.split("-"))
    tfs = [tf.strip() for tf in args.tfs.split(",") if tf.strip()]

    if not DB.exists():
        print(f"ERROR: DB not found at {DB}\n  Run scripts/setup.py first.")
        return 1

    con = sqlite3.connect(DB)
    cfg = dict(DEFAULTS)
    result = run_intraday_walkforward(con, tfs, test_year, test_month, cfg)
    con.close()

    # Cross-TF classification
    classes = cross_tf_classify(result["efficacy"], cfg)
    promoted = {s: c for s, c in classes.items() if c["classification"] == "promoted"}
    rejected = {s: c for s, c in classes.items() if c["classification"] == "rejected_overfit"}
    no_go    = {s: c for s, c in classes.items() if c["classification"] == "no_go"}

    # Save trades
    trades_dump = {}
    for tf, by_strat in result["trades"].items():
        trades_dump[tf] = {s: t for s, t in by_strat.items() if t}
    TRADES_JSON.write_text(json.dumps(trades_dump, indent=1, default=str))

    # ── Build report ──
    L = []
    L.append("# Intraday Proof Report")
    L.append("")
    L.append(f"**Test month: {args.month}** · Train: {result['train_start']} → {result['train_end']}")
    L.append("")
    L.append(f"Universe: 148 NSE stocks (Nifty 200 subset). 107 long-only strategies tested per timeframe.")
    L.append(f"Per-stock 20-day RS filter (top 33%) applied at entry. ATR-based stop, RR=2:1, end-of-day forced exit.")
    L.append(f"Cost model: {args.cost_bps} bps round-trip applied to win/loss accounting.")
    L.append("")
    L.append(f"Promotion rule: strategy must pass (n≥{cfg['min_trades_per_tf']}, PF≥{cfg['min_pf_pass']}, "
             f"WR after cost≥{cfg['min_wr_after_cost_pass']*100:.0f}%) on ≥{cfg['min_tfs_for_promotion']} of {len(tfs)} timeframes.")
    L.append("")

    # ── Section 1: Headline ──
    L.append("## 1. Headline")
    L.append("")
    L.append(f"| Outcome | Count | % of 107 |")
    L.append("|---|---|---|")
    L.append(f"| Promoted (passed ≥{cfg['min_tfs_for_promotion']} of {len(tfs)} TFs) | **{len(promoted)}** | {len(promoted)/107*100:.1f}% |")
    L.append(f"| Rejected as TF-overfit (passed 1 TF only) | {len(rejected)} | {len(rejected)/107*100:.1f}% |")
    L.append(f"| No-go (failed all TFs) | {len(no_go)} | {len(no_go)/107*100:.1f}% |")
    L.append("")

    # ── Section 2: Timeframe leaderboard ──
    L.append("## 2. Timeframe leaderboard")
    L.append("")
    L.append("Aggregate performance per TF: how productive is each timeframe?")
    L.append("")
    L.append("| Timeframe | Strategies passed | Total trades | Median PF net | Best PF net | Median WR after cost | Median Sharpe/trade |")
    L.append("|---|---|---|---|---|---|---|")
    for tf in tfs:
        if tf not in result["efficacy"]: continue
        tf_eff = result["efficacy"][tf]
        passed = sum(1 for e in tf_eff.values()
                     if e.get("n",0) >= cfg["min_trades_per_tf"]
                     and e.get("pf_net",0) >= cfg["min_pf_pass"]
                     and e.get("wr_after_cost",0) >= cfg["min_wr_after_cost_pass"])
        all_eff = [e for e in tf_eff.values() if e.get("n",0) > 0]
        all_pf  = sorted([e.get("pf_net",0) for e in all_eff])
        all_wr  = sorted([e.get("wr_after_cost",0) for e in all_eff])
        all_sh  = sorted([e.get("sharpe_per_trade",0) for e in all_eff])
        n_trades_total = sum(e.get("n",0) for e in all_eff)
        if all_pf:
            med_pf = all_pf[len(all_pf)//2]
            best_pf = all_pf[-1]
            med_wr = all_wr[len(all_wr)//2]
            med_sh = all_sh[len(all_sh)//2]
        else:
            med_pf=best_pf=med_wr=med_sh=0
        L.append(f"| {tf} | {passed} | {n_trades_total:,} | {fmt_pf(med_pf)} | {fmt_pf(best_pf)} | "
                 f"{med_wr*100:.0f}% | {med_sh:.2f} |")
    L.append("")

    # ── Section 3: Strategy-family leaderboard ──
    L.append("## 3. Strategy-family leaderboard")
    L.append("")
    L.append("Median PF net per family, aggregated across timeframes.")
    L.append("")
    by_family = defaultdict(list)
    for tf, tf_eff in result["efficacy"].items():
        for s, e in tf_eff.items():
            if e.get("n", 0) >= cfg["min_trades_per_tf"]:
                by_family[e.get("family", "other")].append({
                    "strategy": s, "tf": tf,
                    "pf_net": e.get("pf_net", 0),
                    "wr": e.get("wr_after_cost", 0),
                    "n": e.get("n", 0),
                })
    L.append("| Family | Tested rows | Median PF net | Best PF net | Strategies w/ PF≥1.10 |")
    L.append("|---|---|---|---|---|")
    for fam, rows in sorted(by_family.items(), key=lambda kv: -len(kv[1])):
        pfs = sorted([r["pf_net"] for r in rows])
        passing = sum(1 for r in rows if r["pf_net"] >= cfg["min_pf_pass"])
        if pfs:
            L.append(f"| {fam} | {len(rows)} | {fmt_pf(pfs[len(pfs)//2])} | "
                     f"{fmt_pf(pfs[-1])} | {passing} |")
    L.append("")

    # ── Section 4: Promoted candidates ──
    L.append(f"## 4. Promoted candidates ({len(promoted)})")
    L.append("")
    if not promoted:
        L.append("_No strategy passed on ≥2 timeframes. Daily-bar TA ceiling persists at intraday._")
        L.append("")
    else:
        L.append("These passed the per-TF bar on ≥2 timeframes — cross-TF consistency suggests real edge.")
        L.append("")
        L.append("| Strategy | Family | Passed TFs | Best TF (PF) | Mean PF (passed TFs) | Mean WR (passed TFs) |")
        L.append("|---|---|---|---|---|---|")
        ranked = sorted(promoted.items(),
                        key=lambda kv: -max((kv[1]["per_tf"].get(t,{}).get("pf_net",0) for t in kv[1]["passed_tfs"]),
                                            default=0))
        for s, c in ranked:
            best_pf = max(c["per_tf"].get(t,{}).get("pf_net",0) for t in c["passed_tfs"])
            mean_pf = sum(c["per_tf"][t].get("pf_net",0) for t in c["passed_tfs"]) / len(c["passed_tfs"])
            mean_wr = sum(c["per_tf"][t].get("wr_after_cost",0) for t in c["passed_tfs"]) / len(c["passed_tfs"])
            L.append(f"| {s} | {c['family']} | {','.join(c['passed_tfs'])} | "
                     f"{fmt_pf(best_pf)} | {fmt_pf(mean_pf)} | {mean_wr*100:.0f}% |")
        L.append("")

        # Per-promoted detailed breakdown
        L.append("### Promoted candidates — detail per TF")
        L.append("")
        for s, c in ranked[:15]:
            L.append(f"**{s}** ({c['family']}, passed on {len(c['passed_tfs'])} TFs)")
            L.append("")
            L.append("| TF | n | PF net | WR after cost | Avg net % | Sharpe/trade |")
            L.append("|---|---|---|---|---|---|")
            for tf in tfs:
                e = c["per_tf"].get(tf, {})
                if e.get("n", 0) == 0:
                    L.append(f"| {tf} | 0 | — | — | — | — |")
                    continue
                tag = "✓" if tf in c["passed_tfs"] else " "
                L.append(f"| {tf} {tag} | {e['n']} | {fmt_pf(e['pf_net'])} | "
                         f"{e['wr_after_cost']*100:.0f}% | {e['avg_net']:+.2f}% | "
                         f"{e['sharpe_per_trade']:.2f} |")
            L.append("")

    # ── Section 5: Rejected as TF-overfit ──
    L.append(f"## 5. Rejected as TF-overfit ({len(rejected)})")
    L.append("")
    L.append("These passed on exactly **one** timeframe and failed on others — single-TF outliers, almost certainly noise.")
    L.append("")
    if rejected:
        L.append("| Strategy | Family | Lone passing TF | That TF's PF net | n |")
        L.append("|---|---|---|---|---|")
        for s, c in sorted(rejected.items(), key=lambda kv: -kv[1]["per_tf"][kv[1]["passed_tfs"][0]].get("pf_net",0))[:30]:
            tf = c["passed_tfs"][0]
            e = c["per_tf"][tf]
            L.append(f"| {s} | {c['family']} | {tf} | {fmt_pf(e['pf_net'])} | {e['n']} |")
        if len(rejected) > 30:
            L.append(f"| _… and {len(rejected)-30} more_ |  |  |  |  |")
    L.append("")

    # ── Deflated Sharpe sanity ──
    L.append("## 6. Deflated Sharpe sanity check")
    L.append("")
    L.append(f"Total strategies tested: 107 × {len(tfs)} TFs = {107*len(tfs)} hypothesis tests.")
    L.append(f"Naïve best Sharpe-per-trade across all: ", )
    all_sh = []
    for tf, tf_eff in result["efficacy"].items():
        for s, e in tf_eff.items():
            if e.get("n",0) >= cfg["min_trades_per_tf"]:
                all_sh.append((s, tf, e.get("sharpe_per_trade",0), e.get("n",0)))
    if all_sh:
        all_sh.sort(key=lambda x: -x[2])
        top = all_sh[0]
        n_trials = len(all_sh)
        dsr = deflated_sharpe_ratio(top[2], n_trials=n_trials, n_obs=top[3])
        L.append("")
        L.append(f"- Best raw Sharpe/trade: **{top[0]}** on **{top[1]}** = {top[2]:.2f} (n={top[3]})")
        L.append(f"- Deflated Sharpe (adjusted for {n_trials} concurrent tests): **{dsr:.2f}**")
        if dsr > 1.0:
            L.append(f"- Verdict: even after multiple-comparisons adjustment, this strategy clears bar — strong evidence.")
        elif dsr > 0:
            L.append(f"- Verdict: marginal — survives selection bias but with low confidence.")
        else:
            L.append(f"- Verdict: **the apparent best result is consistent with selection bias**. With {n_trials} hypothesis tests, "
                     f"the expected best by pure noise has Sharpe ≈ that observed. Treat as null.")
    L.append("")

    # ── Final verdict ──
    L.append("## 7. Verdict")
    L.append("")
    if len(promoted) >= 5:
        L.append(f"**Real edge surfaces at intraday timeframes.** {len(promoted)} strategies pass cross-TF consistency. "
                 f"Recommend running full multi-month walk-forward on this promoted subset before live deployment.")
    elif len(promoted) >= 1:
        L.append(f"**Marginal evidence.** {len(promoted)} strategies pass cross-TF, but small set; could be lucky window. "
                 f"Recommend running 6+ test months before any deployment decision.")
    else:
        L.append(f"**Intraday timeframes do not lift edge above retail-cost ceiling either.** "
                 f"Same conclusion as daily-bar test: 107 well-known long-only TA patterns, even at finer time resolution, "
                 f"do not generate net-positive expectancy after 30 bps round-trip cost on this universe. "
                 f"Architecture is correct, methodology is correct — the data layer (price+volume only, retail-cost) is exhausted.")
        L.append("")
        L.append("Recommended next probes:")
        L.append("- F&O Open Interest + Implied Volatility term-structure (genuinely under-exploited in retail India)")
        L.append("- Cross-sectional factor models (relative momentum + value + quality + low-vol)")
        L.append("- Discretionary use of engine output (treat signals as a structured watchlist, add human regime/event judgment)")
    L.append("")

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text("\n".join(L), encoding="utf-8")
    print(f"\n  Report → {REPORT}")
    print(f"  Promoted: {len(promoted)} | Rejected (TF-overfit): {len(rejected)} | No-go: {len(no_go)}")


if __name__ == "__main__":
    sys.exit(main() or 0)
