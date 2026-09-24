"""
Run multi-TF IAS analysis (Lab adapted for our DB) and write a report
that compares 1m / 5m / 15m / 30m at the SAME 13:45-15:29 window.

Combines with the pattern-walk-forward result already in INTRADAY_PROOF_REPORT.md
to produce a single comprehensive INTRADAY_FULL_REPORT.md.
"""
from __future__ import annotations
import json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.ias_multi_tf import run_multi_tf

DB        = ROOT / "data" / "db" / "kanida_universe.db"
REPORT    = ROOT / "reports" / "IAS_MULTI_TF_REPORT.md"
COMBINED  = ROOT / "reports" / "INTRADAY_FULL_REPORT.md"
PARTIAL   = ROOT / "reports" / "INTRADAY_PROOF_REPORT.md"   # from pattern walk-forward
RESULTS_JSON = ROOT / "scripts" / "_ias_multi_tf_results.json"


def fmt_p(p):
    if p is None: return "—"
    if p < 0.001: return f"{p:.2e}"
    return f"{p:.4f}"


def build_md(results: dict) -> str:
    L = []
    L.append("# IAS Multi-Timeframe Causal Analysis")
    L.append("")
    L.append("**Adapted from Kanida Intraday Lab** — IAS (Institutional Accumulation Score) "
             "computed at the SAME 13:45-15:29 IST window using 4 different bar resolutions.")
    L.append("")
    L.append("- Window: **13:45 → 15:29 IST** (fixed across all TFs)")
    L.append("- IAS components scaled relative to bars-in-window per TF")
    L.append("- Signal threshold: IAS ≥ 5.5")
    L.append("- 'Up day' definition: next-day close-to-close ≥ 1.0%")
    L.append("- 5 statistical tests run per TF: Welch t-test base rates, dose-response trend, "
             "multi-day forward returns (Bonferroni), Granger causality")
    L.append("")

    # ── Headline comparison ──
    L.append("## Headline — IAS strength across timeframes")
    L.append("")
    L.append("| TF | Panel rows | Signal days | No-sig days | Hit rate (signal) | Hit rate (no-sig) | Lift | t-stat | p-value | Cohen's d |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for tf in ("1min", "5min", "15min", "30min"):
        if tf not in results: continue
        r = results[tf]
        t1 = r["test1_base_rates"]
        L.append(f"| {tf} | {r['panel_size']:,} | {t1['n_signal']} | {t1['n_nosignal']} | "
                 f"{t1['hit_signal']*100:.1f}% | {t1['hit_nosignal']*100:.1f}% | "
                 f"{t1['lift']:.2f} | {t1['t']:.2f} | {fmt_p(t1['p'])} | {t1['d']:.2f} |")
    L.append("")

    # ── Test 2: Dose-response ──
    L.append("## Test 2 — IAS Dose-Response (5 quintiles)")
    L.append("")
    L.append("Monotone increase in hit-rate across IAS quintiles = dose-response = causal evidence.")
    L.append("Cochran-Armitage trend test p-value reported per TF.")
    L.append("")
    for tf in ("1min", "5min", "15min", "30min"):
        if tf not in results or not results[tf]["test2_dose_response"]: continue
        L.append(f"**{tf}** (trend p = {fmt_p(results[tf]['test2_dose_response'][0]['trend_p'])}):")
        L.append("")
        L.append("| Quintile | IAS range | n | Hit rate | Mean fwd_d1 |")
        L.append("|---|---|---|---|---|")
        for r in results[tf]["test2_dose_response"]:
            L.append(f"| Q{r['quintile']} | {r['ias_lo']}–{r['ias_hi']} | {r['n']} | "
                     f"{r['hit_rate']*100:.1f}% | {r['mean_fwd_d1']:+.3f}% |")
        L.append("")

    # ── Test 3: Multi-day ──
    L.append("## Test 3 — Multi-day forward returns (D+1 to D+5, Bonferroni-corrected)")
    L.append("")
    L.append("| TF | Day | n_sig | n_nosig | Mean signal | Mean control | t | p (raw) | p (Bonf) | Cohen's d |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for tf in ("1min", "5min", "15min", "30min"):
        if tf not in results: continue
        for r in results[tf]["test3_multiday"]:
            L.append(f"| {tf} | D+{r['day']} | {r['n_signal']} | {r['n_nosignal']} | "
                     f"{r['mean_signal']:+.3f}% | {r['mean_nosignal']:+.3f}% | "
                     f"{r['t']:.2f} | {fmt_p(r['p'])} | {fmt_p(r['p_bonferroni'])} | "
                     f"{r['cohens_d']:.2f} |")
    L.append("")

    # ── Test 4: Granger ──
    L.append("## Test 4 — Granger causality (does IAS predict beyond lagged returns?)")
    L.append("")
    L.append("OLS F-test: full model `fwd_d1 ~ const + lag1 + lag2 + IAS` vs restricted `fwd_d1 ~ const + lag1 + lag2`.")
    L.append("p < 0.05 means IAS adds significant predictive power beyond pure return autocorrelation.")
    L.append("")
    L.append("| TF | n | F | df1 | df2 | p | IAS coefficient |")
    L.append("|---|---|---|---|---|---|---|")
    for tf in ("1min", "5min", "15min", "30min"):
        if tf not in results: continue
        r = results[tf]["test4_granger"]
        if "error" in r:
            L.append(f"| {tf} | {r.get('n','?')} | — | — | — | — | err: {r['error']} |")
            continue
        L.append(f"| {tf} | {r['n']} | {r.get('F','?')} | {r.get('df1','?')} | "
                 f"{r.get('df2','?')} | {fmt_p(r.get('p',1))} | {r.get('ias_coef','?')} |")
    L.append("")

    # ── Verdict ──
    L.append("## Verdict — which TF gives the strongest causal evidence?")
    L.append("")
    summary_rows = []
    for tf in ("1min", "5min", "15min", "30min"):
        if tf not in results: continue
        r = results[tf]
        score = 0
        # Signal-vs-control test
        t1 = r["test1_base_rates"]
        if t1["p"] < 0.05: score += 1
        # Dose-response
        if r["test2_dose_response"]:
            if r["test2_dose_response"][0]["trend_p"] < 0.05: score += 1
        # Multi-day Bonferroni
        sig_days = sum(1 for x in r["test3_multiday"] if x["p_bonferroni"] < 0.05)
        if sig_days >= 1: score += 1
        # Granger
        g = r["test4_granger"]
        if "p" in g and g["p"] < 0.05: score += 1
        summary_rows.append((tf, score, t1["lift"], sig_days,
                             g.get("p", 1) if "p" in g else 1))
    L.append("| TF | Tests passed (of 4) | Lift (Test 1) | Bonf-sig fwd days | Granger p |")
    L.append("|---|---|---|---|---|")
    for tf, score, lift, sd, gp in summary_rows:
        L.append(f"| {tf} | {score}/4 | {lift:.2f} | {sd}/5 | {fmt_p(gp)} |")
    L.append("")

    L.append("**Reading the verdict:**")
    L.append("- 4/4 = strong causal evidence at this TF — worth deploying")
    L.append("- 2-3/4 = real but partial signal — investigate further")
    L.append("- 0-1/4 = no causal evidence — IAS doesn't predict at this resolution")
    L.append("")

    return "\n".join(L)


def main():
    if not DB.exists():
        sys.exit(f"ERROR: {DB} not found")

    tfs = ["1min", "5min", "15min", "30min"]
    results = run_multi_tf(DB, tfs, n_workers=16)

    # Save raw results
    RESULTS_JSON.write_text(json.dumps(results, indent=1, default=str))

    md = build_md(results)
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(md, encoding="utf-8")
    print(f"\n  Report  → {REPORT}")
    print(f"  Results → {RESULTS_JSON}")

    # Combined report (pattern proof + IAS analysis)
    combined_parts = ["# Intraday Full Report — Pattern walk-forward + IAS causal analysis\n"]
    if PARTIAL.exists():
        combined_parts.append(f"## Part 1 — Pattern walk-forward (107 strategies × 30m/15m/5m)\n")
        combined_parts.append(PARTIAL.read_text(encoding="utf-8"))
        combined_parts.append("\n\n---\n\n")
    combined_parts.append(f"## Part 2 — IAS causal analysis (Kanida Lab method, multi-TF)\n")
    combined_parts.append(md)
    COMBINED.write_text("\n".join(combined_parts), encoding="utf-8")
    print(f"  Combined → {COMBINED}")


if __name__ == "__main__":
    main()
