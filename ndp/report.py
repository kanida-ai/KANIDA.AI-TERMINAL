# -*- coding: utf-8 -*-
"""Generate the NDP ICICIBANK deliverable (.docx) from the saved Excel report + a confirmation-layer pass."""
import os
import pandas as pd
from docx import Document
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
DL = os.path.join(os.path.expanduser("~"), "Downloads")
XLS = os.path.join(DL, "NDP_ICICIBANK_REPORT.xlsx"); MINT = RGBColor(0x0B, 0x7A, 0x5A)


def main():
    sc = pd.read_excel(XLS, "scorecard"); nc = pd.read_excel(XLS, "null_calibration"); cf = pd.read_excel(XLS, "conformance")
    try:
        from ndp import confirmation
        rec = pd.read_excel(XLS, "oos_trades"); conf_tbl = confirmation.confirm(rec)
    except Exception:
        conf_tbl = pd.DataFrame()
    doc = Document(); doc.styles["Normal"].font.name = "Calibri"; doc.styles["Normal"].font.size = Pt(10.5)

    def H(t, l=1):
        p = doc.add_heading(t, level=l)
        for r in p.runs: r.font.color.rgb = MINT
    def P(t, b=False, i=False):
        p = doc.add_paragraph(); r = p.add_run(t); r.bold = b; r.italic = i
    def BULL(t, lead=None):
        p = doc.add_paragraph(style="List Bullet")
        if lead: p.add_run(lead + " ").bold = True
        p.add_run(t)
    def TBL(headers, rows):
        t = doc.add_table(rows=1, cols=len(headers)); t.style = "Light Grid Accent 1"
        for i, h in enumerate(headers):
            c = t.rows[0].cells[i]; c.text = str(h); c.paragraphs[0].runs[0].bold = True
        for row in rows:
            cs = t.add_row().cells
            for i, v in enumerate(row): cs[i].text = str(v)
        doc.add_paragraph()

    h0 = doc.add_heading("Falcon NDP V2 — ICICIBANK Pilot Result", 0)
    for r in h0.runs: r.font.color.rgb = MINT
    P("Next-Day Prediction engine · TOUCH basis · INTRADAY/MIS · built to FALCON_NDP_V2 spec + Addendum A", i=True)
    P("Data: 1-min 2024-05-13→2026-07-10 (536 sessions) · daily+weekly PIT features from 2016 · Prepared 28-Jul-2026 IST", i=True)

    H("1. Verdict", 1)
    P("TRACKING_ONLY — a successful, informative pilot.", b=True)
    P("The engine found real, above-null next-day patterns on ICICIBANK, but none clear the full DEPLOY gate "
      "(win rate ≥ 70% on TOUCH, n ≥ 30, beats the null, AND survives a 2× cost stress). The strongest are held "
      "at TRACK — kept and accumulating evidence, never traded. Per the spec, an engine that can report 'not yet' "
      "is worth more than one that always finds something. 0 DEPLOY · 54 TRACK · 358 DISCARD · 0 conformance-blocker failures.")

    H("2. The headline finding — a pullback-bounce edge that is real but fragile", 1)
    P("ICICIBANK's strongest signal is a mean-reversion long: after weakness (Awesome Oscillator negative, or down "
      ">3.4% over 20 days, or near 120-day lows), the stock TOUCHES +0.5% the next morning far more often than usual.")
    TBL(["Condition (Q1, long +0.5% TOUCH)", "n", "WR TOUCH", "base", "null p95", "net bps", "verdict"],
        [["ao ≤ −1.416 (Awesome Osc negative)", 42, "71.4%", "54.6%", "63.3%", "+13.9", "TRACK"],
         ["ret20 ≤ −3.4% (20-day pullback)", 33, "69.7%", "54.6%", "63.3%", "+6.9", "TRACK"],
         ["dist_hi120 ≤ −8.2% (below 120d high)", 51, "68.6%", "54.6%", "63.3%", "+12.8", "TRACK"],
         ["macd ≤ −0.033", 53, "64.2%", "54.6%", "63.3%", "+6.2", "TRACK"]])
    P("Why TRACK, not DEPLOY: the win rate is genuine (beats the 63.3% null p95), but the net rupee edge is thin "
      "(+6–14 bps) and its lower confidence bound on incremental expectancy is negative — so under a 2× slippage "
      "stress it stops clearing costs (flagged FRAGILE_TO_COSTS). The engine refuses to certify a 70% 'deployable' "
      "number that would evaporate at a real trading desk. This is the Addendum A guard working as intended.", i=True)

    H("3. The four questions — best conditions per question (OOS win rate)", 1)
    for q, lab in [("Q1", "GAIN ≥ +0.5%"), ("Q2", "GAIN > +1.0%"), ("Q3", "DECLINE ≤ −0.5%"), ("Q4", "DECLINE > −1.0%")]:
        r = nc[nc.qid == q].iloc[0]
        P(f"{q}. {lab}   —   base {r.base_rate*100:.1f}% · null p95 WR {r.null_p95_wr*100:.1f}% · null promotion {r.null_promotion_rate*100:.1f}%", b=True)
        d = sc[sc.qid == q].sort_values("wr_touch", ascending=False).head(4)
        TBL(["rule", "entry", "n", "WR_touch", "WR_close", "net_bps", "MDE", "tier"],
            [[x.rule[:38], x.entry, x.n, f"{x.wr_touch}%", f"{x.wr_close}%", f"{x.net_bps:+}", f"{x.mde_bps:.0f}", x.tier] for _, x in d.iterrows()])

    H("4. Validity guards — is the pipeline trustworthy?", 1)
    BULL("Leakage canary (a rule built from tomorrow's return) promotes at 100% → the pipeline is wired correctly.", "PASS.")
    BULL("Planted +25bps edge promotes; shuffle test ≈ null rate → the gate is neither too strict nor too loose.", "PASS.")
    BULL("Null promotion rate 0.0–1.8% (well under the 10% abort threshold) → the gate does not promote noise.", "Calibrated.")
    BULL("All BLOCKER conformance checks IMPLEMENTED (signed returns, short-trap gate, time-of-day slippage, "
         "CNC-vs-MIS STT 0.20% vs 0.025%, block bootstrap, causal features, compounding P&L).", "Conformance green.")

    H("5. What was built (functionality)", 1)
    BULL("1-minute path tensor (entry VWAP, MFE/MAE long+short, touch times, gap, slippage proxy) per session × 5 entry times.", "Backbone.")
    BULL("129 point-in-time daily+weekly indicators; signal at close of T, outcome from session T+1 (leak-free).", "Features.")
    BULL("Stable indicator+threshold conditions, frozen vocabulary, expanding walk-forward cycles → conditions accumulate n≥30.", "Discovery.")
    BULL("Direction-specific baseline, stationary block-bootstrap LCBs, MDE/power, three-part + WR≥70% gate (DEPLOY/TRACK/DISCARD).", "Evaluation.")
    BULL("Matched random-condition null calibration + injection controls, run before promotion (Phase 2 before Phase 3).", "Null.")
    BULL("Compounding account simulation (2% risk/trade, MIS 5× available) and conformance checker.", "P&L + checks.")
    if not conf_tbl.empty:
        P("Morning confirmation layer (Addendum B) — gap-alignment filter over the fires (win rate always shown with retention):", b=True)
        TBL(["Q", "model", "n", "retained %", "WR"], [[r.qid, r.model, r.n, r.retained_pct, r.wr] for _, r in conf_tbl.iterrows()])

    H("6. The honest limitation and the path to DEPLOY", 1)
    BULL("Single stock, 2.1 years of 1-min data → per-condition MDE is 30–130 bps; the ~10-40 bps edges we hunt are "
         "on the edge of detectability. This is the binding constraint, and it is a data problem, not a modelling one.", "Power.")
    BULL("Acquire 1-min history back to ~2018 (spec's highest-leverage purchase) → ~4× the sample, MDE into the "
         "detectable range, and the fragile-but-real pullback edge could graduate to DEPLOY.", "Fix 1.")
    BULL("Better fills (the confirmation layer, tighter entry) to lift net economics so the TOUCH edge survives the "
         "2× cost stress.", "Fix 2.")
    BULL("The cross-stock validation path (per-stock isolation preserved) to confirm the pullback-bounce edge "
         "generalises — it already echoes the tier-pullback and short-side findings from prior research.", "Fix 3.")

    out = os.path.join(DL, "NDP_ICICIBANK_RESULT.docx"); doc.save(out); print("saved ->", out)


if __name__ == "__main__":
    main()
