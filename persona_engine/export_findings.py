"""
Export the full persona-expansion findings + analysis to a .xlsx workbook and a
.docx report. Read-only on the DB. Saves into the prod-tree outputs/persona_findings/.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from persona_engine import db

OUT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_findings")

# ---- precomputed result tables (from the walk-forward runs) ----
THRESH_BY_YEAR = pd.DataFrame([
    ["2023", "LONG", 11.2, 10.0, 8.4, 24.0, 1.86, 4.5],
    ["2023", "SHORT", 5.3, 4.5, 3.7, 28.7, 0.75, 4.9],
    ["2024", "LONG", 13.4, 11.5, 9.8, 23.5, 2.46, 4.0],
    ["2024", "SHORT", 5.5, 4.8, 4.0, 22.0, 1.38, 2.9],
    ["2025", "LONG", 6.4, 6.3, 5.8, 22.6, 1.59, 3.6],
    ["2025", "SHORT", 5.4, 4.0, 3.3, 21.7, 1.16, 2.8],
    ["2026", "LONG", 5.1, 4.5, 4.5, 13.6, 2.15, 2.1],
    ["2026", "SHORT", 3.9, 4.9, 4.1, 12.0, 1.74, 2.4],
], columns=["year", "direction", "precision@3_%", "precision@5_%", "precision@10_%",
            "recall_%", "base_rate_%", "lift_x"])

OVERLAP = pd.DataFrame([
    ["v1 EOD (price/vol/RS)", "Long Top-10 overlap", "2.9%", "below random (~4.7%)"],
    ["v1 EOD", "Short Top-10 overlap", "13.5%", "~3x random — real edge"],
    ["v2 EOD (magnitude+lags)", "Long Top-10 overlap", "10.3%", "static model beat v1 IC-learning"],
    ["v2 EOD", "Short Top-10 overlap", "14.6%", "~3x random"],
    ["v2 two-stage 9:45 GAP-confirm", "Long vs open->close [capturable]", "9.6%", "from open"],
    ["v2 two-stage 9:45 momentum", "Long vs 9:45->close [CAPTURABLE]", "8.5%", "honest tradeable"],
    ["v2 two-stage 9:45 NAME screen", "Long vs open->close [look-ahead]", "18-38%", "screen only, not capturable"],
    ["LT 4-week", "Top-10 overlap", "4.2%", "decays to ~0 in trending regime"],
    ["LT 8-week", "Top-10 overlap", "3.5%", "regime-dependent"],
], columns=["model", "metric", "result", "note"])

BASE_IC = pd.DataFrame([
    ["next-day return", "max |rank-IC| over all price features", "~0.04",
     "direction ~unpredictable; magnitude predictable"],
    ["20-day return (LT)", "max |rank-IC|", "~0.025", "mild mean-reversion (rs_index_60d -0.025)"],
    ["9:15->9:45 momentum vs 9:45->close", "rank-IC", "-0.015", "rest-of-day is efficient"],
    ["delivery% vs next-day", "rank-IC (2024)", "+0.049", "best single feature; did NOT survive integration"],
    ["earnings-day", "lift to be a top-10 mover", "2.0x", "but only 3.2% of daily movers had results"],
    ["+5% next-day move", "base rate (F&O)", "2.0%", "rare event -> bounds absolute precision"],
    ["-5% next-day move", "base rate (F&O)", "1.3%", "rare event"],
], columns=["target", "measure", "value", "interpretation"])

DATA_INV = pd.DataFrame([
    ["ohlc_daily", "2016-2026 (2020-21 thin ~138 syms)", "price/volume", "in DB"],
    ["falcon_features / outcomes", "2017-2026", "reused reference", "in DB"],
    ["ohlc_1min", "2024-05 -> 2026-05", "intraday opening confirm", "in DB"],
    ["corp_earnings_dates", "2022-2026, 66,295 rows / 2,454 syms", "earnings/results dates", "FETCHED from NSE"],
    ["delivery_daily", "2024-2026, 1.2M rows", "delivery % (conviction)", "FETCHED from NSE"],
    ["ohlc_futures_daily (OI)", "2026 only (~2 months)", "OI", "GAP - history not obtainable"],
    ["PCR / IV / options", "none", "options", "GAP - not available"],
    ["bulk/block deals history", "recent only", "institutional flow", "GAP - NSE API blocked"],
], columns=["dataset", "coverage", "signal_class", "status"])

EDGES = pd.DataFrame([
    ["F&O Short-side weakness", "~14-15% next-day Top-10 overlap (~3x random)", "directional, capturable"],
    ["F&O EOD magnitude Long", "~10% next-day Top-10 overlap", "directional, capturable"],
    ["F&O Threshold +5% Long", "precision@3 ~11-13% good yrs, 2-5x lift", "high-conviction mover screen"],
    ["F&O Threshold -5% Short", "precision@3 ~5%, 3-5x lift", "high-conviction mover screen"],
    ["09:45 'today's movers' screen", "~25-38% open->close overlap", "watchlist (look-ahead-flagged)"],
    ["'Will move big' magnitude screen", "strong, direction-agnostic", "volatility/event screen"],
], columns=["edge", "measured_performance", "type"])

FINDINGS_TEXT = [
    ("Bottom line",
     "Reliably predicting the EXACT next-day Top-10 gainers/losers is not achievable "
     "with any obtainable data: measured rank-IC <=0.04 (next-day) means direction is "
     "near-random; only magnitude (will-it-move-big) is predictable. The objective was "
     "revised to a practical threshold target (+/-5% next-day moves), where the model "
     "has real 2-5x skill though absolute precision is bounded by the rare ~2% base rate."),
    ("What was built",
     "Two additive self-learning personas (F&O + Long-Term) on the existing Falcon infra "
     "(nothing in Falcon/tier/auto-trade/portal touched). Stock->sector->persona agents, "
     "closed loop (predict->measure->learn), runtime Kite/NSE universe, daily real-time "
     "feedback loop, read-only portal access. Branch feat/persona-expansion."),
    ("F&O threshold model (current objective)",
     "Walk-forward HistGradientBoosting classifier -> P(>=+5%)/P(<=-5%) -> ranked Long/Short "
     "lists, retrained yearly (no lookahead). LONG precision@3 ~11-13% in normal regimes "
     "(5-6% in 2025-26), recall ~23%; SHORT @3 ~5%, recall ~22%. Best at high-conviction "
     "top-3. Hit differentiators: ATR, consolidation, delivery%, distance-from-high, 60d RS."),
    ("Data hunt result",
     "Fetched NEW non-price classes from NSE: earnings dates (2.0x mover-lift but only 3.2% "
     "of daily movers had results) and delivery% (best standalone IC +0.049 but did not "
     "survive integration). Neither cracks the target. OI/options history, news feeds, and "
     "bulk-deal history remain unobtainable."),
    ("Recommendation",
     "Adopt the threshold +/-5% framing as a high-conviction mover screen (top-3 strongest). "
     "Optional levers: lower the bar to +/-3% (higher base rate -> higher absolute precision), "
     "probability-cutoff signalling, regime gating to reduce 2025-26 decay, and live OI/options "
     "capture going forward to build history the backtest lacks."),
]


def _df(con, sql):
    return pd.read_sql_query(sql, con)


def build_xlsx(con, path: Path):
    sheets = {}
    ft = pd.DataFrame(FINDINGS_TEXT, columns=["section", "summary"])
    sheets["Findings_Summary"] = ft
    sheets["FO_Threshold_by_Year"] = THRESH_BY_YEAR
    sheets["Top10_Overlap_v1_v2_LT"] = OVERLAP
    sheets["Base_Rates_and_IC"] = BASE_IC
    sheets["Data_Inventory"] = DATA_INV
    sheets["Real_Edges"] = EDGES
    # live tables
    try:
        sheets["FO_Threshold_FeatureImp"] = _df(con,
            "SELECT trained_for year, direction, feature, importance "
            "FROM fo_threshold_feature_importance ORDER BY trained_for,direction,importance DESC")
        sheets["FO_Threshold_Outcomes"] = _df(con,
            "SELECT prediction_date,outcome_date,direction,symbol,ROUND(prob,3) prob,"
            "ROUND(actual_move,2) actual_move_pct,hit,false_positive "
            "FROM fo_threshold_outcomes ORDER BY prediction_date,direction,prob DESC")
        sheets["FO_Threshold_MissedMovers"] = _df(con,
            "SELECT outcome_date,direction,symbol,ROUND(actual_move,2) actual_move_pct,"
            "ROUND(prob,3) prob FROM fo_threshold_missed ORDER BY outcome_date")
    except Exception as e:
        print("  (threshold tables not all present:", e, ")")
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        for name, d in sheets.items():
            d.head(1_000_000).to_excel(xl, sheet_name=name[:31], index=False)
    print(f"XLSX -> {path}  ({len(sheets)} sheets)")
    return sheets


def build_docx(path: Path):
    from docx import Document
    from docx.shared import Pt, RGBColor
    doc = Document()
    doc.add_heading("Kanida.AI — Persona Expansion: Findings & Analysis", 0)
    doc.add_paragraph("F&O Trader + Long-Term Investor self-learning engines · "
                      "walk-forward 2022–2026 · generated 2026-06-22").italic = True

    for section, summary in FINDINGS_TEXT:
        doc.add_heading(section, level=1)
        doc.add_paragraph(summary)

    doc.add_heading("F&O Threshold model — precision by year (±5% next-day)", level=1)
    t = doc.add_table(rows=1, cols=len(THRESH_BY_YEAR.columns)); t.style = "Light Grid Accent 1"
    for i, c in enumerate(THRESH_BY_YEAR.columns):
        t.rows[0].cells[i].text = str(c)
    for _, r in THRESH_BY_YEAR.iterrows():
        cells = t.add_row().cells
        for i, v in enumerate(r):
            cells[i].text = str(v)

    doc.add_heading("Top-10 overlap across model versions", level=1)
    t2 = doc.add_table(rows=1, cols=len(OVERLAP.columns)); t2.style = "Light Grid Accent 1"
    for i, c in enumerate(OVERLAP.columns):
        t2.rows[0].cells[i].text = str(c)
    for _, r in OVERLAP.iterrows():
        cells = t2.add_row().cells
        for i, v in enumerate(r):
            cells[i].text = str(v)

    doc.add_heading("Why exact Top-10 / 80% is not reachable", level=1)
    t3 = doc.add_table(rows=1, cols=len(BASE_IC.columns)); t3.style = "Light Grid Accent 1"
    for i, c in enumerate(BASE_IC.columns):
        t3.rows[0].cells[i].text = str(c)
    for _, r in BASE_IC.iterrows():
        cells = t3.add_row().cells
        for i, v in enumerate(r):
            cells[i].text = str(v)

    doc.add_heading("Data inventory & gaps", level=1)
    t4 = doc.add_table(rows=1, cols=len(DATA_INV.columns)); t4.style = "Light Grid Accent 1"
    for i, c in enumerate(DATA_INV.columns):
        t4.rows[0].cells[i].text = str(c)
    for _, r in DATA_INV.iterrows():
        cells = t4.add_row().cells
        for i, v in enumerate(r):
            cells[i].text = str(v)

    doc.add_heading("Genuinely real, productisable edges", level=1)
    t5 = doc.add_table(rows=1, cols=len(EDGES.columns)); t5.style = "Light Grid Accent 1"
    for i, c in enumerate(EDGES.columns):
        t5.rows[0].cells[i].text = str(c)
    for _, r in EDGES.iterrows():
        cells = t5.add_row().cells
        for i, v in enumerate(r):
            cells[i].text = str(v)

    doc.save(path)
    print(f"DOCX -> {path}")


if __name__ == "__main__":
    stamp = sys.argv[1] if len(sys.argv) > 1 else "2026-06-22"
    OUT.mkdir(parents=True, exist_ok=True)
    con = db.connect(read_only=True)
    try:
        build_xlsx(con, OUT / f"Persona_Expansion_Findings_{stamp}.xlsx")
        build_docx(OUT / f"Persona_Expansion_Findings_{stamp}.docx")
    finally:
        con.close()
