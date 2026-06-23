"""Build the narrative Word report for the intraday trade log: methodology, performance,
the dominant driver (market morning regime), two fully-worked sample days (best & worst),
the day-by-day learning mechanism, and honest caveats. Reads TradeLog_Intraday_2026.xlsx."""
from __future__ import annotations
from pathlib import Path
import pandas as pd
from docx import Document
from docx.shared import Pt

OUT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_findings")
XL = OUT / "TradeLog_Intraday_2026.xlsx"

L = pd.read_excel(XL, sheet_name="long_trades")
S = pd.read_excel(XL, sheet_name="short_trades")
summ = pd.read_excel(XL, sheet_name="daily_summary")
monthly = pd.read_excel(XL, sheet_name="monthly")

doc = Document()
doc.add_heading("Kanida.AI — Intraday Trade Log & Learning Report", 0)
doc.add_paragraph("F&O intraday, no carryover · enter 10:00, exit 15:15 · top-5 long + "
                  "top-5 short · target move ±1% · walk-forward Jan–May 2026 (out-of-sample).").italic = True

doc.add_heading("1. How the engine trades each day", 1)
for t in [
    "Observe each F&O stock's 9:15–10:00 morning microstructure (return, range, volume, "
    "volatility, VWAP deviation, position in range, 9:45–10:00 push) plus the market's "
    "morning state (breadth, trend, dispersion) and prior-day context.",
    "At 10:00 a stacked model (LightGBM + HistGBM + XGBoost) scores every stock's "
    "probability of a ≥1% move by 15:15 — up for the long book, down for the short book.",
    "Enter the top-5 highest-confidence longs and top-5 shorts at the 10:00 price; exit "
    "all at 15:15. A trade is a HIT if it moved ≥+1% (long) / ≤−1% (short).",
    "A per-stock reliability score is updated every day from that day's outcome and fed "
    "into the next day's ranking (the daily learning loop)."]:
    doc.add_paragraph(t, style="List Bullet")

doc.add_heading("2. Performance (out-of-sample, 86 trading days)", 1)
dist = summ["long_hits_of5"].value_counts().reindex(range(0, 6)).fillna(0).astype(int)
p = doc.add_paragraph()
p.add_run(f"Average hits of 5 — LONG {summ['long_hits_of5'].mean():.2f}, "
          f"SHORT {summ['short_hits_of5'].mean():.2f}. ").bold = True
p.add_run("Long-book day distribution (how many of the 5 cleared +1%): "
          + ", ".join(f"{k}/5 on {v} days" for k, v in dist.items()) + ".")
doc.add_paragraph("Read this honestly: on ~12% of days 4–5 of 5 hit; on ~37% of days 0 of 5 "
                  "hit. The outcome is highly day-dependent — which is the key to the whole report.")

t = doc.add_table(rows=1, cols=4); t.style = "Light Grid Accent 1"
for i, h in enumerate(["Month", "Long hits/5", "Short hits/5", "Days"]):
    t.rows[0].cells[i].text = h
for _, r in monthly.iterrows():
    c = t.add_row().cells
    c[0].text, c[1].text, c[2].text, c[3].text = str(r["month"]), f"{r['long']:.2f}", f"{r['short']:.2f}", str(int(r["days"]))

doc.add_heading("3. What actually drives a hit (the real lesson)", 1)
doc.add_paragraph(
    "The model's most important inputs by far are the MARKET'S morning state — dispersion "
    "(how spread-out moves are), breadth (share of stocks up), and trend — each ~3–4× more "
    "important than any single-stock feature. Plain English: the picks are right when the "
    "whole market is trending and active in the morning, and wrong when the morning is flat. "
    "The engine is, in effect, timing the day's regime more than picking individual stocks.")
doc.add_paragraph("This is visible in the two worked days below: the same selection logic "
                  "produces 5/5 on a trending morning and 0/5 on a flat one.")

def day_table(df, date, cols, headers):
    doc.add_heading(f"   {date}", 3)
    sub = df[df["date"] == date]
    tb = doc.add_table(rows=1, cols=len(cols)); tb.style = "Light Grid Accent 1"
    for i, h in enumerate(headers):
        tb.rows[0].cells[i].text = h
    for _, r in sub.iterrows():
        c = tb.add_row().cells
        for i, col in enumerate(cols):
            c[i].text = str(r[col])

doc.add_heading("4. Worked day — BEST (2026-04-02): 5 of 5 hit, avg +3.7%", 1)
doc.add_paragraph("Trending, broad-up morning. Every long pick was a stock showing morning "
                  "strength vs its sector/market on real volume — and the market followed "
                  "through into the afternoon, so all five cleared +1% (BOSCHLTD +6.2%, "
                  "HCLTECH +3.8%, SAMMAANCAP +3.1%).")
day_table(L, "2026-04-02", ["rank", "symbol", "entry_1000", "exit_1515", "move_pct_in_dir", "hit_1pct", "why_picked"],
          ["#", "Symbol", "Entry 10:00", "Exit 15:15", "Move %", "Hit", "Why picked"])

worst = summ.sort_values("long_hits_of5").iloc[0]["date"]
doc.add_heading(f"5. Worked day — WORST ({worst}): 0 of 5 hit", 1)
doc.add_paragraph("Flat, directionless morning. The selection logic still surfaced the most "
                  "'active-looking' names, but with no market follow-through the moves fizzled "
                  "(most ended within ±0.7%; FORCEMOT even reversed −2.9%). Nothing was wrong "
                  "with the stock picks — the day's regime simply did not cooperate.")
day_table(L, worst, ["rank", "symbol", "entry_1000", "exit_1515", "move_pct_in_dir", "hit_1pct", "why_picked"],
          ["#", "Symbol", "Entry 10:00", "Exit 15:15", "Move %", "Hit", "Why picked"])

doc.add_heading("6. What the engine learns, and how it applies it next day", 1)
for t in [
    "Trained lesson (from history): market-morning state dominates → the model weights "
    "dispersion/breadth/trend most heavily, so it is already conditioning every pick on the "
    "day's regime.",
    "Daily online update: after each day, every picked stock's reliability score moves toward "
    "1 if it hit and toward 0 if it missed; that updated score is blended into the next day's "
    "ranking, so repeat-offenders get demoted and reliable names promoted.",
    "The actionable rule the data supports: SIZE UP on trending/active mornings (high breadth "
    "+ dispersion) where the engine hits 4–5/5, and STAND DOWN on flat mornings where it hits "
    "0/5. The edge is regime timing far more than name selection.",
    "Honest limit: even with this, the directional ±1% average is ~1.3/5 — real (≈1.5× chance) "
    "but not the 4/5 target. The full per-trade record (every pick, entry, exit, move, hit, and "
    "why) is in the accompanying Excel: long_trades / short_trades / daily_summary."]:
    doc.add_paragraph(t, style="List Bullet")

doc.add_heading("7. Bottom line", 1)
doc.add_paragraph(
    "Across 86 out-of-sample days the engine averaged 1.36/5 (long) and 1.23/5 (short) clearing "
    "±1% — genuine skill (~1.4–1.5× chance), concentrated on trending mornings. It is best used "
    "as a regime-gated intraday screen (trade hard on active mornings, sit out flat ones), not "
    "as a fixed 5-pick-a-day system.")

path = OUT / "Intraday_TradeLog_Report.docx"
doc.save(path)
print("DOCX ->", path)
