"""Generate the futures long/short momentum strategy doc with a 3-week worked example
(both legs, day by day) from real futures 1-min data. Uses python-docx.
"""
from pathlib import Path
from docx import Document
from docx.shared import Pt, RGBColor, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
import fut_strategy_examples as X

OUT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine") / "docs" / "strategy" / "FALCON_FUTURES_MOMENTUM_STRATEGY.docx"
TEAL = RGBColor(0x1F, 0x6F, 0x8B); GREY = RGBColor(0x66, 0x66, 0x66)
GRN = RGBColor(0x1E, 0x7E, 0x34); RED = RGBColor(0xB0, 0x00, 0x20)


def run(par, text, bold=False, color=None, size=None, italic=False):
    r = par.add_run(text); r.bold = bold; r.italic = italic
    if color: r.font.color.rgb = color
    if size: r.font.size = Pt(size)
    return r


def h1(doc, t):
    p = doc.add_heading(level=1); run(p, t, bold=True, color=TEAL, size=15)
def h2(doc, t):
    p = doc.add_heading(level=2); run(p, t, bold=True, size=12)
def para(doc, t, **k):
    p = doc.add_paragraph(); run(p, t, **k); return p
def bullet(doc, t):
    p = doc.add_paragraph(style="List Bullet");
    for seg in _segs(t): run(p, seg[0], bold=seg[1])
def _segs(t):
    import re
    return [(s.strip("*"), s.startswith("**")) for s in re.split(r"(\*\*[^*]+\*\*)", t) if s]


def fill_table(doc, headers, rows, ret_cols=()):
    tb = doc.add_table(rows=1, cols=len(headers)); tb.style = "Table Grid"; tb.alignment = WD_TABLE_ALIGNMENT.CENTER
    for c, hh in enumerate(headers):
        cell = tb.rows[0].cells[c]; cell.paragraphs[0].clear()
        run(cell.paragraphs[0], hh, bold=True, size=8)
    for row in rows:
        cells = tb.add_row().cells
        for c, v in enumerate(row):
            cell = cells[c]; cell.paragraphs[0].clear()
            txt = str(v); col = None
            if c in ret_cols:
                try:
                    fv = float(str(v).replace("%", "").replace("+", "")); col = GRN if fv >= 0 else RED
                except ValueError:
                    col = None
            run(cell.paragraphs[0], txt, size=8, color=col, bold=(c in ret_cols))
    return tb


def main():
    ex = X.build_examples()
    doc = Document()
    doc.styles["Normal"].font.name = "Calibri"; doc.styles["Normal"].font.size = Pt(10)

    p = doc.add_paragraph(); run(p, "Falcon Futures — Long/Short Momentum Strategy", bold=True, color=TEAL, size=20)
    p = doc.add_paragraph(); run(p, "Proposed from exploratory analysis of real NFO stock-futures 1-min data", color=GREY, size=11)
    para(doc, "Draft for review · data window 2026-04-29 to 2026-07-03 · worked example 2026-06-16 to 2026-07-03 (3 weeks) · "
              "all example returns are GROSS (before cost).", color=GREY, size=9, italic=True)

    h1(doc, "1. The idea in one line")
    para(doc, "Each day, rank the liquid F&O stocks by how they moved yesterday. Go LONG the biggest winners and "
              "SHORT the biggest losers — using stock futures — because in this data winners keep rising and losers keep "
              "falling the next day, and the SHORT side is the stronger leg (which only futures let you trade freely).")
    para(doc, "Why futures (not equity): (1) you can short freely and hold overnight; (2) leverage via margin; "
              "(3) Open Interest (OI) is available as a confirming signal — none of which cash equity gives you.", size=9, color=GREY, italic=True)

    h1(doc, "2. What the data showed (the edges this is built on)")
    fill_table(doc,
        ["Signal (next-day open→close)", "Effect", "t-stat", "Reads as"],
        [["Yesterday's losers (bottom 20%)", "-0.52%", "-5.8", "keep falling → SHORT"],
         ["Yesterday's winners (top 20%)", "+0.40%", "+4.8", "keep rising → LONG"],
         ["Short-buildup (price down + OI up)", "-0.25%", "-4.9", "shorts continue"],
         ["Long-buildup (price up + OI up)", "+0.19%", "+4.1", "longs continue"],
         ["Big gap-down >1% at open", "+0.74%", "+6.3", "bounces intraday → time long entries here"],
         ["Big gap-up >1% at open", "-0.40%", "-3.1", "fades intraday → time short entries here"]],
        ret_cols=(1,))
    para(doc, "There is NO 'just be long' drift in this data (intraday ≈ flat, ~50% up), so the edge is cross-sectional "
              "(winners vs losers) and conditional (OI, gaps) — not market direction.", size=9, color=GREY, italic=True)

    h1(doc, "3. The rules")
    bullet(doc, "**Universe:** the liquid half of the F&O list each day (by futures turnover).")
    bullet(doc, "**Signal:** yesterday's close-to-close return of each stock's front-month future.")
    bullet(doc, "**LONG leg:** the top quintile (~20 biggest winners), preferring names with **long-buildup** (price up + OI up).")
    bullet(doc, "**SHORT leg:** the bottom quintile (~20 biggest losers), preferring names with **short-buildup** (price down + OI up).")
    bullet(doc, "**Entry timing:** enter at the 09:15 open; since big gaps mean-revert intraday, fill longs into gap-downs and shorts into gap-ups for better prices.")
    bullet(doc, "**Hold:** one day (enter 09:15, exit 15:29). Rebalance daily on the fresh ranking.")
    bullet(doc, "**Sizing:** equal-weight each leg; futures margin sets the leverage. Roughly market-neutral (long ≈ short notional), so it leans on stock selection, not market direction.")

    h1(doc, "4. Three weeks, day by day (worked example, GROSS)")
    para(doc, "Each trade day: enter yesterday's winners long and yesterday's losers short at 09:15, exit 15:29. "
              "'Long leg%' = average return of the long names; 'Short leg%' = average SHORT return (a falling stock is a "
              "positive short). 'Combined' = long + short. 'Cumulative' sums combined across the 3 weeks.", size=9, color=GREY, italic=True)
    rows = []; cum = 0.0
    for e in ex:
        combo = e["long_leg"] + e["short_leg"]; cum += combo
        rows.append([e["date"], e["n_long"], f"{e['long_leg']:+.2f}%", e["n_short"], f"{e['short_leg']:+.2f}%",
                     f"{combo:+.2f}%", f"{cum:+.2f}%"])
    fill_table(doc, ["Trade date", "# long", "Long leg %", "# short", "Short leg %", "Combined %", "Cumulative %"],
               rows, ret_cols=(2, 4, 5, 6))
    cl = sum(e["long_leg"] for e in ex); cs = sum(e["short_leg"] for e in ex)
    p = doc.add_paragraph(); run(p, f"Period total: long {cl:+.2f}%  ·  short {cs:+.2f}%  ·  combined {cl+cs:+.2f}% over {len(ex)} days (GROSS).", bold=True)
    para(doc, "Notice: there are losing days (e.g. 2026-06-29 combined -1.27%) and strong days (2026-06-19 +2.99%). The "
              "short leg carried the two strongest days — the free-shorting edge.", size=9, color=GREY, italic=True)

    h1(doc, "5. Name-level detail — three example days")
    pick = {e["date"]: e for e in ex}
    for d, label in [("2026-06-19", "a STRONG day (+2.99%)"), ("2026-06-24", "a MIXED day (+0.46%)"),
                     ("2026-06-29", "a LOSING day (-1.27%)")]:
        if d not in pick: continue
        e = pick[d]
        h2(doc, f"{d} — {label}")
        para(doc, "LONGS (yesterday's winners — bought 09:15, sold 15:29):", bold=True, size=9)
        lr = []
        for _, r in e["long_top"].iterrows():
            ob = "Yes" if (r["sig_ret"] > 0 and r["sig_oichg"] > 0) else "—"
            lr.append([r["symbol"], f"{r['sig_ret']*100:+.2f}%", ob, f"{r['opn']:.1f}", f"{r['cls']:.1f}", f"{r['day_ret']*100:+.2f}%"])
        fill_table(doc, ["Stock", "Yest. move", "OI build", "Entry", "Exit", "Trade P&L"], lr, ret_cols=(1, 5))
        para(doc, "SHORTS (yesterday's losers — sold 09:15, bought back 15:29):", bold=True, size=9)
        sr = []
        for _, r in e["short_top"].iterrows():
            ob = "Yes" if (r["sig_ret"] < 0 and r["sig_oichg"] > 0) else "—"
            sr.append([r["symbol"], f"{r['sig_ret']*100:+.2f}%", ob, f"{r['opn']:.1f}", f"{r['cls']:.1f}", f"{-r['day_ret']*100:+.2f}%"])
        fill_table(doc, ["Stock", "Yest. move", "OI build", "Entry", "Exit", "Short P&L"], sr, ret_cols=(1, 5))

    h1(doc, "6. What to notice")
    bullet(doc, "Individual names are noisy — some 'winners' fall and some 'losers' bounce on any given day. The edge is in the AVERAGE across ~20 names a side and in the tails, not any single trade.")
    bullet(doc, "The SHORT leg is the workhorse: shorting yesterday's biggest losers was the stronger, more consistent side (only futures let you do this overnight).")
    bullet(doc, "OI-buildup filters for conviction — names where positions are being ADDED in the direction of the move tend to follow through.")
    bullet(doc, "Being long-and-short at once keeps the book roughly market-neutral, so a red market day needn't be a red strategy day.")

    h1(doc, "7. Honest caveats (read before believing the numbers)")
    bullet(doc, "**Tiny sample:** only ~3 weeks of truly liquid front-month data (real 2-yr futures 1-min isn't available from Kite). Direction and statistical significance look real; the MAGNITUDE is optimistic.")
    bullet(doc, "**Costs bite:** this rebalances daily across ~40 names — high turnover. Gross ≈ +0.3-0.9%/day; after ~0.10%/leg cost the daily hit-rate falls near 50%, so the profit lives in the tails. Execution discipline is everything.")
    bullet(doc, "**Momentum crashes:** sharp trend reversals (the classic momentum risk) are NOT in this window. That is the real tail risk and we cannot see it without more history.")
    bullet(doc, "**Next step to trust it:** forward paper-trade net-of-cost, and/or get longer futures history from a data vendor before any real capital.")

    try:
        doc.save(OUT); out = OUT
    except PermissionError:
        out = OUT.with_name(OUT.stem + "_v2.docx"); doc.save(out)
    print(f"[*] WROTE {out}")


if __name__ == "__main__":
    main()
