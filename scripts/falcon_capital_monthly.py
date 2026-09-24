"""Capital growth — deploy the running capital at each MONTH's beginning (start
Rs5L), let it grow/erode through the month from daily trade performance (SIMPLE
within the month: month P&L = sum of daily return% x month's opening capital),
then carry the closing balance to the next month (monthly compounding).
Recommended config incl. regime gate. GROSS of costs. Writes a new xlsx.
"""
import sys
sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\backend")
from collections import defaultdict
import numpy as np
from falcon_regime_filter import ROOT, strategy_returns, selfreg

START = 500000.0
MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def main():
    dates, rets = strategy_returns()
    gate = selfreg(dates, rets, 10)
    by = defaultdict(list)
    for d in dates:
        by[d[:7]].append((gate[d] > 0, rets[d]))

    rows = []
    cum = 0.0
    for ym in sorted(by):
        items = by[ym]
        traded = [r for t, r in items if t]
        wins = [r for r in traded if r > 0]; losses = [r for r in traded if r < 0]
        month_ret = sum(traded)                    # simple sum of daily % (traded days)
        opening = START                            # FRESH Rs5L each month (no carry / no compounding)
        pnl = opening * month_ret / 100.0
        cum += pnl
        rows.append(dict(year=int(ym[:4]), month=int(ym[5:7]), traded=len(traded), skip=len(items) - len(traded),
                         win=len(wins), loss=len(losses),
                         winp=round(len(wins) / len(traded) * 100, 1) if traded else 0.0,
                         opening=round(opening, 0), ret=round(month_ret, 2),
                         pnl=round(pnl, 0), closing=round(opening + pnl, 0)))

    total_pnl = cum
    print(f"[*] Fresh Rs{START:,.0f} each month (simple, no compounding), {len(rows)} months, gross.")
    print(f"[*] total P&L across all months: +Rs{total_pnl:,.0f}  (avg Rs{total_pnl/len(rows):,.0f}/month on a Rs5L stake)")
    best = max(rows, key=lambda r: r["ret"]); worst = min(rows, key=lambda r: r["ret"])
    print(f"    best month  {best['year']}-{best['month']:02d}: {best['ret']:+.1f}%  Rs{best['opening']:,.0f}->Rs{best['closing']:,.0f}")
    print(f"    worst month {worst['year']}-{worst['month']:02d}: {worst['ret']:+.1f}%  Rs{worst['opening']:,.0f}->Rs{worst['closing']:,.0f}")

    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    F = "Calibri"
    TITLE = Font(name=F, bold=True, size=13, color="1F6F8B"); NOTE = Font(name=F, size=9, italic=True, color="666666")
    H = Font(name=F, bold=True, size=9, color="FFFFFF"); HDR = PatternFill("solid", start_color="1F6F8B")
    NORM = Font(name=F, size=10); BOLD = Font(name=F, bold=True, size=10)
    GRN = Font(name=F, size=10, color="1E7E34"); RED = Font(name=F, size=10, color="B00020")
    ALT = PatternFill("solid", start_color="F5FAFB"); TOT = PatternFill("solid", start_color="FFF4CE")
    GRW = PatternFill("solid", start_color="E7F4EA"); ERO = PatternFill("solid", start_color="FDECEA")
    thin = Side(style="thin", color="DDDDDD"); BORD = Border(left=thin, right=thin, top=thin, bottom=thin)
    Cc = Alignment(horizontal="center"); Rr = Alignment(horizontal="right")
    RS = '#,##0;[Red]-#,##0'
    wb = Workbook(); ws = wb.active; ws.title = "Capital by Month"
    ws.cell(1, 1, "Capital by Month — deploy a FRESH Rs5,00,000 each month (simple, NO compounding)").font = TITLE
    ws.cell(2, 1, "Recommended config (Top-5 cash, arm2.5/floor1/give1.5/stop3, square-off 15:29, regime gate self-trail10>0). "
                  "Each month you deploy Rs5L; Month P&L = sum of daily return% (traded days) x Rs5L. Month-End = Rs5L +/- P&L. "
                  "Independent each month (no carry) so the number stays realistic. GROSS of costs.").font = NOTE
    heads = ["Year", "Month", "Traded", "Win", "Loss", "Win%", "Deployed Rs", "Month Ret%", "Month P&L Rs", "Month-End Rs", "Grew/Eroded"]
    for c, h in enumerate(heads, 1):
        cell = ws.cell(4, c, h); cell.font = H; cell.fill = HDR; cell.border = BORD; cell.alignment = Cc
    r = 5
    alt = False
    for row in rows:
        arrow = "GREW" if row["pnl"] >= 0 else "ERODED"
        vals = [row["year"], MONTHS[row["month"]], row["traded"], row["win"], row["loss"], row["winp"],
                row["opening"], row["ret"], row["pnl"], row["closing"], arrow]
        for c, v in enumerate(vals, 1):
            cell = ws.cell(r, c, v); cell.border = BORD; cell.font = NORM; cell.alignment = Cc if c <= 2 or c == 11 else Rr
            if c in (7, 9, 10): cell.number_format = RS
            if c in (6, 8): cell.number_format = '0.0"%"' if c == 6 else '0.00"%"'
            if c == 9: cell.font = GRN if v >= 0 else RED
        fill = GRW if row["pnl"] >= 0 else ERO
        for c in range(1, 12): ws.cell(r, c).fill = fill
        r += 1
    # total (sum of monthly P&Ls on a fixed Rs5L stake; cumulative pocketed)
    tw = sum(r["win"] for r in rows); tl = sum(r["loss"] for r in rows); ttr = sum(r["traded"] for r in rows)
    grew = sum(1 for r in rows if r["pnl"] >= 0); eroded = len(rows) - grew
    ws.cell(r, 1, "ALL").font = BOLD; ws.cell(r, 2, "TOTAL").font = BOLD
    # cols 3..11: Traded, Win, Loss, Win%, Deployed, MonthRet%, TotalP&L, CumPocketed(5L+total), Grew/Eroded
    tv = [ttr, tw, tl, round(tw / ttr * 100, 1), "", "", round(total_pnl, 0), round(START + total_pnl, 0),
          f"{grew} grew / {eroded} eroded"]
    for i, v in enumerate(tv, 3):
        cell = ws.cell(r, i, v); cell.font = BOLD; cell.alignment = Cc if i in (3, 4, 5, 6, 11) else Rr; cell.border = BORD; cell.fill = TOT
        if i in (9, 10): cell.number_format = RS
        if i == 6: cell.number_format = '0.0"%"'
    for c in (1, 2, 7, 8): ws.cell(r, c).fill = TOT; ws.cell(r, c).border = BORD
    for i, w in enumerate([6, 7, 7, 6, 6, 7, 13, 11, 13, 13, 12], 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "C5"
    out = ROOT / "docs" / "ops" / "AUTOTRADE_CAPITAL_MONTHLY.xlsx"
    wb.save(out)
    print(f"[*] WROTE {out}")


if __name__ == "__main__":
    main()
