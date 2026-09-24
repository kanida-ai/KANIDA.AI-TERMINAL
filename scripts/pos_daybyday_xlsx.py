"""Day-by-day workbook so you can SEE the positional strategy run: the fixed-Rs5L /
3-sleeve book traced every calendar day, a trade log of every basket, and a 'trade
journey' showing each basket's basket-return at every hold day's close (D0..D3) with
its MFE/MAE and exit. Months: Apr / May / Jun 2026 (Apr+May complete, Jun to the 15th).

Each basket = one sleeve = Rs1,66,667 (Rs5L / 3). All Rs figures are on that sleeve basis,
so they tie exactly to the laddered book.
"""
import numpy as np
import pos_sim as P
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from pathlib import Path

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
OUT = ROOT / "docs" / "ops" / "FALCON_POSITIONAL_DAYBYDAY.xlsx"
ARM, FL, GV, ST, MH = 3.0, 1.0, 4.0, 6.0, 3
BOOK = 500000.0; SLOTS = 3; PER = BOOK / SLOTS
MONTHS = ["2026-04", "2026-05", "2026-06"]

F = "Calibri"
TITLE = Font(name=F, bold=True, size=13, color="1F6F8B"); NOTE = Font(name=F, size=9, italic=True, color="666666")
H = Font(name=F, bold=True, size=9, color="FFFFFF"); HDR = PatternFill("solid", start_color="1F6F8B")
NORM = Font(name=F, size=10); BOLD = Font(name=F, bold=True, size=10)
GRN = Font(name=F, size=10, color="1E7E34"); RED = Font(name=F, size=10, color="B00020")
GRNF = PatternFill("solid", start_color="E7F4EA"); REDF = PatternFill("solid", start_color="FDECEA")
AMBF = PatternFill("solid", start_color="FFF4CE"); NEWF = PatternFill("solid", start_color="E3F0F8")
thin = Side(style="thin", color="DDDDDD"); BORD = Border(left=thin, right=thin, top=thin, bottom=thin)
Cc = Alignment(horizontal="center"); Ll = Alignment(horizontal="left"); Rr = Alignment(horizontal="right")
RS = '#,##0;[Red]-#,##0'


def hdr(ws, r, heads, widths):
    for c, h in enumerate(heads, 1):
        x = ws.cell(r, c, h); x.font = H; x.fill = HDR; x.border = BORD; x.alignment = Cc
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w


def build_trades(ds):
    T = []
    for m in ds:
        r = P.basket_trail(m, ARM, FL, GV, ST, MH)
        retC, _ = P.paths(m)
        mm = P.mfe_mae_by_day(m)
        ed = r["exit_day"]
        T.append(dict(entry=m["signal_date"], exit=m["dates"][ed], exit_day=ed, hold=ed + 1,
                      reason=r["reason"], syms=m["syms"], ranks=m["rank"], nstk=m["nstocks"],
                      gross=r["gross"], net=r["net"], netrs=PER * r["net"] / 100.0,
                      closes=[float(retC[m["eod"][k]]) for k in range(ed + 1)],
                      mfe=[mm[k]["mfe"] for k in range(ed + 1)],
                      mae=[mm[k]["mae"] for k in range(ed + 1)],
                      peak=max(mm[k]["mfe"] for k in range(ed + 1)),
                      trough=min(mm[k]["mae"] for k in range(ed + 1))))
    return T


def book_trace(T):
    tmap = {}
    for t in T:
        tmap.setdefault(t["entry"], []).append(t)
    days = sorted(set([t["entry"] for t in T] + [t["exit"] for t in T]))
    sleeves = []; booked = 0.0; rows = []
    for d in days:
        ex = [s for s in sleeves if s["exit"] == d]
        real = sum(PER * s["net"] / 100.0 for s in ex)
        booked += real
        sleeves = [s for s in sleeves if s["exit"] > d]
        used = len(sleeves) * PER; free = BOOK - used
        opened = None
        if d in tmap and free >= PER - 1:
            t = tmap[d][0]; sleeves.append(dict(entry=d, exit=t["exit"], net=t["net"], syms=t["syms"]))
            opened = t; used += PER; free -= PER
        rows.append(dict(date=d, opened=opened, exits=ex, nopen=len(sleeves),
                         used=used, free=free, realized=real, book=BOOK + booked))
    return rows


def main():
    ds = P.load()
    T = build_trades(ds)
    inmonths = [t for t in T if t["entry"][:7] in MONTHS]
    rows = book_trace(T)
    rows = [r for r in rows if r["date"][:7] in MONTHS]
    # rebase the book-value curve so the shown window STARTS at Rs5L (fresh book on day 1),
    # rather than carrying cumulative P&L since 2024.
    if rows:
        offset = rows[0]["book"] - rows[0]["realized"] - BOOK
        for rr in rows:
            rr["book"] -= offset
    wb = Workbook()

    # ---------- Sheet 1: Book Day-by-Day ----------
    ws = wb.active; ws.title = "Book Day-by-Day"
    ws.cell(1, 1, "Fixed Rs5,00,000 book, 3 sleeves of Rs1,66,667 — traced every trading day (Apr-Jun 2026)").font = TITLE
    ws.cell(2, 1, "Each signal day opens ONE new Top-5 basket if a sleeve is free; a basket frees its sleeve when it exits. "
                  "Total deployed is hard-capped at Rs5L. 'Book value' = Rs5L + cumulative REALIZED P&L (open baskets marked at cost).").font = NOTE
    heads = ["date", "new basket opened (Top-5)", "baskets that EXITED today (freed a sleeve)", "# open", "deployed Rs", "free Rs", "realized P&L today", "book value Rs"]
    hdr(ws, 4, heads, [11, 34, 40, 7, 12, 11, 16, 14])
    r = 5
    for row in rows:
        op = ", ".join(row["opened"]["syms"]) if row["opened"] else "—"
        ex = " | ".join(f"{s['entry']} {s['syms'][0]}+{len(s['syms'])-1} ({PER*s['net']/100.0:+,.0f})" for s in row["exits"]) if row["exits"] else "—"
        vals = [row["date"], op, ex, row["nopen"], round(row["used"], 0), round(row["free"], 0),
                round(row["realized"], 0), round(row["book"], 0)]
        for c, v in enumerate(vals, 1):
            x = ws.cell(r, c, v); x.font = NORM; x.border = BORD; x.alignment = Ll if c in (2, 3) else Cc
            if c in (5, 6, 7, 8): x.number_format = RS
        if row["opened"]:
            ws.cell(r, 2).fill = NEWF
        if row["realized"] > 0.5: ws.cell(r, 7).font = GRN; ws.cell(r, 7).fill = GRNF
        elif row["realized"] < -0.5: ws.cell(r, 7).font = RED; ws.cell(r, 7).fill = REDF
        if row["free"] > 1:
            ws.cell(r, 6).fill = AMBF
        r += 1
    ws.freeze_panes = "A5"

    # ---------- Sheet 2: Trade Log ----------
    ws = wb.create_sheet("Trade Log")
    ws.cell(1, 1, "Trade Log — every basket entered Apr-Jun 2026 (one sleeve = Rs1,66,667)").font = TITLE
    heads = ["entry", "exit", "hold (sessions)", "exit reason", "n_stk", "symbols (rank)", "deployed Rs", "peak MFE %", "worst MAE %", "gross %", "NET %", "NET Rs"]
    hdr(ws, 3, heads, [11, 11, 14, 11, 6, 42, 12, 10, 10, 9, 8, 12])
    r = 4
    for t in inmonths:
        symstr = ", ".join(f"{s}({rk})" for s, rk in zip(t["syms"], t["ranks"]))
        vals = [t["entry"], t["exit"], t["hold"], t["reason"], t["nstk"], symstr, round(PER, 0),
                round(t["peak"], 2), round(t["trough"], 2), round(t["gross"], 2), round(t["net"], 2), round(t["netrs"], 0)]
        for c, v in enumerate(vals, 1):
            x = ws.cell(r, c, v); x.font = NORM; x.border = BORD; x.alignment = Ll if c in (1, 2, 4, 6) else Cc
            if c in (7, 12): x.number_format = RS
        fill = GRNF if t["net"] > 0 else (REDF if t["net"] < 0 else None)
        if fill:
            for c in range(1, 13): ws.cell(r, c).fill = fill
        r += 1
    ws.freeze_panes = "A4"; ws.auto_filter.ref = f"A3:L{r-1}"

    # ---------- Sheet 3: Trade Journey ----------
    ws = wb.create_sheet("Trade Journey")
    ws.cell(1, 1, "Trade Journey — each basket's return at every hold day's CLOSE (D0=entry day ... D3), with MFE/MAE").font = TITLE
    ws.cell(2, 1, "Day columns show the basket's GROSS return% at that session's close (blank after it exits). MFE/MAE = the best/worst "
                  "intraday point over the whole hold. Watch winners build across days (e.g. +2% -> +5% -> +8%) and losers get stopped.").font = NOTE
    heads = ["entry", "symbols", "D0 close%", "D1 close%", "D2 close%", "D3 close%", "peak MFE%", "worst MAE%", "exit day", "reason", "NET %"]
    hdr(ws, 4, heads, [11, 40, 10, 10, 10, 10, 10, 10, 9, 9, 8])
    r = 4
    r = 5
    for t in inmonths:
        symstr = ", ".join(t["syms"])
        dcl = t["closes"] + [None] * (4 - len(t["closes"]))
        vals = [t["entry"], symstr] + [round(x, 2) if x is not None else "" for x in dcl[:4]] + \
               [round(t["peak"], 2), round(t["trough"], 2), f"D{t['exit_day']}", t["reason"], round(t["net"], 2)]
        for c, v in enumerate(vals, 1):
            x = ws.cell(r, c, v); x.font = NORM; x.border = BORD; x.alignment = Ll if c == 2 else Cc
            if 3 <= c <= 6 and isinstance(v, (int, float)):
                x.font = GRN if v > 0 else (RED if v < 0 else NORM)
            if c == 11: x.font = GRN if t["net"] > 0 else RED
        # mark the exit-day close cell
        exc = 3 + t["exit_day"]
        if exc <= 6: ws.cell(r, exc).fill = AMBF
        r += 1
    ws.freeze_panes = "C5"

    # ---------- Sheet 4: Month summary ----------
    ws = wb.create_sheet("Month Summary")
    ws.cell(1, 1, "Month summary (baskets entered in month, on the Rs5L / 3-sleeve book)").font = TITLE
    hdr(ws, 3, ["month", "baskets", "avg net%/basket", "win", "win%", "best%", "worst%", "P&L Rs (on Rs5L)", "% on Rs5L"], [10, 8, 15, 6, 7, 8, 8, 18, 11])
    r = 4
    from collections import defaultdict
    bym = defaultdict(list)
    for t in inmonths: bym[t["entry"][:7]].append(t)
    for ym in sorted(bym):
        ts = bym[ym]; nets = np.array([x["net"] for x in ts]); pnl = sum(PER * x["net"] / 100.0 for x in ts)
        vals = [ym, len(ts), round(float(nets.mean()), 2), int((nets > 0).sum()), round(float((nets > 0).mean() * 100), 0),
                round(float(nets.max()), 2), round(float(nets.min()), 2), round(pnl, 0), round(pnl / BOOK * 100, 1)]
        for c, v in enumerate(vals, 1):
            x = ws.cell(r, c, v); x.font = NORM; x.border = BORD; x.alignment = Cc
            if c == 8: x.number_format = RS; x.font = GRN if v >= 0 else RED
            if c == 9: x.number_format = '0.0"%"'
        note = "  (partial: signals end 2026-06-15)" if ym == "2026-06" else ""
        if note: ws.cell(r, 1, ym + note)
        r += 1

    try:
        wb.save(OUT); out = OUT
    except PermissionError:
        out = OUT.with_name(OUT.stem + "_v2.xlsx"); wb.save(out)
    print(f"[*] WROTE {out}  ({len(inmonths)} baskets across {MONTHS}, sheets: {wb.sheetnames})")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
    main()
