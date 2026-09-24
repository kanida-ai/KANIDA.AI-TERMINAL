"""Simplified MONTHLY VIEW: realized capture (current live config) side-by-side
with the intraday OPPORTUNITY (basket MFE touch counts) and RISK (basket went
negative intraday = MAE). Top-5, 09:15, cash, same RND DB. Writes a clean xlsx."""
import sqlite3, math
from pathlib import Path
from collections import defaultdict
import numpy as np
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

ROOT = Path(__file__).resolve().parents[1]
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
PERSONA = "falcon_top10_daily"; ALIASES = {"ZOMATO": "ETERNAL"}
OPEN, CLOSE = "09:15:00", "15:29:00"; TOPN = 5; CAP = 100000.0
ARM, FLOOR, GIVE, STOP = 2.0, 1.0, 0.5, 1.5   # current live config (realized)
MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def load_signals(con):
    out = {}
    for ed, rk, sym in con.execute(
        "SELECT entry_date,engine_rank,symbol FROM falcon_signal_day_study "
        "WHERE persona=? AND engine_rank BETWEEN 1 AND ? ORDER BY entry_date,engine_rank",
        (PERSONA, TOPN)):
        out.setdefault(ed, []).append(sym)
    return out


def load_day(con, day, syms):
    fetch = {ALIASES.get(s, s): s for s in syms}; ph = ",".join("?" * len(fetch))
    rows = con.execute(
        f"SELECT symbol,substr(bar_time,12,5) hm,open,high,low,close FROM ohlc_1min "
        f"WHERE bar_time BETWEEN ? AND ? AND symbol IN ({ph})",
        [f"{day} {OPEN}", f"{day} {CLOSE}", *fetch.keys()]).fetchall()
    per = {}
    for osym, hm, o, h, l, c in rows:
        per.setdefault(fetch[osym], {})[hm] = (o, h, l, c)
    return per


def build(per, order):
    present = [(s, per[s]["09:15"][0]) for s in order
               if s in per and "09:15" in per[s] and per[s]["09:15"][0] and per[s]["09:15"][0] > 0]
    if not present:
        return None
    alloc = CAP / len(present)
    legs = [(s, e, math.floor(alloc / e)) for s, e in present if math.floor(alloc / e) >= 1]
    if not legs:
        return None
    syms = [s for s, _, _ in legs]
    entry = np.array([e for _, e, _ in legs], float); qty = np.array([q for _, _, q in legs], float)
    grid = sorted({m for s in syms for m in per[s]} | {"09:15"}); grid = [m for m in grid if m >= OPEN[:5]]

    def series(s, idx):
        vals, last = [], None
        for m in grid:
            v = per[s].get(m)
            if v and v[idx] and np.isfinite(v[idx]):
                last = v[idx]
            vals.append(last)
        e = per[s]["09:15"][0]
        return [x if x is not None else e for x in vals]

    close = np.column_stack([series(s, 3) for s in syms]); high = np.column_stack([series(s, 1) for s in syms])
    low = np.column_stack([series(s, 2) for s in syms]); opn = np.column_stack([series(s, 0) for s in syms])
    dep = float((qty * entry).sum())
    return dict(entry=entry, qty=qty, close=close, high=high, low=low, opn=opn, dep=dep, n=len(grid))


def sim(M):
    qty, close, opn, dep, n = M["qty"], M["close"], M["opn"], M["dep"], M["n"]
    port = close @ qty; ret = (port - dep) / dep * 100.0
    openval = opn @ qty; nxt = np.empty_like(openval); nxt[:-1] = openval[1:]; nxt[-1] = port[-1]
    armed = False; peak = None
    for i in range(n - 1):
        r = ret[i]
        if r <= -STOP:
            return (nxt[i] - dep) / dep * 100.0
        if not armed:
            if r >= ARM:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(FLOOR, peak - GIVE):
            return (nxt[i] - dep) / dep * 100.0
    return (port[-1] - dep) / dep * 100.0


def main():
    con = sqlite3.connect(str(RND)); sig = load_signals(con); days = sorted(sig)
    rows = []  # (year, month, ret, mfe, mae)
    for d in days:
        M = build(load_day(con, d, sig[d]), sig[d])
        if not M:
            continue
        r = sim(M)
        mfe = float((((M["high"] - M["entry"]) * M["qty"]).sum(axis=1) / M["dep"] * 100).max())
        mae = float((((M["low"] - M["entry"]) * M["qty"]).sum(axis=1) / M["dep"] * 100).min())
        rows.append((int(d[:4]), int(d[5:7]), r, mfe, mae))
    con.close()

    by = defaultdict(list)
    for y, m, r, mfe, mae in rows:
        by[(y, m)].append((r, mfe, mae))

    def agg(items):
        rr = np.array([x[0] for x in items]); mf = np.array([x[1] for x in items]); ma = np.array([x[2] for x in items])
        wins = rr[rr > 0]; losses = rr[rr < 0]; dneg = int((ma < 0).sum())
        return dict(days=len(rr), win=int((rr > 0).sum()), loss=int((rr < 0).sum()),
                    winp=round((rr > 0).mean() * 100, 1),
                    aw=round(float(wins.mean()), 2) if len(wins) else 0.0,
                    al=round(float(losses.mean()), 2) if len(losses) else 0.0,
                    summ=round(float(rr.sum()), 1),
                    d1=int((rr >= 1).sum()),
                    t05=int((mf >= 0.5).sum()), t1=int((mf >= 1).sum()), t15=int((mf >= 1.5).sum()),
                    t2=int((mf >= 2).sum()), t3=int((mf >= 3).sum()),
                    dneg=dneg, negp=round(dneg / len(rr) * 100, 1))

    # ---- workbook ----
    F = "Calibri"
    TITLE = Font(name=F, bold=True, size=13, color="1F6F8B")
    NOTE = Font(name=F, size=9, italic=True, color="666666")
    H = Font(name=F, bold=True, size=9, color="FFFFFF")
    G1 = PatternFill("solid", start_color="1F6F8B"); G2 = PatternFill("solid", start_color="2E8B9E"); G3 = PatternFill("solid", start_color="B0662A")
    NORM = Font(name=F, size=10); BOLD = Font(name=F, bold=True, size=10)
    ALT = PatternFill("solid", start_color="F5FAFB"); TOT = PatternFill("solid", start_color="FFF4CE")
    thin = Side(style="thin", color="DDDDDD"); BORD = Border(left=thin, right=thin, top=thin, bottom=thin)
    Cc = Alignment(horizontal="center"); Rr = Alignment(horizontal="right")

    wb = Workbook(); ws = wb.active; ws.title = "Monthly View"
    ws.cell(1, 1, "Monthly View — Capture vs Opportunity vs Risk").font = TITLE
    ws.cell(2, 1, "REALIZED = current live config (arm 2% / floor 1% / giveback 0.5% / stop 1.5%, cash Top-5). "
                  "TOUCH = days the basket reached that level intraday (MFE, the ceiling). NEG = days it went below 0 intraday (MAE).").font = NOTE
    # group header row
    groups = [("", 1, 3, None), ("REALIZED (what we captured)", 4, 11, G1),
              ("OPPORTUNITY — days basket TOUCHED intraday (MFE)", 12, 16, G2),
              ("RISK (MAE)", 17, 18, G3)]
    gr = 4
    for name, c1, c2, fill in groups:
        if name:
            ws.merge_cells(start_row=gr, start_column=c1, end_row=gr, end_column=c2)
            cell = ws.cell(gr, c1, name); cell.font = H; cell.fill = fill; cell.alignment = Cc; cell.border = BORD
    hr = gr + 1
    heads = ["Year", "Month", "Days", "Win", "Loss", "Win%", "Avg_win%", "Avg_loss%", "Sum_month%",
             "Days≥+1%", ">+0.5%", ">+1%", ">+1.5%", ">+2%", ">+3%", "Days<0", "%Neg"]
    for c, h in enumerate(heads, 1):
        cell = ws.cell(hr, c, h); cell.font = H; cell.alignment = Cc; cell.border = BORD
        cell.fill = G1 if c <= 11 else (G2 if c <= 16 else G3)
    r = hr + 1

    def writerow(y, mlabel, a, bold=False, fill=None):
        nonlocal r
        vals = [y, mlabel, a["days"], a["win"], a["loss"], a["winp"], a["aw"], a["al"], a["summ"],
                a["d1"], a["t05"], a["t1"], a["t15"], a["t2"], a["t3"], a["dneg"], a["negp"]]
        for c, v in enumerate(vals, 1):
            cell = ws.cell(r, c, v); cell.border = BORD
            cell.font = BOLD if bold else NORM
            cell.alignment = Cc if c <= 2 else Rr
            if c in (6,) : cell.number_format = '0.0"%"'
            if c in (7, 8, 9): cell.number_format = '0.00'
            if c == 17: cell.number_format = '0.0"%"'
            if fill: cell.fill = fill
        r += 1

    keys = sorted(by)
    alt = False
    for (y, m) in keys:
        writerow(y, MONTHS[m], agg(by[(y, m)]), fill=(ALT if alt else None))
        alt = not alt
    # yearly + grand total
    for y in sorted({k[0] for k in keys}):
        items = [x for (yy, mm), lst in by.items() if yy == y for x in lst]
        writerow(y, "YEAR", agg(items), bold=True, fill=TOT)
    allitems = [x for lst in by.values() for x in lst]
    writerow("ALL", "TOTAL", agg(allitems), bold=True, fill=TOT)

    widths = [6, 7, 6, 6, 6, 7, 9, 10, 11, 8, 8, 7, 8, 7, 7, 7, 7]
    for i, w in enumerate(widths, 1): ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "C" + str(hr + 1)
    out = ROOT / "docs" / "ops" / "AUTOTRADE_MONTHLY_VIEW.xlsx"
    wb.save(out)
    print("WROTE", out)
    # console preview
    a = agg(allitems)
    print(f"\nALL {a['days']}d | Win {a['win']} Loss {a['loss']} Win% {a['winp']} | Sum {a['summ']}% | Days>=1% {a['d1']}")
    print(f"TOUCH: >0.5%={a['t05']} >1%={a['t1']} >1.5%={a['t15']} >2%={a['t2']} >3%={a['t3']} | Days<0={a['dneg']} ({a['negp']}%)")
    print(f"Capture gap: touched +1% on {a['t1']} days but closed >=+1% on only {a['d1']} days.")


if __name__ == "__main__":
    main()
