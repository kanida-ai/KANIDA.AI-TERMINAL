"""Optimized monthly view: base (RND, entry<=2026-06-15) + EXTENDED days
(2026-06-16..2026-07-03) with signals from falcon_signals_live and 1-min from
RND (<=Jun25) or Kite historical (>Jun25). Finds the config that MAXIMIZES
realized return while CAPTURING the opportunity, then writes a new monthly xlsx
with an Avg-stocks/day integrity column + touch(MFE)/risk(MAE) columns.
"""
import sys
sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\backend")
import sqlite3, math, pickle
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import numpy as np

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
IST = timezone(timedelta(hours=5, minutes=30))
PERSONA = "falcon_top10_daily"; ALIASES = {"ZOMATO": "ETERNAL"}
OPEN, CLOSE = "09:15:00", "15:29:00"; TOPN = 5; CAP = 100000.0
MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
RND_1MIN_MAX = "2026-06-25"


def rnd_signals(con):
    out = {}
    for ed, rk, sym in con.execute(
        "SELECT entry_date,engine_rank,symbol FROM falcon_signal_day_study "
        "WHERE persona=? AND engine_rank BETWEEN 1 AND ? ORDER BY entry_date,engine_rank",
        (PERSONA, TOPN)):
        out.setdefault(ed, []).append(sym)
    return out


def extended_signals():
    from falcon.db import falcon_conn
    out = {}
    with falcon_conn() as c:
        for ed, rk, sym in c.execute(
            "SELECT entry_date,rank,symbol FROM falcon_signals_live "
            "WHERE entry_date BETWEEN '2026-06-16' AND '2026-07-03' AND rank<=? "
            "ORDER BY entry_date,rank", (TOPN,)):
            out.setdefault(ed, []).append(sym)
    # Calendar fix: 2026-06-26 is an NSE holiday (no market data); the live signal
    # table mislabeled that basket's entry to 26 and skipped 29 (a real trading
    # day). Remap the basket to enter on the true next trading day, 2026-06-29.
    if "2026-06-26" in out and "2026-06-29" not in out:
        out["2026-06-29"] = out.pop("2026-06-26")
    return out


def rnd_1min(con, day, syms):
    fetch = {ALIASES.get(s, s): s for s in syms}; ph = ",".join("?" * len(fetch))
    rows = con.execute(
        f"SELECT symbol,substr(bar_time,12,5) hm,open,high,low,close FROM ohlc_1min "
        f"WHERE bar_time BETWEEN ? AND ? AND symbol IN ({ph})",
        [f"{day} {OPEN}", f"{day} {CLOSE}", *fetch.keys()]).fetchall()
    per = {}
    for osym, hm, o, h, l, c in rows:
        per.setdefault(fetch[osym], {})[hm] = (o, h, l, c)
    return per


def kite_1min(kite, tokmap, day, syms):
    per = {}
    start = datetime.strptime(f"{day} 09:15", "%Y-%m-%d %H:%M").replace(tzinfo=IST)
    end = datetime.strptime(f"{day} 15:29", "%Y-%m-%d %H:%M").replace(tzinfo=IST)
    for s in syms:
        tk = tokmap.get(ALIASES.get(s, s)) or tokmap.get(s)
        if not tk:
            continue
        try:
            bars = kite.historical_data(tk, start, end, "minute")
        except Exception:
            continue
        d = {}
        for b in bars:
            hm = b["date"].strftime("%H:%M")
            d[hm] = (b["open"], b["high"], b["low"], b["close"])
        if d:
            per[s] = d
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
            if v and v[idx] is not None and np.isfinite(v[idx]):
                last = v[idx]
            vals.append(last)
        e = per[s]["09:15"][0]
        return [x if x is not None else e for x in vals]

    close = np.column_stack([series(s, 3) for s in syms]); high = np.column_stack([series(s, 1) for s in syms])
    low = np.column_stack([series(s, 2) for s in syms]); opn = np.column_stack([series(s, 0) for s in syms])
    dep = float((qty * entry).sum())
    return dict(entry=entry, qty=qty, close=close, high=high, low=low, opn=opn, dep=dep,
                n=len(grid), nstocks=len(syms), syms=syms, rank=[order.index(s) + 1 for s in syms], grid=grid)


def sim(M, arm, floor, give, stop):
    qty, close, opn, dep, n = M["qty"], M["close"], M["opn"], M["dep"], M["n"]
    port = close @ qty; ret = (port - dep) / dep * 100.0
    openval = opn @ qty; nxt = np.empty_like(openval); nxt[:-1] = openval[1:]; nxt[-1] = port[-1]
    armed = False; peak = None
    for i in range(n - 1):
        r = ret[i]
        if r <= -stop:
            return (nxt[i] - dep) / dep * 100.0
        if not armed:
            if r >= arm:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(floor, peak - give):
            return (nxt[i] - dep) / dep * 100.0
    return (port[-1] - dep) / dep * 100.0


def assemble():
    con = sqlite3.connect(str(RND))
    base = rnd_signals(con)
    ext = extended_signals()
    # kite setup for days > RND_1MIN_MAX
    from services.kite_auth import get_kite_client
    kite = get_kite_client(check=False)
    tokmap = {i["tradingsymbol"]: i["instrument_token"] for i in kite.instruments("NSE")}
    mats = []  # (date, M)
    # base days (entry <= 2026-06-15)
    for d in sorted(base):
        M = build(rnd_1min(con, d, base[d]), base[d])
        if M:
            mats.append((d, M))
    # extended days
    for d in sorted(ext):
        if d <= RND_1MIN_MAX:
            per = rnd_1min(con, d, ext[d])
        else:
            per = kite_1min(kite, tokmap, d, ext[d])
        M = build(per, ext[d])
        if M:
            mats.append((d, M))
        print(f"  extended {d}: {'OK' if M else 'NO DATA'} ({M['nstocks'] if M else 0} stk)", flush=True)
    con.close()
    mats.sort(key=lambda x: x[0])
    return mats


def main():
    mats = assemble()
    print(f"[*] total days: {len(mats)}  ({mats[0][0]} -> {mats[-1][0]})", flush=True)
    # ---- optimize: max mean realized return (capturing opportunity via lock) ----
    grid = []
    for stop in [1.5, 2.0, 2.5, 3.0]:
        for arm in [1.0, 1.5, 2.0, 2.5]:
            for give in [0.5, 0.75, 1.0, 1.5]:
                grid.append((stop, arm, 1.0, give))
    best = None
    scored = []
    for (stop, arm, floor, give) in grid:
        rets = np.array([sim(M, arm, floor, give, stop) for _, M in mats])
        mean = rets.mean(); d1 = (rets >= 1).mean() * 100; pos = (rets > 0).mean() * 100; worst = rets.min()
        scored.append((mean, d1, pos, worst, stop, arm, floor, give))
    scored.sort(reverse=True)  # by mean
    print("\n=== TOP 8 configs by MEAN realized return (capture via lock) ===")
    print(f"{'stop':>5}{'arm':>5}{'floor':>6}{'give':>6}{'mean%':>8}{'d>=1%':>7}{'pos%':>7}{'worst%':>8}")
    for mean, d1, pos, worst, stop, arm, floor, give in scored[:8]:
        print(f"{stop:>5}{arm:>5}{floor:>6}{give:>6}{mean:>8.3f}{d1:>7.1f}{pos:>7.1f}{worst:>8.2f}")
    bm, bd1, bpos, bworst, BSTOP, BARM, BFLOOR, BGIVE = scored[0]
    print(f"\n[*] CHOSEN (max mean): stop{BSTOP} arm{BARM} floor{BFLOOR} give{BGIVE} -> mean {bm:.3f}% d>=1% {bd1:.1f}% pos {bpos:.1f}% worst {bworst:.2f}%")

    # ---- monthly view with chosen config ----
    rows = []
    for d, M in mats:
        r = sim(M, BARM, BFLOOR, BGIVE, BSTOP)
        mfe = float((((M["high"] - M["entry"]) * M["qty"]).sum(axis=1) / M["dep"] * 100).max())
        mae = float((((M["low"] - M["entry"]) * M["qty"]).sum(axis=1) / M["dep"] * 100).min())
        rows.append((int(d[:4]), int(d[5:7]), r, mfe, mae, M["nstocks"]))
    pickle.dump({"rows": rows, "cfg": (BSTOP, BARM, BFLOOR, BGIVE)},
                open(ROOT / "docs" / "ops" / "_opt_monthly.pkl", "wb"))
    build_xlsx(rows, (BSTOP, BARM, BFLOOR, BGIVE))


def build_xlsx(rows, cfg):
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    BSTOP, BARM, BFLOOR, BGIVE = cfg
    by = defaultdict(list)
    for y, m, r, mfe, mae, ns in rows:
        by[(y, m)].append((r, mfe, mae, ns))

    def agg(items):
        rr = np.array([x[0] for x in items]); mf = np.array([x[1] for x in items])
        ma = np.array([x[2] for x in items]); ns = np.array([x[3] for x in items])
        wins = rr[rr > 0]; losses = rr[rr < 0]
        return dict(days=len(rr), avgstk=round(float(ns.mean()), 2), win=int((rr > 0).sum()),
                    loss=int((rr < 0).sum()), winp=round((rr > 0).mean() * 100, 1),
                    aw=round(float(wins.mean()), 2) if len(wins) else 0.0,
                    al=round(float(losses.mean()), 2) if len(losses) else 0.0,
                    summ=round(float(rr.sum()), 1), d1=int((rr >= 1).sum()),
                    t05=int((mf >= 0.5).sum()), t1=int((mf >= 1).sum()), t15=int((mf >= 1.5).sum()),
                    t2=int((mf >= 2).sum()), t3=int((mf >= 3).sum()),
                    dneg=int((ma < 0).sum()), negp=round((ma < 0).mean() * 100, 1))

    F = "Calibri"
    TITLE = Font(name=F, bold=True, size=13, color="1F6F8B"); NOTE = Font(name=F, size=9, italic=True, color="666666")
    H = Font(name=F, bold=True, size=9, color="FFFFFF")
    G1 = PatternFill("solid", start_color="1F6F8B"); G2 = PatternFill("solid", start_color="2E8B9E"); G3 = PatternFill("solid", start_color="B0662A")
    NORM = Font(name=F, size=10); BOLD = Font(name=F, bold=True, size=10)
    ALT = PatternFill("solid", start_color="F5FAFB"); TOT = PatternFill("solid", start_color="FFF4CE"); WARN = PatternFill("solid", start_color="FDECEA")
    thin = Side(style="thin", color="DDDDDD"); BORD = Border(left=thin, right=thin, top=thin, bottom=thin)
    Cc = Alignment(horizontal="center"); Rr = Alignment(horizontal="right")
    wb = Workbook(); ws = wb.active; ws.title = "Monthly View"
    ws.cell(1, 1, "Optimized Monthly View — Capture-Maximized").font = TITLE
    ws.cell(2, 1, f"REALIZED = OPTIMIZED config arm {BARM}% / floor {BFLOOR}% / giveback {BGIVE}% / stop {BSTOP}% (cash Top-5, next-open fills). "
                  f"Includes extended days to 2026-07-03 (June 26+ fetched from Kite). Avg_stk should be ~5 (data-integrity check).").font = NOTE
    groups = [("", 1, 4, None), ("REALIZED (optimized capture)", 5, 12, G1),
              ("OPPORTUNITY — days basket TOUCHED intraday (MFE)", 13, 17, G2), ("RISK (MAE)", 18, 19, G3)]
    gr = 4
    for name, c1, c2, fill in groups:
        if name:
            ws.merge_cells(start_row=gr, start_column=c1, end_row=gr, end_column=c2)
            cell = ws.cell(gr, c1, name); cell.font = H; cell.fill = fill; cell.alignment = Cc; cell.border = BORD
    hr = gr + 1
    heads = ["Year", "Month", "Days", "Avg_stk", "Win", "Loss", "Win%", "Avg_win%", "Avg_loss%",
             "Sum_month%", "Days>=1%", ">+0.5%", ">+1%", ">+1.5%", ">+2%", ">+3%", "Days<0", "%Neg"]
    # NOTE: 18 headers map to 19 cols group plan; adjust: put Days>=1% under realized
    for c, h in enumerate(heads, 1):
        cell = ws.cell(hr, c, h); cell.font = H; cell.alignment = Cc; cell.border = BORD
        cell.fill = G1 if c <= 11 else (G2 if c <= 16 else G3)
    r = hr + 1

    def wr(y, ml, a, bold=False, fill=None):
        nonlocal r
        vals = [y, ml, a["days"], a["avgstk"], a["win"], a["loss"], a["winp"], a["aw"], a["al"],
                a["summ"], a["d1"], a["t05"], a["t1"], a["t15"], a["t2"], a["t3"], a["dneg"], a["negp"]]
        for c, v in enumerate(vals, 1):
            cell = ws.cell(r, c, v); cell.border = BORD; cell.font = BOLD if bold else NORM
            cell.alignment = Cc if c <= 2 else Rr
            if c in (7,): cell.number_format = '0.0"%"'
            if c in (8, 9, 10): cell.number_format = '0.00'
            if c == 18: cell.number_format = '0.0"%"'
            if fill: cell.fill = fill
            if c == 4 and isinstance(v, (int, float)) and v < 4.9:  # integrity flag
                cell.fill = WARN
        r += 1

    keys = sorted(by); alt = False
    for k in keys:
        wr(k[0], MONTHS[k[1]], agg(by[k]), fill=(ALT if alt else None)); alt = not alt
    for y in sorted({k[0] for k in keys}):
        wr(y, "YEAR", agg([x for (yy, mm), l in by.items() if yy == y for x in l]), bold=True, fill=TOT)
    wr("ALL", "TOTAL", agg([x for l in by.values() for x in l]), bold=True, fill=TOT)
    for i, w in enumerate([6, 7, 6, 8, 6, 6, 7, 9, 10, 11, 9, 8, 7, 8, 7, 7, 7, 7], 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "C" + str(hr + 1)
    out = ROOT / "docs" / "ops" / "AUTOTRADE_MONTHLY_VIEW_OPTIMIZED.xlsx"
    wb.save(out)
    print(f"\n[*] WROTE {out}")
    a = agg([x for l in by.values() for x in l])
    print(f"ALL {a['days']}d avg_stk {a['avgstk']} | Win% {a['winp']} Sum {a['summ']}% Days>=1% {a['d1']} | touch>1%={a['t1']} | Days<0 {a['dneg']} ({a['negp']}%)")


if __name__ == "__main__":
    main()
