"""Regime filter on the OPTIMIZED intraday basket. Walk-forward, no lookahead:
each entry day is GATED by market state known at the PRIOR close. Sit out (cash,
0%) on unfavorable regimes. Compares filters vs trade-all on consistency:
%days>=1%, %positive, mean, worst, drawdown, monthly pass. Cash (non-MTF).
"""
import sys, pickle, sqlite3
sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\backend")
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import numpy as np
from falcon_optimized_monthly import ROOT, RND, sim

ARM, FLOOR, GIVE, STOP = 2.5, 1.0, 1.5, 3.0
IST = timezone(timedelta(hours=5, minutes=30))
PROXY = "NIFTYBEES"   # Nifty-50 ETF as the market-trend proxy (equity, in RND + Kite)


def strategy_returns():
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    dates = [d for d, _ in mats]
    rets = {d: sim(M, ARM, FLOOR, GIVE, STOP) for d, M in mats}
    return dates, rets


def proxy_series(dates):
    """Nifty-50 INDEX daily closes via Kite (token 256265), from 2024-01-01 (enough
    warm-up for a 50D SMA before the first entry day) to the last entry day."""
    from services.kite_auth import get_kite_client
    k = get_kite_client(check=False)
    NIFTY50_TOKEN = 256265
    s = datetime(2024, 1, 1, tzinfo=IST)
    e = datetime.strptime(max(dates), "%Y-%m-%d").replace(hour=16, tzinfo=IST)
    ser = {}
    try:
        for b in k.historical_data(NIFTY50_TOKEN, s, e, "day"):
            ser[b["date"].strftime("%Y-%m-%d")] = float(b["close"])
    except Exception as ex:
        print("  Nifty index fetch error:", ex)
    return ser


def regimes(dates, ser):
    """For each entry day, market state using ONLY data strictly before it."""
    tdays = sorted(ser)
    closes = np.array([ser[d] for d in tdays])
    idx_of = {d: i for i, d in enumerate(tdays)}
    out = {}
    for d in dates:
        # locate the last proxy day strictly before entry day d
        j = None
        for i in range(len(tdays) - 1, -1, -1):
            if tdays[i] < d:
                j = i; break
        if j is None or j < 50:
            out[d] = dict(above20=True, above50=True, ret5=0.0, ok=True)  # warm-up: allow
            continue
        c = closes[j]
        sma10 = closes[j - 9:j + 1].mean(); sma20 = closes[j - 19:j + 1].mean(); sma50 = closes[j - 49:j + 1].mean()
        ret5 = (closes[j] / closes[j - 5] - 1) * 100
        out[d] = dict(above10=c > sma10, above20=c > sma20, above50=c > sma50, ret5=ret5)
    return out


def selfreg(dates, rets, window=10):
    """Trailing mean strategy return over the prior `window` traded days (past only)."""
    out = {}
    hist = []
    for d in dates:
        out[d] = (np.mean(hist[-window:]) if len(hist) >= window else 0.0)  # warm-up: 0 => allow
        hist.append(rets[d])
    return out


def metrics(dates, rets, traded):
    tr = np.array([rets[d] for d in dates if traded[d]])
    alln = np.array([rets[d] if traded[d] else 0.0 for d in dates])  # skipped = cash 0%
    if len(tr) == 0:
        return None
    # monthly %>=1% on traded days
    md = defaultdict(list)
    for d in dates:
        if traded[d]:
            md[d[:7]].append(rets[d])
    m_ge1 = [np.mean(np.array(v) >= 1) * 100 for v in md.values() if len(v) >= 5]
    # drawdown on the cash-inclusive equity curve (additive)
    eq = np.cumsum(alln); dd = float((np.maximum.accumulate(eq) - eq).max())
    return dict(traded=len(tr), skip=len(dates) - len(tr),
                mean_tr=round(float(tr.mean()), 3), ge1_tr=round(float((tr >= 1).mean() * 100), 1),
                pos_tr=round(float((tr > 0).mean() * 100), 1), worst=round(float(tr.min()), 2),
                sum_ret=round(float(tr.sum()), 1), mean_all=round(float(alln.mean()), 3),
                maxdd=round(dd, 1), mo_ge1=round(float(np.mean(m_ge1)), 1) if m_ge1 else 0.0,
                mo_pass=sum(1 for x in m_ge1 if x >= 90), mo_n=len(m_ge1))


def main():
    dates, rets = strategy_returns()
    ser = proxy_series(dates)
    reg = regimes(dates, ser); sr = selfreg(dates, rets)
    print(f"[*] {len(dates)} days | proxy {PROXY} covers to {max(ser)}")

    FILTERS = {
        "BASE (trade all)": lambda d: True,
        "MKT: Nifty>20D SMA": lambda d: reg[d]["above20"],
        "MKT: Nifty>50D SMA": lambda d: reg[d]["above50"],
        "MKT: Nifty 5d ret>-2%": lambda d: reg[d].get("ret5", 0) > -2.0,
        "SELF: trail10 mean>0": lambda d: sr[d] > 0,
        "SELF: trail10 mean>0.3": lambda d: sr[d] > 0.3,
        "COMBO: >20D & self>0": lambda d: reg[d]["above20"] and sr[d] > 0,
    }
    results = {}
    for name, fn in FILTERS.items():
        traded = {d: bool(fn(d)) for d in dates}
        results[name] = metrics(dates, rets, traded)

    print(f"\n{'filter':<26}{'traded':>7}{'skip':>6}{'mean_tr':>8}{'d>=1%':>7}{'pos%':>7}{'worst':>7}{'meanALL':>8}{'maxDD':>7}{'mo>=1%avg':>10}{'moPass':>8}")
    for name, m in results.items():
        if not m:
            continue
        print(f"{name:<26}{m['traded']:>7}{m['skip']:>6}{m['mean_tr']:>8}{m['ge1_tr']:>7}{m['pos_tr']:>7}{m['worst']:>7}{m['mean_all']:>8}{m['maxdd']:>7}{m['mo_ge1']:>10}{str(m['mo_pass'])+'/'+str(m['mo_n']):>8}")

    # write xlsx
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    F = "Calibri"; TITLE = Font(name=F, bold=True, size=13, color="1F6F8B"); NOTE = Font(name=F, size=9, italic=True, color="666666")
    H = Font(name=F, bold=True, size=9, color="FFFFFF"); HDR = PatternFill("solid", start_color="1F6F8B")
    NORM = Font(name=F, size=10); GRNF = PatternFill("solid", start_color="E7F4EA"); TOT = PatternFill("solid", start_color="FFF4CE")
    thin = Side(style="thin", color="DDDDDD"); BORD = Border(left=thin, right=thin, top=thin, bottom=thin)
    Cc = Alignment(horizontal="center"); Rr = Alignment(horizontal="right"); Ll = Alignment(horizontal="left")
    wb = Workbook(); ws = wb.active; ws.title = "Regime Filters"
    ws.cell(1, 1, "Regime Filter Comparison — OPTIMIZED basket (cash, walk-forward)").font = TITLE
    ws.cell(2, 1, "Each day gated by market state at the PRIOR close (no lookahead). Skipped days = cash (0%). "
                  "'_tr' = on traded days; meanALL includes cash days; maxDD on the additive equity curve.").font = NOTE
    heads = ["Filter", "Traded", "Skip", "Mean_tr%", "Days>=1%_tr", "Pos%_tr", "Worst%", "MeanALL%", "MaxDD%", "Mo>=1% avg", "MonthsPass90"]
    for c, h in enumerate(heads, 1):
        cell = ws.cell(4, c, h); cell.font = H; cell.fill = HDR; cell.border = BORD; cell.alignment = Cc
    r = 5
    base_ge1 = results["BASE (trade all)"]["ge1_tr"]
    for name, m in results.items():
        if not m:
            continue
        vals = [name, m["traded"], m["skip"], m["mean_tr"], m["ge1_tr"], m["pos_tr"], m["worst"],
                m["mean_all"], m["maxdd"], m["mo_ge1"], f"{m['mo_pass']}/{m['mo_n']}"]
        for c, v in enumerate(vals, 1):
            cell = ws.cell(r, c, v); cell.font = NORM; cell.alignment = Ll if c == 1 or c == 11 else Rr; cell.border = BORD
        if name.startswith("BASE"):
            for c in range(1, 12): ws.cell(r, c).fill = TOT
        elif m["ge1_tr"] > base_ge1 and m["pos_tr"] > results["BASE (trade all)"]["pos_tr"]:
            for c in range(1, 12): ws.cell(r, c).fill = GRNF
        r += 1
    for i, w in enumerate([26, 8, 6, 9, 12, 8, 8, 9, 8, 11, 13], 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    # per-day regime sheet
    ws2 = wb.create_sheet("Daily Regime")
    for c, h in enumerate(["date", "strat_ret%", "Nifty>20D", "Nifty>50D", "Nifty_5dret%", "self_trail10%", "COMBO_traded"], 1):
        cell = ws2.cell(1, c, h); cell.font = H; cell.fill = HDR; cell.border = BORD; cell.alignment = Cc
    for ri, d in enumerate(dates, 2):
        traded = reg[d]["above20"] and sr[d] > 0
        vals = [d, round(rets[d], 3), "Y" if reg[d]["above20"] else "n", "Y" if reg[d]["above50"] else "n",
                round(reg[d].get("ret5", 0), 2), round(sr[d], 3), "TRADE" if traded else "skip"]
        for c, v in enumerate(vals, 1):
            cell = ws2.cell(ri, c, v); cell.font = NORM; cell.alignment = Ll if c in (1, 3, 4, 7) else Rr
            if c == 2: cell.number_format = '0.000'
    for i, w in enumerate([12, 11, 10, 10, 12, 13, 12], 1): ws2.column_dimensions[get_column_letter(i)].width = w
    ws2.freeze_panes = "A2"; ws2.auto_filter.ref = f"A1:G{len(dates)+1}"
    out = ROOT / "docs" / "ops" / "AUTOTRADE_REGIME_FILTER.xlsx"
    wb.save(out); print(f"\n[*] WROTE {out}")


if __name__ == "__main__":
    main()
