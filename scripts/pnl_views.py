"""
KANIDA — segmented monthly P&L views (v2, leverage-correct per Zerodha product rules).

CORRECTED per real order-type rules:
  * 1X return = the UNLEVERAGED (delivery) return — product-AGNOSTIC (one cost), so the same
    intraday trade shows the IDENTICAL 1X whether viewed as MIS or CNC. Leverage is applied on top.
  * MIS (intraday, 1D)  = 5X leverage  -> show 1X (reference) + 5X (actual); ROC = 5X.
  * CNC (1D / 2D / 5D)  = 1X only, delivery, NO leverage -> 5X columns BLANK; ROC = 1X.
  * NRML (futures, multi-day short) = 5X via margin -> 1X (reference) + 5X (actual); ROC = 5X.
  * 5X = exactly 5 x 1X (same cost, leverage just scales the return).
Intraday (1D) trades appear under BOTH MIS (5x) and CNC (1x) — same trade, the trader's lane choice.
Rs1,00,000 = capital/margin per position. max_capital_required = peak concurrent positions x Rs1L
(same margin whether 1x or 5x). Built from trade_log_rev (sealed 2026).

Columns: month | days | days_pos | max_capital_required | ROC | ret5x_pct | ret1x_pct | pnl_rs_1X | pnl_rs_5X
(CNC sheets leave ret5x_pct / pnl_rs_5X blank — CNC cannot be leveraged.)

Run: python pnl_views.py [all|tradeable]   ->  reports/KANIDA_pnl_views[_tradeable].xlsx
"""
import sys, sqlite3
from pathlib import Path
import pandas as pd

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
OUT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\reports")
CAPITAL = 100_000.0
COST_PCT = 0.10                                            # SINGLE round-trip % (product-agnostic 1x base)
LEVERAGED = {"MIS": True, "CNC": False, "NRML": True}      # CNC can NEVER be leveraged


def base_pnl(gross_move_pct):                              # unleveraged (1x) rupee P&L
    return CAPITAL * (gross_move_pct - COST_PCT) / 100.0


def peak_capital(df):
    ev = []
    for _, r in df.iterrows():
        ev.append((r.entry_d, 1)); ev.append((r.exit_d + pd.Timedelta(days=1), -1))
    ev.sort(); cur = mx = 0
    for _, delta in ev:
        cur += delta; mx = max(mx, cur)
    return mx * CAPITAL


def expand(tl):
    """One trade -> segment rows. Intraday(MIS) -> both MIS(5x) and CNC(1x) with the SAME 1x base."""
    rows = []
    for r in tl.itertuples():
        hold = r.outcome_target.split("_")[-1].upper(); d = "long" if r.direction == "LONG" else "short"
        ed = pd.Timestamp(r.entry_date); xd = pd.Timestamp(r.exit_date)
        b = base_pnl(r.gross_move_pct)
        prod = r.laneA_product
        orders = ["MIS", "CNC"] if prod == "MIS" else [prod]
        for order in orders:
            rows.append(dict(order=order, hold=hold, dir=d, month=ed.strftime("%Y-%m"),
                             entry_d=ed, exit_d=xd, base=b))
    return pd.DataFrame(rows)


def monthly(seg, order):
    lev = LEVERAGED[order]; out = []
    for month, g in seg.groupby("month"):
        by_day = g.groupby("entry_d").base.sum(); maxcap = peak_capital(g)
        p1 = g.base.sum(); p5 = 5 * p1
        ret1 = round(p1 / maxcap * 100, 1) if maxcap else 0
        ret5 = round(p5 / maxcap * 100, 1) if maxcap else 0
        out.append(dict(month=month, days=by_day.size, days_pos=int((by_day > 0).sum()),
                        max_capital_required=round(maxcap, 0),
                        ROC=(ret5 if lev else ret1),
                        ret5x_pct=(ret5 if lev else ""), ret1x_pct=ret1,
                        pnl_rs_1X=round(p1, 0), pnl_rs_5X=(round(p5, 0) if lev else "")))
    df = pd.DataFrame(out).sort_values("month")
    tot = dict(month="TOTAL", days=df.days.sum(), days_pos=df.days_pos.sum(),
               max_capital_required=df.max_capital_required.max(), ROC="", ret5x_pct="", ret1x_pct="",
               pnl_rs_1X=df.pnl_rs_1X.sum(), pnl_rs_5X=(df[df.pnl_rs_5X != ""].pnl_rs_5X.sum() if lev else ""))
    return pd.concat([df, pd.DataFrame([tot])], ignore_index=True)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    con = sqlite3.connect(SNR)
    if mode == "tradeable":
        tl = pd.read_sql("SELECT tl.* FROM trade_log_rev tl JOIN stock_verdict v ON tl.Stock=v.Stock "
                         "WHERE v.verdict='Tradeable'", con); fname = "KANIDA_pnl_views_tradeable.xlsx"
    else:
        tl = pd.read_sql("SELECT * FROM trade_log_rev", con); fname = "KANIDA_pnl_views.xlsx"
    con.close()
    print(f"mode={mode} | trades={len(tl)} | single cost={COST_PCT}% | CNC=1x only, MIS/NRML=5x")
    exp = expand(tl)
    order_rank = {"MIS": 0, "CNC": 1, "NRML": 2}; hold_rank = {"1D": 0, "2D": 1, "5D": 2}
    segs = sorted(exp.groupby(["order", "hold", "dir"]).groups.keys(),
                  key=lambda k: (order_rank[k[0]], hold_rank[k[1]], k[2]))
    OUT.mkdir(exist_ok=True); summary = []
    with pd.ExcelWriter(OUT / fname, engine="openpyxl") as xl:
        for (order, hold, d) in segs:
            seg = exp[(exp.order == order) & (exp.hold == hold) & (exp["dir"] == d)]
            if seg.empty: continue
            tbl = monthly(seg, order); sheet = f"{order}_{hold}_{d}"[:31]
            tbl.to_excel(xl, sheet_name=sheet, index=False)
            t = tbl[tbl.month == "TOTAL"].iloc[0]
            summary.append(dict(segment=sheet, order=order, hold=hold, direction=d,
                                leverage=("5x" if LEVERAGED[order] else "1x"), trades=len(seg),
                                total_pnl_1X=t.pnl_rs_1X, total_pnl_5X=t.pnl_rs_5X))
            print(f"  {sheet:16} lev={'5x' if LEVERAGED[order] else '1x'} trades={len(seg):>4} "
                  f"pnl_1X=Rs{t.pnl_rs_1X:>12,.0f} pnl_5X={t.pnl_rs_5X if LEVERAGED[order] else '(n/a)'}")
        s = pd.DataFrame(summary)
        s.to_excel(xl, sheet_name="SUMMARY", index=False)
    print(f"\nWritten: reports/{fname}")


if __name__ == "__main__":
    main()
