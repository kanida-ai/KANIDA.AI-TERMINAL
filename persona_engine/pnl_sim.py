"""
Costed intraday P&L for the volatility-touch screen.

Strategy (monetises the 'will move +/-1%' magnitude signal via a breakout):
  - Each day take the 5 most-volatile-morning F&O stocks (9:15-10:00 vol).
  - At 10:00 (ref price) place an OCO: LONG if price breaks +BRK%, SHORT if -BRK%.
  - Target = +/-1% from ref; hard STOP = STOP% from entry; else exit at 15:15 close.
  - Simulated bar-by-bar (path-dependent; if a bar hits both stop & target, assume
    STOP first = conservative). Costs = COST% round-trip (brokerage+slippage).
  Capital = Rs CAP per pick (5 picks/day).
"""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db, universe
from persona_engine.intraday_eod import morning_features

CAP = 100_000          # Rs/pick
BRK = 0.25             # breakout trigger %
TGT = 1.0              # target % from ref
STOP = 0.5             # stop % from entry
COST = 0.10            # round-trip cost % (brokerage+slippage)
NPICK = 5


def simulate_pick(bars, ref):
    """bars: list of (open,high,low,close) ordered 10:00->15:15. Return net % or None."""
    lt, st = ref*(1+BRK/100), ref*(1-BRK/100)
    entry = direction = tgt = stop = None
    for o, h, l, c in bars:
        if entry is None:
            if h >= lt:
                entry, direction = lt, 1; tgt = ref*(1+TGT/100); stop = lt*(1-STOP/100)
            elif l <= st:
                entry, direction = st, -1; tgt = ref*(1-TGT/100); stop = st*(1+STOP/100)
            else:
                continue
            # same bar can still hit stop/target after entry; check below next bars
            continue
        if direction == 1:               # long: profit if price rises
            if l <= stop:
                return (stop/entry-1)*100 - COST
            if h >= tgt:
                return (tgt/entry-1)*100 - COST
        else:                            # short: profit if price falls
            if h >= stop:
                return (1 - stop/entry)*100 - COST
            if l <= tgt:
                return (1 - tgt/entry)*100 - COST
    if entry is None:
        return None  # no breakout -> no trade
    # exit at last close
    last = bars[-1][3]
    gross = (last/entry-1)*100*direction
    return gross - COST


def run(con, fo):
    mf = morning_features(con, fo)[["symbol", "date", "m_volat"]].dropna()
    mf["rk"] = mf.groupby("date")["m_volat"].rank(ascending=False, method="first")
    picks = mf[mf["rk"] <= NPICK]
    dates = sorted(picks["date"].unique())
    qmark = "?"
    rows = []
    for dt in dates:
        syms = picks[picks["date"] == dt]["symbol"].tolist()
        qs = ",".join("?"*len(syms))
        lo, hi = f"{dt} 10:00:00", f"{dt} 15:15:59"   # indexed range (sargable)
        bars = con.execute(
            f"SELECT symbol, bar_time, open, high, low, close FROM ohlc_1min "
            f"WHERE symbol IN ({qs}) AND bar_time >= ? AND bar_time <= ? ORDER BY symbol,bar_time",
            syms+[lo, hi]).fetchall()
        bd = {}
        for s, bt, o, h, l, c in bars:
            bd.setdefault(s, []).append((o, h, l, c))
        for s in syms:
            b = bd.get(s)
            if not b or len(b) < 5:
                continue
            ref = b[0][0]
            net = simulate_pick(b, ref)
            if net is None:
                rows.append((dt, s, "no_trade", 0.0, 0.0)); continue
            rows.append((dt, s, "trade", round(net, 3), round(CAP*net/100, 1)))
    tr = pd.DataFrame(rows, columns=["date", "symbol", "status", "net_pct", "pnl_rs"])
    traded = tr[tr["status"] == "trade"]
    daily = tr.groupby("date")["pnl_rs"].sum().reset_index()
    daily["ym"] = daily["date"].str[:7]; daily["year"] = daily["date"].str[:4]

    print(f"=== INTRADAY BREAKOUT P&L (Rs{CAP:,}/pick, 5 picks, brk{BRK}/tgt{TGT}/stop{STOP}/cost{COST}%) ===")
    print(f"trades taken: {len(traded)} of {len(tr)} pick-days ({len(traded)/len(tr)*100:.0f}% triggered)")
    print(f"win rate: {(traded['net_pct']>0).mean()*100:.1f}%   avg net/trade: {traded['net_pct'].mean():+.3f}%")
    print(f"TOTAL P&L: Rs{tr['pnl_rs'].sum():,.0f}  over {daily.shape[0]} days "
          f"(avg Rs{daily['pnl_rs'].mean():,.0f}/day on Rs{CAP*NPICK:,} deployed)")
    cap_deployed = CAP*NPICK
    print(f"avg daily return on deployed capital: {daily['pnl_rs'].mean()/cap_deployed*100:+.3f}%/day")
    print("\n=== YEARLY ===")
    print(daily.groupby("year")["pnl_rs"].agg(["sum", "mean", "count"]).round(0).to_string())
    print("\n=== MONTHLY ===")
    print(daily.groupby("ym")["pnl_rs"].agg(["sum", "mean", "count"]).round(0).to_string())
    # save
    from pathlib import Path
    OUT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_findings")
    with pd.ExcelWriter(OUT/"Intraday_Breakout_PnL.xlsx", engine="openpyxl") as xl:
        tr.to_excel(xl, "trades", index=False)
        daily.to_excel(xl, "daily_pnl", index=False)
        daily.groupby("ym")["pnl_rs"].agg(["sum", "mean", "count"]).to_excel(xl, "monthly")
        daily.groupby("year")["pnl_rs"].agg(["sum", "mean", "count"]).to_excel(xl, "yearly")
    daily.to_csv(OUT/"Breakout_daily_pnl.csv", index=False)
    print("\nsaved Intraday_Breakout_PnL.xlsx + Breakout_daily_pnl.csv")


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("PNL_DONE")
