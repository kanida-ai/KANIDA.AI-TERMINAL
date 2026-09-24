"""Event study to TEST the margin-signal hypothesis on REAL NSE VaR-margin history.

Timing: the C_VAR1 file dated D holds the margins APPLICABLE for trading day D, computed
from data through D-1. So a hike from D-1->D is KNOWN at the open of D and reflects
volatility already realized by D-1. Question:
  - Was the big MOVE/VOL before D (pre-window) -> margin is REACTIVE (lags), or
  - after D (forward window)                   -> margin is LEADING (predictive)?
  - And is the forward move skewed UP (user's hypothesis), down, or symmetric?

Event = margin hike: total_pct(D) - total_pct(D-1) >= THRESH, prior margin not already maxed.
Prices = ohlc_1min aggregated to daily for the stocks present in both datasets.
"""
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
THRESH = 3.0      # margin-hike = +3 percentage points day-over-day
ALIAS = {"ZOMATO": "ETERNAL"}


def load():
    # ALL-STOCK price panel from NSE bhavcopy (covers small/mid-caps where margins move).
    con = sqlite3.connect(str(DB))
    m = pd.read_sql_query("SELECT trade_date date, symbol, total_pct FROM nse_var_margin WHERE series='EQ'", con)
    px = pd.read_sql_query(
        "SELECT trade_date date, symbol, open opn, high hi, low lo, close FROM nse_bhav_daily "
        "WHERE series IN ('EQ','BE') AND close>0", con)
    con.close()
    return m, px


def main():
    m, px = load()
    print(f"margin rows {len(m):,} | price rows {len(px):,} | overlap stocks {px.symbol.nunique()} | {m.date.min()}..{m.date.max()}")
    m = m.sort_values(["symbol", "date"])
    m["prev"] = m.groupby("symbol").total_pct.shift(1)
    m["dmargin"] = m.total_pct - m.prev
    # events: hikes and (control) cuts
    hikes = m[(m.dmargin >= THRESH) & (m.prev <= 40)].copy()      # not already near-max
    cuts = m[(m.dmargin <= -THRESH) & (m.total_pct <= 40)].copy()
    print(f"margin-HIKE events (>= +{THRESH}pts): {len(hikes):,} | CUT events: {len(cuts):,}")

    # per-symbol close/high/low arrays for window math
    px = px.sort_values(["symbol", "date"])
    by = {s: g.reset_index(drop=True) for s, g in px.groupby("symbol")}
    didx = {s: {d: i for i, d in enumerate(g.date)} for s, g in by.items()}

    def study(events, label):
        pre_r, fwd_r, pre_v, fwd_v, fup, fdn, base_r = [], [], [], [], [], [], []
        for _, e in events.iterrows():
            s = e.symbol
            if s not in didx or e.date not in didx[s]:
                continue
            g = by[s]; i = didx[s][e.date]; n = len(g)
            if i < 6 or i + 5 >= n:
                continue
            c = g.close.values.astype(float); hi = g.hi.values.astype(float); lo = g.lo.values.astype(float)
            c0 = c[i - 1]                                   # close the day BEFORE the hike applies
            if not (c0 > 0): continue
            pre_r.append(c[i - 1] / c[i - 6] - 1)           # 5-day return BEFORE
            fwd_r.append(c[i + 4] / c0 - 1)                 # 5-day return AFTER (from c0)
            rr = np.diff(c[i - 6:i]) / c[i - 6:i - 1]
            pre_v.append(np.std(rr))
            rf = np.diff(c[i - 1:i + 5]) / c[i - 1:i + 4]
            fwd_v.append(np.std(rf))
            fup.append(hi[i:i + 5].max() / c0 - 1)          # max favorable over [D..D+4]
            fdn.append(lo[i:i + 5].min() / c0 - 1)          # max adverse
        pr, fr = np.array(pre_r), np.array(fwd_r)
        pv, fv = np.array(pre_v), np.array(fwd_v)
        print(f"\n=== {label} (n={len(fr)}) ===")
        print(f"  5-day return  BEFORE: {pr.mean()*100:+.2f}%   |  AFTER: {fr.mean()*100:+.2f}%   (% up after: {(fr>0).mean()*100:.0f}%)")
        print(f"  daily vol     BEFORE: {pv.mean()*100:.2f}%    |  AFTER: {fv.mean()*100:.2f}%   -> vol { 'ROSE before (REACTIVE)' if pv.mean()>fv.mean() else 'rises after' }")
        print(f"  forward max-up {np.mean(fup)*100:+.2f}%  vs  max-down {np.mean(fdn)*100:+.2f}%  -> { 'UP-skewed' if abs(np.mean(fup))>abs(np.mean(fdn)) else 'DOWN-skewed' }")
        return fr

    fr_h = study(hikes, "MARGIN HIKE")
    study(cuts, "MARGIN CUT (control)")
    # baseline: forward 5-day return over ALL stock-days
    allc = []
    for s, g in by.items():
        c = g.close.values.astype(float)
        for i in range(6, len(g) - 5):
            if c[i - 1] > 0: allc.append(c[i + 4] / c[i - 1] - 1)
    base = np.array(allc)
    print(f"\n=== BASELINE (all stock-days, n={len(base):,}) 5-day fwd return: {base.mean()*100:+.2f}% | % up {(base>0).mean()*100:.0f}% ===")
    print(f"\nVERDICT: hike-forward {fr_h.mean()*100:+.2f}% vs baseline {base.mean()*100:+.2f}%  "
          f"-> edge {(fr_h.mean()-base.mean())*100:+.2f}pp")


if __name__ == "__main__":
    main()
