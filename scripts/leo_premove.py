"""
PRE-MOVE ATTRIBUTION — do the daily top-5 explosive movers show a Carter setup the day BEFORE they pop?
For every stock-day we compute the setups AS OF D-1 (leak-free, shifted) and compare their prevalence among
MOVERS (top-5 by daily return that day) vs the BASE rate across all liquid stock-days. A setup only matters
if LIFT = P(setup | mover) / P(setup | any day) is clearly > 1.

Per-stock daily setups tested (the ones that describe a single name's chart):
  squeeze_on   : Bollinger(20,2) INSIDE Keltner(20,1.5)  -> coiled (Carter Ch4/11)
  bb_tight     : Bollinger width in its own bottom 20% (trailing 6mo) -> volatility contraction
  ema_uptrend  : 8-EMA > 21-EMA  (Ch18 trend)     ema_pullback: uptrend AND close within 2% of 8-EMA
  rsi_low/hi   : 7-period RSI < 30 / > 70 (Ch1-3)
  near_high    : close within 3% of its 20d high  (coiling under resistance)
  vol_build    : volume(D-1) > 1.5x its 20d avg WHILE price flat  -> 'silent accumulation'
  gap_up       : the move day OPENS > +3% (same-day, pre-close: an actionable trigger, not a predictor)
(Market-internals $TICK/$TRIN, floor pivots, last-hour scalp = tape/intraday level -> not per-stock, excluded.)

Run: python scripts/leo_premove.py
"""
from __future__ import annotations
import sys, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC
MIN_PRICE = 20.0; MIN_DVOL = 2e7


def main():
    t0 = time.time()
    f, _ = DC.wide_all()
    o, h, l, c, v = f["o"], f["h"], f["l"], f["c"], f["v"]
    pc = c.shift(1)
    ret = c.pct_change(fill_method=None)
    dvol = (c * v).rolling(20).mean()

    ema8 = c.ewm(span=8, adjust=False).mean(); ema21 = c.ewm(span=21, adjust=False).mean()
    ema20 = c.ewm(span=20, adjust=False).mean()
    ma20 = c.rolling(20).mean(); sd20 = c.rolling(20).std()
    bb_up, bb_lo = ma20 + 2 * sd20, ma20 - 2 * sd20
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()]).groupby(level=0).max()
    tr = np.maximum(np.maximum(h - l, (h - pc).abs()), (l - pc).abs())
    atr20 = tr.rolling(20).mean()
    kc_up, kc_lo = ema20 + 1.5 * atr20, ema20 - 1.5 * atr20
    bb_width = (bb_up - bb_lo) / ma20
    # rsi7
    d = c.diff(); up = d.clip(lower=0).ewm(alpha=1 / 7, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / 7, adjust=False).mean()
    rsi7 = 100 - 100 / (1 + up / dn.replace(0, np.nan))
    vol_ratio = v / v.rolling(20).mean()
    hi20 = c.rolling(20).max()

    # --- setups as of D-1 (shift so row D holds prior-close info) ---
    S = {}
    S["squeeze_on"] = ((bb_up < kc_up) & (bb_lo > kc_lo)).shift(1)
    S["bb_tight"] = (bb_width <= bb_width.rolling(126).quantile(0.20)).shift(1)
    S["ema_uptrend"] = (ema8 > ema21).shift(1)
    S["ema_pullback"] = ((ema8 > ema21) & ((c - ema8).abs() / c < 0.02)).shift(1)
    S["rsi_low<30"] = (rsi7 < 30).shift(1)
    S["rsi_hi>70"] = (rsi7 > 70).shift(1)
    S["near_20dhigh"] = (c / hi20 > 0.97).shift(1)
    S["vol_build_flat"] = ((vol_ratio > 1.5) & (ret.abs() < 0.02)).shift(1)   # heavy volume, price flat
    S["gap_up>3%(sameday)"] = (o / pc - 1 > 0.03)                              # move-day open gap (trigger)

    liq = (c > MIN_PRICE) & (dvol > MIN_DVOL) & ret.notna()
    # movers = top-5 by daily return each day, among liquid
    rr = ret.where(liq)
    rank = rr.rank(axis=1, ascending=False, method="first")
    mover = (rank <= 5)
    # also a softer definition: any liquid day with return > +8%
    big = liq & (ret > 0.08)

    base_mask = liq
    print(f"loaded & computed features in {time.time()-t0:.0f}s")
    print(f"movers (top-5/day): {int(mover.values.sum()):,} events | base liquid stock-days: {int(base_mask.values.sum()):,}\n")
    print("=== PRE-MOVE SETUP ATTRIBUTION (setup measured at D-1; lift = mover_rate / base_rate) ===")
    print(f"  {'setup (as of D-1)':<24}{'base rate':>11}{'mover rate':>12}{'LIFT':>8}   verdict")
    for name, feat in S.items():
        b = feat.where(base_mask)
        base_rate = np.nanmean(b.values)
        mv = feat.where(mover)
        mover_rate = np.nanmean(mv.values)
        lift = mover_rate / base_rate if base_rate > 0 else np.nan
        verdict = "STRONG" if lift >= 2 else ("some" if lift >= 1.3 else ("~none" if lift >= 0.8 else "NEGATIVE"))
        print(f"  {name:<24}{base_rate*100:>10.1f}%{mover_rate*100:>11.1f}%{lift:>8.2f}   {verdict}")

    # continuous pre-move context among movers vs base
    print("\n=== CONTINUOUS context at D-1 (mean among movers vs base) ===")
    for name, frame in [("rsi7", rsi7.shift(1)), ("vol_ratio(x20davg)", vol_ratio.shift(1)),
                        ("bb_width_pctile", bb_width.rank(pct=True).shift(1)), ("5d_prior_return%", (c.shift(1) / c.shift(6) - 1) * 100)]:
        bm = np.nanmean(frame.where(base_mask).values)
        mm = np.nanmean(frame.where(mover).values)
        print(f"  {name:<22} base {bm:>7.2f}   movers {mm:>7.2f}")
    print(f"\n  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
