"""Baseline profiling: what the stock does on its own, before any state fires.

WHY THIS MODULE EXISTS
----------------------
A state that hits 70% is impressive on a stock whose base rate is 34% and
worthless on one whose base rate is 68%. Without a per-symbol baseline you
cannot tell those apart, and a single global base rate averages 500 different
stocks into a number that describes none of them.

So this runs FIRST, per symbol, per side, and produces the denominator:

  1. Can it move enough?        average range, distribution of excursions
  2. How often does it hit?     P(reach +0.5/0.7/1.0/1.5/2.0%) both directions
  3. Does it hold or fade?      retention: touched +1% -> closed up?
  4. Long or short bias?        directional split
  5. Is the sample real?        n days, and per-year so regime shifts are visible

Then two things the daily profile alone cannot answer:

  CONTINUATION  given it reached +0.5%, how often does it reach +0.7%, +1.0%?
                (nested events, so this is exact, not estimated)
  PERSISTENCE   given it moved +1% today, how often does it move +1% again
                tomorrow, or within the next 2/3/5 sessions?

That last one is the multi-horizon question: the trigger is measured at the
daily level, the outcome is measured over several days. It is the number a state
has to beat to be worth anything.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

import numpy as np
import pandas as pd

MILESTONES = (0.005, 0.007, 0.010, 0.015, 0.020)


@dataclass
class BaselineSpec:
    milestones: Sequence[float] = MILESTONES
    horizons: Sequence[int] = (1, 2, 3, 5)
    persistence_trigger: float = 0.010     # "moved 1%" definition
    min_days: int = 250


# --------------------------------------------------------------------------- #
# core excursions                                                               #
# --------------------------------------------------------------------------- #
def add_excursions(panel: pd.DataFrame) -> pd.DataFrame:
    """Same-day excursions measured from the OPEN, which is where you enter."""
    d = panel.sort_values(["symbol", "date"]).copy()
    d["ex_up"] = d["high"] / d["open"] - 1.0
    d["ex_dn"] = d["low"] / d["open"] - 1.0
    d["ex_close"] = d["close"] / d["open"] - 1.0
    d["ex_range"] = (d["high"] - d["low"]) / d["open"]
    return d


# --------------------------------------------------------------------------- #
# 1. the dashboard                                                              #
# --------------------------------------------------------------------------- #
def symbol_baseline(panel: pd.DataFrame, spec: BaselineSpec | None = None,
                    by_year: bool = False) -> pd.DataFrame:
    """One row per symbol (or per symbol-year): the baseline dashboard."""
    spec = spec or BaselineSpec()
    d = add_excursions(panel)
    keys = ["symbol", "year"] if by_year else ["symbol"]
    if by_year:
        d["year"] = d["date"].dt.year

    rows = []
    for key, g in d.groupby(keys, sort=True):
        r = {"symbol": key[0] if isinstance(key, tuple) else key}
        if by_year:
            r["year"] = key[1]
        r["n_days"] = len(g)
        r["avg_range"] = float(g["ex_range"].mean())
        r["median_range"] = float(g["ex_range"].median())
        r["pct_up_days"] = float((g["ex_close"] > 0).mean())

        for m in spec.milestones:
            tag = f"{m*100:g}"
            r[f"p_up_{tag}"] = float((g["ex_up"] >= m).mean())
            r[f"p_dn_{tag}"] = float((g["ex_dn"] <= -m).mean())

        # retention: does it hold the move to the close, or fade it?
        t = spec.persistence_trigger
        up_t, dn_t = g["ex_up"] >= t, g["ex_dn"] <= -t
        r["retention_up"] = float((g.loc[up_t, "ex_close"] > 0).mean()) if up_t.any() else np.nan
        r["retention_dn"] = float((g.loc[dn_t, "ex_close"] < 0).mean()) if dn_t.any() else np.nan
        r["n_touch_up"] = int(up_t.sum())
        r["n_touch_dn"] = int(dn_t.sum())

        # character: >0.6 retention = momentum, <0.4 = mean reverting
        ru, rd = r["retention_up"], r["retention_dn"]
        avg = np.nanmean([ru, rd])
        r["character"] = ("momentum" if avg >= 0.60 else
                          "mean-reverting" if avg <= 0.40 else "mixed")
        rows.append(r)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# 2. continuation                                                               #
# --------------------------------------------------------------------------- #
def continuation_curve(panel: pd.DataFrame, spec: BaselineSpec | None = None,
                       side: str = "long") -> pd.DataFrame:
    """Given it reached milestone i, how often does it reach milestone j?

    Reaching a larger milestone implies reaching every smaller one, so this is a
    ratio of unconditional probabilities -- exact, not an estimate.
    """
    spec = spec or BaselineSpec()
    d = add_excursions(panel)
    col, sign = ("ex_up", 1.0) if side == "long" else ("ex_dn", -1.0)

    rows = []
    for sym, g in d.groupby("symbol", sort=True):
        n = len(g)
        reach = {m: float((g[col] * sign >= m).mean()) for m in spec.milestones}
        for i, mi in enumerate(spec.milestones):
            n_i = int(round(reach[mi] * n))
            r = {"symbol": sym, "side": side, "reached": mi,
                 "n": n_i, "p_reach": reach[mi]}
            for mj in spec.milestones[i + 1:]:
                r[f"to_{mj*100:g}"] = (reach[mj] / reach[mi]) if reach[mi] > 0 else np.nan
            nxt = spec.milestones[i + 1] if i + 1 < len(spec.milestones) else None
            r["failed_next"] = (1 - r[f"to_{nxt*100:g}"]) if nxt else np.nan
            rows.append(r)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# 3. persistence across days (the multi-horizon question)                       #
# --------------------------------------------------------------------------- #
def persistence_profile(panel: pd.DataFrame, spec: BaselineSpec | None = None,
                        side: str = "long") -> pd.DataFrame:
    """It moved today. Does it move again?

    trigger  : day D touched >= +X% (or <= -X% for short) intraday
    outcome  : the same thing happens within the next H sessions

    Reports the conditional probability next to the unconditional one, because
    the conditional number alone is meaningless. `lift` is the whole point: if
    it is ~0, "it moved yesterday" tells you nothing and any state built on top
    of that trigger is starting from zero.
    """
    spec = spec or BaselineSpec()
    d = add_excursions(panel)
    col, sign = ("ex_up", 1.0) if side == "long" else ("ex_dn", -1.0)
    t = spec.persistence_trigger

    rows = []
    for sym, g in d.groupby("symbol", sort=True):
        g = g.sort_values("date")
        hit = (g[col] * sign >= t).astype(float)
        for H in spec.horizons:
            # did it happen on any of the next H sessions?
            fwd = (hit.shift(-1).rolling(H, min_periods=1).max()
                   if H > 1 else hit.shift(-1))
            if H > 1:
                fwd = hit[::-1].rolling(H, min_periods=1).max()[::-1].shift(-1)
            ok = fwd.notna()
            if ok.sum() < 30:
                continue
            trig = (hit == 1) & ok
            uncond = float(fwd[ok].mean())
            cond = float(fwd[trig].mean()) if trig.any() else np.nan
            rows.append({
                "symbol": sym, "side": side, "trigger_pct": t, "horizon_days": H,
                "n_trigger": int(trig.sum()), "n_total": int(ok.sum()),
                "p_uncond": uncond, "p_given_moved": cond,
                "lift": cond - uncond,
            })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# 4. naive profitability of the baseline trade                                  #
# --------------------------------------------------------------------------- #
def profitability_profile(panel: pd.DataFrame, target: float = 0.007,
                          stop: float = 0.005, side: str = "long",
                          cost_bps: float = 5.0) -> pd.DataFrame:
    """"Buy every open, target +X, stop -Y." What does that alone earn?

    This is the null strategy. If a mined state cannot beat it after costs, the
    state is decoration. Daily bars cannot order the touches, so this uses the
    conservative reading (target hit AND stop never breached) and is therefore a
    LOWER bound. Use the intraday cache for the exact number.
    """
    d = add_excursions(panel)
    fav, adv = ("ex_up", "ex_dn") if side == "long" else ("ex_dn", "ex_up")
    s = 1.0 if side == "long" else -1.0

    rows = []
    for sym, g in d.groupby("symbol", sort=True):
        hit = (g[fav] * s >= target) & (g[adv] * s > -stop)
        stopped = (g[adv] * s <= -stop)
        timeout = ~hit & ~stopped
        n = len(g)
        cost = cost_bps / 10000.0
        exp = (hit.mean() * target - stopped.mean() * stop
               + (g.loc[timeout, "ex_close"] * s).sum() / max(n, 1)) - cost
        rows.append({
            "symbol": sym, "side": side, "n": n,
            "p_target": float(hit.mean()), "p_stop": float(stopped.mean()),
            "p_timeout": float(timeout.mean()),
            "expectancy_net": float(exp),
            "breakeven_hit_rate": stop / (target + stop),
            "edge_vs_breakeven": float(hit.mean()) - stop / (target + stop),
        })
    return pd.DataFrame(rows).sort_values("expectancy_net", ascending=False)


# --------------------------------------------------------------------------- #
# reporting                                                                     #
# --------------------------------------------------------------------------- #
def print_symbol_report(panel: pd.DataFrame, symbol: str,
                        spec: BaselineSpec | None = None) -> None:
    """The single-stock dashboard, both sides, in one place."""
    spec = spec or BaselineSpec()
    g = panel[panel["symbol"] == symbol]
    if g.empty:
        print(f"no data for {symbol}")
        return

    b = symbol_baseline(g, spec).iloc[0]
    print(f"\n{'=' * 68}\nBASELINE: {symbol}   "
          f"{g['date'].min().date()} -> {g['date'].max().date()}\n{'=' * 68}")
    print(f"  trading days      : {int(b['n_days']):,}")
    print(f"  avg intraday range: {b['avg_range']:.2%}   (median {b['median_range']:.2%})")
    print(f"  directional bias  : {b['pct_up_days']:.0%} up / {1 - b['pct_up_days']:.0%} down")
    print(f"  character         : {b['character'].upper()}  "
          f"(retention up {b['retention_up']:.0%} / down {b['retention_dn']:.0%})")

    print("\n  reach probability (from open)")
    print(f"    {'level':>8} {'long':>8} {'short':>8}")
    for m in spec.milestones:
        tag = f"{m*100:g}"
        print(f"    {m:>7.1%} {b[f'p_up_{tag}']:>8.0%} {b[f'p_dn_{tag}']:>8.0%}")

    for side in ("long", "short"):
        cc = continuation_curve(g, spec, side)
        print(f"\n  continuation, {side.upper()}")
        cols = [c for c in cc.columns if c.startswith("to_")]
        print("    " + f"{'reached':>8} {'n':>6} " +
              " ".join(f"{c.replace('to_', '+'):>7}" for c in cols))
        for _, r in cc.iterrows():
            vals = " ".join(("     - " if pd.isna(r[c]) else f"{r[c]:>7.0%}") for c in cols)
            print(f"    {r['reached']:>7.1%} {int(r['n']):>6} {vals}")

    print("\n  persistence: it moved "
          f"{spec.persistence_trigger:.1%} today, does it move again?")
    print(f"    {'side':>6} {'horizon':>8} {'n':>6} {'uncond':>8} {'given':>8} {'lift':>8}")
    for side in ("long", "short"):
        pp = persistence_profile(g, spec, side)
        for _, r in pp.iterrows():
            print(f"    {side:>6} {int(r['horizon_days']):>7}d {int(r['n_trigger']):>6} "
                  f"{r['p_uncond']:>8.0%} {r['p_given_moved']:>8.0%} {r['lift']:>+8.1%}")

    print("\n  null strategy (enter every open, +0.7% target / -0.5% stop, 5bps)")
    print(f"    {'side':>6} {'p_target':>9} {'breakeven':>10} {'edge':>8} {'expectancy':>11}")
    for side in ("long", "short"):
        pr = profitability_profile(g, side=side).iloc[0]
        print(f"    {side:>6} {pr['p_target']:>9.0%} {pr['breakeven_hit_rate']:>10.0%} "
              f"{pr['edge_vs_breakeven']:>+8.1%} {pr['expectancy_net']:>+11.4f}")
    print("=" * 68)


def run_baseline(panel: pd.DataFrame, spec: BaselineSpec | None = None,
                 out_dir: str = "outputs") -> dict:
    """Full-universe baseline. Writes one CSV per view."""
    import os
    spec = spec or BaselineSpec()
    os.makedirs(out_dir, exist_ok=True)
    res = {
        "baseline": symbol_baseline(panel, spec),
        "baseline_by_year": symbol_baseline(panel, spec, by_year=True),
        "continuation_long": continuation_curve(panel, spec, "long"),
        "continuation_short": continuation_curve(panel, spec, "short"),
        "persistence_long": persistence_profile(panel, spec, "long"),
        "persistence_short": persistence_profile(panel, spec, "short"),
        "profitability_long": profitability_profile(panel, side="long"),
        "profitability_short": profitability_profile(panel, side="short"),
    }
    for k, v in res.items():
        v.to_csv(f"{out_dir}/baseline_{k}.csv", index=False)

    b = res["baseline"]
    print(f"\n{'=' * 68}\nUNIVERSE BASELINE  ({len(b)} symbols)\n{'=' * 68}")
    print(f"  character mix     : {b['character'].value_counts().to_dict()}")
    print(f"  avg range         : {b['avg_range'].median():.2%} (median symbol)")
    print(f"  P(+1%) spread     : {b['p_up_1'].quantile(0.1):.0%} .. "
          f"{b['p_up_1'].quantile(0.9):.0%}  (10th-90th pct)")
    print("  ^ that spread is why a single global base rate is useless:")
    print("    the same state means different things on different symbols.")
    pl = res["persistence_long"]
    if not pl.empty:
        h1 = pl[pl["horizon_days"] == 1]
        print(f"  next-day persistence lift, long, median symbol: {h1['lift'].median():+.1%}")
    print(f"\n  wrote {len(res)} CSVs to {out_dir}/")
    return res
