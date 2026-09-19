"""Higher-timeframe state, frozen point-in-time.

WHY A 60-DAY RETURN IS NOT A WEEKLY FEATURE
-------------------------------------------
The daily factory already spans up to 60 days, so it has long-horizon
INFORMATION. What it does not have is higher-timeframe STRUCTURE: where this
week's close sits inside this week's range, how many of the last six weekly bars
closed up, whether the monthly bar is making a higher low. Those are properties
of the weekly and monthly bars themselves and cannot be recovered from a rolling
daily window.

THE POINT-IN-TIME RULE THAT MATTERS HERE
----------------------------------------
On Wednesday, this week has not finished. Its high, low and close do not exist
yet. Using them is lookahead of the most seductive kind, because the resampled
series looks perfectly innocent in a dataframe.

So every weekly feature on day D comes from the last week that CLOSED strictly
before D, and every monthly feature from the last month that closed before D.
That is what `merge_asof(..., allow_exact_matches=False)` on the period end date
enforces below, and what the truncation test in leakcheck.py will verify.

The cost is deliberate staleness: on Monday your weekly features describe the
week before last... no, the week just finished. On Thursday they still describe
that same week. That lag is not a flaw to be optimised away. It is the honest
answer to "what did you actually know?"
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# --------------------------------------------------------------------------- #
def _resample_ohlcv(g: pd.DataFrame, rule: str) -> pd.DataFrame:
    r = (g.set_index("date")
          .resample(rule)
          .agg({"open": "first", "high": "max", "low": "min",
                "close": "last", "volume": "sum"})
          .dropna(subset=["close"]))
    r.index.name = "period_end"
    return r


def _period_features(r: pd.DataFrame, tag: str) -> pd.DataFrame:
    """Features of a completed higher-timeframe bar series."""
    c, h, l, o, v = r["close"], r["high"], r["low"], r["open"], r["volume"]
    f = pd.DataFrame(index=r.index)

    f[f"{tag}_ret1"] = c.pct_change()
    f[f"{tag}_ret3"] = c.pct_change(3)
    f[f"{tag}_ret6"] = c.pct_change(6)
    f[f"{tag}_range"] = (h - l) / c.replace(0, np.nan)
    # where the bar closed inside its own range: 1 = on the high, 0 = on the low
    f[f"{tag}_clv"] = (c - l) / (h - l).replace(0, np.nan)
    f[f"{tag}_body"] = (c - o) / o.replace(0, np.nan)

    for w in (4, 12):
        f[f"{tag}_up_frac{w}"] = (c.pct_change() > 0).rolling(w, min_periods=2).mean()
        f[f"{tag}_z{w}"] = ((c - c.rolling(w, min_periods=2).mean())
                            / c.rolling(w, min_periods=2).std().replace(0, np.nan))
        f[f"{tag}_distmax{w}"] = c / h.rolling(w, min_periods=2).max() - 1.0
        f[f"{tag}_distmin{w}"] = c / l.rolling(w, min_periods=2).min() - 1.0
        f[f"{tag}_vol{w}"] = c.pct_change().rolling(w, min_periods=2).std()
        f[f"{tag}_volume_ratio{w}"] = v / v.rolling(w, min_periods=2).mean().replace(0, np.nan)

    # higher-low / lower-high structure, the thing daily windows cannot express
    f[f"{tag}_higher_low"] = (l > l.shift(1)).astype(float)
    f[f"{tag}_higher_high"] = (h > h.shift(1)).astype(float)
    f[f"{tag}_inside"] = ((h <= h.shift(1)) & (l >= l.shift(1))).astype(float)
    return f


def add_timeframe_features(panel: pd.DataFrame,
                           rules: dict[str, str] | None = None,
                           verbose: bool = True) -> tuple[pd.DataFrame, list[str]]:
    """Attach weekly and monthly point-in-time features to a daily panel."""
    rules = rules or {"w": "W-FRI", "m": "ME"}
    d = panel.sort_values(["symbol", "date"]).copy()
    out_cols: list[str] = []

    for tag, rule in rules.items():
        pieces = []
        for sym, g in d.groupby("symbol", sort=False):
            r = _resample_ohlcv(g, rule)
            if len(r) < 3:
                continue
            f = _period_features(r, tag).reset_index()
            f["symbol"] = sym
            pieces.append(f)
        if not pieces:
            continue
        hi = pd.concat(pieces, ignore_index=True)
        cols = [c for c in hi.columns if c.startswith(f"{tag}_")]

        merged = []
        for sym, g in d.groupby("symbol", sort=False):
            hs = hi[hi["symbol"] == sym].sort_values("period_end")
            if hs.empty:
                merged.append(g)
                continue
            # the period must have CLOSED strictly before this day
            m = pd.merge_asof(g.sort_values("date"),
                              hs[["period_end"] + cols].rename(
                                  columns={"period_end": "date"}),
                              on="date", direction="backward",
                              allow_exact_matches=False)
            merged.append(m)
        d = pd.concat(merged, ignore_index=True)
        out_cols += cols
        if verbose:
            print(f"  {rule:>6} features   : +{len(cols)}")

    return d.sort_values(["symbol", "date"]).reset_index(drop=True), out_cols


# --------------------------------------------------------------------------- #
# multi-horizon outcomes                                                        #
# --------------------------------------------------------------------------- #
def add_multi_horizon(panel: pd.DataFrame, horizons=(1, 2, 3, 5, 10),
                      cost: float = 0.0011,
                      verbose: bool = True) -> tuple[pd.DataFrame, dict]:
    """One frozen state, evaluated against several holding periods at once.

    A state that shifts tomorrow's odds and a state that shifts the next two
    weeks are different discoveries with different uses, and testing only H=1
    can only ever find the first kind.

    Entry is the next open in every case; exit is the close H sessions later.
    Reported per horizon: the return, the best and worst excursion along the
    way, and a binary label. Costs are charged once per round trip regardless
    of H, which is why longer horizons need a lower hit rate to break even.
    """
    d = panel.sort_values(["symbol", "date"]).copy()
    g = d.groupby("symbol", sort=False)
    entry = g["open"].shift(-1)
    d["mh_entry"] = entry
    cols: dict[str, list] = {}

    for H in horizons:
        # rolling forward window over sessions t+1 .. t+H
        fwd_hi = g["high"].shift(-1).rolling(H, min_periods=1).max().shift(-(H - 1))
        fwd_lo = g["low"].shift(-1).rolling(H, min_periods=1).min().shift(-(H - 1))
        exit_c = g["close"].shift(-H)

        r = exit_c / entry - 1.0 - cost
        d[f"y{H}_ret"] = r
        d[f"y{H}_mfe"] = fwd_hi / entry - 1.0
        d[f"y{H}_mae"] = fwd_lo / entry - 1.0
        d[f"y{H}_up"] = (r > 0).astype(float)
        d.loc[r.isna(), f"y{H}_up"] = np.nan
        cols[str(H)] = [f"y{H}_ret", f"y{H}_mfe", f"y{H}_mae", f"y{H}_up"]

    if verbose:
        print(f"\n  MULTI-HORIZON OUTCOMES  (cost {cost:.2%} charged once per trip)")
        print(f"    {'H':>4} {'n':>8} {'mean_%':>9} {'win':>7} {'mean_MFE':>10} {'mean_MAE':>10}")
        for H in horizons:
            ok = d[f"y{H}_ret"].notna()
            print(f"    {H:>4} {int(ok.sum()):>8,} "
                  f"{d.loc[ok, f'y{H}_ret'].mean()*100:>8.3f}% "
                  f"{d.loc[ok, f'y{H}_up'].mean():>7.3f} "
                  f"{d.loc[ok, f'y{H}_mfe'].mean()*100:>9.2f}% "
                  f"{d.loc[ok, f'y{H}_mae'].mean()*100:>9.2f}%")
        print("\n    These are UNCONDITIONAL base rates per horizon. Any state must")
        print("    beat the row for its own horizon, not the H=1 row.")
    return d, cols
