"""Intraday layer: exact labels + intraday features, collapsed to one row per symbol-day.

WHY THIS EXISTS
---------------
A daily bar gives you the high and the low but not which came first, so
"+1% before -0.5%" is unresolvable and the daily engine has to fall back on
`conservative` / `optimistic` bounds. Intraday bars resolve it exactly.

But 500 symbols x ~540 sessions x 375 one-minute bars is ~100M rows. The
research loop must never see that. This module runs ONCE, collapses intraday
into one row per symbol-day, and caches it. Everything downstream joins on
(date, symbol) and never knows intraday data exists.

WHAT EACH CACHED ROW CONTAINS
-----------------------------
For symbol S on session D:
  iv_*  intraday FEATURES of day D, computable at D's close  -> causal features
  ft_*  OUTCOME of entering at D's OPEN and managing through D's close

Those are different time roles and must not be confused. A state frozen at the
close of day D is scored against the ft_* of day D+1. `attach_intraday` does
that join against the real trading calendar, so a missing session shifts nothing
by accident.

RESIDUAL AMBIGUITY
------------------
If both barriers are touched inside the SAME bar, order is still unknown. The
resolver flags those rows (`ft_ambiguous`) rather than guessing. Run the same
period at 1-minute and 5-minute resolution and compare: if the ambiguous
fraction at 5-minute is under ~1%, research at 5-minute and keep 1-minute for
spot checks. That is a measured decision, not an assumption.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .sqlite_io import DBConfig, iter_intraday, list_symbols

EPS = 1e-12


@dataclass
class IntradaySpec:
    target_pct: float = 0.01
    stop_pct: float = 0.005
    bar_minutes: int = 5            # resolution used for resolution/features
    ambiguous_policy: str = "loss"  # loss | win | drop
    session_minutes: int = 375      # NSE 09:15-15:30


FT_COLS = ["ft_hit", "ft_outcome", "ft_mfe", "ft_mae", "ft_ret",
           "ft_minutes_to_target", "ft_minutes_to_stop", "ft_ambiguous", "ft_entry"]

IV_COLS = ["iv_or_ret", "iv_or_range", "iv_first_hour_vol_share", "iv_time_above_vwap",
           "iv_tod_high", "iv_tod_low", "iv_late_push", "iv_realized_vol",
           "iv_intraday_ret", "iv_vol_concentration", "iv_bars"]


# --------------------------------------------------------------------------- #
# per-symbol collapse                                                           #
# --------------------------------------------------------------------------- #
def collapse_symbol(bars: pd.DataFrame, spec: IntradaySpec) -> pd.DataFrame:
    """One symbol's intraday bars -> one row per session.

    `bars` needs columns: date (bar timestamp), open, high, low, close, volume.
    Fully vectorised; no per-day Python loop.
    """
    d = bars.dropna(subset=["open", "high", "low", "close"]).copy()
    if d.empty:
        return pd.DataFrame()
    d["session"] = pd.to_datetime(d["date"]).dt.normalize()
    d = d.sort_values("date")
    g = d.groupby("session", sort=True)

    entry = g["open"].transform("first")
    n_bars = g["close"].transform("size")
    bar_no = g.cumcount()

    up = entry * (1.0 + spec.target_pct)
    dn = entry * (1.0 - spec.stop_pct)
    inf = np.inf

    up_pos = pd.Series(np.where(d["high"].to_numpy() >= up.to_numpy(),
                                bar_no.to_numpy(), inf), index=d.index)
    dn_pos = pd.Series(np.where(d["low"].to_numpy() <= dn.to_numpy(),
                                bar_no.to_numpy(), inf), index=d.index)

    # running VWAP, for the time-above-VWAP feature
    pv = ((d["high"] + d["low"] + d["close"]) / 3.0) * d["volume"]
    cum_pv = pv.groupby(d["session"], sort=False).cumsum()
    cum_v = d["volume"].groupby(d["session"], sort=False).cumsum()
    vwap = cum_pv / (cum_v + EPS)
    above = (d["close"] > vwap).astype(float)

    bar_ret = np.log(d["close"] / d["close"].shift(1))
    bar_ret[bar_no.to_numpy() == 0] = np.nan

    or_bars = max(int(round(15.0 / spec.bar_minutes)), 1)
    hour_bars = max(int(round(60.0 / spec.bar_minutes)), 1)
    late_bars = max(int(round(30.0 / spec.bar_minutes)), 1)

    d["_above"] = above
    d["_barret"] = bar_ret
    d["_or"] = (bar_no < or_bars).astype(float)
    d["_hour"] = (bar_no < hour_bars).astype(float)
    d["_or_vol"] = d["volume"] * d["_or"]
    d["_hour_vol"] = d["volume"] * d["_hour"]
    d["_v2"] = d["volume"] ** 2
    d["_up_pos"] = up_pos
    d["_dn_pos"] = dn_pos
    d["_hi_pos"] = bar_no
    d["_entry"] = entry
    d["_nbars"] = n_bars

    g2 = d.groupby("session", sort=True)
    out = pd.DataFrame({
        "entry": g2["_entry"].first(),
        "n_bars": g2["_nbars"].first(),
        "day_high": g2["high"].max(),
        "day_low": g2["low"].min(),
        "day_close": g2["close"].last(),
        "day_vol": g2["volume"].sum(),
        "up_pos": g2["_up_pos"].min(),
        "dn_pos": g2["_dn_pos"].min(),
        "or_high": g2.apply(lambda x: x.loc[x["_or"] == 1, "high"].max(), include_groups=False),
        "or_low": g2.apply(lambda x: x.loc[x["_or"] == 1, "low"].min(), include_groups=False),
        "or_close": g2.apply(lambda x: x.loc[x["_or"] == 1, "close"].iloc[-1]
                             if (x["_or"] == 1).any() else np.nan, include_groups=False),
        "hour_vol": g2["_hour_vol"].sum(),
        "time_above_vwap": g2["_above"].mean(),
        "argmax_high": g2["high"].apply(lambda s: float(np.argmax(s.to_numpy()))),
        "argmin_low": g2["low"].apply(lambda s: float(np.argmin(s.to_numpy()))),
        "bar_ret_std": g2["_barret"].std(),
        "v2": g2["_v2"].sum(),
        "late_close": g2.apply(
            lambda x: x["close"].iloc[max(len(x) - late_bars - 1, 0)], include_groups=False),
    })
    out.index.name = "date"

    # ---------------- first-touch resolution ----------------
    up_hit = np.isfinite(out["up_pos"])
    dn_hit = np.isfinite(out["dn_pos"])
    same_bar = up_hit & dn_hit & (out["up_pos"] == out["dn_pos"])

    win = (up_hit & ~dn_hit) | (up_hit & dn_hit & (out["up_pos"] < out["dn_pos"]))
    loss = (dn_hit & ~up_hit) | (up_hit & dn_hit & (out["dn_pos"] < out["up_pos"]))

    if spec.ambiguous_policy == "win":
        win = win | same_bar
    elif spec.ambiguous_policy == "loss":
        loss = loss | same_bar

    outcome = pd.Series("timeout", index=out.index, dtype=object)
    outcome[loss] = "stop"
    outcome[win] = "target"

    res = pd.DataFrame(index=out.index)
    res["ft_entry"] = out["entry"]
    res["ft_hit"] = win.astype(float)
    res["ft_outcome"] = outcome
    res["ft_mfe"] = out["day_high"] / out["entry"] - 1.0
    res["ft_mae"] = out["day_low"] / out["entry"] - 1.0
    res["ft_ret"] = out["day_close"] / out["entry"] - 1.0
    res["ft_minutes_to_target"] = out["up_pos"].replace(np.inf, np.nan) * spec.bar_minutes
    res["ft_minutes_to_stop"] = out["dn_pos"].replace(np.inf, np.nan) * spec.bar_minutes
    res["ft_ambiguous"] = same_bar.astype(float)
    if spec.ambiguous_policy == "drop":
        res.loc[same_bar, ["ft_hit", "ft_outcome"]] = np.nan

    # ---------------- intraday features (causal at this day's close) ----------------
    res["iv_or_ret"] = out["or_close"] / out["entry"] - 1.0
    res["iv_or_range"] = (out["or_high"] - out["or_low"]) / out["entry"]
    res["iv_first_hour_vol_share"] = out["hour_vol"] / (out["day_vol"] + EPS)
    res["iv_time_above_vwap"] = out["time_above_vwap"]
    res["iv_tod_high"] = out["argmax_high"] / np.maximum(out["n_bars"] - 1, 1)
    res["iv_tod_low"] = out["argmin_low"] / np.maximum(out["n_bars"] - 1, 1)
    res["iv_late_push"] = out["day_close"] / (out["late_close"] + EPS) - 1.0
    res["iv_realized_vol"] = out["bar_ret_std"] * np.sqrt(out["n_bars"].clip(lower=1))
    res["iv_intraday_ret"] = out["day_close"] / out["entry"] - 1.0
    res["iv_vol_concentration"] = out["v2"] / (out["day_vol"] ** 2 + EPS)
    res["iv_bars"] = out["n_bars"]

    return res.reset_index()


# --------------------------------------------------------------------------- #
# cache builder                                                                 #
# --------------------------------------------------------------------------- #
def build_cache(db: DBConfig, spec: IntradaySpec, out_path: str,
                symbols: list[str] | None = None, start: str | None = None,
                end: str | None = None, verbose: bool = True) -> pd.DataFrame:
    """Stream the intraday table symbol by symbol and write the collapsed cache.

    Run this ONCE per (target, stop, bar_minutes) combination. Changing the
    barriers changes the labels, so the cache filename should encode them.
    """
    syms = symbols or list_symbols(db, db.intraday_table)
    resample = f"{spec.bar_minutes}min" if spec.bar_minutes > 1 else None
    frames, t0, done = [], time.time(), 0

    if verbose:
        print(f"\nBUILDING INTRADAY CACHE  ({len(syms)} symbols, "
              f"{spec.bar_minutes}-min bars, +{spec.target_pct:.2%}/-{spec.stop_pct:.2%})")
        print("-" * 68)

    for sym, bars in iter_intraday(db, syms, start, end, resample=resample):
        try:
            r = collapse_symbol(bars, spec)
        except Exception as e:                      # one bad symbol must not kill the job
            if verbose:
                print(f"  [skip] {sym}: {e}")
            continue
        if r.empty:
            continue
        r.insert(1, "symbol", sym)
        frames.append(r)
        done += 1
        if verbose and done % 25 == 0:
            el = time.time() - t0
            print(f"  {done:>4}/{len(syms)} symbols   {el:6.1f}s   "
                  f"eta {el / done * (len(syms) - done):6.1f}s")

    if not frames:
        raise RuntimeError("no intraday data collapsed; check the DB config and date range")

    cache = pd.concat(frames, ignore_index=True)
    cache["date"] = pd.to_datetime(cache["date"]).dt.normalize()
    cache = cache.sort_values(["symbol", "date"]).reset_index(drop=True)
    _write(cache, out_path)

    if verbose:
        amb = cache["ft_ambiguous"].mean()
        print("-" * 68)
        print(f"  rows              : {len(cache):,}  ({cache['symbol'].nunique()} symbols)")
        print(f"  dates             : {cache['date'].min().date()} -> {cache['date'].max().date()}")
        print(f"  outcome mix       : "
              f"{cache['ft_outcome'].value_counts(normalize=True).round(3).to_dict()}")
        print(f"  same-bar ambiguous: {amb:.3%}", end="")
        print("   <- fine at this resolution" if amb < 0.01
              else "   <- HIGH: re-run at 1-minute")
        print(f"  wrote {out_path}  ({time.time() - t0:.1f}s)")
    return cache


def _write(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    try:
        df.to_parquet(path, index=False)
    except Exception:
        alt = os.path.splitext(path)[0] + ".csv.gz"
        df.to_csv(alt, index=False)
        print(f"  (parquet unavailable, wrote {alt} instead - pip install pyarrow)")


def load_cache(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        alt = os.path.splitext(path)[0] + ".csv.gz"
        if os.path.exists(alt):
            path = alt
        else:
            raise FileNotFoundError(f"no intraday cache at {path}")
    df = pd.read_csv(path) if path.endswith(".gz") else pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


# --------------------------------------------------------------------------- #
# joining into the daily panel                                                  #
# --------------------------------------------------------------------------- #
def attach_intraday(panel: pd.DataFrame, cache: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Merge intraday features (day D) and exact labels (day D+1) into the panel.

    The label join goes through the actual trading calendar of each symbol, so a
    session missing from the intraday table produces a NaN rather than silently
    pairing day D with day D+2.
    """
    p = panel.sort_values(["symbol", "date"]).copy()

    # 1. intraday FEATURES of day D -> same-day merge (causal)
    iv = cache[["date", "symbol"] + [c for c in IV_COLS if c in cache.columns]]
    p = p.merge(iv, on=["date", "symbol"], how="left")

    # 2. exact OUTCOME of day D+1 -> merge on each symbol's next trading date
    p["_next_date"] = p.groupby("symbol", sort=False)["date"].shift(-1)
    ft = cache[["date", "symbol"] + [c for c in FT_COLS if c in cache.columns]].copy()
    ft = ft.rename(columns={"date": "_next_date"})
    ft = ft.rename(columns={c: ("y_" + c) for c in FT_COLS if c in ft.columns})
    p = p.merge(ft, on=["_next_date", "symbol"], how="left").drop(columns=["_next_date"])

    iv_feats = [c for c in IV_COLS if c in p.columns]
    return p, iv_feats


# --------------------------------------------------------------------------- #
# calibration: is the daily proxy label trustworthy?                            #
# --------------------------------------------------------------------------- #
def calibrate_labels(panel: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """Compare the daily proxy label against the exact first-touch label.

    This is the whole point of having intraday data for only part of the sample.
    If the proxy tracks the exact label on the overlap, you can trust states
    discovered on the full daily history. If it does not, only the intraday
    period counts as evidence.
    """
    ok = panel["y_hit"].notna() & panel["y_ft_hit"].notna()
    d = panel.loc[ok]
    if d.empty:
        raise RuntimeError("no overlap between daily labels and intraday labels")

    proxy, exact = d["y_hit"], d["y_ft_hit"]
    rows = [{
        "n": len(d),
        "proxy_rate": float(proxy.mean()),
        "exact_rate": float(exact.mean()),
        "bias": float(proxy.mean() - exact.mean()),
        "agreement": float((proxy == exact).mean()),
        "proxy_true_exact_false": float(((proxy == 1) & (exact == 0)).mean()),
        "proxy_false_exact_true": float(((proxy == 0) & (exact == 1)).mean()),
        "ambiguous_share": float(d["y_ft_ambiguous"].mean()),
    }]
    out = pd.DataFrame(rows)
    if verbose:
        r = out.iloc[0]
        print("\nLABEL CALIBRATION  (daily proxy vs exact first-touch)")
        print("-" * 68)
        print(f"  overlapping rows        : {int(r['n']):,}")
        print(f"  proxy hit rate          : {r['proxy_rate']:.4f}")
        print(f"  exact  hit rate         : {r['exact_rate']:.4f}")
        print(f"  bias (proxy - exact)    : {r['bias']:+.4f}")
        print(f"  row-level agreement     : {r['agreement']:.4f}")
        print(f"  proxy says win, was loss: {r['proxy_true_exact_false']:.4f}")
        print(f"  proxy says loss, was win: {r['proxy_false_exact_true']:.4f}")
        print(f"  same-bar ambiguous      : {r['ambiguous_share']:.4%}")
        print("-" * 68)
        if abs(r["bias"]) < 0.01 and r["agreement"] > 0.95:
            print("  VERDICT: proxy is well calibrated -> trust states mined on")
            print("           the full 2022-2026 daily history.")
        elif abs(r["bias"]) < 0.03:
            print("  VERDICT: mild bias. Mine on daily history, but report every")
            print("           hit rate against the exact label on the overlap.")
        else:
            print("  VERDICT: proxy is NOT trustworthy. Treat only the intraday")
            print("           period as evidence, and use exact labels throughout.")
    return out
