"""Order-flow microstructure feature engine — VENDORED into the repo.

This is a faithful, byte-for-byte port of the feature functions in the operator's
batch reference:

    C:\\Users\\SPS\\Documents\\Kanida Ai\\outputs\\falcon_tesla\\falcons_tesla_probe.py
        (add_microstructure_features, assign_phase, + helpers)

It is copied verbatim (only the module framing changed) so the live AutoTrade
strategy runs entirely FROM THE REPO with NO import of the user's Documents
folder. Do NOT "improve" the math here — its whole purpose is to reproduce the
batch signal exactly (see the parity test in
tests/autotrade/test_tesla_short_engine.py). If the research math changes, re-port
it here deliberately and re-run parity.

Attribution: falcons_tesla_probe.py (Falcon's Tesla order-flow prototype).
"""
from __future__ import annotations

import math
import warnings

import numpy as np
import pandas as pd

try:  # pandas>=1.5
    from pandas.errors import PerformanceWarning
    warnings.simplefilter("ignore", PerformanceWarning)
except Exception:  # pragma: no cover - defensive
    pass


# ── helpers (verbatim from the probe) ────────────────────────────────────────

def clamp01(x):
    return np.clip(x, 0.0, 1.0)


def signed_clip(x, limit=1.0):
    return np.clip(x, -limit, limit)


def nonzero(s, default=np.nan):
    return s.where(s.abs() > 1e-12, default)


def weighted_nan_mean(frame: pd.DataFrame, weights: dict) -> pd.Series:
    value = pd.Series(0.0, index=frame.index)
    weight = pd.Series(0.0, index=frame.index)
    for col, w in weights.items():
        if col not in frame:
            continue
        valid = frame[col].notna()
        value.loc[valid] += frame.loc[valid, col] * w
        weight.loc[valid] += w
    return value / weight.replace(0, np.nan)


def trailing_median(s: pd.Series, window=30, min_periods=5) -> pd.Series:
    return s.rolling(window=window, min_periods=min_periods).median().combine_first(
        s.expanding(min_periods=1).median()
    )


def same_value_streak(s: pd.Series) -> pd.Series:
    same = s.eq(s.shift()) & s.notna()
    groups = same.ne(same.shift()).cumsum()
    streak = same.groupby(groups).cumsum()
    return streak.astype(float)


def root_symbol(instrument: str) -> str:
    if instrument.endswith("FUT"):
        return instrument[:-3]
    return instrument


# ── the microstructure feature pipeline (verbatim from the probe) ─────────────

def add_microstructure_features(group: pd.DataFrame) -> pd.DataFrame:
    """Compute the full minute-level microstructure feature set for ONE
    (instrument, day) group. Must be called per-group (it is path-dependent:
    cumulative volume, trailing medians, streaks)."""
    g = group.sort_values("bar_time").copy()

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "atp",
        "oi",
        "total_buy_qty",
        "total_sell_qty",
        "buy_imb%",
        "depth_bid%",
        "tick_buy%",
        "tick_avg_size",
        "tick_max_burst",
        "tick_max_order",
        "block_x",
        "last_qty",
        "avg_bid_ord",
        "avg_ask_ord",
        "b1q",
        "b2q",
        "b3q",
        "b4q",
        "b5q",
        "a1q",
        "a2q",
        "a3q",
        "a4q",
        "a5q",
        "b1o",
        "a1o",
    ]:
        if col in g:
            g[col] = pd.to_numeric(g[col], errors="coerce")

    # Exchange ATP is cumulative VWAP. This reverses the equation to estimate
    # the minute's own trade VWAP, which is the core "ATP torque" input.
    g["cum_volume"] = g["volume"].fillna(0).cumsum()
    g["prev_cum_volume"] = g["cum_volume"].shift().fillna(0)
    g["prev_atp"] = g["atp"].shift()
    g["atp_delta"] = g["atp"].diff()
    g["minute_vwap_from_atp"] = (
        (g["atp"] * g["cum_volume"]) - (g["prev_atp"] * g["prev_cum_volume"])
    ) / nonzero(g["volume"], np.nan)
    g.loc[g["minute_vwap_from_atp"].isna(), "minute_vwap_from_atp"] = g["atp"]
    g["vwap_gap_bps"] = ((g["minute_vwap_from_atp"] - g["prev_atp"]) / nonzero(g["close"], np.nan)) * 10000
    g["vwap_gap_bps"] = g["vwap_gap_bps"].replace([np.inf, -np.inf], np.nan).fillna(0)
    gap_base = trailing_median(g["vwap_gap_bps"].abs(), window=45).replace(0, np.nan).fillna(1.0)
    g["atp_torque"] = signed_clip(g["vwap_gap_bps"] / (gap_base * 2.2), 1.0)
    g["atp_bend_energy"] = (g["atp_delta"].abs().fillna(0) * g["cum_volume"]).fillna(0)
    bend_base = trailing_median(g["atp_bend_energy"], window=45).replace(0, np.nan).fillna(1.0)
    g["atp_bend_pressure"] = clamp01(g["atp_bend_energy"] / (bend_base * 2.5))

    g["ret_bps"] = g["close"].pct_change().replace([np.inf, -np.inf], np.nan).fillna(0) * 10000
    g["range_bps"] = ((g["high"] - g["low"]) / nonzero(g["close"], np.nan)).replace(
        [np.inf, -np.inf], np.nan
    ) * 10000
    g["close_pos"] = ((g["close"] - g["low"]) / nonzero(g["high"] - g["low"], np.nan)).clip(0, 1)
    g["close_pos"] = g["close_pos"].fillna(0.5)
    g["candle_aggr"] = (g["close_pos"] - 0.5) * 2.0

    median_vol = trailing_median(g["volume"].astype(float), window=45).replace(0, np.nan)
    g["volume_ratio"] = (g["volume"] / median_vol).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    g["volume_pressure"] = clamp01((g["volume_ratio"] - 0.75) / 2.25)

    g["tick_aggr"] = np.nan
    if "tick_buy%" in g:
        g["tick_aggr"] = ((g["tick_buy%"] - 50.0) / 50.0).clip(-1, 1)
    g["book_aggr"] = ((g.get("buy_imb%", np.nan) - 50.0) / 50.0).clip(-1, 1)
    g["depth_aggr"] = ((g.get("depth_bid%", np.nan) - 50.0) / 50.0).clip(-1, 1)
    g["net_aggression"] = weighted_nan_mean(
        g,
        {
            "tick_aggr": 0.45,
            "book_aggr": 0.25,
            "depth_aggr": 0.15,
            "candle_aggr": 0.15,
        },
    ).fillna(0)
    g["buy_pressure"] = clamp01(g["net_aggression"])
    g["sell_pressure"] = clamp01(-g["net_aggression"])

    bq_cols = [c for c in ["b1q", "b2q", "b3q", "b4q", "b5q"] if c in g]
    aq_cols = [c for c in ["a1q", "a2q", "a3q", "a4q", "a5q"] if c in g]
    g["top_bid_qty"] = g[bq_cols].sum(axis=1, min_count=1) if bq_cols else np.nan
    g["top_ask_qty"] = g[aq_cols].sum(axis=1, min_count=1) if aq_cols else np.nan

    if "avg_bid_ord" not in g and "b1o" in g:
        g["avg_bid_ord"] = g["b1q"] / nonzero(g["b1o"], np.nan)
    if "avg_ask_ord" not in g and "a1o" in g:
        g["avg_ask_ord"] = g["a1q"] / nonzero(g["a1o"], np.nan)

    bid_unit = g["avg_bid_ord"] if "avg_bid_ord" in g else g["top_bid_qty"]
    ask_unit = g["avg_ask_ord"] if "avg_ask_ord" in g else g["top_ask_qty"]
    bid_ask_ratio = (bid_unit / nonzero(ask_unit, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    ask_bid_ratio = (ask_unit / nonzero(bid_unit, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    g["bid_whale_pressure"] = clamp01(np.log(bid_ask_ratio) / math.log(2.5))
    g["ask_whale_pressure"] = clamp01(np.log(ask_bid_ratio) / math.log(2.5))

    g["same_b1p_streak"] = same_value_streak(g["b1p"]) if "b1p" in g else 0.0
    g["same_a1p_streak"] = same_value_streak(g["a1p"]) if "a1p" in g else 0.0
    g["bid_refill_pressure"] = clamp01((g["same_b1p_streak"] - 1.0) / 3.0) * g["bid_whale_pressure"]
    g["ask_refill_pressure"] = clamp01((g["same_a1p_streak"] - 1.0) / 3.0) * g["ask_whale_pressure"]

    if "block_x" in g:
        g["block_pressure"] = clamp01((pd.to_numeric(g["block_x"], errors="coerce").fillna(0) - 2.5) / 7.5)
    else:
        g["block_pressure"] = 0.0
    if "last_qty" in g:
        last_ratio = (g["last_qty"] / nonzero(g["volume"], np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0)
        g["block_pressure"] = np.maximum(g["block_pressure"], clamp01(last_ratio * 25.0))

    prior_5_atp = g["atp"].shift(5)  # noqa: F841 (kept for parity w/ source)
    prior_5_close = g["close"].shift(5)
    recent_atp_peak = g["atp"].shift(1).rolling(15, min_periods=3).max()
    recent_atp_trough = g["atp"].shift(1).rolling(15, min_periods=3).min()
    g["atp_turn_up_after_fall"] = ((g["atp_delta"] > 0) & (recent_atp_peak > g["atp"].shift(1))).astype(float)
    g["atp_turn_down_after_rise"] = ((g["atp_delta"] < 0) & (recent_atp_trough < g["atp"].shift(1))).astype(float)
    g["price_stall_after_fall"] = ((g["close"] >= g["close"].shift(1)) & (prior_5_close > g["close"].shift(1))).astype(float)
    g["price_stall_after_rise"] = ((g["close"] <= g["close"].shift(1)) & (prior_5_close < g["close"].shift(1))).astype(float)

    g["range_down_close_pressure"] = clamp01(1.0 - g["close_pos"])
    g["range_up_close_pressure"] = clamp01(g["close_pos"])
    g["atp_down_pressure"] = clamp01(-g["atp_torque"])
    g["atp_up_pressure"] = clamp01(g["atp_torque"])
    g["atp_bid_reversal_pressure"] = (
        g["atp_turn_up_after_fall"]
        * clamp01(0.55 * g["atp_bend_pressure"] + 0.45 * g["atp_up_pressure"])
        * clamp01(0.35 + g["sell_pressure"] + 0.50 * g["volume_pressure"])
    )
    g["atp_ask_reversal_pressure"] = (
        g["atp_turn_down_after_rise"]
        * clamp01(0.55 * g["atp_bend_pressure"] + 0.45 * g["atp_down_pressure"])
        * clamp01(0.35 + g["buy_pressure"] + 0.50 * g["volume_pressure"])
    )

    g["short_drive_core"] = (
        0.32 * g["sell_pressure"]
        + 0.24 * g["atp_down_pressure"]
        + 0.16 * g["range_down_close_pressure"]
        + 0.12 * g["volume_pressure"]
        + 0.08 * g["ask_whale_pressure"]
        + 0.08 * g["block_pressure"]
    )
    g["long_drive_core"] = (
        0.32 * g["buy_pressure"]
        + 0.24 * g["atp_up_pressure"]
        + 0.16 * g["range_up_close_pressure"]
        + 0.12 * g["volume_pressure"]
        + 0.08 * g["bid_whale_pressure"]
        + 0.08 * g["block_pressure"]
    )

    # Absorption is not just "buying"; it is opposite-side pressure showing up
    # while aggressive flow is still pushing into it.
    g["bid_absorption_core"] = (
        0.24 * g["sell_pressure"]
        + 0.17 * g["volume_pressure"]
        + 0.15 * g["bid_whale_pressure"]
        + 0.12 * g["bid_refill_pressure"]
        + 0.22 * g["atp_bid_reversal_pressure"]
        + 0.10 * g["price_stall_after_fall"]
    )
    g["ask_absorption_core"] = (
        0.24 * g["buy_pressure"]
        + 0.17 * g["volume_pressure"]
        + 0.15 * g["ask_whale_pressure"]
        + 0.12 * g["ask_refill_pressure"]
        + 0.22 * g["atp_ask_reversal_pressure"]
        + 0.10 * g["price_stall_after_rise"]
    )

    if "oi_from_open%" not in g and "oi" in g:
        first_oi = g["oi"].dropna().iloc[0] if g["oi"].notna().any() else np.nan
        g["oi_from_open%"] = ((g["oi"] - first_oi) / first_oi) * 100 if first_oi else np.nan
    first_close = g["close"].dropna().iloc[0] if g["close"].notna().any() else np.nan
    g["move_from_first_close%"] = ((g["close"] - first_close) / first_close) * 100 if first_close else np.nan

    g["oi_long_pressure"] = clamp01(g.get("oi_from_open%", 0).fillna(0) / 1.0) * (g["move_from_first_close%"] >= 0)
    g["oi_short_pressure"] = clamp01(g.get("oi_from_open%", 0).fillna(0) / 1.0) * (g["move_from_first_close%"] < 0)
    return g


def assign_phase(df: pd.DataFrame) -> pd.DataFrame:
    """Assign the discrete falcon_phase label from the drive/absorption scores
    (verbatim from the probe). Requires short_drive/long_drive/bid_absorption/
    ask_absorption/atp_*_reversal_pressure columns to already exist."""
    out = df.copy()
    conditions = [
        (out["atp_bid_reversal_pressure"] >= 0.18) & (out["bid_absorption"] >= 0.22),
        (out["atp_ask_reversal_pressure"] >= 0.18) & (out["ask_absorption"] >= 0.22),
        (out["short_drive"] >= 0.58) & (out["bid_absorption"] < 0.48),
        (out["short_drive"] >= 0.45) & (out["bid_absorption"] >= 0.48),
        (out["long_drive"] >= 0.58) & (out["ask_absorption"] < 0.48),
        (out["long_drive"] >= 0.45) & (out["ask_absorption"] >= 0.48),
        (out["bid_absorption"] >= 0.55),
        (out["ask_absorption"] >= 0.55),
    ]
    phases = [
        "ATP_BID_REVERSAL_ABSORPTION",
        "ATP_ASK_REVERSAL_DISTRIBUTION",
        "SHORT_DRIVE",
        "SELL_INTO_BID_ABSORPTION",
        "LONG_DRIVE",
        "BUY_INTO_ASK_ABSORPTION",
        "BID_ABSORPTION",
        "ASK_ABSORPTION",
    ]
    out["falcon_phase"] = np.select(conditions, phases, default="NEUTRAL")
    return out


# ── VECTORISED microstructure pipeline (byte-identical multi-group equivalent) ─
#
# add_microstructure_features is per-(instrument,day) and is called in a 498-group
# Python loop by the scorer. Profiling proved that loop is PER-GROUP-overhead bound
# (2 rows/group ≈ 376 rows/group in wall time) — so the ONLY lever is to compute
# every group at once with grouped (C-level) pandas ops instead of the Python loop.
#
# add_microstructure_features_vectorized computes the EXACT same feature set for a
# multi-(instrument,day) frame in one pass. It mirrors the per-group function
# line-for-line, replacing each path-dependent op (cumsum / shift / diff /
# pct_change / rolling / expanding / streak / first) with its group-wise form. The
# ONLY parity claim is a pure-function equivalence:
#     concat(add_microstructure_features(g) for g in groups)  ==  this(frame)
# on the SAME per-(instrument,bar_time) rows — verified byte-for-byte on the real
# universe in tests/autotrade/test_tesla_features_vectorized.py. No carried state,
# no caches — it is a stateless drop-in, so it is trivially subprocess-/pool-safe.
# Do NOT change the math here without re-running that equivalence test.
#
# PARITY NOTE (cumsum): pandas' groupby(...).cumsum() uses a DIFFERENT float
# accumulation than a per-group Series.cumsum() (a ~1e-11 ULP drift — verified),
# so cum_volume (and everything downstream of it: minute_vwap_from_atp /
# vwap_gap_bps / atp_torque / atp_bend_energy …) is computed with the byte-exact
# _segmented_cumsum below (np.cumsum per contiguous group == the per-group oracle),
# NOT groupby.cumsum. Every OTHER grouped op used here (shift / diff / pct_change /
# rolling median|mean|max|min|sum / expanding median / transform("first")) IS
# byte-identical grouped-vs-loop (verified), so cum_volume was the only fix needed.

_GKEYS = ["instrument", "day"]


def _grp(work: pd.DataFrame, gkeys):
    return work.groupby(gkeys, sort=False)


def _segmented_cumsum(values: pd.Series, group_codes: np.ndarray) -> np.ndarray:
    """Byte-identical replacement for per-group Series.cumsum() over a frame whose
    groups are CONTIGUOUS (the vectorised pipeline sorts by [*gkeys, bar_time] and
    resets the index first, so each (instrument, day) run is a contiguous block).

    pandas' groupby.cumsum() does NOT match a per-group `.cumsum()` bit-for-bit,
    so we run np.cumsum on each contiguous slice — which IS exactly what the
    per-group oracle (`group['volume'].fillna(0).cumsum()`) does. `group_codes`
    must be the row-aligned integer group id (e.g. groupby(...).ngroup())."""
    v = np.asarray(values, dtype="float64")
    out = np.empty_like(v)
    n = v.shape[0]
    if n:
        change = np.flatnonzero(group_codes[1:] != group_codes[:-1]) + 1
        starts = np.concatenate(([0], change))
        ends = np.concatenate((change, [n]))
        for s, e in zip(starts, ends):
            out[s:e] = np.cumsum(v[s:e])
    return out


def _align(series: pd.Series) -> pd.Series:
    """A grouped rolling/expanding result is MultiIndexed (gkeys…, orig_index).
    Collapse to the original RangeIndex so it aligns row-for-row with `work`."""
    s = series.copy()
    s.index = s.index.get_level_values(-1)
    return s.sort_index()


def _grouped_trailing_median(work, col, gkeys, window=30, min_periods=5) -> pd.Series:
    """Group-wise trailing_median: rolling(window,min_periods).median() with the
    expanding(min_periods=1).median() fallback for the first <min_periods rows —
    identical per group to trailing_median()."""
    roll = _align(_grp(work, gkeys)[col].rolling(window=window,
                                                 min_periods=min_periods).median())
    exp = _align(_grp(work, gkeys)[col].expanding(min_periods=1).median())
    return roll.combine_first(exp)


def _grouped_same_value_streak(work, col, gkeys) -> pd.Series:
    """Group-wise same_value_streak. `same` is s.eq(within-group shift) & notna;
    the run id is a GLOBAL cumsum of the change flag — because a group boundary
    forces change=True (the within-group shift is NaN there), runs never cross
    groups, so this equals same_value_streak computed per group."""
    s = work[col]
    shifted = _grp(work, gkeys)[col].shift()
    same = s.eq(shifted) & s.notna()
    shifted_same = same.groupby([work[k] for k in gkeys], sort=False).shift()
    change = same.ne(shifted_same)
    run_id = change.cumsum()
    return same.groupby(run_id, sort=False).cumsum().astype(float)


def add_microstructure_features_vectorized(
        df: pd.DataFrame, gkeys=_GKEYS) -> pd.DataFrame:
    """Vectorised, multi-group equivalent of add_microstructure_features.

    Byte-identical to concatenating the per-group function over every
    (instrument, day) group of `df` (see the equivalence test). Stateless."""
    g = df.sort_values([*gkeys, "bar_time"]).reset_index(drop=True).copy()

    for col in [
        "open", "high", "low", "close", "volume", "atp", "oi",
        "total_buy_qty", "total_sell_qty", "buy_imb%", "depth_bid%",
        "tick_buy%", "tick_avg_size", "tick_max_burst", "tick_max_order",
        "block_x", "last_qty", "avg_bid_ord", "avg_ask_ord",
        "b1q", "b2q", "b3q", "b4q", "b5q", "a1q", "a2q", "a3q", "a4q", "a5q",
        "b1o", "a1o",
    ]:
        if col in g:
            g[col] = pd.to_numeric(g[col], errors="coerce")

    # Byte-exact per-group cumsum (groupby.cumsum drifts ~1e-11 vs per-group).
    _codes = _grp(g, gkeys).ngroup().to_numpy()
    g["cum_volume"] = _segmented_cumsum(g["volume"].fillna(0), _codes)
    g["prev_cum_volume"] = _grp(g, gkeys)["cum_volume"].shift().fillna(0)
    g["prev_atp"] = _grp(g, gkeys)["atp"].shift()
    g["atp_delta"] = _grp(g, gkeys)["atp"].diff()
    g["minute_vwap_from_atp"] = (
        (g["atp"] * g["cum_volume"]) - (g["prev_atp"] * g["prev_cum_volume"])
    ) / nonzero(g["volume"], np.nan)
    g.loc[g["minute_vwap_from_atp"].isna(), "minute_vwap_from_atp"] = g["atp"]
    g["vwap_gap_bps"] = ((g["minute_vwap_from_atp"] - g["prev_atp"]) / nonzero(g["close"], np.nan)) * 10000
    g["vwap_gap_bps"] = g["vwap_gap_bps"].replace([np.inf, -np.inf], np.nan).fillna(0)
    gap_base = _grouped_trailing_median(
        g.assign(_vwap_abs=g["vwap_gap_bps"].abs()), "_vwap_abs", gkeys, window=45
    ).replace(0, np.nan).fillna(1.0)
    g["atp_torque"] = signed_clip(g["vwap_gap_bps"] / (gap_base * 2.2), 1.0)
    g["atp_bend_energy"] = (g["atp_delta"].abs().fillna(0) * g["cum_volume"]).fillna(0)
    bend_base = _grouped_trailing_median(g, "atp_bend_energy", gkeys, window=45).replace(0, np.nan).fillna(1.0)
    g["atp_bend_pressure"] = clamp01(g["atp_bend_energy"] / (bend_base * 2.5))

    g["ret_bps"] = _grp(g, gkeys)["close"].pct_change().replace([np.inf, -np.inf], np.nan).fillna(0) * 10000
    g["range_bps"] = ((g["high"] - g["low"]) / nonzero(g["close"], np.nan)).replace(
        [np.inf, -np.inf], np.nan) * 10000
    g["close_pos"] = ((g["close"] - g["low"]) / nonzero(g["high"] - g["low"], np.nan)).clip(0, 1)
    g["close_pos"] = g["close_pos"].fillna(0.5)
    g["candle_aggr"] = (g["close_pos"] - 0.5) * 2.0

    median_vol = _grouped_trailing_median(
        g.assign(_vol_f=g["volume"].astype(float)), "_vol_f", gkeys, window=45).replace(0, np.nan)
    g["volume_ratio"] = (g["volume"] / median_vol).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    g["volume_pressure"] = clamp01((g["volume_ratio"] - 0.75) / 2.25)

    g["tick_aggr"] = np.nan
    if "tick_buy%" in g:
        g["tick_aggr"] = ((g["tick_buy%"] - 50.0) / 50.0).clip(-1, 1)
    g["book_aggr"] = ((g.get("buy_imb%", np.nan) - 50.0) / 50.0).clip(-1, 1)
    g["depth_aggr"] = ((g.get("depth_bid%", np.nan) - 50.0) / 50.0).clip(-1, 1)
    g["net_aggression"] = weighted_nan_mean(
        g, {"tick_aggr": 0.45, "book_aggr": 0.25, "depth_aggr": 0.15, "candle_aggr": 0.15}
    ).fillna(0)
    g["buy_pressure"] = clamp01(g["net_aggression"])
    g["sell_pressure"] = clamp01(-g["net_aggression"])

    bq_cols = [c for c in ["b1q", "b2q", "b3q", "b4q", "b5q"] if c in g]
    aq_cols = [c for c in ["a1q", "a2q", "a3q", "a4q", "a5q"] if c in g]
    g["top_bid_qty"] = g[bq_cols].sum(axis=1, min_count=1) if bq_cols else np.nan
    g["top_ask_qty"] = g[aq_cols].sum(axis=1, min_count=1) if aq_cols else np.nan

    if "avg_bid_ord" not in g and "b1o" in g:
        g["avg_bid_ord"] = g["b1q"] / nonzero(g["b1o"], np.nan)
    if "avg_ask_ord" not in g and "a1o" in g:
        g["avg_ask_ord"] = g["a1q"] / nonzero(g["a1o"], np.nan)

    bid_unit = g["avg_bid_ord"] if "avg_bid_ord" in g else g["top_bid_qty"]
    ask_unit = g["avg_ask_ord"] if "avg_ask_ord" in g else g["top_ask_qty"]
    bid_ask_ratio = (bid_unit / nonzero(ask_unit, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    ask_bid_ratio = (ask_unit / nonzero(bid_unit, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    g["bid_whale_pressure"] = clamp01(np.log(bid_ask_ratio) / math.log(2.5))
    g["ask_whale_pressure"] = clamp01(np.log(ask_bid_ratio) / math.log(2.5))

    g["same_b1p_streak"] = _grouped_same_value_streak(g, "b1p", gkeys) if "b1p" in g else 0.0
    g["same_a1p_streak"] = _grouped_same_value_streak(g, "a1p", gkeys) if "a1p" in g else 0.0
    g["bid_refill_pressure"] = clamp01((g["same_b1p_streak"] - 1.0) / 3.0) * g["bid_whale_pressure"]
    g["ask_refill_pressure"] = clamp01((g["same_a1p_streak"] - 1.0) / 3.0) * g["ask_whale_pressure"]

    if "block_x" in g:
        g["block_pressure"] = clamp01((pd.to_numeric(g["block_x"], errors="coerce").fillna(0) - 2.5) / 7.5)
    else:
        g["block_pressure"] = 0.0
    if "last_qty" in g:
        last_ratio = (g["last_qty"] / nonzero(g["volume"], np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0)
        g["block_pressure"] = np.maximum(g["block_pressure"], clamp01(last_ratio * 25.0))

    prior_5_close = _grp(g, gkeys)["close"].shift(5)
    g["_atp_s1"] = _grp(g, gkeys)["atp"].shift(1)
    recent_atp_peak = _align(_grp(g, gkeys)["_atp_s1"].rolling(15, min_periods=3).max())
    recent_atp_trough = _align(_grp(g, gkeys)["_atp_s1"].rolling(15, min_periods=3).min())
    close_s1 = _grp(g, gkeys)["close"].shift(1)
    g["atp_turn_up_after_fall"] = ((g["atp_delta"] > 0) & (recent_atp_peak > g["_atp_s1"])).astype(float)
    g["atp_turn_down_after_rise"] = ((g["atp_delta"] < 0) & (recent_atp_trough < g["_atp_s1"])).astype(float)
    g["price_stall_after_fall"] = ((g["close"] >= close_s1) & (prior_5_close > close_s1)).astype(float)
    g["price_stall_after_rise"] = ((g["close"] <= close_s1) & (prior_5_close < close_s1)).astype(float)
    g = g.drop(columns=["_atp_s1"])

    g["range_down_close_pressure"] = clamp01(1.0 - g["close_pos"])
    g["range_up_close_pressure"] = clamp01(g["close_pos"])
    g["atp_down_pressure"] = clamp01(-g["atp_torque"])
    g["atp_up_pressure"] = clamp01(g["atp_torque"])
    g["atp_bid_reversal_pressure"] = (
        g["atp_turn_up_after_fall"]
        * clamp01(0.55 * g["atp_bend_pressure"] + 0.45 * g["atp_up_pressure"])
        * clamp01(0.35 + g["sell_pressure"] + 0.50 * g["volume_pressure"])
    )
    g["atp_ask_reversal_pressure"] = (
        g["atp_turn_down_after_rise"]
        * clamp01(0.55 * g["atp_bend_pressure"] + 0.45 * g["atp_down_pressure"])
        * clamp01(0.35 + g["buy_pressure"] + 0.50 * g["volume_pressure"])
    )

    g["short_drive_core"] = (
        0.32 * g["sell_pressure"] + 0.24 * g["atp_down_pressure"]
        + 0.16 * g["range_down_close_pressure"] + 0.12 * g["volume_pressure"]
        + 0.08 * g["ask_whale_pressure"] + 0.08 * g["block_pressure"]
    )
    g["long_drive_core"] = (
        0.32 * g["buy_pressure"] + 0.24 * g["atp_up_pressure"]
        + 0.16 * g["range_up_close_pressure"] + 0.12 * g["volume_pressure"]
        + 0.08 * g["bid_whale_pressure"] + 0.08 * g["block_pressure"]
    )
    g["bid_absorption_core"] = (
        0.24 * g["sell_pressure"] + 0.17 * g["volume_pressure"]
        + 0.15 * g["bid_whale_pressure"] + 0.12 * g["bid_refill_pressure"]
        + 0.22 * g["atp_bid_reversal_pressure"] + 0.10 * g["price_stall_after_fall"]
    )
    g["ask_absorption_core"] = (
        0.24 * g["buy_pressure"] + 0.17 * g["volume_pressure"]
        + 0.15 * g["ask_whale_pressure"] + 0.12 * g["ask_refill_pressure"]
        + 0.22 * g["atp_ask_reversal_pressure"] + 0.10 * g["price_stall_after_rise"]
    )

    # first-of-day scalars (per group): oi_from_open% / move_from_first_close%.
    # The per-group form uses .dropna().iloc[0] and multiplies the WHOLE group by
    # the scalar only when it is truthy (non-zero, non-NaN), else NaN everywhere.
    if "oi_from_open%" not in g and "oi" in g:
        first_oi = _grp(g, gkeys)["oi"].transform("first")
        oi_ok = first_oi.notna() & (first_oi != 0)
        g["oi_from_open%"] = np.where(oi_ok, ((g["oi"] - first_oi) / first_oi) * 100, np.nan)
    first_close = _grp(g, gkeys)["close"].transform("first")
    fc_ok = first_close.notna() & (first_close != 0)
    g["move_from_first_close%"] = np.where(fc_ok, ((g["close"] - first_close) / first_close) * 100, np.nan)

    oi_open = g["oi_from_open%"] if "oi_from_open%" in g else pd.Series(0, index=g.index)
    g["oi_long_pressure"] = clamp01(pd.Series(oi_open, index=g.index).fillna(0) / 1.0) * (g["move_from_first_close%"] >= 0)
    g["oi_short_pressure"] = clamp01(pd.Series(oi_open, index=g.index).fillna(0) / 1.0) * (g["move_from_first_close%"] < 0)
    return g
