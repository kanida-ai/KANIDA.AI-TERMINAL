"""Tests for the intraday layer.

The two things that can quietly ruin everything here:
  1. resolving first-touch the wrong way round
  2. joining the day-D+1 outcome onto the wrong row
Both are pinned below on hand-built bars where the answer is known by eye.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from state_engine.intraday import (IntradaySpec, attach_intraday, collapse_symbol,
                                   calibrate_labels)


def _bars(day: str, path: list[tuple[float, float, float, float]]) -> pd.DataFrame:
    """Build one session from explicit (open, high, low, close) tuples."""
    ts = pd.Timestamp(day) + pd.Timedelta(hours=9, minutes=15) + \
        pd.to_timedelta(np.arange(len(path)), unit="min")
    o, h, l, c = zip(*path)
    return pd.DataFrame({"date": ts, "open": o, "high": h, "low": l,
                         "close": c, "volume": [1000] * len(path)})


SPEC = IntradaySpec(target_pct=0.01, stop_pct=0.005, bar_minutes=1)


def test_target_before_stop_is_a_win():
    # entry 100 -> up barrier 101, down barrier 99.5
    bars = _bars("2024-06-03", [
        (100.0, 100.2, 99.9, 100.1),
        (100.1, 101.3, 100.0, 101.2),   # target touched here
        (101.2, 101.4, 99.0, 99.2),     # stop touched later
    ])
    r = collapse_symbol(bars, SPEC).iloc[0]
    assert r["ft_hit"] == 1.0
    assert r["ft_outcome"] == "target"
    assert r["ft_minutes_to_target"] == 1
    assert r["ft_minutes_to_stop"] == 2


def test_stop_before_target_is_a_loss():
    bars = _bars("2024-06-03", [
        (100.0, 100.2, 99.4, 99.5),     # stop touched first
        (99.5, 101.5, 99.4, 101.4),     # target later - too late
    ])
    r = collapse_symbol(bars, SPEC).iloc[0]
    assert r["ft_hit"] == 0.0
    assert r["ft_outcome"] == "stop"


def test_neither_barrier_is_a_timeout():
    bars = _bars("2024-06-03", [
        (100.0, 100.4, 99.8, 100.2),
        (100.2, 100.6, 99.7, 100.3),
    ])
    r = collapse_symbol(bars, SPEC).iloc[0]
    assert r["ft_outcome"] == "timeout"
    assert r["ft_hit"] == 0.0


def test_same_bar_touch_is_flagged_not_guessed():
    bars = _bars("2024-06-03", [(100.0, 101.5, 99.0, 100.5)])   # both inside bar 0
    r = collapse_symbol(bars, SPEC).iloc[0]
    assert r["ft_ambiguous"] == 1.0
    assert r["ft_outcome"] == "stop"                            # default policy = loss

    r2 = collapse_symbol(bars, IntradaySpec(ambiguous_policy="win", bar_minutes=1)).iloc[0]
    assert r2["ft_outcome"] == "target"

    r3 = collapse_symbol(bars, IntradaySpec(ambiguous_policy="drop", bar_minutes=1)).iloc[0]
    assert pd.isna(r3["ft_hit"])


def test_entry_is_the_first_bar_open():
    bars = _bars("2024-06-03", [(100.0, 100.5, 99.9, 100.4), (100.4, 100.6, 100.1, 100.5)])
    r = collapse_symbol(bars, SPEC).iloc[0]
    assert r["ft_entry"] == 100.0
    assert r["ft_mfe"] == pytest.approx(100.6 / 100.0 - 1)
    assert r["ft_mae"] == pytest.approx(99.9 / 100.0 - 1)


def test_mfe_never_below_mae():
    rng = np.random.default_rng(0)
    path = []
    p = 100.0
    for _ in range(60):
        o = p
        c = p * (1 + rng.normal(0, 0.002))
        path.append((o, max(o, c) * 1.001, min(o, c) * 0.999, c))
        p = c
    r = collapse_symbol(_bars("2024-06-03", path), SPEC).iloc[0]
    assert r["ft_mfe"] >= r["ft_mae"]


def test_sessions_are_resolved_independently():
    b1 = _bars("2024-06-03", [(100.0, 101.5, 99.9, 101.4)])     # win
    b2 = _bars("2024-06-04", [(100.0, 100.1, 99.0, 99.1)])      # loss
    out = collapse_symbol(pd.concat([b1, b2], ignore_index=True), SPEC)
    assert len(out) == 2
    assert list(out["ft_outcome"]) == ["target", "stop"]


# --------------------------------------------------------------------------- #
# the join                                                                      #
# --------------------------------------------------------------------------- #
def _panel(dates, sym="A"):
    return pd.DataFrame({"date": pd.to_datetime(dates), "symbol": sym,
                         "close": np.arange(len(dates), dtype=float)})


def test_outcome_is_taken_from_the_next_session():
    dates = ["2024-06-03", "2024-06-04", "2024-06-05"]
    panel = _panel(dates)
    cache = pd.DataFrame({
        "date": pd.to_datetime(dates), "symbol": "A",
        "ft_hit": [0.0, 1.0, 0.0], "ft_mfe": [0.0, 0.02, 0.0],
        "ft_mae": [0.0, 0.0, 0.0], "ft_ret": [0.0, 0.0, 0.0],
        "ft_outcome": ["stop", "target", "stop"], "ft_entry": [1.0, 1.0, 1.0],
        "ft_minutes_to_target": [np.nan, 30.0, np.nan],
        "ft_minutes_to_stop": [10.0, np.nan, 10.0],
        "ft_ambiguous": [0.0, 0.0, 0.0],
        "iv_or_ret": [0.1, 0.2, 0.3],
    })
    out, iv = attach_intraday(panel, cache)

    # day D's row carries day D+1's outcome
    assert out.loc[0, "y_ft_hit"] == cache.loc[1, "ft_hit"]
    assert out.loc[1, "y_ft_hit"] == cache.loc[2, "ft_hit"]
    assert pd.isna(out.loc[2, "y_ft_hit"])  # no next session

    # intraday FEATURES stay on their own day
    assert out.loc[0, "iv_or_ret"] == 0.1
    assert "iv_or_ret" in iv


def test_missing_session_produces_nan_not_a_silent_shift():
    """If 06-04 is absent from the intraday table, the 06-03 row must be NaN,
    not silently paired with 06-05."""
    panel = _panel(["2024-06-03", "2024-06-04", "2024-06-05"])
    cache = pd.DataFrame({
        "date": pd.to_datetime(["2024-06-03", "2024-06-05"]), "symbol": "A",
        "ft_hit": [0.0, 1.0], "ft_mfe": [0.0, 0.0], "ft_mae": [0.0, 0.0],
        "ft_ret": [0.0, 0.0], "ft_outcome": ["stop", "target"],
        "ft_entry": [1.0, 1.0], "ft_minutes_to_target": [np.nan, 5.0],
        "ft_minutes_to_stop": [1.0, np.nan], "ft_ambiguous": [0.0, 0.0],
        "iv_or_ret": [0.1, 0.3],
    })
    out, _ = attach_intraday(panel, cache)
    assert pd.isna(out.loc[0, "y_ft_hit"])          # 06-04 missing -> NaN
    assert out.loc[1, "y_ft_hit"] == 1.0            # 06-04 row -> 06-05 outcome


def test_symbols_do_not_bleed_into_each_other():
    panel = pd.concat([_panel(["2024-06-03", "2024-06-04"], "A"),
                       _panel(["2024-06-03", "2024-06-04"], "B")], ignore_index=True)
    cache = pd.DataFrame({
        "date": pd.to_datetime(["2024-06-03", "2024-06-04"] * 2),
        "symbol": ["A", "A", "B", "B"],
        "ft_hit": [0.0, 1.0, 0.0, 0.0], "ft_mfe": [0.0] * 4, "ft_mae": [0.0] * 4,
        "ft_ret": [0.0] * 4, "ft_outcome": ["stop"] * 4, "ft_entry": [1.0] * 4,
        "ft_minutes_to_target": [np.nan] * 4, "ft_minutes_to_stop": [1.0] * 4,
        "ft_ambiguous": [0.0] * 4, "iv_or_ret": [0.1, 0.2, 0.3, 0.4],
    })
    out, _ = attach_intraday(panel, cache)
    a = out[out["symbol"] == "A"].reset_index(drop=True)
    b = out[out["symbol"] == "B"].reset_index(drop=True)
    assert a.loc[0, "y_ft_hit"] == 1.0
    assert b.loc[0, "y_ft_hit"] == 0.0


def test_conservative_proxy_never_claims_a_win_the_exact_label_denies():
    """The conservative daily label is a strict subset of the exact one, so the
    'proxy says win, exact says loss' cell should be ~0. If it is not, the two
    labels are measuring different trades."""
    n = 300
    rng = np.random.default_rng(1)
    exact = rng.integers(0, 2, n).astype(float)
    proxy = np.where(rng.random(n) < 0.7, exact, 0.0)   # only ever misses wins
    panel = pd.DataFrame({"y_hit": proxy, "y_ft_hit": exact,
                          "y_ft_ambiguous": np.zeros(n)})
    r = calibrate_labels(panel, verbose=False).iloc[0]
    assert r["proxy_true_exact_false"] == 0.0
    assert r["bias"] <= 0
