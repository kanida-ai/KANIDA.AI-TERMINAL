"""Unit tests for the read-only diagnosis.  No network, no real DB."""

from __future__ import annotations

import pytest

from market_data.repair import diagnose as D


# ---------------------------------------------------------------------------
# fixtures: the PIIND shape, recorded from docs/pattern_research/PIIND_SOURCE_AUDIT.md
# ---------------------------------------------------------------------------

def _flat_session(day: str, price: float, n: int = 24, start_min: int = 9 * 60 + 15,
                  step: int = 5) -> list[tuple]:
    rows = []
    for i in range(n):
        m = start_min + i * step
        rows.append((f"{day} {m // 60:02d}:{m % 60:02d}:00",
                     price, price + 0.5, price - 0.5, price, 1000))
    return rows


PIIND_ROWS = [
    ("2019-09-05 11:00:00", 1193.90, 1193.90, 1191.30, 1191.30, 500),
    ("2019-09-05 11:03:00", 1191.30, 1191.30, 1191.30, 1191.30, 1),
    ("2019-09-05 11:04:00", 66.00, 66.05, 65.85, 65.90, 3246),   # the bad row
    ("2019-09-05 11:05:00", 1191.30, 1191.30, 1191.00, 1191.00, 29),
    ("2019-09-05 11:06:00", 1191.00, 1192.00, 1191.00, 1191.80, 400),
]


def test_intrabucket_discontinuity_catches_the_piind_row():
    found = D.intrabucket_discontinuities(PIIND_ROWS, window=2)
    assert [f[2]["source_row"]["bar_time"] for f in found] == ["2019-09-05 11:04:00"]
    check, sev, ev = found[0]
    assert check == "intrabucket_discontinuity"
    assert sev > 10                      # ~1191 / 65.85
    assert ev["neighbour_median_close"] == pytest.approx(1191.3, abs=1.0)
    # the evidence carries the actual row, not just a count
    assert ev["source_row"]["low"] == 65.85


def test_intrabucket_discontinuity_does_not_fire_on_a_clean_session():
    assert D.intrabucket_discontinuities(_flat_session("2019-09-06", 1190.0)) == []


def test_intrabucket_discontinuity_does_not_cross_sessions():
    # a genuine overnight gap of 30% must not be flagged as an intraday spike
    rows = _flat_session("2019-09-05", 100.0) + _flat_session("2019-09-06", 70.0)
    assert D.intrabucket_discontinuities(rows) == []


# ---------------------------------------------------------------------------
# session scale / adjustment basis
# ---------------------------------------------------------------------------

def test_session_scale_ignores_a_spurious_low():
    agg = {"open": 1190.0, "high": 1218.0, "low": 65.35, "close": 1213.0}
    daily = {"open": 1190.0, "high": 1218.0, "low": 1183.1, "close": 1213.0}
    scale, instability = D.session_scale(agg, daily)
    assert scale == pytest.approx(1.0)
    assert instability == pytest.approx(0.0)


def test_session_scale_reports_shape_disagreement():
    agg = {"open": 100.0, "close": 100.0}
    daily = {"open": 100.0, "close": 110.0}
    scale, instability = D.session_scale(agg, daily)
    assert instability == pytest.approx(0.10, rel=1e-6)
    assert scale == pytest.approx(1.05)


def _series(n, value, start_day=1):
    return {f"2015-01-{start_day + i:02d}": value for i in range(n)}


def test_basis_segments_finds_one_constant_shift():
    scales = {}
    scales.update({f"2015-0{1 + i // 20}-{1 + i % 20:02d}": 4.0 for i in range(40)})
    scales.update({f"2015-0{3 + i // 20}-{1 + i % 20:02d}": 1.0 for i in range(40)})
    segs, per_session = D.basis_segments(scales)
    real = [s for s in segs if not s["same_basis"] and s["reliable"]]
    assert len(real) == 1
    assert real[0]["median_scale"] == pytest.approx(4.0)
    # the segment scale, not a per-session one, is what comparisons use, and a
    # transitional segment straddling the step is never trusted
    assert set(per_session.values()) <= {4.0, 1.0}


def test_basis_segments_rejects_a_wandering_ratio():
    """PIIND 2018-12 looked like a basis shift but the ratio ran 5.08 .. 10.10.
    Corrupt data must not be scaled away as if it were a corporate action."""
    scales = {f"2018-12-{10 + i:02d}": v for i, v in
              enumerate([5.08, 9.1, 10.1, 6.2, 8.8, 9.9, 5.5, 10.0, 7.7, 8.1])}
    segs, _ = D.basis_segments(scales)
    assert all(not s["reliable"] for s in segs if not s["same_basis"])


def test_basis_segments_tolerates_close_auction_noise():
    """A 0.3% wobble from NSE's closing VWAP must not split into segments."""
    scales = {f"2020-0{1 + i // 25}-{1 + i % 25:02d}": 1.0 + (0.003 if i % 2 else -0.003)
              for i in range(50)}
    segs, _ = D.basis_segments(scales)
    assert len(segs) == 1
    assert segs[0]["same_basis"] is True


# ---------------------------------------------------------------------------
# row rules and aggregation
# ---------------------------------------------------------------------------

def test_bar_checks_flags_zero_prices_and_stops():
    out = D.bar_checks({"open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0,
                        "volume": 0}, None)
    assert [c for c, _, _ in out] == ["non_positive_price"]


def test_bar_checks_uses_the_basis_scale_for_the_daily_comparison():
    bar = {"open": 15.2, "high": 15.25, "low": 15.1, "close": 15.2, "volume": 10}
    daily = {"open": 60.7, "high": 64.25, "low": 60.55, "close": 63.6, "_scale": 4.0}
    assert D.bar_checks(bar, daily) == []            # explained by the 4x basis
    daily_unscaled = {**daily, "_scale": 1.0}
    codes = [c for c, _, _ in D.bar_checks(bar, daily_unscaled)]
    assert "intraday_low_below_daily" in codes


def test_bar_checks_still_catches_a_spike_inside_a_scaled_session():
    bar = {"open": 1192.0, "high": 1193.0, "low": 65.35, "close": 1192.0, "volume": 10}
    daily = {"open": 1190.0, "high": 1218.0, "low": 1183.1, "close": 1213.0, "_scale": 1.0}
    codes = [c for c, _, _ in D.bar_checks(bar, daily)]
    assert "intraday_low_below_daily" in codes
    assert "low_below_half_body" in codes


def test_local_aggregation_matches_w2():
    rows = _flat_session("2021-06-01", 100.0, n=75, step=5)
    mine = D._aggregate_15m_local(rows)
    theirs, w2_bars, engine = D.aggregate_15m(rows, engine="w2")
    assert engine == "market_data.aggregate.to_15m"
    assert len(mine) == len(theirs) == 25
    for a, b in zip(mine, theirs):
        assert a["bar_start"] == b["bar_start"]
        assert (a["open"], a["high"], a["low"], a["close"], a["volume"]) == \
               (b["open"], b["high"], b["low"], b["close"], b["volume"])


def test_merge_windows_pads_and_joins():
    assert D.merge_windows(["2019-09-05"], pad_days=2) == [("2019-09-03", "2019-09-07")]
    got = D.merge_windows(["2019-09-05", "2019-09-06", "2020-01-10"], pad_days=1)
    assert got == [("2019-09-04", "2019-09-07"), ("2020-01-09", "2020-01-11")]
