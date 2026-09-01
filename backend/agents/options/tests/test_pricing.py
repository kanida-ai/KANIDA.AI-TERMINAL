"""
Options Agent · pricing tests.

Every greek is checked against a FINITE DIFFERENCE of the price function rather than against
a second hand-written formula. That matters: two hand-derived formulas can share the same
mistake, but a finite difference of price() can only agree with greeks() if the closed form
is genuinely the derivative of the price we are actually quoting.

Pure math — no market data, no credentials, no I/O.
"""
from __future__ import annotations

import math
import pytest

from agents.options import pricing as P


# A realistic NIFTY-scale case: F ~ 24800, one month out, 13% vol.
F0, T0, SIG0 = 24800.0, P.year_fraction(30), 0.13


# ────────────────────────────────────────────────────────────────────── price sanity
def test_call_put_prices_are_positive_and_bounded():
    for K in (23000.0, 24800.0, 26500.0):
        for right in ("CE", "PE"):
            p = P.price(F0, K, T0, SIG0, right)
            lo, hi = P.price_bounds(F0, K, T0, right)
            assert p > 0.0
            assert lo - 1e-9 <= p <= hi + 1e-9, (K, right, p, lo, hi)


def test_price_is_monotone_increasing_in_vol():
    """The property the IV bisection relies on. If this ever fails, implied_vol is unsound."""
    for K in (23000.0, 24800.0, 26500.0):
        for right in ("CE", "PE"):
            prev = -1.0
            for sig in (0.05, 0.10, 0.15, 0.25, 0.40, 0.80):
                p = P.price(F0, K, T0, sig, right)
                assert p > prev, (K, right, sig)
                prev = p


def test_expiry_and_zero_vol_collapse_to_intrinsic():
    assert P.price(F0, 24000.0, 0.0, SIG0, "CE") == pytest.approx(800.0)
    assert P.price(F0, 25000.0, 0.0, SIG0, "CE") == pytest.approx(0.0)
    assert P.price(F0, 25000.0, 0.0, SIG0, "PE") == pytest.approx(200.0)
    # sigma == 0 is the same limit, only discounted
    disc = math.exp(-P.R_DEFAULT * T0)
    assert P.price(F0, 24000.0, T0, 0.0, "CE") == pytest.approx(disc * 800.0)


# ─────────────────────────────────────────────────────────────────── put-call parity
def test_put_call_parity_holds_exactly():
    for K in (23000.0, 24800.0, 26500.0):
        c = P.price(F0, K, T0, SIG0, "CE")
        p = P.price(F0, K, T0, SIG0, "PE")
        assert P.parity_residual(c, p, F0, K, T0) == pytest.approx(0.0, abs=1e-8)


# ─────────────────────────────────────────────────────────────────────── implied vol
def test_iv_round_trip_recovers_sigma():
    """price -> IV -> price must return the input vol across strikes AND vol levels."""
    for K in (22000.0, 24000.0, 24800.0, 25500.0, 27000.0):
        for sig in (0.08, 0.13, 0.22, 0.45):
            for right in ("CE", "PE"):
                px = P.price(F0, K, T0, sig, right)
                iv = P.implied_vol(px, F0, K, T0, right)
                assert iv is not None, (K, sig, right)
                assert iv == pytest.approx(sig, abs=1e-5), (K, sig, right, iv)


def test_iv_returns_none_rather_than_a_fabricated_number():
    """None means 'unknown'. A caller must never receive a made-up vol for a bad quote."""
    # price above the no-arbitrage ceiling
    _, hi = P.price_bounds(F0, 24800.0, T0, "CE")
    assert P.implied_vol(hi * 1.5, F0, 24800.0, T0, "CE") is None
    # price at/below intrinsic (no time value)
    lo, _ = P.price_bounds(F0, 24000.0, T0, "CE")
    assert P.implied_vol(lo, F0, 24000.0, T0, "CE") is None
    # non-positive quote, and an expired contract
    assert P.implied_vol(0.0, F0, 24800.0, T0, "CE") is None
    assert P.implied_vol(-5.0, F0, 24800.0, T0, "CE") is None
    assert P.implied_vol(120.0, F0, 24800.0, 0.0, "CE") is None


# ───────────────────────────────────────────────────── greeks vs finite differences
def _fd(f, x, h):
    """Central difference — O(h^2) accurate, so a tight tolerance is meaningful."""
    return (f(x + h) - f(x - h)) / (2.0 * h)


@pytest.mark.parametrize("K", [23500.0, 24800.0, 26000.0])
@pytest.mark.parametrize("right", ["CE", "PE"])
def test_delta_matches_finite_difference(K, right):
    g = P.greeks(F0, K, T0, SIG0, right)
    fd = _fd(lambda f: P.price(f, K, T0, SIG0, right), F0, 1.0)
    assert g["delta"] == pytest.approx(fd, rel=1e-5, abs=1e-7)


@pytest.mark.parametrize("K", [23500.0, 24800.0, 26000.0])
@pytest.mark.parametrize("right", ["CE", "PE"])
def test_gamma_matches_second_finite_difference(K, right):
    g = P.greeks(F0, K, T0, SIG0, right)
    h = 5.0
    fd2 = (P.price(F0 + h, K, T0, SIG0, right)
           - 2.0 * P.price(F0, K, T0, SIG0, right)
           + P.price(F0 - h, K, T0, SIG0, right)) / (h * h)
    assert g["gamma"] == pytest.approx(fd2, rel=1e-3, abs=1e-9)


@pytest.mark.parametrize("K", [23500.0, 24800.0, 26000.0])
@pytest.mark.parametrize("right", ["CE", "PE"])
def test_vega_matches_finite_difference(K, right):
    g = P.greeks(F0, K, T0, SIG0, right)
    fd = _fd(lambda s: P.price(F0, K, T0, s, right), SIG0, 1e-4)
    assert g["vega_per_1_00_vol"] == pytest.approx(fd, rel=1e-5, abs=1e-6)
    # the human-facing unit must be exactly 1/100 of it — no silent unit drift
    assert g["vega_per_1pct_vol"] == pytest.approx(g["vega_per_1_00_vol"] / 100.0)


@pytest.mark.parametrize("K", [23500.0, 24800.0, 26000.0])
@pytest.mark.parametrize("right", ["CE", "PE"])
def test_theta_matches_finite_difference(K, right):
    """theta = dPrice/dt where t is calendar time, i.e. -dPrice/dT."""
    g = P.greeks(F0, K, T0, SIG0, right)
    fd = -_fd(lambda t: P.price(F0, K, t, SIG0, right), T0, 1e-5)
    assert g["theta_per_year"] == pytest.approx(fd, rel=1e-4, abs=1e-5)
    assert g["theta_per_day"] == pytest.approx(g["theta_per_year"] / 365.0)


def test_short_dated_atm_theta_is_negative_for_both_rights():
    """A long ATM option decays. Cheap sanity check that the sign convention is right."""
    for right in ("CE", "PE"):
        g = P.greeks(F0, F0, P.year_fraction(3), 0.13, right)
        assert g["theta_per_day"] < 0.0, right


def test_greeks_at_expiry_are_degenerate_not_nan():
    g = P.greeks(F0, 24000.0, 0.0, SIG0, "CE")
    assert g["delta"] == pytest.approx(1.0)
    assert g["gamma"] == 0.0 and g["vega_per_1_00_vol"] == 0.0
    for v in g.values():
        assert not math.isnan(v)


# ─────────────────────────────────────────────────── terminal distribution + probability
def test_terminal_density_integrates_to_one_and_has_mean_F():
    """Validates lognormal_terminal_pdf by numerical integration on a fine grid."""
    n, lo, hi = 400_001, 1.0, F0 * 6.0
    step = (hi - lo) / (n - 1)
    xs = [lo + i * step for i in range(n)]
    dens = P.lognormal_terminal_pdf(F0, T0, SIG0, xs)
    mass = sum(dens) * step
    mean = sum(x * d for x, d in zip(xs, dens)) * step
    assert mass == pytest.approx(1.0, abs=2e-3)
    assert mean == pytest.approx(F0, rel=2e-3)


def test_prob_between_matches_numerical_integration():
    lo_k, hi_k = 23800.0, 25800.0
    closed = P.prob_between(F0, T0, SIG0, lo_k, hi_k)
    n = 200_001
    step = (hi_k - lo_k) / (n - 1)
    xs = [lo_k + i * step for i in range(n)]
    numeric = sum(P.lognormal_terminal_pdf(F0, T0, SIG0, xs)) * step
    assert closed == pytest.approx(numeric, abs=1e-4)


def test_prob_between_unbounded_sides():
    assert P.prob_between(F0, T0, SIG0, None, None) == pytest.approx(1.0)
    below = P.prob_between(F0, T0, SIG0, None, F0)
    above = P.prob_between(F0, T0, SIG0, F0, None)
    assert below + above == pytest.approx(1.0, abs=1e-9)


def test_prob_itm_is_not_delta():
    """Guards the classic POP overstatement: N(d2) != delta. They must differ measurably."""
    K = 26000.0
    pitm = P.prob_itm(F0, K, T0, SIG0, "CE")
    delta = P.greeks(F0, K, T0, SIG0, "CE")["delta"]
    assert 0.0 < pitm < 1.0
    assert abs(pitm - delta) > 1e-3
    assert pitm < delta          # for a call, N(d2) < N(d1)


def test_prob_itm_agrees_with_prob_between():
    K = 26000.0
    assert (P.prob_itm(F0, K, T0, SIG0, "CE")
            == pytest.approx(P.prob_between(F0, T0, SIG0, K, None), abs=1e-9))


# ─────────────────────────────────────────────────────────────── governed-rate sensitivity
def test_rate_assumption_is_second_order():
    """Justifies the frozen R_DEFAULT: a full 1% rate error moves a NIFTY-scale option by
    less than a rupee or two, far below the bid-ask spread it will be traded across."""
    for K in (23500.0, 24800.0, 26000.0):
        for right in ("CE", "PE"):
            assert abs(P.rate_sensitivity(F0, K, T0, SIG0, right, bump=0.01)) < 3.0


# ────────────────────────────────────────────────────────────────────── input validation
def test_bad_right_and_bad_inputs_raise():
    with pytest.raises(ValueError):
        P.price(F0, 24800.0, T0, SIG0, "XX")
    with pytest.raises(ValueError):
        P.price(-1.0, 24800.0, T0, SIG0, "CE")
    with pytest.raises(ValueError):
        P.lognormal_terminal_pdf(F0, 0.0, SIG0, [1.0])
