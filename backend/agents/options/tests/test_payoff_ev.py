"""
Options Agent · payoff / cost / EV tests.

THE ANTI-FABRICATION TEST is `test_fair_priced_condor_ev_matches_theory`. Build an iron
condor where every leg is priced at exact Black-76 fair value, and the risk-neutral EV of
the structure is then known in CLOSED FORM. If our numerical integration, our payoff
geometry, our sign convention or our credit arithmetic is wrong in any way, that test fails.

It also pins down the one thing that is easy to get wrong and easy to hide: the EV of a
fairly-priced credit structure is NOT zero — it is a small NEGATIVE carry term, because the
credit is received today while the payoff settles at expiry. Asserting "approximately zero"
would paper over that. We assert the exact value instead.

Pure math — no market data, no credentials, no I/O.
"""
from __future__ import annotations

import math
import pytest

from agents.options import payoff as PO
from agents.options import pricing as P


LOT = 75            # NIFTY-scale lot
F0 = 24800.0
DTE = 30
T0 = P.year_fraction(DTE)
SIG = 0.13
DISC = math.exp(-P.R_DEFAULT * T0)


def _leg(leg_id, action, right, strike, *, lots=1, spread=2.0, oi=100000, vol=50000):
    """A leg priced at EXACT Black-76 fair value, with a symmetric quoted spread around it."""
    fair = P.price(F0, strike, T0, SIG, right)
    g = P.greeks(F0, strike, T0, SIG, right)
    return {
        "leg_id": leg_id, "action": action, "right": right, "strike": float(strike),
        "expiry": "2026-09-30", "tradingsymbol": "SYN%s%s" % (int(strike), right),
        "instrument_token": 10000 + leg_id, "lot_size": LOT, "lots": lots,
        "ltp": fair, "bid": fair - spread / 2.0, "ask": fair + spread / 2.0,
        "mid": fair, "price_used": fair, "price_source": "mid",
        "spread_pct": (spread / fair * 100.0) if fair > 0 else None,
        "oi": oi, "volume": vol,
        "iv": SIG, "delta": g["delta"], "gamma": g["gamma"],
        "theta": g["theta_per_day"], "vega": g["vega_per_1pct_vol"],
    }


def fair_condor(short_otm=400.0, width=200.0, **kw):
    """Short strangle at +/- short_otm, long wings `width` further out. All fair-priced."""
    sp, lp = F0 - short_otm, F0 - short_otm - width
    sc, lc = F0 + short_otm, F0 + short_otm + width
    return [_leg(1, "SELL", "PE", sp, **kw), _leg(2, "BUY", "PE", lp, **kw),
            _leg(3, "SELL", "CE", sc, **kw), _leg(4, "BUY", "CE", lc, **kw)]


# ────────────────────────────────────────────────────────────── structure geometry
def test_condor_is_a_credit_structure():
    legs = fair_condor()
    assert PO.net_credit(legs) > 0.0
    assert PO.net_premium(legs) == pytest.approx(-PO.net_credit(legs))


def test_condor_is_defined_risk():
    assert PO.is_defined_risk(fair_condor()) is True


def test_naked_strangle_is_not_defined_risk():
    """The check must be structural, not trust the label. Drop the wings -> unbounded."""
    legs = [l for l in fair_condor() if l["action"] == "SELL"]
    assert PO.is_defined_risk(legs) is False


def test_max_profit_and_max_loss_match_closed_form():
    width, short_otm = 200.0, 400.0
    legs = fair_condor(short_otm=short_otm, width=width)
    credit_unit = PO.net_credit(legs)
    costs = PO.total_costs(legs)["total"]

    # Between the short strikes everything expires worthless -> keep the credit, less costs.
    assert PO.max_profit(legs) == pytest.approx(credit_unit * LOT - costs, rel=1e-9)
    # Beyond a wing -> lose (width - credit) per unit, plus costs.
    assert PO.max_loss(legs) == pytest.approx(-(width - credit_unit) * LOT - costs, rel=1e-9)
    assert PO.max_loss(legs) < 0.0 < PO.max_profit(legs)


def test_payoff_is_flat_and_equal_to_credit_between_short_strikes():
    legs = fair_condor()
    a = PO.payoff_money_at(legs, F0, include_costs=False)
    b = PO.payoff_money_at(legs, F0 + 100.0, include_costs=False)
    assert a == pytest.approx(b)
    assert a == pytest.approx(PO.net_credit(legs) * LOT)


def test_breakevens_sit_outside_the_short_strikes_by_the_credit():
    """The credit keeps you profitable a little PAST each short strike, so the breakevens
    are WIDER than the body -- not inside it."""
    legs = fair_condor()
    bes = PO.breakevens(legs)
    assert len(bes) == 2
    lo, hi = bes
    assert lo < F0 < hi
    shorts = sorted(l["strike"] for l in legs if l["action"] == "SELL")
    assert lo < shorts[0] and shorts[1] < hi

    # and the distance out is exactly the credit, net of the per-unit cost drag
    credit_unit = PO.net_credit(legs)
    cost_unit = PO.total_costs(legs)["total"] / LOT
    assert shorts[0] - lo == pytest.approx(credit_unit - cost_unit, abs=1e-4)
    assert hi - shorts[1] == pytest.approx(credit_unit - cost_unit, abs=1e-4)

    # and the P&L really is ~0 there. Tolerance is set by breakevens() rounding to 6dp on
    # the underlying: 5e-7 points x LOT units x unit slope -> ~4e-5 rupees of residual.
    for be in bes:
        assert PO.payoff_money_at(legs, be) == pytest.approx(0.0, abs=1e-3)


# ────────────────────────────────────────── THE ANTI-FABRICATION TEST (EV vs closed form)
def test_fair_priced_condor_ev_matches_theory():
    """For legs priced at exact fair value, the risk-neutral EV of the TERMINAL payoff is

        EV_unit = (1/disc - 1) * net_premium_unit = -(1/disc - 1) * credit_unit

    i.e. a small NEGATIVE carry for a credit structure -- never a positive edge.
    """
    legs = fair_condor()
    credit_unit = PO.net_credit(legs)
    expected_nocost = -(1.0 / DISC - 1.0) * credit_unit * LOT

    ev = PO.expected_value(legs, F0, T0, SIG, include_costs=False)
    assert ev["ev_riskneutral"] == pytest.approx(expected_nocost, abs=1.0)

    # The headline claim: it is NOT a positive edge.
    assert ev["ev_riskneutral"] < 0.0


def test_ev_with_costs_is_approximately_minus_costs():
    """The stated theorem, end to end: fair prices in, ~ -costs out."""
    legs = fair_condor()
    costs = PO.total_costs(legs)["total"]
    carry = -(1.0 / DISC - 1.0) * PO.net_credit(legs) * LOT

    ev = PO.expected_value(legs, F0, T0, SIG, include_costs=True)
    assert ev["ev_riskneutral"] == pytest.approx(carry - costs, abs=1.5)
    assert ev["ev_riskneutral"] < 0.0
    assert ev["costs_charged"] == pytest.approx(costs)


def test_ev_realworld_is_none_not_zero():
    """Unknown must never be reported as 0.0 -- that would read as 'no edge, measured'."""
    ev = PO.expected_value(fair_condor(), F0, T0, SIG)
    assert ev["ev_realworld"] is None
    assert "variance-risk-premium" in ev["ev_realworld_reason"]


def test_ev_integration_accounts_for_the_tails_when_they_carry_mass():
    """Truncating the tails is the classic way to flatter a short-premium structure.

    At 13% vol and 30 DTE the integration bounds sit ~14 sigma out, so the tail mass is
    genuinely 0.0 -- correct, not a bug. To prove the tails are actually ACCOUNTED for
    rather than silently dropped, re-run in a regime where they carry real mass (high vol,
    long dated) and check the closed-form tail terms are non-zero and finite."""
    calm = PO.expected_value(fair_condor(), F0, T0, SIG)
    assert calm["tail_mass_lo"] == 0.0 and calm["tail_mass_hi"] == 0.0

    wild = PO.expected_value(fair_condor(), F0, P.year_fraction(365), 0.60)
    assert wild["tail_mass_lo"] > 0.0
    assert wild["tail_mass_hi"] > 0.0
    assert math.isfinite(wild["ev_riskneutral"])
    # with real tail mass the structure is bounded by its wings, so EV cannot fall below
    # the max loss -- proof the tails use the flat beyond-wing payoff, not an extrapolation
    assert wild["ev_riskneutral"] >= PO.max_loss(fair_condor()) - 1e-6


def test_selling_above_fair_value_raises_ev_and_below_lowers_it():
    """Directional sanity: a richer credit must improve EV, monotonically."""
    base = PO.expected_value(fair_condor(), F0, T0, SIG)["ev_riskneutral"]

    rich = fair_condor()
    for l in rich:                       # +5 points on each short, -5 on each long
        l["price_used"] += 5.0 if l["action"] == "SELL" else -5.0
    assert PO.expected_value(rich, F0, T0, SIG)["ev_riskneutral"] > base

    poor = fair_condor()
    for l in poor:
        l["price_used"] -= 5.0 if l["action"] == "SELL" else -5.0
    assert PO.expected_value(poor, F0, T0, SIG)["ev_riskneutral"] < base


# ──────────────────────────────────────────────────────────────────────────── POP
def test_pop_breakeven_exceeds_pop_body():
    """P(any profit) > P(max profit), because the credit keeps you green past the shorts.
    These two must never be conflated -- pop_breakeven is the headline number."""
    pp = PO.pop(fair_condor(), F0, T0, SIG)
    assert 0.0 < pp["pop_body"] < pp["pop_breakeven"] < 1.0


def test_high_pop_still_has_negative_ev_when_fairly_priced():
    """The steamroller, encoded as a test: a wide condor has a very high POP and STILL is
    not a positive-expectancy trade at fair value. POP alone must never promote anything."""
    legs = fair_condor(short_otm=1400.0, width=200.0)
    pp = PO.pop(legs, F0, T0, SIG)
    ev = PO.expected_value(legs, F0, T0, SIG)
    assert pp["pop_body"] > 0.85
    assert ev["ev_riskneutral"] < 0.0


# ────────────────────────────────────────────────────────────────────────── costs
def test_costs_are_charged_on_all_four_legs():
    legs = fair_condor()
    c = PO.total_costs(legs, round_trip=False)
    assert len(c["per_leg"]) == 4
    assert c["brokerage"] == pytest.approx(4 * PO.COSTS["brokerage_per_order"])
    assert c["total"] > 0.0


def test_stt_only_on_sells_and_stamp_only_on_buys():
    for l in fair_condor():
        c = PO.leg_costs(l)
        if l["action"] == "SELL":
            assert c["stt"] > 0.0 and c["stamp_duty"] == 0.0
        else:
            assert c["stt"] == 0.0 and c["stamp_duty"] > 0.0


def test_round_trip_costs_are_double_the_one_way():
    legs = fair_condor()
    one = PO.total_costs(legs, round_trip=False)["total"]
    two = PO.total_costs(legs, round_trip=True)["total"]
    assert two == pytest.approx(2.0 * one)


def test_costs_are_labelled_as_a_governed_estimate_not_a_measurement():
    c = PO.total_costs(fair_condor())
    assert c["costs_basis"] == "governed_table"
    assert "UNVERIFIED" in c["costs_version"]


def test_calibrate_costs_does_not_mutate_the_governed_table():
    before = dict(PO.COSTS)
    out = PO.calibrate_costs({"charges": {"total": 123.45}})
    assert out["costs_basis"] == "measured_contract_note"
    assert PO.COSTS == before


def test_wider_spread_costs_more_slippage():
    tight = PO.total_costs(fair_condor(spread=1.0))["slippage"]
    wide = PO.total_costs(fair_condor(spread=20.0))["slippage"]
    assert wide > tight


# ───────────────────────────────────────────────────────────────────────── margin
def test_margin_estimate_is_labelled_unmeasured_and_covers_max_loss():
    legs = fair_condor()
    m = PO.margin_estimate(legs)
    assert m["measured"] is False
    assert m["basis"] == "conservative_defined_risk_estimate"
    assert m["margin_est"] > abs(PO.max_loss(legs, include_costs=False))


# ───────────────────────────────────────────────────────────────────────── greeks
def test_short_condor_is_short_vega_and_long_theta():
    """A short-premium, market-neutral structure must collect theta and be short vol."""
    g = PO.net_greeks(fair_condor())
    assert g["theta_per_day"] > 0.0
    assert g["vega_per_1pct_vol"] < 0.0
    assert abs(g["delta"]) < 0.35 * LOT      # near delta-neutral by construction


def test_net_greeks_scale_with_lots():
    one = PO.net_greeks(fair_condor())
    three = PO.net_greeks(fair_condor(lots=3))
    assert three["theta_per_day"] == pytest.approx(3.0 * one["theta_per_day"])


# ────────────────────────────────────────────────────────────────────── liquidity
def test_liquidity_reports_the_worst_leg_not_an_average():
    legs = fair_condor()
    legs[1]["spread_pct"] = 40.0        # one bad wing
    legs[1]["oi"] = 12
    liq = PO.liquidity(legs)
    assert liq["worst_spread_pct"] == 40.0
    assert liq["min_oi"] == 12
    assert liq["n_legs"] == 4


def test_missing_quotes_are_counted_not_invented():
    legs = fair_condor()
    legs[3]["bid"] = None
    legs[3]["ask"] = None
    assert PO.liquidity(legs)["legs_missing_quote"] == 1
    # and a leg with no spread contributes no invented slippage
    assert PO.leg_costs(legs[3])["slippage"] == 0.0


# ────────────────────────────────────────────────────────────── one-shot evaluate()
def test_evaluate_bundle_is_complete_and_self_consistent():
    ev = PO.evaluate(fair_condor(), F0, T0, SIG)
    for k in ("credit_total", "max_profit", "max_loss", "risk_reward", "defined_risk",
              "breakevens", "pop_breakeven", "pop_body", "ev", "costs", "margin",
              "greeks", "liquidity", "price_sources"):
        assert k in ev, k
    assert ev["defined_risk"] is True
    assert ev["risk_reward"] > 0.0
    assert ev["price_sources"] == ["mid"]
    assert ev["ev"]["ev_riskneutral"] < 0.0        # fair-priced -> no edge, as it must be


def test_bad_action_and_bad_right_raise():
    legs = fair_condor()
    legs[0]["action"] = "HOLD"
    with pytest.raises(ValueError):
        PO.net_credit(legs)
    legs = fair_condor()
    legs[0]["right"] = "XX"
    with pytest.raises(ValueError):
        PO.payoff_at(legs, F0)
