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


def test_flat_vol_diagnostic_accounts_for_the_tails_when_they_carry_mass():
    """The flat-vol integration is now only a DIAGNOSTIC, but it must still be correct.

    At 13% vol and 30 DTE the integration bounds sit ~14 sigma out, so the tail mass is
    genuinely 0.0 -- correct, not a bug. To prove the tails are ACCOUNTED for rather than
    silently dropped, re-run where they carry real mass (high vol, long dated)."""
    calm = PO.expected_value(fair_condor(), F0, T0, SIG)["flat_vol_diagnostic"]
    assert calm["tail_mass_lo"] == 0.0 and calm["tail_mass_hi"] == 0.0

    wild = PO.expected_value(fair_condor(), F0, P.year_fraction(365), 0.60)
    d = wild["flat_vol_diagnostic"]
    assert d["tail_mass_lo"] > 0.0 and d["tail_mass_hi"] > 0.0
    assert math.isfinite(d["flat_vol_ev"])
    # bounded by the wings, so the diagnostic cannot fall below max loss -- proof the tails
    # use the flat beyond-wing payoff rather than an extrapolation
    assert d["flat_vol_ev"] >= PO.max_loss(fair_condor()) - 1e-6


def test_flat_vol_model_error_is_surfaced_not_hidden():
    """The gap between the flat-vol integration and the analytic truth must be reported, so
    nobody can mistake model error for signal."""
    ev = PO.expected_value(fair_condor(), F0, T0, SIG)
    d = ev["flat_vol_diagnostic"]
    assert "model_error_vs_analytic" in d
    assert d["model_error_vs_analytic"] == pytest.approx(
        d["flat_vol_ev"] - ev["ev_riskneutral"])
    assert "NOT signal" in d["note"]


def test_risk_neutral_ev_cannot_reward_a_better_price():
    """This test replaces one that encoded the very fallacy this module exists to prevent.

    The intuition "a richer credit must mean a better EV" is FALSE under risk-neutral
    pricing: the measure is calibrated to the prices themselves, so a larger credit is
    exactly offset by a larger expected payout. All that changes is the CARRY on the extra
    premium received today -- which makes EV very slightly MORE negative, not less.

    The consequence, and the reason this matters: risk-neutral EV can never identify a cheap
    or rich structure. Only realised, tracked outcomes can. Anything claiming otherwise is
    reading model error."""
    base = PO.expected_value(fair_condor(), F0, T0, SIG)
    rich = fair_condor()
    for l in rich:                       # +5 on each short, -5 on each long => bigger credit
        l["price_used"] += 5.0 if l["action"] == "SELL" else -5.0
    richer = PO.expected_value(rich, F0, T0, SIG)

    assert PO.net_credit(rich) > PO.net_credit(fair_condor())
    # a bigger credit carries MORE, so EV is slightly more negative -- never a free lunch
    assert richer["ev_riskneutral"] < base["ev_riskneutral"]
    # and the whole difference decomposes exactly into (a) carry on the extra credit and
    # (b) the extra percentage-based charges on the larger premium -- nothing unexplained
    d_credit = (PO.net_credit(rich) - PO.net_credit(fair_condor())) * LOT
    d_costs = PO.total_costs(rich)["total"] - PO.total_costs(fair_condor())["total"]
    assert d_costs > 0.0                     # STT/exchange/GST scale with premium
    assert (base["ev_riskneutral"] - richer["ev_riskneutral"]) == pytest.approx(
        (1.0 / DISC - 1.0) * d_credit + d_costs, rel=1e-9)


def test_ev_matches_the_analytic_carry_identity_exactly():
    """EV = -(1/disc - 1) * credit - costs, exactly, for any market-priced structure."""
    for otm, width in ((400.0, 200.0), (900.0, 300.0), (1400.0, 100.0)):
        legs = fair_condor(short_otm=otm, width=width)
        ev = PO.expected_value(legs, F0, T0, SIG)
        expect = -(1.0 / DISC - 1.0) * PO.net_credit(legs) * LOT - PO.total_costs(legs)["total"]
        assert ev["ev_riskneutral"] == pytest.approx(expect, rel=1e-12)
        assert ev["basis"] == "analytic_risk_neutral_identity"
        assert ev["ev_riskneutral"] < 0.0


# ────────────────────────────────────────────────────────────────── the volatility smile
def test_interp_iv_interpolates_and_flat_extrapolates():
    smile = [(23000.0, 0.16), (24000.0, 0.12), (25000.0, 0.10)]
    assert PO.interp_iv(smile, 23500.0) == pytest.approx(0.14)
    assert PO.interp_iv(smile, 24500.0) == pytest.approx(0.11)
    assert PO.interp_iv(smile, 22000.0) == pytest.approx(0.16)     # flat below
    assert PO.interp_iv(smile, 26000.0) == pytest.approx(0.10)     # flat above
    assert PO.interp_iv([], 24000.0) is None


def test_put_skew_lowers_pop_versus_flat_vol():
    """The measured NIFTY reality: 16-delta puts trade ~1.8 vol points ABOVE ATM. A flat-vol
    POP therefore understates the chance the put side is breached and OVERSTATES a condor's
    POP. The smile-aware path must report the lower, honest number."""
    legs = fair_condor()
    ks = sorted(l["strike"] for l in legs)
    # a realistic downward-sloping smile: cheap calls, rich puts
    skewed = [(ks[0], SIG + 0.030), (ks[1], SIG + 0.022),
              (F0, SIG), (ks[2], SIG - 0.004), (ks[3], SIG - 0.006)]

    flat = PO.pop(legs, F0, T0, SIG)
    smiled = PO.pop(legs, F0, T0, SIG, smile=sorted(skewed))

    assert flat["basis"] == "flat_atm_vol"
    assert smiled["basis"] == "smile_interpolated"
    assert smiled["pop_breakeven"] < flat["pop_breakeven"]
    assert "OVERSTATES" in flat["smile_note"]


def test_evaluate_derives_a_smile_from_the_legs_when_none_supplied():
    ev = PO.evaluate(fair_condor(), F0, T0, SIG)
    assert ev["ev"]["smile_used_by_diagnostic_only"] is True   # legs carry iv -> smile derivable
    # and the POP's OWN provenance now travels with the POP, not borrowed from the EV object
    assert ev["pop_basis"] == "smile_interpolated"


def test_build_smile_from_rows():
    rows = [{"strike": 24000.0, "iv": 0.12}, {"strike": 24000.0, "iv": 0.14},
            {"strike": 23000.0, "iv": 0.16}, {"strike": 25000.0, "iv": None}]
    s = PO.build_smile(rows)
    assert s == [(23000.0, 0.16), (24000.0, pytest.approx(0.13))]   # averaged, sorted, None dropped


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
    # A leg with no book still must not have a spread SYNTHESISED for it...
    assert legs[3]["bid"] is None and legs[3]["ask"] is None
    # ...but it must still be CHARGED, from a labelled governed assumption. The previous
    # version of this test asserted slippage == 0.0 and so locked the defect in as if it
    # were a virtue: post-market and backfilled rows have no book, which is the agent's
    # NORMAL mode, so "no book -> no slippage" silently zeroed most of the cost of trading.
    c = PO.leg_costs(legs[3])
    assert c["slippage"] > 0.0
    assert c["slippage_basis"] == PO.SLIPPAGE_BASIS_ASSUMED


def test_no_book_costs_are_charged_and_labelled_not_silently_zero():
    """The 86%-understatement regression, pinned."""
    book = fair_condor()
    nobook = fair_condor()
    for l in nobook:
        l["bid"] = l["ask"] = None

    cb, cn = PO.total_costs(book), PO.total_costs(nobook)
    assert cb["slippage"] > 0.0 and cn["slippage"] > 0.0
    # the two must be DISTINGUISHABLE by basis -- previously both said "governed_table"
    assert cb["costs_basis"] == "governed_table"
    assert cn["costs_basis"] == "governed_table+assumed_slippage"
    assert cn["slippage_legs_assumed"] == 4 and cn["slippage_legs_measured"] == 0
    assert cb["slippage_legs_assumed"] == 0
    assert "governed assumption" in cn["costs_note"]


def test_unbounded_structure_reports_no_max_loss_rather_than_a_fabricated_bound():
    """A naked short call previously reported a plausible finite max loss (~ -Rs 800k) that was
    purely an artefact of where the evaluation grid stopped."""
    naked = [l for l in fair_condor() if l["action"] == "SELL" and l["right"] == "CE"]
    assert PO.is_defined_risk(naked) is False
    assert PO.max_loss(naked) is None
    ev = PO.evaluate(naked, F0, T0, SIG)
    assert ev["max_loss"] is None
    assert ev["risk_reward"] is None
    assert "unbounded" in ev["max_loss_reason"]
    # and margin has no defined-risk proxy to lean on
    assert ev["margin"]["margin_est"] is None
    assert ev["margin"]["basis"] == "unavailable_unbounded_risk"


def test_payoff_money_uses_per_leg_units_not_leg_zero():
    """Scaling the whole structure by leg 0's units inverted the sign on non-uniform legs."""
    legs = fair_condor()
    legs[1]["lots"] = 2                       # one wing doubled
    expect = sum(PO._sign(l["action"])
                 * (PO.intrinsic(l["right"], l["strike"], 23000.0) - l["price_used"])
                 * (l["lot_size"] * l["lots"])
                 for l in legs)
    assert PO.payoff_money_at(legs, 23000.0, include_costs=False) == pytest.approx(expect)


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
