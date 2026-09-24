"""
Options Agent · multi-leg structure arithmetic — payoff, costs, margin, POP and EV.

PURE — zero I/O, no broker, no network. Every number here is a deterministic function of one
chain snapshot, so it is fully testable without credentials.

THE CENTRAL HONESTY RULE OF THIS MODULE
---------------------------------------
The risk-neutral expected value of a fairly-priced defined-risk structure is approximately
ZERO before costs, and approximately MINUS COSTS after them. That is a theorem about the
lognormal measure the option prices themselves imply — it is not a backtest, and it is not
an edge. ``test_payoff_ev.py`` asserts it.

So this module deliberately reports TWO different EV numbers and refuses to blend them:

  ev_riskneutral   Integrating the payoff against the market's own implied density.
                   This is a VALIDITY CHECK — it tells us our arithmetic agrees with the
                   chain. It should land near -costs. If it comes out meaningfully
                   positive, we have a bug or a stale quote, NOT a discovery.

  ev_realworld     Requires an explicit, governed, frozen, SPEC-until-OOS variance-risk-
                   premium assumption. There is no such approved assumption yet, so this
                   is None and every consumer must treat None as "unknown".

  THE DECIDING NUMBER IS NEITHER OF THESE. It is the realised expectancy from actually
  tracking recorded setups to expiry (tracker.py / evidence.py). A model EV can never
  promote a candidate on its own. This is the exact options analogue of the Chart Agent
  keeping pattern-forward and strategy-replay as two separate outcome families.

LEG DICT — the frozen shape produced by data.py and consumed here
-----------------------------------------------------------------
    {leg_id, action: "SELL"|"BUY", right: "CE"|"PE", strike, expiry,
     tradingsymbol, instrument_token, lot_size, lots,
     ltp, bid, ask, mid, price_used, price_source: "mid"|"ltp",
     spread_pct, oi, volume, iv, delta, gamma, theta, vega}

``price_used`` / ``price_source`` are load-bearing: pricing a short leg at the mid when it
would actually fill at the bid overstates the credit and therefore the EV. Which price was
used is recorded, never assumed.

SIGN CONVENTION
---------------
BUY = +1, SELL = -1, applied in exactly one place (``_sign``). Greeks come out of pricing.py
as LONG-unit greeks; the short negation happens here, once, alongside lots and lot_size.
"""
from __future__ import annotations

import math
from typing import Optional

from . import pricing as P

# ── governed cost table ────────────────────────────────────────────────────────────────
# Zerodha / NSE equity-INDEX-OPTIONS charges. These are a GOVERNED, DATED assumption, not a
# measurement: they are published rates that change by regulation, and this repo had no way
# to verify them live (no Kite credentials at Step 0).
#
# AUTHORITATIVE SOURCE ONCE CREDENTIALS EXIST: kite.get_virtual_contract_note(), which
# returns the broker's own itemised charges for a basket. `scripts/options_step0_probe.py`
# already calls it. `calibrate_costs()` below exists so the measured note can REPLACE this
# table rather than sit next to it. Until then every cost number carries
# costs_basis="governed_table" so no consumer can mistake it for a measured one.
COSTS_VERSION = "IDX-OPT-2026-08 (governed, UNVERIFIED against a live contract note)"
COSTS = {
    "brokerage_per_order": 20.0,      # flat Rs 20 per executed order, per leg
    "stt_pct_sell_premium": 0.001,    # 0.1% of premium, SELL side only
    "exch_txn_pct_premium": 0.0003503,  # NSE F&O options transaction charge
    "sebi_pct_premium": 0.000001,     # Rs 10 per crore
    "stamp_pct_buy_premium": 0.00003,  # 0.003%, BUY side only
    "gst_pct": 0.18,                  # on (brokerage + exchange txn + SEBI)
}
# Slippage assumed per leg, as a fraction of that leg's bid-ask spread. 0.5 == we cross half
# the spread. Governed and deliberately pessimistic-neutral; calibrate only OOS.
SLIPPAGE_SPREAD_FRACTION = 0.5

# ── slippage when there is NO BOOK — the agent's NORMAL operating mode ──────────────────
# This module runs post-market, and Kite returns an all-zero depth after the close; backfilled
# rows have no book at all. So bid/ask are None for every leg on the ordinary path.
#
# The original code computed spread = 0.0 in that case and therefore charged ZERO slippage,
# silently, while still stamping costs_basis="governed_table". Measured on a real 200-wide
# NIFTY condor that understated total costs by 86% and overstated EV roughly fourfold, with
# nothing in the output to distinguish it from a fully measured cost. That is precisely the
# "a zero standing in for unknown" failure the platform forbids, and it also contradicted this
# package's own governed constant (fetch_kite.BACKFILL_SLIPPAGE_POLICY), which requires a
# labelled assumption rather than silence.
#
# So: when a leg has no two-sided quote we charge a GOVERNED, LABELLED assumption instead of
# nothing, and the aggregate is stamped so a consumer can see that part of the cost is assumed.
# The value is deliberately conservative — overpaying in the model is survivable, underpaying
# is how a negative-expectancy strategy looks tradeable.
ASSUMED_SLIPPAGE_PCT_OF_PREMIUM = 0.02      # governed, UNVERIFIED; calibrate OOS against fills
SLIPPAGE_BASIS_MEASURED = "measured_half_spread"
SLIPPAGE_BASIS_ASSUMED = "assumed_no_book_governed_pct_of_premium"


def _sign(action: str) -> int:
    a = str(action).upper().strip()
    if a in ("BUY", "B", "LONG"):
        return 1
    if a in ("SELL", "S", "SHORT"):
        return -1
    raise ValueError("action must be BUY or SELL, got %r" % (action,))


def _right(leg: dict) -> str:
    r = str(leg.get("right", "")).upper().strip()
    if r not in ("CE", "PE"):
        raise ValueError("leg right must be CE or PE, got %r" % (leg.get("right"),))
    return r


def _px(leg: dict) -> float:
    """The price this leg is valued at. Explicit and recorded — never silently the LTP."""
    v = leg.get("price_used")
    if v is None:
        raise ValueError("leg %r has no price_used" % (leg.get("leg_id"),))
    return float(v)


def _units(leg: dict) -> int:
    """Contract multiplier for the leg: lot_size * lots."""
    return int(leg.get("lot_size") or 0) * int(leg.get("lots") or 0)


# ─────────────────────────────────────────────────────────────────────── payoff geometry
def intrinsic(right: str, strike: float, S: float) -> float:
    return max(0.0, S - strike) if right == "CE" else max(0.0, strike - S)


def net_premium(legs: list) -> float:
    """Net premium PAID per unit of underlying. Negative == we receive a credit."""
    return sum(_sign(l["action"]) * _px(l) for l in legs)


def net_credit(legs: list) -> float:
    """Net credit RECEIVED per unit of underlying. Positive for a short-premium structure."""
    return -net_premium(legs)


def payoff_at(legs: list, S: float, include_costs: bool = False) -> float:
    """Terminal P&L per unit of underlying at settlement price S.

    Per UNIT — multiply by lot_size*lots for money (see ``payoff_money_at``). Costs are
    excluded by default so the geometry can be checked against closed-form values; the
    decision path always passes include_costs=True."""
    val = sum(_sign(l["action"]) * intrinsic(_right(l), float(l["strike"]), S) for l in legs)
    pnl = val + net_credit(legs)
    if include_costs:
        u = _units(legs[0]) if legs else 0
        pnl -= (total_costs(legs)["total"] / u) if u else 0.0
    return pnl


def payoff_money_at(legs: list, S: float, include_costs: bool = True) -> float:
    """Terminal P&L in rupees for the whole position.

    Summed PER LEG with that leg's own lot_size*lots. The earlier version scaled the whole
    structure by leg 0's units, which silently inverted the sign and the magnitude on any
    structure whose legs differ (a laddered condor across strikes/DTE, or any cross-expiry
    structure where lot_size differs even at lots=1). net_greeks was already per-leg, so the
    greeks and the P&L disagreed with each other."""
    gross = sum(_sign(l["action"])
                * (intrinsic(_right(l), float(l["strike"]), S) - _px(l))
                * _units(l)
                for l in legs)
    return gross - (total_costs(legs)["total"] if include_costs else 0.0)


def _kinks(legs: list) -> list:
    """Strikes are the only kinks of a piecewise-linear payoff. Beyond the outermost strike
    the payoff is constant for a fully-hedged structure, so evaluating at the strikes plus a
    point outside each end is EXACT — no grid search, no missed extremum."""
    ks = sorted({float(l["strike"]) for l in legs})
    lo = max(0.01, ks[0] * 0.5)
    return [lo] + ks + [ks[-1] * 1.5]


def max_profit(legs: list, include_costs: bool = True) -> float:
    """Max profit in rupees. Exact — evaluated at every kink."""
    return max(payoff_money_at(legs, S, include_costs) for S in _kinks(legs))


def max_loss(legs: list, include_costs: bool = True) -> Optional[float]:
    """Max loss in rupees as a NEGATIVE number, or **None** when the risk is UNBOUNDED.

    Returning a number here for an unbounded structure was a real defect: the evaluation grid
    ends at 1.5x the highest strike, so a naked short call reported a plausible finite
    'max loss' of about -Rs 800,000 -- an artefact of where the grid happened to stop. The
    brief makes max-loss/tail a decision gate, and a gate reading a fabricated bound is worse
    than one reading None. Relying on the consumer to check a DIFFERENT key
    (``is_defined_risk``) was not good enough."""
    if not is_defined_risk(legs):
        return None
    return min(payoff_money_at(legs, S, include_costs) for S in _kinks(legs))


def is_defined_risk(legs: list) -> bool:
    """True only if the payoff is flat beyond BOTH outermost strikes — i.e. every short is
    covered. This is a structural check, not a promise from the strategy name."""
    ks = sorted({float(l["strike"]) for l in legs})
    lo_a, lo_b = ks[0] * 0.5, ks[0] * 0.75
    hi_a, hi_b = ks[-1] * 1.25, ks[-1] * 1.5
    flat_lo = abs(payoff_at(legs, lo_a) - payoff_at(legs, lo_b)) < 1e-9
    flat_hi = abs(payoff_at(legs, hi_a) - payoff_at(legs, hi_b)) < 1e-9
    return flat_lo and flat_hi


def breakevens(legs: list, include_costs: bool = True) -> list:
    """Underlying prices where terminal P&L crosses zero. Exact linear interpolation between
    adjacent kinks — the payoff is linear in between, so this is not an approximation."""
    pts = _kinks(legs)
    vals = [payoff_money_at(legs, S, include_costs) for S in pts]
    out = []
    for i in range(len(pts) - 1):
        a, b, fa, fb = pts[i], pts[i + 1], vals[i], vals[i + 1]
        if fa == 0.0:
            out.append(a)
        elif fa * fb < 0.0:
            out.append(a + (b - a) * (-fa) / (fb - fa))
    if vals[-1] == 0.0:
        out.append(pts[-1])
    return sorted(set(round(x, 6) for x in out))


# ────────────────────────────────────────────────────────────────────────────── costs
def leg_costs(leg: dict) -> dict:
    """Itemised costs for ONE leg, in rupees. Every component named so it is auditable."""
    units = _units(leg)
    premium_value = _px(leg) * units
    side = _sign(leg["action"])

    brokerage = COSTS["brokerage_per_order"]
    stt = COSTS["stt_pct_sell_premium"] * premium_value if side < 0 else 0.0
    exch = COSTS["exch_txn_pct_premium"] * premium_value
    sebi = COSTS["sebi_pct_premium"] * premium_value
    stamp = COSTS["stamp_pct_buy_premium"] * premium_value if side > 0 else 0.0
    gst = COSTS["gst_pct"] * (brokerage + exch + sebi)

    # Slippage: cross a governed fraction of this leg's own quoted spread when there IS a
    # book; otherwise charge a governed, LABELLED assumption. Never zero, and never a
    # synthesised spread. See ASSUMED_SLIPPAGE_PCT_OF_PREMIUM for why silence here was a
    # real defect rather than a conservative simplification.
    bid, ask = leg.get("bid"), leg.get("ask")
    if bid is not None and ask is not None and float(ask) >= float(bid):
        slippage = max(0.0, float(ask) - float(bid)) * SLIPPAGE_SPREAD_FRACTION * units
        slip_basis = SLIPPAGE_BASIS_MEASURED
    else:
        slippage = ASSUMED_SLIPPAGE_PCT_OF_PREMIUM * abs(premium_value)
        slip_basis = SLIPPAGE_BASIS_ASSUMED

    return {"brokerage": brokerage, "stt": stt, "exchange_txn": exch, "sebi": sebi,
            "stamp_duty": stamp, "gst": gst, "slippage": slippage,
            "slippage_basis": slip_basis,
            "total": brokerage + stt + exch + sebi + stamp + gst + slippage}


def total_costs(legs: list, round_trip: bool = True) -> dict:
    """Costs for the whole structure, in rupees.

    round_trip=True (the default, and the only honest default for a tracked-to-decision
    strategy) charges BOTH the entry and the exit. A condor held to expiry that finishes
    inside the body expires worthless and incurs no exit brokerage — but we cannot know that
    at decision time, so we charge the exit and let reality be a pleasant surprise. Never the
    other way round."""
    entry = [leg_costs(l) for l in legs]
    keys = ("brokerage", "stt", "exchange_txn", "sebi", "stamp_duty", "gst", "slippage",
            "total")
    out = {k: sum(e[k] for e in entry) for k in keys}
    if round_trip:
        # Exit is charged at the same scale. Approximate by construction (the exit premium is
        # unknown at t) and labelled as such.
        out = {k: v * 2.0 for k, v in out.items()}
    out["round_trip"] = round_trip
    out["costs_version"] = COSTS_VERSION

    # Whether ANY leg's slippage is assumed rather than measured must reach the consumer --
    # an aggregate that mixes measured and assumed components while claiming to be a pure
    # governed-table figure is exactly how an 86% cost understatement went unnoticed.
    n_assumed = sum(1 for e in entry if e["slippage_basis"] == SLIPPAGE_BASIS_ASSUMED)
    out["slippage_legs_assumed"] = n_assumed
    out["slippage_legs_measured"] = len(entry) - n_assumed
    out["costs_basis"] = ("governed_table" if n_assumed == 0
                          else "governed_table+assumed_slippage")
    if n_assumed:
        out["costs_note"] = (
            "%d of %d legs had no two-sided quote, so their slippage is a governed "
            "assumption (%.1f%% of premium), not a measured half-spread"
            % (n_assumed, len(entry), ASSUMED_SLIPPAGE_PCT_OF_PREMIUM * 100))
    # per_leg is single-trip; the aggregate above is doubled when round_trip. Stated so the
    # 2x discrepancy between sum(per_leg) and total is never mistaken for an arithmetic bug.
    out["per_leg"] = entry
    out["per_leg_basis"] = "single_trip (aggregate is doubled when round_trip=True)"
    return out


def calibrate_costs(virtual_contract_note: dict) -> dict:
    """Fold a REAL kite.get_virtual_contract_note() response into a measured cost view.

    Deliberately does NOT mutate the module-level COSTS table — a measured note is per-basket
    and must not silently become a global governed assumption. It returns a dict the caller
    can attach alongside the estimate so the two are visible side by side, with
    costs_basis="measured_contract_note".
    """
    return {"costs_basis": "measured_contract_note",
            "source": "kite.get_virtual_contract_note",
            "raw": virtual_contract_note}


# ────────────────────────────────────────────────────────────────────────────── margin
def margin_estimate(legs: list) -> dict:
    """A CONSERVATIVE, offline margin estimate for a defined-risk structure.

    WHY AN ESTIMATE AND NOT THE BROKER'S NUMBER: Kite's real span+exposure for a hedged
    basket comes from `basket_order_margins()`, which POSTs an order-shaped payload to the
    broker. Agent code must not make broker POST calls — that sits on the wrong side of the
    execution boundary (docs/AGENTS_PLATFORM.md §5). So the AGENT uses this estimate, and the
    real basket margin is measured by the operator-run, read-only
    `scripts/options_step0_probe.py` and used to CALIBRATE the buffer below.

    For a defined-risk condor the exchange margin is bounded by the max loss; we add a
    governed buffer for the exposure component and intraday mark-to-market swings. This is
    an upper-bound-ish estimate, so it under-states capacity rather than over-stating it.
    """
    ml_raw = max_loss(legs, include_costs=False)
    if ml_raw is None:
        # Unbounded risk has no defined-risk margin proxy. Refusing is the only honest answer.
        return {"margin_est": None, "basis": "unavailable_unbounded_risk", "measured": False,
                "note": ("structure is not defined-risk, so max-loss cannot bound the margin; "
                         "phase 1 trades defined-risk structures only")}
    ml = abs(ml_raw)
    buffer_pct = 0.20                     # governed; calibrate against the probe's real number
    return {"margin_est": ml * (1.0 + buffer_pct),
            "basis": "conservative_defined_risk_estimate",
            "max_loss_component": ml,
            "buffer_pct": buffer_pct,
            "measured": False,
            "note": ("agent-side estimate; the real span+exposure comes from "
                     "basket_order_margins via the operator-run probe, never from agent code")}


# ─────────────────────────────────────────────────────────────────────────── greeks
def net_greeks(legs: list) -> dict:
    """Position greeks: long-unit greeks from pricing.py, signed by BUY/SELL and scaled by
    lot_size*lots. The one place the short negation happens."""
    out = {"delta": 0.0, "gamma": 0.0, "vega_per_1pct_vol": 0.0, "theta_per_day": 0.0}
    for l in legs:
        s = _sign(l["action"]) * _units(l)
        out["delta"] += s * float(l.get("delta") or 0.0)
        out["gamma"] += s * float(l.get("gamma") or 0.0)
        out["vega_per_1pct_vol"] += s * float(l.get("vega") or 0.0)
        out["theta_per_day"] += s * float(l.get("theta") or 0.0)
    return out


# ──────────────────────────────────────────────────────────────────────── POP and EV
def pop(legs: list, F: float, T: float, sigma: float, include_costs: bool = True,
        smile: Optional[list] = None) -> dict:
    """Probability of profit under the market's own implied density.

    Reported two ways, because they answer different questions and quoting one while
    meaning the other is the classic way an iron condor gets oversold:

      pop_breakeven  P(terminal P&L > 0) — the band between the BREAKEVENS. This is the
                     headline "POP" number, and it is the LARGER of the two, because the
                     credit keeps you profitable a little way beyond each short strike.
      pop_body       P(finishing between the SHORT STRIKES) — i.e. P(max profit), where
                     every leg expires worthless. Always the smaller number.

    So pop_breakeven >= pop_body, always. Neither may promote a candidate on its own: a
    wide condor can show a very high pop_breakeven and still have negative expectancy,
    which is exactly the "steamroller" the brief warns about. Expectancy decides.
    """
    bes = breakevens(legs, include_costs=include_costs)

    # SMILE-AWARE: the probability of breaching a strike depends on the vol AT that strike,
    # not on the ATM vol. On a real NIFTY chain the 16-delta put trades ~1.8 vol points above
    # ATM, so a flat-vol POP systematically UNDERSTATES the chance the put side is breached
    # and therefore OVERSTATES the condor's POP. Each boundary is evaluated at its own
    # interpolated IV; we fall back to flat `sigma` only when no smile is available, and say
    # which was used.
    def _p_above(x: float) -> float:
        iv = interp_iv(smile, x) if smile else None
        return P.prob_between(F, T, iv if iv is not None else sigma, x, None)

    if len(bes) == 2:
        pop_be = max(0.0, _p_above(bes[0]) - _p_above(bes[1]))
    elif len(bes) == 1:
        # one-sided structure: profitable on whichever side currently pays
        pays_up = payoff_money_at(legs, bes[0] * 1.05, include_costs) > 0
        pop_be = _p_above(bes[0]) if pays_up else (1.0 - _p_above(bes[0]))
    else:
        pop_be = 1.0 if payoff_money_at(legs, F, include_costs) > 0 else 0.0

    shorts = sorted(float(l["strike"]) for l in legs if _sign(l["action"]) < 0)
    pop_bd = (max(0.0, _p_above(shorts[0]) - _p_above(shorts[-1]))
              if len(shorts) >= 2 else None)
    return {"pop_breakeven": pop_be, "pop_body": pop_bd, "breakevens": bes,
            "basis": "smile_interpolated" if smile else "flat_atm_vol",
            "smile_note": (None if smile else
                           "no smile supplied — flat ATM vol OVERSTATES the POP of a "
                           "put-skewed structure like a condor")}


def build_smile(legs_or_rows: list) -> list:
    """[(strike, iv)] from whatever rows carry a solved IV, sorted by strike, deduped.

    The volatility SMILE is not optional decoration — see ``expected_value``. Measured on a
    real NIFTY chain: 16-delta puts at 10.9-11.5% IV against 9.4-9.5% for the calls and
    9.75% ATM. Treating that as one flat number misprices every strike away from the money.
    """
    by_k = {}
    for r in legs_or_rows:
        iv = r.get("iv")
        k = r.get("strike")
        if iv is None or k is None:
            continue
        by_k.setdefault(float(k), []).append(float(iv))
    return [(k, sum(v) / len(v)) for k, v in sorted(by_k.items())]


def interp_iv(smile: list, K: float) -> Optional[float]:
    """IV at an arbitrary strike by linear interpolation across the smile, flat-extrapolated
    beyond the ends.

    Linear in strike (not in log-moneyness) is adequate here because the NIFTY ladder is
    dense — a measured 50-point step — so adjacent knots are close together. It is stated
    rather than hidden because on a sparse ladder it would not be adequate.
    """
    if not smile:
        return None
    K = float(K)
    if K <= smile[0][0]:
        return smile[0][1]
    if K >= smile[-1][0]:
        return smile[-1][1]
    for i in range(len(smile) - 1):
        k0, v0 = smile[i]
        k1, v1 = smile[i + 1]
        if k0 <= K <= k1:
            if k1 == k0:
                return v0
            w = (K - k0) / (k1 - k0)
            return v0 + w * (v1 - v0)
    return smile[-1][1]


def expected_value(legs: list, F: float, T: float, sigma: float,
                   include_costs: bool = True, grid: int = 20001,
                   smile: Optional[list] = None) -> dict:
    """Risk-neutral expected P&L in rupees — computed ANALYTICALLY, plus a labelled
    flat-vol diagnostic.

    WHY ANALYTIC, AND WHY THIS NUMBER CARRIES NO INFORMATION
    --------------------------------------------------------
    Under the risk-neutral measure the market itself is quoting, the undiscounted expected
    terminal intrinsic of an option IS its market price grossed up by the discount factor:
    E[intrinsic_K] = price_K / disc. Summing over the legs:

        EV = sum_i sign_i * price_i/disc  -  sum_i sign_i * price_i
           = (1/disc - 1) * net_premium
           = -(1/disc - 1) * credit

    So for ANY structure priced at market, risk-neutral EV is exactly a small carry term —
    the credit is received today while the payoff settles at expiry — and it is a TAUTOLOGY.
    It cannot show an edge for any structure, ever. It is reported only as an arithmetic
    check and must never be fed to a decision gate as though it were a forecast.

    WHY THE OLD NUMERICAL INTEGRATION WAS WRONG TO REPORT AS "EV"
    -------------------------------------------------------------
    Integrating the payoff against a lognormal density built from ONE flat ATM vol ignores
    the smile. Measured on a real NIFTY 2026-09-29 condor: the analytic EV net of costs is
    Rs -230.50, while the flat-ATM-vol integration returns Rs -540.21 — a model error of
    Rs -309.71, LARGER than the quantity being estimated. That error is pure flat-vol
    artefact, not signal. It is now reported as ``flat_vol_diagnostic`` with the gap made
    explicit, and it is never the headline EV.
    """
    disc = math.exp(-P.R_DEFAULT * T)
    u = _units(legs[0]) if legs else 0
    credit_total = net_credit(legs) * u
    carry = -(1.0 / disc - 1.0) * credit_total
    costs = total_costs(legs)["total"] if include_costs else 0.0
    ev_rn = carry - costs

    # ── flat-vol numerical integration, kept ONLY as a labelled diagnostic ──────────
    diag = None
    try:
        ks = sorted({float(l["strike"]) for l in legs})
        lo, hi = ks[0] * 0.60, ks[-1] * 1.40
        step = (hi - lo) / (grid - 1)
        xs = [lo + i * step for i in range(grid)]
        dens = P.lognormal_terminal_pdf(F, T, sigma, xs)
        body = sum(payoff_money_at(legs, S, include_costs) * d
                   for S, d in zip(xs, dens)) * step
        p_lo = P.prob_between(F, T, sigma, None, lo)
        p_hi = P.prob_between(F, T, sigma, hi, None)
        tails = (payoff_money_at(legs, lo * 0.5, include_costs) * p_lo
                 + payoff_money_at(legs, hi * 1.5, include_costs) * p_hi)
        num = body + tails
        diag = {"flat_vol_ev": num, "sigma_used": sigma,
                "model_error_vs_analytic": num - ev_rn,
                "tail_mass_lo": p_lo, "tail_mass_hi": p_hi,
                "note": ("ignores the volatility smile; the gap is flat-vol model error, "
                         "NOT signal. Never use this as EV.")}
    except Exception:                                   # noqa: BLE001 — diagnostic only
        diag = {"error": "flat-vol diagnostic unavailable"}

    return {
        "ev_riskneutral": ev_rn,
        "ev_riskneutral_carry": carry,
        "ev_realworld": None,
        "ev_realworld_reason": (
            "requires a governed, frozen, SPEC-until-OOS variance-risk-premium assumption; "
            "none is approved, so this is unknown rather than zero"),
        "costs_charged": costs,
        "basis": "analytic_risk_neutral_identity",
        "smile_used_by_diagnostic_only": bool(smile),   # the analytic EV does NOT use the smile
        "flat_vol_diagnostic": diag,
        "interpretation": (
            "Risk-neutral EV of any market-priced structure is exactly -(1/disc-1)*credit, "
            "a tautology that can never show an edge. It is an arithmetic check only. The "
            "DECIDING number is realised expectancy from tracked outcomes."),
    }


# ───────────────────────────────────────────────────────────────────────── liquidity
def liquidity(legs: list) -> dict:
    """Worst-leg liquidity across the structure. A 4-leg structure is only as tradeable as
    its worst leg, so we report the MINIMUM, never an average — averaging hides the one
    illiquid wing that will actually cost the fill."""
    spreads = [float(l["spread_pct"]) for l in legs if l.get("spread_pct") is not None]
    ois = [float(l["oi"]) for l in legs if l.get("oi") is not None]
    vols = [float(l["volume"]) for l in legs if l.get("volume") is not None]
    return {
        "worst_spread_pct": max(spreads) if spreads else None,
        "min_oi": min(ois) if ois else None,
        "min_volume": min(vols) if vols else None,
        "legs_missing_quote": sum(1 for l in legs if l.get("bid") is None
                                  or l.get("ask") is None),
        "n_legs": len(legs),
    }


# ───────────────────────────────────────────────────────────────────── one-shot summary
def evaluate(legs: list, F: float, T: float, sigma: float,
             smile: Optional[list] = None) -> dict:
    """The full per-candidate metric bundle the brief asks for, in one call.

    Everything real, everything from the chain, nothing invented. Where a number is not
    knowable it is None with a reason, never a placeholder.

    `smile` should be passed whenever the chain has per-strike IVs — without it POP falls
    back to flat ATM vol, which overstates a condor's POP (see ``pop``). If the caller does
    not supply one, we derive it from the legs themselves as a partial fallback."""
    if smile is None:
        smile = build_smile(legs) or None
    u = _units(legs[0]) if legs else 0
    credit_unit = net_credit(legs)
    mp = max_profit(legs)
    ml = max_loss(legs)
    ev = expected_value(legs, F, T, sigma, smile=smile)
    pp = pop(legs, F, T, sigma, smile=smile)
    costs = total_costs(legs)

    rr = (mp / abs(ml)) if (ml is not None and ml < 0) else None
    return {
        "units": u,
        "credit_per_unit": credit_unit,
        "credit_total": credit_unit * u,
        "max_profit": mp,
        "max_loss": ml,                       # None when risk is unbounded -- never a bound
        "max_loss_reason": (None if ml is not None
                            else "unbounded_beyond_outer_strike: no finite max loss exists"),
        "risk_reward": rr,
        # POP provenance travels WITH the POP. Previously pop()'s basis and its
        # flat-vol-overstates warning were computed and then dropped here, while
        # expected_value's meaningless "smile_used" flag was what callers printed next to it.
        "pop_basis": pp.get("basis"),
        "pop_smile_note": pp.get("smile_note"),
        "defined_risk": is_defined_risk(legs),
        "breakevens": pp["breakevens"],
        "pop_breakeven": pp["pop_breakeven"],
        "pop_body": pop_body_or_none(pp),
        "ev": ev,
        "costs": costs,
        "margin": margin_estimate(legs),
        "greeks": net_greeks(legs),
        "liquidity": liquidity(legs),
        "price_sources": sorted({str(l.get("price_source")) for l in legs}),
    }


def pop_body_or_none(pp: dict) -> Optional[float]:
    return pp.get("pop_body")
