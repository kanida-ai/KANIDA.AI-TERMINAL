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
    """Terminal P&L in rupees for the whole position."""
    u = _units(legs[0]) if legs else 0
    gross = payoff_at(legs, S, include_costs=False) * u
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


def max_loss(legs: list, include_costs: bool = True) -> float:
    """Max loss in rupees, returned as a NEGATIVE number. Exact — evaluated at every kink.

    For a defined-risk structure this is finite by construction. If it comes back
    unbounded-looking (the outer evaluation point being the minimum), the structure is not
    actually defined-risk and ``is_defined_risk`` will say so."""
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

    # Slippage: we cross a governed fraction of this leg's own quoted spread. A leg with no
    # usable spread contributes 0 here and is instead caught by the liquidity gate — we do
    # NOT invent a spread for it.
    bid, ask = leg.get("bid"), leg.get("ask")
    spread = (float(ask) - float(bid)) if (bid is not None and ask is not None) else 0.0
    slippage = max(0.0, spread) * SLIPPAGE_SPREAD_FRACTION * units

    return {"brokerage": brokerage, "stt": stt, "exchange_txn": exch, "sebi": sebi,
            "stamp_duty": stamp, "gst": gst, "slippage": slippage,
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
    out["costs_basis"] = "governed_table"      # never "measured" until calibrate_costs runs
    out["per_leg"] = entry
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
    ml = abs(max_loss(legs, include_costs=False))
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
def pop(legs: list, F: float, T: float, sigma: float, include_costs: bool = True) -> dict:
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
    if len(bes) == 2:
        pop_be = P.prob_between(F, T, sigma, bes[0], bes[1])
    elif len(bes) == 1:
        # one-sided structure: profitable on whichever side currently pays
        mid_hi = payoff_money_at(legs, bes[0] * 1.05, include_costs)
        pop_be = (P.prob_between(F, T, sigma, bes[0], None) if mid_hi > 0
                  else P.prob_between(F, T, sigma, None, bes[0]))
    else:
        pop_be = 1.0 if payoff_money_at(legs, F, include_costs) > 0 else 0.0

    shorts = sorted(float(l["strike"]) for l in legs if _sign(l["action"]) < 0)
    pop_bd = (P.prob_between(F, T, sigma, shorts[0], shorts[-1])
              if len(shorts) >= 2 else None)
    return {"pop_breakeven": pop_be, "pop_body": pop_bd, "breakevens": bes}


def expected_value(legs: list, F: float, T: float, sigma: float,
                   include_costs: bool = True, grid: int = 20001) -> dict:
    """Risk-neutral expected P&L in rupees, plus the honest labelling around it.

    Method: the payoff is bounded and piecewise-linear, so we integrate it on a dense grid
    across the strike region and add the two CONSTANT tails in closed form via
    ``prob_between``. No tail mass is dropped — dropping it is a common way to flatter a
    short-premium structure, because the tails are exactly where it loses.
    """
    ks = sorted({float(l["strike"]) for l in legs})
    lo, hi = ks[0] * 0.60, ks[-1] * 1.40

    step = (hi - lo) / (grid - 1)
    xs = [lo + i * step for i in range(grid)]
    dens = P.lognormal_terminal_pdf(F, T, sigma, xs)
    body = sum(payoff_money_at(legs, S, include_costs) * d
               for S, d in zip(xs, dens)) * step

    # Constant-payoff tails, integrated exactly rather than truncated.
    p_lo = P.prob_between(F, T, sigma, None, lo)
    p_hi = P.prob_between(F, T, sigma, hi, None)
    tails = (payoff_money_at(legs, lo * 0.5, include_costs) * p_lo
             + payoff_money_at(legs, hi * 1.5, include_costs) * p_hi)

    ev_rn = body + tails
    costs = total_costs(legs)["total"] if include_costs else 0.0
    return {
        "ev_riskneutral": ev_rn,
        "ev_realworld": None,
        "ev_realworld_reason": (
            "requires a governed, frozen, SPEC-until-OOS variance-risk-premium assumption; "
            "none is approved, so this is unknown rather than zero"),
        "costs_charged": costs,
        "tail_mass_lo": p_lo,
        "tail_mass_hi": p_hi,
        "basis": "risk_neutral_implied_density",
        "interpretation": (
            "A fairly-priced structure integrates to about -costs under the market's own "
            "density. This is a validity CHECK on the arithmetic, NOT an edge. The deciding "
            "number is realised expectancy from tracked outcomes."),
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
def evaluate(legs: list, F: float, T: float, sigma: float) -> dict:
    """The full per-candidate metric bundle the brief asks for, in one call.

    Everything real, everything from the chain, nothing invented. Where a number is not
    knowable it is None with a reason, never a placeholder."""
    u = _units(legs[0]) if legs else 0
    credit_unit = net_credit(legs)
    mp = max_profit(legs)
    ml = max_loss(legs)
    ev = expected_value(legs, F, T, sigma)
    pp = pop(legs, F, T, sigma)
    costs = total_costs(legs)

    rr = (mp / abs(ml)) if ml < 0 else None
    return {
        "units": u,
        "credit_per_unit": credit_unit,
        "credit_total": credit_unit * u,
        "max_profit": mp,
        "max_loss": ml,
        "risk_reward": rr,
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
