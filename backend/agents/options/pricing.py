"""
Options Agent · Black-76 pricing, implied vol and greeks.  PURE — zero I/O.

WHY THIS MODULE EXISTS
----------------------
Kite does not publish implied volatility or greeks (Step-0 finding, see
docs/agents/options_STEP0_FINDINGS.md — to be re-confirmed by the live probe). We therefore
compute them ourselves. Everything here is a deterministic function of numbers that come off
one chain snapshot, so it is unit-testable with no market data and no credentials.

WHY BLACK-76 AND NOT BLACK-SCHOLES
----------------------------------
Black-76 prices an option on a FORWARD/FUTURES price F rather than on spot S. We use the
same-expiry FUTURES price taken from the SAME snapshot as F. That is deliberate and it buys
three things:

  1. It removes the dividend-yield assumption entirely (the future already embeds it).
  2. It removes most of the interest-rate sensitivity: r enters only through the discount
     factor e^(-rT), which is common to calls and puts and nearly cancels in a spread.
     We have NO risk-free-rate source in this repo, so an assumption that barely matters is
     far safer than one that does.
  3. It lets us run a put-call parity consistency check on the snapshot itself
     (see ``parity_residual``), which catches stale or crossed quotes before they poison a
     credit or an EV number.

CONVENTIONS (stated once, obeyed everywhere)
--------------------------------------------
  F     forward/futures price for the option's expiry, same snapshot
  K     strike
  T     time to expiry in YEARS (use ``year_fraction``)
  r     risk-free rate, annualised, continuously compounded. A frozen governed constant —
        see R_DEFAULT. Never a fabricated daily series.
  sigma annualised implied volatility as a DECIMAL (0.14 == 14%), never a percentage.
  right "CE" (call) or "PE" (put).

  vega  is returned per 1.00 of vol (i.e. +100 vol points). Divide by 100 for "per 1% vol".
  theta is returned per YEAR. Divide by 365 for "per calendar day".
  Both are labelled in ``greeks()``'s output keys so a caller cannot mix them up silently.

HONESTY NOTE
------------
Nothing here produces an "edge". Black-76 is the market's own arithmetic: it translates a
price into an IV and a set of sensitivities. The risk-neutral expected value of a fairly
priced structure is approximately MINUS the costs — that is a property of the model, not a
finding. Any positive expectancy must come from realised, tracked outcomes, never from here.
"""
from __future__ import annotations

import math
from typing import Optional

# ── governed constants (frozen; change only through governance, never ad hoc) ──────────
# Residual discounting rate. Under Black-76 with a real futures forward, r affects calls and
# puts through the SAME factor e^(-rT) and very nearly cancels inside a spread, so a
# mis-specification here is second-order. Sensitivity is reported by `rate_sensitivity()`
# rather than assumed away.
R_DEFAULT = 0.065          # ~India short-rate scale; governed constant, NOT a fitted value
DAYS_PER_YEAR = 365.0

# Numerical bounds for the implied-vol solver.
_IV_LO = 1e-4              # 0.01% vol
_IV_HI = 5.0               # 500% vol
_IV_TOL = 1e-8
_IV_MAX_ITER = 100


# ─────────────────────────────────────────────────────────── normal distribution helpers
def _pdf(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _cdf(x: float) -> float:
    """Standard normal CDF via the error function (exact to double precision)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def year_fraction(days: float) -> float:
    """Calendar days -> years. Calendar (not trading) days, because option time decay runs
    over the weekend too; the convention is stated so it is never silently changed."""
    return max(float(days), 0.0) / DAYS_PER_YEAR


def _d1_d2(F: float, K: float, T: float, sigma: float):
    v = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / v
    return d1, d1 - v


def _norm_right(right: str) -> str:
    r = str(right).upper().strip()
    if r in ("CE", "C", "CALL"):
        return "CE"
    if r in ("PE", "P", "PUT"):
        return "PE"
    raise ValueError("right must be CE or PE, got %r" % (right,))


# ────────────────────────────────────────────────────────────────────────────── pricing
def price(F: float, K: float, T: float, sigma: float, right: str,
          r: float = R_DEFAULT) -> float:
    """Black-76 price of a European option on a forward F.

    Degenerate inputs collapse to the discounted intrinsic value rather than raising —
    T<=0 (expiry) and sigma<=0 (no uncertainty) are both real states a snapshot can be in,
    and returning intrinsic is the mathematically correct limit, not a fudge."""
    right = _norm_right(right)
    F, K, T, sigma = float(F), float(K), float(T), float(sigma)
    if F <= 0.0 or K <= 0.0:
        raise ValueError("F and K must be positive (F=%r K=%r)" % (F, K))
    disc = math.exp(-r * T)
    if T <= 0.0 or sigma <= 0.0:
        intrinsic = (F - K) if right == "CE" else (K - F)
        return disc * max(intrinsic, 0.0)
    d1, d2 = _d1_d2(F, K, T, sigma)
    if right == "CE":
        return disc * (F * _cdf(d1) - K * _cdf(d2))
    return disc * (K * _cdf(-d2) - F * _cdf(-d1))


def price_bounds(F: float, K: float, T: float, right: str,
                 r: float = R_DEFAULT) -> tuple:
    """(lo, hi) no-arbitrage bounds for a Black-76 option price.

    A quote outside these bounds has no implied vol — it is a bad/stale/crossed quote, and
    `implied_vol` reports that honestly instead of returning a made-up number."""
    right = _norm_right(right)
    disc = math.exp(-r * T)
    if right == "CE":
        return disc * max(F - K, 0.0), disc * F
    return disc * max(K - F, 0.0), disc * K


# ──────────────────────────────────────────────────────────────────────── implied vol
def implied_vol(option_price: float, F: float, K: float, T: float, right: str,
                r: float = R_DEFAULT) -> Optional[float]:
    """Solve Black-76 for sigma. Returns None when there is no solution.

    None is a first-class, meaningful answer here: an expired option, a zero/negative quote,
    or a price outside the no-arbitrage bounds genuinely HAS no implied vol. Callers must
    treat None as "unknown", never coerce it to 0.0 — a zero vol would silently claim
    certainty about a contract we could not price.

    Method: bisection on a strictly increasing function (price is monotone in sigma), which
    cannot diverge the way Newton can at the wings where vega collapses to ~0.
    """
    right = _norm_right(right)
    option_price, F, K, T = float(option_price), float(F), float(K), float(T)
    if T <= 0.0 or option_price <= 0.0 or F <= 0.0 or K <= 0.0:
        return None

    lo_b, hi_b = price_bounds(F, K, T, right, r)
    # Tiny tolerance so a quote exactly on the intrinsic bound (deep ITM, no time value)
    # reads as "no time value" rather than as a bad quote.
    if option_price <= lo_b + 1e-12 or option_price >= hi_b - 1e-12:
        return None

    lo, hi = _IV_LO, _IV_HI
    p_lo = price(F, K, T, lo, right, r)
    p_hi = price(F, K, T, hi, right, r)
    if not (p_lo <= option_price <= p_hi):
        return None                      # outside what the solver's vol range can produce

    for _ in range(_IV_MAX_ITER):
        mid = 0.5 * (lo + hi)
        pm = price(F, K, T, mid, right, r)
        if abs(pm - option_price) < _IV_TOL or (hi - lo) < _IV_TOL:
            return mid
        if pm < option_price:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


# ────────────────────────────────────────────────────────────────────────────── greeks
def greeks(F: float, K: float, T: float, sigma: float, right: str,
           r: float = R_DEFAULT) -> dict:
    """Black-76 greeks for ONE option, per 1 unit of the underlying (not per lot).

    Units are in the key names so they can never be silently mixed:
      delta                 per 1.00 move in F
      gamma                 d(delta)/dF
      vega_per_1_00_vol     per 1.00 (=100 vol points) change in sigma
      vega_per_1pct_vol     per 0.01 change in sigma  (the number humans read)
      theta_per_year        per 1.0 year
      theta_per_day         per 1 calendar day        (the number humans read)

    Sign convention: these are the greeks of a LONG unit. A short leg negates them; that
    negation happens in payoff.py where lots and lot_size are also applied, so it is done in
    exactly one place.
    """
    right = _norm_right(right)
    F, K, T, sigma = float(F), float(K), float(T), float(sigma)
    disc = math.exp(-r * T)

    if T <= 0.0 or sigma <= 0.0:
        # At/after expiry the payoff is piecewise-linear: delta is 0 or +/-1, everything
        # else is 0. Reported honestly rather than dividing by zero.
        itm = (F > K) if right == "CE" else (F < K)
        d = (1.0 if right == "CE" else -1.0) if itm else 0.0
        return {"delta": disc * d, "gamma": 0.0,
                "vega_per_1_00_vol": 0.0, "vega_per_1pct_vol": 0.0,
                "theta_per_year": 0.0, "theta_per_day": 0.0}

    d1, d2 = _d1_d2(F, K, T, sigma)
    sqrtT = math.sqrt(T)
    nd1 = _pdf(d1)

    if right == "CE":
        delta = disc * _cdf(d1)
        theta = (-F * disc * nd1 * sigma / (2.0 * sqrtT)
                 - r * K * disc * _cdf(d2) + r * F * disc * _cdf(d1))
    else:
        delta = -disc * _cdf(-d1)
        theta = (-F * disc * nd1 * sigma / (2.0 * sqrtT)
                 + r * K * disc * _cdf(-d2) - r * F * disc * _cdf(-d1))

    gamma = disc * nd1 / (F * sigma * sqrtT)
    vega = F * disc * nd1 * sqrtT

    return {"delta": delta, "gamma": gamma,
            "vega_per_1_00_vol": vega, "vega_per_1pct_vol": vega / 100.0,
            "theta_per_year": theta, "theta_per_day": theta / DAYS_PER_YEAR}


# ─────────────────────────────────────────────────────── terminal distribution (for EV/POP)
def lognormal_terminal_pdf(F: float, T: float, sigma: float, points):
    """Risk-neutral density of the terminal forward F_T over `points`.

    Under Black-76, F_T is lognormal with median F and log-variance sigma^2*T. This is THE
    distribution the market is quoting — using it for POP/EV means our probabilities are the
    market's own, not our opinion.

    READ THIS BEFORE USING IT FOR EV: integrating any fairly-priced structure's payoff
    against this density returns approximately ZERO before costs, and approximately MINUS
    COSTS after them. That is a theorem, not a backtest. It is a validity CHECK on our math.
    A positive expectancy can only ever come from realised tracked outcomes.
    """
    F, T, sigma = float(F), float(T), float(sigma)
    if T <= 0.0 or sigma <= 0.0 or F <= 0.0:
        raise ValueError("lognormal_terminal_pdf needs F>0, T>0, sigma>0")
    v = sigma * math.sqrt(T)
    mu = math.log(F) - 0.5 * v * v          # so that E[F_T] == F
    out = []
    for S in points:
        S = float(S)
        if S <= 0.0:
            out.append(0.0)
            continue
        z = (math.log(S) - mu) / v
        out.append(_pdf(z) / (S * v))
    return out


def prob_between(F: float, T: float, sigma: float,
                 lo: Optional[float], hi: Optional[float]) -> float:
    """Risk-neutral P(lo <= F_T <= hi) in closed form (no numerical integration).

    This is the POP of a short strangle/condor body: the probability the underlying finishes
    between the two short strikes. `None` means unbounded on that side."""
    F, T, sigma = float(F), float(T), float(sigma)
    if T <= 0.0 or sigma <= 0.0:
        inside = ((lo is None or F >= lo) and (hi is None or F <= hi))
        return 1.0 if inside else 0.0
    v = sigma * math.sqrt(T)
    mu = math.log(F) - 0.5 * v * v

    def _cum(x):
        if x is None:
            return None
        if float(x) <= 0.0:
            return 0.0
        return _cdf((math.log(float(x)) - mu) / v)

    c_lo, c_hi = _cum(lo), _cum(hi)
    return (1.0 if c_hi is None else c_hi) - (0.0 if c_lo is None else c_lo)


def prob_itm(F: float, K: float, T: float, sigma: float, right: str) -> float:
    """Risk-neutral probability the option finishes in the money, i.e. N(d2) for a call.

    NOT the same as delta, though they are close for near-ATM options. Kept distinct
    because conflating them is a classic way to overstate an iron condor's POP."""
    right = _norm_right(right)
    if T <= 0.0 or sigma <= 0.0:
        return 1.0 if ((F > K) if right == "CE" else (F < K)) else 0.0
    _, d2 = _d1_d2(float(F), float(K), float(T), float(sigma))
    return _cdf(d2) if right == "CE" else _cdf(-d2)


# ───────────────────────────────────────────────────────────── snapshot sanity checks
def parity_residual(call_price: float, put_price: float, F: float, K: float, T: float,
                    r: float = R_DEFAULT) -> float:
    """Put-call parity residual: (C - P) - e^(-rT)(F - K). Should be ~0 on a clean snapshot.

    A large residual means the snapshot is internally inconsistent — stale leg, crossed
    quote, or a future/option timestamp mismatch. That must abort a snapshot write rather
    than silently flow into a credit and an EV."""
    return (float(call_price) - float(put_price)) - math.exp(-r * T) * (float(F) - float(K))


def rate_sensitivity(F: float, K: float, T: float, sigma: float, right: str,
                     bump: float = 0.01) -> float:
    """How much the price moves for a `bump` change in r — so the governed R_DEFAULT can be
    shown to be second-order rather than merely asserted to be."""
    return (price(F, K, T, sigma, right, R_DEFAULT + bump)
            - price(F, K, T, sigma, right, R_DEFAULT))
