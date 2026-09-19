"""Implied volatility — the one number on the Derivative tab this server COMPUTES.

Everything else the tab serves was reported by the exchange and captured by the D1/D2 workers. Kite does not
supply implied volatility at all, so it cannot be read; it can only be solved for. This module does that and
nothing else: no database, no I/O, no state. `derivatives.py` feeds it rows and labels the result `computed`.

What the model is
-----------------
Black-Scholes-Merton for a EUROPEAN option on a non-dividend-paying underlying, solved numerically from the
option's own last traded price, its strike, the underlying's spot at the SAME 15-minute reading, and the time
left to expiry. NSE index options and NSE single-stock options are both European-style, so the European
formula is the right one; the assumption that carries real weight is the missing dividend, which is stated in
`ASSUMPTIONS` rather than hidden.

Why a solver and not a formula
------------------------------
There is no closed form for volatility given a price. The price is strictly increasing in volatility, so a
bisection on [`IV_MIN`, `IV_MAX`] converges from either side and cannot overshoot the way Newton's method can
on a nearly worthless option. It is deterministic: the same inputs always give the same number.

When this module refuses
------------------------
A model output that cannot be trusted is `None` with a NAMED reason - never a guess and never a fallback
number. Every reason is in `REASONS` with the plain sentence the card prints. The reasons:

* `expiry_today`            - the option expires on the day of the reading; there is no time left to solve for.
* `no_time_value`           - the price is the intrinsic value; any volatility fits, so none is implied.
* `price_below_intrinsic`   - the last price is under the no-arbitrage floor, so the model has no root at all.
* `price_above_upper_bound` - the last price is at or above the no-arbitrage ceiling, same.
* `stale_last_trade`        - the last trade is older than one 15-minute reading, so the price is not current.
* `no_convergence`          - the bisection did not settle inside its tolerance.
* `outside_bracket`         - the price implies a volatility outside [`IV_MIN`, `IV_MAX`].
* `missing_price` / `missing_spot` / `missing_strike` / `missing_expiry` - an input the store did not carry.
* `non_positive_price`      - a last price of zero or less.

Nothing here predicts anything. An implied volatility is a restatement of a price that has already traded.
"""
from __future__ import annotations
import math

#: The model, named on every response so a reader can never take this for an exchange-reported figure.
MODEL='Black-Scholes-Merton (European, no dividend)'
METHOD='bisection on the option price, solved for the volatility'
#: ACT/365, from the reading to the expiry session's close.
DAY_COUNT='ACT/365'
#: NSE settles an F&O expiry on the closing price of the expiry session; the tab measures time to 15:30 IST on
#: that date. The CAS auction (15:30-15:35 from 2026-08-03) is not a traded bar and is not used here.
EXPIRY_TIME_IST='15:30'
SECONDS_PER_YEAR=365.0*24.0*3600.0

#: The rate the discounting uses. It is a CONSTANT IN THIS FILE, not a market quote: no rate feed is wired to
#: this server, and inventing a citation for a number nobody fetched would be worse than saying so. It is
#: stated on every response together with `RISK_FREE_RATE_SOURCE`, and it is overridable per call so a caller
#: that does have a rate can pass it.
RISK_FREE_RATE=0.065
RISK_FREE_RATE_SOURCE={
 'kind':'code constant',
 'where':'kanida_pilot/implied_vol.py RISK_FREE_RATE',
 'live_feed':False,
 'text':('The risk-free rate is a fixed constant in this server\'s code — 6.50% a year — not a rate this '
  'server fetched from anywhere. No rate feed is connected to it. For a front-month option the solved '
  'volatility barely moves with this number; for a far expiry it matters more, and the figure below says by '
  'how much.'),
}
#: How far the solved volatility moves if the rate is wrong by this much, reported per point so the reader can
#: see the size of the one assumption that has no source.
RATE_SENSITIVITY_STEP=0.01

#: The bracket the bisection searches. Outside it the price is not one this model can explain.
IV_MIN=0.0001   # 0.01% a year
IV_MAX=10.0     # 1000% a year
#: Stop when the bracket is this narrow, in volatility terms. 1e-7 is far finer than any price tick implies.
IV_TOLERANCE=1e-7
#: A hard ceiling on the loop, so a pathological input cannot spin. 200 halvings of a 10-wide bracket is
#: ~1e-59, so the tolerance is always hit long before this.
MAX_ITERATIONS=200
#: Prices equal to the no-arbitrage floor within this many rupees carry no time value.
PRICE_EPSILON=1e-6
#: A last trade older than one 15-minute reading means nothing changed hands in the bar the mark closes, so
#: the "last price" is not this reading's price.
STALE_SECONDS=900

#: What may be said about a reading whose implied volatility could not be solved. One entry per reason, and a
#: point ALWAYS carries one of these when its `iv` is null - a null with no reason never leaves this module.
REASONS={
 'expiry_today':'This option expires today, so there is no time left for a volatility to be implied from.',
 'no_time_value':'The last price is the option\'s intrinsic value, so no volatility is implied by it.',
 'price_below_intrinsic':'The last price is below the option\'s intrinsic value, so the model has no solution.',
 'price_above_upper_bound':'The last price is at or above the most the option can be worth, so the model has no solution.',
 'stale_last_trade':'The last trade is older than one 15-minute reading, so this price is not the reading\'s price.',
 'no_convergence':'The solver did not settle, so no volatility is reported for this reading.',
 'outside_bracket':f'The price implies a volatility outside {IV_MIN:.2%}-{IV_MAX:.0%} a year, which this model does not report.',
 'missing_price':'No last price was captured for this contract at this reading.',
 'missing_spot':'No spot price was captured for this underlying at this reading.',
 'missing_strike':'No strike is stored for this contract.',
 'missing_expiry':'No expiry date is stored for this contract.',
 'non_positive_price':'The captured last price is zero or negative, so nothing can be solved from it.',
 'unknown_option_type':'This contract is not a listed call or put, so no option model applies to it.',
 #: Not a failure of the maths: the store simply holds no row for this contract at this reading. It keeps its
 #: slot in the series with no value, so the hole is never closed up.
 'no_reading':'This contract has no row at this 15-min reading.',
}
#: Every assumption, listed. A reader who disagrees with one can see exactly which number it changes.
ASSUMPTIONS={
 'style':'European exercise — NSE index and single-stock options are both European-style.',
 'dividend':'No dividend is subtracted. On a single stock that goes ex-dividend before expiry, this solves slightly high on calls and slightly low on puts.',
 'rate':RISK_FREE_RATE_SOURCE['text'],
 'day_count':f'{DAY_COUNT}, measured from the 15-minute reading to {EXPIRY_TIME_IST} IST on the expiry date.',
 'price':'The option\'s own last traded price at that reading, as the exchange reported it — never a mid, never a model price.',
 'spot':'The underlying\'s spot at the SAME reading. The futures price is not used.',
 'american_early_exercise':'Not modelled, because these contracts have none.',
}
#: The one sentence the card prints beside any implied-volatility figure.
COMPUTED_TEXT=('Implied volatility is COMPUTED here, not reported by the exchange. It is solved from this '
 'option\'s own last traded price with a '+MODEL+' model. Every other number on this tab is a figure the '
 'exchange reported.')


def _norm_cdf(x):
 """Standard normal CDF. `math.erf` is exact to double precision; no table, no approximation."""
 return 0.5*(1.0+math.erf(x/math.sqrt(2.0)))


def price_bs(spot,strike,years,rate,sigma,option_type):
 """The Black-Scholes price of one European option. Used by the solver and by the sensitivity report."""
 if sigma<=0 or years<=0:
  discounted=strike*math.exp(-rate*years)
  return max(0.0,spot-discounted) if option_type=='CE' else max(0.0,discounted-spot)
 root=sigma*math.sqrt(years)
 d1=(math.log(spot/strike)+(rate+0.5*sigma*sigma)*years)/root
 d2=d1-root
 discounted=strike*math.exp(-rate*years)
 if option_type=='CE':return spot*_norm_cdf(d1)-discounted*_norm_cdf(d2)
 return discounted*_norm_cdf(-d2)-spot*_norm_cdf(-d1)


def bounds(spot,strike,years,rate,option_type):
 """(floor, ceiling) — the prices between which a European option must trade for a volatility to exist."""
 discounted=strike*math.exp(-rate*years)
 if option_type=='CE':return max(0.0,spot-discounted),spot
 return max(0.0,discounted-spot),discounted


def years_to_expiry(at,expiry_moment):
 """ACT/365 years between a reading and the expiry moment. Negative or zero when the moment has passed."""
 if at is None or expiry_moment is None:return None
 return (expiry_moment-at).total_seconds()/SECONDS_PER_YEAR


def solve(price,spot,strike,years,option_type,rate=RISK_FREE_RATE,days_to_expiry=None,
  seconds_since_last_trade=None,sensitivity=True):
 """The implied volatility of one option at one reading, or None with the reason it could not be solved.

 Returns `{'iv': float|None, 'reason': str|None, ...}`. `iv` is an annualised volatility as a fraction
 (0.18 = 18% a year). `reason` is a key of REASONS and is set whenever `iv` is None — never the other way
 round, so a caller can always print WHY a reading is blank.
 """
 out={'iv':None,'reason':None,'iv_pct':None,'years':None,'intrinsic':None,'time_value':None,
  'iterations':0,'rate':rate,'rate_sensitivity':None,'bounds':None}
 kind=str(option_type or '').strip().upper()
 if kind not in ('CE','PE'):return {**out,'reason':'unknown_option_type'}
 if strike is None or strike<=0:return {**out,'reason':'missing_strike'}
 if spot is None or spot<=0:return {**out,'reason':'missing_spot'}
 if years is None:return {**out,'reason':'missing_expiry'}
 out['years']=years
 # The staleness gate runs BEFORE the maths: a price that is not this reading's price cannot be trusted
 # however well it solves.
 if seconds_since_last_trade is not None and seconds_since_last_trade>STALE_SECONDS:
  return {**out,'reason':'stale_last_trade'}
 if days_to_expiry is not None and days_to_expiry<=0:return {**out,'reason':'expiry_today'}
 if years<=0:return {**out,'reason':'expiry_today'}
 if price is None:return {**out,'reason':'missing_price'}
 if price<=0:return {**out,'reason':'non_positive_price'}
 low_bound,high_bound=bounds(spot,strike,years,rate,kind)
 out['bounds']=[low_bound,high_bound]
 out['intrinsic']=low_bound
 out['time_value']=price-low_bound
 if price<low_bound-PRICE_EPSILON:return {**out,'reason':'price_below_intrinsic'}
 if price<=low_bound+PRICE_EPSILON:return {**out,'reason':'no_time_value'}
 if price>=high_bound-PRICE_EPSILON:return {**out,'reason':'price_above_upper_bound'}
 # The price is strictly increasing in sigma between the two bounds, so if it sits outside the bracket's own
 # price range the answer is a volatility this module does not report, not a failure of the search.
 if price<=price_bs(spot,strike,years,rate,IV_MIN,kind) or price>=price_bs(spot,strike,years,rate,IV_MAX,kind):
  return {**out,'reason':'outside_bracket'}
 low,high=IV_MIN,IV_MAX
 iterations=0
 while high-low>IV_TOLERANCE and iterations<MAX_ITERATIONS:
  iterations+=1
  mid=0.5*(low+high)
  if price_bs(spot,strike,years,rate,mid,kind)<price:low=mid
  else:high=mid
 out['iterations']=iterations
 if high-low>IV_TOLERANCE:return {**out,'reason':'no_convergence'}
 sigma=0.5*(low+high)
 if not (IV_MIN<sigma<IV_MAX):return {**out,'reason':'outside_bracket'}
 out['iv']=sigma
 out['iv_pct']=sigma*100.0
 # The rate is the one input with no source, so its weight is REPORTED rather than assumed away: this is the
 # same solve run once more at a rate one percentage point higher. `sensitivity=False` stops the recursion.
 if sensitivity:
  shifted=solve(price,spot,strike,years,kind,rate=rate+RATE_SENSITIVITY_STEP,sensitivity=False)
  if shifted.get('iv') is not None:
   out['rate_sensitivity']={'rate_step':RATE_SENSITIVITY_STEP,'iv_change':shifted['iv']-sigma,
    'iv_change_pct_points':(shifted['iv']-sigma)*100.0}
 return out
