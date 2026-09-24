"""Implied volatility, delta and gamma — COMPUTED, never read, and labelled so wherever they appear.

IV is `implied_vol.solve` itself, called exactly the way the Derivative tab's IV route calls it: the option's own
last traded price, the spot at the SAME reading, strike, time to 15:30 IST on expiry (ACT/365), the stored
days-to-expiry, and the staleness gate on the contract's last trade time. Every refusal keeps its named reason
(`implied_vol.REASONS`): a contract that cannot be solved at a reading has no IV there, not a guess.

Delta and gamma are the same Black-Scholes-Merton model at the solved volatility (European, no dividend, the
module's stated constant rate). No other model and no other input.

Solved values are cached in var/screener.db, keyed (instrument_token, reading): a reading's inputs never change
once captured, so a value is solved once and read thereafter.
"""
from __future__ import annotations
import math
from datetime import datetime
from .. import implied_vol as IV
from .data import isnan

STAMP='%Y-%m-%d %H:%M:%S'


def _stamp(text):
 if not text:return None
 for shape in (STAMP,'%Y-%m-%d %H:%M','%Y-%m-%dT%H:%M:%S'):
  try:return datetime.strptime(str(text)[:19],shape)
  except ValueError:continue
 return None


def greeks(spot,strike,years,sigma,option_type,rate=IV.RISK_FREE_RATE):
 """(delta, gamma) of a European option under BSM, no dividend."""
 root=sigma*math.sqrt(years)
 d1=(math.log(spot/strike)+(rate+0.5*sigma*sigma)*years)/root
 nd1=IV._norm_cdf(d1)
 delta=nd1 if option_type=='CE' else nd1-1.0
 gamma=math.exp(-0.5*d1*d1)/math.sqrt(2*math.pi)/(spot*root)
 return delta,gamma


class Greeks:
 """{(session, token, reading index): (iv_pct, reason, delta, gamma)} for one session, backed by the screener store."""
 def __init__(self,store,cache):
  self.store,self.cache=store,cache
  self.mem={}
  self.loaded=set()

 def value(self,session,token,i):
  return self.mem.get((session,token,i))

 def ensure(self,session,wanted):
  """Solve (or read back) every (token, reading) in `wanted`: {reading index: set(tokens)}."""
  for i,tokens in wanted.items():
   at=session.readings[i]
   if (session.session,i) not in self.loaded:
    for token,iv_pct,reason,delta,gamma in self.cache.iv_rows(at):
     self.mem[(session.session,token,i)]=(iv_pct,reason,delta,gamma)
    self.loaded.add((session.session,i))
   todo=[t for t in tokens if (session.session,t,i) not in self.mem]
   if not todo:continue
   trades={r[0]:r[1] for r in self.store.rows(
    'select instrument_token,last_trade_time from snapshots where captured_at=?',(at,))}
   mark=_stamp(at)
   out=[]
   for t in todo:
    c=session.contracts[t]
    price,spot=c.price[i],c.spot[i]
    moment=_stamp(f'{c.expiry} {IV.EXPIRY_TIME_IST}:00')
    years=IV.years_to_expiry(mark,moment) if (mark and moment) else None
    last=_stamp(trades.get(t))
    age=None if (last is None or mark is None) else max(0.0,(mark-last).total_seconds())
    solved=IV.solve(None if isnan(price) else price,None if isnan(spot) else spot,c.strike,years,c.type,
     days_to_expiry=c.dte,seconds_since_last_trade=age,sensitivity=False)
    sigma=solved.get('iv')
    delta=gamma=None
    if sigma is not None:delta,gamma=greeks(spot,c.strike,years,sigma,c.type)
    row=(solved.get('iv_pct'),solved.get('reason'),delta,gamma)
    self.mem[(session.session,t,i)]=row
    out.append((t,at,*row))
   self.cache.put_iv(out)

 def series(self,session,token,n,which):
  """The per-reading list of one COMPUTED figure (NaN where it could not be solved)."""
  k={'iv':0,'delta':2,'gamma':3}[which]
  out=[]
  for i in range(n):
   r=self.mem.get((session,token,i))
   v=None if r is None else r[k]
   if v is not None and which=='delta':v=abs(v)
   out.append(float('nan') if v is None else float(v))
  return out

 def reason(self,session,token,i):
  r=self.mem.get((session,token,i))
  return r[1] if r else None
