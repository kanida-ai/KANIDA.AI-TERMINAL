"""Strategy analytics — payoff, extrema, breakevens, scenario value, Greeks, model POP. PURE: no I/O, no state.

Conventions (docs/strategy_builder_study/BUILD_PLAN_MERGED.md §1, blueprint B §7):
  * Signed units q = side_sign x lots x lot_size (buy +, sell -). The lot size is the contract's own, read from the
    instrument master with the leg; it is never a constant here.
  * Expiry P&L at terminal spot S is the sum of q x (intrinsic(S) - entry). It is piecewise linear in S with kinks at
    the strikes, so maxima, minima and breakevens are computed EXACTLY on the segments and tails - a plotted range
    can never make an unbounded loss look bounded.
  * Scenario (target-date) values are Black-Scholes-Merton, European, no dividend - the same model and constant
    rate as kanida_pilot/implied_vol.py, which is reused, not re-implemented.
  * Every metric is an envelope {status, value, unit, basis, reason?}. A metric that cannot be computed is
    'unavailable' or 'unsupported' with a reason - never a zero.
  * Multi-expiry (calendars, diagonals - slice 12): the "expiry" payoff is the value AT THE NEAR EXPIRY - near legs at
    intrinsic, later legs repriced by BSM at their own market IV for their remaining time. That is a MODEL, labelled
    'model_at_near_expiry'; its extremes, breakevens and POP come from a fine grid, never a terminal formula. Each
    leg's Greeks use its own time to expiry.
"""
from __future__ import annotations
import math
from datetime import datetime,timedelta
from .. import implied_vol as IV

MODEL_VERSION='sb-analytics-v1'
RATE=IV.RISK_FREE_RATE
EXPIRY_CLOSE=(15,30)          # NSE F&O settles on the expiry session close, 15:30 IST
SECONDS_PER_YEAR=IV.SECONDS_PER_YEAR
MAX_LEGS=8


def ok(value,unit='INR',basis=None,**extra):
 out={'status':'available','value':value,'unit':unit}
 if basis:out['basis']=basis
 out.update(extra);return out
def na(reason,status='unavailable',**extra):return {'status':status,'reason':reason,**extra}


def expiry_moment(expiry):
 """The expiry session close as a naive IST datetime. `expiry` is the exchange-local date string YYYY-MM-DD."""
 d=datetime.strptime(str(expiry)[:10],'%Y-%m-%d')
 return d.replace(hour=EXPIRY_CLOSE[0],minute=EXPIRY_CLOSE[1])


def parse_ist(text):
 if not text:return None
 for shape in ('%Y-%m-%d %H:%M:%S','%Y-%m-%d %H:%M','%Y-%m-%dT%H:%M:%S','%Y-%m-%dT%H:%M','%Y-%m-%d'):
  try:return datetime.strptime(str(text)[:19],shape)
  except ValueError:continue
 return None


def years_between(at,expiry):
 return (expiry_moment(expiry)-at).total_seconds()/SECONDS_PER_YEAR


def units(leg):
 return (1 if leg['side']=='B' else -1)*int(leg['lots'])*int(leg['lot_size'])


def intrinsic(kind,strike,s):
 return max(s-strike,0.0) if kind=='CE' else max(strike-s,0.0)


# --- Black-Scholes-Merton (European, no dividend) -------------------------------------------------------------
def _pdf(x):return math.exp(-0.5*x*x)/math.sqrt(2*math.pi)
def bs_price(s,k,t,sigma,kind,r=RATE):
 """Two different boundaries, never merged (E05): at/after settlement (t<=0) the option is worth its intrinsic
 value against the spot; with time left but zero volatility the outcome is deterministic and the value is the
 DISCOUNTED-strike payoff, (S - K e^{-rT})+ for a call and (K e^{-rT} - S)+ for a put."""
 if t<=0:return intrinsic(kind,k,s)
 if sigma<=0:
  disc=k*math.exp(-r*t)
  return max(s-disc,0.0) if kind=='CE' else max(disc-s,0.0)
 return IV.price_bs(s,k,t,r,sigma,kind)
def bs_greeks(s,k,t,sigma,kind,r=RATE):
 """Per ONE option unit: delta (per 1 point), gamma, theta (INR per calendar day), vega (INR per 1 IV point).

 t<=0 is settlement (intrinsic; delta is the exercise indicator, a convention - exactly at the strike it is not a
 smooth sensitivity). sigma<=0 with t>0 is the deterministic discounted-strike value: moneyness is against
 K e^{-rT}, gamma and vega are 0, and theta is the carry on the discounted strike."""
 if t<=0 or s<=0:
  itm=(s>k) if kind=='CE' else (s<k)
  return {'delta':(1.0 if itm else 0.0) if kind=='CE' else (-1.0 if itm else 0.0),'gamma':0.0,'theta':0.0,'vega':0.0}
 if sigma<=0:
  disc=k*math.exp(-r*t)
  itm=(s>disc) if kind=='CE' else (s<disc)
  if not itm:return {'delta':0.0,'gamma':0.0,'theta':0.0,'vega':0.0}
  # d/dt of (S - K e^{-rt}) is r K e^{-rt}; theta is value change per calendar day as time PASSES (t shrinks).
  carry=r*disc/365.0
  return ({'delta':1.0,'gamma':0.0,'theta':-carry,'vega':0.0} if kind=='CE'
   else {'delta':-1.0,'gamma':0.0,'theta':carry,'vega':0.0})
 root=sigma*math.sqrt(t)
 d1=(math.log(s/k)+(r+0.5*sigma*sigma)*t)/root;d2=d1-root
 n1=IV._norm_cdf(d1);pdf=_pdf(d1);disc=math.exp(-r*t)
 if kind=='CE':
  delta=n1;theta=(-s*pdf*sigma/(2*math.sqrt(t))-r*k*disc*IV._norm_cdf(d2))
 else:
  delta=n1-1.0;theta=(-s*pdf*sigma/(2*math.sqrt(t))+r*k*disc*IV._norm_cdf(-d2))
 return {'delta':delta,'gamma':pdf/(s*root),'theta':theta/365.0,'vega':s*pdf*math.sqrt(t)/100.0}


# --- the expiry payoff: exact, piecewise linear ---------------------------------------------------------------
FREEZE_UNITS={'NIFTY':1800,'BANKNIFTY':900,'FINNIFTY':1800,'MIDCPNIFTY':2800,'NIFTYNXT50':600}


def insights(active,spot,t_now,sigma,out):
 """Plain-language risk warnings for the structure as it stands (Blueprint A insights list). Each: key, level, text,
 and leg_id/fix when a one-tap fix exists (slice 12: add a hedge, snap to tick, change expiry)."""
 res=[];cur={}
 def add(k,lv,tx,**x):res.append({'key':k,'level':lv,'text':tx,**cur,**x})
 shorts=[l for l in active if l['side']=='S']
 for l in active:
  bid,ask=l.get('bid'),l.get('ask')
  lab=f"{int(l['strike']) if float(l['strike']).is_integer() else l['strike']} {l['type']}"
  if bid and ask and (ask+bid)>0 and (ask-bid)/((ask+bid)/2)>0.05:
   add('wide_spread','warn',f"{lab}: the bid-ask spread is {(ask-bid)/((ask+bid)/2)*100:.0f}% of the price - fills will cost more than the mid.")
  elif l.get('price_basis')!='manual' and not (bid and ask) and l.get('ltp') is not None:
   add('no_quote','info',f"{lab}: no live bid/ask - priced at the last trade, which may be stale for an illiquid strike.")
  if l.get('off_tick') is not None:
   add('off_tick','warn',f"{lab}: the typed price {l['price']} is not on the {l.get('tick') or 0.05} tick - an exchange order would be rejected; the nearest valid price is {l['off_tick']}.",
    fix={'action':'snap_tick','leg_id':l['id'],'price':l['off_tick'],'label':f"Use {l['off_tick']}"})
  if 'stale' in (l.get('flags') or []) or 'no_trade' in (l.get('flags') or []):
   add('illiquid','warn',f"{lab}: the price is stale or the strike did not trade - it may be illiquid.")
 for l in shorts:
  lab=f"{int(l['strike']) if float(l['strike']).is_integer() else l['strike']} {l['type']}"
  itm=(spot>l['strike']) if l['type']=='CE' else (spot<l['strike'])
  if itm:add('short_itm','warn',f"Short {lab} is in the money: it carries its full intrinsic loss now and is likely to be exercised at expiry.",
   fix={'action':'open_adjust','leg_id':l['id'],'label':'See adjustments'})
  if sigma and t_now>0:
   z=math.log(l['strike']/spot)/(sigma*math.sqrt(t_now))
   if (l['type']=='CE' and z>2) or (l['type']=='PE' and z<-2):
    add('far_short','info',f"Short {lab} is beyond 2 standard deviations: a small credit against a rare but large move.")
 if shorts and t_now*365<1:
  add('expiry_gamma','warn','Expiry day with short options: gamma is at its highest, so small moves swing the P&L sharply.',fix={'action':'change_expiry','label':'Choose a later expiry'})
 lot=next((int(l.get('lot_size') or 0) for l in active if l.get('lot_size')),0)
 for l in active:
  u=int(l['lots'])*int(l.get('lot_size') or lot or 0)
  for k,v in FREEZE_UNITS.items():
   if str(l.get('symbol') or '').startswith(k) and not str(l.get('symbol') or '').startswith(k+'NXT') and u>v:
    add('freeze','info',f"{int(l['strike'])} {l['type']}: {u} units exceeds the {v}-unit exchange freeze quantity - it will be sent as several orders.")
    break
 if (out.get('max_loss') or {}).get('unlimited'):
  naked=[k for k in ('CE','PE') if sum(int(l['lots'])*(1 if l['side']=='B' else -1) for l in active if l['type']==k)<0]
  add('unlimited','warn','Unlimited loss on at least one side: add a hedge leg to cap it.',fix={'action':'add_hedge','types':naked or ['CE'],'label':'Add a hedge'})
 seen=set();uniq=[]
 for r in res:
  if (r['key'],r['text']) not in seen:seen.add((r['key'],r['text']));uniq.append(r)
 return uniq


def expiry_pnl(legs,s):
 return sum(units(l)*(intrinsic(l['type'],l['strike'],s)-l['price']) for l in legs)


def _slopes(legs):
 """Slope of the expiry P&L above the highest strike and below the lowest."""
 right=sum(units(l) for l in legs if l['type']=='CE')
 left=-sum(units(l) for l in legs if l['type']=='PE')
 return left,right


def expiry_profile(legs,offset=0.0):
 """Max profit, max loss and breakevens of the expiry payoff less a constant `offset` (e.g. charges), exactly.
 Spot is bounded below by 0, unbounded above."""
 strikes=sorted({float(l['strike']) for l in legs})
 _left,right=_slopes(legs)
 points=[0.0]+strikes
 values=[expiry_pnl(legs,x)-offset for x in points]
 hi=max(values);lo=min(values)
 unlimited_profit=right>1e-9
 unlimited_loss=right<-1e-9
 # breakevens: roots on every segment [p_i, p_i+1] and on the right tail
 roots=[]
 for a,b,va,vb in zip(points,points[1:],values,values[1:]):
  if va==0 and (not roots or abs(roots[-1]-a)>1e-9):roots.append(a)
  if va*vb<0:roots.append(a+(b-a)*(-va)/(vb-va))
 last,vlast=points[-1],values[-1]
 if vlast==0 and (not roots or abs(roots[-1]-last)>1e-9) and last>0:roots.append(last)
 if right!=0 and vlast*right<0:roots.append(last-vlast/right)
 roots=[r for r in roots if r>0]
 return {'max_profit':None if unlimited_profit else hi,'max_loss':None if unlimited_loss else lo,
  'unlimited_profit':unlimited_profit,'unlimited_loss':unlimited_loss,'breakevens':sorted(set(round(r,2) for r in roots)),
  'slope_right':right}


# --- implied volatility per leg -------------------------------------------------------------------------------
def leg_iv(leg,spot,t):
 """The leg's MARKET IV: supplied with it (solved from the current mark), else solved from its mark here. The entry
 price is a cost basis and never sets the volatility (GTM audit P04). Only a leg with no market value at all
 (a pure hypothetical) falls back to its entry - and says so. Returns (sigma or None, source or reason)."""
 if leg.get('iv') is not None:return float(leg['iv']),leg.get('iv_source') or 'market'
 if leg.get('mark') is not None:
  got=IV.solve(float(leg['mark']),spot,leg['strike'],t,leg['type'],sensitivity=False)
  if got.get('iv') is not None:return got['iv'],'market_'+(leg.get('mark_basis') or 'mark')
  return None,got.get('reason') or 'unsolved'
 got=IV.solve(leg['price'],spot,leg['strike'],t,leg['type'],sensitivity=False)
 if got.get('iv') is not None:return got['iv'],'solved_from_entry'
 return None,got.get('reason') or 'unsolved'


# --- probability (model) --------------------------------------------------------------------------------------
def _lognormal_cdf(x,spot,sigma,t,r=RATE):
 if x<=0:return 0.0
 mu=math.log(spot)+(r-0.5*sigma*sigma)*t;sd=sigma*math.sqrt(t)
 return IV._norm_cdf((math.log(x)-mu)/sd)


def outcome_probabilities(legs,spot,sigma,t,profile):
 """Model probabilities (lognormal, drift r) of the EXPIRY outcome: profit, loss, and hitting the capped max profit /
 max loss (the flat extremes of a defined-risk payoff). Integrated over a fine grid of the terminal distribution."""
 if not sigma or t<=0:return None
 sd=sigma*math.sqrt(t);mu=math.log(spot)+(RATE-0.5*sigma*sigma)*t
 lo=math.exp(mu-7*sd);hi=math.exp(mu+7*sd);N=4000
 mp=profile.get('max_profit');ml=profile.get('max_loss')
 tol_p=abs(mp)*1e-6+0.5 if mp is not None else None;tol_l=abs(ml)*1e-6+0.5 if ml is not None else None
 pr=ls=pmax=pmin=0.0;prev=_lognormal_cdf(lo,spot,sigma,t)
 for i in range(1,N+1):
  x1=lo+(hi-lo)*i/N;c=_lognormal_cdf(x1,spot,sigma,t);w=c-prev;prev=c
  v=expiry_pnl(legs,x1-(hi-lo)/(2*N))
  if v>0:pr+=w
  elif v<0:ls+=w
  if mp is not None and abs(v-mp)<=tol_p:pmax+=w
  if ml is not None and abs(v-ml)<=tol_l:pmin+=w
 return {'profit':round(pr*100,1),'loss':round(ls*100,1),'max_profit':round(pmax*100,1) if mp is not None else None,
  'max_loss':round(pmin*100,1) if ml is not None else None}


def grid_probabilities(f,spot,sigma,t,profile):
 """outcome_probabilities for any payoff function f(S) at horizon t (used for the near-expiry model value)."""
 if not sigma or t<=0:return {'profit':None,'loss':None,'max_profit':None,'max_loss':None}
 sd=sigma*math.sqrt(t);mu=math.log(spot)+(RATE-0.5*sigma*sigma)*t
 lo=math.exp(mu-7*sd);hi=math.exp(mu+7*sd);N=2000
 mp=profile.get('max_profit');ml=profile.get('max_loss');pr=ls=pmax=pmin=0.0;prev=_lognormal_cdf(lo,spot,sigma,t)
 for i in range(1,N+1):
  x1=lo+(hi-lo)*i/N;c=_lognormal_cdf(x1,spot,sigma,t);w=c-prev;prev=c;v=f(x1-(hi-lo)/(2*N))
  if v>0:pr+=w
  elif v<0:ls+=w
  if mp is not None and abs(v-mp)<=abs(mp)*0.01+1:pmax+=w
  if ml is not None and abs(v-ml)<=abs(ml)*0.01+1:pmin+=w
 # a multi-expiry payoff peaks at a POINT (not a flat plateau), so "hits max profit/loss" has no meaning there
 return {'profit':round(pr*100,1),'loss':round(ls*100,1),'max_profit':None,'max_loss':None,'note':'Hitting the exact max profit/loss is not meaningful for a curved (multi-expiry) payoff.'}


def near_expiry_profile(legs,spot,sigma,t,f,n=1600):
 """Extremes and breakevens of the model value at the near expiry, on a grid from ~0 to far beyond +-6 SD; the tails'
 boundedness comes from the exact slopes (every call's delta -> 1 as S -> inf, every put's -> 0), not from the grid."""
 left,right=_slopes(legs)
 span=max(0.5,6*sigma*math.sqrt(max(t,1/365)))
 lo=max(0.01,spot*math.exp(-span));hi=spot*math.exp(span)
 # the strikes are kinks: on the grid, a piecewise-linear (same-expiry) payoff's extremes and roots are exact
 xs=sorted({0.01,*(lo+(hi-lo)*i/n for i in range(n+1)),*(float(l['strike']) for l in legs if 0.01<float(l['strike'])<hi)})
 vs=[f(x) for x in xs]
 roots=[]
 for (a,va),(b,vb) in zip(zip(xs,vs),zip(xs[1:],vs[1:])):
  if va==0:roots.append(a)
  elif (va<0)!=(vb<0):roots.append(a+(b-a)*(-va)/(vb-va))
 # the exact far-spot limit (quant C9): with a flat right slope the value keeps changing beyond any grid as the far
 # legs' time value vanishes; evaluate far out (every far call -> S - K e^-rt) so the extremes include the limit
 far=[f(spot*math.exp(span*m)) for m in (2,4,8)]
 vs_ext=vs+far
 return {'max_profit':None if right>1e-9 else max(vs_ext),'max_loss':None if right<-1e-9 else min(vs_ext),'unlimited_profit':right>1e-9,'unlimited_loss':right<-1e-9,
  'breakevens':sorted({round(r,2) for r in roots if r>0}),'slope_right':right}


def pop(legs,spot,sigma,t,profile):
 """P(expiry P&L > 0) under a lognormal at `sigma`, drift r. A MODEL value, labelled so by the caller."""
 if not sigma or t<=0:return None
 cuts=[0.0]+profile['breakevens']+[float('inf')]
 total=0.0
 for a,b in zip(cuts,cuts[1:]):
  mid=(a+b)/2 if b!=float('inf') else (a*1.05+1 if a>0 else spot)
  if expiry_pnl(legs,mid)>0:
   hi=1.0 if b==float('inf') else _lognormal_cdf(b,spot,sigma,t)
   total+=hi-_lognormal_cdf(a,spot,sigma,t)
 return max(0.0,min(1.0,total))


# --- one valuation contract ------------------------------------------------------------------------------------
def horizon_of(expiry,same_expiry):
 """The horizon every strategy-wide number is valued at, with the label the UI shows beside it (audit P03)."""
 at=expiry_moment(expiry);when=at.strftime('%d %b %H:%M').lstrip('0')
 if same_expiry:
  return {'kind':'expiry_exact','expiry':expiry,'at':at.strftime('%Y-%m-%d %H:%M'),'label':f'At expiry - {when} IST, gross',
   'extrema':'exact','note':'Exact payoff at expiry from the entry prices, before charges.'}
 return {'kind':'model_near_expiry','expiry':expiry,'at':at.strftime('%Y-%m-%d %H:%M'),'label':f'Model at near expiry - {when} IST',
  'extrema':'modelled','note':"Later legs valued by Black-Scholes at today's implied volatility. Max profit and loss are modelled at that IV, not guaranteed caps."}


def valuation(active,spot,reading_at,ref_iv=None):
 """The shared valuation of a leg set: horizon (the nearest expiry), each leg's time beyond it, market IVs, the
 probability reference and value_at(S, t, iv_shift). analyze() and the adjustment assistant BOTH use this, so the
 builder and Adjust can never value the same position two ways (audit P02)."""
 expiries=sorted({l['expiry'] for l in active})
 same_expiry=len(expiries)==1;expiry=expiries[0]
 t_now=years_between(reading_at,expiry)
 # each leg's extra time beyond the NEAR expiry (0 for same-expiry strategies)
 off={l['id']:max(0.0,(expiry_moment(l['expiry'])-expiry_moment(expiry)).total_seconds()/SECONDS_PER_YEAR) for l in active}
 ivs={};iv_notes=[]
 for l in active:
  sigma,src=leg_iv(l,spot,t_now+off[l['id']])
  if sigma is None:iv_notes.append(f"{int(l['strike'])} {l['type']}: IV unavailable ({src})")
  ivs[l['id']]=(sigma,src)
 iv_ready=all(x[0] is not None for x in ivs.values())
 # the reference volatility for POP and SD bands: the chain's true ATM IV when supplied; otherwise the included leg
 # nearest spot - a PROXY, and labelled as one (it is not called ATM IV)
 atm=min(active,key=lambda l:abs(l['strike']-spot))
 if ref_iv:sigma_ref,ref_basis=ref_iv,'chain_atm_iv'
 else:sigma_ref=ivs[atm['id']][0] or next((x[0] for x in ivs.values() if x[0]),None);ref_basis='nearest_leg_iv_proxy'

 def value_at(s,t,iv_shift=0.0):
  total=0.0
  for l in active:
   sigma=ivs[l['id']][0];tt=t+off[l['id']]
   x=intrinsic(l['type'],l['strike'],s) if tt<=0 else bs_price(s,l['strike'],tt,max(0.0001,sigma+iv_shift),l['type'])
   total+=units(l)*(x-l['price'])
  return total
 return {'expiry':expiry,'same_expiry':same_expiry,'t_now':t_now,'off':off,'ivs':ivs,'iv_notes':iv_notes,'iv_ready':iv_ready,
  'sigma_ref':sigma_ref,'ref_basis':ref_basis,'value_at':value_at,'horizon':horizon_of(expiry,same_expiry)}


def horizon_profile(legs,spot,reading_at,ref_iv=None,fees=0.0):
 """Worst, best and breakevens of a position AT ITS HORIZON, net of `fees` (a constant, e.g. an adjustment's
 charges): exact at expiry for one expiry, the near-expiry model for several - the builder's own numbers when
 fees=0. Returns None when a later leg has no IV (the model cannot value it)."""
 active=[l for l in legs if l.get('include',True)]
 if not active:return None
 v=valuation(active,spot,reading_at,ref_iv)
 if v['same_expiry']:
  p=expiry_profile(active,fees)
  prof={'max_profit':p['max_profit'],'max_loss':p['max_loss'],'unlimited_profit':p['unlimited_profit'],'unlimited_loss':p['unlimited_loss'],'breakevens':p['breakevens']}
 elif not v['iv_ready']:return None
 else:prof=near_expiry_profile(active,spot,v['sigma_ref'] or 0.15,v['t_now'],lambda x:v['value_at'](x,0.0)-fees)
 f0=(lambda x:expiry_pnl(active,x)-fees) if v['same_expiry'] else (lambda x:v['value_at'](x,0.0)-fees)
 return {'worst':None if prof['unlimited_loss'] else round(prof['max_loss'],2),'best':None if prof['unlimited_profit'] else round(prof['max_profit'],2),
  'unlimited_loss':prof['unlimited_loss'],'unlimited_profit':prof['unlimited_profit'],'breakevens':sorted({round(b,2) for b in prof['breakevens']}),
  'horizon':v['horizon'],'value':f0}


# --- the full analysis ----------------------------------------------------------------------------------------
def analyze(legs,spot,reading_at,scenario=None,charges=None,grid_points=241,table_step=None,table_rows=10,ref_iv=None):
 """Everything the Analyze panel shows, for ONE set of legs at ONE reading and ONE scenario.

 legs: [{id,type,strike,side,lots,lot_size,price,expiry,include,iv?,ltp?}] - excluded legs are ignored everywhere.
 spot: underlying at the reading. reading_at: naive IST datetime of the reading.
 scenario: {spot?, at?, iv_shift?} - at is an IST 'YYYY-MM-DD HH:MM'. Defaults: current spot, the reading, 0.
 """
 scenario=scenario or {}
 active=[l for l in legs if l.get('include',True)]
 warnings=[]
 if not active:return {'status':'empty','model_version':MODEL_VERSION,'warnings':['Add at least one leg to analyse.']}
 if len(active)>MAX_LEGS:return {'status':'invalid','model_version':MODEL_VERSION,'warnings':[f'At most {MAX_LEGS} legs can be analysed.']}
 missing=[l for l in active if l.get('price') is None]
 if missing:return {'status':'invalid','model_version':MODEL_VERSION,
  'warnings':['A leg has no price in the stored reading, so it cannot be valued. Remove it or enter a price.']}
 v=valuation(active,spot,reading_at,ref_iv)
 expiry,same_expiry,t_now,off=v['expiry'],v['same_expiry'],v['t_now'],v['off']
 ivs,iv_notes,iv_ready,sigma_ref,ref_basis=v['ivs'],v['iv_notes'],v['iv_ready'],v['sigma_ref'],v['ref_basis']
 target_at=parse_ist(scenario.get('at')) or reading_at
 if target_at>expiry_moment(expiry):target_at=expiry_moment(expiry)
 if target_at<reading_at:target_at=reading_at
 t_target=max(0.0,years_between(target_at,expiry))
 target_spot=float(scenario.get('spot') or spot)
 iv_shift=float(scenario.get('iv_shift') or 0.0)/100.0

 def value_at(s,t):return v['value_at'](s,t,iv_shift)

 out={'status':'ok','model_version':MODEL_VERSION,'expiry':expiry,'same_expiry':same_expiry,'horizon':v['horizon'],
  'reading_at':reading_at.strftime('%Y-%m-%d %H:%M'),'spot':spot,
  'scenario':{'spot':target_spot,'at':target_at.strftime('%Y-%m-%d %H:%M'),'iv_shift':iv_shift*100,
   'days_to_expiry':round(t_target*365,2),'is_expiry':t_target<=0}}
 premium=sum(-units(l)*l['price'] for l in active)   # + = credit received, - = debit paid
 out['premium']=ok(round(premium,2),basis='entry_prices',direction='credit' if premium>0 else 'debit' if premium<0 else 'zero')
 fees=charges(active) if charges else None
 out['charges']=ok(round(fees['total'],2),basis='estimate_entry',breakdown=fees) if fees else na('CHARGES_NOT_COMPUTED')

 if same_expiry:
  prof=expiry_profile(active)
  out['max_profit']=ok(round(prof['max_profit'],2),basis='expiry_gross') if not prof['unlimited_profit'] else ok(None,basis='expiry_gross',unlimited=True)
  out['max_loss']=ok(round(prof['max_loss'],2),basis='expiry_gross') if not prof['unlimited_loss'] else ok(None,basis='expiry_gross',unlimited=True)
  out['breakevens']=ok(prof['breakevens'],unit='points',basis='expiry')
  rr=None
  if not prof['unlimited_profit'] and not prof['unlimited_loss'] and prof['max_loss']<0:
   rr=round(prof['max_profit']/abs(prof['max_loss']),2)
  out['reward_risk']=ok(rr,unit='ratio',basis='expiry_gross') if rr is not None else na('UNBOUNDED_OR_NO_LOSS')
  # capital at risk: the structural maximum loss. Exchange margin is NOT this and is reported separately.
  out['capital_at_risk']=ok(round(-prof['max_loss'],2),basis='structural_max_loss') if not prof['unlimited_loss'] else ok(None,unlimited=True,basis='structural_max_loss')
  p=pop(active,spot,sigma_ref,t_now,prof) if sigma_ref else None
  out['pop']=ok(round(p*100,1),unit='percent',basis='model_lognormal_at_'+ref_basis,sigma=round(sigma_ref*100,2),sigma_basis=ref_basis) if p is not None else na('NO_IV')
  od=outcome_probabilities(active,spot,sigma_ref,t_now,prof) if sigma_ref else None
  out['outcomes']=ok(od,unit='percent',basis='model_lognormal_at_'+ref_basis) if od else na('NO_IV')
  if prof['unlimited_loss']:warnings.append('Unlimited loss: this structure has no hedge on one side.')
 elif not iv_ready:
  for key in ('max_profit','max_loss','breakevens','reward_risk','capital_at_risk','pop'):
   out[key]=na('IV_UNAVAILABLE',detail=iv_notes)
  warnings.append('A later-expiry leg has no implied volatility, so the value at the near expiry cannot be modelled.')
 else:
  prof=near_expiry_profile(active,spot,sigma_ref or 0.15,t_now,lambda x:value_at(x,0.0))
  B='model_at_near_expiry'
  out['max_profit']=ok(round(prof['max_profit'],2),basis=B) if not prof['unlimited_profit'] else ok(None,basis=B,unlimited=True)
  out['max_loss']=ok(round(prof['max_loss'],2),basis=B) if not prof['unlimited_loss'] else ok(None,basis=B,unlimited=True)
  out['breakevens']=ok(prof['breakevens'],unit='points',basis=B)
  rr=round(prof['max_profit']/abs(prof['max_loss']),2) if (not prof['unlimited_profit'] and not prof['unlimited_loss'] and prof['max_loss']<0) else None
  out['reward_risk']=ok(rr,unit='ratio',basis=B) if rr is not None else na('UNBOUNDED_OR_NO_LOSS')
  out['capital_at_risk']=ok(round(-prof['max_loss'],2),basis=B) if not prof['unlimited_loss'] else ok(None,unlimited=True,basis=B)
  if sigma_ref:
   od=grid_probabilities(lambda x:value_at(x,0.0),spot,sigma_ref,t_now,prof)
   out['pop']=ok(od['profit'],unit='percent',basis=f'{B}_lognormal_at_'+ref_basis,sigma=round(sigma_ref*100,2),sigma_basis=ref_basis)
   out['outcomes']=ok(od,unit='percent',basis=f'{B}_lognormal_at_'+ref_basis)
  else:out['pop']=na('NO_IV');out['outcomes']=na('NO_IV')
  if prof['unlimited_loss']:warnings.append('Unlimited loss: this structure has no hedge on one side.')
  warnings.append(f"Multi-expiry: the payoff shown for {expiry} values the later legs by Black-Scholes at today's implied volatility - a model, not a certain result.")
 out['margin']=na('BROKER_NOT_CONNECTED',note='Exchange (SPAN + exposure) margin needs a connected broker. Not estimated here.')

 # the curves: exact kinks + a grid wide enough to show both tails
 sd_move=spot*(sigma_ref or 0.15)*math.sqrt(max(t_now,1/365))
 lo=max(0.01,min(spot,target_spot)-4*sd_move);hi=max(spot,target_spot)+4*sd_move
 xs=sorted({round(lo+(hi-lo)*i/(grid_points-1),2) for i in range(grid_points)}|
  {float(l['strike']) for l in active if lo<=l['strike']<=hi}|{round(target_spot,2),round(spot,2)})
 curve=[]
 for x in xs:
  point={'s':x,'expiry':round(expiry_pnl(active,x),2) if same_expiry else (round(value_at(x,0.0),2) if iv_ready else None)}
  point['target']=round(value_at(x,t_target),2) if iv_ready else None
  curve.append(point)
 out['curve']=curve
 out['sd']={'sigma':round(sigma_ref*100,2) if sigma_ref else None,'basis':ref_basis+'_to_expiry','sigma_basis':ref_basis,
  'bands':[{'k':k,'low':round(spot*math.exp(-k*(sigma_ref or 0)*math.sqrt(t_now)),2),
   'high':round(spot*math.exp(k*(sigma_ref or 0)*math.sqrt(t_now)),2)} for k in (1,2)] if sigma_ref and t_now>0 else []}
 # the scenario point
 if iv_ready:
  out['scenario_pnl']=ok(round(value_at(target_spot,t_target),2),basis='model_bsm' if t_target>0 else 'expiry_gross')
 else:
  out['scenario_pnl']=na('IV_UNAVAILABLE',detail=iv_notes)
 # the leg table and Greeks
 rows=[];tot={'delta':0.0,'gamma':0.0,'theta':0.0,'vega':0.0};tot_s={'delta':0.0,'gamma':0.0,'theta':0.0,'vega':0.0}
 for l in active:
  sigma,src=ivs[l['id']]
  q=units(l)
  tt=t_target+off[l['id']]
  tv=(intrinsic(l['type'],l['strike'],target_spot) if tt<=0 else bs_price(target_spot,l['strike'],tt,max(0.0001,(sigma or 0)+iv_shift),l['type'])) if sigma is not None or tt<=0 else None
  g=bs_greeks(spot,l['strike'],t_now+off[l['id']],sigma,l['type']) if sigma is not None else None
  # every leg label carries its expiry (fresh audit P01): equal strike/type in two expiries are two contracts
  row={'id':l['id'],'contract_id':l.get('contract_id'),'expiry':l['expiry'],'label':f"{'Buy' if l['side']=='B' else 'Sell'} {l['lots']} x {int(l['strike']) if float(l['strike']).is_integer() else l['strike']} {l['type']} {expiry_moment(l['expiry']).strftime('%d %b').lstrip('0')}",
   'units':q,'entry':l['price'],'ltp':l.get('ltp'),'mark':l.get('mark'),'mark_basis':l.get('mark_basis'),'iv':round(sigma*100,2) if sigma is not None else None,'iv_source':src,
   'target_price':round(tv,2) if tv is not None else None,'target_pnl':round(q*(tv-l['price']),2) if tv is not None else None,
   'greeks':({k:round(v*q,4) for k,v in g.items()} if g else None),'greeks_per_unit':({k:round(v,6) for k,v in g.items()} if g else None),
   # premium split at the reading: intrinsic (what exercising now is worth) and time value (the rest, which decays)
   # time value is a MARKET quantity: the mark (not the cost basis) minus intrinsic; a pure hypothetical falls back to its price
   'intrinsic':round(intrinsic(l['type'],l['strike'],spot),2),'time_value':round((l['mark'] if l.get('mark') is not None else l['price'])-intrinsic(l['type'],l['strike'],spot),2)}
  gs=bs_greeks(target_spot,l['strike'],tt,max(0.0001,sigma+iv_shift),l['type']) if (sigma is not None and t_target>0) else None
  row['greeks_scenario']={k:round(v*q,4) for k,v in gs.items()} if gs else None
  if gs:
   for k in tot_s:tot_s[k]+=gs[k]*q
  rows.append(row)
  if g:
   for k in tot:tot[k]+=g[k]*q
 out['legs']=rows
 out['greeks']=({'status':'available','delta':round(tot['delta'],2),'gamma':round(tot['gamma'],4),'theta':round(tot['theta'],2),
  'vega':round(tot['vega'],2),'units':{'delta':'units per 1 point','gamma':'delta per 1 point','theta':'INR per calendar day',
  'vega':'INR per 1 IV point'},'basis':'model_bsm_at_reading'} if iv_ready else na('IV_UNAVAILABLE',detail=iv_notes))
 # Greeks, POP and breakevens AT THE WHAT-IF (spot, date, IV shift) - Rupeezy recomputes nothing when the date moves;
 # these follow the scenario. At expiry the Greeks are not defined (the position is settled), so they are unavailable.
 scen=(abs(target_spot-spot)>1e-9 or target_at!=reading_at or iv_shift!=0.0)
 out['scenario']['active']=scen
 if iv_ready and t_target>0:
  out['greeks_scenario']={'status':'available','delta':round(tot_s['delta'],2),'gamma':round(tot_s['gamma'],4),'theta':round(tot_s['theta'],2),
   'vega':round(tot_s['vega'],2),'basis':'model_bsm_at_scenario','at':out['scenario']['at'],'spot':target_spot,'iv_shift':iv_shift*100}
 else:
  out['greeks_scenario']=na('AT_EXPIRY' if t_target<=0 else 'IV_UNAVAILABLE')
 if same_expiry and sigma_ref and t_target>0:
  ps=pop(active,target_spot,max(0.0001,sigma_ref+iv_shift),t_target,prof)
  out['pop_scenario']=ok(round(ps*100,1),unit='percent',basis='model_lognormal_from_scenario',sigma=round((sigma_ref+iv_shift)*100,2)) if ps is not None else na('NO_IV')
 else:
  out['pop_scenario']=na('AT_EXPIRY' if t_target<=0 else ('MULTI_EXPIRY' if not same_expiry else 'NO_IV'))
 # breakevens of the target-date curve (where the model P&L at the what-if date crosses zero), from a dense grid
 if iv_ready and t_target>0:
  gx=[lo+(hi-lo)*i/400 for i in range(401)];gv=[value_at(x,t_target) for x in gx];bt=[]
  for (x0,v0),(x1,v1) in zip(zip(gx,gv),zip(gx[1:],gv[1:])):
   if v0==0:bt.append(x0)
   elif (v0<0)!=(v1<0):bt.append(x0+(x1-x0)*(-v0)/(v1-v0))
  out['breakevens_target']=ok(sorted({round(b,2) for b in bt}),unit='points',basis='model_bsm_at_scenario_date',range=[round(lo,2),round(hi,2)])
 else:
  out['breakevens_target']=na('AT_EXPIRY' if t_target<=0 else 'IV_UNAVAILABLE')
 # standard-deviation bands to the what-if date (Dynamic) alongside the bands to expiry (Fixed)
 dt=max(0.0,t_now-t_target)
 out['sd']['bands_to_date']=[{'k':k,'low':round(spot*math.exp(-k*(sigma_ref or 0)*math.sqrt(dt)),2),
  'high':round(spot*math.exp(k*(sigma_ref or 0)*math.sqrt(dt)),2)} for k in (1,2)] if sigma_ref and dt>0 else []
 out['insights']=insights(active,spot,t_now,sigma_ref,out)
 # the payoff table: spot levels around the current spot, target-date and expiry P&L from the SAME maths
 if table_step:
  step=float(table_step);base=round(spot/step)*step
  out['table']=[{'s':x,'pct':round((x/spot-1)*100,2),'target':round(value_at(x,t_target),2) if iv_ready else None,
   'expiry':round(expiry_pnl(active,x),2) if same_expiry else (round(value_at(x,0.0),2) if iv_ready else None)}
   for x in (base+i*step for i in range(-table_rows,table_rows+1)) if x>0]
 out['warnings']=warnings+iv_notes
 return out


