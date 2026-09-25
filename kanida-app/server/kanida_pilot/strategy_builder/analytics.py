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
  * Only same-expiry strategies are analysed in this release. Mixed expiries return 'unsupported' for the expiry
    metrics instead of applying a terminal formula that would be wrong.
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
 if t<=0 or sigma<=0:return intrinsic(kind,k,s)
 return IV.price_bs(s,k,t,r,sigma,kind)
def bs_greeks(s,k,t,sigma,kind,r=RATE):
 """Per ONE option unit: delta (per 1 point), gamma, theta (INR per calendar day), vega (INR per 1 IV point)."""
 if t<=0 or sigma<=0 or s<=0:
  itm=(s>k) if kind=='CE' else (s<k)
  return {'delta':(1.0 if itm else 0.0) if kind=='CE' else (-1.0 if itm else 0.0),'gamma':0.0,'theta':0.0,'vega':0.0}
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
 """Plain-language risk warnings for the structure as it stands (Blueprint A insights list). Each: key, level, text."""
 res=[];add=lambda k,lv,tx:res.append({'key':k,'level':lv,'text':tx})
 shorts=[l for l in active if l['side']=='S']
 for l in active:
  bid,ask=l.get('bid'),l.get('ask')
  lab=f"{int(l['strike']) if float(l['strike']).is_integer() else l['strike']} {l['type']}"
  if bid and ask and (ask+bid)>0 and (ask-bid)/((ask+bid)/2)>0.05:
   add('wide_spread','warn',f"{lab}: the bid-ask spread is {(ask-bid)/((ask+bid)/2)*100:.0f}% of the price - fills will cost more than the mid.")
  elif l.get('price_basis')!='manual' and not (bid and ask) and l.get('ltp') is not None:
   add('no_quote','info',f"{lab}: no live bid/ask - priced at the last trade, which may be stale for an illiquid strike.")
  if 'stale' in (l.get('flags') or []) or 'no_trade' in (l.get('flags') or []):
   add('illiquid','warn',f"{lab}: the price is stale or the strike did not trade - it may be illiquid.")
 for l in shorts:
  lab=f"{int(l['strike']) if float(l['strike']).is_integer() else l['strike']} {l['type']}"
  itm=(spot>l['strike']) if l['type']=='CE' else (spot<l['strike'])
  if itm:add('short_itm','warn',f"Short {lab} is in the money: it carries its full intrinsic loss now and is likely to be exercised at expiry.")
  if sigma and t_now>0:
   z=math.log(l['strike']/spot)/(sigma*math.sqrt(t_now))
   if (l['type']=='CE' and z>2) or (l['type']=='PE' and z<-2):
    add('far_short','info',f"Short {lab} is beyond 2 standard deviations: a small credit against a rare but large move.")
 if shorts and t_now*365<1:
  add('expiry_gamma','warn','Expiry day with short options: gamma is at its highest, so small moves swing the P&L sharply.')
 lot=next((int(l.get('lot_size') or 0) for l in active if l.get('lot_size')),0)
 for l in active:
  u=int(l['lots'])*int(l.get('lot_size') or lot or 0)
  for k,v in FREEZE_UNITS.items():
   if str(l.get('symbol') or '').startswith(k) and not str(l.get('symbol') or '').startswith(k+'NXT') and u>v:
    add('freeze','info',f"{int(l['strike'])} {l['type']}: {u} units exceeds the {v}-unit exchange freeze quantity - it will be sent as several orders.")
    break
 if (out.get('max_loss') or {}).get('unlimited'):
  add('unlimited','warn','Unlimited loss on at least one side: add a hedge leg to cap it.')
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


def expiry_profile(legs):
 """Max profit, max loss and breakevens of the expiry payoff, exactly. Spot is bounded below by 0, unbounded above."""
 strikes=sorted({float(l['strike']) for l in legs})
 _left,right=_slopes(legs)
 points=[0.0]+strikes
 values=[expiry_pnl(legs,x) for x in points]
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
 """The leg's IV solved from ITS OWN entry price, or the chain IV supplied with it, or None with the reason."""
 if leg.get('iv') is not None:return float(leg['iv']),leg.get('iv_source') or 'chain'
 got=IV.solve(leg['price'],spot,leg['strike'],t,leg['type'],sensitivity=False)
 if got.get('iv') is not None:return got['iv'],'solved_from_entry'
 return None,got.get('reason') or 'unsolved'


# --- probability (model) --------------------------------------------------------------------------------------
def _lognormal_cdf(x,spot,sigma,t,r=RATE):
 if x<=0:return 0.0
 mu=math.log(spot)+(r-0.5*sigma*sigma)*t;sd=sigma*math.sqrt(t)
 return IV._norm_cdf((math.log(x)-mu)/sd)


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


# --- the full analysis ----------------------------------------------------------------------------------------
def analyze(legs,spot,reading_at,scenario=None,charges=None,grid_points=241,table_step=None,table_rows=10):
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
 expiries=sorted({l['expiry'] for l in active})
 same_expiry=len(expiries)==1
 expiry=expiries[0]
 t_now=years_between(reading_at,expiry)
 target_at=parse_ist(scenario.get('at')) or reading_at
 if target_at>expiry_moment(expiry):target_at=expiry_moment(expiry)
 if target_at<reading_at:target_at=reading_at
 t_target=max(0.0,years_between(target_at,expiry))
 target_spot=float(scenario.get('spot') or spot)
 iv_shift=float(scenario.get('iv_shift') or 0.0)/100.0

 ivs={};iv_notes=[]
 for l in active:
  sigma,src=leg_iv(l,spot,t_now)
  if sigma is None:iv_notes.append(f"{int(l['strike'])} {l['type']}: IV unavailable ({src})")
  ivs[l['id']]=(sigma,src)
 iv_ready=all(v[0] is not None for v in ivs.values())
 atm=min(active,key=lambda l:abs(l['strike']-spot))
 sigma_ref=ivs[atm['id']][0] or next((v[0] for v in ivs.values() if v[0]),None)

 def value_at(s,t):
  total=0.0
  for l in active:
   sigma=ivs[l['id']][0]
   v=intrinsic(l['type'],l['strike'],s) if t<=0 else bs_price(s,l['strike'],t,max(0.0001,sigma+iv_shift),l['type'])
   total+=units(l)*(v-l['price'])
  return total

 out={'status':'ok','model_version':MODEL_VERSION,'expiry':expiry,'same_expiry':same_expiry,
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
  out['pop']=ok(round(p*100,1),unit='percent',basis='model_lognormal_at_atm_iv',sigma=round(sigma_ref*100,2)) if p is not None else na('NO_IV')
  if prof['unlimited_loss']:warnings.append('Unlimited loss: this structure has no hedge on one side.')
 else:
  for key in ('max_profit','max_loss','breakevens','reward_risk','capital_at_risk','pop'):
   out[key]=na('MULTI_EXPIRY',status='unsupported')
  warnings.append('Mixed expiries are not analysed in this release.')
 out['margin']=na('BROKER_NOT_CONNECTED',note='Exchange (SPAN + exposure) margin needs a connected broker. Not estimated here.')

 # the curves: exact kinks + a grid wide enough to show both tails
 sd_move=spot*(sigma_ref or 0.15)*math.sqrt(max(t_now,1/365))
 lo=max(0.01,min(spot,target_spot)-4*sd_move);hi=max(spot,target_spot)+4*sd_move
 xs=sorted({round(lo+(hi-lo)*i/(grid_points-1),2) for i in range(grid_points)}|
  {float(l['strike']) for l in active if lo<=l['strike']<=hi}|{round(target_spot,2),round(spot,2)})
 curve=[]
 for x in xs:
  point={'s':x,'expiry':round(expiry_pnl(active,x),2) if same_expiry else None}
  point['target']=round(value_at(x,t_target),2) if iv_ready else None
  curve.append(point)
 out['curve']=curve
 out['sd']={'sigma':round(sigma_ref*100,2) if sigma_ref else None,'basis':'atm_iv_to_expiry',
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
  tv=(intrinsic(l['type'],l['strike'],target_spot) if t_target<=0 else bs_price(target_spot,l['strike'],t_target,max(0.0001,(sigma or 0)+iv_shift),l['type'])) if sigma is not None or t_target<=0 else None
  g=bs_greeks(spot,l['strike'],t_now,sigma,l['type']) if sigma is not None else None
  row={'id':l['id'],'label':f"{'Buy' if l['side']=='B' else 'Sell'} {l['lots']} x {int(l['strike']) if float(l['strike']).is_integer() else l['strike']} {l['type']}",
   'units':q,'entry':l['price'],'ltp':l.get('ltp'),'iv':round(sigma*100,2) if sigma is not None else None,'iv_source':src,
   'target_price':round(tv,2) if tv is not None else None,'target_pnl':round(q*(tv-l['price']),2) if tv is not None else None,
   'greeks':({k:round(v*q,4) for k,v in g.items()} if g else None),'greeks_per_unit':({k:round(v,6) for k,v in g.items()} if g else None),
   # premium split at the reading: intrinsic (what exercising now is worth) and time value (the rest, which decays)
   'intrinsic':round(intrinsic(l['type'],l['strike'],spot),2),'time_value':round(l['price']-intrinsic(l['type'],l['strike'],spot),2)}
  gs=bs_greeks(target_spot,l['strike'],t_target,max(0.0001,sigma+iv_shift),l['type']) if (sigma is not None and t_target>0) else None
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
   'expiry':round(expiry_pnl(active,x),2) if same_expiry else None}
   for x in (base+i*step for i in range(-table_rows,table_rows+1)) if x>0]
 out['warnings']=warnings+iv_notes
 return out


