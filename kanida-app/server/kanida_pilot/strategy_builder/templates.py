"""Template recipes, their resolution to concrete strikes, and recognition of a leg set's structure.

A template is a RELATIVE recipe (Rupeezy's idea, made explicit): 'Buy 1 ATM CE, Sell 1 CE +width strikes'. It becomes
contracts only against one chain at one reading. Nothing here claims a template works - templates carry no returns.

Strike rules
  {'ref':'atm','steps':a,'per':b}   strike = ATM + (a + b x param) strike steps
  {'ref':'sd','sd':+-1,'steps':a,'per':b}  strike = nearest(spot x e^(+-1 x sigma x sqrt t)) + (a + b x param) steps,
                                     sigma = the chain's ATM IV - the '1 standard deviation' short strike
Every resolved strike snaps to the nearest listed strike that has a price for that option type.

Availability (blueprint B §6): the initial catalogue is simple defined-risk structures; short options are listed
with an explicit UNHEDGED label (a short call's loss is unlimited; a short put's runs to a zero spot) and are never in a beginner shortlist; multi-expiry, ratio and futures structures
are listed as 'later' and cannot be used until their analytics exist.
"""
from __future__ import annotations
import math

def _atm(steps,per=0):return {'ref':'atm','steps':steps,'per':per}
def _sd(sign,steps=0,per=0):return {'ref':'sd','sd':sign,'steps':steps,'per':per}

TEMPLATES=[
 {'key':'long_call','name':'Long Call','intent':'bullish','risk':'defined','tier':'core','complexity':1,
  'legs':[{'side':'B','type':'CE','strike':_atm(0,1)}],
  'param':{'name':'moneyness','label':'Strikes above ATM','default':0,'variants':[0,1,2]},
  'recipe':'Buy 1 call at ATM (+ moneyness strikes)',
  'use':'You expect a rise and want the loss capped at the premium paid.','loses':'NIFTY stays flat or falls, and time decay erodes the premium.'},
 {'key':'long_put','name':'Long Put','intent':'bearish','risk':'defined','tier':'core','complexity':1,
  'legs':[{'side':'B','type':'PE','strike':_atm(0,-1)}],
  'param':{'name':'moneyness','label':'Strikes below ATM','default':0,'variants':[0,1,2]},
  'recipe':'Buy 1 put at ATM (- moneyness strikes)',
  'use':'You expect a fall and want the loss capped at the premium paid.','loses':'The underlying stays flat or rises, and time decay erodes the premium.'},
 {'key':'bull_call_spread','name':'Bull Call Spread','intent':'bullish','risk':'defined','tier':'core','complexity':2,
  'legs':[{'side':'B','type':'CE','strike':_atm(0)},{'side':'S','type':'CE','strike':_atm(0,1)}],
  'param':{'name':'width','label':'Width (strikes)','default':4,'variants':[2,4,6,8]},
  'recipe':'Buy 1 ATM call · Sell 1 call +width strikes',
  'use':'A moderate rise. Selling the higher call lowers the cost and caps the profit.','loses':'The underlying ends below the bought strike.'},
 {'key':'bear_put_spread','name':'Bear Put Spread','intent':'bearish','risk':'defined','tier':'core','complexity':2,
  'legs':[{'side':'B','type':'PE','strike':_atm(0)},{'side':'S','type':'PE','strike':_atm(0,-1)}],
  'param':{'name':'width','label':'Width (strikes)','default':4,'variants':[2,4,6,8]},
  'recipe':'Buy 1 ATM put · Sell 1 put -width strikes',
  'use':'A moderate fall at a lower cost than a long put.','loses':'The underlying ends above the bought strike.'},
 {'key':'bull_put_spread','name':'Bull Put Spread','intent':'bullish','risk':'defined','tier':'core','complexity':2,
  'legs':[{'side':'S','type':'PE','strike':_atm(-1)},{'side':'B','type':'PE','strike':_atm(-1,-1)}],
  'param':{'name':'width','label':'Width (strikes)','default':4,'variants':[2,4,6,8]},
  'recipe':'Sell 1 put 1 strike below ATM · Buy 1 put a further width strikes below',
  'use':'Collect premium if the underlying holds above the sold put.','loses':'The underlying falls through both puts.'},
 {'key':'bear_call_spread','name':'Bear Call Spread','intent':'bearish','risk':'defined','tier':'core','complexity':2,
  'legs':[{'side':'S','type':'CE','strike':_atm(1)},{'side':'B','type':'CE','strike':_atm(1,1)}],
  'param':{'name':'width','label':'Width (strikes)','default':4,'variants':[2,4,6,8]},
  'recipe':'Sell 1 call 1 strike above ATM · Buy 1 call a further width strikes above',
  'use':'Collect premium if the underlying stays below the sold call.','loses':'The underlying rises through both calls.'},
 {'key':'long_straddle','name':'Long Straddle','intent':'volatility','risk':'defined','tier':'core','complexity':2,
  'legs':[{'side':'B','type':'CE','strike':_atm(0)},{'side':'B','type':'PE','strike':_atm(0)}],
  'param':None,'recipe':'Buy 1 ATM call · Buy 1 ATM put',
  'use':'A large move in either direction, e.g. around an event.','loses':'The underlying stays near the strike and time decay erodes both options.'},
 {'key':'long_strangle','name':'Long Strangle','intent':'volatility','risk':'defined','tier':'core','complexity':2,
  'legs':[{'side':'B','type':'CE','strike':_atm(0,1)},{'side':'B','type':'PE','strike':_atm(0,-1)}],
  'param':{'name':'width','label':'Strikes from ATM','default':2,'variants':[1,2,4]},
  'recipe':'Buy 1 call +width · Buy 1 put -width',
  'use':'A very large move in either direction, cheaper than a straddle.','loses':'The underlying stays between the strikes.'},
 {'key':'iron_condor','name':'Iron Condor','intent':'range','risk':'defined','tier':'core','complexity':3,
  'legs':[{'side':'B','type':'PE','strike':_sd(-1,0,-1)},{'side':'S','type':'PE','strike':_sd(-1)},
          {'side':'S','type':'CE','strike':_sd(1)},{'side':'B','type':'CE','strike':_sd(1,0,1)}],
  'param':{'name':'wings','label':'Wing width (strikes)','default':4,'variants':[2,4,6]},
  'recipe':'Sell 1 put and 1 call about 1 standard deviation out · Buy wings a further width strikes out',
  'use':'The underlying stays inside a range; the wings cap the loss.','loses':'The underlying moves beyond either short strike.'},
 {'key':'iron_butterfly','name':'Iron Butterfly','intent':'range','risk':'defined','tier':'core','complexity':3,
  'legs':[{'side':'B','type':'PE','strike':_atm(0,-1)},{'side':'S','type':'PE','strike':_atm(0)},
          {'side':'S','type':'CE','strike':_atm(0)},{'side':'B','type':'CE','strike':_atm(0,1)}],
  'param':{'name':'wings','label':'Wing width (strikes)','default':4,'variants':[2,4,6]},
  'recipe':'Sell 1 ATM call and 1 ATM put · Buy wings width strikes out',
  'use':'The underlying pins near the current level; a larger credit than a condor.','loses':'Any sizeable move away from the centre strike.'},
 {'key':'short_put','name':'Short Put','intent':'bullish','risk':'unhedged','tier':'advanced','complexity':1,
  'legs':[{'side':'S','type':'PE','strike':_atm(0,-1)}],
  'param':{'name':'moneyness','label':'Strikes below ATM','default':1,'variants':[0,1,2]},
  'recipe':'Sell 1 put below ATM','use':'Collect premium if the underlying holds.','loses':'A large fall - the loss grows with every point down.'},
 {'key':'short_call','name':'Short Call','intent':'bearish','risk':'unhedged','tier':'advanced','complexity':1,
  'legs':[{'side':'S','type':'CE','strike':_atm(0,1)}],
  'param':{'name':'moneyness','label':'Strikes above ATM','default':1,'variants':[0,1,2]},
  'recipe':'Sell 1 call above ATM','use':'Collect premium if the underlying stays below the strike.','loses':'Any rally - the loss is unlimited.'},
 {'key':'short_straddle','name':'Short Straddle','intent':'range','risk':'unhedged','tier':'advanced','complexity':2,
  'legs':[{'side':'S','type':'CE','strike':_atm(0)},{'side':'S','type':'PE','strike':_atm(0)}],
  'param':None,'recipe':'Sell 1 ATM call · Sell 1 ATM put','use':'The underlying pins at the strike.','loses':'A move either way - the call side is unlimited.'},
 {'key':'short_strangle','name':'Short Strangle','intent':'range','risk':'unhedged','tier':'advanced','complexity':2,
  'legs':[{'side':'S','type':'PE','strike':_sd(-1)},{'side':'S','type':'CE','strike':_sd(1)}],
  'param':None,'recipe':'Sell 1 put and 1 call about 1 standard deviation out','use':'The underlying stays inside a range.',
  'loses':'A move beyond either strike - the call side is unlimited.'},
]
LATER=[
 {'key':'call_butterfly','name':'Call / Put Butterfly','reason':'1:-2:1 ratio analytics and asymmetric wings are not validated yet.'},
 {'key':'ratio_spread','name':'Ratio spreads / back-spreads','reason':'Uncovered tail disclosure is not built yet.'},
 {'key':'calendar','name':'Calendar / diagonal','reason':'Needs multi-expiry valuation - blocked until validated.'},
 {'key':'synthetic_future','name':'Futures & synthetics','reason':'Futures valuation, margin and settlement are not built yet.'},
 {'key':'jade_lizard','name':'Jade Lizard, Batman and other named combinations','reason':'Kept out of the initial catalogue on purpose.'},
]
BY_KEY={t['key']:t for t in TEMPLATES}


def public():
 keep=('key','name','intent','risk','tier','complexity','param','recipe','use','loses')
 return {'templates':[{k:t[k] for k in keep}|{'legs':len(t['legs'])} for t in TEMPLATES],'later':LATER}


class ResolveError(Exception):
 def __init__(self,code,message):super().__init__(message);self.code=code;self.message=message


def resolve(key,chain,param=None,lots=1):
 """Concrete legs for template `key` against `chain` (market.Market.chain output)."""
 t=BY_KEY.get(key)
 if not t:raise ResolveError('TEMPLATE_NOT_FOUND','There is no such template.')
 p=t['param']
 value=param if param is not None else (p['default'] if p else 0)
 if p and value not in p['variants']:value=min(p['variants'],key=lambda v:abs(v-float(value)))
 step=chain['strike_step'];spot=chain['spot'];atm=chain['atm_strike']
 if not step or atm is None:raise ResolveError('NO_CHAIN','This expiry has no usable strikes in the stored reading.')
 sigma=(chain.get('atm_iv') or 0)/100.0;t_years=max(chain['days_to_expiry'],0.01)/365.0
 priced={kind:sorted(r['strike'] for r in chain['rows'] if r.get(kind) and r[kind].get('ltp') is not None) for kind in ('CE','PE')}
 legs=[]
 for i,spec in enumerate(t['legs']):
  rule=spec['strike'];offset=(rule['steps']+rule['per']*(value or 0))*step
  if rule['ref']=='atm':target=atm+offset
  else:
   if not sigma:raise ResolveError('NO_IV','The ATM implied volatility is unavailable, so a 1-standard-deviation strike cannot be placed.')
   target=spot*math.exp(rule['sd']*sigma*math.sqrt(t_years))+offset
  pool=priced[spec['type']]
  if not pool:raise ResolveError('NO_PRICES',f"No {spec['type']} in this expiry has a price in the stored reading.")
  strike=min(pool,key=lambda k:abs(k-target))
  row=next(r for r in chain['rows'] if r['strike']==strike)[spec['type']]
  legs.append({'id':f'L{i+1}','type':spec['type'],'side':spec['side'],'strike':strike,'lots':lots,'lot_size':chain['lot_size'],
   'expiry':chain['expiry'],'price':row['ltp'],'price_basis':'ltp','ltp':row['ltp'],'include':True,'token':row['token'],'symbol':row['symbol']})
 # a snapped recipe that collapses two legs onto one strike is no longer the structure it names
 shape=[(l['type'],l['strike'],l['side']) for l in legs]
 if len(set(shape))<len(shape) or (key not in ('long_straddle','short_straddle','iron_butterfly') and
   len({(l['type'],l['strike']) for l in legs})<len(legs)):
  raise ResolveError('NOT_ENOUGH_STRIKES','This expiry does not list enough priced strikes for that width.')
 return {'template':key,'param':value,'legs':legs}


def recognise(legs):
 """The structure a leg set IS, or 'custom'. Pure shape: sides, types, strike order, equal sizes."""
 act=[l for l in legs if l.get('include',True)]
 n=len(act)
 name=lambda k:{'key':k,'name':BY_KEY[k]['name'] if k in BY_KEY else k.replace('_',' ').title(),'exact':True}
 if n==0:return {'key':None,'name':'Empty','exact':False}
 if len({int(l['lots']) for l in act})!=1 or len({l['expiry'] for l in act})!=1:
  return {'key':'custom','name':f'Custom ({n} legs)','exact':False}
 if n==1:
  l=act[0];return name(('long_' if l['side']=='B' else 'short_')+('call' if l['type']=='CE' else 'put'))
 if n==2:
  a,b=sorted(act,key=lambda l:(l['strike'],l['type']))
  if a['type']==b['type'] and a['side']!=b['side'] and a['strike']!=b['strike']:
   low_buy=a['side']=='B'
   if a['type']=='CE':return name('bull_call_spread' if low_buy else 'bear_call_spread')
   return name('bull_put_spread' if low_buy else 'bear_put_spread')
  if a['type']!=b['type'] and a['side']==b['side']:
   ce=a if a['type']=='CE' else b;pe=b if ce is a else a
   if ce['strike']==pe['strike']:return name('long_straddle' if a['side']=='B' else 'short_straddle')
   if ce['strike']>pe['strike']:return name('long_strangle' if a['side']=='B' else 'short_strangle')
 if n==4:
  ces=sorted([l for l in act if l['type']=='CE'],key=lambda l:l['strike']);pes=sorted([l for l in act if l['type']=='PE'],key=lambda l:l['strike'])
  if len(ces)==2 and len(pes)==2:
   if pes[0]['side']=='B' and pes[1]['side']=='S' and ces[0]['side']=='S' and ces[1]['side']=='B' and pes[1]['strike']<=ces[0]['strike']:
    return name('iron_butterfly' if pes[1]['strike']==ces[0]['strike'] else 'iron_condor')
 return {'key':'custom','name':f'Custom ({n} legs)','exact':False}
