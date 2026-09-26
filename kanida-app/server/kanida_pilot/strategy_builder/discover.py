"""Discover: a user's thesis and limits -> a SMALL, explained shortlist of structures that match them.

What the ranking means (blueprint B §1, BUILD_PLAN_MERGED §1): 'matches the constraints you gave', ranked by the
expiry return on capital-at-risk AT YOUR OWN target. It is not a forecast and not advice. No candidate carries a
historical-performance badge yet: every card says 'Model only - not tested on history' until the Lab exists and a
rule has out-of-sample evidence (blueprint A §7.3 switches on then).

Rules that are never relaxed silently:
  * defined-risk only unless the user turns that off;
  * a maximum loss or budget excludes unlimited-risk structures (their exchange margin is not computable here);
  * a candidate that loses money at the user's own target is excluded, and the exclusion is counted and named.
"""
from __future__ import annotations
import threading,time,uuid
from . import analytics as A
from . import service as S
from .store import checksum
from .templates import BY_KEY,ResolveError,resolve

FAMILIES={'up':['long_call','bull_call_spread','bull_put_spread','call_backspread','short_put','call_ratio_spread','risk_reversal_bullish'],
 'down':['long_put','bear_put_spread','bear_call_spread','put_backspread','short_call','put_ratio_spread','risk_reversal_bearish'],
 'range':['iron_condor','iron_butterfly','long_call_butterfly','long_put_butterfly','short_strangle','short_straddle'],
 'big_move':['long_straddle','long_strangle','long_iron_butterfly','long_iron_condor','strip','strap']}
VIEWS={'up':'Rise','down':'Fall','range':'Stay in a range','big_move':'Big move either way'}
EXCLUDED={'unhedged':'unhedged (naked short) structures are off - defined-risk only','over_max_loss':'maximum loss above your limit',
 'over_budget':'structural maximum loss above your max-loss budget','margin_unknown':'unlimited-risk structure with a max-loss budget set (its loss has no cap)',
 'loses_at_target':'loses money at your target','unresolvable':'not enough priced strikes in this expiry'}
LIMIT=6


class DiscoverError(Exception):
 def __init__(self,message):super().__init__(message);self.message=message


def _num(v,name,required=False):
 if v in (None,''):
  if required:raise DiscoverError(f'{name} is required for this view.')
  return None
 try:x=float(v)
 except (TypeError,ValueError):raise DiscoverError(f'{name} must be a number.')
 if x<=0:raise DiscoverError(f'{name} must be positive.')
 return x


def run(chain,req):
 view=str(req.get('view') or '')
 if view not in FAMILIES:raise DiscoverError('Choose a view: up, down, range or big_move.')
 target=_num(req.get('target'),'Target',view in ('up','down'))
 low=_num(req.get('low'),'Range low',view in ('range','big_move'));high=_num(req.get('high'),'Range high',view in ('range','big_move'))
 if view in ('range','big_move') and low>=high:raise DiscoverError('The range low must be below the range high.')
 max_loss=_num(req.get('max_loss'),'Maximum loss');budget=_num(req.get('budget'),'Budget')
 hedged=req.get('hedged_only',True) is not False
 lots=int(req.get('lots') or 1)
 spot=chain['spot'];reading=A.parse_ist(chain['as_of'])
 if view=='up' and target<=spot:raise DiscoverError(f'For a rise, the target must be above the current spot ({spot:,.2f}).')
 if view=='down' and target>=spot:raise DiscoverError(f'For a fall, the target must be below the current spot ({spot:,.2f}).')
 tally={k:0 for k in EXCLUDED};best={};considered=0
 for key in FAMILIES[view]:
  t=BY_KEY[key]
  for v in ((t['param'] or {}).get('variants') or [None]):
   considered+=1
   try:res=resolve(key,chain,v,lots)
   except ResolveError:tally['unresolvable']+=1;continue
   legs=res['legs']
   # joined, priced and referenced exactly as the builder will analyse the same draft (audit P06)
   legs,a=S.analyze_on_chain(chain,legs,{'at':A.expiry_moment(chain['expiry']).strftime('%Y-%m-%d %H:%M')},grid_points=3)
   if a.get('status')!='ok':tally['unresolvable']+=1;continue
   unlimited=bool(a['max_loss'].get('unlimited'))
   if hedged and t['risk']!='defined':tally['unhedged']+=1;continue
   if unlimited and budget:tally['margin_unknown']+=1;continue
   loss=None if unlimited else -a['max_loss']['value']
   if max_loss and (unlimited or loss>max_loss):tally['over_max_loss']+=1;continue
   capital=loss
   if budget and capital is not None and capital>budget:tally['over_budget']+=1;continue
   pnl=lambda s:A.expiry_pnl(legs,s)
   if view in ('up','down'):fit=pnl(target);zone=None
   elif view=='range':
    pts=[low+(high-low)*i/20 for i in range(21)];vals=[pnl(x) for x in pts]
    fit=sum(vals)/len(vals);zone=sum(1 for x in vals if x>0)/len(vals)
   else:fit=min(pnl(low),pnl(high));zone=None
   if fit<=0 or (view=='range' and zone<0.5):tally['loses_at_target']+=1;continue
   score=fit/capital if capital else 0.0
   card={'template':key,'name':t['name'],'recipe':t['recipe'],'risk':t['risk'],'param':res['param'],
    'param_label':(t['param'] or {}).get('label'),'legs':legs,
    'max_profit':a['max_profit'],'max_loss':a['max_loss'],'breakevens':a['breakevens']['value'],'premium':a['premium']['value'],
    'capital_at_risk':capital,'pop':a['pop'].get('value'),'pnl_at_view':round(fit,2),'return_on_risk':round(score*100,1) if capital else None,
    'profit_zone_share':round(zone*100) if zone is not None else None,'charges':None,'price_basis':sorted({l.get('basis_used') or 'ltp' for l in legs}),'pop_basis':(a['pop'] or {}).get('basis'),
    'reference':{'iv':(a['pop'] or {}).get('sigma'),'source':(a['pop'] or {}).get('sigma_basis'),'as_of':chain['as_of'],'model_version':a.get('model_version')},
    # the budget constrains STRUCTURAL maximum loss. Required funds (exchange margin) are a different number that
    # Discover does not estimate - stated here, never implied by the budget filter (GTM audit P03)
    'funds':{'status':'unknown','note':'Exchange margin is not estimated here. The builder and order review show it (live data only).'},
    'evidence':{'status':'model_only','label':'Model only - not tested on history'},'why':[]}
   if key not in best or score>best[key]['_score']:best[key]={**card,'_score':score}
 cards=sorted(best.values(),key=lambda c:-c['_score'])[:LIMIT]
 for c in cards:c.pop('_score',None)
 if cards:_explain(cards,view,target,low,high)
 binding=max(tally,key=tally.get) if not cards and any(tally.values()) else None
 return {'view':view,'view_label':VIEWS[view],'as_of':chain['as_of'],'spot':spot,'expiry':chain['expiry'],
  'inputs':{'target':target,'low':low,'high':high,'max_loss':max_loss,'budget':budget,'hedged_only':hedged,'lots':lots},
  'considered':considered,'candidates':cards,
  'excluded':[{'reason':k,'label':EXCLUDED[k],'count':n} for k,n in tally.items() if n],
  'binding':({'reason':binding,'label':EXCLUDED[binding],
   'suggestion':{'over_max_loss':'Raise your maximum loss or pick a nearer expiry.','over_budget':'Raise the max-loss budget or reduce lots.',
    'loses_at_target':'Your target may be too close to spot for this expiry; try a further target or a later expiry.',
    'unhedged':'Allow unhedged structures only if you understand the tail risk.',
    'margin_unknown':'Remove the max-loss budget to see unlimited-risk structures.',
    'unresolvable':'Try another expiry with more listed strikes.'}[binding]} if binding else None),
  'basis':'Ranked by expiry P&L at your view divided by capital at risk (structural maximum loss). Prices are last traded (LTP) at this reading; the draft reprices at buy-at-ask / sell-at-bid where quotes exist. Model values; not a forecast.'}


def _explain(cards,view,target,low,high):
 def mark(fn,label,rev=False):
  vals=[(fn(c),c) for c in cards if fn(c) is not None]
  if len(vals)>1:(min if not rev else max)(vals,key=lambda x:x[0])[1]['why'].append(label)
 mark(lambda c:c['capital_at_risk'],'Lowest capital at risk')
 mark(lambda c:c['return_on_risk'],'Highest return on risk at your view',rev=True)
 mark(lambda c:(c['max_profit'] or {}).get('value') if not (c['max_profit'] or {}).get('unlimited') else None,'Largest capped profit',rev=True)
 mark(lambda c:c['pop'],'Highest model probability of profit',rev=True)
 for c in cards:
  p=c['pnl_at_view'];cap=c['capital_at_risk']
  if view in ('up','down'):head=f"At {target:,.0f} on expiry: {'+' if p>=0 else ''}{p:,.0f}"
  elif view=='range':head=f"Average across {low:,.0f}-{high:,.0f}: {p:,.0f}; profitable on {c['profit_zone_share']}% of it"
  else:head=f"Worst of {low:,.0f} / {high:,.0f} on expiry: {p:,.0f}"
  c['why'].insert(0,head+(f" for {cap:,.0f} at risk" if cap else ''))


class Results:
 """Discover answers, held server-side so "Use as draft" builds EXACTLY the candidate that was shown (its own
 underlying, expiry, legs, lots and thesis) - never the current, possibly edited, form plus old legs (GTM audit P03).
 In-process and short-lived by design: a result older than TTL must be found again against newer prices."""
 TTL=1800;MAX=500
 def __init__(self):self.lock=threading.Lock();self._r={}

 def put(self,user_id,req,out):
  rid=uuid.uuid4().hex[:16];t=time.time()
  norm={k:req.get(k) for k in ('underlying','expiry','view','target','low','high','max_loss','budget','hedged_only','lots')}
  out['result_id']=rid;out['request_hash']=checksum({**norm,'as_of':out['as_of']});out['expires_at']=t+self.TTL
  for i,c in enumerate(out['candidates']):c['candidate_id']=f'{rid}.{i}'
  with self.lock:
   if len(self._r)>=self.MAX:
    for k in sorted(self._r,key=lambda k:self._r[k]['t'])[:self.MAX//5]:self._r.pop(k,None)
   self._r[rid]={'user':user_id,'t':t,'out':out,'underlying':str(req.get('underlying') or '').upper()}
  return out

 def candidate(self,user_id,candidate_id):
  """(result, candidate) or raises DiscoverError."""
  rid,_,_i=str(candidate_id or '').partition('.')
  with self.lock:r=self._r.get(rid)
  if not r or r['user']!=user_id:raise DiscoverError('This result is no longer available - find strategies again.')
  if time.time()-r['t']>self.TTL:raise DiscoverError('This result has expired (prices move) - find strategies again.')
  c=next((c for c in r['out']['candidates'] if c['candidate_id']==candidate_id),None)
  if not c:raise DiscoverError('There is no such candidate in this result.')
  return r,c

 def draft_body(self,user_id,candidate_id):
  r,c=self.candidate(user_id,candidate_id);o=r['out'];inp=o['inputs']
  legs=[{'id':f'L{i+1}','type':l['type'],'side':l['side'],'strike':l['strike'],'lots':l['lots'],'expiry':o['expiry'],
   'price_basis':'exec','price':None,'include':True} for i,l in enumerate(c['legs'])]
  scen={'spot':inp['target']} if o['view'] in ('up','down') and inp.get('target') else {}
  body={'underlying':r['underlying'],'expiry':o['expiry'],'legs':legs,'scenario':scen,'template':c['template'],'param':c.get('param')}
  thesis=(f"Discover: {o['view_label']} " + (f"to {inp['target']:,.0f}" if o['view'] in ('up','down') else f"{inp['low']:,.0f}-{inp['high']:,.0f}")
   + f" by {o['expiry']} (reading {o['as_of']} IST; candidate priced at LTP, draft at buy-at-ask / sell-at-bid)")
  return body,c,thesis
