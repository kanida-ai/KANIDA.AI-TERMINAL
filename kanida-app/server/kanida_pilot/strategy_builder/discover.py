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
from . import analytics as A
from .templates import BY_KEY,ResolveError,resolve

FAMILIES={'up':['long_call','bull_call_spread','bull_put_spread','short_put'],
 'down':['long_put','bear_put_spread','bear_call_spread','short_call'],
 'range':['iron_condor','iron_butterfly','short_strangle','short_straddle'],
 'big_move':['long_straddle','long_strangle']}
VIEWS={'up':'Rise','down':'Fall','range':'Stay in a range','big_move':'Big move either way'}
EXCLUDED={'unhedged':'unhedged (naked short) structures are off - defined-risk only','over_max_loss':'maximum loss above your limit',
 'over_budget':'capital at risk above your budget','margin_unknown':'unlimited-risk structure with a budget set (its margin is not computable here)',
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
   a=A.analyze(legs,spot,reading,{'at':A.expiry_moment(chain['expiry']).strftime('%Y-%m-%d %H:%M')},grid_points=3)
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
    'profit_zone_share':round(zone*100) if zone is not None else None,'charges':None,
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
   'suggestion':{'over_max_loss':'Raise your maximum loss or pick a nearer expiry.','over_budget':'Raise the budget or reduce lots.',
    'loses_at_target':'Your target may be too close to spot for this expiry; try a further target or a later expiry.',
    'unhedged':'Allow unhedged structures only if you understand the tail risk.',
    'margin_unknown':'Remove the budget to see unlimited-risk structures (margin is shown after a broker connects).',
    'unresolvable':'Try another expiry with more listed strikes.'}[binding]} if binding else None),
  'basis':'Ranked by expiry P&L at your view divided by capital at risk (structural maximum loss). Model values at the stored reading; not a forecast.'}


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
