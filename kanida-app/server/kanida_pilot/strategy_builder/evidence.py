"""Slice 9 — the evidence board behind Discover's ranking (blueprint A §7.3, with stated deviations).

One HYPOTHESIS per RULE, not per run. A rule is (template, width, decision weekday, days-to-expiry range, exits,
slippage). H0: "the rule's out-of-sample expectancy after costs and slippage <= 0".
  * Each completed plain Lab run of the rule (no adjustment) yields a p-value from its own stored out-of-sample trades
    (entry on/after its split; split-straddling trades were already dropped). Runs with n_oos < 30 are not tests.
  * The rule's p is the MAXIMUM over its runs (the most conservative) - re-running a rule, or moving its split/period
    until one run looks good, can never improve it; and duplicates never pad the family.
  * m = the number of DISTINCT rules tested by the user; Benjamini-Hochberg at FDR 10% (rank-based step-up).
  * p-value: Johnson's skew-adjusted t (1978), one-sided, normal approximation (n >= 30). Option P&L is skewed; a plain
    t-test and even a block bootstrap over-reject on short-premium P&L (simulated: 13-17% at a nominal 5-10%).
  * Tail stress (defined-risk only): a sample can simply not contain the rare max-loss trade, and then NO data-only
    test sees it. When losses of >= 50% of the maximum loss appear fewer than 5 times, their frequency is raised to its
    95% upper bound (Wilson) and the mean must stay above zero. Simulated null pass rates with both guards: <= 11% at
    FDR 10% for credit structures with 2-5% tails, debit 50/50 and normal P&L.
  * Undefined-risk structures (no maximum loss) are never "Tested ✓": their tail cannot be bounded at all.
  * The 95% low used for ranking is a moving-block bootstrap (block ~ n^(1/3), keeps autocorrelation) of the same
    per-trade series.
  * One statistic for badge AND ranking: per-trade return per ₹100 of maximum model loss when every OOS trade has a
    defined max loss; otherwise per-trade ₹ net (and the rule never enters the ranked tier).
Ranking (a stated deviation from the blueprint's single sort): tier 1 = "Tested ✓" DEFINED-RISK rules whose OOS 95%
lower bound is above zero, by that lower bound; tier 2 = everything else in the model order. A rule with weak or
negative evidence therefore never outranks the model order. Adjustment runs are another family (slice 8, Bonferroni).
"""
from __future__ import annotations
import hashlib,json,math,random
from datetime import date,timedelta
from typing import Any,Dict,List,Optional

FDR_Q=0.10
MIN_OOS=30
BOOT=2000
_CACHE:Dict[str,Dict[str,Any]]={}          # completed runs never change: their per-run entry is cached by id
WEEKDAYS=['Mon','Tue','Wed','Thu','Fri']


def _block_boot(xs:List[float],seed:int)->List[float]:
 n=len(xs);b=max(1,round(n**(1/3)));rng=random.Random(seed);k=math.ceil(n/b);out=[]
 for _ in range(BOOT):
  s=[]
  for _j in range(k):
   i=rng.randrange(0,n-b+1);s.extend(xs[i:i+b])
  s=s[:n];out.append(sum(s)/n)
 return sorted(out)


def johnson_p(xs:List[float])->float:
 n=len(xs);m=sum(xs)/n;s2=sum((x-m)**2 for x in xs)/(n-1)
 if s2<=0:return 0.0 if m>0 else 1.0
 mu3=n*sum((x-m)**3 for x in xs)/((n-1)*(n-2))
 t=(m+mu3/(6*s2*n)+mu3/(3*s2*s2)*m*m)/math.sqrt(s2/n)
 return 0.5*math.erfc(t/math.sqrt(2))


def tail_stress(ror:List[float])->float:
 """Mean return per ₹100 of max loss after raising an under-observed tail (< 5 losses of >= 50% of max loss) to its
 95% upper frequency (Wilson, one-sided z=1.645), each extra tail event being a full maximum loss."""
 n=len(ror);k=sum(1 for x in ror if x<=-50);m=sum(ror)/n
 if k>=5:return m
 z=1.645;pu=(k+z*z/2+z*math.sqrt(k*(n-k)/n+z*z/4))/(n+z*z)
 non=[x for x in ror if x>-50];nm=sum(non)/len(non) if non else 0.0
 return m-(pu-k/n)*(nm+100)


def rule_key(spec:Dict[str,Any])->str:
 return json.dumps([spec['template'],spec.get('param'),spec['weekday'],spec['dte_min'],spec['dte_max'],
  spec.get('target_pct'),spec.get('stop_pct'),spec.get('exit_dte'),spec.get('slippage')],default=str)


def entry(run_id:str,spec:Dict[str,Any],result:Dict[str,Any],created_at:float)->Dict[str,Any]:
 if run_id in _CACHE:return _CACHE[run_id]
 split=spec['split'];oos=[t for t in (result.get('trades') or []) if t['entry']>=split]
 defined=bool(oos) and all(t.get('capital_at_risk') for t in oos)
 series=[t['net']/t['capital_at_risk']*100 for t in oos] if defined else [t['net'] for t in oos]
 e={'run_id':run_id,'rule':rule_key(spec),'template':spec['template'],'param':spec.get('param'),'weekday':spec['weekday'],
  'dte':[spec['dte_min'],spec['dte_max']],'exits':{k:spec.get(k) for k in ('target_pct','stop_pct','exit_dte')},
  'slippage':spec.get('slippage'),'period':[spec['from'],spec['to']],'split':split,'created_at':created_at,
  'n_oos':len(oos),'mean_oos':round(sum(t['net'] for t in oos)/len(oos),2) if oos else None,
  'defined_risk':defined,'unit':'per ₹100 of max model loss' if defined else '₹ per trade','p':None,'low':None,'mean_stat':None,'stress':None}
 if len(oos)>=MIN_OOS:
  boot=_block_boot(series,int(hashlib.sha256(run_id.encode()).hexdigest()[:8],16))
  e['p']=johnson_p(series)
  e['low']=round(boot[int(0.025*BOOT)],3);e['mean_stat']=round(sum(series)/len(series),3)
  e['stress']=round(tail_stress(series),3) if defined else None
 _CACHE[run_id]=e
 return e


def board(rows)->Dict[str,Any]:
 """rows: (id, spec_json, result_json, created_at) of completed backtests -> rules with BH-corrected status."""
 runs=[]
 for rid,spec,res,at in rows:
  s=json.loads(spec) if isinstance(spec,str) else spec
  if s.get('adjust'):continue
  r=json.loads(res) if isinstance(res,str) else res
  if not r or r.get('kind')!='backtest':continue
  runs.append(entry(rid,s,r,at))
 rules={}
 for e in runs:rules.setdefault(e['rule'],[]).append(e)
 out=[]
 for key,rs in rules.items():
  tested=[e for e in rs if e['p'] is not None]
  worst=max(tested,key=lambda e:(e['p'],-(e['low'] if e['low'] is not None else 0))) if tested else None   # the most conservative run decides
  base=rs[0]
  out.append({'rule':key,'template':base['template'],'param':base['param'],'weekday':base['weekday'],'dte':base['dte'],'exits':base['exits'],
   'slippage':base['slippage'],'runs':len(rs),'tested_runs':len(tested),'run_ids':[e['run_id'] for e in rs],
   'deciding_run':worst['run_id'] if worst else None,'p':worst['p'] if worst else None,'low':worst['low'] if worst else None,
   'mean_stat':worst['mean_stat'] if worst else None,'stress':worst['stress'] if worst else None,'n_oos':worst['n_oos'] if worst else max(e['n_oos'] for e in rs),
   'mean_oos':worst['mean_oos'] if worst else None,'defined_risk':worst['defined_risk'] if worst else base['defined_risk'],
   'unit':(worst or base)['unit'],'latest':max(e['created_at'] for e in rs)})
 tests=sorted([r for r in out if r['p'] is not None],key=lambda r:r['p']);m=len(tests);k=0
 for i,r in enumerate(tests,1):
  if r['p']<=i/m*FDR_Q:k=i                                     # BH step-up: the largest rank that passes
 passed={id(r) for r in tests[:k]}
 for r in out:
  r['reason']=None
  if r['p'] is None:r['status']='insufficient'
  elif id(r) not in passed or (r['mean_stat'] or 0)<=0:r['status']='tested_not_significant';r['reason']='not_significant'
  elif not r['defined_risk']:r['status']='tested_not_significant';r['reason']='tail_undefined'
  elif (r['stress'] or 0)<=0:r['status']='tested_not_significant';r['reason']='tail_stress'
  else:r['status']='tested_significant'
  r['tests']=m
 by_run={rid:r for r in out for rid in r['run_ids']}
 return {'rules':out,'by_run':by_run,'tests':m,'runs':len(runs),'survivors':sum(1 for r in out if r['status']=='tested_significant'),'fdr_q':FDR_Q,'min_oos':MIN_OOS}


def next_session_dte(expiry:str,as_of:str)->int:
 """Calendar days to expiry from the NEXT session after the reading - the Lab enters at the next open after its
 decision. Weekends are skipped; exchange holidays are not known here (conservative by at most the holiday)."""
 d=date.fromisoformat(as_of[:10])+timedelta(days=1)
 while d.weekday()>=5:d+=timedelta(days=1)
 return (date.fromisoformat(expiry)-d).days


def for_candidate(b:Dict[str,Any],template:str,param,dte:Optional[int])->Optional[Dict[str,Any]]:
 """Evidence for the rule Discover shows (template, width, held to expiry). Several schedules may match; only those
 whose DTE range covers this expiry apply, and the most conservative of them decides."""
 same=[r for r in b['rules'] if r['template']==template and r['param']==param and not any(v is not None for v in r['exits'].values())]
 if not same:return None
 fits=[r for r in same if dte is not None and r['dte'][0]<=dte<=r['dte'][1]]
 m=b['tests']
 def note(r):
  wd='every day' if r['weekday']=='daily' else WEEKDAYS[int(r['weekday'])]
  return (f"Lab rule: decisions on {wd}, {r['dte'][0]}-{r['dte'][1]} days to expiry, held to expiry, slippage {(r['slippage'] or 0)*100:.1f}%; "
          f"{r['runs']} run(s) of it, the most conservative decides; {m} distinct rule(s) in your Lab corrected together (Benjamini-Hochberg, FDR {int(FDR_Q*100)}%). "
          "The Lab placed strikes by India VIX; this card uses today's chain.")
 if not fits:
  r=max(same,key=lambda x:x['latest'])
  return {'status':'model_only','label':'Model only - Lab conditions differ','rule':r['rule'],'tests':m,
   'note':f"The Lab tested {r['dte'][0]}-{r['dte'][1]} days to expiry (counted from the next session); this expiry is {dte}. "+note(r)}
 order={'insufficient':0,'tested_not_significant':1,'tested_significant':2}
 r=min(fits,key=lambda x:(order[x['status']],-(x['p'] or 0)))              # the most conservative applicable schedule
 wd='daily' if r['weekday']=='daily' else WEEKDAYS[int(r['weekday'])]
 base={'rule':r['rule'],'run_id':r['deciding_run'],'n_oos':r['n_oos'],'mean_oos':r['mean_oos'],'p':round(r['p'],4) if r['p'] is not None else None,
  'low':r['low'],'unit':r['unit'],'defined_risk':r['defined_risk'],'tests':m,'runs_of_rule':r['runs'],'note':note(r)}
 if r['status']=='insufficient':
  return {**base,'status':'insufficient','label':f"Model only - too few out-of-sample trades in the Lab (n={r['n_oos']}, need {MIN_OOS})"}
 if r['status']=='tested_significant':
  low=f", 95% low {r['low']:+.2f} {r['unit']}" if r['defined_risk'] else ''
  return {**base,'status':'tested_significant','label':f"Tested ✓ (model-priced, {wd} decisions) - n={r['n_oos']} out of sample, +₹{r['mean_oos']:,.0f}/trade after costs{low}"}
 why={'tail_undefined':'Tested - never badged: an undefined-risk structure\'s tail cannot be bounded',
  'tail_stress':'Tested - not significant once the rarely-seen maximum-loss trades are stressed'}.get(r.get('reason'),'Tested - not significant (after correcting for every rule tested)')
 return {**base,'status':'tested_not_significant','reason':r.get('reason'),'label':why}


def rank(cards:List[Dict[str,Any]])->List[Dict[str,Any]]:
 """Tier 1: Tested ✓ defined-risk rules with a positive OOS 95% low, by that low; tier 2: the model order."""
 def key(ic):
  i,c=ic;ev=c.get('evidence') or {}
  if ev.get('status')=='tested_significant' and ev.get('defined_risk') and (ev.get('low') or 0)>0:
   return (0,-ev['low'],i)
  return (1,0,i)
 return [c for _i,c in sorted(enumerate(cards),key=key)]
