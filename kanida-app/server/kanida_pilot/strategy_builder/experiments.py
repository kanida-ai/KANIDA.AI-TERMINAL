"""Experiment batches: a PRE-REGISTERED grid of Lab rules, run once, every result reported.

Pre-registration: the grid, its planned rule list and a hash are written BEFORE the first run. A batch is never re-run
to replace results; a new grid is a new batch, and every rule it contains joins the same evidence family (the
per-underlying Benjamini-Hochberg correction in evidence.py), so a larger grid makes each rule HARDER to pass.
Runs are ordinary Lab backtests (same model, costs, split rules), stored as completed lab_runs tagged with the batch id.

CLI (runs against a pilot's strategy-builder store; safe while the pilot runs - SQLite WAL):
  python -m kanida_pilot.strategy_builder.experiments --db var/strategy_builder.db --user <owner id> --grid nifty_v1
"""
from __future__ import annotations
import argparse,hashlib,json,logging,os,time,uuid
from concurrent.futures import ProcessPoolExecutor,as_completed
from typing import Any,Dict,List,Optional

from .templates import TEMPLATES

log=logging.getLogger('strategy_builder.experiments')

GRIDS={
 'stocks_v1':{'name':'F&O stocks defined-risk grid v1','underlying':'STOCKS','templates':['long_call','long_put','bull_call_spread','bear_put_spread','bull_put_spread','bear_call_spread','long_straddle','long_strangle','iron_condor','iron_butterfly'],'weekdays':[2],
  'dte':[[5,14],[15,35]],'from':'2016-01-01','to':'2026-07-29','split':'2022-01-03','slippage_pct':1.0,'exit_dte':2,'min_history_from':'2016-06-30',
  'why':'Every defined-risk template x width on every F&O stock (today\'s list) with daily history from mid-2016, Wednesday decisions, two '
        'days-to-expiry windows, exit at the open of the session before expiry (physical settlement), 1% slippage. Out of sample from '
        '2022-01-03. All stock rules form ONE evidence family.'},
 'nifty_v1':{'name':'NIFTY defined-risk grid v1','underlying':'NIFTY','templates':['long_call','long_put','bull_call_spread','bear_put_spread','bull_put_spread','bear_call_spread','long_straddle','long_strangle','iron_condor','iron_butterfly'],'weekdays':[0,1,2,3,4],
  'dte':[[1,7],[8,14],[15,35]],'from':'2019-03-01','to':None,'split':'2023-01-02','slippage_pct':0.5,
  'why':'Every defined-risk template x width x decision weekday x days-to-expiry window, held to expiry. Weekly options '
        'exist for the whole period (from 2019-02-11); out of sample from 2023-01-02.'},
}

SCHEMA='''
create table if not exists lab_batches(
 id text primary key, user_id text not null, grid_key text not null, name text not null, grid text not null, plan_hash text not null,
 planned integer not null, done integer not null default 0, failed integer not null default 0, status text not null,
 created_at real not null, started_at real, finished_at real, error text);
'''


def plan(lab,grid:Dict[str,Any])->List[Dict[str,Any]]:
 """The exact rule list (validated specs) a grid expands to - deterministic order."""
 tpls=[t for t in TEMPLATES if (grid['templates']=='defined' and t['risk']=='defined') or (isinstance(grid['templates'],list) and t['key'] in grid['templates'])]
 out=[]
 unders=universe(lab,grid) if grid['underlying']=='STOCKS' else [grid['underlying']]
 for u in unders:
  out+=_plan_one(lab,grid,tpls,u)
 return out


def universe(lab,grid)->List[str]:
 """Today's F&O stocks that have kanida.db daily history starting on/before grid['min_history_from']."""
 import sqlite3
 c=sqlite3.connect(f'file:{lab.daily.kanida_db}?mode=ro',uri=True,timeout=20)
 have={r[0] for r in c.execute("select symbol from ohlc_daily group by symbol having min(substr(bar_time,1,10))<=?",(grid.get('min_history_from','2016-06-30'),))}
 c.close()
 return sorted(lab.stocks()&have)


def _plan_one(lab,grid,tpls,u):
 out=[]
 for t in tpls:
  for v in ((t['param'] or {}).get('variants') or [None]):
   for wd in grid['weekdays']:
    for lo,hi in grid['dte']:
     out.append(lab.validate({'underlying':u,'template':t['key'],'param':v,'weekday':wd,'dte_min':lo,'dte_max':hi,
      'from':grid['from'],'to':grid.get('to') or None,'split':grid['split'],'slippage_pct':grid['slippage_pct'],'exit_dte':grid.get('exit_dte')}))
 return out


# --- worker processes: the series are sent ONCE per process, not per rule -------------------------------------------
_W={}
def _init(kanida_db,nifty,vix):
 _W['k']=kanida_db;_W['n']=nifty;_W['v']=vix


def _series(u,special):
 """Worker-side data: the underlying's cleaned daily series and its volatility series (NIFTY: India VIX)."""
 from .lab import Daily,clean_series,scaled_vol
 if u=='NIFTY':return _W['n'],_W['v']
 d=Daily(_W['k'],None,None);s=clean_series(d.series(u),special)
 return s,scaled_vol(s,_W['n'],_W['v'])


def _group(args):
 """All rules of ONE underlying in one task: its data is loaded once."""
 u,specs,lot,special=args
 from .lab import backtest
 try:s,v=_series(u,set(special))
 except Exception as e:  # noqa: BLE001
  return [(uuid.uuid4().hex[:16],sp,None,f'NO_DATA {type(e).__name__}: {e}'[:300],None) for sp in specs]
 from .evidence import entry
 out=[]
 for sp in specs:
  rid=uuid.uuid4().hex[:16]
  try:
   res=compact(backtest(sp,s,v,lot))
   out.append((rid,sp,res,None,entry(rid,sp,res,time.time())))
  except Exception as e:  # noqa: BLE001 - a failed rule is reported, never dropped
   out.append((rid,sp,None,f'{type(e).__name__}: {e}'[:300],None))
 return out


def compact(res):
 """A batch run keeps everything the evidence and the summary need - stats, badge, control, provenance and per-trade
 entry/exit/net/risk - and drops per-leg detail and the equity curve (ten thousand full results would be ~0.5 GB)."""
 return {**{k:v for k,v in res.items() if k not in ('trades','equity')},'compact':True,
  'trades':[{k:t.get(k) for k in ('decision','entry','exit','expiry','reason','net','fees','capital_at_risk','hold_days')} for t in res.get('trades') or []]}


def run_batch(lab,user_id:str,grid_key:str,workers:Optional[int]=None,on_progress=None)->Dict[str,Any]:
 grid=GRIDS.get(grid_key)
 if not grid:raise ValueError(f'unknown grid {grid_key}')
 with lab.lock:lab.c.executescript(SCHEMA);lab.c.commit()
 specs=plan(lab,grid);bid='B'+uuid.uuid4().hex[:15]
 for s in specs:s['batch']=bid
 ph=hashlib.sha256(json.dumps([{k:v for k,v in s.items() if k!='batch'} for s in specs],sort_keys=True).encode()).hexdigest()
 t0=time.time()
 with lab.lock:                                                    # pre-registration: written before anything runs
  lab.c.execute('insert into lab_batches values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(bid,user_id,grid_key,grid['name'],json.dumps({**grid,'rules':len(specs)}),ph,
   len(specs),0,0,'running',t0,t0,None,None));lab.c.commit()
 nifty,vix=lab.series_for('NIFTY')
 special=sorted(lab.daily.special_sessions())
 by_u={}
 for sp in specs:by_u.setdefault(sp['underlying'],[]).append(sp)
 lots={u:(lab.lot_size(u) if hasattr(lab,'lot_size') else 65) for u in by_u}
 # NIFTY has one underlying: split its rules into chunks so every worker gets some; stocks are one task per stock
 tasks=[]
 for u,sps in by_u.items():
  n=max(1,len(sps)//((os.cpu_count() or 2)*2)) if len(by_u)==1 else len(sps)
  for i in range(0,len(sps),n):tasks.append((u,sps[i:i+n],lots[u],special))
 try:
  done,failed=_execute(lab,user_id,bid,specs,tasks,workers,nifty,vix,on_progress)
 except Exception as e:  # noqa: BLE001 - a crashed batch is recorded as failed, never left 'running'
  with lab.lock:
   lab.c.execute("update lab_batches set status='failed',finished_at=?,error=? where id=?",(time.time(),f'{type(e).__name__}: {e}'[:300],bid));lab.c.commit()
  raise
 with lab.lock:
  lab.c.execute("update lab_batches set status=?,finished_at=? where id=?",('completed' if not failed else 'completed_with_failures',time.time(),bid));lab.c.commit()
 return batch(lab,user_id,bid)


def _execute(lab,user_id,bid,specs,tasks,workers,nifty,vix,on_progress):
 done=failed=0
 with ProcessPoolExecutor(max_workers=workers or max(1,(os.cpu_count() or 2)-1),initializer=_init,initargs=(lab.daily.kanida_db,nifty,vix)) as ex:
  futs=[ex.submit(_group,tk) for tk in tasks]
  for f in as_completed(futs):
   for rid,spec,res,err,ev in f.result():
    t=time.time()
    with lab.lock:
     lab.c.execute('insert into lab_runs values(?,?,?,?,?,?,?,?,?,?,?)',(rid,user_id,None,'backtest',json.dumps(spec),'failed' if err else 'completed',
      1.0,json.dumps(res) if res else None,err,t,t))
     if ev:lab.c.execute('insert or replace into lab_evidence values(?,?)',(rid,json.dumps({**ev,'created_at':t})))
     if err:failed+=1
     else:done+=1
    if on_progress:on_progress(done+failed,len(specs))
   with lab.lock:lab.c.execute('update lab_batches set done=?,failed=? where id=?',(done,failed,bid));lab.c.commit()
 return done,failed


STALE_RUNNING=6*3600   # a batch still 'running' this long after it started lost its worker (crash / restart)


def sweep_stale(lab)->int:
 """Mark batches whose worker died as 'abandoned' so they never sit at 'running' (quant audit F1). Returns the count."""
 with lab.lock:
  lab.c.executescript(SCHEMA)
  n=lab.c.execute("update lab_batches set status='abandoned',finished_at=?,error=coalesce(error,'worker stopped before the batch finished') "
   "where status='running' and coalesce(started_at,created_at)<?",(time.time(),time.time()-STALE_RUNNING)).rowcount
  lab.c.commit()
 return n


def batches(lab,user_id:str)->List[Dict[str,Any]]:
 with lab.lock:
  lab.c.executescript(SCHEMA)
  rows=lab.c.execute('select * from lab_batches where user_id=? order by created_at desc',(user_id,)).fetchall()
 return [{**dict(r),'grid':json.loads(r['grid'])} for r in rows]


def batch(lab,user_id:str,bid:str)->Optional[Dict[str,Any]]:
 """The batch and EVERY one of its rules with its evidence status (family = the batch's underlying, all rules tested)."""
 with lab.lock:
  lab.c.executescript(SCHEMA)
  b=lab.c.execute('select * from lab_batches where id=? and user_id=?',(bid,user_id)).fetchone()
  if not b:return None
  runs=lab.c.execute("select id,spec,status,error from lab_runs where user_id=? and spec like ?",(user_id,f'%"batch": "{bid}"%')).fetchall()
 board=lab.evidence_board(user_id)
 ids={r['id'] for r in runs}
 rules=[r for r in board['rules'] if set(r['run_ids'])&ids]
 from .evidence import family_of
 fam=board['families'].get(family_of(json.loads(b['grid'])['underlying']),{})
 order={'tested_significant':0,'tested_not_significant':1,'insufficient':2}
 rules.sort(key=lambda r:(order[r['status']],r['p'] if r['p'] is not None else 2))
 # one concise failure summary (distinct reasons, counted) - never thousands of repeated exception strings (GTM P06)
 from collections import Counter
 reasons=Counter(str(r['error'] or 'unknown error').strip().splitlines()[-1][:160] for r in runs if r['status']=='failed')
 healthy=b['status']=='completed'
 return {**dict(b),'grid':json.loads(b['grid']),'family':fam,'healthy':healthy,
  'quarantine':None if healthy else ('Still running - its rules cannot be survivors yet.' if b['status']=='running' else
   'The worker stopped before this batch finished (abandoned) - none of its rules can be a survivor; its planned rules still count as tests.' if b['status'] in ('abandoned','failed') else
   f"{b['failed']} of {b['planned']} rules failed, so no rule of this batch can be a survivor until a healthy re-run. Every planned rule still counts as a test in the multiple-testing correction."),
  'failure_summary':[{'reason':k,'count':n} for k,n in reasons.most_common(3)],'failure_kinds':len(reasons),
  'failed_runs':[{'id':r['id']} for r in runs if r['status']=='failed'][:5],
  'rules':[{k:r.get(k) for k in ('template','param','weekday','dte','n_oos','mean_oos','p','low','stress','status','reason','runs','tests','unit','defined_risk')} for r in rules],
  'counts':{s:sum(1 for r in rules if r['status']==s) for s in order}}


def main(argv=None):
 ap=argparse.ArgumentParser();ap.add_argument('--db',required=True);ap.add_argument('--user',required=True);ap.add_argument('--grid',required=True)
 ap.add_argument('--kanida-db',default=None);ap.add_argument('--derivatives-db',default=None);ap.add_argument('--workers',type=int,default=None)
 a=ap.parse_args(argv)
 from pathlib import Path
 from .store import Store
 from .market import Market
 from .lab import Lab
 root=Path(__file__).resolve().parents[4]
 store=Store(a.db);deriv=a.derivatives_db or str(root/'db'/'derivatives.db')
 lab=Lab(store,Market(deriv),a.kanida_db or str(root/'db'/'kanida.db'),deriv)
 t=time.time()
 out=run_batch(lab,a.user,a.grid,a.workers,on_progress=lambda i,n:print(f'\r{i}/{n}',end='',flush=True) if i%10==0 or i==n else None)
 print(f"\n{out['name']}: {out['done']} done, {out['failed']} failed in {time.time()-t:.0f}s; family: {out['family']}; counts: {out['counts']}")
 for r in out['rules'][:15]:print(' ',r['status'],r['template'],r['param'],r['weekday'],r['dte'],'n',r['n_oos'],'p',None if r['p'] is None else round(r['p'],4),'low',r['low'],r['reason'])


if __name__=='__main__':
 main()
