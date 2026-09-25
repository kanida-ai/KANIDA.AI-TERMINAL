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
 'nifty_v1':{'name':'NIFTY defined-risk grid v1','underlying':'NIFTY','templates':'defined','weekdays':[0,1,2,3,4],
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
 for t in tpls:
  for v in ((t['param'] or {}).get('variants') or [None]):
   for wd in grid['weekdays']:
    for lo,hi in grid['dte']:
     out.append(lab.validate({'underlying':grid['underlying'],'template':t['key'],'param':v,'weekday':wd,'dte_min':lo,'dte_max':hi,
      'from':grid['from'],'to':grid.get('to') or None,'split':grid['split'],'slippage_pct':grid['slippage_pct']}))
 return out


# --- worker processes: the series are sent ONCE per process, not per rule -------------------------------------------
_W={}
def _init(series,vix,lot):
 _W['s']=series;_W['v']=vix;_W['lot']=lot


def _one(spec):
 from .lab import backtest
 try:return spec,backtest(spec,_W['s'],_W['v'],_W['lot']),None
 except Exception as e:  # noqa: BLE001 - a failed rule is reported, never dropped
  return spec,None,f'{type(e).__name__}: {e}'[:300]


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
 series,vix=lab.series_for(grid['underlying'])
 lot=lab.lot_size() if hasattr(lab,'lot_size') else 65
 done=failed=0
 with ProcessPoolExecutor(max_workers=workers or max(1,(os.cpu_count() or 2)-1),initializer=_init,initargs=(series,vix,lot)) as ex:
  futs=[ex.submit(_one,s) for s in specs]
  for f in as_completed(futs):
   spec,res,err=f.result();rid=uuid.uuid4().hex[:16];t=time.time()
   with lab.lock:
    lab.c.execute('insert into lab_runs values(?,?,?,?,?,?,?,?,?,?,?)',(rid,user_id,None,'backtest',json.dumps(spec),'failed' if err else 'completed',
     1.0,json.dumps(res) if res else None,err,t,t))
    if err:failed+=1
    else:done+=1
    lab.c.execute('update lab_batches set done=?,failed=? where id=?',(done,failed,bid));lab.c.commit()
   if on_progress:on_progress(done+failed,len(specs))
 with lab.lock:
  lab.c.execute("update lab_batches set status=?,finished_at=? where id=?",('completed' if not failed else 'completed_with_failures',time.time(),bid));lab.c.commit()
 return batch(lab,user_id,bid)


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
 fam=board['families'].get(json.loads(b['grid'])['underlying'],{})
 order={'tested_significant':0,'tested_not_significant':1,'insufficient':2}
 rules.sort(key=lambda r:(order[r['status']],r['p'] if r['p'] is not None else 2))
 return {**dict(b),'grid':json.loads(b['grid']),'family':fam,'failed_runs':[{'id':r['id'],'error':r['error']} for r in runs if r['status']=='failed'],
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
