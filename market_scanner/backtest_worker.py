"""Run all active database stocks, all enabled patterns, all four timeframes."""
from __future__ import annotations
import argparse
from concurrent.futures import ProcessPoolExecutor,as_completed
from datetime import datetime
import hashlib
import json
import os
import time
import traceback
from .data import ROOT,load_config,connect_source,universe,now_ist
from .historical import HistoricalReplay,prepare_stock
from .backtest import episodes,mine_cell,rules_config
from .backtest_store import initialize,connection,save_progress,save_stock,save_history
from .detectors import NAMES

SIDES={'cup_handle':['long'],'horizontal_breakout':['long'],'flag_pole':['long','short'],
    'symmetrical_triangle':['long','short'],'falling_wedge':['long'],'rising_wedge':['short'],
    'channel':['long','short'],'descending_triangle':['short'],'head_shoulders':['short'],'inverse_head_shoulders':['long']}


def process_stock(symbol,run,through):
    from .replay_filter import masks
    start=time.monotonic();config=load_config();rules=rules_config()
    frames=prepare_stock(symbol,config['timeframes'],datetime.fromisoformat(through))
    digest=save_history(run,symbol,frames);cells=[];info={}
    for tf,(bars,quality) in frames.items():
        replay=HistoricalReplay(bars,config['history_bars'],config['enabled_patterns'])
        mask=masks(replay.h,replay.l,replay.c,replay.v,replay.tr,replay.tops,replay.bottoms,config['history_bars'])
        groups,observations=episodes(replay,mask,rules['episode_rearm_absent_bars'])
        info[tf]={'bars':len(bars),'observations':observations,'quality':quality,'episodes':sum(len({e['episode'] for e in g}) for g in groups.values())}
        for pattern in config['enabled_patterns']:
            for side in SIDES[pattern]:
                result=mine_cell(groups.get((pattern,side),[]),bars,tf,side,rules)
                result.update(symbol=symbol,timeframe=tf,pattern=pattern,pattern_name=NAMES[pattern],side=side,quality=quality,
                    detector_version=config['pattern_version'],rules_version=rules['version'],history_sha256=digest)
                cells.append(result)
    return {'status':'complete','seconds':round(time.monotonic()-start,2),'history_sha256':digest,'timeframes':info,'cells':cells}


def run(symbols=None,workers=8,new=False):
    initialize();config=load_config();rules=rules_config()
    signature=hashlib.sha256(json.dumps([config,rules],sort_keys=True).encode())
    for file in ('backtest.py','historical.py','replay_filter.py','detectors.py','data.py'):
        signature.update((ROOT/file).read_bytes())
    run_id=signature.hexdigest()[:16]
    if new:run_id+='-'+now_ist().strftime('%Y%m%d%H%M%S')
    with connect_source(ROOT/config['database']) as source:
        source_version=source.execute('PRAGMA data_version').fetchone()[0]
        stocks=universe(source);all_symbols=[s['symbol'] for s in stocks]
        selected=[s for s in all_symbols if not symbols or s in symbols]
        if not selected:raise ValueError('No selected stocks exist in the active universe')
        with connection() as con:
            old=con.execute('SELECT payload FROM runs WHERE id=?',(run_id,)).fetchone()
            completed={s for s, in con.execute("SELECT symbol FROM stocks WHERE run=? AND status='complete'",(run_id,))}
        previous=json.loads(old[0]) if old else {}
        through=previous.get('through',now_ist().isoformat(sep=' '))
        todo=[s for s in selected if s not in completed]
        # Give the named example stocks early results, then cover the full universe.
        examples=['TITAN','BEL','ICICIBANK','MARUTI','LT','RELIANCE','HEG']
        todo.sort(key=lambda s:(examples.index(s) if s in examples else len(examples),s))
        state={**previous,'id':run_id,'status':'running','pid':os.getpid(),'done':len(selected)-len(todo),'total':len(selected),
            'through':through,'started_at':previous.get('started_at',now_ist().isoformat(sep=' ')),
            'updated_at':now_ist().isoformat(sep=' '),'rules':rules,'detector_version':config['pattern_version'],
            'scope':'All active NSE equities in supplied database' if symbols is None else 'Selected pilot stocks',
            'source_latest':max((s['daily_latest'] or '' for s in stocks),default=''),'source_changed_during_run':False,
            'failed':0,'workers':workers}
        save_progress(state);start=time.monotonic()
        try:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures={pool.submit(process_stock,s,run_id,through):s for s in todo}
                for future in as_completed(futures):
                    symbol=futures.pop(future)
                    try:result=future.result()
                    except Exception as error:
                        result={'status':'error','error':str(error),'traceback':traceback.format_exc()};state['failed']+=1
                    save_stock(run_id,symbol,result)
                    state.update(done=state['done']+1,symbol=symbol,updated_at=now_ist().isoformat(sep=' '),
                        elapsed_seconds=round(time.monotonic()-start,1),source_changed_during_run=source.execute('PRAGMA data_version').fetchone()[0]!=source_version)
                    save_progress(state)
                    print(json.dumps({k:state[k] for k in ('done','total','symbol','failed','elapsed_seconds')}),flush=True)
            state['status']='complete_with_errors' if state['failed'] else 'complete'
        except BaseException as error:
            state.update(status='interrupted',error=str(error));raise
        finally:
            state['updated_at']=now_ist().isoformat(sep=' ');save_progress(state)
    return state


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--symbols',nargs='*');p.add_argument('--workers',type=int,default=8)
    p.add_argument('--new',action='store_true',help='Start a fresh snapshot instead of resuming matching rules and code')
    a=p.parse_args()
    # Windows releases this advisory lock even if the worker is interrupted.
    import msvcrt
    lock_path=ROOT/'output'/'backtest.lock';lock_path.parent.mkdir(exist_ok=True)
    with lock_path.open('a+b') as lock:
        lock.seek(0)
        if not lock.read(1):lock.write(b'0');lock.flush()
        lock.seek(0)
        try:msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
        except OSError:raise SystemExit('A KANIDA historical worker is already running')
        try:run(a.symbols,a.workers,a.new)
        finally:
            lock.seek(0);msvcrt.locking(lock.fileno(),msvcrt.LK_UNLCK,1)
