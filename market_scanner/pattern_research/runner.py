"""Resumable multi-process research over verified frozen local OHLCV histories."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib
import json
import math
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from . import store

PACKAGE = Path(__file__).resolve().parents[1]
MODULES = ('legacy','candlesticks','chart_patterns','price_action','harmonics')


def stamp():
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def readonly(path):
    con = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=30)
    con.execute('PRAGMA query_only=ON')
    try:
        yield con
    finally:
        con.close()


def key(spec):
    return spec['pattern_id'],spec['variant'],spec['side']


def registry(names):
    modules = [importlib.import_module('.'+name,__package__) for name in names]
    specs = []
    for module in modules:
        if not callable(getattr(module,'detect',None)):
            raise ValueError('Missing detector implementation: '+module.__name__)
        for spec in module.specifications():
            if spec['side'] not in ('long','short') or not spec.get('definition_version'):
                raise ValueError('Invalid specification: '+str(spec))
            if not spec.get('states') or not set(spec['states']) <= {'setup','confirmed'}:
                raise ValueError('Unsupported detector states: '+str(spec))
            specs.append(spec)
    identities = [key(spec) for spec in specs]
    if len(identities)!=len(set(identities)):
        raise ValueError('Duplicate detector study keys')
    return modules,specs


def validate_events(groups, specs, bars):
    """Fail unsupported/malformed output instead of storing false no-occurrence results."""
    allowed = {key(spec):spec for spec in specs}
    for identity,events in groups.items():
        if identity not in allowed:
            raise ValueError('Unregistered detector output: '+str(identity))
        previous = -1
        seen = set()
        for event in events:
            i = event['signal_index']
            if not isinstance(i,int) or not 0 <= i < len(bars) or i < previous:
                raise ValueError('Invalid or unsorted signal index: '+str(identity))
            previous=i
            if event['state'] not in allowed[identity]['states']:
                raise ValueError('Undeclared signal state: '+str(identity))
            event_key=(i,event['episode'],event['state'])
            if event_key in seen:
                raise ValueError('Duplicate event: '+str((identity,event_key)))
            seen.add(event_key)
            if event.get('formation_start_index',i)>i or event.get('detected_index',i)>i:
                raise ValueError('Event contains unavailable future formation: '+str(identity))
            if not math.isfinite(event['atr']) or event['atr']<=0 or not math.isfinite(event['score']):
                raise ValueError('Invalid signal measures: '+str(identity))
            if event['direction'] not in ('bullish','bearish','neutral'):
                raise ValueError('Invalid signal direction: '+str(identity))
            if event['pattern_start'] > bars[i]['end']:
                raise ValueError('Future pattern start: '+str(identity))
    store.dumps([{ 'key':list(k),'events':v} for k,v in groups.items()])


def code_files():
    # Include transitive local dependencies, not credentials, output or environment files.
    # outcomes*.py reads finished research (post-hoc "what happened next" evidence) and never affects
    # detection, evaluation or storage, so it stays out of the run's code identity.
    files=list(PACKAGE.glob('*.py'))+[p for p in (PACKAGE/'pattern_research').glob('*.py') if not p.name.startswith('outcomes')]
    files += [PACKAGE/name for name in ('config.json','backtest_rules.json','RULES.md','BACKTESTING.md','STUDIES.md')]
    files += [PACKAGE/'pattern_research'/'requirements.txt']
    return sorted(files)


def dependencies():
    import numpy, numba, talib
    return dict(python=sys.version,numpy=numpy.__version__,numba=numba.__version__,
                talib=talib.__version__,ta_core=str(talib.__ta_version__))


def verify_snapshot(path, manifest):
    frozen=Path(path).resolve().parent/'code'/'market_scanner'
    if PACKAGE.resolve()!=frozen:
        raise ValueError('Execute this manifest from its frozen code directory')
    for relative,digest in manifest['code_hashes'].items():
        if hashlib.sha256((frozen/relative).read_bytes()).hexdigest()!=digest:
            raise ValueError('Frozen code integrity failure: '+relative)
    if dependencies()!=manifest['dependencies']:
        raise ValueError('Research dependency versions differ from the frozen manifest')
    if registry(manifest['modules'])[1]!=manifest['specifications']:
        raise ValueError('Research registry differs from the frozen manifest')


def prepare(args):
    modules,specs=registry(args.modules)
    with readonly(PACKAGE/'output'/'backtests.sqlite3') as con:
        source_run=con.execute("SELECT value FROM settings WHERE key='active_run'").fetchone()[0]
        metadata=json.loads(con.execute('SELECT payload FROM runs WHERE id=?',(source_run,)).fetchone()[0])
        stocks={symbol:json.loads(payload) for symbol,payload in con.execute(
            "SELECT symbol,payload FROM stocks WHERE run=? AND status='complete' ORDER BY symbol",(source_run,))}
    selected=sorted(set(args.symbols) if args.symbols else stocks)
    if not selected or set(selected)-stocks.keys():
        raise ValueError('Unknown or empty frozen-stock selection: '+str(set(selected)-stocks.keys()))
    blobs={str(p.relative_to(PACKAGE)):p.read_bytes() for p in code_files()}
    files={relative:hashlib.sha256(raw).hexdigest() for relative,raw in blobs.items()}
    catalogue=json.loads((PACKAGE.parent/'docs'/'PATTERN_CATALOGUE_PROPOSAL.json').read_text(encoding='utf-8'))
    if not args.symbols and set(args.modules)==set(MODULES):
        validation=json.loads((PACKAGE/'output'/'expanded_research'/'independent_validation.json').read_text(encoding='utf-8'))
        if not validation.get('succeeded') or set(validation['pattern_ids'])!={e['id'] for e in catalogue['entries']}:
            raise ValueError('Full-universe research requires successful independent catalogue validation')
        for name,digest in validation['code_hashes'].items():
            if files.get(str(Path('pattern_research')/name))!=digest:
                raise ValueError('Code changed after independent validation: '+name)
    source_hashes={s:stocks[s]['history_sha256'] for s in selected}
    experiment=dict(source_run=source_run,history_hashes=source_hashes,symbols=selected,
        modules=args.modules,specifications=specs,code_hashes=files,dependencies=dependencies(),
        catalogue_ids=[e['id'] for e in catalogue['entries']],source_universe_count=len(stocks),
        timeframes=['1H','4H','1D','1W'],protocol='expansion-v1-36m-train-6m-test-min20-40bps')
    run=hashlib.sha256(store.dumps(experiment).encode()).hexdigest()[:20]
    output=Path(args.output).resolve()
    folder=output/run
    folder.mkdir(parents=True,exist_ok=True)
    frozen=folder/'code'/'market_scanner'
    for relative,digest in files.items():
        dst=frozen/relative;dst.parent.mkdir(parents=True,exist_ok=True)
        if dst.exists() and hashlib.sha256(dst.read_bytes()).hexdigest()!=digest:
            raise ValueError('Frozen code changed: '+relative)
        if not dst.exists():
            dst.write_bytes(blobs[relative])
    histories={}
    for symbol in selected:
        filename=hashlib.sha256(symbol.encode()).hexdigest()+'.json.gz'
        src=PACKAGE/'output'/'history'/source_run/filename
        dst=folder/'history'/filename
        dst.parent.mkdir(exist_ok=True)
        if not dst.exists():
            shutil.copyfile(src,dst)
        histories[symbol]=str(dst)
    manifest=dict(experiment,id=run,output=str(output),histories=histories,workers=args.workers,
        prepared_at=stamp(),source_market_latest=metadata.get('source_latest'),
        source_description='Verified frozen aggregation of existing db/kanida.db; no vendor calls',
        status='prepared',done=0,total=len(selected),errors=0)
    path=folder/'manifest.json'
    store.atomic_json(path,manifest)
    print(store.dumps(dict(prepared=run,total=len(selected),study_cells_per_timeframe=len(specs),manifest=str(path))),flush=True)
    return path


def process_stock(manifest, symbol):
    from .evaluation import evaluate_cell
    started=time.monotonic()
    raw=gzip.decompress(Path(manifest['histories'][symbol]).read_bytes())
    if hashlib.sha256(raw).hexdigest()!=manifest['history_hashes'][symbol]:
        raise ValueError('Frozen OHLCV history integrity failure: '+symbol)
    frames=json.loads(raw)
    modules,specs=registry(manifest['modules'])
    cells=[];coverage={};timings={}
    for tf in manifest['timeframes']:
        bars,quality=frames.get(tf,([],{}))
        coverage[tf]=dict(bars=len(bars),quality=quality,first=bars[0]['time'] if bars else None,
                          last=bars[-1]['end'] if bars else None)
        groups={}
        for module in modules:
            start=time.monotonic()
            found=module.detect(bars,tf)
            validate_events(found,module.specifications(),bars)
            for identity,events in found.items():
                if identity in groups:
                    raise ValueError('Module collision: '+str(identity))
                groups[identity]=events
            timings[tf+':'+module.__name__.split('.')[-1]]=round(time.monotonic()-start,3)
        for spec in specs:
            events=groups.get(key(spec),[])
            evaluation=evaluate_cell(bars,events,tf,spec['side'],spec['states'])
            n=evaluation['walkforward']['stats'].get('n',0)
            mean=evaluation['walkforward']['stats'].get('expectancy_pct')
            if not bars:
                status='missing_data'
            elif not events and len(bars)<spec.get('minimum_history',spec.get('lookback',40)):
                status='insufficient_history'
            elif not events:
                status='no_occurrences'
            elif not evaluation['folds']:
                status='insufficient_walkforward_history'
            elif n==0:
                status='no_walkforward_trades'
            elif n<20:
                status='small_walkforward_sample'
            else:
                status='tested_positive' if mean is not None and mean>0 else 'tested_nonpositive'
            cells.append(dict(spec,timeframe=tf,status=status,occurrences=len({e['episode'] for e in events}),
                observations=len(events),occurrence_events=events,evaluation=evaluation))
    result=dict(symbol=symbol,run=manifest['id'],source_run=manifest['source_run'],
        history_sha256=manifest['history_hashes'][symbol],coverage=coverage,timings=timings,
        seconds=round(time.monotonic()-started,2),cells=cells)
    path,digest=store.write_stock_artifact(manifest['id'],symbol,result,manifest['output'])
    return dict(symbol=symbol,path=path,digest=digest,seconds=result['seconds'],cells=len(cells))


def execute(path):
    manifest=json.loads(Path(path).read_text(encoding='utf-8'))
    verify_snapshot(path,manifest)
    con=store.connect(manifest['output'])
    run=manifest['id']
    done=set()
    for symbol,payload in con.execute("SELECT symbol,metadata FROM stocks WHERE run=? AND status='complete'",(run,)).fetchall():
        try:
            store.verify_stock(con,manifest,symbol,json.loads(payload),manifest['output'])
            done.add(symbol)
        except Exception as exc:
            store.save_error(con,run,symbol,dict(error='Resume integrity check: '+str(exc)))
    todo=[s for s in manifest['symbols'] if s not in done]
    examples=['TITAN','RELIANCE','LTTS','CEMPRO','BEL']
    todo.sort(key=lambda s:(examples.index(s) if s in examples else len(examples),s))
    state=dict(manifest,status='running',done=len(done),errors=0,started_at=stamp(),pid=os.getpid())
    progress=Path(manifest['output'])/run/'progress.json'
    def save():
        state['updated_at']=stamp();store.save_run(con,state);store.atomic_json(progress,state)
    save()
    try:
        with ProcessPoolExecutor(max_workers=manifest['workers']) as pool:
            pending={};remaining=iter(todo)
            def submit_next():
                symbol=next(remaining,None)
                if symbol is not None:
                    pending[pool.submit(process_stock,manifest,symbol)]=symbol
            for _ in range(min(len(todo),manifest['workers']*2)):
                submit_next()
            while pending:
                future=next(as_completed(pending));symbol=pending.pop(future)
                try:
                    result=future.result()
                    store.commit_stock(con,run,symbol,result['path'],result['digest'],manifest['output'])
                    state['last_stock_seconds']=result['seconds']
                except Exception as exc:
                    error=dict(error=str(exc),traceback=traceback.format_exc())
                    store.save_error(con,run,symbol,error);state['errors']+=1
                    state['last_error']=dict(symbol=symbol,**error)
                state['done']+=1;state['last_symbol']=symbol;save()
                print(store.dumps({k:state.get(k) for k in ('id','done','total','errors','last_symbol','last_stock_seconds')}),flush=True)
                submit_next()
        state['status']='complete_with_errors' if state['errors'] else 'complete'
    except BaseException as exc:
        state.update(status='interrupted',error=str(exc));raise
    finally:
        save();con.close()
    return 1 if state['errors'] else 0


@contextmanager
def run_lock(manifest_path):
    """Prevent duplicate coordinators for one run; OS releases lock after interruption."""
    lock_path=Path(manifest_path).parent/'run.lock'
    with lock_path.open('a+b') as handle:
        if os.fstat(handle.fileno()).st_size==0:
            handle.write(b'0');handle.flush()
        handle.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError('This research run already has an active coordinator') from exc
        try:
            yield
        finally:
            handle.seek(0)
            if os.name=='nt':
                msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)
            else:
                fcntl.flock(handle,fcntl.LOCK_UN)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbols',nargs='+')
    parser.add_argument('--modules',nargs='+',choices=MODULES,default=list(MODULES))
    parser.add_argument('--workers',type=int,default=4)
    parser.add_argument('--output',default=str(store.ROOT))
    parser.add_argument('--prepare-only',action='store_true')
    parser.add_argument('--execute-manifest')
    args=parser.parse_args()
    if args.execute_manifest:
        path=Path(args.execute_manifest).resolve()
        frozen=path.parent/'code'/'market_scanner'
        if PACKAGE.resolve()!=frozen:
            return subprocess.call([sys.executable,'-m','market_scanner.pattern_research.runner',
                                    '--execute-manifest',str(path)],cwd=str(frozen.parent))
        with run_lock(args.execute_manifest):
            return execute(args.execute_manifest)
    if not 1<=args.workers<=8:
        parser.error('workers must be between 1 and 8')
    path=prepare(args)
    if args.prepare_only:
        return 0
    # Spawn from the frozen code root; process-pool children inherit that code too.
    return subprocess.call([sys.executable,'-m','market_scanner.pattern_research.runner',
                            '--execute-manifest',str(path)],cwd=str(path.parent/'code'))


if __name__=='__main__':
    raise SystemExit(main())
