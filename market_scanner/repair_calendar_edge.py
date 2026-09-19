"""Create a corrected snapshot from frozen history, preserving the original.

Only the first weekly bucket predating the observed calendar is removed. Other
timeframe inputs are byte-for-byte identical and their existing results carry
forward. Weekly signals, selection and trades are recomputed in full.
"""
from concurrent.futures import ProcessPoolExecutor,as_completed
from datetime import datetime,timedelta
import hashlib
import json
import shutil
import time
import zlib
from .backtest import episodes,mine_cell,rules_config
from .backtest_store import connection,state,history_path,load_history,save_history,save_stock,save_progress,dumps
from .backtest_worker import SIDES
from .historical import HistoricalReplay
from .detectors import NAMES
from .data import ROOT,load_config,connect_source,observed_sessions,now_ist


def repair(symbol,old_run,new_run):
    from .replay_filter import masks
    frames=load_history(old_run,symbol)
    bars,quality=frames['1W'];bars=bars[1:]
    if bars:bars[0]['gap']=False
    quality={**quality,'incomplete_buckets':quality['incomplete_buckets']+1,'gaps':sum(b['gap'] for b in bars)}
    frames['1W']=[bars,quality]
    digest=save_history(new_run,symbol,frames)
    config=load_config();rules=rules_config();replay=HistoricalReplay(bars)
    mask=masks(replay.h,replay.l,replay.c,replay.v,replay.tr,replay.tops,replay.bottoms)
    groups,observations=episodes(replay,mask,rules['episode_rearm_absent_bars'])
    with connection() as con:
        info=json.loads(con.execute('SELECT payload FROM stocks WHERE run=? AND symbol=?',(old_run,symbol)).fetchone()[0])
        cells=[json.loads(zlib.decompress(r[0])) for r in con.execute("SELECT payload FROM cells WHERE run=? AND symbol=? AND timeframe!='1W'",(old_run,symbol))]
    for c in cells:c['history_sha256']=digest
    for pattern in config['enabled_patterns']:
        for side in SIDES[pattern]:
            d=mine_cell(groups.get((pattern,side),[]),bars,'1W',side,rules)
            d.update(symbol=symbol,timeframe='1W',pattern=pattern,pattern_name=NAMES[pattern],side=side,quality=quality,
                detector_version=config['pattern_version'],rules_version=rules['version'],history_sha256=digest)
            cells.append(d)
    info['timeframes']['1W']={'bars':len(bars),'observations':observations,'quality':quality,
        'episodes':sum(len({e['episode'] for e in group}) for group in groups.values())}
    info.update(history_sha256=digest,cells=cells,calendar_edge_removed=True)
    return info


def run():
    original=state();assert original['status']=='complete',original['status']
    old=original['id'];config=load_config();rules=rules_config()
    signature=hashlib.sha256(json.dumps([config,rules],sort_keys=True).encode())
    files=('backtest.py','historical.py','replay_filter.py','detectors.py','data.py')
    for f in files:signature.update((ROOT/f).read_bytes())
    new=signature.hexdigest()[:16]
    assert new!=old
    archive=ROOT/'output'/'research-code'/new;archive.mkdir(parents=True,exist_ok=True)
    for f in files+('config.json','backtest_rules.json','backtest_worker.py','backtest_store.py','repair_calendar_edge.py'):
        (archive/f).write_bytes((ROOT/f).read_bytes())
    (archive/'manifest.json').write_text(json.dumps({'run':new,'parent_run':old,'full_sha256':signature.hexdigest()},indent=2),encoding='utf-8')
    with connect_source(ROOT/config['database']) as con:first=min(observed_sessions(con))
    with connection() as con:
        symbols=[s for s, in con.execute('SELECT symbol FROM stocks WHERE run=?',(old,))]
        rows=[json.loads(r[0]) for r in con.execute("SELECT summary FROM cells WHERE run=? AND timeframe='1W' AND pattern='cup_handle' AND side='long'",(old,))]
    affected=[]
    for r in rows:
        if not r['first_candle']:continue
        day=datetime.fromisoformat(r['first_candle']).date();monday=day-timedelta(days=day.weekday())
        if monday.isoformat()<first and monday.year not in config['calendar_years']:affected.append(r['symbol'])
    print('Affected first weekly buckets:',len(affected),flush=True)
    original.update(status='correcting_calendar',calendar_correction_total=len(affected),calendar_correction_done=0)
    save_progress(original)
    with connection() as con:
        assert not con.execute('SELECT 1 FROM runs WHERE id=?',(new,)).fetchone()
        con.execute('INSERT INTO stocks SELECT ?,symbol,status,payload FROM stocks WHERE run=?',(new,old))
        con.execute('INSERT INTO cells SELECT ?,symbol,timeframe,pattern,side,status,episodes,test_n,expectancy,summary,payload FROM cells WHERE run=?',(new,old))
    for symbol in symbols:
        if symbol not in affected:
            target=history_path(new,symbol);target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(history_path(old,symbol),target)
    started=time.monotonic()
    with ProcessPoolExecutor(max_workers=8) as pool:
        futures={pool.submit(repair,s,old,new):s for s in affected}
        for future in as_completed(futures):
            symbol=futures.pop(future);save_stock(new,symbol,future.result())
            original.update(calendar_correction_done=original['calendar_correction_done']+1,symbol=symbol,updated_at=now_ist().isoformat(sep=' '))
            save_progress(original)
            if original['calendar_correction_done']%25==0:print(original['calendar_correction_done'],'/',len(affected),flush=True)
    corrected={**original,'id':new,'status':'complete','parent_run':old,'completed_at':now_ist().isoformat(sep=' '),
        'calendar_correction':'Exclude first weekly bucket whose earlier weekdays predate the observed session calendar',
        'calendar_corrected_stocks':len(affected),'calendar_correction_seconds':round(time.monotonic()-started,2)}
    save_progress(corrected)
    print(json.dumps({'run':new,'stocks':len(symbols),'weekly_studies_recalculated':len(affected)*13,
        'seconds':corrected['calendar_correction_seconds']}),flush=True)


if __name__=='__main__':run()
