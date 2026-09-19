"""Isolated, resumable backtest snapshots; never writes the OHLC database."""
from __future__ import annotations
import gzip
import hashlib
import json
import sqlite3
import zlib
from functools import lru_cache
from .data import ROOT,ClosingConnection
from . import catalog, performance

DB=ROOT/'output'/'backtests.sqlite3'


def dumps(value):return json.dumps(value,allow_nan=False,separators=(',',':'))


def connection():
    DB.parent.mkdir(exist_ok=True)
    con=sqlite3.connect(DB,timeout=30,factory=ClosingConnection)
    # SQL filtering and displayed ranges must agree at decimal boundaries on every SQLite version.
    con.create_function('kanida_display_return',1,performance.displayed_return,deterministic=True)
    con.execute('PRAGMA journal_mode=WAL')
    return con


def initialize():
    with connection() as con:
        con.executescript('''CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, payload TEXT);
        CREATE TABLE IF NOT EXISTS stocks (run TEXT,symbol TEXT,status TEXT,payload TEXT,PRIMARY KEY(run,symbol));
        CREATE TABLE IF NOT EXISTS cells (run TEXT,symbol TEXT,timeframe TEXT,pattern TEXT,side TEXT,status TEXT,
          episodes INTEGER,test_n INTEGER,expectancy REAL,summary TEXT,payload BLOB,PRIMARY KEY(run,symbol,timeframe,pattern,side));
        CREATE INDEX IF NOT EXISTS cell_lookup ON cells(run,pattern,timeframe,status);
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY,value TEXT);''')


def save_progress(run):
    with connection() as con:
        con.execute('INSERT OR REPLACE INTO runs VALUES(?,?)',(run['id'],dumps(run)))
        con.execute("INSERT OR REPLACE INTO settings VALUES('active_run',?)",(run['id'],))


def active_run(con):
    row=con.execute("SELECT value FROM settings WHERE key='active_run'").fetchone()
    return row[0] if row else None


def state():
    if not DB.exists():return {'status':'not_started','done':0,'total':0}
    with connection() as con:
        run=active_run(con)
        if not run:return {'status':'not_started','done':0,'total':0}
        value=json.loads(con.execute('SELECT payload FROM runs WHERE id=?',(run,)).fetchone()[0])
        if value.get('status')=='running':
            # A killed worker cannot update its persisted status. Use the
            # Windows query API: os.kill(pid, 0) is unsafe on Windows.
            import ctypes
            from ctypes import wintypes
            kernel=ctypes.WinDLL('kernel32',use_last_error=True)
            kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD]
            kernel.OpenProcess.restype=wintypes.HANDLE
            kernel.GetExitCodeProcess.argtypes=[wintypes.HANDLE,ctypes.POINTER(wintypes.DWORD)]
            kernel.CloseHandle.argtypes=[wintypes.HANDLE]
            handle=kernel.OpenProcess(0x1000,False,value.get('pid',0))
            if handle:
                try:
                    code=wintypes.DWORD()
                    if kernel.GetExitCodeProcess(handle,ctypes.byref(code)) and code.value!=259:value['status']='interrupted'
                finally:kernel.CloseHandle(handle)
            else:value['status']='interrupted'
        value['coverage']={s:n for s,n in con.execute('SELECT status,count(*) FROM cells WHERE run=? GROUP BY status',(run,))}
        value['errors']=[{'symbol':s,**json.loads(p)} for s,p in con.execute("SELECT symbol,payload FROM stocks WHERE run=? AND status='error'",(run,))]
        return value


def save_stock(run,symbol,result):
    records=[]
    for cell in result.get('cells',[]):
        summary={k:cell[k] for k in ('symbol','timeframe','pattern','pattern_name','side','status','episodes','events','bars','first_candle','last_candle','rule_description','next_expectancy_pct','historical_win_probability_pct','estimate_note')}
        summary['test']=cell['splits'].get('test',{'n':0})
        summary['reference']=cell['reference']
        records.append((run,symbol,cell['timeframe'],cell['pattern'],cell['side'],cell['status'],cell['episodes'],summary['test']['n'],
            cell['next_expectancy_pct'],dumps(summary),zlib.compress(dumps(cell).encode(),6)))
    metadata={k:v for k,v in result.items() if k!='cells'}
    with connection() as con:
        con.execute('DELETE FROM cells WHERE run=? AND symbol=?',(run,symbol))
        con.executemany('INSERT INTO cells VALUES(?,?,?,?,?,?,?,?,?,?,?)',records)
        con.execute('INSERT OR REPLACE INTO stocks VALUES(?,?,?,?)',(run,symbol,result.get('status','complete'),dumps(metadata)))


def listing(filters):
    mode, screen, screen_values = performance.sql_screen(filters)
    symbols = catalog.selected_symbols(filters)
    if not DB.exists():return {'total':0,'rows':[]}
    with connection() as con:
        run=active_run(con);conditions=['run=?'];args=[run]
        for key in ('timeframe','pattern','side','status'):
            if filters.get(key):conditions.append(key+'=?');args.append(filters[key])
        if filters.get('symbol'):
            conditions.append('instr(symbol,?)>0');args.append(filters['symbol'].upper())
        if filters.get('occurrences')=='true':conditions.append('episodes>0')
        if symbols is not None:
            conditions.append('symbol IN ('+','.join('?' for _ in symbols)+')' if symbols else '0')
            args.extend(symbols)
        conditions.extend(screen);args.extend(screen_values)
        where=' AND '.join(conditions)
        total=con.execute('SELECT count(*) FROM cells WHERE '+where,args).fetchone()[0]
        metric=lambda field: f"json_extract(summary,'$.{mode}.{field}')"
        order={'expectancy':metric('expectancy_pct')+' DESC,'+metric('n')+' DESC',
               'win_rate':metric('win_rate')+' DESC,'+metric('n')+' DESC',
               'sample':metric('n')+' DESC,episodes DESC','episodes':'episodes DESC'}.get(filters.get('sort'),'symbol')
        order+=',symbol,timeframe,pattern,side'
        limit=max(1,min(200,int(filters.get('limit') or 60)));offset=max(0,int(filters.get('offset') or 0))
        rows=[json.loads(r[0]) for r in con.execute('SELECT summary FROM cells WHERE '+where+' ORDER BY '+order+' LIMIT ? OFFSET ?',args+[limit,offset])]
        return {'run':run,'total':total,'rows':catalog.annotate([performance.decorate(row) for row in rows]),'offset':offset,'limit':limit,'mode':mode}


def match_history(matches):
    """Only the same stock, pattern, timeframe and eligible direction can supply evidence."""
    result=catalog.annotate(matches)
    for match in result:match['history']=[]
    if not DB.exists():return result
    with connection() as con:
        run=active_run(con)
        for match in result:
            sides={'bullish':('long',),'bearish':('short',),'neutral':('long','short')}.get(match.get('direction'),())
            for side in sides:
                row=con.execute('SELECT summary FROM cells WHERE run=? AND symbol=? AND timeframe=? AND pattern=? AND side=?',
                    (run,match['symbol'],match['timeframe'],match['pattern'],side)).fetchone()
                if row:match['history'].append(performance.compact(json.loads(row[0]),run))
    return result


def cell(symbol,timeframe,pattern,side):
    with connection() as con:
        run=active_run(con)
        row=con.execute('SELECT payload FROM cells WHERE run=? AND symbol=? AND timeframe=? AND pattern=? AND side=?',(run,symbol,timeframe,pattern,side)).fetchone()
        if not row:return None
        result=json.loads(zlib.decompress(row[0]))
        metadata=json.loads(con.execute('SELECT payload FROM runs WHERE id=?',(run,)).fetchone()[0])
        result.update(run_id=run,assumptions=metadata['rules'])
        return result


def history_path(run,symbol):
    return ROOT/'output'/'history'/run/(hashlib.sha256(symbol.encode()).hexdigest()+'.json.gz')


def save_history(run,symbol,frames):
    path=history_path(run,symbol);path.parent.mkdir(parents=True,exist_ok=True)
    raw=dumps(frames).encode();digest=hashlib.sha256(raw).hexdigest()
    path.write_bytes(gzip.compress(raw,compresslevel=3))
    return digest


@lru_cache(maxsize=2)
def load_history(run,symbol):
    path=history_path(run,symbol)
    return json.loads(gzip.decompress(path.read_bytes())) if path.exists() else None


def chart(symbol,timeframe,signal,pattern='',side='',segment='reference'):
    from .detectors import detect
    with connection() as con:run=active_run(con)
    history=load_history(run,symbol)
    if not history or timeframe not in history:return None
    bars=history[timeframe][0]
    if not 39<=signal<len(bars):return None
    begin=max(0,signal-259);window=bars[begin:signal+1]
    matches=detect(window);trade=None
    if pattern and side:
        study=cell(symbol,timeframe,pattern,side)
        if study:
            ledger=study['reference_trades'] if segment=='reference' else [t for t in study['trades'] if t['split']==segment]
            recorded=next((t for t in ledger if t['signal_index']==signal),None)
            if recorded:
                trade={**recorded,'entry_chart_index':recorded['entry_index']-begin,'exit_chart_index':recorded['exit_index']-begin,
                       'signal_chart_index':signal-begin,'side':side,'segment':segment}
                window=bars[begin:recorded['exit_index']+1]
    return {'symbol':symbol,'stock':{'company':'Historical signal · '+bars[signal]['end']},'timeframe':timeframe,'bars':window,'bar_count':len(window),
            'matches':matches,'trade':trade,'current':False,'last_candle':window[-1]['end'],'source':'Frozen backtest history',
            'status':'scanned','quality':history[timeframe][1]}
