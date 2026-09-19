"""Single-writer expansion storage, isolated from source and production databases."""
from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / 'output' / 'expanded_research'


def dumps(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':'))


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(dumps(value), encoding='utf-8')
    temporary.replace(path)


def connect(root=ROOT):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(root / 'research.sqlite3', timeout=60)
    con.execute('PRAGMA journal_mode=WAL')
    con.execute('PRAGMA foreign_keys=ON')
    con.executescript('''
      CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY, status TEXT NOT NULL, metadata TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS stocks(run TEXT NOT NULL, symbol TEXT NOT NULL, status TEXT NOT NULL,
        metadata TEXT NOT NULL, PRIMARY KEY(run,symbol));
      CREATE TABLE IF NOT EXISTS cells(run TEXT NOT NULL,symbol TEXT NOT NULL,timeframe TEXT NOT NULL,
        pattern_id TEXT NOT NULL,variant TEXT NOT NULL,side TEXT NOT NULL,status TEXT NOT NULL,
        occurrences INTEGER NOT NULL,reference_n INTEGER NOT NULL,wf_n INTEGER NOT NULL,
        reference_mean REAL,wf_mean REAL,summary TEXT NOT NULL,artifact TEXT NOT NULL,
        PRIMARY KEY(run,symbol,timeframe,pattern_id,variant,side));
      CREATE INDEX IF NOT EXISTS result_lookup ON cells(run,pattern_id,timeframe,status);
    ''')
    return con


def save_run(con, metadata):
    con.execute('INSERT OR REPLACE INTO runs VALUES(?,?,?)',
                (metadata['id'], metadata['status'], dumps(metadata)))
    con.commit()


def stock_artifact(run, symbol, root=ROOT):
    return Path(root) / run / 'stocks' / (hashlib.sha256(symbol.encode()).hexdigest() + '.json.gz')


def write_stock_artifact(run, symbol, result, root=ROOT):
    path = stock_artifact(run, symbol, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = dumps(result).encode('utf-8')
    temporary = path.with_suffix('.tmp')
    temporary.write_bytes(gzip.compress(raw, compresslevel=3))
    temporary.replace(path)
    return str(path), hashlib.sha256(raw).hexdigest()


def commit_stock(con, run, symbol, path, digest, root=ROOT):
    raw=gzip.decompress(Path(path).read_bytes())
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError('Stock artifact digest mismatch: ' + symbol)
    result=json.loads(raw)
    if result.get('run')!=run or result.get('symbol')!=symbol:
        raise ValueError('Stock artifact identity mismatch: '+symbol)
    records = []
    relative = Path(path).relative_to(Path(root)).as_posix()
    for cell in result['cells']:
        summary = {k:v for k,v in cell.items() if k not in ('evaluation','occurrence_events')}
        evaluation = cell['evaluation']
        summary['reference'] = evaluation['reference']['stats']
        summary['walkforward'] = evaluation['walkforward']['stats']
        summary['fold_count'] = len(evaluation['folds'])
        ref, wf = evaluation['reference']['stats'], evaluation['walkforward']['stats']
        records.append((run,symbol,cell['timeframe'],cell['pattern_id'],cell['variant'],cell['side'],
            cell['status'],cell['occurrences'],ref.get('n',0),wf.get('n',0),
            ref.get('expectancy_pct'),wf.get('expectancy_pct'),dumps(summary),relative))
    metadata = {k:v for k,v in result.items() if k != 'cells'}
    metadata.update(artifact=relative,artifact_sha256=digest,cells=len(records))
    metadata['cell_keys_sha256']=cell_keys_digest((r[2],r[3],r[4],r[5]) for r in records)
    with con:
        con.execute('DELETE FROM cells WHERE run=? AND symbol=?',(run,symbol))
        con.executemany('INSERT INTO cells VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',records)
        con.execute('INSERT OR REPLACE INTO stocks VALUES(?,?,?,?)',
                    (run,symbol,'complete',dumps(metadata)))


def save_error(con, run, symbol, error):
    with con:
        con.execute('INSERT OR REPLACE INTO stocks VALUES(?,?,?,?)',
                    (run,symbol,'error',dumps(error)))


def verify_stock(con, manifest, symbol, metadata, root=ROOT):
    """Verify a completed artifact before resume or a complete-coverage claim."""
    expected={(tf,s['pattern_id'],s['variant'],s['side']) for tf in manifest['timeframes']
              for s in manifest['specifications']}
    stored=list(con.execute('SELECT timeframe,pattern_id,variant,side FROM cells WHERE run=? AND symbol=?',
                            (manifest['id'],symbol)))
    if metadata.get('cell_keys_sha256'):
        # The coordinator already parsed/validated these exact bytes at commit. Stream
        # the checksum to avoid decoding hundreds of MB of ledgers again per stock.
        digest=hashlib.sha256()
        with gzip.open(Path(root)/metadata['artifact'],'rb') as stream:
            for chunk in iter(lambda:stream.read(1024*1024),b''):
                digest.update(chunk)
        if digest.hexdigest()!=metadata['artifact_sha256']:
            raise ValueError('Stock artifact digest mismatch: '+symbol)
        if metadata.get('symbol')!=symbol or metadata.get('run')!=manifest['id']:
            raise ValueError('Stock artifact identity mismatch: '+symbol)
        if (metadata['cells']!=len(expected) or metadata['cell_keys_sha256']!=cell_keys_digest(expected)
                or set(stored)!=expected):
            raise ValueError('Stock study-cell coverage mismatch: '+symbol)
        if metadata.get('history_sha256')!=manifest['history_hashes'][symbol]:
            raise ValueError('Stock source-history identity mismatch: '+symbol)
        return True
    raw=gzip.decompress((Path(root)/metadata['artifact']).read_bytes())
    if hashlib.sha256(raw).hexdigest()!=metadata['artifact_sha256']:
        raise ValueError('Stock artifact digest mismatch: '+symbol)
    result=json.loads(raw)
    if result.get('symbol')!=symbol or result.get('run')!=manifest['id']:
        raise ValueError('Stock artifact identity mismatch: '+symbol)
    actual=[(c['timeframe'],c['pattern_id'],c['variant'],c['side']) for c in result['cells']]
    if len(actual)!=len(expected) or set(actual)!=expected or set(stored)!=expected:
        raise ValueError('Stock study-cell coverage mismatch: '+symbol)
    if result.get('history_sha256')!=manifest['history_hashes'][symbol]:
        raise ValueError('Stock source-history identity mismatch: '+symbol)
    return True


def cell_keys_digest(keys):
    return hashlib.sha256(dumps(sorted(keys)).encode('utf-8')).hexdigest()
