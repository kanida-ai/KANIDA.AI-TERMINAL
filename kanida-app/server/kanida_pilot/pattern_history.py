"""Two-pattern, daily descriptive price history. No fitting, orders or detector execution.

The offline builder reads checksum-verified frozen artifacts. The serving class only
reads its separate cache. Gross movements are observations, never strategy profits.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median

from .errors import PilotError

VERSION = 'gross-pattern-history-1'
HORIZONS = (1, 3, 5, 10)
PATTERNS = {'CH16': 'Double Bottom', 'CDLENGULFING': 'Bullish Engulfing'}
METHOD = {
    'kind': 'descriptive_price_movement', 'reference': 'next daily candle open after the signal',
    'costs_included': False, 'training': False, 'horizons': list(HORIZONS),
    'horizon_rule': 'h=1 is reference-bar open to its close; h=10 ends at reference+9',
    'excursions': 'maximum upward and downward price excursion from reference, including the reference candle',
    'excursion_timing': 'first bar of the maximum; bar 1 is the reference candle, bar 0 means no excursion beyond reference',
    'quality_rule': 'signal/reference flags or a quality gap inside a horizon exclude that horizon; no fabricated exit',
    'pending_rule': 'a horizon without all required future candles remains pending',
    'overlap': 'occurrences can overlap; counts are not independent trials',
    'baseline': 'all daily signal bars in the exact first-to-last occurrence window, same reference and quality rules',
    'recent': 'latest five completed at the selected horizon; pending/excluded and latest detections shown separately; not similarity-ranked',
    'interpretation': 'gross historical price movement, not net profit, a forecast, or trading performance',
}


def _dump(value):
    return json.dumps(value, separators=(',', ':'), allow_nan=False)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _ro(path):
    con = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    con.execute('pragma query_only=on')
    return con


def _valid(bar):
    values = [bar.get(k) for k in ('open', 'high', 'low', 'close')]
    return all(isinstance(v, (float, int)) and math.isfinite(v) and v > 0 for v in values) and \
        bar['low'] <= min(bar['open'], bar['close']) <= max(bar['open'], bar['close']) <= bar['high']


def movement(bars, signal, horizon):
    """One fixed-horizon observation. Quality exclusions never turn into zero returns."""
    blank = dict(horizon=horizon, gross_return_pct=None, max_up_pct=None, max_down_pct=None,
                 max_up_bar=None, max_down_bar=None, reference_time=None, reference_price=None, end_time=None)
    if not 0 <= signal < len(bars):
        return dict(blank, status='quality_excluded', reason='invalid_signal_index')
    if not _valid(bars[signal]) or bars[signal].get('gap'):
        return dict(blank, status='quality_excluded', reason='signal_quality')
    entry = signal + 1
    if entry >= len(bars):
        return dict(blank, status='pending', reason='no_reference_candle')
    ref = bars[entry]
    if not _valid(ref) or ref.get('gap'):
        return dict(blank, status='quality_excluded', reason='reference_quality')
    blank.update(reference_time=ref['time'], reference_price=ref['open'])
    available = bars[entry:min(entry + horizon, len(bars))]
    if any(not _valid(b) or b.get('gap') for b in available):
        return dict(blank, status='quality_excluded', reason='quality_gap_in_window')
    if entry + horizon > len(bars):
        return dict(blank, status='pending', reason='horizon_incomplete')
    price = ref['open']
    up_index = max(range(horizon), key=lambda i: available[i]['high'])
    down_index = min(range(horizon), key=lambda i: available[i]['low'])
    return dict(blank, status='measured', reason=None, end_time=available[-1]['end'],
                gross_return_pct=round((available[-1]['close'] / price - 1) * 100, 6),
                max_up_bar=up_index + 1 if available[up_index]['high'] > price else 0,
                max_down_bar=down_index + 1 if available[down_index]['low'] < price else 0,
                max_up_pct=round(max(0, max(b['high'] for b in available) / price - 1) * 100, 6),
                max_down_pct=round(max(0, 1 - min(b['low'] for b in available) / price) * 100, 6))


def _stats(rows):
    values = [r for r in rows if r['status'] == 'measured']
    returns = [r['gross_return_pct'] for r in values]
    n = len(values)
    return dict(n=n, total=len(rows), pending=sum(r['status'] == 'pending' for r in rows),
                quality_excluded=sum(r['status'] == 'quality_excluded' for r in rows),
                up_n=sum(v > 0 for v in returns), down_n=sum(v < 0 for v in returns),
                unchanged_n=sum(v == 0 for v in returns),
                up_pct=round(sum(v > 0 for v in returns) / n * 100, 3) if n else None,
                median_gross_return_pct=round(median(returns), 6) if n else None,
                mean_gross_return_pct=round(mean(returns), 6) if n else None,
                median_max_up_pct=round(median(r['max_up_pct'] for r in values), 6) if n else None,
                median_max_down_pct=round(median(r['max_down_pct'] for r in values), 6) if n else None,
                median_bars_to_max_up=median(r['max_up_bar'] for r in values) if n else None,
                median_bars_to_max_down=median(r['max_down_bar'] for r in values) if n else None)


def build_cell(bars, events, symbol, spec, state, run, paths=None):
    """Pure computation, also used by synthetic acceptance tests."""
    identity = dict(symbol=symbol, timeframe='1D', pattern_id=spec['pattern_id'],
                    variant=spec['variant'], side='long', state=state)
    selected = sorted((e for e in events if e['state'] == state), key=lambda e: (e['signal_index'], str(e['episode'])))
    occurrences = []
    seen = set()
    for event in selected:
        sig = int(event['signal_index'])
        key = (str(event['episode']), sig, state)
        if key in seen:
            continue
        seen.add(key)
        oid = _sha(_dump([run, identity, key]).encode())[:24]
        occurrences.append(dict(id=oid, signal_index=sig, signal_time=bars[sig]['end'] if 0 <= sig < len(bars) else None,
            episode=event['episode'], formation_start_index=event.get('formation_start_index', sig),
            pattern_start=event.get('pattern_start'), detected_index=event.get('detected_index', sig),
            geometry=event.get('geometry') or {}, score=event.get('score'),
            quality_tags=event.get('quality_tags') or [],
            horizons=[paths[h][sig] if paths is not None and 0 <= sig < len(bars) else movement(bars, sig, h) for h in HORIZONS]))
    valid_signals = [o['signal_index'] for o in occurrences if 0 <= o['signal_index'] < len(bars)]
    low, high = (min(valid_signals), max(valid_signals)) if valid_signals else (None, None)
    horizons = []
    for offset, h in enumerate(HORIZONS):
        observed = _stats([o['horizons'][offset] for o in occurrences])
        baseline = _stats(paths[h][low:high + 1] if paths is not None else [movement(bars, i, h) for i in range(low, high + 1)]) if low is not None else _stats([])
        delta = observed['mean_gross_return_pct'] - baseline['mean_gross_return_pct'] if observed['n'] and baseline['n'] else None
        horizons.append(dict(horizon=h, observed=observed, baseline=baseline,
                             mean_difference_pct=round(delta, 6) if delta is not None else None))
    summary = dict(**identity, name=PATTERNS[spec['pattern_id']], definition_version=spec['definition_version'],
        definition=spec.get('definition'), occurrence_count=len(occurrences),
        first_seen=occurrences[0]['signal_time'] if occurrences else None,
        last_seen=occurrences[-1]['signal_time'] if occurrences else None,
        data_start=bars[0]['time'] if bars else None, data_end=bars[-1]['end'] if bars else None,
        baseline_window=[bars[low]['end'], bars[high]['end']] if low is not None else None,
        horizons=horizons, recent_occurrences=list(reversed(occurrences[-5:])))
    return summary, occurrences


def _cells(path):
    """Stream stock cells, rather than retaining hundreds of MB of other strategies."""
    decoder = json.JSONDecoder()
    with gzip.open(path, 'rt', encoding='utf-8') as source:
        buffer = ''
        while '"cells":[' not in buffer:
            part = source.read(65536)
            if not part:
                raise ValueError('Frozen stock artifact has no cells')
            buffer += part
        buffer = buffer.split('"cells":[', 1)[1]
        while True:
            buffer = buffer.lstrip(' \r\n\t,')
            if buffer.startswith(']'):
                return
            try:
                value, end = decoder.raw_decode(buffer)
            except json.JSONDecodeError:
                part = source.read(262144)
                if not part:
                    raise ValueError('Truncated frozen stock artifact') from None
                buffer += part
                continue
            buffer = buffer[end:]
            yield value


def _pilot_cells(path):
    """Select cells from the frozen writer's canonical JSON without parsing trade ledgers.

    Object prefixes cannot match escaped strings. Headers must parse as real cell
    objects with the correct identity. The caller additionally checks the complete
    decompressed artifact digest and requires every expected pilot specification.
    This is an extraction optimization only; `_cells` is the reference decoder.
    """
    marker = re.compile(r'\{"pattern_id":"(?:CH16|CDLENGULFING)",')
    decoder = json.JSONDecoder()
    with gzip.open(path, 'rt', encoding='utf-8') as source:
        buffer, eof = '', False
        while True:
            match = marker.search(buffer)
            if not match:
                if eof:
                    return
                buffer = buffer[-128:] + source.read(1024 * 1024)
                if len(buffer) <= 128:
                    eof = True
                continue
            buffer = buffer[match.start():]
            header_end = buffer.find('"occurrence_events":')
            while header_end < 0 and len(buffer) < 65536 and not eof:
                part = source.read(65536)
                eof = not part
                buffer += part
                header_end = buffer.find('"occurrence_events":')
            if header_end < 0:
                raise ValueError('Unrecognized frozen cell header')
            header = json.loads(buffer[:header_end] + '"occurrence_events":[]}')
            if header.get('timeframe') != '1D' or header.get('side') != 'long':
                buffer = buffer[header_end + len('"occurrence_events":'):]
                continue
            while True:
                try:
                    cell, end = decoder.raw_decode(buffer)
                    break
                except json.JSONDecodeError:
                    part = source.read(max(262144, min(len(buffer), 4 * 1024 * 1024)))
                    if not part:
                        raise ValueError('Truncated frozen pilot cell') from None
                    buffer += part
            buffer = buffer[end:]
            yield cell


SCHEMA = '''
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS histories(symbol TEXT PRIMARY KEY,bars BLOB NOT NULL,history_sha256 TEXT NOT NULL,artifact_sha256 TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cells(symbol TEXT,pattern_id TEXT,variant TEXT,state TEXT,occurrence_count INTEGER,last_seen TEXT,summary TEXT,occurrences TEXT,
 PRIMARY KEY(symbol,pattern_id,variant,state));
CREATE INDEX IF NOT EXISTS pilot_cells ON cells(pattern_id,variant,state,last_seen);
'''


def _prepare_stock(job):
    root, symbol, expected_history, stock_meta, specs, run = job
    root = Path(root)
    filename = hashlib.sha256(symbol.encode()).hexdigest() + '.json.gz'
    stock_path, history_path = root / 'stocks' / filename, root / 'history' / filename
    with gzip.open(stock_path, 'rb') as f:
        artifact_hash = hashlib.file_digest(f, 'sha256').hexdigest()
    history_raw = gzip.decompress(history_path.read_bytes())
    history_hash = _sha(history_raw)
    if artifact_hash != stock_meta['artifact_sha256'] or history_hash != expected_history or history_hash != stock_meta['history_sha256']:
        raise ValueError('Frozen checksum mismatch: ' + symbol)
    bars = json.loads(history_raw).get('1D', [[], {}])[0]
    if any(bars[i]['time'] <= bars[i - 1]['time'] for i in range(1, len(bars))):
        raise ValueError('Frozen candles are not chronological: ' + symbol)
    found = {}
    for cell in _pilot_cells(stock_path):
        if cell['timeframe'] == '1D' and cell['side'] == 'long' and cell['pattern_id'] in PATTERNS:
            found[(cell['pattern_id'], cell['variant'])] = cell
    paths = {h: [movement(bars, i, h) for i in range(len(bars))] for h in HORIZONS}
    prepared = []
    for spec in specs:
        cell = found.get((spec['pattern_id'], spec['variant']))
        if cell is None or cell['definition_version'] != spec['definition_version'] or cell.get('definition') != spec.get('definition'):
            raise ValueError('Missing or incompatible frozen cell: ' + symbol)
        for state in spec['states']:
            summary, occurrences = build_cell(bars, cell['occurrence_events'], symbol, spec, state, run, paths)
            prepared.append((symbol, spec['pattern_id'], spec['variant'], state, summary['occurrence_count'], summary['last_seen'], _dump(summary), _dump(occurrences)))
    return symbol, gzip.compress(_dump(bars).encode()), history_hash, artifact_hash, prepared


def build_cache(store, cache_path, symbols=None, progress=None, workers=1):
    """Explicit offline build; writes ONLY cache_path. Restartable after each verified stock."""
    root = store.directory / store.research_run
    manifest_raw = (root / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_raw)
    run_row = store.connect('research').execute('select status from runs where id=?', (store.research_run,)).fetchone()
    if not run_row or run_row['status'] != 'complete' or manifest['id'] != store.research_run:
        raise PilotError(409, 'HISTORY_IDENTITY', 'The frozen research run is not complete or does not match.')
    specs = [s for s in manifest['specifications'] if s['pattern_id'] in PATTERNS and s['side'] == 'long']
    if not specs:
        raise PilotError(409, 'HISTORY_SCOPE', 'This frozen run has no supported pilot definitions.')
    signature = _sha(_dump([VERSION, _sha(manifest_raw)]).encode())
    cache_path = Path(cache_path)
    if cache_path.resolve().is_relative_to(store.directory.resolve()):
        raise ValueError('Pilot cache must be outside the read-only research directory')
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(cache_path)) as con:
        con.executescript(SCHEMA)
        old = con.execute("select value from meta where key='signature'").fetchone()
        if old and json.loads(old[0]) != signature:
            raise PilotError(409, 'HISTORY_IDENTITY', 'Cache belongs to different source bytes or methodology; use a new cache path.')
        meta = dict(version=VERSION, run=store.research_run, source_run=manifest.get('source_run'),
                    manifest_sha256=_sha(manifest_raw), signature=signature, specifications=specs,
                    source_market_latest=manifest.get('source_market_latest'),
                    detector_identity=_sha(_dump([manifest['code_hashes'], manifest['dependencies']]).encode()),
                    universe_count=len(manifest['symbols']), built_at=datetime.now(timezone.utc).isoformat())
        con.executemany('insert or replace into meta values (?,?)', [(k, _dump(v)) for k, v in meta.items()])
        con.commit()
        requested = symbols if symbols is not None else manifest['symbols']
        jobs = []
        for symbol in requested:
            if symbol not in manifest['history_hashes']:
                raise ValueError('Symbol absent from frozen universe: ' + symbol)
            if con.execute('select 1 from histories where symbol=?', (symbol,)).fetchone():
                continue
            row = store.connect('research').execute('select status,metadata from stocks where run=? and symbol=?', (store.research_run, symbol)).fetchone()
            if not row or row['status'] != 'complete':
                raise ValueError('Unfinished frozen stock: ' + symbol)
            jobs.append((str(root), symbol, manifest['history_hashes'][symbol], json.loads(row['metadata']), specs, store.research_run))
        from concurrent.futures import ProcessPoolExecutor
        pool = ProcessPoolExecutor(max_workers=max(1, min(4, workers))) if workers > 1 else None
        try:
            results = pool.map(_prepare_stock, jobs) if pool else map(_prepare_stock, jobs)
            for symbol, packed_bars, history_hash, artifact_hash, prepared in results:
                with con:
                    con.executemany('insert or replace into cells values (?,?,?,?,?,?,?,?)', prepared)
                    con.execute('insert into histories values (?,?,?,?)', (symbol, packed_bars, history_hash, artifact_hash))
                if progress:
                    progress(symbol, con.execute('select count(*) from histories').fetchone()[0])
        finally:
            if pool:
                pool.shutdown(wait=True, cancel_futures=True)
        return dict(meta, cached_symbols=con.execute('select count(*) from histories').fetchone()[0])


class PatternHistoryService:
    def __init__(self, research_store, cache_path):
        self.store, self.cache_path = research_store, Path(cache_path)

    def _open(self):
        if not self.cache_path.is_file():
            raise PilotError(503, 'HISTORY_NOT_BUILT', 'The two-pattern history pilot is not built yet.')
        con = _ro(self.cache_path)
        try:
            meta = {r['key']: json.loads(r['value']) for r in con.execute('select * from meta')}
            current = (self.store.directory / self.store.research_run / 'manifest.json').read_bytes()
            if meta.get('version') != VERSION or meta.get('run') != self.store.research_run or meta.get('manifest_sha256') != _sha(current):
                raise PilotError(409, 'HISTORY_IDENTITY', 'Historical source or methodology identity does not match this pilot.')
            for spec in meta['specifications']:
                active = self.store.catalogue.spec(spec['pattern_id'], spec['variant'], 'long')
                if not active or active['definition_version'] != spec['definition_version'] or active.get('definition') != spec.get('definition'):
                    raise PilotError(409, 'HISTORY_IDENTITY', 'The displayed detector definition differs from this frozen evidence.')
            publication = self.store.publication()
            if publication['publication_status'] == 'withheld_source_quality_review':
                raise PilotError(409, 'HISTORY_WITHHELD', 'Historical data is withheld pending source-quality review.')
            cached = con.execute('select count(*) from histories').fetchone()[0]
            envelope = dict(version=VERSION, run=meta['run'], source_run=meta['source_run'],
                detector_identity=meta['detector_identity'], source_market_latest=meta['source_market_latest'],
                publication=publication, review_required=not publication['released'],
                review_label=None if publication['released'] else 'Historical data requires review',
                coverage=dict(cached_symbols=cached, universe_symbols=meta['universe_count'], complete=cached == meta['universe_count']),
                methodology=METHOD)
            return con, meta, envelope
        except (OSError, sqlite3.Error) as error:
            con.close()
            raise PilotError(503, 'HISTORY_SOURCE_UNAVAILABLE', 'The pilot cache or its frozen source manifest is unavailable.') from error
        except Exception:
            con.close()
            raise

    @staticmethod
    def _check(meta, pattern_id, variant, state):
        for spec in meta['specifications']:
            if spec['pattern_id'] == pattern_id and spec['variant'] == variant and state in spec['states']:
                return spec
        raise PilotError(400, 'HISTORY_SCOPE', 'Choose a supported daily bullish pattern, variant and state.')

    def catalogue(self):
        con, meta, envelope = self._open()
        con.close()
        return dict(envelope, patterns=[dict(pattern_id=s['pattern_id'], name=PATTERNS[s['pattern_id']],
            variant=s['variant'], states=s['states'], side='long', timeframe='1D',
            definition_version=s['definition_version'], definition=s.get('definition')) for s in meta['specifications']])

    def stocks(self, pattern_id, variant, state, search='', limit=100, offset=0):
        con, meta, envelope = self._open()
        with closing(con):
            self._check(meta, pattern_id, variant, state)
            limit, offset = max(1, min(500, int(limit))), max(0, int(offset))
            where = 'pattern_id=? and variant=? and state=? and instr(symbol,?)>0'
            args = (pattern_id, variant, state, search.upper().strip())
            total = con.execute('select count(*) from cells where ' + where, args).fetchone()[0]
            rows = con.execute('select symbol,occurrence_count,last_seen from cells where ' + where +
                               ' order by last_seen desc,symbol limit ? offset ?', (*args, limit, offset)).fetchall()
            return dict(envelope, pattern_id=pattern_id, variant=variant, state=state, total=total,
                        limit=limit, offset=offset, stocks=[dict(r) for r in rows])

    def _cell(self, con, meta, symbol, pattern_id, variant, state):
        self._check(meta, pattern_id, variant, state)
        row = con.execute('select summary,occurrences from cells where symbol=? and pattern_id=? and variant=? and state=?',
                          (symbol.upper(), pattern_id, variant, state)).fetchone()
        if not row:
            raise PilotError(404, 'HISTORY_NOT_CACHED', 'This stock is not yet in the pilot cache; it does not mean zero occurrences.')
        return json.loads(row['summary']), json.loads(row['occurrences'])

    def history(self, symbol, pattern_id, variant, state, horizon=5):
        con, meta, envelope = self._open()
        with closing(con):
            if horizon not in HORIZONS:
                raise PilotError(400, 'HISTORY_HORIZON', 'Choose 1, 3, 5 or 10 daily candles.')
            summary, occurrences = self._cell(con, meta, symbol, pattern_id, variant, state)
            offset = HORIZONS.index(horizon)
            recent = list(reversed(occurrences))
            return dict(envelope, **summary, selected_horizon=horizon,
                recent_completed=[o for o in recent if o['horizons'][offset]['status'] == 'measured'][:5],
                recent_pending=[o for o in recent if o['horizons'][offset]['status'] == 'pending'][:5],
                recent_excluded=[o for o in recent if o['horizons'][offset]['status'] == 'quality_excluded'][:5])

    def replay(self, symbol, pattern_id, variant, state, occurrence_id):
        con, meta, envelope = self._open()
        with closing(con):
            summary, occurrences = self._cell(con, meta, symbol, pattern_id, variant, state)
            event = next((o for o in occurrences if o['id'] == occurrence_id), None)
            if event is None:
                raise PilotError(404, 'HISTORY_OCCURRENCE', 'This occurrence does not belong to the selected historical cell.')
            source = con.execute('select * from histories where symbol=?', (symbol.upper(),)).fetchone()
            bars = json.loads(gzip.decompress(source['bars']))
            signal = event['signal_index']
            start = max(0, min(int(event['formation_start_index']), signal) - 10)
            stop = min(len(bars), signal + max(HORIZONS) + 2)
            points = [dict(b, index=i, phase='formation' if i <= signal else 'after_signal') for i, b in enumerate(bars[start:stop], start)]
            return dict(envelope, symbol=symbol.upper(), pattern_id=pattern_id, variant=variant, state=state,
                timeframe='1D', side='long', occurrence=event, bars=points,
                geometry=event['geometry'], geometry_source='frozen_detector_event',
                signal_index=signal, reference_index=signal + 1 if signal + 1 < len(bars) else None,
                source_hashes=dict(history=source['history_sha256'], events=source['artifact_sha256']),
                replay_note='Original frozen candles and detector geometry; after-signal candles are historical outcomes, not detection inputs.')
