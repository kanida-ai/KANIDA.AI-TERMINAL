"""Versioned study jobs and replay bundles over immutable research candles."""
from __future__ import annotations
import gzip
import hashlib
import importlib.util
import json
import math
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from . import backtest_store as store, catalog
from .data import ROOT
from .backtest import Rule, candidates, rules_config
from .detectors import NAMES
from .study_engine import VERSION, outcome, learn_rule, portfolio, add_months

HOME = ROOT / 'output' / 'studies'
POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix='historical-study')
LOCK = threading.RLock()
CANCEL = {}
FUTURES = {}


class MarkHistory:
    """Read just timestamp/open/close marks without retaining all OHLC dictionaries."""
    def __init__(self, run, symbol, timeframe, bars):
        import numpy as np
        signature = hashlib.sha256((run + '|' + symbol + '|' + timeframe).encode()).hexdigest()
        self.file = HOME / 'marks' / (signature + '.npy')
        with LOCK:
            if not self.file.exists():
                self.file.parent.mkdir(parents=True, exist_ok=True)
                values = np.array([(b['time'], b['end'], b['open'], b['close']) for b in bars],
                                  dtype=[('time', 'S19'), ('end', 'S19'), ('open', 'f8'), ('close', 'f8')])
                temp = self.file.with_suffix('.tmp')
                with temp.open('wb') as stream:np.save(stream, values, allow_pickle=False)
                temp.replace(self.file)

    def __iter__(self):
        import numpy as np
        values = np.load(self.file, mmap_mode='r', allow_pickle=False)
        try:
            for b in values:
                yield dict(time=b['time'].decode(), end=b['end'].decode(), open=float(b['open']), close=float(b['close']))
        finally:
            values._mmap.close()


def packed_write(path, value):
    with LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
        raw = json.dumps(value, allow_nan=False, separators=(',', ':')).encode()
        temp.write_bytes(gzip.compress(raw) if path.suffix == '.gz' else raw)
        for attempt in range(6):
            try:
                temp.replace(path)
                break
            except PermissionError:
                if attempt == 5:raise
                time.sleep(.05)


def packed_read(path):
    with LOCK:raw = path.read_bytes()
    return json.loads(gzip.decompress(raw) if path.suffix == '.gz' else raw)


def identity(value):
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}', str(value)):
        raise ValueError('Invalid study identity')
    return str(value)


@lru_cache(maxsize=4)
def frozen(run):
    directory = ROOT / 'output' / 'research-code' / identity(run)
    required = ['data.py', 'detectors.py', 'historical.py', 'config.json']
    if not all((directory / f).is_file() for f in required):
        raise ValueError('The detector version for this historical snapshot is unavailable')
    name = 'kanida_frozen_' + run.replace('-', '_')
    with LOCK:
        if name + '.historical' not in sys.modules:
            package = ModuleType(name)
            package.__path__ = [str(directory)]
            sys.modules[name] = package
            for module in ('data', 'detectors', 'historical'):
                spec = importlib.util.spec_from_file_location(name + '.' + module, directory / (module + '.py'))
                loaded = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = loaded
                spec.loader.exec_module(loaded)
        return sys.modules[name + '.historical'], packed_read(directory / 'config.json')


def event_history(run, symbol, timeframe, patterns, cancelled):
    mod, config = frozen(run)
    frames = store.load_history(run, symbol)
    if not frames or timeframe not in frames:
        return [], {}, None
    bars, quality = frames[timeframe]
    signature = hashlib.sha256(json.dumps([run, symbol, timeframe, sorted(patterns), config['pattern_version'], VERSION]).encode()).hexdigest()
    file = HOME / 'signals' / (signature + '.json.gz')
    if file.exists():
        return bars, packed_read(file), quality
    replay = mod.HistoricalReplay(bars, config['history_bars'], patterns)
    # A changed accelerator cannot discard signals from an older detector.
    accelerator = ROOT / 'output' / 'research-code' / run / 'replay_filter.py'
    if accelerator.read_bytes() == (ROOT / 'replay_filter.py').read_bytes():
        from .replay_filter import masks
        mask = masks(replay.h, replay.l, replay.c, replay.v, replay.tr, replay.tops, replay.bottoms, config['history_bars'])
    else:
        mask = [63] * len(bars)
    active, groups = {}, {}
    for i in range(39, len(bars)):
        if i % 128 == 0 and cancelled():
            raise InterruptedError('Study cancelled')
        for m in replay.at(i, int(mask[i])):
            sides = ('long', 'short') if m['direction'] == 'neutral' else ('long',) if m['direction'] == 'bullish' else ('short',)
            for side in sides:
                key = m['pattern'] + '|' + side
                previous = active.get(key)
                if previous is None or i - previous['last'] > 3:
                    previous = {'last': i, 'seen': set(), 'episode': i}
                    active[key] = previous
                previous['last'] = i
                if m['state'] in previous['seen']:
                    continue
                previous['seen'].add(m['state'])
                groups.setdefault(key, []).append(dict(signal_index=i, episode=previous['episode'], state=m['state'],
                    atr=max(float(replay.tr[i - 20:i].mean()), float(replay.c[i]) * .001), score=m['score'],
                    direction=m['direction'], pattern_start=m['pattern_start'], shape=m))
    packed_write(file, groups)
    return bars, groups, quality


def normalize(value):
    if not isinstance(value, dict):
        raise ValueError('Study settings are required')
    def number(name, default, lo, hi, integer=False):
        x = float(value.get(name, default))
        if not math.isfinite(x) or not lo <= x <= hi or integer and x != int(x):
            raise ValueError('Invalid ' + name.replace('_', ' '))
        return int(x) if integer else x
    start, end = str(value.get('start', '2020-01-01')), str(value.get('end', '2026-07-31'))
    date.fromisoformat(start); date.fromisoformat(end)
    if start > end:
        raise ValueError('Start date must be before end date')
    patterns = value.get('patterns') or list(NAMES)
    timeframes = value.get('timeframes') or ['1D']
    if not isinstance(patterns, list) or not set(patterns) <= set(NAMES):
        raise ValueError('Choose approved patterns')
    if not isinstance(timeframes, list) or not set(timeframes) <= {'1H', '4H', '1D', '1W'}:
        raise ValueError('Choose supported timeframes')
    method, product, side = value.get('method', 'backtest'), value.get('product', 'CNC'), value.get('side', 'long')
    if method not in ('backtest', 'walkforward') or product not in ('CNC', 'MIS') or side not in ('long', 'short', 'both'):
        raise ValueError('Choose a supported test method, side and execution model')
    trigger = value.get('trigger', 'setup')
    if trigger not in ('setup', 'confirmed'):
        raise ValueError('Choose setup or confirmed breakout entry')
    symbols = value.get('symbols') or []
    if not isinstance(symbols, list) or len(symbols) > 2000 or any(not isinstance(s, str) or len(s) > 45 for s in symbols):
        raise ValueError('Invalid stock selection')
    selected = catalog.selected_symbols({'universe': value.get('universe', ''), 'sector': value.get('sector', '')})
    with store.connection() as con:
        run = store.active_run(con)
        available = [r[0] for r in con.execute("SELECT symbol FROM stocks WHERE run=? AND status='complete' ORDER BY symbol", (run,))]
    chosen = set(s.upper() for s in symbols) if symbols else set(available)
    unknown = chosen - set(available)
    if unknown:
        raise ValueError('No completed history for: ' + ', '.join(sorted(unknown)[:8]))
    if selected is not None:
        chosen &= set(selected)
    if not chosen:
        raise ValueError('No stocks match the selected universe and sector')
    defaults = {'1H': 6, '4H': 6, '1D': 10, '1W': 4}
    hold = value.get('hold', {})
    if not isinstance(hold, dict):
        raise ValueError('Holding periods must be set per timeframe')
    for tf in timeframes:
        n = float(hold.get(tf, defaults[tf]))
        if not math.isfinite(n) or n != int(n) or not 1 <= n <= 260:
            raise ValueError('Holding periods must be 1–260 candles')
        defaults[tf] = int(n)
    return dict(run=run, agent='chart', method=method, product=product, side=side, trigger=trigger,
        symbols=sorted(chosen), universe=value.get('universe', ''), sector=value.get('sector', ''),
        patterns=sorted(set(patterns)), timeframes=sorted(set(timeframes)), start=start, end=end,
        capital=number('capital', 10000, 100, 1e9), max_positions=number('max_positions', 1 if len(chosen) == 1 else 5, 1, 100, True),
        allocation_pct=number('allocation_pct', 100 if len(chosen) == 1 else 20, .1, 100),
        risk_pct=number('risk_pct', 2, .01, 100), fee_bps=number('fee_bps', 30, 0, 1000),
        slippage_bps=number('slippage_bps', 5, 0, 1000), stop_atr=number('stop_atr', 0, 0, 10),
        target_r=number('target_r', 2, .1, 20), hold=defaults, reinvest=value.get('reinvest', True) is True,
        training_months=number('training_months', 36, 3, 180, True), test_months=number('test_months', 6, 1, 36, True),
        minimum_training=number('minimum_training', 20, 5, 500, True), benchmark=value.get('benchmark', True) is True,
        candidate_family={tf:[asdict_rule(r) for r in candidates(tf, rules_config()) if r.trigger==trigger] for tf in timeframes},
        name=str(value.get('name', ''))[:100])


def job_path(job):
    return HOME / 'jobs' / (identity(job) + '.json')


def get_job(owner, job, result=False):
    file = job_path(job)
    if not file.exists():
        raise ValueError('Study not found')
    item = packed_read(file)
    if item['owner'] != owner:
        raise ValueError('Study not found')
    if item['status'] in ('queued', 'running') and job not in CANCEL:
        item.update(status='interrupted', message='The research service restarted. Run these saved settings again.')
    item.pop('owner', None)
    if result and item['status'] == 'complete':
        item['result'] = packed_read(HOME / 'results' / (job + '.json.gz'))
    return item


def jobs(owner):
    folder = HOME / 'jobs'
    return sorted([get_job(owner, p.stem) for p in folder.glob('*.json') if packed_read(p).get('owner') == owner],
                  key=lambda x: x['created'], reverse=True)[:50]


def start(owner, value):
    owner = identity(owner)
    settings = normalize(value)
    with LOCK:
        running = [j for j in jobs(owner) if j['status'] in ('queued', 'running')]
        if len(running) >= 2:
            raise ValueError('Two studies are already running. Finish or cancel one first.')
        job = uuid.uuid4().hex
        event = threading.Event()
        CANCEL[job] = event
        item = dict(id=job, owner=owner, status='queued', created=time.time(), settings=settings,
                    done=0, total=len(settings['symbols']) * len(settings['timeframes']), message='Queued for historical replay')
        packed_write(job_path(job), item)
        FUTURES[job] = POOL.submit(run_job, item, event)
    return get_job(owner, job)


def cancel(owner, job):
    item = get_job(owner, job)
    if job in CANCEL:
        CANCEL[job].set()
    return item


def chart_bundle(run, symbol, tf, signal, pattern, trade=None, shape=None):
    frames = store.load_history(run, symbol)
    if not frames or tf not in frames:
        raise ValueError('Historical candles are unavailable')
    bars = frames[tf][0]
    if not 39 <= signal < len(bars):
        raise ValueError('Signal is outside available history')
    begin = max(0, signal - 259)
    if shape is None:
        module, config = frozen(run)
        shape = next((m for m in module.HistoricalReplay(bars[:signal + 1], config['history_bars'], [pattern]).at(signal) if m['pattern'] == pattern), None)
    end = min(len(bars), (trade['exit_index'] + 1 if trade else signal + 1))
    return dict(symbol=symbol, timeframe=tf, pattern=pattern, bars=bars[begin:end], matches=[shape] if shape else [],
                shape=shape, signal_index=signal - begin, offset=begin,
                entry_index=trade['entry_index'] - begin if trade else None,
                exit_index=trade['exit_index'] - begin if trade else None,
                trade=trade, geometry_version=run, pattern_name=NAMES[pattern])


def legacy_replay(params):
    symbol, tf, pattern, side = (params[k] for k in ('symbol', 'timeframe', 'pattern', 'side'))
    study = store.cell(symbol, tf, pattern, side)
    if not study:
        raise ValueError('No historical study for this selection')
    run = study['run_id']
    capital = float(params.get('capital', 10000))
    if not math.isfinite(capital) or not 100 <= capital <= 1e9:
        raise ValueError('Capital must be ₹100–₹1,000,000,000')
    source = study['reference_trades']
    if not source:
        return dict(trades=[], curve=[], occurrences=[], summary={'trades': 0}, legacy=True, run=run)
    bars = store.load_history(run, symbol)[tf][0]
    settings = dict(start=bars[0]['time'][:10], end=bars[-1]['end'][:10], capital=capital, max_positions=1,
                    allocation_pct=100, reinvest=True, risk_pct=100, fee_bps=source[0]['cost_pct'] * 100,
                    slippage_bps=0, product='Historical price study')
    trades = []
    for t in source:
        trades.append(dict(t, symbol=symbol, timeframe=tf, pattern=pattern, pattern_name=NAMES[pattern], side=side,
                          raw_entry=t['entry'], raw_exit=t['exit'], product='Historical price study',
                          formation_start=t.get('pattern_start'), holding_days=(date.fromisoformat(t['exit_candle_end'][:10]) - date.fromisoformat(t['entry_time'][:10])).days,
                          overnight=t['entry_time'][:10] != t['exit_candle_end'][:10],
                          rule=study['reference']['rule'], exit_time=t['exit_candle_start'] if t['exit_timing'] == 'open' else t['exit_candle_end']))
    result = portfolio(trades, {symbol + '|' + tf: bars}, settings)
    result.update(legacy=True, run=run, settings=settings, validation='Whole-history baseline',
                  occurrences=[dict(id=t['id'], symbol=symbol, timeframe=tf, pattern=pattern, side=side,
                    signal_index=t['signal_index'], time=t['signal_time'], formation_start=t['formation_start'], state='historical trade') for t in trades],
                  note='Original historical fills and 0.40% assumed costs. Open-position marks added. No broker-product eligibility model. Use Simulate to apply CNC or intraday rules.')
    if result['trades']:
        result['first_chart'] = chart_bundle(run, symbol, tf, result['trades'][0]['signal_index'], pattern, result['trades'][0])
    return result


def trade_chart(owner, job, trade_id='', occurrence_id=''):
    meta = get_job(owner, job)
    if meta['status'] != 'complete':
        raise ValueError('This study has not completed')
    data = packed_read(HOME / 'results' / (job + '.json.gz'))
    collection = data['trades'] + data['skipped'] if trade_id else data['occurrences']
    key = trade_id or occurrence_id
    row = next((r for r in collection if r['id'] == key), None)
    if row is None:
        raise ValueError('Historical event not found')
    return chart_bundle(meta['settings']['run'], row['symbol'], row['timeframe'], row['signal_index'], row['pattern'],
                        row if 'entry_index' in row and not row.get('skipped_reason') else None, row.get('shape'))


def run_job(item, event):
    job, settings = item['id'], item['settings']
    item['status'] = 'running'
    trades, rejected, occurrences, folds, histories, coverage = [], [], [], [], {}, []
    def progress(message):
        item.update(message=message, updated=time.time())
        packed_write(job_path(job), item)
    try:
        packed_write(job_path(job), item)
        for symbol in settings['symbols']:
            for tf in settings['timeframes']:
                if event.is_set():
                    raise InterruptedError('Study cancelled')
                progress('Reading ' + symbol + ' · ' + tf)
                bars, groups, quality = event_history(settings['run'], symbol, tf, settings['patterns'], event.is_set)
                item['done'] += 1
                if not bars:
                    coverage.append(dict(symbol=symbol, timeframe=tf, message='No stored candles'))
                    continue
                histories[symbol + '|' + tf] = MarkHistory(settings['run'], symbol, tf, bars)
                coverage.append(dict(symbol=symbol, timeframe=tf, first=bars[0]['time'], last=bars[-1]['end'], quality=quality))
                for group, events in groups.items():
                    pattern, side = group.split('|')
                    if settings['side'] != 'both' and side != settings['side']:
                        continue
                    local = dict(settings, timeframe=tf)
                    for e in events:
                        if settings['start'] <= bars[e['signal_index']]['end'][:10] <= settings['end']:
                            occurrences.append(dict(id='o' + str(len(occurrences) + 1), symbol=symbol, timeframe=tf,
                                pattern=pattern, pattern_name=NAMES[pattern], side=side, signal_index=e['signal_index'],
                                time=bars[e['signal_index']]['end'], formation_start=e['pattern_start'], state=e['state']))
                    periods = []
                    if settings['method'] == 'backtest':
                        periods.append((settings['start'], add_months(settings['end'], 1),
                                        Rule(settings['trigger'], settings['hold'][tf], settings['stop_atr'], settings['target_r']), None))
                    else:
                        cursor = settings['start']
                        rules = [Rule(**r) for r in settings['candidate_family'][tf]] if settings.get('candidate_family') else candidates(tf, rules_config())
                        # Candidate family is fixed in advance; trigger is user-selected.
                        rules = [r for r in rules if r.trigger == settings['trigger']]
                        embargo = max(r.hold for r in rules)
                        while cursor <= settings['end']:
                            until = add_months(cursor, settings['test_months'])
                            prior = add_months(cursor, -settings['training_months'])
                            best = learn_rule(events, bars, side, rules, local, cursor, prior, embargo, settings['minimum_training'])
                            fold = dict(symbol=symbol, timeframe=tf, pattern=pattern, side=side, learning_start=prior,
                                        test_start=cursor, test_end=min((date.fromisoformat(until)-timedelta(days=1)).isoformat(), settings['end']),
                                        training_trades=best['samples'] if best else 0,
                                        rule=asdict_rule(best['rule']) if best else None,
                                        status='frozen_for_test' if best else 'insufficient_positive_training_evidence')
                            folds.append(fold)
                            periods.append((cursor, until, best['rule'] if best else None, len(folds) - 1))
                            cursor = until
                    for lo, hi, rule, fold in periods:
                        if rule is None:
                            continue
                        for e in events:
                            entry = e['signal_index'] + 1
                            if entry >= len(bars) or not lo <= bars[entry]['time'][:10] < hi or bars[entry]['time'][:10] > settings['end']:
                                continue
                            trade = outcome(e, bars, side, rule, local)
                            if trade is None:
                                continue
                            trade.update(symbol=symbol, timeframe=tf, pattern=pattern, pattern_name=NAMES[pattern], side=side,
                                         fold=fold, formation_start=e['pattern_start'])
                            if trade.get('skipped_reason'):
                                trade.update(id='ineligible' + str(len(rejected) + 1), signal_time=bars[e['signal_index']]['end'])
                                rejected.append(trade)
                            else:
                                trades.append(trade)
                progress('Prepared ' + symbol + ' · ' + tf)
        progress('Replaying the shared account')
        result = portfolio(trades, histories, settings, event.is_set)
        result['skipped'].extend(rejected)
        result['summary']['skipped'] = len(result['skipped'])
        result.update(settings=settings, run=settings['run'], engine_version=VERSION, occurrences=occurrences, folds=folds, coverage=coverage,
            validation='Rolling unseen test periods' if settings['method'] == 'walkforward' else 'Fixed-rule historical backtest',
            note='Unleveraged whole-share account. Declared percentage fees and per-side slippage are assumptions, not actual broker charges. MIS exits at the last completed candle no later than 15:15. Intrabar exits settle at candle end. Gap exits use the first available open. Snapshot index membership; no dividend/corporate-action adjustment supplied.')
        for fold_index, f in enumerate(folds):
            subset = [t for t in result['trades'] if t['fold'] == fold_index]
            f.update(test_trades=len(subset), net_pnl=round(sum(t['net_pnl'] for t in subset), 2))
        contributions = []
        for symbol in settings['symbols']:
            subset = [t for t in result['trades'] if t['symbol'] == symbol]
            if subset:
                contributions.append(dict(symbol=symbol, trades=len(subset), wins=sum(t['net_pnl'] > 0 for t in subset),
                    net_pnl=round(sum(t['net_pnl'] for t in subset), 2),
                    average_return_pct=sum(t['net_return_pct'] for t in subset) / len(subset)))
        result['contributions'] = sorted(contributions, key=lambda x: -x['net_pnl'])
        if settings['benchmark'] and histories:
            benchmark = []
            seen = set()
            for key, bars in histories.items():
                symbol, tf = key.split('|')
                eligible = [(i, b) for i, b in enumerate(bars) if settings['start'] <= b['time'][:10] and b['end'][:10] <= settings['end']]
                if symbol in seen or not eligible:
                    continue
                seen.add(symbol)
                (i, first), (j, last) = eligible[0], eligible[-1]
                benchmark.append(dict(symbol=symbol, timeframe=tf, pattern='buy_hold', side='long', score=0,
                    entry_index=i, exit_index=j, entry_time=first['time'], exit_time=last['end'], exit_timing='close',
                    entry=first['open'], raw_entry=first['open'], exit=last['close'], stop=None,
                    net_return_pct=(last['close']/first['open']-1)*100-settings['fee_bps']/100,
                    holding_days=(date.fromisoformat(last['end'][:10])-date.fromisoformat(first['time'][:10])).days,
                    holding_bars=j-i+1,mfe_pct=0,mae_pct=0))
            if benchmark:
                b = portfolio(benchmark, histories, dict(settings, max_positions=len(benchmark), allocation_pct=100/len(benchmark), reinvest=False), event.is_set)
                result['benchmark'] = dict(label='Equal-allocation buy & hold · selected stocks · fees, no slippage', curve=b['curve'], summary=b['summary'])
        if result['trades']:
            first = result['trades'][0]
            result['first_chart'] = chart_bundle(settings['run'], first['symbol'], first['timeframe'], first['signal_index'], first['pattern'], first)
        # Geometry can be reproduced from the pinned detector version on demand.
        for row in result['trades'] + result['skipped'] + occurrences:
            row.pop('shape', None)
        occurrences.sort(key=lambda row: (row['time'], row['symbol'], row['timeframe'], row['pattern']))
        packed_write(HOME / 'results' / (job + '.json.gz'), result)
        item.update(status='complete', message='Historical study ready', completed=time.time())
        packed_write(job_path(job), item)
    except InterruptedError:
        item.update(status='cancelled', message='Study cancelled. No account or broker orders were changed.')
        packed_write(job_path(job), item)
    except Exception as e:
        import logging
        logging.exception('Historical study failed')
        item.update(status='error', message=str(e)[:240])
        packed_write(job_path(job), item)
    finally:
        CANCEL.pop(job, None)
        FUTURES.pop(job, None)


def asdict_rule(rule):
    return dict(trigger=rule.trigger, hold=rule.hold, stop_atr=rule.stop_atr, target_r=rule.target_r)
