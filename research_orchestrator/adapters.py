"""Read-only data and feature adapters around existing engines."""
import gzip
import json
import math
import sqlite3
from pathlib import Path
from datetime import date, datetime, timedelta, timezone

import numpy as np

from market_scanner import strategy_core as core
from .contracts import leaves
from .feature_probe import load_pure_module
from .inventory import TERMINAL
from .numerical_probe import benchmark, digest


class LocalSnapshotProvider:
    """A vendor adapter can implement load(symbols, cutoff) with this same result.

    Its contract must include completed candles, dates, instrument identities and
    provenance. This interface alone does not implement a paid vendor integration.
    """
    def load(self, symbols, cutoff, as_of=None):
        from market_scanner import backtest_store as store
        with sqlite3.connect(store.DB.resolve().as_uri() + '?mode=ro', uri=True) as con:
            run_id = store.active_run(con)
        if not run_id:
            raise ValueError('No frozen historical snapshot is available')
        histories = {}
        for symbol in symbols:
            path = store.history_path(run_id, symbol)
            if not path.is_file():
                raise ValueError('No frozen history for ' + symbol)
            data = json.loads(gzip.decompress(path.read_bytes()))
            if '1D' not in data:
                raise ValueError('No daily history for ' + symbol)
            histories[symbol] = [b for b in data['1D'][0] if b['end'][:10] <= cutoff]
        index = benchmark(cutoff)
        return histories, index, dict(provider='local_frozen_snapshot', run_id=run_id,
            hashes={**{s: digest(b) for s, b in histories.items()}, 'NIFTY 50': digest(index)},
            adjustments='unverified', calendar='Nifty observed sessions, not an independently verified exchange calendar',
            benchmark_source='ohlc_daily read at run time; archived with the research evidence',
            membership='explicit surviving stocks; no point-in-time membership claim')


class RecordedSnapshotProvider:
    """Replay archived equities AND index even if the live source DB changes."""
    def __init__(self, path):
        self.path = Path(path)

    def load(self, symbols, cutoff, as_of=None):
        value = json.loads(gzip.decompress(self.path.read_bytes()))
        if digest(value) != self.path.name.split('.')[0]:
            raise ValueError('Recorded input checksum failed')
        if any(symbol not in value['histories'] for symbol in symbols):
            raise ValueError('Recorded input does not contain the requested symbols')
        return ({s: [b for b in value['histories'][s] if b['end'][:10] <= cutoff] for s in symbols},
                [b for b in value['benchmark'] if b['end'][:10] <= cutoff], value['provenance'])


def validate_data(histories, index, request, as_of=None):
    start = request['folds'][0]['train_start']
    end = request['folds'][-1]['test_end']
    observations = []
    benchmark_days = {b['end'][:10] for b in index if start <= b['end'][:10] <= end}
    as_of = as_of or datetime.now(timezone.utc)
    india = timezone(timedelta(hours=5, minutes=30))
    for symbol, bars in {**histories, 'NIFTY 50': index}.items():
        if len(bars) < 253:
            raise ValueError(symbol + ': need at least 253 completed candles including warm-up')
        days = [b['end'][:10] for b in bars]
        if days != sorted(set(days)):
            raise ValueError(symbol + ': duplicated or out-of-order candle dates')
        if days[-1] > end:
            raise ValueError(symbol + ': provider returned candles after the requested cutoff')
        for b in bars:
            if any(not isinstance(b.get(k), (int, float)) or not math.isfinite(b[k]) or b[k] <= 0
                   for k in ('open', 'high', 'low', 'close')):
                raise ValueError(symbol + ': invalid OHLC prices')
            if not b['low'] <= min(b['open'], b['close']) <= max(b['open'], b['close']) <= b['high']:
                raise ValueError(symbol + ': inconsistent OHLC prices')
            if not isinstance(b.get('volume'), (int, float)) or not math.isfinite(b['volume']) or b['volume'] < 0:
                raise ValueError(symbol + ': invalid volume')
            if b['time'] >= b['end'] or b['time'][:10] != b['end'][:10]:
                raise ValueError(symbol + ': invalid daily candle timestamps')
            candle_end = datetime.fromisoformat(b['end'])
            if candle_end.tzinfo is None:
                candle_end = candle_end.replace(tzinfo=india)
            if candle_end > as_of:
                raise ValueError(symbol + ': candle has not completed at the research as-of time')
        if sum(d < start for d in days) < 252:
            raise ValueError(symbol + ': fewer than 252 warm-up sessions before the requested study')
        if (date.fromisoformat(end) - date.fromisoformat(days[-1])).days > 7:
            raise ValueError(symbol + ': requested test period exceeds available history')
        for fold in request['folds']:
            for boundary in ('train_end', 'test_end'):
                last = next((d for d in reversed(days) if d <= fold[boundary]), None)
                if last is None or (date.fromisoformat(fold[boundary]) - date.fromisoformat(last)).days > 7:
                    raise ValueError(symbol + ': stale/missing data at fold boundary ' + fold[boundary])
        longest = streak = 0
        available_days = set(days)
        for d in sorted(benchmark_days):
            streak = streak + 1 if d not in available_days else 0
            longest = max(longest, streak)
        suspicious = [dict(date=b['time'][:10], move_pct=round(100*(b['open']/a['close']-1), 3))
                      for a,b in zip(bars,bars[1:]) if start <= b['time'][:10] <= end and abs(b['open']/a['close']-1) > .2]
        observations.append(dict(symbol=symbol, first=days[0], last=days[-1], candles=len(bars),
                                 flagged_gaps=sum(bool(b.get('gap')) for b in bars),
                                 missing_benchmark_dates=sorted(benchmark_days - set(days)),
                                 longest_missing_session_run=longest, large_overnight_moves=suspicious))
    return observations


class Features:
    """One feature pass per stock/cutoff, reused across every candidate."""
    def __init__(self, strategies):
        self.nodes = [leaf for s in strategies for leaf in leaves(s['entry'])]
        self.need_ndp = any(n.get('engine') == 'ndp' for n in self.nodes)
        self.need_miner = any(n.get('engine') == 'stock_miner' for n in self.nodes)
        self.ndp = self.miner = None
        if self.need_ndp:
            self.ndp, _ = load_pure_module(TERMINAL / 'ndp/indicator_library.py', 'research_ndp_adapter')
        if self.need_miner:
            self.miner, _ = load_pure_module(TERMINAL / 'stock_miner/indicators.py', 'research_stock_adapter')

    def prepare(self, histories, index, cutoff, strategies):
        import pandas as pd
        bench = [b for b in index if b['end'][:10] <= cutoff]
        out = {}
        for symbol, full in histories.items():
            bars = [b for b in full if b['end'][:10] <= cutoff]
            f = core.features(bars, bench)
            signals = {};stock = {}
            if self.need_ndp or self.need_miner:
                frame = pd.DataFrame([dict(trade_date=b['end'][:10], **{k: b[k] for k in
                     ('open', 'high', 'low', 'close', 'volume')}) for b in bars])
                if self.need_ndp:
                    inputs = {k: frame[c].to_numpy(float) for k, c in
                              [('o', 'open'), ('h', 'high'), ('l', 'low'), ('c', 'close'), ('v', 'volume')]}
                    signals = {(name, params): values for name, params, values in self.ndp.build_all(inputs)}
                if self.need_miner:
                    stock['d_sma200'] = self.miner.compute_features(frame)['d_sma200'].to_numpy()
            leaf_cache = {}
            masks = {}
            for strategy in strategies:
                mask = compile_mask(strategy['entry'], f, signals, stock, leaf_cache).copy()
                # This admission warm-up and finite check applies after all groups.
                mask[:252] = False
                mask &= np.isfinite(f['atr'])
                masks[strategy['id']] = mask
            out[symbol] = (bars, f, masks)
        return out, bench


def compile_mask(node, features, signals, stock, cache=None):
    cache = cache if cache is not None else {}
    for op, reducer in [('all', np.logical_and.reduce), ('any', np.logical_or.reduce)]:
        if op in node:
            return reducer([compile_mask(c, features, signals, stock, cache) for c in node[op]])
    key = digest(node)
    if key not in cache:
        if node.get('engine') == 'ndp':
            cache[key] = signals[(node['name'], node['params'])] == node['signal']
        elif node.get('engine') == 'stock_miner':
            cache[key] = stock['d_sma200'] > 0
        else:
            cache[key] = core.condition_mask(node, features)
    return cache[key]
