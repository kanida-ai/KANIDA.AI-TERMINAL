"""Explicit research contracts. Unsupported clauses are never silently discarded."""
from copy import deepcopy
from datetime import date
import re

from market_scanner import strategy_core as core

SCHEMA = 'kanida.research.v1'
# Inspected signal definitions, not the entire legacy library's marketing catalogue.
NDP = {
    'RSI': ('n=14,os=30', 'n=7,os=30', 'n=21,os=30', 'n=9,os=25'),
    'EMA_cross': ('5x20', '9x21', '12x26', '5x50', '3x18', '7x34', '8x21', '10x30'),
    'SMA_cross': ('10x20', '20x50', '50x100', '50x200'),
    'price_vs_SMA': ('n=10', 'n=20', 'n=50', 'n=100', 'n=200'),
    'Donchian_break': ('n=20', 'n=55'),
}
ACCOUNT_KEYS = {'capital', 'max_positions', 'allocation_pct', 'risk_pct', 'fee_bps', 'slippage_bps'}
EXIT_KEYS = {'hold', 'stop_kind', 'stop', 'target', 'trailing', 'disqualify'}


def keys(value, allowed, label):
    if not isinstance(value, dict):
        raise ValueError(label + ' must be an object')
    extra = set(value) - set(allowed)
    if extra:
        raise ValueError(label + ' contains unsupported fields: ' + ', '.join(sorted(extra)))


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', value):
        raise ValueError('IDs must contain 1-64 letters, digits, underscores or hyphens')
    return value


def required(value, fields, label):
    missing = set(fields) - set(value)
    if missing:
        raise ValueError(label + ' needs explicit settings: ' + ', '.join(sorted(missing)))


def numbers(value, fields, label):
    for field in fields:
        if field in value and type(value[field]) not in (int, float):
            raise ValueError(label + '.' + field + ' must be a JSON number, not a boolean or string')


def entry(node, depth=0, budget=None):
    budget = budget if budget is not None else [0]
    budget[0] += 1
    if depth > 5 or budget[0] > 32:
        raise ValueError('Limit entry expressions to 32 nodes and five nesting levels')
    if not isinstance(node, dict):
        raise ValueError('An entry rule must be an object')
    if 'all' in node or 'any' in node:
        op = 'all' if 'all' in node else 'any'
        keys(node, {op}, 'Boolean expression')
        children = node[op]
        if not isinstance(children, list) or not 1 <= len(children) <= 12:
            raise ValueError('Each all/any group needs 1-12 rules')
        return {op: [entry(child, depth + 1, budget) for child in children]}
    if node.get('engine') == 'ndp':
        keys(node, {'engine', 'name', 'params', 'signal'}, 'NDP rule')
        if node.get('params') not in NDP.get(node.get('name'), ()) or type(node.get('signal')) is not int or node.get('signal') not in (-1, 1):
            raise ValueError('Choose an explicitly supported NDP name, parameters and signal (+1/-1)')
        return deepcopy(node)
    if node.get('engine') == 'stock_miner':
        keys(node, {'engine', 'feature', 'op', 'value'}, 'Stock Miner rule')
        if type(node.get('value')) not in (int, float) or (node.get('feature'), node.get('op'), node.get('value')) != ('d_sma200', '>', 0):
            raise ValueError('Only Stock Miner d_sma200 > 0 is admitted; future labels are forbidden')
        return deepcopy(node)
    keys(node, {'kind', 'value'}, 'Core rule')
    required(node, {'kind', 'value'}, 'Core rule')
    numbers(node, {'value'}, 'Core rule')
    if node.get('kind') == 'pattern':
        raise ValueError('Historical pattern replay exists separately; its adapter is not connected to this CLI runner yet')
    validated = core.validate({'conditions': [node]})['conditions'][0]
    return {'kind': validated['kind'], 'value': validated['value']}


def leaves(node):
    for op in ('all', 'any'):
        if op in node:
            return [leaf for child in node[op] for leaf in leaves(child)]
    return [node]


def validate(request):
    keys(request, {'schema', 'id', 'question', 'symbols', 'universe', 'timeframe', 'product', 'side',
                   'account', 'objectives', 'minimum_trades', 'folds', 'strategies'}, 'Research request')
    if request.get('schema') != SCHEMA:
        raise ValueError('Use schema ' + SCHEMA)
    identifier(request.get('id'))
    if request.get('universe', 'explicit') != 'explicit':
        raise ValueError('This pilot requires explicit stock symbols. Historical index/F&O membership is unverified; no substitute universe was tested')
    for key, expected in [('timeframe', '1D'), ('product', 'CNC'), ('side', 'long')]:
        if request.get(key, expected) != expected:
            raise ValueError('This runner supports 1D CNC long cash equities only; no derivative or short substitution')
    symbols = request.get('symbols')
    if not isinstance(symbols, list) or not 1 <= len(symbols) <= 64 or any(
            not isinstance(s, str) or not re.fullmatch(r'[A-Z0-9&_.-]{1,45}', s) for s in symbols):
        raise ValueError('Supply 1-64 explicit uppercase stock symbols')
    if len(set(symbols)) != len(symbols):
        raise ValueError('Duplicate stock symbols')
    folds = request.get('folds')
    if not isinstance(folds, list) or not 1 <= len(folds) <= 6:
        raise ValueError('Supply 1-6 chronological training/test folds')
    previous_test_end = ''
    for fold in folds:
        names = ['train_start', 'train_end', 'test_start', 'test_end']
        keys(fold, names, 'Walk-forward fold')
        if set(fold) != set(names):
            raise ValueError('Each fold needs train_start, train_end, test_start and test_end')
        values = [fold[n] for n in names]
        for value in values:
            if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                raise ValueError('Use ISO YYYY-MM-DD dates')
        if not values[0] < values[1] < values[2] < values[3] or values[2] <= previous_test_end:
            raise ValueError('Training must precede testing; test folds must not overlap and must be chronological')
        previous_test_end = values[3]
    account = request.get('account', {})
    keys(account, ACCOUNT_KEYS, 'Account')
    required(account, ACCOUNT_KEYS, 'Account')
    numbers(account, ACCOUNT_KEYS, 'Account')
    objectives = request.get('objectives', {})
    keys(objectives, {'cagr', 'drawdown', 'beat_nifty'}, 'Objectives')
    required(objectives, {'cagr', 'drawdown', 'beat_nifty'}, 'Objectives')
    numbers(objectives, {'cagr', 'drawdown'}, 'Objectives')
    numbers(request, {'minimum_trades'}, 'Research request')
    if 'beat_nifty' in objectives and type(objectives['beat_nifty']) is not bool:
        raise ValueError('beat_nifty must be true or false')
    strategies = request.get('strategies')
    if not isinstance(strategies, list) or not 1 <= len(strategies) <= 32:
        raise ValueError('Supply 1-32 explicitly declared candidate strategies')
    out = deepcopy(request)
    out.update(symbols=sorted(symbols), universe='explicit', timeframe='1D', product='CNC', side='long')
    normalized = []
    ids = set()
    for strategy in strategies:
        keys(strategy, {'id', 'name', 'entry', 'exit', 'ranking', 'schedule'}, 'Strategy')
        required(strategy, {'id', 'entry', 'exit', 'ranking', 'schedule'}, 'Strategy')
        sid = identifier(strategy.get('id'))
        if sid in ids:
            raise ValueError('Duplicate strategy ID: ' + sid)
        ids.add(sid)
        expression = entry(strategy.get('entry'))
        keys(strategy.get('exit', {}), EXIT_KEYS, 'Exit')
        required(strategy['exit'], EXIT_KEYS, 'Exit')
        numbers(strategy['exit'], {'hold', 'stop', 'target', 'trailing'}, 'Exit')
        if 'disqualify' in strategy.get('exit', {}) and type(strategy['exit']['disqualify']) is not bool:
            raise ValueError('disqualify must be true or false')
        if strategy['exit']['disqualify'] and any(n.get('name') in ('EMA_cross', 'SMA_cross', 'Donchian_break') or n.get('kind') == 'breakout' for n in leaves(expression)):
            raise ValueError('Event/breakout entries need a separate continuing exit condition; disqualify cannot reuse their entry pulse')
        validated = core.validate(dict(account, name=strategy.get('name', sid), universe='', symbols=symbols,
            conditions=[{'kind': 'momentum', 'value': 0}], start=folds[0]['train_start'], end=folds[-1]['test_end'],
            objectives=objectives, min_trades=request.get('minimum_trades', 5),
            exit=strategy.get('exit', {}), ranking=strategy.get('ranking', 'symbol'),
            rebalance=strategy.get('schedule', 'daily')))
        out['account'] = {k: validated[k] for k in sorted(ACCOUNT_KEYS)}
        out['objectives'] = validated['objectives']
        out['minimum_trades'] = validated['min_trades']
        normalized.append(dict(id=sid, name=validated['name'], entry=expression, exit=validated['exit'],
                               ranking=validated['ranking'], schedule=validated['rebalance']))
    out['strategies'] = normalized
    return out


def execution_spec(request, strategy, start, end):
    # Core's entry parser is not used. The original expression is compiled by our
    # adapter and handed directly to trade_path; no placeholder rule is executed.
    return dict(request['account'], name=strategy['name'], start=start, end=end,
                timeframe='1D', product='CNC', side='long', exit=deepcopy(strategy['exit']),
                ranking=strategy['ranking'], rebalance=strategy['schedule'])


def capabilities():
    return dict(schema=SCHEMA, scope='Daily CNC long equities; explicit symbols; local research only',
        core_conditions={k: v for k, v in core.CONDITIONS.items() if k != 'pattern'},
        ndp_signals=NDP, stock_miner=['d_sma200 > 0'], boolean_groups=['all (AND)', 'any (OR)', 'nested groups'],
        execution='Next observed session open, whole shares, shared cash, no borrowing, stop/target/trailing/time exits',
        data_provider='Frozen local OHLCV snapshot + actual Nifty 50 daily price index, read only',
        blocked=['Historical constituent membership', 'Fundamentals', 'Derivatives/shorts/intraday',
                 'Arbitrary natural-language compilation', 'Pattern replay adapter', 'Live deployment'],
        limits=dict(symbols=64, strategies=32, folds=6),
        note='Supported computation is not a claim of strategy profitability or an under-one-minute SLA.')
