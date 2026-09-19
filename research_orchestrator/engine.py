"""Bounded multi-strategy research, training-only selection and exact evidence reuse."""
from copy import deepcopy
from datetime import date, datetime, timezone
from bisect import bisect_left, bisect_right
import hashlib
import gzip
import json
import os
from pathlib import Path
import time
import warnings
import math
import statistics
import tempfile

import numpy as np

from market_scanner import strategy_core as core
from .adapters import Features, LocalSnapshotProvider, validate_data
from .contracts import execution_spec, identifier, validate
from .inventory import WORKSPACE, inventory
from .numerical_probe import digest
from .accounting_checks import verify_account_strict

VERSION = 'research-runner-0.1'
LIMITATIONS = [
    'Explicit surviving stocks only. Price adjustments, delistings and historical constituents are unverified.',
    'Candidate lists are declared before this run. Previous human experiments can still introduce selection bias.',
    'Nifty 50 is the price index, excluding dividends and index investment costs.',
    'Each fold starts a new account. Fold returns must not be added or compounded as one continuous portfolio.',
    'Training excludes signals whose maximum holding horizon crosses the training boundary. Test positions close at the test boundary.',
    'Fees are illustrative round-trip basis points on entry notional, charged upfront; slippage applies separately to each fill.',
    'Daily OHLC assumes stop first if stop and target are touched. Intrabar excursion order is unknown. Drawdown uses observed marks.',
    'Weekly/monthly schedules review on the first observed session of the period; these are entry/disqualification schedules, not target-weight rebalancing.',
    'Training selection uses a declared heuristic, not a trained profitability model. An untouched final holdout and broader data validation remain required.',
    'This local engine does not promise future returns or execute orders. It does not yet meet a whole-market, concurrent-user latency SLA.',
]


def source_identity():
    discovered = inventory()
    sources = {row['id']: row.get('sha256') for row in discovered['engines']}
    for name in ('strategy_core.py', 'detectors.py', 'data.py', 'backtest_store.py'):
        sources['market_scanner/' + name] = hashlib.sha256((WORKSPACE / 'market_scanner' / name).read_bytes()).hexdigest()
    for name in ('engine.py', 'contracts.py', 'adapters.py', 'accounting_checks.py', 'numerical_probe.py', 'feature_probe.py', 'inventory.py'):
        sources['research_orchestrator/' + name] = hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
    import pandas as pd
    import platform
    return dict(sources=sources, python=platform.python_version(), numpy=np.__version__, pandas=pd.__version__)


def choose(training, minimum, minimum_days=365):
    eligible = {key: row['metrics']['cagr'] - .5 * row['metrics']['max_drawdown_pct']
                for key, row in training.items() if row['metrics']['trades'] >= minimum and row['metrics'].get('period_days', 0) >= minimum_days}
    return max(eligible, key=lambda k: (eligible[k], k)) if eligible else None


def objective_check(metrics, objectives, minimum, quality_verified=False):
    checks = dict(minimum_trades=metrics['trades'] >= minimum,
                  cagr=metrics['cagr'] >= objectives['cagr'],
                  drawdown=metrics['max_drawdown_pct'] <= objectives['drawdown'])
    if objectives['beat_nifty']:
        checks['beat_nifty'] = metrics.get('excess_cagr') is not None and metrics['excess_cagr'] > 0
    if objectives['cagr'] > 0:
        checks['full_year_for_annual_target'] = metrics.get('period_days', 0) >= 365
    numeric = all(checks.values())
    checks['data_quality_verified'] = quality_verified
    return dict(numeric_constraints_met=numeric, historical_constraints_met=all(checks.values()), checks=checks, promotion_allowed=False)


def period_metrics(account, index, start, end):
    """Correct boundary conventions additively, without rewriting the legacy core."""
    metrics = core.metrics(account, index, start, end)
    metrics['period_days'] = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
    metrics['period_years'] = metrics['period_days'] / 365.25
    metrics['annualized_from_short_window'] = metrics['period_days'] < 365
    # The initial midnight point and the first close have the same date. Avoid
    # legacy daily de-duplication dropping the first session's return.
    points = account['daily_curve']
    returns = [b['equity'] / a['equity'] - 1 for a, b in zip(points, points[1:])]
    sd = statistics.stdev(returns) if len(returns) > 1 else None
    metrics['volatility_pct'] = sd * math.sqrt(252) * 100 if sd is not None else None
    metrics['sharpe'] = statistics.mean(returns) / sd * math.sqrt(252) if sd else None
    dates = [b['end'][:10] for b in index]
    first = bisect_left(dates, start) - 1
    last = bisect_right(dates, end) - 1
    valid = first >= 0 and last > first and (date.fromisoformat(start) - date.fromisoformat(dates[first])).days <= 7 and (date.fromisoformat(end) - date.fromisoformat(dates[last])).days <= 7
    br = (index[last]['close'] / index[first]['close'] - 1) * 100 if valid else None
    years = max(1, (date.fromisoformat(end) - date.fromisoformat(start)).days) / 365.25
    bc = ((1 + br / 100) ** (1 / years) - 1) * 100 if br is not None else None
    metrics.update(benchmark_return_pct=br, benchmark_cagr=bc,
                   excess_return_pct=metrics['return_pct'] - br if br is not None else None,
                   excess_cagr=metrics['cagr'] - bc if bc is not None else None,
                   benchmark_definition='Last close strictly before study start to last close on/before study end; price index, without costs/dividends',
                   sharpe_definition='Daily account returns, including the first session and idle cash; 252-session annualization, zero risk-free rate')
    for row in metrics['annual']:
        year = row['year']
        row['partial'] = (year == start[:4] and start > year + '-01-01') or (year == end[:4] and end < year + '-12-31')
    return metrics


def simulate(prepared, index, request, strategy, start, end, training=False):
    spec = execution_spec(request, strategy, start, end)
    candidates = [];histories = {};purged = 0
    for symbol, (bars, f, masks) in prepared.items():
        mask = masks[strategy['id']]
        histories[symbol + '|1D'] = bars
        for i in np.flatnonzero(mask):
            i = int(i)
            if i + 1 >= len(bars) or not start <= bars[i + 1]['time'][:10] <= end:
                continue
            if bars[i].get('gap'):
                continue
            if not core.review_day(bars, i, spec['rebalance']):
                continue
            if training and (i + spec['exit']['hold'] >= len(bars) or bars[i + spec['exit']['hold']]['end'][:10] > end):
                purged += 1
                continue
            trade = core.trade_path(spec, bars, f, mask, i, end=end)
            if trade:
                trade.update(symbol=symbol, timeframe='1D', pattern='strategy', variant=strategy['id'],
                             id=f'{strategy["id"]}-{symbol}-{i}', horizon_time=trade['exit_time'])
                candidates.append(trade)
    account = core.run_account(candidates, histories, spec)
    verify_account_strict(account, spec)
    # Mark cash-only sessions too; volatility must not silently omit idle periods.
    observed = {p['time'][:10]: p for p in account['daily_curve'] if p['time'] != start + ' 00:00:00'}
    previous = dict(equity=spec['capital'], cash=spec['capital'], positions=0)
    dense = [dict(previous, time=start + ' 00:00:00')]
    days = sorted(set([b['end'][:10] for b in index if start <= b['end'][:10] <= end] + list(observed)))
    for day in days:
        previous = dict(observed.get(day, previous))
        dense.append(dict(previous, time=day + ' 15:30:00'))
    account['daily_curve'] = dense
    metrics = period_metrics(account, index, start, end)
    by_stock = {}
    for symbol in request['symbols']:
        trades = [t for t in account['trades'] if t['symbol'] == symbol]
        by_stock[symbol] = dict(trades=len(trades), net_pnl=round(sum(t['net_pnl'] for t in trades), 2),
            average_return_pct=sum(t['net_return_pct'] for t in trades) / len(trades) if trades else None,
            win_rate=100 * sum(t['net_pnl'] > 0 for t in trades) / len(trades) if trades else None,
            best_trade_pct=max((t['net_return_pct'] for t in trades), default=None),
            max_favorable_pct=max((t['mfe_pct'] for t in trades), default=None),
            max_adverse_pct=max((t['mae_pct'] for t in trades), default=None))
    return dict(metrics=metrics, objective=objective_check(metrics, request['objectives'], request['minimum_trades']),
        trades=account['trades'], daily_curve=dense, per_stock=by_stock,
        diagnostics=core.diagnostics(account, index), candidate_count=len(candidates),
        purged_candidate_count=purged, accounting_checked=True,
        skipped=[dict(symbol=t['symbol'], signal_time=t['signal_time'], entry_time=t['entry_time'],
                      reason=t['skipped_reason']) for t in account['skipped']],
        period=dict(start=start, end=end))


def cache_identity(request, provenance, engines, owner):
    stable = {k:v for k,v in provenance.items() if k not in {'fetched_at', 'as_of_utc', 'fetch_seconds', 'request_id'}}
    return digest(dict(version=VERSION, request=request, data=stable, engines=engines, owner=owner))


def publish(path, data):
    """Only complete files become visible; never overwrite an evidence artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.kanida-', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(data)
        os.link(temporary, path)
    finally:
        os.unlink(temporary)


def conclusions(folds):
    walk = []
    for fold in folds:
        sid = fold['selection']['strategy_id']
        selected = fold['testing'].get(sid)
        walk.append(dict(test_start=fold['test_start'], test_end=fold['test_end'], strategy_id=sid,
            metrics=selected['metrics'] if selected else None, objective=selected['objective'] if selected else None))
    all_numeric = all(r['objective'] and r['objective']['numeric_constraints_met'] for r in walk)
    all_verified = all(r['objective'] and r['objective']['historical_constraints_met'] for r in walk)
    matches = [sid for sid in folds[0]['testing'] if all(f['testing'][sid]['objective']['numeric_constraints_met'] for f in folds)]
    verdict = ('Training-selected strategies met the numeric criteria in every later test fold; data quality and final holdout still require validation.'
               if all_numeric else 'The training-selected strategies did not meet all criteria and minimum sample sizes in every later test fold. This bounded search does not prove the objective impossible.')
    return dict(selected_folds=walk, numeric_criteria_met_all_folds=bool(all_numeric), verified_criteria_met_all_folds=bool(all_verified),
                account_continuity='Independent accounts reset each fold; this is not a continuous compounded track record'), matches, verdict


def read_evidence(path, expected_key):
    envelope = json.loads(path.read_text(encoding='utf-8'))
    if envelope.get('sha256') != digest(envelope.get('evidence')):
        raise ValueError('Evidence checksum failed; cached result was not reused')
    evidence = envelope['evidence']
    if evidence.get('cache_key') != expected_key:
        raise ValueError('Evidence identity mismatch; cached result was not reused')
    return evidence


def run(request, owner='local-pilot', provider=None, output=None, refresh=False, progress=None):
    began = time.perf_counter()
    request = validate(request)
    owner = identifier(owner)
    progress = progress or (lambda message: None)
    progress('Loading completed daily candles and verifying the strategy contract')
    provider = provider or LocalSnapshotProvider()
    as_of = datetime.now(timezone.utc)
    histories, index, provenance = provider.load(request['symbols'], request['folds'][-1]['test_end'], as_of=as_of)
    if set(histories) != set(request['symbols']):
        raise ValueError('Data provider did not return exactly the requested stock universe')
    coverage = validate_data(histories, index, request, as_of=as_of)
    # Independently hash returned candles, even when a future provider supplies its own metadata.
    provenance = dict(provenance, content_hashes={**{s: digest(b) for s, b in histories.items()}, 'NIFTY 50': digest(index)})
    engines = source_identity()
    key = cache_identity(request, provenance, engines, owner)
    folder = Path(output) if output is not None else WORKSPACE / 'reports/research-orchestrator/library' / owner
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / (key + '.json')
    if path.exists() and not refresh:
        evidence = read_evidence(path, key)
        return dict(evidence=evidence, cache_hit=True, seconds=round(time.perf_counter() - began, 3), report=str(path))
    inputs = dict(histories=histories, benchmark=index, provenance=provenance)
    input_key = digest(inputs)
    input_path = folder / 'inputs' / (input_key + '.json.gz')
    if not input_path.exists():
        publish(input_path, gzip.compress(json.dumps(inputs, allow_nan=False, separators=(',', ':')).encode('utf-8')))
    elif digest(json.loads(gzip.decompress(input_path.read_bytes()))) != input_key:
        raise ValueError('Archived market-input checksum failed')
    features = Features(request['strategies'])
    folds = [];prefix_checks = 0
    with warnings.catch_warnings(record=True) as observed:
        warnings.simplefilter('always')
        for number, fold in enumerate(request['folds'], 1):
            progress(f"Fold {number}/{len(request['folds'])}: training through {fold['train_end']}")
            train_data, train_index = features.prepare(histories, index, fold['train_end'], request['strategies'])
            training = {s['id']: simulate(train_data, train_index, request, s, fold['train_start'], fold['train_end'], True)
                        for s in request['strategies']}
            selected = choose(training, request['minimum_trades'], minimum_days=730 if request['objectives']['cagr'] > 0 else 365)
            selection = dict(strategy_id=selected, chosen_before=fold['test_start'], training_evidence_hash=digest(training))
            progress(f"Fold {number}: selection frozen; evaluating {fold['test_start']} to {fold['test_end']}")
            test_data, test_index = features.prepare(histories, index, fold['test_end'], request['strategies'])
            for symbol in request['symbols']:
                n = len(train_data[symbol][0])
                for s in request['strategies']:
                    prefix_checks += 1
                    if not np.array_equal(train_data[symbol][2][s['id']], test_data[symbol][2][s['id']][:n]):
                        raise ValueError('Later candles changed earlier signals: ' + symbol + '/' + s['id'])
            testing = {s['id']: simulate(test_data, test_index, request, s, fold['test_start'], fold['test_end'])
                       for s in request['strategies']}
            folds.append(dict(**fold, selection=selection, training=training, testing=testing))
    walk_forward, matches, verdict = conclusions(folds)
    if source_identity() != engines:
        raise ValueError('Engine sources changed during research; results were not registered')
    evidence = dict(version=VERSION, created=datetime.now(timezone.utc).isoformat(), cache_key=key,
        owner_scope=owner, request=request, provenance=provenance, coverage=coverage, engines=engines,
        input_artifact=dict(path=str(input_path), sha256=input_key), as_of_utc=as_of.isoformat(),
        question_role='Description only. The explicit normalized strategy contract governs execution; this command does not infer arbitrary rules from question text.',
        folds=folds, prefix_checks=prefix_checks, actual_candidates=len(request['strategies']),
        backtest_accounts=len(folds) * len(request['strategies']) * 2,
        walk_forward=walk_forward, post_hoc_test_matches=matches, promotion_allowed=False,
        verdict=verdict,
        selection_rule='Maximum training CAGR minus 0.5 times observed drawdown among candidates with the minimum trades and >=365 training days (>=730 for positive CAGR targets). Test outcomes never choose the candidate.',
        selection_scope='Every predeclared candidate is shown in testing for transparency; editing after seeing these results requires new untouched data.',
        limitations=LIMITATIONS, warnings=sorted({str(w.message) for w in observed}),
        compute_seconds=round(time.perf_counter() - began, 3))
    # A rerun keeps the prior evidence intact.
    if path.exists():
        old = read_evidence(path, key)
        checked = ('folds', 'walk_forward', 'post_hoc_test_matches')
        if digest({k:old[k] for k in checked}) != digest({k:evidence[k] for k in checked}):
            raise ValueError('Identical request/data/code produced different results; determinism check failed and the result was not registered')
        evidence['determinism_check'] = dict(passed=True, compared_to=str(path))
        path = folder / (key + '-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.json')
    envelope = dict(sha256=digest(evidence), evidence=evidence)
    publish(path, json.dumps(envelope, indent=2, allow_nan=False).encode('utf-8'))
    return dict(evidence=evidence, cache_hit=False, seconds=round(time.perf_counter() - began, 3), report=str(path))


def readout(result):
    evidence = result['evidence']
    lines = [f"KANIDA | {evidence['request']['id']} | {'saved evidence' if result['cache_hit'] else 'new research'} | {result['seconds']:.3f}s",
             f"{evidence['actual_candidates']} candidates | {len(evidence['folds'])} walk-forward folds | {evidence['backtest_accounts']} accounts",
             'Each account starts independently with INR ' + str(evidence['request']['account']['capital']),
             'Period       Strategy                End INR    Return   CAGR     Max DD   Trades   Avg/trade  Win rate']
    lines.insert(3, 'Allocation is a ceiling: whole-share quantity is limited by cash, allocation and stop-distance risk, including costs.')
    for fold in evidence['folds']:
        lines.append('Test dates: ' + fold['test_start'] + ' to ' + fold['test_end'])
        for sid, row in fold['testing'].items():
            m = row['metrics']
            avg = f"{m['average_return_pct']:+.2f}%" if m['average_return_pct'] is not None else 'n/a'
            win = f"{m['win_rate']:.1f}%" if m['win_rate'] is not None else 'n/a'
            lines.append(f"{fold['test_start'][:4]}         {sid[:22]:22} {m['ending_equity']:9.2f} {m['return_pct']:+7.2f}% {m['cagr']:+7.2f}% {m['max_drawdown_pct']:7.2f}% {m['trades']:6} {avg:>10} {win:>8}")
        lines.append('  Selected using training only: ' + str(fold['selection']['strategy_id']))
    lines += [evidence['verdict'], 'Research only; not qualified for deployment. Data adjustments and final holdout remain unverified.',
              'Full trades, equity curves, stock-level evidence and rules: ' + result['report']]
    return '\n'.join(lines)
