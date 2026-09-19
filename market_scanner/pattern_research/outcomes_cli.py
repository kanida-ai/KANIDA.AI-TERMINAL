"""Resumable, multi-process "what happened next" outcome pass over a frozen research run.

Reads the frozen histories of an existing run (``manifest.json`` + ``history/*.json.gz``),
re-detects occurrences with the *same* versioned detectors (code hashes are verified
against the run manifest), and writes forward-outcome evidence to
``output/expanded_research/outcomes.sqlite3``.

It never writes to the app database, the source OHLCV database, or the research store.

    python -m market_scanner.pattern_research.outcomes_cli \
        --run 8ae6ddc251e80668239e --symbols RELIANCE TITAN --timeframes 1D 1W --workers 4
"""
from __future__ import annotations

import argparse
import ast
import gzip
import hashlib
import json
import os
import sqlite3
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from . import outcomes, store
from .runner import key, registry, validate_events

PACKAGE = Path(__file__).resolve().parents[1]


def stamp():
    return datetime.now(timezone.utc).isoformat()


def load_manifest(run, root):
    path = Path(root) / run / 'manifest.json'
    if not path.exists():
        raise ValueError('No frozen research run at ' + str(path))
    return json.loads(path.read_text(encoding='utf-8'))


def detection_closure(modules):
    """Every ``market_scanner`` source file whose content can change detector output.

    Walked from the detector modules through their package-relative imports, so it
    stays correct as the legacy replay chain moves; nothing is hard-coded.  Files
    outside this closure (the runner, the live scanner's data/engine layer) cannot
    change which occurrences a frozen history produces.
    """
    seen, closure = set(), set()
    stack = ['pattern_research.common', 'pattern_research.evaluation']
    stack += ['pattern_research.' + name for name in modules]
    while stack:
        name = stack.pop()
        if name in seen:
            continue
        seen.add(name)
        path = PACKAGE / (name.replace('.', '/') + '.py')
        if not path.exists():
            continue
        closure.add(str(Path(name.replace('.', '/') + '.py')))
        for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
            target = None
            if isinstance(node, ast.ImportFrom) and node.level:
                parts = name.split('.')[:-node.level]
                target = '.'.join(parts + ([node.module] if node.module else []))
            elif isinstance(node, ast.ImportFrom) and (node.module or '').startswith('market_scanner.'):
                target = node.module.split('.', 1)[1]
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith('market_scanner.'):
                        stack.append(alias.name.split('.', 1)[1])
            if target:
                stack.append(target)
                # ``from .package import name`` may name a module, not an attribute.
                for alias in getattr(node, 'names', ()):
                    stack.append(target + '.' + alias.name)
    return closure


def reproduces_frozen_detection(manifest, symbol, modules):
    """Re-detect one symbol and compare against the frozen run's own stored events.

    A fact, not an argument: it decides whether drift in a transitively imported
    module actually changed what the detectors emit on a frozen history.
    """
    artifact = store.stock_artifact(manifest['id'], symbol, manifest['output'])
    if not Path(artifact).exists():
        return None
    frozen = json.loads(gzip.decompress(Path(artifact).read_bytes()))
    stored = {(c['timeframe'],) + key(c): c['occurrence_events'] for c in frozen['cells']}
    frames = json.loads(gzip.decompress(Path(manifest['histories'][symbol]).read_bytes()))
    loaded, _ = registry(modules)
    matched, differing, events = 0, [], 0
    for tf in manifest['timeframes']:
        bars, _ = frames.get(tf, ([], {}))
        found = {}
        for module in loaded:
            found.update(module.detect(bars, tf))
        for identity, old in stored.items():
            if identity[0] != tf:
                continue
            live = found.get(identity[1:], [])
            if outcomes.dumps(live) == outcomes.dumps(old):
                matched += 1
                events += len(old)
            else:
                differing.append(list(identity))
    return dict(symbol=symbol, cells_matched=matched, cells_differing=len(differing),
                events_compared=events, examples=differing[:5],
                reproduced=not differing)


def verify_detector_code(manifest, modules=None, probe=None):
    """Refuse to re-detect with code that differs from the run's frozen detectors.

    Two tiers.  A changed ``pattern_research`` detection file is always fatal.  A
    changed file that detection only reaches transitively (the legacy replay chain
    and its imports) is fatal *unless* a re-detection probe reproduces the frozen
    run's occurrences byte for byte.  Everything else is recorded as drift.
    """
    modules = modules or manifest['modules']
    closure = detection_closure(modules)
    core = {path for path in closure if path.replace('\\', '/').startswith('pattern_research/')}
    fatal, indirect, drift = [], [], []
    for relative, digest in manifest['code_hashes'].items():
        live = PACKAGE / relative
        current = hashlib.sha256(live.read_bytes()).hexdigest() if live.exists() else None
        if current == digest:
            continue
        bucket = fatal if relative in core else indirect if relative in closure else drift
        bucket.append(relative)
    if fatal:
        raise ValueError('Detection code differs from the frozen run; re-detection would not '
                         'reproduce its occurrences: ' + ', '.join(sorted(fatal)))
    result = dict(detection_files=len(closure), detection_core_files=len(core),
                  changed_in_detection_closure=sorted(indirect),
                  code_drift_outside_detection=sorted(drift), reproduction_probe=None)
    if not indirect:
        return dict(result, detection_verified='by_code_hash')
    evidence = reproduces_frozen_detection(manifest, probe, modules) if probe else None
    if not evidence or not evidence['reproduced']:
        raise ValueError('Modules that detection imports have changed (' +
                         ', '.join(sorted(indirect)) + ') and re-detection could not be shown '
                         'to reproduce the frozen occurrences; rerun the research instead')
    return dict(result, detection_verified='by_reproduction_probe', reproduction_probe=evidence)


LABELS_DB = PACKAGE.parent / 'db' / 'kanida.db'


def load_labels(symbols, path=None):
    """Read-only sector / index-membership labels for the cell-level columns.

    These are a CURRENT snapshot, not point-in-time: a stock's index membership and
    sector today are not what they were in 2015.  They are stored so results can be
    grouped across symbols, never used as a point-in-time input to a decision.
    """
    path = Path(path or LABELS_DB)
    if not path.exists():
        return {}, dict(available=False, reason='labels database not found: ' + str(path))
    con = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=30)
    con.execute('PRAGMA query_only=ON')
    try:
        rows = con.execute('SELECT symbol,sector,in_nifty100,in_nifty200,in_nifty500,is_fno '
                           'FROM instrument_labels').fetchall()
    except sqlite3.Error as exc:
        con.close()
        return {}, dict(available=False, reason='instrument_labels unreadable: ' + str(exc))
    con.close()
    wanted = set(symbols)
    labels = {}
    for symbol, sector, n100, n200, n500, fno in rows:
        if symbol not in wanted:
            continue
        flags = dict(in_nifty100=n100, in_nifty200=n200, in_nifty500=n500)
        tier = next((name for column, name in outcomes.CAP_TIERS if flags.get(column)), None)
        labels[symbol] = dict(sector=sector, market_cap_tier=tier,
                              is_fno=None if fno is None else int(fno))
    return labels, dict(available=True, source=str(path), matched=len(labels),
                        requested=len(wanted),
                        tier_definition='index-membership proxy, current snapshot, '
                                        'not point-in-time',
                        tiers=[name for _, name in outcomes.CAP_TIERS])


def plan(manifest, args):
    modules = args.modules or manifest['modules']
    specs = [s for s in manifest['specifications']
             if (not args.patterns or s['pattern_id'] in args.patterns)
             and (not args.families or s['family'] in args.families)
             and (not args.sides or s['side'] in args.sides)]
    if not specs:
        raise ValueError('Pattern/family/side filter selected no study cells')
    timeframes = args.timeframes or manifest['timeframes']
    unknown = set(timeframes) - set(manifest['timeframes'])
    if unknown:
        raise ValueError('Timeframes not present in the frozen run: ' + str(sorted(unknown)))
    symbols = sorted(set(args.symbols)) if args.symbols else sorted(manifest['symbols'])
    missing = set(symbols) - set(manifest['symbols'])
    if missing:
        raise ValueError('Symbols not in the frozen run: ' + str(sorted(missing)))
    override = {tf: int(v) for tf, v in (p.split('=') for p in (args.horizon or []))} or None
    for tf in timeframes:
        outcomes.horizon_grid(tf, override)
    identity = dict(research_run=manifest['id'], source_run=manifest['source_run'],
                    modules=sorted(modules), timeframes=sorted(timeframes),
                    specifications=[key(s) for s in specs],
                    horizons={tf: outcomes.horizon_grid(tf, override) for tf in timeframes},
                    cost_pct=outcomes.COST_PCT, replicates=int(args.replicates),
                    baseline=not args.no_baseline, last_n=int(args.last_n),
                    barrier_grid=[p['id'] for p in outcomes.BARRIER_GRID],
                    bucket_minimum=outcomes.BUCKET_MIN_SAMPLE,
                    engine_version=outcomes.OUTCOME_ENGINE_VERSION,
                    protocol='outcome-v1-36m-train-6m-test-min20-40bps')
    run = hashlib.sha256(outcomes.dumps(identity).encode()).hexdigest()[:20]
    labels, label_source = load_labels(symbols, args.labels_db)
    return dict(identity, id=run, symbols=symbols, modules=modules, specs=specs,
                horizon_override=override, snapshot_id=args.snapshot_id,
                history_hashes={s: manifest['history_hashes'][s] for s in symbols},
                histories={s: manifest['histories'][s] for s in symbols},
                labels=labels, label_source=label_source,
                display_grid={tf: outcomes.display_grid(tf, outcomes.horizon_grid(tf, override))
                              for tf in timeframes},
                output=str(Path(args.output).resolve()), created_at=stamp(), status='prepared',
                done=0, total=len(symbols), errors=0)


def process_symbol(job, symbol):
    started = time.monotonic()
    raw = gzip.decompress(Path(job['histories'][symbol]).read_bytes())
    digest = hashlib.sha256(raw).hexdigest()
    if digest != job['history_hashes'][symbol]:
        raise ValueError('Frozen OHLCV history integrity failure: ' + symbol)
    frames = json.loads(raw)
    modules, _ = registry(job['modules'])
    specs = job['specs']
    wanted = {key(s) for s in specs}
    cells, coverage = [], {}
    for tf in job['timeframes']:
        bars, quality = frames.get(tf, ([], {}))
        coverage[tf] = dict(bars=len(bars), quality=quality,
                            first=bars[0]['time'] if bars else None,
                            last=bars[-1]['end'] if bars else None)
        groups = {}
        for module in modules:
            found = module.detect(bars, tf)
            validate_events(found, module.specifications(), bars)
            for identity, events in found.items():
                if identity in wanted:
                    groups[identity] = events
        # One unconditional-baseline cache per timeframe: every cell of this stock and
        # timeframe shares the same eligible-bar universe, so it is computed once.
        cache = {}
        for spec in specs:
            events = groups.get(key(spec), [])
            for state in spec['states']:
                subset = [e for e in events if e['state'] == state]
                cell = outcomes.outcome_cell(bars, subset, tf, spec['side'], state,
                                             job['horizon_override'],
                                             keep_occurrences=not job.get('summary_only'),
                                             cache=cache, baseline=not job.get('no_baseline'),
                                             replicates=job['replicates'],
                                             last_n=job.get('last_n',
                                                            outcomes.LAST_N_OCCURRENCES))
                cell.update(pattern_id=spec['pattern_id'], variant=spec['variant'],
                            side=spec['side'], family=spec.get('family'),
                            name=spec.get('name'),
                            definition_version=spec.get('definition_version'))
                cells.append(cell)
    result = dict(run=job['id'], research_run=job['research_run'], source_run=job['source_run'],
                  symbol=symbol, snapshot_id=job.get('snapshot_id'), history_sha256=digest,
                  outcome_engine_version=outcomes.OUTCOME_ENGINE_VERSION,
                  horizons=job['horizons'], coverage=coverage,
                  labels=(job.get('labels') or {}).get(symbol),
                  seconds=round(time.monotonic() - started, 2), cells=cells)
    path, artifact_digest = outcomes.write_symbol_artifact(job['id'], symbol, result, job['output'])
    return dict(symbol=symbol, path=path, digest=artifact_digest, seconds=result['seconds'],
                cells=len(cells))


def execute(job, workers, log=print):
    con = outcomes.connect(job['output'])
    run = job['id']
    outcomes.save_outcome_run(con, dict(job, specs=None, histories=None, status='running',
                                        started_at=stamp(), pid=os.getpid()))
    done = outcomes.completed_symbols(con, run)
    todo = [s for s in job['symbols'] if s not in done]
    log(outcomes.dumps(dict(run=run, total=len(job['symbols']), resumed=len(done),
                            todo=len(todo), cells_per_symbol_estimate=len(job['specs']) *
                            len(job['timeframes']))))
    state = dict(done=len(done), errors=0, occurrence_rows=0, cell_rows=0)
    started = time.monotonic()
    timings = {}
    try:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            pending, remaining = {}, iter(todo)

            def submit_next():
                symbol = next(remaining, None)
                if symbol is not None:
                    pending[pool.submit(process_symbol, job, symbol)] = symbol

            for _ in range(min(len(todo), workers * 2)):
                submit_next()
            while pending:
                future = next(as_completed(pending))
                symbol = pending.pop(future)
                try:
                    result = future.result()
                    cells, rows = outcomes.commit_symbol(con, run, symbol, result['path'],
                                                         result['digest'], job['output'])
                    state['cell_rows'] += cells
                    state['occurrence_rows'] += rows
                    timings[symbol] = result['seconds']
                    log(outcomes.dumps(dict(symbol=symbol, seconds=result['seconds'], cells=cells,
                                            occurrences=rows, done=state['done'] + 1,
                                            total=len(job['symbols']))))
                except Exception as exc:
                    error = dict(error=str(exc), traceback=traceback.format_exc())
                    outcomes.save_symbol_error(con, run, symbol, error)
                    state['errors'] += 1
                    log(outcomes.dumps(dict(symbol=symbol, error=str(exc))))
                state['done'] += 1
                submit_next()
        status = 'complete_with_errors' if state['errors'] else 'complete'
        # Multiplicity belongs to the run, so BH runs once every symbol is committed.
        # Cells and condition buckets are separate families and are never pooled.
        state['fdr'] = outcomes.apply_all_fdr(con, run)
        log(outcomes.dumps(state['fdr']))
    except BaseException as exc:
        outcomes.save_outcome_run(con, dict(job, specs=None, histories=None, status='interrupted',
                                            error=str(exc), **state))
        raise
    finally:
        con.commit()
    outcomes.save_outcome_run(con, dict(job, specs=None, histories=None, status=status,
                                        finished_at=stamp(), wall_seconds=round(time.monotonic() - started, 2),
                                        seconds_per_symbol=timings, **state))
    con.close()
    return state, timings


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True, help='frozen research run id')
    parser.add_argument('--symbols', nargs='+')
    parser.add_argument('--timeframes', nargs='+')
    parser.add_argument('--patterns', nargs='+', help='pattern_id filter')
    parser.add_argument('--families', nargs='+')
    parser.add_argument('--sides', nargs='+', choices=['long', 'short'])
    parser.add_argument('--modules', nargs='+')
    parser.add_argument('--horizon', nargs='+', metavar='TF=BARS',
                        help='override the horizon grid, e.g. 1D=15 1W=10')
    parser.add_argument('--snapshot-id', help='market-data snapshot this history came from')
    parser.add_argument('--summary-only', action='store_true',
                        help='skip per-occurrence rows (aggregates only)')
    parser.add_argument('--no-baseline', action='store_true',
                        help='skip the unconditional every-eligible-bar baseline')
    parser.add_argument('--replicates', type=int, default=outcomes.BOOTSTRAP_REPLICATES,
                        help='bootstrap replicates for the baseline CI and the p-value')
    parser.add_argument('--last-n', type=int, default=outcomes.LAST_N_OCCURRENCES,
                        help='how many recent occurrences to keep per cell for display')
    parser.add_argument('--labels-db', help='read-only database holding instrument_labels '
                                            '(sector / index membership); default db/kanida.db')
    parser.add_argument('--probe', help='symbol used to prove re-detection still reproduces '
                                       'the frozen run when imported modules have changed')
    parser.add_argument('--refresh-fdr', action='store_true',
                        help='recompute Benjamini-Hochberg q-values for an existing run and exit')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--output', default=str(outcomes.ROOT))
    parser.add_argument('--plan-only', action='store_true')
    args = parser.parse_args(argv)
    if not 1 <= args.workers <= 8:
        parser.error('workers must be between 1 and 8')
    manifest = load_manifest(args.run, args.output)
    manifest['output'] = str(Path(args.output).resolve())
    job = plan(manifest, args)
    job['code_verification'] = verify_detector_code(manifest, job['modules'],
                                                    args.probe or job['symbols'][0])
    job['summary_only'] = args.summary_only
    job['no_baseline'] = args.no_baseline
    if args.refresh_fdr:
        con = outcomes.connect(job['output'])
        print(outcomes.dumps(outcomes.apply_all_fdr(con, job['id'])))
        con.close()
        return 0
    if args.plan_only:
        print(outcomes.dumps({k: v for k, v in job.items()
                              if k not in ('specs', 'histories', 'history_hashes', 'symbols')}))
        return 0
    state, _ = execute(job, args.workers)
    print(outcomes.dumps(dict(run=job['id'], **state)))
    return 1 if state['errors'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
