"""Headline evidence numbers for one outcome run. Read-only, writes nothing to the store.

This is the reporting query behind `docs/pattern_research/RERUN_REPORT.md`: how many cells
carry >= 20 out-of-sample trades, how many are positive, how many survive Benjamini-Hochberg
in each of the two testing families, the conditional-vs-unconditional medians by timeframe,
and the first-touch barrier table at the declared display pair (+2% / -1%).

It is named ``outcomes_*`` on purpose: ``runner.code_files()`` leaves that prefix out of a
research run's code identity, because reading finished evidence cannot change detection.

    python -m market_scanner.pattern_research.outcomes_summary --run <outcome_run_id>
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
from pathlib import Path

from . import outcomes

TIMEFRAMES = ('1H', '4H', '1D', '1W')


def median(values):
    values = [v for v in values if v is not None]
    return round(statistics.median(values), 4) if values else None


def connect(root):
    path = (Path(root) / outcomes.DB_NAME).resolve()
    con = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=300)
    con.execute('PRAGMA query_only=ON')
    con.row_factory = sqlite3.Row
    return con


def summarise(con, run):
    meta = con.execute('SELECT * FROM outcome_runs WHERE id=?', (run,)).fetchone()
    if meta is None:
        raise SystemExit('unknown outcome run ' + run)
    md = json.loads(meta['metadata'])
    out = dict(outcome_run=run, research_run=meta['research_run'], source_run=meta['source_run'],
               snapshot_id=meta['snapshot_id'], engine_version=meta['engine_version'],
               status=meta['status'], wall_seconds=md.get('wall_seconds'),
               horizons=md.get('horizons'), cost_pct=md.get('cost_pct'),
               replicates=md.get('replicates'),
               symbols={r[0]: r[1] for r in con.execute(
                   'SELECT status,count(*) FROM outcome_symbols WHERE run=? GROUP BY status', (run,))})

    out['cells_total'] = con.execute('SELECT count(*) FROM cell_outcomes WHERE run=?', (run,)).fetchone()[0]
    out['cells_with_occurrences'] = con.execute(
        'SELECT count(*) FROM cell_outcomes WHERE run=? AND events>0', (run,)).fetchone()[0]
    out['buckets_total'] = con.execute('SELECT count(*) FROM bucket_outcomes WHERE run=?', (run,)).fetchone()[0]

    row = con.execute('SELECT count(*) n, SUM(oos_expectancy_pct>0) positive, SUM(oos_n) trades, '
                      'SUM(COALESCE(discovery_q10,0)) q10, SUM(COALESCE(discovery_q05,0)) q05, '
                      'SUM(COALESCE(baseline_beats_unconditional,0)) beats, AVG(oos_expectancy_pct) mean '
                      'FROM cell_outcomes WHERE run=? AND oos_n>=20', (run,)).fetchone()
    expectancies = [r[0] for r in con.execute(
        'SELECT oos_expectancy_pct FROM cell_outcomes WHERE run=? AND oos_n>=20 '
        'AND oos_expectancy_pct IS NOT NULL', (run,))]
    out['cells_oos_n_ge_20'] = dict(
        n=row['n'], positive=row['positive'], out_of_sample_trades=row['trades'],
        bh_q10=row['q10'], bh_q05=row['q05'], baseline_ci_excludes_zero=row['beats'],
        mean_expectancy_pct=None if row['mean'] is None else round(row['mean'], 4),
        median_expectancy_pct=median(expectancies))
    out['cells_by_selection_status'] = {r[0]: r[1] for r in con.execute(
        'SELECT selection_status,count(*) FROM cell_outcomes WHERE run=? GROUP BY selection_status', (run,))}

    # -- the two testing families, never pooled -------------------------------
    out['fdr'] = {}
    for name, table, p, q in (('cells', 'cell_outcomes', 'oos_p_value', 'oos_q_value'),
                              ('buckets', 'bucket_outcomes', 'p_value', 'q_value')):
        r = con.execute(f'SELECT count(*) tested, MAX(fdr_trials) trials, SUM(discovery_q10) q10, '
                        f'SUM(discovery_q05) q05, MIN({p}) minp, MIN({q}) minq, SUM({p}<0.05) p05 '
                        f'FROM {table} WHERE run=? AND {p} IS NOT NULL', (run,)).fetchone()
        out['fdr'][name] = dict(
            tested=r['tested'], trials=r['trials'], q10=r['q10'], q05=r['q05'],
            smallest_p=r['minp'], smallest_q=r['minq'],
            uncorrected_p_lt_0_05=r['p05'],
            uncorrected_p_lt_0_05_share=None if not r['tested'] else round(r['p05'] / r['tested'], 4))

    out['cells_by_timeframe_oos20'] = {}
    for tf in TIMEFRAMES:
        r = con.execute('SELECT count(*) n, SUM(oos_expectancy_pct>0) pos, '
                        'SUM(COALESCE(discovery_q10,0)) q10, SUM(COALESCE(discovery_q05,0)) q05 '
                        'FROM cell_outcomes WHERE run=? AND timeframe=? AND oos_n>=20', (run, tf)).fetchone()
        out['cells_by_timeframe_oos20'][tf] = dict(n=r['n'], positive=r['pos'],
                                                   bh_q10=r['q10'], bh_q05=r['q05'])

    # -- conditional vs the unconditional every-eligible-bar baseline ---------
    out['conditional_vs_baseline'] = {}
    for tf in TIMEFRAMES:
        rows = con.execute(
            'SELECT baseline_mean_net_return_pct b, baseline_diff_mean_net_return_pct d, '
            'baseline_diff_ci95_low lo FROM cell_outcomes WHERE run=? AND timeframe=? '
            'AND baseline_mean_net_return_pct IS NOT NULL '
            'AND baseline_diff_mean_net_return_pct IS NOT NULL', (run, tf)).fetchall()
        out['conditional_vs_baseline'][tf] = dict(
            cells=len(rows),
            median_baseline_mean_net_return_pct=median([r['b'] for r in rows]),
            median_conditional_mean_net_return_pct=median([r['b'] + r['d'] for r in rows]),
            median_difference_pct=median([r['d'] for r in rows]),
            cells_with_positive_difference=sum(1 for r in rows if r['d'] > 0),
            cells_ci95_low_above_zero=sum(1 for r in rows if r['lo'] is not None and r['lo'] > 0))

    # -- first touch of the declared display barrier --------------------------
    out['barrier_' + outcomes.DISPLAY_BARRIER_ID.replace(':', '_').replace('.', '_')] = table = {}
    for tf in TIMEFRAMES:
        rows = con.execute(
            'SELECT barrier_n n, p_target_first t, p_stop_first s, p_neither x, '
            'median_bars_to_target bt, median_bars_to_stop bs FROM cell_outcomes '
            'WHERE run=? AND timeframe=? AND display_barrier_id=? AND barrier_n>0',
            (run, tf, outcomes.DISPLAY_BARRIER_ID)).fetchall()

        def weighted(key):
            total = sum(r['n'] for r in rows if r[key] is not None)
            return None if not total else round(
                sum(r['n'] * r[key] for r in rows if r[key] is not None) / total, 4)

        table[tf] = dict(cells=len(rows), occurrences=sum(r['n'] for r in rows),
                         p_target_first_weighted=weighted('t'), p_stop_first_weighted=weighted('s'),
                         p_neither_weighted=weighted('x'),
                         median_cell_p_target_first=median([r['t'] for r in rows]),
                         median_bars_to_target=median([r['bt'] for r in rows]),
                         median_bars_to_stop=median([r['bs'] for r in rows]))
    out['barrier_break_even_note'] = (
        'a +2%/-1% pair needs P(target first) > 33.33% to break even before costs; ties inside a '
        'bar are counted as the stop')

    out['cell_survivors_q05'] = [dict(r) for r in con.execute(
        'SELECT symbol,timeframe,pattern_id,variant,side,state,oos_n,oos_expectancy_pct,'
        'oos_win_rate_pct,oos_q_value,baseline_diff_mean_net_return_pct,p_target_first '
        'FROM cell_outcomes WHERE run=? AND discovery_q05=1 ORDER BY oos_q_value LIMIT 50', (run,))]
    out['bucket_survivors_q05'] = [dict(r) for r in con.execute(
        'SELECT symbol,timeframe,pattern_id,side,state,dimension,bucket,n,mean_net_return_pct,'
        'diff_mean_net_return_pct,q_value FROM bucket_outcomes WHERE run=? AND discovery_q05=1 '
        'ORDER BY q_value LIMIT 50', (run,))]
    return out


def floor_free_crosscheck(con, run, levels=(0.1, 0.05)):
    """BH over the Student-t p-value alone.

    ``oos_p_value`` is ``max(student_t, stationary_bootstrap)`` and the bootstrap cannot return
    less than ``1/(replicates+1)``. When that floor times the number of trials exceeds the FDR
    level, the official q-values cannot reject anything whatever the data says. This re-runs the
    same correction on the unfloored parametric p-value so the difference is visible instead of
    implied. It is a diagnostic, not a result: the t-test over-rejects on fat-tailed returns.
    """
    rows = con.execute('SELECT rowid,symbol,timeframe,pattern_id,variant,side,state,oos_n,'
                       'oos_expectancy_pct,oos_p_value_t,oos_p_value_bootstrap,oos_q_value '
                       'FROM cell_outcomes WHERE run=? AND oos_p_value_t IS NOT NULL ORDER BY rowid',
                       (run,)).fetchall()
    if not rows:
        return dict(trials=0)
    q = outcomes.benjamini_hochberg([r['oos_p_value_t'] for r in rows])
    found = [dict(symbol=r['symbol'], timeframe=r['timeframe'], pattern_id=r['pattern_id'],
                  variant=r['variant'], side=r['side'], state=r['state'], oos_n=r['oos_n'],
                  oos_expectancy_pct=r['oos_expectancy_pct'], p_value_t=r['oos_p_value_t'],
                  p_value_bootstrap=r['oos_p_value_bootstrap'], q_value_t=round(x, 6),
                  q_value_official=r['oos_q_value'])
             for r, x in zip(rows, q) if x < max(levels)]
    return dict(trials=len(rows), smallest_q=round(min(q), 8),
                discoveries={f'q<{lvl}': sum(1 for x in q if x < lvl) for lvl in levels},
                positive=sum(1 for f in found if (f['oos_expectancy_pct'] or 0) > 0),
                symbols=sorted({f['symbol'] for f in found}), rows=found)


# ----------------------------------------------------- two-stage FDR refinement
#: Stage-2 candidate threshold on the UNFLOORED parametric p-value. Declared here,
#: before any refined p-value was computed. Everything at or above it keeps its
#: stage-1 p-value, which can only be too LARGE (the bootstrap floor inflates it),
#: and an inflated p-value can only lower a refined cell's rank and raise its
#: q-value -- so the mixed family is conservative, never optimistic.
REFINE_P_T_THRESHOLD = 0.01

#: Replicates for the refined stationary bootstrap. The floor is 1/(R+1); at
#: R = 2,000,000 that is 5.0e-7, below 0.05/94,979 = 5.26e-7, so BH rank 1 is
#: reachable at q<0.05 for a run of this size. State the floor with the result.
REFINE_REPLICATES = 2_000_000
REFINE_CHUNK = 20_000

#: Sequential stop: once this many replicate means have matched or beaten the
#: observed one, the p-value is already far above anything BH could select, and
#: more replicates only buy precision we do not need. `replicates_used` is
#: reported per cell so the early stop is visible.
REFINE_STOP_EXCEEDANCES = 1_000
REFINE_MIN_REPLICATES = 20_000


def refined_bootstrap_p(values, block=outcomes.OOS_BOOTSTRAP_BLOCK,
                        replicates=REFINE_REPLICATES,
                        seed=outcomes.BOOTSTRAP_SEED + 2,
                        chunk=REFINE_CHUNK,
                        stop_exceedances=REFINE_STOP_EXCEEDANCES,
                        min_replicates=REFINE_MIN_REPLICATES):
    """``outcomes.bootstrap_p_value`` with a resolvable floor, computed in chunks.

    Same estimator, same null (sample re-centred, H0: mean = 0), same
    Politis-Romano stationary resample with mean block ``block``, same add-one
    correction. Only two things change: the replicate matrix is drawn in chunks
    so 2e6 replicates fit in memory, and the loop stops early once the answer is
    certain to be irrelevant to BH. Chunking re-seeds per chunk, so the draw is
    not bit-identical to a single-shot call of the same size -- it is the same
    estimator, not the same random numbers.
    """
    import numpy as np

    values = np.asarray(values, dtype=np.float64)
    n = values.size
    if n < 2:
        return None, 0, 0
    observed = abs(float(values.mean()))
    centred = values - values.mean()
    exceedances = 0
    done = 0
    index = 0
    while done < replicates:
        size = min(chunk, replicates - done)
        indices = outcomes._stationary_indices(n, size, block, [seed, index])
        means = np.abs(centred[indices].mean(axis=1))
        exceedances += int(np.count_nonzero(means >= observed))
        done += size
        index += 1
        if exceedances >= stop_exceedances and done >= min_replicates:
            break
    return float((exceedances + 1) / (done + 1)), done, exceedances


def _refine_one(job):
    rowid, returns = job['rowid'], job['returns']
    p, used, exceedances = refined_bootstrap_p(
        returns, job['block'], job['replicates'], job['seed'],
        job['chunk'], job['stop_exceedances'], job['min_replicates'])
    return dict(rowid=rowid, p_bootstrap_refined=p, replicates_used=used,
                exceedances=exceedances)


def refine_fdr(con, run, threshold=REFINE_P_T_THRESHOLD, replicates=REFINE_REPLICATES,
               workers=8, levels=(0.1, 0.05), log=None):
    """Two-stage Benjamini-Hochberg with a bootstrap that can resolve rank 1.

    Stage 1 is the run's own screen, unchanged. Stage 2 recomputes the stationary
    block bootstrap at ``replicates`` for every cell whose Student-t p-value is
    below ``threshold``, then re-runs BH across the **whole** family using the
    refined p-value where one was computed and the stage-1 p-value everywhere
    else. Nothing is written to the store; the selection protocol, the baseline
    and the barrier analysis are untouched.
    """
    import time
    from concurrent.futures import ProcessPoolExecutor, as_completed

    log = log or (lambda *_: None)
    family = con.execute('SELECT rowid, oos_p_value, oos_p_value_t FROM cell_outcomes '
                         'WHERE run=? AND oos_p_value IS NOT NULL ORDER BY rowid',
                         (run,)).fetchall()
    trials = len(family)
    floor_stage1 = 1.0 / (outcomes.BOOTSTRAP_REPLICATES + 1)
    floor_stage2 = 1.0 / (replicates + 1)

    candidates = con.execute(
        'SELECT rowid, summary FROM cell_outcomes WHERE run=? AND oos_p_value_t IS NOT NULL '
        'AND oos_p_value_t<? ORDER BY rowid', (run, threshold)).fetchall()
    jobs = []
    for row in candidates:
        returns = ((json.loads(row['summary']).get('selection') or {})
                   .get('out_of_sample') or {}).get('returns') or []
        if len(returns) >= 2:
            jobs.append(dict(rowid=row['rowid'], returns=returns,
                             block=outcomes.OOS_BOOTSTRAP_BLOCK, replicates=replicates,
                             seed=outcomes.BOOTSTRAP_SEED + 2, chunk=REFINE_CHUNK,
                             stop_exceedances=REFINE_STOP_EXCEEDANCES,
                             min_replicates=REFINE_MIN_REPLICATES))
    log(f'refining {len(jobs)} candidates of {trials} trials at R={replicates:,}')

    refined, started = {}, time.monotonic()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        pending = {pool.submit(_refine_one, job): job['rowid'] for job in jobs}
        for done, future in enumerate(as_completed(pending), 1):
            result = future.result()
            refined[result['rowid']] = result
            if done % 50 == 0 or done == len(jobs):
                log(f'  {done}/{len(jobs)} refined, {time.monotonic() - started:.0f}s')
    seconds = round(time.monotonic() - started, 1)

    # BH across the whole family, refined where computed, stage-1 elsewhere.
    merged = []
    for row in family:
        hit = refined.get(row['rowid'])
        if hit and hit['p_bootstrap_refined'] is not None:
            # the run's own rule, at the finer resolution
            p = max(row['oos_p_value_t'], hit['p_bootstrap_refined'])
        else:
            p = row['oos_p_value']
        merged.append((row['rowid'], p))
    q = outcomes.benjamini_hochberg([p for _, p in merged])
    by_rowid = {rowid: (p, qv) for (rowid, p), qv in zip(merged, q)}

    discoveries = {f'q<{lvl}': sum(1 for x in q if x < lvl) for lvl in levels}
    top = sorted(((rowid, p, qv) for rowid, (p, qv) in by_rowid.items()), key=lambda r: r[2])[:10]
    detail = []
    for rowid, p, qv in top:
        r = con.execute(
            'SELECT symbol,timeframe,pattern_id,variant,side,state,oos_n,oos_expectancy_pct,'
            'oos_win_rate_pct,oos_p_value,oos_p_value_t,oos_p_value_bootstrap,oos_q_value,'
            'baseline_diff_mean_net_return_pct,baseline_diff_ci95_low,baseline_diff_ci95_high,'
            'stability_edge_concentrated,stability_years,stability_positive_years,p_target_first '
            'FROM cell_outcomes WHERE rowid=?', (rowid,)).fetchone()
        hit = refined.get(rowid) or {}
        detail.append(dict(dict(r), p_refined=p, q_refined=qv,
                           p_bootstrap_refined=hit.get('p_bootstrap_refined'),
                           replicates_used=hit.get('replicates_used')))
    return dict(
        stage2_threshold_p_t=threshold, trials=trials, candidates=len(jobs),
        replicates=replicates, stage1_floor=round(floor_stage1, 9),
        stage2_floor=round(floor_stage2, 12),
        bh_rank1_requirement={f'q<{lvl}': lvl / trials for lvl in levels},
        stage2_floor_binds_at_rank1={f'q<{lvl}': bool(floor_stage2 >= lvl / trials) for lvl in levels},
        unreachable_ranks={f'q<{lvl}': max(0, int(floor_stage2 * trials / lvl))
                           for lvl in levels},
        discoveries=discoveries, smallest_q=round(min(q), 10), smallest_p=min(p for _, p in merged),
        refined_cells_at_stage2_floor=sum(
            1 for h in refined.values()
            if h['p_bootstrap_refined'] is not None and h['p_bootstrap_refined'] <= floor_stage2 * 1.001),
        mixed_family_note=(
            'cells above the stage-2 threshold keep their stage-1 p-value, which the 1/401 '
            'bootstrap floor can only inflate; an inflated p-value lowers a refined cell rank '
            'and raises its q-value, so this mixture is conservative'),
        seconds=seconds, workers=workers, top10_by_q=detail,
        refined_by_rowid={str(k): v for k, v in refined.items()})


def crosscheck_detail(con, run, rows, refined=None):
    """Full evidence for a handful of named cells: baseline difference + CI, the
    year-by-year table, and the refined q-value when one was computed."""
    out = []
    for spec in rows:
        r = con.execute(
            'SELECT rowid,* FROM cell_outcomes WHERE run=? AND symbol=? AND timeframe=? '
            'AND pattern_id=? AND variant=? AND side=? AND state=?',
            (run, spec['symbol'], spec['timeframe'], spec['pattern_id'], spec['variant'],
             spec['side'], spec['state'])).fetchone()
        if r is None:
            continue
        summary = json.loads(r['summary'])
        stability = (summary.get('stability') or {}).get('all_history') or {}
        entry = dict(
            symbol=r['symbol'], timeframe=r['timeframe'], pattern_id=r['pattern_id'],
            variant=r['variant'], side=r['side'], state=r['state'], oos_n=r['oos_n'],
            oos_expectancy_pct=r['oos_expectancy_pct'], oos_win_rate_pct=r['oos_win_rate_pct'],
            p_value_t=r['oos_p_value_t'], p_value_bootstrap_400=r['oos_p_value_bootstrap'],
            q_value_official=r['oos_q_value'],
            baseline_status=r['baseline_status'],
            baseline_mean_net_return_pct=r['baseline_mean_net_return_pct'],
            baseline_diff_mean_net_return_pct=r['baseline_diff_mean_net_return_pct'],
            baseline_diff_ci95=[r['baseline_diff_ci95_low'], r['baseline_diff_ci95_high']],
            baseline_window_coverage=r['baseline_window_coverage'],
            stability_horizon=r['stability_horizon'], stability_years=r['stability_years'],
            stability_positive_years=r['stability_positive_years'],
            stability_edge_concentrated=r['stability_edge_concentrated'],
            by_year=stability.get('by_year'),
            all_history_stats={k: stability.get('stats', {}).get(k)
                               for k in ('n', 'win_rate_pct', 'expectancy_pct',
                                         'expectancy_ci95', 'profit_factor')},
            p_target_first=r['p_target_first'], p_stop_first=r['p_stop_first'])
        if refined and str(r['rowid']) in (refined.get('refined_by_rowid') or {}):
            hit = refined['refined_by_rowid'][str(r['rowid'])]
            entry['p_bootstrap_refined'] = hit['p_bootstrap_refined']
            entry['replicates_used'] = hit['replicates_used']
        out.append(entry)
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True, help='outcome run id')
    parser.add_argument('--output', default=str(outcomes.ROOT))
    parser.add_argument('--crosscheck', action='store_true',
                        help='also run the floor-free Student-t BH diagnostic')
    parser.add_argument('--refine', action='store_true',
                        help='two-stage FDR: recompute the bootstrap at high resolution for '
                             'candidates and re-run BH across the whole family')
    parser.add_argument('--refine-threshold', type=float, default=REFINE_P_T_THRESHOLD)
    parser.add_argument('--refine-replicates', type=int, default=REFINE_REPLICATES)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--write')
    args = parser.parse_args(argv)
    con = connect(args.output)
    try:
        result = summarise(con, args.run)
        if args.crosscheck or args.refine:
            result['floor_free_crosscheck'] = floor_free_crosscheck(con, args.run)
        if args.refine:
            result['two_stage_fdr'] = refine_fdr(
                con, args.run, args.refine_threshold, args.refine_replicates,
                args.workers, log=lambda m: print(m, flush=True))
            result['crosscheck_cells_detail'] = crosscheck_detail(
                con, args.run, result['floor_free_crosscheck'].get('rows', []),
                result['two_stage_fdr'])
            result['two_stage_fdr'].pop('refined_by_rowid', None)
    finally:
        con.close()
    text = json.dumps(result, indent=1, default=str)
    print(text)
    if args.write:
        Path(args.write).write_text(text, encoding='utf-8')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
