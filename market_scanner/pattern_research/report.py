"""Read-only result analysis with explicit research coverage and sample limitations."""
from __future__ import annotations
import argparse
import json
import sqlite3
from pathlib import Path
from . import store


def build(run, output=store.ROOT):
    output=Path(output).resolve()
    con=sqlite3.connect((output/'research.sqlite3').as_uri()+'?mode=ro',uri=True)
    con.execute('PRAGMA query_only=ON')
    row=con.execute('SELECT metadata FROM runs WHERE id=?',(run,)).fetchone()
    if row is None:
        raise ValueError('Unknown research run')
    meta=json.loads(row[0])
    stocks=dict(con.execute('SELECT status,count(*) FROM stocks WHERE run=? GROUP BY status',(run,)))
    statuses=dict(con.execute('SELECT status,count(*) FROM cells WHERE run=? GROUP BY status',(run,)))
    coverage=[]
    for pattern,n,occur,ref,wf,sufficient,positive in con.execute('''SELECT pattern_id,count(*),
      sum(CASE WHEN occurrences>0 THEN 1 ELSE 0 END),sum(CASE WHEN reference_n>0 THEN 1 ELSE 0 END),
      sum(CASE WHEN wf_n>0 THEN 1 ELSE 0 END),sum(CASE WHEN wf_n>=20 THEN 1 ELSE 0 END),
      sum(CASE WHEN wf_n>=20 AND wf_mean>0 THEN 1 ELSE 0 END)
      FROM cells WHERE run=? GROUP BY pattern_id ORDER BY pattern_id''',(run,)):
        coverage.append(dict(pattern_id=pattern,cells=n,with_occurrences=occur,with_baseline_trades=ref,
            with_walkforward_trades=wf,walkforward_n_at_least_20=sufficient,positive_with_n_at_least_20=positive))
    examples=[]
    for symbol,tf,pattern,variant,side,n,mean,summary in con.execute('''SELECT symbol,timeframe,pattern_id,
      variant,side,wf_n,wf_mean,summary FROM cells WHERE run=? AND wf_n>=20
      ORDER BY wf_mean DESC LIMIT 12''',(run,)):
        s=json.loads(summary)
        examples.append(dict(symbol=symbol,timeframe=tf,pattern_id=pattern,variant=variant,side=side,n=n,
                             average_net_return_pct=mean,stats=s['walkforward']))
    errors=[dict(symbol=symbol,**json.loads(payload)) for symbol,payload in con.execute(
        "SELECT symbol,metadata FROM stocks WHERE run=? AND status='error'",(run,))]
    integrity_errors=[]
    for symbol,payload in con.execute("SELECT symbol,metadata FROM stocks WHERE run=? AND status='complete'",(run,)).fetchall():
        try:
            store.verify_stock(con,meta,symbol,json.loads(payload),output)
        except Exception as exc:
            integrity_errors.append(dict(symbol=symbol,error=str(exc)))
    con.close()
    expected=len(meta['specifications'])*len(meta['timeframes'])*len(meta['symbols'])
    total=sum(statuses.values())
    complete=stocks.get('complete',0)==len(meta['symbols']) and not errors and not integrity_errors and total==expected and meta['status']=='complete'
    selected_ids={s['pattern_id'] for s in meta['specifications']}
    full_ids=set(meta.get('catalogue_ids',[]))
    full_coverage=bool(full_ids) and selected_ids==full_ids and len(meta['symbols'])==meta.get('source_universe_count') and complete
    report=dict(run=run,status=meta['status'],coverage_complete=complete,source_run=meta['source_run'],
        coverage_scope='selected_run',selected_pattern_count=len(selected_ids),
        full_catalogue_count=len(full_ids) if full_ids else None,
        source_universe_count=meta.get('source_universe_count'),full_catalogue_and_universe_complete=full_coverage,
        modules=meta['modules'],integrity_errors=integrity_errors,
        source_market_latest=meta['source_market_latest'],expected_cells=expected,stored_cells=total,
        stocks=stocks,cell_statuses=statuses,pattern_coverage=coverage,exploratory_examples=examples,errors=errors,
        assumptions='36-month training/6-month tests; minimum20 training trades; 40bps round-trip notional cost; per-cell independent account studies',
        interpretation='Counts summarize separate overlapping study cells. Returns and trade counts are not a pooled portfolio, calibrated probability, or independent replication across patterns.')
    folder=output/run
    store.atomic_json(folder/'analysis.json',report)
    lines=['# KANIDA expanded pattern research results','',f'Run: `{run}`',
        f'\n**Selected-run coverage: {"complete" if complete else "partial / requires attention"}.** '
        f'{stocks.get("complete",0):,}/{len(meta["symbols"]):,} stocks; {total:,}/{expected:,} study cells.',
        f'\nThis run selects {len(selected_ids)} pattern IDs in modules: {", ".join(meta["modules"])}. '
        f'Full catalogue-ID and source-universe coverage: {"complete" if full_coverage else "not established by this run"}. '
        'This counts the registered variants; deferred optional variants are listed in detector notes.',
        f'\nMarket-data snapshot: {meta["source_market_latest"]}. Research uses existing local OHLCV.',
        '\n## What these results mean','',
        'Baseline returns describe past fixed-horizon trades. Walk-forward rules were selected using earlier data and frozen for each subsequent test window. A positive average is an exploratory historical result, not a validated prediction.',
        '\nThe sample threshold is 20 total walk-forward trades. Many patterns, variants and rules were compared; the examples below have not passed a separate multiple-testing-adjusted release holdout. Short-side studies do not model borrowing or contract availability.',
        '\n## Result statuses','', '| Status | Cells |','|---|---:|']
    lines += [f'| {status} | {count:,} |' for status,count in statuses.items()]
    lines += ['','## Catalogue coverage','',
        'Each row counts stock/timeframe/variant/direction studies; overlapping definitions are not independent evidence.',
        '', '| Pattern ID | Cells | Occurrences present | Baseline present | WF trades present | WF N ≥ 20 | Positive, N ≥ 20 |',
        '|---|---:|---:|---:|---:|---:|---:|']
    for c in coverage:
        lines.append('| '+' | '.join(str(c[k]) for k in ('pattern_id','cells','with_occurrences','with_baseline_trades',
            'with_walkforward_trades','walkforward_n_at_least_20','positive_with_n_at_least_20'))+' |')
    lines += ['','## Exploratory cells with the highest average test return','',
        'Ranked only among cells with at least 20 walk-forward trades. Selection of this display uses the test results; it is a research shortlist, not evidence from an untouched final holdout.',
        '', '| Stock | Timeframe | Pattern | Variant | Side | Trades | Mean net return/trade |',
        '|---|---|---|---|---|---:|---:|']
    for e in examples:
        lines.append(f'| {e["symbol"]} | {e["timeframe"]} | {e["pattern_id"]} | {e["variant"]} | {e["side"]} | {e["n"]} | {e["average_net_return_pct"]:.3f}% |')
    if not examples:
        lines.append('| No cells meet the sample threshold | — | — | — | — | — | — |')
    lines += ['','## Reproducibility','',
        'The adjacent manifest records the selected universe, exact source-history checksums, detector specifications, dependencies and code hashes. The code and candles are frozen under this run. Stock artifacts contain every occurrence, baseline/selected trade ledger and walk-forward fold, including failed selections.',
        '', 'Costs: 30 bps round-trip fee plus 10 bps round-trip slippage, deducted as 0.40% of entry notional. Data-quality gaps at entry skip a stale signal; gaps during a held position liquidate at the first available open. No future-gap censoring. Stops win ambiguous same-bar collisions.',
        '', 'Fold summaries are entry-cohort outcomes and can include exits after the named fold. Per-cell results must not be added together as a portfolio. Source adjustment, snapshot-universe and closing-stub limitations remain.']
    if errors:
        lines += ['','## Errors','']+[f'- {e["symbol"]}: {e.get("error")}' for e in errors]
    (folder/'ANALYSIS.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run');parser.add_argument('--output',default=str(store.ROOT))
    args=parser.parse_args()
    report=build(args.run,args.output)
    print(store.dumps({k:report[k] for k in ('run','coverage_complete','stored_cells','expected_cells','stocks','cell_statuses')}))
