"""Independent catalogue, event-contract and prefix-causality integration gate."""
from __future__ import annotations

import gzip
import hashlib
import json
import time
from pathlib import Path

from . import runner, store


def _visible(groups, last):
    return {key:[e for e in events if e['signal_index']<=last] for key,events in groups.items()}


def identity_hashes():
    """Hash exactly the ``pattern_research`` files that carry a run's identity.

    Derived from ``runner.code_files()`` rather than restating its rule, so the
    gate and the runner can never disagree about which files they are talking
    about.  ``runner.prepare`` looks every recorded name up in that same set; a
    file the runner deliberately leaves out of run identity (``outcomes*.py``,
    which reads finished research and cannot change detection) must not be
    recorded here or the gate could never be satisfied again.
    """
    names={p.name for p in runner.code_files()
           if p.parent.name=='pattern_research' and p.suffix=='.py'}
    return {p.name:hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (runner.PACKAGE/'pattern_research').glob('*.py') if p.name in names}


def validate(symbol='TITAN'):
    modules,specs=runner.registry(runner.MODULES)
    catalogue=json.loads((runner.PACKAGE.parent/'docs'/'PATTERN_CATALOGUE_PROPOSAL.json').read_text(encoding='utf-8'))
    expected={e['id'] for e in catalogue['entries']}
    actual={s['pattern_id'] for s in specs}
    if actual!=expected:
        raise ValueError(f'Catalogue mismatch; missing={sorted(expected-actual)}, extra={sorted(actual-expected)}')
    with runner.readonly(runner.PACKAGE/'output'/'backtests.sqlite3') as con:
        source=con.execute("SELECT value FROM settings WHERE key='active_run'").fetchone()[0]
        metadata=json.loads(con.execute('SELECT payload FROM stocks WHERE run=? AND symbol=?',(source,symbol)).fetchone()[0])
    filename=hashlib.sha256(symbol.encode()).hexdigest()+'.json.gz'
    raw=gzip.decompress((runner.PACKAGE/'output'/'history'/source/filename).read_bytes())
    if hashlib.sha256(raw).hexdigest()!=metadata['history_sha256']:
        raise ValueError('Validation source-history hash mismatch')
    frames=json.loads(raw)
    rows=[]
    hashes=identity_hashes()
    for tf,(history,quality) in frames.items():
        # Recent history contains a different market regime from the synthetic unit fixtures.
        bars=history[-1600:]
        for module in modules:
            start=time.monotonic()
            definitions=module.specifications()
            full=module.detect(bars,tf)
            runner.validate_events(full,definitions,bars)
            full={runner.key(s):full.get(runner.key(s),[]) for s in definitions}
            cuts=sorted({min(len(bars)-1,i) for i in (159,399,799,1199) if i<len(bars)})
            for cut in cuts:
                prefix=module.detect(bars[:cut+1],tf)
                runner.validate_events(prefix,definitions,bars[:cut+1])
                prefix={runner.key(s):prefix.get(runner.key(s),[]) for s in definitions}
                if prefix!=_visible(full,cut):
                    mismatches=[str(k) for k in full if prefix.get(k)!=_visible(full,cut).get(k)]
                    raise ValueError(f'Future-dependent detector output: {module.__name__}/{tf}/{cut}: {mismatches[:8]}')
            row=dict(module=module.__name__,timeframe=tf,bars=len(bars),prefixes=cuts,
                     observations=sum(map(len,full.values())),seconds=round(time.monotonic()-start,2))
            rows.append(row)
            print(store.dumps(row),flush=True)
    after=identity_hashes()
    if after!=hashes:
        raise ValueError('Source changed during validation; repeat after workers finish')
    result=dict(succeeded=True,checked_at=runner.stamp(),symbol=symbol,source_run=source,
                history_sha256=metadata['history_sha256'],pattern_ids=sorted(actual),
                directional_variant_count=len(specs),code_hashes=hashes,checks=rows,
                scope='Real-data event contracts and causal prefix equivalence; complements detector fixtures and evaluator tests')
    store.atomic_json(runner.PACKAGE/'output'/'expanded_research'/'independent_validation.json',result)
    return result


if __name__=='__main__':
    result=validate()
    print(store.dumps(dict(succeeded=result['succeeded'],patterns=len(result['pattern_ids']),
                          directional_variants=result['directional_variant_count'])),flush=True)
