"""CLI contract probe for the existing unpublished interpreter.

This is a diagnostic baseline, not a new interpreter, profitability test, or
claim that arbitrary requests are supported. There are no LLM or broker calls.
Only newly named report files in the requested output directory are written.
"""
from __future__ import annotations
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from unittest.mock import patch


def field(data,path):
    for part in path.split('.'):
        if not isinstance(data,dict):return None
        data=data.get(part)
    return data


def score_case(case,response):
    findings=[];spec=response.get('spec',{})
    for key,expected in case.get('expect',{}).items():
        actual=response.get('action') if key=='action' else field(spec,key)
        if actual!=expected:findings.append(dict(check=key,expected=expected,actual=actual))
    for expected in case.get('conditions',[]):
        if not any(b.get('kind')==expected['kind'] and b.get('value')==expected['value'] for b in spec.get('conditions',[])):
            findings.append(dict(check='condition_preserved',expected=expected,actual=spec.get('conditions',[])))
    # Current adapter exposes unresolved requirements as blockers. Human
    # evaluation still checks the quality and meaning of its explanation.
    if case.get('requires_clarification') and not response.get('blockers'):
        findings.append(dict(check='unresolved_requirement_acknowledged',expected='An explicit blocker/clarification',actual=response.get('notes',[])))
    return findings


def run(output,only=None):
    from market_scanner import strategy_lab as engine, backtest_store as store
    cases=json.loads(Path(__file__).with_name('scenarios.json').read_text(encoding='utf-8'))
    if only:cases=[c for c in cases if c['persona']==only]
    # Supply read-only context to avoid the draft capability function's cache
    # writes. This still calls its actual interpret and validate functions.
    with store.connection() as con:run_id=store.active_run(con)
    frames=store.load_history(run_id,'TITAN');bars=frames['1D'][0]
    caps=dict(first=bars[0]['end'][:10],last=bars[-1]['end'][:10])
    rows=[]
    with patch.object(engine,'capabilities',return_value=caps):
        base=engine.default_spec()
        for case in cases:
            previous=deepcopy(base) if case.get('previous') else None
            if previous:previous.update(deepcopy(case['previous']))
            started=time.perf_counter()
            try:
                response=engine.interpret(case['prompt'],previous);findings=score_case(case,response)
            except Exception as error:
                response={'error':type(error).__name__+': '+str(error)};findings=[dict(check='no_unhandled_exception',actual=response['error'])]
            rows.append(dict(id=case['id'],persona=case['persona'],prompt=case['prompt'],latency_ms=round((time.perf_counter()-started)*1000,2),
                passed=not findings,findings=findings,response=response,evaluation_note=case['question']))
            print(('PASS' if not findings else 'FAIL')+' '+case['id']+' — '+str(len(findings))+' contract findings',flush=True)
    result=dict(created=datetime.now(timezone.utc).isoformat(),kind='interpretation_contract_baseline',engine='market_scanner.strategy_lab.interpret',
        source_run=run_id,cases=len(rows),passed=sum(r['passed'] for r in rows),failed=sum(not r['passed'] for r in rows),rows=rows,
        limitations=['No backtest or walk-forward was run by this command.','These assertions test intent preservation, not general language understanding.',
            'Timing measures local interpretation only, not the one-minute research objective.','Read-only history context replaces the draft capability loader to avoid creating its caches.'])
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ');json_path=output/('intent-baseline-'+stamp+'.json')
    json_path.write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
    lines=['# CLI interpretation baseline','',f"{result['passed']} / {result['cases']} checks passed; {result['failed']} cases have findings.",
        '', 'This is a test of the unfinished local interpreter, not of strategy profitability or end-to-end research latency.', '',
        '| Persona | Scenario | Result | Findings |','|---|---|---|---|']
    for row in rows:lines.append('| '+row['persona']+' | '+row['id']+' | '+('PASS' if row['passed'] else 'FAIL')+' | '+', '.join(f['check'] for f in row['findings'])+' |')
    lines+=['','## Interpretation expectations','']
    for row in rows:lines+=['- **'+row['id']+'**: '+row['evaluation_note']]
    lines+=['','Machine-readable details: '+json_path.name,'','Existing engines were not modified; no broker or paid model calls were made.']
    report_path=json_path.with_suffix('.md');report_path.write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print(json.dumps(dict(passed=result['passed'],failed=result['failed'],report=str(report_path)),indent=2))
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',default='reports/research-orchestrator');parser.add_argument('--persona',choices=['ordinary','ambitious','expert','investor']);args=parser.parse_args()
    run(args.output,args.persona)


if __name__=='__main__':main()
