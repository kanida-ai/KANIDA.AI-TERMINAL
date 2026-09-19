"""Exercise the new intent adapter against baseline and varied edit scenarios."""
from datetime import datetime,timezone
import json
from pathlib import Path
from .intent import interpret,default_spec
from .evaluate import score_case


def run():
    cases=json.loads(Path(__file__).with_name('scenarios.json').read_text(encoding='utf-8'))
    cases.extend([
        dict(id='variation_fno',persona='variation',prompt='Use F and O equities and target 85% CAGR with maximum drawdown 12%.',expect={'universe':'fno','objectives.cagr':85,'objectives.drawdown':12}),
        dict(id='variation_or',persona='variation',prompt='Buy RSI under 22 OR a new 52 week high.',expect={'join':'any'},conditions=[{'kind':'rsi_below','value':22},{'kind':'breakout','value':252}]),
        dict(id='variation_consistency',persona='variation',prompt='Achieve 24% return every year with Rs 300000.',expect={'capital':300000,'objectives.cagr':24},requires_clarification=True),
        dict(id='variation_private_capital',persona='variation',prompt='Change capital to Rs 750000.',previous={'conditions':[{'id':'v','kind':'volume','value':1.5}]},expect={'capital':750000},conditions=[{'kind':'volume','value':1.5}]),
        dict(id='variation_no_return_claim',persona='variation',prompt='Build a momentum strategy with 25% annualized returns.',expect={'objectives.cagr':25}),
        dict(id='variation_negation',persona='variation',prompt='Buy RSI below 30 except during earnings weeks.',requires_clarification=True),
    ])
    rows=[]
    for case in cases:
        previous=default_spec() if case.get('previous') else None
        if previous:previous.update(case['previous'])
        r=interpret(case['prompt'],previous);failures=score_case(case,r)
        if r.get('profitability_established') or r.get('evidence'):failures.append({'check':'no_performance_without_research'})
        rows.append(dict(id=case['id'],passed=not failures,findings=failures,response=r))
    result=dict(created=datetime.now(timezone.utc).isoformat(),scope='Local intent adapter only; not general language or financial validation',cases=len(rows),passed=sum(r['passed'] for r in rows),rows=rows)
    folder=Path('reports/research-orchestrator');folder.mkdir(parents=True,exist_ok=True);path=folder/('intent-integration-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.json');path.write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(dict(cases=result['cases'],passed=result['passed'],failed=[r['id'] for r in rows if not r['passed']],report=str(path)),indent=2))
    return result


if __name__=='__main__':run()
