"""Read-only source fingerprints for additive engine integration."""
import argparse
import ast
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path

WORKSPACE=Path(__file__).resolve().parents[1]
TERMINAL=Path(r'C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine')
SOURCES=[
    ('chart_detector',WORKSPACE/'market_scanner/detectors.py','Geometric chart-pattern detection'),
    ('chart_replay',WORKSPACE/'market_scanner/historical.py','Causal historical detector replay'),
    ('portfolio_accounting',WORKSPACE/'market_scanner/study_engine.py','Chronological shared-capital execution and accounting'),
    ('stock_features',TERMINAL/'stock_miner/indicators.py','Stock Miner daily technical features'),
    ('stock_rule_miner',TERMINAL/'stock_miner/miner.py','Per-stock weekly tree-rule learning'),
    ('indicator_library',TERMINAL/'ndp/indicator_library.py','NDP parameterized technical signal library'),
    ('stock_dna',TERMINAL/'ndp/dna.py','Per-stock family precomputation and walk-forward statistics'),
    ('falcon_features',TERMINAL/'universe_engine/engine/falcon_features.py','Falcon market-state feature generation'),
    ('falcon_miner',TERMINAL/'universe_engine/engine/falcon_miner.py','Falcon candidate rule extraction'),
    ('falcon_walkforward',TERMINAL/'universe_engine/engine/walkforward.py','Falcon temporal strategy validation'),
    ('falcon_portfolio',TERMINAL/'universe_engine/engine/falcon_portfolio.py','Falcon portfolio construction'),
    ('ndp_regime',TERMINAL/'ndp/state_engine.py','NDP state research'),
]


def inventory():
    rows=[]
    for identity,path,role in SOURCES:
        row=dict(id=identity,path=str(path),role=role,status='missing')
        if path.is_file():
            raw=path.read_bytes();row.update(sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),status='discovered_not_validated')
            try:
                tree=ast.parse(raw.decode('utf-8-sig'));row['entry_points']=[n.name for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef))]
            except (SyntaxError,UnicodeError) as e:row.update(status='source_parse_failed',reason=str(e)[:150])
        rows.append(row)
    return dict(created=datetime.now(timezone.utc).isoformat(),engines=rows,
        note='Discovery and fingerprints only. No module executed, no source overwritten, and no claim of API integration or financial validation.')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',default=str(WORKSPACE/'reports/research-orchestrator'));args=parser.parse_args()
    result=inventory();folder=Path(args.output);folder.mkdir(parents=True,exist_ok=True)
    path=folder/('engine-inventory-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.json');path.write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(dict(discovered=sum(e['status']=='discovered_not_validated' for e in result['engines']),total=len(result['engines']),report=str(path)),indent=2))


if __name__=='__main__':main()
