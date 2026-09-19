"""CLI: inspect user intent before allowing a research engine to execute it."""
import argparse
import json
from pathlib import Path
from .intent import interpret


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
    ask=sub.add_parser('ask');ask.add_argument('question');ask.add_argument('--previous',help='JSON file containing the previous strategy spec');ask.add_argument('--json',action='store_true')
    sub.add_parser('check');sub.add_parser('inventory');sub.add_parser('probe')
    sub.add_parser('capabilities')
    sub.add_parser('demo',help='Run ordinary, ambitious and expert examples using explicit research contracts')
    research=sub.add_parser('research',help='Execute an explicit strategy batch with walk-forward evidence')
    research.add_argument('--spec',required=True,help='Research contract JSON file')
    research.add_argument('--owner',default='local-pilot',help='Local evidence namespace (not multi-user authentication)')
    research.add_argument('--refresh',action='store_true',help='Recompute and preserve the prior evidence file')
    research.add_argument('--data',help='Replay a recorded inputs/<hash>.json.gz file instead of reading current data')
    research.add_argument('--json',action='store_true',help='Print compact machine-readable result')
    args=parser.parse_args()
    if args.command=='demo':
        from .personas import run_demo
        run_demo();return 0
    if args.command=='capabilities':
        from .contracts import capabilities
        print(json.dumps(capabilities(),indent=2));return 0
    if args.command=='research':
        import sys
        from .engine import run,readout
        try:
            request=json.loads(Path(args.spec).read_text(encoding='utf-8-sig'))
            from .adapters import RecordedSnapshotProvider
            result=run(request,owner=args.owner,refresh=args.refresh,
                       provider=RecordedSnapshotProvider(args.data) if args.data else None,
                       progress=lambda m:print(m,file=sys.stderr,flush=True))
        except (ValueError,TypeError,KeyError,OSError) as error:
            print(json.dumps(dict(status='blocked',reason=str(error),research_completed=False,promotion_allowed=False)))
            return 2
        if args.json:
            e=result['evidence'];print(json.dumps(dict(status='research_only',report=result['report'],seconds=result['seconds'],
                cache_hit=result['cache_hit'],actual_candidates=e['actual_candidates'],backtest_accounts=e['backtest_accounts'],
                prefix_checks=e['prefix_checks'],post_hoc_matches=e['post_hoc_test_matches'],
                walk_forward_numeric_criteria_met=e['walk_forward']['numeric_criteria_met_all_folds'],
                verdict=e['verdict'],promotion_allowed=False),indent=2))
        else:print(readout(result))
        return 0
    if args.command=='check':
        from .check_integration import run
        result=run();return 0 if result['passed']==result['cases'] else 1
    if args.command=='inventory':
        from .inventory import inventory
        print(json.dumps(inventory(),indent=2));return 0
    if args.command=='probe':
        from .numerical_probe import run
        run();return 0
    previous=json.loads(Path(args.previous).read_text(encoding='utf-8')) if args.previous else None
    result=interpret(args.question,previous)
    if args.json:print(json.dumps(result,indent=2,ensure_ascii=False));return 0
    print('KANIDA local intent adapter - '+result['action']);print(result['message'])
    spec=result['spec'];print('Capital: INR '+str(spec['capital'])+' | Universe: '+spec['universe'])
    for b in spec['conditions']:print('  '+b['kind']+' '+str(b['value']))
    for note in result.get('notes',[]):print('Assumption: '+note)
    for question in result['blockers']:print('Needs resolution: '+question)
    print('No backtest or profitability claim has been produced by this intent-only command.')
    return 0


if __name__=='__main__':raise SystemExit(main())
