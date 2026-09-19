"""Reproducible normal, ambitious and expert CLI demonstrations."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

from .engine import run, readout
from .intent import interpret
from .inventory import WORKSPACE


def demonstrations():
    base = json.loads((Path(__file__).parent / 'examples/pilot-strategies.json').read_text())
    ordinary = deepcopy(base)
    ordinary.update(id='normal-50pct-objective', question='Build me a portfolio targeting 50% annual returns.')
    ordinary['account']['capital'] = 1_000_000
    ordinary['objectives']['cagr'] = 50
    ambitious = deepcopy(ordinary)
    ambitious.update(id='ambitious-100pct-explicit', question='Can any of these six declared cash-equity candidates achieve 100% CAGR and less than 15% drawdown on these three stocks?')
    ambitious['objectives']['cagr'] = 100
    fno = deepcopy(ambitious)
    fno.update(id='fno-data-requirement', universe='fno', question='Build me a strategy using Nifty F&O stocks with 100% CAGR and less than 15% drawdown.')
    serious = deepcopy(base)
    serious.update(id='serious-nested-rules', question='On these three stocks, buy when RSI 14 is below 30 OR SMA20 crosses above SMA50, AND price is above SMA200 AND volume is at least the prior 20-session average. Start with a 20-session hold, 7% stop and 15% target; compare adding a 5% trailing stop, and separately extending the hold to 30 sessions, keeping the other exits.')
    original = deepcopy(base['strategies'][-1]);original['id'] = 'original'
    trailing = deepcopy(original);trailing['id'] = 'trailing_5pct';trailing['exit']['trailing'] = 5
    longer = deepcopy(original);longer['id'] = 'hold_30';longer['exit']['hold'] = 30
    serious['strategies'] = [original, trailing, longer]
    return [ordinary, ambitious, fno, serious]


def run_demo(progress=print):
    rows = []
    folder = WORKSPACE / 'reports/research-orchestrator/persona-inputs'
    folder.mkdir(parents=True, exist_ok=True)
    for request in demonstrations():
        progress('\nUSER: ' + request['question'])
        progress('Declared test scope: TITAN, ICICIBANK, MARUTI; 2020-2022 test years. These examples do not claim a whole-market search.')
        path = folder / (request['id'] + '.json')
        path.write_text(json.dumps(request, indent=2), encoding='utf-8')
        try:
            result = run(request, progress=progress)
            progress(readout(result))
            rows.append(dict(id=request['id'], status='research_only', seconds=result['seconds'], cache_hit=result['cache_hit'],
                request=str(path), report=result['report'], matches=result['evidence']['post_hoc_test_matches']))
        except ValueError as error:
            progress('Needs resolution: ' + str(error))
            rows.append(dict(id=request['id'], status='blocked', request=str(path), reason=str(error)))
    consistency = interpret('I have Rs 10 lakh. Find a strategy that can achieve 30% return consistently')
    progress('\nUSER: I have Rs 10 lakh. Find a strategy that can achieve 30% return consistently.')
    for reason in consistency['blockers']:
        progress('Needs resolution: ' + reason)
    rows.append(dict(id='consistency-clarification', status='blocked' if not consistency['runnable'] else 'intent_only',
                     reasons=consistency['blockers']))
    path = WORKSPACE / 'reports/research-orchestrator' / ('persona-research-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.json')
    path.write_text(json.dumps(rows, indent=2), encoding='utf-8')
    progress('\nScenario evidence: ' + str(path))
    return rows
