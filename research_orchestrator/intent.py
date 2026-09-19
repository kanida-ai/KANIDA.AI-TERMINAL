"""Conservative local intent adapter for CLI integration tests.

This adapter covers declared phrases; it is not a general language model. The
same compiled contract can later accept schema-validated hosted-model output.
Unknown execution requirements remain blocked, never silently promoted.
"""
from copy import deepcopy
import re
from market_scanner import strategy_core as core

VERSION='intent-contract-0.1'


def default_spec(end='2026-07-29'):
    return core.validate(dict(name='Momentum hypothesis',start=f'{int(end[:4])-10}{end[4:]}',end=end,
        universe='nifty50',conditions=[dict(id='momentum',kind='momentum',value=10)]))


def interpret(text,previous=None,context=None):
    if not isinstance(text,str) or not 1<=len(text.strip())<=4000:raise ValueError('Describe your request in 1–4,000 characters')
    context=context or {};spec=deepcopy(previous) if previous else default_spec(context.get('last','2026-07-29'))
    q=text.lower().replace('₹','rs ').replace(',','').replace('–','-').replace('—','-');blockers=[];notes=[];requested={};recognized=False;parsed=[]
    def block(message):
        if message not in blockers:blockers.append(message)
    if re.search(r'\b(?:analy[sz]e|review) my (?:portfolio|holdings)\b|portfolio risk',q):
        return dict(action='portfolio',spec=spec,blockers=[],notes=[],runnable=False,message='Supply or connect actual holdings to measure their risk and concentration.',interpretation=VERSION)
    if re.search(r'^\s*(?:deploy|activate|invest|allocate)\b',q) and re.search(r'\b(?:version|strategy|balanced|conservative|aggressive)\b',q):
        return dict(action='deploy',spec=spec,blockers=[],notes=[],runnable=False,message='Resolve the exact researched strategy version and review its paper deployment. No order has been submitted.',interpretation=VERSION)
    if re.search(r'guarantee|risk[- ]free|no risk|zero (?:risk|losing|loss)|no losses',q):
        block('Historical evidence cannot guarantee future returns or zero losses. Choose a measurable historical objective and an acceptable loss limit.')
    target=re.search(r'(\d+(?:\.\d+)?)\s*%\s*(?:cagr|annual(?:ized)?(?: returns?)?|returns?(?: consistently| every year)?)',q)
    if not target:target=re.search(r'(?:target(?:ing)?|achieve|cagr(?: of)?)\s*(\d+(?:\.\d+)?)\s*%',q)
    if target:
        requested['return_target_pct']=float(target[1]);spec.setdefault('objectives',{})['cagr']=float(target[1]);recognized=True
        requested['return_target_metric']='annualized' if re.search(r'cagr|annual',q) else 'unresolved'
        if requested['return_target_metric']=='unresolved':block('Define the return period: annualized CAGR, each calendar year, or another holding period.')
    if re.search(r'consistent|every year|each year',q):
        block('Define consistency: every calendar year, a percentage of rolling 12-month periods, or annualized return over the whole period. These are different tests.')
        requested['consistency']=True
    m=re.search(r'(\d+(?:\.\d+)?)\s*%\s*(?:maximum )?drawdown',q) or re.search(r'drawdown[^\d.!?]{0,25}(\d+(?:\.\d+)?)\s*%',q)
    if m:spec['objectives']['drawdown']=float(m[1]);recognized=True
    m=re.search(r'(?:rs\.?\s*|capital(?: of| to)?\s*|i have\s*)(\d+(?:\.\d+)?)\s*(lakh|lac|crore|k|million)?',q)
    if m:spec['capital']=float(m[1])*{None:1,'lakh':1e5,'lac':1e5,'crore':1e7,'k':1e3,'million':1e6}[m[2]];recognized=True
    if re.search(r'\bf\s*(?:&|and)\s*o\b',q):spec['universe']='fno';recognized=True
    else:
        indices=re.findall(r'nifty\s*(50|100|200|500|1000)\b',q)
        universe=next((i for i in indices if i!='50'),indices[0] if indices else None)
        if universe=='1000':block('Nifty 1000 membership is not available in the supplied data. Select a supported universe explicitly.')
        elif universe:spec['universe']='nifty'+universe;recognized=True
    if re.search(r'\bfutures?\b|\boptions?\b|\bshort(?:ing| sell)?\b|intraday|\b(?:1h|4h|1w)\b',q):
        block('This integration accepts daily long cash-equity research. Derivatives need actual contracts, lots, margin and expiry rules; unsupported execution cannot be substituted.')
    for words,kind in [(['profitable companies','profitable nifty','quality','valuation','earnings'],'quality'),(['revenue','fundamental'],'revenue_growth'),(['market cap','large-cap','blue-chip'],'market_cap'),(['sector strength','sector momentum','outperforming their sector'],'sector_strength'),(['institutional'],'institutional')]:
        if any(w in q for w in words):block(core.UNAVAILABLE[kind])
    if re.search(r'\bstrong stocks?\b',q):block('Define “strong”: price trend, relative performance or company fundamentals. I will not choose a different meaning silently.')
    if re.search(r'\bsector\b',q) and re.search(r'(?:no more than|at most|max(?:imum)?|cap|limit)',q):
        m=re.search(r'(\d+(?:\.\d+)?)\s*%[^.!?]{0,25}\bsector',q)
        if m:requested['max_sector_pct']=float(m[1])
        block('The current adapter cannot enforce sector allocation limits. The request is blocked until a capable portfolio adapter is selected.')
    m=re.search(r'(?:no more than|at most|max(?:imum)?|up to)\s*(\d+)\s*(?:stocks|positions)',q)
    if m:spec['max_positions']=int(m[1]);spec['allocation_pct']=100/int(m[1]) if int(m[1]) else 0;recognized=True
    m=re.search(r'(?:last|past|over)\s*(\d+)\s*years?',q)
    if m:spec['start']=f'{int(spec["end"][:4])-int(m[1])}{spec["end"][4:]}';recognized=True
    dates=re.findall(r'\b\d{4}-\d{2}-\d{2}\b',q)
    if len(dates)>=2:spec['start'],spec['end']=dates[:2];recognized=True
    def add(kind,value):
        nonlocal recognized
        b=dict(id='condition-'+str(len(parsed)),kind=kind,value=value)
        if not any(x['kind']==kind and x['value']==value for x in parsed):parsed.append(b)
        recognized=True
    crossover=bool(re.search(r'cross(?:es|ing)?\s+(?:above|below)',q))
    if crossover:block('A crossover is a change across two observations, not an above/below state. This local adapter cannot compile it faithfully yet.')
    for m in re.finditer(r'rsi(?:\s*\(?14\)?)?\s*(?:is\s+|falls?\s+|rises?\s+)?(below|under|less than|<|above|over|>)\s*(\d+(?:\.\d+)?)',q):
        if not crossover:add('rsi_below' if m[1] in ('below','under','less than','<') else 'rsi_above',float(m[2]))
    if 'rsi' in q and not crossover and not any(b['kind'].startswith('rsi') for b in parsed):block('Specify the RSI comparison, threshold and period; no threshold was assumed.')
    if re.search(r'52[- ]week high',q):
        m=re.search(r'(\d+(?:\.\d+)?)\s*%\s*(?:below|from)[^.!?]{0,35}52[- ]week high',q) or re.search(r'(?:fall|dip)[^.!?]{0,18}(\d+(?:\.\d+)?)\s*%',q)
        if m:add('dip',float(m[1]))
        if not m or re.search(r'(?:breaks?|breakout|making|makes|new)[^.!?]{0,22}52[- ]week high',q):add('breakout',252)
    elif re.search(r'breakout|breaks? (?:a |the )?high',q):add('breakout',63);notes.append('No high lookback specified; 63 trading days is a proposed default.')
    if re.search(r'volume (?:above|>|greater than)[^.!?]{0,25}(?:20[- ]day|average)',q):add('volume',1)
    elif re.search(r'increasing volume|volume confirmation|high volume',q):add('volume',1.2);notes.append('Volume confirmation proposes 1.2× the prior 20-day average; inspect this assumption.')
    if 'momentum' in q and not any(b['kind']=='momentum' for b in parsed):add('momentum',10);notes.append('Momentum proposes a 63-day return of at least 10%.')
    if re.search(r'(?:stock|price)[^.!?]{0,20}above[^.!?]{0,15}200[- ]day',q):add('above_sma',200)
    if re.search(r'(?:nifty|market)[^.!?]{0,25}above[^.!?]{0,15}200[- ]day|bull markets?',q):add('market_trend',200)
    for words,key in [('cup and handle','cup_handle'),('cup & handle','cup_handle'),('falling wedge','falling_wedge'),('symmetrical triangle','symmetrical_triangle')]:
        if words in q:add('pattern',key)
    if re.search(r'\bor\b',q):
        spec['join']='any'
        if re.search(r'\band\b',q) and len(parsed)>2:block('Mixed AND/OR conditions require explicit grouping; a flat rule would change the meaning.')
    add_edit=bool(previous and re.search(r'^\s*(?:add|include|also require)',q));remove_edit=bool(previous and re.search(r'^\s*(?:remove|drop|delete)',q))
    if remove_edit:block('Removal requires an explicit existing condition identifier in this local adapter.')
    elif parsed:
        if add_edit:
            old=deepcopy(spec['conditions'])
            for b in parsed:
                if not any(x['kind']==b['kind'] and x['value']==b['value'] for x in old):b['id']='added-'+str(len(old));old.append(b)
            spec['conditions']=old
        else:spec['conditions']=parsed
    if re.search(r'\bnot\b|\bexcept\b|\bunless\b',q):block('This request includes a negation or exception that needs explicit structured rules before execution.')
    for name,pattern in [('hold',r'(?:hold(?: for)?|exit after)\s*(\d+)\s*(?:trading )?days'),('target',r'(?:profit target|take profit)\s*\+?(\d+(?:\.\d+)?)\s*%'),('stop',r'stop(?:[- ]loss)?\s*-?(\d+(?:\.\d+)?)\s*%'),('trailing',r'trailing stop\s*(\d+(?:\.\d+)?)\s*%')]:
        m=re.search(pattern,q)
        if m:spec['exit'][name]=float(m[1]);recognized=True
    if re.search(r'weekly rebalance',q):spec['rebalance']='weekly';recognized=True
    if re.search(r'monthly rebalance',q):spec['rebalance']='monthly';recognized=True
    if not previous and not parsed:notes.append('No entry rules were supplied. A 63-day momentum hypothesis is proposed, not inferred as your chosen strategy.')
    if not recognized:block('This request is outside the local phrase adapter. A structured strategy or clarification is required; no research was started.')
    if context.get('first') and spec['start']<context['first']:block('Requested dates precede the available history.')
    if context.get('last') and spec['end']>context['last']:block('Requested dates exceed the available history.')
    try:spec=core.validate(spec)
    except (ValueError,TypeError,KeyError) as e:block(str(e))
    return dict(action='research',spec=spec,blockers=blockers,notes=notes,requested_constraints=requested,runnable=not blockers,
        interpretation=VERSION,needs_model_for_general_language=True,
        message='The declared request can be submitted for numerical research; no return has been calculated yet.' if not blockers else 'Clarification or another engine capability is required before research.',
        evidence=None,profitability_established=False)
