"""The screener's closed vocabulary: every metric, side, state word, window and strike range it offers.

ONE CATALOGUE. The visual builder draws its chips from this file (served by `/api/screener/vocabulary`), the
natural-language parser maps words onto it, the evaluator refuses anything outside it, and the "Reads as"
sentence is written from it. A word that is not here cannot reach a scanner definition by any route.

NO NUMBERS FOR THE USER. A state is a word; what the word MEANS is the tab's existing rule over the 15-minute
readings (see `states.py`), not a threshold the user types. The only numbers a user ever sees are the ones
they choose to see: the delta bands in "More options" (named, with the band printed beside the name) and the
strike offsets of a custom range.
"""
from __future__ import annotations

READING_MINUTES=15

#: Canonical state ids -> the universal words. The words are the owner's, verbatim.
STATE_WORDS={
 'up':'Increasing','down':'Decreasing',
 'up_cont':'Increasing continuously','down_cont':'Decreasing continuously',
 'up_rapid':'Expanding rapidly','down_rapid':'Contracting rapidly',
 'stable':'Stable',
 'rev_up':'Reversing higher','rev_down':'Reversing lower',
 'unusual':'Unusually active','quiet':'Quiet',
 'broadening':'Broadening across strikes','concentrating':'Concentrating at fewer strikes',
 'shift_up':'Shifting higher','shift_down':'Shifting lower',
 # build-up: the four FLOW_LABELS behaviours that name a participant (summary.ts DIRECTIONAL)
 'writing':'Writing','buying':'Buying','short_covering':'Short covering','buyers_exiting':'Buyers exiting',
}
#: The arrow beside each word. The word is always printed; the arrow never stands alone.
STATE_ARROWS={
 'up':'↑','down':'↓','up_cont':'↑↑','down_cont':'↓↓','up_rapid':'⇈','down_rapid':'⇊','stable':'→',
 'rev_up':'↗','rev_down':'↘','unusual':'✦','quiet':'·','broadening':'⇔','concentrating':'⇥',
 'shift_up':'⤴','shift_down':'⤵','writing':'✎','buying':'＋','short_covering':'↺','buyers_exiting':'↤',
}
#: Tone per state, in the tab's own meaning (signal.ts STATE_TONE): green = activity expanding, red =
#: contracting, amber = a caveat, grey = unchanged. None of them is a market direction or a verdict.
STATE_TONE={
 'up':'up','up_cont':'up','up_rapid':'up','rev_up':'warn','down':'down','down_cont':'down','down_rapid':'down',
 'rev_down':'warn','stable':'flat','unusual':'up','quiet':'flat','broadening':'up','concentrating':'warn',
 'shift_up':'warn','shift_down':'warn','writing':'up','buying':'up','short_covering':'down','buyers_exiting':'down',
}

DIRECTIONAL=['up','down','up_cont','down_cont','up_rapid','down_rapid','stable','rev_up','rev_down']
TREND_ONLY=['up','down','stable']
FLOW_STATES=['writing','buying','short_covering','buyers_exiting']

#: family = which of the tab's direction rules reads this metric (states.py):
#:   grid_oi    - logic.ts gridDirection on ΔOI (OI against the previous session's close)
#:   grid_price - logic.ts gridPriceDirection on the contract's own premium
#:   session    - logic.ts sessionDirection (PCR, max pain, IV, spot, futures OI)
#: grain = 'contract' (one option contract), 'book' (one underlying + expiry) or 'side' (one side of one book).
METRICS={
 'premium':dict(label='Premium',group='Price',grain='contract',family='grid_price',sided=True,
  states=DIRECTIONAL,default_state='up_cont',field='metrics.last_price',
  words={'up':'Rising','down':'Falling','up_cont':'Rising continuously','down_cont':'Falling continuously'},
  noun='premium',help='The option\'s own last traded price at each 15-min reading.'),
 'oi':dict(label='OI',group='Positions',grain='contract',family='grid_oi',sided=True,
  states=DIRECTIONAL,default_state='up_cont',field='metrics.oi_change_day',
  words={},noun='OI',help='Open interest. Read as its change against the previous session\'s close, like the ΔOI tiles.'),
 'flow':dict(label='Build-up',group='Positions',grain='contract',family='flow',sided=True,
  states=FLOW_STATES,default_state='writing',field='metrics.last_price + metrics.oi_change_day',
  words={},noun='build-up',help='Premium and OI read together, in the tab\'s own words (call writing, put buying…).'),
 'iv':dict(label='IV',group='Volatility',grain='contract',family='session',sided=True,computed=True,
  states=DIRECTIONAL,default_state='up',field='computed: implied_vol.solve',
  words={'up':'Expanding','down':'Cooling','up_cont':'Expanding continuously','down_cont':'Cooling continuously',
   'up_rapid':'Expanding rapidly','down_rapid':'Cooling rapidly'},
  noun='IV',help='Implied volatility, COMPUTED from the option\'s own price (Black-Scholes-Merton). Not an exchange figure.'),
 'delta':dict(label='Delta',group='Volatility',grain='contract',family='session',sided=True,computed=True,
  states=TREND_ONLY,default_state='up',field='computed: BSM delta at the solved IV',
  words={},noun='delta',help='COMPUTED from the solved IV. Increasing means moving toward ±1 in size (deeper in the money).'),
 'gamma':dict(label='Gamma',group='Volatility',grain='contract',family='session',sided=True,computed=True,
  states=TREND_ONLY,default_state='up',field='computed: BSM gamma at the solved IV',
  words={},noun='gamma',help='COMPUTED from the solved IV. Rises mechanically as expiry nears for near-ATM strikes.'),
 'volume':dict(label='Volume',group='Activity',grain='contract',family='activity',sided=True,
  states=['unusual','quiet'],default_state='unusual',field='metrics.unusual / metrics.vol_tod_ratio',
  words={},noun='volume',help='Unusually active uses the stored rule (volume ≥ 2× its own time-of-day median, or the volume/OI spike).'),
 'spread':dict(label='OI across strikes',group='Activity',grain='side',family='spread',sided=True,sides=['CE','PE','either','both'],
  states=['broadening','concentrating','shift_up','shift_down'],default_state='broadening',
  field='per-strike metrics.oi_change_day within the strike range',
  words={},noun='OI activity',help='Which strikes in the range are adding OI, and whether that set is widening, narrowing or moving.'),
 'pcr':dict(label='PCR',group='Book',grain='book',family='session',sided=False,
  states=DIRECTIONAL,default_state='up_cont',field='metrics.pcr_oi (underlying scope)',
  words={'up':'Rising','down':'Falling','up_cont':'Rising continuously','down_cont':'Falling continuously'},
  noun='PCR',help='Put open interest divided by call open interest across the expiry.'),
 'maxpain':dict(label='Max pain',group='Book',grain='book',family='session',sided=False,
  states=['up','down','up_cont','down_cont','stable','rev_up','rev_down'],default_state='up',
  field='metrics.max_pain_strike (underlying scope)',
  words={'up':'Shifting higher','down':'Shifting lower','up_cont':'Shifting higher at every reading',
   'down_cont':'Shifting lower at every reading'},
  noun='max pain',help='The strike where option buyers\' total payout at expiry would be smallest.'),
 'underlying':dict(label='Underlying',group='Price',grain='book',family='session',sided=False,
  states=DIRECTIONAL,default_state='up',field='metrics.spot (underlying scope)',
  words={'up':'Moving higher','down':'Moving lower','up_cont':'Rising continuously','down_cont':'Falling continuously',
   'up_rapid':'Rising rapidly','down_rapid':'Falling rapidly'},
  noun='the underlying',help='The spot price captured at the same reading.'),
 'futures_oi':dict(label='Futures OI',group='Positions',grain='book',family='session',sided=False,
  states=['up','down','up_cont','down_cont','stable'],default_state='up',field='metrics.oi (FUT contract, front expiry)',
  words={'up':'Building','down':'Unwinding','up_cont':'Building continuously','down_cont':'Unwinding continuously'},
  noun='futures OI',help='Open interest of the front futures contract.'),
}
CONTRACT_METRICS=[k for k,m in METRICS.items() if m['grain']=='contract']
COMPUTED_METRICS=[k for k,m in METRICS.items() if m.get('computed')]

SIDES={'CE':'Calls','PE':'Puts','either':'Either side','both':'Both sides'}
DEFAULT_SIDES=['CE','PE','either']

#: Windows, all multiples of the 15-min reading. `steps` = how many 15-min intervals the state is read over.
WINDOWS=[
 {'kind':'minutes','value':15,'label':'Last 15 min'},
 {'kind':'minutes','value':30,'label':'Last 30 min'},
 {'kind':'minutes','value':45,'label':'Last 45 min'},
 {'kind':'minutes','value':60,'label':'Last 60 min'},
 {'kind':'open','label':'Since market open'},
 {'kind':'readings','value':2,'label':'Last 2 readings'},
 {'kind':'readings','value':3,'label':'Last 3 readings'},
 {'kind':'readings','value':4,'label':'Last 4 readings'},
 {'kind':'custom','label':'Custom window'},
]
MAX_STEPS=24

UNIVERSES={'all':'Indices + F&O stocks','indices':'Indices','stocks':'F&O stocks','symbols':'Chosen symbols'}
#: Every index with options in the capture scope. (derivatives.INDEX_UNDERLYINGS lists only three of these.)
INDEX_UNDERLYINGS=('NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY','NIFTYNXT50')
EXPIRIES={'nearest':'Nearest expiry','next':'Next expiry','both':'Both captured expiries'}

#: Strike ranges. Offsets are listed strikes (rungs) from the ATM strike - the grid's own ATM rule, the listed
#: strike nearest spot. The widest range the screener reads is ±MAX_RUNGS.
MAX_RUNGS=10
STRIKE_PRESETS=[
 {'kind':'atm','below':5,'above':5,'label':'ATM ±5'},
 {'kind':'atm','below':2,'above':2,'label':'ATM ±2'},
 {'kind':'atm','below':3,'above':3,'label':'ATM ±3'},
 {'kind':'atm','below':0,'above':0,'label':'ATM only'},
 {'kind':'atm','below':10,'above':10,'label':'ATM ±10'},
 {'kind':'otm','depth':5,'label':'OTM, 5 strikes'},
 {'kind':'itm','depth':5,'label':'ITM, 5 strikes'},
]
#: Delta bands: a FILTER the user picks by name, with the band printed beside it. They are conventions a trader
#: already knows, not an interpretation KANIDA applies behind a word. |delta|, from the COMPUTED delta.
DELTA_BANDS={
 'atm_like':{'label':'ATM-like','lo':0.40,'hi':0.60},
 'otm':{'label':'OTM','lo':0.15,'hi':0.40},
 'deep_otm':{'label':'Deep OTM','lo':0.0,'hi':0.15},
 'itm':{'label':'ITM','lo':0.60,'hi':1.0},
}
MAX_CONDITIONS=6

LIFECYCLE={'new':'New match','still':'Still matching','strengthening':'Strengthening','weakening':'Weakening',
 'ended':'Condition ended'}


def state_word(metric,state):
 """The word a metric prints for a state: the metric's own word where it has one, else the universal word."""
 m=METRICS.get(metric) or {}
 return (m.get('words') or {}).get(state) or STATE_WORDS.get(state,state)


def sides_for(metric):
 m=METRICS.get(metric) or {}
 if not m.get('sided'):return []
 return list(m.get('sides') or DEFAULT_SIDES)


def window_steps(window,reading_count=None):
 """How many 15-min intervals a window spans. `open` depends on the readings so far; `custom` is handled by
 the evaluator (it has fixed clock bounds)."""
 kind=(window or {}).get('kind')
 if kind=='minutes':return max(1,int(window.get('value',15))//READING_MINUTES)
 if kind=='readings':return max(1,int(window.get('value',1)))
 if kind=='open':return None
 return None


def window_label(window):
 kind=(window or {}).get('kind')
 if kind=='minutes':return f"last {int(window['value'])} min"
 if kind=='readings':return f"last {int(window['value'])} readings"
 if kind=='open':return 'since market open'
 if kind=='custom':return f"{window.get('from')}–{window.get('to')}"
 return ''


def catalogue():
 """The whole vocabulary, as the builder draws it."""
 return {
  'metrics':[{'key':k,'label':m['label'],'group':m['group'],'grain':m['grain'],'computed':bool(m.get('computed')),
   'sides':sides_for(k),'states':[{'key':s,'label':state_word(k,s),'arrow':STATE_ARROWS.get(s,''),
    'tone':STATE_TONE.get(s,'flat')} for s in m['states']],
   'default_state':m['default_state'],'help':m['help'],'field':m['field']} for k,m in METRICS.items()],
  'sides':[{'key':k,'label':v} for k,v in SIDES.items()],
  'windows':WINDOWS,'universes':[{'key':k,'label':v} for k,v in UNIVERSES.items()],
  'expiries':[{'key':k,'label':v} for k,v in EXPIRIES.items()],
  'strike_presets':STRIKE_PRESETS,'max_rungs':MAX_RUNGS,
  'delta_bands':[{'key':k,**v} for k,v in DELTA_BANDS.items()],
  'lifecycle':[{'key':k,'label':v} for k,v in LIFECYCLE.items()],
  'index_underlyings':list(INDEX_UNDERLYINGS),'reading_minutes':READING_MINUTES,'max_conditions':MAX_CONDITIONS,
 }
