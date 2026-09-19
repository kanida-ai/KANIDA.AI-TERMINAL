"""Strategy registry, Discover catalog and results (docs/FALCON_DISCOVER_SPEC.md §3, §5, §6; contract src/strategies/types.ts).

Point-in-time: rows come only from the stored scan (`/api/matches`) and its stored evidence; nothing is re-run here.
"95% low" is `history[side].reference.expectancy_ci95[0]`: pattern hold-period history after costs, never a tested exit
rule unless the cell status is `tested`. Ranking is 95% low (nulls last), then n, then symbol. Win rate is context only.
"""
from __future__ import annotations
import math,re
from datetime import date
from sqlalchemy import select,func
from .db import strategy_blocks,strategies,audit,record,now,row
from .evidence import data_age,day_text,market_today,max_data_age
from .errors import PilotError
from . import cards
from . import detections as live_detections
from .research_store import TIMEFRAMES as RESEARCH_TIMEFRAMES,strategy_key as research_key

# Hardcoded from market_scanner detectors.py PATTERNS and backtest_worker.py SIDES so the seed never needs research up.
PATTERNS=[
 ('cup_handle','Cup & Handle','Rounded cup, aligned rims, a shallow handle, and a prior advance.',('long',)),
 ('horizontal_breakout','Horizontal Breakout','Repeated resistance tests followed by a fresh close above resistance with volume expansion.',('long',)),
 ('flag_pole','Flag & Pole','An impulsive pole followed by a compact, counter-trend parallel consolidation.',('long','short')),
 ('symmetrical_triangle','Symmetrical Triangle','Falling resistance and rising support converge, with repeated touches on both sides.',('long','short')),
 ('falling_wedge','Falling Wedge','Both boundaries fall; resistance falls faster, compressing the range.',('long',)),
 ('rising_wedge','Rising Wedge','Both boundaries rise; support rises faster, compressing the range.',('short',)),
 ('channel','Channel','Two parallel boundaries contain price, with repeated alternating reactions.',('long','short')),
 ('descending_triangle','Descending Triangle','Falling resistance converges toward a horizontal support level.',('short',)),
 ('head_shoulders','Head & Shoulders','A higher head between two balanced shoulders, two neckline pivots, and a prior advance.',('short',)),
 ('inverse_head_shoulders','Inverse Head & Shoulders','A lower head between two balanced shoulders, two neckline pivots, and a prior decline.',('long',)),
]
NAMES={p[0]:p[1] for p in PATTERNS};DESCRIPTIONS={p[0]:p[2] for p in PATTERNS};SIDES={p[0]:p[3] for p in PATTERNS}
TIMEFRAMES=('1D','1H','4H','1W')  # seed/display order: 1D first
BLOCK_KINDS=('chart','quant','results','options','candlestick','events','price_action','harmonic')
SOURCE_TYPES=('stored_pattern','research_pattern')  # later: quant_rule, scanner_endpoint (no schema change needed)
AUDIENCES=('trader','investor','both')
CELL_STATUSES=('tested','small_test_sample','no_validated_rule','no_occurrences')
DIRECTION_SIDES={'bullish':('long',),'bearish':('short',),'neutral':('long','short')}
SAMPLE_MODERATE_MIN,SAMPLE_LARGER_MIN=10,30  # src/decision.ts
DEFAULT_MIN_TRADES=10;DEFAULT_SLOTS={'falling_wedge-1D-long':'A','channel-1D-long':'B'}
SUMMARY_FIELDS=('found','best_low_pct','positive_low_count','tested_count','pattern_name')
REGISTRY_KIND='strategy_registry'
BLOCK_KEY=re.compile(r'^[a-z0-9][a-z0-9_-]{1,39}$');STRATEGY_KEY=re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{2,63}$')
SEED_BLOCKS=[
 ('chart','Chart Strategies','Stored chart-pattern scans, ranked by 95% low after costs.','chart',10,True),
 ('quant','Quant Strategies','Rule-based quant strategies. Not connected yet.','quant',20,False),
 ('results','Results & Events','Earnings results and corporate events. Not connected yet.','results',30,False),
 ('options','Options Strategies','Options strategies. Not connected yet.','options',40,False),
]

# --- researched pattern catalogue (docs/pattern_research/IMPLEMENTED_CATALOGUE.json) -------------------------
# 107 catalogue entries / 262 direction-variant combinations x 4 timeframes = 1,048 strategies. The four blocks
# below are seeded from the catalogue itself, so names and descriptions are never invented here. Only
# "Chart patterns" is enabled by default; the owner enables the rest from /admin/strategies.
RESEARCH_BLOCKS=[
 ('chart_patterns','Chart patterns','Chart formations: wedges, triangles, channels, head & shoulders, cups, rounding and island patterns.','chart',110,'chart',True),
 ('candlestick','Candlestick patterns','Candlestick patterns, on their own and in trend context.','candlestick',120,'candlestick',False),
 ('price_action','Price action','Bar-level price action: tweezers, inside bars, pin bars, narrow ranges, gaps and trend runs.','price_action',130,'price_action',False),
 ('harmonics','Harmonics','Harmonic templates: AB=CD, Gartley, Bat, Butterfly, Crab, Shark and 5-0.','harmonic',140,'harmonic',False),
]
# The first seed's block descriptions. A block still carrying one of these was never edited by the owner, so the
# seed moves it to the plain wording above; an edited description is left exactly as saved.
OLD_RESEARCH_BLOCK_DESCRIPTIONS={
 'chart_patterns':'Researched chart formations: wedges, triangles, channels, head & shoulders, cups, rounding and island patterns.',
 'candlestick':'Researched candlestick recognitions, evaluated on their own and in trend context.',
 'price_action':'Researched bar-level price action: tweezers, inside bars, pin bars, narrow ranges, gaps and trend runs.',
 'harmonics':'Researched harmonic templates: AB=CD, Gartley, Bat, Butterfly, Crab, Shark and 5-0.',
}
FAMILY_BLOCK={family:key for key,_,_,_,_,family,_ in RESEARCH_BLOCKS}
FAMILY_TAG={'chart':'Chart patterns','candlestick':'Candlestick patterns','price_action':'Price action','harmonic':'Harmonics'}
# EVIDENCE_SERVING_CONTRACT.md §6: fewer than 20 walk-forward trades is a "Limited historical sample".
RESEARCH_MIN_TRADES=cards.WALKFORWARD_MIN
RESEARCH_SUMMARY_FIELDS=('live_detection','detections_today','detections_week','detections_live','detections_symbols',
 'detections_as_of','detections_scanned_at','detections_last_detected','history_today','history_week','researched_stocks','evidence_ready',
 'tier_result','tier_limited','tier_history','evidence_summary',
 'evidence_total','cells','cells_with_evidence','cells_tested','cells_loading','occurrences','best_edge_low_pct',
 'best_edge_symbol','beats_baseline','last_seen','evidence_pending')
# Default A/B slots for the one block enabled at seed: a long and a short chart formation on the daily.
RESEARCH_SLOTS={'ch05-legacy_1.0.1-long-1d':'A','ch06-legacy_1.0.1-short-1d':'B'}
STATE_WORD={'confirmed':'confirmed breakout','setup':'setup'}
# What a stored block says while the scanner runs the researched set. Its 10 detectors are the CH01-CH10
# subset of the 107, so the honest line points at the research blocks rather than reporting an error.
NOT_STUDIED_NOTE=('The research run never studied {symbol} on {timeframe} for {name}, so there is no compatible '
 'historical evidence for this detection. The scanner detects on a wider universe than the research run covers.')
RESEARCH_PATTERN_SET='research'  # market_scanner SCANNER_PATTERN_SET; mirrored, not imported
STORED_SUPERSEDED=('The scanner is running the researched pattern set, so the stored 10-pattern scan has no '
 'current matches.')

def research_variant_label(variant):
 return 'canonical' if variant.startswith('legacy') else variant.replace('_',' ')
def research_name(spec,timeframe):
 """Catalogue name + variant + timeframe, e.g. "Bullish engulfing · context · 4H". The variant is left out when it
 only repeats the name ("V bottom · v bottom") or is the canonical form."""
 variant=research_variant_label(spec['variant'])
 repeats=variant=='canonical' or variant.lower() in spec['name'].lower()
 head=spec['name'] if repeats else f"{spec['name']} · {variant}"
 return f'{head} · {timeframe}'[:100]
def _first_seed_research_name(spec,timeframe):
 """The name the first seed generated. A strategy still carrying it was never renamed by the owner."""
 variant=research_variant_label(spec['variant'])
 head=spec['name'] if variant=='canonical' else f"{spec['name']} · {variant}"
 return f'{head} · {timeframe}'[:100]
def research_definition(spec,limit=500):
 """The frozen catalogue's own definition text, whitespace-normalised and cut on a word boundary. Never rewritten."""
 body=' '.join((spec.get('definition') or '').split())
 if len(body)>limit:body=body[:limit-1].rsplit(' ',1)[0]+'…'
 return body
def research_description(spec,timeframe):
 """From the frozen catalogue definition. Truncated on a word boundary; never rewritten."""
 word=STATE_WORD.get(spec['state'],spec['state'])
 head=f"{spec['side'].title()} on {timeframe} candles at the {word}. "
 return (head+research_definition(spec,500-len(head)))[:500]
def research_tags(spec,timeframe):
 return [FAMILY_TAG.get(spec['family'],spec['family']),'Bullish' if spec['side']=='long' else 'Bearish',timeframe]

def old_default_name(pattern,tf,side):  # v1 seed wording ("Horizontal Breakout breakout · 1D"); only used to migrate untouched names
 word='breakout' if side=='long' else 'breakdown'
 return f'{NAMES[pattern]} {word} · {tf}'+(' · Short' if side=='short' and len(SIDES[pattern])>1 else '')
def default_name(pattern,tf,side):
 word='breakout' if side=='long' else 'breakdown';name=NAMES[pattern]
 head=name if name.lower().endswith(('breakout','breakdown')) else f'{name} {word}'
 return f'{head} · {tf}'+(' · Short' if side=='short' and len(SIDES[pattern])>1 else '')
UNIVERSE_KEY=re.compile(r'^nifty',re.I)
def universe_label(key,label):
 """NSE index universes are shown NSE-style in upper case ("Nifty 500" -> "NIFTY 500"); other labels pass through."""
 label=str(label or key)
 return label.upper() if UNIVERSE_KEY.match(str(key or '')) or UNIVERSE_KEY.match(label) else label
def slug(value):return re.sub(r'-{2,}','-',re.sub(r'[^a-z0-9]+','-',str(value or '').lower())).strip('-')[:60].strip('-')
def default_description(pattern,tf,side):
 way='Long' if side=='long' else 'Short (hypothetical price study; no borrow modelled)'
 return f'{DESCRIPTIONS[pattern]} {way} on {tf} candles. Evidence is stored pattern hold-period history after costs unless a tested exit rule exists.'
def default_tags(tf,side):return ['Chart patterns','Bullish' if side=='long' else 'Bearish',tf]
def default_audience(tf):return 'investor' if tf=='1W' else 'trader'
def sample_label(n):return 'No history' if not n else 'Larger sample' if n>=SAMPLE_LARGER_MIN else 'Moderate sample' if n>=SAMPLE_MODERATE_MIN else 'Small sample'
def num(value,places=4):return round(float(value),places) if isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) else None
def pair(value):
 if isinstance(value,(list,tuple)) and len(value)==2 and all(num(v) is not None for v in value):return [num(value[0]),num(value[1])]
 return None

def block_def(r):return dict(key=r['key'],title=r['title'],description=r['description'],kind=r['kind'],order=r['position'],enabled=bool(r['enabled']))
def strategy_def(r):
 """One shape for both sources. `stored_pattern` carries a scanner pattern id; `research_pattern` carries the
 catalogue identity (pattern_id + variant + side + timeframe + research_run) that resolves an evidence cell."""
 cfg=r['source_config'] or {}
 research=r['source_type']=='research_pattern'
 pattern=cfg.get('pattern_id') if research else cfg.get('pattern')
 return dict(key=r['key'],block_key=r['block_key'],name=r['name'],description=r['description'],tags=list(r['tags'] or []),
  source_type=r['source_type'],pattern=pattern,
  pattern_name=(cfg.get('name') or pattern or '') if research else NAMES.get(cfg.get('pattern'),cfg.get('pattern') or ''),
  timeframe=cfg.get('timeframe'),side=cfg.get('side'),
  pattern_id=cfg.get('pattern_id'),variant=cfg.get('variant'),state=cfg.get('state'),family=cfg.get('family'),
  research_run=cfg.get('research_run'),
  audience=r['audience'],min_trades=r['min_trades'],default_slot=r['default_slot'],order=r['position'],enabled=bool(r['enabled']))

# THREE different things share a research card and are never merged into one number:
#   detected today     - `live_detection` + `detections_*`, counted from the scanner's detection ledger
#                        (docs/LIVE_DETECTION.md §5A) and real only while the scanner runs the research set;
#   researched history - `history_today`/`history_week`/`last_seen`/`researched_stocks`/`occurrences`, how
#                        recently and how widely the pattern occurred in the FROZEN research run;
#   evidence           - `evidence_*`/`cells_*`/`tier_*`/`best_edge_*`, from the outcome engine.
# With the scanner on the legacy pattern set there is no ledger, so `live_detection` is False, every
# `detections_*` is None, and no caller can present the size of the history list as "N found today".
RESEARCH_LIVE_DETECTION=False
def research_live(live,key):
 """`(live_detection, {detections_*})` for one strategy, from the ledger aggregate. Absent = None, not 0."""
 available=bool(live and live.get('available'))
 if not available:
  return False,dict(detections_today=None,detections_week=None,detections_live=None,detections_symbols=None,
   detections_as_of=None,detections_scanned_at=None,detections_last_detected=None)
 bucket=(live.get('counts') or {}).get(key) or {}
 return True,dict(
  # Signal bar closed today (IST) / within the last 7 days, whatever the detection's lifecycle state now is.
  detections_today=int(bucket.get('today') or 0),detections_week=int(bucket.get('week') or 0),
  # Standing in `forming` or `confirmed` as of the newest committed pass - the live book, however long a
  # detection has been standing. A different question from "detected today"; both are served.
  detections_live=int(bucket.get('live') or 0),detections_symbols=int(bucket.get('symbols') or 0),
  # The newest detection the ledger holds for this strategy in ANY state - "last seen" for an empty active list.
  detections_last_detected=bucket.get('last_detected'),
  detections_as_of=bucket.get('as_of') or live.get('as_of'),
  detections_scanned_at=bucket.get('last_seen') or live.get('scanned_at'))

def evidence_summary_line(tier_result,tier_limited,tier_history):
 """One honest sentence about how much USABLE evidence a strategy has, composed server-side so the picker and
 the card cannot drift from the counts. A strategy whose rows are all `no_walkforward_trades` says exactly that
 instead of implying an out-of-sample result exists somewhere."""
 def stocks(n):return f'{n:,} stock{"" if n==1 else "s"}'
 if tier_result:
  tail=f' · {stocks(tier_limited)} on a limited sample' if tier_limited else ''
  rest=f' · {stocks(tier_history)} with history only' if tier_history else ''
  return f'{stocks(tier_result)} with an out-of-sample result{tail}{rest}'
 if tier_limited:
  rest=f' · {stocks(tier_history)} with history only' if tier_history else ''
  return f'No out-of-sample result yet · {stocks(tier_limited)} on a limited sample{rest}'
 if tier_history:return f'No out-of-sample result on any stock — {stocks(tier_history)} with history only'
 return 'No researched history on this timeframe'

STAMP=re.compile(r'^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})')
def stamp_text(value):
 """"2026-09-16 15:30:00" -> "16 Sep 15:30". Anything unparseable returns None rather than a guess."""
 found=STAMP.match(str(value or ''))
 if not found:return None
 try:moment=date(int(found[1]),int(found[2]),int(found[3]))
 except ValueError:return None
 return f'{moment.strftime("%d %b").lstrip("0")} {found[4]}:{found[5]}'

def research_provenance(live,evidence_end,timeframe):
 """A research card stands on TWO different clocks: live detections now, and evidence from a frozen research
 run. The footer names both. It never says "unknown age" - when a date is missing it says which one is."""
 live=live or {}
 if live.get('available'):
  moment=stamp_text(live.get('as_of')) or stamp_text(live.get('scanned_at'))
  detections=f'Detections live · {moment}' if moment else 'Detections live · time not recorded'
 else:
  detections='No live detections yet'
 if evidence_end:
  try:
   parts=[int(p) for p in str(evidence_end).split('-')]
   evidence=f'evidence from research to {day_text(date(*parts))}'
  except (ValueError,TypeError):evidence=f'evidence from research to {evidence_end}'
 else:
  evidence=f'evidence date not recorded for {timeframe}'
 return f'{detections} · {evidence}'

def research_index_key(definition):
 """The precomputed index (research_index.py) and the scanner's detection ledger are keyed by the CATALOGUE
 identity, never by the registry key. For every seeded strategy the two strings are identical. They differ only
 when the owner adds a SECOND registry entry over the same combination from the admin form (his own name, block
 or minimum sample), and that entry must resolve the same evidence rows instead of an empty strategy."""
 if definition.get('source_type')!='research_pattern':return definition['key']
 parts=[definition.get(f) for f in ('pattern_id','variant','side','timeframe')]
 return research_key(*parts) if all(parts) else definition['key']

def research_summary(definition,stat,live=None):
 """Per-strategy counts straight out of the precomputed index, plus the live ledger's own counts. A missing
 index row is "evidence pending", never a zero that reads like "we looked and found nothing"."""
 detected,live_counts=research_live(live,research_index_key(definition))
 if not stat:
  return dict(definition,live_detection=detected,**live_counts,history_today=None,history_week=None,
   researched_stocks=None,cells=None,cells_with_evidence=0,cells_tested=0,cells_loading=None,
   evidence_ready=0,evidence_total=None,tier_result=None,tier_limited=None,tier_history=None,
   evidence_summary=None,occurrences=None,best_edge_low_pct=None,best_edge_symbol=None,
   beats_baseline=0,last_seen=None,evidence_pending=True)
 # `listed` is counted from the rows this strategy actually lists, so the status line and the list can never
 # disagree about how many stocks are on screen.
 cells=int(stat['cells'] or 0);listed=int(stat['listed'] or 0)
 return dict(definition,live_detection=detected,**live_counts,
  # RESEARCHED HISTORY, not a scan: the pattern last occurred on the newest bar the index holds for this
  # timeframe / within 7 days of it. Kept under its own names so it can never be read as a detection count.
  history_today=int(stat['found'] or 0),history_week=int(stat['found_week'] or 0),
  researched_stocks=listed,cells=cells,
  cells_with_evidence=int(stat['cells_with_evidence'] or 0),cells_tested=int(stat['cells_tested'] or 0),
  cells_loading=int(stat['cells_loading'] or 0),
  evidence_ready=int(stat['cells_with_evidence'] or 0),evidence_total=listed,
  # Rows by how usable their evidence is: an accepted out-of-sample result / a limited sample / history only.
  tier_result=int(stat['tier_result'] or 0),tier_limited=int(stat['tier_limited'] or 0),
  tier_history=int(stat['tier_history'] or 0),
  evidence_summary=evidence_summary_line(int(stat['tier_result'] or 0),int(stat['tier_limited'] or 0),
   int(stat['tier_history'] or 0)),
  occurrences=int(stat['occurrences'] or 0),
  best_edge_low_pct=num(stat['best_edge_low_pct']),best_edge_symbol=stat['best_edge_symbol'],
  beats_baseline=int(stat['beats_baseline'] or 0),last_seen=stat['last_seen'],evidence_pending=False)

def live_default_slots(summaries):
 """A research block's default A/B = its two strategies with the most ACTIVE setups (forming + confirmed) right
 now, so no card opens on an empty list. Ties go to registry order. Only when fewer than two strategies have a
 live setup does a slot fall back to the registry default. The registry's own choice is kept as
 `registry_default_slot`; a user's saved choice still wins on the client (logic.resolveSlot)."""
 ranked=sorted((s for s in summaries if (s.get('detections_live') or 0)>0),
  key=lambda s:(-int(s['detections_live']),s.get('order') or 0,s['key']))
 if not ranked:return [dict(s,registry_default_slot=s.get('default_slot')) for s in summaries]
 chosen=[s['key'] for s in ranked[:2]]
 registry={s['default_slot']:s['key'] for s in summaries if s.get('default_slot') in ('A','B')}
 for slot in ('A','B'):
  if len(chosen)>=2:break
  fallback=registry.get(slot)
  if fallback and fallback not in chosen:chosen.append(fallback)
 slots={key:slot for key,slot in zip(chosen,('A','B'))}
 return [dict(s,registry_default_slot=s.get('default_slot'),default_slot=slots.get(s['key'])) for s in summaries]

def research_row(r):
 """One scanner row. `edge_*` is the engine's measured difference from entering the same stock on any eligible
 bar (same window, horizon and cost model) - not a raw hold-to-close return."""
 state=r['evidence_state'] or 'loading'
 return dict(symbol=r['symbol'],company=r['company'] or r['symbol'],sector=r['sector'],market_cap_tier=r['market_cap_tier'],
  has_evidence=bool(r['has_evidence']),
  # `evidence_tier` is the ranking tier (0 = accepted out-of-sample result, higher = less usable).
  # `has_numbers` is about THIS ROW's three columns (edge low / out-of-sample average / walk-forward n): it is
  # False whenever they would all be blank, which is the case for every cell without an accepted walk-forward
  # selection. When it is False the UI shows `label` -- the contract's reason -- instead of three dashes.
  evidence_tier=int(r['evidence_tier'] if r['evidence_tier'] is not None else cards.TIER_UNKNOWN),
  has_numbers=r['edge_low_pct'] is not None or bool(r['oos_n']),
  occurrences=r['occurrences'],walkforward_n=r['oos_n'],
  walkforward_mean_pct=num(r['oos_expectancy_pct']),win_rate=num(r['oos_win_rate_pct']),
  edge_mean_pct=num(r['edge_mean_pct']),edge_low_pct=num(r['edge_low_pct']),edge_high_pct=num(r['edge_high_pct']),
  beats_baseline=bool(r['beats_baseline']),baseline_mean_pct=num(r['baseline_mean_net_return_pct']),
  p_target_first=num(r['p_target_first'],1),p_stop_first=num(r['p_stop_first'],1),
  selected_horizon=r['selected_horizon'],q_value=num(r['q_value'],4),p_value=num(r['p_value'],4),
  last_seen=r['last_seen'],found=bool(r['found']),
  evidence_state=state,label=cards.label_for(state),research_status=r['research_status'],selection_status=r['selection_status'])

def strategy_row(match,side):
 h=next((h for h in match.get('history') or [] if h.get('side')==side),None) or {}
 ref=h.get('reference') or {};ci=pair(ref.get('expectancy_ci95'));n=int(ref.get('n') or 0)
 status=h.get('status') if h.get('status') in CELL_STATUSES else 'unknown';test=None
 if status=='tested':
  t=h.get('test') or {};test=dict(n=int(t.get('n') or 0),expectancy_pct=num(t.get('expectancy_pct')),expectancy_ci95=pair(t.get('expectancy_ci95')))
 return dict(symbol=match.get('symbol'),company=match.get('company') or match.get('symbol'),sector=match.get('sector'),match_id=match.get('id'),
  timeframe=match.get('timeframe'),side=side,direction=match.get('direction') or '',state=match.get('state') or '',price=num(match.get('price')),
  candle_end=match.get('candle_end'),low_pct=ci[0] if ci else None,high_pct=ci[1] if ci else None,avg_pct=num(ref.get('expectancy_pct')),
  win_rate=num(ref.get('win_rate')),n=n,sample_label=sample_label(n),status=status,test=test,
  evidence_basis='tested_rule' if status=='tested' else 'hold_period_history' if n>0 else 'none')
def rank_key(r):return (r['low_pct'] is None,-(r['low_pct'] or 0),-r['n'],r['symbol'] or '')
def strategy_rows(definition,matches):
 p,tf,side=definition['pattern'],definition['timeframe'],definition['side']
 rows=[strategy_row(m,side) for m in matches if m.get('pattern')==p and m.get('timeframe')==tf and side in DIRECTION_SIDES.get(m.get('direction'),())]
 return sorted(rows,key=rank_key)
def summarize(definition,rows):
 lows=[r['low_pct'] for r in rows if r['n']>=definition['min_trades'] and r['low_pct'] is not None]
 return dict(definition,found=len(rows),best_low_pct=max(lows) if lows else None,positive_low_count=sum(v>0 for v in lows),
  tested_count=sum(r['status']=='tested' for r in rows))

def text(data,field,limit,default=None,required=False):
 value=data.get(field,default)
 if value is None and not required:return default
 if not isinstance(value,str) or len(value.strip())>limit or (required and not value.strip()):
  raise PilotError(400,'FIELD_INVALID',f'{field} must be text of up to {limit} characters'+(' and is required.' if required else '.'))
 return value.strip()
def whole(data,field,low,high,default):
 value=data.get(field,default)
 if isinstance(value,bool) or not isinstance(value,int) or not low<=value<=high:raise PilotError(400,'FIELD_INVALID',f'{field} must be a whole number from {low} to {high}.')
 return value
def flag(data,field,default):
 value=data.get(field,default)
 if not isinstance(value,bool):raise PilotError(400,'FIELD_INVALID',f'{field} must be true or false.')
 return value
def tag_list(data,default):
 value=data.get('tags',default)
 if not isinstance(value,list) or len(value)>12 or any(not isinstance(t,str) or not t.strip() or len(t.strip())>30 for t in value):
  raise PilotError(400,'FIELD_INVALID','tags must be a list of up to 12 labels, each up to 30 characters.')
 return list(dict.fromkeys(t.strip() for t in value))
def choice(data,field,options,default):
 value=data.get(field,default)
 if value not in options:raise PilotError(400,'FIELD_INVALID',f"{field} must be one of: {', '.join('null' if o is None else str(o) for o in options)}.")
 return value

class Strategies:
 def __init__(self,db,evidence,settings,store=None,index=None,live=None):
  self.db=db;self.evidence=evidence;self.settings=settings;self.store=store;self.index=index
  # Read-only reader over the scanner's detection ledger. None (or a ledger that is not there) means live
  # detection is off and every research card keeps serving the researched history exactly as before.
  self.live=live

 def seed(self):
  """Insert the v1 registry once, when it is empty. Never overwrites admin changes; needs no research service.
  On an already-seeded registry it only renames strategies still carrying the old generated default name to the new
  default, and adds any researched-pattern block that is not present yet (so an existing pilot database gains the
  107-pattern catalogue without losing the owner's edits)."""
  with self.db.tx() as c:
   if c.execute(select(strategy_blocks.c.key).limit(1)).first():
    for r in c.execute(select(strategies.c.key,strategies.c.name,strategies.c.source_config)).mappings().all():
     cfg=r['source_config'] or {};p,tf,side=cfg.get('pattern'),cfg.get('timeframe'),cfg.get('side')
     if p in NAMES and side in SIDES[p] and tf in TIMEFRAMES and r['name']==old_default_name(p,tf,side)!=default_name(p,tf,side):
      c.execute(strategies.update().where(strategies.c.key==r['key']).values(name=default_name(p,tf,side),updated=now()))
    self._seed_research(c)
    return False
   t=now()
   for key,title,description,kind,position,enabled in SEED_BLOCKS:
    c.execute(strategy_blocks.insert().values(key=key,title=title,description=description,kind=kind,position=position,enabled=enabled,created=t,updated=t))
   position=0
   for tf in TIMEFRAMES:
    for pattern,*_ ,sides in PATTERNS:
     for side in sides:
      position+=10;key=f'{pattern}-{tf}-{side}'
      c.execute(strategies.insert().values(key=key,block_key='chart',name=default_name(pattern,tf,side),description=default_description(pattern,tf,side),
       tags=default_tags(tf,side),source_type='stored_pattern',source_config=dict(pattern=pattern,timeframe=tf,side=side),audience=default_audience(tf),
       min_trades=DEFAULT_MIN_TRADES,default_slot=DEFAULT_SLOTS.get(key),position=position,enabled=True,created_by=None,created=t,updated=t))
   self._seed_research(c)
  return True

 def _seed_research(self,c):
  """Seed the four researched-pattern blocks from the frozen catalogue. Idempotent and additive: an existing
  block or strategy key is left exactly as the owner last saved it."""
  store=self.store
  if not store or not store.catalogue.specs:return 0
  present={r['key'] for r in c.execute(select(strategy_blocks.c.key)).mappings()}
  t=now();added=0
  for key,title,description,kind,position,family,enabled in RESEARCH_BLOCKS:
   if key in present:
    old=OLD_RESEARCH_BLOCK_DESCRIPTIONS.get(key)
    if old and old!=description:
     c.execute(strategy_blocks.update().where(strategy_blocks.c.key==key,strategy_blocks.c.description==old)
      .values(description=description,updated=t))
    continue
   c.execute(strategy_blocks.insert().values(key=key,title=title,description=description,kind=kind,position=position,
    enabled=enabled,created=t,updated=t))
  existing={r['key'] for r in c.execute(select(strategies.c.key)).mappings()}
  rows=[];position=0
  for spec in store.catalogue.specs:
   block=FAMILY_BLOCK.get(spec['family'])
   if not block:continue
   for tf in RESEARCH_TIMEFRAMES:
    position+=10;key=research_key(spec['pattern_id'],spec['variant'],spec['side'],tf)
    if key in existing:
     old,new=_first_seed_research_name(spec,tf),research_name(spec,tf)
     if old!=new:c.execute(strategies.update().where(strategies.c.key==key,strategies.c.name==old).values(name=new,updated=t))
     continue
    rows.append(dict(key=key,block_key=block,name=research_name(spec,tf),description=research_description(spec,tf),
     tags=research_tags(spec,tf),source_type='research_pattern',
     source_config=dict(pattern_id=spec['pattern_id'],variant=spec['variant'],side=spec['side'],timeframe=tf,
      research_run=store.research_run,state=spec['state'],family=spec['family'],name=spec['name'],
      definition_version=spec.get('definition_version')),
     audience=default_audience(tf),min_trades=RESEARCH_MIN_TRADES,default_slot=RESEARCH_SLOTS.get(key),
     position=position,enabled=True,created_by=None,created=t,updated=t))
  if rows:c.execute(strategies.insert(),rows);added=len(rows)
  return added

 def version(self,c):return 1+(c.execute(select(func.count()).select_from(audit).where(audit.c.kind==REGISTRY_KIND)).scalar() or 0)
 def registry(self,enabled_only):
  with self.db.tx() as c:
   q=select(strategy_blocks).order_by(strategy_blocks.c.position,strategy_blocks.c.key)
   s=select(strategies).order_by(strategies.c.position,strategies.c.key)
   if enabled_only:q=q.where(strategy_blocks.c.enabled==True);s=s.where(strategies.c.enabled==True)
   blocks=[dict(r) for r in c.execute(q).mappings()];items=[dict(r) for r in c.execute(s).mappings()]
   return self.version(c),blocks,items

 # Research reads happen outside any database transaction (SQLite write lock).
 def freshness(self):
  """Data dates for the page header, plus how the scanner answered (BACKLOG item 2a).

  A scanner that is restarting or still loading is NOT an error here: the last successful `/api/state` is
  replayed with provenance (`served_from_cache`, `cached_at`, `upstream_error`, `upstream_down_seconds`) so
  the page can say "Showing the last scan - 17 Sep 15:30 - reconnecting" instead of a full-page error. With
  nothing cached at all - a first ever run - the dates are unknown and `unreachable` says why, and the
  researched blocks (which read local indexes, not the scanner) still serve.
  """
  try:state=self.evidence.get('/api/state',{},cache=True) or {}
  except PilotError as error:
   if error.status<500:raise
   return dict(data_end=None,age_days=None,stale=True,scanned_at=None,pattern_set='legacy',
    served_from_cache=False,cached_at=None,cached_as_of=None,unreachable=True,
    upstream_error=self.evidence.upstream_error or error.message,upstream_down_seconds=self.evidence.down_seconds())
  end,age=data_age(state)
  runs=[v.get('last_scan') for v in (state.get('schedule') or {}).values() if isinstance(v,dict) and v.get('last_scan')]
  stale=end is None or age>max_data_age(self.settings) or state.get('source_stale') is True
  # The replayed body carries its own provenance (evidence.Evidence.provenance), so the page reads it from
  # the same object it read the dates from - a cached date and a "live" stamp can never be mixed.
  cached=state.get('served_from_cache') is True
  # Which detector set the scanner is running (docs/LIVE_DETECTION.md §4). `research` means the stored
  # 10-pattern scan legitimately has nothing to show, which is a different thing from the scanner being down.
  return dict(data_end=end.isoformat() if end else None,age_days=age,stale=stale,scanned_at=str(max(runs)) if runs else None,
   pattern_set=state.get('pattern_set') or 'legacy',unreachable=False,
   served_from_cache=cached,cached_at=state.get('cached_at') if cached else None,
   cached_as_of=state.get('cached_as_of') if cached else None,
   upstream_error=state.get('upstream_error') if cached else None,
   upstream_down_seconds=int(state.get('upstream_down_seconds') or 0) if cached else 0)
 def universe(self,key):
  options=self.evidence.get('/api/filter-options',{},cache=True) or {}
  found=next((u for u in options.get('universes') or [] if isinstance(u,dict) and u.get('value')==key),None)
  if not found:raise PilotError(400,'UNIVERSE','Choose a supported stock universe.')
  return dict(key=key,label=universe_label(key,found.get('label')),count=int(found.get('count') or 0))
 def stored_matches(self,universe):
  """`(matches, reason)` for the stored 10-pattern scan.

  The scanner on the RESEARCH pattern set answers with an object whose matches are research detections with
  no legacy backtest history. Joining those into a stored-pattern strategy would be exactly the
  pattern-name-only fallback EVIDENCE_SERVING_CONTRACT.md §2 forbids, so the stored blocks get an EMPTY list
  and the reason - not a 503. A 503 would take the admin page, the preview and every stored results read down
  with it, although nothing is actually broken: that detector simply is not running.
  """
  value=self.evidence.get('/api/matches',{'min_trades':'0','universe':universe},cache=True)
  if isinstance(value,dict) and value.get('pattern_set')==RESEARCH_PATTERN_SET:return [],STORED_SUPERSEDED
  if not isinstance(value,list):raise PilotError(503,'RESEARCH_UNAVAILABLE','Strategy results are unavailable right now. Retry shortly.')
  return value,None
 def matches(self,universe):
  """Kept for callers that want the list alone; an unavailable stored scan is still a 503 for them."""
  found,reason=self.stored_matches(universe)
  if reason:raise PilotError(503,'STORED_SCAN_UNAVAILABLE',reason)
  return found

 # --- live detections of the researched patterns (docs/LIVE_DETECTION.md) ----------------------------------
 def live_state(self):
  """What live detection can actually serve right now. Cheap: one cached aggregate over the ledger."""
  if not self.live:return dict(available=False,ledger=None,reason='Live detection is not configured on this server.')
  try:return self.live.state()
  except Exception as error:  # the ledger is the scanner's file; an unreadable one degrades, never 500s
    return dict(available=False,ledger=None,reason=f'{type(error).__name__}: {error}')
 def live_counts(self):
  """The whole per-strategy aggregate, or None. Passed to research_summary for every card in one call."""
  if not self.live:return None
  try:
   snapshot=self.live.snapshot()
  except Exception:return None
  return snapshot if snapshot.get('available') else None

 # --- researched pattern catalogue -------------------------------------
 def research_state(self):
  """Index freshness + publication gate. Cheap: one read of the index metadata table."""
  if not self.index:return None
  state=self.index.state();self.index.ensure()  # a stale index rebuilds in the background; this call never waits
  return state

 def catalog(self,universe='nifty500'):
  version,blocks,items=self.registry(True)
  fresh=self.freshness()
  # The stored scan and the researched catalogue are independent sources. If the scanner is down, the research
  # blocks still serve and the stored blocks say so - one unavailable source never blanks the whole page.
  needs_scan=any(s['source_type']=='stored_pattern' for s in items)
  info=None;matches=[];scan_error=None
  if needs_scan:
   try:info=self.universe(universe);matches,scan_error=self.stored_matches(universe)
   except PilotError as error:
    # Only an unavailable source degrades. A bad `universe` from the caller is still a 400.
    if error.status<500:raise
    scan_error=error.message
  stats=self.index.stats() if self.index else {}
  # ONE cached aggregate over the detection ledger serves the live counts for all 1,048 research strategies.
  # Per-strategy fan-out over the scanner's /api/matches is not an option at this scale (see detections.py).
  live=self.live_counts()
  out=[]
  for b in blocks:
   defs=[strategy_def(s) for s in items if s['block_key']==b['key']]
   summaries=[];unavailable=None;stored=False
   for d in defs:
    if d['source_type']=='stored_pattern':
     stored=True
     if scan_error:unavailable=scan_error;summaries.append(dict(d,found=None,best_low_pct=None,positive_low_count=0,tested_count=0))
     else:summaries.append(summarize(d,strategy_rows(d,matches)))
    elif d['source_type']=='research_pattern':summaries.append(research_summary(d,stats.get(research_index_key(d)),live))
    else:summaries.append(dict(d,found=0,best_low_pct=None,positive_low_count=0,tested_count=0))
   # A block made only of stored patterns, while the scanner runs the research set, is not broken - its 10
   # patterns are a SUBSET of the 107 the research blocks already show. Discover leaves it out entirely
   # (BACKLOG item 2.6: hidden, not explained). The admin registry (self.admin) still lists it, and on the
   # legacy set it is served exactly as before. A mixed block is never dropped.
   superseded=bool(stored and defs and all(d['source_type']=='stored_pattern' for d in defs)
    and fresh.get('pattern_set')==RESEARCH_PATTERN_SET)
   if superseded:continue
   # No card opens empty: a block of researched patterns defaults to the strategies with setups right now.
   if any(s.get('source_type')=='research_pattern' for s in summaries):summaries=live_default_slots(summaries)
   out.append(dict(block_def(b),strategies=summaries,unavailable=unavailable,superseded=False,superseded_note=None))
  return dict(fresh,registry_version=version,universe=info,universe_error=scan_error,blocks=out,
   research=self.research_state(),live=self.live_state())

 def results(self,key,universe='nifty500',limit=None,enabled_only=True,offset=0,search=None):
  with self.db.tx() as c:
   item=row(c,select(strategies).where(strategies.c.key==key));block=item and row(c,select(strategy_blocks).where(strategy_blocks.c.key==item['block_key']))
  if not item or enabled_only and not (item['enabled'] and block and block['enabled']):raise PilotError(404,'STRATEGY_NOT_FOUND','This strategy is not available.')
  definition=strategy_def(item)
  if definition['source_type']=='research_pattern':return self.research_results(definition,limit=limit,offset=offset,search=search)
  if definition['source_type']!='stored_pattern':raise PilotError(409,'SOURCE_UNSUPPORTED','This strategy source is not connected yet.')
  fresh=self.freshness();info=self.universe(universe)
  found,reason=self.stored_matches(universe)
  rows=strategy_rows(definition,found)
  # An empty set WITH a reason is not the same claim as an empty set without one, so the reason is served
  # beside the rows and the card prints it instead of "no stock matches this strategy".
  return dict(fresh,strategy=summarize(definition,rows),universe=info,source='stored_scan',
   rows=rows[:limit] if limit else rows,total=len(rows),unavailable=reason)

 def research_results(self,definition,limit=None,offset=0,search=None):
  """Rows come only from the precomputed index; the research databases are never scanned on a request."""
  if not self.index:raise PilotError(503,'RESEARCH_UNAVAILABLE','Pattern research is not available on this server.')
  key=research_index_key(definition);stats=self.index.stats([key])
  rows=[research_row(r) for r in self.index.rows(key,limit=limit,offset=offset,search=search)]
  state=self.research_state() or {};coverage=self.index.coverage(key)
  live=self.live_counts();summary=research_summary(definition,stats.get(key),live)
  live_state=self.live_state()
  evidence_end=(state.get('latest_by_timeframe') or {}).get(definition['timeframe'])
  evidence_age=None
  if evidence_end:
   try:evidence_age=max(0,(market_today()-date(*[int(p) for p in evidence_end.split('-')])).days)
   except (ValueError,TypeError):evidence_age=None
  return dict(strategy=summary,source='research_index',rows=rows,
   total=self.index.row_count(key,search=search),offset=int(offset or 0),
   live_detection=bool(summary['live_detection']),live=live_state,coverage=coverage,
   universe=dict(key='research',label='Researched stocks',count=state.get('symbols_research') or 0),
   # Freshness on a research card is NOT the stored-scan kind. `data_end` is the research run's own data end for
   # THIS timeframe (a real date, so nothing can print "unknown age"), and `stale` stays False because a frozen
   # research run is not stale prices - index staleness lives under `research.stale`, which is a different thing.
   data_end=evidence_end,age_days=evidence_age,stale=False,scanned_at=(live_state or {}).get('as_of') or state.get('built_at'),
   evidence_end=evidence_end,evidence_age_days=evidence_age,
   provenance=research_provenance(live_state,evidence_end,definition['timeframe']),
   research=state)

 def research_strategy(self,key):
  """The registered research strategy behind `key`, or the right refusal. Shared by the card and the list."""
  with self.db.tx() as c:item=row(c,select(strategies).where(strategies.c.key==key))
  if not item:raise PilotError(404,'STRATEGY_NOT_FOUND','This strategy does not exist.')
  definition=strategy_def(item)
  if definition['source_type']!='research_pattern':raise PilotError(409,'SOURCE_UNSUPPORTED','This strategy has no researched evidence card.')
  return item,definition

 def detections(self,key,scope=live_detections.DEFAULT_SCOPE,limit=None,offset=0):
  """Today's live detections for ONE research strategy, straight from the scanner's detection ledger.

  Bounded by construction: `limit` is capped at detections.ROW_LIMIT_MAX and `total` is the real count
  before the cut, so the card can say "showing 100 of 412" instead of hiding the rest.
  """
  item,definition=self.research_strategy(key)
  cfg=item['source_config'] or {}
  scope='live' if scope=='active' else scope if scope in live_detections.SCOPES else live_detections.DEFAULT_SCOPE
  state=self.live_state()
  live=self.live_counts()
  identity=live_detections.evidence_identity(cfg.get('research_run'),(live or {}).get('detector_spec_hash'))
  index_key=research_index_key(definition)
  base=dict(strategy=research_summary(definition,(self.index.stats([index_key]) if self.index else {}).get(index_key),live),
   source='detection_ledger',scope=scope,live=state,rows=[],total=0,offset=int(offset or 0),
   limit=min(int(limit or live_detections.DEFAULT_ROW_LIMIT),live_detections.ROW_LIMIT_MAX),
   evidence=dict(identity,label=cards.LABELS['incompatible'] if identity['status']!='identity_match' else None),
   live_detection=bool(state.get('available')))
  if not self.live or not state.get('available'):return base
  rows,total=self.live.rows(cfg.get('pattern_id'),cfg.get('variant'),cfg.get('side'),cfg.get('timeframe'),
   scope=scope,limit=base['limit'],offset=base['offset'])
  # Per-row evidence compatibility: the row's OWN detector identity decides, never the aggregate's. A ledger
  # that still holds rows written by an older detector must not inherit today's identity_match.
  # ... and the run must actually have STUDIED this cell. The scanner's universe is wider than the research
  # run's (509 symbols detected against 495 researched on this tree), so a detection can be perfectly valid and
  # still have no cell behind it. That is "never studied", not "not enough history", and it is read from the
  # index in one query rather than guessed.
  studied=self.store.studied_symbols([r['symbol'] for r in rows],cfg.get('timeframe'),cfg.get('pattern_id'),
   cfg.get('variant'),cfg.get('side')) if self.store else set()
  for r in rows:
   r['evidence']=live_detections.evidence_identity(cfg.get('research_run'),r.get('detector_spec_hash'))
   if r['evidence']['status']=='identity_match' and r['symbol'] not in studied:
    r['evidence']=dict(r['evidence'],status='not_studied',note=NOT_STUDIED_NOTE.format(symbol=r['symbol'],
     timeframe=cfg.get('timeframe'),name=definition['pattern_name'] or cfg.get('pattern_id')))
   r['evidence_compatible']=r['evidence']['status']=='identity_match'
  return dict(base,rows=rows,total=total)

 def card(self,key,symbol,detection_id=None):
  """The trader evidence card for one strategy on one stock: one precomputed cell_outcomes row plus its
  bucket rows. Nothing is computed here, and a cell never borrows another cell's numbers."""
  if not self.index or not self.store:raise PilotError(503,'RESEARCH_UNAVAILABLE','Pattern research is not available on this server.')
  item,definition=self.research_strategy(key)
  symbol=str(symbol or '').strip().upper()
  if not symbol or len(symbol)>40:raise PilotError(400,'FIELD_INVALID','Choose a stock.')
  cfg=item['source_config'] or {};state=cfg.get('state') or 'confirmed'
  outcome=self.store.outcome_run() or {};run=outcome.get('id')
  # A card opened from a LIVE detection carries that detection's id. Evidence attaches only when the
  # detection's own identity matches the run this strategy was registered against: pattern id, variant, side,
  # timeframe AND detector spec hash. Anything else is "Incompatible historical evidence" (contract §6) - the
  # card never borrows another cell's numbers and never falls back to a pattern-name match.
  detection=live=None
  if detection_id:
   detection=self.live.detection(detection_id) if self.live else None
   if not detection:raise PilotError(404,'DETECTION_NOT_FOUND','This detection is no longer in the live ledger.')
   live=live_detections.evidence_identity(cfg.get('research_run'),detection.get('detector_spec_hash'))
   same=(detection['symbol']==symbol and detection['pattern_id']==cfg.get('pattern_id')
    and detection['variant']==cfg.get('variant') and detection['side']==cfg.get('side')
    and detection['timeframe']==cfg.get('timeframe'))
   if not same:live=dict(live,status='detector_mismatch',
    note='This detection is a different pattern, variant, side or timeframe from this strategy; no compatible historical evidence.')
  identity=dict(strategy_key=key,name=definition['name'],symbol=symbol,timeframe=cfg.get('timeframe'),
   pattern_id=cfg.get('pattern_id'),variant=cfg.get('variant'),side=cfg.get('side'),state=state,
   family=cfg.get('family'),definition_version=cfg.get('definition_version'),research_run=cfg.get('research_run'),
   outcome_run=run,snapshot_id=outcome.get('snapshot_id'),source_run=outcome.get('source_run'),
   detection_id=detection['detection_id'] if detection else None)
  # Compatibility (contract §2): the card's evidence must come from the run this strategy was registered
  # against AND, when the card was opened from a live detection, that detection's own identity must match it.
  compatible=bool(run) and outcome.get('research_run')==cfg.get('research_run')
  if detection is not None and live['status']!='identity_match':compatible=False
  args=(symbol,cfg.get('timeframe'),cfg.get('pattern_id'),cfg.get('variant'),cfg.get('side'))
  research_cell=self.store.research_cell(*args)
  # A live detection can be on a stock the research run never studied at all (its universe is wider). There is
  # then no cell to be compatible WITH, so no numbers are served and the reason says exactly that, rather than
  # the misleading "Not enough historical data" a missing cell would otherwise resolve to.
  if detection is not None and research_cell is None:
   compatible=False
   live=dict(live,status='not_studied',note=NOT_STUDIED_NOTE.format(symbol=symbol,timeframe=cfg.get('timeframe'),
    name=definition['pattern_name'] or cfg.get('pattern_id')))
  row_out=self.store.cell_outcome(run,*args,state) if compatible else None
  buckets=self.store.buckets(run,*args,state) if row_out else []
  card=cards.build_card(identity,research_cell,row_out,buckets,self.store.publication(cfg.get('research_run')),compatible=compatible)
  card['strategy']=definition;card['research']=self.research_state()
  # The live side is reported separately from the evidence verdict: "detected today" and "evidence" are
  # different claims and the card must never let one stand in for the other.
  card['detection']=detection;card['live_evidence']=live
  if detection is not None and live['status']!='identity_match':
   card['note']=live.get('note') or card.get('note')
  return card

 def preview(self,key,universe='nifty500'):return self.results(key,universe,limit=5,enabled_only=False)

 def admin(self):
  version,blocks,items=self.registry(False)
  return dict(registry_version=version,blocks=[dict(block_def(b),strategies=[strategy_def(s) for s in items if s['block_key']==b['key']]) for b in blocks])
 def admin_blocks(self):
  version,blocks,_=self.registry(False)
  return dict(registry_version=version,blocks=[block_def(b) for b in blocks])
 def admin_sources(self):
  """Exactly what `create` will accept, in the registry's own words - the ONLY list the add-strategy form may
  offer. The form used to build its pattern chips from the scanner's `/api/state`, which is the detector set the
  scanner happens to be running, not the registry's accept-list; on the researched set every chip was a research
  catalogue id posted as a `stored_pattern` and the server refused all of them. Both sources are served here
  whatever the scanner is running, because `create` accepts both whatever the scanner is running.

  Nothing new is registered by reading this: a strategy created from it still goes through `create`/`create_research`
  and its evidence still passes the same publication gate (`var/evidence_release.json`)."""
  stored=dict(available=True,reason=None,block_key=SEED_BLOCKS[0][0],timeframes=list(TIMEFRAMES),
   patterns=[dict(id=pattern,name=NAMES[pattern],description=DESCRIPTIONS[pattern],sides=list(SIDES[pattern]),
    variants=[],block_key=SEED_BLOCKS[0][0]) for pattern,*_ in PATTERNS])
  if not self.store or not self.store.catalogue.specs:
   reason=('The pattern catalogue is not available on this server.' if not self.store
    else self.store.catalogue.error or 'The pattern catalogue is empty.')
   research=dict(available=False,reason=reason,run=None,timeframes=list(RESEARCH_TIMEFRAMES),patterns=[])
   return dict(stored=stored,research=research)
  # Grouped pattern -> variant -> sides, the order the form asks in. Names and descriptions come from the frozen
  # catalogue, never invented here.
  by_id={}
  for spec in self.store.catalogue.specs:
   entry=by_id.get(spec['pattern_id'])
   if entry is None:
    # The catalogue's definition, with no timeframe or side in front of it: the form has not been told either yet.
    entry=by_id[spec['pattern_id']]=dict(id=spec['pattern_id'],name=spec['name'],family=spec['family'],
     block_key=FAMILY_BLOCK.get(spec['family']),description=research_definition(spec,300),variants=[])
   variant=next((v for v in entry['variants'] if v['id']==spec['variant']),None)
   if variant is None:
    variant=dict(id=spec['variant'],label=research_variant_label(spec['variant']),state=spec['state'],sides=[])
    entry['variants'].append(variant)
   if spec['side'] not in variant['sides']:variant['sides'].append(spec['side'])
  patterns=sorted(by_id.values(),key=lambda p:(p['family'],p['id']))
  research=dict(available=True,reason=None,run=self.store.research_run,timeframes=list(RESEARCH_TIMEFRAMES),patterns=patterns)
  return dict(stored=stored,research=research)

 def create(self,user,data):
  if not isinstance(data,dict):raise PilotError(400,'FIELD_INVALID','Send the strategy as a JSON object.')
  source=choice(data,'source_type',SOURCE_TYPES,'stored_pattern')
  if source=='research_pattern':return self.create_research(user,data)
  # A researched catalogue id (CH01, CDLDOJI, ...) sent as a stored pattern is still refused - the two sources
  # resolve different evidence - but it is named for what it is instead of only listing the stored ten.
  if data.get('pattern') not in NAMES and self.store and self.store.catalogue.spec_ids(data.get('pattern')):
   raise PilotError(400,'SOURCE_MISMATCH',f"{data['pattern']} is a researched pattern. Add it with source_type "
    '"research_pattern" and its pattern_id, variant, side and timeframe.')
  pattern=choice(data,'pattern',tuple(NAMES),None);tf=choice(data,'timeframe',TIMEFRAMES,None);side=choice(data,'side',('long','short'),None)
  if side not in SIDES[pattern]:raise PilotError(400,'SIDE_UNSUPPORTED',f"{NAMES[pattern]} is researched {' and '.join(SIDES[pattern])} only.")
  explicit=data.get('key') not in (None,'')
  key=data.get('key') if explicit else None
  if explicit and (not isinstance(key,str) or not STRATEGY_KEY.fullmatch(key)):raise PilotError(400,'FIELD_INVALID','key must be 3-64 letters, digits, dashes or underscores.')
  block_key=text(data,'block_key',40,required=True);name=text(data,'name',100,default_name(pattern,tf,side),True)
  if not explicit:  # generated: slug of the name, else pattern-TF-side; made unique inside the transaction below
   key=slug(name)
   if not STRATEGY_KEY.fullmatch(key):key=f'{pattern}-{tf}-{side}'
  values=dict(key=key,block_key=block_key,name=name,
   description=text(data,'description',500,default_description(pattern,tf,side)),tags=tag_list(data,default_tags(tf,side)),source_type=source,
   source_config=dict(pattern=pattern,timeframe=tf,side=side),audience=choice(data,'audience',AUDIENCES,default_audience(tf)),
   min_trades=whole(data,'min_trades',1,1000,DEFAULT_MIN_TRADES),default_slot=choice(data,'default_slot',('A','B',None),None),enabled=flag(data,'enabled',False))
  with self.db.tx() as c:
   if not row(c,select(strategy_blocks.c.key).where(strategy_blocks.c.key==block_key)):raise PilotError(400,'BLOCK_NOT_FOUND','Choose an existing block.')
   if explicit and row(c,select(strategies.c.key).where(strategies.c.key==key)):raise PilotError(409,'STRATEGY_EXISTS',f'A strategy with key {key} already exists. Give the new one a different key.')
   if not explicit:
    base,n=key,1
    while row(c,select(strategies.c.key).where(strategies.c.key==key)):n+=1;sfx=f'-{n}';key=base[:64-len(sfx)]+sfx
    values['key']=key
   last=c.execute(select(func.max(strategies.c.position))).scalar() or 0
   t=now();position=whole(data,'order',0,1000000,last+10) if data.get('order') is not None else last+10
   if values['default_slot']:c.execute(strategies.update().where(strategies.c.block_key==block_key,strategies.c.default_slot==values['default_slot']).values(default_slot=None,updated=t))
   c.execute(strategies.insert().values(**values,position=position,created_by=user['id'],created=t,updated=t))
   record(c,user['id'],REGISTRY_KIND,'Strategy added',f"{values['name']} was added to the {block_key} block ({'enabled' if values['enabled'] else 'disabled'}).",key,dict(key=key))
   return strategy_def(row(c,select(strategies).where(strategies.c.key==key)))

 def update(self,user,key,data):
  if not isinstance(data,dict):raise PilotError(400,'FIELD_INVALID','Send the changes as a JSON object.')
  editable={'name','description','tags','audience','min_trades','default_slot','order','enabled','block_key'};fixed={'key','source_type','pattern','timeframe','side'}
  unknown=set(data)-editable-fixed-set(SUMMARY_FIELDS)
  if unknown:raise PilotError(400,'FIELD_INVALID','Unknown strategy fields: '+', '.join(sorted(unknown))+'.')
  with self.db.tx() as c:
   item=row(c,select(strategies).where(strategies.c.key==key))
   if not item:raise PilotError(404,'STRATEGY_NOT_FOUND','This strategy does not exist.')
   current=strategy_def(item)
   changed=sorted(f for f in fixed if f in data and data[f]!=current[f])
   if changed:raise PilotError(400,'FIELD_IMMUTABLE','These fields cannot change after a strategy is created: '+', '.join(changed)+'. Add a new strategy instead.')
   values={}
   if 'name' in data:values['name']=text(data,'name',100,required=True)
   if 'description' in data:values['description']=text(data,'description',500,'')
   if 'tags' in data:values['tags']=tag_list(data,[])
   if 'audience' in data:values['audience']=choice(data,'audience',AUDIENCES,None)
   if 'min_trades' in data:values['min_trades']=whole(data,'min_trades',1,1000,None)
   if 'default_slot' in data:values['default_slot']=choice(data,'default_slot',('A','B',None),None)
   if 'order' in data:values['position']=whole(data,'order',0,1000000,None)
   if 'enabled' in data:values['enabled']=flag(data,'enabled',None)
   if 'block_key' in data:
    values['block_key']=text(data,'block_key',40,required=True)
    if not row(c,select(strategy_blocks.c.key).where(strategy_blocks.c.key==values['block_key'])):raise PilotError(400,'BLOCK_NOT_FOUND','Choose an existing block.')
   values={k:v for k,v in values.items() if item[k]!=v}
   if not values:return current
   t=now();block_key=values.get('block_key',item['block_key']);slot=values.get('default_slot',item['default_slot'])
   if slot and ('default_slot' in values or 'block_key' in values):
    c.execute(strategies.update().where(strategies.c.block_key==block_key,strategies.c.default_slot==slot,strategies.c.key!=key).values(default_slot=None,updated=t))
   c.execute(strategies.update().where(strategies.c.key==key).values(**values,updated=t))
   fields=', '.join('order' if k=='position' else k for k in sorted(values))
   record(c,user['id'],REGISTRY_KIND,'Strategy updated',f"{values.get('name',item['name'])}: changed {fields}.",key,dict(key=key,fields=sorted(values)))
   return strategy_def(row(c,select(strategies).where(strategies.c.key==key)))

 def create_research(self,user,data):
  """Add a researched-pattern strategy. The identity must exist in the frozen catalogue: the admin picks a
  combination that was actually studied, never a name that has no cell behind it.

  Keys work as they do for a stored pattern. The catalogue key is used when the owner keeps the catalogue name,
  so re-adding a seeded combination is still the honest 409. Give it a name of its own (or an explicit key) and
  it becomes a SECOND registry entry over the same combination - the owner's own block, minimum sample or slot -
  which is the only kind of research strategy he can add once the seed has registered all 1,048. Its evidence
  still comes from the same catalogue cell (`research_index_key`) and through the same publication gate."""
  if not self.store or not self.store.catalogue.specs:raise PilotError(503,'RESEARCH_UNAVAILABLE','The pattern catalogue is not available on this server.')
  pattern_id=text(data,'pattern_id',40,required=True);variant=text(data,'variant',60,required=True)
  side=choice(data,'side',('long','short'),None);tf=choice(data,'timeframe',RESEARCH_TIMEFRAMES,None)
  spec=self.store.catalogue.spec(pattern_id,variant,side)
  if not spec:raise PilotError(400,'PATTERN_NOT_FOUND',f'{pattern_id} {variant} {side} is not in the researched catalogue.')
  block_key=text(data,'block_key',40,required=True)
  explicit=data.get('key') not in (None,'')
  if explicit and (not isinstance(data['key'],str) or not STRATEGY_KEY.fullmatch(data['key'])):
   raise PilotError(400,'FIELD_INVALID','key must be 3-64 letters, digits, dashes or underscores.')
  name=text(data,'name',100,research_name(spec,tf),True)
  catalogue_key=research_key(pattern_id,variant,side,tf)
  key=data['key'] if explicit else catalogue_key if name==research_name(spec,tf) else (slug(name) if STRATEGY_KEY.fullmatch(slug(name)) else catalogue_key)
  values=dict(key=key,block_key=block_key,name=name,
   description=text(data,'description',500,research_description(spec,tf)),tags=tag_list(data,research_tags(spec,tf)),
   source_type='research_pattern',source_config=dict(pattern_id=pattern_id,variant=variant,side=side,timeframe=tf,
    research_run=self.store.research_run,state=spec['state'],family=spec['family'],name=spec['name'],
    definition_version=spec.get('definition_version')),
   audience=choice(data,'audience',AUDIENCES,default_audience(tf)),
   min_trades=whole(data,'min_trades',1,1000,RESEARCH_MIN_TRADES),default_slot=choice(data,'default_slot',('A','B',None),None),
   enabled=flag(data,'enabled',False))
  with self.db.tx() as c:
   if not row(c,select(strategy_blocks.c.key).where(strategy_blocks.c.key==block_key)):raise PilotError(400,'BLOCK_NOT_FOUND','Choose an existing block.')
   taken=bool(row(c,select(strategies.c.key).where(strategies.c.key==key)))
   if taken and key==catalogue_key and not explicit:
    raise PilotError(409,'STRATEGY_EXISTS',f'{research_name(spec,tf)} is already in the registry. '
     'Give this one a name of its own to add it beside the catalogue strategy.')
   if taken and explicit:raise PilotError(409,'STRATEGY_EXISTS',f'A strategy with key {key} already exists. Give the new one a different key.')
   if taken:  # generated from the owner's own name: made unique exactly as a stored pattern's is
    base,n=key,1
    while row(c,select(strategies.c.key).where(strategies.c.key==key)):n+=1;sfx=f'-{n}';key=base[:64-len(sfx)]+sfx
    values['key']=key
   last=c.execute(select(func.max(strategies.c.position))).scalar() or 0
   t=now();position=whole(data,'order',0,1000000,last+10) if data.get('order') is not None else last+10
   if values['default_slot']:c.execute(strategies.update().where(strategies.c.block_key==block_key,strategies.c.default_slot==values['default_slot']).values(default_slot=None,updated=t))
   c.execute(strategies.insert().values(**values,position=position,created_by=user['id'],created=t,updated=t))
   record(c,user['id'],REGISTRY_KIND,'Strategy added',f"{values['name']} was added to the {block_key} block ({'enabled' if values['enabled'] else 'disabled'}).",key,dict(key=key))
   return strategy_def(row(c,select(strategies).where(strategies.c.key==key)))

 def create_block(self,user,data):
  if not isinstance(data,dict):raise PilotError(400,'FIELD_INVALID','Send the block as a JSON object.')
  key=data.get('key')
  if not isinstance(key,str) or not BLOCK_KEY.fullmatch(key):raise PilotError(400,'FIELD_INVALID','key must be 2-40 lowercase letters, digits, dashes or underscores.')
  values=dict(key=key,title=text(data,'title',80,required=True),description=text(data,'description',300,''),kind=choice(data,'kind',BLOCK_KINDS,None),enabled=flag(data,'enabled',False))
  with self.db.tx() as c:
   if row(c,select(strategy_blocks.c.key).where(strategy_blocks.c.key==key)):raise PilotError(409,'BLOCK_EXISTS',f'A block with key {key} already exists.')
   last=c.execute(select(func.max(strategy_blocks.c.position))).scalar() or 0
   position=whole(data,'order',0,1000000,last+10) if data.get('order') is not None else last+10;t=now()
   c.execute(strategy_blocks.insert().values(**values,position=position,created=t,updated=t))
   record(c,user['id'],REGISTRY_KIND,'Strategy block added',f"{values['title']} ({values['kind']}) was added ({'enabled' if values['enabled'] else 'disabled'}).",key,dict(block=key))
   return block_def(row(c,select(strategy_blocks).where(strategy_blocks.c.key==key)))

 def update_block(self,user,key,data):
  if not isinstance(data,dict):raise PilotError(400,'FIELD_INVALID','Send the changes as a JSON object.')
  unknown=set(data)-{'key','title','description','kind','order','enabled'}
  if unknown:raise PilotError(400,'FIELD_INVALID','Unknown block fields: '+', '.join(sorted(unknown))+'.')
  if 'key' in data and data['key']!=key:raise PilotError(400,'FIELD_IMMUTABLE','A block key cannot change.')
  with self.db.tx() as c:
   item=row(c,select(strategy_blocks).where(strategy_blocks.c.key==key))
   if not item:raise PilotError(404,'BLOCK_NOT_FOUND','This block does not exist.')
   values={}
   if 'title' in data:values['title']=text(data,'title',80,required=True)
   if 'description' in data:values['description']=text(data,'description',300,'')
   if 'kind' in data:values['kind']=choice(data,'kind',BLOCK_KINDS,None)
   if 'order' in data:values['position']=whole(data,'order',0,1000000,None)
   if 'enabled' in data:values['enabled']=flag(data,'enabled',None)
   values={k:v for k,v in values.items() if item[k]!=v}
   if not values:return block_def(item)
   c.execute(strategy_blocks.update().where(strategy_blocks.c.key==key).values(**values,updated=now()))
   fields=', '.join('order' if k=='position' else k for k in sorted(values))
   record(c,user['id'],REGISTRY_KIND,'Strategy block updated',f"{values.get('title',item['title'])}: changed {fields}.",key,dict(block=key,fields=sorted(values)))
   return block_def(row(c,select(strategy_blocks).where(strategy_blocks.c.key==key)))
