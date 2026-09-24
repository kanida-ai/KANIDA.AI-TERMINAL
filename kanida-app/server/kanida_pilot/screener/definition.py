"""The scanner definition: ONE structured object behind the visual builder, the natural-language box, the
15-minute evaluation, the notifications, the saved profiles and the API.

    {
      "universe":   {"kind": "all" | "indices" | "stocks" | "symbols", "symbols": [...]},
      "expiry":     "nearest" | "next" | "both",
      "strikes":    {"kind": "atm", "below": 5, "above": 5} | {"kind": "otm"|"itm", "depth": 5}
                    | {"kind": "delta", "band": "atm_like"},
      "liquid_only": false,
      "conditions": [
        {"id": "c1", "metric": "oi", "side": "CE", "state": "up_cont",
         "window": {"kind": "minutes", "value": 45}},
        {"id": "c2", "join": "and", "metric": "iv", "side": "CE", "state": "up",
         "window": {"kind": "minutes", "value": 30}}
      ]
    }

AND binds tighter than OR, the way the builder reads top to bottom: `A AND B OR C` is `(A AND B) OR C`.
`normalize` is the only way in: it fills defaults, refuses anything outside `vocab.py` with a sentence that
names it, and returns a canonical object, so two routes that mean the same scanner store the same bytes.
"""
from __future__ import annotations
import hashlib,json,re
from . import vocab as V

TIME=re.compile(r'^(0?9|1[0-5]):[0-5][05]$')


class DefinitionError(ValueError):
 pass


def _window(raw,metric):
 w=raw if isinstance(raw,dict) else {}
 kind=w.get('kind') or 'minutes'
 if kind=='minutes':
  value=int(w.get('value') or 45)
  if value%V.READING_MINUTES or not V.READING_MINUTES<=value<=V.MAX_STEPS*V.READING_MINUTES:
   raise DefinitionError(f'A window must be a whole number of 15-min readings (15-{V.MAX_STEPS*15} min); got {value} min.')
  return {'kind':'minutes','value':value}
 if kind=='readings':
  value=int(w.get('value') or 3)
  if not 1<=value<=V.MAX_STEPS:raise DefinitionError(f'A window must be 1-{V.MAX_STEPS} readings.')
  return {'kind':'readings','value':value}
 if kind=='open':return {'kind':'open'}
 if kind=='custom':
  start,end=str(w.get('from') or ''),str(w.get('to') or '')
  if not (TIME.match(start) and TIME.match(end)):
   raise DefinitionError('A custom window runs between two 15-min reading times, for example 10:15 to 11:30.')
  start,end=start.zfill(5),end.zfill(5)
  if end<=start:raise DefinitionError('A custom window must end after it starts.')
  return {'kind':'custom','from':start,'to':end}
 raise DefinitionError(f'Unknown time window "{kind}".')


def _condition(raw,index):
 if not isinstance(raw,dict):raise DefinitionError('Each condition must be an object.')
 metric=raw.get('metric')
 if metric not in V.METRICS:raise DefinitionError(f'Unknown metric "{metric}".')
 m=V.METRICS[metric]
 state=raw.get('state') or m['default_state']
 if state not in m['states']:
  raise DefinitionError(f'{m["label"]} cannot be "{V.STATE_WORDS.get(state,state)}". It can be: '
   +', '.join(V.state_word(metric,s) for s in m['states'])+'.')
 sides=V.sides_for(metric)
 side=raw.get('side') if sides else None
 if sides:
  side=side or 'either'
  if side not in sides:raise DefinitionError(f'{m["label"]} cannot be read on "{side}".')
 out={'id':str(raw.get('id') or f'c{index+1}')[:12],'metric':metric,'side':side,'state':state,
  'window':_window(raw.get('window'),metric)}
 if index:
  join=str(raw.get('join') or 'and').lower()
  if join not in ('and','or'):raise DefinitionError('Conditions join with AND or OR.')
  out['join']=join
 return out


def _strikes(raw):
 s=raw if isinstance(raw,dict) else {}
 kind=s.get('kind') or 'atm'
 if kind=='atm':
  below,above=int(s.get('below',5)),int(s.get('above',5))
  if not (0<=below<=V.MAX_RUNGS and 0<=above<=V.MAX_RUNGS):
   raise DefinitionError(f'The strike range reaches at most {V.MAX_RUNGS} strikes either side of ATM.')
  return {'kind':'atm','below':below,'above':above}
 if kind in ('otm','itm'):
  depth=int(s.get('depth',5))
  if not 1<=depth<=V.MAX_RUNGS:raise DefinitionError(f'{kind.upper()} depth is 1-{V.MAX_RUNGS} strikes.')
  return {'kind':kind,'depth':depth}
 if kind=='delta':
  band=s.get('band') or 'atm_like'
  if band not in V.DELTA_BANDS:raise DefinitionError(f'Unknown delta band "{band}".')
  return {'kind':'delta','band':band}
 raise DefinitionError(f'Unknown strike range "{kind}".')


def normalize(raw):
 """The canonical definition, or DefinitionError with the sentence the builder prints."""
 if not isinstance(raw,dict):raise DefinitionError('A scanner definition must be an object.')
 u=raw.get('universe') or {}
 if isinstance(u,str):u={'kind':u}
 kind=u.get('kind') or 'all'
 if kind not in V.UNIVERSES:raise DefinitionError(f'Unknown universe "{kind}".')
 universe={'kind':kind}
 if kind=='symbols':
  symbols=sorted({re.sub(r'[^A-Z0-9&-]','',str(x).upper()) for x in (u.get('symbols') or [])}-{''})
  if not symbols:raise DefinitionError('Choose at least one symbol.')
  if len(symbols)>50:raise DefinitionError('A scanner reads at most 50 chosen symbols.')
  universe['symbols']=symbols
 expiry=raw.get('expiry') or 'nearest'
 if expiry not in V.EXPIRIES:raise DefinitionError(f'Unknown expiry choice "{expiry}".')
 conditions=raw.get('conditions') or []
 if not isinstance(conditions,list) or not conditions:raise DefinitionError('Add at least one condition.')
 if len(conditions)>V.MAX_CONDITIONS:raise DefinitionError(f'A scanner holds at most {V.MAX_CONDITIONS} conditions.')
 conds=[_condition(c,i) for i,c in enumerate(conditions)]
 ids=[c['id'] for c in conds]
 if len(set(ids))!=len(ids):conds=[{**c,'id':f'c{i+1}'} for i,c in enumerate(conds)]
 strikes=_strikes(raw.get('strikes'))
 return {'universe':universe,'expiry':expiry,'strikes':strikes,'liquid_only':bool(raw.get('liquid_only')),
  'conditions':conds}


def groups(definition):
 """The OR of AND-groups the conditions read as, top to bottom."""
 out=[[]]
 for c in definition['conditions']:
  if c.get('join')=='or':out.append([])
  out[-1].append(c)
 return out


def grain(definition):
 """'contract' when every option-contract condition is about ONE side (or the contract's own side), so a
 single contract can meet them all; 'book' (one underlying + expiry) otherwise — a cross-side scanner such
 as "call OI building AND put OI unwinding" is a statement about the book, never about one contract."""
 contract=[c for c in definition['conditions'] if V.METRICS[c['metric']]['grain']=='contract']
 if not contract:return 'book'
 # 'either' means "the contract's own side", so it never makes a scanner cross-side on its own
 named={c['side'] for c in contract}-{'either'}
 return 'contract' if len(named)<=1 else 'book'


def digest(definition):
 """Stable id of what a definition MEANS. A saved scanner's history belongs to this, not to its name."""
 return hashlib.sha256(json.dumps(definition,sort_keys=True,separators=(',',':')).encode()).hexdigest()[:16]


def uses_computed(definition):
 return any(c['metric'] in V.COMPUTED_METRICS for c in definition['conditions']) or definition['strikes']['kind']=='delta'


def condition_text(c):
 m=V.METRICS[c['metric']]
 side=c.get('side')
 who={'CE':'call ','PE':'put ','either':'','both':'call and put '}.get(side or '','')
 noun=m['noun']
 if c['metric']=='flow':
  word={'writing':'writing','buying':'buying','short_covering':'short covering','buyers_exiting':'buyers exiting'}[c['state']]
  subject={'CE':'Call','PE':'Put','either':'Call or put'}.get(side,'')
  return f"{subject} {word} over the {V.window_label(c['window'])}"
 if c['metric']=='spread':
  return f"{who}{noun} {V.state_word(c['metric'],c['state']).lower()} over the {V.window_label(c['window'])}".strip().capitalize()
 word=V.state_word(c['metric'],c['state']).lower()
 w=c['window']
 when=V.window_label(w)
 when='since market open' if w['kind']=='open' else (f'between {w["from"]} and {w["to"]}' if w['kind']=='custom' else f'over the {when}')
 text=f"{who}{noun} {word} {when}"
 if c['metric']=='volume':
  text=f"{who}{noun} {word} at the latest reading" if w.get('value')==15 else f"{who}{noun} {word} across the {V.window_label(w)}"
 return text[0].upper()+text[1:]


def reads_as(definition):
 """The one plain sentence under the builder. Written from the definition, never from the UI's text."""
 def lower(text):
  # "Call OI ..." -> "call OI ...", but an acronym stays whole: "PCR ...", "IV ..."
  return text if len(text)>1 and text[1].isupper() else text[:1].lower()+text[1:]
 parts=[]
 for i,g in enumerate(groups(definition)):
  body=' AND '.join(condition_text(c) if j==0 else lower(condition_text(c)) for j,c in enumerate(g))
  parts.append(body if len(groups(definition))==1 else f'({body})')
 u=definition['universe']
 where={'all':'indices and F&O stocks','indices':'indices','stocks':'F&O stocks'}.get(u['kind']) or ', '.join(u.get('symbols') or [])
 s=definition['strikes']
 band=(f"ATM ±{s['below']}" if s['kind']=='atm' and s['below']==s['above'] else
  f"ATM −{s['below']} to +{s['above']}" if s['kind']=='atm' else
  f"{s['depth']} {s['kind'].upper()} strikes" if s['kind'] in ('otm','itm') else
  f"{V.DELTA_BANDS[s['band']]['label']} strikes (|Δ| {V.DELTA_BANDS[s['band']]['lo']:.2f}–{V.DELTA_BANDS[s['band']]['hi']:.2f})")
 expiry={'nearest':'the nearest expiry','next':'the next expiry','both':'both captured expiries'}[definition['expiry']]
 # the strike range only matters to conditions read on contracts; PCR, max pain and the like read the whole book
 uses_strikes=any(V.METRICS[c['metric']]['grain'] in ('contract','side') for c in definition['conditions'])
 tail=f", within {band} of {expiry}, across {where}" if uses_strikes else f", for {expiry}, across {where}"
 if definition.get('liquid_only'):tail+=', liquid strikes only'
 return ' OR '.join(parts)+tail+'.'
