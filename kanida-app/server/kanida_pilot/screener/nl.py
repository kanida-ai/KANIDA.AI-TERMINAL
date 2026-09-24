"""Natural language → the SAME scanner definition the visual builder makes (owner decision Q4: a deterministic
parser first — no model call, no key, the same sentence always gives the same scanner).

It never guesses silently. Every phrase it used is returned in `understood`, every default or carry-over it
applied is in `assumptions`, and every meaningful word it could not map is in `unmapped`, which the builder
shows as a red chip. A number the user typed ("grown 100%") is not a threshold here: the parser says what state
word it read it as, and the state word's own rule decides.
"""
from __future__ import annotations
import re
from . import definition as D,vocab as V

NUMBERS={'one':1,'two':2,'three':3,'four':4,'five':5,'six':6,'eight':8,'ten':10}
NUM=r'(\d+|one|two|three|four|five|six|eight|ten)'

# --- metric phrases, longest / most specific first ------------------------------------------------------
METRIC_PHRASES=[
 (r'max[\s-]*pain','maxpain'),(r'put[\s-]*call[\s-]*ratio|\bpcr\b','pcr'),
 (r'futures?\s*(?:oi|open interest)|\bfut\s*oi\b','futures_oi'),
 (r'across\s*(?:multiple|several|many|more|nearby)?\s*strikes|multiple\s*strikes|nearby\s*strikes|\bbroad\b|strikes?\s*spread|'
  r'more\s*strikes|fewer\s*strikes','spread'),
 (r'implied\s*vol(?:atility)?|\bvolatility\b|\bivs?\b','iv'),
 (r'short[\s-]*covering','flow:short_covering'),(r'buyers?\s*(?:are\s*)?(?:exiting|exit)|long\s*unwinding','flow:buyers_exiting'),
 (r'\bwrit(?:ing|ten|ers?|e)\b','flow:writing'),(r'\bbuying\b|\bbuyers?\s*(?:are\s*)?(?:entering|adding)','flow:buying'),
 (r'build[\s-]*up','flow'),
 (r'open\s*interest|\bois?\b','oi'),
 (r'\bspot\b|\bunderlying\b|index\s*level|stock\s*price','underlying'),
 (r'\bpremiums?\b|option\s*price|\bltp\b|\bprices?\b','premium'),
 (r'\bdelta\b','delta'),(r'\bgamma\b','gamma'),
 (r'\bvolumes?\b|\bturnover\b|\btraded\b','volume'),
]
STATE_PHRASES=[
 (r'reversing\s*higher|turn(?:ing|ed|s)?\s*(?:up|higher)|bouncing|rebounding','rev_up'),
 (r'reversing\s*lower|turn(?:ing|ed|s)?\s*(?:down|lower)|rolling\s*over','rev_down'),
 (r'contracting\s*rapidly|crash(?:ing)?|collaps(?:ing|e)|(?:falling|dropping|cooling)\s*(?:sharply|fast|rapidly)','down_rapid'),
 (r'expanding\s*rapidly|rapid(?:ly)?|sharp(?:ly)?|surg(?:e|ing|ed)|spik(?:e|ing|ed)|jump(?:ing|ed)?|explod(?:ing|ed)|'
  r'shoot(?:ing)?\s*up|doubl(?:ed|ing|e)|\bfast\b|\d+\s*%','up_rapid'),
 (r'concentrat(?:ing|ed|e)|narrow(?:ing|ed)?|fewer\s*strikes','concentrating'),
 (r'broaden(?:ing|ed)?|spread(?:ing)?\s*(?:out)?|widen(?:ing|ed)?','broadening'),
 (r'unusual(?:ly)?|abnormal|heavy|very\s*active|spike\s*in\s*volume','unusual'),
 (r'\bquiet\b|\bcalm\b|\bdull\b|no\s*activity','quiet'),
 (r'\bstable\b|\bflat\b|\bsteady\b|unchanged|sideways','stable'),
 (r'shift(?:ing|ed)?\s*(?:higher|up)|mov(?:ing|ed)\s*(?:higher|up)','up'),
 (r'shift(?:ing|ed)?\s*(?:lower|down)|mov(?:ing|ed)\s*(?:lower|down)','down'),
 (r'increas(?:ing|ed|e)|ris(?:ing|en|e)|\bup\b|higher|build(?:ing|s)?|\badding\b|grow(?:ing|n|s)?|grew|expand(?:ing|ed|s)?|'
  r'climb(?:ing|ed)?|gain(?:ing|ed)?','up'),
 (r'decreas(?:ing|ed|e)|fall(?:ing|en)?|fell|\bdown\b|lower|dropp?(?:ing|ed)?|declin(?:ing|ed|e)|cool(?:ing|ed)?|'
  r'unwind(?:ing)?|shedding|reduc(?:ing|ed)|contract(?:ing|ed)?','down'),
]
CONTINUOUS=r'continu(?:ous(?:ly)?|ing|e)|steadily|consistently|persistently|every\s*reading|each\s*reading|non[\s-]*stop|'\
 r'consecutive(?:ly)?|straight|in\s*a\s*row|has\s*been|have\s*been|kept|keeps'
SIDES=[(r'\bboth\s*sides?\b','both'),(r'\beither\s*side\b','either'),(r'\bcalls?\b|\bce\b','CE'),(r'\bputs?\b|\bpe\b','PE')]
STOP=set('''show me find list give get where which that the a an has have had been is are was were be being for of in on
at to from last past previous over during with by it its their there any all some stocks stock f&o fno options option
contracts contract instruments instrument and or but while also side sides please scan scanner market markets today
minutes minute mins min readings reading cycles cycle candles candle bars bar intervals interval hour hours where
whose when than more most very is are grown just now currently still'''.split())


def _num(text):return NUMBERS.get(text,None) if not text.isdigit() else int(text)


def _scope(text,underlyings):
 """Universe, expiry, strike range and liquidity — the parts of a sentence that describe WHERE to look."""
 found,notes={},[]
 t=text.lower()
 # An index name is recognised in any case; a stock symbol only in CAPITALS, so that ordinary words ("idea",
 # "page", "bel") never turn into a symbol filter.
 syms=[u for u in underlyings if (u in V.INDEX_UNDERLYINGS and re.search(rf'(?<![a-z0-9]){re.escape(u.lower())}(?![a-z0-9])',t))
  or re.search(rf'(?<![A-Za-z0-9]){re.escape(u)}(?![A-Za-z0-9])',text)]
 if syms:
  found['universe']={'kind':'symbols','symbols':syms}
  for u in syms:t=re.sub(rf'(?<![a-z0-9]){re.escape(u.lower())}(?![a-z0-9])',' ',t)
 elif re.search(r'f\s*&\s*o\s*stocks?|fno\s*stocks?|\bstocks?\b|stock\s*options',t):found['universe']={'kind':'stocks'}
 elif re.search(r'\bindices\b|\bindexes\b|\bindex\b(?!\s*level)',t):found['universe']={'kind':'indices'}
 m=re.search(r'\batm\s*(?:±|\+/-|\+-|plus\s*or\s*minus)\s*(\d+)|\bwithin\s*(\d+)\s*strikes?\s*(?:of\s*)?(?:the\s*)?atm',t)
 if m:
  k=int(m.group(1) or m.group(2));found['strikes']={'kind':'atm','below':min(k,V.MAX_RUNGS),'above':min(k,V.MAX_RUNGS)}
  t=t[:m.start()]+' '+t[m.end():]
 elif re.search(r'\batm\s*only\b|at\s*the\s*money\s*only',t):found['strikes']={'kind':'atm','below':0,'above':0}
 elif re.search(r'\b(?:near|around|close\s*to)\s*(?:the\s*)?(?:atm|at\s*the\s*money|money)\b',t):
  found['strikes']={'kind':'atm','below':2,'above':2};notes.append('"Near ATM" read as ATM ±2.')
 elif re.search(r'\bdeep\s*otm\b',t):found['strikes']={'kind':'delta','band':'deep_otm'}
 elif re.search(r'\botm\b|out\s*of\s*the\s*money',t):found['strikes']={'kind':'otm','depth':5}
 elif re.search(r'\bitm\b|in\s*the\s*money',t):found['strikes']={'kind':'itm','depth':5}
 t=re.sub(r'\b(?:near|around|close\s*to)\s*(?:the\s*)?(?:atm|at\s*the\s*money)\b|\bdeep\s*otm\b|\botm\b|\bitm\b|\batm(?:\s*only)?\b|'
  r'(?:out\s*of|in|at)\s*the\s*money(?:\s*only)?',' ',t)
 if re.search(r'next\s*(?:week\'?s?\s*)?expiry|next\s*expir',t):found['expiry']='next'
 elif re.search(r'(?:both|all)\s*expir',t):found['expiry']='both'
 t=re.sub(r'(?:next|nearest|current|this\s*week\'?s?|both|all)\s*expir\w*',' ',t)
 if re.search(r'\bliquid\b',t):found['liquid_only']=True;t=re.sub(r'\bliquid\b',' ',t)
 t=re.sub(r'f\s*&\s*o\s*stocks?|fno\s*stocks?|stock\s*options|\bindices\b|\bindexes\b',' ',t)
 return found,notes,t


def _window(clause):
 """(window, text-without-it). Windows are always whole 15-min readings; anything else is rounded and said."""
 notes=[]
 m=re.search(rf'(?:last|past|previous)?\s*{NUM}\s*(?:consecutive\s*|concurrent\s*)?15[\s-]*min(?:ute)?s?\s*(?:cycles?|readings?|candles?|bars?|intervals?)?',clause)
 if m:
  n=_num(m.group(1))
  return {'kind':'readings','value':max(1,min(n,V.MAX_STEPS))},notes,clause[:m.start()]+' '+clause[m.end():]
 m=re.search(rf'(?:last|past|previous)?\s*{NUM}\s*(?:readings?|candles?|bars?|cycles?|intervals?)\b',clause)
 if m:
  n=_num(m.group(1))
  return {'kind':'readings','value':max(1,min(n,V.MAX_STEPS))},notes,clause[:m.start()]+' '+clause[m.end():]
 m=re.search(r'(?:between|from)\s*(\d{1,2}[:.]\d{2})\s*(?:and|to|-|–)\s*(\d{1,2}[:.]\d{2})',clause)
 if m:
  a,b=m.group(1).replace('.',':').zfill(5),m.group(2).replace('.',':').zfill(5)
  return {'kind':'custom','from':a,'to':b},notes,clause[:m.start()]+' '+clause[m.end():]
 m=re.search(rf'(?:last|past|previous|for)?\s*{NUM}\s*(?:minutes?|mins?|m)\b',clause)
 if m:
  n=_num(m.group(1));value=max(15,min(V.MAX_STEPS*15,round(n/15)*15))
  if value!=n:notes.append(f'{n} min rounded to {value} min — KANIDA reads a new snapshot every 15 minutes.')
  return {'kind':'minutes','value':value},notes,clause[:m.start()]+' '+clause[m.end():]
 m=re.search(r'(?:last|past|previous)?\s*(?:an?\s*|one\s*|1\s*)hour|(?:last|past)\s*hour',clause)
 if m:return {'kind':'minutes','value':60},notes,clause[:m.start()]+' '+clause[m.end():]
 m=re.search(r'(?:last|past)?\s*half\s*(?:an\s*)?hour',clause)
 if m:return {'kind':'minutes','value':30},notes,clause[:m.start()]+' '+clause[m.end():]
 m=re.search(r'since\s*(?:the\s*)?(?:market\s*)?open(?:ing)?|since\s*morning|through\s*the\s*(?:day|session)|all\s*day|\btoday\b|intraday',clause)
 if m:return {'kind':'open'},notes,clause[:m.start()]+' '+clause[m.end():]
 m=re.search(r'(?:the\s*)?(?:last|latest)\s*reading|right\s*now',clause)
 if m:return {'kind':'minutes','value':15},notes,clause[:m.start()]+' '+clause[m.end():]
 return None,notes,clause


def _first(patterns,text):
 best=None
 for pat,val in patterns:
  m=re.search(pat,text)
  if m and (best is None or (m.start(),-(m.end()-m.start()))<(best[1].start(),-(best[1].end()-best[1].start()))):
   best=(val,m)
 return best


def _ordered(patterns,clause):
 """The first pattern IN LIST ORDER that matches: the lists are ordered by specificity, not by position, so
 "grown 100%" reads as rapid and "OI spreading across strikes" as the strike-spread metric."""
 for pat,val in patterns:
  m=re.search(pat,clause)
  if m:return val,m
 return None


def _metric(clause):return _ordered(METRIC_PHRASES,clause)


def _fit_state(metric,state,cont,notes):
 """Map a parsed direction word onto the states this metric offers, saying so whenever it changed."""
 m=V.METRICS[metric]
 allowed=m['states']
 if state in ('up','down') and cont:state=state+'_cont'
 if state in allowed:return state
 fallbacks={'maxpain':{'up_rapid':'up','down_rapid':'down'},'futures_oi':{'up_rapid':'up_cont','down_rapid':'down_cont',
  'rev_up':'up','rev_down':'down'},'delta':{'up_cont':'up','down_cont':'down','up_rapid':'up','down_rapid':'down'},
  'gamma':{'up_cont':'up','down_cont':'down','up_rapid':'up','down_rapid':'down'},
  'volume':{'up':'unusual','up_cont':'unusual','up_rapid':'unusual','down':'quiet','down_cont':'quiet','stable':'quiet'},
  'spread':{'up':'broadening','up_cont':'broadening','down':'concentrating','down_cont':'concentrating'}}
 new=fallbacks.get(metric,{}).get(state)
 if new in allowed:
  notes.append(f'{m["label"]} has no "{V.STATE_WORDS.get(state,state)}"; read as "{V.state_word(metric,new)}".')
  return new
 notes.append(f'{m["label"]}: "{V.STATE_WORDS.get(state,state)}" is not offered; used "{V.state_word(metric,m["default_state"])}".')
 return m['default_state']


def parse(text,underlyings=()):
 """{definition, reads_as, understood, assumptions, unmapped} — or DefinitionError with the reason."""
 raw=' '+str(text or '').strip().replace('’',"'")+' '
 if len(raw)>600:raise D.DefinitionError('Keep the description under 600 characters.')
 scope,notes,rest=_scope(raw,[u for u in underlyings])
 parts=re.split(r'\s*(,|;|\band\b|\bor\b|\bbut\b|\bwhile\b|\bwhereas\b|\balso\b|\bplus\b)\s*',rest)
 clauses,join=[],None
 for p in parts:
  if p in (',',';','and','but','while','whereas','also','plus'):join=join or 'and';continue
  if p=='or':join='or';continue
  if p.strip():clauses.append((join,p.strip()));join=None
 conditions,understood,unmapped=[],[],[]
 prev=None
 for join,clause in clauses:
  window,wnotes,c2=_window(clause)
  notes.extend(wnotes)
  side_hit=_first(SIDES,c2)
  side=side_hit[0] if side_hit else None
  met=_metric(c2)
  st=_ordered(STATE_PHRASES,c2)
  cont=bool(re.search(CONTINUOUS,c2))
  if not met and not st:
   words=[w for w in re.findall(r"[a-z%']+",c2) if w not in STOP and len(w)>2]
   if words:unmapped.append(' '.join(words))
   continue
  if met:metric,flow_state=(met[0].split(':')+[None])[:2]
  elif prev:
   metric,flow_state=prev['metric'],None
   notes.append(f'"{clause}" has no metric of its own; read as {V.METRICS[metric]["label"]}, like the condition before it.')
  else:
   unmapped.append(clause);continue
  m=V.METRICS[metric]
  if metric=='flow':
   state=flow_state or (st[0] if st and st[0] in V.FLOW_STATES else m['default_state'])
  elif st:
   state=_fit_state(metric,st[0],cont,notes)
   if st[0]=='up_rapid' and re.search(r'\d+\s*%',c2):
    notes.append('A percentage is not a threshold here: KANIDA read it as "Expanding rapidly" (rising at every reading '
     'of the window, and speeding up).')
  else:
   state='up_cont' if cont and 'up_cont' in m['states'] else m['default_state']
   notes.append(f'No state word for {m["label"]}; used "{V.state_word(metric,state)}".')
  sides=V.sides_for(metric)
  if sides:
   if side not in sides:
    if side is None and prev and prev.get('side') in sides and prev['side']!='either':
     side=prev['side'];notes.append(f'{m["label"]}: no side named; kept {V.SIDES[side].lower()}, like the condition before it.')
    else:side='either'
  else:side=None
  if window is None:
   if prev:window=prev['window']
   else:
    window={'kind':'open'} if metric=='maxpain' else {'kind':'minutes','value':15} if metric=='volume' else {'kind':'minutes','value':45}
    notes.append(f'No time window named; used {V.window_label(window)}.')
  cond={'metric':metric,'side':side,'state':state,'window':window}
  if conditions:cond['join']=join or 'and'
  conditions.append(cond)
  understood.append({'phrase':clause,'condition':len(conditions)-1})
  prev=cond
 if not conditions:
  raise D.DefinitionError('KANIDA could not find a metric and a behaviour in that. Try "call OI increasing '
   'continuously for 45 min" or "max pain shifting higher since open".')
 d=D.normalize({**scope,'conditions':conditions[:V.MAX_CONDITIONS]})
 if len(conditions)>V.MAX_CONDITIONS:notes.append(f'Only the first {V.MAX_CONDITIONS} conditions were kept.')
 return {'definition':d,'reads_as':D.reads_as(d),'understood':understood,'assumptions':notes,'unmapped':unmapped}
