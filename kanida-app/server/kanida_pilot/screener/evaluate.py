"""Evaluate one scanner definition over one session, reading by reading, POINT IN TIME.

At reading i every condition is read from values captured at or before i and nothing later, so the whole
session's replay is what a live run at each reading would have returned — which is what makes "first matched
10:15" and the lifecycle honest rather than hindsight.

GRAIN (definition.grain). A 'contract' scanner returns option contracts (NIFTY 29 Sep 24,500 CE); a 'book'
scanner returns an underlying + expiry, when its conditions are about the book (PCR, max pain) or span both
sides (call OI building AND put OI unwinding — a statement no single contract can satisfy).

THE STRIKE RANGE is re-read at every reading from that reading's spot (the grid's ATM rule). A contract that
drifts out of the range as spot moves stops matching then, and its card says so.

The result names what matched and why; it never says buy, sell, good, bad or best.
"""
from __future__ import annotations
from . import definition as D,lifecycle as L,states as S,vocab as V
from .data import isnan

VERB={'default':('rose','fell','higher','lower'),'iv':('expanded','cooled','higher','lower'),
 'maxpain':('shifted higher','shifted lower','higher','lower'),'futures_oi':('built','unwound','higher','lower'),
 'underlying':('moved higher','moved lower','higher','lower')}
FAMILY={'premium':'grid_price','oi':'grid_oi','iv':'session','delta':'session','gamma':'session','pcr':'session',
 'maxpain':'session','underlying':'session','futures_oi':'session'}


def hhmm(at):return str(at or '')[11:16]


def units(v):
 """logic.ts deltaUnits: +2.4L, −80K, +1.2Cr."""
 if v is None:return '—'
 a=abs(v);sign='+' if v>0 else '−' if v<0 else ''
 def fit(x):
  t=f'{x:.0f}' if x>=100 else f'{x:.1f}'
  return t[:-2] if t.endswith('.0') else t
 if a>=1e7:return f'{sign}{fit(a/1e7)}Cr'
 if a>=1e5:return f'{sign}{fit(a/1e5)}L'
 if a>=1e3:return f'{sign}{fit(a/1e3)}K'
 return f'{sign}{round(a)}'


def money(v,places=2):return '—' if v is None else f'₹{v:,.{places}f}'


def fmt(metric,v):
 if v is None:return '—'
 if metric=='premium':return money(v)
 if metric=='underlying':return money(v,1)
 if metric=='iv':return f'{v:.1f}%'
 if metric=='delta':return f'{v:.2f}'
 if metric=='gamma':return f'{v:.4f}'
 if metric=='pcr':return f'{v:.2f}'
 if metric=='maxpain':return f'{v:,.0f}'
 if metric in ('futures_oi','oi'):return units(v)
 return f'{v:g}'


def strike_text(v):return '—' if v is None else (f'{v:,.0f}' if float(v).is_integer() else f'{v:,.1f}')


class Run:
 """Everything one evaluation needs, memoised for its own lifetime."""
 def __init__(self,session,greeks,d):
  self.s,self.g,self.d=session,greeks,d
  self.n=session.n
  self.series={}
  self.notes=[]

 # --- series ----------------------------------------------------------------------------------------------
 def contract_series(self,token,metric):
  key=(token,metric)
  hit=self.series.get(key)
  if hit is None:
   c=self.s.contracts[token]
   if metric=='premium':values=c.price
   elif metric in ('oi','flow_oi'):values=c.oid
   elif metric in ('iv','delta','gamma'):values=self.g.series(self.s.session,token,self.n,metric)
   else:raise KeyError(metric)
   hit=self.series[key]=S.Series(values)
  return hit

 def book_series(self,book,metric):
  key=((book.underlying,book.expiry),metric)
  hit=self.series.get(key)
  if hit is None:
   if metric=='pcr':values=book.pcr
   elif metric=='maxpain':values=book.maxpain
   elif metric=='underlying':values=[self.s.spot_at(book,i) for i in range(self.n)]
   elif metric=='futures_oi':
    t=self.s.futures.get(book.underlying)
    values=self.s.contracts[t].oi if t else []
   else:raise KeyError(metric)
   hit=self.series[key]=S.Series(list(values))
  return hit

 # --- windows ---------------------------------------------------------------------------------------------
 def window_k(self,w,series,i):
  """(k, j): intervals to read over and the captured index to read at, or (None, None) when the window
  cannot be read at reading i. A custom window FREEZES at its end reading: what it said at 11:30 is what it
  says after 11:30."""
  kind=w['kind']
  if kind=='custom':
   times=[hhmm(a) for a in self.s.readings]
   if hhmm(self.s.readings[i])<w['from']:return None,None
   end=max((x for x in range(i+1) if times[x]<=w['to']),default=None)
   start=next((x for x in range(self.n) if times[x]>=w['from']),None)
   if end is None or start is None:return None,None
   j,js=series.index(end),series.index(start)
   if j is None or js is None or j<=js:return None,None
   return j-js,j
  j=series.index(i)
  if j is None:return None,None
  if kind=='open':return (j if j>=1 else None),j
  return V.window_steps(w),j

 # --- one condition on one contract -------------------------------------------------------------------------
 def contract_condition(self,c,token,i):
  contract=self.s.contracts[token]
  if c['side'] in ('CE','PE') and contract.type!=c['side']:return False,{'why':'side'}
  metric,state=c['metric'],c['state']
  if metric=='volume':
   k=V.window_steps(c['window']) or 1
   lo=i-k+1
   if lo<0:return None,{'k':k}
   if state=='unusual':
    flags=[contract.unusual[x] for x in range(lo,i+1)]
    if -1 in flags:return None,{'k':k}
    vtr=contract.vtr[i]
    return all(f==1 for f in flags),{'k':k,'vtr':None if isnan(vtr) else vtr}
   p,o=self.contract_series(token,'premium'),self.contract_series(token,'oi')
   jp,jo=p.index(i),o.index(i)
   if jp is None or jo is None or jp<k or jo<k:return None,{'k':k}
   quiet=all(p.direction('grid_price',jp-b,1)=='flat' and o.direction('grid_oi',jo-b,1)=='flat' for b in range(k))
   return quiet,{'k':k}
  if metric=='flow':
   p,o=self.contract_series(token,'premium'),self.contract_series(token,'oi')
   k,jp=self.window_k(c['window'],p,i)
   jo=o.index(i) if c['window']['kind']!='custom' else o.index(p.at[jp]) if jp is not None else None
   if k is None or jo is None or jp<k or jo<k:return None,{}
   pd,od=p.direction('grid_price',jp,k),o.direction('grid_oi',jo,k)
   behaviour=S.FLOW.get((contract.type,pd,od),'none')
   return behaviour==state,{'k':k,'price_direction':pd,'oi_direction':od,'behaviour':behaviour,
    'start_at':p.at[jp-k],'price_start':p.v[jp-k],'price_end':p.v[jp],'oi_change':o.v[jo]-o.v[jo-k],
    'pace':o.pace(jo)}
  series=self.contract_series(token,metric)
  k,j=self.window_k(c['window'],series,i)
  if k is None:return None,{}
  return series.read(FAMILY[metric],j,k,state)

 # --- one condition on one book ---------------------------------------------------------------------------
 def building(self,book,band,i,side):
  """The strikes on `side` inside the range at reading i whose OI rose over the interval ending at i (the
  grid rule, lookback 1), and the one that added the most. None when no strike carried a comparable value."""
  strikes,lead,best,measured=[],None,None,0
  for t in band:
   c=self.s.contracts[t]
   if c.type!=side:continue
   o=self.contract_series(t,'oi')
   j=o.index(i)
   if j is None or j<1:continue
   measured+=1
   if o.direction('grid_oi',j,1)=='up':
    strikes.append(c.strike)
    add=o.v[j]-o.v[j-1]
    if best is None or add>best:best,lead=add,c.strike
  if not measured:return None
  return sorted(strikes),lead

 def spread_condition(self,c,book,bands,i):
  kind=c['window']['kind']
  k=V.window_steps(c['window']) if kind in ('minutes','readings') else (i if kind=='open' else 1)
  if i-k<0 or i<1:return None,{}
  sides=['CE','PE'] if c['side'] in ('either','both') else [c['side']]
  results={}
  for side in sides:
   now,prev,base=(self.building(book,bands[x],x,side) for x in (i,i-1,i-k))
   if now is None or prev is None or base is None:results[side]=(None,{});continue
   joined=[s for s in now[0] if s not in prev[0]];left=[s for s in prev[0] if s not in now[0]]
   state=c['state']
   if state=='broadening':ok=bool(joined) and not left and len(now[0])>len(base[0]) and len(now[0])>=2
   elif state=='concentrating':ok=bool(left) and not joined and len(now[0])<len(base[0])
   elif state=='shift_up':ok=now[1] is not None and base[1] is not None and now[1]>base[1]
   else:ok=now[1] is not None and base[1] is not None and now[1]<base[1]
   results[side]=(ok,{'side':side,'k':k,'strikes':now[0],'base_strikes':base[0],'lead':now[1],'base_lead':base[1],
    'breadth':S.breadth_word(now[0],book.ladder),'start_at':i-k})
  vals=[r[0] for r in results.values()]
  if c['side']=='both':holds=None if None in vals else all(vals)
  else:holds=True if True in vals else (None if None in vals else False)
  hit=[r[1] for r in results.values() if r[0]] or [r[1] for r in results.values() if r[1]]
  return holds,(hit[0] if hit else {})

 def book_condition(self,c,book,bands,i):
  metric=c['metric']
  if metric=='spread':return self.spread_condition(c,book,bands,i)
  if V.METRICS[metric]['grain']=='contract':
   # a contract condition read at the book: it holds when ANY contract in the range meets it
   hits,unknown=[],False
   for t in bands[i]:
    holds,info=self.contract_condition(c,t,i)
    if holds:hits.append((t,info))
    elif holds is None and not (c['side'] in ('CE','PE') and self.s.contracts[t].type!=c['side']):unknown=True
   if hits:
    hits.sort(key=lambda h:-abs((h[1] or {}).get('change') or (h[1] or {}).get('oi_change') or 0))
    return True,{'contracts':[h[0] for h in hits],'lead':hits[0]}
   if not bands[i]:return None,{}
   return (None if unknown else False),{}
  series=self.book_series(book,metric)
  k,j=self.window_k(c['window'],series,i)
  if k is None:return None,{}
  return series.read(FAMILY[metric],j,k,c['state'])


def _combine(values):
 """AND over a group: False wins, then None (not observed), then True."""
 if any(v is False for v in values):return False
 if any(v is None for v in values):return None
 return True


def _any(values):
 if any(v is True for v in values):return True
 if any(v is None for v in values):return None
 return False


class Evaluator:
 def __init__(self,sessions,greeks):
  self.sessions,self.greeks=sessions,greeks

 def books(self,s,d):
  """The (book, reason) list the definition reads, with the expiry-day roll applied (owner decision Q6:
  on the nearest expiry's own expiry day, a scanner with an IV / delta / gamma condition reads the NEXT expiry,
  because implied volatility cannot be solved on expiry day)."""
  u=d['universe']
  names=s.underlyings()
  if u['kind']=='indices':names=[x for x in names if x in V.INDEX_UNDERLYINGS]
  elif u['kind']=='stocks':names=[x for x in names if x not in V.INDEX_UNDERLYINGS]
  elif u['kind']=='symbols':names=[x for x in names if x in set(u['symbols'])]
  computed=D.uses_computed(d)
  out,rolled=[],[]
  for name in names:
   ex=s.expiries(name)
   if not ex:continue
   if computed and ex[0]<=s.session and len(ex)>1:
    rolled.append(name);ex=ex[1:]
   chosen={'nearest':ex[:1],'next':ex[1:2],'both':ex[:2]}[d['expiry']]
   out.extend(s.books[(name,e)] for e in chosen)
  notes=[]
  if rolled:
   notes.append({'kind':'expiry_roll','text':(f'Expiry day for {", ".join(rolled[:6])}'
    f'{" and others" if len(rolled)>6 else ""}: implied volatility cannot be solved on the day an option expires, '
    'so IV, delta and gamma conditions read the next expiry.'),'underlyings':rolled})
  return out,notes

 def run(self,d,session=None):
  s=self.sessions.get(session)
  if s is None or not s.n:
   return {'available':False,'session':session,'readings':[],'matches':[],'notes':[],'grain':D.grain(d),
    'text':'No F&O readings are stored yet — the first 15-min reading of the session lands at 09:30.'}
  run=Run(s,self.greeks,d)
  books,notes=self.books(s,d)
  grain=D.grain(d)
  groups=D.groups(d)
  conds=d['conditions']
  contract_conds=[c for c in conds if V.METRICS[c['metric']]['grain']=='contract']
  sides={c['side'] for c in contract_conds}
  wanted_types={'CE','PE'} if (not sides or 'either' in sides) else sides
  n=s.n
  # the strike range at every reading, per book
  bands={}
  for b in books:
   per=[]
   for i in range(n):
    band=s.band(b,i,d['strikes'])
    if d.get('liquid_only'):band={t:o for t,o in band.items() if s.contracts[t].floors[i]==1}
    per.append(band)
   bands[(b.underlying,b.expiry)]=per
  # computed figures, solved once for every contract they could be read on
  if D.uses_computed(d):
   wanted={}
   for key,per in bands.items():
    for i,band in enumerate(per):
     for t in band:
      if s.contracts[t].type in wanted_types:wanted.setdefault(i,set()).add(t)
   self.greeks.ensure(s,wanted)
   if d['strikes']['kind']=='delta':
    lo,hi=V.DELTA_BANDS[d['strikes']['band']]['lo'],V.DELTA_BANDS[d['strikes']['band']]['hi']
    for key,per in bands.items():
     for i,band in enumerate(per):
      keep={}
      for t,o in band.items():
       r=self.greeks.value(s.session,t,i)
       if r and r[2] is not None and lo<=abs(r[2])<=hi:keep[t]=o
      per[i]=keep
   notes.append({'kind':'computed','text':('IV, delta and gamma are COMPUTED here with a Black-Scholes-Merton '
    'model from each option\'s own traded price — they are not exchange figures.')})
  # book-level conditions, once per book per reading
  book_conds=[c for c in conds if V.METRICS[c['metric']]['grain']!='contract' or grain=='book']
  bookv={}
  for b in books:
   key=(b.underlying,b.expiry)
   for c in book_conds:
    bookv[(key,c['id'])]=[run.book_condition(c,b,[set(x) for x in bands[key]],i) for i in range(n)]
  entities={}
  if grain=='book':
   for b in books:
    key=(b.underlying,b.expiry)
    matched,pace=[],[]
    for i in range(n):
     per_group=[_combine([bookv[(key,c['id'])][i][0] for c in g]) for g in groups]
     m=_any(per_group)
     matched.append(m)
     pace.append(self._pace(run,groups,per_group,bookv,key,None,i) if m else None)
    if any(matched):entities[key]=(matched,pace,b,None)
  else:
   for b in books:
    key=(b.underlying,b.expiry)
    per=bands[key]
    tokens=set()
    for band in per:tokens.update(t for t in band if s.contracts[t].type in wanted_types)
    for t in tokens:
     matched,pace=[],[]
     for i in range(n):
      if t not in per[i]:
       c=s.contracts[t]
       # no row at this reading: NOT OBSERVED (a gap). A row outside the range: it left the range.
       absent=isnan(c.price[i]) and isnan(c.oi[i])
       matched.append(None if absent or not any(t in per[x] for x in range(i)) else False);pace.append(None);continue
      per_group=[]
      for g in groups:
       vals=[]
       for c in g:
        if V.METRICS[c['metric']]['grain']=='contract':vals.append(run.contract_condition(c,t,i)[0])
        else:vals.append(bookv[(key,c['id'])][i][0])
       per_group.append(_combine(vals))
      m=_any(per_group)
      matched.append(m)
      pace.append(self._pace(run,groups,per_group,bookv,key,t,i) if m else None)
     if any(matched):entities[(key,t)]=(matched,pace,b,t)
  cards=[self._card(run,d,grain,groups,bookv,bands,ekey,*val) for ekey,val in entities.items()]
  order={'new':0,'strengthening':1,'still':2,'weakening':3,'ended':4}
  cards.sort(key=lambda c:(order.get(c['status'],9),-(c['status_index'] or 0),c['title']))
  active=sum(1 for c in cards if c['active'])
  return {'available':True,'session':s.session,'as_of':s.readings[-1],'readings':[hhmm(a) for a in s.readings],
   'grain':grain,'notes':notes,'matches':cards,'counts':{'active':active,'ended':len(cards)-active,'all':len(cards)},
   'books':len(books)}

 # --- pace, for strengthening / weakening -----------------------------------------------------------------
 def _pace(self,run,groups,per_group,bookv,key,token,i):
  """The lead condition — the first condition of the first group that holds — against its own previous move."""
  g=next((g for g,v in zip(groups,per_group) if v),None)
  if not g:return None
  c=g[0]
  if c['metric'] in ('volume','spread'):return None
  if token is not None and V.METRICS[c['metric']]['grain']=='contract':
   holds,info=run.contract_condition(c,token,i)
  else:
   holds,info=bookv[(key,c['id'])][i]
   if info and 'lead' in info and isinstance(info['lead'],tuple):info=info['lead'][1]
  if not holds:return None
  return S.pace_word((info or {}).get('pace'))

 # --- the card --------------------------------------------------------------------------------------------
 def _card(self,run,d,grain,groups,bookv,bands,ekey,matched,pace,book,token):
  s=run.s
  timeline,episodes,events=L.walk(matched,pace)
  status,at=L.current(timeline)
  ep=episodes[-1]
  active=ep['end'] is None
  key=(book.underlying,book.expiry)
  explain_at=ep['last']
  because=self._because(run,groups,bookv,bands,key,token,explain_at)
  ended=None
  if not active:ended=self._ended(run,d,groups,bookv,bands,key,token,ep['end'])
  if token is not None:
   c=s.contracts[token]
   title=f"{book.underlying} · {strike_text(c.strike)} {c.type}"
   ident={'underlying':book.underlying,'expiry':book.expiry,'strike':c.strike,'type':c.type,'symbol':c.symbol,'token':token}
   thin=c.floors[explain_at]==0
  else:
   title=book.underlying
   ident={'underlying':book.underlying,'expiry':book.expiry}
   thin=False
  reads=[x for x in timeline if x in ('new','still','strengthening','weakening')]
  first=episodes[0]['start']
  return {'key':f"{book.underlying}|{book.expiry}|{token or ''}",'title':title,**ident,
   'status':status,'status_index':at,'status_at':hhmm(s.readings[at]) if at is not None else None,
   'active':active,'first_matched':hhmm(s.readings[first]),
   'episode_started':hhmm(s.readings[ep['start']]),'last_matched':hhmm(s.readings[ep['last']]),
   'ended_at':None if active else hhmm(s.readings[ep['end']]),
   'readings_matched':len(reads),'episode_readings':ep['readings'],
   'duration_min':(ep['last']-ep['start'])*V.READING_MINUTES,
   'episodes':[{'start':hhmm(s.readings[e['start']]),'end':None if e['end'] is None else hhmm(s.readings[e['end']]),
    'readings':e['readings']} for e in episodes],
   'timeline':timeline,'events':[{'at':hhmm(s.readings[i]),'kind':k,'status':timeline[i]} for i,k in events],
   'because':because,'ended_because':ended,'thin':thin,
   'computed':any(c['metric'] in V.COMPUTED_METRICS for c in d['conditions'])}

 def _cond_info(self,run,c,bookv,bands,key,token,i):
  if token is not None and V.METRICS[c['metric']]['grain']=='contract':return run.contract_condition(c,token,i)
  return bookv[(key,c['id'])][i]

 def _because(self,run,groups,bookv,bands,key,token,i):
  lines=[]
  for g in groups:
   got=[(c,*self._cond_info(run,c,bookv,bands,key,token,i)) for c in g]
   if all(h for _c,h,_i in got):
    lines=[self._sentence(run,c,info,key,token,i) for c,_h,info in got]
    break
  ctx=self._context(run,bookv,bands,key,token,i,groups)
  if ctx:lines.append(ctx)
  return [x for x in lines if x]

 def _ended(self,run,d,groups,bookv,bands,key,token,i):
  s=run.s
  when=hhmm(s.readings[i])
  if token is not None and token not in bands[key][i]:
   book=s.books[key];atm=s.atm(book,i)
   return f"It left the strike range at the {when} reading (ATM moved to {strike_text(atm)})."
  for c in groups[0]:
   holds,info=self._cond_info(run,c,bookv,bands,key,token,i)
   if holds is False:
    subject=self._subject(run,c,token)
    word=V.state_word(c['metric'],c['state']).lower()
    d_=(info or {}).get('direction')
    how={'flat':' — it was flat','up':' — it rose','down':' — it fell'}.get(d_,'')
    steps=(info or {}).get('step_directions') or []
    if c['state'].endswith('_cont') and steps:
     # "continuously" broke on an interval, not on the window: say which interval, and what it did
     want='up' if c['state'].startswith('up') else 'down'
     k=len(steps)
     for back,sd in enumerate(reversed(steps)):
      if sd!=want:
       idx=i-back
       did={'flat':'was flat','up':'rose','down':'fell'}.get(sd,'was not measured')
       how=f" — in the 15 minutes to {hhmm(s.readings[idx])} it {did} (of the last {k} intervals)"
       break
    if c['metric']=='flow' and info:how=f" — premium {info.get('price_direction')}, OI {info.get('oi_direction')}"
    return f"{subject} was no longer {word} at the {when} reading{how}."
  return f"The conditions stopped holding at the {when} reading."

 def _subject(self,run,c,token):
  m=V.METRICS[c['metric']]
  side=c.get('side')
  if token is not None and m['grain']=='contract':side=run.s.contracts[token].type
  who={'CE':'Call ','PE':'Put '}.get(side or '','')
  noun=m['noun']
  if c['metric']=='underlying':return 'The underlying'
  if c['metric']=='maxpain':return 'Max pain'
  text=f"{who}{noun}"
  return text[0].upper()+text[1:]

 def _window_text(self,c,info):
  w=c['window']
  if w['kind']=='open':return 'since market open'
  if w['kind']=='custom':return f"between {w['from']} and {w['to']}"
  return f"over the last {V.window_label(w).replace('last ','')}"

 def _sentence(self,run,c,info,key,token,i):
  s=run.s
  info=info or {}
  metric,state=c['metric'],c['state']
  subject=self._subject(run,c,token)
  if 'lead' in info and isinstance(info.get('lead'),tuple):
   # a contract condition held at the book: say which contracts
   lead_token,lead_info=info['lead']
   lc=s.contracts[lead_token]
   more=len(info.get('contracts') or [])-1
   base=self._sentence(run,c,lead_info,key,lead_token,i)
   extra=f" (and {more} more strike{'s' if more!=1 else ''} in the range)" if more>0 else ''
   return f"{base.rstrip('.')} at {strike_text(lc.strike)} {lc.type}{extra}."
  if metric=='volume':
   if state=='unusual':
    k=info.get('k',1)
    vtr=info.get('vtr')
    tail=f" — volume {vtr:.1f}× its own time-of-day median" if vtr else ''
    when='at the latest reading' if k==1 else f'at each of the last {k} readings'
    return f"{subject} was unusually active {when}{tail}."
   return f"Premium and OI barely moved at each of the last {info.get('k',1)} readings."
  if metric=='flow':
   side=s.contracts[token].type if token is not None else c['side']
   label=S.FLOW_TEXT.get((side,state),V.STATE_WORDS[state])
   since=hhmm(s.readings[info['start_at']]) if info.get('start_at') is not None else ''
   return (f"{label} since {since}: premium {money(info.get('price_start'))} → {money(info.get('price_end'))}, "
    f"OI {units(info.get('oi_change'))}.")
  if metric=='spread':
   side={'CE':'call','PE':'put'}.get(info.get('side'),'')
   now,base=info.get('strikes') or [],info.get('base_strikes') or []
   since=hhmm(s.readings[info['start_at']]) if info.get('start_at') is not None else ''
   span=f"{strike_text(now[0])}–{strike_text(now[-1])}" if len(now)>1 else (strike_text(now[0]) if now else 'none')
   if state in ('broadening','concentrating'):
    verb='widened' if state=='broadening' else 'narrowed'
    return (f"The {side} strikes adding OI {verb} from {len(base)} to {len(now)} since {since} "
     f"({span}, {info.get('breadth')}).")
   return (f"The {side} strike adding the most OI moved from {strike_text(info.get('base_lead'))} to "
    f"{strike_text(info.get('lead'))} since {since}.")
  up,down,hi,lo=VERB.get(metric,VERB['default'])
  start_at=info.get('start_at')
  since=hhmm(s.readings[start_at]) if start_at is not None else ''
  start,end=info.get('start'),info.get('end')
  if metric=='oi':
   change=f"{units(info.get('change'))} OI since {since}"
  else:
   pct=''
   if metric=='premium' and start:pct=f" ({(end-start)/abs(start)*100:+.0f}%)"
   change=f"{fmt(metric,start)} → {fmt(metric,end)}{pct} since {since}"
  k=info.get('k',1)
  if state in ('up','down'):
   return f"{subject} {up if state=='up' else down} {self._window_text(c,info)}: {change}."
  if state in ('up_cont','down_cont'):
   return f"{subject} {up if state=='up_cont' else down} at each of the last {k} reading{'s' if k!=1 else ''}: {change}."
  if state in ('up_rapid','down_rapid'):
   p=info.get('pace')
   return (f"{subject} {up if state=='up_rapid' else down} at each reading and the latest move was "
    f"{p:.1f}× the one before: {change}.")
  if state=='stable':return f"{subject} held steady {self._window_text(c,info)}: {change}."
  if state in ('rev_up','rev_down'):
   turned=hi if state=='rev_up' else lo
   before=down if state=='rev_up' else up
   return f"{subject} turned {turned} at the latest reading after it {before} {self._window_text(c,info)}: {change}."
  return ''

 def _context(self,run,bookv,bands,key,token,i,groups):
  """Where the OI activity sits relative to ATM, for a contract card whose conditions are about positions."""
  if token is None:return ''
  if not any(c['metric'] in ('oi','flow') for g in groups for c in g):return ''
  s=run.s
  book=s.books[key]
  c=s.contracts[token]
  got=run.building(book,bands[key][i],i,c.type)
  if not got or not got[0]:return ''
  atm=s.atm(book,i)
  if atm is None:return ''
  pos={x:j for j,x in enumerate(book.ladder)}
  offs=sorted(pos[x]-pos[atm] for x in got[0])
  def rung(o):return 'ATM' if o==0 else f'ATM{o:+d}'
  side='call' if c.type=='CE' else 'put'
  where=rung(offs[0]) if len(offs)==1 else f"{rung(offs[0])} to {rung(offs[-1])}"
  return (f"OI is being added on {len(offs)} {side} strike{'s' if len(offs)!=1 else ''} in the range "
   f"({where}, {S.breadth_word(got[0],book.ladder)}).")
