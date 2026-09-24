"""One trading session of 15-minute readings, loaded READ-ONLY from db/derivatives.db.

The store is opened `mode=ro`: the capture and metrics workers own every write, and the screener never takes
a write lock on it. Readings are appended as they land (`refresh`), so a session is read from disk once and
each new 15-minute reading costs one indexed query.

POINT IN TIME. Every array is indexed by reading, oldest first. A state at reading `i` is only ever computed
from indexes <= i, so a replay of the session gives exactly what a live run at each reading would have said.
A contract with no row at a reading holds NaN there — never the previous value carried forward.
"""
from __future__ import annotations
import math,sqlite3,threading
from array import array
from pathlib import Path
from . import vocab as V

NAN=float('nan')
#: The last trading mark of a session. The 15:45 post-close mark is not an intraday reading.
LAST_MARK='15:30'
CONTRACT_COLUMNS=('instrument_token','tradingsymbol','underlying','instrument_type','strike','expiry','lot_size',
 'days_to_expiry','last_price','oi','oi_change_day','volume','spot','unusual','floors_passed','vol_tod_ratio')
BOOK_COLUMNS=('underlying','expiry','days_to_expiry','spot','pcr_oi','max_pain_strike','total_ce_oi','total_pe_oi')
SERIES_FIELDS=('price','oi','oid','spot','vtr')


def isnan(v):return v is None or (isinstance(v,float) and math.isnan(v))


def val(v):return None if isnan(v) else v


class Contract:
 __slots__=('token','symbol','underlying','type','strike','expiry','lot','dte','price','oi','oid','spot','vtr',
  'unusual','floors')
 def __init__(self,token,symbol,underlying,kind,strike,expiry,lot,dte):
  self.token,self.symbol,self.underlying,self.type=token,symbol,underlying,kind
  self.strike,self.expiry,self.lot,self.dte=strike,expiry,lot,dte
  for f in SERIES_FIELDS:setattr(self,f,array('d'))
  self.unusual=array('b');self.floors=array('b')
 def pad(self,n):
  for f in SERIES_FIELDS:
   a=getattr(self,f)
   while len(a)<n:a.append(NAN)
  while len(self.unusual)<n:self.unusual.append(-1)
  while len(self.floors)<n:self.floors.append(-1)


class Book:
 """One underlying + expiry: the book-level series."""
 __slots__=('underlying','expiry','dte','spot','pcr','maxpain','ce_oi','pe_oi','tokens','ladder')
 def __init__(self,underlying,expiry):
  self.underlying,self.expiry,self.dte=underlying,expiry,None
  self.spot,self.pcr,self.maxpain,self.ce_oi,self.pe_oi=array('d'),array('d'),array('d'),array('d'),array('d')
  self.tokens=set();self.ladder=[]
 def pad(self,n):
  for a in (self.spot,self.pcr,self.maxpain,self.ce_oi,self.pe_oi):
   while len(a)<n:a.append(NAN)


class Store:
 """The read-only connection. One per process; sqlite3 connections are not shared across threads."""
 def __init__(self,path):
  self.path=str(path);self._local=threading.local()
 def available(self):return Path(self.path).is_file()
 def conn(self):
  c=getattr(self._local,'c',None)
  if c is None:
   c=sqlite3.connect(Path(self.path).resolve().as_uri()+'?mode=ro',uri=True,check_same_thread=False,timeout=10)
   self._local.c=c
  return c
 def rows(self,sql,args=()):return self.conn().execute(sql,args).fetchall()
 def latest_session(self):
  r=self.rows("select max(captured_at) from metrics where scope='underlying'")
  return r[0][0][:10] if r and r[0][0] else None
 def readings(self,session):
  return [r[0] for r in self.rows("select distinct captured_at from metrics where scope='underlying'"
   " and captured_at>=? and captured_at<=? order by 1",(f'{session} 09:00',f'{session} {LAST_MARK}:59'))]


class Session:
 """Every reading of one session loaded so far, and the arrays the evaluator reads."""
 def __init__(self,store,session):
  self.store,self.session=store,session
  self.readings=[];self.contracts={};self.books={};self.futures={};self.lock=threading.Lock()

 @property
 def n(self):return len(self.readings)

 def refresh(self):
  """Append any reading the store has that this session has not loaded. Returns how many were added."""
  with self.lock:
   stored=self.store.readings(self.session)
   new=[r for r in stored if not self.readings or r>self.readings[-1]]
   for at in new:self._load(at)
   return len(new)

 def _load(self,at):
  i=len(self.readings)
  self.readings.append(at)
  cols=','.join(CONTRACT_COLUMNS)
  for r in self.store.rows(f"select {cols} from metrics where captured_at=? and scope='contract'",(at,)):
   (token,symbol,und,kind,strike,expiry,lot,dte,price,oi,oid,vol,spot,unusual,floors,vtr)=r
   if token is None:continue
   c=self.contracts.get(token)
   if c is None:
    c=self.contracts[token]=Contract(token,symbol,und,kind,strike,expiry,lot,dte)
   c.pad(i)
   c.price.append(NAN if price is None else float(price))
   c.oi.append(NAN if oi is None else float(oi))
   c.oid.append(NAN if oid is None else float(oid))
   c.spot.append(NAN if spot is None else float(spot))
   c.vtr.append(NAN if vtr is None else float(vtr))
   c.unusual.append(-1 if unusual is None else int(bool(unusual)))
   c.floors.append(-1 if floors is None else int(bool(floors)))
   if kind=='FUT':
    front=self.futures.get(und)
    if front is None or expiry<self.contracts[front].expiry:self.futures[und]=token
   elif strike is not None:
    b=self._book(und,expiry)
    if token not in b.tokens:
     b.tokens.add(token)
     if strike not in b.ladder:b.ladder.append(strike);b.ladder.sort()
  cols=','.join(BOOK_COLUMNS)
  for r in self.store.rows(f"select {cols} from metrics where captured_at=? and scope='underlying'",(at,)):
   und,expiry,dte,spot,pcr,mp,ce,pe=r
   if not und or not expiry:continue
   b=self._book(und,expiry);b.pad(i);b.dte=dte
   for a,v in ((b.spot,spot),(b.pcr,pcr),(b.maxpain,mp),(b.ce_oi,ce),(b.pe_oi,pe)):a.append(NAN if v is None else float(v))
  n=i+1
  for c in self.contracts.values():c.pad(n)
  for b in self.books.values():b.pad(n)

 def _book(self,und,expiry):
  key=(und,expiry)
  b=self.books.get(key)
  if b is None:b=self.books[key]=Book(und,expiry)
  return b

 # --- the strike range -----------------------------------------------------------------------------------
 def spot_at(self,book,i):
  v=book.spot[i] if i<len(book.spot) else NAN
  if not isnan(v):return v
  for t in book.tokens:
   s=self.contracts[t].spot[i]
   if not isnan(s):return s
  return None

 def atm(self,book,i):
  """The grid's own rule (derivatives._atm_pair): the listed strike nearest spot; a tie goes to the higher."""
  spot=self.spot_at(book,i)
  if spot is None or not book.ladder:return None
  return min(book.ladder,key=lambda s:(abs(s-spot),-s))

 def band(self,book,i,strikes):
  """{token: rung offset from ATM} for the contracts inside the strike range at reading i. Delta bands are
  applied by the evaluator (they need the COMPUTED delta); here they read as ATM ±MAX_RUNGS."""
  atm=self.atm(book,i)
  if atm is None:return {}
  pos={s:j for j,s in enumerate(book.ladder)}
  a=pos[atm]
  kind=strikes['kind']
  out={}
  for t in book.tokens:
   c=self.contracts[t]
   if isnan(c.price[i]) and isnan(c.oi[i]):continue  # no row at this reading
   off=pos[c.strike]-a
   if kind=='atm':ok=-strikes['below']<=off<=strikes['above']
   elif kind=='otm':ok=(1<=off<=strikes['depth']) if c.type=='CE' else (-strikes['depth']<=off<=-1)
   elif kind=='itm':ok=(-strikes['depth']<=off<=-1) if c.type=='CE' else (1<=off<=strikes['depth'])
   else:ok=abs(off)<=V.MAX_RUNGS
   if ok:out[t]=off
  return out

 def expiries(self,und):
  return sorted(e for (u,e) in self.books if u==und)

 def underlyings(self):
  return sorted({u for (u,_e) in self.books})


class Sessions:
 """A small cache of loaded sessions, refreshed on read. Today's session is the one that grows."""
 def __init__(self,store,keep=2):
  self.store,self.keep,self.cache,self.lock=store,keep,{},threading.Lock()
 def get(self,session=None):
  session=session or self.store.latest_session()
  if not session:return None
  with self.lock:
   s=self.cache.get(session)
   if s is None:
    s=self.cache[session]=Session(self.store,session)
    for old in sorted(self.cache)[:-self.keep]:self.cache.pop(old,None)
  s.refresh()
  return s
