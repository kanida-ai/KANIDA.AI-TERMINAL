"""The builder's market reads: underlyings, expiries and one option chain at ONE stored reading. READ-ONLY.

db/derivatives.db belongs to the capture workers (docs/DERIVATIVES_SPEC.md §2). It is opened mode=ro with
query_only, exactly like kanida_pilot/derivatives.py. Nothing here writes, and nothing here is a live quote: every
answer names the reading it came from (`as_of`, IST) and the fields that reading does not carry.

Facts about the store this module states rather than hides:
  * The capture writes last traded price, OI and volume. It does not capture bid/ask, so `bid`/`ask` are null and
    every price is flagged `basis: 'ltp'` with quality flag `no_bid_ask`.
  * Expiry is stored as the exchange-local date string (YYYY-MM-DD). It is served exactly as stored and never
    converted through a UTC timestamp (the date bug seen in two competitors).
  * The first release supports index options only; stock options wait for settlement/delivery handling.
"""
from __future__ import annotations
import sqlite3,threading
from collections import Counter
from datetime import datetime
from .analytics import parse_ist,years_between
from .. import implied_vol as IV

INDEX_UNDERLYINGS=('NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY','NIFTYNXT50','SENSEX','BANKEX')
STALE_TRADE_SECONDS=900


class MarketUnavailable(Exception):pass


class Market:
 def __init__(self,path):
  self.path=path;self.lock=threading.Lock();self._c=None;self._memo={}

 def _conn(self):
  if self._c is None:
   try:
    c=sqlite3.connect(f'file:{self.path}?mode=ro',uri=True,check_same_thread=False,timeout=10)
    c.execute('pragma query_only=1');c.row_factory=sqlite3.Row
    c.execute('select 1 from contracts limit 1')
   except sqlite3.Error as error:raise MarketUnavailable(f'The option store is not readable ({type(error).__name__}).')
   self._c=c
  return self._c

 def _q(self,sql,args=()):
  with self.lock:return self._conn().execute(sql,args).fetchall()

 def close(self):
  if self._c:self._c.close();self._c=None

 def underlyings(self):
  have={r['underlying'] for r in self._q('select distinct underlying from underlying_snapshots where spot is not null')}
  return [{'symbol':u,'kind':'index','supported':True} for u in INDEX_UNDERLYINGS if u in have]

 def reading(self,underlying):
  """The newest reading of this underlying that carries a spot: (captured_at text, spot)."""
  r=self._q('select captured_at,spot from underlying_snapshots where underlying=? and spot is not null '
   'order by captured_at desc limit 1',(underlying,))
  if not r:return None
  return r[0]['captured_at'],float(r[0]['spot'])

 def expiries(self,underlying):
  got=self.reading(underlying)
  if not got:return {'underlying':underlying,'as_of':None,'expiries':[]}
  at,_spot=got
  rows=self._q("select expiry,max(lot_size) lot,count(*) n from contracts where underlying=? and instrument_type in ('CE','PE') "
   "and expiry>=? group by expiry order by expiry",(underlying,at[:10]))
  out=[];by_month={}
  for r in rows:by_month.setdefault(r['expiry'][:7],[]).append(r['expiry'])
  now=parse_ist(at)
  for r in rows:
   e=r['expiry'];dte=round(years_between(now,e)*365,2)
   out.append({'expiry':e,'lot_size':r['lot'],'contracts':r['n'],'monthly':by_month[e[:7]][-1]==e,'days_to_expiry':dte})
  return {'underlying':underlying,'as_of':at,'expiries':out}

 def chain(self,underlying,expiry):
  """One row per strike at the newest reading: CE/PE last price, OI, volume, IV (computed), quality flags."""
  got=self.reading(underlying)
  if not got:return None
  at,spot=got
  key=(underlying,expiry,at)
  if key in self._memo:return self._memo[key]
  rows=self._q("select c.instrument_token token,c.tradingsymbol symbol,c.strike,c.instrument_type type,c.lot_size,c.tick_size,"
   "s.last_price ltp,s.bid,s.ask,s.oi,s.volume,s.last_trade_time from contracts c join snapshots s on s.instrument_token=c.instrument_token "
   "and s.captured_at=? where c.underlying=? and c.expiry=? and c.instrument_type in ('CE','PE') order by c.strike",(at,underlying,expiry))
  reading_at=parse_ist(at);t=years_between(reading_at,expiry)
  strikes={};lot=None;tick=None
  for r in rows:
   k=float(r['strike']);lot=r['lot_size'] or lot;tick=r['tick_size'] or tick
   flags=[]
   if r['bid'] is None or r['ask'] is None:flags.append('no_bid_ask')
   lt=parse_ist(r['last_trade_time'])
   age=(reading_at-lt).total_seconds() if lt else None
   if r['ltp'] is None:flags.append('no_price')
   elif age is not None and age>STALE_TRADE_SECONDS:flags.append('stale_trade')
   iv=None;iv_reason=None
   if r['ltp'] is not None:
    solved=IV.solve(float(r['ltp']),spot,k,t,r['type'],sensitivity=False)
    iv=solved.get('iv');iv_reason=solved.get('reason')
   side={'token':r['token'],'symbol':r['symbol'],'ltp':r['ltp'],'bid':r['bid'],'ask':r['ask'],'oi':r['oi'],'volume':r['volume'],
    'last_trade_time':r['last_trade_time'],'iv':round(iv*100,2) if iv else None,'iv_reason':None if iv else iv_reason,
    'flags':flags,'basis':'ltp'}
   strikes.setdefault(k,{'strike':k,'CE':None,'PE':None})[r['type']]=side
  ordered=sorted(strikes.values(),key=lambda x:x['strike'])
  ks=[x['strike'] for x in ordered]
  steps=Counter(round(b-a,2) for a,b in zip(ks,ks[1:]) if b>a)
  step=steps.most_common(1)[0][0] if steps else None
  atm=min(ks,key=lambda k:abs(k-spot)) if ks else None
  atm_row=strikes.get(atm) if atm is not None else None
  atm_iv=None
  if atm_row:
   vals=[x['iv'] for x in (atm_row['CE'],atm_row['PE']) if x and x['iv']]
   atm_iv=round(sum(vals)/len(vals),2) if vals else None
  out={'underlying':underlying,'expiry':expiry,'as_of':at,'spot':spot,'lot_size':lot,'tick_size':tick,'strike_step':step,
   'atm_strike':atm,'atm_iv':atm_iv,'days_to_expiry':round(t*365,2),'rows':ordered,
   'quality':{'bid_ask':'not captured by this store; prices are last traded (LTP)','source':'db/derivatives.db (stored reading)',
    'live':False}}
  if len(self._memo)>64:self._memo.clear()
  self._memo[key]=out
  return out

 def contract(self,underlying,expiry,strike,kind):
  ch=self.chain(underlying,expiry)
  if not ch:return None,None
  row=next((r for r in ch['rows'] if abs(r['strike']-float(strike))<1e-6),None)
  return ch,(row or {}).get(kind)
