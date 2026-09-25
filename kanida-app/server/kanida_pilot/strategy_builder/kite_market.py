"""Live option chains, quotes and exchange margin from Zerodha Kite — READ-ONLY market data.

Credentials: exactly the engine project's single source of truth, the same one market_data/kite_provider.py reads:
the newest row of `kite_tokens` in the DB at KANIDA_DB_PATH, and KITE_API_KEY, both from the engine's config/.env.
This module NEVER mints a token (the engine's auth worker does that), NEVER places, modifies or cancels an order,
and never logs or returns a secret. The only Kite endpoints it calls are:
  GET  /instruments/NFO      contract master (lot size, tick size, strikes, expiries)
  GET  /quote                 last price, depth (best bid/ask), OI, volume, exchange timestamp
  POST /margins/basket        exchange SPAN + exposure margin for a hypothetical basket (no order is created)

If the token is missing or rejected, `available()` is False with a reason and the builder falls back to the stored
reading - with the banner saying so.
"""
from __future__ import annotations
import csv,io,logging,os,re,sqlite3,threading,time
from datetime import datetime
from pathlib import Path
import httpx
from .analytics import parse_ist,years_between
from .market import INDEX_UNDERLYINGS,STALE_TRADE_SECONDS
from . import exchange as XC
from .. import implied_vol as IV

log=logging.getLogger('strategy_builder.kite')
API='https://api.kite.trade'
ENGINE_ENV=Path(os.getenv('KANIDA_ENGINE_ROOT',str(Path.home()/'Kanida'/'engine')))/'config'/'.env'
SPOT_SYMBOL={'NIFTY':'NSE:NIFTY 50','BANKNIFTY':'NSE:NIFTY BANK','FINNIFTY':'NSE:NIFTY FIN SERVICE',
 'MIDCPNIFTY':'NSE:NIFTY MID SELECT','NIFTYNXT50':'NSE:NIFTY NEXT 50'}
CHAIN_TTL=4.0        # seconds a chain is reused (quotes move; the UI refreshes on its own cadence)
QUOTE_BATCH=400      # Kite accepts up to 500 instruments per /quote call


class KiteUnavailable(Exception):pass


def _env(path=ENGINE_ENV):
 out={}
 try:
  for line in open(path,encoding='utf-8'):
   m=re.match(r'^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)\s*$',line)
   if m:out[m.group(1)]=m.group(2).strip().strip('"').strip("'")
 except OSError:pass
 return out


class KiteMarket:
 live=True
 def __init__(self,env_path=ENGINE_ENV,http=None):
  self.env_path=env_path;self.http=http or httpx.Client(timeout=12)
  self.lock=threading.Lock();self._instruments=None;self._inst_day=None;self._chains={};self._reason=None;self._checked=0.0

 # --- credentials -------------------------------------------------------------------------------------------------
 def _headers(self):
  env=_env(self.env_path);key=env.get('KITE_API_KEY');db=env.get('KANIDA_DB_PATH')
  if not key or not db:raise KiteUnavailable('Kite is not configured on this machine (engine config/.env).')
  try:
   c=sqlite3.connect(f'file:{db}?mode=ro',uri=True,timeout=5)
   row=c.execute('select access_token from kite_tokens order by id desc limit 1').fetchone();c.close()
  except sqlite3.Error:row=None
  tok=(row[0] if row else None) or env.get('KITE_ACCESS_TOKEN')
  if not tok:raise KiteUnavailable('No Kite access token has been minted yet.')
  return {'X-Kite-Version':'3','Authorization':f'token {key}:{tok}'}

 def _get(self,path,**params):
  r=self.http.get(API+path,params=params,headers=self._headers())
  if r.status_code==403:raise KiteUnavailable('The Kite token was rejected (it expires every morning; the engine auth worker mints a new one).')
  if r.status_code>=400:raise KiteUnavailable(f'Kite answered {r.status_code}.')
  return r

 def available(self):
  """(ok, reason). Probed at most every 30 s."""
  if time.time()-self._checked<30:return self._reason is None,self._reason
  self._checked=time.time()
  try:self._get('/quote',i='NSE:NIFTY 50');self._reason=None
  except (KiteUnavailable,httpx.HTTPError) as e:self._reason=str(e) if isinstance(e,KiteUnavailable) else 'Kite is unreachable.'
  return self._reason is None,self._reason

 # --- instrument master ------------------------------------------------------------------------------------------
 def instruments(self):
  today=XC.now_ist().strftime('%Y-%m-%d')
  with self.lock:
   if self._instruments is not None and self._inst_day==today:return self._instruments
  text=self._get('/instruments/NFO').text
  rows=[r for r in csv.DictReader(io.StringIO(text)) if r['name'] in INDEX_UNDERLYINGS and r['instrument_type'] in ('CE','PE')]
  for r in rows:
   r['strike']=float(r['strike']);r['lot_size']=int(r['lot_size']);r['tick_size']=float(r['tick_size']);r['instrument_token']=int(r['instrument_token'])
  with self.lock:self._instruments=rows;self._inst_day=today
  return rows

 def quotes(self,keys):
  out={}
  for i in range(0,len(keys),QUOTE_BATCH):
   out.update(self._get('/quote',i=keys[i:i+QUOTE_BATCH]).json().get('data') or {})
  return out

 # --- the Market interface the builder uses ----------------------------------------------------------------------
 def underlyings(self):
  names={r['name'] for r in self.instruments()}
  return [{'symbol':u,'kind':'index','supported':True} for u in INDEX_UNDERLYINGS if u in names and u in SPOT_SYMBOL]

 def reading(self,underlying):
  q=self.quotes([SPOT_SYMBOL[underlying]]).get(SPOT_SYMBOL[underlying])
  if not q or not q.get('timestamp'):return None      # never invent a sample time from the host clock (quant audit F3)
  return str(q['timestamp'])[:19],float(q['last_price'])

 def expiries(self,underlying):
  rows=[r for r in self.instruments() if r['name']==underlying]
  got=self.reading(underlying);at=got[0] if got else XC.now_ist().strftime('%Y-%m-%d %H:%M:%S')   # only filters past expiries
  by={}
  for r in rows:by.setdefault(r['expiry'],[]).append(r)
  exps=sorted(e for e in by if e>=at[:10]);months={}
  for e in exps:months.setdefault(e[:7],[]).append(e)
  now=parse_ist(at)
  return {'underlying':underlying,'as_of':at,'expiries':[{'expiry':e,'lot_size':by[e][0]['lot_size'],'contracts':len(by[e]),
   'monthly':months[e[:7]][-1]==e,'days_to_expiry':round(years_between(now,e)*365,2)} for e in exps]}

 def chain(self,underlying,expiry):
  key=(underlying,expiry)
  hit=self._chains.get(key)
  if hit and time.time()-hit[0]<CHAIN_TTL:return hit[1]
  rows=[r for r in self.instruments() if r['name']==underlying and r['expiry']==expiry]
  if not rows:return None
  spot_key=SPOT_SYMBOL[underlying]
  data=self.quotes([spot_key]+[f"NFO:{r['tradingsymbol']}" for r in rows])
  sq=data.get(spot_key)
  if not sq:return None
  if not sq.get('timestamp'):raise KiteUnavailable('Kite returned the index quote without a time stamp.')   # router falls back, honestly
  spot=float(sq['last_price']);at=str(sq.get('timestamp'))[:19];reading_at=parse_ist(at);t=years_between(reading_at,expiry)
  strikes={}
  for r in rows:
   q=data.get(f"NFO:{r['tradingsymbol']}") or {}
   depth=q.get('depth') or {}
   bid=(depth.get('buy') or [{}])[0].get('price') or None;ask=(depth.get('sell') or [{}])[0].get('price') or None
   ltp=q.get('last_price') or None
   flags=[]
   if not bid or not ask:flags.append('no_bid_ask')
   lt=parse_ist(q.get('last_trade_time'))
   if ltp is None:flags.append('no_price')
   elif lt and (reading_at-lt).total_seconds()>STALE_TRADE_SECONDS:flags.append('stale_trade')
   if bid and ask and bid>ask:flags.append('crossed')
   valid_book=bool(bid and ask and bid<=ask)
   mid=(bid+ask)/2 if valid_book else ltp       # a crossed book has no meaningful mid: value it at the last trade
   iv=None;reason=None
   if mid:
    s=IV.solve(float(mid),spot,r['strike'],t,r['instrument_type'],sensitivity=False);iv=s.get('iv');reason=s.get('reason')
   strikes.setdefault(r['strike'],{'strike':r['strike'],'CE':None,'PE':None})[r['instrument_type']]={
    'token':r['instrument_token'],'symbol':r['tradingsymbol'],'ltp':ltp,'bid':bid,'ask':ask,'oi':q.get('oi'),'volume':q.get('volume'),
    'last_trade_time':q.get('last_trade_time'),'iv':round(iv*100,2) if iv else None,'iv_x':iv,'iv_reason':None if iv else reason,'flags':flags,
    'basis':'mid' if valid_book else 'ltp','quote_at':str(q.get('timestamp') or '')[:19] or None}
  ordered=sorted(strikes.values(),key=lambda x:x['strike']);ks=[x['strike'] for x in ordered]
  from collections import Counter
  steps=Counter(round(b-a,2) for a,b in zip(ks,ks[1:]));step=steps.most_common(1)[0][0] if steps else None
  atm=min(ks,key=lambda k:abs(k-spot));ar=strikes[atm];ivs=[x['iv'] for x in (ar['CE'],ar['PE']) if x and x['iv']]
  out={'underlying':underlying,'expiry':expiry,'as_of':at,'spot':spot,'lot_size':rows[0]['lot_size'],'tick_size':rows[0]['tick_size'],
   'strike_step':step,'atm_strike':atm,'atm_iv':round(sum(ivs)/len(ivs),2) if ivs else None,'days_to_expiry':round(t*365,2),'rows':ordered,
   'quality':{'bid_ask':'best bid/ask from Kite market depth','source':'Zerodha Kite (live)','live':True}}
  self._chains[key]=(time.time(),out)
  if len(self._chains)>32:self._chains.clear()
  return out

 def contract(self,underlying,expiry,strike,kind):
  ch=self.chain(underlying,expiry)
  if not ch:return None,None
  row=next((r for r in ch['rows'] if abs(r['strike']-float(strike))<1e-6),None)
  return ch,(row or {}).get(kind)

 def basket_margin(self,orders):
  """Exchange margin for a HYPOTHETICAL basket. orders: [{symbol, side, qty, price, product}]. Nothing is placed."""
  body=[{'exchange':'NFO','tradingsymbol':o['symbol'],'transaction_type':'BUY' if o['side']=='B' else 'SELL','variety':'regular',
   'product':o.get('product','NRML'),'order_type':'LIMIT','quantity':int(o['qty']),'price':float(o['price']),'trigger_price':0} for o in orders]
  r=self.http.post(API+'/margins/basket',params={'consider_positions':'false','mode':'compact'},headers=self._headers(),json=body)
  if r.status_code>=400:raise KiteUnavailable(f'Kite margin answered {r.status_code}.')
  d=r.json().get('data') or {}
  return {'initial':(d.get('initial') or {}).get('total'),'final':(d.get('final') or {}).get('total'),
   'per_leg':[{'symbol':o.get('tradingsymbol'),'total':o.get('total')} for o in d.get('orders') or []],'source':'kite_basket_margin'}

 def close(self):self.http.close()


class MarketRouter:
 """Live Kite when its token works, otherwise the stored reading - one object, same interface, honest `source`."""
 def __init__(self,stored,live=None):self.stored=stored;self.live_market=live
 @property
 def current(self):
  if self.live_market:
   ok,_=self.live_market.available()
   if ok:return self.live_market
  return self.stored
 def status(self):
  if not self.live_market:return {'live':False,'source':'stored','reason':'Live data is not configured.'}
  ok,reason=self.live_market.available()
  return {'live':ok,'source':'kite' if ok else 'stored','reason':None if ok else reason}
 def _call(self,name,*a):
  m=self.current
  try:return getattr(m,name)(*a)
  except (KiteUnavailable,httpx.HTTPError) as e:
   if m is self.stored:raise
   log.warning('Kite %s failed (%s); serving the stored reading.',name,type(e).__name__)
   self.live_market._checked=0;return getattr(self.stored,name)(*a)
 def underlyings(self):return self._call('underlyings')
 def reading(self,u):return self._call('reading',u)
 def expiries(self,u):return self._call('expiries',u)
 def chain(self,u,e):return self._call('chain',u,e)
 def contract(self,*a):return self._call('contract',*a)
 def basket_margin(self,orders):
  m=self.current
  if m is self.stored:raise KiteUnavailable('Exchange margin needs live Kite access.')
  return m.basket_margin(orders)
 def close(self):
  self.stored.close()
  if self.live_market:self.live_market.close()
