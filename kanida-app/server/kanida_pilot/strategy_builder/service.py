"""Glue between the store, the market reader and the pure analytics: draft validation, leg hydration, analysis,
paper fills. No route logic here and no I/O of its own beyond the Market and Store it is given."""
from __future__ import annotations
import math,re
from . import analytics as A
from . import charges as CH
from .store import checksum
from .templates import recognise

DATE=re.compile(r'^\d{4}-\d{2}-\d{2}$');SYMBOL=re.compile(r'^[A-Z0-9&-]{1,20}$')
SLIPPAGE_PCT=0.005   # paper fills: 0.5% of price or one tick, whichever is larger (no bid/ask in the store)


class Invalid(Exception):
 def __init__(self,message,code='FIELD_INVALID'):super().__init__(message);self.message=message;self.code=code


def normalize_body(raw):
 """A draft body the server will store: validated types, bounded sizes, nothing it does not understand."""
 if not isinstance(raw,dict):raise Invalid('The strategy body must be an object.')
 u=str(raw.get('underlying') or '').strip().upper()
 if u and not SYMBOL.match(u):raise Invalid('underlying is not a valid symbol.')
 e=str(raw.get('expiry') or '').strip()
 if e and not DATE.match(e):raise Invalid('expiry must be YYYY-MM-DD (the exchange-local expiry date).')
 legs=[]
 for i,l in enumerate(raw.get('legs') or []):
  if i>=A.MAX_LEGS:raise Invalid(f'At most {A.MAX_LEGS} legs are supported in this release.')
  if not isinstance(l,dict):raise Invalid('Each leg must be an object.')
  kind=str(l.get('type') or '').upper();side=str(l.get('side') or '').upper()
  if kind not in ('CE','PE'):raise Invalid('Leg type must be CE or PE (futures are not in this release).')
  if side not in ('B','S'):raise Invalid('Leg side must be B or S.')
  try:strike=float(l.get('strike'));lots=int(1 if l.get('lots') is None else l.get('lots'))
  except (TypeError,ValueError):raise Invalid('Leg strike and lots must be numbers.')
  if strike<=0 or not math.isfinite(strike):raise Invalid('Leg strike must be positive.')
  if not 1<=lots<=500:raise Invalid('Lots must be a whole number from 1 to 500.')
  basis=str(l.get('price_basis') or 'ltp')
  if basis not in ('ltp','manual'):raise Invalid('price_basis must be ltp or manual.')
  price=l.get('price')
  if basis=='manual':
   try:price=float(price)
   except (TypeError,ValueError):raise Invalid('A manual price must be a number.')
   if price<0 or not math.isfinite(price):raise Invalid('A manual price cannot be negative.')
  legs.append({'id':str(l.get('id') or f'L{i+1}')[:12],'type':kind,'side':side,'strike':strike,'lots':lots,
   'expiry':str(l.get('expiry') or e)[:10],'price_basis':basis,'price':price if basis=='manual' else None,'include':bool(l.get('include',True))})
 if len({l['id'] for l in legs})!=len(legs):raise Invalid('Leg ids must be unique.')
 sc=raw.get('scenario') or {}
 scenario={}
 if sc.get('spot') not in (None,''):
  try:scenario['spot']=float(sc['spot'])
  except (TypeError,ValueError):raise Invalid('Scenario spot must be a number.')
  if scenario['spot']<=0:raise Invalid('Scenario spot must be positive.')
 if sc.get('at'):
  if not A.parse_ist(sc['at']):raise Invalid('Scenario time must be YYYY-MM-DD HH:MM (IST).')
  scenario['at']=str(sc['at'])[:16]
 if sc.get('iv_shift') not in (None,''):
  try:scenario['iv_shift']=max(-50.0,min(50.0,float(sc['iv_shift'])))
  except (TypeError,ValueError):raise Invalid('IV shift must be a number of percentage points.')
 return {'underlying':u,'expiry':e,'legs':legs,'scenario':scenario,'template':str(raw.get('template') or '')[:40] or None}


def hydrate(market,body):
 """Legs with the contract's own lot size and the stored reading's last price attached. Returns (chain, legs, problems)."""
 if not body['underlying'] or not body['expiry']:return None,[],[]
 chain=market.chain(body['underlying'],body['expiry'])
 if not chain:return None,[],['The option store has no reading for this underlying.']
 rows={r['strike']:r for r in chain['rows']}
 out=[];problems=[]
 for l in body['legs']:
  if l['expiry']!=body['expiry']:problems.append('Mixed expiries are not supported in this release.')
  row=(rows.get(l['strike']) or {}).get(l['type'])
  if not row:
   problems.append(f"{int(l['strike'])} {l['type']} is not listed for {body['expiry']} - it may have expired or never existed.")
   continue
  price=l['price'] if l['price_basis']=='manual' else row['ltp']
  out.append({**l,'lot_size':chain['lot_size'],'ltp':row['ltp'],'price':price,'token':row['token'],'symbol':row['symbol'],
   # IV is solved by the analytics from THIS leg's own entry price (so the scenario at the reading reprices the leg
   # at exactly its entry); the chain's rounded display IV is not reused as a model input.
   'iv':None,'flags':row['flags']})
 return chain,out,problems


def analysis(market,body,table=True):
 chain,legs,problems=hydrate(market,body)
 base={'input_hash':checksum(body),'structure':recognise(legs) if legs else {'key':None,'name':'Empty','exact':False}}
 if not chain:return {**base,'status':'no_market','warnings':problems or ['Choose an underlying and an expiry.']}
 reading=A.parse_ist(chain['as_of'])
 a=A.analyze(legs,chain['spot'],reading,body.get('scenario'),charges=CH.estimate,table_step=chain['strike_step'] if table else None)
 a['warnings']=problems+(a.get('warnings') or [])
 if problems and a.get('status')=='ok':a['status']='partial'
 return {**base,**a,'as_of':chain['as_of'],'underlying':chain['underlying'],'lot_size':chain['lot_size'],
  'quality':chain['quality'],'price_basis':sorted({l['price_basis'] for l in legs})}


# --- paper ------------------------------------------------------------------------------------------------------
def tick_round(x,tick):
 tick=tick or 0.05
 return round(round(x/tick)*tick,2)


def fill_price(ltp,side,tick,opening=True):
 """Buy fills above LTP and sell fills below it by the slippage allowance (no bid/ask exists in the stored reading)."""
 slip=max(ltp*SLIPPAGE_PCT,tick or 0.05)
 buying=(side=='B') if opening else (side=='S')
 return tick_round(ltp+slip if buying else max(tick or 0.05,ltp-slip),tick)


POLICY={'name':'ltp_plus_slippage','slippage':f'max({SLIPPAGE_PCT*100:.1f}% of price, 1 tick) against you',
 'fees':CH.VERSION,'basis':'Last traded price at the stored reading - this store has no bid/ask.',
 'disclaimer':'Simulated fills. No order is sent to any broker.'}


def paper_fills(chain,legs,opening=True):
 fills=[]
 for l in legs:
  if not l.get('include',True):continue
  if l.get('ltp') is None:raise Invalid(f"{int(l['strike'])} {l['type']} has no price in the stored reading, so it cannot be filled.",'MISSING_QUOTE')
  u=A.units(l);side=l['side'] if opening else ('S' if l['side']=='B' else 'B')
  price=fill_price(float(l['ltp']),l['side'],chain.get('tick_size'),opening)
  fees=CH.leg_charges(side,price,abs(u))['total']
  fills.append({'leg_id':l['id'],'side':side,'units':abs(u) if side=='B' else -abs(u),'price':price,'basis':'ltp+slippage','fees':round(fees,2),
   'type':l['type'],'strike':l['strike']})
 return fills


def paper_view(market,store,user_id,run):
 """A paper run with its marks at the newest reading: realised, unrealised (at LTP), fees, and a close-now estimate."""
 r=store.paper(user_id,run)
 if not r:return None
 rev=store.revision(user_id,r['revision_id'])
 body=rev['body'];chain,legs,problems=hydrate(market,body)
 marks={l['id']:l.get('ltp') for l in legs}
 opens=[f for f in r['fills'] if f['action']=='open'];closes={f['leg_id']:f for f in r['fills'] if f['action']=='close'}
 rows=[];realised=unreal=fees=0.0;close_now=0.0;missing=False
 meta={l['id']:l for l in body['legs']}
 for f in opens:
  fees+=f['fees'];c=closes.get(f['leg_id'])
  if c:fees+=c['fees'];pnl=f['units']*(c['price']-f['price']);realised+=pnl
  else:
   m=marks.get(f['leg_id'])
   if m is None:missing=True;pnl=None
   else:
    pnl=f['units']*(m-f['price']);unreal+=pnl
    exit_side='S' if f['units']>0 else 'B'
    px=fill_price(m,'B' if f['units']>0 else 'S',chain.get('tick_size') if chain else 0.05,opening=False)
    close_now+=f['units']*(px-f['price'])-CH.leg_charges(exit_side,px,abs(f['units']))['total']
  lm=meta.get(f['leg_id'],{})
  rows.append({'leg_id':f['leg_id'],'label':f"{'Buy' if f['units']>0 else 'Sell'} {int(lm.get('strike',0))} {lm.get('type','')}",
   'units':f['units'],'entry':f['price'],'exit':c['price'] if c else None,'mark':None if c else marks.get(f['leg_id']),'pnl':None if pnl is None else round(pnl,2)})
 return {**r,'strategy_name':None,'revision':{'id':rev['id'],'n':rev['n'],'name':rev['name']},'rows':rows,
  'as_of':chain['as_of'] if chain else None,'realised':round(realised,2),'unrealised':None if missing else round(unreal,2),'fees':round(fees,2),
  'net':None if missing else round(realised+unreal-fees,2),'close_now_estimate':None if (missing or r['status']!='open') else round(realised+close_now-fees,2),
  'warnings':problems+(['A leg has no mark at the newest reading; unrealised P&L is unavailable, not zero.'] if missing else [])}
