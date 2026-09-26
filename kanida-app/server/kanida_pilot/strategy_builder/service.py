"""Glue between the store, the market reader and the pure analytics: draft validation, leg hydration, analysis,
paper fills. No route logic here and no I/O of its own beyond the Market and Store it is given."""
from __future__ import annotations
import math,re
from . import analytics as A
from . import charges as CH
from . import quotes as Q
from .store import checksum
from .templates import recognise

DATE=re.compile(r'^\d{4}-\d{2}-\d{2}$');SYMBOL=re.compile(r'^[A-Z0-9&-]{1,20}$')
SLIPPAGE_PCT=0.005
# entry price bases: exec = buy at the ask / sell at the bid (the price you could trade at now); mid; ltp; manual
BASES=('exec','mid','ltp','manual')   # paper fills: 0.5% of price or one tick, whichever is larger (no bid/ask in the store)


class Invalid(Exception):
 def __init__(self,message,code='FIELD_INVALID'):super().__init__(message);self.message=message;self.code=code


def _finite(v,name):
 """A finite number, or a structured 400 - never NaN/Infinity, never a bool, never a silent default (audit P07)."""
 if isinstance(v,bool):raise Invalid(f'{name} must be a number.')
 try:x=float(v)
 except (TypeError,ValueError):raise Invalid(f'{name} must be a number.')
 if not math.isfinite(x):raise Invalid(f'{name} must be a finite number.')
 return x


def _whole(v,name):
 """A whole number; 1.9 is rejected, never truncated to 1 (audit P07). '2' and 2.0 are accepted."""
 x=_finite(v,name)
 if not x.is_integer():raise Invalid(f'{name} must be a whole number (got {v}).')
 return int(x)


def _real_date(text):
 if not DATE.match(text):return False
 from datetime import date
 try:date.fromisoformat(text);return True
 except ValueError:return False


def normalize_body(raw):
 """A draft body the server will store: validated types, bounded sizes, nothing it does not understand."""
 if not isinstance(raw,dict):raise Invalid('The strategy body must be an object.')
 u=str(raw.get('underlying') or '').strip().upper()
 if u and not SYMBOL.match(u):raise Invalid('underlying is not a valid symbol.')
 e=str(raw.get('expiry') or '').strip()
 if e and not _real_date(e):raise Invalid('expiry must be a real date, YYYY-MM-DD (the exchange-local expiry date).')
 if not isinstance(raw.get('legs') or [],list):raise Invalid('legs must be a list.')
 if not isinstance(raw.get('scenario') or {},dict):raise Invalid('scenario must be an object.')
 legs=[]
 for i,l in enumerate(raw.get('legs') or []):
  if i>=A.MAX_LEGS:raise Invalid(f'At most {A.MAX_LEGS} legs are supported in this release.')
  if not isinstance(l,dict):raise Invalid('Each leg must be an object.')
  kind=str(l.get('type') or '').upper();side=str(l.get('side') or '').upper()
  if kind not in ('CE','PE'):raise Invalid('Leg type must be CE or PE (futures are not in this release).')
  if side not in ('B','S'):raise Invalid('Leg side must be B or S.')
  strike=_finite(l.get('strike'),'Leg strike')
  if strike<=0:raise Invalid('Leg strike must be positive.','FIELD_INVALID')
  lots=_whole(1 if l.get('lots') is None else l.get('lots'),'Lots')
  if not 1<=lots<=500:raise Invalid('Lots must be a whole number from 1 to 500.')
  basis=str(l.get('price_basis') or 'exec')
  if basis not in BASES:raise Invalid('price_basis must be one of: '+', '.join(BASES)+'.')
  price=l.get('price')
  if basis=='manual':
   price=_finite(price,'A manual price')
   if price<0:raise Invalid('A manual price cannot be negative.')
  le=str(l.get('expiry') or e).strip()
  if le and not _real_date(le):raise Invalid(f'Leg expiry {le[:12]} is not a real date (YYYY-MM-DD).')
  legs.append({'id':str(l.get('id') or f'L{i+1}')[:12],'type':kind,'side':side,'strike':strike,'lots':lots,
   'expiry':le[:10],'price_basis':basis,'price':price if basis=='manual' else None,'include':bool(l.get('include',True))})
 if len({l['id'] for l in legs})!=len(legs):raise Invalid('Leg ids must be unique.')
 sc=raw.get('scenario') or {}
 scenario={}
 if sc.get('spot') not in (None,''):
  scenario['spot']=_finite(sc['spot'],'Scenario spot')
  if scenario['spot']<=0:raise Invalid('Scenario spot must be positive.')
 if sc.get('at'):
  if not A.parse_ist(sc['at']):raise Invalid('Scenario time must be YYYY-MM-DD HH:MM (IST).')
  scenario['at']=str(sc['at'])[:16]
 if sc.get('iv_shift') not in (None,''):
  scenario['iv_shift']=_finite(sc['iv_shift'],'IV shift')
  if not -50<=scenario['iv_shift']<=50:raise Invalid('IV shift must be between -50 and +50 percentage points.')
 tpl=str(raw.get('template') or '')[:40] or None
 param=raw.get('param') if tpl else None
 if param is not None:
  param=_whole(param,'param')
 out={'underlying':u,'expiry':e,'legs':legs,'scenario':scenario,'template':tpl,'param':param,'linked':bool(raw.get('linked',False))}
 og=raw.get('origin')
 if isinstance(og,dict) and og.get('source')=='discover':      # P15: bounded, known keys only
  num=lambda v:(float(v) if isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) else None)
  sh=og.get('shown') if isinstance(og.get('shown'),dict) else {}
  out['origin']={'source':'discover','candidate_id':str(og.get('candidate_id') or '')[:40],'view':str(og.get('view') or '')[:12],
   'view_label':str(og.get('view_label') or '')[:40],'target':num(og.get('target')),'low':num(og.get('low')),'high':num(og.get('high')),
   'max_loss':num(og.get('max_loss')),'lots':num(og.get('lots')),'as_of':str(og.get('as_of') or '')[:19],'template':str(og.get('template') or '')[:40],
   'shown':{k:num(sh.get(k)) for k in ('pop','max_loss','max_profit','premium')}}
 return out


def hydrate(market,body):
 """Legs with the contract's own lot size and the stored reading's last price attached. Returns (chain, legs, problems)."""
 if not body['underlying'] or not body['expiry']:return None,[],[]
 chain=market.chain(body['underlying'],body['expiry'])
 if not chain:
  # the strategy expiry has passed but later legs remain (a calendar's far leg, a rolled position): reference the
  # nearest leg expiry that still has a chain (review H2)
  for e in sorted({l['expiry'] for l in body['legs'] if l.get('expiry') and l['expiry']!=body['expiry']}):
   chain=market.chain(body['underlying'],e)
   if chain:body={**body,'expiry':e};break
 if not chain:return None,[],['The option store has no reading for this underlying.']
 # multi-expiry (slice 12): each leg resolves in ITS OWN expiry's chain; the strategy expiry's chain is the reference
 chains={body['expiry']:chain}
 out=[];problems=[]
 for l in body['legs']:
  ch=chains.get(l['expiry'])
  if ch is None:
   ch=chains[l['expiry']]=market.chain(body['underlying'],l['expiry'])
  if not ch:
   problems.append(f"No reading for the {l['expiry']} expiry of {body['underlying']}.");continue
  rows={r['strike']:r for r in ch['rows']}
  row=(rows.get(l['strike']) or {}).get(l['type'])
  if not row:
   problems.append(f"{int(l['strike'])} {l['type']} is not listed for {l['expiry']} - it may have expired or never existed.")
   continue
  out.append(_resolved(l,row,ch,chain))
 return chain,out,problems


def _resolved(l,row,ch,chain):
 """One leg joined to its listed contract: lot, tick, quotes, entry price by its basis, market mark and market IV.
 The ONE place a leg is priced, so the builder, Discover and spreads see identical legs (audit P06)."""
 price,used=leg_price(l,row)
 tick=ch.get('tick_size') or chain.get('tick_size') or 0.05
 # a typed research price off the exchange tick is WARNED (insights) on stored bodies; new input is rejected at the
 # API (check_ticks). Fill averages are exempt.
 off_tick=(round(round(price/tick)*tick,2) if (used=='manual' and price is not None and not l.get('entry_from_fills')
           and abs(round(price/tick)*tick-price)>1e-6) else None)
 mark,mark_basis=market_mark(row)
 return {**l,'basis_used':used,'lot_size':ch.get('lot_size') or chain.get('lot_size'),'ltp':row['ltp'],'bid':row.get('bid'),'ask':row.get('ask'),'price':price,'token':row.get('token'),'symbol':row.get('symbol'),
  # Cost basis and market valuation are SEPARATE (GTM audit P04): `price` is what the leg cost (entry / fill /
  # typed), `mark` is what the market says it is worth now, and `iv` is the market's implied volatility solved from
  # that mark - never from the entry. A custom entry moves P&L and breakevens, not the Greeks.
  'mark':mark,'mark_basis':mark_basis,'iv':row.get('iv_x'),'iv_source':('market_'+mark_basis) if row.get('iv_x') is not None else None,
  'quote_at':row.get('quote_at'),'last_trade_time':row.get('last_trade_time'),'flags':row.get('flags') or [],'off_tick':off_tick,'tick':tick,
  'contract_id':contract_id(chain.get('underlying'),l.get('expiry') or ch.get('expiry'),l['strike'],l['type'])}


def contract_id(underlying,expiry,strike,kind):
 """The canonical identity of one listed option: exchange|underlying|expiry|strike|type (audit P01). Equal strike and
 type in different expiries are DIFFERENT contracts everywhere - selection, edits, orders, snapshots."""
 k=float(strike);ks=str(int(k)) if k.is_integer() else f'{k:g}'
 return f"NFO|{underlying}|{expiry}|{ks}|{kind}"


def analyze_on_chain(chain,legs,scenario=None,**kw):
 """Analytics for legs resolved in ONE chain, joined and priced exactly as the builder joins them, with the same
 probability reference (the chain's ATM IV) - Discover, spreads and the builder agree by construction (audit P06)."""
 rows={r['strike']:r for r in chain['rows']}
 joined=[]
 for l in legs:
  row=(rows.get(l['strike']) or {}).get(l['type']) if (l.get('expiry') or chain['expiry'])==chain['expiry'] else None
  joined.append(_resolved({**l,'expiry':l.get('expiry') or chain['expiry']},row,chain,chain) if row else l)
 reading=A.parse_ist(chain['as_of'])
 return joined,A.analyze(joined,chain['spot'],reading,scenario,ref_iv=reference_iv(chain),**kw)


def reference_iv(chain):
 """The chain's ATM IV as the POP/SD reference - but only from a trustworthy ATM price: a valid-book mid, or a last
 trade not flagged stale. Otherwise None, and the analytics fall back to a labelled proxy (quant audit P1)."""
 if not chain.get('atm_iv') or chain.get('atm_strike') is None:return None
 row=next((r for r in chain['rows'] if r['strike']==chain['atm_strike']),None)
 sides=[x for x in ((row or {}).get('CE'),(row or {}).get('PE')) if x and x.get('iv') is not None]
 if not sides or any('stale_trade' in (x.get('flags') or []) and x.get('basis')!='mid' for x in sides):return None
 return chain['atm_iv']/100.0


def market_mark(row):
 """(current market value of one option unit, basis). Mid of a valid book; the last trade otherwise - and said so."""
 bid,ask=row.get('bid'),row.get('ask')
 if Q.book(bid,ask)['ok']:return round((bid+ask)/2,4),'mid'
 return row.get('ltp'),'ltp'


def exec_price(row,side):
 """(price, basis, reason) where one could trade now: buy at the ask, sell at the bid - ONLY from a valid book
 (quotes.book: no crossed, zero or missing sides). Otherwise the last trade, labelled 'ltp' with the reason, so an
 indicative price can never pass as executable (audit P05). Shared by research pricing, spreads and the assistant."""
 bid,ask,ltp=row.get('bid'),row.get('ask'),row.get('ltp')
 b=Q.book(bid,ask)
 if b['ok']:return (ask if side=='B' else bid),'exec',None
 return ltp,'ltp',b['reason']


def leg_price(l,row):
 """(price, basis actually used). exec/mid need a valid book; otherwise LTP - and the basis says so."""
 if l['price_basis']=='manual':return l['price'],'manual'
 if l['price_basis']=='exec':
  px,basis,_=exec_price(row,l['side']);return px,basis
 if l['price_basis']=='mid' and Q.book(row.get('bid'),row.get('ask'))['ok']:return round((row['bid']+row['ask'])/2,2),'mid'
 return row.get('ltp'),'ltp'


_MARGIN={}
def margin(market,legs,product='NRML'):
 """Exchange margin (SPAN + exposure) from Kite's basket-margin read for these legs; cached 30 s per leg set."""
 act=[l for l in legs if l.get('include',True) and l.get('price') is not None]
 key=tuple(sorted((l['symbol'],l['side'],int(l['lots'])*int(l['lot_size'])) for l in act))+(product,)
 hit=_MARGIN.get(key)
 import time as _t
 if hit and _t.time()-hit[0]<30:return hit[1]
 try:
  m=market.basket_margin([{'symbol':l['symbol'],'side':l['side'],'qty':int(l['lots'])*int(l['lot_size']),'price':l['price'],'product':product} for l in act])
  single=sum((x.get('total') or 0) for x in m['per_leg'])
  out=A.ok(round(m['final'],2),basis='exchange_span_exposure',source='Zerodha Kite basket margin (read-only)',
   initial=round(m['initial'],2) if m.get('initial') is not None else None,hedge_benefit=round(single-m['final'],2) if m.get('final') is not None else None,product=product)
 except Exception as e:  # noqa: BLE001 - unavailable margin is stated, never zero
  out=A.na('MARGIN_UNAVAILABLE',note=f'Exchange margin could not be read ({type(e).__name__}).')
 if len(_MARGIN)>256:_MARGIN.clear()
 _MARGIN[key]=(_t.time(),out)
 return out


POSITION_KEYS=('max_profit','max_loss','breakevens','reward_risk','capital_at_risk','pop','outcomes','scenario_pnl','greeks',
 'greeks_scenario','pop_scenario','breakevens_target','premium','charges','margin')


def check_ticks(market,body):
 """Tick before calculate (Blueprint A): a NEW typed price off the contract's tick is a 400 naming the nearest valid
 price - never calculated or saved. The UI snaps on blur, so users meet this only through the API. Fill averages
 (entry_from_fills) are exempt; bodies already stored keep their warning."""
 if not any(l['price_basis']=='manual' for l in body.get('legs') or []):return
 try:_c,legs,_p=hydrate(market,body)
 except Exception:return                     # no reading to check against: the draft is validated on its next analysis
 bad=[l for l in legs if l.get('off_tick') is not None]
 if bad:
  l=bad[0]
  raise Invalid(f"{int(l['strike'])} {l['type']}: the price {l['price']} is not on the {l['tick']} tick; use {l['off_tick']}.",'OFF_TICK')


def analysis(market,body,table=True):
 chain,legs,problems=hydrate(market,body)
 wanted=[l for l in body.get('legs') or [] if l.get('include',True)]
 # the structure is what the USER asked for, never what happened to resolve (audit P04)
 base={'input_hash':checksum(body),'structure':recognise(wanted) if wanted else {'key':None,'name':'Empty','exact':False}}
 if not chain:return {**base,'status':'no_market','warnings':problems or ['Choose an underlying and an expiry.']}
 got={l['id'] for l in legs}
 unresolved=[l for l in wanted if l['id'] not in got]
 if unresolved:
  # fail closed: a strategy with an included leg that did not resolve has NO strategy-wide numbers. The survivors'
  # payoff is a different position (a spread missing its hedge is a naked option) and is never shown as this one.
  ids=[{'leg_id':l['id'],'contract_id':contract_id(body['underlying'],l['expiry'],l['strike'],l['type']),
        'label':f"{'Buy' if l['side']=='B' else 'Sell'} {l['strike']:g} {l['type']} {A.expiry_moment(l['expiry']).strftime('%d %b %Y').lstrip('0')}"} for l in unresolved]
  na=A.na('LEG_UNRESOLVED',note='Strategy-wide numbers need every included leg to resolve to a listed contract.',unresolved=ids)
  return {**base,'status':'incomplete','model_version':A.MODEL_VERSION,**{k:na for k in POSITION_KEYS},'curve':[],'table':[],'legs':[],
   'insights':[],'unresolved':ids,'as_of':chain['as_of'],'underlying':chain['underlying'],'lot_size':chain['lot_size'],'quality':chain['quality'],
   'warnings':problems+['Fix or exclude the missing leg: strategy-wide risk is unavailable until every included leg resolves.'],
   'legs_quotes':[{'id':l['id'],'bid':l.get('bid'),'ask':l.get('ask'),'ltp':l.get('ltp'),'basis_used':l['basis_used'],'mark':l.get('mark'),
    'mark_basis':l.get('mark_basis'),'quote_at':l.get('quote_at'),'contract_id':l.get('contract_id')} for l in legs]}
 reading=A.parse_ist(chain['as_of'])
 ref=reference_iv(chain)
 a=A.analyze(legs,chain['spot'],reading,body.get('scenario'),charges=CH.estimate,table_step=chain['strike_step'] if table else None,ref_iv=ref)
 a['warnings']=problems+(a.get('warnings') or [])
 from .market import INDEX_UNDERLYINGS
 if chain['underlying'] not in INDEX_UNDERLYINGS and a.get('insights') is not None:
  # stock options are PHYSICALLY settled: an ITM leg held into expiry becomes a delivery obligation, not cash
  d=chain.get('days_to_expiry') or 0
  a['insights'].insert(0,{'key':'physical','level':'warn' if d<=4 else 'info','text':(f'Stock option, physically settled: an in-the-money leg held into expiry means taking or giving delivery of {chain["underlying"]} shares ({d:.0f} days left). Brokers square off or ask for full margin in the last days - plan to exit before expiry.' if d<=4 else
   f'Stock option, physically settled: in-the-money legs held into expiry become delivery of {chain["underlying"]} shares. The Lab exits stock rules the session before expiry for this reason.')})
 if a.get('status') in ('ok','partial') and chain['quality'].get('live') and hasattr(market,'basket_margin'):
  a['margin']=margin(market,legs)
 if problems and a.get('status')=='ok':a['status']='partial'
 # P09: gross AND an estimated net view. Exit charges are estimated as closing every leg at its current market mark (a
 # named assumption); options held to expiry may settle instead. Gross numbers are unchanged and stay available.
 act=[l for l in legs if l.get('include',True)]
 if act and a.get('status') in ('ok','partial'):
  try:
   entry=(a.get('charges') or {}).get('value') or 0.0
   ex=CH.estimate([{**l,'side':'S' if l['side']=='B' else 'B','price':(l.get('mark') if l.get('mark') is not None else l['price'])} for l in act])['total']
   rt=round(entry+ex,2)
   netv=lambda m:(round(m['value']-rt,2) if (m or {}).get('status')=='available' and m.get('value') is not None else None)
   a['costs']={'entry':round(entry,2),'exit_estimate':round(ex,2),'round_trip':rt,'basis':'exit_at_current_mark',
    'note':'Entry at your entry prices plus an exit at today\'s market marks, from published rates and Rs 20 per order. Holding to expiry may cost less (settlement) or more (exercise STT).',
    'max_profit_net':netv(a.get('max_profit')),'max_loss_net':netv(a.get('max_loss'))}
  except Exception:a['costs']=A.na('COSTS_UNAVAILABLE')
 return {**base,**a,'as_of':chain['as_of'],'underlying':chain['underlying'],'lot_size':chain['lot_size'],
  'quality':chain['quality'],'price_basis':sorted({l['basis_used'] for l in legs}),'legs_quotes':[{'id':l['id'],'bid':l.get('bid'),'ask':l.get('ask'),'ltp':l.get('ltp'),'basis_used':l['basis_used'],'mark':l.get('mark'),
   'mark_basis':l.get('mark_basis'),'quote_at':l.get('quote_at'),'last_trade_time':l.get('last_trade_time'),'contract_id':l.get('contract_id'),
   'symbol':l.get('symbol'),'quote':Q.leg(l,A.parse_ist(chain['as_of']) or reading,'order')} for l in legs]}


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
  rows.append({'leg_id':f['leg_id'],'label':f"{'Buy' if f['units']>0 else 'Sell'} {int(lm.get('strike',0))} {lm.get('type','')}"+(f" {A.expiry_moment(lm['expiry']).strftime('%d %b').lstrip('0')}" if lm.get('expiry') else ''),
   'units':f['units'],'entry':f['price'],'exit':c['price'] if c else None,'mark':None if c else marks.get(f['leg_id']),'pnl':None if pnl is None else round(pnl,2)})
 # P08: the mode is named, and ACTION time (when the user acted) is kept apart from MARKET time (the prices used). A
 # practice run is priced from a stored reading; what it can and cannot do is stated, never implied.
 from datetime import datetime as _dt,timedelta as _td,timezone as _tz
 stamp=lambda t:_dt.fromtimestamp(t,_tz(_td(hours=5,minutes=30))).strftime('%Y-%m-%d %H:%M:%S') if t else None
 times={'opened_action_at':stamp(r.get('created_at')),'opened_market_at':r.get('opened_reading'),
        'closed_action_at':stamp(r.get('closed_at')),'closed_market_at':r.get('closed_reading')}
 eligible={'close':{'ok':r['status']=='open','reason':None if r['status']=='open' else 'Already closed.'},
  'monitor':{'ok':False,'reason':'Practice runs use stored prices; the live monitor is for live-quote paper deployments.'},
  'alerts':{'ok':False,'reason':'Alerts watch live-quote deployments or a strategy draft - not a stored-price practice run.'},
  'adjust':{'ok':False,'reason':'Adjust the strategy itself, or deploy it as live-quote paper to adjust a held position.'}}
 return {**r,'mode':'practice_stored','mode_label':'Stored-price practice',**times,'eligible':eligible,
  'strategy_name':None,'revision':{'id':rev['id'],'n':rev['n'],'name':rev['name']},'rows':rows,
  'as_of':chain['as_of'] if chain else None,'realised':round(realised,2),'unrealised':None if missing else round(unreal,2),'fees':round(fees,2),
  'net':None if missing else round(realised+unreal-fees,2),'close_now_estimate':None if (missing or r['status']!='open') else round(realised+close_now-fees,2),
  'warnings':problems+(['A leg has no mark at the newest reading; unrealised P&L is unavailable, not zero.'] if missing else [])}
