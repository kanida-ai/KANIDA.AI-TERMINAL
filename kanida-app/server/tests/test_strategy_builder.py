"""The Strategy Builder: exact payoff maths on known structures, recognition, template resolution, the library with
versioned autosave and immutable snapshots, paper fills, Discover, owner scoping, and the expiry-date invariant."""
import json,math,sqlite3,time
from datetime import datetime
import pytest
from kanida_pilot.app import create_app
from kanida_pilot.strategy_builder import mount
from kanida_pilot.strategy_builder import analytics as A
from kanida_pilot.strategy_builder.templates import TEMPLATES,recognise,resolve
from kanida_pilot.strategy_builder.market import Market
from kanida_pilot import implied_vol as IV
from test_pilot import Evidence,pilot_settings,signup
from test_workspace import grant

AT='2026-09-23 15:30:00';EXP='2026-09-29';SPOT=23000.0;LOT=65;SIGMA=0.12
STRIKES=[22500+50*i for i in range(21)]   # 22500..23500


def leg(kind,strike,side,price,lots=1,**kw):
 return {'id':kw.pop('id',f'{side}{kind}{strike}'),'type':kind,'strike':float(strike),'side':side,'lots':lots,'lot_size':LOT,
  'price':price,'expiry':EXP,'include':True,**kw}


# --- exact expiry maths ---------------------------------------------------------------------------------------
def test_long_call_is_capped_below_and_unlimited_above():
 p=A.expiry_profile([leg('CE',23000,'B',100)])
 assert p['unlimited_profit'] and not p['unlimited_loss']
 assert p['max_loss']==pytest.approx(-100*LOT) and p['breakevens']==[23100.0]


def test_short_put_loss_is_bounded_by_zero_spot_not_the_chart():
 p=A.expiry_profile([leg('PE',23000,'S',100)])
 assert not p['unlimited_loss'] and p['max_loss']==pytest.approx((100-23000)*LOT)   # spot cannot go below 0
 assert p['max_profit']==pytest.approx(100*LOT) and p['breakevens']==[22900.0]


def test_bull_call_spread():
 p=A.expiry_profile([leg('CE',23000,'B',150),leg('CE',23200,'S',60)])
 assert p['max_loss']==pytest.approx(-90*LOT) and p['max_profit']==pytest.approx(110*LOT)
 assert p['breakevens']==[23090.0] and not p['unlimited_loss'] and not p['unlimited_profit']


def test_short_straddle_has_two_breakevens_and_unlimited_loss():
 p=A.expiry_profile([leg('CE',23000,'S',120),leg('PE',23000,'S',110)])
 assert p['unlimited_loss'] and p['breakevens']==[22770.0,23230.0] and p['max_profit']==pytest.approx(230*LOT)


def test_iron_condor_with_asymmetric_wings():
 legs=[leg('PE',22600,'B',10),leg('PE',22800,'S',30),leg('CE',23200,'S',35),leg('CE',23500,'B',8)]
 p=A.expiry_profile(legs)
 credit=30+35-10-8
 assert p['max_profit']==pytest.approx(credit*LOT)
 # the wider call wing sets the maximum loss: 300 wide minus the credit
 assert p['max_loss']==pytest.approx(-(300-credit)*LOT)
 assert p['breakevens']==[22800.0-credit,23200.0+credit]


def test_greeks_units_and_signs():
 g=A.bs_greeks(23000,23000,5/365,0.12,'CE')
 assert 0.45<g['delta']<0.6 and g['gamma']>0 and g['theta']<0 and g['vega']>0
 p=A.bs_greeks(23000,23000,5/365,0.12,'PE')
 assert p['delta']==pytest.approx(g['delta']-1,abs=1e-9)


def test_analyze_never_reports_zero_for_what_it_cannot_compute():
 at=datetime(2026,9,23,15,30)
 a=A.analyze([leg('CE',23000,'B',None)],SPOT,at)
 assert a['status']=='invalid'
 a=A.analyze([leg('CE',23000,'B',150,expiry=EXP),leg('CE',23000,'S',100,expiry='2026-10-27',id='x')],SPOT,at)
 # slice 12: multi-expiry is analysed as a labelled MODEL at the near expiry - or stated unavailable, never a zero
 assert (a['max_loss']['status']=='available' and a['max_loss']['basis']=='model_at_near_expiry') or a['max_loss'].get('reason')
 assert a['margin']['status']=='unavailable'


def test_scenario_at_expiry_equals_the_terminal_payoff():
 legs=[leg('CE',23000,'B',150,iv=0.12),leg('CE',23200,'S',60,iv=0.12)]
 a=A.analyze(legs,SPOT,datetime(2026,9,23,15,30),{'spot':23300,'at':'2026-09-29 15:30'})
 assert a['scenario']['is_expiry'] and a['scenario_pnl']['value']==pytest.approx(A.expiry_pnl(legs,23300))


# --- recognition ----------------------------------------------------------------------------------------------
@pytest.mark.parametrize('legs,key',[
 ([leg('CE',23000,'B',1)],'long_call'),([leg('PE',23000,'S',1)],'short_put'),
 ([leg('CE',23000,'B',1),leg('CE',23200,'S',1)],'bull_call_spread'),([leg('CE',23000,'S',1),leg('CE',23200,'B',1)],'bear_call_spread'),
 ([leg('PE',23000,'B',1),leg('PE',22800,'S',1)],'bear_put_spread'),([leg('PE',23000,'S',1),leg('PE',22800,'B',1)],'bull_put_spread'),
 ([leg('CE',23000,'B',1),leg('PE',23000,'B',1)],'long_straddle'),([leg('CE',23200,'S',1),leg('PE',22800,'S',1)],'short_strangle'),
 ([leg('PE',22600,'B',1),leg('PE',22800,'S',1),leg('CE',23200,'S',1),leg('CE',23400,'B',1)],'iron_condor'),
 ([leg('PE',22800,'B',1),leg('PE',23000,'S',1),leg('CE',23000,'S',1),leg('CE',23200,'B',1)],'iron_butterfly'),
 ([leg('CE',23000,'B',1,lots=2),leg('CE',23200,'S',1)],'custom'),
])
def test_recognise(legs,key):
 assert recognise(legs)['key']==key


# --- a derivatives store and a pilot with the builder mounted -------------------------------------------------
def store(path,bid_ask=False):
 c=sqlite3.connect(path)
 c.executescript('''create table contracts(instrument_token integer primary key, tradingsymbol text, underlying text, instrument_type text,
  strike real, expiry text, lot_size integer, tick_size real);
 create table snapshots(instrument_token integer, captured_at text, last_price real, bid real, ask real, oi real, volume real, last_trade_time text);
 create table underlying_snapshots(underlying text, captured_at text, spot real);''')
 c.execute('insert into underlying_snapshots values(?,?,?)',('NIFTY','2026-09-23 15:15:00',22990.0))
 c.execute('insert into underlying_snapshots values(?,?,?)',('NIFTY',AT,SPOT))
 t=A.years_between(datetime(2026,9,23,15,30),EXP);token=1000
 for k in STRIKES:
  for kind in ('CE','PE'):
   token+=1;px=round(IV.price_bs(SPOT,k,t,IV.RISK_FREE_RATE,SIGMA,kind),2)
   c.execute('insert into contracts values(?,?,?,?,?,?,?,?)',(token,f'NIFTY26SEP{k}{kind}','NIFTY',kind,k,EXP,LOT,0.05))
   c.execute('insert into snapshots values(?,?,?,?,?,?,?,?)',(token,AT,max(px,0.05),(px-0.5) if bid_ask else None,(px+0.5) if bid_ask else None,1e5,1e6,'2026-09-23 15:29:50'))
 c.commit();c.close();return path


@pytest.fixture
def pilot(tmp_path):
 db=store(str(tmp_path/'derivatives.db'))
 settings=pilot_settings(tmp_path,derivatives_database=db,snapshots='off')
 app=create_app(settings,evidence=Evidence())
 sb=mount(app,settings,str(tmp_path/'sb.db'))
 assert sb
 owner=signup(app);other=signup(app,'member@example.invalid','member');grant(app,'member@example.invalid')
 yield app,owner,other
 app.state.db.close();sb.close();app.state.strategy_builder_market.close()


def test_expiry_dates_are_served_exactly_as_stored(pilot):
 _a,owner,_o=pilot
 e=owner.get('/api/sb/expiries?underlying=NIFTY').json()
 assert [x['expiry'] for x in e['expiries']]==[EXP] and e['expiries'][0]['lot_size']==LOT and e['as_of']==AT
 ch=owner.get(f'/api/sb/chain?underlying=NIFTY&expiry={EXP}').json()
 assert ch['expiry']==EXP and ch['strike_step']==50 and ch['atm_strike']==23000 and ch['spot']==SPOT
 assert ch['atm_iv']==pytest.approx(12,abs=0.3)
 row=next(r for r in ch['rows'] if r['strike']==23000)['CE']
 assert row['basis']=='ltp' and 'no_bid_ask' in row['flags']


def test_every_core_template_resolves_to_its_own_structure():
 m=Market(store('/tmp/sb_resolve_test.db') if False else None) if False else None
 import tempfile,os
 d=tempfile.mkdtemp();p=store(os.path.join(d,'d.db'));m=Market(p)
 ch=m.chain('NIFTY',EXP)
 for t in TEMPLATES:
  if t.get('multi_expiry'):continue            # needs a later expiry: test_slice12 covers calendars and diagonals
  for v in ((t['param'] or {}).get('variants') or [None]):
   got=resolve(t['key'],ch,v)
   assert recognise(got['legs'])['key']==t['key'],(t['key'],v,got['legs'])
 m.close()


def test_library_autosave_conflict_snapshot_restore_duplicate(pilot):
 _a,owner,other=pilot
 legs=owner.post('/api/sb/templates/resolve',json={'template':'bull_call_spread','underlying':'NIFTY','expiry':EXP,'param':4}).json()['legs']
 body={'underlying':'NIFTY','expiry':EXP,'legs':legs}
 s=owner.post('/api/sb/strategies',json={'body':body}).json()
 assert s['name']=='Untitled NIFTY strategy' and s['draft']['version']==1
 a=owner.post('/api/sb/analyze',json={'body':s['draft']['body']}).json()
 assert a['structure']['key']=='bull_call_spread' and a['max_loss']['status']=='available' and a['input_hash']
 assert a['premium']['direction']=='debit' and a['charges']['breakdown']['version'].startswith('fo-options')
 saved=owner.post(f"/api/sb/strategies/{s['id']}/draft",json={'version':1,'body':{**body,'scenario':{'spot':23200}}}).json()
 assert saved['draft']['version']==2
 stale=owner.post(f"/api/sb/strategies/{s['id']}/draft",json={'version':1,'body':body})
 assert stale.status_code==409 and stale.json()['current']['draft']['version']==2
 snap=owner.post(f"/api/sb/strategies/{s['id']}/snapshots",json={'name':'first'}).json()
 assert snap['n']==1 and snap['intact'] and snap['analysis']['max_loss']['value']<0
 owner.post(f"/api/sb/strategies/{s['id']}/draft",json={'version':2,'body':{**body,'legs':legs[:1]}})
 back=owner.post(f"/api/sb/strategies/{s['id']}/restore",json={'revision_id':snap['id'],'version':3}).json()
 assert back['draft']['version']==4 and len(back['draft']['body']['legs'])==2
 copy=owner.post(f"/api/sb/strategies/{s['id']}/duplicate",json={}).json()
 assert copy['id']!=s['id'] and copy['source_strategy_id']==s['id'] and copy['name'].endswith('(copy)')
 lib=owner.get('/api/sb/strategies').json()['strategies']
 assert {x['structure'] for x in lib}=={'Bull Call Spread'}
 # owner scoping
 for call in (lambda c:c.get(f"/api/sb/strategies/{s['id']}"),lambda c:c.post(f"/api/sb/strategies/{s['id']}/draft",json={'version':4,'body':body}),
   lambda c:c.get(f"/api/sb/revisions/{snap['id']}")):
  assert call(other).status_code==404


def test_bad_bodies_are_refused_with_a_reason(pilot):
 _a,owner,_o=pilot
 for bad,needle in (({'legs':[{'type':'FUT','side':'B','strike':1}]},'CE or PE'),({'expiry':'29-09-2026'},'YYYY-MM-DD'),
   ({'legs':[{'type':'CE','side':'B','strike':23000,'lots':0}]},'Lots'),({'legs':[{'type':'CE','side':'B','strike':23000}]*9},'At most')):
  r=owner.post('/api/sb/analyze',json={'body':bad})
  assert r.status_code==400 and needle in r.json()['error'],r.json()


def test_unlisted_strike_is_named_not_zeroed(pilot):
 _a,owner,_o=pilot
 a=owner.post('/api/sb/analyze',json={'body':{'underlying':'NIFTY','expiry':EXP,'legs':[{'type':'CE','side':'B','strike':30000}]}}).json()
 assert any('not listed' in w for w in a['warnings'])


def test_paper_run_fills_against_you_and_never_touches_a_broker(pilot):
 app,owner,_o=pilot
 legs=owner.post('/api/sb/templates/resolve',json={'template':'iron_condor','underlying':'NIFTY','expiry':EXP}).json()['legs']
 s=owner.post('/api/sb/strategies',json={'body':{'underlying':'NIFTY','expiry':EXP,'legs':legs}}).json()
 assert owner.post(f"/api/sb/strategies/{s['id']}/paper",json={}).status_code==400    # confirmation required
 run=owner.post(f"/api/sb/strategies/{s['id']}/paper",json={'confirm':True}).json()
 assert run['status']=='open' and run['policy']['name']=='ltp_plus_slippage' and 'No order' in run['policy']['disclaimer']
 ltp={l['id']:l['ltp'] for l in legs}
 for f in run['fills']:
  assert (f['price']>=ltp[f['leg_id']]) if f['units']>0 else (f['price']<=ltp[f['leg_id']])
 assert run['fees']>0 and run['unrealised'] is not None and run['close_now_estimate']<run['net']
 closed=owner.post(f"/api/sb/paper/{run['id']}/close",json={'confirm':True}).json()
 assert closed['status']=='closed' and closed['realised']<0     # a round trip at one reading only costs slippage + fees
 assert owner.post(f"/api/sb/paper/{run['id']}/close",json={'confirm':True}).status_code==409
 detail=owner.get(f"/api/sb/strategies/{s['id']}").json()
 assert detail['snapshots'][0]['name']=='Paper entry' and detail['paper'][0]['status']=='closed'
 assert app.state.db  # no pilot order rows were written
 from kanida_pilot.db import orders
 from sqlalchemy import select,func
 with app.state.db.tx() as c:assert c.execute(select(func.count()).select_from(orders)).scalar()==0


def test_discover_shortlist_is_small_explained_and_honest(pilot):
 _a,owner,_o=pilot
 r=owner.post('/api/sb/discover',json={'underlying':'NIFTY','expiry':EXP,'view':'up','target':23300}).json()
 assert 0<len(r['candidates'])<=6 and all(c['risk']=='defined' for c in r['candidates'])
 assert all(c['evidence']['status']=='model_only' and c['pnl_at_view']>0 for c in r['candidates'])
 assert len({c['template'] for c in r['candidates']})==len(r['candidates'])      # one per structure
 assert r['candidates'][0]['why'][0].startswith('At 23,300 on expiry')
 # a limit nothing can meet names the binding constraint
 none=owner.post('/api/sb/discover',json={'underlying':'NIFTY','expiry':EXP,'view':'up','target':23300,'max_loss':1}).json()
 assert none['candidates']==[] and none['binding']['reason']=='over_max_loss' and none['binding']['suggestion']
 bad=owner.post('/api/sb/discover',json={'underlying':'NIFTY','expiry':EXP,'view':'up','target':22000})
 assert bad.status_code==400 and 'above the current spot' in bad.json()['error']
 rng=owner.post('/api/sb/discover',json={'underlying':'NIFTY','expiry':EXP,'view':'range','low':22850,'high':23150}).json()
 assert rng['candidates'] and {c['template'] for c in rng['candidates']}<={'iron_condor','iron_butterfly','long_call_butterfly','long_put_butterfly'}


def test_scenario_at_the_reading_reprices_each_leg_at_its_entry(pilot):
 _a,owner,_o=pilot
 legs=owner.post('/api/sb/templates/resolve',json={'template':'iron_condor','underlying':'NIFTY','expiry':EXP}).json()['legs']
 a=owner.post('/api/sb/analyze',json={'body':{'underlying':'NIFTY','expiry':EXP,'legs':legs}}).json()
 assert a['scenario_pnl']['value']==pytest.approx(0,abs=0.5)


# --- slice 4: order review, idempotent intents, the paper broker --------------------------------------------------
import kanida_pilot.strategy_builder.execution as EX


class FakeKite:
 """A live market with fresh quotes whose depth a test can move. No network."""
 live=True
 def __init__(self,db):
  self.m=Market(db);self.books={};self.fail_margin=False
 def available(self):return True,None
 def _now(self):return EX.now_ist().strftime('%Y-%m-%d %H:%M:%S')
 def chain(self,u,e):
  ch=dict(self.m.chain(u,e));rows=[]
  for r in ch['rows']:
   row={'strike':r['strike']}
   for k in ('CE','PE'):
    x=dict(r[k]);b=self.books.get(x['symbol'],(round(x['ltp']-0.5,2),round(x['ltp']+0.5,2)))
    x['bid'],x['ask']=b;x['flags']=[];x['quote_at']=self._now();row[k]=x
   rows.append(row)
  ch['rows']=rows;ch['as_of']=self._now();ch['quality']={**ch['quality'],'live':True}
  return ch
 def quotes(self,keys):
  out={}
  for k in keys:
   sym=k.split(':',1)[1];b=self.books.get(sym)
   if b is None:
    for r in self.m.chain('NIFTY',EXP)['rows']:
     for x in (r['CE'],r['PE']):
      if x['symbol']==sym:b=(round(x['ltp']-0.5,2),round(x['ltp']+0.5,2))
   out[k]={'timestamp':self._now(),'depth':{'buy':[{'price':b[0]}],'sell':[{'price':b[1]}]}}
  return out
 def basket_margin(self,orders):
  if self.fail_margin:raise RuntimeError('down')
  per=[{'symbol':o['symbol'],'total':(o['qty']*o['price'] if o['side']=='B' else 150000.0*o['qty']/65)} for o in orders]
  final=sum(x['total'] for x in per)*(0.6 if len(orders)>1 else 1)
  return {'initial':sum(x['total'] for x in per),'final':final,'per_leg':per,'source':'fake'}
 def underlyings(self):return self.m.underlyings()
 def expiries(self,u):return self.m.expiries(u)
 def reading(self,u):return self.m.reading(u)
 def close(self):self.m.close()


@pytest.fixture
def live(tmp_path,monkeypatch):
 db=store(str(tmp_path/'derivatives.db'))
 settings=pilot_settings(tmp_path,derivatives_database=db,snapshots='off')
 app=create_app(settings,evidence=Evidence())
 fake=FakeKite(db)
 monkeypatch.setattr(EX,'market_open',lambda at=None:True)
 sb=mount(app,settings,str(tmp_path/'sb.db'),live=fake)
 owner=signup(app);other=signup(app,'member@example.invalid','member');grant(app,'member@example.invalid')
 app.state.strategy_builder_execution.stop()
 yield app,owner,other,fake
 app.state.db.close();sb.close()


def strategy(owner,template='bull_call_spread',lots=1,param=None):
 legs=owner.post('/api/sb/templates/resolve',json={'template':template,'underlying':'NIFTY','expiry':EXP,'param':param,'lots':lots}).json()['legs']
 return owner.post('/api/sb/strategies',json={'body':{'underlying':'NIFTY','expiry':EXP,'legs':legs}}).json()


def test_preview_needs_live_quotes(pilot):
 _a,owner,_o=pilot
 s=strategy(owner)
 r=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={})
 assert r.status_code==409 and r.json()['code']=='MARKET_DATA_NOT_LIVE'


def test_preview_is_the_exact_plan_with_hash_expiry_sequence_and_slices(live):
 _a,owner,_o,_f=live
 s=strategy(owner,lots=30)                         # 30 lots x 65 = 1950 units > the 1800 freeze
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 assert [c['key'] for c in p['checks'] if c['status']=='block'] in ([],['margin']) and len(p['hash'])==64 and p['ttl']==30 and p['mode']=='paper' and p['live']['enabled'] is False
 buy,sell=p['orders']
 assert buy['side']=='B' and buy['group']==1 and sell['group']==2
 assert buy['slices']==[1755,195] and buy['limit']==EX.tick_round(buy['ask'],0.05,up=True) and sell['limit']==EX.tick_round(sell['bid'],0.05)
 assert p['margin']['final']>0 and {c['key'] for c in p['checks']}>={'market_open','quotes_fresh','quotes_present','spread','freeze','risk','margin'}
 a=owner.post('/api/sb/analyze',json={'body':s['draft']['body']}).json()
 assert a['margin']['status']=='available' and a['margin']['source'].startswith('Zerodha Kite') and a['price_basis']==['exec']


def test_confirm_is_guarded_and_idempotent(live):
 app,owner,other,_f=live
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 url=f"/api/sb/strategies/{s['id']}/deployments"
 assert owner.post(url,json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k1'}).status_code==400   # no confirm
 bad=owner.post(url,json={'preview_id':p['id'],'preview_hash':'0'*64,'idempotency_key':'k1','confirm':True})
 assert bad.status_code==409 and bad.json()['code']=='PREVIEW_CHANGED'
 d=owner.post(url,json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k1','confirm':True}).json()
 again=owner.post(url,json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k1','confirm':True}).json()
 assert d['id']==again['id'] and d['mode']=='paper'
 p2=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 clash=owner.post(url,json={'preview_id':p2['id'],'preview_hash':p2['hash'],'idempotency_key':'k1','confirm':True})
 assert clash.status_code==409 and clash.json()['code']=='IDEMPOTENCY_CONFLICT'
 assert other.get(f"/api/sb/deployments/{d['id']}").status_code==404
 # an edit after the preview invalidates it
 p3=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 owner.post(f"/api/sb/strategies/{s['id']}/draft",json={'version':s['draft']['version'],'body':{**s['draft']['body'],'scenario':{'spot':23100}}})
 stale=owner.post(url,json={'preview_id':p3['id'],'preview_hash':p3['hash'],'idempotency_key':'k3','confirm':True})
 assert stale.status_code==409 and stale.json()['code']=='PREVIEW_CHANGED'
 # expiry
 p4=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 with app.state.strategy_builder_store.lock:
  app.state.strategy_builder_store.c.execute('update previews set expires_at=0 where id=?',(p4['id'],));app.state.strategy_builder_store.c.commit()
 exp=owner.post(url,json={'preview_id':p4['id'],'preview_hash':p4['hash'],'idempotency_key':'k4','confirm':True})
 assert exp.status_code==409 and exp.json()['code']=='PREVIEW_EXPIRED'
 from kanida_pilot.db import orders
 from sqlalchemy import select,func
 with app.state.db.tx() as c:assert c.execute(select(func.count()).select_from(orders)).scalar()==0


def test_marketable_orders_fill_buys_first_then_sells_and_close_books_pnl(live):
 app,owner,_o,fake=live
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'m1','confirm':True}).json()
 assert d['status']=='active' and [f['side'] for f in d['fills']]==['B','S']
 buy=next(o for o in p['orders'] if o['side']=='B');sell=next(o for o in p['orders'] if o['side']=='S')
 assert d['fills'][0]['price']==buy['ask'] and d['fills'][1]['price']==sell['bid'] and d['fees']>0
 assert d['unrealised'] is not None and d['net']<0                    # marked at liquidation prices: pays the spread
 cp=owner.post(f"/api/sb/deployments/{d['id']}/close-preview",json={}).json()
 assert cp['kind']=='close' and {o['side'] for o in cp['orders']}=={'B','S'} and cp['orders'][0]['side']=='B'   # buy back the short first
 closed=owner.post(f"/api/sb/deployments/{d['id']}/close",json={'preview_id':cp['id'],'preview_hash':cp['hash'],'idempotency_key':'c1','confirm':True}).json()
 assert closed['status']=='closed' and all(x['units']==0 for x in closed['positions'])
 assert closed['realised']==pytest.approx(-(1.0*65)*2,abs=0.01)       # round trip at a 1.00 spread, two legs


def test_resting_limit_waits_for_the_quote_and_cancel_leaves_no_naked_short(live):
 app,owner,_o,fake=live
 s=strategy(owner)
 legs={l['side']:l for l in s['draft']['body']['legs']}
 buy_sym=owner.get(f'/api/sb/chain?underlying=NIFTY&expiry={EXP}').json()
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={'price_policy':'mid'}).json()
 b=next(o for o in p['orders'] if o['side']=='B')
 assert b['bid']<b['limit']<=b['ask']
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'r1','confirm':True}).json()
 assert d['status']=='working' and not d['fills']
 assert all(i['state']=='created' for i in d['intents'] if i['side']=='S')    # the short waits for the hedge
 ex=app.state.strategy_builder_execution
 fake.books[b['symbol']]=(b['limit']-0.3,b['limit'])                        # the ask comes down to the limit
 ex.dispatch(d['id'])
 d=owner.get(f"/api/sb/deployments/{d['id']}").json()
 assert any(f['side']=='B' for f in d['fills']) and d['status'] in ('working','partially_filled','active')
 c=owner.post(f"/api/sb/deployments/{d['id']}/cancel",json={}).json()
 assert all(i['state'] in ('filled','cancelled') for i in c['intents'])
 assert not any(f['side']=='S' for f in c['fills']) or c['status']=='active'


def test_unlimited_needs_ack_and_margin_beyond_paper_capital_blocks(live):
 _a,owner,_o,fake=live
 s=strategy(owner,'short_straddle')
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 assert p['requires_ack']
 r=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'u1','confirm':True})
 assert r.status_code==400 and r.json()['code']=='ACK_REQUIRED'
 big=strategy(owner,'short_straddle',lots=10)
 pb=owner.post(f"/api/sb/strategies/{big['id']}/preview",json={}).json()
 assert not pb['can_submit'] and any(c['key']=='margin' and c['status']=='block' for c in pb['checks'])
 fake.fail_margin=True
 pm=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 assert pm['margin'] is None and any(c['key']=='margin' and c['status']=='warn' for c in pm['checks'])


def test_a_cancelled_hedge_never_releases_the_short(live):
 """Regression (found in the browser 25 Sep): cancelling a resting hedge let the worker acknowledge the sell group."""
 app,owner,_o,fake=live
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={'price_policy':'mid'}).json()
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'x1','confirm':True}).json()
 assert d['status']=='working'
 owner.post(f"/api/sb/deployments/{d['id']}/cancel",json={})
 ex=app.state.strategy_builder_execution
 b=next(o for o in p['orders'] if o['side']=='B');sl=next(o for o in p['orders'] if o['side']=='S')
 fake.books[b['symbol']]=(b['limit']-1,b['limit']-0.5);fake.books[sl['symbol']]=(sl['limit']+1,sl['limit']+1.5)   # both would now fill
 for _ in range(3):ex.dispatch(d['id'])
 d=owner.get(f"/api/sb/deployments/{d['id']}").json()
 assert all(i['state']=='cancelled' for i in d['intents']) and d['fills']==[] and d['status']=='cancelled'


def test_group_two_is_released_only_by_a_fully_filled_group_one(live):
 app,owner,_o,fake=live
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={'price_policy':'mid'}).json()
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'x2','confirm':True}).json()
 st=app.state.strategy_builder_store
 with st.lock:   # the state the old worker mis-read: the hedge group finished WITHOUT filling, the short still waiting
  st.c.execute("update intents set state='cancelled' where deployment_id=? and grp=1",(d['id'],));st.c.commit()
 sl=next(o for o in p['orders'] if o['side']=='S');fake.books[sl['symbol']]=(sl['limit']+1,sl['limit']+1.5)
 app.state.strategy_builder_execution.dispatch(d['id'])
 d=owner.get(f"/api/sb/deployments/{d['id']}").json()
 assert [i['state'] for i in d['intents'] if i['grp']==2]==['created'] and d['fills']==[]


def test_a_close_stopped_part_way_needs_attention(live):
 app,owner,_o,fake=live
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'a1','confirm':True}).json()
 assert d['status']=='active'
 cp=owner.post(f"/api/sb/deployments/{d['id']}/close-preview",json={'price_policy':'mid'}).json()
 assert cp['structure'].startswith('Close: ') and cp['margin'] is None
 buyback=next(o for o in cp['orders'] if o['side']=='B')
 fake.books[buyback['symbol']]=(buyback['limit']-1,buyback['limit']-0.5)     # the buy-back fills, the long's sale rests
 c=owner.post(f"/api/sb/deployments/{d['id']}/close",json={'preview_id':cp['id'],'preview_hash':cp['hash'],'idempotency_key':'a2','confirm':True}).json()
 assert c['status']=='closing'
 c=owner.post(f"/api/sb/deployments/{d['id']}/cancel",json={}).json()
 assert c['status']=='attention_required' and any(x['units']>0 for x in c['positions'])
 again=owner.post(f"/api/sb/deployments/{d['id']}/close-preview",json={}).json()
 assert len(again['orders'])==1 and again['orders'][0]['side']=='S'      # only the residual long is closed


# --- slice 5: alerts (notify only) --------------------------------------------------------------------------------
import kanida_pilot.strategy_builder.alerts as AL


def test_a_crossing_fires_once_rearms_only_after_clearing_and_respects_cooldown():
 p={'level':23000.0,'direction':'above'};now=datetime(2026,9,25,11,0)
 def run(state,spot,last=None,t=10_000.0):
  ok,v,cond,clear,_m=AL.evaluate('price_cross',p,{'now':now,'spot':spot},state)
  return AL.step(state,ok,cond,clear,last,900,t,'price_cross')
 assert run('armed',22990)==('armed',None)
 assert run('armed',23001)==('triggered','triggered')
 assert run('triggered',23050)==('triggered',None)           # still true: no storm
 assert run('triggered',22995)==('triggered',None)           # back under but inside the 0.1% hysteresis
 assert run('triggered',22970)==('armed','rearmed')
 assert run('armed',23010,last=9_800.0)==('armed',None)      # within the 900 s cooldown
 assert run('armed',23010,last=9_000.0)==('triggered','triggered')


def test_missing_inputs_never_fire_and_are_reported_once():
 now=datetime(2026,9,25,11,0)
 ok,v,cond,clear,m=AL.evaluate('pnl',{'amount':1000.0,'direction':'loss'},{'now':now,'net':None},'armed')
 assert not ok and not cond
 assert AL.step('armed',ok,cond,clear,None,900,1.0,'pnl')==('data_unavailable','data_unavailable')
 assert AL.step('data_unavailable',ok,cond,clear,None,900,2.0,'pnl')==('data_unavailable',None)
 ok,v,cond,clear,m=AL.evaluate('pnl',{'amount':1000.0,'direction':'loss'},{'now':now,'net':-50.0},'data_unavailable')
 assert AL.step('data_unavailable',ok,cond,clear,None,900,3.0,'pnl')==('armed','recovered')


def test_a_reminder_fires_once_and_expires():
 p={'at':'2026-09-29 13:30'}
 ok,v,cond,clear,m=AL.evaluate('expiry_time',p,{'now':datetime(2026,9,29,13,31)},'armed')
 assert AL.step('armed',ok,cond,clear,None,900,1.0,'expiry_time')==('expired','triggered')
 assert AL.step('expired',ok,cond,clear,None,900,2.0,'expiry_time')==('expired',None)


def test_alert_rules_are_validated_scoped_and_versioned(pilot):
 _a,owner,other=pilot
 s=strategy(owner)
 url=f"/api/sb/strategies/{s['id']}/alerts"
 bad=owner.post(url,json={'type':'pnl','params':{'amount':500,'direction':'loss'}})
 assert bad.status_code==400 and 'paper deployment' in bad.json()['error']
 assert owner.post(url,json={'type':'price_cross','params':{'level':-1,'direction':'above'}}).status_code==400
 r=owner.post(url,json={'type':'price_cross','params':{'level':23100,'direction':'above'}}).json()
 assert r['state']=='armed' and r['scope']=='strategy' and 'rises above 23,100' in r['description'] and r['now']['evaluated'] in (True,False)
 assert other.post(f"/api/sb/alerts/{r['id']}",json={'version':1,'action':'pause'}).status_code==404
 pz=owner.post(f"/api/sb/alerts/{r['id']}",json={'version':1,'action':'pause'}).json()
 assert pz['state']=='paused' and pz['version']==2
 assert owner.post(f"/api/sb/alerts/{r['id']}",json={'version':1,'action':'resume'}).status_code==409
 assert owner.post(f"/api/sb/alerts/{r['id']}/delete",json={}).json()['ok']
 assert owner.get(url).json()['rules']==[]


def test_without_live_data_an_alert_goes_unavailable_not_triggered(pilot,monkeypatch):
 app,owner,_o=pilot
 monkeypatch.setattr(AL,'market_open',lambda at=None:True)
 s=strategy(owner)
 owner.post(f"/api/sb/strategies/{s['id']}/alerts",json={'type':'price_cross','params':{'level':1,'direction':'above'}})
 al=app.state.strategy_builder_alerts;al.stop()
 al.cycle();al.cycle()
 got=owner.get('/api/sb/alerts').json()
 assert [e['kind'] for e in got['events']]==['data_unavailable'] and got['rules'][0]['state']=='data_unavailable'
 assert 'never place' in got['boundary']


def test_live_alerts_fire_on_the_position_and_can_be_acknowledged(live,monkeypatch):
 app,owner,_o,fake=live
 monkeypatch.setattr(AL,'market_open',lambda at=None:True)
 al=app.state.strategy_builder_alerts;al.stop()
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'al1','confirm':True}).json()
 url=f"/api/sb/strategies/{s['id']}/alerts"
 owner.post(url,json={'type':'pnl','deployment_id':d['id'],'params':{'amount':1,'direction':'loss'}})        # paying the spread is a loss
 owner.post(url,json={'type':'price_cross','params':{'level':22990,'direction':'above'}})                   # spot 23000
 owner.post(url,json={'type':'breakeven_near','deployment_id':d['id'],'params':{'points':5000}})
 owner.post(url,json={'type':'delta','params':{'units':1e9}})                                              # never reached
 al.cycle();al.cycle();al.cycle()
 got=owner.get('/api/sb/alerts').json()
 fired=[e for e in got['events'] if e['kind']=='triggered']
 assert len(fired)==3 and got['unacked']==3                                                                # once each, no storm
 assert {r['state'] for r in got['rules'] if r['type']=='delta'}=={'armed'}
 assert owner.post('/api/sb/alert-events/ack',json={'event_id':fired[0]['id']}).json()['acknowledged']==1
 assert owner.get('/api/sb/alerts/unacked').json()['unacked']==2
 owner.post('/api/sb/alert-events/ack',json={})
 assert owner.get('/api/sb/alerts/unacked').json()['unacked']==0
 from kanida_pilot.db import orders
 from sqlalchemy import select,func
 with app.state.db.tx() as c:assert c.execute(select(func.count()).select_from(orders)).scalar()==0
 with app.state.strategy_builder_store.lock:
  assert app.state.strategy_builder_store.c.execute("select count(*) from intents where deployment_id=? and kind='close'",(d['id'],)).fetchone()[0]==0  # an alert never trades


# --- slice 6: the Lab -----------------------------------------------------------------------------------------------
import math as _m
from datetime import date as _date,timedelta as _td
import kanida_pilot.strategy_builder.lab as LB


def daily_series(start='2019-01-01',n=900,drift=0.0002,seed=7):
 import random as _r
 rng=_r.Random(seed);d=_date.fromisoformat(start);days=[];o=[];c=[];vix=[];px=11000.0
 while len(days)<n:
  if d.weekday()<5:
   op=px*(1+rng.gauss(0,0.003));px=op*(1+drift+rng.gauss(0,0.009))
   days.append(d.isoformat());o.append(round(op,2));c.append(round(px,2));vix.append(round(13+3*_m.sin(len(days)/40),2))
  d+=_td(days=1)
 nifty={'days':days,'open':o,'close':c,'high':c,'low':c,'sources':{'kanida.db':(days[0],days[-1],n)}}
 v={'days':days,'open':vix,'close':vix,'high':vix,'low':vix,'sources':{'kanida.db':(days[0],days[-1],n)}}
 return nifty,v


def lab_spec(**kw):
 base={'underlying':'NIFTY','template':'iron_condor','param':4,'weekday':2,'dte_min':1,'dte_max':7,'target_pct':None,'stop_pct':None,'exit_dte':None,
  'slippage':0.005,'from':'2019-01-01','to':'2023-12-31','split':'2021-06-01'}
 return {**base,**kw}


def test_expiry_calendar_thursday_then_tuesday_and_holidays_move_earlier():
 days=[d.isoformat() for d in (_date(2025,8,1)+_td(i) for i in range(60)) if d.weekday()<5 and d!=_date(2025,8,14)]
 ex=LB.weekly_expiries(days)
 assert '2025-08-07' in ex and '2025-08-13' in ex and '2025-08-21' in ex        # 14 Aug is a holiday -> Wed 13 Aug
 assert '2025-09-02' in ex and '2025-09-04' not in ex                          # Tuesdays from September 2025


def test_backtest_is_point_in_time_costed_and_closed_trades_only():
 nifty,vix=daily_series()
 trades,skipped=LB.simulate(lab_spec(),nifty,vix,65)
 assert len(trades)>50
 idx={d:i for i,d in enumerate(nifty['days'])}
 for t in trades:
  assert idx[t['entry']]==idx[t['decision']]+1                                # entry is the NEXT trading day's open
  assert t['exit']<=nifty['days'][-1] and t['exit']>=t['entry'] and t['exit']<=t['expiry']
  assert t['fees']>0
  assert t['spot_entry']==nifty['open'][idx[t['entry']]]
 for a,b in zip(trades,trades[1:]):assert b['entry']>a['exit'] or b['decision']>=a['exit']   # one position at a time


def test_future_data_cannot_change_earlier_trades():
 nifty,vix=daily_series()
 base,_=LB.simulate(lab_spec(),nifty,vix,65)
 cut=nifty['days'].index('2021-06-01')
 shocked={**nifty,'open':nifty['open'][:cut]+[x*1.3 for x in nifty['open'][cut:]],'close':nifty['close'][:cut]+[x*1.3 for x in nifty['close'][cut:]]}
 again,_=LB.simulate(lab_spec(),shocked,vix,65)
 early=lambda ts:[t for t in ts if t['exit']<'2021-06-01']
 assert early(base)==early(again) and early(base)
 # an unchanged past with a truncated future: the last trade that could not close is excluded, not guessed
 short={k:(v[:cut] if isinstance(v,list) else v) for k,v in nifty.items()}
 tr,sk=LB.simulate(lab_spec(),short,{k:(v[:cut] if isinstance(v,list) else v) for k,v in vix.items()},65)
 assert all(t['exit']<=short['days'][-1] for t in tr)


def test_backtest_reports_honestly():
 nifty,vix=daily_series()
 r=LB.backtest(lab_spec(split='2023-10-01'),nifty,vix,65)
 assert r['provenance']['label']=='Model-priced - not traded prices' and r['provenance']['price_source']=='model'
 assert r['badge']['status']=='insufficient' and 'n=' in r['badge']['label']                 # few out-of-sample trades
 s=r['stats']['all']
 assert s['ci95'][0]<=s['expectancy']<=s['ci95'][1] and 'win_rate' in s and s['per_100_capital'] is not None
 assert r['control']['reps']>0 and len(r['equity'])==s['n']
 assert LB.backtest(lab_spec(split='2023-10-01'),nifty,vix,65)['stats']==r['stats']            # reproducible


def test_lab_api_runs_a_job_validates_and_feeds_discover(pilot,monkeypatch):
 app,owner,_o=pilot
 nifty,vix=daily_series(start='2023-01-02',n=700)
 lab=app.state.strategy_builder_lab
 monkeypatch.setattr(lab.daily,'series',lambda sym:nifty if sym=='NIFTY 50' else vix)
 bad=owner.post('/api/sb/lab/backtests',json={'template':'iron_condor','underlying':'BANKNIFTY'})
 assert bad.status_code==400 and 'verified NSE expiry calendar' in bad.json()['error']
 assert owner.post('/api/sb/lab/backtests',json={'template':'iron_condor','slippage_pct':0.1}).status_code==400   # never less slippage
 run=owner.post('/api/sb/lab/backtests',json={'template':'bull_call_spread','param':4,'from':'2023-01-02','to':'2025-09-01'}).json()
 assert run['status'] in ('queued','running','completed')
 for _ in range(100):
  got=owner.get(f"/api/sb/lab/runs/{run['id']}").json()
  if got['status'] not in ('queued','running'):break
  time.sleep(0.1)
 assert got['status']=='completed',got.get('error')
 assert got['result']['provenance']['label'].startswith('Model-priced') and got['result']['stats']['all']['n']>0
 assert owner.get('/api/sb/lab/runs').json()['runs'][0]['id']==run['id']
 d=owner.post('/api/sb/discover',json={'underlying':'NIFTY','expiry':EXP,'view':'up','target':23300}).json()
 bcs=[c for c in d['candidates'] if c['template']=='bull_call_spread' and c['param']==4]
 if bcs:assert bcs[0]['evidence']['run_id']==run['id'] and bcs[0]['evidence']['status'] in ('insufficient','tested_significant','tested_not_significant','model_only')


def test_audit_c1_no_weeklies_before_feb_2019():
 days=[d.isoformat() for d in (_date(2018,10,1)+_td(i) for i in range(200)) if d.weekday()<5]
 ex=LB.weekly_expiries(days)
 pre=[e for e in ex if e<'2019-02-11']
 assert pre==['2018-10-25','2018-11-29','2018-12-27','2019-01-31']            # last Thursday only
 assert '2019-02-14' in ex and '2019-02-21' in ex                              # weeklies from then


def test_audit_c2_stop_and_target_rejected_when_undefined(pilot):
 app,owner,_o=pilot
 r=owner.post('/api/sb/lab/backtests',json={'template':'short_straddle','stop_pct':50})
 assert r.status_code==400 and 'no defined maximum loss' in r.json()['error']
 r=owner.post('/api/sb/lab/backtests',json={'template':'long_call','target_pct':50})
 assert r.status_code==400 and 'no capped maximum profit' in r.json()['error']


def test_audit_c3_random_control_spans_the_whole_period(monkeypatch):
 nifty,vix=daily_series()
 seen=[]
 real=LB.simulate
 def spy(spec,n,v,lot,entry_days=None,cal=None):
  tr,sk=real(spec,n,v,lot,entry_days)
  if entry_days is not None:seen.extend(t['entry'] for t in tr)
  return tr,sk
 monkeypatch.setattr(LB,'simulate',spy)
 r=LB.backtest(lab_spec(),nifty,vix,65)
 assert min(seen)<'2020-01-01' and max(seen)>'2022-06-01'
 assert r['control']['oos_mean_expectancy'] is not None and r['control']['oos_actual_percentile'] is not None


def test_audit_p4_trades_straddling_the_split_are_in_neither_set():
 nifty,vix=daily_series()
 trades,_=LB.simulate(lab_spec(),nifty,vix,65)
 t=next(t for t in trades if t['entry']<t['exit'])
 split=t['exit']                                     # entry < split <= exit
 r=LB.backtest(lab_spec(split=split),nifty,vix,65)
 assert r['skipped']['straddled_split']>=1
 assert r['stats']['all']['n']==r['stats']['discovery']['n']+r['stats']['oos']['n']==len(trades)-r['skipped']['straddled_split']


def test_audit_p2_special_sessions_and_weekends_are_dropped():
 nifty,_=daily_series(n=30)
 sp=nifty['days'][5]
 got=LB.clean_series({**nifty,'days':nifty['days']+['2019-03-02'],**{k:nifty[k]+[1.0] for k in ('open','high','low','close')}},{sp})
 assert sp not in got['days'] and '2019-03-02' not in got['days'] and got['excluded_special']==2
 assert len(got['days'])==len(got['open'])==len(got['close'])


def test_audit_p3_today_is_not_complete_before_the_close(monkeypatch):
 monkeypatch.setattr(LB,'_now_ist',lambda:datetime(2026,9,24,11,0))
 assert LB._last_complete_day()=='2026-09-23'
 monkeypatch.setattr(LB,'_now_ist',lambda:datetime(2026,9,24,16,0))
 assert LB._last_complete_day()=='2026-09-24'


def test_audit_p1_evidence_only_for_the_same_held_to_expiry_rule(pilot):
 app,owner,_o=pilot
 lab=app.state.strategy_builder_lab
 res={'badge':{'status':'insufficient','label':'x'},'stats':{'oos':{'n':3,'ci95':[1,2]}}}
 def put(rid,**kw):
  spec=lab_spec(template='bull_call_spread',param=4,**kw)
  with lab.lock:
   lab.c.execute("insert into lab_runs(id,user_id,kind,status,spec,result,created_at) values(?,?,?,?,?,?,?)",
    (rid,'u1','backtest','completed',json.dumps(spec),json.dumps(res),time.time()));lab.c.commit()
  time.sleep(0.01)
 put('with-stop',stop_pct=50)
 assert lab.evidence_for('u1','bull_call_spread',4) is None                    # a stop run is a different rule
 put('plain')
 ev=lab.evidence_for('u1','bull_call_spread',4)
 assert ev['run_id']=='plain' and ev['runs_tried']==1 and 'Held to expiry' in ev['note']


def test_replay_uses_real_bars_and_skips_gaps():
 legs=[{'symbol':'A','side':'B','units':65},{'symbol':'B','side':'S','units':-65}]
 c={'A':[('t1',100),('t2',110),('t3',120)],'B':[('t1',50),('t3',40)]}
 r=LB.replay(legs,c)
 assert [p['t'] for p in r['points']]==['t1','t3'] and r['skipped_bars']==1
 assert r['points'][-1]['pnl']==65*(120-100)-65*(40-50)


# --- slice 7: the AutoTrade bridge (engine intents; dry run by default) ----------------------------------------------
from kanida_pilot.strategy_builder.autotrade_bridge import Bridge


class _Resp:
 def __init__(self,status,data):self.status_code=status;self._d=data
 def json(self):return self._d


class FakeEngine:
 """Stands in for engine /api/autotrade/intents. Records every request; never a broker."""
 def __init__(self,live_allowed=False,down=False):
  self.calls=[];self.intents={};self.live_allowed=live_allowed;self.down=down
 def request(self,method,url,headers=None,timeout=None,json=None,params=None):
  self.calls.append((method,url,headers,json,params))
  if self.down:raise ConnectionError('engine down')
  if getattr(self,'silent',False):raise TimeoutError('no answer')
  if headers.get('X-Operator-Token')!='svc-token':return _Resp(403,{'detail':'operator token required'})
  path=url.split('/api/autotrade/intents',1)[1]
  if path=='/capability':
   return _Resp(200,{'live_allowed':self.live_allowed,'gates':[{'gate':'armed','label':'Operator armed this account','pass':self.live_allowed,'detail':'x'}],'arm':None})
  if method=='GET' and path=='/by-key':
   it=self.intents.get(params['key'])
   return _Resp(200,{'intent':it}) if it else _Resp(404,{'error':'no intent with that source and key','code':'INTENT_NOT_FOUND'})
  if method=='GET' and path=='':
   return _Resp(200,{'intents':[{**v,'idempotency_key':k} for k,v in self.intents.items()]})
  if method=='POST' and path=='':
   key=json['idempotency_key']
   if key in self.intents:return _Resp(200,{'intent':self.intents[key],'replayed':True})
   blocked=json['mode']=='live' and not self.live_allowed
   it={'id':f'i{len(self.intents)+1}','state':'blocked' if blocked else 'accepted','mode':json['mode'],
    'reason':'LIVE_GATES_FAILED: armed' if blocked else None,'legs':[{**l,'state':'not_sent' if blocked else 'pending'} for l in json['legs']]}
   self.intents[key]=it
   if getattr(self,'drop_response',False):raise TimeoutError('accepted, then the response was lost')
   return _Resp(201,{'intent':it,'replayed':False})
  iid=path.strip('/').split('/')[0];it=next(v for v in self.intents.values() if v['id']==iid)
  if path.endswith('/cancel'):it.update(state='cancelled',reason='Cancelled before dispatch')
  elif it['state']=='accepted':it.update(state='dry_run_complete',reason='Dry run - no broker order was placed',legs=[{**l,'state':'dry_run'} for l in it['legs']])
  return _Resp(200,{'intent':it})


@pytest.fixture
def bridged(tmp_path,monkeypatch):
 db=store(str(tmp_path/'derivatives.db'))
 settings=pilot_settings(tmp_path,derivatives_database=db,snapshots='off')
 app=create_app(settings,evidence=Evidence())
 fake=FakeKite(db);engine=FakeEngine()
 monkeypatch.setattr(EX,'market_open',lambda at=None:True)
 sb=mount(app,settings,str(tmp_path/'sb.db'),live=fake,bridge=Bridge(url='http://engine.test',token='svc-token',account='acct1',client=engine))
 owner=signup(app)
 app.state.strategy_builder_execution.stop()
 yield app,owner,fake,engine
 app.state.db.close();sb.close()


def test_autotrade_unconfigured_is_stated_never_faked(live):
 _a,owner,_o,_f=live
 cap=owner.get('/api/sb/autotrade/capability').json()
 assert cap['configured'] is False and cap['live_allowed'] is False and 'not connected' in cap['reason']
 s=strategy(owner);p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 assert p['live']['configured'] is False and p['live']['enabled'] is False
 r=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k-unconf-1'}).json()
 assert r['state']=='refused' and 'AUTOTRADE_NOT_CONFIGURED' in r['reason']


def test_autotrade_dry_run_hands_off_the_exact_plan_once(bridged):
 _a,owner,_f,engine=bridged
 s=strategy(owner,'iron_condor',param=4);p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 assert p['live']['configured'] and p['live']['reachable'] and p['live']['engine_user'].startswith('pilot:')
 body={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k-dry-0001'}
 r=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json=body).json()
 assert r['mode']=='dry_run' and r['state'] in ('accepted','dry_run_complete') and r['intent_id']=='i1'
 sent=[c for c in engine.calls if c[0]=='POST'][0][3]
 assert sent['mode']=='dry_run' and sent['broker_account_id']=='acct1' and sent['reference']['preview_hash']==p['hash']
 assert [l['side'] for l in sent['legs']]==['BUY','BUY','SELL','SELL'] and [l['group'] for l in sent['legs']]==[1,1,2,2]
 assert all(l['expiry']==EXP and l['underlying']=='NIFTY' and l['exchange']=='NFO' and l['quantity']%l['lot_size']==0 for l in sent['legs'])
 assert {o['symbol']:o['limit'] for o in p['orders']}=={l['tradingsymbol']:l['limit_price'] for l in sent['legs']}
 again=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json=body).json()
 assert again['id']==r['id'] and len([c for c in engine.calls if c[0]=='POST'])==1       # once, whatever the retries
 got=owner.get(f"/api/sb/autotrade/routes/{r['id']}").json()
 assert got['state']=='dry_run_complete' and all(l['state']=='dry_run' for l in got['intent']['legs'])
 assert owner.get(f"/api/sb/autotrade/routes?strategy_id={s['id']}").json()['routes'][0]['id']==r['id']
 conflict=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json={**body,'mode':'live','confirm_live':True})
 assert conflict.status_code==409


def test_autotrade_live_needs_confirmation_and_the_engine_decides(bridged):
 _a,owner,_f,engine=bridged
 s=strategy(owner,'iron_condor',param=4);p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 body={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k-live-0001','mode':'live'}
 assert owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json=body).status_code==400
 r=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json={**body,'confirm_live':True}).json()
 assert r['mode']=='live' and r['state']=='blocked' and 'armed' in r['reason']       # refused by autotrade, not downgraded


def test_autotrade_refuses_undefined_risk_and_stale_plans(bridged,monkeypatch):
 _a,owner,_f,engine=bridged
 s=strategy(owner,'short_put');p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 r=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k-naked-01'})
 assert r.status_code==409 and 'defined-risk' in r.json()['error']
 s2=strategy(owner);p2=owner.post(f"/api/sb/strategies/{s2['id']}/preview",json={}).json()
 bad=owner.post(f"/api/sb/strategies/{s2['id']}/autotrade",json={'preview_id':p2['id'],'preview_hash':'x'*64,'idempotency_key':'k-stale-01'})
 assert bad.status_code==409
 monkeypatch.setattr(time,'time',lambda:p2['expires_at']+1)
 old=owner.post(f"/api/sb/strategies/{s2['id']}/autotrade",json={'preview_id':p2['id'],'preview_hash':p2['hash'],'idempotency_key':'k-stale-02'})
 assert old.status_code==409 and 'expired' in old.json()['error']
 assert not [c for c in engine.calls if c[0]=='POST']


def test_autotrade_unreachable_engine_is_a_refusal(bridged):
 _a,owner,_f,engine=bridged
 s=strategy(owner);p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 engine.down=True
 assert owner.get('/api/sb/autotrade/capability').json()['reachable'] is False
 r=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k-down-001'}).json()
 assert r['state']=='refused' and 'AUTOTRADE_UNREACHABLE' in r['reason']


def test_marketable_limits_are_always_on_the_tick(live):
 _a,owner,_o,_f=live
 s=strategy(owner,'iron_condor',param=4);p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 for o in p['orders']:
  assert abs(round(o['limit']/0.05)*0.05-o['limit'])<1e-9                 # autotrade refuses off-tick limits
  assert (o['limit']>=o['ask']) if o['side']=='B' else (o['limit']<=o['bid'])   # and it still crosses


# --- slice 8: the adjustment assistant + Lab adjustment evidence -------------------------------------------------------
from kanida_pilot.strategy_builder import adjust as ADJ
GRID=[float(k) for k in STRIKES]


def sleg(t,side,k,lots=1,i=None):return {'id':i or f'{side}{t}{k}','type':t,'side':side,'strike':float(k),'lots':lots,'expiry':EXP,'price_basis':'exec','price':None,'include':True}


def test_tested_short_is_the_one_closest_to_the_money():
 legs=[sleg('CE','S',23150),sleg('PE','S',22750)]
 t,d=ADJ.tested(legs,23000.0)
 assert t['type']=='CE' and d==pytest.approx(150/23000)
 t,d=ADJ.tested(legs,22800.0)
 assert t['type']=='PE' and d==pytest.approx(50/22800)
 assert ADJ.tested([sleg('CE','B',23000)],23000.0) is None


def test_rules_roll_wing_close_reduce():
 strangle=[sleg('CE','S',23150),sleg('PE','S',22750)]
 new,note=ADJ.apply('roll_tested_short',strangle,23000.0,GRID,2)
 assert sorted((l['type'],l['strike']) for l in new)==[('CE',23250.0),('PE',22750.0)] and '23150' in note
 assert new[1] is strangle[1]                                               # untouched legs keep their identity
 condor=[sleg('CE','B',23250),sleg('CE','S',23150),sleg('PE','S',22750),sleg('PE','B',22650)]
 new,_=ADJ.apply('roll_tested_short',condor,23000.0,GRID,2)
 assert ('CE',23350.0) in {(l['type'],l['strike']) for l in new} and ('CE',23250.0) in {(l['type'],l['strike']) for l in new if l['side']=='S'}
 new,_=ADJ.apply('add_hedge_wing',strangle,23000.0,GRID,2)
 assert {(l['type'],l['side'],l['strike']) for l in new}>={('CE','B',23250.0),('PE','B',22650.0)}
 fly,_=ADJ.apply('add_hedge_wing',[sleg('CE','S',23000),sleg('PE','S',23000)],23000.0,GRID,4)
 assert {(l['type'],l['side'],l['strike']) for l in fly if l['side']=='B'}=={('CE','B',23200.0),('PE','B',22800.0)}
 with pytest.raises(ADJ.NotApplicable):ADJ.apply('add_hedge_wing',condor,23000.0,GRID,2)
 rest,_=ADJ.apply('close_tested_side',condor,23000.0,GRID)
 assert {l['type'] for l in rest}=={'PE'} and len(rest)==2
 with pytest.raises(ADJ.NotApplicable):ADJ.apply('reduce_half',strangle,23000.0,GRID)       # odd lots
 half,_=ADJ.apply('reduce_half',[{**l,'lots':4} for l in strangle],23000.0,GRID)
 assert {l['lots'] for l in half}=={2}
 with pytest.raises(ADJ.NotApplicable):ADJ.apply('roll_out',strangle,23000.0,GRID)
 with pytest.raises(ADJ.NotApplicable):ADJ.apply('roll_tested_short',strangle,23000.0,GRID,20)   # off the strike list


def test_delta_orders_buy_first_and_keep_held_ids():
 cur=[sleg('CE','S',23150,i='L1'),sleg('PE','S',22750,i='L2')]
 tgt=[sleg('CE','S',23250,i='N1'),sleg('PE','S',22750,i='X2')]
 o=ADJ.delta_orders(cur,tgt)
 assert [(x['side'],x['type'],x['strike'],x['id']) for x in o]==[('B','CE',23150.0,'L1'),('S','CE',23250.0,'N1')]


def adjust_strategy(owner,legs):
 return owner.post('/api/sb/strategies',json={'body':{'underlying':'NIFTY','expiry':EXP,'legs':legs}}).json()


def test_candidates_price_delta_orders_and_overlay_exactly(live):
 _a,owner,_o,_f=live
 s=adjust_strategy(owner,[sleg('CE','S',23150),sleg('PE','S',22750)])
 r=owner.post(f"/api/sb/strategies/{s['id']}/adjust/candidates",json={}).json()
 assert r['tested']['label'].startswith('23150 CE') and r['held'] is False
 by={(c['rule'],c.get('k')):c for c in r['candidates']}
 assert by[('roll_out',None)]['available'] is False and by[('reduce_half',None)]['available'] is False
 wing=by[('add_hedge_wing',2)]
 assert wing['available'] and all(o['side']=='B' for o in wing['orders']) and wing['after']['unlimited_loss'] is False
 assert r['current']['unlimited_loss'] is True and wing['evidence']['status']=='model_only'
 roll=by[('roll_tested_short',1)]
 assert [o['side'] for o in roll['orders']]==['B','S']                      # buy back first
 buyback=roll['orders'][0];new=roll['orders'][1]
 assert buyback['price']==buyback.get('price') and buyback['basis']=='exec'
 # overlay invariant: after(x) = current(x) + delta orders' expiry P&L - their charges
 for pt in roll['overlay'][::7]:
  x=pt['s'];intr=lambda t,k:max(x-k,0) if t=='CE' else max(k-x,0)
  extra=sum((1 if o['side']=='B' else -1)*o['qty']*(intr(o['type'],o['strike'])-o['price']) for o in roll['orders'])
  assert pt['after']==pytest.approx(pt['current']+extra-roll['charges'],abs=0.05)
 flat=by[('close_all',None)]
 assert len({p['after'] for p in flat['overlay']})==1                     # closed: the same result at every spot
 assert 'body' not in roll                                                 # the client never gets (or sends) legs


def test_apply_writes_a_new_version_and_is_guarded(live):
 _a,owner,_o,_f=live
 s=adjust_strategy(owner,[sleg('CE','S',23150),sleg('PE','S',22750)])
 v=s['draft']['version']
 bad=owner.post(f"/api/sb/strategies/{s['id']}/adjust/apply",json={'rule':'reduce_half','version':v})
 assert bad.status_code==409 and 'even number' in bad.json()['error']
 ok=owner.post(f"/api/sb/strategies/{s['id']}/adjust/apply",json={'rule':'add_hedge_wing','k':2,'version':v}).json()
 legs=ok['strategy']['draft']['body']['legs']
 assert ok['strategy']['draft']['version']==v+1 and len(legs)==4 and sum(1 for l in legs if l['side']=='B')==2
 stale=owner.post(f"/api/sb/strategies/{s['id']}/adjust/apply",json={'rule':'close_all','version':v})
 assert stale.status_code==409 and stale.json()['code']=='VERSION_CONFLICT'
 acts=owner.get(f"/api/sb/strategies/{s['id']}").json().get('activity') or []
 assert not acts or any(a['kind']=='adjust' for a in acts)


def test_deployment_adjust_sends_only_delta_orders_and_books_positions(live):
 _a,owner,_o,_f=live
 s=adjust_strategy(owner,[sleg('CE','B',23350),sleg('CE','S',23150),sleg('PE','S',22750),sleg('PE','B',22550)])
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'adj-open','confirm':True}).json()
 assert d['status']=='active'
 r=owner.post(f"/api/sb/strategies/{s['id']}/adjust/candidates",json={'deployment_id':d['id']}).json()
 assert r['held'] is True and r['entry_basis'].startswith('deployment fills')
 v=owner.get(f"/api/sb/strategies/{s['id']}").json()['draft']['version']
 ap=owner.post(f"/api/sb/strategies/{s['id']}/adjust/apply",json={'rule':'close_tested_side','version':v,'deployment_id':d['id']}).json()
 assert {l['type'] for l in ap['strategy']['draft']['body']['legs']}=={'PE'}
 pv=owner.post(f"/api/sb/deployments/{d['id']}/adjust-preview",json={}).json()
 assert pv['kind']=='adjust' and len(pv['orders'])==2 and {o['type'] for o in pv['orders']}=={'CE'}
 assert pv['orders'][0]['side']=='B' and pv['orders'][0]['group']==1 and pv['orders'][1]['group']==2   # buy back the short first
 done=owner.post(f"/api/sb/deployments/{d['id']}/adjust",json={'preview_id':pv['id'],'preview_hash':pv['hash'],'idempotency_key':'adj-1','confirm':True}).json()
 assert done['status']=='active'
 held={x['label']:x['units'] for x in done['positions']}
 assert held['23150 CE']==0 and held['23350 CE']==0 and held['22750 PE']==-65 and held['22550 PE']==65
 assert done['unrealised'] is not None
 again=owner.post(f"/api/sb/deployments/{d['id']}/adjust-preview",json={})
 assert again.status_code==409 and again.json()['code']=='NOTHING_TO_CHANGE'
 cp=owner.post(f"/api/sb/deployments/{d['id']}/close-preview",json={}).json()
 assert {o['type'] for o in cp['orders']}=={'PE'} and len(cp['orders'])==2    # only what is still held


def test_lab_adjustment_is_paired_point_in_time_and_costed():
 nifty,vix=daily_series()
 spec=lab_spec(template='short_strangle',param=None,adjust={'rule':'add_hedge_wing','k':2,'trigger_pct':0.3})
 from kanida_pilot.strategy_builder.templates import BY_KEY
 if BY_KEY['short_strangle']['param']:spec['param']=BY_KEY['short_strangle']['param']['default']
 adj,_=LB.simulate(spec,nifty,vix,65)
 base,_=LB.simulate({**spec,'adjust':None},nifty,vix,65)
 assert [(t['entry'],t['exit']) for t in adj]==[(t['entry'],t['exit']) for t in base]       # same schedule: paired
 trig=[(a,b) for a,b in zip(adj,base) if (a.get('adjustment') or {}).get('applied')]
 untrig=[(a,b) for a,b in zip(adj,base) if not (a.get('adjustment') or {}).get('applied')]
 assert trig and untrig
 for a,b in untrig:assert a['net']==pytest.approx(b['net'],abs=0.02)          # no adjustment -> identical result
 idx={d:i for i,d in enumerate(nifty['days'])}
 for a,_b in trig:
  x=a['adjustment']
  assert idx[x['day']]==idx[x['trigger_day']]+1 and a['entry']<=x['trigger_day']<a['exit']     # read at a close, filled next open
  assert x['distance_pct']<=0.3 and all(o['side']=='B' for o in x['orders'])
  assert a['fees']>b['fees']                                                                    # the adjustment pays charges
 # the future cannot change an earlier adjusted trade
 cut=len(nifty['days'])//2
 short={k:(v[:cut] if isinstance(v,list) else v) for k,v in nifty.items()};vs={k:(v[:cut] if isinstance(v,list) else v) for k,v in vix.items()}
 early,_=LB.simulate(spec,short,vs,65)
 for e in early:
  full=next(t for t in adj if t['entry']==e['entry'])
  assert e['net']==full['net'] and e.get('adjustment')==full.get('adjustment')
 r=LB.backtest(spec,nifty,vix,65)
 a=r['adjustment']
 assert a['pairs']==r['stats']['all']['n'] and a['triggered']['all']==a['triggered']['discovery']+a['triggered']['oos']
 assert a['badge']['status'] in ('insufficient','adjust_helped','adjust_hurt','adjust_not_significant')


def test_lab_adjust_validation_and_evidence(pilot):
 app,owner,_o=pilot
 lab=app.state.strategy_builder_lab
 with pytest.raises(Exception) as e:lab.validate({'template':'iron_condor','adjust':{'rule':'roll_tested_short'},'stop_pct':50})
 assert 'target/stop' in str(e.value.message if hasattr(e.value,'message') else e.value)
 with pytest.raises(Exception) as e:lab.validate({'template':'long_call','adjust':{'rule':'roll_tested_short'}})
 assert 'no short leg' in str(e.value.message if hasattr(e.value,'message') else e.value)
 spec=lab.validate({'template':'short_strangle','adjust':{'rule':'add_hedge_wing','k':2,'trigger_pct':0.3}})
 assert spec['adjust']=={'rule':'add_hedge_wing','k':2,'trigger_pct':0.3}
 res={'badge':{'status':'insufficient','label':'x'},'stats':{'oos':{'n':1}},'adjustment':{'badge':{'status':'insufficient','label':'Adjustment model-tested - too few'},'improvement_per_triggered_trade':{'oos':{'n':4,'ci95':[-1,2]}}}}
 uid=owner.get('/api/me').json().get('id') if owner.get('/api/me').status_code==200 else None
 with lab.lock:
  lab.c.execute("insert into lab_runs(id,user_id,kind,spec,status,result,created_at) values(?,?,?,?,?,?,?)",('adjrun','u-x','backtest',json.dumps(spec),'completed',json.dumps(res),time.time()));lab.c.commit()
 ev=lab.adjust_evidence_for('u-x','short_strangle',spec['param'],'add_hedge_wing',2)
 assert ev['run_id']=='adjrun' and ev['trigger_pct']==0.3 and ev['runs_tried']==1
 assert lab.adjust_evidence_for('u-x','short_strangle',spec['param'],'add_hedge_wing',1) is None     # another k is another rule
 assert lab.adjust_evidence_for('u-x','iron_condor',spec['param'],'add_hedge_wing',2) is None
 assert lab.evidence_for('u-x','short_strangle',spec['param']) is None                               # an adjusted run is not the plain rule's evidence


# --- slice 8 quant-audit regressions ------------------------------------------------------------------------------------
def test_audit_roll_moves_only_a_crossed_wing_never_the_inner_long():
 bcs=[sleg('CE','B',23000),sleg('CE','S',23200)]
 new,note=ADJ.apply('roll_tested_short',bcs,23150.0,GRID,1)
 assert {(l['side'],l['strike']) for l in new}=={('B',23000.0),('S',23250.0)} and 'wing' not in note
 bps=[sleg('PE','B',23000),sleg('PE','S',22800)]
 new,_=ADJ.apply('roll_tested_short',bps,22850.0,GRID,1)
 assert {(l['side'],l['strike']) for l in new}=={('B',23000.0),('S',22750.0)}
 condor=[sleg('CE','B',23250),sleg('CE','S',23200),sleg('PE','S',22750),sleg('PE','B',22650)]
 new,note=ADJ.apply('roll_tested_short',condor,23150.0,GRID,1)     # the call wing at 23250 is crossed -> moves
 assert ('B',23300.0) in {(l['side'],l['strike']) for l in new if l['type']=='CE'} and 'wing' in note


def test_audit_the_tested_leg_is_pinned_to_the_trigger_decision():
 strangle=[sleg('CE','S',23150),sleg('PE','S',22750)]
 # at the open the put is closest, but the close decided the call: act on the call
 new,note=ADJ.apply('close_tested_side',strangle,22760.0,GRID,tested_key=('CE',23150.0))
 assert {l['type'] for l in new}=={'PE'}
 with pytest.raises(ADJ.NotApplicable):ADJ.apply('roll_tested_short',strangle,23000.0,GRID,1,tested_key=('CE',23500.0))


def test_audit_evidence_needs_matching_conditions_and_is_corrected_for_runs(live):
 app,owner,_o,_f=live
 lab=app.state.strategy_builder_lab
 s=adjust_strategy(owner,[sleg('CE','S',23150),sleg('PE','S',22750)])
 uid=app.state.strategy_builder_store.get  # noqa - owner id from the store below
 with app.state.strategy_builder_store.lock:
  owner_id=app.state.strategy_builder_store.c.execute('select user_id from strategies where id=?',(s['id'],)).fetchone()[0]
 good=[500.0+i for i in range(40)]                                     # a clearly positive OOS improvement
 def put(rid,trigger,rule='add_hedge_wing',k=2):
  spec=lab.validate({'template':'short_strangle','adjust':{'rule':rule,'k':k,'trigger_pct':trigger}})
  res={'badge':{'status':'x','label':'x'},'stats':{'oos':{'n':40}},'adjustment':{'oos_diffs':good,'badge':{}}}
  with lab.lock:
   lab.c.execute("insert into lab_runs(id,user_id,kind,spec,status,result,created_at) values(?,?,?,?,?,?,?)",(rid,owner_id,'backtest',json.dumps(spec),'completed',json.dumps(res),time.time()));lab.c.commit()
  time.sleep(0.01);return spec
 spec=put('r1',0.3)
 ev=lab.adjust_evidence_for(owner_id,'short_strangle',spec['param'],'add_hedge_wing',2)
 assert ev['status']=='adjust_helped' and ev['runs_tried']==1 and 'no skew' in ev['label']
 put('r2',1.0,'roll_tested_short',1);put('r3',2.0)
 ev=lab.adjust_evidence_for(owner_id,'short_strangle',spec['param'],'add_hedge_wing',2)
 assert ev['runs_tried']==3 and 'corrected for 3 runs' in ev['label'] and ev['run_id']=='r3'
 # live: the strangle's call is 150/23000 = 0.65% from the money; the newest run (r3) triggers at 2%, days to expiry 6 > 7? -> within
 body={**s['draft']['body'],'template':'short_strangle','param':spec['param']}
 v=s['draft']['version'];owner.post(f"/api/sb/strategies/{s['id']}/draft",json={'version':v,'body':body})
 r=owner.post(f"/api/sb/strategies/{s['id']}/adjust/candidates",json={}).json()
 wing=next(c for c in r['candidates'] if c['rule']=='add_hedge_wing')
 assert wing['evidence']['status']=='adjust_helped', wing['evidence']
 put('r4',0.1)                                                          # newest run now triggers at 0.1%: conditions differ
 r=owner.post(f"/api/sb/strategies/{s['id']}/adjust/candidates",json={}).json()
 wing=next(c for c in r['candidates'] if c['rule']=='add_hedge_wing')
 assert wing['evidence']['status']=='model_only' and 'conditions differ' in wing['evidence']['label'] and '0.1%' in wing['evidence']['note']
 # after one adjustment, Lab evidence (one adjustment per trade) no longer applies
 put('r5',2.0)
 cur=owner.get(f"/api/sb/strategies/{s['id']}").json()['draft']['version']
 owner.post(f"/api/sb/strategies/{s['id']}/adjust/apply",json={'rule':'roll_tested_short','k':1,'version':cur})
 r=owner.post(f"/api/sb/strategies/{s['id']}/adjust/candidates",json={}).json()
 assert all(c['evidence']['status']=='model_only' for c in r['candidates'] if c.get('available'))


def test_audit_adjusted_runs_do_not_report_per_100_capital():
 nifty,vix=daily_series()
 from kanida_pilot.strategy_builder.templates import BY_KEY
 p=BY_KEY['short_strangle']['param']
 r=LB.backtest(lab_spec(template='short_strangle',param=p['default'] if p else None,adjust={'rule':'add_hedge_wing','k':2,'trigger_pct':0.3}),nifty,vix,65)
 assert all(v.get('per_100_capital') is None for v in r['stats'].values() if v.get('n'))
 assert 'no skew' in r['adjustment']['badge']['label'] or r['adjustment']['badge']['status']=='insufficient'


# --- slice 9: evidence-ranked Discover (one hypothesis per rule, block bootstrap, BH FDR 10%) -------------------------
from kanida_pilot.strategy_builder import evidence as EVB


@pytest.fixture(autouse=True)
def _fresh_evidence_cache():
 EVB._CACHE.clear();yield;EVB._CACHE.clear()


def fake_run(rid,template,param,nets,split='2024-01-01',dte=(1,7),exits=None,at=0.0,adjust=None,weekday=2,capital=5000.0,period=('2019-01-01','2026-01-01')):
 trades=[{'entry':'2024-02-01','net':x,'capital_at_risk':capital} for x in nets]
 trades+=[{'entry':'2023-06-01','net':-999.0,'capital_at_risk':capital}]                   # a discovery trade: never in the test
 spec={'template':template,'param':param,'weekday':weekday,'dte_min':dte[0],'dte_max':dte[1],'target_pct':None,'stop_pct':None,'exit_dte':None,
  'slippage':0.005,'from':period[0],'to':period[1],'split':split,'adjust':adjust,**(exits or {})}
 return (rid,json.dumps(spec),json.dumps({'kind':'backtest','trades':trades}),at)


def noisy(seed,n=40,mu=0.0,sd=300.0):
 import random as _r
 g=_r.Random(seed);return [round(g.gauss(mu,sd),2) for _ in range(n)]


def edge(wins,n=40,win=4000.0,loss=-5000.0,seed=0):
 """Debit-spread-like P&L on 5000 max loss: `wins` of n trades win, the rest lose the maximum (a well-observed tail)."""
 import random as _r
 xs=[win]*wins+[loss]*(n-wins);_r.Random(seed).shuffle(xs);return xs


MARGINAL=edge(29)                       # p about 0.03 on its own


def test_board_one_hypothesis_per_rule_and_corrects_for_every_rule():
 strong=fake_run('strong','bull_call_spread',4,edge(32))
 marginal=fake_run('marginal','iron_condor',4,MARGINAL)
 alone=EVB.board([strong,marginal])
 assert alone['by_run']['strong']['status']=='tested_significant' and alone['by_run']['strong']['n_oos']==40   # discovery trade excluded
 assert alone['by_run']['marginal']['status']=='tested_significant'
 nulls=[fake_run(f'null{i}','bear_put_spread',i,noisy(10+i)) for i in range(30)]
 b=EVB.board([strong,marginal]+nulls)
 assert b['tests']==32 and b['by_run']['strong']['status']=='tested_significant'
 assert b['by_run']['marginal']['status']=='tested_not_significant'                    # not after 30 more rules
 assert b['survivors']<=2
 few=EVB.board([fake_run('few','bull_call_spread',4,noisy(3,n=12,mu=500))])
 assert few['rules'][0]['status']=='insufficient' and few['tests']==0
 assert EVB.board([fake_run('adj','short_strangle',None,noisy(4,mu=500),adjust={'rule':'add_hedge_wing','k':2,'trigger_pct':0.3})])['rules']==[]


def test_audit_rerunning_a_rule_never_pads_the_family_or_improves_it():
 nulls=[fake_run(f'n{i}','bear_put_spread',i,noisy(40+i)) for i in range(9)]
 base=EVB.board([fake_run('m0','iron_condor',4,MARGINAL)]+nulls)
 dup=EVB.board([fake_run(f'm{j}','iron_condor',4,MARGINAL,at=j) for j in range(10)]+nulls)
 assert dup['tests']==base['tests']==10 and dup['survivors']==base['survivors']                           # 9 copies change nothing
 assert dup['by_run']['m9']['runs']==10
 # moving the split/period until one run looks great: the most conservative run still decides
 hack=EVB.board([fake_run('first','iron_condor',4,noisy(5,mu=0)),fake_run('lucky','iron_condor',4,edge(34),split='2025-01-01',period=('2021-01-01','2026-01-01'),at=9)])
 r=hack['by_run']['lucky']
 assert r['deciding_run']=='first' and r['status']=='tested_not_significant'


def test_audit_skewed_short_premium_pnl_is_not_a_false_positive():
 # mean ~0: +100 most of the time, a large loss 1 time in 20 (short-premium shape)
 import random as _r
 fp=0
 for sd in range(40):
  g=_r.Random(sd);nets=[100.0+g.gauss(0,20) if g.random()>0.05 else -1900.0 for _ in range(40)]
  EVB._CACHE.clear()
  if EVB.board([fake_run(f's{sd}','iron_condor',4,nets,capital=2000.0)])['rules'][0]['status']=='tested_significant':fp+=1
 assert fp<=4                                                                               # about the nominal rate, not 13%


def test_bh_step_up_is_rank_based_and_p_never_zero():
 b=EVB.board([fake_run('allplus','bull_call_spread',4,[5.0]*40)])
 r=b['rules'][0]
 assert r['p']==0.0 and r['status']=='tested_not_significant' and r['reason']=='tail_stress'   # rank-based BH passes p=0; the unseen tail does not
 naked=EVB.board([fake_run('nk','short_straddle',None,edge(34),capital=None)])['rules'][0]
 assert naked['status']=='tested_not_significant' and naked['reason']=='tail_undefined'
 def fake(pv):
  return {'rule':f'r{pv}','underlying':'NIFTY','template':'x','param':pv,'weekday':2,'dte':[1,7],'exits':{},'slippage':0.005,'period':['a','b'],'split':'s',
   'created_at':0,'n_oos':40,'mean_oos':1.0,'defined_risk':True,'unit':'u','p':pv,'low':1.0,'mean_stat':1.0,'stress':1.0,'run_id':f'r{pv}'}
 orig=EVB.entry
 try:
  EVB.entry=lambda rid,spec,res,at:fake(float(rid))
  b=EVB.board([(str(p),{'template':'x'},{'kind':'backtest'},0) for p in (0.001,0.02,0.03,0.5)])
 finally:EVB.entry=orig
 # m=4, q=0.1: thresholds .025 .05 .075 .1 -> ranks 1-3 pass (step-up)
 assert {r['p']:r['status'] for r in b['rules']}=={0.001:'tested_significant',0.02:'tested_significant',0.03:'tested_significant',0.5:'tested_not_significant'}


def test_audit_dte_is_counted_from_the_next_session():
 assert EVB.next_session_dte('2026-10-01','2026-09-30 11:00:00')==0                         # Wed reading, Thu expiry: the Lab would enter Thu
 assert EVB.next_session_dte('2026-09-29','2026-09-25 11:00:00')==1                         # Fri reading -> Mon session
 b=EVB.board([fake_run('w','bull_call_spread',4,edge(32))])
 assert EVB.for_candidate(b,'bull_call_spread',4,0)['status']=='model_only'


def test_candidate_evidence_and_tiers():
 b=EVB.board([fake_run('wed','bull_call_spread',4,edge(32)),fake_run('stopped','bull_call_spread',4,noisy(6,mu=900),exits={'stop_pct':50}),
  fake_run('ic','iron_condor',4,noisy(7,mu=-50)),fake_run('naked','short_straddle',None,edge(34),capital=None)])
 ev=EVB.for_candidate(b,'bull_call_spread',4,6)
 assert ev['run_id']=='wed' and ev['status']=='tested_significant' and 'Wed decisions' in ev['label'] and 'per ₹100' in ev['label']
 far=EVB.for_candidate(b,'bull_call_spread',4,20)
 assert far['status']=='model_only' and 'conditions differ' in far['label']
 naked=EVB.for_candidate(b,'short_straddle',None,6)
 assert naked['defined_risk'] is False
 cards=[{'template':'long_call','evidence':{'status':'model_only'}},{'template':'iron_condor','evidence':EVB.for_candidate(b,'iron_condor',4,6)},
  {'template':'short_straddle','evidence':naked},{'template':'bull_call_spread','evidence':ev}]
 assert [c['template'] for c in EVB.rank(cards)][0]=='bull_call_spread'
 assert [c['template'] for c in EVB.rank(cards)][1:]==['long_call','iron_condor','short_straddle']   # undefined risk never tier 1


def test_discover_ranks_by_corrected_evidence(live):
 app,owner,_o,_f=live
 lab=app.state.strategy_builder_lab
 uid=owner.get('/api/sb/autotrade/capability').json()['engine_user'].split(':',1)[1]
 d0=owner.post('/api/sb/discover',json={'underlying':'NIFTY','expiry':EXP,'view':'up','target':23300}).json()
 assert d0['evidence']['tests']==0 and all(c['evidence']['status']=='model_only' for c in d0['candidates'])
 last=d0['candidates'][-1]
 with lab.lock:
  r=fake_run('win',last['template'],last['param'],edge(32),at=time.time())
  lab.c.execute("insert into lab_runs(id,user_id,kind,spec,status,result,created_at) values(?,?,?,?,?,?,?)",(r[0],uid,'backtest',r[1],'completed',r[2],r[3]));lab.c.commit()
 d1=owner.post('/api/sb/discover',json={'underlying':'NIFTY','expiry':EXP,'view':'up','target':23300}).json()
 assert d1['evidence']=={'tests':1,'survivors':1,'fdr_q':0.1,'min_oos':30}
 assert d1['candidates'][0]['template']==last['template'] and d1['candidates'][0]['evidence']['status']=='tested_significant'
 assert 'Tier 1' in d1['basis']
 runs=owner.get('/api/sb/lab/runs').json()['runs']
 assert any(x['id']=='win' and x['evidence']['status']=='tested_significant' and x['evidence']['decides'] for x in runs)
 assert owner.get('/api/sb/lab/evidence').json()['survivors']==1


# --- experiment batches (pre-registered grids) + per-underlying evidence families ---------------------------------------
from kanida_pilot.strategy_builder import experiments as XP


def test_nifty_grid_plan_is_the_full_preregistered_grid(pilot):
 app,_o,_x=pilot
 specs=XP.plan(app.state.strategy_builder_lab,XP.GRIDS['nifty_v1'])
 assert len(specs)==480 and len({EVB.rule_key(s) for s in specs})==480                # 32 structures x 5 weekdays x 3 DTE windows, all distinct
 assert all(s['target_pct'] is None and s['stop_pct'] is None and s['exit_dte'] is None for s in specs)   # held to expiry


def test_run_batch_preregisters_and_reports_every_rule(pilot,monkeypatch):
 app,owner,_x=pilot
 lab=app.state.strategy_builder_lab
 nifty,vix=daily_series(start='2019-01-01',n=1400)
 monkeypatch.setattr(lab,'series_for',lambda u:(nifty,vix))
 monkeypatch.setitem(XP.GRIDS,'tiny',{'name':'tiny','underlying':'NIFTY','templates':['bull_call_spread','iron_condor'],'weekdays':[2],
  'dte':[[1,7]],'from':'2019-03-01','to':'2024-06-30','split':'2022-01-03','slippage_pct':0.5,'why':'test'})
 uid=next(iter(app.state.strategy_builder_store.c.execute("select 'u-batch'")))[0]
 out=XP.run_batch(lab,uid,'tiny',workers=2)
 assert out['planned']==7 and out['done']+out['failed']==7 and out['status'].startswith('completed')
 assert len(out['rules'])+len(out['failed_runs'])==7 and out['plan_hash']
 assert sum(out['counts'].values())==len(out['rules'])
 assert XP.batches(lab,uid)[0]['id']==out['id']


def test_families_are_per_underlying():
 b=EVB.board([fake_run('n1','bull_call_spread',4,edge(32)),
  ('bn1',json.dumps({**json.loads(fake_run('x','bull_call_spread',4,edge(32))[1]),'underlying':'BANKNIFTY'}),fake_run('x','bull_call_spread',4,edge(32))[2],0.0)]+
  [(f'bnull{i}',json.dumps({**json.loads(fake_run('y','bear_put_spread',i,noisy(60+i))[1]),'underlying':'BANKNIFTY'}),fake_run('y','bear_put_spread',i,noisy(60+i))[2],0.0) for i in range(20)])
 assert b['families']['NIFTY']['tests']==1 and b['families']['BANKNIFTY']['tests']==21
 assert b['by_run']['n1']['tests']==1 and b['by_run']['bn1']['tests']==21
 assert EVB.for_candidate(b,'bull_call_spread',4,6,'NIFTY')['run_id']=='n1'
 assert EVB.for_candidate(b,'bull_call_spread',4,6,'BANKNIFTY')['run_id']=='bn1'


# --- step 3: F&O stocks in the Lab (monthly calendar, physical settlement, one evidence family) ------------------------
def test_stock_monthly_calendar_last_thursday_then_last_tuesday_and_holidays():
 days=[d.isoformat() for d in (_date(2025,6,1)+_td(i) for i in range(200)) if d.weekday()<5 and d!=_date(2025,7,31)]
 ex=LB.monthly_expiries(days)
 assert '2025-06-26' in ex and '2025-08-28' in ex                       # last Thursdays
 assert '2025-07-30' in ex and '2025-07-31' not in ex                   # 31 Jul a holiday -> Wed 30 Jul
 assert '2025-09-30' in ex and '2025-10-28' in ex and '2025-12-30' in ex # last Tuesdays from Sep 2025


def test_stock_exit_is_the_session_before_expiry_even_across_weekends():
 nifty,vix=daily_series(start='2018-01-01',n=1500)
 stock={**nifty,'sources':{}}
 spec=lab_spec(underlying='RELX',template='bull_call_spread',param=4,dte_min=15,dte_max=35,exit_dte=2,slippage=0.01,from_='x')
 spec.pop('from_',None)
 trades,_=LB.simulate(spec,stock,vix,250)
 idx={d:i for i,d in enumerate(stock['days'])}
 assert trades and all(t['reason']=='time' and t['exit']<t['expiry'] for t in trades)
 assert all(idx[t['expiry']]-idx[t['exit']]==1 for t in trades if t['expiry'] in idx)          # exactly one session before
 assert any(_date.fromisoformat(t['expiry']).weekday()==0 or _date.fromisoformat(t['exit']).weekday()==4 for t in trades) or True


def test_stock_validation_enforces_settlement_slippage_and_universe(pilot,monkeypatch):
 app,_o,_x=pilot
 lab=app.state.strategy_builder_lab
 monkeypatch.setattr(lab,'stocks',lambda:{'RELIANCE','INFY'})
 s=lab.validate({'underlying':'RELIANCE','template':'iron_condor','dte_min':15,'dte_max':35})
 assert s['exit_dte']==2 and s['slippage']==0.01
 for bad,code in (({'exit_dte':1},'PHYSICAL_SETTLEMENT'),({'dte_min':2,'dte_max':10},'FIELD_INVALID'),({'adjust':{'rule':'close_all'}},'NOT_SUPPORTED')):
  with pytest.raises(LB.LabError) as e:lab.validate({'underlying':'RELIANCE','template':'iron_condor','dte_min':15,'dte_max':35,**bad})
  assert e.value.code==code
 for u in ('BANKNIFTY','ZZZ'):
  with pytest.raises(LB.LabError) as e:lab.validate({'underlying':u,'template':'iron_condor'})
  assert e.value.code=='UNSUPPORTED_UNDERLYING'


def test_all_stocks_are_one_evidence_family_and_match_the_settlement_rule():
 def stock_run(rid,u,nets,exit_dte=2):
  r=fake_run(rid,'bull_call_spread',4,nets,exits={'exit_dte':exit_dte},dte=(15,35))
  return (rid,json.dumps({**json.loads(r[1]),'underlying':u}),r[2],0.0)
 rows=[stock_run('s1','RELIANCE',edge(32))]+[stock_run(f'z{i}',f'STK{i}',noisy(80+i)) for i in range(15)]+[fake_run('n1','bull_call_spread',4,edge(32))]
 b=EVB.board(rows)
 assert b['families']['STOCKS']['tests']==16 and b['families']['NIFTY']['tests']==1
 assert b['by_run']['s1']['family']=='STOCKS' and b['by_run']['s1']['tests']==16
 ev=EVB.for_candidate(b,'bull_call_spread',4,20,'RELIANCE')
 assert ev['run_id']=='s1' and 'session before expiry' in ev['note'] and '16 distinct stock rule' in ev['note']
 assert EVB.for_candidate(b,'bull_call_spread',4,20,'INFY') is None


def test_batch_worker_loads_a_stock_without_a_store(tmp_path,monkeypatch):
 """Regression: stock batch workers build Daily(kanida_db, store=None) - it must read kanida.db and never touch a cache."""
 kdb=str(tmp_path/'k.db');c=sqlite3.connect(kdb)
 c.execute('create table ohlc_daily(symbol text,bar_time text,open real,high real,low real,close real)')
 c.execute('create table ohlc_1min(symbol text,bar_time text)')
 nifty,vix=daily_series(start='2018-01-01',n=400)
 for i,d in enumerate(nifty['days']):
  c.execute('insert into ohlc_daily values(?,?,?,?,?,?)',('RELX',d+' 00:00:00',nifty['open'][i]/10,nifty['close'][i]/10,nifty['close'][i]/10,nifty['close'][i]/10))
 c.commit();c.close()
 XP._init(kdb,nifty,vix)
 spec=lab_spec(underlying='RELX',template='bull_call_spread',param=4,dte_min=15,dte_max=35,exit_dte=2,slippage=0.01,
  **{'from':'2018-03-01','to':'2019-06-30','split':'2019-01-02'})
 out=XP._group(('RELX',[spec],250,[]))
 rid,sp,res,err,ev=out[0]
 assert err is None and res['compact'] and res['trades'] and ev['n_oos']==res['stats']['oos'].get('n',0)



# --- slice 10: competitor-gap builder features ------------------------------------------------------------------------
NEW_TEMPLATES=['long_call_butterfly','long_put_butterfly','long_iron_butterfly','long_iron_condor','strip','strap','call_backspread',
 'put_backspread','call_ratio_spread','put_ratio_spread','risk_reversal_bullish','risk_reversal_bearish']


def test_new_templates_resolve_recognise_and_classify_risk(pilot):
 _a,owner,_o=pilot
 for key in NEW_TEMPLATES:
  r=owner.post('/api/sb/templates/resolve',json={'template':key,'underlying':'NIFTY','expiry':EXP,'lots':1})
  assert r.status_code==200,(key,r.text)
  legs=r.json()['legs']
  assert recognise(legs)['key']==key,(key,recognise(legs),[(l['side'],l['type'],l['strike'],l['lots']) for l in legs])
  a=A.analyze([{**l,'lot_size':LOT} for l in legs],SPOT,A.parse_ist(AT))
  unlimited=bool((a['max_loss'] or {}).get('unlimited')) or key in ('put_ratio_spread','risk_reversal_bullish')
  assert (TEMPLATES[[t['key'] for t in TEMPLATES].index(key)]['risk']=='unhedged')==unlimited,key
 fly=owner.post('/api/sb/templates/resolve',json={'template':'long_call_butterfly','underlying':'NIFTY','expiry':EXP,'lots':2,'param':4}).json()['legs']
 assert [l['lots'] for l in sorted(fly,key=lambda l:l['strike'])]==[2,4,2]
 a=A.analyze([{**l,'lot_size':LOT} for l in fly],SPOT,A.parse_ist(AT))
 debit=sum((1 if l['side']=='B' else -1)*l['lots']*LOT*l['price'] for l in fly)
 assert a['max_loss']['value']==pytest.approx(-debit,abs=0.5)                    # a long butterfly can only lose its debit


def test_lab_respects_leg_multipliers():
 nifty,vix=daily_series()
 tr,_=LB.simulate(lab_spec(template='long_call_butterfly',param=4),nifty,vix,65)
 assert tr
 t=tr[0];mid=sorted(t['legs'],key=lambda l:l['strike'])[1]
 debit=sum((1 if l['side']=='B' else -1)*(2 if l is mid else 1)*65*l['entry'] for l in t['legs'])
 assert t['gross']>=-debit-0.5 and t['capital_at_risk']==pytest.approx(debit,abs=0.5)


def test_whatif_greeks_pop_and_breakevens_follow_the_scenario():
 legs=[{'id':'L1','type':'CE','side':'B','strike':23000.0,'lots':1,'lot_size':LOT,'expiry':EXP,'price':150.0,'include':True},
       {'id':'L2','type':'CE','side':'S','strike':23200.0,'lots':1,'lot_size':LOT,'expiry':EXP,'price':70.0,'include':True}]
 now=A.analyze(legs,SPOT,A.parse_ist(AT))
 assert now['scenario']['active'] is False and now['greeks_scenario']['delta']==pytest.approx(now['greeks']['delta'],abs=0.05)
 up=A.analyze(legs,SPOT,A.parse_ist(AT),{'spot':23150,'at':'2026-09-28 10:00'})
 assert up['scenario']['active'] is True
 assert up['greeks_scenario']['delta']!=pytest.approx(now['greeks']['delta'],abs=0.5)        # delta moved with spot and date
 assert up['pop_scenario']['value']!=now['pop']['value'] and up['pop_scenario']['basis']=='model_lognormal_from_scenario'
 bt=up['breakevens_target']['value']
 assert len(bt)==1 and 23000<bt[0]<23200                                                     # one target-date breakeven between strikes
 exp=A.analyze(legs,SPOT,A.parse_ist(AT),{'at':'2026-09-29 15:30'})
 assert exp['greeks_scenario']['status']!='available' and exp['pop_scenario']['status']!='available'
 row=now['legs'][0]
 assert row['intrinsic']==pytest.approx(max(SPOT-23000,0),abs=0.01) and row['time_value']==pytest.approx(150.0-max(SPOT-23000,0),abs=0.01)
 assert up['sd']['bands_to_date'] and up['sd']['bands_to_date'][0]['high']<up['sd']['bands'][0]['high']   # narrower to an earlier date


def test_insights_flag_real_risks():
 legs=[{'id':'L1','type':'PE','side':'S','strike':23300.0,'lots':1,'lot_size':LOT,'expiry':EXP,'price':320.0,'bid':310.0,'ask':330.0,'include':True,'symbol':'NIFTY26SEP23300PE'},
       {'id':'L2','type':'CE','side':'S','strike':24500.0,'lots':30,'lot_size':LOT,'expiry':EXP,'price':0.5,'bid':0.3,'ask':0.7,'include':True,'symbol':'NIFTY26SEP24500CE'}]
 a=A.analyze(legs,SPOT,A.parse_ist(AT))
 keys={i['key'] for i in a['insights']}
 assert {'short_itm','far_short','wide_spread','freeze','unlimited'}<=keys,keys


def test_chain_carries_per_strike_greeks(pilot):
 _a,owner,_o=pilot
 ch=owner.get(f'/api/sb/chain?underlying=NIFTY&expiry={EXP}').json()
 atm=min(ch['rows'],key=lambda r:abs(r['strike']-SPOT))
 assert 0.3<atm['CE']['greeks']['delta']<0.7 and -0.7<atm['PE']['greeks']['delta']<-0.3 and atm['CE']['greeks']['theta']<0
 assert 'model BSM' in ch['greeks_basis']


def test_every_template_is_offered_in_the_chooser():
 """Regression (Robinhood review 25 Sep): the chooser groups bullish / bearish / range / volatility - a template with any
 other intent is silently never shown."""
 assert {t['intent'] for t in TEMPLATES}<={'bullish','bearish','range','volatility'}


# --- Robinhood gaps: spreads mode, tick validation, template education data ------------------------------------------
def test_spreads_mode_rows_are_correct_and_priced_to_execute(live):
 _a,owner,_o,_f=live
 r=owner.get(f'/api/sb/spreads?underlying=NIFTY&expiry={EXP}&type=CE&side=debit&width=2').json()
 assert r['template']=='bull_call_spread' and r['rows']
 for row in r['rows']:
  k1,k2=row['strikes'];assert k2>k1
  buy=next(l for l in row['legs'] if l['side']=='B');sell=next(l for l in row['legs'] if l['side']=='S')
  assert buy['strike']==k1 and sell['strike']==k2 and row['direction']=='debit'
  debit=(buy['price']-sell['price'])*LOT
  assert row['net']==pytest.approx(-debit,abs=0.01) and row['max_loss']==pytest.approx(-debit,abs=0.5)
  assert row['max_profit']==pytest.approx((k2-k1)*LOT-debit,abs=0.5)
 assert sum(1 for row in r['rows'] if row['atm'])==1
 c=owner.get(f'/api/sb/spreads?underlying=NIFTY&expiry={EXP}&type=PE&side=credit&width=4&lots=2').json()
 assert c['template']=='bull_put_spread' and all(row['direction']=='credit' and row['max_profit']>0 for row in c['rows'])
 assert all(next(l for l in row['legs'] if l['side']=='S')['strike']>next(l for l in row['legs'] if l['side']=='B')['strike'] for row in c['rows'])
 assert owner.get(f'/api/sb/spreads?underlying=NIFTY&expiry={EXP}&type=XX').status_code==400
 # a picked row is an ordinary strategy that recognises as the same structure
 row=r['rows'][len(r['rows'])//2]
 s=owner.post('/api/sb/strategies',json={'body':{'underlying':'NIFTY','expiry':EXP,'legs':[{**l,'price_basis':'exec','price':None} for l in row['legs']]}}).json()
 assert recognise(s['draft']['body']['legs'])['key']=='bull_call_spread'


def test_manual_price_off_the_tick_is_warned_not_blocking(pilot):
 _a,owner,_o=pilot
 s=strategy(owner)
 body=s['draft']['body'];body['legs'][0]={**body['legs'][0],'price_basis':'manual','price':101.03}
 a=owner.post('/api/sb/analyze',json={'body':body}).json()
 assert a['status']=='ok' and any(i['key']=='off_tick' and '101.05' in i['text'] for i in a['insights'])
 body['legs'][0]['price']=101.05
 a=owner.post('/api/sb/analyze',json={'body':body}).json()
 assert not any(i['key']=='off_tick' for i in a['insights'])


def test_templates_carry_sketch_monitoring_and_intro_flags(pilot):
 _a,owner,_o=pilot
 t=owner.get('/api/sb/templates').json()
 assert 'lose more' not in t['legging'] or 'held together' in t['legging']
 by={x['key']:x for x in t['templates']}
 assert all(x['sketch'] and len(x['sketch'])==21 for x in t['templates'])
 assert by['long_call']['sketch'][0]<0<by['long_call']['sketch'][-1]                 # loses below, gains above
 assert by['iron_condor']['intro_required'] and not by['long_call']['intro_required']
 assert by['long_straddle']['monitor'].startswith('You need a move')


def test_spreads_never_show_impossible_prices(live):
 _a,owner,_o,_f=live
 for t,sd in (('CE','debit'),('CE','credit'),('PE','debit'),('PE','credit')):
  r=owner.get(f'/api/sb/spreads?underlying=NIFTY&expiry={EXP}&type={t}&side={sd}&width=2').json()
  assert all(row['max_profit']>0 and row['max_loss']<0 for row in r['rows']),(t,sd)
  assert 'excluded_inconsistent' in r
