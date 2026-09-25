"""The Strategy Builder: exact payoff maths on known structures, recognition, template resolution, the library with
versioned autosave and immutable snapshots, paper fills, Discover, owner scoping, and the expiry-date invariant."""
import math,sqlite3
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
 assert a['max_loss']['status']=='unsupported' and a['max_loss']['reason']=='MULTI_EXPIRY'
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
 assert rng['candidates'] and {c['template'] for c in rng['candidates']}<={'iron_condor','iron_butterfly'}


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
    x['bid'],x['ask']=b;x['flags']=[];row[k]=x
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
 assert buy['slices']==[1755,195] and buy['limit']==buy['ask'] and sell['limit']==sell['bid']
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
