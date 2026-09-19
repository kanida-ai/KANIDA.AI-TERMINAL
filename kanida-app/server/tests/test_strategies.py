"""Strategy registry, Discover catalog/results and owner-only admin (docs/FALCON_DISCOVER_SPEC.md §5, §6; src/strategies/types.ts).
The research service is faked: Evidence.get is monkeypatched, port 8765 is never called."""
from datetime import timedelta
import pytest
from sqlalchemy import select,func
from kanida_pilot.db import strategy_blocks,strategies,audit
from kanida_pilot.evidence import market_today
from test_pilot import app,signup  # noqa: F401  (shared fixture)

def hist(side,n,ci,avg=1.0,wr=50.0,status='no_validated_rule',test=None):
 return dict(run='r1',side=side,status=status,reference=dict(n=n,win_rate=wr,expectancy_pct=avg,expectancy_ci95=ci),
  test=test or dict(n=0,win_rate=None,expectancy_pct=None,expectancy_ci95=None))
def match(symbol,pattern,tf,direction,history,sector='Capital Goods'):
 return dict(id=f'{symbol}:{tf}:{pattern}',symbol=symbol,company=symbol+' Ltd.',sector=sector,timeframe=tf,pattern=pattern,pattern_name=pattern,
  direction=direction,state='setup',price=100.5,candle_end='2026-07-31 15:30:00',score=80,universes=['nifty500'],history=history)
MATCHES=[
 match('GESHIP','falling_wedge','1D','bullish',[hist('long',10,[0.7366,8.93],avg=4.83,wr=70)]),
 match('AAA','falling_wedge','1D','bullish',[hist('long',4,[1.2,9.0])]),
 match('ABC','falling_wedge','1D','bullish',[hist('long',4,[1.2,9.0])]),
 match('ZZZ','falling_wedge','1D','bullish',[hist('long',9,[1.2,9.0])]),
 match('BBB','falling_wedge','1D','bullish',[hist('long',30,None)]),
 match('CCC','falling_wedge','1D','bullish',[hist('long',12,[-1.0,2.0],status='tested',test=dict(n=22,win_rate=55,expectancy_pct=.5,expectancy_ci95=[.1,.9]))]),
 match('DDD','falling_wedge','1D','bullish',[]),
 match('GGG','falling_wedge','4H','bullish',[hist('long',40,[2.0,3.0])]),
 match('EEE','channel','1D','neutral',[hist('long',13,[-0.5,6.0]),hist('short',13,[-2.0,1.0])]),
 match('FFF','channel','1D','bearish',[hist('short',35,[-0.2,1.0])]),
]
def research(source_latest=None,stale=False,matches=MATCHES):
 latest=source_latest or market_today().isoformat()+' 00:00:00';calls=[]
 def get(path,params=None,cache=False):
  calls.append((path,dict(params or {})))
  if path=='/api/state':return dict(source_latest=latest,source_stale=stale,schedule={'1D':{'last_scan':'2026-09-12 11:22:28'},'1W':{'last_scan':'2026-09-12 11:20:00'}})
  if path=='/api/filter-options':return dict(universes=[dict(value='nifty500',label='Nifty 500',count=501),dict(value='nifty50',label='Nifty 50',count=48)])
  if path=='/api/matches':return [m for m in matches if params.get('universe')!='nifty50']
  raise AssertionError('unexpected research path '+path)
 get.calls=calls;return get
@pytest.fixture
def owner(app,monkeypatch):
 monkeypatch.setattr(app.state.evidence,'get',research());return signup(app)

def test_seed_is_idempotent_and_block_extensible(app):
 registry=app.state.strategies
 assert registry.seed() is False  # already seeded at startup
 with app.state.db.tx() as c:
  blocks={r['key']:dict(r) for r in c.execute(select(strategy_blocks)).mappings()}
  items={r['key']:dict(r) for r in c.execute(select(strategies)).mappings()}
 assert set(blocks)=={'chart','quant','results','options'} and blocks['chart']['enabled'] and not any(blocks[k]['enabled'] for k in ('quant','results','options'))
 assert len(items)==13*4 and {i['block_key'] for i in items.values()}=={'chart'}  # 13 pattern-sides x 4 timeframes
 fw=items['falling_wedge-1D-long'];assert fw['name']=='Falling Wedge breakout · 1D' and fw['tags']==['Chart patterns','Bullish','1D']
 assert fw['default_slot']=='A' and items['channel-1D-long']['default_slot']=='B' and fw['min_trades']==10 and fw['audience']=='trader'
 assert fw['source_type']=='stored_pattern' and fw['source_config']==dict(pattern='falling_wedge',timeframe='1D',side='long')
 assert items['channel-1D-short']['name']=='Channel breakdown · 1D · Short' and items['rising_wedge-1W-short']['name']=='Rising Wedge breakdown · 1W'
 assert items['rising_wedge-1W-short']['audience']=='investor' and 'falling_wedge-1D-short' not in items
 assert items['horizontal_breakout-1D-long']['name']=='Horizontal Breakout · 1D'  # never "Breakout breakout"
 assert min(items.values(),key=lambda i:i['position'])['source_config']['timeframe']=='1D'
 assert sum(1 for i in items.values() if i['default_slot'])==2
 assert registry.seed() is False
 with app.state.db.tx() as c:assert c.execute(select(func.count()).select_from(strategies)).scalar()==52

def test_seed_renames_only_untouched_old_default_names(app):
 registry=app.state.strategies;S=strategies.c
 with app.state.db.tx() as c:
  c.execute(strategies.update().where(S.key=='horizontal_breakout-1D-long').values(name='Horizontal Breakout breakout · 1D'))  # v1 seed wording
  c.execute(strategies.update().where(S.key=='horizontal_breakout-1W-long').values(name='My horizontal breakout'))  # admin-edited
  c.execute(strategies.update().where(S.key=='falling_wedge-4H-long').values(name='Horizontal Breakout breakout · 4H'))  # old name of another pattern
 assert registry.seed() is False
 with app.state.db.tx() as c:names={r['key']:r['name'] for r in c.execute(select(S.key,S.name)).mappings()}
 assert names['horizontal_breakout-1D-long']=='Horizontal Breakout · 1D' and names['horizontal_breakout-1W-long']=='My horizontal breakout'
 assert names['falling_wedge-4H-long']=='Horizontal Breakout breakout · 4H' and names['channel-1D-short']=='Channel breakdown · 1D · Short'
 assert len(names)==52 and registry.seed() is False
 with app.state.db.tx() as c:assert c.execute(select(func.count()).select_from(audit).where(audit.c.kind=='strategy_registry')).scalar()==0

def test_universe_label_and_slug_helpers():
 from kanida_pilot.strategies import universe_label,slug
 assert universe_label('nifty500','Nifty 500')=='NIFTY 500' and universe_label('niftymidcap150','Nifty Midcap 150')=='NIFTY MIDCAP 150'
 assert universe_label('custom','My watchlist')=='My watchlist' and universe_label('x',None)=='x'
 assert slug('Falling Wedge strict · 1D')=='falling-wedge-strict-1d' and slug(' ·· ')=='' and len(slug('a'*100))==60

def test_add_without_key_generates_unique_key(app,owner):
 body=dict(block_key='chart',source_type='stored_pattern',pattern='falling_wedge',timeframe='1D',side='long')
 keys=[]
 for _ in range(3):
  r=owner.post('/api/admin/strategies',json=dict(body,name='Falling Wedge strict · 1D'));assert r.status_code==200,r.text;keys.append(r.json()['key'])
 assert keys==['falling-wedge-strict-1d','falling-wedge-strict-1d-2','falling-wedge-strict-1d-3']
 r=owner.post('/api/admin/strategies',json=body);assert r.status_code==200 and r.json()['key']=='falling-wedge-breakout-1d' and r.json()['name']=='Falling Wedge breakout · 1D'
 r=owner.post('/api/admin/strategies',json=dict(body,name='···',key=''));assert r.status_code==200 and r.json()['key']=='falling_wedge-1D-long-2'  # no usable slug -> pattern-TF-side, seeded one taken
 long=[owner.post('/api/admin/strategies',json=dict(body,name='x'*100)).json()['key'] for _ in range(2)]
 assert long==['x'*60,'x'*60+'-2']
 r=owner.post('/api/admin/strategies',json=dict(body,key='falling-wedge-strict-1d'));assert r.status_code==409 and r.json()['code']=='STRATEGY_EXISTS'
 r=owner.post('/api/admin/strategies',json=dict(body,side='short',name='Nope'));assert r.status_code==400 and r.json()['code']=='SIDE_UNSUPPORTED'  # other validation unchanged
 reg=owner.get('/api/admin/strategies').json();assert reg['registry_version']==8
 made={s['key']:s for s in reg['blocks'][0]['strategies']};assert made['falling-wedge-strict-1d-2']['pattern']=='falling_wedge' and made['falling-wedge-strict-1d-2']['enabled'] is False

def test_catalog_counts_and_freshness(app,owner,monkeypatch):
 value=owner.get('/api/strategies/catalog').json()
 assert (value['data_end'],value['age_days'],value['stale'],value['scanned_at'])==(market_today().isoformat(),0,False,'2026-09-12 11:22:28')
 assert value['registry_version']==1 and value['universe']==dict(key='nifty500',label='NIFTY 500',count=501)  # research says "Nifty 500"; shown NSE-style
 assert owner.get('/api/strategies/catalog?universe=nifty50').json()['universe']['label']=='NIFTY 50'
 assert [b['key'] for b in value['blocks']]==['chart']  # disabled placeholders are hidden
 found={s['key']:s for s in value['blocks'][0]['strategies']}
 fw=found['falling_wedge-1D-long']
 assert (fw['found'],fw['best_low_pct'],fw['positive_low_count'],fw['tested_count'])==(7,0.7366,1,1)
 assert set(fw)>={'key','block_key','name','description','tags','source_type','pattern','pattern_name','timeframe','side','audience','min_trades','default_slot','order','enabled'}
 assert fw['pattern_name']=='Falling Wedge' and fw['default_slot']=='A'
 assert (found['channel-1D-long']['found'],found['channel-1D-short']['found'])==(1,2)  # neutral counts on both sides, bearish only short
 assert found['channel-1D-short']['best_low_pct']==-0.2 and found['channel-1D-short']['positive_low_count']==0
 assert found['falling_wedge-4H-long']['found']==1 and found['cup_handle-1D-long']['found']==0 and found['cup_handle-1D-long']['best_low_pct'] is None
 assert ('/api/matches',{'min_trades':'0','universe':'nifty500'}) in app.state.evidence.get.calls
 monkeypatch.setattr(app.state.evidence,'get',research((market_today()-timedelta(days=45)).isoformat()+' 00:00:00'))
 old=owner.get('/api/strategies/catalog').json();assert old['stale'] is True and old['age_days']==45
 monkeypatch.setattr(app.state.evidence,'get',research(stale=True));assert owner.get('/api/strategies/catalog').json()['stale'] is True
 assert owner.get('/api/strategies/catalog?universe=unknown').status_code==400

def test_results_sorting_nulls_and_evidence_labels(app,owner):
 r=owner.get('/api/strategies/falling_wedge-1D-long/results?universe=nifty500');assert r.status_code==200,r.text;value=r.json()
 assert [x['symbol'] for x in value['rows']]==['ZZZ','AAA','ABC','GESHIP','CCC','BBB','DDD'] and value['total']==7 and value['source']=='stored_scan'
 rows={x['symbol']:x for x in value['rows']}
 g=rows['GESHIP'];assert (g['low_pct'],g['high_pct'],g['avg_pct'],g['win_rate'],g['n'],g['sample_label'])==(0.7366,8.93,4.83,70.0,10,'Moderate sample')
 assert g['evidence_basis']=='hold_period_history' and g['test'] is None and g['status']=='no_validated_rule' and g['match_id']=='GESHIP:1D:falling_wedge'
 assert g['side']=='long' and g['company']=='GESHIP Ltd.' and g['sector']=='Capital Goods' and g['price']==100.5
 assert rows['CCC']['evidence_basis']=='tested_rule' and rows['CCC']['test']==dict(n=22,expectancy_pct=.5,expectancy_ci95=[.1,.9])
 assert rows['BBB']['low_pct'] is None and rows['BBB']['sample_label']=='Larger sample'
 d=rows['DDD'];assert (d['low_pct'],d['n'],d['sample_label'],d['status'],d['evidence_basis'])==(None,0,'No history','unknown','none')
 assert rows['AAA']['sample_label']=='Small sample' and value['strategy']['best_low_pct']==0.7366
 assert value['universe']['count']==501 and value['stale'] is False
 assert owner.get('/api/strategies/nope-1D-long/results').json()['code']=='STRATEGY_NOT_FOUND'
 assert owner.patch('/api/admin/strategies/falling_wedge-1D-long',json={'enabled':False}).status_code==200
 r=owner.get('/api/strategies/falling_wedge-1D-long/results');assert r.status_code==404 and r.json()['code']=='STRATEGY_NOT_FOUND'
 assert 'falling_wedge-1D-long' not in [s['key'] for s in owner.get('/api/strategies/catalog').json()['blocks'][0]['strategies']]

def test_admin_is_owner_only(app,owner):
 m=signup(app,'member-admin@example.invalid','member')
 assert m.get('/api/admin/strategies').status_code==403 and m.get('/api/admin/blocks').status_code==403
 assert m.post('/api/admin/strategies',json={'block_key':'chart','pattern':'channel','timeframe':'1D','side':'long','key':'x-member'}).status_code==403
 assert m.patch('/api/admin/strategies/channel-1D-long',json={'enabled':False}).status_code==403
 assert m.post('/api/admin/strategies/channel-1D-long/preview',json={}).status_code==403
 assert m.post('/api/admin/blocks',json={'key':'labs','title':'Labs','kind':'quant'}).status_code==403
 assert m.patch('/api/admin/blocks/quant',json={'enabled':True}).status_code==403
 assert m.get('/api/strategies/catalog').status_code==402  # member without pilot access
 with app.state.db.tx() as c:assert c.execute(select(func.count()).select_from(audit).where(audit.c.kind=='strategy_registry')).scalar()==0

def test_admin_validation_version_and_activity(app,owner):
 base=owner.get('/api/admin/strategies').json();assert base['registry_version']==1
 assert [b['key'] for b in base['blocks']]==['chart','quant','results','options'] and len(base['blocks'][0]['strategies'])==52 and base['blocks'][1]['strategies']==[]
 body=dict(block_key='chart',source_type='stored_pattern',pattern='falling_wedge',timeframe='1D',side='long')
 for change,code in ((dict(pattern='triangle'),'FIELD_INVALID'),(dict(timeframe='15m'),'FIELD_INVALID'),(dict(side='up'),'FIELD_INVALID'),(dict(side='short'),'SIDE_UNSUPPORTED'),
  (dict(source_type='quant_rule'),'FIELD_INVALID'),(dict(block_key='missing',key='fw-missing'),'BLOCK_NOT_FOUND'),(dict(min_trades=0,key='fw-zero'),'FIELD_INVALID'),(dict(key='bad key!'),'FIELD_INVALID')):
  r=owner.post('/api/admin/strategies',json=dict(body,**change));assert r.status_code==400 and r.json()['code']==code,(change,r.text)
 r=owner.post('/api/admin/strategies',json=dict(body,key='falling_wedge-1D-long'));assert r.status_code==409 and r.json()['code']=='STRATEGY_EXISTS'  # explicit keys still collide
 assert owner.get('/api/admin/strategies').json()['registry_version']==1
 r=owner.post('/api/admin/strategies',json=dict(body,key='falling_wedge-1D-long-strict',name='Falling Wedge strict · 1D',min_trades=30,default_slot='A',tags=['Chart patterns','Bullish','1D','Strict']))
 assert r.status_code==200,r.text;created=r.json()
 assert created['key']=='falling_wedge-1D-long-strict' and created['enabled'] is False and created['pattern_name']=='Falling Wedge' and created['audience']=='trader'
 assert created['order']>max(s['order'] for s in base['blocks'][0]['strategies']) and created['default_slot']=='A'
 after=owner.get('/api/admin/strategies').json();assert after['registry_version']==2
 slots={s['key']:s['default_slot'] for s in after['blocks'][0]['strategies']};assert slots['falling_wedge-1D-long'] is None and slots['channel-1D-long']=='B'
 r=owner.patch('/api/admin/strategies/channel-1D-long',json={'name':'Channel breakout · Daily','min_trades':20,'order':1,'tags':['Chart patterns']})
 assert r.status_code==200 and (r.json()['name'],r.json()['min_trades'],r.json()['order'])==('Channel breakout · Daily',20,1)
 assert owner.patch('/api/admin/strategies/channel-1D-long',json={'pattern':'falling_wedge'}).json()['code']=='FIELD_IMMUTABLE'
 assert owner.patch('/api/admin/strategies/channel-1D-long',json={'pattern':'channel','timeframe':'1D'}).status_code==200  # unchanged fixed fields are accepted
 assert owner.patch('/api/admin/strategies/channel-1D-long',json={'audience':'everyone'}).status_code==400
 assert owner.patch('/api/admin/strategies/channel-1D-long',json={'bogus':1}).status_code==400
 assert owner.patch('/api/admin/strategies/missing-1D-long',json={'enabled':False}).status_code==404
 assert owner.get('/api/admin/strategies').json()['registry_version']==3
 r=owner.post('/api/admin/blocks',json={'key':'labs','title':'Quant Labs','description':'Later.','kind':'quant','enabled':False});assert r.status_code==200,r.text
 assert r.json()==dict(key='labs',title='Quant Labs',description='Later.',kind='quant',order=50,enabled=False)
 assert owner.post('/api/admin/blocks',json={'key':'labs','title':'Again','kind':'quant'}).status_code==409
 assert owner.post('/api/admin/blocks',json={'key':'x2','title':'Bad','kind':'crypto'}).status_code==400
 r=owner.patch('/api/admin/blocks/quant',json={'enabled':True,'title':'Quant Strategies'});assert r.status_code==200 and r.json()['enabled'] is True
 assert owner.patch('/api/admin/blocks/missing',json={'enabled':True}).status_code==404
 assert owner.patch('/api/admin/strategies/falling_wedge-1D-long-strict',json={'block_key':'labs'}).json()['block_key']=='labs'
 # 5 effective changes: strategy added, channel updated, block added, quant block updated, strict strategy moved (no-op patches do not count)
 blocks=owner.get('/api/admin/blocks').json();assert blocks['registry_version']==6 and [b['key'] for b in blocks['blocks']][-1]=='labs'
 catalog=owner.get('/api/strategies/catalog').json()
 assert catalog['registry_version']==6 and [b['key'] for b in catalog['blocks']]==['chart','quant']  # enabled block with no enabled strategies is listed empty
 assert catalog['blocks'][1]['strategies']==[]
 with app.state.db.tx() as c:
  titles=[r['title'] for r in c.execute(select(audit).where(audit.c.kind=='strategy_registry').order_by(audit.c.created)).mappings()]
 assert len(titles)==5 and titles.count('Strategy updated')==2 and {'Strategy added','Strategy block added','Strategy block updated'}<=set(titles)

def test_preview_top_five_works_for_disabled(app,owner):
 created=owner.post('/api/admin/strategies',json=dict(block_key='chart',source_type='stored_pattern',pattern='falling_wedge',timeframe='1D',side='long',key='fw-preview',enabled=False)).json()
 assert created['enabled'] is False
 assert owner.get('/api/strategies/fw-preview/results').status_code==404
 r=owner.post('/api/admin/strategies/fw-preview/preview',json={});assert r.status_code==200,r.text;value=r.json()
 assert [x['symbol'] for x in value['rows']]==['ZZZ','AAA','ABC','GESHIP','CCC'] and value['total']==7 and value['strategy']['key']=='fw-preview'
 assert value['strategy']['found']==7 and value['data_end']==market_today().isoformat()
 assert owner.post('/api/admin/strategies/missing/preview',json={}).status_code==404
 version=owner.get('/api/admin/strategies').json()['registry_version']
 owner.post('/api/admin/strategies/fw-preview/preview',json={'universe':'nifty50'})
 assert owner.get('/api/admin/strategies').json()['registry_version']==version  # previews never change the registry


def test_sources_without_a_catalogue_still_offer_the_stored_ten(app,owner):
 """BACKLOG item 4. No research tree on this server: the add-strategy form must still be able to add a stored
 pattern, and must say WHY the researched half is missing instead of offering ids the server would refuse."""
 body=owner.get('/api/admin/strategy-sources')
 assert body.status_code==200,body.text
 value=body.json()
 assert value['stored']['available'] is True and len(value['stored']['patterns'])==10
 falling=next(p for p in value['stored']['patterns'] if p['id']=='falling_wedge')
 assert falling['name']=='Falling Wedge' and falling['sides']==['long'] and falling['description']
 assert value['research']['available'] is False and value['research']['patterns']==[]
 assert value['research']['reason'],'an unavailable source says why, in the server\'s words'
 assert value['research']['run'] is None
 # Everything offered is accepted, and adding it goes in disabled.
 for pattern in value['stored']['patterns']:
  for side in pattern['sides']:
   made=owner.post('/api/admin/strategies',json=dict(block_key=pattern['block_key'],source_type='stored_pattern',
    pattern=pattern['id'],timeframe=value['stored']['timeframes'][0],side=side,name='Owner '+pattern['id']+' '+side))
   assert made.status_code==200,(pattern['id'],side,made.text)
   assert made.json()['enabled'] is False and made.json()['source_type']=='stored_pattern'

def test_adding_a_research_pattern_without_a_catalogue_is_refused_not_guessed(app,owner):
 result=owner.post('/api/admin/strategies',json=dict(block_key='chart',source_type='research_pattern',
  pattern_id='CH05',variant='legacy_1.0.1',side='long',timeframe='1D'))
 assert result.status_code==503 and result.json()['code']=='RESEARCH_UNAVAILABLE'
 # With no catalogue to compare against, an unknown stored pattern is still the plain stored-source refusal.
 stored=owner.post('/api/admin/strategies',json=dict(block_key='chart',source_type='stored_pattern',
  pattern='CH05',timeframe='1D',side='long',name='No catalogue here'))
 assert stored.status_code==400 and stored.json()['code']=='FIELD_INVALID'

def test_the_sources_list_needs_the_owner(app):
 member=signup(app,email='member@example.invalid',role='member')
 assert member.get('/api/admin/strategy-sources').status_code==403
