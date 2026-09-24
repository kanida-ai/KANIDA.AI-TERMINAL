"""The evaluator on a synthetic F&O store: states, the lifecycle, the strike range, grain, the expiry-day roll,
IV, and the property everything else rests on — POINT IN TIME: the status the full session's replay gives at
reading i is exactly what a run with only readings up to i would have given."""
import pytest
from kanida_pilot import implied_vol as IV
from kanida_pilot.screener.service import Screener
from screener_fixture import build,ladder

SESSION='2026-09-21'
EXP='2026-09-29'
TIMES=['09:30','09:45','10:00','10:15','10:30','10:45','11:00','11:15']
N=len(TIMES)
STRIKES=[24300,24400,24500,24600,24700]


def oi_scanner(side='CE',state='up_cont',minutes=45,**extra):
 return {'strikes':{'kind':'atm','below':2,'above':2},'conditions':[{'metric':'oi','side':side,'state':state,
  'window':{'kind':'minutes','value':minutes}}],**extra}


def make(tmp_path,books,contracts,times=TIMES,session=SESSION,name='d.db'):
 path=build(str(tmp_path/name),session,times,books,contracts)
 return Screener(path,str(tmp_path/f'{name}.screener.db'))


def nifty(spot=None,**more):
 return {('NIFTY',EXP):{'spot':spot or [24500.0]*N,'pcr':more.get('pcr',[0.9]*N),'maxpain':more.get('maxpain',[24500.0]*N)}}


def test_call_oi_building_lifecycle_and_explanation(tmp_path):
 # 24,500 CE adds OI at every reading 09:45..10:45, then is flat at 11:00
 oid=[0,1e5,2e5,3e5,4e5,5e5,5e5,5e5]
 sc=make(tmp_path,nifty(),ladder('NIFTY',EXP,STRIKES,N,{(24500,'CE'):{'oid':oid}}))
 r=sc.results(oi_scanner())
 assert r['grain']=='contract' and r['counts']=={'active':0,'ended':1,'all':1}
 m=r['matches'][0]
 assert m['title']=='NIFTY · 24,500 CE'
 assert m['first_matched']=='10:15'          # the first reading with three rising intervals behind it
 # a 45-min window cannot be read before 10:15: those readings are "unseen", not misses
 assert m['timeline']==['unseen','unseen','unseen','new','still','still','ended','none']
 assert m['ended_at']=='11:00' and m['readings_matched']==3 and m['duration_min']==30
 assert m['because'][0].startswith('Call OI rose at each of the last 3 readings: +3L OI since 10:00')
 assert 'no longer increasing continuously at the 11:00 reading' in m['ended_because']
 text=' '.join(m['because']+[m['ended_because']]).lower()
 for banned in ('buy ','sell','good','bad','best'):assert banned not in text


def test_point_in_time_replay_equals_live_runs(tmp_path):
 oid={(24500,'CE'):{'oid':[0,1e5,2e5,3e5,4e5,4.2e5,6e5,5e5]},(24600,'CE'):{'oid':[0,0,5e4,1e5,2e5,4e5,8e5,1.6e6]},
  (24400,'PE'):{'oid':[0,-1e5,-2e5,-3e5,-2e5,-1e5,0,1e5]}}
 spot=[24500,24510,24520,24560,24610,24650,24700,24720]
 books=nifty(spot=[float(x) for x in spot])
 contracts=ladder('NIFTY',EXP,STRIKES,N,oid)
 full=make(tmp_path,books,contracts).results(oi_scanner(side='either',state='up_cont',minutes=30))
 statuses={m['key']:m['timeline'] for m in full['matches']}
 assert statuses
 for i in range(1,N):
  cut={k:{**v,'spot':v['spot'][:i+1],'pcr':v['pcr'][:i+1],'maxpain':v['maxpain'][:i+1]} for k,v in books.items()}
  part=make(tmp_path,cut,[{**c,'price':c['price'][:i+1],'oid':c['oid'][:i+1]} for c in contracts],
   times=TIMES[:i+1],name=f'cut{i}.db').results(oi_scanner(side='either',state='up_cont',minutes=30))
  live={m['key']:m['timeline'][-1] for m in part['matches']}
  for key,timeline in statuses.items():
   expected=timeline[i]
   if expected in ('unseen','none'):assert key not in live or live[key] in ('unseen','none','ended')
   else:assert live.get(key)==expected,(key,i,expected,live.get(key))


def test_leaving_the_strike_range_ends_the_match(tmp_path):
 oid={(24300,'CE'):{'oid':[0,1e5,2e5,3e5,4e5,5e5,6e5,7e5]}}
 # spot walks up; at 11:00 ATM is 24,600, so 24,300 is 3 strikes below and outside ATM ±2
 spot=[24400.0,24400,24400,24400,24400,24500,24600,24600]
 sc=make(tmp_path,nifty(spot=spot),ladder('NIFTY',EXP,STRIKES,N,oid))
 m=sc.results(oi_scanner())['matches'][0]
 assert m['ended_at']=='11:00' and 'left the strike range' in m['ended_because']


def test_a_missing_reading_is_a_gap_not_an_end(tmp_path):
 oid=[0,1e5,2e5,3e5,None,5e5,6e5,7e5]
 price=[100.0]*N;price[4]=None
 sc=make(tmp_path,nifty(),ladder('NIFTY',EXP,STRIKES,N,{(24500,'CE'):{'oid':oid,'price':price}}))
 m=sc.results(oi_scanner())['matches'][0]
 assert m['timeline'][3:]==['new','gap','still','still','still'] and m['active']


def test_cross_side_scanner_is_read_at_the_book(tmp_path):
 oid={(24500,'CE'):{'oid':[0,1e5,2e5,3e5,4e5,5e5,6e5,7e5]},(24500,'PE'):{'oid':[0,-1e5,-2e5,-3e5,-4e5,-5e5,-6e5,-7e5]}}
 sc=make(tmp_path,nifty(),ladder('NIFTY',EXP,STRIKES,N,oid))
 d={'strikes':{'kind':'atm','below':2,'above':2},'conditions':[
  {'metric':'oi','side':'CE','state':'up_cont','window':{'kind':'minutes','value':30}},
  {'metric':'oi','side':'PE','state':'down_cont','join':'and','window':{'kind':'minutes','value':30}}]}
 r=sc.results(d)
 assert r['grain']=='book' and len(r['matches'])==1
 m=r['matches'][0]
 assert m['title']=='NIFTY' and m['active'] and m['first_matched']=='10:00'
 assert any('at 24,500 CE' in b for b in m['because']) and any('at 24,500 PE' in b for b in m['because'])


def test_max_pain_since_open(tmp_path):
 sc=make(tmp_path,nifty(maxpain=[24400.0,24400,24400,24500,24500,24500,24600,24600]),ladder('NIFTY',EXP,STRIKES,N))
 r=sc.results({'conditions':[{'metric':'maxpain','state':'up','window':{'kind':'open'}}]})
 m=r['matches'][0]
 assert m['first_matched']=='10:15' and m['active']
 assert 'Max pain shifted higher since market open: 24,400 → 24,600' in m['because'][0]


def test_liquid_only_drops_contracts_below_the_floors(tmp_path):
 oid={(24500,'CE'):{'oid':[0,1e5,2e5,3e5,4e5,5e5,6e5,7e5],'floors':[0]*N}}
 sc=make(tmp_path,nifty(),ladder('NIFTY',EXP,STRIKES,N,oid))
 assert sc.results(oi_scanner())['matches'][0]['thin'] is True
 assert sc.results(oi_scanner(liquid_only=True))['matches']==[]


def _bsm_prices(strike,sigmas,spot=24500.0,dte=7):
 out=[]
 for i,s in enumerate(sigmas):
  h,mi=map(int,TIMES[i].split(':'))
  years=((dte*24*60)+(15*60+30)-(h*60+mi))/(365*24*60)
  out.append(round(IV.price_bs(spot,strike,years,IV.RISK_FREE_RATE,s,'CE'),2))
 return out


def test_iv_expanding_is_solved_and_labelled_computed(tmp_path):
 sigmas=[0.12,0.12,0.125,0.13,0.135,0.14,0.145,0.15]
 series={(24500,'CE'):{'price':_bsm_prices(24500,sigmas)}}
 sc=make(tmp_path,nifty(),ladder('NIFTY',EXP,STRIKES,N,series))
 r=sc.results({'strikes':{'kind':'atm','below':2,'above':2},'conditions':[
  {'metric':'iv','side':'CE','state':'up_cont','window':{'kind':'minutes','value':45}}]})
 keys=[m['title'] for m in r['matches']]
 assert 'NIFTY · 24,500 CE' in keys
 m=next(m for m in r['matches'] if m['title']=='NIFTY · 24,500 CE')
 assert m['computed'] and 'Call IV expanded at each of the last 3 readings' in m['because'][0]
 assert any(n['kind']=='computed' for n in r['notes'])


def test_expiry_day_rolls_iv_conditions_to_the_next_expiry(tmp_path):
 today,nxt='2026-09-22','2026-09-29'
 books={('NIFTY',today):{'spot':[24500.0]*N,'dte':0},('NIFTY',nxt):{'spot':[24500.0]*N}}
 contracts=ladder('NIFTY',today,STRIKES,N,dte=0)+ladder('NIFTY',nxt,STRIKES,N,start=5000)
 sc=make(tmp_path,books,contracts,session=today)
 iv=sc.results({'conditions':[{'metric':'iv','side':'CE','state':'stable'}]})
 assert any(n['kind']=='expiry_roll' and 'NIFTY' in n['text'] for n in iv['notes'])
 assert all(m['expiry']==nxt for m in iv['matches'])
 oi=sc.results(oi_scanner(state='stable'))
 assert not any(n['kind']=='expiry_roll' for n in oi['notes'])
 assert all(m['expiry']==today for m in oi['matches'])


def test_results_are_cached_until_a_new_reading_lands(tmp_path):
 sc=make(tmp_path,nifty(),ladder('NIFTY',EXP,STRIKES,N))
 a=sc.results(oi_scanner(state='stable'))
 b=sc.results(oi_scanner(state='stable'))
 assert a==b and 'elapsed_ms' in a
