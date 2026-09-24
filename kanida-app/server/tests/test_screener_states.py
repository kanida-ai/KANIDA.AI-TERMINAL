"""The screener's state words are the Derivative tab's own rules — checked three ways.

1. Hand-worked cases for each rule and each composed word (continuously, rapidly, reversing, not enough readings).
2. `Series` (the O(1) prefix version the evaluator runs) equals the reference functions at EVERY reading of
   random series, for every family, window and state.
3. The reference functions reproduce `fixtures/screener_parity.json`; `scripts/check-screener.cjs` runs the
   TypeScript originals (logic.ts gridDirection / gridPriceDirection / sessionDirection) on the same file. A
   rule that drifts on either side fails one of the two.
"""
import json,os,random
from pathlib import Path
import pytest
from kanida_pilot.screener import states as S

FIXTURE=Path(__file__).parent/'fixtures'/'screener_parity.json'
DIRECTIONAL=['up','down','up_cont','down_cont','up_rapid','down_rapid','stable','rev_up','rev_down']


def test_grid_oi_matches_logic_ts_shape():
 assert S.grid_oi([100])is None
 assert S.grid_oi([0,50,100,150,200],4)=='up'
 assert S.grid_oi([200,150,100],4)=='down'
 # the flat band is 5% of the contract's own largest |ΔOI| today
 assert S.grid_oi([1000,1000,1040],1)=='flat'
 assert S.grid_oi([1000,1000,1060],1)=='up'
 assert S.grid_oi([0,0,0],1)=='flat'


def test_grid_price_band_is_the_days_own_range():
 assert S.grid_price([100,110,120],1)=='up'
 assert S.grid_price([100,120,120.5],1)=='flat'   # 0.5 < 5% of the 20-point range
 assert S.grid_price([100,120,119.5],1)=='flat'
 assert S.grid_price([100,120,119],1)=='down'     # exactly 5% is not inside the band: logic.ts uses `<`


def test_session_direction_uses_less_or_equal_like_logic_ts():
 assert S.session([1,2],1)=='up'
 assert S.session([5,5],1)=='flat'
 assert S.session([1.0,1.0,0.0],1)=='down'


def test_continuously_needs_every_interval():
 up,info=S.read('session',[1,2,3,4],3,'up_cont')
 assert up is True and info['step_directions']==['up','up','up']
 broken,_=S.read('session',[1,2,2,4],3,'up_cont')
 assert broken is False
 assert S.read('session',[1,2,3,4],3,'up')[0] is True


def test_a_window_needs_every_reading_it_names():
 held,info=S.read('session',[1,2],3,'up_cont')
 assert held is None and info['need']==4


def test_rapidly_is_the_pace_band():
 # +1 then +2: the latest move is 2x the one before (> PACE_UP 1.25)
 assert S.read('session',[10,11,13],2,'up_rapid')[0] is True
 # +1 then +1.2: continuing, not speeding up
 assert S.read('session',[10,11,12.2],2,'up_rapid')[0] is False
 assert S.read('session',[13,11,10],2,'down_rapid')[0] is False
 assert S.read('session',[13,12,10],2,'down_rapid')[0] is True


def test_reversing():
 assert S.read('session',[10,8,6,4,7],3,'rev_up')[0] is True
 assert S.read('session',[4,6,8,10,7],3,'rev_down')[0] is True
 assert S.read('session',[4,6,8,10,12],3,'rev_down')[0] is False


def test_pace_never_compares_across_a_gap():
 assert S.pace([1,2,4])==2.0
 assert S.pace([1,2,4],at=[0,1,3]) is None
 fast=S.Series([1,2,None,4])
 assert fast.pace(fast.index(3)) is None


def test_pace_words():
 assert S.pace_word(1.3)=='strengthening' and S.pace_word(0.7)=='weakening' and S.pace_word(1.0) is None


def test_flow_is_flow_labels():
 assert S.flow('CE',[100,95,90],[0,100,200],2)[0]=='writing'
 assert S.flow('PE',[50,60,70],[0,100,200],2)[0]=='buying'
 assert S.flow('CE',[50,60,70],[300,200,100],2)[0]=='short_covering'
 assert S.FLOW_TEXT[('PE','writing')]=='Put writing increasing'


def test_breadth_word():
 ladder=[100,110,120,130]
 assert S.breadth_word([110],ladder)=='isolated'
 assert S.breadth_word([110,120],ladder)=='clustered'
 assert S.breadth_word([100,130],ladder)=='dispersed'


def _series(rng,n):
 shape=rng.choice(['walk','flat','steps','zeros','gaps'])
 v=[];x=rng.uniform(-50,50)
 for _ in range(n):
  if shape=='flat':x+=rng.choice([0,0,0,0.01])
  elif shape=='steps':x+=rng.choice([-10,0,10,20])
  elif shape=='zeros':x=0.0
  else:x+=rng.gauss(0,5)
  v.append(round(x,3))
 if shape=='gaps':v=[None if rng.random()<0.25 else y for y in v]
 return v


@pytest.mark.parametrize('seed',range(12))
def test_fast_series_equals_reference_at_every_reading(seed):
 rng=random.Random(seed)
 for _ in range(15):
  values=_series(rng,rng.randint(2,26))
  fast=S.Series(values)
  for family in ('grid_oi','grid_price','session'):
   for i in range(len(values)):
    j=fast.index(i)
    if j is None:continue
    upto=[x for x in values[:i+1] if x is not None]
    at=[x for x in range(i+1) if values[x] is not None]
    for lb in (1,2,3,4):
     assert fast.direction(family,j,lb)==S.direction(family,upto,lb),(family,values,i,lb)
    for k in (1,2,3,4):
     for state in DIRECTIONAL:
      a,ai=fast.read(family,j,k,state);b,bi=S.read(family,upto,k,state,at)
      assert a==b,(family,state,k,values,i)


def _parity_cases():
 rng=random.Random(2026)
 cases=[]
 for _ in range(60):
  values=[x for x in _series(rng,rng.randint(2,20)) if x is not None] or [0.0,0.0]
  lb=rng.choice([1,2,3,4])
  cases.append({'values':values,'lookback':lb,
   'grid_oi':S.grid_oi(values,lb),'grid_price':S.grid_price(values,lb),'session':S.session(values,lb)})
 return cases


def test_reference_rules_reproduce_the_parity_fixture():
 cases=_parity_cases()
 if os.getenv('SCREENER_REGEN_PARITY') or not FIXTURE.exists():
  FIXTURE.parent.mkdir(exist_ok=True)
  FIXTURE.write_text(json.dumps({'constants':{'GRID_FLAT_FRACTION':S.GRID_FLAT_FRACTION,
   'GRID_PRICE_FLAT_FRACTION':S.GRID_PRICE_FLAT_FRACTION,'SESSION_FLAT_FRACTION':S.SESSION_FLAT_FRACTION,
   'PACE_UP':S.PACE_UP,'PACE_DOWN':S.PACE_DOWN},'cases':cases},indent=1),encoding='utf-8')
 stored=json.loads(FIXTURE.read_text(encoding='utf-8'))
 assert stored['cases']==cases
 assert stored['constants']['PACE_UP']==S.PACE_UP
