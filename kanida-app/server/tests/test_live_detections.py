"""Live detections of the researched patterns in Discover (docs/LIVE_DETECTION.md).

The real ledger is written by the scanner into its own multi-hundred-megabyte scan cache, so these tests build
their own `detections` table with the SAME schema `market_scanner/pattern_live.SCHEMA` creates, and assert:

 * the catalog's live counts come from the ledger, one aggregate for all 1,048 strategies, and are ABSENT (not
   zero) when the ledger is not there;
 * "detected today", "researched history" and "evidence" stay three separately named numbers;
 * the per-strategy list is bounded, paginates, and reports the real total before the cut;
 * evidence attaches ONLY on identity match - a detection written by a different detector, or one that is a
   different pattern/variant/side/timeframe, gets the contract's "Incompatible historical evidence" and no
   numbers, never another cell's;
 * the legacy 10-pattern block and the legacy trade gates keep working when the scanner runs either set.
"""
import json,sqlite3
from datetime import datetime,timedelta
import pytest
from kanida_pilot import detections as live
from kanida_pilot.app import create_app
from kanida_pilot.errors import PilotError
from test_pilot import Evidence,pilot_settings,signup  # noqa: F401
from test_research_cards import (CATALOGUE,RESEARCH_RUN,ScannerEvidence,build_labels,build_research_tree,specs)  # noqa: F401

# The one strategy the fixtures drive, and the identity it was registered against.
PATTERN,VARIANT,SIDE,TIMEFRAME='CH05','legacy_1.0.1','long','1D'
KEY='ch05-legacy_1.0.1-long-1d'
SPEC_HASH='a'*64            # what the research run declares
OTHER_HASH='b'*64           # a detector that is NOT the research run's
TODAY='2026-09-16'
YESTERDAY='2026-09-15'
OLD='2026-09-01'
SCAN='2026-09-16 15:20:41'
OLDER_SCAN='2026-09-16 11:20:03'

# Exactly market_scanner/pattern_live.SCHEMA. Copied, not imported, so a drift in the scanner's schema shows up
# here as a failing test rather than as a silently different fixture.
SCHEMA='''
CREATE TABLE IF NOT EXISTS detections(
  detection_id TEXT PRIMARY KEY,
  episode_id TEXT NOT NULL, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
  pattern_id TEXT NOT NULL, variant TEXT NOT NULL, side TEXT NOT NULL, family TEXT NOT NULL,
  detector_spec_hash TEXT NOT NULL, live_rules_version TEXT NOT NULL,
  state TEXT NOT NULL, state_reason TEXT, state_at TEXT,
  formation_start TEXT NOT NULL, detected_at TEXT NOT NULL, signal_at TEXT NOT NULL,
  confirmed_at TEXT, direction TEXT, fit_score REAL, atr REAL,
  as_of TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS detections_live ON detections(state, timeframe, pattern_id);
CREATE INDEX IF NOT EXISTS detections_symbol ON detections(symbol, timeframe);
CREATE INDEX IF NOT EXISTS detections_episode ON detections(episode_id);
'''

def row(detection_id,symbol,state='forming',detected=TODAY,*,pattern=PATTERN,variant=VARIANT,side=SIDE,
        timeframe=TIMEFRAME,spec_hash=SPEC_HASH,seen=SCAN,geometry_note='',fit=0.8):
 detected_at=f'{detected} 15:30'
 payload=json.dumps({'pattern_name':'Falling wedge','detector_state':'setup' if state=='forming' else 'confirmed',
  'score':round(fit*100),'bars_since_state':2,'bars_since_signal':2,'price':101.5,'geometry_note':geometry_note,'quality_tags':[]},
  separators=(',',':'))
 return (detection_id,'ep-'+detection_id,symbol,timeframe,pattern,variant,side,'chart',spec_hash,'live-1',
  state,'setup standing',detected_at,f'{detected} 09:15',detected_at,detected_at,
  None if state=='forming' else detected_at,'bullish' if side=='long' else 'bearish',fit,3.25,
  detected_at,seen,seen,payload)

ROWS=[
 # today, the live book
 row('d1','AAAA','forming',TODAY),
 row('d2','BBBB','confirmed',TODAY),
 row('d3','CCCC','forming',TODAY,geometry_note='CDL-style pattern: the detector publishes no drawable geometry'),
 # today but already terminated: still "detected today", no longer live
 row('d4','EEEE','invalidated',TODAY),
 # an older bar, still standing: live but NOT detected today
 row('d5','AAAA','confirmed',YESTERDAY),
 # outside the 7-day window entirely
 row('d6','FFFF','expired',OLD),
 # a detection written by a DIFFERENT detector build - same cell, incompatible identity
 row('d7','AAAA','forming',TODAY,spec_hash=OTHER_HASH),
 # a different cell entirely, so the per-strategy filter has something to exclude
 row('d8','AAAA','forming',TODAY,pattern='CH06',side='short'),
 row('d9','BBBB','forming',TODAY,timeframe='1H'),
 # last seen in an older pass of the same timeframe: reported, but flagged as not current
 row('d10','DDDD','forming',TODAY,seen=OLDER_SCAN),
 # a stock the RESEARCH RUN never studied: the scanner's universe is wider than the research run's, so this
 # is a perfectly valid detection with no cell behind it at all.
 row('d11','ZZZZ','confirmed',TODAY),
]

def build_ledger(path,rows=ROWS):
 c=sqlite3.connect(path);c.executescript(SCHEMA)
 c.executemany('insert into detections values('+','.join('?'*24)+')',rows)
 c.commit();c.close()
 return path

@pytest.fixture(autouse=True)
def frozen_identity(monkeypatch):
 """The research run's detector identity, without reading the real research tree."""
 live._IDENTITY.clear()
 monkeypatch.setattr(live,'identity_source',
  lambda run:{'run':run,'status':'known','detector_spec_hash':SPEC_HASH} if run==RESEARCH_RUN
  else {'run':run,'status':'missing','note':f'No research manifest for {run}'})
 monkeypatch.setattr(live,'today_ist',lambda:TODAY)
 yield
 live._IDENTITY.clear()

def make_app(tmp_path,ledger=True,rows=ROWS):
 directory=tmp_path/'expanded_research';build_research_tree(directory,_SPECS)
 build_labels(tmp_path/'labels.sqlite3')
 path=tmp_path/'scan-cache.sqlite3'
 if ledger:build_ledger(path,rows)
 settings=pilot_settings(tmp_path,pattern_research_directory=str(directory),pattern_catalogue_path=str(CATALOGUE),
  pattern_research_run=RESEARCH_RUN,pattern_labels_database=str(tmp_path/'labels.sqlite3'),
  pattern_detection_db=str(path),pattern_detection_cache_seconds=0)
 app=create_app(settings,evidence=ScannerEvidence())
 assert app.state.strategies.index.build() is True,app.state.strategies.index._last_error
 return app

_SPECS=None
@pytest.fixture(autouse=True,scope='module')
def _load_specs():
 global _SPECS
 data=json.loads(CATALOGUE.read_text(encoding='utf-8'))
 out=[]
 for spec in data['specifications']:
  states=tuple(spec.get('states') or ('confirmed',))
  out.append(dict(pattern_id=spec['pattern_id'],variant=spec['variant'],side=spec['side'],family=spec['family'],
   state=next((s for s in ('confirmed','setup') if s in states),states[0])))
 _SPECS=out
 yield

@pytest.fixture
def app(tmp_path):
 value=make_app(tmp_path);yield value;value.state.db.close()
@pytest.fixture
def client(app):return signup(app)


# --- the ledger reader itself ------------------------------------------------------------------------------
def test_reader_counts_today_week_and_the_live_book_separately(tmp_path):
 reader=live.LiveDetections(build_ledger(tmp_path/'cache.sqlite3'),cache_seconds=0)
 assert reader.available() is True
 snapshot=reader.snapshot(today=TODAY)
 bucket=snapshot['counts'][KEY]
 # d1,d2,d3,d4,d7,d10,d11 closed their signal bar today; d5 is yesterday; d6 is outside the week.
 assert bucket['today']==7 and bucket['week']==8 and bucket['detections']==9
 # the live book is a DIFFERENT question: forming/confirmed now, whenever they were detected.
 assert bucket['live']==7 and bucket['forming']==4 and bucket['confirmed']==3
 assert bucket['invalidated']==1 and bucket['expired']==1
 # the other cells are counted under their own keys, never folded into this one
 assert snapshot['counts']['ch06-legacy_1.0.1-short-1d']['today']==1
 assert snapshot['counts']['ch05-legacy_1.0.1-long-1h']['today']==1
 assert snapshot['rows']==len(ROWS) and snapshot['symbols']==7

def test_reader_reports_a_mixed_detector_identity_rather_than_picking_one(tmp_path):
 reader=live.LiveDetections(build_ledger(tmp_path/'cache.sqlite3'),cache_seconds=0)
 state=reader.state(today=TODAY)
 assert state['mixed_detector_identity'] is True and state['detector_spec_hash'] is None
 single=live.LiveDetections(build_ledger(tmp_path/'one.sqlite3',[r for r in ROWS if r[8]==SPEC_HASH]),cache_seconds=0)
 assert single.state(today=TODAY)['detector_spec_hash']==SPEC_HASH

def test_a_missing_or_legacy_scan_cache_is_unavailable_not_empty(tmp_path):
 assert live.LiveDetections(tmp_path/'nothing.sqlite3').available() is False
 legacy=tmp_path/'legacy.sqlite3'
 connection=sqlite3.connect(legacy);connection.execute('create table cells(a text)');connection.commit();connection.close()
 reader=live.LiveDetections(legacy)
 assert reader.available() is False
 state=reader.state()
 assert state['available'] is False and state['detections']==0
 assert reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME)==([],0)

def test_reader_rows_are_bounded_and_ordered_live_first(tmp_path):
 reader=live.LiveDetections(build_ledger(tmp_path/'cache.sqlite3'),cache_seconds=0)
 rows,total=reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,scope='today',limit=500,today=TODAY)
 assert total==7 and rows[0]['state']=='confirmed','a confirmed detection leads the list'
 assert [r['state'] for r in rows][-1]=='invalidated'
 # an absurd limit is capped, never honoured
 _,capped=reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,limit=10**6,today=TODAY)
 assert capped==7
 page=reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,limit=2,offset=2,today=TODAY)[0]
 assert len(page)==2 and {r['detection_id'] for r in page}.isdisjoint(
  {r['detection_id'] for r in reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,limit=2,today=TODAY)[0]})

def test_reader_marks_marker_only_and_stale_rows(tmp_path):
 reader=live.LiveDetections(build_ledger(tmp_path/'cache.sqlite3'),cache_seconds=0)
 rows={r['symbol']:r for r in reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,scope='today',today=TODAY)[0]}
 assert rows['CCCC']['drawable'] is False and 'no drawable geometry' in rows['CCCC']['geometry_note']
 assert rows['AAAA']['drawable'] is True and rows['AAAA']['geometry_note']==''
 assert rows['DDDD']['current'] is False,'seen only in an older pass of this timeframe'
 assert rows['AAAA']['current'] is True
 assert rows['AAAA']['company']=='Alpha Industries' or rows['AAAA']['company']=='AAAA'

def test_scopes_ask_different_questions(tmp_path):
 reader=live.LiveDetections(build_ledger(tmp_path/'cache.sqlite3'),cache_seconds=0)
 today=reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,scope='today',today=TODAY)[1]
 book=reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,scope='live',today=TODAY)[1]
 week=reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,scope='week',today=TODAY)[1]
 assert (today,book,week)==(7,7,8)
 symbols=lambda scope:{r['detection_id'] for r in reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,scope=scope,today=TODAY)[0]}
 assert 'd5' in symbols('live') and 'd5' not in symbols('today'),'a standing detection from an older bar is live, not today'
 assert 'd4' in symbols('today') and 'd4' not in symbols('live'),'an invalidated detection was still detected today'


# --- evidence identity -------------------------------------------------------------------------------------
def test_identity_match_requires_the_research_run_hash():
 assert live.evidence_identity(RESEARCH_RUN,SPEC_HASH)['status']=='identity_match'
 assert live.evidence_identity(RESEARCH_RUN,OTHER_HASH)['status']=='detector_mismatch'
 assert live.evidence_identity(RESEARCH_RUN,None)['status']=='unavailable'
 assert live.evidence_identity('not-a-run',SPEC_HASH)['status']=='unavailable'
 assert live.evidence_identity('',SPEC_HASH)['status']=='unavailable'


# --- serving -----------------------------------------------------------------------------------------------
def test_catalog_serves_real_live_counts_beside_the_researched_history(client):
 body=client.get('/api/strategies/catalog').json()
 assert body['live']['available'] is True and body['live']['detections']==len(ROWS)
 one=next(s for b in body['blocks'] if b['key']=='chart_patterns' for s in b['strategies'] if s['key']==KEY)
 assert one['live_detection'] is True
 # DETECTED TODAY (the ledger)
 assert one['detections_today']==7 and one['detections_week']==8 and one['detections_live']==7
 # RESEARCHED HISTORY (the frozen run) keeps its own names and is never relabelled as a detection count
 assert one['history_today']==3 and one['researched_stocks']==5
 assert 'found' not in one
 # EVIDENCE (the outcome engine) is a third, separate set of numbers
 assert one['evidence_ready']==3 and one['cells_tested']==1

def test_no_ledger_means_absent_counts_not_zero(tmp_path):
 app=make_app(tmp_path,ledger=False)
 try:
  body=signup(app).get('/api/strategies/catalog').json()
  assert body['live']['available'] is False
  one=next(s for b in body['blocks'] if b['key']=='chart_patterns' for s in b['strategies'] if s['key']==KEY)
  assert one['live_detection'] is False
  assert one['detections_today'] is None and one['detections_week'] is None and one['detections_live'] is None
  assert one['history_today']==3,'the researched history is unchanged when live detection is off'
 finally:app.state.db.close()

def test_detections_endpoint_lists_todays_detections_for_one_strategy(client):
 body=client.get(f'/api/strategies/{KEY}/detections?scope=today').json()
 assert body['source']=='detection_ledger' and body['scope']=='today' and body['total']==7
 assert body['live_detection'] is True and body['live']['available'] is True
 rows={r['symbol']:r for r in body['rows']}
 assert set(rows)=={'AAAA','BBBB','CCCC','DDDD','EEEE','ZZZZ'}
 assert rows['BBBB']['state']=='confirmed' and rows['BBBB']['state_label']=='Confirmed'
 assert rows['AAAA']['state']=='forming' and rows['AAAA']['detected_at']==f'{TODAY} 15:30'
 assert rows['AAAA']['match_id']=='AAAA:1D:CH05:legacy_1.0.1:long'
 assert rows['CCCC']['drawable'] is False and rows['CCCC']['geometry_note']
 # the summary rides along, so the card's header count and its list can never disagree
 assert body['strategy']['detections_today']==7

def test_detections_endpoint_is_bounded_and_paginates(client):
 first=client.get(f'/api/strategies/{KEY}/detections?limit=2').json()
 assert len(first['rows'])==2 and first['total']==7 and first['limit']==2 and first['offset']==0
 second=client.get(f'/api/strategies/{KEY}/detections?limit=2&offset=2').json()
 assert len(second['rows'])==2 and second['offset']==2
 assert {r['detection_id'] for r in first['rows']}.isdisjoint({r['detection_id'] for r in second['rows']})
 # the cap is enforced by the endpoint, so no caller can ask for an unbounded page
 assert client.get(f'/api/strategies/{KEY}/detections?limit={live.ROW_LIMIT_MAX+1}').status_code==400
 assert client.get(f'/api/strategies/{KEY}/detections?scope=nonsense').status_code==400
 assert client.get(f'/api/strategies/{KEY}/detections?limit={live.ROW_LIMIT_MAX}').status_code==200

def test_detections_scopes_are_served_separately(client):
 assert client.get(f'/api/strategies/{KEY}/detections?scope=live').json()['total']==7
 assert client.get(f'/api/strategies/{KEY}/detections?scope=week').json()['total']==8
 assert client.get(f'/api/strategies/{KEY}/detections?scope=today').json()['total']==7

def test_each_row_carries_its_own_evidence_identity(client):
 body=client.get(f'/api/strategies/{KEY}/detections').json()
 rows={r['detection_id']:r for r in body['rows']}
 assert rows['d1']['evidence_compatible'] is True and rows['d1']['evidence']['status']=='identity_match'
 assert rows['d7']['evidence_compatible'] is False and rows['d7']['evidence']['status']=='detector_mismatch'
 assert 'no compatible historical evidence' in rows['d7']['evidence']['note']

def test_the_endpoint_refuses_a_stored_pattern_strategy(client):
 assert client.get('/api/strategies/falling_wedge-1D-long/detections').status_code==409
 assert client.get('/api/strategies/not-a-strategy/detections').status_code==404


# --- evidence attaches only on identity match ---------------------------------------------------------------
def test_a_matching_detection_opens_the_research_card(client):
 card=client.get(f'/api/strategies/{KEY}/card?symbol=AAAA&detection_id=d1').json()
 assert card['identity']['detection_id']=='d1'
 assert card['live_evidence']['status']=='identity_match'
 assert card['evidence_state']=='walkforward_result' and card['summary'] is not None
 assert card['detection']['symbol']=='AAAA' and card['detection']['state']=='forming'

def test_a_detection_from_another_detector_gets_no_numbers(client):
 """d7 is the same stock, pattern, variant, side and timeframe as d1 - only the detector build differs. It must
 NOT inherit d1's card: that would be exactly the identity fallback the evidence contract forbids."""
 good=client.get(f'/api/strategies/{KEY}/card?symbol=AAAA&detection_id=d1').json()
 assert good['summary'] is not None,'the same cell does have numbers when the identity matches'
 card=client.get(f'/api/strategies/{KEY}/card?symbol=AAAA&detection_id=d7').json()
 assert card['live_evidence']['status']=='detector_mismatch'
 assert card['evidence_state']=='incompatible' and card['label']=='Incompatible historical evidence'
 assert card['summary'] is None and card['barriers'] is None and card['forward_curve'] is None
 assert card['conditions'] is None and card['last_occurrences'] is None
 assert 'no compatible historical evidence' in card['note']

def test_a_detection_of_another_cell_never_borrows_this_ones_numbers(client):
 """d8 is CH06/short/1D and d9 is 1H. Passing either to this strategy's card must be refused, not served."""
 for detection_id in ('d8','d9'):
  card=client.get(f'/api/strategies/{KEY}/card?symbol=AAAA&detection_id={detection_id}').json()
  assert card['live_evidence']['status']=='detector_mismatch',detection_id
  assert card['summary'] is None and card['label']=='Incompatible historical evidence'

def test_an_unknown_detection_is_a_404_not_a_borrowed_card(client):
 assert client.get(f'/api/strategies/{KEY}/card?symbol=AAAA&detection_id=deadbeef').status_code==404
 assert client.get(f'/api/strategies/{KEY}/card?symbol=AAAA&detection_id=not-hex!').status_code==400

def test_the_card_without_a_detection_is_exactly_what_it_was(client):
 card=client.get(f'/api/strategies/{KEY}/card?symbol=AAAA').json()
 assert card['identity']['detection_id'] is None and card['detection'] is None and card['live_evidence'] is None
 assert card['evidence_state']=='walkforward_result' and card['summary'] is not None


# --- the legacy block and the legacy gates ------------------------------------------------------------------
def test_the_stored_block_degrades_with_a_reason_when_the_scanner_runs_the_research_set(tmp_path):
 """With the research pattern set on, /api/matches answers with an OBJECT and its matches are research
 detections with no legacy history. The stored block must say so, not silently show zero or borrow them."""
 class ResearchScanner(ScannerEvidence):
  def get(self,path,params=None,cache=False):
   if path=='/api/matches':
    return {'matches':[{'id':'X','symbol':'AAAA','pattern_id':'CH05','pattern':'CH05'}],'total':1,
     'returned':1,'limit':500,'pattern_set':'research'}
   return super().get(path,params,cache)
 directory=tmp_path/'expanded_research';build_research_tree(directory,_SPECS);build_labels(tmp_path/'labels.sqlite3')
 settings=pilot_settings(tmp_path,pattern_research_directory=str(directory),pattern_catalogue_path=str(CATALOGUE),
  pattern_research_run=RESEARCH_RUN,pattern_labels_database=str(tmp_path/'labels.sqlite3'),
  pattern_detection_db=str(build_ledger(tmp_path/'scan-cache.sqlite3')),pattern_detection_cache_seconds=0)
 app=create_app(settings,evidence=ResearchScanner())
 try:
  assert app.state.strategies.index.build() is True
  client=signup(app)
  body=client.get('/api/strategies/catalog').json()
  blocks={b['key']:b for b in body['blocks']}
  assert 'researched pattern set' in (blocks['chart']['unavailable'] or '')
  assert all(s['found'] is None for s in blocks['chart']['strategies']),'never reported as 0 found'
  # the researched blocks are unaffected and now carry real live counts
  assert blocks['chart_patterns']['unavailable'] is None
  one=next(s for s in blocks['chart_patterns']['strategies'] if s['key']==KEY)
  assert one['live_detection'] is True and one['detections_today']==7
 finally:app.state.db.close()

def test_a_legacy_scanner_leaves_every_stored_surface_untouched(client):
 """The ledger is present but the scanner is on the legacy set, which is the normal case while live detection
 is being rolled out: the stored block still serves its own matches."""
 body=client.get('/api/strategies/catalog').json()
 blocks={b['key']:b for b in body['blocks']}
 assert blocks['chart']['unavailable'] is None and body['universe_error'] is None
 assert all(s['found']==0 for s in blocks['chart']['strategies']),'an empty stored scan is a measured zero'
 assert client.get('/api/strategies/falling_wedge-1D-long/results').json()['source']=='stored_scan'


def test_the_legacy_evidence_path_fails_closed_on_a_research_matches_payload():
 """The REAL Evidence.matches() against the research pattern set: an object payload becomes an empty list, so
 the exit-plan / plan / trade path reports SETUP_CHANGED instead of sizing a trade off a research detection
 that the legacy backtest store has never studied."""
 from kanida_pilot.evidence import Evidence as RealEvidence
 class Fake(RealEvidence):
  def __init__(self,payload):self.payload=payload;self.cache={}
  def get(self,path,params=None,cache=False):return self.payload
 research={'matches':[{'id':'X','symbol':'AAAA','pattern':'CH05'}],'total':1,'pattern_set':'research'}
 assert Fake(research).matches()==[]
 with pytest.raises(PilotError) as error:Fake(research).match('X')
 assert error.value.code=='SETUP_CHANGED'
 legacy=[{'id':'Y','symbol':'AAAA','pattern':'cup_handle'}]
 assert Fake(legacy).matches()==legacy,'the legacy list is passed through exactly as before'
 assert Fake(legacy).match('Y')['id']=='Y'


def test_the_aggregate_is_rebuilt_only_when_the_ledger_actually_moves(tmp_path):
 """One full pass writes ~89,000 rows and counting them four ways is a full scan of a ~130 MB table. It is
 redone when the ledger file (or its WAL) moves and not otherwise, so the page never pays for it twice."""
 path=build_ledger(tmp_path/'cache.sqlite3')
 reader=live.LiveDetections(path,cache_seconds=0)
 first=reader.snapshot(today=TODAY)
 assert reader.snapshot(today=TODAY) is first,'an unchanged ledger reuses the aggregate object itself'
 # A different trading day is a different question and is always recomputed.
 assert reader.snapshot(today=YESTERDAY) is not first
 # A committed pass moves the file; the previous aggregate keeps serving while the new one is built, and it
 # carries its own `scanned_at`, so it is never presented as newer than it is.
 connection=sqlite3.connect(path)
 connection.executemany('insert into detections values('+','.join('?'*24)+')',[row('d99','GGGG','forming',TODAY)])
 connection.commit();connection.close()
 served=reader.snapshot(today=TODAY)
 assert served['rows'] in (len(ROWS),len(ROWS)+1)
 for _ in range(200):
  if reader.snapshot(today=TODAY)['rows']==len(ROWS)+1:break
  import time as _t;_t.sleep(0.02)
 assert reader.snapshot(today=TODAY)['rows']==len(ROWS)+1,'the refresh lands without a request ever blocking on it'

def test_the_list_query_reaches_the_scanners_own_index(tmp_path):
 """Without a predicate on `state` -- the leading column of `detections_live` -- every card list would be a
 full scan. The states come from the ledger itself, so the constraint can never exclude a real row."""
 path=build_ledger(tmp_path/'cache.sqlite3')
 reader=live.LiveDetections(path,cache_seconds=0)
 assert set(reader.snapshot(today=TODAY)['present_states'])=={'forming','confirmed','invalidated','expired'}
 connection=sqlite3.connect(f'file:{path.as_posix()}?mode=ro',uri=True)
 plan=' '.join(r[3] for r in connection.execute(
  "explain query plan select * from detections where state in ('forming','confirmed','invalidated','expired')"
  ' and timeframe=? and pattern_id=? and variant=? and side=?',(TIMEFRAME,PATTERN,VARIANT,SIDE)))
 connection.close()
 assert 'detections_live' in plan and 'SCAN detections' not in plan,plan

def test_a_state_the_scanner_adds_later_is_still_listed(tmp_path):
 """The state list is read from the ledger, never assumed, so a lifecycle the scanner adds after this code
 was written widens the query instead of silently dropping its detections."""
 rows=list(ROWS)+[row('d98','GGGG','superseded',TODAY)]
 reader=live.LiveDetections(build_ledger(tmp_path/'cache.sqlite3',rows),cache_seconds=0)
 assert 'superseded' in reader.snapshot(today=TODAY)['present_states']
 listed=reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,scope='today',today=TODAY)[0]
 assert 'd98' in {r['detection_id'] for r in listed}


# --- a detection on a stock the research run never studied ---------------------------------------------------
def test_an_unstudied_stock_says_so_instead_of_not_enough_history(client):
 """ZZZZ is detected by the scanner but is outside the research run's universe. There is no cell to be
 compatible WITH, so no numbers are served and the reason says exactly that - "Not enough historical data"
 would be a claim about a measurement that was never attempted."""
 rows={r['detection_id']:r for r in client.get(f'/api/strategies/{KEY}/detections').json()['rows']}
 assert rows['d11']['symbol']=='ZZZZ'
 assert rows['d11']['evidence']['status']=='not_studied' and rows['d11']['evidence_compatible'] is False
 assert 'never studied ZZZZ' in rows['d11']['evidence']['note']
 assert rows['d1']['evidence']['status']=='identity_match','a studied stock is unaffected'
 assert rows['d7']['evidence']['status']=='detector_mismatch','the detector check still comes first'
 card=client.get(f"/api/strategies/{KEY}/card?symbol=ZZZZ&detection_id=d11").json()
 assert card['live_evidence']['status']=='not_studied'
 assert card['summary'] is None and card['label']!='Not enough historical data'
 assert 'never studied ZZZZ' in card['live_evidence']['note']

def test_a_studied_cell_with_no_occurrences_is_not_called_unstudied(client):
 """DDDD's cell exists in the research run and simply never occurred. The Discover index deliberately stores
 no ROW for it, so "was this studied?" must be answered from `cells`, not from the index."""
 rows={r['detection_id']:r for r in client.get(f'/api/strategies/{KEY}/detections').json()['rows']}
 assert rows['d10']['symbol']=='DDDD' and rows['d10']['evidence']['status']=='identity_match'
 card=client.get(f"/api/strategies/{KEY}/card?symbol=DDDD&detection_id=d10").json()
 assert card['evidence_state']=='no_occurrences' and card['label']=='No occurrences in this historical sample'


# --- the stored block while the scanner runs the researched set -----------------------------------------------
def _research_scanner_app(tmp_path):
 class ResearchScanner(ScannerEvidence):
  def get(self,path,params=None,cache=False):
   if path=='/api/matches':
    return {'matches':[],'total':0,'returned':0,'limit':500,'pattern_set':'research'}
   if path=='/api/state':
    return dict(super().get(path,params,cache),pattern_set='research')
   return super().get(path,params,cache)
 directory=tmp_path/'expanded_research';build_research_tree(directory,_SPECS);build_labels(tmp_path/'labels.sqlite3')
 settings=pilot_settings(tmp_path,pattern_research_directory=str(directory),pattern_catalogue_path=str(CATALOGUE),
  pattern_research_run=RESEARCH_RUN,pattern_labels_database=str(tmp_path/'labels.sqlite3'),
  pattern_detection_db=str(build_ledger(tmp_path/'scan-cache.sqlite3')),pattern_detection_cache_seconds=0)
 app=create_app(settings,evidence=ResearchScanner())
 assert app.state.strategies.index.build() is True
 return app

def test_the_stored_block_is_omitted_on_the_research_set(tmp_path):
 """BACKLOG 2.6: hidden, not explained. The stored 10-pattern block is not in the Discover catalog at all while
 the scanner runs the research set, and the admin registry still lists it."""
 app=_research_scanner_app(tmp_path)
 try:
  client=signup(app)
  body=client.get('/api/strategies/catalog').json()
  assert body['pattern_set']=='research'
  blocks={b['key']:b for b in body['blocks']}
  assert 'chart' not in blocks,'no banner, no collapsed row - the block is simply not served'
  assert not any(s['source_type']=='stored_pattern' for b in body['blocks'] for s in b['strategies'])
  # the researched blocks are unaffected
  assert blocks['chart_patterns']['superseded'] is False and blocks['chart_patterns']['unavailable'] is None
  # the owner's registry keeps it
  registry=app.state.strategies.admin()
  assert any(b['key']=='chart' for b in registry['blocks'])
 finally:app.state.db.close()

def test_the_stored_block_is_served_as_before_on_the_legacy_set(client):
 body=client.get('/api/strategies/catalog').json()
 assert body['pattern_set']=='legacy'
 blocks={b['key']:b for b in body['blocks']}
 assert blocks['chart']['superseded'] is False and blocks['chart']['superseded_note'] is None
 assert blocks['chart']['unavailable'] is None
 assert all(s['found']==0 for s in blocks['chart']['strategies'])
 # a stored block keeps the registry's own default slots - live defaults apply to researched blocks only
 assert all('registry_default_slot' not in s for s in blocks['chart']['strategies'])

def test_the_history_count_is_servable_without_the_per_card_results_fetch(client):
 """The catalog summary already carries `researched_stocks`, so the "Researched history (N)" tab never has to
 wait on (or read a zero from) the per-card results request."""
 body=client.get('/api/strategies/catalog').json()
 one=next(s for b in body['blocks'] if b['key']=='chart_patterns' for s in b['strategies'] if s['key']==KEY)
 assert one['researched_stocks']==5
 assert client.get(f'/api/strategies/{KEY}/results').json()['total']==one['researched_stocks']


# --- the DISPLAY match set vs the TRADE match set --------------------------------------------------------
# Two different sets, and the difference is the whole point: the chart workspace, the legend and the watchlist
# draw today's detections, while the exit plan and the trade gates stay legacy-only and fail closed.
RESEARCH_MATCH={'id':'AAAA:1D:CH05:legacy_1.0.1:long','symbol':'AAAA','company':'Alpha Industries','sector':'Auto',
 'timeframe':'1D','pattern':'CH05','pattern_name':'Falling wedge','pattern_id':'CH05','variant':'legacy_1.0.1',
 'side':'long','family':'chart','direction':'bullish','state':'forming','detector_state':'setup','live':True,
 'detection_id':'d1','score':61,'price':101.5,'candle_end':'2026-09-16 15:30','current':True,'start_index':3,
 'universes':['nifty500'],'history':[]}
def research_payload(matches):
 return {'matches':matches,'total':len(matches),'returned':len(matches),'limit':500,'pattern_set':'research'}

class _Fake:
 """The real Evidence over a stubbed scanner, so the two sets are exercised through the real code path."""
 def __init__(self,state,matches):
  from kanida_pilot.evidence import Evidence as RealEvidence
  self.calls=[]
  class Stub(RealEvidence):
   def __init__(inner):inner.cache={}
   def get(inner,path,params=None,cache=False):
    self.calls.append((path,dict(params or {})))
    return state if path=='/api/state' else matches
  self.evidence=Stub()

def test_display_matches_pass_the_legacy_list_through_untouched():
 legacy=[{'id':'X','symbol':'AAAA','pattern':'cup_handle','history':[{'side':'long'}]}]
 fake=_Fake({'pattern_set':'legacy'},legacy)
 assert fake.evidence.display_matches({'min_trades':'0'})==legacy
 # and the request is the one the app has always made - no narrowing, no extra filters
 assert ('/api/matches',{'min_trades':'0'}) in fake.calls

def test_display_matches_unwrap_research_detections_without_any_history():
 fake=_Fake({'pattern_set':'research'},research_payload([RESEARCH_MATCH]))
 rows=fake.evidence.display_matches({'min_trades':'0'})
 assert len(rows)==1
 row=rows[0]
 # `state` becomes the detector's OWN event, which is the vocabulary the legacy surfaces read; the research
 # lifecycle is kept beside it rather than overwritten.
 assert row['state']=='setup' and row['lifecycle']=='forming' and row['pattern_set']=='research'
 # The one thing that must never happen: a research detection carrying legacy evidence.
 assert row['history']==[]
 assert row['symbol']=='AAAA' and row['universes']==['nifty500'] and row['detection_id']=='d1'
 # the live book on the newest candle, capped - never the whole 50,000-row book
 path,params=next(c for c in fake.calls if c[0]=='/api/matches')
 assert params['live']=='true' and params['current']=='true'
 assert int(params['limit'])==500

def test_display_matches_keep_one_row_per_cell():
 """A scanner match id identifies a CELL, not an episode, so two standing episodes share one id. The served
 ids must stay unique or the workspace's own id lookups would collide."""
 second=dict(RESEARCH_MATCH,detection_id='d2',score=40)
 fake=_Fake({'pattern_set':'research'},research_payload([RESEARCH_MATCH,second]))
 rows=fake.evidence.display_matches({})
 assert len(rows)==1 and rows[0]['detection_id']=='d1','the best-fitting episode is kept'

def test_the_display_call_never_asks_for_a_legacy_history_screen():
 """A research detection has no legacy history, so the scanner refuses a screen over it (400) and reports
 `history_screen.applied: false` on the rest. The DISPLAY call must therefore never send one - and a bare
 call must not inherit the scanner's own DEFAULT_MIN_TRADES either, which is what emptied this page."""
 fake=_Fake({'pattern_set':'research'},research_payload([RESEARCH_MATCH]))
 assert fake.evidence.display_matches({})
 path,params=next(c for c in fake.calls if c[0]=='/api/matches')
 assert not {'performance','return_band'}&set(params)
 assert params.get('min_trades') in (None,'0')

@pytest.mark.parametrize('params',[{'min_trades':'10'},{'performance':'positive'},{'return_band':'1_2'},{'min_trades':'x'}])
def test_a_legacy_history_screen_is_refused_on_the_research_set(params):
 """The scanner answers this 400. Refusing it here gives the caller the same honest answer instead of an
 upstream 4xx, which the last-good cache would otherwise read as an outage."""
 fake=_Fake({'pattern_set':'research'},research_payload([RESEARCH_MATCH]))
 with pytest.raises(PilotError) as error:fake.evidence.display_matches(params)
 assert error.value.status==400 and error.value.code=='NO_LEGACY_HISTORY'
 assert not [c for c in fake.calls if c[0]=='/api/matches'],'the scanner is never asked'

def test_a_legacy_history_screen_still_works_on_the_legacy_set():
 legacy=[{'id':'X','symbol':'AAAA','pattern':'cup_handle','history':[{'side':'long'}]}]
 fake=_Fake({'pattern_set':'legacy'},legacy)
 assert fake.evidence.display_matches({'min_trades':'10'})==legacy
 assert ('/api/matches',{'min_trades':'10'}) in fake.calls

def test_the_trade_match_set_stays_legacy_only():
 fake=_Fake({'pattern_set':'research'},research_payload([RESEARCH_MATCH]))
 assert fake.evidence.display_matches({})  # display has rows...
 assert fake.evidence.matches()==[]        # ...and the trade path has none
 with pytest.raises(PilotError) as error:fake.evidence.match(RESEARCH_MATCH['id'])
 assert error.value.code=='SETUP_CHANGED'

def test_the_matches_endpoint_serves_the_display_set(client):
 body=client.get('/api/matches?min_trades=0').json()
 assert isinstance(body,list),'the app reads a list on either pattern set'

def test_watching_a_detection_works_while_the_trade_gates_stay_closed(client,app):
 """Watching is a display action. It must work on either pattern set, and it must not open any trade path."""
 evidence=app.state.evidence
 evidence.display_match=lambda identity:dict(RESEARCH_MATCH) if identity==RESEARCH_MATCH['id'] else (_ for _ in ()).throw(PilotError(409,'SETUP_CHANGED','gone'))
 added=client.post('/api/product/watch',json={'action':'add','match_id':RESEARCH_MATCH['id']})
 assert added.status_code==200,added.text
 watch=client.get('/api/product').json()['watchlist']
 assert [w['symbol'] for w in watch]==['AAAA'] and watch[0]['pattern']=='CH05'
 # Watching resolved against the DISPLAY set; the trade path is a different set entirely, and
 # test_the_trade_match_set_stays_legacy_only asserts it stays empty for a research detection.
 evidence.display_match=lambda identity:(_ for _ in ()).throw(PilotError(409,'SETUP_CHANGED','gone'))
 gone=client.post('/api/product/watch',json={'action':'add','match_id':'NOPE:1D:CH05:legacy_1.0.1:long'})
 assert gone.status_code==409 and gone.json()['code']=='SETUP_CHANGED'

def test_the_stored_results_endpoint_degrades_instead_of_failing(tmp_path):
 """A 503 here would take the admin page, the preview and every stored read down although nothing is broken."""
 app=_research_scanner_app(tmp_path)
 try:
  client=signup(app)
  body=client.get('/api/strategies/falling_wedge-1D-long/results')
  assert body.status_code==200,body.text
  value=body.json()
  assert value['total']==0 and value['rows']==[] and value['source']=='stored_scan'
  assert 'researched pattern set' in (value['unavailable'] or '')
  assert value['strategy']['found']==0
  # the admin preview rides the same path
  preview=client.post('/api/admin/strategies/falling_wedge-1D-long/preview',json={})
  assert preview.status_code==200 and preview.json()['unavailable']
 finally:app.state.db.close()

def test_the_stored_results_endpoint_is_unchanged_on_the_legacy_set(client):
 value=client.get('/api/strategies/falling_wedge-1D-long/results').json()
 assert value['total']==0 and value['unavailable'] is None,'an empty legacy scan is a measured zero, with no reason'


# --- the default view is the ACTIVE book, not "first detected today" --------------------------------------
def test_the_default_scope_is_every_active_setup_newest_first(client):
 """A setup that formed yesterday and is still valid is what a trader needs; "today only" hid it."""
 body=client.get(f'/api/strategies/{KEY}/detections').json()
 assert body['scope']=='live' and body['total']==7
 ids=[r['detection_id'] for r in body['rows']]
 assert 'd5' in ids,'an older detection that is still confirmed is listed'
 assert 'd4' not in ids and 'd6' not in ids,'terminated detections are not active'
 assert ids[-1]=='d5','newest detection first, so the older one reads as older'
 assert all(r['state'] in ('forming','confirmed') for r in body['rows'])
 assert body['rows'][0]['bars_since_signal']==2,'each row carries its own age'
 # `active` is the same scope under the label the card uses
 assert client.get(f'/api/strategies/{KEY}/detections?scope=active').json()['total']==7

def test_the_summary_says_when_the_ledger_last_saw_a_strategy(client):
 body=client.get('/api/strategies/catalog').json()
 one=next(s for b in body['blocks'] if b['key']=='chart_patterns' for s in b['strategies'] if s['key']==KEY)
 assert one['detections_last_detected']==f'{TODAY} 15:30'
 other=next(s for b in body['blocks'] if b['key']=='chart_patterns' for s in b['strategies'] if s['key']=='ch01-legacy_1.0.1-long-1h')
 assert other['detections_live']==0 and other['detections_last_detected'] is None,'nothing seen is None, never a date'

def test_spotted_today_means_the_latest_session_not_the_calendar_day(tmp_path):
 """After midnight IST, on a weekend or a holiday the calendar day has no candle. "Spotted today" is measured
 against the newest session the ledger holds for the timeframe, so the card does not go blank overnight."""
 reader=live.LiveDetections(build_ledger(tmp_path/'cache.sqlite3'),cache_seconds=0)
 overnight=reader.snapshot(today='2026-09-17')['counts'][KEY]
 assert overnight['today']==7 and overnight['week']==8,'the next calendar day still reads the latest session'
 assert reader.snapshot(today='2026-09-17')['timeframes']['1D']['session']==TODAY
 rows,total=reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,scope='today',today='2026-09-20')
 assert total==7 and {r['detected_day'] for r in rows}=={TODAY}


# --- BACKLOG 2.1: no card opens empty - defaults are the strategies with active setups ---------------------------
from kanida_pilot.strategies import live_default_slots  # noqa: E402

def _slots(strategies):
 return {s['default_slot']:s for s in strategies if s.get('default_slot')}

def test_catalog_defaults_are_the_two_strategies_with_the_most_active_setups(client):
 body=client.get('/api/strategies/catalog').json()
 block=next(b for b in body['blocks'] if b['key']=='chart_patterns')
 live=[s for s in block['strategies'] if (s['detections_live'] or 0)>0]
 assert len(live)>=2,'the fixture ledger must give this block two strategies with active setups'
 slots=_slots(block['strategies'])
 assert set(slots)=={'A','B'} and slots['A']['key']!=slots['B']['key']
 assert slots['A']['detections_live']>0 and slots['B']['detections_live']>0,'no default card opens empty'
 top=sorted(live,key=lambda s:(-s['detections_live'],s['order'],s['key']))[:2]
 assert [slots['A']['key'],slots['B']['key']]==[t['key'] for t in top]
 assert slots['A']['detections_live']>=slots['B']['detections_live']
 # the owner's registry choice is still served, under its own name
 assert any(s.get('registry_default_slot')=='A' for s in block['strategies'])

def _summary(key,live,slot=None,order=0):
 return dict(key=key,order=order,default_slot=slot,detections_live=live,source_type='research_pattern')

def test_live_defaults_fall_back_to_the_registry_only_without_live_setups():
 none=live_default_slots([_summary('a',0,'A',0),_summary('b',None,'B',1),_summary('c',0,None,2)])
 assert {s['key']:s['default_slot'] for s in none}=={'a':'A','b':'B','c':None},'no live detections: registry slots'
 one=live_default_slots([_summary('a',0,'A',0),_summary('b',0,'B',1),_summary('c',5,None,2)])
 assert {s['key']:s['default_slot'] for s in one}=={'a':'B','b':None,'c':'A'},'the live one leads, registry A fills the other'
 same=live_default_slots([_summary('a',3,'A',0),_summary('b',0,'B',1),_summary('c',0,None,2)])
 slots={s['default_slot']:s['key'] for s in same if s['default_slot']}
 assert slots=={'A':'a','B':'b'},'never the same strategy in both slots'
 many=live_default_slots([_summary('a',0,'A',0),_summary('b',0,'B',1),_summary('c',2,None,2),_summary('d',9,None,3),_summary('e',2,None,4)])
 assert {s['default_slot']:s['key'] for s in many if s['default_slot']}=={'A':'d','B':'c'},'ties go to registry order'
 assert all(s['registry_default_slot']==('A' if s['key']=='a' else 'B' if s['key']=='b' else None) for s in many)


# --- BACKLOG 2.2: the small "new" marker ------------------------------------------------------------------------
def test_a_row_is_new_only_when_first_found_on_the_latest_session(client):
 rows={r['detection_id']:r for r in client.get(f'/api/strategies/{KEY}/detections').json()['rows']}
 assert rows['d1']['new'] is True and rows['d2']['new'] is True,'first found on the newest 1D session'
 assert rows['d5']['new'] is False,'still standing, but first found on an earlier session'
 assert all(isinstance(r['new'],bool) for r in rows.values())

def test_the_new_marker_follows_the_session_not_the_calendar_day(tmp_path,monkeypatch):
 """Overnight / at a weekend the newest session is still YESTERDAY's; its rows stay new."""
 monkeypatch.setattr(live,'today_ist',lambda:'2026-09-19')
 reader=live.LiveDetections(build_ledger(tmp_path/'cache.sqlite3'),cache_seconds=0)
 found,_=reader.rows(PATTERN,VARIANT,SIDE,TIMEFRAME,scope='live',today='2026-09-19')
 by={r['detection_id']:r for r in found}
 assert by['d1']['new'] is True and by['d5']['new'] is False


# --- plain block wording: the seed moves untouched old descriptions, never an edited one -------------------------
def test_the_seed_rewrites_only_untouched_old_block_descriptions(app):
 from sqlalchemy import update
 from kanida_pilot.db import strategy_blocks
 from kanida_pilot.strategies import OLD_RESEARCH_BLOCK_DESCRIPTIONS
 registry=app.state.strategies
 with app.state.db.tx() as c:
  c.execute(update(strategy_blocks).where(strategy_blocks.c.key=='chart_patterns').values(description=OLD_RESEARCH_BLOCK_DESCRIPTIONS['chart_patterns']))
  c.execute(update(strategy_blocks).where(strategy_blocks.c.key=='harmonics').values(description='Owner wording, researched.'))
 registry.seed()
 blocks={b['key']:b for b in registry.admin_blocks()['blocks']}
 assert blocks['chart_patterns']['description'].startswith('Chart formations:')
 assert 'Researched' not in blocks['chart_patterns']['description']
 assert blocks['harmonics']['description']=='Owner wording, researched.','an edited description is left as saved'
