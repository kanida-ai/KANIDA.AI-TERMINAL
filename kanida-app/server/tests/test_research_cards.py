"""Researched pattern catalogue in Discover: registry seed at scale, catalog/results from the precomputed index,
and the trader evidence card with the EVIDENCE_SERVING_CONTRACT.md §6 label table.

The real research tree is multi-gigabyte and is being written by a live outcome run, so these tests build their
own small research/outcome databases in tmp_path with the SAME schema, and load the REAL frozen catalogue
(docs/pattern_research/IMPLEMENTED_CATALOGUE.json) so the 262 x 4 = 1,048 strategy scale is genuine.
"""
import json,sqlite3,time
from pathlib import Path
import pytest
from fastapi.testclient import TestClient
from kanida_pilot.app import create_app
from kanida_pilot import cards
from kanida_pilot.research_store import ResearchStore,strategy_key
from kanida_pilot.errors import PilotError
from test_pilot import Evidence,pilot_settings,signup  # noqa: F401

CATALOGUE=Path(__file__).resolve().parents[3]/'docs'/'pattern_research'/'IMPLEMENTED_CATALOGUE.json'
RESEARCH_RUN='testrun0001';OUTCOME_RUN='testout0001';SNAPSHOT='snap_test';SOURCE_RUN='m15test'
SYMBOLS=['AAAA','BBBB','CCCC','DDDD','EEEE','FFFF']
# One symbol per evidence state, so every row of the contract's label table is exercised by real stored data.
PLAN={'AAAA':'tested','BBBB':'small','CCCC':'none','DDDD':'no_occurrences','EEEE':'insufficient','FFFF':'loading'}

RESEARCH_SCHEMA="""
CREATE TABLE runs(id TEXT PRIMARY KEY, status TEXT NOT NULL, metadata TEXT NOT NULL);
CREATE TABLE cells(run TEXT NOT NULL,symbol TEXT NOT NULL,timeframe TEXT NOT NULL,pattern_id TEXT NOT NULL,
 variant TEXT NOT NULL,side TEXT NOT NULL,status TEXT NOT NULL,occurrences INTEGER NOT NULL,reference_n INTEGER NOT NULL,
 wf_n INTEGER NOT NULL,reference_mean REAL,wf_mean REAL,summary TEXT NOT NULL,artifact TEXT NOT NULL,
 PRIMARY KEY(run,symbol,timeframe,pattern_id,variant,side));
"""
OUTCOMES_SCHEMA="""
CREATE TABLE outcome_runs(id TEXT PRIMARY KEY, research_run TEXT NOT NULL, source_run TEXT, snapshot_id TEXT,
 engine_version TEXT NOT NULL, created_at TEXT NOT NULL, status TEXT NOT NULL, metadata TEXT NOT NULL);
CREATE TABLE outcome_symbols(run TEXT NOT NULL,symbol TEXT NOT NULL,status TEXT NOT NULL,seconds REAL,cells INTEGER,
 occurrences INTEGER,history_sha256 TEXT,metadata TEXT NOT NULL,PRIMARY KEY(run,symbol));
CREATE TABLE cell_outcomes(run TEXT NOT NULL,research_run TEXT NOT NULL,snapshot_id TEXT,history_sha256 TEXT,
 symbol TEXT NOT NULL,timeframe TEXT NOT NULL,pattern_id TEXT NOT NULL,variant TEXT NOT NULL,side TEXT NOT NULL,
 state TEXT NOT NULL,family TEXT,max_horizon INTEGER,events INTEGER,admitted INTEGER,occurrences INTEGER,
 sample_label TEXT,mfe_flatten_h_90pct INTEGER,mfe_flatten_h_95pct INTEGER,mfe_terminal_median_pct REAL,
 selection_status TEXT,selected_horizon INTEGER,in_sample_median_training_expectancy_pct REAL,oos_n INTEGER,
 oos_expectancy_pct REAL,oos_win_rate_pct REAL,oos_sample_label TEXT,baseline_status TEXT,baseline_window_coverage REAL,
 baseline_window_mismatch INTEGER,baseline_mean_net_return_pct REAL,baseline_diff_mean_net_return_pct REAL,
 baseline_diff_ci95_low REAL,baseline_diff_ci95_high REAL,baseline_beats_unconditional INTEGER,stability_horizon INTEGER,
 stability_horizon_source TEXT,stability_years INTEGER,stability_positive_years INTEGER,stability_edge_concentrated INTEGER,
 oos_p_value REAL,oos_p_value_t REAL,oos_p_value_bootstrap REAL,oos_q_value REAL,fdr_trials INTEGER,discovery_q10 INTEGER,
 discovery_q05 INTEGER,display_barrier_id TEXT,barrier_n INTEGER,p_target_first REAL,p_stop_first REAL,p_neither REAL,
 median_bars_to_target REAL,median_bars_to_stop REAL,return_peak_h INTEGER,return_peak_net_return_pct REAL,
 market_cap_tier TEXT,sector TEXT,is_fno INTEGER,display_grid TEXT,last_occurrences TEXT,summary TEXT NOT NULL,
 PRIMARY KEY(run,symbol,timeframe,pattern_id,variant,side,state));
CREATE TABLE bucket_outcomes(run TEXT NOT NULL,research_run TEXT NOT NULL,symbol TEXT NOT NULL,timeframe TEXT NOT NULL,
 pattern_id TEXT NOT NULL,variant TEXT NOT NULL,side TEXT NOT NULL,state TEXT NOT NULL,horizon INTEGER,
 dimension TEXT NOT NULL,bucket TEXT NOT NULL,n INTEGER,sample_label TEXT,status TEXT,win_rate_pct REAL,
 mean_net_return_pct REAL,median_net_return_pct REAL,median_mfe_pct REAL,median_mae_pct REAL,baseline_scope TEXT,
 baseline_n INTEGER,baseline_mean_net_return_pct REAL,diff_mean_net_return_pct REAL,p_value REAL,p_value_method TEXT,
 p_value_vs_zero REAL,q_value REAL,fdr_trials INTEGER,discovery_q10 INTEGER,discovery_q05 INTEGER,detail TEXT NOT NULL,
 PRIMARY KEY(run,symbol,timeframe,pattern_id,variant,side,state,dimension,bucket));
"""
GRID={'1H':[1,2,4,8,12,24],'4H':[1,2,3,5,8,10],'1D':[1,2,3,5,10],'1W':[1,2,4,8,12]}
# The shared instrument catalogue the stored-scan path reads (market_scanner/data.py `universe`).
LABELS_SCHEMA="""
CREATE TABLE instrument_labels(symbol TEXT,exchange TEXT,instrument_type TEXT,sector TEXT,company TEXT,
 company_name TEXT,is_active INTEGER);
"""
COMPANIES={'AAAA':('AAAA Industries Ltd.','Power'),'BBBB':('BBBB Motors Ltd.','Automobile'),
 'CCCC':('CCCC Cements Ltd.','Construction'),'DDDD':('DDDD Pharma Ltd.','Healthcare'),
 'EEEE':('EEEE Bank Ltd.','Financial Services')}  # FFFF deliberately absent: it falls back to its symbol
def build_labels(path):
 c=sqlite3.connect(path);c.executescript(LABELS_SCHEMA)
 c.executemany('insert into instrument_labels values(?,?,?,?,?,?,?)',
  [(sym,'NSE','STOCK',sector,company,company,1) for sym,(company,sector) in COMPANIES.items()])
 c.commit();c.close()

def summary_json(timeframe,horizon):
 grid=GRID[timeframe]
 horizons=[dict(h=h,n=120,sample_label='adequate',mean_net_return_pct=round(-0.4+0.35*i,3),
   median_net_return_pct=round(-0.5+0.30*i,3),win_rate_pct=round(40+2.0*i,3),
   median_mfe_pct=round(1.0+0.5*i,3),median_mae_pct=round(0.8+0.3*i,3),
   percentiles=dict(p10=-3.0,p25=-1.5,p50=-0.5,p75=1.5,p90=3.0)) for i,h in enumerate(sorted(set(grid+[horizon])))]
 base=[dict(h=x['h'],baseline_n=9000,conditional_n=120,baseline_mean_net_return_pct=round(x['mean_net_return_pct']-0.3,3),
   baseline_win_rate_pct=round(x['win_rate_pct']-4.0,3),diff_mean_net_return_pct=0.3,diff_excludes_zero=False) for x in horizons]
 buckets=[dict(dimension='volume',bucket='high',n=64),dict(dimension='volume',bucket='low',n=9),
  dict(dimension='regime',bucket='up',n=50),dict(dimension='quality_tertile',bucket='q3_high',n=44)]
 return json.dumps(dict(
  outcome_engine_version='1.2.0',timeframe=timeframe,side='long',state='confirmed',max_horizon=max(grid),
  occurrences=120,sample_label='adequate',horizons=horizons,
  baseline=dict(status='tested',horizons=base,method='eligible_bar_entries_same_window_same_cost_stationary_block_bootstrap'),
  barriers=dict(tie_rule='stop_first_when_both_touched_in_the_same_bar',display_barrier_id='pct:2.0:1.0',
   grid=['pct:2.0:1.0'],rows=[dict(id='pct:2.0:1.0',unit='pct',target=2.0,stop=1.0,n=120,sample_label='adequate',
    both_touched_same_bar_n=7,n_target=77,p_target=64.0,n_stop=35,p_stop=29.0,n_neither=8,p_neither=7.0,
    bars_to_target=dict(n=77,p25=2.0,median=4.0,p75=7.0),bars_to_stop=dict(n=35,p25=1.0,median=3.0,p75=5.0))]),
  buckets=dict(horizon=horizon,minimum_sample=30,regime_source='stock_own_200_bar_trend',
   definitions=dict(volume='signal-bar volume / median of the prior 20 bars; high >= 1.50x, low <= 0.70x',
    regime='200-bar return; up > +5%, down < -5%',quality='score tertiles within the cell'),rows=buckets),
  selection=dict(status='tested',horizon=horizon,
   out_of_sample=dict(scope='test_folds_at_the_training_selected_horizon',sample_label='adequate',
    stats=dict(n=46,win_rate_pct=61.0,expectancy_pct=1.4,expectancy_ci95=[0.2,2.6])),
   in_sample=dict(horizon=horizon,scope='training_folds_only_never_an_expected_return',stats=dict(n=90,expectancy_pct=2.9))),
  last_occurrences=dict(barrier_id='pct:2.0:1.0',tie_rule='stop_first_when_both_touched_in_the_same_bar',
   note='bars_held is the first touch of the display barrier'),
  display_grid=dict(declared=grid,shown=grid,dropped_beyond_horizon=[],max_horizon=max(grid)),
  assumptions=dict(costs='Fixed 0.4% of entry notional charged once at the measured horizon',
   entry='Next observed open after the signal bar; signal or entry gap disqualifies')),separators=(',',':'))

LAST=json.dumps([dict(entry_date=f'2026-09-{d:02d}',entry_time=f'2026-09-{d:02d} 15:30:00',entry_price=100.0+d,
 horizon=8,net_return_pct=v,mfe_pct=abs(v)+2.0,mae_pct=1.2,bars_held=3,outcome='target_hit' if v>0 else 'stop_hit')
 for d,v in zip((15,12,10,8,4),(1.8,2.4,-0.9,3.1,0.6))])

def build_research_tree(directory,specs,outcome_status='running'):
 directory.mkdir(parents=True,exist_ok=True)
 research=sqlite3.connect(directory/'research.sqlite3');research.executescript(RESEARCH_SCHEMA)
 research.execute('insert into runs values(?,?,?)',(RESEARCH_RUN,'complete','{}'))
 outcomes=sqlite3.connect(directory/'outcomes.sqlite3');outcomes.executescript(OUTCOMES_SCHEMA)
 outcomes.execute('insert into outcome_runs values(?,?,?,?,?,?,?,?)',
  (OUTCOME_RUN,RESEARCH_RUN,SOURCE_RUN,SNAPSHOT,'1.2.0','2026-09-16T07:13:07+00:00',outcome_status,'{}'))
 cells=[];outs=[];buckets=[]
 for spec in specs:
  for timeframe in ('1D','1H','4H','1W'):
   for symbol in SYMBOLS:
    plan=PLAN[symbol];horizon=8
    status={'tested':'tested_positive','small':'small_walkforward_sample','none':'no_walkforward_trades',
     'no_occurrences':'no_occurrences','insufficient':'insufficient_history','loading':'tested_positive'}[plan]
    occurrences=0 if plan=='no_occurrences' else 120
    cells.append((RESEARCH_RUN,symbol,timeframe,spec['pattern_id'],spec['variant'],spec['side'],status,occurrences,
     110,46 if plan=='tested' else 0,1.1,1.4 if plan=='tested' else None,'{}','artifact'))
    if plan in ('no_occurrences','insufficient','loading'):continue  # `loading` has no outcome row on purpose
    selection={'tested':'tested','small':'small_out_of_sample_sample','none':'insufficient'}[plan]
    tested=plan=='tested'
    outs.append(dict(run=OUTCOME_RUN,research_run=RESEARCH_RUN,snapshot_id=SNAPSHOT,history_sha256='sha',symbol=symbol,
     timeframe=timeframe,pattern_id=spec['pattern_id'],variant=spec['variant'],side=spec['side'],state=spec['state'],
     family=spec['family'],max_horizon=max(GRID[timeframe]),events=121,admitted=120,occurrences=120,
     sample_label='adequate',mfe_flatten_h_90pct=5,mfe_flatten_h_95pct=8,mfe_terminal_median_pct=3.7,
     selection_status=selection,selected_horizon=horizon,in_sample_median_training_expectancy_pct=2.9,
     # `none` = measured, no accepted walk-forward selection: no sample and no edge, as in the real data.
     oos_n=46 if tested else (6 if plan=='small' else 0),
     oos_expectancy_pct=1.4 if tested else (-0.3 if plan=='small' else None),
     oos_win_rate_pct=61.0 if tested else (33.0 if plan=='small' else None),
     oos_sample_label='adequate',baseline_status='tested',baseline_window_coverage=0.99,baseline_window_mismatch=0,
     baseline_mean_net_return_pct=0.42,baseline_diff_mean_net_return_pct=0.98,
     baseline_diff_ci95_low=0.31 if tested else (-0.9 if plan=='small' else None),
     baseline_diff_ci95_high=1.65 if plan!='none' else None,
     baseline_beats_unconditional=1 if tested else 0,stability_horizon=horizon,stability_horizon_source='selected',
     stability_years=12,stability_positive_years=9,stability_edge_concentrated=0,oos_p_value=0.031,oos_p_value_t=0.030,
     oos_p_value_bootstrap=0.031,oos_q_value=0.004 if tested else 0.42,fdr_trials=4820,discovery_q10=1 if tested else 0,
     discovery_q05=0,display_barrier_id='pct:2.0:1.0',barrier_n=120,p_target_first=64.0,p_stop_first=29.0,p_neither=7.0,
     median_bars_to_target=4.0,median_bars_to_stop=3.0,return_peak_h=horizon,return_peak_net_return_pct=1.9,
     market_cap_tier='large',sector='Power',is_fno=1,
     display_grid=json.dumps(dict(declared=GRID[timeframe],shown=GRID[timeframe],dropped_beyond_horizon=[],
      max_horizon=max(GRID[timeframe]))),last_occurrences=LAST,summary=summary_json(timeframe,horizon)))
    for dimension,bucket,n in (('volume','high',64),('volume','low',9),('regime','up',50),('quality_tertile','q3_high',44)):
     buckets.append((OUTCOME_RUN,RESEARCH_RUN,symbol,timeframe,spec['pattern_id'],spec['variant'],spec['side'],spec['state'],
      horizon,dimension,bucket,n,'adequate','tested' if n>=30 else 'insufficient',58.0,1.9,1.2,3.0,1.4,
      'unconditional_entries_in_the_same_bucket',5000,0.4,1.5,0.02,'stationary_bootstrap_of_the_difference',0.01,0.06,4820,1,0,'{}'))
 research.executemany('insert into cells values('+','.join('?'*14)+')',cells)
 columns=[d[1] for d in outcomes.execute('PRAGMA table_info(cell_outcomes)')]
 assert set(columns)==set(outs[0]),'fixture drifted from the cell_outcomes schema'
 outcomes.executemany('insert into cell_outcomes('+','.join(columns)+') values('+','.join(':'+c for c in columns)+')',outs)
 outcomes.executemany('insert into bucket_outcomes values('+','.join('?'*31)+')',buckets)
 outcomes.executemany('insert into outcome_symbols values(?,?,?,?,?,?,?,?)',
  [(OUTCOME_RUN,s,'complete',1.0,10,120,'sha','{}') for s in SYMBOLS if PLAN[s]!='loading'])
 research.commit();research.close();outcomes.commit();outcomes.close()

@pytest.fixture(scope='session')
def specs():
 assert CATALOGUE.is_file(),f'frozen catalogue missing at {CATALOGUE}'
 data=json.loads(CATALOGUE.read_text(encoding='utf-8'))
 out=[]
 for spec in data['specifications']:
  states=tuple(spec.get('states') or ('confirmed',))
  out.append(dict(pattern_id=spec['pattern_id'],variant=spec['variant'],side=spec['side'],family=spec['family'],
   state=next((s for s in ('confirmed','setup') if s in states),states[0])))
 assert len(out)==262 and len({s['pattern_id'] for s in out})==107
 return out

class ScannerEvidence(Evidence):
 """Adds the scanner responses the stored-scan block needs, so the researched blocks are tested beside a
 working stored block rather than in place of one."""
 available=True
 def get(self,path,params=None,cache=False):
  if path=='/api/filter-options':return dict(universes=[dict(value='nifty500',label='Nifty 500',count=501)])
  if path=='/api/matches':
   if not self.available:raise PilotError(503,'RESEARCH_UNAVAILABLE','Strategy results are unavailable right now. Retry shortly.')
   return []
  return super().get(path,params,cache)

@pytest.fixture
def research_app(tmp_path,specs):
 directory=tmp_path/'expanded_research';build_research_tree(directory,specs)
 build_labels(tmp_path/'labels.sqlite3')
 settings=pilot_settings(tmp_path,pattern_research_directory=str(directory),pattern_catalogue_path=str(CATALOGUE),
  pattern_research_run=RESEARCH_RUN,pattern_labels_database=str(tmp_path/'labels.sqlite3'))
 app=create_app(settings,evidence=ScannerEvidence())
 assert app.state.strategies.index.build() is True,app.state.strategies.index._last_error
 yield app;app.state.db.close()

@pytest.fixture
def client(research_app):return signup(research_app)

# --- registry ---------------------------------------------------------------
def test_seed_registers_1048_research_strategies_in_four_blocks(research_app):
 catalog=research_app.state.strategies.admin()
 blocks={b['key']:b for b in catalog['blocks']}
 assert {'chart_patterns','candlestick','price_action','harmonics'}<=set(blocks)
 assert blocks['chart_patterns']['enabled'] is True
 assert not any(blocks[k]['enabled'] for k in ('candlestick','price_action','harmonics')),'only Chart patterns ships enabled'
 research=[s for b in catalog['blocks'] for s in b['strategies'] if s['source_type']=='research_pattern']
 assert len(research)==1048,f'262 combinations x 4 timeframes, got {len(research)}'
 assert len({s['key'] for s in research})==1048
 counts={}
 for s in research:counts[s['block_key']]=counts.get(s['block_key'],0)+1
 assert counts=={'chart_patterns':45*4,'candlestick':165*4,'price_action':26*4,'harmonics':26*4}
 # The 10-pattern stored-scan block is untouched.
 stored=[s for b in catalog['blocks'] for s in b['strategies'] if s['source_type']=='stored_pattern']
 assert len(stored)==52 and blocks['chart']['enabled'] is True
 one=next(s for s in research if s['key']=='ch05-legacy_1.0.1-long-1d')
 assert one['name'].startswith('Falling Wedge') and one['timeframe']=='1D' and one['side']=='long'
 assert one['pattern_id']=='CH05' and one['variant']=='legacy_1.0.1' and one['research_run']==RESEARCH_RUN
 assert one['min_trades']==cards.WALKFORWARD_MIN and one['default_slot']=='A'
 assert one['description'] and one['tags']==['Chart patterns','Bullish','1D']

def test_seed_is_idempotent_and_keeps_owner_edits(research_app,client):
 registry=research_app.state.strategies
 before=len([s for b in registry.admin()['blocks'] for s in b['strategies']])
 assert client.patch('/api/admin/strategies/ch05-legacy_1.0.1-long-1d',json={'name':'Owner renamed'}).status_code==200
 assert registry.seed() is False
 after=registry.admin()['blocks']
 assert len([s for b in after for s in b['strategies']])==before
 renamed=next(s for b in after for s in b['strategies'] if s['key']=='ch05-legacy_1.0.1-long-1d')
 assert renamed['name']=='Owner renamed','re-seeding must never overwrite an admin edit'

# --- index / catalog / results ---------------------------------------------
def test_index_state_reports_run_identity_and_coverage(research_app):
 state=research_app.state.strategies.research_state()
 assert state['research_run']==RESEARCH_RUN and state['outcome_run']==OUTCOME_RUN
 assert state['snapshot_id']==SNAPSHOT and state['source_run']==SOURCE_RUN and state['engine_version']=='1.2.0'
 assert state['strategies']==1048 and state['symbols_research']==len(SYMBOLS)
 assert state['symbols_with_evidence']==len([s for s in SYMBOLS if PLAN[s] not in ('no_occurrences','insufficient','loading')])
 assert state['publication']['publication_status']=='unreviewed' and state['publication']['released'] is False

def test_catalog_serves_research_blocks_fast(client):
 started=time.perf_counter();result=client.get('/api/strategies/catalog');elapsed=time.perf_counter()-started
 assert result.status_code==200,result.text
 body=result.json();blocks={b['key']:b for b in body['blocks']}
 assert 'chart_patterns' in blocks and 'candlestick' not in blocks,'disabled blocks stay out of the catalog'
 assert len(blocks['chart_patterns']['strategies'])==45*4
 assert blocks['chart_patterns']['unavailable'] is None and body['universe_error'] is None
 assert elapsed<2.0,f'catalog took {elapsed:.2f}s'
 one=next(s for s in blocks['chart_patterns']['strategies'] if s['key']=='ch05-legacy_1.0.1-long-1d')
 assert one['cells']==len(SYMBOLS) and one['cells_with_evidence']==3 and one['cells_tested']==1
 assert one['cells_loading']==1 and one['evidence_pending'] is False
 # The counts are separate and named. Researched-history recency lives under history_*; detections_* come from
 # the live ledger and stay empty until live detection is switched on. Neither is ever called "found".
 assert one['live_detection'] is False and one['researched_stocks']==len(SYMBOLS)-1  # DDDD never occurred
 assert one['history_today']==3 and one['history_week']==3  # 3 cells last occurred on the newest bar
 assert 'found' not in one,'the history list size is never served under the name "found"'
 assert not one['detections_today'],'with no live detector there are no detections to report'
 assert one['evidence_ready']==3 and one['evidence_total']==len(SYMBOLS)-1
 assert body['research']['strategies']==1048

def test_a_down_stored_scan_never_blanks_the_research_blocks(client,research_app):
 """The two sources are independent: if the scanner is unavailable the stored block says so and the researched
 blocks still serve. An unavailable source is never reported as "0 found"."""
 research_app.state.evidence.available=False
 body=client.get('/api/strategies/catalog').json();blocks={b['key']:b for b in body['blocks']}
 assert blocks['chart']['unavailable'] and body['universe_error']
 assert all(s['found'] is None for s in blocks['chart']['strategies'])
 assert blocks['chart_patterns']['unavailable'] is None
 assert len(blocks['chart_patterns']['strategies'])==45*4
 # A bad universe from the caller is still a validation error, not a degraded source.
 assert client.get('/api/strategies/catalog?universe=unknown').status_code==400

def test_results_rank_by_measured_edge_and_label_every_row(client):
 result=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results')
 assert result.status_code==200,result.text
 body=result.json();rows={r['symbol']:r for r in body['rows']}
 assert body['source']=='research_index' and body['total']==len([s for s in SYMBOLS if PLAN[s]!='no_occurrences'])
 assert rows['AAAA']['evidence_state']=='walkforward_result' and rows['AAAA']['label']=='Historical walk-forward result'
 assert rows['BBBB']['evidence_state']=='limited_sample' and rows['BBBB']['label']=='Limited historical sample'
 assert rows['CCCC']['evidence_state']=='no_walkforward_trades' and rows['CCCC']['label']=='No selected walk-forward trades'
 assert rows['EEEE']['evidence_state']=='insufficient_history' and rows['EEEE']['label']=='Not enough historical data'
 assert rows['FFFF']['evidence_state']=='loading','a symbol the outcome run has not reached is loading, not zero'
 assert 'DDDD' not in rows,'a cell with no occurrences is counted, not listed as a result'
 assert rows['AAAA']['edge_low_pct']==0.31 and rows['AAAA']['beats_baseline'] is True
 # §6: a thin sample still shows its real number, next to its sample size and its label - it is just never ranked on.
 assert rows['BBBB']['edge_low_pct']==-0.9 and rows['BBBB']['walkforward_n']==6
 assert [r['symbol'] for r in body['rows']][0]=='AAAA'
 summary=body['strategy']
 assert summary['best_edge_low_pct']==0.31 and summary['best_edge_symbol']=='AAAA','ranking ignores thin samples'
 assert body['live_detection'] is False and summary['live_detection'] is False
 assert body['coverage']['ready']==3 and body['coverage']['total']==5

def test_results_paginate_and_search(client):
 first=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results?limit=2').json()
 assert len(first['rows'])==2 and first['total']==5
 second=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results?limit=2&offset=2').json()
 assert [r['symbol'] for r in first['rows']]!=[r['symbol'] for r in second['rows']]
 found=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results?q=aaaa').json()
 assert [r['symbol'] for r in found['rows']]==['AAAA'] and found['total']==1

def test_catalog_and_results_stay_fast_at_full_scale(client):
 """Every enabled block, then a results read on each of 40 strategies: the request path only reads the index."""
 for block in ('candlestick','price_action','harmonics'):
  assert client.patch(f'/api/admin/blocks/{block}',json={'enabled':True}).status_code==200
 started=time.perf_counter();body=client.get('/api/strategies/catalog').json();catalog_s=time.perf_counter()-started
 total=sum(len(b['strategies']) for b in body['blocks'] if b['key'] in ('chart_patterns','candlestick','price_action','harmonics'))
 assert total==1048,total
 keys=[s['key'] for b in body['blocks'] if b['key']=='candlestick' for s in b['strategies']][:40]
 started=time.perf_counter()
 for key in keys:assert client.get(f'/api/strategies/{key}/results?limit=50').status_code==200
 per_call=(time.perf_counter()-started)/len(keys)
 assert catalog_s<3.0 and per_call<0.25,f'catalog {catalog_s:.2f}s, results {per_call*1000:.0f}ms each'

# --- the trader evidence card ----------------------------------------------
def test_card_sections_are_in_the_owner_order_with_real_numbers(client):
 body=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol=AAAA').json()
 assert body['sections']==['summary','barriers','forward_curve','conditions','last_occurrences','honesty']
 assert body['evidence_state']=='walkforward_result' and body['label']=='Historical walk-forward result'
 summary=body['summary']
 # The fixture's out-of-sample win rate is 61%; its unconditional baseline at the same horizon is 44%.
 assert summary['win_rate_pct']==61.0 and summary['baseline_win_rate_pct']==44.0
 assert summary['win_rate_text']=='61% vs 44% for the stock alone'
 assert summary['win_rate_scope']=='out_of_sample' and summary['walkforward_n']==46
 assert summary['occurrences']==120 and summary['median_net_return_pct'] is not None
 assert summary['median_mfe_pct'] is not None and summary['median_mae_pct'] is not None
 assert summary['window_text']=='most of the move happens within 5–8 daily candles'
 barriers=body['barriers']
 assert barriers['headline']=='hit +2.0% before −1.0%: 64% · hit −1.0% first: 29% · neither: 7%'
 assert barriers['tie_rule']=='stop_first_when_both_touched_in_the_same_bar' and 'stop is counted first' in barriers['tie_rule_text']
 assert barriers['median_bars_to_target']==4.0 and barriers['median_bars_to_stop']==3.0
 curve=body['forward_curve']
 assert curve['grid']==GRID['1D'] and [p['h'] for p in curve['points']]==GRID['1D']
 assert curve['peak_h']==max(curve['points'],key=lambda p:p['median_net_return_pct'])['h']
 assert all(p['baseline_mean_net_return_pct'] is not None for p in curve['points'])
 groups={g['dimension']:g for g in body['conditions']['groups']}
 assert list(groups)==['volume','regime','quality_tertile']
 volume={b['bucket']:b for b in groups['volume']['buckets']}
 assert volume['high']['win_rate_pct']==58.0 and volume['high']['baseline_mean_net_return_pct']==0.4
 assert volume['low']['enough'] is False and volume['low']['note']=='not enough cases'
 assert volume['low']['win_rate_pct'] is None,'a thin bucket shows no rate at all'
 last=body['last_occurrences']
 assert len(last['rows'])==5 and last['read']=='4 of the last 5 were positive after costs.'
 assert last['rows'][0]['date']=='2026-09-15' and last['rows'][0]['best_move_pct'] is not None
 assert any('0.40% included' in line for line in body['honesty'])
 assert any('next open after the signal bar' in line for line in body['honesty'])
 assert any('out-of-sample' in line for line in body['honesty'])

def test_card_significance_says_plainly_when_it_fails_correction(client):
 passes=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol=AAAA').json()['significance']
 assert passes['q_value']==0.004 and 'Survives multiple-testing correction' in passes['line']
 fails=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol=CCCC').json()
 line=cards.significance_line(0.42,0.031,4820,'walkforward_result')
 assert line=='Not statistically distinguishable from chance after correcting for the number of patterns tested (q = 0.42 across 4,820 tested cells).'
 assert fails['evidence_state']=='no_walkforward_trades'

def test_card_states_use_the_contract_labels_and_never_borrow_numbers(client):
 # Cells that HAVE their own occurrences still show their own descriptive numbers, scoped honestly; cells with
 # nothing of their own show nothing at all.
 for symbol,state,label,has_numbers in (('BBBB','limited_sample','Limited historical sample',True),
   ('CCCC','no_walkforward_trades','No selected walk-forward trades',True),
   ('DDDD','no_occurrences','No occurrences in this historical sample',False),
   ('EEEE','insufficient_history','Not enough historical data',False),
   ('FFFF','loading','Evidence loading',False)):
  body=client.get(f'/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol={symbol}').json()
  assert body['evidence_state']==state and body['label']==label,(symbol,body['evidence_state'],body['label'])
  if has_numbers:
   assert body['summary'] and body['barriers'] and body['forward_curve']
   if state=='no_walkforward_trades':
    assert body['summary']['win_rate_scope']=='all_history_descriptive'
    assert 'not an expected return' in body['note']
  else:
   assert body['summary'] is None and body['barriers'] is None and body['forward_curve'] is None
   assert body['last_occurrences'] is None,'a card with no compatible evidence shows nothing, not another cell'
   assert body['note']
 # A stock the research run never studied is not "loading" - there is nothing pending for it.
 unknown=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol=ZZZZ').json()
 assert unknown['evidence_state']=='insufficient_history' and unknown['label']=='Not enough historical data'
 assert unknown['summary'] is None
 # FFFF WAS studied; only the outcome engine has not reached it, so it stays pending.
 pending=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol=FFFF').json()
 assert pending['evidence_state']=='loading' and 'still being computed' in pending['note']

def test_card_label_table_is_complete_and_verbatim():
 assert cards.LABELS=={'insufficient_history':'Not enough historical data',
  'no_occurrences':'No occurrences in this historical sample','no_walkforward_trades':'No selected walk-forward trades',
  'limited_sample':'Limited historical sample','walkforward_result':'Historical walk-forward result',
  'incompatible':'Incompatible historical evidence','requires_review':'Historical data requires review'}
 assert cards.label_for('loading')=='Evidence loading'

def test_publication_gate_withholds_numbers_until_a_decision_is_recorded(research_app,client,tmp_path):
 card=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol=AAAA').json()
 assert card['review_required'] is True and card['review_label']=='Historical data requires review'
 assert any('requires review' in line for line in card['honesty']),'an unreviewed release says so on every card'
 release=tmp_path/'evidence_release.json'
 release.write_text(json.dumps({'runs':{RESEARCH_RUN:{'publication_status':'withheld_source_quality_review'}}}),encoding='utf-8')
 withheld=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol=AAAA').json()
 assert withheld['evidence_state']=='requires_review' and withheld['label']=='Historical data requires review'
 assert withheld['summary'] is None,'a withheld release must not serve its numbers'
 release.write_text(json.dumps({'runs':{RESEARCH_RUN:{'publication_status':'released','decided_by':'owner',
  'source_quality_status':'resolved'}}}),encoding='utf-8')
 released=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol=AAAA').json()
 assert released['review_required'] is False and released['review_label'] is None and released['summary'] is not None

def test_card_refuses_evidence_from_a_different_research_run(research_app,client):
 store=research_app.state.strategies.store
 original=store.research_run;store.research_run='someotherrun'
 try:
  body=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol=AAAA').json()
  assert body['evidence_state']=='incompatible' and body['label']=='Incompatible historical evidence'
  assert body['summary'] is None
 finally:store.research_run=original

def test_card_is_refused_for_a_stored_scan_strategy(client):
 result=client.get('/api/strategies/falling_wedge-1D-long/card?symbol=AAAA')
 assert result.status_code==409 and result.json()['code']=='SOURCE_UNSUPPORTED'

def test_research_endpoints_need_a_member(research_app):
 anonymous=TestClient(research_app)
 for path in ('/api/strategies/research','/api/strategies/ch05-legacy_1.0.1-long-1d/card?symbol=AAAA',
   '/api/strategies/ch05-legacy_1.0.1-long-1d/results'):
  assert anonymous.get(path).status_code in (401,403),path

def test_admin_can_add_a_catalogue_combination_only(client):
 ok=client.post('/api/admin/strategies',json=dict(source_type='research_pattern',block_key='harmonics',
  pattern_id='HA01',variant='canonical',side='short',timeframe='4H'))
 assert ok.status_code in (200,409),ok.text  # already seeded: the point is that it validates, not that it inserts
 bad=client.post('/api/admin/strategies',json=dict(source_type='research_pattern',block_key='harmonics',
  pattern_id='HA01',variant='not_a_real_variant',side='long',timeframe='4H'))
 assert bad.status_code==400 and bad.json()['code']=='PATTERN_NOT_FOUND'


# --- the three live-page fixes ---------------------------------------------
def test_rows_carry_company_and_sector_like_the_stored_scan(client):
 rows={r['symbol']:r for r in client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results').json()['rows']}
 assert rows['AAAA']['company']=='AAAA Industries Ltd.' and rows['AAAA']['sector']=='Power'
 assert rows['BBBB']['company']=='BBBB Motors Ltd.' and rows['CCCC']['sector']=='Construction'
 assert not any(r['company']==sym for sym,r in rows.items() if sym in COMPANIES),'a labelled stock never shows its symbol twice'
 assert rows['FFFF']['company']=='FFFF','an unlabelled stock degrades to its symbol rather than breaking'

def test_rows_with_evidence_sort_above_rows_still_loading(client):
 rows=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results').json()['rows']
 flags=[r['has_evidence'] for r in rows]
 assert flags==sorted(flags,reverse=True),'evidence-bearing rows come first'
 assert rows[0]['has_evidence'] is True and rows[-1]['has_evidence'] is False
 assert rows[-1]['evidence_state']=='loading' and rows[-1]['label']=='Evidence loading'
 assert [r['symbol'] for r in rows if r['has_evidence']][0]=='AAAA','secondary order unchanged'

def test_results_report_detections_and_history_as_separate_numbers(client):
 body=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results').json();st=body['strategy']
 assert st['live_detection'] is False,'researched patterns have no live detector yet'
 assert st['researched_stocks']==5 and st['history_today']==3 and st['history_week']==3
 assert not st['detections_today'] and not st['detections_week'],'no detector, so no detections'
 assert st['researched_stocks']!=st['detections_today'],'the history list size is never a detection count'
 assert 'found' not in st,'the history list size is never served under the name "found"'
 assert body['coverage']['ready']==3 and body['coverage']['total']==5

def test_search_matches_company_as_well_as_symbol(client):
 body=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results?q=motors').json()
 assert [r['symbol'] for r in body['rows']]==['BBBB'] and body['total']==1

def test_index_refreshes_when_the_outcome_run_completes(research_app,tmp_path):
 """A finishing outcome run keeps its id and only flips status, so staleness must watch the status too -
 otherwise the last symbols would wait for the refresh timer. No restart is involved."""
 index=research_app.state.strategies.index
 assert index.state()['stale'] is False and index.state()['current_outcome_status']=='running'
 outcomes=sqlite3.connect(tmp_path/'expanded_research'/'outcomes.sqlite3')
 outcomes.execute("update outcome_runs set status='complete' where id=?",(OUTCOME_RUN,));outcomes.commit();outcomes.close()
 state=index.state()
 assert state['current_outcome_status']=='complete' and state['indexed_outcome_status']=='running'
 assert state['stale'] is True,'a completed run marks the index stale although the run id is unchanged'
 assert index.build() is True
 after=index.state()
 assert after['indexed_outcome_status']=='complete' and after['stale'] is False

def test_a_failed_rebuild_keeps_serving_the_previous_index(research_app):
 index=research_app.state.strategies.index;before=index.state()['rows']
 original=index.store.research_run;index.store.research_run=None  # provokes a build failure
 try:
  assert index.build() is False and index._last_error
 finally:index.store.research_run=original
 assert index.state()['rows']==before,'a failed build never empties the serving index'
 assert index.build() is True and index._last_error is None


# --- the index must always recover by itself -------------------------------
def _store_and_index(tmp_path,specs,path=None):
 from kanida_pilot.research_index import ResearchIndex
 from kanida_pilot.research_store import ResearchStore
 directory=tmp_path/'expanded_research'
 if not (directory/'research.sqlite3').is_file():build_research_tree(directory,specs)
 store=ResearchStore(directory,RESEARCH_RUN,CATALOGUE,tmp_path/'release.json')
 return store,ResearchIndex(store,path or (tmp_path/'index.sqlite3'))

def test_a_schema_only_index_is_always_stale_and_rebuilds(tmp_path,specs):
 """The exact failure seen on the live page: a migration dropped the tables and left a file holding only
 `schema_version`, so the app said "still being indexed" forever. Metadata alone must never look fresh."""
 store,index=_store_and_index(tmp_path,specs)
 assert index.build() is True,index._last_error
 assert index.state()['stale'] is False and index.state()['populated'] is True
 # Reduce it to exactly what was on disk: schema-only, with stale metadata left behind.
 connection=sqlite3.connect(index.path)
 connection.execute('delete from strategy_stats');connection.execute('delete from strategy_rows')
 connection.commit();connection.close()
 index._local.read=None
 state=index.state()
 assert state['populated'] is False
 assert state['stale'] is True,'an index with no rows is stale whatever its metadata says'
 assert index.build() is True,index._last_error
 assert index.state()['populated'] is True and index.stats(),'it repopulates itself'

def test_ensure_starts_a_build_on_an_empty_index(tmp_path,specs):
 store,index=_store_and_index(tmp_path,specs)
 assert index.state()['populated'] is False
 assert index.ensure() is True,'a fresh, empty index must kick a build'
 for _ in range(600):
  if not index._building and index._populated():break
  time.sleep(0.1)
 index._local.read=None
 assert index.state()['populated'] is True and index.state()['stale'] is False

def test_migration_from_an_older_schema_rebuilds_end_to_end(tmp_path,specs):
 """Boot on an older index file: the tables are dropped, and the very next ensure() repopulates them with the
 new columns. This is the whole path the live page was stuck in."""
 from kanida_pilot.research_index import ResearchIndex,SCHEMA_VERSION
 from kanida_pilot.research_store import ResearchStore
 directory=tmp_path/'expanded_research';build_research_tree(directory,specs)
 path=tmp_path/'legacy.sqlite3'
 legacy=sqlite3.connect(path)
 legacy.executescript('CREATE TABLE index_meta(key TEXT PRIMARY KEY,value TEXT);'
  'CREATE TABLE strategy_stats(strategy_key TEXT PRIMARY KEY,found INTEGER);'
  'CREATE TABLE strategy_rows(strategy_key TEXT,symbol TEXT,PRIMARY KEY(strategy_key,symbol));')
 # Metadata that would otherwise satisfy every staleness comparison.
 for key,value in (('schema_version','1'),('research_run',RESEARCH_RUN),('outcome_run',OUTCOME_RUN),
   ('outcome_status','running'),('rows','999'),('built_at','2099-01-01T00:00:00+00:00')):
  legacy.execute('insert into index_meta values(?,?)',(key,value))
 legacy.execute("insert into strategy_rows values('ch05-legacy_1.0.1-long-1d','STALE')")
 legacy.commit();legacy.close()
 store=ResearchStore(directory,RESEARCH_RUN,CATALOGUE,tmp_path/'release.json')
 index=ResearchIndex(store,path)   # boot
 assert index.state()['populated'] is False,'the migration dropped the unreadable tables'
 assert index.state()['stale'] is True,'and left the index needing a build'
 assert index.ensure() is True
 for _ in range(600):
  if not index._building and index._populated():break
  time.sleep(0.1)
 index._local.read=None
 columns=[d[1] for d in index.read().execute('PRAGMA table_info(strategy_rows)')]
 assert 'company' in columns and 'has_evidence' in columns
 assert not index.rows('ch05-legacy_1.0.1-long-1d',search='STALE'),'old content is gone, not merged'
 assert index.read().execute("select value from index_meta where key='schema_version'").fetchone()[0]==SCHEMA_VERSION
 assert index.state()['stale'] is False and index.stats()

def test_two_processes_sharing_one_index_file_do_not_both_build(tmp_path,specs):
 """The pilot and the UI-QA instance can point at one file. A per-process lock cannot stop them deleting each
 other's rows, so the build takes a lease inside the index itself."""
 store,first=_store_and_index(tmp_path,specs)
 second=type(first)(store,first.path)      # a second "process" on the same file
 assert first._take_lease() is True
 try:
  assert second._take_lease() is False,'the second process must not start a competing build'
  assert second.build() is False and second._last_error is None,'refusing the lease is not a failure'
  assert second.state()['build_lease']
 finally:first._release_lease()
 assert second._take_lease() is True,'the lease is available again once released'
 second._release_lease()

def test_an_abandoned_build_lease_is_stolen(tmp_path,specs):
 from kanida_pilot.research_index import LEASE_SECONDS
 store,index=_store_and_index(tmp_path,specs)
 connection=sqlite3.connect(index.path)
 connection.execute("insert into index_meta values('build_lease',?)",
  (f'deadhost:999@{time.time()-LEASE_SECONDS-60:.0f}',))
 connection.commit();connection.close();index._local.read=None
 assert index._take_lease() is True,'a lease older than LEASE_SECONDS belongs to a dead process'
 index._release_lease()
 assert index.build() is True,index._last_error

def test_build_force_overrides_a_live_lease(tmp_path,specs):
 store,index=_store_and_index(tmp_path,specs)
 assert index._take_lease() is True
 assert index.build() is False,'a live lease blocks an ordinary build'
 assert index.build(force=True) is True,'--force steals it'
 assert index.state()['build_lease'] is None,'a finished build always releases its lease'

def test_cli_builds_without_the_app(tmp_path,specs,monkeypatch,capsys):
 """`python -m kanida_pilot.research_index --build` must rebuild using the server's own settings, so a stuck
 index can be fixed without starting the app."""
 from kanida_pilot import research_index
 directory=tmp_path/'expanded_research';build_research_tree(directory,specs)
 settings=pilot_settings(tmp_path,pattern_research_directory=str(directory),pattern_catalogue_path=str(CATALOGUE),
  pattern_research_run=RESEARCH_RUN)
 pair=research_index.from_settings(settings)
 monkeypatch.setattr(research_index,'from_settings',lambda s=None:pair)
 assert research_index.main(['--build'])==0
 printed=capsys.readouterr().out
 assert '1,048 strategies' in printed and 'evidence for' in printed
 assert research_index.main([])==0,'no arguments prints the state'
 assert '"populated": true' in capsys.readouterr().out

def test_cli_from_settings_builds_the_same_pair_the_server_uses(tmp_path,specs):
 from kanida_pilot.research_index import from_settings,ResearchIndex
 directory=tmp_path/'expanded_research';build_research_tree(directory,specs)
 settings=pilot_settings(tmp_path,pattern_research_directory=str(directory),pattern_catalogue_path=str(CATALOGUE),
  pattern_research_run=RESEARCH_RUN)
 store,index=from_settings(settings)
 assert isinstance(index,ResearchIndex) and store.research_run==RESEARCH_RUN
 assert str(index.path)==settings.pattern_index_path
 assert index.build() is True,index._last_error
 assert index.state()['strategies']==1048


# --- ranking by USABLE evidence -------------------------------------------
def test_rows_rank_by_usable_evidence_not_by_presence_of_a_row(client):
 """Most indexed cells are measured but carry no accepted walk-forward selection. Sorting on presence alone put
 those empty-looking rows at the top of every card; the tier is what decides now."""
 rows=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results').json()['rows']
 tiers=[r['evidence_tier'] for r in rows]
 assert tiers==sorted(tiers),'rows are served already ordered by tier'
 by_symbol={r['symbol']:r for r in rows}
 assert by_symbol['AAAA']['evidence_tier']==0 and by_symbol['AAAA']['label']=='Historical walk-forward result'
 assert by_symbol['BBBB']['evidence_tier']==1 and by_symbol['BBBB']['label']=='Limited historical sample'
 assert by_symbol['CCCC']['evidence_tier']==2 and by_symbol['CCCC']['label']=='No selected walk-forward trades'
 assert by_symbol['EEEE']['evidence_tier']==3
 assert by_symbol['FFFF']['evidence_tier']==4,'a cell the run never reached ranks below known history'
 assert rows[0]['symbol']=='AAAA','the only stock with an out-of-sample result leads'
 assert rows[-1]['symbol']=='FFFF'
 # A row whose three columns would be blank says so, so the UI can print the reason instead of dashes.
 assert by_symbol['AAAA']['has_numbers'] is True and by_symbol['BBBB']['has_numbers'] is True
 assert by_symbol['CCCC']['has_numbers'] is False and by_symbol['FFFF']['has_numbers'] is False
 assert by_symbol['CCCC']['label'] and by_symbol['FFFF']['label'],'every row carries its reason'

def test_top_tier_keeps_the_edge_ordering(research_app,client,tmp_path):
 """Inside the top tier the existing order stands: edge 95% low, then the out-of-sample mean, then n."""
 index=research_app.state.strategies.index
 connection=sqlite3.connect(index.path)
 # Four tier-0 rows that isolate each key in turn:
 #   T1 wins on edge low; T4 and T2 tie on edge and mean, so the larger sample wins; T3 loses on the mean.
 for symbol,edge,mean,n in (('T1',0.9,1.0,30),('T2',0.5,2.0,30),('T3',0.5,1.0,90),('T4',0.5,2.0,90)):
  connection.execute('insert or replace into strategy_rows(strategy_key,symbol,company,evidence_state,has_evidence,'
   'evidence_tier,occurrences,oos_n,oos_expectancy_pct,edge_low_pct) values(?,?,?,?,1,0,10,?,?,?)',
   ('ch05-legacy_1.0.1-long-1d',symbol,symbol,'walkforward_result',n,mean,edge))
 connection.commit();connection.close();index._local.read=None
 rows=[r['symbol'] for r in index.rows('ch05-legacy_1.0.1-long-1d') if r['symbol'].startswith('T')]
 assert rows==['T1','T4','T2','T3'],f'edge low, then out-of-sample mean, then n: {rows}'

def test_strategy_reports_how_many_rows_are_in_each_tier(client):
 body=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results').json();st=body['strategy']
 assert (st['tier_result'],st['tier_limited'],st['tier_history'])==(1,1,3)
 assert st['tier_result']+st['tier_limited']+st['tier_history']==st['researched_stocks']
 assert body['coverage']['tier_result']==1 and body['coverage']['tier_limited']==1
 assert body['coverage']['tier_history']==3
 assert st['evidence_summary']=='1 stock with an out-of-sample result · 1 stock on a limited sample · 3 stocks with history only'

def test_the_evidence_summary_never_implies_a_result_that_does_not_exist():
 from kanida_pilot.strategies import evidence_summary_line as line
 assert line(26,0,469)=='26 stocks with an out-of-sample result · 469 stocks with history only'
 assert line(26,12,469)=='26 stocks with an out-of-sample result · 12 stocks on a limited sample · 469 stocks with history only'
 # The case that matters: every row is `no_walkforward_trades`.
 assert line(0,0,495)=='No out-of-sample result on any stock — 495 stocks with history only'
 assert line(0,12,483)=='No out-of-sample result yet · 12 stocks on a limited sample · 483 stocks with history only'
 assert line(0,0,0)=='No researched history on this timeframe'
 assert line(1,0,0)=='1 stock with an out-of-sample result'
 for text in (line(0,0,495),line(0,12,483)):
  assert 'with an out-of-sample result' not in text,'a strategy with no result must not imply one'

def test_catalog_carries_the_tier_counts_for_the_picker(client):
 blocks={b['key']:b for b in client.get('/api/strategies/catalog').json()['blocks']}
 one=next(x for x in blocks['chart_patterns']['strategies'] if x['key']=='ch05-legacy_1.0.1-long-1d')
 assert one['tier_result']==1 and one['tier_limited']==1 and one['tier_history']==3
 assert one['evidence_summary'] and 'out-of-sample result' in one['evidence_summary']
 assert all(x['evidence_summary'] for x in blocks['chart_patterns']['strategies']),'every strategy explains itself'

def test_evidence_tiers_match_the_contract_labels():
 assert cards.TIER['walkforward_result']==0 and cards.TIER['limited_sample']==1
 assert cards.TIER['no_walkforward_trades']==2
 assert cards.TIER['insufficient_history']==cards.TIER['no_occurrences']==3
 assert cards.TIER['loading']==4
 assert set(cards.TIER)>=set(cards.LABELS),'every contract label has a ranking tier'
 assert cards.tier_for('nonsense')==cards.TIER_UNKNOWN

def test_store_is_read_only_on_the_research_tree(research_app):
 store=research_app.state.strategies.store
 with pytest.raises(sqlite3.OperationalError):
  store.connect('research').execute("insert into runs values('x','complete','{}')")

def test_an_index_from_an_older_schema_is_rebuilt_not_read(tmp_path,specs):
 """The index is a derived cache. An older file must be discarded and rebuilt, never read column-by-column:
 `CREATE TABLE IF NOT EXISTS` would leave it short of new columns and every row read would fail."""
 from kanida_pilot.research_index import ResearchIndex,SCHEMA_VERSION
 from kanida_pilot.research_store import ResearchStore
 directory=tmp_path/'expanded_research';build_research_tree(directory,specs)
 path=tmp_path/'old_index.sqlite3'
 old=sqlite3.connect(path)
 old.executescript('CREATE TABLE index_meta(key TEXT PRIMARY KEY,value TEXT);'
  'CREATE TABLE strategy_stats(strategy_key TEXT PRIMARY KEY,found INTEGER);'
  'CREATE TABLE strategy_rows(strategy_key TEXT,symbol TEXT,PRIMARY KEY(strategy_key,symbol));')
 old.execute("insert into index_meta values('schema_version','1')")
 old.execute("insert into index_meta values('rows','999')")
 old.execute("insert into strategy_rows values('ch05-legacy_1.0.1-long-1d','STALE')")
 old.commit();old.close()
 store=ResearchStore(directory,RESEARCH_RUN,CATALOGUE,tmp_path/'release.json')
 index=ResearchIndex(store,path)
 assert index.state()['stale'] is True,'an older schema is always stale'
 assert index.build() is True,index._last_error
 columns=[d[1] for d in index.read().execute('PRAGMA table_info(strategy_rows)')]
 assert 'company' in columns and 'has_evidence' in columns
 assert not index.rows('ch05-legacy_1.0.1-long-1d',search='STALE'),'the old content is gone, not merged'
 assert index.read().execute("select value from index_meta where key='schema_version'").fetchone()[0]==SCHEMA_VERSION

# --- the research card's footer names BOTH clocks ---------------------------
def test_research_results_never_report_prices_of_unknown_age(client):
 """A research card stands on two clocks: live detections now, and a frozen research run. Reusing the stored
 scan's `stale`/`age_days` made the shared footer say "Research only - prices of unknown age" on live data."""
 body=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results').json()
 assert body['stale'] is False,'a frozen research run is not stale prices'
 assert body['data_end']=='2026-09-15','the research run data end for THIS timeframe, as a real date'
 assert body['evidence_end']==body['data_end']
 assert isinstance(body['age_days'],int) and body['age_days']>=0,'never null, so nothing can print "unknown age"'
 assert body['evidence_age_days']==body['age_days']
 assert 'evidence from research to 15 Sep 2026' in body['provenance']
 assert 'unknown age' not in body['provenance']

def test_provenance_names_whichever_date_is_missing():
 from kanida_pilot.strategies import research_provenance,stamp_text
 assert stamp_text('2026-09-16 15:30:00')=='16 Sep 15:30'
 assert stamp_text('2026-09-16T15:30')=='16 Sep 15:30'
 assert stamp_text('nonsense') is None and stamp_text(None) is None
 live={'available':True,'as_of':'2026-09-16 15:30:00'}
 assert research_provenance(live,'2026-09-15','1D')==(
  'Detections live · 16 Sep 15:30 · evidence from research to 15 Sep 2026')
 assert research_provenance({'available':False},'2026-09-15','1D')==(
  'No live detections yet · evidence from research to 15 Sep 2026')
 # Whatever is missing is NAMED; the line never falls back to "unknown".
 assert research_provenance(live,None,'4H')=='Detections live · 16 Sep 15:30 · evidence date not recorded for 4H'
 assert research_provenance({'available':True},None,'1W')==(
  'Detections live · time not recorded · evidence date not recorded for 1W')
 for case in (research_provenance(None,None,'1D'),research_provenance(live,None,'1D'),
   research_provenance({'available':False},None,'1D')):
  assert 'unknown' not in case.lower(),case

def test_catalog_carries_both_clocks_for_the_card(client):
 body=client.get('/api/strategies/catalog').json()
 assert body['research']['latest_by_timeframe']['1D']=='2026-09-15'
 assert set(body['research']['latest_by_timeframe'])<= {'1D','1H','4H','1W'}
 assert 'live' in body and 'available' in body['live'],'the live as_of travels with the catalog'

def test_index_parses_the_per_timeframe_evidence_dates():
 from kanida_pilot.research_index import _parse_latest
 assert _parse_latest('1D=2026-09-15 1H=2026-09-15 1W=2026-09-07')=={'1D':'2026-09-15','1H':'2026-09-15','1W':'2026-09-07'}
 assert _parse_latest('')=={} and _parse_latest(None)=={}
 assert _parse_latest('1D=not-a-date 9X=2026-09-15')=={},'unreadable or unknown entries are dropped, not guessed'


# --- BACKLOG item 4: the admin add-strategy form ---------------------------
# The form used to build its pattern chips from the scanner's /api/state - the detector set the scanner happens
# to be running - and posted every one of them as a `stored_pattern`. On the researched set that is 107 catalogue
# ids the stored path has never accepted, so every add answered 400. The form is now offered the registry's own
# accept-list, and both sources go in through the one POST /api/admin/strategies.

def test_the_add_form_is_offered_the_registry_accept_list_not_the_scanner_set(client):
 sources=client.get('/api/admin/strategy-sources')
 assert sources.status_code==200,sources.text
 body=sources.json();stored,research=body['stored'],body['research']
 # Both sources are offered whatever detector set the scanner is running, because create accepts both.
 assert stored['available'] is True and len(stored['patterns'])==10
 assert {p['id'] for p in stored['patterns']}>={'falling_wedge','channel','head_shoulders'}
 assert stored['timeframes']==['1D','1H','4H','1W'] and all(p['block_key']=='chart' for p in stored['patterns'])
 assert research['available'] is True and research['run']==RESEARCH_RUN and len(research['patterns'])==107
 combinations=sum(len(v['sides']) for p in research['patterns'] for v in p['variants'])
 assert combinations==262,'every direction-variant combination the catalogue holds is offerable'
 by_id={p['id']:p for p in research['patterns']}
 assert by_id['CH05']['block_key']=='chart_patterns' and by_id['CH05']['name'] and by_id['CH05']['variants']
 assert {p['block_key'] for p in research['patterns']}=={'chart_patterns','candlestick','price_action','harmonics'}

def test_every_offered_combination_is_one_the_registry_would_accept(research_app):
 """The 400 was the form offering ids the server refuses. Whatever the list offers must resolve a catalogue spec."""
 registry=research_app.state.strategies;catalogue=registry.store.catalogue
 offered=registry.admin_sources()['research']['patterns']
 for pattern in offered:
  for variant in pattern['variants']:
   for side in variant['sides']:
    assert catalogue.spec(pattern['id'],variant['id'],side),(pattern['id'],variant['id'],side)

def test_the_form_adds_a_stored_pattern_and_a_research_pattern_through_one_path(client):
 stored=client.post('/api/admin/strategies',json=dict(block_key='chart',source_type='stored_pattern',
  pattern='falling_wedge',timeframe='1D',side='long',name='Owner falling wedge 1D',tags=['Chart patterns','Bullish','1D']))
 assert stored.status_code==200,stored.text
 assert stored.json()['source_type']=='stored_pattern' and stored.json()['pattern']=='falling_wedge'
 assert stored.json()['enabled'] is False,'a new strategy is never visible to members until it is switched on'
 research=client.post('/api/admin/strategies',json=dict(block_key='chart_patterns',source_type='research_pattern',
  pattern_id='CH05',variant='legacy_1.0.1',side='long',timeframe='1D',name='Owner cup and handle strict 1D',min_trades=30))
 assert research.status_code==200,research.text
 made=research.json()
 assert made['source_type']=='research_pattern' and made['pattern_id']=='CH05' and made['variant']=='legacy_1.0.1'
 assert made['timeframe']=='1D' and made['side']=='long' and made['min_trades']==30 and made['enabled'] is False
 assert made['key']=='owner-cup-and-handle-strict-1d' and made['research_run']==RESEARCH_RUN
 registry={s['key']:s for b in client.get('/api/admin/strategies').json()['blocks'] for s in b['strategies']}
 assert registry[made['key']]['block_key']=='chart_patterns' and registry['owner-falling-wedge-1d']['block_key']=='chart'

def test_an_owner_added_research_strategy_resolves_the_same_cell_and_the_same_gate(client,tmp_path):
 made=client.post('/api/admin/strategies',json=dict(block_key='chart_patterns',source_type='research_pattern',
  pattern_id='CH05',variant='legacy_1.0.1',side='long',timeframe='1D',name='Owner cup and handle strict 1D',enabled=True)).json()
 key=made['key'];assert key!='ch05-legacy_1.0.1-long-1d','a second entry gets its own registry key'
 mine=client.get('/api/strategies/'+key+'/results');seeded=client.get('/api/strategies/ch05-legacy_1.0.1-long-1d/results')
 assert mine.status_code==200 and seeded.status_code==200,mine.text
 assert [r['symbol'] for r in mine.json()['rows']]==[r['symbol'] for r in seeded.json()['rows']]
 assert mine.json()['total']==seeded.json()['total'] and mine.json()['total']>0,'it resolves real evidence, not an empty strategy'
 # Adding from the form never bypasses the publication gate: with no decision recorded the card still says so.
 card=client.get('/api/strategies/'+key+'/card?symbol=AAAA').json()
 assert card['review_required'] is True and card['review_label']=='Historical data requires review'
 release=tmp_path/'evidence_release.json'
 release.write_text(json.dumps({'runs':{RESEARCH_RUN:{'publication_status':'withheld_source_quality_review'}}}),encoding='utf-8')
 withheld=client.get('/api/strategies/'+key+'/card?symbol=AAAA').json()
 assert withheld['evidence_state']=='requires_review' and withheld['summary'] is None

def test_the_form_can_preview_and_switch_on_what_it_added(client):
 made=client.post('/api/admin/strategies',json=dict(block_key='chart_patterns',source_type='research_pattern',
  pattern_id='CH05',variant='legacy_1.0.1',side='long',timeframe='1D',name='Owner cup and handle preview 1D')).json()
 preview=client.post('/api/admin/strategies/'+made['key']+'/preview',json={})
 assert preview.status_code==200,preview.text
 assert preview.json()['source']=='research_index' and len(preview.json()['rows'])<=5
 switched=client.patch('/api/admin/strategies/'+made['key'],json=dict(enabled=True))
 assert switched.status_code==200 and switched.json()['enabled'] is True

def test_a_researched_pattern_posted_as_a_stored_one_is_named_and_still_refused(client):
 # The exact body the old form sent for every chip once the scanner moved to the researched set.
 bad=client.post('/api/admin/strategies',json=dict(block_key='chart_patterns',source_type='stored_pattern',
  pattern='CH05',pattern_name='Cup and Handle',timeframe='1D',side='long',name='Old form body'))
 assert bad.status_code==400 and bad.json()['code']=='SOURCE_MISMATCH'
 assert 'research_pattern' in bad.json()['error'],'the refusal names the source that would work'
 for body,code in (
   (dict(block_key='chart_patterns',source_type='research_pattern',pattern_id='CH05',variant='not_a_variant',side='long',timeframe='1D'),'PATTERN_NOT_FOUND'),
   (dict(block_key='chart_patterns',source_type='research_pattern',pattern_id='CH05',variant='legacy_1.0.1',side='long',timeframe='3D'),'FIELD_INVALID'),
   (dict(block_key='chart_patterns',source_type='research_pattern',pattern_id='CH05',variant='legacy_1.0.1',side='sideways',timeframe='1D'),'FIELD_INVALID'),
   (dict(block_key='not_a_block',source_type='research_pattern',pattern_id='CH05',variant='legacy_1.0.1',side='long',timeframe='1D',name='Nowhere at all'),'BLOCK_NOT_FOUND'),
   (dict(block_key='chart_patterns',source_type='research_pattern',pattern_id='CH05',variant='legacy_1.0.1',side='long',timeframe='1D',key='no'),'FIELD_INVALID'),
   (dict(block_key='chart_patterns',source_type='not_a_source',pattern_id='CH05',variant='legacy_1.0.1',side='long',timeframe='1D'),'FIELD_INVALID')):
  result=client.post('/api/admin/strategies',json=body)
  assert result.status_code in (400,404) and result.json()['code']==code,(body,result.status_code,result.text)
 # Re-adding a seeded combination under its catalogue name is still the honest 409, not a silent duplicate.
 again=client.post('/api/admin/strategies',json=dict(block_key='chart_patterns',source_type='research_pattern',
  pattern_id='CH05',variant='legacy_1.0.1',side='long',timeframe='1D'))
 assert again.status_code==409 and again.json()['code']=='STRATEGY_EXISTS'

def test_the_sources_list_is_owner_only(research_app):
 member=signup(research_app,email='member@example.invalid',role='member')
 assert member.get('/api/admin/strategy-sources').status_code==403
 assert TestClient(research_app).get('/api/admin/strategy-sources').status_code in (401,403)
