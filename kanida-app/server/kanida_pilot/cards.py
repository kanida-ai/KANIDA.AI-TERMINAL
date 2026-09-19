"""The trader evidence card: a pure transform of ONE precomputed `cell_outcomes` row (+ its `bucket_outcomes`
rows) into the card the Discover block renders. No research is computed here and nothing is re-run.

Section order is fixed by the owner's spec and mirrored by the client:
  summary -> barriers -> forward_curve -> conditions -> last_occurrences -> honesty

Labels come from EVIDENCE_SERVING_CONTRACT.md §6 verbatim. A card NEVER borrows another cell's numbers: if
this exact identity has no compatible evidence, the numbers are absent and the state says why.
"""
from __future__ import annotations
import json,math
from .research_store import COST_PCT,DISPLAY_GRID,BAR_WORD

# EVIDENCE_SERVING_CONTRACT.md §6 label table - verbatim strings, keyed by evidence state.
LABELS={
 'insufficient_history':'Not enough historical data',
 'no_occurrences':'No occurrences in this historical sample',
 'no_walkforward_trades':'No selected walk-forward trades',
 'limited_sample':'Limited historical sample',
 'walkforward_result':'Historical walk-forward result',
 'incompatible':'Incompatible historical evidence',
 'requires_review':'Historical data requires review',
}
# Not a contract row: the outcome engine has not reached this symbol yet, so nothing is known either way.
LOADING_LABEL='Evidence loading'
# Ranking tier for a row: how USABLE its evidence is, not merely whether an outcome row exists. Most indexed
# cells are `no_walkforward_trades` - the engine measured them but no walk-forward selection was accepted - so
# ordering by presence alone floats rows with nothing to show to the top of every list. Lower tier sorts first.
#   0 an accepted out-of-sample result          3 measured, but there is nothing to report
#   1 an out-of-sample result on a thin sample   4 not computed yet (unknown, so it ranks below known history)
#   2 measured, no accepted selection
TIER={'walkforward_result':0,'limited_sample':1,'no_walkforward_trades':2,'insufficient_history':3,
 'no_occurrences':3,'incompatible':3,'requires_review':3,'loading':4}
TIER_UNKNOWN=4
# Only tier 0 carries an out-of-sample number a trader can act on; tier <=1 has some out-of-sample evidence.
TIER_RESULT,TIER_LIMITED=0,1
def tier_for(state):return TIER.get(state,TIER_UNKNOWN)
def has_numbers(state):
 """True when the row has its own measured numbers to print (as opposed to only a reason)."""
 return state in ('walkforward_result','limited_sample','no_walkforward_trades')
WALKFORWARD_MIN=20  # §6: "fewer than 20 walk-forward trades" -> Limited historical sample
# Research `cells.status` -> evidence state, used when no outcome row exists yet and for cells the engine skipped.
RESEARCH_STATUS_STATE={
 'insufficient_history':'insufficient_history','insufficient_walkforward_history':'insufficient_history',
 'no_occurrences':'no_occurrences','no_walkforward_trades':'no_walkforward_trades',
 'small_walkforward_sample':'limited_sample','tested_positive':'walkforward_result','tested_nonpositive':'walkforward_result',
}
# `cell_outcomes.selection_status` -> evidence state.
SELECTION_STATE={'tested':'walkforward_result','small_out_of_sample_sample':'limited_sample',
 'insufficient':'no_walkforward_trades','insufficient_history':'insufficient_history'}
TIE_RULE_TEXT='If one candle touches both, the stop is counted first: OHLC cannot reveal which came first inside a bar.'
BUCKET_TITLES={'volume':'Volume at the signal','regime':'Market regime','quality_tertile':'Pattern quality'}
BUCKET_NAMES={'high':'High','low':'Low','normal':'Normal','up':'Up','down':'Down','flat':'Flat',
 'q1_low':'Weakest third','q2_mid':'Middle third','q3_high':'Strongest third'}
NOT_ENOUGH_CASES='not enough cases'

def number(value,places=2):
 if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value):return None
 return round(float(value),places)
def loads(value,default=None):
 if isinstance(value,(dict,list)):return value
 try:return json.loads(value) if value else default
 except (TypeError,ValueError):return default

def bars_phrase(low,high,timeframe):
 """"most of the move happens within 5-8 4H candles" - the window in that timeframe's own bars."""
 word=BAR_WORD.get(timeframe,'candles')
 if low is None and high is None:return None
 if low is None or high is None or low==high:return f'most of the move happens within {high or low} {word}'
 return f'most of the move happens within {min(low,high)}–{max(low,high)} {word}'

def evidence_state(research_status,selection_status,oos_n,has_outcome):
 """Resolve the contract state. Sample sufficiency is kept separate from compatibility and freshness (§6)."""
 if not has_outcome:
  # No outcome row. If the research run never studied this cell at all there is no history to speak of; if it did
  # and only the outcome engine has not reached it, that is "loading" - a pending state, not a finding.
  if not research_status:return 'insufficient_history'
  state=RESEARCH_STATUS_STATE.get(research_status)
  if state in ('no_occurrences','insufficient_history'):return state
  return 'loading'
 state=SELECTION_STATE.get(selection_status,'no_walkforward_trades')
 if state=='walkforward_result' and (oos_n or 0)<WALKFORWARD_MIN:return 'limited_sample'
 return state

def label_for(state):return LOADING_LABEL if state=='loading' else LABELS.get(state,LABELS['incompatible'])

def significance_line(q_value,p_value,fdr_trials,state):
 """Never dress an unadjusted p-value as a discovery. If the cell fails BH-FDR, say so plainly."""
 if state not in ('walkforward_result','limited_sample'):return 'No walk-forward result, so there is nothing to test for significance.'
 if q_value is None:
  if p_value is None:return 'Significance has not been computed for this cell.'
  return (f'Unadjusted p-value {number(p_value,3)}. The multiple-testing correction across every pattern tested has not '
   'been applied to this cell yet, so this is not evidence of an edge.')
 trials=f' across {fdr_trials:,} tested cells' if fdr_trials else ''
 if q_value>0.10:
  return ('Not statistically distinguishable from chance after correcting for the number of patterns tested'
   f' (q = {number(q_value,3)}{trials}).')
 return (f'Survives multiple-testing correction at q = {number(q_value,3)}{trials}. That is a historical result, '
  'not a forecast and not an expected return.')

def honesty(row,summary,state,publication,in_sample_only):
 lines=[f'Costs {COST_PCT:.2f}% included, charged once on the entry notional.','Entry is the next open after the signal bar.']
 scope=(summary.get('selection') or {}).get('out_of_sample',{}).get('scope')
 if state in ('walkforward_result','limited_sample'):
  lines.append('Returns shown are out-of-sample: the holding window was chosen on training folds only and measured on later, unseen folds.'
   if scope else 'Returns shown are from the walk-forward test folds.')
 if in_sample_only:lines.append('The in-sample best is never shown as an expected return.')
 if row.get('baseline_status')=='tested':
  lines.append('Every number is compared with entering the same stock on any eligible bar over the same window and cost model.')
 if publication and not publication.get('released'):
  lines.append(f'{LABELS["requires_review"]}: this research release has no recorded source-quality decision, so it is historical evidence only.')
 lines.append('Short studies are hypothetical price studies; borrow, funding and eligibility are not modelled.'
  ) if row.get('side')=='short' else None
 return [l for l in lines if l]

def _horizon(summary,h):
 return next((x for x in (summary.get('horizons') or []) if x.get('h')==h),None)
def _baseline_horizon(summary,h):
 return next((x for x in ((summary.get('baseline') or {}).get('horizons') or []) if x.get('h')==h),None)

def build_summary(row,summary,timeframe):
 """Occurrences, win rate WITH its baseline beside it, median net / MFE / MAE, and the window in own bars."""
 h=row.get('selected_horizon') or row.get('return_peak_h')
 window=_horizon(summary,h) or {};base=_baseline_horizon(summary,h) or {}
 tested=row.get('selection_status')=='tested'
 # The headline win rate is the out-of-sample one when a walk-forward selection exists; otherwise the
 # whole-history descriptive rate, which is labelled as such by the caller's evidence state.
 win=number(row.get('oos_win_rate_pct') if tested else window.get('win_rate_pct'),1)
 base_win=number(base.get('baseline_win_rate_pct'),1)
 return dict(
  occurrences=row.get('occurrences'),
  sample_label=row.get('sample_label'),
  horizon=h,horizon_bars_word=BAR_WORD.get(timeframe,'candles'),
  win_rate_pct=win,baseline_win_rate_pct=base_win,
  win_rate_text=None if win is None else (f'{win:.0f}% vs {base_win:.0f}% for the stock alone' if base_win is not None else f'{win:.0f}%'),
  win_rate_scope='out_of_sample' if tested else 'all_history_descriptive',
  median_net_return_pct=number(window.get('median_net_return_pct')),
  mean_net_return_pct=number(row.get('oos_expectancy_pct') if tested else window.get('mean_net_return_pct')),
  baseline_mean_net_return_pct=number(row.get('baseline_mean_net_return_pct')),
  median_mfe_pct=number(window.get('median_mfe_pct')),median_mae_pct=number(window.get('median_mae_pct')),
  walkforward_n=row.get('oos_n'),
  flatten_low=row.get('mfe_flatten_h_90pct'),flatten_high=row.get('mfe_flatten_h_95pct'),
  window_text=bars_phrase(row.get('mfe_flatten_h_90pct'),row.get('mfe_flatten_h_95pct'),timeframe),
 )

def build_barriers(row,summary):
 """First touch of the declared display barrier, with the tie rule stated in words."""
 barriers=summary.get('barriers') or {};display=row.get('display_barrier_id') or barriers.get('display_barrier_id')
 found=next((b for b in (barriers.get('rows') or []) if b.get('id')==display),None) or {}
 target=found.get('target');stop=found.get('stop');unit='%' if (found.get('unit') or 'pct')=='pct' else 'R'
 target_text=f'+{number(target,2)}{unit}' if target is not None else None
 stop_text=f'−{number(stop,2)}{unit}' if stop is not None else None
 p_target=number(row.get('p_target_first'),1);p_stop=number(row.get('p_stop_first'),1);p_neither=number(row.get('p_neither'),1)
 headline=None
 if None not in (p_target,p_stop,p_neither) and target_text and stop_text:
  headline=(f'hit {target_text} before {stop_text}: {p_target:.0f}% · hit {stop_text} first: {p_stop:.0f}% '
   f'· neither: {p_neither:.0f}%')
 return dict(barrier_id=display,target_pct=number(target,2),stop_pct=number(stop,2),n=row.get('barrier_n'),
  p_target_first=p_target,p_stop_first=p_stop,p_neither=p_neither,headline=headline,
  tie_rule=barriers.get('tie_rule'),tie_rule_text=TIE_RULE_TEXT,
  median_bars_to_target=number(row.get('median_bars_to_target'),1),median_bars_to_stop=number(row.get('median_bars_to_stop'),1),
  both_touched_same_bar_n=found.get('both_touched_same_bar_n'),
  undetermined_n=found.get('n_neither'),sample_label=found.get('sample_label'))

def build_curve(row,summary,timeframe):
 """Forward-return curve on the declared display grid for this timeframe, with the peak marked."""
 grid=loads(row.get('display_grid'),{}) or summary.get('display_grid') or {}
 shown=list(grid.get('shown') or DISPLAY_GRID.get(timeframe,()))
 points=[]
 for h in shown:
  window=_horizon(summary,h)
  if not window:continue
  base=_baseline_horizon(summary,h) or {}
  points.append(dict(h=h,n=window.get('n'),median_net_return_pct=number(window.get('median_net_return_pct')),
   mean_net_return_pct=number(window.get('mean_net_return_pct')),win_rate_pct=number(window.get('win_rate_pct'),1),
   baseline_mean_net_return_pct=number(base.get('baseline_mean_net_return_pct')),
   median_mfe_pct=number(window.get('median_mfe_pct')),median_mae_pct=number(window.get('median_mae_pct'))))
 values=[p for p in points if p['median_net_return_pct'] is not None]
 peak=max(values,key=lambda p:p['median_net_return_pct'])['h'] if values else None
 return dict(grid=shown,declared=list(grid.get('declared') or DISPLAY_GRID.get(timeframe,())),
  dropped_beyond_horizon=list(grid.get('dropped_beyond_horizon') or []),max_horizon=row.get('max_horizon'),
  points=points,peak_h=peak,selected_h=row.get('selected_horizon'),bars_word=BAR_WORD.get(timeframe,'candles'),
  basis='median net return after costs')

def build_conditions(buckets,summary):
 """Volume / regime / quality buckets, each against its OWN baseline. Thin buckets say so and show no rate."""
 minimum=int(((summary.get('buckets') or {}).get('minimum_sample')) or 30)
 definitions=(summary.get('buckets') or {}).get('definitions') or {}
 groups={}
 for b in buckets:
  dimension=b.get('dimension');n=b.get('n') or 0;thin=n<minimum or b.get('status')!='tested'
  groups.setdefault(dimension,dict(dimension=dimension,title=BUCKET_TITLES.get(dimension,dimension),
   definition=definitions.get('quality' if dimension=='quality_tertile' else dimension),buckets=[]))
  groups[dimension]['buckets'].append(dict(
   bucket=b.get('bucket'),name=BUCKET_NAMES.get(b.get('bucket'),b.get('bucket')),n=n,
   enough=not thin,note=NOT_ENOUGH_CASES if thin else None,
   win_rate_pct=None if thin else number(b.get('win_rate_pct'),1),
   mean_net_return_pct=None if thin else number(b.get('mean_net_return_pct')),
   median_net_return_pct=None if thin else number(b.get('median_net_return_pct')),
   baseline_mean_net_return_pct=None if thin else number(b.get('baseline_mean_net_return_pct')),
   baseline_n=b.get('baseline_n'),baseline_scope=b.get('baseline_scope'),
   diff_mean_net_return_pct=None if thin else number(b.get('diff_mean_net_return_pct')),
   q_value=None if thin else number(b.get('q_value'),3)))
 order={'volume':0,'regime':1,'quality_tertile':2}
 return dict(minimum_sample=minimum,horizon=(summary.get('buckets') or {}).get('horizon'),
  scope='Whole history, non-overlapping. Each bucket is compared with unconditional entries in the same bucket, '
   'so a bucket cannot look good only because the market rose in it.',
  groups=sorted(groups.values(),key=lambda g:order.get(g['dimension'],9)))

def build_last(row,summary):
 """The last 5 similar occurrences, then a one-line read of them."""
 stored=loads(row.get('last_occurrences'),[]) or []
 meta=summary.get('last_occurrences') or {}
 rows=[dict(date=o.get('entry_date'),entry_time=o.get('entry_time'),entry_price=number(o.get('entry_price')),
   next_move_pct=number(o.get('net_return_pct')),best_move_pct=number(o.get('mfe_pct')),worst_move_pct=number(o.get('mae_pct')),
   bars_held=o.get('bars_held'),outcome=o.get('outcome'),horizon=o.get('horizon')) for o in stored][:5]
 positive=[r for r in rows if r['next_move_pct'] is not None and r['next_move_pct']>0]
 read=None
 if rows:
  known=[r for r in rows if r['next_move_pct'] is not None]
  if known:read=f'{len(positive)} of the last {len(known)} {"was" if len(known)==1 else "were"} positive after costs.'
 return dict(rows=rows,read=read,barrier_id=meta.get('barrier_id'),tie_rule=meta.get('tie_rule'),note=meta.get('note'))

def build_card(identity,research_cell,outcome_row,buckets,publication,compatible=True,loading_reason=None):
 """Assemble the card. `outcome_row` may be None (engine has not reached this symbol) - that is a state, not a blank."""
 timeframe=identity.get('timeframe')
 research_status=(research_cell or {}).get('status')
 has_outcome=bool(outcome_row)
 summary=loads((outcome_row or {}).get('summary'),{}) or {}
 state=('incompatible' if not compatible else
  evidence_state(research_status,(outcome_row or {}).get('selection_status'),(outcome_row or {}).get('oos_n'),has_outcome))
 withheld=bool(publication) and publication.get('publication_status')=='withheld_source_quality_review'
 if withheld:state='requires_review'
 card=dict(identity=identity,evidence_state=state,label=label_for(state),
  research_run=identity.get('research_run'),outcome_run=(outcome_row or {}).get('run'),
  snapshot_id=(outcome_row or {}).get('snapshot_id'),engine_version=summary.get('outcome_engine_version'),
  publication=publication,review_required=bool(publication and not publication.get('released')),
  review_label=LABELS['requires_review'] if publication and not publication.get('released') else None,
  occurrences=(outcome_row or research_cell or {}).get('occurrences'),
  sections=['summary','barriers','forward_curve','conditions','last_occurrences','honesty'],
  summary=None,barriers=None,forward_curve=None,conditions=None,last_occurrences=None,honesty=[],
  significance=None,note=None)
 if state=='loading':
  card['note']=loading_reason or 'Evidence for this stock is still being computed by the current research run.'
  return card
 if state in ('incompatible','requires_review','no_occurrences','insufficient_history') or not has_outcome:
  card['note']={
   'incompatible':'The live detector, data snapshot or cost model does not match this evidence, so its numbers are not shown.',
   'requires_review':'This research release has no recorded source-quality decision, so its numbers are withheld.',
   'no_occurrences':'This pattern never occurred on this stock and timeframe in the historical sample.',
   'insufficient_history':'This stock and timeframe do not have enough history to measure this pattern.',
  }.get(state,'No compatible evidence exists for this exact strategy, stock and timeframe.')
  card['honesty']=honesty(outcome_row or {},summary,state,publication,False)
  return card
 in_sample_only=outcome_row.get('selection_status')!='tested'
 card.update(
  summary=build_summary(outcome_row,summary,timeframe),
  barriers=build_barriers(outcome_row,summary),
  forward_curve=build_curve(outcome_row,summary,timeframe),
  conditions=build_conditions(buckets or [],summary),
  last_occurrences=build_last(outcome_row,summary),
  significance=dict(q_value=number(outcome_row.get('oos_q_value'),4),p_value=number(outcome_row.get('oos_p_value'),4),
   fdr_trials=outcome_row.get('fdr_trials'),discovery_q10=outcome_row.get('discovery_q10'),
   line=significance_line(outcome_row.get('oos_q_value'),outcome_row.get('oos_p_value'),outcome_row.get('fdr_trials'),state)),
  honesty=honesty(outcome_row,summary,state,publication,in_sample_only))
 if in_sample_only:
  card['note']=('The holding window has no accepted walk-forward selection for this cell, so the numbers below describe '
   'the whole history and are not an expected return.')
 return card
