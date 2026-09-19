// Trader evidence card: pure formatting + the research-row adapter. No React; exercised by scripts/check-card-logic.cjs.
// Every string here is presentation only. The card NEVER derives a statistic the server did not send: a missing
// number renders as an em dash, not as zero and not as a value borrowed from another horizon or another cell.
import type {CardBucket,CardCurve,CardLast,CardOccurrence,CardSummary,CardBarriers,EvidenceCard,EvidenceState,
 ResearchRow,ResearchSummary,StrategyRow} from '../strategies/types';
import {EVIDENCE_LABELS} from '../strategies/types';

export const CARD_COST_LINE='Costs 0.40% included · next-open entry';
export const EVIDENCE_LOADING_NOTE='Evidence for this stock is still being computed.';
/** Sections in the owner's fixed order; the card renders exactly this sequence. */
export const CARD_SECTIONS=['summary','barriers','forward_curve','conditions','last_occurrences','honesty'] as const;

const finite=(v:unknown):v is number=>typeof v==='number'&&Number.isFinite(v);
/** Signed percent with one decimal: 1.4 -> "+1.4%". Missing stays missing. */
export function signed(v:number|null|undefined,places=1){return finite(v)?`${v>0?'+':v<0?'−':''}${Math.abs(v).toFixed(places)}%`:'—';}
/** Unsigned percent, for rates and excursions. */
export function plain(v:number|null|undefined,places=1){return finite(v)?`${v.toFixed(places)}%`:'—';}
export function rate(v:number|null|undefined){return finite(v)?`${Math.round(v)}%`:'—';}
export function count(v:number|null|undefined){return finite(v)?v.toLocaleString('en-IN'):'—';}
export function bars(v:number|null|undefined,word:string){return finite(v)?`${Number.isInteger(v)?v:v.toFixed(1)} ${word}`:'—';}

export type EvidenceTone='good'|'weak'|'neutral'|'pending';
/** Tone is about the STATE, never about whether the number is flattering. */
export function evidenceTone(state:EvidenceState):EvidenceTone{
 if(state==='walkforward_result')return 'good';
 if(state==='limited_sample'||state==='no_walkforward_trades')return 'weak';
 if(state==='loading')return 'pending';
 return 'neutral';
}
export function evidenceLabel(state:EvidenceState){return EVIDENCE_LABELS[state]||EVIDENCE_LABELS.incompatible;}
/** True when the card has this cell's own measured numbers to show. */
export function hasNumbers(card:EvidenceCard|null|undefined){return !!card&&!!card.summary;}

/** Summary line 1: occurrences + sample label. */
export function occurrenceText(s:CardSummary|null|undefined){
 if(!s||!finite(s.occurrences))return 'Occurrences not recorded';
 return `${count(s.occurrences)} occurrence${s.occurrences===1?'':'s'}`;
}
/** Win rate beside its baseline, exactly as the server composed it ("61% vs 58% for the stock alone"). */
export function winRateText(s:CardSummary|null|undefined){return s?.win_rate_text||'—';}
export function winRateScopeNote(s:CardSummary|null|undefined){
 return s?.win_rate_scope==='out_of_sample'?'out-of-sample':'whole history, descriptive';
}
/** "most of the move happens within 5-8 4H candles". Absent when the engine did not measure a flattening point. */
export function windowText(s:CardSummary|null|undefined){return s?.window_text||null;}

/** Barrier headline, with the tie rule always stated beside it. */
export function barrierText(b:CardBarriers|null|undefined){return b?.headline||null;}
export function barrierTimingText(b:CardBarriers|null|undefined,word:string){
 if(!b)return null;
 const target=finite(b.median_bars_to_target)?`target in ${bars(b.median_bars_to_target,word)}`:null;
 const stop=finite(b.median_bars_to_stop)?`failure in ${bars(b.median_bars_to_stop,word)}`:null;
 const parts=[target,stop].filter(Boolean);
 return parts.length?`Typical time to ${parts.join(' · ')}`:null;
}

export type CurveBar={h:number;value:number|null;height:number;peak:boolean;selected:boolean;label:string;a11y:string};
/** Forward-return curve as drawable bars on a shared zero baseline. Points with no median are kept in place
 * (so the x-axis stays the declared grid) but draw nothing. */
export function curveBars(curve:CardCurve|null|undefined):CurveBar[]{
 if(!curve||!curve.points?.length)return [];
 const values=curve.points.map(p=>p.median_net_return_pct).filter(finite) as number[];
 const span=Math.max(...values.map(Math.abs),0.001);
 return curve.points.map(p=>{
  const v=finite(p.median_net_return_pct)?p.median_net_return_pct:null;
  return {h:p.h,value:v,height:v==null?0:Math.abs(v)/span,peak:curve.peak_h===p.h,selected:curve.selected_h===p.h,
   label:String(p.h),
   a11y:`${p.h} ${curve.bars_word}: median ${signed(v)} after costs${finite(p.baseline_mean_net_return_pct)?`, stock alone ${signed(p.baseline_mean_net_return_pct)}`:''}${curve.peak_h===p.h?', the peak':''}`};
 });
}
export function curveCaption(curve:CardCurve|null|undefined){
 if(!curve)return '';
 const peak=finite(curve.peak_h)?`peaks at ${curve.peak_h} ${curve.bars_word}`:'no peak measured';
 const dropped=curve.dropped_beyond_horizon?.length?` · ${curve.dropped_beyond_horizon.join(', ')} beyond the measured horizon`:'';
 return `Median net return after costs · ${peak}${dropped}`;
}

/** A bucket line. A bucket below the minimum sample says "not enough cases" and shows no rate at all. */
export function bucketText(b:CardBucket){
 if(!b.enough)return b.note||'not enough cases';
 const base=finite(b.baseline_mean_net_return_pct)?` vs ${signed(b.baseline_mean_net_return_pct)} for the stock alone`:'';
 return `${rate(b.win_rate_pct)} win · avg ${signed(b.mean_net_return_pct)}${base}`;
}
export function bucketA11y(b:CardBucket,group:string){
 return `${group}, ${b.name}, ${count(b.n)} cases: ${b.enough?bucketText(b):'not enough cases to report a rate'}`;
}

export const OUTCOME_WORDS:Record<string,string>={target_hit:'Target first',stop_hit:'Stop first',
 horizon:'Held to the window',gap_exit:'Gapped out',neither:'Neither'};
export function occurrenceOutcome(o:CardOccurrence){return OUTCOME_WORDS[String(o.outcome)]||o.outcome||'—';}
/** "4 of the last 5 were positive" - composed by the server so the count can never drift from the rows. */
export function lastRead(last:CardLast|null|undefined){return last?.read||null;}

/** Every honesty line the card must show, in order, with the significance sentence last. */
export function honestyLines(card:EvidenceCard|null|undefined){
 if(!card)return [];
 const lines=[...(card.honesty||[])];
 if(card.significance?.line)lines.push(card.significance.line);
 return lines;
}

/** Research rows reuse the scanner card. The column MEANINGS differ from the stored scan, so the headers differ
 * too (see RESEARCH_COLUMNS) - the numbers are never relabelled as something they are not. */
export const RESEARCH_COLUMNS={low:'Edge low',avg:'Avg',n:'Trades'};
/** The list's plain words for each evidence state. The contract's own labels (EVIDENCE_LABELS) stay on the
 * evidence card; the list and its spoken labels never read out a research term or a raw code value. */
export const PLAIN_EVIDENCE:Record<string,string>={
 walkforward_result:'tested on later data',limited_sample:'few past cases',no_walkforward_trades:'no tested trades',
 insufficient_history:'not enough history',no_occurrences:'never happened before',incompatible:'no matching past data',
 requires_review:'past data being checked',loading:'past results loading'};
export function plainEvidence(state:string|null|undefined){return PLAIN_EVIDENCE[String(state||'')]||'no matching past data';}
export const STORED_COLUMNS={low:'95% low',avg:'Avg',n:'n'};
export function researchRowToStrategyRow(r:ResearchRow,st:ResearchSummary):StrategyRow{
 return {
  symbol:r.symbol,company:r.company||r.symbol,sector:r.sector,
  match_id:`${r.symbol}:${st.timeframe}:${st.pattern_id}:${st.variant}:${st.side}`,
  timeframe:st.timeframe,side:st.side,direction:st.side==='long'?'bullish':'bearish',state:st.state,
  price:null,candle_end:r.last_seen,
  low_pct:r.edge_low_pct,high_pct:r.edge_high_pct,avg_pct:r.walkforward_mean_pct,
  win_rate:r.win_rate,n:r.walkforward_n??0,sample_label:r.label,
  status:r.evidence_state==='walkforward_result'?'tested':'no_validated_rule',
  test:null,
  evidence_basis:r.evidence_state==='walkforward_result'?'tested_rule':r.evidence_state==='loading'?'none':'hold_period_history',
  // Carried through so the scanner row can show its own state without re-joining the source list.
  evidence_state:r.evidence_state,has_evidence:r.has_evidence,
 } as StrategyRow;
}
/** Row accessibility text for a research row: the edge, its sample and its contract label. */
export function researchRowA11y(r:ResearchRow){
 return `${r.symbol}, ${plainEvidence(r.evidence_state)}, edge low ${signed(r.edge_low_pct)}, average ${signed(r.walkforward_mean_pct)} after costs, ${count(r.walkforward_n)} tested trades`;
}
/** Picker tag for a researched strategy. Never says "found". With live scanning on, the NOW count leads - the
 * same number the card's "Now" tab shows - so the owner can pick strategies with setups right now. */
export function researchPickerTag(st:ResearchSummary){
 if(st.evidence_pending)return 'past results loading';
 const best=finite(st.best_edge_low_pct)?`best edge low ${signed(st.best_edge_low_pct)}`:'no ranked edge yet';
 if(!st.live_detection){
  const n=finite(st.researched_stocks)?`${st.researched_stocks} stocks`:'history unknown';
  return `${n} · ${best}`;
 }
 const now=finite(st.detections_live)?`${st.detections_live} now`:'count unknown';
 return `${now} · ${best}`;
}
// --- status line for a researched strategy ----------------------------------------------------------------
// The stored scan says "N found" because a scanner really did match N stocks today. A researched pattern has NO
// live detector yet (IMPLEMENTED_CATALOGUE.md: "not activated in the live scanner"), so the size of its history
// list is NOT a detection count and must never be printed as "found". Three separate numbers, each named:
//   detections_today / detections_week  - how recently the pattern last occurred in the researched history
//   researched         - how many stocks are in the history list
export type ResearchStatusInput={phase:'idle'|'loading'|'done'|'error'|'stopped';date:string;researched:number;
 shown:number;revealing:boolean;live_detection:boolean;detections_today:number|null;detections_week:number|null;
 detections_live?:number|null;pending?:boolean};
export const NO_LIVE_DETECTION='Not scanning live yet — showing past cases';
export const NO_LIVE_DETECTION_SHORT='Not scanning live yet';
export const EVIDENCE_INDEXING='Past results are still loading — the list fills in when they are ready.';
/** The History tab's status line. The tab already carries the one count, so a finished list says what it is
 * instead of repeating a number; only a list still revealing says how far it has got. */
export function researchStatusText(i:ResearchStatusInput,short=false){
 const stocks=`${i.researched.toLocaleString('en-IN')} stock${i.researched===1?'':'s'}`;
 if(i.phase==='idle')return 'Choose a strategy to list stocks.';
 if(i.phase==='loading')return 'Loading history…';
 if(i.phase==='stopped')return short?'Stopped before loading.':'Stopped before the history loaded.';
 if(i.phase==='error')return short?'History unavailable.':'History is unavailable right now.';
 if(i.pending)return EVIDENCE_INDEXING;
 if(i.revealing)return short?`Loading… ${i.shown} of ${stocks}`:`Loading history… ${i.shown} of ${stocks}`;
 if(!i.researched)return short?'No history on this timeframe.':'This pattern has no history on this timeframe.';
 if(!i.live_detection)return short?NO_LIVE_DETECTION_SHORT:`${NO_LIVE_DETECTION}.`;
 return short?'Past cases by stock':'Stocks where this pattern happened before, and what followed.';
}
/** "Evidence ready for 200 of 495 stocks" - shown only while the outcome run is still working. A finished run
 * adds no line (and no second count) to the card. */
export function evidenceCoverageText(coverage:{ready:number;total:number}|null|undefined){
 if(!coverage||!coverage.total)return null;
 if(coverage.ready>=coverage.total)return null;
 return `Evidence ready for ${count(coverage.ready)} of ${count(coverage.total)} stocks.`;
}
/** A row the outcome run has not reached shows this chip instead of three dashes. */
export const ROW_PENDING='evidence loading';
export function rowPending(r:{evidence_state?:string;has_evidence?:boolean}|null|undefined){
 return !!r&&(r.evidence_state==='loading'||r.has_evidence===false);
}

/** Index coverage line for the block header: honest about a research run still in progress. */
export function coverageText(state:{symbols_with_evidence:number;symbols_research:number;current_outcome_status:string|null}|null|undefined){
 if(!state||!state.symbols_research)return null;
 const done=state.symbols_with_evidence,total=state.symbols_research;
 if(done>=total)return `Evidence for all ${count(total)} stocks.`;
 return `Evidence for ${count(done)} of ${count(total)} stocks${state.current_outcome_status==='running'?' — the rest are still being computed':''}.`;
}

// --- live detections (docs/LIVE_DETECTION.md) -------------------------------------------------------------
// A research card has THREE distinct things on it and this file never lets one stand in for another:
//   "detected today"     - the scanner's detection ledger, right now, with a lifecycle state per detection;
//   "researched history" - how often and how recently the pattern occurred in the frozen research run;
//   "evidence"           - what the outcome engine measured for that cell, with the contract's own label.
// Every string below names which of the three it is describing.
import type {DetectionEvidence,DetectionScope,DetectionState,LiveDetection,LiveState} from '../strategies/types';

/** The list a research scanner card is showing. `live` is only offered when the ledger is actually readable. */
export type ListMode='live'|'history';
export const LIST_MODES:ListMode[]=['live','history'];
export const LIST_MODE_LABELS:Record<ListMode,string>={live:'Now',history:'History'};
/** Live detection is available only when the scanner is running the research pattern set. */
export function liveAvailable(live:LiveState|null|undefined){return !!live&&live.available===true;}
/** Default list for a research card: today's detections when live detection can serve them, else history. */
export function defaultListMode(live:LiveState|null|undefined):ListMode{return liveAvailable(live)?'live':'history';}
/** A stored preference only wins while it is still servable; `live` falls back to `history` when the ledger is off. */
export function resolveListMode(stored:unknown,live:LiveState|null|undefined):ListMode{
 if(stored==='history')return 'history';
 if(stored==='live'&&liveAvailable(live))return 'live';
 return defaultListMode(live);
}
/** Why live detection is off, in the operator's own words. Never phrased as "nothing was detected". */
export const LIVE_OFF='Live scanning for these patterns is off, so there are no current setups to show.';
export function liveOffText(live:LiveState|null|undefined){
 if(liveAvailable(live))return null;
 return live?.error?`Current setups are unavailable: ${live.error}`:live?.reason||LIVE_OFF;
}

/** Lifecycle tone. About the STATE, never about whether the detection is flattering. */
export type DetectionTone='confirmed'|'forming'|'done';
export function detectionTone(state:DetectionState|string|null|undefined):DetectionTone{
 return state==='confirmed'?'confirmed':state==='forming'?'forming':'done';
}
export const DETECTION_STATE_LABELS:Record<DetectionState,string>={
 forming:'Forming',confirmed:'Confirmed',invalidated:'Invalidated',expired:'Expired'};
export function detectionStateLabel(d:{state?:string;state_label?:string|null}|null|undefined){
 if(!d)return '—';
 return d.state_label||DETECTION_STATE_LABELS[d.state as DetectionState]||d.state||'—';
}
/** Month names are fixed here rather than taken from the runtime locale: ICU renders September as both "Sep"
 * and "Sept" depending on the engine, and a detection row's date must read identically everywhere. */
export const MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
/** Time the signal bar closed. Today shows the clock ("15:15"); an older bar keeps its day ("12 Sep 15:15"). */
export function detectionTimeText(detected_at:string|null|undefined,today?:string|null){
 const text=String(detected_at||'');
 if(text.length<10)return '—';
 const day=text.slice(0,10),clock=text.length>=16?text.slice(11,16):'';
 if(today&&day===today)return clock||day;
 const parts=/^(\d{4})-(\d{2})-(\d{2})$/.exec(day),month=parts?MONTHS[Number(parts[2])-1]:null;
 const label=parts&&month?`${Number(parts[3])} ${month}`:day;
 return clock?`${label} ${clock}`:label;
}
/** "since 16 Sep" - the day the setup was first found (its signal candle). Always the day, never a clock, so a
 * row reads the same at 09:45 and at 15:30 and never disagrees with the top bar's price time. */
export function sinceText(d:{detected_day?:string|null;detected_at?:string|null}|null|undefined){
 const day=String(d?.detected_day||d?.detected_at||'').slice(0,10);
 const parts=/^(\d{4})-(\d{2})-(\d{2})$/.exec(day),month=parts?MONTHS[Number(parts[2])-1]:null;
 return parts&&month?`since ${Number(parts[3])} ${month}`:'';
}
/** The newest session the scanner holds for a timeframe, read from the live state it already serves. */
export function latestSession(live:LiveState|null|undefined,timeframe:string|null|undefined){
 const frame=timeframe?(live?.timeframes as any)?.[timeframe]:null;
 const session=frame?.session;
 return typeof session==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(session)?session:null;
}
/** "new" = first found on the latest session for its timeframe (not the calendar day, so it does not go blank
 * overnight or at a weekend). A server `new` flag, when present, is used as served. */
export function isNewDetection(d:{new?:boolean|null;detected_day?:string|null;detected_at?:string|null}|null|undefined,session?:string|null){
 if(!d)return false;
 if(typeof d.new==='boolean')return d.new;
 const day=String(d.detected_day||d.detected_at||'').slice(0,10);
 return !!session&&day===session;
}
/** "Forming · since 16 Sep · new · 2 candles ago". Composed here so the row and its spoken label cannot drift;
 * the row shows the first two (and the "new" marker), the spoken label carries all of it. */
export function detectionDetailText(d:LiveDetection,session?:string|null){
 const parts=[detectionStateLabel(d)];
 const since=sinceText(d);if(since)parts.push(since);
 if(isNewDetection(d as any,session))parts.push('new');
 const age=detectionAgeText(d);if(age)parts.push(age);
 if(!d.current)parts.push('not in the latest scan');
 return parts.join(' · ');
}
/** "3 candles ago" - completed candles since the detector's signal bar, as of the pass that last saw it. */
export function detectionAgeText(d:{bars_since_signal?:number|null}|null|undefined){
 const n=d?.bars_since_signal;
 if(!finite(n))return null;
 // Short enough to stay whole on a 4-across card; the detection time beside it carries the date.
 return n===0?'latest candle':`${n} candle${n===1?'':'s'} ago`;
}
export function detectionRowA11y(d:LiveDetection,session?:string|null){
 const drawn=d.drawable?'':', no chart outline for this pattern';
 const past=d.evidence_compatible===false?', no matching past data':'';
 return `${d.symbol}, ${d.company}. ${detectionDetailText(d,session)}${drawn}${past}`;
}
/** Shown instead of an empty chart when the detector published nothing drawable (pattern_lines.build). */
export const MARKER_ONLY='marker only — this detector publishes no drawable geometry';
export function geometryNoteText(d:{drawable?:boolean;geometry_note?:string|null}|null|undefined){
 if(!d||d.drawable!==false)return null;
 return d.geometry_note||MARKER_ONLY;
}

// --- evidence identity ------------------------------------------------------------------------------------
// LIVE_DETECTION.md §6 / EVIDENCE_SERVING_CONTRACT.md §6: a live detection may show a research card only when
// pattern id, variant, side, timeframe AND detector spec hash match the research run. Anything else carries
// the contract's own label and NO numbers - a card never borrows another cell's.
export const NO_COMPATIBLE_EVIDENCE=EVIDENCE_LABELS.incompatible;
export function evidenceCompatible(e:DetectionEvidence|null|undefined){return e?.status==='identity_match';}
export function detectionEvidenceLabel(e:DetectionEvidence|null|undefined){
 return evidenceCompatible(e)?null:NO_COMPATIBLE_EVIDENCE;
}
export function detectionEvidenceNote(e:DetectionEvidence|null|undefined){
 if(!e||evidenceCompatible(e))return null;
 return e.note||NO_COMPATIBLE_EVIDENCE;
}

// --- the live list's status line ---------------------------------------------------------------------------
// The Now tab carries the card's ONE count, so this line never repeats it. A finished, complete list has no
// status text at all; a list the server cut says how much is shown; an empty list says so in plain words.
// It prints no clock either: the page's single data line owns "when", so no two dates on the page disagree.
export type LiveStatusInput={phase:'idle'|'loading'|'done'|'error'|'stopped';
 total:number;shown:number;available:boolean;reason?:string|null;lastDetected?:string|null};
export const NO_ACTIVE='No setup right now';
export function liveStatusText(i:LiveStatusInput,short=false){
 if(!i.available)return short?NO_LIVE_DETECTION_SHORT:(i.reason||LIVE_OFF);
 if(i.phase==='idle')return 'Choose a strategy to see setups.';
 if(i.phase==='loading')return 'Loading setups…';
 if(i.phase==='stopped')return short?'Stopped before loading.':'Stopped before the setups loaded.';
 if(i.phase==='error')return short?'Setups unavailable.':'Current setups are unavailable right now.';
 if(!i.total){
  // Never a bare blank: say there is nothing standing, and the day this strategy last had one.
  const seen=i.lastDetected?sinceText({detected_at:i.lastDetected}).replace(/^since /,''):'';
  return short||!seen?NO_ACTIVE:`${NO_ACTIVE} · last seen ${seen}.`;
 }
 return i.shown<i.total?`Showing ${count(i.shown)} of ${count(i.total)}`:'';
}
/** The tab label carries its own count, so the two lists can never be confused for one another. */
export function listModeLabel(mode:ListMode,live:number|null|undefined,researched:number|null|undefined){
 const n=mode==='live'?live:researched;
 return finite(n)?`${LIST_MODE_LABELS[mode]} (${count(n)})`:LIST_MODE_LABELS[mode];
}
