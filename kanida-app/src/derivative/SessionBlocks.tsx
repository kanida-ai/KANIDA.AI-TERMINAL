// Four blocks of the Derivative tab, all drawn through the ONE template every block on this tab is drawn
// through (frame.Block): a title, ONE headline figure with the comparison that gives it meaning, ONE as-of
// line, ONE symbol pill, ONE "How to read this", and under them the two panels of the skeleton -
// [ Chart ][ Content ] - at the template's widths and the template's height. Not one of them sets its own.
//
//   * PCR through the session      - pcr_oi and pcr_volume, each with its own direction chip.
//   * Max pain through the session - the max-pain strike against spot on ONE scale, so the distance is visible.
//                                    Distance is STRIKE MINUS SPOT, the store's own convention, and the wording
//                                    names its subject so the sign cannot be read backwards.
//   * IV through the session       - the at-the-money line, and a 2 x 5 grid of the SAME ten strikes the ΔOI
//                                    block draws, in the same order and at the same tile size. COMPUTED, said so
//                                    on every tile and everywhere else it appears, AND with what it rests on: the
//                                    risk-free rate is a code constant with no feed behind it, so the rate, that
//                                    fact, and the weight of it are on screen rather than in a footnote.
//   * Futures build-up             - open interest against its own average, and the basis, each with its chip.
//                                    These were THREE panels; they are two now, because both charts are the same
//                                    session of the same contract and share the template's one chart panel.
//
// Every one of them is pointed at the tab's ONE symbol, handed down; not one resolves its own. Every direction
// word is the SERVER's (`direction_words` / `direction_labels`), so this file hardcodes none of them. Every
// series sits on the store's reading grid: a reading with no row is a gap slot, drawn as a hole and never as a
// zero, never carried forward, and never presented as a value in a readings panel (section 5).
import React,{useMemo} from 'react';
import {View,ScrollView,Pressable} from 'react-native';
import {C,T,s} from '../ui';
import {useDerivativeRead,type Read} from './useDerivatives';
import {Block,DirectionChip,InfoDisclosure,Tag,head,metaText,stateOf,tone,typeColor,
 type HeadlineProps,type InfoGroup,type PaneStyle} from './frame';
import {SessionPanel,SessionPairPanel,ReadingsPanel,type ReadingRow,type SessionSeries} from './SessionPanel';
// The IV block's content panel is the 2 x 5 grid — the SAME ten contracts the ΔOI block draws, in the same
// order, at the same tile size, so the two blocks can be read tile for tile. It owns its own file; this one
// keeps only the block shell, its headline, its rate caveat and its one disclosure.
import {IV_GRID_COMPUTED_TEXT,IV_GRID_GAP_TEXT,IV_GRID_HIGHLIGHT_TEXT,IV_GRID_TEXT,
 IvStrikeGrid} from './IvGridSection';
import {BASIS_NO_SPOT,BASIS_NO_SPOT_ROW,DASH,FUTURES_BUILDUP_DEFINITION,FUTURES_BUILDUP_NO_POINTS,FUTURES_BUILDUP_READING_TEXT,
 IV_ATM_LABEL,IV_COMPUTED_TAG,IV_DEFINITION,IV_NO_POINTS,IV_READING_TEXT,
 MAX_PAIN_DEFINITION,MAX_PAIN_GAP_TEXT,MAX_PAIN_NO_POINTS,MAX_PAIN_READING_TEXT,
 NO_SYMBOL_TEXT,PCR_DEFINITION,PCR_NO_POINTS,PCR_OI_LABEL,PCR_READING_TEXT,PCR_VOLUME_LABEL,
 asOfText,basisPair,basisText,compact,deltaRatio,deltaReadingsText,deltaReasonText,deltaSince,deltaStrikePoints,
 directionRule,directionWords,dteText,expiryText,futuresChartTitle,futuresLine,previousCloseAtText,
 figureAtText,headlineAgainst,ivComputedText,ivLatestRefusal,ivMethodText,ivRateIsAssumed,ivRateSensitivity,
 ivRateSensitivityText,ivRateSourceShort,ivRateSourceText,ivRateText,ivRejectionLines,ivText,maxPainAgainstSpot,
 maxPainDistanceRule,maxPainDistanceText,maxPainLine,oiVsAvgText,pcrAgainst,pcrLine,pcrText,price,priceTick,ratio,
 readingsText,seriesTimes,seriesValues,servedBuildup,servedChip,servedTone,signed,strike as strikeText,
 withheldText,withheldLines} from './logic';
import type {BlockSymbol} from './logic';
import type {ChartTarget,FuturesBuildup,IvLeg,IvLegPoint,IvSeries,MaxPainDeltas,MaxPainSeries,PcrDeltas,
 OiGrid,PcrSeries} from './types';

/** Every session block takes the same props, and NONE of them is a width, a flex or a breakpoint: the template
 *  hands each panel the style it is to wear (frame.Block). `linked` says the symbol is one the reader picked in
 *  the pinned screener rather than a default, which is what lights the pill and the rail. */
export type SessionBlockProps={symbol:string;choice:BlockSymbol;expiry:string;seq:number;
 stacked?:boolean;badge:string;linked?:boolean;
 /** Set ONLY while a widget is expanded to fill the page. Every other block is the template's one height. */
 height?:number;
 target?:ChartTarget|null;onTarget?:(t:ChartTarget)=>void;
 hidden?:Record<string,boolean>;onHide?:(key:string)=>void;expanded?:string;onExpand?:(key:string)=>void};
/** When a panel of a block is showing ONE SENTENCE, the whole block collapses rather than holding a chart-sized
 *  void. Two cases qualify and only two: the tab has no symbol yet, and the endpoint is unavailable - both of
 *  which are a sentence and a control, nothing more.
 *
 *  A panel that is empty because the READING is empty is a different thing and keeps its full height: that is a
 *  captured answer, and points will be there at the next reading. Nothing is hidden either way - the sentence and
 *  its Retry are on screen at IDLE_H exactly as they were at full height. */
export function oneSentence(phase:string){return phase==='error';}
export function shrink(symbol:string,phase:string){return !symbol||oneSentence(phase);}
/** One subtitle shape for all four, so the blocks read as one family: symbol, expiry, days to expiry. */
export function blockSubtitle(symbol:string,expiry?:string|null,dte?:number|null){
 const when=expiry?expiryText(expiry,dte??null):dte==null?'':dteText(dte);
 return symbol+(when&&when!==DASH?' · '+when:'');
}
/** The two lines every block's disclosure ends with: how much of the session carried a value and why the rest
 *  did not, in the server's own sentences. A block whose session is whole says nothing here. */
function gapGroup(body:any):InfoGroup|null{
 const lines=withheldLines(body);
 const counted=readingsText(body);
 if(!lines.length&&!counted)return null;
 return {heading:'What is missing, and why',
  lines:[counted?counted+'.':'',String(body?.gaps_text||'').trim(),...lines]};
}
/** The headline when the tab has no symbol at all: a dash and the one sentence that says what to do about it. */
const noSymbolHeadline=(label:string):HeadlineProps=>({label,value:DASH,reason:NO_SYMBOL_TEXT});

// =================================================================================================================
// 1. PCR through the session
// =================================================================================================================
export function PcrSection({symbol,expiry,seq,stacked,height,badge,linked,hidden,onHide,expanded,
 onExpand}:SessionBlockProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 const path=symbol?`/api/derivatives/pcr-series?underlying=${encodeURIComponent(symbol)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<PcrSeries&PcrDeltas>(path,seq);
 const body=read.data;
 const state=symbol?stateOf(read,PCR_NO_POINTS):{phase:'empty' as const,text:NO_SYMBOL_TEXT};
 const points=body?.points||[];
 const times=useMemo(()=>seriesTimes(points),[points]);
 const oi=useMemo(()=>seriesValues(points,p=>p.pcr_oi),[points]);
 const volume=useMemo(()=>seriesValues(points,p=>p.pcr_volume),[points]);
 // both chips are the SERVER's own words, arrows apart
 const words=directionWords(body?.direction_words);
 const chip=servedChip(body?.direction,words,body?.direction_labels);
 const chipTone=servedTone(body?.direction,words);
 const volChip=servedChip(body?.volume_direction,words,body?.direction_labels);
 const volTone=servedTone(body?.volume_direction,words);
 const caveat=withheldText(body);
 const idle=shrink(symbol,read.phase);
 const lastPcr=[...points].reverse().find(p=>p&&!p.gap);
 const series:SessionSeries[]=[
  {key:'pcr_oi',label:PCR_OI_LABEL,color:C.mint,values:oi,format:pcrText},
  {key:'pcr_volume',label:PCR_VOLUME_LABEL,color:C.amber,second:true,values:volume,format:pcrText},
 ];
 // EVERY FIGURE FROM THE READING THAT CARRIES IT, and every figure saying which reading that was when it is
 // not the block's own. Two numbers captured an hour apart are two readings, and this panel says so.
 const at=(key:string)=>figureAtText((body as any)?.[`latest_${key}_at`],body?.as_of);
 // The Δ is the owner's ask: the word said "flat" and the number it was hiding was 0.03. So the change sits
 // UNDER the level it belongs to, in the ratio's own unit, with the reading it was measured from named - and
 // with the server's reason in its place when there is no previous close to measure from at all.
 const deltaOi=deltaRatio(body?.latest_delta_pcr_oi);
 const deltaVolume=deltaRatio(body?.latest_delta_pcr_volume);
 const noBaseline=deltaReasonText(body,'no_baseline');
 const rows:ReadingRow[]=[
  {label:PCR_OI_LABEL,value:pcrText(body?.latest_pcr_oi),chip,chipTone,at:at('pcr_oi')},
  {label:'Change since previous close (OI)',value:deltaOi,at:at('delta_pcr_oi'),reason:noBaseline},
  {label:PCR_VOLUME_LABEL,value:pcrText(body?.latest_pcr_volume),chip:volChip,chipTone:volTone,
   at:at('pcr_volume')},
  {label:'Change since previous close (volume)',value:deltaVolume,at:at('delta_pcr_volume'),
   reason:noBaseline},
  {label:'Previous close (OI)',value:pcrText(body?.previous_close_pcr_oi),
   at:previousCloseAtText(body?.previous_close_at),reason:noBaseline},
  // the totals are per READING, not per session, so they come off the reading that carried each of them
  {label:'Total call open interest',value:compact(body?.latest_total_ce_oi??lastPcr?.total_ce_oi),
   at:at('total_ce_oi')},
  {label:'Total put open interest',value:compact(body?.latest_total_pe_oi??lastPcr?.total_pe_oi),
   at:at('total_pe_oi')},
 ];
 // THE FIRST THING TO LOOK AT. Not a bare ratio: the two open-interest totals it is the division of stand
 // beside it, so the figure carries its own meaning instead of needing a definition read first.
 const headline:HeadlineProps=symbol
  ?{label:PCR_OI_LABEL,value:pcrText(body?.latest_pcr_oi),
    // the LEVEL and HOW FAR IT HAS MOVED, together. A bare ratio has no comparison in it.
    against:headlineAgainst(deltaSince(deltaOi,body,'no_baseline'),
     pcrAgainst(body?.latest_total_pe_oi??lastPcr?.total_pe_oi,
      body?.latest_total_ce_oi??lastPcr?.total_ce_oi)),chip,chipTone,
    reason:caveat||PCR_NO_POINTS}
  :noSymbolHeadline(PCR_OI_LABEL);
 const chart=(style:PaneStyle)=>!shows('pcr_chart')?null:<SessionPanel key="pcr_chart"
  name="PCR through the session" subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body}
  state={state} times={times} series={series} shared axisFormat={v=>pcrText(v)} chip={chip} chipTone={chipTone}
  caveat={caveat} note={pcrLine(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('pcr_chart'):undefined} expanded={expanded==='pcr_chart'}
  onClose={onHide?()=>onHide('pcr_chart'):undefined}
  emptyDetail="Each line needs two 15-min readings that carried a value before it is a line rather than a dot."
  style={style}/>;
 const content=(style:PaneStyle)=>!shows('pcr_readings')?null:<ReadingsPanel key="pcr_readings"
  name="Latest PCR reading" subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body}
  state={state} rows={rows} caveat={caveat} note={readingsText(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('pcr_readings'):undefined} expanded={expanded==='pcr_readings'}
  onClose={onHide?()=>onHide('pcr_readings'):undefined}
  emptyDetail="Nothing has been captured for this symbol at this 15-min reading."
  style={style}/>;
 return <Block title="PCR through the session"
  subtitle="Put open interest against call open interest, and the same division over volume, at every 15-min reading."
  asOf={asOfText(body?.as_of)} badge={badge} linked={linked} headline={headline}
  info={<InfoDisclosure title="PCR through the session" groups={[
   {heading:'PCR through the session',lines:[body?.definition||PCR_DEFINITION,PCR_READING_TEXT,
    directionRule(body),body?.chain_floors_text||'']},
   {heading:'Change since the previous close',lines:[String(body?.delta_text||''),
    String(body?.delta_unit_text||''),deltaReadingsText(body),
    String(body?.previous_close_reason||'')]},
   gapGroup(body)||{heading:'',lines:[]},
  ]}/>}
  stacked={stacked} height={height} idle={idle} chart={chart} content={content}/>;
}

// =================================================================================================================
// 2. Max pain through the session
// =================================================================================================================
export function MaxPainSection({symbol,expiry,seq,stacked,height,badge,linked,hidden,onHide,expanded,
 onExpand}:SessionBlockProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 // the route is `maxpain-series`, one word: the tab reads what the server serves, not what reads better
 const path=symbol?`/api/derivatives/maxpain-series?underlying=${encodeURIComponent(symbol)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<MaxPainSeries&MaxPainDeltas>(path,seq);
 const body=read.data;
 const state=symbol?stateOf(read,MAX_PAIN_NO_POINTS):{phase:'empty' as const,text:NO_SYMBOL_TEXT};
 const points=body?.points||[];
 const times=useMemo(()=>seriesTimes(points),[points]);
 const pain=useMemo(()=>seriesValues(points,p=>p.max_pain_strike),[points]);
 const spot=useMemo(()=>seriesValues(points,p=>p.spot),[points]);
 const words=directionWords(body?.direction_words);
 const chip=servedChip(body?.direction,words,body?.direction_labels);
 const chipTone=servedTone(body?.direction,words);
 const caveat=withheldText(body);
 const idle=shrink(symbol,read.phase);
 // ONE scale, and the only place on this tab where two lines share one: a max-pain strike and spot are both
 // rupee levels of the same underlying, and the whole point of the panel is the distance between them.
 const series:SessionSeries[]=[
  {key:'max_pain',label:'Max pain',color:C.mint,values:pain,format:v=>strikeText(v)},
  {key:'spot',label:'Spot',color:C.amber,second:true,values:spot,format:v=>price(v)},
 ];
 // EVERY FIGURE FROM THE READING THAT CARRIES IT. The strike and the spot are routinely from two different
 // readings on a rebuilt session - a candle carries no spot - and the panel names both rather than printing
 // them side by side as though they were one reading.
 const at=(key:string)=>figureAtText((body as any)?.[`latest_${key}_at`],body?.as_of);
 // BOTH TIMES, OR NEITHER, in the ONE line that sets the strike against the spot. `at()` suppresses a stamp
 // that equals the BLOCK's as-of, which is right everywhere else and wrong here: a 15:45 strike IS the
 // block's as-of, so the line printed a time on the 11:30 spot and none on the strike - and a 15:45 strike
 // beside an 11:30 spot with only one of them stamped reads as one reading. Here the stamp is suppressed
 // only when the two figures came from the SAME reading.
 const samePair=body?.latest_max_pain_strike_at===body?.latest_spot_at;
 const pairAt=(key:string)=>figureAtText((body as any)?.[`latest_${key}_at`],samePair?body?.as_of:null);
 // Max pain is a STRIKE, so its change is in POINTS and moves in whole strike steps - never a percentage.
 // It sits directly under the strike, because "23,350" on its own is the bare number the owner objected to.
 const deltaPain=deltaStrikePoints(body?.latest_delta_max_pain_strike);
 const noBaseline=deltaReasonText(body,'no_baseline');
 const rows:ReadingRow[]=[
  {label:'Max pain strike',value:strikeText(body?.latest_max_pain_strike),chip,chipTone,
   at:at('max_pain_strike')},
  {label:'Change since previous close',value:deltaPain,at:at('delta_max_pain_strike'),reason:noBaseline},
  {label:'Previous close',value:strikeText(body?.previous_close_max_pain_strike),
   at:previousCloseAtText(body?.previous_close_at),reason:noBaseline},
  {label:'Spot',value:price(body?.latest_spot),at:at('spot'),
   reason:'No spot was captured for this underlying at any reading of this session.'},
  // the label names the arithmetic, so the sign on the line under it is unambiguous
  // THE SERVER'S OWN REASON, never a sentence written here. The hard-coded one said no reading of the session
  // carried both a strike and a spot - which is simply false whenever the newest STRIKE has no spot beside it
  // but an earlier reading carried the pair: on 18 Sep 2026 the 11:30 reading carried both. The server already
  // tells the three cases apart (no strike at all, no spot at all, no spot at THIS strike's reading) and says
  // which one this is.
  {label:'Distance (strike − spot)',value:maxPainDistanceText(body?.latest_distance),
   at:at('distance'),reason:body?.latest_distance_reason||undefined},
  {label:'Open interest behind it',value:compact(body?.latest_total_oi),at:at('total_oi'),
   reason:'Not captured at any reading of this session.'},
 ];
 // A strike on its own is a number nobody can read. It carries the captured spot and the gap to it, in the
 // store's own arithmetic, with the direction named - never a level, never a target.
 const headline:HeadlineProps=symbol
  ?{label:'Max pain strike',value:strikeText(body?.latest_max_pain_strike),
    // the strike, how far it has moved since yesterday's close, and the spot it sits against
    against:headlineAgainst(deltaSince(deltaPain,body,'no_baseline'),
     maxPainAgainstSpot(body?.latest_max_pain_strike,body?.latest_spot,body?.latest_distance,pairAt('spot'),
      pairAt('max_pain_strike'))),
    chip,chipTone,reason:caveat||MAX_PAIN_NO_POINTS}
  :noSymbolHeadline('Max pain strike');
 const chart=(style:PaneStyle)=>!shows('max_pain_chart')?null:<SessionPanel key="max_pain_chart"
  name="Max pain against spot" subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body}
  state={state} times={times} series={series} shared axisFormat={v=>priceTick(v)} chip={chip} chipTone={chipTone}
  caveat={caveat} note={maxPainLine(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('max_pain_chart'):undefined} expanded={expanded==='max_pain_chart'}
  onClose={onHide?()=>onHide('max_pain_chart'):undefined}
  emptyDetail="Each line needs two 15-min readings that carried a value before it is a line rather than a dot."
  style={style}/>;
 const content=(style:PaneStyle)=>!shows('max_pain_readings')?null:<ReadingsPanel key="max_pain_readings"
  name="Latest max pain" subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body}
  state={state} rows={rows} caveat={caveat} note={maxPainDistanceRule(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('max_pain_readings'):undefined} expanded={expanded==='max_pain_readings'}
  onClose={onHide?()=>onHide('max_pain_readings'):undefined}
  emptyDetail="Nothing has been captured for this symbol at this 15-min reading."
  style={style}/>;
 return <Block title="Max pain through the session"
  subtitle="The max-pain strike and the captured spot on one scale, so the distance between them is the distance on screen."
  asOf={asOfText(body?.as_of)} badge={badge} linked={linked} headline={headline}
  info={<InfoDisclosure title="Max pain through the session" groups={[
   {heading:'Max pain through the session',lines:[body?.definition||MAX_PAIN_DEFINITION,MAX_PAIN_GAP_TEXT,
    maxPainDistanceRule(body),MAX_PAIN_READING_TEXT,directionRule(body),body?.chain_floors_text||'']},
   {heading:'Change since the previous close',lines:[String(body?.delta_text||''),
    String(body?.delta_unit_text||''),deltaReadingsText(body),
    String(body?.previous_close_reason||'')]},
   gapGroup(body)||{heading:'',lines:[]},
  ]}/>}
  stacked={stacked} height={height} idle={idle} chart={chart} content={content}/>;
}

// =================================================================================================================
// 3. IV through the session - THE ONE COMPUTED NUMBER ON THIS TAB
//
// Everything else here is something the exchange said. This is a model's output, and the reader must never be able
// to mistake the two. So the COMPUTED label rides on the block's headline figure, the chart panel, the legend, the
// legs list, every solved row and what a screen reader hears.
//
// But a label is not enough. A reader also has to be able to find out WHAT the number rests on, and the weakest of
// those inputs is the risk-free rate: a constant in the server's code, 6.5%, with no feed behind it. So the rate,
// that fact, and what a percentage point of it is worth to the answer are all ON SCREEN - the amber line under the
// chart, because an input nothing is feeding is a genuine caveat on the data - with the server's full sentence and
// every other modelling assumption behind the block's one control.
// =================================================================================================================
export function IvSection({symbol,expiry,seq,stacked,height,badge,linked,target,onTarget,hidden,onHide,expanded,
 onExpand,highlight,onHighlight,gridRead}:SessionBlockProps&{highlight?:number|null;
 onHighlight?:(strike:number|null)=>void;
 /** The TAB's one ΔOI-grid read, handed down so the IV grid's ladder is the very same ten contracts the ΔOI
  *  grid and the signal table are built on - fetched once for all three. */
 gridRead:Read<OiGrid>}){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 const path=symbol?`/api/derivatives/iv-series?underlying=${encodeURIComponent(symbol)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<IvSeries>(path,seq);
 const body=read.data;
 const state=symbol?stateOf(read,IV_NO_POINTS):{phase:'empty' as const,text:NO_SYMBOL_TEXT};
 const points=body?.points||[];
 const times=useMemo(()=>seriesTimes(points),[points]);
 // iv_pct is served, so nothing here has to guess whether a number is a fraction or a percentage
 const iv=useMemo(()=>seriesValues(points,p=>p.iv_pct??p.iv),[points]);
 const words=directionWords(body?.direction_words);
 const chip=servedChip(body?.direction,words,body?.direction_labels);
 const chipTone=servedTone(body?.direction,words);
 const idle=shrink(symbol,read.phase);
 const legs=[body?.atm?.ce,body?.atm?.pe];
 const shift=ivRateSensitivity(legs);
 // THE CAVEAT ON THIS BLOCK. Not the fact that it is computed - the label says that - but that its rate is an
 // assumption nothing is feeding, and what that assumption is worth. Amber, because it is a caveat on the data.
 const rateCaveat=ivRateIsAssumed(body)
  ?(ivRateSourceShort(body)+' '+ivRateSensitivityText(shift)).trim():'';
 const refusals=ivRejectionLines(body);
 const latestRefusal=ivLatestRefusal(points,body?.reason_text);
 const series:SessionSeries[]=[
  {key:'iv',label:IV_ATM_LABEL,color:C.mint,values:iv,format:ivText,tag:IV_COMPUTED_TAG},
 ];
 const at=(key:string)=>figureAtText((body as any)?.[`latest_${key}_at`],body?.as_of);
 const rows:ReadingRow[]=[
  {label:`${IV_ATM_LABEL} (computed)`,value:ivText(body?.latest_iv_pct??body?.latest_iv),
   at:at(body?.latest_iv_pct!=null?'iv_pct':'iv'),
   tag:(body?.latest_iv_pct??body?.latest_iv)==null?undefined:IV_COMPUTED_TAG,chip,chipTone,
   reason:latestRefusal||'The server returned no volatility and no reason for this reading.'},
  {label:'At-the-money strike',value:strikeText(body?.atm_strike)},
  {label:'Risk-free rate used',value:ivRateText(body)||DASH,
   reason:'The server did not name the rate these were solved at.'},
 ];
 // The one computed figure on the tab, headlined WITH its label and with what it was solved at and for.
 const solvedAt=headlineAgainst(body?.atm_strike==null?'':'at the '+strikeText(body?.atm_strike)+' strike',
  ivRateText(body)?'solved at '+ivRateText(body):'');
 const headline:HeadlineProps=symbol
  ?{label:IV_ATM_LABEL,value:ivText(body?.latest_iv_pct??body?.latest_iv),tag:IV_COMPUTED_TAG,
    against:solvedAt,chip,chipTone,
    reason:latestRefusal||IV_NO_POINTS}
  :noSymbolHeadline(IV_ATM_LABEL);
 const chart=(style:PaneStyle)=>!shows('iv_chart')?null:<SessionPanel key="iv_chart"
  name="ATM implied volatility" subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body}
  state={state} times={times} series={series} axisFormat={v=>ivText(v)} chip={chip} chipTone={chipTone}
  tag={IV_COMPUTED_TAG} tagA11y="Computed by a pricing model, not reported by the exchange"
  caveat={rateCaveat} note={body?(readingsText(body)+'. '+ivMethodText(body)).trim():undefined}
  onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('iv_chart'):undefined} expanded={expanded==='iv_chart'}
  onClose={onHide?()=>onHide('iv_chart'):undefined}
  emptyDetail="Each line needs two solved 15-min readings before it is a line rather than a dot."
  style={style}/>;
 const content=(style:PaneStyle)=>!shows('iv_strikes')?null:<ReadingsPanel key="iv_strikes"
  name="Volatility by strike" subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body}
  state={state} rows={rows} caveat={rateCaveat}
  note={refusals.length?'Refused readings — '+refusals.join(' '):undefined}
  onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('iv_strikes'):undefined} expanded={expanded==='iv_strikes'}
  onClose={onHide?()=>onHide('iv_strikes'):undefined}
  emptyDetail="Nothing has been solved for this symbol at this 15-min reading."
  style={style}>
  <IvStrikeGrid underlying={symbol} ladder={gridRead} expiry={body?.expiry||expiry} seq={seq} highlight={highlight}
   onHighlight={onHighlight}
   onPick={onTarget?(leg,last)=>onTarget({underlying:body?.underlying||symbol,
    instrumentToken:leg.instrument_token,
    label:leg.tradingsymbol||strikeText(leg.strike)+' '+String(leg.option_type||'').toUpperCase(),
    detail:strikeText(leg.strike)+' '+String(leg.option_type||'').toUpperCase()+' · implied volatility '+ivText(last?.iv_pct??last?.iv)+' '+IV_COMPUTED_TAG}):undefined}
   selected={leg=>!!leg&&leg.instrument_token!=null&&target?.instrumentToken===leg.instrument_token}/>
 </ReadingsPanel>;
 return <Block title="IV through the session"
  subtitle="Implied volatility is COMPUTED by a model from captured prices — every other number on this tab is one the exchange reported."
  asOf={asOfText(body?.as_of)} badge={badge} linked={linked} headline={headline}
  actions={<Tag label={IV_COMPUTED_TAG} a11y="Every number in this block is computed by a pricing model, not reported by the exchange"/>}
  info={<InfoDisclosure title="IV through the session" groups={[
   {heading:'Computed, not reported',lines:[ivComputedText(body),ivMethodText(body),body?.atm_rule||'',
    body?.atm?.basis_rule||'']},
   {heading:'What the rate rests on',lines:[
    // the weakest input in the whole computation, in the server's own words, marked as the caveat it is
    ivRateIsAssumed(body)?{text:ivRateSourceText(body),tone:'amber' as const}:ivRateSourceText(body),
    ivRateSensitivityText(shift)]},
   {heading:'Every other assumption',lines:Object.values(body?.assumptions||{}).filter(t=>t!==body?.assumptions?.rate)},
   {heading:'IV through the session',lines:[IV_DEFINITION,IV_READING_TEXT,directionRule(body)]},
   {heading:'The ten strikes at the money',lines:[IV_GRID_TEXT,IV_GRID_COMPUTED_TEXT,IV_GRID_GAP_TEXT,
    IV_GRID_HIGHLIGHT_TEXT]},
   {heading:'When there is no volatility',lines:refusals.length?refusals
    :['Every reading in this session was solved.']},
  ]}/>}
  stacked={stacked} height={height} idle={idle} chart={chart} content={content}/>;
}

// =================================================================================================================
// 4. Futures build-up
//
// THREE panels became two. Open interest and the basis are two readings of the SAME contract over the SAME
// session on the SAME reading grid, so they share the template's one chart panel - stacked, each keeping its own
// legend, its own scale, its own direction chip and its own line under it. The readings take the content panel,
// exactly as they do in the three blocks above. Nothing was merged and nothing was dropped: two charts that used
// to sit beside each other now sit above each other in one box.
// =================================================================================================================
export function FuturesBuildupSection({symbol,seq,stacked,height,badge,linked,hidden,onHide,expanded,
 onExpand}:SessionBlockProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 const path=symbol?`/api/derivatives/futures-buildup?underlying=${encodeURIComponent(symbol)}`:null;
 const read=useDerivativeRead<FuturesBuildup>(path,seq);
 const body=read.data;
 const state=symbol?stateOf(read,FUTURES_BUILDUP_NO_POINTS):{phase:'empty' as const,text:NO_SYMBOL_TEXT};
 const points=body?.points||[];
 const times=useMemo(()=>seriesTimes(points),[points]);
 const oi=useMemo(()=>seriesValues(points,p=>p.oi),[points]);
 const share=useMemo(()=>seriesValues(points,p=>p.oi_vs_avg),[points]);
 const basis=useMemo(()=>seriesValues(points,p=>p.basis),[points]);
 const basisPct=useMemo(()=>seriesValues(points,p=>p.basis_pct),[points]);
 // TWO vocabularies here, named for the two readings: open interest builds or unwinds, the basis widens or
 // narrows. Handing one chip the other's words would cost it its arrow and its colour without a word changing.
 const oiWords=directionWords(body?.direction_words,'oi');
 const basisWords=directionWords(body?.direction_words,'basis');
 const oiChip=servedChip(body?.oi_direction,oiWords,body?.direction_labels);
 const oiTone=servedTone(body?.oi_direction,oiWords);
 const basisChip=servedChip(body?.basis_direction,basisWords,body?.direction_labels);
 const basisTone=servedTone(body?.basis_direction,basisWords);
 const caveat=withheldText(body);
 const idle=shrink(symbol,read.phase);
 const contract=futuresChartTitle(body?.contract,symbol);
 // OI and its own 20-day share are different units, so they get their own scales - the rule everywhere here.
 const oiSeries:SessionSeries[]=[
  {key:'oi',label:'Open interest',color:C.mint,values:oi,format:compact},
  {key:'share',label:'Against its 20-day average',color:C.amber,second:true,values:share,format:v=>ratio(v)},
 ];
 const basisSeries:SessionSeries[]=[
  {key:'basis',label:'Basis',color:C.mint,values:basis,format:basisText},
  {key:'basis_pct',label:'Basis %',color:C.amber,second:true,values:basisPct,format:v=>signed(v,2)},
 ];
 const last=[...points].reverse().find(p=>p&&!p.gap);
 const at=(key:string)=>figureAtText((body as any)?.[`latest_${key}_at`],body?.as_of);
 const rows:ReadingRow[]=[
  {label:'Open interest',value:compact(body?.latest_oi??last?.oi),chip:oiChip,chipTone:oiTone,at:at('oi')},
  {label:'Against its own 20-day average',value:oiVsAvgText(body?.latest_oi_vs_avg),at:at('oi_vs_avg')},
  {label:'Basis (futures less spot)',value:basisPair(body?.latest_basis,body?.latest_basis_pct),
   chip:basisChip,chipTone:basisTone,at:at('basis'),reason:BASIS_NO_SPOT_ROW},
  {label:'Change since the previous close',
   value:signed(body?.latest_oi_change_pct_day??last?.oi_change_pct_day),
   at:at('oi_change_pct_day'),
   tone:(body?.latest_oi_change_pct_day??last?.oi_change_pct_day)==null?undefined
    :tone(body?.latest_oi_change_pct_day??last?.oi_change_pct_day)},
  // section 3.1's two windows, never merged into one label
  {label:'Build-up on the day',value:servedBuildup(body?.latest_buildup_day),at:at('buildup_day')},
  {label:'Build-up over 15 minutes',value:servedBuildup(body?.latest_buildup_15m),at:at('buildup_15m')},
 ];
 const basisLine=basisPair(body?.latest_basis,body?.latest_basis_pct)===DASH
  ?'Basis is the futures price less spot at the same 15-min reading. At the latest one, '+BASIS_NO_SPOT+'.'
  :'Basis is the futures price less spot at the same 15-min reading: '+basisPair(body?.latest_basis,body?.latest_basis_pct)+' at the latest one.';
 // Open interest is the figure this block is about; what it is read against is its own 20-day average.
 const headline:HeadlineProps=symbol
  ?{label:'Futures open interest',value:compact(body?.latest_oi??last?.oi),
    against:headlineAgainst(oiVsAvgText(body?.latest_oi_vs_avg),
     'basis '+basisPair(body?.latest_basis,body?.latest_basis_pct)),
    chip:oiChip,chipTone:oiTone,reason:caveat||FUTURES_BUILDUP_NO_POINTS}
  :noSymbolHeadline('Futures open interest');
 const chart=(style:PaneStyle)=>!shows('fut_oi')?null:<SessionPairPanel key="fut_oi"
  name="Futures open interest and basis" subtitle={contract} body={body} state={state} times={times}
  top={{name:'Futures open interest',series:oiSeries,axisFormat:compact,chip:oiChip,chipTone:oiTone,
   note:futuresLine(body)}}
  bottom={{name:'Basis through the session',series:basisSeries,axisFormat:priceTick,chip:basisChip,
   chipTone:basisTone,note:basisLine}}
  caveat={caveat} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('fut_oi'):undefined} expanded={expanded==='fut_oi'}
  onClose={onHide?()=>onHide('fut_oi'):undefined}
  emptyDetail="Each line needs two 15-min readings that carried a value before it is a line rather than a dot."
  style={style}/>;
 const content=(style:PaneStyle)=>!shows('fut_readings')?null:<ReadingsPanel key="fut_readings"
  name="Latest futures reading" subtitle={contract} body={body} state={state} rows={rows} caveat={caveat}
  note={readingsText(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('fut_readings'):undefined} expanded={expanded==='fut_readings'}
  onClose={onHide?()=>onHide('fut_readings'):undefined}
  emptyDetail="Nothing has been captured for this contract at this 15-min reading."
  style={style}/>;
 return <Block title="Futures build-up"
  subtitle="The front futures contract of this symbol: open interest against its own average, and the basis against spot."
  asOf={asOfText(body?.as_of)} badge={badge} linked={linked} headline={headline}
  info={<InfoDisclosure title="Futures build-up" groups={[
   {heading:'Futures build-up',lines:[body?.definition||FUTURES_BUILDUP_DEFINITION,FUTURES_BUILDUP_READING_TEXT,
    directionRule(body),
    'Open interest and its share of the 20-day average are different units, so each line keeps its own scale.',
    'Open interest and the basis are two readings of the same contract over the same session, so they share one panel and keep a scale each.']},
   gapGroup(body)||{heading:'',lines:[]},
  ]}/>}
  stacked={stacked} height={height} idle={idle} chart={chart} content={content}/>;
}

/** The direction chip is re-exported for the blocks that draw one outside a panel. */
export {DirectionChip};
