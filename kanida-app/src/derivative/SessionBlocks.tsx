// Four blocks of the Derivative tab, all built on the same rhythm the ΔOI block set: a Section title in large
// text, ONE as-of line and ONE "How to read this" in its header, the symbol badge beside them, and panels of a
// fixed height under it that collapse to IDLE_H when there is one sentence to show rather than a chart.
//
//   * PCR through the session      — pcr_oi and pcr_volume, each with its own direction chip.
//   * Max pain through the session — the max-pain strike against spot on ONE scale, so the distance is visible.
//                                    Distance is STRIKE MINUS SPOT, the store's own convention, and the wording
//                                    names its subject so the sign cannot be read backwards.
//   * IV through the session       — the at-the-money reading and its two legs. COMPUTED, said so everywhere it
//                                    appears, AND with what it rests on: the risk-free rate is a code constant
//                                    with no feed behind it, so the rate, that fact, and the weight of it are on
//                                    screen rather than in a footnote.
//   * Futures build-up             — open interest against its own average, and the basis, each with its chip.
//
// Every one of them is pointed at the tab's ONE symbol, handed down; not one resolves its own. Every direction
// word is the SERVER's (`direction_words` / `direction_labels`), so this file hardcodes none of them. Every
// series sits on the store's reading grid: a reading with no row is a gap slot, drawn as a hole and never as a
// zero, never carried forward, and never presented as a value in a readings panel (§5).
import React,{useMemo} from 'react';
import {View,ScrollView,Pressable} from 'react-native';
import {C,T,s} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {DirectionChip,IDLE_H,InfoDisclosure,Section,Tag,head,metaText,stateOf,tone,typeColor,
 type InfoGroup} from './frame';
import {SessionPanel,ReadingsPanel,type ReadingRow,type SessionSeries} from './SessionPanel';
import {BASIS_NO_SPOT,BASIS_NO_SPOT_ROW,DASH,FUTURES_BUILDUP_DEFINITION,FUTURES_BUILDUP_NO_POINTS,FUTURES_BUILDUP_READING_TEXT,
 IV_ATM_LABEL,IV_COMPUTED_TAG,IV_DEFINITION,IV_NO_POINTS,IV_READING_TEXT,
 MAX_PAIN_DEFINITION,MAX_PAIN_GAP_TEXT,MAX_PAIN_NO_POINTS,MAX_PAIN_READING_TEXT,
 NO_SYMBOL_TEXT,PCR_DEFINITION,PCR_NO_POINTS,PCR_OI_LABEL,PCR_READING_TEXT,PCR_VOLUME_LABEL,
 asOfText,basisPair,basisText,compact,directionRule,directionWords,dteText,expiryText,futuresChartTitle,futuresLine,
 ivComputedText,ivLatestRefusal,ivMethodText,ivPointReason,ivRateIsAssumed,ivRateSensitivity,
 ivRateSensitivityText,ivRateSourceShort,ivRateSourceText,ivRateText,ivRejectionLines,ivText,maxPainDistanceRule,
 maxPainDistanceText,maxPainLine,oiVsAvgText,pcrLine,pcrText,price,priceTick,ratio,readingsText,
 seriesTimes,seriesValues,servedBuildup,servedChip,servedTone,signed,strike as strikeText,withheldLines,
 withheldText} from './logic';
import type {BlockSymbol} from './logic';
import type {ChartTarget,FuturesBuildup,IvLeg,IvLegPoint,IvSeries,MaxPainSeries,PcrSeries} from './types';

/** Every session block sits on this rhythm: panels beside each other, one height, collapsing together. */
export type SessionBlockProps={symbol:string;choice:BlockSymbol;expiry:string;seq:number;height:number;
 stacked?:boolean;badge:string;
 target?:ChartTarget|null;onTarget?:(t:ChartTarget)=>void;
 hidden?:Record<string,boolean>;onHide?:(key:string)=>void;expanded?:string;onExpand?:(key:string)=>void};
/** The chart takes the larger share; the readings beside it are a narrow column of numbers. */
export const CHART_FLEX=2.2,READINGS_FLEX=1;
/** When a panel of a block is showing ONE SENTENCE, the whole block collapses to IDLE_H rather than holding a
 *  chart-sized void. Two cases qualify and only two: the tab has no symbol yet, and the endpoint is unavailable —
 *  both of which are a sentence and a control, nothing more.
 *
 *  A panel that is empty because the READING is empty is a different thing and keeps its full height: that is a
 *  captured answer, and points will be there at the next reading. Nothing is hidden either way — the sentence and
 *  its Retry are on screen at IDLE_H exactly as they were at full height. */
export function oneSentence(phase:string){return phase==='error';}
export function shrink(symbol:string,phase:string){return !symbol||oneSentence(phase);}
const paneStyle=(flex:number,height:number,stacked?:boolean,idle?:boolean)=>
 idle?(stacked?{height:IDLE_H}:{flex,minWidth:0,height:IDLE_H})
  :stacked?{height}:{flex,minWidth:0,height};
/** One subtitle shape for all four, so the blocks read as one family: symbol · expiry · days to expiry. */
export function blockSubtitle(symbol:string,expiry?:string|null,dte?:number|null){
 const when=expiry?expiryText(expiry,dte??null):dte==null?'':dteText(dte);
 return `${symbol}${when&&when!==DASH?` · ${when}`:''}`;
}
/** The two lines every block's disclosure ends with: how much of the session carried a value and why the rest
 *  did not, in the server's own sentences. A block whose session is whole says nothing here. */
function gapGroup(body:any):InfoGroup|null{
 const lines=withheldLines(body);
 const counted=readingsText(body);
 if(!lines.length&&!counted)return null;
 return {heading:'What is missing, and why',
  lines:[counted?`${counted}.`:'',String(body?.gaps_text||'').trim(),...lines]};
}

// =================================================================================================================
// 2. PCR through the session
// =================================================================================================================
export function PcrSection({symbol,expiry,seq,height,stacked,badge,hidden,onHide,expanded,onExpand}:SessionBlockProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 const path=symbol?`/api/derivatives/pcr-series?underlying=${encodeURIComponent(symbol)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<PcrSeries>(path,seq);
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
 const rows:ReadingRow[]=[
  {label:PCR_OI_LABEL,value:pcrText(body?.latest_pcr_oi),chip,chipTone},
  {label:PCR_VOLUME_LABEL,value:pcrText(body?.latest_pcr_volume),chip:volChip,chipTone:volTone},
  // the totals are per READING, not per session, so they come off the last reading that carried one
  {label:'Total call open interest',value:compact(lastPcr?.total_ce_oi)},
  {label:'Total put open interest',value:compact(lastPcr?.total_pe_oi)},
 ];
 const chart=!shows('pcr_chart')?null:<SessionPanel key="pcr_chart" name="PCR through the session"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state}
  times={times} series={series} shared axisFormat={v=>pcrText(v)} chip={chip} chipTone={chipTone} caveat={caveat}
  note={pcrLine(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('pcr_chart'):undefined} expanded={expanded==='pcr_chart'}
  onClose={onHide?()=>onHide('pcr_chart'):undefined}
  emptyDetail="Each line needs two 15-min readings that carried a value before it is a line rather than a dot."
  style={paneStyle(CHART_FLEX,height,stacked,idle)}/>;
 const readings=!shows('pcr_readings')?null:<ReadingsPanel key="pcr_readings" name="Latest PCR reading"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state} rows={rows}
  caveat={caveat} note={readingsText(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('pcr_readings'):undefined} expanded={expanded==='pcr_readings'}
  onClose={onHide?()=>onHide('pcr_readings'):undefined}
  emptyDetail="Nothing has been captured for this symbol at this 15-min reading."
  style={paneStyle(READINGS_FLEX,height,stacked,idle)}/>;
 if(!chart&&!readings)return null;
 return <Section title="PCR through the session"
  subtitle="Put open interest against call open interest, and the same division over volume, at every 15-min reading."
  asOf={asOfText(body?.as_of)} badge={badge}
  info={<InfoDisclosure title="PCR through the session" groups={[
   {heading:'PCR through the session',lines:[body?.definition||PCR_DEFINITION,PCR_READING_TEXT,
    directionRule(body),body?.chain_floors_text||'']},
   gapGroup(body)||{heading:'',lines:[]},
  ]}/>}
  stacked={stacked}>{chart}{readings}</Section>;
}

// =================================================================================================================
// 3. Max pain through the session
// =================================================================================================================
export function MaxPainSection({symbol,expiry,seq,height,stacked,badge,hidden,onHide,expanded,
 onExpand}:SessionBlockProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 // the route is `maxpain-series`, one word: the tab reads what the server serves, not what reads better
 const path=symbol?`/api/derivatives/maxpain-series?underlying=${encodeURIComponent(symbol)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<MaxPainSeries>(path,seq);
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
 const rows:ReadingRow[]=[
  {label:'Max pain strike',value:strikeText(body?.latest_max_pain_strike),chip,chipTone},
  {label:'Spot',value:price(body?.latest_spot),
   reason:'No spot was captured for this underlying at the latest reading.'},
  // the label names the arithmetic, so the sign on the line under it is unambiguous
  {label:'Distance (strike − spot)',value:maxPainDistanceText(body?.latest_distance),
   reason:'Distance needs both a strike and a spot at the same reading.'},
  {label:'Open interest behind it',value:compact(body?.latest_total_oi),
   reason:'Not captured at this 15-min reading.'},
 ];
 const chart=!shows('max_pain_chart')?null:<SessionPanel key="max_pain_chart" name="Max pain against spot"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state}
  times={times} series={series} shared axisFormat={v=>priceTick(v)} chip={chip} chipTone={chipTone}
  caveat={caveat} note={maxPainLine(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('max_pain_chart'):undefined} expanded={expanded==='max_pain_chart'}
  onClose={onHide?()=>onHide('max_pain_chart'):undefined}
  emptyDetail="Each line needs two 15-min readings that carried a value before it is a line rather than a dot."
  style={paneStyle(CHART_FLEX,height,stacked,idle)}/>;
 const readings=!shows('max_pain_readings')?null:<ReadingsPanel key="max_pain_readings" name="Latest max pain"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state} rows={rows}
  caveat={caveat} note={maxPainDistanceRule(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('max_pain_readings'):undefined} expanded={expanded==='max_pain_readings'}
  onClose={onHide?()=>onHide('max_pain_readings'):undefined}
  emptyDetail="Nothing has been captured for this symbol at this 15-min reading."
  style={paneStyle(READINGS_FLEX,height,stacked,idle)}/>;
 if(!chart&&!readings)return null;
 return <Section title="Max pain through the session"
  subtitle="The max-pain strike and the captured spot on one scale, so the distance between them is the distance on screen."
  asOf={asOfText(body?.as_of)} badge={badge}
  info={<InfoDisclosure title="Max pain through the session" groups={[
   {heading:'Max pain through the session',lines:[body?.definition||MAX_PAIN_DEFINITION,MAX_PAIN_GAP_TEXT,
    maxPainDistanceRule(body),MAX_PAIN_READING_TEXT,directionRule(body),body?.chain_floors_text||'']},
   gapGroup(body)||{heading:'',lines:[]},
  ]}/>}
  stacked={stacked}>{chart}{readings}</Section>;
}

// =================================================================================================================
// 4. IV through the session — THE ONE COMPUTED NUMBER ON THIS TAB
//
// Everything else here is something the exchange said. This is a model's output, and the reader must never be able
// to mistake the two. So the COMPUTED label rides on the block header, the chart panel, the legend, the legs list,
// every solved row and what a screen reader hears.
//
// But a label is not enough. A reader also has to be able to find out WHAT the number rests on, and the weakest of
// those inputs is the risk-free rate: a constant in the server's code, 6.5%, with no feed behind it. So the rate,
// that fact, and what a percentage point of it is worth to the answer are all ON SCREEN — the amber line under the
// chart, because an input nothing is feeding is a genuine caveat on the data — with the server's full sentence and
// every other modelling assumption behind the block's one control.
// =================================================================================================================
function IvLegList({legs,reasons,onPick,selected}:{legs:(IvLeg|null|undefined)[];
 reasons?:Record<string,string>|null;onPick?:(leg:IvLeg,point:IvLegPoint|null)=>void;
 selected?:(leg:IvLeg)=>boolean}){
 const rows=(legs||[]).filter((leg):leg is IvLeg=>!!leg);
 if(!rows.length)return null;
 return <ScrollView style={{flex:1}} contentContainerStyle={{paddingBottom:6}}>
  <View style={[s.row,{gap:6,paddingHorizontal:10,paddingVertical:4,borderBottomWidth:1,borderColor:C.line}]}>
   <T style={[head,{flex:1}]}>Leg at the money</T>
   {/* the label sits in the COLUMN HEADER too, so a reader scanning never meets a bare percentage */}
   <T style={head}>Implied vol</T><Tag label={IV_COMPUTED_TAG}/>
  </View>
  {rows.map(leg=>{
   const last=[...(leg.points||[])].reverse().find(p=>p&&p.iv!=null)||null;
   const refusal=last?'':ivLatestRefusal(leg.points,reasons)||String(leg.missing_text||'');
   const on=!!selected?.(leg);
   const inner=<>
    <View style={{flex:1,minWidth:0}}>
     <T numberOfLines={1} style={{fontSize:11,lineHeight:15,fontFamily:'InterSemi',
      color:typeColor(String(leg.option_type).toUpperCase()==='PE'?'put':'call')}}>
      {strikeText(leg.strike)} {String(leg.option_type||'').toUpperCase()}</T>
     <T numberOfLines={1} style={{fontSize:9,lineHeight:13,color:C.muted}}>
      {leg.tradingsymbol||DASH}{leg.readings_with_value!=null?` · ${leg.readings_with_value} solved`:''}</T>
    </View>
    {last
     ?<View style={[s.row,{gap:4}]}>
       <T style={{fontSize:11,lineHeight:15,fontVariant:['tabular-nums']}}>{ivText(last.iv_pct??last.iv)}</T>
       <Tag label={IV_COMPUTED_TAG}/></View>
     // a null is never a blank: the server's reason is what stands in its place
     :<T numberOfLines={3} style={{flex:1.4,fontSize:9,lineHeight:12,color:C.muted,textAlign:'right'}}>{refusal}</T>}
   </>;
   if(!onPick)return <View key={leg.option_type} style={[s.row,{gap:6,paddingHorizontal:10,paddingVertical:6,
    borderBottomWidth:1,borderColor:'#0F1B22'}]}>{inner}</View>;
   return <Pressable key={leg.option_type} accessibilityRole="button" accessibilityState={{selected:on}}
    accessibilityLabel={`${strikeText(leg.strike)} ${String(leg.option_type||'').toUpperCase()}. ${last?`computed implied volatility ${ivText(last.iv_pct??last.iv)}`:refusal||'no computed implied volatility'}`}
    onPress={()=>onPick(leg,last)}
    style={(st:any)=>[s.row,{gap:6,paddingHorizontal:10,paddingVertical:6,borderBottomWidth:1,
     borderColor:'#0F1B22',backgroundColor:on?C.soft:st.hovered||st.focused?C.dark:'transparent'}]}>{inner}</Pressable>;
  })}
 </ScrollView>;
}
export function IvSection({symbol,expiry,seq,height,stacked,badge,target,onTarget,hidden,onHide,expanded,
 onExpand}:SessionBlockProps){
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
 // THE CAVEAT ON THIS BLOCK. Not the fact that it is computed — the label says that — but that its rate is an
 // assumption nothing is feeding, and what that assumption is worth. Amber, because it is a caveat on the data.
 const rateCaveat=ivRateIsAssumed(body)
  ?`${ivRateSourceShort(body)} ${ivRateSensitivityText(shift)}`.trim():'';
 const refusals=ivRejectionLines(body);
 const latestRefusal=ivLatestRefusal(points,body?.reason_text);
 const series:SessionSeries[]=[
  {key:'iv',label:IV_ATM_LABEL,color:C.mint,values:iv,format:ivText,tag:IV_COMPUTED_TAG},
 ];
 const rows:ReadingRow[]=[
  {label:`${IV_ATM_LABEL} (computed)`,value:ivText(body?.latest_iv_pct??body?.latest_iv),
   tag:(body?.latest_iv_pct??body?.latest_iv)==null?undefined:IV_COMPUTED_TAG,chip,chipTone,
   reason:latestRefusal||'The server returned no volatility and no reason for this reading.'},
  {label:'At-the-money strike',value:strikeText(body?.atm_strike)},
  {label:'Risk-free rate used',value:ivRateText(body)||DASH,
   reason:'The server did not name the rate these were solved at.'},
 ];
 const chart=!shows('iv_chart')?null:<SessionPanel key="iv_chart" name="ATM implied volatility"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state}
  times={times} series={series} axisFormat={v=>ivText(v)} chip={chip} chipTone={chipTone}
  tag={IV_COMPUTED_TAG} tagA11y="Computed by a pricing model, not reported by the exchange"
  caveat={rateCaveat} note={body?`${readingsText(body)}. ${ivMethodText(body)}`.trim():undefined}
  onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('iv_chart'):undefined} expanded={expanded==='iv_chart'}
  onClose={onHide?()=>onHide('iv_chart'):undefined}
  emptyDetail="Each line needs two solved 15-min readings before it is a line rather than a dot."
  style={paneStyle(CHART_FLEX,height,stacked,idle)}/>;
 const list=!shows('iv_strikes')?null:<ReadingsPanel key="iv_strikes" name="Volatility by strike"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state} rows={rows}
  caveat={rateCaveat} note={refusals.length?`Refused readings — ${refusals.join(' ')}`:undefined}
  onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('iv_strikes'):undefined} expanded={expanded==='iv_strikes'}
  onClose={onHide?()=>onHide('iv_strikes'):undefined}
  emptyDetail="Nothing has been solved for this symbol at this 15-min reading."
  style={paneStyle(READINGS_FLEX,height,stacked,idle)}>
  <View style={{flex:1,minHeight:0,marginHorizontal:-10,marginBottom:-10}}>
   <IvLegList legs={legs} reasons={body?.reason_text}
    onPick={onTarget?(leg,last)=>onTarget({underlying:body?.underlying||symbol,
     instrumentToken:leg.instrument_token,
     label:leg.tradingsymbol||`${strikeText(leg.strike)} ${String(leg.option_type||'').toUpperCase()}`,
     detail:`${strikeText(leg.strike)} ${String(leg.option_type||'').toUpperCase()} · implied volatility ${ivText(last?.iv_pct??last?.iv)} ${IV_COMPUTED_TAG}`}):undefined}
    selected={leg=>leg.instrument_token!=null&&target?.instrumentToken===leg.instrument_token}/>
  </View>
 </ReadingsPanel>;
 if(!chart&&!list)return null;
 return <Section title="IV through the session"
  subtitle="Implied volatility is COMPUTED by a model from captured prices — every other number on this tab is one the exchange reported."
  asOf={asOfText(body?.as_of)} badge={badge}
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
   {heading:'When there is no volatility',lines:refusals.length?refusals
    :['Every reading in this session was solved.']},
  ]}/>}
  stacked={stacked}>{chart}{list}</Section>;
}

// =================================================================================================================
// 5. Futures build-up
// =================================================================================================================
export function FuturesBuildupSection({symbol,seq,height,stacked,badge,hidden,onHide,expanded,
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
 // two readings, two chips, both in the server's own words
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
 // OI and its own 20-day share are different units, so they get their own scales — the rule everywhere here.
 const oiSeries:SessionSeries[]=[
  {key:'oi',label:'Open interest',color:C.mint,values:oi,format:compact},
  {key:'share',label:'Against its 20-day average',color:C.amber,second:true,values:share,format:v=>ratio(v)},
 ];
 const basisSeries:SessionSeries[]=[
  {key:'basis',label:'Basis',color:C.mint,values:basis,format:basisText},
  {key:'basis_pct',label:'Basis %',color:C.amber,second:true,values:basisPct,format:v=>signed(v,2)},
 ];
 const last=[...points].reverse().find(p=>p&&!p.gap);
 const rows:ReadingRow[]=[
  {label:'Open interest',value:compact(last?.oi),chip:oiChip,chipTone:oiTone},
  {label:'Against its own 20-day average',value:oiVsAvgText(body?.latest_oi_vs_avg)},
  {label:'Basis (futures less spot)',value:basisPair(body?.latest_basis,body?.latest_basis_pct),
   chip:basisChip,chipTone:basisTone,reason:BASIS_NO_SPOT_ROW},
  {label:'Change since the previous close',value:signed(last?.oi_change_pct_day),
   tone:last?.oi_change_pct_day==null?undefined:tone(last.oi_change_pct_day)},
  // §3.1's two windows, never merged into one label
  {label:'Build-up on the day',value:servedBuildup(body?.latest_buildup_day)},
  {label:'Build-up over 15 minutes',value:servedBuildup(body?.latest_buildup_15m)},
 ];
 const oiPanel=!shows('fut_oi')?null:<SessionPanel key="fut_oi" name="Futures open interest"
  subtitle={contract} body={body} state={state} times={times} series={oiSeries} axisFormat={compact}
  chip={oiChip} chipTone={oiTone} caveat={caveat} note={futuresLine(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('fut_oi'):undefined} expanded={expanded==='fut_oi'}
  onClose={onHide?()=>onHide('fut_oi'):undefined}
  emptyDetail="Each line needs two 15-min readings that carried a value before it is a line rather than a dot."
  style={paneStyle(1.4,height,stacked,idle)}/>;
 const basisPanel=!shows('fut_basis')?null:<SessionPanel key="fut_basis" name="Basis through the session"
  subtitle={contract} body={body} state={state} times={times} series={basisSeries} axisFormat={priceTick}
  chip={basisChip} chipTone={basisTone} caveat={caveat}
  note={basisPair(body?.latest_basis,body?.latest_basis_pct)===DASH
   ?`Basis is the futures price less spot at the same 15-min reading. At the latest one, ${BASIS_NO_SPOT}.`
   :`Basis is the futures price less spot at the same 15-min reading: ${basisPair(body?.latest_basis,body?.latest_basis_pct)} at the latest one.`}
  onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('fut_basis'):undefined} expanded={expanded==='fut_basis'}
  onClose={onHide?()=>onHide('fut_basis'):undefined}
  emptyDetail="Each line needs two 15-min readings that carried a value before it is a line rather than a dot."
  style={paneStyle(1.4,height,stacked,idle)}/>;
 const readings=!shows('fut_readings')?null:<ReadingsPanel key="fut_readings" name="Latest futures reading"
  subtitle={contract} body={body} state={state} rows={rows} caveat={caveat} note={readingsText(body)}
  onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('fut_readings'):undefined} expanded={expanded==='fut_readings'}
  onClose={onHide?()=>onHide('fut_readings'):undefined}
  emptyDetail="Nothing has been captured for this contract at this 15-min reading."
  style={paneStyle(READINGS_FLEX,height,stacked,idle)}/>;
 if(!oiPanel&&!basisPanel&&!readings)return null;
 return <Section title="Futures build-up"
  subtitle="The front futures contract of this symbol: open interest against its own average, and the basis against spot."
  asOf={asOfText(body?.as_of)} badge={badge}
  info={<InfoDisclosure title="Futures build-up" groups={[
   {heading:'Futures build-up',lines:[body?.definition||FUTURES_BUILDUP_DEFINITION,FUTURES_BUILDUP_READING_TEXT,
    directionRule(body),
    'Open interest and its share of the 20-day average are different units, so each line keeps its own scale.']},
   gapGroup(body)||{heading:'',lines:[]},
  ]}/>}
  stacked={stacked}>{oiPanel}{basisPanel}{readings}</Section>;
}
