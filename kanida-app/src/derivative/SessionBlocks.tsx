// Four blocks of the Derivative tab, all built on the same rhythm the ΔOI block set: a Section title in large
// text, ONE as-of line and ONE "How to read this" in its header, the symbol badge beside them, and panels of a
// fixed height under it that collapse to IDLE_H when the tab has no symbol yet.
//
//   * PCR through the session      — pcr_oi and pcr_volume, with the direction chip.
//   * Max pain through the session — max pain against spot, so the gap between them is a gap you can see.
//   * IV through the session       — ATM implied volatility, and the latest per strike. COMPUTED, and said so
//                                    everywhere it appears rather than once at the bottom.
//   * Futures build-up             — open interest against its own average, and the basis, with the chip.
//
// Every one of them is pointed at the tab's ONE symbol, handed down; not one of them resolves its own. Every one
// of them describes readings that were captured and says nothing about what comes next (§5). A number the server
// withheld because a thin chain failed the liquidity floors is the server's own sentence in amber — a caveat on
// the data — and never a blank.
import React,{useMemo,useState} from 'react';
import {View,ScrollView,Pressable} from 'react-native';
import {C,T,s} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {DirectionChip,IDLE_H,InfoDisclosure,Section,Tag,head,metaText,stateOf,tone,typeColor} from './frame';
import {SessionPanel,ReadingsPanel,type ReadingRow,type SessionSeries} from './SessionPanel';
import {DASH,FUTURES_BUILDUP_DEFINITION,FUTURES_BUILDUP_NO_POINTS,FUTURES_BUILDUP_READING_TEXT,FUTURES_CHIPS,
 FUTURES_KEYS,IV_ATM_LABEL,IV_CHIPS,IV_COMPUTED_TAG,IV_DEFINITION,IV_KEYS,IV_NO_POINTS,IV_READING_TEXT,
 MAX_PAIN_CHIPS,MAX_PAIN_DEFINITION,MAX_PAIN_GAP_TEXT,MAX_PAIN_KEYS,MAX_PAIN_NO_POINTS,MAX_PAIN_READING_TEXT,
 NO_SYMBOL_TEXT,PCR_CHIPS,PCR_DEFINITION,PCR_KEYS,PCR_NO_POINTS,PCR_OI_LABEL,PCR_READING_TEXT,PCR_VOLUME_LABEL,
 asOfText,basisPair,basisText,buildupLabel,buildupTone,compact,crore,dteText,expiryText,futuresBuildupSummary,
 futuresChartTitle,ivComputedText,ivCoverageText,ivModelText,ivReasonText,ivStrikeSpoken,ivText,maxPainGapText,
 maxPainSummary,oiVsAvgText,pcrSummary,pcrText,price,priceTick,ratio,servedDirection,sessionChip,sessionTone,
 signed,signedUnits,strike as strikeText} from './logic';
import type {BlockSymbol} from './logic';
import type {ChartTarget,FuturesBuildup,IvSeries,IvStrikeRow,MaxPainSeries,PcrSeries} from './types';

/** Every session block sits on this rhythm: two panels beside each other, one height, collapsing together. */
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

// =================================================================================================================
// 2. PCR through the session
// =================================================================================================================
export function PcrSection({symbol,choice,expiry,seq,height,stacked,badge,hidden,onHide,expanded,onExpand}:SessionBlockProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 const path=symbol?`/api/derivatives/pcr-series?underlying=${encodeURIComponent(symbol)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<PcrSeries>(path,seq);
 const body=read.data;
 const state=symbol?stateOf(read,PCR_NO_POINTS):{phase:'empty' as const,text:NO_SYMBOL_TEXT};
 const points=body?.points||[];
 const times=useMemo(()=>points.map(p=>p.at??null),[points]);
 const oi=useMemo(()=>points.map(p=>p.pcr_oi??null),[points]);
 const volume=useMemo(()=>points.map(p=>p.pcr_volume??null),[points]);
 const direction=servedDirection(body?.direction,oi,PCR_KEYS);
 const chip=sessionChip(PCR_CHIPS,direction),chipTone=sessionTone(PCR_CHIPS,direction);
 const caveat=body?.withheld?(body.withheld_text||'').trim():'';
 const idle=shrink(symbol,read.phase);
 const series:SessionSeries[]=[
  {key:'pcr_oi',label:PCR_OI_LABEL,color:C.mint,values:oi,format:pcrText},
  {key:'pcr_volume',label:PCR_VOLUME_LABEL,color:C.amber,second:true,values:volume,format:pcrText},
 ];
 const rows:ReadingRow[]=[
  {label:PCR_OI_LABEL,value:pcrText(body?.latest_pcr_oi)},
  {label:PCR_VOLUME_LABEL,value:pcrText(body?.latest_pcr_volume)},
  {label:'Total call open interest',value:compact(body?.total_ce_oi)},
  {label:'Total put open interest',value:compact(body?.total_pe_oi)},
 ];
 const chart=!shows('pcr_chart')?null:<SessionPanel key="pcr_chart" name="PCR through the session"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state}
  times={times} series={series} axisFormat={v=>pcrText(v)} chip={chip} chipTone={chipTone} caveat={caveat}
  note={pcrSummary(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('pcr_chart'):undefined} expanded={expanded==='pcr_chart'}
  onClose={onHide?()=>onHide('pcr_chart'):undefined}
  emptyDetail="Each line needs two 15-min readings before it is a line rather than a dot."
  style={paneStyle(CHART_FLEX,height,stacked,idle)}/>;
 const readings=!shows('pcr_readings')?null:<ReadingsPanel key="pcr_readings" name="Latest PCR reading"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state} rows={rows}
  caveat={caveat} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('pcr_readings'):undefined} expanded={expanded==='pcr_readings'}
  onClose={onHide?()=>onHide('pcr_readings'):undefined}
  emptyDetail="Nothing has been captured for this symbol at this 15-min reading."
  style={paneStyle(READINGS_FLEX,height,stacked,idle)}>
  <View style={[s.row,{gap:6}]}><T style={metaText}>Over the last hour</T><DirectionChip label={chip} tone={chipTone}/></View>
 </ReadingsPanel>;
 if(!chart&&!readings)return null;
 return <Section title="PCR through the session"
  subtitle="Put open interest against call open interest, and the same division over volume, at every 15-min reading."
  asOf={asOfText(body?.as_of)} badge={badge}
  info={<InfoDisclosure title="PCR through the session" groups={[{heading:'PCR through the session',
   lines:[PCR_DEFINITION,body?.note||'',PCR_READING_TEXT,
    'A reading with no capture is a gap on both lines; nothing here is interpolated.',
    caveat?{text:caveat,tone:'amber' as const}:null]}]}/>}
  stacked={stacked}>{chart}{readings}</Section>;
}

// =================================================================================================================
// 3. Max pain through the session
// =================================================================================================================
export function MaxPainSection({symbol,choice,expiry,seq,height,stacked,badge,hidden,onHide,expanded,onExpand}:SessionBlockProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 const path=symbol?`/api/derivatives/max-pain-series?underlying=${encodeURIComponent(symbol)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<MaxPainSeries>(path,seq);
 const body=read.data;
 const state=symbol?stateOf(read,MAX_PAIN_NO_POINTS):{phase:'empty' as const,text:NO_SYMBOL_TEXT};
 const points=body?.points||[];
 const times=useMemo(()=>points.map(p=>p.at??null),[points]);
 const pain=useMemo(()=>points.map(p=>p.max_pain_strike??null),[points]);
 const spot=useMemo(()=>points.map(p=>p.spot??null),[points]);
 const direction=servedDirection(body?.direction,pain,MAX_PAIN_KEYS);
 const chip=sessionChip(MAX_PAIN_CHIPS,direction),chipTone=sessionTone(MAX_PAIN_CHIPS,direction);
 const caveat=body?.withheld?(body.withheld_text||'').trim():'';
 const idle=shrink(symbol,read.phase);
 // ONE scale, and the only place on this tab where two lines share one: a max-pain strike and spot are both
 // rupee levels of the same underlying, and the whole point of the panel is the distance between them.
 const series:SessionSeries[]=[
  {key:'max_pain',label:'Max pain',color:C.mint,values:pain,format:v=>strikeText(v)},
  {key:'spot',label:'Spot',color:C.amber,second:true,values:spot,format:v=>price(v)},
 ];
 const rows:ReadingRow[]=[
  {label:'Max pain strike',value:strikeText(body?.latest_max_pain)},
  {label:'Spot',value:price(body?.latest_spot)},
  {label:'Gap',value:maxPainGapText(body?.latest_gap)},
  {label:'Open interest behind it',value:compact(body?.total_oi),
   reason:'Not captured at this 15-min reading.'},
 ];
 const chart=!shows('max_pain_chart')?null:<SessionPanel key="max_pain_chart" name="Max pain against spot"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state}
  times={times} series={series} shared axisFormat={v=>priceTick(v)} chip={chip} chipTone={chipTone}
  caveat={caveat} note={maxPainSummary(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('max_pain_chart'):undefined} expanded={expanded==='max_pain_chart'}
  onClose={onHide?()=>onHide('max_pain_chart'):undefined}
  emptyDetail="Each line needs two 15-min readings before it is a line rather than a dot."
  style={paneStyle(CHART_FLEX,height,stacked,idle)}/>;
 const readings=!shows('max_pain_readings')?null:<ReadingsPanel key="max_pain_readings" name="Latest max pain"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state} rows={rows}
  caveat={caveat} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('max_pain_readings'):undefined} expanded={expanded==='max_pain_readings'}
  onClose={onHide?()=>onHide('max_pain_readings'):undefined}
  emptyDetail="Nothing has been captured for this symbol at this 15-min reading."
  style={paneStyle(READINGS_FLEX,height,stacked,idle)}>
  <View style={[s.row,{gap:6}]}><T style={metaText}>Over the last hour</T><DirectionChip label={chip} tone={chipTone}/></View>
 </ReadingsPanel>;
 if(!chart&&!readings)return null;
 return <Section title="Max pain through the session"
  subtitle="The max-pain strike and the captured spot on one scale, so the distance between them is the distance on screen."
  asOf={asOfText(body?.as_of)} badge={badge}
  info={<InfoDisclosure title="Max pain through the session" groups={[{heading:'Max pain through the session',
   lines:[MAX_PAIN_DEFINITION,MAX_PAIN_GAP_TEXT,body?.note||'',MAX_PAIN_READING_TEXT,
    'A reading with no capture is a gap on both lines; nothing here is interpolated.',
    caveat?{text:caveat,tone:'amber' as const}:null]}]}/>}
  stacked={stacked}>{chart}{readings}</Section>;
}

// =================================================================================================================
// 4. IV through the session — THE ONE COMPUTED NUMBER ON THIS TAB
//
// Everything else here is something the exchange said. This is a model's output, and the reader must never be able
// to mistake the two. So: the block's title carries the tag, the chart panel carries the tag, the strike list
// carries the tag in its header AND on every solved row, the summary line carries it, and what a screen reader
// hears carries it. Not a footnote. Where the server returns null with a reason, the reason is printed and no
// point is drawn.
// =================================================================================================================
function IvStrikeList({rows,onPick,selected}:{rows:IvStrikeRow[];onPick?:(row:IvStrikeRow)=>void;
 selected?:(row:IvStrikeRow)=>boolean}){
 return <ScrollView style={{flex:1}} contentContainerStyle={{paddingBottom:6}}>
  <View style={[s.row,{gap:6,paddingHorizontal:10,paddingVertical:4,borderBottomWidth:1,borderColor:C.line}]}>
   <T style={[head,{flex:1}]}>Strike</T>
   {/* the tag sits in the COLUMN HEADER as well, so a reader scanning the list never sees a bare percentage */}
   <T style={head}>Implied vol</T><Tag label={IV_COMPUTED_TAG}/>
  </View>
  {rows.map((row,i)=>{
   const has=row.iv!=null;
   const reason=has?'':ivReasonText(row.reason,row.reason_text);
   const on=!!selected?.(row);
   const inner=<>
    <View style={{flex:1,minWidth:0}}>
     <T numberOfLines={1} style={{fontSize:11,lineHeight:15,fontFamily:'InterSemi',
      color:typeColor(String(row.option_type).toUpperCase()==='PE'?'put':'call')}}>
      {strikeText(row.strike)} {String(row.option_type||'').toUpperCase()}</T>
     {!!row.moneyness&&<T numberOfLines={1} style={{fontSize:9,lineHeight:13,color:C.muted}}>{row.moneyness}</T>}
    </View>
    {has
     ?<View style={[s.row,{gap:4}]}>
       <T style={{fontSize:11,lineHeight:15,fontVariant:['tabular-nums']}}>{ivText(row.iv)}</T>
       <Tag label={IV_COMPUTED_TAG}/></View>
     // a null is never a blank: the server's reason is what stands in its place
     :<T numberOfLines={2} style={{flex:1.3,fontSize:9,lineHeight:12,color:C.muted,textAlign:'right'}}>{reason}</T>}
   </>;
   if(!onPick)return <View key={`${row.strike}-${row.option_type}-${i}`} style={[s.row,{gap:6,paddingHorizontal:10,
    paddingVertical:5,borderBottomWidth:1,borderColor:'#0F1B22'}]}>{inner}</View>;
   return <Pressable key={`${row.strike}-${row.option_type}-${i}`} accessibilityRole="button"
    accessibilityState={{selected:on}} accessibilityLabel={ivStrikeSpoken(row)} onPress={()=>onPick(row)}
    style={(st:any)=>[s.row,{gap:6,paddingHorizontal:10,paddingVertical:5,borderBottomWidth:1,
     borderColor:'#0F1B22',backgroundColor:on?C.soft:st.hovered||st.focused?C.dark:'transparent'}]}>{inner}</Pressable>;
  })}
 </ScrollView>;
}
export function IvSection({symbol,choice,expiry,seq,height,stacked,badge,target,onTarget,hidden,onHide,expanded,
 onExpand}:SessionBlockProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 const path=symbol?`/api/derivatives/iv-series?underlying=${encodeURIComponent(symbol)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<IvSeries>(path,seq);
 const body=read.data;
 const state=symbol?stateOf(read,IV_NO_POINTS):{phase:'empty' as const,text:NO_SYMBOL_TEXT};
 const points=body?.points||[];
 const times=useMemo(()=>points.map(p=>p.at??null),[points]);
 const iv=useMemo(()=>points.map(p=>p.iv??null),[points]);
 const direction=servedDirection(body?.direction,iv,IV_KEYS);
 const chip=sessionChip(IV_CHIPS,direction),chipTone=sessionTone(IV_CHIPS,direction);
 const caveat=body?.withheld?(body.withheld_text||'').trim():'';
 const idle=shrink(symbol,read.phase);
 const strikes=body?.strikes||[];
 // the latest reading that carried no volatility still carries WHY, and that is what the number panel prints
 const lastNull=[...points].reverse().find(p=>p.iv==null&&!!String(p.reason||'').trim());
 const series:SessionSeries[]=[
  {key:'iv',label:IV_ATM_LABEL,color:C.mint,values:iv,format:ivText,tag:IV_COMPUTED_TAG},
 ];
 const rows:ReadingRow[]=[
  {label:`${IV_ATM_LABEL} (computed)`,value:ivText(body?.latest_iv),tag:body?.latest_iv==null?undefined:IV_COMPUTED_TAG,
   reason:ivReasonText(lastNull?.reason,lastNull?.reason_text)||'The server returned no volatility and no reason for this reading.'},
  {label:'At-the-money strike',value:strikeText(body?.atm_strike)},
  {label:'Spot',value:price(body?.spot)},
 ];
 const chart=!shows('iv_chart')?null:<SessionPanel key="iv_chart" name="ATM implied volatility"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state}
  times={times} series={series} axisFormat={v=>ivText(v)} chip={chip} chipTone={chipTone}
  tag={IV_COMPUTED_TAG} tagA11y="Computed by a pricing model, not reported by the exchange"
  caveat={caveat} note={body?`${ivCoverageText(points)} ${ivModelText(body)}`.trim():undefined}
  onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('iv_chart'):undefined} expanded={expanded==='iv_chart'}
  onClose={onHide?()=>onHide('iv_chart'):undefined}
  emptyDetail="Each line needs two solved 15-min readings before it is a line rather than a dot."
  style={paneStyle(CHART_FLEX,height,stacked,idle)}/>;
 const list=!shows('iv_strikes')?null:<ReadingsPanel key="iv_strikes" name="Volatility by strike"
  subtitle={blockSubtitle(symbol,body?.expiry,body?.days_to_expiry)} body={body} state={state} rows={rows}
  caveat={caveat} note={body?ivModelText(body):undefined} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('iv_strikes'):undefined} expanded={expanded==='iv_strikes'}
  onClose={onHide?()=>onHide('iv_strikes'):undefined}
  emptyDetail="Nothing has been solved for this symbol at this 15-min reading."
  style={paneStyle(READINGS_FLEX,height,stacked,idle)}>
  <View style={{flex:1,minHeight:0,marginHorizontal:-10,marginBottom:-10}}>
   <IvStrikeList rows={strikes}
    onPick={onTarget?row=>onTarget({underlying:body?.underlying||symbol,instrumentToken:row.instrument_token,
     label:row.tradingsymbol||`${strikeText(row.strike)} ${String(row.option_type||'').toUpperCase()}`,
     detail:`${strikeText(row.strike)} ${String(row.option_type||'').toUpperCase()} · implied volatility ${ivText(row.iv)} ${IV_COMPUTED_TAG}`}):undefined}
    selected={row=>row.instrument_token!=null&&target?.instrumentToken===row.instrument_token}/>
  </View>
 </ReadingsPanel>;
 if(!chart&&!list)return null;
 return <Section title="IV through the session"
  subtitle="Implied volatility is COMPUTED by a model from captured prices — every other number on this tab is one the exchange reported."
  asOf={asOfText(body?.as_of)} badge={badge}
  actions={<Tag label={IV_COMPUTED_TAG} a11y="Every number in this block is computed by a pricing model, not reported by the exchange"/>}
  info={<InfoDisclosure title="IV through the session" groups={[
   {heading:'Computed, not reported',lines:[ivComputedText(body),ivModelText(body),
    'Where a price could not be solved the server returns nothing and says why, and no point is drawn for that reading.']},
   {heading:'IV through the session',lines:[IV_DEFINITION,body?.note||'',IV_READING_TEXT,ivCoverageText(points),
    caveat?{text:caveat,tone:'amber' as const}:null]},
   {heading:'When there is no volatility',lines:Object.values(IV_REASON_LINES)},
  ]}/>}
  stacked={stacked}>{chart}{list}</Section>;
}
/** The five reasons a solve returns nothing, listed once in the block's definitions so a reader meets the words
 *  before they meet them on a row. */
const IV_REASON_LINES:Record<string,string>={
 stale_trade:'Stale trade: the last trade in the contract is older than the reading it would be solved at.',
 no_time_value:'No time value: at that price there is nothing left to solve.',
 below_intrinsic:'Below intrinsic: the traded price is under what the contract is already worth at expiry.',
 no_convergence:'No convergence: the solver did not settle on an answer.',
 expiry_today:'Expires today: there is no time left to price.',
};

// =================================================================================================================
// 5. Futures build-up
// =================================================================================================================
export function FuturesBuildupSection({symbol,choice,seq,height,stacked,badge,hidden,onHide,expanded,
 onExpand}:SessionBlockProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 const path=symbol?`/api/derivatives/futures-buildup?underlying=${encodeURIComponent(symbol)}`:null;
 const read=useDerivativeRead<FuturesBuildup>(path,seq);
 const body=read.data;
 const state=symbol?stateOf(read,FUTURES_BUILDUP_NO_POINTS):{phase:'empty' as const,text:NO_SYMBOL_TEXT};
 const points=body?.points||[];
 const times=useMemo(()=>points.map(p=>p.at??null),[points]);
 const oi=useMemo(()=>points.map(p=>p.oi??null),[points]);
 const share=useMemo(()=>points.map(p=>p.oi_vs_avg??null),[points]);
 const basis=useMemo(()=>points.map(p=>p.basis??null),[points]);
 const basisPct=useMemo(()=>points.map(p=>p.basis_pct??null),[points]);
 const direction=servedDirection(body?.direction,oi,FUTURES_KEYS);
 const chip=sessionChip(FUTURES_CHIPS,direction),chipTone=sessionTone(FUTURES_CHIPS,direction);
 const caveat=body?.withheld?(body.withheld_text||'').trim():'';
 const idle=shrink(symbol,read.phase);
 const contract=futuresChartTitle(body?.contract,symbol);
 // OI and its own 20-day share are different units, so they get their own scales — the same rule as everywhere.
 const oiSeries:SessionSeries[]=[
  {key:'oi',label:'Open interest',color:C.mint,values:oi,format:compact},
  {key:'share',label:'Against its 20-day average',color:C.amber,second:true,values:share,format:v=>ratio(v)},
 ];
 const basisSeries:SessionSeries[]=[
  {key:'basis',label:'Basis',color:C.mint,values:basis,format:basisText},
  {key:'basis_pct',label:'Basis %',color:C.amber,second:true,values:basisPct,format:v=>signed(v,2)},
 ];
 const rows:ReadingRow[]=[
  {label:'Open interest',value:compact(body?.latest_oi)},
  {label:'Change since the previous close',value:signedUnits(body?.oi_change_day),
   tone:body?.oi_change_day==null?undefined:tone(body.oi_change_day)},
  {label:'Against its own 20-day average',value:oiVsAvgText(body?.oi_vs_20d_avg,body?.avg_sessions)},
  {label:'Basis (futures less spot)',value:basisPair(body?.basis,body?.basis_pct)},
  {label:'Build-up on the day',value:buildupLabel(body?.buildup_day),
   tone:buildupTone(body?.buildup_day)==='flat'?undefined:(buildupTone(body?.buildup_day)==='up'?C.green:C.red)},
  {label:'Build-up over 15 minutes',value:buildupLabel(body?.buildup_15m)},
 ];
 const oiPanel=!shows('fut_oi')?null:<SessionPanel key="fut_oi" name="Futures open interest"
  subtitle={contract} body={body} state={state} times={times} series={oiSeries} axisFormat={compact}
  chip={chip} chipTone={chipTone} caveat={caveat} note={futuresBuildupSummary(body)} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('fut_oi'):undefined} expanded={expanded==='fut_oi'}
  onClose={onHide?()=>onHide('fut_oi'):undefined}
  emptyDetail="Each line needs two 15-min readings before it is a line rather than a dot."
  style={paneStyle(1.4,height,stacked,idle)}/>;
 const basisPanel=!shows('fut_basis')?null:<SessionPanel key="fut_basis" name="Basis through the session"
  subtitle={contract} body={body} state={state} times={times} series={basisSeries} axisFormat={priceTick}
  caveat={caveat}
  // the sentence only earns its place when there is a number to put in it; a dash needs no explaining
  note={basisPair(body?.basis,body?.basis_pct)===DASH?'Basis is the futures price less spot at the same 15-min reading.'
   :`Basis is the futures price less spot at the same 15-min reading: ${basisPair(body?.basis,body?.basis_pct)} at the latest one.`}
  onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('fut_basis'):undefined} expanded={expanded==='fut_basis'}
  onClose={onHide?()=>onHide('fut_basis'):undefined}
  emptyDetail="Each line needs two 15-min readings before it is a line rather than a dot."
  style={paneStyle(1.4,height,stacked,idle)}/>;
 const readings=!shows('fut_readings')?null:<ReadingsPanel key="fut_readings" name="Latest futures reading"
  subtitle={contract} body={body} state={state} rows={rows} caveat={caveat} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('fut_readings'):undefined} expanded={expanded==='fut_readings'}
  onClose={onHide?()=>onHide('fut_readings'):undefined}
  emptyDetail="Nothing has been captured for this contract at this 15-min reading."
  style={paneStyle(READINGS_FLEX,height,stacked,idle)}>
  <View style={[s.row,{gap:6}]}><T style={metaText}>Over the last hour</T><DirectionChip label={chip} tone={chipTone}/></View>
 </ReadingsPanel>;
 if(!oiPanel&&!basisPanel&&!readings)return null;
 return <Section title="Futures build-up"
  subtitle="The front futures contract of this symbol: open interest against its own average, and the basis against spot."
  asOf={asOfText(body?.as_of)} badge={badge}
  info={<InfoDisclosure title="Futures build-up" groups={[{heading:'Futures build-up',
   lines:[FUTURES_BUILDUP_DEFINITION,body?.note||'',FUTURES_BUILDUP_READING_TEXT,
    'Open interest and its share of the 20-day average are different units, so each line keeps its own scale.',
    'A reading with no capture is a gap on every line; nothing here is interpolated.',
    caveat?{text:caveat,tone:'amber' as const}:null]}]}/>}
  stacked={stacked}>{oiPanel}{basisPanel}{readings}</Section>;
}
