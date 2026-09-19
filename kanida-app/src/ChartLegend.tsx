import React,{memo} from 'react';
import {View,Pressable,Platform} from 'react-native';
import {C,T,s,Icon} from './ui';
import {Match,History,pct,dateText} from './model';
import {boundaryValuesAt,boundaryConfirmIndex,BoundaryValue} from './patternGeometry';
import {useTitle} from './layout/shared';
import {sampleSize,dataAgeText} from './decision';
import {ROUND_TRIP_COST_PCT} from './constants';
// Phase 0.3 legend control centre. Pure presentation: every number is a stored candle, a detector line value or a server evidence value.
// Nothing here computes statistics; missing values render "n/a" (or the shared pct() "—"), never a back-filled number.

export type LegendBar={time:string;end?:string;open?:number|null;high?:number|null;low?:number|null;close?:number|null;volume?:number|null};
export type LegendPatternRow={key:string;name:string;timeframe:string;direction?:string;found:boolean;boundaries?:BoundaryValue[];/** Q4: the readout bar is before the pattern's confirmation index, so boundary values are withheld (null) and shown as "—". */ beforeConfirm?:boolean;confirmIndex?:number|null;visible:boolean};
export type LegendExitState='default_benchmark'|'limited'|'supported'|'failed'|'unknown';
export type LegendEvidence={keyMatches:boolean;avg?:number|null;low95?:number|null;high95?:number|null;n?:number|null;sampleLabel?:string;costPct?:number;/** true when the server sent no cost_pct and ROUND_TRIP_COST_PCT is shown instead */ costAssumed?:boolean;nextOpen:boolean;historyLabel?:string;exitState?:LegendExitState;tradable?:boolean;tradableReason?:string;dataAgeDays?:number|null;stale?:boolean;side?:string;
 wins?:number|null;testedHold?:string;breakout?:'waiting'|'recorded';current?:boolean;exitRule?:string};
export type ChartLegendProps={bars:LegendBar[];cursor:number|null;symbol:string;timeframe:string;dataEnd?:string;rows:LegendPatternRow[];selectedKey?:string;evidence?:LegendEvidence|null;onToggleVisible?:(rowKey:string)=>void;onSelect?:(rowKey:string)=>void;onOpenEvidence?:()=>void;onHide?:(rowKey:string)=>void;compact?:boolean};
export type PatternCatalogueItem={id:string;name:string};

const finite=(v:unknown):v is number=>typeof v==='number'&&Number.isFinite(v);
export const legendPrice=(v?:number|null)=>finite(v)?v.toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2}):'n/a';
const legendPct=(v?:number|null)=>finite(v)?pct(v):'n/a';
// /api/chart bar times are naive exchange-local (IST) strings "YYYY-MM-DD HH:MM:SS", so the clock is read from the string, never re-zoned by the browser.
export function legendBarTime(bar:LegendBar|undefined,timeframe:string){if(!bar?.time)return 'n/a';const date=dateText(bar.time),clock=bar.time.slice(11,16);return /H$/i.test(timeframe)&&/^\d{2}:\d{2}$/.test(clock)?`${date} · ${clock} IST`:date;}
// OHLC for the hovered bar, or the last bar when the cursor is null or out of range. Change is close vs the previous stored close; n/a on the first bar.
export function barReadout(bars:LegendBar[],cursor:number|null){
 const list=Array.isArray(bars)?bars:[],hovered=cursor!=null&&!!list[cursor],index=hovered?cursor as number:list.length-1,bar=list[index],prev=index>0?list[index-1]:undefined;
 const change=finite(bar?.close)&&finite(prev?.close)?bar!.close!-prev!.close!:null,changePct=change!=null&&prev!.close?change/prev!.close!*100:null;
 return {index,bar,change,changePct,hovered};
}
// The chart response is for this exact symbol and timeframe (the canvas keeps its previous snapshot while a new one loads).
export function chartKeyMatches(chartData:any,symbol?:string|null,timeframe?:string|null){return !!chartData&&!!symbol&&!!timeframe&&chartData.symbol===symbol&&chartData.timeframe===timeframe;}
// Rows from GET /api/chart (engine cell: {symbol,timeframe,last_candle,bars,matches:[{id,pattern,pattern_name,direction,lines}]}).
// Detected matches are found (selected first); enabled pattern ids without a match are "Not found on this chart". A chart for another key yields [].
export const chartMatchKey=(match:{id?:string;detection_id?:string})=>match.detection_id||match.id||'';
export function legendRowsFromChart(chartData:any,selectedMatch:Pick<Match,'id'|'symbol'|'timeframe'|'detection_id'>|null|undefined,options:{enabledPatterns?:string[];catalogue?:PatternCatalogueItem[];cursor?:number|null;hidden?:string[]}={}):LegendPatternRow[]{
 if(!selectedMatch||!chartKeyMatches(chartData,selectedMatch.symbol,selectedMatch.timeframe))return [];
 const {symbol,timeframe}=selectedMatch,bars:any[]=Array.isArray(chartData.bars)?chartData.bars:[],at=options.cursor!=null&&bars[options.cursor]?options.cursor:bars.length-1;
 const hidden=new Set(options.hidden||[]),keyOf=(pattern:string)=>`${symbol}:${timeframe}:${pattern}`,matches:any[]=Array.isArray(chartData.matches)?chartData.matches:[];
 // Q4: lines are fitted as of data end; values only for bars at/after the confirmation index (latest line point / end_index), else null ("—").
 const found:LegendPatternRow[]=matches.map(m=>{const key=String(chartMatchKey(m)||keyOf(m.pattern)),from=boundaryConfirmIndex(m),before=from==null||at<from,values=boundaryValuesAt(m,at);return {key,name:String(m.pattern_name||m.pattern),timeframe,direction:m.direction,found:true,boundaries:before?values.map(b=>({...b,value:null})):values,beforeConfirm:before,confirmIndex:from,visible:!hidden.has(key)}});
 found.sort((a,b)=>Number(b.key===chartMatchKey(selectedMatch))-Number(a.key===chartMatchKey(selectedMatch)));
 const seen=new Set(matches.map(m=>m.pattern));
 const missing=(options.enabledPatterns||[]).filter((id,i,all)=>!seen.has(id)&&all.indexOf(id)===i).map(id=>{const key=keyOf(id);return {key,name:options.catalogue?.find(p=>p.id===id)?.name||id,timeframe,found:false,visible:!hidden.has(key)}});
 return [...found,...missing];
}
export const HOLD_PERIOD_HISTORY_LABEL='Pattern hold-period history, not this exit rule';
// Maps the values Workspace's StockWorkspace already holds into LegendEvidence. Headline numbers are the scanner's fixed-hold `reference` history
// (next-open fills, 0.40% round trip netted on both sides in market_scanner/backtest.py simulate), never the exit rule, so the history label is always set.
// keyMatches is false (no numbers) unless: the history belongs to this match, the legend key and chart snapshot are this match's symbol/timeframe/data_end,
// the exit side (when present) is the history's side, and the exit-plan request has settled.
export function evidenceFromWorkspace({match,history,exit,chartData,legendKey,age,stale}:{match?:Match|null;history?:History|null;exit?:{data:any;error?:string}|null;chartData?:any;legendKey?:{symbol:string;timeframe:string;selectedKey?:string};age:number|null;stale:boolean}):LegendEvidence|null{
 // A live match without history has unavailable evidence, not an outstanding evidence request.
 if(match&&!history&&!match.history.length)return null;
 const d=exit?.data,settled=!exit||!!d||!!exit.error;
 const episodeMatches=!match?.detection_id||((chartData?.matches||[]).some((m:any)=>m.detection_id===match.detection_id&&m.id===match.id)&&(!d||d.detection_id===match.detection_id));
 const same=episodeMatches&&!!match&&!!history&&match.history.includes(history)&&(!legendKey||(legendKey.symbol===match.symbol&&legendKey.timeframe===match.timeframe&&(legendKey.selectedKey==null||legendKey.selectedKey===chartMatchKey(match))))
  &&(chartData===undefined||(chartKeyMatches(chartData,match.symbol,match.timeframe)&&(!chartData.last_candle||chartData.last_candle===match.candle_end)))&&(!d?.side||d.side===history.side);
 if(!same||!settled)return {keyMatches:false,nextOpen:false};
 const stats=history!.reference,status=d?.status,ci=stats?.expectancy_ci95,serverCost=finite(d?.cost_pct)?d.cost_pct as number:null;
 // Q2 lines use only what SetupSummary used: reference win_rate×n (profitable x of n), reference holding.duration, match.state/current, and the exit plan's tradable_evidence/reward/holding.
 const wins=finite(stats?.win_rate)&&finite(stats?.n)?Math.round(stats!.win_rate!*stats!.n/100):null,exitRule=d?`${d.tradable_evidence===true?'Tested on later data · this exact rule':'Illustrative · no tested evidence for this exact rule'} · 1:${d.reward} · ${d.holding?.duration||'n/a'} maximum hold`:undefined;
 return {keyMatches:true,avg:stats?.display_return_pct??null,low95:finite(ci?.[0])?ci![0]:null,high95:finite(ci?.[1])?ci![1]:null,n:finite(stats?.n)?stats.n:null,sampleLabel:stats?sampleSize(stats.n).label:undefined,
  // Q5 cost: exit-plan cost_pct (market_scanner/exit_plan.py: the run's round_trip_fee_bps + round_trip_slippage_bps, the same config backtest.py nets from each reference trade); ROUND_TRIP_COST_PCT only when absent, labelled "assumed".
  // Q5 entry: no server field describes the reference history's entry (exit-plan `entry` describes the exit rule), so next-open stays fixed. Source of truth: market_scanner/backtest.py simulate() "Fill next open".
  costPct:serverCost??ROUND_TRIP_COST_PCT,costAssumed:serverCost==null,nextOpen:true,historyLabel:String(d?.history_label||HOLD_PERIOD_HISTORY_LABEL),
  exitState:status==='default'?'default_benchmark':status==='limited'||status==='supported'||status==='failed'?status:'unknown',
  tradable:d?d.tradable_evidence===true:undefined,tradableReason:d?String(d.tradable_reason||''):exit?.error?'Exit-rule evidence could not load.':undefined,dataAgeDays:age,stale,side:history!.side,
  wins,testedHold:stats?.holding?.duration,breakout:match!.state==='setup'?'waiting':'recorded',current:!!match!.current,exitRule};
}

type Tone='green'|'amber'|'red'|'neutral';
const toneColor=(t:Tone)=>t==='green'?C.green:t==='amber'?C.amber:t==='red'?C.red:C.muted;
const EXIT_STATE:Record<LegendExitState,[string,Tone]>={default_benchmark:['OOS: Default benchmark','amber'],limited:['OOS: Limited evidence','amber'],supported:['OOS: History supported','green'],failed:['OOS: Exit test failed','red'],unknown:['OOS: n/a','neutral']};
const sampleTone=(label?:string):Tone=>/larger/i.test(label||'')?'green':/moderate/i.test(label||'')?'neutral':'amber';
// aria-pressed reaches the DOM on web; native uses accessibilityState.
const pressed=(on:boolean):any=>Platform.OS==='web'?{'aria-pressed':on}:{};
function Pill({label,tone='neutral'}:{label:string;tone?:Tone}){return <View style={{borderRadius:5,paddingHorizontal:6,paddingVertical:1,backgroundColor:tone==='amber'?C.amberBg:tone==='neutral'?C.bg:C.soft,borderWidth:1,borderColor:tone==='red'?'#4A2227':C.line}}><T style={{fontSize:10,lineHeight:15,color:toneColor(tone)}}>{label}</T></View>;}

const LegendHeader=memo(function LegendHeader({bars,cursor,symbol,timeframe,dataEnd}:Pick<ChartLegendProps,'bars'|'cursor'|'symbol'|'timeframe'|'dataEnd'>){
 const r=barReadout(bars,cursor),b=r.bar,col=r.change==null?C.muted:r.change>=0?C.green:C.red,v={fontSize:11,color:C.ink};
 return <View style={[s.row,{flexWrap:'wrap',gap:8,rowGap:0}]}>
  <T style={{fontSize:12,lineHeight:18,fontFamily:'InterSemi'}}>{symbol||'n/a'} · {timeframe||'n/a'} · NSE</T>
  <T style={{fontSize:11,lineHeight:18,color:C.muted}}>O <T style={v}>{legendPrice(b?.open)}</T>  H <T style={v}>{legendPrice(b?.high)}</T>  L <T style={v}>{legendPrice(b?.low)}</T>  C <T style={v}>{legendPrice(b?.close)}</T></T>
  <T style={{fontSize:11,lineHeight:18,color:col}}>{r.change==null?'n/a':`${r.change>0?'+':''}${legendPrice(r.change)} (${legendPct(r.changePct)})`}</T>
  <T style={{fontSize:11,lineHeight:18,color:C.muted}}>{legendBarTime(b,timeframe)}{b&&!r.hovered?' · last bar':''}{dataEnd?` · data to ${dateText(dataEnd)}`:''}</T>
 </View>;
});

const LegendRow=memo(function LegendRow({row,selected,onToggleVisible,onSelect,onHide}:{row:LegendPatternRow;selected:boolean;onToggleVisible?:(k:string)=>void;onSelect?:(k:string)=>void;onHide?:(k:string)=>void}){
 const values=row.found?(row.boundaries?.length?row.boundaries.map(b=>`${b.label} ${row.beforeConfirm?'—':legendPrice(b.value)}`).join(' · '):'boundaries n/a'):'';
 // Q4 hint (hover title + accessible name): the boundary lines are fitted as of data end, so earlier bars show "—".
 const fitted=row.found&&!!row.boundaries?.length?`Line fitted as of data end${row.beforeConfirm?'; values shown only from the pattern’s confirmation bar (its last line point) onward':''}`:undefined,tip=useTitle(fitted);
 const icon={minWidth:30,minHeight:30,alignItems:'center' as const,justifyContent:'center' as const,borderRadius:6};
 return <View style={[s.row,{gap:2,opacity:row.visible?1:.55}]}>
  <Pressable ref={tip} accessibilityRole="button" accessibilityLabel={`${row.name}, ${row.timeframe}. ${row.found?`Found on this chart. ${values}.${fitted?` ${fitted}.`:''}`:'Not found on this chart.'}${selected?' Selected.':' Select this pattern.'}`} accessibilityState={{selected}} {...pressed(selected)} disabled={!onSelect} onPress={()=>onSelect?.(row.key)} style={{flex:1,minWidth:0,minHeight:30,justifyContent:'center',paddingHorizontal:6,borderRadius:6,borderLeftWidth:2,borderLeftColor:selected?C.green:'transparent',backgroundColor:selected?C.soft:'transparent'}}>
   <T numberOfLines={selected?2:1} style={{fontSize:11,lineHeight:16,color:C.muted}}><T style={{fontSize:11,fontFamily:'InterSemi',color:selected?C.green:C.ink}}>{row.name}</T> · {row.timeframe}{row.direction?` · ${row.direction}`:''} · {row.found?<><T style={{fontSize:11,color:C.green}}>Found</T>{values?`  ${values}`:''}</>:<T style={{fontSize:11,color:C.amber,opacity:.78}}>Not found on this chart</T>}</T>
  </Pressable>
  {row.found&&!!onToggleVisible&&<Pressable accessibilityRole="button" accessibilityLabel={`${row.visible?'Hide':'Show'} ${row.name} outline on the chart`} accessibilityState={{checked:row.visible}} {...pressed(row.visible)} onPress={()=>onToggleVisible(row.key)} style={icon}><Icon name={row.visible?'eye':'eye-off'} size={14} color={row.visible?C.ink:C.muted}/></Pressable>}
  {!!onHide&&<Pressable accessibilityRole="button" accessibilityLabel={`Remove ${row.name} from the legend`} onPress={()=>onHide(row.key)} style={icon}><Icon name="x" size={14} color={C.muted}/></Pressable>}
 </View>;
});

// Key guard: evidence for another key (or still loading) renders a neutral state and no numbers.
const LegendEvidenceBlock=memo(function LegendEvidenceBlock({evidence,onOpenEvidence}:{evidence?:LegendEvidence|null;onOpenEvidence?:()=>void}){
 if(evidence===undefined)return null;
 if(!evidence||!evidence.keyMatches)return <View style={{paddingLeft:10,paddingBottom:2}}><T accessibilityLiveRegion="polite" style={{fontSize:11,color:C.muted}}>{evidence?'Loading evidence…':'Evidence n/a'}</T></View>;
 const e=evidence,exit=EXIT_STATE[e.exitState||'unknown'],sample=e.sampleLabel,n=finite(e.n)?String(e.n):'n/a';
 const age=`${e.dataAgeDays===undefined?'Data age unknown':dataAgeText(e.dataAgeDays)}${e.stale?' · stale':''}`;
 const pills:[string,Tone][]=[[`avg ${legendPct(e.avg)}/trade`,finite(e.avg)&&e.avg>0?'green':'neutral'],[`95% low ${legendPct(e.low95)}`,'neutral'],[`n ${n}${sample?` · ${sample}`:''}`,sampleTone(sample)],
  [finite(e.costPct)?`costs ${e.costPct.toFixed(2)}% incl.${e.costAssumed?' (assumed)':''}`:'costs n/a',finite(e.costPct)&&!e.costAssumed?'neutral':'amber'],[e.nextOpen?'next-open entry':'entry basis n/a',e.nextOpen?'neutral':'amber'],exit,[age,e.stale?'amber':'neutral']];
 if(e.side==='short')pills.push(['hypothetical short study','amber']);
 if(e.tradable===true)pills.push(['exact exit rule tested','green']);
 // Q2 lines (formerly SetupSummary): profitable x of n + tested hold, breakout state (+ fresh prices needed), exit-rule line, one "partly luck" caveat.
 const small={fontSize:10,lineHeight:14,color:C.muted},profitable=`Profitable ${finite(e.wins)&&finite(e.n)?`${e.wins} of ${e.n}`:'n/a'} · tested hold ${e.testedHold||'n/a'}`,breakout=`${e.breakout==='waiting'?'Waiting for breakout':'Breakout recorded'}${e.current===false?' · Fresh prices are needed before this stored setup can become a current trade.':''}`;
 const exitLine=e.exitRule?`Exit rule: ${e.exitRule}${e.tradable===true?'':' · the history above does not describe this stop, target and hold'}`:'',caveat='Picked from many results: partly luck; the range doesn’t correct for that.';
 const label=`Evidence for the selected pattern. ${e.historyLabel||''}. Average ${legendPct(e.avg)} per trade, 95% range ${legendPct(e.low95)} to ${legendPct(e.high95)}, ${n} trades${sample?`, ${sample}`:''}. ${pills.slice(3).map(p=>p[0]).join(', ')}. ${profitable}. ${breakout}${exitLine?` ${exitLine}.`:''} ${caveat}`;
 return <View role="group" accessibilityLabel={label} style={{gap:3,paddingLeft:10,paddingBottom:3}}>
  {!!e.historyLabel&&<T style={{fontSize:10,lineHeight:14,fontFamily:'InterMedium',color:C.amber}}>{e.historyLabel}</T>}
  <View style={[s.row,{flexWrap:'wrap',gap:3}]}>{pills.map(([l,t])=><Pill key={l} label={l} tone={t}/>)}</View>
  <T style={small}>{profitable}</T>
  <T style={{...small,color:e.current===false?C.amber:C.muted}}>{breakout}</T>
  {!!exitLine&&<T style={{...small,color:e.tradable===true?C.ink:C.amber}}>{exitLine}</T>}
  <T style={small}>{caveat}</T>
  {e.tradable===false&&<T style={{fontSize:10,lineHeight:14,color:C.amber}}>{e.tradableReason||'Exit rule illustrative · no tested evidence for this exact rule.'}</T>}
  {!!onOpenEvidence&&<Pressable accessibilityRole="link" accessibilityLabel="Show evidence for the selected pattern" onPress={onOpenEvidence} style={{alignSelf:'flex-start',minHeight:26,justifyContent:'center'}}><T style={{fontSize:11,color:C.green}}>Show evidence ›</T></Pressable>}
 </View>;
});

export const ChartLegend=memo(function ChartLegend({bars,cursor,symbol,timeframe,dataEnd,rows,selectedKey,evidence,onToggleVisible,onSelect,onOpenEvidence,onHide,compact=false}:ChartLegendProps){
 if(compact){
  // Collapsed one-liner: symbol · TF · OHLC of the hovered (or last) bar, then the selected (or first found) pattern, its found state and, only when the key matches, the headline evidence.
  // Two truncating segments: the OHLC segment gives up width first so the pattern/evidence segment stays readable in a narrow overlay.
  const row=rows.find(r=>r.key===selectedKey)||rows.find(r=>r.found)||rows[0],b=barReadout(bars,cursor).bar,ohlc=b?` · O ${legendPrice(b.open)} H ${legendPrice(b.high)} L ${legendPrice(b.low)} C ${legendPrice(b.close)}`:'';
  const seg={fontSize:11,lineHeight:16,color:C.muted,minWidth:0},lead=<T numberOfLines={1} style={[seg,{flexShrink:3}]}><T style={{fontSize:11,fontFamily:'InterSemi',color:C.ink}}>{symbol||'n/a'} · {timeframe||'n/a'}</T>{ohlc}</T>,leadText=`${symbol||'n/a'} ${timeframe||'n/a'}${ohlc.replace(/ · /g,', ')}`;
  if(!row)return <View accessibilityLabel={`Chart legend for ${symbol||'n/a'} ${timeframe||''}. No patterns listed for this chart`} style={[s.row,{minHeight:30,gap:6,paddingHorizontal:6}]}>{lead}<T numberOfLines={1} style={[seg,{flexShrink:1}]}>No patterns listed for this chart</T></View>;
  const ev=row.key===selectedKey&&row.found?evidence:undefined;
  // Q3: the one-liner keeps the hold-period and sample labels; numbers only when the key matches.
  const tail=ev===undefined?'':!ev?' · Evidence n/a':!ev.keyMatches?' · Loading evidence…':` · hold-period avg ${legendPct(ev.avg)} · 95% low ${legendPct(ev.low95)} · n ${finite(ev.n)?ev.n:'n/a'}${ev.sampleLabel?` ${ev.sampleLabel}`:''}`;
  return <Pressable accessibilityRole="button" accessibilityLabel={`Chart legend for ${symbol||'n/a'} ${timeframe||''}. ${leadText}. ${row.name}, ${row.found?'found':'not found on this chart'}${tail.replace(/ · /g,', ')}. Expand legend`} disabled={!onSelect} onPress={()=>onSelect?.(row.key)} style={[s.row,{minHeight:30,gap:6,paddingHorizontal:6,minWidth:0}]}>
   {lead}
   <T numberOfLines={1} style={[seg,{flexShrink:1}]}><T style={{fontSize:11,fontFamily:'InterSemi',color:C.ink}}>{row.name}</T> · {row.found?<T style={{fontSize:11,color:C.green}}>Found</T>:<T style={{fontSize:11,color:C.amber,opacity:.78}}>Not found on this chart</T>}{tail}</T>
  </Pressable>;
 }
 return <View role="group" accessibilityLabel={`Chart legend for ${symbol||'n/a'} ${timeframe||''}`} style={{gap:2,paddingVertical:5,paddingHorizontal:6}}>
  <LegendHeader bars={bars} cursor={cursor} symbol={symbol} timeframe={timeframe} dataEnd={dataEnd}/>
  {rows.length?rows.map(r=>{const selected=r.key===selectedKey;return <View key={r.key}><LegendRow row={r} selected={selected} onToggleVisible={onToggleVisible} onSelect={onSelect} onHide={onHide}/>{selected&&r.found&&<LegendEvidenceBlock evidence={evidence} onOpenEvidence={onOpenEvidence}/>}</View>}):<T style={{fontSize:11,color:C.muted,paddingHorizontal:6}}>No patterns detected or enabled for this chart.</T>}
 </View>;
});
