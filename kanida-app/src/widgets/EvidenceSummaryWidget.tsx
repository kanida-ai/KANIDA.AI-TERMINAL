import React,{useRef} from 'react';
import {View,Pressable} from 'react-native';
import {useProduct} from '../context';
import {C,T,s,Button,Badge,Stat} from '../ui';
import {Match,pct} from '../model';
import {decision,sampleSize,dataAgeText} from '../decision';
import {useActiveSymbol,evidenceKey,type ActiveSymbol} from '../activeSymbol';
import {evidenceFromWorkspace} from '../ChartLegend';
import {ExitBrief,prepareBlock,PRICE_ADJUSTMENT_NOTE} from '../ExitPlan';
import {useCaseEvidence} from '../AgentCase';
import {Widget,WidgetError,type SidebarWidget} from '../layout';
import {matchKey,useDiscoverRows} from '../workspace/index';
// Sidebar Evidence summary (TRENDSPIDER_STUDY §10.1): a compact AgentCase + ExitBrief for the ACTIVE match only.
// Every number is the stored scanner history (hold-period `reference`) or the server exit-plan response; nothing is computed or back-filled here.
// Key guard: numbers render only when evidenceFromWorkspace() confirms the history, the active symbol/timeframe/match and the exit side all belong to this key AND the exit request for this evidenceKey has settled. Otherwise "Loading evidence…".
export const EVIDENCE_WIDGET_KEY='evidence',EVIDENCE_WIDGET_TITLE='Evidence summary';
export type EvidenceSummaryWidgetProps={onOpenEvidence?:()=>void;onPrepare?:()=>void;framed?:boolean};
/** Sidebar descriptor. Memoise the result in the shell: Sidebar re-renders a widget only when its descriptor object changes. */
export function evidenceSummaryWidget(opts:{onOpenEvidence?:()=>void;onPrepare?:()=>void;onClose?:()=>void;onMaximize?:()=>void}={}):SidebarWidget{return {key:EVIDENCE_WIDGET_KEY,title:EVIDENCE_WIDGET_TITLE,menu:opts.onOpenEvidence?[{label:'Show evidence',icon:'bar-chart-2',onPress:opts.onOpenEvidence}]:undefined,onClose:opts.onClose,onMaximize:opts.onMaximize,render:()=><EvidenceSummaryWidget onOpenEvidence={opts.onOpenEvidence} onPrepare={opts.onPrepare}/>};}
/** Body only by default (Sidebar supplies the Widget card). framed wraps it in its own Widget card for use outside Sidebar. */
export function EvidenceSummaryWidget({onOpenEvidence,onPrepare,framed=false}:EvidenceSummaryWidgetProps){
 const body=<EvidenceSummaryBody onOpenEvidence={onOpenEvidence} onPrepare={onPrepare}/>;
 return framed?<Widget title={EVIDENCE_WIDGET_TITLE}>{body}</Widget>:body;
}
const muted={fontSize:12,lineHeight:18,color:C.muted};
const loadingLine=<T accessibilityLiveRegion="polite" style={muted}>Loading evidence…</T>;
function EvidenceSummaryBody({onOpenEvidence,onPrepare}:Omit<EvidenceSummaryWidgetProps,'framed'>){
 const p=useProduct(),{active,setActive}=useActiveSymbol(),listed=useDiscoverRows().rows;
 if(!active)return <T style={muted}>Pick a setup to see its evidence</T>;
 const match:Match|undefined=active.matchId?(p.matches as Match[]).find(m=>m.id===active.matchId):undefined;
 // Q6: remount per full evidence identity matchKey(match, history) = (symbol, pattern, TF, side, run, candle_end), so no per-case state (exit request, error guard) survives a switch of any part.
 // The history is chosen exactly as useCaseEvidence chooses it inside EvidenceCase.
 if(match&&match.symbol===active.symbol&&match.timeframe===active.timeframe){const dd=decision(match,p.filters,active.side),h=match.history.find(x=>x.side===(active.side||dd.history?.side))||dd.history;return <EvidenceCase key={matchKey(match,h)} match={match} active={active} onOpenEvidence={onOpenEvidence} onPrepare={onPrepare}/>}
 if(p.loading)return loadingLine;
 if(p.error&&!p.state)return <WidgetError title="Could not load evidence" message={p.error} onRetry={p.refresh}/>;
 // Symbol-only (search, URL, a watch setup missing from the scan): no pick here. The chart writes back its auto-selected setup only when one exists within the current filters (Q1); otherwise stored setups for this
 // symbol × timeframe are offered in stored-scan order, each one outside the current Discover filters marked so; nothing is ranked or inferred here.
 const stored=(p.matches as Match[]).filter(m=>m.symbol===active.symbol&&m.timeframe===active.timeframe).slice(0,4);
 return <View style={{gap:8}}>
  <T style={{fontFamily:'InterMedium',fontSize:13}}>No stored setup selected for {active.symbol}</T>
  <T style={muted}>{active.matchId?'The selected setup is not in the latest stored scan, so no evidence is shown.':`Evidence belongs to one stored setup (pattern, side, exit rule and data end), not to the ${active.timeframe} chart as a whole.`}</T>
  {stored.length?<View style={{gap:4}}><T style={{fontSize:11,color:C.muted}}>Stored setups for {active.symbol} · {active.timeframe}</T>{stored.map(m=>{const out=!listed.includes(m);return <Pressable key={m.id} accessibilityRole="button" accessibilityLabel={`Select ${m.pattern_name} ${m.timeframe} for ${m.symbol}${out?', outside your filters':''}`} onPress={()=>setActive({symbol:m.symbol,timeframe:m.timeframe,matchId:m.id,source:active.source})} style={({pressed})=>({minHeight:36,justifyContent:'center',paddingHorizontal:8,borderRadius:8,borderWidth:1,borderColor:C.line,backgroundColor:pressed?C.paper:'transparent'})}><T numberOfLines={1} style={{fontSize:12}}>{m.pattern_name} · <T style={{fontSize:12,color:C.muted}}>{decision(m,p.filters).title}</T>{out&&<T style={{fontSize:11,color:C.amber}}> · outside your filters</T>}</T></Pressable>})}</View>:<T style={muted}>No stored setups for {active.symbol} on {active.timeframe}.</T>}
 </View>;
}
function EvidenceCase({match,active,onOpenEvidence,onPrepare}:{match:Match;active:ActiveSymbol;onOpenEvidence?:()=>void;onPrepare?:()=>void}){
 const p=useProduct(),{d,history,exit,age,stale}=useCaseEvidence(match,active.side);
 // (symbol, pattern, timeframe, side, rule=run, data_end). '' when any part is missing → no evidence. useExitPlan's data is keyed by (match id = SYMBOL:TF:pattern, side, run, candle_end): the same identity.
 const key=evidenceKey({symbol:match.symbol,pattern:match.pattern,timeframe:match.timeframe,side:history?.side,rule:history?.run,dataEnd:match.candle_end});
 // useExitPlan clears its error one effect AFTER a key change, so an error string only counts once it has been seen changing under THIS key.
 const seen=useRef({key:'',error:''});if(exit.error!==seen.current.error)seen.current={key,error:exit.error};
 const exitError=seen.current.key===key?exit.error:'',guarded={data:exit.data,error:exitError};
 const ev=key?evidenceFromWorkspace({match,history,exit:guarded,legendKey:{symbol:active.symbol,timeframe:active.timeframe,selectedKey:active.matchId},age,stale}):null;
 // Exactly Discover's gate and reason text (fresh prices + tested evidence for the exact exit rule), fed the key-guarded exit state.
 const blocked=prepareBlock({stale,age,match,history,exit:guarded}),ready=!!ev?.keyMatches;
 function prepare(){if(blocked)return;p.setPlan({match,side:history?.side});onPrepare?.()}
 const tone=d.verdict==='review'?'green':d.verdict==='pass'?'red':'amber',short=history?.side==='short',btn={flex:1,minHeight:38,paddingVertical:6,paddingHorizontal:8};
 return <View style={{gap:10}}>
  <View style={{gap:3}}><View style={[s.row,{gap:6}]}><T numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:15,flexShrink:1}}>{match.symbol}</T><View style={{flex:1}}/>{ready&&<Badge label={d.title.toUpperCase()} tone={tone}/>}</View><T numberOfLines={2} style={{fontSize:12,lineHeight:17,color:C.muted}}>{match.pattern_name} · {match.timeframe}{history?` · ${short?'Short':'Long'}`:''}</T></View>
  {!key?<T style={muted}>No historical evidence for this setup.</T>:!ready?loadingLine:<View role="group" accessibilityLabel={`Evidence summary for ${match.symbol} ${match.pattern_name} ${match.timeframe}`} style={{gap:8}}>
   <T style={{fontSize:11,lineHeight:16,fontFamily:'InterMedium',color:C.amber}}>{ev!.historyLabel}</T>
   <View style={[s.row,{gap:10,alignItems:'flex-start'}]}><Stat label="Hold-period avg · not this exit rule" value={pct(ev!.avg)} size={18} color={(history?.reference?.expectancy_pct||0)>0?C.green:C.ink}/><Stat label="95% range · this result alone" value={ev!.low95!=null&&ev!.high95!=null?`${pct(ev!.low95)} to ${pct(ev!.high95)}`:'—'} size={13}/></View>
   <View style={[s.row,{flexWrap:'wrap',gap:4}]}>
    <Badge label={`N ${ev!.n??'n/a'}`} tone="neutral"/><Badge label={(ev!.sampleLabel||'Sample n/a').toUpperCase()} tone={sampleSize(ev!.n).tone}/>
    <Badge label={ev!.costPct!=null?`COSTS ${ev!.costPct.toFixed(2)}% INCL.${ev!.costAssumed?' (ASSUMED)':''}`:'COSTS N/A'} tone={ev!.costPct!=null&&!ev!.costAssumed?'neutral':'amber'}/><Badge label={ev!.nextOpen?'NEXT-OPEN ENTRY':'ENTRY BASIS N/A'} tone={ev!.nextOpen?'neutral':'amber'}/>
    <Badge label={`${dataAgeText(age).toUpperCase()}${stale?' · STALE':''}`} tone={stale?'amber':'neutral'}/>{short&&<Badge label="HYPOTHETICAL SHORT STUDY" tone="amber"/>}
   </View>
   <T style={{fontSize:10,lineHeight:15,color:C.muted}}>{short?'Hypothetical short price study':'After assumed costs'} · fixed holding-period exit, not the exit rule below. Picked from many stock × pattern results: top averages are partly luck and usually shrink, and the range does not correct for that.</T>
   {exitError?<View style={{gap:6}}><T accessibilityRole="alert" style={{fontSize:11,lineHeight:16,color:C.amber}}>Exit-rule evidence could not load. {exitError}</T><Button label="Retry exit evidence" kind="outline" onPress={exit.retry} style={{minHeight:36,paddingVertical:4}}/><T style={{fontSize:10,color:C.muted}}>{PRICE_ADJUSTMENT_NOTE}</T></View>:<ExitBrief data={exit.data} error="" retry={exit.retry} compact/>}
  </View>}
  <View style={[s.row,{gap:6}]}><Button label="Show evidence" icon="bar-chart-2" kind="soft" onPress={onOpenEvidence||(()=>p.setDetail(match))} style={btn}/><Button label="Prepare trade" icon="zap" disabled={!!blocked} accessibilityLabel={blocked?`Prepare trade unavailable: ${blocked}`:'Prepare trade'} onPress={prepare} style={btn}/></View>
  {!!blocked&&<T accessibilityRole="alert" style={{fontSize:11,lineHeight:16,color:C.amber}}>Prepare trade unavailable: {blocked}</T>}
 </View>;
}
