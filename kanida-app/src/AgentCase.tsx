import React,{useEffect,useState} from 'react';
import {View,useWindowDimensions} from 'react-native';
import {C,T,s,Icon,Button,Badge,Stat} from './ui';
import {PatternCanvas} from './PatternCanvas';
import {useExitPlan,ExitBrief,prepareBlock} from './ExitPlan';
import {Match,api,cellQuery,money,pct,dateText} from './model';
import {decision,dataAgeDays,isStale} from './decision';
import {useProduct} from './context';
// One source of truth for a case's verdict, history side, exit-rule request and Prepare gate. Used by AgentCase and the sidebar EvidenceSummaryWidget.
// History choice: the requested side, else the verdict's history (histories(match,filters)[0] || match.history[0], the same pick as Discover).
export function useCaseEvidence(match:Match,side?:string){
 const {filters,state}=useProduct();
 const d=decision(match,filters,side);const history=match.history.find(h=>h.side===(side||d.history?.side))||d.history;const stats=history?.reference;
 const exit=useExitPlan(match,history?.side||'long');
 // Same gate and reason text as Discover: fresh prices AND tested evidence for the exact exit rule.
 const age=dataAgeDays(state?.source_latest),stale=isStale(age,state?.source_stale),blocked=prepareBlock({stale,age,match,history,exit});
 return {d,history,stats,exit,age,stale,blocked};
}
export function AgentCase({match,side,heading=true,onHistory}: {match:Match;side?:string;heading?:boolean;onHistory?:()=>void}){
 const {setDetail,setPlan,product,refreshProduct,setToast}=useProduct();const {width}=useWindowDimensions();
 const {d,history,stats,exit,blocked}=useCaseEvidence(match,side);
 const [snapshot,setSnapshot]=useState<any>(null),[study,setStudy]=useState<any>(null),[saving,setSaving]=useState(false);
 useEffect(()=>{let active=true;setStudy(null);if(history)api('/api/backtests/cell?'+cellQuery(match,history.side)).then(v=>{if(active&&v.run_id===history.run)setStudy(v)}).catch(()=>{});return()=>{active=false}},[match.id,history?.side,history?.run]);
 useEffect(()=>setSnapshot(null),[match.id,match.candle_end]);
 const found=snapshot?.matches?.find((m:any)=>m.pattern===match.pattern);const boundaries=(found?.lines||[]).filter((l:any)=>l.role==='boundary');
 const watching=product.watchlist?.some((w:any)=>w.id===match.id);
 async function watch(){setSaving(true);try{await api('/api/product/watch',{action:watching?'remove':'add',match_id:match.id});await refreshProduct();setToast(watching?'Removed from your watchlist.':'Added to your watch. I’ll compare it with refreshed scans while KANIDA is open.');}catch(e:any){setToast(e.message)}finally{setSaving(false)}}
 return <View style={{gap:18}}>
  {heading&&<><View style={s.between}><View style={{gap:4}}><T style={{fontFamily:'ManropeBold',fontSize:27,lineHeight:35}}>{match.symbol}</T><T style={{fontSize:11,color:C.muted}}>{match.company} · NSE</T></View><Badge label={d.title.toUpperCase()} tone={d.verdict==='review'?'green':d.verdict==='pass'?'red':'amber'}/></View><View style={s.between}><T style={{fontSize:13,fontFamily:'InterMedium'}}>{match.pattern_name}</T><Badge label={match.timeframe} tone="neutral"/></View><View style={s.between}><T style={{fontFamily:'ManropeBold',fontSize:26,lineHeight:33}}>{money(match.price)}</T><T style={{fontSize:10,color:C.muted}}>Snapshot · {dateText(match.candle_end)}</T></View></>}
  <PatternCanvas match={match} side={history?.side} onSnapshot={setSnapshot} exitPlan={exit.data}/>
  <ExitBrief {...exit}/>
  <View style={{borderLeftWidth:2,borderColor:d.verdict==='pass'?C.red:C.green,paddingLeft:15,gap:7}}><View style={s.row}><Icon name="aperture" size={15} color={C.green}/><T style={{fontFamily:'InterSemi',fontSize:13}}>{d.verdict==='pass'?"Why I’m passing":d.verdict==='watch'?"Why I’m waiting":"Why this made the list"}</T></View><T style={{fontSize:13,color:C.muted,lineHeight:22}}>{d.why} {d.caution}</T>{found?.evidence?.slice(0,2).map((e:string)=><T key={e} style={{fontSize:11,color:C.muted}}>· {e}</T>)}</View>
  <View style={[s.card,{padding:17,gap:14}]}><View style={s.between}><T style={{fontFamily:'InterSemi',fontSize:12}}>Time-exit history</T><T style={{fontSize:10,color:C.muted}}>{history?.side==='short'?'SHORT':'LONG'} · {stats?.n||0} trades</T></View><T accessibilityRole="text" style={{fontSize:11,fontFamily:'InterMedium',color:C.amber,marginTop:-6}}>{exit.history_label}. These stats come from a fixed holding-period exit, not the stop, target and hold in Your exit plan above.</T><View style={s.row}><Stat label="Avg. net / trade" value={pct(stats?.display_return_pct)} size={23} color={(stats?.expectancy_pct||0)>=0?C.green:C.red}/><Stat label="Trades that won" value={stats?.win_rate==null?'—':`${stats.win_rate.toFixed(0)}%`} size={23}/></View><View style={[s.between,{borderTopWidth:1,borderColor:C.line,paddingTop:12}]}><View style={[s.row,{flex:1}]}><Icon name="clock" size={13} color={C.muted}/><T style={{fontSize:11,color:C.muted}}>Tested hold</T></View><T style={{fontSize:11}}>{stats?.holding?.duration||'Unavailable'}</T></View>{!!stats?.expectancy_ci95&&<T style={{fontSize:10,color:C.muted}}>95% range for this result alone: {pct(stats.expectancy_ci95[0])} to {pct(stats.expectancy_ci95[1])}. It does not correct for comparing many stocks and patterns; top-ranked averages usually shrink.</T>}<T style={{fontSize:10,color:C.muted}}>After assumed costs. May carry overnight. Past average, not a forecast.</T></View>
  {study?.reference?.n>0&&<View style={{gap:10}}><T style={{fontFamily:'InterMedium',fontSize:12}}>What happened in past trades</T><View style={s.row}><View style={{flex:1,backgroundColor:C.soft,borderRadius:9,padding:13,gap:5}}><T style={{fontSize:10,color:C.green}}>When it won</T><T style={{fontFamily:'ManropeBold',fontSize:19,color:C.green}}>{pct(study.reference.avg_win_pct)}</T><T style={{fontSize:10,color:C.muted}}>Average of {study.reference.wins} winners</T></View><View style={{flex:1,backgroundColor:'#26181E',borderRadius:9,padding:13,gap:5}}><T style={{fontSize:10,color:C.red}}>When it lost</T><T style={{fontFamily:'ManropeBold',fontSize:19,color:C.red}}>{pct(study.reference.avg_loss_pct)}</T><T style={{fontSize:10,color:C.muted}}>Average of {study.reference.losses} losers</T></View></View></View>}
  <View style={[s.card,{backgroundColor:'#0A1D20',borderColor:'#20423F',padding:17}]}><View style={s.row}><Icon name="eye" color={C.green} size={16}/><T style={{fontFamily:'InterSemi',fontSize:13}}>What I need to see next</T></View><T style={{fontSize:12,color:C.muted,lineHeight:21}}>{d.next}</T>{boundaries.length>0&&<View style={[s.row,{flexWrap:'wrap'}]}>{boundaries.slice(0,2).map((line:any)=><View key={line.label} style={{flex:1,minWidth:100,gap:4}}><T style={{fontSize:10,color:C.muted}}>{line.label}</T><T style={{fontFamily:'InterSemi',fontSize:16}}>{money(line.points[line.points.length-1]?.value)}</T></View>)}</View>}<T style={{fontSize:10,color:C.muted}}>Levels belong to the stored pattern. They are not live entry, stop or target prices.</T></View>
  <View style={s.row}><Button label={watching?'Watching':'Add to watch'} icon={watching?'check':'eye'} kind="outline" loading={saving} onPress={watch} style={{flex:1}}/><Button label="See evidence" kind="soft" icon="bar-chart-2" style={{flex:1}} onPress={onHistory||(()=>setDetail(match))}/></View>
  {heading&&<><Button label="Prepare AutoTrade" icon="zap" disabled={!!blocked} accessibilityLabel={blocked?`Prepare AutoTrade unavailable: ${blocked}`:'Prepare AutoTrade'} onPress={()=>setPlan({match,side:history?.side})}/>{!!blocked&&<T accessibilityRole="alert" style={{fontSize:11,lineHeight:16,color:C.amber}}>Prepare AutoTrade unavailable: {blocked}</T>}</>}
 </View>;
}
