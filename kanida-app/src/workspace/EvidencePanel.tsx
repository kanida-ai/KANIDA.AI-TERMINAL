import React from 'react';
import {View,Pressable} from 'react-native';
import {useProduct} from '../context';
import {C,T,s,Loading,Empty} from '../ui';
import {Match,History} from '../model';
import {StockReplay} from '../ReplayStudio';
import {useDiscoverRows,useWorkspaceMatch} from './DiscoverPanel';
import {matchKey,openSimulate} from './ChartCentre';
// Phase 0 (wave 0B) Evidence/Replay: the old StockWorkspace "Historical replay" tab for the active match. Replay markers still draw inside ReplayStudio's own chart (Phase 1 moves them to the main chart).
export function EvidencePanel(){
 const d=useDiscoverRows(),w=useWorkspaceMatch(d),p=useProduct();
 if(w.kind==='loading')return <Loading/>;
 if(w.kind==='error')return <View style={{flex:1,padding:16}}><Empty title="Research couldn’t load" detail={p.error}/></View>;
 if(w.kind==='empty')return <View style={{flex:1,padding:16}}><Empty icon="bar-chart-2" title="Choose a setup from Discover" detail="Historical replay follows the active setup."/></View>;
 // Q1: stored setups outside the filters are never auto-selected; the chart legend offers them as an explicit pick list.
 if(w.kind==='none'||!w.match)return <View style={{flex:1,padding:16,gap:6}}><T style={{fontFamily:'InterSemi'}}>{w.outsideSetups.length?`No setup for ${w.symbol} within your filters`:`No stored setup for ${w.symbol} on ${w.timeframe}`}</T><T style={{fontSize:12,color:C.muted}}>{w.outsideSetups.length?`${w.outsideSetups.length} stored ${w.outsideSetups.length===1?'setup is':'setups are'} outside your filters. Pick one from the chart legend to replay its history.`:'There is no historical replay for this stock and timeframe. Pick a Discover result to replay its history.'}</T></View>;
 // Keyed by the match's evidence identity: a new symbol/pattern/timeframe/side/run/data end remounts StockReplay, which starts with no result (Loading), never the previous stock's replay.
 return <EvidenceBody key={matchKey(w.match,w.history)} match={w.match} history={w.history}/>;
}
function EvidenceBody({match,history:h}:{match:Match;history?:History}){
 const p=useProduct();
 return <View style={{flex:1,minHeight:0,paddingHorizontal:12}}>
  <View style={[s.between,{paddingVertical:6,gap:8,flexWrap:'wrap'}]}><T numberOfLines={1} style={{fontSize:13,flexShrink:1}}><T style={{fontFamily:'InterSemi',fontSize:13}}>{match.symbol}</T> · {match.timeframe} · {match.pattern_name} · historical replay</T><Pressable accessibilityRole="link" accessibilityLabel={`Test these rules in Simulate for ${match.symbol}`} onPress={()=>openSimulate(p,match,h?.side)} style={{minHeight:32,justifyContent:'center'}}><T style={{fontSize:12,color:C.green}}>Test these rules in Simulate →</T></Pressable></View>
  <StockReplay match={match} onTest={()=>openSimulate(p,match,h?.side)}/>
 </View>;
}
