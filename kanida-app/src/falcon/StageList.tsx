import React from 'react';
import {View,ActivityIndicator} from 'react-native';
import {C,T,Icon} from '../ui';
import type {ScanStage} from '../strategies/types';
import type {PaceStatus} from './pacing';
// Stage list: status icon + title + the server's real reference lines. Lines appear only once a stage is revealed; an unavailable stage says so instead of showing numbers.
const WORD:Record<PaceStatus,string>={waiting:'waiting',running:'running',done:'done',unavailable:'not available',failed:'failed'};
function StatusIcon({status,reduce}:{status:PaceStatus;reduce:boolean}){
 if(status==='running')return reduce?<Icon name="loader" size={16} color={C.green}/>:<ActivityIndicator size="small" color={C.green} style={{width:16,height:16}}/>;
 const [name,color]=status==='done'?['check-circle',C.green]:status==='unavailable'?['minus-circle',C.muted]:status==='failed'?['x-circle',C.red]:['circle',C.line];
 return <Icon name={name} size={16} color={color}/>;
}
export function StageList({stages,statuses,reduce}:{stages:ScanStage[];statuses:PaceStatus[];reduce:boolean}){
 return <View accessibilityRole="list" style={{gap:4}}>
  {stages.map((st,i)=>{const status=statuses[i]??'waiting',open=status!=='waiting',on=status==='running';
   return <View key={st.key} accessibilityRole={'listitem' as any} accessibilityLabel={`Stage ${i+1} of ${stages.length}: ${st.title}, ${WORD[status]}${open&&st.lines.length?'. '+st.lines.join('. '):''}`} style={{flexDirection:'row',gap:12,paddingVertical:9,paddingHorizontal:12,borderRadius:10,backgroundColor:on?C.soft:'transparent',borderWidth:1,borderColor:on?'#1D4A3D':'transparent'}}>
    <View style={{paddingTop:3,width:16,alignItems:'center'}}><StatusIcon status={status} reduce={reduce}/></View>
    <View style={{flex:1,gap:3}}>
     <View style={{flexDirection:'row',alignItems:'baseline',gap:8,flexWrap:'wrap'}}>
      <T style={{fontSize:14,fontFamily:open?'InterSemi':'Inter',color:open?C.ink:C.muted}}>{st.title}</T>
      {status==='unavailable'&&<T style={{fontSize:11,color:C.muted}}>not available</T>}
      {status==='failed'&&<T style={{fontSize:11,color:C.red}}>failed</T>}
     </View>
     {open&&st.lines.map((line,j)=><T key={j} style={{fontSize:12,lineHeight:18,color:status==='done'?C.muted:C.mint}}>{line}</T>)}
     {status==='unavailable'&&!st.lines.length&&<T style={{fontSize:12,lineHeight:18,color:C.muted}}>No data for this stage — nothing is shown in its place.</T>}
    </View>
   </View>;})}
 </View>;
}
