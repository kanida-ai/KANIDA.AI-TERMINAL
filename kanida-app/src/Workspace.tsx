import React,{useEffect,useRef,useState} from 'react';
import {View,useWindowDimensions} from 'react-native';
import {useProduct} from './context';
import {C,T,s,Button,Chip,Sheet,Loading,Empty,BREAKPOINTS} from './ui';
import {agents} from './agents';
import {useActiveSymbol} from './activeSymbol';
// Explicit /index: on case-insensitive filesystems './workspace' would resolve to this file (Workspace.tsx).
import {ChartCentre,DiscoverPanel,EvidencePanel,SetupSummary,useDiscoverRows} from './workspace/index';
// "/" route until the Phase 0 shell lands: a thin composition of the workspace parts. Desktop = Discover list + chart centre (Setup) or Evidence (Historical replay); phone = list / detail.
export function Workspace(){
 const p=useProduct(),{active}=useActiveSymbol(),{width}=useWindowDimensions(),desktop=width>=BREAKPOINTS.desktop;
 const {rows}=useDiscoverRows(),originId:string=p.studyDraft?.origin?.matchId||'',first=rows[0]?.id||'';
 const sig=active?`${active.symbol}|${active.timeframe}|${active.matchId||''}`:'';
 const [phoneDetail,setPhoneDetail]=useState(!!(sig||p.workspace.selected||originId)),[agentOpen,setAgentOpen]=useState(false),[retrying,setRetrying]=useState(false);
 // A selection made anywhere (Discover row, Prev/Next, evidence link from a stock sheet, URL) opens the phone detail view.
 const seen=useRef(sig);useEffect(()=>{if(sig===seen.current)return;seen.current=sig;if(sig)setPhoneDetail(true)},[sig]);
 // Desktop parity with the old Workspace: with nothing selected, the chart shows the top Discover result.
 useEffect(()=>{if(desktop&&!p.loading&&!active&&!p.workspace.selected&&!originId&&first)p.setWorkspace((w:any)=>({...w,selected:first}))},[desktop,p.loading,active,p.workspace.selected,originId,first]);
 const tab=p.workspace.tab||'setup',setTab=(t:string)=>p.setWorkspace((w:any)=>({...w,tab:t})),openEvidence=()=>setTab('history');
 async function retry(){setRetrying(true);try{await p.refresh()}finally{setRetrying(false)}}
 if(p.loading)return <Loading/>;
 if(p.error&&!p.state)return <View style={{flex:1,padding:desktop?30:16}}><Empty title="Research couldn’t load" detail={p.error} action={<Button label="Retry" icon="rotate-ccw" loading={retrying} onPress={retry}/>}/></View>;
 return <View style={{flex:1,minHeight:0}}>
  {(desktop||!phoneDetail)&&<View style={[s.between,{paddingHorizontal:desktop?22:14,paddingVertical:2,borderBottomWidth:1,borderColor:C.line}]}><Button label="Chart Agent" icon="aperture" kind="ghost" onPress={()=>setAgentOpen(true)}/></View>}
  <View style={{flex:1,minHeight:0,flexDirection:desktop?'row':'column'}}>
   {(desktop||!phoneDetail)&&<View style={{width:desktop?308:'100%',flex:desktop?undefined:1,minHeight:0,borderRightWidth:desktop?1:0,borderColor:C.line,backgroundColor:C.paper}}><DiscoverPanel layout="dock" onSelect={()=>setPhoneDetail(true)}/></View>}
   {(desktop||phoneDetail)&&<View style={{flex:1,minWidth:0,minHeight:0}}>
    <View style={[s.row,{gap:5,paddingHorizontal:12,paddingVertical:6,borderBottomWidth:1,borderColor:C.line}]}>{!desktop&&<Button label="Results" icon="chevron-left" kind="ghost" onPress={()=>setPhoneDetail(false)} style={{paddingHorizontal:0}}/>}<Chip label="Setup" active={tab==='setup'||(desktop&&tab==='summary')} onPress={()=>setTab('setup')}/><Chip label="Historical replay" active={tab==='history'} onPress={openEvidence}/>{!desktop&&<Chip label="Summary" active={tab==='summary'} onPress={()=>setTab('summary')}/>}</View>
    {tab==='history'?<EvidencePanel/>:!desktop&&tab==='summary'?<SetupSummary/>:desktop?<View style={{flex:1,minHeight:0,flexDirection:'row'}}><View style={{flex:1,minWidth:0,minHeight:0}}><ChartCentre onOpenEvidence={openEvidence}/></View><SetupSummary column/></View>:<ChartCentre phone onOpenEvidence={openEvidence}/>}
   </View>}
  </View>
  <Sheet visible={agentOpen} onClose={()=>setAgentOpen(false)} title="Your agents" subtitle="One workspace for every kind of research.">{agents.map(a=><View key={a.id} style={s.card}><T style={{fontSize:22,fontFamily:'ManropeBold'}}>{a.name}</T><T style={{color:C.green}}>{a.audience}</T><T>{a.description}</T><Button label="Open Chart Agent" onPress={()=>setAgentOpen(false)}/></View>)}<T style={s.muted}>Additional Terminal agents will appear here when their signals and historical evidence are integrated.</T></Sheet>
 </View>;
}
