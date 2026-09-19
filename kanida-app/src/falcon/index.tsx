import React,{useEffect,useRef,useState} from 'react';
import {View,ScrollView,Pressable,Platform,useWindowDimensions} from 'react-native';
import {C,T,Icon,Button} from '../ui';
import {useProduct} from '../context';
import {dataAgeDays,formatDataDate} from '../layout';
import {DATA_STALE_DAYS} from '../constants';
import {dateTimeText} from '../model';
import type {Insight,ScanJob} from '../strategies/types';
import {useFalconScan,useReducedMotion,toMs} from './useFalconScan';
import {shownStatus,paceProgress,PaceStatus} from './pacing';
import {FalconMark} from './FalconMark';
import {NeuralNet} from './NeuralNet';
import {StageList} from './StageList';
import {InsightCard} from './InsightCard';
import {ContextStrip} from './ContextStrip';
// Falcon — the AI home at `/` (docs/FALCON_DISCOVER_SPEC.md §2). Fills its container (flex:1); only the results state scrolls. Every number shown comes from the server job.
const web=Platform.OS==='web';
const liveProps=(polite=true):any=>({accessibilityLiveRegion:polite?'polite':'assertive',...(web?{'aria-live':polite?'polite':'assertive'}:{})});
/** "09:12 IST", or "13 Sep 2026 · 09:12 IST" when not today (IST). */
function scanTime(v:number|string|null|undefined){const ms=toMs(v as any);if(!Number.isFinite(ms))return '';const full=dateTimeText(ms);const today=dateTimeText(Date.now()).split(' · ')[0];const [day,time]=full.split(' · ');return day===today?time:full;}
function staleInfo(job:ScanJob|null,stateLatest?:string,stateStale?:boolean){
 const r=job?.result,live=dataAgeDays(stateLatest);const age=live??(r?.age_days??undefined);
 const stale=live!=null?live>DATA_STALE_DAYS||!!stateStale:!!r?.stale||(age!=null&&age>DATA_STALE_DAYS);
 return {age:age??null,stale};
}
function PrimaryButton({label,onPress,icon='zap'}:{label:string;onPress:()=>void;icon?:string}){
 return <Pressable accessibilityRole="button" accessibilityLabel={label} onPress={onPress}
  style={(st:any)=>[{minHeight:60,paddingHorizontal:34,borderRadius:16,flexDirection:'row',alignItems:'center',justifyContent:'center',gap:12,backgroundColor:st.pressed?'#2BC48A':st.hovered?'#5AF0B6':C.green,transform:[{scale:st.pressed?.98:1}]},web&&st.focused?({outlineWidth:3,outlineStyle:'solid',outlineColor:C.mint,outlineOffset:3} as any):null,web?({cursor:'pointer',transitionProperty:'background-color, transform',transitionDuration:'120ms'} as any):null]}>
  <Icon name={icon} size={20} color="#041B12"/><T style={{fontFamily:'ManropeBold',fontSize:18,lineHeight:24,color:'#041B12'}}>{label}</T>
 </Pressable>;
}
function LinkText({label,onPress}:{label:string;onPress:()=>void}){return <Pressable accessibilityRole="button" accessibilityLabel={label} onPress={onPress} hitSlop={8} style={(st:any)=>[{borderRadius:4},web&&st.focused?({outlineWidth:2,outlineStyle:'solid',outlineColor:C.green} as any):null]}>{(st:any)=><T style={{fontSize:13,fontFamily:'InterSemi',color:C.green,textDecorationLine:st.hovered||st.focused?'underline':'none'}}>{label}</T>}</Pressable>;}
export function FalconHome(){
 const p=useProduct(),f=useFalconScan(),reduce=useReducedMotion(),{width}=useWindowDimensions();
 const narrow=width<760;
 if(f.phase==='scanning')return <Scanning f={f} reduce={reduce} narrow={narrow}/>;
 if(f.phase==='error')return <ErrorState message={f.error} onRetry={f.retry} onBack={f.backToIdle}/>;
 if(f.phase==='results'&&f.job?.result)return <Results job={f.job} narrow={narrow} onRescan={f.rescan} state={p?.state}/>;
 return <Idle f={f} state={p?.state}/>;
}
export default FalconHome;
function Idle({f,state}:{f:ReturnType<typeof useFalconScan>;state:any}){
 const {age,stale}=staleInfo(f.latest,state?.source_latest,state?.source_stale);const r=f.latest?.result;
 return <ScrollView style={{flex:1,backgroundColor:C.bg}} contentContainerStyle={{flexGrow:1,alignItems:'center',justifyContent:'center',padding:24,gap:22}}>
  <FalconMark size={104}/>
  <View style={{alignItems:'center',gap:8,maxWidth:560}}>
   <T role="heading" aria-level={1} style={{fontFamily:'ManropeBold',fontSize:30,lineHeight:38,letterSpacing:-.6}}>Falcon</T>
   <T style={{fontSize:16,lineHeight:25,color:C.muted,textAlign:'center'}}>Falcon reads every strategy across NIFTY 500 and tells you what matters today.</T>
  </View>
  {stale&&age!=null&&<View {...liveProps()} style={{flexDirection:'row',alignItems:'center',gap:8,backgroundColor:C.amberBg,borderRadius:10,paddingHorizontal:14,paddingVertical:9,maxWidth:560}}><Icon name="alert-triangle" size={15} color={C.amber}/><T style={{fontSize:13,lineHeight:19,color:C.amber,flexShrink:1}}>Prices are {age} days old — insights will be research only, not current.</T></View>}
  <PrimaryButton label="Scan the market in 30 sec" onPress={f.start}/>
  {!!r&&<View style={{flexDirection:'row',flexWrap:'wrap',alignItems:'center',justifyContent:'center',gap:6}}>
   <T style={{fontSize:13,color:C.muted}}>Last scan {scanTime(r.scanned_at??f.latest?.finished_at)}{r.data_end?` · data to ${formatDataDate(r.data_end)}`:''} ·</T>
   <LinkText label="View results" onPress={f.viewResults}/>
  </View>}
 </ScrollView>;
}
function Scanning({f,reduce,narrow}:{f:ReturnType<typeof useFalconScan>;reduce:boolean;narrow:boolean}){
 const stages=f.job?.stages??[];const statuses:PaceStatus[]=stages.map((_,i)=>shownStatus(f.pace,stages,i));
 const idx=Math.min(f.pace.shown,Math.max(0,stages.length-1));const cur=stages[idx];const allDone=stages.length>0&&f.pace.shown>=stages.length;
 const progress=paceProgress(f.pace,stages,f.now),secs=Math.floor(f.elapsedMs/1000),jobDone=f.job?.status==='done';
 const refs=cur&&statuses[idx]!=='waiting'?(cur.references??[]):[];
 // Live region: announce each revealed stage change once.
 const [announce,setAnnounce]=useState('');const last=useRef('');
 useEffect(()=>{if(!cur)return;const msg=allDone?'All stages done. Opening results.':`Stage ${idx+1} of ${stages.length}: ${cur.title}, ${statuses[idx]==='running'?'running':statuses[idx]}.`;if(msg!==last.current){last.current=msg;setAnnounce(msg);}},[idx,statuses[idx],allDone,cur?.title]);
 const header=<View style={{flexDirection:'row',alignItems:'center',justifyContent:'space-between',gap:12,flexWrap:'wrap'}}>
  <View style={{flexDirection:'row',alignItems:'center',gap:12}}><FalconMark size={40}/><View><T style={{fontFamily:'ManropeBold',fontSize:20,lineHeight:26}}>Falcon is scanning</T><T style={{fontSize:12,color:C.muted}}>{stages.length?`Stage ${Math.min(f.pace.shown+1,stages.length)} of ${stages.length} · ${secs} s`:`Starting · ${secs} s`}</T></View></View>
  <View style={{flexDirection:'row',gap:8}}>
   {f.completedOnce&&<Button kind="outline" icon="fast-forward" label={f.skip&&!jobDone?'Skipping when ready…':'Skip animation'} accessibilityLabel="Skip animation and show results when the scan is done" disabled={f.skip} onPress={f.requestSkip}/>}
   <Button kind="outline" icon="x" label="Cancel" accessibilityLabel="Cancel the scan" onPress={f.cancel}/>
  </View>
 </View>;
 const bar=<View style={{gap:6}}>
  <View accessibilityRole="progressbar" accessibilityValue={{min:0,max:stages.length||7,now:Math.min(f.pace.shown,stages.length)}} {...(web?{'aria-valuetext':`${Math.min(f.pace.shown,stages.length)} of ${stages.length||7} stages shown`}:{})} style={{height:6,borderRadius:3,backgroundColor:C.line,overflow:'hidden'}}>
   <View style={{height:6,width:`${Math.round(progress*100)}%`,backgroundColor:C.green,borderRadius:3}}/>
  </View>
  {jobDone&&!allDone&&!f.skip&&<T style={{fontSize:11,color:C.muted}}>The server has finished; showing each stage before the results.</T>}
 </View>;
 const list=<View style={{gap:10}}>
  {!stages.length?<T style={{fontSize:13,color:C.muted}}>Waiting for the server to report the first stage…</T>:<StageList stages={stages} statuses={statuses} reduce={reduce}/>}
 </View>;
 return <View style={{flex:1,backgroundColor:C.bg}}>
  <T {...liveProps()} style={{position:'absolute',width:1,height:1,overflow:'hidden',opacity:0}}>{announce}</T>
  <ScrollView style={{flex:1}} contentContainerStyle={{padding:narrow?16:24,gap:18,flexGrow:1}}>
   {header}{bar}
   {reduce?<View style={{maxWidth:760,width:'100%',alignSelf:'center'}}>{list}</View>
    :<View style={{flexDirection:narrow?'column':'row',gap:20,flex:narrow?undefined:1}}>
     <View style={{flex:narrow?undefined:1.4,minHeight:narrow?220:320,justifyContent:'center',borderRadius:16,borderWidth:1,borderColor:C.line,backgroundColor:C.dark,padding:8}} {...({'aria-hidden':true} as any)} accessibilityElementsHidden importantForAccessibility="no-hide-descendants">
      <NeuralNet active={allDone?stages.length:f.pace.shown} done={allDone} references={refs} height={narrow?220:340}/>
     </View>
     <View style={{flex:narrow?undefined:1,minWidth:narrow?undefined:320}}>{list}</View>
    </View>}
  </ScrollView>
 </View>;
}
function ErrorState({message,onRetry,onBack}:{message:string;onRetry:()=>void;onBack:()=>void}){
 return <View style={{flex:1,backgroundColor:C.bg,alignItems:'center',justifyContent:'center',padding:24,gap:16}}>
  <FalconMark size={72} color={C.amber}/>
  <T role="heading" aria-level={1} style={{fontFamily:'ManropeBold',fontSize:22}}>The scan did not finish</T>
  <T accessibilityRole="alert" style={{fontSize:14,lineHeight:22,color:C.muted,textAlign:'center',maxWidth:520}}>{message||'Something went wrong on the server.'} No insights are shown from an unfinished scan.</T>
  <View style={{flexDirection:'row',gap:10}}><Button label="Retry" icon="refresh-cw" onPress={onRetry}/><Button kind="outline" label="Back" onPress={onBack}/></View>
 </View>;
}
function Column({title,subtitle,audience,items,staleDays}:{title:string;subtitle:string;audience:string;items:Insight[];staleDays:number|null}){
 const shown=items.slice(0,5);
 return <View style={{flex:1,gap:12,minWidth:0}}>
  <View style={{gap:2}}><T role="heading" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:19,lineHeight:26}}>{title}</T><T style={{fontSize:12,color:C.muted}}>{subtitle}</T></View>
  {!shown.length?<View style={{padding:16,borderRadius:14,borderWidth:1,borderColor:C.line,borderStyle:'dashed'}}><T style={{fontSize:13,lineHeight:20,color:C.muted}}>Nothing reliable to report for {audience} on this data.</T></View>
   :<>{shown.length<5&&<T style={{fontSize:12,color:C.muted}}>Only {shown.length} insight{shown.length===1?'':'s'} for {audience} today</T>}{shown.map((it,i)=><InsightCard key={`${it.rank}-${i}`} insight={it} staleDays={staleDays}/>)}</>}
 </View>;
}
function Results({job,narrow,onRescan,state}:{job:ScanJob;narrow:boolean;onRescan:()=>void;state:any}){
 const r=job.result!;const staleDays=r.stale?(r.age_days??dataAgeDays(r.data_end??undefined)??null):null;
 return <ScrollView style={{flex:1,backgroundColor:C.bg}} contentContainerStyle={{padding:narrow?16:24,gap:18,paddingBottom:40}}>
  <View style={{flexDirection:'row',alignItems:'center',justifyContent:'space-between',gap:12,flexWrap:'wrap'}}>
   <View style={{flexDirection:'row',alignItems:'center',gap:12}}><FalconMark size={40}/><View><T role="heading" aria-level={1} style={{fontFamily:'ManropeBold',fontSize:22,lineHeight:28}}>What matters today</T><T style={{fontSize:12,color:C.muted}}>Scanned {scanTime(r.scanned_at??job.finished_at)}{r.data_end?` · data to ${formatDataDate(r.data_end)}`:''}</T></View></View>
   <Button kind="outline" icon="refresh-cw" label="Rescan" accessibilityLabel="Rescan the market" onPress={onRescan}/>
  </View>
  {staleDays!=null&&<View style={{flexDirection:'row',alignItems:'center',gap:8,backgroundColor:C.amberBg,borderRadius:10,paddingHorizontal:14,paddingVertical:9}}><Icon name="alert-triangle" size={15} color={C.amber}/><T style={{fontSize:13,lineHeight:19,color:C.amber,flexShrink:1}}>Prices are {staleDays} days old — these insights are research only, not current.</T></View>}
  <ContextStrip context={r.context}/>
  <View style={{flexDirection:narrow?'column':'row',gap:narrow?28:20,alignItems:'flex-start'}}>
   <Column title="For traders" subtitle="1H–1D setups" audience="traders" items={r.traders??[]} staleDays={staleDays}/>
   <Column title="For investors" subtitle="1W setups" audience="investors" items={r.investors??[]} staleDays={staleDays}/>
  </View>
 </ScrollView>;
}
