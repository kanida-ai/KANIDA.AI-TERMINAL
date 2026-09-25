// K12 Deployment monitor (GTM audit P20): one paper deployment on its own durable route - mode, revision, what is
// held versus what the plan intended (residual risk first), marks with their basis, and the last good valuation with
// its age when current marks fail (never shown as current, never zero). Paper only; live monitoring is not claimed.
import React,{useCallback,useEffect,useState} from 'react';
import {View,useWindowDimensions} from 'react-native';
import {router} from 'expo-router';
import {Badge,Button,C,T,s} from '../ui';
import {exec,type Deployment} from './api';
import {DeploymentCard,OrderReview} from './OrderReview';
import {istStamp,num,signed} from './format';
import {LoadState,Scrollable} from './States';

export function DeploymentMonitor({id}:{id:string}){
 const narrow=useWindowDimensions().width<700;
 const [d,setD]=useState<Deployment|null>(null);const [err,setErr]=useState('');const [closing,setClosing]=useState(false);
 const load=useCallback(()=>{exec.get(id).then(x=>{setD(x);setErr('');}).catch(e=>setErr(e.message));},[id]);
 useEffect(()=>{load();const t=setInterval(load,10000);return()=>clearInterval(t);},[load]);
 const x:any=d;
 return <View style={{padding:narrow?14:24,gap:16,maxWidth:1000,width:'100%',alignSelf:'center'}}>
  <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
   <Button label="My Strategies" icon="chevron-left" kind="outline" onPress={()=>router.replace('/strategies' as any)}/>
   {d&&<Button label="Open the strategy" kind="outline" onPress={()=>router.push({pathname:'/strategies',params:{id:d.strategy_id}} as any)}/>}
   {d&&<Button label="Alerts" icon="bell" kind="outline" onPress={()=>router.push({pathname:'/strategies',params:{view:'alerts'}} as any)}/>}
   {d&&d.status==='active'&&<Button label="Adjust" icon="sliders" kind="outline" onPress={()=>router.push({pathname:'/strategies',params:{id:d.strategy_id,open:'adjust'}} as any)}/>}
  </View>
  <View style={{gap:4}}><T style={{fontFamily:'ManropeBold',fontSize:22}}>Paper deployment</T>
   <T style={{fontSize:12,color:C.muted}}>Simulated fills against live quotes. No order reaches a broker; live monitoring is not part of this release.</T></View>
  <LoadState loading={!d&&!err} error={err} onRetry={load} what="The deployment">
   {d&&<>
    <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><Badge label={`PAPER · ${d.status.replace('_',' ').toUpperCase()}`} tone={d.status==='active'?'green':d.status==='attention_required'?'red':'neutral'}/>
     {d.revision&&<T style={{fontSize:12,color:C.muted}}>{`Version #${d.revision.n} "${d.revision.name}"`}</T>}</View>
    {x.exposure_mismatch&&<View accessibilityRole="alert" style={{backgroundColor:'#2A1519',borderRadius:12,padding:12,gap:6}}>
     <T style={{color:C.red,fontFamily:'InterSemi'}}>What is held is not what the plan intended</T>
     <T style={{color:C.red,fontSize:12}}>A partly filled structure carries a different risk - for example a hedge bought without its short, or a short without its hedge. Complete, cancel the resting orders, or close.</T>
     <Scrollable narrow={narrow} min={420}><View>{[['Leg','Planned units','Held units','Residual'],...x.exposure.map((e:any)=>[e.label||e.leg_id,String(e.planned??'—'),String(e.held),e.residual==null?'—':String(e.residual)])]
      .map((r:string[],i:number)=><View key={i} style={[s.row,{gap:6,paddingVertical:3,borderTopWidth:i?1:0,borderColor:'#5A2A30'}]}>{r.map((c,j)=><T key={j} style={{flex:j?1:1.4,fontSize:i?12:10,color:i&&j===3&&c!=='0'?C.red:C.ink,textAlign:j?'right':'left',fontVariant:['tabular-nums'] as any}}>{c}</T>)}</View>)}</View></Scrollable></View>}
    {d.net==null&&x.last_known&&<View accessibilityRole="alert" style={{backgroundColor:C.amberBg,borderRadius:12,padding:12,gap:4}}>
     <T style={{color:C.amber,fontFamily:'InterSemi'}}>Current marks are unavailable</T>
     <T style={{color:C.amber,fontSize:12}}>{`Last good valuation: net ${signed(x.last_known.net)} (unrealised ${signed(x.last_known.unrealised)}), marked ${istStamp(x.last_known.marked_at)} - ${Math.round(x.last_known.age_seconds/60)} min ago. This is not a current value.`}</T></View>}
    {d.net==null&&!x.last_known&&<T style={{color:C.amber,fontSize:12}}>No current mark and no earlier valuation - net P&L is unavailable, not zero.</T>}
    <DeploymentCard d={d} onChanged={load} onClose={()=>setClosing(true)} onAdjust={()=>router.push({pathname:'/strategies',params:{id:d.strategy_id,open:'adjust'}} as any)}/>
    <T style={{fontSize:11,color:C.muted}}>{`Realised ${signed(d.realised)} · unrealised ${d.unrealised==null?'unavailable':signed(d.unrealised)} · fees ${num(d.fees)} · marked ${istStamp(d.marked_at)}. Refreshes every 10 s.`}</T>
    {closing&&<OrderReview visible strategyId={d.strategy_id} deployment={d} onClose={()=>setClosing(false)} onPlaced={()=>{setClosing(false);load();}}/>}
   </>}
  </LoadState>
 </View>;
}
