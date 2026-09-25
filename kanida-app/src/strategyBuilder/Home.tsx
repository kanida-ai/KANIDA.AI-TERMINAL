// K01 My Strategies + K02 Start. One library; Research / Paper / Archived are derived views over the same strategies.
import React,{useCallback,useEffect,useMemo,useState} from 'react';
import {Pressable,TextInput,View,useWindowDimensions} from 'react-native';
import {router} from 'expo-router';
import {Badge,Button,C,Chip,Empty,Icon,Loading,T,s} from '../ui';
import {sb,type LibraryRow} from './api';
import {dayMonth,istEpoch} from './format';
import {useAlertNotifications} from './Alerts';

type Filter='research'|'paper'|'archived';

export function Home(){
 const {width}=useWindowDimensions();const wide=width>=900;
 const bell=useAlertNotifications();
 const [rows,setRows]=useState<LibraryRow[]|null>(null);const [error,setError]=useState('');const [filter,setFilter]=useState<Filter>('research');const [q,setQ]=useState('');const [busy,setBusy]=useState('');
 const load=useCallback(()=>sb.list().then(r=>setRows(r.strategies)).catch(e=>setError(e.message)),[]);
 useEffect(()=>{load();},[load]);
 const shown=useMemo(()=>(rows||[]).filter(r=>filter==='archived'?!!r.archived_at:!r.archived_at&&(filter==='paper'?r.paper_total>0:true))
  .filter(r=>!q||`${r.name} ${r.underlying||''} ${r.structure}`.toLowerCase().includes(q.toLowerCase())),[rows,filter,q]);
 async function start(kind:'scratch'|'template'){setBusy(kind);
  try{const s=await sb.create({underlying:'NIFTY',expiry:'',legs:[],scenario:{}});router.push({pathname:'/strategies',params:{id:s.id,...(kind==='template'?{open:'template'}:{})}} as any);}
  catch(e:any){setError(e.message);}finally{setBusy('');}}
 return <View style={{padding:wide?24:14,gap:18,maxWidth:1200,width:'100%',alignSelf:'center'}}>
  <View style={{gap:4}}><T style={{fontFamily:'ManropeBold',fontSize:24}}>Strategies</T>
   <T style={{color:C.muted,fontSize:13}}>Build an options strategy, understand its risk, save it, and paper trade it. Nothing here sends an order.</T></View>
  <View style={{flexDirection:wide?'row':'column',gap:12}}>
   <StartCard icon="edit-3" title="Build from scratch" detail="Pick exact contracts from the option chain." busy={busy==='scratch'} onPress={()=>start('scratch')}/>
   <StartCard icon="layout" title="Use a template" detail="Start from a recipe like a bull call spread or an iron condor." busy={busy==='template'} onPress={()=>start('template')}/>
   <StartCard icon="compass" title="Find a strategy" detail="Tell us your view and limits; get a short, explained list." onPress={()=>router.push({pathname:'/strategies',params:{view:'discover'}} as any)}/>
  </View>
  {!!error&&<T style={{color:C.red}}>{error}</T>}
  <View style={[s.between,{flexWrap:'wrap',gap:10}]}>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
    {(['research','paper','archived'] as Filter[]).map(f=><Chip key={f} label={{research:'All active',paper:'Paper traded',archived:'Archived'}[f]} active={filter===f} onPress={()=>setFilter(f)}/>)}
    <Chip label="Lab" icon="activity" onPress={()=>router.push({pathname:'/strategies',params:{view:'lab'}} as any)}/>
    <Chip label="Paper runs" icon="play" onPress={()=>router.push({pathname:'/strategies',params:{view:'paper'}} as any)}/>
    <Chip label={bell.count?`Alerts (${bell.count})`:'Alerts'} icon="bell" active={bell.count>0} onPress={()=>router.push({pathname:'/strategies',params:{view:'alerts'}} as any)}/>
   </View>
   <TextInput value={q} onChangeText={setQ} placeholder="Search name, underlying, structure" placeholderTextColor={C.muted} accessibilityLabel="Search strategies"
    style={{height:40,minWidth:260,borderWidth:1,borderColor:C.line,borderRadius:10,paddingHorizontal:12,color:C.ink,fontFamily:'Inter',fontSize:13,backgroundColor:C.paper}}/>
  </View>
  {rows===null?<Loading/>:!rows.length?<Empty icon="layers" title="No strategies yet" detail="Start one above. Every change is saved automatically, and snapshots freeze a version you want to keep."/>:
   !shown.length?<View style={[s.row,{gap:10}]}><T style={{color:C.muted}}>Nothing matches this filter.</T><Button label="Clear filters" kind="outline" onPress={()=>{setQ('');setFilter('research');}}/></View>:
   <View style={{borderWidth:1,borderColor:C.line,borderRadius:12,overflow:'hidden'}}>
    {wide&&<View style={[s.row,{backgroundColor:C.paper,paddingHorizontal:14,paddingVertical:8}]}>
     {['Name','Structure','Underlying · expiry','Snapshots','Paper','Updated',''].map((h,i)=><T key={i} style={{flex:i===0?2.2:i===6?1.4:1,fontSize:10,letterSpacing:.6,textTransform:'uppercase',color:C.muted,fontFamily:'InterMedium'}}>{h}</T>)}</View>}
    {shown.map(r=><Pressable key={r.id} accessibilityRole="button" accessibilityLabel={`Open ${r.name}`} onPress={()=>router.push({pathname:'/strategies',params:{id:r.id}} as any)}
     style={({pressed})=>[{flexDirection:wide?'row':'column',alignItems:wide?'center':'flex-start',gap:wide?0:4,paddingHorizontal:14,paddingVertical:12,borderTopWidth:1,borderColor:C.line,backgroundColor:pressed?C.soft:'transparent'}]}>
     <T style={{flex:wide?2.2:undefined,fontFamily:'InterSemi',fontSize:13}}>{r.name}</T>
     <T style={{flex:wide?1:undefined,fontSize:12,color:C.muted}}>{r.structure}{r.legs?` · ${r.legs} leg${r.legs>1?'s':''}`:''}</T>
     <T style={{flex:wide?1:undefined,fontSize:12}}>{`${r.underlying||'—'} · ${dayMonth(r.expiry)}`}</T>
     <T style={{flex:wide?1:undefined,fontSize:12,color:C.muted}}>{`${r.snapshots} snapshot${r.snapshots===1?'':'s'}`}</T>
     <View style={{flex:wide?1:undefined}}>{r.paper_open?<Badge label={`${r.paper_open} open`} tone="green"/>:r.paper_total?<Badge label={`${r.paper_total} closed`} tone="neutral"/>:<T style={{fontSize:12,color:C.muted}}>—</T>}</View>
     <T style={{flex:wide?1:undefined,fontSize:11,color:C.muted}}>{istEpoch(r.updated_at)}</T>
     <View style={[s.row,{flex:wide?1.4:undefined,gap:6,justifyContent:'flex-end'}]}>
      <Button label="Duplicate" kind="outline" onPress={async()=>{try{await sb.duplicate(r.id);load();}catch(e:any){setError(e.message);}}}/>
      <Button label={r.archived_at?'Restore':'Archive'} kind="outline" onPress={async()=>{try{await sb.archive(r.id,!r.archived_at);load();}catch(e:any){setError(e.message);}}}/>
     </View>
    </Pressable>)}
   </View>}
  <T style={{fontSize:11,color:C.muted}}>Archive hides a strategy; it never deletes its snapshots or paper history.</T>
 </View>;
}
function StartCard({icon,title,detail,onPress,busy}:{icon:string;title:string;detail:string;onPress:()=>void;busy?:boolean}){
 return <Pressable accessibilityRole="button" accessibilityLabel={title} onPress={onPress} disabled={busy}
  style={({pressed})=>({flex:1,backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:18,gap:8,opacity:pressed||busy?.7:1})}>
  <View style={{alignSelf:'flex-start',padding:10,borderRadius:12,backgroundColor:C.soft}}><Icon name={icon} size={18} color={C.green}/></View>
  <T style={{fontFamily:'InterSemi',fontSize:15}}>{title}</T><T style={{fontSize:12,color:C.muted}}>{detail}</T>
 </Pressable>;
}
