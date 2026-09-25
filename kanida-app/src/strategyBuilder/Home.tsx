// K01 My Strategies + K02 Start. One library; Research / Paper / Archived are derived views over the same strategies.
import React,{useCallback,useEffect,useMemo,useState} from 'react';
import {Pressable,TextInput,View,useWindowDimensions} from 'react-native';
import {router} from 'expo-router';
import {Badge,Button,C,Chip,Empty,Icon,Loading,Sheet,T,s} from '../ui';
import {sb,type Expiry,type LibraryRow,type Template} from './api';
import {TemplateSheet} from './Templates';
import {UnderlyingPicker} from './BuilderParts';
import {ErrorRetry} from './States';
import {dayMonth,istEpoch} from './format';
import {useAlertNotifications} from './Alerts';

type Filter='research'|'paper'|'archived';

export function Home(){
 const {width}=useWindowDimensions();const wide=width>=900;
 const bell=useAlertNotifications();
 const [rows,setRows]=useState<LibraryRow[]|null>(null);const [error,setError]=useState('');const [loadErr,setLoadErr]=useState('');const [filter,setFilter]=useState<Filter>('research');const [q,setQ]=useState('');
 const [sort,setSort]=useState('updated');const [startKind,setStartKind]=useState<'scratch'|'template'|null>(null);
 const load=useCallback(()=>{setLoadErr('');sb.list(sort).then(r=>setRows(r.strategies)).catch(e=>setLoadErr(e.message));},[sort]);
 useEffect(()=>{load();},[load]);
 const shown=useMemo(()=>(rows||[]).filter(r=>filter==='archived'?!!r.archived_at:!r.archived_at&&(filter==='paper'?r.paper_total>0:true))
  .filter(r=>!q||`${r.name} ${r.underlying||''} ${r.structure}`.toLowerCase().includes(q.toLowerCase())),[rows,filter,q]);

 return <View style={{padding:wide?24:14,gap:18,maxWidth:1200,width:'100%',alignSelf:'center'}}>
  <View style={{gap:4}}><T style={{fontFamily:'ManropeBold',fontSize:24}}>Strategies</T>
   <T style={{color:C.muted,fontSize:13}}>Build an options strategy, understand its risk, save it, and paper trade it. Nothing here sends an order.</T></View>
  <View style={{flexDirection:wide?'row':'column',gap:12}}>
   <StartCard icon="edit-3" title="Build from scratch" detail="Choose the underlying, then pick exact contracts from the option chain." onPress={()=>setStartKind('scratch')}/>
   <StartCard icon="layout" title="Use a template" detail="Choose the underlying, then a recipe like a bull call spread or an iron condor." onPress={()=>setStartKind('template')}/>
   <StartCard icon="compass" title="Find a strategy" detail="Tell us your view and limits; get a short, explained list." onPress={()=>router.push({pathname:'/strategies',params:{view:'discover'}} as any)}/>
  </View>
  {!!error&&<T style={{color:C.red}}>{error}</T>}
  {/* destinations are buttons, filters are chips - they no longer look alike (GTM audit P11) */}
  <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><T style={{fontSize:11,color:C.muted}}>Go to</T>
   <Button label="Lab" icon="activity" kind="soft" onPress={()=>router.push({pathname:'/strategies',params:{view:'lab'}} as any)}/>
   <Button label="Paper runs" icon="play" kind="soft" onPress={()=>router.push({pathname:'/strategies',params:{view:'paper'}} as any)}/>
   <Button label={bell.count?`Alerts (${bell.count})`:'Alerts'} icon="bell" kind="soft" onPress={()=>router.push({pathname:'/strategies',params:{view:'alerts'}} as any)}/>
  </View>
  <View style={[s.between,{flexWrap:'wrap',gap:10}]}>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
    <T style={{fontSize:11,color:C.muted}}>Show</T>
    {(['research','paper','archived'] as Filter[]).map(f=><Chip key={f} label={{research:'All active',paper:'Paper traded',archived:'Archived'}[f]} active={filter===f} onPress={()=>setFilter(f)}/>)}
    <T style={{fontSize:11,color:C.muted,marginLeft:6}}>Sort</T>
    {([['updated','Recent'],['name','Name'],['expiry','Expiry'],['created','Created']] as const).map(([k,l])=><Chip key={k} label={l} active={sort===k} onPress={()=>setSort(k)}/>)}
   </View>
   <TextInput value={q} onChangeText={setQ} placeholder="Search name, underlying, structure" placeholderTextColor={C.muted} accessibilityLabel="Search strategies"
    style={{height:40,minWidth:260,borderWidth:1,borderColor:C.line,borderRadius:10,paddingHorizontal:12,color:C.ink,fontFamily:'Inter',fontSize:13,backgroundColor:C.paper}}/>
  </View>
  {loadErr?<ErrorRetry what="Your strategies" error={loadErr} onRetry={load}/>:rows===null?<Loading/>:!rows.length?<Empty icon="layers" title="No strategies yet" detail="Start one above. Every change is saved automatically, and snapshots freeze a version you want to keep."/>:
   !shown.length?<View style={[s.row,{gap:10}]}><T style={{color:C.muted}}>Nothing matches this filter.</T><Button label="Clear filters" kind="outline" onPress={()=>{setQ('');setFilter('research');}}/></View>:
   <View style={{borderWidth:1,borderColor:C.line,borderRadius:12,overflow:'hidden'}}>
    {wide&&<View style={[s.row,{backgroundColor:C.paper,paddingHorizontal:14,paddingVertical:8}]}>
     {['Name','Structure','Underlying · expiry','Snapshots','Paper','Updated',''].map((h,i)=><T key={i} style={{flex:i===0?2.2:i===6?1.4:1,fontSize:10,letterSpacing:.6,textTransform:'uppercase',color:C.muted,fontFamily:'InterMedium'}}>{h}</T>)}</View>}
    {shown.map(r=><Pressable key={r.id} accessibilityRole="button" accessibilityLabel={`Open ${r.name}`} onPress={()=>router.push({pathname:'/strategies',params:{id:r.id}} as any)}
     style={({pressed})=>[{flexDirection:wide?'row':'column',alignItems:wide?'center':'flex-start',gap:wide?0:4,paddingHorizontal:14,paddingVertical:12,borderTopWidth:1,borderColor:C.line,backgroundColor:pressed?C.soft:'transparent'}]}>
     <View style={{flex:wide?2.2:undefined,gap:4}}><T style={{fontFamily:'InterSemi',fontSize:13}}>{r.name}</T>
      {!!(r.badges?.length||r.deployments?.open||r.deployments?.attention)&&<View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
       {r.badges?.includes('expired')&&<Badge label="Expired contracts" tone="red"/>}{r.badges?.includes('empty')&&<Badge label="No legs" tone="neutral"/>}
       {!!r.deployments?.open&&<Badge label={`${r.deployments.open} paper deployment${r.deployments.open>1?'s':''} open`} tone="green"/>}
       {!!r.deployments?.attention&&<Badge label={`${r.deployments.attention} need attention`} tone="red"/>}</View>}</View>
     <T style={{flex:wide?1:undefined,fontSize:12,color:C.muted}}>{r.structure}{r.legs?` · ${r.legs} leg${r.legs>1?'s':''}`:''}</T>
     <T style={{flex:wide?1:undefined,fontSize:12}}>{`${r.underlying||'—'} · ${dayMonth(r.expiry)}`}</T>
     <T style={{flex:wide?1:undefined,fontSize:12,color:C.muted}}>{`${r.snapshots} snapshot${r.snapshots===1?'':'s'}`}</T>
     <View style={{flex:wide?1:undefined}}>{(r.paper_open||r.deployments?.open)?<Badge label={`${(r.paper_open||0)+(r.deployments?.open||0)} open`} tone="green"/>:(r.paper_total||r.deployments?.closed)?<Badge label={`${(r.paper_total||0)+(r.deployments?.closed||0)} closed`} tone="neutral"/>:<T style={{fontSize:12,color:C.muted}}>—</T>}</View>
     <T style={{flex:wide?1:undefined,fontSize:11,color:C.muted}}>{istEpoch(r.updated_at)}</T>
     <View style={[s.row,{flex:wide?1.4:undefined,gap:6,justifyContent:'flex-end'}]}>
      <Button label="Duplicate" kind="outline" onPress={async()=>{try{await sb.duplicate(r.id);load();}catch(e:any){setError(e.message);}}}/>
      <Button label={r.archived_at?'Restore':'Archive'} kind="outline" onPress={async()=>{try{await sb.archive(r.id,!r.archived_at);load();}catch(e:any){setError(e.message);}}}/>
     </View>
    </Pressable>)}
   </View>}
  <T style={{fontSize:11,color:C.muted}}>Archive hides a strategy; it never deletes its snapshots or paper history, and never closes a paper deployment.</T>
  <StartSheet kind={startKind} onClose={()=>setStartKind(null)} onError={setError}/>
 </View>;
}

/** K02 start (GTM audit P17): the underlying is chosen deliberately first. Scratch creates the draft and opens the
 *  chain; a template creates nothing until a recipe is picked - browsing recipes never leaves an empty draft behind. */
function StartSheet({kind,onClose,onError}:{kind:'scratch'|'template'|null;onClose:()=>void;onError:(e:string)=>void}){
 const [unds,setUnds]=useState<{symbol:string;kind?:string}[]>([]);const [u,setU]=useState('NIFTY');const [exps,setExps]=useState<Expiry[]>([]);const [e,setE]=useState('');
 const [err,setErr]=useState('');const [busy,setBusy]=useState(false);
 useEffect(()=>{if(!kind)return;setErr('');sb.underlyings().then(r=>setUnds(r.underlyings)).catch(x=>setErr(x.message));},[kind]);
 useEffect(()=>{if(!kind)return;let live=true;setExps([]);setE('');sb.expiries(u).then(r=>{if(!live)return;setExps(r.expiries);setE((r.expiries.find(x=>(x.days_to_expiry??0)>=1)||r.expiries[0])?.expiry||'');}).catch(x=>{if(live)setErr(x.message);});return()=>{live=false};},[kind,u]);
 const context=<View style={{gap:10}}>
  <T style={{fontSize:12,color:C.muted}}>Underlying</T>
  <UnderlyingPicker list={unds} value={u} onPick={setU}/>
  <T style={{fontSize:12,color:C.muted}}>Expiry (the first with at least one full day left is chosen for you)</T>
  <View style={[s.row,{flexWrap:'wrap',gap:8}]}>{exps.map(x=><Chip key={x.expiry} label={`${dayMonth(x.expiry)} ${x.monthly?'M':'W'} · ${Math.max(0,Math.round(x.days_to_expiry))}d`} active={e===x.expiry} onPress={()=>setE(x.expiry)}/>)}</View>
  {!!err&&<ErrorRetry what="Market context" error={err}/>}
 </View>;
 async function scratch(){setBusy(true);
  try{const s=await sb.create({underlying:u,expiry:e,legs:[],scenario:{}},`${u} strategy ${dayMonth(e)}`);onClose();router.push({pathname:'/strategies',params:{id:s.id,open:'chain'}} as any);}
  catch(x:any){onError(x.message);}finally{setBusy(false);}}
 async function fromTemplate(t:Template,param:number|null){
  if(busy||!e)return;setBusy(true);                                   // one tap = one strategy
  try{const r=await sb.resolve(t.key,u,e,param);
   const legs=r.legs.map((l:any,i:number)=>({id:`L${i+1}`,type:l.type,side:l.side,strike:l.strike,lots:l.lots,expiry:l.expiry,price_basis:'exec' as const,price:null,include:true}));
   const s=await sb.create({underlying:u,expiry:e,legs,scenario:{},template:t.key,param},`${u} ${t.name} ${dayMonth(e)}`);onClose();router.push({pathname:'/strategies',params:{id:s.id}} as any);}
  catch(x:any){setErr(x.message);}finally{setBusy(false);}}
 if(kind==='template')return <TemplateSheet visible onClose={onClose} replacing={0} onPick={fromTemplate} title={`Use a template on ${u}`} header={context}/>;
 return <Sheet visible={kind==='scratch'} onClose={onClose} title="Build from scratch" subtitle="Choose what you are trading first. Nothing is created until you continue."
  footer={<View style={[s.row,{justifyContent:'flex-end',gap:8}]}><Button label="Cancel" kind="outline" onPress={onClose}/>
   <Button label="Create and open the chain" icon="arrow-right" loading={busy} disabled={!u||!e} onPress={scratch}/></View>}>{context}</Sheet>;
}
function StartCard({icon,title,detail,onPress,busy}:{icon:string;title:string;detail:string;onPress:()=>void;busy?:boolean}){
 return <Pressable accessibilityRole="button" accessibilityLabel={title} onPress={onPress} disabled={busy}
  style={({pressed})=>({flex:1,backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:18,gap:8,opacity:pressed||busy?.7:1})}>
  <View style={{alignSelf:'flex-start',padding:10,borderRadius:12,backgroundColor:C.soft}}><Icon name={icon} size={18} color={C.green}/></View>
  <T style={{fontFamily:'InterSemi',fontSize:15}}>{title}</T><T style={{fontSize:12,color:C.muted}}>{detail}</T>
 </Pressable>;
}
