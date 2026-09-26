// K13 Alerts (slice 5). Rules watch a research draft or a paper deployment's actual position; they fire once on the
// transition into the condition, re-arm after it clears, and never fire on missing data. NOTIFY ONLY - an alert never
// places, modifies or cancels an order. Browser notifications are opt-in and only while a KANIDA tab is open.
import React,{useCallback,useEffect,useRef,useState} from 'react';
import {Platform,Pressable,TextInput,View} from 'react-native';
import {router} from 'expo-router';
import {Badge,Button,C,Chip,Empty,Icon,Loading,T,s} from '../ui';
import {alerts,type Analysis,type AlertEvent,type AlertRule,type Deployment} from './api';
import {istStamp,dayMonth,inr,istEpoch,num} from './format';
import {ErrorRetry} from './States';

const TYPES:[string,string,('strategy'|'deployment')[]][]=[
 ['price_cross','Underlying crosses a level',['strategy','deployment']],['breakeven_near','Spot near a breakeven',['strategy','deployment']],
 ['pnl','Position P&L threshold',['deployment']],['delta','Net delta limit',['strategy','deployment']],
 ['short_itm','A short leg goes in the money',['strategy','deployment']],['expiry_time','Reminder at a time',['strategy','deployment']]];
const tone=(st:string)=>st==='triggered'?'red':st==='armed'?'green':st==='data_unavailable'?'amber':'neutral';
const msg=(e:any)=>e?.message||'KANIDA could not complete that.';

/** Browser notifications: opt-in, fired only for NEW unacknowledged events while a tab is open. */
export function useAlertNotifications(){
 const seen=useRef<Set<string>|null>(null);const [count,setCount]=useState(0);
 const [perm,setPerm]=useState<string>(Platform.OS==='web'&&typeof Notification!=='undefined'?Notification.permission:'unsupported');
 useEffect(()=>{let live=true;
  const poll=()=>alerts.unacked().then(r=>{if(!live)return;setCount(r.unacked);
   const ids=new Set(r.latest.map(e=>e.id));
   if(seen.current&&perm==='granted')for(const e of r.latest)if(!seen.current.has(e.id)&&e.kind==='triggered'){try{new Notification('KANIDA alert',{body:e.message,tag:e.id});}catch{}}
   seen.current=new Set([...(seen.current||[]),...ids]);}).catch(()=>{});
  poll();const t=setInterval(poll,15000);return()=>{live=false;clearInterval(t);};},[perm]);
 const enable=async()=>{if(typeof Notification==='undefined')return;setPerm(await Notification.requestPermission());};
 return {count,perm,enable};
}

export function AlertsPanel({strategyId,analysis,deployments,expiry,onChanged}:{strategyId:string;analysis:Analysis|null;deployments:Deployment[];expiry:string;onChanged?:()=>void}){
 const [data,setData]=useState<{rules:AlertRule[];events:AlertEvent[]}|null>(null);const [error,setError]=useState('');
 const [type,setType]=useState('price_cross');const [scope,setScope]=useState<string>('strategy');
 const [f,setF]=useState<Record<string,string>>({});const [busy,setBusy]=useState(false);const [note,setNote]=useState('');
 const active=deployments.filter(d=>!['closed','cancelled'].includes(d.status));
 const [loadErr,setLoadErr]=useState('');
 const load=useCallback(()=>alerts.forStrategy(strategyId).then(d=>{setData(d);setLoadErr('');}).catch(e=>setLoadErr(msg(e))),[strategyId]);
 useEffect(()=>{load();const t=setInterval(load,15000);return()=>clearInterval(t);},[load]);
 const spot=analysis?.spot;
 useEffect(()=>{ // sensible defaults from the live analysis
  const level=spot?String(Math.round(spot/50)*50+(f.direction==='below'?-100:100)):'';
  setF(x=>({level:x.level||level,direction:x.direction||'above',points:x.points||'50',amount:x.amount||String(Math.max(500,Math.round(Math.abs(analysis?.capital_at_risk?.value||2000)*0.5/100)*100)),
   loss:x.loss||'loss',units:x.units||'50',at:x.at||(expiry?`${expiry} 13:30`:'')}));},[spot,expiry]);// eslint-disable-line react-hooks/exhaustive-deps
 const allowed=TYPES.filter(t=>t[2].includes(scope==='strategy'?'strategy':'deployment'));
 useEffect(()=>{if(!allowed.find(t=>t[0]===type))setType(allowed[0][0]);},[scope]);// eslint-disable-line react-hooks/exhaustive-deps
 const params=()=>({price_cross:{level:Number(f.level),direction:f.direction},breakeven_near:{points:Number(f.points)},pnl:{amount:Number(f.amount),direction:f.loss},
  delta:{units:Number(f.units)},short_itm:{},expiry_time:{at:f.at}} as any)[type];
 async function create(t=type,p=params(),dep=scope==='strategy'?null:scope){setBusy(true);setError('');setNote('');
  try{const r=await alerts.create(strategyId,{type:t,params:p,deployment_id:dep,channels:['in_app','browser']});
   setNote(r.now?.evaluated?`Created. Right now: ${r.now.message}.`:`Created. ${r.now?.reason||'It is evaluated during market hours.'}`);load();onChanged?.();}
  catch(e:any){setError(msg(e));}finally{setBusy(false);}}
 async function suggested(d:Deployment){
  const loss=Math.max(500,Math.round(Math.abs(analysis?.capital_at_risk?.value||2000)*0.5/100)*100);
  const batch=[['breakeven_near',{points:50}],['pnl',{amount:loss,direction:'loss'}],['short_itm',{}],['expiry_time',{at:`${expiry} 13:30`}]] as [string,any][];
  const failed:string[]=[];setError('');setNote('');
  for(const [t,p] of batch){try{await alerts.create(strategyId,{type:t,params:p,deployment_id:d.id,channels:['in_app','browser']});}catch(e:any){failed.push(`${t.replace('_',' ')}: ${msg(e)}`);}}
  // never report success for a partly failed batch (GTM audit P21)
  if(failed.length)setError(`${batch.length-failed.length} of ${batch.length} suggested alerts added. Not added - ${failed.join('; ')}`);
  else setNote(`All ${batch.length} suggested alerts added for this deployment.`);
  load();}
 const field=(k:string,label:string,w=110)=><View style={{gap:3}}><T style={{fontSize:11,color:C.muted}}>{label}</T>
  <TextInput value={f[k]||''} onChangeText={v=>setF(x=>({...x,[k]:v}))} accessibilityLabel={label} style={{width:w,height:32,borderWidth:1,borderColor:C.line,borderRadius:8,paddingHorizontal:8,color:C.ink,fontFamily:'Inter',fontSize:12}}/></View>;
 if(!data)return loadErr?<ErrorRetry what="Alerts" error={loadErr} onRetry={load}/>:<Loading/>;
 return <View style={{gap:14}}>
  {!!loadErr&&<ErrorRetry what="The latest alert state" error={loadErr} onRetry={load}/>}
  <T style={{fontSize:11,color:C.muted}}>Alerts notify only - they never place, change or cancel an order. They fire once when the condition becomes true, re-arm after it clears, and never fire on missing data.</T>
  <View style={{backgroundColor:C.bg,borderWidth:1,borderColor:C.line,borderRadius:10,padding:12,gap:10}}>
   <View style={[s.row,{flexWrap:'wrap',gap:6}]}><T style={{fontSize:11,color:C.muted}}>Watch</T>
    <Chip label="This strategy's draft" active={scope==='strategy'} onPress={()=>setScope('strategy')}/>
    {active.map(d=><Chip key={d.id} label={`Paper deployment · ${d.status}`} active={scope===d.id} onPress={()=>setScope(d.id)}/>)}</View>
   <View style={[s.row,{flexWrap:'wrap',gap:6}]}>{allowed.map(([k,l])=><Chip key={k} label={l} active={type===k} onPress={()=>setType(k)}/>)}</View>
   <View style={[s.row,{flexWrap:'wrap',gap:10,alignItems:'flex-end'}]}>
    {type==='price_cross'&&<>{field('level','Level')}<View style={[s.row,{gap:6}]}>{['above','below'].map(d=><Chip key={d} label={d==='above'?'Rises above':'Falls below'} active={f.direction===d} onPress={()=>setF(x=>({...x,direction:d}))}/>)}</View></>}
    {type==='breakeven_near'&&field('points','Within points')}
    {type==='pnl'&&<>{field('amount','Amount ₹')}<View style={[s.row,{gap:6}]}>{['loss','profit'].map(d=><Chip key={d} label={d==='loss'?'Loss of':'Profit of'} active={f.loss===d} onPress={()=>setF(x=>({...x,loss:d}))}/>)}</View></>}
    {type==='delta'&&field('units','|Delta| units')}
    {type==='expiry_time'&&field('at','At (IST, YYYY-MM-DD HH:MM)',170)}
    {type==='short_itm'&&<T style={{fontSize:12,color:C.muted}}>Fires when the underlying moves past any short strike.</T>}
    <Button label="Create alert" icon="bell" loading={busy} onPress={()=>create()}/>
   </View>
   {spot!=null&&<T style={{fontSize:11,color:C.muted}}>{`${(analysis as any)?.quality?.live?`Live at ${istStamp(analysis?.as_of)}`:`Stored reading from ${istStamp(analysis?.as_of)} (not live)`}: spot ${num(spot,2)}${analysis?.breakevens?.value?.length?` · breakevens ${analysis.breakevens.value.map((b:number)=>num(b,0)).join(' / ')}`:''}${analysis?.greeks?.delta!=null?` · delta ${num(analysis.greeks.delta,1)}`:''}`}</T>}
   {active.map(d=><Button key={d.id} label="Add suggested alerts for the paper deployment" kind="outline" icon="zap" onPress={()=>suggested(d)}/>)}
   {!!note&&<T style={{fontSize:12,color:C.green}}>{note}</T>}{!!error&&<T style={{fontSize:12,color:C.red}}>{error}</T>}
  </View>
  <RuleList rules={data.rules} onChanged={load}/>
  <EventList events={data.events} onChanged={load}/>
 </View>;
}

export function RuleList({rules,onChanged,showStrategy}:{rules:AlertRule[];onChanged:()=>void;showStrategy?:boolean}){
 const [checked,setChecked]=useState<Record<string,string>>({});const [errs,setErrs]=useState<Record<string,{text:string;retry:()=>void}>>({});const [open,setOpen]=useState<string|null>(null);
 // a failed pause/resume/delete/settings change keeps the rule on screen with the reason and a Retry (GTM audit P21)
 const act=async(r:AlertRule,fn:()=>Promise<any>,what:string)=>{setErrs(e=>{const n={...e};delete n[r.id];return n;});
  try{await fn();onChanged();}catch(e:any){setErrs(x=>({...x,[r.id]:{text:`${what} failed: ${msg(e)}`,retry:()=>act(r,fn,what)}}));}};
 if(!rules.length)return <T style={{fontSize:12,color:C.muted}}>No alert rules yet.</T>;
 return <View style={{gap:8}}>{rules.map(r=><View key={r.id} style={[s.between,{flexWrap:'wrap',gap:8,borderTopWidth:1,borderColor:C.line,paddingTop:8}]}>
  <View style={{flex:1,minWidth:240,gap:3}}>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><Badge label={r.state.replace('_',' ').toUpperCase()} tone={tone(r.state) as any}/><T style={{fontSize:13,fontFamily:'InterSemi'}}>{r.description}</T></View>
   <T style={{fontSize:11,color:C.muted}}>{`${showStrategy&&r.strategy_name?r.strategy_name+' · ':''}${r.scope==='deployment'?'Paper deployment':'Research draft'} · ${r.session==='market'?'market hours':'always'} · cooldown ${Math.round(r.cooldown/60)}m · last checked ${r.last_eval_at?r.last_eval_at.slice(11,19)+' IST':'—'}`}</T>
   {r.state==='data_unavailable'&&!!r.suppressed&&<T style={{fontSize:11,color:C.amber}}>{`Suppressed: ${r.suppressed}.${r.last_value!=null?` Last valid value ${num(r.last_value,2)}.`:''} It will not fire on stale data.`}</T>}
   {!!checked[r.id]&&<T style={{fontSize:11,color:C.mint}}>{checked[r.id]}</T>}
   {!!errs[r.id]&&<View style={[s.row,{gap:8}]}><T accessibilityRole="alert" style={{fontSize:11,color:C.red,flex:1}}>{errs[r.id].text}</T><Button label="Retry" kind="outline" onPress={errs[r.id].retry}/></View>}
   {open===r.id&&<View style={{gap:6,paddingTop:4}}>
    <View style={[s.row,{gap:6,flexWrap:'wrap'}]}><T style={{fontSize:11,color:C.muted}}>Cooldown</T>
     {([[300,'5 min'],[900,'15 min'],[3600,'1 hour'],[14400,'4 hours']] as [number,string][]).map(([v,l])=><Chip key={v} label={String(l)} active={r.cooldown===v} onPress={()=>act(r,()=>alerts.update(r.id,{version:r.version,cooldown:v}),'Changing the cooldown')}/>)}</View>
    {r.type!=='expiry_time'&&<View style={[s.row,{gap:6,flexWrap:'wrap'}]}><T style={{fontSize:11,color:C.muted}}>When</T>
     {[['market','Market hours'],['always','Always']].map(([v,l])=><Chip key={v} label={l} active={r.session===v} onPress={()=>act(r,()=>alerts.update(r.id,{version:r.version,session:v}),'Changing when it runs')}/>)}</View>}
    <View style={[s.row,{gap:6,flexWrap:'wrap'}]}><T style={{fontSize:11,color:C.muted}}>Notify</T>
     <Chip label="In app" active/><Chip label="Browser (tab open)" active={!!r.channels?.includes('browser')}
      onPress={()=>act(r,()=>alerts.update(r.id,{version:r.version,channels:r.channels?.includes('browser')?['in_app']:['in_app','browser']}),'Changing notifications')}/></View>
    <T style={{fontSize:10,color:C.muted}}>Closed-tab push, e-mail and SMS are not available in this release.</T>
   </View>}
  </View>
  <View style={[s.row,{gap:6}]}>
   <Button label="Check now" kind="outline" onPress={async()=>{try{const x=await alerts.check(r.id);setChecked(c=>({...c,[r.id]:x.evaluated?`${x.message}${x.condition?' - condition TRUE':''}`:x.reason}));}catch(e:any){setChecked(c=>({...c,[r.id]:msg(e)}));}}}/>
   <Button label="Settings" kind="outline" accessibilityState={{expanded:open===r.id}} onPress={()=>setOpen(open===r.id?null:r.id)}/>
   <Button label={r.state==='paused'?'Resume':'Pause'} kind="outline" onPress={()=>act(r,()=>alerts.update(r.id,{version:r.version,action:r.state==='paused'?'resume':'pause'}),r.state==='paused'?'Resume':'Pause')}/>
   <Pressable accessibilityRole="button" accessibilityLabel={`Delete alert: ${r.description}`} onPress={()=>act(r,()=>alerts.remove(r.id),'Delete')} style={{padding:12}}><Icon name="trash-2" size={16} color={C.muted}/></Pressable>
  </View></View>)}</View>;
}

export function EventList({events,onChanged,showStrategy}:{events:AlertEvent[];onChanged:()=>void;showStrategy?:boolean}){
 const [err,setErr]=useState('');
 if(!events.length)return <T style={{fontSize:12,color:C.muted}}>No alert events yet.</T>;
 const open=events.filter(e=>!e.acked_at).length;
 return <View style={{gap:6}}>
  <View style={s.between}><T style={{fontFamily:'InterMedium',fontSize:10,letterSpacing:.6,textTransform:'uppercase',color:C.muted}}>{`Events · ${open} to acknowledge`}</T>
   {open>0&&<Button label="Acknowledge all" kind="outline" onPress={async()=>{try{await alerts.ack();setErr('');}catch(e:any){setErr(msg(e));}onChanged();}}/>}</View>
  {!!err&&<ErrorRetry what="Acknowledging" error={err} onRetry={onChanged}/>}
  {events.map(e=><View key={e.id} style={[s.row,{gap:8,alignItems:'flex-start',opacity:e.acked_at?.55:1,borderTopWidth:1,borderColor:C.line,paddingTop:6}]}>
   <Icon name={e.kind==='triggered'?'bell':e.kind==='data_unavailable'?'wifi-off':'rotate-ccw'} size={14} color={e.kind==='triggered'?C.red:e.kind==='data_unavailable'?C.amber:C.muted}/>
   <View style={{flex:1,gap:2}}><T style={{fontSize:12}}>{e.message}</T>
    <T style={{fontSize:11,color:C.muted}}>{`${showStrategy&&e.strategy_name?e.strategy_name+' · ':''}${e.occurred_at.slice(0,16)} IST · ${e.kind.replace('_',' ')}`}</T></View>
   {showStrategy&&<Pressable onPress={()=>router.push({pathname:'/strategies',params:{id:e.strategy_id}} as any)}><T style={{fontSize:11,color:C.green}}>Open</T></Pressable>}
   <Pressable accessibilityRole="button" accessibilityLabel="Adjust this strategy" onPress={()=>router.push({pathname:'/strategies',params:{id:e.strategy_id,open:'adjust'}} as any)}><T style={{fontSize:11,color:C.green}}>Adjust</T></Pressable>
   {!e.acked_at&&<Pressable accessibilityRole="button" onPress={async()=>{try{await alerts.ack(e.id);setErr('');}catch(x:any){setErr(msg(x));}onChanged();}}><T style={{fontSize:11,color:C.green}}>Acknowledge</T></Pressable>}
  </View>)}
 </View>;
}

/** The alerts centre: every rule and event across strategies. */
export function AlertsCenter(){
 const [data,setData]=useState<Awaited<ReturnType<typeof alerts.all>>|null>(null);const [error,setError]=useState('');
 const note=useAlertNotifications();
 const load=useCallback(()=>alerts.all().then(d=>{setData(d);setError('');}).catch(e=>setError(msg(e))),[]);
 useEffect(()=>{load();const t=setInterval(load,15000);return()=>clearInterval(t);},[load]);
 return <View style={{padding:20,gap:16,maxWidth:1100,width:'100%',alignSelf:'center'}}>
  <View style={[s.row,{gap:8}]}><Button label="My Strategies" icon="chevron-left" kind="outline" onPress={()=>router.replace('/strategies' as any)}/></View>
  <View style={{gap:4}}><T style={{fontFamily:'ManropeBold',fontSize:22}}>Alerts</T><T style={{fontSize:13,color:C.muted}}>{data?.boundary||''}</T></View>
  {note.perm==='default'&&<View style={[s.row,{gap:10,flexWrap:'wrap'}]}><Button label="Enable browser notifications" icon="bell" kind="outline" onPress={note.enable}/>
   <T style={{fontSize:11,color:C.muted}}>Shown only while a KANIDA tab is open.</T></View>}
  {note.perm==='denied'&&<T style={{fontSize:11,color:C.muted}}>Browser notifications are blocked for this site in your browser settings; alerts still appear here.</T>}
  {!!error&&<ErrorRetry what="Alerts" error={error} onRetry={load}/>}
  {!data?(error?null:<Loading/>):<>
   <View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:12,padding:14,gap:10}}><EventList events={data.events} onChanged={load} showStrategy/></View>
   <View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:12,padding:14,gap:10}}>
    <T style={{fontFamily:'InterSemi'}}>Rules</T>
    {data.rules.length?<RuleList rules={data.rules} onChanged={load} showStrategy/>:<Empty icon="bell" title="No alerts yet" detail="Open a strategy and use its Alerts tab. A paper deployment can get suggested alerts in one click."/>}
   </View>
  </>}
 </View>;
}
