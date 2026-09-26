// Owner admin panel (cloud preview): the old portal's access workflow rebuilt in this app - invite codes, access
// requests (waitlist), users, and a read-only jobs & health view. Everything here is this app's own data.
import React,{useCallback,useEffect,useState} from 'react';
import {ScrollView,TextInput,View,useWindowDimensions} from 'react-native';
import {Badge,Button,C,Chip,T,s} from '../ui';
import {api} from '../model';
import {useAuth} from '../Auth';

type Code={id:string;code_hint:string;uses:number;uses_max:number;expires:number|null;note:string;created:number;state:string};
type Req={id:string;email:string;name:string;note:string;status:string;created:number};
type User={id:string;email:string;name:string;role:string;active:boolean;created:number;last_sign_in:number|null};
type Job={key:string;name:string;cadence:string;last_run:number|string|null;health:string;detail?:Record<string,number>;last_status?:string;last_error_text?:string;missed_latest_session?:number};

const when=(t:number|string|null|undefined)=>{if(t==null)return '—';const d=typeof t==='number'?new Date(t*1000):new Date(String(t).replace(' ','T')+'+05:30');
 return d.toLocaleString('en-IN',{timeZone:'Asia/Kolkata',day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'})+' IST';};
const TONE:Record<string,any>={ok:'green',active:'green',approved:'green',waiting:'neutral',pending:'amber',late:'amber','used up':'neutral',expired:'neutral',
 failing:'red',stopped:'red',revoked:'red',declined:'neutral','not configured':'neutral','switched off':'neutral'};

export function AdminPanel(){
 const auth=useAuth();const narrow=useWindowDimensions().width<700;
 const [tab,setTab]=useState<'access'|'users'|'jobs'>('access');
 if(auth.user?.role!=='owner')return <View style={{padding:24}}><T style={{color:C.muted}}>The admin panel is available to the owner only.</T></View>;
 return <ScrollView contentContainerStyle={{padding:narrow?14:24,gap:16,maxWidth:1100,width:'100%',alignSelf:'center'}}>
  <View style={{gap:4}}><T style={{fontFamily:'ManropeBold',fontSize:24}}>Admin</T>
   <T style={{fontSize:13,color:C.muted}}>Who can get in, who is in, and whether the background jobs are healthy.</T></View>
  <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>{([['access','Access'],['users','Users'],['jobs','Jobs & health']] as const).map(([k,l])=><Chip key={k} label={l} active={tab===k} onPress={()=>setTab(k)}/>)}</View>
  {tab==='access'&&<><Codes/><Requests/></>}
  {tab==='users'&&<Users/>}
  {tab==='jobs'&&<Jobs/>}
 </ScrollView>;
}

function Panel({title,children}:{title:string;children:React.ReactNode}){
 return <View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:14,gap:10}}><T style={{fontFamily:'InterSemi',fontSize:15}}>{title}</T>{children}</View>;
}
const box={minHeight:44,borderWidth:1,borderColor:C.line,borderRadius:10,paddingHorizontal:10,color:C.ink,fontFamily:'Inter',fontSize:13,backgroundColor:C.bg};

function Codes(){
 const [rows,setRows]=useState<Code[]|null>(null);const [fresh,setFresh]=useState<string[]>([]);const [err,setErr]=useState('');const [busy,setBusy]=useState(false);
 const [count,setCount]=useState('1');const [uses,setUses]=useState('1');const [days,setDays]=useState('7');const [note,setNote]=useState('');
 const load=useCallback(()=>api('/api/admin/codes').then(r=>setRows(r.codes)).catch(e=>setErr(e.message)),[]);
 useEffect(()=>{load();},[load]);
 async function issue(){setBusy(true);setErr('');try{const r=await api('/api/admin/codes',{count:Number(count),uses:Number(uses),days:days.trim()?Number(days):null,note});setFresh(r.codes);load();}catch(e:any){setErr(e.message);}finally{setBusy(false);}}
 return <Panel title="Invite codes">
  <T style={{fontSize:12,color:C.muted}}>Share a code with a tester; they enter it on the sign-up page (or with Google). Codes are shown in full only once.</T>
  <View style={[s.row,{gap:10,flexWrap:'wrap',alignItems:'flex-end'}]}>
   {([['How many',count,setCount,70],['Sign-ups per code',uses,setUses,70],['Expires in days (blank = never)',days,setDays,90]] as const).map(([l,v,f,w])=><View key={l} style={{gap:3}}><T style={{fontSize:11,color:C.muted}}>{l}</T>
    <TextInput value={v} onChangeText={f as any} keyboardType="number-pad" accessibilityLabel={l} style={[box,{width:w}]}/></View>)}
   <View style={{gap:3,flex:1,minWidth:160}}><T style={{fontSize:11,color:C.muted}}>Note (who it is for)</T><TextInput value={note} onChangeText={setNote} accessibilityLabel="Note" style={box}/></View>
   <Button label="Create codes" icon="plus" loading={busy} onPress={issue}/></View>
  {!!fresh.length&&<View accessibilityRole="alert" style={{backgroundColor:C.soft,borderRadius:10,padding:10,gap:4}}>
   <T style={{fontFamily:'InterSemi'}}>Copy these now - they will not be shown again</T>
   {fresh.map(c=><T key={c} selectable style={{fontFamily:'InterSemi',fontSize:15,letterSpacing:1}}>{c}</T>)}
   <Button label="Done" kind="outline" onPress={()=>setFresh([])}/></View>}
  {!!err&&<T accessibilityRole="alert" style={{color:C.red,fontSize:12}}>{err}</T>}
  {rows===null?<T style={{color:C.muted}}>Loading…</T>:!rows.length?<T style={{color:C.muted,fontSize:12}}>No codes yet.</T>:
   rows.map(r=><View key={r.id} style={[s.row,{gap:10,flexWrap:'wrap',alignItems:'center',borderTopWidth:1,borderColor:C.line,paddingTop:8}]}>
    <Badge label={r.state.toUpperCase()} tone={TONE[r.state]||'neutral'}/><T style={{fontFamily:'InterSemi'}}>{r.code_hint}</T>
    <T style={{fontSize:12,color:C.muted,flex:1}}>{`${r.uses}/${r.uses_max} used · ${r.expires?`expires ${when(r.expires)}`:'no expiry'}${r.note?` · ${r.note}`:''} · created ${when(r.created)}`}</T>
    {r.state==='active'&&<Button label="Revoke" kind="outline" onPress={async()=>{try{await api(`/api/admin/codes/${r.id}/revoke`,{});load();}catch(e:any){setErr(e.message);}}}/>}</View>)}
 </Panel>;
}

function Requests(){
 const [rows,setRows]=useState<Req[]|null>(null);const [status,setStatus]=useState('pending');const [link,setLink]=useState<{email:string;url:string}|null>(null);const [err,setErr]=useState('');
 const load=useCallback(()=>api(`/api/admin/requests?status=${status}`).then(r=>setRows(r.requests)).catch(e=>setErr(e.message)),[status]);
 useEffect(()=>{load();},[load]);
 async function decide(r:Req,d:'approve'|'decline'){setErr('');try{const x=await api(`/api/admin/requests/${r.id}/${d}`,{});if(x.invite_url)setLink({email:r.email,url:x.invite_url});load();}catch(e:any){setErr(e.message);}}
 return <Panel title="Access requests (waitlist)">
  <View style={[s.row,{gap:8}]}>{['pending','approved','declined'].map(k=><Chip key={k} label={k[0].toUpperCase()+k.slice(1)} active={status===k} onPress={()=>setStatus(k)}/>)}</View>
  {!!link&&<View accessibilityRole="alert" style={{backgroundColor:C.soft,borderRadius:10,padding:10,gap:4}}>
   <T style={{fontFamily:'InterSemi'}}>{`Invitation for ${link.email} - send them this link (works once, 7 days; no email was sent)`}</T>
   <T selectable style={{fontSize:12}}>{link.url}</T><Button label="Done" kind="outline" onPress={()=>setLink(null)}/></View>}
  {!!err&&<T accessibilityRole="alert" style={{color:C.red,fontSize:12}}>{err}</T>}
  {rows===null?<T style={{color:C.muted}}>Loading…</T>:!rows.length?<T style={{color:C.muted,fontSize:12}}>{`No ${status} requests.`}</T>:
   rows.map(r=><View key={r.id} style={[s.row,{gap:10,flexWrap:'wrap',alignItems:'center',borderTopWidth:1,borderColor:C.line,paddingTop:8}]}>
    <View style={{flex:1,minWidth:200,gap:2}}><T style={{fontFamily:'InterSemi'}}>{`${r.name||'—'} · ${r.email}`}</T><T style={{fontSize:12,color:C.muted}}>{`${r.note||'No note'} · ${when(r.created)}`}</T></View>
    {r.status==='pending'?<><Button label="Approve" icon="check" onPress={()=>decide(r,'approve')}/><Button label="Decline" kind="outline" onPress={()=>decide(r,'decline')}/></>:<Badge label={r.status.toUpperCase()} tone={TONE[r.status]}/>}</View>)}
 </Panel>;
}

function Users(){
 const [rows,setRows]=useState<User[]|null>(null);const [err,setErr]=useState('');
 const load=useCallback(()=>api('/api/admin/users').then(r=>setRows(r.users)).catch(e=>setErr(e.message)),[]);
 useEffect(()=>{load();},[load]);
 return <Panel title={`Users${rows?` (${rows.length})`:''}`}>
  {!!err&&<T accessibilityRole="alert" style={{color:C.red,fontSize:12}}>{err}</T>}
  {rows===null?<T style={{color:C.muted}}>Loading…</T>:rows.map(u=><View key={u.id} style={[s.row,{gap:10,flexWrap:'wrap',alignItems:'center',borderTopWidth:1,borderColor:C.line,paddingTop:8}]}>
   <Badge label={u.active?'ACTIVE':'DEACTIVATED'} tone={u.active?'green':'red'}/>
   <View style={{flex:1,minWidth:200,gap:2}}><T style={{fontFamily:'InterSemi'}}>{`${u.name} · ${u.email}${u.role==='owner'?' · owner':''}`}</T>
    <T style={{fontSize:12,color:C.muted}}>{`Joined ${when(u.created)} · last sign-in ${when(u.last_sign_in)}`}</T></View>
   {u.role!=='owner'&&<Button label={u.active?'Deactivate':'Reactivate'} kind="outline" onPress={async()=>{
    if(u.active&&typeof window!=='undefined'&&window.confirm&&!window.confirm(`Deactivate ${u.email}? They are signed out everywhere immediately.`))return;
    try{await api(`/api/admin/users/${u.id}/${u.active?'deactivate':'reactivate'}`,{});load();}catch(e:any){setErr(e.message);}}}/>}</View>)}
 </Panel>;
}

function Jobs(){
 const [d,setD]=useState<{jobs:Job[];feed:{live:boolean;message?:string;detail?:string}}|null>(null);const [err,setErr]=useState('');
 const load=useCallback(()=>api('/api/admin/jobs').then(setD).catch(e=>setErr(e.message)),[]);
 useEffect(()=>{load();const t=setInterval(load,15000);return()=>clearInterval(t);},[load]);
 return <Panel title="Jobs & health">
  <T style={{fontSize:12,color:C.muted}}>Refreshes every 15 s. Read-only: nothing is started or stopped from here.</T>
  {!!err&&<T accessibilityRole="alert" style={{color:C.red,fontSize:12}}>{err}</T>}
  {d&&<View style={[s.row,{gap:8,alignItems:'center',flexWrap:'wrap'}]}><Badge label={d.feed.live?'LIVE PRICES':'STORED PRICES'} tone={d.feed.live?'green':'amber'}/>
   <T style={{fontSize:12,color:C.muted,flex:1}}>{d.feed.live?'Live option prices are connected.':(d.feed.message||'Live option prices are not connected.')}{d.feed.detail?` (${d.feed.detail})`:''}</T></View>}
  {d?.jobs.map(j=><View key={j.key} style={[s.row,{gap:10,flexWrap:'wrap',alignItems:'center',borderTopWidth:1,borderColor:C.line,paddingTop:8}]}>
   <Badge label={j.health.toUpperCase()} tone={TONE[j.health]||'neutral'}/>
   <View style={{flex:1,minWidth:220,gap:2}}><T style={{fontFamily:'InterSemi'}}>{j.name}</T>
    <T style={{fontSize:12,color:C.muted}}>{`${j.cadence} · last good run ${when(j.last_run)}${j.detail?` · ${Object.entries(j.detail).map(([k,v])=>`${v} ${k}`).join(', ')}`:''}${j.missed_latest_session?` · ${j.missed_latest_session} missed in the latest session`:''}`}</T>
    {!!j.last_error_text&&<T style={{fontSize:11,color:C.amber}}>{j.last_error_text}</T>}</View></View>)}
 </Panel>;
}
