// K05 Build workspace + K07 Analyze + K08 Scenario (docs/strategy_builder_study/BUILD_PLAN_MERGED.md §3).
//
// ONE DRAFT, ALWAYS SAVED: every edit updates the draft, autosaves 800 ms later with the version it was based on (a
// stale save from a second window loads the newer copy instead of overwriting it), and asks the server for a fresh
// analysis. Analyses are numbered; only the answer to the newest request is shown, and while it is on the way the
// previous result stays visible but dimmed and labelled "Updating" - never presented as current.
import React,{useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {Pressable,ScrollView,TextInput,View,useWindowDimensions} from 'react-native';
import {router} from 'expo-router';
import {Badge,Button,C,Chip,Icon,Loading,Sheet,T,s} from '../ui';
import {ApiError} from '../model';
import {exec,sb,type Analysis,type Basis,type Body,type Chain,type Deployment,type Detail,type Expiry,type Kind,type Leg,type PaperRun,type Side,type Status,type Template} from './api';
import {DeploymentCard,OrderReview} from './OrderReview';
import {AlertsPanel,useAlertNotifications} from './Alerts';
import {ChainDrawer} from './ChainDrawer';
import {PayoffChart} from './PayoffChart';
import {addDays,dayMonth,inr,isWeekend,istEpoch,istStamp,num,signed,strikeText,weekday} from './format';

const SAVE_DELAY=800,ANALYZE_DELAY=220;
const uid=()=>'L'+Math.random().toString(36).slice(2,8);
const msg=(e:any)=>e?.message||'KANIDA could not complete that.';
type SaveState='saved'|'dirty'|'saving'|'conflict'|'error';
type Tab='pnl'|'greeks'|'table'|'snapshots'|'paper'|'alerts'|'activity';

export function Builder({id,openTemplate=false}:{id:string;openTemplate?:boolean}){
 const {width}=useWindowDimensions();const wide=width>=1100;
 const [detail,setDetail]=useState<Detail|null>(null);const [error,setError]=useState('');
 const [body,setBody]=useState<Body|null>(null);const [version,setVersion]=useState(0);const [save,setSave]=useState<SaveState>('saved');
 const [expiries,setExpiries]=useState<Expiry[]>([]);const [underlyings,setUnderlyings]=useState<string[]>([]);
 const [chain,setChain]=useState<Chain|null>(null);
 const [analysis,setAnalysis]=useState<Analysis|null>(null);const [pending,setPending]=useState(false);
 const [chainOpen,setChainOpen]=useState(false);const [tplOpen,setTplOpen]=useState(openTemplate);const [paperOpen,setPaperOpen]=useState(false);
 const [tab,setTab]=useState<Tab>('pnl');const [toast,setToast]=useState('');const [undo,setUndo]=useState<{leg:Leg;index:number}|null>(null);
 const [runs,setRuns]=useState<PaperRun[]>([]);const [name,setName]=useState('');
 const [st,setSt]=useState<Status|null>(null);const [tick,setTick]=useState(0);const [deps,setDeps]=useState<Deployment[]>([]);
 const [review,setReview]=useState<{open:boolean;closing?:Deployment|null}>({open:false});
 const bell=useAlertNotifications();
 const seq=useRef(0);const saveTimer=useRef<any>(null);const anTimer=useRef<any>(null);const ctl=useRef<AbortController|null>(null);
 const versionRef=useRef(0);versionRef.current=version;

 const flash=(t:string)=>{setToast(t);setTimeout(()=>setToast(x=>x===t?'':x),4200);};
 const reload=useCallback(async()=>{
  try{const d=await sb.get(id);setDetail(d);setName(d.name);if(d.draft){setBody(d.draft.body);setVersion(d.draft.version);}setSave('saved');}
  catch(e:any){setError(msg(e));}
 },[id]);
 const loadRuns=useCallback(async()=>{try{const r=await sb.paperList();setRuns(r.runs.filter(x=>x.strategy_id===id));}catch{}
  try{const d=await exec.list();setDeps(d.deployments.filter(x=>x.strategy_id===id));}catch{}},[id]);
 useEffect(()=>{let live=true;const poll=()=>exec.status().then(x=>{if(live)setSt(x);}).catch(()=>{});poll();const t=setInterval(()=>{poll();setTick(n=>n+1);},10000);return()=>{live=false;clearInterval(t);};},[]);
 useEffect(()=>{if(deps.some(d=>!['closed','cancelled'].includes(d.status)))loadRuns();},[tick]);// eslint-disable-line react-hooks/exhaustive-deps
 useEffect(()=>{reload();loadRuns();sb.underlyings().then(r=>setUnderlyings(r.underlyings.map(u=>u.symbol))).catch(()=>{});},[reload,loadRuns]);

 // expiries and chain follow the draft's underlying/expiry
 useEffect(()=>{if(!body?.underlying)return;let live=true;
  sb.expiries(body.underlying).then(r=>{if(!live)return;setExpiries(r.expiries);
   if(!body.expiry&&r.expiries[0])edit(b=>({...b,expiry:r.expiries[0].expiry}));}).catch(e=>setError(msg(e)));
  return()=>{live=false};},[body?.underlying]);// eslint-disable-line react-hooks/exhaustive-deps
 useEffect(()=>{if(!body?.underlying||!body.expiry){setChain(null);return;}const c=new AbortController();
  sb.chain(body.underlying,body.expiry,c.signal).then(setChain).catch(e=>{if(e?.name!=='AbortError')flash(msg(e));});
  return()=>c.abort();},[body?.underlying,body?.expiry,st?.live?tick:0]);

 // analysis: newest request wins
 useEffect(()=>{if(!body)return;clearTimeout(anTimer.current);setPending(true);
  anTimer.current=setTimeout(async()=>{const mine=++seq.current;ctl.current?.abort();const c=new AbortController();ctl.current=c;
   try{const a=await sb.analyze(body,c.signal);if(mine===seq.current){setAnalysis(a);setPending(false);}}
   catch(e:any){if(e?.name!=='AbortError'&&mine===seq.current){setPending(false);flash(msg(e));}}},ANALYZE_DELAY);
  return()=>clearTimeout(anTimer.current);},[body,st?.live?tick:0]);

 // autosave
 const persist=useCallback(async(next:Body)=>{setSave('saving');
  try{const s=await sb.save(id,versionRef.current,next);setVersion(s.draft!.version);setSave('saved');}
  catch(e:any){if(e instanceof ApiError&&e.status===409){setSave('conflict');flash('This strategy changed in another window - the newer copy is loaded.');await reload();}
   else{setSave('error');flash(msg(e));}}},[id,reload]);
 function edit(fn:(b:Body)=>Body){setBody(prev=>{if(!prev)return prev;const next=fn(prev);setSave('dirty');clearTimeout(saveTimer.current);
  saveTimer.current=setTimeout(()=>persist(next),SAVE_DELAY);return next;});}
 useEffect(()=>()=>clearTimeout(saveTimer.current),[]);

 const priceOf=(k:number,kind:Kind)=>chain?.rows.find(r=>r.strike===k)?.[kind]?.ltp??null;
 const strikes=useMemo(()=>chain?.rows.map(r=>r.strike)??[],[chain]);
 const moveStrike=(k:number,steps:number)=>{if(!strikes.length)return k;const i=strikes.indexOf(k);const j=Math.max(0,Math.min(strikes.length-1,(i<0?strikes.findIndex(x=>x>=k):i)+steps));return strikes[j];};

 function toggleFromChain(strike:number,kind:Kind,side:Side){
  edit(b=>{const hit=b.legs.find(l=>l.strike===strike&&l.type===kind);
   if(hit&&hit.side===side)return {...b,legs:b.legs.filter(l=>l!==hit)};
   if(hit)return {...b,legs:b.legs.map(l=>l===hit?{...l,side}:l)};
   if(b.legs.length>=8){flash('At most 8 legs are supported in this release.');return b;}
   return {...b,template:null,legs:[...b.legs,{id:uid(),type:kind,side,strike,lots:1,expiry:b.expiry,price_basis:'exec',price:null,include:true}]};});
 }
 const setLeg=(lid:string,patch:Partial<Leg>)=>edit(b=>({...b,template:null,legs:b.legs.map(l=>l.id===lid?{...l,...patch}:l)}));
 function removeLeg(lid:string){if(!body)return;const index=body.legs.findIndex(l=>l.id===lid);const leg=body.legs[index];setUndo({leg,index});
  edit(b=>({...b,legs:b.legs.filter(l=>l.id!==lid)}));setTimeout(()=>setUndo(u=>u?.leg.id===lid?null:u),6000);}
 function adjust(kind:'shift'|'width'|'wings',k:number){
  edit(b=>{const act=b.legs;if(!act.length)return b;
   const shorts=act.filter(l=>l.side==='S');const ref=(shorts.length?shorts:act).map(l=>l.strike);const centre=ref.reduce((a,x)=>a+x,0)/ref.length;
   const legs=act.map(l=>{let steps=0;
    if(kind==='shift')steps=k;
    else if(kind==='width'){if(l.side==='S'||!shorts.length)steps=(l.strike>centre||(l.strike===centre&&l.type==='CE'))?k:-k;else steps=(l.strike>centre?k:-k);}
    else if(kind==='wings'&&l.side==='B'&&shorts.length)steps=l.strike>centre?k:-k;
    return steps?{...l,strike:moveStrike(l.strike,steps)}:l;});
   return {...b,template:null,legs};});
 }
 async function applyTemplate(t:Template,param:number|null){
  if(!body?.underlying||!body.expiry)return;
  try{const r=await sb.resolve(t.key,body.underlying,body.expiry,param);
   const legs:Leg[]=r.legs.map((l:any)=>({id:uid(),type:l.type,side:l.side,strike:l.strike,lots:l.lots,expiry:l.expiry,price_basis:'exec',price:null,include:true}));
   edit(b=>({...b,template:t.key,legs}));setTplOpen(false);flash(`${t.name} loaded - ${legs.length} legs. Everything stays editable.`);}
  catch(e:any){flash(msg(e));}
 }
 async function snapshot(){try{const r=await sb.snapshot(id);flash(`Saved snapshot ${r.n}. It will never change.`);reload();setTab('snapshots');}catch(e:any){flash(msg(e));}}
 async function duplicate(){try{const c=await sb.duplicate(id);router.replace({pathname:'/strategies',params:{id:c.id}} as any);flash('Copy created - research only, no paper runs copied.');}catch(e:any){flash(msg(e));}}
 async function rename(){if(!detail||!name.trim()||name===detail.name)return;try{const s=await sb.meta(id,{name});setDetail(d=>d?{...d,name:s.name}:d);}catch(e:any){flash(msg(e));}}

 if(error)return <View style={{padding:24}}><T style={{color:C.red}}>{error}</T><Button label="Back to strategies" kind="outline" onPress={()=>router.replace('/strategies' as any)}/></View>;
 if(!detail||!body)return <Loading/>;
 const a=analysis;const dim=pending&&!!a;
 const scenario=body.scenario||{};const reading=chain?.as_of||a?.as_of;
 const expiryInfo=expiries.find(e=>e.expiry===body.expiry);

 return <View style={{padding:wide?22:14,gap:16,maxWidth:1500,width:'100%',alignSelf:'center'}}>
  {/* header */}
  <View style={[s.between,{flexWrap:'wrap',gap:10}]}>
   <View style={[s.row,{flexWrap:'wrap',gap:10,flex:1,minWidth:280}]}>
    <Pressable accessibilityRole="button" accessibilityLabel="Back to My Strategies" onPress={()=>router.replace('/strategies' as any)} style={[s.row,{gap:4}]}><Icon name="chevron-left" size={16} color={C.muted}/><T style={{fontSize:12,color:C.muted}}>My Strategies</T></Pressable>
    <TextInput value={name} onChangeText={setName} onBlur={rename} onSubmitEditing={rename} maxLength={80} accessibilityLabel="Strategy name"
     style={{fontFamily:'ManropeBold',fontSize:20,color:C.ink,minWidth:220,flex:1,paddingVertical:4,borderBottomWidth:1,borderColor:C.line}}/>
    <SaveBadge state={save} version={version}/>
   </View>
   <View style={[s.row,{flexWrap:'wrap',gap:8,width:wide?undefined:'100%'}]}>
    <Button label="Save snapshot" icon="bookmark" kind="outline" onPress={snapshot} disabled={!body.legs.length}/>
    <Button label={bell.count?`Alerts (${bell.count})`:'Alerts'} icon="bell" kind="outline" onPress={()=>router.push({pathname:'/strategies',params:{view:'alerts'}} as any)}/>
    <Button label="Duplicate" icon="copy" kind="outline" onPress={duplicate}/>
    <Button label={st?.live?'Review paper orders':'Paper trade'} icon="play" onPress={()=>st?.live?setReview({open:true}):setPaperOpen(true)} disabled={!body.legs.length||!a||a.status==='invalid'}/>
   </View>
  </View>
  <DataBanner reading={reading} live={!!chain?.quality.live} status={st}/>

  {/* market context */}
  <ChipRow wrap={wide}>
   {underlyings.map(u=><Chip key={u} label={u} active={body.underlying===u} onPress={()=>{if(u!==body.underlying){if(body.legs.length&&!confirmSwitch())return;edit(b=>({...b,underlying:u,expiry:'',legs:[],template:null,scenario:{}}));}}}/>)}
  </ChipRow>
  <ChipRow wrap={wide}>
   {expiries.slice(0,8).map(e=><Chip key={e.expiry} active={body.expiry===e.expiry} label={`${dayMonth(e.expiry)} ${e.monthly?'M':'W'} · ${Math.max(0,Math.round(e.days_to_expiry))}d`}
    onPress={()=>edit(b=>({...b,expiry:e.expiry,legs:b.legs.map(l=>({...l,expiry:e.expiry})),scenario:{...b.scenario,at:undefined}}))}/>)}
  </ChipRow>

  <View style={{flexDirection:wide?'row':'column-reverse',gap:16,alignItems:wide?'flex-start':'stretch'}}>
   {/* LEFT: legs */}
   <View style={[panel,{flex:wide?45:undefined,width:wide?undefined:'100%'}]}>
    <View style={[s.between,{flexWrap:'wrap'}]}><View style={{gap:2}}><T style={label}>Legs</T>
     <T style={{fontFamily:'InterSemi',fontSize:14}}>{a?.structure?.name||'—'}{a?.structure&&!a.structure.exact&&body.legs.length?'':''}</T></View>
     <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><Button label="Template" icon="layout" kind="outline" onPress={()=>setTplOpen(true)} disabled={!body.expiry}/><Button label="Add from chain" icon="plus" kind="soft" onPress={()=>setChainOpen(true)} disabled={!chain}/></View></View>
    {!body.legs.length?<View style={{paddingVertical:26,alignItems:'center',gap:8}}><Icon name="layers" size={22} color={C.green}/>
      <T style={{fontFamily:'InterSemi'}}>No legs yet</T><T style={{fontSize:12,color:C.muted,textAlign:'center',maxWidth:340}}>Pick a template for a ready structure, or add exact contracts from the chain.</T></View>:
     body.legs.map(l=>{const row=a?.legs?.find(r=>r.id===l.id);const ltp=priceOf(l.strike,l.type);
      return <View key={l.id} style={{borderTopWidth:1,borderColor:C.line,paddingTop:10,gap:8,opacity:l.include?1:.5}}>
       <View style={[s.row,{flexWrap:'wrap',gap:8}]}>
        <Pressable role="checkbox" aria-checked={l.include} accessibilityLabel={`Include ${strikeText(l.strike)} ${l.type} in analysis`} onPress={()=>setLeg(l.id,{include:!l.include})}
         style={{width:22,height:22,borderRadius:5,borderWidth:1.5,borderColor:l.include?C.green:C.muted,backgroundColor:l.include?C.green:'transparent',alignItems:'center',justifyContent:'center'}}>{l.include&&<Icon name="check" size={13} color="#041B12"/>}</Pressable>
        <Pressable accessibilityRole="button" accessibilityLabel={`Side: ${l.side==='B'?'Buy':'Sell'}. Switch`} onPress={()=>setLeg(l.id,{side:l.side==='B'?'S':'B'})}
         style={{paddingHorizontal:10,height:28,borderRadius:7,justifyContent:'center',backgroundColor:l.side==='B'?C.soft:'#2A1519',borderWidth:1,borderColor:l.side==='B'?C.green:C.red}}>
         <T style={{fontSize:11,fontFamily:'InterSemi',color:l.side==='B'?C.green:C.red}}>{l.side==='B'?'BUY':'SELL'}</T></Pressable>
        <Stepper label={`${strikeText(l.strike)}`} a11y={`Strike ${strikeText(l.strike)}`} onMinus={()=>setLeg(l.id,{strike:moveStrike(l.strike,-1)})} onPlus={()=>setLeg(l.id,{strike:moveStrike(l.strike,1)})}/>
        <Pressable accessibilityRole="button" accessibilityLabel={`Type ${l.type}. Switch to ${l.type==='CE'?'PE':'CE'}`} onPress={()=>setLeg(l.id,{type:l.type==='CE'?'PE':'CE'})} style={tag}><T style={{fontSize:11,fontFamily:'InterSemi'}}>{l.type}</T></Pressable>
        <Stepper label={`${l.lots} lot${l.lots>1?'s':''}`} a11y={`${l.lots} lots`} onMinus={()=>l.lots>1&&setLeg(l.id,{lots:l.lots-1})} onPlus={()=>l.lots<500&&setLeg(l.id,{lots:l.lots+1})}/>
        <View style={{flex:1}}/>
        <Pressable accessibilityRole="button" accessibilityLabel={`Remove ${strikeText(l.strike)} ${l.type}`} onPress={()=>removeLeg(l.id)} style={{padding:6}}><Icon name="trash-2" size={15} color={C.muted}/></Pressable>
       </View>
       <View style={[s.row,{flexWrap:'wrap',gap:14}]}>
        <PriceField leg={l} ltp={ltp} quote={a?.legs_quotes?.find(q=>q.id===l.id)} onManual={(v)=>setLeg(l.id,{price_basis:'manual',price:v})} onBasis={(b)=>setLeg(l.id,{price_basis:b,price:null})}/>
        <Mini k="IV" v={row?.iv!=null?`${row.iv}%`:'—'}/><Mini k="Δ" v={row?.greeks?num(row.greeks.delta,1):'—'}/>
        <Mini k="Units" v={row?String(row.units):String(l.lots*(chain?.lot_size||0))}/>
       </View>
      </View>;})}
    {undo&&<View style={[s.between,{backgroundColor:C.paper,borderRadius:10,padding:10}]}><T style={{fontSize:12}}>{`Removed ${strikeText(undo.leg.strike)} ${undo.leg.type}`}</T>
     <Button label="Undo" kind="outline" onPress={()=>{const u=undo;setUndo(null);edit(b=>{const legs=[...b.legs];legs.splice(Math.min(u.index,legs.length),0,u.leg);return {...b,legs};});}}/></View>}
    {body.legs.length>0&&<View style={[s.row,{flexWrap:'wrap',gap:10,borderTopWidth:1,borderColor:C.line,paddingTop:10}]}>
     <T style={label}>Adjust all</T>
     <Stepper label="Shift" a11y="Shift all strikes" onMinus={()=>adjust('shift',-1)} onPlus={()=>adjust('shift',1)}/>
     <Stepper label="Width" a11y="Width between legs" onMinus={()=>adjust('width',-1)} onPlus={()=>adjust('width',1)}/>
     <Stepper label="Wings" a11y="Hedge wing distance" onMinus={()=>adjust('wings',-1)} onPlus={()=>adjust('wings',1)}/>
    </View>}
   </View>

   {/* RIGHT: analysis */}
   <View style={{flex:wide?55:undefined,width:wide?undefined:'100%',gap:16}}>
    <RiskStrip a={a} dim={dim}/>
    <View style={panel}>
     <View style={s.between}><T style={label}>Payoff</T>{pending&&<T style={{fontSize:11,color:C.amber}}>{a?'Updating…':'Calculating…'}</T>}</View>
     <PayoffChart curve={a?.curve||[]} spot={a?.spot||chain?.spot||0} scenarioSpot={a?.scenario?.spot} breakevens={a?.breakevens?.value||[]} bands={a?.sd?.bands||[]} dim={dim}
      scenarioLabel={a?.scenario?.is_expiry?'Scenario (expiry)':`Scenario ${a?.scenario?istStamp(a.scenario.at):''}`}/>
     <ScenarioBar body={body} chain={chain} expiry={body.expiry} onChange={sc=>edit(b=>({...b,scenario:sc}))} result={a}/>
    </View>
    {!!a?.warnings?.length&&<View style={[panel,{borderColor:'#5A4A1F',backgroundColor:C.amberBg,gap:6}]}>{a.warnings.map((w,i)=><View key={i} style={[s.row,{alignItems:'flex-start',gap:8}]}><Icon name="alert-triangle" size={13} color={C.amber}/><T style={{fontSize:12,color:C.amber,flex:1}}>{w}</T></View>)}</View>}
    <View style={panel}>
     <View style={[s.row,{flexWrap:'wrap',gap:6}]}>
      {(['pnl','greeks','table','snapshots','paper','alerts','activity'] as Tab[]).map(t=><Chip key={t} active={tab===t} onPress={()=>setTab(t)}
       label={{pnl:'P&L by leg',greeks:'Greeks',table:'Payoff table',snapshots:`Snapshots (${detail.snapshots.length})`,paper:`Paper (${runs.length+deps.length})`,alerts:`Alerts${bell.count?` (${bell.count})`:''}`,activity:'Activity'}[t]}/>)}
     </View>
     {tab==='pnl'&&<LegTable a={a}/>}
     {tab==='greeks'&&<GreeksTable a={a}/>}
     {tab==='table'&&<PayoffTable a={a}/>}
     {tab==='snapshots'&&<Snapshots detail={detail} onRestore={async(rid)=>{try{const s=await sb.restore(id,rid,versionRef.current);setBody(s.draft!.body);setVersion(s.draft!.version);flash('Snapshot restored as a new draft version.');}catch(e:any){flash(msg(e));reload();}}}
      onDuplicate={async(rid)=>{try{const c=await sb.duplicate(id,rid);router.replace({pathname:'/strategies',params:{id:c.id}} as any);}catch(e:any){flash(msg(e));}}}/>}
     {tab==='paper'&&<View style={{gap:10}}>{deps.map(d=><DeploymentCard key={d.id} d={d} onChanged={loadRuns} onClose={(x)=>setReview({open:true,closing:x})}/>)}{!deps.length&&st?.live&&<T style={{fontSize:12,color:C.muted}}>No paper deployments yet. "Review paper orders" builds the exact plan from live quotes.</T>}</View>}
     {tab==='paper'&&<PaperList runs={runs} onClose={async(run)=>{try{await sb.paperClose(run);flash('Paper run closed at the stored reading.');loadRuns();reload();}catch(e:any){flash(msg(e));}}} onOpen={()=>router.push({pathname:'/strategies',params:{view:'paper'}} as any)}/>}
     {tab==='alerts'&&<AlertsPanel strategyId={id} analysis={a} deployments={deps} expiry={body.expiry} onChanged={reload}/>}
     {tab==='activity'&&<View style={{gap:6}}>{detail.activity.map((x,i)=><View key={i} style={[s.between,{borderTopWidth:1,borderColor:C.line,paddingTop:6}]}><T style={{fontSize:12,flex:1}}>{x.detail}</T><T style={{fontSize:11,color:C.muted}}>{istEpoch(x.created_at)}</T></View>)}</View>}
    </View>
   </View>
  </View>

  <ChainDrawer visible={chainOpen} chain={chain} legs={body.legs} onClose={()=>setChainOpen(false)} onToggle={(k,kind,side)=>toggleFromChain(k,kind,side)}/>
  <OrderReview visible={review.open} strategyId={id} deployment={review.closing||null} onClose={()=>setReview({open:false})}
   onPlaced={(d)=>{setReview({open:false});flash(`Paper ${review.closing?'close':'orders'} placed - deployment ${d.status.replace('_',' ')}. No order reached a broker.`);loadRuns();reload();setTab('paper');}}/>
  <TemplateSheet visible={tplOpen} onClose={()=>setTplOpen(false)} onPick={applyTemplate} replacing={body.legs.length}/>
  <PaperSheet visible={paperOpen} onClose={()=>setPaperOpen(false)} a={a} body={body} chain={chain}
   onStart={async()=>{try{await persist(body);const r=await sb.paperStart(id);setPaperOpen(false);flash(`Paper run started - ${r.fills.length} simulated fills. No order was sent.`);loadRuns();reload();setTab('paper');}catch(e:any){flash(msg(e));}}}/>
  {!!toast&&<View style={{position:'fixed' as any,bottom:24,left:0,right:0,alignItems:'center',zIndex:50}}><View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.green,borderRadius:12,paddingHorizontal:16,paddingVertical:10,maxWidth:560}}><T style={{fontSize:13}}>{toast}</T></View></View>}
 </View>;

 function confirmSwitch(){return typeof window==='undefined'||window.confirm('Switching the underlying clears the current legs. Continue?');}
}

function ChipRow({wrap,children}:{wrap:boolean;children:React.ReactNode}){
 if(wrap)return <View style={[s.row,{flexWrap:'wrap',gap:8}]}>{children}</View>;
 return <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{gap:8,paddingRight:8}}>{children}</ScrollView>;
}
const panel={backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:12,padding:14,gap:12};
const label={fontFamily:'InterMedium',fontSize:10,letterSpacing:.6,textTransform:'uppercase' as const,color:C.muted};
const tag={paddingHorizontal:9,height:28,borderRadius:7,borderWidth:1,borderColor:C.line,justifyContent:'center' as const};

function SaveBadge({state,version}:{state:SaveState;version:number}){
 const m={saved:['green',`Draft v${version} · saved`],dirty:['neutral','Unsaved changes…'],saving:['neutral','Saving…'],conflict:['amber','Newer copy loaded'],error:['red','Not saved - retrying on next edit']}[state];
 return <Badge tone={m[0]} label={m[1]} dot/>;
}
function DataBanner({reading,live,status}:{reading?:string;live:boolean;status:Status|null}){
 return <View style={[s.row,{flexWrap:'wrap',gap:8,backgroundColor:live?C.soft:C.amberBg,borderRadius:10,paddingHorizontal:12,paddingVertical:8}]}>
  <Icon name={live?'radio':'clock'} size={13} color={live?C.green:C.amber}/>
  <T style={{fontSize:12,color:live?C.green:C.amber,flex:1}}>{live?`Live · Zerodha Kite · quotes ${istStamp(reading)} · refreshes every 10 s · market ${status?.market_open?'open':'closed'}. Prices: buy at the ask, sell at the bid. Orders here are paper only.`:`Stored reading · ${istStamp(reading)} · not live${status?.reason?` (${status.reason})`:''}. Prices are last traded (no bid/ask). Research and paper only - nothing here sends an order.`}</T>
 </View>;
}
function Stepper({label:l,a11y,onMinus,onPlus}:{label:string;a11y:string;onMinus:()=>void;onPlus:()=>void}){
 const b=(icon:string,fn:()=>void,what:string)=><Pressable accessibilityRole="button" accessibilityLabel={`${a11y}: ${what}`} onPress={fn} style={({pressed})=>({width:26,height:28,alignItems:'center',justifyContent:'center',opacity:pressed?.6:1})}><Icon name={icon} size={13} color={C.muted}/></Pressable>;
 return <View style={[s.row,{gap:0,borderWidth:1,borderColor:C.line,borderRadius:7,height:28}]}>{b('minus',onMinus,'decrease')}<T style={{fontSize:12,fontFamily:'InterMedium',minWidth:44,textAlign:'center',fontVariant:['tabular-nums'] as any}}>{l}</T>{b('plus',onPlus,'increase')}</View>;
}
function Mini({k,v}:{k:string;v:string}){return <View style={[s.row,{gap:5}]}><T style={{fontSize:11,color:C.muted}}>{k}</T><T style={{fontSize:12,fontVariant:['tabular-nums'] as any}}>{v}</T></View>;}
const NEXT:Record<string,Basis>={exec:'mid',mid:'ltp',ltp:'exec',manual:'exec'};
function PriceField({leg,ltp,quote,onManual,onBasis}:{leg:Leg;ltp:number|null;quote?:{bid:number|null;ask:number|null;ltp:number|null;basis_used:string};onManual:(v:number)=>void;onBasis:(b:Basis)=>void}){
 const onLtp=()=>onBasis('exec');
 const shown=quote?(quote.basis_used==='exec'?(leg.side==='B'?quote.ask:quote.bid):quote.basis_used==='mid'&&quote.bid&&quote.ask?Math.round((quote.bid+quote.ask)*50)/100:quote.ltp):ltp;
 const tag=leg.price_basis==='manual'?'manual':quote?.basis_used==='exec'?(leg.side==='B'?'ask':'bid'):quote?.basis_used||leg.price_basis;
 const [text,setText]=useState(leg.price_basis==='manual'?String(leg.price):'');
 useEffect(()=>{setText(leg.price_basis==='manual'?String(leg.price):'');},[leg.price_basis,leg.price]);
 const commit=()=>{const v=Number(text);if(text.trim()===''){onLtp();return;}if(Number.isFinite(v)&&v>=0)onManual(v);};
 return <View style={[s.row,{gap:6}]}>
  <T style={{fontSize:11,color:C.muted}}>Entry</T>
  <TextInput value={leg.price_basis==='manual'?text:(shown!=null?String(shown):'')} onChangeText={setText} onFocus={()=>{if(leg.price_basis!=='manual')setText(shown!=null?String(shown):'');}}
   onBlur={commit} onSubmitEditing={commit} keyboardType="decimal-pad" accessibilityLabel={`Entry price for ${strikeText(leg.strike)} ${leg.type}`}
   style={{width:74,height:28,borderWidth:1,borderColor:leg.price_basis==='manual'?C.amber:C.line,borderRadius:7,paddingHorizontal:8,color:C.ink,fontFamily:'Inter',fontSize:12}}/>
  <Pressable accessibilityRole="button" accessibilityLabel={`Price basis ${tag}. Change`} onPress={()=>onBasis(NEXT[leg.price_basis])} style={{paddingHorizontal:6,paddingVertical:3,borderRadius:6,borderWidth:1,borderColor:leg.price_basis==='manual'?C.amber:C.line}}><T style={{fontSize:11,color:leg.price_basis==='manual'?C.amber:C.muted}}>{tag}{leg.price_basis!=='manual'&&quote?.bid&&quote?.ask?` · ${quote.bid}/${quote.ask}`:''}</T></Pressable>
 </View>;
}

function RiskStrip({a,dim}:{a:Analysis|null;dim:boolean}){
 if(!a||a.status==='no_market'||a.status==='empty')return <View style={[panel,{minHeight:84,justifyContent:'center'}]}><T style={{color:C.muted,fontSize:12}}>{a?.warnings?.[0]||'Risk numbers appear once the strategy has legs.'}</T></View>;
 if(a.status==='invalid')return <View style={panel}><T style={{color:C.red,fontSize:12}}>{a.warnings[0]}</T></View>;
 const money=(m?:any,prefix='')=>!m?'—':m.status!=='available'?(m.status==='unsupported'?'Not supported':'Unavailable'):m.unlimited?'Unlimited':signed(m.value);
 const items=[
  ['Max loss',money(a.max_loss),a.max_loss?.unlimited?C.red:C.red,'At expiry, gross of charges'],
  ['Max profit',money(a.max_profit),C.green,'At expiry, gross of charges'],
  ['Breakeven',a.breakevens?.status==='available'?(a.breakevens.value.length?a.breakevens.value.map((b:number)=>num(b,0)).join(' · '):'None'):'—',C.ink,'At expiry'],
  ['Capital at risk',a.capital_at_risk?.unlimited?'Unlimited':inr(a.capital_at_risk?.value),C.ink,'Structural max loss - not exchange margin'],
  [a.premium?.direction==='credit'?'Net credit':'Net debit',inr(Math.abs(a.premium?.value||0)),C.ink,'At entry prices'],
  ['POP (model)',a.pop?.status==='available'?`${a.pop.value}%`:'—',C.ink,a.pop?.sigma?`Lognormal at ${a.pop.sigma}% ATM IV`:'Model value'],
  ['Charges (est.)',inr(a.charges?.value),C.ink,'Entry orders, published rates'],
  ['Margin',a.margin?.status==='available'?inr(a.margin.value):'Needs live data',a.margin?.status==='available'?C.ink:C.muted,a.margin?.status==='available'?`Kite SPAN+exposure · hedge benefit ${inr(a.margin.hedge_benefit)}`:'Exchange margin comes from Kite when live'],
 ] as const;
 return <View style={[panel,{flexDirection:'row',flexWrap:'wrap',gap:14,opacity:dim?.5:1}]}>
  {items.map(([k,v,col,help])=><View key={k} style={{minWidth:130,flex:1,gap:2}} accessibilityLabel={`${k}: ${v}. ${help}`}>
   <T style={{fontSize:11,color:C.muted}}>{k}</T><T style={{fontFamily:'InterSemi',fontSize:16,color:col,fontVariant:['tabular-nums'] as any}}>{v}</T><T style={{fontSize:10,color:C.muted}}>{help}</T></View>)}
 </View>;
}

function ScenarioBar({body,chain,expiry,onChange,result}:{body:Body;chain:Chain|null;expiry:string;onChange:(sc:Body['scenario'])=>void;result:Analysis|null}){
 const sc=body.scenario||{};const spot=chain?.spot||0;
 const [spotText,setSpotText]=useState(sc.spot?String(sc.spot):'');
 useEffect(()=>{setSpotText(sc.spot?String(sc.spot):'');},[sc.spot]);
 const readingDay=(chain?.as_of||'').slice(0,10);
 const days=useMemo(()=>{if(!readingDay||!expiry)return [] as string[];const out:string[]=[];let d=readingDay;for(let i=0;i<60&&d<=expiry;i++){if(!isWeekend(d))out.push(d);d=addDays(d,1);}if(!out.includes(expiry))out.push(expiry);return out;},[readingDay,expiry]);
 const atDay=(sc.at||'').slice(0,10)||readingDay;const idx=Math.max(0,days.indexOf(atDay));
 const setDay=(i:number)=>{const d=days[Math.max(0,Math.min(days.length-1,i))];if(!d)return;onChange({...sc,at:d===readingDay?undefined:`${d} 15:30`});};
 const commitSpot=()=>{const v=Number(spotText);onChange({...sc,spot:spotText.trim()===''||!Number.isFinite(v)||v<=0?undefined:v});};
 const pct=sc.spot&&spot?((sc.spot/spot-1)*100):0;
 return <View style={{gap:10,borderTopWidth:1,borderColor:C.line,paddingTop:10}}>
  <T style={label}>Scenario - hypothetical, not a forecast</T>
  <View style={[s.row,{flexWrap:'wrap',gap:14}]}>
   <View style={[s.row,{gap:6}]}><T style={{fontSize:12,color:C.muted}}>{chain?.underlying||'Spot'} at</T>
    <Stepperish what="price" onMinus={()=>onChange({...sc,spot:Math.round(((sc.spot||spot)-(chain?.strike_step||50))*100)/100})} onPlus={()=>onChange({...sc,spot:Math.round(((sc.spot||spot)+(chain?.strike_step||50))*100)/100})}>
     <TextInput value={spotText} placeholder={num(spot,2)} placeholderTextColor={C.muted} onChangeText={setSpotText} onBlur={commitSpot} onSubmitEditing={commitSpot} keyboardType="decimal-pad"
      accessibilityLabel="Scenario underlying price" style={{width:90,height:28,color:C.ink,fontFamily:'Inter',fontSize:12,textAlign:'center'}}/></Stepperish>
    <T style={{fontSize:11,color:C.muted,minWidth:48}}>{sc.spot?`${pct>=0?'+':''}${pct.toFixed(2)}%`:'current'}</T></View>
   <View style={[s.row,{gap:6}]}><T style={{fontSize:12,color:C.muted}}>on</T>
    <Stepperish what="date" onMinus={()=>setDay(idx-1)} onPlus={()=>setDay(idx+1)}><T style={{fontSize:12,minWidth:110,textAlign:'center'}}>{atDay===readingDay?`${weekday(atDay)} ${dayMonth(atDay)} (now)`:`${weekday(atDay)} ${dayMonth(atDay)} 15:30`}</T></Stepperish>
    <T style={{fontSize:11,color:C.muted}}>{result?.scenario?`${result.scenario.days_to_expiry}d to expiry`:''}</T></View>
   <View style={[s.row,{gap:6}]}><T style={{fontSize:12,color:C.muted}}>IV</T>
    <Stepperish what="IV shift" onMinus={()=>onChange({...sc,iv_shift:(sc.iv_shift||0)-1})} onPlus={()=>onChange({...sc,iv_shift:(sc.iv_shift||0)+1})}><T style={{fontSize:12,minWidth:52,textAlign:'center'}}>{`${(sc.iv_shift||0)>0?'+':''}${sc.iv_shift||0} pts`}</T></Stepperish></View>
   <Button label="Reset" kind="outline" onPress={()=>onChange({})} style={{minHeight:32,paddingVertical:4}}/>
  </View>
  <T style={{fontSize:13,fontFamily:'InterSemi',color:(result?.scenario_pnl?.value||0)>=0?C.green:C.red}}>
   {result?.scenario_pnl?.status==='available'?`Scenario P&L: ${signed(result.scenario_pnl.value)} ${result.scenario?.is_expiry?'(at expiry)':'(model, Black-Scholes)'} · weekends skipped; exchange holidays are not`:
    result?.scenario_pnl?`Scenario P&L unavailable: ${result.scenario_pnl.reason}`:''}
  </T>
 </View>;
}
function Stepperish({children,onMinus,onPlus,what='value'}:any){return <View style={[s.row,{gap:0,borderWidth:1,borderColor:C.line,borderRadius:7,height:30}]}>
 <Pressable accessibilityRole="button" accessibilityLabel={`Decrease scenario ${what}`} onPress={onMinus} style={{width:26,height:28,alignItems:'center',justifyContent:'center'}}><Icon name="minus" size={13} color={C.muted}/></Pressable>
 {children}<Pressable accessibilityRole="button" accessibilityLabel={`Increase scenario ${what}`} onPress={onPlus} style={{width:26,height:28,alignItems:'center',justifyContent:'center'}}><Icon name="plus" size={13} color={C.muted}/></Pressable></View>;}

function Table({head,rows,right=[]}:{head:string[];rows:(string|number)[][];right?:number[]}){
 return <View>{[head,...rows].map((r,i)=><View key={i} style={[s.row,{gap:8,paddingVertical:6,borderTopWidth:i?1:0,borderColor:C.line}]}>
  {r.map((c,j)=><T key={j} style={{flex:j===0?2:1,fontSize:i?12:10,color:i?C.ink:C.muted,textAlign:right.includes(j)?'right':'left',fontFamily:i?'Inter':'InterMedium',
   textTransform:i?'none':'uppercase',fontVariant:['tabular-nums'] as any}}>{String(c)}</T>)}</View>)}</View>;
}
function LegTable({a}:{a:Analysis|null}){
 if(!a?.legs?.length)return <T style={{fontSize:12,color:C.muted}}>No legs to value.</T>;
 const tot=a.legs.reduce((x,l)=>x+(l.target_pnl||0),0);
 return <View style={{gap:6}}><Table head={['Leg','Units','Entry','Scenario price','Scenario P&L']} right={[1,2,3,4]}
  rows={[...a.legs.map(l=>[l.label,l.units,num(l.entry),num(l.target_price),signed(l.target_pnl)]),['Total (gross)','','','',signed(tot)]]}/>
  <T style={{fontSize:11,color:C.muted}}>{`Scenario: ${a.scenario?num(a.scenario.spot,2):''} on ${a.scenario?istStamp(a.scenario.at):''}. Entry = last traded price unless marked manual. Charges ${inr(a.charges?.value)} not included.`}</T></View>;
}
function GreeksTable({a}:{a:Analysis|null}){
 if(!a?.legs?.length)return <T style={{fontSize:12,color:C.muted}}>No legs.</T>;
 if(a.greeks?.status!=='available')return <T style={{fontSize:12,color:C.amber}}>{`Greeks unavailable: ${a.greeks?.reason||''}`}</T>;
 return <View style={{gap:6}}><Table head={['Leg','Delta','Gamma','Theta ₹/day','Vega ₹/IV pt']} right={[1,2,3,4]}
  rows={[...a.legs.map(l=>[l.label,num(l.greeks?.delta,1),num(l.greeks?.gamma,3),num(l.greeks?.theta,0),num(l.greeks?.vega,0)]),['Strategy total',num(a.greeks.delta,1),num(a.greeks.gamma,3),num(a.greeks.theta,0),num(a.greeks.vega,0)]]}/>
  <T style={{fontSize:11,color:C.muted}}>Whole-strategy units (lots × lot size). Delta in underlying units per 1 point; theta per calendar day; vega per 1 percentage point of IV. Model values at the reading.</T></View>;
}
function PayoffTable({a}:{a:Analysis|null}){
 if(!a?.table?.length)return <T style={{fontSize:12,color:C.muted}}>No payoff table yet.</T>;
 return <Table head={[`${a.underlying||'Underlying'} level`,'% from spot','Scenario date','At expiry']} right={[1,2,3]} rows={a.table.map(r=>[num(r.s,0),`${r.pct>0?'+':''}${r.pct}%`,signed(r.target),signed(r.expiry)])}/>;
}
function Snapshots({detail,onRestore,onDuplicate}:{detail:Detail;onRestore:(id:string)=>void;onDuplicate:(id:string)=>void}){
 if(!detail.snapshots.length)return <T style={{fontSize:12,color:C.muted}}>No snapshots yet. "Save snapshot" freezes the current legs and their analysis; it never changes afterwards.</T>;
 return <View style={{gap:8}}>{detail.snapshots.map(x=><View key={x.id} style={[s.between,{borderTopWidth:1,borderColor:C.line,paddingTop:8,flexWrap:'wrap'}]}>
  <View style={{gap:2,flex:1,minWidth:200}}><T style={{fontSize:13,fontFamily:'InterSemi'}}>{`#${x.n} ${x.name}`}</T>
   <T style={{fontSize:11,color:C.muted}}>{`Priced at ${istStamp(x.reading_at)} · max loss ${x.summary.unlimited_loss?'unlimited':signed(x.summary.max_loss)} · max profit ${signed(x.summary.max_profit)} · ${x.checksum.slice(0,8)}`}</T></View>
  <View style={[s.row,{gap:6}]}><Button label="Restore" kind="outline" onPress={()=>onRestore(x.id)}/><Button label="Copy to new" kind="outline" onPress={()=>onDuplicate(x.id)}/></View></View>)}</View>;
}
function PaperList({runs,onClose,onOpen}:{runs:PaperRun[];onClose:(id:string)=>void;onOpen:()=>void}){
 if(!runs.length)return <T style={{fontSize:12,color:C.muted}}>No paper runs for this strategy. "Paper trade" records simulated fills against a frozen snapshot - it never sends an order.</T>;
 return <View style={{gap:10}}>{runs.map(r=><View key={r.id} style={{borderTopWidth:1,borderColor:C.line,paddingTop:8,gap:4}}>
  <View style={s.between}><View style={[s.row,{gap:8}]}><Badge label={r.status==='open'?'PAPER · OPEN':'PAPER · CLOSED'} tone={r.status==='open'?'green':'neutral'}/><T style={{fontSize:12,color:C.muted}}>{`Snapshot #${r.revision.n} · opened ${istStamp(r.opened_reading)}`}</T></View>
   {r.status==='open'&&<Button label="Close (simulated)" kind="outline" onPress={()=>onClose(r.id)}/>}</View>
  <T style={{fontSize:13}}>{`Net ${signed(r.net)} (realised ${signed(r.realised)}, unrealised ${signed(r.unrealised)}, fees ${inr(r.fees)})`}{r.close_now_estimate!=null?` · if closed now ≈ ${signed(r.close_now_estimate)}`:''}</T>
 </View>)}<Button label="All paper runs" kind="outline" icon="list" onPress={onOpen}/></View>;
}

function TemplateSheet({visible,onClose,onPick,replacing}:{visible:boolean;onClose:()=>void;onPick:(t:Template,p:number|null)=>void;replacing:number}){
 const [data,setData]=useState<{templates:Template[];later:any[]}|null>(null);const [adv,setAdv]=useState(false);const [param,setParam]=useState<Record<string,number>>({});
 useEffect(()=>{if(visible&&!data)sb.templates().then(setData).catch(()=>{});},[visible,data]);
 const groups=[['bullish','Bullish'],['bearish','Bearish'],['range','Range'],['volatility','Big move']] as const;
 return <Sheet visible={visible} onClose={onClose} wide title="Choose a template" subtitle={replacing?`Using a template replaces the ${replacing} current leg${replacing>1?'s':''} (undo with a snapshot restore).`:'A template is a recipe; it becomes exact contracts from the current chain, all editable.'}>
  {!data?<Loading/>:<>
   {groups.map(([k,title])=>{const list=data.templates.filter(t=>t.intent===k&&(adv||t.risk==='defined'));if(!list.length)return null;
    return <View key={k} style={{gap:8}}><T style={label}>{title}</T>{list.map(t=><View key={t.key} style={[panel,{gap:6}]}>
     <View style={s.between}><T style={{fontFamily:'InterSemi'}}>{t.name}</T><Badge label={t.risk==='defined'?'Defined risk':'UNHEDGED'} tone={t.risk==='defined'?'green':'red'}/></View>
     <T style={{fontSize:12,color:C.muted}}>{t.recipe}</T><T style={{fontSize:12}}>{`Use: ${t.use}`}</T><T style={{fontSize:12,color:C.amber}}>{`Loses when: ${t.loses}`}</T>
     <View style={[s.row,{flexWrap:'wrap',gap:6}]}>
      {t.param&&<><T style={{fontSize:11,color:C.muted}}>{t.param.label}</T>{t.param.variants.map(v=><Chip key={v} label={String(v)} active={(param[t.key]??t.param!.default)===v} onPress={()=>setParam(p=>({...p,[t.key]:v}))}/>)}</>}
      <View style={{flex:1}}/><Button label="Use" icon="arrow-right" onPress={()=>onPick(t,t.param?(param[t.key]??t.param.default):null)}/></View>
    </View>)}</View>;})}
   <Pressable accessibilityRole="button" onPress={()=>setAdv(!adv)}><T style={{fontSize:12,color:C.green}}>{adv?'Hide unhedged structures':'Show unhedged structures (short options - large or unlimited loss)'}</T></Pressable>
   <View style={{gap:4}}><T style={label}>Later</T>{data.later.map(l=><T key={l.key} style={{fontSize:12,color:C.muted}}>{`${l.name} - ${l.reason}`}</T>)}</View>
  </>}
 </Sheet>;
}

function PaperSheet({visible,onClose,a,body,chain,onStart}:{visible:boolean;onClose:()=>void;a:Analysis|null;body:Body;chain:Chain|null;onStart:()=>void}){
 const [busy,setBusy]=useState(false);
 const unlimited=!!a?.max_loss?.unlimited;
 return <Sheet visible={visible} onClose={onClose} title="Start a paper run" subtitle="Simulated fills recorded against a frozen snapshot. No order is sent to any broker."
  footer={<View style={[s.row,{justifyContent:'flex-end',gap:8}]}><Button label="Cancel" kind="outline" onPress={onClose}/><Button label="Record simulated fills" icon="play" loading={busy} onPress={async()=>{setBusy(true);await onStart();setBusy(false);}}/></View>}>
  <View style={{gap:8}}>
   {body.legs.filter(l=>l.include).map(l=>{const ltp=chain?.rows.find(r=>r.strike===l.strike)?.[l.type]?.ltp;
    return <View key={l.id} style={s.between}><T style={{fontSize:13}}>{`${l.side==='B'?'Buy':'Sell'} ${l.lots} × ${strikeText(l.strike)} ${l.type} · ${dayMonth(l.expiry)}`}</T><T style={{fontSize:12,color:C.muted}}>{`LTP ${num(ltp)} · fills ${l.side==='B'?'above':'below'} by slippage`}</T></View>;})}
  </View>
  <View style={[panel,{gap:6}]}>
   <T style={{fontSize:12}}>Fill policy: last traded price at the stored reading, moved against you by the larger of 0.5% or one tick. Estimated charges are recorded on every fill.</T>
   <T style={{fontSize:12,color:C.muted}}>{`Priced at ${istStamp(chain?.as_of)}. A snapshot named "Paper entry" is saved so the run always points at exactly these legs.`}</T>
   {unlimited&&<T style={{fontSize:12,color:C.red}}>This structure has unlimited loss. A paper run is a safe place to learn how that behaves.</T>}
  </View>
 </Sheet>;
}
