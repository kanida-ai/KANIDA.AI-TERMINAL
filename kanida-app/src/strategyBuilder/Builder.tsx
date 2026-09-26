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
import {AdjustSheet} from './Adjust';
import {AboutSheet,SpreadsSheet} from './Learn';
import {TemplateSheet} from './Templates';
import {ActionsSheet,ExpirySheet,LegSheet,SnapshotSheet,UnderlyingPicker,snap} from './BuilderParts';
import {ErrorRetry,Scrollable} from './States';
import {AlertsPanel,useAlertNotifications} from './Alerts';
import {ChainDrawer} from './ChainDrawer';
import {PayoffChart} from './PayoffChart';
import {legText,addDays,dayMonth,inr,isWeekend,istEpoch,istStamp,num,signed,strikeText,weekday} from './format';

const SAVE_DELAY=800,ANALYZE_DELAY=220;
const uid=()=>'L'+Math.random().toString(36).slice(2,8);
const msg=(e:any)=>e?.message||'KANIDA could not complete that.';
type SaveState='saved'|'dirty'|'saving'|'conflict'|'error';
type Tab='pnl'|'greeks'|'table'|'snapshots'|'paper'|'alerts'|'activity';

export function Builder({id,openTemplate=false,openAdjust=false,openChain=false}:{id:string;openTemplate?:boolean;openAdjust?:boolean;openChain?:boolean}){
 const {width}=useWindowDimensions();const wide=width>=1100;const coarse=width<700;
 const [detail,setDetail]=useState<Detail|null>(null);const [error,setError]=useState('');
 const [body,setBody]=useState<Body|null>(null);const [version,setVersion]=useState(0);const [save,setSave]=useState<SaveState>('saved');
 const [expiries,setExpiries]=useState<Expiry[]>([]);const [underlyings,setUnderlyings]=useState<{symbol:string;kind?:string}[]>([]);
 const [chain,setChain]=useState<Chain|null>(null);
 const [analysis,setAnalysis]=useState<Analysis|null>(null);const [pending,setPending]=useState(false);
 const [chainOpen,setChainOpen]=useState(openChain);const [tplOpen,setTplOpen]=useState(openTemplate);const [paperOpen,setPaperOpen]=useState(false);
 const [tab,setTab]=useState<Tab>('pnl');const [toast,setToast]=useState('');const [undo,setUndo]=useState<{leg:Leg;index:number}|null>(null);
 const [runs,setRuns]=useState<PaperRun[]>([]);const [name,setName]=useState('');
 const [st,setSt]=useState<Status|null>(null);const [tick,setTick]=useState(0);const [deps,setDeps]=useState<Deployment[]>([]);
 const [review,setReview]=useState<{open:boolean;closing?:Deployment|null;adjusting?:Deployment|null}>({open:false});
 const [adjustFor,setAdjustFor]=useState<{open:boolean;deployment?:Deployment|null}>({open:false});
 const [spreadsOpen,setSpreadsOpen]=useState(false);const [aboutOpen,setAboutOpen]=useState(false);
 // slice 11: undo/redo, previewed replacements and transforms, sheets (GTM audit P05, P12, P17, P18, P21)
 const undoStack=useRef<Body[]>([]);const redoStack=useRef<Body[]>([]);const [hist,setHist]=useState({u:0,r:0});
 const [change,setChange]=useState<{title:string;lines:string[];apply:(b:Body)=>Body;blocked?:string;base?:string}|null>(null);
 const setPendingChange=(c:{title:string;lines:string[];apply:(b:Body)=>Body;blocked?:string}|null)=>setChange(c?{...c,base:J(bodyRef.current)}:null);
 const [legEdit,setLegEdit]=useState<Leg|null>(null);const [actionsOpen,setActionsOpen]=useState(false);const [expiryOpen,setExpiryOpen]=useState(false);
 const [alertsOpen,setAlertsOpen]=useState(false);const [snapView,setSnapView]=useState<string|null>(null);
 const [chainErr,setChainErr]=useState('');const [chainRetry,setChainRetry]=useState(0);
 const adjustDeep=useRef(openAdjust);
 const bell=useAlertNotifications();
 const seq=useRef(0);const saveTimer=useRef<any>(null);const anTimer=useRef<any>(null);const ctl=useRef<AbortController|null>(null);
 const versionRef=useRef(0);versionRef.current=version;
 // GTM audit P01 - one ordered save lane per strategy. bodyRef is the body ON SCREEN (updated synchronously by every
 // edit, so a blur-commit right before a click is never missed); ackedJson is the body the server last acknowledged.
 // "Saved" means the two are identical. Every downstream action flushes and awaits the save first.
 const bodyRef=useRef<Body|null>(null);const ackedJson=useRef('');const checksumRef=useRef('');
 const saveQ=useRef<Promise<any>>(Promise.resolve());const saveRef=useRef<SaveState>('saved');saveRef.current=save;
 const [conflict,setConflict]=useState<{local:Body}|null>(null);const [anFailed,setAnFailed]=useState('');const [anRetry,setAnRetry]=useState(0);

 const flash=(t:string)=>{setToast(t);setTimeout(()=>setToast(x=>x===t?'':x),4200);};
 const reload=useCallback(async()=>{
  try{const d=await sb.get(id);setDetail(d);setName(d.name);
   // a reload never moves the draft backwards and never replaces unsaved work on screen (review F6): a slower GET
   // than the save that just landed, or any pending edit, keeps what is on screen
   const pendingEdit=!!bodyRef.current&&J(bodyRef.current)!==ackedJson.current;
   if(d.draft&&(pendingEdit||d.draft.version<versionRef.current)){}
   else if(d.draft){const b=d.draft.body;clearHistory();bodyRef.current=b;ackedJson.current=J(b);checksumRef.current=d.draft.checksum||'';versionRef.current=d.draft.version;
    setBody(b);setVersion(d.draft.version);setSave('saved');
    // an edit that never reached the server (reload, lost network, closed tab) is recovered, never silently dropped
    const loc=readLocal(id);
    if(loc&&J(loc.body)!==ackedJson.current){
     if(loc.version===d.draft.version){bodyRef.current=loc.body;setBody(loc.body);setSave('dirty');clearTimeout(saveTimer.current);
      saveTimer.current=setTimeout(()=>persistRef.current(loc.body),SAVE_DELAY);flash('Recovered edits that had not been saved yet.');}
     else{setConflict({local:loc.body});setSave('conflict');}}}
   else setSave('saved');}
  catch(e:any){setError(msg(e));}
 },[id]);// eslint-disable-line react-hooks/exhaustive-deps
 const loadRuns=useCallback(async()=>{try{const r=await sb.paperList();setRuns(r.runs.filter(x=>x.strategy_id===id));}catch{}
  try{const d=await exec.list();setDeps(d.deployments.filter(x=>x.strategy_id===id));}catch{}},[id]);
 useEffect(()=>{let live=true;const poll=()=>exec.status().then(x=>{if(live)setSt(x);}).catch(()=>{});poll();const t=setInterval(()=>{poll();setTick(n=>n+1);},10000);return()=>{live=false;clearInterval(t);};},[]);
 useEffect(()=>{if(deps.some(d=>!['closed','cancelled'].includes(d.status)))loadRuns();},[tick]);// eslint-disable-line react-hooks/exhaustive-deps
 useEffect(()=>{reload();loadRuns();sb.underlyings().then(r=>setUnderlyings(r.underlyings)).catch(()=>{});},[reload,loadRuns]);

 // an alert's "Adjust" link lands here: open the assistant once the strategy (and its deployments) have loaded
 useEffect(()=>{if(!adjustDeep.current||!detail||!body?.legs?.length)return;adjustDeep.current=false;
  setAdjustFor({open:true,deployment:deps.find(d=>d.status==='active')||null});},[detail,deps]);// eslint-disable-line react-hooks/exhaustive-deps

 // expiries and chain follow the draft's underlying/expiry
 useEffect(()=>{if(!body?.underlying)return;let live=true;
  sb.expiries(body.underlying).then(r=>{if(!live)return;setExpiries(r.expiries);
   // default to the first expiry at least a day away - never same-day (0DTE) by default; it stays one click away
   const first=r.expiries.find(x=>(x.days_to_expiry??0)>=1)||r.expiries[0];
   if(!body.expiry&&first)edit(b=>({...b,expiry:first.expiry}));}).catch(e=>setError(msg(e)));
  return()=>{live=false};},[body?.underlying]);// eslint-disable-line react-hooks/exhaustive-deps
 useEffect(()=>{if(!body?.underlying||!body.expiry){setChain(null);return;}const c=new AbortController();
  sb.chain(body.underlying,body.expiry,c.signal).then(x=>{setChain(x);setChainErr('');}).catch(e=>{if(e?.name!=='AbortError')setChainErr(msg(e));});
  return()=>c.abort();},[body?.underlying,body?.expiry,st?.live?tick:0,chainRetry]);

 // analysis: the request is numbered AT THE EDIT (not after the debounce), so a late answer for an older body can never
 // clear "updating" or land as the current result. A failed recalculation keeps the old numbers visibly stale.
 useEffect(()=>{if(!body)return;clearTimeout(anTimer.current);const mine=++seq.current;setPending(true);
  anTimer.current=setTimeout(async()=>{if(mine!==seq.current)return;ctl.current?.abort();const c=new AbortController();ctl.current=c;
   try{const a=await sb.analyze(body,c.signal);if(mine===seq.current){setAnalysis(a);setPending(false);setAnFailed('');}}
   catch(e:any){if(e?.name!=='AbortError'&&mine===seq.current){setPending(false);setAnFailed(msg(e));}}},ANALYZE_DELAY);
  return()=>clearTimeout(anTimer.current);},[body,st?.live?tick:0,anRetry]);

 // autosave - serialized; resolves true only when the server acknowledged exactly this body
 const persist=useCallback((next:Body):Promise<boolean>=>{
  const run=async()=>{const j=J(next);
   if(saveRef.current==='conflict')return false;
   if(j===ackedJson.current){const clean=J(bodyRef.current)===j;setSave(clean?'saved':'dirty');if(clean)clearLocal(id);return true;}
   setSave('saving');
   try{const s=await sb.save(id,versionRef.current,next);versionRef.current=s.draft!.version;ackedJson.current=j;checksumRef.current=s.draft!.checksum||'';
    setVersion(s.draft!.version);const clean=J(bodyRef.current)===j;setSave(clean?'saved':'dirty');
    if(clean)clearLocal(id);else writeLocal(id,s.draft!.version,bodyRef.current!);return true;}
   catch(e:any){if(e instanceof ApiError&&e.status===409){saveRef.current='conflict';setSave('conflict');setConflict({local:next});return false;}
    setSave('error');return false;}};
  const p=saveQ.current.then(run,run);saveQ.current=p.catch(()=>false);return p;},[id]);
 const persistRef=useRef(persist);persistRef.current=persist;
 function edit(fn:(b:Body)=>Body,history=true){const prev=bodyRef.current;if(!prev)return;const next=fn(prev);if(next===prev)return;
  if(history){undoStack.current=[...undoStack.current.slice(-49),prev];redoStack.current=[];setHist({u:undoStack.current.length,r:0});}
  bodyRef.current=next;setBody(next);writeLocal(id,versionRef.current,next);
  if(saveRef.current==='conflict')return;           // keep editing locally; the conflict banner decides where it goes
  setSave('dirty');clearTimeout(saveTimer.current);saveTimer.current=setTimeout(()=>persist(next),SAVE_DELAY);}
 function clearHistory(){undoStack.current=[];redoStack.current=[];setHist({u:0,r:0});}
 function undoEdit(){const prev=undoStack.current.pop();if(!prev||!bodyRef.current)return;redoStack.current.push(bodyRef.current);
  setHist({u:undoStack.current.length,r:redoStack.current.length});edit(()=>prev,false);}
 function redoEdit(){const next=redoStack.current.pop();if(!next||!bodyRef.current)return;undoStack.current.push(bodyRef.current);
  setHist({u:undoStack.current.length,r:redoStack.current.length});edit(()=>next,false);}
 /** Flush the pending edit and wait for the server to acknowledge it. False = not saved (the caller must not act). */
 async function flush():Promise<boolean>{blurActive();clearTimeout(saveTimer.current);const cur=bodyRef.current;if(!cur)return false;
  const ok=await persist(cur);return ok&&J(bodyRef.current)===ackedJson.current;}
 async function afterSave(fn:()=>void){if(await flush())fn();
  else flash(saveRef.current==='conflict'?'Resolve the version conflict first - your edits are kept.':'Your latest edit is not saved yet, so nothing was done. Use "Retry now".');}
 // leaving the screen never drops an edit: the pending save is sent (not cancelled), and a local copy survives a reload
 useEffect(()=>()=>{clearTimeout(saveTimer.current);if(bodyRef.current&&J(bodyRef.current)!==ackedJson.current&&saveRef.current!=='conflict')persistRef.current(bodyRef.current);},[]);
 useEffect(()=>{if(typeof window==='undefined'||!window.addEventListener)return;
  const h=(e:any)=>{if(bodyRef.current&&J(bodyRef.current)!==ackedJson.current){e.preventDefault();e.returnValue='';}};
  window.addEventListener('beforeunload',h);return()=>window.removeEventListener('beforeunload',h);},[]);

 // a leg's price comes from ITS contract (fresh audit P01): the chain on screen is one expiry, so a later-expiry leg is
 // priced from the analysis' own per-contract quote, never from the same strike in the near chain
 const priceOf=(k:number,kind:Kind,exp?:string,lid?:string)=>(!exp||exp===chain?.expiry)?(chain?.rows.find(r=>r.strike===k)?.[kind]?.ltp??null):(analysis?.legs_quotes?.find(q=>q.id===lid)?.ltp??null);
 const strikes=useMemo(()=>chain?.rows.map(r=>r.strike)??[],[chain]);
 const moveStrike=(k:number,steps:number)=>{if(!strikes.length)return k;const i=strikes.indexOf(k);const j=Math.max(0,Math.min(strikes.length-1,(i<0?strikes.findIndex(x=>x>=k):i)+steps));return strikes[j];};

 function toggleFromChain(strike:number,kind:Kind,side:Side,expiry:string){
  // identity = (expiry, strike, type): the same strike in another expiry is a different contract (fresh audit P01)
  edit(b=>{const hit=b.legs.find(l=>(l.expiry||b.expiry)===expiry&&l.strike===strike&&l.type===kind);
   if(hit&&hit.side===side)return {...b,legs:b.legs.filter(l=>l!==hit)};
   if(hit)return {...b,legs:b.legs.map(l=>l===hit?{...l,side}:l)};
   if(b.legs.length>=8){flash('At most 8 legs are supported in this release.');return b;}
   return {...b,template:null,legs:[...b.legs,{id:uid(),type:kind,side,strike,lots:1,expiry,price_basis:'exec',price:null,include:true}]};});
 }
 const setLeg=(lid:string,patch:Partial<Leg>)=>edit(b=>({...b,template:null,legs:b.legs.map(l=>l.id===lid?{...l,...patch}:l)}));
 function removeLeg(lid:string){if(!body)return;const index=body.legs.findIndex(l=>l.id===lid);const leg=body.legs[index];setUndo({leg,index});
  edit(b=>({...b,legs:b.legs.filter(l=>l.id!==lid)}));setTimeout(()=>setUndo(u=>u?.leg.id===lid?null:u),6000);}
 // Size ×: step the strategy multiplier up or down, keeping every leg's ratio (a 1:2:1 fly stays 1:2:1)
 function resize(dir:1|-1){edit(b=>{const g=b.legs.reduce((x,l)=>{let a=x,c=l.lots;while(c){[a,c]=[c,a%c];}return a;},0)||1;
  const base=b.legs.map(l=>l.lots/g);const next=Math.max(1,g+dir);if(next===g||base.some(x=>x*next>500))return b;
  return {...b,legs:b.legs.map((l,i)=>({...l,lots:base[i]*next}))};});}
 function clearAll(){if(typeof window!=='undefined'&&window.confirm&&!window.confirm('Remove every leg? A snapshot keeps anything you want to restore.'))return;
  edit(b=>({...b,template:null,param:null,legs:[]}));}
 /** Why a whole-strategy transform does not fit this structure, or null (GTM audit P05: compatible structures only). */
 function transformBlock(kind:'shift'|'width'|'wings',b:Body):string|null{
  const act=b.legs;if(!act.length)return 'Add legs first.';
  const shorts=act.filter(l=>l.side==='S'),longs=act.filter(l=>l.side==='B');
  if(kind==='width'&&act.length<2)return 'Width needs at least two legs.';
  if(kind==='wings'&&(!shorts.length||!longs.length))return 'Wings needs short legs with long hedges (a spread, condor or butterfly).';
  return null;}
 function propose(kind:'shift'|'width'|'wings',k:number,b:Body):{legs:Leg[]}|{error:string}{
  const act=b.legs;const shorts=act.filter(l=>l.side==='S');const ref=(shorts.length?shorts:act).map(l=>l.strike);const centre=ref.reduce((a,x)=>a+x,0)/ref.length;
  const legs:Leg[]=[];
  for(const l of act){let steps=0;
   if(kind==='shift')steps=k;
   else if(kind==='width'){if(l.side==='S'||!shorts.length)steps=(l.strike>centre||(l.strike===centre&&l.type==='CE'))?k:-k;else steps=(l.strike>centre?k:-k);}
   else if(kind==='wings'&&l.side==='B'&&shorts.length)steps=l.strike>centre?k:-k;
   if(!steps){legs.push(l);continue;}
   const i=strikes.indexOf(l.strike);const j=i+steps;
   if(i<0||j<0||j>=strikes.length)return {error:`${strikeText(l.strike)} ${l.type} would move past the listed strikes - nothing was changed.`};
   legs.push({...l,strike:strikes[j]});}
  const keys=legs.map(l=>`${l.expiry}|${l.type}${l.strike}${l.side}`);
  if(new Set(keys).size<keys.length)return {error:'Two legs would land on the same contract - nothing was changed.'};
  if(kind==='width'&&shorts.length){const ss=legs.filter(l=>l.side==='S');const bs=legs.filter(l=>l.side==='B');
   if(ss.some(x=>bs.some(y=>y.type===x.type&&y.strike===x.strike)))return {error:'The legs would cross - nothing was changed.'};}
  return {legs};}
 /** B1 linked strikes: with the link on, stepping one strike moves every leg together (the same move as Shift). */
 function stepStrike(l:Leg,k:number){
  const b=bodyRef.current;if(!b)return;
  if(!b.linked){const i=strikes.indexOf(l.strike);const j=i+k;if(i<0||j<0||j>=strikes.length){flash(`${strikeText(l.strike)} ${l.type} is at the edge of the listed strikes.`);return;}setLeg(l.id,{strike:strikes[j]});return;}
  const r=propose('shift',k,b);if('error' in r){flash(r.error);return;}
  edit(x=>({...x,template:null,legs:r.legs}));}
 /** B6: a warning's one-tap fix, always as a previewed change (undoable). */
 function applyFix(f:any){
  const b=bodyRef.current;if(!b||!f)return;
  if(f.action==='snap_tick'){setLeg(f.leg_id,{price_basis:'manual',price:f.price});flash(`Price set to ${f.price}, on the exchange tick.`);return;}
  if(f.action==='change_expiry'){setExpiryOpen(true);return;}
  if(f.action==='open_adjust'){afterSave(()=>setAdjustFor({open:true,deployment:deps.find(d=>d.status==='active')||null}));return;}
  if(f.action==='add_hedge'){
   const adds:Leg[]=[];
   for(const k of (f.types as Kind[])){
    const net=b.legs.filter(l=>l.type===k).reduce((x,l)=>x+(l.side==='B'?1:-1)*l.lots,0);if(net>=0)continue;
    const shortLegs=b.legs.filter(l=>l.type===k&&l.side==='S');const shorts=shortLegs.map(l=>l.strike);
    const far=k==='CE'?Math.max(...shorts):Math.min(...shorts);const i=strikes.indexOf(far);const j=k==='CE'?i+2:i-2;
    if(i<0||j<0||j>=strikes.length){flash(`No listed strike two steps beyond ${strikeText(far)} ${k} to hedge with.`);return;}
    // the hedge lives in the SAME expiry as the short it covers (a near hedge would expire before a far short - review)
    const hedgeExp=shortLegs.map(l=>l.expiry).sort().reverse()[0]||b.expiry;
    adds.push({id:uid(),type:k,side:'B',strike:strikes[j],lots:-net,expiry:hedgeExp,price_basis:'exec',price:null,include:true});}
   if(!adds.length){flash('Nothing to hedge.');return;}
   setPendingChange({title:'Add a hedge',lines:adds.map(l=>`Add: ${legText(l)} (two strikes beyond the furthest short)`),apply:x=>({...x,template:null,legs:[...x.legs,...adds]})});
  }}
 function adjust(kind:'shift'|'width'|'wings',k:number){
  const b=bodyRef.current;if(!b)return;
  const block=transformBlock(kind,b);if(block){flash(block);return;}
  const r=propose(kind,k,b);
  if('error' in r){setPendingChange({title:`${{shift:'Shift',width:'Width',wings:'Wings'}[kind]} ${k>0?'+':'−'}1`,lines:[],apply:x=>x,blocked:r.error});return;}
  const lines=b.legs.map((l,i)=>l.strike===r.legs[i].strike?null:`${l.side==='B'?'Buy':'Sell'} ${l.type} ${dayMonth(l.expiry)}: ${strikeText(l.strike)} → ${strikeText(r.legs[i].strike)}`).filter(Boolean) as string[];
  if(!lines.length){flash('Nothing to change for this structure.');return;}
  setPendingChange({title:`${{shift:'Shift',width:'Width',wings:'Wings'}[kind]} ${k>0?'+':'−'}1`,lines,apply:x=>({...x,template:null,legs:r.legs})});
 }
 async function applyTemplate(t:Template,param:number|null){
  if(!body?.underlying||!body.expiry)return;
  try{const r=await sb.resolve(t.key,body.underlying,body.expiry,param);
   const legs:Leg[]=r.legs.map((l:any)=>({id:uid(),type:l.type,side:l.side,strike:l.strike,lots:l.lots,expiry:l.expiry,price_basis:'exec',price:null,include:true}));
   setTplOpen(false);
   const doIt=(b:Body)=>({...b,template:t.key,param:param??null,legs});
   if(bodyRef.current?.legs.length){
    setPendingChange({title:`Replace ${bodyRef.current.legs.length} leg${bodyRef.current.legs.length>1?'s':''} with ${t.name}`,
     lines:[...bodyRef.current.legs.map(l=>`Remove: ${legText(l)}`),...legs.map(l=>`Add: ${legText(l)}`)],apply:doIt});
   }else{edit(doIt);flash(`${t.name} loaded - ${legs.length} legs. Everything stays editable.`);}}
  catch(e:any){flash(msg(e));}
 }
 async function snapshot(){afterSave(async()=>{const v=versionRef.current;
  try{const r=await sb.snapshot(id,undefined,{expected_version:v,input_hash:checksumRef.current,request_id:`v${v}`});
   flash(r.draft_version===undefined?`Snapshot ${r.n} already holds this exact version.`:`Saved snapshot ${r.n} of draft v${v}. It will never change.`);reload();setTab('snapshots');}
  catch(e:any){flash(msg(e));}});}
 async function duplicate(){afterSave(async()=>{try{const c=await sb.duplicate(id,undefined,{expected_version:versionRef.current,input_hash:checksumRef.current});
  router.replace({pathname:'/strategies',params:{id:c.id}} as any);flash('Copy created - research only, no paper runs copied.');}catch(e:any){flash(msg(e));}});}
 // the conflict choices act on the body ON SCREEN now (it includes edits made while the banner was showing - review F1)
 async function keepMineAsCopy(){if(!conflict)return;const mine=bodyRef.current||conflict.local;try{const c=await sb.create(mine,`${detail?.name||'Strategy'} (my edits)`);clearLocal(id);setConflict(null);
  router.replace({pathname:'/strategies',params:{id:c.id}} as any);flash('Your edits were saved as a new strategy. The original is unchanged.');}catch(e:any){flash(msg(e));}}
 async function loadNewer(){clearHistory();clearLocal(id);setConflict(null);saveRef.current='saved';bodyRef.current=null;ackedJson.current='';versionRef.current=0;await reload();flash('Loaded the newer saved copy. Your local edits were discarded.');}
 async function keepMineHere(){if(!conflict)return;const mine=bodyRef.current||conflict.local;try{const d=await sb.get(id);const s2=await sb.save(id,d.draft!.version,mine);clearLocal(id);setConflict(null);saveRef.current='saved';
  clearHistory();bodyRef.current=mine;ackedJson.current=J(mine);checksumRef.current=s2.draft!.checksum||'';versionRef.current=s2.draft!.version;setBody(mine);setVersion(s2.draft!.version);setSave('saved');
  flash('Your version replaced the other copy as a new draft version.');}catch(e:any){flash(msg(e));}}
 async function repairAsNew(){
  const b=bodyRef.current;if(!b)return;const next=expiries.find(x=>(x.days_to_expiry??0)>=1);
  if(!next){flash('No listed expiry with a full day left to repair into.');return;}
  try{const ch=await sb.chain(b.underlying,next.expiry);const ks=ch.rows.filter(r=>r.CE?.ltp!=null||r.PE?.ltp!=null).map(r=>r.strike);
   if(!ks.length){flash('The new expiry has no priced strikes yet.');return;}
   const near=(k:number)=>ks.reduce((a,x)=>Math.abs(x-k)<Math.abs(a-k)?x:a,ks[0]);
   const legs=b.legs.map(l=>({...l,expiry:next.expiry,strike:near(l.strike),price_basis:'exec' as const,price:null}));
   const moved=b.legs.filter((l,i)=>legs[i].strike!==l.strike).length;
   const c=await sb.create({...b,expiry:next.expiry,legs,scenario:{}},`${detail?.name||'Strategy'} (repaired to ${dayMonth(next.expiry)})`,detail?.thesis||'');
   router.replace({pathname:'/strategies',params:{id:c.id}} as any);
   flash(`Repaired as a NEW draft on ${dayMonth(next.expiry)}${moved?` - ${moved} strike${moved>1?'s':''} moved to the nearest listed`:''}. The expired original is unchanged.`);}
  catch(e:any){flash(msg(e));}}
 async function rename(){if(!detail||!name.trim()||name===detail.name)return;try{const s=await sb.meta(id,{name});setDetail(d=>d?{...d,name:s.name}:d);}catch(e:any){flash(msg(e));}}

 if(error)return <View style={{padding:24}}><T style={{color:C.red}}>{error}</T><Button label="Back to strategies" kind="outline" onPress={()=>router.replace('/strategies' as any)}/></View>;
 if(!detail||!body)return <Loading/>;
 const a=analysis;const dim=(pending||!!anFailed)&&!!a;
 const multiExp=new Set(body.legs.map(l=>l.expiry)).size>1;
 const scenario=body.scenario||{};const reading=chain?.as_of||a?.as_of;
 const expiryInfo=expiries.find(e=>e.expiry===body.expiry);

 return <View style={{padding:wide?22:14,gap:16,maxWidth:1500,width:'100%',alignSelf:'center'}}>
  {/* header */}
  <View style={[s.between,{flexWrap:'wrap',gap:10}]}>
   <View style={[s.row,{flexWrap:'wrap',gap:10,flex:1,minWidth:280}]}>
    <Pressable accessibilityRole="button" accessibilityLabel="Back to My Strategies" onPress={()=>{blurActive();persist(bodyRef.current!);router.replace('/strategies' as any);}} style={[s.row,{gap:4}]}><Icon name="chevron-left" size={16} color={C.muted}/><T style={{fontSize:12,color:C.muted}}>My Strategies</T></Pressable>
    <TextInput value={name} onChangeText={setName} onBlur={rename} onSubmitEditing={rename} maxLength={80} accessibilityLabel="Strategy name"
     style={{fontFamily:'ManropeBold',fontSize:20,color:C.ink,minWidth:220,flex:1,paddingVertical:4,borderBottomWidth:1,borderColor:C.line}}/>
    <SaveBadge state={save} version={version}/>
    {save==='error'&&<Button label="Retry now" icon="refresh-cw" kind="outline" onPress={()=>flush()}/>}
   </View>
   {coarse?<View style={[s.row,{gap:8,width:'100%'}]}>
    <Button label={st?.live?'Review orders':'Paper trade'} icon="play" onPress={()=>afterSave(()=>st?.live?setReview({open:true}):setPaperOpen(true))} disabled={!body.legs.length||!a||a.status==='invalid'}/>
    <Button label="Undo" icon="rotate-ccw" kind="outline" disabled={!hist.u} accessibilityLabel="Undo the last change" onPress={undoEdit}/>
    <Button label="More" icon="more-horizontal" kind="outline" accessibilityLabel="More actions" onPress={()=>setActionsOpen(true)}/>
   </View>:
   <View style={[s.row,{flexWrap:'wrap',gap:8,width:wide?undefined:'100%'}]}>
    <Button label="Undo" icon="rotate-ccw" kind="outline" disabled={!hist.u} accessibilityLabel="Undo the last change" onPress={undoEdit}/>
    <Button label="Redo" icon="rotate-cw" kind="outline" disabled={!hist.r} accessibilityLabel="Redo" onPress={redoEdit}/>
    <Button label="Prove in Lab" icon="activity" kind="outline" disabled={!body.legs.length} onPress={()=>afterSave(()=>router.push({pathname:'/strategies',params:{view:'lab',strategy:id,v:String(versionRef.current),underlying:body.underlying,lots:String(Math.min(...body.legs.map(l=>l.lots))),structure:a?.structure?.name||'',
     ...(a?.structure?.exact&&a.structure.key?{template:a.structure.key,...(body.template===a.structure.key&&body.param!=null?{param:String(body.param)}:{})}:{mode:'replay'})}} as any))}/>
    <Button label="Adjust" icon="sliders" kind="outline" disabled={!body.legs.length} onPress={()=>afterSave(()=>setAdjustFor({open:true,deployment:deps.find(d=>d.status==='active')||null}))}/>
    <Button label="Save snapshot" icon="bookmark" kind="outline" onPress={snapshot} disabled={!body.legs.length}/>
    <Button label={bell.count?`Alerts (${bell.count})`:'Alerts'} icon="bell" kind="outline" onPress={()=>afterSave(()=>setAlertsOpen(true))}/>
    <Button label="Duplicate" icon="copy" kind="outline" onPress={duplicate}/>
    <Button label={st?.live?'Review paper orders':'Paper trade'} icon="play" onPress={()=>afterSave(()=>st?.live?setReview({open:true}):setPaperOpen(true))} disabled={!body.legs.length||!a||a.status==='invalid'}/>
   </View>}
  </View>
  {conflict&&<View accessibilityRole="alert" style={{backgroundColor:C.amberBg,borderRadius:10,padding:12,gap:8}}>
   <T style={{fontSize:12,color:C.amber}}>This strategy was saved from another window or device after your last save. Your edits on this screen are kept - nothing was overwritten. Choose where they go:</T>
   <View style={[s.row,{flexWrap:'wrap',gap:8}]}><Button label="Keep mine as a new copy" kind="outline" onPress={keepMineAsCopy}/>
    <Button label="Replace the other version with mine" kind="outline" onPress={keepMineHere}/><Button label="Discard mine, load the newer copy" kind="outline" onPress={loadNewer}/></View></View>}
  <DataBanner reading={reading} live={!!chain?.quality.live} status={st}/>
  {chain&&body.legs.length>0&&chain.days_to_expiry<1&&<View style={{backgroundColor:chain.days_to_expiry<=0?'#2A1519':'#2A2210',borderRadius:10,padding:10}} accessibilityRole="alert">
   {chain.days_to_expiry<=0&&<View style={{marginBottom:6,alignSelf:'flex-start'}}><Button label="Repair as a new draft" icon="copy" kind="outline" onPress={repairAsNew}/></View>}
   <T style={{fontSize:12,color:chain.days_to_expiry<=0?C.red:C.amber}}>{chain.days_to_expiry<=0?`Expired: the ${dayMonth(body.expiry)} contracts have settled and can no longer be traded. "Repair as a new draft" copies the structure to the next expiry and keeps this one as it is.`:
    `Expiry day: these contracts settle today at 15:30 IST. Premiums and Greeks move very fast now - small moves swing the P&L sharply.`}</T></View>}

  {/* market context */}
  <UnderlyingPicker list={underlyings} value={body.underlying} onPick={u=>{if(u!==body.underlying){if(body.legs.length&&!confirmSwitch())return;edit(b=>({...b,underlying:u,expiry:'',legs:[],template:null,scenario:{}}));}}}/>
  <ChipRow wrap={wide}>
   {expiries.slice(0,6).map(e=><Chip key={e.expiry} active={body.expiry===e.expiry} label={`${dayMonth(e.expiry)} ${e.monthly?'M':'W'} · ${Math.max(0,Math.round(e.days_to_expiry))}d`}
    onPress={()=>e.expiry===body.expiry?null:multiExp?flash('This strategy spans expiries - change each leg\'s expiry in its editor.'):body.legs.length?setExpiryOpen(true):edit(b=>({...b,expiry:e.expiry,scenario:{...b.scenario,at:undefined}}))}/>)}
   <Chip label={expiries.length>1?`All expiries (${expiries.length})`:'Expiry details'} icon="calendar" onPress={()=>setExpiryOpen(true)}/>
  </ChipRow>

  {!!chainErr&&<ErrorRetry what="The option chain" error={chainErr} onRetry={()=>setChainRetry(n=>n+1)}/>}
  {change&&<View accessibilityRole="alert" style={{backgroundColor:change.blocked?'#2A1519':C.soft,borderWidth:1,borderColor:change.blocked?C.red:C.green,borderRadius:12,padding:12,gap:6}}>
   <T style={{fontFamily:'InterSemi',color:change.blocked?C.red:C.ink}}>{change.blocked?`${change.title}: not possible`:`Preview - ${change.title}`}</T>
   {change.blocked?<T style={{fontSize:12,color:C.red}}>{change.blocked}</T>:change.lines.map((x,i)=><T key={i} style={{fontSize:12,fontVariant:['tabular-nums'] as any}}>{x}</T>)}
   <View style={[s.row,{gap:8}]}>{!change.blocked&&<Button label="Apply" icon="check" onPress={()=>{const c=change;setPendingChange(null);
     // the preview was computed from a specific body: if the strategy changed since, applying it would overwrite those edits
     if(c.base!==J(bodyRef.current)){flash('The strategy changed after this preview, so it was not applied. Run the change again.');return;}
     edit(c.apply);flash('Applied. Undo brings back the previous legs.');}}/>}
    <Button label={change.blocked?'OK':'Cancel'} kind="outline" onPress={()=>setPendingChange(null)}/></View></View>}
  <View style={{flexDirection:wide?'row':'column-reverse',gap:16,alignItems:wide?'flex-start':'stretch'}}>
   {/* LEFT: legs */}
   <View style={[panel,{flex:wide?45:undefined,width:wide?undefined:'100%'}]}>
    <View style={[s.between,{flexWrap:'wrap'}]}><View style={{gap:2}}><T style={label}>Legs</T>
     <View style={[s.row,{gap:6}]}><T style={{fontFamily:'InterSemi',fontSize:14}}>{a?.structure?.name||'—'}</T>
      {body.legs.length>0&&<Pressable accessibilityRole="button" accessibilityLabel="About this strategy" onPress={()=>setAboutOpen(true)}><Icon name="info" size={15} color={C.green}/></Pressable>}</View></View>
     <View style={[s.row,{gap:8,flexWrap:'wrap',flexShrink:1,maxWidth:'100%'}]}><Button label="Template" icon="layout" kind="outline" onPress={()=>setTplOpen(true)} disabled={!body.expiry}/>
      <Button label="Spreads" icon="list" kind="outline" onPress={()=>setSpreadsOpen(true)} disabled={!body.expiry||!chain}/><Button label="Add from chain" icon="plus" kind="soft" onPress={()=>setChainOpen(true)} disabled={!chain}/></View></View>
    {!body.legs.length?<View style={{paddingVertical:26,alignItems:'center',gap:8}}><Icon name="layers" size={22} color={C.green}/>
      <T style={{fontFamily:'InterSemi'}}>No legs yet</T><T style={{fontSize:12,color:C.muted,textAlign:'center',maxWidth:340}}>Pick a template for a ready structure, or add exact contracts from the chain.</T></View>:
     body.legs.map(l=>{const row=a?.legs?.find(r=>r.id===l.id);const ltp=priceOf(l.strike,l.type,l.expiry,l.id);
      if(coarse)return <Pressable key={l.id} accessibilityRole="button" accessibilityLabel={`Edit leg: ${legText(l)}`} onPress={()=>setLegEdit(l)}
       style={({pressed})=>[s.between,{borderTopWidth:1,borderColor:C.line,paddingVertical:10,minHeight:56,opacity:!l.include?.5:pressed?.7:1}]}>
       <View style={{gap:3,flex:1}}><T style={{fontSize:14,fontFamily:'InterSemi',color:l.side==='B'?C.green:C.red}}>{`${l.side==='B'?'BUY':'SELL'} ${l.lots} × ${strikeText(l.strike)} ${l.type} · ${dayMonth(l.expiry)}${l.include?'':' (excluded)'}`}</T>
       <T style={{fontSize:11,color:C.muted}}>{`${l.lots} lot${l.lots>1?'s':''} × ${chain?.lot_size||'?'} = ${chain?.lot_size?l.lots*chain.lot_size:'?'} units`}</T>
        <T style={{fontSize:11,color:C.muted}}>{`Entry ${num(row?.entry)} ${BASIS_TAG[(a?.legs_quotes?.find(q=>q.id===l.id)?.basis_used)||'']||''} · IV ${row?.iv!=null?`${row.iv}%`:'—'} · Δ ${row?.greeks?num(row.greeks.delta,1):'—'}`}</T></View>
       <Icon name="edit-2" size={16} color={C.muted}/></Pressable>;
      return <View key={l.id} style={{borderTopWidth:1,borderColor:C.line,paddingTop:10,gap:8,opacity:l.include?1:.5}}>
       <View style={[s.row,{flexWrap:'wrap',gap:8}]}>
        <Pressable role="checkbox" aria-checked={l.include} accessibilityLabel={`Include ${strikeText(l.strike)} ${l.type} in analysis`} onPress={()=>setLeg(l.id,{include:!l.include})}
         style={{width:22,height:22,borderRadius:5,borderWidth:1.5,borderColor:l.include?C.green:C.muted,backgroundColor:l.include?C.green:'transparent',alignItems:'center',justifyContent:'center'}}>{l.include&&<Icon name="check" size={13} color="#041B12"/>}</Pressable>
        <Pressable accessibilityRole="button" accessibilityLabel={`Side: ${l.side==='B'?'Buy':'Sell'}. Switch`} onPress={()=>setLeg(l.id,{side:l.side==='B'?'S':'B'})}
         style={{paddingHorizontal:10,height:coarse?44:28,minWidth:coarse?52:undefined,alignItems:'center',borderRadius:7,justifyContent:'center',backgroundColor:l.side==='B'?C.soft:'#2A1519',borderWidth:1,borderColor:l.side==='B'?C.green:C.red}}>
         <T style={{fontSize:11,fontFamily:'InterSemi',color:l.side==='B'?C.green:C.red}}>{l.side==='B'?'BUY':'SELL'}</T></Pressable>
        <Stepper label={`${body.linked?'🔗 ':''}${strikeText(l.strike)}`} a11y={`Strike ${strikeText(l.strike)}${body.linked?' (linked: moves every leg)':''}`} onMinus={()=>stepStrike(l,-1)} onPlus={()=>stepStrike(l,1)}/>
        <Pressable accessibilityRole="button" accessibilityLabel={`Expiry ${dayMonth(l.expiry)}. Change`} onPress={()=>setLegEdit(l)} style={[tag,l.expiry!==body.expiry&&{borderColor:C.amber}]}>
         <T style={{fontSize:11,fontFamily:'InterSemi',color:l.expiry!==body.expiry?C.amber:C.muted}}>{dayMonth(l.expiry)}</T></Pressable>
        <Pressable accessibilityRole="button" accessibilityLabel={`Type ${l.type}. Switch to ${l.type==='CE'?'PE':'CE'}`} onPress={()=>setLeg(l.id,{type:l.type==='CE'?'PE':'CE'})} style={[tag,coarse&&{height:44,minWidth:44,alignItems:'center'}]}><T style={{fontSize:11,fontFamily:'InterSemi'}}>{l.type}</T></Pressable>
        <Stepper label={`${l.lots} lot${l.lots>1?'s':''}`} a11y={`${l.lots} lots`} onMinus={()=>l.lots>1&&setLeg(l.id,{lots:l.lots-1})} onPlus={()=>l.lots<500&&setLeg(l.id,{lots:l.lots+1})}/>
        <View style={{flex:1}}/>
        <Pressable accessibilityRole="button" accessibilityLabel={`Remove ${strikeText(l.strike)} ${l.type}`} onPress={()=>removeLeg(l.id)} style={{padding:coarse?14:6}}><Icon name="trash-2" size={15} color={C.muted}/></Pressable>
       </View>
       <View style={[s.row,{flexWrap:'wrap',gap:14}]}>
        <PriceField leg={l} ltp={ltp} quote={a?.legs_quotes?.find(q=>q.id===l.id)} tick={chain?.tick_size} onManual={(v,from)=>{setLeg(l.id,{price_basis:'manual',price:v});if(from!=null)flash(`Rounded ${from} to ${v}, on the ${chain?.tick_size||0.05} exchange tick.`);}} onBasis={(b)=>setLeg(l.id,{price_basis:b,price:null})}/>
        <Mini k="IV" v={row?.iv!=null?`${row.iv}%`:'—'}/><Mini k="Δ" v={row?.greeks?num(row.greeks.delta,1):'—'}/>
        <Mini k="Units" v={row?String(row.units):String(l.lots*(chain?.lot_size||0))}/>
       </View>
      </View>;})}
    {!!a?.legs?.length&&<T style={{fontSize:11,color:C.muted,fontVariant:['tabular-nums'] as any}} accessibilityLabel="Cost formula">
     {`${a.legs.map((l,i)=>`${i?(l.units>0?' − ':' + '):(l.units>0?'−':'+')}${num(l.entry)} × ${Math.abs(l.units)}`).join('')} = ${inr(Math.abs(a.premium?.value||0))} ${(a.premium?.value||0)>=0?'credit':'debit'} · charges ~${inr(a.charges?.value)}`}</T>}
    {undo&&<View style={[s.between,{backgroundColor:C.paper,borderRadius:10,padding:10}]}><T style={{fontSize:12}}>{`Removed ${strikeText(undo.leg.strike)} ${undo.leg.type}`}</T>
     <Button label="Undo" kind="outline" onPress={()=>{const u=undo;setUndo(null);edit(b=>{const legs=[...b.legs];legs.splice(Math.min(u.index,legs.length),0,u.leg);return {...b,legs};});}}/></View>}
    {body.legs.length>0&&<View style={[s.row,{flexWrap:'wrap',gap:10,borderTopWidth:1,borderColor:C.line,paddingTop:10}]}>
     <T style={label}>Adjust all</T>
     <Stepper label="Shift" a11y="Shift all strikes" onMinus={()=>adjust('shift',-1)} onPlus={()=>adjust('shift',1)}/>
     <Stepper label="Width" a11y="Width between legs" onMinus={()=>adjust('width',-1)} onPlus={()=>adjust('width',1)}/>
     <Stepper label="Wings" a11y="Hedge wing distance" onMinus={()=>adjust('wings',-1)} onPlus={()=>adjust('wings',1)}/>
     <Stepper label="Size ×" a11y="Multiply or divide every leg's lots, keeping the ratios" onMinus={()=>resize(-1)} onPlus={()=>resize(1)}/>
     <Chip label={body.linked?'Strikes linked':'Link strikes'} icon="link" active={!!body.linked} onPress={()=>edit(b=>({...b,linked:!b.linked}))}/>
     <Button label="Clear all" icon="x" kind="outline" onPress={clearAll}/>
    </View>}
   </View>

   {/* RIGHT: analysis */}
   <View style={{flex:wide?55:undefined,width:wide?undefined:'100%',gap:16}}>
    <RiskStrip a={a} dim={dim} onFix={applyFix}/>
    <EvidenceLine id={id} version={version} onOpen={(rid)=>router.push({pathname:'/strategies',params:{view:'lab',strategy:id}} as any)}/>
    <View style={panel}>
     <View style={s.between}><T style={label}>Payoff</T>{pending&&<T style={{fontSize:11,color:C.amber}}>{a?'Updating…':'Calculating…'}</T>}
      {!pending&&!!anFailed&&<View style={[s.row,{gap:8}]}><T style={{fontSize:11,color:C.red}}>{a?'Not updated - these numbers are for an earlier version':'Could not calculate'}{` (${anFailed})`}</T><Button label="Retry" kind="outline" onPress={()=>setAnRetry(n=>n+1)}/></View>}</View>
     <PayoffChart curve={a?.curve||[]} spot={a?.spot||chain?.spot||0} scenarioSpot={a?.scenario?.spot} breakevens={a?.breakevens?.value||[]} bands={(a?.scenario?.active&&a?.sd?.bands_to_date?.length?a.sd.bands_to_date:a?.sd?.bands)||[]} dim={dim}
      scenarioLabel={a?.scenario?.is_expiry?'Scenario (expiry)':`Scenario ${a?.scenario?istStamp(a.scenario.at):''}`} expiryLabel={horizonShort(a)} empty={a?.status==='incomplete'?'No payoff: a leg is not a listed contract, and the rest is a different position.':undefined}/>
     <ScenarioBar body={body} chain={chain} expiry={body.expiry} onChange={sc=>edit(b=>({...b,scenario:sc}))} result={a}/>
    </View>
    {!!a?.warnings?.length&&<View style={[panel,{borderColor:'#5A4A1F',backgroundColor:C.amberBg,gap:6}]}>{a.warnings.map((w,i)=><View key={i} style={[s.row,{alignItems:'flex-start',gap:8}]}><Icon name="alert-triangle" size={13} color={C.amber}/><T style={{fontSize:12,color:C.amber,flex:1}}>{w}</T></View>)}</View>}
    <View style={panel}>
     <View style={[s.row,{flexWrap:'wrap',gap:6}]}>
      {(['pnl','greeks','table','snapshots','paper','alerts','activity'] as Tab[]).map(t=><Chip key={t} active={tab===t} onPress={()=>setTab(t)}
       label={{pnl:'P&L by leg',greeks:'Greeks',table:'Payoff table',snapshots:`Snapshots (${detail.snapshots.length})`,paper:`Paper (${runs.length+deps.length})`,alerts:`Alerts${bell.count?` (${bell.count})`:''}`,activity:'Activity'}[t]}/>)}
     </View>
     {/* one calculation state for every dependent output (fresh audit P12): loading is never "No legs", and a table
         for an earlier leg set is dimmed and labelled, exactly like the risk strip and the chart */}
     {(tab==='pnl'||tab==='greeks'||tab==='table')&&(!a&&pending?<T style={{fontSize:12,color:C.muted}} accessibilityLiveRegion="polite">Calculating…</T>:
      a?.status==='incomplete'?<T style={{fontSize:12,color:C.red}}>Unavailable until every included leg resolves to a listed contract.</T>:
      <View style={{opacity:dim?.5:1}}>{dim&&<T style={{fontSize:11,color:C.amber}}>{pending?'Updating - these values are for the previous version.':'Not updated - these values are for an earlier version.'}</T>}
       {tab==='pnl'&&<Scrollable narrow={coarse} min={720}><LegTable a={a}/></Scrollable>}
       {tab==='greeks'&&<Scrollable narrow={coarse} min={560}><GreeksTable a={a}/></Scrollable>}
       {tab==='table'&&<PayoffTable a={a}/>}</View>)}
     {tab==='snapshots'&&<Snapshots detail={detail} onView={setSnapView} onRestore={(rid)=>afterSave(async()=>{try{const s=await sb.restore(id,rid,versionRef.current);clearHistory();bodyRef.current=s.draft!.body;ackedJson.current=J(s.draft!.body);checksumRef.current=s.draft!.checksum||'';versionRef.current=s.draft!.version;
       setBody(s.draft!.body);setVersion(s.draft!.version);setSave('saved');flash('Snapshot restored as a new draft version.');}catch(e:any){flash(msg(e));reload();}})}
      onDuplicate={async(rid)=>{try{const c=await sb.duplicate(id,rid);router.replace({pathname:'/strategies',params:{id:c.id}} as any);}catch(e:any){flash(msg(e));}}}/>}
     {tab==='paper'&&<View style={{gap:10}}>{deps.map(d=><View key={d.id} style={{gap:6}}><DeploymentCard d={d} onChanged={loadRuns} onClose={(x)=>setReview({open:true,closing:x})} onAdjust={(x)=>setAdjustFor({open:true,deployment:x})}/>
      <View style={{alignSelf:'flex-start'}}><Button label="Open monitor" icon="external-link" kind="outline" onPress={()=>router.push({pathname:'/strategies',params:{view:'deployment',id:d.id}} as any)}/></View></View>)}{!deps.length&&st?.live&&<T style={{fontSize:12,color:C.muted}}>No paper deployments yet. "Review paper orders" builds the exact plan from live quotes.</T>}</View>}
     {tab==='paper'&&<PaperList runs={runs} onClose={async(run)=>{try{await sb.paperClose(run);flash('Paper run closed at the stored reading.');loadRuns();reload();}catch(e:any){flash(msg(e));}}} onOpen={()=>router.push({pathname:'/strategies',params:{view:'paper'}} as any)}/>}
     {tab==='alerts'&&<AlertsPanel strategyId={id} analysis={a} deployments={deps} expiry={body.expiry} onChanged={reload}/>}
     {tab==='activity'&&<View style={{gap:6}}>{detail.activity.map((x,i)=><View key={i} style={[s.between,{borderTopWidth:1,borderColor:C.line,paddingTop:6}]}><T style={{fontSize:12,flex:1}}>{x.detail}</T><T style={{fontSize:11,color:C.muted}}>{istEpoch(x.created_at)}</T></View>)}</View>}
    </View>
   </View>
  </View>

  <ChainDrawer visible={chainOpen} chain={chain} legs={body.legs} onClose={()=>setChainOpen(false)} onToggle={(k,kind,side,_p,exp)=>toggleFromChain(k,kind,side,exp)}/>
  <SpreadsSheet visible={spreadsOpen} onClose={()=>setSpreadsOpen(false)} underlying={body.underlying} expiry={body.expiry} lots={Math.max(1,Math.min(...(body.legs.length?body.legs.map(l=>l.lots):[1])))}
   onPick={(legs,tpl,w)=>{setSpreadsOpen(false);const nl=legs.map((l,i)=>({id:uid(),type:l.type,side:l.side,strike:l.strike,lots:l.lots,expiry:l.expiry,price_basis:'exec' as const,price:null,include:true}));
    const doIt=(b:Body)=>({...b,template:tpl,param:[2,4,6,8].includes(w)?w:null,legs:nl});
    if(bodyRef.current?.legs.length)setPendingChange({title:`Replace ${bodyRef.current.legs.length} leg${bodyRef.current.legs.length>1?'s':''} with this spread`,
     lines:[...bodyRef.current.legs.map(l=>`Remove: ${legText(l)}`),...nl.map(l=>`Add: ${legText(l)}`)],apply:doIt});
    else{edit(doIt);flash('Spread loaded - every leg stays editable.');}}}/>
  <AboutSheet visible={aboutOpen} onClose={()=>setAboutOpen(false)} a={a} templateKey={a?.structure?.exact?a.structure.key:null}/>
  <AdjustSheet visible={adjustFor.open} strategyId={id} version={version} deployment={adjustFor.deployment||null} onClose={()=>setAdjustFor({open:false})}
   onApplied={async(r)=>{const dep=adjustFor.deployment;setAdjustFor({open:false});await reload();
    flash(`${r.adjustment.name} saved as a new version.${dep?' Review the delta orders next.':''}`);if(dep)setReview({open:true,adjusting:dep});}}/>
  <OrderReview visible={review.open} strategyId={id} deployment={review.closing||null} adjusting={review.adjusting||null} onClose={()=>setReview({open:false})}
   draft={{expected_version:versionRef.current,input_hash:checksumRef.current}}
   onPlaced={(d)=>{setReview({open:false});flash(`Paper ${review.closing?'close':review.adjusting?'adjustment':'orders'} placed - deployment ${d.status.replace('_',' ')}. No order reached a broker.`);loadRuns();reload();setTab('paper');}}/>
  <TemplateSheet visible={tplOpen} onClose={()=>setTplOpen(false)} onPick={applyTemplate} replacing={body.legs.length}/>
  <LegSheet leg={legEdit} chain={chain} moveStrike={moveStrike} onClose={()=>setLegEdit(null)} onRemove={removeLeg} expiries={expiries}
   onApply={l=>{setLegEdit(null);edit(b=>{const legs=b.legs.map(x=>x.id===l.id?l:x);
    // the strategy's expiry is always its NEAREST leg's (the chain, scenario dates and settlement follow it)
    const near=legs.map(x=>x.expiry).sort()[0]||b.expiry;return {...b,template:null,legs,expiry:near,scenario:near!==b.expiry?{...b.scenario,at:undefined}:b.scenario};});}}/>
  <ExpirySheet visible={expiryOpen} onClose={()=>setExpiryOpen(false)} expiries={expiries} current={body.expiry} underlying={body.underlying} legs={body.legs}
   onApply={e=>edit(b=>({...b,expiry:e,legs:b.legs.map(l=>({...l,expiry:e})),scenario:{...b.scenario,at:undefined}}))}/>
  <SnapshotSheet rid={snapView} onClose={()=>setSnapView(null)} onSaved={reload}/>
  <Sheet visible={alertsOpen} onClose={()=>setAlertsOpen(false)} wide title="Alerts for this strategy" subtitle="Notify only - an alert never places, changes or cancels an order.">
   <AlertsPanel strategyId={id} analysis={a} deployments={deps} expiry={body.expiry} onChanged={reload}/>
   <Button label="Open the alerts centre" kind="outline" onPress={()=>{setAlertsOpen(false);router.push({pathname:'/strategies',params:{view:'alerts'}} as any);}}/>
  </Sheet>
  <ActionsSheet visible={actionsOpen} onClose={()=>setActionsOpen(false)} actions={[
   {label:'Redo',icon:'rotate-cw',disabled:!hist.r,onPress:redoEdit},
   {label:'Prove in Lab',icon:'activity',disabled:!body.legs.length,onPress:()=>afterSave(()=>router.push({pathname:'/strategies',params:{view:'lab',strategy:id,v:String(versionRef.current),underlying:body.underlying,lots:String(Math.min(...body.legs.map(l=>l.lots))),structure:a?.structure?.name||'',...(a?.structure?.exact&&a.structure.key?{template:a.structure.key,...(body.template===a.structure.key&&body.param!=null?{param:String(body.param)}:{})}:{mode:'replay'})}} as any))},
   {label:'Adjust',icon:'sliders',disabled:!body.legs.length,onPress:()=>afterSave(()=>setAdjustFor({open:true,deployment:deps.find(d=>d.status==='active')||null}))},
   {label:'Save snapshot',icon:'bookmark',disabled:!body.legs.length,onPress:snapshot},
   {label:bell.count?`Alerts (${bell.count})`:'Alerts',icon:'bell',onPress:()=>afterSave(()=>setAlertsOpen(true))},
   {label:'Duplicate',icon:'copy',onPress:duplicate},
  ]}/>
  <PaperSheet visible={paperOpen} onClose={()=>setPaperOpen(false)} a={a} body={body} chain={chain}
   onStart={async()=>{try{if(!(await flush())){flash('Your latest edit is not saved yet, so no paper run was started.');return;}const r=await sb.paperStart(id,{expected_version:versionRef.current,input_hash:checksumRef.current});setPaperOpen(false);flash(`Paper run started - ${r.fills.length} simulated fills. No order was sent.`);loadRuns();reload();setTab('paper');}catch(e:any){flash(msg(e));}}}/>
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
 const m={saved:['green',`Draft v${version} · saved`],dirty:['neutral','Unsaved changes…'],saving:['neutral','Saving…'],conflict:['amber','Version conflict - your edits are kept'],error:['red','Not saved']}[state];
 return <Badge tone={m[0]} label={m[1]} dot/>;
}
function DataBanner({reading,live,status}:{reading?:string;live:boolean;status:Status|null}){
 return <View style={[s.row,{flexWrap:'wrap',gap:8,backgroundColor:live?C.soft:C.amberBg,borderRadius:10,paddingHorizontal:12,paddingVertical:8}]}>
  <Icon name={live?'radio':'clock'} size={13} color={live?C.green:C.amber}/>
  {!!(status as any)?.calendar?.warning&&<T style={{fontSize:12,color:C.red,width:'100%'}}>{(status as any).calendar.warning}</T>}
  <T style={{fontSize:12,color:live?C.green:C.amber,flex:1}}>{live?`Live · Zerodha Kite · quotes ${istStamp(reading)} · refreshes every 10 s · market ${status?.market_open?'open':'closed'}. Prices: buy at the ask, sell at the bid. Orders here are paper only.`:`Stored reading · ${istStamp(reading)} · not live${status?.reason?` (${status.reason})`:''}. Prices are last traded (no bid/ask). Research and paper only - nothing here sends an order.`}</T>
 </View>;
}
// touch screens get 44 px targets (GTM audit P13); a mouse keeps the compact desktop control with a larger hit area
function useCoarse(){return useWindowDimensions().width<700;}
function Stepper({label:l,a11y,onMinus,onPlus}:{label:string;a11y:string;onMinus:()=>void;onPlus:()=>void}){
 const big=useCoarse();const h=big?44:28;
 const b=(icon:string,fn:()=>void,what:string)=><Pressable accessibilityRole="button" accessibilityLabel={`${a11y}: ${what}`} onPress={fn} hitSlop={big?undefined:{top:8,bottom:8}} style={({pressed})=>({width:big?44:26,height:h,alignItems:'center',justifyContent:'center',opacity:pressed?.6:1})}><Icon name={icon} size={big?16:13} color={C.muted}/></Pressable>;
 return <View style={[s.row,{gap:0,borderWidth:1,borderColor:C.line,borderRadius:7,height:h}]}>{b('minus',onMinus,'decrease')}<T style={{fontSize:12,fontFamily:'InterMedium',minWidth:44,textAlign:'center',fontVariant:['tabular-nums'] as any}}>{l}</T>{b('plus',onPlus,'increase')}</View>;
}
function Mini({k,v}:{k:string;v:string}){return <View style={[s.row,{gap:5}]}><T style={{fontSize:11,color:C.muted}}>{k}</T><T style={{fontSize:12,fontVariant:['tabular-nums'] as any}}>{v}</T></View>;}
/** The short name of the horizon strategy-wide numbers are valued at (fresh audit P03): exact expiry, or the model. */
function horizonShort(a:Analysis|null){return a?.horizon?.kind==='model_near_expiry'?`Model at ${dayMonth(a.horizon.expiry)} expiry`:'At expiry';}
const NEXT:Record<string,Basis>={exec:'mid',mid:'ltp',ltp:'exec',manual:'exec'};
function PriceField({leg,ltp,quote,tick,onManual,onBasis}:{leg:Leg;ltp:number|null;quote?:{bid:number|null;ask:number|null;ltp:number|null;basis_used:string};tick?:number|null;onManual:(v:number,roundedFrom?:number)=>void;onBasis:(b:Basis)=>void}){
 const onLtp=()=>onBasis('exec');
 const shown=quote?(quote.basis_used==='exec'?(leg.side==='B'?quote.ask:quote.bid):quote.basis_used==='mid'&&quote.bid&&quote.ask?Math.round((quote.bid+quote.ask)*50)/100:quote.ltp):ltp;
 const tag=leg.price_basis==='manual'?'manual':quote?.basis_used==='exec'?(leg.side==='B'?'ask':'bid'):quote?.basis_used||leg.price_basis;
 const [text,setText]=useState(leg.price_basis==='manual'?String(leg.price):'');
 useEffect(()=>{setText(leg.price_basis==='manual'?String(leg.price):'');},[leg.price_basis,leg.price]);
 // tick before calculate (Blueprint A): the typed price snaps to the tick before it is calculated or saved
 const commit=()=>{const v=Number(text);if(text.trim()===''){onLtp();return;}if(Number.isFinite(v)&&v>=0){const t=snap(v,tick);setText(String(t));onManual(t,t!==v?v:undefined);}};
 return <View style={[s.row,{gap:6}]}>
  <T style={{fontSize:11,color:C.muted}}>Entry</T>
  <TextInput value={leg.price_basis==='manual'?text:(shown!=null?String(shown):'')} onChangeText={setText} onFocus={()=>{if(leg.price_basis!=='manual')setText(shown!=null?String(shown):'');}}
   onBlur={commit} onSubmitEditing={commit} keyboardType="decimal-pad" accessibilityLabel={`Entry price for ${strikeText(leg.strike)} ${leg.type}`}
   style={{width:74,height:28,borderWidth:1,borderColor:leg.price_basis==='manual'?C.amber:C.line,borderRadius:7,paddingHorizontal:8,color:C.ink,fontFamily:'Inter',fontSize:12}}/>
  <Pressable accessibilityRole="button" accessibilityLabel={`Price basis ${tag}. Change`} onPress={()=>onBasis(NEXT[leg.price_basis])} style={{paddingHorizontal:6,paddingVertical:3,borderRadius:6,borderWidth:1,borderColor:leg.price_basis==='manual'?C.amber:C.line}}><T style={{fontSize:11,color:leg.price_basis==='manual'?C.amber:C.muted}}>{tag}{leg.price_basis!=='manual'&&quote?.bid&&quote?.ask?` · ${quote.bid}/${quote.ask}`:''}</T></Pressable>
 </View>;
}

function RiskStrip({a,dim,onFix}:{a:Analysis|null;dim:boolean;onFix?:(f:any)=>void}){
 const [more,setMore]=useState(false);
 if(!a||a.status==='no_market'||a.status==='empty')return <View style={[panel,{minHeight:84,justifyContent:'center'}]}><T style={{color:C.muted,fontSize:12}}>{a?.warnings?.[0]||'Risk numbers appear once the strategy has legs.'}</T></View>;
 if(a.status==='invalid')return <View style={panel}><T style={{color:C.red,fontSize:12}}>{a.warnings[0]}</T></View>;
 // fail closed (fresh audit P04): a leg that did not resolve means NO strategy-wide numbers - never the survivors'
 if(a.status==='incomplete')return <View style={[panel,{gap:6,borderColor:'#5A2A30'}]} accessibilityRole="alert">
  <T style={{color:C.red,fontFamily:'InterSemi',fontSize:13}}>Risk unavailable - a leg is not a listed contract</T>
  {(a.unresolved||[]).map(u=><T key={u.leg_id} style={{color:C.red,fontSize:12}}>{`• ${u.label}`}</T>)}
  <T style={{color:C.muted,fontSize:12}}>Max loss, profit and breakevens need every included leg. Change the strike or expiry of the missing leg, or untick "Include" to explore the rest deliberately.</T></View>;
 const hz=a.horizon;const model=hz?.kind==='model_near_expiry';
 const at=hz?hz.label:'At expiry, gross';
 const money=(m?:any,prefix='')=>!m?'—':m.status!=='available'?(m.status==='unsupported'?'Not supported':'Unavailable'):m.unlimited?'Unlimited':signed(m.value);
 const items=[
  ['Max loss',money(a.max_loss),a.max_loss?.unlimited?C.red:C.red,model?`${at} · modelled at today's IV, not a guaranteed floor`:`${at} of charges`],
  ['Max profit',money(a.max_profit),C.green,model?`${at} · modelled at today's IV, not a guaranteed cap`:`${at} of charges`],
  ['Breakeven',a.breakevens?.status==='available'?(a.breakevens.value.length?a.breakevens.value.map((b:number)=>num(b,0)).join(' · '):'None'):'—',C.ink,
   a.scenario?.active&&a.breakevens_target?.status==='available'?`${horizonShort(a)} · on the what-if date: ${a.breakevens_target.value.length?a.breakevens_target.value.map((b:number)=>num(b,0)).join(' · '):'none in range'}`:at],
  ['Required funds',a.margin?.status==='available'?inr(a.margin.value):'Needs live data',a.margin?.status==='available'?C.ink:C.muted,a.margin?.status==='available'?`Exchange margin (Kite) · hedge benefit ${inr(a.margin.hedge_benefit)}`:'Exchange margin - not the same as max loss'],
  ['Reward : risk',a.reward_risk?.status==='available'&&a.reward_risk.value!=null?`${num(a.reward_risk.value,2)} : 1`:'—',C.ink,a.reward_risk?.status==='available'?`Max profit ÷ max loss · ${horizonShort(a).toLowerCase()}`:'Unbounded on one side'],
  ['Capital at risk',a.capital_at_risk?.unlimited?'Unlimited':inr(a.capital_at_risk?.value),C.ink,model?'Modelled max loss at the near expiry - not exchange margin':'Structural max loss - not exchange margin'],
  [a.premium?.direction==='credit'?'Net credit':'Net debit',inr(Math.abs(a.premium?.value||0)),C.ink,'At entry prices'],
  [a.scenario?.active&&a.pop_scenario?.status==='available'?'POP (model) · Scenario':'POP (model)',a.scenario?.active&&a.pop_scenario?.status==='available'?`${a.pop_scenario.value}%`:a.pop?.status==='available'?`${a.pop.value}%`:'—',C.ink,
   a.scenario?.active&&a.pop_scenario?.status==='available'?`From the what-if point · ${a.pop?.status==='available'?`${a.pop.value}% from now`:''}`:a.pop?.sigma?`Lognormal at ${a.pop.sigma}% ${a.pop.sigma_basis==='chain_atm_iv'?'chain ATM IV':'IV of the leg nearest spot (proxy)'}`:'Model value'],
  ['Charges (est.)',inr(a.charges?.value),C.ink,'Entry orders, published rates'],
  ['Chance of loss (model)',(a as any).outcomes?.status==='available'?`${(a as any).outcomes.value.loss}%`:'—',C.ink,(a as any).outcomes?.status==='available'?
   `Profit ${(a as any).outcomes.value.profit}% · loss ${(a as any).outcomes.value.loss}%${(a as any).outcomes.value.max_loss!=null?` · max loss ${(a as any).outcomes.value.max_loss}%`:''}${(a as any).outcomes.value.max_profit!=null?` · max profit ${(a as any).outcomes.value.max_profit}%`:''} · ${horizonShort(a).toLowerCase()}`:'Model value'],
 ] as const;
 return <View style={[panel,{gap:10,opacity:dim?.5:1}]}>
  {model&&<View style={[s.row,{gap:8,alignItems:'flex-start'}]}><Badge label="MODEL" tone="amber"/><T style={{fontSize:12,color:C.amber,flex:1}}>{`${hz!.label}. ${hz!.note}`}</T></View>}
  <View style={{flexDirection:'row',flexWrap:'wrap',gap:14}}>
  {items.slice(0,more?items.length:4).map(([k,v,col,help])=><View key={k} style={{minWidth:130,flex:1,gap:2}} accessibilityLabel={`${k}: ${v}. ${help}`}>
   <T style={{fontSize:11,color:C.muted}}>{k}</T><T style={{fontFamily:'InterSemi',fontSize:16,color:col,fontVariant:['tabular-nums'] as any}}>{v}</T><T style={{fontSize:10,color:C.muted}}>{help}</T></View>)}
 </View>
  <Pressable accessibilityRole="button" accessibilityState={{expanded:more}} onPress={()=>setMore(!more)} style={{minHeight:32,justifyContent:'center'}}>
   <T style={{fontSize:12,color:C.green}}>{more?'Fewer numbers':'More numbers: reward:risk, capital at risk, premium, probability, charges'}</T></Pressable>
  {!!a.insights?.length&&<View style={{gap:4,borderTopWidth:1,borderColor:C.line,paddingTop:8}} accessibilityLabel="Risk warnings">
   {a.insights.map((w,i)=><View key={i} style={[s.row,{gap:6,alignItems:'flex-start'}]}><Icon name={w.level==='warn'?'alert-triangle':'info'} size={13} color={w.level==='warn'?C.amber:C.muted}/>
    <T style={{fontSize:12,flex:1,color:w.level==='warn'?C.ink:C.muted}}>{w.text}</T>
    {(w as any).fix&&onFix&&<Pressable accessibilityRole="button" accessibilityLabel={`${(w as any).fix.label}: ${w.text}`} onPress={()=>onFix((w as any).fix)} style={{minHeight:28,justifyContent:'center'}}>
     <T style={{fontSize:12,color:C.green}}>{(w as any).fix.label}</T></Pressable>}</View>)}</View>}
 </View>;
}

function ScenarioBar({body,chain,expiry,onChange,result}:{body:Body;chain:Chain|null;expiry:string;onChange:(sc:Body['scenario'])=>void;result:Analysis|null}){
 const sc=body.scenario||{};const spot=chain?.spot||0;
 const [spotText,setSpotText]=useState(sc.spot?String(sc.spot):'');
 useEffect(()=>{setSpotText(sc.spot?String(sc.spot):'');},[sc.spot]);
 const readingDay=(chain?.as_of||'').slice(0,10);const hol=useHolidays();
 const days=useMemo(()=>{if(!readingDay||!expiry)return [] as string[];const out:string[]=[];let d=readingDay;for(let i=0;i<60&&d<=expiry;i++){if(!isWeekend(d)&&!hol.set.has(d))out.push(d);d=addDays(d,1);}if(!out.includes(expiry))out.push(expiry);return out;},[readingDay,expiry,hol]);
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
   {result?.scenario_pnl?.status==='available'?`Scenario P&L: ${signed(result.scenario_pnl.value)} ${result.scenario?.is_expiry?'(at expiry)':'(model, Black-Scholes)'} · ${hol.version?'weekends and NSE holidays skipped':'weekends skipped; NSE holiday list not loaded'}`:
    result?.scenario_pnl?`Scenario P&L unavailable: ${result.scenario_pnl.reason}`:''}
  </T>
 </View>;
}
function Stepperish({children,onMinus,onPlus,what='value'}:any){const big=useCoarse();return <View style={[s.row,{gap:0,borderWidth:1,borderColor:C.line,borderRadius:7,height:big?44:30}]}>
 <Pressable accessibilityRole="button" accessibilityLabel={`Decrease scenario ${what}`} onPress={onMinus} style={{width:big?44:26,height:big?44:28,alignItems:'center',justifyContent:'center'}}><Icon name="minus" size={13} color={C.muted}/></Pressable>
 {children}<Pressable accessibilityRole="button" accessibilityLabel={`Increase scenario ${what}`} onPress={onPlus} style={{width:big?44:26,height:big?44:28,alignItems:'center',justifyContent:'center'}}><Icon name="plus" size={13} color={C.muted}/></Pressable></View>;}

function Table({head,rows,right=[]}:{head:string[];rows:(string|number)[][];right?:number[]}){
 // semantic table roles (web) so assistive tech reads header/cell relationships (GTM audit P13)
 return <View {...({role:'table'} as any)}>{[head,...rows].map((r,i)=><View key={i} {...({role:'row'} as any)} style={[s.row,{gap:8,paddingVertical:6,borderTopWidth:i?1:0,borderColor:C.line}]}>
  {r.map((c,j)=><T key={j} {...({role:i?(j===0?'rowheader':'cell'):'columnheader'} as any)} style={{flex:j===0?2:1,fontSize:i?12:10,color:i?C.ink:C.muted,textAlign:right.includes(j)?'right':'left',fontFamily:i?'Inter':'InterMedium',
   textTransform:i?'none':'uppercase',fontVariant:['tabular-nums'] as any}}>{String(c)}</T>)}</View>)}</View>;
}
function LegTable({a}:{a:Analysis|null}){
 if(!a?.legs?.length)return <T style={{fontSize:12,color:C.muted}}>No legs to value.</T>;
 const tot=a.legs.reduce((x,l)=>x+(l.target_pnl||0),0);
 return <View style={{gap:6}}><Table head={['Leg','Units','Entry (basis)','Mark now','Intrinsic','Time value','Scenario price','Scenario P&L']} right={[1,2,3,4,5,6,7]}
  rows={[...a.legs.map(l=>[l.label,l.units,`${num(l.entry)} ${BASIS_TAG[(a.legs_quotes?.find(q=>q.id===l.id)?.basis_used)||'']||''}`,`${num((l as any).mark)} ${(l as any).mark_basis==='mid'?'mid':(l as any).mark_basis==='ltp'?'LTP':''}`,num((l as any).intrinsic),num((l as any).time_value),num(l.target_price),signed(l.target_pnl)]),['Total (gross)','','','','','','',signed(tot)]]}/>
  <T style={{fontSize:11,color:C.muted}}>{`Scenario: ${a.scenario?num(a.scenario.spot,2):''} on ${a.scenario?istStamp(a.scenario.at):''}. Entry is each leg's cost basis (ask/bid = executable estimate, mid, LTP = last trade, manual = typed); Mark now is the market value (bid/ask mid, or LTP when there is no valid book) that the Greeks and IV use. Charges ${inr(a.charges?.value)} not included.`}</T></View>;
}
function GreeksTable({a}:{a:Analysis|null}){
 const [at,setAt]=useState<'now'|'whatif'>('now');
 if(!a?.legs?.length)return <T style={{fontSize:12,color:C.muted}}>No legs.</T>;
 if(a.greeks?.status!=='available')return <T style={{fontSize:12,color:C.amber}}>{`Greeks unavailable: ${a.greeks?.reason||''}`}</T>;
 const w=at==='whatif'&&a.greeks_scenario?.status==='available';const G=w?a.greeks_scenario:a.greeks;
 return <View style={{gap:6}}>
  <View style={[s.row,{gap:6,flexWrap:'wrap'}]}><Chip label="At the reading" active={at==='now'} onPress={()=>setAt('now')}/>
   <Chip label="At the what-if" active={at==='whatif'} onPress={()=>setAt('whatif')}/>
   {at==='whatif'&&a.greeks_scenario?.status!=='available'&&<T style={{fontSize:11,color:C.muted}}>{a.greeks_scenario?.reason==='AT_EXPIRY'?'Not defined at expiry - the position is settled.':'Unavailable'}</T>}</View>
  <Table head={['Leg','Delta','Gamma','Theta ₹/day','Vega ₹/IV pt']} right={[1,2,3,4]}
  rows={at==='whatif'&&!w?a.legs.map(l=>[l.label,'—','—','—','—']):[...a.legs.map(l=>{const g=w?(l as any).greeks_scenario:l.greeks;return [l.label,num(g?.delta,1),num(g?.gamma,3),num(g?.theta,0),num(g?.vega,0)];}),['Strategy total',num(G.delta,1),num(G.gamma,3),num(G.theta,0),num(G.vega,0)]]}/>
  {w&&<T style={{fontSize:11,color:C.muted}}>{`At ${num(a.greeks_scenario.spot,2)} on ${istStamp(a.greeks_scenario.at)}${a.greeks_scenario.iv_shift?`, IV ${a.greeks_scenario.iv_shift>0?'+':''}${a.greeks_scenario.iv_shift} pts`:''}.`}</T>}
  <T style={{fontSize:11,color:C.muted}}>Whole-strategy units (lots × lot size). Delta in underlying units per 1 point; theta per calendar day; vega per 1 percentage point of IV. {w?'Model values at the what-if point.':'Model values at the reading.'}</T></View>;
}
function PayoffTable({a}:{a:Analysis|null}){
 if(!a?.table?.length)return <T style={{fontSize:12,color:C.muted}}>No payoff table yet.</T>;
 return <Table head={[`${a.underlying||'Underlying'} level`,'% from spot','Scenario date',horizonShort(a)]} right={[1,2,3]} rows={a.table.map(r=>[num(r.s,0),`${r.pct>0?'+':''}${r.pct}%`,signed(r.target),signed(r.expiry)])}/>;
}
function Snapshots({detail,onRestore,onDuplicate,onView}:{detail:Detail;onRestore:(id:string)=>void;onDuplicate:(id:string)=>void;onView:(id:string)=>void}){
 if(!detail.snapshots.length)return <T style={{fontSize:12,color:C.muted}}>No snapshots yet. "Save snapshot" freezes the current legs and their analysis; it never changes afterwards.</T>;
 return <View style={{gap:8}}>{detail.snapshots.map(x=><View key={x.id} style={[s.between,{borderTopWidth:1,borderColor:C.line,paddingTop:8,flexWrap:'wrap'}]}>
  <View style={{gap:2,flex:1,minWidth:200}}><T style={{fontSize:13,fontFamily:'InterSemi'}}>{`#${x.n} ${x.name}`}</T>
   <T style={{fontSize:11,color:C.muted}}>{`Priced at ${istStamp(x.reading_at)} · max loss ${x.summary.unlimited_loss?'unlimited':signed(x.summary.max_loss)} · max profit ${signed(x.summary.max_profit)} · ${x.checksum.slice(0,8)}`}</T></View>
  {!!x.notes&&<T style={{fontSize:11,color:C.ink,width:'100%'}} numberOfLines={2}>{x.notes}</T>}
  <View style={[s.row,{gap:6,flexWrap:'wrap'}]}><Button label="View" kind="outline" onPress={()=>onView(x.id)}/><Button label="Restore" kind="outline" onPress={()=>onRestore(x.id)}/><Button label="Copy to new" kind="outline" onPress={()=>onDuplicate(x.id)}/></View></View>)}</View>;
}
function PaperList({runs,onClose,onOpen}:{runs:PaperRun[];onClose:(id:string)=>void;onOpen:()=>void}){
 if(!runs.length)return <T style={{fontSize:12,color:C.muted}}>No paper runs for this strategy. "Paper trade" records simulated fills against a frozen snapshot - it never sends an order.</T>;
 return <View style={{gap:10}}>{runs.map(r=><View key={r.id} style={{borderTopWidth:1,borderColor:C.line,paddingTop:8,gap:4}}>
  <View style={s.between}><View style={[s.row,{gap:8}]}><Badge label={r.status==='open'?'PAPER · OPEN':'PAPER · CLOSED'} tone={r.status==='open'?'green':'neutral'}/><T style={{fontSize:12,color:C.muted}}>{`Snapshot #${r.revision.n} · opened ${istStamp(r.opened_reading)}`}</T></View>
   {r.status==='open'&&<Button label="Close (simulated)" kind="outline" onPress={()=>onClose(r.id)}/>}</View>
  <T style={{fontSize:13}}>{`Net ${signed(r.net)} (realised ${signed(r.realised)}, unrealised ${signed(r.unrealised)}, fees ${inr(r.fees)})`}{r.close_now_estimate!=null?` · if closed now ≈ ${signed(r.close_now_estimate)}`:''}</T>
 </View>)}<Button label="All paper runs" kind="outline" icon="list" onPress={onOpen}/></View>;
}

function PaperSheet({visible,onClose,a,body,chain,onStart}:{visible:boolean;onClose:()=>void;a:Analysis|null;body:Body;chain:Chain|null;onStart:()=>void}){
 const [busy,setBusy]=useState(false);
 const unlimited=!!a?.max_loss?.unlimited;
 return <Sheet visible={visible} onClose={onClose} title="Start a paper run" subtitle="Simulated fills recorded against a frozen snapshot. No order is sent to any broker."
  footer={<View style={[s.row,{justifyContent:'flex-end',gap:8}]}><Button label="Cancel" kind="outline" onPress={onClose}/><Button label="Record simulated fills" icon="play" loading={busy} onPress={async()=>{setBusy(true);await onStart();setBusy(false);}}/></View>}>
  <View style={{gap:8}}>
   {body.legs.filter(l=>l.include).map(l=>{const ltp=chain?.rows.find(r=>r.strike===l.strike)?.[l.type]?.ltp;
    return <View key={l.id} style={s.between}><T style={{fontSize:13}}>{`${l.side==='B'?'Buy':'Sell'} ${l.lots} × ${strikeText(l.strike)} ${l.type} · ${dayMonth(l.expiry)}`}</T><T style={{fontSize:12,color:C.muted}}>{`Stored LTP ${num(ltp)} · simulated fill ${l.side==='B'?'above':'below'} it by slippage`}</T></View>;})}
  </View>
  <View style={[panel,{gap:6}]}>
   <T style={{fontSize:12}}>Fill policy: last traded price at the stored reading, moved against you by the larger of 0.5% or one tick. Estimated charges are recorded on every fill.</T>
   <T style={{fontSize:12,color:C.muted}}>{`Priced at ${istStamp(chain?.as_of)}. A snapshot named "Paper entry" is saved so the run always points at exactly these legs.`}</T>
   {unlimited&&<T style={{fontSize:12,color:C.red}}>This structure has unlimited loss. A paper run is a safe place to learn how that behaves.</T>}
  </View>
 </Sheet>;
}

// --- save-lane helpers (GTM audit P01) ------------------------------------------------------------------------
function J(b:any){return JSON.stringify(b??null);}
function blurActive(){try{const el:any=typeof document!=='undefined'?document.activeElement:null;el?.blur?.();}catch{}}
const LOCAL=(id:string)=>`kanida.sb.draft.${id}`;
function readLocal(id:string):{version:number;body:Body}|null{try{const raw=typeof localStorage!=='undefined'?localStorage.getItem(LOCAL(id)):null;return raw?JSON.parse(raw):null;}catch{return null;}}
function writeLocal(id:string,version:number,body:Body){try{if(typeof localStorage!=='undefined')localStorage.setItem(LOCAL(id),JSON.stringify({version,body,at:Date.now()}));}catch{}}
function clearLocal(id:string){try{if(typeof localStorage!=='undefined')localStorage.removeItem(LOCAL(id));}catch{}}

// NSE holidays from the server's versioned calendar (one fetch per session); stepping skips them like weekends
let _hol:{set:Set<string>;version:string|null}|null=null;let _holP:Promise<any>|null=null;
function useHolidays(){const [h,setH]=useState(_hol||{set:new Set<string>(),version:null});
 useEffect(()=>{if(_hol){setH(_hol);return;}_holP=_holP||sb.calendar().then(c=>{_hol={set:new Set(c.holidays.map(x=>x.date)),version:c.version};return _hol;}).catch(()=>null);
  let live=true;_holP.then(x=>{if(live&&x)setH(x);});return()=>{live=false};},[]);
 return h;}
const BASIS_TAG:Record<string,string>={exec:'(ask/bid)',mid:'(mid)',ltp:'(LTP)',manual:'(manual)'};

/** A8: the Lab evidence behind this strategy's structure, where it is built - with its version and the deciding run's date. */
function EvidenceLine({id,version,onOpen}:{id:string;version:number;onOpen:(rid:string)=>void}){
 const [e,setE]=useState<any>(null);const [err,setErr]=useState('');
 useEffect(()=>{let live=true;const t=setTimeout(()=>sb.evidence(id).then(x=>{if(live){setE(x);setErr('');}}).catch(x=>{if(live)setErr(x.message);}),600);return()=>{live=false;clearTimeout(t);};},[id,version]);
 if(err)return <T style={{fontSize:11,color:C.muted}}>{`Lab evidence could not be read (${err}).`}</T>;
 if(!e||e.status==='none')return e?<T style={{fontSize:11,color:C.muted}}>{`Lab evidence: ${e.label}.`}</T>:null;
 const tone=e.status==='tested_significant'?'green':e.status==='tested_not_significant'?'neutral':'amber';
 return <View style={[panel,{gap:4,paddingVertical:10}]} accessibilityLabel={`Lab evidence: ${e.label}`}>
  <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><T style={label}>Lab evidence</T><Badge label={e.label} tone={tone as any}/></View>
  {!!e.note&&<T style={{fontSize:11,color:C.muted}}>{e.note}</T>}
  <View style={[s.row,{gap:10,flexWrap:'wrap'}]}><T style={{fontSize:10,color:C.muted}}>{`${e.deciding_run_at?`Deciding run ${istEpoch(e.deciding_run_at)} · `:''}statistics ${(e.evidence_version||'?').split(':')[0]} · model-priced, not a forecast`}</T>
   {!!e.run_id&&<Pressable accessibilityRole="button" onPress={()=>onOpen(e.run_id)}><T style={{fontSize:11,color:C.green}}>Open the Lab</T></Pressable>}</View>
 </View>;
}
