import {useCallback,useEffect,useRef,useState} from 'react';
import {AccessibilityInfo,Platform} from 'react-native';
import {readStore,writeStore} from '../layout/shared';
import type {ScanJob} from '../strategies/types';
import {falconApi} from './api';
import {Pace,initPace,stepPace,skipPace,paceComplete} from './pacing';
export type FalconPhase='idle'|'scanning'|'results'|'error';
type Stored={runningId?:string;completedOnce?:boolean};
const STORE='falcon',KEY='home',POLL_MS=400,TICK_MS=100,MAX_POLL_FAILS=8;
/** Server stamps may be epoch seconds or ms. */
export const toMs=(v:number|string|null|undefined)=>{if(v==null||v==='')return NaN;const n=typeof v==='number'?v:Date.parse(v);return typeof v==='number'?(n<1e12?n*1000:n):n;};
export function useReducedMotion(){
 const [reduce,setReduce]=useState(false);
 useEffect(()=>{
  if(Platform.OS==='web'){const mq=(globalThis as any).matchMedia?.('(prefers-reduced-motion: reduce)');if(!mq)return;setReduce(!!mq.matches);const on=(e:any)=>setReduce(!!e.matches);mq.addEventListener?.('change',on);return()=>mq.removeEventListener?.('change',on);}
  let live=true;AccessibilityInfo.isReduceMotionEnabled().then(v=>{if(live)setReduce(v)}).catch(()=>{});const sub=AccessibilityInfo.addEventListener('reduceMotionChanged',setReduce);return()=>{live=false;sub.remove()};
 },[]);
 return reduce;
}
// Scan state machine: idle → scanning (poll 400 ms + paced reveal) → results | error. Resumes a job this browser started (id kept in writeStore).
export function useFalconScan(){
 const [phase,setPhase]=useState<FalconPhase>('idle');
 const [job,setJob]=useState<ScanJob|null>(null),[latest,setLatest]=useState<ScanJob|null>(null),[latestLoaded,setLatestLoaded]=useState(false);
 const [error,setError]=useState(''),[pace,setPace]=useState<Pace>(()=>initPace(Date.now())),[now,setNow]=useState(Date.now());
 const [completedOnce,setCompletedOnce]=useState(()=>!!readStore<Stored>(STORE,KEY).completedOnce),[skip,setSkip]=useState(false);
 const run=useRef(0),jobRef=useRef<ScanJob|null>(null),paceRef=useRef(pace),skipRef=useRef(false),startedAt=useRef(0);
 jobRef.current=job;paceRef.current=pace;skipRef.current=skip;
 const persist=useCallback((patch:Stored)=>{const cur=readStore<Stored>(STORE,KEY);const next={...cur,...patch};if(!patch.runningId&&'runningId' in patch)delete next.runningId;writeStore(STORE,KEY,next);},[]);
 const poll=useCallback((id:string,token:number)=>{
  let fails=0;
  const loop=async()=>{
   if(token!==run.current)return;
   try{const j=await falconApi.get(id);if(token!==run.current)return;fails=0;setJob(j);
    if(j.status==='running'){setTimeout(loop,POLL_MS);return;}
    persist({runningId:undefined});
    if(j.status==='cancelled'){run.current++;setJob(null);setPhase('idle');return;}
    if(j.status==='failed'){const bad=j.stages.find(s=>s.status==='failed');setError(j.error||(bad?`${bad.title} failed on the server.`:'The scan failed on the server.'));}
    // done → the tick effect moves to results once the paced reveal has caught up
   }catch(e:any){if(token!==run.current)return;if(e?.status===404){persist({runningId:undefined});run.current++;setError('This scan is no longer on the server.');setPhase('error');return;}
    if(++fails>=MAX_POLL_FAILS){setError(e?.message?`Lost contact with the scan: ${e.message}`:'Lost contact with the scan.');setPhase('error');return;}setTimeout(loop,POLL_MS*2);}
  };
  loop();
 },[persist]);
 const begin=useCallback((j:ScanJob|null,id:string,start:number)=>{const token=++run.current;startedAt.current=start;setJob(j);setError('');setSkip(false);const t=Date.now();setPace(initPace(t,start));setNow(t);setPhase('scanning');poll(id,token);},[poll]);
 const start=useCallback(async()=>{
  const token=++run.current;setError('');setSkip(false);setJob(null);const t=Date.now();startedAt.current=t;setPace(initPace(t));setNow(t);setPhase('scanning');
  try{const {id}=await falconApi.start();if(token!==run.current){falconApi.cancel(id).catch(()=>{});return;}if(!id)throw new Error('The server did not return a scan id.');persist({runningId:id});run.current--;begin(null,id,t);}
  catch(e:any){if(token!==run.current)return;setError(e?.message?`Falcon could not start a scan: ${e.message}`:'Falcon could not start a scan.');setPhase('error');}
 },[begin,persist]);
 const cancel=useCallback(()=>{const id=jobRef.current?.id||readStore<Stored>(STORE,KEY).runningId;run.current++;persist({runningId:undefined});setJob(null);setSkip(false);setPhase('idle');if(id)falconApi.cancel(id).catch(()=>{});},[persist]);
 const viewResults=useCallback(()=>{if(latest?.result){run.current++;setJob(latest);setPhase('results');}},[latest]);
 const backToIdle=useCallback(()=>{run.current++;setError('');setPhase('idle');},[]);
 // Mount: last completed scan + resume a job this browser started.
 useEffect(()=>{let live=true;
  falconApi.latest().then(j=>{if(live)setLatest(j?.result?j:null)}).catch(()=>{}).finally(()=>{if(live)setLatestLoaded(true)});
  const id=readStore<Stored>(STORE,KEY).runningId;
  if(id)falconApi.get(id).then(j=>{if(!live)return;if(j.status==='running'){const s=toMs(j.started_at);begin(j,id,Number.isFinite(s)?Math.min(s,Date.now()):Date.now());}else persist({runningId:undefined});}).catch(()=>{if(live)persist({runningId:undefined})});
  return()=>{live=false;run.current++;};
 },[]);
 // Tick: advance the paced reveal; finish when the server is done AND the reveal has caught up (or the user skipped).
 useEffect(()=>{if(phase!=='scanning')return;const timer=setInterval(()=>{
  const t=Date.now(),j=jobRef.current;setNow(t);if(!j)return;
  const p=skipRef.current?skipPace(paceRef.current,j.stages,t):stepPace(paceRef.current,j.stages,t);if(p!==paceRef.current){paceRef.current=p;setPace(p);}
  if(j.status==='failed'){setPhase('error');return;}
  if(j.status==='done'&&(paceComplete(p,j.stages)||skipRef.current&&j.result)){
   run.current++;setPhase(j.result?'results':'error');if(!j.result)setError('The scan finished without results.');
   if(j.result){setLatest(j);setCompletedOnce(true);persist({completedOnce:true,runningId:undefined});}
  }
 },TICK_MS);return()=>clearInterval(timer);},[phase,persist]);
 const elapsedMs=phase==='scanning'?Math.max(0,now-startedAt.current):0;
 return {phase,job,latest,latestLoaded,error,pace,now,elapsedMs,completedOnce,skip,start,cancel,retry:start,rescan:start,viewResults,backToIdle,requestSkip:()=>setSkip(true)};
}
