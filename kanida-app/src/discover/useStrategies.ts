// Catalog + per-strategy results fetch with a small module cache (60 s). Stale responses (key changed, Stop, newer request) are dropped.
import {useCallback,useEffect,useRef,useState} from 'react';
import {api} from '../model';
import type {Catalog,DetectionScope,LiveDetectionResults,ResearchResults,ResearchRow,ResearchSummary,StrategyResults} from '../strategies/types';
import {researchRowToStrategyRow} from './cardLogic';
/** Researched-pattern results come from the precomputed index with their own row shape. They are adapted to the
 * scanner's row type here, in one place, so the API stays truthful about what each number is. The scanner card
 * relabels its columns for this source (cardLogic.RESEARCH_COLUMNS) - a value is never shown under a heading
 * that would misdescribe it. */
function adapt(d:any):StrategyResults{
 if(d?.source!=='research_index')return d as StrategyResults;
 const st=d.strategy as ResearchSummary;
 return {...d,rows:(d.rows as ResearchRow[]).map(r=>researchRowToStrategyRow(r,st)),
  // `found` exists only to satisfy the shared StrategySummary shape used for ordering and slot defaults. It is
  // the real detection count, NOT the size of the history list, and research cards never render it - they use
  // researchStatusText / researchPickerTag, which name each number.
  strategy:{...st,best_low_pct:st.best_edge_low_pct,positive_low_count:st.beats_baseline,tested_count:st.cells_tested,
   found:st.detections_today??0},research_rows:d.rows} as unknown as StrategyResults;
}
const TTL=60000;
let catalogSlot:{data:Catalog|null;at:number;promise:Promise<Catalog>|null}={data:null,at:0,promise:null};
const results=new Map<string,{data:StrategyResults;at:number}>();
export function clearStrategyCache(){results.clear();clearDetectionCache();catalogSlot={data:catalogSlot.data,at:0,promise:null};}
function loadCatalog(force:boolean):Promise<Catalog>{
 if(!force&&catalogSlot.data&&Date.now()-catalogSlot.at<TTL)return Promise.resolve(catalogSlot.data);
 if(!force&&catalogSlot.promise)return catalogSlot.promise;
 const p:Promise<Catalog>=api('/api/strategies/catalog').then((d:Catalog)=>{if(!d||!Array.isArray(d.blocks))throw new Error('Strategy catalog is unavailable right now.');if(catalogSlot.promise===p)catalogSlot={data:d,at:Date.now(),promise:null};return d},(e:any)=>{if(catalogSlot.promise===p)catalogSlot.promise=null;throw e});
 catalogSlot.promise=p;return p;
}
export function useCatalog(){
 const [data,setData]=useState<Catalog|null>(catalogSlot.data),[error,setError]=useState(''),[loading,setLoading]=useState(!catalogSlot.data),seq=useRef(0);
 const reload=useCallback((force=false)=>{const id=++seq.current;setLoading(true);setError('');loadCatalog(force).then(d=>{if(id===seq.current){setData(d);setLoading(false)}},(e:any)=>{if(id===seq.current){setError(e?.message||'Unable to load');setLoading(false)}})},[]);
 useEffect(()=>{reload(false);return()=>{seq.current++}},[reload]);
 return {data,error,loading,reload};
}
export type ResultsPhase='idle'|'loading'|'done'|'error'|'stopped';
export const resultsPath=(key:string,universe:string)=>`/api/strategies/${encodeURIComponent(key)}/results?universe=${encodeURIComponent(universe)}`;
type ResultsState={key:string;phase:ResultsPhase;data:StrategyResults|null;error:string;at:number};
/** One card's results. A new key returns phase 'loading' in the same render (no frame of the old strategy's rows). refreshSeq change → forced reload. stop() drops the in-flight response. */
export function useStrategyResults(key:string|null,universe='nifty500',refreshSeq=0){
 const cacheKey=key?`${key}|${universe}`:'',seq=useRef(0),lastRefresh=useRef(refreshSeq);
 const [state,setState]=useState<ResultsState>({key:'',phase:'idle',data:null,error:'',at:0});
 const run=useCallback((force:boolean)=>{
  const id=++seq.current;if(!key){setState({key:'',phase:'idle',data:null,error:'',at:0});return}
  const hit=results.get(cacheKey);if(!force&&hit&&Date.now()-hit.at<TTL){setState({key:cacheKey,phase:'done',data:hit.data,error:'',at:hit.at});return}
  setState({key:cacheKey,phase:'loading',data:null,error:'',at:0});
  api(resultsPath(key,universe)).then((raw:any)=>{
   if(id!==seq.current)return;
   const d=adapt(raw);
   if(!d||!Array.isArray(d.rows)||(d.strategy?.key&&d.strategy.key!==key))throw new Error('Strategy results are unavailable right now.');
   const at=Date.now();results.set(cacheKey,{data:d,at});setState({key:cacheKey,phase:'done',data:d,error:'',at});
  }).catch((e:any)=>{if(id===seq.current)setState({key:cacheKey,phase:'error',data:null,error:e?.message||'Unable to load',at:0})});
 },[key,universe,cacheKey]);
 useEffect(()=>{const force=refreshSeq!==lastRefresh.current;lastRefresh.current=refreshSeq;run(force);return()=>{seq.current++}},[run,refreshSeq]);
 const stop=useCallback(()=>{seq.current++;setState(x=>x.phase==='loading'?{...x,phase:'stopped'}:x)},[]);
 const reload=useCallback(()=>run(true),[run]);
 const fresh=state.key===cacheKey;
 return {phase:(fresh?state.phase:key?'loading':'idle') as ResultsPhase,data:fresh?state.data:null,error:fresh?state.error:'',loadedAt:fresh?state.at:0,reload,stop};
}

// --- live detections ---------------------------------------------------------------------------------------
// One research strategy's detections from the scanner's ledger, served by /api/strategies/{key}/detections.
// ALWAYS bounded: the server caps `limit` and returns the real `total`, so a card that is showing 100 of 412
// says so rather than issuing a second, unbounded request. Cached for the same 60 s as the other card sources;
// the ledger only moves when a scan pass commits (2-10 minutes, docs/LIVE_DETECTION.md §7).
export const DETECTION_LIMIT=100;
export const detectionsPath=(key:string,scope:DetectionScope,limit=DETECTION_LIMIT)=>
 `/api/strategies/${encodeURIComponent(key)}/detections?scope=${encodeURIComponent(scope)}&limit=${limit}`;
const detections=new Map<string,{data:LiveDetectionResults;at:number}>();
export function clearDetectionCache(){detections.clear();}
export type DetectionsState={key:string;phase:ResultsPhase;data:LiveDetectionResults|null;error:string;at:number};
/** One card's live detections. Mirrors useStrategyResults: a new key returns `loading` in the same render,
 * refreshSeq forces a reload, and a response for an older key is dropped rather than shown. */
export function useLiveDetections(key:string|null,scope:DetectionScope='today',refreshSeq=0,enabled=true){
 const cacheKey=key&&enabled?`${key}|${scope}`:'',seq=useRef(0),lastRefresh=useRef(refreshSeq);
 const [state,setState]=useState<DetectionsState>({key:'',phase:'idle',data:null,error:'',at:0});
 const run=useCallback((force:boolean)=>{
  const id=++seq.current;
  if(!key||!enabled){setState({key:'',phase:'idle',data:null,error:'',at:0});return}
  const hit=detections.get(cacheKey);
  if(!force&&hit&&Date.now()-hit.at<TTL){setState({key:cacheKey,phase:'done',data:hit.data,error:'',at:hit.at});return}
  setState({key:cacheKey,phase:'loading',data:null,error:'',at:0});
  api(detectionsPath(key,scope)).then((raw:any)=>{
   if(id!==seq.current)return;
   // Key guard: a payload for another strategy or another scope is never rendered under this header.
   if(!raw||!Array.isArray(raw.rows)||(raw.strategy?.key&&raw.strategy.key!==key)||(raw.scope&&raw.scope!==scope))
    throw new Error('Live detections are unavailable right now.');
   const at=Date.now();detections.set(cacheKey,{data:raw as LiveDetectionResults,at});
   setState({key:cacheKey,phase:'done',data:raw as LiveDetectionResults,error:'',at});
  }).catch((e:any)=>{if(id===seq.current)setState({key:cacheKey,phase:'error',data:null,error:e?.message||'Unable to load',at:0})});
 },[key,scope,cacheKey,enabled]);
 useEffect(()=>{const force=refreshSeq!==lastRefresh.current;lastRefresh.current=refreshSeq;run(force);return()=>{seq.current++}},[run,refreshSeq]);
 const stop=useCallback(()=>{seq.current++;setState(x=>x.phase==='loading'?{...x,phase:'stopped'}:x)},[]);
 const reload=useCallback(()=>run(true),[run]);
 const fresh=state.key===cacheKey;
 return {phase:(fresh?state.phase:cacheKey?'loading':'idle') as ResultsPhase,data:fresh?state.data:null,
  error:fresh?state.error:'',loadedAt:fresh?state.at:0,reload,stop};
}
