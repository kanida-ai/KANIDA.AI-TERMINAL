import React,{createContext,useCallback,useContext,useEffect,useMemo,useRef,useState} from 'react';
import {Platform} from 'react-native';
import {router,usePathname,useGlobalSearchParams} from 'expo-router';
// Single active-symbol store (TRENDSPIDER_STUDY §10.2, Phase 0 item 0.1). It is the ONLY source of truth for "which stock / match is selected".
// Quant rules: a matchId is only kept when it belongs to the same symbol × timeframe (ids are "SYMBOL:TF:pattern"); side/asOf/matchId never carry over to another symbol.
// Evidence is keyed by evidenceKey(); useKeyedValue() drops a value the moment its key changes, so a previous symbol's evidence is never rendered.
export type ActiveSource='search'|'discover'|'watch'|'simulate'|'events'|'map'|'replay'|'nav'|'url'|'restore';
export type ActiveSide='long'|'short';
export type ActiveSymbol={symbol:string;timeframe:string;matchId?:string;detectionId?:string;side?:ActiveSide;asOf?:string;source:ActiveSource};
export type ActiveInput={symbol:string;timeframe?:string;matchId?:string;detectionId?:string;side?:ActiveSide;asOf?:string;source:ActiveSource};
export type ActiveHistory={entries:ActiveSymbol[];index:number};
export type ActiveNavItem={symbol:string;timeframe:string;matchId?:string;side?:ActiveSide};
export type ActiveParams={s?:string;tf?:string;m?:string;d?:string};
export type ActiveSymbolApi={active:ActiveSymbol|null;history:ActiveHistory;setActive:(next:ActiveInput,opts?:{replace?:boolean})=>boolean;back:()=>void;forward:()=>void;canBack:boolean;canForward:boolean;isActive:(symbol:string,matchId?:string)=>boolean;reset:()=>void};
export const HISTORY_CAP=50,DEFAULT_TIMEFRAME='1D',emptyHistory:ActiveHistory={entries:[],index:-1};
/** Routes whose document title names the active symbol; every other route (auth, onboarding, account, legal) shows plain "KANIDA". */
export const WORKSPACE_TITLE_PATHS=['/chart','/simulate','/watch','/autotrade','/activity'];
/** The full-chart workspace route whose URL carries ?s=&tf=&m= (FALCON_DISCOVER_SPEC §1; was "/"). */
const URL_PATH='/chart';
const SYMBOL_RE=/^[A-Z0-9][A-Z0-9&._-]{0,39}$/,FRAME_RE=/^[0-9]{1,3}[A-Z]{1,2}$/,MATCH_RE=/^[A-Za-z0-9&._:-]{1,160}$/,AS_OF_RE=/^\d{4}-\d{2}-\d{2}([ T][0-9:.+Z-]{0,20})?$/;
const clean=(v:unknown)=>typeof v==='string'?v.trim():'';
const first=(v:unknown)=>Array.isArray(v)?v[0]:v;

// ---- Pure logic (no React; exercised by a node check) ----
/** Parses a server match id "SYMBOL:TF:pattern". Returns null when the id is not in that shape. */
export function parseMatchId(id:string):{symbol:string;timeframe:string;pattern:string;variant?:string;side?:ActiveSide}|null{const p=clean(id).split(':');if(p.length!==3&&p.length!==5)return null;const symbol=p[0].toUpperCase(),timeframe=p[1].toUpperCase(),pattern=p[2];if(!SYMBOL_RE.test(symbol)||!FRAME_RE.test(timeframe)||!pattern)return null;if(p.length===5){if(!p[3]||(p[4]!=='long'&&p[4]!=='short'))return null;return {symbol,timeframe,pattern,variant:p[3],side:p[4]};}return {symbol,timeframe,pattern};}
/** Validates and normalises a write. Symbol/timeframe are upper-cased; timeframe falls back to the match id's, then the current entry's (the chart keeps its timeframe), then 1D. A matchId from another symbol × timeframe is dropped. Invalid symbol/timeframe → null (write rejected). */
export function normalizeActive(next:ActiveInput,current?:ActiveSymbol|null):ActiveSymbol|null{
 const symbol=clean(next?.symbol).toUpperCase();if(!SYMBOL_RE.test(symbol))return null;
 const m=clean(next.matchId),ref=m&&MATCH_RE.test(m)?parseMatchId(m):null;
 const timeframe=clean(next.timeframe).toUpperCase()||(ref&&ref.symbol===symbol?ref.timeframe:'')||current?.timeframe||DEFAULT_TIMEFRAME;if(!FRAME_RE.test(timeframe))return null;
 const out:ActiveSymbol={symbol,timeframe,source:next.source};
 if(m&&MATCH_RE.test(m)&&(!ref||(ref.symbol===symbol&&ref.timeframe===timeframe)))out.matchId=m;
 const detection=clean(next.detectionId);if(out.matchId&&ref&&/^[A-Za-z0-9]{1,64}$/.test(detection))out.detectionId=detection;
 if(out.matchId&&ref?.side)out.side=ref.side;
 else if(next.side==='long'||next.side==='short')out.side=next.side;
 const asOf=clean(next.asOf);if(asOf&&AS_OF_RE.test(asOf))out.asOf=asOf;
 return out;
}
/** Identity ignores `source`: the same symbol/timeframe/match/side/asOf from a different list is the same selection. */
export function sameActive(a?:ActiveSymbol|null,b?:ActiveSymbol|null){if(!a||!b)return !a&&!b;return a.symbol===b.symbol&&a.timeframe===b.timeframe&&(a.matchId||'')===(b.matchId||'')&&(a.detectionId||'')===(b.detectionId||'')&&(a.side||'')===(b.side||'')&&(a.asOf||'')===(b.asOf||'');}
/** Identical to current → no new entry (only `source` is refreshed in place). replace → overwrite current entry. Otherwise drop forward entries, append, keep the newest HISTORY_CAP. */
export function applyActive(h:ActiveHistory,next:ActiveSymbol,replace=false):ActiveHistory{
 const cur=h.entries[h.index],swap=()=>({entries:h.entries.map((e,i)=>i===h.index?next:e),index:h.index});
 if(cur&&sameActive(cur,next))return cur.source===next.source?h:swap();
 if(cur&&replace)return swap();
 const entries=[...h.entries.slice(0,h.index+1),next].slice(-HISTORY_CAP);return {entries,index:entries.length-1};
}
export function stepHistory(h:ActiveHistory,delta:number):ActiveHistory{const index=h.index+delta;return index<0||index>=h.entries.length?h:{entries:h.entries,index};}
export function matchesActive(a:ActiveSymbol|null|undefined,symbol:string,matchId?:string){return !!a&&clean(symbol).toUpperCase()===a.symbol&&(!matchId||a.matchId===matchId);}
/** Explicit selection: an episode id never falls back to another episode in the same cell. */
export function selectedActiveMatch<T extends {id:string;symbol:string;timeframe:string;detection_id?:string}>(active:ActiveSymbol,matches:T[]):T|undefined{return matches.find(m=>m.id===active.matchId&&m.symbol===active.symbol&&m.timeframe===active.timeframe&&(!active.detectionId||m.detection_id===active.detectionId));}
/** Resolve an explicit live episode from its chart, independently of the filtered/capped match list.
 * The chart's own symbol/timeframe and the complete cell id must agree before using any match. */
export function selectedChartDetection(active:ActiveSymbol,chart:any):any|undefined{
 if(!active.detectionId||chart?.symbol!==active.symbol||chart?.timeframe!==active.timeframe||!Array.isArray(chart.matches))return;
 const match=selectedActiveMatch(active,chart.matches.map((m:any)=>({...m,symbol:m.symbol||chart.symbol,timeframe:m.timeframe||chart.timeframe})));
 // Live detections have no legacy per-match performance history. Never attach a cached neighbour's evidence.
 return match?{...match,history:[]}:undefined;
}
/** URL → store. Needs a valid `s`; an invalid `tf` rejects the link (never guessed); an `m` for another symbol × timeframe is dropped. */
export function parseActiveParams(params:Record<string,unknown>|null|undefined):ActiveSymbol|null{const s=clean(first(params?.s));if(!s)return null;return normalizeActive({symbol:s,timeframe:clean(first(params?.tf))||undefined,matchId:clean(first(params?.m))||undefined,detectionId:clean(first(params?.d))||undefined,source:'url'});}
/** Store → URL params. Keys with no value are `undefined`, which router.setParams removes. */
export function serializeActiveParams(a:ActiveSymbol|null|undefined):{s:string|undefined;tf:string|undefined;m:string|undefined;d:string|undefined}{return {s:a?.symbol,tf:a?a.timeframe:undefined,m:a?.matchId,d:a?.detectionId};}
export function paramsSignature(p:Record<string,unknown>|null|undefined){return ['s','tf','m','d'].map(k=>clean(first(p?.[k]))).join('|');}
export function activeTitle(a:ActiveSymbol|null|undefined){return a?`${a.symbol} · ${a.timeframe} — KANIDA`:'KANIDA';}
/** Position of the active selection in an ordered list: by matchId first, then symbol × timeframe. -1 when absent. */
export function navIndex(list:ActiveNavItem[],a:ActiveSymbol|null|undefined){if(!a)return -1;const byMatch=a.matchId?list.findIndex(x=>x.matchId===a.matchId):-1;return byMatch>=0?byMatch:list.findIndex(x=>clean(x.symbol).toUpperCase()===a.symbol&&clean(x.timeframe).toUpperCase()===a.timeframe);}
/** Next/previous item, no wrap-around. When the active selection is not in the list, next → first item and prev → last item. */
export function stepTarget(list:ActiveNavItem[],a:ActiveSymbol|null|undefined,delta:1|-1):ActiveNavItem|null{if(!list.length)return null;const i=navIndex(list,a),j=i<0?(delta>0?0:list.length-1):i+delta;return j<0||j>=list.length?null:list[j];}
const stableJson=(v:unknown):string=>v===null||typeof v!=='object'?JSON.stringify(v)??'null':Array.isArray(v)?'['+v.map(stableJson).join(',')+']':'{'+Object.keys(v as object).sort().filter(k=>(v as any)[k]!==undefined).map(k=>JSON.stringify(k)+':'+stableJson((v as any)[k])).join(',')+'}';
export type EvidenceKeyParts={symbol?:string|null;pattern?:string|null;timeframe?:string|null;side?:string|null;rule?:unknown;dataEnd?:string|null};
/** Stable evidence identity (symbol, pattern, timeframe, side, rule, data_end). `rule` may be a string id or a rule object (keys sorted). Returns '' when ANY part is missing: incomplete identity must never load or show evidence. */
export function evidenceKey(p:EvidenceKeyParts):string{
 const symbol=clean(p?.symbol).toUpperCase(),pattern=clean(p?.pattern),timeframe=clean(p?.timeframe).toUpperCase(),side=clean(p?.side).toLowerCase(),dataEnd=clean(p?.dataEnd);
 const rule=typeof p?.rule==='string'?p.rule.trim():p?.rule==null?'':stableJson(p.rule);
 if(!symbol||!pattern||!timeframe||(side!=='long'&&side!=='short')||!rule||!dataEnd)return '';
 return ['ev1',symbol,pattern,timeframe,side,rule,dataEnd].map(encodeURIComponent).join('|');
}
export type KeyedSlot<T>={key:string;value?:T;error?:string};
/** A stored slot is only readable for the exact key it was loaded for. */
export function readKeyed<T>(slot:KeyedSlot<T>|null|undefined,key:string){const fresh=!!key&&!!slot&&slot.key===key;return {value:fresh?slot!.value:undefined,error:fresh?slot!.error||'':'',fresh};}

// ---- React ----
/** Loads a value per key. While the key is new the hook returns value undefined + loading true in the SAME render the key changed (no stale frame); late results for an old key are discarded. Empty key → nothing loads. */
export function useKeyedValue<T>(key:string|null|undefined,loader:(key:string,isCurrent:()=>boolean)=>Promise<T>|T){
 const k=key||'',[slot,setSlot]=useState<KeyedSlot<T>|null>(null),[tick,setTick]=useState(0),loaderRef=useRef(loader);
 useEffect(()=>{loaderRef.current=loader});
 useEffect(()=>{if(!k){setSlot(null);return}let live=true;const isCurrent=()=>live;Promise.resolve().then(()=>loaderRef.current(k,isCurrent)).then(value=>{if(live)setSlot({key:k,value})},(e:any)=>{if(live)setSlot({key:k,error:e?.message||'Unable to load'})});return()=>{live=false}},[k,tick]);
 const read=readKeyed(slot,k),reload=useCallback(()=>setTick(t=>t+1),[]);
 return {value:read.value,error:read.error,loading:!!k&&!read.fresh,key:k,reload};
}
const ActiveContext=createContext<ActiveSymbolApi|null>(null);
/**
 * URL (web only, route "/" only): `/?s=SYMBOL&tf=TF&m=MATCHID`.
 * - Writes use router.setParams, which expo-router's linking applies with history.replace (route count unchanged) → no browser-history spam. In-app back/forward is this store's own history.
 * - Read on first load (source 'url') and when the params change while already on "/". On ARRIVING at "/" from another route the store wins and the URL is rewritten, so browser Back never reverts a newer choice.
 * - Never touches other routes (auth/onboarding/billing keep their URLs); native has no window and skips all URL work.
 * Title (web): "SYMBOL · TF — KANIDA" on WORKSPACE_TITLE_PATHS, else "KANIDA". Re-asserted after expo-router's own title writes.
 */
export function ActiveSymbolProvider({children,onWrite}:{children?:React.ReactNode;/** Called on every setActive call (accepted or not), before the write. ProductProvider uses it to drop a pending Discover id. */ onWrite?:()=>void}){
 const [history,setHistory]=useState<ActiveHistory>(emptyHistory),historyRef=useRef<ActiveHistory>(emptyHistory),onWriteRef=useRef(onWrite);onWriteRef.current=onWrite;
 const commit=useCallback((next:ActiveHistory)=>{if(next!==historyRef.current){historyRef.current=next;setHistory(next)}},[]);
 const setActive=useCallback((next:ActiveInput,opts?:{replace?:boolean})=>{onWriteRef.current?.();const h=historyRef.current,value=normalizeActive(next,h.entries[h.index]);if(!value)return false;commit(applyActive(h,value,!!opts?.replace));return true},[commit]);
 const back=useCallback(()=>commit(stepHistory(historyRef.current,-1)),[commit]),forward=useCallback(()=>commit(stepHistory(historyRef.current,1)),[commit]),reset=useCallback(()=>commit(emptyHistory),[commit]);
 const active=history.entries[history.index]||null,isActive=useCallback((symbol:string,matchId?:string)=>matchesActive(active,symbol,matchId),[active]);
 const web=Platform.OS==='web'&&typeof window!=='undefined',pathname=usePathname(),params=useGlobalSearchParams() as Record<string,unknown>,urlSig=paramsSignature(params);
 const url=useRef({loaded:false,seen:'',path:''});
 useEffect(()=>{const u=url.current,firstRun=!u.loaded,was=u.path;u.loaded=true;u.path=pathname;if(!web||pathname!==URL_PATH)return;const changedHere=was===URL_PATH&&u.seen!==urlSig,episodeLink=was!==URL_PATH&&!!clean(first(params.d));u.seen=urlSig;if(!firstRun&&!changedHere&&!episodeLink)return;const h=historyRef.current;if(urlSig===paramsSignature(serializeActiveParams(h.entries[h.index])))return;const parsed=parseActiveParams(params);if(parsed)setActive(parsed);},[web,pathname,urlSig]);
 useEffect(()=>{if(!web||pathname!==URL_PATH)return;const h=historyRef.current,cur=h.entries[h.index];if(!cur)return;const want=serializeActiveParams(cur);if(paramsSignature(want)===urlSig)return;try{router.setParams(want as any)}catch{}},[web,pathname,history,urlSig]);
 const title=activeTitle(WORKSPACE_TITLE_PATHS.includes(pathname)?active:null);
 useEffect(()=>{const doc=(globalThis as any).document;if(!web||!doc)return;const apply=()=>{if(doc.title!==title)doc.title=title};apply();const timer=setTimeout(apply,0);let observer:any;try{const MO=(globalThis as any).MutationObserver;if(MO&&doc.head){observer=new MO(apply);observer.observe(doc.head,{subtree:true,childList:true,characterData:true})}}catch{}return()=>{clearTimeout(timer);observer?.disconnect()}},[web,title]);
 const value=useMemo<ActiveSymbolApi>(()=>({active,history,setActive,back,forward,canBack:history.index>0,canForward:history.index<history.entries.length-1,isActive,reset}),[active,history,setActive,back,forward,isActive,reset]);
 return <ActiveContext.Provider value={value}>{children}</ActiveContext.Provider>;
}
export function useActiveSymbol():ActiveSymbolApi{const v=useContext(ActiveContext);if(!v)throw new Error('useActiveSymbol must be used inside ActiveSymbolProvider (mounted by ProductProvider in src/context.tsx)');return v;}
/** Ordered-list stepping for ↑/↓ or previous/next buttons. Consumers bind keys; no wrap-around. Returns whether the write was accepted. */
export function useActiveNavigation(list:ActiveNavItem[],source:ActiveSource='nav'){
 const {active,setActive}=useActiveSymbol(),index=useMemo(()=>navIndex(list,active),[list,active]);
 const go=useCallback((delta:1|-1)=>{const t=stepTarget(list,active,delta);return t?setActive({...t,source}):false},[list,active,setActive,source]);
 const next=useCallback(()=>go(1),[go]),prev=useCallback(()=>go(-1),[go]);
 return {index,next,prev,hasNext:list.length>0&&index<list.length-1,hasPrev:list.length>0&&index!==0};
}
