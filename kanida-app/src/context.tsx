import React,{createContext,useContext,useEffect,useState,useCallback,useRef,useMemo} from 'react';
import {api,Match} from './model';
import {useAuth} from './Auth';
import {DiscoverFilters,discoverDefaults} from './decision';
import {ActiveSymbolProvider,useActiveSymbol,parseMatchId} from './activeSymbol';
export const initialWorkspace={selected:'',tab:'setup',listOffset:0,sort:'return',positive:true,verdict:'all'};
const {selected:_initialSelected,...initialWorkspaceRest}=initialWorkspace;
const Context=createContext<any>(null);
export function useProduct(){return useContext(Context);}
// ProductProvider mounts the active-symbol store, so app/_layout.tsx needs no change. `workspace.selected` is DERIVED from that store (single source of truth);
// setWorkspace keeps its old signature (object or updater) and routes a selection change to setActive(source 'discover'). Filters, query and list state are never reset by a symbol change.
// Minor fix: any setActive call (from any surface) clears a pending Discover id, so an id that never resolves can't hijack a later selection. Bridged by ref because ProductState lives inside the store.
export function ProductProvider({children}:any){const onWrite=useRef<()=>void>(()=>{}),write=useCallback(()=>onWrite.current(),[]);return <ActiveSymbolProvider onWrite={write}><ProductState writeRef={onWrite}>{children}</ProductState></ActiveSymbolProvider>;}
function ProductState({children,writeRef}:any){
 const auth=useAuth();const allowed=!!auth.user?.onboarded&&!!auth.billing?.access;const ownerKey=allowed?auth.user.id:'';const generation=useRef(0);
 const store=useActiveSymbol(),{active,setActive,reset:resetActive}=store;
 const prefs=auth.user?.preferences?.timeframes;const prefFrames=Array.isArray(prefs)?prefs.join(','):'';
 const [state,setState]=useState<any>(null),[matches,setMatches]=useState<Match[]>([]),[options,setOptions]=useState<any>(null);
 // The researched detector set carries NO per-match trade history - each detection's evidence is its own
 // research card, looked up by identity. A "minimum trades" filter over that set would hide every detection
 // rather than filter it, so it defaults to 0 in that mode. Nothing else about the filters changes, and the
 // legacy default is untouched.
 const researchMode=state?.pattern_set==='research';
 const defaultFilters=useMemo(()=>{const base=discoverDefaults(prefFrames?prefFrames.split(','):[]);
  return researchMode?{...base,minimum:0}:base},[prefFrames,researchMode]);
 const [product,setProduct]=useState<any>({plans:[],events:[]}),[error,setError]=useState(''),[loading,setLoading]=useState(true);
 const [filters,setFilters]=useState<DiscoverFilters>(defaultFilters),[query,setQuery]=useState('');
 // Switching pattern set changes what a default filter MEANS, so the defaults are re-adopted - but only while
 // the user is still on them. A filter they chose themselves is never overwritten by a scanner restart.
 const appliedDefaults=useRef(defaultFilters);
 useEffect(()=>{const previous=appliedDefaults.current;if(previous===defaultFilters)return;appliedDefaults.current=defaultFilters;
  setFilters(current=>JSON.stringify(current)===JSON.stringify(previous)?defaultFilters:current)},[defaultFilters]);
 const [detail,setDetail]=useState<Match|null>(null),[plan,setPlan]=useState<{match:Match;side:string}|null>(null),[filterOpen,setFilterOpen]=useState(false);
 const [toast,setToast]=useState('');const refreshing=useRef(false);
 const [stock,setStock]=useState<any>(null);
 // Non-selection workspace UI state (tab, sort, scroll). `pendingSelected` only holds a match id whose symbol can't be resolved yet (matches still loading).
 const [workspaceRest,setWorkspaceRest]=useState<any>(initialWorkspaceRest),[pendingSelected,setPendingSelected]=useState('');
 writeRef.current=()=>setPendingSelected('');
 // Q1: the stored setup the chart auto-selected for a symbol-only selection ({symbol,timeframe,matchId,source,replaced}). The header says "Auto-selected" only while the store holds exactly that entry.
 const [autoPick,setAutoPick]=useState<any>(null);
 const selected=pendingSelected||active?.matchId||'';
 const workspace=useMemo(()=>({...workspaceRest,selected}),[workspaceRest,selected]);
 const workspaceRef=useRef(workspace);workspaceRef.current=workspace;const matchesRef=useRef<Match[]>(matches);matchesRef.current=matches;
 const selectMatch=useCallback((id:string)=>{
  if(!id){setPendingSelected('');const a=store.active;if(a?.matchId)setActive({symbol:a.symbol,timeframe:a.timeframe,source:a.source},{replace:true});return}
  const m=matchesRef.current.find(x=>x.id===id),ref=m?{symbol:m.symbol,timeframe:m.timeframe}:parseMatchId(id);
  if(ref&&setActive({symbol:ref.symbol,timeframe:ref.timeframe,matchId:id,source:'discover'})){setPendingSelected('');return}
  setPendingSelected(id);
 },[setActive,store.active]);
 const setWorkspace=useCallback((update:any)=>{const prev=workspaceRef.current,next={...(typeof update==='function'?update(prev):update)};workspaceRef.current=next;const {selected:nextSelected,...rest}=next;setWorkspaceRest(rest);if((nextSelected||'')!==(prev.selected||''))selectMatch(nextSelected||'')},[selectMatch]);
 useEffect(()=>{if(!pendingSelected)return;const m=matches.find(x=>x.id===pendingSelected);if(m&&setActive({symbol:m.symbol,timeframe:m.timeframe,matchId:m.id,source:'discover'}))setPendingSelected('')},[pendingSelected,matches,setActive]);
 const [studyDraft,setStudyDraft]=useState<any>({});
 const owner=useRef<string|null>(null);
 const refresh=useCallback(async()=>{if(!ownerKey||refreshing.current)return;const version=generation.current;refreshing.current=true;try{const [a,b,c,d]=await Promise.all([api('/api/state'),api('/api/matches?min_trades=0'),api('/api/filter-options'),api('/api/product')]);if(version!==generation.current)return;setState(a);
     // The server serves the DISPLAY match set on either pattern set: the stored 10-pattern scan, or - when
     // the scanner runs the researched 107 - today's live detections, always with an empty `history` so no
     // evidence or trade gate can read a number off them. A non-array can only mean a shape this build does
     // not understand, and an empty list is the honest reading of that.
     setMatches(Array.isArray(b)?b:[]);setOptions(c);setProduct(d);setError('');}catch(e:any){if(version===generation.current){setError(e.message||'Unable to connect');if([401,402,403].includes(e.status))auth.refresh()}}finally{if(version===generation.current){setLoading(false);refreshing.current=false;}}},[ownerKey]);
 const refreshProduct=useCallback(async()=>{if(!ownerKey)return;const version=generation.current;const value=await api('/api/product');if(version===generation.current)setProduct(value)},[ownerKey]);
 // The active symbol is only cleared on a real account switch/sign-out (not on first mount), so a /?s=&tf=&m= deep link survives sign-in.
 useEffect(()=>{generation.current++;refreshing.current=false;setState(null);setMatches([]);setOptions(null);setProduct({plans:[],events:[],watchlist:[]});setDetail(null);setPlan(null);setStock(null);setFilterOpen(false);setQuery('');setFilters(defaultFilters);setWorkspaceRest(initialWorkspaceRest);setPendingSelected('');setAutoPick(null);if(owner.current&&owner.current!==ownerKey)resetActive();owner.current=ownerKey;setStudyDraft({});setError('');setLoading(!!ownerKey);if(ownerKey)refresh();const timer=setInterval(refresh,60000);return()=>{generation.current++;clearInterval(timer)};},[ownerKey,refresh]);
 useEffect(()=>{if(toast){const timer=setTimeout(()=>setToast(''),5000);return()=>clearTimeout(timer);}},[toast]);
 return <Context.Provider value={{state,matches,options,product,error,loading,refresh,refreshProduct,filters,setFilters,defaultFilters,query,setQuery,detail,setDetail,plan,setPlan,filterOpen,setFilterOpen,toast,setToast,stock,setStock,workspace,setWorkspace,studyDraft,setStudyDraft,autoPick,setAutoPick}}>{children}</Context.Provider>;
}
