import React,{useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {View,TextInput,Pressable,FlatList,ScrollView,useWindowDimensions} from 'react-native';
import {router} from 'expo-router';
import {useProduct,initialWorkspace} from '../context';
import {C,T,s,Button,Loading,Badge,Empty,Checkbox,Icon} from '../ui';
import {Match,History,Filters,api,filterMatches,pct,dateText,histories} from '../model';
import {decision,Verdict,frameMatch,dataAgeDays,dataAgeText,isStale,ALL_FRAMES,historyScreenOff,appliedFilters,NO_HISTORY_SCREEN_NOTE,NO_HISTORY_SCREEN_LABEL} from '../decision';
import {useActiveSymbol,useActiveNavigation,useKeyedValue,selectedActiveMatch,selectedChartDetection,ActiveSymbol,ActiveSource} from '../activeSymbol';
import {webOnly} from '../layout/shared';
import {Popover,useTitle} from '../layout/index';
// Phase 0 (wave 0B) Discover results feed, split out of Workspace.tsx. Also hosts the shared Discover ordering + active-match resolution used by ChartCentre and EvidencePanel.
export const verdictTone=(v:Verdict)=>v==='review'?'green':v==='watch'?'amber':'neutral';
type Decision=ReturnType<typeof decision>;type Scored={m:Match;d:Decision};
// F6: dock rows are two dense lines (46px) so ≥3 rows fit under the toolbar in a ~260px panel; the phone sheet keeps its three-line rows.
const HEAD_HEIGHT={dock:30,sheet:40} as const,ROW_HEIGHT={dock:46,sheet:74} as const;
const toneCol=(t:string)=>t==='green'?C.green:t==='amber'?C.amber:t==='red'?C.red:C.muted,toneBg=(t:string)=>t==='amber'?C.amberBg:t==='neutral'?C.bg:C.soft;
const CAVEAT_SHORT='Top averages are partly luck: listed from many stock × pattern results.',CAVEAT_FULL='Listed from many stock × pattern results. Top averages are partly luck and usually shrink. Each 95% range covers one result alone and does not correct for comparing many stocks.';
// fix3: dock panels shorter than this use the one-line toolbar with the caveat as a pinned chip (short-window default panel = 142px; normal default panels ≥ 190px, which still fit 3 rows under the 40px compact line).
const COMPACT_PANEL=230,CAVEAT_CHIP='Top averages partly luck';
// A row gets the full selection highlight only when the store's selection came from Discover itself (or its own stepping / a restored link). Any other source → faint "active" marker (§10.2).
const STRONG_SOURCES:ActiveSource[]=['discover','nav','url','restore'];

// Expectancy decides: positive averages first, ordered by the server's per-result 95% lower bound (expectancy_ci95[0]) to damp best-of-many luck, then by the average. Non-positive averages never interleave with positives.
export function discoverSort(list:Scored[],sort:string){
 const ret=(x:Scored)=>x.d.stats?.expectancy_pct,low=(x:Scored)=>x.d.stats?.expectancy_ci95?.[0],good=(x:Scored)=>(ret(x)??0)>0;
 const order=(a:Scored,b:Scored)=>sort==='fit'?b.m.score-a.m.score:((low(b)??-Infinity)-(low(a)??-Infinity))||((ret(b)??-Infinity)-(ret(a)??-Infinity))||0;
 return {good:list.filter(good).sort(order),bad:list.filter(x=>!good(x)).sort(order)};
}
/** Today's Discover result order (filters, query, timeframes, positive-only, verdict chip, sort). Filters and list state live in ProductProvider, so a symbol change never resets them. */
export function useDiscoverRows(){
 const p=useProduct(),ws={...initialWorkspace,...p.workspace},sort:string=ws.sort,verdict:Verdict|'all'=ws.verdict;
 // "Positive average only" filters on a PER-MATCH average. The researched detector set does not produce one
 // - each detection's evidence is its own research card, looked up by identity - so the toggle has nothing
 // to act on there and is not applied: it would drop every detection rather than filter any. The control
 // reports that it is unavailable rather than silently doing nothing.
 const noAverages=p.state?.pattern_set==='research';
 const positive:boolean=ws.positive&&!noAverages;
 // "Minimum historical trades" and "past average return" read the same absent legacy history, so on the
 // researched set they are not applied either - they would empty the list rather than filter it, which is
 // the same silent deletion the scanner's /api/matches used to do on a bare request. `applied` is what the
 // rows are actually built from, and the controls below report that those two are off.
 const noHistoryScreen=historyScreenOff(p.state?.pattern_set);
 const applied=useMemo(()=>appliedFilters(p.filters,noHistoryScreen),[p.filters,noHistoryScreen]);
 const setWorkspace=p.setWorkspace,setWs=useCallback((patch:any)=>setWorkspace((w:any)=>({...w,...patch})),[setWorkspace]);
 const age=dataAgeDays(p.state?.source_latest),stale=isStale(age,p.state?.source_stale);
 const base=useMemo<Scored[]>(()=>filterMatches(p.matches,applied,p.query).filter((m:Match)=>frameMatch(m,applied)).map((m:Match)=>({m,d:decision(m,applied)})),[p.matches,applied,p.query]);
 const ranked=useMemo(()=>{
  const pool=positive?base.filter(x=>(x.d.stats?.expectancy_pct??0)>0):base,counts:Record<Verdict,number>={review:0,watch:0,pass:0};pool.forEach(x=>counts[x.d.verdict]++);
  const kept=pool.filter(x=>verdict==='all'||x.d.verdict===verdict),{good,bad}=discoverSort(kept,sort);
  // The "Zero or negative average" divider separates results whose average FAILED from those that passed.
  // Where there is no per-match average at all, every row would fall below it and the divider would assert
  // something measured about numbers that were never produced. It is not drawn there.
  return {rows:[...good,...bad].map(x=>x.m),losers:noAverages?0:bad.length,counts,decisions:new Map<string,Decision>(kept.map(x=>[x.m.id,x.d]))};
 },[base,positive,sort,verdict,noAverages]);
 return {...ranked,age,stale,sort,positive,verdict,setWs,noAverages,noHistoryScreen,applied};
}
export type DiscoverRows=ReturnType<typeof useDiscoverRows>;
/** Same history choice as the old StockWorkspace (first history eligible under the filters, else the first), preferring the store's side only when that side exists. */
export function historyFor(m:Match,f:Filters,side?:string):History|undefined{return (side?m.history.find(h=>h.side===side):undefined)||histories(m,f)[0]||m.history[0];}
/** Q1 symbol-only resolution: ONLY matches inside today's Discover rows (filters, query, timeframes, positive-only, min trades, verdict chip) are eligible; the first same symbol × timeframe in Discover order, preferring the store's side. Never falls back outside the filters. (`matches`/`filters`/`sort` are accepted for call compatibility and unused.) */
export function resolveSymbolMatch({rows,symbol,timeframe,side}:{rows:Match[];matches?:Match[];filters?:Filters;sort?:string;symbol:string;timeframe:string;side?:string}):Match|undefined{
 const listed=rows.filter(m=>m.symbol===symbol&&m.timeframe===timeframe);if(!listed.length)return undefined;
 return (side?listed.find(m=>m.history.some(h=>h.side===side)):undefined)||listed[0];
}
/** Stored setups for symbol × timeframe that the current filters exclude, in Discover order. Offered only as an explicit pick list marked "outside your filters"; never auto-selected. */
export function outsideSetups({rows,matches,filters,sort,symbol,timeframe}:{rows:Match[];matches:Match[];filters:Filters;sort:string;symbol:string;timeframe:string}):Match[]{
 const listed=new Set(rows.map(m=>m.id)),rest=(matches||[]).filter(m=>m.symbol===symbol&&m.timeframe===timeframe&&!listed.has(m.id)).map(m=>({m,d:decision(m,filters)}));
 const {good,bad}=discoverSort(rest,sort);return [...good,...bad].map(x=>x.m);
}
export type WorkspaceMatch={kind:'loading'|'error'|'empty'|'match'|'none';active:ActiveSymbol|null;match?:Match;history?:History;chartSnapshot?:any;error?:string;retry?:()=>void;symbol:string;timeframe:string;outside:boolean;replaced:boolean;/** the chart chose this setup for a symbol-only selection (label "Auto-selected") */ auto:boolean;/** kind 'none': stored setups outside the filters (explicit pick only) */ outsideSetups:Match[];age:number|null;stale:boolean};
/**
 * Which stored match the chart / evidence show. Never a match for another symbol × timeframe than the store's.
 * - store has matchId → that match while it still exists in the stored scan for the same symbol × timeframe (stable across refreshes and filter changes; "outside your current filters" when the filters exclude it).
 *   If it has disappeared → `replaced` and fall through to symbol-only.
 * - symbol-only (Q1) → resolveSymbolMatch within the current filters only. A result is WRITTEN BACK to the store (same source, replace:true) and recorded as p.autoPick, so the header says
 *   "Auto-selected: top setup for SYMBOL" and later refreshes keep that matchId. No result within the filters → kind 'none' with `outsideSetups` (explicit pick list only, never auto-selected).
 * - empty store → a pending Discover id (matches still loading) or kind 'empty'. A Simulate origin match seeds an empty store, as the old Workspace did.
 */
export function useWorkspaceMatch(d:DiscoverRows):WorkspaceMatch{
 const p=useProduct(),{active,setActive}=useActiveSymbol(),originId:string=p.studyDraft?.origin?.matchId||'',selected:string=p.workspace.selected||'',setWorkspace=p.setWorkspace,autoPick=p.autoPick,setAutoPick=p.setAutoPick;
 const episodeKey=active?.detectionId?[active.symbol,active.timeframe,active.matchId,active.detectionId,p.state?.source_latest||''].join('|'):'';
 const episode=useKeyedValue<any>(episodeKey,()=>api(`/api/chart?symbol=${encodeURIComponent(active!.symbol)}&timeframe=${encodeURIComponent(active!.timeframe)}`));
 useEffect(()=>{if(!active&&!selected&&originId)setWorkspace((w:any)=>({...w,selected:originId}))},[active,selected,originId,setWorkspace]);
 const w=useMemo<WorkspaceMatch>(()=>{
  const out={active,symbol:active?.symbol||'',timeframe:active?.timeframe||'',outside:false,replaced:false,auto:false,outsideSetups:[] as Match[],age:d.age,stale:d.stale};
  // /api/matches may be capped, filtered, or older than Discover's live detection. The exact chart
  // is authoritative for an explicit episode link; never substitute a listed neighbour or its evidence.
  if(active?.detectionId){
   if(episode.loading)return {...out,kind:'loading'};
   if(episode.error)return {...out,kind:'error',error:episode.error,retry:episode.reload};
   const match=selectedChartDetection(active,episode.value);
   return match?{...out,kind:'match',match,chartSnapshot:episode.value}:{...out,kind:'none',replaced:true};
  }
  if(p.loading)return {...out,kind:'loading'};
  if(p.error&&!p.state)return {...out,kind:'error'};
  const all:Match[]=p.matches||[],byId=(id:string)=>d.rows.find(m=>m.id===id)||all.find(m=>m.id===id);
  if(!active){const m=selected?byId(selected):undefined;return m?{...out,kind:'match',match:m,history:historyFor(m,p.filters),symbol:m.symbol,timeframe:m.timeframe,outside:!d.rows.includes(m)}:{...out,kind:'empty'}}
  let m=selectedActiveMatch(active,[...d.rows,...all]);
  if(m){const auto=!!autoPick&&autoPick.matchId===m.id&&autoPick.symbol===active.symbol&&autoPick.timeframe===active.timeframe&&autoPick.source===active.source;return {...out,kind:'match',match:m,history:historyFor(m,p.filters,active.side),outside:!d.rows.includes(m),auto,replaced:auto&&!!autoPick.replaced}}
  // An episode link must never silently open another pattern, variant or side.
  if(active.detectionId)return {...out,kind:'none',replaced:true};
  const replaced=!!active.matchId;
  m=resolveSymbolMatch({rows:d.rows,symbol:active.symbol,timeframe:active.timeframe,side:active.side});
  return m?{...out,kind:'match',match:m,history:historyFor(m,p.filters,active.side),auto:true,replaced}:{...out,kind:'none',replaced,outsideSetups:outsideSetups({rows:d.rows,matches:all,filters:p.filters,sort:d.sort,symbol:active.symbol,timeframe:active.timeframe})};
 },[p.loading,p.error,p.state,p.matches,p.filters,active,selected,d.rows,d.sort,d.age,d.stale,autoPick,episode.loading,episode.error,episode.value,episode.reload]);
 // Q1 write-back: a symbol-only (or vanished-matchId) selection that resolved within the filters is stored as that matchId, keeping the store's source, side and asOf. Idempotent across the several hook instances.
 const resolvedId=w.kind==='match'&&!!active&&!!w.match&&active.matchId!==w.match.id?w.match.id:'',replacedNow=w.replaced;
 useEffect(()=>{if(!resolvedId||!active)return;const pick={symbol:active.symbol,timeframe:active.timeframe,matchId:resolvedId,source:active.source,replaced:replacedNow};
  setAutoPick?.((v:any)=>v&&v.symbol===pick.symbol&&v.timeframe===pick.timeframe&&v.matchId===pick.matchId&&v.source===pick.source&&v.replaced===pick.replaced?v:pick);
  setActive({symbol:active.symbol,timeframe:active.timeframe,matchId:resolvedId,side:active.side,asOf:active.asOf,source:active.source},{replace:true});
 },[resolvedId,active,replacedNow,setActive,setAutoPick]);
 return w;
}
function d0Title(v:string){return v==='review'?'For review':v==='watch'?'Watch':'Pass'}

export type DiscoverPanelProps={layout:'dock'|'sheet';/** Optional: fires after a row press (the store is already updated). The legacy Workspace uses it to open the phone detail view. */ onSelect?:(m:Match)=>void};
export function DiscoverPanel({layout,onSelect}:DiscoverPanelProps){
 const p=useProduct(),{rows,losers,counts,decisions,age,stale,sort,positive,verdict,setWs,noAverages,noHistoryScreen,applied}=useDiscoverRows(),{active,setActive}=useActiveSymbol();
 const sheet=layout==='sheet',ROW=ROW_HEIGHT[layout],HEADH=HEAD_HEIGHT[layout],pad=sheet?10:10;
 const [input,setInput]=useState(p.query),[stockHits,setStockHits]=useState<any[]>([]),[retrying,setRetrying]=useState(false),[caveatOpen,setCaveatOpen]=useState(false);
 const listRef=useRef<FlatList>(null);
 // fix3 compact dock toolbar: measured panel height (the root View's own height does not depend on the toolbar mode, so no feedback loop); before the first measure, guess from the window (short desktop windows).
 const {height:winH}=useWindowDimensions(),[panelH,setPanelH]=useState(0),caveatTip=useTitle(CAVEAT_FULL);
 const onPanelLayout=useCallback((e:any)=>{const h=Math.round(e?.nativeEvent?.layout?.height||0);setPanelH(v=>Math.abs(v-h)<2?v:h)},[]);
 const compact=!sheet&&(panelH>0?panelH<COMPACT_PANEL:winH<707);
 const winners=rows.length-losers,listData:any[]=losers?[...rows.slice(0,winners),{id:'__losers__',header:true},...rows.slice(winners)]:rows,listIndex=(at:number)=>losers&&at>=winners?at+1:at;
 const navList=useMemo(()=>rows.map(m=>({symbol:m.symbol,timeframe:m.timeframe,matchId:m.id})),[rows]),nav=useActiveNavigation(navList,'discover');
 const defaults=p.defaultFilters||{},mine:string[]=defaults.frames||[],f=p.filters,changed=Number(!!f.patterns?.length)+Number(!noHistoryScreen&&!!f.band)+Number(!!f.sector)+Number(!!f.universe)+Number(f.direction!=='all')+Number(!noHistoryScreen&&f.minimum!==defaults.minimum);
 const strong=!!active&&STRONG_SOURCES.includes(active.source);
 // Symbol-only selection from another source: the faint marker goes on the row the chart resolved to.
 const resolvedId=useMemo(()=>active&&!active.matchId?resolveSymbolMatch({rows,matches:[],filters:f,sort,symbol:active.symbol,timeframe:active.timeframe,side:active.side})?.id:undefined,[active,rows,f,sort]);
 useEffect(()=>{let live=true;setStockHits([]);if(input.trim().length<2)return;const timer=setTimeout(()=>api('/api/stocks?q='+encodeURIComponent(input.trim())).then(v=>{if(live)setStockHits(v)}).catch(()=>{}),240);return()=>{live=false;clearTimeout(timer)}},[input]);
 // --- where the list sits ---------------------------------------------------------------------------------
 // `workspace.listOffset` is the USER's scroll position, and only theirs. Two scrolls here belong to the app
 // — restoring that offset when the panel mounts, and bringing the active row into view — and neither may be
 // written back as if the user had made it.
 //
 // They used to be. The active-row scroll ran with `animated:true` and `viewPosition:.4`, so it emitted a
 // stream of intermediate offsets, and it scrolled even when the row was already on screen. At mount the dock
 // is still settling, so `onScroll` persisted one of those intermediates and `onLayout` re-applied it on every
 // later layout pass. The dock fits two 46px rows in about 102px — ten pixels of slack — so a stray 18px
 // offset hid the second row, and which offset won was a coin flip (check-phase0 "short 2.11").
 //
 // Four changes, none of them a delay or a retry: the app's scrolls are unanimated; the active row is moved
 // only when it is actually off screen, and then by the smallest amount that shows it; the saved offset is
 // restored ONCE, on the first layout, not on every layout; and nothing is recorded before that restore or
 // for a scroll the app itself asked for. Nothing here depends on how long layout takes.
 const selfScrolling=useRef(false),restored=useRef(false),listOffset=useRef(0),listH=useRef(0);
 const scrollSelf=useCallback((run:()=>void)=>{selfScrolling.current=true;try{run()}catch{}},[]);
 // ONE source of truth for row geometry: the list's own `getItemLayout` and the "is the active row on screen"
 // test below both read it, so the test can never aim at a position the list would not produce.
 const itemLayout=useCallback((i:number)=>{const h=losers?winners:-1;
  return i===h?{length:HEADH,offset:ROW*i,index:i}
   :{length:ROW,offset:h>=0&&i>h?ROW*(i-1)+HEADH:ROW*i,index:i};},[losers,winners,ROW,HEADH]);
 // Keep the active row in view when the selection changes from anywhere (row press, ↑/↓, Prev/Next, Watch, URL).
 const revealActive=useCallback(()=>{
  if(nav.index<0||!listH.current)return;
  const {offset:top,length}=itemLayout(listIndex(nav.index)),y=listOffset.current;
  // Already whole on screen: do nothing. This is the mount case — the first row of a fresh list is visible,
  // so the list stays at 0 instead of being nudged to a fraction of a row.
  const next=top<y?top:top+length>y+listH.current?top+length-listH.current:null;
  if(next===null||Math.round(next)===Math.round(y))return;
  const offset=Math.max(0,next);listOffset.current=offset;
  scrollSelf(()=>listRef.current?.scrollToOffset({offset,animated:false}));
 },[nav.index,itemLayout,listIndex,scrollSelf]);
 const sig=active?`${active.symbol}|${active.timeframe}|${active.matchId||''}`:'',seen=useRef(sig);
 useEffect(()=>{if(sig===seen.current)return;seen.current=sig;revealActive()},[sig]);
 const onListScroll=useCallback((e:any)=>{
  const y=Math.round(e?.nativeEvent?.contentOffset?.y||0);listOffset.current=y;
  if(selfScrolling.current){selfScrolling.current=false;return;}
  if(!restored.current)return;
  setWs({listOffset:y});
 },[setWs]);
 const onListLayout=useCallback((e:any)=>{
  listH.current=Math.round(e?.nativeEvent?.layout?.height||0);
  if(restored.current)return;          // a later layout is a resize, not a reason to move the user's list
  restored.current=true;
  // `workspace.listOffset` lives in memory for the session only (context.tsx), so a fresh load always starts
  // at 0. If it is ever PERSISTED, clamp this restore to the current `listH.current` before applying it: a
  // position saved in a tall dock would otherwise be restored into a short one, where the list has about ten
  // pixels of slack, and check-phase0 "short 2.11" would go intermittent again — on stored state this time,
  // not on the mount race the code above closes.
  const saved=Number(p.workspace.listOffset)||0;
  if(saved>0){listOffset.current=saved;scrollSelf(()=>listRef.current?.scrollToOffset({offset:saved,animated:false}));}
  // A selection that arrived before the first layout could not be measured then; it is honoured here.
  revealActive();
 },[p.workspace.listOffset,revealActive,scrollSelf]);
 function select(m:Match){setActive({symbol:m.symbol,timeframe:m.timeframe,matchId:m.id,source:'discover'});onSelect?.(m)}
 function search(text:string){const q=text.trim();if(/\b(pnl|p&l|profit|simulation|backtest)\b/i.test(q)){p.setQuery('');setInput('');router.push('/simulate' as any);return}p.setQuery(q);setInput(q)}
 function reset(){p.setFilters(p.defaultFilters);p.setQuery('');setInput('');setWs({sort:'return',positive:true,verdict:'all'})}
 async function retry(){setRetrying(true);try{await p.refresh()}finally{setRetrying(false)}}
 // Web: ↑/↓ step the active stock while focus is anywhere inside the panel (not while typing in the search box).
 const onKeyDown=useCallback((e:any)=>{const k=e?.key??e?.nativeEvent?.key;if(k!=='ArrowDown'&&k!=='ArrowUp')return;const tag=String(e?.target?.tagName||'').toUpperCase();if(tag==='INPUT'||tag==='TEXTAREA'||e?.altKey||e?.ctrlKey||e?.metaKey)return;e.preventDefault?.();if(k==='ArrowDown')nav.next();else nav.prev()},[nav.next,nav.prev]);
 if(p.loading)return <Loading/>;
 if(p.error&&!p.state)return <View style={{flex:1,padding:16}}><Empty title="Research couldn’t load" detail={p.error} action={<Button label="Retry" icon="rotate-ccw" loading={retrying} onPress={retry}/>}/></View>;
 const tfList=[...(mine.length?['Mine']:[]),'All',...ALL_FRAMES],tfOn=(tf:string)=>tf==='Mine'?f.timeframe==='All'&&!!f.frames?.length:tf==='All'?f.timeframe==='All'&&!f.frames?.length:f.timeframe===tf;
 const verdictList=([['all',`All ${counts.review+counts.watch+counts.pass}`],['review',`For review ${counts.review}`],['watch',`Watch ${counts.watch}`],['pass',`Pass ${counts.pass}`]] as [Verdict|'all',string][]),stocks=new Set(rows.map(m=>m.symbol)).size;
 const chip=(on:boolean,border=true)=>({minHeight:28,paddingHorizontal:8,justifyContent:'center' as const,borderRadius:7,borderWidth:border?1:0,borderColor:on?C.green:C.line,backgroundColor:on?C.soft:'transparent'}),chipT=(on:boolean)=>({fontSize:12,lineHeight:16,color:on?C.green:C.muted});
 // F6 dock: one toolbar that stays put above the scrolling rows: search · Filters (N changed) · TF · verdict chips with counts · sort · positive-only · min trades · reset · count · data age.
 // Normal panels: the toolbar wraps only when narrow and the "partly luck" caveat stays visible as one muted line; ⓘ expands the full text.
 // fix3 compact (panel < COMPACT_PANEL): ONE line; the controls scroll horizontally inside it and the caveat is a pinned, always-visible chip "Top averages partly luck ⓘ" (full text: hover title, accessible name, popover).
 const items=<>
   <TextInput accessibilityLabel="Search stocks or patterns" value={input} onChangeText={v=>{setInput(v);p.setQuery(v)}} onSubmitEditing={()=>search(input)} returnKeyType="search" placeholder="Stock, pattern or P&L…" placeholderTextColor={C.muted} style={[s.input,compact?{width:170,flexShrink:0}:{flexGrow:1,flexBasis:150,minWidth:120,maxWidth:240},{height:30,minHeight:30,paddingVertical:0,paddingHorizontal:9,fontSize:13}]}/>
   <Pressable accessibilityRole="button" accessibilityLabel={`Filters${changed?`, ${changed} changed`:''}`} onPress={()=>p.setFilterOpen(true)} style={[s.row,chip(!!changed),{gap:4}]}><Icon name="sliders" size={12} color={changed?C.green:C.muted}/><T style={chipT(!!changed)}>{`Filters${changed?` (${changed} changed)`:''}`}</T></Pressable>
   {tfList.map(tf=>{const on=tfOn(tf);return <Pressable key={tf} accessibilityRole="button" accessibilityState={{selected:on}} accessibilityLabel={tf==='Mine'?`My timeframes: ${mine.join(', ')}`:tf==='All'?'All timeframes':tf} onPress={()=>p.setFilters({...f,timeframe:tf==='Mine'?'All':tf,frames:tf==='Mine'?mine:[]})} style={chip(on,false)}><T style={chipT(on)}>{tf==='Mine'?`Mine · ${mine.join(' ')}`:tf}</T></Pressable>})}
   {verdictList.map(([v,l])=><Pressable key={v} accessibilityRole="button" accessibilityLabel={`Show ${l}`} accessibilityState={{selected:verdict===v}} onPress={()=>setWs({verdict:v})} style={chip(verdict===v)}><T style={chipT(verdict===v)}>{l}</T></Pressable>)}
   <Pressable accessibilityRole="button" accessibilityLabel={sort==='fit'?'Sorted by pattern fit. Sort by the 95% lower bound of hold-period average net per trade':'Sorted by the 95% lower bound of hold-period average net per trade. Sort by pattern fit'} onPress={()=>setWs({sort:sort==='fit'?'return':'fit'})} style={chip(true,false)}><T style={chipT(true)}>{sort==='fit'?'Pattern fit ↓':'95% low of avg ↓'}</T></Pressable>
   <Pressable accessibilityRole="checkbox" accessibilityState={{checked:positive,disabled:noAverages}} {...webOnly({'aria-checked':positive,'aria-disabled':noAverages})} disabled={noAverages} accessibilityLabel={noAverages?'Positive average only is unavailable: these detections carry no per-match average':'Positive average only'} onPress={()=>setWs({positive:!positive})} style={[s.row,chip(positive),{gap:4,opacity:noAverages?.45:1}]}><Icon name={positive?'check-square':'square'} size={12} color={positive?C.green:C.muted}/><T style={chipT(positive)}>Positive average only</T></Pressable>
   {noHistoryScreen
    ?<View accessibilityRole="text" accessibilityLabel={NO_HISTORY_SCREEN_NOTE} style={[chip(false),{opacity:.45}]}><T style={{fontSize:12,lineHeight:16,color:C.amber}}>{NO_HISTORY_SCREEN_LABEL}</T></View>
    :<Pressable accessibilityRole="button" accessibilityLabel={f.minimum>0?`Minimum ${f.minimum} historical trades is on. Change in Filters`:'Any sample size. Change in Filters'} onPress={()=>p.setFilterOpen(true)} style={chip(f.minimum>0)}><T style={{fontSize:12,lineHeight:16,color:f.minimum>0?C.green:C.amber}}>{f.minimum>0?`Min ${f.minimum} trades ›`:'Any sample size ›'}</T></Pressable>}
   <Pressable accessibilityRole="button" accessibilityLabel="Reset filters to the safe defaults" onPress={reset} style={chip(false,false)}><T style={chipT(false)}>Reset</T></Pressable>
   <T style={{fontSize:11,lineHeight:16,color:C.muted}}>{rows.length} {rows.length===1?'setup':'setups'} · {stocks} {stocks===1?'stock':'stocks'}</T>
   <View style={[s.row,{gap:6,marginLeft:'auto'}]}><T accessibilityLabel={`Stored market ${dateText(p.state?.source_latest)}, ${dataAgeText(age)}`} style={{fontSize:11,lineHeight:16,color:stale?C.amber:C.muted}}>{dataAgeText(age)}</T>{!!p.error&&<Pressable accessibilityRole="button" accessibilityLabel="Research refresh failed. Retry" onPress={retry} style={{minHeight:28,justifyContent:'center'}}><T style={{fontSize:11,color:C.red}}>{retrying?'Retrying…':'Refresh failed · Retry'}</T></Pressable>}</View>
 </>;
 const extrasOn=(!!input&&(/pnl|p&l|backtest|profit|simulat/i.test(input)||stockHits.length>0))||!!f.patterns?.length;
 const caveatLine=<Pressable accessibilityRole="button" accessibilityLabel={`${CAVEAT_FULL} ${caveatOpen?'Collapse':'Show'} the full note`} accessibilityState={{expanded:caveatOpen}} {...webOnly({'aria-expanded':caveatOpen})} onPress={()=>setCaveatOpen(v=>!v)} style={[s.row,{gap:5,minHeight:20,alignItems:'flex-start'}]}>
   <T numberOfLines={caveatOpen?undefined:1} style={{flexShrink:1,minWidth:0,fontSize:11,lineHeight:16,color:caveatOpen?C.amber:C.muted}}>{caveatOpen?CAVEAT_FULL:CAVEAT_SHORT}</T><T style={{fontSize:11,lineHeight:16,color:C.green}}>ⓘ</T>
  </Pressable>;
 const caveatChip=<Pressable ref={caveatTip} accessibilityRole="button" accessibilityLabel={`${CAVEAT_FULL} Show the full note`} accessibilityState={{expanded:caveatOpen}} {...webOnly({'aria-expanded':caveatOpen})} onPress={()=>setCaveatOpen(true)} style={[s.row,chip(false),{gap:4,flexShrink:0}]}>
   <T numberOfLines={1} style={{fontSize:11,lineHeight:16,color:C.amber}}>{CAVEAT_CHIP}</T><T style={{fontSize:11,lineHeight:16,color:C.green}}>ⓘ</T>
  </Pressable>;
 const extras=<>
  {!!input&&(/pnl|p&l|backtest|profit|simulat/i.test(input)||stockHits.length>0)&&<View style={[s.row,{flexWrap:'wrap',gap:4}]}>{/pnl|p&l|backtest|profit|simulat/i.test(input)&&<Pressable accessibilityRole="button" accessibilityLabel="Open historical P&L simulation" onPress={()=>router.push('/simulate' as any)} style={chip(true)}><T style={chipT(true)}>Open historical P&L simulation</T></Pressable>}{stockHits.slice(0,3).map(v=><Pressable key={v.symbol} accessibilityRole="button" accessibilityLabel={`Search ${v.symbol}`} onPress={()=>{if(filterMatches(p.matches,p.filters,v.symbol).length)search(v.symbol);else p.setStock(v)}} style={[chip(false),{maxWidth:260}]}><T numberOfLines={1} style={chipT(false)}><T style={{fontSize:12,color:C.ink}}>{v.symbol}</T> {v.company}</T></Pressable>)}</View>}
  {!!f.patterns?.length&&<T numberOfLines={1} style={{fontSize:11,lineHeight:15,color:C.green}}>{p.state?.patterns?.filter((v:any)=>f.patterns.includes(v.id)).map((v:any)=>v.name).join(' · ')}</T>}
 </>;
 const dockToolbar=compact?<View style={{borderBottomWidth:1,borderColor:C.line}}>
  <View style={[s.row,{gap:4,paddingLeft:8,paddingRight:6,paddingTop:5,paddingBottom:4}]}>
   <ScrollView horizontal showsHorizontalScrollIndicator={false} keyboardShouldPersistTaps="handled" style={{flex:1,minWidth:0}} contentContainerStyle={{flexGrow:1,alignItems:'center'}}>
    <View accessibilityRole="toolbar" accessibilityLabel="Discover search, filters and sort" style={[s.row,{flexGrow:1,gap:4}]}>{items}</View>
   </ScrollView>
   {caveatChip}
  </View>
  {extrasOn&&<View style={{paddingHorizontal:8,paddingBottom:4,gap:3}}>{extras}</View>}
  <Popover open={compact&&caveatOpen} onClose={()=>setCaveatOpen(false)} anchor={caveatTip} placement="top-end" label="Discover caveat: top averages are partly luck" width={320}><T style={{fontSize:12,lineHeight:18,color:C.amber,paddingHorizontal:12,paddingVertical:6}}>{CAVEAT_FULL}</T></Popover>
 </View>
 :<View style={{paddingHorizontal:8,paddingTop:6,paddingBottom:3,gap:3,borderBottomWidth:1,borderColor:C.line}}>
  <View accessibilityRole="toolbar" accessibilityLabel="Discover search, filters and sort" style={[s.row,{flexWrap:'wrap',gap:4,rowGap:4}]}>{items}</View>
  {extras}
  {caveatLine}
 </View>;
 return <View accessibilityLabel="Discover results. Up and down arrow keys change the active stock." onLayout={onPanelLayout} style={{flex:1,minHeight:0}} {...webOnly({tabIndex:0,onKeyDown})}>
  {!sheet?dockToolbar:<View style={{padding:pad,gap:sheet?4:6}}>
   <View style={[s.between,{gap:8,flexWrap:'wrap'}]}><T style={{fontSize:12,color:stale?C.amber:C.muted,flexShrink:1}}>Stored market · {dateText(p.state?.source_latest)} · {dataAgeText(age)}</T>{!!p.error&&<Pressable accessibilityRole="button" accessibilityLabel="Research refresh failed. Retry" onPress={retry} style={{minHeight:32,justifyContent:'center'}}><T style={{fontSize:12,color:C.red}}>{retrying?'Retrying…':'Refresh failed · Retry'}</T></Pressable>}</View>
   <View style={[s.row,{gap:4}]}><TextInput accessibilityLabel="Search stocks or patterns" value={input} onChangeText={v=>{setInput(v);p.setQuery(v)}} onSubmitEditing={()=>search(input)} placeholder="Stock, pattern or P&L…" placeholderTextColor={C.muted} style={[s.input,{flex:1,minWidth:0,fontSize:14}]}/><Button label="Search" icon="search" kind="ghost" onPress={()=>search(input)} style={{paddingHorizontal:10}}/></View>
   {!!input&&<View style={{gap:6}}>{/pnl|p&l|backtest|profit|simulat/i.test(input)&&<Button label="Open historical P&L simulation" kind="soft" onPress={()=>router.push('/simulate' as any)}/>}{stockHits.slice(0,3).map(v=><Pressable key={v.symbol} accessibilityRole="button" accessibilityLabel={`Search ${v.symbol}`} onPress={()=>{if(filterMatches(p.matches,p.filters,v.symbol).length)search(v.symbol);else p.setStock(v)}} style={{paddingVertical:5}}><T>{v.symbol} <T style={{fontSize:12,color:C.muted}}>{v.company}</T></T></Pressable>)}</View>}
   <View style={s.between}><Button label={`Filters${changed?` · ${changed} changed`:''}`} icon="sliders" kind="outline" onPress={()=>p.setFilterOpen(true)} style={{flex:1}}/><Button label="Reset" kind="ghost" onPress={reset}/></View>
   <View style={[s.row,{gap:4,flexWrap:'wrap'}]}>{[...(mine.length?['Mine']:[]),'All',...ALL_FRAMES].map(tf=>{const on=tf==='Mine'?f.timeframe==='All'&&!!f.frames?.length:tf==='All'?f.timeframe==='All'&&!f.frames?.length:f.timeframe===tf;return <Pressable key={tf} accessibilityRole="button" accessibilityState={{selected:on}} accessibilityLabel={tf==='Mine'?`My timeframes: ${mine.join(', ')}`:tf==='All'?'All timeframes':tf} onPress={()=>p.setFilters({...f,timeframe:tf==='Mine'?'All':tf,frames:tf==='Mine'?mine:[]})} style={{minHeight:sheet?36:40,paddingHorizontal:tf==='Mine'?8:12,justifyContent:'center',borderRadius:7,backgroundColor:on?C.soft:'transparent'}}><T style={{color:on?C.green:C.muted,fontSize:13}}>{tf==='Mine'?`Mine · ${mine.join(' ')}`:tf}</T></Pressable>})}</View>
   {!!f.patterns?.length&&<T style={{fontSize:12,color:C.green}}>{p.state?.patterns?.filter((v:any)=>f.patterns.includes(v.id)).map((v:any)=>v.name).join(' · ')}</T>}
   <View style={s.between}><T style={{fontSize:12,color:C.muted}}>{rows.length} {rows.length===1?'setup':'setups'} · {new Set(rows.map(m=>m.symbol)).size} {new Set(rows.map(m=>m.symbol)).size===1?'stock':'stocks'}</T><Pressable accessibilityRole="button" accessibilityLabel={sort==='fit'?'Sorted by pattern fit. Sort by the 95% lower bound of hold-period average net per trade':'Sorted by the 95% lower bound of hold-period average net per trade. Sort by pattern fit'} onPress={()=>setWs({sort:sort==='fit'?'return':'fit'})} style={{minHeight:32,justifyContent:'center'}}><T style={{fontSize:12,color:C.green}}>{sort==='fit'?'Pattern fit ↓':'95% low of avg ↓'}</T></Pressable></View>
   <View style={s.between}><Checkbox checked={positive} disabled={noAverages} onChange={(v:boolean)=>setWs({positive:v})} label={noAverages?'Positive average only (no per-match average)':'Positive average only'} style={{minHeight:32,paddingVertical:6,flexShrink:1}}/>{noHistoryScreen
    ?<View accessibilityRole="text" accessibilityLabel={NO_HISTORY_SCREEN_NOTE} style={{minHeight:32,justifyContent:'center',opacity:.45}}><T style={{fontSize:12,color:C.amber}}>{NO_HISTORY_SCREEN_LABEL}</T></View>
    :<Pressable accessibilityRole="button" accessibilityLabel={f.minimum>0?`Minimum ${f.minimum} historical trades is on. Change in Filters`:'Any sample size. Change in Filters'} onPress={()=>p.setFilterOpen(true)} style={{minHeight:32,justifyContent:'center'}}><T style={{fontSize:12,color:f.minimum>0?C.green:C.amber}}>{f.minimum>0?`Min ${f.minimum} trades ›`:'Any sample size ›'}</T></Pressable>}</View>
   <T accessibilityRole="text" style={{fontSize:11,lineHeight:15,color:C.amber}}>Listed from many stock × pattern results. Top averages are partly luck and usually shrink. Each 95% range covers one result alone and does not correct for comparing many stocks.</T>
   <View style={[s.row,{gap:4,flexWrap:'wrap'}]}>{([['all',`All ${counts.review+counts.watch+counts.pass}`],['review',`For review ${counts.review}`],['watch',`Watch ${counts.watch}`],['pass',`Pass ${counts.pass}`]] as [Verdict|'all',string][]).map(([v,l])=><Pressable key={v} accessibilityRole="button" accessibilityLabel={`Show ${l}`} accessibilityState={{selected:verdict===v}} onPress={()=>setWs({verdict:v})} style={{minHeight:sheet?32:36,paddingHorizontal:8,justifyContent:'center',borderRadius:7,borderWidth:1,borderColor:verdict===v?C.green:C.line,backgroundColor:verdict===v?C.soft:'transparent'}}><T style={{fontSize:12,color:verdict===v?C.green:C.muted}}>{l}</T></Pressable>)}</View>
  </View>}
  <FlatList showsVerticalScrollIndicator={false} getItemLayout={(_,i)=>itemLayout(i)} ref={listRef} data={listData} keyExtractor={(m:any)=>m.id} style={{flex:1,minHeight:0}} initialNumToRender={12} keyboardShouldPersistTaps="handled" onScroll={onListScroll} scrollEventThrottle={150} onLayout={onListLayout} renderItem={({item})=>{
   if(item.header)return <View accessibilityRole="header" style={{height:HEADH,paddingHorizontal:pad,justifyContent:'center',borderBottomWidth:1,borderColor:C.line,backgroundColor:C.bg}}><T style={{fontSize:12,fontFamily:'InterSemi',color:C.amber}}>Zero or negative average ({losers})</T></View>;
   const m=item as Match,d=decisions.get(m.id)||decision(m,p.filters),st=d.stats,exact=active?.matchId===m.id,on=exact&&strong,faint=!on&&(exact||m.id===resolvedId),line={fontSize:12,lineHeight:sheet?16:17,color:C.muted};
   const a11y=`Inspect ${m.symbol} ${m.timeframe} ${m.pattern_name}. ${d.title}. ${pct(st?.display_return_pct)} hold-period average net per trade${st?.expectancy_ci95?`, 95% low ${pct(st.expectancy_ci95[0])}`:''}, ${st?.n||0} trades, ${d.sample.label}. ${dataAgeText(age)}${faint?'. Active stock':''}`;
   // F6 dense dock row: line 1 symbol · verdict · hold-period avg · 95% low · n + sample label; line 2 (muted) pattern · TF · data age · tested hold.
   if(!sheet)return <Pressable accessibilityRole="button" accessibilityLabel={a11y} accessibilityState={{selected:on}} onPress={()=>select(m)} style={{height:ROW,paddingHorizontal:pad,justifyContent:'center',gap:1,borderLeftWidth:3,borderLeftColor:on?C.green:faint?'#39E5A366':'transparent',borderBottomWidth:1,borderBottomColor:C.line,backgroundColor:on?C.soft:'transparent'}}>
    <View style={[s.row,{gap:6,minWidth:0}]}>
     <T numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:13,lineHeight:18,flexShrink:0}}>{m.symbol}</T>
     <View style={{flexShrink:0,borderRadius:4,paddingHorizontal:5,backgroundColor:toneBg(verdictTone(d.verdict))}}><T style={{fontSize:10,lineHeight:16,color:toneCol(verdictTone(d.verdict))}}>{d.title}</T></View>
     {faint&&<T style={{fontSize:10,lineHeight:14,color:C.green,opacity:.75,flexShrink:0}}>active</T>}
     <T numberOfLines={1} style={{flex:1,minWidth:0,fontSize:12,lineHeight:18,color:C.muted,textAlign:'right'}}><T style={{fontSize:10,color:C.muted}}>hold-period avg </T><T style={{fontSize:12,color:(st?.expectancy_pct||0)>0?C.green:C.muted}}>{pct(st?.display_return_pct)}</T>{st?.expectancy_ci95?` · 95% low ${pct(st.expectancy_ci95[0])}`:''} · n {st?.n||0} · <T style={{fontSize:11,color:toneCol(d.sample.tone)}}>{d.sample.label}</T></T>
    </View>
    <T numberOfLines={1} style={{fontSize:11,lineHeight:15,color:C.muted}}>{m.pattern_name} · {m.timeframe} · <T style={{fontSize:11,color:stale?C.amber:C.muted}}>{dataAgeText(age)}</T> · {st?.holding?.duration||'Hold unavailable'}</T>
   </Pressable>;
   return <Pressable accessibilityRole="button" accessibilityLabel={a11y} accessibilityState={{selected:on}} onPress={()=>select(m)} style={{height:ROW,paddingHorizontal:pad,paddingVertical:sheet?5:8,gap:sheet?1:3,borderLeftWidth:3,borderLeftColor:on?C.green:faint?'#39E5A366':'transparent',borderBottomWidth:1,borderBottomColor:C.line,backgroundColor:on?C.soft:'transparent'}}>
    <View style={s.between}><View style={[s.row,{gap:6,flex:1,minWidth:0}]}><T style={{fontFamily:'InterSemi',fontSize:sheet?14:15}}>{m.symbol}</T><Badge label={d.title} tone={verdictTone(d.verdict)}/>{faint&&<T style={{fontSize:10,lineHeight:14,color:C.green,opacity:.75}}>active</T>}</View><T style={{fontSize:13,color:(st?.expectancy_pct||0)>0?C.green:C.muted}}><T style={{fontSize:10,color:C.muted}}>hold-period avg </T>{pct(st?.display_return_pct)}</T></View>
    <T numberOfLines={1} style={line}>{m.pattern_name} · {m.timeframe} · <T style={{...line,color:stale?C.amber:C.muted}}>{dataAgeText(age)}</T></T>
    <View style={[s.row,{gap:6}]}><T numberOfLines={1} style={{...line,flexShrink:1}}>{st?.n||0} trades · {st?.expectancy_ci95?`95% low ${pct(st.expectancy_ci95[0])} · `:''}{st?.holding?.duration||'Hold unavailable'}</T><Badge label={d.sample.label} tone={d.sample.tone}/></View>
   </Pressable>}} ListEmptyComponent={p.error?<View style={{padding:20,gap:15}}><T>Research couldn’t refresh.</T><T style={s.muted}>{p.error}</T><Button label="Retry" loading={retrying} onPress={retry}/></View>:<View style={{padding:20,gap:15}}><T>No setups match these filters.</T><T style={s.muted}>{[applied.minimum>0?`Minimum ${applied.minimum} trades`:'',positive?'positive average only':'',verdict!=='all'?`${d0Title(verdict)} only`:''].filter(Boolean).join(' · ')||'Try another pattern or clear your search.'}. Change them in Filters, or reset to the safe defaults.</T><Button label="Reset to defaults" onPress={reset}/></View>}/>
 </View>;
}
