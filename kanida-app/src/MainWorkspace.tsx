import React,{useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {View,ScrollView,Pressable,Platform,useWindowDimensions} from 'react-native';
import {router,usePathname,useGlobalSearchParams} from 'expo-router';
import {useSafeAreaInsets} from 'react-native-safe-area-context';
import {useProduct} from './context';
import {C,T,Button,Sheet,useLayoutMode} from './ui';
import {useActiveSymbol,useActiveNavigation,serializeActiveParams} from './activeSymbol';
// Explicit /index: on case-insensitive filesystems './workspace' resolves to src/Workspace.tsx.
import {ChartCentre,DiscoverPanel,EvidencePanel,useDiscoverRows,useWorkspaceMatch} from './workspace/index';
import {CHART_MIN_HEIGHT,CHART_MIN_HEIGHT_SHORT} from './workspace/ChartCentre';
import {watchWidget,evidenceSummaryWidget,WatchWidget,EvidenceSummaryWidget,WATCH_WIDGET_KEY,EVIDENCE_WIDGET_KEY} from './widgets/index';
import {TopBar,DataAgePill,DataStatusPopover,ToolRail,Sidebar,Dock,BottomSheet,PanelVisible,readStore,writeStore,DOCK_TAB_BAR,DOCK_HANDLE,type DockTab,type DockStore,type SheetSnap,type ToolRailItem,type SidebarRailItem} from './layout/index';
import {SimulationDesk} from './SimulationDesk';
import {AutoTrade,Activity} from './PilotScreens';
import {WatchDesk,DeskEmbedded} from './TraderDesk';
import {SymbolSearch} from './shell/SymbolSearch';
import {ShellMark,WatchToggle,AccountButton,ConnectButton,ResearchBanner} from './shell/parts';
import {MainNav,NAV_FULL_WIDTH} from './shell/MainNav';
import {IconButton} from './layout/shared';
import {ROUTE_TAB,TAB_ROUTE,CHART_PATH,type DockTabKey} from './shell/routes';
// Phase 0 (wave 0B, items 0.4–0.7) region shell, mounted ONCE by PilotShell for "/chart", "/simulate", "/watch", "/autotrade" and "/activity" (TRENDSPIDER_STUDY §10.1/§10.5/§10.6).
// TopBar carries the shared MainNav (Falcon · Discover Strategies · Watchlist · AutoTrade, FALCON_DISCOVER_SPEC §1); on phone the nav is a bottom tab bar under the sheet.
// desktop ≥1050: TopBar · left ToolRail · ChartCentre · right Sidebar (Watch + Evidence summary) · bottom Dock. tablet 760–1049: TopBar · ChartCentre · Dock, widgets in a right slide-over. phone <760: TopBar · chart (~45%) · widget cards · BottomSheet with the dock content.
// Existing screens are mounted as-is in dock tabs (content, gates and labels unchanged). URL ↔ tab: deep links open their tab full screen; a tab switch router.replace()s to the tab's route ("/chart" keeps ?s=&tf=&m=; /chart?tab=evidence opens Evidence/Replay).
const LAYOUT_KEY='main',PEEK=64;
const DESK_TABS:DockTabKey[]=['discover','evidence','simulate','autotrade','activity','watch'];
const PHONE_TABS:DockTabKey[]=['discover','watch','simulate','autotrade','activity'];
const LABEL:Record<DockTabKey,string>={discover:'Discover',evidence:'Evidence/Replay',simulate:'Simulate',autotrade:'Plans/AutoTrade',activity:'Activity',watch:'Watch'};
const PHONE_LABEL:Record<DockTabKey,string>={...LABEL,evidence:'Evidence',autotrade:'AutoTrade'};
const WIDGET_KEYS=[WATCH_WIDGET_KEY,EVIDENCE_WIDGET_KEY];
type DockView={tab:DockTabKey;full:boolean};
// Dock sizing (F5, retuned fix3). avail = window − top bar. Whole-dock heights: Discover default ≈34% of avail, space-hungry tabs ≈60%; floor = a 190px panel (compact one-line Discover toolbar 40 + 3×46 rows + slack);
// ceiling = 70% of avail AND whatever still leaves the chart centre its chrome (one-line header + slim canvas controls ≈ CENTRE_CHROME) plus the chart floor: 220px, or 190px on short windows (avail < SHORT_AVAIL).
// CENTRE_CHROME is a CONTRACT, not a guess: the chart column's flow may hold ONLY the one-line stock header (46), the
// pane padding (10) and the slim canvas control row (34) + gap. Anything else that wants to sit there (drawing notes,
// captions, banners) comes straight off the plot and breaks the floor, so it belongs on the legend overlay instead
// (PatternCanvas `onNotes` → LegendOverlay). Raising CENTRE_CHROME is not the fix: the dock has no height to give on a
// short window (it needs ≥2 Discover rows).
// The chart floor wins over the panel floor on short windows (where the dock offers a labelled Full screen button). The floor is passed to ChartCentre so the SVG minimum matches it (no clipping).
const HUNGRY_TABS:DockTabKey[]=['simulate','autotrade','activity','watch','evidence'];
const TOP_CHROME=57,CENTRE_CHROME=98,SHORT_AVAIL=650,PANEL_FLOOR=190,BASE_SHARE=.34,TALL_SHARE=.6;
export function dockSizes(winH:number,insetTop=0){
 const avail=Math.max(0,winH-TOP_CHROME-insetTop),frame=DOCK_TAB_BAR+DOCK_HANDLE+2,chartFloor=avail<SHORT_AVAIL?CHART_MIN_HEIGHT_SHORT:CHART_MIN_HEIGHT;
 const max=Math.max(frame+60,Math.min(Math.round(avail*.7),avail-CENTRE_CHROME-chartFloor)),min=Math.min(frame+PANEL_FLOOR,max),at=(f:number)=>Math.max(min,Math.min(max,Math.round(avail*f)));
 return {min,max,base:at(BASE_SHARE),tall:at(TALL_SHARE),chartFloor};
}
type GoOpts={full?:boolean;snap?:SheetSnap};
/** Deep link → its tab, full screen. "/chart?tab=evidence" → Evidence/Replay. "/chart" → the persisted dock tab when it lives on "/chart" (Discover or Evidence/Replay), else Discover. A persisted route tab (e.g. Simulate) is not restored on "/chart", because its URL would then disagree with the tab. */
function initialView(path:string,tab=''):DockView{const t=ROUTE_TAB[path];if(t)return {tab:t,full:true};if(path===CHART_PATH&&tab==='evidence')return {tab:'evidence',full:false};return {tab:readStore<DockStore>('dock',LAYOUT_KEY).active==='evidence'?'evidence':'discover',full:false};}
// memo + stable callbacks: tab switches, dock resize/collapse and sidebar changes do not re-render the chart.
const ChartSlot=React.memo(function ChartSlot({phone=false,onOpenEvidence,minChartHeight}:{phone?:boolean;onOpenEvidence:()=>void;minChartHeight?:number}){return <ChartCentre phone={phone} compactLegend={phone} onOpenEvidence={onOpenEvidence} minChartHeight={minChartHeight}/>;});
/** Full-page screens (DeskPage) inside a dock tab: the shell used to provide their ScrollView; DeskEmbedded drops the 30px page padding. */
function EmbeddedPage({children}:{children:React.ReactNode}){return <DeskEmbedded.Provider value={true}><ScrollView keyboardShouldPersistTaps="handled" style={{flex:1,minHeight:0}} contentContainerStyle={{flexGrow:1}}>{children}</ScrollView></DeskEmbedded.Provider>;}
// F8: `open` = the sheet shows content (not peek); PanelVisible lets polling screens (AutoTrade) pause while hidden.
const SheetPanel=React.memo(function SheetPanel({visible,open=true,label,render}:{visible:boolean;open?:boolean;label:string;render:()=>React.ReactNode}){return <View role="tabpanel" aria-label={label} aria-hidden={!visible} importantForAccessibility={visible?'auto':'no-hide-descendants'} style={{flex:1,minHeight:0,display:visible?'flex':'none'}}><PanelVisible.Provider value={visible&&open}>{render()}</PanelVisible.Provider></View>;});

export function MainWorkspace(){
 const mode=useLayoutMode(),desktop=mode==='desktop',phone=mode==='phone',path=usePathname(),tabParam=String(useGlobalSearchParams().tab||''),p=useProduct(),{active,setActive}=useActiveSymbol(),{height:winH,width:winW}=useWindowDimensions(),inset=useSafeAreaInsets();
 const [view,setView]=useState<DockView>(()=>initialView(path,tabParam)),[snap,setSnap]=useState<SheetSnap>(()=>ROUTE_TAB[path]?'full':path===CHART_PATH&&tabParam==='evidence'?'half':'peek'),[simKey,setSimKey]=useState(0),[widgetsOpen,setWidgetsOpen]=useState(false),[dataOpen,setDataOpen]=useState(false);
 const dock=useMemo(()=>dockSizes(winH,inset.top),[winH,inset.top]);
 const pathRef=useRef(path),expected=useRef(path),activeRef=useRef(active),draftRef=useRef(p.studyDraft),dataRef=useRef<any>(null);pathRef.current=path;activeRef.current=active;draftRef.current=p.studyDraft;
 // Tab change from inside the shell. The URL follows with router.replace (no history spam); `expected` marks that path change as ours so the route effect below ignores it.
 const go=useCallback((tab:DockTabKey,opts:GoOpts={})=>{
  setView(v=>v.tab===tab&&(opts.full==null||opts.full===v.full)?v:{tab,full:opts.full??v.full});
  if(opts.snap)setSnap(opts.snap);
  const target=TAB_ROUTE[tab];if(target===pathRef.current)return;expected.current=target;pathRef.current=target;
  const params=target===CHART_PATH?Object.fromEntries(Object.entries(serializeActiveParams(activeRef.current)).filter(([,v])=>!!v)):{};
  try{router.replace((Object.keys(params).length?{pathname:CHART_PATH,params}:target) as any)}catch{}
 },[]);
 // Navigation from elsewhere (in-app links such as "Test these rules in Simulate →", Watch "Review", browser back/forward): a route tab opens full screen; "/chart" returns to the chart.
 useEffect(()=>{
  if(path===expected.current)return;expected.current=path;const t=ROUTE_TAB[path];
  if(t){setView({tab:t,full:true});setSnap('full');if(t==='simulate')setSimKey(k=>k+1);return}
  if(path===CHART_PATH){setView(v=>TAB_ROUTE[v.tab]===CHART_PATH?(v.full?{...v,full:false}:v):{tab:'discover',full:false});setSnap(v=>v==='full'?'half':v)}
 },[path]);
 // SimulationDesk reads the study draft only when it mounts (the old /simulate route mounted it on every visit). A new draft origin (openSimulate creates one; the desk's own writes keep it) or a navigation to /simulate remounts it, so the new rules load; plain tab switches keep the draft.
 const origin=p.studyDraft?.origin,seenOrigin=useRef(origin);
 useEffect(()=>{if(origin===seenOrigin.current)return;seenOrigin.current=origin;if(origin)setSimKey(k=>k+1)},[origin]);
 const openEvidence=useCallback(()=>go('evidence',{full:false,snap:'half'}),[go]);
 // /chart?tab=evidence (Discover Strategies' "Open evidence & replay →"): open Evidence/Replay, then drop the one-shot param so a later dock switch or reload does not disagree with it.
 useEffect(()=>{if(path!==CHART_PATH||tabParam!=='evidence')return;openEvidence();try{router.setParams({tab:undefined} as any)}catch{}},[path,tabParam]);
 const openDiscover=useCallback(()=>go('discover',{full:false,snap:'half'}),[go]);
 const openWatchFull=useCallback(()=>go('watch',{full:true,snap:'full'}),[go]);
 // ProductSheets' "See evidence" (stock sheet, Watch Review, Welcome example) selects the match with workspace.tab 'history' and pushes "/chart": open Evidence/Replay.
 const wsTab=p.workspace?.tab,setWorkspace=p.setWorkspace;
 useEffect(()=>{if(wsTab!=='history')return;setWorkspace((x:any)=>({...x,tab:'setup'}));openEvidence()},[wsTab]);
 const d=useDiscoverRows(),w=useWorkspaceMatch(d),first=d.rows[0]?.id||'',originId:string=origin?.matchId||'',selected:string=p.workspace?.selected||'';
 // Parity with the old desktop Workspace: with nothing selected (no store entry, URL, pending id or Simulate origin), the chart shows the top Discover result.
 useEffect(()=>{if(!p.loading&&!active&&!selected&&!originId&&first)setWorkspace((x:any)=>({...x,selected:first}))},[p.loading,active,selected,originId,first]);
 const backToChart=useCallback(()=>{const o=draftRef.current?.origin;if(o?.symbol&&o?.timeframe)setActive({symbol:o.symbol,timeframe:o.timeframe,matchId:o.matchId,source:'simulate'});go('discover',{full:false,snap:'peek'})},[go,setActive]);
 const onDockChange=useCallback((k:string)=>go(k as DockTabKey),[go]);
 const toggleFull=useCallback(()=>setView(v=>({...v,full:!v.full})),[]);
 const phonePicked=useCallback(()=>setSnap('peek'),[]);
 const searchPicked=useCallback(()=>{setView(v=>v.full?{...v,full:false}:v);setSnap(v=>v==='full'?'half':v)},[]);
 // Dock tab content: one stable render function per tab (Dock panels are memoised on `render`; inactive panels stay mounted, so drafts survive).
 const discoverDock=useCallback(()=><DiscoverPanel layout="dock"/>,[]),discoverSheet=useCallback(()=><DiscoverPanel layout="sheet" onSelect={phonePicked}/>,[phonePicked]);
 const evidenceR=useCallback(()=><EvidencePanel/>,[]),simulateR=useCallback(()=><SimulationDesk key={simKey} onBack={backToChart}/>,[simKey,backToChart]);
 const autotradeR=useCallback(()=><EmbeddedPage><AutoTrade/></EmbeddedPage>,[]),activityR=useCallback(()=><EmbeddedPage><Activity/></EmbeddedPage>,[]),watchR=useCallback(()=><EmbeddedPage><WatchDesk/></EmbeddedPage>,[]);
 const renders=useMemo<Record<DockTabKey,()=>React.ReactNode>>(()=>({discover:discoverDock,evidence:evidenceR,simulate:simulateR,autotrade:autotradeR,activity:activityR,watch:watchR}),[discoverDock,evidenceR,simulateR,autotradeR,activityR,watchR]);
 const deskTabs=useMemo<DockTab[]>(()=>DESK_TABS.map(k=>({key:k,label:LABEL[k],render:renders[k]})),[renders]);
 // Sidebar: shown widgets persist at kanida.layout.widgets.main; the rail toggles a widget (or expands a collapsed sidebar to it). Collapsed/width persist inside Sidebar (persistKey 'main').
 const [shown,setShown]=useState<string[]>(()=>{const k=readStore<{keys:string[]}>('widgets',LAYOUT_KEY).keys;return Array.isArray(k)?WIDGET_KEYS.filter(x=>k.includes(x)):WIDGET_KEYS;});
 useEffect(()=>{writeStore('widgets',LAYOUT_KEY,{keys:shown})},[shown]);
 const [sideCollapsed,setSideCollapsed]=useState(false),sideRef=useRef(sideCollapsed);sideRef.current=sideCollapsed;
 const toggleSide=useCallback(()=>setSideCollapsed(c=>!c),[]),hideWidget=useCallback((k:string)=>setShown(v=>v.filter(x=>x!==k)),[]);
 const railWidget=useCallback((k:string)=>{const add=(v:string[])=>WIDGET_KEYS.filter(x=>x===k||v.includes(x));if(sideRef.current){setSideCollapsed(false);setShown(add);return}setShown(v=>v.includes(k)?v.filter(x=>x!==k):add(v))},[]);
 const watchW=useMemo(()=>watchWidget({onOpenFull:openWatchFull,onDiscover:openDiscover,onClose:()=>hideWidget(WATCH_WIDGET_KEY)}),[openWatchFull,openDiscover,hideWidget]);
 const evidenceW=useMemo(()=>evidenceSummaryWidget({onOpenEvidence:openEvidence,onClose:()=>hideWidget(EVIDENCE_WIDGET_KEY)}),[openEvidence,hideWidget]);
 const widgets=useMemo(()=>[watchW,evidenceW].filter(x=>shown.includes(x.key)),[watchW,evidenceW,shown]);
 const phoneWidgets=useMemo(()=>[watchWidget({onOpenFull:openWatchFull,onDiscover:openDiscover}),evidenceSummaryWidget({onOpenEvidence:openEvidence})],[openWatchFull,openDiscover,openEvidence]);
 const sideRail=useMemo<SidebarRailItem[]>(()=>[{key:WATCH_WIDGET_KEY,label:'Watch',icon:'eye',active:shown.includes(WATCH_WIDGET_KEY),onPress:()=>railWidget(WATCH_WIDGET_KEY)},{key:EVIDENCE_WIDGET_KEY,label:'Evidence',icon:'bar-chart-2',active:shown.includes(EVIDENCE_WIDGET_KEY),onPress:()=>railWidget(EVIDENCE_WIDGET_KEY)}],[shown,railWidget]);
 // Left tool rail: only existing, working actions. The chart window / Replay drawing / scenarios / inner-swings controls are PatternCanvas-internal state and stay in the chart; the rail steps the Discover order (same store navigation as the chart header) and opens Evidence/Replay.
 const navList=useMemo(()=>d.rows.map(m=>({symbol:m.symbol,timeframe:m.timeframe,matchId:m.id})),[d.rows]),nav=useActiveNavigation(navList,'nav'),match=w.kind==='match'?w.match:undefined;
 const chartRail=useMemo<ToolRailItem[]>(()=>[{key:'prev',label:'Prev stock',icon:'chevron-up',disabled:!nav.hasPrev,onPress:()=>{nav.prev()}},{key:'next',label:'Next stock',icon:'chevron-down',disabled:!nav.hasNext,onPress:()=>{nav.next()}},{key:'evidence',label:'See evidence',icon:'bar-chart-2',disabled:!match,active:view.tab==='evidence',onPress:openEvidence}],[nav.prev,nav.next,nav.hasPrev,nav.hasNext,match,view.tab,openEvidence]);
 const closeWidgets=useCallback(()=>setWidgetsOpen(false),[]);
 const slideEvidence=useCallback(()=>{setWidgetsOpen(false);openEvidence()},[openEvidence]),slideWatchFull=useCallback(()=>{setWidgetsOpen(false);openWatchFull()},[openWatchFull]),slideDiscover=useCallback(()=>{setWidgetsOpen(false);openDiscover()},[openDiscover]);
 const sheetMounted=useRef(new Set<DockTabKey>()).current;sheetMounted.add(view.tab);
 const evidenceSeen=useRef(false);if(view.tab==='evidence')evidenceSeen.current=true;
 // The data pill opens the Data status popover (source, newest bar, last/next refresh, session).
 // `cache`: the state object carries its own provenance when the pilot replayed the last scan (item 2a).
 const topStyle={paddingTop:inset.top+6},pill=p.loading?null:<DataAgePill dataEnd={p.state?.source_latest} status={p.state?.data_status} cache={p.state} error={p.state?'':p.error} compact={phone||winW<NAV_FULL_WIDTH} expanded={dataOpen} onPress={()=>setDataOpen(o=>!o)}/>;
 const pillSlot=<View ref={dataRef}>{pill}</View>;
 const dataPopover=<DataStatusPopover open={dataOpen} onClose={()=>setDataOpen(false)} anchor={dataRef} status={p.state?.data_status} dataEnd={p.state?.source_latest} cache={p.state} error={p.state?'':p.error} routeKey={path}/>;

 if(phone){
  const keys:DockTabKey[]=evidenceSeen.current?[...PHONE_TABS,'evidence']:PHONE_TABS,phoneRenders:Record<DockTabKey,()=>React.ReactNode>={...renders,discover:discoverSheet};
  const header=<ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{alignItems:'center'}}><View role="tablist" aria-label="Workspace panels" style={{flexDirection:'row'}}>{keys.map(k=>{const on=k===view.tab;return <Pressable key={k} role="tab" aria-selected={on} accessibilityState={{selected:on}} accessibilityLabel={PHONE_LABEL[k]} onPress={()=>go(k,{snap:snap==='peek'?'half':undefined})} style={{minHeight:40,paddingHorizontal:9,justifyContent:'center',borderBottomWidth:2,borderBottomColor:on?C.green:'transparent'}}><T numberOfLines={1} style={{fontSize:13,fontFamily:'InterSemi',color:on?C.ink:C.muted}}>{PHONE_LABEL[k]}</T></Pressable>})}</View></ScrollView>;
  return <View style={{flex:1,minHeight:0,backgroundColor:C.bg}}>
   <TopBar style={topStyle} left={<ShellMark compact/>} center={<SymbolSearch compact onPicked={searchPicked}/>} right={<>{pillSlot}<AccountButton connect/></>}/>
   {dataPopover}
   <ResearchBanner/>
   <View style={{flex:1,minHeight:0}}>
   <ScrollView style={{flex:1,minHeight:0}} keyboardShouldPersistTaps="handled" contentContainerStyle={{paddingBottom:PEEK+inset.bottom+16}}>
    <View style={{height:Math.max(280,Math.round(winH*.45)),borderBottomWidth:1,borderColor:C.line}}><ChartSlot phone onOpenEvidence={openEvidence}/></View>
    <View style={{paddingTop:10}}><Sidebar widgets={phoneWidgets} railItems={[]} layout="cards" label="Widgets"/></View>
   </ScrollView>
   <BottomSheet snap={snap} onSnapChange={setSnap} peekHeight={PEEK} label={`${PHONE_LABEL[view.tab]} panel`} header={header}>
    {keys.filter(k=>sheetMounted.has(k)).map(k=><SheetPanel key={k} visible={k===view.tab} open={snap!=='peek'} label={PHONE_LABEL[k]} render={phoneRenders[k]}/>)}
   </BottomSheet>
   </View>
   <MainNav variant="tabs"/>
  </View>;
 }
 const chart=<View style={{flex:1,minWidth:0,minHeight:0}}><ChartSlot onOpenEvidence={openEvidence} minChartHeight={dock.chartFloor}/></View>;
 // F4: desktop/tablet web is a fixed 100vh shell (the page never scrolls); only the dock panels and the widget column scroll inside themselves.
 return <View style={[{flex:1,minHeight:0,backgroundColor:C.bg},Platform.OS==='web'&&{height:winH,maxHeight:winH,overflow:'hidden'}]}>
  <TopBar style={topStyle} left={<><ShellMark compact={!desktop} markOnly={!desktop}/><MainNav/></>} center={<SymbolSearch onPicked={searchPicked}/>} right={<>{pillSlot}<WatchToggle match={match}/>{!desktop&&<IconButton icon="sidebar" label="Open widgets: Watch and Evidence summary" tooltip="Widgets" expanded={widgetsOpen} haspopup="dialog" onPress={()=>setWidgetsOpen(true)} size={17}/>}{desktop&&<ConnectButton/>}<AccountButton connect={!desktop}/></>}/>
  {dataPopover}
  <ResearchBanner/>
  <View style={{flex:1,minHeight:0}}>
   {/* Dock full screen hides the chart region (kept mounted, so the chart does not reload or replay its drawing). */}
   <View style={{flex:1,minHeight:0,flexDirection:'row',display:view.full?'none':'flex'}}>
    {/* F2: Prev/Next live in the rail on desktop AND tablet (compact rail), so the chart header no longer repeats them; Watch lives in the top bar. */}
    <ToolRail items={chartRail} label="Chart tools" orientation="vertical" compact={!desktop} style={{borderRightWidth:1,borderColor:C.line}}/>
    {chart}
    {desktop&&<Sidebar widgets={widgets} railItems={sideRail} collapsed={sideCollapsed} onToggleCollapsed={toggleSide} persistKey={LAYOUT_KEY} label="Widgets" layout="column"/>}
   </View>
   <Dock tabs={deskTabs} active={view.tab} onChange={onDockChange} fullScreen={view.full} onToggleFullScreen={toggleFull} persistKey={LAYOUT_KEY} restoreActive={false} label="Workspace dock" height={HUNGRY_TABS.includes(view.tab)?dock.tall:dock.base} minHeight={dock.min} maxHeight={dock.max}/>
  </View>
  {!desktop&&<Sheet visible={widgetsOpen} onClose={closeWidgets} title="Widgets" subtitle="Your watch and the evidence summary for the active setup."><WatchWidget framed onOpenFull={slideWatchFull} onDiscover={slideDiscover}/><EvidenceSummaryWidget framed onOpenEvidence={slideEvidence}/></Sheet>}
 </View>;
}
