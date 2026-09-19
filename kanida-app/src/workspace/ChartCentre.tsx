import React,{useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {View,Pressable,ScrollView,useWindowDimensions} from 'react-native';
import {router} from 'expo-router';
import {useProduct} from '../context';
import {C,T,s,Button,Loading,Icon,Empty,BREAKPOINTS} from '../ui';
import {Match,History,api,money,pct,dateText} from '../model';
import {PatternCanvas,type ChartNote} from '../PatternCanvas';
import {useExitPlan,prepareBlock} from '../ExitPlan';
import {ChartLegend,LegendEvidence,legendRowsFromChart,evidenceFromWorkspace,chartKeyMatches,chartMatchKey} from '../ChartLegend';
import {sampleSize,dataAgeText,decision} from '../decision';
import {useActiveSymbol,useActiveNavigation,evidenceKey} from '../activeSymbol';
import {useDiscoverRows,useWorkspaceMatch} from './DiscoverPanel';
import {Popover,IconButton,useTitle,readStore,writeStore} from '../layout/index';
import {webOnly} from '../layout/shared';
// Phase 0 chart centre (TrendSpider §3.3–3.4 layout): one-line stock header (+ actions on tablet/desktop, + Prev/Next/Watch on phone) · chart pane that fills the rest (no internal scroll) with the legend overlaid top-left · phone: slim action bar under the chart.
// Desktop/tablet keep Watch in the top bar and Prev/Next in the tool rail (F2). Evidence logic, key guards and prepareBlock gating are unchanged; this file only places them.
export type ChartCentreProps={phone?:boolean;/** Legacy: the legend now starts collapsed by pane height (and remembers the user's choice). */ compactLegend?:boolean;onOpenEvidence?:()=>void;/** Fires after the existing Prepare trade action (p.setPlan) has run. */ onPrepare?:()=>void;/** Desktop/tablet minimum SVG height (the shell's chart floor: CHART_MIN_HEIGHT, or CHART_MIN_HEIGHT_SHORT on short windows). Ignored on phone. */ minChartHeight?:number};
type Nav={prev:()=>boolean;next:()=>boolean;hasPrev:boolean;hasNext:boolean};
// Sizing rules (F1): the canvas fills the pane down to these minimum SVG heights; the legend starts collapsed on panes shorter than LEGEND_AUTO_COLLAPSE; full action labels from ACTIONS_WIDE px of pane width.
// fix3: short desktop windows (shell avail < 650px) use CHART_MIN_HEIGHT_SHORT so the dock can grow to show ≥2 Discover rows.
export const CHART_MIN_HEIGHT=220,CHART_MIN_HEIGHT_SHORT=190,CHART_MIN_HEIGHT_PHONE=180,LEGEND_AUTO_COLLAPSE=360,ACTIONS_WIDE=880;
const PANE_PAD=6;
// Headroom the price scale gives the collapsed legend overlay so the highest drawn mark never sits behind it.
// The overlay is absolutely placed at PANE_PAD+6 from the pane's top; the canvas SVG starts at PANE_PAD+1 (the
// card's border), so the overlay's lower edge is OVERLAY_OVER_SVG + its measured height below the SVG's origin.
// The height is MEASURED (onLayout), so a legend one-liner that wraps, or a wider "3 notes" control, is handled
// on its own terms. OVERLAY_MIN_H is the one-line collapsed overlay and is only the floor used before the first
// measurement lands, so the very first paint already clears it — never zero.
// Expanded, the overlay is a tall panel that is a different problem (it covers the plot wholesale, by design);
// headroom is not applied there, so the scale is untouched in exactly the cases nothing small floats over it.
const OVERLAY_OVER_SVG=PANE_PAD+6-(PANE_PAD+1),OVERLAY_GAP=6,OVERLAY_MIN_H=32;

/** React key for everything shown for one match: evidenceKey(symbol, pattern, timeframe, side, rule=history run, dataEnd=candle_end). Any change remounts, so no state from the previous key survives. */
export function matchKey(match:Match,history?:History){const key=evidenceKey({symbol:match.symbol,pattern:match.pattern,timeframe:match.timeframe,side:history?.side,rule:history?.run,dataEnd:match.candle_end})||`${match.id}|${match.candle_end}`;return match.detection_id?`${key}|${match.detection_id}`:key;}
/** "Test these rules in Simulate →": pins the match, seeds the study draft with its origin, opens /simulate (unchanged from the old StockWorkspace). */
export function openSimulate(p:any,match:Match,side?:string){p.setWorkspace((w:any)=>({...w,selected:match.id}));p.setStudyDraft({symbols:[match.symbol],patterns:[match.pattern],timeframes:[match.timeframe],side:side||'long',product:side==='short'?'MIS':'CNC',start:'',origin:{symbol:match.symbol,pattern:match.pattern,timeframe:match.timeframe,matchId:match.id}});router.push('/simulate' as any);}

export function ChartCentre({phone=false,onOpenEvidence,onPrepare,minChartHeight}:ChartCentreProps){
 const p=useProduct(),d=useDiscoverRows(),w=useWorkspaceMatch(d),{width}=useWindowDimensions(),desktop=!phone&&width>=BREAKPOINTS.desktop;
 const navList=useMemo(()=>d.rows.map(m=>({symbol:m.symbol,timeframe:m.timeframe,matchId:m.id})),[d.rows]),nav=useActiveNavigation(navList,'nav');
 const [retrying,setRetrying]=useState(false);
 const chartMin=phone?CHART_MIN_HEIGHT_PHONE:(minChartHeight??CHART_MIN_HEIGHT);
 const common={phone,nav,age:w.age,stale:w.stale,onOpenEvidence,onPrepare,chartMin};
 if(w.kind==='loading')return <Loading/>;
 if(w.kind==='error')return <View style={{flex:1,padding:desktop?30:16}}><Empty title="Research couldn’t load" detail={w.error||p.error} action={<Button label="Retry" icon="rotate-ccw" loading={retrying} onPress={async()=>{setRetrying(true);try{if(w.retry)w.retry();else await p.refresh()}finally{setRetrying(false)}}}/>}/></View>;
 if(w.kind==='empty')return <View style={{flex:1,padding:desktop?30:16}}><Empty icon="bar-chart-2" title="Choose a setup from Discover" detail="Select a Discover result, or pick a stock from search or Watch, to see its chart, legend and evidence."/></View>;
 if(w.kind==='none'&&w.active?.detectionId)return <View style={{padding:16}}><Empty title="Selected detection unavailable" detail={`The exact selected occurrence for ${w.symbol} ${w.timeframe} is no longer in the current chart. Return to Discover to choose a current detection.`}/></View>;
 if(w.kind==='none'||!w.match)return <NoSetupChart key={w.symbol+'|'+w.timeframe} symbol={w.symbol} timeframe={w.timeframe} replaced={w.replaced} outsideSetups={w.outsideSetups} {...common}/>;
 return <MatchChart key={matchKey(w.match,w.history)} match={w.match} snapshot={w.chartSnapshot} detectionId={w.active?.detectionId||w.match.detection_id} history={w.history} outside={w.outside} replaced={w.replaced} auto={w.auto} {...common}/>;
}

/** Truncated note chip (icon only on phone). Full text: hover title, accessible name, and one press opens it in a popover. */
function InfoChip({text,amber,iconOnly=false,label}:{text:string;amber:boolean;iconOnly?:boolean;label:string}){
 const [open,setOpen]=useState(false),tip=useTitle(text),col=amber?C.amber:C.muted;
 return <>
  <Pressable ref={tip} accessibilityRole="button" accessibilityLabel={`${text}. Show the full note`} accessibilityState={{expanded:open}} {...webOnly({'aria-expanded':open})} onPress={()=>setOpen(true)} style={{flexShrink:1,minWidth:iconOnly?36:0,maxWidth:iconOnly?36:300,minHeight:iconOnly?36:26,flexDirection:'row',alignItems:'center',justifyContent:'center',gap:5,borderRadius:7,paddingHorizontal:iconOnly?0:7,backgroundColor:iconOnly?'transparent':amber?C.amberBg:C.paper,borderWidth:iconOnly?0:1,borderColor:C.line}}>
   <Icon name={amber?'alert-circle':'clock'} size={13} color={col}/>{!iconOnly&&<T numberOfLines={1} style={{flexShrink:1,minWidth:0,fontSize:11,lineHeight:16,color:col}}>{text}</T>}
  </Pressable>
  <Popover open={open} onClose={()=>setOpen(false)} anchor={tip} placement="bottom-end" label={label} width={300}><T style={{fontSize:12,lineHeight:18,color:col,paddingHorizontal:12,paddingVertical:6}}>{text}</T></Popover>
 </>;
}

// One header line: symbol · TF · company · pattern (· price · date) · data-age note. Company/pattern truncate first, then the note chip.
// The symbol · TF · line group is the heading (web role="heading" aria-level 2, native header) named "Chart header for SYMBOL, TF, Company, Pattern"; actions stay outside it. `auto` = subtle "Auto-selected" chip (Q1).
function StockHeader({symbol,timeframe,line,label,auto,meta,metaAmber,nav,phone,actions,watch}:{symbol:string;timeframe:string;line:string;label:string;auto?:string;meta:string;metaAmber:boolean;nav:Nav;phone:boolean;actions?:React.ReactNode;watch?:React.ReactNode}){
 return <View style={[s.row,{minHeight:phone?44:46,paddingLeft:phone?10:12,paddingRight:phone?2:8,paddingVertical:3,gap:phone?6:8,borderBottomWidth:1,borderColor:C.line}]}>
  <View accessibilityRole="header" accessibilityLabel={label} {...webOnly({'aria-level':2})} style={[s.row,{flex:1,minWidth:0,gap:phone?6:8}]}>
   <T numberOfLines={1} style={{fontFamily:'ManropeBold',fontSize:phone?16:18,lineHeight:24,flexShrink:0}}>{symbol}</T>
   <T style={{color:C.muted,fontSize:12,lineHeight:18,flexShrink:0}}>{timeframe}</T>
   <T numberOfLines={1} style={{flex:1,minWidth:0,fontSize:12,lineHeight:18,color:C.muted}}>{line}</T>
  </View>
  {!!auto&&<View accessible accessibilityLabel={auto} style={{flexShrink:1,minWidth:0,maxWidth:phone?120:240,minHeight:22,justifyContent:'center',borderRadius:7,borderWidth:1,borderColor:C.line,paddingHorizontal:6}}><T numberOfLines={1} style={{fontSize:11,lineHeight:16,color:C.muted}}>{auto}</T></View>}
  {!!meta&&<InfoChip text={meta} amber={metaAmber} iconOnly={phone} label="Chart data note"/>}
  {actions}
  {phone&&<><IconButton icon="chevron-left" label="Previous stock" disabled={!nav.hasPrev} color={C.ink} onPress={()=>{nav.prev()}}/><IconButton icon="chevron-right" label="Next stock" disabled={!nav.hasNext} color={C.ink} onPress={()=>{nav.next()}}/></>}
  {watch}
 </View>;
}

// Compact action row: See evidence · Prepare trade (disabled with the gate reason as hover title, accessible name/hint and a one-press "why?" popover) · Test in Simulate →.
function ChartActions({phone,wide,symbol,blocked,onEvidence,onPrepare,onSimulate,onRetry}:{phone:boolean;wide:boolean;symbol:string;blocked:string;onEvidence?:()=>void;onPrepare:()=>void;onSimulate?:()=>void;onRetry?:()=>void}){
 const [why,setWhy]=useState(false),whyRef=useRef<any>(null),reason=blocked?`Prepare trade unavailable: ${blocked}`:'',prepTip=useTitle(reason||undefined);
 const hit=phone?40:34,btn={minHeight:hit,paddingVertical:5,paddingHorizontal:phone?12:11,borderRadius:9};
 const link=(text:string,a11y:string,onPress:()=>void,role:'link'|'button'='link')=><Pressable accessibilityRole={role} accessibilityLabel={a11y} onPress={onPress} style={{minHeight:hit,paddingHorizontal:6,justifyContent:'center'}}><T numberOfLines={1} style={{fontSize:12,color:C.green}}>{text}</T></Pressable>;
 return <View style={[s.row,{gap:phone?4:6,flexShrink:0}]}>
  {!!onEvidence&&(wide||phone?<Button label="See evidence" kind="soft" icon="bar-chart-2" onPress={onEvidence} style={btn}/>:<IconButton icon="bar-chart-2" label="See evidence" color={C.green} onPress={onEvidence}/>)}
  <View ref={prepTip}><Button label={phone?'Plan':'Prepare trade'} icon="zap" disabled={!!blocked} accessibilityLabel={blocked?`Prepare trade, unavailable: ${blocked}`:'Prepare trade'} accessibilityHint={reason||undefined} onPress={onPrepare} style={btn}/></View>
  {!!blocked&&<Pressable ref={whyRef} accessibilityRole="button" accessibilityLabel={`Why is Prepare trade unavailable? ${blocked}`} accessibilityState={{expanded:why}} {...webOnly({'aria-expanded':why,title:reason})} onPress={()=>setWhy(true)} style={[s.row,{gap:3,minHeight:hit,minWidth:34,paddingHorizontal:4}]}><Icon name="help-circle" size={14} color={C.amber}/><T style={{fontSize:11,color:C.amber}}>why?</T></Pressable>}
  {!!onRetry&&link('Retry','Retry exit evidence',onRetry,'button')}
  {!!onSimulate&&link(wide?'Test in Simulate →':'Simulate →',`Test these rules in Simulate for ${symbol}`,onSimulate)}
  <Popover open={why} onClose={()=>setWhy(false)} anchor={whyRef} placement={phone?'top-end':'bottom-end'} label="Why Prepare trade is unavailable" width={320}><View style={{paddingHorizontal:12,paddingVertical:6,gap:4}}><T accessibilityRole="alert" style={{fontSize:12,lineHeight:18,color:C.amber}}>{reason}</T>{!!onRetry&&link('Retry exit evidence','Retry exit evidence',()=>{setWhy(false);onRetry()},'button')}</View></Popover>
 </View>;
}
function ActionBar({children}:{children:React.ReactNode}){return <View style={{borderTopWidth:1,borderColor:C.line,backgroundColor:C.paper}}><ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{flexGrow:1,justifyContent:'flex-end',alignItems:'center',paddingHorizontal:6,paddingVertical:3}}>{children}</ScrollView></View>;}

// Legend state per chart (0.3 wiring): onSnapshot data + cursor state, stable onCursor, per-row visibility. Rows are [] for another symbol/timeframe's snapshot.
// Layout state: measured pane size, drawing status from the canvas, and the collapse choice (auto by pane height until the user picks; the pick persists at kanida.layout.legend.<main|phone>).
function useChartLegend(selected:Pick<Match,'id'|'symbol'|'timeframe'|'detection_id'>,phone:boolean){
 const p=useProduct(),{setActive}=useActiveSymbol();
 const [snapshot,setSnapshot]=useState<any>(null),[cursor,setCursor]=useState<number|null>(null),[hidden,setHidden]=useState<string[]>([]);
 const onSnapshot=useCallback((v:any)=>setSnapshot(v),[]),onCursor=useCallback((i:number|null)=>setCursor(i),[]);
 const rows=useMemo(()=>legendRowsFromChart(snapshot,selected,{enabledPatterns:p.filters?.patterns,catalogue:p.state?.patterns,cursor,hidden}),[snapshot,selected,p.filters?.patterns,p.state?.patterns,cursor,hidden]);
 const keyOk=chartKeyMatches(snapshot,selected.symbol,selected.timeframe),bars=keyOk?snapshot.bars:[];
 const toggle=useCallback((key:string)=>setHidden(h=>h.includes(key)?h.filter(k=>k!==key):[...h,key]),[]);
 // Selecting another found pattern in the legend makes that stored match active (only when it exists as a stored match).
 const pick=useCallback((key:string)=>{if(key===chartMatchKey(selected))return;const chartMatches=chartKeyMatches(snapshot,selected.symbol,selected.timeframe)&&Array.isArray(snapshot.matches)?snapshot.matches:[];const m=[...chartMatches,...p.matches as Match[]].find(x=>chartMatchKey(x)===key);if(m)setActive({symbol:m.symbol||selected.symbol,timeframe:m.timeframe||selected.timeframe,matchId:m.id,detectionId:m.detection_id,source:'nav'})},[snapshot,p.matches,selected.symbol,selected.timeframe,selected.id,selected.detection_id,setActive]);
 const storeId=phone?'phone':'main';
 const [choice,setChoice]=useState<boolean|null>(()=>{const v=readStore<{collapsed:boolean}>('legend',storeId).collapsed;return typeof v==='boolean'?v:null});
 const [pane,setPane]=useState({w:0,h:0}),[status,setStatus]=useState({text:'',done:false}),[notes,setNotes]=useState<ChartNote[]>([]),[overlayH,setOverlayH]=useState(0);
 const onOverlay=useCallback((e:any)=>{const v=Math.round(e.nativeEvent.layout.height);setOverlayH(p=>Math.abs(p-v)<2?p:v)},[]);
 const onPane=useCallback((e:any)=>{const l=e.nativeEvent.layout,w=Math.round(l.width),h=Math.round(l.height);setPane(v=>Math.abs(v.w-w)<2&&Math.abs(v.h-h)<2?v:{w,h})},[]);
 const onStatus=useCallback((text:string,done:boolean)=>setStatus(v=>v.text===text&&v.done===done?v:{text,done}),[]);
 // The canvas's drawing notes are taken OUT of the chart column and shown on the legend overlay. Their number and
 // length depend on the occurrence's stored geometry, and in the chart column they came straight off the plot's
 // height (F1 chart floor), so on a short window the SVG was clipped. Same text, same reader, zero layout cost.
 const onNotes=useCallback((v:ChartNote[])=>setNotes(prev=>prev.length===v.length&&prev.every((n,i)=>n.text===v[i].text&&n.tone===v[i].tone)?prev:v),[]);
 const setCollapsed=useCallback((c:boolean)=>{setChoice(c);writeStore('legend',storeId,{collapsed:c})},[storeId]),expand=useCallback(()=>setCollapsed(false),[setCollapsed]);
 const collapsed=choice??(pane.h>0?pane.h<LEGEND_AUTO_COLLAPSE:true);
 // Only the collapsed overlay earns headroom: it is the small floating strip the price scale can clear cheaply.
 const topInset=collapsed?OVERLAY_OVER_SVG+Math.max(overlayH,OVERLAY_MIN_H)+OVERLAY_GAP:0;
 return {snapshot,onSnapshot,cursor,onCursor,hidden,rows,bars,keyOk,toggle,pick,collapsed,setCollapsed,expand,pane,onPane,status,onStatus,notes,onNotes,overlayH,onOverlay,topInset};
}
type LegendState=ReturnType<typeof useChartLegend>;
// Legend overlaid on the chart pane's top-left on a translucent backing (TrendSpider "Collapse indicator list"). Expanded content scrolls inside ~60% of the pane, so it never pushes the chart. Collapsing never touches PatternCanvas (a sibling), so the canvas does not remount or replay.
// The container is named "Chart legend for SYMBOL TF, collapsed|expanded" in both states. `extra` (the outside-filters pick list) shows in both states.
function LegendOverlay({L,symbol,timeframe,selectedKey,evidence,onOpenEvidence,note,extra}:{L:LegendState;symbol:string;timeframe:string;selectedKey?:string;evidence:LegendEvidence|null;onOpenEvidence?:()=>void;note?:string;extra?:React.ReactNode}){
 const {w,h}=L.pane,collapsed=L.collapsed,statusText=L.status.text||'Loading chart…',statusTip=useTitle(statusText),toggleTip=useTitle(collapsed?'Expand legend':'Collapse legend');
 // Drawing notes are disclosed on request, never parked over the candles: closed they are one control on a row the
 // overlay already had (zero extra height, nothing new covered); open they may cover price, because the reader just
 // asked for them. Always starts closed — MatchChart/NoSetupChart are keyed per setup so the state cannot survive a
 // stock change, and this also shuts it when the notes themselves change under the same setup (e.g. inner swings on).
 const [notesOpen,setNotesOpen]=useState(false),notesRef=useRef<any>(null);
 const noteSig=L.notes.map(n=>n.text).join('|'),lastSig=useRef(noteSig);
 useEffect(()=>{if(lastSig.current===noteSig)return;lastSig.current=noteSig;setNotesOpen(false)},[noteSig]);
 const maxWidth=w>0?Math.max(0,Math.min(w-PANE_PAD*2-12,Math.max(Math.round(w*.55),collapsed?560:420))):undefined;
 const icon=<Icon name={L.status.done?'check-circle':'edit-3'} size={13} color={C.green}/>;
 const toggle=<Pressable ref={toggleTip} accessibilityRole="button" accessibilityLabel={collapsed?'Expand legend':'Collapse legend'} accessibilityState={{expanded:!collapsed}} {...webOnly({'aria-expanded':!collapsed})} onPress={()=>L.setCollapsed(!collapsed)} style={[s.row,{gap:3,minHeight:30,minWidth:30,paddingHorizontal:6,justifyContent:'center',flexShrink:0}]}>{!collapsed&&<T style={{fontSize:11,color:C.green}}>Collapse legend</T>}<Icon name={collapsed?'chevron-down':'chevron-up'} size={14} color={C.green}/></Pressable>;
 const legend=<ChartLegend bars={L.bars} cursor={L.cursor} symbol={symbol} timeframe={timeframe} dataEnd={L.keyOk?L.snapshot.last_candle:undefined} rows={L.rows} selectedKey={selectedKey} evidence={evidence} onToggleVisible={L.toggle} onSelect={collapsed?L.expand:L.pick} onOpenEvidence={onOpenEvidence} compact={collapsed}/>;
 // The canvas's drawing key (what the amber box marks, what the amber/red levels mean, why geometry is missing),
 // behind a control that says how many notes there are. Nothing here is in the chart column, so the plot keeps its
 // full height either way. Rendered only when there is something to read; a warning note makes the control amber.
 const notesN=L.notes.length,notesWarn=L.notes.some(n=>n.tone==='amber');
 const notesWhat=`${notesN} note${notesN===1?'':'s'} about this chart`,notesTip=useTitle(notesN?notesWhat:undefined);
 const notesBtn=notesN?<Pressable ref={(el:any)=>{notesRef.current=el;(notesTip as any).current=el}} accessibilityRole="button" accessibilityLabel={`${notesWhat}. ${notesOpen?'Hide them':'Read them'}`} accessibilityState={{expanded:notesOpen}} {...webOnly({'aria-expanded':notesOpen,'aria-haspopup':'dialog'})} onPress={()=>setNotesOpen(o=>!o)} style={(st:any)=>[s.row,{gap:4,minHeight:30,minWidth:30,paddingHorizontal:6,flexShrink:0,justifyContent:'center',borderRadius:7,backgroundColor:st.hovered||st.focused||st.pressed?C.soft:'transparent'}]}>
  <Icon name="info" size={13} color={notesWarn?C.amber:C.muted}/><T numberOfLines={1} style={{fontSize:11,lineHeight:16,color:notesWarn?C.amber:C.muted}}>{notesN} note{notesN===1?'':'s'}</T>
 </Pressable>:null;
 // Full text, never clamped: the panel scrolls when the notes are longer than the room it has (the uncapped
 // harmonic ratio list is the case that needs this), so nothing is cut off the way the old two-line clamp cut it.
 const notesPanel=notesN?<Popover open={notesOpen} onClose={()=>setNotesOpen(false)} anchor={notesRef} placement="bottom-start" label={`${notesWhat}: ${symbol} ${timeframe}`} width={340}><ScrollView contentContainerStyle={{paddingHorizontal:12,paddingVertical:6,gap:8}}>{L.notes.map((n,i)=><T key={i} accessibilityRole="text" style={{fontSize:12,lineHeight:18,color:n.tone==='amber'?C.amber:C.ink}}>{n.text}</T>)}</ScrollView></Popover>:null;
 return <View role="group" accessibilityLabel={`Chart legend for ${symbol} ${timeframe}, ${collapsed?'collapsed':'expanded'}`} onLayout={L.onOverlay} style={{position:'absolute',top:PANE_PAD+6,left:PANE_PAD+6,zIndex:5,maxWidth,backgroundColor:'rgba(5,12,17,0.84)',borderWidth:1,borderColor:C.line,borderRadius:8}}>
  {collapsed?<><View style={[s.row,{gap:0,paddingLeft:6}]}><View ref={statusTip} accessible role="img" accessibilityLabel={statusText} style={{flexShrink:0}}>{icon}</View><View style={{flexShrink:1,minWidth:0}}>{legend}</View>{notesBtn}{toggle}</View>{extra}{notesPanel}</>
  :<><View style={[s.between,{gap:6,paddingLeft:8,borderBottomWidth:1,borderColor:C.line}]}><View style={[s.row,{gap:5,flexShrink:1,minWidth:0}]}>{icon}<T numberOfLines={1} accessibilityLiveRegion="polite" style={{fontSize:11,lineHeight:16,color:C.green,flexShrink:1,minWidth:0}}>{statusText}</T></View><View style={[s.row,{gap:0,flexShrink:0}]}>{notesBtn}{toggle}</View></View>
   <ScrollView style={{maxHeight:h>0?Math.max(80,Math.round(h*.6)-32):240}} contentContainerStyle={{paddingBottom:2}}>{!!note&&<T accessibilityRole="text" style={{fontSize:11,lineHeight:16,color:C.muted,paddingHorizontal:8,paddingTop:5}}>{note}</T>}{extra}{legend}</ScrollView>{notesPanel}</>}
 </View>;
}
// Chart pane: flex:1, no scroll; the canvas measures and fills it. The overlay is a sibling so collapse/expand never re-renders or remounts the canvas.
function ChartPane({L,phone,children,overlay}:{L:LegendState;phone:boolean;children:React.ReactNode;overlay:React.ReactNode}){
 return <View style={{flex:1,minHeight:0,paddingHorizontal:PANE_PAD,paddingTop:PANE_PAD,paddingBottom:phone?2:4,overflow:'hidden'}} onLayout={L.onPane}>{children}{overlay}</View>;
}

type ChartProps={phone:boolean;nav:Nav;age:number|null;stale:boolean;onOpenEvidence?:()=>void;onPrepare?:()=>void;chartMin:number};
function MatchChart({match,detectionId,snapshot,history:h,outside,replaced,auto,phone,nav,age,stale,onOpenEvidence,onPrepare,chartMin}:ChartProps&{match:Match;detectionId?:string;snapshot?:any;history?:History;outside:boolean;replaced:boolean;auto:boolean}){
 const p=useProduct(),exit=useExitPlan(match,h?.side||'long'),L=useChartLegend(match,phone);
 const [busy,setBusy]=useState(false);const watching=!!p.product.watchlist?.some((x:any)=>x.id===match.id);
 // Prepare trade needs fresh prices AND evidence from the exact exit rule that would be traded (undefined counts as false).
 const blocked=prepareBlock({stale,age,match,history:h,exit});
 // Key guard (0.3): no numbers unless history, legend key, chart snapshot (incl. last_candle) and exit side all belong to this match and the exit request settled.
 const evidence=evidenceFromWorkspace({match,history:h,exit,chartData:L.snapshot,legendKey:{symbol:match.symbol,timeframe:match.timeframe,selectedKey:chartMatchKey(match)},age,stale});
 async function watch(){setBusy(true);try{await api('/api/product/watch',{action:watching?'remove':'add',match_id:match.id});await p.refreshProduct()}catch(e:any){p.setToast(e.message)}finally{setBusy(false)}}
 const actions=<ChartActions phone={phone} wide={L.pane.w>=ACTIONS_WIDE} symbol={match.symbol} blocked={blocked?String(blocked):''} onEvidence={onOpenEvidence} onPrepare={()=>{p.setPlan({match,side:h?.side});onPrepare?.()}} onSimulate={()=>openSimulate(p,match,h?.side)} onRetry={blocked&&exit.error&&!stale?exit.retry:undefined}/>;
 return <View style={{flex:1,minHeight:0}}>
  <StockHeader symbol={match.symbol} timeframe={match.timeframe} phone={phone} nav={nav} metaAmber={stale} label={`Chart header for ${match.symbol}, ${match.timeframe}, ${match.company}, ${match.pattern_name}`} auto={auto?`Auto-selected: top setup for ${match.symbol}`:undefined} line={`${match.company} · ${match.pattern_name}${phone?'':` · ${money(match.price)} · ${dateText(match.candle_end)}`}`} meta={`${dataAgeText(age)}${stale?' · fresh prices needed':''}${outside?' · outside your current filters':''}${replaced?' · the requested setup is no longer listed':''}`} actions={phone?undefined:actions} watch={phone?<IconButton icon="eye" label={watching?'Watching':'Watch'} pressed={watching} disabled={busy} color={watching?C.green:C.ink} onPress={watch}/>:undefined}/>
  <ChartPane L={L} phone={phone} overlay={<LegendOverlay L={L} symbol={match.symbol} timeframe={match.timeframe} selectedKey={chartMatchKey(match)} evidence={evidence} onOpenEvidence={onOpenEvidence}/>}>
   <PatternCanvas match={match} detectionId={detectionId} snapshot={snapshot} side={h?.side} exitPlan={exit.data} compact fill chartHeight={chartMin} onSnapshot={L.onSnapshot} onCursor={L.onCursor} onStatus={L.onStatus} onNotes={L.onNotes} topInset={L.topInset} hideHeader hiddenPatterns={L.hidden}/>
  </ChartPane>
  {phone&&<ActionBar>{actions}</ActionBar>}
 </View>;
}

// Symbol-only store with no stored match for symbol × timeframe: honest state, stored candles only (PatternCanvas draws no outline for an empty pattern), no evidence, no Prepare.
const noop=()=>{};
function NoSetupChart({symbol,timeframe,replaced,outsideSetups=[],phone,nav,age,stale,chartMin}:ChartProps&{symbol:string;timeframe:string;replaced:boolean;outsideSetups?:Match[]}){
 const p=useProduct(),{active,setActive}=useActiveSymbol(),within=outsideSetups.length>0;
 const stub=useMemo<Match>(()=>({id:`${symbol}:${timeframe}:`,symbol,company:'',price:NaN,pattern:'',pattern_name:'price',timeframe,direction:'neutral',state:'',current:false,candle_end:'',score:0,sector:'',universes:[],history:[],start_index:NaN}),[symbol,timeframe]);
 const L=useChartLegend(stub,phone),reason=within?'No setup within your filters is selected.':'No stored setup for this stock and timeframe.';
 // Q1: stored setups that exist only OUTSIDE the current filters are never auto-selected. They're offered as an explicit pick list, each marked "outside your filters".
 const note=within?`No setup for ${symbol} on ${timeframe} within your filters. Stored candles only: no pattern evidence, exit rule or trade preparation until you pick one. ${outsideSetups.length} stored ${outsideSetups.length===1?'setup is':'setups are'} outside your filters:`:`No stored setup for ${symbol} on ${timeframe}. Stored candles only: no pattern evidence, exit rule or trade preparation. Change the timeframe or pick a Discover result.`;
 const extra=within?<View accessibilityLabel={`Stored setups for ${symbol} ${timeframe} outside your filters`} style={{paddingHorizontal:8,paddingTop:3,paddingBottom:5,gap:3}}>{outsideSetups.slice(0,4).map(m=><Pressable key={m.id} accessibilityRole="button" accessibilityLabel={`Select ${m.pattern_name} ${m.timeframe} for ${m.symbol}, outside your filters`} onPress={()=>setActive({symbol:m.symbol,timeframe:m.timeframe,matchId:m.id,source:active?.source||'nav'})} style={(st:any)=>({minHeight:28,justifyContent:'center',paddingHorizontal:7,borderRadius:6,borderWidth:1,borderColor:C.line,backgroundColor:st.hovered||st.pressed?C.soft:'transparent'})}><T numberOfLines={1} style={{fontSize:11,lineHeight:16}}>{m.pattern_name} · <T style={{fontSize:11,color:C.muted}}>{decision(m,p.filters).title}</T> · <T style={{fontSize:11,color:C.amber}}>outside your filters</T></T></Pressable>)}</View>:undefined;
 const actions=<ChartActions phone={phone} wide={L.pane.w>=ACTIONS_WIDE} symbol={symbol} blocked={reason} onPrepare={noop}/>;
 return <View style={{flex:1,minHeight:0}}>
  <StockHeader symbol={symbol} timeframe={timeframe} phone={phone} nav={nav} metaAmber={stale} label={`Chart header for ${symbol}, ${timeframe}, ${within?'no setup within your filters':'no stored setup'}`} line={within?`No setup for ${symbol} within your filters`:`No stored setup for ${symbol} on ${timeframe}`} meta={`${dataAgeText(age)}${stale?' · fresh prices needed':''}${replaced?' · the requested setup is no longer listed':''}`} actions={phone?undefined:actions}/>
  <ChartPane L={L} phone={phone} overlay={<LegendOverlay L={L} symbol={symbol} timeframe={timeframe} evidence={null} note={note} extra={extra}/>}>
   <PatternCanvas match={stub} compact fill chartHeight={chartMin} onSnapshot={L.onSnapshot} onCursor={L.onCursor} onStatus={L.onStatus} onNotes={L.onNotes} topInset={L.topInset} hideHeader/>
  </ChartPane>
  {phone&&<ActionBar>{actions}</ActionBar>}
 </View>;
}

/** The old StockWorkspace "Setup" metrics column / phone "Summary" tab, for the active match. Renders nothing without a stored match. */
export function SetupSummary({column=false}:{column?:boolean}){
 const d=useDiscoverRows(),w=useWorkspaceMatch(d);
 if(w.kind!=='match'||!w.match)return null;
 return <ScrollView showsVerticalScrollIndicator={false} style={column?{width:220,flexGrow:0,borderLeftWidth:1,borderColor:C.line}:{flex:1,minHeight:0}} contentContainerStyle={column?{padding:12,gap:12}:{gap:18,padding:12}}><SetupMetrics key={matchKey(w.match,w.history)} match={w.match} history={w.history}/></ScrollView>;
}
function SetupMetrics({match,history:h}:{match:Match;history?:History}){
 const exit=useExitPlan(match,h?.side||'long'),stats=h?.reference;
 // Headline stats are the scanner's fixed-hold reference history, never the exit rule shown below them; the label says so.
 return <>
  <T accessibilityRole="header" style={{fontSize:11,lineHeight:16,fontFamily:'InterMedium',color:C.amber}}>{exit.data?.history_label||exit.history_label}</T>
  <View style={[s.row,{flexWrap:'wrap',gap:18}]}>{[['Hold-period avg. net / trade',pct(stats?.display_return_pct)],['95% range · this result alone',stats?.expectancy_ci95?`${pct(stats.expectancy_ci95[0])} to ${pct(stats.expectancy_ci95[1])}`:'—'],['Profitable trades',stats?.win_rate==null?'—':`${Math.round(stats.win_rate*stats.n/100)} of ${stats.n}`],['Tested hold',stats?.holding?.duration||'Unavailable']].map(([label,value])=><View key={label} style={{flex:1,minWidth:130,gap:5}}><T style={{fontSize:12,color:C.muted}}>{label}</T><T style={{fontFamily:'InterSemi',fontSize:18}}>{value}</T></View>)}</View>
  <T style={{fontSize:12,color:C.muted}}>{stats?.n||0} historical trades · {sampleSize(stats?.n).label} · {h?.side==='short'?'Hypothetical short price study':'After assumed costs'} · fixed holding-period exit, not the exit rule below. Picked from many stock × pattern results: top averages are partly luck and usually shrink, and the range does not correct for that.</T>
  <View style={{height:1,backgroundColor:C.line}}/>
  <T style={{fontSize:14,fontFamily:'InterSemi'}}>{match.state==='setup'?'Waiting for breakout':'Breakout recorded'}</T>
  <T style={{fontSize:12,color:C.muted}}>{match.current?'Review the historical evidence before preparing a trade.':'Fresh prices are needed before this stored setup can become a current trade.'}</T>
  {exit.data&&<><T style={{fontSize:14,fontFamily:'InterSemi'}}>Exit rule</T><T style={{fontSize:13,color:exit.data.tradable_evidence===true?C.ink:C.amber}}>{exit.data.tradable_evidence===true?'Tested on later data · this exact rule':'Illustrative · no tested evidence for this exact rule'} · 1:{exit.data.reward}</T><T style={{fontSize:12,color:C.muted}}>{exit.data.holding?.duration} maximum hold. {exit.data.tradable_evidence===true?'':'The history above does not describe this stop, target and hold.'}</T></>}
 </>;
}
