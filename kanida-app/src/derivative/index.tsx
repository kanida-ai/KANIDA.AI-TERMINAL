// The Derivative tab (docs/DERIVATIVES_SPEC.md §4).
//
// ONE SCREENER, PINNED, AND ONE TEMPLATE UNDER IT.
//
// The owner, on the version where every section had invented its own shape: "everything is wrong - I asked for
// same block template across for all blocks right? also why other stocks are not populating". Both of those are
// answered here.
//
//  1. THE SHAPE. There is exactly ONE screener on this tab. It sits at the top, it stays pinned while the page
//     scrolls, and it drives everything: clicking a row points every block below at that underlying, and the
//     symbol it resolved is drawn the same way on this bar and on every block header, with a mint rail down the
//     side of each block that is on it. Every block below is the SAME skeleton - [ Chart ][ Content ] - at the
//     same widths, the same height, the same header treatment and the same type scale (frame.Block). Only what
//     sits in the two panels changes.
//
//  2. THE STOCKS. The tab used to open on the newest 15-min reading the store held, full stop. On 18 Sep 2026
//     that was 15:45 and the capture had died at 11:30: everything after it was rebuilt from 15-minute candles,
//     which carry no traded-price average, so `premium_cr` was null for every contract and NOTHING could clear
//     the ₹2 crore floor. Empty screener, no row to click, every block stuck on the fallback index. The server
//     now opens on the newest reading that HAS contracts over the floors and says which reading that is; the
//     reader can still move to any reading, including the empty ones.
//
// What this tab is: a description of what is happening in the F&O book right now, at 15-min capture readings.
// What it is NOT (§5): a prediction, an evidence card, or an input to any trading gate. Every block states its
// own as-of time and the liquidity floors in force; a ratio without a baseline says "no baseline"; a number that
// was not captured is a dash. None of that changed here — only the shape it is presented in.
import React,{useCallback,useMemo,useState} from 'react';
import {View,ScrollView,useWindowDimensions} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {IconButton,readStore,writeStore} from '../layout';
import {HeaderChip} from '../discover/parts';
import {useDerivativeRead} from './useDerivatives';
import {BLOCK_BESIDE,BLOCK_H,BLOCK_PIN_COLUMN,Block,CaptureChip,CaptureContext,head,
 type PaneStyle} from './frame';
import {FilterDialog,ruleLabeller} from './FilterDialog';
import {RAIL_H,ScreenerRail} from './ScreenerSection';
import {ChainWidget} from './ChainWidget';
import {OiByStrikeSection} from './OiByStrikeSection';
import {OiGridSection} from './OiGridSection';
import {PcrSection,MaxPainSection,IvSection,FuturesBuildupSection} from './SessionBlocks';
import {IndexWidget} from './IndexWidget';
import {FuturesWidget} from './FuturesWidget';
import {ChartTile} from './ChartTile';
import {BUILDUP_WINDOW_DEFAULT,DASH,EMPTY_TEXT,SCREENER_PATH,SCREENER_VIEW_DEFAULT,asOfText,resolveTabSymbol,
 rankingLabel,rulesText,rulesToFilters,sanitizeRules,screenerQuery,sourceText,symbolBadge,
 type FilterRule} from './logic';
import {stateOf} from './frame';
import type {AnalysisContext,CaptureHealth,ChartTarget,FilterData,OiGrid,Screener,Series,Status} from './types';
export {EMPTY_TEXT} from './logic';
/** Re-exported so nothing outside has to know where the ONE breakpoint lives. */
export {BLOCK_BESIDE,BLOCK_PIN_COLUMN} from './frame';
/** The height a widget takes when the reader expands it to fill the page. The ONLY height on this tab that is
 *  not the template's own. */
const EXPANDED_H=760;
/** Every widget the page can show, in the order the blocks run: the pinned screener, then the CHART panel and
 *  the CONTENT panel of each block in turn. The pairing is the template, written out. */
export const WIDGET_KEYS=['unusual',
 'chain_chart','chain',
 'oi_chart','oi_table',
 'oi_grid_futures','oi_grid',
 'pcr_chart','pcr_readings',
 'max_pain_chart','max_pain_readings',
 'iv_chart','iv_strikes',
 'fut_oi','fut_readings',
 'indices_chart','indices',
 'futures_chart','futures'] as const;
export type WidgetKey=(typeof WIDGET_KEYS)[number];
/** The screener is the tab's control surface, not one of its widgets: it is what every block answers to, so it
 *  cannot be closed. It can still be expanded to fill the page for a reader browsing the list. */
export const PINNED_KEY:WidgetKey='unusual';
const hiddenFrom=(stored:unknown)=>{
 const raw=(stored||{}) as Record<string,unknown>,out:Record<string,boolean>={};
 for(const key of WIDGET_KEYS)if(key!==PINNED_KEY&&raw[key]===true)out[key]=true;
 return out;
};
// ================================================================================================================
// THE TAB'S ONE CONTEXT, WRITTEN OUT.
//
// The audit of 19 Sep 2026: "The tab mixes readings from different times and then combines them into one
// sentence." Its example was NIFTY's max pain, where the headline printed a strike from the 15:45 reading,
// a spot from the 11:30 reading and a distance that was only ever true of the 11:30 pair. Every number was
// captured. The sentence was false.
//
// The arithmetic fix is the server's: a relationship between two figures now exists only at a reading that
// carried both. This is the other half of it - the reader can SEE which instrument, which expiry, which
// session and which 15-min reading everything below is resolved at, and whether that reading is the live
// edge of the store or a session read back. It is one line, and it never pushes the market data down.
// ================================================================================================================
function ContextBar({context,capture,phone}:{context?:AnalysisContext|null;capture?:CaptureHealth|null;
 phone?:boolean}){
 if(!context&&!capture)return null;
 const parts:{label:string;value:string}[]=[];
 if(context?.underlying)parts.push({label:'Instrument',value:context.underlying});
 if(context?.expiry)parts.push({label:'Expiry',value:context.expiry
  +(context.days_to_expiry==null?'':` · ${context.days_to_expiry}d`)});
 if(context?.session)parts.push({label:'Session',value:context.session});
 if(context?.at)parts.push({label:'Reading',value:asOfText(context.at)||context.at});
 // The BOUNDARY, said plainly whenever it is not the live edge. A reader who does not know a tab is behind
 // cannot tell a quiet book from an old one.
 if(context&&context.is_newest===false&&context.newest_at)
  parts.push({label:'Newest held',value:asOfText(context.newest_at)||context.newest_at});
 if(context?.mode)parts.push({label:'Mode',value:context.mode==='live'?'Live session':'Session read back'});
 if(!parts.length&&!capture)return null;
 return <View accessibilityRole="summary"
  accessibilityLabel={`Analysis context. ${parts.map(p=>`${p.label} ${p.value}`).join('. ')}.`}
  style={[s.row,{flexWrap:'wrap',alignItems:'center',gap:phone?8:14,paddingHorizontal:12,paddingVertical:8,
   borderRadius:10,borderWidth:1,borderColor:C.line,backgroundColor:C.dark}]}>
  {parts.map(part=><View key={part.label} style={{gap:1,minWidth:0}}>
   <T numberOfLines={1} style={head}>{part.label}</T>
   <T numberOfLines={1} style={{fontSize:12,lineHeight:16,fontFamily:'InterSemi',color:C.ink,
    fontVariant:['tabular-nums'] as any}}>{part.value}</T>
  </View>)}
  <View style={{flex:1,minWidth:8}}/>
  <CaptureChip capture={capture}/>
 </View>;
}

export function DerivativeTab(){
 const {width:winW}=useWindowDimensions();
 const [width,setWidth]=useState(0);
 const [seq,setSeq]=useState(0);
 const [rules,setRules]=useState<FilterRule[]>(()=>sanitizeRules(readStore<any>('derivative','rules')));
 const [dialog,setDialog]=useState(false);
 const [hidden,setHidden]=useState<Record<string,boolean>>(()=>hiddenFrom(readStore<any>('derivative','widgets')));
 const [expanded,setExpanded]=useState<WidgetKey|''>('');
 const [target,setTarget]=useState<ChartTarget|null>(null);
 // THE STRIKE UNDER THE POINTER, anywhere on the tab. The delta-OI grid, the option chain, the OI bars and the
 // volatility-by-strike list are all about the same strikes and shared no visual link at all; this is that link.
 // It is a HIGHLIGHT: it selects nothing, it re-queries nothing, and it says nothing the panels do not.
 const [strikeHover,setStrikeHover]=useState<number|null>(null);
 const onStrikeHover=useCallback((v:number|null)=>setStrikeHover(x=>x===v?x:v),[]);
 const onTarget=useCallback((t:ChartTarget)=>setTarget(t),[]);
 const saveRules=useCallback((next:FilterRule[])=>{const clean=sanitizeRules(next);setRules(clean);
  writeStore('derivative','rules',clean)},[]);
 const hide=useCallback((key:string)=>setHidden(h=>{const next={...h,[key]:true};
  writeStore('derivative','widgets',next);return next}),[]);
 const restoreAll=useCallback(()=>{setHidden({});writeStore('derivative','widgets',{})},[]);
 const toggleExpand=useCallback((key:string)=>setExpanded(v=>v===key?'':key as WidgetKey),[]);
 const status=useDerivativeRead<Status>('/api/derivatives/status',seq);
 const filterData=useDerivativeRead<FilterData>('/api/derivatives/filters',seq);
 const captured=!!status.data?.captured;
 const page=width||winW;
 // ONE breakpoint for the whole tab. There is no second one and no block with a wider one: that was the shape
 // the owner rejected.
 const stacked=page<BLOCK_BESIDE,pinFirst=page<BLOCK_PIN_COLUMN,phone=pinFirst;
 const filters=useMemo(()=>rulesToFilters(rules),[rules]);
 const ruleLabel=useMemo(()=>ruleLabeller(filterData.data,rules),[filterData.data,rules]);
 const rulesLine=useMemo(()=>rulesText(rules,ruleLabel),[rules,ruleLabel]);
 // ================================================================================================================
 // THE ONE SCREENER READ.
 //
 // Made here rather than inside the bar, because the PAGE needs it too: the tab's default symbol is the busiest
 // underlying by premium AT THE READING ON SCREEN, and that is row one of this very list. It used to come from a
 // separate /unusual?limit=1 call, which could - and on a rebuilt session did - answer for a different reading
 // from the one the reader was looking at.
 // ================================================================================================================
 const [at,setAt]=useState('');
 const [buildupWindow,setBuildupWindow]=useState(BUILDUP_WINDOW_DEFAULT);
 // ONE ROW PER INSTRUMENT, and no second view to choose between. The owner: "Just show one row per instrument
 // - like nifty show one row, stock show one row." The contract list is sorted by premium and one busy index
 // owns every visible row of it: at 11:30 on 18 Sep 2026, 85 instruments had a contract over the floors and
 // NIFTY held 104 of the 491 contracts, so a hundred-row list was a hundred rows of NIFTY.
 //
 // `group` is sent so the query SAYS which list it wants rather than relying on a default two sides have to
 // agree on; a server that does not offer it refuses the parameter, and that refusal is one the reader sees.
 const query=useMemo(()=>screenerQuery(rules||[],{buildupWindow,at,group:SCREENER_VIEW_DEFAULT}),
  [rules,buildupWindow,at]);
 const screener=useDerivativeRead<Screener>(captured?`${SCREENER_PATH}${query}`:null,seq);
 // The 15-min reading the tab is ON: the one the screener resolved. Every point-in-time block below is read at
 // it, which is what stopped the option chain opening two thousand points from spot - the newest reading of a
 // session rebuilt from candles carries no spot at all, so a chain that took the newest had no anchor.
 const reading=screener.data?.reading_at||screener.data?.coverage?.at||'';
 // ================================================================================================================
 // ONE SYMBOL ACROSS THE TAB.
 //
 // The owner: "If i select nifty on the screener all the charts should show." So the symbol is resolved HERE,
 // exactly once, and handed to every block. No block resolves its own; that is how two panels end up describing
 // two different underlyings under one as-of line.
 //
 // The order is the owner's: a symbol chosen in Customize wins, then the last row clicked ANYWHERE on the tab,
 // then the busiest underlying by premium at the reading the screener is showing, then the first index — which
 // rests only on OI, and is the last resort. A symbol nobody chose is FLAGGED as a default in every header, and
 // `linked` is false, so neither the pill nor the rail lights up for it.
 // ================================================================================================================
 // THE TAB'S CONTEXT AND ITS CAPTURE HEALTH, resolved ONCE from the screener read that already fixed the
 // reading every block below is on. Neither is fetched a second time and neither is derived here: both are
 // the server's own, so a panel cannot disagree with the bar above it.
 // THE SCREENER'S, NOT THE STORE'S, once the screener has answered. Both are real, but they describe two
 // different readings - the store's is resolved at the newest reading it holds, the screener's at the reading
 // the tab is actually on - and putting the newest reading's health beside the on-screen reading's numbers
 // would be the very mixing this round exists to remove. The store-wide one stands in only while the screener
 // has not answered yet, when the tab is claiming no reading at all.
 const capture:CaptureHealth|null=screener.data?(screener.data.capture||null):(status.data?.capture||null);
 // ROW ONE OF THE LIST THE SERVER ACTUALLY SERVED, and the server's own name for the order it served it in.
 // This is not "the busiest by premium" and never was: the screener opens with the unusual first, and premium
 // is its third key. The badge now prints the ranking the server declares rather than a sort assumed here.
 const ranked=((screener.data?.rows||[])[0] as {underlying?:string}|undefined)?.underlying||'';
 const rankLabel=rankingLabel(screener.data?.ranking);
 const chosen=filters.underlying||target?.underlying||'';
 const needIndex=!chosen&&!ranked&&screener.phase!=='loading';
 const indexList=useDerivativeRead<{rows?:{underlying?:string}[]}>(needIndex?'/api/derivatives/indices':null,seq);
 const choice=useMemo(()=>resolveTabSymbol(filters.underlying,target?.underlying,ranked,
  (indexList.data?.rows||[])[0]?.underlying,rankLabel),
  [filters.underlying,target?.underlying,ranked,indexList.data,rankLabel]);
 const symbol=choice.symbol,badge=symbolBadge(choice),linked=!!symbol&&!choice.defaulted;
 // an expiry filter only means anything once a symbol was CHOSEN: a default symbol has its own front expiry
 const symbolExpiry=filters.underlying?filters.expiry:'';
 // The server's context for the reading on screen, with the symbol the TAB resolved written into it: the
 // screener answers for a list, and the instrument every block below is pointed at is chosen here.
 const context:AnalysisContext|null=useMemo(()=>{
  const served=screener.data?.context;
  if(!served)return null;
  return {...served,underlying:symbol||served.underlying,expiry:symbolExpiry||served.expiry};
 },[screener.data,symbol,symbolExpiry]);
 const refresh=()=>setSeq(x=>x+1);
 // ================================================================================================================
 // THE TAB'S ONE ΔOI-GRID READ.
 //
 // The ten at-the-money contracts of the tab's symbol - five calls at and above the money, five puts at and
 // below it. TWO panels are built on them: the ΔOI block's 2 x 5 grid, and the signal table beside the pinned
 // screener. Each used to make its own call to the same endpoint, which is two fetches of one payload and two
 // answers that can drift apart at a refresh. It is made HERE, once, for the same reason the symbol and the
 // screener read are: one read, one envelope, one as-of, and no way for two panels to describe different
 // contracts under one heading.
 // ================================================================================================================
 // `at` IS THE BOUNDARY, not a nicety. Without it this block resolved its own newest session while the chain
 // and the screener above it sat on another, which is one page describing two different moments - and on a
 // session whose capture died at 11:30 those two moments are four hours apart.
 const gridPath=symbol?`/api/derivatives/oi-grid?underlying=${encodeURIComponent(symbol)}${symbolExpiry?`&expiry=${encodeURIComponent(symbolExpiry)}`:''}${reading?`&at=${encodeURIComponent(reading)}`:''}`:null;
 const gridRead=useDerivativeRead<OiGrid>(gridPath,seq);
 // One target, one series read: clicking a row in ANY block re-points the same chart, and the tiles in the
 // blocks that draw it ask the pilot for nothing extra.
 const seriesQuery=target?(target.instrumentToken?`instrument_token=${target.instrumentToken}`
  :`underlying=${encodeURIComponent(target.underlying)}`):'';
 const series=useDerivativeRead<Series>(seriesQuery?`/api/derivatives/series?${seriesQuery}`:null,seq);
 const seriesState=stateOf(series,'Nothing has been captured for this contract yet.');
 const hiddenCount=WIDGET_KEYS.filter(k=>hidden[k]).length;
 const shows=(key:WidgetKey)=>!hidden[key]&&(!expanded||expanded===key);
 const frame=(key:WidgetKey)=>({onExpand:()=>toggleExpand(key),expanded:expanded===key,onClose:()=>hide(key)});
 /** The height a block takes. The template's own, always — except the one widget a reader expanded. */
 const blockHeight=expanded?EXPANDED_H:undefined;
 /** The tab's ONE linked chart, in the panel style the template handed the block it sits in. */
 const linkedChart=(key:WidgetKey)=>(style:PaneStyle)=>shows(key)
  ?<ChartTile key={key} target={target} body={series.data} state={seriesState} onRefresh={series.reload}
    height={Math.max(120,(style.height||BLOCK_H)-190)} style={style} inBlock {...frame(key)}/>
  :null;
 // Everything the four session blocks need, built once so they cannot drift apart: the same symbol, the same
 // badge for it, the same link state, the same expiry and the same show/hide state as every other block.
 // The two cross-underlying lists hand up what only they know, so their blocks carry an as-of and a headline
 // figure like every other block. A count of rows and the reading they came from - nothing derived.
 const [indexSummary,setIndexSummary]=useState<{count:number;asOf:string|null}>({count:0,asOf:null});
 const [futuresSummary,setFuturesSummary]=useState<{count:number;asOf:string|null}>({count:0,asOf:null});
 const onIndexSummary=useCallback((v:{count:number;asOf:string|null})=>setIndexSummary(x=>
  x.count===v.count&&x.asOf===v.asOf?x:v),[]);
 const onFuturesSummary=useCallback((v:{count:number;asOf:string|null})=>setFuturesSummary(x=>
  x.count===v.count&&x.asOf===v.asOf?x:v),[]);
 const sessionProps={symbol,choice,badge,linked,expiry:symbolExpiry,seq,target,onTarget,
  stacked:stacked||!!expanded,height:blockHeight,hidden,onHide:hide,expanded:expanded||undefined,
  onExpand:toggleExpand};
 let body:React.ReactNode;
 if(status.phase==='loading')body=<View style={{padding:28,alignItems:'center'}}>
  <T style={{fontSize:13,color:C.muted}}>Reading the F&amp;O store…</T></View>;
 else if(status.phase==='error')body=<View role="alert" style={{gap:8,padding:14,borderRadius:12,
  backgroundColor:C.amberBg,borderWidth:1,borderColor:'#4A3E1E'}}>
  <T style={{fontFamily:'InterSemi',fontSize:13,color:C.amber}}>The F&amp;O store could not be read</T>
  <T style={{fontSize:12,lineHeight:18}}>No widget is shown while the store is unavailable. This is not the same as
   nothing happening in the market. {status.error}</T>
  <HeaderChip label="Retry" a11y="Retry reading the F&O store" icon="refresh-cw" onPress={refresh}/>
 </View>;
 // Before the first capture the whole tab is one plain sentence — never an error page, and never a widget full of
 // zeros (§5). The widgets are not requested at all until there is something to describe.
 else if(!captured)body=<View style={{gap:10,padding:20,borderRadius:12,borderWidth:1,borderColor:C.line,
  backgroundColor:C.paper,alignItems:'center'}}>
  <View style={{padding:14,borderRadius:20,backgroundColor:C.soft}}><Icon name="clock" size={22} color={C.green}/></View>
  <T role="heading" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:17,lineHeight:24,textAlign:'center'}}>{EMPTY_TEXT}</T>
  <T style={{fontSize:12,lineHeight:18,color:C.muted,textAlign:'center',maxWidth:520}}>
   Snapshots are taken at every 15-minute candle close during the session (09:30, 09:45 … 15:30) for the front two
   expiries of each underlying. Until the first one lands there is nothing to describe, so this tab shows nothing
   rather than a number it does not have.
  </T>
  {!!status.data?.backfill_sessions&&<T style={{fontSize:12,color:C.muted}}>{status.data.backfill_sessions} sessions
   of 15-minute candles have been backfilled.</T>}
  {status.data?.available===false&&<T style={{fontSize:11,color:C.muted,textAlign:'center'}}>The store file does not exist yet.</T>}
  {status.data?.available===true&&status.data?.metrics_ready===false&&<T style={{fontSize:11,color:C.amber,
   textAlign:'center'}}>Contracts have been captured, but no 15-minute metrics have been written yet.</T>}
  <HeaderChip label="Check again" a11y="Check the F&O store again" icon="refresh-cw" onPress={refresh}/>
 </View>;
 else body=<View style={{gap:22}}>
  {/* 1. The chain of the contract the reader picked, and the tab's one chart of that contract beside it. */}
  <Block title="Option chain"
   subtitle="Calls on the left, puts on the right, at the 15-min reading the screener is on."
   badge={badge} linked={linked}
   headline={symbol
    ?{label:'Contract on the chart',value:target?.label||DASH,against:target?.detail||'',
      reason:'Click any strike on this chain, or any row on the screener above, to draw it.'}
    :{label:'Contract on the chart',value:DASH,
      reason:'Click any row on the screener above, or choose a symbol in Customize, to see a chain.'}}
   stacked={stacked||!!expanded} height={blockHeight} idle={!symbol}
   chart={linkedChart('chain_chart')}
   content={style=>shows('chain')?<ChainWidget key="chain" underlying={symbol} expiry={symbolExpiry}
    at={reading} seq={seq}
    target={target} onTarget={onTarget} filterCount={rules.length} onCustomize={()=>setDialog(true)}
    highlight={strikeHover} onHighlight={onStrikeHover} style={style} {...frame('chain')}/>:null}/>
  {/* 2. Where the open interest stands, by strike: the bars on the left, the numbers on the right. */}
  <OiByStrikeSection underlying={symbol} badge={badge} linked={linked} expiry={symbolExpiry} at={reading}
   seq={seq}
   onTarget={onTarget} stacked={stacked||!!expanded} pinFirst={pinFirst} filterCount={rules.length}
   onCustomize={()=>setDialog(true)} height={blockHeight} hidden={hidden} onHide={hide}
   expanded={expanded||undefined} onExpand={toggleExpand}
   highlight={strikeHover} onHighlight={onStrikeHover}/>
  {/* 3. The owner's ΔOI block. The futures chart is the CHART panel and the 2 x 5 grid IS the content panel —
      which is how a block that used to be three panels wide fits the one template without losing a tile. */}
  <OiGridSection symbol={symbol} badge={badge} linked={linked} expiry={symbolExpiry} seq={seq} read={gridRead} rules={rules}
   target={target} onTarget={onTarget} onCustomize={()=>setDialog(true)} stacked={stacked||!!expanded}
   height={blockHeight} hidden={hidden} onHide={hide} expanded={expanded||undefined} onExpand={toggleExpand}
   highlight={strikeHover} onHighlight={onStrikeHover}/>
  {/* 4-7. The four session blocks. Each one is pointed at the SAME symbol every block above is pointed at. */}
  <PcrSection {...sessionProps}/>
  <MaxPainSection {...sessionProps}/>
  <IvSection {...sessionProps} gridRead={gridRead} highlight={strikeHover} onHighlight={onStrikeHover}/>
  <FuturesBuildupSection {...sessionProps}/>
  {/* 8-9. The two cross-underlying LISTS. They are not about the tab's one symbol — they are their own cuts of
      the book — so they are the two blocks whose chart panel is the tab's linked chart rather than a series of
      their own. They still wear the template: same widths, same height, same header. */}
  <Block title="Index dashboard" subtitle="NIFTY, BANKNIFTY and FINNIFTY as captured at this 15-min reading."
   asOf={asOfText(indexSummary.asOf)}
   headline={{label:'Index underlyings captured',value:indexSummary.count?String(indexSummary.count):DASH,
    against:'NIFTY, BANKNIFTY and FINNIFTY, each as one row of this 15-min reading',
    reason:'No index snapshot has been captured yet.'}}
   stacked={stacked||!!expanded} height={blockHeight}
   chart={linkedChart('indices_chart')}
   content={style=>shows('indices')?<IndexWidget key="indices" seq={seq} target={target} onTarget={onTarget}
    onSummary={onIndexSummary} pinFirst={pinFirst} style={style} {...frame('indices')}/>:null}/>
  <Block title="Futures OI build-up"
   subtitle="The front futures contract of each underlying, with its build-up, OI share and basis."
   asOf={asOfText(futuresSummary.asOf)}
   headline={{label:'Front futures contracts',value:futuresSummary.count?String(futuresSummary.count):DASH,
    against:'one per underlying the store carries, at this 15-min reading',
    reason:'No futures contract has been captured yet.'}}
   stacked={stacked||!!expanded} height={blockHeight}
   chart={linkedChart('futures_chart')}
   content={style=>shows('futures')?<FuturesWidget key="futures" underlying={filters.underlying}
    watchlist={filters.watchlist} seq={seq} target={target} onTarget={onTarget} onSummary={onFuturesSummary}
    filterCount={rules.length} onCustomize={()=>setDialog(true)} pinFirst={pinFirst} style={style}
    {...frame('futures')}/>:null}/>
 </View>;
 // The page: a title, the ONE screener, then the blocks — all scrolling together.
 //
 // THE SCREENER IS NOT PINNED, deliberately. It was, and the owner's verdict was immediate: "I can't see
 // anything other than the screener." A sticky bar only works when it is a thin strip; this one is a table
 // beside a signal table, and stuck to the top it held most of the window for itself and left a sliver for
 // nine blocks to scroll through. A reader who has chosen an instrument wants to READ the blocks, not keep
 // the list they chose from in view. It scrolls away like everything else.
 // EVERY BLOCK CARRIES THE CAPTURE CAVEAT WITHOUT BEING TOLD. A partial capture is a fact about the data
 // under all nine of them, and threading it through seven components is seven places to forget it.
 return <CaptureContext.Provider value={capture}>
  <View onLayout={e=>{const w=Math.round(e.nativeEvent.layout.width);setWidth(v=>Math.abs(v-w)<2?v:w)}}
  style={{flex:1,minHeight:0,backgroundColor:C.bg}}>
  <ScrollView style={{flex:1}}
   contentContainerStyle={{paddingHorizontal:phone?12:20,paddingTop:4,paddingBottom:40,gap:14}}>
   <View style={{gap:10}}>
    <View style={[s.row,{flexWrap:'wrap',gap:phone?8:10,paddingTop:phone?6:10}]}>
     <T role="heading" aria-level={1} style={{fontFamily:'ManropeBold',fontSize:phone?20:22,lineHeight:phone?28:30,
      letterSpacing:-.4,marginRight:4}}>Derivative</T>
     <View style={[s.row,{gap:6}]}><Icon name="clock" size={13} color={C.muted}/>
      <T style={{fontSize:12,color:C.muted}}>{asOfText(status.data?.as_of)}</T></View>
     <View style={{flex:1}}/>
     {!!expanded&&<HeaderChip label="Show every widget" a11y="Restore the expanded widget and show the whole tab"
      icon="minimize-2" onPress={()=>setExpanded('')}/>}
     {!!hiddenCount&&<HeaderChip label={`Show ${hiddenCount} closed widget${hiddenCount===1?'':'s'}`}
      a11y={`Bring back the ${hiddenCount} widgets closed on this tab`} icon="eye" onPress={restoreAll}/>}
     {captured&&<HeaderChip label={rules.length?`Filters (${rules.length})`:'Filters'}
      a11y={`Open the filter builder. ${rules.length} filters in force`} icon="filter" onPress={()=>setDialog(true)}/>}
     <IconButton icon="refresh-cw" label="Refresh every widget on the Derivative tab" tooltip="Refresh all"
      onPress={refresh} style={{width:32,height:32}}/>
    </View>
    <T style={{fontSize:12,lineHeight:18,color:C.muted,maxWidth:760}}>
     What the F&amp;O book is doing right now, captured every 15 minutes. Nothing here is a forecast and nothing here
     feeds a trading decision — each block says when it was captured and which liquidity floors it applied.
    </T>
    {captured&&!!sourceText(status.data?.source)&&<T style={{fontSize:11,color:C.muted}}>{sourceText(status.data?.source)}</T>}
   </View>
   {/* The one context bar: what exactly is on screen, and how healthy the capture behind it is. It sits
       ABOVE the screener because it describes everything under it, and it is one line high. */}
   {captured&&<ContextBar context={context} capture={capture} phone={phone}/>}
   {/* index 1: THE PINNED SCREENER. One on the tab, and it stays here while everything else scrolls. */}
   {captured?<ScreenerRail read={screener} rules={rules} rulesLine={rulesLine} ruleLabel={ruleLabel} seq={seq}
    badge={badge} linked={linked} target={target} onTarget={onTarget} onCustomize={()=>setDialog(true)}
    at={at} onAt={setAt} buildupWindow={buildupWindow} onBuildupWindow={setBuildupWindow} pinFirst={pinFirst}
    symbol={symbol} gridRead={gridRead}
    show={shows(PINNED_KEY)} height={expanded===PINNED_KEY?EXPANDED_H:undefined}
    onExpand={()=>toggleExpand(PINNED_KEY)} expanded={expanded===PINNED_KEY}/>:<View/>}
   {body}
  </ScrollView>
  <FilterDialog visible={dialog} onClose={()=>setDialog(false)} name="Derivative" data={filterData.data}
   rules={rules} onApply={saveRules}/>
 </View>
 </CaptureContext.Provider>;
}
