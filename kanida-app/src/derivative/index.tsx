// The Derivative tab (docs/DERIVATIVES_SPEC.md §4), rebuilt to the shape of the TrendSpider dashboard the owner
// benchmarked: a section title in large text, then that section's widgets — a wide dense table on the left and the
// linked chart tile on the right — stacking down the page. Every widget wears the same header bar
// (name · "Customize (N filters)…" · refresh / expand / close) and the filters live in one builder popup instead of
// a permanent row of chips.
//
// What this tab is: a description of what is happening in the F&O book right now, at 15-min capture readings.
// What it is NOT (§5): a prediction, an evidence card, or an input to any trading gate. Every widget states its own
// as-of time and the liquidity floors in force; a ratio without a baseline says "no baseline"; a number that was not
// captured is a dash. The rebuild changed the presentation only — not one number, label or rule.
import React,{useCallback,useMemo,useState} from 'react';
import {View,ScrollView,useWindowDimensions} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {IconButton,readStore,writeStore} from '../layout';
import {HeaderChip} from '../discover/parts';
import {useDerivativeRead} from './useDerivatives';
import {IDLE_H,Section} from './frame';
import {FilterDialog,ruleLabeller} from './FilterDialog';
import {ScreenerSection} from './ScreenerSection';
import {ChainWidget} from './ChainWidget';
import {OiByStrikeSection} from './OiByStrikeSection';
import {OiGridSection} from './OiGridSection';
import {PcrSection,MaxPainSection,IvSection,FuturesBuildupSection} from './SessionBlocks';
import {IndexWidget} from './IndexWidget';
import {FuturesWidget} from './FuturesWidget';
import {ChartTile} from './ChartTile';
import {EMPTY_TEXT,asOfText,resolveTabSymbol,rulesText,rulesToFilters,sanitizeRules,sourceText,symbolBadge,
 type FilterRule} from './logic';
import {stateOf} from './frame';
import type {ChartTarget,FilterData,Series,Status,Unusual} from './types';
export {EMPTY_TEXT} from './logic';
/** Below this the chart tile drops under its table; below the tablet break the page is one pinned-column table. */
export const CHART_BESIDE=1100,PIN_COLUMN=760;
/** The ΔOI block carries THREE panels side by side — screener, futures chart, 2 x 5 grid — so it needs more
 *  room than a table-and-chart row does. Below this the grid would fall under five columns across; the block
 *  stacks into one full-width column instead, which costs scrolling rather than content. */
export const GRID_BESIDE=1340;
const ROW_H=560,STACK_TABLE_H=460,EXPANDED_H=720,CHART_H=210;
// The ΔOI block is the one section whose content sets its own height: two bands of tiles, each tile carrying
// a chart, a direction chip and the two lines under it. At ROW_H the puts band was cut off mid-chart, so this
// block still gets its own figure — but every tile is now a fixed height and the rules that used to be
// printed under the panels moved into the block's one info panel, so the figure is smaller than it was.
const GRID_ROW_H=576,GRID_STACK_H=640;
/** The four session blocks all draw one line chart across one session, so they all take one height. */
const SESSION_ROW_H=384,SESSION_STACK_H=360;
/** Every widget the page can show, in the order the sections run. */
export const WIDGET_KEYS=['unusual','unusual_chart','chain','chain_chart','oi_table','oi_chart',
 'oi_grid_screener','oi_grid_futures','oi_grid','pcr_chart','pcr_readings','max_pain_chart','max_pain_readings',
 'iv_chart','iv_strikes','fut_oi','fut_basis','fut_readings','indices','indices_chart','futures',
 'futures_chart'] as const;
export type WidgetKey=(typeof WIDGET_KEYS)[number];
const hiddenFrom=(stored:unknown)=>{
 const raw=(stored||{}) as Record<string,unknown>,out:Record<string,boolean>={};
 for(const key of WIDGET_KEYS)if(raw[key]===true)out[key]=true;
 return out;
};
export function DerivativeTab(){
 const {width:winW}=useWindowDimensions();
 const [width,setWidth]=useState(0);
 const [seq,setSeq]=useState(0);
 const [rules,setRules]=useState<FilterRule[]>(()=>sanitizeRules(readStore<any>('derivative','rules')));
 const [dialog,setDialog]=useState(false);
 const [hidden,setHidden]=useState<Record<string,boolean>>(()=>hiddenFrom(readStore<any>('derivative','widgets')));
 const [expanded,setExpanded]=useState<WidgetKey|''>('');
 const [target,setTarget]=useState<ChartTarget|null>(null);
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
 const stacked=page<CHART_BESIDE,pinFirst=page<PIN_COLUMN,phone=pinFirst;
 const filters=useMemo(()=>rulesToFilters(rules),[rules]);
 const ruleLabel=useMemo(()=>ruleLabeller(filterData.data,rules),[filterData.data,rules]);
 const rulesLine=useMemo(()=>rulesText(rules,ruleLabel),[rules,ruleLabel]);
 // ================================================================================================================
 // ONE SYMBOL ACROSS THE TAB.
 //
 // The owner: "If i select nifty on the screener all the charts should show." So the symbol is resolved HERE,
 // exactly once, and handed to every block — the chain, the strikes, the ΔOI tiles and their futures chart, PCR,
 // max pain, IV and the futures build-up. No block resolves its own; that is how two panels end up describing two
 // different underlyings under one as-of line.
 //
 // The order is the owner's: a symbol chosen in Customize wins, then the last row clicked ANYWHERE on the tab,
 // then the busiest underlying by premium traded at this 15-min reading, then the first index — which rests only
 // on OI, and is the fallback because the premium list is empty whenever the newest reading was rebuilt from
 // candles, which carry no VWAP. A symbol nobody chose is FLAGGED as a default in every block's header badge.
 // ================================================================================================================
 const chosen=filters.underlying||target?.underlying||'';
 const busiest=useDerivativeRead<Unusual>(chosen?null:'/api/derivatives/unusual?limit=1',seq);
 const indexList=useDerivativeRead<{rows?:{underlying?:string}[]}>(chosen?null:'/api/derivatives/indices',seq);
 const choice=useMemo(()=>resolveTabSymbol(filters.underlying,target?.underlying,
  (busiest.data?.rows||[])[0]?.underlying,(indexList.data?.rows||[])[0]?.underlying),
  [filters.underlying,target?.underlying,busiest.data,indexList.data]);
 const symbol=choice.symbol,badge=symbolBadge(choice);
 // an expiry filter only means anything once a symbol was CHOSEN: a default symbol has its own front expiry
 const symbolExpiry=filters.underlying?filters.expiry:'';
 // no symbol at all means no chain AND no series for the tile beside it: the whole section is idle
 const chainIdle=!symbol;
 const refresh=()=>setSeq(x=>x+1);
 // One target, one series read: clicking a row in ANY section re-points the same chart, and the tiles beside each
 // table draw that one series rather than each asking the pilot for the same marks again.
 const seriesQuery=target?(target.instrumentToken?`instrument_token=${target.instrumentToken}`
  :`underlying=${encodeURIComponent(target.underlying)}`):'';
 const series=useDerivativeRead<Series>(seriesQuery?`/api/derivatives/series?${seriesQuery}`:null,seq);
 const seriesState=stateOf(series,'Nothing has been captured for this contract yet.');
 const hiddenCount=WIDGET_KEYS.filter(k=>hidden[k]).length;
 const shows=(key:WidgetKey)=>!hidden[key]&&(!expanded||expanded===key);
 const frame=(key:WidgetKey)=>({onExpand:()=>toggleExpand(key),expanded:expanded===key,onClose:()=>hide(key)});
 // `idle` = this section has no symbol yet, so BOTH its panels are showing one sentence. They collapse
 // together, which keeps the row square: a section where one panel shrank and the other did not would read
 // worse than the hole it was meant to close.
 const paneStyle=(key:WidgetKey,grow:number,tall:number,idle?:boolean)=>{
  if(expanded===key)return {height:EXPANDED_H};
  const h=idle?IDLE_H:stacked?tall:ROW_H;
  return stacked?{height:h}:{flex:grow,minWidth:0,height:h};
 };
 const tableStyle=(key:WidgetKey,idle?:boolean)=>paneStyle(key,2,STACK_TABLE_H,idle);
 const chartStyle=(key:WidgetKey,idle?:boolean)=>paneStyle(key,1,CHART_H+200,idle);
 const chart=(key:WidgetKey,idle?:boolean)=>shows(key)?<ChartTile key={key} target={target} body={series.data}
  state={seriesState} onRefresh={series.reload} height={expanded===key?EXPANDED_H-190:idle?IDLE_H-90:CHART_H}
  style={chartStyle(key,idle)} {...frame(key)}/>:null;
 const section=(title:string,subtitle:string,keys:WidgetKey[],children:React.ReactNode)=>
  keys.some(shows)?<Section key={title} title={title} subtitle={subtitle} stacked={stacked||!!expanded}>{children}</Section>:null;
 // Everything the four session blocks need, built once so they cannot drift apart: the same symbol, the same
 // badge for it, the same expiry, the same height and the same show/hide state as every other block on the tab.
 const sessionProps={symbol,choice,badge,expiry:symbolExpiry,seq,target,onTarget,
  stacked:stacked||!!expanded,height:expanded?EXPANDED_H:stacked?SESSION_STACK_H:SESSION_ROW_H,
  hidden,onHide:hide,expanded:expanded||undefined,onExpand:toggleExpand};
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
 else body=<View style={{gap:20}}>
  {/* Section 1, the tab's workhorse: the screener and the linked chart of the row it points at. */}
  <ScreenerSection title='Unusual activity' rules={rules} rulesLine={rulesLine} ruleLabel={ruleLabel} seq={seq}
   badge={badge} target={target} onTarget={onTarget} onCustomize={()=>setDialog(true)}
   stacked={stacked||!!expanded} pinFirst={pinFirst} showTable={shows('unusual')}
   height={expanded==='unusual'?EXPANDED_H:stacked?STACK_TABLE_H:ROW_H}
   chart={idle=>chart('unusual_chart',idle)} {...frame('unusual')}/>
  {section('Option chain',symbol?`${symbol} — calls on the left, puts on the right, at the latest 15-min reading.`
   :'Choose a symbol in Customize, or click any row above, to see its chain.',['chain','chain_chart'],<>
   {shows('chain')&&<ChainWidget underlying={symbol} expiry={symbolExpiry} seq={seq} target={target}
    onTarget={onTarget} filterCount={rules.length} onCustomize={()=>setDialog(true)}
    style={tableStyle('chain',chainIdle)} {...frame('chain')}/>}
   {chart('chain_chart',chainIdle)}
  </>)}
  {(shows('oi_table')||shows('oi_chart'))&&<OiByStrikeSection underlying={symbol} expiry={symbolExpiry}
   seq={seq} onTarget={onTarget} stacked={stacked||!!expanded} pinFirst={pinFirst} filterCount={rules.length}
   onCustomize={()=>setDialog(true)} height={expanded?EXPANDED_H:stacked?STACK_TABLE_H:ROW_H}
   chartHeight={expanded?EXPANDED_H-260:CHART_H} hidden={hidden} onHide={hide} expanded={expanded||undefined}
   onExpand={toggleExpand}/>}
  {/* The owner's ΔOI block, left to right: the screener, the futures chart of the tab's symbol, and the
      2 x 5 grid of ΔOI tiles. Clicking a screener row or a tile re-points the whole tab. */}
  {(shows('oi_grid_screener')||shows('oi_grid_futures')||shows('oi_grid'))&&<OiGridSection symbol={symbol}
   badge={badge} expiry={symbolExpiry} seq={seq} rules={rules} rulesLine={rulesLine} target={target}
   onTarget={onTarget} onCustomize={()=>setDialog(true)} stacked={stacked||page<GRID_BESIDE||!!expanded}
   height={expanded?EXPANDED_H:stacked?GRID_STACK_H:GRID_ROW_H} hidden={hidden} onHide={hide}
   expanded={expanded||undefined} onExpand={toggleExpand}/>}
  {/* The four session blocks. Each one is pointed at the SAME symbol every block above is pointed at. */}
  <PcrSection {...sessionProps}/>
  <MaxPainSection {...sessionProps}/>
  <IvSection {...sessionProps}/>
  {/* Three panels, not two, so it needs the room the ΔOI block needs before they sit side by side. */}
  <FuturesBuildupSection {...sessionProps} stacked={stacked||page<GRID_BESIDE||!!expanded}/>
  {section('Index dashboard','NIFTY, BANKNIFTY and FINNIFTY as captured at this 15-min reading.',['indices','indices_chart'],<>
   {shows('indices')&&<IndexWidget seq={seq} target={target} onTarget={onTarget} pinFirst={pinFirst}
    style={tableStyle('indices')} {...frame('indices')}/>}
   {chart('indices_chart')}
  </>)}
  {section('Futures OI build-up','The front futures contract of each underlying, with its build-up, OI share and basis.',
   ['futures','futures_chart'],<>
   {shows('futures')&&<FuturesWidget underlying={filters.underlying} watchlist={filters.watchlist} seq={seq}
    target={target} onTarget={onTarget} filterCount={rules.length} onCustomize={()=>setDialog(true)}
    pinFirst={pinFirst} style={tableStyle('futures')} {...frame('futures')}/>}
   {chart('futures_chart')}
  </>)}
 </View>;
 return <View onLayout={e=>{const w=Math.round(e.nativeEvent.layout.width);setWidth(v=>Math.abs(v-w)<2?v:w)}}
  style={{flex:1,minHeight:0,backgroundColor:C.bg}}>
  <ScrollView style={{flex:1}} contentContainerStyle={{paddingHorizontal:phone?12:20,paddingTop:4,paddingBottom:40,gap:14}}>
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
    feeds a trading decision — each widget says when it was captured and which liquidity floors it applied.
   </T>
   {captured&&!!sourceText(status.data?.source)&&<T style={{fontSize:11,color:C.muted}}>{sourceText(status.data?.source)}</T>}
   {body}
  </ScrollView>
  <FilterDialog visible={dialog} onClose={()=>setDialog(false)} name="Derivative" data={filterData.data}
   rules={rules} onApply={saveRules}/>
 </View>;
}
