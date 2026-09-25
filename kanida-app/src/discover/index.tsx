// Discover Strategies page (docs/FALCON_DISCOVER_SPEC.md §3). Owns its vertical ScrollView (the shell renders /discover full height).
import React,{useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {View,ScrollView,useWindowDimensions} from 'react-native';
import {router,useLocalSearchParams} from 'expo-router';
import {C,T,Icon,s} from '../ui';
import {Popover,BottomSheet,DataAgePill,DataStatusPopover,WidgetError,IconButton,readStore,writeStore,formatDataDate,connectionView,type SheetSnap} from '../layout';
import {useProduct} from '../context';
import {useCatalog,clearStrategyCache} from './useStrategies';
import {gridMode,type ColumnsPref} from './logic';
import {useBlockLinks} from './link';
import {parseDiscoverParams,paramsSig} from './deeplink';
import {Block,LinkedCard,renderersFor} from './Block';
import {HeaderChip,RadioMenu,Skeleton,CenterNote,BLOCK_COLORS} from './parts';
export {discoverHref,chartHref,parseDiscoverParams} from './deeplink';
export function DiscoverStrategies(){
 const params=useLocalSearchParams() as Record<string,unknown>,sig=paramsSig(params);
 const deep=useMemo(()=>parseDiscoverParams(params),[sig]);
 const {data:catalog,error,loading,reload}=useCatalog();
 const {width:winW,height:winH}=useWindowDimensions();const [width,setWidth]=useState(0),[pageH,setPageH]=useState(0);
 const [columns,setColumns]=useState<ColumnsPref>(()=>{const v=readStore<{columns:string}>('discover','page').columns;return v==='2'||v==='4'?v:'auto'});
 const mode=gridMode(width||winW,columns);
 const [refreshSeq,setRefreshSeq]=useState(0);const {links,setLink}=useBlockLinks();
 const [sheet,setSheet]=useState<string|null>(null),[snap,setSnap]=useState<SheetSnap>('half');
 const [menu,setMenu]=useState<''|'universe'|'columns'>('');const uniRef=useRef<any>(null),colRef=useRef<any>(null);
 // The page's data pill opens the same Data status popover as the top bar; its provenance comes
 // from /api/state (polled by ProductProvider), its age from the strategy catalog shown here.
 const product=useProduct();const [dataOpen,setDataOpen]=useState(false);const dataRef=useRef<any>(null);
 const scrollRef=useRef<any>(null),blockY=useRef<Record<string,number>>({}),scrolled=useRef('');
 const blocks=useMemo(()=>(catalog?.blocks||[]).filter(b=>b.enabled).sort((a,b)=>a.order-b.order),[catalog]);
 const target=useMemo(()=>{if(!deep||!blocks.length)return '';if(deep.block&&blocks.some(b=>b.key===deep.block))return deep.block;return (blocks.find(b=>b.strategies.some(x=>x.key===deep.a||x.key===deep.bb))||blocks[0]).key},[deep,blocks]);
 const tryScroll=useCallback((key:string)=>{if(!deep||key!==target||scrolled.current===deep.sig)return;const y=blockY.current[key];if(y==null)return;scrolled.current=deep.sig;setTimeout(()=>scrollRef.current?.scrollTo?.({y:Math.max(0,y-8),animated:true}),0)},[deep,target]);
 const onLayoutY=useCallback((key:string,y:number)=>{blockY.current[key]=y;tryScroll(key)},[tryScroll]);
 useEffect(()=>{if(target)tryScroll(target)},[target,tryScroll]);
 useEffect(()=>{if(mode!=='stack')setSheet(null)},[mode]);
 // Phone: a tapped stock opens chart + backtest at the full snap, so the chart card header and chart are never cut by the half snap.
 const openSheet=useCallback((key:string)=>{setSheet(key);setSnap('full')},[]);
 const phone=mode==='stack';
 const refreshAll=()=>{clearStrategyCache();reload(true);setRefreshSeq(x=>x+1)};
 const uniLabel=catalog?.universe?.label||'NIFTY 500',uniKey=catalog?.universe?.key||'nifty500';
 const sheetBlock=blocks.find(b=>b.key===sheet),sheetLink=sheet?links[sheet]||null:null;
 // The phone sheet IS the leading block's chart + backtest pair, so it must render the same cards the wide
 // layouts give that block. `kind` alone is ambiguous - 'chart' is used by both the stored-scan block and the
 // researched chart-pattern block - so the renderers are resolved from the block itself (Block.renderersFor),
 // exactly as Block does. Without this the researched blocks fell back to the stored BacktestCard on phones,
 // which asks /api/backtests/cell for a research cell that does not exist.
 const sheetRenderers=useMemo(()=>sheetBlock?renderersFor(sheetBlock):undefined,[sheetBlock]);
 const date=formatDataDate(catalog?.data_end||undefined);
 // A live feed that is current is never 'N days old' (a long weekend is not staleness); the header line and
 // the Data status panel say when it is genuinely behind. The banner stays for the stored research source.
 const ds=product?.state?.data_status,liveCurrent=!!ds&&!ds.error&&ds.source?.live===true&&ds.stalled?.value!==true&&ds.stale?.value!==true;
 // The scanner takes minutes to load its scan and can restart mid-day (BACKLOG item 2a). While it is quiet
 // the pilot keeps serving the LAST scan, so the catalog still arrives - carrying when it was taken. The page
 // then says that in one quiet line and shows the cards; the full-page error is kept for the one case that
 // deserves it, nothing cached at all. `conn` prefers the catalog's own provenance and falls back to the
 // global state, so the line always describes the data actually on screen.
 const conn=connectionView(catalog||product?.state,{error:catalog?'':error});
 let body:React.ReactNode;
 if(!catalog&&loading)body=<View style={{height:260,borderWidth:1,borderColor:C.line,borderRadius:12,backgroundColor:C.paper}}><Skeleton chart lines={3}/></View>;
 else if(!catalog)body=<WidgetError title={conn?.mode==='offline'?'No scan to show yet':'Strategies unavailable'} message={conn?`${conn.text} ${conn.detail}`:`Strategy results are unavailable right now.${error?` ${error}`:''}`} onRetry={()=>reload(true)}/>;
 else if(!blocks.length)body=<CenterNote icon="layers" text="No strategy blocks are enabled yet."/>;
 return <View onLayout={e=>{const w=Math.round(e.nativeEvent.layout.width),h=Math.round(e.nativeEvent.layout.height);setWidth(v=>Math.abs(v-w)<2?v:w);setPageH(v=>Math.abs(v-h)<2?v:h)}} style={{flex:1,minHeight:0,backgroundColor:C.bg}}>
  <ScrollView ref={scrollRef} style={{flex:1}} contentContainerStyle={{paddingHorizontal:mode==='stack'?12:20,paddingTop:4,paddingBottom:sheet&&sheetLink?Math.round(winW>0?420:0):40,gap:18}}>
   <View style={[s.row,{flexWrap:'wrap',gap:phone?8:10,paddingTop:phone?6:10}]}>
    <T role="heading" aria-level={1} style={{fontFamily:'ManropeBold',fontSize:phone?20:22,lineHeight:phone?28:30,letterSpacing:-.4,marginRight:phone?0:4}}>Market scans</T>
    <HeaderChip ref={uniRef} label={uniLabel} a11y={`Universe: ${uniLabel}. Change universe`} expanded={menu==='universe'} onPress={()=>setMenu('universe')}/>
    <View ref={dataRef} style={{flexShrink:1,minWidth:0}}><DataAgePill variant="line" dataEnd={catalog?.data_end||undefined} ageDays={catalog?.age_days??undefined} status={product?.state?.data_status} cache={catalog||product?.state} error={catalog?'':error} expanded={dataOpen} onPress={()=>setDataOpen(o=>!o)}/></View>
    <View style={{flex:1}}/>
    <HeaderChip label="Pattern history pilot" a11y="Open Double Bottom and Bullish Engulfing history" icon="bar-chart-2" onPress={()=>router.push('/pattern-history')}/>
    <HeaderChip label="Review drawings" a11y="Review the chart and candlestick pattern drawings" icon="edit-3" onPress={()=>router.push('/pattern-drawing-review')}/>
    {!phone&&<HeaderChip ref={colRef} label={`Columns: ${columns==='auto'?'Auto':columns}`} a11y={`Card columns: ${columns==='auto'?'Auto':columns}. Change density`} expanded={menu==='columns'} onPress={()=>setMenu('columns')}/>}
    {phone?<IconButton icon="refresh-cw" label="Refresh all strategy cards" tooltip="Refresh all" onPress={refreshAll} style={{width:32,height:32}}/>:<HeaderChip label="Refresh all" a11y="Refresh all strategy cards" icon="refresh-cw" onPress={refreshAll}/>}
   </View>
   {!!conn&&<View role="status" accessibilityLiveRegion="polite" accessibilityLabel={conn.a11y} style={[s.row,{gap:8,padding:10,borderRadius:10,backgroundColor:conn.tone==='very-stale'?'#2A1519':C.amberBg,borderWidth:1,borderColor:conn.tone==='very-stale'?C.red+'55':'#4A3E1E'}]}><Icon name="alert-circle" size={15} color={conn.tone==='very-stale'?C.red:C.amber}/><T style={{flex:1,fontSize:12,lineHeight:17,color:conn.tone==='very-stale'?C.red:C.amber}}>{conn.text}</T><HeaderChip label="Retry" a11y="Retry the scanner connection" icon="refresh-cw" onPress={refreshAll}/></View>}
   {!conn&&!!catalog?.stale&&!liveCurrent&&<View role="status" style={[s.row,{gap:8,padding:10,borderRadius:10,backgroundColor:C.amberBg,borderWidth:1,borderColor:'#4A3E1E'}]}><Icon name="alert-circle" size={15} color={C.amber}/><T style={{flex:1,fontSize:12,lineHeight:17,color:C.amber}}>Research only — prices are {catalog.age_days!=null?`${catalog.age_days} days old`:'of unknown age'}{date?` (data to ${date})`:''}. Lists are stored scans, not current signals.</T></View>}
   {!conn&&!!catalog&&!!error&&<T style={{fontSize:12,color:C.amber}}>Could not refresh the strategy catalog. Showing the last loaded version.</T>}
   {body}
   {!!catalog&&blocks.map((b,i)=><Block key={b.key} block={b} index={i} catalog={catalog} mode={mode} deep={b.key===target?deep:null} refreshSeq={refreshSeq} link={links[b.key]||null} setLink={setLink} onOpenSheet={openSheet} onLayoutY={onLayoutY}/>)}
  </ScrollView>
  {mode==='stack'&&!!sheetBlock&&!!sheetLink&&<BottomSheet snap={snap} onSnapChange={setSnap} label="Chart and backtest" fullRatio={winH>0&&pageH>0?Math.min(.92,(pageH-8)/winH):.92} halfRatio={winH>0&&pageH>0?Math.min(.5,pageH*.55/winH):.5} header={<View style={[s.row,{gap:6}]}><T numberOfLines={1} style={{flex:1,fontFamily:'InterSemi',fontSize:14,lineHeight:20}}>{sheetLink.symbol} · {sheetLink.patternName} · {sheetLink.timeframe}</T><IconButton icon="x" label="Close chart and backtest" tooltip="Close" onPress={()=>setSheet(null)}/></View>}>
   <ScrollView style={{flex:1}} contentContainerStyle={{padding:12,gap:12}}>
    <LinkedCard kind={sheetBlock.kind} renderers={sheetRenderers} which="chart" blockTitle={sheetBlock.title} link={sheetLink} style={{height:340}} color={BLOCK_COLORS[Math.max(0,blocks.indexOf(sheetBlock))%BLOCK_COLORS.length]} stale={catalog?.stale} ageDays={catalog?.age_days}/>
    <LinkedCard kind={sheetBlock.kind} renderers={sheetRenderers} which="backtest" blockTitle={sheetBlock.title} link={sheetLink} style={{height:520}} color={BLOCK_COLORS[Math.max(0,blocks.indexOf(sheetBlock))%BLOCK_COLORS.length]} stale={catalog?.stale} ageDays={catalog?.age_days} minTrades={sheetBlock.strategies.find(x=>x.key===sheetLink.strategyKey)?.min_trades??10}/>
   </ScrollView>
  </BottomSheet>}
  <DataStatusPopover open={dataOpen} onClose={()=>setDataOpen(false)} anchor={dataRef} placement="bottom-start" status={product?.state?.data_status} dataEnd={catalog?.data_end||undefined} ageDays={catalog?.age_days??undefined} cache={catalog||product?.state} error={catalog?'':error}/>
  <Popover open={menu==='universe'} onClose={()=>setMenu('')} anchor={uniRef} label="Universe" role="none" width={240}><RadioMenu label="Universe" value={uniKey} options={[{value:uniKey,label:uniLabel,detail:catalog?.universe?.count?`${catalog.universe.count} stocks`:undefined}]} onPick={()=>{}} onClose={()=>setMenu('')} note="More universes are coming later."/></Popover>
  <Popover open={menu==='columns'} onClose={()=>setMenu('')} anchor={colRef} placement="bottom-end" label="Card columns" role="none" width={250}><RadioMenu label="Card columns" value={columns} onClose={()=>setMenu('')} onPick={v=>{const c=v as ColumnsPref;setColumns(c);writeStore('discover','page',{columns:c})}} note={mode==='stack'?'Phones always stack cards.':undefined}
   options={[{value:'auto',label:'Auto',detail:'4 wide on large screens, 2 × 2 below 1280 px'},{value:'2',label:'2 columns',detail:'Scanners, then chart and backtest'},{value:'4',label:'4 columns',detail:'All four cards in one row'}]}/></Popover>
 </View>;
}
