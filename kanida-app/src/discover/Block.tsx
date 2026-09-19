// One Discover block (spec §3.1–3.2): header + [Scanner A][Scanner B][Chart][Backtest]. Generic over the catalog: block.kind picks the chart/backtest renderers; unknown kinds get a "coming soon" card.
import React,{useEffect,useMemo,useRef,useState} from 'react';
import {View,Pressable} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {Popover,MenuList,IconButton,readStore,writeStore} from '../layout';
import {useActiveSymbol} from '../activeSymbol';
import type {BlockDef,BlockKind,Catalog,LiveDetection,StrategyRow,StrategySummary} from '../strategies/types';
import {ScannerCard} from './ScannerCard';
import {ChartCard,openChart} from './ChartCard';
import {BacktestCard} from './BacktestCard';
import {EvidenceCard} from './EvidenceCard';
import {CARD_H,GRID_H,defaultSlots,resolveSlot,type GridMode} from './logic';
import {linkFromDetection,linkFromRow,type BlockLink,type Slot} from './link';
import type {DiscoverDeepLink} from './deeplink';
import {BLOCK_COLORS,ComingSoonCard} from './parts';
export type LinkedCardProps={link:BlockLink|null;style?:any;stale?:boolean;ageDays?:number|null;minTrades?:number;color?:string};
/** 4-across: scanners keep room for a 10-character symbol + the 3 numeric columns; chart and backtest take the remainder. */
const SCANNER_MIN=268;
type Renderer={chart:React.ComponentType<LinkedCardProps>;backtest:React.ComponentType<LinkedCardProps>};
/** Block kind → linked card renderers. Add quant/results/options renderers here when those blocks ship.
 * The stored-scan block (`chart`, keyed "chart") keeps the original backtest card; the four researched-pattern
 * blocks show the trader evidence card instead, since their evidence comes from the outcome engine. */
const RESEARCH_RENDERER:Renderer={chart:ChartCard,backtest:EvidenceCard};
export const BLOCK_RENDERERS:Partial<Record<BlockKind,Renderer>>={chart:{chart:ChartCard,backtest:BacktestCard},
 candlestick:RESEARCH_RENDERER,price_action:RESEARCH_RENDERER,harmonic:RESEARCH_RENDERER};
/** `chart` is used by BOTH the stored-scan block and the researched chart-pattern block, so the renderer is
 * chosen by the block's strategies, not by its kind alone. */
export function renderersFor(block:{key:string;kind:BlockKind;strategies:StrategySummary[]}):Renderer|undefined{
 if((block.strategies||[]).some(x=>(x as any).source_type==='research_pattern'))return RESEARCH_RENDERER;
 return BLOCK_RENDERERS[block.kind];
}
export function LinkedCard({kind,which,blockTitle,renderers,...props}:LinkedCardProps&{kind:BlockKind;which:'chart'|'backtest';blockTitle:string;renderers?:Renderer}){
 const R=(renderers||BLOCK_RENDERERS[kind])?.[which];
 if(!R)return <ComingSoonCard title={which==='chart'?'Chart':'Backtest'} detail={`${which==='chart'?'Charts':'Backtests'} for ${blockTitle} are coming soon.`} style={props.style}/>;
 return <R {...props}/>;
}
export type BlockProps={block:BlockDef&{strategies:StrategySummary[]};index:number;catalog:Catalog;mode:GridMode;deep:DiscoverDeepLink|null;refreshSeq:number;link:BlockLink|null;
 setLink:(block:string,link:BlockLink|null)=>void;onOpenSheet:(block:string)=>void;onLayoutY:(block:string,y:number)=>void};
type Stored={A?:string|null;B?:string|null};
export function Block({block,index,catalog,mode,deep,refreshSeq,link,setLink,onOpenSheet,onLayoutY}:BlockProps){
 const {setActive}=useActiveSymbol();
 const strategies=useMemo(()=>block.strategies.filter(x=>x.enabled!==false).sort((a,b)=>a.order-b.order),[block.strategies]);
 const defaults=useMemo(()=>defaultSlots(strategies),[strategies]);
 const slotsId=`slots.${block.key}`,blockId=`block.${block.key}`;
 const [stored,setStored]=useState<Stored>(()=>readStore<Stored>('discover',slotsId));
 // A stored block while the scanner runs the researched set is not rendered at all (BACKLOG item 2.6). The
 // server already leaves it out of the catalog; this guards a response from an older server that still sends it.
 const superseded=!!block.superseded;
 const [collapsed,setCollapsed]=useState(()=>!!readStore<{collapsed:boolean}>('discover',blockId).collapsed);
 const [resetSeq,setResetSeq]=useState(0),[menuOpen,setMenuOpen]=useState(false);const menuRef=useRef<any>(null);
 const save=(next:Stored)=>{writeStore('discover',slotsId,next);setStored(next)};
 const keyFor=(slot:Slot)=>resolveSlot(stored[slot],defaults[slot],strategies);
 const byKey=(k:string|null)=>k?strategies.find(x=>x.key===k)||null:null;
 const choose=(slot:Slot,key:string|null)=>{save({...stored,[slot]:key});if(key===null&&link?.slot===slot)setLink(block.key,null)};
 const toggle=()=>setCollapsed(v=>{writeStore('discover',blockId,{collapsed:!v});return !v});
 const reset=()=>{save({});setLink(block.key,null);setResetSeq(x=>x+1);if(collapsed)toggle()};
 // Deep link (page passes it only to the target block): set strategies once per link signature.
 const applied=useRef('');
 useEffect(()=>{if(!deep||applied.current===deep.sig||!strategies.length)return;applied.current=deep.sig;const next:Stored={...stored};if(deep.a&&byKey(deep.a))next.A=deep.a;if(deep.bb&&byKey(deep.bb))next.B=deep.bb;save(next);if(collapsed)toggle()},[deep?.sig,strategies]);
 const select=(slot:Slot)=>(row:StrategyRow,st:StrategySummary)=>{setLink(block.key,linkFromRow(slot,st,row));setActive({symbol:row.symbol,timeframe:row.timeframe,matchId:row.match_id,side:row.side,source:'discover'});if(mode==='stack')onOpenSheet(block.key)};
 const openFull=(slot:Slot)=>(row:StrategyRow,st:StrategySummary)=>openChart(linkFromRow(slot,st,row));
 // A LIVE detection links by its own detection id, so the chart draws that episode and the evidence card is
 // gated on that detection's identity rather than on the pattern name.
 const selectDetection=(slot:Slot)=>(d:LiveDetection,st:StrategySummary)=>{setLink(block.key,linkFromDetection(slot,st,d));setActive({symbol:d.symbol,timeframe:d.timeframe,matchId:d.match_id,side:d.side,source:'discover'});if(mode==='stack')onOpenSheet(block.key)};
 const openDetection=(slot:Slot)=>(d:LiveDetection,st:StrategySummary)=>openChart(linkFromDetection(slot,st,d));
 const color=BLOCK_COLORS[index%BLOCK_COLORS.length],common={stale:catalog.stale,ageDays:catalog.age_days};
 const minTrades=byKey(link?.strategyKey??null)?.min_trades??10;
 const scanner=(slot:Slot,style:any)=>{const k=keyFor(slot);return <ScannerCard key={slot} blockKey={block.key} slot={slot} strategy={byKey(k)} strategies={strategies} universeKey={catalog.universe?.key||'nifty500'} universeLabel={catalog.universe?.label||'NIFTY 500'} link={link} refreshSeq={refreshSeq} resetSeq={resetSeq}
  autoSelect={deep?.symbol&&deep.sel===slot?{symbol:deep.symbol,sig:deep.sig}:null} unavailable={(block as any).unavailable||null} onChoose={key=>choose(slot,key)} onSelect={select(slot)} onOpenFull={openFull(slot)}
  onSelectDetection={selectDetection(slot)} onOpenDetection={openDetection(slot)} selectedDetectionId={link?.slot===slot?link?.detectionId??null:null} style={style}/>};
 const renderers=useMemo(()=>renderersFor(block),[block]);
 const linked=(which:'chart'|'backtest',style:any)=><LinkedCard key={which} kind={block.kind} renderers={renderers} which={which} blockTitle={block.title} link={link} style={style} minTrades={minTrades} color={color} {...common}/>;
 if(superseded)return null;
 let grid:React.ReactNode;
 if(!strategies.length)grid=<ComingSoonCard title={block.title} detail="Strategies for this block are coming soon." style={{height:160}}/>;
 else if(mode==='four')grid=<View style={[s.row,{alignItems:'stretch',gap:12}]}>{scanner('A',{flex:.95,minWidth:SCANNER_MIN,height:GRID_H})}{scanner('B',{flex:.95,minWidth:SCANNER_MIN,height:GRID_H})}{linked('chart',{flex:1.45,height:GRID_H})}{linked('backtest',{flex:1.25,height:GRID_H})}</View>;
 else if(mode==='two')grid=<View style={{gap:12}}><View style={[s.row,{alignItems:'stretch',gap:12}]}>{scanner('A',{flex:1,height:GRID_H})}{scanner('B',{flex:1,height:GRID_H})}</View><View style={[s.row,{alignItems:'stretch',gap:12}]}>{linked('chart',{flex:1,height:GRID_H})}{linked('backtest',{flex:1,height:GRID_H})}</View></View>;
 else grid=<View style={{gap:12}}>{scanner('A',{height:CARD_H})}{scanner('B',{height:CARD_H})}
  {link?<Pressable accessibilityRole="button" accessibilityLabel={`Show chart and backtest for ${link.symbol}`} onPress={()=>onOpenSheet(block.key)} style={(st:any)=>[s.row,{gap:8,minHeight:44,paddingHorizontal:12,borderRadius:10,borderWidth:1,borderColor:C.green,backgroundColor:st.pressed?C.soft:'transparent'}]}><Icon name="bar-chart-2" size={15} color={C.green}/><T style={{fontSize:13,fontFamily:'InterSemi',color:C.green}}>Chart & backtest · {link.symbol}</T></Pressable>
   :<T style={{fontSize:12,color:C.muted}}>Tap a stock to open its chart and backtest.</T>}</View>;
 return <View onLayout={e=>onLayoutY(block.key,e.nativeEvent.layout.y)} role="region" aria-label={block.title} style={{gap:10}}>
  <View style={[s.row,{gap:10,minHeight:44}]}>
   <View aria-hidden style={{width:10,height:10,borderRadius:5,backgroundColor:color}}/>
   <View style={{flex:1,minWidth:0}}><T role="heading" aria-level={2} numberOfLines={1} style={{fontFamily:'ManropeBold',fontSize:17,lineHeight:23}}>{block.title}</T>{!!block.description&&<T numberOfLines={mode==='stack'?1:2} style={{fontSize:12,lineHeight:17,color:C.muted}}>{block.description}</T>}</View>
   <IconButton icon={collapsed?'chevron-down':'chevron-up'} label={`${collapsed?'Expand':'Collapse'} ${block.title}`} tooltip={collapsed?'Expand':'Collapse'} expanded={!collapsed} onPress={toggle}/>
   <IconButton ref={menuRef} icon="more-vertical" label={`${block.title} options`} tooltip="Options" haspopup="menu" expanded={menuOpen} onPress={()=>setMenuOpen(true)}/>
  </View>
  {!collapsed&&grid}
  <Popover open={menuOpen} onClose={()=>setMenuOpen(false)} anchor={menuRef} placement="bottom-end" label={`${block.title} options`} role="none" width={220}>
   <MenuList label={`${block.title} options`} onClose={()=>setMenuOpen(false)} items={[{label:'Reset block',icon:'rotate-ccw',onPress:reset},{label:collapsed?'Expand':'Collapse',icon:collapsed?'chevron-down':'chevron-up',onPress:toggle}]}/>
  </Popover>
 </View>;
}
