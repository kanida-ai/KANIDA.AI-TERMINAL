// The owner's ΔOI block: one full-width section with the unusual-derivative screener down the left and a 2 × 5 grid
// of ten small ΔOI charts on the right — ATM CE and the four strikes above it on the calls row, ATM PE and the four
// below it on the puts row.
//
// Every tile draws TWO lines on two scales: ΔOI (solid, the row's own colour) and that contract's own last traded
// price (dashed and thinner, so it is still the secondary line for a reader who cannot use colour). A rupee premium
// and a count of contracts share no units, which is why they never share a scale — the same rule, and the same
// wording, as ChartTile.
//
// The definitions this block works to are NOT deleted and they are NOT pasted under the numbers. They live
// behind ONE control in the block header — "How to read this" — closed on arrival, full text unclamped
// inside, Escape closing it and focus returning to the control (frame.InfoDisclosure, the same pattern
// src/workspace/ChartCentre.tsx uses for its drawing notes). What stays permanently on screen is only what
// the reader needs to trust the numbers: the as-of time, once for the whole block rather than once per panel,
// and the symbol the block resolved. The definitions themselves, word for word:
//   * ΔOI is open interest added or removed SINCE THE PREVIOUS CLOSE. Every line starts at 0 at the day's first reading.
//   * ATM is the listed strike nearest spot in the front expiry; ATM+n is n strikes above, ATM−n n strikes below.
//   * Direction is the latest ΔOI against the reading four back (one hour); flat inside a 5% band of that contract's
//     own largest ΔOI today; under two readings it is "no baseline" and gets no chip at all.
//   * Under the chip, price and ΔOI read together over that same hour: what is happening, and what it means. It is
//     a reading of two numbers, derived in the browser from the very points the tile drew, so a tile can never
//     disagree with its own chart. Nothing in it is bullish, bearish, a level, or a forecast.
//
// Everything drawn here was captured (§5). A reading with no snapshot is a gap in BOTH lines, never an interpolated
// point; a strike the exchange does not list keeps its slot and says so; and the wording is the owner's own —
// BUILDING, FLAT, UNWINDING — with nothing anywhere about what the book is going to do next.
import React,{useCallback,useMemo,useState} from 'react';
import {View,ScrollView,Pressable} from 'react-native';
import Svg,{Path,Line,Circle,Text as SvgText} from 'react-native-svg';
import {C,T,s} from '../ui';
import {useDerivativeRead,type Read} from './useDerivatives';
import {LinkText} from '../discover/parts';
import {Block,InfoDisclosure,WidgetFrame,head,stateOf,tone,buildupColor,typeColor,
 type HeadlineProps,type InfoGroup,type InfoLine,type PaneStyle} from './frame';
import {FuturesChartPanel} from './FuturesChartPanel';
import {DASH,GRID_LEGEND_TEXT,GRID_NOT_ENOUGH_MARKS,asOfText,axisTimes,customizeLabel,deltaAxis,deltaUnits,
 directionChip,directionTone,dteText,expiryText,gridBasis,gridBlockRead,gridDirection,gridFlow,
 gridSlotDetail,gridSlotLabel,headlineAgainst,gridSlotNote,gridSlotTitle,gridSourceText,linePath,optionTone,price,
 scaleDelta,scaleGridPrice,strike as strikeText,type FilterRule} from './logic';
import type {ChartTarget,GridSlot,OiGrid} from './types';

/** Room for the K/L labels down the left of every small chart. */
// PLOT_H was set when a wall of prose sat under the grid. With that gone the block had ~100px of nothing
// under its second row, so the charts take it: the same ten readings, drawn tall enough to read.
const AXIS_W=34,PLOT_H=64,TIME_H=12,CELL_GAP=8;
// ONE rhythm for both rows. Every zone of a tile is given a fixed height, so a call tile and a put tile are
// exactly the same height whatever their labels happen to say, every tile in a row shares a baseline, and the
// numbers line up across the grid for an eye scanning it. Nothing is clamped that used to be whole: the
// reading under the chip keeps its two lines, and the space is simply always reserved for them.
const TITLE_H=16,DETAIL_H=13,CHIP_H=15,WHAT_H=14,MEANING_H=26,CELL_PAD=8,ROW_GAP=4,BORDER=2;
const CHART_H=PLOT_H+TIME_H;
export const TILE_H=CELL_PAD*2+BORDER+ROW_GAP*5+TITLE_H+DETAIL_H+CHART_H+CHIP_H+WHAT_H+MEANING_H;
// What the two lines under each tile ARE. It used to be implied by their presence; it is said once, in the
// one panel that explains the block, rather than seven times down the page.
export const TILE_READING_TEXT='The two lines under each tile read that contract\'s premium and its ΔOI together over that same hour: what is happening, and what that is a reading of. Both are worked out in the browser from the very points the tile drew, so a tile can never disagree with its own chart.';
export const SCREENER_FLOOR_TEXT='A contract is listed only when it clears all three floors above.';
// With no filter in force the screener's subtitle would be a long sentence in the narrowest panel of the
// block. This is the same fact in a phrase that fits; the sentence itself is in How to read this.
export const SCREENER_IDLE_SUB='Over the floors';
export const SCREENER_CLICK_TEXT='Clicking a row points the futures chart and the ten tiles at that contract.';
export const GRID_HIGHLIGHT_TEXT='Pointing at a tile lights the same strike wherever else it is on this tab — on the option chain and in the volatility-by-strike list. It highlights; it changes nothing.';
export const GRID_GAP_TEXT='A 15-min reading with no snapshot is drawn as a gap on both lines; nothing here is interpolated.';
/** The price line is the SECOND series: thinner and dashed, so it reads as secondary without relying on colour. */
const PRICE_DASH='3 2',PRICE_WIDTH=1,DELTA_WIDTH=1.4;
/** The grid is 5 across when there is room for it; narrower panes wrap to 3 and then 2. Never fewer. */
export function gridColumns(width:number){return width<420?2:width<660?3:5;}

// --- one small chart -----------------------------------------------------------------------------------------
function DeltaCell({slot,width,selected,lit,onPress,onHighlight}:{slot:GridSlot;width:number;selected:boolean;
 lit?:boolean;onPress:()=>void;onHighlight?:(strike:number|null)=>void}){
 // The chip AND the two sentences are derived from the very points this cell draws, so a tile can never
 // disagree with its own chart. The server computes the same reading and serves it; neither is trusted over
 // the other, because both are the same rule over the same points.
 const direction=gridDirection(slot.points);
 const chip=directionChip(direction),chipTone=directionTone(direction);
 const flow=gridFlow(slot.option_type,slot.points);
 const note=gridSlotNote(slot);
 const colour=typeColor(optionTone(slot.option_type));
 // the cell's 8px padding and 1px border either side, then the K/L gutter: the line must end inside the box
 const plotW=Math.max(0,width-AXIS_W-22);
 const scaled=scaleDelta(slot.points,plotW,PLOT_H);
 // its OWN scale: ΔOI is measured from a baseline and a premium is not, so they never share one
 const priceLine=scaleGridPrice(slot.points,plotW,PLOT_H);
 const axis=deltaAxis(scaled),times=axisTimes(slot.points,3);
 const last=scaled?[...scaled.points].reverse().find(Boolean):null;
 const lastPrice=[...(slot.points||[])].reverse().find(p=>typeof p?.price==='number');
 const height=CHART_H;
 return <Pressable accessibilityRole="button" accessibilityLabel={gridSlotLabel(slot,direction)}
  accessibilityState={{selected}} disabled={!slot.present} onPress={onPress}
  onHoverIn={()=>onHighlight?.(slot.strike==null?null:Number(slot.strike))}
  onHoverOut={()=>onHighlight?.(null)}
  onFocus={()=>onHighlight?.(slot.strike==null?null:Number(slot.strike))}
  onBlur={()=>onHighlight?.(null)}
  style={(st:any)=>[{width,height:TILE_H,padding:CELL_PAD,borderRadius:10,gap:ROW_GAP,
   // selected is the reader's CHOICE and wins; lit is the same strike under the pointer somewhere else on
   // the tab; focused is the keyboard, which must be as visible as the mouse.
   borderWidth:selected||st.focused?2:1,
   borderColor:selected||st.focused?C.green:lit?C.mint:C.line,
   backgroundColor:selected?C.soft:lit||st.hovered||st.focused?C.dark:'transparent'}]}>
  <T numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:12,lineHeight:TITLE_H,height:TITLE_H,
   color:colour}}>{gridSlotTitle(slot)}</T>
  <T numberOfLines={1} style={{fontSize:9,lineHeight:DETAIL_H,height:DETAIL_H,color:C.muted}}>{gridSlotDetail(slot)}</T>
  <View style={{height}}>
   {note?<View style={{flex:1,justifyContent:'center'}}>
     <T style={{fontSize:9,lineHeight:13,color:C.muted}}>{note}</T></View>
   :plotW>0&&<Svg width={width-18} height={height}>
    {axis.map(t=><SvgText key={`y${t.v}`} x={0} y={Math.min(height-TIME_H,Math.max(8,t.y+3))} fill={C.muted}
     fontSize={8}>{deltaUnits(t.v)}</SvgText>)}
    {!!scaled&&<Line x1={AXIS_W} x2={AXIS_W+plotW} y1={scaled.zero} y2={scaled.zero} stroke={C.line}
     strokeWidth={1} strokeDasharray="2 3"/>}
    {/* the premium, on its own scale: dashed and thinner, so it stays the secondary line without a colour cue */}
    <Path d={linePath(priceLine)} stroke={C.ink} strokeOpacity={0.7} strokeWidth={PRICE_WIDTH}
     strokeDasharray={PRICE_DASH} fill="none" transform={`translate(${AXIS_W},0)`}/>
    <Path d={linePath(scaled)} stroke={colour} strokeWidth={DELTA_WIDTH} fill="none" transform={`translate(${AXIS_W},0)`}/>
    {!!last&&<Circle cx={AXIS_W+last.x} cy={last.y} r={2.4} fill={colour}/>}
    {times.map(t=>{
     const step=slot.points.length>1?plotW/(slot.points.length-1):0;
     return <SvgText key={`x${t.i}`} x={Math.min(width-40,AXIS_W+t.i*step-10)} y={height-2} fill={C.muted}
      fontSize={8}>{t.label}</SvgText>;
    })}
   </Svg>}
  </View>
  {/* the chip on the left and the premium hard right, on every tile, so a row reads as a column of numbers */}
  <View style={[s.row,{gap:6,height:CHIP_H}]}>
   {chip?<View style={{paddingHorizontal:5,paddingVertical:1,borderRadius:4,borderWidth:1,
     borderColor:chipTone==='flat'?C.line:buildupColor(chipTone)}}>
     <T style={{fontSize:9,lineHeight:13,fontFamily:'InterSemi',
      color:chipTone==='flat'?C.muted:buildupColor(chipTone)}}>{chip}</T></View>
   :<T style={{fontSize:9,lineHeight:13,color:C.muted}}>no baseline</T>}
   <View style={{flex:1}}/>
   <T numberOfLines={1} style={{fontSize:9,lineHeight:13,color:C.muted,fontVariant:['tabular-nums']}}>
    {price(lastPrice?.price)}</T>
  </View>
  {/* plain text, never a coloured chip: this is a description of two numbers, not a signal. Both lines keep
      a box of their own whether or not there is anything to put in it, so the two rows stay in step. */}
  <T numberOfLines={1} style={{fontSize:9,lineHeight:13,height:WHAT_H,fontFamily:'InterMedium'}}>{flow.what_label}</T>
  <View style={{height:MEANING_H}}>
   <T numberOfLines={2} style={{fontSize:9,lineHeight:13,color:C.muted}}>{flow.meaning}</T></View>
 </Pressable>;
}

// --- the grid widget -----------------------------------------------------------------------------------------
// The read is NOT made here. The block makes it once (it needs the same envelope for the one as-of line and
// the one "How to read this" panel in its header) and hands it down, so the block and the grid can never be
// looking at two different readings of the same symbol.
type GridProps={underlying:string;read:Read<OiGrid>;target:ChartTarget|null;onTarget:(t:ChartTarget)=>void;
 highlight?:number|null;onHighlight?:(strike:number|null)=>void;
 onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;style?:any};
function DeltaOiGrid({underlying,read,target,onTarget,highlight,onHighlight,onExpand,expanded,onClose,
 style}:GridProps){
 const [width,setWidth]=useState(0);
 const body=read.data;
 const state=underlying?stateOf(read,body?.not_enough_marks||GRID_NOT_ENOUGH_MARKS)
  :{phase:'empty' as const,text:'Choose a symbol in Customize, or click a row in the screener, to see the strikes at the money.'};
 const rows=body?.rows||[];
 // A layout split, not a filter: every slot the server sent lands in exactly one band, in the order it arrived.
 const bands=useMemo(()=>{
  const out=[{key:'calls',label:'Calls — at the money and above',slots:[] as GridSlot[]},
   {key:'puts',label:'Puts — at the money and below',slots:[] as GridSlot[]}];
  for(const slot of rows)(slot.row==='puts'?out[1]:out[0]).slots.push(slot);
  return out;
 },[rows]);
 const columns=gridColumns(width||660);
 const cell=Math.max(120,Math.floor(((width||660)-CELL_GAP*(columns-1))/columns));
 const pick=(slot:GridSlot)=>{
  if(!slot.present)return;
  onTarget({underlying:body?.underlying||underlying,instrumentToken:slot.instrument_token,
   label:slot.tradingsymbol||gridSlotTitle(slot),
   detail:`${gridSlotTitle(slot)} · ${slot.label}${body?.days_to_expiry!=null?` · ${dteText(body.days_to_expiry)}`:''}`});
 };
 // The expiry is written the way the futures panel beside it writes one - "22 Sep 2026", not the raw
 // stored date. Same date, same formatter the rest of the tab already uses; nothing about it changed.
 const expiryLabel=body?.expiry?expiryText(body.expiry,null):'';
 const subtitle=underlying
  ?`${underlying}${expiryLabel&&expiryLabel!==DASH?` · ${expiryLabel}`:''}${body?.atm_strike!=null?` · ATM ${strikeText(body.atm_strike)}`:''}${body?.spot!=null?` · spot ${price(body.spot)}`:''}`
  :'';
 // Derived from the very slots on screen - it aggregates the ten tiles and asks the pilot for nothing extra.
 const blockRead=gridBlockRead(rows);
 // The definitions moved to the block's one info panel. What is left under the panel is the one line the ten
 // tiles above actually earn, and nothing at all when they do not earn it.
 return <WidgetFrame name="Strikes at the money" subtitle={subtitle} body={body} state={state}
  onRefresh={read.reload} onExpand={onExpand} expanded={expanded}
  onClose={onClose} note={blockRead||undefined} inBlock style={style}
  emptyDetail={body?.empty_detail||'Each line needs two 15-min readings before it is a line rather than a dot.'}>
  <ScrollView style={{flex:1}} contentContainerStyle={{padding:10,gap:10}}>
   <View onLayout={e=>{const w=Math.round(e.nativeEvent.layout.width);setWidth(v=>Math.abs(v-w)<3?v:w)}}
    style={{gap:10}}>
    {bands.map(band=><View key={band.key} style={{gap:6}}>
     <T style={head}>{band.label}</T>
     {/* every tile is TILE_H tall, so the calls row and the puts row are the same height to the pixel */}
     <View style={[s.row,{flexWrap:'wrap',alignItems:'flex-start',gap:CELL_GAP}]}>
      {band.slots.map(slot=><DeltaCell key={slot.slot} slot={slot} width={cell} onPress={()=>pick(slot)}
       onHighlight={onHighlight}
       lit={highlight!=null&&slot.strike!=null&&Math.abs(Number(slot.strike)-highlight)<0.5}
       selected={slot.instrument_token!=null&&target?.instrumentToken===slot.instrument_token}/>)}
     </View>
    </View>)}
   </View>
  </ScrollView>
 </WidgetFrame>;
}

// --- the block ------------------------------------------------------------------------------------------------
// TWO panels, on the tab's one template: the front futures chart of the symbol on the left, the 2 x 5 grid of
// delta-OI tiles on the right. It used to be THREE - a screener down the left, then the chart, then the grid -
// and that screener was one of the seven copies of one list the owner objected to. There is now exactly ONE
// screener on this tab, pinned at the top, and it drives this block like every other.
//
// The GRID IS THE CONTENT PANEL. That is the question this block had to answer to fit the skeleton, and the
// answer is yes: ten small charts of one symbol's strikes at the money are what this block is ABOUT, so they
// take the content panel and the futures chart takes the chart panel. The template gives the content panel the
// larger share, which is what keeps the grid five tiles across.
/** `symbol` and `badge` are the TAB's, resolved once in index.tsx and handed down. This block used to resolve its
 *  own, which was right when it was the only block with a symbol and wrong the moment the session blocks arrived:
 *  two resolutions is two answers, and the owner asked for one. */
export type OiGridSectionProps={symbol:string;badge:string;linked?:boolean;expiry:string;seq:number;
 /** The tab's ONE ΔOI-grid read, made in index.tsx and handed to every panel built on it.
  *
  *  It used to be made here, and then the signal table beside the screener needed the SAME ten contracts and
  *  made its own - two fetches of one payload, and two answers that could drift apart at a refresh. One read,
  *  one envelope, one as-of: the grid and the signal table are now built on the very same rows. */
 read:Read<OiGrid>;
 rules:FilterRule[];target:ChartTarget|null;onTarget:(t:ChartTarget)=>void;onCustomize:()=>void;
 stacked?:boolean;height?:number;hidden?:Record<string,boolean>;onHide?:(key:string)=>void;expanded?:string;
 onExpand?:(key:string)=>void;
 /** The strike the pointer is on anywhere on the tab. Lighting it here is what links this grid to the chain
  *  and to the volatility list: three panels about the same strikes that used to share no visual cue at all. */
 highlight?:number|null;onHighlight?:(strike:number|null)=>void};
export function OiGridSection({symbol,badge,linked,expiry,seq,read,rules,target,onTarget,onCustomize,stacked,height,
 hidden,onHide,expanded,onExpand,highlight,onHighlight}:OiGridSectionProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 // The futures panel hands its own definitions UP to the block, which shows them behind ONE control.
 const [chartInfo,setChartInfo]=useState<InfoLine[]>([]);
 const onChartInfo=useCallback((lines:InfoLine[])=>setChartInfo(v=>
  v.length===lines.length&&v.every((l,i)=>l.text===lines[i].text)?v:lines),[]);
 // The symbol is the TAB's, resolved once in index.tsx (logic.resolveTabSymbol) and handed to every block on the
 // page, this one included. The futures chart and all ten delta-OI tiles get that same answer, so a screener
 // click re-points the whole TAB together and no panel can be looking at a different underlying from its
 // neighbour. A symbol nobody chose still says it is a default: `badge` carries that, and `linked` is false.
 // The read is the TAB's, made once in index.tsx and handed down: the signal table beside the screener is
 // built on these very same ten contracts, so neither can drift from the other at a refresh.
 const gridRead=read;
 const gridBody=gridRead.data;
 const futures=(style:PaneStyle)=>!shows('oi_grid_futures')?null:<FuturesChartPanel key="oi_grid_futures"
  underlying={symbol} seq={seq} onInfo={onChartInfo}
  onExpand={onExpand?()=>onExpand('oi_grid_futures'):undefined}
  expanded={expanded==='oi_grid_futures'} onClose={onHide?()=>onHide('oi_grid_futures'):undefined}
  style={style}/>;
 const grid=(style:PaneStyle)=>!shows('oi_grid')?null:<DeltaOiGrid key="oi_grid" underlying={symbol}
  read={gridRead} target={target} onTarget={onTarget} highlight={highlight} onHighlight={onHighlight}
  onExpand={onExpand?()=>onExpand('oi_grid'):undefined} expanded={expanded==='oi_grid'}
  onClose={onHide?()=>onHide('oi_grid'):undefined} style={style}/>;
 // EVERY definition this block works to, in one place, behind one control. Not one word of any of them is
 // changed: the server's own sentences are preferred wherever it sends them, exactly as they were when they
 // sat under the panels, and the fallbacks are the same fallbacks.
 const groups:InfoGroup[]=[
  {heading:'delta-OI by strike',lines:!shows('oi_grid')?[]:[
   gridBody?.delta_oi_text||'ΔOI is open interest added or removed since the previous close. Every line starts at 0 at the first 15-min reading of the day.',
   gridBody?.atm_text||'ATM is the listed strike nearest spot in the front expiry. ATM+n is n strikes above spot, ATM−n is n strikes below.',
   gridBody?.direction_text||'',
   GRID_LEGEND_TEXT,
   gridBody?.price_text||'The dashed line on each tile is that contract\'s own last traded price at the same 15-min readings, on its own scale.',
   TILE_READING_TEXT,
   gridBody?.flow_text||'Every opened contract has a buyer and a seller. These two lines read which side was paying up at each 15-min reading — they do not say what happens next.',
   gridBody?.block_text||'',
   gridBasis(gridBody),
   (GRID_GAP_TEXT+' '+gridSourceText(gridBody?.points_source)).trim(),
   GRID_HIGHLIGHT_TEXT]},
  {heading:'Futures chart',lines:shows('oi_grid_futures')?chartInfo:[]},
 ];
 // The block's one headline: the strike the ten tiles are built around, and the spot it is nearest to.
 const headline:HeadlineProps=symbol
  ?{label:'At-the-money strike',value:strikeText(gridBody?.atm_strike),
    against:headlineAgainst(gridBody?.spot!=null?'nearest the captured spot '+price(gridBody.spot):'',
     gridBody?.expiry?expiryText(gridBody.expiry,gridBody?.days_to_expiry??null):''),
    reason:gridBody?.not_enough_marks||GRID_NOT_ENOUGH_MARKS}
  :{label:'At-the-money strike',value:DASH,
    reason:'Click any row on the screener above, or choose a symbol in Customize, to point this block at one.'};
 return <Block title="ΔOI by strike, through the session"
  subtitle="One symbol across the block: its front futures contract, and the ten strikes at the money."
  asOf={asOfText(gridBody?.as_of)} badge={badge} linked={linked} headline={headline}
  info={<InfoDisclosure title="ΔOI by strike, through the session" groups={groups}/>}
  actions={<LinkText label={customizeLabel((rules||[]).length)}
   a11y={'Customize this block: '+(rules||[]).length+' filters in force. Open the filter builder'}
   onPress={onCustomize}/>}
  stacked={stacked} height={height} chart={futures} content={grid}/>;
}
