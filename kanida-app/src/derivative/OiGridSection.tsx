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
import {InfoDisclosure,Section,WidgetFrame,head,stateOf,tone,buildupColor,typeColor,
 type InfoGroup,type InfoLine} from './frame';
import {FuturesChartPanel} from './FuturesChartPanel';
import {DASH,GRID_LEGEND_TEXT,GRID_NOT_ENOUGH_MARKS,asOfText,axisTimes,buildupLabel,buildupTone,contractSummary,
 crore,customizeLabel,deltaAxis,deltaUnits,directionChip,directionTone,dteText,expiryText,floorsText,gridBasis,
 gridBlockRead,gridDirection,gridFlow,
 gridSlotDetail,gridSlotLabel,gridSlotNote,gridSlotTitle,gridSourceText,groupSummary,linePath,optionTone,price,
 rowKey,rulesToFilters,scaleDelta,scaleGridPrice,signedUnits,strike as strikeText,unusualQuery,
 volumeRatioShort,type FilterRule} from './logic';
import type {ChartTarget,ContractRow,GridSlot,OiGrid,Unusual} from './types';

/** Room for the K/L labels down the left of every small chart. */
// PLOT_H was set when a wall of prose sat under the grid. With that gone the block had ~100px of nothing
// under its second row, so the charts take it: the same ten readings, drawn tall enough to read.
const AXIS_W=34,PLOT_H=80,TIME_H=12,CELL_GAP=8;
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
export const GRID_GAP_TEXT='A 15-min reading with no snapshot is drawn as a gap on both lines; nothing here is interpolated.';
/** The price line is the SECOND series: thinner and dashed, so it reads as secondary without relying on colour. */
const PRICE_DASH='3 2',PRICE_WIDTH=1,DELTA_WIDTH=1.4;
/** The grid is 5 across when there is room for it; narrower panes wrap to 3 and then 2. Never fewer. */
export function gridColumns(width:number){return width<420?2:width<660?3:5;}

// --- one small chart -----------------------------------------------------------------------------------------
function DeltaCell({slot,width,selected,onPress}:{slot:GridSlot;width:number;selected:boolean;onPress:()=>void}){
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
  style={(st:any)=>[{width,height:TILE_H,padding:CELL_PAD,borderRadius:10,borderWidth:1,gap:ROW_GAP,
   borderColor:selected?C.green:C.line,backgroundColor:st.hovered||st.focused?C.dark:'transparent'}]}>
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
 onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;style?:any};
function DeltaOiGrid({underlying,read,target,onTarget,onExpand,expanded,onClose,style}:GridProps){
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
       selected={slot.instrument_token!=null&&target?.instrumentToken===slot.instrument_token}/>)}
     </View>
    </View>)}
   </View>
  </ScrollView>
 </WidgetFrame>;
}

// --- the screener down the left -------------------------------------------------------------------------------
type ScreenerProps={rules:FilterRule[];rulesLine:string;seq:number;target:ChartTarget|null;
 onTarget:(t:ChartTarget)=>void;onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;
 onFloors?:(text:string)=>void;style?:any};
function UnusualScreenerPanel({rules,rulesLine,seq,target,onTarget,onExpand,expanded,onClose,onFloors,
 style}:ScreenerProps){
 const read=useDerivativeRead<Unusual>(`/api/derivatives/unusual${unusualQuery(rulesToFilters(rules||[]))}`,seq);
 const body=read.data;
 const state=stateOf(read,'No contract clears the liquidity floors at this 15-min reading.');
 const required=body?.baseline_sessions_required??3;
 const groups=body?.rows||[];
 const count=groups.reduce((n,g)=>n+(g.strikes||[]).length,0);
 const pick=(row:ContractRow)=>onTarget({underlying:row.underlying,instrumentToken:row.instrument_token,
  label:row.tradingsymbol||row.underlying,
  detail:`${strikeText(row.strike)} ${row.instrument_type} · ${dteText(row.days_to_expiry)}`});
 // the floors this list applied go to the block header, where one copy stands for all three panels
 const floors=floorsText(body?.floors,body?.floors_text);
 React.useEffect(()=>{onFloors?.(floors)},[floors,onFloors]);
 return <WidgetFrame name="Unusual contracts" subtitle={(rules||[]).length?rulesLine:SCREENER_IDLE_SUB}
  body={body} state={state}
  onRefresh={read.reload} onExpand={onExpand}
  expanded={expanded} onClose={onClose}
  showsSignals={['premium_cr','volume_ratio','buildup_day','oi_change_day']}
  note={`${count} strike${count===1?'':'s'} over the floors`}
  emptyDetail="A contract is listed only when it clears all three floors, which are named in How to read this."
  inBlock style={style}>
  <ScrollView style={{flex:1}} contentContainerStyle={{paddingVertical:4}}>
   {groups.map(group=><View key={`g-${group.underlying}`}>
    <View style={{paddingHorizontal:10,paddingVertical:5,backgroundColor:C.dark,gap:1}}>
     <T numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:11,lineHeight:15}}>{group.underlying}</T>
     <T numberOfLines={2} style={{fontSize:9,lineHeight:13,color:C.muted}}>{groupSummary(group)}</T>
    </View>
    {(group.strikes||[]).map((row,i)=>{
     const selected=row.instrument_token!=null&&target?.instrumentToken===row.instrument_token;
     return <Pressable key={rowKey(row,`${group.underlying}-${i}`)} accessibilityRole="button"
      accessibilityState={{selected}} onPress={()=>pick(row)}
      accessibilityLabel={`${row.tradingsymbol||row.underlying}. ${contractSummary(row)}. ${crore(row.premium_cr)} traded. ${buildupLabel(row.buildup_day)} on the day. Volume ${volumeRatioShort(row,required)} of its own median.`}
      style={(st:any)=>[{paddingHorizontal:10,paddingVertical:6,borderBottomWidth:1,borderColor:C.line,gap:2,
       backgroundColor:selected?C.soft:st.hovered||st.focused?C.dark:'transparent'}]}>
      <View style={[s.row,{gap:6}]}>
       <T numberOfLines={1} style={{flex:1,fontFamily:'InterSemi',fontSize:11,lineHeight:15,
        color:typeColor(optionTone(row.instrument_type))}}>{contractSummary(row)}</T>
       <T style={{fontSize:11,lineHeight:15,fontVariant:['tabular-nums']}}>{crore(row.premium_cr)}</T>
      </View>
      <View style={[s.row,{gap:6}]}>
       <T numberOfLines={1} style={{flex:1,fontSize:9,lineHeight:13,
        color:buildupColor(buildupTone(row.buildup_day))}}>{buildupLabel(row.buildup_day)}</T>
       <T style={{fontSize:9,lineHeight:13,color:C.muted}}>vol {volumeRatioShort(row,required)}</T>
       <T style={{fontSize:9,lineHeight:13,color:tone(row.oi_change_day)}}>OI {signedUnits(row.oi_change_day)}</T>
      </View>
     </Pressable>;
    })}
   </View>)}
  </ScrollView>
 </WidgetFrame>;
}

// --- the block ------------------------------------------------------------------------------------------------
// Left to right, one block, all three panels the same height: the screener, the futures chart, the 2 × 5 grid.
// The screener is the narrow dense list it already was; the futures chart is a single tall panel spanning the
// FULL height of the block (as tall as both tile rows together, not one row); the grid keeps its five columns.
// The owner drew the chart roughly one grid-column wide; at that width a candle series is a smear, so it is
// given about the screener's width instead and the grid keeps the largest share of the row. The screener gets
// the extra tenth because its rows carry three numbers on a line and the chart's do not.
// Below index.tsx's GRID_BESIDE the three no longer fit without dropping the grid under five columns, so the
// block stacks into one column instead of shrinking the tiles — nothing here loses content silently.
// The futures panel's share was set when its header only had to fit a name; it now has to fit the CONTRACT
// without cutting it short, at the narrowest width the three still sit side by side. The screener gains a
// little for the same reason. The grid keeps its share untouched, so it still gets five tiles across.
export const SCREENER_FLEX=1.15,FUTURES_FLEX=1.25,GRID_FLEX=2.9;
/** `symbol` and `badge` are the TAB's, resolved once in index.tsx and handed down. This block used to resolve its
 *  own, which was right when it was the only block with a symbol and wrong the moment the session blocks arrived:
 *  two resolutions is two answers, and the owner asked for one. */
export type OiGridSectionProps={symbol:string;badge:string;expiry:string;seq:number;rules:FilterRule[];
 rulesLine:string;target:ChartTarget|null;onTarget:(t:ChartTarget)=>void;onCustomize:()=>void;stacked?:boolean;
 height:number;hidden?:Record<string,boolean>;onHide?:(key:string)=>void;expanded?:string;
 onExpand?:(key:string)=>void};
export function OiGridSection({symbol,badge,expiry,seq,rules,rulesLine,target,onTarget,onCustomize,stacked,height,
 hidden,onHide,expanded,onExpand}:OiGridSectionProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 // The panels hand their own definitions UP to the block, which shows them behind ONE control. A panel the
 // reader closed takes its group with it, so the panel never explains something that is not on screen.
 const [floors,setFloors]=useState('');
 const onFloors=useCallback((text:string)=>setFloors(v=>v===text?v:text),[]);
 const [chartInfo,setChartInfo]=useState<InfoLine[]>([]);
 const onChartInfo=useCallback((lines:InfoLine[])=>setChartInfo(v=>
  v.length===lines.length&&v.every((l,i)=>l.text===lines[i].text)?v:lines),[]);
 // The symbol is the TAB's, resolved once in index.tsx (logic.resolveTabSymbol) and handed to every block on the
 // page, this one included. The futures chart and all ten ΔOI tiles get that same answer, so a screener click
 // re-points the whole TAB together and no panel can be looking at a different underlying from the one beside it.
 // A symbol nobody chose still says it is a default: `badge` carries that, and it came down with the symbol.
 // The grid's read is made HERE, once, because the block header needs the same envelope: its as-of is the
 // block's one as-of line, and its served sentences are the block's one "How to read this" panel. Reading it
 // in two places would let the header and the tiles describe two different readings of the same symbol.
 const gridPath=symbol?`/api/derivatives/oi-grid?underlying=${encodeURIComponent(symbol)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const gridRead=useDerivativeRead<OiGrid>(gridPath,seq);
 const gridBody=gridRead.data;
 // All three panels are the same height when they sit side by side: the chart spans both tile rows.
 const screenerStyle=stacked?{height:Math.min(height,380)}:{flex:SCREENER_FLEX,minWidth:0,height};
 const futuresStyle=stacked?{height:Math.min(height,460)}:{flex:FUTURES_FLEX,minWidth:0,height};
 const gridStyle=stacked?{height}:{flex:GRID_FLEX,minWidth:0,height};
 const screener=!shows('oi_grid_screener')?null:<UnusualScreenerPanel key="oi_grid_screener" rules={rules}
  rulesLine={rulesLine} seq={seq} target={target} onTarget={onTarget} onFloors={onFloors}
  onExpand={onExpand?()=>onExpand('oi_grid_screener'):undefined} expanded={expanded==='oi_grid_screener'}
  onClose={onHide?()=>onHide('oi_grid_screener'):undefined} style={screenerStyle}/>;
 const futures=!shows('oi_grid_futures')?null:<FuturesChartPanel key="oi_grid_futures" underlying={symbol}
  seq={seq} onInfo={onChartInfo} onExpand={onExpand?()=>onExpand('oi_grid_futures'):undefined}
  expanded={expanded==='oi_grid_futures'} onClose={onHide?()=>onHide('oi_grid_futures'):undefined}
  style={futuresStyle}/>;
 const grid=!shows('oi_grid')?null:<DeltaOiGrid key="oi_grid" underlying={symbol} read={gridRead}
  target={target} onTarget={onTarget}
  onExpand={onExpand?()=>onExpand('oi_grid'):undefined} expanded={expanded==='oi_grid'}
  onClose={onHide?()=>onHide('oi_grid'):undefined} style={gridStyle}/>;
 if(!screener&&!futures&&!grid)return null;
 // EVERY definition this block works to, in one place, behind one control. Not one word of any of them is
 // changed by the move: the server's own sentences are preferred wherever it sends them, exactly as they were
 // when they sat under the panels, and the fallbacks are the same fallbacks.
 const groups:InfoGroup[]=[
  {heading:'ΔOI by strike',lines:!grid?[]:[
   gridBody?.delta_oi_text||'ΔOI is open interest added or removed since the previous close. Every line starts at 0 at the first 15-min reading of the day.',
   gridBody?.atm_text||'ATM is the listed strike nearest spot in the front expiry. ATM+n is n strikes above spot, ATM−n is n strikes below.',
   gridBody?.direction_text||'',
   GRID_LEGEND_TEXT,
   gridBody?.price_text||'The dashed line on each tile is that contract\'s own last traded price at the same 15-min readings, on its own scale.',
   TILE_READING_TEXT,
   gridBody?.flow_text||'Every opened contract has a buyer and a seller. These two lines read which side was paying up at each 15-min reading — they do not say what happens next.',
   gridBody?.block_text||'',
   gridBasis(gridBody),
   `${GRID_GAP_TEXT} ${gridSourceText(gridBody?.points_source)}`.trim()]},
  {heading:'Unusual contracts',lines:!screener?[]:[floors,SCREENER_FLOOR_TEXT,SCREENER_CLICK_TEXT]},
  {heading:'Futures chart',lines:futures?chartInfo:[]},
 ];
 // One as-of for the block, not one per panel: all three panels are pointed at the SAME symbol, and the grid's
 // envelope is the read of it. One symbol badge too, handed down from the tab, which is also where a defaulted
 // symbol is flagged as a default rather than the reader's own choice.
 return <Section title="ΔOI by strike, through the session"
  subtitle="One symbol across the block: its front futures contract, and the ten strikes at the money."
  asOf={asOfText(gridBody?.as_of)} badge={badge}
  info={<InfoDisclosure title="ΔOI by strike, through the session" groups={groups}/>}
  actions={<LinkText label={customizeLabel((rules||[]).length)}
   a11y={`Customize this block: ${(rules||[]).length} filters in force. Open the filter builder`}
   onPress={onCustomize}/>}
  stacked={stacked}>{screener}{futures}{grid}</Section>;
}
