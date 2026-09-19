// The owner's ΔOI block: one full-width section with the unusual-derivative screener down the left and a 2 × 5 grid
// of ten small ΔOI charts on the right — ATM CE and the four strikes above it on the calls row, ATM PE and the four
// below it on the puts row.
//
// Every tile draws TWO lines on two scales: ΔOI (solid, the row's own colour) and that contract's own last traded
// price (dashed and thinner, so it is still the secondary line for a reader who cannot use colour). A rupee premium
// and a count of contracts share no units, which is why they never share a scale — the same rule, and the same
// wording, as ChartTile.
//
// The definitions this block works to are printed on it, not implied:
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
import React,{useMemo,useState} from 'react';
import {View,ScrollView,Pressable} from 'react-native';
import Svg,{Path,Line,Circle,Text as SvgText} from 'react-native-svg';
import {C,T,s} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {Section,WidgetFrame,stateOf,tone,buildupColor,typeColor} from './frame';
import {FuturesChartPanel} from './FuturesChartPanel';
import {GRID_LEGEND_TEXT,GRID_NOT_ENOUGH_MARKS,axisTimes,buildupLabel,buildupTone,contractSummary,crore,deltaAxis,
 deltaUnits,directionChip,directionTone,dteText,gridBasis,gridBlockRead,gridDirection,gridFlow,gridSlotDetail,
 gridSlotLabel,gridSlotNote,gridSlotTitle,gridSourceText,groupSummary,linePath,optionTone,price,resolveBlockSymbol,
 rowKey,rulesToFilters,scaleDelta,scaleGridPrice,signedUnits,strike as strikeText,unusualQuery,volumeRatioShort,
 type FilterRule} from './logic';
import type {ChartTarget,ContractRow,GridSlot,OiGrid,Unusual} from './types';

/** Room for the K/L labels down the left of every small chart. */
const AXIS_W=34,PLOT_H=54,TIME_H=12,CELL_GAP=8;
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
 const height=PLOT_H+TIME_H;
 return <Pressable accessibilityRole="button" accessibilityLabel={gridSlotLabel(slot,direction)}
  accessibilityState={{selected}} disabled={!slot.present} onPress={onPress}
  style={(st:any)=>[{width,padding:8,borderRadius:10,borderWidth:1,gap:4,
   borderColor:selected?C.green:C.line,backgroundColor:st.hovered||st.focused?C.dark:'transparent'}]}>
  <T numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:12,lineHeight:16,color:colour}}>{gridSlotTitle(slot)}</T>
  <T numberOfLines={1} style={{fontSize:9,lineHeight:13,color:C.muted}}>{gridSlotDetail(slot)}</T>
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
  <View style={[s.row,{gap:6}]}>
   {chip?<View style={{paddingHorizontal:5,paddingVertical:1,borderRadius:4,borderWidth:1,
     borderColor:chipTone==='flat'?C.line:buildupColor(chipTone)}}>
     <T style={{fontSize:9,lineHeight:13,fontFamily:'InterSemi',
      color:chipTone==='flat'?C.muted:buildupColor(chipTone)}}>{chip}</T></View>
   :<T style={{fontSize:9,lineHeight:13,color:C.muted}}>no baseline</T>}
   <View style={{flex:1}}/>
   <T numberOfLines={1} style={{fontSize:9,lineHeight:13,color:C.muted,fontVariant:['tabular-nums']}}>
    {price(lastPrice?.price)}</T>
  </View>
  {/* plain text, never a coloured chip: this is a description of two numbers, not a signal */}
  <T numberOfLines={2} style={{fontSize:9,lineHeight:13,fontFamily:'InterMedium'}}>{flow.what_label}</T>
  {!!flow.meaning&&<T numberOfLines={2} style={{fontSize:9,lineHeight:13,color:C.muted}}>{flow.meaning}</T>}
 </Pressable>;
}

// --- the grid widget -----------------------------------------------------------------------------------------
type GridProps={underlying:string;expiry:string;seq:number;target:ChartTarget|null;onTarget:(t:ChartTarget)=>void;
 defaulted?:boolean;defaultLabel?:string;filterCount?:number;onCustomize?:()=>void;onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;
 style?:any};
function DeltaOiGrid({underlying,expiry,seq,target,onTarget,defaulted,defaultLabel,filterCount,onCustomize,
 onExpand,expanded,onClose,style}:GridProps){
 const [width,setWidth]=useState(0);
 const path=underlying?`/api/derivatives/oi-grid?underlying=${encodeURIComponent(underlying)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<OiGrid>(path,seq);
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
 const subtitle=underlying
  ?`${defaulted&&defaultLabel?`${defaultLabel}: `:''}${underlying}${body?.expiry?` · ${body.expiry}`:''}${body?.atm_strike!=null?` · ATM ${strikeText(body.atm_strike)}`:''}${body?.spot!=null?` · spot ${price(body.spot)}`:''}`
  :'';
 // Derived from the very slots on screen - it aggregates the ten tiles and asks the pilot for nothing extra.
 const blockRead=gridBlockRead(rows);
 const note=<View style={{gap:3}}>
  <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{body?.delta_oi_text||
   'ΔOI is open interest added or removed since the previous close. Every line starts at 0 at the first 15-min reading of the day.'}</T>
  <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{body?.price_text||
   'The dashed line on each tile is that contract\'s own last traded price at the same 15-min readings, on its own scale.'}</T>
  <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{GRID_LEGEND_TEXT}</T>
  <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{body?.atm_text||
   'ATM is the listed strike nearest spot in the front expiry. ATM+n is n strikes above spot, ATM−n is n strikes below.'}</T>
  <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{body?.direction_text||''}</T>
  <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{body?.block_text||''}</T>
  {/* the one sentence that keeps the two lines honest */}
  <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{body?.flow_text||
   'Every opened contract has a buyer and a seller. These two lines read which side was paying up at each 15-min reading — they do not say what happens next.'}</T>
  <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{gridBasis(body)}</T>
  <T style={{fontSize:10,lineHeight:14,color:C.muted}}>A 15-min reading with no snapshot is drawn as a gap on
   both lines; nothing here is interpolated. {gridSourceText(body?.points_source)}</T>
 </View>;
 return <WidgetFrame name="ΔOI by strike · ten strikes at the money" subtitle={subtitle} body={body} state={state}
  onRefresh={read.reload} filterCount={filterCount} onCustomize={onCustomize} onExpand={onExpand} expanded={expanded}
  onClose={onClose} note={note} style={style}
  emptyDetail={body?.empty_detail||'Each line needs two 15-min readings before it is a line rather than a dot.'}>
  <ScrollView style={{flex:1}} contentContainerStyle={{padding:10,gap:10}}>
   <View onLayout={e=>{const w=Math.round(e.nativeEvent.layout.width);setWidth(v=>Math.abs(v-w)<3?v:w)}}
    style={{gap:10}}>
    {bands.map(band=><View key={band.key} style={{gap:6}}>
     <T style={{fontSize:10,lineHeight:14,fontFamily:'InterMedium',letterSpacing:.6,textTransform:'uppercase',
      color:C.muted}}>{band.label}</T>
     <View style={[s.row,{flexWrap:'wrap',alignItems:'flex-start',gap:CELL_GAP}]}>
      {band.slots.map(slot=><DeltaCell key={slot.slot} slot={slot} width={cell} onPress={()=>pick(slot)}
       selected={slot.instrument_token!=null&&target?.instrumentToken===slot.instrument_token}/>)}
     </View>
    </View>)}
    {/* one line for the whole block, and only when the ten tiles above actually say it */}
    {!!blockRead&&<T style={{fontSize:10,lineHeight:14,color:C.muted}}>{blockRead}</T>}
   </View>
  </ScrollView>
 </WidgetFrame>;
}

// --- the screener down the left -------------------------------------------------------------------------------
type ScreenerProps={rules:FilterRule[];rulesLine:string;seq:number;target:ChartTarget|null;
 onTarget:(t:ChartTarget)=>void;onCustomize:()=>void;onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;style?:any};
function UnusualScreenerPanel({rules,rulesLine,seq,target,onTarget,onCustomize,onExpand,expanded,onClose,
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
 return <WidgetFrame name="Unusual derivative screener" subtitle={rulesLine} body={body} state={state}
  onRefresh={read.reload} filterCount={(rules||[]).length} onCustomize={onCustomize} onExpand={onExpand}
  expanded={expanded} onClose={onClose}
  showsSignals={['premium_cr','volume_ratio','buildup_day','oi_change_day']}
  note={`${count} strike${count===1?'':'s'} over the floors. Clicking one points the grid and the chart at it.`}
  emptyDetail="A contract is listed only when it clears all three floors below." style={style}>
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
export const SCREENER_FLEX=1.1,FUTURES_FLEX=1,GRID_FLEX=2.9;
export type OiGridSectionProps={underlying:string;expiry:string;seq:number;rules:FilterRule[];rulesLine:string;
 target:ChartTarget|null;onTarget:(t:ChartTarget)=>void;onCustomize:()=>void;stacked?:boolean;height:number;
 hidden?:Record<string,boolean>;onHide?:(key:string)=>void;expanded?:string;onExpand?:(key:string)=>void};
export function OiGridSection({underlying,expiry,seq,rules,rulesLine,target,onTarget,onCustomize,stacked,height,
 hidden,onHide,expanded,onExpand}:OiGridSectionProps){
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 // Nothing chosen yet: show something real rather than an empty panel. First choice is the underlying with
 // the most premium traded at this 15-min reading. That list is empty whenever the newest reading was rebuilt
 // from candles instead of captured live - a candle carries no VWAP, so premium traded cannot be computed and
 // nothing clears the floors - so the index dashboard is the second choice, and it rests only on OI, which
 // every reading has. Whichever it lands on is named as a default in the subtitle, so it is never mistaken for
 // the reader's own choice, and the moment a filter or a screener row picks a symbol that choice wins.
 const busiest=useDerivativeRead<Unusual>(underlying?null:'/api/derivatives/unusual?limit=1',seq);
 const indices=useDerivativeRead<{rows?:{underlying?:string}[]}>(underlying?null:'/api/derivatives/indices',seq);
 // ONE symbol for the whole block, resolved HERE and exactly once. The futures chart and all ten ΔOI tiles are
 // handed the same answer, so a screener click re-points the whole block together and no panel can be looking
 // at a different underlying from the one beside it.
 const choice=resolveBlockSymbol(underlying,(busiest.data?.rows||[])[0]?.underlying,
  (indices.data?.rows||[])[0]?.underlying);
 const symbol=choice.symbol;
 // All three panels are the same height when they sit side by side: the chart spans both tile rows.
 const screenerStyle=stacked?{height:Math.min(height,380)}:{flex:SCREENER_FLEX,minWidth:0,height};
 const futuresStyle=stacked?{height:Math.min(height,460)}:{flex:FUTURES_FLEX,minWidth:0,height};
 const gridStyle=stacked?{height}:{flex:GRID_FLEX,minWidth:0,height};
 const screener=!shows('oi_grid_screener')?null:<UnusualScreenerPanel key="oi_grid_screener" rules={rules}
  rulesLine={rulesLine} seq={seq} target={target} onTarget={onTarget} onCustomize={onCustomize}
  onExpand={onExpand?()=>onExpand('oi_grid_screener'):undefined} expanded={expanded==='oi_grid_screener'}
  onClose={onHide?()=>onHide('oi_grid_screener'):undefined} style={screenerStyle}/>;
 const futures=!shows('oi_grid_futures')?null:<FuturesChartPanel key="oi_grid_futures" underlying={symbol}
  seq={seq} defaulted={choice.defaulted} defaultLabel={choice.label} filterCount={(rules||[]).length}
  onCustomize={onCustomize} onExpand={onExpand?()=>onExpand('oi_grid_futures'):undefined}
  expanded={expanded==='oi_grid_futures'} onClose={onHide?()=>onHide('oi_grid_futures'):undefined}
  style={futuresStyle}/>;
 const grid=!shows('oi_grid')?null:<DeltaOiGrid key="oi_grid" underlying={symbol} expiry={underlying?expiry:''}
  seq={seq} defaulted={choice.defaulted} defaultLabel={choice.label}
  target={target} onTarget={onTarget} filterCount={(rules||[]).length} onCustomize={onCustomize}
  onExpand={onExpand?()=>onExpand('oi_grid'):undefined} expanded={expanded==='oi_grid'}
  onClose={onHide?()=>onHide('oi_grid'):undefined} style={gridStyle}/>;
 if(!screener&&!futures&&!grid)return null;
 return <Section title="ΔOI by strike, through the session"
  subtitle="One symbol across the block: its front futures contract, and the open interest added or removed since the previous close at every 15-min reading for the ten strikes at the money."
  stacked={stacked}>{screener}{futures}{grid}</Section>;
}
