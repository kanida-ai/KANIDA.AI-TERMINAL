// The one line chart the four session blocks are built from: PCR, max pain against spot, implied volatility, and
// futures open interest against its own average. One component, because four copies of an axis is four places for
// the axis bug to come back — a chart shipped once with its time labels running backwards, and this is the file
// that makes that a single thing to get right.
//
// The rules it holds, all of them the tab's own and none of them new:
//   * A reading with no value is a GAP. The line breaks and nothing is interpolated or carried forward.
//   * Two series share a scale only when they are the same unit and are read against each other (max pain against
//     spot). Anything else gets its own scale, exactly as the ΔOI tiles do, and the legend says which is which.
//   * The y labels run strictly upward up the box and the x labels strictly forward in time, each one distinct.
//     Both are built in logic.ts (valueAxisLabels, sessionAxisTimes) so both are checked in one place.
//   * A direction chip says what the number DID over the last hour of readings. There is no call here (§5).
//   * A value the server withheld is the server's own sentence, not a blank.
import React,{useState} from 'react';
import {View} from 'react-native';
import Svg,{Path,Line,Circle,Text as SvgText} from 'react-native-svg';
import {C,T,s} from '../ui';
import {DirectionChip,Tag,WidgetFrame,bodyText,metaText} from './frame';
import {DASH,clock,linePath,scaleTogether,scaleValues,sessionAxisTimes,sessionSpoken,valueAxisLabels,
 type CardState,type Scaled} from './logic';
import type {Envelope} from './types';

/** Room for the value labels down the left, and for the clock labels under the line. */
const AXIS_W=46,TIME_H=14,PAD=10,MIN_PLOT_H=110;
/** The second series is thinner and dashed, so it stays the secondary line without relying on colour. */
const SECOND_DASH='3 2',FIRST_WIDTH=1.6,SECOND_WIDTH=1.2;

export type SessionSeries={
 /** The reader's name for this line, printed in the legend beside its latest value. */
 key:string;label:string;color:string;
 /** Drawn dashed and thinner: the secondary reading of the pair. */
 second?:boolean;
 values:(number|null)[];
 /** How this series' own numbers are written — a ratio, a strike, a percentage, a count. */
 format:(v:unknown)=>string;
 /** A tag welded to this line's number wherever it appears. The only one this tab uses is COMPUTED. */
 tag?:string};
export type SessionPanelProps={
 name:string;subtitle?:string;
 body?:Envelope|null;state:CardState;
 onRefresh?:()=>void;onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;
 /** The stamps of the readings, one per value in every series. */
 times:(string|null)[];
 series:SessionSeries[];
 /** True only when every series is the SAME unit and is read against the others. */
 shared?:boolean;
 /** The y-axis labels' format. With `shared` this is the one scale's; otherwise the first series'. */
 axisFormat:(v:number)=>string;
 /** What the first series did over the last hour of readings. Empty draws "no baseline" and no chip. */
 chip?:string;chipTone?:'up'|'down'|'flat';
 /** A standing label for the whole panel — COMPUTED, on the volatility panel. */
 tag?:string;tagA11y?:string;
 /** The server's own sentence when it withheld the number. Drawn in amber: a caveat on the data. */
 caveat?:string;
 /** The panel's own line under the chart. */
 note?:React.ReactNode;
 emptyDetail?:string;style?:any};

export function SessionPanel({name,subtitle,body,state,onRefresh,onExpand,expanded,onClose,times,series,shared,
 axisFormat,chip,chipTone,tag,tagA11y,caveat,note,emptyDetail,style}:SessionPanelProps){
 const [box,setBox]=useState({w:0,h:0});
 const plotW=Math.max(0,box.w-AXIS_W),plotH=Math.max(0,box.h-TIME_H);
 const first=series[0],second=series[1];
 // ONE scale when the two lines are the same unit and are read against each other; otherwise each on its own,
 // which is the same rule the ΔOI tiles work to — a rupee premium and a count of contracts never share an axis.
 const pair=shared&&second?scaleTogether(first?.values,second?.values,plotW,plotH):null;
 const scales:(Scaled|null)[]=pair?[pair.a,pair.b]
  :series.map(line=>scaleValues(line.values,plotW,plotH));
 const axisOf=pair?{lo:pair.lo,hi:pair.hi,points:[]} as Scaled:scales[0];
 const axis=valueAxisLabels(axisOf,plotH,axisFormat,3);
 const ticks=sessionAxisTimes(times,4,plotW);
 const latest=(line:SessionSeries)=>[...(line.values||[])].reverse().find(v=>v!=null);
 const spoken=sessionSpoken(`${name}${tag?`, ${tag.toLowerCase()}`:''}`,first?.values,times);
 return <WidgetFrame name={name} subtitle={subtitle} body={body} state={state} onRefresh={onRefresh}
  onExpand={onExpand} expanded={expanded} onClose={onClose} inBlock style={style}
  emptyDetail={emptyDetail}
  note={<View style={{gap:3}}>
   {!!note&&(typeof note==='string'?<T style={metaText}>{note}</T>:note)}
   {/* the amber is spent HERE and nowhere else in this panel: a number the server withheld is a caveat on
       the data, which is exactly what amber means on this tab */}
   {!!caveat&&<T style={[metaText,{color:C.amber}]}>{caveat}</T>}
  </View>}>
  <View style={{flex:1,minHeight:0,padding:PAD,gap:6}}>
   {/* the legend, the chip and the latest reading of every line, on one row above the chart */}
   <View style={[s.row,{gap:10,flexWrap:'wrap'}]}>
    <DirectionChip label={chip||''} tone={chipTone} size={10}/>
    {!!tag&&<Tag label={tag} a11y={tagA11y}/>}
    <View style={{flex:1,minWidth:4}}/>
    {series.map(line=><View key={line.key} style={[s.row,{gap:5}]}>
     <View style={{width:10,height:line.second?1:2,backgroundColor:line.color,
      opacity:line.second?0.8:1}}/>
     <T numberOfLines={1} style={[metaText,{fontVariant:['tabular-nums']}]}>
      {line.label} {line.format(latest(line))}{line.tag?` ${line.tag}`:''}</T>
    </View>)}
   </View>
   <View style={{flex:1,minHeight:MIN_PLOT_H}}
    onLayout={e=>{const {width,height}=e.nativeEvent.layout;const w=Math.round(width),h=Math.round(height);
     setBox(v=>Math.abs(v.w-w)<3&&Math.abs(v.h-h)<3?v:{w,h})}}>
    {plotW>0&&plotH>0&&<Svg width={box.w} height={box.h} accessibilityLabel={spoken}>
     {axis.map(t=><React.Fragment key={`y${t.label}`}>
      <Line x1={AXIS_W} x2={box.w} y1={Math.min(plotH,Math.max(0,t.y))} y2={Math.min(plotH,Math.max(0,t.y))}
       stroke={C.line} strokeWidth={1} strokeDasharray="2 4"/>
      <SvgText x={0} y={Math.min(plotH-1,Math.max(8,t.y+3))} fill={C.muted} fontSize={9}>{t.label}</SvgText>
     </React.Fragment>)}
     {series.map((line,i)=>{
      const scaled=scales[i];
      if(!scaled)return null;
      const last=[...scaled.points].reverse().find(Boolean);
      return <React.Fragment key={line.key}>
       <Path d={linePath(scaled)} stroke={line.color} strokeWidth={line.second?SECOND_WIDTH:FIRST_WIDTH}
        strokeDasharray={line.second?SECOND_DASH:undefined} fill="none"
        transform={`translate(${AXIS_W},0)`}/>
       {!!last&&<Circle cx={AXIS_W+last.x} cy={last.y} r={2.6} fill={line.color}/>}
      </React.Fragment>;
     })}
     {ticks.map(t=>{
      const step=times.length>1?plotW/(times.length-1):0;
      return <SvgText key={`x${t.i}`} x={Math.min(box.w-30,Math.max(AXIS_W,AXIS_W+t.i*step-15))} y={box.h-3}
       fill={C.muted} fontSize={9}>{t.label}</SvgText>;
     })}
    </Svg>}
   </View>
   {/* How many readings the axis covers. The first and last are already ON the axis — sessionAxisTimes always
       keeps both ends — so printing them again under the plot was chrome saying nothing new, which is the same
       cut the futures candle panel already made. */}
   <View style={[s.row,{gap:8}]}>
    <View style={{flex:1}}/>
    <T style={[metaText,{fontVariant:['tabular-nums']}]}>
     {times.length} captured reading{times.length===1?'':'s'}</T>
    <View style={{flex:1}}/>
   </View>
  </View>
 </WidgetFrame>;
}

/** The small stacked readings panel that sits beside every session chart: a label and a number on each line, with
 *  the number's own tag and its own direction chip when it has them. A number that is not there is a dash, and
 *  the reason it is a dash stands underneath it. A row that is a dash NEVER carries a chip: a direction is a
 *  reading of a number, and there is no number here to have read. */
export type ReadingRow={label:string;value:string;tag?:string;reason?:string;tone?:string;
 chip?:string;chipTone?:'up'|'down'|'flat'};
export function ReadingsPanel({name,subtitle,body,state,rows,onRefresh,onExpand,expanded,onClose,note,caveat,
 emptyDetail,children,style}:{name:string;subtitle?:string;body?:Envelope|null;state:CardState;rows:ReadingRow[];
 onRefresh?:()=>void;onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;note?:React.ReactNode;caveat?:string;
 emptyDetail?:string;children?:React.ReactNode;style?:any}){
 return <WidgetFrame name={name} subtitle={subtitle} body={body} state={state} onRefresh={onRefresh}
  onExpand={onExpand} expanded={expanded} onClose={onClose} inBlock style={style} emptyDetail={emptyDetail}
  note={<View style={{gap:3}}>
   {!!note&&(typeof note==='string'?<T style={metaText}>{note}</T>:note)}
   {!!caveat&&<T style={[metaText,{color:C.amber}]}>{caveat}</T>}
  </View>}>
  <View style={{flex:1,minHeight:0,padding:PAD,gap:8}}>
   {rows.map(row=>{
    const missing=row.value===DASH;
    return <View key={row.label} style={{gap:2}}>
     <T numberOfLines={1} style={metaText}>{row.label}</T>
     <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
      <T numberOfLines={1} style={[bodyText,{fontSize:13,lineHeight:18,color:row.tone||C.ink,
       fontFamily:'InterSemi',fontVariant:['tabular-nums']}]}>{row.value}</T>
      {!!row.tag&&!missing&&<Tag label={row.tag}/>}
      {/* no number, no direction: a chip beside a dash would be a reading of nothing */}
      {!!row.chip&&!missing&&<DirectionChip label={row.chip} tone={row.chipTone}/>}
     </View>
     {/* a dash never stands alone: when the server said why there is no number, that is what is printed */}
     {!!row.reason&&missing&&<T style={metaText}>{row.reason}</T>}
    </View>;
   })}
   {children}
  </View>
 </WidgetFrame>;
}
