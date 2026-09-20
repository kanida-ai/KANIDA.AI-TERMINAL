// The tab's ONE linked chart: the price and open interest of whatever contract the reader last clicked, at the
// 15-min readings the store holds. The page owns the target and the read, so clicking a row in ANY block
// re-points this same tile instead of each block fetching the same readings again.
//
// It is the chart panel of the blocks that have no series of their own — the option chain, the index dashboard
// and the futures build-up list — and it wears the template's panel style like every other panel on the tab.
//
// Why not PatternCanvas: that canvas draws a detected chart pattern from the scanner's /api/chart, keyed on a
// pattern match. A derivatives contract has no pattern and no match, and dressing one up as a Match to reuse the
// canvas would put a price series under a pattern's frame. The full price chart is one click away instead
// ("Edit this chart"), and this tile shows the one series the tab is about: price against open interest.
import React,{useState} from 'react';
import {View} from 'react-native';
import Svg,{Path,Line,Circle} from 'react-native-svg';
import {router} from 'expo-router';
import {C,T,s} from '../ui';
import {chartHref} from '../discover/deeplink';
import {LinkText} from '../discover/parts';
import {CrosshairReadout,WidgetFrame,metaText,tab as tabular,useHoverIndex} from './frame';
import {clock,compact,linePath,price,scaleSeries,type CardState} from './logic';
import type {ChartTarget,Series} from './types';
const PAD=10;
export type ChartTileProps={target:ChartTarget|null;body:Series|null;state:CardState;onRefresh?:()=>void;
 onClose?:()=>void;onExpand?:()=>void;expanded?:boolean;height?:number;inBlock?:boolean;style?:any};
export function ChartTile({target,body,state,onRefresh,onClose,onExpand,expanded,inBlock,style}:ChartTileProps){
 const [box,setBox]=useState({w:0,h:0});
 const points=body?.points||[];
 const plotW=Math.max(0,box.w-PAD*2),plotH=Math.max(0,box.h-26);
 const priceLine=scaleSeries(points,'price',plotW,plotH),oiLine=scaleSeries(points,'oi',plotW,plotH);
 const last=points[points.length-1];
 const href=target?chartHref(target.underlying,'1D'):'';
 const plotted=body?`${body.price_label||'Price'} and ${(body.oi_label||'Open interest').toLowerCase()}`:'price and open interest';
 // "TXN, Candles, Daily" on the benchmark; ours names the subject and what is actually drawn under it.
 const name=target?`${target.label} · ${plotted} · 15-min readings`:'Chart';
 // Hover to read a value off the two lines. The index is clamped to the readings actually drawn, and a reading
 // with no value still reads as a dash: the cursor READS the series, it never fills one in.
 const count=points.length;
 const {cursor,hover}=useHoverIndex((x,width)=>{
  if(!count)return null;
  const scale=width>0&&box.w>0?box.w/width:1;
  const step=count>1?plotW/(count-1):0;
  if(step<=0)return 0;
  return Math.max(0,Math.min(count-1,Math.round((x*scale-PAD)/step)));
 });
 const hovered=cursor!=null&&cursor>=0&&cursor<count;
 const at=hovered?points[cursor as number]:null;
 const step=count>1?plotW/(count-1):0;
 if(!target)return <WidgetFrame name="Chart" body={null} onClose={onClose} inBlock={inBlock}
  state={{phase:'empty',text:'Click any row on this tab to point the chart at it.'}}
  footer={inBlock?undefined:null} style={style}/>;
 return <WidgetFrame name={name} subtitle={target.detail} body={body} state={state} onRefresh={onRefresh}
  onExpand={onExpand} expanded={expanded} onClose={onClose} inBlock={inBlock} style={style}
  note="Both lines are the stored 15-min readings and have their own scales. They are drawn only where a reading exists — a gap in the capture stays a gap. Pointing at the chart reads the values off it."
  emptyDetail="The tile draws only readings that were captured; it never fills a gap in the series.">
  <View style={{flex:1,minHeight:0,padding:10,gap:8}}>
   {hovered&&at
    ?<CrosshairReadout time={clock(at.t)}
      rows={[{label:body?.price_label||'Price',value:price(at.price),color:C.mint},
       {label:body?.oi_label||'Open interest',value:compact(at.oi),color:C.amber}]}/>
    :<View style={[s.row,{gap:12,flexWrap:'wrap',minHeight:24}]}>
      <View style={[s.row,{gap:5}]}><View style={{width:9,height:2,backgroundColor:C.mint}}/>
       <T style={metaText}>{body?.price_label||'Price'} {price(last?.price)}</T></View>
      <View style={[s.row,{gap:5}]}><View style={{width:9,height:2,backgroundColor:C.amber}}/>
       <T style={metaText}>{body?.oi_label||'Open interest'} {compact(last?.oi)}</T></View>
      <View style={{flex:1}}/>
      {!!href&&<LinkText label="Edit this chart" a11y={`Open ${target.underlying} on the full chart workspace`}
       onPress={()=>router.push(href as any)}/>}
     </View>}
   <View style={{flex:1,minHeight:90}} {...hover}
    onLayout={e=>{const {width,height}=e.nativeEvent.layout;const w=Math.round(width),h=Math.round(height);
     setBox(v=>Math.abs(v.w-w)<3&&Math.abs(v.h-h)<3?v:{w,h})}}>
    {box.w>0&&box.h>0&&<Svg width={box.w} height={box.h}
     accessibilityLabel={`${target.label}: ${points.length} captured 15-min readings from ${clock(points[0]?.t)} to ${clock(last?.t)}.`}>
     <Line x1={PAD} x2={box.w-PAD} y1={plotH} y2={plotH} stroke={C.line} strokeWidth={1}/>
     <Path d={linePath(oiLine)} stroke={C.amber} strokeWidth={1.3} fill="none" opacity={0.85} transform={`translate(${PAD},0)`}/>
     <Path d={linePath(priceLine)} stroke={C.mint} strokeWidth={1.7} fill="none" transform={`translate(${PAD},0)`}/>
     {!!priceLine&&(()=>{const p=[...priceLine.points].reverse().find(Boolean);
      return p?<Circle cx={PAD+p.x} cy={p.y} r={3} fill={C.mint}/>:null})()}
     {/* the crosshair: one rule at the reading under the pointer, and a ring on each line that HAS a value
         there. A line with no value at that reading gets no ring, and the readout above says dash. */}
     {hovered&&<React.Fragment key="cursor">
      <Line x1={PAD+(cursor as number)*step} x2={PAD+(cursor as number)*step} y1={0} y2={plotH}
       stroke={C.green} strokeWidth={1} strokeOpacity={0.55} strokeDasharray="3 3"/>
      {[{line:oiLine,color:C.amber},{line:priceLine,color:C.mint}].map((each,i)=>{
       const point=each.line?.points?.[cursor as number];
       return point?<Circle key={i} cx={PAD+point.x} cy={point.y} r={3.4} fill={each.color} stroke={C.bg}
        strokeWidth={1}/>:null;
      })}
     </React.Fragment>}
    </Svg>}
   </View>
   <View style={[s.row,{gap:8}]}>
    <T style={metaText}>{clock(points[0]?.t)}</T><View style={{flex:1}}/>
    <T style={[tabular,metaText]}>{points.length} captured readings</T><View style={{flex:1}}/>
    <T style={metaText}>{clock(last?.t)}</T>
   </View>
  </View>
 </WidgetFrame>;
}
