// The chart tile that sits beside every table, as on the benchmark: a title naming the subject and what is drawn,
// an "Edit this chart" link under it, a close ✕ in the header, and the chart filling the rest.
//
// One tile, one series: the page owns the target and the read, so clicking a row in ANY section re-points this same
// tile instead of each section fetching the same marks again.
//
// Why not PatternCanvas: that canvas draws a detected chart pattern from the scanner's /api/chart, keyed on a pattern
// match. A derivatives contract has no pattern and no match, and dressing one up as a Match to reuse the canvas would
// put a price series under a pattern's frame. The full price chart is one click away instead ("Edit this chart"),
// and this tile shows the one series the tab is about: price against open interest.
import React,{useState} from 'react';
import {View,ScrollView} from 'react-native';
import Svg,{Path,Line,Circle} from 'react-native-svg';
import {router} from 'expo-router';
import {C,T,s} from '../ui';
import {chartHref} from '../discover/deeplink';
import {LinkText} from '../discover/parts';
import {WidgetFrame,tab as tabular} from './frame';
import {clock,compact,linePath,price,scaleSeries,type CardState} from './logic';
import type {ChartTarget,Series} from './types';
const PAD=10;
export type ChartTileProps={target:ChartTarget|null;body:Series|null;state:CardState;onRefresh?:()=>void;
 onClose?:()=>void;onExpand?:()=>void;expanded?:boolean;height?:number;style?:any};
export function ChartTile({target,body,state,onRefresh,onClose,onExpand,expanded,height=210,style}:ChartTileProps){
 const [width,setWidth]=useState(0);
 const points=body?.points||[];
 const plotW=Math.max(0,width-PAD*2),plotH=height-26;
 const priceLine=scaleSeries(points,'price',plotW,plotH),oiLine=scaleSeries(points,'oi',plotW,plotH);
 const last=points[points.length-1];
 const href=target?chartHref(target.underlying,'1D'):'';
 const plotted=body?`${body.price_label||'Price'} and ${(body.oi_label||'Open interest').toLowerCase()}`:'price and open interest';
 // "TXN, Candles, Daily" on the benchmark; ours names the subject and what is actually drawn under it.
 const name=target?`${target.label} · ${plotted} · 15-min readings`:'Chart';
 if(!target)return <WidgetFrame name="Chart" body={null} onClose={onClose}
  state={{phase:'empty',text:'Click any row on this tab to point the chart at it.'}} footer={null} style={style}/>;
 return <WidgetFrame name={name} subtitle={target.detail} body={body} state={state} onRefresh={onRefresh}
  onExpand={onExpand} expanded={expanded} onClose={onClose} style={style}
  note="Both lines are the stored 15-min readings and have their own scales. They are drawn only where a reading exists — a gap in the capture stays a gap."
  emptyDetail="The tile draws only readings that were captured; it never fills a gap in the series.">
  <ScrollView style={{flex:1}} contentContainerStyle={{padding:10,gap:8}}>
   <View style={[s.row,{gap:12,flexWrap:'wrap'}]}>
    <View style={[s.row,{gap:5}]}><View style={{width:9,height:2,backgroundColor:C.mint}}/>
     <T style={{fontSize:11,color:C.muted}}>{body?.price_label||'Price'} {price(last?.price)}</T></View>
    <View style={[s.row,{gap:5}]}><View style={{width:9,height:2,backgroundColor:C.amber}}/>
     <T style={{fontSize:11,color:C.muted}}>{body?.oi_label||'Open interest'} {compact(last?.oi)}</T></View>
    <View style={{flex:1}}/>
    {!!href&&<LinkText label="Edit this chart" a11y={`Open ${target.underlying} on the full chart workspace`}
     onPress={()=>router.push(href as any)}/>}
   </View>
   <View onLayout={e=>{const w=Math.round(e.nativeEvent.layout.width);setWidth(v=>Math.abs(v-w)<3?v:w)}} style={{height}}>
    {width>0&&<Svg width={width} height={height}
     accessibilityLabel={`${target.label}: ${points.length} captured 15-min readings from ${clock(points[0]?.t)} to ${clock(last?.t)}.`}>
     <Line x1={PAD} x2={width-PAD} y1={plotH} y2={plotH} stroke={C.line} strokeWidth={1}/>
     <Path d={linePath(oiLine)} stroke={C.amber} strokeWidth={1.3} fill="none" opacity={0.85} transform={`translate(${PAD},0)`}/>
     <Path d={linePath(priceLine)} stroke={C.mint} strokeWidth={1.7} fill="none" transform={`translate(${PAD},0)`}/>
     {!!priceLine&&(()=>{const p=[...priceLine.points].reverse().find(Boolean);
      return p?<Circle cx={PAD+p.x} cy={p.y} r={3} fill={C.mint}/>:null})()}
    </Svg>}
   </View>
   <View style={[s.row,{gap:8}]}>
    <T style={{fontSize:10,color:C.muted}}>{clock(points[0]?.t)}</T><View style={{flex:1}}/>
    <T style={[tabular,{fontSize:10,color:C.muted}]}>{points.length} captured readings</T><View style={{flex:1}}/>
    <T style={{fontSize:10,color:C.muted}}>{clock(last?.t)}</T>
   </View>
  </ScrollView>
 </WidgetFrame>;
}
