import React from 'react';
import {View} from 'react-native';
import {C,T} from '../ui';
import {formatDataDate} from '../layout';
import type {MarketContext} from '../strategies/types';
// Market context from stage 2 (our own prices). Missing values read "not available" — never filled in.
const NA='not available';
function Item({label,value,tone}:{label:string;value:string;tone?:string}){return <View style={{gap:2,minWidth:120}}><T style={{fontSize:10.5,letterSpacing:1,textTransform:'uppercase',color:C.muted}}>{label}</T><T style={{fontFamily:'InterSemi',fontSize:15,color:value===NA?C.muted:tone||C.ink}}>{value}</T></View>;}
export function ContextStrip({context}:{context:MarketContext|null|undefined}){
 if(!context)return <View style={{padding:14,borderRadius:12,borderWidth:1,borderColor:C.line,backgroundColor:C.paper}}><T style={{fontSize:13,color:C.muted}}>Market context is not available for this scan.</T></View>;
 const c=context,trend=c.nifty_trend;
 const breadth=c.above_50dma_pct==null?NA:`${Math.round(c.above_50dma_pct)}%`;
 const ad=c.advancers==null||c.decliners==null?NA:`${c.advancers} / ${c.decliners}${c.unchanged!=null?` · ${c.unchanged} flat`:''}`;
 return <View accessibilityRole="summary" style={{padding:14,borderRadius:12,borderWidth:1,borderColor:C.line,backgroundColor:C.paper,gap:10}}>
  <View style={{flexDirection:'row',flexWrap:'wrap',columnGap:28,rowGap:10}}>
   <Item label="Above 50-day avg" value={breadth}/>
   <Item label="Advancers / decliners" value={ad}/>
   <Item label="NIFTY trend" value={trend??NA} tone={trend==='up'?C.green:trend==='down'?C.red:undefined}/>
   <Item label="Basis" value={c.basis_date?`${formatDataDate(c.basis_date)}${c.stocks?` · ${c.stocks} stocks`:''}`:NA}/>
  </View>
  {!!c.note&&<T style={{fontSize:12,lineHeight:18,color:C.muted}}>{c.note}</T>}
 </View>;
}
