import React from 'react';
import {View,Pressable} from 'react-native';
import {C,T,Icon,s,useLayoutMode} from '../ui';
import {DATA_STALE_DAYS} from '../constants';
import {useTitle,webOnly} from './shared';
export type TopBarProps={left?:React.ReactNode;center?:React.ReactNode;right?:React.ReactNode;below?:React.ReactNode;label?:string;style?:any};
// Slot layout. desktop/tablet: one row [left][center flex][right]; phone: [left][right] then center on its own full-width row. `below` is a wrapping second row (e.g. analysis toggles).
export function TopBar({left,center,right,below,label='Workspace top bar',style}:TopBarProps){
 const phone=useLayoutMode()==='phone';
 return <View role="group" aria-label={label} style={[{backgroundColor:C.bg,borderBottomWidth:1,borderColor:C.line,paddingHorizontal:phone?10:14,paddingVertical:6,gap:6},style]}>
  <View style={[s.row,{minHeight:44,gap:phone?6:12}]}>
   <View style={[s.row,{flexShrink:0,gap:8}]}>{left}</View>
   {phone?<View style={{flex:1}}/>:<View style={[s.row,{flex:1,minWidth:0,gap:8}]}>{center}</View>}
   <View style={[s.row,{flexShrink:0,gap:6,justifyContent:'flex-end'}]}>{right}</View>
  </View>
  {phone&&center!=null&&center!==false&&<View style={[s.row,{gap:6}]}>{center}</View>}
  {below!=null&&below!==false&&<View style={[s.row,{flexWrap:'wrap',gap:6}]}>{below}</View>}
 </View>;
}
// Data age maths and pill copy live in ./dataStatus (pure, and shared with the Data status
// popover); re-exported here so every existing import keeps working.
export {dataAgeDays,formatDataDate} from './dataStatus';
import {dataAgeDays,formatDataDate,pillContent,type CacheProvenance,type DataStatus,type DataStatusTone} from './dataStatus';
export type DataAgeTone=DataStatusTone;
export function dataAgeTone(age:number|undefined,staleLimit:number=DATA_STALE_DAYS,redAfterDays:number=staleLimit*5):DataAgeTone{return age==null||!Number.isFinite(age)?'unknown':age>redAfterDays?'very-stale':age>staleLimit?'stale':'fresh';}
export type DataAgePillProps={dataEnd?:string;ageDays?:number;staleLimit?:number;redAfterDays?:number;onPress?:()=>void;compact?:boolean;status?:DataStatus|null;expanded?:boolean;
 /** Scanner connection provenance (BACKLOG item 2a): pass the object that carried the data (`state`, or the
  * strategy catalog). When it says the body was replayed from the last good scan, the pill says WHEN that
  * scan was taken and that it is reconnecting - never "Data age unknown". */
 cache?:CacheProvenance|null;
 /** A hard failure with nothing cached to show. */
 error?:string;
 /** 'line': the one-sentence form for a page header ("Prices 09:45 · patterns updated 10:15 · next update 11:15"); wraps instead of truncating. */
 variant?:'pill'|'line'};
// "Data: 31 Jul 2026 · 44d", or "Live · Prices 09:45 · Patterns 10:15" when `status` says the source is live.
// Mint when age <= staleLimit (DATA_STALE_DAYS); amber + "STALE" beyond it; red beyond
// redAfterDays (default 5x limit) or when ingestion is stalled; amber "age unknown" when no date.
// The words STALE / STALLED and the a11y label carry the state, never colour alone.
// With `onPress` the pill becomes a button INSIDE its status region, so the region stays a live
// region (and keeps its "Market data …" name) while the press target is keyboard reachable.
export function DataAgePill({dataEnd,ageDays,staleLimit=DATA_STALE_DAYS,redAfterDays,onPress,compact=false,status,expanded,cache,error,variant='pill'}:DataAgePillProps){
 // Re-render each minute so "next update 10:15" and the today-vs-date wording never go stale between polls.
 const [,tick]=React.useState(0);React.useEffect(()=>{const t=setInterval(()=>tick(x=>x+1),60000);return ()=>clearInterval(t);},[]);
 const line=variant==='line';
 const age=ageDays!=null&&Number.isFinite(ageDays)?Math.max(0,Math.round(ageDays)):dataAgeDays(dataEnd);
 const pill=pillContent(status,{dataEnd,ageDays:age,staleLimit,compact,cache,error});
 const cached=cache?.served_from_cache===true||cache?.unreachable===true||!!error;
 const tone=cached||(status&&!status.error)?pill.tone:dataAgeTone(age,staleLimit,redAfterDays??staleLimit*5);
 const stale=tone==='stale'||tone==='very-stale';
 const col=tone==='fresh'?C.green:tone==='very-stale'?C.red:C.amber,bg=tone==='fresh'?C.soft:tone==='very-stale'?'#2A1519':C.amberBg;
 const a11y=pill.label;
 const ref=useTitle(a11y);
 const body=<><Icon name={stale||tone==='unknown'?'alert-circle':'database'} size={13} color={col}/><T numberOfLines={line?2:1} style={{fontSize:line?12:11,lineHeight:line?17:16,fontFamily:line?'InterMedium':'InterSemi',color:col,flexShrink:1}}>{line?pill.line:pill.text}</T></>;
 const box={flexDirection:'row' as const,alignItems:'center' as const,gap:6,alignSelf:'center' as const,minHeight:28,flexShrink:1,maxWidth:'100%' as const,paddingHorizontal:9,borderRadius:8,backgroundColor:bg,borderWidth:1,borderColor:tone==='fresh'?C.line:col+'55'};
 if(onPress)return <View ref={ref} role="status" aria-label={a11y} accessibilityLabel={a11y} style={{alignSelf:'center'}}>
  <Pressable accessibilityRole="button" accessibilityLabel={`${a11y}. Open data status`} accessibilityState={{expanded:!!expanded}} {...webOnly({'aria-expanded':!!expanded,'aria-haspopup':'dialog'})} onPress={onPress} style={({pressed,hovered,focused}:any)=>[box,{opacity:pressed?.7:1,borderColor:hovered||focused?col:(tone==='fresh'?C.line:col+'55')}]}>{body}</Pressable>
 </View>;
 return <View ref={ref} role="status" aria-label={a11y} accessibilityLabel={a11y} style={box}>{body}</View>;
}
