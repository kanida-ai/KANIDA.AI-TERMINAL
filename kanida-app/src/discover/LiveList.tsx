// The "Now" list of a research scanner card: every setup still forming or confirmed, from the scanner's own
// record (docs/LIVE_DETECTION.md §5A), served by /api/strategies/{key}/detections.
//
// This list is deliberately NOT the researched-history list. It carries no edge, no expectancy and no sample
// size, because a detection that happened an hour ago has none of those - it has a lifecycle state and a time.
// The evidence for it lives in the Evidence card, and only when this detection's own identity matches the
// research run; a row whose identity does not match says so on the row itself and never borrows a number.
import React,{useRef} from 'react';
import {View,Pressable,ScrollView} from 'react-native';
import {C,T,s} from '../ui';
import type {LiveDetection} from '../strategies/types';
import {detectionDetailText,detectionRowA11y,detectionStateLabel,detectionTone,isNewDetection,sinceText} from './cardLogic';
import {web,webOnly} from './parts';

const head={fontSize:10,lineHeight:14,fontFamily:'InterMedium',letterSpacing:.6,textTransform:'uppercase' as const,color:C.muted};
const tab={fontVariant:['tabular-nums'] as any};
/** Right column: the lifecycle chip over "since 16 Sep". Wide enough for "Confirmed" and "since 30 Sep" whole. */
export const LIVE_COL={state:78};
const STATE_COLOR={confirmed:C.green,forming:C.amber,done:C.muted} as const;

/** One lifecycle chip. The colour is about the STATE, never about whether the detection looks good. */
export function StateChip({d}:{d:LiveDetection}){
 const color=STATE_COLOR[detectionTone(d.state)];
 return <View aria-hidden style={{alignSelf:'flex-end',paddingHorizontal:6,minHeight:17,justifyContent:'center',borderRadius:5,borderWidth:1,borderColor:color}}>
  <T numberOfLines={1} style={{fontSize:9.5,lineHeight:13,fontFamily:'InterSemi',color}}>{detectionStateLabel(d)}</T>
 </View>;
}

export type LiveListProps={
 rows:LiveDetection[];total:number;
 /** Newest session the scanner holds for this timeframe: a row first found on it gets the "new" marker. */
 session?:string|null;
 /** `detection_id` of the row this card's block link currently points at. */
 selectedId?:string|null;
 onSelect:(d:LiveDetection)=>void;onOpenFull:(d:LiveDetection)=>void;
 label:string;
};
export function LiveList({rows,total,session,selectedId,onSelect,onOpenFull,label}:LiveListProps){
 const refs=useRef<any[]>([]);
 const selIndex=rows.findIndex(r=>r.detection_id===selectedId);
 const selectAt=(j:number)=>{const row=rows[j];if(!row)return;onSelect(row);
  setTimeout(()=>{refs.current[j]?.focus?.();refs.current[j]?.scrollIntoView?.({block:'nearest'})},0)};
 const onKey=(i:number,e:any)=>{const k=e?.key;
  if(k==='ArrowDown'||k==='ArrowUp'||k==='Home'||k==='End'){e.preventDefault?.();
   selectAt(k==='Home'?0:k==='End'?rows.length-1:Math.max(0,Math.min(rows.length-1,i+(k==='ArrowDown'?1:-1))))}
  else if(k==='Enter'&&rows[i]){e.preventDefault?.();onOpenFull(rows[i])}};
 return <>
  <View style={[s.row,{paddingHorizontal:12,minHeight:24,gap:5}]}>
   <T numberOfLines={1} style={[head,{flex:1,minWidth:0}]}>Stock</T>
   <T numberOfLines={1} style={[head,{width:LIVE_COL.state,textAlign:'right'}]}>Setup</T>
  </View>
  <ScrollView style={{flex:1}} contentContainerStyle={{paddingBottom:6}}>
   <View aria-label={`${label}: ${total} setups now`} {...webOnly({role:'listbox'})}>
    {rows.map((r,i)=>{
     const on=r.detection_id===selectedId,fresh=isNewDetection(r as any,session),since=sinceText(r);
     // A row reads: stock, company, Forming/Confirmed, "since 16 Sep" - and a small "new" marker when it was first
     // found on the latest session. Nothing is cut: the company wraps instead of truncating. The row's age in
     // candles, a missing chart outline and a missing past-data match stay in its spoken label.
     return <Pressable key={r.detection_id} ref={(el:any)=>{refs.current[i]=el}} role="option" aria-selected={on}
      accessibilityState={{selected:on}} accessibilityLabel={`${detectionRowA11y(r,session)}${on?', selected':''}`}
      {...webOnly({tabIndex:(selIndex<0?i===0:on)?0:-1,onKeyDown:(e:any)=>onKey(i,e)})} onPress={()=>onSelect(r)}
      style={(st:any)=>[s.row,{gap:8,minHeight:44,paddingHorizontal:8,paddingVertical:4,marginHorizontal:3,marginVertical:1,borderRadius:8,borderWidth:1,borderColor:on?C.green:'transparent',backgroundColor:on?C.soft:st.hovered?'#0F1F28':'transparent'},st.focused&&web?({outlineStyle:'solid',outlineWidth:2,outlineColor:C.mint,outlineOffset:-1} as any):null]}>
      <View style={{flex:1,minWidth:0}}>
       <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
        <T style={{fontSize:13,lineHeight:17,fontFamily:'InterSemi',color:on?C.mint:C.ink}}>{r.symbol}</T>
        {fresh&&<View aria-hidden style={{paddingHorizontal:5,minHeight:15,justifyContent:'center',borderRadius:4,backgroundColor:C.soft}}>
         <T style={{fontSize:9.5,lineHeight:13,fontFamily:'InterSemi',color:C.mint}}>new</T></View>}
       </View>
       <T style={{fontSize:11,lineHeight:14,color:C.muted}}>{r.company}</T>
      </View>
      <View style={{width:LIVE_COL.state,flexShrink:0,alignItems:'flex-end',gap:2}}>
       <StateChip d={r}/>
       {!!since&&<T style={[tab,{fontSize:10,lineHeight:13,color:C.muted,textAlign:'right'}]}>{since}</T>}
      </View>
     </Pressable>;
    })}
   </View>
  </ScrollView>
 </>;
}
export {detectionDetailText};
