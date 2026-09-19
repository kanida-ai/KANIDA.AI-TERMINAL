import React from 'react';
import {View,ScrollView} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {Popover} from './Popover';
import {web,webOnly} from './shared';
import {dataStatusView,type CacheProvenance,type DataStatus,type DataStatusTone,type DataStatusView} from './dataStatus';

// Data status popover: what the top-bar / Discover data pill opens. Presentational —
// it renders one `data_status` object (src/layout/dataStatus.ts does all the judging).
const TONE_COLOR:Record<DataStatusTone,string>={fresh:C.green,stale:C.amber,'very-stale':C.red,unknown:C.amber};
const TONE_BG:Record<DataStatusTone,string>={fresh:C.soft,stale:C.amberBg,'very-stale':'#2A1519',unknown:C.amberBg};

function Row({label,value,note,tone}:{label:string;value:string;note?:string;tone?:DataStatusTone}){
 return <View style={{gap:2,paddingVertical:5}}>
  <View style={[s.row,{gap:8,alignItems:'flex-start'}]}>
   <T style={{fontSize:11,lineHeight:17,color:C.muted,width:92}}>{label}</T>
   <T style={{flex:1,fontSize:12,lineHeight:18,fontFamily:'InterMedium',color:tone?TONE_COLOR[tone]:C.ink}}>{value}</T>
  </View>
  {!!note&&<T style={{fontSize:10,lineHeight:15,color:C.muted,marginLeft:100}}>{note}</T>}
 </View>;
}

export type DataStatusPanelProps={view:DataStatusView};
export function DataStatusPanel({view}:DataStatusPanelProps){
 const col=TONE_COLOR[view.tone];
 return <ScrollView style={{maxHeight:460}} contentContainerStyle={{paddingHorizontal:14,paddingVertical:10,gap:4}}>
  <T role="heading" aria-level={2} style={{fontSize:11,lineHeight:16,fontFamily:'InterSemi',letterSpacing:1.2,textTransform:'uppercase',color:C.muted}}>Data status</T>
  {/* The live region: a poll that changes the state announces it without moving focus. */}
  <View role="status" {...webOnly({'aria-live':'polite'})} accessibilityLiveRegion="polite" accessibilityLabel={view.a11y} style={{gap:4,paddingBottom:4}}>
   <View style={[s.row,{gap:8,alignItems:'flex-start'}]}>
    <Icon name={view.tone==='fresh'?'database':'alert-circle'} size={15} color={col}/>
    <T style={{flex:1,fontSize:13,lineHeight:19,fontFamily:'InterSemi',color:col}}>{view.headline}</T>
   </View>
   <T style={{fontSize:11,lineHeight:17,color:C.muted}}>{view.detail}</T>
  </View>
  {!!view.warning&&<View style={[s.row,{gap:8,alignItems:'flex-start',padding:9,borderRadius:9,backgroundColor:TONE_BG[view.warning.tone],borderWidth:1,borderColor:TONE_COLOR[view.warning.tone]+'55'}]}>
   <Icon name="alert-triangle" size={14} color={TONE_COLOR[view.warning.tone]}/>
   <T style={{flex:1,fontSize:11,lineHeight:17,color:TONE_COLOR[view.warning.tone]}}>{view.warning.text}</T>
  </View>}
  <View style={{height:1,backgroundColor:C.line,marginVertical:6}}/>
  {view.rows.map(r=><Row key={r.key} label={r.label} value={r.value} note={r.note} tone={r.tone}/>)}
  <View style={{height:1,backgroundColor:C.line,marginVertical:6}}/>
  <T style={{fontSize:11,lineHeight:17,color:C.muted}}>{view.expectation}</T>
 </ScrollView>;
}

export type DataStatusPopoverProps={open:boolean;onClose:()=>void;anchor?:any;status?:DataStatus|null;dataEnd?:string;ageDays?:number;staleLimit?:number;placement?:'bottom-start'|'bottom-end';routeKey?:unknown;
 /** Scanner connection provenance - the object the data arrived on (see DataAgePill.cache). */
 cache?:CacheProvenance|null;error?:string};
// The popover itself, so both pills (top bar and Discover) mount exactly one implementation.
export function DataStatusPopover({open,onClose,anchor,status,dataEnd,ageDays,staleLimit,placement='bottom-end',routeKey,cache,error}:DataStatusPopoverProps){
 const view=React.useMemo(()=>dataStatusView(status,{dataEnd,ageDays,staleLimit,cache,error}),[status,dataEnd,ageDays,staleLimit,cache,error,open]);
 const first=React.useRef<any>(null);
 // Keyboard: the panel takes focus on open so Tab/Escape act inside it, and Popover returns focus to the pill on close.
 React.useEffect(()=>{if(open&&web)setTimeout(()=>first.current?.focus?.(),0);},[open]);
 if(!open)return null;
 return <Popover open={open} onClose={onClose} anchor={anchor} placement={placement} label="Data status" width={330} routeKey={routeKey}>
  <View ref={first} {...webOnly({tabIndex:-1})} style={{flexShrink:1}}><DataStatusPanel view={view}/></View>
 </Popover>;
}
