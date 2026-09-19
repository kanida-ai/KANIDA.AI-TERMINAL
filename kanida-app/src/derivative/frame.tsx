// The Derivative tab's frame, rebuilt on the TrendSpider dashboard the owner benchmarked: a SECTION title in large
// text with a row of WIDGETS under it, and every widget wearing the same header bar —
//   <name>   Customize (N filters)…                                        ⟳  ⤢  ✕
// Nothing about the numbers changes here (docs/DERIVATIVES_SPEC.md §3–§5 still hold): each widget prints its own
// as-of time and the liquidity floors in force, names any §3 signal the metrics worker has not written yet, and says
// in one plain sentence when it has nothing to show. An empty widget is never an error page.
import React from 'react';
import {View,Pressable,ActivityIndicator} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {IconButton} from '../layout';
import {CenterNote,LinkText,webOnly} from '../discover/parts';
import {asOfText,cardState,customizeLabel,floorsText,missingText,sourceText,type CardState} from './logic';
import type {Envelope} from './types';
export {webOnly,CenterNote};

// --- shared cell styling ---------------------------------------------------------------------------------------
export const head={fontSize:10,lineHeight:14,fontFamily:'InterMedium',letterSpacing:.6,
 textTransform:'uppercase' as const,color:C.muted};
export const tab={fontVariant:['tabular-nums'] as any};
export const tone=(v:number|null|undefined)=>v==null?C.muted:v>0?C.green:v<0?C.red:C.ink;
export const buildupColor=(t:'up'|'down'|'flat')=>t==='up'?C.green:t==='down'?C.red:C.muted;
/** CALL green / PUT red, the benchmark's own colour code. An unknown type stays neutral rather than guessing. */
export const typeColor=(t:'call'|'put'|'flat')=>t==='call'?C.green:t==='put'?C.red:C.muted;
export function Pill({label,tone:t=C.muted}:{label:string;tone?:string}){
 return <View style={{alignSelf:'flex-start',paddingHorizontal:6,paddingVertical:2,borderRadius:5,
  backgroundColor:t===C.muted?C.bg:C.soft}}>
  <T numberOfLines={1} style={{fontSize:10,lineHeight:14,fontFamily:'InterSemi',color:t}}>{label}</T></View>;
}
/** Nothing captured is not an error: capture simply has not run yet. */
export function EmptyPanel({text,detail}:{text:string;detail?:string}){
 return <CenterNote icon="clock" text={text}>{!!detail&&<T style={{fontSize:11,lineHeight:16,color:C.muted,
  textAlign:'center',maxWidth:340}}>{detail}</T>}</CenterNote>;
}
export function LoadingPanel(){
 return <View style={{flex:1,alignItems:'center',justifyContent:'center',gap:10,padding:20}}>
  <ActivityIndicator color={C.green}/><T style={{fontSize:12,color:C.muted}}>Reading the F&amp;O store…</T></View>;
}
/** Builds the widget state in one place so every widget degrades the same way. */
export function stateOf(read:{data:any;error:string;phase:string},emptyText?:string):CardState{
 return cardState(read.data,{loading:read.phase==='loading',error:read.phase==='error'?read.error:'',emptyText});
}
export function SourceLine({body}:{body:Envelope|null}){
 const text=sourceText(body?.source);
 return text?<T style={{fontSize:10,lineHeight:14,color:C.muted}}>{text}</T>:null;
}

// --- section ----------------------------------------------------------------------------------------------------
export type SectionProps={title:string;subtitle?:string;stacked?:boolean;children:React.ReactNode};
/** A section title in large text, then its row of widgets. Below ~1100px the row becomes a column, so the chart
 *  tile drops under the table instead of squeezing it. */
export function Section({title,subtitle,stacked,children}:SectionProps){
 return <View style={{gap:8}}>
  <View style={{gap:2}}>
   <T role="heading" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:19,lineHeight:26,letterSpacing:-.3}}>{title}</T>
   {!!subtitle&&<T style={{fontSize:12,lineHeight:17,color:C.muted}}>{subtitle}</T>}
  </View>
  <View style={{flexDirection:stacked?'column':'row',alignItems:'stretch',gap:12}}>{children}</View>
 </View>;
}

// --- widget -----------------------------------------------------------------------------------------------------
export type WidgetFrameProps={
 /** The widget's own name, on the left of the header bar. */
 name:string;
 subtitle?:string;
 /** The envelope whose as-of, floors and missing-signal list are printed under the widget. */
 body?:Envelope|null;
 state?:CardState;
 /** Only the §3 signals this widget actually shows are named as "not captured yet". */
 showsSignals?:string[];
 /** A number turns the "Customize (N filters)…" link on; the count is live. */
 filterCount?:number;
 onCustomize?:()=>void;
 onRefresh?:()=>void;
 onExpand?:()=>void;expanded?:boolean;
 onClose?:()=>void;
 /** A control strip under the header bar, OUTSIDE the state switch. A widget whose control chooses what it
  *  reads (the futures chart's interval) must keep that control on screen while the chosen reading is empty,
  *  or a reader who picks an empty interval has no way back. */
 toolbar?:React.ReactNode;
 /** An extra footer line: what the rows are, what a sort did, what the server capped. */
 note?:React.ReactNode;
 emptyDetail?:string;
 /** Shown instead of the body's as-of/floors footer when this widget draws no captured numbers. */
 footer?:React.ReactNode;
 children?:React.ReactNode;
 style?:any;
};
export function WidgetFrame({name,subtitle,body,state,showsSignals,filterCount,onCustomize,onRefresh,onExpand,
 expanded,onClose,toolbar,note,emptyDetail,footer,children,style}:WidgetFrameProps){
 const missing=missingText(body?.missing,showsSignals||[]);
 const phase=state?.phase||'ready';
 let inner:React.ReactNode=children;
 if(phase==='loading')inner=<LoadingPanel/>;
 else if(phase==='error')inner=<CenterNote icon="alert-triangle"
  text={`No rows are shown while this widget is unavailable. This is not the same as nothing happening. ${state?.text||''}`}>
  {!!onRefresh&&<Pressable accessibilityRole="button" accessibilityLabel={`Retry ${name}`} onPress={onRefresh}
   style={(st:any)=>[{minHeight:32,paddingHorizontal:12,borderRadius:8,borderWidth:1,borderColor:C.line,
    justifyContent:'center',backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}>
   <T style={{fontSize:12,fontFamily:'InterSemi',color:C.green}}>Retry</T></Pressable>}
 </CenterNote>;
 else if(phase==='empty')inner=<EmptyPanel text={state?.text||''} detail={emptyDetail}/>;
 return <View role="region" aria-label={name} style={[{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,
  borderRadius:12,overflow:'hidden',minWidth:0},style]}>
  <View style={[s.row,{paddingLeft:12,paddingRight:2,minHeight:44,borderBottomWidth:1,borderColor:C.line,gap:10}]}>
   <View style={{minWidth:0,flexShrink:1}}>
    <T role="heading" aria-level={3} numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:11,lineHeight:16,
     letterSpacing:1,textTransform:'uppercase'}}>{name}</T>
    {!!subtitle&&<T numberOfLines={1} style={{fontSize:11,lineHeight:15,color:C.muted}}>{subtitle}</T>}
   </View>
   {!!onCustomize&&<LinkText label={customizeLabel(filterCount||0)}
    a11y={`Customize ${name}: ${filterCount||0} filters in force. Open the filter builder`} onPress={onCustomize}/>}
   <View style={{flex:1,minWidth:4}}/>
   {!!onRefresh&&<IconButton icon="refresh-cw" size={13} label={`Refresh ${name}`} tooltip="Refresh"
    onPress={onRefresh} style={{width:30,height:30}}/>}
   {!!onExpand&&<IconButton icon={expanded?'minimize-2':'maximize-2'} size={13} pressed={expanded}
    label={`${expanded?'Restore':'Expand'} ${name}`} tooltip={expanded?'Restore':'Expand'} onPress={onExpand}
    style={{width:30,height:30}}/>}
   {!!onClose&&<IconButton icon="x" size={14} label={`Close ${name}`} tooltip="Close" onPress={onClose}
    style={{width:30,height:30}}/>}
  </View>
  {/* the control strip sits OUTSIDE the state switch: an empty reading must never take away the control
      that chooses the reading */}
  {!!toolbar&&<View style={{paddingHorizontal:12,paddingVertical:8,borderBottomWidth:1,borderColor:C.line,
   gap:5}}>{toolbar}</View>}
  <View style={{flex:1,minHeight:0}}>{inner}</View>
  {/* The as-of line is the widget's own, not the page's: two widgets can be a mark apart and must never pretend
      otherwise. The floors sit beside it because a screen without its floors is a junk list (§3). */}
  {footer!==undefined?(footer!==null&&<View style={{borderTopWidth:1,borderColor:C.line,paddingHorizontal:12,
   paddingVertical:7,gap:3}}>{footer}</View>):
   <View style={{borderTopWidth:1,borderColor:C.line,paddingHorizontal:12,paddingVertical:7,gap:3}}>
    <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
     <Icon name="clock" size={11} color={C.muted}/>
     <T style={{fontSize:11,lineHeight:15,color:C.muted}}>{asOfText(body?.as_of)}</T>
    </View>
    <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{floorsText(body?.floors,body?.floors_text)}</T>
    {!!note&&(typeof note==='string'?<T style={{fontSize:10,lineHeight:14,color:C.muted}}>{note}</T>:note)}
    {!!missing&&<T style={{fontSize:10,lineHeight:14,color:C.amber}}>{missing}</T>}
   </View>}
 </View>;
}
