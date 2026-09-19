// The Derivative tab's frame, rebuilt on the TrendSpider dashboard the owner benchmarked: a SECTION title in large
// text with a row of WIDGETS under it, and every widget wearing the same header bar —
//   <name>   Customize (N filters)…                                        ⟳  ⤢  ✕
// Nothing about the numbers changes here (docs/DERIVATIVES_SPEC.md §3–§5 still hold): each widget prints its own
// as-of time and the liquidity floors in force, names any §3 signal the metrics worker has not written yet, and says
// in one plain sentence when it has nothing to show. An empty widget is never an error page.
import React from 'react';
import {View,Pressable,ActivityIndicator,ScrollView} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {IconButton,Popover} from '../layout';
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
// --- one rhythm for a block of panels ----------------------------------------------------------------------
// Three panels beside each other only read as one block when they share a type scale, a padding and a header.
// These are those numbers, in one place, so a panel cannot drift from its neighbour: BODY is every sentence a
// panel puts under its chart, META is the small print beside a number, and PAD is the gutter every panel uses.
export const SCALE={title:11,sub:11,body:11,meta:10,pad:10,gap:6} as const;
// A widget with NO SYMBOL and no row picked yet has one sentence to show, and a chart-sized box around one
// sentence is a hole in the page. It keeps its header, its controls and its place, and takes the height that
// sentence actually needs. A widget that is empty because the DATA is empty is a different thing and keeps
// its full height: that is a reading, and rows will be there at the next one.
export const IDLE_H=190;
export const bodyText={fontSize:SCALE.body,lineHeight:15,color:C.muted};
export const metaText={fontSize:SCALE.meta,lineHeight:14,color:C.muted};

// --- "How to read this": one affordance for a whole block ----------------------------------------------------
// The definitions a block works to are not deleted and they are not pasted over the numbers. They live behind
// ONE control in the block's header, closed on arrival, with the full text unclamped inside. Escape closes it
// and focus returns to the control (Popover does both), and there is no control at all when there is nothing
// to explain — the same rule src/workspace/ChartCentre.tsx works to for its drawing notes.
export type InfoLine={text:string;tone?:'amber'};
export type InfoGroup={heading:string;lines:(InfoLine|string|null|undefined|false)[]};
export function infoGroups(groups:InfoGroup[]){
 return (groups||[]).map(g=>({heading:g.heading,
  lines:(g.lines||[]).map(l=>typeof l==='string'?{text:l}:l)
   .filter((l):l is InfoLine=>!!l&&!!String(l.text||'').trim())})).filter(g=>g.lines.length>0);
}
export function InfoDisclosure({label='How to read this',title,groups}:{label?:string;title:string;
 groups:InfoGroup[]}){
 const [open,setOpen]=React.useState(false);
 const ref=React.useRef<any>(null);
 const shown=infoGroups(groups);
 // no control when there is nothing to show
 if(!shown.length)return null;
 return <>
  <Pressable ref={ref} accessibilityRole="button" accessibilityState={{expanded:open}}
   accessibilityLabel={`${label}: ${title}. ${open?'Hide the definitions':'Read the definitions'}`}
   {...webOnly({'aria-expanded':open,'aria-haspopup':'dialog'})} onPress={()=>setOpen(o=>!o)}
   style={(st:any)=>[s.row,{gap:5,minHeight:30,paddingHorizontal:9,borderRadius:8,borderWidth:1,
    borderColor:open?C.green:C.line,backgroundColor:open||st.hovered||st.focused?C.soft:'transparent'}]}>
   <Icon name="info" size={13} color={open?C.green:C.muted}/>
   <T numberOfLines={1} style={{fontSize:11,lineHeight:15,color:open?C.green:C.muted}}>{label}</T>
  </Pressable>
  <Popover open={open} onClose={()=>setOpen(false)} anchor={ref} placement="bottom-end" label={title} width={400}>
   {/* full text, never clamped: the panel scrolls when the definitions are longer than the room it has */}
   <ScrollView contentContainerStyle={{paddingHorizontal:14,paddingVertical:8,gap:12}}>
    {shown.map(group=><View key={group.heading} style={{gap:4}}>
     <T style={head}>{group.heading}</T>
     {group.lines.map((line,i)=><T key={i} accessibilityRole="text"
      style={{fontSize:12,lineHeight:18,color:line.tone==='amber'?C.amber:C.ink}}>{line.text}</T>)}
    </View>)}
   </ScrollView>
  </Popover>
 </>;
}

// --- the direction chip, in one place -------------------------------------------------------------------------
// Four blocks on this tab now say what a number DID over the last hour of readings, each in its own vocabulary
// (BUILDING/FLAT/UNWINDING · RISING/STABLE/FALLING · SHIFTING UP/STABLE/SHIFTING DOWN · EXPANDING/STABLE/COOLING).
// They all wear the same chip, built here once, so one of them cannot drift into looking like a signal badge.
// An empty label draws NO chip and the words "no baseline" instead: that is a state, not a fourth direction.
export function DirectionChip({label,tone:t='flat',size=9}:{label:string;tone?:'up'|'down'|'flat';size?:number}){
 if(!label)return <T style={{fontSize:size,lineHeight:size+4,color:C.muted}}>no baseline</T>;
 const colour=t==='flat'?C.muted:buildupColor(t);
 return <View style={{alignSelf:'flex-start',paddingHorizontal:5,paddingVertical:1,borderRadius:4,borderWidth:1,
  borderColor:t==='flat'?C.line:colour}}>
  <T style={{fontSize:size,lineHeight:size+4,fontFamily:'InterSemi',color:colour}}>{label}</T></View>;
}
/** A standing label welded to a number — the one this tab needs is COMPUTED, beside every implied volatility.
 *  It is deliberately NOT amber: amber on this tab means a caveat on the data, and "this came from a model"
 *  is a fact about the number, not a fault in it. */
export function Tag({label,a11y}:{label:string;a11y?:string}){
 return <View accessibilityLabel={a11y||label} style={{alignSelf:'flex-start',paddingHorizontal:4,paddingVertical:1,
  borderRadius:3,borderWidth:1,borderColor:C.mint}}>
  <T style={{fontSize:8,lineHeight:11,letterSpacing:.7,fontFamily:'InterSemi',color:C.mint}}>{label}</T></View>;
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
export type SectionProps={title:string;subtitle?:string;stacked?:boolean;
 /** The as-of line for a whole block, printed ONCE here instead of once per panel. */
 asOf?:string;
 /** A short standing fact about the block — the symbol it resolved, and whether that was a default. */
 badge?:string;
 /** The block's "How to read this" control, and any control that acts on every panel in it. */
 info?:React.ReactNode;actions?:React.ReactNode;
 children:React.ReactNode};
/** A section title in large text, then its row of widgets. Below ~1100px the row becomes a column, so the chart
 *  tile drops under the table instead of squeezing it.
 *
 *  A block that carries asOf/info/actions prints them HERE, once: three panels a reading apart would each need
 *  their own as-of, but three panels driven by ONE read share one, and repeating it three times is noise. */
export function Section({title,subtitle,stacked,asOf,badge,info,actions,children}:SectionProps){
 const aside=!!asOf||!!badge||!!info||!!actions;
 return <View style={{gap:8}}>
  <View style={{flexDirection:'row',alignItems:'center',flexWrap:'wrap',gap:10}}>
   <View style={{gap:2,flexShrink:1,minWidth:0}}>
    <T role="heading" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:19,lineHeight:26,letterSpacing:-.3}}>{title}</T>
    {!!subtitle&&<T style={{fontSize:12,lineHeight:17,color:C.muted}}>{subtitle}</T>}
   </View>
   {/* One group, not four loose children: on a narrow page the whole aside drops under the title and
       stays aligned to one edge, instead of each item wrapping onto a line of its own. */}
   {aside&&<View style={{flex:1,minWidth:190,flexDirection:'row',flexWrap:'wrap',alignItems:'center',
    justifyContent:'flex-end',gap:10}}>
    {/* Pill pins itself to the top of whatever it is in, which left the badge riding above the as-of
        beside it. Wrapped, it sits on the same line as everything else in the header. */}
    {!!badge&&<View><Pill label={badge}/></View>}
    {!!asOf&&<View style={[s.row,{gap:5}]}><Icon name="clock" size={12} color={C.muted}/>
     <T numberOfLines={1} style={{fontSize:11,lineHeight:15,color:C.muted}}>{asOf}</T></View>}
    {info}
    {actions}
   </View>}
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
 /** This widget is one panel of a block whose Section header carries the as-of, the floors and the block's
  *  one "How to read this". The panel then keeps only what is its OWN: a tighter header, and a footer that
  *  prints a data-bearing line when it has one and nothing at all when it does not. */
 inBlock?:boolean;
 children?:React.ReactNode;
 style?:any;
};
export function WidgetFrame({name,subtitle,body,state,showsSignals,filterCount,onCustomize,onRefresh,onExpand,
 expanded,onClose,toolbar,note,emptyDetail,footer,inBlock,children,style}:WidgetFrameProps){
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
 // In a block the header gives every pixel it can to the NAME: the controls shrink, the gutters tighten and
 // the "Customize…" link moves to the block header, where one copy serves all three panels. That is what
 // stops a title being cut short in a narrow panel — the layout gives way, not the characters.
 const btn=inBlock?26:30,pad=inBlock?10:12,gap=inBlock?6:10,bar=inBlock?38:44;
 return <View role="region" aria-label={name} style={[{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,
  borderRadius:12,overflow:'hidden',minWidth:0},style]}>
  <View style={[s.row,{paddingLeft:pad,paddingRight:2,minHeight:bar,borderBottomWidth:1,borderColor:C.line,gap}]}>
   <View style={{minWidth:0,flexShrink:1}}>
    <T role="heading" aria-level={3} numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:11,lineHeight:16,
     letterSpacing:1,textTransform:'uppercase'}}>{name}</T>
    {!!subtitle&&<T numberOfLines={1} style={{fontSize:11,lineHeight:15,color:C.muted}}>{subtitle}</T>}
   </View>
   {!!onCustomize&&<LinkText label={customizeLabel(filterCount||0)}
    a11y={`Customize ${name}: ${filterCount||0} filters in force. Open the filter builder`} onPress={onCustomize}/>}
   <View style={{flex:1,minWidth:4}}/>
   {!!onRefresh&&<IconButton icon="refresh-cw" size={13} label={`Refresh ${name}`} tooltip="Refresh"
    onPress={onRefresh} style={{width:btn,height:btn}}/>}
   {!!onExpand&&<IconButton icon={expanded?'minimize-2':'maximize-2'} size={13} pressed={expanded}
    label={`${expanded?'Restore':'Expand'} ${name}`} tooltip={expanded?'Restore':'Expand'} onPress={onExpand}
    style={{width:btn,height:btn}}/>}
   {!!onClose&&<IconButton icon="x" size={14} label={`Close ${name}`} tooltip="Close" onPress={onClose}
    style={{width:btn,height:btn}}/>}
  </View>
  {/* the control strip sits OUTSIDE the state switch: an empty reading must never take away the control
      that chooses the reading */}
  {!!toolbar&&<View style={{paddingHorizontal:pad,paddingVertical:inBlock?6:8,borderBottomWidth:1,
   borderColor:C.line,gap:5}}>{toolbar}</View>}
  <View style={{flex:1,minHeight:0}}>{inner}</View>
  {/* The as-of line is the widget's own, not the page's: two widgets can be a 15-min reading apart and must
      never pretend otherwise. The floors sit beside it because a screen without its floors is a junk list
      (§3). A panel inside a block is the exception: its block resolved ONE symbol and reads it once, so the
      as-of and the floors are printed once in the block header and this footer keeps only its own line. */}
  {inBlock?((!!note||!!missing)&&<View style={{borderTopWidth:1,borderColor:C.line,paddingHorizontal:pad,
   paddingVertical:6,gap:3}}>
    {!!note&&(typeof note==='string'?<T style={metaText}>{note}</T>:note)}
    {!!missing&&<T style={{...metaText,color:C.amber}}>{missing}</T>}
   </View>):
   footer!==undefined?(footer!==null&&<View style={{borderTopWidth:1,borderColor:C.line,paddingHorizontal:12,
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
