// The Derivative tab's frame, rebuilt on the TrendSpider dashboard the owner benchmarked: a SECTION title in large
// text with a row of WIDGETS under it, and every widget wearing the same header bar —
//   <name>   Customize (N filters)…                                        ⟳  ⤢  ✕
// Nothing about the numbers changes here (docs/DERIVATIVES_SPEC.md §3–§5 still hold): each widget prints its own
// as-of time and the liquidity floors in force, names any §3 signal the metrics worker has not written yet, and says
// in one plain sentence when it has nothing to show. An empty widget is never an error page.
import React from 'react';
import {View,Pressable,ActivityIndicator,ScrollView,Platform} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {IconButton,Popover} from '../layout';
import {CenterNote,LinkText,webOnly} from '../discover/parts';
import {asOfText,cardState,customizeLabel,floorsText,missingText,sourceText,type CardState} from './logic';
import type {CaptureHealth,CaptureState,Envelope} from './types';
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

// --- DERIVATIVE CAPTURE HEALTH, BESIDE THE PANELS IT AFFECTS -------------------------------------------------
// WHY IT IS HERE AND NOT IN THE PAGE HEADER ALONE. On 18 Sep 2026 the F&O capture died at 11:30 and the rest
// of the session was rebuilt from 15-minute candles, which carry no traded-price average and no underlying
// price. Every contract row of the 15:45 reading had a null premium and a null spot: nothing in it could be
// measured against the liquidity floors at all. The page-wide status chip went on saying prices and patterns
// were healthy, because that chip describes the CASH feed and knows nothing about F&O coverage - and the
// screener under it said "No contract cleared the liquidity floors", which reads as a quiet market.
//
// So the derivative tab carries its OWN status, it sits beside the panels the outage affects, and the two
// facts are never merged: what was MEASURED is one sentence, what the measurements SAY is another.
export type CaptureTone='amber'|'green'|'muted';
export type CaptureView={state:CaptureState;label:string;tone:CaptureTone;detail:string;lines:string[]};
/** The reader-facing word for each of the five states. A partial capture is NEVER shown as healthy. */
const CAPTURE_LABELS:Record<CaptureState,string>={
 missing_capture:'Nothing captured',
 partial_capture:'Partial capture',
 complete:'Capture complete',
 no_eligible_rows:'Capture complete',
 filtered_out:'Capture complete',
 failed:'Store unavailable',
};
/** Amber is a caveat on the DATA, and only the three states that are one get it. */
const CAPTURE_TONES:Record<CaptureState,CaptureTone>={
 missing_capture:'amber',partial_capture:'amber',failed:'amber',
 complete:'green',no_eligible_rows:'green',filtered_out:'green',
};
/** Turn the server's capture health into the chip and the lines behind it. Nothing here invents a number:
 *  every count and every sentence comes off the response, and a field the store does not carry is absent
 *  rather than reported as zero coverage. */
export function captureView(capture?:CaptureHealth|null):CaptureView|null{
 if(!capture||!capture.state)return null;
 const state=capture.state;
 const lines:string[]=[capture.state_text].filter(Boolean);
 // WHAT WAS MEASURED, counted. "10,552 rows, 0 with premium traded" is the fact the old wording hid.
 const rows=Number(capture.rows)||0;
 const counted=(capture.required_fields||[]).concat(['spot'])
  .map(key=>({key,entry:(capture.coverage||{})[key]}))
  .filter(f=>f.entry&&f.entry.column)
  .map(f=>`${f.entry.label}: ${Number(f.entry.present||0).toLocaleString()} of ${rows.toLocaleString()}`);
 if(rows&&counted.length)lines.push(`At this 15-min reading — ${counted.join(' · ')}.`);
 if(capture.source==='candles_15m')lines.push('These readings were rebuilt from 15-minute candles. A candle '
  +'carries no traded-price average and no underlying price, so nothing in them can be measured against a '
  +'premium floor.');
 if(capture.cleared!=null&&capture.matched!=null&&capture.cleared>0&&capture.matched===0)
  lines.push(`${capture.cleared.toLocaleString()} contracts cleared the floors here; the filters in force `
   +'excluded every one of them.');
 // The LATEST COMPLETE reading is named, never swapped in. The reader chooses.
 if(capture.latest_complete_at&&!capture.latest_complete_is_here)
  lines.push(`The newest reading with every required field captured is ${asOfText(capture.latest_complete_at)}. `
   +'Choose it above to move the tab there — nothing is substituted for you.');
 else if(!capture.latest_complete_at&&state!=='complete')
  lines.push('No recent reading has every required field captured.');
 if(capture.capture_text)lines.push(capture.capture_text);
 return {state,label:CAPTURE_LABELS[state]||state,tone:CAPTURE_TONES[state]||'muted',
  detail:capture.state_text||'',lines};
}
/** The tab's one capture health, read by every Block without being threaded through seven components. */
export const CaptureContext=React.createContext<CaptureHealth|null>(null);
/** The compact chip. Amber carries the caveat; the full sentences sit one click behind it. */
export function CaptureChip({capture,compact}:{capture?:CaptureHealth|null;compact?:boolean}){
 const view=captureView(capture);
 const [open,setOpen]=React.useState(false);
 const ref=React.useRef<any>(null);
 if(!view)return null;
 const colour=view.tone==='amber'?C.amber:view.tone==='green'?C.green:C.muted;
 return <>
  <Pressable ref={ref} accessibilityRole="button" accessibilityState={{expanded:open}}
   accessibilityLabel={`F&O capture: ${view.label}. ${view.detail} ${open?'Hide the detail':'Read the detail'}`}
   {...webOnly({'aria-expanded':open,'aria-haspopup':'dialog'})} onPress={()=>setOpen(o=>!o)}
   style={(st:any)=>[s.row,{gap:5,minHeight:compact?22:26,paddingHorizontal:7,borderRadius:7,borderWidth:1,
    borderColor:colour,backgroundColor:open||st.hovered||st.focused?C.soft:'transparent'}]}>
   <Icon name={view.tone==='amber'?'alert-triangle':'check-circle'} size={compact?11:12} color={colour}/>
   <T numberOfLines={1} style={{fontSize:compact?10:11,lineHeight:compact?14:15,fontFamily:'InterSemi',
    color:colour}}>{view.label}</T>
  </Pressable>
  <Popover open={open} onClose={()=>setOpen(false)} anchor={ref} placement="bottom-end"
   label="F&O capture health" width={420}>
   <ScrollView contentContainerStyle={{paddingHorizontal:14,paddingVertical:10,gap:8}}>
    {/* WHICH reading this health describes. Without it the chip is a claim with no time on it, and a
        reader cannot tell the reading on screen from the newest one the store holds. */}
    <T style={head}>{capture?.at?`F&O capture at ${asOfText(capture.at)}`:'F&O capture'}</T>
    {view.lines.map((line,i)=><T key={i} accessibilityRole="text"
     style={{fontSize:12,lineHeight:18,color:i===0&&view.tone==='amber'?C.amber:C.ink}}>{line}</T>)}
   </ScrollView>
  </Popover>
 </>;
}
/** The chip a BLOCK shows. Only a state that is a caveat on the data reaches a block header: a complete
 *  reading needs no badge on nine panels, and a badge on every panel is a badge nobody reads. */
export function BlockCaptureChip(){
 const capture=React.useContext(CaptureContext);
 if(!capture||capture.healthy!==false)return null;
 return <CaptureChip capture={capture} compact/>;
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

// =================================================================================================================
// ONE TEMPLATE FOR THE WHOLE TAB
//
// The owner, on the version where every section had invented its own shape: "everything is wrong - I asked for
// same block template across for all blocks right?" He was right. Unusual activity was [screener][chart], the
// chain was [chain][chart], OI by strike was [bars][bars], the delta-OI block was [screener][futures chart][grid],
// the session blocks were [chart][readings] and futures build-up was three panels. Different panel counts,
// different widths, different internals, and seven copies of one screener each eating a quarter of the page.
//
// There is now ONE skeleton and every block on the tab is drawn through it:
//
//     <title>  <headline figure>                      [symbol] [as-of] [How to read this] [Customize...]
//     [--------------- Chart ---------------][---------------- Content -----------------------]
//
// Identical widths, identical height, identical header treatment, identical padding and type scale. The ONLY
// thing that changes between blocks is what sits in the two panels - and the screener is not one of them: it is
// pinned once at the top of the tab and drives every block from there.
//
// The geometry lives HERE and nowhere else. A block does not choose its own height, its own widths or its own
// breakpoint; it is HANDED the style for each panel and applies it. That is what makes "the same template"
// something the code enforces rather than something seven files agree about until one of them is edited.
// =================================================================================================================
/** The one height every block takes. Set by the tallest content panel the tab has - the delta-OI grid's two
 *  bands of five tiles - so that no block scrolls its content away and every block still ends on the same line. */
export const BLOCK_H=530;
/** The content panel carries the block's substance (the grid, the chain, the strikes, the readings); the chart
 *  beside it is support. So the content takes the larger share, and the grid still gets five tiles across. */
export const BLOCK_CHART_FLEX=1,BLOCK_CONTENT_FLEX=1.5;
/** ONE breakpoint for the whole tab. Above it every block is two panels side by side; below it every block is
 *  two panels stacked. There is no second breakpoint and no block with a wider one: that was the old shape. */
export const BLOCK_BESIDE=1200;
/** Below this the narrow tables keep their first column pinned while the rest scrolls sideways. */
export const BLOCK_PIN_COLUMN=760;
/** The gutter between the two panels, and the gap under the block header. */
export const BLOCK_GAP=12;
export type PaneStyle={flex?:number;minWidth?:number;height:number};
/** The two panel styles of a block, built in one place from one height. */
export function paneStyles(stacked:boolean,height:number):{chart:PaneStyle;content:PaneStyle}{
 return stacked
  ?{chart:{height},content:{height}}
  :{chart:{flex:BLOCK_CHART_FLEX,minWidth:0,height},content:{flex:BLOCK_CONTENT_FLEX,minWidth:0,height}};
}

// --- the headline figure ------------------------------------------------------------------------------------
// The owner again: "the layout asthetic has to be so intutive". Every number on the old tab carried the same
// weight, so the eye landed nowhere and the reader had to work out what a block was FOR by reading a sentence.
// Every block now opens with one figure at a size nothing else on it comes close to, and that figure is never
// bare: it carries the comparison that gives it meaning. "23,350" says nothing. "23,350, 4 below spot 23,346"
// says something, and says only what was captured.
//
// It is a DESCRIPTION of the captured number and nothing else (section 5). No lean, no level, no call. When the
// figure was not captured it is a dash and the reason stands where the comparison would be - never a zero, and
// never a headline that quietly reads as a value.
export type HeadlineProps={
 /** What the figure IS, in the block's own words. Small, uppercase, above the number. */
 label:string;
 value:string;
 /** The comparison that gives the figure its meaning - the other number it is read against. */
 against?:string;
 /** What the figure DID across the readings behind it. Never a forecast; the server's own word. */
 chip?:string;chipTone?:'up'|'down'|'flat';
 /** A standing label welded to the number. The only one this tab has is COMPUTED. */
 tag?:string;
 /** Why there is no figure. Printed instead of `against` when the value is a dash. */
 reason?:string;
};
export function Headline({label,value,against,chip,chipTone,tag,reason}:HeadlineProps){
 const missing=!value||value===HEADLINE_DASH;
 const spoken=missing?(reason||'not captured')
  :`${value}${tag?` ${tag}`:''}${against?`. ${against}`:''}${chip?`. ${chip}`:''}`;
 return <View accessibilityRole="text" accessibilityLabel={`${label}: ${spoken}`}
  style={[s.row,{gap:14,flexWrap:'wrap',paddingHorizontal:14,paddingVertical:10,borderRadius:12,
   borderWidth:1,borderColor:C.line,backgroundColor:C.dark}]}>
  <View style={{gap:1,minWidth:0}}>
   <T numberOfLines={1} style={head}>{label}</T>
   <View style={[s.row,{gap:7}]}>
    <T numberOfLines={1} style={{fontFamily:'ManropeBold',fontSize:26,lineHeight:32,letterSpacing:-0.6,
     color:missing?C.muted:C.ink,fontVariant:['tabular-nums'] as any}}>{value}</T>
    {!!tag&&!missing&&<Tag label={tag}/>}
   </View>
  </View>
  {/* the comparison, or - when there is no figure - the reason there is none. Never both, never neither. */}
  <T numberOfLines={2} style={{flex:1,minWidth:150,fontSize:12,lineHeight:17,color:C.muted}}>
   {missing?(reason||''):(against||'')}</T>
  {!missing&&!!chip&&<DirectionChip label={chip} tone={chipTone} size={10}/>}
 </View>;
}
const HEADLINE_DASH='—';

/** The symbol every block is pointed at, drawn the SAME way in the pinned screener and in every block header,
 *  so the reader SEES the link rather than having to read seven subtitles to confirm it. A symbol the reader
 *  picked is mint and carries a live dot; a symbol nobody chose says it is a default and stays grey. */
export function SymbolPill({label,linked,large}:{label:string;linked?:boolean;large?:boolean}){
 if(!label)return null;
 return <View style={[s.row,{gap:5,alignSelf:'flex-start',paddingHorizontal:large?10:7,
  paddingVertical:large?4:2,borderRadius:7,borderWidth:1,
  borderColor:linked?C.green:C.line,backgroundColor:linked?C.soft:C.bg}]}>
  {!!linked&&<View style={{width:5,height:5,borderRadius:3,backgroundColor:C.green}}/>}
  <T numberOfLines={1} style={{fontSize:large?12:10,lineHeight:large?17:14,fontFamily:'InterSemi',
   color:linked?C.green:C.muted}}>{label}</T>
 </View>;
}

// --- the block ------------------------------------------------------------------------------------------------
export type BlockProps={
 title:string;subtitle?:string;
 /** The block's one as-of line and the symbol it is pointed at, printed once here rather than once per panel. */
 asOf?:string;badge?:string;
 /** True when this block is showing the symbol the reader picked in the pinned screener rather than a default.
  *  It lights the symbol pill and draws the mint rail down the left of the block, which is what makes the link
  *  between the screener and everything under it VISIBLE. */
 linked?:boolean;
 headline?:HeadlineProps|null;
 info?:React.ReactNode;actions?:React.ReactNode;
 stacked?:boolean;
 /** Only ever set when a widget of this block is expanded to fill the page. Every other block is BLOCK_H. */
 height?:number;
 /** Both panels are showing one sentence, so the block collapses to the height of one sentence rather than
  *  holding two chart-sized voids. They collapse TOGETHER: a block where one panel shrank and the other did
  *  not reads worse than the hole it was meant to close. */
 idle?:boolean;
 chart:(style:PaneStyle)=>React.ReactNode;
 content:(style:PaneStyle)=>React.ReactNode;
};
export function Block({title,subtitle,asOf,badge,linked,headline,info,actions,stacked,height,idle,
 chart,content}:BlockProps){
 const h=height||(idle?IDLE_H:BLOCK_H);
 const panes=paneStyles(!!stacked,h);
 const left=chart(panes.chart),right=content(panes.content);
 if(!left&&!right)return null;
 // The F&O capture caveat rides in EVERY block header, beside the as-of it qualifies, without any block
 // having to know it exists. A block cannot show a healthy-looking as-of through a capture outage.
 const capture=React.useContext(CaptureContext);
 const flagged=!!capture&&capture.healthy===false;
 const aside=!!asOf||!!badge||!!info||!!actions||flagged;
 return <View style={{gap:8}}>
  <View style={{flexDirection:'row',alignItems:'flex-start',flexWrap:'wrap',gap:10}}>
   <View style={{gap:2,flexShrink:1,minWidth:0}}>
    <T role="heading" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:19,lineHeight:26,
     letterSpacing:-0.3}}>{title}</T>
    {!!subtitle&&<T style={{fontSize:12,lineHeight:17,color:C.muted}}>{subtitle}</T>}
   </View>
   {aside&&<View style={{flex:1,minWidth:190,flexDirection:'row',flexWrap:'wrap',alignItems:'center',
    justifyContent:'flex-end',gap:10}}>
    {!!badge&&<View><SymbolPill label={badge} linked={linked}/></View>}
    {!!asOf&&<View style={[s.row,{gap:5}]}><Icon name="clock" size={12} color={C.muted}/>
     <T numberOfLines={1} style={{fontSize:11,lineHeight:15,color:C.muted}}>{asOf}</T></View>}
    <BlockCaptureChip/>
    {info}
    {actions}
   </View>}
  </View>
  {!!headline&&<Headline {...headline}/>}
  {/* the mint rail: one glance down the page says every block under the screener is on the same symbol */}
  <View style={[s.row,{alignItems:'stretch',gap:BLOCK_GAP}]}>
   {!!linked&&<View style={{width:2,borderRadius:2,backgroundColor:C.green,opacity:0.55}}/>}
   <View style={{flex:1,minWidth:0,flexDirection:stacked?'column':'row',alignItems:'stretch',gap:BLOCK_GAP}}>
    {left}{right}
   </View>
  </View>
 </View>;
}

// --- hover to read a value off a chart --------------------------------------------------------------------
// "Hovering a chart should tell you the values at that moment." Four blocks on this tab draw a time series
// against the same session grid, and until now a reader could see the shape and not one number on it.
//
// One hook, because four copies of pointer-to-index arithmetic is four places for the off-by-one to come back.
// It is the same mechanism src/PatternCanvas.tsx already uses for its candle cursor: at most one update per
// animation frame, touch left alone (a finger has no hover, and a tap is a different gesture), and the index
// clamped to the series so a cursor can never point at a reading that is not there.
//
// What it does NOT do: invent a value. The caller reads its own series at the index this returns, and a slot
// the store has no value for reads as a dash exactly as it does everywhere else on the tab. A crosshair over a
// gap says there is nothing there - it never bridges one.
export function useHoverIndex(indexAt:(x:number,boxWidth:number)=>number|null){
 const [cursor,setCursor]=React.useState<number|null>(null);
 const resolve=React.useRef(indexAt);resolve.current=indexAt;
 const frame=React.useRef(0),at=React.useRef<{target:any;clientX:number}|null>(null);
 React.useEffect(()=>()=>{if(frame.current)cancelAnimationFrame(frame.current)},[]);
 const hover=React.useMemo(()=>Platform.OS!=='web'?{}:{
  onPointerMove:(e:any)=>{
   const native=e?.nativeEvent||e;
   if(native?.pointerType==='touch')return;
   at.current={target:e.currentTarget,clientX:native.clientX};
   if(frame.current)return;
   frame.current=requestAnimationFrame(()=>{
    frame.current=0;
    const here=at.current,box=here?.target?.getBoundingClientRect?.();
    if(!here||!box||!Number.isFinite(here.clientX))return;
    const index=resolve.current(here.clientX-box.left,box.width);
    setCursor(v=>(index==null?null:index===v?v:index));
   });
  },
  onPointerLeave:(e:any)=>{
   const native=e?.nativeEvent||e;
   if(native?.pointerType==='touch')return;
   if(frame.current){cancelAnimationFrame(frame.current);frame.current=0}
   at.current=null;setCursor(null);
  },
 },[]);
 return {cursor,hover};
}

/** The one readout a hovered chart puts over itself: the reading being pointed at, and what each line held at
 *  it. A line with no value at that reading reads as a dash - the crosshair never bridges a gap. */
export function CrosshairReadout({time,rows}:{time:string;rows:{label:string;value:string;color:string;
 tag?:string}[]}){
 return <View style={[s.row,{gap:10,flexWrap:'wrap',paddingHorizontal:8,paddingVertical:4,borderRadius:8,
  borderWidth:1,borderColor:C.line,backgroundColor:C.dark}]}>
  <View style={[s.row,{gap:4}]}><Icon name="clock" size={10} color={C.green}/>
   <T style={{fontSize:10,lineHeight:14,fontFamily:'InterSemi',color:C.green,
    fontVariant:['tabular-nums'] as any}}>{time}</T></View>
  {rows.map(row=><View key={row.label} style={[s.row,{gap:5}]}>
   <View style={{width:8,height:2,backgroundColor:row.color}}/>
   <T numberOfLines={1} style={[metaText,{color:C.ink,fontVariant:['tabular-nums'] as any}]}>
    {row.label} {row.value}{row.tag?` ${row.tag}`:''}</T>
  </View>)}
 </View>;
}
