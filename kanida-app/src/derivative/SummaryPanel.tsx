// BLOCK 1, RIGHT PANE — what the selected instrument is doing, in plain language.
//
// It stands to the RIGHT of the signal table, beside the screener that chooses the instrument, and is built on
// the SAME read: `gridRead`, the tab's one ΔOI-grid fetch. No second request, no second selection, no second
// search, and nothing that was on this row before has moved: the screener still chooses the instrument and
// the signal table still stands between them, showing the very numbers these sentences are made of.
//
// WHAT IT MAY SAY is decided in summary.ts, which holds the observations. This file holds no rule, no
// threshold and no wording that is not handed to it: its whole job is layout, the reveal, and the links back
// into the strikes the sentences name.
import React,{useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {View,Pressable,ScrollView,Platform} from 'react-native';
import Svg,{Path} from 'react-native-svg';
import {C,T,Icon,s} from '../ui';
import {webOnly} from '../discover/parts';
import {useReducedMotion} from '../falcon/useFalconScan';
import type {Read} from './useDerivatives';
import {InfoDisclosure,WidgetFrame,head,metaText,stateOf,type InfoGroup} from './frame';
import {SIGNAL_WINDOW_MINUTES,asOfText,clock,compact,strike as strikeText,type ReadingChoice} from './logic';
import {CAVEAT,crossMarket,narrate,observe,phrases,spotMove,type Narrative,type Rung} from './summary';
import {standing as standingOf,type Standing} from './signal';
import type {ChartTarget,OiGrid} from './types';

export {SIGNAL_W} from './SignalTable';
export const SUMMARY_EMPTY='No 15-min reading of these ten contracts has been captured yet.';
export const SUMMARY_NO_SYMBOL='Click a row on the screener to read what that instrument is doing.';
/** Each phrase is held this long before the next one joins it. Three phrases land inside ~2 seconds and the
 *  underline closes the sequence under four — the band the brief asks for, and short enough that a reader who
 *  ignores it has the whole text before they finish the headline. */
const PHRASE_MS=560,UNDERLINE_MS=620;

/** A restrained hand-drawn underline. The anchors are exact; the ink between them is not. It is drawn under
 *  the ONE thing the sentence is about — the busiest strike — and nowhere near the market data itself. */
function handPath(width:number,seed=11){
 let s=seed;const rnd=()=>{s=(s*16807)%2147483647;return s/2147483647-0.5;};
 const steps=Math.max(3,Math.round(width/16));
 const pts:[number,number][]=[];
 for(let i=0;i<=steps;i++){
  const t=i/steps;
  pts.push([1+t*(width-2),5+rnd()*1.7*Math.sin(Math.PI*t)]);
 }
 let d=`M${pts[0][0].toFixed(1)} ${pts[0][1].toFixed(1)}`;
 for(let i=1;i<pts.length-1;i++){
  const mx=(pts[i][0]+pts[i+1][0])/2,my=(pts[i][1]+pts[i+1][1])/2;
  d+=` Q${pts[i][0].toFixed(1)} ${pts[i][1].toFixed(1)} ${mx.toFixed(1)} ${my.toFixed(1)}`;
 }
 const last=pts[pts.length-1];
 return d+` L${last[0].toFixed(1)} ${last[1].toFixed(1)}`;
}

function Underline({width,progress}:{width:number;progress:number}){
 const d=useMemo(()=>handPath(width),[width]);
 if(width<=0||progress<=0)return null;
 return <Svg width={width} height={11} style={{marginTop:1}}>
  <Path d={d} stroke={C.green} strokeWidth={1.8} fill="none" strokeLinecap="round"
   strokeDasharray={`${width*1.15}`} strokeDashoffset={width*1.15*(1-progress)}/>
 </Svg>;
}

/** A strike the sentences name, wired to the highlight and the chart the rest of the tab already share. */
function StrikeChip({value,side,slot,onHighlight,onTarget,lit}:{value:number;side:string;
 slot?:{tradingsymbol:string|null;instrument_token:number|null;label:string}|null;
 onHighlight?:(v:number|null)=>void;onTarget?:(t:ChartTarget)=>void;lit?:boolean}){
 const label=`${strikeText(value)} ${side}`;
 return <Pressable accessibilityRole="button"
  accessibilityLabel={`${label}. Highlight this strike across the tab and draw it on the chart`}
  onPress={()=>{
   onHighlight?.(value);
   if(onTarget&&slot?.instrument_token!=null)
    onTarget({underlying:'',instrumentToken:slot.instrument_token,label:slot.tradingsymbol||label,
     detail:slot.label||''} as ChartTarget);
  }}
  onHoverIn={()=>onHighlight?.(value)} onHoverOut={()=>onHighlight?.(null)}
  style={(st:any)=>[s.row,{gap:4,minHeight:22,paddingHorizontal:7,borderRadius:6,borderWidth:1,
   borderColor:lit||st.hovered||st.focused?C.green:C.line,
   backgroundColor:lit||st.hovered||st.focused?C.soft:'transparent'}]}>
  <T numberOfLines={1} style={{fontSize:10,lineHeight:14,fontFamily:'InterSemi',
   color:lit?C.green:C.ink,fontVariant:['tabular-nums'] as any}}>{label}</T>
 </Pressable>;
}

/** THE LADDER. One rung per strike the grid holds, measured over the SAME interval the sentence above is
 *  measured over, and labelled with it — a bar with no unit is unreadable, and one that quietly mixes the
 *  interval change with the session change is worse.
 *
 *  Spot sits where its PRICE sits, between the two strikes it falls between. Three kinds of nothing are kept
 *  apart: a rung that moved, a rung measured that did not move, and a rung the capture never reached. */
function Ladder({rungs,spot,window,highlight,onHighlight,onTarget,slotOf}:{
 rungs:Rung[];spot:number|null;window:string;highlight?:number|null;
 onHighlight?:(v:number|null)=>void;onTarget?:(t:ChartTarget)=>void;
 slotOf:(v:number)=>any}){
 const rows=useMemo(()=>rungs.slice().sort((a,b)=>b.strike-a.strike),[rungs]);
 const scale=useMemo(()=>Math.max(1,...rows.map(r=>Math.abs(Number(r.oi_added)||0))),[rows]);
 if(!rows.length)return null;
 return <View style={{gap:3}}>
  <T style={head}>Open interest added · {window}</T>
  {rows.map((r,i)=>{
   const next=rows[i+1];
   // spot's own place on the ladder: below this rung and above the next one
   const showSpot=spot!=null&&r.strike>spot&&(!next||next.strike<=spot);
   const mag=Math.abs(Number(r.oi_added)||0)/scale;
   const lit=highlight===r.strike;
   const bar=r.status==='added'?C.green:r.status==='reduced'?C.red:C.line;
   return <React.Fragment key={r.strike}>
    <Pressable accessibilityRole="button"
     accessibilityLabel={`${strikeText(r.strike)} ${r.side}. `+(
      r.status==='not_captured'?'Not captured at this reading.'
      :r.status==='no_change'?'Measured, no material change this interval.'
      :`Open interest ${r.status==='added'?'added':'reduced'} ${compact(Math.abs(Number(r.oi_added)||0))}`
       +(r.joined?', new this interval':r.since?`, in this episode since ${clock(r.since)} IST`:'')+'.')
      +' Highlight this strike and draw it on the chart'}
     onPress={()=>{onHighlight?.(r.strike);
      const slot=slotOf(r.strike);
      if(onTarget&&slot?.instrument_token!=null)
       onTarget({underlying:'',instrumentToken:slot.instrument_token,
        label:slot.tradingsymbol||`${strikeText(r.strike)} ${r.side}`,detail:slot.label||''} as ChartTarget);}}
     onHoverIn={()=>onHighlight?.(r.strike)} onHoverOut={()=>onHighlight?.(null)}
     style={(st:any)=>[s.row,{gap:6,minHeight:18,paddingHorizontal:3,borderRadius:4,
      backgroundColor:lit||st.hovered||st.focused?C.soft:'transparent'}]}>
     <T style={{width:54,fontSize:10,lineHeight:14,textAlign:'right',
      color:lit?C.green:C.muted,fontVariant:['tabular-nums'] as any}}>{strikeText(r.strike)}</T>
     <View style={{flex:1,minWidth:0,height:8,justifyContent:'center'}}>
      {r.status==='not_captured'
       ? <T style={{fontSize:9,lineHeight:12,color:C.amber}}>not captured</T>
       : r.status==='no_change'
        ? <View style={{width:5,height:2,borderRadius:1,backgroundColor:C.line}}/>
        : <View style={{width:`${Math.max(6,mag*100)}%`,height:8,borderRadius:2,backgroundColor:bar,
           opacity:lit?1:0.85}}/>}
     </View>
     <T numberOfLines={1} style={{width:104,fontSize:9,lineHeight:12,color:C.muted}}>
      {r.status==='not_captured'?''
       :r.status==='no_change'?'no change'
       :r.joined?'new this interval'
       :r.since?`since ${clock(r.since)}`:''}</T>
    </Pressable>
    {showSpot&&<View style={[s.row,{gap:6,paddingHorizontal:3}]}>
     <T style={{width:54,fontSize:9,lineHeight:12,textAlign:'right',color:C.amber,
      fontVariant:['tabular-nums'] as any}}>spot</T>
     <View style={{flex:1,height:1,backgroundColor:C.amber,opacity:0.55}}/>
     <T style={{width:104,fontSize:9,lineHeight:12,color:C.amber,
      fontVariant:['tabular-nums'] as any}}>{strikeText(spot)}</T>
    </View>}
   </React.Fragment>;
  })}
  <T style={metaText}>Bar length is the position change over this interval. A short mark is a strike that was
   {' '}measured and did not move; amber is a strike the capture did not reach.</T>
 </View>;
}

export type SummaryPanelProps={
 symbol:string;expiry?:string;
 /** The TAB's one ΔOI-grid read, shared with the ΔOI block and with the signal table behind this panel. */
 read:Read<OiGrid>;
 /** THE READING IS CHOSEN ON THE SCREENER, two panes to the left and always on screen. This panel had its
  *  own strip of reading chips and it was a second control for one job — in a panel this tall it cost two
  *  rows of the explanation to duplicate a control the reader can already see. */
 /** The two whole-book series the tab already serves, handed down rather than fetched again here. */
 pcr?:{points?:unknown[]}|null;futures?:{points?:unknown[]}|null;
 /** The tab-wide strike highlight and the tab's one linked chart. */
 highlight?:number|null;onHighlight?:(v:number|null)=>void;onTarget?:(t:ChartTarget)=>void;
 /** Where positions stand at this reading (oi-by-strike), for when there is nothing to compare yet. */
 standing?:any;
 style?:any;onExpand?:()=>void;expanded?:boolean};

/** WHERE POSITIONS STAND, in sentences. The pane's answer until the hour-wide comparison exists: the middle pane
 *  carries the same reading as numbers, this one says it. Levels at one stamp; the one comparison is labelled. */
function StandingStory({s,after}:{s:Standing;after:string}){
 return <View style={{gap:8}}>
  <View style={{gap:2}}>
   <T style={head}>Where positions stand · {clock(s.at)} IST</T>
   <T role="heading" aria-level={3} style={{fontFamily:'ManropeBold',fontSize:16,lineHeight:22,letterSpacing:-0.2,
    color:C.ink}}>{s.headline}</T>
  </View>
  {s.lines.map((l,i)=><T key={i} style={{fontSize:12,lineHeight:18,color:i===0?C.ink:C.muted}}>{l}</T>)}
  <T style={{fontSize:11,lineHeight:16,color:C.amber}}>{`${s.caveat} ${after}`}</T>
 </View>;
}

export function SummaryPanel({symbol,expiry,read,pcr,futures,highlight,onHighlight,onTarget,standing,
 style,onExpand,expanded}:SummaryPanelProps){
 const body=read.data;
 const state=symbol?stateOf(read,body?.not_enough_marks||SUMMARY_EMPTY)
  :{phase:'empty' as const,text:SUMMARY_NO_SYMBOL};
 const reduce=useReducedMotion();
 const [quiet,setQuiet]=useState(false);
 const [history,setHistory]=useState(false);
 const still=reduce||quiet;

 // ONE observation walk over the read already on screen. Nothing is fetched here.
 const timeline=useMemo(()=>observe(body),[body]);
 const current=timeline.length?timeline[timeline.length-1]:null;
 // NOTHING TO COMPARE YET — the day's first reading (no grid rows), or a session without an hour behind it. Asked
 // for live 21 Sep: the pane read "not enough readings" while the book's levels were fully known.
 const stand=useMemo(()=>(!current||(current.calls.state==='no_baseline'&&current.puts.state==='no_baseline'))
  ?standingOf(standing):null,[current,standing]);
 const standFrame=stand&&state.phase==='empty'?{phase:'ready' as const,text:''}:state;
 // What is and is not comparable yet, said to the minute. One reading: nothing. Two or more inside the first hour:
 // the fifteen-minute change exists (the middle pane shows it); this pane's hour-wide window fills later.
 const standAfter=useMemo(()=>{
  if(!current||timeline.length<2)return 'A second reading is needed before anything can be said about change.';
  const first=String(timeline[0]?.at||'');const m=first.match(/([0-9]{2}):([0-9]{2})/);
  if(!m)return 'The fifteen-minute change is in the middle pane; this pane compares across the hour.';
  const total=Number(m[1])*60+Number(m[2])+SIGNAL_WINDOW_MINUTES;
  const at=`${String(Math.floor(total/60)).padStart(2,'0')}:${String(total%60).padStart(2,'0')}`;
  return `The fifteen-minute change is in the middle pane; this pane compares across the hour, from ${at} IST.`;
 },[current,timeline]);
 const ladderList=useMemo(()=>Array.from(new Set((body?.rows||[])
  .map(r=>r.strike).filter((v):v is number=>v!=null))).sort((a,b)=>a-b),[body]);
 // THE COMPETING EXPLANATION and the whole-book rows, both from series the tab already serves. The spot
 // move over the SAME interval is what stops a premium change being read as a participant's doing.
 const spot=useMemo(()=>spotMove((futures?.points as any)||null,current?.at),[futures,current]);
 const cross=useMemo(()=>crossMarket({puts:current?current.puts:null,
  pcr:(pcr?.points as any)||null,futures:(futures?.points as any)||null,upto:current?.at}),[current,pcr,futures]);
 const story:Narrative|null=useMemo(()=>narrate(current,
  {underlying:body?.underlying||symbol,expiry:body?.expiry||expiry,ladder:ladderList,spot,cross}),
  [current,body,symbol,expiry,ladderList,spot,cross]);
 const lines=useMemo(()=>phrases(story),[story]);
 const spoken=useMemo(()=>story
  ?[story.headline,story.observed,story.qualified,story.otherSide,story.session,story.context]
    .filter(Boolean).join(' ')
  :'',[story]);

 // --- the reveal ---------------------------------------------------------------------------------------
 // It runs on an explicit Replay and ONCE per completed selection. A refresh re-fetches the same reading and
 // must not re-run it: the key below is the selection, not the request.
 const key=`${body?.underlying||symbol}|${body?.expiry||expiry||''}|${current?.at||''}`;
 const [shown,setShown]=useState(0),[mark,setMark]=useState(0);
 const timers=useRef<any[]>([]),frame=useRef(0),ran=useRef('');
 const stop=useCallback(()=>{
  timers.current.forEach(clearTimeout);timers.current=[];
  if(frame.current){cancelAnimationFrame(frame.current);frame.current=0;}
 },[]);
 const settle=useCallback(()=>{stop();setShown(lines.length);setMark(1);},[stop,lines.length]);
 const play=useCallback(()=>{
  stop();
  if(still||!lines.length){settle();return;}
  setShown(0);setMark(0);
  lines.forEach((_,i)=>timers.current.push(setTimeout(()=>setShown(i+1),i*PHRASE_MS)));
  timers.current.push(setTimeout(()=>{
   const t0=Date.now();
   const step=()=>{
    const p=Math.min(1,(Date.now()-t0)/UNDERLINE_MS);
    setMark(p);
    if(p<1)frame.current=requestAnimationFrame(step);else frame.current=0;
   };
   frame.current=requestAnimationFrame(step);
   // A FRAME IS NOT GUARANTEED. requestAnimationFrame is throttled to nothing in a background tab, and a
   // mark that only a frame can finish would then never be drawn at all. The timer below closes it either
   // way: the animation is the nice version of reaching the same end state, never the only way there.
   timers.current.push(setTimeout(()=>{
    if(frame.current){cancelAnimationFrame(frame.current);frame.current=0;}
    setMark(1);
   },UNDERLINE_MS+80));
  },lines.length*PHRASE_MS));
 },[lines,still,settle,stop]);

 // CANCEL ON CHANGE, then play once for the new selection. A second render of the same selection — a refresh,
 // a re-layout, a hover anywhere on the tab — finds `ran` already set and leaves the text alone.
 useEffect(()=>{
  stop();
  if(!story||!lines.length){setShown(0);setMark(0);ran.current=key;return;}
  if(ran.current===key){settle();return;}
  ran.current=key;
  if(still)settle();else play();
  // eslint-disable-next-line react-hooks/exhaustive-deps
 },[key,lines.length,still]);
 useEffect(()=>()=>stop(),[stop]);
 // A reader who turns motion off mid-flight gets the whole text at once.
 useEffect(()=>{if(still)settle();},[still,settle]);

 const playing=!!lines.length&&shown<lines.length;
 /** The side the cited strikes belong to: the story is written about ONE side, and the chips follow it. */
 const citedRow:'calls'|'puts'=current&&current.puts.weight>current.calls.weight?'puts':'calls';
 const citedSide=citedRow==='puts'?'PE':'CE';
 const slotOf=(value:number)=>(body?.rows||[]).find(r=>r.strike===value&&r.row===citedRow)||null;

 const groups:InfoGroup[]=story?[
  {heading:'Why KANIDA says this',lines:story.evidence},
  {heading:'What this is and is not',lines:[{text:story.caveat,tone:'amber' as const}]},
  {heading:'Where the numbers come from',lines:[body?.flow_text||'',body?.delta_oi_text||'']},
 ]:[{heading:'What this is and is not',lines:[{text:CAVEAT,tone:'amber' as const}]}];


 return <WidgetFrame name="What is happening"
  subtitle={symbol?`${symbol}${body?.expiry?` · ${body.expiry}`:''} · read from the ten at-the-money contracts`:''}
  body={body} state={standFrame} onRefresh={read.reload} onExpand={onExpand} expanded={expanded} inBlock
  emptyDetail="Each sentence needs two 15-min readings of these contracts before anything can be compared."
  toolbar={<><View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
   {!!story&&!story.limited&&<>
    <Pressable accessibilityRole="button" accessibilityLabel="Replay the explanation"
     onPress={play} disabled={playing}
     style={(st:any)=>[s.row,{gap:5,minHeight:24,paddingHorizontal:8,borderRadius:6,borderWidth:1,
      borderColor:C.line,opacity:playing?0.5:1,
      backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}>
     <Icon name="refresh-cw" size={11} color={C.green}/>
     <T style={{fontSize:10,lineHeight:14,color:C.green}}>Replay</T></Pressable>
    <Pressable accessibilityRole="button" accessibilityLabel="Skip the reveal and show the whole explanation"
     onPress={settle} disabled={!playing}
     style={(st:any)=>[s.row,{gap:5,minHeight:24,paddingHorizontal:8,borderRadius:6,borderWidth:1,
      borderColor:C.line,opacity:playing?1:0.5,
      backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}>
     <T style={{fontSize:10,lineHeight:14,color:C.ink}}>Skip</T></Pressable>
    <Pressable accessibilityRole="button" accessibilityState={{checked:quiet}}
     accessibilityLabel="Quiet motion. Show the whole explanation without a reveal"
     onPress={()=>setQuiet(q=>!q)}
     style={(st:any)=>[s.row,{gap:5,minHeight:24,paddingHorizontal:8,borderRadius:6,borderWidth:1,
      borderColor:quiet?C.green:C.line,backgroundColor:quiet||st.hovered?C.soft:'transparent'}]}>
     <T style={{fontSize:10,lineHeight:14,color:quiet?C.green:C.muted}}>Quiet</T></Pressable>
   </>}
   <View style={{flex:1,minWidth:4}}/>
   <InfoDisclosure label="Why this?" title="Why KANIDA says this" groups={groups}/>
  </View>
  </>}
  note={story&&!story.limited
   ?`Rule ${current?.rule_version||''} · ${asOfText(body?.as_of)}`
   :undefined}
  style={style}>
  <ScrollView style={{flex:1}} contentContainerStyle={{padding:12,gap:9}}>
    {stand?<StandingStory s={stand} after={standAfter}/>:!story?null:<>
     {/* THE HEADLINE. Tier 1 — what the measured numbers did, over the interval named beside it. */}
     <View style={{gap:2}}>
      <T style={head}>Latest change · {story.window}</T>
      <T role="heading" aria-level={3}
       style={{fontFamily:'ManropeBold',fontSize:16,lineHeight:22,letterSpacing:-0.2,
        color:story.limited?C.muted:C.ink}}>{story.headline}</T>
      {!story.limited&&<Underline width={Math.min(190,Math.max(60,story.headline.length*3.4))} progress={mark}/>}
     </View>

     {/* THE SENTENCES. One accessible label for the whole thing, so a screen reader hears it once and never
         phrase by phrase; the visible phrases are hidden from it and carry the reveal. */}
     <View accessibilityRole="text" accessibilityLabel={spoken} {...webOnly({'aria-label':spoken})}
      style={{gap:6}}>
      {lines.map((line,i)=><T key={i} {...webOnly({'aria-hidden':true})}
       style={{fontSize:12,lineHeight:18,color:i===0?C.ink:C.muted,
        opacity:i<shown?1:0.12,
        ...(Platform.OS==='web'?{transitionProperty:'opacity',transitionDuration:'240ms'}:{}) as any}}>
       {line}</T>)}
     </View>

     {/* WHERE IT SITS AGAINST PRICE. */}
     {!story.limited&&<View style={{paddingTop:7,borderTopWidth:1,borderColor:C.line}}>
      {/* the ladder now covers both sides for the middle pane; this one draws the side its story is about */}
      <Ladder rungs={story.ladder.filter(r=>r.row===citedRow)} spot={spot?spot.spot:null} window={story.window}
       highlight={highlight} onHighlight={onHighlight} onTarget={onTarget} slotOf={slotOf}/>
     </View>}

     {/* THE EPISODE. When it began and how long it has run — never a claim about intervals nobody checked. */}
     {!!story.context&&<View style={[s.row,{gap:6,flexWrap:'wrap',paddingTop:7,borderTopWidth:1,
      borderColor:C.line}]}>
      <Icon name="clock" size={11} color={C.green}/>
      <T style={{flex:1,minWidth:120,fontSize:11,lineHeight:16,color:C.green,
       fontVariant:['tabular-nums'] as any}}>{story.context}</T>
     </View>}

     {/* THE WHOLE BOOK. Each row is one observation at its own scope, over the interval named above. No
         row is a vote, so none of them carries a tick or a cross. */}
     {!!story.cross.length&&<View style={{gap:3,paddingTop:7,borderTopWidth:1,borderColor:C.line}}>
      <T style={head}>Cross-market context · {story.window}</T>
      {story.cross.map(row=><View key={row.label} style={[s.row,{gap:7,alignItems:'flex-start'}]}>
       <T style={{width:52,fontSize:10,lineHeight:15,color:C.muted}}>{row.label}</T>
       <T style={{flex:1,minWidth:0,fontSize:11,lineHeight:15,color:C.ink}}>{row.text}</T>
      </View>)}
     </View>}

     {/* THE SESSION, one tap away. The story from its first reading, not a window. */}
     {!story.limited&&<View style={{paddingTop:7,borderTopWidth:1,borderColor:C.line}}>
      <Pressable accessibilityRole="button" accessibilityState={{expanded:history}}
       accessibilityLabel={`${history?'Hide':'Show'} how this episode developed through the session`}
       onPress={()=>setHistory(h=>!h)}
       style={(st:any)=>[s.row,{gap:6,minHeight:24,borderRadius:6,paddingHorizontal:4,
        backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}>
       <Icon name={history?'chevron-down':'chevron-right'} size={12} color={C.muted}/>
       <T style={{flex:1,fontSize:11,lineHeight:15,color:C.ink}}>How this developed through the session</T>
      </Pressable>
      {history&&<View style={{gap:3,paddingTop:4}}>
       {timeline.slice().reverse().map((o,i)=>{
        const said=narrate(o,{underlying:body?.underlying||symbol,ladder:ladderList});
        return <View key={`${i}-${o.at}`} style={[s.row,{gap:7,alignItems:'flex-start'}]}>
         <T style={{width:38,fontSize:10,lineHeight:15,color:C.muted,
          fontVariant:['tabular-nums'] as any}}>{clock(o.at)}</T>
         <T style={{flex:1,minWidth:0,fontSize:11,lineHeight:15,
          color:o.covered?C.ink:C.amber}}>{said?said.headline:''}</T>
        </View>;
       })}
      </View>}
     </View>}
    </>}
  </ScrollView>
 </WidgetFrame>;
}
