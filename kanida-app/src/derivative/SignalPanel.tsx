// BLOCK 1, MIDDLE PANE — the market's current state, and the states it moved through to get here.
//
// ONE READING DOMINATES. A reader opening this pane has one question — what is happening NOW — and the
// answer is the whole top of the panel: the state, how long it has held, where it is, what supports it and
// what does not. Everything earlier is context, and context is smaller.
//
// THE SESSION IS A SEQUENCE OF STATES, NOT A LIST OF READINGS. Twenty-four rows that each say the same
// thing are twenty-four rows a reader learns to ignore. Consecutive readings in one state collapse into a
// single period carrying the number of readings it covers; nothing is deleted, and opening the period — or
// "Show all readings" — brings every one of them back.
//
// A QUIET SESSION IS ALLOWED TO LOOK QUIET. This panel will not manufacture transitions to fill itself.
//
// NOTHING BELOW THE TIMELINE. Every colour, every state word and every metric definition lives behind
// "How to read this". A legend under a table is read once and scrolled past for ever after.
import React,{useCallback,useEffect,useMemo,useState} from 'react';
import {api} from '../model';
import {View,Pressable,ScrollView,Platform} from 'react-native';
import {C,T,Icon,s} from '../ui';
const s_=s;
import {useReducedMotion} from '../falcon/useFalconScan';
import {useDerivativeRead,type Read} from './useDerivatives';
import {InfoDisclosure,WidgetFrame,head,stateOf} from './frame';
import {DASH,clock,compact} from './logic';
import {SIGNAL_GUIDE,STATE_LABEL,STATE_TONE,periods,persistenceText,signedLots,signedMoney,signedNum,
 chainStep,standing as standingOf,states,strikeOf,type Period,type SignalState,type Standing} from './signal';
import type {OiGrid} from './types';

export const SIGNAL_EMPTY='No 15-min reading of these ten contracts has been captured yet.';
export const SIGNAL_NO_SYMBOL='Pick an instrument on the left to read how its last fifteen minutes went.';

/** THE COLOUR SYSTEM, in one place. Green is activity EXPANDING and red is it CONTRACTING — neither is a
 *  market direction, and nothing here is painted as good news or bad news. */
const TONE:Record<string,string>={up:C.green,down:C.red,warn:C.amber,flat:C.muted};
const toneOf=(v:number|null|undefined)=>v==null?C.muted:v>0?C.green:v<0?C.red:C.muted;

type Box={label:string;value:string;sub:string;colour:string};
/** The five metrics, in the order a reader asks for them: what the contract did first, then what the book did
 *  around it. THE CHANGE IS THE DEFAULT — an arrow and a size. Levels live one press away, never beside it. */
const arrow=(v:number|null)=>v==null?'':v>0?'↑ ':v<0?'↓ ':'→ ';
const abs=(v:number|null,f:(n:number)=>string)=>v==null?DASH:`${arrow(v)}${f(Math.abs(v))}`;
function boxes(row:SignalState):Box[]{
 const puts=row.row==='puts'||(!row.row
  &&Math.abs(row.put_oi_change||0)>Math.abs(row.call_oi_change||0));
 const sideOi=puts?row.put_oi_change:row.call_oi_change;
 const mp=row.max_pain_current==null?DASH
  :row.max_pain_change?`${strikeOf(row.max_pain_previous)} → ${strikeOf(row.max_pain_current)}`
  :`→ ${strikeOf(row.max_pain_current)}`;
 return [
  {label:'Price',value:abs(row.price_change,n=>`₹${n.toFixed(2)}`),colour:toneOf(row.price_change),
   sub:row.price_contract||''},
  {label:'Δ Open interest',value:abs(sideOi,n=>compact(n)),colour:toneOf(sideOi),sub:puts?'Puts':'Calls'},
  {label:'Δ IV',value:abs(row.iv_change,n=>n.toFixed(1)),colour:toneOf(row.iv_change),sub:''},
  {label:'Δ PCR',value:abs(row.pcr_change,n=>n.toFixed(2)),colour:toneOf(row.pcr_change),sub:''},
  {label:'Max pain',value:mp,colour:row.max_pain_change?C.amber:C.muted,sub:''},
 ];
}

function MetricBox({box}:{box:Box}){
 return <View accessibilityRole="text" accessibilityLabel={`${box.label} ${box.value}. ${box.sub}`}
  style={{width:96,flexGrow:1,gap:1,paddingHorizontal:7,paddingVertical:5,borderRadius:7,
   borderWidth:1,borderColor:C.line,backgroundColor:C.dark}}>
  <T numberOfLines={1} style={{fontSize:8,lineHeight:11,letterSpacing:.5,textTransform:'uppercase',
   color:C.muted}}>{box.label}</T>
  <T numberOfLines={1} style={{fontSize:12.5,lineHeight:16,fontFamily:'InterSemi',color:box.colour,
   fontVariant:['tabular-nums'] as any}}>{box.value}</T>
  <T numberOfLines={2} style={{fontSize:8.5,lineHeight:11,color:C.muted}}>{box.sub}</T>
 </View>;
}

/** The state badge. It is the first thing read and the only large colour on the card. */
function StateChip({state,big}:{state:SignalState['state'];big?:boolean}){
 const colour=TONE[STATE_TONE[state]]||C.muted;
 return <View style={{paddingHorizontal:big?9:6,paddingVertical:big?3:1.5,borderRadius:6,borderWidth:1,
  borderColor:colour,backgroundColor:STATE_TONE[state]==='flat'?'transparent':C.dark}}>
  <T style={{fontSize:big?10.5:9,lineHeight:big?14:12,letterSpacing:.8,textTransform:'uppercase',
   fontFamily:'InterSemi',color:colour}}>{STATE_LABEL[state]}</T>
 </View>;
}

function Tag({label}:{label:string}){
 // A tag naming something missing or unconfirmed is amber; the rest are neutral. None is green — a tag is
 // a qualifier, and a qualifier painted as a positive stops being read as one.
 const warn=/not confirming|unavailable|weak|narrowing/i.test(label);
 return <View style={{paddingHorizontal:6,paddingVertical:1.5,borderRadius:5,borderWidth:1,
  borderColor:warn?C.amber:C.line,backgroundColor:C.dark}}>
  <T style={{fontSize:9,lineHeight:12,color:warn?C.amber:C.muted}}>{label}</T></View>;
}

/** WHAT THE READING RESTS ON, AND WHAT IT DOES NOT. The two are shown together and weighted the same: the
 *  conflict is not a footnote to the evidence, it is half of it. */
function Evidence({row}:{row:SignalState}){
 const line=(text:string,warn:boolean,i:number)=>
  <View key={`${warn}-${i}`} style={[s.row,{gap:5,alignItems:'flex-start'}]}>
   <T style={{width:8,fontSize:10,lineHeight:15,color:warn?C.amber:C.green}}>{warn?'–':'+'}</T>
   <T style={{flex:1,fontSize:10.5,lineHeight:15,color:C.muted}}>{text}</T></View>;
 return <View style={{gap:3}}>
  {row.supporting_evidence.map((e,i)=>line(e.text,false,i))}
  {row.conflicting_evidence.map((e,i)=>line(e.text,true,i))}
  {!row.supporting_evidence.length&&!row.conflicting_evidence.length
   &&<T style={{fontSize:10.5,lineHeight:15,color:C.muted}}>
    Nothing measurable changed over this interval.</T>}
 </View>;
}

/** WHERE IT IS. Strikes matter because they are the only part of this a reader can act on — but a bare list
 *  of five numbers is noise, so each one carries what it did. */
function KeyStrikes({row}:{row:SignalState}){
 if(!row.key_strikes.length)return null;
 return <View style={{gap:3}}>
  <T style={head}>Key strikes</T>
  <View style={[s.row,{gap:5,flexWrap:'wrap'}]}>
   {row.key_strikes.map(k=><View key={`${k.strike}-${k.side}`}
    accessibilityRole="text" accessibilityLabel={`${strikeOf(k.strike)} ${k.side}, ${k.note}`}
    style={[s.row,{gap:5,paddingHorizontal:7,paddingVertical:2.5,borderRadius:6,borderWidth:1,
     borderColor:k.lead?C.green:C.line,backgroundColor:C.dark}]}>
    <T style={{fontSize:10.5,lineHeight:14,fontFamily:'InterSemi',color:k.lead?C.green:C.ink,
     fontVariant:['tabular-nums'] as any}}>{strikeOf(k.strike)} {k.side}</T>
    <T style={{fontSize:9,lineHeight:12,color:C.muted}}>{k.note}</T>
   </View>)}
  </View>
 </View>;
}

/** THE LATEST READING. What changed, where, the one sentence of evidence, the strikes it is about and the
 *  metrics that prove it — and nothing about the software. Persistence appears only once it is established. */
function Latest({row}:{row:SignalState}){
 const [levels,setLevels]=useState(false);
 const window=row.previous_timestamp
  ?`${clock(row.previous_timestamp)} → ${clock(row.timestamp)}`:clock(row.timestamp);
 const persist=persistenceText(row);
 const quiet=row.state==='balanced';
 return <View style={{gap:8,paddingHorizontal:10,paddingVertical:9,borderBottomWidth:1,
  borderColor:C.line,backgroundColor:C.dark}}>
  <View style={[s.row,{gap:7,flexWrap:'wrap'}]}>
   <StateChip state={row.state} big/>
   <T style={{fontSize:10.5,lineHeight:14,color:C.muted,fontVariant:['tabular-nums'] as any}}>{window}</T>
   <View style={{flex:1,minWidth:2}}/>
   {!!persist&&<T style={{fontSize:10,lineHeight:14,color:C.muted}}>{persist}</T>}
  </View>

  <View style={{gap:4}}>
   <T style={{fontSize:16,lineHeight:21,fontFamily:'InterSemi',color:C.ink,letterSpacing:-.2}}>
    {row.plain_language_read}</T>
   {!!row.plain_language_evidence&&<T style={{fontSize:11,lineHeight:16,color:C.muted}}>
    {row.plain_language_evidence}</T>}
  </View>

  {!!row.tags.length&&<View style={[s.row,{gap:5,flexWrap:'wrap'}]}>
   {row.tags.map(t=><Tag key={t} label={t}/>)}</View>}

  <KeyStrikes row={row}/>

  {/* A quiet reading shows the metrics that prove it quiet — no more. Pressing them opens the levels. */}
  {!quiet||row.price_change!=null||row.call_oi_change!=null?<Pressable accessibilityRole="button"
   accessibilityState={{expanded:levels}}
   accessibilityLabel={`${boxes(row).map(b=>`${b.label} ${b.value}`).join(', ')}. ${levels?'Hide':'Show'} the levels behind these changes`}
   onPress={()=>setLevels(v=>!v)}
   style={[s.row,{gap:5,flexWrap:'wrap',alignItems:'stretch'}]}>
   {boxes(row).map(b=><MetricBox key={b.label} box={b}/>)}
  </Pressable>:null}
  {levels&&<Levels row={row}/>}
 </View>;
}

/** THE LEVELS behind a reading's changes — what each figure stood at. One press away, never on by default. */
function Levels({row}:{row:SignalState}){
 const list:[string,string][]=[
  ['Premium',row.price_level==null?DASH
   :`₹${row.price_level.toFixed(2)}${row.price_contract?` (${row.price_contract})`:''}`],
  ['Call open interest',row.call_oi_level==null?DASH:compact(row.call_oi_level)],
  ['Put open interest',row.put_oi_level==null?DASH:compact(row.put_oi_level)],
  ['ATM IV',row.iv_level==null?DASH:`${row.iv_level.toFixed(1)}%`],
  ['Call / put IV change',`${row.call_iv_change==null?DASH:signedNum(row.call_iv_change)} / ${row.put_iv_change==null?DASH:signedNum(row.put_iv_change)} pts`],
  ['PCR',row.pcr_current==null?DASH
   :`${row.pcr_previous==null?'':`${row.pcr_previous.toFixed(2)} → `}${row.pcr_current.toFixed(2)}`],
  ['Max pain',row.max_pain_current==null?DASH:strikeOf(row.max_pain_current)],
 ];
 return <View style={{gap:2}}>
  {list.map(([k,v])=><View key={k} style={[s.row,{gap:8}]}>
   <T style={{width:118,fontSize:10,lineHeight:14,color:C.muted}}>{k}</T>
   <T style={{flex:1,fontSize:10,lineHeight:14,color:C.ink,fontVariant:['tabular-nums'] as any}}>{v}</T>
  </View>)}
 </View>;
}

/** ONE READING, OPENED IN PLACE from the timeline: its evidence, its changes and its levels. */
function Detail({row}:{row:SignalState}){
 return <View style={{gap:8,paddingHorizontal:10,paddingBottom:9,paddingTop:4,backgroundColor:C.dark}}>
  {!!row.plain_language_evidence&&<T style={{fontSize:10.5,lineHeight:15,color:C.muted}}>
   {row.plain_language_evidence}</T>}
  {row.state!=='opening'&&<View style={[s.row,{gap:5,flexWrap:'wrap',alignItems:'stretch'}]}>
   {boxes(row).map(b=><MetricBox key={b.label} box={b}/>)}
  </View>}
  <Levels row={row}/>
 </View>;
}

/** ONE PERIOD ON THE TIMELINE. A run of readings in the same state is one line, and the count rides on it
 *  so a reader can see that nothing was hidden from them. */
function PeriodRow({period,open,onToggle,reduce}:{period:Period;open:string;
 onToggle:(id:string)=>void;reduce:boolean}){
 const id=String(period.to||'');
 const isOpen=open===id;
 const span=period.count>1?`${clock(period.from)} → ${clock(period.to)}`:clock(period.to);
 const colour=TONE[STATE_TONE[period.state]]||C.muted;
 const spoken=`${span}. ${STATE_LABEL[period.state]}`
  +`${period.count>1?`, ${period.count} readings over ${period.minutes} minutes`:''}. ${period.read}.`;
 return <View style={{borderBottomWidth:1,borderColor:'#0F1B22'}}>
  <Pressable accessibilityRole="button" accessibilityState={{expanded:isOpen}}
   accessibilityLabel={`${spoken} ${isOpen?'Close':'Open'} the readings behind this period`}
   onPress={()=>onToggle(id)}
   style={(st:any)=>[{paddingHorizontal:10,paddingVertical:6,gap:2,
    borderLeftWidth:2,borderLeftColor:isOpen?colour:'transparent',
    backgroundColor:isOpen?C.dark:'transparent',
    ...(Platform.OS==='web'&&!reduce?{transitionProperty:'background-color',
     transitionDuration:'160ms'}:{}) as any},
    st.hovered&&!isOpen?{backgroundColor:'#08161D'}:null]}>
   <View style={[s.row,{gap:7,flexWrap:'wrap'}]}>
    <T style={{width:92,fontSize:10,lineHeight:14,color:C.muted,
     fontVariant:['tabular-nums'] as any}}>{span}</T>
    <StateChip state={period.state}/>
    {period.count>1&&<T style={{fontSize:9,lineHeight:12,color:C.muted}}>
     {`${period.count} readings · ${period.minutes} min`}</T>}
    <View style={{flex:1,minWidth:2}}/>
    <Icon name={isOpen?'chevron-down':'chevron-right'} size={11} color={C.muted}/>
   </View>
   <T numberOfLines={isOpen?3:2} style={{fontSize:10.5,lineHeight:14,color:C.ink,paddingLeft:99}}>
    {period.read}</T>
  </Pressable>
  {isOpen&&<View>
   {period.count>1&&<T style={{fontSize:9.5,lineHeight:13,color:C.muted,paddingHorizontal:10,
    paddingTop:5}}>Every reading this period covers, newest first.</T>}
   {period.readings.slice().reverse().map(r=><View key={String(r.timestamp)}
    style={{borderTopWidth:1,borderColor:'#0F1B22'}}>
    <View style={[s.row,{gap:7,paddingHorizontal:10,paddingTop:6}]}>
     <T style={{width:92,fontSize:10,lineHeight:14,color:C.muted,
      fontVariant:['tabular-nums'] as any}}>{clock(r.timestamp)}</T>
     <T style={{flex:1,fontSize:10.5,lineHeight:14,color:C.ink}}>{r.plain_language_read}</T>
    </View>
    <Detail row={r}/>
   </View>)}
  </View>}
 </View>;
}

/** THE OPENING READ — the session's first reading. There is nothing before it, so it says where positions are
 *  concentrated near the money and the levels the book stands at. It does not apologise for having no change. */
function StandingCard({s}:{s:Standing}){
 const facts:[string,string][]=[
  ['Spot',s.spot==null?DASH:`₹${s.spot.toLocaleString('en-IN',{maximumFractionDigits:2})}`],
  ['At the money',strikeOf(s.atm)],
  ['Put/call OI',s.pcr==null?DASH:s.pcr.toFixed(2)],
  ['Max pain',strikeOf(s.max_pain)],
 ];
 return <View style={{gap:9,paddingHorizontal:10,paddingVertical:10,backgroundColor:C.dark}}>
  <View style={[s_.row,{gap:7,flexWrap:'wrap'}]}>
   <StateChip state="opening" big/>
   <T style={{fontSize:10.5,lineHeight:14,color:C.muted,fontVariant:['tabular-nums'] as any}}>{clock(s.at)}</T>
  </View>
  {!!s.opening&&<T style={{fontSize:16,lineHeight:21,fontFamily:'InterSemi',color:C.ink,letterSpacing:-.2}}>
   {s.opening}</T>}
  <View style={[s_.row,{gap:5,flexWrap:'wrap',alignItems:'stretch'}]}>
   {facts.map(([k,v])=><View key={k} style={{width:96,flexGrow:1,gap:1,paddingHorizontal:7,paddingVertical:5,
    borderRadius:7,borderWidth:1,borderColor:C.line,backgroundColor:C.bg}}>
    <T style={{fontSize:8,lineHeight:11,letterSpacing:.5,textTransform:'uppercase',color:C.muted}}>{k}</T>
    <T style={{fontSize:12.5,lineHeight:16,fontFamily:'InterSemi',color:C.ink,
     fontVariant:['tabular-nums'] as any}}>{v}</T></View>)}
  </View>
 </View>;
}

export function SignalPanel({symbol,read,pcr,maxPain,iv,standing,onExpand,expanded,style}:{
 symbol:string;read:Read<OiGrid>;pcr?:any;maxPain?:any;iv?:any;standing?:any;
 onExpand?:()=>void;expanded?:boolean;style?:any}){
 const body=read.data as OiGrid|null;
 const state=symbol?stateOf(read as any,SIGNAL_EMPTY)
  :{phase:'empty' as const,text:SIGNAL_NO_SYMBOL};
 const reduce=useReducedMotion();

 const [open,setOpen]=useState<string>('');
 const [all,setAll]=useState(false);
 const toggle=useCallback((id:string)=>setOpen(o=>o===id?'':id),[]);

 // THE SESSION AS KANIDA SAW IT. Every earlier reading is the IMMUTABLE SNAPSHOT the server wrote at that reading
 // (kanida_pilot/snapshots.py): described from the grid anchored at its own reading, chained to the snapshot
 // before it. Nothing here re-derives the past from today's at-the-money contracts.
 const marks:string[]=useMemo(()=>((body as any)?.marks||[]).map((m:any)=>String(m)),[body]);
 const lastMark=marks.length?marks[marks.length-1]:'';
 const expiry=String((body as any)?.expiry||'');
 const snapRead=useDerivativeRead<any>(symbol&&lastMark
  ?`/api/derivatives/snapshots?underlying=${encodeURIComponent(symbol)}&upto=${encodeURIComponent(lastMark)}`:null);
 const stored:SignalState[]=useMemo(()=>((snapRead.data?.snapshots||[]) as any[])
  .filter(s=>s.status==='ok'&&s.state&&(!expiry||!s.expiry||s.expiry===expiry)).map(s=>s.state as SignalState),
  [snapRead.data,expiry]);
 // this reading's own fifteen minutes, on its own contracts — the same engine the server stores with
 const rows=useMemo(()=>states({grid:body,pcr,maxPain,iv},{intervalOnly:true}),[body,pcr,maxPain,iv]);
 const before=useMemo(()=>stored.filter(s=>String(s.timestamp)<lastMark),[stored,lastMark]);
 // the latest reading: its stored snapshot once the worker has written it (seconds after the metrics land);
 // until then the same engine, chained to the same stored previous snapshot — so the two cannot differ
 const latest=useMemo(()=>stored.find(s=>String(s.timestamp)===lastMark)
  ||(rows[0]?chainStep(before.length?before[before.length-1]:null,rows[0]):undefined),[stored,lastMark,rows,before]);
 const earlier=useMemo(()=>periods(before.slice().reverse()),[before]);
 // THE FIRST READING. No grid rows means no second reading to compare against; the levels are still known,
 // so the pane says where positions stand rather than 'not enough readings'.
 const stand=useMemo(()=>rows.length?null:standingOf(standing),[rows.length,standing]);
 const frameState=stand&&state.phase==='empty'?{phase:'ready' as const,text:''}:state;
 const shown=all?earlier:earlier.slice(0,6);
 const hiddenReadings=earlier.slice(shown.length).reduce((n,p)=>n+p.count,0);

 return <WidgetFrame name="Signal by 15-min reading"
  subtitle="The market's current state, and how it got there"
  body={body} state={frameState} onRefresh={read.reload} onExpand={onExpand} expanded={expanded} inBlock
  emptyDetail="A state needs two 15-min readings of these contracts before anything can be compared."
  toolbar={<View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
   {!!latest&&<View style={[s.row,{gap:5}]}>
    <View style={{width:5,height:5,borderRadius:3,backgroundColor:C.green}}/>
    <T style={{fontSize:10,lineHeight:14,color:C.muted,fontVariant:['tabular-nums'] as any}}>
     {clock(latest.timestamp)} IST</T>
   </View>}
   <View style={{flex:1,minWidth:4}}/>
   <InfoDisclosure label="How to read this" title="How to read this" groups={SIGNAL_GUIDE as any}/>
  </View>}
  /* NOTHING under the timeline. `note` is left undefined on purpose: every explanation lives behind
     "How to read this", and the as-of sits in the header line above. */
  note={undefined}
  footer={null}
  style={style}>
  <ScrollView style={{flex:1}} contentContainerStyle={{paddingBottom:8}}>
   {!!stand&&<StandingCard s={stand}/>}
   {!!latest&&<Latest row={latest}/>}
   {!!earlier.length&&<>
    <View style={[s.row,{gap:7,paddingHorizontal:10,paddingTop:8,paddingBottom:4}]}>
     <T style={head}>Earlier today</T>
     <T style={{fontSize:9.5,lineHeight:13,color:C.muted}}>
      {`${earlier.length} state${earlier.length===1?'':'s'} before this reading`}</T>
    </View>
    {shown.map(p=><PeriodRow key={String(p.to)} period={p} open={open} onToggle={toggle}
     reduce={reduce}/>)}
    {hiddenReadings>0&&<Pressable accessibilityRole="button"
     accessibilityLabel={`Show all readings, ${hiddenReadings} more earlier in the session`}
     onPress={()=>setAll(true)}
     style={(st:any)=>[{paddingHorizontal:10,paddingVertical:7,alignItems:'flex-start'},
      st.hovered?{backgroundColor:'#08161D'}:null]}>
     <T style={{fontSize:10.5,lineHeight:14,color:C.green}}>
      {`Show all readings (${hiddenReadings} earlier)`}</T></Pressable>}
   </>}
   {!!latest&&!earlier.length&&<T style={{fontSize:10.5,lineHeight:15,color:C.muted,
    paddingHorizontal:10,paddingTop:8}}>
    This is the first reading of the session, so there is nothing yet to compare it against.</T>}
  </ScrollView>
 </WidgetFrame>;
}
