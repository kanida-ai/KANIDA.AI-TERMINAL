// Trader evidence card for a researched pattern (replaces the backtest card's content for the four research blocks).
// Sections render in the owner's fixed order: Summary · Barriers · Forward-return curve · Conditions · Last 5 · Honesty.
// Everything is served precomputed by /api/strategies/{key}/card; this file formats and draws, it never computes evidence.
import React,{useMemo} from 'react';
import {View,ScrollView} from 'react-native';
import Svg,{Rect,Line} from 'react-native-svg';
import {C,T,Icon,Badge,s} from '../ui';
import {api} from '../model';
import {useKeyedValue} from '../activeSymbol';
import {WidgetError} from '../layout';
import {linkKey,type BlockLink} from './link';
import {CardShell,CenterNote,LinkText,Skeleton} from './parts';
import {openChart,LinkedHeader,EmptyHeader} from './ChartCard';
import type {CardBucket,CardCurve,EvidenceCard as EvidenceCardData} from '../strategies/types';
import {pilotSelection} from '../patternHistory/logic';
import {PatternHistoryCard} from '../patternHistory/PatternHistoryCard';
import {CARD_COST_LINE,bucketA11y,bucketText,barrierText,barrierTimingText,count,curveBars,curveCaption,
 detectionEvidenceNote,detectionStateLabel,detectionTimeText,evidenceTone,honestyLines,occurrenceOutcome,
 occurrenceText,rate,signed,plain,winRateText,winRateScopeNote} from './cardLogic';

const num={fontVariant:['tabular-nums'] as any};
const tone=(v:number|null|undefined)=>v==null||!Number.isFinite(v)?C.muted:v>0?C.green:v<0?C.red:C.ink;
const TONE_COLOR={good:C.green,weak:C.amber,neutral:C.muted,pending:C.muted} as const;

function Section({title,note,children}:{title:string;note?:string|null;children:React.ReactNode}){
 return <View style={{gap:5}}>
  <View style={[s.row,{gap:8,minWidth:0}]}>
   <T role="heading" aria-level={4} style={[s.label,{fontSize:10,flexShrink:0}]}>{title}</T>
   {!!note&&<T numberOfLines={1} style={{flex:1,minWidth:0,fontSize:10,lineHeight:14,color:C.muted,textAlign:'right'}}>{note}</T>}
  </View>
  {children}
 </View>;
}
function Stat({label,value,color,note,a11y,width}:{label:string;value:string;color?:string;note?:string|null;a11y?:string;width?:any}){
 return <View accessible accessibilityLabel={a11y||`${label} ${value}${note?`, ${note}`:''}`} style={{flexBasis:width||'46%',flexGrow:1,minWidth:0,gap:1}}>
  <T numberOfLines={1} style={{fontSize:10,lineHeight:13,color:C.muted}}>{label}</T>
  <T numberOfLines={1} style={[num,{fontFamily:'InterSemi',fontSize:12.5,lineHeight:17,color:color||C.ink}]}>{value}</T>
  {!!note&&<T numberOfLines={1} style={{fontSize:9.5,lineHeight:13,color:C.muted}}>{note}</T>}
 </View>;
}

/** Forward-return curve: one bar per declared horizon on a shared zero line, the peak outlined. */
function CurveChart({curve}:{curve:CardCurve}){
 const [w,setW]=React.useState(0);const H=64,bars=useMemo(()=>curveBars(curve),[curve]);
 if(!bars.length)return <T style={{fontSize:11,color:C.muted}}>No forward-return grid was measured for this cell.</T>;
 const gap=6,bw=w?Math.max(6,(w-gap*(bars.length-1))/bars.length):0,mid=H/2;
 return <View style={{gap:3}}>
  <View onLayout={e=>setW(Math.round(e.nativeEvent.layout.width))}
   accessible accessibilityLabel={`Forward return by holding window. ${bars.map(b=>b.a11y).join('. ')}`} style={{height:H}}>
   {w>0&&<Svg width={w} height={H}>
    <Line x1={0} x2={w} y1={mid} y2={mid} stroke={C.line} strokeDasharray="3 3"/>
    {bars.map((b,i)=>{
     const h=Math.max(b.value==null?0:2,b.height*(mid-6)),x=i*(bw+gap);
     const up=(b.value??0)>=0,y=up?mid-h:mid;
     return <Rect key={b.h} x={x} y={y} width={bw} height={h} rx={2}
      fill={b.value==null?C.line:up?C.green:C.red} opacity={b.value==null?.4:b.peak?1:.62}
      stroke={b.peak?C.ink:undefined} strokeWidth={b.peak?1:0}/>;
    })}
   </Svg>}
  </View>
  <View aria-hidden style={[s.row,{gap}]}>{bars.map(b=><T key={b.h} numberOfLines={1} style={[num,{width:bw,textAlign:'center',fontSize:9,lineHeight:12,color:b.peak?C.ink:C.muted,fontFamily:b.peak?'InterSemi':undefined}]}>{b.label}</T>)}</View>
 </View>;
}

function BucketRow({b,group,first}:{b:CardBucket;group:string;first:boolean}){
 return <View accessible accessibilityLabel={bucketA11y(b,group)} style={[s.row,{gap:6,minHeight:20,borderTopWidth:first?0:1,borderColor:C.line}]}>
  <T numberOfLines={1} style={{width:86,flexShrink:0,fontSize:11,lineHeight:15,color:b.enough?C.ink:C.muted}}>{b.name}</T>
  <T numberOfLines={1} style={[num,{width:34,flexShrink:0,fontSize:10.5,lineHeight:15,color:C.muted,textAlign:'right'}]}>{count(b.n)}</T>
  <T numberOfLines={1} style={{flex:1,minWidth:0,fontSize:10.5,lineHeight:15,textAlign:'right',color:b.enough?tone(b.diff_mean_net_return_pct):C.amber}}>{bucketText(b)}</T>
 </View>;
}

type EvidenceProps={link:BlockLink|null;style?:any;stale?:boolean;ageDays?:number|null;color?:string};
export function EvidenceCard(props:EvidenceProps){
 const selection=pilotSelection(props.link);
 return selection&&props.link?<PatternHistoryCard link={props.link} selection={selection} style={props.style} color={props.color}/>:<ResearchEvidenceCard {...props}/>;
}
function ResearchEvidenceCard({link,style,stale,ageDays,color}:EvidenceProps){
 const key=linkKey(link);
 const {value:card,error,loading,reload}=useKeyedValue<EvidenceCardData>(key,async()=>{
  const l=link!;
  // A card opened from a live detection carries that detection's id: the server serves this cell's numbers
  // only when the detection's pattern id, variant, side, timeframe AND detector spec hash match the research
  // run, and otherwise returns the contract's "Incompatible historical evidence" with no numbers at all.
  const d=await api(`/api/strategies/${encodeURIComponent(l.strategyKey)}/card?symbol=${encodeURIComponent(l.symbol)}`+(l.detectionId?`&detection_id=${encodeURIComponent(l.detectionId)}`:''));
  // Key guard: a response for a different stock or timeframe is never rendered under this header.
  if(d?.identity&&(String(d.identity.symbol).toUpperCase()!==l.symbol||d.identity.timeframe!==l.timeframe))
   throw new Error('The evidence response did not match the selected stock.');
  if(l.detectionId&&d?.identity&&d.identity.detection_id&&d.identity.detection_id!==l.detectionId)
   throw new Error('The evidence response did not match the selected detection.');
  return d as EvidenceCardData;
 });
 const label=link?`Backtest · ${link.symbol} · ${link.patternName} ${link.timeframe}`:'Backtest';
 // "Detected today" and "evidence" are separate claims: the detection line says what the scanner saw, the
 // label below says what the research run can (or cannot) say about it. Neither ever stands in for the other.
 const detected=link?.detectionId?`Detected ${detectionTimeText(link.detectedAt,null)} · ${detectionStateLabel({state:link.detectionState||''})}`:null;
 const mismatch=detectionEvidenceNote(card?.live_evidence);
 const header=link?<LinkedHeader title={`Evidence · ${link.symbol} · ${link.timeframe}`} slot={link.slot} color={color}
   detail={`${link.patternName} · ${link.side==='long'?'Long':'Short'}${detected?` · ${detected}`:''}`}
   sourceA11y={`${link.patternName} ${link.timeframe}, ${link.side}, from scanner ${link.slot}`}
   link={<LinkText label="Evidence →" a11y={`Open evidence and replay for ${link.symbol} ${link.timeframe}`} onPress={()=>openChart(link,'evidence')}/>}/>
  :<EmptyHeader title="Backtest"/>;
 const state=card?.evidence_state;
 const footer=<>
  {!!card&&<T style={{fontSize:11,lineHeight:15,fontFamily:'InterSemi',color:TONE_COLOR[evidenceTone(state!)]}}>{card.label}</T>}
  <T style={{fontSize:11,lineHeight:15,color:C.muted}}>{CARD_COST_LINE} · {link&&stale?<T style={{fontSize:11,lineHeight:15,color:C.amber}}>research only{ageDays!=null?` (${ageDays}d old)`:''}</T>:'research only'}</T>
 </>;

 let body:React.ReactNode;
 if(!link)body=<CenterNote icon="bar-chart-2" text="Click a stock in a scanner card to see what this pattern has actually done on it, after costs."/>;
 else if(error)body=<View style={{padding:12}}><WidgetError title="Evidence unavailable" message={`Evidence for ${link.symbol} ${link.timeframe} could not be loaded. ${error}`} onRetry={reload}/></View>;
 else if(loading||!card)body=<Skeleton lines={7}/>;
 else if(!card.summary)body=<CenterNote icon={state==='loading'?'clock':'info'} text={mismatch||card.note||card.label}>
   <Badge label={card.label} tone={state==='loading'?'neutral':'amber'}/>
 </CenterNote>;
 else{
  const sum=card.summary,bar=card.barriers,curve=card.forward_curve,cond=card.conditions,last=card.last_occurrences;
  const timing=barrierTimingText(bar,sum.horizon_bars_word);
  body=<ScrollView style={{flex:1}} contentContainerStyle={{paddingHorizontal:12,paddingVertical:10,gap:11}}>
   <View style={[s.row,{flexWrap:'wrap',gap:6}]}>
    <Badge label={card.label} tone={evidenceTone(state!)==='good'?'green':evidenceTone(state!)==='weak'?'amber':'neutral'} dot/>
    {!!sum.sample_label&&<Badge label={sum.sample_label} tone="neutral"/>}
    {card.review_required&&<Badge label={card.review_label||'Historical data requires review'} tone="amber"/>}
   </View>
   {!!card.note&&<T style={{fontSize:10.5,lineHeight:14,color:C.amber}}>{card.note}</T>}
   {!!mismatch&&<T style={{fontSize:10.5,lineHeight:14,color:C.amber}}>{mismatch}</T>}

   {/* 1. Summary */}
   <Section title="Summary" note={occurrenceText(sum)}>
    <View accessible accessibilityLabel={`Win rate ${winRateText(sum)}, ${winRateScopeNote(sum)}`} style={[s.row,{gap:6,minWidth:0}]}>
     <T numberOfLines={1} style={[num,{fontFamily:'InterSemi',fontSize:15,lineHeight:20,color:C.ink}]}>{winRateText(sum)}</T>
    </View>
    <T style={{fontSize:10,lineHeight:13,color:C.muted}}>win rate ({winRateScopeNote(sum)})</T>
    <View style={{flexDirection:'row',flexWrap:'wrap',rowGap:6,columnGap:10,marginTop:2}}>
     <Stat label={`Median net · ${sum.horizon??'?'} ${sum.horizon_bars_word}`} value={signed(sum.median_net_return_pct)} color={tone(sum.median_net_return_pct)}/>
     <Stat label="Mean net" value={signed(sum.mean_net_return_pct)} color={tone(sum.mean_net_return_pct)}
      note={Number.isFinite(sum.baseline_mean_net_return_pct as any)?`stock alone ${signed(sum.baseline_mean_net_return_pct)}`:null}/>
     <Stat label="Median best move (MFE)" value={plain(sum.median_mfe_pct)} color={C.green}/>
     <Stat label="Median worst move (MAE)" value={plain(sum.median_mae_pct)} color={C.red}/>
    </View>
    {!!sum.window_text&&<T style={{fontSize:11,lineHeight:15,color:C.ink}}>{sum.window_text}.</T>}
   </Section>

   {/* 2. Barriers */}
   <Section title="Barriers" note={bar?.n!=null?`${count(bar.n)} decided`:null}>
    {barrierText(bar)
     ? <T style={[num,{fontSize:11.5,lineHeight:16,color:C.ink}]}>{barrierText(bar)}</T>
     : <T style={{fontSize:11,lineHeight:15,color:C.muted}}>No barrier test was measured for this cell.</T>}
    {!!timing&&<T style={{fontSize:10.5,lineHeight:14,color:C.muted}}>{timing}</T>}
    {!!bar?.tie_rule_text&&<T style={{fontSize:10,lineHeight:14,color:C.muted}}>{bar.tie_rule_text}</T>}
   </Section>

   {/* 3. Forward-return curve */}
   {!!curve&&<Section title={`Forward return · ${curve.bars_word}`} note={curve.selected_h?`window ${curve.selected_h}`:null}>
    <CurveChart curve={curve}/>
    <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{curveCaption(curve)}</T>
   </Section>}

   {/* 4. Conditions */}
   {!!cond&&<Section title="Conditions" note={`min ${cond.minimum_sample} cases`}>
    {cond.groups.length
     ? cond.groups.map(g=><View key={g.dimension} style={{gap:1,marginBottom:4}}>
        <T numberOfLines={1} style={{fontSize:10.5,lineHeight:15,fontFamily:'InterSemi',color:C.muted}}>{g.title}</T>
        {g.buckets.map((b,i)=><BucketRow key={b.bucket} b={b} group={g.title} first={i===0}/>)}
       </View>)
     : <T style={{fontSize:11,color:C.muted}}>No condition buckets were measured for this cell.</T>}
    <T style={{fontSize:9.5,lineHeight:13,color:C.muted}}>{cond.scope}</T>
   </Section>}

   {/* 5. Last 5 similar occurrences */}
   {!!last&&<Section title="Last 5 similar occurrences" note={last.rows.length?null:'none recorded'}>
    {!last.rows.length&&<T style={{fontSize:11,color:C.muted}}>No individual occurrences are stored for this cell.</T>}
    {last.rows.map((o,i)=><View key={`${o.entry_time}-${i}`} accessible
      accessibilityLabel={`${o.date}, ${link.symbol}: next move ${signed(o.next_move_pct)}, best ${plain(o.best_move_pct)}, worst ${plain(o.worst_move_pct)}, ${occurrenceOutcome(o)}`}
      style={[s.row,{gap:6,minHeight:21,borderTopWidth:i?1:0,borderColor:C.line}]}>
     <T numberOfLines={1} style={[num,{width:62,flexShrink:0,fontSize:10.5,lineHeight:15}]}>{o.date}</T>
     <T numberOfLines={1} style={[num,{width:52,flexShrink:0,fontSize:10.5,lineHeight:15,textAlign:'right',fontFamily:'InterSemi',color:tone(o.next_move_pct)}]}>{signed(o.next_move_pct)}</T>
     <T numberOfLines={1} style={[num,{width:46,flexShrink:0,fontSize:10,lineHeight:15,textAlign:'right',color:C.green}]}>{plain(o.best_move_pct)}</T>
     <T numberOfLines={1} style={[num,{width:46,flexShrink:0,fontSize:10,lineHeight:15,textAlign:'right',color:C.red}]}>{plain(o.worst_move_pct)}</T>
     <T numberOfLines={1} style={{flex:1,minWidth:0,fontSize:10,lineHeight:15,textAlign:'right',color:C.muted}}>{occurrenceOutcome(o)}</T>
    </View>)}
    {!!last.read&&<T style={{fontSize:11,lineHeight:15,fontFamily:'InterSemi',color:C.ink,marginTop:2}}>{last.read}</T>}
   </Section>}

   {/* 6. Honesty */}
   <Section title="How to read this">
    {honestyLines(card).map((line,i)=><View key={i} style={[s.row,{gap:6,alignItems:'flex-start'}]}>
     <T aria-hidden style={{fontSize:10,lineHeight:15,color:C.muted}}>·</T>
     <T style={{flex:1,minWidth:0,fontSize:10,lineHeight:14,color:/chance|requires review/i.test(line)?C.amber:C.muted}}>{line}</T>
    </View>)}
    <T style={{fontSize:9.5,lineHeight:13,color:C.muted,marginTop:2}}>
     Run {card.research_run?.slice(0,8)} · engine {card.engine_version||'?'}{card.snapshot_id?` · ${card.snapshot_id}`:''}
    </T>
   </Section>
  </ScrollView>;
 }
 return <CardShell label={label} style={style} header={header} footer={footer}>{body}</CardShell>;
}
