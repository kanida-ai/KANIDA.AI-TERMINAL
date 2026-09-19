// Backtest card (spec §3.6 + §8), clicked stock only. Tested cell → later-test (out-of-sample) stats + test trades; otherwise hold-period history, labelled as not a tested exit rule.
// Dense card: header and honesty footer (evidence basis · costs · research only) are fixed; the body scrolls (badges, 2-column metrics, compact cumulative line, last trades).
import React,{useEffect,useMemo,useState} from 'react';
import {View,ScrollView,Pressable} from 'react-native';
import Svg,{Path,Line} from 'react-native-svg';
import {C,T,Badge,s} from '../ui';
import {api,pct,money,dateText} from '../model';
import {sampleSize} from '../decision';
import {useKeyedValue} from '../activeSymbol';
import {WidgetError} from '../layout';
import {linkKey,type BlockLink} from './link';
import {backtestView,tradeSummary,verdictFor,holdShort,holdText,VERDICT_TITLES,COST_LINE} from './logic';
import {CardShell,CenterNote,LinkText,Skeleton} from './parts';
import {openChart,LinkedHeader,EmptyHeader} from './ChartCard';
const num={fontVariant:['tabular-nums'] as any};
function Metric({label,value,color,note,full,a11y}:{label:string;value:string;color?:string;note?:string;full?:boolean;a11y?:string}){
 return <View accessible accessibilityLabel={a11y||`${label} ${value}${note?`, ${note}`:''}`} style={{flexDirection:'row',alignItems:'baseline',gap:5,flexBasis:full?'100%':'40%',flexGrow:1,minWidth:0}}>
  <T numberOfLines={1} style={{flexShrink:0,fontSize:11,lineHeight:16,color:C.muted}}>{label}</T>
  <T numberOfLines={1} style={[num,{flexShrink:1,minWidth:0,fontFamily:'InterSemi',fontSize:12.5,lineHeight:17,color:color||C.ink}]}>{value}</T>
  {!!note&&<T numberOfLines={1} style={{flexShrink:1,minWidth:0,fontSize:10,lineHeight:14,color:C.amber}}>{note}</T>}
 </View>;
}
const tone=(v:number|null|undefined)=>v==null?C.muted:v>0?C.green:v<0?C.red:C.ink;
function CumulativeLine({curve}:{curve:number[]}){
 const [w,setW]=useState(0),H=56;if(curve.length<2)return <T style={{fontSize:11,color:C.muted}}>Not enough trades to draw a line.</T>;
 const vals=[0,...curve],lo=Math.min(...vals),hi=Math.max(...vals),span=hi-lo||1,x=(i:number)=>2+i*(Math.max(w,10)-4)/(vals.length-1),y=(v:number)=>4+(hi-v)/span*(H-8);
 const last=curve[curve.length-1];
 return <View onLayout={e=>setW(Math.round(e.nativeEvent.layout.width))} accessibilityLabel={`Trade-by-trade cumulative return across ${curve.length} trades, ending ${pct(last)}. Not a portfolio.`} style={{height:H}}>{w>0&&<Svg width={w} height={H}><Line x1={0} x2={w} y1={y(0)} y2={y(0)} stroke={C.line} strokeDasharray="3 3"/><Path d={vals.map((v,i)=>`${i?'L':'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(' ')} stroke={last>=0?C.green:C.red} strokeWidth={1.6} fill="none"/></Svg>}</View>;
}
export function BacktestCard({link,minTrades=10,style,stale,ageDays,color}:{link:BlockLink|null;minTrades?:number;style?:any;stale?:boolean;ageDays?:number|null;color?:string}){
 const key=linkKey(link);
 const {value:cell,error,loading,reload}=useKeyedValue<any>(key,async()=>{
  const l=link!;const d=await api(`/api/backtests/cell?symbol=${encodeURIComponent(l.symbol)}&timeframe=${encodeURIComponent(l.timeframe)}&pattern=${encodeURIComponent(l.pattern)}&side=${l.side}`);
  if(d?.symbol&&(String(d.symbol).toUpperCase()!==l.symbol||(d.timeframe&&d.timeframe!==l.timeframe)||(d.pattern&&d.pattern!==l.pattern)||(d.side&&d.side!==l.side)))throw new Error('The backtest response did not match the selected stock.');
  return d;
 });
 const [all,setAll]=useState(false);useEffect(()=>setAll(false),[key]);
 const view=useMemo(()=>backtestView(cell),[cell]),sum=useMemo(()=>tradeSummary(view.trades),[view.trades]);
 const label=link?`Backtest · ${link.symbol} · ${link.patternName} ${link.timeframe}`:'Backtest';
 const header=link?<LinkedHeader title={`Backtest · ${link.symbol} · ${link.timeframe}`} slot={link.slot} color={color} detail={`${link.patternName} · ${link.side==='long'?'Long':'Short'}`} sourceA11y={`${link.patternName} ${link.timeframe}, ${link.side}, from scanner ${link.slot}`}
  link={<LinkText label="Evidence →" a11y={`Open evidence and replay for ${link.symbol} ${link.timeframe}`} onPress={()=>openChart(link,'evidence')}/>}/>:<EmptyHeader title="Backtest"/>;
 const tested=view.basis==='tested_rule',ready=!!link&&!loading&&!error&&view.basis!=='none';
 const footer=<>
  {ready&&<T style={{fontSize:11,lineHeight:15,fontFamily:'InterSemi',color:tested?C.green:C.amber}}>{view.label}</T>}
  <T style={{fontSize:11,lineHeight:15,color:C.muted}}>{COST_LINE}{tested&&ready?' · later-test only':''} · {link&&stale?<T style={{fontSize:11,lineHeight:15,color:C.amber}}>research only{ageDays!=null?` (${ageDays}d old)`:''}</T>:'research only'}</T>
 </>;
 let body:React.ReactNode;
 if(!link)body=<CenterNote icon="bar-chart-2" text="Click a stock in a scanner card to see how this pattern has done on it, after costs."/>;
 else if(error)body=<View style={{padding:12}}><WidgetError title="Backtest unavailable" message={`Backtest results for ${link.symbol} ${link.timeframe} could not be loaded. ${error}`} onRetry={reload}/></View>;
 else if(loading)body=<Skeleton lines={6}/>;
 else if(view.basis==='none')body=<CenterNote icon="info" text={`No historical ${link.patternName} trades are recorded for ${link.symbol} ${link.timeframe}. Nothing to show — no result is implied.`}/>;
 else{
  const st=view.stats,n=st?.n||0,verdict=verdictFor(st,minTrades),sample=sampleSize(n),ci=Array.isArray(st?.expectancy_ci95)?st.expectancy_ci95:null;
  const recent=[...sum.list].reverse(),shown=all?recent:recent.slice(0,5),few=n<minTrades;
  body=<ScrollView style={{flex:1}} contentContainerStyle={{paddingHorizontal:12,paddingVertical:10,gap:9}}>
   <View style={[s.row,{flexWrap:'wrap',gap:6}]}><Badge label={VERDICT_TITLES[verdict]} tone={verdict==='review'?'green':verdict==='pass'?'red':'neutral'} dot/><Badge label={sample.label} tone={sample.tone}/>{cell?.status==='small_test_sample'&&<Badge label="Later-test sample too small" tone="amber"/>}</View>
   {!!view.rule&&<T numberOfLines={2} style={{fontSize:10.5,lineHeight:14,color:C.muted}}>Rule: {view.rule}</T>}
   <View style={{flexDirection:'row',flexWrap:'wrap',rowGap:5,columnGap:12}}>
    <Metric label="Trades" value={String(n)} color={few?C.amber:undefined} note={few?`below ${minTrades}`:undefined} a11y={`Trades ${n}${few?`, below the ${minTrades} needed as evidence`:''}`}/>
    <Metric label="Avg net" value={pct(st?.expectancy_pct)} color={tone(st?.expectancy_pct)} a11y={`Average after costs ${pct(st?.expectancy_pct)}`}/>
    <Metric label="Worst" value={pct(sum.worst)} color={tone(sum.worst)} a11y={`Worst trade ${pct(sum.worst)}`}/>
    <Metric label="Avg hold" value={holdShort(sum.avgHold,link.timeframe)} a11y={`Average hold ${holdText(sum.avgHold,link.timeframe)}`}/>
    <Metric full label="95% range" value={ci?`${pct(ci[0])} to ${pct(ci[1])}`:'—'} color={tone(ci?.[0])} a11y={`95% range of the average after costs: ${ci?`${pct(ci[0])} to ${pct(ci[1])}`:'not available'}`}/>
    <Metric full label="Win rate" value={st?.win_rate==null?'—':`${Number(st.win_rate).toFixed(0)}%`} note="context only" a11y={`Win rate ${st?.win_rate==null?'not available':`${Number(st.win_rate).toFixed(0)}%`}, context only`}/>
   </View>
   <View style={{gap:3}}><T numberOfLines={1} style={{fontSize:10,lineHeight:14,color:C.muted}}>Cumulative · {sum.list.length} trades · sum {pct(sum.total)} · not a portfolio</T><CumulativeLine curve={sum.curve}/></View>
   <View style={{gap:1}}>
    <T style={[s.label,{fontSize:10}]}>{all?'All trades':'Last 5 trades'}</T>
    {!shown.length&&<T style={{fontSize:11,color:C.muted}}>No individual trades are stored for this history.</T>}
    {shown.map((t:any,i:number)=><View key={`${t.entry_time}-${i}`} accessible accessibilityLabel={`Entry ${dateText(t.entry_time)} at ${money(t.entry,2)}, exit ${money(t.exit,2)}, ${pct(t.net_return_pct)} after costs${t.exit_reason?`, ${t.exit_reason}`:''}`} style={[s.row,{gap:6,minHeight:22,borderTopWidth:i?1:0,borderColor:C.line}]}>
     <T numberOfLines={1} style={[num,{fontSize:11,lineHeight:15,width:74,flexShrink:0}]}>{dateText(t.entry_time)}</T>
     <T numberOfLines={1} style={[num,{fontSize:11,lineHeight:15,flex:1,minWidth:0,color:C.muted}]}>{money(t.entry,2)} → {money(t.exit,2)}</T>
     <T numberOfLines={1} style={[num,{fontSize:11,lineHeight:15,fontFamily:'InterSemi',width:56,flexShrink:0,textAlign:'right',color:tone(t.net_return_pct)}]}>{pct(t.net_return_pct)}</T>
    </View>)}
    {recent.length>5&&<Pressable accessibilityRole="button" accessibilityLabel={all?'Show last 5 trades':`Show all ${recent.length} trades`} accessibilityState={{expanded:all}} onPress={()=>setAll(v=>!v)} style={{minHeight:28,justifyContent:'center'}}><T style={{fontSize:12,fontFamily:'InterSemi',color:C.green}}>{all?'Show last 5':`All trades (${recent.length})`}</T></Pressable>}
   </View>
  </ScrollView>;
 }
 return <CardShell label={label} style={style} header={header} footer={footer}>{body}</CardShell>;
}
