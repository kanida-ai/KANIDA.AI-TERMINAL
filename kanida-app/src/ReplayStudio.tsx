import React,{useState,useEffect,useRef} from 'react';
import {View,ScrollView,Pressable,Animated,AccessibilityInfo,Platform,Share,useWindowDimensions} from 'react-native';
import Svg,{Path,Line,Rect,Text as SvgText,Circle,G} from 'react-native-svg';
import {C,T,s,Button,Chip,Checkbox,Loading,Icon,Sheet,BREAKPOINTS,useLayoutMode} from './ui';
import {api,cellQuery,dateText,money,pct} from './model';
import {patternGeometry} from './patternGeometry';
import {TracedLine} from './PatternCanvas';
import {CAPITAL_CHOICES,DEFAULT_CAPITAL} from './constants';

// Named defaults (4.3), shared via constants.ts.
const REPLAY_CAPITAL_CHOICES=CAPITAL_CHOICES,DEFAULT_REPLAY_CAPITAL=DEFAULT_CAPITAL;
// Chart sizing (3.1): windows shorter than REPLAY_SHORT_WINDOW stack the account curve under the price chart and move less-used controls into "More".
const REPLAY_MIN_CHART=240,REPLAY_MAX_CHART=420,REPLAY_SHORT_WINDOW=720,REPLAY_STACKED_EQUITY=200;
const stamp=(s:string)=>new Date((s||'').replace(' ','T')+'+05:30').getTime();
function nearest(curve:any[],time:string){let best=0;for(let i=0;i<curve.length;i++){if(curve[i].time>time)break;best=i}return best}
export function Metric({label,value}:any){return <View style={{flex:1,minWidth:120,gap:3}}><T style={{fontSize:12,color:C.muted}}>{label}</T><T style={{fontSize:18,fontFamily:'InterSemi'}}>{value}</T></View>}

export function EquityPlot({curve=[],time,onTime,benchmark=[],height=250,normalized=false}:any){
 const [width,setWidth]=useState(500),[localTime,setLocalTime]=useState('');useEffect(()=>setLocalTime(''),[curve[0]?.time,curve[curve.length-1]?.time]);if(!curve.length)return <T>No account observations yet.</T>;
 const W=Math.max(280,width),H=height,L=62,R=14,B=36,top=20,now=time||localTime||[curve[curve.length-1].time,benchmark[benchmark.length-1]?.time||''].sort().at(-1);
 const values=curve.map((p:any)=>p.equity),lo=Math.min(...values,...benchmark.map((p:any)=>p.equity)),hi=Math.max(...values,...benchmark.map((p:any)=>p.equity)),pad=(hi-lo)*.08||100;
 const first=Math.min(stamp(curve[0].time),benchmark.length?stamp(benchmark[0].time):Infinity),last=Math.max(stamp(curve[curve.length-1].time),benchmark.length?stamp(benchmark[benchmark.length-1].time):0),span=last-first||1;
 const x=(t:string)=>L+(stamp(t)-first)/span*(W-L-R),y=(v:number)=>top+(hi+pad-v)/(hi-lo+2*pad)*(H-B-top);
 const shown=curve.filter((p:any)=>p.time<=now),selected=shown[shown.length-1]||curve[0];
 const path=(points:any[])=>points.map((p:any,i:number)=>`${i?'L':'M'}${x(p.time)},${y(p.equity)}`).join(' ');
 return <View onLayout={e=>setWidth(e.nativeEvent.layout.width)} style={{gap:6}}><View style={s.between}><T style={{fontSize:12,color:C.muted}}>Account equity · INR</T><T style={{fontFamily:'InterSemi',color:selected.equity>=curve[0].equity?C.green:C.red}}>{money(selected.equity,2)}</T></View><Pressable accessibilityRole="button" accessibilityLabel="Inspect a date on the account curve" onPress={e=>{const ev:any=e.nativeEvent,bounds=(e.currentTarget as any)?.getBoundingClientRect?.();const px=Number.isFinite(ev.locationX)?ev.locationX:ev.clientX-bounds.left;const t=first+Math.max(0,Math.min(1,(px-L)/(W-L-R)))*span;let i=0;while(i<curve.length-1&&stamp(curve[i].time)<t)i++;onTime?onTime(curve[i].time):setLocalTime(curve[i].time)}}><Svg width="100%" height={H} viewBox={`0 0 ${W} ${H}`} pointerEvents="none">
  {[0,1,2,3].map(i=>{const v=lo+(hi-lo)*i/3;return <G key={i}><Line x1={L} x2={W-R} y1={y(v)} y2={y(v)} stroke={C.line}/><SvgText x={L-8} y={y(v)+4} textAnchor="end" fill={C.muted} fontSize={11}>{Math.round(v).toLocaleString('en-IN')}</SvgText></G>})}
  {!!benchmark.length&&<Path d={path(benchmark.filter((p:any)=>p.time<=now))} stroke={C.muted} strokeWidth={1} strokeDasharray="4 4" fill="none"/>}
  {shown.length>1&&<><Path d={path(shown)+`L${x(selected.time)},${H-B}L${x(shown[0].time)},${H-B}Z`} fill={C.green} opacity={.07}/><Path d={path(shown)} stroke={C.green} strokeWidth={2} fill="none"/></>}
  <Line x1={x(selected.time)} x2={x(selected.time)} y1={top} y2={H-B} stroke={C.green} opacity={.35}/><Circle cx={x(selected.time)} cy={y(selected.equity)} r={3.5} fill={C.green}/>
  {[first,first+(last-first)/2,last].map(t=>({time:new Date(t+19800000).toISOString().slice(0,10)+' 00:00:00'})).map((p:any,i:number)=><SvgText key={i} x={i===0?L:i===2?W-R:x(p.time)} y={H-10} textAnchor={i===0?'start':i===2?'end':'middle'} fill={C.muted} fontSize={11}>{dateText(p.time)}</SvgText>)}
 </Svg></Pressable><T style={{fontSize:12,color:C.muted}}>{dateText(now||'')}{!normalized&&` · Cash ${money(selected.cash,2)} · Open P&L ${money(selected.unrealized,2)} · Decline ${selected.drawdown_pct?.toFixed(2)}%`}</T></View>;
}

export function HistoryChart({bundle,cursor,height=270}:any){
 const [width,setWidth]=useState(500),[reduce,setReduce]=useState(false);const progress=useRef(new Animated.Value(0)).current;
 const bars=bundle?.bars||[],index=Math.max(0,Math.min(cursor??bars.length-1,bars.length-1)),known=bars.slice(0,index+1),qualified=index>=bundle?.signal_index,shape=qualified?bundle?.shape:null;
 useEffect(()=>{AccessibilityInfo.isReduceMotionEnabled().then(setReduce)},[]);
 useEffect(()=>{progress.setValue(0);const animation=Animated.timing(progress,{toValue:1,duration:reduce?0:1300,useNativeDriver:false});if(qualified)animation.start();return()=>animation.stop()},[bundle,qualified,reduce]);
 if(!known.length)return <T>No chart candles available.</T>;
 const W=Math.max(280,width),H=height,L=8,R=62,top=22,B=38;
 const start=Math.max(0,Math.min(shape?.start_index??bundle?.shape?.start_index??0,index)-8),shown=known.slice(start);
 const lo=Math.min(...shown.map((b:any)=>b.low)),hi=Math.max(...shown.map((b:any)=>b.high)),pad=(hi-lo)*.1||1;
 const step=(W-L-R)/Math.max(15,shown.length),x=(i:number)=>L+(i-start+.5)*step,y=(v:number)=>top+(hi+pad-v)/(hi-lo+2*pad)*(H-B-top);
 const geometry=patternGeometry(shape,bars.slice(0,bundle.signal_index+1));
 const lines=(shape?.lines||[]).filter((l:any)=>l.role!=='anchors'&&l.points?.length>1);
 const trade=bundle.trade,selected=known[known.length-1];
 return <View style={{gap:6}} onLayout={e=>setWidth(e.nativeEvent.layout.width)}><View style={s.between}><T style={{fontSize:12,color:C.green}}>{qualified?'Pattern qualified · historical drawing':'Formation in progress'}</T><T style={{fontSize:12,color:C.muted}}>{bundle.timeframe} · INR</T></View><Svg width="100%" height={H} viewBox={`0 0 ${W} ${H}`}>
  {[0,1,2,3].map(i=>{const value=lo+(hi-lo)*i/3;return <G key={i}><Line x1={L} x2={W-R} y1={y(value)} y2={y(value)} stroke={C.line}/><SvgText x={W-R+8} y={y(value)+4} fontSize={11} fill={C.muted}>{Math.round(value).toLocaleString('en-IN')}</SvgText></G>})}
  {shown.map((b:any,i:number)=>{const col=b.close>=b.open?C.green:C.red;return <G key={i}><Line x1={x(start+i)} x2={x(start+i)} y1={y(b.high)} y2={y(b.low)} stroke={col} strokeWidth={1}/><Rect x={x(start+i)-step*.28} y={Math.min(y(b.open),y(b.close))} width={Math.max(.8,step*.56)} height={Math.max(1,Math.abs(y(b.close)-y(b.open)))} fill={col}/></G>})}
  {lines.map((l:any,i:number)=><TracedLine key={i} points={l.points.map((p:any)=>({x:x(p.index),y:y(p.value)}))} progress={progress} start={i/Math.max(1,lines.length)*.7} end={(i+1)/Math.max(1,lines.length)*.7} color={l.role==='curve'?'#B4E6FF':C.green}/>)}
  {geometry.swings.length>1&&<TracedLine points={geometry.swings.map(p=>({x:x(p.index),y:y(p.value)}))} progress={progress} start={.7} end={1} color={C.amber}/>}
  {trade&&index>=bundle.entry_index&&<><Line x1={x(bundle.entry_index)} x2={x(bundle.entry_index)} y1={top} y2={H-B} stroke={C.amber} strokeDasharray="3 3"/><SvgText x={Math.min(W-R-35,x(bundle.entry_index)+5)} y={top+10} fontSize={11} fill={C.amber}>Entry</SvgText></>}
  {trade&&index>=bundle.exit_index&&<><Circle cx={x(bundle.exit_index)} cy={y(trade.raw_exit??trade.exit)} r={5} fill={C.bg} stroke={C.ink}/><SvgText x={Math.min(W-R-28,x(bundle.exit_index)+5)} y={H-B-10} fontSize={11} fill={C.ink}>Exit</SvgText></>}
  <SvgText x={L} y={H-10} fontSize={11} fill={C.muted}>{dateText(shown[0].time)}</SvgText><SvgText x={W-R} y={H-10} textAnchor="end" fontSize={11} fill={C.muted}>{dateText(selected.end)}</SvgText>
 </Svg><T style={{fontSize:12,color:C.muted}}>{dateText(selected.time)} · {selected.time.slice(11,16)} IST · Close {money(selected.close)}</T></View>;
}

export async function exportStudy(result:any){
 const fields=['study_id','assumptions','rule','study_version','evidence_run','validation','symbol','timeframe','pattern','side','entry_time','exit_time','quantity','entry','exit','costs','net_pnl','net_return_pct','exit_reason'];
 const quote=(v:any)=>'"'+String(v??'').replace(/"/g,'""')+'"';
 const csv=[fields.join(','),...result.trades.map((t:any)=>fields.map(f=>quote(f==='study_id'?result.study_id||'original-baseline':f==='assumptions'?JSON.stringify(result.settings):f==='rule'?JSON.stringify(t.rule):f==='study_version'?result.engine_version||'legacy':f==='evidence_run'?result.run:f==='validation'?result.validation:t[f])).join(','))].join('\r\n');
 if(Platform.OS==='web'){const a=document.createElement('a');const url=URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'}));a.href=url;a.download='kanida-historical-trades.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000)}else await Share.share({message:csv,title:'KANIDA historical trades'});
}

export function ReplayStudio({result,jobId,onTest,actionLabel='Test these stocks and rules',embedded=false,capital,onCapital,onPlan,toolbar}:any){
 const [selected,setSelected]=useState(0),[mode,setMode]=useState('replay'),[allSignals,setAllSignals]=useState(false),[skippedMode,setSkippedMode]=useState(false),[bundle,setBundle]=useState<any>(result.first_chart),[cursor,setCursor]=useState(result.first_chart?.signal_index||0),[playing,setPlaying]=useState(false),[speed,setSpeed]=useState(1),[error,setError]=useState(''),[width,setWidth]=useState(900),[phonePane,setPhonePane]=useState('pattern'),[clock,setClock]=useState(result.first_chart?.bars?.[result.first_chart.signal_index]?.end||result.curve?.[0]?.time),[showBenchmark,setShowBenchmark]=useState(false);
 const generation=useRef(0),pendingTime=useRef<string|null>(null),continuous=useRef(false),items=skippedMode?result.skipped:allSignals?result.occurrences:result.trades,item=items?.[selected];
 const [fetching,setFetching]=useState(false),[visibleTrades,setVisibleTrades]=useState(100),[visibleSkips,setVisibleSkips]=useState(50),[availableHeight,setAvailableHeight]=useState(500),[controlsOpen,setControlsOpen]=useState(false);
 useEffect(()=>{setSelected(0);setAllSignals(false);setSkippedMode(false);continuous.current=false;pendingTime.current=null;setBundle(result.first_chart);setCursor(result.first_chart?.signal_index||0);setClock(result.first_chart?.bars?.[result.first_chart.signal_index]?.end||result.curve?.[0]?.time);setPlaying(false)},[result]);
 useEffect(()=>{if(!item){setBundle(null);return}const token=++generation.current;setError('');setFetching(true);setBundle(null);
  const path=jobId?`/api/studies/${jobId}/chart?${allSignals?'occurrence':'trade'}=${item.id}`:'/api/replay/chart?'+new URLSearchParams({symbol:item.symbol,timeframe:item.timeframe,pattern:item.pattern,side:item.side,signal:String(item.signal_index),run:result.run});
  api(path).then(v=>{if(token!==generation.current)return;const requested=pendingTime.current;pendingTime.current=null;let at=requested?v.bars.findIndex((b:any)=>b.end>=requested):continuous.current?Math.max(0,(v.shape?.start_index||0)-3):v.signal_index;if(at<0)at=v.bars.length-1;if(continuous.current)at=Math.max(at,Math.max(0,(v.shape?.start_index||0)-3));setBundle(v);setCursor(at);setClock(requested&&requested>v.bars[at]?.end?requested:v.bars[at]?.end);setFetching(false)}).catch(e=>{if(token===generation.current){setError(e.message);setPlaying(false);setFetching(false)}});
  return()=>{generation.current++};
 },[item?.id,allSignals,skippedMode,jobId,result.run]);
 useEffect(()=>{if(!playing||!bundle||fetching)return;const timer=setTimeout(()=>{if(cursor>=bundle.bars.length-1){if(continuous.current&&selected<items.length-1){pendingTime.current=clock;setSelected(selected+1)}else{setPlaying(false);continuous.current=false}return}const next=Math.min(bundle.bars.length-1,cursor+speed);setCursor(next);setClock((old:string)=>continuous.current&&old>bundle.bars[next].end?old:bundle.bars[next].end)},400);return()=>clearTimeout(timer)},[playing,bundle,speed,cursor,fetching,selected]);
 function jump(i:number){continuous.current=false;pendingTime.current=null;setPlaying(false);setSelected(Math.max(0,Math.min(items.length-1,i)))}
 function moveClock(time:string){continuous.current=false;setPlaying(false);setClock(time);const i=result.trades.findIndex((t:any)=>t.entry_time<=time&&t.exit_time>=time);if(i>=0&&(allSignals||skippedMode||i!==selected)){pendingTime.current=time;setSkippedMode(false);setAllSignals(false);setSelected(i)}else if(bundle){let j=bundle.bars.findIndex((b:any)=>b.end>=time);setCursor(j<0?bundle.bars.length-1:j)}}
 const chartReady=!!bundle&&bundle.symbol===item?.symbol&&bundle.pattern===item?.pattern&&bundle.timeframe===item?.timeframe&&bundle.signal_index+bundle.offset===item?.signal_index;
 const {height:windowHeight}=useWindowDimensions(),wide=width>=BREAKPOINTS.tablet,short=windowHeight<REPLAY_SHORT_WINDOW,stack=wide&&short,overhead=wide?(short?270:282):398;
 const summary=result.summary||{},last=result.curve?.[result.curve.length-1],replayHeight=Math.max(REPLAY_MIN_CHART,Math.min(REPLAY_MAX_CHART,availableHeight-overhead));const scrollBody=mode!=='replay'||stack||availableHeight<overhead+REPLAY_MIN_CHART;const Body=scrollBody?ScrollView:View;
 const equity=(h:number)=><EquityPlot height={h} curve={result.curve} time={clock} onTime={moveClock} benchmark={showBenchmark?result.benchmark?.curve:[]}/>;
 return <View style={{flex:1,minHeight:0}} onLayout={e=>{setWidth(e.nativeEvent.layout.width);setAvailableHeight(e.nativeEvent.layout.height)}}><ScrollView horizontal showsHorizontalScrollIndicator={false} style={{flexGrow:0,minHeight:52,maxHeight:52}} contentContainerStyle={{alignItems:'center',gap:6,paddingVertical:4}}>{[['replay','Visual replay'],['results','Results'],['trades','Trades'],['stocks','Stock comparison'],['rules','Rules & data'],...(result.folds?.length?[['walkforward','Walk-forward periods']]:[])].map(([v,l])=><Chip key={v} label={l} active={mode===v} onPress={()=>{setMode(v);setPlaying(false)}}/>)}{!!toolbar&&<><View style={{width:1,height:28,backgroundColor:C.line,marginHorizontal:4}}/>{toolbar}</>}</ScrollView>
 <Body {...(scrollBody?{showsVerticalScrollIndicator:false,contentContainerStyle:{gap:mode==='replay'?6:14,paddingBottom:12}}:{})} style={{flex:1,minHeight:0,...(!scrollBody?{gap:6}:{})}}>
  {mode!=='replay'&&<View style={s.between}><T style={{fontSize:12,color:result.legacy?C.amber:C.green}}>{result.validation}</T><T style={{fontSize:12,color:C.muted}}>{summary.trades||0} executed · {summary.skipped||0} skipped</T></View>}
  {mode==='replay'&&<>
   <View style={[s.between,{flexWrap:'wrap'}]}><View style={{flex:1,minWidth:170}}>{(!embedded||wide)&&<T style={{fontFamily:'InterSemi',fontSize:16}}>{item?.symbol||'No historical trades'} {item?.timeframe} · {item?.pattern_name||bundle?.pattern_name}</T>}<T style={{fontSize:12,color:C.muted}}>Formation {dateText(item?.formation_start)} → {dateText(item?.signal_time||item?.time)} · {skippedMode?'Skipped signal':allSignals?'Occurrence':'Trade'} {items?.length?selected+1:0} / {items?.length||0}</T></View><View style={[s.row,{gap:0}]}><Pressable accessibilityRole="button" accessibilityLabel="Previous instance" onPress={()=>jump(selected-1)} disabled={selected===0} style={{padding:12,opacity:selected===0?.3:1}}><Icon name="chevron-left"/></Pressable><Pressable accessibilityRole="button" accessibilityLabel="Next instance" onPress={()=>jump(selected+1)} disabled={selected>=items.length-1} style={{padding:12,opacity:selected>=items.length-1?.3:1}}><Icon name="chevron-right"/></Pressable></View></View>
   {jobId&&wide&&!short&&<Checkbox checked={allSignals} label="Include every detected occurrence, including untraded signals" onChange={next=>{setSkippedMode(false);setAllSignals(next);setSelected(0);setPlaying(false)}}/>}
   {!wide&&<View style={s.row}><Chip label="Pattern" active={phonePane==='pattern'} onPress={()=>setPhonePane('pattern')}/><Chip label="Money curve" active={phonePane==='equity'} onPress={()=>setPhonePane('equity')}/></View>}
   {skippedMode&&<T style={{fontSize:12,color:C.amber}}>{item?.skipped_reason}</T>}
   {!!error&&<T style={{color:C.red}}>{error}</T>}
   <View style={{flexDirection:wide&&!stack?'row':'column',gap:18}}>{(wide||phonePane==='pattern')&&<View style={{flex:1,minWidth:0,backgroundColor:C.dark,padding:10,borderRadius:10}}>{item&&(fetching||!chartReady)?<Loading/>:bundle?<HistoryChart bundle={bundle} cursor={cursor} height={replayHeight}/>:<T>No entry matched these rules. Inspect all occurrences or change the study settings.</T>}</View>}{((wide&&!stack)||(!wide&&phonePane==='equity'))&&<View style={{flex:1,minWidth:0,backgroundColor:C.dark,padding:10,borderRadius:10}}>{equity(replayHeight)}</View>}</View>
   <View style={[s.row,{flexWrap:'wrap',gap:5}]}>
    <Button label={playing?'Pause':!wide?'Play':'Play this instance'} icon={playing?'pause':'play'} kind="soft" disabled={!bundle} onPress={()=>{continuous.current=false;if(!playing&&bundle){const from=Math.max(0,(bundle.shape?.start_index||0)-3);setCursor(from);setClock(bundle.bars[from]?.end)}setPlaying(!playing)}}/>
    {wide&&<Button label={allSignals?"Play all occurrences":skippedMode?"Play skipped signals":"Play all trades"} kind="outline" disabled={!bundle||!items.length} onPress={()=>{continuous.current=true;pendingTime.current=null;if(selected!==0)setSelected(0);else{const from=Math.max(0,(bundle.shape?.start_index||0)-3);setCursor(from);setClock(bundle.bars[from]?.end)}setPlaying(true)}}/>}
    <Button label={`${speed}× speed`} kind="ghost" onPress={()=>setSpeed(speed===1?3:speed===3?8:1)}/>
    {wide&&!short?<>
     <Button label="Step candle" kind="ghost" disabled={!bundle||cursor>=bundle.bars.length-1} onPress={()=>{setPlaying(false);setCursor(cursor+1);setClock(bundle.bars[cursor+1].end)}}/>
     <Button label="Show final account" kind="ghost" onPress={()=>{setPlaying(false);setClock(last?.time);if(bundle)setCursor(bundle.bars.length-1)}}/>
     {result.benchmark&&<Button label={showBenchmark?'Hide benchmark':'Compare buy & hold'} kind="ghost" onPress={()=>setShowBenchmark(!showBenchmark)}/>}
    </>:<Button label="More" kind="outline" icon="more-horizontal" onPress={()=>setControlsOpen(true)}/>}
   </View>
   {bundle&&(Platform.OS==='web'?React.createElement('input',{type:'range','aria-label':'Historical candle timeline',min:0,max:bundle.bars.length-1,value:cursor,onChange:(e:any)=>{continuous.current=false;setPlaying(false);const i=Number(e.target.value);setCursor(i);setClock(bundle.bars[i].end)},style:{width:'100%',minHeight:22,accentColor:C.green,cursor:'pointer'}}):<Pressable accessibilityRole="adjustable" accessibilityLabel="Historical candle timeline" onPress={e=>{const ev:any=e.nativeEvent,bounds=(e.currentTarget as any)?.getBoundingClientRect?.();const px=Number.isFinite(ev.locationX)?ev.locationX:ev.clientX-bounds.left;const w=bounds?.width||width;const i=Math.round(Math.max(0,Math.min(1,px/w))*(bundle.bars.length-1));setPlaying(false);setCursor(i);setClock(bundle.bars[i].end)}} style={{minHeight:36,justifyContent:'center'}}><View style={{height:5,backgroundColor:C.line,borderRadius:5}}><View style={{width:`${cursor/Math.max(1,bundle.bars.length-1)*100}%`,height:5,backgroundColor:C.green,borderRadius:5}}/></View></Pressable>)}
   {item?.entry_time&&!skippedMode&&<T style={{fontSize:12,color:C.muted}}>Entry {dateText(item.entry_time)} {item.entry_time.slice(11,16)} → Exit {dateText(item.exit_time||item.exit_candle_end)} · {item.quantity} shares · {money(item.net_pnl,2)} net · {item.exit_reason?.replaceAll('_',' ')}</T>}
   {stack&&<View style={{backgroundColor:C.dark,padding:10,borderRadius:10}}>{equity(REPLAY_STACKED_EQUITY)}</View>}
  </>}
  {mode==='results'&&<>
   <View style={[s.row,{flexWrap:'wrap',gap:20}]}>
    <Metric label="Starting capital" value={money(summary.starting_capital)}/>
    <Metric label="Ending account" value={money(summary.ending_equity,2)}/>
    <Metric label="Net profit / loss" value={money(summary.net_profit,2)}/>
    <Metric label="Average net / trade" value={pct(summary.average_return_pct)}/>
    <Metric label="Profitable trades" value={`${summary.wins||0} of ${summary.trades||0}`}/>
    <Metric label="Maximum equity decline" value={pct(-summary.max_drawdown_pct)}/>
    <Metric label="Best completed trade" value={pct(summary.best_trade_pct)}/>
    <Metric label="Worst completed trade" value={pct(summary.worst_trade_pct)}/>
    <Metric label="Median hold" value={`${summary.median_holding_days??'—'} calendar ${summary.median_holding_days===1?'day':'days'}`}/>
    <Metric label="Median hold in candles" value={String(summary.median_holding_bars??'—')}/>
    <Metric label="Largest favorable move" value={pct(summary.max_favorable_pct)}/>
    <Metric label="Largest adverse move" value={summary.max_adverse_pct==null?'—':pct(-summary.max_adverse_pct)}/>
    <Metric label="Total fees assumed" value={money(summary.total_costs,2)}/>
   </View>
   <EquityPlot curve={result.curve} benchmark={result.benchmark?.curve} onTime={(time:string)=>{setMode('replay');moveClock(time)}}/>
   {result.benchmark&&<T style={{fontSize:12,color:C.muted}}>{result.benchmark.label}</T>}
   <T style={{fontFamily:'InterSemi'}}>What the evidence says</T>
   <T>{summary.trades?`${summary.wins} of ${summary.trades} completed trades were profitable. The average net result was ${pct(summary.average_return_pct)} per trade.`:'No trades met the execution and evidence rules in this period.'} {summary.trades<20?'This is a small sample. A few trades can change the result.':''}</T>
   {result.contributions?.slice(0,8).map((c:any)=><Pressable key={c.symbol} accessibilityRole="button" accessibilityLabel={`Replay ${c.symbol} trades`} onPress={()=>{setSelected(result.trades.findIndex((t:any)=>t.symbol===c.symbol));setAllSignals(false);setMode('replay')}} style={[s.between,{paddingVertical:10,borderBottomWidth:1,borderColor:C.line}]}>
    <T>{c.symbol}</T>
    <T style={{fontSize:12,color:C.muted}}>{c.wins} / {c.trades} wins · {pct(c.average_return_pct)} avg.</T>
    <T>{money(c.net_pnl,2)}</T>
   </Pressable>)}
  </>}
  {mode==='trades'&&<>
   <View style={s.between}><T>Every executed trade</T><Button label="Export CSV" kind="outline" onPress={()=>exportStudy({...result,study_id:jobId})}/></View>
   {result.trades.slice(0,visibleTrades).map((t:any,i:number)=><Pressable key={t.id} accessibilityRole="button" accessibilityLabel={`Replay trade ${i+1} ${t.symbol}`} onPress={()=>{setSkippedMode(false);setAllSignals(false);setSelected(i);setMode('replay')}} style={{paddingVertical:13,borderBottomWidth:1,borderColor:C.line,gap:4}}>
    <View style={s.between}><T>{t.symbol} · {t.pattern_name||bundle?.pattern_name} · {t.timeframe}</T><T style={{color:t.net_pnl>=0?C.green:C.red}}>{money(t.net_pnl,2)}</T></View>
    <T style={{fontSize:12,color:C.muted}}>{dateText(t.entry_time)} → {dateText(t.exit_time)} · {t.quantity} shares · {pct(t.net_return_pct)} · {t.holding_bars} candles</T>
   </Pressable>)}
   {result.trades.length>visibleTrades&&<Button label="Show 100 more trades" kind="outline" onPress={()=>setVisibleTrades(visibleTrades+100)}/>}
   <T>Skipped signals</T>
   {result.skipped.slice(0,visibleSkips).map((t:any,i:number)=><Pressable key={t.id} accessibilityRole="button" accessibilityLabel={`Inspect skipped signal ${i+1} ${t.symbol}`} onPress={()=>{setSkippedMode(true);setAllSignals(false);setSelected(i);setMode('replay')}} style={{paddingVertical:8}}>
    <T>{t.symbol} · {t.timeframe}</T>
    <T style={{fontSize:12,color:C.muted}}>{dateText(t.entry_time||t.signal_time)} · {t.skipped_reason}</T>
   </Pressable>)}
   {result.skipped.length>visibleSkips&&<Button label="Show 50 more skipped signals" kind="outline" onPress={()=>setVisibleSkips(visibleSkips+50)}/>}
  </>}
  {mode==='stocks'&&<><T>Each stock keeps its own results. These are contributions to the same shared account.</T>{(result.contributions||[]).map((c:any)=><Pressable key={c.symbol} accessibilityRole="button" accessibilityLabel={`Replay ${c.symbol} contribution`} onPress={()=>{setSkippedMode(false);jump(result.trades.findIndex((t:any)=>t.symbol===c.symbol));setAllSignals(false);setMode('replay')}} style={{padding:14,borderBottomWidth:1,borderColor:C.line,gap:7}}><View style={s.between}><T>{c.symbol}</T><T style={{color:c.net_pnl>=0?C.green:C.red}}>{money(c.net_pnl,2)}</T></View><T style={{fontSize:12,color:C.muted}}>{c.wins} of {c.trades} profitable · {pct(c.average_return_pct)} average per trade{c.trades<20?' · Small sample':''}</T></Pressable>)}{!result.contributions?.length&&<T>This replay contains {item?.symbol||'no executed stock trades'} only.</T>}</>}
  {mode==='rules'&&<><T style={{fontFamily:'InterSemi'}}>Settings saved with this study</T>{Object.entries(result.settings||{}).filter(([k])=>!['run','symbols','candidate_family'].includes(k)).map(([k,v])=><View key={k} style={[s.between,{alignItems:'flex-start',gap:16,paddingVertical:6}]}><T style={{fontSize:12,color:C.muted,flex:1}}>{k.replaceAll('_',' ')}</T><T style={{fontSize:12,flex:2}}>{typeof v==='object'?JSON.stringify(v):String(v)}</T></View>)}<T style={{fontSize:12,color:C.muted}}>Study {jobId||'original baseline'} · Evidence {result.run} · Engine {result.engine_version||'original research'}</T>{result.curve_sampled&&<T style={{fontSize:12,color:C.muted}}>The curve is sampled for display. Maximum decline uses every available account observation.</T>}{result.settings?.candidate_family&&<T style={{fontSize:12,color:C.muted}}>The candidate rule family is frozen with the saved study and included in its CSV assumptions.</T>}{result.coverage?.map((c:any)=><T key={c.symbol+c.timeframe} style={{fontSize:12,color:C.muted}}>{c.symbol} · {c.timeframe} · {dateText(c.first)} → {dateText(c.last)} {c.message||''}</T>)}</>}
  {mode==='walkforward'&&<><T>Each rule was selected using earlier completed trades. Cash continues across test periods.</T>{result.folds.map((f:any,i:number)=><View key={i} style={{padding:14,gap:5,borderBottomWidth:1,borderColor:C.line}}><T>{f.symbol} · {f.timeframe} · {f.pattern.replaceAll('_',' ')}</T><T style={{fontSize:12,color:C.muted}}>Learn from {f.learning_start} · Test {f.test_start} to {f.test_end}</T><T>{f.rule?`${f.training_trades} learning trades · ${f.test_trades} executed test trades · ${money(f.net_pnl,2)}`:'No rule met the minimum positive learning evidence. No new trades in this period.'}</T>{f.rule&&<T style={{fontSize:12,color:C.muted}}>Hold ≤ {f.rule.hold} candles · {f.rule.stop_atr?`${f.rule.stop_atr} ATR stop / ${f.rule.target_r}R target`:'Time exit'}</T>}</View>)}</>}
  {mode!=='replay'&&<T style={{fontSize:12,color:C.muted}}>{result.note}</T>}
  {onPlan&&mode==='results'&&item&&!skippedMode&&!allSignals&&<Button label={`Review ${item.symbol} for AutoTrade`} icon="zap" onPress={()=>onPlan(item)}/>}
  {onTest&&mode!=='replay'&&<Button label={actionLabel} kind="outline" icon="sliders" onPress={onTest}/>}
 </Body>
 <Sheet visible={controlsOpen} title="Replay controls" onClose={()=>setControlsOpen(false)}>
  {jobId&&<Button label={allSignals?"Show executed trades":"Include all detected occurrences"} kind="outline" onPress={()=>{setSkippedMode(false);setAllSignals(!allSignals);setSelected(0);setPlaying(false);setControlsOpen(false)}}/>}
  {onCapital&&<><T>Starting capital</T><View style={[s.row,{flexWrap:'wrap'}]}>{REPLAY_CAPITAL_CHOICES.map(v=><Chip key={v} label={money(v)} active={capital===v} onPress={()=>{onCapital(v);setControlsOpen(false)}}/>)}</View></>}
  <Button label={allSignals?"Play all occurrences":skippedMode?"Play skipped signals":"Play all historical trades"} disabled={!bundle} onPress={()=>{continuous.current=true;pendingTime.current=null;setSelected(0);if(selected===0){const from=Math.max(0,(bundle.shape?.start_index||0)-3);setCursor(from);setClock(bundle.bars[from]?.end)}setPlaying(true);setControlsOpen(false)}}/>
  <Button label="Step one candle" kind="outline" disabled={!bundle||cursor>=bundle.bars.length-1} onPress={()=>{setPlaying(false);setCursor(cursor+1);setClock(bundle.bars[cursor+1].end);setControlsOpen(false)}}/>
  <Button label="Show final account" kind="outline" onPress={()=>{setPlaying(false);setClock(last?.time);if(bundle)setCursor(bundle.bars.length-1);setPhonePane('equity');setControlsOpen(false)}}/>
  {result.benchmark&&<Button label={showBenchmark?'Hide benchmark':'Compare buy & hold'} kind="outline" onPress={()=>{setShowBenchmark(!showBenchmark);setControlsOpen(false)}}/>}
 </Sheet></View>;
}

export function StockReplay({match,onTest}:any){
 const tablet=useLayoutMode()!=='phone';const [result,setResult]=useState<any>(null),[error,setError]=useState(''),[capital,setCapital]=useState(DEFAULT_REPLAY_CAPITAL),[side,setSide]=useState(match.history[0]?.side||'long');
 const chips=<>{REPLAY_CAPITAL_CHOICES.map(v=><Chip key={v} label={money(v)} active={capital===v} onPress={()=>setCapital(v)}/>)}{match.history.length>1&&match.history.map((h:any)=><Chip key={h.side} label={h.side==='long'?'Long history':'Short price history'} active={side===h.side} onPress={()=>setSide(h.side)}/>)}</>;
 // A historical replay is a walk over a STORED backtest cell. A match with no history has no such cell, so
 // the request is not made: it can only fail, and the panel can say why from the same fact.
 const replayable=(match.history||[]).length>0;
 useEffect(()=>{let active=true;setResult(null);setError('');
  if(!replayable){setError('No historical evidence for this setup, so there is nothing to replay.');return}
  api('/api/replay?'+cellQuery(match,side)+'&capital='+capital).then(v=>{if(active)setResult(v)}).catch(e=>{if(active)setError(e.message)});return()=>{active=false}},[match.id,side,capital,replayable]);
 return <View style={{flex:1,minHeight:0}}>
  {tablet&&!result&&<View style={[s.row,{gap:5,flexWrap:'wrap'}]}>{chips}</View>}
  {!tablet&&match.history.length>1&&<View style={s.row}>{match.history.map((h:any)=><Chip key={h.side} label={h.side==='long'?'Long history':'Short history'} active={side===h.side} onPress={()=>setSide(h.side)}/>)}</View>}
  {error?<T style={{color:C.red}}>{error}</T>:result?<ReplayStudio result={result} onTest={onTest} embedded capital={capital} onCapital={setCapital} toolbar={tablet?chips:undefined}/>:<Loading/>}
 </View>;
}
