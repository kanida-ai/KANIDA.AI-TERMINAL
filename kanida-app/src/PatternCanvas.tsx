import React,{useState,useEffect,useLayoutEffect,useId,useMemo,useCallback,useRef,useSyncExternalStore,memo} from 'react';
import {View,Pressable,ScrollView,Animated,AccessibilityInfo,Easing,Platform} from 'react-native';
import Svg,{Line,Rect,Path,Circle,G,Text as SvgText,Defs,ClipPath,LinearGradient,Stop} from 'react-native-svg';
import {api,Match,dateText,money} from './model';
import {C,T,s,Button,Icon,Loading} from './ui';
import {patternGeometry} from './patternGeometry';
import {chartStart,ChartWindow} from './chartWindow';
import {visualPattern,isResearchPattern,isOriginalChart,visibleDrawingValues,candleRanges,drawingColor,hasDrawing,placedLabels,drawingLines} from './researchDrawing';
const InkPath:any=Animated.createAnimatedComponent(Path);
const InkGroup:any=Animated.createAnimatedComponent(G);
type Point={x:number;y:number};
const linePath=(points:Point[])=>points.map((p,i)=>`${i?'L':'M'}${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(' ');
// Animated.timing's default curve, kept so the reveal looks exactly as before.
const DRAW_MS=3300,inkEase=Easing.inOut(Easing.ease);
// Fraction of the reveal's duration at which eased progress reaches v.
const easedTime=(v:number)=>{if(v<=0)return 0;if(v>=1)return 1;let a=0,b=1;for(let i=0;i<32;i++){const m=(a+b)/2;if(inkEase(m)<v)a=m;else b=m}return (a+b)/2};
type Segment={from:number;to:number;t0:number;t1:number;value:Animated.Value;easing:(u:number)=>number};
// One progress window on its own value. It ticks only while that window draws, so finished or waiting ink does no per-frame work.
// The easing reproduces the single 0→1 timing, so each value equals the old global progress at every instant.
const inkSegment=(from:number,to:number):Segment=>{const t0=easedTime(from),t1=easedTime(to),span=Math.max(t1-t0,1e-6);return {from,to,t0,t1,value:new Animated.Value(from),easing:u=>(inkEase(t0+u*span)-from)/((to-from)||1)}};
export const TracedLine=memo(function TracedLine({points,progress,start,end,color,pen=true}:any){
 const ink=useMemo(()=>{
  if(points.length<2)return null;
  let travelled=0;const along=points.map((p:Point,i:number)=>{if(i)travelled+=Math.hypot(p.x-points[i-1].x,p.y-points[i-1].y);return travelled});
  const length=travelled||1,range=along.map((d:number)=>start+(end-start)*d/length);
  const offset=progress.interpolate({inputRange:[start,end],outputRange:[length,0],extrapolate:'clamp'});
  if(!pen)return {d:linePath(points),length,offset};
  const visible=progress.interpolate({inputRange:[start,start+.001,end,end+.001],outputRange:[0,1,1,0],extrapolate:'clamp'});
  // A single translate() moves the whole pen tip (previously 8 x/y interpolations on 3 nodes). Long resampled curves steer the tip
  // through at most 24 of their own vertices; the inked path itself keeps every point.
  const stride=Math.ceil(points.length/24),keep=points.map((_:Point,i:number)=>i).filter((i:number)=>i%stride===0||i===points.length-1);
  const tip=progress.interpolate({inputRange:keep.map((i:number)=>range[i]),outputRange:keep.map((i:number)=>`translate(${points[i].x.toFixed(2)},${points[i].y.toFixed(2)})`),extrapolate:'clamp'});
  return {d:linePath(points),length,offset,visible,tip};
 },[points,progress,start,end,pen]);
 if(!ink)return null;
 return <><InkPath d={ink.d} stroke={color} strokeWidth={1.65} strokeDasharray={`${ink.length} ${ink.length}`} strokeDashoffset={ink.offset} fill="none" strokeLinecap="round" strokeLinejoin="round"/>{pen&&<InkGroup opacity={ink.visible} transform={ink.tip}><Circle r={5} fill={color} opacity={.2}/><Circle r={2} fill="#F4FEFF"/><Line x1={1} x2={8} y1={-1} y2={-8} stroke="#F2FFFF" strokeWidth={2.4} strokeLinecap="round"/></InkGroup>}</>;
});
// Hover/tap cursor lives in a tiny external store, not PatternCanvas state: only the crosshair marks and the readout subscribe, so a cursor move never
// re-renders the canvas body, the memoised candle/ink layers or the reveal animation.
type CursorStore={get:()=>number|null;set:(v:number|null)=>void;subscribe:(f:()=>void)=>()=>void};
function cursorStore():CursorStore{let value:number|null=null;const subs=new Set<()=>void>();return {get:()=>value,set:v=>{if(v===value)return;value=v;subs.forEach(f=>f())},subscribe:f=>{subs.add(f);return ()=>{subs.delete(f)}}}}
const useCursorValue=(store:CursorStore)=>useSyncExternalStore(store.subscribe,store.get,store.get);
const useIsoLayoutEffect=typeof document!=='undefined'?useLayoutEffect:useEffect;
const CursorMarks=memo(function CursorMarks({store,chart}:{store:CursorStore;chart:any}){
 const cursor=useCursorValue(store),{x,y,all,top,H,bottom}=chart;if(cursor==null||!all[cursor])return null;
 return <><Line x1={x(cursor)} x2={x(cursor)} y1={top} y2={H-bottom} stroke="#A3BAC5" strokeDasharray="2 3"/><Circle cx={x(cursor)} cy={y(all[cursor].close)} r={3} fill="white"/></>;
});
const CursorReadout=memo(function CursorReadout({store,chart,compact,hideText,window,setCursor}:{store:CursorStore;chart:any;compact:boolean;hideText:boolean;window:ChartWindow;setCursor:(v:number|null)=>void}){
 const cursor=useCursorValue(store),{all,start,last}=chart,selected=(cursor==null?last:all[cursor])||last;
 return <View style={{gap:5}}>{!hideText&&<T accessibilityLiveRegion="polite" style={{fontSize:12,color:C.ink}}>{dateText(selected.time)} · {selected.time.slice(11,16)} IST · Close {money(selected.close)}</T>}{!compact&&<>{!hideText&&<T style={{fontSize:11,color:C.muted}}>Open {money(selected.open)} · High {money(selected.high)} · Low {money(selected.low)}</T>}<View style={[s.row,{flexWrap:'wrap',gap:4}]}><Button label="Previous candle" kind="ghost" icon="chevron-left" disabled={(cursor??all.length-1)<=start} onPress={()=>setCursor(Math.max(start,(cursor??all.length-1)-1))}/><Button label="Next candle" kind="ghost" icon="chevron-right" disabled={cursor==null||cursor>=all.length-1} onPress={()=>setCursor(Math.min(all.length-1,(cursor??all.length-1)+1))}/></View><T style={{fontSize:11,color:C.muted}}>{window==='recent'?'Recent candles may crop the pattern. Choose Fit pattern to see its full structure.':'Tap a candle, or use Previous / Next to inspect its exact date and price.'}</T></>}</View>;
});
// Geometry the canvas computed, for the legend. `found` is the raw /api/chart match (server lines); null while loading or when nothing is drawn.
export type CanvasGeometry={symbol:string;timeframe:string;pattern:string;dataEnd:string|null;start:number;end:number;bars:any[];found:any|null;geometry:ReturnType<typeof patternGeometry>};
/** One explanatory note about the drawing (the chart's key): which candles the amber box marks, what the amber/red levels mean, why geometry is missing. `cap` = the workspace's old two-line clamp. */
export type ChartNote={text:string;tone:'muted'|'amber';fontSize:number;lineHeight:number;cap?:boolean};
export type PatternCanvasProps={match:Match;side?:string;onSnapshot?:(data:any)=>void;exitPlan?:any;compact?:boolean;chartHeight?:number;
 /** Controlled cursor (bar index into /api/chart bars). Omit for uncontrolled. */ cursor?:number|null;
 /** Fires on hover (web, animation-frame throttled), tap, Previous/Next and resets; only when the index changes. */ onCursor?:(index:number|null)=>void;
 onGeometry?:(g:CanvasGeometry|null)=>void;
 /** Hides info the legend duplicates (timeframe/NSE tag, "Chart view" count, OHLC readout text). Controls stay. */ hideHeader?:boolean;
 /** Legend eye toggle: row keys (chart match id or "SYMBOL:TF:pattern") whose outline is hidden. Only the selected pattern is drawn, so only its key has an effect. Hidden ink stays mounted at opacity 0, so the reveal animation is untouched. */ hiddenPatterns?:string[];
 /** Sizing only: the canvas box takes all remaining height of the parent (which must give it a bounded height); the SVG height follows the measured box, never below `chartHeight`. Controls stay in one slim row. */ fill?:boolean;
 /** Pixels below the SVG's own top that something floats over (the workspace's collapsed legend overlay). The PRICE
  * SCALE gains that much headroom, so the highest drawn mark sits clear of it. Pixels only: `plotH` and every layout
  * box are untouched — the series sits lower in the same plot, it does not get a smaller one. Omitted (every card
  * with nothing floating over it) leaves the scale exactly as it was. */ topInset?:number;
 /** Receives the drawing status line ("Agent tracing the pattern…" … "Structure drawn. Scenarios illustrated.") so the parent can place it (e.g. the legend header). When set, the canvas does not render its own status row. */ onStatus?:(text:string,done:boolean)=>void;
 /** Receives the drawing notes so the parent can place them OUTSIDE the chart column (the workspace puts them in the
  * legend overlay). When set, the canvas does not render its own note rows — which is what keeps a long or extra note
  * from shrinking the plot below `chartHeight` in `fill` mode, where the notes and the canvas share one bounded column. */ onNotes?:(notes:ChartNote[])=>void;
 /** false hides the compact control row (Replay drawing / scenarios / inner swings) for dense cards. Drawing and animation are unchanged. Default true. */ controls?:boolean;
 /** One live research detection's `detection_id`. `/api/chart` can hold several episodes of the same pattern id
  * (different variants, sides and formations) and only `detection_id` is unique, so when this is set the canvas
  * draws THAT episode's lines. Omitted, the match is found by pattern id exactly as it always was. */
 detectionId?:string|null;
 /** A frozen historical snapshot with already-rebased stored lines; no live request or detector run. */
 snapshot?:{symbol:string;timeframe:string;bars:any[];matches:any[];last_candle?:string|null};};
export const PatternCanvas=memo(function PatternCanvas({match,side,onSnapshot,exitPlan,compact=false,chartHeight,cursor:cursorProp,onCursor,onGeometry,hideHeader=false,hiddenPatterns,fill=false,onStatus,onNotes,controls=true,detectionId,snapshot,topInset}:PatternCanvasProps){
 // Which served match this canvas draws. A detection id wins because it is the only identity that separates
 // two episodes of the same pattern on the same stock; otherwise the original pattern-id match is used.
 const pick=React.useCallback((list:any[]|undefined)=>{
  const all=list||[];
  if(detectionId)return all.find((m:any)=>m.detection_id===detectionId)||null;
  // `id` identifies the exact cell (symbol:timeframe:pattern[:variant:side]); the chart cell's matches carry
  // the same ids the match list does, so this is exact for the stored scan and unambiguous for a researched
  // pattern id that has several variants or sides on one chart. The pattern fallback is unchanged.
  const exact=all.find((m:any)=>m.id&&m.id===match.id);
  return exact||(isResearchPattern(match.pattern)?null:all.find((m:any)=>m.pattern===match.pattern))||null;
 },[detectionId,match.id,match.pattern]);
 const [window,setWindow]=useState<ChartWindow>('pattern');
 useEffect(()=>setWindow('pattern'),[match.id,match.candle_end,detectionId]);
 const [data,setData]=useState<any>(null),[error,setError]=useState(''),[retry,setRetry]=useState(0),[width,setWidth]=useState(600),[scenario,setScenario]=useState(true),[replay,setReplay]=useState(0),[phase,setPhase]=useState('structure'),[reduce,setReduce]=useState(false),[detail,setDetail]=useState(true);
 const store=useMemo(cursorStore,[]),controlled=cursorProp!==undefined,live=useRef({controlled,onCursor,onGeometry}),lastCursor=useRef<number|null>(null);live.current={controlled,onCursor,onGeometry};
 useIsoLayoutEffect(()=>{if(controlled){const v=cursorProp??null;lastCursor.current=v;store.set(v)}},[controlled,cursorProp,store]);
 const setCursor=useCallback((next:number|null)=>{const v=next!=null&&Number.isFinite(next)?next:null,prev=live.current.controlled?lastCursor.current:store.get();if(v===prev)return;lastCursor.current=v;if(!live.current.controlled)store.set(v);live.current.onCursor?.(v)},[store]);
 const uid=useId().replace(/\W/g,'');
 useEffect(()=>{AccessibilityInfo.isReduceMotionEnabled().then(setReduce);const subscription=AccessibilityInfo.addEventListener('reduceMotionChanged',setReduce);return()=>subscription.remove()},[]);
 useEffect(()=>{let active=true;setData(null);setError('');setCursor(null);if(snapshot){setData(snapshot);onSnapshot?.(snapshot);return()=>{active=false}}api(`/api/chart?symbol=${encodeURIComponent(match.symbol)}&timeframe=${match.timeframe}`).then(d=>{if(active){setData(d);onSnapshot?.(d)}}).catch(e=>{if(active)setError(e.message)});return()=>{active=false}},[match.id,match.candle_end,detectionId,retry,snapshot]);
 // Whole pixels, ignoring sub-2px jitter. A width change rescales the drawing in place and never restarts the reveal.
 // fill: the box height is measured the same way (whole pixels, sub-2px jitter ignored), so a pane/dock resize rescales the SVG in place without restarting the reveal.
 const [boxH,setBoxH]=useState(0),fillRef=useRef(fill);fillRef.current=fill;
 const onLayout=useCallback((e:any)=>{const next=Math.round(e.nativeEvent.layout.width-2);setWidth(w=>Math.abs(w-next)<2?w:next);if(fillRef.current){const nh=Math.round(e.nativeEvent.layout.height-2);setBoxH(v=>Math.abs(v-nh)<2?v:nh)}},[]);
 const fillH=fill?boxH:0;
 const picked=pick(data?.matches),foundId=picked?.detection_id||picked?.id,outlineHidden=!!hiddenPatterns?.length&&hiddenPatterns.some(k=>(!!foundId&&k===foundId)||(!picked?.detection_id&&(k===match.id||k===`${match.symbol}:${match.timeframe}:${match.pattern}`)));
 const chart=useMemo(()=>{
  const all=data?.bars||[];if(!all.length)return null;
  const found=pick(data.matches);
  const ranges=candleRanges(found);
  const start=window==='pattern'&&ranges.length?Math.max(0,Math.min(all.length-24,Math.min(...ranges.map((r:any)=>r.first))-8)):chartStart(all.length,window,found?.start_index??match.start_index,(found?.lines||[]).flatMap((l:any)=>(l.points||[]).map((p:any)=>p.index)));
  const bars=all.slice(start),last=all[all.length-1];
  const W=Math.max(280,width),H=fill?Math.max(chartHeight||180,fillH):chartHeight|| (W<450?320:365),L=7,R=62,top=27,bottom=57,plotH=H-top-bottom;
  const geometry=patternGeometry(found,all);const showScenario=scenario&&!!found&&!!exitPlan?.usable&&(!found.detection_id||exitPlan?.detection_id===found.detection_id);
  const extra=detail&&geometry.apex?geometry.apex.index-all.length+3:0;
  const shapeWidth=extra>0?(W-R-L)*extra/(bars.length+extra):0;
  const futureWidth=Math.max(showScenario?Math.max(75,(W-R-L)*.27):5,shapeWidth);
  const histW=W-R-L-futureWidth,step=histW/bars.length,x=(index:number)=>L+(index-start+.5)*step;
  const short=(side||((match.direction==='bearish')?'short':'long'))==='short';const sign=short?-1:1;
  const entry=last.close,stop=exitPlan?.stop??entry,target=exitPlan?.target??entry;
  const values=bars.flatMap((b:any)=>[b.low,b.high]);values.push(...visibleDrawingValues(found,start,all.length-1));if(showScenario)values.push(stop,target);if(detail&&geometry.apex)values.push(geometry.apex.value);
  const lo=Math.min(...values),hi=Math.max(...values),span=hi-lo,padding=span*.06||1;
  // Top headroom. Something floats over the plot's top-left (the workspace's collapsed legend overlay), so the
  // highest drawn mark is pushed clear of it by widening the TOP padding alone. This is a price-scale change, not
  // a layout one: plotH, top, bottom and every box are untouched, so the plot keeps every pixel it had and the
  // series simply sits lower inside it. The bottom padding is unchanged and stays positive, so `lo` can never
  // reach the plot floor; the volume strip is drawn from H-bottom, outside the price band, and does not move.
  // The .45 cap is a guard against a freak measurement, not a size — if it ever binds the chart is still readable.
  const headPx=Math.max(0,Math.min((topInset||0)-top,plotH*.45)),f=plotH>0?headPx/plotH:0;
  const padTop=f>0?Math.max(padding,f*(span+padding)/(1-f)):padding;
  const y=(value:number)=>top+(hi+padTop-value)/(span+padTop+padding)*plotH;
  const lines=drawingLines(found);
  const swings=detail&&geometry.swings.length>1,split=swings?.42:.64;
  const volume=Math.max(...bars.map((b:any)=>b.volume||0),1),futureX=L+histW+7,futureEnd=W-R-5;
  const axisIndexes=[start,Math.floor((start+all.length-1)/2),all.length-1];
  const trend=[{x:x(all.length-1),y:y(entry)},{x:futureX+futureWidth*.22,y:y(entry+(target-entry)*.22)},{x:futureX+futureWidth*.4,y:y(entry+(target-entry)*.12)},{x:futureEnd,y:y(target)}];
  const failure=[{x:x(all.length-1),y:y(entry)},{x:futureX+futureWidth*.25,y:y(entry+(stop-entry)*.28)},{x:futureX+futureWidth*.45,y:y(entry+(stop-entry)*.16)},{x:futureEnd,y:y(stop)}];
  return {all,found,start,bars,last,W,H,L,R,top,bottom,plotH,geometry,showScenario,histW,step,x,y,short,sign,entry,stop,target,lo,hi,lines,swings,split,volume,futureX,futureEnd,axisIndexes,trend,failure};
 },[data,window,width,chartHeight,fill,fillH,topInset,match.pattern,match.start_index,match.direction,side,exitPlan,scenario,detail,pick]);
 const chartRef=useRef(chart);chartRef.current=chart;
 // Reported from the snapshot's own symbol/timeframe; null while a new chart loads so the legend never keeps another key's geometry. Resizes don't re-fire.
 useEffect(()=>{const c=chartRef.current;live.current.onGeometry?.(data&&c?{symbol:data.symbol||match.symbol,timeframe:data.timeframe||match.timeframe,pattern:match.pattern,dataEnd:data.last_candle??null,start:c.start,end:c.all.length-1,bars:c.all,found:c.found||null,geometry:c.geometry}:null)},[data,chart?.start,chart?.found,match.pattern]);
 // Pixel → bar index in viewBox units (the SVG scales to the box when the box is narrower than the 280px minimum).
 const indexAt=useCallback((location:number,boxWidth:number)=>{const c=chartRef.current;if(!c||!Number.isFinite(location))return null;const px=location*(boxWidth>0?c.W/boxWidth:1),index=c.start+Math.floor((px-c.L)/c.step);return Number.isFinite(index)?Math.min(c.all.length-1,Math.max(c.start,index)):null},[]);
 // Web mouse/pen hover: at most one cursor update per animation frame, and none unless the bar index changes. Touch keeps tap-to-inspect.
 const frame=useRef(0),hoverAt=useRef<{target:any;clientX:number}|null>(null);
 useEffect(()=>()=>{if(frame.current)cancelAnimationFrame(frame.current)},[]);
 const hover=useMemo(()=>Platform.OS!=='web'?{}:{
  onPointerMove:(e:any)=>{const n=e?.nativeEvent||e;if(n?.pointerType==='touch')return;hoverAt.current={target:e.currentTarget,clientX:n.clientX};if(!frame.current)frame.current=requestAnimationFrame(()=>{frame.current=0;const h=hoverAt.current,b=h?.target?.getBoundingClientRect?.();if(!h||!b||!Number.isFinite(h.clientX))return;const index=indexAt(h.clientX-b.left,b.width);if(index!=null)setCursor(index)})},
  onPointerLeave:(e:any)=>{const n=e?.nativeEvent||e;if(n?.pointerType==='touch')return;if(frame.current){cancelAnimationFrame(frame.current);frame.current=0}hoverAt.current=null;setCursor(null)}},[indexAt,setCursor]);
 // Animated values per reveal stage. They are rebuilt only when what gets drawn changes, not on resize.
 const ink=useMemo(()=>{
  if(!chart)return null;const n=Math.max(1,chart.lines.length),S=chart.split;
  const levels=inkSegment(.63,.82),zones=chart.showScenario?inkSegment(.81,1):null;
  return {lines:chart.lines.map((_:any,i:number)=>inkSegment(i/n*S,(i+1)/n*S+.002)),swings:chart.swings?inkSegment(.42,.642):null,levels,zones,paths:chart.showScenario?inkSegment(.82,1):null,
   reveal:levels.value.interpolate({inputRange:[.63,.82],outputRange:[0,1],extrapolate:'clamp'}),zoneReveal:zones?.value.interpolate({inputRange:[.81,1],outputRange:[0,1],extrapolate:'clamp'})};
 },[data,chart?.lines.length,chart?.split,chart?.swings,chart?.showScenario]);
 useEffect(()=>{
  if(!ink)return;
  const segments=[...ink.lines,ink.swings,ink.levels,ink.zones,ink.paths].filter(Boolean) as Segment[];
  const timers:ReturnType<typeof setTimeout>[]=[],web=Platform.OS==='web'&&typeof document!=='undefined';
  let animation:Animated.CompositeAnimation|null=null;
  const release=()=>{timers.forEach(clearTimeout);timers.length=0;if(web){document.removeEventListener('pointerdown',finish,true);document.removeEventListener('keydown',finish,true)}};
  function finish(){release();animation?.stop();segments.forEach(g=>g.value.setValue(g.to));setPhase('complete')}
  if(reduce){finish();return}
  segments.forEach(g=>g.value.setValue(g.from));setPhase('structure');
  animation=Animated.parallel(segments.map(g=>Animated.timing(g.value,{toValue:g.to,duration:Math.max(1,(g.t1-g.t0)*DRAW_MS),delay:g.t0*DRAW_MS,easing:g.easing,useNativeDriver:false})));
  animation.start(({finished})=>{if(finished){release();setPhase('complete')}});
  // Phase labels change on three timers at the same instants the old per-frame listener crossed them, so only three state updates happen.
  timers.push(setTimeout(()=>setPhase('levels'),easedTime(.64)*DRAW_MS),setTimeout(()=>setPhase('scenarios'),easedTime(.82)*DRAW_MS),setTimeout(()=>setPhase('complete'),easedTime(.999)*DRAW_MS));
  // Any tap or key press on the page jumps the drawing to its finished state, so the rest of the UI never waits on it.
  if(web){document.addEventListener('pointerdown',finish,true);document.addEventListener('keydown',finish,true)}
  return ()=>{release();animation?.stop()};
 },[ink,replay,reduce,scenario,detail,side]);
 // Static layers are memoised on the chart model, so phase labels, cursor moves and animation frames never re-render candles, axes or volume.
 const backLayer=useMemo(()=>{
  if(!chart)return null;const {W,H,L,R,top,bottom,plotH,lo,hi,x,y,all,start,last,histW,axisIndexes}=chart;
  return <><Defs><ClipPath id={uid+'plot'}><Rect x={0} y={top-14} width={W-R} height={plotH+28}/></ClipPath><LinearGradient id={uid+'profitWash'} x1="0" x2="1" y1="0" y2="0"><Stop offset="0" stopColor={C.green} stopOpacity={.01}/><Stop offset="1" stopColor={C.green} stopOpacity={.17}/></LinearGradient><LinearGradient id={uid+'riskWash'} x1="0" x2="1" y1="0" y2="0"><Stop offset="0" stopColor={C.red} stopOpacity={.01}/><Stop offset="1" stopColor={C.red} stopOpacity={.15}/></LinearGradient></Defs>
   {[0,1,2,3,4].map(i=>{const price=lo+(hi-lo)*i/4;return <G key={i}><Line x1={L} x2={W-R} y1={y(price)} y2={y(price)} stroke="#183039" strokeWidth={.6} strokeDasharray="2 5"/><SvgText x={W-R+8} y={y(price)+4} fill="#90A5B1" fontSize={10}>{price.toLocaleString('en-IN',{maximumFractionDigits:price<100?1:0})}</SvgText></G>})}
   {axisIndexes.map((index,i)=><G key={index}><Line x1={x(index)} x2={x(index)} y1={top} y2={H-bottom+24} stroke="#12232C" strokeWidth={.7}/><SvgText x={i===0?L+1:i===2?L+histW:x(index)} y={H-15} textAnchor={i===0?'start':i===2?'end':'middle'} fill="#ABC0CA" fontSize={11}>{new Date(all[index].time.slice(0,10)+'T12:00:00').toLocaleDateString('en-GB',{day:'numeric',month:'short',...((match.timeframe==='1W'||all[start].time.slice(0,4)!==last.time.slice(0,4))?{year:'2-digit' as const}:{})})}</SvgText></G>)}</>;
 },[chart,uid,match.timeframe]);
 const candleLayer=useMemo(()=>{
  if(!chart)return null;const {x,y,bars,start,step,geometry,found}=chart;const ranges=outlineHidden?[]:candleRanges(found);
  return <>{detail&&!outlineHidden&&geometry.envelope.length>0&&<Path d={linePath(geometry.envelope.map(p=>({x:x(p.index),y:y(p.value)})))+' Z'} fill={C.green} opacity={.055}/>}
   {ranges.map((r:any,i:number)=><Rect key={'range'+i} x={x(r.first)-step*.48} y={y(r.high)-5} width={Math.max(step*.96,(r.last-r.first+.96)*step)} height={Math.max(12,y(r.low)-y(r.high)+10)} fill={C.amber} fillOpacity={.075} stroke={C.amber} strokeOpacity={.7} strokeWidth={1} strokeDasharray="3 3"/>)}
   {bars.map((b:any,i:number)=>{const xx=x(start+i),up=b.close>=b.open,col=up?'#26D69C':'#E76A78',marked=ranges.some((r:any)=>start+i>=r.first&&start+i<=r.last);return <G key={i}><Line x1={xx} x2={xx} y1={y(b.high)} y2={y(b.low)} stroke={col} strokeWidth={marked?1.6:.8}/><Rect x={xx-step*(marked?.34:.28)} y={Math.min(y(b.open),y(b.close))} width={Math.max(.75,step*(marked?.68:.56))} height={Math.max(1,Math.abs(y(b.close)-y(b.open)))} fill={col} stroke={marked?'#E9F1F5':'none'} strokeWidth={marked?.6:0}/></G>})}</>;
 },[chart,detail,outlineHidden]);
 const inkLayer=useMemo(()=>{
  if(!chart||!ink)return null;const {x,y,W,L,R,lines,geometry,found,showScenario,futureX,futureEnd,entry,stop,target,short,sign,trend,failure,split}=chart;
  return <>
   <G opacity={outlineHidden?0:1}>{lines.map((line:any,i:number)=><TracedLine key={i} points={line.points.map((p:any)=>({x:x(p.index),y:y(p.value)}))} progress={ink.lines[i].value} start={i/Math.max(1,lines.length)*split} end={(i+1)/Math.max(1,lines.length)*split} color={isResearchPattern(found?.pattern)?drawingColor(line):line.role==='curve'?'#B4E6FF':line.role==='boundary'?'#68F2C2':'#DFE8AA'}/>)}
   {ink.swings&&<TracedLine points={geometry.swings.map(p=>({x:x(p.index),y:y(p.value)}))} progress={ink.swings.value} start={.42} end={.64} color="#DEE3B0"/>}
   {detail&&<InkGroup opacity={ink.reveal}>{geometry.projection.map((points,i)=><Path key={i} d={linePath(points.map(p=>({x:x(p.index),y:y(p.value)})))} fill="none" stroke="#68F2C2" strokeOpacity={.55} strokeWidth={1} strokeDasharray="3 4"/>)}{geometry.apex&&<Circle cx={x(geometry.apex.index)} cy={y(geometry.apex.value)} r={3} fill={C.bg} stroke="#68F2C2"/>}{(isResearchPattern(found?.pattern)&&!isOriginalChart(found?.pattern)?placedLabels(geometry.shapeLabels.filter((l:any)=>!/^HA/.test(found.pattern)||/^[OXABCD]$/.test(l.label)),x,y,L,W-R,chart.top,chart.H-chart.bottom):geometry.shapeLabels.map((l:any)=>({label:l.label,x:x(l.point.index),y:y(l.point.value)-9}))).filter((l:any)=>!l.hidden).map((l:any,i:number)=><G key={l.label+i}>{l.moved&&<Line x1={l.x} y1={l.y+3} x2={l.anchorX} y2={l.anchorY} stroke="#8DA6AB" strokeOpacity={.65} strokeWidth={.6}/>}<SvgText x={l.x} y={l.y} textAnchor="middle" fill="#CAD1C8" fontSize={9}>{l.label.replace('Left shoulder','L shoulder').replace('Right shoulder','R shoulder')}</SvgText></G>)}</InkGroup>}</G>
   <InkGroup opacity={ink.reveal}>{!outlineHidden&&(found?.lines||[]).filter((l:any)=>l.role==='anchors').flatMap((l:any)=>l.points.map((p:any,i:number)=><Circle key={l.label+i} cx={x(p.index)} cy={y(p.value)} r={3.5} fill="#3EE3A026" stroke="#71D7B8" strokeWidth={.8}/>))}<Line x1={L} x2={W-R} y1={y(entry)} y2={y(entry)} stroke="#8FC3C5" strokeDasharray="3 4" strokeWidth={.8}/></InkGroup>
   {showScenario&&ink.paths&&<><InkGroup opacity={ink.zoneReveal}><Rect x={futureX} y={Math.min(y(entry),y(target))} width={Math.max(10,futureEnd-futureX)} height={Math.abs(y(target)-y(entry))} fill={`url(#${uid}profitWash)`}/><Rect x={futureX} y={Math.min(y(entry),y(stop))} width={Math.max(10,futureEnd-futureX)} height={Math.abs(y(stop)-y(entry))} fill={`url(#${uid}riskWash)`}/><Line x1={futureX} x2={futureEnd} y1={y(target)} y2={y(target)} stroke={C.green} strokeDasharray="3 3" strokeWidth={.8}/><Line x1={futureX} x2={futureEnd} y1={y(stop)} y2={y(stop)} stroke={C.red} strokeDasharray="3 3" strokeWidth={.8}/><SvgText x={futureX+3} y={y(target)+(short?14:-7)} fill={C.green} fontSize={10}>{exitPlan?.target_pct.toFixed(1)}% target</SvgText><SvgText x={futureX+3} y={y(stop)+(short?-7:14)} fill={C.red} fontSize={10}>{exitPlan?.stop_pct.toFixed(1)}% stop</SvgText></InkGroup><TracedLine points={trend} progress={ink.paths.value} start={.82} end={1} color={C.green} pen={false}/><TracedLine points={failure} progress={ink.paths.value} start={.82} end={1} color={C.red} pen={false}/><InkGroup opacity={ink.zoneReveal}><Path d={`M${futureEnd-5},${y(target)+sign*8}L${futureEnd},${y(target)}L${futureEnd-7},${y(target)+sign*2}`} stroke={C.green} strokeWidth={1.4} fill="none"/></InkGroup></>}
  </>;
 },[chart,ink,detail,uid,exitPlan,outlineHidden]);
 const frontLayer=useMemo(()=>{
  if(!chart)return null;const {x,H,bottom,top,bars,start,step,volume,showScenario,futureX}=chart;
  return <>{bars.map((b:any,i:number)=><Rect key={i} x={x(start+i)-step*.3} y={H-bottom+23-(b.volume/volume)*22} width={Math.max(.8,step*.6)} height={(b.volume/volume)*22} fill={b.close>=b.open?'#176A55':'#6D3440'} opacity={.9}/>)}
   {showScenario&&<><Line x1={futureX-4} x2={futureX-4} y1={top-7} y2={H-bottom+24} stroke="#35515C" strokeWidth={.8} strokeDasharray="2 3"/><SvgText x={futureX+2} y={16} fill="#9FB5BE" fontSize={9}>{exitPlan?.evidence_applies?'TESTED RULE':'BENCHMARK'}</SvgText><SvgText x={futureX+1} y={H-15} fill="#839CA8" fontSize={9}>Scenario only</SvgText></>}</>;
 },[chart,exitPlan]);
 const svg=useMemo(()=>{
  if(!chart)return null;const {W,H}=chart;
  return <Svg pointerEvents="none" width="100%" height={H} viewBox={`0 0 ${W} ${H}`}>{backLayer}<G clipPath={`url(#${uid}plot)`}>{candleLayer}{inkLayer}<CursorMarks store={store} chart={chart}/></G>{frontLayer}</Svg>;
 },[chart,backLayer,candleLayer,inkLayer,frontLayer,store,uid]);
 // Same status text as the canvas's own status row; reported to the parent only when it changes (a handful of times per reveal, never per frame).
 const statusText=!chart?'':!chart.found?'Stored candles':!hasDrawing(chart.found)?'Pattern geometry unavailable':phase==='structure'?'Agent tracing the pattern…':phase==='levels'?'Marking the decision levels…':phase==='scenarios'&&chart.showScenario?'Illustrating the conditional paths…':chart.showScenario?'Structure drawn. Scenarios illustrated.':['CH19','CH20','CH21'].includes(chart.found.pattern)?'Structure guides drawn.':'Pattern outline shown.';
 const statusDone=!!chart&&phase==='complete';
 useEffect(()=>{onStatus?.(statusText,statusDone)},[statusText,statusDone,onStatus]);
 // The drawing's key. Built once here so it can either be rendered in place (standalone cards) or handed to a parent
 // that places it outside the chart column (`onNotes`). How many notes exist, and how long each one is, depends on the
 // occurrence's stored geometry — so in `fill` mode they must never sit in the same bounded column as the canvas.
 const notes=useMemo<ChartNote[]>(()=>{
  if(!chart)return [];
  const {found,geometry}=chart,out:ChartNote[]=[];
  if(found&&!hasDrawing(found))out.push({text:found.geometry_note||'This occurrence has no usable stored geometry. Candles are shown without a pattern outline.',tone:'amber',fontSize:11,lineHeight:16});
  if(found&&isResearchPattern(found.pattern)&&hasDrawing(found)&&!outlineHidden)out.push({text:`${candleRanges(found).length?`Amber box: ${[...new Set(candleRanges(found).map((r:any)=>r.label))].join(' / ')} · `:''}Structure from stored detection points · amber: trigger · red: failure level`,tone:'muted',fontSize:10,lineHeight:14,cap:true});
  if(geometry.apex&&!outlineHidden&&detail)out.push({text:'Dashed extensions show the mathematical line intersection, not a price forecast.',tone:'muted',fontSize:10,lineHeight:14});
  if(found&&!outlineHidden&&(found.lines||[]).some((line:any)=>line.label==='Projection target'))out.push({text:'Blue: measured-move target (a projection, not an observed outcome).',tone:'muted',fontSize:10,lineHeight:14});
  if(found&&['CH19','CH20','CH21'].includes(found.pattern))out.push({text:'Straight guides connect stored rim and extreme points; no fitted curve was saved for this occurrence.',tone:'amber',fontSize:10,lineHeight:14});
  if(found&&/^HA/.test(found.pattern)&&!outlineHidden&&detail)out.push({text:`Blue levels: projected potential reversal zone (PRZ). Measured ratios: ${geometry.shapeLabels.filter((l:any)=>!/^[OXABCD]$/.test(l.label)&&!(found.pattern==='HA10'&&/^AD\/XA /.test(l.label))).map((l:any)=>l.label.replace(/_/g,' ')).join(' · ')}`,tone:'muted',fontSize:10,lineHeight:15});
  return out;
 },[chart,outlineHidden,detail]);
 useEffect(()=>{onNotes?.(notes)},[notes,onNotes]);
 if(error)return <View style={{gap:10}}><T style={{color:C.red}}>{error}</T><Button label="Retry chart" onPress={()=>setRetry(retry+1)}/></View>;
 if(!data)return <Loading/>;
 if(!chart)return <T>No completed candles available.</T>;
 const {all,found,start,bars,last,showScenario,short,stop,target}=chart;
 // Text alternative built only from values already on screen: the stored candles, the stored close and the exit plan's own levels.
 const summary=`${match.symbol} ${match.pattern_name||match.pattern} chart, ${match.timeframe} candles, NSE, INR. ${found?(outlineHidden?'Pattern outline hidden from the legend. ':''):'Pattern outline not available; stored candles only. '}Showing ${bars.length} candles from ${dateText(all[start].time)} to ${dateText(last.time)}. Last close ${money(last.close,2)}.${showScenario?` Illustrated ${short?'short':'long'} scenario from the stored close: target ${money(target,2)} (${exitPlan.target_pct.toFixed(2)}%), stop ${money(stop,2)} (${exitPlan.stop_pct.toFixed(2)}%), before costs. Scenario paths are not price forecasts.`:''}`;
 const slim={minHeight:34,paddingVertical:5,paddingHorizontal:10,borderRadius:9};
 return <View style={fill?{flex:1,minHeight:0,gap:4}:{gap:10}}>
  {!compact&&<View style={[s.between,{flexWrap:'wrap',gap:5}]}>{hideHeader?<View/>:<T style={{fontSize:11,color:C.muted}}>Chart view · {bars.length} candles</T>}<View role="group" accessibilityLabel="Chart window" style={[s.row,{gap:3}]}>{([['pattern','Fit pattern'],['recent','Recent 40'],['all','All history']] as const).map(([value,label])=><Pressable key={value} accessibilityRole="button" accessibilityLabel={label} accessibilityHint="Changes which candles the chart shows" accessibilityState={{selected:window===value}} onPress={()=>{setWindow(value);setCursor(null)}} style={{minHeight:44,paddingHorizontal:10,justifyContent:'center',borderRadius:8,backgroundColor:window===value?C.soft:'transparent'}}><T style={{fontSize:11,color:window===value?C.green:C.muted}}>{label}</T></Pressable>)}</View></View>}
  {!onStatus&&<View style={[s.between,{flexWrap:'wrap',gap:3}]}><View style={s.row}><Icon name={phase==='complete'?'check-circle':'edit-3'} size={14} color={C.green}/><T style={{fontSize:11,color:C.green}}>{statusText}</T></View>{!hideHeader&&<T style={{fontSize:10,color:C.muted}}>{match.timeframe} · NSE / INR</T>}</View>}
  {/* Notes render here only when no parent took them (onNotes). In `fill` mode a parent that leaves them here is
      accepting that they come out of the canvas's own height budget. */}
  {!onNotes&&notes.map((n,i)=><T key={i} numberOfLines={n.cap&&compact?2:undefined} style={{fontSize:n.fontSize,lineHeight:n.lineHeight,color:n.tone==='amber'?C.amber:C.muted}}>{n.text}</T>)}
  <View role="group" accessibilityLabel={summary} style={[{backgroundColor:'#061117',borderWidth:1,borderColor:C.line,borderRadius:11,overflow:'hidden'},fill&&{flex:1,minHeight:0}]} onLayout={onLayout}>
   <Pressable accessibilityRole="button" accessibilityLabel="Inspect candle date and closing price" accessibilityHint="Selects the candle under the pointer. Previous and Next candle buttons do the same from the keyboard." onPress={e=>{const event:any=e.nativeEvent;const bounds=(e.currentTarget as any)?.getBoundingClientRect?.();const location=Number.isFinite(event.locationX)?event.locationX:bounds&&Number.isFinite(event.clientX)?event.clientX-bounds.left:null;if(location==null)return;const index=indexAt(location,bounds?.width||width);if(index!=null)setCursor(index)}} {...hover}>
    {svg}
   </Pressable>
  </View>
  {!(fill&&compact&&hideHeader)&&<CursorReadout store={store} chart={chart} compact={compact} hideText={hideHeader} window={window} setCursor={setCursor}/>}
  {compact&&controls&&(fill?<ScrollView horizontal showsHorizontalScrollIndicator={false} style={{flexGrow:0,flexShrink:0}} contentContainerStyle={{gap:4,alignItems:'center'}}><Button label="Replay drawing" kind="ghost" icon="edit-3" onPress={()=>setReplay(replay+1)} style={slim}/><Button label={scenario?'Hide scenarios':'Show scenarios'} kind="ghost" onPress={()=>setScenario(!scenario)} disabled={!found||!exitPlan?.usable} style={slim}/><Button label={detail?'Hide inner swings':'Show full pattern'} kind="ghost" onPress={()=>setDetail(!detail)} style={slim}/></ScrollView>:<View style={[s.row,{flexWrap:'wrap',gap:4}]}><Button label="Replay drawing" kind="ghost" icon="edit-3" onPress={()=>setReplay(replay+1)}/><Button label={scenario?'Hide scenarios':'Show scenarios'} kind="ghost" onPress={()=>setScenario(!scenario)} disabled={!found||!exitPlan?.usable}/><Button label={detail?'Hide inner swings':'Show full pattern'} kind="ghost" onPress={()=>setDetail(!detail)}/></View>)}
  {!compact&&<>
  <View style={s.row}><Button label="Replay drawing" kind="outline" icon="edit-3" onPress={()=>setReplay(replay+1)} style={{flex:1}}/><Button label={scenario?'Hide scenarios':'Show scenarios'} kind="soft" icon="git-branch" onPress={()=>setScenario(!scenario)} disabled={!found||!exitPlan?.usable} style={{flex:1}}/></View>
  <Button label={detail?'Hide inner swings':'Show full pattern'} kind="ghost" icon="activity" onPress={()=>setDetail(!detail)}/>
  {detail&&<T style={{fontSize:10,color:C.muted}}>Connected swings follow observed pivots. {chart.geometry.apex?'Dotted edges extend the boundaries to a geometric apex; they are not future prices.':'The shape is an outline of observed price movement.'}</T>}
  {showScenario&&<T style={{fontSize:10,lineHeight:17,color:C.muted}}>{exitPlan.label} · {short?'Short':'Long'} from the stored close · {exitPlan.stop_pct.toFixed(2)}% stop / {exitPlan.target_pct.toFixed(2)}% target, before costs. {exitPlan.stop_basis}. Paths illustrate the rule, not future prices or timing.</T>}
  </>}

 </View>;
});
