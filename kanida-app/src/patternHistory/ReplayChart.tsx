import React,{useState} from 'react';
import {View} from 'react-native';
import Svg,{Line,Rect,Polyline,Circle,Text as SvgText,G} from 'react-native-svg';
import {C,T,Button,s} from '../ui';
import {api} from '../model';
import {useKeyedValue} from '../activeSymbol';
import {WidgetError} from '../layout';
import {Skeleton} from '../discover/parts';
import {query,date,signed} from './logic';
import type {Selection,Replay} from './types';

export function ReplayChart({selection,id,horizon}:{selection:Selection;id:string;horizon:number}){
 const url='/api/pattern-history/replay?'+query(selection,{occurrence_id:id});
 const {value:data,error,loading,reload}=useKeyedValue<Replay>(url,()=>api(url));
 const [future,setFuture]=useState(true),[selected,setSelected]=useState<number|null>(null),[width,setWidth]=useState(650);
 if(error)return <WidgetError title="Historical chart unavailable" message={error} onRetry={reload}/>;
 if(loading||!data)return <Skeleton chart lines={2}/>;
 const obs=data.occurrence.horizons.find(h=>h.horizon===horizon);
 const bars=data.bars.filter(b=>b.index<=data.signal_index+(future?horizon:0));
 if(!bars.length)return <T>No historical candles are available for this occurrence.</T>;
 const W=Math.max(width,280),H=330,L=12,R=65,TOP=34,BOTTOM=32;
 const low=Math.min(...bars.map(b=>b.low)),high=Math.max(...bars.map(b=>b.high)),pad=Math.max((high-low)*.12,high*.002),range=high-low+pad*2;
 const step=(W-L-R)/bars.length,x=(i:number)=>L+(i-bars[0].index+.5)*step,y=(v:number)=>TOP+(high+pad-v)/range*(H-TOP-BOTTOM);
 const geo=data.geometry,first=bars[0].index,last=bars[bars.length-1].index;
 const focused=bars.find(b=>b.index===selected)||bars[bars.length-1];
 const troughs=Array.isArray(geo.troughs)?geo.troughs:[],levels=geo.trough_levels||[];
 const points=troughs.length===2&&Number.isFinite(geo.intervening_extreme)&&Number.isFinite(geo.neckline)?[[troughs[0],levels[0]],[geo.intervening_extreme,geo.neckline],[troughs[1],levels[1]]].filter(([i,v])=>Number.isFinite(i)&&Number.isFinite(v)):[];
 const candleStart=data.occurrence.formation_start_index,candleEnd=data.occurrence.detected_index??data.signal_index;
 const upper=obs?.max_up_bar?bars.find(b=>b.index===data.signal_index+obs.max_up_bar!):undefined;
 const lower=obs?.max_down_bar?bars.find(b=>b.index===data.signal_index+obs.max_down_bar!):undefined;
 return <View style={{gap:12}}>
  <View style={[s.between,{flexWrap:'wrap'}]}><View><T style={{fontFamily:'InterSemi'}}>Detected {date(data.occurrence.signal_time)}</T><T style={{color:C.muted,fontSize:12}}>Original candles and frozen pattern geometry</T></View><Button kind="outline" label={future?'Hide outcome candles':'Show outcome candles'} onPress={()=>setFuture(v=>!v)}/></View>
  <View onLayout={e=>setWidth(Math.round(e.nativeEvent.layout.width))} style={{borderWidth:1,borderColor:C.line,borderRadius:12,overflow:'hidden',backgroundColor:C.dark}}>
   <Svg width="100%" height={H} viewBox={`0 0 ${W} ${H}`} accessibilityLabel={`${selection.symbol} historical daily candles. Amber marks detection; blue marks the next-open reference. Tap a candle for prices.`} role="img">
    {future&&data.reference_index!=null&&data.reference_index<=last&&<Rect x={x(data.reference_index)-step/2} y={TOP} width={Math.max(0,x(last)-x(data.reference_index)+step)} height={H-TOP-BOTTOM} fill="#102334"/>}
    {[0,.25,.5,.75,1].map(t=>{const v=low-pad+t*range;return <G key={t}><Line x1={L} x2={W-R} y1={y(v)} y2={y(v)} stroke={C.line}/><SvgText x={W-R+8} y={y(v)+4} fill={C.muted} fontSize={10}>{v.toFixed(2)}</SvgText></G>})}
    {selection.pattern_id==='CDLENGULFING'&&<Rect x={x(Math.max(first,candleStart))-step/2} y={TOP} width={Math.max(step,(Math.min(last,candleEnd)-Math.max(first,candleStart)+1)*step)} height={H-TOP-BOTTOM} fill={C.amber} fillOpacity={.1} stroke={C.amber} strokeOpacity={.5}/>}
    {bars.map(b=><G key={b.index} onPress={()=>setSelected(b.index)}><Rect x={x(b.index)-step/2} y={TOP} width={step} height={H-TOP-BOTTOM} fill={focused.index===b.index?C.ink:'transparent'} fillOpacity={.06}/><Line x1={x(b.index)} x2={x(b.index)} y1={y(b.high)} y2={y(b.low)} stroke={b.close>=b.open?C.green:C.red}/><Rect x={x(b.index)-Math.max(1,step*.58)/2} y={y(Math.max(b.open,b.close))} width={Math.max(1,step*.58)} height={Math.max(1,Math.abs(y(b.open)-y(b.close)))} fill={b.close>=b.open?C.green:C.red}/></G>)}
    {points.length===3&&<Polyline points={points.map(([i,v])=>`${x(i)},${y(v)}`).join(' ')} fill="none" stroke={C.amber} strokeWidth={2}/>}
    {Number.isFinite(geo.neckline)&&<Line x1={x(Math.max(first,candleStart))} x2={x(data.signal_index)} y1={y(geo.neckline)} y2={y(geo.neckline)} stroke={C.amber} strokeDasharray="4 4"/>}
    <Line x1={x(data.signal_index)} x2={x(data.signal_index)} y1={TOP-7} y2={H-BOTTOM} stroke={C.amber} strokeDasharray="3 3"/>
    <SvgText x={Math.min(W-R-70,Math.max(L,x(data.signal_index)-30))} y={20} fill={C.amber} fontSize={11}>Detected</SvgText>
    {future&&obs?.reference_price!=null&&data.reference_index!=null&&<><Line x1={x(data.reference_index)} x2={W-R} y1={y(obs.reference_price)} y2={y(obs.reference_price)} stroke="#7FB7FF" strokeDasharray="4 4"/><Circle cx={x(data.reference_index)} cy={y(obs.reference_price)} r={4} fill="#7FB7FF"/></>}
    {future&&upper&&<Circle cx={x(upper.index)} cy={y(upper.high)} r={4} stroke={C.green} strokeWidth={2} fill={C.dark}/>}
    {future&&lower&&<Circle cx={x(lower.index)} cy={y(lower.low)} r={4} stroke={C.red} strokeWidth={2} fill={C.dark}/>}
    <SvgText x={L} y={H-10} fill={C.muted} fontSize={10}>{date(bars[0].time)}</SvgText><SvgText x={W-R} y={H-10} textAnchor="end" fill={C.muted} fontSize={10}>{date(bars[bars.length-1].time)}</SvgText>
   </Svg>
  </View>
  <View style={[s.row,{flexWrap:'wrap'}]}><T style={{fontSize:12}}>{date(focused.time)} · Open {focused.open.toFixed(2)} · High {focused.high.toFixed(2)} · Low {focused.low.toFixed(2)} · Close {focused.close.toFixed(2)}</T><Button kind="ghost" label="Previous candle" disabled={focused.index<=first} onPress={()=>setSelected(focused.index-1)}/><Button kind="ghost" label="Next candle" disabled={focused.index>=last} onPress={()=>setSelected(focused.index+1)}/></View>
  <T style={{color:C.muted,fontSize:12}}>Amber: original pattern and detection. Blue: next candle’s opening price; shaded area: the following {horizon} daily candle{horizon===1?'':'s'}. Green / red rings: highest / lowest price in that window.</T>
  <View style={[s.card,{padding:14,gap:5}]}><T>Reference {obs?.reference_price?.toFixed(2)??'not available'} · {date(obs?.reference_time)}</T><T>After {horizon} candles: {signed(obs?.gross_return_pct)} · Largest rise: {signed(obs?.max_up_pct)} · Largest fall: {signed(obs?.max_down_pct==null?null:-obs.max_down_pct)}</T><T style={{color:C.muted,fontSize:12}}>Window ends {date(obs?.end_time)}. {obs?.status==='pending'?'Still unfolding in this frozen data.':obs?.status==='quality_excluded'?'Excluded because the measurement window has a data-quality issue.':'Gross price movements; no trade or achievable profit is implied.'}</T></View>
 </View>;
}
