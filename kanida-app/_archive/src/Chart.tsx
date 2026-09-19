import React,{useState,useEffect} from 'react';
import {View} from 'react-native';
import Svg,{Line,Rect,Path,Text as SvgText,Defs,LinearGradient,Stop} from 'react-native-svg';
import {api,Match,dateText} from './model';
import {C,T,s,Loading,Button} from './ui';
export function CandleChart({match,compact=false,onSnapshot}: {match:Match;compact?:boolean;onSnapshot?:(data:any)=>void}){
 const [data,setData]=useState<any>(null),[error,setError]=useState(''),[zoom,setZoom]=useState(1),[retry,setRetry]=useState(0);
 useEffect(()=>{let active=true;setData(null);setError('');api(`/api/chart?symbol=${encodeURIComponent(match.symbol)}&timeframe=${match.timeframe}`).then(d=>{if(active){setData(d);onSnapshot?.(d)}}).catch(e=>{if(active)setError(e.message)});return()=>{active=false}},[match.symbol,match.timeframe,retry,match.id]);
 if(error)return <View style={{padding:20,gap:12}}><T style={{color:C.red}}>{error}</T><Button label="Retry chart" kind="outline" onPress={()=>setRetry(retry+1)}/></View>;
 if(!data)return <Loading/>;
 const all=data.bars||[];const detected=data.matches?.find((m:any)=>m.pattern===match.pattern);const start=Math.max(0,all.length-Math.round(Math.max(70,Math.min(180,all.length-(detected?.start_index||match.start_index)+15))/zoom));const bars=all.slice(start);
 if(!bars.length)return <T>No chart candles available.</T>;
 const W=720,H=compact?240:320,L=8,R=70,top=15,bottom=50;const plotW=W-L-R,plotH=H-top-bottom;
 const lo=Math.min(...bars.map((b:any)=>b.low)),hi=Math.max(...bars.map((b:any)=>b.high)),pad=(hi-lo)*.12||1;
 const y=(v:number)=>top+(hi+pad-v)/(hi-lo+2*pad)*plotH;const step=plotW/bars.length;const x=(i:number)=>L+(i-start+.5)*step;
 const vol=Math.max(...bars.map((b:any)=>b.volume||0),1);
 let overlays=(detected?.lines||[]).filter((l:any)=>l.points?.length>1);
 overlays=overlays.map((line:any)=>{
  if(line.role!=='curve'||match.pattern!=='cup_handle')return line;
  const first=line.points[0].index,last=line.points[line.points.length-1].index;const segment=all.slice(first,last+1);if(!segment.length)return line;
  let floor=first;for(let i=first;i<=last;i++)if(all[i]?.low<all[floor]?.low)floor=i;
  const left=all[first]?.high,right=all[last]?.high,low=all[floor]?.low;if(![left,right,low].every(Number.isFinite))return line;
  return {...line,points:Array.from({length:41},(_,i)=>{const index=first+(last-first)*i/40;const rim=index<=floor?left:right;const span=index<=floor?floor-first:last-floor;return {index,value:low+(rim-low)*Math.pow((index-floor)/(span||1),2)}})};
 });
 return <View style={{gap:10}}><View style={{backgroundColor:C.dark,borderRadius:17,paddingHorizontal:6,paddingTop:8,overflow:'hidden'}}><Svg width="100%" height={compact?210:280} viewBox={`0 0 ${W} ${H}`}>
  {[0,1,2,3].map(i=>{const value=lo+(hi-lo)*i/3;return <React.Fragment key={i}><Line x1={L} x2={W-R+5} y1={y(value)} y2={y(value)} stroke="#315044" strokeDasharray="3 6" strokeWidth={.6}/><SvgText x={W-R+12} y={y(value)+4} fill="#8DAA98" fontSize={10}>{value.toFixed(value<100?2:0)}</SvgText></React.Fragment>})}
  {bars.map((b:any,i:number)=>{const j=i+start,col=b.close>=b.open?'#92D8AF':'#C47869';return <React.Fragment key={j}><Rect x={x(j)-step*.3} y={H-29-(b.volume/vol)*19} width={Math.max(.7,step*.6)} height={(b.volume/vol)*19} fill={col} opacity={.32}/><Line x1={x(j)} x2={x(j)} y1={y(b.high)} y2={y(b.low)} stroke={col} strokeWidth={1}/><Rect x={x(j)-step*.28} y={Math.min(y(b.open),y(b.close))} width={Math.max(.8,step*.56)} height={Math.max(1,Math.abs(y(b.open)-y(b.close)))} fill={col}/></React.Fragment>})}
  {overlays.map((line:any,i:number)=><Path key={i} d={line.points.filter((p:any)=>p.index>=start&&p.index<all.length).map((p:any,j:number)=>`${j?'L':'M'} ${x(p.index)} ${y(p.value)}`).join(' ')} fill="none" stroke={line.role==='curve'?'#D8F88A':'#F2D295'} strokeWidth={2} strokeDasharray={line.role==='anchors'?'4 4':undefined}/>)}
  <Line x1={L} x2={W-R} y1={y(bars[bars.length-1].close)} y2={y(bars[bars.length-1].close)} stroke="#B8E6A4" strokeDasharray="4 4" opacity={.55}/>
  <SvgText x={L+4} y={H-8} fill="#8DAA98" fontSize={10}>{dateText(bars[0].time)}</SvgText><SvgText x={W-R} y={H-8} textAnchor="end" fill="#8DAA98" fontSize={10}>{dateText(bars[bars.length-1].time)}</SvgText>
 </Svg></View><View style={s.between}><T style={{fontSize:10,color:C.muted,flex:1}}>{detected?'Detected geometry over actual candles':'Actual stored candles'} · {match.timeframe} · IST{!match.current?' · historical':''}</T>{!compact&&<View style={s.row}><Button label="−" kind="outline" disabled={zoom===1} onPress={()=>setZoom(Math.max(1,zoom-1))}/><Button label="+" kind="outline" disabled={zoom===3} onPress={()=>setZoom(Math.min(3,zoom+1))}/></View>}</View></View>;
}
export function CashCurve({curve}:any){
 const values=(curve||[]).map((p:any)=>p.cash??p.capital??p.equity??p.balance).filter(Number.isFinite);if(values.length<2)return null;
 const lo=Math.min(...values),hi=Math.max(...values),span=hi-lo||1;const d=values.map((v:number,i:number)=>`${i?'L':'M'} ${i/(values.length-1)*650+4} ${105-(v-lo)/span*90}`).join(' ');
 return <Svg width="100%" height={130} viewBox="0 0 660 130"><Defs><LinearGradient id="cashFade" x1="0" x2="0" y1="0" y2="1"><Stop offset="0" stopColor={C.green} stopOpacity={.15}/><Stop offset="1" stopColor={C.green} stopOpacity={0}/></LinearGradient></Defs><Path d={d+' L654 125 L4 125 Z'} fill="url(#cashFade)"/><Path d={d} stroke={C.green} strokeWidth={2} fill="none"/></Svg>;
}
