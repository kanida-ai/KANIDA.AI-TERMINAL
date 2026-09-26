// The payoff chart: expiry P&L (solid, profit/loss shaded) and the scenario-date P&L (dashed), ±1σ/±2σ bands, spot,
// the scenario spot and the breakevens. Display only - extrema come from the server's exact maths, never from this
// plotted range. A hover readout gives exact values; the payoff table is the keyboard/screen-reader equivalent.
import React,{useMemo,useState,useRef} from 'react';
import {Platform,View} from 'react-native';
import Svg,{Line,Path,Rect,Text as SvgText} from 'react-native-svg';
import {C,T,s} from '../ui';
import {num,signed} from './format';

type Pt={s:number;expiry:number|null;target:number|null};
const PAD={l:62,r:14,t:14,b:28};

export function PayoffChart({curve,spot,scenarioSpot,breakevens,bands,height=300,dim=false,scenarioLabel,expiryLabel='At expiry',empty='Add legs to draw the payoff.'}:{curve:Pt[];spot:number;scenarioSpot?:number;
 breakevens:number[];bands:{k:number;low:number;high:number}[];height?:number;dim?:boolean;scenarioLabel:string;expiryLabel?:string;empty?:string}){
 const [w,setW]=useState(640);const [hover,setHover]=useState<Pt|null>(null);const pinned=useRef(false);
 const g=useMemo(()=>{
  if(!curve.length)return null;
  const xs=curve.map(p=>p.s);const ys=curve.flatMap(p=>[p.expiry,p.target]).filter((v):v is number=>v!=null);
  const x0=xs[0],x1=xs[xs.length-1];let y0=Math.min(0,...ys),y1=Math.max(0,...ys);const padY=(y1-y0)*.08||1;y0-=padY;y1+=padY;
  const X=(v:number)=>PAD.l+(v-x0)/(x1-x0||1)*(w-PAD.l-PAD.r);const Y=(v:number)=>PAD.t+(y1-v)/(y1-y0||1)*(height-PAD.t-PAD.b);
  const line=(key:'expiry'|'target')=>curve.filter(p=>p[key]!=null).map((p,i)=>`${i?'L':'M'}${X(p.s).toFixed(1)},${Y(p[key] as number).toFixed(1)}`).join('');
  const area=(sign:1|-1)=>{const pts=curve.filter(p=>p.expiry!=null);if(!pts.length)return '';
   return `M${X(pts[0].s)},${Y(0)}`+pts.map(p=>`L${X(p.s).toFixed(1)},${Y(sign>0?Math.max(0,p.expiry as number):Math.min(0,p.expiry as number)).toFixed(1)}`).join('')+`L${X(pts[pts.length-1].s)},${Y(0)}Z`;};
  const ticks=[0,.25,.5,.75,1].map(f=>y0+(y1-y0)*f);
  const xt=[0,.2,.4,.6,.8,1].map(f=>x0+(x1-x0)*f);
  return {X,Y,x0,x1,expiry:line('expiry'),target:line('target'),up:area(1),down:area(-1),ticks,xt};
 },[curve,w,height]);
 if(!g)return <View style={{height,alignItems:'center',justifyContent:'center'}}><T style={{color:C.muted,fontSize:12,textAlign:'center',maxWidth:360}}>{empty}</T></View>;
 const pick=(x:number)=>{const v=g.x0+(x-PAD.l)/(w-PAD.l-PAD.r)*(g.x1-g.x0);
  let best=curve[0];for(const p of curve)if(Math.abs(p.s-v)<Math.abs(best.s-v))best=p;setHover(best);};
 const onMove=(e:any)=>{if(Platform.OS!=='web'||pinned.current)return;const r=e.currentTarget.getBoundingClientRect?.();if(!r)return;pick(e.clientX-r.left);};
 // touch: tap or drag pins the readout (no hover on a phone - GTM audit P12); it stays until the next tap
 const touch={onStartShouldSetResponder:(e:any)=>e?.nativeEvent?.touches?.length>0||Platform.OS!=='web',onMoveShouldSetResponder:()=>false,
  onResponderGrant:(e:any)=>{pinned.current=true;pick(e.nativeEvent.locationX);},onResponderMove:(e:any)=>pick(e.nativeEvent.locationX),onResponderTerminationRequest:()=>true};
 return <View onLayout={e=>setW(Math.max(280,e.nativeEvent.layout.width))} style={{opacity:dim?.45:1}}
  {...touch} {...({onMouseMove:onMove,onMouseLeave:()=>{if(!pinned.current)setHover(null);}} as any)}
  accessibilityLabel={`Payoff chart. Solid line: ${expiryLabel}. Dashed line: ${scenarioLabel}. The payoff table below lists the same values.`}>
  <Svg width={w} height={height}>
   {bands.slice().reverse().map(b=><Rect key={b.k} x={g.X(Math.max(g.x0,b.low))} y={PAD.t} width={Math.max(0,g.X(Math.min(g.x1,b.high))-g.X(Math.max(g.x0,b.low)))}
    height={height-PAD.t-PAD.b} fill={b.k===1?'#39E5A30F':'#39E5A308'}/>)}
   {g.ticks.map((t,i)=><React.Fragment key={i}><Line x1={PAD.l} x2={w-PAD.r} y1={g.Y(t)} y2={g.Y(t)} stroke={C.line} strokeWidth={1}/>
    <SvgText x={PAD.l-6} y={g.Y(t)+3} fill={C.muted} fontSize={10} textAnchor="end">{Math.abs(t)>=1000?`${(t/1000).toFixed(1)}k`:t.toFixed(0)}</SvgText></React.Fragment>)}
   {g.xt.map((x,i)=><SvgText key={i} x={g.X(x)} y={height-8} fill={C.muted} fontSize={10} textAnchor="middle">{Math.round(x).toLocaleString('en-IN')}</SvgText>)}
   <Path d={g.up} fill="#39E5A322"/><Path d={g.down} fill="#F17D8722"/>
   <Line x1={PAD.l} x2={w-PAD.r} y1={g.Y(0)} y2={g.Y(0)} stroke={C.muted} strokeWidth={1}/>
   {breakevens.filter(b=>b>=g.x0&&b<=g.x1).map(b=><Line key={b} x1={g.X(b)} x2={g.X(b)} y1={PAD.t} y2={height-PAD.b} stroke={C.amber} strokeWidth={1} strokeDasharray="2,3"/>)}
   <Line x1={g.X(spot)} x2={g.X(spot)} y1={PAD.t} y2={height-PAD.b} stroke={C.ink} strokeWidth={1}/>
   <SvgText x={g.X(spot)+4} y={PAD.t+10} fill={C.ink} fontSize={10}>{`Spot ${num(spot,2)}`}</SvgText>
   {scenarioSpot!=null&&Math.abs(scenarioSpot-spot)>1e-6&&<><Line x1={g.X(scenarioSpot)} x2={g.X(scenarioSpot)} y1={PAD.t} y2={height-PAD.b} stroke={C.mint} strokeWidth={1.5} strokeDasharray="4,3"/>
    <SvgText x={g.X(scenarioSpot)+4} y={PAD.t+24} fill={C.mint} fontSize={10}>{`Scenario ${num(scenarioSpot,0)}`}</SvgText></>}
   {!!g.target&&<Path d={g.target} stroke={C.mint} strokeWidth={1.8} fill="none" strokeDasharray="6,4"/>}
   {!!g.expiry&&<Path d={g.expiry} stroke={C.green} strokeWidth={2} fill="none"/>}
   {hover&&<Line x1={g.X(hover.s)} x2={g.X(hover.s)} y1={PAD.t} y2={height-PAD.b} stroke={C.muted} strokeWidth={1}/>}
  </Svg>
  <View style={[s.row,{gap:16,flexWrap:'wrap',minHeight:20}]}>
   <Legend color={C.green} label={expiryLabel} solid/><Legend color={C.mint} label={scenarioLabel}/><Legend color={C.amber} label="Breakeven"/>
   {!hover&&<T style={{fontSize:10,color:C.muted}}>{Platform.OS==='web'?'Hover (or tap on a touch screen) for exact values':'Tap the chart for exact values'}</T>}
   {hover&&<T style={{fontSize:11,color:C.ink,fontVariant:['tabular-nums'] as any}}>{`${num(hover.s,0)} (${spot?`${hover.s>=spot?'+':''}${((hover.s/spot-1)*100).toFixed(2)}% from spot`:''}) · ${expiryLabel.toLowerCase()} ${signed(hover.expiry)} · scenario ${signed(hover.target)}`}</T>}
  </View>
 </View>;
}
function Legend({color,label,solid}:{color:string;label:string;solid?:boolean}){
 return <View style={[s.row,{gap:6}]}><View style={{width:16,height:0,borderTopWidth:2,borderColor:color,borderStyle:solid?'solid':'dashed'}}/><T style={{fontSize:11,color:C.muted}}>{label}</T></View>;
}
