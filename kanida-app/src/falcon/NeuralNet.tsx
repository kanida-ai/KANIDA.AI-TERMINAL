import React,{memo,useEffect,useMemo,useRef,useState} from 'react';
import {View} from 'react-native';
import Svg,{Circle,Line,G,Text as SvgText,Defs,RadialGradient,Stop} from 'react-native-svg';
import {C} from '../ui';
// Scanning animation: one node layer per stage, pulses flow along edges up to the stage being shown, the active layer glows, and the stage's real `references` flicker beside it.
// Static edges render once (memo); only pulses, the active layer and labels re-render per requestAnimationFrame. Not rendered at all under reduced motion (the caller skips it).
const VW=720,VH=300,COUNTS=[4,6,8,9,8,6,4];
type Node={x:number;y:number;layer:number};type Edge={a:Node;b:Node;layer:number};
function rng(seed:number){return ()=>{seed=(seed*1664525+1013904223)%4294967296;return seed/4294967296;};}
function build(){
 const L=COUNTS.length,layers:Node[][]=COUNTS.map((n,l)=>Array.from({length:n},(_,i)=>({layer:l,x:40+l*(VW-80)/(L-1),y:VH/2+(i-(n-1)/2)*Math.min(34,(VH-50)/n)})));
 const edges:Edge[]=[];const r=rng(7);
 for(let l=0;l<L-1;l++)for(const a of layers[l]){const next=[...layers[l+1]].sort((p,q)=>Math.abs(p.y-a.y)-Math.abs(q.y-a.y));const k=2+Math.floor(r()*2);for(const b of next.slice(0,k))edges.push({a,b,layer:l});}
 return {layers,edges};
}
const NET=build();
const Edges=memo(function Edges({active}:{active:number}){return <G>{NET.edges.map((e,i)=><Line key={i} x1={e.a.x} y1={e.a.y} x2={e.b.x} y2={e.b.y} stroke={e.layer<active?C.green:C.line} strokeOpacity={e.layer<active?.35:.9} strokeWidth={1}/>)}</G>;});
const Nodes=memo(function Nodes({active,done}:{active:number;done:boolean}){return <G>{NET.layers.flat().map((n,i)=>{const lit=done||n.layer<active;return <Circle key={i} cx={n.x} cy={n.y} r={lit?5:4} fill={lit?C.green:C.paper} fillOpacity={lit?.85:1} stroke={lit?C.mint:C.line} strokeWidth={1.2}/>;})}</G>;});
export function NeuralNet({active,references,done=false,height=260}:{active:number;references:string[];done?:boolean;height?:number}){
 const [t,setT]=useState(0);const frame=useRef(0),t0=useRef(0);
 useEffect(()=>{const loop=(ts:number)=>{if(!t0.current)t0.current=ts;setT(ts-t0.current);frame.current=requestAnimationFrame(loop);};frame.current=requestAnimationFrame(loop);return()=>cancelAnimationFrame(frame.current);},[]);
 const layer=Math.min(COUNTS.length-1,Math.max(0,active));
 // Pulses only on edges feeding the layer being shown (and the ones before it), seeded per stage so they don't jump every frame.
 const pulses=useMemo(()=>{const pool=NET.edges.filter(e=>e.layer<Math.max(1,layer+(done?1:0)));const r=rng(31+layer);return Array.from({length:Math.min(34,pool.length)},()=>({e:pool[Math.floor(r()*pool.length)],off:r(),dur:900+r()*900}));},[layer,done]);
 const refs=references.slice(0,24),front=NET.layers[layer];
 const labels=refs.length?front.filter((_,i)=>i%2===0).slice(0,3).map((n,i)=>{const cycle=Math.floor(t/650)+i*3;const text=refs[cycle%refs.length];const phase=(t/650)%1;return {n,text,opacity:.35+.65*Math.abs(Math.sin((phase+i*.33)*Math.PI))};}):[];
 return <View style={{width:'100%',height}}>
  <Svg width="100%" height="100%" viewBox={`0 0 ${VW} ${VH}`} preserveAspectRatio="xMidYMid meet">
   <Defs><RadialGradient id="falconGlow" cx="50%" cy="50%" r="50%"><Stop offset="0%" stopColor={C.green} stopOpacity={.55}/><Stop offset="100%" stopColor={C.green} stopOpacity={0}/></RadialGradient></Defs>
   <Edges active={done?COUNTS.length:layer}/>
   {pulses.map((p,i)=>{const f=((t/p.dur)+p.off)%1;return <Circle key={i} cx={p.e.a.x+(p.e.b.x-p.e.a.x)*f} cy={p.e.a.y+(p.e.b.y-p.e.a.y)*f} r={2.4} fill={C.mint} fillOpacity={Math.sin(f*Math.PI)}/>;})}
   <Nodes active={done?COUNTS.length:layer} done={done}/>
   {!done&&front.map((n,i)=>{const b=.5+.5*Math.sin(t/260+i*.9);return <G key={i}><Circle cx={n.x} cy={n.y} r={14+6*b} fill="url(#falconGlow)"/><Circle cx={n.x} cy={n.y} r={5.5} fill={C.green} stroke={C.mint} strokeWidth={1.5}/></G>;})}
   {labels.map((l,i)=><SvgText key={i} x={l.n.x+(layer>=COUNTS.length-2?-12:12)} y={l.n.y-9} fill={C.mint} fillOpacity={l.opacity} fontSize={12} fontFamily="Inter" textAnchor={layer>=COUNTS.length-2?'end':'start'}>{l.text}</SvgText>)}
  </Svg>
 </View>;
}
