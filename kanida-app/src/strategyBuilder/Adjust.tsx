// K12 Adjustment assistant (slice 8). Candidate adjustments from a fixed rules catalogue, each with the delta orders
// priced where they would execute, the exact expiry P&L after the adjustment overlaid on the current position, the
// change in worst case / delta / margin, and the Lab evidence for exactly that rule on exactly that structure (or
// "Model only"). Choosing one writes a new draft version; for a paper deployment, Order Review then shows only the
// delta orders. Nothing is ordered from here.
import React,{useEffect,useMemo,useState} from 'react';
import {View,useWindowDimensions} from 'react-native';
import Svg,{Line,Path} from 'react-native-svg';
import {Badge,Button,C,Loading,Sheet,T,s} from '../ui';
import {sb,type AdjustCandidate,type AdjustResult,type Deployment} from './api';
import {inr,istStamp,num,signed} from './format';

const EV_TONE:Record<string,any>={adjust_helped:'green',adjust_hurt:'red',adjust_not_significant:'neutral',insufficient:'amber',model_only:'amber'};

export function AdjustSheet({visible,onClose,strategyId,version,deployment,onApplied}:{visible:boolean;onClose:()=>void;strategyId:string;version:number;
 deployment?:Deployment|null;onApplied:(r:{strategy:any;adjustment:AdjustCandidate;deployment_id:string|null})=>void}){
 const {width}=useWindowDimensions();const wide=width>=900;
 const [r,setR]=useState<AdjustResult|null>(null);const [error,setError]=useState('');const [busy,setBusy]=useState('');
 const load=async()=>{setError('');setR(null);try{setR(await sb.adjustCandidates(strategyId,deployment?.id));}catch(e:any){setError(e.message);}};
 useEffect(()=>{if(visible)load();},[visible,deployment?.id]);// eslint-disable-line react-hooks/exhaustive-deps
 async function use(c:AdjustCandidate){setBusy(c.rule+(c.k??''));setError('');
  try{onApplied(await sb.adjustApply(strategyId,{rule:c.rule,k:c.k,version,deployment_id:deployment?.id}));}
  catch(e:any){setError(e.message);if(/changed|conflict/i.test(e.message))load();}finally{setBusy('');}}
 const avail=(r?.candidates||[]).filter(c=>c.available);const na=(r?.candidates||[]).filter(c=>!c.available);
 return <Sheet visible={visible} onClose={onClose} wide title={deployment?'Adjust paper deployment':'Adjust this strategy'}
  subtitle={r?`${r.structure} · spot ${num(r.spot,2)} · quotes ${istStamp(r.as_of)} · entries from ${r.entry_basis}`:'Evaluating adjustments against the market now…'}
  footer={<View style={[s.between,{flexWrap:'wrap',gap:8}]}><T style={{fontSize:11,color:C.muted,flex:1,minWidth:240}}>Choosing an adjustment writes a new version of this strategy. {deployment?'You then review only the delta orders - nothing is sent before that.':'Nothing is ordered.'}</T>
   <Button label="Refresh" icon="refresh-cw" kind="outline" onPress={load}/></View>}>
  {!!error&&<View style={{backgroundColor:'#2A1519',borderRadius:10,padding:12}}><T style={{color:C.red,fontSize:13}}>{error}</T></View>}
  {!r&&!error&&<Loading/>}
  {r&&<>
   {!!r.horizon&&<View style={[s.row,{gap:8,alignItems:'flex-start'}]}><Badge label={r.horizon.kind==='model_near_expiry'?'MODEL':'EXACT'} tone={r.horizon.kind==='model_near_expiry'?'amber':'green'}/>
    <T style={{fontSize:12,color:r.horizon.kind==='model_near_expiry'?C.amber:C.muted,flex:1}}>{`Current and every proposal share one horizon: ${r.horizon.label}. ${r.horizon.note}`}</T></View>}
   <View style={[s.row,{flexWrap:'wrap',gap:18}]}>
    <Kv k="Tested" v={r.tested?r.tested.label:'No short leg'} note={r.tested?`${r.tested.distance_pct>=0?num(r.tested.distance_pct,2)+'% from the money':num(-r.tested.distance_pct,2)+'% in the money'}`:'Only resizing or closing applies'}/>
    <Kv k="Worst case now" v={r.current.unlimited_loss?'Unlimited':inr(r.current.worst)}/>
    <Kv k="Best case now" v={r.current.unlimited_profit?'Unlimited':r.current.best==null?'—':inr(r.current.best)}/>
    <Kv k="Breakevens now" v={r.current.breakevens.length?r.current.breakevens.map(b=>num(b,0)).join(' / '):'—'}/>
    <Kv k="Delta now" v={r.current.delta==null?'Unavailable':num(r.current.delta,2)} note={r.current.delta==null?'A leg\'s IV cannot be solved from its price':'Model, ₹ per 1-point move'}/>
    <Kv k="Margin now" v={r.current.margin==null?'Unavailable':inr(r.current.margin)} note={r.current.margin==null?'Needs a live broker margin read':'Kite basket margin'}/>
   </View>
   <View style={{flexDirection:wide?'row':'column',flexWrap:'wrap',gap:12}}>
    {avail.map(c=><View key={c.rule+(c.k??'')} style={{flexBasis:wide?'48%':'auto',flexGrow:1,backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:14,gap:8}}>
     <View style={[s.between,{flexWrap:'wrap',gap:6}]}><T style={{fontFamily:'InterSemi',fontSize:15}}>{c.name}</T>
      <Badge label={c.evidence.label} tone={EV_TONE[c.evidence.status||'model_only']||'amber'}/></View>
     <T style={{fontSize:12,color:C.muted}}>{`${c.note} → ${c.structure_after}`}</T>
     {!!c.evidence.note&&<T style={{fontSize:10,color:C.muted}}>{c.evidence.note}</T>}
     <Overlay points={c.overlay||[]} spot={r.spot}/>
     <View style={[s.row,{flexWrap:'wrap',gap:14}]}>
      <Kv k={(c.cash||0)>=0?'Credit':'Debit'} v={inr(Math.abs(c.cash||0))} note={`charges ${inr(c.charges||0)}`}/>
      <Kv k="Worst case after" v={c.after?.unlimited_loss?'Unlimited':inr(c.after?.worst??0)}/>
      <Kv k="Best case after" v={c.after?.unlimited_profit?'Unlimited':c.after?.best==null?'—':inr(c.after.best)}/>
      <Kv k="Breakevens after" v={c.after?.breakevens.length?c.after.breakevens.map(b=>num(b,0)).join(' / '):'—'}/>
      <Kv k="Delta change" v={c.delta?.change==null?'Unavailable':signed(c.delta.change)} note={c.delta?.change==null?'Model delta not computable':undefined}/>
      <Kv k="Margin change" v={c.margin?.change==null?'Unavailable':signed(c.margin.change)}/>
     </View>
     <View style={{gap:2}}>{(c.orders||[]).map((o,i)=><T key={i} style={{fontSize:11,color:o.side==='B'?C.green:C.red,fontVariant:['tabular-nums'] as any}}>
      {`${o.side==='B'?'BUY':'SELL'} ${o.qty} ${o.symbol} @ ${num(o.price)} (${o.basis==='exec'?(o.side==='B'?'ask':'bid'):'LTP - no bid/ask'}) · ${o.effect}`}</T>)}</View>
     <Button label="Use this adjustment" icon="check" loading={busy===c.rule+(c.k??'')} onPress={()=>use(c)}/>
    </View>)}
   </View>
   {!!na.length&&<View style={{gap:4}}><T style={{fontSize:12,fontFamily:'InterSemi'}}>Not available now</T>
    {na.map(c=><T key={c.rule+(c.k??'')} style={{fontSize:11,color:C.muted}}>{`${c.name}: ${c.reason}`}</T>)}</View>}
   <View style={{backgroundColor:C.paper,borderRadius:10,padding:12,gap:3}}>{r.notes.map((n,i)=><T key={i} style={{fontSize:11,color:C.muted}}>{n}</T>)}</View>
  </>}
 </Sheet>;
}

function Kv({k,v,note}:{k:string;v:string;note?:string}){return <View style={{gap:1,minWidth:110}}><T style={{fontSize:11,color:C.muted}}>{k}</T><T style={{fontSize:14,fontFamily:'InterSemi'}}>{v}</T>{!!note&&<T style={{fontSize:10,color:C.muted}}>{note}</T>}</View>;}

/** P&L at the common horizon now (dashed) vs after the adjustment (solid), with the zero line and spot. */
function Overlay({points,spot,height=150}:{points:{s:number;current:number;after:number}[];spot:number;height?:number}){
 const [w,setW]=useState(420);
 const g=useMemo(()=>{if(points.length<2)return null;
  const x0=points[0].s,x1=points[points.length-1].s;const ys=points.flatMap(p=>[p.current,p.after]);let y0=Math.min(0,...ys),y1=Math.max(0,...ys);const pad=(y1-y0)*.08||1;y0-=pad;y1+=pad;
  const X=(v:number)=>8+(v-x0)/(x1-x0||1)*(w-16);const Y=(v:number)=>6+(y1-v)/(y1-y0||1)*(height-12);
  const path=(k:'current'|'after')=>points.map((p,i)=>`${i?'L':'M'}${X(p.s).toFixed(1)},${Y(p[k]).toFixed(1)}`).join('');
  return {X,Y,cur:path('current'),aft:path('after'),x0,x1};},[points,w,height]);
 if(!g)return null;
 return <View onLayout={e=>setW(Math.max(240,e.nativeEvent.layout.width))} accessibilityLabel="Expiry profit and loss: dashed line now, solid line after the adjustment">
  <Svg width={w} height={height}>
   <Line x1={8} x2={w-8} y1={g.Y(0)} y2={g.Y(0)} stroke={C.line} strokeWidth={1}/>
   {spot>=g.x0&&spot<=g.x1&&<Line x1={g.X(spot)} x2={g.X(spot)} y1={4} y2={height-4} stroke={C.muted} strokeWidth={1} strokeDasharray="2,3"/>}
   <Path d={g.cur} stroke={C.muted} strokeWidth={1.5} fill="none" strokeDasharray="5,4"/>
   <Path d={g.aft} stroke={C.green} strokeWidth={2} fill="none"/>
  </Svg>
  <View style={[s.row,{gap:14}]}><T style={{fontSize:10,color:C.muted}}>- - now</T><T style={{fontSize:10,color:C.green}}>—— after</T><T style={{fontSize:10,color:C.muted}}>┆ spot</T></View>
 </View>;
}
