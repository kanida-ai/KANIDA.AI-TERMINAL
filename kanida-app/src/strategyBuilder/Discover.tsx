// K04 Discover and Compare: the user's own thesis and limits -> 3-6 explained structures, never a long list, never a
// silent relaxation. Slice 9: cards carry Lab evidence corrected for everything the user tested (BH FDR 10%);
// 'Tested ✓' rules rank first by their out-of-sample 95% low, everything else keeps the model order.
import React,{useEffect,useMemo,useRef,useState} from 'react';
import {TextInput,View,useWindowDimensions} from 'react-native';
import Svg,{Line,Path} from 'react-native-svg';
import {router} from 'expo-router';
import {Badge,Button,C,Checkbox,Chip,Icon,T,s} from '../ui';
import {sb,type Candidate,type DiscoverResult,type Expiry} from './api';
import {dayMonth,inr,istStamp,num,signed,strikeText} from './format';

const VIEWS=[['up','Rise'],['down','Fall'],['range','Stay in a range'],['big_move','Big move either way']] as const;
const COLORS=['#39E5A3','#EBC66B','#8FB7FF'];

export function Discover(){
 const {width}=useWindowDimensions();const wide=width>=1000;
 const [unds,setUnds]=useState<string[]>([]);const [u,setU]=useState('NIFTY');const [exps,setExps]=useState<Expiry[]>([]);const [e,setE]=useState('');
 const [spot,setSpot]=useState<number|null>(null);const [asOf,setAsOf]=useState<string|null>(null);
 const [view,setView]=useState<'up'|'down'|'range'|'big_move'>('up');
 const [target,setTarget]=useState('');const [low,setLow]=useState('');const [high,setHigh]=useState('');const [maxLoss,setMaxLoss]=useState('');const [budget,setBudget]=useState('');
 const [hedged,setHedged]=useState(true);const [lots,setLots]=useState(1);
 const [res,setRes]=useState<DiscoverResult|null>(null);const [error,setError]=useState('');const [busy,setBusy]=useState(false);const [pick,setPick]=useState<string[]>([]);
 useEffect(()=>{sb.underlyings().then(r=>setUnds(r.underlyings.map(x=>x.symbol))).catch(()=>{});},[]);
 // GTM audit P03: a result belongs to the inputs that produced it. Any material change makes it STALE at once (Use and
 // Compare disabled), and a late reply to a superseded request is dropped.
 const reqSeq=useRef(0);const inputsKey=JSON.stringify([u,e,view,target,low,high,maxLoss,budget,hedged,lots]);const [resKey,setResKey]=useState('');
 const stale=!!res&&resKey!==inputsKey;
 useEffect(()=>{if(res&&resKey!==inputsKey){setPick([]);reqSeq.current++;setBusy(false);}},[inputsKey]);// eslint-disable-line react-hooks/exhaustive-deps
 useEffect(()=>{setRes(null);sb.expiries(u).then(r=>{setExps(r.expiries);setE((r.expiries.find(x=>(x.days_to_expiry??0)>=1)||r.expiries[0])?.expiry||'');setAsOf(r.as_of);}).catch(x=>setError(x.message));},[u]);
 const [src,setSrc]=useState<{live:boolean;as_of:string}|null>(null);
 useEffect(()=>{if(!e)return;sb.chain(u,e).then(c=>{setSpot(c.spot);setSrc({live:!!c.quality?.live,as_of:c.as_of});const step=c.strike_step||50;const r=(x:number)=>String(Math.round(x/step)*step);
  setTarget(r(c.spot*1.01));setLow(r(c.spot*.99));setHigh(r(c.spot*1.01));}).catch(()=>{});},[u,e]);
 useEffect(()=>{if(!spot)return;const step=50;const r=(x:number)=>String(Math.round(x/step)*step);
  if(view==='up')setTarget(r(spot*1.01));if(view==='down')setTarget(r(spot*.99));if(view==='big_move'){setLow(r(spot*.98));setHigh(r(spot*1.02));}if(view==='range'){setLow(r(spot*.99));setHigh(r(spot*1.01));}},[view,spot]);
 async function find(){const mine=++reqSeq.current;const key=inputsKey;setBusy(true);setError('');setPick([]);
  try{const r=await sb.discover({underlying:u,expiry:e,view,target:target||undefined,low:low||undefined,high:high||undefined,max_loss:maxLoss||undefined,budget:budget||undefined,hedged_only:hedged,lots});
   if(mine===reqSeq.current){setRes(r);setResKey(key);}}
  catch(x:any){if(mine===reqSeq.current){setError(x.message);setRes(null);}}finally{if(mine===reqSeq.current)setBusy(false);}}
 async function use(c:Candidate){if(stale||!c.candidate_id)return;
  try{const s=await sb.discoverUse(c.candidate_id);router.push({pathname:'/strategies',params:{id:s.id}} as any);}catch(x:any){setError(x.message);}}
 const compared=useMemo(()=>(res?.candidates||[]).filter(c=>pick.includes(c.template)),[res,pick]);
 const field=(l:string,v:string,set:(x:string)=>void,ph='')=><View style={{gap:4,minWidth:130,flex:1}}><T style={{fontSize:11,color:C.muted}}>{l}</T>
  <TextInput value={v} onChangeText={set} placeholder={ph} placeholderTextColor={C.muted} keyboardType="decimal-pad" accessibilityLabel={l}
   style={{height:40,borderWidth:1,borderColor:C.line,borderRadius:10,paddingHorizontal:12,color:C.ink,fontFamily:'Inter',fontSize:13,backgroundColor:C.bg}}/></View>;

 return <View style={{padding:wide?24:14,gap:16,maxWidth:1200,width:'100%',alignSelf:'center'}}>
  <View style={[s.row,{gap:8}]}><Button label="My Strategies" icon="chevron-left" kind="outline" onPress={()=>router.replace('/strategies' as any)}/></View>
  <View style={{gap:4}}><T style={{fontFamily:'ManropeBold',fontSize:22}}>Find a strategy</T>
   <T style={{fontSize:13,color:C.muted}}>Your view and your limits in; a short list of structures that match them out. It is a matching tool, not a forecast or advice.</T></View>
  <View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:16,gap:14}}>
   <View style={[s.row,{flexWrap:'wrap',gap:8}]}>{unds.map(x=><Chip key={x} label={x} active={u===x} onPress={()=>setU(x)}/>)}</View>
   <View style={[s.row,{flexWrap:'wrap',gap:8}]}>{exps.slice(0,8).map(x=><Chip key={x.expiry} label={`${dayMonth(x.expiry)} ${x.monthly?'M':'W'} · ${Math.round(x.days_to_expiry)}d`} active={e===x.expiry} onPress={()=>setE(x.expiry)}/>)}</View>
   <T style={{fontSize:12,color:src?.live?C.green:C.amber}}>{src?.live?`Live · Zerodha Kite · ${istStamp(src.as_of)} · spot ${num(spot,2)}`:`Stored reading · ${istStamp(src?.as_of||asOf)} · spot ${num(spot,2)} · not live`}</T>
   <View style={{gap:6}}><T style={{fontSize:12,color:C.muted}}>{`Where do you think ${u} goes by ${dayMonth(e)}?`}</T>
    <View style={[s.row,{flexWrap:'wrap',gap:8}]}>{VIEWS.map(([k,l])=><Chip key={k} label={l} active={view===k} onPress={()=>setView(k)}/>)}</View></View>
   <View style={[s.row,{flexWrap:'wrap',gap:12}]}>
    {(view==='up'||view==='down')&&field(`Target on expiry (${view==='up'?'above':'below'} spot)`,target,setTarget)}
    {(view==='range'||view==='big_move')&&<>{field(view==='range'?'Range low':'Move below',low,setLow)}{field(view==='range'?'Range high':'Move above',high,setHigh)}</>}
    {field('Maximum loss I accept (₹, optional)',maxLoss,setMaxLoss,'e.g. 5000')}
    {field('Max-loss budget (₹, optional)',budget,setBudget,'e.g. 20000')}
    <View style={{gap:4}}><T style={{fontSize:11,color:C.muted}}>Lots</T><View style={[s.row,{gap:6}]}><Button label="−" kind="outline" onPress={()=>setLots(Math.max(1,lots-1))}/><T style={{minWidth:24,textAlign:'center'}}>{lots}</T><Button label="+" kind="outline" onPress={()=>setLots(Math.min(50,lots+1))}/></View></View>
   </View>
   <Checkbox checked={hedged} onChange={setHedged} label="Defined risk only (recommended)" detail="Leaves out naked short options, whose loss is unlimited (calls) or runs to a zero price (puts)."/>
   <View style={[s.row,{gap:10}]}><Button label="Find strategies" icon="search" loading={busy} disabled={!e} onPress={find}/>{!!error&&<T style={{color:C.red,fontSize:12,flex:1}}>{error}</T>}</View>
  </View>

  {res&&stale&&<View accessibilityRole="alert" style={[s.row,{gap:8,backgroundColor:C.amberBg,borderRadius:10,padding:10}]}><Icon name="alert-circle" size={14} color={C.amber}/>
   <T style={{fontSize:12,color:C.amber,flex:1}}>Your inputs changed - these results are for the previous inputs. Find again to act on them.</T><Button label="Find again" kind="outline" onPress={find}/></View>}
  {res&&<View style={{gap:12,opacity:stale?.45:1}} pointerEvents={stale?'none':'auto'}>
   <T style={{fontSize:13}}>{res.candidates.length?`${res.candidates.length} of ${res.considered} structures match - ${res.view_label.toLowerCase()} ${res.view==='up'||res.view==='down'?`to ${num(res.inputs.target,0)}`:`${num(res.inputs.low,0)}-${num(res.inputs.high,0)}`} by ${dayMonth(res.expiry)}.`:'Nothing matches these limits.'}</T>
   {res.evidence&&<T style={{fontSize:12}}>{res.evidence.tests?`${res.evidence.tests} rule test${res.evidence.tests===1?'':'s'} in your Lab (n ≥ ${res.evidence.min_oos} out of sample); ${res.evidence.survivors} survive the ${Math.round(res.evidence.fdr_q*100)}% false-discovery correction.`:'No rule in your Lab has enough out-of-sample trades yet - every card is Model only. Prove one in the Lab to rank by evidence.'}</T>}
   <T style={{fontSize:11,color:C.muted}}>{res.basis}</T>
   {!res.candidates.length&&res.binding&&<View style={{backgroundColor:C.amberBg,borderRadius:12,padding:14,gap:6}}>
    <T style={{color:C.amber,fontFamily:'InterSemi'}}>{`Most were excluded for: ${res.binding.label}`}</T><T style={{color:C.amber,fontSize:12}}>{res.binding.suggestion}</T>
    <T style={{color:C.muted,fontSize:11}}>{res.excluded.map(x=>`${x.count} × ${x.label}`).join(' · ')}</T></View>}
   <View style={{flexDirection:wide?'row':'column',flexWrap:'wrap',gap:12}}>
    {res.candidates.map((c,i)=><View key={c.template} style={{flexBasis:wide?'31%':'auto',flexGrow:1,backgroundColor:C.paper,borderWidth:1,borderColor:pick.includes(c.template)?C.green:C.line,borderRadius:14,padding:14,gap:8}}>
     <View style={s.between}><T style={{fontFamily:'InterSemi',fontSize:15}}>{c.name}</T><Badge label={c.evidence.label} tone={({tested_significant:'green',tested_not_significant:'neutral'} as any)[c.evidence.status]||'amber'}/></View>
     {!!c.evidence.note&&<T style={{fontSize:11,color:C.muted}}>{c.evidence.note}</T>}
     <T style={{fontSize:10,color:C.muted}}>{c.evidence.status==='tested_significant'?'Model-priced Lab history (one IV, no skew). Past results do not guarantee future results.':c.evidence.status==='tested_not_significant'?'Tested in your Lab; the out-of-sample result does not survive the correction for everything tested.':'Model only - no reliable history for this structure. Numbers are model values at this reading.'}</T>
     <T style={{fontSize:12,color:C.muted}}>{c.recipe}{c.param!=null?` · ${c.param_label} ${c.param}`:''}</T>
     {c.legs.map((l:any,j:number)=><T key={j} style={{fontSize:12,fontVariant:['tabular-nums'] as any}}>{`${l.side==='B'?'Buy':'Sell'} ${l.lots} × ${strikeText(l.strike)} ${l.type} @ ${num(l.price)} LTP`}</T>)}
     {c.why.map((w,j)=><View key={j} style={[s.row,{gap:6,alignItems:'flex-start'}]}><Icon name={j?'check':'target'} size={12} color={j?C.green:C.mint}/><T style={{fontSize:12,flex:1}}>{w}</T></View>)}
     <View style={[s.row,{flexWrap:'wrap',gap:12}]}>
      <Kv k="Max loss" v={c.max_loss.unlimited?'Unlimited':signed(c.max_loss.value)} tone={C.red}/><Kv k="Max profit" v={c.max_profit.unlimited?'Unlimited':signed(c.max_profit.value)} tone={C.green}/>
      <Kv k="Breakeven" v={c.breakevens.map(b=>num(b,0)).join(' · ')}/><Kv k="Max loss (structural)" v={inr(c.capital_at_risk)}/><Kv k="Funds / margin" v="Not estimated here"/><Kv k="POP (model)" v={c.pop!=null?`${c.pop}%`:'—'}/>
     </View>
     <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
      <Checkbox checked={pick.includes(c.template)} onChange={on=>{if(!stale)setPick(p=>on?(p.length>=3?p:[...p,c.template]):p.filter(x=>x!==c.template));}} label="Compare"/>
      <View style={{flex:1}}/><Button label="Use as draft" icon="arrow-right" disabled={stale} onPress={()=>use(c)}/></View>
     <T style={{fontSize:10,color:C.muted}}>Priced at last traded (LTP). The draft reprices at buy-at-ask / sell-at-bid where live quotes exist, so its numbers can differ.</T>
    </View>)}
   </View>
   {compared.length>1&&<Compare list={compared} target={res.view==='up'||res.view==='down'?res.inputs.target:null}/>}
  </View>}
 </View>;
}
function Kv({k,v,tone}:{k:string;v:string;tone?:string}){return <View style={{gap:1,minWidth:90}}><T style={{fontSize:10,color:C.muted}}>{k}</T><T style={{fontSize:13,fontFamily:'InterSemi',color:tone||C.ink,fontVariant:['tabular-nums'] as any}}>{v}</T></View>;}

function expiryPnl(legs:any[],x:number){return legs.reduce((a,l)=>{const q=(l.side==='B'?1:-1)*l.lots*l.lot_size;const iv=l.type==='CE'?Math.max(x-l.strike,0):Math.max(l.strike-x,0);return a+q*(iv-l.price);},0);}
function Compare({list,target}:{list:Candidate[];target:number|null}){
 const [w,setW]=useState(700);const H=220;
 const ks=list.flatMap(c=>c.legs.map((l:any)=>l.strike));const lo=Math.min(...ks)*.985,hi=Math.max(...ks)*1.015;
 const xs=Array.from({length:120},(_,i)=>lo+(hi-lo)*i/119);
 const series=list.map(c=>xs.map(x=>expiryPnl(c.legs,x)));const all=series.flat();const y0=Math.min(0,...all),y1=Math.max(0,...all);
 const X=(x:number)=>40+(x-lo)/(hi-lo)*(w-50);const Y=(y:number)=>10+(y1-y)/((y1-y0)||1)*(H-30);
 return <View onLayout={e=>setW(e.nativeEvent.layout.width)} style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:14,gap:10}}>
  <T style={{fontFamily:'InterSemi'}}>Compare at expiry (same lots, gross of charges)</T>
  <Svg width={w} height={H}><Line x1={40} x2={w-10} y1={Y(0)} y2={Y(0)} stroke={C.muted}/>
   {target!=null&&<Line x1={X(target)} x2={X(target)} y1={10} y2={H-20} stroke={C.mint} strokeDasharray="4,3"/>}
   {series.map((ys,i)=><Path key={i} d={ys.map((y,j)=>`${j?'L':'M'}${X(xs[j]).toFixed(1)},${Y(y).toFixed(1)}`).join('')} stroke={COLORS[i]} strokeWidth={2} fill="none"/>)}</Svg>
  <View style={{gap:4}}>{[['',...list.map(c=>c.name)],['Max loss',...list.map(c=>c.max_loss.unlimited?'Unlimited':signed(c.max_loss.value))],['Max profit',...list.map(c=>c.max_profit.unlimited?'Unlimited':signed(c.max_profit.value))],
   ['Capital at risk',...list.map(c=>inr(c.capital_at_risk))],['At your view',...list.map(c=>signed(c.pnl_at_view))],['Return on risk',...list.map(c=>c.return_on_risk!=null?`${c.return_on_risk}%`:'—')],['POP (model)',...list.map(c=>c.pop!=null?`${c.pop}%`:'—')]]
   .map((r,i)=><View key={i} style={[s.row,{gap:8,borderTopWidth:i?1:0,borderColor:C.line,paddingVertical:5}]}>{r.map((v,j)=><T key={j} style={{flex:1,fontSize:12,color:j&&i===0?COLORS[j-1]:i===0||j===0?C.muted:C.ink,fontFamily:i===0?'InterSemi':'Inter'}}>{v}</T>)}</View>)}</View>
 </View>;
}
