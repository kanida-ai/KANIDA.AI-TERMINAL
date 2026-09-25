// Contract picker: calls left, puts right, strike in the centre. Buy/Sell are always visible on every priced strike
// (no hover needed). A strike with no price in the stored reading cannot be added - an absent price is never a zero.
import React,{useEffect,useMemo,useRef,useState} from 'react';
import {Pressable,ScrollView,View,useWindowDimensions} from 'react-native';
import {Button,C,Chip,Sheet,T,s} from '../ui';
import type {Chain,ChainSide,Kind,Leg,Side} from './api';
import {dayMonth,istStamp,num,strikeText} from './format';

const WINDOW=12;

export function ChainDrawer({visible,chain,legs,onToggle,onClose}:{visible:boolean;chain:Chain|null;legs:Leg[];
 onToggle:(strike:number,kind:Kind,side:Side,price:number)=>void;onClose:()=>void}){
 const narrow=useWindowDimensions().width<600;
 const [all,setAll]=useState(false);const [view,setView]=useState<'price'|'oi'|'greeks'>('price');
 const rows=useMemo(()=>{
  if(!chain)return [];
  if(all)return chain.rows;
  const i=chain.rows.findIndex(r=>r.strike===chain.atm_strike);
  return chain.rows.slice(Math.max(0,i-WINDOW),i+WINDOW+1);
 },[chain,all]);
 const scroller=useRef<any>(null);const ROW=narrow?56:45;
 const second=(x:ChainSide|null)=>view==='price'?(x?.iv!=null?`IV ${x.iv}%`:'IV —'):view==='oi'?`OI ${compactOi(x?.oi)}`:greek((x as any)?.greeks);
 const fl=narrow?{ltp:1.3,pair:2.5,strike:1.1}:{ltp:1.2,pair:1.6,strike:1.3};
 // open centred on the ATM strike, not at the top of the list
 useEffect(()=>{if(!visible||!chain)return;const i=rows.findIndex(r=>r.strike===chain.atm_strike);
  const t=setTimeout(()=>scroller.current?.scrollTo?.({y:Math.max(0,i*ROW-200),animated:false}),60);return()=>clearTimeout(t);},[visible,chain,rows]);
 const picked=(k:number,kind:Kind)=>legs.find(l=>l.strike===k&&l.type===kind);
 return <Sheet visible={visible} onClose={onClose} wide title="Add from the option chain"
  subtitle={chain?`${chain.underlying} · expiry ${dayMonth(chain.expiry)} · spot ${num(chain.spot,2)} · ${istStamp(chain.as_of)} · ${chain.quality.live?'Live (Kite): the chain shows last traded prices; an added leg is priced at buy-at-ask / sell-at-bid':'Stored reading: last traded prices only (this reading has no bid/ask)'}`:'Loading…'}
  footer={<View style={[s.between,{flexWrap:'wrap'}]}><T style={{fontSize:12,color:C.muted}}>{`${legs.length} leg${legs.length===1?'':'s'} in this strategy`}</T><Button label="Done" icon="check" onPress={onClose}/></View>}>
  <View style={[s.row,{flexWrap:'wrap',gap:8}]}>
   <Chip label="Price & IV" active={view==='price'} onPress={()=>setView('price')}/><Chip label="Open interest" active={view==='oi'} onPress={()=>setView('oi')}/><Chip label="Greeks" active={view==='greeks'} onPress={()=>setView('greeks')}/>
   <Chip label={all?'Near the money':'All strikes'} active={all} onPress={()=>setAll(!all)} icon="list"/>
  </View>
  {!chain?<T style={{color:C.muted}}>Loading the chain…</T>:
  <View style={{borderWidth:1,borderColor:C.line,borderRadius:12,overflow:'hidden'}}>
   <View style={[s.row,{backgroundColor:C.paper,paddingVertical:8,paddingHorizontal:8,gap:0}]}>
    {!narrow&&<H flex={1.2}>{view==='price'?'IV':view==='oi'?'OI':'Δ · Θ/day'}</H>}<H flex={fl.ltp}>{narrow?`Call LTP · ${view==='price'?'IV':view==='oi'?'OI':'Δ·Θ'}`:'Call LTP'}</H><H flex={fl.pair}> </H><H flex={fl.strike} center>Strike</H><H flex={fl.pair}> </H><H flex={fl.ltp} right>{narrow?`Put LTP · ${view==='price'?'IV':view==='oi'?'OI':'Δ·Θ'}`:'Put LTP'}</H>{!narrow&&<H flex={1.2} right>{view==='price'?'IV':view==='oi'?'OI':'Δ · Θ/day'}</H>}
   </View>
   <ScrollView ref={scroller} style={{maxHeight:520}}>
    {rows.map(r=>{const atm=r.strike===chain.atm_strike;const itmCall=r.strike<chain.spot;
     return <View key={r.strike} style={[s.row,{gap:0,paddingHorizontal:8,height:ROW,borderTopWidth:1,borderColor:C.line,backgroundColor:atm?C.soft:'transparent'}]}>
      {!narrow&&<Cell flex={1.2} tone={itmCall}>{view==='price'?(r.CE?.iv!=null?`${r.CE.iv}%`:'—'):view==='oi'?compactOi(r.CE?.oi):greek((r.CE as any)?.greeks)}</Cell>}
      {narrow?<Two flex={fl.ltp} tone={itmCall} a={r.CE?.ltp!=null?num(r.CE.ltp,2):'—'} b={second(r.CE)}/>:<Cell flex={1.2} tone={itmCall} strong>{r.CE?.ltp!=null?num(r.CE.ltp,2):'—'}</Cell>}
      <Pair big={narrow} flex={fl.pair} side={r.CE} picked={picked(r.strike,'CE')} label={`${strikeText(r.strike)} call`} onPress={(sd)=>r.CE?.ltp!=null&&onToggle(r.strike,'CE',sd,r.CE.ltp)}/>
      <View style={{flex:fl.strike,alignItems:'center'}}><T style={{fontFamily:'InterSemi',fontSize:13,fontVariant:['tabular-nums'] as any}}>{strikeText(r.strike)}</T>{atm&&<T style={{fontSize:9,color:C.green}}>ATM</T>}</View>
      <Pair big={narrow} flex={fl.pair} side={r.PE} picked={picked(r.strike,'PE')} label={`${strikeText(r.strike)} put`} onPress={(sd)=>r.PE?.ltp!=null&&onToggle(r.strike,'PE',sd,r.PE.ltp)}/>
      {narrow?<Two flex={fl.ltp} right tone={!itmCall&&!atm} a={r.PE?.ltp!=null?num(r.PE.ltp,2):'—'} b={second(r.PE)}/>:<Cell flex={1.2} right tone={!itmCall&&!atm} strong>{r.PE?.ltp!=null?num(r.PE.ltp,2):'—'}</Cell>}
      {!narrow&&<Cell flex={1.2} right tone={!itmCall&&!atm}>{view==='price'?(r.PE?.iv!=null?`${r.PE.iv}%`:'—'):view==='oi'?compactOi(r.PE?.oi):greek((r.PE as any)?.greeks)}</Cell>}
     </View>;})}
   </ScrollView>
  </View>}
  <T style={{fontSize:11,color:C.muted}}>{`Shaded cells are in the money. IV is computed from each option's ${chain?.quality.live?'bid/ask mid (last trade when there is no valid book)':'last traded price'} (Black-Scholes, 6.5% constant rate) and is blank where it cannot be solved.`}</T>
 </Sheet>;
}
function compactOi(v?:number|null){if(v==null)return '—';return v>=1e7?`${(v/1e7).toFixed(1)}Cr`:v>=1e5?`${(v/1e5).toFixed(1)}L`:v.toLocaleString('en-IN');}
function H({children,flex,right,center}:any){return <T style={{flex,fontSize:10,fontFamily:'InterMedium',letterSpacing:.6,color:C.muted,textTransform:'uppercase',textAlign:right?'right':center?'center':'left'}}>{children}</T>;}
function Cell({children,flex,right,tone,strong}:any){return <T style={{flex,fontSize:12,textAlign:right?'right':'left',fontVariant:['tabular-nums'] as any,color:tone?C.amber:C.ink,fontFamily:strong?'InterMedium':'Inter'}}>{children}</T>;}
function Two({flex,a,b,right,tone}:any){return <View style={{flex,alignItems:right?'flex-end':'flex-start'}}><T style={{fontSize:12,fontFamily:'InterMedium',color:tone?C.amber:C.ink,fontVariant:['tabular-nums'] as any}}>{a}</T>
 <T style={{fontSize:10,color:C.muted,fontVariant:['tabular-nums'] as any}} numberOfLines={1}>{b}</T></View>;}
function Pair({flex,side,picked,label,onPress,big}:{flex:number;side:ChainSide|null;picked?:Leg;label:string;onPress:(s:Side)=>void;big?:boolean}){
 const disabled=!side||side.ltp==null;
 const btn=(sd:Side)=>{const on=picked?.side===sd;const col=sd==='B'?C.green:C.red;
  return <Pressable key={sd} disabled={disabled} onPress={()=>onPress(sd)} accessibilityRole="button" accessibilityState={{selected:on,disabled}}
   accessibilityLabel={`${sd==='B'?'Buy':'Sell'} ${label}${on?' (added)':''}`} {...({'aria-pressed':on} as any)}
   hitSlop={big?undefined:{top:8,bottom:8,left:4,right:4}}
   style={({pressed})=>({width:big?44:32,height:big?44:30,borderRadius:8,borderWidth:1,borderColor:disabled?C.line:col,backgroundColor:on?col:'transparent',alignItems:'center',justifyContent:'center',opacity:disabled?.35:pressed?.7:1})}>
   <T style={{fontSize:11,fontFamily:'InterSemi',color:on?'#041B12':disabled?C.muted:col}}>{sd}</T></Pressable>;};
 return <View style={[s.row,{flex,gap:6,justifyContent:'center'}]}>{btn('B')}{btn('S')}</View>;
}

/** Per-unit model Greeks at the reading (each option at its own IV): delta · theta per calendar day. */
function greek(g?:{delta:number;theta:number}|null){return g?`${g.delta.toFixed(2)} · ${g.theta.toFixed(1)}`:'—';}
