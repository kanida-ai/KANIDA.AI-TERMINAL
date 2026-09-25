// K11 Order review (slice 4). The preview IS the plan: exact contracts, sides, quantities, limits, product, sequence,
// slices, the quotes it came from, the checks, exchange margin and charges - with a hash and a 30 s expiry. Placing
// sends that exact plan, once (idempotency key), to the PAPER broker. Live routing is shown as a disabled capability.
import React,{useCallback,useEffect,useRef,useState} from 'react';
import {Pressable,TextInput,View} from 'react-native';
import {Badge,Button,C,Checkbox,Chip,Icon,Loading,Sheet,T,s} from '../ui';
import {exec,type Deployment,type Preview} from './api';
import {inr,istEpoch,istStamp,num,signed,strikeText} from './format';

const key=()=>'k'+Date.now().toString(36)+Math.random().toString(36).slice(2,8);

export function OrderReview({visible,onClose,strategyId,deployment,onPlaced}:{visible:boolean;onClose:()=>void;strategyId:string;
 deployment?:Deployment|null;onPlaced:(d:Deployment)=>void}){
 const closing=!!deployment;
 const [p,setP]=useState<Preview|null>(null);const [error,setError]=useState('');const [busy,setBusy]=useState(false);
 const [product,setProduct]=useState('NRML');const [policy,setPolicy]=useState('marketable');const [limits,setLimits]=useState<Record<string,string>>({});const [committed,setCommitted]=useState<Record<string,string>>({});
 const [ack,setAck]=useState(false);const [left,setLeft]=useState(0);const idem=useRef(key());
 const load=useCallback(async()=>{setError('');setP(null);
  try{const lim=Object.fromEntries(Object.entries(committed).filter(([,v])=>v.trim()!=='').map(([k,v])=>[k,Number(v)]));
   const r=closing?await exec.closePreview(deployment!.id,policy):await exec.preview(strategyId,{product,price_policy:policy,limits:lim});
   setP(r);idem.current=key();}
  catch(e:any){setError(e.message);}},[closing,deployment,strategyId,product,policy,committed]);
 useEffect(()=>{if(visible)load();},[visible,product,policy,committed]);// eslint-disable-line react-hooks/exhaustive-deps
 useEffect(()=>{if(!visible){setLimits({});setCommitted({});}},[visible]);
 useEffect(()=>{if(!p)return;const t=setInterval(()=>setLeft(Math.max(0,Math.round(p.expires_at-Date.now()/1000))),500);return()=>clearInterval(t);},[p]);
 const expired=!!p&&left<=0;
 async function place(){if(!p)return;setBusy(true);setError('');
  try{const d=closing?await exec.close(deployment!.id,p,idem.current):await exec.deploy(strategyId,p,idem.current,ack);onPlaced(d);}
  catch(e:any){setError(e.message);if(/expired|changed/i.test(e.message))load();}finally{setBusy(false);}}
 const n=p?.orders.reduce((a,o)=>a+o.slices.length,0)||0;
 return <Sheet visible={visible} onClose={onClose} wide title={closing?'Close paper deployment':'Review paper orders'}
  subtitle={p?`${p.structure} · quotes ${istStamp(p.as_of)} · spot ${num(p.spot,2)} · PAPER - no order reaches a broker`:'Building the exact order plan from live quotes…'}
  footer={<View style={[s.between,{flexWrap:'wrap',gap:10}]}>
   <View style={{gap:2,flex:1,minWidth:220}}>{p&&<T style={{fontSize:12,color:expired?C.red:C.muted}}>{expired?'Preview expired - quotes move. Refresh to review again.':`This plan is valid for ${left}s · hash ${p.hash.slice(0,10)}`}</T>}
    {p&&<T style={{fontSize:11,color:C.muted}}>{p.live.reason}</T>}</View>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
    <Button label="Refresh" icon="refresh-cw" kind="outline" onPress={load}/>
    <Button label="Send live" icon="lock" kind="outline" disabled accessibilityHint={p?.live.reason}/>
    <Button label={closing?`Place ${n} close order${n===1?'':'s'} (paper)`:`Place ${n} paper order${n===1?'':'s'}`} icon="check" loading={busy}
     disabled={!p||!p.can_submit||expired||(p.requires_ack&&!ack)} onPress={place}/>
   </View></View>}>
  {!closing&&<View style={[s.row,{flexWrap:'wrap',gap:8}]}>
   <T style={{fontSize:11,color:C.muted}}>Product</T>{['NRML','MIS'].map(x=><Chip key={x} label={x==='NRML'?'NRML (carry)':'MIS (intraday)'} active={product===x} onPress={()=>setProduct(x)}/>)}
   <T style={{fontSize:11,color:C.muted,marginLeft:8}}>Limit price</T>{[['marketable','At the quote (ask/bid)'],['mid','At the mid']].map(([k,l])=><Chip key={k} label={l} active={policy===k} onPress={()=>setPolicy(k)}/>)}
  </View>}
  {closing&&<View style={[s.row,{gap:8}]}>{[['marketable','At the quote'],['mid','At the mid']].map(([k,l])=><Chip key={k} label={l} active={policy===k} onPress={()=>setPolicy(k)}/>)}</View>}
  {!!error&&<View style={{backgroundColor:'#2A1519',borderRadius:10,padding:12}}><T style={{color:C.red,fontSize:13}}>{error}</T></View>}
  {!p&&!error&&<Loading/>}
  {p&&<>
   <View style={{borderWidth:1,borderColor:C.line,borderRadius:12,overflow:'hidden'}}>
    <View style={[s.row,{backgroundColor:C.paper,paddingHorizontal:12,paddingVertical:8}]}>{['#','Order','Qty','Bid / Ask','Limit','Slices','Charges'].map((h,i)=>
     <T key={h} style={{flex:[0.4,2.4,0.8,1.3,1.1,1,0.9][i],fontSize:10,letterSpacing:.6,color:C.muted,textTransform:'uppercase',fontFamily:'InterMedium',textAlign:i>1?'right':'left'}}>{h}</T>)}</View>
    {p.orders.map(o=><View key={o.leg_id} style={[s.row,{paddingHorizontal:12,paddingVertical:10,borderTopWidth:1,borderColor:C.line}]}>
     <T style={{flex:.4,fontSize:12,color:C.muted}}>{o.group}</T>
     <View style={{flex:2.4,gap:2}}><T style={{fontSize:13,fontFamily:'InterSemi',color:o.side==='B'?C.green:C.red}}>{`${o.side==='B'?'BUY':'SELL'} ${o.symbol}`}</T>
      <T style={{fontSize:11,color:C.muted}}>{`${strikeText(o.strike)} ${o.type} · ${o.lots} lot${o.lots>1?'s':''} × ${o.lot_size} · LIMIT · ${p.product}`}</T></View>
     <T style={{flex:.8,fontSize:12,textAlign:'right'}}>{o.qty}</T>
     <T style={{flex:1.3,fontSize:12,textAlign:'right',color:C.muted}}>{`${num(o.bid)} / ${num(o.ask)}`}</T>
     <View style={{flex:1.1,alignItems:'flex-end'}}>{closing?<T style={{fontSize:12,fontFamily:'InterSemi'}}>{num(o.limit)}</T>:
      <TextInput value={limits[o.leg_id]??String(o.limit)} onChangeText={v=>setLimits(l=>({...l,[o.leg_id]:v}))} onBlur={()=>setCommitted(c=>limits[o.leg_id]!=null&&limits[o.leg_id]!==c[o.leg_id]?{...c,[o.leg_id]:limits[o.leg_id]}:c)}
       keyboardType="decimal-pad" accessibilityLabel={`Limit price for ${o.symbol}`} style={{width:76,height:28,borderWidth:1,borderColor:C.line,borderRadius:7,paddingHorizontal:6,color:C.ink,fontFamily:'InterSemi',fontSize:12,textAlign:'right'}}/>}</View>
     <T style={{flex:1,fontSize:12,textAlign:'right',color:o.slices.length>1?C.amber:C.ink}}>{o.slices.length>1?o.slices.join(' + '):'1'}</T>
     <T style={{flex:.9,fontSize:12,textAlign:'right'}}>{inr(o.charges)}</T>
    </View>)}
   </View>
   <T style={{fontSize:12,color:C.muted}}>{p.kind==='close'?'Sequence: buy back shorts first; sell longs only after every buy-back has filled.':`Sequence: ${p.sequence} Group 1 is sent first.`}</T>
   <View style={[s.row,{flexWrap:'wrap',gap:18}]}>
    <Kv k={p.net_premium>=0?'Net credit':'Net debit'} v={inr(Math.abs(p.net_premium))}/>
    {p.kind==='open'&&<Kv k="Exchange margin (Kite)" v={p.margin?inr(p.margin.final):'Unavailable'} note={p.margin?`Before hedge benefit ${inr(p.margin.initial)}`:'Stated, not guessed'}/>}
    <Kv k="Charges (est.)" v={inr(p.charges)} note="Entry orders"/>
   </View>
   <View style={{gap:6}}>{p.checks.map(c=><View key={c.key} style={[s.row,{gap:8,alignItems:'flex-start'}]}>
    <Icon name={c.status==='pass'?'check-circle':c.status==='warn'?'alert-triangle':'x-octagon'} size={14} color={c.status==='pass'?C.green:c.status==='warn'?C.amber:C.red}/>
    <View style={{flex:1}}><T style={{fontSize:12}}>{c.label}</T><T style={{fontSize:11,color:C.muted}}>{c.detail}</T></View></View>)}</View>
   {p.requires_ack&&<Checkbox checked={ack} onChange={setAck} tone={C.red} label="I understand this structure has unlimited loss" detail="Even on paper, this is how an unhedged short behaves."/>}
   <View style={{backgroundColor:C.paper,borderRadius:10,padding:12,gap:4}}>
    <T style={{fontSize:12}}>Fill rule (paper): a BUY fills at the ask once the ask is at or below your limit; a SELL fills at the bid once the bid is at or above your limit. Resting orders are re-checked against live quotes every few seconds while the market is open.</T>
    <T style={{fontSize:11,color:C.muted}}>Pressing Place sends this exact plan once. Pressing it again cannot create a second set of orders.</T>
   </View>
  </>}
 </Sheet>;
}
function Kv({k,v,note}:{k:string;v:string;note?:string}){return <View style={{gap:1,minWidth:140}}><T style={{fontSize:11,color:C.muted}}>{k}</T><T style={{fontSize:15,fontFamily:'InterSemi'}}>{v}</T>{note&&<T style={{fontSize:10,color:C.muted}}>{note}</T>}</View>;}

/** K12 Monitor for one paper deployment: positions marked at liquidation prices, orders and their states, actions. */
export function DeploymentCard({d,onChanged,onClose}:{d:Deployment;onChanged:()=>void;onClose:(d:Deployment)=>void}){
 const [busy,setBusy]=useState(false);const [open,setOpen]=useState(d.status!=='closed'&&d.status!=='cancelled');
 const resting=d.intents.filter(i=>['created','acknowledged','partially_filled'].includes(i.state)).length;
 const tone=d.status==='active'?'green':d.status==='closed'||d.status==='cancelled'?'neutral':'amber';
 return <View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:12,padding:12,gap:8}}>
  <Pressable onPress={()=>setOpen(!open)} accessibilityRole="button" style={[s.between,{flexWrap:'wrap'}]}>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><Badge label={`PAPER · ${d.status.replace('_',' ').toUpperCase()}`} tone={tone}/>
    {d.strategy_name&&<T style={{fontFamily:'InterSemi'}}>{d.strategy_name}</T>}
    <T style={{fontSize:12,color:C.muted}}>{`${d.product} · opened ${istEpoch(d.opened_at)}`}</T></View>
   <T style={{fontSize:14,fontFamily:'InterSemi',color:(d.net||0)>=0?C.green:C.red}}>{d.net==null?'Net —':`Net ${signed(d.net)}`}</T>
  </Pressable>
  {open&&<>
   {d.positions.length>0&&<View>{[['Position','Units','Avg','Mark','Unrealised','Realised'],...d.positions.map(x=>[x.label,String(x.units),num(x.avg),num(x.mark),signed(x.unrealised),signed(x.realised)])]
    .map((r,i)=><View key={i} style={[s.row,{gap:6,paddingVertical:4,borderTopWidth:i?1:0,borderColor:C.line}]}>{r.map((c,j)=><T key={j} style={{flex:j?1:1.6,fontSize:i?12:10,color:i?C.ink:C.muted,textAlign:j?'right':'left',fontVariant:['tabular-nums'] as any}}>{c}</T>)}</View>)}</View>}
   <T style={{fontSize:11,color:C.muted}}>{`Marked ${istStamp(d.marked_at)} · ${d.mark_basis} · fees ${inr(d.fees)}`}</T>
   <View style={{gap:3}}>{d.intents.map(i=><T key={i.id} style={{fontSize:11,color:i.state==='filled'?C.muted:i.state==='cancelled'?C.red:C.amber}}>
    {`${i.kind==='close'?'close':'open'} · g${i.grp} · ${i.side==='B'?'BUY':'SELL'} ${i.qty} ${i.symbol} @ ${num(i.limit_price)} limit · ${i.state}${i.avg_price?` @ ${num(i.avg_price)}`:''}`}</T>)}</View>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
    {resting>0&&<Button label={`Cancel ${resting} resting`} kind="outline" icon="x" loading={busy} onPress={async()=>{setBusy(true);try{await exec.cancel(d.id);onChanged();}finally{setBusy(false);}}}/>}
    {(d.status==='active'||d.status==='attention_required')&&<Button label="Close position" kind="outline" icon="log-out" onPress={()=>onClose(d)}/>}
   </View>
  </>}
 </View>;
}
