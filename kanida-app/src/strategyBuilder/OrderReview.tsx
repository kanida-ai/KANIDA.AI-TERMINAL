// K11 Order review (slice 4). The preview IS the plan: exact contracts, sides, quantities, limits, product, sequence,
// slices, the quotes it came from, the checks, exchange margin and charges - with a hash and a 30 s expiry. Placing
// sends that exact plan, once (idempotency key), to the PAPER broker - or (slice 7) hands it to engine AutoTrade,
// dry run by default; live only when AutoTrade's own gates pass and the user confirms.
import React,{useCallback,useEffect,useRef,useState} from 'react';
import {Pressable,TextInput,View,useWindowDimensions} from 'react-native';
import {Badge,Button,C,Checkbox,Chip,Icon,Loading,Sheet,T,s} from '../ui';
import {exec,type AutotradeRoute,type Deployment,type Preview} from './api';
import {inr,istEpoch,istStamp,num,signed,strikeText} from './format';

const key=()=>'k'+Date.now().toString(36)+Math.random().toString(36).slice(2,8);

export function OrderReview({visible,onClose,strategyId,deployment,adjusting,onPlaced,draft}:{visible:boolean;onClose:()=>void;strategyId:string;
 deployment?:Deployment|null;adjusting?:Deployment|null;onPlaced:(d:Deployment)=>void;draft?:{expected_version:number;input_hash?:string}}){
 const closing=!!deployment;const adj=!closing&&!!adjusting;const {width}=useWindowDimensions();const narrow=width<600;
 const [p,setP]=useState<Preview|null>(null);const [error,setError]=useState('');const [busy,setBusy]=useState(false);
 const [product,setProduct]=useState('NRML');const [policy,setPolicy]=useState('marketable');const [limits,setLimits]=useState<Record<string,string>>({});const [committed,setCommitted]=useState<Record<string,string>>({});
 const [ack,setAck]=useState(false);const [left,setLeft]=useState(0);const idem=useRef(key());
 const load=useCallback(async()=>{setError('');setP(null);
  try{const lim=Object.fromEntries(Object.entries(committed).filter(([,v])=>v.trim()!=='').map(([k,v])=>[k,Number(v)]));
   const r=closing?await exec.closePreview(deployment!.id,policy):adj?await exec.adjustPreview(adjusting!.id,policy,lim):await exec.preview(strategyId,{product,price_policy:policy,limits:lim,...(draft||{})});
   setP(r);idem.current=key();}
  catch(e:any){setError(e.message);}},[closing,adj,adjusting,deployment,strategyId,product,policy,committed]);
 useEffect(()=>{if(visible)load();},[visible,product,policy,committed]);// eslint-disable-line react-hooks/exhaustive-deps
 useEffect(()=>{if(!visible){setLimits({});setCommitted({});}},[visible]);
 useEffect(()=>{if(!p)return;const t=setInterval(()=>setLeft(Math.max(0,Math.round(p.expires_at-Date.now()/1000))),500);return()=>clearInterval(t);},[p]);
 const expired=!!p&&left<=0;
 async function place(){if(!p)return;setBusy(true);setError('');
  try{const d=closing?await exec.close(deployment!.id,p,idem.current):adj?await exec.adjust(adjusting!.id,p,idem.current,ack):await exec.deploy(strategyId,p,idem.current,ack);onPlaced(d);}
  catch(e:any){setError(e.message);if(/expired|changed/i.test(e.message))load();}finally{setBusy(false);}}
 const n=p?.orders.reduce((a,o)=>a+o.slices.length,0)||0;
 // a typed limit that has not been priced into a fresh preview yet can never be placed on the older plan
 const uncommitted=!!p&&Object.entries(limits).some(([k,v])=>v!==(committed[k]??String(p.orders.find(o=>o.leg_id===k)?.limit)));
 const blocks=p?p.checks.filter(c=>c.status==='block'):[];
 const limitInput=(o:Preview['orders'][number],w:number,h:number)=><TextInput value={limits[o.leg_id]??String(o.limit)} onChangeText={v=>setLimits(l=>({...l,[o.leg_id]:v}))}
  onBlur={()=>setCommitted(c=>limits[o.leg_id]!=null&&limits[o.leg_id]!==c[o.leg_id]?{...c,[o.leg_id]:limits[o.leg_id]}:c)}
  keyboardType="decimal-pad" accessibilityLabel={`Limit price for ${o.symbol}`} style={{width:w,height:h,borderWidth:1,borderColor:C.line,borderRadius:7,paddingHorizontal:8,color:C.ink,fontFamily:'InterSemi',fontSize:13,textAlign:'right'}}/>;
 return <Sheet visible={visible} onClose={onClose} wide title={closing?'Close paper deployment':adj?'Review adjustment orders':'Review paper orders'}
  subtitle={p?`${p.structure} · quotes ${istStamp(p.as_of)} · spot ${num(p.spot,2)} · PAPER - no order reaches a broker`:'Building the exact order plan from live quotes…'}
  footer={<View style={[s.between,{flexWrap:'wrap',gap:10}]}>
   <View style={{gap:2,flex:1,minWidth:220}}>{p&&<T style={{fontSize:12,color:expired||uncommitted?C.red:C.muted}}>{uncommitted?'You edited a limit - it is priced into a new plan when you leave the field or press Refresh.':expired?'Preview expired - quotes move. Refresh to review again (your choices stay).':`This plan is valid for ${left}s · hash ${p.hash.slice(0,10)}`}</T>}</View>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
    <Button label="Refresh" icon="refresh-cw" kind="outline" onPress={load}/>
    <Button label={closing?`Place ${n} close order${n===1?'':'s'} (paper)`:adj?`Place ${n} adjustment order${n===1?'':'s'} (paper)`:`Place ${n} paper order${n===1?'':'s'}`} icon="check" loading={busy}
     disabled={!p||!p.can_submit||expired||uncommitted||(p.requires_ack&&!ack)} onPress={place}/>
   </View></View>}>
  {!closing&&!adj&&<View style={[s.row,{flexWrap:'wrap',gap:8}]}>
   <T style={{fontSize:11,color:C.muted}}>Product</T>{['NRML','MIS'].map(x=><Chip key={x} label={x==='NRML'?'NRML (carry)':'MIS (intraday)'} active={product===x} onPress={()=>setProduct(x)}/>)}
   <T style={{fontSize:11,color:C.muted,marginLeft:8}}>Limit price</T>{[['marketable','At the quote (ask/bid)'],['mid','At the mid']].map(([k,l])=><Chip key={k} label={l} active={policy===k} onPress={()=>setPolicy(k)}/>)}
  </View>}
  {(closing||adj)&&<View style={[s.row,{gap:8}]}>{[['marketable','At the quote'],['mid','At the mid']].map(([k,l])=><Chip key={k} label={l} active={policy===k} onPress={()=>setPolicy(k)}/>)}</View>}
  {!!error&&<View style={{backgroundColor:'#2A1519',borderRadius:10,padding:12}}><T style={{color:C.red,fontSize:13}}>{error}</T></View>}
  {!p&&!error&&<Loading/>}
  {p&&<>
   {narrow&&blocks.length>0&&<View accessibilityRole="alert" style={{backgroundColor:'#2A1519',borderRadius:10,padding:10,gap:4}}>
    <T style={{fontSize:12,color:C.red,fontFamily:'InterSemi'}}>{`Cannot place yet - ${blocks.length} check${blocks.length>1?'s':''} block these orders`}</T>
    {blocks.map(c=><T key={c.key} style={{fontSize:11,color:C.red}}>{`${c.label}: ${c.detail}`}</T>)}</View>}
   {narrow?<View style={{gap:8}}>{p.orders.map(o=><View key={o.leg_id} style={{borderWidth:1,borderColor:C.line,borderRadius:12,padding:12,gap:8}}>
     <View style={[s.between,{gap:8}]}><T style={{fontSize:14,fontFamily:'InterSemi',color:o.side==='B'?C.green:C.red,flex:1}}>{`${o.side==='B'?'BUY':'SELL'} ${strikeText(o.strike)} ${o.type}`}</T>
      <Badge label={`Group ${o.group}`} tone="neutral"/></View>
     <T style={{fontSize:11,color:C.muted}} selectable>{`${o.symbol} · LIMIT · ${p.product}`}</T>
     <View style={[s.row,{flexWrap:'wrap',gap:14}]}>
      <Kv k="Quantity" v={`${o.qty}`} note={`${o.lots} lot${o.lots>1?'s':''} × ${o.lot_size}`}/><Kv k="Bid / Ask" v={`${num(o.bid)} / ${num(o.ask)}`}/>
      <Kv k="Slices" v={o.slices.length>1?o.slices.join(' + '):'1'}/><Kv k="Charges" v={inr(o.charges)}/></View>
     <View style={[s.row,{gap:10,alignItems:'center'}]}><T style={{fontSize:12,color:C.muted}}>Limit price</T>
      {closing?<T style={{fontSize:14,fontFamily:'InterSemi'}}>{num(o.limit)}</T>:limitInput(o,120,44)}</View>
    </View>)}</View>:
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
      limitInput(o,84,32)}</View>
     <T style={{flex:1,fontSize:12,textAlign:'right',color:o.slices.length>1?C.amber:C.ink}}>{o.slices.length>1?o.slices.join(' + '):'1'}</T>
     <T style={{flex:.9,fontSize:12,textAlign:'right'}}>{inr(o.charges)}</T>
    </View>)}
   </View>}
   <T style={{fontSize:12,color:C.muted}}>{p.kind==='close'?'Sequence: buy back shorts first; sell longs only after every buy-back has filled.':p.kind==='adjust'?`Only the delta orders that turn what this deployment holds into the new version. ${p.sequence}`:`Sequence: ${p.sequence} Group 1 is sent first.`}</T>
   <View style={[s.row,{flexWrap:'wrap',gap:18}]}>
    <Kv k={p.net_premium>=0?'Net credit':'Net debit'} v={inr(Math.abs(p.net_premium))}/>
    {p.kind==='open'&&<Kv k="Exchange margin (Kite)" v={p.margin?inr(p.margin.final):'Unavailable'} note={p.margin?`Before hedge benefit ${inr(p.margin.initial)}`:'Stated, not guessed'}/>}
    <Kv k="Charges (est.)" v={inr(p.charges)} note="Entry orders"/>
   </View>
   <View style={{gap:6}}>{p.checks.map(c=><View key={c.key} style={[s.row,{gap:8,alignItems:'flex-start'}]}>
    <Icon name={c.status==='pass'?'check-circle':c.status==='warn'?'alert-triangle':'x-octagon'} size={14} color={c.status==='pass'?C.green:c.status==='warn'?C.amber:C.red}/>
    <View style={{flex:1}}><T style={{fontSize:12}}>{c.label}</T><T style={{fontSize:11,color:C.muted}}>{c.detail}</T></View></View>)}</View>
   {p.requires_ack&&<Checkbox checked={ack} onChange={setAck} tone={C.red} label="I understand this structure has unlimited loss" detail="Even on paper, this is how an unhedged short behaves."/>}
   {!closing&&!adj&&<AutotradePanel p={p} strategyId={strategyId} expired={expired}/>}
   <View style={{backgroundColor:C.paper,borderRadius:10,padding:12,gap:4}}>
    <T style={{fontSize:12}}>Fill rule (paper): a BUY fills at the ask once the ask is at or below your limit; a SELL fills at the bid once the bid is at or above your limit. Resting orders are re-checked against live quotes every few seconds while the market is open.</T>
    <T style={{fontSize:11,color:C.muted}}>Pressing Place sends this exact plan once. Pressing it again cannot create a second set of orders.</T>
   </View>
  </>}
 </Sheet>;
}
const FINAL=['completed','dry_run_complete','blocked','failed','cancelled','attention_required','refused','not_received'];
const ROUTE_TONE:Record<string,any>={completed:'green',dry_run_complete:'neutral',accepted:'amber',dispatching:'amber',sending:'amber',blocked:'red',refused:'red',failed:'red',attention_required:'red',cancelled:'neutral',unknown:'red',not_received:'neutral'};

/** Slice 7: hand this exact plan to engine AutoTrade. Dry run by default; live only when AutoTrade reports every gate
 *  passing (including an operator arm this app cannot set) AND the user confirms real orders. AutoTrade decides. */
function AutotradePanel({p,strategyId,expired}:{p:Preview;strategyId:string;expired:boolean}){
 const cap=p.live;const [route,setRoute]=useState<AutotradeRoute|null>(null);const [rel,setRel]=useState(false);const [busy,setBusy]=useState('');const [err,setErr]=useState('');const [sure,setSure]=useState(false);
 const idem=useRef(key());
 useEffect(()=>{setRoute(null);setSure(false);idem.current=key();},[p.id]);
 useEffect(()=>{if(!route||FINAL.includes(route.state))return;const t=setInterval(async()=>{try{setRoute(await exec.autotradeGet(route.id));}catch{}},2000);return()=>clearInterval(t);},[route]);
 async function send(mode:'dry_run'|'live'){setBusy(mode);setErr('');
  try{setRoute(await exec.autotrade(strategyId,p,idem.current+mode,mode,mode==='live'&&sure));}catch(e:any){setErr(e.message);}finally{setBusy('');}}
 const ready=!!cap.enabled&&p.can_submit&&!expired&&!p.requires_ack;
 return <View style={{borderWidth:1,borderColor:C.line,borderRadius:12,padding:12,gap:8}}>
  <View style={[s.between,{flexWrap:'wrap',gap:6}]}><T style={{fontFamily:'InterSemi',fontSize:13}}>AutoTrade hand-off</T>
   <Badge label={!cap.configured?'NOT CONNECTED':!cap.reachable?'UNREACHABLE':cap.live_allowed?'LIVE POSSIBLE':'DRY RUN ONLY'} tone={cap.live_allowed?'red':cap.enabled?'neutral':'amber'}/></View>
  <T style={{fontSize:11,color:C.muted}}>{cap.reason}</T>
  {!!cap.gates?.length&&<View style={{gap:4}}>{cap.gates.map(g=><View key={g.gate} style={[s.row,{gap:8,alignItems:'flex-start'}]}>
   <Icon name={g.deferred?'clock':g.pass?'check-circle':'x-circle'} size={13} color={g.deferred?C.muted:g.pass?C.green:C.muted}/>
   <View style={{flex:1}}><T style={{fontSize:12}}>{g.label}</T><T style={{fontSize:10,color:C.muted}}>{g.detail}</T></View></View>)}</View>}
  {cap.configured&&<T style={{fontSize:10,color:C.muted}}>{`AutoTrade identity ${cap.engine_user}${cap.broker_account_id?` · account ${cap.broker_account_id}`:''}. Only the operator can arm it, outside this app.`}</T>}
  {cap.live_allowed&&<Checkbox checked={sure} onChange={setSure} tone={C.red} label="I confirm AutoTrade may place REAL orders on my broker account for this exact plan" detail="Hedges are bought first; sells go only after every hedge fills. AutoTrade can still refuse."/>}
  <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
   <Button label="Send dry run to AutoTrade" icon="send" kind="outline" loading={busy==='dry_run'} disabled={!ready||!!route} onPress={()=>send('dry_run')}/>
   <Button label="Send live" icon={cap.live_allowed?'zap':'lock'} kind="outline" loading={busy==='live'} disabled={!ready||!cap.live_allowed||!sure||!!route} accessibilityHint={cap.reason} onPress={()=>send('live')}/>
  </View>
  {!!err&&<T style={{fontSize:12,color:C.red}}>{err}</T>}
  {route&&<View style={{backgroundColor:C.paper,borderRadius:10,padding:10,gap:4}}>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><Badge label={`${route.mode==='live'?'LIVE':'DRY RUN'} · ${route.state.replace(/_/g,' ').toUpperCase()}`} tone={ROUTE_TONE[route.state]||'amber'}/>
    {!FINAL.includes(route.state)&&<T style={{fontSize:11,color:C.muted}}>{route.state==='unknown'?'Outcome unknown - reconciling with AutoTrade by its key. Nothing will be re-sent.':'Following AutoTrade…'}</T>}</View>
   {route.state==='unknown'&&<T accessibilityRole="alert" style={{fontSize:12,color:C.red}}>The request left this app but no answer came back, so it may or may not have been accepted. No new orders for this strategy until it resolves.</T>}
   {route.state==='unknown'&&<Checkbox checked={rel} onChange={setRel} tone={C.red} label="I checked AutoTrade and this hand-off created no orders"
    detail="Only release it after checking AutoTrade yourself. This app cannot confirm it; releasing lets a new hand-off of this strategy go out."/>}
   {route.state==='unknown'&&<Button label="Release this hand-off" kind="outline" disabled={!rel} onPress={async()=>{try{setRoute(await exec.autotradeRelease(route.id));}catch(e:any){setErr(e.message);}}}/>}
   {!!route.reason&&<T style={{fontSize:11,color:C.muted}}>{route.reason}</T>}
   {route.intent?.legs.map((l,i)=><T key={i} style={{fontSize:11,color:l.state==='filled'||l.state==='dry_run'?C.muted:C.amber}}>
    {`g${l.group} · ${l.side} ${l.quantity} ${l.tradingsymbol} @ ${num(l.limit_price)} · ${l.state.replace(/_/g,' ')}${l.avg_price?` @ ${num(l.avg_price)}`:''}${l.error?` · ${l.error}`:''}`}</T>)}
   {route.mode==='dry_run'&&FINAL.includes(route.state)&&<T style={{fontSize:10,color:C.muted}}>A dry run walks the same group order through AutoTrade's broker adapter without sending any order.</T>}
   {!FINAL.includes(route.state)&&<Button label="Stop" kind="outline" icon="x" onPress={async()=>{try{setRoute(await exec.autotradeCancel(route.id));}catch(e:any){setErr(e.message);}}}/>}
  </View>}
 </View>;
}
function Kv({k,v,note}:{k:string;v:string;note?:string}){return <View style={{gap:1,minWidth:140}}><T style={{fontSize:11,color:C.muted}}>{k}</T><T style={{fontSize:15,fontFamily:'InterSemi'}}>{v}</T>{note&&<T style={{fontSize:10,color:C.muted}}>{note}</T>}</View>;}

/** K12 Monitor for one paper deployment: positions marked at liquidation prices (or labelled LTP when no valid book), orders and their states, actions. */
export function DeploymentCard({d,onChanged,onClose,onAdjust}:{d:Deployment;onChanged:()=>void;onClose:(d:Deployment)=>void;onAdjust?:(d:Deployment)=>void}){
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
   {d.positions.length>0&&<View>{[['Position','Units','Avg','Mark','Unrealised','Realised'],...d.positions.map(x=>[x.label,String(x.units),num(x.avg),`${num(x.mark)}${(x as any).mark_basis==='ltp'?' LTP':(x as any).mark_basis?` ${(x as any).mark_basis}`:''}`,signed(x.unrealised),signed(x.realised)])]
    .map((r,i)=><View key={i} style={[s.row,{gap:6,paddingVertical:4,borderTopWidth:i?1:0,borderColor:C.line}]}>{r.map((c,j)=><T key={j} style={{flex:j?1:1.6,fontSize:i?12:10,color:i?C.ink:C.muted,textAlign:j?'right':'left',fontVariant:['tabular-nums'] as any}}>{c}</T>)}</View>)}</View>}
   <T style={{fontSize:11,color:C.muted}}>{`Marked ${istStamp(d.marked_at)} · ${d.mark_basis} · fees ${inr(d.fees)}`}</T>
   <View style={{gap:3}}>{d.intents.map(i=><T key={i.id} style={{fontSize:11,color:i.state==='filled'?C.muted:i.state==='cancelled'?C.red:C.amber}}>
    {`${i.kind==='close'?'close':'open'} · g${i.grp} · ${i.side==='B'?'BUY':'SELL'} ${i.qty} ${i.symbol} @ ${num(i.limit_price)} limit · ${i.state}${i.avg_price?` @ ${num(i.avg_price)}`:''}`}</T>)}</View>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
    {resting>0&&<Button label={`Cancel ${resting} resting`} kind="outline" icon="x" loading={busy} onPress={async()=>{setBusy(true);try{await exec.cancel(d.id);onChanged();}finally{setBusy(false);}}}/>}
    {d.status==='active'&&onAdjust&&<Button label="Adjust" kind="outline" icon="sliders" onPress={()=>onAdjust(d)}/>}
    {(d.status==='active'||d.status==='attention_required')&&<Button label="Close position" kind="outline" icon="log-out" onPress={()=>onClose(d)}/>}
   </View>
  </>}
 </View>;
}
