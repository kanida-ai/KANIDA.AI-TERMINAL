// Builder sheets added in slice 11 (GTM audit P05, P12, P18): the one-leg editor, the actions menu, the expiry picker
// with a remap preview, and the read-only snapshot view with its name and notes.
import React,{useEffect,useState} from 'react';
import {Pressable,TextInput,View} from 'react-native';
import {Badge,Button,C,Checkbox,Chip,Icon,Sheet,T,s} from '../ui';
import {sb,type Basis,type Chain,type Expiry,type Leg} from './api';
import {legText,dayMonth,istEpoch,istStamp,num,signed,strikeText} from './format';
import {LoadState} from './States';

/** Tick before calculate (Blueprint A): a typed price snaps to the contract's tick; the server rejects anything else. */
export const snap=(v:number,tick?:number|null)=>{const t=tick||0.05;return Math.round(Math.round(v/t)*t*100)/100;};

const BASES:[Basis,string][]=[['exec','Buy at ask / sell at bid'],['mid','Mid'],['ltp','Last traded'],['manual','Type a price']];

/** One leg, edited on its own sheet (phones): nothing changes until Apply; Cancel leaves the strategy untouched. */
export function LegSheet({leg,chain,moveStrike,onApply,onClose,onRemove,expiries=[]}:{leg:Leg|null;chain:Chain|null;moveStrike:(k:number,n:number)=>number;
 onApply:(l:Leg)=>void;onClose:()=>void;onRemove:(id:string)=>void;expiries?:Expiry[]}){
 const [d,setD]=useState<Leg|null>(leg);const [price,setPrice]=useState('');const [err,setErr]=useState('');const [snapped,setSnapped]=useState('');
 useEffect(()=>{setD(leg);setPrice(leg?.price_basis==='manual'&&leg.price!=null?String(leg.price):'');setErr('');setSnapped('');},[leg]);
 // the contract's details come from ITS expiry's chain (fresh audit P01): after an expiry change the old contract's
 // symbol, LTP and lot are never shown - it reads "Loading" until the selected expiry's chain arrives
 const [other,setOther]=useState<{expiry:string;chain:Chain|null;err?:string}|null>(null);
 const exp=d?.expiry||chain?.expiry||'';
 useEffect(()=>{if(!chain||!exp||exp===chain.expiry){setOther(null);return;}
  const ac=new AbortController();setOther({expiry:exp,chain:null});
  sb.chain(chain.underlying,exp,ac.signal).then(c=>setOther({expiry:exp,chain:c})).catch(e=>{if(!ac.signal.aborted)setOther({expiry:exp,chain:null,err:e.message||'unavailable'});});
  return()=>ac.abort();},[chain,exp]);
 if(!leg||!d)return null;
 const ch=!chain||exp===chain.expiry?chain:(other?.expiry===exp?other.chain:null);
 const pending=!ch&&!!chain&&exp!==chain.expiry&&!other?.err;
 const q=ch?.rows.find(r=>r.strike===d.strike)?.[d.type];
 const missing=!!ch&&!q;
 const symbol=q?.symbol||`${chain?.underlying||''} ${dayMonth(d.expiry)} ${strikeText(d.strike)} ${d.type}`;
 const lot=ch?.lot_size;
 function apply(){
  if(pending){setErr('The selected expiry is still loading.');return;}
  if(other?.err&&exp!==chain?.expiry){setErr(`The ${dayMonth(exp)} chain could not be read (${other.err}).`);return;}
  if(missing){setErr(`${strikeText(d!.strike)} ${d!.type} is not listed for ${dayMonth(exp)}. Choose a listed strike or another expiry.`);return;}
  let next={...d!};
  if(next.price_basis==='manual'){const v=Number(price);if(!price.trim()||!Number.isFinite(v)||v<0){setErr('Type a price of 0 or more, or choose another price basis.');return;}next={...next,price:snap(v,ch?.tick_size)};}
  else next={...next,price:null};
  onApply(next);}
 const big={minHeight:44};
 return <Sheet visible onClose={onClose} title="Edit leg" subtitle={pending?`Loading the ${dayMonth(exp)} contract…`:`${symbol} · ${d.lots} lot${d.lots>1?'s':''} × ${lot||'?'} = ${lot?d.lots*lot:'?'} units`}
  footer={<View style={[s.between,{gap:8,flexWrap:'wrap'}]}><Button label="Remove leg" icon="trash-2" kind="outline" onPress={()=>{onRemove(d.id);onClose();}}/>
   <View style={[s.row,{gap:8}]}><Button label="Cancel" kind="outline" onPress={onClose}/><Button label="Apply" icon="check" onPress={apply}/></View></View>}>
  <View style={{gap:14}}>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><T style={{fontSize:12,color:C.muted,width:60}}>Side</T>
    {(['B','S'] as const).map(x=><Chip key={x} label={x==='B'?'Buy':'Sell'} active={d.side===x} onPress={()=>setD({...d,side:x})}/>)}</View>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><T style={{fontSize:12,color:C.muted,width:60}}>Type</T>
    {(['CE','PE'] as const).map(x=><Chip key={x} label={x==='CE'?'Call (CE)':'Put (PE)'} active={d.type===x} onPress={()=>setD({...d,type:x})}/>)}</View>
   <View style={[s.row,{gap:8,alignItems:'center'}]}><T style={{fontSize:12,color:C.muted,width:60}}>Strike</T>
    <Button label="−" accessibilityLabel="Lower strike" kind="outline" onPress={()=>setD({...d,strike:moveStrike(d.strike,-1)})}/>
    <T style={{fontFamily:'InterSemi',fontSize:16,minWidth:70,textAlign:'center'}}>{strikeText(d.strike)}</T>
    <Button label="+" accessibilityLabel="Higher strike" kind="outline" onPress={()=>setD({...d,strike:moveStrike(d.strike,1)})}/></View>
   {expiries.length>1&&<View style={{gap:6}}><T style={{fontSize:12,color:C.muted}}>Expiry (a leg in another expiry makes a calendar or diagonal)</T>
    <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>{expiries.slice(0,8).map(x=><Chip key={x.expiry} label={`${dayMonth(x.expiry)} · ${Math.max(0,Math.round(x.days_to_expiry))}d`} active={d.expiry===x.expiry} onPress={()=>setD({...d,expiry:x.expiry})}/>)}</View>
    {d.expiry!==leg.expiry&&d.price_basis==='manual'&&<T style={{fontSize:11,color:C.amber}}>{`Your typed entry price (${price||'—'}) is kept for the ${dayMonth(exp)} contract. Change it or choose a market basis if it no longer applies.`}</T>}</View>}
   <View style={[s.row,{gap:8,alignItems:'center'}]}><T style={{fontSize:12,color:C.muted,width:60}}>Lots</T>
    <Button label="−" accessibilityLabel="Fewer lots" kind="outline" onPress={()=>setD({...d,lots:Math.max(1,d.lots-1)})}/>
    <T style={{fontFamily:'InterSemi',fontSize:16,minWidth:70,textAlign:'center'}}>{d.lots}</T>
    <Button label="+" accessibilityLabel="More lots" kind="outline" onPress={()=>setD({...d,lots:Math.min(500,d.lots+1)})}/></View>
   <View style={{gap:8}}><T style={{fontSize:12,color:C.muted}}>Entry price basis</T>
    <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>{BASES.map(([k,l])=><Chip key={k} label={l} active={d.price_basis===k} onPress={()=>setD({...d,price_basis:k})}/>)}</View>
    {d.price_basis==='manual'&&<TextInput value={price} onChangeText={v=>{setPrice(v);setErr('');setSnapped('');}} keyboardType="decimal-pad" accessibilityLabel="Manual entry price"
     onBlur={()=>{const v=Number(price);if(price.trim()&&Number.isFinite(v)&&v>=0){const t=snap(v,ch?.tick_size);if(t!==v){setPrice(String(t));setSnapped(`Rounded from ${price} to the ${ch?.tick_size||0.05} tick.`);}}}}
     style={{...big,borderWidth:1,borderColor:err?C.red:C.line,borderRadius:10,paddingHorizontal:12,color:C.ink,fontFamily:'InterSemi',fontSize:15,backgroundColor:C.paper}}/>}
    {!!snapped&&<T accessibilityLiveRegion="polite" style={{color:C.amber,fontSize:12}}>{snapped}</T>}
    {!!err&&<T accessibilityRole="alert" style={{color:C.red,fontSize:12}}>{err}</T>}
    <T style={{fontSize:11,color:missing?C.red:C.muted}}>{pending?`Loading the ${dayMonth(exp)} quote…`:other?.err&&exp!==chain?.expiry?`The ${dayMonth(exp)} chain could not be read.`:q?`${dayMonth(exp)} · Bid ${num(q.bid)} · Ask ${num(q.ask)} · LTP ${num(q.ltp)}${ch?.quality.live?'':' (stored reading - no bid/ask)'}`:`${strikeText(d.strike)} ${d.type} is not listed for ${dayMonth(exp)}.`}</T></View>
   <Checkbox checked={d.include} onChange={v=>setD({...d,include:v})} label="Include in the analysis" detail="An excluded leg stays in the strategy but is left out of every number."/>
  </View>
 </Sheet>;
}

/** Secondary builder actions on a phone, so the header stays short (GTM audit P12). */
export function ActionsSheet({visible,onClose,actions}:{visible:boolean;onClose:()=>void;actions:{label:string;icon:string;onPress:()=>void;disabled?:boolean}[]}){
 return <Sheet visible={visible} onClose={onClose} title="Actions">
  <View style={{gap:8}}>{actions.map(a=><Pressable key={a.label} accessibilityRole="button" accessibilityState={{disabled:!!a.disabled}} disabled={a.disabled}
   onPress={()=>{onClose();a.onPress();}} style={({pressed})=>[s.row,{gap:12,minHeight:48,paddingHorizontal:14,borderRadius:10,borderWidth:1,borderColor:C.line,opacity:a.disabled?.4:pressed?.7:1}]}>
   <Icon name={a.icon} size={16} color={C.green}/><T style={{fontSize:14}}>{a.label}</T></Pressable>)}</View>
 </Sheet>;
}

/** Every listed expiry; with legs, a remap preview - Apply only when every leg resolves in the new expiry (GTM P05). */
export function ExpirySheet({visible,onClose,expiries,current,underlying,legs,onApply}:{visible:boolean;onClose:()=>void;expiries:Expiry[];current:string;
 underlying:string;legs:Leg[];onApply:(expiry:string)=>void}){
 const [pick,setPick]=useState('');const [check,setCheck]=useState<{ok:string[];missing:string[]}|null>(null);const [err,setErr]=useState('');const [busy,setBusy]=useState(false);
 useEffect(()=>{if(!visible){setPick('');setCheck(null);setErr('');}},[visible]);
 const seq=React.useRef(0);
 async function choose(e:string){const mine=++seq.current;setPick(e);setCheck(null);setErr('');
  if(!legs.length){return;}
  setBusy(true);
  try{const ch=await sb.chain(underlying,e);if(mine!==seq.current)return;       // a later pick won: this answer is for another expiry
   const ok:string[]=[],missing:string[]=[];
   for(const l of legs){const q=ch.rows.find(r=>r.strike===l.strike)?.[l.type];(q&&q.ltp!=null?ok:missing).push(`${l.side==='B'?'Buy':'Sell'} ${strikeText(l.strike)} ${l.type}`);}
   setCheck({ok,missing,for:e} as any);}
  catch(x:any){if(mine===seq.current)setErr(x.message);}finally{if(mine===seq.current)setBusy(false);}}
 // a calendar/diagonal must never be flattened by moving every leg to one expiry (review M5)
 const multi=new Set(legs.map(l=>l.expiry)).size>1;
 const canApply=!multi&&!!pick&&pick!==current&&(!legs.length||(!!check&&(check as any).for===pick&&!check.missing.length));
 return <Sheet visible={visible} onClose={onClose} title="Choose an expiry" subtitle={legs.length?'Every leg moves to the new expiry at the same strike. Nothing changes until you apply, and only if every leg is listed there.':'Pick any listed expiry.'}
  footer={<View style={[s.row,{justifyContent:'flex-end',gap:8}]}><Button label="Cancel" kind="outline" onPress={onClose}/>
   <Button label={pick?`Move to ${dayMonth(pick)}`:'Move'} icon="check" disabled={!canApply} loading={busy} onPress={()=>{onApply(pick);onClose();}}/></View>}>
  <View style={[s.row,{flexWrap:'wrap',gap:8}]}>{expiries.map(x=><Chip key={x.expiry} active={(pick||current)===x.expiry}
   label={`${dayMonth(x.expiry)} ${x.monthly?'M':'W'} · ${Math.max(0,Math.round(x.days_to_expiry))}d${x.expiry===current?' (now)':''}`} onPress={()=>choose(x.expiry)}/>)}</View>
  {multi&&<T accessibilityRole="alert" style={{color:C.amber,fontSize:12}}>This strategy spans expiries (a calendar or diagonal). Moving every leg to one expiry would collapse it - change each leg's expiry in its own editor.</T>}
  {!!err&&<T accessibilityRole="alert" style={{color:C.red,fontSize:12}}>{err}</T>}
  {check&&<View style={{gap:4}}>
   {check.ok.map(x=><T key={x} style={{fontSize:12,color:C.green}}>{`✓ ${x} is listed`}</T>)}
   {check.missing.map(x=><T key={x} style={{fontSize:12,color:C.red}}>{`✗ ${x} is not listed (or has no price) in ${dayMonth(pick)}`}</T>)}
   {!!check.missing.length&&<T style={{fontSize:12,color:C.amber}}>Not applied: your legs stay as they are. Adjust the strikes first, or pick another expiry.</T>}
  </View>}
 </Sheet>;
}

/** A snapshot, read-only: its exact terms and the analysis frozen when it was saved - never re-priced (GTM P18). */
export function SnapshotSheet({rid,onClose,onSaved}:{rid:string|null;onClose:()=>void;onSaved:()=>void}){
 const [rev,setRev]=useState<any>(null);const [err,setErr]=useState('');const [name,setName]=useState('');const [notes,setNotes]=useState('');const [saving,setSaving]=useState(false);const [saveErr,setSaveErr]=useState('');
 const load=()=>{if(!rid)return;setErr('');setRev(null);sb.revision(rid).then(r=>{setRev(r);setName(r.name);setNotes(r.notes||'');}).catch(e=>setErr(e.message));};
 useEffect(load,[rid]);// eslint-disable-line react-hooks/exhaustive-deps
 if(!rid)return null;
 const a=rev?.analysis||{};const m=(x:any)=>!x?'—':x.unlimited?'Unlimited':x.status==='available'?signed(x.value):'—';
 async function save(){setSaving(true);setSaveErr('');try{const r=await sb.revisionMeta(rid!,{name,notes});setRev(r);onSaved();}catch(e:any){setSaveErr(e.message);}finally{setSaving(false);}}
 return <Sheet visible onClose={onClose} wide title={rev?`Snapshot ${rev.n}`:'Snapshot'} subtitle="Frozen when it was saved. It is never re-priced; Restore makes a new working version.">
  <LoadState loading={!rev&&!err} error={err} onRetry={load} what="The snapshot">
   {rev&&<View style={{gap:14}}>
    <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><Badge label={rev.intact?'Checksum verified':'CHECKSUM MISMATCH'} tone={rev.intact?'green':'red'}/>
     <T style={{fontSize:11,color:C.muted}}>{`Saved ${istEpoch(rev.created_at)} · priced at ${istStamp(rev.reading_at)} · ${a.model_version||'model ?'} · ${rev.checksum.slice(0,12)}`}</T></View>
    <View style={{gap:6}}><T style={{fontSize:12,color:C.muted}}>Name</T>
     <TextInput value={name} onChangeText={setName} maxLength={80} accessibilityLabel="Snapshot name" style={{minHeight:44,borderWidth:1,borderColor:C.line,borderRadius:10,paddingHorizontal:12,color:C.ink,fontFamily:'Inter',fontSize:14,backgroundColor:C.paper}}/>
     <T style={{fontSize:12,color:C.muted}}>Notes / thesis</T>
     <TextInput value={notes} onChangeText={setNotes} multiline maxLength={2000} accessibilityLabel="Snapshot notes" style={{minHeight:80,borderWidth:1,borderColor:C.line,borderRadius:10,padding:12,color:C.ink,fontFamily:'Inter',fontSize:13,backgroundColor:C.paper,textAlignVertical:'top'}}/>
     <View style={[s.row,{gap:8}]}><Button label="Save name and notes" kind="outline" loading={saving} disabled={name===rev.name&&notes===(rev.notes||'')} onPress={save}/>
      <T style={{fontSize:11,color:C.muted,flex:1}}>Renaming or notes never change the terms or the checksum.</T></View>
     {!!saveErr&&<T accessibilityRole="alert" style={{color:C.red,fontSize:12}}>{`Not saved: ${saveErr}. Your text is kept - try again.`}</T>}
    </View>
    <View style={{gap:4}}><T style={{fontFamily:'InterSemi'}}>{`${rev.body.underlying} · expiry ${dayMonth(rev.body.expiry)}`}</T>
     {rev.body.legs.map((l:Leg)=><T key={l.id} style={{fontSize:13,fontVariant:['tabular-nums'] as any}}>{`${l.include?'':'(excluded) '}${legText(l)} · entry ${l.price_basis==='manual'?`${num(l.price)} (manual)`:l.price_basis}`}</T>)}</View>
    <View style={[s.row,{gap:18,flexWrap:'wrap'}]}>
     {[['Max loss',m(a.max_loss)],['Max profit',m(a.max_profit)],['Breakeven',a.breakevens?.value?.map((b:number)=>num(b,0)).join(' · ')||'—'],['Premium',a.premium?signed(a.premium.value):'—'],
      ['POP (model)',a.pop?.status==='available'?`${a.pop.value}%`:'—'],['Spot then',num(a.spot,2)]].map(([k,v])=><View key={k} style={{gap:1}}><T style={{fontSize:11,color:C.muted}}>{k}</T><T style={{fontFamily:'InterSemi'}}>{v}</T></View>)}
    </View>
   </View>}
  </LoadState>
 </Sheet>;
}

/** Indices as chips; F&O stocks through a search (216 names never become a wall of chips). Stocks are physically settled. */
export function UnderlyingPicker({list,value,onPick}:{list:{symbol:string;kind?:string}[];value:string;onPick:(u:string)=>void}){
 const [q,setQ]=useState('');
 const idx=list.filter(x=>x.kind!=='stock');const stocks=list.filter(x=>x.kind==='stock');
 const hits=q.trim()?stocks.filter(x=>x.symbol.includes(q.trim().toUpperCase())).slice(0,12):[];
 const isStock=stocks.some(x=>x.symbol===value);
 return <View style={{gap:8}}>
  <View style={[s.row,{flexWrap:'wrap',gap:8}]}>{idx.map(x=><Chip key={x.symbol} label={x.symbol} active={value===x.symbol} onPress={()=>onPick(x.symbol)}/>)}
   {isStock&&<Chip label={`${value} · stock`} active onPress={()=>{}}/>}</View>
  {stocks.length>0&&<View style={{gap:6}}>
   <TextInput value={q} onChangeText={setQ} placeholder={`Search ${stocks.length} F&O stocks (physically settled)`} placeholderTextColor={C.muted} autoCapitalize="characters"
    accessibilityLabel="Search F&O stocks" style={{minHeight:44,borderWidth:1,borderColor:C.line,borderRadius:10,paddingHorizontal:12,color:C.ink,fontFamily:'Inter',fontSize:13,backgroundColor:C.paper,maxWidth:420}}/>
   {hits.length>0&&<View style={[s.row,{flexWrap:'wrap',gap:6}]}>{hits.map(x=><Chip key={x.symbol} label={x.symbol} active={value===x.symbol} onPress={()=>{setQ('');onPick(x.symbol);}}/>)}</View>}
   {!!q.trim()&&!hits.length&&<T style={{fontSize:11,color:C.muted}}>No F&O stock matches.</T>}
  </View>}
 </View>;
}
