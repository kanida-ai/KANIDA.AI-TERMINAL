// THE CONDITION BUILDER. "Tell KANIDA the market behaviour you want to find."
//
// Two ways in, one definition out: the sentence box is parsed on the server into the SAME structured definition
// the chips edit, and the chips are the definition — so what a sentence was understood as is always visible and
// editable, never a hidden query. Every chip opens a short list of WORDS; there is no numeric input anywhere in
// the default view. The only numbers a user can touch are strike offsets and delta bands, under "More options".
import React,{useState} from 'react';
import {Pressable,TextInput,View} from 'react-native';
import {Badge,Button,C,Checkbox,Icon,Sheet,T,s} from '../ui';
import {READING_TIMES,addCondition,metricOf,removeCondition,sameWindow,strikesLabel,toggleJoin,universeLabel,
 updateCondition,windowLabel,withMetric} from './model';
import type {Condition,Definition,Parsed,Side,Strikes,Vocabulary,Window} from './types';

type Pick={kind:'metric'|'side'|'state'|'window'|'universe'|'expiry'|'strikes';index?:number}|null;
type Option={key:string;label:string;arrow?:string;hint?:string;group?:string;active?:boolean};

const TONE:Record<string,string>={up:C.green,down:C.red,warn:C.amber,flat:C.muted};

/** One chip of a condition: a word you can tap to change. */
export function Pill({label,arrow,onPress,tone,a11y,muted}:{label:string;arrow?:string;onPress:()=>void;tone?:string;a11y:string;muted?:boolean}){
 return <Pressable accessibilityRole="button" accessibilityLabel={a11y} onPress={onPress}
  style={({pressed})=>[s.row,{gap:6,minHeight:40,paddingHorizontal:12,paddingVertical:7,borderRadius:10,borderWidth:1,
   borderColor:muted?C.line:(tone||C.line),backgroundColor:muted?C.bg:C.paper,opacity:pressed?.7:1}]}>
  {!!arrow&&<T style={{fontSize:14,color:tone||C.ink,fontFamily:'InterSemi'}}>{arrow}</T>}
  <T style={{fontSize:13,fontFamily:'InterMedium',color:muted?C.muted:C.ink}}>{label}</T>
  <Icon name="chevron-down" size={13} color={C.muted}/>
 </Pressable>;
}
const Arrow=()=><Icon name="chevron-right" size={14} color={C.muted}/>;

function PickSheet({title,subtitle,options,onPick,onClose,children}:{title:string;subtitle?:string;options:Option[];
 onPick:(key:string)=>void;onClose:()=>void;children?:React.ReactNode}){
 let lastGroup='';
 return <Sheet visible onClose={onClose} title={title} subtitle={subtitle}>
  <View style={{gap:6}}>
   {options.map(o=>{
    const head=o.group&&o.group!==lastGroup?o.group:'';if(o.group)lastGroup=o.group;
    return <View key={o.key} style={{gap:6}}>
     {!!head&&<T style={[s.label,{marginTop:10}]}>{head}</T>}
     <Pressable accessibilityRole="button" accessibilityState={{selected:!!o.active}} accessibilityLabel={o.label}
      onPress={()=>onPick(o.key)} style={({pressed})=>[s.row,{minHeight:50,paddingHorizontal:14,borderRadius:12,borderWidth:1,
       borderColor:o.active?C.green:C.line,backgroundColor:o.active?C.soft:C.paper,opacity:pressed?.7:1}]}>
      {!!o.arrow&&<T style={{width:26,fontSize:16,textAlign:'center',color:C.ink}}>{o.arrow}</T>}
      <View style={{flex:1,gap:2,paddingVertical:8}}>
       <T style={{fontFamily:'InterMedium',color:o.active?C.green:C.ink}}>{o.label}</T>
       {!!o.hint&&<T style={{fontSize:11,color:C.muted,lineHeight:16}}>{o.hint}</T>}
      </View>
      {o.active&&<Icon name="check" size={16} color={C.green}/>}
     </Pressable>
    </View>;
   })}
   {children}
  </View>
 </Sheet>;
}

function Stepper({label,value,min,max,onChange}:{label:string;value:number;min:number;max:number;onChange:(n:number)=>void}){
 const btn=(icon:string,next:number,a11y:string)=><Pressable accessibilityRole="button" accessibilityLabel={a11y}
  disabled={next<min||next>max} onPress={()=>onChange(next)}
  style={({pressed})=>({width:36,height:36,borderRadius:9,borderWidth:1,borderColor:C.line,alignItems:'center',justifyContent:'center',
   opacity:next<min||next>max?.35:pressed?.7:1})}><Icon name={icon} size={15} color={C.ink}/></Pressable>;
 return <View style={[s.row,{gap:8}]}>
  <T style={{fontSize:12,color:C.muted,minWidth:88}}>{label}</T>
  {btn('minus',value-1,`Fewer strikes ${label.toLowerCase()}`)}
  <T style={{minWidth:26,textAlign:'center',fontFamily:'InterSemi'}}>{value}</T>
  {btn('plus',value+1,`More strikes ${label.toLowerCase()}`)}
 </View>;
}

export type BuilderProps={vocab:Vocabulary;draft:Definition;onChange:(d:Definition)=>void;readsAs:string;error:string;
 text:string;onText:(t:string)=>void;onParse:()=>void;parsing:boolean;parsed:Parsed|null;compact:boolean;
 /** Inside a workspace widget: the widget header already titles it, so the prompt line drops to body scale. */
 dense?:boolean};

export function Builder({vocab,draft,onChange,readsAs,error,text,onText,onParse,parsing,parsed,compact,dense}:BuilderProps){
 const [pick,setPick]=useState<Pick>(null);
 const [more,setMore]=useState(false);
 const [symbolQuery,setSymbolQuery]=useState('');
 const [customFrom,setCustomFrom]=useState('10:15');
 const close=()=>setPick(null);
 const cond=(i?:number)=>i==null?null:draft.conditions[i];

 const conditionRow=(c:Condition,i:number)=>{
  const m=metricOf(vocab,c.metric);
  const st=m?.states.find(x=>x.key===c.state);
  const sideLabel=vocab.sides.find(x=>x.key===c.side)?.label;
  return <View key={i} style={{gap:8}}>
   {i>0&&<Pressable accessibilityRole="button" accessibilityLabel={`Joined with ${c.join==='or'?'OR':'AND'}. Switch to ${c.join==='or'?'AND':'OR'}`}
    onPress={()=>onChange(toggleJoin(draft,i))}
    style={({pressed})=>[s.row,{alignSelf:'flex-start',gap:6,paddingHorizontal:12,paddingVertical:5,borderRadius:8,
     backgroundColor:c.join==='or'?C.amberBg:C.soft,opacity:pressed?.7:1,marginLeft:4}]}>
    <T style={{fontSize:11,fontFamily:'InterSemi',letterSpacing:1.2,color:c.join==='or'?C.amber:C.green}}>{c.join==='or'?'OR':'AND'}</T>
    <Icon name="repeat" size={12} color={C.muted}/>
   </Pressable>}
   <View style={[s.row,{flexWrap:'wrap',gap:6,padding:10,borderRadius:13,borderWidth:1,borderColor:C.line,backgroundColor:C.dark}]}>
    <T style={{fontSize:11,color:C.muted,width:18,textAlign:'center'}}>{i+1}</T>
    <Pill label={m?.label||c.metric} a11y={`Metric: ${m?.label}. Change`} onPress={()=>setPick({kind:'metric',index:i})}/>
    {!!m?.sides.length&&<><Arrow/><Pill label={sideLabel||'Either side'} a11y={`Side: ${sideLabel}. Change`} onPress={()=>setPick({kind:'side',index:i})}/></>}
    <Arrow/>
    <Pill label={st?.label||c.state} arrow={st?.arrow} tone={TONE[st?.tone||'flat']} a11y={`State: ${st?.label}. Change`}
     onPress={()=>setPick({kind:'state',index:i})}/>
    <Arrow/>
    <Pill label={windowLabel(c.window)} a11y={`Time window: ${windowLabel(c.window)}. Change`} onPress={()=>setPick({kind:'window',index:i})}/>
    {m?.computed&&<Badge label="COMPUTED" tone="amber"/>}
    <View style={{flex:1}}/>
    {draft.conditions.length>1&&<Pressable accessibilityRole="button" accessibilityLabel={`Remove condition ${i+1}`}
     onPress={()=>onChange(removeCondition(draft,i))} style={({pressed})=>({padding:8,opacity:pressed?.6:1})}>
     <Icon name="x" size={16} color={C.muted}/></Pressable>}
   </View>
  </View>;
 };

 // --- the picker sheets -------------------------------------------------------------------------------------
 let sheet:React.ReactNode=null;
 const c=cond(pick?.index);
 if(pick?.kind==='metric'&&c){
  sheet=<PickSheet title="What to watch" subtitle="Each one is read from the 15-minute readings" onClose={close}
   options={vocab.metrics.map(m=>({key:m.key,label:m.label+(m.computed?' · computed':''),hint:m.help,group:m.group,active:m.key===c.metric}))}
   onPick={k=>{const m=metricOf(vocab,k);if(m)onChange(updateCondition(draft,pick.index!,withMetric(c,m)));close()}}/>;
 } else if(pick?.kind==='side'&&c){
  const m=metricOf(vocab,c.metric);
  sheet=<PickSheet title="Which side" onClose={close}
   options={(m?.sides||[]).map(k=>({key:k,label:vocab.sides.find(x=>x.key===k)?.label||k,active:k===c.side,
    hint:k==='either'?'Calls or puts — each contract on its own side':k==='both'?'Calls and puts at the same time':undefined}))}
   onPick={k=>{onChange(updateCondition(draft,pick.index!,{side:k as Side}));close()}}/>;
 } else if(pick?.kind==='state'&&c){
  const m=metricOf(vocab,c.metric);
  sheet=<PickSheet title={`${m?.label||''} is…`} subtitle="KANIDA reads what each word means from the readings — no numbers to set"
   onClose={close} options={(m?.states||[]).map(x=>({key:x.key,label:x.label,arrow:x.arrow,active:x.key===c.state}))}
   onPick={k=>{onChange(updateCondition(draft,pick.index!,{state:k}));close()}}/>;
 } else if(pick?.kind==='window'&&c){
  const custom=c.window.kind==='custom'?c.window:null;
  sheet=<PickSheet title="Over what time" subtitle="KANIDA takes a new snapshot every 15 minutes" onClose={close}
   options={vocab.windows.filter(w=>w.kind!=='custom').map((w,j)=>({key:String(j),label:w.label,active:sameWindow(w as Window,c.window)}))}
   onPick={k=>{const w=vocab.windows.filter(x=>x.kind!=='custom')[Number(k)];const {label:_l,...win}=w as any;
    onChange(updateCondition(draft,pick.index!,{window:win as Window}));close()}}>
   <T style={[s.label,{marginTop:14}]}>Custom window</T>
   <T style={{fontSize:12,color:C.muted}}>From</T>
   <View style={[s.row,{flexWrap:'wrap',gap:6}]}>{READING_TIMES.slice(0,-1).map(t=><Pressable key={t} accessibilityRole="button"
    accessibilityLabel={`From ${t}`} onPress={()=>setCustomFrom(t)} style={{paddingHorizontal:9,paddingVertical:6,borderRadius:8,borderWidth:1,
     borderColor:(custom?.from||customFrom)===t?C.green:C.line}}><T style={{fontSize:12}}>{t}</T></Pressable>)}</View>
   <T style={{fontSize:12,color:C.muted}}>To</T>
   <View style={[s.row,{flexWrap:'wrap',gap:6}]}>{READING_TIMES.filter(t=>t>(custom?.from||customFrom)).map(t=><Pressable key={t}
    accessibilityRole="button" accessibilityLabel={`To ${t}`}
    onPress={()=>{onChange(updateCondition(draft,pick.index!,{window:{kind:'custom',from:custom?.from||customFrom,to:t}}));close()}}
    style={{paddingHorizontal:9,paddingVertical:6,borderRadius:8,borderWidth:1,borderColor:custom?.to===t?C.green:C.line}}>
    <T style={{fontSize:12}}>{t}</T></Pressable>)}</View>
  </PickSheet>;
 } else if(pick?.kind==='universe'){
  const chosen=draft.universe.kind==='symbols'?draft.universe.symbols:[];
  const q=symbolQuery.trim().toUpperCase();
  const list=vocab.underlyings.filter(u=>!q||u.includes(q)).slice(0,60);
  const toggle=(u:string)=>{const next=chosen.includes(u)?chosen.filter(x=>x!==u):[...chosen,u];
   onChange({...draft,universe:next.length?{kind:'symbols',symbols:next}:{kind:'all'}})};
  sheet=<PickSheet title="Where to look" onClose={close}
   options={vocab.universes.filter(u=>u.key!=='symbols').map(u=>({key:u.key,label:u.label,active:draft.universe.kind===u.key}))}
   onPick={k=>{onChange({...draft,universe:{kind:k as 'all'}});close()}}>
   <T style={[s.label,{marginTop:14}]}>Or choose symbols</T>
   <TextInput value={symbolQuery} onChangeText={setSymbolQuery} placeholder="Search NIFTY, RELIANCE…" placeholderTextColor={C.muted}
    accessibilityLabel="Search symbols" autoCapitalize="characters" style={s.input}/>
   {!!chosen.length&&<T style={{fontSize:12,color:C.green}}>{chosen.join(', ')}</T>}
   <View style={[s.row,{flexWrap:'wrap',gap:6}]}>{list.map(u=><Pressable key={u} accessibilityRole="button"
    accessibilityState={{selected:chosen.includes(u)}} accessibilityLabel={u} onPress={()=>toggle(u)}
    style={{paddingHorizontal:10,paddingVertical:7,borderRadius:8,borderWidth:1,borderColor:chosen.includes(u)?C.green:C.line,
     backgroundColor:chosen.includes(u)?C.soft:C.paper}}><T style={{fontSize:12}}>{u}</T></Pressable>)}</View>
  </PickSheet>;
 } else if(pick?.kind==='expiry'){
  sheet=<PickSheet title="Which expiry" onClose={close}
   options={vocab.expiries.map(e=>({key:e.key,label:e.label,active:draft.expiry===e.key,
    hint:e.key==='nearest'?'On its own expiry day, IV / delta / gamma conditions read the next expiry':undefined}))}
   onPick={k=>{onChange({...draft,expiry:k as Definition['expiry']});close()}}/>;
 } else if(pick?.kind==='strikes'){
  const presets=vocab.strike_presets.map((p,j)=>({key:`p${j}`,label:p.label,group:'Around the ATM strike',
   active:JSON.stringify({...p,label:undefined})===JSON.stringify({...draft.strikes,label:undefined})}));
  const deltas=vocab.delta_bands.map(b=>({key:`d${b.key}`,label:`${b.label} (|Δ| ${b.lo.toFixed(2)}–${b.hi.toFixed(2)})`,
   group:'By delta · computed',active:draft.strikes.kind==='delta'&&draft.strikes.band===b.key}));
  sheet=<PickSheet title="Which strikes" subtitle="ATM is the listed strike nearest spot, re-read at every reading" onClose={close}
   options={[...presets,...deltas]} onPick={k=>{
    let next:Strikes;
    if(k.startsWith('d'))next={kind:'delta',band:k.slice(1)};
    else{const {label:_l,...p}=vocab.strike_presets[Number(k.slice(1))] as any;next=p as Strikes}
    onChange({...draft,strikes:next});close()}}/>;
 }

 const atm=draft.strikes.kind==='atm'?draft.strikes:null;
 return <View style={{gap:16}}>
  <View style={{gap:8}}>
   <T style={dense?{fontFamily:'Inter',fontSize:12,lineHeight:18,color:C.muted}:{fontFamily:'ManropeBold',fontSize:compact?18:20}}>
    Tell KANIDA the behaviour you want to find</T>
   <View style={[s.row,{gap:8,alignItems:'stretch'}]}>
    <TextInput value={text} onChangeText={onText} onSubmitEditing={onParse} returnKeyType="go" multiline={compact}
     {...({blurOnSubmit:true} as any)}
     accessibilityLabel="Describe the scanner in your own words"
     placeholder="e.g. call OI building for 45 min and call IV expanding, F&O stocks" placeholderTextColor={C.muted}
     style={[s.input,{flex:1,minHeight:compact?84:48,paddingVertical:12,textAlignVertical:'top'}]}/>
    <Button label={compact?'Build':'Build scanner'} icon="zap" onPress={onParse} loading={parsing} disabled={!text.trim()}/>
   </View>
   {parsed&&<View style={{gap:5}}>
    <T style={{fontSize:12,color:C.muted}}>Understood as the conditions below — change any chip to adjust.</T>
    {parsed.assumptions.map((a,j)=><View key={`a${j}`} style={[s.row,{gap:6,alignItems:'flex-start'}]}>
     <Icon name="info" size={13} color={C.amber}/><T style={{flex:1,fontSize:12,color:C.amber,lineHeight:18}}>{a}</T></View>)}
    {parsed.unmapped.map((u,j)=><View key={`u${j}`} style={[s.row,{gap:6,alignSelf:'flex-start',paddingHorizontal:10,paddingVertical:5,
     borderRadius:8,backgroundColor:'#2A1519'}]}><Icon name="help-circle" size={13} color={C.red}/>
     <T style={{fontSize:12,color:C.red}}>Didn’t understand: “{u}”</T></View>)}
   </View>}
  </View>

  <View style={[s.row,{flexWrap:'wrap',gap:8}]}>
   <T style={{fontSize:12,color:C.muted}}>Look in</T>
   <Pill label={universeLabel(draft,vocab)} a11y="Where to look. Change" onPress={()=>setPick({kind:'universe'})}/>
   <Pill label={vocab.expiries.find(e=>e.key===draft.expiry)?.label||'Nearest expiry'} a11y="Expiry. Change" onPress={()=>setPick({kind:'expiry'})}/>
   <Pill label={strikesLabel(draft.strikes,vocab)} a11y="Strikes. Change" onPress={()=>setPick({kind:'strikes'})}/>
  </View>

  <View style={{gap:8}}>{draft.conditions.map(conditionRow)}</View>

  <View style={[s.row,{flexWrap:'wrap',gap:10}]}>
   {draft.conditions.length<vocab.max_conditions&&<Button label="Add condition" icon="plus" kind="soft" onPress={()=>onChange(addCondition(draft,vocab))}/>}
   <Button label={more?'Fewer options':'More options'} icon="sliders" kind="outline" onPress={()=>setMore(m=>!m)}
    accessibilityState={{expanded:more}}/>
  </View>

  {more&&<View style={[s.card,{padding:16,gap:12}]}>
   <T style={s.label}>Strike range</T>
   {atm?<View style={{gap:8}}>
    <Stepper label="Below ATM" value={atm.below} min={0} max={vocab.max_rungs} onChange={n=>onChange({...draft,strikes:{...atm,below:n}})}/>
    <Stepper label="Above ATM" value={atm.above} min={0} max={vocab.max_rungs} onChange={n=>onChange({...draft,strikes:{...atm,above:n}})}/>
   </View>:<T style={{fontSize:12,color:C.muted}}>{strikesLabel(draft.strikes,vocab)} — pick "ATM ±5" under Strikes to set offsets.</T>}
   <Checkbox checked={draft.liquid_only} onChange={v=>onChange({...draft,liquid_only:v})} label="Liquid strikes only"
    detail="Only contracts that clear the Derivative tab's liquidity floors (₹2 cr premium traded, open interest, price) at each reading."/>
   <T style={{fontSize:11,color:C.muted,lineHeight:17}}>Conditions joined by AND must all hold; OR starts a new group — "A AND B OR C" reads as (A AND B) OR C. Tap AND / OR between two conditions to switch.</T>
  </View>}

  <View style={{gap:4,padding:14,borderRadius:12,backgroundColor:C.dark,borderWidth:1,borderColor:error?C.red:C.line}}>
   <T style={s.label}>Reads as</T>
   <T style={{fontSize:13,lineHeight:20,color:error?C.red:C.ink}}>{error||readsAs||'…'}</T>
  </View>
  {sheet}
 </View>;
}
