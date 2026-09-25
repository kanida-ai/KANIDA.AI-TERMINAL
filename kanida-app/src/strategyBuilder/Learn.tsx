// Robinhood-gap components (slice 10 batch 2):
//  * SpreadsSheet - a pre-paired spread chain: every vertical of one kind and width, one row per anchor strike, priced to
//    execute with its risk numbers; choosing a row loads an ordinary strategy (the same draft/revisions as by hand).
//  * AboutSheet - strategy education for the structure on screen: basics, calculations with THIS strategy's numbers,
//    what to monitor, and whether you can lose more than the maximum loss.
//  * TemplateIntro - the introduction a complex/advanced template shows before it is used (explicit confirmation).
//  * Sketch - the qualitative payoff shape on a template card.
import React,{useEffect,useState} from 'react';
import {Pressable,View,useWindowDimensions} from 'react-native';
import Svg,{Line,Path} from 'react-native-svg';
import {Badge,Button,C,Checkbox,Chip,Loading,Sheet,T,s} from '../ui';
import {sb,type Analysis,type Template} from './api';
import {inr,num,signed,strikeText} from './format';
import {ErrorRetry,Scrollable} from './States';

type SpreadRow={anchor:number;strikes:[number,number];distance_pct:number;atm:boolean;net:number;net_per_unit:number;direction:string;
 max_profit:number|null;max_loss:number|null;breakevens:number[]|null;pop:number|null;return_on_risk:number|null;delta:number|null;theta:number|null;
 charges:number;price_basis:string[];legs:{type:'CE'|'PE';side:'B'|'S';strike:number;lots:number;expiry:string;price:number}[]};

const KINDS:[string,string,string][]=[['CE','debit','Bull call (debit)'],['PE','credit','Bull put (credit)'],['PE','debit','Bear put (debit)'],['CE','credit','Bear call (credit)']];

export function SpreadsSheet({visible,onClose,underlying,expiry,lots,onPick}:{visible:boolean;onClose:()=>void;underlying:string;expiry:string;lots:number;
 onPick:(legs:SpreadRow['legs'],template:string,width:number)=>void}){
 const [kind,setKind]=useState(0);const [width,setWidth]=useState(2);const [data,setData]=useState<any>(null);const [error,setError]=useState('');const [sel,setSel]=useState<number|null>(null);
 const narrow=useWindowDimensions().width<700;const [retry,setRetry]=useState(0);
 // only the answer to the newest kind/width request is shown (a fast change can never show the wrong spread family)
 useEffect(()=>{if(!visible||!underlying||!expiry)return;let live=true;setData(null);setError('');setSel(null);const [t,sd]=KINDS[kind];
  sb.spreads(underlying,expiry,t,sd,width,lots).then(d=>{if(live)setData(d);}).catch(e=>{if(live)setError(e.message);});return()=>{live=false};},[visible,underlying,expiry,kind,width,lots,retry]);
 const row:SpreadRow|null=data&&sel!=null?data.rows[sel]:null;
 return <Sheet visible={visible} onClose={onClose} wide title="Spreads" subtitle={data?`${data.name} · ${underlying} ${expiry} · spot ${num(data.spot,2)} · ${lots} lot${lots>1?'s':''}`:'Every vertical spread of one kind and width, priced to execute'}
  footer={<View style={[s.between,{flexWrap:'wrap',gap:8}]}><T style={{fontSize:11,color:C.muted,flex:1,minWidth:240}}>{data?.basis||''}</T>
   <Button label={row?`Use ${strikeText(row.strikes[0])} / ${strikeText(row.strikes[1])}`:'Pick a row'} icon="check" disabled={!row} onPress={()=>row&&onPick(row.legs,data.template,width)}/></View>}>
  <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>{KINDS.map(([_t,_s,l],i)=><Chip key={l} label={l} active={kind===i} onPress={()=>setKind(i)}/>)}</View>
  <View style={[s.row,{gap:6,flexWrap:'wrap'}]}><T style={{fontSize:11,color:C.muted}}>Width (strikes)</T>{[1,2,4,6,8].map(w=><Chip key={w} label={String(w)} active={width===w} onPress={()=>setWidth(w)}/>)}</View>
  {!!error&&<ErrorRetry what="Spreads" error={error} onRetry={()=>setRetry(n=>n+1)}/>}
  {!data&&!error&&<Loading/>}
  {data&&!data.rows.length&&<T style={{fontSize:12,color:C.muted}}>No priced spreads of this kind and width in this expiry.</T>}
  {data?.excluded_inconsistent>0&&<T style={{fontSize:11,color:C.muted}}>{`${data.excluded_inconsistent} spread${data.excluded_inconsistent>1?'s':''} left out: their prices cost more than the spread can ever pay (stale last-traded prices on illiquid strikes).`}</T>}
  {data&&!!data.rows.length&&<Scrollable narrow={narrow} min={760}><View style={{borderWidth:1,borderColor:C.line,borderRadius:10,overflow:'hidden'}}>
   <View style={[s.row,{backgroundColor:C.paper,paddingHorizontal:10,paddingVertical:6}]}>{['Strikes','From spot',data.side==='debit'?'Debit':'Credit','Max profit','Max loss','Breakeven','POP','Return on risk'].map((h,i)=>
    <T key={h} style={{flex:i===0?1.4:1,fontSize:10,color:C.muted,textAlign:i?'right':'left'}}>{h}</T>)}</View>
   {data.rows.map((r:SpreadRow,i:number)=><Pressable key={r.anchor} accessibilityRole="button" accessibilityState={{selected:sel===i}} accessibilityLabel={`${r.strikes.join(' / ')} spread`}
    onPress={()=>setSel(i)} style={[s.row,{paddingHorizontal:10,paddingVertical:8,borderTopWidth:1,borderColor:C.line,backgroundColor:sel===i?C.soft:r.atm?'#39E5A30F':'transparent'}]}>
    <T style={{flex:1.4,fontSize:12,fontFamily:'InterSemi'}}>{`${strikeText(r.strikes[0])} / ${strikeText(r.strikes[1])}${r.atm?' · ATM':''}`}</T>
    {[`${r.distance_pct>0?'+':''}${r.distance_pct}%`,inr(Math.abs(r.net)),signed(r.max_profit),signed(r.max_loss),(r.breakevens||[]).map(b=>num(b,0)).join(' · ')||'—',
     r.pop!=null?`${r.pop}%`:'—',r.return_on_risk!=null?`${r.return_on_risk}%`:'—'].map((c,j)=><T key={j} style={{flex:1,fontSize:12,textAlign:'right',fontVariant:['tabular-nums'] as any}}>{c}</T>)}
   </Pressable>)}
  </View></Scrollable>}
  {row&&<View style={{gap:3}}>{row.legs.map((l,i)=><T key={i} style={{fontSize:12,color:l.side==='B'?C.green:C.red}}>{`${l.side==='B'?'Buy':'Sell'} ${l.lots} × ${strikeText(l.strike)} ${l.type} @ ${num(l.price)}`}</T>)}
   <T style={{fontSize:11,color:C.muted}}>{`Net delta ${num(row.delta,1)} · theta ${num(row.theta,0)}/day · charges ~${inr(row.charges)}${row.price_basis.includes('ltp')?' · some legs priced at the last trade (no live quote)':''}`}</T></View>}
 </Sheet>;
}

export function AboutSheet({visible,onClose,a,templateKey}:{visible:boolean;onClose:()=>void;a:Analysis|null;templateKey:string|null}){
 const [data,setData]=useState<{templates:any[];legging:string}|null>(null);const [err,setErr]=useState('');
 const load=()=>{setErr('');sb.templates().then(setData as any).catch(e=>setErr(e.message));};
 useEffect(()=>{if(visible&&!data)load();},[visible,data]);// eslint-disable-line react-hooks/exhaustive-deps
 const t=data?.templates.find(x=>x.key===templateKey)||null;
 const prem=a?.premium?.value;const ml=a?.max_loss;const mp=a?.max_profit;
 return <Sheet visible={visible} onClose={onClose} wide title={`About: ${a?.structure?.name||'this strategy'}`} subtitle="Plain-language basics, this strategy's own numbers, and what to watch">
  {err?<ErrorRetry what="The strategy explanations" error={err} onRetry={load}/>:!data?<Loading/>:<>
   <Section title="Basics">{t?<><T style={{fontSize:13}}>{t.recipe}</T><T style={{fontSize:13}}>{`Use: ${t.use}`}</T><T style={{fontSize:13,color:C.amber}}>{`Loses when: ${t.loses}`}</T></>:
    <T style={{fontSize:13,color:C.muted}}>A custom combination - it does not match a named structure, so read the payoff chart and the numbers below.</T>}</Section>
   <Section title="Calculations (this strategy, at the reading)">
    <T style={{fontSize:13}}>{prem==null?'—':prem>0?`You receive a net credit of ${inr(prem)} when you open it.`:`You pay a net debit of ${inr(-prem)} to open it.`}</T>
    <T style={{fontSize:13}}>{`Maximum profit at expiry: ${mp?.unlimited?'unlimited':signed(mp?.value)}. Maximum loss at expiry: ${ml?.unlimited?'UNLIMITED':signed(ml?.value)}.`}</T>
    <T style={{fontSize:13}}>{`Breakeven at expiry: ${a?.breakevens?.value?.length?a.breakevens.value.map((b:number)=>num(b,0)).join(' and '):'none'}.`}</T>
    <T style={{fontSize:12,color:C.muted}}>These come from the exact expiry payoff of every leg at its entry price, gross of charges (about {inr(a?.charges?.value)} to open). Before expiry the model value differs - move the what-if date to see it.</T>
   </Section>
   <Section title="What to monitor"><T style={{fontSize:13}}>{t?.monitor||'Watch the underlying against your breakevens and any short strike; check how the payoff changes as the expiry nears.'}</T>
    {(a?.insights||[]).slice(0,4).map((w,i)=><T key={i} style={{fontSize:12,color:w.level==='warn'?C.amber:C.muted}}>{`• ${w.text}`}</T>)}</Section>
   <Section title="Can I lose more than the maximum loss?"><T style={{fontSize:13}}>{data.legging}</T></Section>
  </>}
 </Sheet>;
}

export function TemplateIntro({t,onCancel,onConfirm,legging}:{t:Template&{monitor?:string}|null;onCancel:()=>void;onConfirm:()=>void;legging:string}){
 const [ok,setOk]=useState(false);
 useEffect(()=>{setOk(false);},[t?.key]);
 return <Sheet visible={!!t} onClose={onCancel} title={t?`Before you use: ${t.name}`:''} subtitle="Complex or advanced structure - read this first"
  footer={<View style={[s.row,{justifyContent:'flex-end',gap:8}]}><Button label="Cancel" kind="outline" onPress={onCancel}/><Button label="Use it" icon="check" disabled={!ok} onPress={onConfirm}/></View>}>
  {t&&<View style={{gap:8}}>
   <View style={[s.row,{gap:8}]}><Badge label={t.risk==='defined'?'Defined risk':'UNHEDGED'} tone={t.risk==='defined'?'green':'red'}/><T style={{fontSize:12,color:C.muted}}>{`${t.legs} legs · complexity ${t.complexity}/3`}</T></View>
   <T style={{fontSize:13}}>{t.recipe}</T><T style={{fontSize:13}}>{`Use: ${t.use}`}</T><T style={{fontSize:13,color:C.amber}}>{`Loses when: ${t.loses}`}</T>
   {!!t.monitor&&<T style={{fontSize:13}}>{`Monitor: ${t.monitor}`}</T>}
   <T style={{fontSize:12,color:C.muted}}>{legging}</T>
   <Checkbox checked={ok} onChange={setOk} tone={t.risk==='defined'?C.green:C.red} label={t.risk==='defined'?'I understand how this structure makes and loses money':'I understand one or more shorts are UNCOVERED and the loss can be very large'}/>
  </View>}
 </Sheet>;
}

export function Sketch({pts,width=120,height=36}:{pts?:number[]|null;width?:number;height?:number}){
 if(!pts||pts.length<2)return null;
 const X=(i:number)=>2+i/(pts.length-1)*(width-4);const Y=(v:number)=>height/2-v*(height/2-3);
 const d=pts.map((v,i)=>`${i?'L':'M'}${X(i).toFixed(1)},${Y(v).toFixed(1)}`).join('');
 return <View accessibilityLabel="Payoff shape at expiry (illustrative)"><Svg width={width} height={height}>
  <Line x1={2} x2={width-2} y1={height/2} y2={height/2} stroke={C.line} strokeWidth={1}/>
  <Path d={d} stroke={C.green} strokeWidth={1.8} fill="none"/></Svg></View>;
}

function Section({title,children}:{title:string;children:React.ReactNode}){
 return <View style={{gap:5,borderTopWidth:1,borderColor:C.line,paddingTop:10}}><T style={{fontFamily:'InterSemi',fontSize:13}}>{title}</T>{children}</View>;
}
