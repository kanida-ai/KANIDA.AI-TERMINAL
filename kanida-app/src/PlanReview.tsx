import React,{useState,useRef,useEffect} from 'react';
import {View,TextInput} from 'react-native';
import {router} from 'expo-router';
import {useProduct} from './context';
import {useExitPlan,ExitBrief,planError} from './ExitPlan';
import {C,T,s,Button,Chip,Badge,Stat,Icon} from './ui';
import {api,money,sizing,dateText,History} from './model';
import {CAPITAL_CHOICES,ROUND_TRIP_COST_PCT} from './constants';
export function PlanReview({selection}:any){
 const {setPlan,refreshProduct,setToast,filters}=useProduct();const {match,side,study}=selection;const exit=useExitPlan(match,side),suggestion=exit.data;const [custom,setCustom]=useState(false),[hold,setHold]=useState('6');
 const [account,setAccount]=useState('100000'),[allocation,setAllocation]=useState('10000'),[risk,setRisk]=useState('0.5'),[stop,setStop]=useState('2.5'),[reward,setReward]=useState('2'),[advanced,setAdvanced]=useState(false),[review,setReview]=useState(false),[busy,setBusy]=useState(false),[error,setError]=useState(''),[refused,setRefused]=useState(false);
 const request=useRef(`${Date.now()}-${Math.random().toString(36).slice(2)}`);const [a,al,r]=[account,allocation,risk].map(Number);const st=custom?Number(stop):suggestion?.stop_pct||0,rw=custom?Number(reward):suggestion?.reward||2,h=custom?Number(hold):suggestion?.rule.hold||6;const z=sizing(match.price,a,al,r,st,suggestion?.cost_pct??ROUND_TRIP_COST_PCT);
 const valid=!!suggestion&&Number.isInteger(h)&&h>=1&&h<=260&&(side!=='short'||st*rw<100)&&[a,al,r,st,rw].every(Number.isFinite)&&a>=1000&&a<=10000000&&al>=100&&al<=a&&r>=.1&&r<=2&&st>=(custom?.25:.001)&&st<=20&&rw>=.5&&rw<=5&&z.shares>0;
 const history=match.history.find((h:History)=>h.side===side);
 // Saving a paper plan requires evidence for the exact rule being traded; the server enforces the same gate.
 // refused: the server rejected this rule's evidence (EXIT_EVIDENCE / EXIT_EVIDENCE_CHANGED); only an illustrative draft remains possible until evidence reloads.
 const tradable=!custom&&!refused&&suggestion?.tradable_evidence===true;const blockReason=custom?'Custom exits are untested: no historical replay of this exact rule exists.':refused?'The server found no current tested evidence for this exact exit rule.':suggestion?.tradable_reason||'No historical replay of this exact exit rule meets the sample requirement.';
 function restore(){if(!suggestion)return;setStop(suggestion.stop_pct.toFixed(4));setReward(String(suggestion.reward));setHold(String(suggestion.rule.hold));setCustom(false);setReview(false);setRefused(false);request.current=`${Date.now()}-${Math.random().toString(36).slice(2)}`;}
 useEffect(()=>{restore()},[suggestion?.id]);
 const edit=(setter:any)=>(value:string)=>{setter(value);if([setStop,setReward,setHold].includes(setter))setCustom(true);setReview(false);setError('');request.current=`${Date.now()}-${Math.random().toString(36).slice(2)}`};
 async function save(illustrative=false){if(!tradable&&!illustrative)return;setBusy(true);setError('');try{await api('/api/product/plans',{match_id:match.id,side,account:a,allocation:al,risk_pct:r,stop_pct:st,reward:rw,hold:h,trigger:suggestion.rule.trigger,suggestion_id:suggestion.id,exit_mode:custom?'custom':'suggested',request_id:request.current,minimum_history:filters.minimum,...(illustrative?{illustrative:true}:{}),...(study?{study_context:{id:study.id,trade_id:study.trade_id}}:{})});await refreshProduct();setPlan(null);router.push('/autotrade');setToast(illustrative?'Illustrative draft saved. It cannot be simulated. No order placed.':'Your paper plan is saved. No order placed.')}catch(e:any){setError(planError(e));if(e?.code==='EXIT_EVIDENCE'||e?.code==='EXIT_EVIDENCE_CHANGED'){setRefused(true);exit.retry()}}finally{setBusy(false)}}
 return <><View style={s.between}><Badge label={review?'REVIEW YOUR PLAN':'PREPARE A PLAN'}/><Badge label="PAPER ONLY" tone="amber"/></View><View style={{gap:7}}><T style={{fontFamily:'ManropeBold',fontSize:27,lineHeight:35}}>{match.symbol}</T><T style={{fontSize:12,color:C.muted}}>{match.pattern_name} · {match.timeframe} · {side==='short'?'Sell / short':'Buy / long'}</T><T style={{fontSize:11,color:C.muted}}>Snapshot {money(match.price)} · {dateText(match.candle_end)}</T></View>
 {study&&<View style={[s.card,{backgroundColor:C.dark}]}><T style={{fontFamily:'InterSemi'}}>Historical study attached</T><T style={{fontSize:12,color:C.muted}}>{study.validation} · Study {study.id.slice(0,6)} · Hold up to {study.rule?.hold} candles. The plan below uses the exit rules shown for this setup. A historical study does not enable live orders or validate different exits.</T></View>}
 {!review?<>
  {custom?<View style={[s.card,{backgroundColor:C.amberBg}]}>
   <Badge label="Custom · untested" tone="amber"/>
   <T style={{fontSize:12}}>You changed the exits. Historical rule evidence no longer applies.</T>
   <Button label="Restore suggested exits" kind="outline" onPress={restore}/>
  </View>:<ExitBrief {...exit} compact/>}
  <View style={s.card}>
   <T style={{fontFamily:'InterSemi',fontSize:14}}>Capital for this idea</T>
   <View style={[s.row,{flexWrap:'wrap'}]}>{CAPITAL_CHOICES.map(String).map(v=><Chip key={v} label={money(Number(v))} active={allocation===v} onPress={()=>edit(setAllocation)(v)}/>)}</View>
   <T style={{fontFamily:'InterSemi',fontSize:14}}>Paper account risk budget</T>
   <View style={[s.row,{flexWrap:'wrap'}]}>{['0.5','1','2'].map(v=><Chip key={v} label={`${v}% · ${money(a*Number(v)/100)}`} active={risk===v} onPress={()=>edit(setRisk)(v)}/>)}</View>
   <Button label={advanced?'Hide detailed settings':'Adjust account & exit rules'} kind="ghost" icon="sliders" onPress={()=>setAdvanced(!advanced)}/>
   {advanced&&[['Paper account (₹)',account,setAccount],['Capital per setup (₹)',allocation,setAllocation],['Account risk limit (%) · 0.1–2',risk,setRisk],['Stop distance (%) · 0.25–20',stop,setStop],['Target / stop multiple · 0.5–5',reward,setReward],['Maximum holding candles · 1–260',hold,setHold]].map(([label,value,setter]:any)=><View key={label} style={{gap:5}}>
    <T style={{fontSize:11,color:C.muted}}>{label}</T>
    <TextInput accessibilityLabel={label} keyboardType="decimal-pad" value={value} onChangeText={edit(setter)} style={s.input}/>
   </View>)}
  </View>
  <View style={[s.card,{backgroundColor:'#0B1E22',borderColor:'#20413E'}]}>
   <View style={s.row}><Icon name="aperture" color={C.green}/><T style={{fontFamily:'InterSemi',fontSize:13}}>Here’s the position that fits.</T></View>
   <View style={s.row}><Stat label="Whole shares" value={valid?z.shares:'—'}/><Stat label="Capital used" value={valid?money(z.notional):'—'}/></View>
   <T style={{fontSize:12,color:C.muted}}>Planned stop loss + costs: {valid?money(z.loss):'—'}. Both your capital and account-risk budgets limit the size. Gaps can exceed the stop.</T>
  </View>
  <Button label="Review paper plan" icon="arrow-right" disabled={!valid} onPress={()=>setReview(true)}/>
 </>:<>
  <Badge label={custom?'Custom · untested':suggestion.label} tone={!custom&&suggestion.evidence_applies?'green':'amber'}/>
  <View style={s.card}>
   <T style={{fontFamily:'InterSemi',fontSize:16}}>One plan. Every rule visible.</T>
   {[['Position',`${z.shares} shares · ${money(z.notional)}`],['Entry',suggestion.entry],['Stop',`${st.toFixed(2)}% preview distance`],['Target',`${(st*rw).toFixed(2)}% · risk 1 to seek ${rw}`],['Maximum hold',custom?`${h} ${match.timeframe} candles`:suggestion.holding.label],['Planned risk + costs',money(z.loss)]].map(([label,value])=><View key={label} style={[s.between,{alignItems:'flex-start'}]}>
    <T style={{fontSize:12,color:C.muted,flex:1}}>{label}</T>
    <T style={{fontSize:12,textAlign:'right',flex:1.5}}>{value}</T>
   </View>)}
   <T style={{fontSize:11,color:C.muted}}>{custom?'Custom percentage exits.':suggestion.stop_basis+'.'} Recalculate the levels and quantity at the actual next-open entry. Exit at the first stop, target or holding limit. Stop first if both barriers are hit in one candle. Reserved costs: {money(z.cost)}.</T>
  </View>
  <View style={[s.card,{backgroundColor:C.amberBg}]}>
   <T style={{fontSize:12,color:C.amber}}>This saves a draft. Fresh prices and an execution connection are required. {custom?'Custom exits need a separate historical test.':suggestion.evidence_applies?'The attached results belong to this exact tested rule.':'This benchmark has no supported historical win rate. The baseline uses different exits.'}</T>
   {side==='short'&&<T style={{fontSize:11,color:C.amber}}>Borrow and instrument eligibility must be verified for short trades.</T>}
  </View>
  {!tradable&&<View style={[s.card,{borderColor:C.red}]}>
   <T style={{fontSize:12,color:C.red}}>Saving a paper plan is blocked. {blockReason}</T>
   <T style={{fontSize:11,color:C.muted}}>You can keep it as an illustrative draft. Illustrative drafts cannot be simulated or executed.</T>
  </View>}
  <Button label="Save paper plan" icon="check" loading={busy&&tradable} disabled={!tradable||busy} onPress={()=>save()}/>
  {!tradable&&<Button label="Save as illustrative draft" icon="bookmark" kind="outline" loading={busy} onPress={()=>save(true)}/>}
  <Button label="Adjust the plan" kind="outline" disabled={busy} onPress={()=>setReview(false)}/>
 </>}
 {!valid&&<T style={{fontSize:12,color:C.red}}>Adjust the settings to cover at least one share within your account, allocation and risk limits.</T>}{error&&<T style={{fontSize:12,color:C.red}}>{error}</T>}{!review&&<T style={{fontSize:11,color:C.muted}}>A size preview using the stored close, not a live fill. Preview exits: {st.toFixed(2)}% stop / {valid?(st*rw).toFixed(2):'—'}% target. Editing exits removes their evidence label. No cash is committed.{suggestion&&!tradable?' These exits are illustrative, not tradable evidence.':''}</T>}</>;
}
