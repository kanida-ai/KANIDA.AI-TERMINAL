import React,{useEffect,useState} from 'react';
import {View} from 'react-native';
import {Match,api,pct,money} from './model';
import {C,T,s,Button,Badge,Stat,Loading} from './ui';
import {MIN_TRADES_DEFAULT,DATA_STALE_DAYS} from './decision';
// One disclosure for every exit brief (compact and expanded). Source research prices carry no corporate-action or dividend adjustment.
const SELECTION_NOTE='Selected from many stocks and rules; later-test average may still be optimistic.';
export const PRICE_ADJUSTMENT_NOTE='Prices not adjusted for splits, bonuses or dividends; costs are assumed.';
// Readable refusals from plan saves, simulations and live submits. Lives here (not PilotScreens) so PlanReview and PilotScreens can share it without a circular import. Unknown codes fall back to the server's message.
export function planError(e:any){const m=String(e?.message||'Something went wrong.'),code=String(e?.code||'');const known:Record<string,string>={
 EXIT_EVIDENCE:'No tested evidence for this exact exit rule. It can only be saved as an illustrative draft, which can’t be simulated.',
 EXIT_EVIDENCE_CHANGED:'The evidence for this exit rule changed or no longer passes since you saved the plan. Prepare the plan again from Discover and review it.',
 DATA_STALE:'Stored market data is older than the allowed age. Fresh prices are needed before simulating. Your plan stays saved and research remains available.',
 PLAN_REQUIRED:'Only a draft plan with tested evidence for its exact exit rule can be simulated. Illustrative, paused and archived plans can’t.',
 PLAN_CHANGED:'This plan changed while its evidence was being checked. Open it again and review it before continuing.',
 EVIDENCE_UNAVAILABLE:'The evidence for this plan can’t be re-checked right now, so nothing was submitted. Try again when research is available.'};
 const text=known[code];return !text?m:m&&m!==text&&code!=='PLAN_REQUIRED'?`${text} ${m}`:text}
// The same Prepare-trade gate everywhere (Discover, Agent case): fresh prices AND evidence for the exact exit rule. '' means allowed.
export function prepareBlock({stale,age,match,history,exit}:{stale:boolean;age:number|null;match:Match;history:any;exit:{error:string;data:any}}){
 return stale?`Fresh prices are needed. ${age!=null&&age<=DATA_STALE_DAYS?'Stored prices are marked stale.':`Stored market data is ${age==null?'of unknown age':age+' days old'} (limit ${DATA_STALE_DAYS} days).`} Research and evidence remain available.`:!match.current?'Fresh prices are needed. This stored setup is not on a current candle.':!history?'No historical evidence for this setup.':exit.error?'Exit-rule evidence could not load.':!exit.data?'Checking exit-rule evidence…':exit.data.tradable_evidence!==true?'No tested evidence for this exit rule yet.':'';
}
const cache=new Map<string,any>();const pending=new Map<string,Promise<any>>();
export function useExitPlan(match:Match,side:string){
 const run=match.history.find(h=>h.side===side)?.run||'';
 const key=[match.id,side,run,match.candle_end].join('|');
 const [value,setValue]=useState<{key:string;data:any}>({key:'',data:null}),[error,setError]=useState(''),[retry,setRetry]=useState(0);
 // A match with NO history has no exit rule to plan from, and the server refuses the request by design (the
 // exit plan resolves against the stored 10-pattern scan and its backtest store, which a researched detection
 // is deliberately not joined to). Asking anyway would log a 409 on every chart open and tell the user
 // nothing: `blockingReason` already states "No historical evidence for this setup." from the same fact.
 const evidenced=(match.history||[]).length>0;
 useEffect(()=>{let active=true;setError('');if(!evidenced){setValue({key,data:null});return}
  const saved=cache.get(key);if(saved){setValue({key,data:saved});return}
  let request=pending.get(key);if(!request){request=api('/api/product/exit-plan?'+new URLSearchParams({match_id:match.id,side,run,snapshot:match.candle_end}));pending.set(key,request)}
  request.then(data=>{cache.set(key,data);if(cache.size>128)cache.delete(cache.keys().next().value!);if(active)setValue({key,data})}).catch(e=>{if(active)setError(e.message)}).finally(()=>pending.delete(key));
  return()=>{active=false};
 },[key,retry,evidenced]);
 const data=!evidenced?null:(value.key===key?value.data:cache.get(key)||null);
 // tradable_evidence is true only when the server has stats for the exact rule that would be traded.
 return {data,error,retry:()=>{cache.delete(key);setRetry(v=>v+1)},tradable_evidence:data?.tradable_evidence===true,
  tradable_reason:String(data?.tradable_reason||''),history_label:String(data?.history_label||'Pattern hold-period history, not this exit rule')};
}
export function ExitBrief({data,error,retry,compact=false}:any){
 const [expanded,setExpanded]=useState(false);
 if(error)return <View style={s.card}><T style={{color:C.amber,fontSize:12}}>{error}</T><Button label="Retry exit evidence" onPress={retry} kind="outline"/></View>;
 if(!data)return <View style={{padding:12}}><T style={{fontSize:12,color:C.muted}}>Checking this stock’s exit rules…</T></View>;
 const e=data.evidence;const supported=data.evidence_applies;const test=e.test;
 return <View style={[s.card,{padding:17,backgroundColor:'#0A1D20',borderColor:'#20423F'}]}>
  <View style={[s.between,{flexWrap:'wrap'}]}><T style={{fontFamily:'InterSemi',fontSize:14}}>Your exit plan</T><Badge label={data.label} tone={supported?'green':data.status==='failed'?'red':'amber'}/></View>
  <T style={{fontSize:11,fontFamily:'InterMedium',color:data.tradable_evidence===true?C.green:C.amber}}>{data.tradable_evidence===true?'Evidence below belongs to this exact exit rule':'Illustrative exit rule · no historical stats for this exact rule'}</T>
  <View style={s.row}><Stat label={supported?'Tested risk / reward':'Paper benchmark'} value={`1 : ${data.reward}`} size={24}/><Stat label="Maximum hold" value={data.holding.duration} size={15}/></View>
  <T style={{fontSize:12,color:C.muted,lineHeight:20}}>{data.why}</T>
  <T style={{fontSize:11,color:C.ink}}>{data.stop_basis}. Stop {data.stop_pct.toFixed(2)}% · target {data.target_pct.toFixed(2)}%, before costs.</T>
  {supported?<><View style={s.row}><Stat label="This rule · later net avg. / trade" value={pct(data.rule_metrics.expectancy_pct)} size={20}/><Stat label={`Win rate · ${data.rule_metrics.n} later trades`} value={`${data.rule_metrics.win_rate.toFixed(0)}%`} size={20}/></View><T style={{fontSize:11,color:C.amber}}>{SELECTION_NOTE}</T></>:<View style={{gap:4,borderTopWidth:1,borderColor:C.line,paddingTop:8}}><T style={{fontSize:11,fontFamily:'InterMedium',color:C.amber}}>{data.history_label||'Pattern hold-period history, not this exit rule'}</T><T style={{fontSize:11,color:C.muted}}>{e.baseline_n} baseline trades used a fixed holding-period exit. Their average and win rate do not describe this stop, target and hold.</T></View>}
  {data.tradable_evidence!==true&&!!data.tradable_reason&&<T style={{fontSize:11,color:C.amber}}>{data.tradable_reason}</T>}
  {!data.usable&&<T style={{fontSize:12,color:C.red}}>The stop is too wide or the target is invalid for this planner. Review the setup; the levels have not been clipped to fit.</T>}
  <T style={{fontSize:10,color:C.muted}}>{PRICE_ADJUSTMENT_NOTE}</T>
  {!compact&&<><Button label={expanded?'Hide exit evidence':'Why this exit plan?'} kind="ghost" onPress={()=>setExpanded(!expanded)}/>{expanded&&<View style={{gap:12}}>
   <T style={{fontSize:11,color:C.muted}}>The original research compared {e.candidates_tested} entry, volatility-stop, target and holding combinations for this stock only. The chosen rule was frozen before its later test. Support also requires positive results in both halves of that test and an uncertainty check adjusted for the search.</T>
   {e.selected_rule&&<View style={{gap:6}}><T style={{fontFamily:'InterMedium',fontSize:12}}>Frozen candidate: {e.selected_description}</T><T style={{fontSize:11,color:C.muted}}>{test.n||0} later trades · {pct(test.expectancy_pct)} average · {test.win_rate==null?'—':test.win_rate.toFixed(0)+'%'} won</T><T style={{fontSize:10,color:C.amber}}>{SELECTION_NOTE}{e.search_adjusted_lower_pct!=null?` Search-adjusted lower bound (this stock’s ${e.candidates_tested} candidates only, not other stocks): ${pct(e.search_adjusted_lower_pct)}.`:''}</T><T style={{fontSize:10,color:C.muted}}>Later period halves: {e.period_means.map((v:any)=>pct(v)).join(' / ')}. A negative or inconclusive later test cannot be replaced by another candidate chosen on that same test.</T></View>}
   {!!e.candidates.length&&<><T style={{fontSize:12,fontFamily:'InterSemi'}}>Earlier candidates · training results only</T>{e.candidates.map((r:any)=><View key={r.id} style={{gap:3,borderTopWidth:1,borderColor:C.line,paddingTop:8}}><T style={{fontSize:11}}>{r.rule.stop_atr?`1:${r.rule.target_r} · ${r.rule.stop_atr}× candle range stop`:'Time exit without a stop/target'} · {r.rule.hold} candles</T><T style={{fontSize:10,color:C.muted}}>{r.train.n} training trades · {pct(r.train.expectancy_pct)} net average</T></View>)}</>}
   <T style={{fontSize:10,color:C.muted}}>The minimum-trades browsing filter (default {MIN_TRADES_DEFAULT}) remains independent of exit validation. Structural benchmarks are not mined rules. Cash sizing and stops must be refreshed at entry. Run {data.run}.</T>
  </View>}</>}
 </View>;
}
