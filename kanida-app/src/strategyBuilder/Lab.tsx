// K07/K10 The Lab (slice 6). Two questions, never mixed:
//   Replay       how did THESE contracts behave? - real traded prices, gaps skipped, gross of costs
//   Backtest     would this RULE have made money? - MODEL-PRICED (Black-Scholes on NIFTY with India VIX), decision at
//                the close, entry at the next open, costs and slippage on every fill, closed trades only, a
//                discovery / out-of-sample split and a random-entry control. Expectancy with a 95% CI decides; win
//                rate is context. The badge reads the out-of-sample lower bound and never says more than 'Model-tested'.
import React,{useCallback,useEffect,useMemo,useState} from 'react';
import {Platform,TextInput,View,useWindowDimensions} from 'react-native';
import Svg,{Line,Path,Rect} from 'react-native-svg';
import {router,useLocalSearchParams} from 'expo-router';
import {Badge,Button,C,Chip,Loading,T,s} from '../ui';
import {lab,sb,type LabRun,type LabStats,type Replay,type Template} from './api';
import {inr,istEpoch,num,signed} from './format';

const WEEKDAYS:[string,string][]=[['0','Mon'],['1','Tue'],['2','Wed'],['3','Thu'],['4','Fri'],['daily','Every day']];
const msg=(e:any)=>e?.message||'KANIDA could not complete that.';

export function LabPage(){
 const p=useLocalSearchParams();const strategyId=String(p.strategy||'');
 const {width}=useWindowDimensions();const wide=width>=1000;
 const [mode,setMode]=useState<'backtest'|'replay'|'experiments'>(String(p.mode||'')==='replay'&&strategyId?'replay':String(p.mode||'')==='experiments'?'experiments':'backtest');
 return <View style={{padding:wide?24:14,gap:16,maxWidth:1200,width:'100%',alignSelf:'center'}}>
  <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
   <Button label={strategyId?'Back to strategy':'My Strategies'} icon="chevron-left" kind="outline" onPress={()=>router.replace((strategyId?{pathname:'/strategies',params:{id:strategyId}}:'/strategies') as any)}/></View>
  <View style={{gap:4}}><T style={{fontFamily:'ManropeBold',fontSize:22}}>Lab</T>
   <T style={{fontSize:13,color:C.muted}}>Replay shows how exact contracts traded. A backtest asks whether a rule would have made money - with modelled option prices, costs on every fill, and an out-of-sample check.</T></View>
  <View style={[s.row,{gap:8}]}><Chip label="Rule backtest (model-priced)" active={mode==='backtest'} onPress={()=>setMode('backtest')}/>
   <Chip label="Replay this strategy's contracts" active={mode==='replay'} onPress={()=>strategyId&&setMode('replay')}/>
   <Chip label="Experiments (pre-registered grids)" active={mode==='experiments'} onPress={()=>setMode('experiments')}/>
   {!strategyId&&<T style={{fontSize:11,color:C.muted}}>Open the Lab from a strategy to replay its contracts.</T>}</View>
  {mode==='experiments'?<Experiments/>:mode==='backtest'?<Backtest strategyId={strategyId} preset={{template:String(p.template||''),param:p.param!=null&&p.param!==''?Number(p.param):undefined,
   underlying:p.underlying?String(p.underlying):undefined,lots:p.lots?Number(p.lots):undefined,structure:p.structure?String(p.structure):undefined}}/>:<ReplayView strategyId={strategyId}/>}
 </View>;
}

function Backtest({strategyId,preset}:{strategyId:string;preset:{template:string;param?:number;underlying?:string;lots?:number;structure?:string}}){
 const [tpls,setTpls]=useState<Template[]>([]);const [tpl,setTpl]=useState(preset.template||'iron_condor');const [param,setParam]=useState<number|null>(preset.param??null);
 const [universe,setUniverse]=useState<string[]>(['NIFTY']);const [und,setUnd]=useState('NIFTY');
 useEffect(()=>{lab.universe().then(r=>{setUniverse(r.underlyings.length?r.underlyings:['NIFTY']);if(preset.underlying&&r.underlyings.includes(preset.underlying))setUnd(preset.underlying);}).catch(()=>{});},[]);// eslint-disable-line react-hooks/exhaustive-deps
 const [weekday,setWeekday]=useState('2');const [f,setF]=useState<Record<string,string>>({dte_min:'1',dte_max:'7',target_pct:'',stop_pct:'',exit_dte:'',from:'2016-01-01',to:'',split:'',slippage_pct:'0.5'});
 const [run,setRun]=useState<LabRun|null>(null);const [error,setError]=useState('');const [history,setHistory]=useState<LabRun[]>([]);
 const [adj,setAdj]=useState({rule:'',k:'2',trigger_pct:'0.3'});
 useEffect(()=>{sb.templates().then(r=>setTpls(r.templates)).catch(()=>{});lab.runs(strategyId||undefined).then(r=>setHistory(r.runs)).catch(()=>{});},[strategyId]);
 const t=tpls.find(x=>x.key===tpl);
 useEffect(()=>{if(t&&t.param&&(param==null||!t.param.variants.includes(param)))setParam(t.param.default);if(t&&!t.param)setParam(null);},[t]);// eslint-disable-line react-hooks/exhaustive-deps
 // GTM audit P06: what the draft was vs what this rule will test, stated before Run (never a silent substitution)
 const fromDraft=!!preset.template;const paramSame=preset.param==null||preset.param===param;const undOk=!preset.underlying||preset.underlying===und;
 const mapping=fromDraft?[`From your draft: ${preset.underlying||'NIFTY'} ${preset.structure||preset.template.replace(/_/g,' ')}${preset.param!=null?` (${t?.param?.label||'width'} ${preset.param})`:''}${preset.lots?`, ${preset.lots} lot${preset.lots>1?'s':''}`:''}.`,
  `This test: ${und} ${t?.name||tpl}${param!=null?` (${t?.param?.label||'width'} ${param})`:''}, one set per trade at the current lot size - results scale linearly with lots.`,
  ...(!paramSame?[`The ${t?.param?.label||'width'} differs from your draft.`]:[]),...(preset.underlying&&!universe.includes(preset.underlying)?[`${preset.underlying} is not in the Lab's universe, so it is tested on ${und}.`]:!undOk?['The underlying differs from your draft.']:[])]:[];
 useEffect(()=>{if(!run||run.status!=='running')return;const id=setInterval(()=>lab.run(run.id).then(r=>{setRun(r);if(r.status!=='running')lab.runs(strategyId||undefined).then(x=>setHistory(x.runs));}).catch(e=>setError(msg(e))),700);return()=>clearInterval(id);},[run,strategyId]);
 async function start(){setError('');
  try{const body:any={template:tpl,param,weekday,underlying:und,strategy_id:strategyId||undefined};
   for(const [k,v] of Object.entries(f))if(v.trim()!=='')body[k]=['from','to','split'].includes(k)?v:Number(v);
   if(adj.rule)body.adjust={rule:adj.rule,k:ADJ_K.includes(adj.rule)?Number(adj.k):null,trigger_pct:Number(adj.trigger_pct)};
   setRun(await lab.start(body));}catch(e:any){setError(msg(e));}}
 const field=(k:string,label:string,w=90,ph='')=><View style={{gap:3}}><T style={{fontSize:11,color:C.muted}}>{label}</T>
  <TextInput value={f[k]} onChangeText={v=>setF(x=>({...x,[k]:v}))} placeholder={ph} placeholderTextColor={C.muted} accessibilityLabel={label}
   style={{width:w,height:34,borderWidth:1,borderColor:C.line,borderRadius:8,paddingHorizontal:8,color:C.ink,fontFamily:'Inter',fontSize:12,backgroundColor:C.bg}}/></View>;
 return <View style={{gap:14}}>
  <View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:16,gap:12}}>
   <T style={{fontSize:12,color:C.muted}}>{`${universe.length>1?`NIFTY and ${universe.length-1} F&O stocks (today's list - past members that left are not included)`:'NIFTY'}. Option prices are modelled from the underlying and India VIX (stocks: scaled volatility) - no traded option history this deep exists here.`}</T>
   {universe.length>1&&<View style={[s.row,{flexWrap:'wrap',gap:6,alignItems:'center'}]}><T style={{fontSize:11,color:C.muted}}>Underlying</T>
    {(universe.includes(und)&&und!=='NIFTY'?['NIFTY',und]:['NIFTY']).map(x=><Chip key={x} label={x} active={und===x} onPress={()=>setUnd(x)}/>)}
    <TextInput defaultValue={und==='NIFTY'?'':und} onChangeText={v=>{const x=v.trim().toUpperCase();if(!x)setUnd('NIFTY');else if(universe.includes(x))setUnd(x);}} placeholder="Stock symbol" placeholderTextColor={C.muted} accessibilityLabel="Stock symbol for the Lab"
     autoCapitalize="characters" style={{width:130,height:32,borderWidth:1,borderColor:C.line,borderRadius:8,paddingHorizontal:8,color:C.ink,fontFamily:'Inter',fontSize:12,backgroundColor:C.bg}}/></View>}
   {mapping.length>0&&<View style={{backgroundColor:mapping.length>2?C.amberBg:C.soft,borderRadius:10,padding:10,gap:2}}>{mapping.map((m,i)=><T key={i} style={{fontSize:12,color:mapping.length>2&&i>1?C.amber:C.ink}}>{m}</T>)}</View>}
   <View style={[s.row,{flexWrap:'wrap',gap:6}]}>{tpls.map(x=><Chip key={x.key} label={x.name+(x.risk==='unhedged'?' ⚠':'')} active={tpl===x.key} onPress={()=>setTpl(x.key)}/>)}</View>
   {t?.param&&<View style={[s.row,{gap:6,flexWrap:'wrap'}]}><T style={{fontSize:11,color:C.muted}}>{t.param.label}</T>{t.param.variants.map(v=><Chip key={v} label={String(v)} active={param===v} onPress={()=>setParam(v)}/>)}</View>}
   {t&&<T style={{fontSize:12,color:C.muted}}>{`Rule: ${t.recipe}`}</T>}
   <View style={[s.row,{gap:6,flexWrap:'wrap'}]}><T style={{fontSize:11,color:C.muted}}>Decide at the close on</T>{WEEKDAYS.map(([k,l])=><Chip key={k} label={l} active={weekday===k} onPress={()=>setWeekday(k)}/>)}</View>
   <View style={[s.row,{gap:12,flexWrap:'wrap',alignItems:'flex-end'}]}>
    {field('dte_min','Min days to expiry')}{field('dte_max','Max days to expiry')}
    {field('target_pct','Take profit at % of max profit',110,'hold')}{field('stop_pct','Stop at % of max loss',110,'hold')}{field('exit_dte','Exit at days to expiry',110,'hold')}
   </View>
   <View style={{gap:6}}>
    <View style={[s.row,{gap:6,flexWrap:'wrap'}]}><T style={{fontSize:11,color:C.muted}}>Adjustment (one per trade)</T>
     {ADJ_RULES.map(([k,l])=><Chip key={k} label={l} active={adj.rule===k} onPress={()=>setAdj(a=>({...a,rule:k}))}/>)}</View>
    {!!adj.rule&&<View style={[s.row,{gap:12,flexWrap:'wrap',alignItems:'flex-end'}]}>
     <AdjField label="Trigger: tested short within % of spot (at a close)" value={adj.trigger_pct} onChange={v=>setAdj(a=>({...a,trigger_pct:v}))}/>
     {ADJ_K.includes(adj.rule)&&<AdjField label="Strikes (k)" value={adj.k} onChange={v=>setAdj(a=>({...a,k:v}))}/>}
     <T style={{fontSize:11,color:C.muted,maxWidth:420}}>Applied at the next open with slippage and charges. Tested with hold-to-expiry or a time exit only, paired with the same rule without the adjustment.</T></View>}
   </View>
   <View style={[s.row,{gap:12,flexWrap:'wrap',alignItems:'flex-end'}]}>
    {field('from','First decision date',120)}{field('to','Last decision date',120,'latest')}{field('split','Out-of-sample from',120,'midpoint')}{field('slippage_pct','Slippage % (min 0.5)')}
    <Button label="Run backtest" icon="play" onPress={start} loading={run?.status==='running'}/>
   </View>
   <T style={{fontSize:11,color:C.muted}}>The dates bound the DECISION days. A trade decided on the last decision date is held to its own exit, which can fall after that date (up to its expiry).</T>
   {!!error&&<T style={{color:C.red,fontSize:12}}>{error}</T>}
   {run?.status==='running'&&<View style={{gap:4}}><View style={{height:6,backgroundColor:C.line,borderRadius:3}}><View style={{height:6,width:`${Math.round(run.progress*100)}%`,backgroundColor:C.green,borderRadius:3}}/></View>
    <T style={{fontSize:11,color:C.muted}}>{`Running · ${Math.round(run.progress*100)}% (simulation, bootstrap, random-entry control)`}</T></View>}
   {run?.status==='failed'&&<T style={{color:C.red,fontSize:12}}>{`Run failed: ${run.error}`}</T>}
  </View>
  {run?.status==='completed'&&run.result&&<Result run={run}/>}
  {history.length>0&&<View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:14,gap:6}}>
   <T style={{fontFamily:'InterSemi'}}>Earlier runs</T>
   {history.map(h=><View key={h.id} style={[s.between,{borderTopWidth:1,borderColor:C.line,paddingTop:6,flexWrap:'wrap'}]}>
    <T style={{fontSize:12,flex:1}}>{`${h.spec.template}${h.spec.param!=null?` (${h.spec.param})`:''} · ${h.spec.from}→${h.spec.to} · ${istEpoch(h.created_at)}`}</T>
    <T style={{fontSize:11,color:h.result?.badge.status==='model_positive'?C.green:C.muted}}>{h.status==='completed'?h.result?.badge.label:h.status}</T>
    {h.evidence&&<Badge label={h.evidence.status==='tested_significant'?`Tested ✓ (${h.evidence.tests} rule${h.evidence.tests===1?'':'s'} corrected)`:h.evidence.status==='tested_not_significant'?`Not significant (p=${h.evidence.p?.toFixed(3)}, ${h.evidence.tests} rules)`:`not a test (n=${h.evidence.n_oos} < 30)`} tone={h.evidence.status==='tested_significant'?'green':'neutral'}/>}
    {h.status==='completed'&&<Button label="Open" kind="outline" onPress={()=>lab.run(h.id).then(setRun)}/>}
   </View>)}</View>}
 </View>;
}

function statRow(label:string,st:LabStats){
 return [label,String(st.n||0),signed(st.expectancy),st.ci95?`${signed(st.ci95[0])} to ${signed(st.ci95[1])}`:'—',st.per_100_capital!=null?`₹${num(st.per_100_capital,2)}`:'—',
  signed(st.total),signed(st.max_drawdown),signed(st.worst),st.win_rate!=null?`${st.win_rate}%`:'—',st.avg_hold_days!=null?`${st.avg_hold_days}d`:'—'];
}
function Result({run}:{run:LabRun}){
 const r=run.result!;const pv=r.provenance||{};
 const tone=r.badge.status==='model_positive'?'green':r.badge.status==='insufficient'?'amber':'neutral';
 const csv=()=>{if(Platform.OS!=='web'||!r.trades)return;const head='decision,entry,exit,expiry,reason,spot_entry,vix,legs,gross,fees,net,capital_at_risk\n';
  const rows=r.trades.map((t:any)=>[t.decision,t.entry,t.exit,t.expiry,t.reason,t.spot_entry,t.vix,'"'+(t.legs||[]).map((l:any)=>`${l.side}${l.strike}${l.type}@${l.entry}->${l.exit}`).join(' ')+'"',t.gross,t.fees,t.net,t.capital_at_risk??''].join(',')).join('\n');
  const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([head+rows],{type:'text/csv'}));a.download=`kanida-backtest-${run.id}.csv`;a.click();};
 return <View style={{gap:12}}>
  <View style={{backgroundColor:C.amberBg,borderRadius:10,padding:12,gap:4}}>
   <T style={{color:C.amber,fontFamily:'InterSemi'}}>{pv.label||'Model-priced'}</T>
   <T style={{color:C.amber,fontSize:12}}>Option prices are Black-Scholes values from the NIFTY 50 daily series and India VIX. They are not prices anyone traded at, and one volatility for every strike misprices the wings.</T></View>
  <View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:16,gap:12}}>
   <View style={[s.row,{gap:10,flexWrap:'wrap'}]}><Badge label={r.badge.label} tone={tone as any}/>
    <T style={{fontSize:12,color:C.muted}}>{`${run.spec.template}${run.spec.param!=null?` · ${run.spec.param}`:''} · ${run.spec.from}→${run.spec.to} · out-of-sample from ${run.spec.split} · lot ${r.lot_size}`}</T></View>
   <View>{[['','Trades','Expectancy / trade','95% CI','Per ₹100 at risk','Total','Max drawdown','Worst','Win rate','Avg hold'],statRow('Discovery',r.stats.discovery),statRow('Out-of-sample',r.stats.oos),statRow('All',r.stats.all)]
    .map((row,i)=><View key={i} style={[s.row,{gap:6,borderTopWidth:i?1:0,borderColor:C.line,paddingVertical:6}]}>{row.map((c,j)=><T key={j} style={{flex:j===3?2.2:j?1:1.3,fontSize:i?12:10,color:i?(j===0?C.muted:C.ink):C.muted,
     fontFamily:i===2&&j<4?'InterSemi':'Inter',textAlign:j?'right':'left',fontVariant:['tabular-nums'] as any}}>{c}</T>)}</View>)}</View>
   <T style={{fontSize:11,color:C.muted}}>Expectancy after charges and slippage decides; win rate is context only. The badge reads only the out-of-sample 95% lower bound and needs at least 30 out-of-sample trades.</T>
   <T style={{fontSize:12}}>{`Random-entry control (${r.control.reps} runs of the same rule on random days): mean expectancy ${signed(r.control.mean_expectancy)} per trade; this rule beat ${r.control.actual_percentile??'—'}% of them.${r.control.oos_mean_expectancy!=null?` Out of sample only: control ${signed(r.control.oos_mean_expectancy)}; this rule beat ${r.control.oos_actual_percentile??'—'}%.`:''}`}</T>
   {r.adjustment&&<AdjustmentBlock a={r.adjustment}/>}
   {r.equity&&r.equity.length>1&&<Equity points={r.equity}/>}
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><Button label="Download trades (CSV)" icon="download" kind="outline" onPress={csv}/>
    {r.skipped&&<T style={{fontSize:11,color:C.muted}}>{`Skipped: ${Object.entries(r.skipped).map(([k,v])=>`${v} ${k.replace(/_/g,' ')}`).join(' · ')}`}</T>}</View>
  </View>
  {r.trades&&<View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:14,gap:4}}>
   <T style={{fontFamily:'InterSemi'}}>{`Last ${Math.min(25,r.trades.length)} of ${r.trades.length} trades`}</T>
   {[['Entry','Exit','Why','Legs','Net'],...r.trades.slice(-25).reverse().map((t:any)=>[t.entry,t.exit,t.reason,(t.legs||[]).map((l:any)=>`${l.side}${num(l.strike,0)}${l.type}`).join(' ')+(t.adjustment?.applied?` · adj ${t.adjustment.day}`:''),signed(t.net)])]
    .map((row,i)=><View key={i} style={[s.row,{gap:6,borderTopWidth:i?1:0,borderColor:C.line,paddingVertical:4}]}>{row.map((c,j)=><T key={j} style={{flex:j===3?3:1,fontSize:i?11:10,color:i?C.ink:C.muted,textAlign:j===4?'right':'left',fontVariant:['tabular-nums'] as any}}>{c}</T>)}</View>)}
  </View>}
  <View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:14,gap:4}}>
   <T style={{fontFamily:'InterSemi'}}>Data and assumptions</T>
   {pv.underlying&&Object.entries(pv.underlying).filter(([,v])=>v).map(([k,v]:any)=><T key={k} style={{fontSize:11,color:C.muted}}>{`NIFTY 50 daily from ${k}: ${v[0]} → ${v[1]} (${v[2]} days)`}</T>)}
   {pv.volatility&&<T style={{fontSize:11,color:C.muted}}>{`Volatility: ${pv.volatility.series}`}</T>}
   {pv.expiry_calendar&&<T style={{fontSize:11,color:C.muted}}>{pv.expiry_calendar}</T>}
   {(pv.assumptions||[]).map((a:string,i:number)=><T key={i} style={{fontSize:11,color:C.muted}}>{`• ${a}`}</T>)}
  </View>
 </View>;
}

function Equity({points}:{points:{day:string;equity:number;split:string}[]}){
 const [w,setW]=useState(700);const H=180;
 const ys=points.map(p=>p.equity);const lo=Math.min(0,...ys),hi=Math.max(0,...ys);
 const X=(i:number)=>40+i/(points.length-1)*(w-50);const Y=(v:number)=>10+(hi-v)/((hi-lo)||1)*(H-20);
 const split=points.findIndex(p=>p.split==='oos');
 const path=(from:number,to:number)=>points.slice(from,to).map((p,i)=>`${i?'L':'M'}${X(from+i).toFixed(1)},${Y(p.equity).toFixed(1)}`).join('');
 return <View onLayout={e=>setW(e.nativeEvent.layout.width)} accessibilityLabel="Cumulative net P&L of the closed trades, discovery then out-of-sample">
  <Svg width={w} height={H}>
   {split>0&&<Rect x={X(split)} y={10} width={w-10-X(split)} height={H-20} fill="#39E5A30D"/>}
   <Line x1={40} x2={w-10} y1={Y(0)} y2={Y(0)} stroke={C.muted}/>
   <Path d={path(0,split>0?split+1:points.length)} stroke={C.muted} strokeWidth={1.6} fill="none"/>
   {split>=0&&<Path d={path(Math.max(0,split),points.length)} stroke={C.green} strokeWidth={1.8} fill="none"/>}
  </Svg>
  <T style={{fontSize:11,color:C.muted}}>Cumulative net P&L (₹) of closed trades · grey = discovery, green = out-of-sample (shaded)</T>
 </View>;
}

function ReplayView({strategyId}:{strategyId:string}){
 const [interval,setInterval_]=useState('15minute');const [days,setDays]=useState(5);const [r,setR]=useState<Replay|null>(null);const [busy,setBusy]=useState(false);const [error,setError]=useState('');
 const go=useCallback(async()=>{setBusy(true);setError('');try{setR(await lab.replay(strategyId,interval,days));}catch(e:any){setError(msg(e));}finally{setBusy(false);}},[strategyId,interval,days]);
 useEffect(()=>{go();},[]);// eslint-disable-line react-hooks/exhaustive-deps
 const [w,setW]=useState(700);const H=200;
 const pts=r?.points||[];const ys=pts.map(p=>p.pnl);const lo=Math.min(0,...ys),hi=Math.max(0,...ys);
 const X=(i:number)=>40+i/Math.max(1,pts.length-1)*(w-50);const Y=(v:number)=>10+(hi-v)/((hi-lo)||1)*(H-20);
 return <View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:16,gap:12}}>
  <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>{[['5minute','5 min'],['15minute','15 min'],['60minute','1 hour'],['day','Daily']].map(([k,l])=><Chip key={k} label={l} active={interval===k} onPress={()=>setInterval_(k)}/>)}
   {[2,5,10,30].map(d=><Chip key={d} label={`${d} days`} active={days===d} onPress={()=>setDays(d)}/>)}<Button label="Replay" icon="rotate-ccw" loading={busy} onPress={go}/></View>
  {!!error&&<T style={{color:C.red,fontSize:12}}>{error}</T>}
  {!r?<Loading/>:<>
   <T style={{fontSize:12,color:C.muted}}>{`${r.source==='kite_historical'?'Zerodha Kite historical candles':'Captured 15-minute candles (db/derivatives.db)'} · ${r.legs.map(l=>l.label).join(', ')}`}</T>
   {!pts.length?<T style={{color:C.amber}}>No bar where every leg traded in this window. Nothing is filled in.</T>:<>
    <View onLayout={e=>setW(e.nativeEvent.layout.width)}><Svg width={w} height={H}><Line x1={40} x2={w-10} y1={Y(0)} y2={Y(0)} stroke={C.muted}/>
     <Path d={pts.map((p,i)=>`${i?'L':'M'}${X(i).toFixed(1)},${Y(p.pnl).toFixed(1)}`).join('')} stroke={C.green} strokeWidth={1.8} fill="none"/></Svg></View>
    <View style={[s.row,{gap:18,flexWrap:'wrap'}]}>
     {[['Entered at',`${r.entry_at?.slice(0,16)} IST`],['Now / last',signed(r.last)],['Best',signed(r.best)],['Worst',signed(r.worst)],['Bars',String(pts.length)],['Bars skipped (a leg missing)',String(r.skipped_bars)]]
      .map(([k,v])=><View key={k} style={{gap:1}}><T style={{fontSize:11,color:C.muted}}>{k}</T><T style={{fontSize:14,fontFamily:'InterSemi'}}>{v}</T></View>)}
    </View>
    <T style={{fontSize:11,color:C.muted}}>{r.note}</T></>}
  </>}
 </View>;
}

const ADJ_RULES:[string,string][]=[['','None'],['roll_tested_short','Roll tested short'],['add_hedge_wing','Add hedge wing'],['close_tested_side','Close tested side'],['reduce_half','Halve'],['close_all','Close all']];
const ADJ_K=['roll_tested_short','add_hedge_wing'];
function AdjField({label,value,onChange}:{label:string;value:string;onChange:(v:string)=>void}){return <View style={{gap:3}}><T style={{fontSize:11,color:C.muted}}>{label}</T>
 <TextInput value={value} onChangeText={onChange} accessibilityLabel={label} keyboardType="decimal-pad" style={{width:110,height:34,borderWidth:1,borderColor:C.line,borderRadius:8,paddingHorizontal:8,color:C.ink,fontFamily:'Inter',fontSize:12,backgroundColor:C.bg}}/></View>;}
function AdjustmentBlock({a}:{a:any}){
 const tone=({adjust_helped:'green',adjust_hurt:'red',adjust_not_significant:'neutral',insufficient:'amber'} as any)[a.badge?.status]||'amber';
 const row=(label:string,x:any)=>[label,String(x?.n??0),x?.n?signed(x.mean):'—',x?.n?`${signed(x.ci95[0])} … ${signed(x.ci95[1])}`:'—',x?.n?`${x.helped} / ${x.hurt}`:'—'];
 const imp=a.improvement_per_triggered_trade||{};
 return <View style={{borderWidth:1,borderColor:C.line,borderRadius:12,padding:12,gap:6}}>
  <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><T style={{fontFamily:'InterSemi'}}>Adjustment vs the same rule without it</T><Badge label={a.badge?.label||'—'} tone={tone}/></View>
  <T style={{fontSize:11,color:C.muted}}>{`${a.rule.rule.replace(/_/g,' ')}${a.rule.k?` +${a.rule.k}`:''} · trigger ${a.rule.trigger_pct}% · triggered in ${a.triggered.all} of ${a.pairs} trades (${a.triggered.oos} out of sample)${a.not_applicable?` · ${a.not_applicable} triggers could not apply`:''}`}</T>
  <View>{[['','Triggered','Improvement / triggered trade','95% CI','Helped / hurt'],row('Discovery',imp.discovery),row('Out-of-sample',imp.oos),row('All',imp.all)]
   .map((rw,i)=><View key={i} style={[s.row,{gap:6,borderTopWidth:i?1:0,borderColor:C.line,paddingVertical:5}]}>{rw.map((c,j)=><T key={j} style={{flex:j===3?2:j?1:1.2,fontSize:i?12:10,color:i?C.ink:C.muted,textAlign:j?'right':'left',fontVariant:['tabular-nums'] as any}}>{c}</T>)}</View>)}</View>
  <T style={{fontSize:11,color:C.muted}}>Paired: each adjusted trade against its twin without the adjustment (same entry and exit days). Untriggered trades are identical, so only triggered trades are compared. The badge reads the out-of-sample 95% interval and needs 30 out-of-sample triggers.</T>
 </View>;
}

const WD=['Mon','Tue','Wed','Thu','Fri'];
const ST:Record<string,[string,any]>={tested_significant:['Tested ✓','green'],tested_not_significant:['Not significant','neutral'],insufficient:['Too few trades','amber']};
function Experiments(){
 const [list,setList]=useState<any[]|null>(null);const [b,setB]=useState<any>(null);const [error,setError]=useState('');const [show,setShow]=useState(60);
 useEffect(()=>{lab.batches().then(r=>{setList(r.batches);if(r.batches[0])lab.batch(r.batches[0].id).then(setB).catch(e=>setError(msg(e)));}).catch(e=>setError(msg(e)));},[]);
 if(error)return <T style={{color:C.red}}>{error}</T>;
 if(!list)return <Loading/>;
 if(!list.length)return <T style={{color:C.muted,fontSize:12}}>No experiment batch has run yet.</T>;
 return <View style={{gap:12}}>
  <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>{list.map(x=><Chip key={x.id} label={`${x.name} · ${x.planned} rules`} active={b?.id===x.id} onPress={()=>lab.batch(x.id).then(setB)}/>)}</View>
  {b&&<View style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:14,gap:8}}>
   <T style={{fontFamily:'InterSemi',fontSize:15}}>{b.name}</T>
   <T style={{fontSize:12,color:C.muted}}>{b.grid.why}</T>
   <T style={{fontSize:12}}>{`Pre-registered ${istEpoch(b.created_at)} · plan hash ${b.plan_hash.slice(0,12)} · ${b.planned} rules planned, ${b.done} completed, ${b.failed} failed · ${b.grid.underlying} ${b.grid.from}→${b.grid.to||'latest'}, out of sample from ${b.grid.split}, slippage ${b.grid.slippage_pct}%`}</T>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>{Object.entries(b.counts).map(([k,v])=><Badge key={k} label={`${ST[k][0]}: ${v}`} tone={ST[k][1]}/>)}</View>
   <T style={{fontSize:11,color:C.muted}}>{`Every rule of the batch is listed. Each is one hypothesis in the ${b.grid.underlying} family (${b.family?.tests??0} rules tested in your Lab), corrected together with Benjamini-Hochberg at FDR 10%, with Johnson's skew-adjusted test and a tail stress for rarely-seen maximum losses. Model-priced (India VIX, no skew).`}</T>
   <View>{[['Status','Structure','Decide','DTE','OOS n','Mean ₹/trade','p','95% low','Why'],...b.rules.slice(0,show).map((r:any)=>[ST[r.status][0],`${r.template.replace(/_/g,' ')}${r.param!=null?` ${r.param}`:''}`,r.weekday==='daily'?'daily':WD[r.weekday],`${r.dte[0]}-${r.dte[1]}`,String(r.n_oos),r.mean_oos==null?'—':signed(r.mean_oos),r.p==null?'—':r.p.toFixed(4),r.low==null?'—':`${r.low>0?'+':''}${r.low.toFixed(2)}`,r.reason?String(r.reason).replace(/_/g,' '):''])]
    .map((row:string[],i:number)=><View key={i} style={[s.row,{gap:6,borderTopWidth:i?1:0,borderColor:C.line,paddingVertical:4}]}>{row.map((c:string,j:number)=><T key={j} style={{flex:[1.1,1.8,.6,.6,.6,1,.7,.8,1.2][j],fontSize:i?11:10,color:i?(j===0&&c==='Tested ✓'?C.green:C.ink):C.muted,textAlign:j>=4&&j<=7?'right':'left',fontVariant:['tabular-nums'] as any}}>{c}</T>)}</View>)}</View>
   {b.rules.length>show&&<Button label={`Show all ${b.rules.length}`} kind="outline" onPress={()=>setShow(10000)}/>}
   {!!b.quarantine&&<View accessibilityRole="alert" style={{backgroundColor:C.amberBg,borderRadius:10,padding:10,gap:4}}>
    <T style={{fontSize:12,color:C.amber,fontFamily:'InterSemi'}}>Not usable as evidence</T><T style={{fontSize:12,color:C.amber}}>{b.quarantine}</T>
    {(b.failure_summary||[]).map((f:any,i:number)=><T key={i} style={{fontSize:11,color:C.muted}}>{`${f.count.toLocaleString('en-IN')} × ${f.reason}`}</T>)}
    {b.failure_kinds>3&&<T style={{fontSize:11,color:C.muted}}>{`…and ${b.failure_kinds-3} other kinds of failure. Full diagnostics stay on the server.`}</T>}</View>}
  </View>}
 </View>;
}
