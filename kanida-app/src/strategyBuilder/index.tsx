// KANIDA Strategy Builder (docs/strategy_builder_study/BUILD_PLAN_MERGED.md). One route, /strategies:
//   /strategies                 My Strategies + start (K01/K02)
//   /strategies?id=<id>         the Build workspace (K05-K09)
//   /strategies?view=discover   Find a strategy (K04)
//   /strategies?view=paper      every paper run, marked at the newest reading (K10 paper)
import React,{useCallback,useEffect,useState} from 'react';
import {View,useWindowDimensions} from 'react-native';
import {router,useLocalSearchParams} from 'expo-router';
import {Badge,Button,C,Empty,Loading,T,s} from '../ui';
import {sb,type PaperRun} from './api';
import {Builder} from './Builder';
import {Discover} from './Discover';
import {Home} from './Home';
import {inr,istStamp,num,signed} from './format';

export function StrategiesTab(){
 const p=useLocalSearchParams();const id=String(p.id||'');const view=String(p.view||'');
 if(id)return <Builder key={id} id={id} openTemplate={p.open==='template'}/>;
 if(view==='discover')return <Discover/>;
 if(view==='paper')return <PaperRuns/>;
 return <Home/>;
}

function PaperRuns(){
 const {width}=useWindowDimensions();const wide=width>=900;
 const [runs,setRuns]=useState<PaperRun[]|null>(null);const [error,setError]=useState('');
 const load=useCallback(()=>sb.paperList().then(r=>setRuns(r.runs)).catch(e=>setError(e.message)),[]);
 useEffect(()=>{load();},[load]);
 return <View style={{padding:wide?24:14,gap:16,maxWidth:1100,width:'100%',alignSelf:'center'}}>
  <View style={[s.row,{gap:8}]}><Button label="My Strategies" icon="chevron-left" kind="outline" onPress={()=>router.replace('/strategies' as any)}/></View>
  <View style={{gap:4}}><T style={{fontFamily:'ManropeBold',fontSize:22}}>Paper runs</T>
   <T style={{fontSize:13,color:C.muted}}>Simulated ledgers only. Fills are last traded prices moved against you by slippage, with estimated charges. No order was ever sent to a broker.</T></View>
  {!!error&&<T style={{color:C.red}}>{error}</T>}
  {runs===null?<Loading/>:!runs.length?<Empty icon="play" title="No paper runs yet" detail="Open a strategy and choose Paper trade to record simulated fills against a frozen snapshot."/>:
   runs.map(r=><View key={r.id} style={{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:14,gap:8}}>
    <View style={[s.between,{flexWrap:'wrap'}]}>
     <View style={[s.row,{gap:8,flexWrap:'wrap'}]}><Badge label={r.status==='open'?'PAPER · OPEN':'PAPER · CLOSED'} tone={r.status==='open'?'green':'neutral'}/>
      <T style={{fontFamily:'InterSemi'}}>{r.strategy_name||'Strategy'}</T><T style={{fontSize:12,color:C.muted}}>{`snapshot #${r.revision.n} "${r.revision.name}"`}</T></View>
     <View style={[s.row,{gap:8}]}><Button label="Open strategy" kind="outline" onPress={()=>router.push({pathname:'/strategies',params:{id:r.strategy_id}} as any)}/>
      {r.status==='open'&&<Button label="Close at stored reading" kind="outline" onPress={async()=>{try{await sb.paperClose(r.id);load();}catch(e:any){setError(e.message);}}}/>}</View>
    </View>
    <T style={{fontSize:12,color:C.muted}}>{`Opened ${istStamp(r.opened_reading)}${r.closed_reading?` · closed ${istStamp(r.closed_reading)}`:` · marked at ${istStamp(r.as_of)}`}`}</T>
    <View>{[['Leg','Units','Entry fill','Exit / mark','P&L'],...r.rows.map(x=>[x.label,String(x.units),num(x.entry),x.exit!=null?`${num(x.exit)} exit`:x.mark!=null?`${num(x.mark)} mark`:'—',signed(x.pnl)])]
     .map((row,i)=><View key={i} style={[s.row,{gap:8,borderTopWidth:i?1:0,borderColor:C.line,paddingVertical:5}]}>{row.map((c,j)=><T key={j} style={{flex:j?1:2,fontSize:i?12:10,color:i?C.ink:C.muted,textAlign:j?'right':'left',fontVariant:['tabular-nums'] as any}}>{c}</T>)}</View>)}</View>
    <T style={{fontSize:13,fontFamily:'InterSemi',color:(r.net||0)>=0?C.green:C.red}}>{`Net ${signed(r.net)} = realised ${signed(r.realised)} + unrealised ${signed(r.unrealised)} − fees ${inr(r.fees)}`}</T>
    {r.close_now_estimate!=null&&<T style={{fontSize:12,color:C.muted}}>{`If closed now (exit slippage and charges included): ${signed(r.close_now_estimate)}`}</T>}
    {r.warnings.map((w,i)=><T key={i} style={{fontSize:12,color:C.amber}}>{w}</T>)}
   </View>)}
 </View>;
}
