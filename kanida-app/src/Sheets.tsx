import React,{useState,useEffect,useRef} from 'react';
import {View,TextInput,ScrollView} from 'react-native';
import {router} from 'expo-router';
import {useProduct} from './context';
import {C,T,s,Sheet,Button,Chip,Badge,Stat,Loading,Empty,Icon} from './ui';
import {Match,History,bands,filterMatches,initialFilters,histories,api,cellQuery,pct,money,dateText,sizing,sectorName} from './model';
import {PlanReview as PlanContent} from './PlanReview';
import {SAMPLE_MODERATE_MIN,ALL_FRAMES,discoverDefaults,frameMatch,historyScreenOff,appliedFilters,NO_HISTORY_SCREEN_PAIR_NOTE} from './decision';
import {MIN_TRADES_DEFAULT,TIMEFRAMES,ROUND_TRIP_COST_PCT} from './constants';

export function FilterSheet(){
 const {filterOpen,setFilterOpen,filters,setFilters,options,matches,query,state,defaultFilters}=useProduct();
 const safe=defaultFilters||discoverDefaults(),mine:string[]=safe.frames||[];
 const [patternSearch,setPatternSearch]=useState('');
 const [draft,setDraft]=useState(filters),[minimum,setMinimum]=useState(String(MIN_TRADES_DEFAULT));
 useEffect(()=>{if(filterOpen){setDraft(filters);setMinimum(String(filters.minimum));}},[filterOpen]);
 const valid=/^\d+$/.test(minimum)&&Number(minimum)<=100000;
 const value={...draft,minimum:valid?Number(minimum):MIN_TRADES_DEFAULT};
 // The researched detector set has no legacy history behind a match, so the two history-based filters
 // (minimum trades, past average return) cannot run - see decision.historyScreenOff. The preview count is
 // taken WITHOUT them, because that is what the list will actually show, and both controls say so.
 const noHistoryScreen=historyScreenOff(state?.pattern_set);
 const applied=appliedFilters(value,noHistoryScreen);
 const count=filterMatches(matches,applied,query).filter((m:Match)=>frameMatch(m,applied)).length;
 const choose=(key:string,v:any)=>setDraft({...draft,[key]:v});
 return <Sheet visible={filterOpen} title="Make the market yours" subtitle="Choose the history you want to see." onClose={()=>setFilterOpen(false)} footer={<View style={s.row}><Button label="Reset to defaults" kind="outline" onPress={()=>{setDraft(safe);setMinimum(String(safe.minimum))}}/><Button label={`Show ${count} setups`} style={{flex:1}} disabled={!valid&&!noHistoryScreen} onPress={()=>{setFilters(value);setFilterOpen(false)}}/></View>}>
  <T style={s.label}>Patterns · {(draft.patterns||[]).length||'All'}</T><TextInput accessibilityLabel="Search patterns" placeholder="Find any approved pattern" placeholderTextColor={C.muted} value={patternSearch} onChangeText={setPatternSearch} style={s.input}/><View style={[s.row,{flexWrap:'wrap'}]}><Chip label="All patterns" active={!draft.patterns?.length} onPress={()=>choose('patterns',[])}/>{(state?.patterns||[]).filter((p:any)=>p.name.toLowerCase().includes(patternSearch.toLowerCase())).map((p:any)=><Chip key={p.id} label={p.name} active={draft.patterns?.includes(p.id)} onPress={()=>choose('patterns',draft.patterns?.includes(p.id)?draft.patterns.filter((v:string)=>v!==p.id):[...(draft.patterns||[]),p.id])}/>)}</View>
  <T style={s.label}>Past average return per trade</T><View style={[s.row,{flexWrap:'wrap'}]}>{bands.map(([v,l])=><Chip key={v} label={l} active={draft.band===v} onPress={()=>choose('band',v)}/>)}</View>{!noHistoryScreen&&<T style={{fontSize:12,color:C.muted}}>{`After the research’s ${ROUND_TRIP_COST_PCT.toFixed(2)}% assumed costs. These ranges describe past averages, not a promised next return. Range upper bounds belong to the next band, except 10% stays in 5–10%.`}</T>}
  <T style={s.label}>Minimum historical trades</T><TextInput accessibilityLabel="Minimum historical trades" value={minimum} onChangeText={setMinimum} keyboardType="number-pad" style={s.input}/>
  {/* Both controls above read the same absent legacy history, so the reason is said ONCE for the pair. */}
  <T style={{fontSize:12,color:noHistoryScreen?C.amber:valid?(Number(minimum)<SAMPLE_MODERATE_MIN?C.amber:C.muted):C.red}}>{noHistoryScreen?NO_HISTORY_SCREEN_PAIR_NOTE:!valid?'Enter a whole number from 0 to 100,000.':`Active: at least ${Number(minimum)} trades. Default is ${MIN_TRADES_DEFAULT}; samples under ${SAMPLE_MODERATE_MIN} trades are labelled Small sample. Enter 0 to include any sample size.`}</T>
  <T style={s.label}>Timeframe</T><View style={[s.row,{flexWrap:'wrap'}]}>{!!mine.length&&<Chip label={`My timeframes · ${mine.join(' ')}`} active={draft.timeframe==='All'&&!!draft.frames?.length} onPress={()=>setDraft({...draft,timeframe:'All',frames:mine})}/>}{['All',...ALL_FRAMES].map(tf=><Chip key={tf} label={tf==='All'?'All timeframes':tf} active={draft.timeframe===tf&&(tf!=='All'||!draft.frames?.length)} onPress={()=>setDraft({...draft,timeframe:tf,frames:[]})}/>)}</View>
  <T style={s.label}>Direction</T><View style={[s.row,{flexWrap:'wrap'}]}>{[['all','Any direction'],['bullish','Bullish'],['bearish','Bearish'],['neutral','Awaiting direction']].map(([v,l])=><Chip key={v} label={l} active={draft.direction===v} onPress={()=>choose('direction',v)}/>)}</View>
  <T style={s.label}>Market coverage</T><View style={[s.row,{flexWrap:'wrap'}]}><Chip label="All stocks" active={!draft.universe} onPress={()=>choose('universe','')}/>{options?.universes?.map((u:any)=><Chip key={u.value} label={u.label} active={draft.universe===u.value} onPress={()=>choose('universe',u.value)}/>)}</View><T style={{fontSize:12,color:C.muted}}>Index membership uses the supplied July labels; coverage is incomplete. Large, mid and small cap labels are unavailable.</T>
  <T style={s.label}>Sector</T><View style={[s.row,{flexWrap:'wrap'}]}><Chip label="All sectors" active={!draft.sector} onPress={()=>choose('sector','')}/>{Array.from(new Set<string>((options?.sectors||[]).map((x:any)=>sectorName(x.value)))).map(x=><Chip key={x} label={x} active={draft.sector===x} onPress={()=>choose('sector',x)}/>)}</View>
 </Sheet>;
}

export function Rule({n,title,text}:any){return <View style={[s.row,{alignItems:'flex-start'}]}><View style={{width:26,height:26,alignItems:'center',justifyContent:'center',borderRadius:8,backgroundColor:C.soft}}><T style={{fontSize:11,fontFamily:'InterSemi',color:C.green}}>{n}</T></View><View style={{flex:1,gap:5}}><T style={{fontFamily:'InterSemi',fontSize:13}}>{title}</T><T style={{fontSize:12,color:C.muted}}>{text}</T></View></View>;}
function StockContent({stock}:any){
 const {matches,setStock,setDetail}=useProduct();const [data,setData]=useState<any>(null),[error,setError]=useState('');
 useEffect(()=>{let active=true;api('/api/stock?symbol='+encodeURIComponent(stock.symbol)).then(d=>{if(active)setData(d)}).catch(e=>{if(active)setError(e.message)});return()=>{active=false}},[stock.symbol]);
 if(error)return <Empty title="Stock unavailable" detail={error}/>;if(!data)return <Loading/>;
 return <><Badge label={stock.sector} tone="neutral"/><T style={{fontSize:13,color:C.muted}}>Every timeframe, independently scanned. This stock view shows all stored detections, including studies below your current trade-count filter.</T>{TIMEFRAMES.map(tf=>{const frame=data.timeframes?.[tf];const rows=matches.filter((m:Match)=>m.symbol===stock.symbol&&m.timeframe===tf);return <View key={tf} style={s.card}><View style={s.between}><T style={{fontFamily:'ManropeBold',fontSize:22}}>{tf}</T><Badge label={frame?.current?'Current':'Historical'} tone={frame?.current?'green':'amber'}/></View><T style={{fontSize:11,color:C.muted}}>Last candle: {dateText(frame?.last_candle)}</T>{rows.length?rows.map((m:Match)=><View key={m.id} style={{gap:9}}><T style={{fontFamily:'InterSemi'}}>{m.pattern_name}</T><T style={{fontSize:12,color:C.muted}}>{m.history.map(h=>`${h.side}: ${pct(h.reference.display_return_pct)} past average · ${h.reference.n} trades`).join('\n')}</T><Button label={`See ${m.pattern_name} evidence`} kind="outline" onPress={()=>{setStock(null);setDetail(m)}}/></View>):<T style={{color:C.muted,fontSize:13}}>{frame?.status==='scanned'?'No qualified pattern in this stored chart.':'Not enough usable candle history to qualify a pattern.'}</T>}</View>})}</>;
}
export function ProductSheets(){
 const p=useProduct(),{detail,setDetail,plan,setPlan,stock,setStock}=p;
 // Opening a stock's evidence keeps the user's filters and search; Discover pins the selected setup even when it sits outside them.
 useEffect(()=>{if(!detail)return;p.setWorkspace((w:any)=>({...w,selected:detail.id,tab:'history'}));setDetail(null);router.push('/chart');},[detail]);
 return <><FilterSheet/><Sheet visible={!!plan||!!stock} title={plan?'Your AutoTrade plan':stock?.symbol||'Stock'} subtitle={plan?'A clear plan, with you in control.':stock?.company} wide={!plan} onClose={()=>{setDetail(null);setPlan(null);setStock(null)}}>{plan?<PlanContent key={plan.match.id+plan.side} selection={plan}/>:stock?<StockContent key={stock.symbol} stock={stock}/>:null}</Sheet></>;
}
