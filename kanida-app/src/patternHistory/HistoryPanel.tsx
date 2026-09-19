import React,{useState} from 'react';
import {View,Pressable} from 'react-native';
import {router} from 'expo-router';
import {C,T,Button,Chip,Badge,Stat,Sheet,s} from '../ui';
import {api} from '../model';
import {useKeyedValue} from '../activeSymbol';
import {WidgetError} from '../layout';
import {Skeleton} from '../discover/parts';
import {query,signed,date,HORIZONS,stateLabel,variantLabel,historyHref} from './logic';
import {ReplayChart} from './ReplayChart';
import type {Selection,History,Occurrence} from './types';

const tone=(v:number|null|undefined)=>v==null?C.muted:v>0?C.green:v<0?C.red:C.ink;
function Section({title,children}:{title:string;children:React.ReactNode}){return <View style={{gap:10}}><T style={{fontFamily:'InterSemi',fontSize:14}}>{title}</T>{children}</View>;}
function OccurrenceRow({item,horizon,onOpen}:{item:Occurrence;horizon:number;onOpen:()=>void}){
 const h=item.horizons.find(x=>x.horizon===horizon);
 return <Pressable accessibilityRole="button" accessibilityLabel={`Open occurrence ${date(item.signal_time)}, ${signed(h?.gross_return_pct)} after ${horizon} candles`} onPress={onOpen} style={({pressed})=>({paddingVertical:11,borderBottomWidth:1,borderColor:C.line,gap:4,opacity:pressed?.7:1})}>
  <View style={s.between}><T style={{fontSize:12,fontFamily:'InterSemi'}}>{date(item.signal_time)}</T><T style={{fontSize:13,fontFamily:'InterSemi',color:tone(h?.gross_return_pct)}}>{h?.status==='measured'?`${signed(h.gross_return_pct)}  →`:h?.status==='pending'?'Still unfolding →':'Excluded →'}</T></View>
  {h?.status==='measured'?<T style={{fontSize:11,color:C.muted}}>Largest rise {signed(h.max_up_pct)} · fall {signed(h.max_down_pct==null?null:-h.max_down_pct)}</T>:<T style={{fontSize:11,color:C.muted}}>{h?.reason?.replace(/_/g,' ')}</T>}
 </Pressable>;
}
export function HistoryPanel({selection,compact=false,initialHorizon=5,onHorizonChange}:{selection:Selection;compact?:boolean;initialHorizon?:number;onHorizonChange?:(h:number)=>void}){
 const [horizon,setHorizon]=useState(HORIZONS.includes(initialHorizon)?initialHorizon:5),[replay,setReplay]=useState<string|null>(null),[details,setDetails]=useState(false);
 const chooseHorizon=(h:number)=>{setHorizon(h);setReplay(null);onHorizonChange?.(h)};
 const url='/api/pattern-history/history?'+query(selection,{horizon});
 const {value:data,error,loading,reload}=useKeyedValue<History>(url,async()=>{
  const d=await api(url);
  if(d.symbol!==selection.symbol||d.pattern_id!==selection.pattern_id||d.variant!==selection.variant||d.state!==selection.state||d.selected_horizon!==horizon)throw new Error('History does not match this stock, pattern and window.');
  return d;
 });
 const row=data?.horizons.find(x=>x.horizon===horizon),obs=row?.observed;
 const latest=data?.recent_completed||[],positive=latest.filter(o=>(o.horizons.find(h=>h.horizon===horizon)?.gross_return_pct??0)>0).length;
 return <View style={{gap:compact?15:24}}>
  <View style={{gap:8}}><T style={{fontSize:12,color:C.muted}}>What happened after the pattern?</T><View style={[s.row,{flexWrap:'wrap',gap:5}]}>{HORIZONS.map(h=><Chip key={h} label={`${h} candle${h===1?'':'s'}`} active={h===horizon} onPress={()=>chooseHorizon(h)}/>)}</View></View>
  {error?<WidgetError title="History unavailable" message={error} onRetry={reload}/>:loading||!data||!obs?<Skeleton lines={7}/>:<>
   <View style={[s.row,{flexWrap:'wrap',gap:6}]}><Badge label={`History through ${date(data.data_end)}`} tone="neutral"/>{data.review_required&&<Badge label={data.review_label||'Historical data requires review'} tone="amber"/>}{!data.coverage.complete&&<Badge label={`${data.coverage.cached_symbols}/${data.coverage.universe_symbols} stocks prepared`} tone="amber"/>}</View>
   <View style={{gap:8}}><T style={{fontFamily:'ManropeBold',fontSize:compact?21:30,lineHeight:compact?29:39}}>{obs.n?`${obs.up_n} of ${obs.n} finished higher`:'No completed observations'}</T><T style={{color:C.muted,fontSize:12}}>After {horizon} daily candle{horizon===1?'':'s'} · {obs.down_n} lower · {obs.unchanged_n} unchanged</T><T style={{fontSize:12,color:C.muted}}>{data.occurrence_count} occurrences · Last seen {date(data.last_seen)}</T></View>
   <View style={{flexDirection:'row',flexWrap:'wrap',gap:16}}><Stat label={`Median change · ${horizon} candles`} value={signed(obs.median_gross_return_pct)} color={tone(obs.median_gross_return_pct)} size={compact?20:28}/><Stat label="Typical largest rise" value={signed(obs.median_max_up_pct)} color={C.green} size={compact?20:28}/><Stat label="Typical largest fall" value={signed(obs.median_max_down_pct==null?null:-obs.median_max_down_pct)} color={C.red} size={compact?20:28}/></View>
   <T style={{color:C.muted,fontSize:11,lineHeight:17}}>“Typical” means the median across measured occurrences. Largest rise and fall are extremes inside the window, not captured trading returns.</T>
   {!!obs.n&&obs.median_bars_to_max_up!=null&&<T style={{fontSize:12,color:C.muted}}>Largest rise around candle {obs.median_bars_to_max_up}; largest fall around candle {obs.median_bars_to_max_down} (median timing; 0 means no excursion).</T>}
   {obs.n>0&&obs.n<20&&<T style={{color:C.amber,fontSize:12}}>Only {obs.n} completed cases. Read the individual charts before treating this as a recurring tendency.</T>}
   <View style={{backgroundColor:C.dark,borderWidth:1,borderColor:C.line,borderRadius:12,padding:12,gap:6}}><T style={{fontFamily:'InterSemi',fontSize:12}}>Does this differ from the stock’s usual movement?</T><T style={{fontSize:12}}>Average after pattern: {signed(obs.mean_gross_return_pct)}{compact?'\n':'  ·  '}On all measured dates: {signed(row?.baseline.mean_gross_return_pct)}</T><T style={{fontSize:11,color:C.muted}}>Same stock and {horizon}-candle window · {row?.baseline.n??0} baseline observations. Difference: {row?.mean_difference_pct==null?'—':`${row.mean_difference_pct>0?'+':''}${row.mean_difference_pct.toFixed(2)} percentage points`}.</T>{!compact&&<T style={{fontSize:11,color:C.muted}}>Signal dates {date(data.baseline_window?.[0])} to {date(data.baseline_window?.[1])}. Includes pattern dates; overlapping windows are not independent. This comparison does not establish a predictive edge.</T>}</View>
   {!compact&&<Section title="Compare fixed windows"><View style={{borderWidth:1,borderColor:C.line,borderRadius:12,overflow:'hidden'}}><View style={[s.row,{padding:12,backgroundColor:C.dark}]}>{['Candles','Higher / measured','Median change'].map(t=><T key={t} style={{flex:1,fontSize:11,color:C.muted}}>{t}</T>)}</View>{data.horizons.map(h=><Pressable key={h.horizon} accessibilityRole="button" accessibilityLabel={`Select ${h.horizon} candles`} onPress={()=>chooseHorizon(h.horizon)} style={[s.row,{padding:12,backgroundColor:h.horizon===horizon?C.soft:'transparent'}]}><T style={{flex:1}}>{h.horizon}</T><T style={{flex:1}}>{h.observed.up_n} / {h.observed.n}</T><T style={{flex:1,color:tone(h.observed.median_gross_return_pct)}}>{signed(h.observed.median_gross_return_pct)}</T></Pressable>)}</View><T style={{fontSize:11,color:C.muted}}>These windows are fixed in advance. The largest historical number is not presented as the best future holding period.</T></Section>}
   <Section title={`Latest ${latest.length||5} completed occurrences`}><T style={{fontSize:11,color:C.muted}}>Same stock, variant and state · next open to candle {horizon} close</T>{latest.length?latest.map(item=><OccurrenceRow key={item.id} item={item} horizon={horizon} onOpen={()=>setReplay(item.id)}/>):<T style={{color:C.muted}}>No completed cases for this window.</T>}{!!latest.length&&<T style={{fontSize:12}}>{positive} of these {latest.length} finished higher. Open a row to inspect its chart.</T>}</Section>
   <Section title="Unfinished and excluded"><T style={{fontSize:12,color:C.muted}}>{obs.pending} still unfolding · {obs.quality_excluded} excluded for data quality. Neither is counted as a gain, loss or unchanged result.</T>{(data.recent_pending||[]).map(item=><OccurrenceRow key={item.id} item={item} horizon={horizon} onOpen={()=>setReplay(item.id)}/>)}{(data.recent_excluded||[]).map(item=><OccurrenceRow key={item.id} item={item} horizon={horizon} onOpen={()=>setReplay(item.id)}/>)}</Section>
   {compact?<Button kind="outline" label="Explore full pattern history" icon="arrow-up-right" onPress={()=>router.push(historyHref(selection,horizon) as any)}/>:<>
    <Button kind="ghost" label={details?'Hide measurement details':'How these numbers are measured'} icon="info" onPress={()=>setDetails(v=>!v)}/>
    {details&&<View style={[s.card,{padding:16,gap:10}]}><T>1. Read each stored {stateLabel(selection.state).toLowerCase()} occurrence using the frozen {variantLabel(selection.variant).toLowerCase()}.</T><T>2. Start at the next daily candle’s open. Candle 1 ends at that same candle’s close.</T><T>3. Measure the closing change and the highest / lowest prices over the selected window.</T><T>4. Keep every matching occurrence, including unfavorable outcomes. Report unfinished and quality-excluded cases separately.</T><T>5. Compare with the same stock over all daily signal dates from its first through last occurrence.</T><T style={{color:C.muted,fontSize:12}}>History available: {date(data.data_start)} to {date(data.data_end)}. Fixed snapshot; “still unfolding” refers to its data cutoff, not today’s market. No model fitting, trading costs or executed trades are part of this view. Forming history includes setups that never confirmed.</T><T selectable style={{color:C.muted,fontSize:11}}>Research run {data.run} · Source {data.source_run}</T></View>}
   </>}
   <T style={{fontSize:11,color:C.muted}}>Gross historical price movement · data through {date(data.data_end)} · no costs deducted</T>
  </>}
  <Sheet visible={!!replay} onClose={()=>setReplay(null)} title={`${selection.symbol} · historical occurrence`} subtitle={`${stateLabel(selection.state)} · ${horizon} daily candles`} wide>{replay&&<ReplayChart key={query(selection)+replay} selection={selection} id={replay} horizon={horizon}/>}</Sheet>
 </View>;
}
