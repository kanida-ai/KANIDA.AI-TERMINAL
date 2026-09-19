import React,{useEffect,useState} from 'react';
import {View,ScrollView,TextInput,Pressable,Platform} from 'react-native';
import {router} from 'expo-router';
import {C,T,s,Button,Chip,Checkbox,Sheet,Loading,useLayoutMode} from './ui';
import {useProduct} from './context';
import {api,initialFilters,money,sectorName} from './model';
import {ReplayStudio,Metric,EquityPlot} from './ReplayStudio';
import {DEFAULT_STUDY_START,DEFAULT_STUDY_END,DEFAULT_STUDY_PATTERNS,DEFAULT_STUDY_TIMEFRAMES,DEFAULT_STUDY_UNIVERSE,DEFAULT_CAPITAL,CAPITAL_CHOICES,TIMEFRAMES} from './constants';

// Named defaults (4.3), shared via constants.ts. DEFAULT_START/END mirror the study engine's own fallbacks.
const DEFAULT_START=DEFAULT_STUDY_START,DEFAULT_END=DEFAULT_STUDY_END,DEFAULT_PATTERNS=DEFAULT_STUDY_PATTERNS,DEFAULT_TIMEFRAMES=DEFAULT_STUDY_TIMEFRAMES,DEFAULT_UNIVERSE=DEFAULT_STUDY_UNIVERSE;

function Field({label,value,onChange,date=false}:any){return <View style={{gap:5,flex:1,minWidth:110}}><T style={{fontSize:12,color:C.muted}}>{label}</T>{date&&Platform.OS==='web'?React.createElement('input',{type:'date','aria-label':label,value,onChange:(e:any)=>onChange(e.target.value),style:{color:'#E9F1F5',background:'#0B151C',border:'1px solid #1B2B35',borderRadius:9,minHeight:44,width:'100%',boxSizing:'border-box',padding:'8px',fontSize:14,colorScheme:'dark'}}):<TextInput accessibilityLabel={label} value={String(value)} onChangeText={onChange} placeholder={date?'YYYY-MM-DD':undefined} placeholderTextColor={C.muted} style={[s.input,{fontSize:14}]} keyboardType={date?'default':'decimal-pad'}/>}</View>}
// onBack (Phase 0 shell): inside the workspace dock, "← Back to chart" switches the dock to Discover instead of navigating. Without it the link keeps router.push('/chart').
export function SimulationDesk({onBack}:{onBack?:()=>void}={}){
 const p=useProduct(),desktop=useLayoutMode()==='desktop',draft=p.studyDraft;
 const [method,setMethod]=useState(draft.method||'backtest'),[symbols,setSymbols]=useState<string>((draft.symbols||[]).join(', ')),[universe,setUniverse]=useState(draft.universe??(draft.symbols?.length?'':DEFAULT_UNIVERSE)),[sector,setSector]=useState(draft.sector||'');
 const [patterns,setPatterns]=useState<string[]>(draft.patterns||[...DEFAULT_PATTERNS]),[frames,setFrames]=useState<string[]>(draft.timeframes||[...DEFAULT_TIMEFRAMES]),[patternOpen,setPatternOpen]=useState(false),[patternSearch,setPatternSearch]=useState(''),[sectorOpen,setSectorOpen]=useState(false);
 const [from,setFrom]=useState<string>(draft.start??(draft.origin?'':DEFAULT_START)),[to,setTo]=useState(draft.end||p.state?.source_latest?.slice(0,10)||DEFAULT_END),[capital,setCapital]=useState(String(draft.capital||DEFAULT_CAPITAL)),[maxPositions,setMaxPositions]=useState(String(draft.max_positions??(draft.symbols?.length===1?1:5))),[allocation,setAllocation]=useState(String(draft.allocation_pct??(draft.symbols?.length===1?100:20)));
 const [product,setProduct]=useState(draft.product||'CNC'),[side,setSide]=useState(draft.side||'long'),[trigger,setTrigger]=useState(draft.trigger||'setup'),[hold,setHold]=useState<any>(draft.hold||{'1H':'6','4H':'6','1D':'10','1W':'4'}),[stop,setStop]=useState(String(draft.stop_atr??0)),[target,setTarget]=useState(String(draft.target_r??2)),[risk,setRisk]=useState(String(draft.risk_pct??2)),[fee,setFee]=useState(String(draft.fee_bps??30)),[slip,setSlip]=useState(String(draft.slippage_bps??5)),[reinvest,setReinvest]=useState(draft.reinvest!==false);
 const [training,setTraining]=useState(String(draft.training_months??36)),[test,setTest]=useState(String(draft.test_months??6)),[minimum,setMinimum]=useState(String(draft.minimum_training??20)),[advanced,setAdvanced]=useState(false),[pane,setPane]=useState(draft.jobId?'results':'settings'),[job,setJob]=useState<any>(null),[saved,setSaved]=useState<any[]>([]),[savedOpen,setSavedOpen]=useState(false),[busy,setBusy]=useState(false),[error,setError]=useState(''),[hits,setHits]=useState<any[]>([]),[compareIds,setCompareIds]=useState<string[]>([]),[comparison,setComparison]=useState<any[]>([]),[compareOpen,setCompareOpen]=useState(false),[cancelOpen,setCancelOpen]=useState(false),[canceling,setCanceling]=useState(false),[earliest,setEarliest]=useState<any>(null);
 // Blank start = full available history. The earliest date comes from the server's own replay of this exact cell (settings.start = first stored candle); never guessed.
 const cellSymbols=symbols.split(',').map(v=>v.trim().toUpperCase()).filter(Boolean),cell=cellSymbols.length===1&&frames.length===1&&patterns.length===1?{symbol:cellSymbols[0],timeframe:frames[0],pattern:patterns[0],side:side==='short'?'short':'long'}:null,cellKey=cell?[cell.symbol,cell.timeframe,cell.pattern,cell.side].join('|'):'';
 // The earliest stored candle comes from a replay of the STORED backtest cell. While the scanner runs the
 // researched pattern set that store holds nothing for these pattern ids, so the probe is not made: it can
 // only fail, and the note below says what a blank start means instead of reporting an error.
 const storedReplay=p.state?.pattern_set!=='research';
 useEffect(()=>{if(from||!cell||!storedReplay){setEarliest(null);return}let active=true;setEarliest({key:cellKey,loading:true});api('/api/replay?'+new URLSearchParams({...cell,capital:String(DEFAULT_CAPITAL)})).then((v:any)=>{if(active)setEarliest({key:cellKey,date:v?.settings?.start||'',error:v?.settings?.start?'':'The earliest stored date is unavailable for this selection.'})}).catch((e:any)=>{if(active)setEarliest({key:cellKey,date:'',error:e.message})});return()=>{active=false}},[!from,cellKey]);
 const fullHistory=!from,resolved=earliest?.key===cellKey&&!earliest?.loading,effectiveStart=from||(resolved?earliest.date:'')||'';
 const startNote=!fullHistory?'':!storedReplay?'Blank start = full available history. The earliest stored candle is not available while the scanner runs the researched pattern set.':!cell?'A blank start date needs exactly one stock, pattern and timeframe. Choose a start date for wider studies.':!resolved?'Blank start = full available history. Finding the earliest stored candle…':earliest.date?`Blank start = full available history, from ${earliest.date} (first stored ${cell.timeframe} candle for ${cell.symbol}, same history as the stock replay).`:`${earliest.error} Choose a start date.`;
 const o=draft.origin,originLabel=o?[o.symbol,(p.state?.patterns||[]).find((v:any)=>v.id===o.pattern)?.name||String(o.pattern||'').replaceAll('_',' '),o.timeframe].filter(Boolean).join(' · '):'';
 useEffect(()=>{api('/api/studies').then(setSaved).catch(()=>{});if(draft.jobId)api('/api/studies/'+draft.jobId).then(setJob).catch(()=>{})},[]);
 useEffect(()=>{let active=true;const last=symbols.split(',').pop()?.trim();if(!last||last.length<2){setHits([]);return}const timer=setTimeout(()=>api('/api/stocks?q='+encodeURIComponent(last)).then(v=>{if(active)setHits(v.slice(0,4))}).catch(()=>{}),240);return()=>{active=false;clearTimeout(timer)}},[symbols]);
 useEffect(()=>{if(!job?.id||job.result)return;let alive=true,working=false;let timer:any;async function poll(){if(working)return;working=true;try{const v=await api('/api/studies/'+job.id);if(!alive)return;setJob(v);if(!['queued','running'].includes(v.status)){clearInterval(timer);api('/api/studies').then(setSaved).catch(()=>{});}}catch(e:any){if(alive)setError(e.message)}finally{working=false}}timer=setInterval(poll,2200);poll();return()=>{alive=false;clearInterval(timer)}},[job?.id,!!job?.result]);
 const currentSettings=()=>({agent:'chart',symbols:symbols.split(',').map(s=>s.trim().toUpperCase()).filter(Boolean),universe,sector,patterns,timeframes:frames,method,product,side,trigger,start:effectiveStart,end:to,capital:Number(capital),max_positions:Number(maxPositions),allocation_pct:Number(allocation),risk_pct:Number(risk),stop_atr:Number(stop),target_r:Number(target),fee_bps:Number(fee),slippage_bps:Number(slip),hold:Object.fromEntries(Object.entries(hold).map(([k,v])=>[k,Number(v)])),training_months:Number(training),test_months:Number(test),minimum_training:Number(minimum),reinvest,benchmark:true});
 async function run(){if(!effectiveStart){setError(startNote||'Choose a start date.');return}setBusy(true);setError('');setJob(null);try{const settings=currentSettings();p.setStudyDraft({...settings,origin:draft.origin});const v=await api('/api/studies',settings);setJob(v);p.setStudyDraft({...settings,origin:draft.origin,jobId:v.id});setPane('results')}catch(e:any){setError(e.message)}finally{setBusy(false)}}
 async function cancelStudy(){if(!job)return;const id=job.id;setCanceling(true);try{await api('/api/studies/'+id+'/cancel',{});setJob((j:any)=>j?.id===id?{...j,message:'Cancellation requested…'}:j)}catch(e:any){setError(e.message)}finally{setCanceling(false);setCancelOpen(false)}}
 const toggle=(values:string[],value:string)=>values.includes(value)?values.filter(v=>v!==value):[...values,value];
 function useSettings(v:any){setMethod(v.method);setSymbols(v.symbols.join(', '));setUniverse(v.universe);setSector(v.sector);setPatterns(v.patterns);setFrames(v.timeframes);setFrom(v.start);setTo(v.end);setCapital(String(v.capital));setMaxPositions(String(v.max_positions));setAllocation(String(v.allocation_pct));setProduct(v.product);setSide(v.side);setTrigger(v.trigger);setHold(v.hold);setStop(String(v.stop_atr));setTarget(String(v.target_r));setRisk(String(v.risk_pct));setFee(String(v.fee_bps));setSlip(String(v.slippage_bps));setTraining(String(v.training_months));setTest(String(v.test_months));setMinimum(String(v.minimum_training));setReinvest(v.reinvest);setPane('settings')}
 async function compare(){setError('');try{const results=await Promise.all(compareIds.map(id=>api('/api/studies/'+id)));setComparison(results);setSavedOpen(false);setCompareOpen(true)}catch(e:any){setError(e.message)}}
 function reviewStock(trade:any){const match=p.matches.find((m:any)=>m.symbol===trade.symbol&&m.pattern===trade.pattern&&m.timeframe===trade.timeframe&&m.history.some((h:any)=>h.side===trade.side));if(!match){p.setToast('There is no matching setup in the latest scan. This historical study remains available.');return}p.setPlan({match,side:trade.side,study:{id:job.id,trade_id:trade.id,rule:trade.rule,validation:job.result.validation}})}
 const settingsChanged=!!job?.result&&Object.entries(currentSettings()).some(([key,value])=>{if(key==='symbols'&&!symbols.trim()||key==='patterns'&&!patterns.length)return false;const old=job.settings[key];return JSON.stringify(Array.isArray(value)?[...value].sort():value)!==JSON.stringify(Array.isArray(old)?[...old].sort():old)});
 const names=(p.state?.patterns||[]).filter((v:any)=>patterns.includes(v.id)).map((v:any)=>v.name);
 return <View style={{flex:1,minHeight:0}}><View style={[s.between,{paddingHorizontal:18,paddingVertical:12,borderBottomWidth:1,borderColor:C.line}]}><View style={{flex:1}}><T style={{fontSize:19,fontFamily:'ManropeBold'}}>Simulation</T><T style={{fontSize:12,color:C.muted}}>Chart Agent · one stock or a shared portfolio</T>{!!o&&<Pressable accessibilityRole="link" accessibilityLabel={`Back to ${originLabel} chart`} onPress={()=>onBack?onBack():router.push('/chart')} style={{alignSelf:'flex-start',paddingVertical:4}}><T style={{fontSize:13,fontFamily:'InterSemi',color:C.green}}>← Back to {originLabel} chart</T></Pressable>}</View><Button label="Saved studies" kind="outline" icon="folder" onPress={()=>{api('/api/studies').then(setSaved).catch(()=>{});setSavedOpen(true)}}/></View>
 {!!error&&<T accessibilityRole="alert" style={{fontSize:12,color:C.red,padding:10}}>{error}</T>}{!desktop&&<View style={[s.row,{padding:10}]}><Chip label="Study settings" active={pane==='settings'} onPress={()=>setPane('settings')}/><Chip label="Results & replay" active={pane==='results'} onPress={()=>setPane('results')}/></View>}
 <View style={{flex:1,minHeight:0,flexDirection:desktop?'row':'column'}}>
 {(desktop||pane==='settings')&&<View style={{width:desktop?330:'100%',flex:desktop?undefined:1,minHeight:0,borderRightWidth:1,borderColor:C.line,backgroundColor:C.paper}}><ScrollView showsVerticalScrollIndicator={false} keyboardShouldPersistTaps="handled" contentContainerStyle={{padding:16,gap:16}}>
  <View style={[s.row,{gap:4}]}><Chip label="Backtest" active={method==='backtest'} onPress={()=>setMethod('backtest')}/><Chip label="Walk-forward" active={method==='walkforward'} onPress={()=>setMethod('walkforward')}/></View><T style={{fontSize:12,color:C.muted}}>{method==='walkforward'?'Learn on earlier completed trades, freeze a rule, then test the next period.':'Replay the same declared entry and exit rules throughout the selected dates.'}</T>
  <T style={{fontFamily:'InterSemi'}}>Stocks & universe</T><TextInput accessibilityLabel="Stocks to simulate" value={symbols} onChangeText={setSymbols} placeholder="All selected stocks, or TITAN, BEL…" placeholderTextColor={C.muted} autoCapitalize="characters" style={s.input}/>
  {!!symbols&&hits.map(v=><Pressable key={v.symbol} accessibilityRole="button" accessibilityLabel={`Add ${v.symbol}, ${v.company}`} onPress={()=>{setSymbols([...symbols.split(',').slice(0,-1),v.symbol].map(s=>s.trim()).join(', '));setHits([])}}><T style={{fontSize:12}}>{v.symbol} · {v.company}</T></Pressable>)}
  <View style={[s.row,{flexWrap:'wrap',gap:5}]}><Chip label="All stocks" active={!universe} onPress={()=>setUniverse('')}/>{p.options?.universes?.map((u:any)=><Chip key={u.value} label={u.label} active={universe===u.value} onPress={()=>setUniverse(u.value)}/>)}</View>
  <T style={{fontSize:12,color:C.muted}}>Uses the supplied membership snapshot, not historical index membership. Leave stocks blank to include the whole selected universe.</T>
  <Button label={sector||"All sectors"} kind="outline" icon="layers" onPress={()=>setSectorOpen(true)}/>
  <Button label={`${patterns.length?patterns.length:'All'} patterns · ${names.slice(0,2).join(', ')}`} icon="sliders" kind="outline" onPress={()=>setPatternOpen(true)}/>
  <View style={[s.row,{flexWrap:'wrap',gap:4}]}>{TIMEFRAMES.map(tf=><Chip key={tf} label={tf} active={frames.includes(tf)} onPress={()=>setFrames(frames.length===1&&frames[0]===tf?frames:toggle(frames,tf))}/>)}</View>
  <View style={s.row}><Field label="Start date" value={from} onChange={setFrom} date/><Field label="End date" value={to} onChange={setTo} date/></View>
  {!!startNote&&<T accessibilityLiveRegion="polite" style={{fontSize:12,color:resolved&&!earliest?.date||!cell?C.amber:C.muted}}>{startNote}</T>}{!!o&&!fullHistory&&!!cell&&<Button label="Use full available history" kind="ghost" onPress={()=>setFrom('')}/>}
  <Field label="Starting capital (₹)" value={capital} onChange={setCapital}/><View style={[s.row,{gap:5,flexWrap:'wrap'}]}>{CAPITAL_CHOICES.map(n=><Chip key={n} label={money(n)} active={Number(capital)===n} onPress={()=>setCapital(String(n))}/>)}</View>
  <View style={s.row}><Field label="Maximum positions" value={maxPositions} onChange={setMaxPositions}/><Field label="Allocation per trade (%)" value={allocation} onChange={setAllocation}/></View>
  <T style={{fontFamily:'InterSemi'}}>Execution model</T><View style={[s.row,{flexWrap:'wrap',gap:4}]}><Chip label="CNC delivery" active={product==='CNC'} onPress={()=>{setProduct('CNC');setSide('long')}}/><Chip label="MIS intraday" active={product==='MIS'} onPress={()=>{setProduct('MIS');if(frames.some(tf=>tf==='1D'||tf==='1W'))setFrames(['1H'])}}/></View><View style={[s.row,{flexWrap:'wrap',gap:4}]}>{(product==='MIS'?['long','short','both']:['long']).map(v=><Chip key={v} label={v==='both'?'Both directions':v==='long'?'Long':'Short'} active={side===v} onPress={()=>setSide(v)}/>)}</View><T style={{fontSize:12,color:C.muted}}>{product==='CNC'?'Unleveraged cash-equity buying. Overnight holds allowed.':'Unleveraged intraday simulation. Exit by the last completed candle at or before 15:15.'} NRML needs actual derivative contracts and history.</T>
  <Button label={advanced?'Hide detailed rules':'Entry, exits, costs & learning'} kind="ghost" icon="settings" onPress={()=>setAdvanced(!advanced)}/>
  {advanced&&<>
   <View style={s.row}><Chip label="Qualified setup" active={trigger==='setup'} onPress={()=>setTrigger('setup')}/><Chip label="Breakout" active={trigger==='confirmed'} onPress={()=>setTrigger('confirmed')}/></View>
   <T style={{fontSize:12,color:C.muted}}>Entry is at the next candle open, with the chosen slippage.</T>
   {method==='backtest'?<>
    {frames.map(tf=><Field key={tf} label={`${tf} maximum holding candles`} value={hold[tf]} onChange={(v:string)=>setHold({...hold,[tf]:v})}/>)}
    <Field label="Stop distance (ATR; 0 = time exit)" value={stop} onChange={setStop}/>
    <Field label="Target in multiples of stop" value={target} onChange={setTarget}/>
   </>:<T style={{fontSize:12,color:C.muted}}>Holding periods and stop/target rules are learned separately for each stock from a frozen candidate family. Inspect the chosen rule in each walk-forward period.</T>}
   <Field label="Risk per trade with a stop (%)" value={risk} onChange={setRisk}/>
   <Field label="Round-trip fees (basis points)" value={fee} onChange={setFee}/>
   <Field label="Slippage each side (basis points)" value={slip} onChange={setSlip}/>
   <T style={{fontSize:12,color:C.muted}}>100 basis points = 1%. These are declared cost assumptions, not broker-specific charges.</T>
   <Checkbox checked={reinvest} onChange={setReinvest} label="Reinvest available capital"/>
  </>}
  {method==='walkforward'&&<><Field label="Learning window (months)" value={training} onChange={setTraining}/><Field label="Next test window (months)" value={test} onChange={setTest}/><Field label="Minimum completed learning trades" value={minimum} onChange={setMinimum}/><T style={{fontSize:12,color:C.muted}}>A separate learning threshold, not your Discover history filter. Cells without a positive supported rule do not open new trades. The engine tests its declared holding/stop/target family per stock.</T></>}
  {!!error&&<T accessibilityRole="alert" style={{color:C.red}}>{error}</T>}
 </ScrollView><View style={{padding:14,borderTopWidth:1,borderColor:C.line}}><Button label={method==='walkforward'?'Run walk-forward study':'Run historical simulation'} icon="play" loading={busy} onPress={run}/></View></View>}
 {(desktop||pane==='results')&&<View style={{flex:1,minWidth:0,minHeight:0,padding:desktop?18:12}}>
  {job?.result?<>
   {settingsChanged&&desktop&&<T style={{fontSize:12,color:C.amber,paddingBottom:8}}>Settings changed. These results belong to the saved study; run again to test your changes.</T>}
   {desktop&&<View style={[s.between,{flexWrap:'wrap'}]}>
    <T style={{fontSize:12,color:C.muted}}>{job.settings.symbols.length} stocks · {job.settings.start} → {job.settings.end} · {job.settings.product}</T>
    <Button label="Edit these settings" kind="ghost" onPress={()=>useSettings(job.settings)}/>
   </View>}
   <ReplayStudio key={job.id} result={job.result} jobId={job.id} actionLabel="Discover matching setups" onPlan={reviewStock} onTest={()=>{
    p.setFilters({...initialFilters,patterns:job.settings.patterns,timeframe:job.settings.timeframes.length===1?job.settings.timeframes[0]:'All',universe:job.settings.universe});
    p.setQuery(job.settings.symbols.length===1?job.settings.symbols[0]:'');router.push('/chart')}}/>
  </>:job?<View style={{padding:20,gap:16}}>
   <T style={{fontSize:22,fontFamily:'ManropeBold'}}>{job.status==='running'||job.status==='queued'?'Replaying your selected market':job.status==='cancelled'?'Study cancelled':'Study needs attention'}</T>
   <T>{job.message}</T>
   {['error','cancelled','interrupted'].includes(job.status)&&<Button label="Review saved settings and retry" kind="outline" onPress={()=>useSettings(job.settings)}/>}
   <View style={{height:6,backgroundColor:C.line,borderRadius:6}}><View style={{height:6,backgroundColor:C.green,width:`${Math.min(100,100*job.done/Math.max(1,job.total))}%`,borderRadius:6}}/></View>
   <T style={s.muted}>{job.done} / {job.total} stock–timeframe histories prepared. You can continue exploring while this runs.</T>
   {['queued','running'].includes(job.status)&&<Button label="Cancel study" kind="outline" onPress={()=>setCancelOpen(true)}/>}
  </View>:<View style={{padding:24,gap:20,justifyContent:'center',flex:1,maxWidth:650,alignSelf:'center'}}>
   <T style={{fontSize:27,fontFamily:'ManropeBold',lineHeight:35}}>What would your money have done?</T>
   <T>Choose a stock or a universe, select the patterns and dates, and replay one shared account.</T>
   <View style={{gap:9,borderLeftWidth:2,borderColor:C.green,paddingLeft:16}}>
    <T>{names.join(' · ')||'All approved patterns'}</T>
    <T style={s.muted}>{frames.join(' · ')} · {symbols||p.options?.universes?.find((u:any)=>u.value===universe)?.label||'All stocks'}</T>
    <T>{money(Number(capital))} · {effectiveStart||(fullHistory?'Full available history':'Start date needed')} → {to}{fullHistory&&!!effectiveStart&&' · full available history'}</T>
   </View>
   <T style={{fontSize:12,color:C.muted}}>Results include actual historical pattern drawings, costs, skipped trades, portfolio drawdown and the selected stock’s contribution.</T>
   {!desktop&&<Button label="Configure study" onPress={()=>setPane('settings')}/>}
  </View>}
 </View>}
 </View>
<Sheet visible={cancelOpen} title="Cancel this study?" onClose={()=>setCancelOpen(false)}><T style={{fontSize:16,lineHeight:26}}>The replay stops before any results are produced. The study's settings stay in Saved studies so you can review them and run again.</T><Button label="Cancel study" loading={canceling} disabled={!job||!['queued','running'].includes(job.status)} onPress={cancelStudy}/><Button label="Keep running" kind="outline" onPress={()=>setCancelOpen(false)}/></Sheet>
 <Sheet visible={sectorOpen} title="Choose a sector" onClose={()=>setSectorOpen(false)}><Chip label="All sectors" active={!sector} onPress={()=>{setSector('');setSectorOpen(false)}}/>{Array.from(new Set<string>((p.options?.sectors||[]).map((x:any)=>sectorName(x.value)))).map(x=><Chip key={x} label={x} active={sector===x} onPress={()=>{setSector(x);setSectorOpen(false)}}/>)}</Sheet>
 <Sheet visible={patternOpen} title="Patterns to test" subtitle="Each detector runs independently on each selected timeframe." onClose={()=>setPatternOpen(false)} footer={<Button label="Use these patterns" onPress={()=>setPatternOpen(false)}/>}><TextInput accessibilityLabel="Find pattern to simulate" value={patternSearch} onChangeText={setPatternSearch} style={s.input} placeholder="Search all approved patterns" placeholderTextColor={C.muted}/><Button label="Select all patterns" kind="outline" onPress={()=>setPatterns(p.state.patterns.map((v:any)=>v.id))}/>{p.state?.patterns?.filter((v:any)=>v.name.toLowerCase().includes(patternSearch.toLowerCase())).map((v:any)=><Chip key={v.id} label={v.name} active={patterns.includes(v.id)} onPress={()=>setPatterns(toggle(patterns,v.id))}/>)}<T style={{fontSize:12,color:C.muted}}>No individual selection means all approved patterns.</T></Sheet>
 <Sheet visible={savedOpen} title="Saved studies" subtitle="Every run retains its settings and evidence version." footer={<Button label={compareIds.length===2?'Compare selected studies':'Select two studies to compare'} disabled={compareIds.length!==2} onPress={compare}/>}  onClose={()=>setSavedOpen(false)}>
  {saved.length?saved.map(v=><View key={v.id} style={{gap:4}}>
   {v.status==='complete'&&<Checkbox checked={compareIds.includes(v.id)} label={`Compare study ${v.id.slice(0,6)}`} onChange={()=>setCompareIds(compareIds.includes(v.id)?compareIds.filter(id=>id!==v.id):[...compareIds.slice(-1),v.id])}><T style={{fontSize:12,color:C.green}}>Compare · {v.id.slice(0,6)}</T></Checkbox>}
   <Pressable accessibilityRole="button" accessibilityLabel={`Open saved ${v.settings.method} study`} onPress={()=>{useSettings(v.settings);setJob(v);p.setStudyDraft({...v.settings,origin:draft.origin,jobId:v.id});setPane('results');setSavedOpen(false)}} style={{gap:6,paddingVertical:16,borderBottomWidth:1,borderColor:C.line}}>
    <T>{v.settings.name||`${v.settings.symbols.length===1?v.settings.symbols[0]:v.settings.symbols.length+' stocks'} · ${v.settings.method==='walkforward'?'Walk-forward':'Backtest'}`}</T>
    <T style={{fontSize:12,color:C.muted}}>{v.settings.timeframes.join(', ')} · {v.settings.start} → {v.settings.end} · {v.status}</T>
   </Pressable>
  </View>):<T>Your completed and running studies will appear here.</T>}
 </Sheet>
 <Sheet visible={compareOpen} title="Compare saved studies" subtitle="Different dates, stock selections or rules change the comparison. Repeatedly choosing winners from these results is research, not an untouched test." wide onClose={()=>setCompareOpen(false)}>
  {comparison.length===2&&<>
   <EquityPlot normalized curve={comparison[0].result.curve.map((v:any)=>({...v,equity:10000*v.equity/comparison[0].settings.capital}))} benchmark={comparison[1].result.curve.map((v:any)=>({...v,equity:10000*v.equity/comparison[1].settings.capital}))}/>
   <T style={{fontSize:12,color:C.muted}}>Both curves rebased to ₹10,000. Solid: first study. Dashed: second. Dates use the same calendar scale.</T>
  </>}
  {comparison.map((v,i)=><View key={v.id} style={s.card}>
   <T style={{fontFamily:'InterSemi'}}>Study {i+1} · {v.id.slice(0,6)} · {v.settings.method}</T>
   <T style={{fontSize:12,color:C.muted}}>{v.settings.symbols.join(', ')} · {v.settings.timeframes.join(', ')} · {v.settings.start} → {v.settings.end}</T>
   <T style={{fontSize:12,color:C.muted}}>{v.settings.patterns.join(', ')} · {v.settings.product} · {v.settings.trigger}</T>
   <View style={[s.row,{flexWrap:'wrap',gap:15}]}>
    <Metric label="Starting capital" value={money(v.settings.capital)}/>
    <Metric label="Ending equity" value={money(v.result.summary.ending_equity)}/>
    <Metric label="Average per trade" value={`${v.result.summary.average_return_pct?.toFixed(2)??'—'}%`}/>
    <Metric label="Wins / trades" value={`${v.result.summary.wins} / ${v.result.summary.trades}`}/>
    <Metric label="Equity decline" value={`${v.result.summary.max_drawdown_pct.toFixed(2)}%`}/>
   </View>
  </View>)}
 </Sheet>
 </View>;
}

