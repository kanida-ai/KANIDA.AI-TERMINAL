// THE OPTIONS SCREENER (docs/OPTIONS_SCREENER_SPEC.md, docs/OPTIONS_SCREENER_LAYOUT.md).
//
// "Tell KANIDA the market behaviour you want to find" — not "program a traditional screener". The page is three
// things: the scanner list (KANIDA's defaults and the reader's own), the builder (a sentence box and word chips
// that edit ONE structured definition), and the results with their lifecycle. The server owns every meaning:
// what a state word means, what a sentence was understood as, what "Reads as" says, and what matched.
//
// It follows the market: the newest reading is checked once a minute, and when a new 15-minute reading lands
// the results and alerts are read again — no reader has to press refresh to see 11:30 after 11:15.
import React,{useCallback,useEffect,useRef,useState} from 'react';
import {Pressable,ScrollView,TextInput,View} from 'react-native';
import {Badge,Button,C,Checkbox,Empty,Icon,Loading,Sheet,T,s,useLayoutMode} from '../ui';
import {screenerApi,type View as ResultView} from './api';
import {Builder} from './Builder';
import {ResultsPanel} from './Results';
import {newDefinition} from './model';
import type {Alert,Definition,Parsed,Results,Scanner,ScreenerStatus,Vocabulary} from './types';

type Mode={kind:'view';id:string}|{kind:'new'}|{kind:'edit';id:string};
const message=(e:any)=>e?.message||'KANIDA could not complete that.';

function ScannerRow({sc,active,onPress}:{sc:Scanner;active:boolean;onPress:()=>void}){
 const n=sc.counts?.active;
 return <Pressable accessibilityRole="button" accessibilityState={{selected:active}} accessibilityLabel={`${sc.name}${n!=null?`, ${n} active`:''}`}
  onPress={onPress} style={({pressed})=>[s.between,{paddingHorizontal:12,paddingVertical:10,borderRadius:10,
   backgroundColor:active?C.soft:'transparent',opacity:pressed?.7:1,gap:8}]}>
  <View style={[s.row,{gap:8,flex:1}]}>
   {sc.notify.new||sc.notify.ended||sc.notify.changed?<Icon name="bell" size={12} color={C.green}/>:null}
   <T numberOfLines={1} style={{flex:1,fontSize:13,color:active?C.green:C.ink,fontFamily:active?'InterSemi':'Inter'}}>{sc.name}</T>
  </View>
  {n!=null&&<T style={{fontSize:11,color:n?C.green:C.muted}}>{n}</T>}
 </Pressable>;
}

export function ScreenerTab(){
 const layout=useLayoutMode();
 const wide=layout==='desktop';
 const [vocab,setVocab]=useState<Vocabulary|null>(null);
 const [scanners,setScanners]=useState<Scanner[]>([]);
 const [status,setStatus]=useState<ScreenerStatus|null>(null);
 const [unread,setUnread]=useState(0);
 const [bootError,setBootError]=useState('');
 const [mode,setMode]=useState<Mode|null>(null);
 const [draft,setDraft]=useState<Definition>(newDefinition());
 const [text,setText]=useState('');
 const [parsed,setParsed]=useState<Parsed|null>(null);
 const [parsing,setParsing]=useState(false);
 const [source,setSource]=useState<'visual'|'nl'>('visual');
 const [readsAs,setReadsAs]=useState('');
 const [draftError,setDraftError]=useState('');
 const [view,setView]=useState<ResultView>('active');
 const [results,setResults]=useState<Results|null>(null);
 const [loading,setLoading]=useState(false);
 const [resultError,setResultError]=useState('');
 const [saveOpen,setSaveOpen]=useState(false);
 const [saveName,setSaveName]=useState('');
 const [confirmDelete,setConfirmDelete]=useState(false);
 const [alertsOpen,setAlertsOpen]=useState(false);
 const [alerts,setAlerts]=useState<Alert[]>([]);
 const [busy,setBusy]=useState('');
 const [ranFor,setRanFor]=useState('');
 const [toast,setToast]=useState('');
 const seq=useRef(0);
 const asOf=useRef<string|undefined>(undefined);

 const selected=mode&&mode.kind!=='new'?scanners.find(x=>x.id===mode.id)||null:null;

 const loadScanners=useCallback(async()=>{
  const body=await screenerApi.scanners();
  setScanners(body.scanners);setStatus(body.status);setUnread(body.unread);
  return body;
 },[]);

 useEffect(()=>{(async()=>{
  try{
   const [v,body]=await Promise.all([screenerApi.vocabulary(),loadScanners()]);
   setVocab(v);asOf.current=body.status?.as_of;
   const first=body.scanners[0];
   setMode(first?{kind:'view',id:first.id}:{kind:'new'});
  }catch(e){setBootError(message(e))}
 })()},[loadScanners]);

 // --- results: a saved scanner's, or the draft's when "Run now" was pressed -----------------------------------
 const runSaved=useCallback(async(id:string,v:ResultView)=>{
  const mine=++seq.current;setLoading(true);setResultError('');
  try{
   const body=await screenerApi.results(id,v);
   if(mine===seq.current){setResults(body);if(body.scanner)setScanners(list=>list.map(x=>x.id===id?{...x,
    counts:body.counts?{...body.counts,through:body.as_of||'',current:true}:x.counts}:x))}
  }
  catch(e){if(mine===seq.current){setResults(null);setResultError(message(e))}}
  finally{if(mine===seq.current)setLoading(false)}
 },[]);
 const runDraft=useCallback(async(d:Definition,v:ResultView)=>{
  const mine=++seq.current;setLoading(true);setResultError('');
  try{const body=await screenerApi.run(d,v);if(mine===seq.current){setResults(body);setRanFor(JSON.stringify(d))}}
  catch(e){if(mine===seq.current){setResults(null);setResultError(message(e))}}
  finally{if(mine===seq.current)setLoading(false)}
 },[]);

 useEffect(()=>{
  if(mode?.kind==='view'){setResults(null);runSaved(mode.id,view)}
  else{seq.current++;setResults(null);setLoading(false)}
 },[mode,runSaved]);  // eslint-disable-line react-hooks/exhaustive-deps

 const changeView=(v:ResultView)=>{
  setView(v);
  if(mode?.kind==='view')runSaved(mode.id,v);else if(results)runDraft(draft,v);
 };

 // --- "Reads as": the server's sentence for the draft, re-read shortly after each edit -------------------
 useEffect(()=>{
  if(!mode||mode.kind==='view')return;
  const t=setTimeout(()=>{screenerApi.describe(draft).then(r=>{setReadsAs(r.reads_as);setDraftError('')},e=>setDraftError(message(e)))},250);
  return ()=>clearTimeout(t);
 },[draft,mode]);

 // --- follow the market ----------------------------------------------------------------------------------
 useEffect(()=>{
  const timer=setInterval(async()=>{
   try{
    const st=await screenerApi.status();
    if(st.as_of&&st.as_of!==asOf.current){
     asOf.current=st.as_of;setStatus(st);
     await loadScanners();
     if(mode?.kind==='view')runSaved(mode.id,view);
     else if(results)runDraft(draft,view);
    }
   }catch{/* the next minute tries again; a failed poll is not a page error */}
  },60000);
  return ()=>clearInterval(timer);
 },[mode,view,results,draft,loadScanners,runSaved,runDraft]);

 // --- actions ----------------------------------------------------------------------------------------------
 const flash=(t:string)=>{setToast(t);setTimeout(()=>setToast(''),3500)};
 const startNew=()=>{setMode({kind:'new'});setDraft(newDefinition());setText('');setParsed(null);setSource('visual');setView('active')};
 const startEdit=(sc:Scanner)=>{setMode({kind:'edit',id:sc.id});setDraft(sc.definition);setText(sc.nl_text||'');setParsed(null);
  setSource(sc.source==='nl'?'nl':'visual')};
 const parse=async()=>{
  if(!text.trim())return;
  setParsing(true);
  try{const r=await screenerApi.parse(text);setParsed(r);setDraft(r.definition);setReadsAs(r.reads_as);setDraftError('');setSource('nl')}
  catch(e){setParsed(null);setDraftError(message(e))}
  finally{setParsing(false)}
 };
 const editDraft=(d:Definition)=>{setDraft(d);if(parsed)setParsed({...parsed,assumptions:[],unmapped:[]})};
 const save=async()=>{
  setBusy('save');
  try{
   if(mode?.kind==='edit'){
    const sc=await screenerApi.update(mode.id,{definition:draft,nl_text:source==='nl'?text:''});
    await loadScanners();setMode({kind:'view',id:sc.id});flash('Scanner saved.');
   }else{
    const sc=await screenerApi.create(saveName||'My scanner',draft,source,source==='nl'?text:undefined);
    await loadScanners();setSaveOpen(false);setSaveName('');setMode({kind:'view',id:sc.id});flash(`Saved “${sc.name}”.`);
   }
  }catch(e){flash(message(e))}
  finally{setBusy('')}
 };
 const duplicate=async(sc:Scanner)=>{
  setBusy('dup');
  try{const copy=await screenerApi.duplicate(sc.id);await loadScanners();startEdit(copy);flash(`Copied to “${copy.name}” — edit it below.`)}
  catch(e){flash(message(e))}finally{setBusy('')}
 };
 const remove=async(sc:Scanner)=>{
  setBusy('del');
  try{await screenerApi.remove(sc.id);const body=await loadScanners();setConfirmDelete(false);
   setMode(body.scanners[0]?{kind:'view',id:body.scanners[0].id}:{kind:'new'});flash(`Deleted “${sc.name}”.`)}
  catch(e){flash(message(e))}finally{setBusy('')}
 };
 const rename=async(sc:Scanner,name:string)=>{
  try{await screenerApi.update(sc.id,{name});await loadScanners()}catch(e){flash(message(e))}
 };
 const setNotify=async(sc:Scanner,key:'new'|'ended'|'changed',value:boolean)=>{
  try{await screenerApi.notify(sc.id,{...sc.notify,[key]:value});await loadScanners()}catch(e){flash(message(e))}
 };
 const openAlerts=async()=>{
  setAlertsOpen(true);
  try{const r=await screenerApi.alerts();setAlerts(r.alerts);setUnread(r.unread)}catch(e){flash(message(e))}
 };
 const readAll=async()=>{try{const r=await screenerApi.readAlerts();setUnread(r.unread);setAlerts(a=>a.map(x=>({...x,read:1})))}catch(e){flash(message(e))}};

 if(bootError)return <View style={{padding:24}}><Empty icon="alert-circle" title="The screener could not load" detail={bootError}/></View>;
 if(!vocab||!mode)return <Loading/>;

 const defaults=scanners.filter(x=>x.is_default),mine=scanners.filter(x=>!x.is_default);
 const pickScanner=(id:string)=>{setView('active');setMode({kind:'view',id})};

 const rail=wide?<View style={{width:280,gap:14}}>
  <Button label="New scanner" icon="plus" onPress={startNew}/>
  <View style={{gap:2}}>
   <T style={[s.label,{paddingHorizontal:12,marginBottom:4}]}>My scanners</T>
   {mine.length?mine.map(sc=><ScannerRow key={sc.id} sc={sc} active={mode.kind!=='new'&&mode.id===sc.id} onPress={()=>pickScanner(sc.id)}/>)
    :<T style={{fontSize:12,color:C.muted,paddingHorizontal:12}}>None yet — build one, or duplicate a KANIDA scanner.</T>}
  </View>
  <View style={{gap:2}}>
   <T style={[s.label,{paddingHorizontal:12,marginBottom:4}]}>KANIDA scanners</T>
   {defaults.map(sc=><ScannerRow key={sc.id} sc={sc} active={mode.kind!=='new'&&mode.id===sc.id} onPress={()=>pickScanner(sc.id)}/>)}
  </View>
 </View>:<ScrollView horizontal showsHorizontalScrollIndicator={false} style={{flexGrow:0,width:'100%'}}
  contentContainerStyle={{gap:8,paddingVertical:4,alignItems:'center'}}>
  <Button label="New" icon="plus" onPress={startNew}/>
  {[...mine,...defaults].map(sc=><Pressable key={sc.id} accessibilityRole="button" onPress={()=>pickScanner(sc.id)}
   accessibilityState={{selected:mode.kind!=='new'&&mode.id===sc.id}}
   style={{paddingHorizontal:12,paddingVertical:10,borderRadius:10,borderWidth:1,
    borderColor:mode.kind!=='new'&&mode.id===sc.id?C.green:C.line,backgroundColor:mode.kind!=='new'&&mode.id===sc.id?C.soft:C.paper}}>
   <T style={{fontSize:12}}>{sc.name}{sc.counts?` · ${sc.counts.active}`:''}</T></Pressable>)}
 </ScrollView>;

 const header=<View style={[s.between,{flexWrap:'wrap',gap:10}]}>
  <View style={{gap:4,flex:1,minWidth:240}}>
   <T style={{fontFamily:'ManropeBold',fontSize:wide?26:22,letterSpacing:-.5}}>Options Screener</T>
   <T style={{fontSize:13,color:C.muted}}>Describe the behaviour; KANIDA reads it off every 15-minute reading of index and F&O stock options.
    {status?.as_of?` Latest reading ${status.as_of.slice(11,16)}, ${status.session}.`:''}</T>
  </View>
  <Pressable accessibilityRole="button" accessibilityLabel={`Alerts${unread?`, ${unread} unread`:''}`} onPress={openAlerts}
   style={({pressed})=>[s.row,{gap:6,paddingHorizontal:12,paddingVertical:9,borderRadius:10,borderWidth:1,borderColor:C.line,opacity:pressed?.7:1}]}>
   <Icon name="bell" size={15} color={unread?C.green:C.muted}/><T style={{fontSize:12}}>Alerts</T>
   {unread>0&&<Badge label={String(unread)}/>}
  </Pressable>
 </View>;

 const editing=mode.kind!=='view';
 const savedPanel=selected&&mode.kind==='view'?<View style={[s.card,{padding:18,gap:12}]}>
  <View style={[s.between,{flexWrap:'wrap',alignItems:'flex-start'}]}>
   <View style={{gap:4,flex:1,minWidth:220}}>
    <View style={[s.row,{gap:8}]}>
     <Badge label={selected.is_default?'KANIDA SCANNER':selected.source==='nl'?'MINE · FROM A SENTENCE':'MINE'} tone={selected.is_default?'neutral':'green'}/>
    </View>
    {selected.is_default?<T style={{fontFamily:'ManropeBold',fontSize:20}}>{selected.name}</T>
     :<TextInput defaultValue={selected.name} key={selected.id+selected.name} accessibilityLabel="Scanner name"
       onEndEditing={e=>{const v=e.nativeEvent.text.trim();if(v&&v!==selected.name)rename(selected,v)}}
       style={{fontFamily:'ManropeBold',fontSize:20,color:C.ink,padding:0}}/>}
    {!!selected.description&&<T style={{fontSize:13,color:C.muted}}>{selected.description}</T>}
   </View>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
    {!selected.is_default&&<Button label="Edit" icon="edit-2" kind="soft" onPress={()=>startEdit(selected)}/>}
    <Button label={selected.is_default?'Duplicate to edit':'Duplicate'} icon="copy" kind="outline" loading={busy==='dup'} onPress={()=>duplicate(selected)}/>
    {!selected.is_default&&<Button label="Delete" icon="trash-2" kind="outline" onPress={()=>setConfirmDelete(true)}/>}
   </View>
  </View>
  <View style={{padding:12,borderRadius:10,backgroundColor:C.dark,gap:4}}>
   <T style={s.label}>Reads as</T><T style={{fontSize:13,lineHeight:20}}>{selected.reads_as}</T>
   {!!selected.nl_text&&<T style={{fontSize:12,color:C.muted}}>Started from the sentence: “{selected.nl_text}”</T>}
  </View>
  <View style={[s.row,{flexWrap:'wrap',gap:14}]}>
   <T style={{fontSize:12,color:C.muted}}>Notify me when</T>
   <Checkbox checked={selected.notify.new} onChange={v=>setNotify(selected,'new',v)} label="a new instrument matches"/>
   <Checkbox checked={selected.notify.ended} onChange={v=>setNotify(selected,'ended',v)} label="a match ends"/>
   <Checkbox checked={selected.notify.changed} onChange={v=>setNotify(selected,'changed',v)} label="a match strengthens or weakens"/>
  </View>
 </View>:null;

 const builderPanel=editing?<View style={[s.card,{padding:wide?22:16}]}>
  <Builder vocab={vocab} draft={draft} onChange={editDraft} readsAs={readsAs} error={draftError} text={text} onText={setText}
   onParse={parse} parsing={parsing} parsed={parsed} compact={!wide}/>
  <View style={[s.row,{flexWrap:'wrap',gap:10}]}>
   <Button label="Run now" icon="play" onPress={()=>runDraft(draft,view)} disabled={!!draftError} loading={loading&&!results}/>
   {mode.kind==='edit'?<Button label="Save changes" icon="save" kind="soft" loading={busy==='save'} disabled={!!draftError} onPress={save}/>
    :<Button label="Save as…" icon="save" kind="soft" disabled={!!draftError} onPress={()=>setSaveOpen(true)}/>}
   {mode.kind==='edit'&&<Button label="Cancel" kind="outline" onPress={()=>setMode({kind:'view',id:mode.id})}/>}
  </View>
 </View>:null;

 const resultsTitle=mode.kind==='view'?'Matches':'What this scanner finds right now';
 const showResults=mode.kind==='view'||results||loading||resultError;

 return <View style={{flex:1,padding:wide?28:16,gap:18,backgroundColor:C.bg}}>
  {header}
  <View style={{flexDirection:wide?'row':'column',gap:wide?28:14,alignItems:'flex-start'}}>
   {rail}
   <View style={{flex:1,gap:16,width:wide?undefined:'100%',minWidth:0}}>
    {savedPanel}
    {builderPanel}
    {editing&&results&&ranFor&&ranFor!==JSON.stringify(draft)&&<View style={[s.row,{gap:8,padding:12,borderRadius:10,borderWidth:1,borderColor:C.amber}]}>
     <Icon name="refresh-cw" size={14} color={C.amber}/>
     <T style={{flex:1,fontSize:12,color:C.amber}}>The conditions changed since these results — press Run now to see what the new scanner finds.</T>
    </View>}
    {showResults&&<View style={{opacity:editing&&ranFor&&ranFor!==JSON.stringify(draft)?.45:1}}>
     <ResultsPanel body={results} loading={loading} error={resultError} view={view} onView={changeView} title={resultsTitle}/></View>}
   </View>
  </View>
  {!!toast&&<View style={{position:'absolute',bottom:24,alignSelf:'center',backgroundColor:C.paper,borderColor:C.green,borderWidth:1,
   borderRadius:12,paddingHorizontal:16,paddingVertical:10}}><T style={{fontSize:13}}>{toast}</T></View>}

  {saveOpen&&<Sheet visible onClose={()=>setSaveOpen(false)} title="Save scanner" subtitle="Only you can see your scanners"
   footer={<Button label="Save" icon="save" loading={busy==='save'} onPress={save}/>}>
   <TextInput value={saveName} onChangeText={setSaveName} placeholder="Name it — e.g. Bank Nifty call build-up" placeholderTextColor={C.muted}
    accessibilityLabel="Scanner name" maxLength={60} autoFocus style={s.input} onSubmitEditing={save}/>
   <T style={{fontSize:13,lineHeight:20,color:C.muted}}>{readsAs}</T>
  </Sheet>}
  {confirmDelete&&selected&&<Sheet visible onClose={()=>setConfirmDelete(false)} title={`Delete “${selected.name}”?`}
   subtitle="Its alerts stop. This cannot be undone." footer={<View style={[s.row,{gap:10}]}>
    <Button label="Delete" icon="trash-2" loading={busy==='del'} onPress={()=>remove(selected)}/>
    <Button label="Keep it" kind="outline" onPress={()=>setConfirmDelete(false)}/></View>}>
   <T style={{fontSize:13,color:C.muted}}>{selected.reads_as}</T>
  </Sheet>}
  {alertsOpen&&<Sheet visible onClose={()=>setAlertsOpen(false)} title="Alerts" subtitle="Only changes are sent — a match that is still matching never repeats"
   footer={unread?<Button label="Mark all read" icon="check" kind="soft" onPress={readAll}/>:undefined}>
   {!alerts.length?<T style={{color:C.muted}}>No alerts yet. Turn on "Notify me when…" on any scanner.</T>
    :alerts.map(a=><View key={a.id} style={{gap:4,paddingVertical:10,borderBottomWidth:1,borderColor:C.line,opacity:a.read?.65:1}}>
     <View style={[s.row,{gap:8}]}>
      <Badge label={a.kind==='new'?'NEW MATCH':a.kind==='ended'?'ENDED':'CHANGED'} tone={a.kind==='ended'?'neutral':a.kind==='changed'?'amber':'green'}/>
      <T style={{fontSize:11,color:C.muted}}>{a.reading_at.slice(11,16)} · {a.scanner_name}</T>
     </View>
     <T style={{fontFamily:'InterSemi',fontSize:13}}>{a.title}</T>
     <T style={{fontSize:12,lineHeight:18,color:C.muted}}>{a.text}</T>
    </View>)}
  </Sheet>}
 </View>;
}
