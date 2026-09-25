import React,{useCallback,useEffect,useRef,useState} from 'react';
import {View,Pressable,ScrollView,TextInput} from 'react-native';
import {router} from 'expo-router';
import {useAuth} from '../Auth';
import {C,T,s,Icon,Button,Badge,Chip,Empty,Loading,Sheet,useLayoutMode} from '../ui';
import {pct,dateText} from '../model';
import {DeskPage} from '../TraderDesk';
import type {Audience,BlockDef,BlockKind,Side,SourcePattern,SourceType,StrategyDef,StrategyResults,StrategyRow,StrategySource,StrategySources} from '../strategies/types';
import {adminApi,patchSupported,type AdminBlock,type AdminRegistry,type StrategyInput,type StrategyPatch} from './adminApi';
// Owner-only strategy registry (FALCON_DISCOVER_SPEC §6). Blocks as sections (enable, reorder, edit, add) · strategies per block (enable, reorder, edit, preview) · Add/Edit strategy form with validation · Preview (found count + top 5 by 95% low).
// Honesty: preview rows keep their evidence basis; anything without a tested exit rule is labelled as such and never presented as evidence. Every write is confirmed; server errors are shown inline where the action was taken.
// BACKLOG item 4: the pattern chips come from /api/admin/strategy-sources — the registry's own accept-list — and
// NEVER from /api/state, which is whichever detector set the scanner happens to be running. Offering the scanner's
// researched ids as `stored_pattern` was the 400 that made this form unusable.
const TIMEFRAMES=['1H','4H','1D','1W'],KINDS:BlockKind[]=['chart','quant','results','options','candlestick','events'];
const SOURCE_LABEL:Record<SourceType,string>={stored_pattern:'Stored scan',research_pattern:'Researched catalogue'};
const CHIP_LIMIT=60;  // the researched catalogue is 107 patterns: the list is searched, never dumped
const SIDE_LABEL:Record<Side,string>={long:'Long (bullish)',short:'Short (bearish)'},AUDIENCE_LABEL:Record<Audience,string>={trader:'Trader',investor:'Investor',both:'Both'};
const BASIS_LABEL:Record<StrategyRow['evidence_basis'],string>={tested_rule:'Tested exit rule (later-test)',hold_period_history:'Pattern hold-period history · not a tested exit rule',none:'Not tested'};
type Draft={source_type:SourceType;block_key:string;key:string;name:string;description:string;pattern:string;variant:string;timeframe:string;side:Side;audience:Audience;tags:string;min_trades:string;default_slot:'A'|'B'|null;enable:boolean};
type StratForm={mode:'add'|'edit';key?:string;draft:Draft;error:string;busy:boolean;tried?:boolean};
type BlockForm={mode:'add'|'edit';key:string;title:string;description:string;kind:BlockKind;error:string;busy:boolean};
type Confirm={title:string;detail:string;label:string;scope:string;run:()=>Promise<unknown>};
type Preview={strategy:StrategyDef;loading:boolean;error:string;data:StrategyResults|null};
const KEY_RE=/^[a-z0-9][a-z0-9_-]{2,63}$/;
const blank=(block_key='',source_type:SourceType='stored_pattern'):Draft=>({source_type,block_key,key:'',name:'',description:'',pattern:'',variant:'',timeframe:'1D',side:'long',audience:'trader',tags:'',min_trades:'10',default_slot:null,enable:false});
const fromDef=(d:StrategyDef):Draft=>({source_type:d.source_type,block_key:d.block_key,key:d.key,name:d.name,description:d.description||'',pattern:d.pattern,variant:(d as {variant?:string}).variant||'',timeframe:d.timeframe,side:d.side,audience:d.audience,tags:(d.tags||[]).join(', '),min_trades:String(d.min_trades??10),default_slot:d.default_slot??null,enable:d.enabled});
/** The half of the accept-list this draft is drawing from. Null while it is still loading. */
export const sourceOf=(all:StrategySources|null,t:SourceType):StrategySource|null=>(t==='research_pattern'?all?.research:all?.stored)||null;
export const patternOf=(all:StrategySources|null,d:{source_type:SourceType;pattern:string}):SourcePattern|null=>
 sourceOf(all,d.source_type)?.patterns.find(p=>p.id===d.pattern)||null;
/** Only the sides this pattern (research: this variant) was actually studied on — the server refuses the rest. */
export function sidesFor(all:StrategySources|null,d:{source_type:SourceType;pattern:string;variant:string}):Side[]{
 const p=patternOf(all,d);if(!p)return [];
 if(d.source_type!=='research_pattern')return p.sides||[];
 return (p.variants.find(v=>v.id===d.variant)||p.variants[0])?.sides||[];
}
const timeframesFor=(all:StrategySources|null,t:SourceType)=>sourceOf(all,t)?.timeframes?.length?sourceOf(all,t)!.timeframes:TIMEFRAMES;
const tagList=(v:string)=>Array.from(new Set(v.split(',').map(x=>x.trim()).filter(Boolean)));
const sorted=<X extends {order:number}>(v:X[])=>[...v].sort((a,b)=>(a.order??0)-(b.order??0));
/** Which source a block is already made of, so "+ Strategy" inside it opens on that half of the accept-list. */
const defaultSource=(b?:AdminBlock):SourceType=>b?.strategies?.[0]?.source_type||'stored_pattern';
const errText=(e:any)=>e?.status===403?'Only the owner can change the strategy registry.':e?.status===404?`Not found on the server${e?.message?`: ${e.message}`:''}.`:e?.message||'The server could not complete this change.';
/** Returns the validation messages for a strategy draft (empty = valid). */
export function validateDraft(d:Draft,ctx:{blocks:AdminBlock[];sources:StrategySources|null;editKey?:string}):string[]{
 const out:string[]=[],name=d.name.trim(),min=Number(d.min_trades),tags=tagList(d.tags);
 if(!ctx.blocks.some(b=>b.key===d.block_key))out.push('Choose a block.');
 if(name.length<3)out.push('Name needs at least 3 characters.');else if(name.length>80)out.push('Name must be 80 characters or fewer.');
 if(d.description.trim().length>300)out.push('Description must be 300 characters or fewer.');
 if(!ctx.editKey){
  // Everything below is checked against the SERVER's accept-list, so the form can never offer a combination the
  // server refuses. While it is still loading nothing is asserted about the pattern — saving is blocked instead.
  const source=sourceOf(ctx.sources,d.source_type),known=patternOf(ctx.sources,d);
  if(!ctx.sources)out.push('The list of patterns you can add has not loaded yet.');
  else if(!source||!source.available)out.push(source?.reason||'That source is not available on this server.');
  if(!d.pattern)out.push('Choose a pattern.');
  else if(ctx.sources&&source?.available&&!known)out.push(`${d.pattern} is not a pattern this server can add.`);
  if(d.source_type==='research_pattern'&&known){
   if(!d.variant)out.push('Choose a variant.');
   else if(!known.variants.some(v=>v.id===d.variant))out.push('That variant was not researched for this pattern.');
  }
  const timeframes=timeframesFor(ctx.sources,d.source_type);
  if(!timeframes.includes(d.timeframe))out.push('Choose a timeframe.');
  if(d.side!=='long'&&d.side!=='short')out.push('Choose a side.');
  else if(known){const sides=sidesFor(ctx.sources,d);if(sides.length&&!sides.includes(d.side))out.push(`${known.name} was researched ${sides.join(' and ')} only.`);}
 }
 if(!['trader','investor','both'].includes(d.audience))out.push('Choose an audience.');
 if(!/^\d+$/.test(d.min_trades.trim())||!Number.isInteger(min)||min<1||min>1000)out.push('Minimum trades must be a whole number from 1 to 1000.');
 if(tags.length>8)out.push('Use at most 8 tags.');if(tags.some(t=>t.length>30))out.push('Each tag must be 30 characters or fewer.');
 const all=ctx.blocks.flatMap(b=>b.strategies),key=d.key.trim();
 if(all.some(x=>x.key!==ctx.editKey&&x.name.trim().toLowerCase()===name.toLowerCase()))out.push('Another strategy already uses this name.');
 if(!ctx.editKey&&key){if(!KEY_RE.test(key))out.push('Key: 3–64 lower-case letters, digits, - or _, starting with a letter or digit (or leave it empty to create it from the name).');else if(all.some(x=>x.key===key))out.push('A strategy with this key already exists. Choose another key or leave it empty.');}
 return out;
}
/** Non-blocking notes for a new strategy draft: same pattern/timeframe/side as an existing strategy in the block is allowed (e.g. a stricter minimum trades), but flagged. */
export function draftWarnings(d:Draft,ctx:{blocks:AdminBlock[];editKey?:string}):string[]{
 if(ctx.editKey||!d.pattern)return [];
 const same=ctx.blocks.find(b=>b.key===d.block_key)?.strategies.find(x=>x.source_type===d.source_type&&x.pattern===d.pattern
  &&x.timeframe===d.timeframe&&x.side===d.side&&(d.source_type!=='research_pattern'||(x as {variant?:string}).variant===d.variant));
 return same?[`Same pattern, timeframe and side as “${same.name}” — results will be identical unless its settings differ.`]:[];
}
function Toggle({on,label,onPress,disabled=false}:{on:boolean;label:string;onPress:()=>void;disabled?:boolean}){
 return <Pressable role="switch" aria-checked={on} accessibilityState={{checked:on,disabled}} accessibilityLabel={label} disabled={disabled} onPress={onPress} style={(st:any)=>[s.row,{gap:8,minHeight:36,paddingHorizontal:4,borderRadius:8,borderWidth:1,borderColor:st.focused?C.green:'transparent',opacity:disabled?.45:st.pressed?.7:1}]}>
  <View style={{width:34,height:20,borderRadius:10,backgroundColor:on?C.green:C.line,padding:2,alignItems:on?'flex-end':'flex-start'}}><View style={{width:16,height:16,borderRadius:8,backgroundColor:on?'#041B12':C.muted}}/></View>
  <T style={{fontSize:12,color:on?C.green:C.muted}}>{on?'Enabled':'Disabled'}</T>
 </Pressable>;
}
function SmallBtn({icon,label,onPress,disabled=false,text}:{icon?:string;label:string;onPress:()=>void;disabled?:boolean;text?:string}){
 return <Pressable accessibilityRole="button" accessibilityLabel={label} disabled={disabled} onPress={onPress} style={(st:any)=>[s.row,{gap:5,minHeight:32,minWidth:32,justifyContent:'center',paddingHorizontal:text?10:6,borderRadius:8,borderWidth:1,borderColor:st.focused?C.green:C.line,backgroundColor:st.hovered?C.soft:C.paper,opacity:disabled?.35:st.pressed?.7:1}]}>
  {!!icon&&<Icon name={icon} size={14} color={C.muted}/>}{!!text&&<T style={{fontSize:12,fontFamily:'InterMedium'}}>{text}</T>}
 </Pressable>;
}
function Label({children}:{children:React.ReactNode}){return <T style={[s.label,{fontSize:10}]}>{children}</T>;}
function Input({label,value,onChangeText,multiline=false,placeholder,keyboardType}:{label:string;value:string;onChangeText:(v:string)=>void;multiline?:boolean;placeholder?:string;keyboardType?:any}){
 return <View style={{gap:6}}><Label>{label}</Label><TextInput accessibilityLabel={label} value={value} onChangeText={onChangeText} multiline={multiline} placeholder={placeholder} placeholderTextColor={C.muted} keyboardType={keyboardType} autoCorrect={false} style={[s.input,{minHeight:multiline?76:44,fontSize:14,paddingVertical:multiline?10:0,textAlignVertical:multiline?'top':'center'}]}/></View>;
}
function Choice<V extends string|null>({label,value,options,onChange,disabled=false}:{label:string;value:V;options:[V,string][];onChange:(v:V)=>void;disabled?:boolean}){
 return <View style={{gap:6}}><Label>{label}</Label><View role="radiogroup" aria-label={label} style={[s.row,{flexWrap:'wrap',gap:6,opacity:disabled?.5:1}]}>{options.map(([v,l])=><Chip key={String(v)} label={l} active={value===v} onPress={disabled?undefined:()=>onChange(v)}/>)}</View></View>;
}
function ErrorLine({text}:{text:string}){if(!text)return null;return <View accessibilityRole="alert" style={[s.row,{gap:8,padding:10,borderRadius:10,backgroundColor:'#2A1519',alignItems:'flex-start'}]}><Icon name="alert-triangle" size={15} color={C.red}/><T style={{flex:1,fontSize:13,lineHeight:19,color:C.red}}>{text}</T></View>;}
/** The pattern chips. The researched catalogue is 107 patterns, so the list is searched rather than dumped, and
 * only ever holds what the server said it will accept. */
function PatternChoice({source,value,disabled,fallbackName,onChange}:{source:StrategySource|null;value:string;disabled:boolean;fallbackName:string;onChange:(id:string)=>void}){
 const [q,setQ]=useState('');
 if(disabled)return <View style={{gap:6}}><Label>Pattern</Label><T style={{fontSize:14}}>{fallbackName||value}</T></View>;
 if(!source)return <View style={{gap:6}}><Label>Pattern</Label><T style={{fontSize:12,color:C.muted}}>Loading what this server can add…</T></View>;
 if(!source.available)return <View style={{gap:6}}><Label>Pattern</Label><T accessibilityRole="alert" style={{fontSize:13,color:C.amber}}>{source.reason||'This source is not available on this server.'}</T></View>;
 const needle=q.trim().toLowerCase();
 const all=source.patterns,hits=needle?all.filter(x=>x.id.toLowerCase().includes(needle)||x.name.toLowerCase().includes(needle)):all;
 const shown=hits.slice(0,CHIP_LIMIT);
 return <View style={{gap:6}}>
  <Label>Pattern</Label>
  {all.length>CHIP_LIMIT&&<TextInput accessibilityLabel="Search patterns" value={q} onChangeText={setQ} placeholder={`Search ${all.length} patterns`} placeholderTextColor={C.muted} autoCorrect={false} style={[s.input,{minHeight:40,fontSize:13}]}/>}
  <View role="radiogroup" aria-label="Pattern" style={[s.row,{flexWrap:'wrap',gap:6}]}>{shown.map(x=><Chip key={x.id} label={x.name} active={value===x.id} onPress={()=>onChange(x.id)}/>)}</View>
  {!hits.length&&<T style={{fontSize:12,color:C.muted}}>No pattern matches “{q.trim()}”.</T>}
  {hits.length>shown.length&&<T style={{fontSize:11,color:C.muted}}>Showing {shown.length} of {hits.length} — keep typing to narrow it down.</T>}
 </View>;
}

export function StrategyAdmin(){
 const auth=useAuth(),phone=useLayoutMode()==='phone';
 const owner=auth.user?.role==='owner';
 const [sources,setSources]=useState<StrategySources|null>(null),[sourcesError,setSourcesError]=useState('');
 const [reg,setReg]=useState<AdminRegistry|null>(null),[loadError,setLoadError]=useState(''),[loading,setLoading]=useState(true);
 const [errors,setErrors]=useState<Record<string,string>>({}),[busy,setBusy]=useState('');
 const [form,setForm]=useState<StratForm|null>(null),[blockForm,setBlockForm]=useState<BlockForm|null>(null),[confirm,setConfirm]=useState<Confirm|null>(null),[preview,setPreview]=useState<Preview|null>(null);
 const load=useCallback(async()=>{setLoading(true);try{const r=await adminApi.registry();setReg({registry_version:r?.registry_version??0,blocks:sorted((r?.blocks||[]).map(b=>({...b,strategies:sorted(b.strategies||[])})))});setLoadError('')}catch(e:any){setLoadError(errText(e))}finally{setLoading(false)}},[]);
 // The accept-list is loaded beside the registry and does not block it: a registry that lists fine while the
 // sources call fails still renders, with the add form saying it cannot offer patterns yet.
 const loadSources=useCallback(async()=>{try{setSources(await adminApi.sources());setSourcesError('')}catch(e:any){setSources(null);setSourcesError(errText(e))}},[]);
 useEffect(()=>{if(owner){load();loadSources()}},[owner,load,loadSources]);
 // Newly added strategy: highlighted and scrolled into view once the registry reloads.
 const [fresh,setFresh]=useState(''),rowEls=useRef<Record<string,any>>({});
 useEffect(()=>{if(!fresh||!reg)return;const t=setTimeout(()=>rowEls.current[fresh]?.scrollIntoView?.({block:'center',behavior:'smooth'}),60);return ()=>clearTimeout(t)},[fresh,reg]);
 const setErr=(scope:string,text:string)=>setErrors(v=>({...v,[scope]:text}));
 async function act(scope:string,fn:()=>Promise<unknown>){setBusy(scope);setErr(scope,'');try{await fn();await load();return true}catch(e:any){setErr(scope,errText(e));return false}finally{setBusy('')}}
 if(!auth.user)return <Loading/>;
 if(!owner)return <DeskPage><Empty icon="lock" title="Only the owner can manage strategies" detail="The strategy registry decides which strategies every member sees in Market scans. Ask the pilot owner if a strategy should be added or changed." action={<Button label="Go to Market scans" kind="outline" onPress={()=>router.push('/discover' as any)}/>}/></DeskPage>;
 const blocks=reg?.blocks||[];
 // Display name for a pattern id, from the accept-list (both halves), falling back to the id itself.
 const patternName=(id:string)=>[...(sources?.stored.patterns||[]),...(sources?.research.patterns||[])].find(x=>x.id===id)?.name||id.replaceAll('_',' ');
 // Reorder = swap the `order` of two neighbours (two PATCHes), then reload. Orders are re-spread first when neighbours share a value.
 function swapPatches<X extends {key:string;order:number}>(list:X[],i:number,j:number){const a=list[i],b=list[j];let oa=a.order??i,ob=b.order??j;if(oa===ob){oa=i;ob=j}return [[a.key,ob],[b.key,oa]] as [string,number][];}
 const moveBlock=(i:number,d:-1|1)=>{const j=i+d;if(j<0||j>=blocks.length)return;const scope='block:'+blocks[i].key;act(scope,async()=>{for(const [key,order] of swapPatches(blocks,i,j))await adminApi.updateBlock(key,{order})})};
 const moveStrategy=(b:AdminBlock,i:number,d:-1|1)=>{const list=b.strategies,j=i+d;if(j<0||j>=list.length)return;act('block:'+b.key,async()=>{for(const [key,order] of swapPatches(list,i,j))await adminApi.updateStrategy(key,{order})})};
 const askToggleBlock=(b:AdminBlock)=>setConfirm({scope:'block:'+b.key,title:b.enabled?`Disable “${b.title}”?`:`Enable “${b.title}”?`,label:b.enabled?'Disable block':'Enable block',detail:b.enabled?'The block and all its strategy cards disappear from Market scans for every member. Strategies inside it keep their own settings.':`The block appears on Market scans for every member, showing its enabled strategies (${b.strategies.filter(x=>x.enabled).length} of ${b.strategies.length}).`,run:()=>adminApi.updateBlock(b.key,{enabled:!b.enabled})});
 const askToggleStrategy=(b:AdminBlock,x:StrategyDef)=>setConfirm({scope:'block:'+b.key,title:x.enabled?`Disable “${x.name}”?`:`Enable “${x.name}”?`,label:x.enabled?'Disable strategy':'Enable strategy',detail:x.enabled?'It disappears from the Market scans picker and from Falcon for every member. Cards that were using it fall back to the block defaults.':`It appears in the Market scans picker${b.enabled?'':' once the block is enabled'} for every member. Preview it first: rows without a tested exit rule are shown as “not tested”, never as evidence.`,run:()=>adminApi.updateStrategy(x.key,{enabled:!x.enabled})});
 async function openPreview(x:StrategyDef){setPreview({strategy:x,loading:true,error:'',data:null});try{const data=await adminApi.preview(x.key);setPreview(v=>v&&v.strategy.key===x.key?{...v,loading:false,data}:v)}catch(e:any){setPreview(v=>v&&v.strategy.key===x.key?{...v,loading:false,error:errText(e)}:v)}}
 // ---- strategy form ----
 const formIssues=form?validateDraft(form.draft,{blocks,sources,editKey:form.key}):[];
 const formWarnings=form?draftWarnings(form.draft,{blocks,editKey:form.key}):[];
 const showIssues=!!form?.tried;
 const setDraft=(patch:Partial<Draft>)=>setForm(f=>f&&{...f,draft:{...f.draft,...patch},error:''});
 const slotClash=form&&form.draft.default_slot?blocks.find(b=>b.key===form.draft.block_key)?.strategies.find(x=>x.key!==form.key&&x.default_slot===form.draft.default_slot):undefined;
 // Choosing a source, a pattern or a variant clears whatever no longer applies and moves the draft to the block
 // the server puts that family in, so the draft can never hold a combination the server would refuse.
 function pickSource(t:SourceType){setForm(f=>{if(!f)return f;const src=sourceOf(sources,t),tfs=src?.timeframes?.length?src.timeframes:TIMEFRAMES;
  const block=src?.block_key&&blocks.some(b=>b.key===src.block_key)?src.block_key:f.draft.block_key;
  return {...f,error:'',draft:{...f.draft,source_type:t,pattern:'',variant:'',side:'long',block_key:block,timeframe:tfs.includes(f.draft.timeframe)?f.draft.timeframe:tfs[0]}};});}
 function pickPattern(id:string){setForm(f=>{if(!f)return f;const d=f.draft;
  const p=sourceOf(sources,d.source_type)?.patterns.find(x=>x.id===id)||null;
  const variant=d.source_type==='research_pattern'?(p?.variants[0]?.id||''):'';
  const sides=sidesFor(sources,{source_type:d.source_type,pattern:id,variant});
  const block=p?.block_key&&blocks.some(b=>b.key===p.block_key)?p.block_key:d.block_key;
  return {...f,error:'',draft:{...d,pattern:id,variant,block_key:block,side:sides.length&&!sides.includes(d.side)?sides[0]:d.side}};});}
 function pickVariant(id:string){setForm(f=>{if(!f)return f;const d=f.draft,sides=sidesFor(sources,{...d,variant:id});
  return {...f,error:'',draft:{...d,variant:id,side:sides.length&&!sides.includes(d.side)?sides[0]:d.side}};});}
 function saveForm(){
  if(!form)return;if(formIssues.length){setForm(f=>f&&{...f,tried:true});return}
  const d=form.draft,tags=tagList(d.tags),min_trades=Number(d.min_trades);
  if(form.mode==='add'){
   // Each source is posted in ITS OWN shape. A researched pattern is named by the catalogue identity
   // (pattern_id + variant + side + timeframe), which is what resolves its evidence cell; a stored one by the
   // scanner pattern id. Sending a researched id as `pattern` is what the server refused with a 400.
   const common={...(d.key.trim()?{key:d.key.trim()}:{}),block_key:d.block_key,name:d.name.trim(),description:d.description.trim(),tags,timeframe:d.timeframe,side:d.side,audience:d.audience,min_trades,default_slot:d.default_slot,enabled:d.enable};
   const body:StrategyInput=d.source_type==='research_pattern'
    ?{...common,source_type:'research_pattern',pattern_id:d.pattern,variant:d.variant}
    :{...common,source_type:'stored_pattern',pattern:d.pattern,pattern_name:patternName(d.pattern)};
   setConfirm({scope:'form',title:`Save “${body.name}”?`,label:d.enable?'Save and enable':'Save as disabled',detail:d.enable?'It is added to the registry and appears in Market scans for every member right away. Strategies without tested evidence are shown as “not tested”.':'It is added to the registry as disabled. Use Preview to check what it finds, then enable it.',run:async()=>{setForm(f=>f&&{...f,busy:true,error:''});try{const saved=await adminApi.createStrategy(body);setForm(null);if(saved?.key){setFresh(saved.key);openPreview(saved)}}catch(e:any){setForm(f=>f&&{...f,busy:false,error:errText(e)});throw e}}});
  }else if(form.key){
   const orig=blocks.flatMap(b=>b.strategies).find(x=>x.key===form.key);const patch:StrategyPatch={};
   if(!orig)return setForm(f=>f&&{...f,error:'This strategy is no longer in the registry. Close the form and refresh.'});
   if(d.name.trim()!==orig.name)patch.name=d.name.trim();if(d.description.trim()!==(orig.description||''))patch.description=d.description.trim();if(tags.join('|')!==(orig.tags||[]).join('|'))patch.tags=tags;
   if(d.audience!==orig.audience)patch.audience=d.audience;if(min_trades!==orig.min_trades)patch.min_trades=min_trades;if(d.default_slot!==(orig.default_slot??null))patch.default_slot=d.default_slot;if(d.block_key!==orig.block_key)patch.block_key=d.block_key;if(d.enable!==orig.enabled)patch.enabled=d.enable;
   if(!Object.keys(patch).length)return setForm(f=>f&&{...f,error:'Nothing has changed.'});
   setConfirm({scope:'form',title:`Save changes to “${orig.name}”?`,label:'Save changes',detail:`Changes: ${Object.keys(patch).join(', ').replace('min_trades','minimum trades').replace('default_slot','default slot').replace('block_key','block')}. ${orig.enabled||patch.enabled?'Members see the change in Market scans right away.':'The strategy stays disabled.'}`,run:async()=>{setForm(f=>f&&{...f,busy:true,error:''});try{await adminApi.updateStrategy(orig.key,patch);setForm(null)}catch(e:any){setForm(f=>f&&{...f,busy:false,error:errText(e)});throw e}}});
  }
 }
 // ---- block form ----
 const blockIssues=blockForm?[...(blockForm.mode==='add'&&!/^[a-z][a-z0-9_]{1,39}$/.test(blockForm.key)?['Key: 2–40 lower-case letters, digits or _, starting with a letter.']:[]),...(blockForm.mode==='add'&&blocks.some(b=>b.key===blockForm.key)?['A block with this key already exists.']:[]),...(blockForm.title.trim().length<2||blockForm.title.trim().length>60?['Title: 2–60 characters.']:[]),...(blockForm.description.trim().length>200?['Description: 200 characters or fewer.']:[])]:[];
 function saveBlock(){
  if(!blockForm)return;if(blockIssues.length)return setBlockForm(f=>f&&{...f,error:blockIssues.join(' ')});
  const bf=blockForm,title=bf.title.trim(),description=bf.description.trim();
  if(bf.mode==='add'){const body:BlockDef={key:bf.key,title,description,kind:bf.kind,order:blocks.length?Math.max(...blocks.map(b=>b.order??0))+1:0,enabled:false};
   setConfirm({scope:'blockform',title:`Add block “${title}”?`,label:'Add block (disabled)',detail:'The block is created disabled, so members see nothing until you add strategies and enable it.',run:async()=>{setBlockForm(f=>f&&{...f,busy:true});try{await adminApi.createBlock(body);setBlockForm(null)}catch(e:any){setBlockForm(f=>f&&{...f,busy:false,error:errText(e)});throw e}}});
  }else{const orig=blocks.find(b=>b.key===bf.key);if(!orig)return;const patch:any={};if(title!==orig.title)patch.title=title;if(description!==(orig.description||''))patch.description=description;if(bf.kind!==orig.kind)patch.kind=bf.kind;if(!Object.keys(patch).length)return setBlockForm(f=>f&&{...f,error:'Nothing has changed.'});
   setConfirm({scope:'blockform',title:`Save “${title}”?`,label:'Save block',detail:orig.enabled?'Members see the new title and description on Market scans right away.':'The block stays disabled.',run:async()=>{setBlockForm(f=>f&&{...f,busy:true});try{await adminApi.updateBlock(orig.key,patch);setBlockForm(null)}catch(e:any){setBlockForm(f=>f&&{...f,busy:false,error:errText(e)});throw e}}});
  }
 }
 async function runConfirm(){const c=confirm;if(!c)return;setConfirm(null);if(c.scope==='form'||c.scope==='blockform'){try{await c.run();await load()}catch{}return}await act(c.scope,c.run)}

 const header=<View style={[s.between,{flexWrap:'wrap',alignItems:'flex-end'}]}>
  <View style={{gap:6,flexShrink:1}}><T style={s.label}>Owner · Strategy registry</T><T role="heading" aria-level={1} style={[s.title,phone&&{fontSize:24,lineHeight:32}]}>Manage strategies</T><T style={{color:C.muted,maxWidth:720}}>Blocks and strategies shown on Market scans and used by Falcon. Changes apply to every member without a code release.</T></View>
  <View style={[s.row,{flexWrap:'wrap'}]}>{reg&&<Badge label={`Registry version ${reg.registry_version}`} tone="neutral"/>}<Button label="Refresh" kind="ghost" icon="refresh-cw" loading={loading&&!!reg} onPress={()=>{load();loadSources()}}/><Button label="Add block" kind="outline" icon="plus" disabled={!reg} onPress={()=>setBlockForm({mode:'add',key:'',title:'',description:'',kind:'quant',error:'',busy:false})}/><Button label="Add strategy" icon="plus" disabled={!blocks.length} onPress={()=>setForm({mode:'add',draft:blank(blocks[0]?.key,defaultSource(blocks[0])),error:'',busy:false})}/></View>
 </View>;
 return <DeskPage>
  {header}
  {!patchSupported&&<View style={[s.row,{padding:12,borderRadius:10,backgroundColor:C.amberBg}]}><Icon name="info" size={15} color={C.amber}/><T style={{flex:1,fontSize:13,color:C.amber}}>Enable, reorder and edit need the web app. You can view the registry, add and preview here.</T></View>}
  {!!loadError&&<View style={{gap:10}}><ErrorLine text={`Strategy registry is unavailable: ${loadError}`}/><Button label="Retry" kind="outline" onPress={load} style={{alignSelf:'flex-start'}}/></View>}
  {loading&&!reg&&!loadError&&<Loading/>}
  {reg&&!blocks.length&&<Empty icon="layers" title="No blocks yet" detail="Add a block (for example Chart Strategies), then add strategies to it." action={<Button label="Add block" onPress={()=>setBlockForm({mode:'add',key:'chart',title:'Chart Strategies',description:'',kind:'chart',error:'',busy:false})}/>}/>}
  {blocks.map((b,bi)=>{const scope='block:'+b.key,working=busy===scope;return <View key={b.key} role="region" aria-label={`Block ${b.title}`} style={[s.card,{padding:phone?14:18,gap:12,opacity:working?.7:1}]}>
   <View style={[s.between,{flexWrap:'wrap',alignItems:'flex-start'}]}>
    <View style={{flex:1,minWidth:220,gap:4}}>
     <View style={[s.row,{flexWrap:'wrap',gap:8}]}><T role="heading" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:19,lineHeight:26}}>{b.title}</T><Badge label={b.kind} tone="neutral"/><T style={{fontSize:11,color:C.muted}}>key {b.key} · order {b.order}</T></View>
     <T style={{fontSize:13,color:C.muted}}>{b.description||'No description.'}</T>
     <T style={{fontSize:12,color:C.muted}}>{b.strategies.length} strateg{b.strategies.length===1?'y':'ies'} · {b.strategies.filter(x=>x.enabled).length} enabled</T>
    </View>
    <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
     <Toggle on={b.enabled} label={`Block ${b.title}: ${b.enabled?'enabled':'disabled'}`} disabled={working||!patchSupported} onPress={()=>askToggleBlock(b)}/>
     <SmallBtn icon="arrow-up" label={`Move block ${b.title} up`} disabled={bi===0||working||!patchSupported} onPress={()=>moveBlock(bi,-1)}/>
     <SmallBtn icon="arrow-down" label={`Move block ${b.title} down`} disabled={bi===blocks.length-1||working||!patchSupported} onPress={()=>moveBlock(bi,1)}/>
     <SmallBtn icon="edit-2" text="Edit" label={`Edit block ${b.title}`} disabled={!patchSupported} onPress={()=>setBlockForm({mode:'edit',key:b.key,title:b.title,description:b.description||'',kind:b.kind,error:'',busy:false})}/>
     <SmallBtn icon="plus" text="Strategy" label={`Add strategy to ${b.title}`} onPress={()=>setForm({mode:'add',draft:blank(b.key,defaultSource(b)),error:'',busy:false})}/>
    </View>
   </View>
   <ErrorLine text={errors[scope]||''}/>
   {!b.strategies.length?<T style={{fontSize:13,color:C.muted,paddingVertical:8}}>No strategies in this block yet.</T>:
   <ScrollView horizontal showsHorizontalScrollIndicator contentContainerStyle={{minWidth:'100%'}}><View role="table" aria-label={`Strategies in ${b.title}`} style={{minWidth:980,flex:1}}>
    <View role="row" style={[s.row,{gap:10,paddingVertical:8,borderBottomWidth:1,borderColor:C.line}]}>{[['Name',2.4],['Pattern',1.3],['TF',.45],['Side',.7],['Audience',.8],['Slot',.5],['Enabled',1],['Order',.9],['',1.6]].map(([l,f])=><T key={String(l)||'actions'} role="columnheader" style={[s.label,{flex:f as number,fontSize:10}]}>{l}</T>)}</View>
    {b.strategies.map((x,i)=><View key={x.key} ref={(el:any)=>{rowEls.current[x.key]=el}} role="row" style={[s.row,{gap:10,minHeight:52,paddingVertical:6,borderBottomWidth:i===b.strategies.length-1?0:1,borderColor:C.line},x.key===fresh&&{backgroundColor:C.soft,borderRadius:8,paddingHorizontal:6}]}>
     <View role="cell" style={{flex:2.4,minWidth:0}}><View style={[s.row,{gap:6}]}>{x.key===fresh&&<Badge label="New" tone="green"/>}<T numberOfLines={1} style={{flexShrink:1,fontFamily:'InterSemi',fontSize:13,color:x.enabled?C.ink:C.muted}}>{x.name}</T></View><T numberOfLines={1} style={{fontSize:11,lineHeight:16,color:C.muted}}>{x.key}{x.tags?.length?` · ${x.tags.join(', ')}`:''} · min {x.min_trades} trades</T></View>
     <T role="cell" numberOfLines={1} style={{flex:1.3,fontSize:13}}>{x.pattern_name||patternName(x.pattern)}</T>
     <T role="cell" style={{flex:.45,fontSize:13}}>{x.timeframe}</T>
     <T role="cell" style={{flex:.7,fontSize:13,color:x.side==='long'?C.green:C.red}}>{x.side}</T>
     <T role="cell" style={{flex:.8,fontSize:13}}>{AUDIENCE_LABEL[x.audience]||x.audience}</T>
     <T role="cell" style={{flex:.5,fontSize:13,color:x.default_slot?C.ink:C.muted}}>{x.default_slot||'—'}</T>
     <View role="cell" style={{flex:1}}><Toggle on={x.enabled} label={`${x.name}: ${x.enabled?'enabled':'disabled'}`} disabled={working||!patchSupported} onPress={()=>askToggleStrategy(b,x)}/></View>
     <View role="cell" style={[s.row,{flex:.9,gap:4}]}><SmallBtn icon="arrow-up" label={`Move ${x.name} up`} disabled={i===0||working||!patchSupported} onPress={()=>moveStrategy(b,i,-1)}/><SmallBtn icon="arrow-down" label={`Move ${x.name} down`} disabled={i===b.strategies.length-1||working||!patchSupported} onPress={()=>moveStrategy(b,i,1)}/></View>
     <View role="cell" style={[s.row,{flex:1.6,gap:6,justifyContent:'flex-end'}]}><SmallBtn text="Edit" label={`Edit ${x.name}`} onPress={()=>setForm({mode:'edit',key:x.key,draft:fromDef(x),error:'',busy:false})}/><SmallBtn icon="eye" text="Preview" label={`Preview ${x.name}`} onPress={()=>openPreview(x)}/></View>
    </View>)}
   </View></ScrollView>}
  </View>})}

  {/* Add / edit strategy */}
  <Sheet visible={!!form} onClose={()=>!form?.busy&&setForm(null)} title={form?.mode==='edit'?'Edit strategy':'Add strategy'} subtitle={form?.mode==='edit'?`Key ${form.key}`:'Saved strategies can be previewed before they are enabled.'} footer={form&&<View style={[s.row,{justifyContent:'flex-end',flexWrap:'wrap'}]}><Button label="Cancel" kind="ghost" disabled={form.busy} onPress={()=>setForm(null)}/><Button label={form.mode==='edit'?'Save changes':form.draft.enable?'Save and enable':'Save as disabled'} loading={form.busy} onPress={saveForm}/></View>}>
   {form&&<ScrollView keyboardShouldPersistTaps="handled" contentContainerStyle={{padding:24,gap:16}}>
    <Choice label="Block" value={form.draft.block_key} options={blocks.map(b=>[b.key,b.title+(b.enabled?'':' (disabled)')] as [string,string])} onChange={v=>setDraft({block_key:v})}/>
    <Input label="Name" value={form.draft.name} onChangeText={v=>setDraft({name:v})} placeholder="Falling Wedge strict · 1D"/>
    {form.mode==='add'&&<View style={{gap:4}}><Input label="Key (advanced)" value={form.draft.key} onChangeText={v=>setDraft({key:v.trim().toLowerCase()})} placeholder="auto from name"/><T style={{fontSize:11,color:C.muted}}>Optional. Leave empty and the server creates a unique key from the name (e.g. falling-wedge-strict-1d). A key can’t change later.</T></View>}
    <Input label="Description" value={form.draft.description} onChangeText={v=>setDraft({description:v})} multiline placeholder="What the strategy looks for, in one or two sentences."/>
    <View style={{gap:6}}>
     <Label>Source</Label>
     {form.mode==='edit'
      ?<T style={{fontSize:14}}>{SOURCE_LABEL[form.draft.source_type]}</T>
      :<View role="radiogroup" aria-label="Source" style={[s.row,{flexWrap:'wrap',gap:6}]}>{(['stored_pattern','research_pattern'] as SourceType[]).map(t=>
        <Chip key={t} label={SOURCE_LABEL[t]+(sources&&!sourceOf(sources,t)?.available?' (unavailable)':'')} active={form.draft.source_type===t} onPress={()=>pickSource(t)}/>)}</View>}
     <T style={{fontSize:11,color:C.muted}}>{form.draft.source_type==='research_pattern'
      ?'One combination of the researched catalogue: pattern, variant, side and timeframe. Its evidence is that combination\u2019s own research cell, and it still passes the publication gate before a single number is shown.'
      :'Matches come from the stored market scan (point-in-time, data end stated).'}</T>
     {!!sourcesError&&<T accessibilityRole="alert" style={{fontSize:12,color:C.amber}}>The list of patterns this server can add did not load: {sourcesError}</T>}
    </View>
    {form.mode==='edit'&&<T style={{fontSize:12,color:C.muted}}>Pattern, timeframe and side can’t change after saving (they define what the strategy is). Add a new strategy instead.</T>}
    <PatternChoice source={sourceOf(sources,form.draft.source_type)} value={form.draft.pattern} disabled={form.mode==='edit'} fallbackName={patternName(form.draft.pattern)} onChange={pickPattern}/>
    {form.mode==='add'&&form.draft.source_type==='research_pattern'&&!!patternOf(sources,form.draft)&&
     <Choice label="Variant" value={form.draft.variant} options={patternOf(sources,form.draft)!.variants.map(v=>[v.id,v.label] as [string,string])} onChange={pickVariant}/>}
    <Choice label="Timeframe" value={form.draft.timeframe} disabled={form.mode==='edit'} options={timeframesFor(sources,form.draft.source_type).map(t=>[t,t] as [string,string])} onChange={v=>setDraft({timeframe:v,audience:form.mode==='add'?(v==='1W'?'investor':'trader'):form.draft.audience})}/>
    <Choice<Side> label="Side" value={form.draft.side} disabled={form.mode==='edit'} options={(sidesFor(sources,form.draft).length?sidesFor(sources,form.draft):(['long','short'] as Side[])).map(v=>[v,SIDE_LABEL[v]] as [Side,string])} onChange={v=>setDraft({side:v})}/>
    <Choice<Audience> label="Audience" value={form.draft.audience} options={[['trader','Trader'],['investor','Investor'],['both','Both']]} onChange={v=>setDraft({audience:v})}/>
    <Input label="Tags (comma separated)" value={form.draft.tags} onChangeText={v=>setDraft({tags:v})} placeholder="Chart patterns, Bullish, 1D"/>
    <Input label="Minimum trades" value={form.draft.min_trades} onChangeText={v=>setDraft({min_trades:v.replace(/[^0-9]/g,'')})} keyboardType="number-pad"/>
    <T style={{fontSize:11,color:C.muted,marginTop:-10}}>Sample needed before a 95% low is shown as evidence (default 10).</T>
    <Choice<'A'|'B'|null> label="Default slot" value={form.draft.default_slot} options={[[null,'None'],['A','Scanner A'],['B','Scanner B']]} onChange={v=>setDraft({default_slot:v})}/>
    {!!slotClash&&<T style={{fontSize:12,color:C.amber}}>“{slotClash.name}” is also the default for slot {form.draft.default_slot} in this block.</T>}
    <View style={{gap:6}}><Label>Status</Label><Toggle on={form.draft.enable} label={form.mode==='edit'?'Enabled':'Enable after saving'} disabled={form.mode==='edit'&&!patchSupported} onPress={()=>setDraft({enable:!form.draft.enable})}/><T style={{fontSize:11,color:C.muted}}>{form.mode==='add'?'Leave disabled to preview what it finds before members see it.':'Disabled strategies are hidden from members.'}</T></View>
    {formWarnings.map(t=><View key={t} accessibilityRole="alert" style={[s.row,{gap:8,padding:10,borderRadius:10,backgroundColor:C.amberBg,alignItems:'flex-start'}]}><Icon name="info" size={15} color={C.amber}/><T style={{flex:1,fontSize:13,lineHeight:19,color:C.amber}}>{t}</T></View>)}
    {showIssues&&formIssues.length>0&&<View accessibilityRole="alert" style={{gap:4,padding:12,borderRadius:10,backgroundColor:C.amberBg}}><T style={{fontFamily:'InterSemi',fontSize:13,color:C.amber}}>Fix before saving:</T>{formIssues.map(t=><T key={t} style={{fontSize:13,color:C.amber}}>• {t}</T>)}</View>}
    <ErrorLine text={form.error}/>
   </ScrollView>}
  </Sheet>

  {/* Add / edit block */}
  <Sheet visible={!!blockForm} onClose={()=>!blockForm?.busy&&setBlockForm(null)} title={blockForm?.mode==='edit'?'Edit block':'Add block'} subtitle={blockForm?.mode==='edit'?`Key ${blockForm.key}`:'New blocks start disabled.'} footer={blockForm&&<View style={[s.row,{justifyContent:'flex-end'}]}><Button label="Cancel" kind="ghost" disabled={blockForm.busy} onPress={()=>setBlockForm(null)}/><Button label={blockForm.mode==='edit'?'Save block':'Add block'} loading={blockForm.busy} onPress={saveBlock}/></View>}>
   {blockForm&&<ScrollView keyboardShouldPersistTaps="handled" contentContainerStyle={{padding:24,gap:16}}>
    {blockForm.mode==='add'&&<Input label="Key" value={blockForm.key} onChangeText={v=>setBlockForm(f=>f&&{...f,key:v.toLowerCase().replace(/[^a-z0-9_]/g,''),error:''})} placeholder="quant"/>}
    <Input label="Title" value={blockForm.title} onChangeText={v=>setBlockForm(f=>f&&{...f,title:v,error:''})} placeholder="Quant Strategies"/>
    <Input label="Description" value={blockForm.description} onChangeText={v=>setBlockForm(f=>f&&{...f,description:v,error:''})} multiline/>
    <Choice<BlockKind> label="Kind" value={blockForm.kind} options={KINDS.map(k=>[k,k] as [BlockKind,string])} onChange={v=>setBlockForm(f=>f&&{...f,kind:v,error:''})}/>
    <ErrorLine text={blockForm.error}/>
   </ScrollView>}
  </Sheet>

  {/* Preview */}
  <Sheet wide visible={!!preview} onClose={()=>setPreview(null)} title={preview?`Preview · ${preview.strategy.name}`:'Preview'} subtitle={preview?`${preview.strategy.pattern_name||patternName(preview.strategy.pattern)} · ${preview.strategy.timeframe} · ${preview.strategy.side} · ${preview.strategy.enabled?'enabled':'disabled (not visible to members)'}`:undefined}>
   {preview&&<ScrollView contentContainerStyle={{padding:24,gap:16}}>
    {preview.loading&&<Loading/>}
    {!!preview.error&&<View style={{gap:10}}><ErrorLine text={`Preview failed: ${preview.error}`}/><Button label="Retry" kind="outline" onPress={()=>openPreview(preview.strategy)} style={{alignSelf:'flex-start'}}/></View>}
    {preview.data&&<PreviewBody data={preview.data} minTrades={preview.strategy.min_trades}/>}
    <View style={[s.row,{gap:8,padding:12,borderRadius:10,backgroundColor:C.soft,alignItems:'flex-start'}]}><Icon name="shield" size={15} color={C.green}/><T style={{flex:1,fontSize:12,lineHeight:18,color:C.mint}}>Strategies without tested evidence are shown as ‘not tested’ — never as evidence.</T></View>
   </ScrollView>}
  </Sheet>

  {/* Confirm every write */}
  <Sheet visible={!!confirm} onClose={()=>setConfirm(null)} title={confirm?.title||'Confirm'} footer={confirm&&<View style={[s.row,{justifyContent:'flex-end'}]}><Button label="Cancel" kind="ghost" onPress={()=>setConfirm(null)}/><Button label={confirm.label} onPress={runConfirm}/></View>}>
   {confirm&&<View style={{padding:24}}><T style={{color:C.muted,lineHeight:22}}>{confirm.detail}</T></View>}
  </Sheet>
 </DeskPage>;
}
function PreviewBody({data,minTrades}:{data:StrategyResults;minTrades:number}){
 const rows=(data.rows||[]).slice(0,5),st=data.strategy;
 return <View style={{gap:14}}>
  <View style={[s.row,{flexWrap:'wrap',gap:18}]}>
   <View><T style={{fontSize:11,color:C.muted}}>Found</T><T style={{fontFamily:'ManropeBold',fontSize:26,lineHeight:33}}>{data.total??rows.length}</T><T style={{fontSize:11,color:C.muted}}>{data.universe?.label||'Universe'}{data.universe?.count?` · ${data.universe.count} stocks`:''}</T></View>
   {st&&<View><T style={{fontSize:11,color:C.muted}}>Positive 95% low (n ≥ {minTrades})</T><T style={{fontFamily:'ManropeBold',fontSize:26,lineHeight:33}}>{st.positive_low_count??'—'}</T></View>}
   {st&&<View><T style={{fontSize:11,color:C.muted}}>Tested exit rule</T><T style={{fontFamily:'ManropeBold',fontSize:26,lineHeight:33,color:st.tested_count?C.ink:C.muted}}>{st.tested_count??'—'}</T><T style={{fontSize:11,color:C.muted}}>{st.tested_count?'rows with a later-test':'not tested'}</T></View>}
  </View>
  <View style={[s.row,{flexWrap:'wrap',gap:8}]}><Badge label={data.source==='stored_scan'?'Stored scan':String(data.source||'')} tone="neutral"/><Badge label={`Data to ${data.data_end?dateText(data.data_end):'unknown'}${data.age_days!=null?` · ${data.age_days}d old`:''}`} tone={data.stale?'amber':'green'}/>{data.stale&&<Badge label="Stale — research only" tone="amber"/>}</View>
  {!rows.length?<T style={{color:C.muted}}>No stock in {data.universe?.label||'the universe'} matches this strategy on data to {data.data_end?dateText(data.data_end):'the latest scan'}.</T>:
  <ScrollView horizontal contentContainerStyle={{minWidth:'100%'}}><View role="table" aria-label="Top 5 by 95% low" style={{minWidth:640,flex:1}}>
   <View role="row" style={[s.row,{gap:10,paddingVertical:8,borderBottomWidth:1,borderColor:C.line}]}>{[['Symbol',1.3],['95% low',.8],['Avg',.8],['n',.9],['Evidence basis',2.4]].map(([l,f])=><T key={String(l)} role="columnheader" style={[s.label,{flex:f as number,fontSize:10}]}>{l}</T>)}</View>
   {rows.map(r=>{const small=r.n<minTrades;return <View key={r.match_id||r.symbol} role="row" style={[s.row,{gap:10,minHeight:46,paddingVertical:6,borderBottomWidth:1,borderColor:C.line}]}>
    <View role="cell" style={{flex:1.3,minWidth:0}}><T style={{fontFamily:'InterSemi',fontSize:13}}>{r.symbol}</T><T numberOfLines={1} style={{fontSize:11,lineHeight:16,color:C.muted}}>{r.company}</T></View>
    <T role="cell" style={{flex:.8,fontSize:13,color:r.low_pct==null?C.muted:r.low_pct>0?C.green:C.red}}>{pct(r.low_pct)}</T>
    <T role="cell" style={{flex:.8,fontSize:13}}>{pct(r.avg_pct)}</T>
    <View role="cell" style={{flex:.9}}><T style={{fontSize:13}}>{r.n}</T><T style={{fontSize:10,lineHeight:14,color:small?C.amber:C.muted}}>{small?`below min ${minTrades}`:r.sample_label}</T></View>
    <T role="cell" style={{flex:2.4,fontSize:12,color:r.evidence_basis==='tested_rule'?C.green:C.muted}}>{BASIS_LABEL[r.evidence_basis]||'Not tested'}</T>
   </View>})}
  </View></ScrollView>}
 </View>;
}
