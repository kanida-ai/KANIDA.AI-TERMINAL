// The workspace's four sheets: Add widget (searchable library), a widget's settings, the workspace menu, and the
// instrument picker. Every choice is a chip or a list row — no number boxes, no technical configuration unless the
// user opens a widget's settings.
import React,{useMemo,useState} from 'react';
import {Pressable,TextInput,View} from 'react-native';
import {Badge,Button,C,Icon,Sheet,T,s} from '../ui';
import {useDerivativeRead} from '../derivative/useDerivatives';
import {expiriesFor} from '../derivative/logic';
import type {Scanner} from '../screener/types';
import {TYPE} from './tokens';
import type {Registry,Size,Template,Widget,WidgetSpec,Workspace} from './types';

const SIZE_LABEL:Record<Size,string>={S:'Small',M:'Medium',L:'Large',F:'Full width'};
const VALUE_LABEL:Record<string,Record<string,string>>={
 view:{auto:'Fit to size',both:'Chart + readings',chart:'Chart only',readings:'Readings only',active:'Active',ended:'Ended today',all:'All'},
 atm:{'2':'ATM ±2','3':'ATM ±3','5':'ATM ±5'},scope:{connected:'Connected widgets only',all:'Every source'},
 list:{all:'All scanners',kanida:'KANIDA scanners',mine:'My scanners'},
};
const SETTING_LABEL:Record<string,string>={view:'Show',atm:'Strikes',scope:'Read from',list:'List'};

function Chip({label,active,onPress,a11y}:{label:string;active?:boolean;onPress:()=>void;a11y?:string}){
 return <Pressable accessibilityRole="button" accessibilityState={{selected:!!active}} accessibilityLabel={a11y||label} onPress={onPress}
  style={({pressed})=>({paddingHorizontal:12,paddingVertical:8,borderRadius:9,borderWidth:1,borderColor:active?C.green:C.line,
   backgroundColor:active?C.soft:C.paper,opacity:pressed?.7:1})}>
  <T style={[TYPE.body,{color:active?C.green:C.ink}]}>{label}</T></Pressable>;
}
const Group=({title,children}:{title:string;children:React.ReactNode})=><View style={{gap:8}}>
 <T style={TYPE.label}>{title}</T><View style={[s.row,{flexWrap:'wrap',gap:8}]}>{children}</View></View>;

export function useUnderlyings(){
 const v=useDerivativeRead<any>('/api/screener/vocabulary',0);
 const list:string[]=v.data?.underlyings||[];
 const idx:string[]=v.data?.index_underlyings||[];
 return useMemo(()=>[...idx.filter(x=>list.includes(x)),...list.filter(x=>!idx.includes(x))],[list,idx]);
}

function SymbolList({value,onPick}:{value:string|null;onPick:(s:string)=>void}){
 const all=useUnderlyings();
 const [q,setQ]=useState('');
 const shown=all.filter(x=>!q.trim()||x.includes(q.trim().toUpperCase())).slice(0,80);
 return <View style={{gap:10}}>
  <TextInput value={q} onChangeText={setQ} placeholder="Search NIFTY, BANKNIFTY, RELIANCE…" placeholderTextColor={C.muted}
   autoCapitalize="characters" accessibilityLabel="Search instruments" style={s.input}/>
  <View style={[s.row,{flexWrap:'wrap',gap:6}]}>{shown.map(x=><Chip key={x} label={x} active={value===x} onPress={()=>onPick(x)}/>)}</View>
 </View>;
}

export function InstrumentSheet({value,onPick,onClose}:{value:string|null;onPick:(s:string)=>void;onClose:()=>void}){
 return <Sheet visible onClose={onClose} title="Point the workspace at" subtitle="Every widget that follows the workspace moves with it">
  <SymbolList value={value} onPick={x=>{onPick(x);onClose()}}/>
 </Sheet>;
}

export function AddWidgetSheet({registry,onAdd,onClose}:{registry:Registry;onAdd:(type:string)=>void;onClose:()=>void}){
 const [q,setQ]=useState('');
 const term=q.trim().toLowerCase();
 const hits=registry.widgets.filter(w=>!term||w.label.toLowerCase().includes(term)||w.blurb.toLowerCase().includes(term)||w.category.toLowerCase().includes(term));
 return <Sheet visible onClose={onClose} title="Add widget" subtitle="Each one is backed by live KANIDA data — add the same one twice to compare">
  <TextInput value={q} onChangeText={setQ} placeholder="Search — e.g. IV, chain, alerts" placeholderTextColor={C.muted} autoFocus
   accessibilityLabel="Search widgets" style={s.input}/>
  {registry.categories.map(cat=>{const rows=hits.filter(w=>w.category===cat);if(!rows.length)return null;
   return <View key={cat} style={{gap:6}}>
    <T style={TYPE.label}>{cat}</T>
    {rows.map(w=><Pressable key={w.type} accessibilityRole="button" accessibilityLabel={`Add ${w.label}`} onPress={()=>{onAdd(w.type);onClose()}}
     style={({pressed,hovered}:any)=>[s.row,{gap:12,padding:12,borderRadius:12,borderWidth:1,borderColor:hovered?C.green:C.line,
      backgroundColor:C.paper,opacity:pressed?.7:1}]}>
     <View style={{flex:1,gap:3}}>
      <View style={[s.row,{gap:8}]}><T style={TYPE.title}>{w.label}</T>
       {w.instrument?<Badge label="FOLLOWS" tone="green"/>:<Badge label="SETS THE INSTRUMENT" tone="neutral"/>}</View>
      <T style={[TYPE.body,{color:C.muted}]}>{w.blurb}</T>
     </View>
     <Icon name="plus" size={16} color={C.green}/>
    </Pressable>)}
   </View>;})}
  {!hits.length&&<T style={{color:C.muted}}>No widget matches “{q}”.</T>}
 </Sheet>;
}

export function SettingsSheet({w,spec,selectedSymbol,onPatch,onDuplicate,onMove,onRemove,onClose,canUp,canDown}:{w:Widget;spec:WidgetSpec;
 selectedSymbol:string|null;onPatch:(p:Partial<Widget>)=>void;onDuplicate:()=>void;onMove:(d:-1|1)=>void;onRemove:()=>void;onClose:()=>void;
 canUp:boolean;canDown:boolean}){
 const [pinning,setPinning]=useState(false);
 const symbol=w.follow_workspace?selectedSymbol:w.instrument;
 const filters=useDerivativeRead<any>(spec.settings.expiry?'/api/derivatives/filters':null,0);
 // only the front two expiries are CAPTURED (the capture scope); a later listed expiry has no readings to show
 const expiries=symbol&&filters.data?expiriesFor(filters.data.expiries||[],symbol).slice(0,2):[];
 const scanners=useDerivativeRead<{scanners:Scanner[]}>(spec.settings.scanner_id?'/api/screener/scanners':null,0);
 const set=(key:string,value:string)=>onPatch({settings:{...w.settings,[key]:value}});
 return <Sheet visible onClose={onClose} title={`${spec.label} settings`} subtitle={spec.blurb}
  footer={<View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
   <Button label="Duplicate" icon="copy" kind="soft" onPress={()=>{onDuplicate();onClose()}}/>
   <Button label="Move up" icon="arrow-up" kind="outline" disabled={!canUp} onPress={()=>onMove(-1)}/>
   <Button label="Move down" icon="arrow-down" kind="outline" disabled={!canDown} onPress={()=>onMove(1)}/>
   <Button label="Remove" icon="trash-2" kind="outline" onPress={()=>{onRemove();onClose()}}/></View>}>
  {spec.instrument&&<Group title="Instrument">
   <Chip label={`Follow workspace${selectedSymbol?` (${selectedSymbol})`:''}`} active={w.follow_workspace}
    onPress={()=>{setPinning(false);onPatch({follow_workspace:true,instrument:null})}}/>
   <Chip label={!w.follow_workspace&&w.instrument?`Pinned to ${w.instrument}`:'Pin to an instrument…'} active={!w.follow_workspace}
    onPress={()=>setPinning(true)}/>
  </Group>}
  {pinning&&<SymbolList value={w.instrument} onPick={x=>{onPatch({follow_workspace:false,instrument:x,expiry:null});setPinning(false)}}/>}
  {!!spec.settings.expiry&&<Group title="Expiry">
   <Chip label={w.follow_workspace?'Follow workspace':'Nearest'} active={!w.expiry} onPress={()=>onPatch({expiry:null})}/>
   {expiries.map((e:any)=><Chip key={e.expiry} label={`${e.expiry}${e.days_to_expiry!=null?` · ${e.days_to_expiry}d`:''}`}
    active={w.expiry===e.expiry} onPress={()=>onPatch({expiry:e.expiry})}/>)}
   {!expiries.length&&<T style={[TYPE.body,{color:C.muted}]}>{symbol?'No expiries listed for this instrument.':'Pick an instrument first.'}</T>}
   {!!expiries.length&&<T style={[TYPE.helper,{color:C.muted,width:'100%'}]}>KANIDA captures the front two expiries of each instrument.</T>}
  </Group>}
  {!!spec.settings.scanner_id&&<Group title="Scanner">
   {(scanners.data?.scanners||[]).map(sc=><Chip key={sc.id} label={sc.is_default?sc.name:`${sc.name} · mine`} active={w.scanner_id===sc.id}
    onPress={()=>onPatch({scanner_id:sc.id})}/>)}
  </Group>}
  {Object.entries(spec.settings).filter(([,v])=>Array.isArray(v)).map(([key,values])=><Group key={key} title={SETTING_LABEL[key]||key}>
   {(values as string[]).map(v=><Chip key={v} label={VALUE_LABEL[key]?.[v]||v} active={(w.settings[key]??(values as string[])[0])===v} onPress={()=>set(key,v)}/>)}
  </Group>)}
  <Group title="Size">
   {(['S','M','L','F'] as Size[]).map(z=><Chip key={z} label={SIZE_LABEL[z]} active={w.size===z} onPress={()=>onPatch({size:z})}/>)}
  </Group>
  <Group title="Height">
   <Chip label="Short" active={w.height==='short'} onPress={()=>onPatch({height:'short'})}/>
   <Chip label="Tall" active={w.height==='tall'} onPress={()=>onPatch({height:'tall'})}/>
  </Group>
 </Sheet>;
}

export function WorkspaceMenu({workspaces,current,templates,onOpen,onCreate,onRename,onDuplicate,onDelete,onClose}:{
 workspaces:Workspace[];current:Workspace;templates:Template[];onOpen:(id:string)=>void;onCreate:(template:string)=>void;
 onRename:(name:string)=>void;onDuplicate:()=>void;onDelete:()=>void;onClose:()=>void}){
 const [name,setName]=useState(current.name);
 const [confirm,setConfirm]=useState(false);
 return <Sheet visible onClose={onClose} title="Workspaces" subtitle="Saved to your profile automatically · private to you">
  <View style={{gap:6}}>
   {workspaces.map(x=><Pressable key={x.id} accessibilityRole="button" onPress={()=>{onOpen(x.id);onClose()}}
    style={({pressed})=>[s.between,{padding:12,borderRadius:10,borderWidth:1,borderColor:x.id===current.id?C.green:C.line,
     backgroundColor:x.id===current.id?C.soft:C.paper,opacity:pressed?.7:1}]}>
    <T style={{fontFamily:'InterMedium'}}>{x.name}</T><T style={[TYPE.helper,{color:C.muted}]}>{x.widgets.length} widgets</T>
   </Pressable>)}
  </View>
  <View style={{gap:8}}>
   <T style={TYPE.label}>This workspace</T>
   <View style={[s.row,{gap:8}]}>
    <TextInput value={name} onChangeText={setName} maxLength={60} accessibilityLabel="Workspace name" style={[s.input,{flex:1}]}
     onSubmitEditing={()=>name.trim()&&onRename(name.trim())}/>
    <Button label="Rename" kind="soft" disabled={!name.trim()||name.trim()===current.name} onPress={()=>onRename(name.trim())}/>
   </View>
   <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
    <Button label="Duplicate" icon="copy" kind="outline" onPress={()=>{onDuplicate();onClose()}}/>
    {confirm?<><Button label="Delete it" icon="trash-2" onPress={()=>{onDelete();onClose()}}/><Button label="Keep" kind="outline" onPress={()=>setConfirm(false)}/></>
     :<Button label="Delete" icon="trash-2" kind="outline" disabled={workspaces.length<=1} onPress={()=>setConfirm(true)}/>}
   </View>
  </View>
  <View style={{gap:6}}>
   <T style={TYPE.label}>New workspace from a template</T>
   {templates.map(t=><Pressable key={t.key} accessibilityRole="button" onPress={()=>{onCreate(t.key);onClose()}}
    style={({pressed})=>({gap:3,padding:12,borderRadius:10,borderWidth:1,borderColor:C.line,opacity:pressed?.7:1})}>
    <T style={TYPE.title}>{t.name}</T>
    <T style={[TYPE.body,{color:C.muted}]}>{t.description}</T>
   </Pressable>)}
  </View>
 </Sheet>;
}
