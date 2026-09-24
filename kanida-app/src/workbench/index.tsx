// THE KANIDA WORKSPACE (docs/WORKSPACE_SPEC.md, docs/WORKSPACE_LAYOUT.md, docs/WORKSPACE_UX_AUDIT.md).
//
// Screener discovers → the user selects an instrument → connected widgets investigate → the AI summary explains.
//
// COMPOSED, NOT APPENDED. The widgets are an ordered list; the layout engine (layout.ts) turns it into balanced
// rows whose widgets share the row's width by weight and whose heights share the screen. Adding a widget re-
// composes every row, so a third widget sits beside the first two instead of on a new line, and a fourth makes a
// clean 2 x 2 — the screen stays full and nobody has to scroll because one more widget was added.
//
// ONE SELECTION. The workspace holds one selected instrument and the focus a scanner match carried with it. Every
// widget that follows the workspace reads it; a pinned widget reads its own and says so in its header.
//
// SAVED AS A DEFINITION, AUTOMATICALLY, with a version check (a stale save from a second window loads the newer
// copy). One hub under every read (hub.ts); one status check a minute; off-screen widgets render their header only.
import React,{useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {Modal,Platform,Pressable,View,useWindowDimensions} from 'react-native';
import {Button,C,Empty,Icon,Loading,T,s,useLayoutMode} from '../ui';
import {api} from '../model';
import {readStore,writeStore} from '../layout';
import {Ctx,type WorkspaceCtx} from './context';
import {installHub,nextReading,uninstallHub} from './hub';
import {installScrollRule} from './scroll';
import {wsApi} from './api';
import {AddWidgetSheet,InstrumentSheet,SettingsSheet,WorkspaceMenu} from './Sheets';
import {Tile} from './Tile';
import {layout,type LSize} from './layout';
import {CHROME,TYPE} from './tokens';
import type {Columns,Definition,Registry,Selected,Template,Widget,Workspace} from './types';

const GAP=CHROME.gutter;
const SAVE_DELAY=800;
const web=Platform.OS==='web';
const uid=()=>Math.random().toString(36).slice(2,12);
const message=(e:any)=>e?.message||'KANIDA could not complete that.';
const defOf=(w:Workspace):Definition=>({name:w.name,selected:w.selected,widgets:w.widgets,layout:(w as any).layout||{columns:'auto'}});
const COLUMN_CHOICES:Columns[]=['auto',2,3,4,5];

/** Between two rows: a quiet place to insert — it only shows itself when the pointer is on it. */
function Inserter({onPress}:{onPress:()=>void}){
 const [on,setOn]=useState(false);
 return <Pressable accessibilityRole="button" accessibilityLabel="Add a widget here" onPress={onPress}
  {...({onMouseEnter:()=>setOn(true),onMouseLeave:()=>setOn(false)} as any)}
  style={{height:GAP,alignItems:'center',justifyContent:'center',zIndex:2}}>
  {on&&<View style={[s.row,{gap:4,paddingHorizontal:10,height:22,borderRadius:11,backgroundColor:C.soft,borderWidth:1,borderColor:C.green}]}>
   <Icon name="plus" size={11} color={C.green}/><T style={[TYPE.helper,{color:C.green,lineHeight:14}]}>Add widget here</T></View>}
 </Pressable>;
}

/** The height left for the grid on this screen, measured from where the grid actually starts. */
function useAvailableHeight(grid:React.RefObject<any>,winH:number){
 const [top,setTop]=useState(190);
 const measure=useCallback(()=>{
  const el=grid.current as any;
  if(!web||!el?.getBoundingClientRect)return;
  let scroll=0,p=el.parentElement;
  while(p){scroll+=p.scrollTop||0;p=p.parentElement;}
  setTop(Math.max(80,Math.round(el.getBoundingClientRect().top+scroll)));
 },[grid]);
 return {available:Math.max(360,winH-top-CHROME.gutter*2),measure};
}

export function WorkbenchTab(){
 const mode=useLayoutMode();
 const phone=mode==='phone';
 const {width:winW,height:winH}=useWindowDimensions();
 const [registry,setRegistry]=useState<Registry|null>(null);
 const [templates,setTemplates]=useState<Template[]>([]);
 const [list,setList]=useState<Workspace[]>([]);
 const [ws,setWs]=useState<Workspace|null>(null);
 const [def,setDef]=useState<Definition|null>(null);
 const [boot,setBoot]=useState('');
 const [save,setSave]=useState<'saved'|'saving'|'error'>('saved');
 const [seq,setSeq]=useState(0);
 const [asOf,setAsOf]=useState<string|null>(null);
 const [strike,setStrike]=useState<number|null>(null);
 const [sheet,setSheet]=useState<{kind:'add';at?:number}|{kind:'menu'|'instrument'}|{kind:'settings';id:string}|null>(null);
 const [expanded,setExpanded]=useState<string|null>(null);
 const [collapsed,setCollapsed]=useState<Record<string,boolean>>({});
 const [toast,setToast]=useState<{text:string;undo?:()=>void}|null>(null);
 const [gridW,setGridW]=useState(0);
 const [drag,setDrag]=useState<{id:string;over:string|null;side:'before'|'after'}|null>(null);
 const [preview,setPreview]=useState<Record<string,LSize>|null>(null);
 const [flash,setFlash]=useState<string|null>(null);
 const els=useRef<Record<string,any>>({});
 const dragRef=useRef<{id:string;over:string|null;side:'before'|'after'}|null>(null);
 const gridRef=useRef<any>(null);
 const version=useRef(0);
 const dirty=useRef(false);
 const {available,measure}=useAvailableHeight(gridRef,winH);

 useEffect(()=>{installHub();const unscroll=installScrollRule();return ()=>{uninstallHub();unscroll()}},[]);

 const flashToast=useCallback((text:string,undo?:()=>void)=>{setToast({text,undo});setTimeout(()=>setToast(t=>t&&t.text===text?null:t),undo?6000:3500)},[]);
 const open=useCallback((w:Workspace)=>{setWs(w);setDef(defOf(w));version.current=w.version;dirty.current=false;setExpanded(null);
  writeStore('workbench','last',{id:w.id})},[]);

 useEffect(()=>{(async()=>{
  try{
   const [r,t,l]=await Promise.all([wsApi.registry(),wsApi.templates(),wsApi.list()]);
   setRegistry(r);setTemplates(t.templates);setList(l.workspaces);
   const last=readStore<{id:string}>('workbench','last').id;
   const first=l.workspaces.find(x=>x.id===last)||l.workspaces[0];
   if(first)open(first);
  }catch(e){setBoot(message(e))}
 })()},[open]);

 // follow the market: one status check a minute, one re-read per new reading
 useEffect(()=>{
  let last:string|null=null;
  const check=async()=>{try{const st:any=await api('/api/screener/status');
   if(st?.as_of&&st.as_of!==last){if(last){nextReading();setSeq(x=>x+1)}last=st.as_of;setAsOf(st.as_of)}}catch{/* next minute */}};
  check();const t=setInterval(check,60000);return ()=>clearInterval(t);
 },[]);

 // autosave
 const change=useCallback((fn:(d:Definition)=>Definition)=>{setDef(d=>{if(!d)return d;dirty.current=true;return fn(d)})},[]);
 useEffect(()=>{
  if(!ws||!def||!dirty.current)return;
  setSave('saving');
  const t=setTimeout(async()=>{
   try{const saved=await wsApi.save(ws.id,def,version.current);version.current=saved.version;dirty.current=false;setSave('saved');
    setList(l=>l.map(x=>x.id===saved.id?saved:x));}
   catch(e:any){
    if(e?.status===409){const fresh=await wsApi.list();setList(fresh.workspaces);const cur=fresh.workspaces.find(x=>x.id===ws.id);
     if(cur)open(cur);flashToast('This workspace changed in another window — the newer copy is loaded.');setSave('saved');}
    else{setSave('error');flashToast(message(e))}
   }
  },SAVE_DELAY);
  return ()=>clearTimeout(t);
 },[def,ws,open,flashToast]);

 // --- widget operations ----------------------------------------------------------------------------------
 const widgets=def?.widgets||[];
 const specOf=useCallback((type:string)=>registry?.widgets.find(x=>x.type===type),[registry]);
 const renumber=(xs:Widget[])=>xs.map((w,i)=>({...w,position:i}));
 const pulse=(id:string)=>{setFlash(id);setTimeout(()=>setFlash(f=>f===id?null:f),1600)};
 const add=(type:string,at?:number)=>{const spec=specOf(type);if(!spec)return;
  const w:Widget={widget_id:uid(),widget_type:type,position:0,size:spec.size,height:spec.height,
   follow_workspace:spec.instrument,instrument:null,expiry:null,strike_range:null,timeframe:null,
   scanner_id:type==='screener_results'?'call-oi-building':null,settings:{}};
  change(d=>{const xs=[...d.widgets];xs.splice(at==null?xs.length:Math.max(0,Math.min(at,xs.length)),0,w);return {...d,widgets:renumber(xs)}});
  pulse(w.widget_id);flashToast(`Added ${spec.label} — the layout re-balanced.`);};
 const patch=(id:string,p:Partial<Widget>)=>change(d=>({...d,widgets:d.widgets.map(w=>w.widget_id===id?{...w,...p}:w)}));
 const remove=(id:string)=>{const before=def?.widgets||[];const w=before.find(x=>x.widget_id===id);
  change(d=>({...d,widgets:renumber(d.widgets.filter(x=>x.widget_id!==id))}));
  if(expanded===id)setExpanded(null);
  flashToast(`Removed ${specOf(w?.widget_type||'')?.label||'widget'}.`,()=>change(d=>({...d,widgets:renumber(before)})));};
 const duplicate=(id:string)=>{const nid=uid();change(d=>{const i=d.widgets.findIndex(x=>x.widget_id===id);if(i<0)return d;
  const copy={...d.widgets[i],widget_id:nid,settings:{...d.widgets[i].settings}};
  const next=[...d.widgets];next.splice(i+1,0,copy);return {...d,widgets:renumber(next)};});pulse(nid);};
 const move=(id:string,dir:-1|1)=>change(d=>{const i=d.widgets.findIndex(x=>x.widget_id===id);const j=i+dir;
  if(i<0||j<0||j>=d.widgets.length)return d;const next=[...d.widgets];[next[i],next[j]]=[next[j],next[i]];return {...d,widgets:renumber(next)};});
 const place=(id:string,over:string,side:'before'|'after')=>change(d=>{
  const from=d.widgets.findIndex(x=>x.widget_id===id);if(from<0||id===over)return d;
  const next=[...d.widgets];const [w]=next.splice(from,1);
  let to=next.findIndex(x=>x.widget_id===over);if(to<0)return d;if(side==='after')to+=1;
  next.splice(to,0,w);return {...d,widgets:renumber(next)};});
 const select=useCallback((next:Selected)=>change(d=>({...d,selected:next})),[change]);
 const openScanner=useCallback((scannerId:string)=>change(d=>{
  const i=d.widgets.findIndex(x=>x.widget_type==='screener_results');
  if(i>=0)return {...d,widgets:d.widgets.map((w,j)=>j===i?{...w,scanner_id:scannerId}:w)};
  const w:Widget={widget_id:uid(),widget_type:'screener_results',position:0,size:'M',height:'tall',follow_workspace:false,
   instrument:null,expiry:null,strike_range:null,timeframe:null,scanner_id:scannerId,settings:{}};
  return {...d,widgets:renumber([w,...d.widgets])};}),[change]);
 const setColumns=(columns:Columns)=>change(d=>({...d,layout:{columns}}));

 // --- workspace operations -------------------------------------------------------------------------------
 const createFrom=async(template:string)=>{try{const w=await wsApi.create(template);setList(l=>[...l,w]);open(w);flashToast(`Created “${w.name}”.`)}catch(e){flashToast(message(e))}};
 const dupWs=async()=>{if(!ws)return;try{const w=await wsApi.duplicate(ws.id);setList(l=>[...l,w]);open(w);flashToast(`Created “${w.name}”.`)}catch(e){flashToast(message(e))}};
 const delWs=async()=>{if(!ws)return;try{const r=await wsApi.remove(ws.id);setList(r.workspaces);if(r.workspaces[0])open(r.workspaces[0]);flashToast(`Deleted “${ws.name}”.`)}catch(e){flashToast(message(e))}};

 // --- drag to reorder: the pointer against the tiles' own boxes; the left half drops before, the right after
 const onDragStart=(id:string)=>{dragRef.current={id,over:null,side:'before'};setDrag(dragRef.current);};
 const onDragMove=(x:number,y:number)=>{
  let over:string|null=null,side:'before'|'after'='before';
  for(const [id,el] of Object.entries(els.current)){
   const r=el?.getBoundingClientRect?.();
   if(r&&x>=r.left&&x<=r.right&&y>=r.top&&y<=r.bottom){over=id;side=x<r.left+r.width/2?'before':'after';break;}
  }
  const cur=dragRef.current;if(!cur)return;
  if(cur.over!==over||cur.side!==side){dragRef.current={...cur,over,side};setDrag(dragRef.current);}
 };
 const onDragEnd=()=>{const d=dragRef.current;dragRef.current=null;setDrag(null);if(d?.over&&d.over!==d.id)place(d.id,d.over,d.side);};
 // --- resize the edge between two neighbours: a live preview while dragging, committed on release
 const onEdge=(left:string,right:string,sizes:[LSize,LSize]|null,live:boolean)=>{
  if(live){if(sizes)setPreview(p=>{const cur=p||{};return cur[left]===sizes[0]&&cur[right]===sizes[1]?p:{...cur,[left]:sizes[0],[right]:sizes[1]}});return;}
  setPreview(null);
  if(sizes)change(d=>({...d,widgets:d.widgets.map(w=>w.widget_id===left?{...w,size:sizes[0] as any}:w.widget_id===right?{...w,size:sizes[1] as any}:w)}));
 };

 const ctx:WorkspaceCtx|null=useMemo(()=>def?{selected:def.selected,select,seq,asOf,strike,setStrike,widgets:def.widgets,openScanner}:null,
  [def,select,seq,asOf,strike,openScanner]);
 const columns:Columns=def?.layout?.columns||'auto';
 const rows=useMemo(()=>layout(widgets.map(w=>({id:w.widget_id,size:(preview?.[w.widget_id]||w.size) as LSize,tall:w.height==='tall',
   min:registry?.widgets.find(x=>x.type===w.widget_type)?.min_width||0})),
  {width:gridW||winW-48,viewport:available,gap:GAP,columns,phone}),[widgets,preview,gridW,winW,available,columns,phone,registry]);

 if(boot)return <View style={{padding:24}}><Empty icon="alert-circle" title="The workspace could not load" detail={boot}/></View>;
 if(!registry||!ws||!def||!ctx)return <Loading/>;

 const byId=Object.fromEntries(widgets.map(w=>[w.widget_id,w]));
 const indexOf=(id:string)=>widgets.findIndex(w=>w.widget_id===id);
 const sel=def.selected;
 const focus=sel.focus;
 const current=sheet?.kind==='settings'?widgets.find(w=>w.widget_id===sheet.id):null;
 const expandedW=expanded?widgets.find(w=>w.widget_id===expanded):null;

 const tile=(w:Widget,cell:{weight:number;min:number},height:number,next:Widget|null,eager:boolean,expandedView=false)=>{
  const spec=specOf(w.widget_type);if(!spec)return null;
  return <Tile key={w.widget_id+(expandedView?'-x':'')} w={w} spec={spec} weight={cell.weight} minWidth={cell.min} height={height}
   phone={phone} next={next} flash={flash===w.widget_id}
   dragging={drag?.id===w.widget_id} drop={drag&&drag.over===w.widget_id&&drag.id!==w.widget_id?drag.side:null}
   register={(id,el)=>{if(!expandedView){if(el)els.current[id]=el;else delete els.current[id];}}}
   onDragStart={onDragStart} onDragMove={onDragMove} onDragEnd={onDragEnd} onEdge={onEdge}
   onPatch={p=>patch(w.widget_id,p)} onSettings={()=>setSheet({kind:'settings',id:w.widget_id})}
   onExpand={()=>setExpanded(x=>x===w.widget_id?null:w.widget_id)} onRemove={()=>remove(w.widget_id)}
   collapsed={phone&&!!collapsed[w.widget_id]} onCollapse={()=>setCollapsed(c=>({...c,[w.widget_id]:!c[w.widget_id]}))}
   expandedView={expandedView} eager={eager}/>;};

 return <Ctx.Provider value={ctx}>
  <View {...({dataSet:{kwRoot:'1'}} as any)} style={{flex:1,paddingHorizontal:phone?12:24,paddingTop:phone?10:16,paddingBottom:24,gap:12,backgroundColor:C.bg}}>
   {/* --- the top bar: workspace · instrument · focus · columns · state · add. One line on desktop; on a phone it
       stays pinned while the feed scrolls, so the instrument every widget follows is always in view. ------ */}
   <View onLayout={measure} style={[s.row,{flexWrap:'wrap',gap:10,minHeight:36},phone&&web?{position:'sticky' as any,top:0,zIndex:5,
    backgroundColor:C.bg,paddingVertical:8,marginTop:-8}:null]}>
    <Pressable accessibilityRole="button" accessibilityLabel={`Workspace: ${def.name}. Switch or manage`} onPress={()=>setSheet({kind:'menu'})}
     style={({pressed})=>[s.row,{gap:4,opacity:pressed?.7:1}]}>
     <T style={TYPE.page}>{def.name}</T><Icon name="chevron-down" size={15} color={C.muted}/>
    </Pressable>
    <Pressable accessibilityRole="button" accessibilityLabel={`Selected instrument ${sel.underlying||'none'}. Change`} onPress={()=>setSheet({kind:'instrument'})}
     style={({pressed})=>[s.row,{gap:6,paddingHorizontal:10,height:30,borderRadius:8,borderWidth:1,borderColor:C.green,backgroundColor:C.soft,opacity:pressed?.7:1}]}>
     <View style={{width:6,height:6,borderRadius:3,backgroundColor:C.green}}/>
     <T style={[TYPE.title,{color:C.green}]}>{sel.underlying||'Pick an instrument'}</T>
     {!!sel.expiry&&<T style={TYPE.helper}>{sel.expiry}</T>}
     <Icon name="chevron-down" size={12} color={C.green}/>
    </Pressable>
    {focus&&<View style={[s.row,{gap:6,paddingHorizontal:10,height:30,borderRadius:8,backgroundColor:C.dark,maxWidth:520,flexShrink:1}]}>
     <Icon name="crosshair" size={12} color={C.amber}/>
     <T numberOfLines={1} style={[TYPE.helper,{color:C.ink,flexShrink:1}]}>
      {focus.source?`${focus.source} · `:''}{focus.strikes.length?`${focus.strikes.map(x=>x.toLocaleString('en-IN')).join(', ')} ${focus.side||''} · `:''}{focus.from?`since ${focus.from}`:''}</T>
     <Pressable accessibilityRole="button" accessibilityLabel="Clear the focus" onPress={()=>select({...sel,focus:null})}><Icon name="x" size={12} color={C.muted}/></Pressable>
    </View>}
    <View style={{flex:1,minWidth:8}}/>
    {!phone&&<View accessibilityLabel="Widgets per row" style={[s.row,{height:30,borderRadius:8,borderWidth:1,borderColor:C.line,overflow:'hidden'}]}>
     <T style={[TYPE.helper,{paddingHorizontal:8}]}>Per row</T>
     {COLUMN_CHOICES.map(c=><Pressable key={String(c)} accessibilityRole="button" accessibilityState={{selected:columns===c}}
      accessibilityLabel={c==='auto'?'As many as fit':`At most ${c} per row`} onPress={()=>setColumns(c)}
      style={{paddingHorizontal:9,height:30,justifyContent:'center',backgroundColor:columns===c?C.soft:'transparent',borderLeftWidth:1,borderColor:C.line}}>
      <T style={[TYPE.helper,{color:columns===c?C.green:C.ink}]}>{c==='auto'?'Auto':c}</T></Pressable>)}
    </View>}
    <T style={[TYPE.helper,{color:save==='error'?C.red:C.muted}]}>{save==='saving'?'Saving…':save==='error'?'Not saved':'Saved'}{asOf?` · ${asOf.slice(11,16)}`:''}</T>
    <Pressable accessibilityRole="button" accessibilityLabel="Add widget" onPress={()=>setSheet({kind:'add'})}
     style={({pressed})=>[s.row,{gap:6,paddingHorizontal:12,height:30,borderRadius:8,backgroundColor:C.green,opacity:pressed?.8:1}]}>
     <Icon name="plus" size={14} color="#041B12"/><T style={[TYPE.title,{color:'#041B12'}]}>Add widget</T></Pressable>
   </View>

   {/* --- the composed rows (desktop) or the ordered feed (phone) ------------------------------------------ */}
   {!widgets.length?<Empty icon="grid" title="An empty workspace" detail="Add a screener to find something, then the widgets you want to investigate it with."
     action={<View style={[s.row,{gap:10,flexWrap:'wrap',justifyContent:'center'}]}>
      <Button label="Add widget" icon="plus" onPress={()=>setSheet({kind:'add'})}/>
      <Button label="Start from a template" kind="outline" onPress={()=>setSheet({kind:'menu'})}/></View>}/>
    :<View ref={gridRef} onLayout={e=>{setGridW(e.nativeEvent.layout.width);measure();}} style={{gap:0}}>
      {rows.map((row,r)=>{
       const firstIndex=indexOf(row.items[0].id);
       return <React.Fragment key={row.items.map(x=>x.id).join('|')}>
        {r>0&&!phone&&<Inserter onPress={()=>setSheet({kind:'add',at:firstIndex})}/>}
        {r>0&&phone&&<View style={{height:GAP}}/>}
        <View style={{flexDirection:'row',gap:GAP,height:phone&&collapsed[row.items[0].id]?undefined:row.height}}>
         {row.items.map((cell,i)=>{const w=byId[cell.id];if(!w)return null;
          const nextCell=row.items[i+1];
          return tile(w,cell,row.height,nextCell?byId[nextCell.id]:null,r<2);})}
        </View>
       </React.Fragment>;})}
      {!phone&&<Pressable accessibilityRole="button" accessibilityLabel="Add a widget at the end" onPress={()=>setSheet({kind:'add'})}
       style={({hovered}:any)=>[s.row,{gap:6,alignSelf:'flex-start',marginTop:GAP,paddingHorizontal:8,height:28,borderRadius:7,
        backgroundColor:hovered?C.dark:'transparent'}]}>
       <Icon name="plus" size={12} color={C.green}/><T style={[TYPE.helper,{color:C.green}]}>Add widget</T></Pressable>}
     </View>}
   {!!drag&&<T style={[TYPE.helper,{textAlign:'center'}]}>Drop on the left or right half of a widget to place it there.</T>}

   {!!toast&&<View style={[s.row,{position:'absolute',bottom:24,alignSelf:'center',gap:12,backgroundColor:C.paper,borderColor:C.green,
    borderWidth:1,borderRadius:10,paddingHorizontal:14,height:40}]}>
    <T style={TYPE.body}>{toast.text}</T>
    {toast.undo&&<Pressable accessibilityRole="button" onPress={()=>{toast.undo!();setToast(null)}}><T style={[TYPE.title,{color:C.green}]}>Undo</T></Pressable>}
   </View>}
  </View>

  {expandedW&&<Modal visible transparent animationType="fade" onRequestClose={()=>setExpanded(null)}>
   <View {...({dataSet:{kwRoot:'1'}} as any)} style={{flex:1,backgroundColor:'#00070DE6',padding:phone?8:28}}>{tile(expandedW,{weight:1,min:0},0,null,true,true)}</View>
  </Modal>}
  {sheet?.kind==='add'&&<AddWidgetSheet registry={registry} onAdd={t=>add(t,(sheet as any).at)} onClose={()=>setSheet(null)}/>}
  {sheet?.kind==='instrument'&&<InstrumentSheet value={sel.underlying} onPick={x=>select({underlying:x,expiry:null,focus:null})} onClose={()=>setSheet(null)}/>}
  {sheet?.kind==='menu'&&<WorkspaceMenu workspaces={list} current={ws} templates={templates} onOpen={id=>{const w=list.find(x=>x.id===id);if(w)open(w)}}
   onCreate={createFrom} onRename={name=>change(d=>({...d,name}))} onDuplicate={dupWs} onDelete={delWs} onClose={()=>setSheet(null)}/>}
  {current&&specOf(current.widget_type)&&<SettingsSheet w={current} spec={specOf(current.widget_type)!} selectedSymbol={sel.underlying}
   onPatch={p=>patch(current.widget_id,p)} onDuplicate={()=>duplicate(current.widget_id)} onMove={d=>move(current.widget_id,d)}
   onRemove={()=>remove(current.widget_id)} onClose={()=>setSheet(null)}
   canUp={widgets.indexOf(current)>0} canDown={widgets.indexOf(current)<widgets.length-1}/>}
 </Ctx.Provider>;
}
