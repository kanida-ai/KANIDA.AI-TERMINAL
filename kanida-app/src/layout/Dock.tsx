import React from 'react';
import {View,Pressable,ScrollView,Animated,useWindowDimensions} from 'react-native';
import {C,T,Icon,useLayoutMode} from '../ui';
import {webOnly,readStore,writeStore,IconButton,useRoving,useDragResize,clamp,domId} from './shared';
export type DockTab={key:string;label:string;badge?:string;render:()=>React.ReactNode};
export type DockProps={tabs:DockTab[];active:string;onChange:(key:string)=>void;height?:number;minHeight?:number;maxHeight?:number;onResize?:(h:number)=>void;fullScreen?:boolean;onToggleFullScreen?:()=>void;collapsed?:boolean;onToggleCollapsed?:()=>void;rightSlot?:React.ReactNode;persistKey?:string;label?:string;restoreActive?:boolean};
export type DockStore={height:number;active:string;collapsed:boolean;/** true once the user resized the dock (drag/keyboard); only then is `height` restored and the `height` prop ignored. */ userSized?:boolean};
export const DOCK_TAB_BAR=44,DOCK_HANDLE=10,DOCK_DEFAULT_HEIGHT=300;
// A panel mounts on first visit and then stays mounted (display:none when inactive or collapsed), so drafts survive tab switches without a discard modal. memo: Dock-only state changes (collapse, resize commit) skip tab content unless `render` changed.
// F8: whether the panel a component lives in is actually on screen (active tab AND dock body shown). Default true outside a Dock/sheet, so full-page screens behave as before.
export const PanelVisible=React.createContext(true);
export function usePanelVisible(){return React.useContext(PanelVisible);}
const Panel=React.memo(function Panel({id,tabId,visible,render}:{id:string;tabId:string;visible:boolean;render:()=>React.ReactNode}){
 return <View role="tabpanel" nativeID={id} aria-labelledby={tabId} aria-hidden={!visible} importantForAccessibility={visible?'auto':'no-hide-descendants'} style={{flex:1,minHeight:0,display:visible?'flex':'none'}}><PanelVisible.Provider value={visible}>{render()}</PanelVisible.Provider></View>;
});
function DockTabButton({tab,on,tabId,panelId,compact,onPress,onKeyDown,setRef}:{tab:DockTab;on:boolean;tabId:string;panelId:string;compact:boolean;onPress:()=>void;onKeyDown:(e:any)=>void;setRef:(el:any)=>void}){
 return <Pressable ref={setRef} role="tab" nativeID={tabId} aria-selected={on} accessibilityState={{selected:on}} accessibilityLabel={tab.badge?`${tab.label}, ${tab.badge}`:tab.label} {...webOnly({'aria-controls':panelId,tabIndex:on?0:-1,onKeyDown})} onPress={onPress} style={(st:any)=>[{minHeight:DOCK_TAB_BAR,flexDirection:'row',alignItems:'center',gap:6,paddingHorizontal:compact?11:15,borderBottomWidth:2,borderBottomColor:on?C.green:'transparent',backgroundColor:!on&&(st.hovered||st.focused)?C.paper:'transparent',opacity:st.pressed?.7:1}]}>
  <T numberOfLines={1} style={{fontSize:13,fontFamily:'InterSemi',color:on?C.ink:C.muted}}>{tab.label}</T>
  {!!tab.badge&&<View style={{backgroundColor:on?C.soft:C.paper,borderRadius:6,paddingHorizontal:6,paddingVertical:1}}><T style={{fontSize:10,lineHeight:15,fontFamily:'InterSemi',color:C.green}}>{tab.badge}</T></View>}
 </Pressable>;
}
// Bottom dock. `active`/`onChange` are controlled; height and collapsed work controlled (pass the prop) or uncontrolled. With persistKey, height/active/collapsed are restored from localStorage key `kanida.layout.dock.<persistKey>` (restoreActive=false lets a deep link win). `height` is the whole dock (handle + tab bar + body). fullScreen makes the dock flex:1; the parent hides the chart.
export function Dock(p:DockProps){
 const {tabs,active,onChange,minHeight=120,fullScreen=false,onToggleFullScreen,onToggleCollapsed,rightSlot,persistKey,label='Dock',restoreActive=true}=p;
 const {height:winH}=useWindowDimensions();const phone=useLayoutMode()==='phone';
 const maxH=Math.max(minHeight,Math.round(p.maxHeight??winH*.85));
 const [saved]=React.useState(()=>readStore<DockStore>('dock',persistKey));
 // Only a user-set height is restored (clamped to the current window on load); stores without userSized (older builds wrote the default) are ignored, so a stale value can't pin the layout.
 // Until the user resizes, the dock follows the `height` prop (window-relative default, taller for space-hungry tabs); after that the user's height wins and is clamped on every render.
 const [userSized,setUserSized]=React.useState(()=>saved.userSized===true&&typeof saved.height==='number'),userRef=React.useRef(userSized);userRef.current=userSized;
 const [h,setH]=React.useState(()=>userSized&&typeof saved.height==='number'?clamp(saved.height,minHeight,maxH):p.height??DOCK_DEFAULT_HEIGHT);
 const prevHeight=React.useRef(p.height);
 React.useEffect(()=>{if(p.height!=null&&p.height!==prevHeight.current&&!userRef.current)setH(p.height);prevHeight.current=p.height;},[p.height]);
 const height=clamp(h,minHeight,maxH);
 const isCtl=p.collapsed!=null;
 const [innerCollapsed,setInnerCollapsed]=React.useState(()=>typeof saved.collapsed==='boolean'?saved.collapsed:!!p.collapsed);
 const collapsed=isCtl?!!p.collapsed:innerCollapsed;
 const toggleCollapsed=React.useCallback(()=>{if(!isCtl)setInnerCollapsed(c=>!c);onToggleCollapsed?.();},[isCtl,onToggleCollapsed]);
 const restored=React.useRef(false);
 React.useEffect(()=>{if(restored.current)return;restored.current=true;
  if(restoreActive&&typeof saved.active==='string'&&saved.active!==active&&tabs.some(t=>t.key===saved.active))onChange(saved.active);
  if(isCtl&&typeof saved.collapsed==='boolean'&&saved.collapsed!==!!p.collapsed)onToggleCollapsed?.();
 },[]);
 React.useEffect(()=>{if(restored.current)writeStore('dock',persistKey,{height:h,active,collapsed,userSized});},[persistKey,h,active,collapsed,userSized]);
 const mounted=React.useRef(new Set<string>()).current;mounted.add(active);
 const ids=domId(React.useId());const bodyId=`${ids}-body`;
 const roving=useRoving(tabs.length,'horizontal');
 const resize=useDragResize({axis:'y',value:height,min:minHeight,max:maxH,label:`Resize ${label}`,onCommit:v=>{setH(v);setUserSized(true);p.onResize?.(v);},onEnter:toggleCollapsed});
 const showBody=fullScreen||!collapsed,showHandle=showBody&&!fullScreen&&!phone;
 // Short windows (<650px): the full-screen control is a labelled button, not just an icon, so the one-click escape from a cramped dock is obvious.
 const short=winH<650&&!phone;
 const select=(key:string)=>{if(key!==active)onChange(key);if(collapsed&&!fullScreen)toggleCollapsed();};
 return <View role="region" aria-label={label} style={[{backgroundColor:C.bg,borderTopWidth:1,borderColor:C.line},fullScreen?{flex:1,minHeight:0}:{height:showBody?height:DOCK_TAB_BAR+1}]}>
  {showHandle&&<Animated.View aria-hidden style={resize.ghostStyle}/>}
  {showHandle&&<View {...resize.handleProps} style={[{height:DOCK_HANDLE,alignItems:'center',justifyContent:'center'},resize.handleStyle]}><View style={{width:44,height:4,borderRadius:2,backgroundColor:C.line}}/></View>}
  <View style={{flexDirection:'row',alignItems:'center',minHeight:DOCK_TAB_BAR,borderBottomWidth:showBody?1:0,borderColor:C.line,paddingRight:4}}>
   <ScrollView horizontal showsHorizontalScrollIndicator={false} style={{flex:1}} contentContainerStyle={{flexGrow:1}}>
    <View role="tablist" aria-label={`${label} tabs`} style={{flexDirection:'row'}}>{tabs.map((t,i)=><DockTabButton key={t.key} tab={t} on={t.key===active} tabId={`${ids}-tab-${domId(t.key)}`} panelId={`${ids}-panel-${domId(t.key)}`} compact={phone} onPress={()=>select(t.key)} onKeyDown={(e:any)=>roving.onKeyDown(i,e,j=>onChange(tabs[j].key))} setRef={(el:any)=>{roving.refs.current[i]=el;}}/>)}</View>
   </ScrollView>
   <View style={{flexDirection:'row',alignItems:'center',gap:2,paddingLeft:6}}>
    {rightSlot}
    {!!onToggleFullScreen&&short&&<Pressable accessibilityRole="button" accessibilityLabel={fullScreen?`Exit full screen ${label}`:`Open ${label} full screen`} accessibilityState={{selected:fullScreen}} {...webOnly({'aria-pressed':fullScreen})} onPress={onToggleFullScreen} style={(st:any)=>[{minHeight:32,flexDirection:'row',alignItems:'center',gap:6,paddingHorizontal:10,marginRight:2,borderRadius:8,borderWidth:1,borderColor:fullScreen?C.green:C.line,backgroundColor:fullScreen||st.hovered||st.focused?C.soft:'transparent',opacity:st.pressed?.7:1}]}><Icon name={fullScreen?'minimize-2':'maximize-2'} size={14} color={C.green}/><T numberOfLines={1} style={{fontSize:12,fontFamily:'InterSemi',color:C.green}}>{fullScreen?'Exit full screen':'Full screen'}</T></Pressable>}
    {!!onToggleFullScreen&&!short&&<IconButton icon={fullScreen?'minimize-2':'maximize-2'} label={fullScreen?`Exit full screen ${label}`:`Open ${label} full screen`} tooltip={fullScreen?'Exit full screen':'Full screen'} pressed={fullScreen} onPress={onToggleFullScreen}/>}
    {!fullScreen&&<IconButton icon={collapsed?'chevron-up':'chevron-down'} label={collapsed?`Expand ${label}`:`Collapse ${label} to tab bar`} tooltip={collapsed?'Expand':'Collapse to tab bar'} expanded={!collapsed} controls={bodyId} onPress={toggleCollapsed}/>}
   </View>
  </View>
  <View nativeID={bodyId} style={{flex:1,minHeight:0,display:showBody?'flex':'none'}}>{tabs.filter(t=>mounted.has(t.key)).map(t=><Panel key={t.key} id={`${ids}-panel-${domId(t.key)}`} tabId={`${ids}-tab-${domId(t.key)}`} visible={t.key===active&&showBody} render={t.render}/>)}</View>
 </View>;
}
