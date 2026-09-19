import React from 'react';
import {View,ScrollView,Animated,useWindowDimensions} from 'react-native';
import {C,T,useLayoutMode} from '../ui';
import {readStore,writeStore,IconButton,useDragResize,clamp,domId} from './shared';
import {Widget,type WidgetMenuItem,type WidgetErrorState} from './Widget';
import {ToolRail,type ToolRailItem} from './ToolRail';
export type SidebarWidget={key:string;title:string;subtitle?:string;render:()=>React.ReactNode;menu?:WidgetMenuItem[];onMaximize?:()=>void;onClose?:()=>void;errorState?:WidgetErrorState|null};
export type SidebarRailItem=ToolRailItem;
export type SidebarProps={widgets:SidebarWidget[];railItems:SidebarRailItem[];width?:number;minWidth?:number;maxWidth?:number;onResize?:(w:number)=>void;collapsed?:boolean;onToggleCollapsed?:()=>void;persistKey?:string;label?:string;layout?:'column'|'cards'};
export type SidebarStore={width:number;collapsed:boolean};
export const SIDEBAR_DEFAULT_WIDTH=320;
// memo: resize commits and collapse toggles in the sidebar do not re-render a widget unless its descriptor object changed.
const WidgetSlot=React.memo(function WidgetSlot({wd}:{wd:SidebarWidget}){return <Widget title={wd.title} subtitle={wd.subtitle} menu={wd.menu} onMaximize={wd.onMaximize} onClose={wd.onClose} errorState={wd.errorState}>{wd.render()}</Widget>;});
// Right widget sidebar. column (desktop/tablet): [resize handle][scrolling widget stack][labelled icon rail with collapse toggle]; collapsed shows the rail only. cards (default on phone): labelled horizontal rail + horizontal snap-scrolling widget cards (§10.5). width = widget column width, controlled or uncontrolled; persisted with collapsed at `kanida.layout.sidebar.<persistKey>`.
export function Sidebar({widgets,railItems,minWidth=240,maxWidth=520,onResize,onToggleCollapsed,persistKey,label='Sidebar',layout,...p}:SidebarProps){
 const {width:winW}=useWindowDimensions();const phone=useLayoutMode()==='phone';const cards=(layout??(phone?'cards':'column'))==='cards';
 const [saved]=React.useState(()=>readStore<SidebarStore>('sidebar',persistKey));
 const [w,setW]=React.useState(()=>typeof saved.width==='number'?saved.width:p.width??SIDEBAR_DEFAULT_WIDTH);
 const prevWidth=React.useRef(p.width);
 React.useEffect(()=>{if(p.width!=null&&p.width!==prevWidth.current)setW(p.width);prevWidth.current=p.width;},[p.width]);
 const isCtl=p.collapsed!=null;
 const [innerCollapsed,setInnerCollapsed]=React.useState(()=>typeof saved.collapsed==='boolean'?saved.collapsed:!!p.collapsed);
 const collapsed=isCtl?!!p.collapsed:innerCollapsed;
 const toggle=React.useCallback(()=>{if(!isCtl)setInnerCollapsed(c=>!c);onToggleCollapsed?.();},[isCtl,onToggleCollapsed]);
 const restored=React.useRef(false);
 React.useEffect(()=>{if(restored.current)return;restored.current=true;if(isCtl&&typeof saved.collapsed==='boolean'&&saved.collapsed!==!!p.collapsed)onToggleCollapsed?.();},[]);
 React.useEffect(()=>{if(restored.current)writeStore('sidebar',persistKey,{width:w,collapsed});},[persistKey,w,collapsed]);
 const maxW=Math.max(minWidth,Math.min(maxWidth,winW-360));const width=clamp(w,minWidth,maxW);
 const bodyId=`${domId(React.useId())}-widgets`;
 const resize=useDragResize({axis:'x',value:width,min:minWidth,max:maxW,label:`Resize ${label}`,onCommit:v=>{setW(v);onResize?.(v);},onEnter:toggle});
 const empty=<T style={{fontSize:12,color:C.muted,padding:12}}>No widgets open. Add one from the rail.</T>;
 if(cards){const cardW=Math.max(220,Math.min(winW-48,340));
  return <View role="complementary" aria-label={label} style={{gap:8}}>
   {railItems.length>0&&<ToolRail items={railItems} label={`${label} widgets`} orientation="horizontal"/>}
   {widgets.length?<ScrollView nativeID={bodyId} horizontal showsHorizontalScrollIndicator={false} snapToInterval={cardW+10} decelerationRate="fast" contentContainerStyle={{paddingHorizontal:12,gap:10}}>{widgets.map(wd=><View key={wd.key} style={{width:cardW}}><WidgetSlot wd={wd}/></View>)}</ScrollView>:empty}
  </View>;
 }
 const collapseBtn=<IconButton icon={collapsed?'chevrons-left':'chevrons-right'} label={collapsed?`Expand ${label}`:`Collapse ${label}`} tooltip={collapsed?'Expand sidebar':'Collapse sidebar'} expanded={!collapsed} controls={bodyId} onPress={toggle}/>;
 return <View role="complementary" aria-label={label} style={{flexDirection:'row',height:'100%',minHeight:0,backgroundColor:C.bg,borderLeftWidth:1,borderColor:C.line}}>
  {!collapsed&&<View style={{flexDirection:'row',minHeight:0}}>
   <Animated.View aria-hidden style={resize.ghostStyle}/>
   <View {...resize.handleProps} style={[{width:8,alignItems:'center',justifyContent:'center'},resize.handleStyle]}><View style={{width:3,height:36,borderRadius:2,backgroundColor:C.line}}/></View>
   <ScrollView nativeID={bodyId} style={{width}} contentContainerStyle={{paddingVertical:10,paddingRight:10,gap:10}}>{widgets.length?widgets.map(wd=><WidgetSlot key={wd.key} wd={wd}/>):empty}</ScrollView>
  </View>}
  <ToolRail items={railItems} label={`${label} widgets`} orientation="vertical" header={collapseBtn} style={{borderLeftWidth:1,borderColor:C.line}}/>
 </View>;
}
