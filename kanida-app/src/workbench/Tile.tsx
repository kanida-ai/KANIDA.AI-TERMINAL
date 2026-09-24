// ONE WIDGET IN A ROW: its frame, its drag grip, the resize edge it shares with its right-hand neighbour, the drop
// marker, and lazy rendering.
//
// The tile does not choose its own size. The layout engine (layout.ts) hands it a share of its row (flex weight,
// with a minimum width) and the row's height; every tile in a row is the same height, every header the same
// height, every control the same size (tokens.ts). Resizing drags the EDGE between two neighbours — the pair
// snaps to the nearest S / M / L split and both reflow together, the way a terminal's splitter does.
import React,{useEffect,useRef,useState} from 'react';
import {PanResponder,Platform,Pressable,ScrollView,View} from 'react-native';
import {C,Icon,T,s} from '../ui';
import * as Frame from '../derivative/frame';
import {BODIES,FIT,SELF_SCROLLING} from './widgets';
import {instrumentOf,useWorkspace} from './context';
import {CHROME,TYPE} from './tokens';
import {snapPair,type LSize} from './layout';
import type {Size,Widget,WidgetSpec} from './types';

// The tile header already names the widget, so a reused Derivative block should not print its own title row
// again. src/derivative/frame.tsx gains `BlockBareContext` in the one after-close change (scripts/
// apply-workspace-hooks.py); until it exists this renders the block exactly as the Derivative tab does.
const Bare:React.Context<boolean>|undefined=(Frame as any).BlockBareContext;
const bare=(node:React.ReactNode)=>Bare?<Bare.Provider value>{node}</Bare.Provider>:node;
const web=Platform.OS==='web';

function useVisible(ref:React.RefObject<any>,eager:boolean){
 const [seen,setSeen]=useState(eager||!web||typeof (globalThis as any).IntersectionObserver==='undefined');
 useEffect(()=>{
  if(seen)return;
  const el=ref.current as any;
  if(!el||typeof el.getBoundingClientRect!=='function'){setSeen(true);return;}
  const io=new (globalThis as any).IntersectionObserver((entries:any[])=>{if(entries.some(e=>e.isIntersecting)){setSeen(true);io.disconnect();}},
   {rootMargin:'400px 0px'});
  io.observe(el);
  return ()=>io.disconnect();
 },[seen,ref]);
 return seen;
}

function Control({icon,label,onPress}:{icon:string;label:string;onPress:()=>void}){
 return <Pressable accessibilityRole="button" accessibilityLabel={label} onPress={onPress}
  style={({pressed,hovered}:any)=>({width:CHROME.control,height:CHROME.control,borderRadius:7,alignItems:'center',justifyContent:'center',
   backgroundColor:hovered?C.dark:'transparent',opacity:pressed?.6:1})}>
  <Icon name={icon} size={CHROME.icon} color={C.muted}/>
 </Pressable>;
}

export type TileProps={w:Widget;spec:WidgetSpec;weight:number;minWidth:number;height:number;phone:boolean;
 dragging:boolean;drop:'before'|'after'|null;flash:boolean;next:Widget|null;
 register:(id:string,el:any)=>void;onDragStart:(id:string)=>void;onDragMove:(x:number,y:number)=>void;onDragEnd:()=>void;
 onEdge:(left:string,right:string,sizes:[LSize,LSize]|null,preview:boolean)=>void;
 onSettings:()=>void;onExpand:()=>void;onRemove:()=>void;onPatch:(p:Partial<Widget>)=>void;
 collapsed?:boolean;onCollapse?:()=>void;expandedView?:boolean;eager?:boolean};

export function Tile(p:TileProps){
 const {w,spec,weight,minWidth,height,phone,dragging,drop,flash,next,expandedView,collapsed}=p;
 const ws=useWorkspace();
 const ref=useRef<any>(null);
 const visible=useVisible(ref,!!p.eager||!!expandedView);
 const [edgeHover,setEdgeHover]=useState(false);
 const [tileW,setTileW]=useState(0);
 const symbol=spec.instrument?instrumentOf(w,ws.selected):'';

 // the responders are made once; everything they call is read through a ref, so nothing is ever stale
 const cb=useRef(p);cb.current=p;
 const drag=useRef(PanResponder.create({
  onStartShouldSetPanResponder:()=>true,onMoveShouldSetPanResponder:()=>true,
  onPanResponderGrant:()=>cb.current.onDragStart(cb.current.w.widget_id),
  onPanResponderMove:e=>cb.current.onDragMove(e.nativeEvent.pageX,e.nativeEvent.pageY),
  onPanResponderRelease:()=>cb.current.onDragEnd(),onPanResponderTerminate:()=>cb.current.onDragEnd(),
 })).current;
 const edge=useRef({left:0,right:0});
 const resize=useRef(PanResponder.create({
  onStartShouldSetPanResponder:()=>true,onMoveShouldSetPanResponder:()=>true,
  onPanResponderGrant:()=>{
   const me=ref.current?.getBoundingClientRect?.(),nb=ref.current?.nextElementSibling?.getBoundingClientRect?.();
   edge.current={left:me?.width||0,right:nb?.width||0};
  },
  onPanResponderMove:(_e,g)=>{const n=cb.current.next;if(!n)return;
   const {left,right}=edge.current;const l=Math.max(40,left+g.dx),r=Math.max(40,right-g.dx);
   cb.current.onEdge(cb.current.w.widget_id,n.widget_id,snap(l,r),true);},
  onPanResponderRelease:(_e,g)=>{const n=cb.current.next;if(!n)return;
   const {left,right}=edge.current;cb.current.onEdge(cb.current.w.widget_id,n.widget_id,snap(left+g.dx,right-g.dx),false);},
  onPanResponderTerminate:()=>{const n=cb.current.next;if(n)cb.current.onEdge(cb.current.w.widget_id,n.widget_id,null,false);},
 })).current;

 // a widget that was just added scrolls into view and says so with one pulse of its border
 useEffect(()=>{if(flash&&web)ref.current?.scrollIntoView?.({block:'nearest',behavior:'smooth'});},[flash]);

 const Body=BODIES[w.widget_type];
 const bodyH=expandedView?900:height-CHROME.header;
 const bodyW=tileW||600;
 const body=!Body?null:bare(<Body w={w} spec={spec} width={bodyW} height={bodyH} expanded={!!expandedView} onPatch={p.onPatch} openSettings={p.onSettings}/>);
 // the header's priority is the TITLE: on a narrow tile the follow / pin chip keeps its icon and gives up its word
 const narrow=tileW>0&&tileW<340;
 const chip=spec.instrument?(w.follow_workspace
  ?<View accessibilityLabel={`Follows the workspace${symbol?`: ${symbol}`:''}`} style={[s.row,{gap:4,paddingHorizontal:6,height:20,borderRadius:6,backgroundColor:C.soft,flexShrink:0}]}>
    <Icon name="link" size={10} color={C.green}/>{!narrow&&<T style={[TYPE.helper,{color:C.green,lineHeight:14}]}>{symbol||'follows'}</T>}</View>
  :<View accessibilityLabel={`Pinned to ${symbol}`} style={[s.row,{gap:4,paddingHorizontal:6,height:20,borderRadius:6,backgroundColor:C.amberBg,flexShrink:0}]}>
    <Icon name="map-pin" size={10} color={C.amber}/>{!narrow&&<T style={[TYPE.helper,{color:C.amber,lineHeight:14}]}>{symbol}</T>}</View>):null;
 const border=flash?C.green:C.line;

 return <View ref={el=>{ref.current=el;p.register(w.widget_id,el)}} onLayout={e=>{const v=Math.round(e.nativeEvent.layout.width);setTileW(x=>x===v?x:v)}} style={{
  flexGrow:phone||expandedView?undefined:weight,flexShrink:1,flexBasis:phone||expandedView?undefined:0,
  minWidth:phone||expandedView?undefined:minWidth,width:phone?'100%':undefined,
  height:expandedView?undefined:(collapsed?CHROME.header+2:height),flex:expandedView?1:undefined,
  borderRadius:CHROME.radius,borderWidth:CHROME.border,borderColor:border,backgroundColor:C.paper,overflow:'hidden',
  opacity:dragging?.4:1,...(web?{transition:'border-color .6s ease, opacity .15s ease'} as any:{})}}>
  {/* header: grip · title · follow/pin · settings · expand · remove. Nothing else, on every widget. */}
  <View style={[s.row,{height:CHROME.header,paddingLeft:4,paddingRight:6,gap:6,borderBottomWidth:collapsed?0:1,borderColor:C.line}]}>
   {!phone&&!expandedView?<View {...drag.panHandlers} accessibilityLabel={`Drag ${spec.label} to move it`}
    style={{width:20,height:CHROME.control,alignItems:'center',justifyContent:'center',...(web?{cursor:'grab'} as any:{})}}>
    <Icon name="more-vertical" size={13} color={C.muted}/></View>:<View style={{width:8}}/>}
   <T numberOfLines={1} role="heading" aria-level={3} style={[TYPE.title,{flexShrink:1}]}>{spec.label}</T>
   {chip}
   <View style={{flex:1}}/>
   {phone&&p.onCollapse&&<Control icon={collapsed?'chevron-down':'chevron-up'} label={collapsed?'Show':'Collapse'} onPress={p.onCollapse}/>}
   <Control icon="settings" label={`${spec.label} settings`} onPress={p.onSettings}/>
   <Control icon={expandedView?'minimize-2':'maximize-2'} label={expandedView?'Close full screen':`Expand ${spec.label}`} onPress={p.onExpand}/>
   {!expandedView&&<Control icon="x" label={`Remove ${spec.label}`} onPress={p.onRemove}/>}
  </View>
  {!collapsed&&(visible?(FIT.has(w.widget_type)&&!expandedView
   // laid out to fit: sized to this box by widgets.tsx fit(), and never given a scroll
   ?<View style={{flex:1,minHeight:0,padding:CHROME.pad,overflow:'hidden'}}>{body}</View>
   :SELF_SCROLLING.has(w.widget_type)&&!expandedView
   ?<View style={{flex:1,minHeight:0}}>{body}</View>
   :<ScrollView style={{flex:1}} contentContainerStyle={{padding:CHROME.pad,gap:CHROME.gap}}>{body}</ScrollView>)
   :<View style={{flex:1}}/>)}
  {/* the drop marker: where the widget being dragged will land */}
  {!!drop&&<View pointerEvents="none" style={{position:'absolute',top:6,bottom:6,width:3,borderRadius:2,backgroundColor:C.green,
   [drop==='before'?'left':'right']:2} as any}/>}
  {/* the shared edge with the right-hand neighbour: drag it and the pair re-splits S / M / L */}
  {!phone&&!expandedView&&!!next&&<View {...resize.panHandlers} accessibilityLabel={`Resize ${spec.label} against ${next?'its neighbour':''}`}
   {...({onMouseEnter:()=>setEdgeHover(true),onMouseLeave:()=>setEdgeHover(false)} as any)}
   style={{position:'absolute',right:-1,top:0,bottom:0,width:8,alignItems:'center',justifyContent:'center',
    ...(web?{cursor:'col-resize'} as any:{})}}>
   <View style={{width:2,height:36,borderRadius:1,backgroundColor:edgeHover?C.green:'transparent'}}/></View>}
 </View>;
}

const snap=(l:number,r:number):[LSize,LSize]=>snapPair(l,r);
export type {Size};
