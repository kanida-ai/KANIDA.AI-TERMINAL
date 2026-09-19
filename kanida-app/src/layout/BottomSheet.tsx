import React from 'react';
import {View,Animated,PanResponder,useWindowDimensions} from 'react-native';
import {useSafeAreaInsets} from 'react-native-safe-area-context';
import {C,s} from '../ui';
import {web,webOnly,IconButton,clamp} from './shared';
export type SheetSnap='peek'|'half'|'full';
export const SHEET_SNAPS:SheetSnap[]=['peek','half','full'];
export type BottomSheetProps={snap:SheetSnap;onSnapChange:(snap:SheetSnap)=>void;header:React.ReactNode;children?:React.ReactNode;peekHeight?:number;label?:string;halfRatio?:number;fullRatio?:number};
const GRAB=14,SLOP=6,FLING=.5;
// Phone bottom sheet (non-modal): absolutely positioned at the bottom of its parent, so the chart behind stays visible and interactive at peek (header only, peekHeight + safe-area) and half (50% of the window); full = 92%.
// Drag the grab strip or header: pointer events on web (capture only after a 6px move, so header buttons still click), PanResponder on native (claims only vertical moves). During a drag only translateY moves; release snaps to the nearest point (or the next one in a fling direction) and reports onSnapChange once. Expand/Collapse buttons give an accessible alternative.
export function BottomSheet({snap,onSnapChange,header,children,peekHeight=64,label='Panel',halfRatio=.5,fullRatio=.92}:BottomSheetProps){
 const {height:H}=useWindowDimensions();const inset=useSafeAreaInsets();
 const fullH=Math.round(H*fullRatio);
 const shown=React.useCallback((k:SheetSnap)=>k==='peek'?peekHeight+inset.bottom:k==='half'?Math.round(H*halfRatio):fullH,[peekHeight,inset.bottom,H,halfRatio,fullH]);
 const y=React.useRef(new Animated.Value(fullH-shown(snap))).current;
 const live=React.useRef({fullH,shown,snap,onSnapChange});live.current={fullH,shown,snap,onSnapChange};
 const settle=React.useCallback((to:SheetSnap)=>{Animated.spring(y,{toValue:live.current.fullH-live.current.shown(to),useNativeDriver:!web,bounciness:0,speed:20}).start();},[y]);
 React.useEffect(()=>{settle(snap);},[snap,shown,settle]);
 const drag=React.useRef({armed:false,on:false,origin:0,start:0,last:0,t:0,v:0});
 const api=React.useRef({
  arm(origin:number){drag.current={armed:true,on:false,origin,start:0,last:origin,t:Date.now(),v:0};},
  begin(){const d=drag.current,L=live.current;d.on=true;d.start=L.fullH-L.shown(L.snap);y.stopAnimation((v:number)=>{if(Number.isFinite(v))d.start=v;});},
  move(pos:number){const d=drag.current;if(!d.on)return;const L=live.current,now=Date.now();d.v=(pos-d.last)/Math.max(1,now-d.t);d.last=pos;d.t=now;y.setValue(clamp(d.start+pos-d.origin,0,L.fullH-L.shown('peek')));},
  end(pos:number,v?:number){const d=drag.current;d.armed=false;if(!d.on)return;d.on=false;const L=live.current;const vis=L.fullH-clamp(d.start+pos-d.origin,0,L.fullH-L.shown('peek'));const vel=v??d.v;
   let to:SheetSnap=SHEET_SNAPS.reduce((b,k)=>Math.abs(L.shown(k)-vis)<Math.abs(L.shown(b)-vis)?k:b,L.snap);
   if(vel<-FLING)to=SHEET_SNAPS.find(k=>L.shown(k)>vis+8)??'full';else if(vel>FLING)to=[...SHEET_SNAPS].reverse().find(k=>L.shown(k)<vis-8)??'peek';
   settle(to);if(to!==L.snap)L.onSnapChange(to);},
 }).current;
 const pan=React.useRef(PanResponder.create({onStartShouldSetPanResponder:()=>false,onMoveShouldSetPanResponder:(_,g)=>Math.abs(g.dy)>SLOP&&Math.abs(g.dy)>Math.abs(g.dx),onPanResponderGrant:()=>{api.arm(0);api.begin();},onPanResponderMove:(_,g)=>api.move(g.dy),onPanResponderRelease:(_,g)=>api.end(g.dy,g.vy),onPanResponderTerminate:(_,g)=>api.end(g.dy,g.vy)})).current;
 // F7 (mouse on web): pointerdown on the header arms, then pointermove/pointerup/pointercancel are followed on WINDOW (so a mouse that leaves the header, or events a child Pressable swallows, still
 // drive the drag), with pointer capture + no text selection once the 6px slop is crossed. The click that ends a real drag is swallowed once, so releasing over a tab doesn't also switch tabs.
 const unbind=React.useRef<(()=>void)|null>(null);
 React.useEffect(()=>()=>unbind.current?.(),[]);
 const webDrag=webOnly({
  onPointerDown:(e:any)=>{if(e.button>0)return;unbind.current?.();api.arm(e.clientY);
   const win=(globalThis as any).window,doc=win?.document;if(!win||typeof win.addEventListener!=='function')return;
   const id=e.pointerId,target=e.currentTarget,body=doc?.body,prevSelect=body?.style?.userSelect??'';
   const move=(ev:any)=>{const d=drag.current;if(ev.pointerId!==id||!d.armed)return;if(ev.pointerType==='mouse'&&ev.buttons===0){up(ev);return;}
    if(!d.on){if(Math.abs(ev.clientY-d.origin)<SLOP)return;try{target?.setPointerCapture?.(id);}catch{}if(body)body.style.userSelect='none';api.begin();}
    if(ev.cancelable)ev.preventDefault();api.move(ev.clientY);};
   const up=(ev:any)=>{if(ev.pointerId!==id)return;const dragged=drag.current.on;off();api.end(ev.clientY);
    if(dragged&&ev.type==='pointerup'){const swallow=(c:any)=>{c.stopPropagation();c.preventDefault();win.removeEventListener('click',swallow,true);};win.addEventListener('click',swallow,true);setTimeout(()=>win.removeEventListener('click',swallow,true),0);}};
   const off=()=>{win.removeEventListener('pointermove',move);win.removeEventListener('pointerup',up);win.removeEventListener('pointercancel',up);if(body)body.style.userSelect=prevSelect;try{target?.releasePointerCapture?.(id);}catch{}unbind.current=null;};
   win.addEventListener('pointermove',move,{passive:false});win.addEventListener('pointerup',up);win.addEventListener('pointercancel',up);unbind.current=()=>{off();drag.current.armed=false;drag.current.on=false;};
  },
 });
 const i=SHEET_SNAPS.indexOf(snap);const [headH,setHeadH]=React.useState(peekHeight);
 const state=snap==='peek'?'collapsed':snap==='half'?'half open':'full screen';
 return <Animated.View role="region" aria-label={`${label}, ${state}`} style={{position:'absolute',left:0,right:0,bottom:0,height:fullH,transform:[{translateY:y}],backgroundColor:C.bg,borderTopLeftRadius:20,borderTopRightRadius:20,borderTopWidth:1,borderColor:C.line,shadowColor:'#000',shadowOpacity:.5,shadowRadius:20,shadowOffset:{width:0,height:-6},elevation:16,zIndex:30}}>
  <View onLayout={e=>{const hh=Math.round(e.nativeEvent.layout.height);if(Math.abs(hh-headH)>1)setHeadH(hh);}} {...webDrag} {...(web?{}:pan.panHandlers)} style={web?({touchAction:'pan-x',cursor:'grab'} as any):undefined}>
   <View aria-hidden style={{alignItems:'center',paddingTop:6,height:GRAB}}><View style={{width:40,height:4,borderRadius:2,backgroundColor:C.line}}/></View>
   <View style={[s.row,{minHeight:Math.max(40,peekHeight-GRAB),paddingLeft:16,paddingRight:6,gap:2}]}>
    <View style={{flex:1,minWidth:0}}>{header}</View>
    <IconButton icon="chevron-down" label={`Collapse ${label}`} tooltip="Collapse" disabled={i<=0} onPress={()=>onSnapChange(SHEET_SNAPS[Math.max(0,i-1)])}/>
    <IconButton icon="chevron-up" label={`Expand ${label}`} tooltip="Expand" disabled={i>=2} onPress={()=>onSnapChange(SHEET_SNAPS[Math.min(2,i+1)])}/>
   </View>
  </View>
  <View aria-hidden={snap==='peek'} importantForAccessibility={snap==='peek'?'no-hide-descendants':'auto'} style={{height:Math.max(0,shown(snap)-headH),minHeight:0,overflow:'hidden'}}>{children}</View>
 </Animated.View>;
}
