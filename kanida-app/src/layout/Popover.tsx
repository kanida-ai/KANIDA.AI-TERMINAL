import React from 'react';
import {View,Pressable,Modal,StyleSheet,useWindowDimensions} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {web,webOnly,useRoving,clamp} from './shared';
// Only the most recently opened dismissable reacts to Escape, so a popover inside a Sheet closes alone.
const escStack:number[]=[];let escSeq=0;
export type DismissOptions={routeKey?:unknown;insideRefs?:React.RefObject<any>[];escape?:boolean};
// useDismiss(open,onClose): web Escape (keydown is swallowed, close fires on the matching keyup, and both are stopped in the window capture phase so RN-web Modal's document keyup listener of an underlying Sheet never also closes), browser back/forward (popstate), and a change of opts.routeKey (pass usePathname()).
// Non-modal use: pass insideRefs (panel AND trigger) to close on outside pointerdown; including the trigger stops its own press from re-opening what the outside press just closed.
export function useDismiss(open:boolean,onClose:()=>void,opts:DismissOptions={}){
 const cb=React.useRef(onClose);cb.current=onClose;const inside=React.useRef(opts.insideRefs);inside.current=opts.insideRefs;
 const outside=!!opts.insideRefs?.length,escape=opts.escape!==false;
 React.useEffect(()=>{
  if(!open||!web||typeof window==='undefined')return;
  const id=++escSeq;escStack.push(id);let armed=false;const top=()=>escStack[escStack.length-1]===id;
  const down=(e:KeyboardEvent)=>{if(!escape||e.key!=='Escape'||!top())return;armed=true;e.preventDefault();e.stopPropagation();};
  const up=(e:KeyboardEvent)=>{if(!escape||e.key!=='Escape'||!top())return;e.stopPropagation();if(armed){armed=false;cb.current();}};
  const press=(e:PointerEvent)=>{const t=e.target as any;if(inside.current?.some(r=>r.current?.contains?.(t)))return;if(top())cb.current();};
  const pop=()=>cb.current();
  window.addEventListener('keydown',down,true);window.addEventListener('keyup',up,true);window.addEventListener('popstate',pop);if(outside)window.addEventListener('pointerdown',press,true);
  return ()=>{const i=escStack.indexOf(id);if(i>=0)escStack.splice(i,1);window.removeEventListener('keydown',down,true);window.removeEventListener('keyup',up,true);window.removeEventListener('popstate',pop);window.removeEventListener('pointerdown',press,true);};
 },[open,outside,escape]);
 const route=React.useRef(opts.routeKey);
 React.useEffect(()=>{if(route.current===opts.routeKey)return;route.current=opts.routeKey;if(open)cb.current();},[opts.routeKey,open]);
}
export type PopoverRect={x:number;y:number;width?:number;height?:number};
export type PopoverAnchor=PopoverRect|React.RefObject<any>;
export type PopoverPlacement='bottom-start'|'bottom-end'|'top-start'|'top-end'|'right-start'|'left-start';
export type PopoverProps={open:boolean;onClose:()=>void;anchor?:PopoverAnchor;children?:React.ReactNode;placement?:PopoverPlacement;label:string;width?:number;routeKey?:unknown;role?:'dialog'|'menu'|'none'};
const isRef=(a:any):a is React.RefObject<any>=>!!a&&typeof a==='object'&&'current' in a;
/** How much roomier the opposite side must be before a clipped panel flips to it. */
const FLIP=1.5;
// Popover: rendered in a transparent Modal (escapes overflow clipping, native + web). A full-screen backdrop takes every outside press, so a second click on the trigger (or on anything under the panel) only closes it and can never select what is underneath. Flips to the other side when there is no room, then clamps inside the window.
export function Popover({open,onClose,anchor,children,placement='bottom-start',label,width=240,routeKey,role='dialog'}:PopoverProps){
 const {width:W,height:H}=useWindowDimensions();
 useDismiss(open,onClose,{routeKey});
 const [rect,setRect]=React.useState<Required<PopoverRect>|null>(null);const [size,setSize]=React.useState<{w:number;h:number}|null>(null);
 const ref=isRef(anchor)?anchor:null,pt=isRef(anchor)?null:anchor;const ax=pt?.x,ay=pt?.y,aw=pt?.width??0,ah=pt?.height??0;
 React.useEffect(()=>{
  if(!open){setSize(null);return;}
  if(ref){const el=ref.current;if(!el){setRect(null);return;}if(web&&typeof el.getBoundingClientRect==='function'){const r=el.getBoundingClientRect();setRect({x:r.left,y:r.top,width:r.width,height:r.height});}else if(typeof el.measureInWindow==='function')el.measureInWindow((x:number,y:number,w:number,h:number)=>setRect({x,y,width:w,height:h}));else setRect(null);}
  else setRect(ax==null||ay==null?null:{x:ax,y:ay,width:aw,height:ah});
 },[open,ref,ax,ay,aw,ah,W,H]);
 const was=React.useRef(open);
 React.useEffect(()=>{if(was.current&&!open&&web&&ref)ref.current?.focus?.();was.current=open;},[open]);
 if(!open)return null;
 const pw=size?.w??width,ph=size?.h??0,gap=6,m=8;let left=(W-pw)/2,top=H/3,maxH=H-2*m;
 if(rect){const [side,align]=placement.split('-');
  // Vertical: keep the preferred side and cap the panel to the room on that side (content scrolls), so a tall panel never slides over its own anchor. Flip only when the panel is clipped and the other side has more room.
  // A panel whose content is longer than the window (a long strategy list) always measures exactly as tall as
  // the room it was last given, so "clipped" is true on BOTH sides and the side with a few more pixels would
  // win. That flipped the picker above its own title as soon as its card sat past the middle of the page.
  // The preferred side is kept unless the other one is MATERIALLY roomier (FLIP x), so a flip means the
  // anchor really is near that edge of the window, not that the two sides are almost equal.
  if(side==='bottom'||side==='top'){left=align==='end'?rect.x+rect.width-pw:rect.x;const below=rect.y+rect.height+gap,roomB=H-m-below,roomA=rect.y-gap-m,clipB=ph>=roomB-1&&roomA>=roomB*FLIP,clipA=ph>=roomA-1&&roomB>=roomA*FLIP;const up=side==='bottom'?clipB:!clipA;maxH=Math.max(96,up?roomA:roomB);top=up?rect.y-gap-Math.min(ph,maxH):below;}
  else{top=rect.y;const right=rect.x+rect.width+gap,leftOf=rect.x-pw-gap;left=side==='right'?(right+pw>W-m&&leftOf>=m?leftOf:right):(leftOf<m?right:leftOf);}
 }
 left=clamp(left,m,Math.max(m,W-pw-m));top=clamp(top,m,Math.max(m,H-ph-m));
 return <Modal visible transparent animationType="none" onRequestClose={onClose} statusBarTranslucent>
  <View style={StyleSheet.absoluteFill}>
   <Pressable aria-hidden accessible={false} importantForAccessibility="no-hide-descendants" focusable={false} onPress={onClose} style={StyleSheet.absoluteFill}/>
   <View role={role} aria-label={role==='none'?undefined:label} accessibilityViewIsModal onLayout={e=>{const {width:w,height:h}=e.nativeEvent.layout;if(!size||Math.abs(size.w-w)>1||Math.abs(size.h-h)>1)setSize({w,h});}} style={{position:'absolute',left,top,opacity:size?1:0,width,maxWidth:W-2*m,maxHeight:maxH,backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:12,paddingVertical:6,overflow:'hidden',shadowColor:'#000',shadowOpacity:.45,shadowRadius:18,shadowOffset:{width:0,height:8},elevation:12}}>{children}</View>
  </View>
 </Modal>;
}
export type MenuItem={label:string;onPress:()=>void;danger?:boolean;disabled?:boolean;icon?:string};
// Menu list for popovers: role=menu/menuitem, first item focused on open (web), Arrow Up/Down + Home/End roving focus, Enter/Space activate (Pressable). Closes before running the action.
export function MenuList({items,onClose,label}:{items:MenuItem[];onClose?:()=>void;label:string}){
 const roving=useRoving(items.length,'vertical');
 React.useEffect(()=>{if(!web)return;const t=setTimeout(()=>roving.refs.current[0]?.focus?.(),0);return ()=>clearTimeout(t);},[]);
 return <View role="menu" aria-label={label} {...webOnly({'aria-orientation':'vertical'})}>{items.map((it,i)=><Pressable key={it.label+i} ref={(el:any)=>{roving.refs.current[i]=el;}} role="menuitem" aria-disabled={!!it.disabled} accessibilityState={{disabled:!!it.disabled}} accessibilityLabel={it.label} disabled={it.disabled} {...webOnly({tabIndex:i===0?0:-1,onKeyDown:(e:any)=>roving.onKeyDown(i,e)})} onPress={()=>{onClose?.();it.onPress();}} style={(st:any)=>[s.row,{minHeight:40,paddingHorizontal:14,gap:10,backgroundColor:st.hovered||st.focused||st.pressed?C.soft:'transparent',opacity:it.disabled?.45:1}]}>{!!it.icon&&<Icon name={it.icon} size={15} color={it.danger?C.red:C.muted}/>}<T style={{fontSize:13,color:it.danger?C.red:C.ink}}>{it.label}</T></Pressable>)}</View>;
}
