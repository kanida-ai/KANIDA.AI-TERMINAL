import React from 'react';
import {Platform,Pressable,Animated,PanResponder} from 'react-native';
import {C,Icon} from '../ui';
// Shared plumbing for the layout primitives: web-only DOM props, tooltips, localStorage persistence, roving focus, icon buttons and ref-driven resize drags.
export const web=Platform.OS==='web';
export const clamp=(v:number,lo:number,hi:number)=>Math.max(lo,Math.min(hi,v));
export const domId=(v:string)=>v.replace(/[^a-zA-Z0-9_-]/g,'_');
// DOM-only props (aria-* RN types omit, tabIndex, onKeyDown, pointer events). Native gets nothing; undefined values are dropped so RN-web never writes "undefined" attributes.
export function webOnly(props:Record<string,any>):any{if(!web)return {};const out:Record<string,any>={};for(const k in props)if(props[k]!==undefined)out[k]=props[k];return out;}
// Persistence: key = `kanida.layout.<kind>.<persistKey>`, JSON value. Any storage failure (native, private mode, quota) falls back to props.
export const storeKey=(kind:string,key:string)=>`kanida.layout.${kind}.${key}`;
export function readStore<V extends object>(kind:string,key?:string):Partial<V>{if(!key)return {};try{const raw=(globalThis as any).localStorage?.getItem(storeKey(kind,key));const v=raw?JSON.parse(raw):null;return v&&typeof v==='object'?v:{};}catch{return {};}}
export function writeStore(kind:string,key:string|undefined,value:object){if(!key)return;try{(globalThis as any).localStorage?.setItem(storeKey(kind,key),JSON.stringify(value));}catch{}}
export function useMergedRef<V>(...refs:(React.Ref<V>|undefined)[]){return React.useCallback((el:V|null)=>{for(const r of refs){if(typeof r==='function')r(el);else if(r)(r as any).current=el;}},refs);}
// RN-web does not forward `title`, so the tooltip is written onto the host DOM node after each render (cheap attribute compare).
export function useTitle(text?:string){const ref=React.useRef<any>(null);React.useEffect(()=>{const el=ref.current;if(!web||!el||typeof el.setAttribute!=='function')return;const cur=el.getAttribute('title');if(text&&cur!==text)el.setAttribute('title',text);else if(!text&&cur!=null)el.removeAttribute('title');});return ref;}
// Roving focus for tab lists, toolbars and menus: arrows move focus (wrapping), Home/End jump; `activate` lets tabs follow focus.
export function useRoving(count:number,orientation:'horizontal'|'vertical'){
 const refs=React.useRef<any[]>([]);
 const onKeyDown=React.useCallback((i:number,e:any,activate?:(j:number)=>void)=>{const k=e?.key??e?.nativeEvent?.key;const fwd=orientation==='horizontal'?'ArrowRight':'ArrowDown',back=orientation==='horizontal'?'ArrowLeft':'ArrowUp';let j=-1;if(k===fwd)j=(i+1)%count;else if(k===back)j=(i-1+count)%count;else if(k==='Home')j=0;else if(k==='End')j=count-1;if(j<0||!count)return;e.preventDefault?.();refs.current[j]?.focus?.();activate?.(j);},[count,orientation]);
 return {refs,onKeyDown};
}
export type IconButtonProps={icon:string;label:string;onPress:()=>void;tooltip?:string;pressed?:boolean;expanded?:boolean;haspopup?:'menu'|'dialog'|'listbox'|boolean;controls?:string;disabled?:boolean;color?:string;size?:number;style?:any};
// Square icon button: whole 36px box is the hit target (hitSlop adds more on native), accessible name always set, tooltip via title on web, press-only activation.
export const IconButton=React.forwardRef<any,IconButtonProps>(function IconButton({icon,label,onPress,tooltip,pressed,expanded,haspopup,controls,disabled=false,color,size=16,style},fwd){
 const tip=useTitle(tooltip??label);const ref=useMergedRef(fwd,tip);const state:any={disabled};if(pressed!=null)state.selected=pressed;if(expanded!=null)state.expanded=expanded;
 return <Pressable ref={ref} accessibilityRole="button" accessibilityLabel={label} accessibilityState={state} disabled={disabled} {...webOnly({'aria-pressed':pressed,'aria-expanded':expanded,'aria-haspopup':haspopup,'aria-controls':controls})} onPress={onPress} hitSlop={6} style={(st:any)=>[{width:36,height:36,alignItems:'center',justifyContent:'center',borderRadius:8,backgroundColor:pressed?C.soft:st.hovered||st.focused?C.paper:'transparent',opacity:disabled?.4:st.pressed?.7:1},style]}><Icon name={icon} size={size} color={color??(pressed?C.green:C.muted)}/></Pressable>;
});
export type DragResizeOpts={axis:'x'|'y';value:number;min:number;max:number;onCommit:(v:number)=>void;label:string;step?:number;onEnter?:()=>void};
// Resize handle for a panel that grows AWAY from the pointer's travel (bottom dock grows as the pointer moves up, right sidebar as it moves left).
// During a drag only an Animated ghost line moves (no React state, no child re-render); the size is committed once on release. Web: pointer capture + separator keyboard (arrows, Shift = 4x, Home/End, Enter). Native: PanResponder + adjustable increment/decrement actions.
export function useDragResize(o:DragResizeOpts){
 const live=React.useRef(o);live.current=o;
 const offset=React.useRef(new Animated.Value(0)).current,show=React.useRef(new Animated.Value(0)).current;
 const st=React.useRef({on:false,origin:0,start:0,next:0});
 const api=React.useRef({
  begin(origin:number){const L=live.current;st.current={on:true,origin,start:L.value,next:L.value};offset.setValue(0);show.setValue(1);},
  move(pos:number){const d=st.current;if(!d.on)return;const L=live.current;d.next=clamp(d.start-(pos-d.origin),L.min,L.max);offset.setValue(d.start-d.next);},
  end(){const d=st.current;if(!d.on)return;d.on=false;show.setValue(0);offset.setValue(0);const v=Math.round(d.next);if(v!==Math.round(live.current.value))live.current.onCommit(v);},
 }).current;
 const pan=React.useRef(PanResponder.create({onStartShouldSetPanResponder:()=>true,onMoveShouldSetPanResponder:()=>true,onPanResponderTerminationRequest:()=>false,onPanResponderGrant:()=>api.begin(0),onPanResponderMove:(_,g)=>api.move(live.current.axis==='y'?g.dy:g.dx),onPanResponderRelease:()=>api.end(),onPanResponderTerminate:()=>api.end()})).current;
 const {axis,value,min,max,label,step=24}=o;const now=Math.round(value);
 const nudge=(d:number)=>{const L=live.current;const v=Math.round(clamp(L.value+d,L.min,L.max));if(v!==Math.round(L.value))L.onCommit(v);};
 const pos=(e:any)=>axis==='y'?e.clientY:e.clientX;const grow=axis==='y'?'ArrowUp':'ArrowLeft',shrink=axis==='y'?'ArrowDown':'ArrowRight';
 const handleProps:any=web?{role:'separator','aria-orientation':axis==='y'?'horizontal':'vertical',tabIndex:0,'aria-label':label,'aria-valuenow':now,'aria-valuemin':min,'aria-valuemax':max,'aria-valuetext':`${now} pixels`,
  onPointerDown:(e:any)=>{if(e.button>0)return;e.preventDefault();try{e.currentTarget.setPointerCapture(e.pointerId);}catch{}api.begin(pos(e));},
  onPointerMove:(e:any)=>api.move(pos(e)),onPointerUp:()=>api.end(),onPointerCancel:()=>api.end(),onLostPointerCapture:()=>api.end(),
  onKeyDown:(e:any)=>{const k=e.key,big=e.shiftKey?step*4:step;if(k===grow)nudge(big);else if(k===shrink)nudge(-big);else if(k==='Home')nudge(min-value);else if(k==='End')nudge(max-value);else if((k==='Enter'||k===' ')&&live.current.onEnter)live.current.onEnter();else return;e.preventDefault();}}
  :{accessibilityRole:'adjustable',accessibilityLabel:label,accessibilityValue:{min,max,now},accessibilityActions:[{name:'increment',label:'Larger'},{name:'decrement',label:'Smaller'}],onAccessibilityAction:(e:any)=>nudge(e.nativeEvent.actionName==='increment'?step*2:-step*2),...pan.panHandlers};
 const ghostStyle:any={position:'absolute',zIndex:50,pointerEvents:'none',backgroundColor:C.green,opacity:show,...(axis==='y'?{left:0,right:0,top:-1,height:2,transform:[{translateY:offset}]}:{top:0,bottom:0,left:-1,width:2,transform:[{translateX:offset}]})};
 const handleStyle:any=web?{cursor:axis==='y'?'row-resize':'col-resize',touchAction:'none',userSelect:'none'}:null;
 return {handleProps,handleStyle,ghostStyle};
}
