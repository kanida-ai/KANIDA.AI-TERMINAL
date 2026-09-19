// Small shared pieces for Discover Strategies: card frame, header chip button, radio menu, skeleton, link text, coming-soon and stale lines.
import React from 'react';
import {View,Pressable} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {useRoving} from '../layout';
import {web,webOnly} from '../layout/shared';
import {cardWarning} from '../layout/dataStatus';
import {useProduct} from '../context';
export {web,webOnly};
export const BLOCK_COLORS=[C.green,C.amber,'#7FB7FF','#C9A2FF'];
export function CardShell({label,header,children,style,footer}:{label:string;header:React.ReactNode;children?:React.ReactNode;style?:any;footer?:React.ReactNode}){
 return <View role="region" aria-label={label} style={[{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:12,overflow:'hidden',minWidth:0},style]}>
  <View style={[s.row,{paddingLeft:12,paddingRight:2,minHeight:44,borderBottomWidth:1,borderColor:C.line,gap:4}]}>{header}</View>
  <View style={{flex:1,minHeight:0}}>{children}</View>
  {footer!=null&&footer!==false&&<View style={{borderTopWidth:1,borderColor:C.line,paddingHorizontal:12,paddingVertical:7,gap:3}}>{footer}</View>}
 </View>;
}
export const HeaderChip=React.forwardRef<any,{label:string;a11y:string;onPress:()=>void;expanded?:boolean;icon?:string;tone?:string;style?:any}>(function HeaderChip({label,a11y,onPress,expanded,icon='chevron-down',tone,style},ref){
 return <Pressable ref={ref} accessibilityRole="button" accessibilityLabel={a11y} {...webOnly({'aria-haspopup':expanded==null?undefined:'menu','aria-expanded':expanded})} onPress={onPress} style={(st:any)=>[s.row,{gap:6,minHeight:32,paddingHorizontal:10,borderRadius:8,borderWidth:1,borderColor:expanded?C.green:C.line,backgroundColor:st.hovered||st.focused?C.soft:C.paper,opacity:st.pressed?.7:1},style]}>
  <T numberOfLines={1} style={{fontSize:12,lineHeight:16,fontFamily:'InterSemi',color:tone||C.ink}}>{label}</T>{!!icon&&<Icon name={icon} size={14} color={C.muted}/>}
 </Pressable>;
});
export type RadioOption={value:string;label:string;detail?:string;disabled?:boolean};
// Radio menu for popovers: role menuitemradio + aria-checked, roving arrows, first/checked item focused on open.
export function RadioMenu({label,options,value,onPick,onClose,note}:{label:string;options:RadioOption[];value:string;onPick:(v:string)=>void;onClose:()=>void;note?:string}){
 const roving=useRoving(options.length,'vertical');
 React.useEffect(()=>{if(!web)return;const i=Math.max(0,options.findIndex(o=>o.value===value));const t=setTimeout(()=>roving.refs.current[i]?.focus?.(),0);return ()=>clearTimeout(t)},[]);
 return <View role="menu" aria-label={label}>
  {options.map((o,i)=>{const on=o.value===value;return <Pressable key={o.value} ref={(el:any)=>{roving.refs.current[i]=el}} role="menuitemradio" aria-checked={on} aria-disabled={!!o.disabled} accessibilityLabel={o.label} accessibilityState={{checked:on,disabled:!!o.disabled}} disabled={o.disabled} {...webOnly({tabIndex:on?0:-1,onKeyDown:(e:any)=>roving.onKeyDown(i,e)})} onPress={()=>{onClose();if(!on)onPick(o.value)}} style={(st:any)=>[s.row,{minHeight:40,paddingHorizontal:14,gap:10,backgroundColor:st.hovered||st.focused?C.soft:'transparent',opacity:o.disabled?.45:1}]}>
   <View style={{width:15}}>{on&&<Icon name="check" size={15} color={C.green}/>}</View>
   <View style={{flex:1}}><T style={{fontSize:13,color:on?C.green:C.ink}}>{o.label}</T>{!!o.detail&&<T style={{fontSize:11,color:C.muted}}>{o.detail}</T>}</View>
  </Pressable>})}
  {!!note&&<T style={{fontSize:11,color:C.muted,paddingHorizontal:14,paddingVertical:6}}>{note}</T>}
 </View>;
}
export function Skeleton({lines=4,chart=false}:{lines?:number;chart?:boolean}){
 return <View aria-hidden style={{flex:1,padding:12,gap:10}}>{chart&&<View style={{flex:1,minHeight:120,borderRadius:8,backgroundColor:C.soft,opacity:.5}}/>}{Array.from({length:lines}).map((_,i)=><View key={i} style={{height:12,width:`${90-i*14}%` as any,borderRadius:4,backgroundColor:C.line}}/>)}</View>;
}
export function LinkText({label,a11y,onPress,disabled}:{label:string;a11y?:string;onPress:()=>void;disabled?:boolean}){
 return <Pressable accessibilityRole="link" accessibilityLabel={a11y||label} accessibilityState={{disabled:!!disabled}} disabled={disabled} onPress={onPress} hitSlop={6} style={(st:any)=>({minHeight:28,justifyContent:'center',opacity:disabled?.4:st.pressed?.7:1})}>{(st:any)=><T numberOfLines={1} style={{fontSize:12,lineHeight:16,fontFamily:'InterSemi',color:C.green,textDecorationLine:st.hovered||st.focused?'underline':'none'}}>{label}</T>}</Pressable>;
}
export function CenterNote({icon='mouse-pointer',text,children}:{icon?:string;text:string;children?:React.ReactNode}){
 return <View style={{flex:1,alignItems:'center',justifyContent:'center',padding:20,gap:10}}><Icon name={icon} size={20} color={C.muted}/><T style={{fontSize:13,color:C.muted,textAlign:'center',maxWidth:300}}>{text}</T>{children}</View>;
}
// Same story as the top bar (layout/dataStatus.ts): a live feed that is current never reads as 'N days old'.
export function StaleLine({stale,ageDays}:{stale?:boolean;ageDays?:number|null}){
 const status=useProduct()?.state?.data_status;
 if(!stale)return null;
 const text=cardWarning(status,{stale,ageDays});if(!text)return null;
 return <View style={[s.row,{gap:6}]}><Icon name="alert-circle" size={12} color={C.amber}/><T style={{fontSize:11,lineHeight:15,color:C.amber,flex:1}}>{text}</T></View>;
}
export function ComingSoonCard({title,detail,style}:{title:string;detail:string;style?:any}){
 return <CardShell label={title} style={style} header={<T numberOfLines={1} style={{flex:1,fontFamily:'InterSemi',fontSize:11,letterSpacing:1,textTransform:'uppercase'}}>{title}</T>}><CenterNote icon="clock" text={detail}/></CardShell>;
}
