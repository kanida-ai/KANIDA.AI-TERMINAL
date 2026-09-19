import React from 'react';
import {View,Pressable,ScrollView} from 'react-native';
import {C,T,Icon,useLayoutMode} from '../ui';
import {webOnly,useTitle,useRoving} from './shared';
export type ToolRailItem={key:string;label:string;icon:string;onPress:()=>void;active?:boolean;shortcut?:string;disabled?:boolean};
export type ToolRailProps={items:ToolRailItem[];label?:string;orientation?:'vertical'|'horizontal';compact?:boolean;header?:React.ReactNode;footer?:React.ReactNode;style?:any};
function RailButton({item,index,vertical,compact,tabbable,onKeyDown,refs}:{item:ToolRailItem;index:number;vertical:boolean;compact:boolean;tabbable:boolean;onKeyDown:(i:number,e:any)=>void;refs:React.RefObject<any[]>}){
 const tip=item.shortcut?`${item.label} (${item.shortcut})`:item.label;const tref=useTitle(tip);const on=!!item.active;
 const setRef=React.useCallback((el:any)=>{tref.current=el;refs.current[index]=el;},[index]);
 return <Pressable ref={setRef} accessibilityRole="button" accessibilityLabel={item.label} accessibilityHint={item.shortcut?`Shortcut ${item.shortcut}`:undefined} accessibilityState={{selected:on,disabled:!!item.disabled}} disabled={item.disabled} {...webOnly({'aria-pressed':on,'aria-keyshortcuts':item.shortcut,tabIndex:tabbable?0:-1,onKeyDown:(e:any)=>onKeyDown(index,e)})} onPress={item.onPress} style={(st:any)=>[{alignItems:'center',justifyContent:'center',gap:3,minHeight:compact?40:54,minWidth:vertical?undefined:compact?44:64,paddingHorizontal:4,paddingVertical:6,borderRadius:10,borderWidth:1,borderColor:on?C.green:'transparent',backgroundColor:on?C.soft:st.hovered||st.focused?C.paper:'transparent',opacity:item.disabled?.4:st.pressed?.7:1}]}>
  <Icon name={item.icon} size={18} color={on?C.green:C.muted}/>
  {!compact&&<T numberOfLines={2} style={{fontSize:10,lineHeight:12,textAlign:'center',fontFamily:'InterMedium',color:on?C.green:C.muted}}>{item.label}</T>}
 </Pressable>;
}
// Labelled tool rail (left drawing rail, sidebar widget rail). role=toolbar with aria-orientation; each button has an accessible name, a visible label under the icon (compact hides it but keeps the name + tooltip), a web title tooltip incl. shortcut, aria-keyshortcuts and aria-pressed. Arrow keys move focus along the rail; Tab enters at the active item. Default orientation: vertical, horizontal (scrolling row) on phone.
export function ToolRail({items,label='Tools',orientation,compact=false,header,footer,style}:ToolRailProps){
 const phone=useLayoutMode()==='phone';const dir=orientation??(phone?'horizontal':'vertical');const vertical=dir==='vertical';
 const roving=useRoving(items.length,dir);const tabIdx=Math.max(0,items.findIndex(i=>i.active));
 const list=<View role="toolbar" aria-label={label} {...webOnly({'aria-orientation':dir})} style={vertical?{gap:4}:{flexDirection:'row',gap:4}}>{items.map((it,i)=><RailButton key={it.key} item={it} index={i} vertical={vertical} compact={compact} tabbable={i===tabIdx} onKeyDown={roving.onKeyDown} refs={roving.refs}/>)}</View>;
 if(!vertical)return <View style={[{flexDirection:'row',alignItems:'center',gap:4},style]}>{header}<ScrollView horizontal showsHorizontalScrollIndicator={false} style={{flex:1}} contentContainerStyle={{paddingHorizontal:8}}>{list}</ScrollView>{footer}</View>;
 return <View style={[{width:compact?50:70,backgroundColor:C.bg,paddingVertical:6,paddingHorizontal:4,gap:6,alignItems:'stretch'},style]}>{!!header&&<View style={{alignItems:'center'}}>{header}</View>}<ScrollView style={{flex:1}} showsVerticalScrollIndicator={false}>{list}</ScrollView>{!!footer&&<View style={{alignItems:'center'}}>{footer}</View>}</View>;
}
