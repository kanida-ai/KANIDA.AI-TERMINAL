import React from 'react';
import {View,Pressable,Linking} from 'react-native';
import {C,T,Icon,Button,s} from '../ui';
import {web,webOnly,IconButton} from './shared';
import {Popover,MenuList,type MenuItem} from './Popover';
export type WidgetMenuItem=MenuItem;
export type WidgetStatusLink=string|{label?:string;href?:string;onPress?:()=>void};
export type WidgetErrorState={message:string;onRetry?:()=>void;statusLink?:WidgetStatusLink;title?:string};
// Standard explicit error state: warning title + message, Retry when provided, optional status link. Never a blank widget.
export function WidgetError({message,onRetry,statusLink,title='Could not load'}:WidgetErrorState){
 const link=typeof statusLink==='string'?{label:'Service status',href:statusLink}:statusLink;
 const webHref=web&&!!link?.href&&!link?.onPress;
 const open=()=>{if(link?.onPress)link.onPress();else if(link?.href)Linking.openURL(link.href).catch(()=>{});};
 return <View role="alert" style={{gap:10,padding:12,borderRadius:10,backgroundColor:C.amberBg,borderWidth:1,borderColor:'#4A3E1E'}}>
  <View style={[s.row,{alignItems:'flex-start',gap:8}]}><Icon name="alert-triangle" size={16} color={C.amber}/><View style={{flex:1,gap:2}}><T style={{fontFamily:'InterSemi',fontSize:13,color:C.amber}}>{title}</T><T style={{fontSize:12,lineHeight:18}}>{message}</T></View></View>
  {(!!onRetry||!!link)&&<View style={[s.row,{flexWrap:'wrap',gap:12}]}>
   {!!onRetry&&<Button label="Retry" icon="refresh-cw" kind="outline" onPress={onRetry} style={{minHeight:36,paddingVertical:6}}/>}
   {!!link&&<Pressable accessibilityRole="link" accessibilityLabel={link.label||'Service status'} {...webOnly(webHref?{href:link.href,hrefAttrs:{target:'_blank',rel:'noopener noreferrer'}}:{})} onPress={webHref?undefined:open} style={{minHeight:36,justifyContent:'center'}}><T style={{fontSize:12,color:C.green,textDecorationLine:'underline'}}>{link.label||'Service status'}</T></Pressable>}
  </View>}
 </View>;
}
export type WidgetProps={title:string;subtitle?:string;onMaximize?:()=>void;maximized?:boolean;onClose?:()=>void;menu?:WidgetMenuItem[];children?:React.ReactNode;errorState?:WidgetErrorState|null;emptyText?:string;style?:any;bodyStyle?:any};
// Widget card: header (h3 title, subtitle, ⋮ options menu in a Popover, ⛶ maximize/restore, ✕ close), body = errorState renderer, children, or an explicit empty line.
export function Widget({title,subtitle,onMaximize,maximized=false,onClose,menu,children,errorState,emptyText='Nothing to show yet.',style,bodyStyle}:WidgetProps){
 const [menuOpen,setMenuOpen]=React.useState(false);const menuRef=React.useRef<any>(null);const headId=React.useId();const hasMenu=!!menu?.length;
 const close=React.useCallback(()=>setMenuOpen(false),[]);
 return <View role="region" aria-labelledby={headId} style={[{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:12,overflow:'hidden'},style]}>
  <View style={[s.row,{paddingLeft:12,paddingRight:2,minHeight:42,borderBottomWidth:1,borderColor:C.line,gap:4}]}>
   <View style={{flex:1,minWidth:0}}><T nativeID={headId} role="heading" aria-level={3} numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:11,lineHeight:16,letterSpacing:1,textTransform:'uppercase'}}>{title}</T>{!!subtitle&&<T numberOfLines={1} style={{fontSize:11,lineHeight:15,color:C.muted}}>{subtitle}</T>}</View>
   {hasMenu&&<IconButton ref={menuRef} icon="more-vertical" label={`${title} options`} tooltip="Options" haspopup="menu" expanded={menuOpen} onPress={()=>setMenuOpen(o=>!o)}/>}
   {!!onMaximize&&<IconButton icon={maximized?'minimize-2':'maximize-2'} label={`${maximized?'Restore':'Maximize'} ${title}`} tooltip={maximized?'Restore':'Maximize'} pressed={maximized} onPress={onMaximize}/>}
   {!!onClose&&<IconButton icon="x" label={`Close ${title}`} tooltip="Close" onPress={onClose}/>}
  </View>
  <View style={[{padding:12,gap:10},bodyStyle]}>{errorState?<WidgetError {...errorState}/>:children==null||children===false?<T style={{fontSize:12,color:C.muted}}>{emptyText}</T>:children}</View>
  {hasMenu&&<Popover open={menuOpen} onClose={close} anchor={menuRef} placement="bottom-end" label={`${title} options`} role="none" width={220}><MenuList items={menu!} onClose={close} label={`${title} options`}/></Popover>}
 </View>;
}
