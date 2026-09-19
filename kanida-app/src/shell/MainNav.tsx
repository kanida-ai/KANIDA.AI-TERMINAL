import React from 'react';
import {View,Pressable,useWindowDimensions} from 'react-native';
import {router,usePathname} from 'expo-router';
import {useSafeAreaInsets} from 'react-native-safe-area-context';
import {C,T,Icon} from '../ui';
/** Below this width the top-bar links drop their icons and use short labels (the search field keeps room). */
export const NAV_FULL_WIDTH=1280;
import {webOnly} from '../layout/shared';
import {NAV_ITEMS,navActive,type NavItem} from './routes';
// Main navigation (FALCON_DISCOVER_SPEC §1): Falcon · Discover Strategies · Watchlist · AutoTrade. ONE component for both shells (PilotShell header pages and MainWorkspace's TopBar).
// variant 'bar' = inline links in the top bar (desktop: icon + label; tablet 760–1049: short labels); 'tabs' = the phone (<760) bottom tab bar. Links are focusable (Tab), Enter/Space activate, focus ring = green border; aria-current marks the page.
function go(route:string,current:boolean){if(current)return;try{router.push(route as any)}catch{}}
function NavLink({item,on,tabs,compact}:{item:NavItem;on:boolean;tabs:boolean;compact:boolean}){
 const label=tabs||compact?item.short:item.label;
 return <Pressable accessibilityRole="link" accessibilityLabel={on?`${item.label}, current page`:item.label} accessibilityState={{selected:on}} {...webOnly({'aria-current':on?'page':undefined})} onPress={()=>go(item.route,on)}
  style={(st:any)=>tabs?[{flex:1,minHeight:52,alignItems:'center',justifyContent:'center',gap:4,paddingHorizontal:2,borderRadius:10,borderWidth:2,borderColor:st.focused?C.green:'transparent',opacity:st.pressed?.65:1}]
   :[{flexDirection:'row',alignItems:'center',gap:7,minHeight:38,paddingHorizontal:compact?9:12,borderRadius:9,borderWidth:1,borderColor:st.focused?C.green:on?C.line:'transparent',backgroundColor:on?C.paper:st.hovered?C.soft:'transparent',opacity:st.pressed?.7:1}]}>
  {(tabs||!compact)&&<Icon name={item.icon} size={tabs?20:15} color={on?C.green:C.muted}/>}
  <T numberOfLines={1} style={{fontSize:tabs?11:13,lineHeight:tabs?15:18,fontFamily:'InterSemi',color:on?C.green:tabs?C.muted:C.ink}}>{label}</T>
  {!tabs&&on&&<View style={{position:'absolute',left:10,right:10,bottom:-7,height:2,borderRadius:1,backgroundColor:C.green}}/>}
 </Pressable>;
}
export function MainNav({variant='bar'}:{variant?:'bar'|'tabs'}){
 const path=usePathname(),{width}=useWindowDimensions(),inset=useSafeAreaInsets(),current=navActive(path),tabs=variant==='tabs';
 const links=NAV_ITEMS.map(it=><NavLink key={it.route} item={it} on={current===it.route} tabs={tabs} compact={width<NAV_FULL_WIDTH}/>);
 if(tabs)return <View role="navigation" aria-label="Main navigation" style={{flexDirection:'row',gap:4,backgroundColor:C.bg,borderTopWidth:1,borderColor:C.line,paddingHorizontal:6,paddingTop:6,paddingBottom:Math.max(inset.bottom,8)}}>{links}</View>;
 return <View role="navigation" aria-label="Main navigation" style={{flexDirection:'row',alignItems:'center',gap:2}}>{links}</View>;
}
