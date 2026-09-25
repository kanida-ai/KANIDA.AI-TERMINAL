import React from 'react';
import {View,Text,Pressable,StyleSheet,ActivityIndicator,Modal,ScrollView,Platform,useWindowDimensions} from 'react-native';
import Feather from '@expo/vector-icons/Feather';
import {useSafeAreaInsets} from 'react-native-safe-area-context';
export const C={bg:'#050C11',paper:'#0B151C',ink:'#E9F1F5',muted:'#90A2AD',line:'#1B2B35',green:'#39E5A3',mint:'#B7F5DD',soft:'#0D2B25',amber:'#EBC66B',amberBg:'#282315',red:'#F17D87',dark:'#071219'};
export const s=StyleSheet.create({row:{flexDirection:'row',alignItems:'center',gap:10},between:{flexDirection:'row',alignItems:'center',justifyContent:'space-between',gap:12},card:{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:14,padding:20,gap:16},line:{height:1,backgroundColor:C.line},muted:{color:C.muted},label:{fontFamily:'InterMedium',fontSize:11,letterSpacing:1.5,textTransform:'uppercase',color:C.muted},title:{fontFamily:'ManropeBold',fontSize:30,lineHeight:39,letterSpacing:-1,color:C.ink},input:{fontFamily:'Inter',color:C.ink,fontSize:15,minHeight:48,paddingHorizontal:14,borderWidth:1,borderColor:C.line,borderRadius:12,backgroundColor:C.paper}});
// Shared layout breakpoints (px, compared with window width). desktop: split panes + header tabs; tablet: roomier single column; sheet: below this, sheets rise from the bottom.
export const BREAKPOINTS={desktop:1050,tablet:760,sheet:700} as const;
export type LayoutMode='phone'|'tablet'|'desktop';
export function layoutMode(width:number):LayoutMode{return width>=BREAKPOINTS.desktop?'desktop':width>=BREAKPOINTS.tablet?'tablet':'phone';}
export function useLayoutMode():LayoutMode{const {width}=useWindowDimensions();return layoutMode(width);}
export function T({children,style,...props}:any){return <Text {...props} style={[{fontFamily:'Inter',fontSize:14,lineHeight:21,color:C.ink},style]}>{children}</Text>;}
export function Icon({name,size=19,color=C.ink}:any){return <Feather name={name} size={size} color={color}/>;}
// Off-screen text that stays in the DOM, so aria-describedby can reference it.
const hiddenText={position:'absolute' as const,width:1,height:1,overflow:'hidden' as const,opacity:0};
// Optional accessibilityState/accessibilityHint serve toggles such as Show/Hide. RN-web ignores both, so web also gets aria-pressed (selected), aria-expanded (expanded) and an aria-describedby hint.
export function Button({label,onPress,kind='primary',icon,disabled=false,loading=false,style,accessibilityLabel,accessibilityState,accessibilityHint}:any){
 const filled=kind==='primary',hintId=React.useId(),web=Platform.OS==='web';
 const aria:any={};
 if(web&&accessibilityState?.selected!=null)aria['aria-pressed']=!!accessibilityState.selected;
 if(web&&accessibilityState?.expanded!=null)aria['aria-expanded']=!!accessibilityState.expanded;
 if(web&&accessibilityHint)aria['aria-describedby']=hintId;
 return <Pressable accessibilityRole="button" accessibilityLabel={accessibilityLabel||label} accessibilityState={accessibilityState} accessibilityHint={accessibilityHint} {...aria} disabled={disabled||loading} onPress={onPress} style={({pressed})=>[{minHeight:46,paddingHorizontal:17,paddingVertical:11,borderRadius:12,backgroundColor:filled?C.green:kind==='soft'?C.soft:'transparent',borderWidth:kind==='outline'?1:0,borderColor:C.line,flexDirection:'row',alignItems:'center',justifyContent:'center',gap:9,opacity:disabled?.42:pressed?.72:1},style]}>
  {loading?<ActivityIndicator color={filled?'#041B12':C.green}/>:icon?<Icon name={icon} size={17} color={filled?'#041B12':C.green}/>:null}
  <T style={{fontFamily:'InterSemi',fontSize:13,color:filled?'#041B12':C.ink}}>{label}</T>
  {web&&!!accessibilityHint&&<T nativeID={hintId} style={hiddenText}>{accessibilityHint}</T>}
 </Pressable>;
}
// Toggle chip: accessibilityState covers native; RN-web ignores it, so aria-pressed exposes the on/off state on web (valid for role=button, unlike aria-selected).
export function Chip({label,active,onPress,icon}:any){const on=!!active;return <Pressable accessibilityRole="button" accessibilityState={{selected:on}} {...({'aria-pressed':on} as any)} accessibilityLabel={label} onPress={onPress} style={({pressed})=>[s.row,{minHeight:44,borderRadius:11,paddingHorizontal:14,paddingVertical:9,borderWidth:1,borderColor:active?C.green:C.line,backgroundColor:active?C.soft:C.paper,opacity:pressed?.7:1,gap:7}]}>{icon&&<Icon name={icon} size={15} color={active?C.green:C.muted}/>}<T style={{fontSize:12,fontFamily:'InterMedium',color:active?C.green:C.muted}}>{label}</T></Pressable>;}
// Accessible checkbox: role + checked state for native and web (RN-web ignores accessibilityState, so aria-checked is set too). Box drawn with a View + Feather check, no text glyphs.
export function Checkbox({checked,onChange,label,detail,children,disabled=false,tone=C.green,style}:{checked:boolean;onChange:(next:boolean)=>void;label:string;detail?:string;children?:React.ReactNode;disabled?:boolean;tone?:string;style?:any}){return <Pressable role="checkbox" aria-checked={checked} aria-disabled={disabled} accessibilityState={{checked,disabled}} accessibilityLabel={label} disabled={disabled} onPress={()=>onChange(!checked)} style={({pressed})=>[s.row,{minHeight:44,paddingVertical:8,alignItems:'flex-start',opacity:disabled?.42:pressed?.72:1},style]}><View style={{width:20,height:20,marginTop:1,borderRadius:5,borderWidth:1.5,borderColor:checked?tone:C.muted,backgroundColor:checked?tone:'transparent',alignItems:'center',justifyContent:'center'}}>{checked&&<Icon name="check" size={14} color="#041B12"/>}</View><View style={{flex:1,gap:2}}>{children??<T style={{fontSize:12,lineHeight:20,color:checked?tone:C.ink}}>{label}</T>}{!!detail&&<T style={{fontSize:11,color:C.muted}}>{detail}</T>}</View></Pressable>;}
export function Badge({label,tone='green',dot=false}:any){const col=tone==='amber'?C.amber:tone==='red'?C.red:tone==='neutral'?C.muted:C.green;return <View style={[s.row,{alignSelf:'flex-start',gap:5,backgroundColor:tone==='amber'?C.amberBg:tone==='neutral'?C.bg:C.soft,borderRadius:6,paddingHorizontal:8,paddingVertical:4}]}>{dot&&<View style={{width:5,height:5,borderRadius:3,backgroundColor:col}}/>}<T style={{fontSize:10,lineHeight:15,fontFamily:'InterSemi',color:col}}>{label}</T></View>;}
export function Stat({label,value,note,color,size=24}:any){return <View style={{gap:4,flex:1,minWidth:95}}><T style={{fontSize:11,color:C.muted}}>{label}</T><T style={{fontFamily:'ManropeBold',fontSize:size,lineHeight:size+7,letterSpacing:-.5,color:color||C.ink}}>{value}</T>{note&&<T style={{fontSize:10,color:C.muted,lineHeight:16}}>{note}</T>}</View>;}
export function Empty({icon='search',title,detail,action}:any){return <View style={[s.card,{alignItems:'center',paddingVertical:40}]}><View style={{padding:18,backgroundColor:C.soft,borderRadius:22}}><Icon name={icon} size={25} color={C.green}/></View><T style={{fontFamily:'ManropeBold',fontSize:20,textAlign:'center'}}>{title}</T><T style={{color:C.muted,textAlign:'center',maxWidth:420}}>{detail}</T>{action}</View>;}
export function Loading(){return <View style={{padding:60,alignItems:'center',gap:15}}><ActivityIndicator color={C.green}/><T style={s.muted}>Reading your market…</T></View>;}
// Escape already closes Sheet on web: RN-web 0.21 Modal listens for keyup Escape on the top-most modal and calls onRequestClose (Android back uses the same prop).
export function Sheet({visible,onClose,title,subtitle,children,footer,wide=false}:any){
 const {width,height}=useWindowDimensions();const inset=useSafeAreaInsets();
 // focus returns to the control that opened the panel when it closes (keyboard and screen-reader users keep their place)
 const opener=React.useRef<any>(null);
 React.useEffect(()=>{if(Platform.OS!=='web'||typeof document==='undefined')return;
  if(visible){opener.current=document.activeElement;return;}
  const el=opener.current;opener.current=null;if(el&&typeof el.focus==='function'&&document.contains(el))setTimeout(()=>{try{el.focus();}catch{}},0);},[visible]);
 const narrow=width<BREAKPOINTS.sheet;const name=typeof title==='string'?title:'Panel';
 return <Modal visible={visible} onRequestClose={onClose} transparent animationType="fade">
  <View style={{flex:1,backgroundColor:'#00070DD9',alignItems:narrow?'stretch':'flex-end',justifyContent:narrow?'flex-end':'center'}}>
   {/* Backdrop is mouse/touch-only; the labelled Close button and Escape/back cover assistive tech, so it stays out of the a11y tree and never shadows the panel. */}
   <Pressable aria-hidden accessible={false} importantForAccessibility="no-hide-descendants" focusable={false} onPress={onClose} style={StyleSheet.absoluteFill}/>
   <View role="dialog" aria-modal aria-label={name} accessibilityViewIsModal importantForAccessibility="yes" style={{backgroundColor:C.bg,width:narrow?'100%':wide?Math.min(width-80,900):600,height:narrow?height-inset.top-12:'100%',borderTopLeftRadius:24,borderTopRightRadius:narrow?24:0,paddingTop:narrow?8:inset.top,minHeight:200}}>
    <View style={[s.between,{paddingHorizontal:24,paddingVertical:18,borderBottomWidth:1,borderColor:C.line}]}>
     <View style={{flex:1,gap:4}}>
      <T role="heading" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:23,lineHeight:31}}>{title}</T>
      {subtitle&&<T style={{fontSize:12,color:C.muted}}>{subtitle}</T>}
     </View>
     <Button label="Close" accessibilityLabel={`Close ${name}`} icon="x" kind="outline" onPress={onClose}/>
    </View>
    <ScrollView key={title+subtitle} automaticallyAdjustKeyboardInsets keyboardDismissMode="on-drag" keyboardShouldPersistTaps="handled" contentContainerStyle={{padding:narrow?18:26,gap:20,paddingBottom:30}}>{children}</ScrollView>
    {footer&&<View style={{padding:18,paddingBottom:Math.max(inset.bottom,18),backgroundColor:C.paper,borderTopWidth:1,borderColor:C.line}}>{footer}</View>}
   </View>
  </View>
 </Modal>;
}
