import React from 'react';
import {View,Pressable,Platform} from 'react-native';
import {useRouter} from 'expo-router';
import {C,T,Icon} from '../src/ui';
import {FalconMark} from '../src/falcon/FalconMark';
// `/` = Falcon placeholder (docs/FALCON_DISCOVER_SPEC.md §2 is deferred). The full scan UI lives unmounted in src/falcon (FalconHome) for later.
const web=Platform.OS==='web';
export default function FalconPlaceholder(){
 const router=useRouter();
 return <View style={{flex:1,backgroundColor:C.bg,alignItems:'center',justifyContent:'center',padding:24,gap:20}}>
  <FalconMark size={104}/>
  <View style={{alignItems:'center',gap:8,maxWidth:560}}>
   <T role="heading" aria-level={1} style={{fontFamily:'ManropeBold',fontSize:30,lineHeight:38,letterSpacing:-.6}}>Falcon</T>
   <T style={{fontSize:16,lineHeight:25,color:C.muted,textAlign:'center'}}>Falcon reads every strategy across NIFTY 500 and tells you what matters today.</T>
  </View>
  <View style={{alignItems:'center',gap:6}}>
   <Pressable disabled accessibilityRole="button" accessibilityLabel="Scan the market in 30 sec, coming soon" accessibilityState={{disabled:true}} style={{minHeight:56,paddingHorizontal:30,borderRadius:16,flexDirection:'row',alignItems:'center',gap:10,borderWidth:1,borderColor:C.line,backgroundColor:C.paper,opacity:.6}}>
    <Icon name="zap" size={18} color={C.muted}/><T style={{fontFamily:'ManropeBold',fontSize:17,color:C.muted}}>Scan the market in 30 sec</T>
   </Pressable>
   <T style={{fontSize:12,color:C.muted}}>Coming soon</T>
  </View>
  <Pressable accessibilityRole="link" accessibilityLabel="Open Discover Strategies" onPress={()=>router.push('/discover' as any)}
   style={(st:any)=>[{minHeight:52,paddingHorizontal:26,borderRadius:14,flexDirection:'row',alignItems:'center',gap:10,backgroundColor:st.pressed?'#2BC48A':st.hovered?'#5AF0B6':C.green},web&&st.focused?({outlineWidth:3,outlineStyle:'solid',outlineColor:C.mint,outlineOffset:3} as any):null,web?({cursor:'pointer'} as any):null]}>
   <T style={{fontFamily:'InterSemi',fontSize:15,color:'#041B12'}}>Open Discover Strategies</T><Icon name="arrow-right" size={17} color="#041B12"/>
  </Pressable>
 </View>;
}
