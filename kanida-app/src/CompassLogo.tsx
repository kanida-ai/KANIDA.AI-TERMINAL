// KANIDA.AI brand mark - the compass from the Power User portal (components/power/CompassLogo.tsx), rebuilt for React
// Native: a fixed ring with N/E/S/W ticks and a needle that swings gently (8 s), still when the OS asks for reduced motion.
import React,{useEffect,useRef,useState} from 'react';
import {AccessibilityInfo,Animated,Easing,View} from 'react-native';
import Svg,{Circle,G,Line,Polygon} from 'react-native-svg';

const GREEN='#3FE3A4';

export function CompassLogo({size=24}:{size?:number}){
 const turn=useRef(new Animated.Value(0)).current;const [still,setStill]=useState(false);
 useEffect(()=>{let live=true;AccessibilityInfo.isReduceMotionEnabled?.().then(v=>{if(live)setStill(!!v);}).catch(()=>{});return()=>{live=false;};},[]);
 useEffect(()=>{if(still)return;
  const loop=Animated.loop(Animated.sequence([Animated.timing(turn,{toValue:1,duration:4000,easing:Easing.inOut(Easing.ease),useNativeDriver:true}),
   Animated.timing(turn,{toValue:0,duration:4000,easing:Easing.inOut(Easing.ease),useNativeDriver:true})]));
  loop.start();return()=>loop.stop();},[still,turn]);
 const rotate=turn.interpolate({inputRange:[0,1],outputRange:['0deg','24deg']});
 return <View style={{width:size,height:size}} accessibilityElementsHidden importantForAccessibility="no-hide-descendants">
  <Svg viewBox="0 0 32 32" width={size} height={size} style={{position:'absolute'}}>
   <Circle cx="16" cy="16" r="14" fill="none" stroke="rgba(63,227,164,0.85)" strokeWidth="1.5"/>
   <G stroke="rgba(63,227,164,0.55)" strokeWidth="1"><Line x1="16" y1="2" x2="16" y2="4.5"/><Line x1="16" y1="27.5" x2="16" y2="30"/><Line x1="2" y1="16" x2="4.5" y2="16"/><Line x1="27.5" y1="16" x2="30" y2="16"/></G>
  </Svg>
  <Animated.View style={{position:'absolute',width:size,height:size,transform:[{rotate}]}}>
   <Svg viewBox="0 0 32 32" width={size} height={size}>
    <Polygon points="16,4 13.5,16 18.5,16" fill={GREEN}/><Polygon points="16,28 13.5,16 18.5,16" fill="rgba(63,227,164,0.35)"/>
    <Circle cx="16" cy="16" r="1.5" fill="#0a0a0a" stroke={GREEN} strokeWidth="0.8"/>
   </Svg>
  </Animated.View>
 </View>;
}
