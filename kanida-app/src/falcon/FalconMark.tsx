import React from 'react';
import Svg,{Circle,Path,G} from 'react-native-svg';
import {C} from '../ui';
// Original KANIDA Falcon glyph: a faceted falcon head in profile (hooked beak, malar stripe) with a circuit trace running from the eye to the nape. Decorative: the page text names Falcon.
export function FalconMark({size=96,color=C.green}:{size?:number;color?:string}){
 return <Svg width={size} height={size} viewBox="0 0 64 64" accessibilityElementsHidden importantForAccessibility="no-hide-descendants" {...({'aria-hidden':true} as any)}>
  <Circle cx={32} cy={32} r={30} stroke={C.line} strokeWidth={1.5} fill={C.paper}/>
  <Circle cx={32} cy={32} r={26.5} stroke={color} strokeOpacity={.25} strokeWidth={1} strokeDasharray="2 3" fill="none"/>
  <G strokeLinejoin="round" strokeLinecap="round">
   <Path d="M14 44 L20 22 L32 14 L45 15 L53 22 L55 30 L49 27 L45 29 L43 36 L36 50 L24 51 Z" fill={C.soft} stroke={color} strokeWidth={2}/>
   <Path d="M45 15 L53 22 L55 30 L49 27 Z" fill={color} fillOpacity={.9} stroke={color} strokeWidth={1.2}/>
   <Path d="M39 23 L37 36" stroke={color} strokeWidth={2.4}/>
   <Path d="M38 21 L28 27 L22 36 L20 44" stroke={color} strokeOpacity={.7} strokeWidth={1.2} fill="none"/>
   <Circle cx={28} cy={27} r={1.6} fill={color}/><Circle cx={22} cy={36} r={1.6} fill={color}/><Circle cx={20} cy={44} r={1.6} fill={color}/>
   <Circle cx={40} cy={21} r={2.6} fill={C.bg} stroke={color} strokeWidth={1.5}/><Circle cx={40.5} cy={20.6} r={1} fill={C.mint}/>
  </G>
 </Svg>;
}
