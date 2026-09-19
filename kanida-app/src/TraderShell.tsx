import React from 'react';
import Svg,{Path,Circle} from 'react-native-svg';
import {C} from './ui';
// The unmounted TraderShell layout component was archived to _archive/src/TraderShell.dead.tsx. Only AgentMark is live.
export function AgentMark({size=36,color=C.green}:any){return <Svg width={size} height={size} viewBox="0 0 40 40"><Circle cx="20" cy="20" r="18" fill="none" stroke={color} strokeOpacity=".35" strokeWidth=".7"/><Path d="M20 6L23 17L34 20L23 23L20 34L17 23L6 20L17 17Z" fill={color}/><Circle cx="20" cy="20" r="3" fill={C.bg}/><Path d="M10 10L15 14M27 27L30 30M30 10L27 13M10 30L13 27" stroke={color} strokeWidth=".8"/></Svg>;}
