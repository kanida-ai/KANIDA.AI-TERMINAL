import React,{useMemo} from 'react';
import {View,Linking} from 'react-native';
import Svg,{Path,Rect} from 'react-native-svg';
import qrcode from 'qrcode-generator';
import {C,T,s,Button,Badge,useLayoutMode} from './ui';
import {Rule} from './Sheets';

// Dead screens (Discover, Portfolios, AutoTrade, Agent, SetupCard, BriefCard, Orb, Avatar) were archived to
// _archive/src/Screens.dead.tsx. Only the /connect route remains here.
// Shared layout modes (3.3): phone = <760 (was <700); desktop = >=1050 (was >=1100).
export function Page({children}:any){const mode=useLayoutMode(),phone=mode==='phone';return <View style={{width:'100%',maxWidth:1390,alignSelf:'center',padding:phone?20:mode==='desktop'?40:28,gap:phone?18:26,paddingBottom:44}}>{children}</View>;}
function Heading({eyebrow,title,detail,action}:any){const phone=useLayoutMode()==='phone';return <View style={[s.between,{alignItems:'flex-start'}]}><View style={{gap:9,flex:1}}>{!phone&&<T style={s.label}>{eyebrow}</T>}<T accessibilityRole="header" style={[s.title,{fontSize:phone?30:38,lineHeight:phone?39:48}]}>{title}</T>{detail&&<T style={{color:C.muted,fontSize:13,maxWidth:620}}>{detail}</T>}</View>{action}</View>;}

export function Connect(){
 const url=process.env.EXPO_PUBLIC_EXPO_URL||'';const code=useMemo(()=>{const qr=qrcode(0,'M');qr.addData(url);qr.make();return qr},[url]);const n=code.getModuleCount();let d='';for(let r=0;r<n;r++)for(let c=0;c<n;c++)if(code.isDark(r,c))d+=`M${c+4} ${r+4}h1v1h-1z`;
 return <Page><Heading eyebrow="KANIDA, WITH YOU" title="Meet your iPhone app." detail="Open the same KANIDA experience in Expo Go, with native navigation and touch controls."/><View style={[s.card,{alignItems:'center',padding:30}]}><Badge label="Expo SDK 57 · local preview"/><View style={{padding:12,backgroundColor:'white',borderRadius:15}}><Svg accessibilityLabel="Expo Go connection QR code" width={250} height={250} viewBox={`0 0 ${n+8} ${n+8}`}><Rect x="0" y="0" width={n+8} height={n+8} fill="white"/><Path d={d} fill={C.dark}/></Svg></View><T style={{fontFamily:'ManropeBold',fontSize:22,textAlign:'center'}}>Scan. Open. Explore.</T><View style={{maxWidth:470,gap:18}}><Rule n="1" title="Connect to the same Wi-Fi" text="Keep this computer and your iPhone on the same local network."/><Rule n="2" title="Install or update Expo Go" text="Use the current Expo Go app from the iPhone App Store."/><Rule n="3" title="Scan with your iPhone camera" text="Point the camera at this QR code, then choose Open in Expo Go. Keep the local servers running."/></View><Button label="Open in Expo Go" icon="smartphone" onPress={()=>Linking.openURL(url)}/><T selectable style={{fontSize:12,color:C.muted}}>{url}</T><T style={{fontSize:12,color:C.muted,textAlign:'center',maxWidth:520}}>Safari preview: {process.env.EXPO_PUBLIC_API_URL}. On a different Wi-Fi network, restart the preview script to refresh the local address.</T></View></Page>;
}
