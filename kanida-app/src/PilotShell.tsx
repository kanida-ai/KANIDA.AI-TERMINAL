import React from 'react';
import {View,Pressable,ScrollView,useWindowDimensions} from 'react-native';
import {Slot,router,usePathname,useGlobalSearchParams,Redirect} from 'expo-router';
import {StatusBar} from 'expo-status-bar';
import {useSafeAreaInsets} from 'react-native-safe-area-context';
import {C,T,s,Icon,Button,Loading,useLayoutMode} from './ui';
import {useProduct} from './context';
import {useAuth} from './Auth';
import {ProductSheets} from './Sheets';
import {AgentMark} from './TraderShell';
import {MainWorkspace} from './MainWorkspace';
import {TopBar,DataAgePill,DataStatusPopover} from './layout/index';
import {isWorkspacePath,FULL_HEIGHT_PATHS,CHART_PATH} from './shell/routes';
import {MainNav,NAV_FULL_WIDTH} from './shell/MainNav';
import {SymbolSearch} from './shell/SymbolSearch';
import {ShellMark,AccountButton,ResearchBanner} from './shell/parts';
const publicPaths=['/welcome','/signin','/signup','/connect','/terms','/privacy','/device','/recover'];
const first=(v:unknown)=>typeof v==='string'?v:Array.isArray(v)&&typeof v[0]==='string'?v[0]:'';
export function PilotShell(){
 const mode=useLayoutMode(),phone=mode==='phone',{width}=useWindowDimensions(),path=usePathname(),params=useGlobalSearchParams(),inset=useSafeAreaInsets(),auth=useAuth();
 const {toast,state,loading,error}=useProduct();const publicPage=publicPaths.includes(path);
 // Data status popover, anchored on the top-bar data pill. Declared with the other hooks: the
 // auth/redirect guards below return early.
 const [dataOpen,setDataOpen]=React.useState(false);const dataRef=React.useRef<any>(null);
 // Legacy chart links (/?s=…&tf=…&m=…) → /chart with the same params (§1). Falcon lives on "/" without params.
 const legacy=path==='/'&&!!first(params.s);
 if(legacy){const q:Record<string,string>={};for(const k of ['s','tf','m','tab']){const v=first((params as any)[k]);if(v)q[k]=v}return <Redirect href={{pathname:CHART_PATH,params:q} as any}/>}
 if(!auth.ready)return <Loading/>;
 if(auth.error)return <View style={{flex:1,backgroundColor:C.bg,justifyContent:'center',padding:28,gap:18}}><T>{auth.error}</T><Button label="Reconnect to KANIDA" onPress={auth.refresh}/></View>;
 if(!publicPage&&!auth.user)return <Redirect href="/welcome"/>;
 if(auth.user&&!auth.user.onboarded&&!publicPage&&path!=='/onboarding')return <Redirect href="/onboarding"/>;
 if(auth.user?.onboarded&&!auth.billing?.access&&!publicPage&&!['/account','/billing','/onboarding'].includes(path))return <Redirect href="/billing"/>;
 const nav=!!auth.user?.onboarded&&!publicPage;
 const toastView=!!toast&&<View pointerEvents="none" style={{position:'absolute',left:20,right:20,bottom:phone&&nav?90:20,alignItems:'center',zIndex:40}}><View style={{backgroundColor:'#163E32',borderWidth:1,borderColor:'#2D7158',borderRadius:12,padding:16}}><T style={{color:C.mint,fontSize:13}}>{toast}</T></View></View>;
 // Workspace routes (/chart, /simulate, /watch, /autotrade, /activity) after the gates above: one persistent region shell (TopBar with MainNav + chart + sidebar + dock). Route components render nothing; the Slot stays mounted (hidden) so expo-router keeps routing.
 if(nav&&isWorkspacePath(path))return <View style={{flex:1,backgroundColor:C.bg}}><StatusBar style="light"/><View aria-hidden style={{display:'none'}}><Slot/></View><MainWorkspace/>{toastView}<ProductSheets/></View>;
 // Signed-out / onboarding / public pages: the plain header (logo, Sign in or Private pilot, Open on iPhone).
 const plainHeader=<View style={{backgroundColor:C.bg,borderBottomWidth:1,borderColor:C.line,paddingTop:inset.top}}><View style={[s.between,{width:'100%',maxWidth:1500,alignSelf:'center',paddingHorizontal:phone?18:30,height:phone?56:64}]}><Pressable accessibilityRole="link" accessibilityLabel="KANIDA home" onPress={()=>router.push(auth.user?.onboarded?'/':'/welcome')} style={[s.row,{gap:10}]}><AgentMark size={36}/><T style={{fontFamily:'InterMedium',fontSize:20,letterSpacing:2}}>KANIDA<T style={{fontSize:12,color:C.muted}}>.AI</T></T></Pressable><View style={s.row}>{!auth.user?<Button label="Sign in" kind="outline" onPress={()=>router.push('/signin')}/>:<T style={{fontSize:12,color:C.muted}}>Private pilot</T>}{!!auth.user&&<Pressable accessibilityRole="button" accessibilityLabel={path==='/account'?'Account, current page':'Account'} onPress={()=>router.push('/account')} style={{padding:12}}><Icon name="user" color={path==='/account'?C.green:C.muted} size={19}/></Pressable>}<Pressable accessibilityRole="button" accessibilityLabel="Open on iPhone" onPress={()=>router.push('/connect')} style={{padding:12}}><Icon name="smartphone" color={C.muted} size={18}/></Pressable></View></View></View>;
 // Member pages (Falcon "/", Discover Strategies, Account, admin…): the same TopBar + MainNav as the workspace. Phone: nav moves to a bottom tab bar, search stays in the top bar.
 // The pill opens the Data status popover (source, newest bar, last/next refresh, session).
 // `cache` is the state object itself: when the pilot replayed the last scan it carries its own provenance,
 // so the pill says "Last scan 15:30 · reconnecting" instead of "Data age unknown" (BACKLOG item 2a).
 const pill=loading?null:<DataAgePill dataEnd={state?.source_latest} status={state?.data_status} cache={state} error={state?'':error} compact={width<NAV_FULL_WIDTH} expanded={dataOpen} onPress={()=>setDataOpen(o=>!o)}/>;
 const header=nav?<TopBar label="Top bar" style={{paddingTop:inset.top+6}} left={<><ShellMark compact={mode!=='desktop'} markOnly={mode==='tablet'}/>{!phone&&<MainNav/>}</>} center={<SymbolSearch compact={phone}/>} right={<><View ref={dataRef}>{pill}</View><AccountButton connect/></>}/>:plainHeader;
 return <View style={{flex:1,backgroundColor:C.bg}}><StatusBar style="light"/>{header}
 <DataStatusPopover open={dataOpen} onClose={()=>setDataOpen(false)} anchor={dataRef} status={state?.data_status} dataEnd={state?.source_latest} cache={state} error={state?'':error} routeKey={path}/>
 {!publicPage&&<ResearchBanner/>}
 {FULL_HEIGHT_PATHS.includes(path)?<View style={{flex:1,minHeight:0,overflow:'hidden'}}><Slot/></View>:<ScrollView key={path} keyboardShouldPersistTaps="handled" style={{flex:1,minHeight:0}} contentContainerStyle={{flexGrow:1}}><Slot/></ScrollView>}
 {phone&&nav&&<MainNav variant="tabs"/>}
 {toastView}{auth.user&&<ProductSheets/>}</View>
}
