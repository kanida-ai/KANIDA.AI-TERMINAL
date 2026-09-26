import React,{useCallback,useRef,useState} from 'react';
import {View,Pressable} from 'react-native';
import {router,usePathname} from 'expo-router';
import {useProduct} from '../context';
import {useAuth} from '../Auth';
import {C,T,s,Button} from '../ui';
import {api,Match} from '../model';
import {AgentMark} from '../TraderShell';
import {IconButton,Popover,MenuList,connectionView,type MenuItem} from '../layout/index';
import {CompassLogo} from '../CompassLogo';
// Small top-bar parts shared by both shells (PilotShell header pages and MainWorkspace). Existing controls only: home mark (→ Falcon "/"), watch toggle (existing /api/product/watch), Open on iPhone, the account menu, and the research-unavailable banner.
/** markOnly: just the mark (tablet top bars, where the nav needs the room); the accessible name stays. */
export function ShellMark({compact=false,markOnly=false}:{compact?:boolean;markOnly?:boolean}){return <Pressable accessibilityRole="link" accessibilityLabel="KANIDA home, Falcon" onPress={()=>router.push('/')} style={(st:any)=>[s.row,{gap:8,borderRadius:8,borderWidth:1,borderColor:st.focused?C.green:'transparent',paddingRight:markOnly?0:4}]}><CompassLogo size={compact?26:30}/>{!markOnly&&<T style={{fontFamily:'InterMedium',fontSize:compact?16:18,letterSpacing:2}}>KANIDA{!compact&&<T style={{fontSize:11,color:C.muted}}>.AI</T>}</T>}</Pressable>;}
/** Account menu: Account and billing · Manage strategies (owner only, §6) · Open on iPhone (when the top bar has no separate button). */
export function AccountButton({connect=false}:{connect?:boolean}){
 const auth=useAuth(),path=usePathname(),[open,setOpen]=useState(false),ref=useRef<any>(null),close=useCallback(()=>setOpen(false),[]);
 const items:MenuItem[]=[{label:'Account and billing',icon:'user',onPress:()=>router.push('/account')}];
 if(auth.user?.role==='owner')items.push({label:'Manage strategies',icon:'sliders',onPress:()=>router.push('/admin/strategies' as any)});
 if(connect)items.push({label:'Open on iPhone',icon:'smartphone',onPress:()=>router.push('/connect')});
 const here=path==='/account'||path.startsWith('/admin');
 return <><IconButton ref={ref} icon="user" label="Account menu" tooltip="Account" haspopup="menu" expanded={open} pressed={here} onPress={()=>setOpen(o=>!o)} size={18}/>
  <Popover open={open} onClose={close} anchor={ref} placement="bottom-end" label="Account menu" width={220} routeKey={path} role="none"><MenuList items={items} onClose={close} label="Account menu"/></Popover></>;
}
export function ConnectButton(){return <IconButton icon="smartphone" label="Open on iPhone" tooltip="Open on iPhone" onPress={()=>router.push('/connect')} size={17}/>;}
/** Watch / Watching for the stored setup the chart shows (same API call and toast as the chart header's Watch button). Disabled when no stored setup is resolved. */
export function WatchToggle({match}:{match?:Match}){
 const p=useProduct();const [busy,setBusy]=useState(false);
 const watching=!!match&&!!p.product?.watchlist?.some((x:any)=>x.id===match.id);
 async function toggle(){if(!match)return;setBusy(true);try{await api('/api/product/watch',{action:watching?'remove':'add',match_id:match.id});await p.refreshProduct()}catch(e:any){p.setToast(e.message)}finally{setBusy(false)}}
 const name=match?`${match.symbol} ${match.pattern_name} ${match.timeframe}`:'';
 return <Button label={watching?'Watching':'Watch'} icon="eye" kind="outline" loading={busy} disabled={!match} accessibilityLabel={!match?'Watch unavailable: no stored setup selected':watching?`Watching ${name}. Remove from watch`:`Watch ${name}`} accessibilityState={{selected:watching,disabled:!match}} onPress={toggle} style={{minHeight:36,paddingVertical:6,paddingHorizontal:12}}/>;
}
/** The scanner-connection line under the top bar (BACKLOG item 2a).
 *
 * The scanner takes minutes to load its scan at startup and can be restarted mid-day. While it is quiet the
 * pilot keeps serving the LAST scan, so the honest thing to say is when that scan was taken, not that
 * research is unavailable. Three states, and only the last two are errors:
 *   - a scan is on screen, quiet for under 5 minutes  → "Showing the last scan · 17 Sep 15:30 · reconnecting…"
 *   - the same, but it has not come back              → same data, plainly told the connection is not back
 *   - nothing was ever cached                         → there is genuinely nothing to show
 * Retry stays in every case. Nothing here loosens a trade gate: a plan built on a scan this old is still
 * refused at simulate and live submission by the server (kanida_pilot/evidence.require_current_evidence). */
export function ResearchBanner(){
 const {error,state,refresh}=useProduct();
 // scoped to scanner-backed pages: the strategy product does not read the scanner, so its screens never show a
 // "nothing has changed" line about it (GTM audit P11 - one status, where it applies)
 const path=usePathname()||'';
 // `error` only counts as "no scan at all" when there is genuinely no state in hand. With a state already
 // loaded, a failed poll means the screen is a moment behind - which is worth a quiet line, not an error.
 const view=connectionView(state,{error:state?'':error});
 const text=view?view.text:error?'Could not refresh just now — showing what loaded a moment ago.':'';
 if(!text||path.startsWith('/strategies'))return null;
 const red=view?.tone==='very-stale';
 const label=view?view.a11y:text;
 return <View role="status" accessibilityLiveRegion="polite" accessibilityLabel={label} style={[s.between,{padding:12,backgroundColor:red?'#2A1519':C.amberBg}]}>
  <T style={{fontSize:13,color:red?C.red:C.amber,flex:1}}>{text}</T>
  <Button label="Retry" kind="ghost" onPress={refresh}/>
 </View>;
}
