import React,{createContext,useContext,useEffect,useState,useCallback,useRef} from 'react';
import {Platform,Linking,AppState} from 'react-native';
import * as SecureStore from 'expo-secure-store';
import * as Crypto from 'expo-crypto';
import * as WebBrowser from 'expo-web-browser';
import {api,apiBase,setApiSession} from './model';
const AuthContext=createContext<any>(null);
export const useAuth=()=>useContext(AuthContext);
const KEY='kanida.pilot.session';
export function AuthProvider({children}:any){
 const [user,setUser]=useState<any>(null),[config,setConfig]=useState<any>(null),[billing,setBilling]=useState<any>(null),[ready,setReady]=useState(false),[error,setError]=useState(''),[device,setDevice]=useState<any>(null);
 const nativeToken=useRef(''),generation=useRef(0);
 const refresh=useCallback(async()=>{const version=generation.current;try{const [c,me]=await Promise.all([api('/api/pilot/config'),api('/api/auth/me')]);if(version!==generation.current)return;setConfig(c);setUser(me.user);setBilling(me.billing);setApiSession(nativeToken.current,me.csrf||'');setError('');}catch(e:any){if(version===generation.current)setError('The private pilot service is unavailable. Please reconnect.');}finally{if(version===generation.current)setReady(true)}},[]);
 useEffect(()=>{let active=true;(async()=>{if(Platform.OS!=='web'){nativeToken.current=await SecureStore.getItemAsync(KEY)||'';setApiSession(nativeToken.current)}if(active)await refresh()})().catch(()=>{setError('Your secure session could not be opened. Please sign in again.');setReady(true)});return()=>{active=false}},[]);
 useEffect(()=>{if(!ready)return;const timer=setInterval(refresh,60000);const subscription=AppState.addEventListener('change',state=>{if(state==='active')refresh()});return()=>{clearInterval(timer);subscription.remove()}},[ready,refresh]);
 async function accept(value:any){generation.current++;if(Platform.OS!=='web'&&value.access_token){nativeToken.current=value.access_token;await SecureStore.setItemAsync(KEY,value.access_token)}setApiSession(nativeToken.current,value.csrf||'');await refresh();}
 async function signIn(data:any){await accept(await api('/api/auth/login',data,Platform.OS==='web'?{}:{'X-Kanida-Client':'native'}))}
 async function signUp(data:any){await accept(await api('/api/auth/register',data,Platform.OS==='web'?{}:{'X-Kanida-Client':'native'}))}
 async function signOut(all=false){await api('/api/auth/logout',{all_devices:all});generation.current++;nativeToken.current='';if(Platform.OS!=='web')await SecureStore.deleteItemAsync(KEY);setApiSession();setUser(null);setBilling(null);}
 async function connectBrowser(){
  const verifier=Array.from(Crypto.getRandomBytes(32)).map(n=>n.toString(16).padStart(2,'0')).join('');
  const base64=await Crypto.digestStringAsync(Crypto.CryptoDigestAlgorithm.SHA256,verifier,{encoding:Crypto.CryptoEncoding.BASE64});
  const result=await api('/api/auth/device/start',{challenge:base64.replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'')});
  setDevice({...result,verifier});await WebBrowser.openBrowserAsync(result.url);
 }
 async function finishDevice(){if(!device)return;const value=await api('/api/auth/device/complete',{code:device.code,verifier:device.verifier});if(value.pending)throw Error('Approve the device in the browser first, then return here.');await accept(value);setDevice(null);}
 function google(invite=''){if(Platform.OS==='web')window.location.assign('/api/auth/google/start'+(invite?'?invite='+encodeURIComponent(invite):''));else return connectBrowser()}
 return <AuthContext.Provider value={{user,config,billing,ready,error,refresh,signIn,signUp,signOut,google,device,finishDevice,connectBrowser}}>{children}</AuthContext.Provider>
}
export function openExternal(url:string){if(Platform.OS==='web')window.location.assign(url);else return Linking.openURL(url)}
