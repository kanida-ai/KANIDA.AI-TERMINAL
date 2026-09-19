import {Platform} from 'react-native';
import * as SecureStore from 'expo-secure-store';
import {api,apiBase,ApiError} from '../model';
import type {ScanJob} from '../strategies/types';
// Falcon endpoints (spec §5). `api()` only does GET/POST and keeps the session private, so DELETE goes through a tiny local fetch that mirrors its headers:
// native = Bearer token from the same SecureStore key Auth uses (no CSRF needed); web = session cookie + CSRF from /api/auth/me.
const SESSION_KEY='kanida.pilot.session';
async function send(path:string,method:'DELETE'){
 const headers:Record<string,string>={};
 if(Platform.OS!=='web'){const token=await SecureStore.getItemAsync(SESSION_KEY).catch(()=>null);if(token)headers.Authorization='Bearer '+token;}
 else{const me=await api('/api/auth/me').catch(()=>null);if(me?.csrf)headers['X-Kanida-CSRF']=me.csrf;}
 const controller=new AbortController();const timeout=setTimeout(()=>controller.abort(),15000);
 try{const r=await fetch(apiBase+path,{method,signal:controller.signal,credentials:'include',headers});const d=await r.json().catch(()=>({}));if(!r.ok)throw new ApiError(d.error||'Unable to reach KANIDA',r.status,d.code||'REQUEST_FAILED');return d;}finally{clearTimeout(timeout);}
}
export const falconApi={
 start:async():Promise<{id:string}>=>api('/api/falcon/scan',{}),
 get:async(id:string):Promise<ScanJob>=>api(`/api/falcon/scan/${encodeURIComponent(id)}`),
 cancel:async(id:string)=>send(`/api/falcon/scan/${encodeURIComponent(id)}`,'DELETE'),
 latest:async():Promise<ScanJob|null>=>{const d=await api('/api/falcon/latest');return d&&typeof d==='object'&&Array.isArray(d.stages)?d:d?.job&&Array.isArray(d.job.stages)?d.job:null;},
};
