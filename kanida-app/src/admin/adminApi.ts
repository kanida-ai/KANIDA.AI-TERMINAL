import {Platform} from 'react-native';
import {api,apiBase,ApiError} from '../model';
import type {Audience,BlockDef,Side,StrategyDef,StrategyResults,StrategySources} from '../strategies/types';
// Owner registry endpoints (FALCON_DISCOVER_SPEC §5/§6; implemented by the pilot server). GET/POST go through api(); api() has no PATCH, so PATCH sends the same session headers itself:
// web = session cookie + the session CSRF token (read fresh from /api/auth/me, the value Auth stores). Native has no access to the bearer token here, so PATCH is web-only.
export type AdminBlock=BlockDef&{strategies:StrategyDef[]};
export type AdminRegistry={registry_version:number;blocks:AdminBlock[]};
// The two shapes POST /api/admin/strategies accepts. A stored pattern is named by the scanner pattern id; a
// researched one by its catalogue identity (pattern_id + variant + side + timeframe), which is what resolves its
// evidence cell. Sending the wrong one is the 400 that broke the form (BACKLOG item 4), so the type forbids it.
type CommonInput={key?:string;block_key:string;name?:string;description?:string;tags?:string[];audience?:Audience;
 min_trades?:number;default_slot?:'A'|'B'|null;order?:number;enabled?:boolean;timeframe:string;side:Side};
export type StoredStrategyInput=CommonInput&{source_type:'stored_pattern';pattern:string;pattern_name?:string};
export type ResearchStrategyInput=CommonInput&{source_type:'research_pattern';pattern_id:string;variant:string};
export type StrategyInput=StoredStrategyInput|ResearchStrategyInput;
export type StrategyPatch=Partial<Pick<StrategyDef,'name'|'description'|'tags'|'audience'|'min_trades'|'default_slot'|'order'|'enabled'|'block_key'>>;
export type BlockPatch=Partial<Pick<BlockDef,'title'|'description'|'kind'|'order'|'enabled'>>;
export const patchSupported=Platform.OS==='web';
function message(d:any,status:number){if(typeof d?.error==='string')return d.error;if(Array.isArray(d?.detail))return d.detail.map((x:any)=>[Array.isArray(x?.loc)?x.loc.filter((v:any)=>v!=='body').join('.'):'',x?.msg].filter(Boolean).join(': ')).join(' · ')||`Request failed (${status})`;if(typeof d?.detail==='string')return d.detail;return `Request failed (${status})`;}
async function patch<R>(path:string,body:unknown):Promise<R>{
 if(!patchSupported)throw new ApiError('Editing the registry is available on the web app only.',400,'WEB_ONLY');
 const me:any=await api('/api/auth/me'),controller=new AbortController(),timer=setTimeout(()=>controller.abort(),30000);
 try{const r=await fetch(apiBase+path,{method:'PATCH',signal:controller.signal,credentials:'include',headers:{'Content-Type':'application/json',...(me?.csrf?{'X-Kanida-CSRF':me.csrf}:{})},body:JSON.stringify(body)});let d:any={};try{d=await r.json()}catch{}if(!r.ok)throw new ApiError(message(d,r.status),r.status,d?.code||'REQUEST_FAILED');return d as R;}
 catch(e:any){if(e?.name==='AbortError')throw new ApiError('The server took too long to answer. Refresh to check whether the change was saved.',504,'TIMEOUT');throw e}
 finally{clearTimeout(timer)}
}
const k=(key:string)=>encodeURIComponent(key);
export const adminApi={
 registry:()=>api('/api/admin/strategies') as Promise<AdminRegistry>,
 /** What the add form may offer. Owner-only, and the only list the form is allowed to build its chips from. */
 sources:()=>api('/api/admin/strategy-sources') as Promise<StrategySources>,
 createStrategy:(body:StrategyInput)=>api('/api/admin/strategies',body) as Promise<StrategyDef>,
 updateStrategy:(key:string,body:StrategyPatch)=>patch<StrategyDef>(`/api/admin/strategies/${k(key)}`,body),
 preview:(key:string)=>api(`/api/admin/strategies/${k(key)}/preview`,{}) as Promise<StrategyResults>,
 createBlock:(body:BlockDef)=>api('/api/admin/blocks',body) as Promise<BlockDef>,
 updateBlock:(key:string,body:BlockPatch)=>patch<BlockDef>(`/api/admin/blocks/${k(key)}`,body),
};
