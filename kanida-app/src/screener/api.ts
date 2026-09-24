// The screener's calls. Every one goes through the app's `api` (cookie or bearer, CSRF, 30-second timeout).
import {api} from '../model';
import type {Alert,Definition,Notify,Parsed,Results,Scanner,ScreenerStatus,Vocabulary} from './types';

export type View='active'|'ended'|'all';
export const screenerApi={
 vocabulary:()=>api('/api/screener/vocabulary') as Promise<Vocabulary>,
 status:()=>api('/api/screener/status') as Promise<ScreenerStatus>,
 scanners:()=>api('/api/screener/scanners') as Promise<{scanners:Scanner[];status:ScreenerStatus;unread:number}>,
 create:(name:string,definition:Definition,source:'visual'|'nl',nl_text?:string)=>
  api('/api/screener/scanners',{name,definition,source,nl_text}) as Promise<Scanner>,
 update:(id:string,patch:{name?:string;definition?:Definition;nl_text?:string})=>
  api(`/api/screener/scanners/${encodeURIComponent(id)}`,patch) as Promise<Scanner>,
 duplicate:(id:string)=>api(`/api/screener/scanners/${encodeURIComponent(id)}/duplicate`,{}) as Promise<Scanner>,
 remove:(id:string)=>api(`/api/screener/scanners/${encodeURIComponent(id)}/delete`,{}) as Promise<{ok:boolean}>,
 notify:(id:string,n:Notify)=>api(`/api/screener/scanners/${encodeURIComponent(id)}/notify`,n) as Promise<Scanner>,
 results:(id:string,view:View,signal?:AbortSignal)=>
  api(`/api/screener/scanners/${encodeURIComponent(id)}/results?view=${view}&limit=100`,undefined,{},signal) as Promise<Results&{scanner:Scanner}>,
 run:(definition:Definition,view:View)=>api('/api/screener/run',{definition,view,limit:100}) as Promise<Results>,
 describe:(definition:Definition)=>api('/api/screener/describe',{definition}) as Promise<{definition:Definition;reads_as:string;grain:string;computed:boolean}>,
 parse:(text:string)=>api('/api/screener/parse',{text}) as Promise<Parsed>,
 alerts:()=>api('/api/screener/alerts') as Promise<{alerts:Alert[];unread:number}>,
 readAlerts:(ids?:number[])=>api('/api/screener/alerts/read',{ids:ids||[]}) as Promise<{ok:boolean;unread:number}>,
};
