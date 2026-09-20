import { Platform } from 'react-native';
export type Stats = {n:number; display_return_pct:number|null; expectancy_pct:number|null; win_rate:number|null; return_band:string|null; holding?:{duration:string; label:string; detail:string; bars:number}; max_adverse_pct?:number|null; expectancy_ci95?:number[];};
export type History = {side:'long'|'short'; run:string; status:string; reference:Stats; test:Stats};
export type Match = {id:string; detection_id?:string; symbol:string; company:string; price:number; pattern:string; pattern_name:string; timeframe:string; direction:string; state:string; current:boolean; candle_end:string; score:number; sector:string; universes:string[]; history:History[]; start_index:number;};
export type Filters = {timeframe:string; direction:string; band:string; minimum:number; sector:string; universe:string; current:boolean; patterns?:string[]};
// minimum must equal MIN_TRADES_DEFAULT in constants.ts (the source of truth; decision.ts re-exports it), and sizing's costPct default must equal ROUND_TRIP_COST_PCT there.
// Both stay literals: scripts/check-model.cjs evaluates this file alone with a require shim that only allows 'react-native', so importing './constants' would break that check.
export const initialFilters:Filters={timeframe:'All',direction:'all',band:'',minimum:10,sector:'',universe:'',current:false,patterns:[]};
export const sectorName=(value:string)=>['it','information technology'].includes((value||'').toLowerCase())?'Information Technology':value;
export const bands=[['','Any return'],['0_0.5','0–0.5%'],['0.5_1','0.5–1%'],['1_2','1–2%'],['2_5','2–5%'],['5_10','5–10%'],['over_10','Over 10%']];
export function eligible(h:History, f:Filters){const s=h.reference; return (s.n||0)>=f.minimum && (!f.band || ((s.n||0)>0&&s.return_band===f.band));}
export function histories(m:Match, f:Filters){return m.history.filter(h=>eligible(h,f));}
export function searchText(value:string){return value.toLowerCase().replace(/&/g,' and ').replace(/\b(1|4)\s*(hour|hours|hr)\b/g,'$1h').replace(/\bdaily\b/g,'1d').replace(/\bweekly\b/g,'1w').replace(/[^a-z0-9]+/g,' ').trim();}
export function filterMatches(matches:Match[],f:Filters,term=''){
 const terms=searchText(term).split(' ').filter(Boolean);
 return matches.filter(m=>(f.timeframe==='All'||m.timeframe===f.timeframe)&&
  (f.direction==='all'||m.direction===f.direction)&&(!f.current||m.current)&&
  (!f.sector||sectorName(m.sector)===sectorName(f.sector))&&(!f.universe||m.universes.includes(f.universe))&&
  (!f.patterns?.length||f.patterns.includes(m.pattern))&&
  terms.every(q=>searchText(`${m.symbol} ${m.company} ${m.pattern_name} ${m.timeframe}`).includes(q))&&
  (histories(m,f).length>0||(f.minimum===0&&!f.band&&!m.history.length)));
}
export function pct(v:number|null|undefined,d=2){return v==null||!Number.isFinite(v)?'—':`${v>0?'+':''}${v.toFixed(d)}%`;}
export function money(v:number|undefined,decimals=0){return v==null?'—':'₹'+v.toLocaleString('en-IN',{minimumFractionDigits:decimals,maximumFractionDigits:decimals});}
export function dateText(v?:string){if(!v)return 'Unavailable'; const d=new Date(v.slice(0,10)+'T12:00:00'); return d.toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'});}
const MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
/** "13 Sep 2026 · 07:25 IST" in Asia/Kolkata (fixed +05:30, no DST). Accepts an ISO string, epoch seconds (server stamps) or epoch milliseconds. */
export function dateTimeText(v?:string|number|null){if(v==null||v===''||v===0)return 'Unavailable'; const ms=typeof v==='number'?(v<1e12?v*1000:v):Date.parse(v); if(!Number.isFinite(ms))return 'Unavailable'; const d=new Date(ms+19800000),p=(n:number)=>String(n).padStart(2,'0'); return `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()} · ${p(d.getUTCHours())}:${p(d.getUTCMinutes())} IST`;}
export const apiBase=Platform.OS==='web'?'':process.env.EXPO_PUBLIC_API_URL||'';
let sessionToken='',sessionCsrf='';
export function setApiSession(token='',csrf=''){sessionToken=token;sessionCsrf=csrf;}
export class ApiError extends Error{constructor(message:string,public status:number,public code:string){super(message)}}
/** `signal` lets a CALLER cancel a request it no longer wants. The 30-second timeout is unchanged and still
 *  applies; a caller's signal simply aborts the same controller early. It is optional, so nothing that does
 *  not pass one behaves differently. The Derivative tab uses it: on a rapid symbol or expiry change the
 *  answer to the symbol the reader has already moved off is not just dropped on arrival, it is never
 *  finished - which is one fewer way for two panels to end up describing two different instruments. */
export async function api(path:string,body?:unknown,extraHeaders:Record<string,string>={},signal?:AbortSignal){
 const controller=new AbortController();const timeout=setTimeout(()=>controller.abort(),30000);
 const stop=()=>controller.abort();
 if(signal){if(signal.aborted)controller.abort();else signal.addEventListener('abort',stop);}
 try {const r=await fetch(apiBase+path,{signal:controller.signal,credentials:'include',headers:{...(body?{'Content-Type':'application/json'}:{}),...(sessionToken?{Authorization:'Bearer '+sessionToken}:{}),...(sessionCsrf?{'X-Kanida-CSRF':sessionCsrf}:{}),...extraHeaders},...(body?{method:'POST',body:JSON.stringify(body)}:{})});const d=await r.json();if(!r.ok)throw new ApiError(d.error||'Unable to load KANIDA',r.status,d.code||'REQUEST_FAILED');return d;} finally {clearTimeout(timeout);if(signal)signal.removeEventListener('abort',stop);}
}
export function cellQuery(m:Match,side:string){return `symbol=${encodeURIComponent(m.symbol)}&timeframe=${m.timeframe}&pattern=${m.pattern}&side=${side}`;}
export function sizing(price:number,account:number,allocation:number,risk:number,stop:number,costPct=.4){
 if(![price,account,allocation,risk,stop].every(Number.isFinite)||Math.min(price,account,allocation,risk,stop)<=0)return {shares:0,notional:0,cost:0,loss:0};
 const shares=Math.max(0,Math.min(Math.floor(allocation/(price*(1+costPct/100))),Math.floor((account*risk/100)/(price*(stop+costPct)/100))));
 return {shares,notional:shares*price,cost:shares*price*costPct/100,loss:shares*price*(stop+costPct)/100};
}
