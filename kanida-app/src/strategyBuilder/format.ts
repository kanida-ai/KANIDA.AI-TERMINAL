// Formatting for the builder. Every market time is IST as the server stored it: strings are parsed as text, never
// through the browser's Date/timezone, so a 29 Sep expiry reads 29 Sep in every timezone (a bug seen in two competitors).
const MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const DAYS=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];

/** '2026-09-29' → '29 Sep' (the calendar date, no timezone conversion). */
export function dayMonth(d?:string|null){if(!d)return '—';const [y,m,dd]=d.slice(0,10).split('-').map(Number);return `${dd} ${MONTHS[m-1]}`+(y!==2026?` ${y}`:'');}
/** Weekday of a calendar date computed in UTC arithmetic so the browser zone cannot shift it. */
export function weekday(d:string){const [y,m,dd]=d.slice(0,10).split('-').map(Number);return DAYS[new Date(Date.UTC(y,m-1,dd)).getUTCDay()];}
/** '2026-09-23 15:45:00' → 'Wed 23 Sep 15:45 IST'. */
export function istStamp(t?:string|null){if(!t)return '—';const d=t.slice(0,10);return `${weekday(d)} ${dayMonth(d)} ${t.slice(11,16)} IST`;}
/** Add n calendar days to a YYYY-MM-DD string (UTC arithmetic, no zone). */
export function addDays(d:string,n:number){const [y,m,dd]=d.slice(0,10).split('-').map(Number);const x=new Date(Date.UTC(y,m-1,dd+n));return x.toISOString().slice(0,10);}
export function isWeekend(d:string){const w=weekday(d);return w==='Sat'||w==='Sun';}

export const inr=(v:number|null|undefined,dp=0)=>v==null||!Number.isFinite(v)?'—':(v<0?'−':'')+'₹'+Math.abs(v).toLocaleString('en-IN',{maximumFractionDigits:dp,minimumFractionDigits:dp});
export const signed=(v:number|null|undefined,dp=0)=>v==null||!Number.isFinite(v)?'—':(v>0?'+':v<0?'−':'')+'₹'+Math.abs(v).toLocaleString('en-IN',{maximumFractionDigits:dp,minimumFractionDigits:dp});
export const num=(v:number|null|undefined,dp=2)=>v==null||!Number.isFinite(v)?'—':v.toLocaleString('en-IN',{maximumFractionDigits:dp,minimumFractionDigits:0});
export const pts=(v:number|null|undefined)=>v==null?'—':v.toLocaleString('en-IN',{maximumFractionDigits:2});
export const strikeText=(k:number)=>Number.isInteger(k)?String(k):k.toFixed(2);
