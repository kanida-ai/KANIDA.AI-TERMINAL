// THE WORKSPACE'S ONE DATA HUB.
//
// Five widgets on NIFTY must not fetch NIFTY's PCR five times. Most widgets here are the Derivative tab's own
// blocks, which make their own reads through the app's `api` — so the dedupe sits one level below them, at
// `fetch`, while the workspace page is mounted, and is removed when it unmounts:
//
//   · IN FLIGHT: two identical GETs share one network request. Each caller gets its own clone of the response.
//   · CACHED FOR THE READING: a finished GET is kept until the next 15-min reading lands (`nextReading()`), so a
//     widget that scrolls into view, is duplicated or is expanded reads the answer already on the page.
//   · ONLY the read routes this page draws (/api/derivatives/, /api/screener/, /api/workspace/) and never a
//     status route, which is what tells the page a new reading has landed.
//   · A caller that aborts gets its own AbortError; the shared request carries on for the others.
//
// Nothing is ever blended: a response is one server answer, whole, or it is not used.
const SHARED=/\/api\/(derivatives|screener|workspace)\//;
const NEVER=/\/status\b|\/alerts\b/;
const MAX=240;
let installs=0;
let original:typeof fetch|null=null;
let epoch=0;
const inflight=new Map<string,Promise<Response>>();
const cache=new Map<string,{epoch:number;res:Response}>();
export const hubStats={network:0,shared:0,cached:0};

function keyOf(input:RequestInfo|URL){return typeof input==='string'?input:input instanceof URL?input.href:input.url;}

function abortable(p:Promise<Response>,signal?:AbortSignal|null){
 if(!signal)return p;
 if(signal.aborted)return Promise.reject(new DOMException('Aborted','AbortError'));
 return new Promise<Response>((resolve,reject)=>{
  const stop=()=>reject(new DOMException('Aborted','AbortError'));
  signal.addEventListener('abort',stop,{once:true});
  p.then(r=>{signal.removeEventListener('abort',stop);resolve(r)},e=>{signal.removeEventListener('abort',stop);reject(e)});
 });
}

export function installHub(){
 installs++;
 if(installs>1||typeof globalThis.fetch!=='function')return;
 original=globalThis.fetch.bind(globalThis);
 globalThis.fetch=((input:RequestInfo|URL,init?:RequestInit)=>{
  const method=(init?.method||(typeof input!=='string'&&!(input instanceof URL)?input.method:'GET')||'GET').toUpperCase();
  const url=keyOf(input);
  if(method!=='GET'||!SHARED.test(url)||NEVER.test(url))return original!(input,init);
  const hit=cache.get(url);
  if(hit&&hit.epoch===epoch){hubStats.cached++;return abortable(Promise.resolve(hit.res.clone()),init?.signal);}
  let p=inflight.get(url);
  if(p)hubStats.shared++;
  else{
   hubStats.network++;
   const mine=epoch;
   const {signal:_ignored,...rest}=init||{};
   p=original!(input,rest).then(res=>{
    inflight.delete(url);
    if(res.ok&&mine===epoch){cache.set(url,{epoch:mine,res:res.clone()});
     if(cache.size>MAX)cache.delete(cache.keys().next().value as string);}
    return res;
   },e=>{inflight.delete(url);throw e});
   inflight.set(url,p);
  }
  return abortable(p.then(r=>r.clone()),init?.signal);
 }) as typeof fetch;
}

export function uninstallHub(){
 installs=Math.max(0,installs-1);
 if(installs===0&&original){globalThis.fetch=original;original=null;cache.clear();inflight.clear();}
}

/** A new 15-min reading landed: every cached answer is now the previous reading's. */
export function nextReading(){epoch++;cache.clear();}
