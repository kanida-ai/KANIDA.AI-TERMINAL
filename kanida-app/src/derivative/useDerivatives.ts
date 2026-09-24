// One read per card, from the pilot's read-only F&O routes. Deliberately plain: each card owns its request,
// a stale response (the path changed, or a newer request started) is dropped, and nothing is cached across
// filter changes - a 15-minute store has nothing to gain from a stale-while-revalidate cache and everything
// to lose from showing one reading's numbers under another reading's as-of line.
//
// TWO GUARDS, NOT ONE.
//
//  1. THE SEQUENCE GUARD (unchanged). Every run takes a number; a response whose number is not the current
//     one is thrown away rather than written into state. This is what stops a slow answer for NIFTY landing
//     in a panel the reader has already pointed at RELIANCE.
//
//  2. CANCELLATION (added). The obsolete request is now ABORTED as well as ignored, so it is never finished
//     at all. The sequence guard alone leaves the browser holding several in-flight requests for the same
//     panel through a rapid symbol or expiry change, and every one of them still arrives, still parses, and
//     still has to be identified as stale. One of those being mis-identified is exactly how two panels come
//     to describe two different instruments under one heading, which is the defect this tab is being
//     repaired for. `api` takes the signal; the 30-second timeout it already applies is unchanged.
//
// Neither guard EVER blends two answers. A response is used whole or not at all.
import {useCallback,useEffect,useRef,useState} from 'react';
import {api} from '../model';
export type Phase='idle'|'loading'|'done'|'error';
export type Read<T>={data:T|null;error:string;phase:Phase;reload:()=>void};
/** `path` null = do not fetch (e.g. no underlying chosen yet). `seq` bumps to force a refresh. */
export function useDerivativeRead<T>(path:string|null,seq=0):Read<T>{
 const [state,setState]=useState<{path:string;data:T|null;error:string;phase:Phase}>({path:'',data:null,error:'',phase:'idle'});
 const id=useRef(0);
 const abort=useRef<AbortController|null>(null);
 const run=useCallback(()=>{
  const mine=++id.current;
  // whatever this hook had in flight is now obsolete by definition: a new run replaced it
  abort.current?.abort();
  if(!path){abort.current=null;setState({path:'',data:null,error:'',phase:'idle'});return}
  const controller=new AbortController();
  abort.current=controller;
  setState({path,data:null,error:'',phase:'loading'});
  api(path,undefined,undefined,controller.signal).then((d:any)=>{if(mine===id.current)setState({path,data:d as T,error:'',phase:'done'})},
   (e:any)=>{
    // a request WE cancelled is not a failure the reader should see: the panel it belonged to has already
    // moved on, and an "Unable to load" under a symbol nobody is looking at is noise, not information.
    if(mine!==id.current||controller.signal.aborted)return;
    setState({path,data:null,error:e?.message||'Unable to load',phase:'error'});
   });
 },[path]);
 useEffect(()=>{run();return()=>{id.current++;abort.current?.abort();abort.current=null}},[run,seq]);
 const fresh=state.path===(path||'');
 return {data:fresh?state.data:null,error:fresh?state.error:'',phase:fresh?state.phase:(path?'loading':'idle'),reload:run};
}

// ================================================================================================================
// THE TAB FOLLOWS THE MARKET.
//
// Found live on 21 Sep 2026 at 09:33 IST: the 09:30 reading was captured, computed and served by the API — the
// screener route answered "2026-09-21 09:30:00" — while the open tab went on printing Friday's 15:45 header and
// Friday's 11:30 story, indefinitely. Every read on this tab is re-run only when `seq` bumps, and before this
// nothing bumped it but the Refresh button. A tab that is right when it loads and silently wrong fifteen
// minutes later is the worst kind of stale: nothing on it says so.
//
// So one light read — the status route's `as_of`, the newest reading the store holds — runs once a minute, and when that reading CHANGES the tab re-reads everything, exactly as the Refresh
// button does. It never moves a reader who has pinned a past reading: `active` is false then, and a view of
// 10:15 that stays on 10:15 is not stale, it is what was asked for.
// ================================================================================================================
export const LIVE_POLL_MS=10*1000; // a 50 ms read; a new reading reaches the screen within ~10 s of its metrics landing

/** True when the store has a reading newer than the one this page last saw. The first sighting only records. */
export function readingAdvanced(seen:string,newest:string):boolean{
 return !!seen&&!!newest&&newest!==seen;
}

export function useFollowLatest(active:boolean,onNewReading:()=>void){
 const seen=useRef('');
 const cb=useRef(onNewReading);
 cb.current=onNewReading;
 useEffect(()=>{
  if(!active)return;
  let alive=true;
  // Hidden tabs poll too. A tab left in the background should already be current when the reader comes
  // back to it, and one small read a minute costs nothing; skipping hidden tabs also made the fix
  // unverifiable from an audit tab that is not in front (found 21 Sep 09:48).
  const tick=()=>{
   api('/api/derivatives/status').then((d:any)=>{
    if(!alive)return;
    const newest=String(d?.as_of||'');
    if(readingAdvanced(seen.current,newest))cb.current();
    if(newest)seen.current=newest;
   },()=>{});
  };
  tick();
  const timer=setInterval(tick,LIVE_POLL_MS);
  // a tab brought back to the front checks at once, rather than up to a minute later
  const onVisible=()=>{if(typeof document!=='undefined'&&document.visibilityState==='visible')tick();};
  if(typeof document!=='undefined')document.addEventListener('visibilitychange',onVisible);
  return ()=>{alive=false;clearInterval(timer);
   if(typeof document!=='undefined')document.removeEventListener('visibilitychange',onVisible);};
 },[active]);
}
