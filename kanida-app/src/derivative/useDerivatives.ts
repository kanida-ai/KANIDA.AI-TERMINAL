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
