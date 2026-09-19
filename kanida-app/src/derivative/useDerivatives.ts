// One read per card, from the pilot's read-only F&O routes. Deliberately plain: each card owns its request,
// a stale response (the path changed, or a newer request started) is dropped, and nothing is cached across
// filter changes - a 15-minute store has nothing to gain from a stale-while-revalidate cache and everything
// to lose from showing one mark's numbers under another mark's as-of line.
import {useCallback,useEffect,useRef,useState} from 'react';
import {api} from '../model';
export type Phase='idle'|'loading'|'done'|'error';
export type Read<T>={data:T|null;error:string;phase:Phase;reload:()=>void};
/** `path` null = do not fetch (e.g. no underlying chosen yet). `seq` bumps to force a refresh. */
export function useDerivativeRead<T>(path:string|null,seq=0):Read<T>{
 const [state,setState]=useState<{path:string;data:T|null;error:string;phase:Phase}>({path:'',data:null,error:'',phase:'idle'});
 const id=useRef(0);
 const run=useCallback(()=>{
  const mine=++id.current;
  if(!path){setState({path:'',data:null,error:'',phase:'idle'});return}
  setState({path,data:null,error:'',phase:'loading'});
  api(path).then((d:any)=>{if(mine===id.current)setState({path,data:d as T,error:'',phase:'done'})},
   (e:any)=>{if(mine===id.current)setState({path,data:null,error:e?.message||'Unable to load',phase:'error'})});
 },[path]);
 useEffect(()=>{run();return()=>{id.current++}},[run,seq]);
 const fresh=state.path===(path||'');
 return {data:fresh?state.data:null,error:fresh?state.error:'',phase:fresh?state.phase:(path?'loading':'idle'),reload:run};
}
