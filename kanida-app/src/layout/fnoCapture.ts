// F&O CAPTURE HEALTH, BESIDE THE APP-WIDE DATA STATUS.
//
// WHY THIS FILE EXISTS. `/api/derivatives/capture` was built, tested and then called by nothing. It reports
// what was MEASURED in the F&O store at a 15-min reading — one of missing_capture / partial_capture /
// complete / no_eligible_rows / filtered_out / failed — and it exists precisely because the app's own data
// status describes the CASH feed: prices and patterns. Those two are different pipelines with different
// failure modes, and on 18 Sep 2026 they disagreed for a whole afternoon. F&O capture stopped at 11:30 IST
// and the rest of the session was rebuilt from 15-minute candles, which carry no traded-price average and no
// underlying price. The data pill stayed green the entire time, because the cash feed was fine, and nothing
// anywhere on screen said the F&O book had gone dark.
//
// So the pill now carries a second, separate fact. It does not merge with the first and it never changes the
// cash feed's own tone: a healthy cash feed is still reported as healthy. It is one extra amber chip that
// appears only when F&O capture is degraded, with the SERVER's own sentence behind it.
//
// This file does no judging of its own (section 5). The state, the wording and the newest complete reading
// are all the server's; everything here is presentation and a poll.
import React from 'react';
import {api} from '../model';

/** The shape `/api/derivatives/capture` serves. Only the fields this surface shows are named; the route
 *  carries more (per-field coverage, the floors in force) and the Derivative tab reads those itself. */
export type FnoCapture={
 state?:string|null;state_text?:string|null;at?:string|null;source?:string|null;
 /** NEVER true for a partial capture. A reading nothing was measured at cannot be healthy. */
 healthy?:boolean|null;
 /** The newest reading whose required fields were all captured. Named, never substituted. */
 latest_complete_at?:string|null;latest_complete_is_here?:boolean|null;
 /** Present when the store itself could not be read. */
 available?:boolean|null;captured?:boolean|null;
};
/** What the pill and the panel draw. `null` when there is nothing to say — which is the common case. */
export type FnoCaptureView={label:string;headline:string;detail:string;a11y:string};

/** The reader-facing word for each state that is a CAVEAT. A healthy capture needs no badge anywhere: a
 *  badge that is always on is a badge nobody reads. */
const FNO_LABELS:Record<string,string>={
 missing_capture:'F&O not captured',
 partial_capture:'F&O partly captured',
 failed:'F&O store unreadable',
};
/** The one sentence that says what a degraded F&O capture is NOT. It is the distinction the whole route
 *  exists for, and it is stated here once rather than implied by an amber colour. */
export const FNO_SEPARATE_TEXT='This describes F&O capture only. Prices and patterns are a different feed '
 +'and are reported above; one can be healthy while the other is not.';

/** Turn the route's answer into the chip and the sentence behind it, or null when nothing is wrong.
 *
 *  Pure, so scripts/check-derivative.cjs can run it without a browser. It invents no wording: the headline
 *  is a fixed label per state and the detail is the server's own `state_text`. A state this table does not
 *  know is reported as unrecognised rather than assumed healthy — an unknown state is not a good state. */
export function fnoCaptureView(capture?:FnoCapture|null):FnoCaptureView|null{
 if(!capture)return null;
 const state=String(capture.state||'').trim();
 if(!state)return null;
 // healthy===false is the server's own verdict and outranks the table below; a state the table knows is a
 // caveat is shown whatever `healthy` says, because the two must never disagree in the reader's favour.
 const known=FNO_LABELS[state];
 if(!known&&capture.healthy!==false)return null;
 const label=known||'F&O capture unrecognised';
 const detail=String(capture.state_text||'').trim()
  ||'The F&O store reported a capture state this app does not recognise.';
 const when=String(capture.at||'').trim();
 const headline=when?`${label} at the ${when.slice(11,16)} reading`:label;
 return {label,headline,detail,
  a11y:`${headline}. ${detail} ${FNO_SEPARATE_TEXT}`};
}

// --- ONE POLL FOR EVERY CONSUMER --------------------------------------------------------------------------
// The pill and the panel behind it are siblings, and both want this. Two components each running their own
// interval is two requests a minute for one answer that changes every fifteen, so the fetch lives here once
// and every consumer subscribes to the same value.
let latest:FnoCapture|null=null;
let pending:Promise<void>|null=null;
let fetchedAt=0;
const listeners=new Set<(v:FnoCapture|null)=>void>();
/** How long an answer stands before it is fetched again. The store moves on a 15-minute grid, so a five
 *  minute refresh is already three times finer than the data it describes. */
export const FNO_POLL_MS=5*60*1000;

function publish(value:FnoCapture|null){
 latest=value;
 for(const fn of listeners)fn(value);
}
/** A forced refresh still will not fire twice inside this. Each consumer runs its own timer, and two pills
 *  ticking a millisecond apart is two requests for one answer; this collapses them without either consumer
 *  having to know the other exists. */
const FORCE_FLOOR_MS=30*1000;
function refresh(force=false):Promise<void>{
 if(pending)return pending;
 if(force&&fetchedAt&&Date.now()-fetchedAt<FORCE_FLOOR_MS)return Promise.resolve();
 if(!force&&latest&&Date.now()-fetchedAt<FNO_POLL_MS)return Promise.resolve();
 pending=api('/api/derivatives/capture').then((d:any)=>{
  fetchedAt=Date.now();publish((d||null) as FnoCapture|null);
 }).catch(()=>{
  // A request that did not arrive says NOTHING about capture health, so nothing is claimed: the previous
  // answer stands and the chip is simply absent until one does. Inventing a red state from a failed fetch
  // would be the same mistake in the other direction.
  fetchedAt=Date.now();
 }).finally(()=>{pending=null});
 return pending;
}
/** Subscribe to the one capture answer. Every consumer shares one request and one value. */
export function useFnoCapture():FnoCapture|null{
 const [value,setValue]=React.useState<FnoCapture|null>(latest);
 React.useEffect(()=>{
  listeners.add(setValue);
  refresh();
  const timer=setInterval(()=>refresh(true),FNO_POLL_MS);
  return ()=>{listeners.delete(setValue);clearInterval(timer)};
 },[]);
 return value;
}
/** Drop what is held so the next read fetches again. The SUBSCRIBERS are deliberately left alone: clearing
 *  them would leave every mounted pill silently frozen on the answer it happened to have, which is the exact
 *  failure this file exists to stop. */
export function resetFnoCapture(){latest=null;fetchedAt=0;pending=null;publish(null);}
