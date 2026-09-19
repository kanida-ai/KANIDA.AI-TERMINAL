// Per-block link state (spec §3.2). Each block is its own link group; the last row click in scanner A or B wins.
import {useCallback,useState} from 'react';
import {evidenceKey} from '../activeSymbol';
import type {LiveDetection,Side,StrategyRow,StrategySummary} from '../strategies/types';
export type Slot='A'|'B';
export type BlockLink={slot:Slot;strategyKey:string;strategyName:string;symbol:string;company:string;timeframe:string;side:Side;matchId:string;pattern:string;patternName:string;dataEnd:string|null;
 /** Set only when the row came from a LIVE detection. `detectionId` is the one identity that is unique per
  * episode, so it is what the chart draws by and what the evidence card is gated on; `geometryNote` is
  * non-empty exactly when the detector published nothing drawable. */
 detectionId?:string|null;detectionState?:string|null;detectedAt?:string|null;drawable?:boolean;geometryNote?:string|null;
 evidenceCompatible?:boolean;evidenceNote?:string|null};
export function linkFromRow(slot:Slot,st:StrategySummary,row:StrategyRow):BlockLink{
 const pattern=row.match_id?.split(':')[2]||st.pattern;
 return {slot,strategyKey:st.key,strategyName:st.name,symbol:row.symbol,company:row.company,timeframe:row.timeframe||st.timeframe,side:row.side||st.side,matchId:row.match_id,pattern,patternName:st.pattern_name,dataEnd:row.candle_end};
}
/** A link from one LIVE detection. Carries the detection's own identity so the chart draws THIS episode and
 * the evidence card is gated on THIS detection's detector hash, never on the pattern name. */
export function linkFromDetection(slot:Slot,st:StrategySummary,d:LiveDetection):BlockLink{
 return {slot,strategyKey:st.key,strategyName:st.name,symbol:d.symbol,company:d.company||d.symbol,
  timeframe:d.timeframe,side:d.side,matchId:d.match_id,pattern:d.pattern_id,patternName:d.pattern_name||st.pattern_name,
  dataEnd:d.as_of,detectionId:d.detection_id,detectionState:d.state,detectedAt:d.detected_at,
  drawable:d.drawable!==false,geometryNote:d.geometry_note||null,
  evidenceCompatible:d.evidence_compatible!==false,evidenceNote:d.evidence?.note||null};
}
/** Link identity = strategy + symbol + timeframe + side + data end (evidenceKey with rule = strategy key), plus
 * the detection id when there is one: two episodes of the same pattern on the same stock are different cards. */
export function linkKey(l:BlockLink|null|undefined){
 if(!l)return '';
 const base=evidenceKey({symbol:l.symbol,pattern:l.pattern,timeframe:l.timeframe,side:l.side,rule:l.strategyKey,dataEnd:l.dataEnd})||['lk1',l.strategyKey,l.symbol,l.timeframe,l.side,l.matchId].map(v=>encodeURIComponent(v||'')).join('|');
 return l.detectionId?`${base}|d:${encodeURIComponent(l.detectionId)}`:base;
}
export function sameRow(l:BlockLink|null|undefined,slot:Slot,strategyKey:string|null,row:StrategyRow){return !!l&&l.slot===slot&&l.strategyKey===strategyKey&&l.symbol===row.symbol&&l.timeframe===row.timeframe;}
export function useBlockLinks(){
 const [links,setLinks]=useState<Record<string,BlockLink|null>>({});
 const setLink=useCallback((block:string,link:BlockLink|null)=>setLinks(x=>x[block]===link?x:{...x,[block]:link}),[]);
 return {links,setLink};
}
