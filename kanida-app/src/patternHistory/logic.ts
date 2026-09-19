import type {Selection} from './types';
export const HORIZONS=[1,3,5,10];
export const signed=(value:number|null|undefined)=>value==null||!Number.isFinite(value)?'—':`${value>0?'+':''}${value.toFixed(2)}%`;
export const date=(value:string|null|undefined)=>value?value.slice(0,10):'—';
export const stateLabel=(state:string)=>state==='setup'?'Forming / setup':'Confirmed';
export const variantLabel=(variant:string)=>variant==='canonical_context'?'With prior downtrend':'Standard definition';
export function query(selection:Selection,extra:Record<string,string|number>={}){return Object.entries({...selection,...extra}).filter(([,v])=>v!=null&&v!=='').map(([k,v])=>`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`).join('&');}
// `state` is reserved by Expo's navigator. API queries retain state; page links use phase.
export function historyHref(selection:Selection,horizon=5){return '/pattern-history?'+query(selection,{horizon}).replace(/(^|&)state=/,'$1phase=');}
export function pilotSelection(link:{strategyKey:string;symbol:string;timeframe:string;side:string;detectionId?:string|null;detectionState?:string|null}|null):Selection|null{
 if(!link||link.timeframe!=='1D'||link.side!=='long')return null;
 const m=/^(ch16|cdlengulfing)-(canonical(?:_context)?)-long-1d$/i.exec(link.strategyKey);
 if(!m||m[1].toLowerCase()==='ch16'&&m[2]!=='canonical')return null;
 return {symbol:link.symbol,pattern_id:m[1].toUpperCase(),variant:m[2],state:['setup','forming'].includes(link.detectionState||'')?'setup':'confirmed',strategy_key:link.strategyKey,...(link.detectionId?{detection_id:link.detectionId}:{})};
}
