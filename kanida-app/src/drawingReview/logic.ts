import type {Match} from '../model';

export type DrawingSpec={pattern_id:string;name:string;family:string;variant:string;side:string;definition?:string};
export type DrawingExample={spec:DrawingSpec;symbol:string;timeframe:string;event:Record<string,any>;source:{run:string;stock_artifact:string;stock_sha256:string;history_artifact:string;history_sha256:string};bars:any[];window_offset:number;formation_start:number;signal_index:number;signal_at:string;detected_at:string;signal_close_at:string;lines:any[];geometry_note:string;validation:Record<string,boolean>;semantic_review:string;drawing_source:string};
export type MissingExample={spec:DrawingSpec;reason?:string;note?:string};
export type DrawingAudit={run:string;method:string;causality:string;coverage:{catalogue_entries:number;directional_variants:number;patterns_with_examples:number;variants_with_examples:number;variants_without_frozen_occurrences:number;drawable_examples:number};examples:DrawingExample[];missing:MissingExample[]};
export type CatalogueRow={id:string;name:string;family:string;examples:DrawingExample[];missing:MissingExample[]};
export const PAGE_SIZE=9;
export const exampleKey=(e:DrawingExample)=>[e.spec.pattern_id,e.spec.variant,e.spec.side,e.timeframe,e.symbol,e.signal_at].join('|');
export const variantKey=(s:DrawingSpec)=>`${s.variant}|${s.side}`;
export const familyName=(s:string)=>({chart:'Chart patterns',candlestick:'Candlesticks',harmonic:'Harmonics',price_action:'Price action'}[s]||s);
export function drawingCatalogue(data:DrawingAudit):CatalogueRow[]{
 const rows=new Map<string,CatalogueRow>();
 const get=(s:DrawingSpec)=>{let r=rows.get(s.pattern_id);if(!r){r={id:s.pattern_id,name:s.name,family:s.family,examples:[],missing:[]};rows.set(r.id,r)}return r;};
 data.examples.forEach(e=>get(e.spec).examples.push(e));
 data.missing.forEach(e=>get(e.spec).missing.push(e));
 return [...rows.values()].sort((a,b)=>a.id.localeCompare(b.id,undefined,{numeric:true}));
}
/** Each example is already rebased and stops at its signal candle. Never request current market bars. */
export function exampleSnapshot(e:DrawingExample){
 const id=`${e.symbol}:${e.timeframe}:${e.spec.pattern_id}:${e.spec.variant}:${e.spec.side}`;
 const match:Match&Record<string,any>={id,symbol:e.symbol,company:e.symbol,pattern:e.spec.pattern_id,pattern_name:e.spec.name,timeframe:e.timeframe,
  direction:e.event.direction||(e.spec.side==='short'?'bearish':'bullish'),state:e.event.state||'setup',current:false,candle_end:e.signal_close_at,
  price:e.bars[e.bars.length-1]?.close||0,score:e.event.score||0,sector:'',universes:[],history:[],start_index:e.formation_start,end_index:e.signal_index,
  detected_index:Number.isFinite(e.event.detected_index)?e.event.detected_index-e.window_offset:e.signal_index,
  variant:e.spec.variant,side:e.spec.side,family:e.spec.family,lines:e.lines,geometry_note:e.geometry_note,drawable:e.lines.length>0};
 return {match,snapshot:{symbol:e.symbol,timeframe:e.timeframe,bars:e.bars,matches:[match],last_candle:e.signal_close_at}};
}
