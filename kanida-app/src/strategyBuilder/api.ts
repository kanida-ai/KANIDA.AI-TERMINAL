// The Strategy Builder's calls (server/kanida_pilot/strategy_builder/routes.py). GET + POST only, through the app's `api`.
import {api} from '../model';

export type Side='B'|'S';export type Kind='CE'|'PE';
export type Leg={id:string;type:Kind;side:Side;strike:number;lots:number;expiry:string;price_basis:'ltp'|'manual';price:number|null;include:boolean};
export type Scenario={spot?:number;at?:string;iv_shift?:number};
export type Body={underlying:string;expiry:string;legs:Leg[];scenario:Scenario;template?:string|null};
export type Metric={status:'available'|'unavailable'|'unsupported';value?:any;unit?:string;basis?:string;reason?:string;unlimited?:boolean;[k:string]:any};
export type ChainSide={token:number;symbol:string;ltp:number|null;bid:number|null;ask:number|null;oi:number|null;volume:number|null;iv:number|null;iv_reason:string|null;flags:string[];basis:string;last_trade_time:string|null};
export type ChainRow={strike:number;CE:ChainSide|null;PE:ChainSide|null};
export type Chain={underlying:string;expiry:string;as_of:string;spot:number;lot_size:number;tick_size:number;strike_step:number;atm_strike:number;atm_iv:number|null;days_to_expiry:number;rows:ChainRow[];quality:{bid_ask:string;source:string;live:boolean}};
export type Expiry={expiry:string;lot_size:number;contracts:number;monthly:boolean;days_to_expiry:number};
export type LegRow={id:string;label:string;units:number;entry:number;ltp:number|null;iv:number|null;iv_source:string;target_price:number|null;target_pnl:number|null;greeks:Record<string,number>|null};
export type Analysis={status:string;input_hash:string;underlying?:string;structure:{key:string|null;name:string;exact:boolean};model_version?:string;as_of?:string;spot?:number;expiry?:string;lot_size?:number;
 scenario?:{spot:number;at:string;iv_shift:number;days_to_expiry:number;is_expiry:boolean};premium?:Metric;charges?:Metric;max_profit?:Metric;max_loss?:Metric;breakevens?:Metric;
 reward_risk?:Metric;capital_at_risk?:Metric;pop?:Metric;margin?:Metric;scenario_pnl?:Metric;greeks?:any;legs?:LegRow[];curve?:{s:number;expiry:number|null;target:number|null}[];
 sd?:{sigma:number|null;bands:{k:number;low:number;high:number}[]};table?:{s:number;pct:number;target:number|null;expiry:number|null}[];warnings:string[];quality?:any;price_basis?:string[]};
export type Draft={version:number;body:Body;updated_at:number};
export type Strategy={id:string;name:string;thesis:string;tags:string[];underlying:string|null;created_at:number;updated_at:number;archived_at:number|null;source_strategy_id:string|null;draft:Draft|null};
export type LibraryRow=Strategy&{draft_version:number;snapshots:number;paper_open:number;paper_total:number;legs:number;expiry:string|null;structure:string};
export type Snapshot={id:string;n:number;name:string;checksum:string;reading_at:string|null;created_at:number;summary:{max_profit:number|null;max_loss:number|null;unlimited_loss:boolean;breakevens:number[]|null;premium:number|null}};
export type PaperRun={id:string;strategy_id:string;strategy_name:string|null;status:'open'|'closed';revision:{id:string;n:number;name:string};policy:any;opened_reading:string;closed_reading:string|null;
 fills:{leg_id:string;action:string;side:Side;units:number;price:number;basis:string;fees:number;reading_at:string}[];rows:{leg_id:string;label:string;units:number;entry:number;exit:number|null;mark:number|null;pnl:number|null}[];
 as_of:string|null;realised:number;unrealised:number|null;fees:number;net:number|null;close_now_estimate:number|null;warnings:string[]};
export type Detail=Strategy&{snapshots:Snapshot[];activity:{kind:string;detail:string;created_at:number}[];paper:{id:string;status:string;opened_reading:string;created_at:number}[]};
export type Template={key:string;name:string;intent:string;risk:'defined'|'unhedged';tier:string;complexity:number;legs:number;param:{name:string;label:string;default:number;variants:number[]}|null;recipe:string;use:string;loses:string};
export type Candidate={template:string;name:string;recipe:string;risk:string;param:number|null;param_label:string|null;legs:any[];max_profit:Metric;max_loss:Metric;breakevens:number[];premium:number;
 capital_at_risk:number|null;pop:number|null;pnl_at_view:number;return_on_risk:number|null;profit_zone_share:number|null;evidence:{status:string;label:string};why:string[]};
export type DiscoverResult={view:string;view_label:string;as_of:string;spot:number;expiry:string;inputs:any;considered:number;candidates:Candidate[];excluded:{reason:string;label:string;count:number}[];
 binding:{reason:string;label:string;suggestion:string}|null;basis:string};

export const sb={
 underlyings:()=>api('/api/sb/underlyings') as Promise<{underlyings:{symbol:string}[]}>,
 expiries:(u:string)=>api(`/api/sb/expiries?underlying=${encodeURIComponent(u)}`) as Promise<{underlying:string;as_of:string|null;expiries:Expiry[]}>,
 chain:(u:string,e:string,signal?:AbortSignal)=>api(`/api/sb/chain?underlying=${encodeURIComponent(u)}&expiry=${e}`,undefined,{},signal) as Promise<Chain>,
 templates:()=>api('/api/sb/templates') as Promise<{templates:Template[];later:{key:string;name:string;reason:string}[]}>,
 resolve:(template:string,underlying:string,expiry:string,param?:number|null,lots=1)=>api('/api/sb/templates/resolve',{template,underlying,expiry,param,lots}) as Promise<{template:string;param:number;legs:any[];as_of:string}>,
 analyze:(body:Body,signal?:AbortSignal)=>api('/api/sb/analyze',{body},{},signal) as Promise<Analysis>,
 discover:(req:any)=>api('/api/sb/discover',req) as Promise<DiscoverResult>,
 list:()=>api('/api/sb/strategies') as Promise<{strategies:LibraryRow[]}>,
 create:(body:Partial<Body>,name?:string,thesis?:string)=>api('/api/sb/strategies',{body,name,thesis}) as Promise<Strategy>,
 get:(id:string)=>api(`/api/sb/strategies/${id}`) as Promise<Detail>,
 save:(id:string,version:number,body:Body)=>api(`/api/sb/strategies/${id}/draft`,{version,body}) as Promise<Strategy>,
 meta:(id:string,m:{name?:string;thesis?:string;tags?:string[]})=>api(`/api/sb/strategies/${id}/meta`,m) as Promise<Strategy>,
 snapshot:(id:string,name?:string)=>api(`/api/sb/strategies/${id}/snapshots`,{name}) as Promise<any>,
 restore:(id:string,revision_id:string,version:number)=>api(`/api/sb/strategies/${id}/restore`,{revision_id,version}) as Promise<Strategy>,
 duplicate:(id:string,revision_id?:string)=>api(`/api/sb/strategies/${id}/duplicate`,{revision_id}) as Promise<Strategy>,
 archive:(id:string,archived:boolean)=>api(`/api/sb/strategies/${id}/archive`,{archived}) as Promise<Strategy>,
 paperStart:(id:string)=>api(`/api/sb/strategies/${id}/paper`,{confirm:true}) as Promise<PaperRun>,
 paperList:()=>api('/api/sb/paper') as Promise<{runs:PaperRun[]}>,
 paperClose:(run:string)=>api(`/api/sb/paper/${run}/close`,{confirm:true}) as Promise<PaperRun>,
};
