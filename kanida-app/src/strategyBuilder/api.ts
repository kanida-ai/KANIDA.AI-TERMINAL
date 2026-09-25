// The Strategy Builder's calls (server/kanida_pilot/strategy_builder/routes.py). GET + POST only, through the app's `api`.
import {api} from '../model';

export type Side='B'|'S';export type Kind='CE'|'PE';
export type Basis='exec'|'mid'|'ltp'|'manual';
export type Leg={id:string;type:Kind;side:Side;strike:number;lots:number;expiry:string;price_basis:Basis;price:number|null;include:boolean};
export type Scenario={spot?:number;at?:string;iv_shift?:number};
export type Body={underlying:string;expiry:string;legs:Leg[];scenario:Scenario;template?:string|null;param?:number|null};
export type Metric={status:'available'|'unavailable'|'unsupported';value?:any;unit?:string;basis?:string;reason?:string;unlimited?:boolean;[k:string]:any};
export type ChainSide={token:number;symbol:string;ltp:number|null;bid:number|null;ask:number|null;oi:number|null;volume:number|null;iv:number|null;iv_reason:string|null;flags:string[];basis:string;last_trade_time:string|null};
export type ChainRow={strike:number;CE:ChainSide|null;PE:ChainSide|null};
export type Chain={underlying:string;expiry:string;as_of:string;spot:number;lot_size:number;tick_size:number;strike_step:number;atm_strike:number;atm_iv:number|null;days_to_expiry:number;rows:ChainRow[];quality:{bid_ask:string;source:string;live:boolean}};
export type Expiry={expiry:string;lot_size:number;contracts:number;monthly:boolean;days_to_expiry:number};
export type LegRow={id:string;label:string;units:number;entry:number;ltp:number|null;iv:number|null;iv_source:string;target_price:number|null;target_pnl:number|null;greeks:Record<string,number>|null};
export type Analysis={status:string;input_hash:string;underlying?:string;structure:{key:string|null;name:string;exact:boolean};model_version?:string;as_of?:string;spot?:number;expiry?:string;lot_size?:number;
 scenario?:{spot:number;at:string;iv_shift:number;days_to_expiry:number;is_expiry:boolean;active?:boolean};greeks_scenario?:any;pop_scenario?:Metric;breakevens_target?:Metric;insights?:{key:string;level:'warn'|'info';text:string}[];premium?:Metric;charges?:Metric;max_profit?:Metric;max_loss?:Metric;breakevens?:Metric;
 reward_risk?:Metric;capital_at_risk?:Metric;pop?:Metric;margin?:Metric;scenario_pnl?:Metric;greeks?:any;legs?:LegRow[];curve?:{s:number;expiry:number|null;target:number|null}[];
 legs_quotes?:{id:string;bid:number|null;ask:number|null;ltp:number|null;basis_used:string}[];sd?:{sigma:number|null;bands:{k:number;low:number;high:number}[];bands_to_date?:{k:number;low:number;high:number}[]};table?:{s:number;pct:number;target:number|null;expiry:number|null}[];warnings:string[];quality?:any;price_basis?:string[]};
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
 capital_at_risk:number|null;pop:number|null;pnl_at_view:number;return_on_risk:number|null;profit_zone_share:number|null;evidence:{status:string;label:string;note?:string;runs_tried?:number;n_oos?:number;mean_oos?:number|null;ror_low?:number|null;tests?:number;runs_of_rule?:number};why:string[]};
export type DiscoverResult={evidence?:{tests:number;survivors:number;fdr_q:number;min_oos:number};view:string;view_label:string;as_of:string;spot:number;expiry:string;inputs:any;considered:number;candidates:Candidate[];excluded:{reason:string;label:string;count:number}[];
 binding:{reason:string;label:string;suggestion:string}|null;basis:string};

export type AdjustOrder={id:string;type:Kind;strike:number;side:Side;lots:number;qty:number;symbol:string;price:number;basis:string;charges:number;effect:'open'|'close'};
export type AdjustCandidate={rule:string;k:number|null;name:string;explain:string;available:boolean;reason?:string;note?:string;orders?:AdjustOrder[];price_basis?:string[];cash?:number;charges?:number;
 after?:{worst:number|null;unlimited_loss:boolean;best:number|null;breakevens:number[]};delta?:{current:number|null;after:number|null;change:number|null};margin?:{current:number|null;after:number|null;change:number|null};
 overlay?:{s:number;current:number;after:number}[];structure_after?:string;evidence:{status?:string;label:string;note?:string;run_id?:string;runs_tried?:number}};
export type AdjustResult={as_of:string;spot:number;lot_size:number;structure:string;template:string|null;param:number|null;held:boolean;tested:{leg_id:string;label:string;distance_pct:number}|null;
 current:{worst:number|null;unlimited_loss:boolean;breakevens:number[];delta:number|null;margin:number|null};entry_basis:string;candidates:AdjustCandidate[];notes:string[];deployment_id:string|null;draft_version:number};
export const sb={
 adjustCandidates:(id:string,deploymentId?:string)=>api(`/api/sb/strategies/${id}/adjust/candidates`,{deployment_id:deploymentId||null}) as Promise<AdjustResult>,
 adjustApply:(id:string,o:{rule:string;k:number|null;version:number;deployment_id?:string})=>api(`/api/sb/strategies/${id}/adjust/apply`,o) as Promise<{strategy:any;adjustment:AdjustCandidate;deployment_id:string|null}>,
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

export type Gate={gate:string;label:string;pass:boolean;detail:string;deferred?:boolean};
export type LiveCapability={enabled:boolean;reason:string;configured?:boolean;reachable?:boolean;live_allowed?:boolean;gates?:Gate[];engine_user?:string;broker_account_id?:string|null;
 arm?:{expires_at:string;armed_by:string;max_baskets:number;baskets_used:number;max_loss_per_basket:number}|null};
export type AutotradeLeg={tradingsymbol:string;side:string;quantity:number;limit_price:number;group:number;state:string;filled_qty?:number;avg_price?:number|null;error?:string|null};
export type AutotradeRoute={id:string;strategy_id:string;preview_id:string;mode:'dry_run'|'live';intent_id:string|null;state:string;reason:string|null;created_at:number;updated_at:number;
 intent:{id:string;state:string;mode:string;reason:string|null;max_loss?:number;legs:AutotradeLeg[]}|null;refresh_error?:string};
export type Check={key:string;label:string;status:'pass'|'warn'|'block';detail:string};
export type PreviewOrder={leg_id:string;symbol:string;type:Kind;strike:number;side:Side;qty:number;lots:number;lot_size:number;limit:number;bid:number;ask:number;group:number;slices:number[];charges:number};
export type Preview={id:string;hash:string;kind:'open'|'close'|'adjust';strategy_id:string;deployment_id:string|null;product:string;price_policy:string;orders:PreviewOrder[];checks:Check[];can_submit:boolean;requires_ack:boolean;
 margin:{initial:number;final:number;per_leg:any[]}|null;charges:number;structure:string;as_of:string;spot:number;expires_at:number;ttl:number;mode:string;live:LiveCapability;sequence:string;net_premium:number};
export type Intent={id:string;kind:string;grp:number;seq:number;leg_id:string;symbol:string;side:Side;qty:number;limit_price:number;state:string;filled_qty:number;avg_price:number|null;fees:number;reason:string|null};
export type Deployment={id:string;strategy_id:string;strategy_name?:string;status:string;mode:string;product:string;opened_at:number;closed_at:number|null;margin:any;intents:Intent[];
 fills:{leg_id:string;side:Side;qty:number;price:number;fees:number;quote_ts:string}[];positions:{leg_id:string;label:string;units:number;avg:number|null;mark:number|null;unrealised:number|null;realised:number;fees:number}[];
 realised:number;unrealised:number|null;fees:number;net:number|null;marked_at:string|null;mark_basis:string;revision:{id:string;n:number;name:string}|null;live:{enabled:boolean;reason:string}};
export type Status={live:boolean;source:string;reason:string|null;market_open:boolean;now_ist:string;live_orders:{enabled:boolean;reason:string};paper_capital:{capital:number;blocked:number;available:number}|null};
export const exec={
 status:()=>api('/api/sb/status') as Promise<Status>,
 preview:(id:string,o:{product?:string;price_policy?:string;limits?:Record<string,number>})=>api(`/api/sb/strategies/${id}/preview`,o) as Promise<Preview>,
 deploy:(id:string,p:Preview,key:string,ack=false)=>api(`/api/sb/strategies/${id}/deployments`,{preview_id:p.id,preview_hash:p.hash,idempotency_key:key,confirm:true,ack_unlimited:ack}) as Promise<Deployment>,
 list:()=>api('/api/sb/deployments') as Promise<{deployments:Deployment[];paper_capital:any}>,
 get:(id:string)=>api(`/api/sb/deployments/${id}`) as Promise<Deployment>,
 cancel:(id:string)=>api(`/api/sb/deployments/${id}/cancel`,{}) as Promise<Deployment>,
 closePreview:(id:string,price_policy='marketable')=>api(`/api/sb/deployments/${id}/close-preview`,{price_policy}) as Promise<Preview>,
 autotradeCapability:()=>api('/api/sb/autotrade/capability') as Promise<LiveCapability>,
 autotrade:(id:string,p:Preview,key:string,mode:'dry_run'|'live',confirmLive=false)=>api(`/api/sb/strategies/${id}/autotrade`,{preview_id:p.id,preview_hash:p.hash,idempotency_key:key,mode,confirm_live:confirmLive}) as Promise<AutotradeRoute>,
 autotradeRoutes:(strategyId:string)=>api(`/api/sb/autotrade/routes?strategy_id=${encodeURIComponent(strategyId)}`) as Promise<{routes:AutotradeRoute[]}>,
 autotradeGet:(rid:string)=>api(`/api/sb/autotrade/routes/${rid}`) as Promise<AutotradeRoute>,
 autotradeCancel:(rid:string)=>api(`/api/sb/autotrade/routes/${rid}/cancel`,{}) as Promise<AutotradeRoute>,
 adjustPreview:(id:string,price_policy='marketable',limits?:Record<string,number>)=>api(`/api/sb/deployments/${id}/adjust-preview`,{price_policy,limits}) as Promise<Preview>,
 adjust:(id:string,p:Preview,key:string,ack=false)=>api(`/api/sb/deployments/${id}/adjust`,{preview_id:p.id,preview_hash:p.hash,idempotency_key:key,confirm:true,ack_unlimited:ack}) as Promise<Deployment>,
 close:(id:string,p:Preview,key:string)=>api(`/api/sb/deployments/${id}/close`,{preview_id:p.id,preview_hash:p.hash,idempotency_key:key,confirm:true}) as Promise<Deployment>,
};

export type AlertRule={id:string;strategy_id:string;strategy_name?:string;deployment_id:string|null;type:string;params:any;session:string;cooldown:number;channels:string[];state:string;version:number;
 last_eval_at:string|null;last_value:number|null;last_triggered_at:number|null;label:string;description:string;scope:'strategy'|'deployment'};
export type AlertEvent={id:string;rule_id:string;strategy_id:string;strategy_name?:string;deployment_id:string|null;kind:string;value:number|null;message:string;occurred_at:string;created_at:number;acked_at:number|null};
export const alerts={
 all:()=>api('/api/sb/alerts') as Promise<{rules:AlertRule[];events:AlertEvent[];unacked:number;types:{key:string;label:string;scopes:string[]}[];boundary:string}>,
 unacked:()=>api('/api/sb/alerts/unacked') as Promise<{unacked:number;latest:AlertEvent[]}>,
 forStrategy:(id:string)=>api(`/api/sb/strategies/${id}/alerts`) as Promise<{rules:AlertRule[];events:AlertEvent[]}>,
 create:(id:string,body:{type:string;params:any;deployment_id?:string|null;channels?:string[]})=>api(`/api/sb/strategies/${id}/alerts`,body) as Promise<AlertRule&{now:any}>,
 update:(rid:string,body:{version:number;action?:string;params?:any})=>api(`/api/sb/alerts/${rid}`,body) as Promise<AlertRule>,
 remove:(rid:string)=>api(`/api/sb/alerts/${rid}/delete`,{}) as Promise<{ok:boolean}>,
 check:(rid:string)=>api(`/api/sb/alerts/${rid}/check`,{}) as Promise<any>,
 ack:(eid?:string)=>api('/api/sb/alert-events/ack',eid?{event_id:eid}:{}) as Promise<{acknowledged:number}>,
};

export type LabStats={n:number;expectancy?:number;ci95?:[number,number];per_100_capital?:number|null;total?:number;max_drawdown?:number;worst?:number;best?:number;win_rate?:number;avg_win?:number|null;avg_loss?:number|null;avg_hold_days?:number};
export type LabRun={evidence?:{status:string;p:number|null;tests:number;n_oos:number;low:number|null;runs_of_rule:number;decides:boolean};id:string;strategy_id:string|null;kind:string;spec:any;status:'running'|'completed'|'failed';progress:number;error:string|null;created_at:number;finished_at:number|null;
 result:null|{kind:string;model?:string;badge:{status:string;label:string};stats:{all:LabStats;discovery:LabStats;oos:LabStats};adjustment?:any;control:{reps:number;mean_expectancy:number|null;actual_percentile:number|null;oos_mean_expectancy?:number|null;oos_actual_percentile?:number|null};
  equity?:{day:string;equity:number;split:string}[];trades?:any[];skipped?:Record<string,number>;lot_size?:number;provenance?:any}};
export type Replay={kind:string;entry_at?:string;points:{t:string;pnl:number}[];skipped_bars:number;last?:number;best?:number;worst?:number;coverage:Record<string,number>;note?:string;source:string;legs:{symbol:string;label:string}[];interval:string;problems:string[]};
export const lab={
 start:(spec:any)=>api('/api/sb/lab/backtests',spec) as Promise<LabRun>,
 batches:()=>api('/api/sb/lab/batches') as Promise<{batches:any[];grids:Record<string,{name:string;underlying:string;why:string}>}>,
 batch:(id:string)=>api(`/api/sb/lab/batches/${id}`) as Promise<any>,
 runs:(strategyId?:string)=>api(`/api/sb/lab/runs${strategyId?`?strategy_id=${strategyId}`:''}`) as Promise<{runs:LabRun[]}>,
 run:(id:string)=>api(`/api/sb/lab/runs/${id}`) as Promise<LabRun>,
 replay:(strategyId:string,interval:string,days:number)=>api(`/api/sb/strategies/${strategyId}/replay`,{interval,days}) as Promise<Replay>,
};
