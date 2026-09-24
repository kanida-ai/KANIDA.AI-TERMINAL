// The Options Screener's wire types — the same structured definition the server stores, evaluates and alerts on
// (server/kanida_pilot/screener/definition.py). Nothing here invents a field the server does not send.
export type Side='CE'|'PE'|'either'|'both';
export type Window={kind:'minutes'|'readings';value:number}|{kind:'open'}|{kind:'custom';from:string;to:string};
export type Condition={id?:string;join?:'and'|'or';metric:string;side?:Side|null;state:string;window:Window};
export type Strikes={kind:'atm';below:number;above:number}|{kind:'otm'|'itm';depth:number}|{kind:'delta';band:string};
export type Universe={kind:'all'|'indices'|'stocks'}|{kind:'symbols';symbols:string[]};
export type Definition={universe:Universe;expiry:'nearest'|'next'|'both';strikes:Strikes;liquid_only:boolean;conditions:Condition[]};

export type StateOption={key:string;label:string;arrow:string;tone:'up'|'down'|'warn'|'flat'};
export type MetricOption={key:string;label:string;group:string;grain:'contract'|'book'|'side';computed:boolean;
 sides:Side[];states:StateOption[];default_state:string;help:string;field:string};
export type Vocabulary={metrics:MetricOption[];sides:{key:Side;label:string}[];windows:(Window&{label:string})[];
 universes:{key:string;label:string}[];expiries:{key:string;label:string}[];
 strike_presets:(Strikes&{label:string})[];max_rungs:number;delta_bands:{key:string;label:string;lo:number;hi:number}[];
 lifecycle:{key:string;label:string}[];index_underlyings:string[];reading_minutes:number;max_conditions:number;
 underlyings:string[]};

export type Notify={new:boolean;ended:boolean;changed:boolean};
export type Scanner={id:string;name:string;description?:string|null;is_default:boolean;mine:boolean;source:string;
 nl_text?:string|null;definition:Definition;reads_as:string;grain:'contract'|'book';updated:number;notify:Notify;
 counts?:{active:number;ended:number;all:number;through:string;current:boolean}|null};

export type Status='new'|'still'|'strengthening'|'weakening'|'ended';
export type Tick='none'|'unseen'|'gap'|Status;
export type Match={key:string;title:string;underlying:string;expiry:string;strike?:number;type?:'CE'|'PE';symbol?:string;
 status:Status;status_at:string|null;active:boolean;first_matched:string;episode_started:string;last_matched:string;
 ended_at:string|null;readings_matched:number;episode_readings:number;duration_min:number;
 episodes:{start:string;end:string|null;readings:number}[];timeline:Tick[];events:{at:string;kind:string;status:string}[];
 because:string[];ended_because:string|null;thin:boolean;computed:boolean};
export type Results={available:boolean;text?:string;session?:string;as_of?:string;readings:string[];grain:string;
 notes:{kind:string;text:string}[];matches:Match[];counts?:{active:number;ended:number;all:number};view?:string;
 truncated?:boolean;shown?:number;reads_as?:string;elapsed_ms?:number};
export type Parsed={definition:Definition;reads_as:string;understood:{phrase:string;condition:number}[];
 assumptions:string[];unmapped:string[]};
export type Alert={id:number;scanner_id:string;scanner_name?:string;session:string;reading_at:string;entity_key:string;
 kind:'new'|'ended'|'changed';title:string;text:string;created:number;read:number};
export type ScreenerStatus={available:boolean;text?:string;session?:string;as_of?:string;readings?:number;first?:string};
