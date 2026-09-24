// The workspace's wire types — the structured definition the server validates and stores
// (server/kanida_pilot/workspace/model.py). The page never invents a field the server would drop.
export type Size='S'|'M'|'L'|'F';
export type Height='short'|'tall';
export type Focus={side:'CE'|'PE'|null;strikes:number[];from:string|null;to:string|null;source:string|null;because:string[]};
export type Selected={underlying:string|null;expiry:string|null;focus:Focus|null};
export type Widget={widget_id:string;widget_type:string;position:number;size:Size;height:Height;follow_workspace:boolean;
 instrument:string|null;expiry:string|null;strike_range:null;timeframe:null;scanner_id:string|null;settings:Record<string,any>};
export type Columns='auto'|2|3|4|5;
export type Definition={name:string;selected:Selected;widgets:Widget[];layout?:{columns:Columns}};
export type Workspace=Definition&{id:string;version:number;template:string|null;created_at:number;updated_at:number;position:number};

export type WidgetSpec={type:string;label:string;category:string;instrument:boolean;size:Size;height:Height;blurb:string;
 settings:Record<string,string|string[]>;source:string;min_width?:number};
export type Registry={categories:string[];sizes:Record<Size,number>;heights:Height[];widgets:WidgetSpec[]};
export type Template={key:string;name:string;description:string;widgets:string[]};

export type SummaryLine={text:string;source:string};
export type Summary={available:boolean;text?:string;underlying:string;expiry?:string;as_of?:string;
 sections?:Record<'context'|'changed'|'where'|'persistent'|'conflicting'|'key_strikes'|'unusual',SummaryLine[]>;
 read_from?:string[];not_read?:string[];caveat?:string;readings?:number};
